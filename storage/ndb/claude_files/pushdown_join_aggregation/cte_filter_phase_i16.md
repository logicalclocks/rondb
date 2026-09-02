# Phase I.16 — partial-key joins to multi-key CTEs

## Status

**I.16a + I.16b + I.16c shipped.**

### I.16a — clean rejection

| Commit | Scope |
|--------|-------|
| `4e6f96dc5ae` | RonSQL — emit_child_ops's CTE_LOOKUP arm gains a permanent-error guard.  Counts the CTE body's `groupby_columns` (the virt PK shape used by `build_cte_virtual_tables`) and compares against `op.num_key_cols`.  Mismatch throws with a self-explanatory message naming the workaround instead of letting `lookupCte()` return NULL with the opaque "Failed to create child operation" downstream |
| `0906344763c` + `f43c822ce60` | MTR — `ronsql_cte_partial_key.test` exercises the rejected shape and confirms the new message.  Recorded `.result` shows `Error handling: RPE` followed by the I.16a string — the SRE,te→RPE chain that wrapped the old NdbQueryBuilder failure is gone |

### I.16b — auto-rewrite to CTE_SCAN root

Took the AST-rewrite approach (cheaper than mutating the planner
output): pre-planner pass in `load_join` swaps `root_table` with
`joins[0].table` and flips every ON condition's child / parent
fields, then `QueryPlanner::plan` runs against the swapped AST and
produces a CTE_SCAN root with the original parent as a child via
its existing PK / unique / index lookup logic.

Conservative first cut — applies only when:
- exactly one JOIN clause, INNER
- the JOIN target is a CTE in scope (multi-key)
- the CTE has GROUP BY with N columns and the join binds fewer
  than N column-pairs (i.e. would otherwise hit the I.16a guard)
- the original root is a real table (not a CTE)

| Commit | Scope |
|--------|-------|
| `9fcbf1e8352` | RonSQL — `load_join` AST swap.  TableRef root is a pointer, JoinClause's table is a value; copy goes through a saved TableRef temporary.  Each JoinCondition has child_table/column and parent_table/column swapped 1-for-1 |
| `f417051a4c4` + recorded result | MTR — `ronsql_cte_partial_key.test` recast as I.16a + I.16b combo.  Test 1: same shape that I.16a rejected now executes via the rewrite, strict diff against MySQL's reference passes.  Test 2: LEFT JOIN with the same shape stays on the I.16a clean-reject path — outside I.16b's scope because outer-join semantics would shift under the swap |

Shapes still falling through to the I.16a clean-reject path:
multi-table queries with the multi-key CTE somewhere in the join
chain, LEFT_OUTER joins, and CTE-on-CTE shapes.  These remain
queued for a follow-up if real queries surface them.

### I.16c — N-table chain rewrite + outer-join handling

**Redefined scope (per user direction):** stay on the
root-reordering approach.  Don't pursue true non-root CTE_SCAN
children — the NDB API doesn't support them, so RonSQL should
follow what the API offers.  Instead extend I.16b's AST-rewrite
to handle:

| Commit | Scope |
|--------|-------|
| `8fd075b7ad9` | RonSQL — `load_join`'s rewrite walks the joins list (instead of only checking `joins[0]`) and finds the first INNER-join whose target is a multi-key CTE.  Splices the matched JoinClause out, allocates a new JoinClause carrying the original root with the matched conditions flipped (child<->parent), inserts that new clause at the head of the joins list, and promotes the matched TableRef to root.  Other joins keep their alias-based parent references.  Plan doc updated with the redefined scope and the "out of scope: non-root CTE_SCAN children" note |
| `318066232e9` + recorded result | MTR — `ronsql_cte_partial_key.test` Tests 3-5: 3-table inner chain (CTE in joins[0]), 3-table chain with LEFT JOIN to a real table elsewhere, and 3-table chain with the multi-key CTE in joins[1].  All match MySQL's reference exactly |
- Any number of joined tables (not just two).
- Outer joins ELSEWHERE in the query (the multikey-CTE-join
  itself must still be INNER for the swap to preserve
  semantics).
- Reject cleanly when the multikey-CTE-join is LEFT_OUTER —
  swapping would replace `A LEFT JOIN cte` with
  `cte INNER JOIN A` and lose the unmatched-A rows.

Mechanics:
- Walk `ast_root.joins` and find the JoinClause referencing the
  partial-key CTE.  Bail to I.16a if its join_type is
  LEFT_OUTER.
- Remove that JoinClause from the linked list.
- Promote its TableRef to `ast_root.root_table`.  Demote the
  original root's TableRef into a synthetic JoinClause inserted
  at position 0 of the joins list, with the CTE's ON conditions
  flipped (child<->parent).
- Other joins keep their alias-based parent references — they
  resolve unchanged after QueryPlanner re-runs against the
  rewritten AST.

Constraints kept:
- Original root is a real table (CTE-on-CTE shapes still
  rejected).
- Original root must be reachable as a child of the CTE — i.e.
  it must have a usable PK / unique / index on the join columns.
  Same downstream guarantee QueryPlanner already enforces for
  the I.16b two-table case; extends for free.

Out of scope:
- True non-root CTE_SCAN child support (would need NDB API
  changes — `scanCte()` doesn't accept a key array, and
  parent-row correlation isn't wired through DBSPJ for
  CTE_SCAN nodes today).  Held permanently unless NDB API
  evolves.

## Problem

RonSQL currently plans every joined CTE child as `CTE_LOOKUP`.
That is correct only when the join predicate supplies the complete
virtual CTE primary key.

The virtual CTE primary key is derived from the CTE body's `GROUP BY`
columns.  For example:

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, SUM(GREATEST(pairs.k, pairs.amt)) AS gmax
FROM cte_customer AS c
JOIN pairs ON pairs.k = c.c_id
GROUP BY c.c_id;
```

The CTE key is `(k, amt)`, but the join supplies only `k`.  The
planner still emits `CTE_LOOKUP`, `emit_child_ops()` calls
`lookupCte()` with one key for a two-key virtual table, and the
operation builder returns `NULL`.  The current SQL-visible error is:

```text
Failed to create child operation.
NDB Success 0, No error
```

That error is too late and too opaque.  It also hides that the query
shape is semantically doable if the CTE is scanned instead of looked
up by full key.

## Desired Behaviour

RonSQL should classify joined CTE children by key coverage:

1. Full CTE key supplied by equality predicates:
   use `CTE_LOOKUP`.
2. Partial CTE key supplied:
   do not emit `CTE_LOOKUP`.
3. No CTE key supplied:
   do not emit `CTE_LOOKUP`.

For cases 2 and 3, RonSQL should either choose a supported CTE scan
plan or reject early with a clear error.

## Phase I.16a — planner validation and clear reject

Add a RonSQL-side guard before `lookupCte()` is emitted.

Implementation outline:

- In `QueryPlanner::plan()` or an immediate validation pass after it,
  identify `JoinOp::CTE_LOOKUP` children.
- Derive the CTE virtual key columns from the CTE body's `GROUP BY`
  outputs, using the same output order and names as
  `build_cte_virtual_tables()`.
- Compare the child join key columns against the complete CTE key.
- If the join does not bind the complete key, reject with a clear
  permanent error, for example:

```text
Partial CTE lookup key not supported; use CTE as the joined root scan.
```

This does not add new execution support, but it prevents the confusing
`Failed to create child operation` error.

Test coverage:

- MTR negative test for a two-column grouped CTE joined on one key.
- Confirm full-key CTE_LOOKUP tests still pass.

## Phase I.16b — supported rewrite to CTE_SCAN root

Add planner support for choosing `CTE_SCAN` when the CTE can legally
drive the join.

The first supported rewrite can be conservative:

- Inner joins only.
- CTE appears as the joined child in SQL.
- The partial-key predicate is equality-only.
- All other joined tables can be reached as real-table children from
  the CTE scan root using their existing lookup/index paths.
- Preserve user-visible grouping/projection by remapping the select
  and group-by references from the original parent table to the CTE
  key where needed.

For the example above, the plan becomes equivalent to:

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT pairs.k AS c_id, SUM(GREATEST(pairs.k, pairs.amt)) AS gmax
FROM pairs
JOIN cte_customer AS c ON c.c_id = pairs.k
GROUP BY pairs.k;
```

This is already the manual workaround used by the Phase I.5 v6 tests.

Test coverage:

- Positive MTR for the original SQL shape:
  real table first, multi-key CTE child, partial key predicate.
- Include an aggregate expression that uses two CTE columns, e.g.
  `SUM(GREATEST(pairs.k, pairs.amt))`, to ensure the rewritten plan
  still exposes both CTE columns to the aggregation compiler.
- Include a variant with a joined real-table column in the expression,
  e.g. `GREATEST(pairs.k, pairs.amt, c.c_floor)`.

## Phase I.16c — non-root CTE_SCAN child, if needed

If root reordering is too restrictive, implement true non-root
`CTE_SCAN` child support.

Today `emit_child_ops()` explicitly rejects `CTE_SCAN` children with
linked keys because `scanCte()` takes no key array.  Supporting this
shape likely needs:

- A way to attach the join predicate as a filter program or
  post-scan predicate.
- Correct linked-attribute availability for parent columns.
- Clear semantics for batching and correlation when the child scan can
  return many rows per parent.
- Outer-join handling deferred unless explicitly included.

This is larger than I.16b and may belong in a later phase if root
reordering handles the important SQL shapes.

## Non-Goals

- Do not change CTE key derivation.  `GROUP BY` outputs remain the
  virtual primary key.
- Do not support partial-key `lookupCte()` directly unless the kernel
  gains an indexed/range lookup over materialized CTE results.
- Do not include LEFT JOIN support in the first implementation.

## Completion Criteria

- RonSQL never reaches `lookupCte()` with fewer keys than the virtual
  CTE primary key requires.
- Partial-key CTE joins produce either a supported CTE scan plan or a
  clear RonSQL error.
- The original Phase I.5 v6 linked-vs-linked query shape can be added
  as a positive MTR test once I.16b or equivalent support lands.

## Addendum (August 2026 — non_aggregate_phase_6.md)

The I.16b/c rewrite now also runs for NON-aggregate queries: extracted
into `RonSQLPreparer::maybe_rewrite_partial_key_cte_root()` and called
from `parse()` ahead of the projection-only gate (the original
`load_join()` call site remains, now a no-op for rewritten queries via
the `root_is_cte` bail).  The demoted-root JoinClause copy additionally
clears its index-hint fields for parity across the two call sites.
Pass-through coverage: gc-14..16 + gc-P1a/b/c in
`body_passthrough_groupby_cte.inc`.
