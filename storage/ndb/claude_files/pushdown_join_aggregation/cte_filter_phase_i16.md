# Phase I.16 — partial-key joins to multi-key CTEs

## Status

**I.16a shipped.**  I.16b and I.16c remain deferred.

| Commit | Scope |
|--------|-------|
| `4e6f96dc5ae` | RonSQL — emit_child_ops's CTE_LOOKUP arm gains a permanent-error guard.  Counts the CTE body's `groupby_columns` (the virt PK shape used by `build_cte_virtual_tables`) and compares against `op.num_key_cols`.  Mismatch throws with a self-explanatory message naming the workaround instead of letting `lookupCte()` return NULL with the opaque "Failed to create child operation" downstream |
| `0906344763c` + recorded result | MTR — `ronsql_cte_partial_key.test` exercises the rejected shape (two-column GROUP BY CTE, join on one key) and confirms the new message.  Recorded `.result` shows `Error handling: RPE` followed by the I.16a string — the SRE,te→RPE chain that wrapped the old NdbQueryBuilder failure is gone |

I.16b (planner-side rewrite to CTE_SCAN root when partial key
detected) and I.16c (true non-root CTE_SCAN child support) stay
on the queue per the original phase split below.

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
