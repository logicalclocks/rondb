# Phase I.20 - CTE lookup key coverage and rewrite validation

## Status

**Planned.**  This phase follows review of the shipped Phase I.16
partial-key CTE join work and runs alongside Phase I.21's scalar-CTE
guardrails.

I.16a added a clean guard for joined `CTE_LOOKUP` operations whose
join supplies fewer key predicates than the virtual CTE primary key.
I.16b / I.16c then rewrote supported partial-key INNER joins so the
multi-key CTE becomes a `CTE_SCAN` root and the original parent table
becomes a child.

The review found that I.16 currently treats **key count** as **key
coverage**.  That is not strong enough.  The virtual CTE primary key is
defined by the CTE body's `GROUP BY` output order, so RonSQL must also
validate which CTE output columns are bound and in what order.

## Current state in the tree

Anchors used throughout this plan:

- `RonSQLPreparer.cpp:1192-1274` — the I.16b/c rewrite in
  `load_join()`.  Walks `m_context.ast_root.joins`, breaks on the
  first INNER multi-key CTE join with `join_cols < cte_pk_cols`,
  splices it out, builds a new `JoinClause` with flipped per-condition
  `child_table` / `parent_table` fields, and promotes the matched CTE
  TableRef to root.  Uses count-only coverage.
- `RonSQLPreparer.cpp:7343-7388` — the `JoinOp::CTE_LOOKUP` arm in
  `emit_child_ops()`.  Reads `op.cte_def->stmt->groupby_columns` to
  derive `cte_pk_cols`, then enforces
  `op.num_key_cols == cte_pk_cols`.  The keys array is built earlier
  at line 7218 from `op.parent_key_col_names[k]` in JoinCondition
  order.  Lines 7372-7382 hold I.17's dummy-key carve-out for
  scalar-CTE cross-join (`op.num_key_cols == 0 && cte_pk_cols == 0`).
- The CTE virtual PK column order is exactly the GROUP BY column
  order; `build_cte_virtual_tables()` honours that ordering when it
  populates the synthetic columns.

Implications I.20 has to honour:

- The keys[] array passed to `qb->lookupCte()` is in JoinCondition
  order, not in virtual-PK order.  Today that happens to coincide
  for tests written so far; I.20 makes the difference visible.
- I.17's dummy-key path (`op.num_key_cols == 0 && cte_pk_cols == 0`)
  must keep working unchanged when the new helper is in place.  Phase
  I.21 may further constrain when the dummy key is allowed; I.20's
  helper just has to recognise the shape and not reject it.
- The I.16c rewrite happens in `load_join()` *before* the planner
  populates `JoinOp::parent_key_col_names[]`, which means key
  reordering is most natural at AST level (reorder
  `JoinCondition` linked list) rather than after planning.

## Problems

### 1. Full-key `CTE_LOOKUP` validates only count, not identity/order

The I.16a guard in `emit_child_ops()` currently compares:

```cpp
op.num_key_cols == cte_pk_cols
```

That avoids the opaque NDB API failure when too few keys are supplied,
but it still allows semantically wrong lookups.  Example:

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT ...
FROM cte_customer AS c
JOIN pairs ON pairs.amt = c.x AND pairs.k = c.c_id;
```

The CTE virtual primary key order is `(k, amt)`, but the join
conditions produce child key order `(amt, k)`.  `lookupCte()` receives
keys in `JoinCondition` order, so the query can silently bind the
wrong values.

Wrong-column full-count cases have the same issue:

```sql
JOIN pairs ON pairs.k = c.c_id AND pairs.cnt = c.region
```

This has two predicates for a two-column PK, but `cnt` is not part of
the virtual key.

### 2. I.16 rewrite detects partial key by count only

The I.16b/c rewrite in `load_join()` currently triggers only when:

```cpp
join_cols < cte_pk_cols
```

That misses malformed full-count shapes, such as reversed `(amt, k)` or
`(k, cnt)`.  These should not proceed as normal `CTE_LOOKUP`.  They
should either be reordered to the virtual PK order or rejected clearly.

### 3. N-table rewrite assumes the matched CTE joins to the original root

I.16c walks the join list and promotes the first matching multi-key CTE
to root.  It always demotes the original SQL root as the child of the
promoted CTE and attaches the matched join's flipped ON conditions to
that new child.

That is only valid when the matched CTE join's parent alias is the
original root alias.  This is valid:

```sql
FROM c
JOIN r ON r.r_id = c.c_region
JOIN pairs ON pairs.k = c.c_id
```

because `pairs` joins to `c`, the original root.

This is not valid:

```sql
FROM c
JOIN r ON r.r_id = c.c_region
JOIN pairs ON pairs.k = r.r_id
```

The matched CTE joins to `r`, not `c`.  Promoting `pairs` and demoting
`c` with ON conditions that reference `r` creates an invalid or
misleading AST for the planner.

## Required fixes

### 1. Add CTE virtual-PK coverage helper

Add a RonSQL helper that derives the ordered virtual CTE key columns
from the CTE body's `GROUP BY` list and compares them against a
`JoinOp` / `JoinClause` key list.

The helper should answer at least:

```cpp
enum class CteKeyCoverage {
  ExactOrdered,    // keys match the virtual PK in order
  ExactPermuted,   // keys match the virtual PK as a permutation
  Partial,         // some PK columns are missing
  WrongColumns,    // at least one bound key references a non-PK CTE column
  ScalarDummy      // virtual PK is empty and no keys bound — I.17 shape
};
```

The state alone is not enough — the rest of I.20 needs richer output
to act on it.  Return a small struct:

```cpp
struct CteKeyCoverageResult {
  CteKeyCoverage state;
  // For ExactOrdered, ExactPermuted, Partial: pk_index_for_key[k] is
  // the virtual PK column index that the k-th bound join key
  // corresponds to.  -1 for keys that target a non-PK column
  // (WrongColumns case).
  int pk_index_for_key[MAX_JOIN_KEY_COLS];
  // pk_covered[i] is true iff at least one bound join key targets
  // the i-th virtual PK column.  Built from pk_index_for_key.
  bool pk_covered[MAX_JOIN_KEY_COLS];
  Uint32 num_keys;     // count of bound join keys (== op.num_key_cols)
  Uint32 num_pk_cols;  // count of virtual PK cols (== cte_pk_cols)
};
```

The implementation must compare CTE output column names, not just
counts.  The virtual PK columns are the CTE outputs corresponding to
the CTE body's `GROUP BY` columns, in the same order used by
`build_cte_virtual_tables()`.

Special cases the helper has to recognise without rejecting:

- `num_pk_cols == 0 && num_keys == 0` is `ScalarDummy` (I.17's
  cross-join shape; emit_child_ops's existing dummy-key carve-out
  at `RonSQLPreparer.cpp:7372-7382` keeps working).
- Duplicate keys on the same PK column (e.g.,
  `pairs.k = c.c_id AND pairs.k = c.c_region`) produce an entry per
  binding in `pk_index_for_key`, but the corresponding `pk_covered`
  bit is set only once.  This naturally classifies as `Partial`
  whenever some other PK column is unbound — the I.16c rewrite is
  still the right answer, the duplicate predicate is just a redundant
  filter that the planner / interpreter already handles.

Inputs to the helper:

- The CTE definition (for `groupby_columns`).
- The bound key list — either the AST-level `JoinClause::conditions`
  (when called from `load_join` for the I.16 rewrite decision) or the
  planner-resolved `JoinOp::parent_key_col_names[]` plus the matching
  child-side names (when called from `emit_child_ops`).  Both call
  sites need the same answer, so the helper takes the bound-key
  CTE-side names as a `(const LexCString*, Uint32)` array, not the
  raw `JoinClause`.

### 2. Tighten `CTE_LOOKUP` emission and reorder permuted keys

Decision: **I.20 reorders permuted keys, does not reject them.**  ON
predicate order is not user-visible in SQL semantics; making RonSQL
demand a specific order would surprise users for no functional benefit.

Before `lookupCte()` is emitted:

- `ExactOrdered`: accept as-is.
- `ExactPermuted`: reorder so the keys[] array passed to
  `qb->lookupCte()` is in virtual-PK order.  See section 6 below for
  the implementation site decision.
- `ScalarDummy`: keep the existing carve-out at
  `RonSQLPreparer.cpp:7372-7382`.  The helper's role is just to
  classify; emit_child_ops's existing branch handles the rest.
- `Partial`: keep the I.16a clear error path unless the I.16 root
  rewrite already promoted this CTE to a `CTE_SCAN` root.  Note that
  `Partial` includes the duplicate-predicate shape from section 1, so
  the rewrite remains the correct answer there too.
- `WrongColumns`: reject clearly with a message explaining that the
  join must bind virtual CTE primary key columns derived from
  `GROUP BY`, and naming which bound key referenced a non-PK column.

### 3. Use coverage helper in the I.16 rewrite decision

In `load_join()` at `RonSQLPreparer.cpp:1206-1240`, replace the
count-only check:

```cpp
if (cte_pk_cols > 0 && join_cols < cte_pk_cols) { match = cur; ... }
```

with coverage-based logic driven by the helper:

- `Partial` on an INNER join to a multi-key CTE — including the
  duplicate-predicate shape — is eligible for the I.16 root rewrite.
- `ExactOrdered` and `ExactPermuted` should remain `CTE_LOOKUP` and
  be reordered (if permuted) at the chosen reorder site, not by the
  rewrite.
- `WrongColumns` does not rewrite.  The rewrite scan continues so a
  later valid match elsewhere in the chain still works; if no valid
  match is found, the I.16a-style permanent error fires later in
  `emit_child_ops` with the column-level message from section 2.
- `ScalarDummy` does not participate in the rewrite at all.

This keeps the rewrite focused on the real partial-key shape and
avoids masking invalid full-count joins.

### 4. Validate matched CTE parent alias before N-table rewrite

The current loop in `RonSQLPreparer.cpp:1213-1240` walks the joins
list and `break`s on the first INNER multi-key CTE join with
partial-key coverage, regardless of which alias the join's
`parent_table` references.  Today's I.16c implementation then assumes
that parent alias is the original SQL root.

Two decisions for I.20:

1. **What is "the original root alias"?**  It is
   `m_context.ast_root.root_table->name` as observed at the **start**
   of the I.16 rewrite pass, before the rewrite mutates the field.
   Capture it into a local at the top of the rewrite block so the
   parent-alias check is unambiguous.

2. **What does the loop do on a wrong-parent-alias multi-key CTE
   join?**  Continue past it.  Reasoning: the chain may contain a
   later valid CTE join that *does* reference the original root, and
   rejecting on the first wrong-parent match would prevent that
   query from working.  If no valid match is found by the end of the
   walk, no rewrite happens and emit_child_ops fires the
   `WrongColumns` / `Partial` error from section 2 with the
   column-level message.

Concretely, change the rewrite scan to: walk the entire joins list,
record the first **valid** match (`Partial` AND every condition's
parent alias equals the captured original root alias), apply the
rewrite once.  No broader join-tree reordering — moving an
intermediate parent path under the CTE root is a planner-shaped
problem and stays out of I.20.

### 5. Where to perform key reordering

Three candidate sites for the `ExactPermuted` reorder; trade-offs:

1. **AST level inside `load_join()` after the I.16 rewrite scan,
   before `QueryPlanner::plan()` runs.**

   For each multi-key INNER CTE join whose coverage is
   `ExactPermuted`, sort its `JoinCondition` linked list so the
   conditions appear in virtual-PK order (the helper already returns
   `pk_index_for_key[]`, so this is a one-pass linked-list reorder
   keyed by that array).  After this, the planner naturally builds
   `op.parent_key_col_names[]` in virtual-PK order, and
   `emit_child_ops` builds `keys[]` correctly without any further
   change.

   Pros: smallest emit-side delta — `emit_child_ops` does not change
   except to call the helper for classification / error reporting.
   Same site as the I.16 rewrite, so all CTE-key normalisation lives
   in one place.  No new fields on `JoinOp`.

   Cons: the rewrite has to run on the original AST list, which is
   already the case in `load_join`.

2. **Planner level — make `QueryPlanner::plan()` build
   `parent_key_col_names[]` in virtual-PK order when the target is a
   CTE.**

   Pros: keeps the AST untouched.

   Cons: planner has to depend on CTE virtual PK derivation logic,
   which today lives in RonSQL's CTE virtual-table layer, not in the
   shared planner.  Crosses a layer that has been deliberately kept
   thin.  Also makes the I.16 rewrite decision and the reorder
   decision happen in two different files.

3. **Emit level — apply the permutation when constructing `keys[]`
   at `RonSQLPreparer.cpp:7218`.**

   Pros: most local; only emit_child_ops changes.

   Cons: requires a new per-op permutation field on `JoinOp` (or a
   per-CTE_LOOKUP side table) so the helper's verdict survives
   between classification and emit.  Adds a state-passing step that
   site 1 avoids entirely.

**Recommendation: site 1 (AST-level reorder in `load_join`).**  It is
the only option that keeps the change confined to one file and one
data structure that is already being mutated by I.16.  The reorder
is a stable sort on a small linked list (≤ `MAX_JOIN_KEY_COLS`
entries), called only on `ExactPermuted` matches, so the cost is
negligible.

Implementation sketch:

```cpp
// Inside load_join, after the I.16 rewrite scan but before
// QueryPlanner::plan().  Walk every JoinClause in m_context.ast_root.joins
// (and m_context.ast_root.root_table is unaffected — only joins to a
// multi-key CTE need this).  For each CTE-targeted clause:
//   - call cte_key_coverage(child_cte, jc->conditions) → result
//   - if result.state == ExactPermuted, splice jc->conditions into a
//     temp array, then rebuild the linked list in result.pk_index_for_key
//     order.
//   - if result.state == WrongColumns, do not rewrite the conditions;
//     emit_child_ops will fire the column-level error.
```

`emit_child_ops` then re-classifies (cheap, deterministic) only to
choose between `ExactOrdered`, `ScalarDummy`, `Partial`-after-rewrite-
failed, and `WrongColumns` paths for error/dispatch — it never has to
reorder again.

### 6. Interaction with I.21

I.21 (scalar CTE guardrails) further constrains when the
`ScalarDummy` shape is allowed (e.g., disallowing a root scalar CTE
with a `WHERE` predicate that depends on the dummy key).  I.20's
helper recognises `ScalarDummy` but does not enforce I.21's
restrictions; it just classifies.  When both phases are in tree:

- I.20 declares the shape valid and routes to emit_child_ops's
  existing dummy-key branch.
- I.21 owns the additional rejection rules around that shape.

If I.21 lands first, I.20 must not regress its rejections.  If I.20
lands first, I.21 builds on the `ScalarDummy` classification and adds
its checks at the call sites.  Either order works; just keep
`ScalarDummy` as a shared classification across the two phases.

## Test plan

Extend `mysql-test/suite/ronsql/t/ronsql_cte_partial_key.test`.
The fixture should add one parent-side column that can match
`pairs.amt`, for example `cte_customer.c_amt BIGINT`, so full-key
lookup tests can bind both `(k, amt)` without inventing unrelated
tables.

### Positive tests

1. **Full-key CTE lookup with predicates in virtual PK order**

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, SUM(pairs.cnt) AS s
FROM cte_customer AS c
JOIN pairs ON pairs.k = c.c_id AND pairs.amt = c.c_amt
GROUP BY c.c_id;
```

Expected: matches MySQL and remains a `CTE_LOOKUP` child.  This is the
baseline for exact ordered key coverage.

2. **Full-key CTE lookup with predicates in reversed order
   (`ExactPermuted` reorder path)**

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, SUM(pairs.cnt) AS s
FROM cte_customer AS c
JOIN pairs ON pairs.amt = c.c_amt AND pairs.k = c.c_id
GROUP BY c.c_id;
```

Expected: matches MySQL.  The AST-level reorder in `load_join` swaps
the two `JoinCondition` entries so the planner builds keys in
virtual-PK order `(k, amt)` and `lookupCte()` is called correctly.

3. **Full-key CTE lookup with extra residual predicate on a CTE
   output column**

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, SUM(pairs.cnt) AS s
FROM cte_customer AS c
JOIN pairs ON pairs.k = c.c_id AND pairs.amt = c.c_amt
WHERE pairs.cnt > 0
GROUP BY c.c_id;
```

Expected: matches MySQL.  The `WHERE pairs.cnt > 0` clause flows
through the existing CTE_LOOKUP filter pushdown path
(`scope.join_where_ce[i]` at `RonSQLPreparer.cpp:7241-7254`); it is
not part of the helper's input.  This test guards that adding the
helper to the JOIN-condition path does not accidentally reroute
WHERE handling.

4. **Existing partial-key two-table rewrite still positive**

Keep the current I.16 Test 1 shape:

```sql
JOIN pairs ON pairs.k = c.c_id
```

Expected: still rewrites to `CTE_SCAN` root and matches MySQL.

5. **Existing N-table rewrite with CTE not in `joins[0]` still positive**

Keep the current I.16 Test 5 shape where `pairs` is later in the join
list but still joins to the original root alias `c`.

Expected: still rewrites and matches MySQL.

6. **Existing LEFT JOIN elsewhere in chain still positive**

Keep the current I.16 Test 4 shape: the multikey-CTE join is INNER,
while an unrelated real-table join is LEFT OUTER.

Expected: still rewrites and preserves the unrelated outer-join
semantics.

7. **Full predicate-count but duplicate key column → `Partial` rewrite**

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, SUM(pairs.cnt) AS s
FROM cte_customer AS c
JOIN pairs ON pairs.k = c.c_id AND pairs.k = c.c_region
GROUP BY c.c_id;
```

Expected: matches MySQL.  Coverage classifies this as `Partial`
(`pk_covered = [true, false]`, since only `k` is bound and `amt` is
missing).  The I.16c rewrite promotes `pairs` to root; the duplicate
`pairs.k = c.c_region` predicate becomes a normal join / filter
condition handled by the existing planner / interpreter path.
Tightening to column-level coverage subsumes the duplicate-key shape
under the existing partial-key rewrite, so this is a positive test,
not a rejection.

### Negative tests

8. **Full-count but wrong CTE column**

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, SUM(pairs.cnt) AS s
FROM cte_customer AS c
JOIN pairs ON pairs.k = c.c_id AND pairs.cnt = c.c_region
GROUP BY c.c_id;
```

Expected: clear RonSQL permanent error, not a silent `lookupCte()`
wrong-key execution.  Coverage classifies as `WrongColumns` because
`pairs.cnt` is a CTE output but not in the virtual PK.

9. **Partial-key CTE join whose parent alias is not the original
   root and no other valid match in the chain**

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, r.r_name, SUM(pairs.cnt) AS s
FROM cte_customer AS c
JOIN cte_region AS r ON r.r_id = c.c_region
JOIN pairs ON pairs.k = r.r_id
GROUP BY c.c_id, r.r_name;
```

Expected: clear RonSQL permanent error.  Per section 4, the rewrite
walker skips this match (`pairs` joins to `r`, not the original root
`c`); no later valid match exists in the chain, so no rewrite
happens; `emit_child_ops` then fires the column-level `Partial`
error from section 2.

9b. **Wrong-parent CTE join earlier, valid CTE join later — rewrite
    on the later match**

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, r.r_name, SUM(pairs.cnt) AS s
FROM cte_customer AS c
JOIN cte_region AS r ON r.r_id = c.c_region
JOIN pairs AS pa ON pa.k = r.r_id
JOIN pairs AS pb ON pb.k = c.c_id
GROUP BY c.c_id, r.r_name;
```

Expected: matches MySQL.  The walker skips `pa` (wrong parent),
finds `pb` (valid: parent alias is `c`, the captured original root),
applies the rewrite to `pb`.  This guards section 4's "continue past
wrong-parent matches" decision against regressions.

10. **LEFT JOIN on the multikey CTE itself remains rejected**

Keep the existing I.16 Test 2 shape:

```sql
LEFT JOIN pairs ON pairs.k = c.c_id
```

Expected: still rejects clearly; the root rewrite must not be applied
to the CTE join itself when it is LEFT OUTER.

11. **No-key join to multikey CTE remains rejected**

If the parser/planner allows an unconditional join form such as
`JOIN pairs ON 1 = 1`, add it as a negative test.

Expected: clear permanent error.  This is `Partial` coverage with zero
bound virtual-PK columns.

## Non-goals

- No NDB API changes.
- No true non-root `CTE_SCAN` child support.
- No general bushy join reordering.
- No change to virtual CTE key derivation.

## Completion criteria

- `CTE_LOOKUP` is never emitted unless the join keys match the virtual
  CTE primary key columns, not merely the key count.
- Reversed full-key ON predicate order is reordered locally and works.
- The I.16c rewrite runs only when the matched partial-key CTE join
  is attached to the original root alias; wrong-parent matches are
  skipped, not rejected, so a later valid match still works.
- I.17's `ScalarDummy` shape continues to emit through the existing
  dummy-key carve-out at `RonSQLPreparer.cpp:7372-7382`.
- New and existing `ronsql_cte_partial_key` MTR coverage passes.

## Implementation checklist

1. Add the `CteKeyCoverage` enum and `CteKeyCoverageResult` struct
   plus a helper `cte_key_coverage(const CteDefinition*, const
   LexCString* bound_cte_side_names, Uint32 num_keys,
   CteKeyCoverageResult& out)` in `RonSQLPreparer.cpp` (or the same
   header that already exposes the CTE-side helpers).
2. In `load_join()`, capture
   `m_context.ast_root.root_table->name` into a local before the
   I.16 rewrite scan.
3. Replace the count-only check in `RonSQLPreparer.cpp:1206-1240`:
   - call the helper for every INNER multi-key CTE join encountered,
   - skip matches whose conditions reference any non-original-root
     parent alias,
   - apply the rewrite to the first valid `Partial` match.
4. Immediately after the rewrite scan and before
   `QueryPlanner::plan()`, walk the joins list a second time.  For
   each CTE-targeted clause whose coverage is `ExactPermuted`,
   reorder its `JoinCondition` linked list into virtual-PK order
   using `pk_index_for_key[]`.  Skip clauses already promoted to
   root by step 3 — they have no `JoinCondition` left at the
   matched site.
5. In `emit_child_ops()` at `RonSQLPreparer.cpp:7343-7388`, replace
   the `op.num_key_cols == cte_pk_cols` count check with a helper
   call.  Dispatch:
   - `ExactOrdered` → existing `qb->lookupCte()` path.
   - `ScalarDummy` → existing carve-out at lines 7372-7382, no
     change.
   - `Partial` → emit the column-level permanent error (the rewrite
     in step 3 should have eliminated all valid `Partial`s; if one
     reaches here, the message names the missing PK column).
   - `WrongColumns` → permanent error naming the offending bound
     CTE-side column.
   - `ExactPermuted` should never reach this site because step 4
     normalised it; if seen, treat as a planner bug — `abort()` in
     debug, fall through to a permanent error in release.
6. Add the new `ndberror.cpp` entries for any new permanent error
   codes introduced in steps 3 and 5.
7. Extend `mysql-test/suite/ronsql/t/ronsql_cte_partial_key.test`
   and the matching result file with the test plan above.  Add
   `cte_customer.c_amt BIGINT` to the fixture.
8. Rebuild the touched binaries:

   ```bash
   cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
   ```

9. Run the focused MTR:

   ```bash
   cd debug_build/mysql-test
   ./mtr --suite=ronsql ronsql_cte_partial_key
   ```

10. Run the regression suites:

    ```bash
    ./mtr --suite=ronsql
    ./mtr --suite=ndb_push_agg
    ```

    Confirm zero regressions before committing.

11. Update the I.16 entry in
    `storage/ndb/claude_files/pushdown_join_aggregation/CLAUDE.md`
    to note that I.16a's count-only check was tightened to
    column-level coverage by I.20, and add an I.20 entry below it.
