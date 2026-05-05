# Phase I.9 — `scanIndex` inside a CTE materialisation subtree

## What testCteNdbApi.cpp Test 18 demonstrates

```
WITH cte0 AS (SELECT grp, SUM(val) AS total
                FROM cte_src GROUP BY grp)
SELECT * FROM cte0;
```

Tree shape (from the test):

```
Node 0: CTE 0 subtree container
Node 1: scanIndex(cte_src, idx_cte_src_val)   ← the new bit
Node 2: readTuple(cte_src, key=linked(N1,"pk"),
                  setAggregation(cte0Agg))     ← T_AGGREGATE_LEAF
Node 3: scanCte(0)                              ← MAIN ROOT
```

Test 18 calls `qb->scanIndex(idx, srcTab, /*bound=*/nullptr)` — a
full unordered scan via an ordered index, no bounds, no ordering.
Functionally equivalent to `scanTable` for materialisation; the
test only proves the kernel + NDB API accept `scanIndex` as the
materialisation root inside `beginCteSubtree…endCteSubtree`.

## What's missing on the RonSQL side

Two independent gaps stand between RonSQL and Test 18:

### A. Planner never picks `INDEX_SCAN` for a CTE body's root

`build_cte_scopes` (`RonSQLPreparer.cpp:3239`) calls
`QueryPlanner::plan` per CTE body but does NOT run the
scan-config scoring (`m_scan_config_candidates`).  For each CTE
body, the planner decides:

- Real-table root → `JoinOp::TABLE_SCAN`.
- Predecessor-CTE root → `JoinOp::CTE_SCAN`.
- Join-children → `JoinOp::INDEX_SCAN` already happens via
  `findOrderedIndex` in `QueryPlanner.cpp:257`, but only for
  non-root operations driven by linked keys.

There is no SQL shape today that produces `JoinOp::INDEX_SCAN`
for the *root* of a CTE body.

### B. Single-op CTE-body emit is hardcoded to `scanTable`

`execute_join` (`RonSQLPreparer.cpp:5119+`):

```cpp
if (cp.num_ops == 1 && cp.ops[0].type == JoinOp::TABLE_SCAN) {
  ...
  cteOpDefs[0] = qb->scanTable(srcTab, &rootOpts);
  ...
  cteOpDefs[1] = qb->readTuple(srcTab, linked_pk, leafOpts);
}
```

The branch checks `JoinOp::TABLE_SCAN` exactly.  An
`INDEX_SCAN` body's root would fall through to the multi-op
`else` arm (`emit_root_op + emit_child_ops`), which:

- Builds `qb->scanIndex(idx, srcTab, &bound, &opts)` correctly
  for the root, but
- Doesn't synthesize the `readTuple(linked_pk)` agg-leaf — that
  pattern is the CTE-materialisation contract and is unique to
  the single-op self-join branch.

So even if the planner picked `INDEX_SCAN` today, the emit path
wouldn't produce the materialisation pattern.

## Scope decision

Test 18's specific shape (full index scan, no bounds, no order) has
no natural SQL trigger.  The first SQL shapes that would benefit
from `scanIndex` inside a CTE body are:

1. **WHERE with range bounds on an indexed column** in a CTE body.
   Currently planned as `TABLE_SCAN + WHERE filter`; could be
   `INDEX_SCAN + bounds`.  This is Phase I.9's concrete SQL-facing
   scope.
2. **ORDER BY DESC + LIMIT 1 over a scalar CTE** — the MIN/MAX
   optimisation tracked as Phase I.10.  Requires `scanIndex` with
   descending order + small batch size.

Both shapes need the same root `INDEX_SCAN` materialisation emit path,
but they should not be landed as one large optimisation.  Phase I.9
should first add the general CTE-body root scan-config infrastructure:
per-CTE bound selection, residual filter tracking, and the single-op
`INDEX_SCAN` materialisation branch.  Phase I.10 can then build the
MIN/MAX-via-index rewrite on top of that infrastructure.

If an MTR-only "verify the kernel doesn't regress on CTE-body
`scanIndex` materialisation" probe is wanted now (e.g. to keep CI
honest while I.10 is being designed), it has to be a block test
(`testCteNdbApi.cpp:5022 testScanIndexCteMaterialization` already
covers this) rather than a RonSQL MTR test.

## Phase I.9 implementation plan

Phase I.9 is responsible for the CTE-local scan-config state.  This is
not optional and must not reuse `RonSQLPreparer`'s main-query globals
directly:

```cpp
DynamicArray<ConditionalExpression*> m_toplevel_conditions;
DynamicArray<ScanConfig> m_scan_config_candidates;
ScanConfig* m_scan_config;
```

Those fields belong to the main query's root scan planning.  Reusing
them while planning CTE bodies would corrupt or confuse main-scope
state, and would not work for multiple CTEs.  I.9 must introduce a
local/per-scope equivalent.

### 1. Add per-CTE / per-QueryScope scan config

Extend `QueryScope` or add a side table indexed by CTE definition with
the data needed for CTE-body root scans:

- A local list of simplified top-level WHERE conditions.
- A local `ScanConfig` candidate list.
- The chosen scan config.
- A residual WHERE expression or residual condition list containing
  only predicates not consumed as index bounds.

The existing main-scope helper logic can be refactored into reusable
functions, but the state must be passed in explicitly.  A suitable
shape is:

```cpp
collect_toplevel_conditions(ce, out_conditions);
generate_scan_config_candidates(scope, out_conditions, out_candidates);
choose_scan_config(out_candidates);
build_residual_where(out_conditions, chosen.condition_handling_map);
```

The existing member-based functions can then remain thin wrappers for
the main query.

### Planner change (`build_cte_scopes` or `QueryPlanner::plan`)

Add a hook for the CTE-body root: when `cte->stmt->where_expression`
contains range bounds on an indexed column AND the source table has
a matching ordered index, set `cp.ops[0].type = INDEX_SCAN` and
`cp.ops[0].index = idx`.  Mirrors the existing
`findOrderedIndex` use at line 257 but applied to the body's root
instead of join children.

The cleanest place is a small helper invoked from `build_cte_scopes`
after `QueryPlanner::plan` returns and after column resolution is
available for the CTE scope.  The helper should produce both the
chosen scan config and the residual filter.  `classify_where_by_table`
must receive the residual WHERE, not the original full WHERE, otherwise
bound predicates will also be installed as interpreted filters.

### Emit change (`RonSQLPreparer.cpp` ~line 5119)

Add a parallel branch:

```cpp
} else if (cp.num_ops == 1 && cp.ops[0].type == JoinOp::INDEX_SCAN) {
  // Single-table CTE body via ordered index — same self-join
  // pattern as TABLE_SCAN, but use scanIndex(idx, srcTab, bound)
  // for the root.  Bounds are derived from cs.join_where_ce[0]
  // by an existing helper (or built inline using the same
  // scan-config bound construction the main scope uses).
  const NdbDictionary::Table* srcTab = cp.ops[0].table;
  const NdbDictionary::Index* idx = cp.ops[0].index;
  NdbQueryOptions rootOpts;
  NdbInterpretedCode rootCode(srcTab);
  // (1) Apply only the residual WHERE as interpreted-code filter.
  // (2) Build NdbQueryIndexBound from the chosen CTE-local scan config.
  cteOpDefs[0] = qb->scanIndex(idx, srcTab, &bound, &rootOpts);
  // ... readTuple(linked_pk) leaf identical to the TABLE_SCAN branch.
}
```

The TABLE_SCAN branch must continue to apply the full CTE-body WHERE as
a filter when no index config is chosen.  The INDEX_SCAN branch applies
only the residual predicates not consumed as bounds.

### MTR test

A CTE body with `WHERE indexed_col BETWEEN x AND y GROUP BY ...`
where `indexed_col` is an ordered index.  Compares the result
against the same query through mysql.

### Risks

1. The CTE-body-root WHERE distribution must split the condition into
   `(bounds, residual_filter)`.  Code already exists in
   `RonSQLPreparer.cpp:2112+` for main-scope scoring, but the state
   needs to be made local instead of reusing `m_scan_config`.
2. CTE-body planning runs during `build_cte_scopes`, before the
   main scope's scan-config scoring.  Putting CTE-body bounds work
   in the same pre-emit pipeline keeps everything in one place.

## Recommendation

**Ship Phase I.9 before Phase I.10.**  I.9 should be the infrastructure
phase that adds CTE-local scan-config state plus the single-op
`INDEX_SCAN` materialisation emit branch.  It should have a normal
RonSQL MTR with a CTE-body indexed range predicate, so the feature is
not just dead code.

When we get to I.10, the implementation order will be:
1. Reuse I.9's CTE-local scan-config state and root `INDEX_SCAN`
   materialisation branch.
2. Planner: detect scalar CTE `MIN(index_col)` / `MAX(index_col)` where
   the aggregate argument exactly matches the first column of an ordered
   index, there is no `GROUP BY`, and residual predicates can be applied
   safely before aggregation.
3. Wire ASC / DESC direction and LIMIT / maxRows detection through to the
   `scanIndex` ordering / batch-size options.
4. MTR: scalar CTE `MAX(indexed_col) FROM ...` and `MIN(indexed_col)
   FROM ...` shapes.
