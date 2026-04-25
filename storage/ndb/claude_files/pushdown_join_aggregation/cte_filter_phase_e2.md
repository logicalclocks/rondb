# CTE Filter Phase E.2 — Chained CTEs (CTE-of-CTE)

## Context

Phase E.1 lands `scanCte` as a main-query root. This phase covers
the second NDB-API CTE shape that requires `scanCte` emit on the
RonSQL side: chained CTEs, where one CTE's body references another.

```sql
WITH a AS (SELECT g, SUM(v) AS s FROM t GROUP BY g),
     b AS (SELECT g, MAX(s) AS m FROM a GROUP BY g)
SELECT g, m FROM b;
```

Today `b`'s body has `cp.ops[0].type == CTE_SCAN` and `cp.ops[0].
table == NULL`. The single-op CTE-body emit branch at
`RonSQLPreparer.cpp:4634-4679` immediately reads `srcTab =
cp.ops[0].table` and trips `require_run(srcTab != NULL, "CTE body
root has no physical table.")` at line 4641.

The planner already produces the right plan for chained CTE bodies:
`build_cte_scopes` calls `QueryPlanner::plan(cte->stmt->root_table,
…, visible_head)` per CTE, where `visible_head` is the
CTE-list-so-far. The same root-resolution code at
`QueryPlanner.cpp:86-127` runs and produces `CTE_SCAN` when the CTE
body's FROM names a predecessor CTE. **No planner change is needed.**

## Goal

Lift the CTE-body restriction so a chained CTE body emits `scanCte`
for its root op (and optionally `scanCte` as a join child for the
rare `WITH a, b AS (SELECT … FROM real_tab JOIN a)` shape). All four
sub-shapes ship green:

1. Basic chain (`b` reads `a`, both with GB).
2. Chain with WHERE on the intermediate.
3. Chain feeding a real-table join in the outer SELECT.
4. (Optional) chained CTE used as a join-child scan.

## Scope

In scope:
- Restructure the single-op CTE-body emit path at
  `RonSQLPreparer.cpp:4634-4679` to dispatch on `cp.ops[0].type`:
  TABLE_SCAN keeps the existing self-join inline emit; CTE_SCAN
  emits `scanCte(predecessor_cte_idx, …)` against the predecessor's
  virt table, with optional WHERE filter (reusing
  `emit_cte_lookup_filter`) and the body's aggregator on the root.
- Move `build_cte_virtual_tables` before the aggregator-anchor
  selection in the per-CTE loop, so a chained body whose
  `agg_leaf_idx == 0` and root is CTE_SCAN can anchor its
  `NdbAggregator` on the predecessor's virt-table.
- `emit_child_ops`: add `case JoinOp::CTE_SCAN` to handle the rare
  shape where a CTE body has `FROM real_tab JOIN <cte>` and the
  planner picks CTE_SCAN as a child. NDB-API semantics: scanCte
  takes a virtTab + options, no `keys` operand list.
  - Reject cleanly when the child carries linked keys
    (`num_key_cols > 0`) — typical join-child shapes need
    linked-key support that scanCte doesn't have.
- `defineCte` `srcTab` argument at line 4686: for chained bodies
  whose root is CTE_SCAN, pass the predecessor's virt table
  (`cteChildVT[0]`) rather than `cs.table` (NULL).

Out of scope:
- CTE_SCAN as outer-join child — Phase G reject-cleanly guard.
- Mutual recursion / forward references — already rejected by
  `analyze_ctes`.

## Files

- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`:
  - Per-CTE loop (~line 4615-4691): build `cteChildVT[]` before the
    aggregator-anchor selection (currently runs after the dispatch
    decisions). Anchor `cteAgg` on `cteChildVT[cp.agg_leaf_idx]`
    when `cp.ops[cp.agg_leaf_idx].table == NULL`.
  - Single-op CTE-body branch (lines 4634-4679): split into three
    cases — TABLE_SCAN (existing), CTE_SCAN (new), else (existing
    multi-op fall-through).
  - `defineCte` srcTab at line 4686: dispatch on
    `cp.ops[0].type == CTE_SCAN` to select `cteChildVT[0]`.
  - `emit_child_ops` (line 5370-5549): add `case JoinOp::CTE_SCAN`
    with `num_key_cols == 0` precondition.
- `mysql-test/suite/ronsql/t/ronsql_cte_scan.test`: append Tests
  7-9.
- `mysql-test/suite/ronsql/r/ronsql_cte_scan.result`: re-record.

## Test plan

| Test | Shape | Notes |
|------|-------|-------|
| 7 | `WITH a AS (SELECT … GB), b AS (SELECT k, MAX(s) FROM a GB k) SELECT … FROM b` | basic chain, GB on each layer |
| 8 | `WITH a AS (…), b AS (SELECT k, s FROM a WHERE s > 50) SELECT … FROM b` | WHERE on intermediate, no GB on `b` |
| 9 | chain feeding a real-table join in the outer SELECT | end-to-end mix |

## Risks

1. **Aggregator anchor for chained bodies.** Today line 4621-4625
   builds `cteAgg(cte_leaf_table)` where `cte_leaf_table =
   cp.ops[cp.agg_leaf_idx].table`. For a chained body that's NULL.
   Mirror the main-aggregator's NULL-fallback (build virt tables
   first, then anchor on `cteChildVT[agg_leaf_idx]`). The challenge
   is that `cteChildVT` is currently allocated INSIDE the per-CTE
   block after the aggregator construction — needs reordering.
2. **`defineCte` srcTab semantics.** The NDB API expects a table
   descriptor that matches the CTE's output schema. For chained
   CTEs, the predecessor's virt table is the right descriptor —
   verify via `testCteNdbApi.cpp` chained-CTE patterns
   (`testCrossJoinTwoScalarCtes`, etc.). If a different shape is
   required, fall back to building the local body's virt table
   for that purpose.
3. **CTE_SCAN as join child with linked keys.** Rare shape; reject
   cleanly with a clear message rather than emitting incorrect
   code.

## Verification

```
cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
cd debug_build/mysql-test && ./mtr --record --suite=ronsql ronsql_cte_scan
./mtr --suite=ronsql                   # full suite — no regressions
./mtr --suite=ndb_push_agg             # block tests — no regressions
```

After green, commit + push. Update Phase E status in
`ronsql_cte_plan.md`. Memory file `project_cte_branch_state.md`
updated to drop Phase E from open follow-ups.
