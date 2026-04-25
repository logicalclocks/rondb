# CTE Filter Phase E.1 — scanCte as main-query root

## Context

Phases A through D2 wired RonSQL through every NDB-API CTE shape
*except* the two reachable only via `scanCte`. This phase lands the
first one: `SELECT … FROM <cte_name> …` as a main-query root, with or
without WHERE / GROUP BY / joins to real tables.

Today `RonSQLPreparer.cpp:4389-4393` throws
`"scanCte as main-query root not yet supported. Use EXPLAIN to see
the plan."` and bails before any emit happens. The audit confirms
that most groundwork has already landed in earlier phases:

- `JoinOp::CTE_SCAN` enum (`QueryPlanner.hpp:43`).
- Root-name → CTE detection in `QueryPlanner.cpp:86-127` — `findCte`
  runs before `dict->getTable`, and when matched produces
  `rootOp.type = CTE_SCAN`, `rootOp.table = NULL`,
  `rootOp.cte_def_idx`. **No planner change is needed.**
- `build_cte_virtual_tables` (RonSQLPreparer.cpp:4900) already
  iterates both CTE_LOOKUP and CTE_SCAN ops.
- Main-aggregator construction (RonSQLPreparer.cpp:4406-4416) already
  falls back to `cteVirtualTables[plan.agg_leaf_idx]` when the leaf
  table is NULL — comment explicitly mentions both CTE op kinds.
- `resolve_cte_output_columns` (RonSQLPreparer.cpp:2806-2847) already
  handles both CTE op kinds for ResultPrinter's column-type metadata.
- `emit_cte_lookup_filter` (RonSQLPreparer.cpp:5088) is reusable
  verbatim — `testCteNdbApiFilter::testCteScanFilterRoot` confirms
  the kernel accepts the same `branch_linked_*` opcode family on a
  scanCte interpreted-code program.

What's missing is the RonSQL-side emit dispatch. Zero `scanCte(`
calls exist anywhere in `RonSQLPreparer.cpp` today.

## Goal

Lift the guard and emit `qb->scanCte(...)` for CTE_SCAN main-query
roots. Cover four sub-shapes end-to-end:

1. Pass-through (`SELECT k, t FROM cte ORDER BY k`).
2. WHERE on root (filter via `emit_cte_lookup_filter` →
   `setInterpretedCode`).
3. Main-query aggregation over the CTE_SCAN root (`agg_leaf_idx ==
   0`, attach `singleAgg` via `setAggregation` on the root options).
4. CTE_SCAN root joined to real-table children (real-table lookup
   children with linked keys from the CTE_SCAN root's virt-table
   columns).

## Scope

In scope:
- Remove the existing root guard.
- Route CTE-root queries through `load_join` (which builds
  `m_main_scope.join_plan` via the planner) instead of
  `load_single_table` (which would call `m_dict->getTable(<cte_name>)`
  and throw "Failed to get table"). Done by widening the `is_join`
  check in `prepare()`.
- Extend `emit_root_op` to dispatch on CTE_SCAN at the top, before
  the existing PK / index / scan logic. Reuse
  `emit_cte_lookup_filter` for WHERE and `setAggregation` for the
  agg-leaf-at-root case.
- Sweep callers of `m_main_scope.table` for NULL safety on the
  CTE-root path.

Out of scope (deferred to later phases):
- Chained CTEs (CTE body whose root is itself a CTE_SCAN) —
  separate phase E.2.
- Reject-cleanly guards for CTE_SCAN as outer-join child — Phase G.
- WHERE shapes already rejected by `emit_cte_lookup_filter`
  (column-vs-column, expressions, OR, MIN/MAX over non-numeric
  source) inherit the existing rejections without change.

## Files

- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`:
  - `prepare()` (~line 797): widen `is_join` so CTE-root routes
    through `load_join`. Add a small static helper
    `root_is_cte_name(const TableRef&, const CteDefinition*)`
    using the existing `findCte` semantics.
  - Remove guard at lines 4387-4394.
  - `emit_root_op` (line 4784): change signature to take optional
    `NdbAggregator* singleAgg = nullptr` and `NdbDictionary::Table**
    cteVirtualTables = nullptr`. Add CTE_SCAN dispatch at top of
    function. Reuse `emit_cte_lookup_filter` for the WHERE conjuncts
    on `scope.join_where_ce[0]`.
  - Update main-query call site at line 4696 to pass `&singleAgg,
    cteVirtualTables`. The existing CTE-body call site at line
    4681 keeps the default-NULL params.
  - NULL-guard `unload_schema` (~line 5694) — `if
    (m_main_scope.table != NULL)` around the `invalidateTable`
    call.
- `storage/ndb/src/ronsql/RonSQLPreparer.hpp:307-308`: matching
  `emit_root_op` declaration.
- `mysql-test/suite/ronsql/t/ronsql_cte_scan.test` — new file. Same
  fixtures as `ronsql_cte_basic.test` (cte_orders / cte_customer /
  cte_lineitem / cte_regions). Use the `ronsql_compare.inc` harness.
- `mysql-test/suite/ronsql/r/ronsql_cte_scan.result` — recorded on
  first MTR run.

## Test plan

| Test | Shape | Notes |
|------|-------|-------|
| 1 | `SELECT k, t FROM sums ORDER BY k` | pass-through (no WHERE, no GB) |
| 2 | `SELECT k, t FROM sums WHERE t > 50` | inline-type filter on aggregate output |
| 3 | `SELECT k, t FROM sums WHERE k = 200` | mem-opcode filter on GB column |
| 4 | `SELECT s.k, s.t, c.c_name FROM sums s JOIN cte_customer c ON c.c_id = s.k` | CTE_SCAN root joined to real-table child |
| 5 | `SELECT SUM(t) AS gt FROM sums` | main-query aggregation over CTE_SCAN root, agg_leaf_idx == 0 |
| 6 | `SELECT k, SUM(t) FROM sums GROUP BY k` | main-query GB over CTE_SCAN root |

The kernel side is already validated by `testCteScanRootLargeResult`
/ `testCteScanRootSmallBatch` in `testCteNdbApiFilter.cpp`, so
Phase F's multi-batch concern is implicitly covered by Test 1 once
it runs against larger seeded data (or via a follow-on test).

## Risks

1. **NULL `m_main_scope.table` propagation.** With CTE-root taking
   the `load_join` path, `m_main_scope.table` stays NULL. Most
   readers are guarded already because they're on
   `load_single_table`-only paths, but `unload_schema` definitely
   needs a NULL check. Implementation step: grep for
   `m_main_scope.table` and patch any reader that runs on the
   CTE-root path.
2. **Ordering of `build_cte_virtual_tables` vs `emit_root_op`** —
   today the multi-op CTE-body branch (line 4680) builds virt
   tables AFTER calling `emit_root_op`. E.1's new dispatch needs the
   virt table at root-emit time. **Swap the order** in E.1 even
   though only E.2 ultimately exercises it inside CTE bodies — the
   change is mechanical and avoids re-touching the file.
3. **EXPLAIN output for CTE_SCAN root** —
   `RonSQLPreparer.cpp:7322` already prints `CTE_SCAN`. Spot-check
   on first run.
4. **ResultPrinter for pass-through aggregate outputs.**
   `resolve_cte_output_columns` populates `column_map` for COLUMN
   outputs (source real column) and leaves NULL for AGGREGATE
   outputs. ResultPrinter's existing fallback covers numeric
   aggregates; if a runtime gap surfaces, patch minimally.

## Verification

```
cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
cd debug_build/mysql-test && ./mtr --record --suite=ronsql ronsql_cte_scan
./mtr --suite=ronsql                   # full suite — no regressions
./mtr --suite=ndb_push_agg             # block tests — no regressions
```

After green, commit + push. Update Phase E status in
`ronsql_cte_plan.md`.
