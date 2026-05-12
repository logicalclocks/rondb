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
roots. Cover three sub-shapes end-to-end:

1. WHERE on root (filter via `emit_cte_lookup_filter` →
   `setInterpretedCode`), both on aggregate output and on GB column.
2. Main-query aggregation over the CTE_SCAN root (`agg_leaf_idx ==
   0`, attach `singleAgg` via `setAggregation` on the root options).
3. CTE_SCAN root joined to real-table children (real-table lookup
   children with linked keys from the CTE_SCAN root's virt-table
   columns), with main-query aggregation.

**Pass-through (projection-only) main SELECTs are deferred to a
separate Phase E.3.** RonSQL today rejects any non-aggregating SELECT
with `"Not an aggregate query."` at `RonSQLPreparer.cpp:447-455` —
this applies regardless of CTE_SCAN root vs real-table root.
Lifting that restriction is an independent gap (would benefit
projection-only real-table SELECTs too) and not required for the
scanCte emit work. CTE bodies remain aggregate-only by design.

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

All tests aggregate in the main SELECT (per the deferral note above).

| Test | Shape | Notes |
|------|-------|-------|
| 1 | `SELECT k, SUM(t) FROM sums GROUP BY k` | main GB on CTE_SCAN root col + agg over another |
| 2 | `SELECT k, SUM(t) FROM sums WHERE t > 50 GROUP BY k` | WHERE on agg output (inline-type filter) |
| 3 | `SELECT k, SUM(t) FROM sums WHERE k = 200 GROUP BY k` | WHERE on GB column |
| 4 | `SELECT SUM(t) AS gt FROM sums` | main agg over CTE_SCAN root, no GB (single result row, agg_leaf_idx == 0) |

**Deferred — `SELECT s.k, SUM(s.t) FROM sums s JOIN cte_customer c ON
c.c_id = s.k GROUP BY s.k` (CTE_SCAN root + real-table child + main
aggregator on the child leaf).** This shape exposes a pre-existing
kernel gap: `Dbspj::appendFromParent` writes a junk schemaVersion
when the parent tree node is a CTE op (`m_primaryTableId == 0`),
producing `[tableId=0][schemaVersion=junk]` linked-attr entries that
fail the version check in `JoinAggInterpreter::initGBTypes` (error
1227). The fix is sequenced as Phase E.1K immediately after E.1
landing — see `cte_filter_phase_e1k.md`. The dropped test block is
preserved as commented-out reference in `ronsql_cte_scan.test` so
E.1K can restore it verbatim.

The kernel side for the four landed shapes is validated by
`testCteScanRootLargeResult` / `testCteScanRootSmallBatch` in
`testCteNdbApiFilter.cpp`. Phase F's multi-batch concern can fold in
via a 600-group seeded variant once Phase E.3 enables pass-through;
for now Tests 1–4 cover the emit path end-to-end on the standard
fixture.

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
