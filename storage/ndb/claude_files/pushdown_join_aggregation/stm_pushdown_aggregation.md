# Single-Table Pushdown Aggregation (STM) — Plan

## Overview

Single-table aggregate queries (e.g., TPC-H Q1 on `lineitem`) cannot use pushdown
today because the RONDB-733 framework requires a pushed join (SPJ path).
`ndb_push_aggregation()` checks `member_of_pushed_join() != nullptr` for all tables,
which is always nullptr for single-table scans.

However, single-table pushdown aggregation **already works end-to-end at the NDB API
level** — RonSQL uses `NdbScanOperation::setAggregationCode()` + `DoAggregation()`
for exactly this purpose. The NDB kernel (DBLQH/DBTUP AggInterpreter) handles
aggregation for plain scans identically to the SPJ path.

The work is therefore concentrated in the MySQL handler layer: detecting single-table
aggregate queries, building an NdbAggregator program, attaching it to the
NdbScanOperation, and feeding results back through the existing Item_sum pushed value
mechanism.

### Signal Flow Comparison

**Current join aggregation (SPJ path):**
```
MySQL → NdbQueryBuilder → DBTC → DBSPJ → JOIN_AGG_SETUP_REQ → DblqhProxy
  → AggInterpreter (per LDM thread) → JOIN_AGG_COMPLETE → merged results
  → TRANSID_AI → NdbQuery::getAggregator() → MySQL
```

**Single-table aggregation (NdbScanOperation path):**
```
MySQL → NdbScanOperation::setAggregationCode() → SCAN_TABREQ → DBTC
  → SCAN_FRAGREQ → DBLQH → AggInterpreter (per fragment)
  → TRANSID_AI → NdbScanOperation::DoAggregation() merges per-fragment results
  → NdbAggregator → MySQL
```

Key differences:
- No DBSPJ involvement — DBTC sends SCAN_FRAGREQ directly to DBLQH
- No JOIN_AGG_SETUP/COMPLETE/RELEASE signals — aggregation code embedded in SCAN_FRAGREQ
- Result merging happens API-side (NdbScanOperation::PrepareResults), not kernel-side
- No linked projections needed — all GROUP BY and aggregate columns are on the same table
- WHERE filter pushdown integrates naturally via `ha_ndbcluster_cond` (NdbScanFilter)

### What Already Exists

| Component | Status |
|-----------|--------|
| AggInterpreter (DBLQH/DBTUP kernel) | Complete — handles both SPJ and scan paths |
| NdbScanOperation::setAggregationCode() | Complete — used by RonSQL |
| NdbScanOperation::DoAggregation() | Complete — API-side merge of per-fragment results |
| NdbAggregator (program build + result iteration) | Complete |
| Item_sum pushed value mechanism | Complete (from join agg: set_pushed_value_int/double/null) |
| AccessPath surgery (AGGREGATE node removal) | Complete (from join agg) |
| store_group_column() type handling | Complete (all NDB types) |
| ndb_fetch_next_aggregate_row() result iteration | Complete |
| Feature gate (ndb_join_pushdown_aggregate THDVAR) | Complete |

### What's Missing

1. **Detection**: `ndbcluster_push_to_engine()` only checks for pushable aggregation
   after `make_pushed_join()` succeeds. Single-table queries have no pushed join.
2. **Program build for scan path**: Need to attach NdbAggregator to NdbScanOperation
   instead of NdbQueryOptions.
3. **Scan execution with aggregation**: `full_table_scan()` / index scan needs to
   call `setAggregationCode()` + `DoAggregation()` instead of normal `nextResult()`.
4. **Result flow**: Need to fetch results from NdbScanOperation's aggregator and
   feed them through the same Item_sum pushed value path.
5. **AccessPath surgery for single-table**: The AGGREGATE node removal logic assumes
   a pushed join. Need to handle the single-table case.

---

## Implementation Plan

### Phase 1: Feature Gate and Detection

**Goal:** Detect single-table aggregate queries that can be pushed in
`ndbcluster_push_to_engine()`, independently of join pushdown.

**Changes:**

1. **New THDVAR** (`ha_ndbcluster.cc`): Add `ndb_pushdown_aggregate` session variable
   (separate from `ndb_join_pushdown_aggregate`), default OFF.
   ```cpp
   static MYSQL_THDVAR_BOOL(pushdown_aggregate,
       PLUGIN_VAR_OPCMDARG,
       "Enable pushing down of aggregation for single-table queries to datanodes",
       nullptr, nullptr, false);
   ```
   Register in `system_variables[]` array.

2. **Detection function** (`ha_ndbcluster_push_agg.cc`): Add
   `ndb_push_single_table_aggregation()` that:
   - Checks the feature gate
   - Identifies single-table queries (exactly one NDB table in the query)
   - Validates aggregation pushability using the same logic as
     `ndb_can_push_aggregation()` but without the pushed-join requirement:
     - Has aggregate functions (COUNT/SUM/MIN/MAX, no DISTINCT/ROLLUP)
     - GROUP BY columns are simple field references from the single table
     - Aggregate arguments are field references from the single table
   - Returns true if pushable

3. **Hook in `ndbcluster_push_to_engine()`** (`ha_ndbcluster.cc`): After the
   existing join pushdown block, add single-table aggregation detection:
   ```cpp
   // After join pushdown block:
   if (!has_pushed_aggregation && THDVAR(thd, pushdown_aggregate)) {
     has_pushed_aggregation = ndb_push_single_table_aggregation(
         thd, join, pushed_builder);
   }
   ```

**Key design decision:** Reuse `ndb_can_push_aggregation()` logic but extract
the aggregate validation into a shared helper that works with or without a
pushed join context. The `member_of_pushed_join()` check is only needed for the
join path — for single-table, all columns are guaranteed to be on the same table.

**Verification:**
- `SET ndb_pushdown_aggregate=ON` accepted
- Debug logging shows single-table aggregate queries detected
- Existing MTR tests pass unchanged

### Phase 2: NdbAggregator Program Build for Single Table

**Goal:** Build an NdbAggregator program from the MySQL query plan for a single-table
aggregate query. Store it on the handler for use during scan execution.

**Changes:**

1. **Program builder** (`ha_ndbcluster_push_agg.cc`): Add
   `ndb_build_single_table_aggregation_program()`:
   - Simplified version of `ndb_build_aggregation_program()` — no linked projections
   - All GROUP BY columns use `agg.GroupBy(col_id)` directly (single table)
   - All aggregate columns use `agg.LoadColumn(col_id, reg)` directly
   - COUNT(*): `agg.LoadUint64(1, kReg1) + agg.Count(agg_id, kReg1)`
   - COUNT(column): `agg.LoadColumn(col_id, kReg1) + agg.Count(agg_id, kReg1)`
   - SUM/MIN/MAX: `agg.LoadColumn(col_id, kReg1) + agg.Sum/Min/Max(agg_id, kReg1)`

2. **Handler state** (`ha_ndbcluster.h`): Add members for single-table aggregation:
   ```cpp
   NdbAggregator *m_stm_aggregator{nullptr};     // Owned, heap-allocated
   bool m_stm_agg_mode{false};                   // In single-table agg mode
   bool m_stm_agg_results_initialized{false};     // Results drained
   const JOIN *m_stm_agg_join{nullptr};           // For accessing sum_funcs
   ```
   Clean up `m_stm_aggregator` in destructor/reset.

3. **Wire up in detection** (`ha_ndbcluster_push_agg.cc`): When
   `ndb_push_single_table_aggregation()` detects a pushable query:
   - Build the NdbAggregator program
   - Store on the root handler via `m_stm_aggregator`
   - Set `m_stm_agg_mode = true`, `m_stm_agg_join = join`
   - Return true to trigger AccessPath surgery

**Verification:**
- Debug logging shows NdbAggregator program built
- Program contains correct GROUP BY and aggregate instructions
- Existing tests pass

### Phase 3: AccessPath Surgery for Single Table

**Goal:** When single-table aggregation is pushed, remove the AGGREGATE AccessPath
node so MySQL doesn't re-aggregate results that are already aggregated.

**Changes:**

1. **Extend `fixup_pushed_access_paths()`** (`ha_ndbcluster.cc`): The existing
   AGGREGATE case already handles `has_pushed_aggregation`. For single-table
   queries, the AccessPath tree is simpler:
   ```
   Before surgery:
   AGGREGATE (or TEMPTABLE_AGGREGATE)
     └── TABLE_SCAN (or INDEX_SCAN)

   After surgery:
   TABLE_SCAN (or INDEX_SCAN)
   ```

   The `strip_pushed_child_nljs()` call is unnecessary for single-table (no NLJs),
   but harmless — it returns the child unchanged when there are no NLJs.

   The existing code should work as-is if `has_pushed_aggregation` is set to true.
   Verify and adjust if the single-table AccessPath structure differs.

2. **Ensure handler `has_pushed_aggregation()` works**: The existing virtual method
   checks `m_pushed_agg_join != nullptr`. For single-table, we use
   `m_stm_agg_join` instead. Add:
   ```cpp
   bool ha_ndbcluster::has_pushed_aggregation() const {
     return m_pushed_agg_join != nullptr || m_stm_agg_join != nullptr;
   }
   ```

**Verification:**
- EXPLAIN shows AGGREGATE node removed for single-table aggregate query
- Existing join aggregation tests still pass

### Phase 4: Scan Execution with Aggregation

**Goal:** Attach the NdbAggregator to the NdbScanOperation during scan execution
and drain results via `DoAggregation()`.

This is the core phase. There are two possible scan entry points:
- `full_table_scan()` — for full table scans
- `full_index_scan()` / `ordered_index_scan()` — for index scans

**Changes:**

1. **Attach aggregation to scan** (`ha_ndbcluster.cc`): In `full_table_scan()`,
   after the NdbScanOperation is created and configured but before `execute()`:
   ```cpp
   if (m_stm_agg_mode && m_stm_aggregator != nullptr) {
     op->setAggregationCode(m_stm_aggregator);
   }
   ```
   Similarly for index scan paths if applicable.

2. **Override result fetching**: When in STM aggregate mode, `next_result()`
   should not iterate rows normally. Instead:
   - On first call: call `op->DoAggregation()` to drain the scan and merge
     per-fragment results
   - Then iterate NdbAggregator results via `FetchResultRecord()`

   The cleanest approach is to intercept in `rnd_next()`:
   ```cpp
   int ha_ndbcluster::rnd_next(uchar *buf) {
     if (m_stm_agg_mode) {
       return ndb_fetch_stm_aggregate(this);
     }
     // ... existing code ...
   }
   ```

3. **Aggregate result fetch** (`ha_ndbcluster_push_agg.cc`): Add
   `ndb_fetch_stm_aggregate()`:
   ```cpp
   int ndb_fetch_stm_aggregate(ha_ndbcluster *handler) {
     NdbScanOperation *scan_op = handler->m_active_cursor;
     NdbAggregator *agg = handler->m_stm_aggregator;

     if (!handler->m_stm_agg_results_initialized) {
       // Drain scan and merge per-fragment results
       scan_op->DoAggregation();
       handler->m_stm_agg_results_initialized = true;
     }

     // Reuse existing ndb_fetch_next_aggregate_row() logic
     return ndb_fetch_next_aggregate_row(agg, handler->m_stm_agg_join);
   }
   ```

   This reuses `ndb_fetch_next_aggregate_row()` (already implemented for join agg)
   which populates GROUP BY columns via `store_group_column()` and sets Item_sum
   pushed values.

**Key consideration:** `DoAggregation()` is a blocking call that drains the entire
scan. This is correct behavior for aggregation — we must process all rows before
producing any aggregate results. For large tables, this may take significant time.
The existing RonSQL path uses the same approach.

**Verification:**
```sql
SET ndb_pushdown_aggregate=ON;
-- Basic single-table aggregation
SELECT COUNT(*) FROM t1;
SELECT col1, SUM(col2) FROM t1 GROUP BY col1;
SELECT col1, COUNT(*), MIN(col2), MAX(col2) FROM t1 GROUP BY col1;
-- Compare ON vs OFF results
```

### Phase 5: Condition Pushdown Integration

**Goal:** Verify that WHERE clause condition pushdown works correctly with
single-table aggregation pushdown.

The existing `ha_ndbcluster_cond` infrastructure compiles WHERE clauses into
NdbScanFilter programs that are attached to the NdbScanOperation. This happens
before scan execution in `full_table_scan()` / index scan setup.

**Expected behavior:** Condition pushdown and aggregation pushdown are independent
mechanisms that compose naturally:
- The NdbScanFilter (WHERE clause) goes into the NDB interpreter program
- The NdbAggregator (aggregation) goes into a separate section
- DBLQH first applies the WHERE filter, then routes qualifying rows through
  AggInterpreter

**Changes:** Likely none — verify that `setAggregationCode()` and the existing
condition pushdown via `NdbScanFilter` / `setInterpretedCode()` don't conflict.

**Verification:**
```sql
SET ndb_pushdown_aggregate=ON;
-- WHERE with aggregation
SELECT col1, SUM(col2) FROM t1 WHERE col3 > 100 GROUP BY col1;
-- WHERE with date function (TPC-H Q1 pattern)
SELECT l_returnflag, SUM(l_quantity) FROM lineitem
WHERE l_shipdate <= '1998-09-01' GROUP BY l_returnflag;
```

### Phase 6: Index Scan Aggregation

**Goal:** Support aggregation pushdown for index scans (ordered and unordered).

Single-table queries often use index scans when there's a WHERE clause with
an index-compatible predicate (range scan) or when the optimizer chooses an
index scan for other reasons.

**Changes:**

1. **Attach aggregation in index scan paths**: Similar to Phase 4, add
   `setAggregationCode()` call in:
   - `full_index_scan()` (unordered index scan)
   - `ordered_index_scan()` (ordered index scan with bounds)

2. **Override result fetching for index scans**: The `index_next()` path needs
   the same aggregation intercept as `rnd_next()`:
   ```cpp
   int ha_ndbcluster::index_next(uchar *buf) {
     if (m_stm_agg_mode) {
       return ndb_fetch_stm_aggregate(this);
     }
     // ... existing code ...
   }
   ```

**Verification:**
```sql
-- Index range scan with aggregation
SELECT indexed_col, SUM(val) FROM t1
WHERE indexed_col BETWEEN 10 AND 100 GROUP BY indexed_col;
```

### Phase 7: EXPLAIN Support

**Goal:** Show "Using pushed aggregation" in EXPLAIN output for single-table
aggregate queries.

**Changes:**

1. Add `ET_PUSHED_AGGREGATION` Extra_tag (if not reusing the existing
   `ET_PUSHED_JOIN_AGGREGATION`). Or reuse the existing one with updated text.

2. Return the appropriate tag from `ha_ndbcluster::info()` when
   `m_stm_agg_mode` is true.

**Verification:**
```sql
EXPLAIN SELECT col1, SUM(col2) FROM t1 GROUP BY col1;
-- Should show "Using pushed aggregation" in Extra column
```

### Phase 8: HAVING, ORDER BY, LIMIT

**Goal:** Verify post-aggregation processing works correctly. Same approach as
join aggregation Phase 6 — MySQL handles these above the (now-removed) AGGREGATE
node using the pushed Item_sum values.

**Expected to work automatically:**
- HAVING: FILTER node evaluates `Item_sum::val_int()` → returns pushed values
- ORDER BY: SORT node reads GROUP BY columns from row buffer + Item_sum values
- LIMIT: LIMIT_OFFSET node stops reading after N rows

**Verification:**
```sql
SET ndb_pushdown_aggregate=ON;
SELECT col1, SUM(col2) AS total FROM t1 GROUP BY col1 HAVING total > 100;
SELECT col1, SUM(col2) FROM t1 GROUP BY col1 ORDER BY col1;
SELECT col1, SUM(col2) AS total FROM t1 GROUP BY col1 ORDER BY total DESC;
SELECT col1, SUM(col2) FROM t1 GROUP BY col1 LIMIT 5;
SELECT col1, SUM(col2) AS total FROM t1
  GROUP BY col1 HAVING total > 50 ORDER BY total DESC LIMIT 3;
```

### Phase 9: Implicit Aggregation (No GROUP BY)

**Goal:** Support queries with aggregate functions but no GROUP BY clause
(e.g., `SELECT COUNT(*) FROM t1`). These produce a single result row.

**Changes:** The NdbAggregator handles this naturally — when no GroupBy()
instructions are present, it produces a single result group. Verify it works
with the single-table scan path.

**Verification:**
```sql
SELECT COUNT(*) FROM t1;
SELECT COUNT(*), SUM(col2), MIN(col2), MAX(col2) FROM t1;
SELECT COUNT(*) FROM t1 WHERE col1 > 100;
-- Empty table
SELECT COUNT(*), SUM(col2) FROM empty_table;  -- Should return 0, NULL
```

### Phase 10: MTR Test Suite

**Goal:** Comprehensive MTR test coverage.

**Test file:** `mysql-test/suite/ndb_push_agg/t/ndb_pushdown_agg.test`

**Test categories:**
1. Basic: Each aggregate function (COUNT, SUM, MIN, MAX) with ON/OFF comparison
2. GROUP BY: Single column, multi-column
3. Types: INT, BIGINT, FLOAT, DOUBLE, CHAR, VARCHAR, DATE, DATETIME
4. WHERE integration: Simple predicates, range scans, date comparisons
5. Edge cases: Empty tables, single row, NULL values, all-NULL groups
6. Post-aggregation: HAVING, ORDER BY, LIMIT, combined
7. Implicit aggregation: No GROUP BY
8. Index scans: Range scan with aggregation
9. Fallback: Unsupported queries (DISTINCT, ROLLUP, AVG) produce correct results
   via MySQL fallback
10. EXPLAIN: Verify annotation

All tests should use `--sorted_result` or explicit `ORDER BY` to avoid
non-deterministic hash iteration order.

---

## Future Extensions (Not in Scope)

These are documented in `next_steps.md` and apply to both join and single-table
aggregation:

- **AVG support**: Rewrite AVG(x) as SUM(x) + COUNT(x) at handler level
- **DECIMAL precision**: Native DECIMAL arithmetic in NdbAggregator
- **Arithmetic expressions in aggregates**: TPC-H Q1 `SUM(l_extendedprice * (1 - l_discount))`
- **Expression-based GROUP BY**: `EXTRACT(YEAR FROM date_col)`
- **CASE in aggregates**: `SUM(CASE WHEN ... THEN ... END)`

---

## File Change Summary

| Phase | New/Modified Files | Est. Lines |
|-------|-------------------|------------|
| 1 | MOD: `ha_ndbcluster.cc`, `ha_ndbcluster_push_agg.h/.cc` | ~60 |
| 2 | MOD: `ha_ndbcluster_push_agg.cc`, `ha_ndbcluster.h` | ~80 |
| 3 | MOD: `ha_ndbcluster.cc`, `ha_ndbcluster_push_agg.cc` | ~20 |
| 4 | MOD: `ha_ndbcluster.cc`, `ha_ndbcluster_push_agg.h/.cc` | ~60 |
| 5 | Verification only (likely no code changes) | ~0 |
| 6 | MOD: `ha_ndbcluster.cc` | ~20 |
| 7 | MOD: `ha_ndbcluster.cc` or handler info method | ~10 |
| 8 | Verification only | ~0 |
| 9 | Verification only | ~0 |
| 10 | NEW: MTR test file | ~200 |

**Total estimated new code: ~250 lines** (excluding tests).

Most of the infrastructure is already in place from the join aggregation work.
The main new code is the detection logic, the NdbAggregator program build
(simpler than the join version — no linked projections), and the scan-level
aggregation attachment.

---

## Key Design Decisions

1. **Separate THDVAR**: `ndb_pushdown_aggregate` (single-table) is independent
   from `ndb_join_pushdown_aggregate` (join). This allows enabling them
   independently and testing each path in isolation.

2. **Reuse existing result flow**: `ndb_fetch_next_aggregate_row()` and
   `store_group_column()` are shared between join and single-table paths.
   Only the scan execution differs (NdbScanOperation vs NdbQueryOperation).

3. **NdbScanOperation::DoAggregation()**: Uses the existing RonSQL-proven API
   path. No kernel changes needed — the aggregation already works at the
   SCAN_FRAGREQ/AggInterpreter level.

4. **Intercept at rnd_next/index_next**: Clean interception point that avoids
   modifying the complex scan state machine. When in STM agg mode, the entire
   scan draining + result iteration is handled by the aggregation path.

5. **No SPJ involvement**: Single-table aggregation uses the direct
   DBTC→DBLQH path, which is simpler and has lower overhead than the
   DBTC→DBSPJ→DBLQH path used for joins.
