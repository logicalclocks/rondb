# Pushdown Join Aggregation — Next Steps

## Current Status (February 2026)

### Completed

**Local database (DBLQH/DBTUP) — Phases 1–6:**
- AggInterpreter: chunk allocator, group-by hash map, all aggregate types
  (COUNT, SUM, MAX, MIN), CASE expressions via embedded interpreter
- JoinAggregationState: shared state with mutex / mutex-free strategies
- Signal handlers: JOIN_AGG_SETUP/COMPLETE/RELEASE in DblqhProxy
- handleJoinAggRow interception in DbtupExecQuery
- Group eviction with TRANSID_AI to API when hash table full
- rowsExamined in SCAN_FRAGCONF (version-gated)
- ERROR_INSERT 5090 for forced eviction testing

**Coordinator (DBTC/DBSPJ) — Phase 7:**
- DBTC: JOIN_AGG_SETUP_REQ/CONF handling, aggStateKeys section building,
  SCAN_TABREQ with JoinAgg flag, SCAN_NEXTREQ/SCAN_TABCONF flow
- DBSPJ: aggStateKeys extraction from SCAN_FRAGREQ, JoinAggFlag on
  LQHKEYREQ, T_AGGREGATE_LEAF/T_EXPECT_TRANSID_AI suppression,
  FLUSH_AI suppression for intermediate nodes, parseDA() for
  NI_AGGREGATE/NI_AGGREGATE_LEAF/PI_ATTR_AGGREGATE
- QueryTree protocol: DABits for aggregation, linked attribute
  pass-through of GROUP BY columns
- Table metadata (tableId, schemaVersion) prepended to linked attr entries
  for type-aware column resolution
- Eviction row tracking through signal chain:
  handleJoinAggRow → LQHKEYCONF readLen → DBSPJ m_rows →
  SCAN_FRAGCONF completedOps → DBTC SCAN_TABCONF m_ops → API
- DBSPJ bypass optimization: skip SCAN_FRAGCONF→DBTC→API round-trip
  when m_rows == 0 (no evictions), with SCAN_HBREP heartbeats
- Dynamic memory: Request arrays (m_lookup_node_data, m_aggStateKeys)
  dynamically allocated via lc_ndbd_pool_malloc sized to MAX_NDB_NODES

**NDB API Integration — Phase 8 (Steps 1–10, all complete):**
- NdbQueryOptions: `setAggregation()`, `addLinkedProjection()` API
- NdbQueryOperationDefImpl: `m_isAggregateLeaf` flag
- NdbQueryDefImpl: `m_hasAggregation`, aggregate leaf tracking
- Serialization: NI_AGGREGATE / NI_AGGREGATE_LEAF DABits
- NdbQueryImpl: `m_aggReceivers[]`, `m_aggProgram`, `m_aggResultData`
- doSend(): combined Section 2 (boundsLen + aggReceiverId + aggProgram),
  JoinAgg flag, explicit scanParallelism (DATA 15)
- DBTC Section 2 header parsing: split bounds / receiverId / aggProgram
- NDB_AGG_RECEIVER type in Ndbif.cpp for TRANSID_AI dispatch
- Result handling: accumulate-then-process via `processAggResults()`
- Public API: `NdbQuery::getAggregator()` → `FetchResultRecord()`

**MySQL Handler Integration — Phases 1–9 (all complete):**
- Phase 1: Feature gate (`ndb_join_pushdown_aggregate` THDVAR, default OFF),
  `ha_ndbcluster_push_agg.h/.cc` stubs, CMakeLists wiring
- Phase 2: Aggregation candidate detection in `ndb_can_push_aggregation()`:
  feature gate, full-join-push check, aggregate func validation
  (COUNT/SUM/MIN/MAX, no DISTINCT/ROLLUP), GROUP BY column validation
- Phase 3: NdbAggregator program build from MySQL query plan — maps
  Item_sum/Item_field to NdbAggregator instructions, attaches to ndb_pushed_join
- Phase 4: AccessPath surgery (remove AGGREGATE/TEMPTABLE_AGGREGATE node)
  and Item_sum pushed value support (`set_pushed_value_int/double/null`
  in item_sum.h/cc)
- Phase 5: Runtime aggregate result flow — `ndb_fetch_next_aggregate()`
  iterates NdbAggregator results, populates GROUP BY columns + Item_sum values
- Phase 6: HAVING, ORDER BY, LIMIT verified working with pushed values
- Phase 7: Implicit aggregation (no GROUP BY) — single result row
- Phase 8: Extended GROUP BY column type support to all NDB types
  (FLOAT, DOUBLE, CHAR, VARCHAR, DATE, DATETIME, TIMESTAMP, etc.)
- Phase 9: Multi-table GROUP BY via linked projections — GROUP BY columns
  from non-root tables passed through SPJ linked attributes

**MySQL Handler — Phase 10: 3+ Way Joins:**
- Added leaf-table validation for SUM/MIN/MAX aggregate source columns in
  `ndb_build_aggregation_program()` — prevents silently loading wrong column
  from intermediate table when LoadColumn() operates on leaf table namespace
- Verified: no explicit 2-table restriction existed; code already supported
  multi-table GROUP BY via linked projections
- MTR test: `mysql-test/suite/ndb_push_agg/t/ndb_join_pushdown_agg.test`
  (11 SQL-level test cases for 3-way join aggregation)

**EXPLAIN Support — Phase 11** (commit 72aace778d1):
- Added `ET_PUSHED_JOIN_AGGREGATION` to Extra_tag enum and format arrays
- Added `has_pushed_aggregation()` virtual method to handler interface
- Implemented in ha_ndbcluster using `m_pushed_agg_join` pointer
- Clear `m_pushed_agg_join` in `reset()` to avoid stale state across queries
- Shows "Using pushed join aggregation" in EXPLAIN Extra on root table
- MTR EXPLAIN tests verify annotation appears ON and disappears OFF

**SQL-Level MTR Test Suite — Phase 12** (commit aff382f93e1):
- 20 new test cases (14-33) in `ndb_join_pushdown_agg.test` covering:
  2-way joins, single row/group, empty results, COUNT(column) vs COUNT(*)
  with NULLs, HAVING+ORDER BY+LIMIT, multiple aggregates on same column,
  type coverage (INT/BIGINT/FLOAT/DOUBLE/DATE/DATETIME/CHAR/VARCHAR),
  GROUP BY on all types, MIN/MAX on strings/dates, all-NULL columns, EXPLAIN
- Fix: reject COUNT(nullable_column) pushdown in `ndb_can_push_aggregation()`
  since data node Count instruction does not skip NULL values
  (Note: this restriction is lifted in Phase 14 — Count() actually does skip NULLs)

**ERROR_INSERT Eviction Testing — Phase 13** (commit 0a88b35840e):
- ERROR_INSERT 5090 (force maxGroups=3) and 4040 (intermittent eviction)
  for testing group eviction through the full signal chain

**Additional completed work:**
- Scan-scan join aggregation support (DBSPJ dummy program + child scan)
- 10 TPC-H NDB API benchmarks (Q1, Q3, Q4, Q5, Q9, Q10, Q12, Q13, Q18, Q19)

### Integration Test Coverage

**NDB API tests** (testJoinAggNdbApi.cpp, Tests 1–22):

| Test | What It Tests |
|------|---------------|
| 1–13 | SUM/GROUP BY, COUNT+SUM, multi-agg, 3-way join, type coverage |
| 14–17 | Eviction via ERROR_INSERT 5090/4040: 2-table, all agg funcs, 3-way, dual pressure |
| 18–19 | Multi-fragment (2000 rows, 16 fragments), eviction during SCAN_NEXTREQ batching |
| 20 | SETUP_REF via ERROR_INSERT 5091: graceful failure, RELEASE, recovery |
| 21 | Early close: execute then close immediately, SCAN_CLOSE → RELEASE path |
| 22 | COMPLETE_REF via ERROR_INSERT 5092: scan runs, COMPLETE fails, cleanup |

**Block-level tests** (testJoinAgg):

| Test | What It Tests |
|------|---------------|
| testJoinAgg (18 tests) | All agg types, GROUP BY, eviction, mutex-free, empty table, negative values, flow control, rowsExamined |
| testJoinAggSpj (7 tests) | Full QueryTree through DBTC→DBSPJ→DBLQH |
| testJoinAggScanScan | Scan-scan join aggregation |
| testCaseAgg | CASE expression in aggregation |
| Benchmarks | benchJoinAgg, bench_q12_tpch, bench_q12_dbtc, bench_q9_dbtc, bench_q9_ndbapi, 10 TPC-H |

**MTR test suites:**
- `mysql-test/suite/ndb_push_agg/` — block_unit_tests wrapper (5 functional + 4 benchmarks)
- `mysql-test/suite/ndb_push_agg/t/ndb_join_pushdown_agg.test` — 33 SQL-level test cases
- `mysql-test/suite/ndb_push_agg_dist/` — 2-node distributed tests (8 categories)

---

## Remaining Work

### Phase 14: COUNT(column) Pushdown & Outer Join Restriction (Priority: High)

**COUNT(column) for nullable columns:**
The Phase 12 fix rejected COUNT(nullable_column), but the data node's
`Count()` function (AggInterpreter.cpp:1365-1368) already checks `a.is_null`
and skips NULL values without incrementing. The fix is to:
1. Remove the nullable rejection in `ndb_can_push_aggregation()`
   (ha_ndbcluster_push_agg.cc:98-102)
2. Differentiate COUNT(*) vs COUNT(column) in `ndb_build_aggregation_program()`
   — COUNT(*) keeps `LoadUint64(1)`, COUNT(column) uses `LoadColumn(col_id)`
   so the register carries NULL info to Count()
3. Same leaf-table restriction as SUM/MIN/MAX (LoadColumn namespace)

**Outer/anti/semi join restriction:**
No explicit rejection exists today. Aggregation with outer joins would produce
incorrect results because:
- Aggregation runs in DBLQH on the leaf table via `handleJoinAggRow`
- For outer joins, when the inner-side table has no match, DBSPJ produces a
  NULL-extended row at the coordinator level
- DBLQH never processes this non-existent row, so COUNT(*) misses it and
  groups that exist only due to unmatched outer rows are entirely missing
- Add rejection in `ndb_push_aggregation()` using `isOuterJoined()`,
  `isAntiJoined()`, `isSemiJoined()` from pushed_table (ha_ndbcluster_push.h:405-416)

**MTR tests:** COUNT(nullable_column) push+skip-NULLs, LEFT JOIN EXPLAIN rejection.

### Future: Outer Join Aggregation Support (Priority: Low)

To support aggregation with outer joins, DBSPJ would need to participate in
the aggregation when it produces NULL-extended rows for unmatched outer joins:
- When LQHKEYREF arrives for an outer-joined lookup, DBSPJ must inject a
  NULL-extended row into the aggregation engine (either at DBSPJ level or by
  forwarding to DBLQH with a special "null row" marker)
- Requires architectural changes to the aggregation protocol
- Blocked on the outer join restriction (Phase 14) being implemented first

### 5b. 64-bit rowsExamined (Priority: Low)

For very large joins (millions of leaf rows), the 32-bit rowsExamined
counter may overflow. Extend SCAN_FRAGCONF with version-gated
SignalLength_v3 carrying the upper 32 bits.
