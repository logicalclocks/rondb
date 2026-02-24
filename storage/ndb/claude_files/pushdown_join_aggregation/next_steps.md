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

**Additional completed work:**
- Scan-scan join aggregation support (DBSPJ dummy program + child scan)
- ERROR_INSERT 4040 for intermittent group eviction testing
- 10 TPC-H NDB API benchmarks (Q1, Q3, Q4, Q5, Q9, Q10, Q12, Q13, Q18, Q19)
- DBSPJ parallelism optimization: bypass DBTC/API round-trip for JoinAgg batches

### Test Coverage

| Test | Signal Path | What It Tests |
|------|-------------|---------------|
| testJoinAgg (18 tests) | Direct DBLQH | All agg types, GROUP BY, eviction, mutex-free, empty table, negative values, flow control, rowsExamined |
| testJoinAggSpj (7 tests) | DBTC→DBSPJ→DBLQH | Basic aggregation through full QueryTree, empty/single row, large dataset, many groups |
| testJoinAggScanScan | DBTC→DBSPJ→DBLQH | Scan-scan join aggregation |
| testJoinAggNdbApi (6+ tests) | NdbQueryBuilder API | SUM/GROUP BY, COUNT+SUM, multi-agg GROUP BY, 3-way join, wide type coverage |
| testCaseAgg | Direct DBLQH | CASE expression in aggregation |
| benchJoinAgg | Direct DBLQH (LQHKEYREQ) | Performance: pipelined lookups with linked attrs |
| bench_q12_tpch | Direct DBLQH (LQHKEYREQ) | TPC-H Q12 with CASE, CHAR comparison, date filters |
| bench_q12_dbtc | DBTC→DBSPJ→DBLQH | TPC-H Q12 through full orchestration with pushdown WHERE filter |
| bench_q9_dbtc | DBTC→DBSPJ→DBLQH | TPC-H Q9: 6-table join with multi-level linked attrs, composite keys |
| bench_q9_ndbapi | NDB API | TPC-H Q9 via NdbQueryBuilder |
| 10 TPC-H benchmarks | NDB API | Q1, Q3, Q4, Q5, Q9, Q10, Q12, Q13, Q18, Q19 |
| load_tpch | — | TPC-H data loader |

**MTR test suite:** `mysql-test/suite/ndb_push_agg/` wraps block_unit_tests
for automated regression testing (5 functional tests + 4 benchmarks).

---

**MySQL Handler — Phase 10: 3+ Way Joins:**
- Added leaf-table validation for SUM/MIN/MAX aggregate source columns in
  `ndb_build_aggregation_program()` — prevents silently loading wrong column
  from intermediate table when LoadColumn() operates on leaf table namespace
- Verified: no explicit 2-table restriction existed; code already supported
  multi-table GROUP BY via linked projections
- MTR test: `mysql-test/suite/ndb_push_agg/t/ndb_join_pushdown_agg.test`
  (11 SQL-level test cases for 3-way join aggregation)

---

## Next Steps

### 1. EXPLAIN Support (Priority: High)

Make EXPLAIN output show when aggregation has been pushed. Add a note like
"Using pushed join aggregation" to the Extra column, similar to how
"Using pushed join" is shown for pushed joins.

### 3. SQL-Level MTR Test Suite (Priority: High)

Write comprehensive MTR tests exercising pushdown aggregation through SQL.
The existing `ndb_push_agg` suite tests block-level/NDB API programs;
this suite tests the full MySQL→handler→NDB API→data node path.

**Test file:** `mysql-test/suite/ndb/t/ndb_join_pushdown_agg.test`

**Categories:**
1. Basic correctness: Each aggregate function with ON/OFF comparison
2. GROUP BY: single column, multi-column, multi-table
3. Types: INT, BIGINT, FLOAT, DOUBLE, CHAR, VARCHAR, DATE
4. Edge cases: empty tables, single row, NULL values, all-NULL groups
5. Post-aggregation: HAVING, ORDER BY, LIMIT, combined
6. Implicit aggregation (no GROUP BY)
7. Multi-way joins: 3-table, 4-table
8. Fallback: queries that can't be pushed produce correct MySQL results
9. EXPLAIN: verify pushed aggregation appears in output

### 4. More Integration Tests (Priority: Medium)

#### 4a. Eviction Through NDB API Path

Test that group overflow (eviction) works correctly through the NDB API.
When the hash table fills up at DBLQH, evicted groups are sent as
TRANSID_AI directly to the API receiver. Verify that:
- The API receives partial results during the scan (evicted groups)
- The final COMPLETE phase merges/sends remaining groups
- Total result is correct (evicted + final groups combined)

**Approach:** Use ERROR_INSERT 5090 (forces maxGroups=3) with
testJoinAggNdbApi. Create data with >3 distinct GROUP BY values.

#### 4b. Multi-Fragment / Multi-Node

Current tests run on a single data node. Test with:
- Multiple fragments of the root scan table
- Verify that SCAN_NEXTREQ batching works correctly
- Verify that each node's aggregation state is independent
- Verify COMPLETE merges results from all nodes

#### 4c. Abort / Error Paths

- API disconnect during scan (SCAN_TABREF)
- Node failure during aggregation (if testable with ERROR_INSERT)
- SETUP_REF error handling (e.g., out-of-memory at setup time)

### 5. Secondary Features (Priority: Low)

These are documented in coordinator_implementation.md sections 8–9
but not yet implemented. They improve observability and correctness
for large-scale queries but are not required for basic functionality.

#### 5a. Eviction Row Tracking

Track evicted group rows through the signal chain:
- LQHKEYCONF → DBSPJ: report per-operation eviction count
- DBSPJ accumulates in `m_agg_rows_sent_to_api` on Request
- SCAN_FRAGCONF → DBTC: report cumulative evictions
- DBTC uses for flow control (throttle SCAN_NEXTREQ if API backlogged)

#### 5b. 64-bit rowsExamined

For very large joins (millions of leaf rows), the 32-bit rowsExamined
counter may overflow. Extend SCAN_FRAGCONF with version-gated
SignalLength_v3 carrying the upper 32 bits.

#### 5c. Dynamic Memory in DBSPJ Request

coordinator_implementation.md Section 1 proposes replacing the fixed
`m_lookup_node_data[ABS_MAX_NDB_NODES]` array with dynamically
allocated memory to reduce per-Request size. Currently uses fixed
arrays (working but not memory-optimal).
