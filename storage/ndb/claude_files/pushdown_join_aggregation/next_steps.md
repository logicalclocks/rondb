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

### Test Coverage

| Test | Signal Path | What It Tests |
|------|-------------|---------------|
| testJoinAgg (18 tests) | Direct DBLQH | All agg types, GROUP BY, eviction, mutex-free, empty table, negative values, flow control, rowsExamined |
| testJoinAggSpj (7 tests) | DBTC→DBSPJ→DBLQH | Basic aggregation through full QueryTree, empty/single row, large dataset, many groups |
| testJoinAggNdbApi (4 tests) | NdbQueryBuilder API | SUM/GROUP BY, COUNT+SUM, multi-agg GROUP BY, 3-way join with linked param filter |
| testCaseAgg | Direct DBLQH | CASE expression in aggregation |
| benchJoinAgg | Direct DBLQH (LQHKEYREQ) | Performance: pipelined lookups with linked attrs |
| bench_q12_tpch | Direct DBLQH (LQHKEYREQ) | TPC-H Q12 with CASE, CHAR comparison, date filters |
| bench_q12_dbtc | DBTC→DBSPJ→DBLQH | TPC-H Q12 through full orchestration with pushdown WHERE filter |
| bench_q9_dbtc | DBTC→DBSPJ→DBLQH | TPC-H Q9: 6-table join with multi-level linked attrs, composite keys |
| load_tpch | — | TPC-H data loader for bench_q9_dbtc |

---

## Next Steps

### 1. MySQL Handler Integration (Priority: High)

The MySQL handler for NDB (`ha_ndbcluster`) decides whether to push
joins. It needs to recognize queries with GROUP BY + aggregate functions
on pushed joins and route them through the NDB API aggregation path.

#### 1a. ha_ndbcluster_push.cpp

- Detect aggregation candidates in the query plan
- Generate aggregation program via NdbAggregator's program builder
- Pass program through NdbQueryOptions::setAggregation()
- Set up linked projections for GROUP BY columns from parent tables

**Key files:**
- `storage/ndb/plugin/ha_ndbcluster_push.cpp`
- `storage/ndb/plugin/ha_ndbcluster.cpp`

#### 1b. MTR Tests

Once handler integration works, write MTR tests with SQL queries:
```sql
-- Basic pushed aggregate
SELECT l_shipmode, COUNT(*), SUM(...)
FROM lineitem JOIN orders ON l_orderkey = o_orderkey
GROUP BY l_shipmode;

-- With WHERE filter
SELECT ... WHERE l_receiptdate >= '1994-01-01'
  AND l_receiptdate < '1995-01-01'
GROUP BY l_shipmode;

-- Edge cases: empty result, single group, HAVING clause
```

### 2. More Integration Tests (Priority: Medium)

#### 2a. Eviction Through NDB API Path

Test that group overflow (eviction) works correctly through the NDB API.
When the hash table fills up at DBLQH, evicted groups are sent as
TRANSID_AI directly to the API receiver. Verify that:
- The API receives partial results during the scan (evicted groups)
- The final COMPLETE phase merges/sends remaining groups
- Total result is correct (evicted + final groups combined)

**Approach:** Use ERROR_INSERT 5090 (forces maxGroups=3) with
testJoinAggNdbApi. Create data with >3 distinct GROUP BY values.

#### 2b. Multi-Fragment / Multi-Node

Current tests run on a single data node. Test with:
- Multiple fragments of the root scan table
- Verify that SCAN_NEXTREQ batching works correctly
- Verify that each node's aggregation state is independent
- Verify COMPLETE merges results from all nodes

#### 2c. Abort / Error Paths

- API disconnect during scan (SCAN_TABREF)
- Node failure during aggregation (if testable with ERROR_INSERT)
- SETUP_REF error handling (e.g., out-of-memory at setup time)

### 3. Secondary Features (Priority: Low)

These are documented in coordinator_implementation.md sections 8–9
but not yet implemented. They improve observability and correctness
for large-scale queries but are not required for basic functionality.

#### 3a. Eviction Row Tracking

Track evicted group rows through the signal chain:
- LQHKEYCONF → DBSPJ: report per-operation eviction count
- DBSPJ accumulates in `m_agg_rows_sent_to_api` on Request
- SCAN_FRAGCONF → DBTC: report cumulative evictions
- DBTC uses for flow control (throttle SCAN_NEXTREQ if API backlogged)

#### 3b. 64-bit rowsExamined

For very large joins (millions of leaf rows), the 32-bit rowsExamined
counter may overflow. Extend SCAN_FRAGCONF with version-gated
SignalLength_v3 carrying the upper 32 bits.

#### 3c. Dynamic Memory in DBSPJ Request

coordinator_implementation.md Section 1 proposes replacing the fixed
`m_lookup_node_data[ABS_MAX_NDB_NODES]` array with dynamically
allocated memory to reduce per-Request size. Currently uses fixed
arrays (working but not memory-optimal).
