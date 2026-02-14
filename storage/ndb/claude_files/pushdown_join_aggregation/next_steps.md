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

### Test Coverage

| Test | Signal Path | What It Tests |
|------|-------------|---------------|
| testJoinAgg (18 tests) | Direct DBLQH | All agg types, GROUP BY, eviction, mutex-free, empty table, negative values, flow control, rowsExamined |
| testJoinAggSpj (7 tests) | DBTC→DBSPJ→DBLQH | Basic aggregation through full QueryTree, empty/single row, large dataset, many groups |
| testCaseAgg | Direct DBLQH | CASE expression in aggregation |
| benchJoinAgg | Direct DBLQH (LQHKEYREQ) | Performance: pipelined lookups with linked attrs |
| bench_q12_tpch | Direct DBLQH (LQHKEYREQ) | TPC-H Q12 with CASE, CHAR comparison, date filters |
| bench_q12_dbtc | DBTC→DBSPJ→DBLQH | TPC-H Q12 through full orchestration with pushdown WHERE filter |

---

## Next Steps

### 1. More Integration Tests (Priority: High)

The current testJoinAggSpj tests cover the happy path through
DBTC→DBSPJ→DBLQH but lack stress scenarios that exercise edge cases
in the coordinator layer.

#### 1a. Eviction Through Full Path

Test that group overflow (eviction) works correctly through DBTC/DBSPJ.
When the hash table fills up at DBLQH, evicted groups are sent as
TRANSID_AI directly to the API. Verify that:
- The API receives partial results during the scan (evicted groups)
- The final COMPLETE phase merges/sends remaining groups
- Total result is correct (evicted + final groups combined)

**Approach:** Use ERROR_INSERT 5090 (forces maxGroups=3) in
testJoinAggSpj or a new test. Create data with >3 distinct GROUP BY
values. Verify all groups appear in the API results.

#### 1b. Pushdown WHERE Filter Through DBSPJ

bench_q12_dbtc uses PI_ATTR_INTERPRET in QN_ScanFragParameters, but
testJoinAggSpj does not. Add a test that combines:
- WHERE filter on root scan table (PI_ATTR_INTERPRET)
- JOIN_AGG on leaf table
- Verify that only filtered rows participate in aggregation

#### 1c. Multi-Fragment / Multi-Node

Current tests run on a single data node. Test with:
- Multiple fragments of the root scan table
- Verify that SCAN_NEXTREQ batching works correctly
- Verify that each node's aggregation state is independent
- Verify COMPLETE merges results from all nodes

Requires a cluster with >=2 data nodes or a table with multiple
fragments on the same node.

#### 1d. Abort / Error Paths

- API disconnect during scan (SCAN_TABREF)
- Node failure during aggregation (if testable with ERROR_INSERT)
- SETUP_REF error handling (e.g., out-of-memory at setup time)

#### 1e. Large Result Sets

- Many groups (>1000) to stress hash table resizing
- Large batch sizes to verify SCAN_NEXTREQ flow control
- Zero matching rows (WHERE filter rejects everything)

### 2. NDB API Integration (Priority: Medium)

The NDB API layer needs changes so that real SQL queries (via MySQL
handler or NdbQueryBuilder) can trigger pushdown join aggregation
without manual signal construction.

#### 2a. NdbQueryBuilder Changes

NdbQueryBuilder constructs QueryTree for pushed joins. It needs to:
- Detect aggregation in the query plan
- Set NI_AGGREGATE / NI_AGGREGATE_LEAF / PI_ATTR_AGGREGATE bits
- Build aggregation program from the query's GROUP BY / aggregate
  functions
- Include GROUP BY columns in linked attributes of intermediate nodes
- Set PI_ATTR_INTERPRET for WHERE clause pushdown on root scan

**Key files:**
- `storage/ndb/src/ndbapi/NdbQueryBuilder.cpp`
- `storage/ndb/src/ndbapi/NdbQueryOperation.cpp`
- `storage/ndb/include/ndbapi/NdbQueryBuilder.hpp`

#### 2b. MySQL Handler Integration

The MySQL handler for NDB (`ha_ndbcluster`) decides whether to push
joins. It needs to:
- Recognize queries with GROUP BY + aggregate functions on pushed joins
- Generate the aggregation program via AggInterpreter's program builder
- Pass the program through NdbQueryBuilder

**Key files:**
- `storage/ndb/plugin/ha_ndbcluster_push.cpp`
- `storage/ndb/plugin/ha_ndbcluster.cpp`

#### 2c. MTR Tests

Once the API integration works, write MTR tests with SQL queries:
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
