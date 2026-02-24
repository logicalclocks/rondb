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

### Phase 15: Single-Table Aggregation Pushdown (Priority: High)

Single-table aggregate queries (e.g., TPC-H Q1 on lineitem) cannot use pushdown
today because the RONDB-733 framework requires a pushed join (SPJ path).
`ndb_push_aggregation()` checks `member_of_pushed_join() != nullptr` for all
tables (ha_ndbcluster_push_agg.cc:321-326), which is always nullptr for
single-table scans.

**What's needed:**

1. **Single-table aggregation path in ha_ndbcluster**
   - New code path that attaches an NdbAggregator program to a plain
     SCAN_FRAGREQ without SPJ, or route single-table scans through SPJ
     as a degenerate 1-table "join"
   - Detection: single-table query with pushable aggregates, no join required

2. **AVG support**
   - `ndb_can_push_aggregation()` rejects AVG (falls through to default
     return false at ha_ndbcluster_push_agg.cc:121)
   - NdbAggregator has no native AVG — only COUNT, SUM, MIN, MAX
   - Rewrite AVG(x) as SUM(x) + COUNT(x) at the MySQL handler level
   - Compute AVG = SUM/COUNT when fetching results in
     `ndb_fetch_next_aggregate_row()`
   - Benefits both join and single-table paths

3. **DECIMAL precision**
   - RonSQL test (ronsql_dbt3_1_2.test) shows DECIMAL→double precision loss:
     MySQL `26777.986560` vs RonSQL `26777.986559999998`
   - Root cause: NdbAggregator converts DECIMAL to double internally
     (noted in test line 81: "remove convert-to-double shortcut")
   - Need native DECIMAL arithmetic in NdbAggregator

4. **Arithmetic expressions in aggregate arguments**
   - TPC-H Q1: `SUM(l_extendedprice * (1 - l_discount))`
   - `ndb_can_push_aggregation()` requires arguments to be simple
     Item::FIELD_ITEM (ha_ndbcluster_push_agg.cc:112)
   - Need to compile arithmetic expressions into NdbAggregator program,
     or evaluate via NDB interpreted program before feeding to aggregator

5. **WHERE with date functions (verification)**
   - TPC-H Q1: `WHERE l_shipDATE <= date_sub('1998-12-01', interval '90' day)`
   - For single-table path, the WHERE filter compiles into the scan's
     interpreted program (existing ha_ndbcluster_cond infrastructure)
   - Verify condition pushdown integrates correctly with aggregation path

**Target query (TPC-H Q1):**
```sql
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity), SUM(l_extendedprice),
       SUM(l_extendedprice * (1 - l_discount)),
       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
       AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount),
       COUNT(*)
FROM lineitem
WHERE l_shipdate <= date_sub('1998-12-01', interval '90' day)
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

### Phase 16: Correlated Subquery Support for Aggregation (Priority: Medium)

TPC-H Q2 (Minimum Cost Supplier) has a correlated scalar subquery with
`MIN(ps_supplycost)` inside a 4-table join, correlated on `p_partkey` from the
outer 5-table join. This pattern is common in TPC-H (Q2, Q4, Q17, Q20, Q22).

**What's needed:**

1. **Correlated subquery decorrelation**
   - NDB SPJ has no subquery support — the MySQL optimizer must decorrelate
     the subquery into a derived table or semi-join before pushdown is possible
   - Verify what MySQL's optimizer produces for Q2 and whether the resulting
     plan is a flat join that SPJ can handle

2. **Scalar subquery result as join predicate**
   - `ps_supplycost = (SELECT MIN(...))` compares a column to a subquery result
   - If decorrelated into a derived table, this becomes an equi-join on
     (p_partkey, min_cost), which SPJ can handle
   - The derived table itself (4-table join + MIN aggregate) is pushable
     with existing aggregation support

3. **Large join tree support**
   - Q2 outer query is 5 tables; with decorrelated subquery it could be 9+ tables
   - NDB SPJ supports up to MAX_NDB_NODES tables in a pushed join, but
     MySQL's pushed join builder may have practical limits
   - Verify SPJ topology constraints for deep/wide join trees

**Target query (TPC-H Q2):**
```sql
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr,
       s_address, s_phone, s_comment
FROM part, supplier, partsupp, nation, region
WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey
  AND p_size = 15 AND p_type LIKE '%BRASS'
  AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
  AND r_name = 'EUROPE'
  AND ps_supplycost = (
    SELECT MIN(ps_supplycost)
    FROM partsupp, supplier, nation, region
    WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey
      AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
      AND r_name = 'EUROPE')
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100;
```

### Phase 17: Semi-Join Aggregation & Related Features (Priority: Medium)

TPC-H Q4 (Order Priority Checking) uses EXISTS with a correlated subquery,
which MySQL converts to a semi-join. This requires aggregation pushdown to
work with semi-join semantics in SPJ.

**What's needed:**

1. **Semi-join with aggregation pushdown**
   - MySQL converts `EXISTS (SELECT ... WHERE l_orderkey = o_orderkey ...)`
     to a semi-join with first-match semantics
   - NDB SPJ already has `NI_FIRST_MATCH` (QueryTree.hpp) for semi-joins
   - Verify that aggregation pushdown works when a join node uses
     first-match/semi-join — the aggregation runs on the leaf table,
     but semi-join may change which node is the leaf or how rows flow
   - Phase 16 covers correlated scalar subqueries (Q2); this covers
     EXISTS/semi-join which is a different optimizer transformation

2. **Cross-column comparison in pushed filters**
   - Q4 subquery: `l_commitdate < l_receiptdate` (column vs column)
   - NDB interpreter supports column-vs-column comparison
   - For RonSQL: verify cross-column comparison support in WHERE clauses

3. **DATE_ADD() support (RonSQL)**
   - Q4: `o_orderdate < date_add('1993-07-01', interval '3' month)`
   - For NDB: MySQL evaluates constant date expressions before pushdown,
     so ha_ndbcluster_cond receives a resolved date constant — no issue
   - For RonSQL: DATE_ADD() with interval MONTH/YEAR needs verification
     alongside existing DATE_SUB() support (Phase 15.5)

4. **BETWEEN support (RonSQL)**
   - Q6: `l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01`
   - For NDB: MySQL rewrites BETWEEN as two comparisons before pushdown —
     no issue
   - For RonSQL: verify BETWEEN is supported in the parser/grammar

**Target query (TPC-H Q4):**
```sql
SELECT o_orderpriority, COUNT(*) AS order_count
FROM orders
WHERE o_orderdate >= '1993-07-01'
  AND o_orderdate < date_add('1993-07-01', interval '3' month)
  AND EXISTS (
    SELECT * FROM lineitem
    WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
GROUP BY o_orderpriority
ORDER BY o_orderpriority;
```

### Phase 18: Expressions in GROUP BY, Derived Tables, Cross-Table OR (Priority: Medium)

TPC-H Q7 (Volume Shipping) is a 6-table join with a derived table and
aggregation on expressions. Introduces several new requirements.

**What's needed:**

1. **Expression in GROUP BY (EXTRACT)**
   - Q7: `EXTRACT(YEAR FROM l_shipdate) AS l_year` used in GROUP BY
   - `ndb_can_push_aggregation()` requires GROUP BY items to be simple
     `Item::FIELD_ITEM` (ha_ndbcluster_push_agg.cc:130)
   - EXTRACT produces `Item_extract`, not a field reference — rejected today
   - Need to compile expression-based GROUP BY keys into the NdbAggregator
     program, or evaluate via NDB interpreted code
   - For RonSQL: verify EXTRACT() function support

2. **Derived table (inline view) merging with aggregation**
   - Q7 aggregates over a derived table `shipping` wrapping a 6-table join
   - If MySQL merges the derived table, it becomes a flat 6-table join with
     aggregation — potentially pushable via existing SPJ path
   - If MySQL materializes it, aggregation runs on a temporary table — not
     pushable
   - Need to verify MySQL optimizer behavior and whether the merged form
     is visible to the NDB push path

3. **Cross-table OR predicate**
   - Q7: `(n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE') OR
          (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')`
   - OR conditions spanning different tables cannot be pushed as a single
     NDB scan filter — MySQL must evaluate post-join or SPJ needs to handle it

4. **Self-join with aggregation**
   - Q7: `nation n1, nation n2` — same NDB table with different aliases
   - SPJ supports self-joins but verify correctness with aggregation pushdown
     since both operations reference the same tableId/schemaVersion

5. **6-table join aggregation at MySQL handler level**
   - bench_q9_dbtc tests 6-table join aggregation at NDB API level
   - MySQL handler integration for 6+ table pushed aggregation needs testing

**Target query (TPC-H Q7):**
```sql
SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue
FROM (
  SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation,
         EXTRACT(YEAR FROM l_shipdate) AS l_year,
         l_extendedprice * (1 - l_discount) AS volume
  FROM supplier, lineitem, orders, customer, nation n1, nation n2
  WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey
    AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey
    AND c_nationkey = n2.n_nationkey
    AND ((n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
      OR (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY'))
    AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year;
```

### Future: Outer Join Aggregation Support (Priority: Low)

To support aggregation with outer joins, DBSPJ would need to participate in
the aggregation when it produces NULL-extended rows for unmatched outer joins:
- When LQHKEYREF arrives for an outer-joined lookup, DBSPJ must inject a
  NULL-extended row into the aggregation engine (either at DBSPJ level or by
  forwarding to DBLQH with a special "null row" marker)
- Requires architectural changes to the aggregation protocol
- Blocked on the outer join restriction (Phase 14) being implemented first

### Test Suite Fixes: Non-Deterministic Result Order (Priority: High)

Existing MTR test cases for pushdown aggregation may produce results in
non-deterministic order because NDB's group-by hash map does not guarantee
output order. When aggregation is pushed, results come back in hash iteration
order rather than MySQL's usual sort-based grouping order.

**Fix:** Add `--sorted_result` directives or explicit `ORDER BY` clauses to
affected test cases in:
- `mysql-test/suite/ndb_push_agg/t/ndb_join_pushdown_agg.test`
- Any other MTR tests that compare aggregate pushdown ON vs OFF results

Without this fix, test results are flaky — passing or failing depending on
hash table layout which varies across runs and platforms.

### 5b. 64-bit rowsExamined (Priority: Low)

For very large joins (millions of leaf rows), the 32-bit rowsExamined
counter may overflow. Extend SCAN_FRAGCONF with version-gated
SignalLength_v3 carrying the upper 32 bits.
