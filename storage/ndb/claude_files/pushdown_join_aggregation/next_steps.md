# Pushdown Join Aggregation — Next Steps

## Current Status (March 2026)

### Completed

**Local database (DBLQH/DBTUP) — Phases 1–6:**
- AggInterpreter: chunk allocator, group-by hash map, all aggregate types
  (COUNT, SUM, MAX, MIN), CASE expressions via embedded interpreter
- JoinAggregationState: shared state with mutex / mutex-free strategies
- Signal handlers: JOIN_AGG_SETUP/COMPLETE/RELEASE in DblqhProxy
- handleJoinAggRow interception in DbtupExecQuery
- Group eviction with TRANSID_AI to API when hash table full
- rowsExamined in SCAN_FRAGCONF (version-gated)
- ERROR_INSERT 5116 for forced eviction testing

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

**MySQL Handler: Join Aggregation — Phases 1–13 (all complete):**
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
- Phase 10: 3+ way joins — leaf-table validation for aggregate source columns
- Phase 11: EXPLAIN — "Using pushed join aggregation" in Extra column
- Phase 12: SQL-level MTR test suite — 33 test cases
- Phase 13: ERROR_INSERT eviction testing (5090, 4040)

**Phase 14: COUNT(column) Pushdown & Outer Join Restriction** (commit cdf07458e75):
- COUNT(nullable_column): removed incorrect rejection; data node Count()
  already skips NULLs. COUNT(*) uses LoadUint64(1), COUNT(column) uses
  LoadColumn(col_id) so NULL info reaches Count()
- Outer/anti/semi join rejection: aggregation pushdown now explicitly
  rejected when any pushed table uses outer/anti/semi join semantics
- MTR tests: COUNT nullable column, LEFT JOIN EXPLAIN rejection

**Single-Table Aggregation Pushdown (STM) — Phases 1–10 (all complete):**
- Phase 1: `ndb_pushdown_aggregate` THDVAR (separate from join agg),
  `ndb_push_single_table_aggregation()` detection
- Phase 2: `ndb_build_stm_aggregation_program()`, `ndb_start_stm_aggregate_scan()`,
  `ndb_fetch_stm_aggregate()` — full scan path via NdbScanOperation
- Phase 3: AccessPath surgery for single-table (reuses join agg infrastructure)
- Phase 4: Scan execution with SO_AGGREGATION in ScanOptions for NdbRecord scans,
  DoAggregation() for API-side per-fragment result merge
- Phase 5: WHERE pushdown integration verified (NdbScanFilter + aggregation compose)
- Phase 6: Index scan aggregation — SO_AGGREGATION in ordered_index_scan(),
  aggregation intercept in index_next()
- Phase 7: EXPLAIN — `ET_PUSHED_AGGREGATION` ("Using pushed aggregation")
- Phase 8: HAVING, ORDER BY, LIMIT verified working
- Phase 9: Implicit aggregation (no GROUP BY) verified working
- Phase 10: MTR test suite — 31 test cases in `ndb_pushdown_agg.test`
- Bug fixes: NdbRecord scan support (getValue_NdbRecord_scan), NextResult
  translation to HA_ERR_END_OF_FILE, FILTER child crash in fixup, default
  MRR fallback when m_stm_aggregator is set, read_range_next() intercept
- Bug fix: FLOAT/DOUBLE MIN/MAX — Item_sum_hybrid::reset_field() now checks
  hybrid_type == REAL_RESULT and uses result_field->store(double)
- Bug fix: WHERE not pushed — fixup_pushed_access_paths() TEMPTABLE_AGGREGATE
  case now re-walks children so FILTER nodes are processed
- Bug fix: Reject PK/unique key lookups (single-row access never executes scan path)

**CASE Expressions in Aggregation Pushdown** (commits a08fee2a695, d1c5983f18e):
- Searched CASE: `SUM(CASE WHEN col OP const THEN val ELSE val END)`
- Simple CASE: `SUM(CASE col WHEN val1 THEN v1 WHEN val2 THEN v2 ELSE ve END)`
- Multi-WHEN CASE with up to 32 WHEN/THEN pairs
- String column comparisons (CHAR EQ/NE) in CASE conditions
- Works in both join and single-table aggregation paths

**ERROR_INSERT Eviction Testing — Phase 13** (commit 0a88b35840e):
- ERROR_INSERT 5116 (force maxGroups=3) and 4040 (intermittent eviction)
  for testing group eviction through the full signal chain
**Arithmetic Expressions in Aggregation** (commit d1c5983f18e):
- Arithmetic operators (+, -, *) in THEN/ELSE values and directly in
  SUM/MIN/MAX arguments (e.g., `SUM(l_extendedprice * (1 - l_discount))`)
- Recursive expression tree compilation into NdbAggregator program
- DECIMAL aggregation fix: `m_pushed_is_double` flag on Item_sum so
  val_decimal() and reset_field() use correct pushed value type

**Additional completed work:**
- Scan-scan join aggregation support (DBSPJ dummy program + child scan)
- 10 TPC-H NDB API benchmarks (Q1, Q3, Q4, Q5, Q9, Q10, Q12, Q13, Q18, Q19)

### Integration Test Coverage

**NDB API tests** (testJoinAggNdbApi.cpp, Tests 1–22):

| Test | What It Tests |
|------|---------------|
| 1–13 | SUM/GROUP BY, COUNT+SUM, multi-agg, 3-way join, type coverage |
| 14–17 | Eviction via ERROR_INSERT 5116/4040: 2-table, all agg funcs, 3-way, dual pressure |
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
- `mysql-test/suite/ndb_push_agg/t/ndb_join_pushdown_agg.test` — 35 SQL-level join agg tests
- `mysql-test/suite/ndb_push_agg/t/ndb_pushdown_agg.test` — 31 SQL-level single-table agg tests
- `mysql-test/suite/ndb_push_agg_dist/` — 2-node distributed tests (8 categories)

---

## Remaining Work

### Phase 15: AVG Support (Priority: High)

AVG is currently rejected by both `ndb_can_push_aggregation()` and
`ndb_push_single_table_aggregation()`. NdbAggregator has no native AVG.

**What's needed:**
- Rewrite AVG(x) as SUM(x) + COUNT(x) at the MySQL handler level
- In `ndb_build_aggregation_program()` / `ndb_build_stm_aggregation_program()`:
  emit two aggregation slots (one SUM, one COUNT) for each AVG
- In `ndb_fetch_next_aggregate_row()` / `ndb_fetch_stm_aggregate()`:
  compute AVG = SUM/COUNT when populating Item_sum pushed values
- Handle NULL: if COUNT is 0, AVG is NULL
- Benefits both join and single-table paths

**Target**: TPC-H Q1 `AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount)`

### Phase 16: DECIMAL Precision (Priority: High)

RonSQL test (ronsql_dbt3_1_2.test) shows DECIMAL→double precision loss:
MySQL `26777.986560` vs RonSQL `26777.986559999998`.

**What's needed:**
- Native DECIMAL arithmetic in NdbAggregator (currently converts DECIMAL
  to double internally)
- Or: keep double internally but convert back to DECIMAL at result time
  with proper rounding to match MySQL's precision guarantees

### Phase 17: Correlated Subquery Support for Aggregation (Priority: Medium)

TPC-H Q2 (Minimum Cost Supplier) has a correlated scalar subquery with
`MIN(ps_supplycost)` inside a 4-table join, correlated on `p_partkey` from the
outer 5-table join. This pattern is common in TPC-H (Q2, Q4, Q17, Q20, Q22).

**RonSQL plan**: `storage/ndb/src/ronsql/ronsql_join_phase7.md` (Phase 7,
Steps 36-44) covers subquery support using a hybrid approach:
- Multi-phase execution for uncorrelated subqueries (Steps 38-39)
- Decorrelation to semi-join/anti-join for correlated EXISTS/NOT EXISTS (Steps 41-42)
- Materialization + hash-join for correlated scalar subquery with agg (Step 43)

**MySQL handler**: Depends on what MySQL's optimizer produces — may decorrelate
into a flat join that SPJ can handle without handler-side subquery logic.

**What's needed (MySQL handler side):**

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

4. **Scalar subquery in HAVING clause**
   - Q11: `HAVING SUM(...) > (SELECT SUM(...) * 0.0001 FROM ...)`
   - Non-correlated subquery producing a scalar used in HAVING filter
   - MySQL likely evaluates the subquery independently, then uses the result
     as a constant in the HAVING filter — outer query pushdown unaffected
   - HAVING evaluation (Phase 6) already uses Item_sum pushed values
   - Verify MySQL doesn't merge/transform this in a way that breaks the
     push path for the outer query

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

### Phase 18: Semi-Join Aggregation & Related Features (Priority: Medium)

TPC-H Q4 (Order Priority Checking) uses EXISTS with a correlated subquery,
which MySQL converts to a semi-join. This requires aggregation pushdown to
work with semi-join semantics in SPJ.

**RonSQL plan**: Covered by Phase 7 Steps 40-42 in
`storage/ndb/src/ronsql/ronsql_join_phase7.md`. RonSQL decorrelates
EXISTS → semi-join with `MatchFirst`, NOT EXISTS → anti-join with
`MatchNullOnly`, leveraging SPJ's existing FIRST_MATCH/ANTI_JOIN support.

**What's needed:**

1. **Semi-join with aggregation pushdown**
   - MySQL converts `EXISTS (SELECT ... WHERE l_orderkey = o_orderkey ...)`
     to a semi-join with first-match semantics
   - NDB SPJ already has `NI_FIRST_MATCH` (QueryTree.hpp) for semi-joins
   - Verify that aggregation pushdown works when a join node uses
     first-match/semi-join — the aggregation runs on the leaf table,
     but semi-join may change which node is the leaf or how rows flow
   - Phase 17 covers correlated scalar subqueries (Q2); this covers
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
     alongside existing DATE_SUB() support (if added)

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

### Phase 19: Expressions in GROUP BY, Derived Tables, Cross-Table OR (Priority: Medium)

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

### Phase 20: Post-Aggregation Expressions & Large Join Trees (Priority: Medium)

TPC-H Q8 (National Market Share) uses division of two aggregate results.
8-table join with derived table. CASE in aggregates is already supported
(see completed work above).

**What's needed:**

1. **Post-aggregation arithmetic expressions in SELECT/HAVING**
   - Q8: `SUM(CASE...) / SUM(volume) AS mkt_share` — division of two aggregates
   - Q11: `SUM(ps_supplycost * ps_availqty) * 0.0001` — aggregate times constant
   - These are `Item_func_div`/`Item_func_mul` over `Item_sum` with pushed values
   - MySQL evaluates post-pushdown — verify that Item_sum::val_real() with
     pushed values composes correctly through arithmetic Item_func nodes

2. **6-table and 8-table join aggregation at MySQL handler level**
   - bench_q9_dbtc tests 6-table join aggregation at NDB API level
   - Verify SPJ topology constraints and handler limits for 6+ and 8-table
     pushed joins with aggregation through MySQL

**Target query (TPC-H Q8):**
```sql
SELECT o_year,
       SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END)
         / SUM(volume) AS mkt_share
FROM (
  SELECT EXTRACT(YEAR FROM o_orderdate) AS o_year,
         l_extendedprice * (1 - l_discount) AS volume,
         n2.n_name AS nation
  FROM part, supplier, lineitem, orders, customer,
       nation n1, nation n2, region
  WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey
    AND l_orderkey = o_orderkey AND o_custkey = c_custkey
    AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey
    AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey
    AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
    AND p_type = 'ECONOMY ANODIZED STEEL'
) AS all_nations
GROUP BY o_year
ORDER BY o_year;
```

### Phase 21: Outer Join Aggregation Pushdown at MySQL Handler (Priority: High)

**Status: IN PROGRESS**

The entire data node stack (DBLQH, DBSPJ, NDB API) already supports outer join
aggregation — DBSPJ tracks matched/unmatched parents and injects null-extended
rows via JOIN_AGG_NULL_ROW_REQ/CONF. The `testOuterJoinAggNdbApi` suite has
17 tests covering scan-lookup, scan-scan, COUNT(*) vs COUNT(col), multi-batch.

**What's implemented:**
- `ndb_join_pushdown_aggregate_outer_join` THDVAR (default OFF) gates the feature
- `ndb_push_aggregation()` accepts `allow_outer_join` parameter
- Semi-join and anti-join remain rejected (not yet supported at data node level)
- `build_query()` in `ha_ndbcluster_push.cc` already handles outer join nest
  metadata (setFirstInnerJoin, setUpperJoin, MatchAll default) — no changes needed

**What's needed:**
- MTR tests: update Test 35 (LEFT JOIN now pushable when enabled), add
  Tests 36-44 covering COUNT(*), COUNT(col), SUM, implicit agg, HAVING,
  ORDER BY+LIMIT, 3-way join, multi-batch
- Verify AccessPath surgery works for outer join NLJs
- Verify chain topology reordering preserves outer join nest structure
- Test with multi-node configuration in `ndb_push_agg_dist` suite

### Test Suite Fixes: Non-Deterministic Result Order (Priority: Medium)

MTR test cases for pushdown aggregation may produce results in
non-deterministic order because NDB's group-by hash map does not guarantee
output order. When aggregation is pushed, results come back in hash iteration
order rather than MySQL's usual sort-based grouping order.

**Fix:** Add `--sorted_result` directives or explicit `ORDER BY` clauses to
affected test cases in:
- `mysql-test/suite/ndb_push_agg/t/ndb_join_pushdown_agg.test`
- `mysql-test/suite/ndb_push_agg/t/ndb_pushdown_agg.test`
- Any other MTR tests that compare aggregate pushdown ON vs OFF results

Without this fix, test results are flaky — passing or failing depending on
hash table layout which varies across runs and platforms.

### Future: ASOF JOIN Support (RonSQL Only) (Priority: Low)

ASOF JOIN matches each row from the left table to the closest row in the
right table based on a temporal or ordered column, without requiring exact
equality. Common in time-series and financial data analysis.

**Example:**
```sql
SELECT t.*, q.price
FROM trades t
ASOF JOIN quotes q ON t.symbol = q.symbol AND t.timestamp >= q.timestamp;
```

**What's needed in RonSQL:**
- New join type in the RonSQL parser/grammar (ASOF JOIN keyword)
- Semantics: for each left row, find the right row with the largest key
  value that is <= the left row's key (or >= depending on direction)
- Execution: ordered index scan on the right table with upper/lower bound
  derived from each left row's join column value
- Aggregation support: ASOF JOIN combined with GROUP BY/SUM/COUNT etc.

### Future: Vector Search Join (RonSQL Only) (Priority: Low)

Support joins where the final (leaf) table is accessed via a vector
similarity search (k-nearest neighbors) rather than an exact key lookup
or index scan.

**Example:**
```sql
SELECT p.product_name, r.review_text, r.similarity
FROM products p
JOIN reviews r ON VECTOR_SEARCH(r.embedding, p.query_vector, 10);
```

**What's needed in RonSQL:**
- Vector similarity search as a join condition (KNN lookup on leaf table)
- Integration with NDB's vector index infrastructure
- The join produces top-K nearest neighbors from the right table for each
  left row, rather than exact-match or range-match rows
- Aggregation over vector search results (e.g., AVG similarity score
  per product category)

### Future: Complete RonSQL Join-Root Index Scan Follow-Ups (Priority: Medium)

Commit `82729085179` added scan-config-selected ordered-index scans for
main-query roots in join queries. The committed envelope is useful but still
root-focused; a review on July 2026 found the following follow-up items.

**Correctness / optimizer TODOs:**

1. **Fix composite root bound discovery when residual predicates appear first.**
   `build_scan_config_candidates()` currently sets `later_columns_blocked` when
   a condition does not match the current index column. This makes composite
   bound discovery sensitive to WHERE conjunct order. Example:

   ```sql
   WHERE residual_col > 0 AND indexed_a = 1 AND indexed_b >= 2
   ```

   On index `(indexed_a, indexed_b)`, the residual conjunct can prevent the
   second bound from being discovered. The block should happen only after all
   conjuncts have been scanned and no usable bound was found for the current
   index column.

2. **~~Discover constant bounds for non-root scans.~~ DONE (August
   2026, child_bounds feature)**: `assign_cross_table_index_bounds`
   generalized to `assign_child_index_bounds(QueryScope&)` —
   child-local constant conjuncts (same eligibility set as the root
   generator; NOT NULL columns only in v1) become `RangeBound`s with
   `const_cond` set, consumed out of `join_where_ce[op]`; emit builds
   `constValue` operands (memoized so EQ pairs collapse to BoundEQ).
   Required a DBSPJ fix: `scanFrag_fixupBound`'s renumbering assumed
   every upper bound follows a lower for the same column — an
   upper-only tail (EQ,...,EQ,GE — `col <= X` after the key prefix)
   got the previous column's id.  Fixed by tracking the previous
   entry's bound type.  (That bug was also reachable via the
   cross-table `T_LT/T_LE` direction, untested before.)  MTR:
   `body_child_bounds.inc` cb-1..12 ×5 topologies + sr-6 EXPLAIN pins.

3. **~~Choose the best child ordered index, not the first matching
   prefix.~~ DONE (August 2026, same feature)**: `assign_child_index_bounds`
   scores every join-key-prefix candidate
   (`QueryPlanner::collectOrderedIndexCandidates`) by additional
   bindable columns (EQ=3 and continue, range=1 and stop) and switches
   on strictly-better; ties keep the planner's choice so existing
   plans are stable.  While landing this, a latent hazard was found
   and fixed in `QueryPlanner::plan`: the index matchers accept a
   PERMUTED join-key set but emit binds key operands positionally, so
   a permuted ON order bound keys to the wrong index / PK columns —
   keys are now normalized to physical column order with an exact
   bijection check (which also rejects duplicate child columns in one
   ON list).

4. **Enable multi-op CTE body root index scans.**
   `build_cte_scopes()` still calls `select_root_scan_config()` only when
   `scope->join_plan.num_ops == 1`. Multi-op CTE bodies should use the same
   root selection once they pass only `scope.join_where_ce[0]`, not the whole
   CTE WHERE.

5. **Tighten plan regression tests.**
   `body_main_root_index.inc` currently keeps EXPLAIN greps non-fatal with
   `|| true`. Once the plan output is stable, make these assertions hard
   failures so a regression back to TABLE_SCAN is caught.

**Implementation notes (items 2+3 as landed):**

- No per-op ScanConfig was needed: children already carry per-op state
  (`join_where_ce[op]` for conjuncts, `JoinOp::low_bounds/high_bounds`
  for bounds); the `RangeBound::const_cond` extension (whole conjunct
  stored — RHS for `encode_constant`, inclusivity, printable node) was
  sufficient.  `body_scan_config` stays root-only.
- The pass runs for the main scope (load_join, after classification +
  LEFT→INNER promotion) and each CTE-body scope (build_cte_scopes), so
  multi-op CTE-body children are covered too.
- Subquery-leaf inner filters merge into `join_where_ce` AFTER the
  pass, so they are deliberately not bound-eligible (safe-retreat
  choice; revisit if profiling asks for it).
- Joined-table index hints remain rejected (`reject_index_hints_on_joins`);
  child index selection is now automatic via scoring — hint support on
  joined tables stays deferred.
- ~~New correctness suspicion recorded while landing: the ROOT path's
  `build_scan_config_candidates` has no nullability guard~~ —
  **CONFIRMED and FIXED (August 2026, findings/nullable_bounds.md)**:
  three instances (single-table bounds, join-root/CTE-body roots, and
  cross-table child bounds also lacked the guard).  Single-table keeps
  the bound and appends the mysqld `> NULL` idiom (`setBound(col,
  BoundLT, NULL)`); the NdbQueryBuilder-emitted paths cannot express a
  NULL bound operand, so nullable high-only conjuncts revert to
  residual filters there.  MTR `body_nullable_bounds.inc` nb-1..9 ×5.
- Remaining MTR wish: reordered residual/root composite predicates
  (item 1) and multi-op CTE body root bounds (item 4) — both still
  open.

### Future: Reduce Fixed Overhead for Small-Range CTE Queries (RonSQL) (Priority: Low)

Observed July 2026 with the `fs_batch` benchmark (customer JOIN aggregating
CTE over orders, 100-key range, ~1,128 orders rows): even with the root
INDEX_SCAN fix (commit 82729085179), MySQL is ~2x faster using ~2x less CPU.
Both engines push the two scans identically (pushed condition + MRR); the gap
is entirely the join/aggregation execution locus. At this selectivity the
query is overhead-bound — the distributed CTE machinery has a fixed cost
floor that MySQL's "ship ~1,200 rows to mysqld, join via in-memory temp
table" plan doesn't pay:

1. **Two serialized phases with a cluster-wide barrier** — JOIN_AGG_SETUP to
   every node, full CTE materialisation on all fragments, group merge +
   redistribute, JOIN_AGG_COMPLETE round trips, and only then the main scan.
2. **Per-row CTE_LOOKUP signaling** — one signal chain per probe row, routed
   (often cross-node) to the group's hash-owner, vs a local hashtable probe
   in mysqld.
3. **Fan-out fixed costs** — aggregate queries force `m_fragsPerWorker = 1`;
   every LDM on every node instantiates aggregator state (hash table, chunk
   allocator, ~8 KB query memory) for source CTE agg + target CTE state +
   main agg, then tears it down at RELEASE — the 2x CPU.
4. **Per-request envelope** — RDRS HTTP/JSON plus full parse → prepare →
   NdbQueryBuilder build per execution.

The pushdown's value is proportional to rows *not shipped*; at ~1,200 rows
there is nothing to save while the orchestration still costs full price.
The crossover should invert at larger ranges (10k–100k keys) — verify with
widened `fs_batch` spans or the `offline_fs_*` variants, and use the
phase-timing instrumentation planned in `ronsql_cli_benchmarks.md` to split
barrier latency from per-row signaling.

**Candidate optimizations (real projects, not tweaks):**
- **Skip/short-circuit the redistribute round** when the CTE produced few
  enough groups that shipping them all directly to the DBTC-co-located owner
  is cheaper — the scalar path from Phase I.17e already has the
  `keyLen == 0` JoinAggRedistributeReq variant to build on.
- **Batch CTE_LOOKUPs per owner node** instead of one signal chain per probe
  row.

Only worth pursuing if the small-range interactive case matters for the
feature-store workload.

### 5b. 64-bit rowsExamined (Priority: Low)

For very large joins (millions of leaf rows), the 32-bit rowsExamined
counter may overflow. Extend SCAN_FRAGCONF with version-gated
SignalLength_v3 carrying the upper 32 bits.

### 6. CTE outer-join remaining shapes (Priority: Low)

One outer-join shape remains deferred from the CTE outer-join branch
(`cte_outer_join_plan.md`):

**6a. CTE_LOOKUP agg-feed NULL injection.** SHIPPED in commit
`47d81b43903` (Phase 5 / `cte_outer_join_phase_5.md`). The REF-time
injection approach taken there is simpler than the
`T_BUFFER_MATCH` + match-bit-sweep approach originally sketched here:
each `CTE_LOOKUP_REF(GROUP_NOT_FOUND)` uniquely identifies one
unmatched parent via the correlation already echoed in the REF
signal (Phase 1 extension), so we only need
`T_BUFFER_ROW | T_BUFFER_MAP` on the scan ancestor + one
`getBufferedRow` + `sendJoinAggNullRow` call per REF. Verified by
Test 5 in `testCteNdbApiOuterJoin.cpp`.

**6b. CTE_SCAN as outer-join child.**
Rare SQL shape (cross-join + NULL-fill-if-empty). Dropped from this
branch; design notes in `cte_outer_join_phase_3.md`. Requires a new
`cte_scan_parent_row` handler, per-parent state tracking, and a
per-scan match-bit sweep.
