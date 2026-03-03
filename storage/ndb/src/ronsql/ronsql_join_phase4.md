# RonSQL Join Phases 4–6 — Aggregation Query Roadmap

## Scope

This plan covers RonSQL aggregation join queries only. MySQL handler
integration (`ha_ndbcluster_push.cpp`) is in a separate work tree.
Projection-only joins (no GROUP BY / no aggregation) are deferred.

## Prerequisites

Phase 3 is complete (all steps including Step 20 WHERE filter on
child/middle tables).

---

## Phase 4 — Aggregation Query Completeness

Features that make RonSQL aggregation joins production-useful.

### Step 25: HAVING clause

#### Problem
No way to filter on aggregate results. Queries like
`SELECT o.o_custkey, SUM(l.l_price) FROM ... GROUP BY o.o_custkey
HAVING SUM(l.l_price) > 1000` are not supported.

#### Approach
HAVING is a post-aggregation filter applied after all groups are computed.
Parse HAVING as a separate clause, evaluate the condition against each
result row client-side in RonSQLPreparer or ResultPrinter, and suppress
rows that don't match.

#### Sub-steps
- **25a.** Add `T_HAVING` token to lexer; add HAVING grammar rule in
  `RonSQLParser.y` that accepts the same expression syntax as WHERE but
  allows aggregate function references
- **25b.** Store parsed HAVING expression in `SelectStatement`
- **25c.** After aggregator returns results, evaluate HAVING condition per
  row and skip non-matching rows before output
- **25d.** MTR tests: basic HAVING with SUM threshold, HAVING with COUNT,
  HAVING that eliminates all rows (empty result)

#### Files to modify
- `storage/ndb/src/ronsql/RonSQLLexer.l`
- `storage/ndb/src/ronsql/RonSQLParser.y`
- `storage/ndb/src/ronsql/RonSQLCommon.hpp`
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `mysql-test/suite/ronsql/t/ronsql_join_agg.test`

---

### Step 26: ORDER BY on aggregated results

#### Problem
Aggregated join results come back in arbitrary order. Queries like
`SELECT o.o_custkey, SUM(l.l_price) AS total FROM ... GROUP BY o.o_custkey
ORDER BY total DESC` are not supported for joins.

#### Approach
ORDER BY for single-table aggregation queries may already work. For join
queries, verify that the existing ORDER BY logic in RonSQLPreparer applies
to aggregated results. If not, add client-side sorting of the result rows
after aggregation completes.

#### Sub-steps
- **26a.** Verify whether ORDER BY is already parsed and accepted for join
  queries or rejected by validation
- **26b.** If rejected, remove the restriction and route to existing
  single-table ORDER BY logic
- **26c.** Support ordering by aggregate expressions (`ORDER BY SUM(...)`)
  and by alias if supported
- **26d.** MTR tests: ORDER BY group column ASC/DESC, ORDER BY aggregate
  expression, ORDER BY with LIMIT

#### Files to modify
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `mysql-test/suite/ronsql/t/ronsql_join_agg.test`

---

### Step 27: LIMIT on aggregated results

#### Problem
No way to restrict the number of returned groups. `LIMIT 10` after
aggregation should return only the first 10 groups (after ORDER BY if
present).

#### Approach
LIMIT for single-table queries may already work. Verify for joins and
enable if needed. This is simple post-processing — stop emitting rows
after N groups.

#### Sub-steps
- **27a.** Verify LIMIT acceptance for join queries
- **27b.** If rejected, remove the restriction
- **27c.** MTR tests: LIMIT alone, ORDER BY + LIMIT, LIMIT 0 (edge case)

#### Files to modify
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `mysql-test/suite/ronsql/t/ronsql_join_agg.test`

---

### Step 28: COUNT(DISTINCT col)

#### Problem
`COUNT(DISTINCT l.l_shipmode)` counts unique values within each group.
Not currently supported.

#### Approach
This requires either AggInterpreter support (per-group distinct tracking
at the data node) or client-side deduplication. Data-node support is
preferred for performance but may be complex. Evaluate feasibility of
both approaches.

#### Sub-steps
- **28a.** Check if AggInterpreter has DISTINCT support or can be extended
- **28b.** If not feasible at data node, implement client-side: collect
  all values per group and deduplicate before counting
- **28c.** MTR tests: COUNT(DISTINCT col), COUNT(DISTINCT col) with
  GROUP BY, edge case with all-same values

#### Files to modify
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `storage/ndb/src/ronsql/AggregationAPICompiler.cpp` (if data-node path)
- `mysql-test/suite/ronsql/t/ronsql_join_agg.test`

---

## Phase 5 — Broader SQL Coverage

### Step 29: LEFT JOIN with aggregation

#### Problem
Only INNER JOIN is supported. LEFT JOIN preserves all root rows even when
no child match exists, producing NULL for child columns. This is important
for queries like "all customers and their order totals, including customers
with no orders".

#### Approach
NdbQueryBuilder supports outer joins via `NdbQueryOptions::setMatchType()`.
Add LEFT JOIN grammar, set the match type, and handle NULL child columns
in aggregation (SUM/COUNT should treat NULLs correctly by default).

#### Sub-steps
- **29a.** Add `T_LEFT` token and LEFT JOIN grammar rule
- **29b.** Set `NdbQueryOptions::MatchNonNull` vs `MatchAll` based on
  join type in `execute_join()`
- **29c.** Verify aggregate functions handle NULL child rows correctly
- **29d.** MTR tests: LEFT JOIN with matching and non-matching root rows,
  LEFT JOIN with COUNT(*) vs COUNT(col) difference

#### Files to modify
- `storage/ndb/src/ronsql/RonSQLLexer.l`
- `storage/ndb/src/ronsql/RonSQLParser.y`
- `storage/ndb/src/ronsql/RonSQLCommon.hpp`
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `mysql-test/suite/ronsql/t/ronsql_join_agg.test`

---

### Step 30: Multiple GROUP BY columns from different tables

#### Problem
GROUP BY columns may come from different tables in the join chain, e.g.,
`GROUP BY o.o_custkey, l.l_shipmode`. This may already partially work
via linked projections but needs verification and test coverage.

#### Sub-steps
- **30a.** Verify that GROUP BY columns from both parent and child tables
  are correctly resolved and compiled
- **30b.** MTR tests: GROUP BY mixing parent + child columns, GROUP BY on
  3 columns from 3 different tables

#### Files to modify
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` (if fix needed)
- `mysql-test/suite/ronsql/t/ronsql_join_agg.test`

---

### Step 31: Self-joins with aggregation

#### Problem
Joining a table to itself with different aliases, e.g.,
`FROM orders o1 JOIN orders o2 ON o2.o_custkey = o1.o_custkey`
is not tested and may fail in schema loading (same table opened twice).

#### Sub-steps
- **31a.** Verify schema loading handles the same table referenced twice
  with different aliases
- **31b.** MTR test: self-join with aggregation

#### Files to modify
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` (if fix needed)
- `mysql-test/suite/ronsql/t/ronsql_join_agg.test`

---

### Step 32: Broader data type coverage in join keys

#### Problem
Join conditions currently tested with INT columns only. VARCHAR, BIGINT,
and date-type join keys need verification.

#### Sub-steps
- **32a.** MTR tests with VARCHAR join keys
- **32b.** MTR tests with DATE/TIMESTAMP join keys

#### Files to modify
- `mysql-test/suite/ronsql/t/ronsql_join_agg.test`

---

## Phase 6 — Robustness

### Step 33: Group eviction through RonSQL path

#### Problem
When the AggInterpreter hash table fills up, groups are evicted via
TRANSID_AI to the API. This is tested at signal level (ERROR_INSERT 5090)
but not through the RonSQL REST API path.

#### Approach
Create a test with enough distinct GROUP BY values to trigger eviction.
Verify that the REST API correctly receives and merges evicted + final
groups.

#### Sub-steps
- **33a.** Create a test dataset with many distinct groups (> default
  hash table capacity)
- **33b.** Verify RonSQL returns correct aggregated results despite
  eviction
- **33c.** If ERROR_INSERT is available through MTR, use 5090 to force
  eviction with small datasets

---

### Step 34: Large-scale stress tests

#### Problem
Current tests use small datasets (< 20 rows). Need confidence with
larger data.

#### Sub-steps
- **34a.** Test with 1000+ rows, 100+ groups
- **34b.** Test with 4+ table join chains
- **34c.** Test with wide rows (many columns in GROUP BY and aggregation)

---

### Step 35: Error message improvements

#### Problem
Unsupported query combinations should produce clear, actionable error
messages rather than cryptic failures.

#### Sub-steps
- **35a.** Audit all error paths in join validation
- **35b.** Ensure each unsupported combination has a specific message
  (e.g., "LEFT JOIN not supported", "HAVING requires GROUP BY")
- **35c.** MTR tests for error messages

---

## Implementation Order

### Phase 4 (aggregation completeness)
1. **Step 25** — HAVING (new parser + post-filter logic) — **DONE**
2. **Step 26** — ORDER BY on aggregated results — **DONE**
3. **Step 27** — LIMIT on aggregated results — **DONE**
4. **Step 28** — COUNT(DISTINCT) (most complex, may need AggInterpreter)

### Phase 5 (broader SQL)
5. **Step 29** — LEFT JOIN (new grammar + NdbQueryOptions match type)
6. **Step 30** — Multi-table GROUP BY — **DONE** (verified + tested)
7. **Step 31** — Self-joins — **DONE** (verified + tested)
8. **Step 32** — Data type coverage (test-only)

### Phase 6 (robustness)
9. **Step 33** — Eviction through RonSQL
10. **Step 34** — Large-scale stress
11. **Step 35** — Error messages

### Additional features implemented (beyond original plan)
- CHAR column type in WHERE comparisons
- LIKE operator in WHERE clauses
- IN operator in WHERE clauses
- CASE/WHEN/THEN/ELSE/END in aggregation expressions
- TPC-H Q9/Q12 end-to-end tests via REST API

---

## Phase 7 — Subquery Support

See `ronsql_join_phase7.md` for the full plan. Hybrid approach:
- Multi-phase execution for uncorrelated subqueries (scalar, IN-list)
- Decorrelation into semi-join/anti-join for correlated subqueries
  (EXISTS, NOT EXISTS, correlated scalar with aggregation)
- Leverages SPJ's existing `MatchFirst`/`MatchNullOnly` support

Steps 36-44. Prerequisite: Step 29 (LEFT JOIN) for MatchType infrastructure.

## Verification

After each step:
```bash
cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
cd mysql-test && perl ./mtr --suite=ronsql ronsql_join ronsql_join_agg
```
