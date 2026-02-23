# RonSQL Join Phase 3 — Split Tests and New Features

## Scope

**The current focus is exclusively on RonSQL aggregation queries.** All join
work on this branch targets queries with GROUP BY + aggregate functions (SUM,
COUNT, MIN, MAX, AVG). Non-aggregation join queries (projection-only) are out
of scope for now. MySQL handler integration (`ha_ndbcluster_push.cpp`) is
handled in a separate work tree — this branch only concerns RonSQL.

## Context

Phase 2 added Tests 5-14 to `ronsql_join.test`, covering 3-table joins, WHERE
filters, parent-column aggregation, composite keys, and PK index scan. The
test file has grown large and mixes basic join validation with advanced features.
The user wants to split new tests into a separate file and plan the next round
of join+aggregation features.

## Step 19: Move Tests 5-14 to `ronsql_join_agg.test`

### Problem
`ronsql_join.test` now contains 14 tests mixing basic and advanced join
features. New test cases should live in a separate file.

### Approach
Create `ronsql_join_agg.test` with its own schema setup and Tests 5-14.
Keep Tests 1-4 (basic 2-table join) in `ronsql_join.test`.

### Files to modify

**`mysql-test/suite/ronsql/t/ronsql_join.test`** — Remove:
- `customer` table creation, `CREATE INDEX idx_custkey`, `CREATE INDEX idx_oid`
- Customer data inserts
- Tests 5-14 (including error test 10)
- `order_extra` table creation/data/drop
- `DROP TABLE customer`
- Keep: `orders`, `lineitem` schema, Tests 1-4, cleanup of orders+lineitem

**`mysql-test/suite/ronsql/t/ronsql_join_agg.test`** — New file containing:
- `--source include/have_ndb.inc`
- `call mtr.add_suppression("Schema dist coordinator detected timeout");`
- Full schema setup: `orders`, `lineitem`, `customer`, `order_extra` tables
  with all indexes (`idx_orderkey`, `idx_custkey`, `idx_oid`)
- Tests 5-14 moved verbatim
- Cleanup: drop all 4 tables

**`mysql-test/suite/ronsql/r/ronsql_join.result`** — Re-record with Tests 1-4 only

**`mysql-test/suite/ronsql/r/ronsql_join_agg.result`** — Re-record with Tests 5-14

### Verification
```bash
cd debug_build/mysql-test
perl ./mtr --suite=ronsql ronsql_join ronsql_join_agg
```
Both tests must pass independently.

---

## Step 20: WHERE filter on child/middle tables

### Problem
Currently `validate_where_join()` rejects any WHERE column not on the root
table. Queries like `WHERE l.l_quantity > 10` fail with an error. This is
the most requested missing feature for join queries.

### Approach
For columns on child tables, push the filter condition to the child
operation's `NdbQueryOptions::setInterpretedCode()`. The root operation
already supports this pattern.

### 20a. Remove root-only restriction in `validate_where_join()`
In `RonSQLPreparer.cpp`, change `validate_where_join()` to classify WHERE
conditions by which table they reference instead of rejecting non-root.
Collect into per-table condition lists.

### 20b. Apply per-table filters in `execute_join()`
For each operation (root and children), build an `NdbInterpretedCode` +
`NdbScanFilter` from that table's conditions and attach via
`NdbQueryOptions::setInterpretedCode()`.

For child lookup operations (`readTuple`), filters may need to be applied
differently — check if `setInterpretedCode` works on lookup operations or
if the row must be filtered post-fetch.

### 20c. Add MTR tests
Add to `ronsql_join_agg.test`:
- Test 15: `WHERE l.l_quantity > 10` — filter on leaf table
- Test 16: `WHERE o.o_custkey = 100 AND l.l_price > 100` — filter on both
  root and leaf

### Files to modify
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` — `validate_where_join()`,
  `execute_join()`
- `mysql-test/suite/ronsql/t/ronsql_join_agg.test`

---

## Step 21: COUNT(*) support for joins

### Problem
`COUNT(*)` counts all rows regardless of NULLs. Currently RonSQL may only
support `COUNT(col)`. Need to verify whether `COUNT(*)` is parsed and
handled correctly for join queries.

### 21a. Check parser support for COUNT(*)
Verify `RonSQLParser.y` accepts `COUNT(*)` syntax. If not, add grammar rule.

### 21b. Verify aggregator emits correct instruction
`COUNT(*)` should emit an unconditional increment (no NULL check on any
column). Check `programAggregator_join()` handles this case.

### 21c. Add MTR test
- Test 17: `SELECT o.o_custkey, COUNT(*) FROM orders AS o JOIN lineitem AS l
  ON l.l_orderkey = o.o_id GROUP BY o.o_custkey`

### Files to modify
- `storage/ndb/src/ronsql/RonSQLParser.y` (if needed)
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` (if needed)
- `mysql-test/suite/ronsql/t/ronsql_join_agg.test`

---

## Step 22: Joins without aggregation (projection-only) — DEFERRED

**Deferred.** Current scope is RonSQL aggregation queries only. Projection-only
joins will be revisited separately.

---

## Step 23: AVG aggregate on joins

### Problem
AVG is supported for single-table queries (emitted as SUM+COUNT pair with
client-side division). Verify it works for join queries and add test
coverage.

### 23a. Verify programAggregator_join() handles AVG
Check if `AggOp::Avg` in the compiled program triggers the correct
SUM+COUNT pair via `LoadColumn`/`LoadLinkedColumn` for join queries.

### 23b. Add MTR test
- Test 20: `SELECT o.o_custkey, AVG(l.l_quantity) FROM orders o JOIN
  lineitem l ON l.l_orderkey = o.o_id GROUP BY o.o_custkey`

### Files to modify
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` (if fix needed)
- `mysql-test/suite/ronsql/t/ronsql_join_agg.test`

---

## Step 24: Multiple aggregates mixed with expressions

### Problem
Queries combining multiple aggregates and arithmetic should work:
`SELECT o.o_custkey, SUM(l.l_price) - SUM(l.l_quantity) FROM ...`.
Need test coverage.

### 24a. Verify expression handling in programAggregator_join()
Arithmetic expressions between aggregates should already work since
`AggregationAPICompiler` handles `Add`, `Sub`, `Mul`, `Div` between
aggregate results.

### 24b. Add MTR tests
- Test 21: `SUM(l.l_price) - SUM(l.l_quantity)` — arithmetic between
  aggregates
- Test 22: `SUM(l.l_quantity * l.l_price)` — expression inside aggregate

### Files to modify
- `mysql-test/suite/ronsql/t/ronsql_join_agg.test`

---

## Implementation Order

1. **Step 19** — Split tests (mechanical, no code changes) — **DONE**
2. **Step 21** — COUNT(*) (likely already works, just verify + test) — **DONE**
3. **Step 23** — AVG (likely already works, just verify + test) — **DONE**
4. **Step 24** — Mixed expressions (likely already works, just test) — **DONE**
5. **Step 20** — WHERE on child tables (new feature, highest complexity)
6. ~~**Step 22**~~ — Projection-only joins — **DEFERRED** (out of scope,
   current focus is aggregation queries only)

Steps 21, 23, 24 were verification-only (tests added, all working).
Step 20 is the remaining feature implementation for Phase 3.

## Verification

After each step:
```bash
cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
cd mysql-test && perl ./mtr --suite=ronsql ronsql_join ronsql_join_agg
```
