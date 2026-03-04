# RonSQL Join Phase 7 — Subquery Support (Hybrid Approach)

## Scope

Add subquery support to RonSQL using a hybrid strategy:
- **Multi-phase execution** for uncorrelated subqueries (simple, handles
  scalar and IN-list patterns)
- **Decorrelation into joins** for correlated subqueries (leverages SPJ's
  existing `MatchFirst`/`MatchNullOnly` semi-join/anti-join support)

This covers the RonSQL side of RONDB-733 Phases 17-18 (correlated subquery
support and semi-join aggregation). MySQL handler integration is separate.

## Prerequisites

- Phase 5 Step 29 (LEFT JOIN) should be done first. It introduces
  `MatchType` on `JoinOp` and passes it through to `NdbQueryOptions` in
  `execute_join()`. Semi-join and anti-join steps below extend this same
  infrastructure with `MatchFirst` and `MatchNullOnly`.
- Phase 4 Step 25 (HAVING) must be complete (it is).

## Current Limitations

RonSQL today has:
- No `(SELECT ...)` production anywhere in the grammar
- No `EXISTS`, `ALL`, `ANY` keywords implemented
- `IN` only accepts a literal value list (expanded to OR-chain at parse time)
- `SelectStatement` is a flat struct with no nesting
- `ConditionalExpression` has no subquery variant
- Exactly one `NdbQuery` per request — no multi-phase execution
- `QueryPlanner` produces a single linear `JoinPlan` with inner-join-only

NDB SPJ already supports:
- `NdbQueryOptions::MatchFirst` — semi-join (first match, early stop)
- `NdbQueryOptions::MatchNullOnly` — anti-join (non-matching parents)
- `DABits::NI_FIRST_MATCH` / `NI_ANTI_JOIN` in QueryTree protocol
- DBSPJ `T_REDUCE_KEYS` for cross-fragment first-match deduplication
- MySQL already pushes semi-joins and anti-joins through this path

## Architecture Overview

```
                      +------------------+
                      | SubqueryAnalyzer |
                      +--------+---------+
                               |
              +----------------+----------------+
              |                                 |
     uncorrelated?                      correlated?
              |                                 |
   +----------v-----------+          +----------v-----------+
   | Multi-Phase Executor |          | Decorrelation Pass   |
   | (execute subquery    |          | (rewrite to join     |
   |  first, substitute   |          |  with MatchFirst/    |
   |  result into outer)  |          |  MatchNullOnly)      |
   +-----------+----------+          +----------+-----------+
               |                                |
               +---------->  QueryPlanner  <----+
                             (single JoinPlan,
                              now with MatchType
                              per join node)
                                    |
                              NdbQueryBuilder
                              (single NdbQuery)
```

Multi-phase execution runs the subquery as a separate NdbQuery first,
collects the results, and substitutes them as constants or filter values
in the outer query. Decorrelation rewrites the correlated subquery into
an additional join node so everything runs as a single pushed NdbQuery.

---

## Step 36: Parser and AST Infrastructure for Subqueries

### Problem
No grammar production allows `(SELECT ...)` to appear in WHERE, HAVING,
or as an operand. No AST node represents a subquery.

### Approach
Add the foundational grammar and AST changes that all subsequent steps
build on. After this step, subqueries parse into the AST but execution
rejects them with a clear error until Steps 38+ implement each form.

### Sub-steps

- **36a.** Add keywords to `keywords_implemented_in_ronsql[]`:
  `EXISTS`, `NOT EXISTS` (NOT is already implemented). `ALL`, `ANY`,
  `SOME` can be deferred until needed.

- **36b.** Add `SubqueryExpr` to `RonSQLCommon.hpp`:
  ```
  struct SubqueryExpr {
    SelectStatement *subquery;   // the inner SELECT
    enum Kind { SCALAR, EXISTS, NOT_EXISTS, IN_SUBQUERY } kind;
  };
  ```
  Extend `ConditionalExpression` with a subquery variant.

- **36c.** Add grammar rules in `RonSQLParser.y`:
  - `T_LEFT selectstatement T_RIGHT` as a `subquery` production
    (parenthesized SELECT). This requires making `selectstatement`
    reusable as a non-terminal without the trailing semicolon — split
    into `select_body` (no semicolon) and `selectstatement` (adds
    semicolon).
  - `T_EXISTS T_LEFT select_body T_RIGHT` as a `cond_expr` variant
  - `T_NOT T_EXISTS T_LEFT select_body T_RIGHT` as a `cond_expr` variant
  - `cond_expr T_IN T_LEFT select_body T_RIGHT` as a `cond_expr` variant
    (extends the existing IN rule which only accepts literal lists)
  - Scalar subquery: `T_LEFT select_body T_RIGHT` as a `cond_expr`
    variant (returns a single value for comparison)

- **36d.** Validation in `RonSQLPreparer::load()`: detect subquery nodes
  and reject with `"Subqueries are not yet supported"` until later steps
  enable each form.

- **36e.** MTR tests: verify that subquery syntax parses without crash
  and produces a clear "not yet supported" error message.

### Files to modify
- `storage/ndb/src/ronsql/Keywords.hpp`
- `storage/ndb/src/ronsql/RonSQLParser.y`
- `storage/ndb/src/ronsql/RonSQLCommon.hpp`
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `mysql-test/suite/ronsql/t/ronsql_join_agg.test`

### Design considerations

The inner `select_body` reuses the same grammar as the outer query. This
means the inner query can itself have JOINs, WHERE, GROUP BY, HAVING,
ORDER BY, LIMIT — all of which are useful (e.g., TPC-H Q2's inner query
is a 4-table join with MIN aggregate).

Nested subqueries (subquery inside subquery) parse naturally via grammar
recursion but should be rejected initially. Supporting them is a future
extension once single-level subqueries are solid.

---

## Step 37: Multi-Phase Execution Orchestrator

### Problem
RonSQL executes exactly one NdbQuery per request. Subqueries that are
executed separately need a mechanism to run multiple queries in sequence,
passing results from earlier phases to later phases.

### Approach
Add an execution orchestrator layer between `RonSQLPreparer::execute()`
and the NDB API. The orchestrator manages a sequence of execution phases:

1. Execute inner queries (subqueries), collecting results
2. Substitute results into the outer query
3. Execute the outer query

### Sub-steps

- **37a.** Add `SubqueryResult` struct to hold materialized subquery output:
  ```
  struct SubqueryResult {
    enum Kind { SCALAR, VALUE_SET } kind;
    // For SCALAR: single value (int64, double, or NULL)
    // For VALUE_SET: list of values (for IN-subquery)
  };
  ```

- **37b.** Add `SubqueryAnalyzer` pass (between `load()` and `compile()`):
  walks the AST, finds SubqueryExpr nodes, classifies each as:
  - Uncorrelated (no outer column references) → multi-phase
  - Correlated (references outer columns) → decorrelation (Step 41+)
  Reports an error for correlated subqueries until decorrelation is
  implemented.

- **37c.** Add `execute_subqueries()` method: for each uncorrelated
  subquery, builds and executes a standalone NdbQuery/NdbScanOperation
  (reusing existing single-table or join execution paths), collects
  results into `SubqueryResult`.

- **37d.** Add result substitution: after subquery execution, replace
  `SubqueryExpr` nodes in the outer query's AST with the materialized
  values (constants or IN-list expansion).

- **37e.** The outer query then executes through the normal path
  (`execute()` or `execute_join()`) with the substituted constants.

### Files to modify
- `storage/ndb/src/ronsql/RonSQLCommon.hpp`
- `storage/ndb/src/ronsql/RonSQLPreparer.hpp`
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`

### Design considerations

The inner query's `RonSQLPreparer` needs its own `NdbTransaction`. NDB
allows only one scanning query per transaction, so the inner and outer
queries need separate transactions. Alternatively, the inner scan can
complete and close before the outer scan starts (sequential, same
transaction reused). Evaluate which approach is simpler.

Since RonSQL currently requires aggregation, the inner query for an
IN-subquery (which returns a set of raw values, not aggregates) would
need the aggregation requirement relaxed for subqueries. This is also
useful groundwork for projection-only queries (Step 22, currently
deferred).

The orchestrator is also the foundation for derived tables (Phase 19 in
next_steps.md) — a FROM-clause subquery is essentially a multi-phase
execution where phase 1 materializes the derived table.

---

## Step 38: Uncorrelated Scalar Subqueries

### Problem
Queries like `WHERE col > (SELECT MAX(x) FROM t2)` or
`WHERE col = (SELECT MIN(x) FROM t2 WHERE ...)` use a subquery that
returns a single value, independent of the outer query.

### Approach
Multi-phase execution: run the inner aggregate query first, extract the
scalar result, substitute as a constant in the outer WHERE/HAVING.

### Sub-steps

- **38a.** In `SubqueryAnalyzer`: detect scalar subquery (subquery in
  comparison context, expected to return one row with one column).
  Validate: inner query must have aggregation without GROUP BY (implicit
  aggregation = guaranteed single row), or have LIMIT 1.

- **38b.** In `execute_subqueries()`: execute the inner query via the
  existing single-table or join execution path. Extract the single
  aggregate result from the NdbAggregator. Store in `SubqueryResult`
  as SCALAR.

- **38c.** Substitute: replace the `SubqueryExpr` node in the outer
  WHERE/HAVING `ConditionalExpression` tree with a constant node
  holding the scalar value.

- **38d.** MTR tests:
  - `WHERE col > (SELECT MAX(x) FROM t2)` — scalar in WHERE
  - `WHERE col = (SELECT MIN(x) FROM t2 WHERE y = 5)` — scalar with
    inner WHERE
  - `HAVING SUM(x) > (SELECT SUM(y) * 0.0001 FROM t2)` — scalar in
    HAVING (TPC-H Q11 pattern)
  - Edge case: inner query returns NULL (empty table)

### Files to modify
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `mysql-test/suite/ronsql/t/ronsql_subquery.test` (new test file)

---

## Step 39: Uncorrelated IN (SELECT ...) Subqueries

### Problem
Queries like `WHERE col IN (SELECT x FROM t2 WHERE ...)` filter the
outer query against a set of values produced by the inner query.

### Approach
Multi-phase execution: run the inner query first, collect all result
values, then build an OR-chain NdbScanFilter for the outer query. This
reuses the existing `IN (v1, v2, v3)` → OR-chain expansion that the
parser already does for literal IN-lists.

### Sub-steps

- **39a.** In `SubqueryAnalyzer`: detect IN-subquery (subquery after
  `IN` keyword in cond_expr). The inner query must return a single
  column.

- **39b.** In `execute_subqueries()`: execute the inner query. If it
  has aggregation + GROUP BY, iterate all result groups and collect the
  GROUP BY column values. If it has no aggregation (projection-only),
  this requires relaxing the aggregation requirement (see Step 37
  design note) — alternatively, wrap as `SELECT DISTINCT x` using
  GROUP BY internally.

- **39c.** Substitute: replace the `SubqueryExpr` IN node with an
  OR-chain of equality comparisons, same as the parser does for
  literal IN-lists. If the result set is empty, the condition evaluates
  to FALSE (no rows pass the filter).

- **39d.** Guard against large result sets: if the inner query returns
  more than N values (e.g., 1000), reject with an error rather than
  building a massive OR-chain. Document the limit.

- **39e.** MTR tests:
  - `WHERE col IN (SELECT x FROM t2 WHERE y > 5)` — basic IN-subquery
  - `WHERE col IN (SELECT x FROM t2)` with empty t2 — empty result set
  - `WHERE col NOT IN (SELECT x FROM t2)` — NOT IN variant
  - Large result set exceeding the limit → clear error

### Files to modify
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `mysql-test/suite/ronsql/t/ronsql_subquery.test`

### Design considerations

For large IN-lists, an alternative to OR-chain is to materialize the
subquery result into a hash set and filter client-side after the outer
scan. This avoids the NdbScanFilter size limit but loses pushdown
(all outer rows are scanned). Evaluate the tradeoff based on typical
subquery result sizes.

NOT IN has NULL semantics: if the subquery result contains NULL, the
NOT IN condition is unknown (not TRUE) for all outer values. This is
standard SQL behavior and needs careful handling.

---

## Step 40: Semi-Join and Anti-Join in QueryPlanner

### Problem
Correlated subqueries (Steps 41-43) are decorrelated into join nodes
with `MatchFirst` (semi-join) or `MatchNullOnly` (anti-join). The
QueryPlanner and execution path need to support these match types.

### Prerequisite
Step 29 (LEFT JOIN) introduces `MatchType` on `JoinOp` for outer joins
(`MatchAll`). This step extends that to semi-join and anti-join types.

### Approach
Extend `JoinOp` and `execute_join()` to handle `MatchFirst` and
`MatchNullOnly`. This is infrastructure — no new SQL syntax is exposed
in this step.

### Sub-steps

- **40a.** Extend `JoinOp::MatchType` enum (if not already done in
  Step 29) to include `SEMI_JOIN` and `ANTI_JOIN` in addition to
  `INNER` and `LEFT_OUTER`.

- **40b.** In `execute_join()`, when building NdbQueryOptions for a
  child operation: map `JoinOp::SEMI_JOIN` →
  `NdbQueryOptions::MatchFirst`, `JoinOp::ANTI_JOIN` →
  `NdbQueryOptions::MatchNullOnly`.

- **40c.** Verify that aggregation composes correctly with semi-join:
  aggregation is on the leaf of the outer query; the semi-joined
  table is a filter (it restricts which outer rows participate but
  does not contribute aggregate columns). The semi-joined table must
  NOT be the aggregation leaf.

- **40d.** Verify DBSPJ handling: `MatchFirst` on a lookup child means
  DBSPJ returns at most one match per parent row (which is already the
  case for lookups). On a scan child, DBSPJ stops the scan after the
  first match per parent key (`T_REDUCE_KEYS`).

### Files to modify
- `storage/ndb/src/ronsql/QueryPlanner.hpp`
- `storage/ndb/src/ronsql/QueryPlanner.cpp`
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`

### Design considerations

For aggregation with semi-join, the join tree topology matters. Consider
TPC-H Q4:
```sql
SELECT o_orderpriority, COUNT(*) FROM orders
WHERE EXISTS (SELECT * FROM lineitem
              WHERE l_orderkey = o_orderkey
              AND l_commitdate < l_receiptdate)
GROUP BY o_orderpriority;
```

After decorrelation, this becomes:
```
orders (root, scan) → lineitem (semi-join child, MatchFirst)
```

Aggregation (COUNT, GROUP BY o_orderpriority) is on `orders`, which is
the root — not the leaf. This differs from the current model where
aggregation always attaches to the leaf (deepest) table. Two approaches:

**Option A: Aggregation on root with semi-join child.** The semi-joined
table acts as a filter, not as an aggregation source. The NdbAggregator
runs on the root scan operation, counting root rows that have at least
one matching child. This requires changing the aggregation placement
rule ("aggregation must be on the leaf") for semi-join cases.

**Option B: Re-model as filter.** Instead of semi-join in SPJ, execute
the inner query to get matching orderkeys, then filter the outer scan
with those keys (like Step 39 but for correlated). This falls back to
multi-phase and loses the single-NdbQuery benefit.

Option A is more efficient but needs verification that SPJ supports
aggregation on a non-leaf node when the leaf is a semi-join filter.
This is a key architectural question to resolve before implementing
Steps 41-42.

---

## Step 41: Correlated EXISTS → Semi-Join Decorrelation

### Problem
`WHERE EXISTS (SELECT ... WHERE inner.col = outer.col AND ...)` is a
correlated subquery — the inner query references an outer column.
Cannot be solved by multi-phase execution (inner depends on each outer
row).

### Approach
Decorrelate: rewrite the EXISTS subquery into an additional join node
in the outer query with `MatchFirst` semantics. The correlation
predicate (`inner.col = outer.col`) becomes the equi-join condition.

### Sub-steps

- **41a.** In `SubqueryAnalyzer`: detect correlated EXISTS subquery.
  Identify correlation predicates (inner WHERE conditions that
  reference outer table columns). Non-correlation predicates stay
  as filters on the new join node.

- **41b.** Add `DecorrelationPass`: takes a correlated EXISTS
  subquery and rewrites the outer query's AST:
  - Add the inner query's tables as new join nodes in the outer
    query's join list
  - Set `MatchType = SEMI_JOIN` on the new join nodes
  - Convert correlation predicates to equi-join ON conditions
  - Move non-correlation predicates to WHERE filters on the new
    join nodes (pushed via `setInterpretedCode`)

- **41c.** If the inner query itself is a multi-table join, all inner
  tables become additional join nodes in the outer query tree. The
  semi-join match type goes on the first inner table (the one that
  joins to an outer table via the correlation predicate).

- **41d.** Feed the rewritten query through the normal `QueryPlanner`
  → `execute_join()` path. The planner sees a wider join tree with
  semi-join nodes and produces a single NdbQuery.

- **41e.** MTR tests:
  - Basic: `WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.fk = t1.pk)`
  - With filter: `WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.fk = t1.pk
    AND t2.val > 10)` — non-correlation predicate becomes filter
  - With aggregation on outer: `SELECT t1.grp, COUNT(*) FROM t1
    WHERE EXISTS (...) GROUP BY t1.grp` (Q4 pattern)
  - No match: outer row has no matching inner rows → excluded
  - Empty inner table: no outer rows match

### Files to modify
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `storage/ndb/src/ronsql/RonSQLPreparer.hpp`
- `mysql-test/suite/ronsql/t/ronsql_subquery.test`

### Target query: TPC-H Q4

```sql
SELECT o_orderpriority, COUNT(*) AS order_count
FROM orders
WHERE o_orderdate >= '1993-07-01'
  AND o_orderdate < '1993-10-01'
  AND EXISTS (
    SELECT * FROM lineitem
    WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
GROUP BY o_orderpriority
ORDER BY o_orderpriority;
```

Decorrelates to:
```sql
SELECT o_orderpriority, COUNT(*) AS order_count
FROM orders
SEMI JOIN lineitem ON l_orderkey = o_orderkey
WHERE o_orderdate >= '1993-07-01'
  AND o_orderdate < '1993-10-01'
  AND l_commitdate < l_receiptdate
GROUP BY o_orderpriority
ORDER BY o_orderpriority;
```

Note: `l_commitdate < l_receiptdate` is a cross-column comparison on
the child table (both columns from lineitem). NDB interpreter supports
column-vs-column comparison via `branch_col_eq_col` etc. RonSQL's
WHERE pushdown needs to support this (currently only column-vs-constant).

---

## Step 42: Correlated NOT EXISTS → Anti-Join Decorrelation

### Problem
`WHERE NOT EXISTS (SELECT ... WHERE inner.col = outer.col)` returns
outer rows that have NO matching inner row. This is the anti-join
pattern.

### Approach
Same decorrelation as Step 41, but with `MatchNullOnly` instead of
`MatchFirst`. The mechanics are identical — only the match type differs.

### Sub-steps

- **42a.** Extend `DecorrelationPass` to handle NOT EXISTS:
  set `MatchType = ANTI_JOIN` on the new join nodes.

- **42b.** Verify SPJ behavior: `MatchNullOnly` means DBSPJ returns
  parent rows only when NO child match is found. For lookup children,
  this means `LQHKEYREF` (no match) keeps the parent. For scan
  children, DBSPJ must scan all fragments before confirming no match
  (cannot early-stop like semi-join).

- **42c.** MTR tests:
  - Basic: `WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.fk = t1.pk)`
  - With aggregation: `SELECT t1.grp, COUNT(*) FROM t1
    WHERE NOT EXISTS (...) GROUP BY t1.grp`
  - All match: every outer row has a match → empty result
  - None match: no inner rows → all outer rows returned

### Files to modify
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `mysql-test/suite/ronsql/t/ronsql_subquery.test`

---

## Step 43: Correlated Scalar Subquery with Aggregation

### Problem
TPC-H Q2 pattern: `WHERE ps_supplycost = (SELECT MIN(ps_supplycost)
FROM partsupp, supplier, ... WHERE ps_partkey = p_partkey AND ...)`
The inner query is correlated (references outer `p_partkey`), returns
a scalar aggregate, and the outer query compares against it.

### Approach
This is the hardest subquery form. Two strategies:

**Strategy A: Decorrelation to derived-table join.**
Rewrite the correlated scalar subquery as an independent aggregation
query producing `(correlation_key, agg_result)` pairs, then join the
result back to the outer query on the correlation key with an equality
condition on the aggregate result.

For Q2:
```
Inner: SELECT ps_partkey, MIN(ps_supplycost)
       FROM partsupp JOIN supplier JOIN nation JOIN region
       WHERE ... GROUP BY ps_partkey
       → produces (ps_partkey, min_cost) pairs
Outer: ... WHERE ps_supplycost = min_cost
       AND ps_partkey = inner.ps_partkey
```

This requires multi-phase execution (run inner first to materialize
the derived table), then join the outer query against the materialized
result. The materialized result is a hash map keyed by correlation key.

**Strategy B: Flatten into a single join with aggregation.**
If the inner query's tables don't overlap with the outer query's
tables, decorrelate the entire subquery into additional join nodes
(like EXISTS), with the aggregation running on the inner sub-tree.
The comparison `= MIN(...)` becomes a post-join HAVING-like filter.

This requires SPJ to support aggregation on a sub-tree (not just the
leaf), which is not currently possible.

Strategy A is more feasible with existing infrastructure.

### Sub-steps

- **43a.** Detect correlated scalar subquery with aggregation in
  `SubqueryAnalyzer`. Identify: correlation columns, inner aggregate
  function, inner GROUP BY (implicit from correlation columns).

- **43b.** Rewrite inner query: add correlation columns to GROUP BY
  (if not already there). The inner query becomes an uncorrelated
  aggregate query producing `(key, agg_value)` rows.

- **43c.** Execute inner query via multi-phase (Step 37 orchestrator),
  collect results into a `(correlation_key → scalar_result)` hash map.

- **43d.** For the outer query: add a client-side filter that looks up
  each outer row's correlation key in the hash map and compares against
  the aggregate result. This is applied after the outer scan/join but
  before aggregation.

- **43e.** Alternative: if the result set is small, substitute as an
  IN-list filter (for equality comparison). For Q2:
  `WHERE (ps_partkey, ps_supplycost) IN materialized_set`.

- **43f.** MTR tests:
  - Basic: `WHERE col = (SELECT MIN(x) FROM t2 WHERE t2.fk = t1.pk)`
  - Q2-like: multi-table inner query with MIN aggregate
  - No match: inner query produces NULL for some keys

### Files to modify
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `storage/ndb/src/ronsql/RonSQLPreparer.hpp`
- `mysql-test/suite/ronsql/t/ronsql_subquery.test`

### Target query: TPC-H Q2 (simplified)

```sql
SELECT s_name, p_partkey, ps_supplycost
FROM part
JOIN partsupp ON ps_partkey = p_partkey
JOIN supplier ON s_suppkey = ps_suppkey
WHERE ps_supplycost = (
    SELECT MIN(ps_supplycost)
    FROM partsupp ps2
    JOIN supplier s2 ON s2.s_suppkey = ps2.ps_suppkey
    JOIN nation ON n_nationkey = s2.s_nationkey
    JOIN region ON r_regionkey = n_regionkey
    WHERE ps2.ps_partkey = p_partkey
      AND r_name = 'EUROPE')
ORDER BY p_partkey;
```

---

## Step 44: Cross-Column Comparison in WHERE Pushdown

### Problem
TPC-H Q4's inner query has `l_commitdate < l_receiptdate` — a
comparison between two columns of the same table. RonSQL's WHERE
pushdown currently only handles column-vs-constant comparisons.

### Approach
NDB interpreter supports column-vs-column comparison via
`branch_col_eq_col`, `branch_col_lt_col`, etc. (in NdbInterpretedCode).
Extend `compile_where_filter()` to detect column-vs-column comparisons
and emit the appropriate interpreter instructions.

### Sub-steps

- **44a.** In `ConditionalExpression` comparison handling: detect when
  both sides of a comparison are column references (currently assumes
  one side is a constant).

- **44b.** In `compile_filter()` / NdbScanFilter construction: use
  `NdbScanFilter::cmp_col()` or raw `NdbInterpretedCode::branch_col_*_col()`
  for column-vs-column comparisons.

- **44c.** Verify the inverted inequality behavior (documented in
  CLAUDE.md) applies to column-vs-column variants too.

- **44d.** MTR tests:
  - `WHERE t.col1 < t.col2` — same-table column comparison
  - `WHERE t.col1 = t.col2` — equality (EQ is not inverted)
  - Combined with other conditions: `WHERE t.col1 < t.col2 AND t.col3 > 5`

### Files to modify
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `mysql-test/suite/ronsql/t/ronsql_subquery.test`

---

## Step 45: Non-Aggregate (Projection-Only) Query Execution

### Problem
RonSQL currently rejects any query without aggregate functions with
`"Not an aggregate query."` (RonSQLPreparer.cpp line ~404). Subquery
inner queries often return raw column values without aggregation, e.g.,
`WHERE col IN (SELECT x FROM t2 WHERE y > 5)` — the inner SELECT
returns plain `x` values, not `SUM(x)` or `COUNT(*)`.

Beyond subqueries, projection-only execution is also useful for
standalone queries (`SELECT col1, col2 FROM t WHERE ...`) and derived
tables (Phase 19).

### Approach
Add an execution path that bypasses the AggregationAPICompiler and
NdbAggregator, returning raw scan/lookup rows directly. The key
changes:

1. **Remove the aggregate-only gate**: in `load()`, allow queries
   with no aggregate outputs to proceed instead of throwing an error.

2. **Non-aggregate scan execution**: execute a plain `NdbScanOperation`
   (or `NdbIndexScanOperation`) without calling `setAggregationCode()`
   or `DoAggregation()`. Iterate results with `nextResult()` and
   output each row directly.

3. **Non-aggregate join execution**: execute via `NdbQueryBuilder` /
   `NdbQuery` without setting aggregation options on the leaf
   operation. Iterate `NdbQuery::nextResult()` and read column values
   from the leaf (or all) operations via `getValue()`.

4. **Result output**: reuse the existing output formatting (TEXT/JSON)
   but emit one row per scan result instead of one row per aggregate
   group.

### Sub-steps

- **45a.** Remove the `"Not an aggregate query"` rejection in
  `load()`. Instead, set a flag (`m_is_aggregate = false`) and skip
  `AggregationAPICompiler` initialization for non-aggregate queries.

- **45b.** Add `execute_projection()` for single-table non-aggregate
  scans: open scan, iterate `nextResult()`, output each row. Handle
  WHERE filter pushdown (existing `setInterpretedCode` path works
  unchanged).

- **45c.** Add non-aggregate path in `execute_join()`: build the
  NdbQuery tree without aggregation options. For each `nextResult()`,
  read column values from the appropriate query operations and output.

- **45d.** Support ORDER BY and LIMIT for non-aggregate queries
  (client-side sort + row count limit, reusing existing logic).

- **45e.** MTR tests:
  - `SELECT col1, col2 FROM t WHERE col1 > 5` — single-table projection
  - `SELECT o.o_id, l.l_quantity FROM orders o JOIN lineitem l
    ON l.l_orderkey = o.o_id WHERE o.o_custkey = 100` — join projection
  - `SELECT x FROM t ORDER BY x LIMIT 10` — with ORDER BY + LIMIT
  - Verify aggregate queries still work unchanged

### Files to modify
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `storage/ndb/src/ronsql/RonSQLPreparer.hpp` (if new methods/flags needed)
- `mysql-test/suite/ronsql/t/ronsql_subquery.test` (or new test file)

### Design considerations

For subquery inner queries specifically, Step 39 could work around the
lack of projection-only execution by wrapping the inner query as
`SELECT col FROM t GROUP BY col` (effectively a DISTINCT). This
produces the same value set via the existing aggregate path. However,
this workaround has overhead (hash table, group tracking) and doesn't
generalize. Step 45 provides the clean solution.

The non-aggregate path also enables standalone projection queries as a
first-class RonSQL feature, which is valuable independently of
subqueries.

GROUP BY without aggregation (`SELECT col FROM t GROUP BY col`) is a
distinct feature (DISTINCT semantics) that can continue to use the
aggregate path. Step 45 is specifically for queries with NO GROUP BY
and NO aggregate functions.

---

## Implementation Status

### Completed Steps

| Step | Description | Commit | Status |
|------|-------------|--------|--------|
| 36 | Parser/AST infrastructure | 067d705289c (partial) | DONE |
| 37 | Multi-phase execution orchestrator | 72f558ff153 | DONE |
| 38 | Uncorrelated scalar subqueries | d84a94c8660 | DONE |
| 39 | Uncorrelated IN subqueries | 067d705289c | DONE |
| 40 | Semi-join/anti-join in QueryPlanner | 5f8b975a14e | DONE (dead code — see note) |
| 41 | Correlated EXISTS → IN transformation | 753d6140ee6 | DONE |
| 44 | Cross-column comparison | 067d705289c | DONE |

**Note on Step 40**: The MatchFirst/MatchNullOnly semi-join infrastructure
was added to QueryPlanner but is currently dead code. Step 41 discovered
that MatchFirst doesn't work with aggregation pushdown because
`handleJoinAggRow` in DbtupExecQuery.cpp bypasses `sendReadAttrinfo`
where FirstMatch is checked. EXISTS subqueries use IN-subquery
transformation instead. The semi-join infrastructure may be useful for
non-aggregate queries in the future.

### Remaining Steps (in implementation order)

| Step | Description | Status |
|------|-------------|--------|
| 42 | NOT EXISTS → NOT IN transformation | NOT STARTED |
| 43 | Correlated scalar subquery with aggregation | NOT STARTED |
| 45 | Non-aggregate (projection-only) query execution | NOT STARTED |

Steps 42 and 43 continue using the MIN()/GROUP BY workaround for the
aggregate-only restriction. Step 45 removes that restriction afterwards,
enabling standalone projection queries and cleaner subquery inner queries.
Once Step 45 is done, Steps 41/42's EXISTS/NOT EXISTS transformations could
optionally be simplified to drop the MIN() wrapper.

---

## Step 42: NOT EXISTS → NOT IN Transformation (Revised)

### Original Plan
Decorrelate NOT EXISTS to anti-join with MatchNullOnly.

### Revised Approach
Same as Step 41's revised approach: transform NOT EXISTS into NOT IN
using the multi-phase execution infrastructure.

`WHERE NOT EXISTS (SELECT ... FROM inner WHERE inner.col = outer.col AND filter)`
becomes:
`WHERE outer.col NOT IN (SELECT MIN(inner.col) FROM inner WHERE filter GROUP BY inner.col)`

### Challenges

**NULL semantics**: NOT IN has three-valued logic. If the subquery result
set contains NULL, `x NOT IN (NULL, 1, 2)` is UNKNOWN (not TRUE) for all
x values. This is standard SQL but tricky to implement correctly.

For RonSQL's current use case (integer primary/foreign keys that are NOT
NULL), this is not a practical concern. Two options:

1. **Simple**: Require NOT NULL on the correlation columns. Reject with
   error if nullability detected.
2. **Full**: Implement three-valued NOT IN logic: if any inner value is
   NULL, the entire NOT IN evaluates to FALSE for all outer rows.

Option 1 is recommended for initial implementation.

### Implementation
1. In `decorrelate_exists()`: detect NOT EXISTS (T_NOT → T_EXISTS)
2. Synthesize inner SQL same as EXISTS case
3. Create `I_NOT_IN_SUBQUERY` CE node (new node type, or use I_IN_SUBQUERY
   with a negation flag)
4. In `substitute_subquery_results_ce()`: for NOT IN, build AND-chain of
   inequalities instead of OR-chain of equalities. Or equivalently, wrap
   the OR-chain in T_NOT.

### Files to modify
- `storage/ndb/src/ronsql/RonSQLCommon.hpp` — new node type or flag
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` — decorrelate_exists(), substitute
- `mysql-test/suite/ronsql/t/ronsql_subquery.test` — NOT EXISTS tests
- `mysql-test/suite/ronsql/r/ronsql_subquery.result` — re-record

---

## Step 43: Correlated Scalar Subquery with Aggregation (Revised)

### Original Plan
Decorrelate into a derived-table join or flatten into a single join with
aggregation on a sub-tree.

### Revised Approach
Use multi-phase execution with decorrelation into an uncorrelated
aggregate query, similar to Steps 41-42's approach.

`WHERE col = (SELECT MIN(x) FROM t2 WHERE t2.fk = t1.pk AND ...)`
becomes:
1. Run inner as uncorrelated: `SELECT pk, MIN(x) FROM t2 WHERE ... GROUP BY pk`
   → produces (correlation_key, scalar_result) pairs
2. Materialize into a hash map: `correlation_key → scalar_result`
3. In the outer query, substitute as a client-side filter: for each outer
   row, look up `t1.pk` in the hash map and compare `col` against the result

### Challenges

1. **Hash map storage**: Must define memory limits. Reject if result exceeds
   N entries (e.g., 10000).
2. **Client-side filter**: The comparison cannot be pushed into NdbScanFilter
   because it depends on materialized data. Must filter after scan results
   arrive but before aggregation. This requires a new execution path.
3. **Multi-table inner query**: TPC-H Q2's inner is a 4-table join. The
   inner query must be decorrelated (remove correlation predicate, add
   correlation column to GROUP BY) and then run as a standalone join query
   through RonSQL's existing join execution path.

### Alternative: Simple EXISTS-like Transformation
For equality comparisons (`col = (SELECT MIN(x) ...)`), an alternative is:
`WHERE (col, pk) IN (SELECT MIN(x), pk FROM t2 ... GROUP BY pk)`
This reuses the existing IN-subquery infrastructure but requires tuple-IN
support (comparing multiple columns). Tuple-IN is simpler than client-side
hash map filtering but requires parser/AST changes for tuple comparison.

### Files to modify
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `storage/ndb/src/ronsql/RonSQLPreparer.hpp`
- `mysql-test/suite/ronsql/t/ronsql_subquery.test`

### Target query: TPC-H Q2 (simplified)
```sql
SELECT s_name, p_partkey, ps_supplycost
FROM part
JOIN partsupp ON ps_partkey = p_partkey
JOIN supplier ON s_suppkey = ps_suppkey
WHERE ps_supplycost = (
    SELECT MIN(ps_supplycost)
    FROM partsupp ps2
    JOIN supplier s2 ON s2.s_suppkey = ps2.ps_suppkey
    WHERE ps2.ps_partkey = p_partkey)
ORDER BY p_partkey;
```

---

## Open Questions

1. **NOT IN NULL semantics**: Reject nullable columns or implement full
   three-valued logic? See Step 42 discussion.

2. **Materialization memory limits**: Step 43 materializes inner query
   results into a hash map. Define limits and error behavior.

3. **Nested subqueries**: Grammar allows recursive nesting. Single-level
   is sufficient for all TPC-H queries. Continue rejecting nested.

4. **Semi-join revival**: If a non-aggregate RonSQL execution path is
   added (plain SELECT without GROUP BY/aggregation), the MatchFirst
   semi-join approach from Step 40 could work. The incompatibility is
   specifically with aggregation pushdown (handleJoinAggRow bypasses
   FirstMatch check).

## Verification

After each step:
```bash
cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
cd mysql-test && perl ./mtr --suite=ronsql ronsql_join ronsql_join_agg ronsql_subquery ronsql_basic
```

## Future Extensions (Beyond Phase 7)

- **Derived tables (FROM subquery)**: `FROM (SELECT ... GROUP BY ...)
  AS derived JOIN t2 ON ...` — uses the multi-phase orchestrator from
  Step 37 to materialize the derived table, then joins against it.
  This is the RonSQL equivalent of RONDB-733 Phase 19.

- **NOT IN with NULL semantics**: Proper three-valued logic for
  `NOT IN (SELECT ...)` when the subquery can return NULL values.

- **Subqueries in SELECT list**: `SELECT col, (SELECT COUNT(*)
  FROM t2 WHERE t2.fk = t1.pk) AS cnt FROM t1` — correlated scalar
  subquery in the output. Requires per-row execution or decorrelation
  to a left-join with aggregation.

- **Multiple correlation predicates**: Step 41's EXISTS→IN transformation
  currently supports a single correlation predicate. Extending to multiple
  predicates (e.g., `WHERE inner.a = outer.a AND inner.b = outer.b`)
  requires tuple-IN support or a different transformation strategy.
