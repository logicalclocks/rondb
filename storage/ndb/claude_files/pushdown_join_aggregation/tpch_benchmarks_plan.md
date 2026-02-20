# TPC-H NDB API Benchmark Implementation Plan

This document specifies 10 TPC-H benchmark tests using the NDB API with
pushdown join aggregation (NdbQueryBuilder + NdbAggregator). Each benchmark
is a standalone .cpp file in `storage/ndb/block_unit_test/`.

All benchmarks follow the pattern established by `bench_q9_ndbapi.cpp`.

---

## Table of Contents

1. [Common Structure](#common-structure)
2. [CMakeLists.txt Additions](#cmakeliststxt-additions)
3. [Implementation Order](#implementation-order)
4. [Benchmark 1: bench_q4_ndbapi](#benchmark-1-bench_q4_ndbapi)
5. [Benchmark 2: bench_minmax_ndbapi](#benchmark-2-bench_minmax_ndbapi)
6. [Benchmark 3: bench_q3_ndbapi](#benchmark-3-bench_q3_ndbapi)
7. [Benchmark 4: bench_nogroup_ndbapi](#benchmark-4-bench_nogroup_ndbapi)
8. [Benchmark 5: bench_q5_ndbapi](#benchmark-5-bench_q5_ndbapi)
9. [Benchmark 6: bench_q2_ndbapi](#benchmark-6-bench_q2_ndbapi)
10. [Benchmark 7: bench_q10_ndbapi](#benchmark-7-bench_q10_ndbapi)
11. [Benchmark 8: bench_q11_ndbapi](#benchmark-8-bench_q11_ndbapi)
12. [Benchmark 9: bench_orderscan_ndbapi](#benchmark-9-bench_orderscan_ndbapi)
13. [Benchmark 10: bench_datescan_ndbapi](#benchmark-10-bench_datescan_ndbapi)

---

## Common Structure

Every benchmark follows the identical skeleton from `bench_q9_ndbapi.cpp`:

### Includes

```cpp
#include <ndb_global.h>
#include <ndb_opts.h>
#include <NdbApi.hpp>
#include <NdbSleep.h>
#include <NdbAggregator.hpp>
#include <ndbapi/NdbAggregationCommon.hpp>
#include "NdbQueryBuilder.hpp"
#include "NdbQueryOperation.hpp"
#include <mysql.h>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <set>
#include <string>
```

### Globals and Helpers

- `static bool verbose` + `V(...)` macro
- `Clock`, `TimePoint`, `elapsedMs()` for timing
- `connectMysql(int port)` returning `MYSQL*`

### main() Function

1. Parse CLI args: `-c <connect_string>`, `-m <mysql_port>`, `--iterations <N>`, `-v`/`--verbose`
2. Print banner with benchmark name, connect string, port, iterations
3. `ndb_init()`
4. Scoped block: `Ndb_cluster_connection` + `connect()` + `wait_until_ready()`
5. `connectMysql()` for verification queries
6. Inner scoped block: `Ndb ndb(&clusterConn, "test")` + `ndb.init()`
7. Loop `numIterations` times calling `runBenchmark(&ndb, mysqlConn, iter)`
8. Close MySQL, end scope for Ndb objects
9. `ndb_end(0)`

### runBenchmark() Function

1. **Dictionary lookup**: `invalidateTable()` + `getTable()` for each table
2. **Column lookup**: `getColumn()` for columns needed in aggregation and filters
3. **Build NdbAggregator program**: `GroupBy`/`GroupByLinked`/`LoadColumn`/`LoadLinkedColumn`/arithmetic/agg/`Finalize`
4. **Build NdbInterpretedCode filter** (if applicable): branch/exit_ok/exit_nok/finalise
5. **Build NdbQueryBuilder**: `create()`, define operations with `scanTable`/`scanIndex`/`readTuple`, set parents, linked projections, aggregation on leaf, `prepare(ndb)`
6. **Execute**: `startTransaction()`, `createQuery()`, `execute(NoCommit)`, loop `nextResult(true)` consuming rows
7. **Fetch results**: `getAggregator()`, loop `FetchResultRecord()`/`FetchGroupbyColumn()`/`FetchAggregationResult()`
8. **MySQL verification**: Run equivalent SQL, `mysql_store_result()`, compare NDB vs SQL results with tolerance
9. **Print timing**: Prepare+Execute, Scan+Join, Results, Total, SQL query
10. **Cleanup**: `query->close()`, `trans->close()`, `queryDef->destroy()`

### Do NOT Call getValue()

Critical: Never call `getValue()` on any NdbQueryOperation in an aggregate query.
FLUSH_AI is suppressed for non-leaf aggregate nodes, so PI_ATTR_LIST columns
from getValue() shift col() indices for child key patterns, causing lookup failures.

### CHAR Column Handling in Results

When reading CHAR(N) GROUP BY columns from `FetchGroupbyColumn()`:
- `byte_size()` returns N (full column width)
- Use `strnlen(ptr, byteSize)` to find effective string length
- Trim trailing spaces to match MySQL behavior

### NdbInterpretedCode Inverted Branch Semantics

For inequality comparisons (documented in CLAUDE.md MEMORY.md):
- `branch_col_lt` actually branches when `col > val`
- `branch_col_le` actually branches when `col >= val`
- `branch_col_gt` actually branches when `col < val`
- `branch_col_ge` actually branches when `col <= val`
- `branch_col_eq` and `branch_col_ne` are correct (not inverted)

### Comparison Tolerance for Verification

NDB uses DOUBLE internally for aggregation; MySQL DECIMAL(15,2) has higher
precision. Use tolerance: `max(0.01, fabs(sql_value) * 1e-9)`.

---

## CMakeLists.txt Additions

Add these lines to `storage/ndb/block_unit_test/CMakeLists.txt`, after the
existing `bench_q9_ndbapi` line (line 55). All 10 new targets need the
`NdbQueryBuilder.hpp` include path already added at line 53.

```cmake
NDB_ADD_EXECUTABLE(bench_q4_ndbapi bench_q4_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_minmax_ndbapi bench_minmax_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_q3_ndbapi bench_q3_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_nogroup_ndbapi bench_nogroup_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_q5_ndbapi bench_q5_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_q2_ndbapi bench_q2_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_q10_ndbapi bench_q10_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_q11_ndbapi bench_q11_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_orderscan_ndbapi bench_orderscan_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
NDB_ADD_EXECUTABLE(bench_datescan_ndbapi bench_datescan_ndbapi.cpp NDBTEST NDBCLIENT MYSQLCLIENT)
```

These must appear after the `INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/storage/ndb/src/ndbapi)` line (line 53), which provides the `NdbQueryBuilder.hpp` header path.

---

## Implementation Order

Build benchmarks in order of increasing complexity:

1. **bench_q4_ndbapi** (2 tables, COUNT only, no filter, 5 groups) — simplest possible
2. **bench_minmax_ndbapi** (2 tables, all 4 agg types, no filter, 25 groups) — tests MIN/MAX/COUNT/SUM
3. **bench_nogroup_ndbapi** (3 tables, no GROUP BY, CHAR filter) — tests global aggregation
4. **bench_q3_ndbapi** (3 tables, SUM with arithmetic, CHAR filter, GROUP BY) — combines filter + computation
5. **bench_q11_ndbapi** (3 tables, SUM with MUL, CHAR filter, high-cardinality INT GROUP BY) — stress test group count
6. **bench_q10_ndbapi** (4 tables, SUM + COUNT, VARCHAR GROUP BY) — high-cardinality VARCHAR groups
7. **bench_q5_ndbapi** (5 tables, SUM, CHAR filter, deep chain) — deep join tree
8. **bench_q2_ndbapi** (5 tables, all 4 agg types, INT inequality filter) — inequality filter semantics
9. **bench_orderscan_ndbapi** (2 tables, ordered index scan, INT range bounds) — first scanIndex test
10. **bench_datescan_ndbapi** (2 tables, ordered index scan, DATE range bounds) — DATE type handling

---

## Benchmark 1: bench_q4_ndbapi

**Goal**: Simplest possible benchmark. 2-table join, COUNT only, CHAR(15) GROUP BY, 5 groups.

### SQL Query

```sql
SELECT o.o_orderpriority, COUNT(*)
FROM tpch_lineitem l
  JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
GROUP BY o.o_orderpriority
ORDER BY o.o_orderpriority
```

### Join Tree

```
Node 0: lineitem  (scanTable, root)
  +- Node 1: orders  (readTuple, key: lineitem.l_orderkey, aggregate leaf)
```

### Tables

- `tpch_lineitem`: root scan, provides `l_orderkey` for join
- `tpch_orders`: lookup child, provides `o_orderpriority` for GROUP BY

### Aggregation (NdbAggregator on orders)

Local table: `tpch_orders`

**GROUP BY columns:**
- `o_orderpriority` (local CHAR(15), position in orders table)

**Linked projections on orders:** None needed. All GROUP BY columns are local to
the leaf table, and there are no linked columns needed for computation.

**Program:**
```
GroupBy("o_orderpriority")          // GROUP BY o_orderpriority (local)
LoadUint64(1, reg 0)               // reg0 = 1 (dummy for COUNT)
Count(0, 0)                        // agg[0] = COUNT(reg0)
Finalize()
```

### NdbInterpretedCode Filter

None.

### NdbQueryBuilder Construction

```
qb = NdbQueryBuilder::create()

// Node 0: scan lineitem (root)
lineitemOp = qb->scanTable(lineitemTab)

// Node 1: lookup orders (key: lineitem.l_orderkey, aggregate leaf)
ordersKey[] = { qb->linkedValue(lineitemOp, "l_orderkey"), nullptr }
ordersOpts.setMatchType(MatchNonNull)
ordersOpts.setAggregation(agg)
ordersOp = qb->readTuple(ordersTab, ordersKey, &ordersOpts)

queryDef = qb->prepare(ndb)
```

### Result Retrieval

```
rec.FetchGroupbyColumn()   -> o_orderpriority CHAR(15): trim spaces
rec.FetchAggregationResult() -> agg[0] COUNT: data_int64()
```

Map key: `std::string` (o_orderpriority). Map value: `int64_t` (count).

### MySQL Verification Query

```sql
SELECT o.o_orderpriority, COUNT(*)
FROM tpch_lineitem l
JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
GROUP BY o.o_orderpriority
ORDER BY o.o_orderpriority
```

Compare: 5 groups, exact integer match for COUNT.

### Expected Groups

5 groups: "1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"

---

## Benchmark 2: bench_minmax_ndbapi

**Goal**: Test all 4 aggregation types (MIN, MAX, COUNT, SUM) on DECIMAL column.
2-table join, CHAR(25) GROUP BY, 25 groups.

### SQL Query

```sql
SELECT n.n_name,
       MIN(s.s_acctbal), MAX(s.s_acctbal),
       COUNT(*), SUM(s.s_acctbal)
FROM tpch_supplier s
  JOIN tpch_nation n ON n.n_nationkey = s.s_nationkey
GROUP BY n.n_name
ORDER BY n.n_name
```

### Join Tree

```
Node 0: supplier  (scanTable, root)
  +- Node 1: nation   (readTuple, key: supplier.s_nationkey, aggregate leaf)
```

### Tables

- `tpch_supplier`: root scan, provides `s_nationkey` for join, `s_acctbal` via linked projection
- `tpch_nation`: lookup child, provides `n_name` for GROUP BY

### Aggregation (NdbAggregator on nation)

Local table: `tpch_nation`

**GROUP BY columns:**
- `n_name` (local CHAR(25))

**Linked projections on nation:**
- pos 0: `linkedValue(supplierOp, "s_acctbal")` -> MIN/MAX/SUM computation

**Column lookups needed:**
- `s_acctbal` from `tpch_supplier` (DECIMAL(15,2)) -> NdbDictionary::Column pointer for LoadLinkedColumn

**Program:**
```
GroupBy("n_name")                              // GROUP BY n_name (local)
LoadLinkedColumn(0, reg 0, s_acctbalCol)       // reg0 = s_acctbal
Min(0, 0)                                      // agg[0] = MIN(reg0)
Max(1, 0)                                      // agg[1] = MAX(reg0)
LoadUint64(1, reg 1)                           // reg1 = 1 (dummy for COUNT)
Count(2, 1)                                    // agg[2] = COUNT(reg1)
Sum(3, 0)                                      // agg[3] = SUM(reg0)
Finalize()
```

Note: reg0 still holds s_acctbal after Min/Max (they don't modify the register).
But to be safe, the COUNT uses a separate register. Actually, we can reuse reg0
for Sum since Min/Max only read the register. Let us verify: the aggregation
instructions Sum/Min/Max/Count all READ from the register. They don't modify it.
So we can safely do:

```
GroupBy("n_name")
LoadLinkedColumn(0, reg 0, s_acctbalCol)   // reg0 = s_acctbal
Min(0, 0)                                  // agg[0] = MIN(reg0)
Max(1, 0)                                  // agg[1] = MAX(reg0)
Count(2, 0)                                // agg[2] = COUNT(reg0)
Sum(3, 0)                                  // agg[3] = SUM(reg0)
Finalize()
```

### NdbInterpretedCode Filter

None.

### NdbQueryBuilder Construction

```
qb = NdbQueryBuilder::create()

// Node 0: scan supplier (root)
supplierOp = qb->scanTable(supplierTab)

// Node 1: lookup nation (key: supplier.s_nationkey, aggregate leaf)
nationKey[] = { qb->linkedValue(supplierOp, "s_nationkey"), nullptr }
nationOpts.setMatchType(MatchNonNull)
nationOpts.setAggregation(agg)

link0 = qb->linkedValue(supplierOp, "s_acctbal")   // pos 0
nationOpts.addLinkedProjection(link0)

nationOp = qb->readTuple(nationTab, nationKey, &nationOpts)

queryDef = qb->prepare(ndb)
```

### Result Retrieval

```
rec.FetchGroupbyColumn()     -> n_name CHAR(25): trim spaces
rec.FetchAggregationResult() -> agg[0] MIN: data_double()
rec.FetchAggregationResult() -> agg[1] MAX: data_double()
rec.FetchAggregationResult() -> agg[2] COUNT: data_int64()
rec.FetchAggregationResult() -> agg[3] SUM: data_double()
```

Map key: `std::string` (n_name). Map value: struct { double min, max, sum; int64_t count }.

### MySQL Verification Query

```sql
SELECT n.n_name, MIN(s.s_acctbal), MAX(s.s_acctbal),
       COUNT(*), SUM(s.s_acctbal)
FROM tpch_supplier s
JOIN tpch_nation n ON n.n_nationkey = s.s_nationkey
GROUP BY n.n_name
ORDER BY n.n_name
```

### Expected Groups

25 groups (one per nation).

---

## Benchmark 3: bench_q3_ndbapi

**Goal**: 3-table join with SUM of arithmetic expression, CHAR equality filter,
compound GROUP BY.

### SQL Query

```sql
SELECT o.o_orderyear, o.o_orderpriority,
       SUM(l.l_extendedprice * (1 - l.l_discount)),
       COUNT(*)
FROM tpch_lineitem l
  JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
  JOIN tpch_customer c ON c.c_custkey = o.o_custkey
WHERE c.c_mktsegment = 'BUILDING'
GROUP BY o.o_orderyear, o.o_orderpriority
ORDER BY o.o_orderyear, o.o_orderpriority
```

### Join Tree

```
Node 0: lineitem  (scanTable, root)
  +- Node 1: orders    (readTuple, key: lineitem.l_orderkey)
       +- Node 2: customer  (readTuple, key: orders.o_custkey,
                              filter: c_mktsegment = 'BUILDING',
                              setParent(orders), aggregate leaf)
```

### Tables

- `tpch_lineitem`: root scan, provides `l_orderkey`, `l_extendedprice`, `l_discount`
- `tpch_orders`: lookup, provides `o_orderkey` (PK), `o_custkey` for next join, `o_orderyear`+`o_orderpriority` for GROUP BY
- `tpch_customer`: lookup leaf, provides `c_mktsegment` for filter

### Aggregation (NdbAggregator on customer)

Local table: `tpch_customer`

**GROUP BY columns:**
- `o_orderyear` (linked, INT) from orders — position 0 in linked projections
- `o_orderpriority` (linked, CHAR(15)) from orders — position 1 in linked projections

**Linked projections on customer:**
- pos 0: `linkedValue(ordersOp, "o_orderyear")` -> GROUP BY
- pos 1: `linkedValue(ordersOp, "o_orderpriority")` -> GROUP BY
- pos 2: `linkedValue(lineitemOp, "l_extendedprice")` -> computation
- pos 3: `linkedValue(lineitemOp, "l_discount")` -> computation

**Column lookups needed:**
- `o_orderyear` from `tpch_orders` (INT)
- `o_orderpriority` from `tpch_orders` (CHAR(15))
- `l_extendedprice` from `tpch_lineitem` (DECIMAL(15,2))
- `l_discount` from `tpch_lineitem` (DECIMAL(15,2))

**Program:**
```
GroupByLinked(0, orderyearCol)                   // GROUP BY o_orderyear (linked pos 0)
GroupByLinked(1, orderpriorityCol)               // GROUP BY o_orderpriority (linked pos 1)
LoadLinkedColumn(2, reg 0, extendedpriceCol)     // reg0 = l_extendedprice
LoadDouble(1.0, reg 1)                           // reg1 = 1.0
LoadLinkedColumn(3, reg 2, discountCol)          // reg2 = l_discount
Minus(1, 2)                                      // reg1 = 1 - discount
Mul(0, 1)                                        // reg0 = price * (1-disc)
Sum(0, 0)                                        // agg[0] = SUM(reg0)
LoadUint64(1, reg 1)                             // reg1 = 1
Count(1, 1)                                      // agg[1] = COUNT
Finalize()
```

### NdbInterpretedCode Filter (on customer)

Filter: `c_mktsegment = 'BUILDING'`. CHAR(10) column must be space-padded to 10 chars.

```cpp
NdbInterpretedCode filter(customerTab);
static const char BUILDING_PADDED[10] = {'B','U','I','L','D','I','N','G',' ',' '};
Uint32 mktAttrId = customerTab->getColumn("c_mktsegment")->getColumnNo();
filter.branch_col_eq(BUILDING_PADDED, 10, mktAttrId, 0);  // EQ is correct (not inverted)
filter.interpret_exit_nok();       // fall-through: not BUILDING -> reject
filter.def_label(0);
filter.interpret_exit_ok();        // equal -> accept
filter.finalise();
```

### NdbQueryBuilder Construction

```
qb = NdbQueryBuilder::create()

// Node 0: scan lineitem (root)
lineitemOp = qb->scanTable(lineitemTab)

// Node 1: lookup orders (key: lineitem.l_orderkey)
ordersKey[] = { qb->linkedValue(lineitemOp, "l_orderkey"), nullptr }
ordersOpts.setMatchType(MatchNonNull)
ordersOp = qb->readTuple(ordersTab, ordersKey, &ordersOpts)

// Node 2: lookup customer (key: orders.o_custkey, parent: orders, filter, aggregate leaf)
customerKey[] = { qb->linkedValue(ordersOp, "o_custkey"), nullptr }
customerOpts.setMatchType(MatchNonNull)
customerOpts.setParent(ordersOp)
customerOpts.setInterpretedCode(filter)
customerOpts.setAggregation(agg)

link0 = qb->linkedValue(ordersOp, "o_orderyear")        // pos 0: GROUP BY
link1 = qb->linkedValue(ordersOp, "o_orderpriority")    // pos 1: GROUP BY
link2 = qb->linkedValue(lineitemOp, "l_extendedprice")  // pos 2: computation
link3 = qb->linkedValue(lineitemOp, "l_discount")       // pos 3: computation

customerOpts.addLinkedProjection(link0)
customerOpts.addLinkedProjection(link1)
customerOpts.addLinkedProjection(link2)
customerOpts.addLinkedProjection(link3)

customerOp = qb->readTuple(customerTab, customerKey, &customerOpts)
queryDef = qb->prepare(ndb)
```

### Result Retrieval

```
rec.FetchGroupbyColumn()     -> o_orderyear INT (linked): data_int32()
rec.FetchGroupbyColumn()     -> o_orderpriority CHAR(15) (linked): trim spaces
rec.FetchAggregationResult() -> agg[0] SUM(revenue): data_double()
rec.FetchAggregationResult() -> agg[1] COUNT: data_int64()
```

Map key: `std::pair<int, std::string>` (year, priority). Map value: struct { double sum; int64_t count }.

### MySQL Verification Query

```sql
SELECT o.o_orderyear, o.o_orderpriority,
       SUM(l.l_extendedprice * (1 - l.l_discount)), COUNT(*)
FROM tpch_lineitem l
JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
JOIN tpch_customer c ON c.c_custkey = o.o_custkey
WHERE c.c_mktsegment = 'BUILDING'
GROUP BY o.o_orderyear, o.o_orderpriority
ORDER BY o.o_orderyear, o.o_orderpriority
```

### Expected Groups

~35 groups (7 years x 5 priorities, but not all combinations may exist depending
on data distribution).

---

## Benchmark 4: bench_nogroup_ndbapi

**Goal**: Global aggregation (no GROUP BY), 5 aggregate results, CHAR filter.
Tests the non-GROUP-BY path.

### SQL Query

```sql
SELECT COUNT(*),
       SUM(l.l_extendedprice),
       SUM(l.l_quantity),
       MIN(l.l_extendedprice),
       MAX(l.l_extendedprice)
FROM tpch_lineitem l
  JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
  JOIN tpch_customer c ON c.c_custkey = o.o_custkey
WHERE c.c_mktsegment = 'AUTOMOBILE'
```

### Join Tree

```
Node 0: lineitem  (scanTable, root)
  +- Node 1: orders    (readTuple, key: lineitem.l_orderkey)
       +- Node 2: customer  (readTuple, key: orders.o_custkey,
                              filter: c_mktsegment = 'AUTOMOBILE',
                              setParent(orders), aggregate leaf)
```

### Tables

- `tpch_lineitem`: root scan, provides `l_orderkey`, `l_extendedprice`, `l_quantity`
- `tpch_orders`: lookup, provides `o_custkey` for next join
- `tpch_customer`: lookup leaf with filter

### Aggregation (NdbAggregator on customer)

Local table: `tpch_customer`

**GROUP BY columns:** None (global aggregation).

**Linked projections on customer:**
- pos 0: `linkedValue(lineitemOp, "l_extendedprice")` -> SUM/MIN/MAX
- pos 1: `linkedValue(lineitemOp, "l_quantity")` -> SUM

**Column lookups needed:**
- `l_extendedprice` from `tpch_lineitem` (DECIMAL(15,2))
- `l_quantity` from `tpch_lineitem` (DECIMAL(15,2))

**Program:**
```
LoadLinkedColumn(0, reg 0, extendedpriceCol)  // reg0 = l_extendedprice
LoadLinkedColumn(1, reg 1, quantityCol)       // reg1 = l_quantity
Count(0, 0)                                   // agg[0] = COUNT(*)
Sum(1, 0)                                     // agg[1] = SUM(l_extendedprice)
Sum(2, 1)                                     // agg[2] = SUM(l_quantity)
Min(3, 0)                                     // agg[3] = MIN(l_extendedprice)
Max(4, 0)                                     // agg[4] = MAX(l_extendedprice)
Finalize()
```

### NdbInterpretedCode Filter (on customer)

Filter: `c_mktsegment = 'AUTOMOBILE'`. CHAR(10) column, 10 chars exactly.

```cpp
NdbInterpretedCode filter(customerTab);
static const char AUTO_PADDED[10] = {'A','U','T','O','M','O','B','I','L','E'};
Uint32 mktAttrId = customerTab->getColumn("c_mktsegment")->getColumnNo();
filter.branch_col_eq(AUTO_PADDED, 10, mktAttrId, 0);
filter.interpret_exit_nok();
filter.def_label(0);
filter.interpret_exit_ok();
filter.finalise();
```

### NdbQueryBuilder Construction

```
qb = NdbQueryBuilder::create()

// Node 0: scan lineitem (root)
lineitemOp = qb->scanTable(lineitemTab)

// Node 1: lookup orders
ordersKey[] = { qb->linkedValue(lineitemOp, "l_orderkey"), nullptr }
ordersOpts.setMatchType(MatchNonNull)
ordersOp = qb->readTuple(ordersTab, ordersKey, &ordersOpts)

// Node 2: lookup customer (aggregate leaf, filter, no GROUP BY)
customerKey[] = { qb->linkedValue(ordersOp, "o_custkey"), nullptr }
customerOpts.setMatchType(MatchNonNull)
customerOpts.setParent(ordersOp)
customerOpts.setInterpretedCode(filter)
customerOpts.setAggregation(agg)

link0 = qb->linkedValue(lineitemOp, "l_extendedprice")  // pos 0
link1 = qb->linkedValue(lineitemOp, "l_quantity")        // pos 1
customerOpts.addLinkedProjection(link0)
customerOpts.addLinkedProjection(link1)

customerOp = qb->readTuple(customerTab, customerKey, &customerOpts)
queryDef = qb->prepare(ndb)
```

### Result Retrieval

No GROUP BY, so there is exactly 1 result record (the global aggregation).

```
rec.FetchAggregationResult() -> agg[0] COUNT: data_int64()
rec.FetchAggregationResult() -> agg[1] SUM(price): data_double()
rec.FetchAggregationResult() -> agg[2] SUM(qty): data_double()
rec.FetchAggregationResult() -> agg[3] MIN(price): data_double()
rec.FetchAggregationResult() -> agg[4] MAX(price): data_double()
```

Store as single struct with 5 values. Compare against single-row SQL result.

### MySQL Verification Query

```sql
SELECT COUNT(*), SUM(l.l_extendedprice), SUM(l.l_quantity),
       MIN(l.l_extendedprice), MAX(l.l_extendedprice)
FROM tpch_lineitem l
JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
JOIN tpch_customer c ON c.c_custkey = o.o_custkey
WHERE c.c_mktsegment = 'AUTOMOBILE'
```

### Expected Groups

1 (global aggregation, no groups).

---

## Benchmark 5: bench_q5_ndbapi

**Goal**: Deep 5-table join chain, CHAR filter on region, SUM of revenue,
GROUP BY nation name.

### SQL Query

```sql
SELECT n.n_name,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM tpch_lineitem l
  JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
  JOIN tpch_customer c ON c.c_custkey = o.o_custkey
  JOIN tpch_nation n ON n.n_nationkey = c.c_nationkey
  JOIN tpch_region r ON r.r_regionkey = n.n_regionkey
WHERE r.r_name = 'ASIA'
GROUP BY n.n_name
ORDER BY n.n_name
```

### Join Tree

```
Node 0: lineitem  (scanTable, root)
  +- Node 1: orders    (readTuple, key: lineitem.l_orderkey)
       +- Node 2: customer  (readTuple, key: orders.o_custkey,
       |                      setParent(orders))
            +- Node 3: nation    (readTuple, key: customer.c_nationkey,
            |                      setParent(customer))
                 +- Node 4: region    (readTuple, key: nation.n_regionkey,
                                       filter: r_name = 'ASIA',
                                       setParent(nation), aggregate leaf)
```

### Tables

- `tpch_lineitem`: root scan, provides `l_orderkey`, `l_extendedprice`, `l_discount`
- `tpch_orders`: lookup, join key `o_orderkey`, provides `o_custkey`
- `tpch_customer`: lookup, join key `c_custkey`, provides `c_nationkey`
- `tpch_nation`: lookup, join key `n_nationkey`, provides `n_name` (for GROUP BY via linked), `n_regionkey`
- `tpch_region`: lookup leaf, join key `r_regionkey`, filter on `r_name`

### Aggregation (NdbAggregator on region)

Local table: `tpch_region`

**GROUP BY columns:**
- `n_name` (linked, CHAR(25)) from nation — position 0 in linked projections

**Linked projections on region:**
- pos 0: `linkedValue(nationOp, "n_name")` -> GROUP BY
- pos 1: `linkedValue(lineitemOp, "l_extendedprice")` -> computation
- pos 2: `linkedValue(lineitemOp, "l_discount")` -> computation

**Column lookups needed:**
- `n_name` from `tpch_nation` (CHAR(25))
- `l_extendedprice` from `tpch_lineitem` (DECIMAL(15,2))
- `l_discount` from `tpch_lineitem` (DECIMAL(15,2))

**Program:**
```
GroupByLinked(0, n_nameCol)                      // GROUP BY n_name (linked pos 0)
LoadLinkedColumn(1, reg 0, extendedpriceCol)     // reg0 = l_extendedprice
LoadDouble(1.0, reg 1)                           // reg1 = 1.0
LoadLinkedColumn(2, reg 2, discountCol)          // reg2 = l_discount
Minus(1, 2)                                      // reg1 = 1 - discount
Mul(0, 1)                                        // reg0 = price * (1-disc) = revenue
Sum(0, 0)                                        // agg[0] = SUM(revenue)
Finalize()
```

### NdbInterpretedCode Filter (on region)

Filter: `r_name = 'ASIA'`. CHAR(25) column.

```cpp
NdbInterpretedCode filter(regionTab);
char ASIA_PADDED[25];
memset(ASIA_PADDED, ' ', 25);
memcpy(ASIA_PADDED, "ASIA", 4);
Uint32 rNameAttrId = regionTab->getColumn("r_name")->getColumnNo();
filter.branch_col_eq(ASIA_PADDED, 25, rNameAttrId, 0);
filter.interpret_exit_nok();
filter.def_label(0);
filter.interpret_exit_ok();
filter.finalise();
```

### NdbQueryBuilder Construction

```
qb = NdbQueryBuilder::create()

// Node 0: scan lineitem (root)
lineitemOp = qb->scanTable(lineitemTab)

// Node 1: lookup orders
ordersKey[] = { qb->linkedValue(lineitemOp, "l_orderkey"), nullptr }
ordersOpts.setMatchType(MatchNonNull)
ordersOp = qb->readTuple(ordersTab, ordersKey, &ordersOpts)

// Node 2: lookup customer (parent: orders)
customerKey[] = { qb->linkedValue(ordersOp, "o_custkey"), nullptr }
customerOpts.setMatchType(MatchNonNull)
customerOpts.setParent(ordersOp)
customerOp = qb->readTuple(customerTab, customerKey, &customerOpts)

// Node 3: lookup nation (parent: customer)
nationKey[] = { qb->linkedValue(customerOp, "c_nationkey"), nullptr }
nationOpts.setMatchType(MatchNonNull)
nationOpts.setParent(customerOp)
nationOp = qb->readTuple(nationTab, nationKey, &nationOpts)

// Node 4: lookup region (parent: nation, filter, aggregate leaf)
regionKey[] = { qb->linkedValue(nationOp, "n_regionkey"), nullptr }
regionOpts.setMatchType(MatchNonNull)
regionOpts.setParent(nationOp)
regionOpts.setInterpretedCode(filter)
regionOpts.setAggregation(agg)

link0 = qb->linkedValue(nationOp, "n_name")             // pos 0: GROUP BY
link1 = qb->linkedValue(lineitemOp, "l_extendedprice")  // pos 1: computation
link2 = qb->linkedValue(lineitemOp, "l_discount")       // pos 2: computation
regionOpts.addLinkedProjection(link0)
regionOpts.addLinkedProjection(link1)
regionOpts.addLinkedProjection(link2)

regionOp = qb->readTuple(regionTab, regionKey, &regionOpts)
queryDef = qb->prepare(ndb)
```

### Result Retrieval

```
rec.FetchGroupbyColumn()     -> n_name CHAR(25) (linked): trim spaces
rec.FetchAggregationResult() -> agg[0] SUM(revenue): data_double()
```

Map key: `std::string` (n_name). Map value: `double` (sum).

### MySQL Verification Query

```sql
SELECT n.n_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM tpch_lineitem l
JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
JOIN tpch_customer c ON c.c_custkey = o.o_custkey
JOIN tpch_nation n ON n.n_nationkey = c.c_nationkey
JOIN tpch_region r ON r.r_regionkey = n.n_regionkey
WHERE r.r_name = 'ASIA'
GROUP BY n.n_name
ORDER BY n.n_name
```

### Expected Groups

~5 groups (nations in ASIA: INDIA, INDONESIA, JAPAN, CHINA, VIETNAM).

---

## Benchmark 6: bench_q2_ndbapi

**Goal**: 5-table join with all 4 aggregation types, INT inequality filter,
GROUP BY region name.

### SQL Query

```sql
SELECT r.r_name,
       MIN(ps.ps_supplycost), MAX(ps.ps_supplycost),
       SUM(ps.ps_supplycost), COUNT(*)
FROM tpch_partsupp ps
  JOIN tpch_part p ON p.p_partkey = ps.ps_partkey
  JOIN tpch_supplier s ON s.s_suppkey = ps.ps_suppkey
  JOIN tpch_nation n ON n.n_nationkey = s.s_nationkey
  JOIN tpch_region r ON r.r_regionkey = n.n_regionkey
WHERE p.p_size > 25
GROUP BY r.r_name
ORDER BY r.r_name
```

### Join Tree

```
Node 0: partsupp  (scanTable, root)
  +- Node 1: part      (readTuple, key: partsupp.ps_partkey,
  |                      filter: p_size > 25)
       +- Node 2: supplier  (readTuple, key: partsupp.ps_suppkey,
       |                      setParent(part))
            +- Node 3: nation    (readTuple, key: supplier.s_nationkey,
            |                      setParent(supplier))
                 +- Node 4: region    (readTuple, key: nation.n_regionkey,
                                       setParent(nation), aggregate leaf)
```

### Tables

- `tpch_partsupp`: root scan (PK: ps_partkey, ps_suppkey), provides both join keys and `ps_supplycost`
- `tpch_part`: lookup, filter on `p_size`
- `tpch_supplier`: lookup, provides `s_nationkey`
- `tpch_nation`: lookup, provides `n_regionkey`
- `tpch_region`: lookup leaf, provides `r_name` for GROUP BY

### Aggregation (NdbAggregator on region)

Local table: `tpch_region`

**GROUP BY columns:**
- `r_name` (local, CHAR(25))

**Linked projections on region:**
- pos 0: `linkedValue(partsuppOp, "ps_supplycost")` -> MIN/MAX/SUM computation

**Column lookups needed:**
- `ps_supplycost` from `tpch_partsupp` (DECIMAL(15,2))

**Program:**
```
GroupBy("r_name")                               // GROUP BY r_name (local)
LoadLinkedColumn(0, reg 0, supplycostCol)       // reg0 = ps_supplycost
Min(0, 0)                                       // agg[0] = MIN(reg0)
Max(1, 0)                                       // agg[1] = MAX(reg0)
Sum(2, 0)                                       // agg[2] = SUM(reg0)
Count(3, 0)                                     // agg[3] = COUNT(*)
Finalize()
```

### NdbInterpretedCode Filter (on part)

Filter: `p_size > 25`. INT column.

NDB interpreter inequality branches are inverted. We want to ACCEPT rows where
`p_size > 25`, i.e., reject rows where `p_size <= 25`.

Strategy: branch to label 0 (accept) if `p_size > 25`. Using inverted semantics:
`branch_col_lt(val, attrId, label)` actually branches when `col > val`.

```cpp
NdbInterpretedCode filter(partTab);
Uint32 pSizeAttrId = partTab->getColumn("p_size")->getColumnNo();
Int32 threshold = 25;
// branch_col_lt branches when col > val (inverted!)
// So: branch to label 0 when p_size > 25
filter.branch_col_lt(&threshold, sizeof(threshold), pSizeAttrId, 0);
filter.interpret_exit_nok();       // fall-through: p_size <= 25 -> reject
filter.def_label(0);
filter.interpret_exit_ok();        // p_size > 25 -> accept
filter.finalise();
```

### NdbQueryBuilder Construction

```
qb = NdbQueryBuilder::create()

// Node 0: scan partsupp (root)
partsuppOp = qb->scanTable(partsuppTab)

// Node 1: lookup part (key: partsupp.ps_partkey, filter)
partKey[] = { qb->linkedValue(partsuppOp, "ps_partkey"), nullptr }
partOpts.setMatchType(MatchNonNull)
partOpts.setInterpretedCode(filter)
partOp = qb->readTuple(partTab, partKey, &partOpts)

// Node 2: lookup supplier (key: partsupp.ps_suppkey, parent: part)
supplierKey[] = { qb->linkedValue(partsuppOp, "ps_suppkey"), nullptr }
supplierOpts.setMatchType(MatchNonNull)
supplierOpts.setParent(partOp)
supplierOp = qb->readTuple(supplierTab, supplierKey, &supplierOpts)

// Node 3: lookup nation (key: supplier.s_nationkey, parent: supplier)
nationKey[] = { qb->linkedValue(supplierOp, "s_nationkey"), nullptr }
nationOpts.setMatchType(MatchNonNull)
nationOpts.setParent(supplierOp)
nationOp = qb->readTuple(nationTab, nationKey, &nationOpts)

// Node 4: lookup region (key: nation.n_regionkey, parent: nation, aggregate leaf)
regionKey[] = { qb->linkedValue(nationOp, "n_regionkey"), nullptr }
regionOpts.setMatchType(MatchNonNull)
regionOpts.setParent(nationOp)
regionOpts.setAggregation(agg)

link0 = qb->linkedValue(partsuppOp, "ps_supplycost")  // pos 0
regionOpts.addLinkedProjection(link0)

regionOp = qb->readTuple(regionTab, regionKey, &regionOpts)
queryDef = qb->prepare(ndb)
```

### Result Retrieval

```
rec.FetchGroupbyColumn()     -> r_name CHAR(25): trim spaces
rec.FetchAggregationResult() -> agg[0] MIN: data_double()
rec.FetchAggregationResult() -> agg[1] MAX: data_double()
rec.FetchAggregationResult() -> agg[2] SUM: data_double()
rec.FetchAggregationResult() -> agg[3] COUNT: data_int64()
```

Map key: `std::string` (r_name). Map value: struct { double min, max, sum; int64_t count }.

### MySQL Verification Query

```sql
SELECT r.r_name, MIN(ps.ps_supplycost), MAX(ps.ps_supplycost),
       SUM(ps.ps_supplycost), COUNT(*)
FROM tpch_partsupp ps
JOIN tpch_part p ON p.p_partkey = ps.ps_partkey
JOIN tpch_supplier s ON s.s_suppkey = ps.ps_suppkey
JOIN tpch_nation n ON n.n_nationkey = s.s_nationkey
JOIN tpch_region r ON r.r_regionkey = n.n_regionkey
WHERE p.p_size > 25
GROUP BY r.r_name
ORDER BY r.r_name
```

### Expected Groups

5 groups (one per region: AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST).

---

## Benchmark 7: bench_q10_ndbapi

**Goal**: 4-table join, SUM + COUNT, VARCHAR(25) GROUP BY with very high
cardinality (one group per customer).

### SQL Query

```sql
SELECT c.c_name,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
       COUNT(*)
FROM tpch_lineitem l
  JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
  JOIN tpch_customer c ON c.c_custkey = o.o_custkey
  JOIN tpch_nation n ON n.n_nationkey = c.c_nationkey
GROUP BY c.c_name
ORDER BY revenue DESC
```

### Join Tree

```
Node 0: lineitem  (scanTable, root)
  +- Node 1: orders    (readTuple, key: lineitem.l_orderkey)
       +- Node 2: customer  (readTuple, key: orders.o_custkey,
       |                      setParent(orders))
            +- Node 3: nation    (readTuple, key: customer.c_nationkey,
                                   setParent(customer), aggregate leaf)
```

### Tables

- `tpch_lineitem`: root scan, provides `l_orderkey`, `l_extendedprice`, `l_discount`
- `tpch_orders`: lookup, provides `o_custkey`
- `tpch_customer`: lookup, provides `c_name` (VARCHAR(25)) for GROUP BY, `c_nationkey`
- `tpch_nation`: lookup leaf (just provides a valid join endpoint for aggregation)

### Aggregation (NdbAggregator on nation)

Local table: `tpch_nation`

**GROUP BY columns:**
- `c_name` (linked, VARCHAR(25)) from customer — position 0 in linked projections

**Linked projections on nation:**
- pos 0: `linkedValue(customerOp, "c_name")` -> GROUP BY
- pos 1: `linkedValue(lineitemOp, "l_extendedprice")` -> computation
- pos 2: `linkedValue(lineitemOp, "l_discount")` -> computation

**Column lookups needed:**
- `c_name` from `tpch_customer` (VARCHAR(25))
- `l_extendedprice` from `tpch_lineitem` (DECIMAL(15,2))
- `l_discount` from `tpch_lineitem` (DECIMAL(15,2))

**Program:**
```
GroupByLinked(0, c_nameCol)                      // GROUP BY c_name (linked pos 0)
LoadLinkedColumn(1, reg 0, extendedpriceCol)     // reg0 = l_extendedprice
LoadDouble(1.0, reg 1)                           // reg1 = 1.0
LoadLinkedColumn(2, reg 2, discountCol)          // reg2 = l_discount
Minus(1, 2)                                      // reg1 = 1 - discount
Mul(0, 1)                                        // reg0 = revenue
Sum(0, 0)                                        // agg[0] = SUM(revenue)
LoadUint64(1, reg 1)                             // reg1 = 1
Count(1, 1)                                      // agg[1] = COUNT(*)
Finalize()
```

### NdbInterpretedCode Filter

None.

### NdbQueryBuilder Construction

```
qb = NdbQueryBuilder::create()

lineitemOp = qb->scanTable(lineitemTab)

ordersKey[] = { qb->linkedValue(lineitemOp, "l_orderkey"), nullptr }
ordersOpts.setMatchType(MatchNonNull)
ordersOp = qb->readTuple(ordersTab, ordersKey, &ordersOpts)

customerKey[] = { qb->linkedValue(ordersOp, "o_custkey"), nullptr }
customerOpts.setMatchType(MatchNonNull)
customerOpts.setParent(ordersOp)
customerOp = qb->readTuple(customerTab, customerKey, &customerOpts)

nationKey[] = { qb->linkedValue(customerOp, "c_nationkey"), nullptr }
nationOpts.setMatchType(MatchNonNull)
nationOpts.setParent(customerOp)
nationOpts.setAggregation(agg)

link0 = qb->linkedValue(customerOp, "c_name")           // pos 0: GROUP BY
link1 = qb->linkedValue(lineitemOp, "l_extendedprice")  // pos 1
link2 = qb->linkedValue(lineitemOp, "l_discount")       // pos 2
nationOpts.addLinkedProjection(link0)
nationOpts.addLinkedProjection(link1)
nationOpts.addLinkedProjection(link2)

nationOp = qb->readTuple(nationTab, nationKey, &nationOpts)
queryDef = qb->prepare(ndb)
```

### Result Retrieval

```
rec.FetchGroupbyColumn()     -> c_name VARCHAR(25) (linked): handle VARCHAR format
rec.FetchAggregationResult() -> agg[0] SUM(revenue): data_double()
rec.FetchAggregationResult() -> agg[1] COUNT: data_int64()
```

**VARCHAR handling**: VARCHAR columns in NDB have a 1-byte or 2-byte length prefix.
When reading from FetchGroupbyColumn(), the `data()` pointer may include this
prefix, so the effective string extraction depends on the column type. For
VARCHAR(25) with latin1 charset, the NDB storage uses a 1-byte length prefix.
The `byte_size()` includes the data portion. Extract the string from the raw
data pointer accordingly. (Check actual FetchGroupbyColumn behavior: it returns
the raw column data as stored in NDB.)

Map key: `std::string` (c_name). Map value: struct { double sum; int64_t count }.

### MySQL Verification Query

```sql
SELECT c.c_name,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, COUNT(*)
FROM tpch_lineitem l
JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
JOIN tpch_customer c ON c.c_custkey = o.o_custkey
JOIN tpch_nation n ON n.n_nationkey = c.c_nationkey
GROUP BY c.c_name
ORDER BY revenue DESC
```

### Expected Groups

Very high cardinality: up to 150,000 groups (one per customer at SF=1.0).
At default SF (which uses `--sf` option from load_tpch), may be fewer.
With default SF=1.0: 150,000 customers, but not all may have orders, so
expect ~130,000-150,000 groups. This is the stress test for the aggregation
hash map.

---

## Benchmark 8: bench_q11_ndbapi

**Goal**: 3-table join, SUM of product expression, CHAR filter, high-cardinality
INT GROUP BY (thousands of groups by ps_partkey).

### SQL Query

```sql
SELECT ps.ps_partkey,
       SUM(ps.ps_supplycost * ps.ps_availqty) AS value
FROM tpch_partsupp ps
  JOIN tpch_supplier s ON s.s_suppkey = ps.ps_suppkey
  JOIN tpch_nation n ON n.n_nationkey = s.s_nationkey
WHERE n.n_name = 'GERMANY'
GROUP BY ps.ps_partkey
ORDER BY value DESC
```

### Join Tree

```
Node 0: partsupp  (scanTable, root)
  +- Node 1: supplier  (readTuple, key: partsupp.ps_suppkey)
       +- Node 2: nation    (readTuple, key: supplier.s_nationkey,
                              filter: n_name = 'GERMANY',
                              setParent(supplier), aggregate leaf)
```

### Tables

- `tpch_partsupp`: root scan, provides `ps_suppkey`, `ps_partkey`, `ps_supplycost`, `ps_availqty`
- `tpch_supplier`: lookup, provides `s_nationkey`
- `tpch_nation`: lookup leaf, filter on `n_name`

### Aggregation (NdbAggregator on nation)

Local table: `tpch_nation`

**GROUP BY columns:**
- `ps_partkey` (linked, INT) from partsupp — position 0 in linked projections

**Linked projections on nation:**
- pos 0: `linkedValue(partsuppOp, "ps_partkey")` -> GROUP BY (INT)
- pos 1: `linkedValue(partsuppOp, "ps_supplycost")` -> computation
- pos 2: `linkedValue(partsuppOp, "ps_availqty")` -> computation

**Column lookups needed:**
- `ps_partkey` from `tpch_partsupp` (INT) — needed for GroupByLinked column metadata
- `ps_supplycost` from `tpch_partsupp` (DECIMAL(15,2))
- `ps_availqty` from `tpch_partsupp` (INT)

Note: `ps_partkey` is part of the primary key of `tpch_partsupp`, but we still
need its NdbDictionary::Column pointer for `GroupByLinked()`.

**Program:**
```
GroupByLinked(0, ps_partkeyCol)                  // GROUP BY ps_partkey (linked pos 0)
LoadLinkedColumn(1, reg 0, supplycostCol)        // reg0 = ps_supplycost
LoadLinkedColumn(2, reg 1, availqtyCol)          // reg1 = ps_availqty
Mul(0, 1)                                        // reg0 = supplycost * availqty
Sum(0, 0)                                        // agg[0] = SUM(reg0)
Finalize()
```

### NdbInterpretedCode Filter (on nation)

Filter: `n_name = 'GERMANY'`. CHAR(25) column.

```cpp
NdbInterpretedCode filter(nationTab);
char GERMANY_PADDED[25];
memset(GERMANY_PADDED, ' ', 25);
memcpy(GERMANY_PADDED, "GERMANY", 7);
Uint32 nNameAttrId = nationTab->getColumn("n_name")->getColumnNo();
filter.branch_col_eq(GERMANY_PADDED, 25, nNameAttrId, 0);
filter.interpret_exit_nok();
filter.def_label(0);
filter.interpret_exit_ok();
filter.finalise();
```

### NdbQueryBuilder Construction

```
qb = NdbQueryBuilder::create()

partsuppOp = qb->scanTable(partsuppTab)

supplierKey[] = { qb->linkedValue(partsuppOp, "ps_suppkey"), nullptr }
supplierOpts.setMatchType(MatchNonNull)
supplierOp = qb->readTuple(supplierTab, supplierKey, &supplierOpts)

nationKey[] = { qb->linkedValue(supplierOp, "s_nationkey"), nullptr }
nationOpts.setMatchType(MatchNonNull)
nationOpts.setParent(supplierOp)
nationOpts.setInterpretedCode(filter)
nationOpts.setAggregation(agg)

link0 = qb->linkedValue(partsuppOp, "ps_partkey")      // pos 0: GROUP BY
link1 = qb->linkedValue(partsuppOp, "ps_supplycost")   // pos 1: computation
link2 = qb->linkedValue(partsuppOp, "ps_availqty")     // pos 2: computation
nationOpts.addLinkedProjection(link0)
nationOpts.addLinkedProjection(link1)
nationOpts.addLinkedProjection(link2)

nationOp = qb->readTuple(nationTab, nationKey, &nationOpts)
queryDef = qb->prepare(ndb)
```

### Result Retrieval

```
rec.FetchGroupbyColumn()     -> ps_partkey INT (linked): data_int32()
rec.FetchAggregationResult() -> agg[0] SUM(value): data_double()
```

Map key: `int32_t` (ps_partkey). Map value: `double` (sum).

### MySQL Verification Query

```sql
SELECT ps.ps_partkey, SUM(ps.ps_supplycost * ps.ps_availqty) AS value
FROM tpch_partsupp ps
JOIN tpch_supplier s ON s.s_suppkey = ps.ps_suppkey
JOIN tpch_nation n ON n.n_nationkey = s.s_nationkey
WHERE n.n_name = 'GERMANY'
GROUP BY ps.ps_partkey
ORDER BY value DESC
```

### Expected Groups

High cardinality. GERMANY is nation 7 (from NATION_NAMES array). Suppliers
assigned to GERMANY: `suppkey % 25 == 7`. At SF=1.0 with 10,000 suppliers,
that's ~400 German suppliers. Each has 4 partsupp entries (via the data
generation formula), so ~1,600 partsupp rows, with distinct ps_partkey values
likely numbering in the hundreds to low thousands.

---

## Benchmark 9: bench_orderscan_ndbapi

**Goal**: Ordered index scan with INT range bounds on the root table, 2-table
join, SUM/COUNT/MIN/MAX on orders totalprice, GROUP BY o_orderyear.

### Prerequisites: Index Creation

This benchmark requires an ordered index on `tpch_orders(o_orderyear)`. The
benchmark must create this index if it does not exist, using MySQL:

```sql
CREATE INDEX idx_orders_year ON tpch_orders(o_orderyear) USING BTREE
```

Note: In NDB, all indexes are automatically distributed. Ordered indexes
support range scans. The index is an `OrderedIndex` type in NDB dictionary
terms.

### SQL Query

```sql
SELECT o.o_orderyear,
       SUM(o.o_totalprice), COUNT(*),
       MIN(o.o_totalprice), MAX(o.o_totalprice)
FROM tpch_orders o
  JOIN tpch_customer c ON c.c_custkey = o.o_custkey
WHERE o.o_orderyear BETWEEN 1994 AND 1996
GROUP BY o.o_orderyear
ORDER BY o.o_orderyear
```

### Join Tree

```
Node 0: orders    (scanIndex on idx_orders_year,
                    bounds: [1994, 1996] inclusive,
                    root)
  +- Node 1: customer  (readTuple, key: orders.o_custkey,
                          aggregate leaf)
```

### Tables

- `tpch_orders`: root ordered index scan with range bounds on `o_orderyear`, provides `o_orderyear` (for GROUP BY), `o_totalprice`, `o_custkey`
- `tpch_customer`: lookup leaf

### Aggregation (NdbAggregator on customer)

Local table: `tpch_customer`

**GROUP BY columns:**
- `o_orderyear` (linked, INT) from orders — position 0 in linked projections

**Linked projections on customer:**
- pos 0: `linkedValue(ordersOp, "o_orderyear")` -> GROUP BY
- pos 1: `linkedValue(ordersOp, "o_totalprice")` -> SUM/MIN/MAX computation

**Column lookups needed:**
- `o_orderyear` from `tpch_orders` (INT)
- `o_totalprice` from `tpch_orders` (DECIMAL(15,2))

**Program:**
```
GroupByLinked(0, orderyearCol)                // GROUP BY o_orderyear (linked pos 0)
LoadLinkedColumn(1, reg 0, totalpriceCol)     // reg0 = o_totalprice
Sum(0, 0)                                     // agg[0] = SUM(reg0)
Count(1, 0)                                   // agg[1] = COUNT(*)
Min(2, 0)                                     // agg[2] = MIN(reg0)
Max(3, 0)                                     // agg[3] = MAX(reg0)
Finalize()
```

### NdbInterpretedCode Filter

None. The range restriction is handled by the index scan bounds.

### NdbQueryBuilder Construction (with scanIndex)

The key difference from all previous benchmarks: the root operation uses
`qb->scanIndex()` instead of `qb->scanTable()`, with an `NdbQueryIndexBound`.

```
qb = NdbQueryBuilder::create()

// Look up the ordered index
const NdbDictionary::Index *yearIdx =
    dict->getIndex("idx_orders_year", "tpch_orders");

// Build range bounds: [1994, 1996] inclusive
const NdbQueryOperand *lowBound[] = { qb->constValue((Int32)1994), nullptr };
const NdbQueryOperand *highBound[] = { qb->constValue((Int32)1996), nullptr };
NdbQueryIndexBound bound(lowBound, true, highBound, true);  // inclusive both ends

// Node 0: ordered index scan on orders (root)
ordersOp = qb->scanIndex(yearIdx, ordersTab, &bound)

// Node 1: lookup customer (aggregate leaf)
customerKey[] = { qb->linkedValue(ordersOp, "o_custkey"), nullptr }
customerOpts.setMatchType(MatchNonNull)
customerOpts.setAggregation(agg)

link0 = qb->linkedValue(ordersOp, "o_orderyear")    // pos 0: GROUP BY
link1 = qb->linkedValue(ordersOp, "o_totalprice")   // pos 1: computation
customerOpts.addLinkedProjection(link0)
customerOpts.addLinkedProjection(link1)

customerOp = qb->readTuple(customerTab, customerKey, &customerOpts)
queryDef = qb->prepare(ndb)
```

### Index Creation in runBenchmark

Before building the query, create the index via MySQL if it does not exist:

```cpp
if (mysqlConn != nullptr) {
  // Ignore error if index already exists
  mysql_query(mysqlConn,
    "CREATE INDEX idx_orders_year ON tpch_orders(o_orderyear) USING BTREE");
}

// Then invalidate + get the index
dict->invalidateIndex("idx_orders_year", "tpch_orders");
const NdbDictionary::Index *yearIdx =
    dict->getIndex("idx_orders_year", "tpch_orders");
if (yearIdx == nullptr) {
  fprintf(stderr, "Index idx_orders_year not found: %s\n",
          dict->getNdbError().message);
  return -1;
}
```

### Result Retrieval

```
rec.FetchGroupbyColumn()     -> o_orderyear INT (linked): data_int32()
rec.FetchAggregationResult() -> agg[0] SUM: data_double()
rec.FetchAggregationResult() -> agg[1] COUNT: data_int64()
rec.FetchAggregationResult() -> agg[2] MIN: data_double()
rec.FetchAggregationResult() -> agg[3] MAX: data_double()
```

Map key: `int32_t` (orderyear). Map value: struct { double sum, min, max; int64_t count }.

### MySQL Verification Query

```sql
SELECT o.o_orderyear,
       SUM(o.o_totalprice), COUNT(*),
       MIN(o.o_totalprice), MAX(o.o_totalprice)
FROM tpch_orders o
JOIN tpch_customer c ON c.c_custkey = o.o_custkey
WHERE o.o_orderyear BETWEEN 1994 AND 1996
GROUP BY o.o_orderyear
ORDER BY o.o_orderyear
```

### Expected Groups

3 groups: 1994, 1995, 1996.

---

## Benchmark 10: bench_datescan_ndbapi

**Goal**: Ordered index scan with DATE range bounds, 2-table join, SUM + COUNT,
CHAR(10) GROUP BY on l_shipmode.

### Prerequisites: Index Creation

Requires an ordered index on `tpch_lineitem(l_shipdate)`:

```sql
CREATE INDEX idx_lineitem_shipdate ON tpch_lineitem(l_shipdate) USING BTREE
```

### SQL Query

```sql
SELECT l.l_shipmode,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
       COUNT(*)
FROM tpch_lineitem l
  JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
WHERE l.l_shipdate BETWEEN '1994-01-01' AND '1994-12-31'
GROUP BY l.l_shipmode
ORDER BY l.l_shipmode
```

### Join Tree

```
Node 0: lineitem  (scanIndex on idx_lineitem_shipdate,
                    bounds: ['1994-01-01', '1994-12-31'] inclusive,
                    root)
  +- Node 1: orders    (readTuple, key: lineitem.l_orderkey,
                          aggregate leaf)
```

### Tables

- `tpch_lineitem`: root ordered index scan on `l_shipdate`, provides `l_orderkey`, `l_shipmode`, `l_extendedprice`, `l_discount`
- `tpch_orders`: lookup leaf

### NDB DATE Encoding for Bounds

NDB stores DATE as a 3-byte packed integer:
`packed = (year << 9) | (month << 5) | day`

For the bounds:
- 1994-01-01: `(1994 << 9) | (1 << 5) | 1 = 1020929 + 32 + 1 = 0x0F9221`
- 1994-12-31: `(1994 << 9) | (12 << 5) | 31 = 1020929 + 384 + 31 = 0x0F95BF`

However, for NdbQueryBuilder bounds with `constValue()`, we need to pass the
value in the column's native format. For DATE columns, we use `constValue(const void*, Uint32 len)`
with the packed 3-byte representation (actually stored as 4 bytes for alignment,
with the high byte being 0).

```cpp
// Pack NDB DATE
static Uint32 packNdbDate(int year, int month, int day) {
  return ((Uint32)year << 9) | ((Uint32)month << 5) | (Uint32)day;
}

Uint32 lowDate = packNdbDate(1994, 1, 1);
Uint32 highDate = packNdbDate(1994, 12, 31);

// Store as 3-byte little-endian for constValue
char lowBuf[4] = {0}, highBuf[4] = {0};
lowBuf[0] = (char)(lowDate & 0xFF);
lowBuf[1] = (char)((lowDate >> 8) & 0xFF);
lowBuf[2] = (char)((lowDate >> 16) & 0xFF);
highBuf[0] = (char)(highDate & 0xFF);
highBuf[1] = (char)((highDate >> 8) & 0xFF);
highBuf[2] = (char)((highDate >> 16) & 0xFF);

const NdbQueryOperand *lowBound[] = { qb->constValue(lowBuf, 3), nullptr };
const NdbQueryOperand *highBound[] = { qb->constValue(highBuf, 3), nullptr };
```

### Aggregation (NdbAggregator on orders)

Local table: `tpch_orders`

**GROUP BY columns:**
- `l_shipmode` (linked, CHAR(10)) from lineitem — position 0 in linked projections

**Linked projections on orders:**
- pos 0: `linkedValue(lineitemOp, "l_shipmode")` -> GROUP BY
- pos 1: `linkedValue(lineitemOp, "l_extendedprice")` -> computation
- pos 2: `linkedValue(lineitemOp, "l_discount")` -> computation

**Column lookups needed:**
- `l_shipmode` from `tpch_lineitem` (CHAR(10))
- `l_extendedprice` from `tpch_lineitem` (DECIMAL(15,2))
- `l_discount` from `tpch_lineitem` (DECIMAL(15,2))

**Program:**
```
GroupByLinked(0, shipmodeCol)                    // GROUP BY l_shipmode (linked pos 0)
LoadLinkedColumn(1, reg 0, extendedpriceCol)     // reg0 = l_extendedprice
LoadDouble(1.0, reg 1)                           // reg1 = 1.0
LoadLinkedColumn(2, reg 2, discountCol)          // reg2 = l_discount
Minus(1, 2)                                      // reg1 = 1 - discount
Mul(0, 1)                                        // reg0 = revenue
Sum(0, 0)                                        // agg[0] = SUM(revenue)
LoadUint64(1, reg 1)                             // reg1 = 1
Count(1, 1)                                      // agg[1] = COUNT(*)
Finalize()
```

### NdbInterpretedCode Filter

None. Range restriction is via index scan bounds.

### NdbQueryBuilder Construction (with scanIndex + DATE bounds)

```
qb = NdbQueryBuilder::create()

// Look up the ordered index
const NdbDictionary::Index *shipIdx =
    dict->getIndex("idx_lineitem_shipdate", "tpch_lineitem");

// Build DATE range bounds
char lowBuf[4] = {0}, highBuf[4] = {0};
Uint32 lowPacked = (1994 << 9) | (1 << 5) | 1;
Uint32 highPacked = (1994 << 9) | (12 << 5) | 31;
lowBuf[0] = (char)(lowPacked & 0xFF);
lowBuf[1] = (char)((lowPacked >> 8) & 0xFF);
lowBuf[2] = (char)((lowPacked >> 16) & 0xFF);
highBuf[0] = (char)(highPacked & 0xFF);
highBuf[1] = (char)((highPacked >> 8) & 0xFF);
highBuf[2] = (char)((highPacked >> 16) & 0xFF);

const NdbQueryOperand *lowBound[] = { qb->constValue(lowBuf, 3), nullptr };
const NdbQueryOperand *highBound[] = { qb->constValue(highBuf, 3), nullptr };
NdbQueryIndexBound bound(lowBound, true, highBound, true);

// Node 0: ordered index scan on lineitem (root)
lineitemOp = qb->scanIndex(shipIdx, lineitemTab, &bound)

// Node 1: lookup orders (aggregate leaf)
ordersKey[] = { qb->linkedValue(lineitemOp, "l_orderkey"), nullptr }
ordersOpts.setMatchType(MatchNonNull)
ordersOpts.setAggregation(agg)

link0 = qb->linkedValue(lineitemOp, "l_shipmode")         // pos 0: GROUP BY
link1 = qb->linkedValue(lineitemOp, "l_extendedprice")    // pos 1
link2 = qb->linkedValue(lineitemOp, "l_discount")         // pos 2
ordersOpts.addLinkedProjection(link0)
ordersOpts.addLinkedProjection(link1)
ordersOpts.addLinkedProjection(link2)

ordersOp = qb->readTuple(ordersTab, ordersKey, &ordersOpts)
queryDef = qb->prepare(ndb)
```

### Index Creation in runBenchmark

```cpp
if (mysqlConn != nullptr) {
  mysql_query(mysqlConn,
    "CREATE INDEX idx_lineitem_shipdate ON tpch_lineitem(l_shipdate) USING BTREE");
}

dict->invalidateIndex("idx_lineitem_shipdate", "tpch_lineitem");
const NdbDictionary::Index *shipIdx =
    dict->getIndex("idx_lineitem_shipdate", "tpch_lineitem");
if (shipIdx == nullptr) {
  fprintf(stderr, "Index idx_lineitem_shipdate not found: %s\n",
          dict->getNdbError().message);
  return -1;
}
```

### Result Retrieval

```
rec.FetchGroupbyColumn()     -> l_shipmode CHAR(10) (linked): trim spaces
rec.FetchAggregationResult() -> agg[0] SUM(revenue): data_double()
rec.FetchAggregationResult() -> agg[1] COUNT: data_int64()
```

Map key: `std::string` (l_shipmode). Map value: struct { double sum; int64_t count }.

### MySQL Verification Query

```sql
SELECT l.l_shipmode,
       SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, COUNT(*)
FROM tpch_lineitem l
JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
WHERE l.l_shipdate BETWEEN '1994-01-01' AND '1994-12-31'
GROUP BY l.l_shipmode
ORDER BY l.l_shipmode
```

### Expected Groups

7 groups: "AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"
(from SHIPMODE_NAMES in load_tpch.cpp).

---

## Summary Table

| # | Benchmark | Tables | Root Scan | Filter | GROUP BY | Agg Types | Groups | Key Feature |
|---|-----------|--------|-----------|--------|----------|-----------|--------|-------------|
| 1 | bench_q4_ndbapi | 2 | scanTable | none | CHAR(15) local | COUNT | 5 | Simplest |
| 2 | bench_minmax_ndbapi | 2 | scanTable | none | CHAR(25) local | MIN/MAX/COUNT/SUM | 25 | All 4 agg types |
| 3 | bench_q3_ndbapi | 3 | scanTable | CHAR eq | INT+CHAR(15) linked | SUM+COUNT | ~35 | Arithmetic + filter |
| 4 | bench_nogroup_ndbapi | 3 | scanTable | CHAR eq | none (global) | COUNT/SUM/SUM/MIN/MAX | 1 | No GROUP BY |
| 5 | bench_q5_ndbapi | 5 | scanTable | CHAR eq | CHAR(25) linked | SUM | ~5 | Deep 5-table chain |
| 6 | bench_q2_ndbapi | 5 | scanTable | INT gt | CHAR(25) local | MIN/MAX/SUM/COUNT | 5 | INT inequality filter |
| 7 | bench_q10_ndbapi | 4 | scanTable | none | VARCHAR(25) linked | SUM+COUNT | ~150K | High-cardinality VARCHAR |
| 8 | bench_q11_ndbapi | 3 | scanTable | CHAR eq | INT linked | SUM | ~1000s | High-cardinality INT |
| 9 | bench_orderscan_ndbapi | 2 | scanIndex | INT range | INT linked | SUM/COUNT/MIN/MAX | 3 | Ordered index scan |
| 10 | bench_datescan_ndbapi | 2 | scanIndex | DATE range | CHAR(10) linked | SUM+COUNT | 7 | DATE bounds |
