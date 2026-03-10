# testJoinAggNdbApi New Tests Implementation Plan

8 new test cases for `storage/ndb/block_unit_test/testJoinAggNdbApi.cpp`.

All tests follow the existing pattern:
1. Create tables via MySQL (ENGINE=NDB)
2. Insert data via MySQL
3. Verify expected results via MySQL JOIN query
4. Build NdbAggregator program
5. Build pushed join via NdbQueryBuilder
6. Execute, consume scan batches, fetch aggregator results
7. Compare against expected values
8. Drop tables

---

## Integration with main()

### New table name constants

Add after existing constants (line ~191):

```
static const char *T5_DEPT      = "t5_dept";
static const char *T5_EMP       = "t5_emp";
static const char *T6_CATEGORY  = "t6_category";
static const char *T6_PRODUCT   = "t6_product";
static const char *T7_COUNTRY   = "t7_country";
static const char *T7_CITY      = "t7_city";
static const char *T7_STORE     = "t7_store";
static const char *T7_SALE      = "t7_sale";
static const char *T8_ORDER     = "t8_order";
static const char *T8_ITEM      = "t8_item";
static const char *T10_PARENT   = "t10_parent";
static const char *T10_CHILD    = "t10_child";
static const char *T12_PARENT   = "t12_parent";
static const char *T12_CHILD    = "t12_child";
```

Note: Tests 5 and 7 reuse existing tables (jagg_parent/jagg_child and
t4_region/t4_order/t4_line respectively), so they need no new constants.

### main() structure

Each new test follows the existing pattern of create/insert/test/drop blocks.
Tests 5 and 7 reuse existing tables so they are placed adjacent to those
table blocks. The approximate ordering in main():

```
// Existing Tests 1-3 (use jagg_parent/jagg_child)
// Test 5: testEmptyResult (reuses jagg_parent/jagg_child — run before dropTestTables)
dropTestTables(conn);

// Existing Test 4 (uses t4_region/t4_order/t4_line)
// Test 7: testGlobalAggThreeWay (reuses t4 tables — run before dropTest4Tables)
dropTest4Tables(conn);

// Test 5: testMinMaxWithNull (t5_dept/t5_emp)
// Test 6: testCharGroupByWithIndex (t6_category/t6_product)
// Test 8: testFourWayCompositeKey (t7_country/t7_city/t7_store/t7_sale)
// Test 9: testArithmeticExpression (t8_order/t8_item)
// Test 10: testHighCardinalityGroupBy (t10_parent/t10_child)
// Test 12: testAllNullAggColumn (t12_parent/t12_child)
```

Exact numbering (continuing from existing Test 4):

| Test # | Function name              | New tables? |
|--------|----------------------------|-------------|
| 5      | testMinMaxWithNull         | t5_dept, t5_emp |
| 6      | testCharGroupByWithIndex   | t6_category, t6_product + ordered index |
| 7      | testFourWayCompositeKey    | t7_country, t7_city, t7_store, t7_sale |
| 8      | testArithmeticExpression   | t8_order, t8_item |
| 9      | testEmptyResult            | reuses jagg_parent/jagg_child |
| 10     | testHighCardinalityGroupBy | t10_parent, t10_child |
| 11     | testGlobalAggThreeWay      | reuses t4_region/t4_order/t4_line |
| 12     | testAllNullAggColumn       | t12_parent, t12_child |

---

## Handling NULL results

### MySQL side

MySQL returns NULL for SUM/MIN/MAX over zero matching non-NULL rows.
`mysql_fetch_row()` returns a `char*` array where NULL columns have
`row[i] == nullptr`. The verification helper must check for nullptr
before calling atoll().

### NDB API side

`NdbAggregator::Result` has `is_null()` method. When the group has
all-NULL input values, SUM/MIN/MAX return `is_null() == true`.
COUNT always returns a numeric value (0 for empty groups).

### New verification helper

Add a `verifyNullableAggWithMysql()` helper that stores results as
`std::optional<Int64>` or a custom NullableInt64 struct:

```cpp
struct NullableInt64 {
  bool is_null;
  Int64 value;
};
```

The MySQL verification reads `row[i]`: if nullptr, `is_null=true`; else
`value = atoll(row[i])`. The NDB result reads `res.is_null()` and
`res.data_int64()`.

---

## Handling CHAR padding in GROUP BY

### NDB CHAR(N) storage

NDB CHAR(N) columns store exactly N bytes, space-padded on the right.
When a GROUP BY column is CHAR(N), the aggregator's
`FetchGroupbyColumn()` returns a Column with `byte_size() == N` and
`data()` pointing to the raw N bytes.

### Extracting the effective string

Follow the pattern from bench_q9_ndbapi.cpp (lines 458-467):

```cpp
NdbAggregator::Column nameCol = rec.FetchGroupbyColumn();
int nameLen = (int)nameCol.byte_size();
const char *namePtr = nameCol.data();
int effLen = (int)strnlen(namePtr, nameLen);
while (effLen > 0 && namePtr[effLen - 1] == ' ')
  effLen--;
std::string name(namePtr, effLen);
```

### MySQL comparison

MySQL trims trailing spaces from CHAR columns in query results, so
the trimmed std::string from NDB should match exactly.

---

## Test 5: testMinMaxWithNull

### Purpose

Tests NULL handling in MIN/MAX/SUM/COUNT aggregation. When all values
in a group are NULL, MIN/MAX/SUM must return NULL, COUNT must return 0.

### New code path exercised

- MIN and MAX aggregation operators (first use in testJoinAggNdbApi)
- Nullable column handling in AggInterpreter
- `NdbAggregator::Result::is_null()` verification
- Mixed NULL / non-NULL groups

### Table schemas

```sql
CREATE TABLE t5_dept (
  id INT NOT NULL PRIMARY KEY,
  dept_name INT NOT NULL
) ENGINE=NDB;

CREATE TABLE t5_emp (
  dept_id INT NOT NULL PRIMARY KEY,
  salary BIGINT NULL
) ENGINE=NDB;
```

The salary column is nullable (`BIGINT NULL`).

### Sample data

```sql
INSERT INTO t5_dept VALUES (1, 10), (2, 10), (3, 20), (4, 20), (5, 20);

INSERT INTO t5_emp VALUES (1, 5000), (2, NULL), (3, NULL), (4, NULL), (5, NULL);
```

- dept_name=10: emp salaries are {5000, NULL} => MIN=5000, MAX=5000, SUM=5000, COUNT=1
- dept_name=20: emp salaries are {NULL, NULL, NULL} => MIN=NULL, MAX=NULL, SUM=NULL, COUNT=0

### SQL equivalent

```sql
SELECT d.dept_name, MIN(e.salary), MAX(e.salary), SUM(e.salary), COUNT(e.salary)
FROM t5_dept d
JOIN t5_emp e ON e.dept_id = d.id
GROUP BY d.dept_name
ORDER BY d.dept_name;
```

### Join tree

```
Node 0: scanTable(t5_dept)        — root, full table scan
  |
  +- Node 1: readTuple(t5_emp)   — child, key: emp.dept_id = dept.id
             aggregate leaf
```

### Linked projections

On Node 1 (child):
- `addLinkedProjection(linkedValue(deptOp, "dept_name"))` => linked position 0

### NdbAggregator program

Table: t5_emp (the leaf)

```
GroupByLinked(0, deptNameCol)        // GROUP BY linked pos 0 = dept.dept_name
LoadColumn("salary", 0)             // reg0 = emp.salary (nullable)
Min(0, 0)                           // agg[0] = MIN(reg0)
Max(1, 0)                           // agg[1] = MAX(reg0)
Sum(2, 0)                           // agg[2] = SUM(reg0)
Count(3, 0)                         // agg[3] = COUNT(salary) — counts non-NULL only
Finalize()
```

Note: `Count(aggId, regId)` counts non-NULL values from the register. When
salary is NULL, the register is NULL-flagged and COUNT skips it.

### Expected results

| dept_name | MIN(salary) | MAX(salary) | SUM(salary) | COUNT(salary) |
|-----------|-------------|-------------|-------------|---------------|
| 10        | 5000        | 5000        | 5000        | 1             |
| 20        | NULL        | NULL        | NULL        | 0             |

### Verification approach

For group dept_name=10: check `is_null() == false` for all results, verify
exact Int64 values.

For group dept_name=20: check `is_null() == true` for MIN, MAX, SUM results.
Check COUNT `is_null() == false` and `data_int64() == 0`.

MySQL verification: `row[1] == nullptr` for NULL results.

---

## Test 6: testCharGroupByWithIndex

### Purpose

Tests CHAR GROUP BY key handling (space padding) and scanIndex as root
operation. Uses an ordered index scan on the category table.

### New code path exercised

- `qb->scanIndex()` as root operation (all existing tests use scanTable)
- `NdbQueryIndexBound` for range bounds
- CHAR(20) GROUP BY column (first CHAR GROUP BY in testJoinAggNdbApi)
- Space-padded CHAR comparison in aggregation hash table

### Table schemas

```sql
CREATE TABLE t6_category (
  id INT NOT NULL PRIMARY KEY,
  cat_name CHAR(20) NOT NULL
) ENGINE=NDB;

CREATE TABLE t6_product (
  category_id INT NOT NULL PRIMARY KEY,
  price BIGINT NOT NULL
) ENGINE=NDB;
```

Ordered index on category name:

```sql
CREATE INDEX idx_cat_name ON t6_category(cat_name);
```

### Sample data

```sql
INSERT INTO t6_category VALUES
  (1, 'Electronics'), (2, 'Electronics'),
  (3, 'Books'), (4, 'Books'),
  (5, 'Toys');

INSERT INTO t6_product VALUES
  (1, 500), (2, 300), (3, 50), (4, 75), (5, 200);
```

- "Books": prices {50, 75} => SUM=125
- "Electronics": prices {500, 300} => SUM=800
- "Toys": prices {200} => SUM=200

### SQL equivalent

```sql
SELECT c.cat_name, SUM(p.price)
FROM t6_category c
JOIN t6_product p ON p.category_id = c.id
WHERE c.cat_name >= 'A' AND c.cat_name <= 'Z'
GROUP BY c.cat_name
ORDER BY c.cat_name;
```

The WHERE clause exercises the index scan bounds (full range effectively
covers all rows, but exercises the scanIndex path).

### Join tree

```
Node 0: scanIndex(idx_cat_name, t6_category, bound)  — root, ordered index scan
  |
  +- Node 1: readTuple(t6_product)                    — child, key: product.category_id = category.id
             aggregate leaf
```

### Index bound setup

```cpp
const NdbDictionary::Index *catIdx = dict->getIndex("idx_cat_name", T6_CATEGORY);

// Bound: cat_name >= 'A' AND cat_name <= 'Z'
// CHAR(20) requires exactly 20 bytes, space-padded
char lowBound[20], highBound[20];
memset(lowBound, ' ', 20);
memset(highBound, ' ', 20);
lowBound[0] = 'A';
highBound[0] = 'Z';

const NdbQueryOperand *lowKey[] = { qb->constValue(lowBound, 20), nullptr };
const NdbQueryOperand *highKey[] = { qb->constValue(highBound, 20), nullptr };
NdbQueryIndexBound bound(lowKey, true, highKey, true);

const NdbQueryIndexScanOperationDef *catOp =
    qb->scanIndex(catIdx, categoryTab, &bound);
```

### Linked projections

On Node 1 (child):
- `addLinkedProjection(linkedValue(catOp, "cat_name"))` => linked position 0

### NdbAggregator program

Table: t6_product (the leaf)

```
GroupByLinked(0, catNameCol)   // GROUP BY linked pos 0 = category.cat_name (CHAR(20))
LoadColumn("price", 0)        // reg0 = product.price
Sum(0, 0)                     // agg[0] = SUM(reg0)
Finalize()
```

The `catNameCol` is obtained via `categoryTab->getColumn("cat_name")` and
passed to `GroupByLinked()` so the aggregator knows the column type is CHAR(20).

### Expected results

| cat_name      | SUM(price) |
|---------------|------------|
| "Books"       | 125        |
| "Electronics" | 800        |
| "Toys"        | 200        |

### Verification approach

Use `std::map<std::string, Int64>` for results. Extract group key using the
CHAR trimming pattern (strnlen + trailing space trim). MySQL comparison
after trimming should match exactly.

---

## Test 7: testFourWayCompositeKey

### Purpose

Tests a 4-table deep join chain with a composite primary key on the leaf
table and 3 linked GROUP BY columns from different ancestor levels.

### New code path exercised

- 4-node pushed join (deepest chain in testJoinAggNdbApi)
- Composite primary key lookup (2 key columns)
- 3 GROUP BY columns from 3 different ancestor levels
- Multiple P_PARENT levels in linked attribute buffer
- `setParent()` on non-adjacent nodes in the join tree

### Table schemas

```sql
CREATE TABLE t7_country (
  id INT NOT NULL PRIMARY KEY,
  name INT NOT NULL
) ENGINE=NDB;

CREATE TABLE t7_city (
  country_id INT NOT NULL PRIMARY KEY,
  region INT NOT NULL
) ENGINE=NDB;

CREATE TABLE t7_store (
  city_id INT NOT NULL PRIMARY KEY,
  category INT NOT NULL
) ENGINE=NDB;

CREATE TABLE t7_sale (
  store_id INT NOT NULL,
  item_id INT NOT NULL,
  amount BIGINT NOT NULL,
  PRIMARY KEY (store_id, item_id)
) ENGINE=NDB;
```

Note: t7_sale has composite PK (store_id, item_id). The join from store to
sale uses store.city_id as store_id and a constant or linked item_id. Since
each store has exactly one sale row in our test data, we use store_id matching.

Actually, to make the composite key work with a pushed join, we need the
child's PK columns to be linked from ancestors. Revision: use city_id as the
link from store to sale (sale.store_id = store.city_id), and item_id is a
second key column that also comes from an ancestor. This is complex.

Simpler approach: make sale a single-PK table with store_id PK, and add a
separate non-PK column item_id. Then the composite key test is about the
join key pattern, not the PK structure.

Revised approach for composite key: sale has composite PK (store_id, item_id).
We need both PK columns linkable from ancestors. Use:
- sale.store_id = store.city_id (from direct parent)
- sale.item_id = city.region (from grandparent)

This means each (city_id, region) pair identifies a sale.

Revised schemas:

```sql
CREATE TABLE t7_country (
  id INT NOT NULL PRIMARY KEY,
  name INT NOT NULL
) ENGINE=NDB;

CREATE TABLE t7_city (
  country_id INT NOT NULL PRIMARY KEY,
  region INT NOT NULL
) ENGINE=NDB;

CREATE TABLE t7_store (
  city_id INT NOT NULL PRIMARY KEY,
  size INT NOT NULL
) ENGINE=NDB;

CREATE TABLE t7_sale (
  store_id INT NOT NULL,
  item_id INT NOT NULL,
  amount BIGINT NOT NULL,
  PRIMARY KEY (store_id, item_id)
) ENGINE=NDB;
```

### Sample data

```sql
INSERT INTO t7_country VALUES (1, 100), (2, 200);

INSERT INTO t7_city VALUES (1, 10), (2, 20);

INSERT INTO t7_store VALUES (1, 5), (2, 8);

INSERT INTO t7_sale VALUES (1, 10, 1000), (2, 20, 2000);
```

Join chain:
- country(1, name=100) -> city(1, region=10) -> store(1, size=5) -> sale(store_id=1, item_id=10, amount=1000)
- country(2, name=200) -> city(2, region=20) -> store(2, size=8) -> sale(store_id=2, item_id=20, amount=2000)

### SQL equivalent

```sql
SELECT co.name, ci.region, st.size, SUM(sa.amount)
FROM t7_country co
JOIN t7_city ci ON ci.country_id = co.id
JOIN t7_store st ON st.city_id = ci.country_id
JOIN t7_sale sa ON sa.store_id = st.city_id AND sa.item_id = ci.region
GROUP BY co.name, ci.region, st.size
ORDER BY co.name;
```

### Join tree

```
Node 0: scanTable(t7_country)         — root
  |
  +- Node 1: readTuple(t7_city)       — key: city.country_id = country.id
       |
       +- Node 2: readTuple(t7_store) — key: store.city_id = city.country_id
            |
            +- Node 3: readTuple(t7_sale) — composite key:
                       sale.store_id = store.city_id
                       sale.item_id = city.region (from grandparent via setParent)
                       aggregate leaf
```

### Linked projections on Node 3 (sale, aggregate leaf)

```
addLinkedProjection(linkedValue(countryOp, "name"))    => linked pos 0 (great-grandparent)
addLinkedProjection(linkedValue(cityOp, "region"))     => linked pos 1 (grandparent)
addLinkedProjection(linkedValue(storeOp, "size"))      => linked pos 2 (parent)
```

### Composite join key for Node 3

```cpp
const NdbQueryOperand *saleJoinKey[] = {
  qb->linkedValue(storeOp, "city_id"),    // sale.store_id = store.city_id
  qb->linkedValue(cityOp, "region"),      // sale.item_id = city.region
  nullptr
};
```

Note: `cityOp` is a grandparent, not the direct parent. This is valid because
NdbQueryBuilder allows linkedValue from any ancestor. However, since cityOp
is not the direct parent of saleOp (storeOp is), we do NOT need setParent()
here -- the framework handles multi-level linked values automatically. The
direct parent is storeOp (by virtue of the first linked key from storeOp).

Actually, we should verify: when key columns reference two different ancestors,
the NdbQueryBuilder determines the parent as the one closest to the root. In
this case both storeOp (immediate parent) and cityOp (grandparent) are
referenced. The framework should infer storeOp as parent.

### NdbAggregator program

Table: t7_sale (the leaf)

```
GroupByLinked(0, nameCol)      // GROUP BY linked pos 0 = country.name (INT)
GroupByLinked(1, regionCol)    // GROUP BY linked pos 1 = city.region (INT)
GroupByLinked(2, sizeCol)      // GROUP BY linked pos 2 = store.size (INT)
LoadColumn("amount", 0)        // reg0 = sale.amount
Sum(0, 0)                      // agg[0] = SUM(reg0)
Finalize()
```

Column pointers:
```cpp
const NdbDictionary::Column *nameCol   = countryTab->getColumn("name");
const NdbDictionary::Column *regionCol = cityTab->getColumn("region");
const NdbDictionary::Column *sizeCol   = storeTab->getColumn("size");
```

### Expected results

| name | region | size | SUM(amount) |
|------|--------|------|-------------|
| 100  | 10     | 5    | 1000        |
| 200  | 20     | 8    | 2000        |

### Verification approach

Use a custom struct or nested map keyed on (name, region, size) -> SUM.
For simplicity, since name is unique in this data, use `std::map<Int32, ...>`
keyed on name.

MySQL verification: run the SQL query, parse 4 columns.

---

## Test 8: testArithmeticExpression

### Purpose

Tests arithmetic operations (Mul, Minus) combined with LoadLinkedColumn
in the aggregation program. Computes SUM(qty * price - discount).

### New code path exercised

- `LoadLinkedColumn()` for computation (not GROUP BY)
- `Mul()` between two registers
- `Minus()` between two registers
- Multi-register arithmetic pipeline in aggregation program

### Table schemas

```sql
CREATE TABLE t8_order (
  id INT NOT NULL PRIMARY KEY,
  grp INT NOT NULL
) ENGINE=NDB;

CREATE TABLE t8_item (
  order_id INT NOT NULL PRIMARY KEY,
  qty BIGINT NOT NULL,
  price BIGINT NOT NULL,
  discount BIGINT NOT NULL
) ENGINE=NDB;
```

### Sample data

```sql
INSERT INTO t8_order VALUES (1, 1), (2, 1), (3, 2), (4, 2);

INSERT INTO t8_item VALUES
  (1, 10, 100, 50),   -- 10*100-50 = 950
  (2, 5, 200, 25),    -- 5*200-25 = 975
  (3, 3, 300, 100),   -- 3*300-100 = 800
  (4, 8, 50, 30);     -- 8*50-30 = 370
```

- grp=1: SUM = 950 + 975 = 1925
- grp=2: SUM = 800 + 370 = 1170

### SQL equivalent

```sql
SELECT o.grp, SUM(i.qty * i.price - i.discount)
FROM t8_order o
JOIN t8_item i ON i.order_id = o.id
GROUP BY o.grp
ORDER BY o.grp;
```

### Join tree

```
Node 0: scanTable(t8_order)      — root
  |
  +- Node 1: readTuple(t8_item) — key: item.order_id = order.id
             aggregate leaf
```

### Linked projections on Node 1 (item, aggregate leaf)

```
addLinkedProjection(linkedValue(orderOp, "grp"))  => linked pos 0 (for GROUP BY)
```

### NdbAggregator program

Table: t8_item (the leaf)

```
GroupByLinked(0, grpCol)        // GROUP BY linked pos 0 = order.grp (INT)
LoadColumn("qty", 0)           // reg0 = item.qty
LoadColumn("price", 1)         // reg1 = item.price
Mul(0, 1)                      // reg0 = qty * price
LoadColumn("discount", 2)     // reg2 = item.discount
Minus(0, 2)                    // reg0 = qty * price - discount
Sum(0, 0)                      // agg[0] = SUM(reg0)
Finalize()
```

Note: This test uses local `LoadColumn()` for qty, price, and discount (all
from the leaf table t8_item), not `LoadLinkedColumn()`. However, we can make
it more interesting by loading some values from the parent via linked columns.

Revised approach to exercise LoadLinkedColumn: move `price` to the parent
table.

Revised schemas:

```sql
CREATE TABLE t8_order (
  id INT NOT NULL PRIMARY KEY,
  grp INT NOT NULL,
  unit_price BIGINT NOT NULL
) ENGINE=NDB;

CREATE TABLE t8_item (
  order_id INT NOT NULL PRIMARY KEY,
  qty BIGINT NOT NULL,
  discount BIGINT NOT NULL
) ENGINE=NDB;
```

Revised data:

```sql
INSERT INTO t8_order VALUES
  (1, 1, 100),
  (2, 1, 200),
  (3, 2, 300),
  (4, 2, 50);

INSERT INTO t8_item VALUES
  (1, 10, 50),   -- 10*100-50 = 950
  (2, 5, 25),    -- 5*200-25 = 975
  (3, 3, 100),   -- 3*300-100 = 800
  (4, 8, 30);    -- 8*50-30 = 370
```

Revised SQL:

```sql
SELECT o.grp, SUM(i.qty * o.unit_price - i.discount)
FROM t8_order o
JOIN t8_item i ON i.order_id = o.id
GROUP BY o.grp
ORDER BY o.grp;
```

Revised linked projections on Node 1:

```
addLinkedProjection(linkedValue(orderOp, "grp"))         => linked pos 0
addLinkedProjection(linkedValue(orderOp, "unit_price"))  => linked pos 1
```

Revised NdbAggregator program:

```
GroupByLinked(0, grpCol)               // GROUP BY linked pos 0 = order.grp
LoadColumn("qty", 0)                   // reg0 = item.qty (local leaf column)
LoadLinkedColumn(1, 1, unitPriceCol)   // reg1 = order.unit_price (linked pos 1)
Mul(0, 1)                              // reg0 = qty * unit_price
LoadColumn("discount", 2)             // reg2 = item.discount (local leaf column)
Minus(0, 2)                            // reg0 = qty * unit_price - discount
Sum(0, 0)                              // agg[0] = SUM(reg0)
Finalize()
```

Column pointers:
```cpp
const NdbDictionary::Column *grpCol       = orderTab->getColumn("grp");
const NdbDictionary::Column *unitPriceCol = orderTab->getColumn("unit_price");
```

### Expected results

| grp | SUM(qty * unit_price - discount) |
|-----|----------------------------------|
| 1   | 1925                             |
| 2   | 1170                             |

### Verification approach

Use existing `verifyWithMysql()` helper (map<Int32, Int64>).

---

## Test 9: testEmptyResult

### Purpose

Tests that an NdbInterpretedCode filter that rejects ALL child rows produces
0 aggregation groups.

### New code path exercised

- NdbInterpretedCode filter on the aggregate leaf that rejects every row
- Empty aggregation result handling (0 groups, FetchResultRecord returns end)
- Combines filter with aggregation on same operation

### Table schemas

Reuses existing jagg_parent / jagg_child tables from Tests 1-3.

### Sample data

Already inserted for Tests 1-3. Data:
- parent: (1,1), (2,1), (3,2), (4,2), (5,3)
- child: (1,100), (2,200), (3,300), (4,400), (5,500)

### SQL equivalent

```sql
SELECT grp, SUM(amount)
FROM jagg_parent
JOIN jagg_child ON jagg_child.parent_id = jagg_parent.id
WHERE jagg_child.amount < 0
GROUP BY grp;
```

Result: empty set (no child rows have amount < 0).

### NdbInterpretedCode filter

Applied to the child table (jagg_child). Rejects all rows unconditionally:

```cpp
NdbInterpretedCode rejectAll(childTab);
rejectAll.interpret_exit_nok();    // unconditionally reject
if (rejectAll.finalise() != 0) { ... }
```

### Join tree

```
Node 0: scanTable(jagg_parent)       — root
  |
  +- Node 1: readTuple(jagg_child)   — key: child.parent_id = parent.id
             filter: reject all
             aggregate leaf
```

### NdbQueryOptions for Node 1

```cpp
NdbQueryOptions childOpts;
childOpts.setAggregation(agg);
childOpts.setInterpretedCode(rejectAll);
const NdbLinkedOperand *grpLink = qb->linkedValue(parentOp, "grp");
childOpts.addLinkedProjection(grpLink);
```

### NdbAggregator program

Table: jagg_child (the leaf)

```
GroupByLinked(0, grpCol)    // GROUP BY linked pos 0 = parent.grp
LoadColumn("amount", 0)    // reg0 = child.amount
Sum(0, 0)                  // agg[0] = SUM(reg0)
Finalize()
```

Note: The grpCol pointer comes from `parentTab->getColumn("grp")`.

### Expected results

0 groups. `resultAgg->FetchResultRecord().end()` should be `true`
immediately.

### Verification approach

MySQL verification: run the SQL query, verify 0 rows returned.
NDB verification: call FetchResultRecord(), assert end() is true.

### Placement in main()

This test runs after Tests 1-3 and before `dropTestTables(conn)`, since it
reuses the same tables and data.

---

## Test 10: testHighCardinalityGroupBy

### Purpose

Tests hash table scaling with 20 groups (1 row per group). Every parent
row maps to a unique group, maximizing GROUP BY cardinality.

### New code path exercised

- 20 unique GROUP BY keys (tests hash table with many entries)
- 1 row per group (tests single-row group aggregation)
- SUM + MIN + MAX on same column (all should equal the single row's value)

### Table schemas

```sql
CREATE TABLE t10_parent (
  id INT NOT NULL PRIMARY KEY,
  grp INT NOT NULL
) ENGINE=NDB;

CREATE TABLE t10_child (
  parent_id INT NOT NULL PRIMARY KEY,
  val BIGINT NOT NULL
) ENGINE=NDB;
```

### Sample data

20 rows, each parent has a unique grp value:

```sql
INSERT INTO t10_parent VALUES
  (1,1),(2,2),(3,3),(4,4),(5,5),
  (6,6),(7,7),(8,8),(9,9),(10,10),
  (11,11),(12,12),(13,13),(14,14),(15,15),
  (16,16),(17,17),(18,18),(19,19),(20,20);

INSERT INTO t10_child VALUES
  (1,100),(2,200),(3,300),(4,400),(5,500),
  (6,600),(7,700),(8,800),(9,900),(10,1000),
  (11,1100),(12,1200),(13,1300),(14,1400),(15,1500),
  (16,1600),(17,1700),(18,1800),(19,1900),(20,2000);
```

Each group has exactly 1 row, so SUM = MIN = MAX = the child's val.

### SQL equivalent

```sql
SELECT p.grp, SUM(c.val), MIN(c.val), MAX(c.val)
FROM t10_parent p
JOIN t10_child c ON c.parent_id = p.id
GROUP BY p.grp
ORDER BY p.grp;
```

### Join tree

```
Node 0: scanTable(t10_parent)       — root
  |
  +- Node 1: readTuple(t10_child)   — key: child.parent_id = parent.id
             aggregate leaf
```

### Linked projections on Node 1

```
addLinkedProjection(linkedValue(parentOp, "grp"))  => linked pos 0
```

### NdbAggregator program

Table: t10_child (the leaf)

```
GroupByLinked(0, grpCol)    // GROUP BY linked pos 0 = parent.grp (INT)
LoadColumn("val", 0)        // reg0 = child.val
Sum(0, 0)                   // agg[0] = SUM(reg0)
Min(1, 0)                   // agg[1] = MIN(reg0)
Max(2, 0)                   // agg[2] = MAX(reg0)
Finalize()
```

### Expected results

20 groups. For group grp=N: SUM = MIN = MAX = N * 100.

| grp | SUM   | MIN   | MAX   |
|-----|-------|-------|-------|
| 1   | 100   | 100   | 100   |
| 2   | 200   | 200   | 200   |
| ... | ...   | ...   | ...   |
| 20  | 2000  | 2000  | 2000  |

### Verification approach

Build `std::map<Int32, std::tuple<Int64, Int64, Int64>>` for (SUM, MIN, MAX).
Loop through all 20 expected groups and verify each triple.

MySQL verification: run the SQL query, parse 4 columns per row, compare.

Need a new verification helper `verifyTripleAggWithMysql()` or use inline
verification similar to Test 4's approach.

---

## Test 11: testGlobalAggThreeWay

### Purpose

Tests global aggregation (no GROUP BY) on a 3-way join. Reuses Test 4's
tables but performs COUNT + SUM + MIN + MAX without any GROUP BY clause.

### New code path exercised

- No GROUP BY on a 3-way join (single-group global aggregation)
- COUNT, SUM, MIN, MAX all in one program
- Global aggregation with linked parameter filter (reuses the priority >= area filter)
- 3-way join without GROUP BY

### Table schemas

Reuses t4_region, t4_order, t4_line from Test 4.

### Sample data

Already inserted for Test 4:
- region: (1,1),(2,2),(3,3),(4,4),(5,5)
- order: (1,3,5),(2,1,10),(3,5,15),(4,2,20),(5,6,25)
- line: (1,100),(2,200),(3,300),(4,400),(5,500)

With filter `priority >= area`, qualifying rows:
- region=1 (area=1), order=(1,3,5) pri=3>=1 yes, line=(1,100)
- region=2 (area=2), order=(2,1,10) pri=1>=2 no
- region=3 (area=3), order=(3,5,15) pri=5>=3 yes, line=(3,300)
- region=4 (area=4), order=(4,2,20) pri=2>=4 no
- region=5 (area=5), order=(5,6,25) pri=6>=5 yes, line=(5,500)

Qualifying line amounts: {100, 300, 500}

### SQL equivalent

```sql
SELECT COUNT(*), SUM(l.amount), MIN(l.amount), MAX(l.amount)
FROM t4_region r
JOIN t4_order o ON o.region_id = r.id
JOIN t4_line l ON l.order_id = o.region_id
WHERE o.priority >= r.area;
```

### Join tree

Same 3-node tree as Test 4:

```
Node 0: scanTable(t4_region)     — root
  |
  +- Node 1: readTuple(t4_order) — key: order.region_id = region.id
       |                            filter: priority >= area (linked param)
       +- Node 2: readTuple(t4_line) — key: line.order_id = order.region_id
                  aggregate leaf (NO linked projections for GROUP BY)
```

### Key difference from Test 4

No linked projections for GROUP BY columns. The NdbAggregator has no GroupBy
calls. No `addLinkedProjection()` calls on the leaf options.

### NdbAggregator program

Table: t4_line (the leaf)

```
LoadColumn("amount", 0)    // reg0 = line.amount
Count(0, 0)                // agg[0] = COUNT(*)
Sum(1, 0)                  // agg[1] = SUM(reg0)
Min(2, 0)                  // agg[2] = MIN(reg0)
Max(3, 0)                  // agg[3] = MAX(reg0)
Finalize()
```

### NdbInterpretedCode filter on Node 1

Same as Test 4:

```cpp
NdbInterpretedCode code(orderTab);
code.branch_col_le_param(priorityAttrId, 0, 0);  // branch when pri >= area
code.interpret_exit_nok();
code.def_label(0);
code.interpret_exit_ok();
code.finalise();
```

With filter param: `linkedValue(regionOp, "area")`.

### Node 2 options

```cpp
NdbQueryOptions lineOpts;
lineOpts.setAggregation(agg);
// NO addLinkedProjection — no GROUP BY
```

### Expected results

Single global row:
- COUNT(*) = 3
- SUM(amount) = 900 (100+300+500)
- MIN(amount) = 100
- MAX(amount) = 500

### Verification approach

Use `verifyScalarWithMysql()` with expected vector {3, 900, 100, 500}.

NDB verification: single FetchResultRecord, 4 FetchAggregationResult calls,
verify second FetchResultRecord returns end().

### Placement in main()

This test runs after Test 4 and before `dropTest4Tables(conn)`, since it
reuses the same tables and data.

---

## Test 12: testAllNullAggColumn

### Purpose

Tests aggregation behavior when one group has ALL NULL values in the
aggregated column and another group has non-NULL values. Focuses on
ensuring proper is_null() reporting for SUM, MIN, MAX.

### New code path exercised

- All-NULL group: SUM, MIN, MAX return is_null()=true
- Mixed group: SUM, MIN, MAX return correct non-NULL values
- Verifies COUNT(col) returns 0 for all-NULL group (not COUNT(*))
- Nullable BIGINT in the leaf table

### Difference from Test 5

Test 5 uses MIN/MAX/SUM/COUNT with a mixed group (some NULL, some non-NULL)
and a fully-NULL group. Test 12 focuses specifically on verifying that the
all-NULL group returns proper NULL indicators and that a second group with
only non-NULL values returns correct results. This is a simpler, more
targeted test.

### Table schemas

```sql
CREATE TABLE t12_parent (
  id INT NOT NULL PRIMARY KEY,
  grp INT NOT NULL
) ENGINE=NDB;

CREATE TABLE t12_child (
  parent_id INT NOT NULL PRIMARY KEY,
  val BIGINT NULL
) ENGINE=NDB;
```

### Sample data

```sql
INSERT INTO t12_parent VALUES (1, 1), (2, 1), (3, 2), (4, 2);

INSERT INTO t12_child VALUES (1, NULL), (2, NULL), (3, 100), (4, 200);
```

- grp=1: val = {NULL, NULL} => all aggregation results should be NULL, COUNT=0
- grp=2: val = {100, 200} => SUM=300, MIN=100, MAX=200, COUNT=2

### SQL equivalent

```sql
SELECT p.grp, SUM(c.val), MIN(c.val), MAX(c.val), COUNT(c.val)
FROM t12_parent p
JOIN t12_child c ON c.parent_id = p.id
GROUP BY p.grp
ORDER BY p.grp;
```

### Join tree

```
Node 0: scanTable(t12_parent)       — root
  |
  +- Node 1: readTuple(t12_child)   — key: child.parent_id = parent.id
             aggregate leaf
```

### Linked projections on Node 1

```
addLinkedProjection(linkedValue(parentOp, "grp"))  => linked pos 0
```

### NdbAggregator program

Table: t12_child (the leaf)

```
GroupByLinked(0, grpCol)    // GROUP BY linked pos 0 = parent.grp (INT)
LoadColumn("val", 0)        // reg0 = child.val (nullable)
Sum(0, 0)                   // agg[0] = SUM(reg0)
Min(1, 0)                   // agg[1] = MIN(reg0)
Max(2, 0)                   // agg[2] = MAX(reg0)
Count(3, 0)                 // agg[3] = COUNT(val) — non-NULL count
Finalize()
```

### Expected results

| grp | SUM  | MIN  | MAX  | COUNT |
|-----|------|------|------|-------|
| 1   | NULL | NULL | NULL | 0     |
| 2   | 300  | 100  | 200  | 2     |

### Verification approach

For grp=1:
- `sumRes.is_null()` must be true
- `minRes.is_null()` must be true
- `maxRes.is_null()` must be true
- `countRes.data_int64()` must be 0

For grp=2:
- `sumRes.is_null()` must be false, `data_int64()` = 300
- `minRes.is_null()` must be false, `data_int64()` = 100
- `maxRes.is_null()` must be false, `data_int64()` = 200
- `countRes.data_int64()` must be 2

MySQL verification: check `row[i] == nullptr` for NULL, else atoll().

Need the `NullableInt64` helper struct and a custom verification function.

---

## New verification helpers

### NullableInt64 struct

```cpp
struct NullableInt64 {
  bool is_null;
  Int64 value;

  NullableInt64() : is_null(true), value(0) {}
  NullableInt64(Int64 v) : is_null(false), value(v) {}
  static NullableInt64 null_value() { return NullableInt64(); }

  bool operator==(const NullableInt64 &other) const {
    if (is_null && other.is_null) return true;
    if (is_null != other.is_null) return false;
    return value == other.value;
  }
};
```

### verifyNullableMultiAgg helper

For Tests 5 and 12 that have nullable aggregation results per group:

```cpp
/* Verify query returning (grp, agg1, agg2, agg3, agg4) where agg columns
 * may be NULL. */
struct NullableAggRow {
  NullableInt64 vals[4];
};

static int
verifyNullableAggWithMysql(MYSQL *conn, const char *testName,
                           const char *query,
                           const std::map<Int32, NullableAggRow> &expected);
```

The implementation reads each column: if `row[col] == nullptr`, store as
NullableInt64::null_value(); else store NullableInt64(atoll(row[col])).

---

## Summary of API patterns per test

| Test | scanTable | scanIndex | readTuple | setParent | setInterpretedCode | setParameters | addLinkedProjection | LoadColumn | LoadLinkedColumn | GroupBy | GroupByLinked | Sum | Min | Max | Count | Mul | Minus |
|------|-----------|-----------|-----------|-----------|-------------------|---------------|--------------------|-----------|--------------------|---------|--------------|-----|-----|-----|-------|-----|-------|
| 5    | x         |           | x         |           |                   |               | x                  | x         |                    |         | x            | x   | x   | x   | x     |     |       |
| 6    |           | x         | x         |           |                   |               | x                  | x         |                    |         | x            | x   |     |     |       |     |       |
| 7    | x         |           | x(3)      |           |                   |               | x(3)               | x         |                    |         | x(3)         | x   |     |     |       |     |       |
| 8    | x         |           | x         |           |                   |               | x(2)               | x(2)      | x                  |         | x            | x   |     |     |       | x   | x     |
| 9    | x         |           | x         |           | x                 |               | x                  | x         |                    |         | x            | x   |     |     |       |     |       |
| 10   | x         |           | x         |           |                   |               | x                  | x         |                    |         | x            | x   | x   | x   |       |     |       |
| 11   | x         |           | x(2)      |           | x                 | x             |                    | x         |                    |         |              | x   | x   | x   | x     |     |       |
| 12   | x         |           | x         |           |                   |               | x                  | x         |                    |         | x            | x   | x   | x   | x     |     |       |

---

## Execution order in main()

```
// ---- Existing block: Tests 1-3 (jagg_parent/jagg_child) ----
//   createTestTables → insertTestData → test1 → test2 → test3

// Test 9: testEmptyResult (reuses jagg_parent/jagg_child)
if (testEmptyResult(&ndb, conn) != 0) exitCode = 1;

// ---- Existing: dropTestTables ----

// ---- Existing block: Test 4 (t4_region/t4_order/t4_line) ----
//   createTest4Tables → insertTest4Data → test4

// Test 11: testGlobalAggThreeWay (reuses t4 tables)
if (testGlobalAggThreeWay(&ndb, conn) != 0) exitCode = 1;

// ---- Existing: dropTest4Tables ----

// Test 5: testMinMaxWithNull
if (createTest5Tables(conn) == 0 && insertTest5Data(conn) == 0) {
  if (testMinMaxWithNull(&ndb, conn) != 0) exitCode = 1;
}
dropTest5Tables(conn);

// Test 6: testCharGroupByWithIndex
if (createTest6Tables(conn) == 0 && insertTest6Data(conn) == 0) {
  if (testCharGroupByWithIndex(&ndb, conn) != 0) exitCode = 1;
}
dropTest6Tables(conn);

// Test 7: testFourWayCompositeKey
if (createTest7Tables(conn) == 0 && insertTest7Data(conn) == 0) {
  if (testFourWayCompositeKey(&ndb, conn) != 0) exitCode = 1;
}
dropTest7Tables(conn);

// Test 8: testArithmeticExpression
if (createTest8Tables(conn) == 0 && insertTest8Data(conn) == 0) {
  if (testArithmeticExpression(&ndb, conn) != 0) exitCode = 1;
}
dropTest8Tables(conn);

// Test 10: testHighCardinalityGroupBy
if (createTest10Tables(conn) == 0 && insertTest10Data(conn) == 0) {
  if (testHighCardinalityGroupBy(&ndb, conn) != 0) exitCode = 1;
}
dropTest10Tables(conn);

// Test 12: testAllNullAggColumn
if (createTest12Tables(conn) == 0 && insertTest12Data(conn) == 0) {
  if (testAllNullAggColumn(&ndb, conn) != 0) exitCode = 1;
}
dropTest12Tables(conn);
```

---

## Index creation for Test 6

The ordered index on t6_category must be created via MySQL after the table:

```sql
CREATE INDEX idx_cat_name ON t6_category(cat_name);
```

In the test function, look it up via:

```cpp
dict->invalidateIndex("idx_cat_name", T6_CATEGORY);
const NdbDictionary::Index *catIdx = dict->getIndex("idx_cat_name", T6_CATEGORY);
```

---

## Important reminders

1. **Do NOT call getValue() on intermediate/leaf aggregate operations** (per
   existing pitfall documentation). Only call getValue() on the root scan
   operation for Test 4/11 style multi-level joins. For 2-table joins
   (Tests 5, 6, 8, 9, 10, 12), call getValue() on the parent (root) only.
   Actually, existing Tests 1-3 DO call getValue() on both parent and child.
   The restriction only applies to 3+ node joins where intermediate nodes
   suppress FLUSH_AI. For 2-node joins, both parent and child getValue() work
   fine because the child IS the aggregate leaf (FLUSH_AI is suppressed for
   non-leaf aggregate nodes, not the leaf itself).

2. **GroupByLinked requires parent column pointer**: The second argument to
   `GroupByLinked(position, parentCol)` must point to the NdbDictionary::Column
   from the ancestor table, not the leaf table.

3. **NDB CHAR(N) values must be space-padded for constValue()**: When
   creating index bounds with `qb->constValue(buf, 20)`, the buffer must be
   exactly 20 bytes, space-padded.

4. **Drop tables in reverse dependency order**: Drop child tables before
   parent tables to avoid foreign key issues (NDB may complain about
   referencing tables).
