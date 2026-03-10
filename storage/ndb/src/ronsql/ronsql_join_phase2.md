# RonSQL Join Support — Phase 2 Implementation Plan

Phase 1 (steps 1-12 in `ronsql_join_impl.md`) delivered basic inner join support
with GROUP BY and aggregation. This document specifies the next steps to remove
key limitations and extend functionality.

Current limitations addressed by this plan:
- WHERE clauses rejected on join queries
- Aggregate functions (SUM, COUNT, ...) cannot reference parent-table columns
- Only single-column join keys (`MAX_JOIN_KEY_COLS = 1`)
- No 3-table join test coverage
- Root table always full table scan (no index/PK optimization)

---

## Step 12b: Verify join test results in result file

### Problem

The current `ronsql_join.test` uses `ronsql_compare.inc` which runs each
query through both MySQL and RonSQL (via RDRS) and diffs the output. But:

1. The actual data rows are not captured in the `.result` file — only the
   line count and the (empty) diff are recorded.
2. The diff uses `|| true`, so mismatches are silently swallowed.

This means a regression could produce wrong results and the test would
still pass. We need the actual expected output baked into the result file
so that MTR's result-file comparison catches any change.

### 12b-a. Add direct MySQL query output to the test

For each join query, also execute it directly via MySQL (using the MTR
`--sorted_result` + `eval` pattern) so the actual rows appear in the
`.result` file. This serves as the ground truth.

**File: `mysql-test/suite/ronsql/t/ronsql_join.test`**

After each `ronsql_compare.inc` call, add:

```
--echo == Expected result ==
--sorted_result
SELECT o.o_custkey, SUM(l.l_quantity), SUM(l.l_price)
FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id
GROUP BY o.o_custkey;
```

The `--sorted_result` directive sorts the output rows so the result is
deterministic regardless of NDB's internal row ordering. The query output
(header + data rows) is captured in the `.result` file and verified by
MTR on every subsequent run.

Do this for every test query (Tests 1-4).

### 12b-b. Re-record the result file

```bash
cd mysql-test
./mtr --record --suite=ronsql ronsql_join
```

Verify the new `.result` file contains the actual data rows under each
`== Expected result ==` section. For example:

```
== Expected result ==
o_custkey	SUM(l.l_quantity)	SUM(l.l_price)
100	50	500
200	75	750
300	75	750
```

### 12b-c. Verify the results are correct

Manually check each expected result against the test data:

**Test data:**
- orders: (1,100), (2,100), (3,200), (4,200), (5,300)
- lineitem: (1,1,10,100), (2,1,20,200), (3,2,5,50), (4,2,15,150),
  (5,3,30,300), (6,3,25,250), (7,4,8,80), (8,4,12,120),
  (9,5,40,400), (10,5,35,350)

**Test 1** — `GROUP BY o.o_custkey, SUM(l_quantity), SUM(l_price)`:
- custkey 100: orders 1,2 → lineitems (10+20)+(5+15) = 50, (100+200)+(50+150) = 500
- custkey 200: orders 3,4 → lineitems (30+25)+(8+12) = 75, (300+250)+(80+120) = 750
- custkey 300: order 5 → lineitems (40+35) = 75, (400+350) = 750

**Test 2** — `COUNT(l.l_quantity)`:
- custkey 100: 4, custkey 200: 4, custkey 300: 2

**Test 3** — `MIN/MAX(l.l_price)`:
- custkey 100: min=50, max=200
- custkey 200: min=80, max=300
- custkey 300: min=350, max=400

**Test 4** — `GROUP BY l.l_orderkey, SUM(l_quantity)`:
- orderkey 1: 30, 2: 20, 3: 55, 4: 20, 5: 75

---

## Step 13: WHERE filter on root table for join queries

### Problem

Join queries currently reject all WHERE clauses. The architecture plan
specifies that WHERE conditions on the **root table** should be supported
via `NdbInterpretedCode` + `NdbQueryOptions::setInterpretedCode()`.

### 13a. Remove the blanket WHERE rejection in `load_join()`

Replace the current rejection with validation that all WHERE columns
belong to the root table:

**File: `RonSQLPreparer.cpp` — `load_join()`**

Replace:
```cpp
if (m_context.ast_root.where_expression != NULL)
{
  err << "WHERE clauses on join queries are not yet supported." << endl;
  throw RonSQLPermanentError(
      "WHERE clauses on join queries are not yet supported.");
}
```

With a recursive validation function that walks the `ConditionalExpression`
tree and verifies every column reference has `m_column_table_idx[col_idx]
== 0` (root table). If any column references a non-root table, throw:

```cpp
"WHERE conditions on joined tables are not yet supported. "
"Only the root table (first in FROM) may be filtered."
```

### 13b. Re-enable `plan_index_and_filter()` for join queries with WHERE

**File: `RonSQLPreparer.cpp` — constructor**

Change:
```cpp
if (m_context.ast_root.joins == NULL)
  plan_index_and_filter();
```

To:
```cpp
if (m_context.ast_root.joins == NULL ||
    m_context.ast_root.where_expression != NULL)
  plan_index_and_filter();
```

For join queries, `plan_index_and_filter()` will analyze the WHERE clause
against the root table's indexes. The resulting `m_scan_config` is used
only by the single-table path, but the populated `m_toplevel_conditions`
will be used by the new filter builder (step 13c).

### 13c. Build `NdbInterpretedCode` filter in `execute_join()`

Add a method to build an `NdbInterpretedCode` filter from the WHERE
conditions, and apply it to the root scan operation.

**File: `RonSQLPreparer.hpp`**
```cpp
void build_join_root_filter(NdbInterpretedCode* code);
```

**File: `RonSQLPreparer.cpp` — `execute_join()`**

In the root operation block, before `qb->scanTable()`:

```cpp
NdbQueryOptions rootOpts;
if (m_context.ast_root.where_expression != NULL)
{
  NdbInterpretedCode code(plan.ops[0].table);
  NdbScanFilter filter(&code);
  filter.setSqlCmpSemantics();
  apply_filter_top_level(&filter);
  filter.end();
  rootOpts.setInterpretedCode(code);
}
opDefs[0] = qb->scanTable(plan.ops[0].table, &rootOpts);
```

This reuses the existing `apply_filter_top_level()` and `apply_filter()`
methods unchanged — they work on `m_toplevel_conditions` which reference
column indices into `m_column_attrId_map`. Since we validated that all
WHERE columns are on the root table, the attrIds are correct for the root
table's `NdbInterpretedCode`.

Note: `NdbScanFilter` can write to an `NdbInterpretedCode` object directly
when constructed with one. This avoids the need to rewrite the filter
generation logic.

### 13d. Add MTR test

**File: `mysql-test/suite/ronsql/t/ronsql_join.test`**

Add test case:
```sql
SELECT o.o_custkey, SUM(l.l_quantity)
FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id
WHERE o.o_custkey = 100
GROUP BY o.o_custkey;
```

This verifies the WHERE filter is pushed to the root scan and only rows
with `o_custkey = 100` are returned.

Also add a negative test verifying WHERE on child table is rejected:
```
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -e \
  "SELECT o.o_custkey, SUM(l.l_quantity) FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id WHERE l.l_quantity > 10 GROUP BY o.o_custkey"
```

---

## Step 14: Aggregate functions on parent-table columns

### Problem

Aggregate functions (`SUM(parent.col)`) currently fail with "Aggregate
references non-leaf column." Supporting this requires:
1. Adding the parent column to linked projections
2. Using `LoadLinkedColumn()` in `programAggregator_join()`

### 14a. Extend linked projection building in `load_join()`

After building linked projections for GROUP BY columns, also scan the
aggregation program outputs for columns on non-leaf tables.

The challenge: at `load_join()` time, the `AggregationAPICompiler`
program hasn't been compiled yet (that happens in `compile()`). However,
we can walk the AST outputs to find column references in aggregate
expressions.

**Alternative approach**: Build linked projections lazily — defer to
`compile()` or `programAggregator_join()`. Move the linked projection
building out of `load_join()` and into a new method
`build_linked_projections()` called after `compile()`.

**Recommended approach**: Walk `m_agg->m_program` (compiled instructions)
in a new method called after `compile()` but before `execute()`.

**File: `RonSQLPreparer.hpp`**
```cpp
void build_join_linked_projections();
```

**File: `RonSQLPreparer.cpp` — constructor**
```cpp
configure();
parse();
load();
if (m_context.ast_root.joins == NULL ||
    m_context.ast_root.where_expression != NULL)
  plan_index_and_filter();
compile();
if (m_context.ast_root.joins != NULL)
  build_join_linked_projections();
determine_explain();
```

**File: `RonSQLPreparer.cpp` — `build_join_linked_projections()`**

Move the existing GROUP BY linked projection code from `load_join()` here,
and add scanning of `m_agg->m_program` for Load instructions:

```cpp
void
RonSQLPreparer::build_join_linked_projections()
{
  Uint32 leaf_idx = m_join_plan.agg_leaf_idx;

  // Linked projections for GROUP BY on non-leaf tables
  struct GroupbyColumns* groupby = m_context.ast_root.groupby_columns;
  while (groupby != NULL)
  {
    Uint32 col_idx = groupby->col_idx;
    if (m_column_table_idx[col_idx] != leaf_idx)
      add_linked_projection(col_idx);
    groupby = groupby->next;
  }

  // Linked projections for aggregation Load instructions on non-leaf tables
  DynamicArray<AggregationAPICompiler::Instr>& program = m_agg->m_program;
  for (Uint32 i = 0; i < program.size(); i++)
  {
    if (program[i].type == AggregationAPICompiler::SVMInstrType::Load)
    {
      Uint32 col_idx = program[i].src;
      if (m_column_table_idx[col_idx] != leaf_idx)
        add_linked_projection(col_idx);
    }
  }
}
```

The helper `add_linked_projection()` checks for duplicates (same column
may appear in both GROUP BY and an aggregate):

```cpp
Uint32
RonSQLPreparer::add_linked_projection(Uint32 col_idx)
{
  // Check if already added
  for (Uint32 j = 0; j < m_join_plan.num_linked_projs; j++)
  {
    if (m_join_plan.linked_projs[j].source_op_idx ==
            m_column_table_idx[col_idx] &&
        strcmp(m_join_plan.linked_projs[j].column_name,
               m_columns[col_idx].c_str()) == 0)
      return j;
  }
  require_prm(m_join_plan.num_linked_projs < MAX_LINKED_PROJS,
              "Too many linked projections.");
  JoinPlan::LinkedProj& lp =
      m_join_plan.linked_projs[m_join_plan.num_linked_projs];
  lp.source_op_idx = m_column_table_idx[col_idx];
  lp.column_name = m_columns[col_idx].c_str();
  return m_join_plan.num_linked_projs++;
}
```

### 14b. Update `programAggregator_join()` Load handling

Remove the "Aggregate references non-leaf column" error check and replace
with `LoadLinkedColumn()`:

```cpp
case AggregationAPICompiler::SVMInstrType::Load:
{
  if (m_column_table_idx[src] != leaf_idx)
  {
    // Find linked projection position for this column
    Uint32 lp_pos = find_linked_projection(src);
    programAggregator_do_or_fail
      (aggregator->LoadLinkedColumn(lp_pos, dest, m_column_map[src]));
  }
  else
  {
    NdbAttrId col_id = m_column_attrId_map[src];
    if (!aggregator->LoadColumn(col_id, dest))
    {
      err << "Failed writing aggregation program "
             "when attempting to load column "
          << quoted_identifier(m_columns[src]) << endl;
      throw RonSQLMaybeStaleSchema(
          "Failed writing aggregation program");
    }
  }
  break;
}
```

Where `find_linked_projection()` searches `m_join_plan.linked_projs[]`:

```cpp
Uint32
RonSQLPreparer::find_linked_projection(Uint32 col_idx)
{
  for (Uint32 j = 0; j < m_join_plan.num_linked_projs; j++)
  {
    if (m_join_plan.linked_projs[j].source_op_idx ==
            m_column_table_idx[col_idx] &&
        strcmp(m_join_plan.linked_projs[j].column_name,
               m_columns[col_idx].c_str()) == 0)
      return j;
  }
  abort(); // Should never happen — build_join_linked_projections ensures it
}
```

### 14c. Add MTR test

```sql
-- SUM on parent column
SELECT o.o_status, SUM(o.o_custkey)
FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id
GROUP BY o.o_status;

-- Mixed: GROUP BY parent, SUM parent + SUM child
SELECT o.o_custkey, SUM(o.o_custkey), SUM(l.l_quantity)
FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id
GROUP BY o.o_custkey;
```

---

## Step 15: Multi-column join keys (composite foreign keys)

### Problem

`MAX_JOIN_KEY_COLS = 1` limits joins to single-column equi-joins. Many
real schemas use composite primary keys or composite foreign keys.

### 15a. Increase `MAX_JOIN_KEY_COLS`

**File: `QueryPlanner.hpp`**
```cpp
static const Uint32 MAX_JOIN_KEY_COLS = 8;
```

### 15b. Extend parser to accept multi-condition ON clause

**File: `RonSQLParser.y`**

Change `join_clause` rule to accept `AND`-separated conditions:

```yacc
join_clause:
    T_JOIN table_ref T_ON join_condition_list
    {
      // ... build JoinClause with all conditions
    }
  ;

join_condition_list:
    join_condition
    {
      // single condition
    }
  | join_condition_list T_AND join_condition
    {
      // append condition
    }
  ;
```

### 15c. Extend `JoinCondition` to support multiple column pairs

**File: `RonSQLCommon.hpp`**

Option A — linked list of conditions:
```cpp
struct JoinCondition
{
  LexCString child_table;
  LexCString child_column;
  LexCString parent_table;
  LexCString parent_column;
  struct JoinCondition *next;  // for multi-column conditions
};
```

Option B — keep `JoinClause` with single condition but allow multiple
`JoinClause` entries for the same child table. This is simpler but
requires `QueryPlanner` to detect and merge them.

**Recommended**: Option A — explicit linked list within a single join.

### 15d. Update `QueryPlanner::plan()`

When processing a `JoinClause`, iterate all conditions in the linked list.
All conditions in one ON clause must reference the same parent table.
For PK lookups, all PK columns must be covered. For unique index lookups,
all index columns must be covered.

```cpp
// Collect all key columns
Uint32 num_keys = 0;
for (JoinCondition* jc = condition; jc != NULL; jc = jc->next)
{
  require_prm(num_keys < MAX_JOIN_KEY_COLS,
              "Too many join key columns.");
  childOp.child_key_col_names[num_keys] = jc->child_column.c_str();
  childOp.parent_key_col_names[num_keys] = jc->parent_column.c_str();
  num_keys++;
}
childOp.num_key_cols = num_keys;
```

Update `isPrimaryKey()` to accept multiple columns:
```cpp
static bool isPrimaryKey(const NdbDictionary::Table *table,
                         const char *col_names[], Uint32 num_cols);
```

Similarly update `findUniqueIndex()` and `findOrderedIndex()`.

### 15e. Update `execute_join()` key building

The existing key building loop already handles `num_key_cols > 1`:
```cpp
for (Uint32 k = 0; k < op.num_key_cols; k++) {
  keys[k] = qb->linkedValue(opDefs[op.parent_op_idx],
                             op.parent_key_col_names[k]);
}
keys[op.num_key_cols] = nullptr;
```

This works because `NdbQueryBuilder::readTuple()` accepts a
NULL-terminated key array. No changes needed here.

### 15f. Add MTR test

Create a table with composite primary key:
```sql
CREATE TABLE order_detail (
  o_id INT NOT NULL,
  line_no INT NOT NULL,
  detail_val INT NOT NULL,
  PRIMARY KEY (o_id, line_no)
) ENGINE=NDB;

SELECT o.o_custkey, SUM(d.detail_val)
FROM orders AS o JOIN order_detail AS d
  ON d.o_id = o.o_id AND d.line_no = 1
GROUP BY o.o_custkey;
```

Note: The `line_no = 1` condition is a constant, not a linked value.
This may require `qb->constValue(1)` in the key array. If so, the parser
must distinguish `child.col = parent.col` from `child.col = constant`
in the ON clause. This can be deferred — for Phase 2, require all ON
conditions to be column=column equi-joins.

---

## Step 16: 3-table join test coverage

### Problem

The planner and executor support N-way joins (up to 16 tables), but the
MTR test only covers 2-table joins.

### 16a. Create 3-table test schema

**File: `mysql-test/suite/ronsql/t/ronsql_join.test`**

Add after existing tables:
```sql
CREATE TABLE customer (
  c_id INT NOT NULL,
  c_name VARCHAR(20) NOT NULL,
  c_region INT NOT NULL,
  PRIMARY KEY USING HASH (c_id)
) ENGINE=NDB;

INSERT INTO customer VALUES
  (100, 'Alice', 1),
  (200, 'Bob', 1),
  (300, 'Charlie', 2);
```

### 16b. Add 3-table join tests

```sql
-- 3-table join: customer → orders → lineitem
SELECT c.c_region, SUM(l.l_quantity)
FROM customer AS c
JOIN orders AS o ON o.o_custkey = c.c_id
JOIN lineitem AS l ON l.l_orderkey = o.o_id
GROUP BY c.c_region;

-- 3-table join: GROUP BY on middle table
SELECT o.o_status, SUM(l.l_quantity), COUNT(l.l_id)
FROM customer AS c
JOIN orders AS o ON o.o_custkey = c.c_id
JOIN lineitem AS l ON l.l_orderkey = o.o_id
GROUP BY o.o_status;
```

Note: `orders.o_custkey` needs an ordered index for the scan-scan join
from customer to orders:
```sql
CREATE INDEX idx_custkey ON orders (o_custkey);
```

---

## Step 17: Root PK lookup optimization

### Problem

The root operation is always `scanTable()`. When the WHERE clause fully
specifies the root table's primary key with equality conditions, the
root should be a `readTuple()` (PK lookup) instead — much more efficient.

### 17a. Detect PK-covered WHERE in `load_join()` or `plan_index_and_filter()`

After running `plan_index_and_filter()` (re-enabled in step 13b), check
if the chosen scan config covers all PK columns with equality bounds.

```cpp
bool is_pk_lookup = true;
int nkeys = m_table->getNoOfPrimaryKeys();
for (int k = 0; k < nkeys; k++)
{
  const char* pk_name = m_table->getPrimaryKey(k);
  bool found_eq = false;
  for (Uint32 i = 0; i < m_toplevel_conditions.size(); i++)
  {
    ConditionalExpression* ce = m_toplevel_conditions[i];
    if (ce->op == T_EQUALS &&
        strcmp(m_columns[ce->args.left->col_idx].c_str(), pk_name) == 0)
    {
      found_eq = true;
      break;
    }
  }
  if (!found_eq) { is_pk_lookup = false; break; }
}
```

If `is_pk_lookup`, store the constant values and set
`m_join_plan.ops[0].type = JoinOp::PK_LOOKUP`.

### 17b. Update `execute_join()` root operation

```cpp
if (plan.ops[0].type == JoinOp::PK_LOOKUP)
{
  // Build const keys from WHERE conditions
  const NdbQueryOperand* keys[MAX_PK_COLS + 1];
  // ... extract constant values from m_toplevel_conditions
  opDefs[0] = qb->readTuple(plan.ops[0].table, keys, &rootOpts);
}
else
{
  // Existing scanTable path
  opDefs[0] = qb->scanTable(plan.ops[0].table, &rootOpts);
}
```

### 17c. Add MTR test

```sql
-- PK lookup root → scan child
SELECT o.o_id, SUM(l.l_quantity)
FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id
WHERE o.o_id = 1
GROUP BY o.o_id;
```

---

## Step 18: Comprehensive MTR test suite for Phase 2 features

### Problem

Steps 13-17 each include inline MTR test snippets, but these need to be
consolidated into a well-structured test file that covers all new features,
edge cases, and error conditions.

### 18a. Extend `ronsql_join.test` with WHERE filter tests

```sql
-- WHERE on root table with equality
SELECT o.o_custkey, SUM(l.l_quantity)
FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id
WHERE o.o_custkey = 100
GROUP BY o.o_custkey;

-- WHERE on root table with range
SELECT o.o_custkey, SUM(l.l_quantity)
FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id
WHERE o.o_custkey >= 200
GROUP BY o.o_custkey;

-- WHERE on root table with multiple conditions (AND)
SELECT o.o_custkey, SUM(l.l_quantity)
FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id
WHERE o.o_custkey >= 100 AND o.o_custkey < 300
GROUP BY o.o_custkey;
```

Verify each with `--sorted_result` + direct MySQL execution so the
expected rows are in the result file.

### 18b. Error case: WHERE on child table

```
--echo === Error: WHERE on child table ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -e \
  "SELECT o.o_custkey, SUM(l.l_quantity) \
   FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id \
   WHERE l.l_quantity > 10 GROUP BY o.o_custkey" 2>&1
```

The error output should contain "WHERE conditions on joined tables are
not yet supported".

### 18c. Aggregate on parent column tests

```sql
-- SUM on parent column
SELECT o.o_status, SUM(o.o_custkey)
FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id
GROUP BY o.o_status;

-- Mixed: SUM on parent column + SUM on child column
SELECT o.o_custkey, SUM(o.o_custkey), SUM(l.l_quantity)
FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id
GROUP BY o.o_custkey;

-- COUNT(*) with GROUP BY on parent
SELECT o.o_custkey, COUNT(l.l_id)
FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id
GROUP BY o.o_custkey;

-- MIN/MAX on parent column
SELECT o.o_status, MIN(o.o_custkey), MAX(o.o_custkey)
FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id
GROUP BY o.o_status;
```

### 18d. 3-table join tests

Create the `customer` table with an ordered index on `orders.o_custkey`:

```sql
CREATE TABLE customer (
  c_id INT NOT NULL,
  c_name VARCHAR(20) NOT NULL,
  c_region INT NOT NULL,
  PRIMARY KEY USING HASH (c_id)
) ENGINE=NDB;
CREATE INDEX idx_custkey ON orders (o_custkey);

INSERT INTO customer VALUES
  (100, 'Alice', 1),
  (200, 'Bob', 1),
  (300, 'Charlie', 2);
```

Test queries:

```sql
-- 3-table: GROUP BY on root (customer), SUM on leaf (lineitem)
SELECT c.c_region, SUM(l.l_quantity)
FROM customer AS c
JOIN orders AS o ON o.o_custkey = c.c_id
JOIN lineitem AS l ON l.l_orderkey = o.o_id
GROUP BY c.c_region;

-- 3-table: GROUP BY on middle table (orders)
SELECT o.o_status, SUM(l.l_quantity), COUNT(l.l_id)
FROM customer AS c
JOIN orders AS o ON o.o_custkey = c.c_id
JOIN lineitem AS l ON l.l_orderkey = o.o_id
GROUP BY o.o_status;

-- 3-table: GROUP BY on root, MIN/MAX on leaf
SELECT c.c_name, MIN(l.l_price), MAX(l.l_price)
FROM customer AS c
JOIN orders AS o ON o.o_custkey = c.c_id
JOIN lineitem AS l ON l.l_orderkey = o.o_id
GROUP BY c.c_name;
```

### 18e. Root PK lookup test

```sql
-- PK lookup on root → index scan on child
SELECT o.o_id, SUM(l.l_quantity)
FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id
WHERE o.o_id = 1
GROUP BY o.o_id;

-- PK lookup root → child → grandchild (3-table with PK root)
SELECT o.o_id, SUM(l.l_quantity)
FROM orders AS o JOIN lineitem AS l ON l.l_orderkey = o.o_id
WHERE o.o_id = 3
GROUP BY o.o_id;
```

### 18f. Multi-column join key tests (if step 15 is implemented)

```sql
CREATE TABLE order_detail (
  o_id INT NOT NULL,
  line_no INT NOT NULL,
  detail_val INT NOT NULL,
  PRIMARY KEY (o_id, line_no)
) ENGINE=NDB;

INSERT INTO order_detail VALUES
  (1, 1, 10), (1, 2, 20),
  (2, 1, 30), (2, 2, 40),
  (3, 1, 50);

-- Composite PK join
SELECT o.o_custkey, SUM(d.detail_val)
FROM orders AS o JOIN order_detail AS d
  ON d.o_id = o.o_id
GROUP BY o.o_custkey;

DROP TABLE order_detail;
```

### 18g. Error case tests

```
-- Ambiguous column (exists in both tables)
-- If o_id existed in both, it would be ambiguous without qualifier

-- Missing index on join column
CREATE TABLE no_index_child (
  id INT NOT NULL,
  parent_ref INT NOT NULL,
  val INT NOT NULL,
  PRIMARY KEY USING HASH (id)
) ENGINE=NDB;
-- No index on parent_ref

--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -e \
  "SELECT o.o_custkey, SUM(n.val) \
   FROM orders AS o JOIN no_index_child AS n ON n.parent_ref = o.o_id \
   GROUP BY o.o_custkey" 2>&1

DROP TABLE no_index_child;
```

### 18h. Re-record and verify

```bash
cd mysql-test
./mtr --record --suite=ronsql ronsql_join
```

Manually verify all expected results in the `.result` file match the
hand-calculated values from the test data.

### 18i. Run full regression

```bash
./mtr --suite=ronsql
```

Confirm all existing ronsql tests still pass.

---

## Implementation Order

The recommended implementation order based on user impact and complexity:

1. **Step 12b** (Verify existing test results) — Prerequisite, low risk
2. **Step 16** (3-table join tests) — Low risk, validates existing code
3. **Step 13** (WHERE on root) — High impact, moderate complexity
4. **Step 14** (Aggregates on parent columns) — High impact, moderate complexity
5. **Step 15** (Multi-column keys) — Medium impact, high complexity
6. **Step 17** (Root PK lookup) — Performance optimization, lower priority
7. **Step 18** (Comprehensive test suite) — Final validation of all features

Steps 13 and 14 can be implemented in parallel as they touch different
code paths. Step 15 requires parser changes and is the most invasive.

---

## Verification

After each step:

```bash
cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli
cd ../mysql-test && ./mtr --suite=ronsql ronsql_join
```

After all steps, run the full ronsql test suite:
```bash
./mtr --suite=ronsql
```
