# RonSQL Join Support — Architecture Plan

## Overview

Add JOIN support to RonSQL so that multi-table aggregate queries can be pushed
down to NDB data nodes via the NdbQueryBuilder API. This leverages the pushdown
join aggregation infrastructure already implemented in DBLQH/DBTUP/DBSPJ
(RONDB-733 Phases 1-7) and the NDB API layer (NdbQueryBuilder +
NdbAggregator + NdbQuery::getAggregator).

## Current Architecture

RonSQL currently processes **single-table aggregate queries**:

```
SQL string
  → [Flex Lexer] → tokens
  → [Bison Parser] → AST (SelectStatement with single table name)
  → [RonSQLPreparer::load()] → schema lookup, column validation
  → [RonSQLPreparer::plan_index_and_filter()] → index selection
  → [RonSQLPreparer::compile()] → AggregationAPICompiler bytecode
  → [RonSQLPreparer::execute()] → NdbScanOperation + NdbAggregator
  → [ResultPrinter::print_result()] → JSON/TEXT output
```

Key data structures:
- `SelectStatement` — AST root with single `LexCString table`
- `m_columns` — flat list of column names (no table qualification)
- `m_column_attrId_map` — maps col_idx → NDB attribute ID (single table)
- `m_table` — single `NdbDictionary::Table*`

## Target Architecture

After this work, RonSQL will process **multi-table aggregate join queries**:

```
SQL string
  → [Flex Lexer] → tokens (new: JOIN, ON, table.column)
  → [Bison Parser] → AST (JoinQuery with table list + join conditions)
  → [RonSQLPreparer::load()] → multi-table schema lookup
  → [QueryPlanner::plan()] → join order, join type selection
  → [RonSQLPreparer::compile()] → AggregationAPICompiler (leaf table)
  → [RonSQLPreparer::execute()] → NdbQueryBuilder + NdbAggregator
  → [ResultPrinter::print_result()] → JSON/TEXT output (unchanged)
```

## Supported SQL Syntax

### Phase 1: Inner joins only

```sql
SELECT t1.col, SUM(t2.amount)
FROM db.t1
JOIN db.t2 ON t2.fk = t1.pk
[JOIN db.t3 ON t3.fk = t2.pk]
[WHERE t1.col > 5]
GROUP BY t1.col
[ORDER BY t1.col]
[LIMIT 10];
```

Rules:
- `FROM table1 JOIN table2 ON condition [JOIN table3 ON condition ...]`
- Table names: `database.table` or `table` (uses default database)
- Table aliases: `FROM db.t1 AS a JOIN db.t2 AS b ON b.fk = a.pk`
- Column references in SELECT/WHERE/GROUP BY must be table-qualified
  (`t.col` or `alias.col`) when ambiguous
- ON conditions must be equi-joins: `child.col = parent.col`
- Each ON references exactly one previously-defined table (the parent)
- Aggregation columns must come from the leaf (last) table
- GROUP BY columns can come from any table (parent columns become linked
  projections)

### What is NOT supported (fail with clear error)

- `LEFT JOIN`, `RIGHT JOIN`, `CROSS JOIN` — inner join only for now
- Subqueries
- Self-joins
- Non-equi-join conditions (e.g., `ON t2.val > t1.val`)
- Multiple join conditions on the same child (e.g., composite foreign keys
  — this requires multi-column linked values, deferred to Phase 2)
- Joins that cannot be expressed as an NdbQueryBuilder tree (no cycles)

## NDB API Mapping

RonSQL join queries map directly to NdbQueryBuilder operations:

| SQL construct | NDB API |
|---|---|
| `FROM t1` (root, no PK filter) | `qb->scanTable(t1)` |
| `FROM t1 WHERE t1.pk = 5` (root, PK fully specified) | `qb->readTuple(t1, {constValue(5)})` |
| `JOIN t2 ON t2.fk = t1.pk` (PK lookup) | `qb->readTuple(t2, {linkedValue(t1Op, "pk")})` |
| `JOIN t2 ON t2.fk = t1.col` (index scan) | `qb->scanIndex(idx, t2, bound={linkedValue(t1Op, "col")})` |
| `GROUP BY t1.grp` (parent column) | `opts.addLinkedProjection(linkedValue(t1Op, "grp"))` + `agg.GroupByLinked(slot, grpCol)` |
| `SUM(t2.amount)` | `agg.LoadColumn("amount", reg); agg.Sum(agg_id, reg)` |
| `WHERE t1.col > 5` (root filter) | `NdbInterpretedCode` + `opts.setInterpretedCode()` on root |

### Root operation selection (QueryPlanner)

The root table (first in FROM) can be either a scan or a lookup:

1. **WHERE fully specifies root PK** (all PK columns have equality
   conditions against constants) → use `readTuple` with `constValue`
   keys. This is a single-row lookup; children may still be scans
   (e.g., `readTuple` root → `scanIndex` child returning 0:N rows).
2. **WHERE partially constrains an ordered index** → use `scanIndex`
   with index bounds.
3. **Otherwise** → use `scanTable` (full table scan).

This reuses the existing `plan_index_and_filter()` logic to detect PK
equality conditions, extended to recognize when ALL PK columns are bound.

### Child join type selection (QueryPlanner)

For each child table's ON condition (`child.fk = parent.col`):

1. **Check if `child.fk` is the primary key of `child`'s table** → use
   `readTuple` (PK lookup, 1:1, most efficient)
2. **Check if there is a unique hash index on `child.fk`** → use
   `readTuple` with index (unique lookup)
3. **Check if there is an ordered index on `child.fk`** → use `scanIndex`
   with linked bound (0:N scan-scan join)
4. **None of the above** → fail with error:
   `"Cannot push join: no suitable index on child.fk for table 'child'"`

The planner applies these rules in order for each table in the FROM list.
Tables are joined in the order they appear in the query (left to right).
No reordering is attempted.

## File Changes

### 1. Parser changes — `RonSQLParser.y` + `RonSQLLexer.l`

**New tokens** (lexer):
- `T_JOIN` — `JOIN` keyword
- `T_ON` — `ON` keyword
- `T_DOT` — `.` for table-qualified column references

**New grammar rules** (parser):
```
from_clause:
    T_FROM table_ref join_list
  ;

table_ref:
    identifier_c                          /* table name */
  | identifier_c T_AS identifier          /* table AS alias */
  ;

join_list:
    %empty
  | join_list join_clause
  ;

join_clause:
    T_JOIN table_ref T_ON join_condition
  ;

join_condition:
    qualified_column T_EQUALS qualified_column
  ;

qualified_column:
    identifier_c T_DOT identifier_c       /* table.column */
  ;
```

The `selectstatement` rule changes from:
```
T_FROM identifier_c
```
to:
```
T_FROM table_ref join_list
```

Column references in SELECT, WHERE, GROUP BY, and aggregate expressions
must support the `table.column` form. Unqualified column names are allowed
when unambiguous (only one table has that column).

### 2. AST changes — `RonSQLCommon.hpp`

```cpp
struct TableRef
{
  LexCString name;         /* table name (possibly database.table) */
  LexCString alias;        /* alias, or same as name if no alias */
  TableRef *next;          /* linked list for join order */
};

struct JoinCondition
{
  LexCString child_table;  /* child table alias */
  LexCString child_column; /* child column name */
  LexCString parent_table; /* parent table alias */
  LexCString parent_column;/* parent column name */
};

struct JoinClause
{
  TableRef *table;
  JoinCondition *condition;
  JoinClause *next;
};
```

Extend `SelectStatement`:
```cpp
struct SelectStatement
{
  bool do_explain = false;
  Outputs* outputs = NULL;
  /* Single-table (backward compat): */
  LexCString table = LexCString{NULL, 0};
  /* Multi-table join: */
  TableRef *root_table = NULL;      /* first table in FROM */
  JoinClause *joins = NULL;         /* linked list of JOINs */
  /* ... rest unchanged ... */
};
```

When `joins != NULL`, the query is a join query. When `joins == NULL` and
`table.c_str() != NULL`, it's a single-table query (backward compatible).

Column resolution changes: `m_columns` currently stores bare column names.
For join queries, columns are stored as `(table_idx, column_name)` pairs.
The `column_name_to_idx` function is extended to accept `table.column`
qualified names and resolve them against the table list.

### 3. Query planner — new file `QueryPlanner.hpp` / `QueryPlanner.cpp`

A simple, minimal query planner that:

1. **Validates the join graph**: each child ON condition references exactly
   one previously-defined parent table
2. **Selects join type per child**: PK lookup, unique index lookup, or
   ordered index scan (see "Join type selection" above)
3. **Classifies columns**: determines which table each referenced column
   belongs to, which are GROUP BY columns (and whether they need linked
   projections), which are aggregation columns
4. **Validates pushdown feasibility**: aggregation must be on the leaf
   table, GROUP BY columns from non-leaf tables become linked projections
5. **Produces a JoinPlan**: ordered list of operations with join types
   and key mappings

```cpp
struct JoinOp
{
  enum Type { TABLE_SCAN, INDEX_SCAN, PK_LOOKUP, UNIQUE_LOOKUP };
  Type type;
  const NdbDictionary::Table *table;
  const NdbDictionary::Index *index;  /* NULL for TABLE_SCAN/PK_LOOKUP */
  Uint32 parent_op_idx;               /* index into JoinPlan::ops, unused for root */
  bool is_root;                        /* true for ops[0] */
  /* Key columns: for root PK_LOOKUP these are const values from WHERE;
   * for child ops these link to parent columns via linkedValue() */
  Uint32 child_key_attr_ids[MAX_JOIN_KEY_COLS];
  const char *parent_key_col_names[MAX_JOIN_KEY_COLS];
  Uint32 num_key_cols;
};

struct JoinPlan
{
  JoinOp ops[MAX_JOIN_TABLES];  /* ops[0] = root scan */
  Uint32 num_ops;
  Uint32 agg_leaf_idx;          /* which op gets the aggregation */
  /* Linked projections needed for GROUP BY on non-leaf columns */
  struct LinkedProj {
    Uint32 source_op_idx;       /* which parent op */
    const char *column_name;    /* column name in parent */
    const NdbDictionary::Column *column; /* for GroupByLinked */
  };
  LinkedProj linked_projs[MAX_LINKED_PROJS];
  Uint32 num_linked_projs;
};
```

### 4. Execution changes — `RonSQLPreparer.cpp`

The `execute()` method gains a new path for join queries:

```cpp
void RonSQLPreparer::execute()
{
  if (is_join_query()) {
    execute_join();
  } else {
    // existing single-table path (unchanged)
    execute_single_table();
  }
}
```

`execute_join()` builds the NdbQueryBuilder tree:

```cpp
void RonSQLPreparer::execute_join()
{
  NdbQueryBuilder *qb = NdbQueryBuilder::create();

  // 1. Root operation — scan or PK lookup depending on WHERE
  const NdbQueryOperationDef *opDefs[MAX_JOIN_TABLES];
  JoinOp &rootOp = plan.ops[0];
  switch (rootOp.type) {
  case JoinOp::TABLE_SCAN:
    opDefs[0] = qb->scanTable(rootOp.table, &rootOpts);
    break;
  case JoinOp::INDEX_SCAN:
    opDefs[0] = qb->scanIndex(rootOp.index, rootOp.table,
                               &rootBound, &rootOpts);
    break;
  case JoinOp::PK_LOOKUP:
    // WHERE fully specifies PK — root is a single-row lookup
    // Keys are constValue()s built from WHERE equality conditions
    opDefs[0] = qb->readTuple(rootOp.table, constKeys, &rootOpts);
    break;
  }

  // 2. For each child operation in plan order:
  for (Uint32 i = 1; i < plan.num_ops; i++) {
    JoinOp &op = plan.ops[i];
    // Build linked key values from parent operation
    const NdbQueryOperand *keys[] = {
      qb->linkedValue(opDefs[op.parent_op_idx],
                       op.parent_key_col_names[0]),
      nullptr
    };

    NdbQueryOptions opts;
    if (i == plan.agg_leaf_idx) {
      // Attach aggregation to leaf
      opts.setAggregation(aggregator);
      // Add linked projections for GROUP BY on parent columns
      for (Uint32 j = 0; j < plan.num_linked_projs; j++) {
        opts.addLinkedProjection(
          qb->linkedValue(opDefs[plan.linked_projs[j].source_op_idx],
                          plan.linked_projs[j].column_name));
      }
    }

    switch (op.type) {
    case JoinOp::PK_LOOKUP:
      opDefs[i] = qb->readTuple(op.table, keys, &opts);
      break;
    case JoinOp::UNIQUE_LOOKUP:
      opDefs[i] = qb->readTuple(op.index, op.table, keys, &opts);
      break;
    case JoinOp::INDEX_SCAN:
      NdbQueryIndexBound bound(keys);
      opDefs[i] = qb->scanIndex(op.index, op.table, &bound, &opts);
      break;
    }
  }

  // 3. Prepare and execute
  const NdbQueryDef *queryDef = qb->prepare(ndb);
  NdbQuery *query = trans->createQuery(queryDef);
  trans->execute(NdbTransaction::NoCommit);

  // 4. Consume all rows
  while (query->nextResult(true) == NdbQuery::NextResult_gotRow) {}

  // 5. Collect aggregation results via getAggregator()
  NdbAggregator *resultAgg = query->getAggregator();
  m_resultprinter->print_result(resultAgg, out_stream);
}
```

### 5. Result printing — `ResultPrinter.cpp`

No changes needed. The `ResultPrinter` already works with `NdbAggregator`
results (GROUP BY columns + aggregate values). The same interface applies
whether the aggregation came from a single-table scan or a pushed join.

### 6. Build system — `CMakeLists.txt`

Add `QueryPlanner.cpp` to the ronsql convenience library.

## Column Resolution Strategy

With joins, column names can be ambiguous. The resolution strategy:

1. **Qualified references** (`t.col` or `alias.col`): look up the table
   alias/name, then find the column in that table's schema. This is the
   required form when a column name exists in multiple tables.

2. **Unqualified references** (`col`): search all tables in join order.
   If found in exactly one table, use it. If found in multiple tables,
   fail with: `"Ambiguous column 'col' — qualify with table name"`.

3. **During parsing**: `column_name_to_idx` is extended to accept both
   `qualified_column` (returns a `(table_idx, col_name)` pair) and bare
   `identifier_c` (deferred resolution until `load()` when schema is known).

4. **Internal representation**: `m_columns` becomes a list of
   `(table_idx, column_name)` pairs rather than bare column names. The
   `m_column_attrId_map` is extended to store per-table attribute IDs.

## Aggregation Placement Rules

The NdbQueryBuilder API requires that aggregation is attached to exactly
one operation (the "aggregate leaf"). Rules:

1. **Aggregate functions** (`SUM`, `COUNT`, `MIN`, `MAX`, `AVG`) may
   reference columns from the leaf table only. The leaf is the last
   table in the join order.

2. **GROUP BY columns** may reference any table:
   - Leaf table columns: use `agg.GroupBy(attrId)` (direct)
   - Non-leaf table columns: use `agg.GroupByLinked(slot, col)` +
     `opts.addLinkedProjection(linkedValue(...))` to project from parent

3. **WHERE conditions** on the root table are pushed as
   `NdbInterpretedCode` filter on the root scan operation.

4. **WHERE conditions** on non-root tables: not supported in Phase 1.
   Fail with clear error. (Phase 2 could add per-operation filters.)

## Error Messages

Clear error messages for unsupported constructs:

| Condition | Error |
|---|---|
| `LEFT JOIN` / `RIGHT JOIN` | `"RonSQL does not support LEFT/RIGHT JOIN. Use inner JOIN."` |
| No index for child join key | `"Cannot push join: no suitable index on 'child.col' for table 'child'. Create a primary key, unique index, or ordered index on the join column."` |
| Aggregate on non-leaf table | `"Aggregate function references column 'parent.col' which is not in the leaf table 'child'. Aggregation columns must come from the last joined table."` |
| Ambiguous column | `"Ambiguous column 'col' found in tables 't1' and 't2'. Use 'table.col' syntax."` |
| Non-equi-join | `"RonSQL only supports equi-join conditions (col1 = col2)."` |
| WHERE on non-root table | `"WHERE conditions on joined tables are not yet supported. Only the root table can be filtered."` |

## Implementation Order

1. **Parser**: Add `T_JOIN`, `T_ON`, `T_DOT` tokens and grammar rules.
   Add `table.column` qualified column references. Update AST structures.

2. **Column resolution**: Extend `column_name_to_idx` for multi-table
   qualified names. Add table alias tracking.

3. **QueryPlanner**: New module — validate join graph, select join types,
   produce JoinPlan.

4. **Execution**: Add `execute_join()` using NdbQueryBuilder. Wire up
   aggregation with linked projections.

5. **Testing**: Add RonSQL join queries to the test suite (ronsql_cli or
   REST API tests).

## Constraints & Limits

- `MAX_JOIN_TABLES = 16` — maximum tables in a single join query
  (matches NDB SPJ limit)
- `MAX_JOIN_KEY_COLS = 1` — Phase 1 supports single-column join keys only
  (Phase 2: composite keys)
- `MAX_LINKED_PROJS = 16` — maximum linked projections (GROUP BY columns
  from parent tables)
