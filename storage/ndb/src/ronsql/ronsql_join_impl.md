# RonSQL Join Support — Implementation Plan

Based on the architecture plan in `ronsql_join.md`. This document specifies
the exact file edits, new code, and build steps for each implementation step.

---

## Step 1: Lexer — Add T_DOT and new keywords

### 1a. Add `T_DOT` token to RonSQLLexer.l

The dot character `\056` is currently in the catch-all error rule at line 526:

```
[\001-\010\013\014\016-\037\042\043\056\072\077-\100\133-\135\137\173\175-\177]
```

**Action**: Remove `\056` from the catch-all character class and add a new
punctuation rule for dot. Insert it alongside the other punctuation rules
(after the `"/"` rule at line 294):

```c
"."       keyword(DOT);
```

The catch-all character class becomes:
```
[\001-\010\013\014\016-\037\042\043\072\077-\100\133-\135\137\173\175-\177]
```

### 1b. Add `JOIN` and `ON` keywords to Keywords.hpp

In `Keywords.hpp`, add to the `keywords_implemented_in_ronsql[]` array
in alphabetical order. `JOIN` goes between `IS` and `LIMIT`; `ON` goes
between `NULL` and `OR`:

```c
  kwdef(IS),
  kwdef(JOIN),    // new
  kwdef(LIMIT),
  ...
  kwdef(NULL),
  kwdef(ON),      // new
  kwdef(OR),
```

Note: `JOIN` is 4 characters and `ON` is 2 characters, both within the
18-character max keyword length. No change to
`max_strlen_for_keyword_implemented_in_ronsql` needed.

### 1c. Declare new tokens in RonSQLParser.y

Add `T_JOIN`, `T_ON`, and `T_DOT` to the token declarations. Insert
`T_JOIN T_ON` on the line with `T_FROM` and friends (line 148):

```
%token T_EXPLAIN T_SELECT T_FROM T_JOIN T_ON T_GROUP ...
```

Add `T_DOT` to the punctuation token line (line 150):

```
%token T_OR T_XOR ... T_DOT
```

### 1d. Regenerate parser and lexer

After editing `.y` and `.l` files, the generated `.cpp`/`.hpp` files are
rebuilt automatically by CMake via `build_parser.sh` and `build_lexer.sh`.
A clean build will regenerate them.

---

## Step 2: AST structures — Extend RonSQLCommon.hpp

Add new structs for representing join queries in the AST. Place them before
`struct SelectStatement`:

```cpp
struct TableRef
{
  LexCString database;     /* database name, or {NULL, 0} if unqualified */
  LexCString name;         /* table name */
  LexCString alias;        /* alias, or same as name if no alias */
};

struct JoinCondition
{
  LexCString child_table;  /* child table alias/name */
  LexCString child_column; /* child column name */
  LexCString parent_table; /* parent table alias/name */
  LexCString parent_column;/* parent column name */
};

struct JoinClause
{
  TableRef table;
  JoinCondition condition;
  struct JoinClause *next;
};
```

Extend `SelectStatement` to support multi-table queries:

```cpp
struct SelectStatement
{
  bool do_explain = false;
  Outputs* outputs = NULL;
  /* Single-table (backward compat): */
  LexCString table = LexCString{NULL, 0};
  /* Multi-table join: */
  TableRef *root_table = NULL;
  JoinClause *joins = NULL;
  /* rest unchanged */
  struct ConditionalExpression* where_expression = NULL;
  struct GroupbyColumns* groupby_columns = NULL;
  struct OrderbyColumns* orderby_columns = NULL;
  Int64 limit = -1;
};
```

When `joins != NULL`, the query is a join query. When `joins == NULL` and
`table.c_str() != NULL`, it is a single-table query (full backward compat).

### Column index encoding for join queries

Currently `col_idx` is a simple index into the flat `m_columns` array.
For join queries, we need to track which table a column belongs to.
This is deferred to the `load()` phase — during parsing, columns are still
stored as bare names (or qualified `table.column` names) in `m_columns`.
The `column_name_to_idx` function handles both forms. Resolution of
table ownership happens in `load()` when schemas are available.

Add a parallel array for storing the table-qualifier portion (or {NULL,0}
for unqualified columns). Add to `RonSQLPreparer`:

```cpp
DynamicArray<LexCString> m_column_qualifiers;  /* table qualifier per col_idx */
```

---

## Step 3: Parser grammar — Add join rules to RonSQLParser.y

### 3a. Add new union types

Add to the `%union` block:

```c
  struct TableRef* table_ref;
  struct JoinClause* join_clause;
  struct {
    JoinClause* head;
    JoinClause* tail;
  } join_list;
```

### 3b. Add type declarations

Add after the existing `%type` declarations:

```
%type<table_ref> table_ref
%type<join_clause> join_clause
%type<join_list> join_list
```

### 3c. New grammar rules

Add `qualified_column` as an alternative in column reference positions.
The key insight: `identifier_c T_DOT identifier_c` produces a qualified
column name. For simplicity, the parser concatenates `table.column` into
a single `LexCString` using a dot separator, and `column_name_to_idx`
detects the dot to split it.

Actually, a cleaner approach: extend `column_name_to_idx` to accept a
second argument (the table qualifier). Add a new parser context method:

```cpp
Uint32 qualified_column_name_to_idx(LexCString table, LexCString column);
```

#### Replace the `selectstatement` rule

Change from:
```
T_FROM identifier_c
```
to:
```
T_FROM table_ref join_list
```

The modified `selectstatement` rule:

```yacc
selectstatement:
  explain_opt T_SELECT outputlist T_FROM table_ref join_list
  where_opt groupby_opt orderby_opt limit_opt T_SEMICOLON
  {
    context->ast_root.do_explain = $1;
    context->ast_root.outputs = $3.head;
    context->ast_root.root_table = $5;
    if ($6.head != NULL) {
      /* Join query */
      context->ast_root.joins = $6.head;
      /* Also set table for backward compat (root table name) */
      context->ast_root.table = $5->name;
    } else {
      /* Single-table query — backward compatible */
      context->ast_root.table = $5->name;
    }
    context->ast_root.where_expression = $7;
    context->ast_root.groupby_columns = $8;
    context->ast_root.orderby_columns = $9;
    context->ast_root.limit = $10;
    /* static_asserts unchanged */
    ...
  }
```

#### New rules

```yacc
table_ref:
  identifier_c
  {
    initptr($$);
    $$->database = LexCString{NULL, 0};
    $$->name = $1;
    $$->alias = $1;
  }
| identifier_c T_DOT identifier_c
  {
    initptr($$);
    $$->database = $1;
    $$->name = $3;
    $$->alias = $3;
  }
| identifier_c T_AS identifier_c
  {
    initptr($$);
    $$->database = LexCString{NULL, 0};
    $$->name = $1;
    $$->alias = $3;
  }
| identifier_c T_DOT identifier_c T_AS identifier_c
  {
    initptr($$);
    $$->database = $1;
    $$->name = $3;
    $$->alias = $5;
  }

join_list:
  %empty                        { $$.head = NULL; $$.tail = NULL; }
| join_list join_clause         { if ($1.head == NULL) {
                                    $$.head = $2; $$.tail = $2;
                                  } else {
                                    $$.head = $1.head; $$.tail = $2;
                                    $1.tail->next = $2;
                                  }
                                }

join_clause:
  T_JOIN table_ref T_ON identifier_c T_DOT identifier_c T_EQUALS
  identifier_c T_DOT identifier_c
  {
    initptr($$);
    $$->table = *$2;
    $$->condition.child_table = $4;
    $$->condition.child_column = $6;
    $$->condition.parent_table = $8;
    $$->condition.parent_column = $10;
    $$->next = NULL;
  }
```

### 3d. Extend column references for qualified names

The places that currently call `column_name_to_idx($1)` with a bare
`identifier_c` need alternatives for `identifier_c T_DOT identifier_c`.

**In `nonaliased_output`** — add new alternative:
```yacc
| identifier_c T_DOT identifier_c
  {
    initptr($$);
    $$->type = Outputs::Type::COLUMN;
    $$->column.col_idx = context->qualified_column_name_to_idx($1, $3);
    /* Build output_name as "table.column" for display */
    $$->output_name = $3;  /* or build "t.c" string */
    $$->next = NULL;
  }
```

**In `arith_expr`** — add new alternative:
```yacc
| identifier_c T_DOT identifier_c
  { $$ = context->get_agg()->Load(context->qualified_column_name_to_idx($1, $3)); }
```

**In `cond_expr`** — add new alternative:
```yacc
| identifier_c T_DOT identifier_c
  { initptr($$); $$->op = T_IDENTIFIER;
    $$->col_idx = context->qualified_column_name_to_idx($1, $3); }
```

**In `groupby_col`** — add new alternative:
```yacc
| identifier_c T_DOT identifier_c
  { initptr($$);
    $$->col_idx = context->qualified_column_name_to_idx($1, $3);
    $$->next = NULL; }
```

**In `orderby_col`** — add alternatives for qualified columns (with ASC/DESC).

### 3e. Potential shift/reduce conflicts

The rule `identifier_c T_DOT identifier_c` in column positions may create
conflicts with the single `identifier_c` rule since the parser doesn't know
whether to reduce `identifier_c` or shift the `T_DOT`. Bison resolves this
in favor of shift by default, which is the correct behavior (always try to
match the longer `table.column` form first). This should work without
explicit disambiguation. Verify with a bison build and check for warnings.

---

## Step 4: Column resolution — Extend RonSQLPreparer

### 4a. Add `qualified_column_name_to_idx` to Context

In `RonSQLPreparer.hpp`, add to the `Context` class:

```cpp
Uint32 qualified_column_name_to_idx(LexCString table, LexCString column);
```

### 4b. Implement in RonSQLPreparer.cpp

Add `m_column_qualifiers` (DynamicArray<LexCString>) to `RonSQLPreparer`
alongside `m_columns`.

```cpp
Uint32
RonSQLPreparer::Context::qualified_column_name_to_idx(
    LexCString table_qualifier, LexCString col_name)
{
  DynamicArray<LexCString>& columns = m_parser.m_columns;
  DynamicArray<LexCString>& qualifiers = m_parser.m_column_qualifiers;
  Uint32 sz = columns.size();
  for (Uint32 i = 0; i < sz; i++)
  {
    if (columns[i] == col_name && qualifiers[i] == table_qualifier)
    {
      return i;
    }
  }
  columns.push(col_name);
  qualifiers.push(table_qualifier);
  return sz;
}
```

Also modify the existing `column_name_to_idx` to push a null qualifier:

```cpp
Uint32
RonSQLPreparer::Context::column_name_to_idx(LexCString col_name)
{
  DynamicArray<LexCString>& columns = m_parser.m_columns;
  DynamicArray<LexCString>& qualifiers = m_parser.m_column_qualifiers;
  Uint32 sz = columns.size();
  for (Uint32 i = 0; i < sz; i++)
  {
    if (columns[i] == col_name &&
        qualifiers[i].c_str() == NULL)  /* unqualified match */
    {
      return i;
    }
  }
  columns.push(col_name);
  qualifiers.push(LexCString{NULL, 0});
  return sz;
}
```

---

## Step 5: Query planner — New module

### 5a. Create `QueryPlanner.hpp`

```cpp
#ifndef STORAGE_NDB_SRC_RONSQL_QUERYPLANNER_HPP
#define STORAGE_NDB_SRC_RONSQL_QUERYPLANNER_HPP 1

#include <NdbApi.hpp>
#include "RonSQLCommon.hpp"
#include "LexString.hpp"

static const Uint32 MAX_JOIN_TABLES = 16;
static const Uint32 MAX_JOIN_KEY_COLS = 1;  /* Phase 1: single-col keys */
static const Uint32 MAX_LINKED_PROJS = 16;

struct JoinOp
{
  enum Type { TABLE_SCAN, INDEX_SCAN, PK_LOOKUP, UNIQUE_LOOKUP };
  Type type;
  const NdbDictionary::Table *table;
  const NdbDictionary::Index *index;  /* NULL for TABLE_SCAN / PK_LOOKUP */
  Uint32 parent_op_idx;               /* index into JoinPlan::ops */
  bool is_root;
  /* Child key: for PK_LOOKUP root these are const values from WHERE;
   * for child ops these link to parent columns via linkedValue() */
  const char *child_key_col_names[MAX_JOIN_KEY_COLS];
  const char *parent_key_col_names[MAX_JOIN_KEY_COLS];
  Uint32 num_key_cols;
};

struct JoinPlan
{
  JoinOp ops[MAX_JOIN_TABLES];
  Uint32 num_ops;
  Uint32 agg_leaf_idx;  /* which op gets the aggregation (last) */

  struct LinkedProj {
    Uint32 source_op_idx;
    const char *column_name;
  };
  LinkedProj linked_projs[MAX_LINKED_PROJS];
  Uint32 num_linked_projs;
};

class QueryPlanner
{
public:
  /*
   * Build a JoinPlan from the parsed AST.
   *
   * @param root_table  The root table from the FROM clause
   * @param joins       Linked list of JoinClauses
   * @param dict        NDB dictionary for index lookups
   * @param err         Error stream for user-facing messages
   * @param plan        Output: populated JoinPlan
   *
   * @throws RonSQLPermanentError if the join cannot be pushed down.
   */
  static void plan(
      const TableRef *root_table,
      const JoinClause *joins,
      const NdbDictionary::Dictionary *dict,
      std::basic_ostream<char> &err,
      JoinPlan &plan);

private:
  /* Check if col_name is the primary key (or part of it) for table */
  static bool isPrimaryKey(const NdbDictionary::Table *table,
                           const char *col_name);

  /* Find a unique hash index on col_name for table */
  static const NdbDictionary::Index *
  findUniqueIndex(const NdbDictionary::Dictionary *dict,
                  const NdbDictionary::Table *table,
                  const char *col_name);

  /* Find an ordered index on col_name for table */
  static const NdbDictionary::Index *
  findOrderedIndex(const NdbDictionary::Dictionary *dict,
                   const NdbDictionary::Table *table,
                   const char *col_name);
};

#endif
```

### 5b. Create `QueryPlanner.cpp`

Implement `QueryPlanner::plan()`:

1. Look up root table via `dict->getTable(root_table->name.c_str())`.
   Store in `plan.ops[0]` with `type = TABLE_SCAN`, `is_root = true`.
   (PK lookup root detection is deferred to later — always scan for now.)

2. For each `JoinClause` in the linked list:
   a. Look up child table via `dict->getTable(...)`.
   b. Determine the parent operation index by matching
      `condition.parent_table` against the alias/name of previously
      added operations.
   c. Determine join type for the child:
      - If `child_key_col_name` is the primary key → `PK_LOOKUP`
      - Else if a unique hash index exists → `UNIQUE_LOOKUP`
      - Else if an ordered index exists → `INDEX_SCAN`
      - Else → throw `RonSQLPermanentError` with message about
        missing index
   d. Populate `plan.ops[i]`.

3. Set `plan.agg_leaf_idx = plan.num_ops - 1` (last operation = leaf).

4. Populate `plan.linked_projs` — not needed yet (deferred to Step 7
   when GROUP BY on parent columns is wired up).

Helper methods:

- `isPrimaryKey`: iterate `table->getColumn(i)` for `i < table->getNoOfPrimaryKeys()`,
  check if any matches `col_name` via `table->getPrimaryKey(i)`.

- `findUniqueIndex`: call `dict->listIndexes(list, *table)`, iterate
  looking for `UniqueHashIndex` whose first column matches `col_name`.

- `findOrderedIndex`: same, but look for `OrderedIndex`.

### 5c. Add to CMakeLists.txt

In `storage/ndb/src/ronsql/CMakeLists.txt`, add `QueryPlanner.cpp` to
the `ADD_CONVENIENCE_LIBRARY(ronsql ...)` at line 50:

```cmake
ADD_CONVENIENCE_LIBRARY(ronsql
  AggregationAPICompiler.cpp
  LexString.cpp
  QueryPlanner.cpp
  ResultPrinter.cpp
  RonSQLPreparer.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/RonSQLParser.y.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/RonSQLLexer.l.cpp
  LINK_LIBRARIES ndbgeneral
  )
```

---

## Step 6: Schema loading — Extend `RonSQLPreparer::load()`

### 6a. Detect join query

At the start of `load()`, check whether this is a join query:

```cpp
bool is_join = (m_context.ast_root.joins != NULL);
```

### 6b. Single-table path (unchanged)

When `!is_join`, the existing code runs unchanged: look up single table,
populate `m_column_attrId_map` and `m_column_map`.

### 6c. Join path — multi-table schema lookup

When `is_join`:

1. Look up root table and each joined table via `m_dict->getTable(...)`.
   Store in a local array `const NdbDictionary::Table* tables[MAX_JOIN_TABLES]`
   with corresponding aliases `LexCString aliases[MAX_JOIN_TABLES]`.

2. Call `QueryPlanner::plan()` to produce the `JoinPlan`. Store it as
   a new member `JoinPlan m_join_plan` in `RonSQLPreparer`.

3. Resolve columns: iterate `m_columns` / `m_column_qualifiers`. For each
   column:
   - If qualifier is non-NULL: find the table by matching qualifier
     against aliases. Look up column in that table.
   - If qualifier is NULL: search all tables. If found in exactly one,
     use it. If ambiguous, throw `RonSQLPermanentError`.

4. Build per-column data:
   - `m_column_attrId_map[col_idx]` = attrId from the resolved table
   - `m_column_map[col_idx]` = NdbDictionary::Column* from resolved table
   - New: `m_column_table_idx[col_idx]` = index into the tables array
     (to know which operation this column belongs to)

Add new member to `RonSQLPreparer`:
```cpp
Uint32 *m_column_table_idx = NULL;  /* table index per col_idx */
```

---

## Step 7: Execution — Add `execute_join()` to RonSQLPreparer

### 7a. Branch in `execute()`

At the top of `execute()`, after the explain handling, add:

```cpp
if (m_context.ast_root.joins != NULL) {
  execute_join();
  return;
}
// ... existing single-table code follows unchanged
```

### 7b. Implement `execute_join()`

Declare in `RonSQLPreparer.hpp` (private):
```cpp
void execute_join();
```

Implementation outline in `RonSQLPreparer.cpp`:

```cpp
void
RonSQLPreparer::execute_join()
{
  Ndb* ndb = m_conf.ndb;
  ndbrequire(m_trans == NULL);
  m_trans = ndb->startTransaction();
  require_run(m_trans != NULL, "Failed to start transaction.");

  JoinPlan& plan = m_join_plan;

  // 1. Program the aggregator
  //    For join queries, the aggregator table is the leaf table.
  const NdbDictionary::Table* leaf_table =
      plan.ops[plan.agg_leaf_idx].table;
  NdbAggregator aggregator(leaf_table);
  programAggregator_join(&aggregator);
  require_prm(aggregator.Finalize(), "Failed to finalize aggregator.");

  // 2. Build NdbQueryBuilder tree
  NdbQueryBuilder* qb = NdbQueryBuilder::create();
  ndbrequire(qb != NULL);

  const NdbQueryOperationDef* opDefs[MAX_JOIN_TABLES];

  // Root operation (always TABLE_SCAN for now)
  {
    JoinOp& rootOp = plan.ops[0];
    NdbQueryOptions rootOpts;
    // Apply WHERE filter on root if present
    if (has_root_filter()) {
      NdbInterpretedCode code(rootOp.table);
      build_root_filter(&code);
      rootOpts.setInterpretedCode(code);
    }
    opDefs[0] = qb->scanTable(rootOp.table, &rootOpts);
    require_run(opDefs[0] != NULL, "Failed to create root scan.");
  }

  // Child operations
  for (Uint32 i = 1; i < plan.num_ops; i++) {
    JoinOp& op = plan.ops[i];
    NdbQueryOptions opts;

    // Build linked key from parent
    const NdbQueryOperand* keys[MAX_JOIN_KEY_COLS + 1];
    for (Uint32 k = 0; k < op.num_key_cols; k++) {
      keys[k] = qb->linkedValue(opDefs[op.parent_op_idx],
                                 op.parent_key_col_names[k]);
      require_run(keys[k] != NULL, "Failed to create linked value.");
    }
    keys[op.num_key_cols] = nullptr;

    // Attach aggregation to leaf
    if (i == plan.agg_leaf_idx) {
      require_run(opts.setAggregation(aggregator) == 0,
                  "Failed to set aggregation.");
      // Add linked projections for GROUP BY on parent columns
      for (Uint32 j = 0; j < plan.num_linked_projs; j++) {
        NdbLinkedOperand* lv = qb->linkedValue(
            opDefs[plan.linked_projs[j].source_op_idx],
            plan.linked_projs[j].column_name);
        require_run(lv != NULL, "Failed to create linked projection.");
        require_run(opts.addLinkedProjection(lv) == 0,
                    "Failed to add linked projection.");
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
    {
      NdbQueryIndexBound bound(keys);
      opDefs[i] = qb->scanIndex(op.index, op.table, &bound, &opts);
      break;
    }
    default:
      abort();
    }
    require_run(opDefs[i] != NULL, "Failed to create child operation.");
  }

  // 3. Prepare and execute
  const NdbQueryDef* queryDef = qb->prepare(ndb);
  require_run(queryDef != NULL, "Failed to prepare query.");

  NdbQuery* query = m_trans->createQuery(queryDef);
  require_run(query != NULL, "Failed to create query.");

  require_run(m_trans->execute(NdbTransaction::NoCommit) == 0,
              "Failed to execute transaction.");

  // 4. Consume all rows
  while (query->nextResult(true) == NdbQuery::NextResult_gotRow) {}

  // 5. Collect and print aggregation results
  NdbAggregator* resultAgg = query->getAggregator();
  ndbrequire(resultAgg != NULL);
  m_resultprinter->print_result(resultAgg, m_conf.out_stream);

  // 6. Cleanup
  query->close();
  queryDef->destroy();
  qb->destroy();
  cleanup_trans();
}
```

### 7c. Implement `programAggregator_join()`

This is similar to the existing `programAggregator()` but handles GROUP BY
columns from different tables. For GROUP BY columns on the leaf table,
use `aggregator->GroupBy(attrId)`. For GROUP BY columns on parent tables,
use `aggregator->GroupByLinked(slot, column)` where `slot` is the linked
projection index.

Declare in `RonSQLPreparer.hpp` (private):
```cpp
void programAggregator_join(NdbAggregator* aggregator);
```

The aggregate function programming (SUM, COUNT, MIN, MAX, Load, etc.)
is identical to the existing `programAggregator()` — it uses the same
`m_agg->m_program` instructions. The only difference is that for join
queries, `m_column_attrId_map[src]` must come from the **leaf table**
(which is already ensured by validation in `load()`).

---

## Step 8: Linked projections for GROUP BY on parent columns

When a GROUP BY column belongs to a non-leaf table, it must be projected
to the leaf via a linked projection. This is handled in two places:

### 8a. In `QueryPlanner::plan()` — detect linked projections

After building all operations, iterate GROUP BY columns from the AST.
For each GROUP BY column whose `m_column_table_idx[col_idx]` is not the
leaf table:

```cpp
plan.linked_projs[plan.num_linked_projs].source_op_idx =
    m_column_table_idx[col_idx];
plan.linked_projs[plan.num_linked_projs].column_name =
    m_columns[col_idx].c_str();
plan.num_linked_projs++;
```

### 8b. In `programAggregator_join()` — use GroupByLinked

For GROUP BY columns that have a linked projection:

```cpp
aggregator->GroupByLinked(linked_proj_slot, column);
```

Where `linked_proj_slot` is the index into the linked projections array,
and `column` is the `NdbDictionary::Column*` from `m_column_map[col_idx]`.

---

## Step 9: WHERE filter on root table

The existing `plan_index_and_filter()` and `apply_filter_top_level()` work
on the root table. For join queries in Phase 1:

- `plan_index_and_filter()` still runs on the root table only.
- WHERE conditions referencing non-root tables: fail with error
  `"WHERE conditions on joined tables are not yet supported."` during
  `load()` validation.
- The root WHERE filter is applied via `NdbInterpretedCode` +
  `NdbQueryOptions::setInterpretedCode()` on the root `scanTable`
  operation (see Step 7b).

For now, convert the existing `NdbScanFilter`-based approach to
`NdbInterpretedCode` for the root operation in join queries. The
single-table path continues using `NdbScanFilter` unchanged.

---

## Step 10: Error handling and validation

### 10a. In the parser

Add error for unsupported join types. If we later add `T_LEFT`, `T_RIGHT`,
`T_INNER`, `T_CROSS` tokens, the parser can reject them with:

```
context->set_err_state(RonSQLPreparer::ErrState::UNSUPPORTED_JOIN_TYPE, ...);
```

For Phase 1, since we don't add these tokens, a `LEFT JOIN` will hit
the `UNIMPLEMENTED_KEYWORD` error for `LEFT` (already handled by the
existing keyword check).

### 10b. In load() — validate column placement

During column resolution for join queries, validate:

1. Aggregate function arguments (SUM, COUNT, etc.) must reference
   columns from the leaf table. If not, throw:
   ```
   "Aggregate function references column 'parent.col' which is not
    in the leaf table 'child'. Aggregation columns must come from
    the last joined table."
   ```

2. WHERE conditions must reference root table columns only (for now).

### 10c. In QueryPlanner — validate join graph

Check that each child's ON condition references a previously-defined
parent table. If not, throw:
```
"Join condition references unknown table 'xxx'. Tables must be
 joined in order, each referencing a previously defined table."
```

---

## Step 11: Build system and build verification

### 11a. Update CMakeLists.txt

Add `QueryPlanner.cpp` as described in Step 5c.

### 11b. Build and verify

```bash
cd debug_build
make -j$(sysctl -n hw.ncpu) ronsql ronsql_cli
```

This builds the ronsql convenience library (including the new
QueryPlanner) and the ronsql_cli binary.

---

## Step 12: Testing

### 12a. Manual testing with ronsql_cli

Start a cluster and run join queries:

```sql
SELECT p.grp, SUM(c.amount) FROM db.parent AS p JOIN db.child AS c
  ON c.parent_id = p.id GROUP BY p.grp;
```

### 12b. Add MTR tests

Create test cases in `mysql-test/suite/ronsql/` or equivalent test
location that exercise:

1. Basic 2-table join with GROUP BY and SUM
2. 3-table join
3. Join with WHERE filter on root table
4. Error cases: missing index, ambiguous column, aggregate on non-leaf

### 12c. Existing tests must still pass

Verify single-table queries are unaffected:

```bash
cd mysql-test
./mtr --suite=ronsql
```

---

## Implementation Order Summary

| Step | Description | Files Modified |
|------|-------------|----------------|
| 1 | Lexer: T_DOT, JOIN, ON tokens | RonSQLLexer.l, Keywords.hpp, RonSQLParser.y |
| 2 | AST: TableRef, JoinClause structs | RonSQLCommon.hpp |
| 3 | Parser: join grammar rules | RonSQLParser.y |
| 4 | Column resolution: qualified names | RonSQLPreparer.hpp, RonSQLPreparer.cpp |
| 5 | QueryPlanner module | QueryPlanner.hpp (new), QueryPlanner.cpp (new), CMakeLists.txt |
| 6 | Multi-table schema loading | RonSQLPreparer.cpp (load()) |
| 7 | Join execution path | RonSQLPreparer.hpp, RonSQLPreparer.cpp (execute_join) |
| 8 | Linked projections for GROUP BY | QueryPlanner.cpp, RonSQLPreparer.cpp |
| 9 | WHERE filter on root | RonSQLPreparer.cpp |
| 10 | Error handling / validation | RonSQLPreparer.cpp, QueryPlanner.cpp |
| 11 | Build verification | CMakeLists.txt |
| 12 | Testing | ronsql_cli, MTR tests |

Steps 1-4 can be implemented and verified together (parser compiles,
single-table queries still work). Steps 5-6 add the planner and schema
loading. Steps 7-9 wire up execution. Step 10-12 harden and test.

---

## Constraints

- `MAX_JOIN_TABLES = 16` (NDB SPJ limit)
- `MAX_JOIN_KEY_COLS = 1` (Phase 1: single-column equi-joins only)
- `MAX_LINKED_PROJS = 16`
- No composite foreign keys (Phase 2)
- No LEFT/RIGHT/CROSS JOIN (Phase 1: inner join only)
- No WHERE on non-root tables (Phase 1)
- No join reordering — tables are joined in FROM-clause order
