# MySQL Handler Integration for Pushdown Join Aggregation (RONDB-733)

## 1. Overview

This document describes the architecture for integrating pushdown join aggregation
into the MySQL server and NDB storage engine handler. The goal: for queries where
the entire join can be pushed to NDB and has GROUP BY + aggregate functions, bypass
MySQL's aggregation layer entirely and return pre-aggregated results from the data
nodes.

### Scope

- **In scope**: Queries where the **entire** join is pushed to NDB and all aggregate
  functions + GROUP BY columns can be expressed in the NdbAggregator program.
- **Out of scope**: Partial pushdown (some tables pushed, some not), HAVING clauses
  (handled by MySQL post-aggregation), DISTINCT aggregates, ROLLUP, window functions.
- **Fallback**: If any condition prevents aggregation pushdown, the query falls back
  to MySQL's normal aggregation with no change in behavior.

### Prerequisites (All Complete)

- DBLQH/DBTUP: AggInterpreter, JoinAggregationState, handleJoinAggRow (Phases 1-6)
- DBTC: JOIN_AGG_SETUP/COMPLETE/RELEASE, aggStateKeys (Phase 7)
- NDB API: NdbQueryOptions::setAggregation(), NdbQuery::getAggregator() (Phase 8)

---

## 2. MySQL Aggregation Architecture

### 2.1 The Two Aggregation Paths

MySQL has two aggregation strategies, represented by AccessPath types:

1. **AGGREGATE (streaming)**: `AggregateIterator` in `composite_iterators.cc:253`.
   Assumes input is sorted by GROUP BY columns. Reads rows one at a time from its
   source, detects group boundaries via `update_item_cache_if_changed()`, and calls
   `Item_sum::aggregator_add()` for each row. Outputs one row per group. Used when
   input is already sorted or for implicit grouping (no GROUP BY).

2. **TEMPTABLE_AGGREGATE**: `TemptableAggregateIterator` in `composite_iterators.cc:3721`.
   Inserts all rows into a temporary table (with grouping), then scans the temp table.
   Used when input is not sorted by GROUP BY columns and sorting would be expensive.

Both paths rely on `Item_sum` subclasses (`Item_sum_count`, `Item_sum_sum`, etc.)
evaluated via the MySQL expression evaluation framework — the aggregation values
accumulate in the `Item_sum` objects themselves.

### 2.2 The AccessPath Tree for a Typical Aggregate Join Query

```sql
SELECT t1.col, COUNT(*), SUM(t2.amount)
FROM t1 JOIN t2 ON t2.parent_id = t1.id
GROUP BY t1.col;
```

Without pushdown, the AccessPath tree looks like:
```
AGGREGATE (or TEMPTABLE_AGGREGATE)
  └── NESTED_LOOP_JOIN (or HASH_JOIN)
        ├── TABLE_SCAN(t1)       [outer]
        └── EQ_REF(t2)           [inner]
```

With join pushdown (current, no aggregation):
```
AGGREGATE
  └── NESTED_LOOP_JOIN
        ├── TABLE_SCAN(t1)       [root of pushed join]
        └── PUSHED_JOIN_REF(t2)  [child, reads from pushed results]
```

### 2.3 Where Aggregation Decisions Are Made

1. **Old optimizer**: `JOIN::create_access_paths()` in `sql/sql_executor.cc:~3160`
   inserts `NewAggregateAccessPath()` or temp table aggregate based on
   `streaming_aggregation` flag and `QEP_TAB::op_type`.

2. **Hypergraph optimizer**: `sql/join_optimizer/join_optimizer.cc` creates
   `AGGREGATE`/`TEMPTABLE_AGGREGATE` access paths.

3. **push_to_engines()**: Called from `sql/sql_optimizer.cc:1099` (old optimizer)
   or `sql/join_optimizer/finalize_plan.cc:765` (hypergraph). This calls
   `handlerton::push_to_engine` which for NDB dispatches to
   `ndbcluster_push_to_engine()` in `ha_ndbcluster.cc:14839`.

**Critical insight**: `push_to_engines()` is called **after** the aggregation
AccessPath has been constructed. The AGGREGATE node already sits above the join
tree. We need to either:
- (a) Replace the AGGREGATE node entirely, or
- (b) Make the AGGREGATE node a no-op that passes through pre-aggregated rows.

### 2.4 Item_sum Interface

```cpp
class Item_sum : public Item_func {
    enum Sumfunctype {
        COUNT_FUNC, COUNT_DISTINCT_FUNC, SUM_FUNC, SUM_DISTINCT_FUNC,
        AVG_FUNC, AVG_DISTINCT_FUNC, MIN_FUNC, MAX_FUNC, ...
    };
    virtual enum Sumfunctype sum_func() const = 0;
    bool aggregator_add();   // Add current row to aggregate
    bool reset_and_add();    // Reset and add first row of new group
    void clear();            // Reset to initial state
};
```

Supported for pushdown: `COUNT_FUNC`, `SUM_FUNC`, `MIN_FUNC`, `MAX_FUNC`.
Not supported: `DISTINCT` variants, `AVG_FUNC` (compute from SUM/COUNT),
`GROUP_CONCAT`, `JSON_AGG_FUNC`, window functions.

---

## 3. NDB Join Pushdown Architecture (Current)

### 3.1 Entry Point: ndbcluster_push_to_engine()

`ha_ndbcluster.cc:14839`:
```cpp
int ndbcluster_push_to_engine(THD *thd, AccessPath *root_path, JOIN *join) {
    ndb_pushed_builder_ctx pushed_builder(thd, root_path, join);

    if (THDVAR(thd, join_pushdown)) {
        const int error = pushed_builder.make_pushed_join();
        if (unlikely(error)) return error;
    }
    // ... condition pushdown for non-pushed tables ...
    fixup_pushed_access_paths(thd, root_path, join, /*filter=*/nullptr);
    return 0;
}
```

### 3.2 The ndb_pushed_builder_ctx

Declared in `ha_ndbcluster_push.h:677`. Key flow:

1. **Constructor**: Walks the AccessPath tree via `construct()`, populates
   `m_tables[]` array of `pushed_table` structs with access types, join nest
   info, key references.

2. **make_pushed_join()** (`ha_ndbcluster_push.cc:644`): For each potential
   root table, calls:
   - `is_pushable_with_root()` — checks if children can be pushed
   - `accept_query_plan()` — heuristic rejection (e.g., too few rows)
   - `optimize_query_plan()` — choose parent for each child, optimize tree
   - `build_query()` — builds NdbQueryDef via NdbQueryBuilder

3. **build_query()** (`ha_ndbcluster_push.cc:2467`): Iterates over pushed tables,
   calls `m_builder->readTuple()`, `scanIndex()`, or `scanTable()` for each.
   Sets NdbQueryOptions (matchType, firstInner, parameters).

4. **Result**: Creates `ndb_pushed_join` with `NdbQueryDef`.
   Each handler gets `m_pushed_join_member` and `m_pushed_join_operation` index.

### 3.3 AccessPath Fixup

`fixup_pushed_access_paths()` (`ha_ndbcluster.cc:14658`) walks the AccessPath tree:
- For `REF`/`EQ_REF` nodes that are children of pushed joins: replaces with
  `PUSHED_JOIN_REF` via `accept_pushed_child_joins()`.
- Removes fully-pushed FILTER conditions.
- Validates containment (no pushed members outside branch for AGGREGATE etc.).

### 3.4 Runtime: create_pushed_join()

`ha_ndbcluster.cc:14989`: Called at scan init time by the root handler.
Creates `NdbQuery` from `NdbQueryDef`, binds each child handler's
`m_pushed_operation` to the corresponding `NdbQueryOperation`, sets up
result buffers and pushed conditions.

### 3.5 ndb_join_pushdown Variable

`ha_ndbcluster.cc:364`:
```cpp
static MYSQL_THDVAR_BOOL(join_pushdown,
    PLUGIN_VAR_OPCMDARG,
    "Enable pushing down of join to datanodes",
    nullptr, nullptr, true  // default ON
);
```
Accessed as `THDVAR(thd, join_pushdown)`. Provides the pattern for our new variable.

---

## 4. Design: Aggregation Pushdown Integration

### 4.1 New Session Variable

```cpp
static MYSQL_THDVAR_BOOL(join_pushdown_aggregate,
    PLUGIN_VAR_OPCMDARG,
    "Enable pushing down of aggregation for pushed joins to datanodes",
    nullptr, nullptr, false  // default OFF initially, enable when stable
);
```

Checked as: `THDVAR(thd, join_pushdown_aggregate)`.

This provides a clean separation: `ndb_join_pushdown` controls join pushdown,
`ndb_join_pushdown_aggregate` controls the aggregation extension. Both must be
ON for aggregation pushdown to occur. Default OFF ensures zero risk to existing
behavior during development.

### 4.2 Detection: Which Queries Can Have Aggregation Pushed?

A pushed join can have aggregation pushed if ALL of these conditions hold:

1. **Entire join is pushed** — all tables in the join are part of the pushed
   query. If MySQL still needs to do any joining, it needs the raw rows.

2. **Query has GROUP BY or implicit aggregation** — `join->grouped` or
   `join->group_optimized_away` or aggregate functions present.

3. **All aggregate functions are pushable** — only COUNT(*), SUM, MIN, MAX
   on columns of pushable types (integer, float, double). No DISTINCT,
   no AVG (unless we decompose to SUM/COUNT), no GROUP_CONCAT.

4. **All GROUP BY columns come from pushed tables** — each `group_fields`
   item must reference a column from a table in the pushed join.

5. **No ROLLUP** — `query_block->olap != ROLLUP_TYPE`.

6. **No HAVING with aggregate references** — HAVING can be applied post-aggregation
   by MySQL, but aggregate functions in HAVING must match what was pushed.
   (Initially: reject if HAVING references aggregates not directly available.)

7. **Feature is enabled** — `THDVAR(thd, join_pushdown_aggregate)` is true.

### 4.3 Strategy: Replace AGGREGATE AccessPath

The most important architectural decision. Two approaches were considered:

**Approach A: Replace AGGREGATE node** (CHOSEN)

In `ndbcluster_push_to_engine()`, after `make_pushed_join()` succeeds and we
detect that aggregation can be pushed:
- Walk the AccessPath tree upward from the pushed join subtree.
- Find the AGGREGATE (or TEMPTABLE_AGGREGATE) node above it.
- Remove the AGGREGATE node from the tree — the pushed join root's handler will
  return pre-aggregated rows directly.
- The source iterator for the join returns aggregate result rows instead of
  raw data rows — one per group.

This approach requires the root handler's scan methods to return aggregate
result rows formatted as if they were table rows — populating the MySQL
row buffer with GROUP BY column values and aggregate results.

**Approach B: Make AGGREGATE a pass-through** (REJECTED)

Keep the AGGREGATE node but mark the `Item_sum` objects as "pre-computed" so
`aggregator_add()` is a no-op and the values are already correct. This is
fragile because:
- AggregateIterator's group-change detection would still fire on every row.
- `update_item_cache_if_changed()` compares GROUP BY values, which requires
  rows to be sorted by GROUP BY — but NDB returns groups in hash order.
- Would need to either sort results or hack the group-change detection.

### 4.4 How to Return Pre-Aggregated Rows

When aggregation is pushed, the root handler in `ha_ndbcluster` needs to:

1. At scan init time: call `NdbQueryOptions::setAggregation(agg)` on the leaf
   operation when creating the NdbQuery.

2. After scan completes (`NdbQuery::NextResult_scanComplete`): retrieve results
   from `NdbQuery::getAggregator()->FetchResultRecord()`.

3. For each `FetchResultRecord()`: populate the MySQL row buffer with:
   - GROUP BY column values (from `FetchGroupbyColumn()`)
   - Aggregate results (from `FetchAggregationResult()`)

4. Return rows one at a time to the caller (MySQL's iterator).

This means the root handler's `next_result()` / `fetch_next_pushed()` path needs
a new branch for aggregation mode where results come from the NdbAggregator
rather than from NdbQueryOperation row buffers.

### 4.5 Bypassing the AGGREGATE AccessPath Node

The key insight: when we remove the AGGREGATE node, the scan root's iterator
directly feeds into whatever is above (LIMIT, HAVING filter, or result output).
Each call to the root iterator's Read() returns one aggregate group row.

In `fixup_pushed_access_paths()`, when we detect a pushed join with aggregation
under an AGGREGATE AccessPath:
```
Before:                          After:
AGGREGATE                        TABLE_SCAN(t1) [agg mode]
  └── NESTED_LOOP_JOIN             (returns aggregate rows directly)
        ├── TABLE_SCAN(t1)
        └── PUSHED_JOIN_REF(t2)
```

The NESTED_LOOP_JOIN and PUSHED_JOIN_REF children become irrelevant — the root
TABLE_SCAN returns pre-aggregated rows. We collapse the tree.

**But wait** — we can't simply remove intermediate nodes if MySQL's iterator
infrastructure expects them. A cleaner approach:

### 4.6 Refined Strategy: Aggregate Result Iterator

Rather than removing the AGGREGATE node from the tree entirely (which would
require careful handling of all parent nodes), we introduce a conceptual change:

1. **Keep the join tree structure intact** during planning (join pushdown
   happens as normal in `make_pushed_join()`).

2. **In `fixup_pushed_access_paths()`**: When we detect a pushed aggregate join:
   - Call `ndb_remove_aggregate_access_path()` (in new file `ha_ndbcluster_push_agg.cc`)
     to remove the AGGREGATE node (replace with its child).
   - The root TABLE_SCAN handler will return rows in "aggregate mode".
   - Each Read() on the root returns one aggregate group.

3. **In `ha_ndbcluster::create_pushed_join()`**: When aggregation is enabled:
   - The NdbAggregator program was already built during `make_pushed_join()`
     (by `ndb_build_aggregation_program()` in the new file).
   - `NdbQueryOptions::setAggregation()` was already applied to the leaf
     operation definition in the NdbQueryDef.
   - Store the NdbAggregator for result retrieval.

4. **In the handler's scan loop**: After the NdbQuery scan completes:
   - The aggregate branch in `fetch_next_pushed()` delegates to
     `ndb_fetch_next_aggregate()` (in the new file).
   - It calls `FetchResultRecord()`, populates MySQL's row buffer with
     aggregate values, and sets `Item_sum` pushed values.
   - Returns 0 (row found) or HA_ERR_END_OF_FILE (done).

### 4.7 Populating MySQL's Row Buffer

This is the critical bridge between NdbAggregator results and MySQL's type system.
All population logic lives in `ndb_populate_aggregate_row()` in the new file
`ha_ndbcluster_push_agg.cc`.

For each result group from `NdbAggregator::ResultRecord`:

**GROUP BY columns**: Use `FetchGroupbyColumn()` which returns `NdbAggregator::Column`
with type, data pointer, null flag. Write into the corresponding `Field` in MySQL's
row buffer:
```cpp
Field *field = table->field[col_index];
if (column.is_null()) {
    field->set_null();
} else {
    field->store(column.ptr(), column.byte_size(), &my_charset_bin);
}
```

**Aggregate results**: Use `FetchAggregationResult()` which returns
`NdbAggregator::Result` with type (Int64/Uint64/Double) and null flag.
For each `Item_sum` in `join->sum_funcs`, use the surgical `set_pushed_value_int()`
/ `set_pushed_value_double()` setters (added to `Item_sum` base class in
`item_sum.h` — see Section 5.7):
```cpp
Item_sum *item = join->sum_funcs[i];
NdbAggregator::Result res = record.FetchAggregationResult();
if (res.is_null()) {
    item->set_pushed_null();
} else if (res.type() == NdbAggregator::Result::Double) {
    item->set_pushed_value_double(res.data_double());
} else {
    item->set_pushed_value_int(res.data_int64());
}
```

This avoids directly poking into Item_sum subclass internals. The setters on the
`Item_sum` base class are a clean, minimal interface (~15 lines in `item_sum.h`).

### 4.8 The precomputed_group_by Mechanism

MySQL already has a concept for engines that provide pre-grouped rows:
`Temp_table_param::precomputed_group_by`. When set, MySQL's aggregation
code (`create_access_paths()`) skips inserting an AGGREGATE AccessPath node.

From `sql_executor.cc:3162`:
```cpp
if (!qep_tab->tmp_table_param->precomputed_group_by) {
    path = NewAggregateAccessPath(thd, path, query_block->olap);
}
```

And from `sql_executor.cc:3406`:
```cpp
if (!tmp_table_param.precomputed_group_by) {
    path = NewAggregateAccessPath(thd, path, query_block->olap);
}
```

This is a promising pattern: if we set `precomputed_group_by = true`, MySQL
won't add the AGGREGATE node in the first place. But this flag is set during
access path construction (before `push_to_engines()` is called), so we'd
need to set it earlier or use a different approach.

**Key finding**: `push_to_engines()` is called AFTER access path construction.
So we cannot use `precomputed_group_by` to prevent the AGGREGATE node from
being created. We must remove or bypass it after the fact.

---

## 5. Detailed Implementation Plan

### Design Principle: New Files + Surgical Changes

All substantial new code goes into **new files** in the NDB plugin directory.
Changes to existing MySQL server and NDB storage engine files are kept
**small and surgical** — typically a single function call, a few-line hook, or
a declaration. This minimizes merge conflicts, protects existing behavior, and
makes the feature easy to review and revert.

### Phase 1: Infrastructure and Detection

#### 5.1 New Session Variable

**File (surgical change)**: `storage/ndb/plugin/ha_ndbcluster.cc`

Add after the `join_pushdown` variable declaration (line ~364):
```cpp
static MYSQL_THDVAR_BOOL(join_pushdown_aggregate,
    PLUGIN_VAR_OPCMDARG,
    "Enable pushing down of aggregation for pushed joins to datanodes",
    nullptr, nullptr, false  // default OFF
);
```

Register in the system variable array (search for `join_pushdown` in the
sys_vars array). **~5 lines changed total**.

#### 5.2 New File: ha_ndbcluster_push_agg.h

**New file**: `storage/ndb/plugin/ha_ndbcluster_push_agg.h`

Header for all aggregation pushdown logic. Contains:

```cpp
#ifndef HA_NDBCLUSTER_PUSH_AGG_H
#define HA_NDBCLUSTER_PUSH_AGG_H

class JOIN;
class ndb_pushed_builder_ctx;
class ndb_pushed_join;
class NdbAggregator;
class NdbDictionary::Table;

/**
 * Check if aggregation can be pushed for a fully-pushed join.
 * Returns true if all conditions hold:
 * - All tables in the query are pushed
 * - Query has GROUP BY or aggregate functions
 * - All aggregate functions are pushable (COUNT, SUM, MIN, MAX)
 * - No DISTINCT, ROLLUP, or unsupported types
 * - Feature is enabled via ndb_join_pushdown_aggregate
 */
bool ndb_can_push_aggregation(THD *thd,
                              const JOIN *join,
                              const ndb_pushed_builder_ctx &builder,
                              const ndb_pushed_join *pushed_join);

/**
 * Build an NdbAggregator program from MySQL's query plan.
 * Maps Item_sum objects to NdbAggregator instructions,
 * GROUP BY columns to GroupBy/GroupByLinked calls.
 * Returns 0 on success, error code on failure.
 */
int ndb_build_aggregation_program(const JOIN *join,
                                  const ndb_pushed_join *pushed_join,
                                  const NdbDictionary::Table *leaf_table,
                                  NdbAggregator &agg);

/**
 * Modify AccessPath tree: remove AGGREGATE (or TEMPTABLE_AGGREGATE)
 * node above a fully-pushed aggregate join. Collapses the tree so the
 * root handler returns pre-aggregated rows directly.
 */
void ndb_remove_aggregate_access_path(AccessPath *root_path,
                                      const JOIN *join);

/**
 * Populate MySQL's row buffer and Item_sum values from NdbAggregator
 * result record. Called once per aggregate group row.
 * Returns 0 on success, HA_ERR_END_OF_FILE when done.
 */
int ndb_populate_aggregate_row(/* ... parameters TBD ... */);

#endif
```

#### 5.3 New File: ha_ndbcluster_push_agg.cc

**New file**: `storage/ndb/plugin/ha_ndbcluster_push_agg.cc`

Contains **all** aggregation pushdown logic. This is the bulk of the new code
(estimated 400-600 lines). Includes implementations for:

1. **`ndb_can_push_aggregation()`** — Detection logic:
   - Checks `THDVAR(thd, join_pushdown_aggregate)`.
   - Verifies all tables are pushed (`pushed_join->get_operation_count()
     == builder.m_table_count`).
   - Iterates `join->sum_funcs` to check `sum_func()` types (only
     COUNT_FUNC, SUM_FUNC, MIN_FUNC, MAX_FUNC).
   - Checks `join->query_block->olap != ROLLUP_TYPE`.
   - Verifies GROUP BY columns reference pushed tables.

2. **`ndb_build_aggregation_program()`** — NdbAggregator construction:
   - For each GROUP BY column in `join->group_list`:
     - Leaf table column: `agg.GroupBy(col_id)`
     - Parent table column: `agg.GroupByLinked(position, col_id, col)`
   - For each `Item_sum` in `join->sum_funcs`:
     - `COUNT_FUNC`: `agg.Count(agg_idx, reg_id)`
     - `SUM_FUNC`: Load column, `agg.Sum(agg_idx, reg_id)`
     - `MIN_FUNC`: Load column, `agg.Min(agg_idx, reg_id)`
     - `MAX_FUNC`: Load column, `agg.Max(agg_idx, reg_id)`
   - Maps `Item_field::field_index` → NDB column ID.

3. **`ndb_remove_aggregate_access_path()`** — AccessPath tree surgery:
   - Walks tree to find AGGREGATE or TEMPTABLE_AGGREGATE above the
     pushed join subtree.
   - Replaces with its child, collapsing the tree.
   - Handles SORT removal (NDB returns groups in hash order).

4. **`ndb_populate_aggregate_row()`** — Result population:
   - Reads GROUP BY values from `FetchGroupbyColumn()`.
   - Reads aggregate values from `FetchAggregationResult()`.
   - Writes GROUP BY columns into MySQL `Field` objects.
   - Sets `Item_sum` pushed values (see 5.7 below).

5. **Helper functions**: Column type mapping, linked projection setup,
   `Item_field` → NDB column resolution.

#### 5.4 Linked Projection for Parent GROUP BY Columns

Handled inside `ndb_build_aggregation_program()` in the new file.

When GROUP BY references a column from a parent table in the pushed join:
1. The column is included in the SPJ linked attributes (data passed from
   parent to child in LQHKEYREQ).
2. The aggregation program uses `GroupByLinked()` with the linked position.

### Phase 2: Surgical Changes to Existing Files

#### 5.5 Hook into ndbcluster_push_to_engine() (~10 lines)

**File (surgical change)**: `storage/ndb/plugin/ha_ndbcluster.cc`

In `ndbcluster_push_to_engine()`, add after the `make_pushed_join()` call:
```cpp
#include "ha_ndbcluster_push_agg.h"

// After make_pushed_join() succeeds:
if (pushed_builder.get_pushed_join() != nullptr &&
    ndb_can_push_aggregation(thd, join, pushed_builder,
                             pushed_builder.get_pushed_join())) {
    const int agg_error = pushed_builder.build_aggregation(join);
    if (unlikely(agg_error)) {
        // Fall through — aggregation won't be pushed, join-only pushdown
    }
}
```

#### 5.6 Hook into fixup_pushed_access_paths() (~5 lines)

**File (surgical change)**: `storage/ndb/plugin/ha_ndbcluster.cc`

In `fixup_pushed_access_paths()`, add a case for AGGREGATE:
```cpp
case AccessPath::AGGREGATE:
case AccessPath::TEMPTABLE_AGGREGATE:
    if (has_pushed_aggregation) {
        ndb_remove_aggregate_access_path(subpath, join);
    }
    break;
```

The `ndb_remove_aggregate_access_path()` function is in the new file.

#### 5.7 Item_sum Pushed Value Support (~15 lines in MySQL server)

**File (surgical change)**: `sql/item_sum.h`

Add minimal members to `Item_sum` base class:
```cpp
class Item_sum : public Item_func {
    // ... existing members ...

    // Pushed aggregate support (NDB pushdown)
    bool m_pushed_aggregate{false};
    int64_t m_pushed_value_int{0};
    double m_pushed_value_double{0.0};
    bool m_pushed_null{false};

 public:
    void set_pushed_value_int(int64_t val) {
        m_pushed_aggregate = true; m_pushed_value_int = val; m_pushed_null = false;
    }
    void set_pushed_value_double(double val) {
        m_pushed_aggregate = true; m_pushed_value_double = val; m_pushed_null = false;
    }
    void set_pushed_null() {
        m_pushed_aggregate = true; m_pushed_null = true;
    }
    bool is_pushed_aggregate() const { return m_pushed_aggregate; }
};
```

**File (surgical change)**: `sql/item_sum.cc`

Add early-return checks to `val_int()` and `val_real()` for the relevant
`Item_sum` subclasses (`Item_sum_count`, `Item_sum_sum`, `Item_sum_min`,
`Item_sum_max`). Each is a 2-line addition at the top of the method:

```cpp
longlong Item_sum_count::val_int() {
    if (m_pushed_aggregate) { null_value = m_pushed_null; return m_pushed_value_int; }
    // ... original code unchanged ...
}
```

**Total MySQL server changes**: ~15 lines in `item_sum.h`, ~8 lines in `item_sum.cc`
(2 lines × 4 subclasses). Zero risk to non-NDB queries since `m_pushed_aggregate`
defaults to `false`.

#### 5.8 Mark ndb_pushed_join for Aggregation (~3 lines)

**File (surgical change)**: `storage/ndb/plugin/ha_ndbcluster_push.h`

Add to `ndb_pushed_join`:
```cpp
class ndb_pushed_join {
    // ... existing members ...
    bool m_has_aggregation{false};
    NdbAggregator *m_aggregator{nullptr};
};
```

**File (surgical change)**: `storage/ndb/plugin/ha_ndbcluster.h`

Add to `ha_ndbcluster`:
```cpp
class ha_ndbcluster : public handler {
    // ... existing members ...
    bool m_pushed_agg_mode{false};
};
```

### Phase 3: Runtime Execution

#### 5.9 Hook into create_pushed_join() (~10 lines)

**File (surgical change)**: `storage/ndb/plugin/ha_ndbcluster.cc`

In `create_pushed_join()`, add aggregation setup:
```cpp
// After NdbQuery creation:
if (m_pushed_join_member->m_has_aggregation) {
    m_pushed_agg_mode = true;
    // NdbQueryDef already has setAggregation() applied
    // (done during build in make_pushed_join)
}
```

#### 5.10 Hook into fetch_next_pushed() (~5 lines)

**File (surgical change)**: `storage/ndb/plugin/ha_ndbcluster.cc`

Add aggregate-mode branch at the top of `fetch_next_pushed()`:
```cpp
int ha_ndbcluster::fetch_next_pushed() {
    if (m_pushed_agg_mode) {
        return ndb_fetch_next_aggregate(this, table->record[0]);
    }
    // ... existing code unchanged ...
}
```

The `ndb_fetch_next_aggregate()` function lives in `ha_ndbcluster_push_agg.cc`.
It handles the full result iteration:
- Calls `NdbAggregator::FetchResultRecord()`
- Populates GROUP BY columns in MySQL's row buffer via `Field::store()`
- Sets `Item_sum` pushed values via `set_pushed_value_int()`/`set_pushed_value_double()`
- Returns 0 (row found) or `HA_ERR_END_OF_FILE` (done)

#### 5.11 Hook into build_query() (~5 lines)

**File (surgical change)**: `storage/ndb/plugin/ha_ndbcluster_push.cc`

In `build_query()`, at the point where NdbQueryOptions are set for the leaf
operation, add:
```cpp
if (m_has_aggregation) {
    leaf_options.setAggregation(*m_aggregator);
}
```

### Summary of Changes by File

#### New Files (all new code here)

| File | Lines (est.) | Purpose |
|------|-------------|---------|
| `storage/ndb/plugin/ha_ndbcluster_push_agg.h` | ~50 | Declarations for all agg pushdown functions |
| `storage/ndb/plugin/ha_ndbcluster_push_agg.cc` | ~500 | Detection, program build, AccessPath surgery, result population |

#### Surgical Changes to Existing Files

| File | Lines Changed | What |
|------|--------------|------|
| `storage/ndb/plugin/ha_ndbcluster.cc` | ~30 | THDVAR, hooks in push_to_engine/fixup/create/fetch |
| `storage/ndb/plugin/ha_ndbcluster.h` | ~3 | `m_pushed_agg_mode` member |
| `storage/ndb/plugin/ha_ndbcluster_push.h` | ~3 | `m_has_aggregation`, `m_aggregator` on ndb_pushed_join |
| `storage/ndb/plugin/ha_ndbcluster_push.cc` | ~5 | `setAggregation()` call in build_query() |
| `sql/item_sum.h` | ~15 | Pushed value members + setters on Item_sum |
| `sql/item_sum.cc` | ~8 | Early-return in val_int()/val_real() for 4 subclasses |
| `storage/ndb/plugin/CMakeLists.txt` | ~1 | Add ha_ndbcluster_push_agg.cc to build |
| **Total surgical** | **~65 lines** | |

---

## 6. AccessPath Tree Transformation — Detailed

### 6.1 Before push_to_engines()

```
LIMIT_OFFSET (if LIMIT clause)
  └── FILTER (if HAVING clause)
        └── AGGREGATE
              └── SORT (if GROUP BY needs sorting)
                    └── NESTED_LOOP_JOIN
                          ├── TABLE_SCAN(t1)         [root]
                          └── EQ_REF(t2)             [child]
```

### 6.2 After push_to_engines() with join pushdown only (current)

```
LIMIT_OFFSET
  └── FILTER (HAVING)
        └── AGGREGATE
              └── SORT
                    └── NESTED_LOOP_JOIN
                          ├── TABLE_SCAN(t1)         [root of push]
                          └── PUSHED_JOIN_REF(t2)    [child]
```

### 6.3 After push_to_engines() with join + aggregation pushdown (new)

```
LIMIT_OFFSET
  └── FILTER (HAVING — evaluated by MySQL on returned rows)
        └── TABLE_SCAN(t1) [aggregate mode — returns pre-aggregated rows]
```

The AGGREGATE node, SORT node, NESTED_LOOP_JOIN, and PUSHED_JOIN_REF are all
removed. The root TABLE_SCAN (or whatever the root's AccessPath type is)
returns one row per group directly.

Note: The SORT can be removed because NDB returns groups in hash order, and
MySQL doesn't need sorted groups when there's no ORDER BY (or ORDER BY will
be added back separately if needed).

If there is an ORDER BY that matches GROUP BY, we may need to keep a SORT
above the root scan, or have MySQL sort the aggregate results.

### 6.4 HAVING Handling

HAVING clauses reference `Item_sum` objects. Since we've set up those objects
to return pre-computed values, the HAVING FILTER can evaluate normally.
MySQL will call `Item_sum::val_int()` etc., which return the pushed values.

---

## 7. File Organization

### Principle: New Code in New Files

All substantial new logic lives in two new files. Existing files receive only
small hook points (function calls, member declarations, early-return guards).
This keeps diffs small, avoids merge conflicts, and makes the feature easy to
review, test, and revert.

### New Files

| File | Lines (est.) | Contents |
|------|-------------|----------|
| `storage/ndb/plugin/ha_ndbcluster_push_agg.h` | ~50 | Public API: `ndb_can_push_aggregation()`, `ndb_build_aggregation_program()`, `ndb_remove_aggregate_access_path()`, `ndb_fetch_next_aggregate()`, `ndb_populate_aggregate_row()` |
| `storage/ndb/plugin/ha_ndbcluster_push_agg.cc` | ~500 | Full implementations: detection, NdbAggregator program construction, AccessPath tree surgery, result iteration, Item_sum value injection, column type mapping |

### Surgical Changes to Existing Files (~65 lines total)

| File | Lines | Change Description |
|------|-------|--------------------|
| `storage/ndb/plugin/ha_ndbcluster.cc` | ~30 | THDVAR declaration, hook in `ndbcluster_push_to_engine()`, hook in `fixup_pushed_access_paths()`, hook in `create_pushed_join()`, branch in `fetch_next_pushed()` |
| `storage/ndb/plugin/ha_ndbcluster.h` | ~3 | `m_pushed_agg_mode` member on `ha_ndbcluster` |
| `storage/ndb/plugin/ha_ndbcluster_push.h` | ~3 | `m_has_aggregation` + `m_aggregator` on `ndb_pushed_join` |
| `storage/ndb/plugin/ha_ndbcluster_push.cc` | ~5 | `setAggregation()` call in `build_query()` |
| `sql/item_sum.h` | ~15 | `m_pushed_aggregate` flag, pushed value members, setter methods on `Item_sum` |
| `sql/item_sum.cc` | ~8 | 2-line early-return in `val_int()`/`val_real()` for 4 subclasses |
| `storage/ndb/plugin/CMakeLists.txt` | ~1 | Add `ha_ndbcluster_push_agg.cc` to source list |

---

## 8. Data Flow Summary

```
                        MySQL Optimizer
                             |
                             v
               ndbcluster_push_to_engine()
                    |              |
                    v              v
          make_pushed_join()   ndb_can_push_aggregation()
                    |              |
                    v              v
          build_query()  +  ndb_build_aggregation_program()
              |                    |
              v                    v
      NdbQueryDef    +    NdbAggregator program
              |                    |
              v                    v
    fixup_pushed_access_paths():
      - Replace EQ_REF → PUSHED_JOIN_REF
      - Remove AGGREGATE node (if agg pushed)
      - Set up Item_sum pushed values
                             |
                             v
                    EXECUTION TIME
                             |
              create_pushed_join()
                    |
                    v
        NdbQuery with setAggregation()
                    |
                    v
    DBTC → DBSPJ → DBLQH (AggInterpreter)
                    |
                    v
        TRANSID_AI results → NdbAggregator
                    |
                    v
    ndb_fetch_next_aggregate()  [ha_ndbcluster_push_agg.cc]
      - FetchResultRecord()
      - Populate row buffer + Item_sum values
                    |
                    v
    MySQL: evaluate HAVING, apply LIMIT, return to client
```

---

## 9. Risks and Mitigations

### 9.1 Item_sum Modification

**Risk**: Modifying `Item_sum` classes in the MySQL server codebase affects
all storage engines and query paths.

**Mitigation**: The change is extremely surgical: ~15 lines in `item_sum.h`
(member declarations + inline setters) and ~8 lines in `item_sum.cc` (2-line
early-return guard in 4 methods). The `m_pushed_aggregate` flag is false by
default and only set for NDB pushed aggregate queries. The changes to
`val_int()` etc. are guarded by an early `if (m_pushed_aggregate)` check with
zero overhead for other engines. All substantial logic remains in the NDB
plugin's new file. Long-term, this could become a MySQL server feature for
any engine that supports aggregate pushdown.

### 9.2 AccessPath Tree Manipulation

**Risk**: Removing AGGREGATE from the tree could break invariants expected by
other parts of MySQL (EXPLAIN, optimizer statistics, etc.).

**Mitigation**: Keep cost estimates on the replacement node. For EXPLAIN, the
pushed aggregation should show as a note (similar to how pushed joins show
"Using pushed join"). Test with EXPLAIN ANALYZE to verify.

### 9.3 Type Conversion

**Risk**: NdbAggregator returns Int64/Uint64/Double, but MySQL's type system
has more nuanced types (DECIMAL, FLOAT vs DOUBLE, signed vs unsigned).

**Mitigation**: Start with integer and double types only. NdbAggregator already
handles INT/BIGINT/FLOAT/DOUBLE. DECIMAL support can be added later. Reject
queries with unsupported column types in `can_push_aggregation()`.

### 9.4 NULL Handling

**Risk**: Aggregate functions handle NULLs specially (COUNT(*) vs COUNT(col),
SUM ignoring NULLs, etc.). NdbAggregator must match MySQL semantics.

**Mitigation**: NdbAggregator already handles NULLs correctly for COUNT/SUM/MIN/MAX.
Verify with test cases including NULL values in GROUP BY columns and aggregated
columns.

### 9.5 Empty Result Sets

**Risk**: For queries without GROUP BY, MySQL returns one row with default
aggregate values (COUNT=0, SUM=NULL). For queries with GROUP BY and no matching
rows, MySQL returns zero rows.

**Mitigation**: NdbAggregator already handles this: with GROUP BY, zero groups
means zero result rows. Without GROUP BY, a single result with initial values.
The handler must correctly translate these cases.

---

## 10. Testing Strategy

### 10.1 MTR Tests

```sql
-- Basic: pushed join + GROUP BY + SUM
SELECT t1.col, SUM(t2.amount)
FROM t1 JOIN t2 ON t2.fk = t1.pk
GROUP BY t1.col;

-- COUNT(*)
SELECT t1.col, COUNT(*)
FROM t1 JOIN t2 ON t2.fk = t1.pk
GROUP BY t1.col;

-- Multiple aggregates
SELECT t1.col, COUNT(*), SUM(t2.amount), MIN(t2.amount), MAX(t2.amount)
FROM t1 JOIN t2 ON t2.fk = t1.pk
GROUP BY t1.col;

-- With WHERE filter
SELECT t1.col, SUM(t2.amount)
FROM t1 JOIN t2 ON t2.fk = t1.pk
WHERE t2.amount > 100
GROUP BY t1.col;

-- With HAVING
SELECT t1.col, SUM(t2.amount) as total
FROM t1 JOIN t2 ON t2.fk = t1.pk
GROUP BY t1.col
HAVING total > 500;

-- Empty result
SELECT t1.col, SUM(t2.amount)
FROM t1 JOIN t2 ON t2.fk = t1.pk
WHERE 1=0
GROUP BY t1.col;

-- Implicit aggregation (no GROUP BY)
SELECT COUNT(*), SUM(t2.amount)
FROM t1 JOIN t2 ON t2.fk = t1.pk;

-- Verify fallback when ndb_join_pushdown_aggregate=OFF
SET ndb_join_pushdown_aggregate=OFF;
SELECT t1.col, SUM(t2.amount)
FROM t1 JOIN t2 ON t2.fk = t1.pk
GROUP BY t1.col;
-- Should produce same results via MySQL aggregation

-- 3-way join
SELECT t1.col, t2.cat, SUM(t3.amount)
FROM t1 JOIN t2 ON t2.fk = t1.pk
         JOIN t3 ON t3.fk = t2.pk
GROUP BY t1.col, t2.cat;
```

### 10.2 Correctness Verification

For every pushdown test, run the same query with `ndb_join_pushdown_aggregate=OFF`
and compare results (order-independent). This validates that pushdown produces
identical results to MySQL's aggregation.

### 10.3 EXPLAIN Verification

Verify that EXPLAIN shows the aggregation pushdown:
```
EXPLAIN SELECT t1.col, SUM(t2.amount) FROM t1 JOIN t2 ... GROUP BY t1.col;
```
Should show something like:
```
-> Table scan on t1  (Using pushed join aggregation)
```

---

## 11. Implementation Order

### Step 1: Create new files + feature gate
- Create `ha_ndbcluster_push_agg.h` (declarations)
- Create `ha_ndbcluster_push_agg.cc` (stub implementations)
- Add `CMakeLists.txt` entry (~1 line)
- Add `ndb_join_pushdown_aggregate` THDVAR to `ha_ndbcluster.cc` (~5 lines)

### Step 2: Detection (new file, read-only analysis)
- Implement `ndb_can_push_aggregation()` in `ha_ndbcluster_push_agg.cc`
- Add hook call in `ndbcluster_push_to_engine()` (~5 lines in `ha_ndbcluster.cc`)

### Step 3: NdbAggregator program build (new file)
- Implement `ndb_build_aggregation_program()` in `ha_ndbcluster_push_agg.cc`
- Add `m_has_aggregation` + `m_aggregator` to `ndb_pushed_join` (~3 lines in `.h`)
- Add `setAggregation()` call in `build_query()` (~5 lines in `ha_ndbcluster_push.cc`)

### Step 4: AccessPath surgery (new file + small hook)
- Implement `ndb_remove_aggregate_access_path()` in `ha_ndbcluster_push_agg.cc`
- Add AGGREGATE case in `fixup_pushed_access_paths()` (~5 lines in `ha_ndbcluster.cc`)

### Step 5: Item_sum pushed values (MySQL server, surgical)
- Add `m_pushed_aggregate` members + setters to `Item_sum` (~15 lines in `item_sum.h`)
- Add early-return in 4 `val_int()`/`val_real()` methods (~8 lines in `item_sum.cc`)

### Step 6: Runtime execution (new file + small hooks)
- Implement `ndb_fetch_next_aggregate()` in `ha_ndbcluster_push_agg.cc`
- Add `m_pushed_agg_mode` to `ha_ndbcluster` (~3 lines in `.h`)
- Add aggregate branch in `fetch_next_pushed()` (~5 lines in `ha_ndbcluster.cc`)
- Add aggregate setup in `create_pushed_join()` (~10 lines in `ha_ndbcluster.cc`)

### Step 7: Testing and verification
- MTR tests (see Section 10)
- Correctness verification: compare with `ndb_join_pushdown_aggregate=OFF`
- EXPLAIN output verification
