# MySQL Handler Implementation Plan (RONDB-733)

This document is the step-by-step implementation plan for integrating pushdown
join aggregation into the MySQL server handler layer. Each phase is designed
to be independently testable and committable.

See `mysql_join_agg.md` for the architecture analysis.

## Completion Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Feature gate + infrastructure | COMPLETE |
| 2 | Aggregation candidate detection | COMPLETE |
| 3 | NdbAggregator program build | COMPLETE |
| 4 | AccessPath surgery + Item_sum pushed values | COMPLETE |
| 5 | Runtime result flow | COMPLETE |
| 6 | HAVING, ORDER BY, LIMIT | COMPLETE |
| 7 | Implicit aggregation (no GROUP BY) | COMPLETE |
| 8 | Wider type support (all NDB types) | COMPLETE |
| 9 | Multi-table GROUP BY (linked projections) | COMPLETE |
| 10 | 3+ way joins | COMPLETE |
| 11 | EXPLAIN support | TODO |
| 12 | SQL-level MTR test suite | TODO |

## Design Principles

1. **New code in new files**: All substantial logic goes into
   `ha_ndbcluster_push_agg.h` and `ha_ndbcluster_push_agg.cc`.
2. **Surgical changes**: Existing MySQL server and NDB plugin files get only
   small hooks — a function call, a member declaration, an early-return guard.
3. **Incremental phases**: Each phase adds one verifiable capability. Earlier
   phases work without later phases. The feature gate (`ndb_join_pushdown_aggregate`,
   default OFF) protects existing behavior throughout.
4. **Simplest query first**: Start with the simplest pushable query (2-table
   join, single aggregate, single GROUP BY column on root table, integer types
   only), then widen support in later phases.

---

## Phase 1: Feature Gate and Infrastructure

### Goal
Add the `ndb_join_pushdown_aggregate` session variable and create the new
source files with stub implementations. After this phase, `SET
ndb_join_pushdown_aggregate=ON` is accepted but has no effect.

### New Files

**`storage/ndb/plugin/ha_ndbcluster_push_agg.h`** (~30 lines):
```cpp
#ifndef HA_NDBCLUSTER_PUSH_AGG_H
#define HA_NDBCLUSTER_PUSH_AGG_H

#include "sql/handler.h"

class THD;
class JOIN;
class AccessPath;
class ha_ndbcluster;
class ndb_pushed_builder_ctx;
class ndb_pushed_join;

/**
 * Check if aggregation can be pushed for a fully-pushed join.
 * Called from ndbcluster_push_to_engine() after make_pushed_join() succeeds.
 */
bool ndb_can_push_aggregation(THD *thd, const JOIN *join,
                              const ndb_pushed_builder_ctx &builder,
                              const ndb_pushed_join *pushed_join);

#endif
```

**`storage/ndb/plugin/ha_ndbcluster_push_agg.cc`** (~20 lines stub):
```cpp
#include "storage/ndb/plugin/ha_ndbcluster_push_agg.h"

bool ndb_can_push_aggregation(THD *thd, const JOIN *join,
                              const ndb_pushed_builder_ctx &builder,
                              const ndb_pushed_join *pushed_join) {
  // Stub — always returns false until Phase 2 implements detection.
  return false;
}
```

### Surgical Changes

**`storage/ndb/plugin/ha_ndbcluster.cc`** (~10 lines):

1. Add THDVAR after `join_pushdown` (line ~364):
```cpp
static MYSQL_THDVAR_BOOL(join_pushdown_aggregate,
    PLUGIN_VAR_OPCMDARG,
    "Enable pushing down of aggregation for pushed joins to datanodes",
    nullptr, nullptr, false  // default OFF
);
```

2. Register in `system_variables[]` array (line ~19210), after `MYSQL_SYSVAR(join_pushdown)`:
```cpp
MYSQL_SYSVAR(join_pushdown_aggregate),
```

3. Add `#include` and hook call in `ndbcluster_push_to_engine()` (line ~14854),
   after the `make_pushed_join()` block:
```cpp
#include "storage/ndb/plugin/ha_ndbcluster_push_agg.h"
// ... inside ndbcluster_push_to_engine(), after make_pushed_join():
if (ndb_can_push_aggregation(thd, join, pushed_builder, /* pushed_join */)) {
    // Phase 2+ will act here
}
```

**`storage/ndb/plugin/CMakeLists.txt`** (~1 line):
Add `plugin/ha_ndbcluster_push_agg.cc` to the `NDBCLUSTER_SOURCES` list.

### Verification
- Build succeeds (`make -j$(sysctl -n hw.ncpu) ndbcluster`)
- `SET ndb_join_pushdown_aggregate=ON;` accepted in mysql client
- `SHOW VARIABLES LIKE 'ndb_join_pushdown_aggregate';` shows OFF by default
- Existing MTR tests pass unchanged (the stub always returns false)

---

## Phase 2: Aggregation Candidate Detection

### Goal
Implement `ndb_can_push_aggregation()` to detect when a fully-pushed join
query has aggregation that can be pushed. Start with the simplest case:
2-table join, COUNT(*)/SUM/MIN/MAX on integer columns, GROUP BY on root
table column(s), no DISTINCT, no ROLLUP, no HAVING.

Log detection results so we can verify which queries are detected as
pushable without actually pushing anything yet.

### Changes to `ha_ndbcluster_push_agg.cc`

Implement `ndb_can_push_aggregation()`:

```cpp
bool ndb_can_push_aggregation(THD *thd, const JOIN *join,
                              const ndb_pushed_builder_ctx &builder,
                              const ndb_pushed_join *pushed_join) {
  // 1. Feature gate
  if (!THDVAR(thd, join_pushdown_aggregate)) return false;

  // 2. Must have a pushed join
  if (pushed_join == nullptr) return false;

  // 3. Entire join must be pushed (all tables in scope)
  //    Compare pushed_join->get_operation_count() with builder table count.
  //    If not all tables pushed, MySQL still needs raw rows for joining.
  if (pushed_join->get_operation_count() != builder.m_table_count)
    return false;

  // 4. Query must have aggregation
  if (!join->grouped && join->sum_funcs == nullptr) return false;
  if (join->sum_funcs[0] == nullptr) return false;  // No aggregate functions

  // 5. No ROLLUP
  if (join->query_block->olap == ROLLUP_TYPE) return false;

  // 6. All aggregate functions must be pushable
  for (Item_sum **func = join->sum_funcs; *func != nullptr; func++) {
    switch ((*func)->sum_func()) {
      case Item_sum::COUNT_FUNC:
      case Item_sum::SUM_FUNC:
      case Item_sum::MIN_FUNC:
      case Item_sum::MAX_FUNC:
        break;  // Supported
      default:
        return false;  // Unsupported (DISTINCT, AVG, GROUP_CONCAT, etc.)
    }
    // Check for DISTINCT variants
    if ((*func)->has_with_distinct()) return false;
  }

  // 7. GROUP BY columns must all come from pushed tables
  //    (Detailed column-to-table mapping — Phase 2 scope)
  for (ORDER *group = join->group_list.order; group; group = group->next) {
    Item *item = *(group->item);
    // Must be a simple column reference, not an expression
    if (item->type() != Item::FIELD_ITEM) return false;
    // The column's table must be in the pushed join
    // (table membership check via handler->member_of_pushed_join())
  }

  return true;
}
```

### Surgical Changes

**`ha_ndbcluster_push.h`** (~2 lines):
Make `m_table_count` accessible (add a public accessor if not already):
```cpp
uint get_table_count() const { return m_table_count; }
```

### Verification
- `SET ndb_join_pushdown_aggregate=ON;`
- Run an aggregate pushed-join query and observe detection via debug logging
- With `ndb_join_pushdown_aggregate=OFF`: no detection occurs
- Existing MTR tests pass unchanged (detection returns true but nothing acts on it yet)

### Test queries for detection verification
```sql
-- Should detect as pushable:
SELECT t1.pk, COUNT(*) FROM t1 JOIN t2 ON t2.fk=t1.pk GROUP BY t1.pk;
SELECT t1.pk, SUM(t2.val) FROM t1 JOIN t2 ON t2.fk=t1.pk GROUP BY t1.pk;

-- Should NOT detect (AVG not pushable):
SELECT t1.pk, AVG(t2.val) FROM t1 JOIN t2 ON t2.fk=t1.pk GROUP BY t1.pk;

-- Should NOT detect (DISTINCT):
SELECT t1.pk, COUNT(DISTINCT t2.val) FROM t1 JOIN t2 ON t2.fk=t1.pk GROUP BY t1.pk;

-- Should NOT detect (ROLLUP):
SELECT t1.pk, SUM(t2.val) FROM t1 JOIN t2 ON t2.fk=t1.pk GROUP BY t1.pk WITH ROLLUP;
```

---

## Phase 3: NdbAggregator Program Build

### Goal
When detection succeeds, build the NdbAggregator program from the MySQL
query plan (Item_sum list + GROUP BY list). Attach it to the `ndb_pushed_join`.
The program is built but not yet executed — no AccessPath changes, no runtime
changes. This phase validates that the translation from MySQL's aggregate
representation to NdbAggregator instructions is correct.

### Changes to `ha_ndbcluster_push_agg.h`

Add new function declaration:
```cpp
/**
 * Build NdbAggregator program from MySQL query plan.
 * Called after ndb_can_push_aggregation() returns true.
 * Attaches aggregator to the ndb_pushed_join.
 * Returns 0 on success, error code on failure.
 */
int ndb_build_aggregation_program(THD *thd, const JOIN *join,
                                  ndb_pushed_join *pushed_join,
                                  NdbQueryBuilder *builder);
```

### Changes to `ha_ndbcluster_push_agg.cc` (~150 lines)

Implement `ndb_build_aggregation_program()`:

1. **Create NdbAggregator** from the leaf operation's NDB table.

2. **Map GROUP BY columns**:
   - For each `ORDER` in `join->group_list.order`:
     - Get the `Item_field` and its `Field` object
     - Determine which pushed table it belongs to
     - If root table: `agg.GroupBy(ndb_col_id)`
     - If parent table: `agg.GroupByLinked(position, parentCol)` using
       linked projection to pass the column from parent to leaf

3. **Map aggregate functions**:
   - For each `Item_sum` in `join->sum_funcs`:
     - `COUNT_FUNC` with COUNT(*): `agg.LoadUint64(1, reg); agg.Count(agg_id, reg)`
     - `COUNT_FUNC` with column: load column, `agg.Count(agg_id, reg)`
     - `SUM_FUNC`: `agg.LoadColumn(col_id, reg); agg.Sum(agg_id, reg)`
     - `MIN_FUNC`: `agg.LoadColumn(col_id, reg); agg.Min(agg_id, reg)`
     - `MAX_FUNC`: `agg.LoadColumn(col_id, reg); agg.Max(agg_id, reg)`
   - Extract NDB column ID from `Item_field::field->field_index()`
   - Map to the correct pushed table's column

4. **Finalize**: `agg.Finalize()`

5. **Store on ndb_pushed_join**: The aggregator needs to persist for
   `build_query()` to call `setAggregation()`.

### Helper: MySQL Field to NDB Column Mapping

```cpp
/**
 * Given a MySQL Item_field from a pushed table, return the NDB column ID
 * and which pushed operation (table index) it belongs to.
 */
static bool resolve_ndb_column(const Item_field *item,
                               const ndb_pushed_join *pushed_join,
                               uint &table_index,
                               int &ndb_col_id);
```

Uses `item->field->table` to find which of `pushed_join->m_tables[]` it
belongs to, then `item->field->field_index()` for the NDB column ID.

### Surgical Changes

**`ha_ndbcluster_push.h`** (~5 lines):
Add to `ndb_pushed_join`:
```cpp
private:
  bool m_has_aggregation{false};
  NdbAggregator *m_aggregator{nullptr};
public:
  bool has_aggregation() const { return m_has_aggregation; }
  NdbAggregator *get_aggregator() const { return m_aggregator; }
  void set_aggregator(NdbAggregator *agg) {
      m_aggregator = agg; m_has_aggregation = true;
  }
```

**`ha_ndbcluster_push.cc`** (~5 lines):
In `build_query()`, after NdbQueryOptions setup for the leaf operation,
add aggregation:
```cpp
if (m_has_aggregation && m_aggregator != nullptr) {
    // The leaf operation gets the aggregation program
    options.setAggregation(*m_aggregator);
}
```

In the `ndb_pushed_join` destructor, delete the aggregator:
```cpp
ndb_pushed_join::~ndb_pushed_join() {
  if (m_query_def) m_query_def->destroy();
  delete m_aggregator;  // NEW
}
```

**`ha_ndbcluster.cc`** (~5 lines):
In `ndbcluster_push_to_engine()`, after detection succeeds:
```cpp
if (ndb_can_push_aggregation(thd, join, pushed_builder, pushed_join)) {
    ndb_build_aggregation_program(thd, join, pushed_join, builder);
    // If build fails, pushed_join->has_aggregation() remains false
    // and we fall back to MySQL aggregation (no harm done)
}
```

### Scope Limitation for Phase 3

Start with the simplest cases:
- GROUP BY columns only on the **root** table (no linked projections yet)
- Aggregated columns only on the **leaf** table
- Integer types only (INT, BIGINT) — no FLOAT, DOUBLE, DECIMAL yet
- COUNT(*) only (not COUNT(column)) for the count case

### Verification
- Build succeeds
- Debug logging shows the NdbAggregator program being constructed
- The NdbAggregator program buffer content can be inspected via debug output
- Existing MTR tests pass unchanged (program is built but never executed)
- The `ndb_pushed_join` destructor correctly cleans up the aggregator

---

## Phase 4: AccessPath Surgery + Item_sum Pushed Values

### Goal
When aggregation is pushed, modify the AccessPath tree to remove the
AGGREGATE node. Add the `m_pushed_aggregate` mechanism to `Item_sum` so
aggregate functions can return pre-computed values. After this phase, the
execution path is set up but no actual aggregation results flow through yet
(that comes in Phase 5).

### Changes to `ha_ndbcluster_push_agg.h`

Add declarations:
```cpp
/**
 * Remove AGGREGATE (or TEMPTABLE_AGGREGATE) AccessPath node from the tree
 * when aggregation has been pushed. Replaces the AGGREGATE node with its
 * child, so the root handler returns pre-aggregated rows directly.
 */
bool ndb_remove_aggregate_access_path(AccessPath *path, const JOIN *join);
```

### Changes to `ha_ndbcluster_push_agg.cc` (~80 lines)

Implement `ndb_remove_aggregate_access_path()`:

Walk the AccessPath tree to find the AGGREGATE or TEMPTABLE_AGGREGATE node.
When found:
- For AGGREGATE: replace `*path` with `*path->aggregate().child`
- For TEMPTABLE_AGGREGATE: replace with the subquery path
- Also remove any SORT node directly below (NDB returns groups in hash
  order; if ORDER BY is needed, MySQL adds a separate SORT above)

The walk uses `WalkAccessPaths()` from MySQL's access path utilities with
a lambda that tracks the parent pointer.

### Surgical Changes to MySQL Server (~25 lines total)

**`sql/item_sum.h`** (~15 lines):
Add to `Item_sum` base class (after existing member declarations):
```cpp
 protected:
  // Pushed aggregate support (NDB pushdown).
  // When m_pushed_aggregate is true, val_int()/val_real() return the
  // pre-computed values instead of computing from accumulated state.
  bool m_pushed_aggregate{false};
  int64_t m_pushed_value_int{0};
  double m_pushed_value_double{0.0};
  bool m_pushed_null{false};

 public:
  void set_pushed_value_int(int64_t val) {
      m_pushed_aggregate = true; m_pushed_value_int = val;
      m_pushed_null = false; null_value = false;
  }
  void set_pushed_value_double(double val) {
      m_pushed_aggregate = true; m_pushed_value_double = val;
      m_pushed_null = false; null_value = false;
  }
  void set_pushed_null() {
      m_pushed_aggregate = true; m_pushed_null = true; null_value = true;
  }
  bool is_pushed_aggregate() const { return m_pushed_aggregate; }
```

**`sql/item_sum.cc`** (~8 lines):
Add early-return guard to four methods:

```cpp
// Item_sum_count::val_int() — at line 2215, after DBUG_TRACE and assert:
  if (m_pushed_aggregate) { null_value = m_pushed_null; return m_pushed_value_int; }

// Item_sum_sum::val_real() — at line 2018, after DBUG_TRACE and assert:
  if (m_pushed_aggregate) { null_value = m_pushed_null; return m_pushed_value_double; }

// Item_sum_hybrid::val_int() — at line 2981, after assert:
  if (m_pushed_aggregate) { null_value = m_pushed_null; return m_pushed_value_int; }

// Item_sum_hybrid::val_real() — at line 2967, after assert:
  if (m_pushed_aggregate) { null_value = m_pushed_null; return m_pushed_value_double; }
```

Also add to `Item_sum_sum::val_int()` (line 1994):
```cpp
  if (m_pushed_aggregate) { null_value = m_pushed_null; return m_pushed_value_int; }
```

### Surgical Changes to NDB Plugin

**`ha_ndbcluster.cc`** (~10 lines):
In `fixup_pushed_access_paths()`, change the existing `#ifndef NDEBUG`
AGGREGATE case to unconditional and add the removal call:

```cpp
case AccessPath::AGGREGATE:
case AccessPath::TEMPTABLE_AGGREGATE: {
    // If aggregation is pushed, remove the AGGREGATE node
    if (has_pushed_aggregation_in_subtree(subpath, join)) {
        ndb_remove_aggregate_access_path(subpath, join);
    }
    #ifndef NDEBUG
    else {
        // Original assertion: no pushed members outside branch
        assert(!has_pushed_members_outside_of_branch(...));
    }
    #endif
    break;
}
```

**`ha_ndbcluster.h`** (~2 lines):
Add to `ha_ndbcluster`:
```cpp
  bool m_pushed_agg_mode{false};  // True when returning aggregate results
```

### Verification
- Build succeeds (including MySQL server rebuild for item_sum changes)
- `EXPLAIN` for a pushable aggregate query shows the AGGREGATE node removed
- Existing MTR tests pass (feature is OFF by default)
- Manual verification: with feature ON, the AGGREGATE AccessPath is removed
  and the query attempts to run through the root handler (will produce wrong
  results or crash at this point — that's expected, Phase 5 connects the data)

---

## Phase 5: Runtime Execution — Result Flow

### Goal
Connect the full data path: the root handler executes the NdbQuery with
aggregation, receives results from the NdbAggregator, populates MySQL's
row buffer with GROUP BY column values, and sets Item_sum pushed values.
After this phase, basic aggregate pushed-join queries produce correct results.

### Changes to `ha_ndbcluster_push_agg.h`

Add declarations:
```cpp
/**
 * Fetch the next aggregate result row from the NdbAggregator.
 * Populates the MySQL row buffer with GROUP BY columns and sets
 * Item_sum pushed values for aggregate results.
 * Returns 0 for row found, HA_ERR_END_OF_FILE when done.
 */
int ndb_fetch_next_aggregate(ha_ndbcluster *handler, uchar *buf);

/**
 * Initialize aggregate result iteration after the NdbQuery scan completes.
 * Calls ProcessRes() on accumulated TRANSID_AI data and PrepareResults().
 */
int ndb_init_aggregate_results(ha_ndbcluster *handler);
```

### Changes to `ha_ndbcluster_push_agg.cc` (~200 lines)

**`ndb_init_aggregate_results()`**:
- Get the NdbAggregator from `handler->m_pushed_join_member->get_aggregator()`
- Call `getAggregator()` on the NdbQuery to get the result aggregator
- Call `PrepareResults()` to finalize group-by hash map
- Store first `FetchResultRecord()` on the handler's aggregate state

**`ndb_fetch_next_aggregate()`**:
- Check if current `ResultRecord` is at end → return `HA_ERR_END_OF_FILE`
- For each GROUP BY column:
  - Call `record.FetchGroupbyColumn()`
  - Write value to the corresponding MySQL `Field` in `table->record[0]`
  - Handle NULL: `field->set_null()` / `field->set_notnull()`
  - Handle types: integer → `field->store(val, unsigned)`,
    double → `field->store(val)`
- For each aggregate function in `join->sum_funcs`:
  - Call `record.FetchAggregationResult()`
  - Based on result type:
    - Int64: `item->set_pushed_value_int(res.data_int64())`
    - Uint64: `item->set_pushed_value_int((int64_t)res.data_uint64())`
    - Double: `item->set_pushed_value_double(res.data_double())`
    - NULL: `item->set_pushed_null()`
- Advance: `record = aggregator->FetchResultRecord()`
- Return 0

### Aggregate Result State on ha_ndbcluster

**`ha_ndbcluster.h`** (~5 lines):
```cpp
  // Aggregate result iteration state
  bool m_pushed_agg_mode{false};
  bool m_agg_results_initialized{false};
  const JOIN *m_pushed_agg_join{nullptr};  // For accessing sum_funcs
```

### Surgical Changes

**`ha_ndbcluster.cc`** — `create_pushed_join()` (~10 lines):
After NdbQuery creation, detect aggregate mode:
```cpp
if (m_pushed_join_member->has_aggregation()) {
    m_pushed_agg_mode = true;
    m_agg_results_initialized = false;
    m_pushed_agg_join = /* the JOIN pointer, needs to be stored */;
}
```

**`ha_ndbcluster.cc`** — `fetch_next_pushed()` (~10 lines):
Add aggregate-mode branch at the top (before existing code):
```cpp
int ha_ndbcluster::fetch_next_pushed() {
  DBUG_TRACE;

  if (m_pushed_agg_mode) {
    if (!m_agg_results_initialized) {
      const int init_err = ndb_init_aggregate_results(this);
      if (init_err) return init_err;
      m_agg_results_initialized = true;
    }
    return ndb_fetch_next_aggregate(this, table->record[0]);
  }

  // ... existing code unchanged ...
```

**`ha_ndbcluster.cc`** — reset aggregate state:
In the scan-end / cleanup path, reset `m_pushed_agg_mode = false` and
`m_agg_results_initialized = false`.

### Scope for Phase 5

Supported query shape:
```sql
SELECT t1.col, COUNT(*), SUM(t2.int_col), MIN(t2.int_col), MAX(t2.int_col)
FROM t1 JOIN t2 ON t2.fk = t1.pk
GROUP BY t1.col;
```

Where:
- GROUP BY on root table columns (integer types)
- Aggregated columns on leaf table (integer types)
- 2-table join
- No HAVING, no ORDER BY, no LIMIT (those come through MySQL normally)

### Verification
- The simplest aggregate pushed-join query produces correct results
- Compare results with `ndb_join_pushdown_aggregate=OFF` (MySQL aggregation)
- Empty result set: `WHERE 1=0` returns zero rows
- Single group result: one-row parent table
- Multiple groups: several distinct GROUP BY values
- NULL handling: NULL values in aggregated columns

---

## Phase 6: HAVING, ORDER BY, LIMIT

### Goal
Verify and fix interaction with MySQL's post-aggregation processing: HAVING
filters, ORDER BY on aggregate results, and LIMIT. These are handled by
MySQL's AccessPath nodes above the (now-removed) AGGREGATE node.

### What Should Work Already

After Phase 5, the AccessPath tree looks like:
```
LIMIT_OFFSET
  └── SORT (if ORDER BY)
        └── FILTER (if HAVING)
              └── TABLE_SCAN(t1) [aggregate mode]
```

- **HAVING**: The FILTER node calls `Item_sum::val_int()` etc., which return
  pushed values. This should work automatically.
- **ORDER BY**: MySQL adds a SORT above the result. This should work if the
  output rows are correctly formatted.
- **LIMIT**: The LIMIT_OFFSET node stops reading after N rows. This should
  work automatically.

### What Might Need Fixing

- **HAVING with aggregate references**: Verify that HAVING expressions
  correctly evaluate pushed aggregate values.
- **ORDER BY on GROUP BY column**: Verify that the SORT node correctly reads
  the GROUP BY column from the row buffer.
- **ORDER BY on aggregate result**: Verify that SORT reads pushed Item_sum
  values correctly.

### Verification
```sql
-- HAVING
SELECT t1.pk, SUM(t2.val) AS total
FROM t1 JOIN t2 ON t2.fk=t1.pk
GROUP BY t1.pk HAVING total > 100;

-- ORDER BY group column
SELECT t1.pk, SUM(t2.val)
FROM t1 JOIN t2 ON t2.fk=t1.pk
GROUP BY t1.pk ORDER BY t1.pk;

-- ORDER BY aggregate
SELECT t1.pk, SUM(t2.val) AS total
FROM t1 JOIN t2 ON t2.fk=t1.pk
GROUP BY t1.pk ORDER BY total DESC;

-- LIMIT
SELECT t1.pk, SUM(t2.val)
FROM t1 JOIN t2 ON t2.fk=t1.pk
GROUP BY t1.pk LIMIT 5;

-- Combined
SELECT t1.pk, SUM(t2.val) AS total
FROM t1 JOIN t2 ON t2.fk=t1.pk
GROUP BY t1.pk HAVING total > 50 ORDER BY total DESC LIMIT 3;
```

Compare all results with `ndb_join_pushdown_aggregate=OFF`.

---

## Phase 7: Implicit Aggregation (No GROUP BY)

### Goal
Support queries with aggregate functions but no GROUP BY clause. MySQL
expects exactly one row with aggregate results (COUNT=0, SUM=NULL for empty
result, or computed values for non-empty).

### Changes

In `ndb_can_push_aggregation()`:
- Allow queries where `join->grouped == false` but `join->sum_funcs[0] != nullptr`
  (implicit aggregation — no GROUP BY, but has aggregate functions)

In `ndb_build_aggregation_program()`:
- When there is no GROUP BY, don't call `agg.GroupBy()` — the NdbAggregator
  produces a single result group

In `ndb_fetch_next_aggregate()`:
- Handle the single-row result case

### Verification
```sql
-- Implicit aggregation
SELECT COUNT(*), SUM(t2.val), MIN(t2.val), MAX(t2.val)
FROM t1 JOIN t2 ON t2.fk=t1.pk;

-- Empty result with implicit aggregation
SELECT COUNT(*), SUM(t2.val)
FROM t1 JOIN t2 ON t2.fk=t1.pk WHERE 1=0;
-- Should return: COUNT=0, SUM=NULL
```

---

## Phase 8: Wider Type Support

### Goal
Extend aggregation pushdown to support more column types beyond integers.

### 8a: FLOAT and DOUBLE columns

- SUM, MIN, MAX on FLOAT/DOUBLE columns
- GROUP BY on FLOAT/DOUBLE columns (less common but valid)
- Double results use `set_pushed_value_double()`

### 8b: CHAR/VARCHAR columns

- GROUP BY on CHAR/VARCHAR columns (common pattern)
- MIN/MAX on CHAR/VARCHAR columns
- Requires proper character set handling in `FetchGroupbyColumn()`
- `field->store(ptr, length, charset)` for writing to row buffer

### 8c: DATE/DATETIME/TIMESTAMP columns

- GROUP BY on date columns
- MIN/MAX on date columns
- Map to NDB's date representation

### Changes

In `ndb_can_push_aggregation()`:
- Relax the type checks incrementally (initially reject, then allow each type)

In `ndb_fetch_next_aggregate()`:
- Handle each NDB column type → MySQL Field type conversion

### Verification
- Test each type combination with correctness comparison

---

## Phase 9: Multi-Table GROUP BY (Linked Projections)

### Goal
Support GROUP BY columns from non-root tables in the pushed join. This
requires linked projections to pass the GROUP BY column values from parent
to leaf via SPJ linked attributes.

### Example
```sql
SELECT t1.region, t2.category, SUM(t2.amount)
FROM t1 JOIN t2 ON t2.fk = t1.pk
GROUP BY t1.region, t2.category;
```

Here `t1.region` is from the root table and `t2.category` is from the leaf.
The root column value must be available at the leaf for grouping.

### Changes

In `ndb_build_aggregation_program()`:
- For GROUP BY columns from parent tables:
  - Call `builder->linkedValue(parent_op, col_name)` to create a linked operand
  - Call `options.addLinkedProjection(linked)` on the leaf operation
  - Use `agg.GroupByLinked(position, parentCol)` in the program

In `ndb_can_push_aggregation()`:
- Remove the root-table-only restriction on GROUP BY columns

### Verification
```sql
SELECT t1.pk, t2.cat, SUM(t2.val)
FROM t1 JOIN t2 ON t2.fk=t1.pk
GROUP BY t1.pk, t2.cat;
```

---

## Phase 10: 3+ Way Joins

### Goal
Support aggregation on pushed joins with 3 or more tables.

### Example
```sql
SELECT t1.region, SUM(t3.amount)
FROM t1 JOIN t2 ON t2.fk=t1.pk
        JOIN t3 ON t3.fk=t2.pk
GROUP BY t1.region;
```

### Changes

In `ndb_build_aggregation_program()`:
- Identify the leaf operation (the deepest table in the pushed join tree)
- GROUP BY columns from any ancestor use linked projections through
  the chain: grandparent → parent → leaf
- Aggregated columns must be on the leaf table

In `ndb_can_push_aggregation()`:
- Remove the 2-table restriction

### Verification
- 3-way join with GROUP BY on root, aggregation on leaf
- Compare with `ndb_join_pushdown_aggregate=OFF`

---

## Phase 11: EXPLAIN Support

### Goal
Make EXPLAIN output clearly show when aggregation has been pushed.

### Changes

In `fixup_pushed_access_paths()` or the handler's `info()` method:
- Add a note like "Using pushed join aggregation" to the EXPLAIN extra column
- Similar to how "Using pushed join" is shown for pushed joins

### Verification
```sql
EXPLAIN SELECT t1.pk, SUM(t2.val)
FROM t1 JOIN t2 ON t2.fk=t1.pk
GROUP BY t1.pk;
```
Should show pushed aggregation in the Extra column.

---

## Phase 12: MTR Test Suite

### Goal
Comprehensive MTR test coverage for aggregation pushdown.

### Test file
`mysql-test/suite/ndb/t/ndb_join_pushdown_agg.test`

### Test categories

1. **Basic correctness**: Each aggregate function (COUNT, SUM, MIN, MAX)
   with ON/OFF comparison
2. **GROUP BY**: Single column, multi-column, expressions
3. **Types**: INT, BIGINT, FLOAT, DOUBLE, CHAR, VARCHAR, DATE
4. **Edge cases**: Empty tables, single row, NULL values, all-NULL groups
5. **Post-aggregation**: HAVING, ORDER BY, LIMIT, combined
6. **Implicit aggregation**: No GROUP BY
7. **Multi-way joins**: 3-table, 4-table
8. **Fallback**: Queries that can't be pushed (partial join, unsupported
   function, ROLLUP) — verify they still produce correct results via MySQL
9. **EXPLAIN**: Verify EXPLAIN output shows pushed aggregation

---

## Summary: File Changes Per Phase

| Phase | New/Modified Files | Surgical Lines |
|-------|-------------------|---------------|
| 1 | NEW: `push_agg.h`, `push_agg.cc`. MOD: `ha_ndbcluster.cc`, `CMakeLists.txt` | ~15 |
| 2 | MOD: `push_agg.cc`, `push.h` | ~2 |
| 3 | MOD: `push_agg.h`, `push_agg.cc`, `push.h`, `push.cc`, `ha_ndbcluster.cc` | ~15 |
| 4 | MOD: `push_agg.h`, `push_agg.cc`, `ha_ndbcluster.cc`, `ha_ndbcluster.h`, `item_sum.h`, `item_sum.cc` | ~25 |
| 5 | MOD: `push_agg.h`, `push_agg.cc`, `ha_ndbcluster.cc`, `ha_ndbcluster.h` | ~25 |
| 6 | MOD: `push_agg.cc` (mostly verification, minimal code) | ~5 |
| 7 | MOD: `push_agg.cc` | ~5 |
| 8 | MOD: `push_agg.cc` | ~30 |
| 9 | MOD: `push_agg.cc`, `push.cc` | ~20 |
| 10 | MOD: `push_agg.cc` | ~10 |
| 11 | MOD: `push_agg.cc` or `ha_ndbcluster.cc` | ~10 |
| 12 | NEW: MTR test files | 0 (test only) |

All `push_agg.h`/`push_agg.cc` references are the new files
`ha_ndbcluster_push_agg.h`/`ha_ndbcluster_push_agg.cc`.
