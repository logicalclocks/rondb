# Plan: RDRS Schema Cache for NDB Dictionary Lookups

Date: 2026-03-26

## Problem

Every RonSQL query calls `dict->getTable()`, `dict->listIndexes()`, and
`dict->getIndex()` fresh. The `load()` phase takes ~800µs, mostly spent in
these NDB dictionary API calls. While the NDB API has an internal dictionary
cache, the `listIndexes()` call is particularly expensive (~400µs per call)
and is called up to 2 times per child table (once in `findUniqueIndex()` and
once in `findOrderedIndex()`).

Other RDRS2 code paths (`rdrs_dal.cpp`, `ndb_api_helper.cpp`, `pk_batch_base_operation.cpp`)
also call `dict->getTable()` on every request, paying the same lookup cost.

## Goal

Cache NDB dictionary objects (tables, index lists, indexes) in RDRS2 so
repeated queries against the same tables skip the dictionary calls entirely.
Expected saving: ~1ms per query. Cache must handle schema changes gracefully.

## Design

### Cache location

A new `RdrsSchemaCache` class, global singleton, shared by all RDRS2 request
threads. Located in `storage/ndb/rest-server2/server/src/schema_cache.hpp/cpp`.

### What to cache

Per fully-qualified table (database + table name):

```cpp
struct CachedTable {
  const NdbDictionary::Table* table;     // from dict->getTable()
  Uint32 tableId;                        // table->getTableId()
  Uint32 schemaVersion;                  // table->getObjectVersion()

  // Index list (from dict->listIndexes())
  struct CachedIndex {
    std::string name;
    NdbDictionary::Object::Type type;    // OrderedIndex or UniqueHashIndex
    NdbDictionary::Object::Status state;
    const NdbDictionary::Index* index;   // from dict->getIndex()
    Uint32 indexId;
    Uint32 indexVersion;
  };
  std::vector<CachedIndex> indexes;
};
```

### Cache key

`std::string` = `"database/table_name"` (the database is set via `ndb->setCatalogName()`
before dictionary calls).

### Thread safety

RDRS2 handles requests on multiple drogon threads. The cache needs either:
- **A**: `std::shared_mutex` — readers share, writers exclusive
- **B**: Per-thread caches (no sharing)
- **C**: Lock-free concurrent hash map

Option A is simplest. Read path (cache hit) takes a shared lock; write path
(cache miss or invalidation) takes an exclusive lock. Since queries overwhelmingly
hit the cache, contention is minimal.

### Lookup flow

```
getTable(dict, db, table_name):
  key = db + "/" + table_name
  shared_lock:
    if cache[key] exists:
      // Validate: check that dict->getTable() returns same tableId/version
      // Actually, skip validation on the hot path — validate lazily on error
      return cache[key].table
  exclusive_lock:
    // Double-check after upgrading lock
    result = dict->getTable(table_name)
    populate cache[key] with result, indexes
    return result

getIndexList(db, table_name):
  key = db + "/" + table_name
  shared_lock:
    if cache[key] exists and indexes populated:
      return cache[key].indexes
  // Otherwise, populate via dict->listIndexes() + dict->getIndex()
```

### Schema change handling (invalidation)

When a query fails with a schema-related error (NDB error codes 241, 284,
710, 1227 — "Invalid schema version" family), the caller invalidates
cache entries for all tables involved in the query:

```cpp
void invalidate(const std::string& db, const std::string& table_name) {
  exclusive_lock:
    cache.erase(db + "/" + table_name);
  // Also call dict->invalidateTable() to clear NDB API internal cache
}
```

The RonSQLPreparer retry loop (in `ronsql_operation.cpp`) already retries
on `RonSQLRetryableError`. The invalidation hook goes there:

```cpp
catch (RonSQLRetryableError& e) {
  // Invalidate cache for tables used in this query
  for (auto& table : query_tables) {
    g_schema_cache->invalidate(db, table);
  }
  ndb_retry_sleep(50);
}
```

After invalidation, the retry will re-populate the cache with fresh metadata.

### Validation strategy

**Option 1 — Lazy validation (recommended):**
Trust the cache on the hot path. If a query fails with schema error, invalidate
and retry. This avoids any validation overhead on the fast path.

**Option 2 — Eager validation:**
On each lookup, compare cached `tableId`/`schemaVersion` against a lightweight
check. But `dict->getTable()` itself is the cheapest way to check, which
defeats the purpose.

Lazy validation is correct because:
- Schema changes are rare (DDL operations)
- The NDB API's own internal cache uses the same lazy approach
- The retry mechanism already exists in `ronsql_operation.cpp`

### Integration points

1. **RonSQLPreparer** (`load()`, `load_join()`):
   - Replace `dict->getTable()` with `g_schema_cache->getTable()`
   - Replace `dict->listIndexes()` + `dict->getIndex()` with `g_schema_cache->getIndexes()`

2. **QueryPlanner** (`plan()`):
   - Same replacements for `dict->getTable()`, `findUniqueIndex()`, `findOrderedIndex()`
   - Pass cache reference instead of raw `dict`

3. **rdrs_dal.cpp** (line 1835, 1966):
   - `dict->getTable()` → `g_schema_cache->getTable()`
   - `dict->getIndex()` → `g_schema_cache->getIndex()`

4. **ndb_api_helper.cpp** (`select_table()`, `get_index_scan_op()`):
   - Same replacements

5. **pk_batch_base_operation.cpp** (line 99):
   - `dict->getTable()` → `g_schema_cache->getTable()`

### Initialization and lifecycle

```cpp
// In RDRS2 startup (rdrs_rondb_connection.cpp or similar):
g_schema_cache = new RdrsSchemaCache();

// In RDRS2 shutdown:
delete g_schema_cache;
```

### Quick win: listIndexes deduplication

Before implementing the full cache, a simpler fix gives immediate benefit:
in `QueryPlanner::plan()`, call `listIndexes()` once per child table and
pass the result to both `findUniqueIndex()` and `findOrderedIndex()`.
This halves the dictionary cost for join queries with no caching complexity.

## Implementation steps

1. **Step 1 (quick win)**: Deduplicate `listIndexes()` in QueryPlanner — pass
   pre-fetched index list to `findUniqueIndex()`/`findOrderedIndex()`.
   Estimated saving: ~400µs per child table.

2. **Step 2**: Create `RdrsSchemaCache` class with `getTable()`, `getIndexes()`,
   `invalidate()`. Use `std::shared_mutex` for thread safety.

3. **Step 3**: Integrate into RonSQLPreparer and QueryPlanner.

4. **Step 4**: Add invalidation in the RonSQLRetryableError catch block.

5. **Step 5**: Integrate into other RDRS2 code paths (rdrs_dal, ndb_api_helper,
   pk_batch_base_operation).

6. **Step 6**: Test with schema changes (ALTER TABLE, CREATE/DROP INDEX) to
   verify invalidation and retry work correctly.

## Expected impact

- RonSQL query preparation: ~800µs → ~100µs (cache hit)
- PK read/write operations: ~200µs saved per table lookup
- Schema change handling: transparent via retry (existing mechanism)
