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

The primary cost is `dict->listIndexes()` (~400µs per call). The NDB API
already caches Table and Index objects internally — `dict->getTable()` and
`dict->getIndex()` are fast on cache hit. The missing piece is knowing
*which indexes exist* for a given table without calling `listIndexes()`.

The cache therefore stores only **names and version identifiers**, not
NdbDictionary objects. This keeps it lightweight and avoids lifetime
management issues with NDB API objects.

Per fully-qualified table (database + table name):

```cpp
struct CachedTable {
  std::string database;
  std::string table_name;
  Uint32 tableId;                        // table->getTableId()
  Uint32 schemaVersion;                  // table->getObjectVersion()

  // Index name list (from dict->listIndexes())
  struct CachedIndex {
    std::string name;
    NdbDictionary::Object::Type type;    // OrderedIndex or UniqueHashIndex
    NdbDictionary::Object::Status state;
    Uint32 indexId;
    Uint32 indexVersion;
  };
  std::vector<CachedIndex> indexes;
};
```

Callers use the cached index names to call `dict->getIndex(name, table)`
which hits the NDB API's internal cache and is fast. The cached tableId
and schemaVersion allow quick validation that the cache is still current
by comparing against the Table object returned by `dict->getTable()`.

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
getIndexNames(dict, db, table_name) -> vector<CachedIndex>&:
  key = db + "/" + table_name
  table = dict->getTable(table_name)    // fast: NDB API internal cache
  shared_lock:
    if cache[key] exists:
      if cache[key].tableId == table->getTableId() &&
         cache[key].schemaVersion == table->getObjectVersion():
        return cache[key].indexes       // cache hit — skip listIndexes()
  exclusive_lock:
    // Cache miss or stale — refresh
    dict->listIndexes(index_list, *table)
    populate cache[key] with table metadata + index names/versions
    return cache[key].indexes
```

Callers then use the returned index names with `dict->getIndex(name, table)`
which is fast (NDB API internal cache). The expensive `listIndexes()` is
only called on first access or after schema changes.

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

The cache replaces `dict->listIndexes()` calls only. `dict->getTable()` and
`dict->getIndex()` are kept as-is (they use the NDB API's internal cache).

1. **QueryPlanner** (`findUniqueIndex()`, `findOrderedIndex()`):
   - Replace `dict->listIndexes()` with `g_schema_cache->getIndexNames()`
   - Keep `dict->getIndex(name, table)` for the actual Index object

2. **RonSQLPreparer** (`load_single_table()` line 725):
   - Replace `dict->listIndexes()` with `g_schema_cache->getIndexNames()`

3. **Any other RDRS2 code** that calls `dict->listIndexes()`:
   - Same replacement pattern

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

- RonSQL query preparation: ~800µs → ~200µs (skip listIndexes, keep getTable/getIndex)
- Join queries: additional ~400µs saved per child table (was calling listIndexes twice)
- Schema change handling: transparent via lazy invalidation + retry (existing mechanism)
- Minimal memory overhead: only index names and version IDs, no NDB API object copies
