/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

#ifndef STORAGE_NDB_SRC_RONSQL_RDRSSCHEMACACHE_HPP
#define STORAGE_NDB_SRC_RONSQL_RDRSSCHEMACACHE_HPP

#include <NdbApi.hpp>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <shared_mutex>

/**
 * Cache for NDB dictionary index lists.
 *
 * The NDB API already caches Table and Index objects internally, so
 * dict->getTable() and dict->getIndex() are fast on repeat calls.
 * However, dict->listIndexes() is expensive (~400µs) and must be called
 * to discover which indexes exist for a table.
 *
 * This cache stores index names and version identifiers per table so
 * that listIndexes() is only called on first access, after schema
 * changes, or after the configurable TTL expires.  Callers use the
 * cached names with dict->getIndex() to get the actual Index objects
 * (fast, NDB API internal cache).
 *
 * The TTL exists because index CREATION is invisible to version checks:
 * CREATE INDEX makes a separate dictionary object and does not bump the
 * base table's schema version, and RDRS does not participate in schema
 * distribution — so without an expiry a warm RDRS would never discover
 * a new index.  With the TTL, new indexes become usable within
 * ttl_seconds.  ttl_seconds == 0 disables expiry (refresh only on
 * version change or schema-error invalidation).
 *
 * Thread-safe via std::shared_mutex (readers share, writers exclusive).
 * The index list is returned as shared_ptr-to-const so a concurrent
 * refresh (TTL expiry, version change) or invalidate() can never
 * destroy a vector a reader is still walking.
 */
class RdrsSchemaCache {
 public:
  struct CachedIndex {
    std::string name;
    NdbDictionary::Object::Type type;     // OrderedIndex or UniqueHashIndex
    NdbDictionary::Object::State state;
  };

  using IndexListPtr = std::shared_ptr<const std::vector<CachedIndex>>;

  struct CachedTable {
    Uint32 tableId;
    Uint32 schemaVersion;
    std::chrono::steady_clock::time_point loaded_at;
    IndexListPtr indexes;
  };

  explicit RdrsSchemaCache(Uint32 ttl_seconds)
      : m_ttl(std::chrono::seconds(ttl_seconds)),
        m_ttl_enabled(ttl_seconds != 0) {}

  /**
   * Get the cached index list for a table. If the cache is empty, stale
   * (tableId/schemaVersion mismatch) or past the TTL, calls
   * dict->listIndexes() to refresh.
   *
   * @param dict      NDB dictionary (for listIndexes on cache miss)
   * @param table     NDB table object (from dict->getTable(), already cached by NDB API)
   * @param db        Database name
   * @param table_name Table name
   * @return Shared pointer to the index vector (kept alive for the
   *         caller even across concurrent refreshes), or nullptr on error
   */
  IndexListPtr getIndexes(
      const NdbDictionary::Dictionary* dict,
      const NdbDictionary::Table* table,
      const std::string& db,
      const std::string& table_name);

  /**
   * Invalidate cache entry for a table. Called on schema-related errors
   * before retrying the query.
   */
  void invalidate(const std::string& db, const std::string& table_name);

 private:
  static std::string makeKey(const std::string& db,
                             const std::string& table_name) {
    return db + "/" + table_name;
  }

  bool fresh(const CachedTable& entry, Uint32 tableId, Uint32 schemaVersion,
             std::chrono::steady_clock::time_point now) const {
    return entry.tableId == tableId && entry.schemaVersion == schemaVersion &&
           (!m_ttl_enabled || now - entry.loaded_at < m_ttl);
  }

  const std::chrono::steady_clock::duration m_ttl;
  const bool m_ttl_enabled;
  mutable std::shared_mutex m_mutex;
  std::unordered_map<std::string, CachedTable> m_cache;
};

/**
 * Global schema cache instance. Initialized by start_schema_cache(),
 * destroyed by stop_schema_cache().
 */
extern RdrsSchemaCache* g_schema_cache;

void start_schema_cache(Uint32 ttl_seconds);
void stop_schema_cache();

#endif  // STORAGE_NDB_SRC_RONSQL_RDRSSCHEMACACHE_HPP
