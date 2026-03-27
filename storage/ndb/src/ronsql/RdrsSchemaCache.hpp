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
 * that listIndexes() is only called on first access or after schema changes.
 * Callers use the cached names with dict->getIndex() to get the actual
 * Index objects (fast, NDB API internal cache).
 *
 * Thread-safe via std::shared_mutex (readers share, writers exclusive).
 */
class RdrsSchemaCache {
 public:
  struct CachedIndex {
    std::string name;
    NdbDictionary::Object::Type type;     // OrderedIndex or UniqueHashIndex
    NdbDictionary::Object::Status state;
  };

  struct CachedTable {
    Uint32 tableId;
    Uint32 schemaVersion;
    std::vector<CachedIndex> indexes;
  };

  /**
   * Get the cached index list for a table. If the cache is empty or stale
   * (tableId/schemaVersion mismatch), calls dict->listIndexes() to refresh.
   *
   * @param dict      NDB dictionary (for listIndexes on cache miss)
   * @param table     NDB table object (from dict->getTable(), already cached by NDB API)
   * @param db        Database name
   * @param table_name Table name
   * @return Pointer to cached index vector, or nullptr on error
   */
  const std::vector<CachedIndex>* getIndexes(
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

  mutable std::shared_mutex m_mutex;
  std::unordered_map<std::string, CachedTable> m_cache;
};

/**
 * Global schema cache instance. Initialized by start_schema_cache(),
 * destroyed by stop_schema_cache().
 */
extern RdrsSchemaCache* g_schema_cache;

void start_schema_cache();
void stop_schema_cache();

#endif  // STORAGE_NDB_SRC_RONSQL_RDRSSCHEMACACHE_HPP
