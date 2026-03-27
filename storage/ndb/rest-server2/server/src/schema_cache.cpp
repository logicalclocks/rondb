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

#include "RdrsSchemaCache.hpp"

RdrsSchemaCache* g_schema_cache = nullptr;

void start_schema_cache() {
  g_schema_cache = new RdrsSchemaCache();
}

void stop_schema_cache() {
  delete g_schema_cache;
  g_schema_cache = nullptr;
}

const std::vector<RdrsSchemaCache::CachedIndex>*
RdrsSchemaCache::getIndexes(
    const NdbDictionary::Dictionary* dict,
    const NdbDictionary::Table* table,
    const std::string& db,
    const std::string& table_name) {

  std::string key = makeKey(db, table_name);
  Uint32 tableId = table->getTableId();
  Uint32 schemaVersion = table->getObjectVersion();

  // Fast path: shared lock, check cache
  {
    std::shared_lock<std::shared_mutex> lock(m_mutex);
    auto it = m_cache.find(key);
    if (it != m_cache.end() &&
        it->second.tableId == tableId &&
        it->second.schemaVersion == schemaVersion) {
      return &it->second.indexes;
    }
  }

  // Slow path: exclusive lock, populate cache
  std::unique_lock<std::shared_mutex> lock(m_mutex);

  // Double-check after acquiring exclusive lock
  auto it = m_cache.find(key);
  if (it != m_cache.end() &&
      it->second.tableId == tableId &&
      it->second.schemaVersion == schemaVersion) {
    return &it->second.indexes;
  }

  // Call the expensive listIndexes()
  NdbDictionary::Dictionary::List index_list;
  if (dict->listIndexes(index_list, *table) != 0) {
    return nullptr;
  }

  // Populate cache entry
  CachedTable& entry = m_cache[key];
  entry.tableId = tableId;
  entry.schemaVersion = schemaVersion;
  entry.indexes.clear();
  entry.indexes.reserve(index_list.count);

  for (Uint32 i = 0; i < index_list.count; i++) {
    NdbDictionary::Dictionary::List::Element& elem = index_list.elements[i];
    CachedIndex ci;
    ci.name = elem.name;
    ci.type = (NdbDictionary::Object::Type)elem.type;
    ci.state = elem.state;
    entry.indexes.push_back(std::move(ci));
  }

  return &entry.indexes;
}

void RdrsSchemaCache::invalidate(const std::string& db,
                                  const std::string& table_name) {
  std::unique_lock<std::shared_mutex> lock(m_mutex);
  m_cache.erase(makeKey(db, table_name));
}
