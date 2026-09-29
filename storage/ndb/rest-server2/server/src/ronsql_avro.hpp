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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_RONSQL_AVRO_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_RONSQL_AVRO_HPP_

#include <deque>
#include <string>

#include "storage/ndb/src/ronsql/RonSQLCommon.hpp"

class FSCacheEntry;
namespace metadata {
class AvroDecoder;
}  // namespace metadata

/*
 * RONDB-1135: the RonSQL AVRO(column) decoder of RDRS, one per /ronsql
 * request.
 *
 * The RonSQL database is an online feature store and the table
 * "<feature group>_<version>" one of its feature groups, so the column is
 * a feature of that feature group.  Its Avro schema comes from the feature
 * store metadata cache (fs_cache): the decoder that a cached feature view
 * serving the feature registered with the Go Avro library, the same
 * decoder the feature_store and batch_feature_store endpoints use.  The
 * cache holds every feature view (preloaded at startup, kept current by
 * events on hopsworks.feature_view), so AVRO() decodes any complex feature
 * that some feature view serves.
 *
 * Each prepared column holds a reference on its cache entry, which keeps
 * the decoder registered, until the request is done.
 */
class RdrsRonSQLAvroDecoder final : public RonSQLAvroDecoder {
 public:
  RdrsRonSQLAvroDecoder() = default;
  ~RdrsRonSQLAvroDecoder() override;
  RdrsRonSQLAvroDecoder(const RdrsRonSQLAvroDecoder &) = delete;
  RdrsRonSQLAvroDecoder &operator=(const RdrsRonSQLAvroDecoder &) = delete;

  const void *prepare_column(const char *database,
                             const char *table,
                             const char *column) override;
  void decode(const void *column_handle,
              const unsigned char *bytes,
              size_t len,
              std::string &json) override;

 private:
  struct Column {
    std::string label;  // `database`.`table`.`column`, for messages
    const metadata::AvroDecoder *decoder;
    FSCacheEntry *cache_entry;  // referenced while decoder is in use
  };
  // Column handles point at the elements, which a deque never moves.
  std::deque<Column> m_columns;
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_RONSQL_AVRO_HPP_
