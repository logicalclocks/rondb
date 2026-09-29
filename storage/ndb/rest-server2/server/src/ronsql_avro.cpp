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

#include "ronsql_avro.hpp"

#include "fs_cache.hpp"
#include "metadata.hpp"

RdrsRonSQLAvroDecoder::~RdrsRonSQLAvroDecoder() {
  for (Column &column : m_columns) {
    fs_cache_dec_ref_count(reinterpret_cast<char *>(column.cache_entry));
  }
}

const void *RdrsRonSQLAvroDecoder::prepare_column(const char *database,
                                                  const char *table,
                                                  const char *column) {
  // Added before the lookup, so that no allocation can fail between taking
  // the cache entry reference and recording it for the destructor.
  m_columns.push_back(Column{std::string("`") + database + "`.`" + table +
                               "`.`" + column + "`",
                             nullptr,
                             nullptr});
  Column &prepared = m_columns.back();
  prepared.decoder = fs_cache_get_complex_feature_decoder(
    database, table, column, &prepared.cache_entry);
  if (prepared.decoder == nullptr) {
    const std::string label = prepared.label;
    m_columns.pop_back();
    throw RonSQLPermanentError(
      RonSQLErrorClass::SEMANTIC,
      "AVRO(" + label + "): the feature store metadata cached in RDRS has"
      " no Avro schema for this column.  AVRO() decodes the complex"
      " features (arrays, structs) of a feature group that a feature view"
      " serves.");
  }
  return &prepared;
}

void RdrsRonSQLAvroDecoder::decode(const void *column_handle,
                                   const unsigned char *bytes,
                                   size_t len,
                                   std::string &json) {
  const Column *prepared = static_cast<const Column *>(column_handle);
  RS_Status status = prepared->decoder->decode(bytes, len, json);
  if (status.http_code != SUCCESS) {
    throw RonSQLPermanentError(
      RonSQLErrorClass::SEMANTIC,
      "AVRO(" + prepared->label + "): a value is not Avro data of the"
      " feature's schema: " + status.message);
  }
}
