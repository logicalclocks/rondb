/*
 * Copyright (c) 2026, Hopsworks and/or its affiliates.
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_SCAN_ROW_SINK_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_SCAN_ROW_SINK_HPP_

#include "rdrs_dal.h"

#include <NdbApi.hpp>
#include <vector>

/*
 * Consumer of the rows produced by a scan (see scan_read_rows()). The
 * default consumer of a scan is the JSON writer of the /scan endpoint; a
 * sink lets other endpoints get at the raw NdbRecord row instead, so they
 * can convert the columns they need without a serialise/parse round-trip.
 *
 * on_row() is called once per fetched row, at most `limit` times. The row
 * memory is only valid during the call. Returning a non-success status
 * stops the scan and is propagated to the caller of scan_read_rows().
 *
 * reset() is called before every attempt of the scan (the scan may be
 * retried on transient NDB errors), so a sink must drop rows it collected
 * in a previous attempt.
 */
class ScanRowSink {
 public:
  virtual ~ScanRowSink() = default;
  virtual void reset() = 0;
  virtual RS_Status on_row(
    const NdbRecord *table_rec,
    const char *row,
    const std::vector<const NdbDictionary::Column*> &read_columns) = 0;
};


// Convert one column of a scanned row to JSON text, exactly as the /scan endpoint.
void ScanColumnToJson(const NdbRecord *table_rec,
                      const char *row,
                      const NdbDictionary::Column *column,
                      std::vector<char> &out);

/*
 * Copy the raw bytes of a BINARY/VARBINARY/LONGVARBINARY column of a
 * scanned row (without the length prefix). Returns false when the column
 * is NULL or not a binary type; `out` is then left empty.
 */
bool ScanColumnRawBytes(const NdbRecord *table_rec,
                        const char *row,
                        const NdbDictionary::Column *column,
                        std::vector<Uint8> &out);

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_SCAN_ROW_SINK_HPP_
