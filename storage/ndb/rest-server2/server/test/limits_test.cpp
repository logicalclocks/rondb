/*
 * Copyright (C) 2026 Hopsworks AB
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

/*
 * RONDB-1144: the compile-time buffer minimums in buffer_sizes.hpp are
 * derived from the NDB limits by a formula. These tests keep the formula
 * honest by writing the worst case through the real encoders and checking
 * that it fits. They use the NDB limit macros, so they exercise 512
 * columns and 30000-byte rows on 26.02 and 4096 and 72000 on 26.10 with no
 * edit. Cluster-free.
 */

#include <gtest/gtest.h>

#include <NdbMutex.h>
#include <mysql_com.h>
#include <ndb_limits.h>

#include <cstring>
#include <string>
#include <vector>

#include "buffer_sizes.hpp"
#include "db_operations/pk/pkr_request.hpp"
#include "db_operations/pk/pkr_response.hpp"
#include "encoding.hpp"
#include "mystring.hpp"
#include "pk_data_structs.hpp"
#include "rdrs_const.h"
#include "rdrs_dal.h"

/* Owned by main.cc in the server binary; test binaries provide their own
 * (same pattern as error_handling_test.cpp). */
NdbMutex *globalConfigsMutex = nullptr;

namespace {

std::string name_of_max_length(char fill) {
  return std::string(MAX_ATTR_NAME_SIZE, fill);
}

}  // namespace

/*
 * The widest response: the longest operation id the config may allow, the
 * longest status message, every column of the widest table, and a row of
 * control characters, which escape to six bytes each. All of it must fit
 * in MIN_RESP_BUFFER through the real writer.
 */
TEST(Limits, WorstCaseResponseFitsMinRespBuffer) {
  std::vector<char> storage(MIN_RESP_BUFFER);
  RS_Buffer buffer{storage.data(), MIN_RESP_BUFFER};
  PKRResponse response(buffer, nullptr, 0);

  const std::string op_id(MAX_OPERATION_ID_RESERVE, 'o');
  ASSERT_EQ(response.SetOperationID(op_id.c_str()).http_code, SUCCESS);
  ASSERT_EQ(response.SetNoOfColumns(MAX_ATTRIBUTES_IN_TABLE).http_code,
            SUCCESS);

  /* Spread MAX_ROW_BYTES of 0x01 over all columns; each escapes to
   * \u0001, six bytes per source byte, the ceiling MAX_TEXT_EXPANSION
   * encodes. */
  Uint32 remaining = MAX_ROW_BYTES;
  Uint32 escaped_total = 0;
  for (Uint32 col = 0; col < MAX_ATTRIBUTES_IN_TABLE; col++) {
    const Uint32 cols_left = MAX_ATTRIBUTES_IN_TABLE - col;
    const Uint32 raw_len = (remaining + cols_left - 1) / cols_left;
    remaining -= raw_len;
    const std::string raw(raw_len, '\x01');
    const std::string escaped = escape_string(raw);
    ASSERT_EQ(escaped.size(), raw_len * MAX_TEXT_EXPANSION);
    escaped_total += escaped.size();
    response.SetSlot(col);
    ASSERT_EQ(response
                  .SetColumnData(escaped.c_str(), RDRS_STRING_DATATYPE,
                                 static_cast<Uint32>(escaped.size()))
                  .http_code,
              SUCCESS)
        << "column " << col << " overflowed MIN_RESP_BUFFER";
  }
  EXPECT_EQ(remaining, 0U);
  EXPECT_EQ(escaped_total, MAX_ROW_BYTES * MAX_TEXT_EXPANSION);

  const std::string message(RS_STATUS_MSG_LEN - 1, 'm');
  ASSERT_EQ(response.SetStatus(SUCCESS, message.c_str()).http_code, SUCCESS);
  Uint32 length = 0;
  ASSERT_EQ(response.Close(length).http_code, SUCCESS);
  EXPECT_LE(length, MIN_RESP_BUFFER);
  EXPECT_GT(length, MAX_ROW_BYTES * MAX_TEXT_EXPANSION);
}

/*
 * The widest operation request that names its columns: db and table names
 * of the maximum length, a key of the maximum width over the maximum
 * number of key columns, the longest operation id, and every table column
 * named at the maximum length.
 */
TEST(Limits, WorstCaseNamedColumnsRequestFitsMinReqBuffer) {
  const std::string db = name_of_max_length('d');
  const std::string table = name_of_max_length('t');
  std::vector<std::string> key_names;
  std::vector<std::string> col_names;
  key_names.reserve(MAX_ATTRIBUTES_IN_INDEX);
  col_names.reserve(MAX_ATTRIBUTES_IN_TABLE);

  PKReadParams params;
  params.path.db = db;
  params.path.table = table;
  params.operationId = std::string(MAX_OPERATION_ID_RESERVE, 'o');
  Uint32 key_bytes_left = MAX_KEY_SIZE_IN_WORDS * 4;
  for (Uint32 i = 0; i < MAX_ATTRIBUTES_IN_INDEX; i++) {
    key_names.push_back(name_of_max_length(static_cast<char>('a' + (i % 26))));
    const Uint32 cols_left = MAX_ATTRIBUTES_IN_INDEX - i;
    const Uint32 share = (key_bytes_left + cols_left - 1) / cols_left;
    key_bytes_left -= share;
    PKReadFilter filter;
    filter.column = key_names.back();
    filter.value.assign(share, 'k');
    params.filters.push_back(filter);
  }
  for (Uint32 i = 0; i < MAX_ATTRIBUTES_IN_TABLE; i++) {
    col_names.push_back(name_of_max_length(static_cast<char>('A' + (i % 26))));
    PKReadReadColumn column;
    column.column = col_names.back();
    params.readColumns.push_back(column);
  }

  std::vector<Uint32> storage(MIN_REQ_BUFFER / ADDRESS_SIZE + 1);
  RS_Buffer view{reinterpret_cast<char *>(storage.data()), MIN_REQ_BUFFER};
  Uint32 consumed = 0;
  const RS_Status status = create_native_request(params, view, consumed);
  ASSERT_EQ(status.http_code, SUCCESS) << status.message;
  EXPECT_LE(consumed, MIN_REQ_BUFFER);
  EXPECT_LE(storage[PK_REQ_LENGTH_IDX], MIN_REQ_BUFFER);
}

/*
 * The widest operation request that names no columns: the encoder reserves
 * room and the dictionary pass later fills in every table column name at
 * the maximum length. The filled-in request must still fit.
 */
TEST(Limits, WorstCaseAllColumnsRequestFitsMinReqBuffer) {
  const std::string db = name_of_max_length('d');
  const std::string table = name_of_max_length('t');
  PKReadParams params;
  params.path.db = db;
  params.path.table = table;
  params.operationId = std::string(MAX_OPERATION_ID_RESERVE, 'o');
  std::vector<std::string> key_names;
  Uint32 key_bytes_left = MAX_KEY_SIZE_IN_WORDS * 4;
  for (Uint32 i = 0; i < MAX_ATTRIBUTES_IN_INDEX; i++) {
    key_names.push_back(name_of_max_length(static_cast<char>('a' + (i % 26))));
    const Uint32 cols_left = MAX_ATTRIBUTES_IN_INDEX - i;
    const Uint32 share = (key_bytes_left + cols_left - 1) / cols_left;
    key_bytes_left -= share;
    PKReadFilter filter;
    filter.column = key_names.back();
    filter.value.assign(share, 'k');
    params.filters.push_back(filter);
  }

  std::vector<Uint32> storage(MIN_REQ_BUFFER / ADDRESS_SIZE + 1);
  RS_Buffer buffer{reinterpret_cast<char *>(storage.data()), MIN_REQ_BUFFER};
  Uint32 consumed = 0;
  const RS_Status status = create_native_request(params, buffer, consumed);
  ASSERT_EQ(status.http_code, SUCCESS) << status.message;
  EXPECT_LE(consumed, MIN_REQ_BUFFER)
      << "encoder reservation for all column names exceeds the minimum";

  PKRRequest request(&buffer);
  ASSERT_TRUE(request.addReadColumns(MAX_ATTRIBUTES_IN_TABLE));
  const std::string name = name_of_max_length('C');
  for (Uint32 i = 0; i < MAX_ATTRIBUTES_IN_TABLE; i++) {
    // addReadColumnName returns true on failure (its caller in
    // create_response treats true as an allocation error).
    ASSERT_FALSE(request.addReadColumnName(i, name.c_str(), DEFAULT_DRT));
  }
  EXPECT_LE(storage[PK_REQ_LENGTH_IDX], MIN_REQ_BUFFER)
      << "filled-in column names exceed the minimum request buffer";
  /* And the encoder's reservation must cover what was actually written,
   * or the names overrun the next operation's request in a packed
   * buffer. */
  EXPECT_LE(storage[PK_REQ_LENGTH_IDX], consumed)
      << "column-name reservation smaller than the names written";
}

/*
 * 26.10: the batch pk-write encoder has the same contract as the read
 * encoder, a bound checked before anything is written. The widest write:
 * the maximum key, the longest operation id and every table column written
 * with a name of the maximum length and a 255-byte value, more than the
 * row limit lets 4096 columns hold, so it over-covers any non-blob write. The bound must cover
 * what the encoder really wrote, and the request must fit the default
 * request buffer; a view smaller than the bound is refused with 400 and
 * nothing is written into it.
 */
TEST(Limits, WidestWriteRequestBoundHoldsAndFits) {
  const std::string db = name_of_max_length('d');
  const std::string table = name_of_max_length('t');
  PKReadParams params;
  params.path.db = db;
  params.path.table = table;
  params.operationId = std::string(MAX_OPERATION_ID_RESERVE, 'o');
  params.writeOperationType = 0;
  Uint32 key_bytes_left = MAX_KEY_SIZE_IN_WORDS * 4;
  for (Uint32 i = 0; i < MAX_ATTRIBUTES_IN_INDEX; i++) {
    const Uint32 cols_left = MAX_ATTRIBUTES_IN_INDEX - i;
    const Uint32 share = (key_bytes_left + cols_left - 1) / cols_left;
    key_bytes_left -= share;
    PKReadFilter filter;
    filter.column = name_of_max_length(static_cast<char>('a' + (i % 26)));
    filter.value.assign(share, 'k');
    params.filters.push_back(filter);
  }
  for (Uint32 i = 0; i < MAX_ATTRIBUTES_IN_TABLE; i++) {
    PKReadFilter column;
    column.column = name_of_max_length(static_cast<char>('A' + (i % 26)));
    /* 255 bytes per value: more than the row limit lets every column hold
     * at once, so this over-covers any non-blob write. */
    column.value.assign(255, 'v');
    params.writeColumns.push_back(column);
  }
  const Uint32 bound = native_write_request_upper_bound(params);
  EXPECT_LE(bound, DEFAULT_REQ_BUFFER);

  std::vector<Uint32> storage(DEFAULT_REQ_BUFFER / ADDRESS_SIZE + 1);
  RS_Buffer view{reinterpret_cast<char *>(storage.data()),
                 DEFAULT_REQ_BUFFER};
  Uint32 consumed = 0;
  RS_Status status = create_native_write_request(params, view, consumed);
  ASSERT_EQ(status.http_code, SUCCESS) << status.message;
  EXPECT_LE(consumed, bound);
  EXPECT_LE(storage[PK_REQ_LENGTH_IDX], bound);
  EXPECT_EQ(storage[PK_REQ_CAPACITY_IDX], DEFAULT_REQ_BUFFER);

  /* A view too small for the bound: refused before the first byte. */
  std::vector<Uint32> small((bound - 1) / ADDRESS_SIZE + 1, 0xA5A5A5A5U);
  RS_Buffer small_view{reinterpret_cast<char *>(small.data()), bound - 1};
  consumed = 0;
  status = create_native_write_request(params, small_view, consumed);
  EXPECT_EQ(status.http_code,
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest));
  EXPECT_NE(std::string(status.message).find("operation too large"),
            std::string::npos);
  EXPECT_EQ(consumed, 0U);
  EXPECT_EQ(small[0], 0xA5A5A5A5U);
}

/* Sanity on the derived defaults: the documented relation to the minimums,
 * and that the sizes are the ones the design expects on this branch's
 * limits (512 KiB and 256 KiB for 512 columns, 1 MiB and 2 MiB for 4096). */
TEST(Limits, DerivedSizesFollowTheLimits) {
  EXPECT_GE(DEFAULT_RESP_BUFFER, 2 * MIN_RESP_BUFFER);
  EXPECT_LT(DEFAULT_RESP_BUFFER, 4 * MIN_RESP_BUFFER);
  EXPECT_GE(DEFAULT_REQ_BUFFER, 2 * MIN_REQ_BUFFER);
  EXPECT_LT(DEFAULT_REQ_BUFFER, 4 * MIN_REQ_BUFFER);
  EXPECT_EQ(MAX_ROW_BYTES, MAX_TUPLE_SIZE_IN_WORDS * 4U);
  if (MAX_ATTRIBUTES_IN_TABLE == 512 && MAX_TUPLE_SIZE_IN_WORDS == 7500) {
    EXPECT_EQ(DEFAULT_RESP_BUFFER, 512U * 1024U);
    EXPECT_EQ(DEFAULT_REQ_BUFFER, 256U * 1024U);
  }
  if (MAX_ATTRIBUTES_IN_TABLE == 4096 && MAX_TUPLE_SIZE_IN_WORDS == 18000) {
    EXPECT_EQ(DEFAULT_RESP_BUFFER, 1024U * 1024U);
    EXPECT_EQ(DEFAULT_REQ_BUFFER, 2048U * 1024U);
  }
}

/* A spilled blob can be close to 4 GiB (the column offset space), so the
 * JSON size of its base64 must be computed in 64 bits: from 1 GiB up,
 * n * 4 wraps in 32 bits and the estimate became a few bytes, so the JSON
 * writer overran its buffer. base64_encoded_len must equal what libbase64
 * writes, and setColumnData must count it in full for the largest sizes
 * (no data is touched; only the estimate is computed). */
TEST(Limits, JsonSizeOfLargestSpilledBlob) {
  for (Uint32 n : {0U, 1U, 2U, 3U, 4U, 5U, 6U, 100U, 1000U, 65537U}) {
    std::vector<char> src(n, '\x5a');
    std::vector<char> dst(base64_encoded_len(n) + 4);
    size_t out = 0;
    base64_encode(src.data(), n, dst.data(), &out, 0);
    EXPECT_EQ(out, base64_encoded_len(n)) << "n = " << n;
  }
  const char name[] = "b";
  for (Uint64 n : {(1ULL << 30) - 1, 1ULL << 30, 3ULL << 30, 0xFFFFFFFFULL}) {
    ResultView view[1];
    PKReadResponseJSON json;
    json.init(1, view);
    const size_t before = json.getSizeJson();
    json.setColumnData(0, name, 1, nullptr, static_cast<Uint32>(n), true,
                       RDRS_BINARY_DATATYPE);
    const Uint64 encoded = 4 * ((n + 2) / 3);
    EXPECT_GE(json.getSizeJson() - before, encoded + 2) << "n = " << n;
    EXPECT_EQ(base64_encoded_len(n), encoded);
  }
}
