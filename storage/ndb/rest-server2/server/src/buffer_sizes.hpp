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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_BUFFER_SIZES_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_BUFFER_SIZES_HPP_

/*
 * RONDB-1144: every size the request/response buffer code depends on,
 * derived at compile time from the NDB limits the data node is built with.
 * No NDB-derived byte count is written here, so a release branch with larger limits
 * (26.10: 4096 columns, 72000-byte rows) gets correct sizes by building.
 *
 * Vocabulary (see the design document): a response is a fixed header, a
 * fixed data part (column table and every non-blob value, stored as text)
 * and then its blobs. The fixed part of one response, and one operation
 * request, must each fit in a single buffer; that is what the minimums
 * below bound.
 */

#include <mysql_com.h>    // NAME_LEN, which ndb_limits.h uses for MAX_ATTR_NAME_SIZE
#include <ndb_limits.h>
#include <ndb_types.h>

#include "rdrs_const.h"
#include "rdrs_dal.h"     // RS_STATUS_MSG_LEN

constexpr Uint32 rdrs_align4(Uint32 n) { return (n + 3U) & ~3U; }

constexpr Uint32 rdrs_next_pow2(Uint32 n) {
  Uint32 pow2 = 1;
  while (pow2 < n) {
    pow2 <<= 1;
  }
  return pow2;
}

/* The NDB row limit in bytes: every non-blob value of a row together. */
constexpr Uint32 MAX_ROW_BYTES = MAX_TUPLE_SIZE_IN_WORDS * 4;

/* A character column is converted to utf8mb4 (at most 3 bytes per source
 * byte for any single-byte charset) and then JSON-escaped (a control
 * character becomes \u00xx, 6 bytes). A byte that grew to 3 is non-ASCII
 * and is never escaped, so the two factors do not multiply: 6 is the
 * ceiling per source byte. */
constexpr Uint32 MAX_TEXT_EXPANSION = 6;

/* Upper bound accepted for Internal.OperationIDMaxSize, so the operation
 * id can be counted in the minimums below. */
constexpr Uint32 MAX_OPERATION_ID_RESERVE = 4096;

/* Space the request encoder reserves for column names when an operation
 * names none and every table column must be filled in later from the
 * dictionary: an offset array, then per column a type word, a length word,
 * the name and its terminator, 4-aligned. Matches what
 * PKRRequest::addReadColumns/addReadColumnName write. */
constexpr Uint32 REQ_COLNAME_RESERVE =
    ADDRESS_SIZE + ((MAX_ATTRIBUTES_IN_TABLE + 1) * ADDRESS_SIZE) +
    (MAX_ATTRIBUTES_IN_TABLE *
     (ADDRESS_SIZE + rdrs_align4(ADDRESS_SIZE + MAX_ATTR_NAME_SIZE + 1)));

/* Largest fixed part of one response: header, status message, operation
 * id, column table (count plus four words per column), every value as
 * escaped text, one terminator per value, alignment slack. */
constexpr Uint32 MIN_RESP_BUFFER =
    PK_RESP_HEADER_END + RS_STATUS_MSG_LEN +
    rdrs_align4(MAX_OPERATION_ID_RESERVE + 1) + ADDRESS_SIZE +
    (MAX_ATTRIBUTES_IN_TABLE * 4 * ADDRESS_SIZE) +
    (MAX_ROW_BYTES * MAX_TEXT_EXPANSION) + MAX_ATTRIBUTES_IN_TABLE +
    ADDRESS_SIZE;

/* Largest operation request: header, db and table names, the primary key
 * (count, per column two pointer words plus the name, all key bytes with a
 * length word each), the operation id, and the column-name reservation.
 * A key whose JSON text exceeds the NDB key limit is placed by
 * native_request_upper_bound instead. */
constexpr Uint32 MIN_REQ_BUFFER =
    PK_REQ_HEADER_END +
    (2 * rdrs_align4(ADDRESS_SIZE + MAX_ATTR_NAME_SIZE + 1)) + ADDRESS_SIZE +
    (MAX_ATTRIBUTES_IN_INDEX *
     ((3 * ADDRESS_SIZE) +
      rdrs_align4(ADDRESS_SIZE + MAX_ATTR_NAME_SIZE + 1))) +
    (MAX_KEY_SIZE_IN_WORDS * 4) + (MAX_ATTRIBUTES_IN_INDEX * 2 * ADDRESS_SIZE) +
    rdrs_align4(ADDRESS_SIZE + MAX_OPERATION_ID_RESERVE + 1) +
    REQ_COLNAME_RESERVE;

/* A new response (or operation request) may only start in a buffer that
 * still has this much room; otherwise it goes to a fresh buffer. */
constexpr Uint32 RESP_START_RESERVE = MIN_RESP_BUFFER;
constexpr Uint32 REQ_START_RESERVE = MIN_REQ_BUFFER;

/* The buffer sizes: the power of two above twice the minimum, so that
 * several responses pack into one buffer. Not settings; see the design
 * document for why a user-chosen value can only be worse. */
constexpr Uint32 DEFAULT_RESP_BUFFER = rdrs_next_pow2(2 * MIN_RESP_BUFFER);
constexpr Uint32 DEFAULT_REQ_BUFFER = rdrs_next_pow2(2 * MIN_REQ_BUFFER);

static_assert(DEFAULT_RESP_BUFFER >= 2 * MIN_RESP_BUFFER,
              "response buffer must hold two worst-case responses");
static_assert(DEFAULT_REQ_BUFFER >= 2 * MIN_REQ_BUFFER,
              "request buffer must hold two worst-case operation requests");
static_assert(DEFAULT_RESP_BUFFER % ADDRESS_SIZE == 0 &&
              DEFAULT_REQ_BUFFER % ADDRESS_SIZE == 0,
              "buffer sizes must be word aligned");
/* A limit increase that outgrows what one buffer can address fails the
 * build here rather than the first wide read after a merge. */
static_assert(MIN_RESP_BUFFER < (1U << 30) && MIN_REQ_BUFFER < (1U << 30),
              "a single buffer must stay addressable with 32-bit offsets");

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_BUFFER_SIZES_HPP_
