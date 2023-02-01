/*
 * Copyright (C) 2022 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_CONST_H_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_CONST_H_

#ifdef __cplusplus
extern "C" {
#endif

// 4 bytes. Max addressable memrory is 4GB
// which is max supported blob size
#define ADDRESS_SIZE 4

// Request Type Identifiers
#define RDRS_PK_REQ_ID     1
#define RDRS_PK_RESP_ID    2
#define RDRS_BATCH_REQ_ID  3
#define RDRS_BATCH_RESP_ID 4

// Data types
// Everything is a string.
// However for RDRS_STRING_DATATYPE the string
// is enclosed in quotes. This is how JSON works
#define RDRS_UNKNOWN_DATATYPE  0
#define RDRS_STRING_DATATYPE   1
#define RDRS_INTEGER_DATATYPE  2
#define RDRS_FLOAT_DATATYPE    3
#define RDRS_BINARY_DATATYPE   4
#define RDRS_DATETIME_DATATYPE 5
#define RDRS_BIT_DATATYPE      6

// Primary Key Read Request Header Indexes
#define PK_REQ_OP_TYPE_IDX   0
#define PK_REQ_CAPACITY_IDX  1
#define PK_REQ_LENGTH_IDX    2
#define PK_REQ_DB_IDX        3
#define PK_REQ_TABLE_IDX     4
#define PK_REQ_PK_COLS_IDX   5
#define PK_REQ_READ_COLS_IDX 6
#define PK_REQ_OP_ID_IDX     7
#define PK_REQ_HEADER_END    32

// Primary Key Read Response Header Indexes
#define PK_RESP_OP_TYPE_IDX   0
#define PK_RESP_OP_STATUS_IDX 1
#define PK_RESP_CAPACITY_IDX  2
#define PK_RESP_LENGTH_IDX    3
#define PK_RESP_DB_IDX        4
#define PK_RESP_TABLE_IDX     5
#define PK_RESP_COLS_IDX      6
#define PK_RESP_OP_ID_IDX     7
#define PK_RESP_HEADER_END    32

// Primary Key Read Request Header Indexes

#ifdef __cplusplus
}
#endif
#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_CONST_H_
