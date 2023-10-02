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

// number of bytes need to store string length for varchar columns in NDB
static inline int bytes_for_ndb_str_len(int ndb_str_len) {
  if (ndb_str_len <= 255) {
    return 1;
  } else {
    return 2;
  }
}

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
#define PK_REQ_FLAGS_IDX     3
#define PK_REQ_DB_IDX        4
#define PK_REQ_TABLE_IDX     5
#define PK_REQ_PK_COLS_IDX   6
#define PK_REQ_READ_COLS_IDX 7
#define PK_REQ_OP_ID_IDX     8
#define PK_REQ_HEADER_END    36  // NOTE: Update this when you add / remove fields from  header

// Primary Key Read Response Header Indexes
#define PK_RESP_OP_TYPE_IDX    0
#define PK_RESP_OP_STATUS_IDX  1
#define PK_RESP_OP_MESSAGE_IDX 2
#define PK_RESP_CAPACITY_IDX   3
#define PK_RESP_LENGTH_IDX     4
#define PK_RESP_DB_IDX         5
#define PK_RESP_TABLE_IDX      6
#define PK_RESP_COLS_IDX       7
#define PK_RESP_OP_ID_IDX      8
#define PK_RESP_HEADER_END     36  // NOTE: Update this when you add / remove fields from  header

// Hopsworks
// Update the following constants if there are changes in the hopsworks schema
#define API_KEY_PREFIX_SIZE                45 + 1   /* +1 for ndb len or '\0'*/
#define API_KEY_NAME_SIZE                  45 + 1   /* +1 for ndb len or '\0'*/
#define API_KEY_SECRET_SIZE                512 + 2  /* +2 for ndb len or '\0'*/
#define API_KEY_SALT_SIZE                  256 + 2  /* +2 for ndb len or '\0'*/
#define USERS_EMAIL_SIZE                   150 + 1  /* +1 for ndb len or '\0'*/
#define PROJECT_TEAM_TEAM_MEMBER_SIZE      150 + 1  /* +1 for ndb len or '\0'*/
#define PROJECT_PROJECTNAME_SIZE           100 + 1  /* +1 for ndb len or '\0'*/
#define FEATURE_STORE_NAME_SIZE            100 + 1  /* +1 for ndb len or '\0'*/
#define FEATURE_VIEW_NAME_SIZE             63 + 1   /* +1 for ndb len or '\0'*/
#define TRAINING_DATASET_JOIN_PREFIX_SIZE  63 + 1   /* +1 for ndb len or '\0'*/
#define FEATURE_GROUP_NAME_SIZE            63 + 1   /* +1 for ndb len or '\0'*/
#define TRAINING_DATASET_FEATURE_NAME_SIZE 1000 + 2 /* +2 for ndb len or '\0'*/
#define TRAINING_DATASET_FEATURE_TYPE_SIZE 1000 + 2 /* +2 for ndb len or '\0'*/
#define TRAINING_DATASET_JOIN_PREFIX_SIZE  63 + 1   /* +1 for ndb len or '\0'*/
#define ON_DEMAND_FEATURE_NAME_SIZE        1000 + 2 /* +2 for ndb len or '\0'*/
#define CACHE_FEATURE_SIZE                 63 + 1   /* +1 for ndb len or '\0'*/
#define SERVING_KEY_FEATURE_NAME_SIZE      1000 + 2 /* +2 for ndb len or '\0'*/
#define SERVING_KEY_JOIN_ON_SIZE           1000 + 2 /* +2 for ndb len or '\0'*/
#define SERVING_KEY_JOIN_PREFIX_SIZE       63 + 1   /* +1 for ndb len or '\0'*/
#define FEATURE_GROUP_SUBJECT_SIZE         255 + 1 /* +1 for ndb len or '\0'*/
#define FEATURE_GROUP_SCHEMA_SIZE          29000 + 2 /* +2 for ndb len or '\0'*/

// Data types
#define DECIMAL_MAX_SIZE_IN_BYTES           9 * 4   /*4 bytes per 9 digits. 65/9 + 1 * 4*/
#define CHAR_MAX_SIZE_IN_BYTES              255 * 4 /*MAX 255 chars. *4 for char set*/
#define DECIMAL_MAX_PRECISION_SIZE_IN_BYTES 65
#define DECIMAL_MAX_STR_LEN_IN_BYTES        DECIMAL_MAX_PRECISION_SIZE_IN_BYTES + 3
#define BINARY_MAX_SIZE_IN_BYTES            255 /*encoding 255 bytes takes max 340 bytes (255*4/3) */
#define KEY_MAX_SIZE_IN_BYTES               1023 * 4 /*encoding 4092 bytes takes max 5456 bytes (4092*4/3) */
#define DATE_MAX_SIZE_IN_BYTES              3
#define TIME2_MAX_SIZE_IN_BYTES             6
#define DATETIME_MAX_SIZE_IN_BYTES          8
#define TIMESTAMP2_MAX_SIZE_IN_BYTES        7
#define MAX_TUPLE_SIZE_IN_BYTES             7501 * 4
#define MAX_TUPLE_SIZE_IN_BYTES_ENCODED     40008
#define MAX_TUPLE_SIZE_IN_BYTES_ESCAPED                                                            \
  MAX_TUPLE_SIZE_IN_BYTES * 2 /*worst cast every character needs to be escaped*/
#define BIT_MAX_SIZE_IN_BYTES         4096 / 8
#define BIT_MAX_SIZE_IN_BYTES_ENCODED 686

#ifdef __cplusplus
}
#endif
#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_CONST_H_
