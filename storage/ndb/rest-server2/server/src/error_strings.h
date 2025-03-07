/*
 * Copyright (C) 2023, 2025 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_ERROR_STRS_H_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_ERROR_STRS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>  // For NULL

// Enum for error codes
typedef enum {
  NO_ERROR                              = 0,
  ERROR_NDB_INIT_FAILED                 = 1,
  ERROR_RONDB_MGM_CONNECT_FAILED        = 2,
  ERROR_CLUSTER_NOT_READY               = 3,
  ERROR_NDB_OBJECT_INIT_FAILED          = 4,
  ERROR_TRANSACTION_START_FAILED        = 5,
  ERROR_OPERATION_ALREADY_CREATED       = 6,
  ERROR_READ_OPERATION_FAILED           = 7,
  ERROR_INVALID_COLUMN_DATA             = 8,
  ERROR_TRANSACTION_EXEC_FAILED         = 9,
  ERROR_RESPONSE_BUFFER_COPY_FAILED     = 10,
  ERROR_DB_TABLE_NOT_EXIST              = 11,
  ERROR_COLUMN_NOT_EXIST                = 12,
  ERROR_WRONG_PRIMARY_KEY_COUNT         = 13,
  ERROR_WRONG_PRIMARY_KEY_COLUMN        = 14,
  ERROR_WRONG_DATA_TYPE                 = 15,
  ERROR_RESPONSE_BUFFER_OVERFLOW        = 16,
  ERROR_UNSUPPORTED_HASH_INDEX          = 17,
  ERROR_UNDEFINED_DATA_TYPE             = 18,
  ERROR_UNABLE_TO_READ_DATA             = 19,
  ERROR_COLUMN_LENGTH_TOO_BIG           = 20,
  ERROR_PROGRAMMING_BUFFER_TOO_SMALL    = 21,
  ERROR_SET_LOCK_MODE_FAILED            = 22,
  ERROR_SET_EQUAL_FAILED                = 23,
  ERROR_NO_FREE_API_SLOT                = 24,
  ERROR_UNSUPPORTED_DATA_RETURN_TYPE    = 25,
  ERROR_UNSUPPORTED_BLOB_TEXT_READ      = 26,
  ERROR_INVALID_DATE_TIME               = 27,
  ERROR_PROGRAMMING_BUG                 = 28,
  ERROR_SCAN_OPERATION_FAILED           = 29,
  ERROR_SET_LOCK_MODE_FAILED_TUPLES     = 30,
  ERROR_SET_FILTER_FAILED               = 31,
  ERROR_LOAD_INDEX_FAILED               = 32,
  ERROR_RONDB_CONNECTION_CLOSED         = 33,
  ERROR_PROGRAMMING_CONNECTION_SHUTDOWN = 34,
  ERROR_RONDB_NOT_INITIALIZED           = 35,
  ERROR_RONDB_RECONNECTION_IN_PROGRESS  = 36,
  ERROR_COLUMN_READ_FAILED              = 37,
  ERROR_EMPTY_IDENTIFIER                = 38,
  ERROR_IDENTIFIER_TOO_LONG             = 39,
  ERROR_INVALID_IDENTIFIER              = 40,
  ERROR_MAX_IDENTIFIER_LENGTH           = 41,
  ERROR_INVALID_FILTER_COLUMN_NAME      = 42,
  ERROR_REQUIRED_FILTER_COLUMN          = 43,
  ERROR_MIN_FILTER_COLUMN               = 44,
  ERROR_MAX_FILTER_COLUMN               = 45,
  ERROR_INVALID_COLUMN_NAME             = 46,
  ERROR_REQUIRED_FILTER_COLUMN_VALUE    = 47,
  ERROR_NULL_FILTER_COLUMN_VALUE        = 48,
  ERROR_MIN_DB                          = 49,
  ERROR_MAX_DB                          = 50,
  ERROR_INVALID_DB_NAME                 = 51,
  ERROR_MIN_TABLE                       = 52,
  ERROR_MAX_TABLE                       = 53,
  ERROR_INVALID_TABLE_NAME              = 54,
  ERROR_INVALID_OPERATION_ID            = 55,
  ERROR_INVALID_FILTERS                 = 56,
  ERROR_UNIQUE_FILTER                   = 57,
  ERROR_MIN_READ_COLUMN                 = 58,
  ERROR_INVALID_READ_COLUMN_NAME        = 59,
  ERROR_INVALID_READ_COLUMNS            = 60,
  ERROR_UNIQUE_READ_COLUMN              = 61,
  ERROR_INVALID_METHOD                  = 62,
  ERROR_INVALID_RELATIVE_URL            = 63,
  ERROR_INVALID_BODY                    = 64,
  ERROR_RONSQL_TEMPORARY                = 65,
  ERROR_RONSQL_PERMANENT                = 66,
  ERROR_MEMORY_ALLOCATION_FAILURE       = 67,
  ERROR_TOO_MANY_COLUMNS                = 68,
  ERROR_TOO_LARGE_ROWS                  = 69,
  ERROR_SET_PK_MULTIPLE                 = 70,
  ERROR_READ_COLUMN_MULTIPLE            = 71,
  ERROR_AVRO_SCHEMA_PARSE_FAIL          = 72,
  ERROR_AVRO_STRUCT_CREATION_FAILED     = 73,
  ERROR_AVRO_SCHEMA_STRUCT_NOT_FOUND    = 74,
  ERROR_AVRO_UNMARSHAL_FAILED           = 75,
  ERROR_AVRO_JSON_CREATION_FAILED       = 76,
  __MAX_INDEX__                         = 77  // this SHOULD always be last with max index number
} ErrorCode;

// Struct to tie error codes with messages
typedef struct {
  ErrorCode code;
  const char *message;
} ErrorEntry;

// Lookup table using direct indexing (O(1) lookup)
static const ErrorEntry errorTable[] = {
    {NO_ERROR, ""},  // Index 0 is unused
    {ERROR_NDB_INIT_FAILED, "ndb_init() failed."},
    {ERROR_RONDB_MGM_CONNECT_FAILED, "Failed to connect to RonDB mgm server."},
    {ERROR_CLUSTER_NOT_READY, "Cluster was not ready within 30 secs."},
    {ERROR_NDB_OBJECT_INIT_FAILED, "Failed to initialize ndb object."},
    {ERROR_TRANSACTION_START_FAILED, "Failed to start transaction."},
    {ERROR_OPERATION_ALREADY_CREATED, "An operation has already been created."},
    {ERROR_READ_OPERATION_FAILED, "Failed to start read operation."},
    {ERROR_INVALID_COLUMN_DATA, "Invalid column data."},
    {ERROR_TRANSACTION_EXEC_FAILED, "Failed to execute transaction."},
    {ERROR_RESPONSE_BUFFER_COPY_FAILED, "Unable to copy data to the response buffer."},
    {ERROR_DB_TABLE_NOT_EXIST, "Database/Table does not exist."},
    {ERROR_COLUMN_NOT_EXIST, "Column does not exist."},
    {ERROR_WRONG_PRIMARY_KEY_COUNT, "Wrong number of primary-key columns."},
    {ERROR_WRONG_PRIMARY_KEY_COLUMN, "Wrong primary-key column."},
    {ERROR_WRONG_DATA_TYPE, "Wrong data type."},
    {ERROR_RESPONSE_BUFFER_OVERFLOW, "Response buffer overflow."},
    {ERROR_UNSUPPORTED_HASH_INDEX, "Hash indexes on float and double; and indexes on Blob types are not supported."},
    {ERROR_UNDEFINED_DATA_TYPE, "Undefined data type."},
    {ERROR_UNABLE_TO_READ_DATA, "Unable to read data."},
    {ERROR_COLUMN_LENGTH_TOO_BIG, "Column length too big."},
    {ERROR_PROGRAMMING_BUFFER_TOO_SMALL, "Programming error buffer is too small."},
    {ERROR_SET_LOCK_MODE_FAILED, "Failed to set lock mode for readTuple."},
    {ERROR_SET_EQUAL_FAILED, "Failed to set NdbOperation::equal()."},
    {ERROR_NO_FREE_API_SLOT, "Failed to find free API node slot."},
    {ERROR_UNSUPPORTED_DATA_RETURN_TYPE, "Data return type is not supported."},
    {ERROR_UNSUPPORTED_BLOB_TEXT_READ, "Reading BLOB/TEXT column is not supported yet."},
    {ERROR_INVALID_DATE_TIME, "Invalid Date/Time."},
    {ERROR_PROGRAMMING_BUG, "Programming error. Please report bug."},
    {ERROR_SCAN_OPERATION_FAILED, "Failed to start scan operation."},
    {ERROR_SET_LOCK_MODE_FAILED_TUPLES, "Failed to set lock mode for readTuples."},
    {ERROR_SET_FILTER_FAILED, "Failed to set filter."},
    {ERROR_LOAD_INDEX_FAILED, "Failed to load index."},
    {ERROR_RONDB_CONNECTION_CLOSED, "RonDB connection is not open."},
    {ERROR_PROGRAMMING_CONNECTION_SHUTDOWN, "RonDB connection has been shut down. Use Init() fn."},
    {ERROR_RONDB_NOT_INITIALIZED, "RonDB connection and object pool is not initialized."},
    {ERROR_RONDB_RECONNECTION_IN_PROGRESS, "RonDB reconnection already in progress."},
    {ERROR_COLUMN_READ_FAILED, "Failed to read column."},
    {ERROR_EMPTY_IDENTIFIER, "identifier is empty"},
    {ERROR_IDENTIFIER_TOO_LONG, "identifier is too large"},
    {ERROR_INVALID_IDENTIFIER, "identifier carries an invalid character"},
    {ERROR_MAX_IDENTIFIER_LENGTH, "max allowed length is"},
    {ERROR_INVALID_FILTER_COLUMN_NAME, "filter column name is invalid"},
    {ERROR_REQUIRED_FILTER_COLUMN, "Field validation for 'Column' failed on the 'required' tag"},
    {ERROR_MIN_FILTER_COLUMN, "Field validation for 'Column' failed on the 'min' tag"},
    {ERROR_MAX_FILTER_COLUMN, "Field validation for 'Column' failed on the 'max' tag"},
    {ERROR_INVALID_COLUMN_NAME, "column name is invalid"},
    {ERROR_REQUIRED_FILTER_COLUMN_VALUE, "Value cannot be empty"},
    {ERROR_NULL_FILTER_COLUMN_VALUE, "Field validation for 'Value' failed on the 'required' tag"},
    {ERROR_MIN_DB, "Field validation for 'DB' failed on the 'min' tag"},
    {ERROR_MAX_DB, "Field validation for 'DB' failed on the 'max' tag"},
    {ERROR_INVALID_DB_NAME, "db name is invalid"},
    {ERROR_MIN_TABLE, "Field validation for 'Table' failed on the 'min' tag"},
    {ERROR_MAX_TABLE, "Field validation for 'Table' failed on the 'max' tag"},
    {ERROR_INVALID_TABLE_NAME, "table name is invalid"},
    {ERROR_INVALID_OPERATION_ID, "operationId is invalid"},
    {ERROR_INVALID_FILTERS, "Field validation for 'Filters' failed"},
    {ERROR_UNIQUE_FILTER, "field validation for filter failed on the 'unique' tag"},
    {ERROR_MIN_READ_COLUMN, "Field validation for 'ReadColumn' failed on the 'min' tag"},
    {ERROR_INVALID_READ_COLUMN_NAME, "read column name is invalid"},
    {ERROR_INVALID_READ_COLUMNS, "field validation for read columns failed"},
    {ERROR_UNIQUE_READ_COLUMN, "field validation for 'ReadColumns' failed on the 'unique' tag"},
    {ERROR_INVALID_METHOD, "Field validation for 'Method' failed"},
    {ERROR_INVALID_RELATIVE_URL, "Field validation for 'RelativeURL' failed"},
    {ERROR_INVALID_BODY, "Field validation for 'Body' failed"},
    {ERROR_RONSQL_TEMPORARY, "RonSQL temporary error"},
    {ERROR_RONSQL_PERMANENT, "RonSQL general error"},
    {ERROR_MEMORY_ALLOCATION_FAILURE, "Memory allocation failure"},
    {ERROR_TOO_MANY_COLUMNS, "Read more columns than table has"},
    {ERROR_TOO_LARGE_ROWS, "Row size read bigger than allowed"},
    {ERROR_SET_PK_MULTIPLE, "Set same PK column several times"},
    {ERROR_READ_COLUMN_MULTIPLE, "Read column several times"},
    {ERROR_AVRO_SCHEMA_PARSE_FAIL, "Failed to parse avro schema"},
    {ERROR_AVRO_STRUCT_CREATION_FAILED, "Failed to create struct for avro schema"},
    {ERROR_AVRO_SCHEMA_STRUCT_NOT_FOUND, "Avro schema struct not found"},
    {ERROR_AVRO_UNMARSHAL_FAILED, "Avro failed to unmarshal data"},
    {ERROR_AVRO_JSON_CREATION_FAILED, "Avro failed create json string"},
    {__MAX_INDEX__, "__MAX_INDEX__ Place holder"}
};

// Fast error message lookup function
static inline const char *
rdrsErrorMessage(ErrorCode code) {
  if (code > 0 && code <= __MAX_INDEX__) {
    return errorTable[code].message ? errorTable[code].message
                                    : "Unknown error";
  }
  return "Unknown error";
}

#ifdef __cplusplus
}
#endif

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_ERROR_STRS_H_

