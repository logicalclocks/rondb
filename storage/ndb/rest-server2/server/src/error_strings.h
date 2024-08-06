/*
 * Copyright (C) 2023 Hopsworks AB
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

#define ERROR_001 "ndb_init() failed."
#define ERROR_002 "failed to connect to RonDB mgm server."
#define ERROR_003 "Cluster was not ready within 30 secs."
#define ERROR_004 "Failed to initialize ndb object."
#define ERROR_005 "Failed to start transaction."
#define ERROR_006 "An operation has already been created."
#define ERROR_007 "Failed to start read operation."
#define ERROR_008 "Invalid column data."
#define ERROR_009 "Failed to execute transaction."
#define ERROR_010 "Unable to copy data to the response buffer."
#define ERROR_011 "Database/Table does not exist."
#define ERROR_012 "Column does not exist."
#define ERROR_013 "Wrong number of primary-key columns."
#define ERROR_014 "Wrong primay-key column."
#define ERROR_015 "Wrong data type."
#define ERROR_016 "Response buffer overflow."
#define ERROR_017 "Hash indexes on float and double; and indexes on Blob types are not supported."
#define ERROR_018 "Undefined data type."
#define ERROR_019 "Unable to read data."
#define ERROR_020 "Column length too big."
#define ERROR_021 "Programming error buffer is too small."
#define ERROR_022 "Failed to set lock mode for readTuple."
#define ERROR_023 "Failed to set NdbOperation::equal()."
#define ERROR_024 "Failed to find free API node slot."
#define ERROR_025 "Data return type is not supported."
#define ERROR_026 "Reading BLOB/TEXT column is not supported yet."
#define ERROR_027 "Invalid Date/Time."
#define ERROR_028 "Programming error. Please report bug."
#define ERROR_029 "Failed to start scan operation."
#define ERROR_030 "Failed to set lock mode for readTuples."
#define ERROR_031 "Failed to set filter."
#define ERROR_032 "Failed to load index."
#define ERROR_033 "RonDB connection is not open."
#define ERROR_034 "Programming error. RonDB connection has been shutdown. Use Init() fn."
#define ERROR_035 "RonDB connection and object pool is not initialized."
#define ERROR_036 "RonDB reconnection already in progress."
#define ERROR_037 "Failed to read column."
#ifdef __cplusplus
constexpr char ERROR_038[]                          = "identifier is empty";
constexpr int ERROR_CODE_EMPTY_IDENTIFIER           = 38;
constexpr char ERROR_039[]                          = "identifier is too large";
constexpr int ERROR_CODE_IDENTIFIER_TOO_LONG        = 39;
constexpr char ERROR_040[]                          = "identifier carries an invalid character";
constexpr int ERROR_CODE_INVALID_IDENTIFIER         = 40;
constexpr char ERROR_041[]                          = "max allowed length is";
constexpr int ERROR_CODE_INVALID_IDENTIFIER_LENGTH  = 41;
constexpr char ERROR_042[]                          = "filter column name is invalid";
constexpr int ERROR_CODE_INVALID_FILTER_COLUMN_NAME = 42;
constexpr char ERROR_043[] = "Field validation for 'Column' failed on the 'required' tag";
constexpr int ERROR_CODE_REQUIRED_FILTER_COLUMN = 43;
constexpr char ERROR_044[] = "Field validation for 'Column' failed on the 'min' tag";
constexpr int ERROR_CODE_MIN_FILTER_COLUMN = 44;
constexpr char ERROR_045[] = "Field validation for 'Column' failed on the 'max' tag";
constexpr int ERROR_CODE_MAX_FILTER_COLUMN            = 45;
constexpr char ERROR_046[]                            = "column name is invalid";
constexpr int ERROR_CODE_INVALID_COLUMN_NAME          = 46;
constexpr char ERROR_047[]                            = "Value cannot be empty";
constexpr int ERROR_CODE_REQUIRED_FILTER_COLUMN_VALUE = 47;
constexpr char ERROR_048[] = "Field validation for 'Value' failed on the 'required' tag";
constexpr int ERROR_CODE_NULL_FILTER_COLUMN_VALUE = 48;
constexpr char ERROR_049[]               = "Field validation for 'DB' failed on the 'min' tag";
constexpr int ERROR_CODE_MIN_DB          = 49;
constexpr char ERROR_050[]               = "Field validation for 'DB' failed on the 'max' tag";
constexpr int ERROR_CODE_MAX_DB          = 50;
constexpr char ERROR_051[]               = "db name is invalid";
constexpr int ERROR_CODE_INVALID_DB_NAME = 51;
constexpr char ERROR_052[]               = "Field validation for 'Table' failed on the 'min' tag";
constexpr int ERROR_CODE_MIN_TABLE       = 52;
constexpr char ERROR_053[]               = "Field validation for 'Table' failed on the 'max' tag";
constexpr int ERROR_CODE_MAX_TABLE       = 53;
constexpr char ERROR_054[]               = "table name is invalid";
constexpr int ERROR_CODE_INVALID_TABLE_NAME   = 54;
constexpr char ERROR_055[]                    = "operationId is invalid";
constexpr int ERROR_CODE_INVALID_OPERATION_ID = 55;
constexpr char ERROR_056[]                    = "Field validation for 'Filters' failed";
constexpr int ERROR_CODE_INVALID_FILTERS      = 56;
constexpr char ERROR_057[]             = "field validation for filter failed on the 'unique' tag";
constexpr int ERROR_CODE_UNIQUE_FILTER = 57;
constexpr char ERROR_058[] = "Field validation for 'ReadColumn' failed on the 'min' tag";
constexpr int ERROR_CODE_MIN_READ_COLUMN          = 58;
constexpr char ERROR_059[]                        = "read column name is invalid";
constexpr int ERROR_CODE_INVALID_READ_COLUMN_NAME = 59;
constexpr char ERROR_060[]                        = "field validation for read columns failed";
constexpr int ERROR_CODE_INVALID_READ_COLUMNS     = 60;
constexpr char ERROR_061[] = "field validation for 'ReadColumns' failed on the 'unique' tag";
constexpr int ERROR_CODE_UNIQUE_READ_COLUMN   = 61;
constexpr char ERROR_062[]                    = "Field validation for 'Method' failed";
constexpr int ERROR_CODE_INVALID_METHOD       = 62;
constexpr char ERROR_063[]                    = "Field validation for 'RelativeURL' failed";
constexpr int ERROR_CODE_INVALID_RELATIVE_URL = 63;
constexpr char ERROR_064[]                    = "Field validation for 'Body' failed";
constexpr int ERROR_CODE_INVALID_BODY         = 64;
constexpr char ERROR_065[]                    = "RonSQL temporary error";
constexpr int ERROR_CODE_RONSQL_TEMPORARY     = 65;
constexpr char ERROR_066[]                    = "RonSQL general error";
constexpr int ERROR_CODE_RONSQL_PERMANENT     = 66;
#endif

#ifdef __cplusplus
}
#endif

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_ERROR_STRS_H_
