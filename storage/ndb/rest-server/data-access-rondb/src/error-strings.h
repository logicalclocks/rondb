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

#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_ERROR_STRS_H_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_ERROR_STRS_H_

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
#define ERROR_022 "Failed to set lock level."
#define ERROR_023 "Failed to set NdbOperation::equal()."
#define ERROR_024 "Failed to find free API node slot."
#define ERROR_025 "Data return type is not supported."
#define ERROR_026 "Reading BLOB/TEXT column is not supported yet."
#define ERROR_027 "Invalid Date/Time."
#define ERROR_028 "Programming error. Please report bug."
#define ERROR_029 "Failed to start scan operation."
#define ERROR_030 "Failed to set lock mode."
#define ERROR_031 "Failed to set filter."
#define ERROR_032 "Failed to load index."
#define ERROR_033 "RonDB connection is not open."
#define ERROR_034 "Programming error. RonDB connection has been shutdown. Use Init() fn."
#define ERROR_035 "RonDB connection and object pool is not initialized."
#define ERROR_036 "RonDB reconnection already in progress."

#ifdef __cplusplus
}
#endif

#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_ERROR_STRS_H_
