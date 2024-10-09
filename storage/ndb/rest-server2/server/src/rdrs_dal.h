/*
 * Copyright (C) 2023, 2024 Hopsworks AB
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

#ifdef __cplusplus
extern "C" {
#endif

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_DAL_H_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_DAL_H_

#include <ndb_types.h>
typedef enum HTTP_CODE {
  SUCCESS      = 200,
  CLIENT_ERROR = 400,
  AUTH_ERROR   = 401,
  NOT_FOUND    = 404,
  SERVER_ERROR = 500
} HTTP_CODE;

// Status
#define RS_STATUS_MSG_LEN       256
#define RS_STATUS_FILE_NAME_LEN 256
typedef struct RS_Status {
  // rest server return code. 200 for successful operation
  HTTP_CODE http_code;
  int status;                       // NdbError.ndberror_status_enum
  int classification;               // NdbError.ndberror_classification_enum
  int code;                         // NdbError.code / ERROR_CODE
  int mysql_code;                   // NdbError.mysql_code
  char message[RS_STATUS_MSG_LEN];  // error message.
  int err_line_no;                  // error line number
  char err_file_name[RS_STATUS_FILE_NAME_LEN];  // error file name.
} RS_Status;

// Log Message
#define RS_LOG_MSG_LEN 256
typedef struct RS_LOG_MSG {
  int level;  // log level
  char message[RS_LOG_MSG_LEN];
} RS_LOG_MSG;

// Data return type. You can change the return type for the column data
// int/floats/decimal are returned as JSON Number type (default),
// varchar/char are returned as strings (default) and varbinary as base64
// (default)
// Right now only default return type is supported
typedef enum DataReturnType {
  DEFAULT_DRT = 1,
  // BASE64 = 2;

  __MAX_TYPE_NOT_A_DRT = 1
} DataReturnType;

// Buffer that contain request or response objects
typedef struct RS_Buffer {
  Uint32 size;  // Buffer size
  char *buffer;       // Buffer
} RS_Buffer;

typedef RS_Buffer *pRS_Buffer;

typedef enum STATE { CONNECTED, CONNECTING, DISCONNECTED } STATE;

// RonDB stats
typedef struct RonDB_Stats {
  Uint32 ndb_objects_created;
  Uint32 ndb_objects_deleted;
  Uint32 ndb_objects_count;
  Uint32 ndb_objects_available;
  STATE connection_state;
  Uint8 is_shutdown;
  Uint8 is_shutting_down;
  Uint8 is_reconnection_in_progress;
} RonDB_Stats;

/**
 * Initialize RonDB Client API
 */
RS_Status init();

/**
 * Connect to RonDB Cluster
 */
RS_Status add_data_connection(const char *connection_string,
                              Uint32 connection_pool_size,
                              Uint32 *node_ids,
                              Uint32 node_ids_len,
                              Uint32 connection_retries,
                              Uint32 connection_retry_delay_in_sec);

/**
 * Connect to RonDB Cluster containing metadata
 */
RS_Status add_metadata_connection(const char *connection_string,
                                  Uint32 connection_pool_size,
                                  Uint32 *node_ids,
                                  Uint32 node_ids_len,
                                  Uint32 connection_retries,
                                  Uint32 connection_retry_delay_in_sec);

/**
 * Set operation retry properties
 */
RS_Status set_data_cluster_op_retry_props(const Uint32 retry_cont,
                                          const Uint32 rety_initial_delay,
                                          const Uint32 jitter);

/**
 * Set operation retry properties for metadata cluster
 */
RS_Status set_metadata_cluster_op_retry_props(const Uint32 retry_cont,
                                              const Uint32 rety_initial_delay,
                                              const Uint32 jitter);

/**
 * Shutdown connection
 */
RS_Status shutdown_connection();

/**
 * Reconnect. Closes the existing connection and then reconnects
 */
RS_Status reconnect();

/**
 * Primary key read operation
 */
RS_Status pk_read(RS_Buffer *reqBuff, RS_Buffer *respBuff);

/**
 * Batched primary key read operation
 */
RS_Status pk_batch_read(Uint32 no_req,
                        RS_Buffer *req_buffs,
                        RS_Buffer *resp_buffs);

/**
 * RonSQL query
 */
struct RonSQLExecParams; /*
                             * struct RonSQLExecParams is defined in
                             * "storage/ndb/src/ronsql/RonSQLCommon.hpp" but we
                             * can't include a .hpp file here.
                             */
RS_Status ronsql_dal(const char* database, struct RonSQLExecParams*);

/**
 * Returns statistis about RonDB connection
 */
RS_Status get_rondb_stats(RonDB_Stats *stats);

/**
 * Call back function for log messages
 */
typedef void (*LogCallBackFn)(RS_LOG_MSG msg);

typedef struct {
  LogCallBackFn logger;
} Callbacks;

/**
 * Register call back function
 */
void register_callbacks(Callbacks cbs);

#endif

#ifdef __cplusplus
}
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_DAL_H_
