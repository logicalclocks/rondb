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

#ifdef __cplusplus
extern "C" {
#endif

#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_DAL_H_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_DAL_H_

typedef enum HTTP_CODE {
  SUCCESS      = 200,
  CLIENT_ERROR = 400,
  NOT_FOUND    = 404,
  SERVER_ERROR = 500
} HTTP_CODE;

// Status
#define RS_STATUS_MSG_LEN       256
#define RS_STATUS_FILE_NAME_LEN 256
typedef struct RS_Status {
  HTTP_CODE http_code;              // rest server return code. 200 for successful operation
  int status;                       // NdbError.ndberror_status_enum
  int classification;               // NdbError.ndberror_classification_enum
  int code;                         // NdbError.code
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
// varchar/char are returned as strings (default) and varbinary as base64 (default)
// Right now only default return type is supported
typedef enum DataReturnType {
  DEFAULT_DRT = 1,
  // BASE64 = 2;

  __MAX_TYPE_NOT_A_DRT = 1
} DataReturnType;

// Buffer that contain request or response objects
typedef struct RS_Buffer {
  unsigned int size;  // Buffer size
  char *buffer;       // Buffer
} RS_Buffer;

typedef RS_Buffer *pRS_Buffer;

typedef enum STATE { CONNECTED, CONNECTING, DISCONNECTED } STATE;

// RonDB stats
typedef struct RonDB_Stats {
  unsigned int ndb_objects_created;
  unsigned int ndb_objects_deleted;
  unsigned int ndb_objects_count;
  unsigned int ndb_objects_available;
  STATE connection_state;
  unsigned char is_shutdown;
  unsigned char is_reconnection_in_progress;
} RonDB_Stats;

/**
 * Initialize RonDB Client API
 */
RS_Status init();

/**
 * Connect to RonDB Cluster
 */
RS_Status add_data_connection(const char *connection_string, unsigned int connection_pool_size,
                              unsigned int *node_ids, unsigned int node_ids_len,
                              unsigned int connection_retries,
                              unsigned int connection_retry_delay_in_sec);

/**
 * Connect to RonDB Cluster containing metadata
 */
RS_Status add_metadata_connection(const char *connection_string, unsigned int connection_pool_size,
                                  unsigned int *node_ids, unsigned int node_ids_len,
                                  unsigned int connection_retries,
                                  unsigned int connection_retry_delay_in_sec);

/**
 * Set operation retry properties
 */
RS_Status set_op_retry_props_data_cluster(const unsigned int retry_cont,
                                          const unsigned int rety_initial_delay,
                                          const unsigned int jitter);

/**
 * Set operation retry properties for metadata cluster
 */
RS_Status set_op_retry_props_metadata_cluster(const unsigned int retry_cont,
                                              const unsigned int rety_initial_delay,
                                              const unsigned int jitter);

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
RS_Status pk_batch_read(unsigned int no_req, RS_Buffer *req_buffs, RS_Buffer *resp_buffs);

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
#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_DAL_H_
