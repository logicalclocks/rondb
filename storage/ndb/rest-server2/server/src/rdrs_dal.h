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

#ifdef __cplusplus
extern "C" {
#endif

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_DAL_H_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_DAL_H_

#include <stdbool.h>

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
  char *buffer;       // Buffer
  unsigned int size;  // Buffer size
  unsigned int next_allocated_buffer;
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
  unsigned char is_shutting_down;
  unsigned char is_reconnection_in_progress;
} RonDB_Stats;

/**
 * Initialize RonDB Client API
 */
RS_Status init(unsigned numThreads, unsigned int num_data_connections);

/**
 * Connect to RonDB Cluster
 */
RS_Status add_data_connection(const char *connection_string,
                              unsigned int connection_pool_size,
                              unsigned int *node_ids,
                              unsigned int node_ids_len,
                              unsigned int connection_retries,
                              unsigned int connection_retry_delay_in_sec);

/**
 * Connect to RonDB Cluster containing metadata
 */
RS_Status add_metadata_connection(const char *connection_string,
                                  unsigned int connection_pool_size,
                                  unsigned int *node_ids,
                                  unsigned int node_ids_len,
                                  unsigned int connection_retries,
                                  unsigned int connection_retry_delay_in_sec);

/**
 * Set operation retry properties
 */
RS_Status set_data_cluster_op_retry_props(const unsigned int retry_cont,
                                          const unsigned int rety_initial_delay,
                                          const unsigned int jitter);

/**
 * Set operation retry properties for metadata cluster
 */
RS_Status set_metadata_cluster_op_retry_props(
  const unsigned int retry_cont,
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
 * Batched primary key read operation
 * Also used for single key read operation
 *
 * The process to handle REST API calls are the following:
 * 1. Receive the request in Drogon, Drogon calls callback
 *
 * 2. Parse JSON request with references into the JSON buffer
 *   - This requires no copying except handling the extra padding
 *   - the JSON parser requires.
 * This step is different for each endpoint since the request protocol
 * differs for the endpoints.
 *
 * 3. Create NDB backend request from parsed JSON request.
 *   - This copies information about databases, tables and columns
 *   - and primary key values into a request buffer.
 * The request buffer created by this is input to the below function.
 *
 * 4. The request is turned into NDB API request where the return
 *   data is copied into an NdbRecord allocated by ArenaMalloc.
 *
 * 5. The actual request to RonDB data nodes is executed.
 *   - Data is copied from receive messages into the NdbRecord row.
 *
 * 6. The JSON response requires changing the data to fit a text-based
 *   protocol. This means that the response is prepared in the response
 *   buffer.
 *   - This step involves copying, but also conversion to Base64-encoding
 *     and conversion from number to strings and other conversions.
 *
 * 7. The final step is to convert the response into a proper JSON
 *  response according to the protocol of the endpoint. This step is
 *  different for all of our endpoints since each endpoint defines its
 *  own protocol.
 *    - This step involves one more step of copying to ensure we add
 *    - JSON {, }, CR, "," and quotation marks according to the protocol.
 *
 * We will also need to track if messages become so long that they require
 * a streaming protocol.
 */
RS_Status pk_batch_read(void *amalloc,
                        unsigned int no_req,
                        bool is_batch,
                        RS_Buffer *req_buffs,
                        RS_Buffer *resp_buffs,
                        unsigned int threadIndex);

/**
 * RonSQL query
 */
struct RonSQLExecParams; /*
                             * struct RonSQLExecParams is defined in
                             * "storage/ndb/src/ronsql/RonSQLCommon.hpp" but we
                             * can't include a .hpp file here.
                             */
RS_Status ronsql_dal(const char* database,
                     struct RonSQLExecParams*,
                     unsigned int threadIndex);

/**
 * Returns statistis about RonDB connection
 */
RS_Status get_rondb_stats(RonDB_Stats *stats);

void* get_rdrs_ndb_object(int thread_index);
void return_rdrs_ndb_object(void *ndb_object, int thread_index);

#endif

#ifdef __cplusplus
}
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_DAL_H_
