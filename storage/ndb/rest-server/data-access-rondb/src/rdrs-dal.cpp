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

#include "src/rdrs-dal.h"
#include <unistd.h>
#include <mgmapi.h>
#include <cstdlib>
#include <cstring>
#include <string>
#include <iostream>
#include <iterator>
#include <sstream>
#include <my_base.h>
#include <NdbApi.hpp>
#include <storage/ndb/include/ndb_global.h>
#include <util/require.h>
#include "src/error-strings.h"
#include "src/logger.hpp"
#include "src/db-operations/pk/pkr-operation.hpp"
#include "src/status.hpp"
#include "src/retry_handler.hpp"
#include "src/ndb_object_pool.hpp"
#include "src/db-operations/pk/common.hpp"

Ndb_cluster_connection *ndb_connection;
Uint32 OP_RETRY_COUNT               = 3;
Uint32 OP_RETRY_INITIAL_DELAY_IN_MS = 500;
Uint32 OP_RETRY_JITTER_IN_MS        = 100;

/**
 * Initialize NDB connection
 * @param connection_string NDB connection string {url}:{port}
 * @param find_available_node_ID if set to 1 then we will first find an available node id to
 * connect to
 * @return status
 */
RS_Status init(const char *connection_string, unsigned int connection_pool_size,
               unsigned int *node_ids, unsigned int node_ids_len, unsigned int connection_retries,
               unsigned int connection_retry_delay_in_sec) {

  // disable buffered stdout
  setbuf(stdout, NULL);

  require(node_ids_len == 1);
  require(connection_pool_size == 1);
  int retCode = 0;
  INFO(std::string("Connecting to ") + connection_string);

  retCode = ndb_init();
  if (retCode != 0) {
    return RS_SERVER_ERROR(ERROR_001 + std::string(" RetCode: ") + std::to_string(retCode));
  }

  ndb_connection = new Ndb_cluster_connection(connection_string, node_ids[0]);
  retCode        = ndb_connection->connect(connection_retries, connection_retry_delay_in_sec, 0);
  if (retCode != 0) {
    return RS_SERVER_ERROR(ERROR_002 + std::string(" RetCode: ") + std::to_string(retCode));
  }

  retCode = ndb_connection->wait_until_ready(30, 0);
  if (retCode != 0) {
    return RS_SERVER_ERROR(
        ERROR_003 + std::string(" RetCode: ") + std::to_string(retCode) +
        std::string(" Lastest Error: ") + std::to_string(ndb_connection->get_latest_error()) +
        std::string(" Lastest Error Msg: ") + std::string(ndb_connection->get_latest_error_msg()));
  }

  // Initialize NDB Object Pool
  NdbObjectPool::InitPool();

  DEBUG("Connected.");
  return RS_OK;
}

RS_Status set_op_retry_props(const unsigned int retry_cont, const unsigned int rety_initial_delay,
                             const unsigned int jitter) {
  OP_RETRY_COUNT               = retry_cont;
  OP_RETRY_INITIAL_DELAY_IN_MS = rety_initial_delay;
  OP_RETRY_JITTER_IN_MS        = jitter;

  return RS_OK;
}

RS_Status shutdown_connection() {
  try {
    // ndb_end(0); // causes seg faults when called repeated from unit tests*/
    NdbObjectPool::GetInstance()->Close();
    delete ndb_connection;
    ndb_connection = nullptr;
  } catch (...) {
    WARN("Exception in Shutdown");
  }
  return RS_OK;
}

/**
 * Closes a NDB Object
 *
 * @param[in] ndb_object
 *
 * @return status
 */
RS_Status closeNDBObject(Ndb *ndb_object) {
  NdbObjectPool::GetInstance()->ReturnResource(ndb_object);
  return RS_OK;
}


RS_Status pk_read(RS_Buffer *reqBuff, RS_Buffer *respBuff) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = NdbObjectPool::GetInstance()->GetNdbObject(ndb_connection, &ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  RETRY_HANDLER(
      PKROperation pkread(reqBuff, respBuff, ndb_object);
      status = pkread.PerformOperation();
  )
  /* clang-format on */

  closeNDBObject(ndb_object);
  return status;
}

/**
 * Batched primary key read operation
 */

RS_Status pk_batch_read(unsigned int no_req, RS_Buffer *req_buffs, RS_Buffer *resp_buffs) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = NdbObjectPool::GetInstance()->GetNdbObject(ndb_connection, &ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  RETRY_HANDLER(
      PKROperation pkread(no_req, req_buffs, resp_buffs, ndb_object);
      status = pkread.PerformOperation();
  )
  /* clang-format on */

  closeNDBObject(ndb_object);
  return status;
}

/**
 * Returns statistis about RonDB connection
 */
RS_Status get_rondb_stats(RonDB_Stats *stats) {
  RonDB_Stats ret              = NdbObjectPool::GetInstance()->GetStats();
  stats->ndb_objects_created   = ret.ndb_objects_created;
  stats->ndb_objects_deleted   = ret.ndb_objects_deleted;
  stats->ndb_objects_count     = ret.ndb_objects_count;
  stats->ndb_objects_available = ret.ndb_objects_available;

  return RS_OK;
}

/**
 * Register callbacks
 */
void register_callbacks(Callbacks cbs) {
  setLogCallBackFns(cbs);
}
