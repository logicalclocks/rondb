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
#include "src/error-strings.h"
#include "src/logger.hpp"
#include "src/db-operations/pk/pkr-operation.hpp"
#include "src/status.hpp"
#include "src/retry_handler.hpp"
#include "src/rdrs_rondb_connection.hpp"
#include "src/db-operations/pk/common.hpp"

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

  INFO(std::string("Connecting to ") + connection_string);

  // Initialize NDB Connection and Object Pool
  return RDRSRonDBConnection::InitPool(connection_string, connection_pool_size, node_ids, node_ids_len,
                                 connection_retries, connection_retry_delay_in_sec);
}

//--------------------------------------------------------------------------------------------------

RS_Status set_op_retry_props(const unsigned int retry_cont, const unsigned int rety_initial_delay,
                             const unsigned int jitter) {
  OP_RETRY_COUNT               = retry_cont;
  OP_RETRY_INITIAL_DELAY_IN_MS = rety_initial_delay;
  OP_RETRY_JITTER_IN_MS        = jitter;

  return RS_OK;
}

//--------------------------------------------------------------------------------------------------

RS_Status shutdown_connection() {
  return RDRSRonDBConnection::GetInstance()->Shutdown();
}

//--------------------------------------------------------------------------------------------------

RS_Status reconnect() {
  return RDRSRonDBConnection::GetInstance()->Reconnect();
}

//--------------------------------------------------------------------------------------------------

RS_Status pk_read(RS_Buffer *reqBuff, RS_Buffer *respBuff) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = RDRSRonDBConnection::GetInstance()->GetNdbObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  RETRY_HANDLER(
      PKROperation pkread(reqBuff, respBuff, ndb_object);
      status = pkread.PerformOperation();
  )
  /* clang-format on */

  RDRSRonDBConnection::GetInstance()->ReturnNDBObjectToPool(ndb_object, &status);
  return status;
}

//--------------------------------------------------------------------------------------------------

/**
 * Batched primary key read operation
 */

RS_Status pk_batch_read(unsigned int no_req, RS_Buffer *req_buffs, RS_Buffer *resp_buffs) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = RDRSRonDBConnection::GetInstance()->GetNdbObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  RETRY_HANDLER(
      PKROperation pkread(no_req, req_buffs, resp_buffs, ndb_object);
      status = pkread.PerformOperation();
  )
  /* clang-format on */

  RDRSRonDBConnection::GetInstance()->ReturnNDBObjectToPool(ndb_object, &status);
  return status;
}

//--------------------------------------------------------------------------------------------------

/**
 * Returns statistis about RonDB connection
 */
RS_Status get_rondb_stats(RonDB_Stats *stats) {
  RonDB_Stats ret              = RDRSRonDBConnection::GetInstance()->GetStats();
  stats->ndb_objects_created   = ret.ndb_objects_created;
  stats->ndb_objects_deleted   = ret.ndb_objects_deleted;
  stats->ndb_objects_count     = ret.ndb_objects_count;
  stats->ndb_objects_available = ret.ndb_objects_available;

  return RS_OK;
}

//--------------------------------------------------------------------------------------------------

/**
 * Register callbacks
 */
void register_callbacks(Callbacks cbs) {
  setLogCallBackFns(cbs);
}
