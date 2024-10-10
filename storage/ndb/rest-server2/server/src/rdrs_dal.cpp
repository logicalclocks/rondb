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

#include "rdrs_dal.h"
#include "db_operations/pk/common.hpp"
#include "db_operations/pk/pkr_operation.hpp"
#include "db_operations/ronsql/ronsql_operation.hpp"
#include "error_strings.h"
#include "logger.hpp"
#include "rdrs_dal.hpp"
#include "rdrs_rondb_connection_pool.hpp"
#include "retry_handler.hpp"
#include "status.hpp"

#include <storage/ndb/include/ndb_global.h>
#include <util/require.h>
#include <mgmapi.h>
#include <my_base.h>
#include <unistd.h>
#include <NdbApi.hpp>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <iterator>
#include <sstream>
#include <memory>

#include "storage/ndb/src/ronsql/RonSQLCommon.hpp"


RDRSRonDBConnectionPool *rdrsRonDBConnectionPool = nullptr;

RS_Status init(unsigned int numThreads) {
  // disable buffered stdout
  setbuf(stdout, NULL);

  // Initialize NDB Connection and Object Pool
  rdrsRonDBConnectionPool = new RDRSRonDBConnectionPool();
  RS_Status status = rdrsRonDBConnectionPool->Init(numThreads);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  return RS_OK;
}

RS_Status add_data_connection(const char *connection_string,
                              unsigned int connection_pool_size,
                              unsigned int *node_ids,
                              unsigned int node_ids_len,
                              unsigned int connection_retries,
                              unsigned int connection_retry_delay_in_sec) {

  RS_Status status = rdrsRonDBConnectionPool->AddConnections(
    connection_string,
    connection_pool_size,
    node_ids,
    node_ids_len,
    connection_retries,
    connection_retry_delay_in_sec);

  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  return RS_OK;
}

RS_Status add_metadata_connection(const char *connection_string,
                                  unsigned int connection_pool_size,
                                  unsigned int *node_ids,
                                  unsigned int node_ids_len,
                                  unsigned int connection_retries,
                                  unsigned int connection_retry_delay_in_sec) {

  RS_Status status = rdrsRonDBConnectionPool->AddMetaConnections(
    connection_string,
    connection_pool_size,
    node_ids,
    node_ids_len,
    connection_retries,
    connection_retry_delay_in_sec);

  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  return RS_OK;
}

RS_Status set_data_cluster_op_retry_props(
  const unsigned int retry_cont,
  const unsigned int rety_initial_delay,
  const unsigned int jitter) {
  DATA_CONN_OP_RETRY_COUNT = retry_cont;
  DATA_CONN_OP_RETRY_INITIAL_DELAY_IN_MS = rety_initial_delay;
  DATA_CONN_OP_RETRY_JITTER_IN_MS = jitter;
  return RS_OK;
}

RS_Status set_metadata_cluster_op_retry_props(
  const unsigned int retry_cont,
  const unsigned int rety_initial_delay,
  const unsigned int jitter) {
  METADATA_CONN_OP_RETRY_COUNT = retry_cont;
  METADATA_CONN_OP_RETRY_INITIAL_DELAY_IN_MS = rety_initial_delay;
  METADATA_CONN_OP_RETRY_JITTER_IN_MS = jitter;
  return RS_OK;
}

RS_Status shutdown_connection() {
  delete rdrsRonDBConnectionPool;
  return RS_OK;
}

RS_Status reconnect() {
  return rdrsRonDBConnectionPool->Reconnect();
}

RS_Status pk_read(RS_Buffer *reqBuff,
                  RS_Buffer *respBuff,
                  unsigned int threadIndex) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetNdbObject(&ndb_object,
                                                           threadIndex);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  DATA_OP_RETRY_HANDLER(
      PKROperation pkread(reqBuff, respBuff, ndb_object);
      status = pkread.PerformOperation();
  )
  rdrsRonDBConnectionPool->ReturnNdbObject(ndb_object,
                                           &status,
                                           threadIndex);
  return status;
}

RS_Status pk_batch_read(unsigned int no_req,
                        RS_Buffer *req_buffs,
                        RS_Buffer *resp_buffs,
                        unsigned int threadIndex) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetNdbObject(&ndb_object,
                                                           threadIndex);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  DATA_OP_RETRY_HANDLER(
      PKROperation pkread(no_req, req_buffs, resp_buffs, ndb_object);
      status = pkread.PerformOperation();
  )
  rdrsRonDBConnectionPool->ReturnNdbObject(ndb_object,
                                           &status,
                                           threadIndex);
  return status;
}

RS_Status ronsql_dal(const char* database,
                     RonSQLExecParams* ep,
                     unsigned int threadIndex) {
  assert(ep != nullptr);
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetNdbObject(&ndb_object,
                                                           threadIndex);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }

  assert(ep->ndb == NULL);
  assert(ndb_object != NULL);
  ep->ndb = ndb_object;
  const char* saved_database_name = ndb_object->getDatabaseName();
  ndb_object->setDatabaseName(database);
  status = ronsql_op(*ep);
  ndb_object->setDatabaseName(saved_database_name);
  ep->ndb = NULL;
  rdrsRonDBConnectionPool->ReturnNdbObject(ndb_object,
                                           &status,
                                           threadIndex);
  return status;
}

/**
 * Returns statistis about RonDB connection
 */
RS_Status get_rondb_stats(RonDB_Stats *stats) {
  RonDB_Stats ret = rdrsRonDBConnectionPool->GetStats();
  stats->ndb_objects_created = ret.ndb_objects_created;
  stats->ndb_objects_deleted = ret.ndb_objects_deleted;
  stats->ndb_objects_count = ret.ndb_objects_count;
  stats->ndb_objects_available = ret.ndb_objects_available;
  stats->connection_state = ret.connection_state;
  return RS_OK;
}

/**
 * Register callbacks
 */
void register_callbacks(Callbacks cbs) {
  RDRSLogger::setLogCallBackFns(cbs);
}

CRS_Status CRS_Status::SUCCESS = CRS_Status(HTTP_CODE::SUCCESS);
