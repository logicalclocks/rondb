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

#include "rdrs_dal.h"
#include "db_operations/pk/common.hpp"
#include "db_operations/pk/pkr_operation.hpp"
#include "error_strings.h"
#include "logger.hpp"
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

RDRSRonDBConnectionPool *rdrsRonDBConnectionPool = nullptr;

//--------------------------------------------------------------------------------------------------

RS_Status init() {
  // disable buffered stdout
  setbuf(stdout, NULL);

  // Initialize NDB Connection and Object Pool
  rdrsRonDBConnectionPool = new RDRSRonDBConnectionPool();
  RS_Status status        = rdrsRonDBConnectionPool->Init();
  if (status.http_code != SUCCESS) {
    return status;
  }

  return RS_OK;
}

//--------------------------------------------------------------------------------------------------

RS_Status add_data_connection(const char *connection_string, unsigned int connection_pool_size,
                              unsigned int *node_ids, unsigned int node_ids_len,
                              unsigned int connection_retries,
                              unsigned int connection_retry_delay_in_sec) {

  RS_Status status = rdrsRonDBConnectionPool->AddConnections(
      connection_string, connection_pool_size, node_ids, node_ids_len, connection_retries,
      connection_retry_delay_in_sec);

  if (status.http_code != SUCCESS) {
    return status;
  }

  return RS_OK;
}

//--------------------------------------------------------------------------------------------------

RS_Status add_metadata_connection(const char *connection_string, unsigned int connection_pool_size,
                                  unsigned int *node_ids, unsigned int node_ids_len,
                                  unsigned int connection_retries,
                                  unsigned int connection_retry_delay_in_sec) {

  RS_Status status = rdrsRonDBConnectionPool->AddMetaConnections(
      connection_string, connection_pool_size, node_ids, node_ids_len, connection_retries,
      connection_retry_delay_in_sec);
  if (status.http_code != SUCCESS) {
    return status;
  }

  return RS_OK;
}

//--------------------------------------------------------------------------------------------------

RS_Status set_data_cluster_op_retry_props(const unsigned int retry_cont,
                                          const unsigned int rety_initial_delay,
                                          const unsigned int jitter) {
  DATA_CONN_OP_RETRY_COUNT               = retry_cont;
  DATA_CONN_OP_RETRY_INITIAL_DELAY_IN_MS = rety_initial_delay;
  DATA_CONN_OP_RETRY_JITTER_IN_MS        = jitter;

  return RS_OK;
}

//--------------------------------------------------------------------------------------------------

RS_Status set_metadata_cluster_op_retry_props(const unsigned int retry_cont,
                                              const unsigned int rety_initial_delay,
                                              const unsigned int jitter) {
  METADATA_CONN_OP_RETRY_COUNT               = retry_cont;
  METADATA_CONN_OP_RETRY_INITIAL_DELAY_IN_MS = rety_initial_delay;
  METADATA_CONN_OP_RETRY_JITTER_IN_MS        = jitter;

  return RS_OK;
}

//--------------------------------------------------------------------------------------------------

RS_Status shutdown_connection() {
  delete rdrsRonDBConnectionPool;
  return RS_OK;
}

//--------------------------------------------------------------------------------------------------

RS_Status reconnect() {
  return rdrsRonDBConnectionPool->Reconnect();
}

//--------------------------------------------------------------------------------------------------

RS_Status pk_read(RS_Buffer *reqBuff, RS_Buffer *respBuff) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetNdbObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  DATA_OP_RETRY_HANDLER(
      PKROperation pkread(reqBuff, respBuff, ndb_object);
      status = pkread.PerformOperation();
  )
  /* clang-format on */

  rdrsRonDBConnectionPool->ReturnNdbObject(ndb_object, &status);
  return status;
}

//--------------------------------------------------------------------------------------------------

RS_Status pk_batch_read(unsigned int no_req, RS_Buffer *req_buffs, RS_Buffer *resp_buffs) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetNdbObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  DATA_OP_RETRY_HANDLER(
      PKROperation pkread(no_req, req_buffs, resp_buffs, ndb_object);
      status = pkread.PerformOperation();
  )
  /* clang-format on */

  rdrsRonDBConnectionPool->ReturnNdbObject(ndb_object, &status);
  return status;
}

//--------------------------------------------------------------------------------------------------

/**
 * Returns statistis about RonDB connection
 */
RS_Status get_rondb_stats(RonDB_Stats *stats) {
  RonDB_Stats ret              = rdrsRonDBConnectionPool->GetStats();
  stats->ndb_objects_created   = ret.ndb_objects_created;
  stats->ndb_objects_deleted   = ret.ndb_objects_deleted;
  stats->ndb_objects_count     = ret.ndb_objects_count;
  stats->ndb_objects_available = ret.ndb_objects_available;
  stats->connection_state      = ret.connection_state;
  return RS_OK;
}

//--------------------------------------------------------------------------------------------------

/**
 * Register callbacks
 */
void register_callbacks(Callbacks cbs) {
  RDRSLogger::setLogCallBackFns(cbs);
}

RS_Status::RS_Status(HTTP_CODE http_code, const char *message) : http_code(http_code) {
  strncpy(this->message, message, RS_STATUS_MSG_LEN - 1);
  this->message[RS_STATUS_MSG_LEN - 1] = '\0';
}

RS_Status::RS_Status(HTTP_CODE http_code, int error_code, const char *message)
    : http_code(http_code), code(error_code) {
  strncpy(this->message, message, RS_STATUS_MSG_LEN - 1);
  this->message[RS_STATUS_MSG_LEN - 1] = '\0';
}

RS_Status::RS_Status(HTTP_CODE http_code, const char *error, const char *location)
    : http_code(http_code) {
  std::string msg =
      "Parsing request failed. Error: " + std::string(error) + " at " + std::string(location);
  strncpy(this->message, msg.c_str(), RS_STATUS_MSG_LEN - 1);
  this->message[RS_STATUS_MSG_LEN - 1] = '\0';
}

void RS_Status::set(HTTP_CODE http_code, const char *message) {
  this->http_code = http_code;
  strncpy(this->message, message, RS_STATUS_MSG_LEN - 1);
  this->message[RS_STATUS_MSG_LEN - 1] = '\0';
}

RS_Buffer::RS_Buffer(const RS_Buffer &other) : size(other.size), buffer(new char[other.size]) {
  std::copy(other.buffer, other.buffer + other.size, buffer);
}

RS_Buffer &RS_Buffer::operator=(const RS_Buffer &other) {
  if (this != &other) {
    delete[] buffer;
    size   = other.size;
    buffer = new char[other.size];
    std::copy(other.buffer, other.buffer + other.size, buffer);
  }
  return *this;
}

RS_Buffer::RS_Buffer(RS_Buffer &&other) noexcept : size(other.size), buffer(other.buffer) {
  other.size   = 0;
  other.buffer = nullptr;
}

RS_Buffer &RS_Buffer::operator=(RS_Buffer &&other) noexcept {
  if (this != &other) {
    delete[] buffer;

    size         = other.size;
    buffer       = other.buffer;
    other.size   = 0;
    other.buffer = nullptr;
  }
  return *this;
}