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

#include "src/rdrs_rondb_connection_pool.hpp"
#include "ndb_global.h"
#include "src/error-strings.h"
#include "src/status.hpp"

RDRSRonDBConnectionPool::RDRSRonDBConnectionPool() {
}

RDRSRonDBConnectionPool::~RDRSRonDBConnectionPool() {
  ndb_end(1);  // sometimes causes seg faults when called repeatedly from unit tests
  delete dataConnection;
  delete metadataConnection;
}

RS_Status RDRSRonDBConnectionPool::Init() {
  int retCode = 0;
  retCode     = ndb_init();
  if (retCode != 0) {
    return RS_SERVER_ERROR(ERROR_001 + std::string(" RetCode: ") + std::to_string(retCode));
  }
  return RS_OK;
}

RS_Status RDRSRonDBConnectionPool::AddConnections(const char *connection_string,
                                                  unsigned int connection_pool_size,
                                                  unsigned int *node_ids, unsigned int node_ids_len,
                                                  unsigned int connection_retries,
                                                  unsigned int connection_retry_delay_in_sec) {
  require(connection_pool_size == 1);

  dataConnection   = new RDRSRonDBConnection(connection_string, node_ids, node_ids_len,
                                             connection_retries, connection_retry_delay_in_sec);
  RS_Status status = dataConnection->Connect();
  if (status.http_code != SUCCESS) {
    return status;
  }

  return RS_OK;
}

RS_Status RDRSRonDBConnectionPool::AddMetaConnections(const char *connection_string,
                                                      unsigned int connection_pool_size,
                                                      unsigned int *node_ids,
                                                      unsigned int node_ids_len,
                                                      unsigned int connection_retries,
                                                      unsigned int connection_retry_delay_in_sec) {
  require(connection_pool_size == 1);

  metadataConnection = new RDRSRonDBConnection(connection_string, node_ids, node_ids_len,
                                               connection_retries, connection_retry_delay_in_sec);
  RS_Status status   = metadataConnection->Connect();
  if (status.http_code != SUCCESS) {
    return status;
  }

  return RS_OK;
}

RS_Status RDRSRonDBConnectionPool::GetNdbObject(Ndb **ndb_object) {
  return dataConnection->GetNdbObject(ndb_object);
}

RS_Status RDRSRonDBConnectionPool::ReturnNdbObject(Ndb *ndb_object, RS_Status *status) {
  dataConnection->ReturnNDBObjectToPool(ndb_object, status);
  return RS_OK;
}

RS_Status RDRSRonDBConnectionPool::GetMetadataNdbObject(Ndb **ndb_object) {
  return metadataConnection->GetNdbObject(ndb_object);
}

RS_Status RDRSRonDBConnectionPool::ReturnMetadataNdbObject(Ndb *ndb_object, RS_Status *status) {
  metadataConnection->ReturnNDBObjectToPool(ndb_object, status);
  return RS_OK;
}

RS_Status RDRSRonDBConnectionPool::Reconnect() {

  RS_Status status = dataConnection->Reconnect();
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = metadataConnection->Reconnect();
  if (status.http_code != SUCCESS) {
    return status;
  }
  return RS_OK;
}

RonDB_Stats RDRSRonDBConnectionPool::GetStats() {
  // TODO FIXME Do not merge stats
  RonDB_Stats dataConnectionStats     = dataConnection->GetStats();
  RonDB_Stats metadataConnectionStats = metadataConnection->GetStats();
  RonDB_Stats merged_stats;

  merged_stats.connection_state =
      dataConnectionStats.connection_state > metadataConnectionStats.connection_state
          ? dataConnectionStats.connection_state
          : metadataConnectionStats.connection_state;
  merged_stats.is_reconnection_in_progress = dataConnectionStats.is_reconnection_in_progress |
                                             metadataConnectionStats.is_reconnection_in_progress;
  merged_stats.is_shutdown = dataConnectionStats.is_shutdown | metadataConnectionStats.is_shutdown;
  merged_stats.ndb_objects_available =
      dataConnectionStats.ndb_objects_available + metadataConnectionStats.ndb_objects_available;
  merged_stats.ndb_objects_count =
      dataConnectionStats.ndb_objects_count + metadataConnectionStats.ndb_objects_count;
  merged_stats.ndb_objects_deleted =
      dataConnectionStats.ndb_objects_deleted + metadataConnectionStats.ndb_objects_deleted;
  merged_stats.ndb_objects_created =
      dataConnectionStats.ndb_objects_created + metadataConnectionStats.ndb_objects_created;
  return merged_stats;
}
