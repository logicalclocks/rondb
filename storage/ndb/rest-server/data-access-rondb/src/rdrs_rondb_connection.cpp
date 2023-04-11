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

#include "src/rdrs_rondb_connection.hpp"
#include <iostream>
#include <string>
#include <storage/ndb/include/ndb_global.h>
#include "src/status.hpp"
#include "src/error-strings.h"
#include "src/logger.hpp"

RDRSRonDBConnection *RDRSRonDBConnection::__instance = nullptr;

RS_Status RDRSRonDBConnection::InitPool(const char *connection_string, unsigned int connection_pool_size,
               unsigned int *node_ids, unsigned int node_ids_len, unsigned int connection_retries,
               unsigned int connection_retry_delay_in_sec) {

  require(node_ids_len == 1);
  require(connection_pool_size == 1);

  __instance                              = new RDRSRonDBConnection();
  __instance->stats.ndb_objects_available = 0;
  __instance->stats.ndb_objects_count     = 0;
  __instance->stats.ndb_objects_created   = 0;
  __instance->stats.ndb_objects_deleted   = 0;

  int retCode = 0;
  retCode = ndb_init();
  if (retCode != 0) {
    return RS_SERVER_ERROR(ERROR_001 + std::string(" RetCode: ") + std::to_string(retCode));
  }

  __instance->ndb_connection = new Ndb_cluster_connection(connection_string, node_ids[0]);
  retCode        = __instance->ndb_connection->connect(connection_retries, connection_retry_delay_in_sec, 0);
  if (retCode != 0) {
    return RS_SERVER_ERROR(ERROR_002 + std::string(" RetCode: ") + std::to_string(retCode));
  }

  retCode = __instance->ndb_connection->wait_until_ready(30, 0);
  if (retCode != 0) {
    return RS_SERVER_ERROR(
        ERROR_003 + std::string(" RetCode: ") + std::to_string(retCode) +
        std::string(" Lastest Error: ") + std::to_string(__instance->ndb_connection->get_latest_error()) +
        std::string(" Lastest Error Msg: ") + std::string(__instance->ndb_connection->get_latest_error_msg()));
  }

  return RS_OK;
}

RDRSRonDBConnection *RDRSRonDBConnection::GetInstance() {
  if (__instance == nullptr) {
    ERROR("NDB object pool is not initialized");
  }

  return __instance;
}

RS_Status RDRSRonDBConnection::GetNdbObject(Ndb **ndb_object) {
  if (ndb_connection == nullptr) {
    return RS_SERVER_ERROR("Failed to get NDB Object. Cluster connection is closed");
  }

  std::lock_guard<std::mutex> guard(__mutex);
  RS_Status ret_status = RS_OK;
  if (__ndb_objects.empty()) {
    *ndb_object = new Ndb(ndb_connection);
    int retCode = (*ndb_object)->init();
    if (retCode != 0) {
      delete ndb_object;
      ret_status = RS_SERVER_ERROR(ERROR_004 + std::string(" RetCode: ") + std::to_string(retCode));
    }
    __atomic_fetch_add(&stats.ndb_objects_created, 1, __ATOMIC_SEQ_CST);
    __atomic_fetch_add(&stats.ndb_objects_count, 1, __ATOMIC_SEQ_CST);
  } else {
    *ndb_object = __ndb_objects.front();
    __ndb_objects.pop_front();
  }
  return ret_status;
}

void RDRSRonDBConnection::ReturnNDBObjectToPool(Ndb *object) {
  std::lock_guard<std::mutex> guard(__mutex);
  // reset transaction and cleanup
  __ndb_objects.push_back(object);
}

RonDB_Stats RDRSRonDBConnection::GetStats() {
  std::lock_guard<std::mutex> guard(__mutex);

  stats.ndb_objects_available = __ndb_objects.size();

  return stats;
}

RS_Status RDRSRonDBConnection::Shutdown() {
  std::lock_guard<std::mutex> guard(__mutex);

  // delete all ndb objects
  while (__ndb_objects.size() > 0) {
    Ndb *ndb_object = __ndb_objects.front();
    __ndb_objects.pop_front();
    delete ndb_object;
  }

  // clean up stats
  stats.ndb_objects_available = 0;
  stats.ndb_objects_count     = 0;
  stats.ndb_objects_created   = 0;
  stats.ndb_objects_deleted   = 0;
  
  // delete connection
  try {
    //ndb_end(0); // causes seg faults when called repeatedly from unit tests*/
    delete __instance->ndb_connection;
    __instance->ndb_connection = nullptr;
  } catch (...) {
    WARN("Exception in Shutdown");
  }

  return RS_OK;
}
