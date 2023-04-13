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
#include <cstddef>
#include <iostream>
#include <string>
#include <storage/ndb/include/ndb_global.h>
#include "src/status.hpp"
#include "src/error-strings.h"
#include "src/logger.hpp"

RDRSRonDBConnection *RDRSRonDBConnection::__instance = nullptr;

//--------------------------------------------------------------------------------------------------

RS_Status RDRSRonDBConnection::Init(const char *connection_string, unsigned int connection_pool_size,
               unsigned int *node_ids, unsigned int node_ids_len, unsigned int connection_retries,
               unsigned int connection_retry_delay_in_sec) {

  require(node_ids_len == 1);
  require(connection_pool_size == 1);
  require(__instance == nullptr);

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

  __instance->ndbConnection = new Ndb_cluster_connection(connection_string, node_ids[0]);
  retCode        = __instance->ndbConnection->connect(connection_retries, connection_retry_delay_in_sec, 0);
  if (retCode != 0) {
    return RS_SERVER_ERROR(ERROR_002 + std::string(" RetCode: ") + std::to_string(retCode));
  }

  retCode = __instance->ndbConnection->wait_until_ready(30, 0);
  if (retCode != 0) {
    return RS_SERVER_ERROR(
        ERROR_003 + std::string(" RetCode: ") + std::to_string(retCode) +
        std::string(" Lastest Error: ") + std::to_string(__instance->ndbConnection->get_latest_error()) +
        std::string(" Lastest Error Msg: ") + std::string(__instance->ndbConnection->get_latest_error_msg()));
  }

  __instance->connectionState = CONNECTED;

  return RS_OK;
}

//--------------------------------------------------------------------------------------------------

RS_Status RDRSRonDBConnection::GetInstance(RDRSRonDBConnection **rdrsRonDBConnection) {
  if (__instance == nullptr) {
    LOG_ERROR(ERROR_035);
    return RS_SERVER_ERROR(ERROR_035);
  }

  if (__instance->ndbConnection == nullptr || __instance->connectionState != CONNECTED) {
    LOG_ERROR(ERROR_033);
    return RS_SERVER_ERROR(ERROR_033);
  }

  *rdrsRonDBConnection = __instance;
  return RS_OK;
}

//--------------------------------------------------------------------------------------------------

RS_Status RDRSRonDBConnection::GetNdbObject(Ndb **ndb_object) {

  if (__instance->ndbConnection == nullptr || __instance->connectionState != CONNECTED) {
    LOG_ERROR(ERROR_033);
    return RS_SERVER_ERROR(ERROR_033);
  }

  std::lock_guard<std::mutex> guard(__mutex);
  RS_Status ret_status = RS_OK;
  if (__ndbObjects.empty()) {
    *ndb_object = new Ndb(ndbConnection);
    int retCode = (*ndb_object)->init();
    if (retCode != 0) {
      delete ndb_object;
      ret_status = RS_SERVER_ERROR(ERROR_004 + std::string(" RetCode: ") + std::to_string(retCode));
    }
    __atomic_fetch_add(&stats.ndb_objects_created, 1, __ATOMIC_SEQ_CST);
    __atomic_fetch_add(&stats.ndb_objects_count, 1, __ATOMIC_SEQ_CST);
  } else {
    *ndb_object = __ndbObjects.front();
    __ndbObjects.pop_front();
  }
  return ret_status;
}

//--------------------------------------------------------------------------------------------------

void RDRSRonDBConnection::ReturnNDBObjectToPool(Ndb *object, RS_Status *status) {
  std::lock_guard<std::mutex> guard(__mutex);
  __ndbObjects.push_back(object);

  // check for errors
  if ( status != nullptr  && status->http_code != SUCCESS) {
    printf("----> returning ndbobject errors occured \n");
  }
}

//--------------------------------------------------------------------------------------------------

RonDB_Stats RDRSRonDBConnection::GetStats() {
  std::lock_guard<std::mutex> guard(__mutex);

  stats.ndb_objects_available = __ndbObjects.size();

  return stats;
}

//--------------------------------------------------------------------------------------------------

RS_Status RDRSRonDBConnection::Shutdown() {
  std::lock_guard<std::mutex> guard(__mutex);

  // delete all ndb objects
  while (__ndbObjects.size() > 0) {
    Ndb *ndb_object = __ndbObjects.front();
    __ndbObjects.pop_front();
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
    delete __instance->ndbConnection;
    __instance->ndbConnection = nullptr;
  } catch (...) {
    LOG_WARN("Exception in Shutdown");
  }

  return RS_OK;
}

//--------------------------------------------------------------------------------------------------

RS_Status RDRSRonDBConnection::Reconnect() {
  connectionState = DISCONNECTED;

  return RS_OK;
}

//--------------------------------------------------------------------------------------------------
