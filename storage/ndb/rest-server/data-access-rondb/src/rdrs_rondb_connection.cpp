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
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <NdbThread.h>
#include <storage/ndb/include/ndb_global.h>
#include "src/status.hpp"
#include "src/error-strings.h"
#include "src/logger.hpp"

RDRSRonDBConnection *RDRSRonDBConnection::__instance = nullptr;

//--------------------------------------------------------------------------------------------------

RS_Status RDRSRonDBConnection::Init(const char *connection_string, Uint32 connection_pool_size,
                                    Uint32 *node_ids, Uint32 node_ids_len,
                                    Uint32 connection_retries,
                                    Uint32 connection_retry_delay_in_sec) {

  require(node_ids_len == 1);
  require(connection_pool_size == 1);
  require(__instance == nullptr);

  __instance = new RDRSRonDBConnection();

  __instance->stats.ndb_objects_available = 0;
  __instance->stats.ndb_objects_count     = 0;
  __instance->stats.ndb_objects_created   = 0;
  __instance->stats.ndb_objects_deleted   = 0;
  __instance->reconnectionInProgress      = false;

  __instance->connection_string = reinterpret_cast<char *>(malloc(strlen(connection_string)));
  std::strncpy(__instance->connection_string, connection_string, strlen(connection_string));
  __instance->connection_pool_size = connection_pool_size;

  __instance->node_ids = reinterpret_cast<Uint32 *>(malloc(node_ids_len * sizeof(Uint32)));
  memcpy(__instance->node_ids, node_ids, node_ids_len * sizeof(Uint32));
  __instance->node_ids_len = node_ids_len;

  __instance->connection_retries            = connection_retries;
  __instance->connection_retry_delay_in_sec = connection_retry_delay_in_sec;

  __instance->connectionState = DISCONNECTED;

  int retCode = 0;
  retCode     = ndb_init();
  if (retCode != 0) {
    return RS_SERVER_ERROR(ERROR_001 + std::string(" RetCode: ") + std::to_string(retCode));
  }

  return __instance->Connect();
}

//--------------------------------------------------------------------------------------------------

RS_Status RDRSRonDBConnection::Connect() {

  require(connectionState != CONNECTED);
  require(ndbConnection == nullptr);

  int retCode   = 0;
  ndbConnection = new Ndb_cluster_connection(connection_string, node_ids[0]);
  retCode       = ndbConnection->connect(connection_retries, connection_retry_delay_in_sec, 0);
  if (retCode != 0) {
    return RS_SERVER_ERROR(ERROR_002 + std::string(" RetCode: ") + std::to_string(retCode));
  }

  retCode = ndbConnection->wait_until_ready(30, 0);
  if (retCode != 0) {
    return RS_SERVER_ERROR(ERROR_003 + std::string(" RetCode: ") + std::to_string(retCode) +
                           std::string(" Lastest Error: ") +
                           std::to_string(__instance->ndbConnection->get_latest_error()) +
                           std::string(" Lastest Error Msg: ") +
                           std::string(__instance->ndbConnection->get_latest_error_msg()));
  }

  connectionState = CONNECTED;

  LOG_INFO("RonDB connection and object pool initialized");
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

    // If previous reconnection attempts have failed then 
    // restart the reconnection process
    std::lock_guard<std::mutex> guard(__mutex);
    if (!reconnectionInProgress) {
      ReconnectInt(true);
    }

    LOG_ERROR(ERROR_033);
    return RS_SERVER_ERROR(ERROR_033);
  }

  std::lock_guard<std::mutex> guard(__mutex);
  RS_Status ret_status = RS_OK;
  if (availableNdbObjects.empty()) {
    *ndb_object = new Ndb(ndbConnection);
    int retCode = (*ndb_object)->init();
    if (retCode != 0) {
      delete ndb_object;
      ret_status = RS_SERVER_ERROR(ERROR_004 + std::string(" RetCode: ") + std::to_string(retCode));
    }
    __atomic_fetch_add(&stats.ndb_objects_created, 1, __ATOMIC_SEQ_CST);
    __atomic_fetch_add(&stats.ndb_objects_count, 1, __ATOMIC_SEQ_CST);
    allAvailableNdbObjects.push_back(*ndb_object);
  } else {
    *ndb_object = availableNdbObjects.front();
    availableNdbObjects.pop_front();
  }
  return ret_status;
}

//--------------------------------------------------------------------------------------------------

void RDRSRonDBConnection::ReturnNDBObjectToPool(Ndb *ndb_object, RS_Status *status) {
  std::lock_guard<std::mutex> guard(__mutex);
  availableNdbObjects.push_back(ndb_object);

  // check for errors
  if (status != nullptr && status->http_code != SUCCESS) {
    // Classification.UnknownResultError is the classification
    // for loss of connectivity to the cluster
    if (status->classification == NdbError::UnknownResultError) {
      LOG_ERROR("Detected connection loss. Restarting RonDB connection\n");
      ReconnectInt(true);
    }
  }
}

//--------------------------------------------------------------------------------------------------

RonDB_Stats RDRSRonDBConnection::GetStats() {
  std::lock_guard<std::mutex> guard(__mutex);

  stats.ndb_objects_available = availableNdbObjects.size();
  return stats;
}

//--------------------------------------------------------------------------------------------------
RS_Status RDRSRonDBConnection::Shutdown() {
  return Shutdown(false);
}

//--------------------------------------------------------------------------------------------------

RS_Status RDRSRonDBConnection::Shutdown(bool end) {
  std::lock_guard<std::mutex> guard(__mutex);

  connectionState = DISCONNECTED;

  // delete all ndb objects
  while (allAvailableNdbObjects.size() > 0) {
    Ndb *ndb_object = allAvailableNdbObjects.front();
    allAvailableNdbObjects.pop_front();
    delete ndb_object;
  }
  availableNdbObjects.clear();
  allAvailableNdbObjects.clear();

  // clean up stats
  stats.ndb_objects_available = 0;
  stats.ndb_objects_count     = 0;
  stats.ndb_objects_created   = 0;
  stats.ndb_objects_deleted   = 0;

  // delete connection
  try {
    delete ndbConnection;
  } catch (...) {
    LOG_WARN("Exception in Shutdown");
  }
  ndbConnection = nullptr;

  LOG_INFO("RonDB connection and object pool shutdown");

  if (end) {
    ndb_end(1);  // sometimes causes seg faults when called repeatedly from unit tests*/
    delete connection_string;
    delete node_ids;
    if (reconnectionThread != nullptr) {
      NdbThread_Destroy(&reconnectionThread);
    }
  }
  return RS_OK;
}

//--------------------------------------------------------------------------------------------------
RS_Status RDRSRonDBConnection::ReconnectHandler() {
  require(reconnectionInProgress);

  // stop all the NDB objects
  using namespace std::chrono;
  Int64 startTime   = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  Int64 timeElapsed = 0;

  bool allNDBObjectsCountedFor = false;
  do {
    std::lock_guard<std::mutex> guard(__mutex);
    if (stats.ndb_objects_created != availableNdbObjects.size()) {
      LOG_INFO("Waiting to all NDB objects to return before shutdown");
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    } else {
      allNDBObjectsCountedFor = true;
      break;
    }
    timeElapsed =
        duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count() - startTime;
  } while (timeElapsed < 60 * 1000);

  if (!allNDBObjectsCountedFor) {
    LOG_ERROR("Timeout waiting for all NDB objects");
  }

  Shutdown(false);
  Connect();
  LOG_INFO("RonDB reconnection completed");

  reconnectionInProgress = false;
  return RS_OK;
}

//--------------------------------------------------------------------------------------------------

static void *reconnect_thread_wrapper(void *arg) {
  LOG_INFO("Started RonDB reconnection thread");
  RDRSRonDBConnection *rdrsRonDBConnection = (RDRSRonDBConnection *)arg;
  rdrsRonDBConnection->ReconnectHandler();
  return NULL;
}

//--------------------------------------------------------------------------------------------------

// Note it is only public for testing
RS_Status RDRSRonDBConnection::Reconnect() {
  return ReconnectInt(false);
}

//--------------------------------------------------------------------------------------------------

RS_Status RDRSRonDBConnection::ReconnectInt(bool internal) {
  if (!internal) {
    std::lock_guard<std::mutex> guard(__mutex);
  }
  if (reconnectionInProgress) {
    LOG_INFO("Ignoring RonDB reconnection request. A reconnection request is already in progress");
    return RS_SERVER_ERROR(ERROR_036);
  }

  reconnectionInProgress = true;

  LOG_INFO("Sarting a reconnection thread");

  if (reconnectionThread != nullptr) {
    NdbThread_Destroy(&reconnectionThread);
  }

  reconnectionThread = NdbThread_Create(reconnect_thread_wrapper, (NDB_THREAD_ARG *)__instance,
                                        0,  // default stack size
                                        "reconnection_thread", NDB_THREAD_PRIO_MEAN);

  if (reconnectionThread == nullptr) {
    LOG_PANIC("Failed to start reconnection thread");
  }

  return RS_OK;
}

//--------------------------------------------------------------------------------------------------

