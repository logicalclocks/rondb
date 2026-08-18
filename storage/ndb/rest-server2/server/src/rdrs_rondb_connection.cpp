/*
 * Copyright (c) 2023, 2026, Hopsworks and/or its affiliates.
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

#include "rdrs_rondb_connection.hpp"
#include "ndb_api_helper.hpp"
#include "status.hpp"
#include "error_strings.h"
#include "logger.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <NdbThread.h>
#include <NdbTick.h>
#include <NdbSleep.h>
#include <ndb_global.h>
#include <util/require.h>

RDRSRonDBConnection::RDRSRonDBConnection(const char *connection_string,
                                         Uint32 node_id,
                                         Uint32 connection_retries,
                                         Uint32 connection_retry_delay_in_sec) {
  require(magic == 0);

  connectionMutex = NdbMutex_Create();
  connectionInfoMutex = NdbMutex_Create();
  stats.ndb_objects_available = 0;
  stats.ndb_objects_count = 0;
  stats.ndb_objects_created = 0;
  stats.ndb_objects_deleted = 0;
  stats.is_reconnection_in_progress = false;
  stats.is_shutdown = false;
  stats.is_shutting_down = false;
  stats.connection_state = DISCONNECTED;

  size_t connection_string_len = strlen(connection_string);
  this->connection_string =
    reinterpret_cast<char *>(malloc(connection_string_len + 1));
  std::strncpy(this->connection_string,
               connection_string,
               connection_string_len + 1);
  this->connection_string[connection_string_len] = '\0';
  this->m_node_id = node_id;
  this->connection_retries = connection_retries;
  this->connection_retry_delay_in_sec = connection_retry_delay_in_sec;

  ndbConnection = nullptr;
  reconnectionThread = nullptr;

  magic = expectedMagic;
}

RS_Status RDRSRonDBConnection::Connect() {
  checkMagic();

  rdrs_logger::info(std::string("Connecting to ") + connection_string);
  {
    NdbMutex_Lock(connectionInfoMutex);
    if (unlikely(stats.is_shutdown || stats.is_shutting_down)) {
      NdbMutex_Unlock(connectionInfoMutex);
      return RS_SERVER_ERROR(std::string(rdrsErrorMessage(ERROR_PROGRAMMING_CONNECTION_SHUTDOWN)));
    }
    require(stats.connection_state != CONNECTED);
    NdbMutex_Unlock(connectionInfoMutex);
  }
  {
    NdbMutex_Lock(connectionMutex);
    require(ndbConnection == nullptr);
    int retCode = 0;
    ndbConnection = new Ndb_cluster_connection(connection_string, m_node_id);
    retCode = ndbConnection->connect(connection_retries,
                                     connection_retry_delay_in_sec,
                                     0);
    if (unlikely(retCode != 0)) {
      NdbMutex_Unlock(connectionMutex);
      return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_RONDB_MGM_CONNECT_FAILED))
        + std::string(" RetCode: ") + std::to_string(retCode));
    }
    retCode = ndbConnection->wait_until_ready(30, 30);
    if (retCode == 1) {
      // Partial availability: at least one data node is connected but not all.
      // This is acceptable - the cluster can serve requests with partial nodes.
      rdrs_logger::warn(
        "Not all data nodes are connected, but at least one is available. "
        "Proceeding with partial cluster availability.");
    } else if (retCode != 0) {
      RS_Status status = RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_CLUSTER_NOT_READY)) +
        std::string(" RetCode: ") + std::to_string(retCode) +
        std::string(" Lastest Error: ") +
        std::to_string(ndbConnection->get_latest_error()) +
        std::string(" Lastest Error Msg: ") +
        std::string(ndbConnection->get_latest_error_msg()));
      NdbMutex_Unlock(connectionMutex);
      return status;
    }
    NdbMutex_Unlock(connectionMutex);
  }
  {
    NdbMutex_Lock(connectionInfoMutex);
    stats.connection_state = CONNECTED;
    NdbMutex_Unlock(connectionInfoMutex);
  }
  rdrs_logger::info("RonDB connection and object pool initialized");
  return RS_OK;
}

RDRSRonDBConnection::~RDRSRonDBConnection() {
  checkMagic();

  {
    NdbMutex_Lock(connectionMutex);
    NdbMutex_Lock(connectionInfoMutex);
    require(stats.ndb_objects_created == stats.ndb_objects_deleted);
    require(stats.ndb_objects_count == 0);
    require(stats.ndb_objects_available == 0);
    require(stats.connection_state == DISCONNECTED);
    require(stats.is_shutdown);
    require(!stats.is_shutting_down);
    require(!stats.is_reconnection_in_progress);
    require(ndbConnection == nullptr);
    require(reconnectionThread == nullptr);
    NdbMutex_Unlock(connectionMutex);
    NdbMutex_Unlock(connectionInfoMutex);
  }

  magic = 0;

  NdbMutex_Destroy(connectionMutex);
  NdbMutex_Destroy(connectionInfoMutex);
}

RS_Status RDRSRonDBConnection::GetNdbObject(Ndb **ndb_object) {

  {
    bool is_shutdown;
    bool reconnection_in_progress;
    STATE connection_state = DISCONNECTED;
    {
      NdbMutex_Lock(connectionInfoMutex);
      is_shutdown = stats.is_shutdown || stats.is_shutting_down;
      reconnection_in_progress = stats.is_reconnection_in_progress;
      connection_state = stats.connection_state;
      NdbMutex_Unlock(connectionInfoMutex);
    }
    if (unlikely(is_shutdown)) {
      rdrs_logger::error(std::string(
        rdrsErrorMessage(ERROR_PROGRAMMING_CONNECTION_SHUTDOWN)));
      return RS_SERVER_ERROR(std::string(
        rdrsErrorMessage(ERROR_PROGRAMMING_CONNECTION_SHUTDOWN)));
    }
    /* Also refuse to hand out objects while a reconnection is in
     * progress: the teardown is waiting for all objects to return before
     * it deletes them together with the cluster connection. */
    if (unlikely(connection_state != CONNECTED || reconnection_in_progress)) {
      if (!reconnection_in_progress) {
        // If previous reconnection attempts have failed then
        // restart the reconnection process
        rdrs_logger::debug("GetNdbObject triggered reconnection");
        Reconnect();
      }
      rdrs_logger::warn(std::string(
        rdrsErrorMessage(ERROR_RONDB_CONNECTION_CLOSED)) + 
                           std::string(" Connection State: ") +
                           std::to_string(connection_state) +
                           std::string(" Reconnection State: ") +
                           std::to_string(reconnection_in_progress));
      return RS_SERVER_ERROR(std::string(
        rdrsErrorMessage(ERROR_RONDB_CONNECTION_CLOSED)));
    }
  }
  {
    NdbMutex_Lock(connectionMutex);
    NdbMutex_Lock(connectionInfoMutex);
    /* Re-validate under the locks: the state check above dropped its lock,
     * and a reconnection teardown may have started - or already deleted
     * ndbConnection - in between. Creating an Ndb object from a deleted
     * (or null) cluster connection is a use-after-free. */
    if (unlikely(stats.is_shutdown || stats.is_shutting_down ||
                 stats.is_reconnection_in_progress ||
                 stats.connection_state != CONNECTED ||
                 ndbConnection == nullptr)) {
      NdbMutex_Unlock(connectionMutex);
      NdbMutex_Unlock(connectionInfoMutex);
      return RS_SERVER_ERROR(std::string(
        rdrsErrorMessage(ERROR_RONDB_CONNECTION_CLOSED)));
    }
    if (unlikely(availableNdbObjects.empty())) {
      *ndb_object = new Ndb(ndbConnection);
      int retCode = (*ndb_object)->init(RDRSRonDBConnection::MAX_PARALLEL_KEY_OPS);
      if (unlikely(retCode != 0)) {
        /* Do not track the failed object: the caller never returns it, so
         * counting it would make every later teardown wait for an object
         * that cannot come back. */
        delete *ndb_object;
        *ndb_object = nullptr;
        NdbMutex_Unlock(connectionMutex);
        NdbMutex_Unlock(connectionInfoMutex);
        return RS_SERVER_ERROR(
              std::string(rdrsErrorMessage(ERROR_NDB_OBJECT_INIT_FAILED)) +
              std::string(" RetCode: ") + std::to_string(retCode));
      }
      stats.ndb_objects_created++;
      stats.ndb_objects_count++;
      allAvailableNdbObjects.push_back(*ndb_object);
    } else {
      *ndb_object = availableNdbObjects.front();
      availableNdbObjects.pop_front();
    }
    NdbMutex_Unlock(connectionMutex);
    NdbMutex_Unlock(connectionInfoMutex);
    return RS_OK;
  }
}

void RDRSRonDBConnection::ReturnNDBObjectToPool(Ndb *ndb_object,
                                                RS_Status *status) {
  {
    NdbMutex_Lock(connectionMutex);
    /* If the object is not tracked any more, a reconnection teardown timed
     * out waiting for it and already deleted it. Pushing the dangling
     * pointer would hand it out again later; drop it instead. */
    bool tracked = std::find(allAvailableNdbObjects.begin(),
                             allAvailableNdbObjects.end(),
                             ndb_object) != allAvailableNdbObjects.end();
    if (likely(tracked)) {
      availableNdbObjects.push_back(ndb_object);
    }
    NdbMutex_Unlock(connectionMutex);
    if (unlikely(!tracked)) {
      rdrs_logger::error(
        "Returned NDB object is no longer tracked by the connection pool."
        " It was deleted by a reconnection. Dropping it.");
    }
  }
  if (unlikely(status != nullptr && status->http_code != SUCCESS)) {
    /* Reconnect only when this API node has lost every data node. Errors
     * on a cluster that is still reachable - single node failure,
     * timeout, overload, single user mode - must not tear down the
     * connection: long-lived event subscribers (TTL schema watcher, cache
     * watchers) only release their Ndb objects on TE_CLUSTER_FAILURE, and
     * the teardown waits for every object. */
    if (ndb_error_cluster_unavailable(status->code)) {
      rdrs_logger::error(
        "Detected connection loss. Triggering reconnection.");
      Reconnect();
    }
  }
}

int RDRSRonDBConnection::GetNumReadyDataNodes() {
  /* connectionMutex is held for tens of seconds only while the cluster
   * connection is rebuilt: Connect()'s wait_until_ready() and the
   * delete ndbConnection in Shutdown(). Those windows are exactly what the
   * state below describes, so check it first and never wait for them.
   * connectionInfoMutex is safe to wait on: its longest critical section is
   * the teardown's Ndb-object deletion loop, bounded by the object count.
   *
   * Every other holder keeps the mutex for microseconds, and one of them is
   * on the hot path: the metadata Ndb objects are not thread-cached, so
   * GetMetadataNdbObject/ReturnMetadataNdbObject take this mutex on every
   * API-key validation and every feature store metadata read. A failed
   * try-lock there means contention, NOT an unavailable cluster - reporting
   * 0 would flap /health on a healthy but busy server - so retry briefly
   * instead of guessing. */
  {
    NdbMutex_Lock(connectionInfoMutex);
    const bool unavailable = stats.is_shutdown || stats.is_shutting_down ||
                             stats.is_reconnection_in_progress ||
                             stats.connection_state != CONNECTED;
    NdbMutex_Unlock(connectionInfoMutex);
    if (unlikely(unavailable)) {
      return 0;
    }
  }
  for (Uint32 attempt = 0; attempt < READY_NODES_TRYLOCK_ATTEMPTS; attempt++) {
    if (likely(NdbMutex_Trylock(connectionMutex) == 0)) {
      int ready = -1;
      if (likely(ndbConnection != nullptr)) {
        ready = ndbConnection->get_no_ready();
      }
      NdbMutex_Unlock(connectionMutex);
      return ready;
    }
    NdbSleep_MilliSleep(1);
  }
  /* Unbroken contention for the whole window: a rebuild must have started
   * after the check above, or the server is pathologically overloaded.
   * Either way it cannot serve right now. */
  rdrs_logger::warn(
    "Health check could not read the number of ready data nodes: "
    "connectionMutex busy for " +
    std::to_string(READY_NODES_TRYLOCK_ATTEMPTS) + " ms.");
  return 0;
}

void RDRSRonDBConnection::GetStats(RonDB_Stats &ret) {
  NdbMutex_Lock(connectionInfoMutex);
  stats.ndb_objects_available = availableNdbObjects.size();
  ret = stats;
  NdbMutex_Unlock(connectionInfoMutex);
  return;
}

RS_Status RDRSRonDBConnection::Shutdown(bool end) {
  checkMagic();

  if (end) {
    // We are shutting down for good
    NdbMutex_Lock(connectionInfoMutex);
    stats.is_shutting_down = true;
    NdbMutex_Unlock(connectionInfoMutex);
  }

  // Wait for all NDB objects to return
  const NDB_TICKS startTime = NdbTick_getCurrentTicks();
  Uint64 msElapsed = 0;
  bool allNDBObjectsAccountedFor = false;
  do {
    size_t expectedSize = 0;
    Uint32 sizeGot = 0;
    {
      NdbMutex_Lock(connectionMutex);
      NdbMutex_Lock(connectionInfoMutex);
      sizeGot      = availableNdbObjects.size();
      expectedSize = stats.ndb_objects_created;
      NdbMutex_Unlock(connectionMutex);
      NdbMutex_Unlock(connectionInfoMutex);
    }
    if (expectedSize != sizeGot) {
      rdrs_logger::warn(
        "Waiting for all NDB objects to return before shutdown."
        " Expected Size: " + std::to_string(expectedSize) +
        " Have: " + std::to_string(sizeGot));
      NdbSleep_MilliSleep(500);
    } else {
      allNDBObjectsAccountedFor = true;
      break;
    }
    const NDB_TICKS now = NdbTick_getCurrentTicks();
    msElapsed = NdbTick_Elapsed(startTime, now).milliSec();
  } while (msElapsed < 120 * 1000);
  if (!allNDBObjectsAccountedFor) {
    rdrs_logger::error("Timed out waiting for all NDB objects.");
  } else {
    rdrs_logger::info(
      "All NDB objects are accounted for. Total objects: " +
      std::to_string(stats.ndb_objects_created));
  }

  if (end) {
    // Wait for reconnection thread to exit
    const NDB_TICKS rtStartTime = NdbTick_getCurrentTicks();
    Uint64 rtMsElapsed = 0;
    bool rtRunning;
    do {
      {
        NdbMutex_Lock(connectionInfoMutex);
        rtRunning = stats.is_reconnection_in_progress;
        NdbMutex_Unlock(connectionInfoMutex);
      }
      if (rtRunning) {
        rdrs_logger::warn(
          "Waiting for reconnection thread to exit.");
        NdbSleep_MilliSleep(500);
      } else {
        break;
      }
      const NDB_TICKS now = NdbTick_getCurrentTicks();
      rtMsElapsed = NdbTick_Elapsed(rtStartTime, now).milliSec();
    } while (rtMsElapsed < 300 * 1000);
    if (rtRunning) {
      rdrs_logger::error("Timed out waiting for reconnection thread to exit. "
                         "Proceeding with shutdown, which could cause a crash.");
    } else {
      rdrs_logger::info(
        "Reconnection thread has exited. Proceeding with shutdown.");
    }
    {
      NdbMutex_Lock(connectionMutex);
      if (reconnectionThread != nullptr) {
        void *thread_status = nullptr;
        NdbThread_WaitFor(reconnectionThread, &thread_status);
        NdbThread_Destroy(&reconnectionThread);
        reconnectionThread = nullptr;
      }
      NdbMutex_Unlock(connectionMutex);
    }
  }

  rdrs_logger::info("Shutting down RonDB connection and NDB object pool");
  {
    NdbMutex_Lock(connectionInfoMutex);
    stats.connection_state = DISCONNECTED;
    NdbMutex_Unlock(connectionInfoMutex);
  }
  {
    NdbMutex_Lock(connectionMutex);
    NdbMutex_Lock(connectionInfoMutex);
    /* Delete only the Ndb objects that were handed back. An object still
     * held by a thread that missed the wait deadline above is leaked
     * instead: deleting it under its holder is a use-after-free, and a
     * leaked object's address can never be recycled into a new Ndb
     * object, so a very late return is dropped by the tracked-check in
     * ReturnNDBObjectToPool rather than corrupting the new pool. */
    Uint32 leakedObjects = 0;
    while (allAvailableNdbObjects.size() > 0) {
      Ndb *ndb_object = allAvailableNdbObjects.front();
      allAvailableNdbObjects.pop_front();
      bool returned = std::find(availableNdbObjects.begin(),
                                availableNdbObjects.end(),
                                ndb_object) != availableNdbObjects.end();
      if (likely(returned)) {
        delete ndb_object;
      } else {
        leakedObjects++;
      }
    }
    availableNdbObjects.clear();
    allAvailableNdbObjects.clear();

    // Clean up stats
    stats.ndb_objects_available = 0;
    stats.ndb_objects_count = 0;
    stats.ndb_objects_created = 0;
    stats.ndb_objects_deleted = 0;
    NdbMutex_Unlock(connectionMutex);
    NdbMutex_Unlock(connectionInfoMutex);
    if (unlikely(leakedObjects > 0)) {
      rdrs_logger::error(
        "Leaked " + std::to_string(leakedObjects) +
        " NDB object(s) still held by their users at teardown time.");
    }
  }
  {
    NdbMutex_Lock(connectionMutex);
    // Delete connection
    try {
      rdrs_logger::debug("delete ndbconnection");
      delete ndbConnection;
    } catch (...) {
      rdrs_logger::warn("Exception in Shutdown");
    }
    ndbConnection = nullptr;
    NdbMutex_Unlock(connectionMutex);
  }
  {
    NdbMutex_Lock(connectionMutex);
    NdbMutex_Lock(connectionInfoMutex);
    if (end) {
      stats.is_shutdown = true;
      stats.is_shutting_down = false;
      free(connection_string);
      if (reconnectionThread != nullptr) {
        void *thread_status = nullptr;
        NdbThread_WaitFor(reconnectionThread, &thread_status);
        NdbThread_Destroy(&reconnectionThread);
        reconnectionThread = nullptr;
      }
    }
    NdbMutex_Unlock(connectionMutex);
    NdbMutex_Unlock(connectionInfoMutex);
    rdrs_logger::info("RonDB connection and NDB object pool shutdown");
    return RS_OK;
  }
}

RS_Status RDRSRonDBConnection::ReconnectHandler() {
  checkMagic();

  {
    NdbMutex_Lock(connectionInfoMutex);
    require(stats.is_reconnection_in_progress);
    NdbMutex_Unlock(connectionInfoMutex);
  }
  RS_Status status = Shutdown(false);
  if (status.http_code != SUCCESS) {
    NdbMutex_Lock(connectionInfoMutex);
    stats.is_reconnection_in_progress = false;
    NdbMutex_Unlock(connectionInfoMutex);
    return RS_SERVER_ERROR(
      "Reconnection. Shutdown failed. " + std::string("code: ") +
      std::to_string(status.code) + std::string(" Classification: ") +
      std::to_string(status.classification) + std::string(" Msg: ") +
      std::string(status.message));
  }
  status = Connect();
  if (status.http_code != SUCCESS) {
    NdbMutex_Lock(connectionInfoMutex);
    stats.is_reconnection_in_progress = false;
    NdbMutex_Unlock(connectionInfoMutex);
    return RS_SERVER_ERROR(
      "Reconnection. Connection failed. " + std::string("code: ") +
      std::to_string(status.code) + std::string(" Classification: ") +
      std::to_string(status.classification) + std::string(" Msg: ") +
      std::string(status.message));
  }
  NdbMutex_Lock(connectionInfoMutex);
  stats.is_reconnection_in_progress = false;
  NdbMutex_Unlock(connectionInfoMutex);
  return RS_OK;
}

static void *reconnect_thread_wrapper(void *arg) {
  errno = 0;
  rdrs_logger::info("Reconnection thread has started running.");
  RDRSRonDBConnection *rdrsRonDBConnection = (RDRSRonDBConnection *)arg;
  rdrsRonDBConnection->ReconnectHandler();
  return NULL;
}

static constexpr int MAX_RECONNECT_LISTENERS = 4;
static RDRSRonDBConnection::ReconnectListener
  g_reconnectListeners[MAX_RECONNECT_LISTENERS];
static std::atomic<int> g_numReconnectListeners{0};

void RDRSRonDBConnection::RegisterReconnectListener(
  ReconnectListener listener) {
  int idx = g_numReconnectListeners.load(std::memory_order_acquire);
  /* Idempotent: the caches re-register on every start (the C++ tests
   * start/stop them many times per process). */
  for (int i = 0; i < idx; i++) {
    if (g_reconnectListeners[i] == listener) {
      return;
    }
  }
  require(idx < MAX_RECONNECT_LISTENERS);
  g_reconnectListeners[idx] = listener;
  g_numReconnectListeners.store(idx + 1, std::memory_order_release);
}

static void notifyReconnectListeners() {
  int cnt = g_numReconnectListeners.load(std::memory_order_acquire);
  for (int i = 0; i < cnt; i++) {
    g_reconnectListeners[i]();
  }
}

// Note it is only public for testing
RS_Status RDRSRonDBConnection::Reconnect() {
  checkMagic();

  NdbMutex_Lock(connectionMutex);
  NdbMutex_Lock(connectionInfoMutex);
  if (stats.is_reconnection_in_progress) {
    NdbMutex_Unlock(connectionMutex);
    NdbMutex_Unlock(connectionInfoMutex);
    rdrs_logger::info(
      "Ignoring RonDB reconnection request. A reconnection request is"
      " already in progress.");
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_RONDB_RECONNECTION_IN_PROGRESS)));
  }
  if (stats.is_shutting_down) {
    NdbMutex_Unlock(connectionMutex);
    NdbMutex_Unlock(connectionInfoMutex);
    rdrs_logger::info(
      "Ignoring RonDB reconnection request during shutdown.");
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_RONDB_SHUTDOWN_IN_PROGRESS)));
  }
  stats.is_reconnection_in_progress = true;
  /* From this point on GetNdbObject() refuses to hand out objects, so any
   * object stamped with an older generation belongs to the connection
   * being torn down and must go back to the pool instead of a cache. */
  m_generation.fetch_add(1, std::memory_order_acq_rel);
  // clean previous failed/completed reconnection thread
  if (reconnectionThread != nullptr) {
    /* The previous handler has cleared is_reconnection_in_progress but its
     * thread may still be exiting; join before freeing the handle. */
    void *thread_status = nullptr;
    NdbThread_WaitFor(reconnectionThread, &thread_status);
    NdbThread_Destroy(&reconnectionThread);
    reconnectionThread = nullptr;
  }
  reconnectionThread = NdbThread_Create(reconnect_thread_wrapper,
                                        (NDB_THREAD_ARG *)this,
                                        0, // default stack size
                                        "reconnection_thread",
                                        NDB_THREAD_PRIO_MEAN);
  if (reconnectionThread == nullptr) {
    rdrs_logger::error("Failed to start reconnection thread");
  }
  NdbMutex_Unlock(connectionMutex);
  NdbMutex_Unlock(connectionInfoMutex);
  /* Ask long-lived Ndb object holders (cache event watchers) to release
   * their objects so the teardown's wait-for-objects converges. Called
   * after unlocking: listeners only set atomic flags, but must not run
   * under our mutexes. */
  notifyReconnectListeners();
  return RS_OK;
}
