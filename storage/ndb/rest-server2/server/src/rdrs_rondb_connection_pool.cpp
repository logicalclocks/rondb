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

#include "rdrs_rondb_connection_pool.hpp"
#include "ndb_global.h"
#include "error_strings.h"
#include "status.hpp"

#include <util/require.h>
#include <EventLogger.hpp>

extern EventLogger *g_eventLogger;

static void check_startup(bool x) {
  if (!x) {
    g_eventLogger->info("Failure to allocate memory during startup of"
                        " rdrs2, failing");
    require(false);
  }
}

static void failed_rondb_connect(RS_Status status) {
  g_eventLogger->error(
    "Failed to start, failed connect to RonDB, status.code: %u, message: %s",
    status.code, status.message);
  require(false);
   
}

ThreadContext::ThreadContext() {
  m_thread_context_mutex = NdbMutex_Create();
  check_startup(m_thread_context_mutex != nullptr);
  m_is_shutdown = false;
  m_is_ndb_object_in_use = false;
  m_ndb_object = nullptr;
}

ThreadContext::~ThreadContext() {
  NdbMutex_Destroy(m_thread_context_mutex);
}

RDRSRonDBConnectionPool::RDRSRonDBConnectionPool() {
  is_shutdown = false;
  m_num_threads = 0;
  m_num_data_connections = 0;
  dataConnections = nullptr;
  metadataConnection = nullptr;
}

RDRSRonDBConnectionPool::~RDRSRonDBConnectionPool() {
  is_shutdown = true;
  if (m_thread_context != nullptr) {
    for (Uint32 i = 0; i < m_num_threads; i++) {
      ThreadContext *thread_context = m_thread_context[i];
      if (thread_context != nullptr) {
        NdbMutex_Lock(thread_context->m_thread_context_mutex);
        thread_context->m_is_shutdown = true;
        Ndb *ndb_object = thread_context->m_ndb_object;
        if (thread_context->m_is_ndb_object_in_use == false &&
            ndb_object != nullptr) {
          thread_context->m_ndb_object = nullptr;
          NdbMutex_Unlock(thread_context->m_thread_context_mutex);
          RS_Status status;
          Uint32 connection = i % m_num_data_connections;
          dataConnections[connection]->ReturnNDBObjectToPool(ndb_object,
                                                             &status);
        } else {
          NdbMutex_Unlock(thread_context->m_thread_context_mutex);
        }
        delete thread_context;
      }
    }
    free(m_thread_context);
    m_thread_context = nullptr;
  }
  if (metadataConnection != dataConnections[0]) {
    /* We borrowed a data connection for meta data connection */
    delete metadataConnection;
  }
  metadataConnection = nullptr;
  if (dataConnections != nullptr) {
    for (Uint32 i = 0; i < m_num_data_connections; i++) {
      delete dataConnections[i];
    }
    free(dataConnections);
    dataConnections = nullptr;
  }
}

RS_Status RDRSRonDBConnectionPool::Init(Uint32 numThreads,
                                        Uint32 numClusterConnections) {
  m_num_threads = numThreads;
  m_num_data_connections = numClusterConnections;

  m_thread_context = (ThreadContext**)
    malloc(sizeof(ThreadContext*) * m_num_threads);
  check_startup(m_thread_context != nullptr);
  memset(m_thread_context, 0, sizeof(ThreadContext*) * m_num_threads);

  dataConnections = (RDRSRonDBConnection**)
    malloc(sizeof(RDRSRonDBConnection**) * m_num_data_connections);
  check_startup(dataConnections != nullptr);
  memset(dataConnections,
         0,
         sizeof(RDRSRonDBConnection**) * m_num_data_connections);

  for (Uint32 i = 0; i < numThreads; i++) {
    m_thread_context[i] = new ThreadContext();
    check_startup(m_thread_context[i] != nullptr);
  }
  return RS_OK;
}

RS_Status RDRSRonDBConnectionPool::AddConnections(
  const char *connection_string,
  Uint32 connection_pool_size,
  Uint32 *node_ids,
  Uint32 node_ids_len,
  Uint32 connection_retries,
  Uint32 connection_retry_delay_in_sec) {

  if (connection_pool_size == 0) {
    g_eventLogger->error("At least one RonDB data connection is required, "
                         "failed startup of REST API Server");
    require(false);
  }
  require(connection_pool_size == m_num_data_connections);
  require(node_ids_len == 0 || node_ids_len == m_num_data_connections);
  for (Uint32 i = 0; i < connection_pool_size; i++) {
    Uint32 node_id = 0;
    if (node_ids_len > 0)
      node_id = node_ids[i];
    dataConnections[i] = new RDRSRonDBConnection(connection_string,
                                                 node_id,
                                                 connection_retries,
                                                 connection_retry_delay_in_sec);
  }
  for (Uint32 i = 0; i < connection_pool_size; i++) {
    RS_Status status = dataConnections[i]->Connect();
    if (unlikely(status.http_code != SUCCESS)) {
      failed_rondb_connect(status);
      return status;
    }
  }
  return RS_OK;
}

RS_Status RDRSRonDBConnectionPool::AddMetaConnections(
  const char *connection_string,
  Uint32 connection_pool_size,
  Uint32 *node_ids,
  Uint32 node_ids_len,
  Uint32 connection_retries,
  Uint32 connection_retry_delay_in_sec) {

  if (connection_pool_size == 0) {
    g_eventLogger->info(
      "No metadata cluster connection provided, we will use one of "
      "the data connections instead");
    metadataConnection = dataConnections[0];
    return RS_OK;
  }
  if (connection_pool_size > 1) {
    g_eventLogger->error("Only 1 metadata connection is supported");
    require(false);
  }
  Uint32 node_id = 0;
  if (node_ids_len > 0)
    node_id = node_ids[0];
  metadataConnection = new RDRSRonDBConnection(connection_string,
                                               node_id,
                                               connection_retries,
                                               connection_retry_delay_in_sec);
  RS_Status status   = metadataConnection->Connect();
  if (unlikely(status.http_code != SUCCESS)) {
    failed_rondb_connect(status);
    return status;
  }
  return RS_OK;
}

RS_Status RDRSRonDBConnectionPool::GetNdbObject(Ndb **ndb_object,
                                                Uint32 threadIndex) {
  ThreadContext *thread_context = m_thread_context[threadIndex];
  require(threadIndex < m_num_threads);
  NdbMutex_Lock(thread_context->m_thread_context_mutex);
  if (likely(thread_context->m_is_shutdown == false &&
             thread_context->m_is_ndb_object_in_use == false &&
             thread_context->m_ndb_object != nullptr)) {
    /**
     * This is by far the most common path through the code. We are not
     * shutting down, the Ndb object isn't in use and it exists. We only
     * allocate the Ndb object normally on the first interaction.
     */
    *ndb_object = thread_context->m_ndb_object;
    thread_context->m_is_ndb_object_in_use = true;
    NdbMutex_Unlock(m_thread_context[threadIndex]->m_thread_context_mutex);
    return RS_OK;
  }
  require(thread_context->m_is_ndb_object_in_use == false);
  if (thread_context->m_is_shutdown) {
    NdbMutex_Unlock(thread_context->m_thread_context_mutex);
    return RS_SERVER_ERROR(ERROR_034);
  }
  NdbMutex_Unlock(thread_context->m_thread_context_mutex);
  Uint32 connection = threadIndex % m_num_data_connections;
  RS_Status status = dataConnections[connection]->GetNdbObject(ndb_object);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  NdbMutex_Lock(thread_context->m_thread_context_mutex);
  thread_context->m_ndb_object = *ndb_object;
  thread_context->m_is_ndb_object_in_use = true;
  NdbMutex_Unlock(thread_context->m_thread_context_mutex);
  return RS_OK;
}

RS_Status RDRSRonDBConnectionPool::ReturnNdbObject(Ndb *ndb_object,
                                                   RS_Status *status,
                                                   Uint32 threadIndex) {
  ThreadContext *thread_context = m_thread_context[threadIndex];
  NdbMutex_Lock(thread_context->m_thread_context_mutex);
  if (thread_context->m_is_shutdown == false) {
    require(ndb_object == thread_context->m_ndb_object);
    require(thread_context->m_is_ndb_object_in_use);
    thread_context->m_is_ndb_object_in_use = false;
    NdbMutex_Unlock(thread_context->m_thread_context_mutex);
    return RS_OK;
  }
  /* We are shutting down, return the Ndb object */
  thread_context->m_is_ndb_object_in_use = false;
  thread_context->m_ndb_object = nullptr;
  NdbMutex_Unlock(thread_context->m_thread_context_mutex);
  Uint32 connection = threadIndex % m_num_data_connections;
  dataConnections[connection]->ReturnNDBObjectToPool(ndb_object, status);
  return RS_OK;
}

RS_Status RDRSRonDBConnectionPool::GetMetadataNdbObject(Ndb **ndb_object) {
  return metadataConnection->GetNdbObject(ndb_object);
}

RS_Status RDRSRonDBConnectionPool::ReturnMetadataNdbObject(Ndb *ndb_object,
                                                           RS_Status *status) {
  metadataConnection->ReturnNDBObjectToPool(ndb_object, status);
  return RS_OK;
}

RS_Status RDRSRonDBConnectionPool::Reconnect() {
  RS_Status status;
  for (Uint32 i = 0; i < m_num_data_connections; i++) {
    status = dataConnections[i]->Reconnect();
    if (unlikely(status.http_code != SUCCESS)) {
      return status;
    }
  }
  status = metadataConnection->Reconnect();
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  return RS_OK;
}

RonDB_Stats RDRSRonDBConnectionPool::GetStats() {
  // TODO FIXME Do not merge stats
  RonDB_Stats metadataConnectionStats;
  metadataConnection->GetStats(metadataConnectionStats);
  RonDB_Stats merged_stats = metadataConnectionStats;

  for (Uint32 i = 0; i < m_num_data_connections; i++) {
    RonDB_Stats dataConnectionStats;
    dataConnections[i]->GetStats(dataConnectionStats);
    merged_stats.ndb_objects_available +=
      dataConnectionStats.ndb_objects_available;
    merged_stats.ndb_objects_count +=
        dataConnectionStats.ndb_objects_count;
    merged_stats.ndb_objects_deleted +=
        dataConnectionStats.ndb_objects_deleted;
    merged_stats.ndb_objects_created +=
        dataConnectionStats.ndb_objects_created;

    merged_stats.connection_state =
      dataConnectionStats.connection_state >
        merged_stats.connection_state
          ? dataConnectionStats.connection_state
          : merged_stats.connection_state;

    merged_stats.is_reconnection_in_progress |=
      dataConnectionStats.is_reconnection_in_progress;

    merged_stats.is_shutdown |=
      dataConnectionStats.is_shutdown;
  }
  return merged_stats;
}
