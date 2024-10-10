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

ThreadContext::ThreadContext() {
  m_thread_context_mutex = NdbMutex_Create();
  require(m_thread_context_mutex);
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
}

RDRSRonDBConnectionPool::~RDRSRonDBConnectionPool() {
  is_shutdown = true;
  for (Uint32 i = 0; i < m_num_threads; i++) {
    ThreadContext *thread_context = m_thread_context[i];
    NdbMutex_Lock(thread_context->m_thread_context_mutex);
    thread_context->m_is_shutdown = true;
    Ndb *ndb_object = thread_context->m_ndb_object;
    if (thread_context->m_is_ndb_object_in_use == false &&
        ndb_object != nullptr) {
      thread_context->m_ndb_object = nullptr;
      NdbMutex_Unlock(thread_context->m_thread_context_mutex);
      RS_Status status;
      dataConnection->ReturnNDBObjectToPool(ndb_object, &status);
    } else {
      NdbMutex_Unlock(thread_context->m_thread_context_mutex);
    }
  }
  delete dataConnection;
  delete metadataConnection;
  dataConnection = nullptr;
  metadataConnection = nullptr;
  for (Uint32 i = 0; i < m_num_threads; i++) {
    delete m_thread_context[i];
  }
  free(m_thread_context);
}

RS_Status RDRSRonDBConnectionPool::Check() {
  if (unlikely(dataConnection == nullptr ||
               metadataConnection == nullptr ||
               is_shutdown == true)) {
    return RS_SERVER_ERROR(ERROR_034);
  }
  return RS_OK;
}

RS_Status RDRSRonDBConnectionPool::Init(Uint32 numThreads) {
  m_num_threads = numThreads;
  m_thread_context = (ThreadContext**)malloc(sizeof(ThreadContext*) * m_num_threads);
  for (Uint32 i = 0; i < numThreads; i++) {
    m_thread_context[i] = new ThreadContext();
    require(m_thread_context[i]);
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
  require(connection_pool_size == 1);

  dataConnection = new RDRSRonDBConnection(connection_string,
                                           node_ids,
                                           node_ids_len,
                                           connection_retries,
                                           connection_retry_delay_in_sec);
  RS_Status status = dataConnection->Connect();
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
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

  require(connection_pool_size == 1);
  metadataConnection = new RDRSRonDBConnection(connection_string,
                                               node_ids,
                                               node_ids_len,
                                               connection_retries,
                                               connection_retry_delay_in_sec);
  RS_Status status   = metadataConnection->Connect();
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  return RS_OK;
}

RS_Status RDRSRonDBConnectionPool::GetNdbObject(Ndb **ndb_object,
                                                Uint32 threadIndex) {
  RS_Status status = Check();
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
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
  status = dataConnection->GetNdbObject(ndb_object);
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
  dataConnection->ReturnNDBObjectToPool(ndb_object, status);
  return RS_OK;
}

RS_Status RDRSRonDBConnectionPool::GetMetadataNdbObject(Ndb **ndb_object) {
  RS_Status s = Check();
  if (unlikely(s.http_code != SUCCESS)) {
    return s;
  }
  RS_Status status = metadataConnection->GetNdbObject(ndb_object);
  return status;
}

RS_Status RDRSRonDBConnectionPool::ReturnMetadataNdbObject(Ndb *ndb_object,
                                                           RS_Status *status) {
  metadataConnection->ReturnNDBObjectToPool(ndb_object, status);
  return RS_OK;
}

RS_Status RDRSRonDBConnectionPool::Reconnect() {
  RS_Status s = Check();
  if (unlikely(s.http_code != SUCCESS)) {
    return s;
  }
  RS_Status status = dataConnection->Reconnect();
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }

  status = metadataConnection->Reconnect();
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  return RS_OK;
}

RonDB_Stats RDRSRonDBConnectionPool::GetStats() {
  // TODO FIXME Do not merge stats
  RonDB_Stats dataConnectionStats;
  dataConnection->GetStats(dataConnectionStats);
  RonDB_Stats metadataConnectionStats;
  metadataConnection->GetStats(metadataConnectionStats);
  RonDB_Stats merged_stats;

  merged_stats.connection_state =
      dataConnectionStats.connection_state >
        metadataConnectionStats.connection_state
          ? dataConnectionStats.connection_state
          : metadataConnectionStats.connection_state;
  merged_stats.is_reconnection_in_progress =
    dataConnectionStats.is_reconnection_in_progress |
      metadataConnectionStats.is_reconnection_in_progress;
  merged_stats.is_shutdown =
    dataConnectionStats.is_shutdown | metadataConnectionStats.is_shutdown;
  merged_stats.ndb_objects_available =
    dataConnectionStats.ndb_objects_available +
      metadataConnectionStats.ndb_objects_available;
  merged_stats.ndb_objects_count =
      dataConnectionStats.ndb_objects_count +
        metadataConnectionStats.ndb_objects_count;
  merged_stats.ndb_objects_deleted =
      dataConnectionStats.ndb_objects_deleted +
        metadataConnectionStats.ndb_objects_deleted;
  merged_stats.ndb_objects_created =
      dataConnectionStats.ndb_objects_created +
        metadataConnectionStats.ndb_objects_created;
  return merged_stats;
}
