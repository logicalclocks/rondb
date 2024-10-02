/*
 * Copyright (C) 2024 Hopsworks AB
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

#include "fs_cache.hpp"
#include "config_structs.hpp"
#include "pk_data_structs.hpp"
#include "rdrs_dal.hpp"

#include <iostream>
#include <memory>
#include <openssl/evp.h>
#include <unordered_map>
#include <functional>
#include <algorithm>
#include <NdbThread.h>
#include <util/require.h>
#include <util/rondb_hash.hpp>
#include <EventLogger.hpp>

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_FS 1
//#define DEBUG_FS_THREAD 1
//#define DEBUG_FS_TIME 1
//#define DEBUG_FS_METADATA 1
#endif

#ifdef DEBUG_FS_METADATA
#define DEB_FS_METADATA(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_FS_METADATA(arglist) do { } while (0)
#endif

#ifdef DEBUG_FS
#define DEB_FS(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_FS(arglist) do { } while (0)
#endif

#ifdef DEBUG_FS_THREAD
#define DEB_FS_THREAD(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_FS_THREAD(arglist) do { } while (0)
#endif

#ifdef DEBUG_FS_TIME
#define DEB_FS_TIME(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_FS_TIME(arglist) do { } while (0)
#endif

#ifndef DEBUG_FS_THREAD
#define CLEANUP_SLEEP_TIME 10
#else
#define CLEANUP_SLEEP_TIME 1000
#endif
extern EventLogger *g_eventLogger;

FSMetadataCache *g_fs_metadata_cache = nullptr;

void start_fs_cache() {
  g_fs_metadata_cache = new FSMetadataCache();
  require(g_fs_metadata_cache != nullptr);
  DEB_FS(("FS Metadata Cache started: %p", g_fs_metadata_cache));
  g_fs_metadata_cache->start_fs_cache_thread();
}

extern "C" void* fs_key_thread_main(void *thr_arg) {
  Uint64 key_cache_id = (Uint64)thr_arg;
  Uint32 id = Uint32(key_cache_id);
  g_fs_metadata_cache->cache_entry_updater(id);
  return nullptr;
}

void FSMetadataCache::start_fs_cache_thread() {
  for (Uint64 i = 0; i < NUM_FS_CACHES; i++) {
    m_first_cache_entry[i] = nullptr;
    m_last_cache_entry[i] = nullptr;
    NdbThread *thread = NdbThread_Create(fs_key_thread_main,
                                         (void**)i,
                                          128 * 1024,
                                          "FS Key Cache thread",
                                          NDB_THREAD_PRIO_LOW);
    require(thread != nullptr);
  }
  return;
}

void stop_fs_cache() {
  DEB_FS(("FS Metadata Cache stopped: %p", g_fs_metadata_cache));
  if (g_fs_metadata_cache != nullptr) {
    delete g_fs_metadata_cache;
    g_fs_metadata_cache = nullptr;
  }
}

FSMetadataCache::FSMetadataCache() : m_fs_cache() {
  m_evicted = false;
  m_is_thread_running = false;
  for (Uint64 i = 0; i < NUM_FS_CACHES; i++) {
    m_rwLock[i] = NdbMutex_Create();
    m_queueLock[i] = NdbMutex_Create();
  }
  DEB_FS(("rwLock: %p, queueLock: %p", m_rwLock[0], m_queueLock[0]));
  m_sleepLock = NdbMutex_Create();
  m_sleepCond = NdbCondition_Create();
}

void FSMetadataCache::cleanup() {
  /* Start by waking all threads */
  DEB_FS(("Cleanup started"));
  NdbMutex_Lock(m_sleepLock);
  for (int i = 0; i < NUM_FS_CACHES; i++)
    NdbMutex_Lock(m_rwLock[i]);
  m_evicted = true;
  NdbCondition_Broadcast(m_sleepCond);
  NdbMutex_Unlock(m_sleepLock);
  for (int i = 0; i < NUM_FS_CACHES; i++)
    NdbMutex_Unlock(m_rwLock[i]);

  /* Wait for all objects to complete cleanup */
  for (int i = 0; i < NUM_FS_CACHES; i++) {
    NdbMutex_Lock(m_rwLock[i]);
    while (m_fs_cache[i].size() > 0) {
#ifdef DEBUG_FS
      Uint32 fs_cache_size = (Uint32)m_fs_cache[i].size();
#endif
      NdbMutex_Unlock(m_rwLock[i]);
      DEB_FS(("m_fs_cache[%d].size() = %u", i, fs_cache_size));
      NdbSleep_MilliSleep(CLEANUP_SLEEP_TIME);
      NdbMutex_Lock(m_rwLock[i]);
    }
    NdbMutex_Unlock(m_rwLock[i]);
  }
  while (true) {
    NdbMutex_Lock(m_sleepLock);
    if (m_is_thread_running) {
      NdbMutex_Unlock(m_sleepLock);
    } else {
      NdbMutex_Unlock(m_sleepLock);
      break;
    }
  }
  DEB_FS(("Cleanup finished"));
}

std::shared_ptr<metadata::FeatureViewMetadata> fs_metadata_cache_get(
  const std::string &fs_key,
  FSCacheEntry** entry) {
  return g_fs_metadata_cache->get_fs_metadata(fs_key, entry);
}

void fs_metadata_update_cache(
  std::shared_ptr<metadata::FeatureViewMetadata> data,
  FSCacheEntry* entry,
  std::shared_ptr<RestErrorCode> errorCode) {
  return g_fs_metadata_cache->update_cache(data, entry, errorCode);
}

std::shared_ptr<metadata::FeatureViewMetadata>
FSMetadataCache::get_fs_metadata(const std::string &fs_key,
                                 FSCacheEntry** entry) {
  *entry = nullptr;
#if (NUM_FS_CACHES == 1)
  Uint32 hash = 0;
#else
  Uint32 hash = rondb_xxhash_std(fs_key.c_str(), fs_key.size());
#endif
  Uint32 key_cache_id = hash & (NUM_FS_CACHES - 1);
  NdbMutex_Lock(m_rwLock[key_cache_id]);
  if (m_evicted) {
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    DEB_FS(("FS Metadata cache shutdown, Line: %u", __LINE__));
    return nullptr;
  }
  auto it = m_fs_cache[key_cache_id].find(fs_key);

  if (it == m_fs_cache[key_cache_id].end()) {
    *entry = allocate_empty_cache_entry(fs_key, key_cache_id);
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    DEB_FS(("FS Key not found, Line: %u", __LINE__));
    return nullptr;
  }
  auto cacheEntry = it->second;
  NdbMutex_Lock(cacheEntry->m_waitLock);
  NdbMutex_Unlock(m_rwLock[key_cache_id]);
  if (cacheEntry->m_state == FSCacheEntry::IS_INVALID) {
#ifdef DEBUG_FS
    int ref_count = cacheEntry->m_ref_count;
#endif
    NdbMutex_Unlock(cacheEntry->m_waitLock);
    DEB_FS(("FS Key found invalid, Line: %u, refCount: %d",
              __LINE__, ref_count));
    require(cacheEntry->m_errorCode != nullptr);
    *entry = cacheEntry;
    return nullptr;
  }
  cacheEntry->m_ref_count++;
  while (cacheEntry->m_state == FSCacheEntry::IS_FILLING) {
    NdbCondition_Wait(cacheEntry->m_waitCond, cacheEntry->m_waitLock);
  }
  cacheEntry->m_ref_count--;
  if (cacheEntry->m_state == FSCacheEntry::IS_INVALID) {
#ifdef DEBUG_FS
    int ref_count = cacheEntry->m_ref_count;
#endif
    NdbMutex_Unlock(cacheEntry->m_waitLock);
    DEB_FS(("FS Key found invalid, Line: %u, refCount: %d",
             __LINE__, ref_count));
    require(cacheEntry->m_errorCode != nullptr);
    *entry = cacheEntry;
    return nullptr;
  }
  require(cacheEntry->m_state == FSCacheEntry::IS_VALID);
#ifdef DEBUG_FS
  {
    int ref_count = cacheEntry->m_ref_count;
    DEB_FS(("Key: %s returned, refCount: %d",
            cacheEntry->m_key.c_str(), ref_count));
  }
#endif
  *entry = cacheEntry;
  std::shared_ptr<metadata::FeatureViewMetadata> data = cacheEntry->m_data;
  NdbMutex_Lock(m_queueLock[key_cache_id]);
  cacheEntry->m_lastUsed = NdbTick_getCurrentTicks();
  remove_entry(cacheEntry, key_cache_id);
  insert_last(cacheEntry, key_cache_id);
  NdbMutex_Unlock(m_queueLock[key_cache_id]);
  NdbMutex_Unlock(cacheEntry->m_waitLock);
  return data;
}

FSCacheEntry*
FSMetadataCache::allocate_empty_cache_entry(
  const std::string &fs_key,
  const Uint32 key_cache_id) {

  auto newCacheEntry = new FSCacheEntry();
  if (newCacheEntry == nullptr) {
    DEB_FS(("FS Key create CacheEntry failed, Line: %u", __LINE__));
    return nullptr;
  }
  DEB_FS(("FS Key %s inserted in cache with refCount: 1", fs_key.c_str()));
  newCacheEntry->m_ref_count = 1;
  newCacheEntry->m_state = FSCacheEntry::IS_FILLING;
  newCacheEntry->m_key_cache_id = key_cache_id;
  newCacheEntry->m_data = nullptr;
  newCacheEntry->m_errorCode = nullptr;
  newCacheEntry->m_key = fs_key;
  m_fs_cache[key_cache_id][fs_key] = newCacheEntry;

  /* Insert last in double linked list for handling refresh interval */
  NdbMutex_Lock(m_queueLock[key_cache_id]);
  newCacheEntry->m_lastUsed = NdbTick_getCurrentTicks();
  insert_last(newCacheEntry, key_cache_id);
  NdbMutex_Unlock(m_queueLock[key_cache_id]);
  return newCacheEntry;
}

void FSMetadataCache::update_cache(
  std::shared_ptr<metadata::FeatureViewMetadata> data,
  FSCacheEntry* entry,
  std::shared_ptr<RestErrorCode> errorCode) {
  /**
   * The cache could be in cleanup state, we only check for cleanup
   * state when retrieving from the cache, most likely a shutdown should
   * not happen while we are retrieving from the database the feature
   * store metadata. Even if it happens the cleanup isn't completed until
   * the use of this cached metadata is completed.
   */

  NdbMutex_Lock(entry->m_waitLock);
  entry->m_data = data;
  entry->m_errorCode = errorCode;
  if (data != nullptr) {
#ifdef DEBUG_FS_METADATA
    DEB_FS_METADATA(("Key %s have metadata: \n%s",
                     entry->m_key.c_str(),
                     entry->m_data->to_string().c_str()));
#endif
    entry->m_state = FSCacheEntry::IS_VALID;
    DEB_FS(("FS Key create CacheEntry succeeded, valid, Line: %u", __LINE__));
  } else {
    DEB_FS(("FS Key create CacheEntry succeeded, invalid, Line: %u", __LINE__));
    entry->m_state = FSCacheEntry::IS_INVALID;
  }
  entry->m_ref_count--;
#ifdef DEBUG_FS
  {
    int ref_count = entry->m_ref_count;
    DEB_FS(("Key: %s set refCount to %d",
            entry->m_key.c_str(), ref_count));
  }
#endif
  NdbCondition_Broadcast(entry->m_waitCond);
  NdbMutex_Unlock(entry->m_waitLock);
  return;
}

void FSMetadataCache::insert_last(FSCacheEntry *entry, Uint32 key_cache_id) {
  entry->m_next_cache_entry = nullptr;
  entry->m_prev_cache_entry = m_last_cache_entry[key_cache_id];
  if (m_first_cache_entry[key_cache_id] == nullptr) {
    m_first_cache_entry[key_cache_id] = entry;
  } else {
    m_last_cache_entry[key_cache_id]->m_next_cache_entry = entry;
  }
  m_last_cache_entry[key_cache_id] = entry;
}

void FSMetadataCache::remove_entry(FSCacheEntry *entry, Uint32 key_cache_id) {
  if (entry == m_first_cache_entry[key_cache_id]) {
    assert(entry->m_prev_cache_entry == nullptr);
    m_first_cache_entry[key_cache_id] = entry->m_next_cache_entry;
    if (entry->m_next_cache_entry != nullptr) {
      entry->m_next_cache_entry->m_prev_cache_entry = nullptr;
    } else {
      m_last_cache_entry[key_cache_id] = nullptr;
    }
  } else if (entry == m_last_cache_entry[key_cache_id]) {
    assert(entry->m_next_cache_entry == nullptr);
    assert(entry->m_prev_cache_entry != nullptr);
    entry->m_prev_cache_entry->m_next_cache_entry = nullptr;
    m_last_cache_entry[key_cache_id] = entry->m_prev_cache_entry;
  } else {
    entry->m_next_cache_entry->m_prev_cache_entry = entry->m_prev_cache_entry;
    entry->m_prev_cache_entry->m_next_cache_entry = entry->m_next_cache_entry;
  }
  entry->m_next_cache_entry = nullptr;
  entry->m_prev_cache_entry = nullptr;
}

void FSMetadataCache::cache_entry_updater(Uint32 key_cache_id) {
  m_is_thread_running = true;
  while (true) {
    Uint32 sleepMillis = 100;
    Uint64 eviction_ms =
      (Uint64)globalConfigs.security.apiKey.cacheUnusedEntriesEvictionMS;
    NdbMutex_Lock(m_rwLock[key_cache_id]);
    FSCacheEntry* first_entry = m_first_cache_entry[key_cache_id];
    if (first_entry != nullptr) {
      NDB_TICKS now = NdbTick_getCurrentTicks();
      NdbMutex_Lock(first_entry->m_waitLock);
      if (first_entry->m_ref_count == 0) {
        NDB_TICKS lastUsed = first_entry->m_lastUsed;
        Uint64 milliSeconds = NdbTick_Elapsed(lastUsed, now).milliSec();
        if (m_evicted || (milliSeconds >= eviction_ms)) {
          DEB_FS(("FS Key %s deleted", first_entry->m_key.c_str()));
          m_fs_cache[key_cache_id].erase(first_entry->m_key);
          NdbMutex_Unlock(m_rwLock[key_cache_id]);
          NdbMutex_Lock(m_queueLock[key_cache_id]);
          remove_entry(first_entry, key_cache_id);
          NdbMutex_Unlock(m_queueLock[key_cache_id]);
          NdbMutex_Unlock(first_entry->m_waitLock);
          delete first_entry;
          continue;
        }
      } else {
#ifdef DEBUG_FS
      int ref_count = first_entry->m_ref_count;
      DEB_FS(("FS Key %s ready for delete, ref_count: %d",
              first_entry->m_key.c_str(), ref_count));
#endif
      }
      NdbMutex_Unlock(first_entry->m_waitLock);
    } else if (m_evicted) {
      /* We have no more cache entries to update so can safely stop here */
      NdbMutex_Unlock(m_rwLock[key_cache_id]);
      NdbMutex_Lock(m_sleepLock);
      m_is_thread_running = false;
      NdbMutex_Unlock(m_sleepLock);
      DEB_FS(("Stop FS cache thread"));
      return;
    }
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    NdbMutex_Lock(m_sleepLock);
    if (!m_evicted)
      NdbCondition_WaitTimeout(m_sleepCond,
                               m_sleepLock,
                               sleepMillis);
    NdbMutex_Unlock(m_sleepLock);
  }
}
