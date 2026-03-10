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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_FS_CACHE_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_FS_CACHE_HPP_

#include "rdrs_hopsworks_dal.h"
#include "pk_data_structs.hpp"
#include "metadata.hpp"
#include "feature_store_error_code.hpp"

#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>
#include <thread>
#include <chrono>
#include <ndb_init.h>
#include <ndb_types.h>
#include <NdbSleep.h>
#include <NdbMutex.h>
#include <NdbCondition.h>
#include <NdbThread.h>

#define NUM_FS_CACHES 1

class FSMetadataCache;
extern FSMetadataCache *g_fs_metadata_cache;

void start_fs_cache();
void stop_fs_cache();
void fs_cache_dec_ref_count(char*);

class FSCacheEntry {
 public:
  metadata::FeatureViewMetadata *m_data;
  std::shared_ptr<RestErrorCode> m_errorCode;
  FSCacheEntry* m_next_cache_entry;
  FSCacheEntry* m_prev_cache_entry;
  NdbMutex *m_waitLock;
  NdbCondition *m_waitCond;
  Uint32 m_key_cache_id;
  std::string m_key;
  enum {
    IS_FILLING = 0,
    IS_INVALID = 1,
    IS_VALID = 2
  };
  Uint8 m_state;
  std::atomic<int> m_ref_count;

  FSCacheEntry() {
    m_data = nullptr;
    m_errorCode = nullptr;
    m_state = IS_FILLING;
    m_waitLock = NdbMutex_Create();
    m_waitCond = NdbCondition_Create();
  }

  ~FSCacheEntry() {
    NdbMutex_Destroy(m_waitLock);
    NdbCondition_Destroy(m_waitCond);
    if (m_data) delete m_data;
  }
};

metadata::FeatureViewMetadata*
  fs_metadata_cache_get(const std::string&, FSCacheEntry**);
void fs_metadata_update_cache(metadata::FeatureViewMetadata*,
                              FSCacheEntry*,
                              std::shared_ptr<RestErrorCode>);

class FSMetadataCache {
 public:
  FSMetadataCache();
  ~FSMetadataCache() {
    cleanup();
    for (int i = 0; i < NUM_FS_CACHES; i++) {
      NdbMutex_Destroy(m_rwLock[i]);
      NdbMutex_Destroy(m_queueLock[i]);
    }
    NdbMutex_Destroy(m_sleepLock);
    NdbCondition_Destroy(m_sleepCond);
  }

  metadata::FeatureViewMetadata*
    get_fs_metadata(const std::string&, FSCacheEntry**);
  void update_cache(metadata::FeatureViewMetadata*,
                    FSCacheEntry*,
                    std::shared_ptr<RestErrorCode>);
  void cache_entry_updater(Uint32);
  void start_fs_cache_thread();

  void preload_all_feature_views();
  void start_event_watcher();
  void event_watcher_job();
  // Force the event watcher to tear down and reconnect (for testing)
  void force_reconnect() { m_force_reconnect = true; }

 private:
  std::unordered_map<std::string, FSCacheEntry*> m_fs_cache[NUM_FS_CACHES];
  std::atomic<bool> m_stopped{false};
  std::atomic<bool> m_force_reconnect{false};
  NdbMutex *m_rwLock[NUM_FS_CACHES];
  NdbMutex *m_queueLock[NUM_FS_CACHES];
  NdbMutex *m_sleepLock;
  NdbCondition *m_sleepCond;
  FSCacheEntry* m_first_cache_entry[NUM_FS_CACHES];
  FSCacheEntry* m_last_cache_entry[NUM_FS_CACHES];
  NdbThread* m_cache_threads[NUM_FS_CACHES];
  NdbThread* m_event_watcher_thread;
  std::string m_event_name;
  bool m_is_thread_running;

  void cleanup();
  FSCacheEntry* allocate_empty_cache_entry(const std::string &fs_key,
                                           const Uint32 key_cache_id);
  void insert_last(FSCacheEntry*, Uint32);
  void remove_entry(FSCacheEntry*, Uint32);

  void load_single_feature_view(const std::string &fsName,
                                const std::string &fvName,
                                int fvVersion);
  void evict_entry(const std::string &cacheKey);
};
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_FS_CACHE_HPP_
