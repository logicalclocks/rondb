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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_CACHE_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_CACHE_HPP_

#include "feature_store_error_code.hpp"
#include "rdrs_dal.hpp"
#include "rdrs_hopsworks_dal.h"
#include "config_structs.hpp"
#include "pk_data_structs.hpp"

#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>
#include <thread>
#include <chrono>
#include <future>
#include <iostream>
#include <map>
#include <pthread.h>
#include <NdbTick.h>
#include <NdbSleep.h>

class ReadLock {
 public:
  explicit ReadLock(pthread_rwlock_t &lock) : rwlock(lock) {
    pthread_rwlock_rdlock(&rwlock);
  }
  ~ReadLock() {
    pthread_rwlock_unlock(&rwlock);
  }
  ReadLock(const ReadLock &)            = delete;
  ReadLock &operator=(const ReadLock &) = delete;

 private:
  pthread_rwlock_t &rwlock;
};

class WriteLock {
 public:
  explicit WriteLock(pthread_rwlock_t &lock) : rwlock(lock) {
    pthread_rwlock_wrlock(&rwlock);
  }
  ~WriteLock() {
    pthread_rwlock_unlock(&rwlock);
  }
  WriteLock(const WriteLock &)            = delete;
  WriteLock &operator=(const WriteLock &) = delete;

 private:
  pthread_rwlock_t &rwlock;
};

template <typename T> class CacheEntry {
 public:
  NDB_TICKS lastUsed;
  NDB_TICKS lastUpdated;
  pthread_rwlock_t rwLock{};
  bool evicted = false;
  Int32 refreshInterval;
  std::atomic<int> refCount = 0;
  T data;

  CacheEntry() {
    pthread_rwlock_init(&rwLock, nullptr);
  }

  ~CacheEntry() {
    pthread_rwlock_destroy(&rwLock);
  }

  std::string to_string() const {
    return data.to_string();
  }
};

template <typename T> class Cache {
 public:
  std::unordered_map<std::string, std::shared_ptr<CacheEntry<T>>> key2Cache;  // Key -> Cached data
  pthread_rwlock_t key2CacheLock{};
  std::mt19937 randomGenerator;

  Cache() : key2Cache(), randomGenerator(std::random_device()()) {
    pthread_rwlock_init(&key2CacheLock, nullptr);
  }

  ~Cache() {
    cleanup();
    pthread_rwlock_destroy(&key2CacheLock);
  }

  RS_Status cleanup() {
    WriteLock lock(key2CacheLock);

    // TODO logger
    // Clean up all entries
    for (auto it = key2Cache.begin(); it != key2Cache.end(); it++) {
      it->second->evicted = true;
    }

    key2Cache = std::unordered_map<std::string, std::shared_ptr<CacheEntry<T>>>();

    return CRS_Status::SUCCESS.status;
  }

  std::shared_ptr<CacheEntry<T>> get_entry(const std::string &key) {
    pthread_rwlock_rdlock(&key2CacheLock);
    auto it = key2Cache.find(key);
    if (it == key2Cache.end()) {
      pthread_rwlock_unlock(&key2CacheLock);
      return nullptr;
    }

    auto entry = it->second;
    if (entry == nullptr) {
      pthread_rwlock_unlock(&key2CacheLock);
      return nullptr;
    }
    ReadLock readLock(entry->rwLock);
    pthread_rwlock_unlock(&key2CacheLock);

    // update TS
    entry->lastUsed = NdbTick_getCurrentTicks();
    return entry;
  }

  RS_Status update_cache(const std::string &key, const T &data) {
    // if the entry does not already exist in the
    // cache then multiple clients will try to read and
    // update the key
    // Trying to prevent multiple writers here
    // Check if the key exists
    bool shouldUpdate = false;
    {
      ReadLock readLock(key2CacheLock);
      if (key2Cache.find(key) == key2Cache.end()) {
        // The entry does not exist, mark for update
        shouldUpdate = true;
      } else {
        // Increase reference count to the entry
        key2Cache[key]->refCount++;
      }
    }  // Mutex is automatically released here

    if (shouldUpdate) {
      // Lock again to update the cache securely
      WriteLock lock(key2CacheLock);
      // Double-check pattern in case another thread has already updated the cache
      if (key2Cache.find(key) ==
          key2Cache.end()) {  // the entry still does not exists. insert a new row
        auto entry = std::make_shared<CacheEntry<T>>();
        entry->refreshInterval = refresh_interval_with_jitter();
        key2Cache[key] = entry;
        start_update_ticker(key, key2Cache[key], data);
      }
      // Increase reference count to the entry
      key2Cache[key]->refCount++;
    }
    return CRS_Status::SUCCESS.status;
  }

  RS_Status update_record(const T &data, CacheEntry<T> *entry) {
    // caller holds the lock
    if (entry->evicted) {
      return CRS_Status::SUCCESS.status;
    }
    entry->data = data;
    entry->lastUpdated = NdbTick_getCurrentTicks();

    return CRS_Status::SUCCESS.status;
  }

  NDB_TICKS last_used(const std::string &key) {
    ReadLock readLock(key2CacheLock);
    auto it = key2Cache.find(key);
    if (it == key2Cache.end()) {
      NDB_TICKS now = NdbTick_getCurrentTicks();
      return now;
    }
    return it->second->lastUsed;
  }

  NDB_TICKS last_updated(const std::string &key) {
    ReadLock readLock(key2CacheLock);
    auto it = key2Cache.find(key);
    if (it == key2Cache.end()) {
      NDB_TICKS now = NdbTick_getCurrentTicks();
      return now;
    }
    return it->second->lastUpdated;
  }

  RS_Status start_update_ticker(const std::string &key,
                                 std::shared_ptr<CacheEntry<T>> /*entry*/,
                                const T &data) {
    auto started = std::make_shared<std::atomic<bool>>(false);
    std::thread([this, key, started, data]() {
      cache_entry_updater(key, started, data); }).detach();

    while (!started->load()) {
      NdbSleep_MilliSleep(50);
    }
    return CRS_Status::SUCCESS.status;
  }

  RS_Status cache_entry_updater(const std::string &key,
                                std::shared_ptr<std::atomic<bool>> started,
                                const T &data) {
    auto it = key2Cache.find(key);
    if (it == key2Cache.end()) {
      return CRS_Status(HTTP_CODE::CLIENT_ERROR,
        "Key not found in cache").status;
    }

    std::shared_ptr<CacheEntry<T>> shared_entry = it->second;  // keeps the entry alive
    CacheEntry<T> *entry = shared_entry.get();
    if (entry == nullptr) {
      return CRS_Status(HTTP_CODE::SERVER_ERROR,
        ("Cache updater failed. Report programming error. Key " +
         key).c_str()).status;
    }
    RS_Status status;

    while (true) {
      pthread_rwlock_wrlock(&entry->rwLock);
      started->store(true);

      if (!entry->evicted) {
        status = update_record(data, entry);
      }

      pthread_rwlock_unlock(&entry->rwLock);

      NdbSleep_MilliSleep(entry->refreshInterval);

      if (entry->evicted) {
        return CRS_Status::SUCCESS.status;
      }

      pthread_rwlock_rdlock(&entry->rwLock);
      auto lastUsed = entry->lastUsed;
      pthread_rwlock_unlock(&entry->rwLock);

      NDB_TICKS now = NdbTick_getCurrentTicks();
      Uint64 milliSeconds = NdbTick_Elapsed(lastUsed, now).milliSec();
      if (milliSeconds >= 
          Uint64(globalConfigs.security.apiKey.cacheUnusedEntriesEvictionMS)) {
        WriteLock lock(key2CacheLock);
        if (entry->refCount <= 0) {
          entry->evicted = true;
          key2Cache.erase(key);
        } else {
          continue;
        }
        break;
      }
    }
    return CRS_Status::SUCCESS.status;
  }

  Int32 refresh_interval_with_jitter() {
    Uint32 cacheRefreshIntervalMS =
      globalConfigs.security.apiKey.cacheRefreshIntervalMS;
    Int32 jitterMS = 
      static_cast<Int32>(globalConfigs.security.apiKey.cacheRefreshIntervalJitterMS);
    std::uniform_int_distribution<Int32> dist(0, jitterMS);
    int32_t jitter = dist(randomGenerator);
    if (jitter % 2 == 0) {
      jitter = -jitter;
    }
    return Int32(cacheRefreshIntervalMS) + jitter;
  }

  std::string to_string() {
    ReadLock readLock(key2CacheLock);
    std::stringstream ss;
    for (const auto &entry : key2Cache) {
      ss << "Key: " << entry.first << ", Data: "
         << entry.second->to_string() << std::endl;
    }
    return ss.str();
  }
};
#endif
