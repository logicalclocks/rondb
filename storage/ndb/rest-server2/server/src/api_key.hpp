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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_API_KEY_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_API_KEY_HPP_

#include "rdrs_hopsworks_dal.h"
#include "config_structs.hpp"
#include "pk_data_structs.hpp"

#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>
#include <boost/thread/lock_types.hpp>
#include <boost/thread/pthread/shared_mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <thread>
#include <chrono>
#include <future>
#include <iostream>
#include <map>
#include <pthread.h>

class ReadLock {
public:
  explicit ReadLock(pthread_rwlock_t& lock) : rwlock(lock) {
    pthread_rwlock_rdlock(&rwlock);
  }
  ~ReadLock() {
    pthread_rwlock_unlock(&rwlock);
  }
  ReadLock(const ReadLock&) = delete;
  ReadLock& operator=(const ReadLock&) = delete;
private:
  pthread_rwlock_t& rwlock;
};

class WriteLock {
public:
  explicit WriteLock(pthread_rwlock_t& lock) : rwlock(lock) {
    pthread_rwlock_wrlock(&rwlock);
  }
  ~WriteLock() {
    pthread_rwlock_unlock(&rwlock);
  }
  WriteLock(const WriteLock&) = delete;
  WriteLock& operator=(const WriteLock&) = delete;
private:
  pthread_rwlock_t& rwlock;
};

RS_Status authenticate(const std::string &apiKey, PKReadParams &params);

class UserDBs {
 public:
  std::unordered_map<std::string, bool> userDBs;      // DBs
  std::chrono::system_clock::time_point lastUsed;     // for removing unused entries
  std::chrono::system_clock::time_point lastUpdated;  // last updated TS
  pthread_rwlock_t rowLock{};                         // this is used to prevent concurrent updates
  bool evicted = false;                               // is evicted or deleted
  std::chrono::milliseconds refreshInterval;          // Cache refresh interval
  std::atomic<int> refCount = 0;                      // Reference count
  
  UserDBs() {
    pthread_rwlock_init(&rowLock, nullptr);
  }

  ~UserDBs() {
    pthread_rwlock_destroy(&rowLock);
  }

  std::string to_string() {
    std::string result = "UserDBs: ";
    for (auto &db : userDBs) {
      result += db.first + " ";
    }
    result += "LastUsed: " + std::to_string(std::chrono::system_clock::to_time_t(lastUsed));
    result += " LastUpdated: " + std::to_string(std::chrono::system_clock::to_time_t(lastUpdated));
    result += " Evicted: " + std::to_string(static_cast<int>(evicted));
    result += " RefreshInterval: " + std::to_string(refreshInterval.count());
    return result;  
  }
};

class Cache {
 public:
  std::unordered_map<std::string, std::shared_ptr<UserDBs>>
      key2UserDBsCache;  // API Key -> User Databases
  pthread_rwlock_t key2UserDBsCacheLock{};
  std::mt19937 randomGenerator;

 public:
  Cache() : key2UserDBsCache(), randomGenerator(std::random_device()()) {
    pthread_rwlock_init(&key2UserDBsCacheLock, nullptr);
  }

  ~Cache() {
    cleanup();
    pthread_rwlock_destroy(&key2UserDBsCacheLock);
  }

  static RS_Status validate_api_key_format(const std::string &);

  /*
  Checking whether the API key can access the given databases
  */
  RS_Status validate_api_key(const std::string &, const std::initializer_list<std::string>&);

  /*
  Checking whether the API key can access the given databases, without caching
  */
  RS_Status validate_api_key_no_cache(const std::string &, const std::initializer_list<std::string>&);

  RS_Status cleanup();

  RS_Status update_cache(const std::string &);

  RS_Status update_record(std::vector<std::string>, UserDBs *);

  std::chrono::system_clock::time_point last_used(const std::string &);

  std::chrono::system_clock::time_point last_updated(const std::string &);

  RS_Status start_update_ticker(const std::string &, std::shared_ptr<UserDBs> &);

  RS_Status cache_entry_updater(const std::string &, std::shared_ptr<std::atomic<bool>>);

  RS_Status find_and_validate(const std::string &, bool &, bool &, const std::initializer_list<std::string>&);

  RS_Status find_and_validate_again(const std::string &, bool &, bool &, const std::initializer_list<std::string>&);

  RS_Status authenticate_user(const std::string &, HopsworksAPIKey &);

  unsigned size();

  RS_Status get_user_databases(HopsworksAPIKey &, std::vector<std::string> &);

  RS_Status get_user_projects(int, std::vector<std::string> &);

  RS_Status get_api_key(const std::string &, HopsworksAPIKey &);

  std::chrono::milliseconds refresh_interval_with_jitter();

  std::string to_string();
};

extern std::shared_ptr<Cache> apiKeyCache;

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_API_KEY_HPP_
