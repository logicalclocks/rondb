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
#include "pk_data_structs.hpp"
#include "ronsql_data_structs.hpp"

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
#include <NdbTick.h>
#include <NdbSleep.h>
#include <NdbMutex.h>
#include <NdbCondition.h>

#define NUM_API_KEY_CACHES 1

static_assert(0 < NUM_API_KEY_CACHES,
              "NUM_API_KEY_CACHES must be greater than zero");
static_assert(NUM_API_KEY_CACHES < INT32_MAX,
              "NUM_API_KEY_CACHES must be less than INT32_MAX");
static_assert((NUM_API_KEY_CACHES & (NUM_API_KEY_CACHES - 1)) == 0,
              "NUM_API_KEY_CACHES must be a power of two");

class APIKeyCache;
APIKeyCache* start_api_key_cache();
void stop_api_key_cache();

RS_Status authenticate(const std::string &apiKey, PKReadParams &params);
RS_Status authenticate(const std::string &apiKey, const std::string_view & db);
RS_Status authenticate(const std::string &apiKey,
                       const std::vector<std::string_view> &);

class UserDBs {
 public:
  std::unordered_map<std::string_view, bool> userDBs;
  char *m_db_ptrs; // Memory to free for database names
  NDB_TICKS m_lastUsed;
  NDB_TICKS m_lastUpdated;
  NdbMutex *m_waitLock;
  NdbCondition *m_waitCond;
  enum {
    IS_VALIDATING = 0,
    IS_INVALID = 1,
    IS_VALID = 2
  };
  Uint8 m_state;
  Int32 m_refresh_interval;
  std::atomic<int> m_ref_count;

  UserDBs() {
    m_db_ptrs = nullptr;
    m_waitLock = NdbMutex_Create();
    m_waitCond = NdbCondition_Create();
  }

  ~UserDBs() {
    NdbMutex_Destroy(m_waitLock);
    NdbCondition_Destroy(m_waitCond);
    free(m_db_ptrs);
  }
};

class APIKeyCache {
 public:
  APIKeyCache() : m_key_cache(), randomGenerator(std::random_device()()) {
    m_evicted = false;
    for (int i = 0; i < NUM_API_KEY_CACHES; i++)
      m_rwLock[i] = NdbMutex_Create();
    m_sleepLock = NdbMutex_Create();
    m_sleepCond = NdbCondition_Create();
  }

  ~APIKeyCache() {
    cleanup();
    for (int i = 0; i < NUM_API_KEY_CACHES; i++)
      NdbMutex_Destroy(m_rwLock[i]);
    NdbMutex_Destroy(m_sleepLock);
    NdbCondition_Destroy(m_sleepCond);
  }

  /*
  Checking whether the API key can access the given databases
  */
  RS_Status validate_api_key(const std::string &,
                             const std::vector<std::string_view> &);

  Uint64 last_updated(const std::string &);
  std::string to_string();
  unsigned size();

  void cache_entry_updater(const std::string &);

 private:
  // API Key -> User Databases
  std::unordered_map<std::string, UserDBs*> m_key_cache[NUM_API_KEY_CACHES];
  std::mt19937 randomGenerator;

  bool m_evicted = false;
  NdbMutex *m_rwLock[NUM_API_KEY_CACHES];
  NdbMutex *m_sleepLock;
  NdbCondition *m_sleepCond;

  static RS_Status validate_api_key_format(const std::string &);

  void cleanup();
  RS_Status update_cache(const std::string &, Uint32 hash);
  RS_Status update_record(std::vector<std::string_view>,
                          UserDBs*,
                          char **db_ptrs);
  RS_Status find_and_validate(const std::string &,
                              bool &,
                              bool &,
                              const std::vector<std::string_view> &,
                              Uint32 hash,
                              bool);

  RS_Status authenticate_user(const std::string &, HopsworksAPIKey &);
  RS_Status get_user_databases(HopsworksAPIKey &,
                               std::vector<std::string_view> &,
                               char ***db_ptrs);
  RS_Status get_user_projects(int,
                              std::vector<std::string_view> &,
                              char ***db_ptrs);
  RS_Status get_api_key(const std::string &, HopsworksAPIKey &);
  Int32 refresh_interval_with_jitter();
};
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_API_KEY_HPP_
