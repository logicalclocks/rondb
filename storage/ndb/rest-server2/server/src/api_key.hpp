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
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
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

RS_Status authenticate_empty(const std::string &apiKey);
RS_Status authenticate(const std::string &apiKey, PKReadParams &params);
RS_Status authenticate(const std::string &apiKey, const std::string_view & db);
RS_Status authenticate(const std::string &apiKey,
                       const std::vector<std::string_view> &);

struct NdbThread;

class UserDBs {
 public:
  std::unordered_set<std::string_view> userDBs;
  char **m_db_ptrs; // Memory to free for database names
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
  std::atomic<int> m_ref_count;

  // Key material for in-memory SHA256 verification
  std::string m_secret;
  std::string m_salt;
  int m_user_id;

  UserDBs() {
    m_db_ptrs = nullptr;
    m_user_id = 0;
    m_waitLock = NdbMutex_Create();
    m_waitCond = NdbCondition_Create();
  }

  ~UserDBs() {
    NdbMutex_Destroy(m_waitLock);
    NdbCondition_Destroy(m_waitCond);
    if (m_db_ptrs) {
      free(m_db_ptrs);
    }
  }
};

class APIKeyCache {
 public:
  APIKeyCache() : m_key_cache() {
    m_refresh_thread = nullptr;
    m_event_watcher_thread = nullptr;
    m_sleepLock = NdbMutex_Create();
    m_sleepCond = NdbCondition_Create();
  }

  ~APIKeyCache() {
    cleanup();
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

  void set_event_name(const std::string &name) { m_event_name = name; }
  void preload_all_keys();
  void start_background_threads();
  void refresh_job();
  void event_watcher_job();
  // Force the event watcher to tear down and reconnect (for testing)
  void force_reconnect() { m_force_reconnect = true; }

 private:
  // Prefix -> User Databases
  std::unordered_map<std::string, UserDBs*> m_key_cache[NUM_API_KEY_CACHES];

  std::atomic<bool> m_stopped{false};
  std::atomic<bool> m_force_reconnect{false};
  // Read-write lock per partition: shared (read) for lookups/snapshots,
  // exclusive (write) for inserts/deletes. This avoids blocking the
  // request path while the refresh thread snapshots a partition.
  std::shared_mutex m_rwLock[NUM_API_KEY_CACHES];
  NdbMutex *m_sleepLock;
  NdbCondition *m_sleepCond;

  struct NdbThread *m_refresh_thread;
  struct NdbThread *m_event_watcher_thread;
  std::string m_event_name;

  void cleanup();
  RS_Status update_cache(const std::string &prefix,
                         const std::string &clientSecret,
                         Uint32 hash);
  RS_Status update_record(std::vector<std::string_view>,
                          UserDBs*,
                          char **db_ptrs);
  RS_Status find_and_validate(const std::string &prefix,
                              const std::string &clientSecret,
                              bool &keyFoundInCache,
                              bool &allowedAccess,
                              const std::vector<std::string_view> &dbs,
                              Uint32 hash,
                              bool inc_refcount_done);

  static RS_Status verify_api_key_hash(const std::string &clientSecret,
                                       const std::string &secret,
                                       const std::string &salt);
  void load_single_key(const std::string &prefix,
                       const std::string &secret,
                       const std::string &salt,
                       int user_id);
  RS_Status get_user_databases(int user_id,
                               std::vector<std::string_view> &dbs,
                               char ***db_ptrs);
  Int32 refresh_interval();
};
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_API_KEY_HPP_
