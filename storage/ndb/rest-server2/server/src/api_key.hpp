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

/*
One access check of a request against the caller's grants:
- metadata_only: visibility check - the caller may resolve feature-view
  metadata from the db (member, restricted member, shared or
  placeholder-shared store, or any table grant). Grants NO data access;
  table and columns are ignored and must be left empty.
- table empty: full-database access required
- table set, columns == nullptr: the request reads the whole row, so the
  whole table must be granted (full db or whole-table grant)
- table set, columns set: every listed column must be granted
The string_views must outlive the authenticate call.
*/
struct TableAccessRequest {
  std::string_view db;
  std::string_view table;
  const std::vector<std::string_view> *columns = nullptr;
  bool metadata_only = false;
};

namespace metadata {
struct FeatureViewMetadata;
}

/*
RONDB-978 username-mode rate limiting: the identity a transaction is
tagged with is the Hopsworks project-user, using the online feature
store MySQL account convention so that REST and MySQL traffic of the
same (project, member) share one kernel rate limit bucket:
  identity = projectname + "_" + users.username,
  clipped to 31 chars ONLY when longer than 32
The clip reproduces OnlineFeaturestoreController.onlineDbUsername
exactly: substring(0, MAX - 1) under a length > MAX guard with MAX =
32, so a 32-char name stays 32 and longer names become 31. Do not
"fix" the off-by-one or its boundary, or the identities stop matching
the accounts Hopsworks provisions.
*/
constexpr size_t RATE_LIMIT_IDENTITY_MAX_LEN = 32;

/*
Builds that identity for one (project, member) pair. project must carry
the project's ORIGINAL case (the MySQL account does) while username is
hopsworks.users.username. The clip boundary is covered by
test/api_key_test.cpp - change it there first if it ever has to move.
*/
std::string make_rate_limit_identity(const std::string &project,
                                     const std::string &username);

/*
Rate limit identities resolved during authentication (username mode).
Scoped to ONE api key, i.e. one owning user: each entry maps a database
the request referenced to the identity of the KEY'S OWNER acting in
that database's project ("<ProjectName>_<username>"). The key is the
lowercased project name, which is simultaneously the project's online
database name (pk-read/scan URLs) and its feature store name
(feature-store requests) - Hopsworks derives both by lowercasing the
project name. Only projects the owner is a MEMBER of have an entry;
other databases (shared stores, system dbs) are absent and their
requests run unmetered.
*/
struct RateLimitIdentities {
  std::unordered_map<std::string, std::string> per_db;
};

RS_Status authenticate_empty(const std::string &apiKey);
RS_Status authenticate(const std::string &apiKey, PKReadParams &params,
                       RateLimitIdentities *rlIdentities = nullptr);
RS_Status authenticate(const std::string &apiKey, const std::string_view & db);
RS_Status authenticate(const std::string &apiKey,
                       const std::vector<std::string_view> &);
RS_Status authenticate(const std::string &apiKey,
                       const std::vector<TableAccessRequest> &,
                       RateLimitIdentities *rlIdentities = nullptr);
RS_Status authenticate(const std::string &apiKey,
                       const metadata::FeatureViewMetadata &fvMetadata,
                       RateLimitIdentities *rlIdentities = nullptr);

struct NdbThread;

class UserDBs {
 public:
  // Full-database data grants: member projects + stores shared entirely
  std::unordered_set<std::string> userDBs;
  // Feature-view metadata visibility only: restricted memberships and
  // placeholder store shares (shared_feature_store.shared_entirely = 0)
  std::unordered_set<std::string> visibleDBs;
  // Table/column grants: db -> table -> granted columns
  // (an empty column set means the whole table is granted)
  std::unordered_map<std::string,
    std::unordered_map<std::string,
      std::unordered_set<std::string>>> fineGrants;
  // Precomputed project-user rate limit identities (RONDB-978): one entry
  // per member project, keyed by the project's lowercased name - which
  // serves both the online db of pk-read/scan URLs and the feature store
  // name of feature-store requests. Value is
  // make_rate_limit_identity(ProjectName, username). RDRS never bills the
  // Hive offline "<project>_featurestore" db, so it has no entry.
  std::unordered_map<std::string, std::string> rlIdentities;
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
  // api_key.expiry as unix epoch seconds; 0 = NULL = never expires
  long long m_expiry_epoch;

  UserDBs() {
    m_user_id = 0;
    m_expiry_epoch = 0;
    m_waitLock = NdbMutex_Create();
    m_waitCond = NdbCondition_Create();
  }

  ~UserDBs() {
    NdbMutex_Destroy(m_waitLock);
    NdbCondition_Destroy(m_waitCond);
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
  /*
  Checking whether the API key satisfies the given table/column access
  requests. On success rlIdentities (when non-null) receives the
  project-user identities for the requested databases (username mode).
  */
  RS_Status validate_api_key(const std::string &,
                             const std::vector<TableAccessRequest> &,
                             RateLimitIdentities *rlIdentities = nullptr);

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
  RS_Status update_record(HopsworksUserGrants &grants,
                          UserDBs*);
  RS_Status find_and_validate(const std::string &prefix,
                              const std::string &clientSecret,
                              bool &keyFoundInCache,
                              bool &allowedAccess,
                              const std::vector<TableAccessRequest> &accessReqs,
                              Uint32 hash,
                              bool inc_refcount_done,
                              RateLimitIdentities *rlIdentities);

  static RS_Status verify_api_key_hash(const std::string &clientSecret,
                                       const std::string &secret,
                                       const std::string &salt);
  void load_single_key(const std::string &prefix,
                       const std::string &secret,
                       const std::string &salt,
                       int user_id,
                       long long expiry_epoch);
  RS_Status get_user_databases(int user_id,
                               HopsworksUserGrants &grants);
  Int32 refresh_interval();
};
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_API_KEY_HPP_
