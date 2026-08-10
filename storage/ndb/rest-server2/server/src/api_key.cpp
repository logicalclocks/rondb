/*
 * Copyright (C) 2024, 2025 Hopsworks AB
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

#include "api_key.hpp"
#include "config_structs.hpp"
#include "metadata.hpp"
#include "ndb_event_utils.hpp"
#include "pk_data_structs.hpp"
#include "rdrs_dal.hpp"
#include "rdrs_rondb_connection_pool.hpp"
#include "ndb_api_helper.hpp"
#include "db_operations/pk/common.hpp"

#include <chrono>
#include <ctime>
#include <iostream>
#include <memory>
#include <openssl/evp.h>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <functional>
#include <algorithm>
#include <NdbThread.h>
#include <NdbApi.hpp>
#include <util/require.h>
#include <EventLogger.hpp>
#include <util/rondb_hash.hpp>

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_AUTH 1
//#define DEBUG_AUTH_THREAD 1
//#define DEBUG_AUTH_TIME 1
//#define DEBUG_AUTH_DBS 1
#endif

#ifdef DEBUG_AUTH
#define DEB_AUTH(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_AUTH(...) do { } while (0)
#endif

#ifdef DEBUG_AUTH_THREAD
#define DEB_AUTH_THREAD(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_AUTH_THREAD(...) do { } while (0)
#endif

#ifdef DEBUG_AUTH_TIME
#define DEB_AUTH_TIME(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_AUTH_TIME(...) do { } while (0)
#endif

#ifdef DEBUG_AUTH_DBS
#define DEB_AUTH_DBS(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_AUTH_DBS(...) do { } while (0)
#endif

#ifndef DEBUG_AUTH_THREAD
#define CLEANUP_SLEEP_TIME 10
#else
#define CLEANUP_SLEEP_TIME 1000
#endif
extern EventLogger *g_eventLogger;

APIKeyCache *apiKeyCache = nullptr;

// RonDB connection pool
extern RDRSRonDBConnectionPool *rdrsRonDBConnectionPool;

bool contains_upper(std::string_view s);
RS_Status computeHash(const std::string &unhashed, std::string &hashed);

/* On reconnection the event watcher must release its metadata Ndb object
 * (it holds a live event subscription) or the connection teardown cannot
 * converge. TE_CLUSTER_FAILURE delivery alone is not reliable enough. */
static void api_key_cache_reconnect_listener() {
  if (apiKeyCache != nullptr) {
    apiKeyCache->force_reconnect();
  }
}

APIKeyCache* start_api_key_cache() {
  apiKeyCache = new APIKeyCache();
  require(apiKeyCache != nullptr);
  apiKeyCache->set_event_name("RDRS_AK_EVT_" + generate_event_uuid());
  DEB_AUTH("API Key Cache started: %p", apiKeyCache);
  RDRSRonDBConnection::RegisterReconnectListener(
    api_key_cache_reconnect_listener);
  return apiKeyCache;
}

void stop_api_key_cache() {
  DEB_AUTH("API Key Cache stopped: %p", apiKeyCache);
  /* Null the global before destroying so the reconnect listener (which
   * may fire from another thread during a cluster failure) sees null
   * instead of a half-destroyed cache. */
  APIKeyCache *cache = apiKeyCache;
  apiKeyCache = nullptr;
  delete cache;
}

void APIKeyCache::cleanup() {
  DEB_AUTH("Cleanup started");

  /* Signal background threads to exit */
  NdbMutex_Lock(m_sleepLock);
  for (int i = 0; i < NUM_API_KEY_CACHES; i++)
    m_rwLock[i].lock();
  m_stopped = true;
  NdbCondition_Broadcast(m_sleepCond);
  NdbMutex_Unlock(m_sleepLock);
  for (int i = 0; i < NUM_API_KEY_CACHES; i++)
    m_rwLock[i].unlock();

  /* Wait for background threads to finish */
  if (m_refresh_thread != nullptr) {
    void *status;
    NdbThread_WaitFor(m_refresh_thread, &status);
    NdbThread_Destroy(&m_refresh_thread);
    m_refresh_thread = nullptr;
  }
  if (m_event_watcher_thread != nullptr) {
    void *status;
    NdbThread_WaitFor(m_event_watcher_thread, &status);
    NdbThread_Destroy(&m_event_watcher_thread);
    m_event_watcher_thread = nullptr;
  }

  /* Delete all cache entries - wait for ref_count == 0 */
  for (int i = 0; i < NUM_API_KEY_CACHES; i++) {
    m_rwLock[i].lock();
    bool all_clear = false;
    while (!all_clear) {
      all_clear = true;
      for (auto &kv : m_key_cache[i]) {
        if (kv.second->m_ref_count > 0) {
          all_clear = false;
          break;
        }
      }
      if (!all_clear) {
#ifdef DEBUG_AUTH
        Uint32 key_cache_size = (Uint32)m_key_cache[i].size();
#endif
        m_rwLock[i].unlock();
        DEB_AUTH("m_key_cache[%d].size() = %u, waiting for ref_count",
                 i, key_cache_size);
        NdbSleep_MilliSleep(CLEANUP_SLEEP_TIME);
        m_rwLock[i].lock();
      }
    }
    for (auto &kv : m_key_cache[i]) {
      delete kv.second;
    }
    m_key_cache[i].clear();
    m_rwLock[i].unlock();
  }
  DEB_AUTH("Cleanup finished");
}

RS_Status authenticate(const std::string &apiKey, PKReadParams &params) {
  std::vector<std::string_view> columns;
  TableAccessRequest accessReq;
  accessReq.db = params.path.db;
  accessReq.table = params.path.table;
  if (params.readColumns.empty()) {
    // no explicit read columns = the whole row is returned
    accessReq.columns = nullptr;
  } else {
    // filter (primary key) columns expose data too - check them like the
    // batch and scan endpoints do
    columns.reserve(params.readColumns.size() + params.filters.size());
    for (const PKReadReadColumn &readColumn : params.readColumns) {
      columns.push_back(readColumn.column);
    }
    for (const PKReadFilter &filter : params.filters) {
      columns.push_back(filter.column);
    }
    accessReq.columns = &columns;
  }
  return apiKeyCache->validate_api_key(apiKey,
    std::vector<TableAccessRequest>{accessReq});
}

RS_Status authenticate_empty(const std::string &apiKey) {
  return apiKeyCache->validate_api_key(apiKey,
    std::vector<std::string_view>{});
}

RS_Status authenticate(const std::string &apiKey, const std::string_view & db) {
  return apiKeyCache->validate_api_key(apiKey,
    std::vector<std::string_view>{db});
}

RS_Status authenticate(const std::string &apiKey,
                       const std::vector<std::string_view> &dbs) {
  return apiKeyCache->validate_api_key(apiKey, dbs);
}

RS_Status authenticate(const std::string &apiKey,
                       const std::vector<TableAccessRequest> &accessReqs) {
  return apiKeyCache->validate_api_key(apiKey, accessReqs);
}

/*
Serving a feature view requires the FV's own store to be at least visible
(members, restricted members, shared and placeholder-shared stores) and
every constituent feature group's online table to be readable for exactly
the features the FV serves. Spine feature groups have no online table -
their store only needs visibility.
*/
RS_Status authenticate(const std::string &apiKey,
                       const metadata::FeatureViewMetadata &fvMetadata) {
  const auto &fgFeatures = fvMetadata.featureGroupFeatures;
  // owners of the table-name strings and column lists the requests point at
  std::vector<std::string> tableNames;
  std::vector<std::vector<std::string_view>> columnLists;
  tableNames.reserve(fgFeatures.size());
  columnLists.reserve(fgFeatures.size());

  std::vector<TableAccessRequest> accessReqs;
  accessReqs.reserve(fgFeatures.size() + 1);
  TableAccessRequest storeReq;
  storeReq.db = fvMetadata.featureStoreName;
  storeReq.metadata_only = true;
  accessReqs.push_back(storeReq);
  for (const metadata::FeatureGroupFeatures &fgf : fgFeatures) {
    TableAccessRequest accessReq;
    accessReq.db = fgf.featureStoreName;
    if (fgf.isSpine()) {
      accessReq.metadata_only = true;
      accessReqs.push_back(accessReq);
      continue;
    }
    tableNames.push_back(fgf.featureGroupName + "_" +
                         std::to_string(fgf.featureGroupVersion));
    accessReq.table = tableNames.back();
    columnLists.emplace_back();
    columnLists.back().reserve(fgf.features.size());
    for (const metadata::FeatureMetadata &feature : fgf.features) {
      columnLists.back().push_back(feature.name);
    }
    accessReq.columns = &columnLists.back();
    accessReqs.push_back(accessReq);
  }
  return apiKeyCache->validate_api_key(apiKey, accessReqs);
}

/*
Database-only access checks: full-database access required for each db
*/
RS_Status APIKeyCache::validate_api_key(const std::string &apiKey,
                                        const std::vector<std::string_view> &dbs) {
  std::vector<TableAccessRequest> accessReqs;
  accessReqs.reserve(dbs.size());
  for (const std::string_view &db : dbs) {
    TableAccessRequest accessReq;
    accessReq.db = db;
    accessReqs.push_back(accessReq);
  }
  return validate_api_key(apiKey, accessReqs);
}

RS_Status APIKeyCache::validate_api_key(const std::string &apiKey,
                                        const std::vector<TableAccessRequest> &accessReqs) {
#ifdef DEBUG_AUTH
  DEB_AUTH("authenticate apiKey: %s", apiKey.c_str());
  for (const auto &accessReq : accessReqs) {
    DEB_AUTH("validate db: %s table: %s",
             std::string(accessReq.db).c_str(),
             std::string(accessReq.table).c_str());
  }
#endif

  // Parse once: extract prefix (first 16 chars) and client secret
  if (apiKey.empty()) {
    return CRS_Status(HTTP_CODE::CLIENT_ERROR, "the apikey is nil").status;
  }
  auto dotPos = apiKey.find('.');
  if (dotPos != 16 || dotPos + 1 >= apiKey.size()) {
    DEB_AUTH("Failed incorrect format, Line: %u", __LINE__);
    return CRS_Status(HTTP_CODE::CLIENT_ERROR,
                      "the apikey has an incorrect format").status;
  }
  std::string prefix = apiKey.substr(0, dotPos);
  std::string clientSecret = apiKey.substr(dotPos + 1);

#if (NUM_API_KEY_CACHES == 1)
  Uint32 hash = 0;
#else
  Uint32 hash = rondb_xxhash_std(prefix.c_str(), prefix.size());
#endif

  // First try: look up by prefix in cache
  bool keyFoundInCache = false;
  bool allowedAccess = false;
  RS_Status status = find_and_validate(prefix,
                                       clientSecret,
                                       keyFoundInCache,
                                       allowedAccess,
                                       accessReqs,
                                       hash,
                                       false);

  if (keyFoundInCache) {
    if (allowedAccess) {
      return CRS_Status::SUCCESS.status;
    } else {
      return status;
    }
  }

  /**
   * Lazy-load fallback: fetch the API key from backend.
   * Only reached if the API Key wasn't found in cache
   * (e.g., created between startup scan and event subscription).
   */
  status = update_cache(prefix, clientSecret, hash);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }

  // Second try after lazy load
  status = find_and_validate(prefix,
                             clientSecret,
                             keyFoundInCache,
                             allowedAccess,
                             accessReqs,
                             hash,
                             true);
  return status;
}

RS_Status APIKeyCache::verify_api_key_hash(const std::string &clientSecret,
                                           const std::string &secret,
                                           const std::string &salt) {
  // sha256(clientSecret + stored_salt) should == stored_secret
  std::string unhashed = clientSecret + salt;
  std::string hashed;
  RS_Status status = computeHash(unhashed, hashed);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }
  if (hashed != secret) {
    return CRS_Status(HTTP_CODE::CLIENT_ERROR, "bad API key").status;
  }
  return CRS_Status::SUCCESS.status;
}

/*
Checks one access request against the cached grants of the key user.
The grant ladder mirrors the per-user MySQL GRANTs Hopsworks maintains for
online reads: full database (member / store shared entirely), whole table
(FG shared or restricted-granted entirely) and column subset. On denial the
returned message names the blocking object - database, table or column(s) -
like MySQL errors 1044/1142/1143 do.

The three grant tiers live in UserDBs (see api_key.hpp): userDBs (full-db),
visibleDBs (FV-metadata visibility only) and fineGrants
(db -> table -> columns, where an EMPTY column set means the whole table).
The request shape is encoded in TableAccessRequest (see api_key.hpp).

Fail-closed by construction: access_ok starts false and is only ever set true
at one of three explicit allow points (steps 2, 6, 8); every other exit is a
401. Names are lowercased before comparison (Hopsworks compares them case-
insensitively). The branches, in order:

  1. Default deny (access_ok = false).
  2. Tier 1 - full database: db in userDBs -> ALLOW (columns irrelevant).
  3. metadata_only request: ALLOW iff db in visibleDBs or the key has any
     fineGrants entry for db (any relationship lets the caller resolve FV
     metadata); otherwise deny naming the database. This branch never grants
     row data.
  4. No data grant in this db: table empty (db-level request) or db absent
     from fineGrants -> deny naming the database (MySQL 1044).
  5. Table not granted: table absent from fineGrants[db] -> deny naming
     db/table (MySQL 1142).
  6. Tier 2 - whole table: fineGrants[db][table] is the EMPTY set -> ALLOW.
     (The DAL guarantees a partial grant always carries >=1 column, so an
     empty set unambiguously means "whole table", never an orphaned partial
     grant - see find_fine_grained_grants_int.)
  7. Whole-row read against a column subset: grant is a non-empty subset but
     accessReq.columns == nullptr -> deny "all columns" (this is what makes a
     SELECT *-style read fail closed on a column-granted table).
  8. Tier 3 - per column: any requested column not in the granted set -> deny
     naming the column(s) (MySQL 1143); otherwise ALLOW.
*/
static RS_Status check_access(const UserDBs *userDBs,
                              const TableAccessRequest &accessReq,
                              bool *access_ok) {
  *access_ok = false;
  // in HW database name comparison is case insensitive
  std::string lower_db = std::string(accessReq.db);
  if (contains_upper(lower_db)) {
    std::transform(lower_db.begin(), lower_db.end(), lower_db.begin(),
               [](unsigned char c) { return std::tolower(c); });
  }
  if (userDBs->userDBs.find(lower_db) != userDBs->userDBs.end()) {
    *access_ok = true;
    return CRS_Status::SUCCESS.status;
  }
  auto db_grants = userDBs->fineGrants.find(lower_db);
  if (accessReq.metadata_only) {
    // feature-view metadata resolution: any relationship with the db is
    // enough (restricted membership, placeholder share or a table grant)
    if (userDBs->visibleDBs.find(lower_db) != userDBs->visibleDBs.end() ||
        db_grants != userDBs->fineGrants.end()) {
      *access_ok = true;
      return CRS_Status::SUCCESS.status;
    }
    return CRS_Status(HTTP_CODE::AUTH_ERROR,
      ("API key not authorized to access " +
      std::string(accessReq.db)).c_str()).status;
  }
  if (accessReq.table.empty() || db_grants == userDBs->fineGrants.end()) {
    // database-only request, or no grant at all in this database
    return CRS_Status(HTTP_CODE::AUTH_ERROR,
      ("API key not authorized to access " +
      std::string(accessReq.db)).c_str()).status;
  }
  std::string lower_table = std::string(accessReq.table);
  if (contains_upper(lower_table)) {
    std::transform(lower_table.begin(), lower_table.end(), lower_table.begin(),
               [](unsigned char c) { return std::tolower(c); });
  }
  auto table_grant = db_grants->second.find(lower_table);
  if (table_grant == db_grants->second.end()) {
    return CRS_Status(HTTP_CODE::AUTH_ERROR,
      ("API key not authorized to access " + std::string(accessReq.db) +
      "/" + std::string(accessReq.table)).c_str()).status;
  }
  const std::unordered_set<std::string> &granted_columns = table_grant->second;
  if (granted_columns.empty()) {
    // the whole table is granted
    *access_ok = true;
    return CRS_Status::SUCCESS.status;
  }
  if (accessReq.columns == nullptr) {
    return CRS_Status(HTTP_CODE::AUTH_ERROR,
      ("API key not authorized to access all columns of " +
      std::string(accessReq.db) + "/" +
      std::string(accessReq.table)).c_str()).status;
  }
  std::string denied_columns;
  for (const std::string_view &column : *accessReq.columns) {
    std::string lower_column = std::string(column);
    if (contains_upper(lower_column)) {
      std::transform(lower_column.begin(), lower_column.end(),
                 lower_column.begin(),
                 [](unsigned char c) { return std::tolower(c); });
    }
    if (granted_columns.find(lower_column) == granted_columns.end()) {
      if (!denied_columns.empty()) {
        denied_columns += ", ";
      }
      denied_columns += std::string(column);
    }
  }
  if (!denied_columns.empty()) {
    return CRS_Status(HTTP_CODE::AUTH_ERROR,
      ("API key not authorized to access column(s) " + denied_columns +
      " of " + std::string(accessReq.db) + "/" +
      std::string(accessReq.table)).c_str()).status;
  }
  *access_ok = true;
  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::find_and_validate(const std::string &prefix,
                                         const std::string &clientSecret,
                                         bool &keyFoundInCache,
                                         bool &allowedAccess,
                                         const std::vector<TableAccessRequest> &accessReqs,
                                         Uint32 hash,
                                         bool inc_refcount_done) {
  Uint32 key_cache_id = hash & (NUM_API_KEY_CACHES - 1);
  m_rwLock[key_cache_id].lock_shared();
  if (m_stopped) {
    m_rwLock[key_cache_id].unlock_shared();
    keyFoundInCache = true; // Make sure we return without inserting it
    DEB_AUTH("API Key cache shutdown, Line: %u", __LINE__);
    return CRS_Status(HTTP_CODE::SERVER_ERROR,
      "API Key cache is shutting down").status;
  }
  auto it = m_key_cache[key_cache_id].find(prefix);
  if (it == m_key_cache[key_cache_id].end()) {
    m_rwLock[key_cache_id].unlock_shared();
    require(!inc_refcount_done);
    DEB_AUTH("API Key prefix not found, Line: %u", __LINE__);
    return CRS_Status(HTTP_CODE::AUTH_ERROR,
      "API key not found in cache").status;
  }
  auto userDBs = it->second;

  keyFoundInCache = true;
  if (userDBs == nullptr) {
    m_rwLock[key_cache_id].unlock_shared();
    require(!inc_refcount_done);
    DEB_AUTH("API Key found UserDBs null, Line: %u", __LINE__);
    return CRS_Status(HTTP_CODE::AUTH_ERROR,
      "API key found in cache but userDBs is null").status;
  }
  NdbMutex_Lock(userDBs->m_waitLock);
  m_rwLock[key_cache_id].unlock_shared();
  if (userDBs->m_state == UserDBs::IS_INVALID) {
    if (inc_refcount_done) userDBs->m_ref_count--;
#ifdef DEBUG_AUTH
    int ref_count = userDBs->m_ref_count;
#endif
    NdbMutex_Unlock(userDBs->m_waitLock);
    DEB_AUTH("API Key found invalid, Line: %u, refCount: %d",
             __LINE__, ref_count);
    return CRS_Status(HTTP_CODE::AUTH_ERROR,
      "API key found in cache but is invalid").status;
  }
  while (userDBs->m_state == UserDBs::IS_VALIDATING) {
    if (!inc_refcount_done) {
      inc_refcount_done = true;
      userDBs->m_ref_count++;
    }
    NdbCondition_Wait(userDBs->m_waitCond, userDBs->m_waitLock);
  }
  if (userDBs->m_state == UserDBs::IS_INVALID) {
    if (inc_refcount_done) userDBs->m_ref_count--;
#ifdef DEBUG_AUTH
    int ref_count = userDBs->m_ref_count;
#endif
    NdbMutex_Unlock(userDBs->m_waitLock);
    DEB_AUTH("API Key found invalid, Line: %u, refCount: %d",
             __LINE__, ref_count);
    return CRS_Status(HTTP_CODE::AUTH_ERROR,
      "API key found in cache but is invalid").status;
  }
  require(userDBs->m_state == UserDBs::IS_VALID);
  userDBs->m_lastUsed = NdbTick_getCurrentTicks();

  // Copy secret+salt+expiry out so we can verify outside the lock
  std::string secret = userDBs->m_secret;
  std::string salt = userDBs->m_salt;
  long long expiry_epoch = userDBs->m_expiry_epoch;

  // Check database/table/column access while we hold the lock.
  // Store the result instead of returning early — the hash check must
  // come first to prevent a timing side-channel where an attacker with
  // a valid prefix but wrong secret can enumerate authorized databases.
  bool db_access_ok = true;
  RS_Status db_error_status = CRS_Status::SUCCESS.status;
  for (const TableAccessRequest &accessReq : accessReqs) {
    bool access_ok = false;
    RS_Status access_status = check_access(userDBs, accessReq, &access_ok);
    if (!access_ok) {
      db_access_ok = false;
      db_error_status = access_status;
      DEB_AUTH("API Key not authorized for db: %s, Line: %u",
               std::string(accessReq.db).c_str(), __LINE__);
      break;
    }
  }
  // Release the per-entry lock before SHA256 computation
  if (inc_refcount_done) userDBs->m_ref_count--;
  NdbMutex_Unlock(userDBs->m_waitLock);

  // Verify API key hash (SHA256) outside the lock — the most expensive
  // operation on the hot path. Uses copied secret+salt so no lock needed.
  // IMPORTANT: Hash is checked BEFORE returning DB access errors to prevent
  // timing side-channel (unauthenticated requests must not learn which
  // databases a key has access to).
  RS_Status hashStatus = verify_api_key_hash(clientSecret, secret, salt);
  if (hashStatus.http_code != HTTP_CODE::SUCCESS) {
    DEB_AUTH("API Key hash mismatch, Line: %u", __LINE__);
    return hashStatus;
  }

  // Expiry check mirrors Hopsworks (ApiKeyUtilities.getApiKeyCheckingExpiry):
  // NULL never expires, otherwise expired when strictly before now. Checked
  // after the hash so an unauthenticated caller cannot probe expiry dates,
  // and before authorization errors like Hopsworks checks it before scopes.
  if (expiry_epoch != 0 && expiry_epoch < (long long)time(nullptr)) {
    DEB_AUTH("API Key expired, Line: %u", __LINE__);
    return CRS_Status(HTTP_CODE::AUTH_ERROR, "API key has expired").status;
  }

  if (!db_access_ok) {
    return db_error_status;
  }

  allowedAccess = true;
  DEB_AUTH("API Key found valid, success, Line: %u", __LINE__);
  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::update_cache(const std::string &prefix,
                                    const std::string &clientSecret,
                                    Uint32 hash) {
  // clientSecret is not used here — hash verification happens in
  // find_and_validate() after the cache lookup.
  (void)clientSecret;
  Uint32 key_cache_id = hash & (NUM_API_KEY_CACHES - 1);
  m_rwLock[key_cache_id].lock();  // exclusive: may insert
  // Check if the key exists, might have been entered since we released lock
  auto it = m_key_cache[key_cache_id].find(prefix);
  if (it != m_key_cache[key_cache_id].end()) {
    // Entry already exists (possibly being validated by another thread).
    // Safe to increment m_ref_count without m_waitLock because we hold
    // exclusive m_rwLock, which prevents the refresh thread's eviction
    // (which also requires exclusive m_rwLock) from deleting this entry.
    it->second->m_ref_count++;
    m_rwLock[key_cache_id].unlock();
    return CRS_Status::SUCCESS.status;
  }

  // Create new entry in IS_VALIDATING state
  auto newUserDBs = new UserDBs();
  // Start with ref_count=1 so the refresh thread won't evict this entry
  // while we release the lock and do the slow DB fetch below.  Decremented
  // back to 0 after the entry state is set to IS_VALID/IS_INVALID.
  newUserDBs->m_ref_count = 1;
  newUserDBs->m_state = UserDBs::IS_VALIDATING;
  m_key_cache[key_cache_id][prefix] = newUserDBs;
  DEB_AUTH("API Key prefix %s inserted in cache with refCount: 1",
           prefix.c_str());
  m_rwLock[key_cache_id].unlock();

  // Validate inline: fetch key data from DB
  bool fail = false;
  HopsworksAPIKey key;
  if (!m_stopped) {
    RS_Status status = find_api_key(prefix.c_str(), &key);
    if (status.http_code != HTTP_CODE::SUCCESS) {
      DEB_AUTH("find_api_key failed for prefix '%s': http_code=%d, message=%s",
               prefix.c_str(), status.http_code, status.message);
      fail = true;
    }
  } else {
    fail = true;
  }

  HopsworksUserGrants grants;
  if (!fail && !m_stopped) {
    RS_Status status = get_user_databases(key.user_id, grants);
    if (status.http_code != HTTP_CODE::SUCCESS) {
      DEB_AUTH("get_user_databases failed for user_id=%d: http_code=%d, message=%s",
               key.user_id, status.http_code, status.message);
      fail = true;
    }
  }

  NDB_TICKS now = NdbTick_getCurrentTicks();
  NdbMutex_Lock(newUserDBs->m_waitLock);
  if (!fail && !m_stopped) {
    // Guard against resurrection: a DELETE event may have marked this entry
    // IS_INVALID while we were resolving permissions (between releasing
    // m_rwLock and acquiring m_waitLock). Don't overwrite back to IS_VALID.
    if (newUserDBs->m_state == UserDBs::IS_INVALID) {
      DEB_AUTH("Lazy load raced with DELETE event for prefix: %s", prefix.c_str());
    } else {
      newUserDBs->m_secret = key.secret;
      newUserDBs->m_salt = key.salt;
      newUserDBs->m_user_id = key.user_id;
      newUserDBs->m_expiry_epoch = key.expiry_epoch;
      newUserDBs->m_lastUsed = now;
      newUserDBs->m_state = UserDBs::IS_VALID;
      update_record(grants, newUserDBs);
      DEB_AUTH("Valid API Key inserted via lazy load: %s", prefix.c_str());
    }
  } else {
    newUserDBs->m_lastUsed = now;
    newUserDBs->m_lastUpdated = now;
    newUserDBs->m_state = UserDBs::IS_INVALID;
    DEB_AUTH("Invalid API Key via lazy load: %s", prefix.c_str());
  }
  NdbCondition_Broadcast(newUserDBs->m_waitCond);
  NdbMutex_Unlock(newUserDBs->m_waitLock);

  return CRS_Status::SUCCESS.status;
}

// lower case in place: name comparisons are case insensitive in HW
static void to_lower(std::string &str) {
  if (contains_upper(str)) {
    std::transform(str.begin(), str.end(), str.begin(),
               [](unsigned char c) { return std::tolower(c); });
  }
}

RS_Status APIKeyCache::update_record(HopsworksUserGrants &grants,
                                     UserDBs *userDBs) {
  NDB_TICKS lastUpdated = NdbTick_getCurrentTicks();
  userDBs->userDBs.clear();
  for (std::string &db : grants.full_dbs) {
    to_lower(db);
    userDBs->userDBs.insert(std::move(db));
  }
  userDBs->visibleDBs.clear();
  for (std::string &db : grants.visible_dbs) {
    to_lower(db);
    userDBs->visibleDBs.insert(std::move(db));
  }
  userDBs->fineGrants.clear();
  for (HopsworksFineGrant &grant : grants.fine_grants) {
    to_lower(grant.db);
    to_lower(grant.table);
    auto &tables = userDBs->fineGrants[grant.db];
    auto table_it = tables.find(grant.table);
    if (table_it == tables.end()) {
      table_it = tables.emplace(grant.table,
                                std::unordered_set<std::string>()).first;
    } else if (table_it->second.empty() || grant.columns.empty()) {
      // grants merged onto the same table: whole-table (empty set) wins
      table_it->second.clear();
      continue;
    }
    for (std::string &column : grant.columns) {
      to_lower(column);
      table_it->second.insert(std::move(column));
    }
  }
  userDBs->m_lastUpdated = lastUpdated;
  assert(userDBs->m_state == UserDBs::IS_VALIDATING ||
         userDBs->m_state == UserDBs::IS_VALID);
  userDBs->m_state = UserDBs::IS_VALID;
  return CRS_Status::SUCCESS.status;
}

void APIKeyCache::load_single_key(const std::string &prefix,
                                  const std::string &secret,
                                  const std::string &salt,
                                  int user_id,
                                  long long expiry_epoch) {
#if (NUM_API_KEY_CACHES == 1)
  Uint32 key_cache_id = 0;
#else
  Uint32 hash = rondb_xxhash_std(prefix.c_str(), prefix.size());
  Uint32 key_cache_id = hash & (NUM_API_KEY_CACHES - 1);
#endif

  m_rwLock[key_cache_id].lock();  // exclusive: may insert
  // Skip if already cached (e.g., from lazy load)
  if (m_key_cache[key_cache_id].find(prefix) !=
      m_key_cache[key_cache_id].end()) {
    m_rwLock[key_cache_id].unlock();
    DEB_AUTH("API Key prefix %s already cached, skipping", prefix.c_str());
    return;
  }

  auto newUserDBs = new UserDBs();
  newUserDBs->m_state = UserDBs::IS_VALIDATING;
  // Start with ref_count=1 so the refresh thread won't evict this entry
  // while we release the lock and do the slow DB fetch below.  Decremented
  // back to 0 after the entry state is set to IS_VALID/IS_INVALID.
  newUserDBs->m_ref_count = 1;
  newUserDBs->m_secret = secret;
  newUserDBs->m_salt = salt;
  newUserDBs->m_user_id = user_id;
  newUserDBs->m_expiry_epoch = expiry_epoch;
  m_key_cache[key_cache_id][prefix] = newUserDBs;
  m_rwLock[key_cache_id].unlock();

  // Resolve permissions
  HopsworksUserGrants grants;
  RS_Status status = get_user_databases(user_id, grants);

  NDB_TICKS now = NdbTick_getCurrentTicks();
  NdbMutex_Lock(newUserDBs->m_waitLock);
  if (status.http_code == HTTP_CODE::SUCCESS) {
    // Guard against resurrection: a DELETE event may have marked this entry
    // IS_INVALID while we were resolving permissions.
    if (newUserDBs->m_state == UserDBs::IS_INVALID) {
      DEB_AUTH("load_single_key raced with DELETE event for prefix: %s",
               prefix.c_str());
    } else {
      newUserDBs->m_lastUsed = now;
      newUserDBs->m_state = UserDBs::IS_VALID;
      update_record(grants, newUserDBs);
      DEB_AUTH("Preloaded API Key: %s", prefix.c_str());
    }
  } else {
    newUserDBs->m_lastUsed = now;
    newUserDBs->m_lastUpdated = now;
    newUserDBs->m_state = UserDBs::IS_INVALID;
    DEB_AUTH("Failed to preload API Key: %s", prefix.c_str());
  }
  NdbCondition_Broadcast(newUserDBs->m_waitCond);
  NdbMutex_Unlock(newUserDBs->m_waitLock);
  newUserDBs->m_ref_count--;
}

void APIKeyCache::preload_all_keys() {
  std::vector<HopsworksAPIKeyEntry> keys;
  RS_Status status = find_all_api_keys(&keys);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    g_eventLogger->warning("[API Key Cache] Failed to preload API keys: %s",
                           status.message);
    return;
  }

  Uint32 num_threads = globalConfigs.security.apiKey.preloadThreads;
  if (num_threads <= 1 || keys.size() <= 1) {
    for (const auto &entry : keys) {
      load_single_key(entry.prefix, entry.secret, entry.salt, entry.user_id,
                      entry.expiry_epoch);
    }
  } else {
    if (num_threads > keys.size()) {
      num_threads = (Uint32)keys.size();
    }
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (Uint32 t = 0; t < num_threads; t++) {
      threads.emplace_back([this, &keys, t, num_threads]() {
        for (size_t i = t; i < keys.size(); i += num_threads) {
          load_single_key(keys[i].prefix, keys[i].secret,
                          keys[i].salt, keys[i].user_id,
                          keys[i].expiry_epoch);
        }
      });
    }
    for (auto &th : threads) {
      th.join();
    }
  }

  g_eventLogger->info("[API Key Cache] Preloaded %d API keys using %u threads",
                      (int)keys.size(), num_threads);
}

extern "C" void* api_key_refresh_thread_main(void *arg) {
  errno = 0;
  ((APIKeyCache*)arg)->refresh_job();
  return nullptr;
}

extern "C" void* api_key_event_thread_main(void *arg) {
  errno = 0;
  ((APIKeyCache*)arg)->event_watcher_job();
  return nullptr;
}

void APIKeyCache::start_background_threads() {
  m_refresh_thread = NdbThread_Create(api_key_refresh_thread_main,
                                       (NDB_THREAD_ARG *)this,
                                       128 * 1024,
                                       "API Key Refresh",
                                       NDB_THREAD_PRIO_LOW);
  if (m_refresh_thread == nullptr) {
    g_eventLogger->warning("[API Key Cache] Failed to start refresh thread");
  }

  m_event_watcher_thread = NdbThread_Create(api_key_event_thread_main,
                                             (NDB_THREAD_ARG *)this,
                                             128 * 1024,
                                             "API Key Event",
                                             NDB_THREAD_PRIO_LOW);
  if (m_event_watcher_thread == nullptr) {
    g_eventLogger->warning("[API Key Cache] Failed to start event watcher thread");
  }
}

void APIKeyCache::refresh_job() {
  DEB_AUTH_THREAD("[API Key Refresh] Started");

  while (!m_stopped) {
    NdbMutex_Lock(m_sleepLock);
    if (!m_stopped)
      NdbCondition_WaitTimeout(m_sleepCond,
                               m_sleepLock,
                               refresh_interval());
    NdbMutex_Unlock(m_sleepLock);

    if (m_stopped) break;

    for (int i = 0; i < NUM_API_KEY_CACHES; i++) {
      if (m_stopped) break;

      // Snapshot entries under shared (read) lock. Safe to use pointers
      // outside the lock because only this thread (refresh) deletes entries.
      std::vector<std::pair<std::string, UserDBs*>> entries;
      m_rwLock[i].lock_shared();
      entries.reserve(m_key_cache[i].size());
      for (auto &kv : m_key_cache[i]) {
        entries.push_back(kv);
      }
      m_rwLock[i].unlock_shared();

      // Spread DB operations across the refresh interval to avoid
      // bursting all N entries at once (thundering herd).
      Uint32 sleep_per_entry_ms = 0;
      if (entries.size() > 1) {
        sleep_per_entry_ms = (Uint32)refresh_interval() /
                             (Uint32)entries.size();
        if (sleep_per_entry_ms > 1000) sleep_per_entry_ms = 1000;
        if (sleep_per_entry_ms == 0) sleep_per_entry_ms = 1;
      }

      for (auto &[prefix, userDBs] : entries) {
        if (m_stopped) break;

        NdbMutex_Lock(userDBs->m_waitLock);
        Uint8 state = userDBs->m_state;
        NDB_TICKS lastUsed = userDBs->m_lastUsed;
        NdbMutex_Unlock(userDBs->m_waitLock);

        // Skip entries still being validated
        if (state == UserDBs::IS_VALIDATING) {
          // no sleep needed — no DB work done
        } else if (state == UserDBs::IS_INVALID) {
          // Evict invalid entries (negative cache) after 5 seconds
          static const Uint64 INVALID_ENTRY_TTL_MS = 5000;
          NDB_TICKS now = NdbTick_getCurrentTicks();
          Uint64 milliSeconds = NdbTick_Elapsed(lastUsed, now).milliSec();
          if (milliSeconds >= INVALID_ENTRY_TTL_MS) {
            m_rwLock[i].lock();  // exclusive: erase
            NdbMutex_Lock(userDBs->m_waitLock);
            if (userDBs->m_ref_count <= 0) {
              m_key_cache[i].erase(prefix);
              m_rwLock[i].unlock();
              NdbMutex_Unlock(userDBs->m_waitLock);
              DEB_AUTH_THREAD("Evicted invalid API Key: %s", prefix.c_str());
              delete userDBs;
            } else {
              m_rwLock[i].unlock();
              NdbMutex_Unlock(userDBs->m_waitLock);
            }
          }
          // no sleep needed — no DB work done
        } else {
          // IS_VALID: re-fetch key from DB to verify it still exists
          HopsworksAPIKey key;
          RS_Status key_status = find_api_key(prefix.c_str(), &key);
          if (key_status.http_code != HTTP_CODE::SUCCESS) {
            // Key no longer exists in DB — evict from cache
            m_rwLock[i].lock();  // exclusive: erase
            NdbMutex_Lock(userDBs->m_waitLock);
            if (userDBs->m_ref_count <= 0) {
              m_key_cache[i].erase(prefix);
              m_rwLock[i].unlock();
              NdbMutex_Unlock(userDBs->m_waitLock);
              DEB_AUTH_THREAD("Evicted API Key (not in DB): %s",
                              prefix.c_str());
              delete userDBs;
            } else {
              userDBs->m_state = UserDBs::IS_INVALID;
              m_rwLock[i].unlock();
              NdbMutex_Unlock(userDBs->m_waitLock);
            }
          } else {
            // Key still exists — update and re-resolve permissions
            HopsworksUserGrants grants;
            RS_Status status = get_user_databases(key.user_id, grants);

            NdbMutex_Lock(userDBs->m_waitLock);
            // Guard against resurrection: a DELETE event may have marked
            // this entry IS_INVALID while we were resolving permissions
            // (same guard as update_cache and load_single_key).
            if (userDBs->m_state == UserDBs::IS_INVALID) {
              DEB_AUTH_THREAD("Refresh raced with DELETE event for prefix: %s",
                              prefix.c_str());
            } else if (status.http_code == HTTP_CODE::SUCCESS) {
              userDBs->m_secret = key.secret;
              userDBs->m_salt = key.salt;
              userDBs->m_user_id = key.user_id;
              userDBs->m_expiry_epoch = key.expiry_epoch;
              update_record(grants, userDBs);
              DEB_AUTH_THREAD("Refreshed API Key: %s", prefix.c_str());
            } else {
              DEB_AUTH_THREAD("Failed to refresh API Key: %s", prefix.c_str());
            }
            NdbMutex_Unlock(userDBs->m_waitLock);
          }

          // Sleep between entries that did DB work (interruptible for shutdown)
          if (sleep_per_entry_ms > 0 && !m_stopped) {
            NdbMutex_Lock(m_sleepLock);
            if (!m_stopped)
              NdbCondition_WaitTimeout(m_sleepCond, m_sleepLock,
                                       sleep_per_entry_ms);
            NdbMutex_Unlock(m_sleepLock);
          }
        }
      }
    }
  }
  DEB_AUTH_THREAD("[API Key Refresh] Stopped");
}

void APIKeyCache::event_watcher_job() {
  const char *EVENT_NAME = m_event_name.c_str();
  bool first_connect = true;
  Uint32 retry_sleep_ms = 1000;
  static const Uint32 MAX_RETRY_SLEEP_MS = 30000;

  Ndb *ndb = nullptr;
  NdbEventOperation *ev_op = nullptr;
  NdbDictionary::Dictionary *dict = nullptr;
  NdbRecAttr *id_val = nullptr;
  NdbRecAttr *prefix_val = nullptr;
  NdbRecAttr *secret_val = nullptr;
  NdbRecAttr *salt_val = nullptr;
  NdbRecAttr *user_id_val = nullptr;
  NdbRecAttr *expiry_val = nullptr;
  NdbRecAttr *prefix_pre_val = nullptr;
  unsigned expiry_prec = 0;

retry:
  ndb = nullptr;
  ev_op = nullptr;
  dict = nullptr;

  if (m_stopped) goto done;

  {
    RS_Status rs = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb);
    if (rs.http_code != SUCCESS) {
      g_eventLogger->warning("[API Key Event] Failed to get NDB object. Retry...");
      goto err;
    }
  }

  if (ndb->setDatabaseName(HOPSWORKS) != 0) {
    g_eventLogger->warning("[API Key Event] Failed to set database: %d(%s). Retry...",
                           ndb->getNdbError().code,
                           ndb->getNdbError().message);
    goto err;
  }

  dict = ndb->getDictionary();
  {
    dict->invalidateTable(API_KEY);
    const NdbDictionary::Table *tab = dict->getTable(API_KEY);
    if (tab == nullptr) {
      g_eventLogger->warning("[API Key Event] Failed to get api_key table: %d(%s). Retry...",
                             dict->getNdbError().code,
                             dict->getNdbError().message);
      goto err;
    }

    const NdbDictionary::Column *expiry_col = tab->getColumn("expiry");
    if (expiry_col == nullptr) {
      g_eventLogger->warning(
        "[API Key Event] Failed to get expiry column "
        "(schema may have changed). Retry...");
      goto err;
    }
    expiry_prec = expiry_col->getPrecision();

    NdbDictionary::Event event(EVENT_NAME);
    event.setTable(*tab);
    event.addTableEvent(NdbDictionary::Event::TE_INSERT);
    event.addTableEvent(NdbDictionary::Event::TE_UPDATE);
    event.addTableEvent(NdbDictionary::Event::TE_DELETE);
    event.mergeEvents(true);
    for (int col = 0; col < tab->getNoOfColumns(); col++) {
      event.addEventColumn(col);
    }

    dict->dropEvent(EVENT_NAME);

    if (dict->createEvent(event)) {
      g_eventLogger->warning("[API Key Event] Failed to create event: %d(%s). Retry...",
                             dict->getNdbError().code,
                             dict->getNdbError().message);
      goto err;
    }
  }

  ev_op = ndb->createEventOperation(EVENT_NAME);
  if (ev_op == nullptr) {
    g_eventLogger->warning(
      "[API Key Event] Failed to create event operation: %d(%s). Retry...",
      ndb->getNdbError().code,
      ndb->getNdbError().message);
    goto err;
  }
  ev_op->mergeEvents(true);

  // Register PK column so PK data is consumed from the event stream.
  // Without this, PK data leaks into the data section and triggers
  // an assertion in NdbEventOperationImpl::receive_event.
  id_val = ev_op->getValue("id");
  (void)ev_op->getPreValue("id");

  prefix_val = ev_op->getValue("prefix");
  secret_val = ev_op->getValue("secret");
  salt_val = ev_op->getValue("salt");
  user_id_val = ev_op->getValue("user_id");
  expiry_val = ev_op->getValue("expiry");

  // Pre-values must be registered for all columns that have after-values.
  // NDB requires matching getValue/getPreValue pairs to properly consume
  // event data. prefix_pre_val is used for TE_DELETE; the rest are unused
  // but must be registered to keep the event stream aligned.
  prefix_pre_val = ev_op->getPreValue("prefix");
  (void)ev_op->getPreValue("secret");
  (void)ev_op->getPreValue("salt");
  (void)ev_op->getPreValue("user_id");
  (void)ev_op->getPreValue("expiry");

  // Null-check all NdbRecAttr pointers (schema may have changed)
  if (id_val == nullptr || prefix_val == nullptr ||
      secret_val == nullptr || salt_val == nullptr ||
      user_id_val == nullptr || expiry_val == nullptr ||
      prefix_pre_val == nullptr) {
    g_eventLogger->warning(
      "[API Key Event] Failed to register event columns "
      "(schema may have changed). Retry...");
    goto err;
  }

  if (ev_op->execute()) {
    g_eventLogger->warning(
      "[API Key Event] Failed to execute event operation: %d(%s). Retry...",
      ev_op->getNdbError().code,
      ev_op->getNdbError().message);
    goto err;
  }

  retry_sleep_ms = 1000;  // Reset backoff on successful setup
  g_eventLogger->info("[API Key Event] Watcher %s",
                      first_connect ? "started" : "reconnected");

  if (!first_connect) {
    // When the event watcher hits an error it jumps to err:, tears down the
    // NDB event subscription, sleeps with exponential backoff (1s → 30s cap),
    // then jumps back to retry: to re-establish the subscription.  During
    // that gap any table events (INSERT/DELETE/UPDATE) are silently lost.
    //
    // Preload picks up missed INSERTs (load_single_key skips already-cached
    // entries, so this is a fast no-op for most keys).  Waking the refresh
    // thread handles missed DELETEs/UPDATEs immediately instead of waiting
    // up to the full refresh interval (180 s).
    preload_all_keys();
    NdbMutex_Lock(m_sleepLock);
    NdbCondition_Broadcast(m_sleepCond);
    NdbMutex_Unlock(m_sleepLock);
  }
  first_connect = false;

  // Poll loop
  while (!m_stopped) {
    if (m_force_reconnect.exchange(false)) {
      g_eventLogger->info("[API Key Event] Forced reconnect requested");
      goto err;
    }
    int res = ndb->pollEvents(1000);
    if (res < 0) {
      g_eventLogger->warning(
        "[API Key Event] pollEvents error: %d(%s). Retry...",
        ndb->getNdbError().code,
        ndb->getNdbError().message);
      goto err;
    }
    if (res == 0) continue;

    NdbEventOperation *op;
    while ((op = ndb->nextEvent())) {
      if (op->hasError()) {
        g_eventLogger->warning(
          "[API Key Event] Event error: %d(%s). Retry...",
          op->getNdbError().code,
          op->getNdbError().message);
        goto err;
      }

      switch (op->getEventType()) {
        case NdbDictionary::Event::TE_INSERT: {
          Uint32 prefix_bytes = 0;
          const char *prefix_start = nullptr;
          if (GetByteArray(prefix_val, &prefix_start, &prefix_bytes) != 0) {
            break;
          }
          std::string prefix(prefix_start, prefix_bytes);

          Uint32 secret_bytes = 0;
          const char *secret_start = nullptr;
          if (GetByteArray(secret_val, &secret_start, &secret_bytes) != 0) {
            break;
          }
          std::string secret(secret_start, secret_bytes);

          Uint32 salt_bytes = 0;
          const char *salt_start = nullptr;
          if (GetByteArray(salt_val, &salt_start, &salt_bytes) != 0) {
            break;
          }
          std::string salt(salt_start, salt_bytes);

          int user_id = user_id_val->int32_value();
          long long expiry_epoch = datetime_attr_to_epoch(expiry_val,
                                                          expiry_prec);

          g_eventLogger->info(
            "[API Key Event] INSERT detected for prefix: %s",
            prefix.c_str());
          load_single_key(prefix, secret, salt, user_id, expiry_epoch);
          break;
        }
        case NdbDictionary::Event::TE_UPDATE: {
          // With mergeEvents(true), only modified columns have valid
          // after-image data — unmodified columns are undefined.
          // Use the PK (id, always available) to do a single PK read
          // and get the full row (O(1) instead of table scan).
          int event_id = id_val->int32_value();
          g_eventLogger->info(
            "[API Key Event] UPDATE detected for id: %d", event_id);

          HopsworksAPIKeyEntry upd_key;
          RS_Status upd_status = find_api_key_by_id(event_id, &upd_key);
          if (upd_status.http_code != HTTP_CODE::SUCCESS) {
            g_eventLogger->warning(
              "[API Key Event] Failed to read updated row id=%d: %s",
              event_id, upd_status.message);
            break;
          }

          const std::string &upd_prefix = upd_key.prefix;
          const std::string &upd_secret = upd_key.secret;
          const std::string &upd_salt = upd_key.salt;
          int upd_user_id = upd_key.user_id;

#if (NUM_API_KEY_CACHES == 1)
          Uint32 upd_cache_id = 0;
#else
          Uint32 upd_h = rondb_xxhash_std(upd_prefix.c_str(),
                                           upd_prefix.size());
          Uint32 upd_cache_id = upd_h & (NUM_API_KEY_CACHES - 1);
#endif
          m_rwLock[upd_cache_id].lock_shared();
          auto upd_it = m_key_cache[upd_cache_id].find(upd_prefix);
          if (upd_it != m_key_cache[upd_cache_id].end()) {
            auto userDBs = upd_it->second;
            NdbMutex_Lock(userDBs->m_waitLock);
            userDBs->m_secret = upd_secret;
            userDBs->m_salt = upd_salt;
            userDBs->m_user_id = upd_user_id;
            userDBs->m_expiry_epoch = upd_key.expiry_epoch;
            NdbMutex_Unlock(userDBs->m_waitLock);
            m_rwLock[upd_cache_id].unlock_shared();
            g_eventLogger->info(
              "[API Key Event] Updated cache entry for prefix: %s",
              upd_prefix.c_str());
          } else {
            m_rwLock[upd_cache_id].unlock_shared();
            load_single_key(upd_prefix, upd_secret, upd_salt, upd_user_id,
                            upd_key.expiry_epoch);
          }
          break;
        }
        case NdbDictionary::Event::TE_DELETE: {
          Uint32 prefix_bytes = 0;
          const char *prefix_start = nullptr;
          if (GetByteArray(prefix_pre_val,
                           &prefix_start, &prefix_bytes) != 0) {
            break;
          }
          std::string prefix(prefix_start, prefix_bytes);
          g_eventLogger->info(
            "[API Key Event] DELETE detected for prefix: %s",
            prefix.c_str());

#if (NUM_API_KEY_CACHES == 1)
          Uint32 key_cache_id = 0;
#else
          Uint32 h = rondb_xxhash_std(prefix.c_str(), prefix.size());
          Uint32 key_cache_id = h & (NUM_API_KEY_CACHES - 1);
#endif
          // Mark as invalid; the refresh thread handles actual deletion.
          m_rwLock[key_cache_id].lock_shared();
          auto it = m_key_cache[key_cache_id].find(prefix);
          if (it != m_key_cache[key_cache_id].end()) {
            auto userDBs = it->second;
            NdbMutex_Lock(userDBs->m_waitLock);
            userDBs->m_state = UserDBs::IS_INVALID;
            // Reset m_lastUsed so the INVALID_ENTRY_TTL countdown
            // starts from this DELETE event, not the last request.
            userDBs->m_lastUsed = NdbTick_getCurrentTicks();
            NdbMutex_Unlock(userDBs->m_waitLock);
          }
          m_rwLock[key_cache_id].unlock_shared();
          break;
        }
        case NdbDictionary::Event::TE_CLUSTER_FAILURE:
        case NdbDictionary::Event::TE_STOP:
        case NdbDictionary::Event::TE_INCONSISTENT:
        case NdbDictionary::Event::TE_OUT_OF_MEMORY:
          g_eventLogger->warning(
            "[API Key Event] System event %d received. Retry...",
            op->getEventType());
          goto err;
        default:
          break;
      }
    }
  }
  goto done;

err:
  if (ev_op != nullptr) {
    ndb->dropEventOperation(ev_op);
    ev_op = nullptr;
  }
  if (dict != nullptr) {
    dict->dropEvent(EVENT_NAME);
    dict = nullptr;
  }
  if (ndb != nullptr) {
    RS_Status rs = RS_OK;
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    ndb = nullptr;
  }
  if (!m_stopped) {
    g_eventLogger->info("[API Key Event] Retrying in %u ms...", retry_sleep_ms);
    NdbMutex_Lock(m_sleepLock);
    if (!m_stopped)
      NdbCondition_WaitTimeout(m_sleepCond, m_sleepLock, retry_sleep_ms);
    NdbMutex_Unlock(m_sleepLock);
    retry_sleep_ms = std::min(retry_sleep_ms * 2, MAX_RETRY_SLEEP_MS);
    goto retry;
  }

done:
  if (ev_op != nullptr) {
    ndb->dropEventOperation(ev_op);
  }
  if (dict != nullptr) {
    dict->dropEvent(EVENT_NAME);
  }
  if (ndb != nullptr) {
    RS_Status rs = RS_OK;
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
  }
  g_eventLogger->info("[API Key Event] Watcher stopped");
}

RS_Status APIKeyCache::get_user_databases(int user_id,
                                          HopsworksUserGrants &grants) {
  // name lowercasing happens in update_record
  return find_user_databases(user_id, &grants);
}

RS_Status computeHash(const std::string &unhashed, std::string &hashed) {
  static const char hex_lut[] = "0123456789abcdef";

  RS_Status status = CRS_Status(HTTP_CODE::CLIENT_ERROR,
                       "Failed to compute hash").status;

  auto deleter = [](EVP_MD_CTX *ctx) { EVP_MD_CTX_free(ctx); };
  std::unique_ptr<EVP_MD_CTX, decltype(deleter)>
    mdCtx(EVP_MD_CTX_new(), deleter);

  if (mdCtx) {
    if (EVP_DigestInit_ex(mdCtx.get(), EVP_sha256(), nullptr) != 0) {
      if (EVP_DigestUpdate(mdCtx.get(),
                           unhashed.c_str(),
                           unhashed.length()) != 0) {
        unsigned char hash[EVP_MAX_MD_SIZE];
        unsigned int lengthOfHash = 0;

        if (EVP_DigestFinal_ex(mdCtx.get(), hash, &lengthOfHash) != 0) {
          hashed.resize(lengthOfHash * 2);
          for (unsigned int i = 0; i < lengthOfHash; ++i) {
            hashed[i * 2]     = hex_lut[(hash[i] >> 4) & 0x0f];
            hashed[i * 2 + 1] = hex_lut[hash[i] & 0x0f];
          }
          status = CRS_Status::SUCCESS.status;
        }
      }
    }
  }
  return status;
}

Int32 APIKeyCache::refresh_interval() {
  return Int32(globalConfigs.security.apiKey.cacheRefreshIntervalMS);
}

/* Below methods only used by unit test program */
unsigned APIKeyCache::size() {
  for (int i = 0; i < NUM_API_KEY_CACHES; i++)
    m_rwLock[i].lock_shared();
  unsigned size_cache = 0;
  for (int i = 0; i < NUM_API_KEY_CACHES; i++)
    size_cache += m_key_cache[i].size();
  for (int i = 0; i < NUM_API_KEY_CACHES; i++)
    m_rwLock[i].unlock_shared();
  return size_cache;
}

std::string APIKeyCache::to_string() {
  std::stringstream ss;
  for (int i = 0; i < NUM_API_KEY_CACHES; i++)
    m_rwLock[i].lock_shared();
  for (int i = 0; i < NUM_API_KEY_CACHES; i++) {
    for (const auto &entry : m_key_cache[i]) {
      ss << "Prefix: " << entry.first << ", UserDBs: ";
      for (const auto &db : entry.second->userDBs) {
        ss << db << ", ";
      }
      ss << std::endl;
    }
  }
  for (int i = 0; i < NUM_API_KEY_CACHES; i++)
    m_rwLock[i].unlock_shared();
  return ss.str();
}

Uint64 APIKeyCache::last_updated(const std::string &apiKey) {
  auto dotPos = apiKey.find('.');
  std::string prefix = (dotPos != std::string::npos)
                        ? apiKey.substr(0, dotPos) : apiKey;
#if (NUM_API_KEY_CACHES == 1)
  Uint32 hash = 0;
#else
  Uint32 hash = rondb_xxhash_std(prefix.c_str(), prefix.size());
#endif
  Uint32 key_cache_id = hash & (NUM_API_KEY_CACHES - 1);
  m_rwLock[key_cache_id].lock_shared();
  auto it = m_key_cache[key_cache_id].find(prefix);
  if (it == m_key_cache[key_cache_id].end()) {
    m_rwLock[key_cache_id].unlock_shared();
    NDB_TICKS now = NdbTick_getCurrentTicks();
    return now.getUint64();
  }
  auto userDBs = it->second;
  NdbMutex_Lock(userDBs->m_waitLock);
  m_rwLock[key_cache_id].unlock_shared();
  Uint64 lastUpdated = userDBs->m_lastUpdated.getUint64();
  NdbMutex_Unlock(userDBs->m_waitLock);
  return lastUpdated;
}

bool contains_upper(std::string_view sv) {
    return std::any_of(sv.begin(), sv.end(), [](unsigned char c) {
        return std::isupper(c);
    });
}
