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

#include "api_key.hpp"
#include "config_structs.hpp"
#include "pk_data_structs.hpp"
#include "rdrs_dal.hpp"

#include <chrono>
#include <iostream>
#include <memory>
#include <openssl/evp.h>
#include <thread>
#include <unordered_map>
#include <functional>
#include <algorithm>
#include <NdbThread.h>
#include <util/require.h>
#include <EventLogger.hpp>

//#define DEBUG_AUTH 1
//#define DEBUG_AUTH_THREAD 1
//#define DEBUG_AUTH_TIME 1
//#define DEBUG_AUTH_DBS 1

#ifdef DEBUG_AUTH
#define DEB_AUTH(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_AUTH(arglist) do { } while (0)
#endif

#ifdef DEBUG_AUTH_THREAD
#define DEB_AUTH_THREAD(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_AUTH_THREAD(arglist) do { } while (0)
#endif

#ifdef DEBUG_AUTH_TIME
#define DEB_AUTH_TIME(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_AUTH_TIME(arglist) do { } while (0)
#endif

#ifdef DEBUG_AUTH_DBS
#define DEB_AUTH_DBS(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_AUTH_DBS(arglist) do { } while (0)
#endif

#ifndef DEBUG_AUTH_THREAD
#define CLEANUP_SLEEP_TIME 10
#else
#define CLEANUP_SLEEP_TIME 1000
#endif
extern EventLogger *g_eventLogger;

APIKeyCache *apiKeyCache = nullptr;

std::vector<std::string> split(const std::string &, char);
RS_Status computeHash(const std::string &unhashed, std::string &hashed);

APIKeyCache* start_api_key_cache() {
  apiKeyCache = new APIKeyCache();
  require(apiKeyCache != nullptr);
  DEB_AUTH(("API Key Cache started: %p", apiKeyCache));
  return apiKeyCache;
}

void stop_api_key_cache() {
  DEB_AUTH(("API Key Cache stopped: %p", apiKeyCache));
  if (apiKeyCache != nullptr) {
    delete apiKeyCache;
    apiKeyCache = nullptr;
  }
}

void APIKeyCache::cleanup() {
  /* Start by waking all threads */
  DEB_AUTH(("Cleanup started"));
  NdbMutex_Lock(m_sleepLock);
  for (int i = 0; i < NUM_API_KEY_CACHES; i++)
    NdbMutex_Lock(m_rwLock[i]);
  m_evicted = true;
  NdbCondition_Broadcast(m_sleepCond);
  NdbMutex_Unlock(m_sleepLock);
  for (int i = 0; i < NUM_API_KEY_CACHES; i++)
    NdbMutex_Unlock(m_rwLock[i]);

  /* Wait for all objects to complete cleanup */
  for (int i = 0; i < NUM_API_KEY_CACHES; i++) {
    NdbMutex_Lock(m_rwLock[i]);
    while (m_key_cache[i].size() > 0) {
#ifdef DEBUG_AUTH
      Uint32 key_cache_size = (Uint32)m_key_cache[i].size();
#endif
      NdbMutex_Unlock(m_rwLock[i]);
      DEB_AUTH(("m_key_cache[%d].size() = %u", i, key_cache_size));
      NdbSleep_MilliSleep(CLEANUP_SLEEP_TIME);
      NdbMutex_Lock(m_rwLock[i]);
    }
    NdbMutex_Unlock(m_rwLock[i]);
  }
  DEB_AUTH(("Cleanup finished"));
}

RS_Status authenticate(const std::string &apiKey, PKReadParams &params) {
  return apiKeyCache->validate_api_key(apiKey, {params.path.db});
}

RS_Status authenticate(const std::string &apiKey, const std::string & db) {
  return apiKeyCache->validate_api_key(apiKey, {db});
}

RS_Status authenticate(const std::string &apiKey,
                       const std::vector<std::string> &dbs) {
  return apiKeyCache->validate_api_key(apiKey, dbs);
}

RS_Status APIKeyCache::validate_api_key_format(const std::string &apiKey) {
  if (apiKey.empty()) {
    return CRS_Status(HTTP_CODE::CLIENT_ERROR, "the apikey is nil").status;
  }
  auto splits = split(apiKey, '.');
  if (splits.size() != 2 ||
      splits[0].length() != 16 ||
      splits[1].length() < 1) {
    DEB_AUTH(("Failed incorrect format, Line: %u", __LINE__));
    return CRS_Status(HTTP_CODE::CLIENT_ERROR,
                      "the apikey has an incorrect format").status;
  }
  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::validate_api_key(const std::string &apiKey,
                                        const std::vector<std::string> &dbs) {
#ifdef DEBUG_AUTH
  DEB_AUTH(("authenticate apiKey: %s", apiKey.c_str()));
  for (const auto &db : dbs) {
    DEB_AUTH(("validate db: %s", db.c_str()));
  }
#endif
  RS_Status status = validate_api_key_format(apiKey);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }
  if (dbs.empty()) {
    DEB_AUTH(("Empty database authenticated, Line: %u", __LINE__));
    return CRS_Status::SUCCESS.status;
  }

  // Authenticates only using the the cache. No request sent to backend
  bool keyFoundInCache = false;
  bool allowedAccess = false;
#if (defined NUM_API_KEY_CACHES == 1)
  Uint32 hash = 0;
#else
  Uint32 hash = rondb_xxhash_std(apiKey.c_str(), apiKey.size());
#endif
  status = find_and_validate(apiKey,
                             keyFoundInCache,
                             allowedAccess,
                             dbs,
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
   * Update the cache by fetching the API key from backend
   * Only come here if the API Key wasn't found in cache.
   */
  status = update_cache(apiKey, hash);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }

  // Authenticates only using the the cache. No request sent to backend
  status = find_and_validate(apiKey,
                             keyFoundInCache,
                             allowedAccess,
                             dbs,
                             hash,
                             true);
  return status;
}

RS_Status APIKeyCache::find_and_validate(const std::string &apiKey,
                                         bool &keyFoundInCache,
                                         bool &allowedAccess,
                                         const std::vector<std::string> &dbs,
                                         Uint64 hash,
                                         bool inc_refcount_done) {
  Uint32 key_cache_id = hash & (NUM_API_KEY_CACHES - 1);
  NdbMutex_Lock(m_rwLock[key_cache_id]);
  if (m_evicted) {
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    keyFoundInCache = true; // Make sure we return without inserting it
    DEB_AUTH(("API Key cache shutdown, Line: %u", __LINE__));
    return CRS_Status(HTTP_CODE::SERVER_ERROR,
      "API Key cache is shutting down").status;
  }
  auto it = m_key_cache[key_cache_id].find(apiKey);
  if (it == m_key_cache[key_cache_id].end()) {
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    require(!inc_refcount_done);
    DEB_AUTH(("API Key not found, Line: %u", __LINE__));
    return CRS_Status(HTTP_CODE::AUTH_ERROR,
      "API key not found in cache").status;
  }
  auto userDBs = it->second;

  keyFoundInCache = true;
  if (userDBs == nullptr) {
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    require(!inc_refcount_done);
    DEB_AUTH(("API Key found UserDBs null, Line: %u", __LINE__));
    return CRS_Status(HTTP_CODE::AUTH_ERROR,
      "API key found in cache but userDBs is null").status;
  }
  if (userDBs->m_state == UserDBs::IS_INVALID) {
    if (inc_refcount_done) userDBs->m_ref_count--;
#ifdef DEBUG_AUTH
    int ref_count = userDBs->m_ref_count;
#endif
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    DEB_AUTH(("API Key found invalid, Line: %u, refCount: %d",
              __LINE__, ref_count));
    return CRS_Status(HTTP_CODE::AUTH_ERROR,
      "API key found in cache but is invalid").status;
  }
  NdbMutex_Lock(userDBs->m_waitLock);
  NdbMutex_Unlock(m_rwLock[key_cache_id]);
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
    DEB_AUTH(("API Key found invalid, Line: %u, refCount: %d",
              __LINE__, ref_count));
    return CRS_Status(HTTP_CODE::AUTH_ERROR,
      "API key found in cache but invalid").status;
  }
  userDBs->m_lastUsed = NdbTick_getCurrentTicks();
  for (const auto &db : dbs) {
    if (userDBs->userDBs.find(db) == userDBs->userDBs.end()) {
      if (inc_refcount_done) userDBs->m_ref_count--;
#ifdef DEBUG_AUTH
      int ref_count = userDBs->m_ref_count;
#endif
      NdbMutex_Unlock(userDBs->m_waitLock);
      allowedAccess = false;
      DEB_AUTH(("API Key found valid, not authorized, Line: %u, refCount: %d",
                __LINE__, ref_count));
      return CRS_Status(HTTP_CODE::AUTH_ERROR,
        ("API key not authorized to access " + db).c_str()).status;
    }
  }
  // Decrement the reference count such that it can be released again
  if (inc_refcount_done) userDBs->m_ref_count--;
#ifdef DEBUG_AUTH
  int ref_count = userDBs->m_ref_count;
#endif
  NdbMutex_Unlock(userDBs->m_waitLock);
  allowedAccess = true;
  DEB_AUTH(("API Key found valid, success, Line: %u, refCount: %d",
            __LINE__, ref_count));
  return CRS_Status::SUCCESS.status;
}

extern "C" void* api_key_thread_main(void *thr_arg) {
  char *api_key_str = (char*)thr_arg;
  std::string apiKey = api_key_str;
  free(api_key_str);
  apiKeyCache->cache_entry_updater(apiKey);
  return nullptr;
}

RS_Status APIKeyCache::update_cache(const std::string &apiKey, Uint64 hash) {
  Uint32 key_cache_id = hash & (NUM_API_KEY_CACHES - 1);
  NdbMutex_Lock(m_rwLock[key_cache_id]);
  // Check if the key exists, might have been entered since we released lock
  auto userDBs = m_key_cache[key_cache_id].find(apiKey);
  if (userDBs == m_key_cache[key_cache_id].end()) {
    // The entry does not exist, mark for update
    char *api_key_str = (char*)malloc(apiKey.size() + 1);
    if (api_key_str == nullptr) {
      NdbMutex_Unlock(m_rwLock[key_cache_id]);
      DEB_AUTH(("API Key malloc failed, Line: %u", __LINE__));
      return CRS_Status(HTTP_CODE::SERVER_ERROR,
        "Failed to allocate API Key record in cache").status;
    }
    auto newUserDBs = new UserDBs();
    if (newUserDBs == nullptr) {
      NdbMutex_Unlock(m_rwLock[key_cache_id]);
      free(api_key_str);
      DEB_AUTH(("API Key create UserDBs failed, Line: %u", __LINE__));
      return CRS_Status(HTTP_CODE::SERVER_ERROR,
        "Failed to allocate API Key record in cache").status;
    }
    strncpy(api_key_str, apiKey.c_str(), apiKey.size());
    api_key_str[apiKey.size()] = 0;
    NdbThread *thread = NdbThread_Create(api_key_thread_main,
                                         (void**)api_key_str,
                                          128 * 1024,
                                          "Api Key Cache thread",
                                          NDB_THREAD_PRIO_LOW);
    if (thread == nullptr) {
      delete newUserDBs;
      free(api_key_str);
      NdbMutex_Unlock(m_rwLock[key_cache_id]);
      DEB_AUTH(("API Key thread create failed, Line: %u", __LINE__));
      return CRS_Status(HTTP_CODE::SERVER_ERROR,
        "Failed to allocate API Key record in cache").status;
    }
    DEB_AUTH(("API Key %s inserted in cache with refCount: 1", apiKey.c_str()));
    newUserDBs->m_refresh_interval = refresh_interval_with_jitter();
    newUserDBs->m_ref_count = 1;
    newUserDBs->m_state = UserDBs::IS_VALIDATING;
    m_key_cache[key_cache_id][apiKey] = newUserDBs;
    DEB_AUTH_TIME(("Api key: %s, refresh interval %d",
                   apiKey.c_str(), newUserDBs->m_refresh_interval));
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    return CRS_Status::SUCCESS.status;
  }
  // Increase reference count to the usdbs to avoid it being released
  userDBs->second->m_ref_count++;
  NdbMutex_Unlock(m_rwLock[key_cache_id]);
  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::update_record(std::vector<std::string> dbs,
                                     UserDBs *userDBs) {
  NDB_TICKS lastUpdated = NdbTick_getCurrentTicks();
  std::unordered_map<std::string, bool> dbsMap;
  for (const auto &db : dbs) {
    DEB_AUTH_DBS(("Valid API Key with db: %s", db.c_str()));
    dbsMap[db] = true;
  }
  userDBs->userDBs = dbsMap;
  userDBs->m_lastUpdated = lastUpdated;
  assert(userDBs->m_state == UserDBs::IS_VALIDATING ||
         userDBs->m_state == UserDBs::IS_VALID);
  userDBs->m_state = UserDBs::IS_VALID;
  return CRS_Status::SUCCESS.status;
}

void APIKeyCache::cache_entry_updater(const std::string &apiKey) {
#if (defined NUM_API_KEY_CACHES == 1)
  Uint32 key_cache_id = 0;
#else
  Uint32 key_cache_id = hash & (NUM_API_KEY_CACHES - 1);
#endif
  NdbMutex_Lock(m_rwLock[key_cache_id]);
  auto it = m_key_cache[key_cache_id].find(apiKey);
  if (it == m_key_cache[key_cache_id].end()) {
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    require(false);
    return;
  }
  auto userDBs = it->second;
  if (userDBs == nullptr) {
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    require(false);
    return;
  }
  NdbMutex_Lock(userDBs->m_waitLock);
  NdbMutex_Unlock(m_rwLock[key_cache_id]);
  require(userDBs->m_state == UserDBs::IS_VALIDATING);
  NdbMutex_Unlock(userDBs->m_waitLock);
  RS_Status status;
  bool first = true;
  (void)first;
  while (true) {
    bool fail = false;

    HopsworksAPIKey key;
    std::vector<std::string> dbs;
    if (!m_evicted) {
      status = authenticate_user(apiKey, key);
      if (status.http_code != HTTP_CODE::SUCCESS) {
        fail = true;
      }
    }

    if (!fail && !m_evicted) {
      status = get_user_databases(key, dbs);
      if (status.http_code != HTTP_CODE::SUCCESS) {
        fail = true;
      }
    }
    /**
     * Wake up all waiters for the API Key validation, if successful
     * we will fill in the validated databases, otherwise it will be
     * an empty list of databases and thus no one will be able to
     * validate against it. The API Key is kept in the API Key Cache
     * even if it is an incorrect one, but lastUpdated will only be
     * updated at successful lookups in the API Key Cache.
     */
    NDB_TICKS lastUpdated = NdbTick_getCurrentTicks();
    NdbMutex_Lock(userDBs->m_waitLock);
    if (!fail && !m_evicted) {
      if (first) {
        userDBs->m_lastUsed = lastUpdated;
        DEB_AUTH(("Valid API Key inserted: %s", apiKey.c_str()));
      } else {
        DEB_AUTH(("Valid API Key updated: %s", apiKey.c_str()));
      }
      update_record(dbs, userDBs);
    } else {
      if (first) {
        userDBs->m_lastUsed = lastUpdated;
      }
      DEB_AUTH(("Invalid API Key: %s", apiKey.c_str()));
      userDBs->m_state = UserDBs::IS_INVALID;
    }
    first = false;
    userDBs->m_lastUpdated = lastUpdated;
    NdbCondition_Broadcast(userDBs->m_waitCond);
#ifdef DEBUG_AUTH_THREAD
    int ref_count = userDBs->m_ref_count;
#endif
    NdbMutex_Unlock(userDBs->m_waitLock);

    /* Ensure it is easy to wake all threads up when time to close down */
    DEB_AUTH_THREAD(("API key %s, thread sleep, refCount: %d",
                    apiKey.c_str(), ref_count));
    NdbMutex_Lock(m_sleepLock);
    if (!m_evicted)
      NdbCondition_WaitTimeout(m_sleepCond,
                               m_sleepLock,
                               userDBs->m_refresh_interval);
    NdbMutex_Unlock(m_sleepLock);

    while (m_evicted) {
      DEB_AUTH_THREAD(("API key %s, thread shutdown", apiKey.c_str()));
      /* First wait for all access to this API Key to finish */
      NdbMutex_Lock(userDBs->m_waitLock);
      int ref_count = userDBs->m_ref_count;
      if (ref_count > 0) {
        NdbMutex_Unlock(userDBs->m_waitLock);
        DEB_AUTH_THREAD(("API key %s, refCount: %d",
                         apiKey.c_str(), ref_count));
        NdbSleep_MilliSleep(CLEANUP_SLEEP_TIME);
        continue;
      }
      NdbMutex_Unlock(userDBs->m_waitLock);
      /**
       * Remove the API Key and update counter to ensure cleanup knows
       * when done.
       */
      DEB_AUTH_THREAD(("API key %s, delete", apiKey.c_str()));
      NdbMutex_Lock(m_rwLock[key_cache_id]);
      m_key_cache[key_cache_id].erase(apiKey);
      NdbMutex_Unlock(m_rwLock[key_cache_id]);
      return;
    }

    NdbMutex_Lock(userDBs->m_waitLock);
    auto lastUsed = userDBs->m_lastUsed;
#ifdef DEBUG_AUTH_THREAD
    int ref_count_after = userDBs->m_ref_count;
#endif
    NdbMutex_Unlock(userDBs->m_waitLock);

    DEB_AUTH_THREAD(("API key %s, thread wakeup, ref_count: %d",
                     apiKey.c_str(), ref_count_after));
    NDB_TICKS now = NdbTick_getCurrentTicks();
    Uint64 milliSeconds = NdbTick_Elapsed(lastUsed, now).milliSec();
    DEB_AUTH_THREAD(("API Key: %s %llu millis since last used",
                     apiKey.c_str(), milliSeconds));
    if (milliSeconds >=
          (Uint64)globalConfigs.security.apiKey.cacheUnusedEntriesEvictionMS) {
      NdbMutex_Lock(m_rwLock[key_cache_id]);
      NdbMutex_Lock(userDBs->m_waitLock);
      if (userDBs->m_ref_count <= 0) {
        m_key_cache[key_cache_id].erase(apiKey);
        NdbMutex_Unlock(m_rwLock[key_cache_id]);
        NdbMutex_Unlock(userDBs->m_waitLock);
        return;
      } else {
        NdbMutex_Unlock(m_rwLock[key_cache_id]);
        NdbMutex_Unlock(userDBs->m_waitLock);
        continue;
      }
      break;
    }
  }
  return;
}

RS_Status APIKeyCache::authenticate_user(const std::string &apiKey,
                                         HopsworksAPIKey &key) {
  auto splits = split(apiKey, '.');
  auto prefix = splits[0];
  auto clientSecret = splits[1];

  RS_Status status = get_api_key(prefix, key);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }

  // sha256(client.secret + db.salt) = db.secret
  std::string unhashed = clientSecret + key.salt;
  std::string hashed;
  status = computeHash(unhashed, hashed);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }
  if (hashed != key.secret) {
    return CRS_Status(HTTP_CODE::CLIENT_ERROR, "bad API key").status;
  }
  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::get_user_databases(HopsworksAPIKey &key,
                                          std::vector<std::string> &dbs) {
  RS_Status status = get_user_projects(key.user_id, dbs);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }
  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::get_user_projects(int uid,
                                         std::vector<std::string> &dbs) {
  int count = 0;
  char **projects = nullptr;
  RS_Status status = find_all_projects(uid, &projects, &count);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }
  for (int i = 0; i < count; i++) {
    dbs.push_back(projects[i]);
  }
  // Free projects array because find_all_projects allocates memory for it
  for (int i = 0; i < count; i++) {
    free(projects[i]);
  }
  free(projects);
  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::get_api_key(const std::string &userKey,
                                   HopsworksAPIKey &key) {
  RS_Status status = find_api_key(userKey.c_str(), &key);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }
  return CRS_Status::SUCCESS.status;
}

std::vector<std::string> split(const std::string &str, char delim) {
  std::vector<std::string> tokens;
  std::stringstream ss(str);
  std::string token;
  while (std::getline(ss, token, delim)) {
    tokens.push_back(token);
  }
  return tokens;
}

RS_Status computeHash(const std::string &unhashed, std::string &hashed) {
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
          std::stringstream ss;
          for (unsigned int i = 0; i < lengthOfHash; ++i) {
            ss << std::hex << std::setw(2)
               << std::setfill('0') << static_cast<int>(hash[i]);
          }
          hashed = ss.str();
          status = CRS_Status::SUCCESS.status;
        }
      }
    }
  }
  return status;
}

Int32 APIKeyCache::refresh_interval_with_jitter() {
  Uint32 cacheRefreshIntervalMS =
    globalConfigs.security.apiKey.cacheRefreshIntervalMS;
  Int32 jitterMS = static_cast<int>(
    globalConfigs.security.apiKey.cacheRefreshIntervalJitterMS);
  std::uniform_int_distribution<int32_t> dist(0, jitterMS);
  Int32 jitter = dist(randomGenerator);
  if (jitter % 2 == 0) {
    jitter = -jitter;
  }
  return Int32(cacheRefreshIntervalMS) + jitter;
}

/* Below methods only used by unit test program */
unsigned APIKeyCache::size() {
#if (defined NUM_API_KEY_CACHES == 1)
  Uint32 hash = 0;
#else
  Uint32 hash = rondb_xxhash_std(apiKey.c_str(), apiKey.size());
#endif
  Uint32 key_cache_id = hash & (NUM_API_KEY_CACHES - 1);
  NdbMutex_Lock(m_rwLock[key_cache_id]);
  unsigned size_cache = m_key_cache[key_cache_id].size();
  NdbMutex_Unlock(m_rwLock[key_cache_id]);
  return size_cache;
}

std::string APIKeyCache::to_string() {
#if (defined NUM_API_KEY_CACHES == 1)
  Uint32 hash = 0;
#else
  Uint32 hash = rondb_xxhash_std(apiKey.c_str(), apiKey.size());
#endif
  Uint32 key_cache_id = hash & (NUM_API_KEY_CACHES - 1);
  NdbMutex_Lock(m_rwLock[key_cache_id]);
  std::stringstream ss;
  for (const auto &entry : m_key_cache[key_cache_id]) {
    ss << "API Key: " << entry.first << ", UserDBs: ";
    for (const auto &db : entry.second->userDBs) {
      ss << db.first << ", ";
    }
    ss << std::endl;
  }
  NdbMutex_Unlock(m_rwLock[key_cache_id]);
  return ss.str();
}

Uint64 APIKeyCache::last_updated(const std::string &apiKey) {
#if (defined NUM_API_KEY_CACHES == 1)
  Uint32 hash = 0;
#else
  Uint32 hash = rondb_xxhash_std(apiKey.c_str(), apiKey.size());
#endif
  Uint32 key_cache_id = hash & (NUM_API_KEY_CACHES - 1);
  NdbMutex_Lock(m_rwLock[key_cache_id]);
  auto it = m_key_cache[key_cache_id].find(apiKey);
  if (it == m_key_cache[key_cache_id].end()) {
    NdbMutex_Unlock(m_rwLock[key_cache_id]);
    NDB_TICKS now = NdbTick_getCurrentTicks();
    return now.getUint64();
  }
  auto userDBs = it->second;
  NdbMutex_Lock(userDBs->m_waitLock);
  NdbMutex_Unlock(m_rwLock[key_cache_id]);
  Uint64 lastUpdated = userDBs->m_lastUpdated.getUint64();
  NdbMutex_Unlock(userDBs->m_waitLock);
  return lastUpdated;
}

