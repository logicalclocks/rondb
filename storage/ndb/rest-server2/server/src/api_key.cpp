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
#include <pthread.h>
#include <thread>
#include <unordered_map>
#include <functional>
#include <algorithm>

std::shared_ptr<APIKeyCache> apiKeyCache;

std::vector<std::string> split(const std::string &, char);
RS_Status computeHash(const std::string &unhashed, std::string &hashed);

RS_Status authenticate(const std::string &apiKey, PKReadParams &params) {
  return apiKeyCache->validate_api_key(apiKey, {params.path.db});
}

RS_Status APIKeyCache::validate_api_key_format(const std::string &apiKey) {
  if (apiKey.empty()) {
    return CRS_Status(HTTP_CODE::CLIENT_ERROR, "the apikey is nil").status;
  }
  auto splits = split(apiKey, '.');
  if (splits.size() != 2 || splits[0].length() != 16 || splits[1].length() < 1) {
    return CRS_Status(HTTP_CODE::CLIENT_ERROR, "the apikey has an incorrect format").status;
  }
  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::validate_api_key(const std::string &apiKey, const std::initializer_list<std::string>& dbs) {
  RS_Status status = validate_api_key_format(apiKey);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }

  if (dbs.size() == 0) {
    return CRS_Statu::SUCCESS.status;
  }

  // Authenticates only using the the cache. No request sent to backend
  bool keyFoundInCache = false;
  bool allowedAccess   = false;
  status               = find_and_validate(apiKey, keyFoundInCache, allowedAccess, dbs);

  if (keyFoundInCache) {
    if (!allowedAccess) {
      return CRS_Status(HTTP_CODE::CLIENT_ERROR,
                        ("unauthorized. Found in cache: " +
                         std::string(keyFoundInCache ? "true" : "false") +
                         ", allowed access: " + std::string(allowedAccess ? "true" : "false"))
                            .c_str())
          .status;
    }
    return CRS_Status::SUCCESS.status;
  }

  // Update the cache by fetching the API key from backend
  status = update_cache(apiKey);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }

  // Authenticates only using the the cache. No request sent to backend
  status = find_and_validate_again(apiKey, keyFoundInCache, allowedAccess, dbs);
  if (!keyFoundInCache || !allowedAccess) {
    return CRS_Status(HTTP_CODE::CLIENT_ERROR,
                      ("api key is unauthorized; updated cache - found in cache: " +
                       std::string(keyFoundInCache ? "true" : "false") +
                       ", allowed access: " + std::string(allowedAccess ? "true" : "false"))
                          .c_str())
        .status;
  }

  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::validate_api_key_no_cache(const std::string &apiKey, const std::initializer_list<std::string>& dbs) {
  RS_Status status = validate_api_key_format(apiKey);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }

  if (dbs.size() == 0) {
    return CRS_Status::SUCCESS.status;
  }

  HopsworksAPIKey key;
  status = authenticate_user(apiKey, key);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }

  std::vector<std::string> userDBs;
  status = get_user_databases(key, userDBs);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }

  // Check if the user has access to the database
  for (const auto &db : dbs) {
    if (std::find(userDBs.begin(), userDBs.end(), db) == userDBs.end()) {
      return CRS_Status(HTTP_CODE::CLIENT_ERROR,
                        ("api key is unauthorized to access " + db).c_str())
          .status;
    }
  }

  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::cleanup() {
  WriteLock lock(key2UserDBsCacheLock);

  // TODO Logger

  // Clean up all entries
  for (auto it = key2UserDBsCache.begin(); it != key2UserDBsCache.end(); it++) {
    it->second->evicted = true;
  }

  key2UserDBsCache = std::unordered_map<std::string, std::shared_ptr<UserDBs>>();

  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::update_cache(const std::string &apiKey) {
  // if the entry does not already exist in the
  // cache then multiple clients will try to read and
  // update the API key from the backend simultaneously.
  // Trying to prevent multiple writers here

  // Check if the key exists
  bool shouldUpdate = false;
  {
    ReadLock readLock(key2UserDBsCacheLock);
    if (key2UserDBsCache.find(apiKey) == key2UserDBsCache.end()) {
      // The entry does not exist, mark for update
      shouldUpdate = true;
    } else {
      // Increase reference count to the usdbs
      key2UserDBsCache[apiKey]->refCount++;
    }
  }  // Mutex is automatically released here

  if (shouldUpdate) {
    // Lock again to update the cache securely
    WriteLock lock(key2UserDBsCacheLock);
    // Double-check pattern in case another thread has already updated the cache
    if (key2UserDBsCache.find(apiKey) ==
        key2UserDBsCache.end()) {  // the entry still does not exists. insert a new row
      auto udbs             = std::shared_ptr<UserDBs>(new UserDBs());  // Copy constructor deleted
      udbs->refreshInterval = refresh_interval_with_jitter();
      key2UserDBsCache[apiKey] = udbs;
      start_update_ticker(apiKey, key2UserDBsCache[apiKey]);
    }
    // Increase reference count to the usdbs
    key2UserDBsCache[apiKey]->refCount++;
  }

  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::update_record(std::vector<std::string> dbs,
                               UserDBs *udbs) {
  // caller holds the lock
  if (udbs->evicted) {
    return CRS_Status::SUCCESS.status;
  }

  std::unordered_map<std::string, bool> dbsMap;
  for (const auto &db : dbs) {
    dbsMap[db] = true;
  }

  udbs->userDBs     = dbsMap;
  udbs->lastUpdated = std::chrono::system_clock::now();

  return CRS_Status::SUCCESS.status;
}

std::chrono::system_clock::time_point APIKeyCache::last_used(const std::string &apiKey) {
  ReadLock readLock(key2UserDBsCacheLock);
  auto it = key2UserDBsCache.find(apiKey);
  if (it == key2UserDBsCache.end()) {
    return std::chrono::system_clock::time_point();
  }
  return it->second->lastUsed;
}

std::chrono::system_clock::time_point APIKeyCache::last_updated(const std::string &apiKey) {
  ReadLock readLock(key2UserDBsCacheLock);
  auto it = key2UserDBsCache.find(apiKey);
  if (it == key2UserDBsCache.end()) {
    return std::chrono::system_clock::time_point();
  }
  return it->second->lastUpdated;
}

RS_Status APIKeyCache::start_update_ticker(const std::string &apiKey,
                                           std::shared_ptr<UserDBs> & /*udbs*/) {
  auto started = std::make_shared<std::atomic<bool>>(false);
  std::thread([this, apiKey, started]() { cache_entry_updater(apiKey, started); }).detach();

  while (!started->load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::cache_entry_updater(const std::string &apiKey,
                                           std::shared_ptr<std::atomic<bool>> started) {
  auto it = key2UserDBsCache.find(apiKey);
  if (it == key2UserDBsCache.end()) {
    return CRS_Status(HTTP_CODE::CLIENT_ERROR, "API key not found in cache").status;
  }

  std::shared_ptr<UserDBs> shared_udbs = it->second;  // keeps UserDBs alive
  UserDBs *udbs                        = shared_udbs.get();
  if (udbs == nullptr) {
    return CRS_Status(HTTP_CODE::SERVER_ERROR,
                      ("Cache updater failed. Report programming error. API Key " + apiKey).c_str())
        .status;
  }

  RS_Status status;

  while (true) {
    pthread_rwlock_wrlock(&udbs->rowLock);
    started->store(true);
    bool fail = false;

    HopsworksAPIKey key;
    if (!udbs->evicted) {
      status = authenticate_user(apiKey, key);
      if (status.http_code != HTTP_CODE::SUCCESS) {
        // TODO log
        fail = true;
      }
    }

    if (!fail && !udbs->evicted) {
      std::vector<std::string> dbs;
      status = get_user_databases(key, dbs);
      if (status.http_code != HTTP_CODE::SUCCESS) {
        // TODO log
      }

      status = update_record(dbs, udbs);
      if (status.http_code != HTTP_CODE::SUCCESS) {
        // TODO log
      }
    }

    pthread_rwlock_unlock(&udbs->rowLock);

    std::this_thread::sleep_for(udbs->refreshInterval);

    if (udbs->evicted) {
      // no need for cleanup as evicter cleans up
      return CRS_Status::SUCCESS.status;
    }

    pthread_rwlock_rdlock(&udbs->rowLock);
    auto lastUsed = udbs->lastUsed;
    pthread_rwlock_unlock(&udbs->rowLock);

    if (std::chrono::system_clock::now() - lastUsed >=
        std::chrono::milliseconds(globalConfigs.security.apiKey.cacheUnusedEntriesEvictionMS)) {
      WriteLock lock(key2UserDBsCacheLock);
      if (udbs->refCount <= 0) {
        udbs->evicted = true;
        key2UserDBsCache.erase(apiKey);
        // TODO debug logger that cache entry removed
      } else {
        continue;
      }
      break;
    }
  }

  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::find_and_validate(const std::string &apiKey, bool &keyFoundInCache,
                                   bool &allowedAccess, const std::initializer_list<std::string>& dbs) {
  keyFoundInCache          = false;
  allowedAccess            = false;

  pthread_rwlock_rdlock(&key2UserDBsCacheLock);
  auto it = key2UserDBsCache.find(apiKey);
  if (it == key2UserDBsCache.end()) {
    pthread_rwlock_unlock(&key2UserDBsCacheLock);
    return CRS_Status(HTTP_CODE::CLIENT_ERROR, "API key not found in cache").status;
  }

  auto userDBs = it->second;
  if (userDBs == nullptr) {
    pthread_rwlock_unlock(&key2UserDBsCacheLock);
    return CRS_Status(HTTP_CODE::CLIENT_ERROR, "API key found in cache but userDBs is null").status;
  }
  pthread_rwlock_unlock(&key2UserDBsCacheLock);
  ReadLock readLock(userDBs->rowLock);

  // update TS
  userDBs->lastUsed = std::chrono::system_clock::now();

  keyFoundInCache = true;

  for (const auto &db : dbs) {
    if (userDBs->userDBs.find(db) == userDBs->userDBs.end()) {
      allowedAccess = false;
      return CRS_Status(HTTP_CODE::CLIENT_ERROR, ("API key not authorized to access " + db).c_str())
          .status;
    }
  }
  allowedAccess = true;

  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::find_and_validate_again(const std::string &apiKey, bool &keyFoundInCache,
                                   bool &allowedAccess, const std::initializer_list<std::string>& dbs) {
  keyFoundInCache          = false;
  allowedAccess            = false;

  pthread_rwlock_rdlock(&key2UserDBsCacheLock);
  auto it = key2UserDBsCache.find(apiKey);
  if (it == key2UserDBsCache.end()) {
    pthread_rwlock_unlock(&key2UserDBsCacheLock);
    return CRS_Status(HTTP_CODE::CLIENT_ERROR, "API key not found in cache").status;
  }

  auto userDBs = it->second;
  if (userDBs == nullptr) {
    pthread_rwlock_unlock(&key2UserDBsCacheLock);
    return CRS_Status(HTTP_CODE::CLIENT_ERROR, "API key found in cache but userDBs is null").status;
  }
  pthread_rwlock_unlock(&key2UserDBsCacheLock);
  ReadLock readLock(userDBs->rowLock);

  // update TS
  userDBs->lastUsed = std::chrono::system_clock::now();

  keyFoundInCache = true;

  for (const auto &db : dbs) {
    if (userDBs->userDBs.find(db) == userDBs->userDBs.end()) {
      allowedAccess = false;
      userDBs->refCount--;
      return CRS_Status(HTTP_CODE::CLIENT_ERROR, ("API key not authorized to access " + db).c_str())
          .status;
    }
  }

  allowedAccess = true;

  // Decrement the reference count
  userDBs->refCount--;

  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::authenticate_user(const std::string &apiKey, HopsworksAPIKey &key) {
  auto splits       = split(apiKey, '.');
  auto prefix       = splits[0];
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

RS_Status APIKeyCache::get_user_databases(HopsworksAPIKey &key, std::vector<std::string> &dbs) {
  RS_Status status = get_user_projects(key.user_id, dbs);
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }
  return CRS_Status::SUCCESS.status;
}

RS_Status APIKeyCache::get_user_projects(int uid, std::vector<std::string> &dbs) {
  int count        = 0;
  char **projects  = nullptr;
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

RS_Status APIKeyCache::get_api_key(const std::string &userKey, HopsworksAPIKey &key) {
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
  RS_Status status = CRS_Status(HTTP_CODE::CLIENT_ERROR, "Failed to compute hash").status;

  auto deleter = [](EVP_MD_CTX *ctx) { EVP_MD_CTX_free(ctx); };
  std::unique_ptr<EVP_MD_CTX, decltype(deleter)> mdCtx(EVP_MD_CTX_new(), deleter);

  if (mdCtx) {
    if (EVP_DigestInit_ex(mdCtx.get(), EVP_sha256(), nullptr) != 0) {
      if (EVP_DigestUpdate(mdCtx.get(), unhashed.c_str(), unhashed.length()) != 0) {
        unsigned char hash[EVP_MAX_MD_SIZE];
        unsigned int lengthOfHash = 0;

        if (EVP_DigestFinal_ex(mdCtx.get(), hash, &lengthOfHash) != 0) {
          std::stringstream ss;
          for (unsigned int i = 0; i < lengthOfHash; ++i) {
            ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
          }

          hashed = ss.str();
          status = CRS_Status().status;
        }
      }
    }
  }

  return status;
}

unsigned APIKeyCache::size() {
  ReadLock readLock(key2UserDBsCacheLock);
  return key2UserDBsCache.size();
}

std::chrono::milliseconds APIKeyCache::refresh_interval_with_jitter() {
  uint32_t cacheRefreshIntervalMS = globalConfigs.security.apiKey.cacheRefreshIntervalMS;
  int jitterMS = static_cast<int>(globalConfigs.security.apiKey.cacheRefreshIntervalJitterMS);
  std::uniform_int_distribution<int32_t> dist(0, jitterMS);
  int32_t jitter = dist(randomGenerator);
  if (jitter % 2 == 0) {
    jitter = -jitter;
  }
  return std::chrono::milliseconds(static_cast<int32_t>(cacheRefreshIntervalMS) + jitter);
}

std::string APIKeyCache::to_string() {
  ReadLock readLock(key2UserDBsCacheLock);
  std::stringstream ss;
  for (const auto &entry : key2UserDBsCache) {
    ss << "API Key: " << entry.first << ", UserDBs: ";
    for (const auto &db : entry.second->userDBs) {
      ss << db.first << ", ";
    }
    ss << std::endl;
  }
  return ss.str();
}
