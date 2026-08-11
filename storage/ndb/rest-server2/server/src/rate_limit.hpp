/*
 * Copyright (c) 2024, 2026, Hopsworks and/or its affiliates.
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_RATE_LIMIT_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_RATE_LIMIT_HPP_

#include "config_structs.hpp"
#include "api_key.hpp"

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>

/**
 * RONDB-978: Compute the rate limit identity for a request. The identity
 * is set on every NdbTransaction (NdbTransaction::setUserId) so the data
 * nodes can enforce USER rate limits. The data nodes resolve the identity
 * against the $-prefixed user entities created with the mgm USER commands;
 * an identity without a matching entity runs without rate limits.
 *
 * An empty result means rate limit tagging is disabled for this request.
 *
 * RateLimitIdentity selects what the identity is:
 * - "apikey": the API key prefix (the part before the '.'), NEVER the
 *   secret; the full key only when RateLimitFullAPIKey is configured.
 * - "username": the Hopsworks project-user of the key's owner acting in
 *   the project that owns target_db, using the online-FS MySQL account
 *   convention clip31(ProjectName + "_" + username) so REST and MySQL
 *   traffic of the same (project, member) share one bucket. rlIdentities
 *   is filled during authenticate(); target_db is the database the
 *   request is billed to - the URL database or the feature view's own
 *   feature store name, which are both the lowercased project name. A db
 *   the key's user is no member of (shared stores, system dbs) yields an
 *   empty identity: unmetered.
 */
inline std::string get_rate_limit_identity(
  const std::string &api_key,
  const RateLimitIdentities &rlIdentities,
  std::string_view target_db) {
  if (!globalConfigs.rest.userRateLimits || api_key.empty()) {
    return {};
  }
  if (globalConfigs.rest.rateLimitIdentity == "username") {
    std::string lower_db(target_db);
    std::transform(lower_db.begin(), lower_db.end(), lower_db.begin(),
               [](unsigned char c) { return std::tolower(c); });
    auto it = rlIdentities.per_db.find(lower_db);
    if (it == rlIdentities.per_db.end()) {
      return {};
    }
    return it->second;
  }
  if (globalConfigs.rest.rateLimitFullApiKey) {
    return api_key;
  }
  size_t dot_pos = api_key.find('.');
  if (dot_pos == std::string::npos) {
    return api_key;
  }
  return api_key.substr(0, dot_pos);
}

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_RATE_LIMIT_HPP_
