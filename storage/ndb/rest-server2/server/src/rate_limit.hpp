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

#include <string>

/**
 * RONDB-978: Compute the rate limit identity for a request. The identity
 * is set on every NdbTransaction (NdbTransaction::setUserId) so the data
 * nodes can enforce USER rate limits. The data nodes resolve the identity
 * against the $-prefixed user entities created with the mgm USER commands;
 * an identity without a matching entity runs without rate limits.
 *
 * An empty result means rate limit tagging is disabled for this request.
 * By default the identity is the API key prefix (the part before the '.'),
 * NEVER the secret; the full key is only used when .RateLimit.FullAPIKey is
 * configured.
 */
inline std::string get_rate_limit_identity(const std::string &api_key) {
  if (!globalConfigs.rateLimit.enable || api_key.empty()) {
    return {};
  }
  if (globalConfigs.rateLimit.fullApiKey) {
    return api_key;
  }
  size_t dot_pos = api_key.find('.');
  if (dot_pos == std::string::npos) {
    return api_key;
  }
  return api_key.substr(0, dot_pos);
}

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_RATE_LIMIT_HPP_
