/*
 * Copyright (C) 2023 Hopsworks AB
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

#include "config_structs.hpp"

#include <drogon/HttpTypes.h>
#include <mutex>
#include <utility>

AllConfigs globalConfig;
std::mutex globalConfigMutex;

Internal::Internal()
    : bufferSize(BUFFER_SIZE), preAllocatedBuffers(32), batchMaxSize(MAX_SIZE),
      operationIdMaxSize(MAX_SIZE) {
}

GRPC::GRPC() : enable(false), serverIP("0.0.0.0"), serverPort(DEFAULT_GRPC_PORT) {
}

REST::REST()
    : enable(true), serverIP("0.0.0.0"), serverPort(DEFAULT_REST_PORT),
      numThreads(DEFAULT_NUM_THREADS) {
}

RonDB::RonDB()
    : Mgmds({Mgmd()}), connectionPoolSize(1), nodeIDs({0}), connectionRetries(CONNECTION_RETRIES),
      connectionRetryDelayInSec(CONNECTION_RETRY_DELAY),
      opRetryOnTransientErrorsCount(OP_RETRY_TRANSIENT_ERRORS_COUNT),
      opRetryInitialDelayInMS(OP_RETRY_INITIAL_DELAY), opRetryJitterInMS(OP_RETRY_JITTER) {
}

TestParameters::TestParameters() {
}

Mgmd::Mgmd() : IP("localhost"), port(MGMD_DEFAULT_PORT) {
}

TLS::TLS() : enableTLS(false), requireAndVerifyClientCert(false) {
}

APIKey::APIKey()
    : useHopsworksAPIKeys(true), cacheRefreshIntervalMS(CACHE_REFRESH_INTERVAL_MS),
      cacheUnusedEntriesEvictionMS(CACHE_UNUSED_ENTRIES_EVICTION_MS),
      cacheRefreshIntervalJitterMS(CACHE_REFRESH_INTERVAL_JITTER_MS) {
}

Security::Security() : tls(TLS()), apiKey(APIKey()) {
}

Testing::Testing() : mySQL(MySQL()), mySQLMetadataCluster(MySQL()) {
}

MySQL::MySQL() : servers({MySQLServer()}), user("root") {
}

MySQLServer::MySQLServer() : IP("localhost"), port(DEFAULT_MYSQL_PORT) {
}

RS_Status Mgmd::validate() const {
  if (IP.empty()) {
    return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                     "the Management server IP cannot be empty");
  }
  if (port == 0) {
    return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                     "the Management server port cannot be empty");
  }
  return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK));
}

RS_Status RonDB::validate() {
  if (Mgmds.empty()) {
    return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                     "at least one Management server has to be defined");
  }
  if (Mgmds.size() > 1) {
    return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                     "we do not support specifying more than one Management server yet");
  }
  RS_Status status;
  for (const auto &server : Mgmds) {
    status = server.validate();
    if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
      return status;
    }
  }

  if (connectionPoolSize < 1 || connectionPoolSize > 1) {
    return RS_Status(
        static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
        "wrong connection pool size. Currently only single RonDB connection is supported");
  }

  if (!nodeIDs.empty() && nodeIDs.size() != connectionPoolSize) {
    return RS_Status(
        static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
        "wrong number of NodeIDs. The number of node ids must match the connection pool size");
  }
  if (nodeIDs.empty()) {
    nodeIDs = {0};
  }
  return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK));
}

std::string RonDB::generate_Mgmd_connect_string() {
  Mgmd mgmd = Mgmds[0];
  return mgmd.IP + ":" + std::to_string(mgmd.port);
}

AllConfigs::AllConfigs() {
  // Call default constructors
  internal             = Internal();
  rest                 = REST();
  grpc                 = GRPC();
  ronDB                = RonDB();
  ronDbMetaDataCluster = RonDB();
  security             = Security();
  log                  = LogConfig();
  testing              = Testing();
}

AllConfigs AllConfigs::getAll() {
  return globalConfig;
}

void AllConfigs::setAll(AllConfigs allConfigs) {
  std::lock_guard<std::mutex> lock(globalConfigMutex);
  globalConfig = std::move(allConfigs);
}

void AllConfigs::setToDefaults() {
  std::lock_guard<std::mutex> lock(globalConfigMutex);
  globalConfig = AllConfigs();
}
