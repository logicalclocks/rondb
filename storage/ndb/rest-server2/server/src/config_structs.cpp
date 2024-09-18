/*
 * Copyright (c) 2023, 2024, Hopsworks and/or its affiliates.
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
#include "json_parser.hpp"
#include "constants.hpp"
#include "mysql_com.h"
#include "rdrs_dal.hpp"

#include <cstdlib>
#include <drogon/HttpAppFramework.h>
#include <drogon/HttpTypes.h>
#include <mutex>
#include <simdjson.h>
#include <sstream>
#include <string_view>
#include <utility>
#include <fstream>

AllConfigs globalConfigs;
std::mutex globalConfigsMutex;

Internal::Internal()
    : reqBufferSize(REQ_BUFFER_SIZE), respBufferSize(RESP_BUFFER_SIZE), preAllocatedBuffers(32),
      batchMaxSize(MAX_SIZE), operationIdMaxSize(MAX_SIZE) {
}

RS_Status Internal::validate() {
  if (preAllocatedBuffers == 0) {
    // TODO warning logger
  }
  if (reqBufferSize < 256 || respBufferSize < 256) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "buffer size too small")
        .status;
  }
  return CRS_Status::SUCCESS.status;
}

GRPC::GRPC() : enable(false), serverIP(DEFAULT_ROUTE), serverPort(DEFAULT_GRPC_PORT) {
}

RS_Status GRPC::validate() {
  // TODO Implement Me
  return CRS_Status::SUCCESS.status;
}

REST::REST()
    : enable(true), serverIP(DEFAULT_ROUTE), serverPort(DEFAULT_REST_PORT),
      numThreads(DEFAULT_NUM_THREADS) {
}

RS_Status REST::validate() {
  if (enable) {
    if (serverIP.empty()) {
      return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                        "REST server IP cannot be empty")
          .status;
    }
    if (serverPort == 0) {
      return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                        "REST server port cannot be empty")
          .status;
    }
  }
  return CRS_Status::SUCCESS.status;
}

RonDB::RonDB()
    : Mgmds({Mgmd()}), connectionPoolSize(1), nodeIDs({0}), connectionRetries(CONNECTION_RETRIES),
      connectionRetryDelayInSec(CONNECTION_RETRY_DELAY),
      opRetryOnTransientErrorsCount(OP_RETRY_TRANSIENT_ERRORS_COUNT),
      opRetryInitialDelayInMS(OP_RETRY_INITIAL_DELAY), opRetryJitterInMS(OP_RETRY_JITTER) {
}

RS_Status RonDB::validate() {
  if (Mgmds.empty()) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "at least one Management server has to be defined")
        .status;
  }
  if (Mgmds.size() > 1) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "we do not support specifying more than one Management server yet")
        .status;
  }
  RS_Status status;
  for (const auto &server : Mgmds) {
    status = server.validate();
    if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
      return status;
    }
  }

  if (connectionPoolSize < 1 || connectionPoolSize > 1) {
    return CRS_Status(
               static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
               "wrong connection pool size. Currently only single RonDB connection is supported")
        .status;
  }

  if (!nodeIDs.empty() && nodeIDs.size() != connectionPoolSize) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "wrong number of NodeIDs. The number of node ids must match the connection "
                      "pool size")
        .status;
  }
  if (nodeIDs.empty()) {
    nodeIDs = {0};
  }
  return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)).status;
}

TestParameters::TestParameters() {
}

RS_Status TestParameters::validate() {
  return CRS_Status::SUCCESS.status;
}

Mgmd::Mgmd() : IP(LOCALHOST), port(MGMD_DEFAULT_PORT) {
}

RS_Status Mgmd::validate() const {
  if (IP.empty()) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "the Management server IP cannot be empty")
        .status;
  }
  if (port == 0) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "the Management server port cannot be empty")
        .status;
  }
  return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)).status;
}

TLS::TLS() : enableTLS(false), requireAndVerifyClientCert(false) {
}

bool isUnitTest() {
  const char *env_var = std::getenv("RUNNING_UNIT_TESTS");
  return (env_var != nullptr && std::string(env_var) == "1");
}

RS_Status TLS::validate() {
  if (isUnitTest()) {
    return CRS_Status::SUCCESS.status;
  }
  if (enableTLS) {
    if (certificateFile.empty() || privateKeyFile.empty()) {
      return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                        "cannot enable TLS if `CertificateFile` or `PrivateKeyFile` is not set")
          .status;
    }
  } else {
    if (requireAndVerifyClientCert) {
      return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                        "cannot require client certificates if TLS is not enabled")
          .status;
    }
  }
  return testParameters.validate();
}

APIKey::APIKey()
    : useHopsworksAPIKeys(true), cacheRefreshIntervalMS(CACHE_REFRESH_INTERVAL_MS),
      cacheUnusedEntriesEvictionMS(CACHE_UNUSED_ENTRIES_EVICTION_MS),
      cacheRefreshIntervalJitterMS(CACHE_REFRESH_INTERVAL_JITTER_MS) {
}

RS_Status APIKey::validate() {
  if (cacheRefreshIntervalMS == 0) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "cache refresh interval cannot be 0")
        .status;
  }
  if (cacheUnusedEntriesEvictionMS == 0) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "cache unused entries eviction cannot be 0")
        .status;
  }
  if (cacheRefreshIntervalMS > cacheUnusedEntriesEvictionMS) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "cache refresh interval cannot be greater than cache unused entries eviction")
        .status;
  }
  if (cacheRefreshIntervalJitterMS >= cacheRefreshIntervalMS) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "cache refresh interval must be smaller than cache refresh interval jitter")
        .status;
  }
  return CRS_Status::SUCCESS.status;
}

Security::Security() : tls(TLS()), apiKey(APIKey()) {
}

RS_Status Security::validate() {
  auto status = tls.validate();
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }
  status = apiKey.validate();
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }

  return CRS_Status::SUCCESS.status;
}

Testing::Testing() : mySQL(MySQL()), mySQLMetadataCluster(MySQL()) {
}

RS_Status Testing::validate() {
  auto status = mySQL.validate();
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }
  if (mySQLMetadataCluster.servers.empty()) {
    mySQLMetadataCluster = mySQL;
  }
  status = mySQLMetadataCluster.validate();
  if (status.http_code != HTTP_CODE::SUCCESS) {
    return status;
  }

  return CRS_Status::SUCCESS.status;
}

std::string Testing::generate_mysqld_connect_string_data_cluster() {
  // user:password@tcp(IP:Port)/
  return mySQL.user + ":" + mySQL.password + "@tcp(" + mySQL.servers[0].IP + ":" +
         std::to_string(mySQL.servers[0].port) + ")/";
}

std::string Testing::generate_mysqld_connect_string_metadata_cluster() {
  // user:password@tcp(IP:Port)/
  return mySQLMetadataCluster.user + ":" + mySQLMetadataCluster.password + "@tcp(" +
         mySQLMetadataCluster.servers[0].IP + ":" +
         std::to_string(mySQLMetadataCluster.servers[0].port) + ")/";
}

MySQL::MySQL() : servers({MySQLServer()}), user(ROOT) {
}

RS_Status MySQL::validate() {
  if (servers.empty()) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "at least one MySQL server has to be defined")
        .status;
  }
  if (servers.size() > 1) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "we do not support specifying more than one MySQL server yet")
        .status;
  }

  for (const auto &server : servers) {
    auto status = server.validate();
    if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
      return status;
    }
  }

  if (user.empty()) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "the MySQL user cannot be empty")
        .status;
  }

  return CRS_Status::SUCCESS.status;
}

MySQLServer::MySQLServer() : IP(LOCALHOST), port(DEFAULT_MYSQL_PORT) {
}

RS_Status MySQLServer::validate() const {
  if (IP.empty()) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "the MySQL server IP cannot be empty")
        .status;
  }
  if (port == 0) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "the MySQL server port cannot be empty")
        .status;
  }
  return CRS_Status::SUCCESS.status;
}

std::string RonDB::generate_Mgmd_connect_string() {
  Mgmd mgmd = Mgmds[0];
  return mgmd.IP + ":" + std::to_string(mgmd.port);
}

AllConfigs::AllConfigs() {
  // Call default constructors
  internal             = Internal();
  pidfile              = "";
  rest                 = REST();
  grpc                 = GRPC();
  ronDB                = RonDB();
  ronDbMetaDataCluster = RonDB();
  security             = Security();
  log                  = LogConfig();
  testing              = Testing();
}

AllConfigs AllConfigs::get_all() {
  return globalConfigs;
}

RS_Status AllConfigs::validate() {
  RS_Status status;
  status = internal.validate();
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    return status;
  }
  status = rest.validate();
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    return status;
  }
  status = grpc.validate();
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    return status;
  }
  status = ronDB.validate();
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    return status;
  }
  status = ronDbMetaDataCluster.validate();
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    return status;
  }
  status = security.validate();
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    return status;
  }
  status = log.validate();
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    return status;
  }
  status = testing.validate();
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    return status;
  }

  return CRS_Status::SUCCESS.status;
}

RS_Status AllConfigs::set_all(AllConfigs newConfigs) {
  std::lock_guard<std::mutex> lock(globalConfigsMutex);
  RS_Status status = newConfigs.validate();
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    return status;
  }
  globalConfigs = newConfigs;
  return status;
}

RS_Status AllConfigs::set_from_file(const std::string &configFile) {
  AllConfigs newConfigs;
  // Read config file
  std::ifstream file(configFile);
  if (!file) {
    return CRS_Status(
               static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
               ("failed reading config file " + configFile +"; error: " + std::string(strerror(errno))).c_str())
        .status;
  }
  std::string configStr((std::istreambuf_iterator<char>(file)), {});

  file.close();

  // Parse config file
  RS_Status status = jsonParser.config_parse(configStr, newConfigs);

  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    return status;
  }

  return set_all(newConfigs);
}

RS_Status AllConfigs::init(std::string configFile) {
  if (configFile.empty()) {
    // Set to defaults
    AllConfigs newConfigs = AllConfigs();
    return set_all(newConfigs);
  }
  return set_from_file(configFile);
}
