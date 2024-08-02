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
  // TODO Implement Me
  return CRS_Status().status;
}

GRPC::GRPC() : enable(false), serverIP(DEFAULT_ROUTE), serverPort(DEFAULT_GRPC_PORT) {
}

RS_Status GRPC::validate() {
  // TODO Implement Me
  return CRS_Status().status;
}

REST::REST()
    : enable(true), serverIP(DEFAULT_ROUTE), serverPort(DEFAULT_REST_PORT),
      numThreads(DEFAULT_NUM_THREADS) {
}

RS_Status REST::validate() {
  // TODO Implement Me
  return CRS_Status().status;
}

std::string REST::string() {
  std::stringstream ss;
  ss << "enable: " << enable << ", serverIP: " << serverIP << ", serverPort: " << serverPort;
  return ss.str();
}

std::string Mgmd::string() {
  std::stringstream ss;
  ss << "IP: " << IP << ", port: " << port;
  return ss.str();
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

std::string RonDB::string() {
  std::stringstream ss;
  ss << "Mgmds: " << std::endl;
  for (auto &server : Mgmds) {
    ss << server.string() << std::endl;
  }
  return ss.str();
}

TestParameters::TestParameters() {
}

RS_Status TestParameters::validate() {
  // TODO Implement Me
  return CRS_Status().status;
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

RS_Status TLS::validate() {
  // TODO Implement Me
  return CRS_Status().status;
}

std::string TLS::string() {
  std::stringstream ss;
  ss << "enableTLS: " << enableTLS
     << ", requireAndVerifyClientCert: " << requireAndVerifyClientCert;
  return ss.str();
}

APIKey::APIKey()
    : useHopsworksAPIKeys(true), cacheRefreshIntervalMS(CACHE_REFRESH_INTERVAL_MS),
      cacheUnusedEntriesEvictionMS(CACHE_UNUSED_ENTRIES_EVICTION_MS),
      cacheRefreshIntervalJitterMS(CACHE_REFRESH_INTERVAL_JITTER_MS) {
}

RS_Status APIKey::validate() {
  // TODO Implement Me
  return CRS_Status().status;
}

std::string APIKey::string() {
  std::stringstream ss;
  ss << "useHopsworksAPIKeys: " << useHopsworksAPIKeys;
  return ss.str();
}

Security::Security() : tls(TLS()), apiKey(APIKey()) {
}

RS_Status Security::validate() {
  // TODO Implement Me
  return CRS_Status().status;
}

std::string Security::string() {
  std::stringstream ss;
  ss << "TLS: " << tls.string() << std::endl;
  ss << "APIKey: " << apiKey.string() << std::endl;
  return ss.str();
}

Testing::Testing() : mySQL(MySQL()), mySQLMetadataCluster(MySQL()) {
}

RS_Status Testing::validate() {
  // TODO Implement Me
  return CRS_Status().status;
}

std::string Testing::string() {
  std::stringstream ss;
  ss << "MySQL: " << mySQL.string() << std::endl;
  return ss.str();
}

MySQL::MySQL() : servers({MySQLServer()}), user(ROOT) {
}

RS_Status MySQL::validate() {
  // TODO Implement Me
  return CRS_Status().status;
}

std::string MySQL::string() {
  std::stringstream ss;
  ss << "servers: " << std::endl;
  for (auto &server : servers) {
    ss << server.string() << std::endl;
  }
  ss << "user: " << user << std::endl;
  ss << "password: " << password << std::endl;
  return ss.str();
}

MySQLServer::MySQLServer() : IP(LOCALHOST), port(DEFAULT_MYSQL_PORT) {
}

std::string MySQLServer::string() {
  std::stringstream ss;
  ss << "IP: " << IP << ", port: " << port;
  return ss.str();
}

RS_Status MySQLServer::validate() {
  // TODO Implement Me
  return CRS_Status().status;
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

  return CRS_Status().status;
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

RS_Status AllConfigs::set_to_defaults() {
  AllConfigs newConfigs = AllConfigs();
  return set_all(newConfigs);
}

RS_Status AllConfigs::set_from_file_if_exists(const std::string &configFile) {
  if (configFile.empty()) {
    return set_to_defaults();
  }
  return set_from_file(configFile);
}

RS_Status AllConfigs::set_from_file(const std::string &configFile) {
  AllConfigs newConfigs;
  // Read config file
  std::ifstream file(configFile);
  if (!file) {
    return CRS_Status(
               static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
               ("failed reading config file; error: " + std::string(strerror(errno))).c_str())
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

RS_Status AllConfigs::init() {
  std::string configFile;
  const char *env_val = std::getenv(CONFIG_FILE_PATH);
  if (env_val != nullptr) {
    configFile = env_val;
  }
  return set_from_file_if_exists(configFile);
}

std::string AllConfigs::string() {
  std::lock_guard<std::mutex> lock(globalConfigsMutex);
  std::stringstream ss;
  ss << "REST: " << rest.string() << std::endl;
  ss << "RonDB: " << ronDB.string() << std::endl;
  ss << "Security: " << security.string() << std::endl;
  ss << "Log: " << log.string() << std::endl;
  ss << "Testing: " << testing.string() << std::endl;
  return ss.str();
}
