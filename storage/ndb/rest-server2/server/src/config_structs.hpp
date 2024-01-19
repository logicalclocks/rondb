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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_CONFIG_STRUCTS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_CONFIG_STRUCTS_HPP_

#include "log.hpp"
#include "rdrs_dal.h"

#include <string>
#include <mutex>
#include <cstdint>
#include <sys/types.h>
#include <vector>

const int BUFFER_SIZE                      = 5 * 1024 * 1024;
const int MAX_SIZE                         = 256;
const int DEFAULT_GRPC_PORT                = 4406;
const int DEFAULT_REST_PORT                = 5406;
const int DEFAULT_NUM_THREADS              = 16;
const int CONNECTION_RETRIES               = 5;
const int CONNECTION_RETRY_DELAY           = 5;
const int OP_RETRY_TRANSIENT_ERRORS_COUNT  = 3;
const int OP_RETRY_INITIAL_DELAY           = 500;
const int OP_RETRY_JITTER                  = 100;
const int MGMD_DEFAULT_PORT                = 13000;
const int CACHE_REFRESH_INTERVAL_MS        = 10000;
const int CACHE_UNUSED_ENTRIES_EVICTION_MS = 60000;
const int CACHE_REFRESH_INTERVAL_JITTER_MS = 1000;
const int DEFAULT_MYSQL_PORT               = 13001;

class AllConfigs;

extern AllConfigs globalConfig;
extern std::mutex globalConfigMutex;

class Internal {
 public:
  uint32_t bufferSize;
  uint32_t preAllocatedBuffers;
  uint32_t batchMaxSize;
  uint32_t operationIdMaxSize;
  RS_Status validate();
  Internal();
  Internal(uint32_t, uint32_t, int, uint32_t, uint32_t);
};

class GRPC {
 public:
  bool enable;
  std::string serverIP;
  uint16_t serverPort;
  RS_Status validate();
  GRPC();
  GRPC(bool, std::string, uint16_t);
};

class REST {
 public:
  bool enable;
  std::string serverIP;
  uint16_t serverPort;
  unsigned numThreads;
  RS_Status validate();
  REST();
  REST(bool, std::string, uint16_t);
};

class MySQLServer {
 public:
  std::string IP;
  uint16_t port;
  RS_Status validate();
  MySQLServer();
  MySQLServer(std::string, uint16_t);
};

class MySQL {
 public:
  std::vector<MySQLServer> servers;
  std::string user;
  std::string password;
  RS_Status validate();
  MySQL();
  MySQL(std::vector<MySQLServer>, std::string, std::string);
};

class Testing {
 public:
  MySQL mySQL;
  MySQL mySQLMetadataCluster;
  RS_Status validate();
  std::string generate_mysqld_connect_string_data_cluster();
  std::string generate_mysqld_connect_string_metadata_cluster();
  Testing();
  Testing(MySQL, MySQL);
};

class Mgmd {
 public:
  std::string IP;
  uint16_t port;
  RS_Status validate() const;
  Mgmd();
  Mgmd(std::string, uint16_t);
};

class RonDB {
 public:
  std::vector<Mgmd> Mgmds;

  // Connection pool size. Default 1
  // Note current implementation only supports 1 connection
  // TODO JIRA RonDB-245
  uint32_t connectionPoolSize;

  // This is the list of node ids to force the connections to be assigned to specific node ids.
  // If this property is specified and connection pool size is not the default,
  // the number of node ids must match the connection pool size
  std::vector<uint32_t> nodeIDs;

  // Connection retry attempts.
  uint32_t connectionRetries;
  uint32_t connectionRetryDelayInSec;

  // Transient error retry count and initial delay
  uint32_t opRetryOnTransientErrorsCount;
  uint32_t opRetryInitialDelayInMS;
  uint32_t opRetryJitterInMS;
  RS_Status validate();
  std::string generate_Mgmd_connect_string();
  RonDB();
  RonDB(std::vector<Mgmd>, uint32_t, std::vector<uint32_t>, uint32_t, uint32_t, uint32_t, uint32_t,
        uint32_t, uint32_t);
};

class TestParameters {
 public:
  std::string clientCertFile;
  std::string clientKeyFile;
  TestParameters();
  TestParameters(std::string, std::string);
};

class APIKey {
 public:
  bool useHopsworksAPIKeys;
  uint32_t cacheRefreshIntervalMS;
  uint32_t cacheUnusedEntriesEvictionMS;
  uint32_t cacheRefreshIntervalJitterMS;
  void validate();
  APIKey();
  APIKey(bool, uint32_t, uint32_t, uint32_t);
};

class TLS {
 public:
  bool enableTLS;
  bool requireAndVerifyClientCert;
  std::string certificateFile;
  std::string privateKeyFile;
  std::string rootCACertFile;
  TestParameters testParameters;
  void validate();
  TLS();
  TLS(bool, bool, std::string, std::string, std::string, TestParameters);
};

class Security {
 public:
  TLS tls;
  APIKey apiKey;
  void validate();
  Security();
  Security(TLS, APIKey);
};

class AllConfigs {
 public:
  Internal internal;
  REST rest;
  GRPC grpc;
  RonDB ronDB;
  RonDB ronDbMetaDataCluster;
  Security security;
  LogConfig log;
  Testing testing;
  void validate();
  std::string string();
  AllConfigs();
  AllConfigs(Internal, REST, GRPC, RonDB, RonDB, Security, LogConfig, Testing);
  static AllConfigs getAll();
  static void setAll(AllConfigs);
  static void setToDefaults();
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_CONFIG_STRUCTS_HPP_
