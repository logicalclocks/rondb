/*
 * Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.
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

/*
 * These class definitions for configuration are wrapped in macros so they can
 * be used for several things:
 * 1) Define the class
 * 2) Define the default constructor
 * 3) Define a function for validating an instance
 * 4) Define a JSON parser for the class
 * 5) Define a JSON printer for the class
 *
 * Macros used:
 * CLASS(Name, Contents) for class definition
 * CM(Datatype, Variablename, JSONKeyname, init expression, docstring) for data
 *     member included in parsing and printing
 * PROBLEM(condition, message) for validation
 * CLASSDEFS(Contents) for all other class definition content
 * VECTOR(Datatype) to indicate that a vector of this datatype will be used
 *
 * These macros should be defined before and undefined after including this
 * file.
 *
 * In order to be able to define all classes without having to first declare
 * them, the class definitions are in depth-first, deepest-first order.
 * I.e., something like:
 *   define_class_including_dependencies(class) {
 *     for ( element : variables_in(class) ) {
 *       if ( not_declared(datatype(element)) ) {
 *         define_class_including_dependencies(datatype(element));
 *       }
 *     }
 *     define_one_class(class);
 *   }
 *   define_class_including_dependencies(AllConfigs);
 */
#include <ndb_types.h>
#include "common.h"
#include "rondb.h"

// todo Add docstring for all CM()

CLASS
(Internal,
 CM(Uint32, maxReqSize, maxReqSize, 4 * 1024 * 1024,
    "The maximum HTTP body size for REST requests, in bytes. Must be at least"
    " 256.")
 CM(Uint32, reqBufferSize, ReqBufferSize, 1024 * 1024, "")
 CM(Uint32, respBufferSize, RespBufferSize, 5 * 1024 * 1024, "")
 CM(Uint32, preAllocatedBuffers, PreAllocatedBuffers, 32, "")
 CM(Uint32, batchMaxSize, BatchMaxSize, 1024,
    "Maximum number of requests contained in a batch request.")
 CM(Uint32, operationIdMaxSize, OperationIDMaxSize, 256,
    "Maximum length of operation ID strings.")
 //todo warn (preallocatedbuffers == 0, "preAllocatedBuffers should be > 0")
 PROBLEM(reqBufferSize < 256, "ReqBufferSize should be >= 256")
 PROBLEM(respBufferSize < 256, "RespBufferSize should be >= 256")
 PROBLEM(batchMaxSize > 1024, "BatchMaxSize should be <= 1024")
 PROBLEM(batchMaxSize < 1, "BatchMaxSize should be >= 1")
)

CLASS
(REST,
 CM(bool, enable, Enable, true, "Whether to enable the REST server.")
 CM(bool, useCompression, UseCompression, true, "Whether to send response gzip compressed")
 CM(std::string, serverIP, ServerIP, "0.0.0.0", "The IP address to listen on.")
 CM(Uint16, serverPort, ServerPort, 5406, "TCP port to listen on.")
 CM(unsigned, numThreads, NumThreads, 16,
    "Number of threads handling REST requests.")
 CM(bool, healthRequiresAPIKey, HealthRequiresAPIKey, false,
    "Set to true to authenticate the health endpoint. Only applies if"
    " .APIKey.UseHopsworksAPIKeys is also set to true.")
 CM(bool, pingRequiresAPIKey, PingRequiresAPIKey, false,
    "Set to true to authenticate the ping endpoint. Only applies if"
    " .APIKey.UseHopsworksAPIKeys is also set to true.")
 CM(bool, useSingleTransaction, UseSingleTransaction, true,
    "Set to true to use single transaction for entire batch.")
 PROBLEM(!enable, "REST must be enabled")
 PROBLEM(serverIP.empty(), "REST server IP cannot be empty")
 PROBLEM(serverPort == 0, "REST server port cannot be zero")
 PROBLEM(numThreads < RDRS_MIN_NUM_THREADS,
         "Number of REST threads cannot be less than "
         MACRO_TO_STRING_CONSTANT(RDRS_MIN_NUM_THREADS))
 PROBLEM(numThreads > 991, "Number of REST threads too high")
)

CLASS
(RondisDatabaseConfig,
 CM(int, index, Index, -1, // Set the default to an illegal value as a hack to
                           // make this field mandatory.
    "The name of the database, in the form of an integer. The first database is"
    " named 0.")
 CM(bool, fastHcount, FastHCOUNT, false, "Whether to optimize for HCOUNT")
 CM(bool, optimizeSmallValues, OptimizeSmallValues, true,
    "Whether to optimize for small values")
 CM(std::string, mode, Mode, "RONDB",
    "Database mode."
    " COMPATIBLE = todo add description."
    " CLUSTER = todo add description."
    " RONDB = todo add description."
    )
 CM(bool, dirtyIncrDecr, DirtyINCRDECR, false,
    "Whether to use dirty write for INCR and DECR")
 PROBLEM(mode != "COMPATIBLE" && mode != "CLUSTER" && mode != "RONDB",
         "Invalid rondis database mode")
 CLASSDEFS
 (
  RondisDatabaseConfig(int index)
  : index(index),
    fastHcount(false),
    mode("RONDB")
  {}
 )
)

VECTOR(RondisDatabaseConfig)

#define TO_STRING_CONSTANT(x) TO_STRING_CONSTANT_(x)
#define TO_STRING_CONSTANT_(x) #x

CLASS
(RondisConfig,
 CM(bool, enable, Enable, false, "Whether to enable the Rondis server.")
 CM(std::string, serverIP, ServerIP, "0.0.0.0", "The IP address to listen on.")
 CM(Uint16, serverPort, ServerPort, 6379, "TCP port to listen on.")
 CM(unsigned, numThreads, NumThreads, 16, "Number of rondis threads.")
 CM(unsigned, numDatabases, NumDatabases, 1, "Number of rondis databases.")
 CM(std::vector<RondisDatabaseConfig>, databases, Databases,
    {RondisDatabaseConfig(0)},
    "Database-specific configuration.")
 PROBLEM(serverIP.empty(), "Rondis server IP cannot be empty")
 PROBLEM(serverPort == 0, "Rondis server port cannot be zero.")
 PROBLEM(numThreads == 0, "Number of rondis threads cannot be zero.")
 PROBLEM(numThreads < RONDIS_MAX_CONNECTIONS,
         "Number of worker threads must be at least "
         TO_STRING_CONSTANT(RONDIS_MAX_CONNECTIONS))
 PROBLEM(numThreads > 991, "Number of rondis threads too high")
 PROBLEM(numDatabases == 0, "Number of rondis databases cannot be zero.")
 PROBLEM(numDatabases > MAX_NUM_DATABASES,
         "Number of rondis databases too high")
 PROBLEM(databases.size() != numDatabases,
         "Rondis.NumDatabases must match the length of Rondis.Databases")
 PROBLEM(value.hasIndexProblem(),
         "The indexes in rondis database configurations must start at 0 and"
         " increment by 1")
 CLASSDEFS
 (
  bool hasIndexProblem() {
   for (int i = 0; (long unsigned)i < databases.size(); i++) {
    if (databases[i].index != i) {
     return true;
    }
   }
   return false;
  }
 )
)

CLASS
(GRPC,
 CM(bool, enable, Enable, false, "Whether to enable the gRPC server.")
 CM(std::string, serverIP, ServerIP, "0.0.0.0", "The IP address to listen on.")
 CM(Uint16, serverPort, ServerPort, 4406, "TCP port to listen on.")
 PROBLEM(enable, "gRPC not supported")
)

CLASS
(Mgmd,
 CM(std::string, IP, IP, "localhost", "The IP address for an ndb_mgmd server.")
 CM(Uint16, port, Port, 1186 , "The TCP port for an ndb_mgmd server.")
 PROBLEM(IP.empty(), "the Management server IP cannot be empty")
 PROBLEM(port == 0, "the Management server port cannot be zero")
)

VECTOR(Mgmd)

VECTOR(Uint32)

CLASS
(RonDB,
 CM(std::vector<Mgmd>, Mgmds, Mgmds, {Mgmd()},
    "An array of ndb_mgmd servers in use by the cluster.")
 CM(Uint32, connectionPoolSize, ConnectionPoolSize, 1,
    "Number of RonDB cluster connections used to access data.")
 CM(std::vector<Uint32>, nodeIDs, NodeIDs, {},
    "An array of ConnectionPoolSize or zero length. Each element is a RonDB"
    " node ID, forcing the corresponding connection to be assigned to a"
    " specific node ID. If length is zero, node IDs are instead assigned by"
    " RonDB.")
 CM(Uint32, connectionRetries, ConnectionRetries, 5,
    "Connection retry attempts.")
 CM(Uint32, connectionRetryDelayInSec, ConnectionRetryDelayInSec, 5,
    "After a failed connection attempt, wait this number of seconds before"
    " trying again.")
 CM(Uint32, opRetryOnTransientErrorsCount, OpRetryOnTransientErrorsCount, 3,
    "Transient error retry count.")
 CM(Uint32, opRetryInitialDelayInMS, OpRetryInitialDelayInMS, 500,
    "Transient error initial delay.")
 CM(Uint32, opRetryJitterInMS, OpRetryJitterInMS, 100, "")
 PROBLEM(Mgmds.empty(), "at least one Management server has to be defined")
 PROBLEM(Mgmds.size() > 1,
         "we do not support specifying more than one Management server yet")
 PROBLEM(connectionPoolSize < 1,
         ".RonDB.ConnectionPoolSize must be at least 1.")
 PROBLEM(connectionPoolSize > 8,
         "wrong connection pool size. Currently at most 8 RonDB connections for"
         " data are supported.")
 PROBLEM(nodeIDs.size() != connectionPoolSize && nodeIDs.size() != 0,
         "wrong number of NodeIDs. The number of node ids must match the"
         " connection pool size or be 0 (in which case the node ids are"
         " selected by RonDB")
 CLASSDEFS
 (
  std::string generate_Mgmd_connect_string();
 )
)

CLASS
(RonDBMeta,
 CM(std::vector<Mgmd>, Mgmds, Mgmds, {Mgmd()},
    "An array of ndb_mgmd servers in use by the cluster.")
 CM(Uint32, connectionPoolSize, ConnectionPoolSize, 0,
    "Number of RonDB cluster connections used to access metadata. If set to 0,"
    " then the first RonDB cluster connection for data will be used for"
    " metadata as well.")
 CM(std::vector<Uint32>, nodeIDs, NodeIDs, {},
    "An array of ConnectionPoolSize or zero length. Each element is a RonDB"
    " node ID, forcing the corresponding connection to be assigned to a"
    " specific node ID. If length is zero, node IDs are instead assigned by"
    " RonDB.")
 CM(Uint32, connectionRetries, ConnectionRetries, 5,
    "Connection retry attempts.")
 CM(Uint32, connectionRetryDelayInSec, ConnectionRetryDelayInSec, 5,
    "After a failed connection attempt, wait this number of seconds before"
    " trying again.")
 CM(Uint32, opRetryOnTransientErrorsCount, OpRetryOnTransientErrorsCount, 3,
    "Transient error retry count.")
 CM(Uint32, opRetryInitialDelayInMS, OpRetryInitialDelayInMS, 500,
    "Transient error initial delay.")
 CM(Uint32, opRetryJitterInMS, OpRetryJitterInMS, 100, "")
 PROBLEM(Mgmds.empty() && connectionPoolSize > 1,
         "Unless .RonDBMeta.ConnectionPoolSize is 0, at least one Management"
         " server has to be defined")
 PROBLEM(Mgmds.size() > 1,
         "we do not support specifying more than one Management server yet")
 PROBLEM(connectionPoolSize > 1,
         "wrong connection pool size. Currently only 0 or 1 RonDB connections"
         " for metadata are supported")
 PROBLEM(nodeIDs.size() != connectionPoolSize && nodeIDs.size() != 0,
         "wrong number of NodeIDs. The number of node ids must match the"
         " connection pool size or be 0 (in which case the node ids are"
         " selected by RonDB")
 CLASSDEFS
 (
  std::string generate_Mgmd_connect_string();
 )
)

CLASS
(TestParameters,
 CM(std::string, clientCertFile, ClientCertFile, "",
    "Example client certificate used for testing")
 CM(std::string, clientKeyFile, ClientKeyFile, "",
    "Example client private key used for testing")
)

CLASS
(TLS,
 CM(bool, enableTLS, EnableTLS, false,
    "Whether to enable TLS for the REST server")
 CM(bool, requireAndVerifyClientCert, RequireAndVerifyClientCert, false,
    "Whether to require REST clients to provide a client certificate")
 CM(std::string, certificateFile, CertificateFile, "",
    "Path to the server certificate file")
 CM(std::string, privateKeyFile, PrivateKeyFile, "",
    "Path to the private key corresponding to the server certificate")
 CM(std::string, rootCACertFile, RootCACertFile, "",
    "Path to the CA certificate used to sign the server certificate")
 CM(TestParameters, testParameters, TestParameters, TestParameters(),
    "Parameters used for testing TLS. rdrs2 will not use these settings.")
 PROBLEM(enableTLS && (certificateFile.empty() ||
         privateKeyFile.empty()),
         "cannot enable TLS if `CertificateFile` or `PrivateKeyFile` is"
         " not set")
 PROBLEM(!enableTLS && requireAndVerifyClientCert,
         "cannot require client certificates if TLS is not enabled")
)

CLASS
(APIKey,
 CM(bool, useHopsworksAPIKeys, UseHopsworksAPIKeys, true, "")
 CM(Uint32, cacheRefreshIntervalMS, CacheRefreshIntervalMS, 10000, "")
 CM(Uint32, cacheUnusedEntriesEvictionMS, CacheUnusedEntriesEvictionMS, 60000,
    "")
 CM(Uint32, cacheRefreshIntervalJitterMS, CacheRefreshIntervalJitterMS, 1000,
    "")
 PROBLEM(cacheRefreshIntervalMS <= 0,
         "cache refresh interval must be greater than 0")
 PROBLEM(cacheUnusedEntriesEvictionMS <= 0,
         "cache unused entries eviction must be greater than 0")
 PROBLEM(cacheRefreshIntervalMS > cacheUnusedEntriesEvictionMS,
         "cache refresh interval cannot be greater than cache unused"
         " entries eviction")
 PROBLEM(cacheRefreshIntervalJitterMS >= cacheRefreshIntervalMS,
         "cache refresh interval must be smaller than cache refresh interval"
         " jitter")
)

CLASS
(Security,
 CM(TLS, tls, TLS, TLS(), "")
 CM(APIKey, apiKey, APIKey, APIKey(), "")
)

CLASS
(LogConfig,
 CM(std::string, level, Level, "warn", "")
 CM(std::string, filePath, FilePath, "", "")
 CM(int, maxSizeMb, MaxSizeMb, 100, "")
 CM(int, maxBackups, MaxBackups, 10, "")
 CM(int, maxAge, MaxAge, 30, "")
 // TODO implement validation
)

CLASS
(MySQLServer,
 CM(std::string, IP, IP, "localhost",
    "The IP or hostname for the MySQL server.")
 CM(Uint16, port, Port, 13001, "The TCP port for the MySQL server.")
 PROBLEM(IP.empty(), "the MySQL server IP cannot be empty")
 PROBLEM(port == 0, "the MySQL server port cannot be empty")
)

VECTOR(MySQLServer)

CLASS
(MySQL,
 CM(std::vector<MySQLServer>, servers, Servers, {MySQLServer()},
    "An array of objects, each describing one MySQL server in the cluster.")
 CM(std::string, user, User, "root",
    "The username to use for connecting to the MySQL servers.")
 CM(std::string, password, Password, "",
    "The password to use for connecting to the MySQL servers.")
 CLASSDEFS
 (
  std::string generate_mysqld_connect_string();
 )
 PROBLEM(servers.empty(), "at least one MySQL server has to be defined")
 PROBLEM(servers.size() > 1,
         "we do not support specifying more than one MySQL server yet")
 PROBLEM(user.empty(), "the MySQL user cannot be empty")
)

CLASS
(Testing,
 CM(MySQL, mySQL, MySQL, MySQL(),
    "An object describing the connection to a MySQL cluster used to store"
    " data.")
 CM(MySQL, mySQLMetadataCluster, MySQLMetadataCluster, MySQL(),
    "An object describing the connection to a MySQL cluster used to store"
    " metadata. It has the same schema as .Testing.MySQL.")
)

CLASS
(AllConfigs,
 CM(Internal, internal, Internal, Internal(), "")
 CM(std::string, pidfile, PIDFile, "",
    "Path to .pid file. The process ID will be written on startup, and the file"
    " will be deleted on exit.")
 CM(REST, rest, REST, REST(), "REST server settings.")
 CM(RondisConfig, rondis, Rondis, RondisConfig(),
    "An object describing configuration for the rondis server")
 CM(GRPC, grpc, GRPC, GRPC(),
    "gRPC server settings. gRPC is currently not supported.")
 CM(RonDB, ronDB, RonDB, RonDB(),
    "An object describing the connection to a RonDB cluster used to store"
    " data.")
 CM(RonDBMeta, ronDBMetadataCluster, RonDBMetadataCluster, RonDBMeta(),
    "An object describing the connection to a RonDB cluster used to store"
    " metadata. It has the same schema as .RonDB. If it is not present in the"
    " config file, then it will be set to .RonDB")
 CM(Security, security, Security, Security(), "")
 CM(LogConfig, log, Log, LogConfig(), "")
 CM(Testing, testing, Testing, Testing(),
    "Connetivity necessary for testing. rdrs2 will validate but not use these"
    " settings.")
 CLASSDEFS
 (
  static AllConfigs get_all();
  static RS_Status set_all(AllConfigs);
  static RS_Status set_from_file(const std::string &);
  static RS_Status init(std::string configFile);
 )
)
