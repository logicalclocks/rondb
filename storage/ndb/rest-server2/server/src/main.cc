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

constexpr const char* const usageHelp =
  "Usage: rdrs2 [ --config PATH ] [ --help ] [ --help-config ]\n"
  "\n"
  "-c, --config PATH   Use a JSON-format config file. Available config variables\n"
  "                    can be printed by --help-config. Additionally, keys\n"
  "                    beginning with a hash (#) are allowed and ignored, and can\n"
  "                    be used as comments.\n"
  "\n"
  "--print-config      Print the effective configuration that would be used.\n"
  "\n"
  "-v, --version       Print version information.\n"
  "\n"
  "-?, --help          Show this usage help.\n"
  "\n"
  "--help-config       List the available config options.\n"
  ;
constexpr const char* const configHelp =
  "Config parameters supported in config file (all are optional):\n"
  "\n"
  // todo Add explanation for all parameters
  ".Internal.ReqBufferSize                        \n"
  "\n"
  ".Internal.RespBufferSize                       \n"
  "\n"
  ".Internal.PreAllocatedBuffers                  \n"
  "\n"
  ".Internal.BatchMaxSize                         \n"
  "\n"
  ".Internal.OperationIDMaxSize                   \n"
  "\n"
  ".REST.Enable                                   \n"
  "\n"
  ".REST.ServerIP                                 \n"
  "\n"
  ".REST.ServerPort                               \n"
  "\n"
  ".GRPC.Enable                                   \n"
  "\n"
  ".GRPC.ServerIP                                 \n"
  "\n"
  ".GRPC.ServerPort                               \n"
  "\n"
  ".PIDFile                                       Path to .pid file. The process ID\n"
  "                                               will be written on startup, and\n"
  "                                               the file will be deleted on exit.\n"
  "\n"
  ".RonDB                                         An object describing the\n"
  "                                               connection to a RonDB cluster\n"
  "                                               used to store data.\n"
  "\n"
  ".RonDB.Mgmds                                   An array of ndb_mgmd servers in\n"
  "                                               use by the cluster.\n"
  "\n"
  ".RonDB.Mgmds[].IP                              The IP address for an ndb_mgmd\n"
  "                                               server.\n"
  "\n"
  ".RonDB.Mgmds[].Port                            The TCP port for an ndb_mgmd\n"
  "                                               server.\n"
  "\n"
  ".RonDB.ConnectionPoolSize                      \n"
  "\n"
  ".RonDB.NodeIDs                                 \n"
  "\n"
  ".RonDB.NodeIDs[]                               \n"
  "\n"
  ".RonDB.ConnectionRetries                       \n"
  "\n"
  ".RonDB.ConnectionRetryDelayInSec               \n"
  "\n"
  ".RonDB.OpRetryOnTransientErrorsCount           \n"
  "\n"
  ".RonDB.OpRetryInitialDelayInMS                 \n"
  "\n"
  ".RonDB.OpRetryJitterInMS                       \n"
  "\n"
  ".RonDBMetadataCluster                          An object describing the\n"
  "                                               connection to a RonDB cluster\n"
  "                                               used to store metadata. It has\n"
  "                                               the same schema as .RonDB.\n"
  "\n"
  ".Security.TLS.EnableTLS                        \n"
  "\n"
  ".Security.TLS.RequireAndVerifyClientCert       \n"
  "\n"
  ".Security.TLS.CertificateFile                  \n"
  "\n"
  ".Security.TLS.PrivateKeyFile                   \n"
  "\n"
  ".Security.TLS.RootCACertFile                   \n"
  "\n"
  ".Security.TLS.TestParameters.ClientCertFile    \n"
  "\n"
  ".Security.TLS.TestParameters.ClientKeyFile     \n"
  "\n"
  ".Security.APIKey.UseHopsworksAPIKeys           \n"
  "\n"
  ".Security.APIKey.CacheRefreshIntervalMS        \n"
  "\n"
  ".Security.APIKey.CacheUnusedEntriesEvictionMS  \n"
  "\n"
  ".Security.APIKey.CacheRefreshIntervalJitterMS  \n"
  "\n"
  ".Log.Level                                     \n"
  "\n"
  ".Log.FilePath                                  \n"
  "\n"
  ".Log.MaxSizeMB                                 \n"
  "\n"
  ".Log.MaxBackups                                \n"
  "\n"
  ".Log.MaxAge                                    \n"
  "\n"
  ".Testing.MySQL                                 \n"
  "\n"
  ".Testing.MySQL.Servers                         \n"
  "\n"
  ".Testing.MySQL.Servers[].IP                    \n"
  "\n"
  ".Testing.MySQL.Servers[].Port                  \n"
  "\n"
  ".Testing.MySQL.User                            \n"
  "\n"
  ".Testing.MySQL.Password                        \n"
  "\n"
  ".Testing.MySQLMetadataCluster                  An object with the same schema as\n"
  "                                               .Testing.MySQL.\n"
  ;

#include "connection.hpp"
#include "config_structs.hpp"
#include "rdrs_dal.h"
#include "json_parser.hpp"
#include "json_printer.hpp"
#include "pk_read_ctrl.hpp"
#include "src/api_key.hpp"
#include "tls_util.hpp"
#include <ndb_opts.h>

#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <sys/errno.h>
#include <thread>
#include <sstream>
#include <unistd.h>
#include <csignal>

const char *pidfile = nullptr;

void do_exit(int exit_code) {
  if (pidfile != nullptr) {
    printf("Removing pidfile %s\n", pidfile);
    remove(pidfile);
  }
  exit(exit_code);
}

void handle_signal(int signal) {
  switch (signal) {
    case SIGINT:
      printf("Main thread received SIGINT\n");
      drogon::app().quit();
      do_exit(128+signal);
      break;
    case SIGTERM:
      printf("Main thread received SIGTERM\n");
      drogon::app().quit();
      do_exit(128+signal);
      break;
    default:
      printf("Signal handler received unexpected signal %d\n", signal);
      do_exit(70);
  }
}

int main(int argc, char *argv[]) {
  signal(SIGTERM, handle_signal);
  signal(SIGINT, handle_signal);

  jsonParser = JSONParser();
  apiKeyCache = std::make_shared<APIKeyCache>();

  /*
    Config is fetched from:
    1. File given by --config argument, or
    2. File given by RDRS_CONFIG_FILE environment variable, or
    3. Hard-coded defaults
  */

  std::string configFile;
  const char *env_config_file_path = std::getenv("RDRS_CONFIG_FILE");
  if (env_config_file_path != nullptr) {
    configFile = env_config_file_path;
  }

  bool seenOptConfig = false;
  bool optPrintConfig = false;
  bool optHelp = false;
  bool optHelpConfig = false;
  bool optVersion = false;
  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0 ||
        strcmp(argv[i], "--config") == 0) {
      if (i + 1 == argc) {
        std::cerr << "Error: --config option requires one argument." << std::endl;
        do_exit(1);
      }
      if (seenOptConfig) {
        std::cerr << "Error: --config option can only be used once." << std::endl;
        do_exit(1);
      }
      configFile = argv[++i];
      seenOptConfig = true;
      continue;
    }
    if (strcmp(argv[i], "--print-config") == 0) {
      optPrintConfig = true;
      continue;
    }
    if (strcmp(argv[i], "-?") == 0 ||
        strcmp(argv[i], "--help") == 0) {
      optHelp = true;
      continue;
    }
    if (strcmp(argv[i], "--help-config") == 0) {
      optHelpConfig = true;
      continue;
    }
    if (strcmp(argv[i], "-v") == 0 ||
        strcmp(argv[i], "--version") == 0) {
      optVersion = true;
      continue;
    }
    std::cerr << "Error: Unknown option " << argv[i] << std::endl;
    do_exit(1);
  }

  if (optVersion || optHelp) {
    printf("rdrs2 distributed as part of ");
    ndb_std_print_version();
    printf("rdrs API supported up to version " API_VERSION "\n");
  }
  if (optHelp) {
    printf("\n%s", usageHelp);
  }
  if (optHelpConfig) {
    if (optVersion || optHelp) {
      printf("\n\n");
    }
    printf("%s", configHelp);
  }
  if ((optVersion || optHelp || optHelpConfig) && !optPrintConfig) {
    do_exit(0);
  }

  RS_Status status = AllConfigs::init(configFile);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    std::cerr << "Error while initializing configuration.\n"
              << "HTTP code " << status.http_code << '\n'
              << status.message << '\n';
    do_exit(1);
  }

  if (optPrintConfig) {
    printJson(globalConfigs, std::cout, 0);
    std::cout << '\n';
    do_exit(0);
  }

  if (!globalConfigs.pidfile.empty()) {
    pidfile = globalConfigs.pidfile.c_str();
  }
  if (pidfile != nullptr) {
    FILE *pidFILE = fopen(pidfile, "w");
    if (pidFILE == nullptr) {
      printf("Failed to open pidfile %s\n", pidfile);
      exit(errno);
    }
    int pid = getpid();
    fprintf(pidFILE, "%d\n", pid);
    fclose(pidFILE);
    printf("Wrote PID=%d to %s\n", pid, pidfile);
  }

  if (globalConfigs.grpc.enable) {
    std::cerr << "Error: gRPC not supported\n";
    do_exit(1);
  }
  if (!globalConfigs.rest.enable) {
    printf("Don't know what to do when REST is disabled.\n");
    do_exit(1);
  }

  // connect to rondb
  RonDBConnection rondbConnection(globalConfigs.ronDB,
                                  globalConfigs.ronDbMetaDataCluster);
  if (globalConfigs.security.tls.enableTLS) {
    status = GenerateTLSConfig(globalConfigs.security.tls.requireAndVerifyClientCert,
                               globalConfigs.security.tls.rootCACertFile,
                               globalConfigs.security.tls.certificateFile,
                               globalConfigs.security.tls.privateKeyFile);
    if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
      std::cerr << "Error while generating TLS configuration.\n"
                << "HTTP code " << status.http_code << '\n'
                << status.message << '\n';
      do_exit(1);
    }
  }

  drogon::app().addListener(globalConfigs.rest.serverIP, globalConfigs.rest.serverPort, globalConfigs.security.tls.enableTLS, globalConfigs.security.tls.certificateFile, globalConfigs.security.tls.privateKeyFile);
  drogon::app().setThreadNum(globalConfigs.rest.numThreads);
  drogon::app().disableSession();
  drogon::app().registerBeginningAdvice([]() {
    auto addresses = drogon::app().getListeners();
    for (auto &address : addresses)
    {
      // todo-asdf print thread id
      printf("Server running on %s\n",
             address.toIpPort().c_str());
    }
  });
  drogon::app().setIntSignalHandler([]() {
    printf("Received SIGINT, will quit.\n");
    // Calling quit() is exactly what the default handler does.
    drogon::app().quit();
  });
  drogon::app().setTermSignalHandler([]() {
    printf("Received SIGTERM, will quit.\n");
    // Calling quit() is exactly what the default handler does.
    drogon::app().quit();
  });
  drogon::app().run();

  do_exit(0);
}
