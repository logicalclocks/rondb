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
  ".Internal.ReqBufferSize\n"
  "\n"
  ".Internal.RespBufferSize\n"
  "\n"
  ".Internal.PreAllocatedBuffers\n"
  "\n"
  ".Internal.BatchMaxSize\n"
  "\n"
  ".Internal.OperationIDMaxSize\n"
  "\n"
  ".PIDFile\n"
  "        Path to .pid file. The process ID will be written on startup, and the\n"
  "        file will be deleted on exit.\n"
  "\n"
  ".REST.Enable\n"
  "        Whether to enable the REST server.\n"
  "\n"
  ".REST.ServerIP\n"
  "        The IP address to listen on.\n"
  "\n"
  ".REST.ServerPort\n"
  "        TCP port to listen on.\n"
  "\n"
  ".REST.NumThreads\n"
  "\n"
  ".GRPC.Enable\n"
  "        Whether to enable the gRPC server.\n"
  "\n"
  ".GRPC.ServerIP\n"
  "        The IP address to listen on.\n"
  "\n"
  ".GRPC.ServerPort\n"
  "        TCP port to listen on.\n"
  "\n"
  ".RonDB\n"
  "        An object describing the connection to a RonDB cluster used to store\n"
  "        data.\n"
  "\n"
  ".RonDB.Mgmds\n"
  "        An array of ndb_mgmd servers in use by the cluster.\n"
  "\n"
  ".RonDB.Mgmds[].IP\n"
  "        The IP address for an ndb_mgmd server.\n"
  "\n"
  ".RonDB.Mgmds[].Port\n"
  "        The TCP port for an ndb_mgmd server.\n"
  "\n"
  ".RonDB.ConnectionPoolSize\n"
  "\n"
  ".RonDB.NodeIDs\n"
  "        An array of ConnectionPoolSize length.\n"
  "\n"
  ".RonDB.NodeIDs[]\n"
  "        Force a RonDB connection to be assigned to a specific node ID.\n"
  "\n"
  ".RonDB.ConnectionRetries\n"
  "        Connection retry attempts.\n"
  "\n"
  ".RonDB.ConnectionRetryDelayInSec\n"
  "\n"
  ".RonDB.OpRetryOnTransientErrorsCount\n"
  "        Transient error retry count.\n"
  "\n"
  ".RonDB.OpRetryInitialDelayInMS\n"
  "        Transient error initial delay.\n"
  "\n"
  ".RonDB.OpRetryJitterInMS\n"
  "\n"
  ".RonDBMetadataCluster\n"
  "        An object describing the connection to a RonDB cluster used to store\n"
  "        metadata. It has the same schema as .RonDB. If it is not present in the\n"
  "        config file, then it will be set to .RonDB\n"
  "\n"
  ".Security.TLS.EnableTLS\n"
  "\n"
  ".Security.TLS.RequireAndVerifyClientCert\n"
  "\n"
  ".Security.TLS.CertificateFile\n"
  "\n"
  ".Security.TLS.PrivateKeyFile\n"
  "\n"
  ".Security.TLS.RootCACertFile\n"
  "\n"
  ".Security.TLS.TestParameters.ClientCertFile\n"
  "\n"
  ".Security.TLS.TestParameters.ClientKeyFile\n"
  "\n"
  ".Security.APIKey.UseHopsworksAPIKeys\n"
  "\n"
  ".Security.APIKey.CacheRefreshIntervalMS\n"
  "\n"
  ".Security.APIKey.CacheUnusedEntriesEvictionMS\n"
  "\n"
  ".Security.APIKey.CacheRefreshIntervalJitterMS\n"
  "\n"
  ".Log.Level\n"
  "\n"
  ".Log.FilePath\n"
  "\n"
  ".Log.MaxSizeMB\n"
  "\n"
  ".Log.MaxBackups\n"
  "\n"
  ".Log.MaxAge\n"
  "\n"
  ".Testing.MySQL\n"
  "        An object describing the connection to a MySQL cluster used to store\n"
  "        data.\n"
  "\n"
  ".Testing.MySQL.Servers\n"
  "        An array of objects, each describing one MySQL server in the cluster.\n"
  "\n"
  ".Testing.MySQL.Servers[].IP\n"
  "        The IP or hostname for the MySQL server.\n"
  "\n"
  ".Testing.MySQL.Servers[].Port\n"
  "        The TCP port for the MySQL server.\n"
  "\n"
  ".Testing.MySQL.User\n"
  "        The username to use for connecting to the MySQL servers.\n"
  "\n"
  ".Testing.MySQL.Password\n"
  "        The password to use for connecting to the MySQL servers.\n"
  "\n"
  ".Testing.MySQLMetadataCluster\n"
  "        An object describing the connection to a MySQL cluster used to store\n"
  "        metadata. It has the same schema as .Testing.MySQL. If it is not present\n"
  "        in the config file, then it will be set to .Testing.MySQL.\n"
  ;

#include "connection.hpp"
#include "config_structs.hpp"
#include "rdrs_dal.h"
#include "json_parser.hpp"
#include "json_printer.hpp"
#include "pk_read_ctrl.hpp"
#include "src/api_key.hpp"
#include "src/fs_cache.hpp"
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

// Cleanup logic
static bool g_did_ndb_init = false;
static bool g_did_start_api_key_cache = false;
static bool g_did_start_fs_cache = false;
static const char* g_pidfile = nullptr;
static RonDBConnection* g_rondbConnection = nullptr;
static bool g_drogon_running = false;
static int g_deferred_exit_code = 0;
static void do_exit(int exit_code) {
  assert(!g_drogon_running);
  if (g_rondbConnection != nullptr) {
    delete g_rondbConnection;
    g_rondbConnection = nullptr;
  }
  if (jsonParsers != nullptr) {
    delete[] jsonParsers;
    jsonParsers = nullptr;
  }
  if (g_pidfile != nullptr) {
    printf("Removing pidfile %s\n", g_pidfile);
    if(remove(g_pidfile) != 0) {
      printf("Failed to remove pidfile %s: %s\n", g_pidfile, strerror(errno));
    }
  }
  if (g_did_start_api_key_cache)
    stop_api_key_cache();
  if (g_did_start_fs_cache)
    stop_fs_cache();
  if (g_did_ndb_init)
    ndb_end(0);
  if (exit_code != 0) {
    printf("rdrs2: Exit with code %d.\n", exit_code);
  }
  exit(exit_code);
}
static void before_drogon_run() {
  g_drogon_running = true;
}
static void after_drogon_run() {
  g_drogon_running = false;
  if (g_deferred_exit_code != 0) {
    do_exit(g_deferred_exit_code);
  }
}
static void handle_signal(int signal) {
  switch (signal) {
    case SIGINT:
      printf("Received SIGINT.\n");
      break;
    case SIGTERM:
      printf("Received SIGTERM.\n");
      break;
    default:
      printf("Received unexpected signal %d\n", signal);
      break;
  }
  int exit_code = 128 + signal; // Because it's tradition.
  if (signal == SIGTERM) {
    exit_code = 0; // SIGTERM is used for clean exit
  }
  if (g_drogon_running) {
    printf("Quitting Drogon...\n");
    drogon::app().quit();
    if (exit_code != 0) {
      g_deferred_exit_code = exit_code;
    }
  }
  else {
    do_exit(exit_code);
  }
}



int main(int argc, char *argv[]) {
  signal(SIGTERM, handle_signal);
  signal(SIGINT, handle_signal);

  ndb_init();
  g_did_ndb_init = true;
  (void)start_api_key_cache();
  g_did_start_api_key_cache = true;

  start_fs_cache();
  g_did_start_fs_cache = true;

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
    g_pidfile = globalConfigs.pidfile.c_str();
  }
  if(g_pidfile != nullptr) {
    FILE *pidFILE = fopen(g_pidfile, "w");
    if (pidFILE == nullptr) {
      printf("Failed to open pidfile %s: %s\n", g_pidfile, strerror(errno));
      do_exit(1);
    }
    int pid = getpid();
    fprintf(pidFILE, "%d\n", pid);
    fclose(pidFILE);
    printf("Wrote PID=%d to %s\n", pid, g_pidfile);
  }

  // Initialize JSON parsers
  assert(jsonParsers == nullptr);
  jsonParsers = new JSONParser[globalConfigs.rest.numThreads];
  if (jsonParsers == nullptr) {
    std::cerr << "Failed to allocate memory for JSON parsers.\n";
    do_exit(1);
  }

  // connect to rondb
  g_rondbConnection = new RonDBConnection(globalConfigs.ronDB,
                                          globalConfigs.ronDBMetadataCluster);
  if (g_rondbConnection == nullptr) {
    std::cerr << "Failed to allocate memory for RonDB connection.\n";
    do_exit(1);
  }

  if (globalConfigs.security.tls.enableTLS) {
    status = GenerateTLSConfig(
      globalConfigs.security.tls.requireAndVerifyClientCert,
      globalConfigs.security.tls.rootCACertFile,
      globalConfigs.security.tls.certificateFile,
      globalConfigs.security.tls.privateKeyFile);
    if (status.http_code !=
        static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
      std::cerr << "Error while generating TLS configuration.\n"
                << "HTTP code " << status.http_code << '\n'
                << status.message << '\n';
      do_exit(1);
    }
  }

  drogon::app().addListener(globalConfigs.rest.serverIP,
                            globalConfigs.rest.serverPort,
                            globalConfigs.security.tls.enableTLS,
                            globalConfigs.security.tls.certificateFile,
                            globalConfigs.security.tls.privateKeyFile);
  drogon::app().setThreadNum(globalConfigs.rest.numThreads);
  drogon::app().disableSession();
  drogon::app().registerBeginningAdvice([]() {
    auto addresses = drogon::app().getListeners();
    for (auto &address : addresses) {
      printf("Server running on %s\n", address.toIpPort().c_str());
    }
  });
  drogon::app().setIntSignalHandler([]() {
    handle_signal(SIGINT);
  });
  drogon::app().setTermSignalHandler([]() {
    handle_signal(SIGTERM);
  });
  before_drogon_run();
  drogon::app().run();
  after_drogon_run();
  do_exit(0);
}
