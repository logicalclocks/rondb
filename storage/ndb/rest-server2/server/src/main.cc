/*
 * Copyright (c) 2023, 2025, Hopsworks and/or its affiliates.
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
  "Usage: rdrs2 [ --config PATH ] [ --help ]\n"
  "\n"
  "-c, --config PATH   Use a JSON-format config file. Keys beginning with a hash\n"
  "                    (#) are allowed and ignored, and can be used as comments.\n"
  "\n"
  "--print-config      Print the effective configuration that would be used,\n"
  "                    together with explanations.\n"
  "\n"
  "-v, --version       Print version information.\n"
  "\n"
  "-h, --help          Show this usage help.\n"
  ;

#include "connection.hpp"
#include "config_structs.hpp"
#include "rdrs_dal.h"
#include "json_parser.hpp"
#include "json_printer.hpp"
#include "metrics.hpp"
#include "src/api_key.hpp"
#include "src/fs_cache.hpp"
#include "tls_util.hpp"
#include "src/ttl_purge.hpp"
#include <ndb_opts.h>
#include <NdbMutex.h>

#include "rondis_thread.h"
#include "rondb.h"
#include "rdrs_rondb_connection_pool.hpp"

#include <cstdio>
#include <iostream>
#include <sys/errno.h>
#include <unistd.h>
#include <csignal>

extern "C" {
  #include <avro.h>
}

using namespace pink;

// Cleanup logic
static bool g_did_ndb_init = false;
static bool g_did_start_api_key_cache = false;
static bool g_did_start_fs_cache = false;
static const char* g_pidfile = nullptr;
static RonDBConnection* g_rondbConnection = nullptr;
static bool g_drogon_running = false;
static ConnFactory* g_rondis_conn_factory = nullptr;
static RondisHandle* g_rondis_handle = nullptr;
static ServerThread* g_rondis_thread = nullptr;
static int *g_database_index = nullptr;
static bool g_rondis_running = false;
static int g_exit_code = 0;
static TTLPurger* g_ttl_purger = nullptr;
NdbMutex *globalConfigsMutex = nullptr;

static void do_exit() {
  // todo Set shutdown flag in RDRS2 connection pool
  if (g_drogon_running) {
    printf("Quitting Drogon...\n");
    drogon::app().quit();
    return;
  }
  if (jsonParsers != nullptr) {
    delete[] jsonParsers;
    jsonParsers = nullptr;
  }
  if (g_rondis_running) {
    g_rondis_thread->StopThread();
    g_rondis_running = false;
  }
  if (g_rondis_thread) {
    delete g_rondis_thread;
    g_rondis_thread = nullptr;
  }
  if (g_rondis_handle) {
    delete g_rondis_handle;
    g_rondis_handle = nullptr;
  }
  if (g_rondis_conn_factory) {
    delete g_rondis_conn_factory;
    g_rondis_conn_factory = nullptr;
  }
  if (g_ttl_purger != nullptr) {
    delete g_ttl_purger;
    g_ttl_purger = nullptr;
  }
  if (g_did_start_api_key_cache)
    stop_api_key_cache();
  if (g_did_start_fs_cache)
    stop_fs_cache();
  if (g_rondbConnection != nullptr) {
    delete g_rondbConnection;
    g_rondbConnection = nullptr;
  }
  NdbMutex_Destroy(globalConfigsMutex);
  if (g_did_ndb_init)
    ndb_end(0);
  if (g_pidfile != nullptr) {
    printf("Removing pidfile %s\n", g_pidfile);
    if(remove(g_pidfile) != 0) {
      printf("Failed to remove pidfile %s: %s\n", g_pidfile, strerror(errno));
    }
  }
  if (g_exit_code != 0) {
    printf("rdrs2: Exit with code %d.\n", g_exit_code);
  }
  free(g_database_index);
  exit(g_exit_code);
}

static void exit_on_rondis_error() {
  g_exit_code = 1;
  do_exit();
}

static void handle_signal(int signal) {
  switch (signal) {
    case SIGHUP:
      printf("Received and ignored SIGHUP.\n");
      return;
    case SIGPIPE:
      printf("Received and ignored SIGPIPE.\n");
      return;
    case SIGINT:
      printf("Received SIGINT.\n");
      break;
    case SIGQUIT:
      printf("Received SIGQUIT.\n");
      break;
    case SIGTERM:
      printf("Received SIGTERM.\n");
      break;
    default:
      printf("Received unexpected signal %d\n", signal);
      break;
  }
  g_exit_code = 128 + signal; // Because it's tradition.
  if (signal == SIGTERM) {
    g_exit_code = 0; // SIGTERM is used for clean exit
  }
  do_exit();
}

int main(int argc, char *argv[]) {
  signal(SIGHUP, handle_signal);
  signal(SIGPIPE, handle_signal);
  signal(SIGINT, handle_signal);
  signal(SIGQUIT, handle_signal);
  signal(SIGTERM, handle_signal);

  ndb_init();
  g_did_ndb_init = true;
  globalConfigsMutex = NdbMutex_Create();
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
  bool optVersion = false;
  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0 ||
        strcmp(argv[i], "--config") == 0) {
      if (i + 1 == argc) {
        std::cerr << "Error: --config option requires one argument." << std::endl;
        g_exit_code = 1;
        do_exit();
      }
      if (seenOptConfig) {
        std::cerr << "Error: --config option can only be used once." << std::endl;
        g_exit_code = 1;
        do_exit();
      }
      configFile = argv[++i];
      seenOptConfig = true;
      continue;
    }
    if (strcmp(argv[i], "--print-config") == 0) {
      optPrintConfig = true;
      continue;
    }
    if (strcmp(argv[i], "-h") == 0 ||
        strcmp(argv[i], "--help") == 0) {
      optHelp = true;
      continue;
    }
    if (strcmp(argv[i], "-v") == 0 ||
        strcmp(argv[i], "--version") == 0) {
      optVersion = true;
      continue;
    }
    std::cerr << "Error: Unknown option " << argv[i] << std::endl;
    g_exit_code = 1;
    do_exit();
  }

  if (optVersion || optHelp) {
    printf("rdrs2 distributed as part of ");
    ndb_std_print_version();
    printf("rdrs API supported up to version " API_VERSION "\n");
  }
  if (optHelp) {
    printf("\n%s", usageHelp);
  }
  if ((optVersion || optHelp) && !optPrintConfig) {
    do_exit();
  }

  RS_Status status = AllConfigs::init(configFile);
  if (status.http_code !=
        static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    std::cerr << "Error while initializing configuration.\n"
              << "HTTP code " << status.http_code << '\n'
              << status.message << '\n';
    g_exit_code = 1;
    do_exit();
  }

  if (optPrintConfig) {
    printJson(globalConfigs, std::cout, 0, true);
    std::cout << '\n';
    do_exit();
  }

  if (!globalConfigs.pidfile.empty()) {
    g_pidfile = globalConfigs.pidfile.c_str();
  }
  if(g_pidfile != nullptr) {
    FILE *pidFILE = fopen(g_pidfile, "w");
    if (pidFILE == nullptr) {
      printf("Failed to open pidfile %s: %s\n", g_pidfile, strerror(errno));
      g_exit_code = 1;
      do_exit();
    }
    int pid = getpid();
    fprintf(pidFILE, "%d\n", pid);
    fclose(pidFILE);
    printf("Wrote PID=%d to %s\n", pid, g_pidfile);
  }

  // Initialize Prometheus Metrics
  rdrs_metrics::initMetrics();

  // Initialize JSON parsers
  assert(jsonParsers == nullptr);
  jsonParsers = new JSONParser[globalConfigs.rest.numThreads];
  if (jsonParsers == nullptr) {
    std::cerr << "Failed to allocate memory for JSON parsers.\n";
    g_exit_code = 1;
    do_exit();
  }

  Uint32 num_rondis_threads = 0;
  if (globalConfigs.rondis.enable) {
    num_rondis_threads = globalConfigs.rondis.numThreads;
  }
  Uint32 num_rdrs_threads = globalConfigs.rest.numThreads;
  Uint32 num_purge_threads = RDRSRonDBConnectionPool::kNoTTLPurgeThreads;
  Uint32 tot_num_threads =
    num_rdrs_threads + num_rondis_threads + num_purge_threads;
  /**
   * The RDRS server, the Rondis server and the TTL purge threads
   * all share the same cluster connections. The RDRS server can also
   * use the metadata connection to connect to another cluster.
   *
   * The threads maintained in g_rondbConnection are using thread
   * ranges to map threads to Ndb objects. The first set of Ndb
   * objects are used by the RDRS server, the next set of Ndb objects
   * are used by the Rondis server and the last Ndb objects are used
   * by the TTL purge object.
   */
  // connect to rondb for all services
  g_rondbConnection = new RonDBConnection(globalConfigs.ronDB,
                                          globalConfigs.ronDBMetadataCluster,
                                          tot_num_threads);
  if (g_rondbConnection == nullptr) {
    std::cerr << "Failed to allocate memory for RonDB connection.\n";
    g_exit_code = 1;
    do_exit();
  }

  // Start TTL purger
  g_ttl_purger = TTLPurger::CreateTTLPurger();
  g_ttl_purger->Run();

  // Start rondis
  if (globalConfigs.rondis.enable) {
    g_rondis_conn_factory = new RondisConnFactory();
    g_rondis_handle = new RondisHandle();
    g_database_index = (int*)malloc(sizeof(int) * num_rondis_threads);
    memset(g_database_index, 0, sizeof(int) * num_rondis_threads);
    bool dirty_incr_decr_flag[MAX_NUM_DATABASES];
    memset(&dirty_incr_decr_flag[0], 0, sizeof(dirty_incr_decr_flag));
    for (Uint32 i = 0; i < globalConfigs.rondis.numDatabases; i++) {
      dirty_incr_decr_flag[i] = globalConfigs.rondis.databases[i].dirtyIncrDecr;
    }
    g_rondis_thread = NewDispatchThread(globalConfigs.rondis.serverIP,
                                        globalConfigs.rondis.serverPort,
                                        globalConfigs.rondis.numThreads,
                                        g_rondis_conn_factory,
                                        1000,
                                        1000,
                                        g_rondis_handle);
    int ret_code = setup_ndb_connection_for_rondis(
      get_rdrs_ndb_object,
      return_rdrs_ndb_object,
      exit_on_rondis_error,
      num_rdrs_threads,
      num_rondis_threads,
      g_database_index,
      globalConfigs.rondis.numDatabases,
      &dirty_incr_decr_flag[0]);
    if (ret_code != 0) {
      printf("Error setting up Rondis Server\n");
      g_exit_code = 1;
      do_exit();
    }
    if (g_rondis_thread->StartThread() != 0)
    {
        printf("Error starting rondis thread\n");
        g_exit_code = 1;
        do_exit();
    }
    g_rondis_running = true;
    printf("Rondis Server running on %s:%u\n",
      globalConfigs.rondis.serverIP.c_str(),
      globalConfigs.rondis.serverPort);
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
      g_exit_code = 1;
      do_exit();
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
      printf("RDRS Server running on %s\n", address.toIpPort().c_str());
    }
  });
  drogon::app().setIntSignalHandler([]() {
    handle_signal(SIGINT);
  });
  drogon::app().setTermSignalHandler([]() {
    handle_signal(SIGTERM);
  });
  g_drogon_running = true;
  drogon::app().run();
  g_drogon_running = false;
  do_exit();
}
