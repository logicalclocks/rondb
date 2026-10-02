/*
 * Copyright (c) 2023, 2026, Hopsworks and/or its affiliates.
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
#include "capped_ostream.hpp"
#include "error_strings.h"
#include "rdrs_dal.h"
#include "json_parser.hpp"
#include "json_printer.hpp"
#include "metrics.hpp"
#include "scan_metrics.hpp"
#include "src/api_key.hpp"
#include "src/fs_cache.hpp"
#include "storage/ndb/src/ronsql/RdrsSchemaCache.hpp"
#include "tls_util.hpp"
#include "src/ttl_purge.hpp"
#include <ndb_opts.h>
#include <NdbMutex.h>

#include "rondis_thread.h"
#include "mysql_conn.h"
#include "mysql_protocol.h"
#include "rondb.h"
#include "rdrs_rondb_connection_pool.hpp"
#include "metrics.hpp"
#include "rdrs_dal.hpp"
#include "ronsql_worker_pool.hpp"
#include "storage/ndb/src/ronsql/RonSQLCommon.hpp"
#include "probe_server.hpp"
#include "logger.hpp"

#include <cstdio>
#include <cstring>
#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>
#include <sys/errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
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
static Uint32 *g_database_index = nullptr;
static bool g_rondis_running = false;
static ConnFactory* g_mysql_router_conn_factory = nullptr;
static MysqlHandle* g_mysql_router_handle = nullptr;
static ServerThread* g_mysql_router_thread = nullptr;
static bool g_mysql_router_running = false;
static int g_exit_code = 0;
/* Stack size of the REST (drogon IO loop) threads and of the RonSQL
 * workers that execute their statements. */
static constexpr Uint32 kRestThreadStackSize = 8 * 1024 * 1024;
static ProbeServer* g_probe_server = nullptr;
TTLPurger* g_ttl_purger = nullptr;
NdbMutex *globalConfigsMutex = nullptr;
static volatile sig_atomic_t g_in_exit = 0;

static void do_exit() {
  // todo Set shutdown flag in RDRS2 connection pool
  if (g_drogon_running) {
    if (g_ronsql_worker_pool != nullptr) {
      /* While the IO loops still run: queued RonSQL requests get a 503 and
       * the running ones finish, so every response is sent. */
      printf("Stopping RonSQL workers...\n");
      g_ronsql_worker_pool->stop();
    }
    printf("Quitting Drogon...\n");
    drogon::app().quit();
    return;
  }
  if (g_in_exit) {
    // Prevent re-entrant calls from signal handlers during cleanup.
    // TTLPurger::~TTLPurger blocks on NdbThread_WaitFor; if a signal
    // arrives while blocked, the handler re-enters do_exit and would
    // double-destroy objects already being torn down.
    _exit(g_exit_code);
  }
  g_in_exit = 1;
  /* First: join the probe listener thread before anything it reads is torn
   * down (it reaches the connection pool through get_rondb_stats). do_exit
   * is the single funnel for every startup-failure path too, so this also
   * covers a probe server left running when a later init step fails. */
  if (g_probe_server != nullptr) {
    g_probe_server->Stop();
    delete g_probe_server;
    g_probe_server = nullptr;
  }
  if (g_ronsql_worker_pool != nullptr) {
    // Before the schema cache and the RonDB connections go away.
    delete g_ronsql_worker_pool;
    g_ronsql_worker_pool = nullptr;
  }
  if (jsonParsers != nullptr) {
    delete[] jsonParsers;
    jsonParsers = nullptr;
  }
  if (g_mysql_router_running) {
    g_mysql_router_thread->StopThread();
    g_mysql_router_running = false;
  }
  if (g_mysql_router_thread) {
    delete g_mysql_router_thread;
    g_mysql_router_thread = nullptr;
  }
  if (g_mysql_router_handle) {
    delete g_mysql_router_handle;
    g_mysql_router_handle = nullptr;
  }
  if (g_mysql_router_conn_factory) {
    delete g_mysql_router_conn_factory;
    g_mysql_router_conn_factory = nullptr;
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
  stop_schema_cache();
  cleanupScanMetrics();
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

static void* rondis_start_cmd() {
  RondisEndPointMetricsUpdater *metricsUpdater =
    new RondisEndPointMetricsUpdater();
  return (void*)metricsUpdater;
}

static void rondis_end_cmd(void *metrics_ptr) {
  RondisEndPointMetricsUpdater *metricsUpdater =
    (RondisEndPointMetricsUpdater*)metrics_ptr;
  delete metricsUpdater;
}

// RonSQL handler for MySQL router — called from MysqlConn::try_ronsql()
static bool mysql_router_ronsql_handler(
    const char* query, size_t query_len,
    const char* database, int thread_index,
    std::string& result_out,
    std::string& error_out) {

  ArenaMalloc amalloc(RonSQLExecParams::ARENA_MALLOC_PAGE_SIZE);
  RonSQLExecParams params;

  params.sql_len = query_len;
  params.sql_buffer = amalloc.alloc<char>(query_len + 2);
  if (params.sql_buffer == nullptr) {
    error_out = "Out of memory";
    return false;
  }
  memcpy(params.sql_buffer, query, query_len);
  params.sql_buffer[params.sql_len++] = '\0';
  params.sql_buffer[params.sql_len++] = '\0';

  params.amalloc = &amalloc;
  params.explain_mode = RonSQLExecParams::ExplainMode::ALLOW;
  params.output_format = RonSQLExecParams::OutputFormat::TEXT;

  // Internal.MaxRespSize bounds the accumulated result here just like
  // on the /ronsql REST endpoint (see ronsql_ctrl.cpp): past the cap,
  // writes are silently dropped with bounded memory and the exceeded()
  // check below converts that into a clean error.
  CappedOStream out_stream(globalConfigs.internal.maxRespSize);
  std::ostringstream err_stream;
  params.out_stream = &out_stream;
  params.err_stream = &err_stream;

  bool do_explain = false;
  params.do_explain = &do_explain;

  // Use the RDRS thread index offset for MySQL router threads.
  // The MySQL router threads come after REST + Rondis + purge threads.
  RS_Status status = ronsql_dal(database, &params,
                                (unsigned int)thread_index);

  if (status.http_code != SUCCESS) {
    error_out = err_stream.str();
    if (error_out.empty()) {
      error_out = status.message;
    }
    return false;
  }

  if (out_stream.exceeded()) {
    std::ostringstream msg;
    msg << rdrsErrorMessage(ERROR_RESPONSE_TOO_LARGE)
        << " (Internal.MaxRespSize = "
        << globalConfigs.internal.maxRespSize
        << " bytes). Narrow the query or add LIMIT, or raise"
           " Internal.MaxRespSize in the RDRS configuration.";
    error_out = msg.str();
    return false;
  }

  result_out = out_stream.take();
  return true;
}

/*
  Graceful shutdown. Only safe to call from a normal execution context, i.e.
  from the Drogon event loop once Drogon is running, or from main()'s wait
  loop when REST is disabled - never from a POSIX signal handler.
*/
static void handle_signal(int signal) {
  switch (signal) {
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

// write(2) is async-signal-safe, printf() is not.
static void write_stdout(const char *msg) {
  ssize_t ignored = write(STDOUT_FILENO, msg, strlen(msg));
  (void)ignored;
}

/*
  Real POSIX signal handler, installed for the window before Drogon runs the
  event loop. It can interrupt the main thread anywhere, in particular inside
  Ndb_cluster_connection::connect()/wait_until_ready(), which blocks for up to
  a minute while the cluster connections are set up and holds NDB API and
  allocator locks while doing so.

  do_exit() is not async-signal-safe: it joins background threads, destroys
  mutexes and calls ndb_end(), which tears down NDB API globals underneath the
  very thread this handler interrupted. Running it from here deadlocks the
  process instead of stopping it, so restrict this handler to async-signal-safe
  work: report, drop the pidfile and leave. Anything the process still holds is
  released by the kernel.

  Once Drogon is running it installs its own handlers and invokes
  handle_signal() from the event loop, where the full teardown is safe. With
  REST disabled Drogon never runs and this handler stays installed, so once
  startup is done (g_startup_done) it only records the signal in
  g_pending_signal; main()'s wait loop then runs the same graceful
  handle_signal(). A second signal while that teardown runs still takes the
  immediate exit below, so a teardown that hangs can always be cut short.
*/
static volatile sig_atomic_t g_startup_done = 0;
static volatile sig_atomic_t g_pending_signal = 0;

static void handle_signal_async(int signal) {
  switch (signal) {
    case SIGHUP:
      write_stdout("Received and ignored SIGHUP.\n");
      return;
    case SIGPIPE:
      write_stdout("Received and ignored SIGPIPE.\n");
      return;
    default:
      break;
  }
  if (g_startup_done && g_pending_signal == 0) {
    g_pending_signal = signal;
    return;
  }
  switch (signal) {
    case SIGINT:
      write_stdout("Received SIGINT");
      break;
    case SIGQUIT:
      write_stdout("Received SIGQUIT");
      break;
    case SIGTERM:
      write_stdout("Received SIGTERM");
      break;
    default:
      write_stdout("Received unexpected signal");
      break;
  }
  write_stdout(g_startup_done ? " during shutdown, exiting immediately.\n"
                              : " during startup, exiting.\n");
  if (g_pidfile != nullptr) {
    unlink(g_pidfile);
  }
  // SIGTERM is used for clean exit, other signals keep the traditional code.
  _exit(signal == SIGTERM ? 0 : 128 + signal);
}

int main(int argc, char *argv[]) {
  signal(SIGHUP, handle_signal_async);
  signal(SIGPIPE, handle_signal_async);
  signal(SIGINT, handle_signal_async);
  signal(SIGQUIT, handle_signal_async);
  signal(SIGTERM, handle_signal_async);

  ndb_init();
  g_did_ndb_init = true;
  globalConfigsMutex = NdbMutex_Create();
  // API key cache and FS cache initialization is deferred until after
  // config is parsed, so we can skip them when REST is disabled.
  APIKeyCache *apiKeyCachePtr = nullptr;

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

  // Initialize REST-dependent subsystems only when REST is enabled
  if (globalConfigs.rest.enable) {
    apiKeyCachePtr = start_api_key_cache();
    g_did_start_api_key_cache = true;

    start_fs_cache();
    g_did_start_fs_cache = true;

    start_schema_cache(globalConfigs.internal.schemaCacheTTLSecs);

    // Initialize Prometheus Metrics
    rdrs_metrics::initMetrics();

    // Initialize Scan Metrics buffer
    initScanMetrics();

    /* Start the probe listener before the RonDB connect below, which can
     * block for a minute: binding early makes a port conflict fail fast and
     * keeps "starting" (503) distinguishable from "dead" (connection refused)
     * for the whole startup window. Both endpoints answer 503 until the
     * server can actually serve: /ping until Drogon is up (g_drogon_up), so
     * the startup probe can use this port with main-port semantics; /health
     * additionally until the pool exists and reports ready data nodes. */
    if (globalConfigs.rest.probeEnable) {
      /* Config validation has already rejected ProbeEnable combined with
       * Ping/HealthRequiresAuth, so the probe port serving without
       * authentication can never contradict the configured auth policy. */
      g_probe_server = new ProbeServer();
      if (!g_probe_server->Start()) {
        g_exit_code = 1;
        do_exit();
      }
    }

    // Initialize JSON parsers
    assert(jsonParsers == nullptr);
    jsonParsers = new JSONParser[globalConfigs.rest.numThreads];
    if (jsonParsers == nullptr) {
      std::cerr << "Failed to allocate memory for JSON parsers.\n";
      g_exit_code = 1;
      do_exit();
    }
  }

  Uint32 num_rondis_threads = 0;
  if (globalConfigs.rondis.enable) {
    num_rondis_threads = globalConfigs.rondis.numThreads;
  }
  Uint32 num_mysql_router_threads = 0;
  if (globalConfigs.mysqlRouter.enable) {
    num_mysql_router_threads = globalConfigs.mysqlRouter.numThreads;
  }
  Uint32 num_rdrs_threads = globalConfigs.rest.enable ?
      globalConfigs.rest.numThreads : 0;
  Uint32 num_purge_threads = globalConfigs.rest.enable ?
      RDRSRonDBConnectionPool::kNoTTLPurgeThreads : 0;
  Uint32 num_ronsql_workers = globalConfigs.rest.enable ?
      globalConfigs.ronsql.numThreads : 0;
  Uint32 ronsql_worker_thread_offset =
    num_rdrs_threads + num_rondis_threads + num_mysql_router_threads;
  Uint32 tot_num_threads =
    num_rdrs_threads + num_rondis_threads +
    num_mysql_router_threads + num_ronsql_workers + num_purge_threads;
  /**
   * The RDRS server, the Rondis server, the MySQL router, the RonSQL
   * workers and the TTL purge threads all share the same cluster
   * connections. The RDRS server can also use the metadata connection
   * to connect to another cluster.
   *
   * The threads maintained in g_rondbConnection are using thread
   * ranges to map threads to Ndb objects. The first set of Ndb
   * objects are used by the RDRS server, the next set of Ndb objects
   * are used by the Rondis server, then the MySQL router threads,
   * then the RonSQL workers, and the last Ndb objects are used by the
   * TTL purge object.
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

  // Start TTL purger, API key cache, and FS cache only when REST is enabled
  if (globalConfigs.rest.enable) {
    g_ttl_purger = TTLPurger::CreateTTLPurger();
    if (g_ttl_purger != nullptr) {
      // Seed the runtime purge config from the config file; the REST config
      // API can change it later at runtime.
      TTLPurgeConfig ttl_purge_config = g_ttl_purger->GetConfig();
      ttl_purge_config.enabled = globalConfigs.ttlPurge.enable;
      TTLPurge::parseActiveWindow(globalConfigs.ttlPurge.activeWindow,
                                  &ttl_purge_config.active_window_start_min,
                                  &ttl_purge_config.active_window_end_min);
      g_ttl_purger->SetConfig(ttl_purge_config);
      g_ttl_purger->Run();
    } else {
      // The TTL purge endpoints will report 503; everything else still works
      std::cerr << "Failed to initialize the TTL purger; TTL purging is "
                   "not running on this node.\n";
    }

    if (globalConfigs.security.apiKey.useHopsworksAPIKeys) {
      apiKeyCachePtr->preload_all_keys();
      apiKeyCachePtr->start_background_threads();
    }

    if (g_fs_metadata_cache != nullptr) {
      g_fs_metadata_cache->preload_all_feature_views();
      g_fs_metadata_cache->start_event_watcher();
    }
  }

  // Start rondis
  if (globalConfigs.rondis.enable) {
    g_rondis_conn_factory = new RondisConnFactory();
    g_rondis_handle = new RondisHandle();
    g_database_index = (Uint32*)malloc(sizeof(Uint32) * num_rondis_threads);
    memset(g_database_index, 0, sizeof(Uint32) * num_rondis_threads);
    bool dirty_incr_decr_flag[MAX_NUM_DATABASES];
    bool opt_small_values_flag[MAX_NUM_DATABASES];
    memset(&dirty_incr_decr_flag[0], 0, sizeof(dirty_incr_decr_flag));
    memset(&opt_small_values_flag[0], 0, sizeof(dirty_incr_decr_flag));
    for (Uint32 i = 0; i < globalConfigs.rondis.numDatabases; i++) {
      dirty_incr_decr_flag[i] = globalConfigs.rondis.databases[i].dirtyIncrDecr;
      opt_small_values_flag[i] =
        globalConfigs.rondis.databases[i].optimizeSmallValues;
    }
    printf("Starting %u Rondis databases on %s:%u with %u threads\n",
      globalConfigs.rondis.numDatabases,
      globalConfigs.rondis.serverIP.c_str(),
      globalConfigs.rondis.serverPort,
      globalConfigs.rondis.numThreads);
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
      rondis_start_cmd,
      rondis_end_cmd,
      num_rdrs_threads,
      (int)num_rondis_threads,
      g_database_index,
      globalConfigs.rondis.numDatabases,
      &dirty_incr_decr_flag[0],
      &opt_small_values_flag[0],
      globalConfigs.rondis.createTables,
      globalConfigs.rondis.requireTablesOnStartup,
      globalConfigs.rondis.mysqlHost.c_str(),
      globalConfigs.rondis.mysqlPort,
      globalConfigs.rondis.mysqlUser.c_str(),
      globalConfigs.rondis.mysqlPassword.c_str());
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
    printf("Rondis Server running on %s:%u with %u databases\n",
      globalConfigs.rondis.serverIP.c_str(),
      globalConfigs.rondis.serverPort,
      globalConfigs.rondis.numDatabases);
  }

  // Start MySQL protocol router
  if (globalConfigs.mysqlRouter.enable) {
    // Set up RonSQL handler for SELECT routing.
    // MySQL router worker_ids are offset by the number of RDRS + Rondis
    // threads so they map to distinct NDB objects in the connection pool.
    g_mysql_ronsql_handler = mysql_router_ronsql_handler;

    // MySQL router threads come after REST + Rondis threads in the
    // NDB connection pool thread index space.
    int mysql_router_thread_offset =
        (int)(num_rdrs_threads + num_rondis_threads);
    const char* tls_cert = globalConfigs.security.tls.enableTLS
        ? globalConfigs.security.tls.certificateFile.c_str() : "";
    const char* tls_key = globalConfigs.security.tls.enableTLS
        ? globalConfigs.security.tls.privateKeyFile.c_str() : "";
    g_mysql_router_conn_factory = new MysqlConnFactory(
        globalConfigs.mysqlRouter.backendHost.c_str(),
        globalConfigs.mysqlRouter.backendPort,
        mysql_router_thread_offset,
        globalConfigs.mysqlRouter.debugLogging,
        tls_cert, tls_key);
    g_mysql_router_handle = new MysqlHandle();

    printf("Starting MySQL Router on %s:%u with %u threads "
           "(backend %s:%u)\n",
        globalConfigs.mysqlRouter.serverIP.c_str(),
        globalConfigs.mysqlRouter.serverPort,
        globalConfigs.mysqlRouter.numThreads,
        globalConfigs.mysqlRouter.backendHost.c_str(),
        globalConfigs.mysqlRouter.backendPort);

    // Wait for backend mysqld to be reachable before accepting client
    // connections. During cluster startup, RDRS may start before mysqld
    // is ready. Without this check, early client connections would fail.
    {
      printf("Waiting for backend mysqld at %s:%u...\n",
          globalConfigs.mysqlRouter.backendHost.c_str(),
          globalConfigs.mysqlRouter.backendPort);
      int backend_fd = -1;
      for (int attempt = 0; attempt < 120; attempt++) {
        backend_fd = mysql_protocol::connect_to_backend(
            globalConfigs.mysqlRouter.backendHost.c_str(),
            globalConfigs.mysqlRouter.backendPort);
        if (backend_fd >= 0) {
          close(backend_fd);
          break;
        }
        usleep(500 * 1000);  // 500ms between retries, up to 60 seconds
      }
      if (backend_fd < 0) {
        printf("Error: backend mysqld at %s:%u not reachable after 60s\n",
            globalConfigs.mysqlRouter.backendHost.c_str(),
            globalConfigs.mysqlRouter.backendPort);
        g_exit_code = 1;
        do_exit();
      }
      printf("Backend mysqld at %s:%u is reachable\n",
          globalConfigs.mysqlRouter.backendHost.c_str(),
          globalConfigs.mysqlRouter.backendPort);
    }

    g_mysql_router_thread = NewDispatchThread(
        globalConfigs.mysqlRouter.serverIP,
        globalConfigs.mysqlRouter.serverPort,
        globalConfigs.mysqlRouter.numThreads,
        g_mysql_router_conn_factory,
        1000,
        1000,
        g_mysql_router_handle);

    if (g_mysql_router_thread->StartThread() != 0) {
      printf("Error starting MySQL router thread\n");
      g_exit_code = 1;
      do_exit();
    }
    g_mysql_router_running = true;
    printf("MySQL Router running on %s:%u\n",
        globalConfigs.mysqlRouter.serverIP.c_str(),
        globalConfigs.mysqlRouter.serverPort);
  }

  if (globalConfigs.rest.enable) {
    if (globalConfigs.security.tls.enableTLS) {
      status = GenerateTLSConfig(
        globalConfigs.security.tls.requireClientCert,
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

    /*
     * RonSQL workers (m3_run6_plan.md B2): /ronsql statements execute
     * here instead of on the IO loop that received them, so a long
     * statement cannot stall the other connections of that loop.
     */
    if (num_ronsql_workers > 0) {
      g_ronsql_worker_pool =
        new RonSQLWorkerPool(num_ronsql_workers,
                             globalConfigs.ronsql.maxQueuedRequests,
                             ronsql_worker_thread_offset,
                             kRestThreadStackSize);
      if (!g_ronsql_worker_pool->start()) {
        std::cerr << "Failed to start the RonSQL worker threads.\n";
        g_exit_code = 1;
        do_exit();
      }
      printf("Started %u RonSQL worker threads (queue limit %u)\n",
             num_ronsql_workers,
             globalConfigs.ronsql.maxQueuedRequests);
    }

    drogon::app().addListener(globalConfigs.rest.serverIP,
                              globalConfigs.rest.serverPort,
                              globalConfigs.security.tls.enableTLS,
                              globalConfigs.security.tls.certificateFile,
                              globalConfigs.security.tls.privateKeyFile);
    drogon::app().setThreadNum(globalConfigs.rest.numThreads);
    drogon::app().setThreadStackSize(kRestThreadStackSize);
    /* Drogon buffers oversized request bodies in files under
     * <uploadPath>/tmp/ and pre-creates that tree at startup, so the path
     * must be writable; empty keeps Drogon's default of ./uploads. */
    if (!globalConfigs.rest.uploadPath.empty()) {
      drogon::app().setUploadPath(globalConfigs.rest.uploadPath);
    }

    /* Connection-lifetime limits: closing long-lived keep-alive connections
     * periodically lets a Kubernetes Service - which balances per TCP
     * connection, not per request - redistribute clients across pods, so a
     * post-restart connection pile-up on the first-ready pod levels out.
     * MaxKeepaliveRequests 0 means no limit (Drogon treats 0 that way);
     * IdleConnectionTimeoutS 0 keeps Drogon's built-in 60 second idle
     * timeout - it does NOT disable idle closing, hence the guarded call. */
    drogon::app().setKeepaliveRequestsNumber(
      globalConfigs.rest.maxKeepaliveRequests);
    if (globalConfigs.rest.idleConnectionTimeoutS > 0) {
      drogon::app().setIdleConnectionTimeout(
        globalConfigs.rest.idleConnectionTimeoutS);
    }

    // Install Internal.maxReqSize as the HTTP server's client body
    // limit.  Without this, drogon's built-in 1 MB default silently
    // SHADOWED the configurable limit: any request over 1 MB was
    // refused with 413 before assembly regardless of maxReqSize, and
    // raising maxReqSize had no effect.  With the two aligned, drogon
    // refuses over-limit requests with 413 up front and the
    // per-controller maxReqSize checks remain as backstops.
    drogon::app().setClientMaxBodySize(globalConfigs.internal.maxReqSize);
    drogon::app().disableSession();
    drogon::app().registerBeginningAdvice([]() {
      auto addresses = drogon::app().getListeners();
      for (auto &address : addresses) {
        printf("RDRS Server running on %s\n", address.toIpPort().c_str());
      }
      /* Both probe-port endpoints require g_drogon_up for a 200: neither a
       * startup probe (ping) nor a readiness probe (health) may pass before
       * the main port accepts connections. This advice runs immediately
       * BEFORE ListenerManager::startListening() (same queued main-loop
       * lambda), and each listener's listen() is then queued to its own IO
       * loop - so setting the flag here directly would open a window where
       * probes pass while the main port still refuses connections. Latch the
       * flag from a one-shot thread that confirms the ground truth instead:
       * a real TCP connect to the main port. A plain connect suffices under
       * TLS too (acceptance happens before any handshake). */
      std::thread([]() {
        std::string ip = globalConfigs.rest.serverIP;
        bool v6 = ip.find(':') != std::string::npos;
        if (ip == "0.0.0.0") ip = "127.0.0.1";
        if (ip == "::") ip = "::1";
        /* Bounded: if the connect never succeeds something is fatally wrong
         * with the listener and do_exit() will tear the process down anyway;
         * do not wedge the latch thread forever. */
        for (int attempt = 0; attempt < 3000; attempt++) {
          int fd = ::socket(v6 ? AF_INET6 : AF_INET, SOCK_STREAM, 0);
          if (fd >= 0) {
            bool connected = false;
            if (v6) {
              struct sockaddr_in6 addr;
              std::memset(&addr, 0, sizeof(addr));
              addr.sin6_family = AF_INET6;
              addr.sin6_port = htons(globalConfigs.rest.serverPort);
              ::inet_pton(AF_INET6, ip.c_str(), &addr.sin6_addr);
              connected = ::connect(fd, (struct sockaddr *)&addr,
                                    sizeof(addr)) == 0;
            } else {
              struct sockaddr_in addr;
              std::memset(&addr, 0, sizeof(addr));
              addr.sin_family = AF_INET;
              addr.sin_port = htons(globalConfigs.rest.serverPort);
              ::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);
              connected = ::connect(fd, (struct sockaddr *)&addr,
                                    sizeof(addr)) == 0;
            }
            ::close(fd);
            if (connected) {
              g_drogon_up.store(true, std::memory_order_release);
              return;
            }
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        rdrs_logger::error(
          "Main REST port never accepted a connection; probe endpoints stay "
          "at 503");
      }).detach();
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
    /* The main port no longer accepts; both probe-port endpoints go back to
     * 503 for the remainder of the teardown. */
    g_drogon_up.store(false, std::memory_order_release);
  } else {
    // REST is disabled — block until signal received.
    // MySQL router and/or Rondis are running in their own threads.
    // handle_signal_async() records the signal; shut down gracefully from
    // here, outside signal context. The signal may be handled on any
    // thread, so it does not always cut this sleep short; the 1s poll
    // bounds the latency.
    g_startup_done = 1;
    printf("REST server disabled, running.\n");
    while (g_pending_signal == 0) {
      sleep(1);
    }
    handle_signal(g_pending_signal);  // Does not return.
  }
  do_exit();
}
