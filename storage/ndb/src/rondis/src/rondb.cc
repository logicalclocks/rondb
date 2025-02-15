/*
   Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include "server_thread.h"
#include "pink_conn.h"
#include "redis_conn.h"
#include "pink_thread.h"
#include "rondb.h"
#include "common.h"
#include "table_definitions.h"
#include "commands.h"
#include <strings.h>
#include <cassert>

//#define DEBUG_NDB_CMD 1

#ifdef DEBUG_NDB_CMD
#define DEB_NDB_CMD(arglist) do { printf arglist ; } while (0)
#else
#define DEB_NDB_CMD(arglist)
#endif

/**
 * Ndb objects are not thread-safe. Hence, each worker thread / RonDB
 * connection should have its own Ndb object. If we have more worker
 * threads than cluster connections, we can create multiple Ndb objects
 * from a single cluster connection.
 * Essentially we want:
 *    num worker threads == number Ndbs objects
 * whereby some cluster connections may have created more Ndb objects
 * than others.
*/
int initialize_ndb_objects(const char *connect_string, int num_ndb_objects) {
  Ndb_cluster_connection *rondb_conn[RONDIS_MAX_CONNECTIONS];
  for (unsigned int i = 0; i < RONDIS_MAX_CONNECTIONS; i++) {
    rondb_conn[i] = new Ndb_cluster_connection(connect_string);
    if (rondb_conn[i]->connect() != 0) {
      printf("Failed with RonDB MGMd connection nr. %d\n", i);
      return -1;
    }
    printf("RonDB MGMd connection nr. %d is ready\n", i);
    if (rondb_conn[i]->wait_until_ready(30, 0) != 0) {
      printf("Failed with RonDB data node connection nr. %d\n", i);
      return -1;
    }
    printf("RonDB data node connection nr. %d is ready\n", i);
  }
  for (int j = 0; j < num_ndb_objects; j++) {
    int connection_num = j % RONDIS_MAX_CONNECTIONS;
    Ndb *ndb = new Ndb(rondb_conn[connection_num], REDIS_DB_NAME);
    if (ndb == nullptr) {
      printf("Failed creating Ndb object nr. %d for"
             " cluster connection %d\n",
             j, connection_num);
      return -1;
    }
    if (ndb->init(MAX_PARALLEL_KEY_OPS) != 0) {
      printf("Failed initializing Ndb object nr. %d for"
             " cluster connection %d\n",
             j, connection_num);
      return -1;
    }
    printf("Successfully initialized Ndb object nr. %d for"
           " cluster connection %d\n",
           j, connection_num);
    ndb_objects[j] = ndb;
  }
  return 0;
}

#define INSIDE_RDRS2 1
#define RONDIS_STANDALONE 2

int rondis_execution_variant = 0;
int setup_rondb(const char *connect_string, int num_ndb_objects) {
  // Creating static thread-safe Ndb objects for all connections
  assert(rondis_execution_variant == 0);
  rondis_execution_variant = RONDIS_STANDALONE;
  ndb_init();
  int res = initialize_ndb_objects(connect_string, num_ndb_objects);
  if (res != 0) {
    return res;
  }
  Ndb *ndb = ndb_objects[0];
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  if (init_string_records(dict, 0) != 0) {
    printf("Failed initializing records for Redis data type STRING;"
           " error: %s\n",
           ndb->getNdbError().message);
    return -1;
  }
  return 0;
}

void rondb_end() {
    ndb_end(0);
}

void print_args(const pink::RedisCmdArgsType &argv) {
  for (const auto &arg : argv) {
    printf("%s ", arg.c_str());
  }
  printf("\n");
}

void* (*g_get_ndb_object_func_ptr)(int);
void (*g_return_ndb_object_func_ptr)(void*,int);
void (*g_exit_func_ptr)(void);
void* (*g_start_cmd_func_ptr)(void);
void (*g_end_cmd_func_ptr)(void*);
Uint32 g_first_thread_id = 0;
Uint32 *g_current_database_index;
bool g_is_incr_decr_dirty[MAX_NUM_DATABASES];
bool g_opt_small_values_flag[MAX_NUM_DATABASES];
Uint32 g_num_databases = 0;
int g_num_threads = 0;

class NdbObjectGuard {
  public:
  NdbObjectGuard(int worker_id, Uint32 database_id)
    : m_worker_id(worker_id + g_first_thread_id)
  {
    if (rondis_execution_variant == INSIDE_RDRS2) {
      Ndb *ndb = (Ndb*)g_get_ndb_object_func_ptr(m_worker_id);
      if(ndb != nullptr) {
        char buf[16];
        snprintf(buf, sizeof(buf), "%s_%u", REDIS_DB_NAME, database_id);
        ndb->setDatabaseName(buf);
      }
      m_ndb = ndb;
      return;
    } else if (rondis_execution_variant == RONDIS_STANDALONE) {
      m_ndb = ndb_objects[worker_id];
      return;
    }
    assert(false);
    m_ndb = nullptr;
  }
  ~NdbObjectGuard() {
    if (m_ndb && rondis_execution_variant == INSIDE_RDRS2) {
      g_return_ndb_object_func_ptr((void*)m_ndb, m_worker_id);
    }
  }
  Ndb *get_guard_ndb_object() {
    return m_ndb;
  }
  Ndb *m_ndb;
  int m_worker_id;
};

int setup_ndb_connection_for_rondis(
 void* (*get_ndb_object_func_ptr)(int),
 void (*return_ndb_object_func_ptr)(void*, int),
 void (*exit_func_ptr)(void),
 void* (*start_cmd_func_ptr)(void),
 void (*end_cmd_func_ptr)(void*),
 Uint32 first_thread_id,
 int num_threads,
 Uint32 *database_index,
 Uint32 num_databases,
 bool *dirty_incr_decr_flag,
 bool *opt_small_values_flag) {
  assert(rondis_execution_variant == 0);
  rondis_execution_variant = INSIDE_RDRS2;
  g_get_ndb_object_func_ptr = get_ndb_object_func_ptr;
  g_return_ndb_object_func_ptr = return_ndb_object_func_ptr;
  g_exit_func_ptr = exit_func_ptr;
  g_start_cmd_func_ptr = start_cmd_func_ptr;
  g_end_cmd_func_ptr = end_cmd_func_ptr;
  g_first_thread_id = first_thread_id;
  g_num_threads = num_threads;
  g_current_database_index = database_index;
  g_num_databases = num_databases;
  for (int i = 0; i < MAX_NUM_DATABASES; i++) {
    g_is_incr_decr_dirty[i] = false;
    g_opt_small_values_flag[i] = true;
  }
  for (Uint32 i = 0; i < num_databases; i++) {
    NdbObjectGuard ndbObjectGuard(0, i);
    Ndb *ndb = ndbObjectGuard.get_guard_ndb_object();
    g_is_incr_decr_dirty[i] = dirty_incr_decr_flag[i];
    g_opt_small_values_flag[i] = opt_small_values_flag[i];
    NdbDictionary::Dictionary *dict = ndb->getDictionary();
    if (init_string_records(dict, i) != 0) {
      printf("Failed initializing records for Redis data type STRING;"
             " error: %s\n",
           ndb->getNdbError().message);
      return -1;
    }
  }
  return 0;
}

void unsupported_command(const pink::RedisCmdArgsType &argv,
                         std::string *response) {
  printf("Unsupported command: ");
  print_args(argv);
  char error_message[256];
  snprintf(error_message,
           sizeof(error_message),
           REDIS_UNKNOWN_COMMAND,
           argv[0].c_str());
  assign_generic_err_to_response(response, error_message);
}

void unavailable_cluster(const pink::RedisCmdArgsType &argv,
                         std::string *response) {
  printf("Cluster unavailble: ");
  print_args(argv);
  char error_message[256];
  snprintf(error_message,
           sizeof(error_message),
           REDIS_UNKNOWN_COMMAND,
           argv[0].c_str());
  assign_generic_err_to_response(response, error_message);
}

void wrong_number_of_arguments(const pink::RedisCmdArgsType &argv,
                               std::string *response) {
  char error_message[256];
  snprintf(error_message,
           sizeof(error_message),
           REDIS_WRONG_NUMBER_OF_ARGS,
           argv[0].c_str());
  assign_generic_err_to_response(response, error_message);
}

class RondisEndPoint {
  public:
  RondisEndPoint() {
    metricsUpdaterObject = g_start_cmd_func_ptr();
  }
  ~RondisEndPoint() {
    g_end_cmd_func_ptr(metricsUpdaterObject);
  }
  void *metricsUpdaterObject;
};

int rondb_redis_handler(const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id) {
  RondisEndPoint rondisEndpoint;
  // First check non-ndb commands
  const char *command = argv[0].c_str();
  if (strcasecmp(command, "ping") == 0) {
    if (argv.size() != 1) {
      wrong_number_of_arguments(argv, response);
      return 0;
    }
    response->append("+PONG\r\n");
  } else if (strcasecmp(command, "echo") == 0) {
    if (argv.size() != 2) {
      wrong_number_of_arguments(argv, response);
      return 0;
    }
    response->assign("$" +
                     std::to_string(argv[1].length()) +
                     "\r\n" +
                     argv[1] +
                     "\r\n");
  } else if (strcasecmp(command, "config") == 0) {
    if (argv.size() != 3) {
      wrong_number_of_arguments(argv, response);
      return 0;
    }
    if (argv[1] == "GET") {
      *response += "*2\r\n";
      *response += "$" + std::to_string(argv[2].length()) + "\r\n";
      *response += argv[2] + "\r\n";
      *response += "*0\r\n";
    } else {
      unsupported_command(argv, response);
    }
  } else if (strcasecmp(command, "select") == 0) {
    if (argv.size() != 2) {
      wrong_number_of_arguments(argv, response);
      return 0;
    }
    char *end_ptr = nullptr;
    const char *val_ptr = argv[1].c_str();
    const char *memory_end = val_ptr + argv[1].size();
    Int64 val = strtoll(val_ptr,
                        &end_ptr,
                        10);
    if (errno == ERANGE || end_ptr != memory_end) {
      assign_err_to_response(response,
                             FAILED_SELECT_COMMAND,
                             1);
      return 0;
    }
    if (val >= g_num_databases) {
      assign_err_to_response(response,
                             FAILLED_SELECT_NO_SUCH_DATABASE,
                             1);
    }
    set_current_database(worker_id, (int)val);
    response->append("+OK\r\n");
    return 0;
  } else {
    Uint32 database_id = get_current_database(worker_id);
    NdbObjectGuard ndbObjectGuard(worker_id, database_id);
    Ndb *ndb = ndbObjectGuard.get_guard_ndb_object();
    if (ndb == nullptr) {
      unavailable_cluster(argv, response);
      return 0;
    }
    DEB_NDB_CMD(("cmd: %s, params: %lu\n", command, argv.size()));
#ifdef DEBUG_NDB_CMD
    for (Uint32 i = 1; i < argv.size(); i++) {
      DEB_NDB_CMD(("param[%u]: %s is len: %lu\n",
        i, argv[i].c_str(), argv[i].size()));
     }
#endif
    if (strcasecmp(command, "GET") == 0) {
      if (argv.size() == 2) {
        rondb_get_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "MGET") == 0) {
      if (argv.size() >= 2) {
        rondb_mget_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "SET") == 0) {
      if (argv.size() >= 3) {
        rondb_set_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "MSET") == 0) {
      if (argv.size() >= 3 && (argv.size() % 2) == 1) {
        rondb_mset_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HGET") == 0) {
      if (argv.size() == 3) {
        rondb_hget_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HMGET") == 0) {
      if (argv.size() >= 3) {
        rondb_hmget_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HSET") == 0) {
      if (argv.size() >= 4 && (argv.size() % 2) == 0) {
        rondb_hset_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HMSET") == 0) {
      if (argv.size() >= 4 && (argv.size() % 2) == 0) {
        rondb_hset_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "DEL") == 0) {
      if (argv.size() >= 2) {
        rondb_del_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HDEL") == 0) {
      if (argv.size() >= 2) {
        rondb_hdel_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "INCR") == 0) {
      if (argv.size() == 2) {
        rondb_incr_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "INCRBY") == 0) {
      if (argv.size() == 3) {
        rondb_incrby_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "DECR") == 0) {
      if (argv.size() == 2) {
        rondb_decr_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "DECRBY") == 0) {
      if (argv.size() == 3) {
        rondb_decrby_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HINCR") == 0) {
      if (argv.size() == 3) {
        rondb_hincr_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HINCRBY") == 0) {
      if (argv.size() == 4) {
        rondb_hincrby_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HDECR") == 0) {
      if (argv.size() == 3) {
        rondb_hdecr_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HDECRBY") == 0) {
      if (argv.size() == 4) {
        rondb_hdecrby_command(ndb, argv, response, worker_id);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else {
      unsupported_command(argv, response);
    }
    if (ndb->getClientStat(ndb->TransStartCount) !=
        ndb->getClientStat(ndb->TransCloseCount)) {
      /**
       * If we are here, we have a transaction that was not closed.
       * Only a certain amount of transactions can be open at the same time.
       * If this limit is reached, the Ndb object will not create any new ones.
       * Hence, better to catch these cases early.
       */
      printf("Failed to stop transaction\n");
      //print_args(argv);
      printf("Number of transactions started: %lld\n",
        ndb->getClientStat(ndb->TransStartCount));
      printf("Number of transactions closed: %lld\n",
        ndb->getClientStat(ndb->TransCloseCount));
      if (rondis_execution_variant == INSIDE_RDRS2) {
        g_exit_func_ptr();
      } else {
        exit(1);
      }
      return 0;
    }
  }
  return 0;
}

void set_current_database(int index, Uint32 database_index) {
  g_current_database_index[index] = database_index;
}

Uint32 get_current_database(int worker_id) {
  assert(worker_id < g_num_threads);
  Uint32 database_id = g_current_database_index[worker_id];
  assert(database_id < g_num_databases);
  return database_id;
}

bool get_dirty_incr_decr_flag(int worker_id) {
  assert(worker_id < g_num_threads);
  Uint32 database_id = g_current_database_index[worker_id];
  assert(database_id < g_num_databases);
  return g_is_incr_decr_dirty[database_id];
}

bool get_opt_small_values_flag(int worker_id) {
  assert(worker_id < g_num_threads);
  Uint32 database_id = g_current_database_index[worker_id];
  assert(database_id < g_num_databases);
  return g_opt_small_values_flag[database_id];
}
