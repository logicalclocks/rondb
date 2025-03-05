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

int setup_rondb(const char *connect_string, int num_ndb_objects) {
  // Creating static thread-safe Ndb objects for all connections
  ndb_init();
  int res = initialize_ndb_objects(connect_string, num_ndb_objects);
  if (res != 0) {
    return res;
  }
  Ndb *ndb = ndb_objects[0];
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  if (init_string_records(dict) != 0) {
    printf("Failed initializing records for Redis data type STRING; error: %s\n",
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

void wrong_number_of_arguments(const pink::RedisCmdArgsType &argv,
                               std::string *response) {
  char error_message[256];
  snprintf(error_message,
           sizeof(error_message),
           REDIS_WRONG_NUMBER_OF_ARGS,
           argv[0].c_str());
  assign_generic_err_to_response(response, error_message);
}

int rondb_redis_handler(const pink::RedisCmdArgsType &argv,
                        std::string *response,
                        int worker_id) {
  // First check non-ndb commands
  const char *command = argv[0].c_str();
  if (strcasecmp(command, "ping") == 0) {
    if (argv.size() != 1) {
      wrong_number_of_arguments(argv, response);
      return 0;
    }
    response->append("+PONG\r\n");
  } else if (argv[0] == "ECHO") {
    if (argv.size() != 2) {
      wrong_number_of_arguments(argv, response);
      return 0;
    }
    response->assign("$" +
                     std::to_string(argv[1].length()) +
                     "\r\n" +
                     argv[1] +
                     "\r\n");
  } else if (argv[0] == "CONFIG") {
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
  } else {
    Ndb *ndb = ndb_objects[worker_id];
    DEB_NDB_CMD(("cmd: %s, params: %lu\n", command, argv.size()));
#ifdef DEBUG_NDB_CMD
    for (Uint32 i = 1; i < argv.size(); i++) {
      DEB_NDB_CMD(("param[%u]: %s is len: %lu\n",
        i, argv[i].c_str(), argv[i].size()));
     }
#endif
    if (strcasecmp(command, "GET") == 0) {
      if (argv.size() == 2) {
        rondb_get_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "MGET") == 0) {
      if (argv.size() >= 2) {
        rondb_mget_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "SET") == 0) {
      if (argv.size() >= 3) {
        rondb_set_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "MSET") == 0) {
      if (argv.size() >= 3 && (argv.size() % 2) == 1) {
        rondb_mset_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HGET") == 0) {
      if (argv.size() == 3) {
        rondb_hget_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HMGET") == 0) {
      if (argv.size() >= 3) {
        rondb_hmget_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HSET") == 0) {
      if (argv.size() >= 4 && (argv.size() % 2) == 0) {
        rondb_hset_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HMSET") == 0) {
      if (argv.size() >= 4 && (argv.size() % 2) == 0) {
        rondb_hset_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "DEL") == 0) {
      if (argv.size() >= 2) {
        rondb_del_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HDEL") == 0) {
      if (argv.size() >= 2) {
        rondb_hdel_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "INCR") == 0) {
      if (argv.size() == 2) {
        rondb_incr_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "INCRBY") == 0) {
      if (argv.size() == 3) {
        rondb_incrby_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "DECR") == 0) {
      if (argv.size() == 2) {
        rondb_decr_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "DECRBY") == 0) {
      if (argv.size() == 3) {
        rondb_decrby_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HINCR") == 0) {
      if (argv.size() == 3) {
        rondb_hincr_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HINCRBY") == 0) {
      if (argv.size() == 4) {
        rondb_hincrby_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HDECR") == 0) {
      if (argv.size() == 3) {
        rondb_hdecr_command(ndb, argv, response);
      } else {
        wrong_number_of_arguments(argv, response);
        return 0;
      }
    } else if (strcasecmp(command, "HDECRBY") == 0) {
      if (argv.size() == 4) {
        rondb_hdecrby_command(ndb, argv, response);
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
      exit(1);
    }
  }
  return 0;
}
