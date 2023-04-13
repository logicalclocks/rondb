/*
 * Copyright (C) 2022 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_RONDB_CONNECTION_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_RONDB_CONNECTION_

#include <list>
#include <mutex>
#include <NdbApi.hpp>
#include "src/rdrs-dal.h"

class RDRSRonDBConnection {

  enum STATE { CONNECTED, CONNECTING, DISCONNECTED };

 private:
  std::list<Ndb *> __ndbObjects;
  std::mutex __mutex;
  RonDB_Stats stats;
  Ndb_cluster_connection *ndbConnection;
  STATE connectionState = DISCONNECTED;

  static RDRSRonDBConnection *__instance;

  RDRSRonDBConnection() {
  }

 public:
  /**
   * Static method for initializing connection and NDB Object pool 
   *
   * @return ObjectPool instance.
   */
  static RS_Status Init(const char *connection_string, unsigned int connection_pool_size,
                            unsigned int *node_ids, unsigned int node_ids_len,
                            unsigned int connection_retries,
                            unsigned int connection_retry_delay_in_sec);

  /**
   * Static method for accessing class instance.
   *
   * @return ObjectPool instance.
   */
  static RS_Status GetInstance(RDRSRonDBConnection **);

  /**
   * Returns Ndb object
   *
   * New resource will be created if all the resources
   * were used at the time of the request.
   *
   * @return Status and Resource instance.
   */
  RS_Status GetNdbObject(Ndb **ndb_object);

  /**
   * Return resource back to the pool.
   *
   * @param object ndb objct
   * @param stauts of last operation performed using this ndb object. it can be null
   * @return void
   */
  void ReturnNDBObjectToPool(Ndb *object, RS_Status *status);

  /**
   * Get status
   *
   * @return RonDB_Stats
   */
  RonDB_Stats GetStats();

  /**
   * Purge. Delete all Ndb objects
   *
   */
  RS_Status Shutdown();

  /**
   * Reconnects. Closes existing connection 
   *
   */
  RS_Status Reconnect();
};
#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_RONDB_CONNECTION_

