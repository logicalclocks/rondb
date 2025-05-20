/*
 * Copyright (C) 2023, 2025 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_RONDB_CONNECTION_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_RONDB_CONNECTION_

#include "rdrs_dal.h"

#include <list>
#include <mutex>
#include <NdbApi.hpp>
#include <NdbMutex.h>
#include <util/require.h>

class RDRSRonDBConnection {
  static constexpr int MAX_PARALLEL_KEY_OPS = 1024;
  static constexpr Uint32 expectedMagic = 0x52b5cb03;

 private:
  Uint32 magic = 0;
  /* connectionMutex protects everything except stats. connectionInfoMutex
   * protects only stats. When used in combination, order of (un)locking is
   * 1) connectionMutex
   * 2) connectionInfoMutex
   */
  NdbMutex *connectionMutex;
  NdbMutex *connectionInfoMutex;
  RonDB_Stats stats;

  Ndb_cluster_connection *ndbConnection;
  char *connection_string;
  Uint32 m_node_id;
  Uint32 connection_retries;
  Uint32 connection_retry_delay_in_sec;
  struct NdbThread *reconnectionThread;

  // This is a list of NDB objects that are available for use.
  // When a  user request an NDB object then we return an
  // NDB object from this list. When a user returns the
  // NDB object then we put it back in this list
  std::list<Ndb *> availableNdbObjects;

  // This a list of all the NDB objects whether the objects
  // are in use or not
  std::list<Ndb *> allAvailableNdbObjects;

  inline void checkMagic() { require(magic == expectedMagic); }

 public:
  RDRSRonDBConnection(const char *connection_string,
                      Uint32 node_id,
                      Uint32 connection_retries,
                      Uint32 connection_retry_delay_in_sec);
  ~RDRSRonDBConnection();

  /**
   * Connect to RonDB
   *
   * @return Status
   */
  RS_Status Connect();

  /**
   * Returns Ndb object
   *
   * New NDB object will be created if all
   * existing NDB Objects are in use
   *
   * @return Status and NDB object
   */
  RS_Status GetNdbObject(Ndb **ndb_object);

  /**
   * Return resource back to the pool.
   *
   * @param ndb_object Ndb object
   * @param status Status of last operation performed using this ndb object.
   *        It can be null
   */
  void ReturnNDBObjectToPool(Ndb *ndb_object, RS_Status *status);

  /**
   * Get status
   */
  void GetStats(RonDB_Stats&);

  /**
   * Starts reconnection thread which calls the ReconnectHandler
   * Note: This is only made public for testing.
   *
   */
  RS_Status Reconnect();

  /**
   * Reconnection Handler
   *
   */
  RS_Status ReconnectHandler();

  /**
   * Purge. Delete all Ndb objects and shutdown connection
   * @param end If true then it will also free the memory
   * used to store nodeIds and connection string
   *
   * @return RS_Status
   */
  RS_Status Shutdown(bool end);
};
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_RONDB_CONNECTION_
