/*
 * Copyright (C) 2023, 2024 Hopsworks AB
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

class RDRSRonDBConnection {

 private:
  // this is used when we update the connection, NDB objects etc.
  NdbMutex *connectionMutex;
  //  this is used togather with connectionMutex to quickly
  //  access simple information such as if the connection is
  //  open or not, if it is in reconnection phase, etc.
  NdbMutex *connectionInfoMutex;
  RonDB_Stats stats;

  Ndb_cluster_connection *ndbConnection;
  char *connection_string;
  Uint32 *node_ids;
  Uint32 node_ids_len;
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

 public:
  RDRSRonDBConnection(const char *connection_string,
                      unsigned int *node_ids,
                      unsigned int node_ids_len,
                      unsigned int connection_retries,
                      unsigned int connection_retry_delay_in_sec);
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
   * @param threadIndex Thread to use the Ndb object
   *
   * @return Status and NDB object
   */
  RS_Status GetNdbObject(Ndb **ndb_object, Uint32 threadIndex);

  /**
   * Return resource back to the pool.
   *
   * @param ndb_object Ndb object
   * @param threadIndex Thread that used the Ndb object
   * @param status Status of last operation performed using this ndb object.
   *        It can be null
   */
  void ReturnNDBObjectToPool(Ndb *ndb_object,
                             RS_Status *status,
                             Uint32 threadIndex);

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

 private:
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
