/*
 * Copyright (C) 2023 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_RONDB_CONNECTION_POOL_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_RONDB_CONNECTION_POOL_

#include "src/rdrs-dal.h"
#include "src/rdrs_rondb_connection.hpp"

class RDRSRonDBConnectionPool {
  private:
    RDRSRonDBConnection * dataConnection;
    RDRSRonDBConnection * metadataConnection;

 public:
  RDRSRonDBConnectionPool();
  ~RDRSRonDBConnectionPool();

  /**
   * @brief Init RonDB Client API 
   *
   * @return RS_Status A struct representing the status of the operation:
   *
   */
  RS_Status Init();

  /**
   * @brief Adds a connection to the RonDB Cluster.
   *
   * This function allows you to add a connection(s) to a RonDB Cluster. The connection(s) can be
   * used to read both data and metatdata from RonDB cluster(s)
   *
   * @param connection_string A C-style string representing the connection string. 
   * @param connection_pool_size The size of the connection pool for this connection. Currently you
   * can have only one connection in the pool. 
   * @param node_ids An array of node IDs to associate with this connection.
   * @param node_ids_len The length of the 'node_ids' array.
   * @param connection_retries The maximum number of connection retries in case of failure.
   * @param connection_retry_delay_in_sec The delay in seconds between connection retry attempts.
   *
   * @return RS_Status A struct representing the status of the operation:
   *
   * @note The function will block during connection establishment
   */
  RS_Status AddConnections(const char *connection_string, unsigned int connection_pool_size,
                          unsigned int *node_ids, unsigned int node_ids_len,
                          unsigned int connection_retries,
                          unsigned int connection_retry_delay_in_sec);

  /**
   * @brief Adds a connection to the RonDB Cluster.
   *
   * This function allows you to add a connection(s) to a RonDB Cluster. These are dedicated
   * connection(s) for reading metadata. If metadata connection(s) are defined then
   * the connection added using @ref AddConnection() will only be used for reading data.     
   *
   * @param connection_string A C-style string representing the connection string. 
   * @param connection_pool_size The size of the connection pool for this connection. Currently you
   * can have only one connection in the pool. 
   * @param node_ids An array of node IDs to associate with this connection.
   * @param node_ids_len The length of the 'node_ids' array.
   * @param connection_retries The maximum number of connection retries in case of failure.
   * @param connection_retry_delay_in_sec The delay in seconds between connection retry attempts.
   *
   * @return RS_Status A struct representing the status of the operation:
   *
   * @note The function will block during connection establishment
   */
  RS_Status AddMetaConnections(const char *connection_string, unsigned int connection_pool_size,
                              unsigned int *node_ids, unsigned int node_ids_len,
                              unsigned int connection_retries,
                              unsigned int connection_retry_delay_in_sec);

 
  /**
   * @brief Get ndb object for data operation
   *
   * @param ndb_object.  
   *
   * @return RS_Status A struct representing the status of the operation:
   */
  RS_Status GetNdbObject(Ndb **ndb_object);

  /**
   * @brief Return NDB Object back to the pool 
   *
   * @param ndb_object.  
   *
   * @return RS_Status A struct representing the status of the operation:
   */
  RS_Status ReturnNdbObject(Ndb *ndb_object, RS_Status *status);

  /**
   * @brief Get ndb object for metadata operation
   *
   * @param ndb_object.  
   *
   * @return RS_Status A struct representing the status of the operation:
   */
  RS_Status GetMetadataNdbObject(Ndb **ndb_object);

  /**
   * @brief Return NDB Object back to the pool 
   *
   * @param ndb_object.  
   * @param status
   *
   * @return RS_Status A struct representing the status of the operation:
   */
  RS_Status ReturnMetadataNdbObject(Ndb *ndb_object, RS_Status *status);

  /**
   * @brief Restart connections 
   *
   * @return RS_Status A struct representing the status of the operation:
   */
  RS_Status Reconnect();

  /**
   * @brief Get connection statistis 
   *
   * @return  
   */
  RonDB_Stats GetStats();

};

#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_RDRS_RONDB_CONNECTION_POOL_
