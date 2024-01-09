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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_CONNECTION_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_CONNECTION_HPP_

#include "config_structs.hpp"
#include "src/logger.hpp"

#include <string>
#include <exception>
#include <iostream>

class RonDBConnection {
 public:
  RonDBConnection(RonDB &data_cluster, RonDB &meta_data_cluster) {
    RS_Status status = init_rondb_connection(data_cluster, meta_data_cluster);
    if (status.http_code != SUCCESS) {
      RDRSLogger::LOG_ERROR("Failed to initialize RonDB connection: " +
                            std::string(status.message));
      throw std::runtime_error(status.message);
    }
  }

  RonDBConnection(RonDB &&data_cluster, RonDB &&meta_data_cluster) {
    RS_Status status = init_rondb_connection(data_cluster, meta_data_cluster);
    if (status.http_code != SUCCESS) {
      RDRSLogger::LOG_ERROR("Failed to initialize RonDB connection: " +
                            std::string(status.message));
      throw std::runtime_error(status.message);
    }
  }

  ~RonDBConnection() {
    RS_Status status = shutdown_rondb_connection();
    if (status.http_code != SUCCESS) {
      RDRSLogger::LOG_ERROR("Failed to shutdown RonDB connection: " + std::string(status.message));
    }
  }

  RonDBConnection(const RonDBConnection &other) = default;

  RonDBConnection &operator=(const RonDBConnection &other) = default;

  RonDBConnection(RonDBConnection &&other) = default;

  RonDBConnection &operator=(RonDBConnection &&other) = default;

 private:
  static RS_Status init_rondb_connection(RonDB &, RonDB &) noexcept;

  static RS_Status shutdown_rondb_connection() noexcept;

  static RS_Status rondb_reconnect() noexcept;
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_CONNECTION_HPP_
