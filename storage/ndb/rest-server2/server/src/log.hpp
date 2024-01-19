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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_LOG_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_LOG_HPP_

#include <string>

class LogConfig {
 public:
  std::string level;
  std::string filePath;
  int maxSizeMb;
  int maxBackups;
  int maxAge;
  LogConfig();
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_LOG_HPP_
