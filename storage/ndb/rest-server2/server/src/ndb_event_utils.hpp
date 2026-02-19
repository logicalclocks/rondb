/*
 * Copyright (C) 2025 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_NDB_EVENT_UTILS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_NDB_EVENT_UTILS_HPP_

#include <cstdint>
#include <cstdio>
#include <random>
#include <string>

inline std::string generate_event_uuid() {
  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dis;
  uint64_t val = dis(gen);
  char buf[17];
  snprintf(buf, sizeof(buf), "%016llx", (unsigned long long)val);
  return std::string(buf);
}

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_NDB_EVENT_UTILS_HPP_
