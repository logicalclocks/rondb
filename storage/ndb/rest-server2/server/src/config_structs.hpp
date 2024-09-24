/*
 * Copyright (c) 2023, 2024, Hopsworks and/or its affiliates.
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_CONFIG_STRUCTS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_CONFIG_STRUCTS_HPP_

#include "rdrs_dal.h"

#include <string>
#include <mutex>
#include <cstdint>
#include <sys/types.h>
#include <vector>

#define CLASS(NAME, ...) class NAME { public: __VA_ARGS__ NAME(); };
#define CM(DATATYPE, VARIABLENAME, JSONKEYNAME, INITEXPR) DATATYPE VARIABLENAME;
#define PROBLEM(CONDITION, MESSAGE)
#define CLASSDEFS(...) __VA_ARGS__
#define VECTOR(DATATYPE)

#include "config_structs_def.hpp"

#undef CLASS
#undef CM
#undef PROBLEM
#undef CLASSDEFS
#undef VECTOR

extern AllConfigs globalConfigs;
extern std::mutex globalConfigsMutex;

bool isUniteTest();

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_CONFIG_STRUCTS_HPP_
