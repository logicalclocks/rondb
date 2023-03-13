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
#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_LOGGER_HPP_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_LOGGER_HPP_

#include <string>
#include "src/rdrs-dal.h"

#define PanicLevel 0
#define FatalLevel 1
#define ErrorLevel 2
#define WarnLevel  3
#define InfoLevel  4
#define DebugLevel 5
#define TraceLevel 6

// FIXME TODO  Make small function inline and pass log level from go layer  JIRA RONDB-287

void log(const int level, const char *msg);

void setLogCallBackFns(const Callbacks cbs);

void log(const int level, const char *msg);

void PANIC(const char *msg);

void PANIC(const std::string msg);

void FATAL(const char *msg);

void FATAL(const std::string msg);

void ERROR(const char *msg);

void ERROR(const std::string msg);

void WARN(const char *msg);

void WARN(const std::string msg);

void INFO(const char *msg);

void INFO(const std::string msg);

void DEBUG(const char *msg);

void DEBUG(const std::string msg);

void TRACE(char *msg);

#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_LOGGER_HPP_
