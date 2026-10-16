/*
 * Copyright (c) 2024, 2026, Hopsworks and/or its affiliates.
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_STATUS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_STATUS_HPP_

#include "rdrs_dal.h"

#include <cstring>
#include <string>
#include <iostream>
#include <NdbApi.hpp>

/**
 * create an object of RS_Status.
 * Note it is the receiver responsibility to free the memory for msg and fileName
 * character array
 */

inline RS_Status __RS_ERROR(const HTTP_CODE http_code,
                            int status,
                            int classification,
                            int code,
                            int mysql_code,
                            std::string msg,
                            int line_no,
                            std::string file_name) {
  RS_Status ret;
  ret.http_code = http_code;
  ret.status = status;
  ret.classification = classification;
  ret.code = code;
  ret.mysql_code = mysql_code;
  ret.err_line_no = line_no;

  snprintf(ret.message, RS_STATUS_MSG_LEN, "%s", msg.c_str());
  snprintf(ret.err_file_name, RS_STATUS_FILE_NAME_LEN, "%s", file_name.c_str());

  return ret;
}

/**
 * Quota / rate limit errors (RONDB-978) map to HTTP 429 so that clients
 * can distinguish throttling from server failures.
 * 243/2203 = write/read rate limit exceeded
 * (TcKeyRef::WriteRateOverflowError / ReadRateOverflowError),
 * 247/248 = too many operations / concurrent transactions for the quota.
 *
 * This is the single source of truth for the 429 set. The bare-code variant
 * exists for callers that only have the code left, not the NdbError: RonSQL
 * closes the transaction before its RonSQLRateLimitError escapes the executor
 * and carries the code instead (ronsql_operation.cpp).
 */
inline HTTP_CODE __RONDB_ERROR_CODE_HTTP_CODE(int error_code) {
  switch (error_code) {
  case 243:
  case 2203:
  case 247:
  case 248:
    return TOO_MANY_REQUESTS;
  default:
    return SERVER_ERROR;
  }
}

inline HTTP_CODE __RONDB_ERROR_HTTP_CODE(const struct NdbError &error) {
  return __RONDB_ERROR_CODE_HTTP_CODE(error.code);
}

inline RS_Status __RS_ERROR_RONDB(const struct NdbError &error,
                                  std::string msg,
                                  int lineNo,
                                  std::string file_name) {
  std::string userMsg = "Error: " + msg + " Error: code: " + std::to_string(error.code) +
                        " MySQL Code: " + std::to_string(error.mysql_code) +
                        " Message: " + error.message;
  return __RS_ERROR(__RONDB_ERROR_HTTP_CODE(error),
                    error.status,
                    error.classification,
                    error.code,
                    error.mysql_code,
                    userMsg,
                    lineNo,
                    file_name);
}

#define __MYFILENAME__ __FILE__

/* The success status. The response writer returns it two or three times
 * per column, so it must not go through __RS_ERROR: that constructor
 * copies two std::strings and formats both with snprintf, which on macOS
 * takes a locale lock each time, and on a 511-column row those calls were
 * about a third of the writer's time (RONDB-1144 benchmarks). Nothing is
 * formatted here: zero-initialised, code fields as __RS_ERROR sets them
 * for a success, empty message and file name. */
inline RS_Status __RS_OK() {
  RS_Status ret{};
  ret.http_code = SUCCESS;
  ret.status = -1;
  ret.classification = -1;
  ret.code = -1;
  ret.mysql_code = -1;
  return ret;
}
#define RS_OK __RS_OK()

#define RS_CLIENT_ERROR(msg) __RS_ERROR(CLIENT_ERROR, \
                                        -1, -1, -1, -1, msg, __LINE__, __MYFILENAME__)

#define RS_CLIENT_404_ERROR()                                                                      \
  __RS_ERROR(NOT_FOUND, -1, -1, -1, -1, "Not Found", __LINE__, __MYFILENAME__)

#define RS_CLIENT_404_WITH_MSG_ERROR(msg)                                                          \
  __RS_ERROR(NOT_FOUND, -1, -1, -1, -1, msg, __LINE__, __MYFILENAME__)

#define RS_SERVER_ERROR(msg) __RS_ERROR(SERVER_ERROR, \
                                        -1, -1, -1, -1, msg, __LINE__, __MYFILENAME__)

#define RS_RONDB_SERVER_ERROR(ndberror, msg)                                                       \
  __RS_ERROR_RONDB(ndberror, msg, __LINE__, __MYFILENAME__)

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_STATUS_HPP_
