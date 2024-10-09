/*
 * Copyright (C) 2024 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_DAL_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_DAL_HPP_

#include "rdrs_dal.h"
#include "rdrs_dal.hpp"

#include <cstring>
#include <string>
#include <iostream>

class CRS_Status {
 public:
  RS_Status status{};

  CRS_Status() {
    initialize(HTTP_CODE::SUCCESS, 0, 0, 0, 0, "", 0, "");
  }

  static CRS_Status SUCCESS;

  explicit CRS_Status(HTTP_CODE http_code) {
    initialize(http_code, 0, 0, 0, 0, "", 0, "");
  }

  CRS_Status(HTTP_CODE http_code, const char *message) {
    initialize(http_code, 0, 0, 0, 0, message, 0, "");
  }

  CRS_Status(HTTP_CODE http_code,
             const char *message,
             const std::string &error_file_name) {
    initialize(http_code, 0, 0, 0, 0, message, 0, error_file_name.c_str());
  }

  CRS_Status(HTTP_CODE http_code, const std::string &message) {
    initialize(http_code, 0, 0, 0, 0, message.c_str(), 0, "");
  }

  CRS_Status(HTTP_CODE http_code, int error_code, const char *message) {
    initialize(http_code, 0, 0, error_code, 0, message, 0, "");
  }

  CRS_Status(HTTP_CODE http_code, int error_code, const std::string &message) {
    initialize(http_code, 0, 0, error_code, 0, message.c_str(), 0, "");
  }

  CRS_Status(HTTP_CODE http_code, const char *error, const char *location) {
    std::string msg =
        "Parsing request failed. Error: " + std::string(error) +
        " at " + std::string(location);
    initialize(http_code, 0, 0, 0, 0, msg.c_str(), 0, "");
  }

  void set(HTTP_CODE http_code, const char *message) {
    initialize(http_code, 0, 0, 0, 0, message, 0, "");
  }

 private:
  void initialize(HTTP_CODE http_code,
                  int status,
                  int classification,
                  int code,
                  int mysql_code,
                  const char *message,
                  int err_line_no,
                  const char *err_file_name) {
    this->status.http_code      = http_code;
    this->status.status         = status;
    this->status.classification = classification;
    this->status.code           = code;
    this->status.mysql_code     = mysql_code;
    strncpy(this->status.message, message, RS_STATUS_MSG_LEN - 1);
    this->status.message[RS_STATUS_MSG_LEN - 1] = '\0';
    this->status.err_line_no                    = err_line_no;
    strncpy(this->status.err_file_name,
            err_file_name,
            RS_STATUS_FILE_NAME_LEN - 1);
    this->status.err_file_name[RS_STATUS_FILE_NAME_LEN - 1] = '\0';
  }
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_DAL_HPP_
