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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_PK_DATA_STRUCTS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_PK_DATA_STRUCTS_HPP_

#include "config_structs.hpp"
#include "rdrs_dal.h"
#include "src/constants.hpp"

#include <drogon/HttpTypes.h>
#include <string>
#include <vector>
#include <map>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <unordered_map>

std::string to_string(DataReturnType);

uint32_t decode_utf8_to_unicode(const std::string &, size_t &);

RS_Status validate_db_identifier(const std::string &);

RS_Status validate_operation_id(const std::string &);

class PKReadFilter {
 public:
  std::string column;
  std::vector<char> value;
  RS_Status validate();
};

class PKReadReadColumn {
 public:
  std::string column;
  std::string returnType;
};

class PKReadPath {
 public:
  PKReadPath();
  PKReadPath(const std::string &, const std::string &);
  std::string db;     // json:"db" uri:"db"  binding:"required,min=1,max=64"
  std::string table;  // Table *string `json:"table" uri:"table"  binding:"required,min=1,max=64"
};

class PKReadParams {
 public:
  PKReadParams();
  explicit PKReadParams(const std::string &);
  explicit PKReadParams(PKReadPath &);
  PKReadParams(const std::string &, PKReadPath &);
  PKReadParams(const std::string &, const std::string &, const std::string &);
  PKReadParams(const std::string &, const std::string &);
  std::string method;
  PKReadPath path;
  std::vector<PKReadFilter> filters;
  std::vector<PKReadReadColumn> readColumns;
  std::string operationId;
  std::string to_string();
  RS_Status validate();
};

struct Column {
  std::string name;
  std::vector<char> value;  // Byte array for the value
};

class PKReadResponse {
 public:
  PKReadResponse()                                                                = default;
  virtual void init()                                                             = 0;
  virtual void setOperationID(std::string &opID)                                  = 0;
  virtual void setColumnData(std::string &column, const std::vector<char> &value) = 0;
  virtual std::string to_string() const                                           = 0;

  virtual ~PKReadResponse() = default;
};

class PKReadResponseJSON : public PKReadResponse {
 private:
  drogon::HttpStatusCode code;
  std::string operationID;
  std::map<std::string, std::vector<char>> data;

 public:
  PKReadResponseJSON() : PKReadResponse() {
  }

  PKReadResponseJSON(const PKReadResponseJSON &other) : PKReadResponse() {
    code        = other.code;
    operationID = other.operationID;
    data        = other.data;
  }

  PKReadResponseJSON &operator=(const PKReadResponseJSON &other) {
    code        = other.code;
    operationID = other.operationID;
    data        = other.data;
    return *this;
  }

  void init() override {
    code = drogon::HttpStatusCode::kUnknown;
    operationID.clear();
    data.clear();
  }

  void setStatusCode(drogon::HttpStatusCode c) {
    code = c;
  }

  void setOperationID(std::string &opID) override {
    operationID = opID;
  }

  void setColumnData(std::string &column, const std::vector<char> &value) override {
    data[column] = value;
  }

  drogon::HttpStatusCode getStatusCode() const {
    return code;
  }

  std::string getOperationID() const {
    return operationID;
  }

  std::map<std::string, std::vector<char>> getData() const {
    return data;
  }

  std::string to_string() const override;

  std::string to_string(int, bool) const;

  static std::string batch_to_string(const std::vector<PKReadResponseJSON> &);
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_PK_DATA_STRUCTS_HPP_