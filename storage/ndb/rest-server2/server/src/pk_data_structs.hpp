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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_PK_DATA_STRUCTS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_PK_DATA_STRUCTS_HPP_

#include "rdrs_dal.h"
#include <ndb_types.h>

#include <drogon/HttpTypes.h>
#include <string>
#include <vector>
#include <map>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <unordered_map>

std::string to_string(DataReturnType);

Uint32 decode_utf8_to_unicode(const std::string &, size_t &);

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
  // json:"db" uri:"db"  binding:"required,min=1,max=64"
  std::string db;
  // Table *string `json:"table" uri:"table"  binding:"required,min=1,max=64"
  std::string table;
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
  RS_Status validate(bool, bool);
};

struct Column {
  std::string name;
  std::vector<char> value;  // Byte array for the value
};

class PKReadResponse {
 public:
  PKReadResponse()= default;
  virtual void init() = 0;
  virtual void setOperationID(std::string &opID)= 0;
  virtual void setColumnData(std::string &column,
                             const std::vector<char> &value) = 0;
  virtual std::string to_string() const = 0;
  virtual ~PKReadResponse() = default;
};

class PKReadResponseJSON : public PKReadResponse {
 private:
  // json:"code"    form:"code"    binding:"required"
  drogon::HttpStatusCode code;
  // json:"operationId" form:"operation-id" binding:"omitempty"
  std::string operationID;
  // json:"data" form:"data" binding:"omitempty"
  std::map<std::string, std::vector<char>>
      data;

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

  void setColumnData(std::string &column,
                     const std::vector<char> &value) override {
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

class PKReadResponseWithCodeJSON {
 private:
  // json:"message"    form:"message"    binding:"required"
  std::string message;
  // json:"body"    form:"body"    binding:"required"
  PKReadResponseJSON body;

 public:
  PKReadResponseWithCodeJSON() = default;

  PKReadResponseWithCodeJSON(const PKReadResponseWithCodeJSON &other) {
    message = other.message;
    body    = other.body;
  }

  PKReadResponseWithCodeJSON &operator=(
    const PKReadResponseWithCodeJSON &other) {
    message = other.message;
    body    = other.body;
    return *this;
  }

  void setMessage(std::string &msg) {
    message = msg;
  }

  void setMessage(const char *msg) {
    message = msg;
  }

  void setBody(const PKReadResponseJSON &b) {
    body = b;
  }

  void setOperationId(std::string &opID) {
    body.setOperationID(opID);
  }

  std::string getMessage() const {
    return message;
  }

  PKReadResponseJSON getBody() const {
    return body;
  }

  std::string to_string() const;
};

class BatchResponseJSON {
 private:
  // json:"result" binding:"required"
  std::vector<PKReadResponseWithCodeJSON> result;

 public:
  BatchResponseJSON() = default;

  BatchResponseJSON(const BatchResponseJSON &other) : result(other.result) {
  }

  BatchResponseJSON &operator=(const BatchResponseJSON &other) {
    if (this != &other) {
      result = other.result;
    }
    return *this;
  }

  void setResult(const std::vector<PKReadResponseWithCodeJSON> &res) {
    result = res;
  }

  std::vector<PKReadResponseWithCodeJSON> getResult() const {
    return result;
  }

  void Init(int numSubResponses) {
    result.resize(numSubResponses);
  }

  static PKReadResponseWithCodeJSON CreateNewSubResponse() {
    PKReadResponseWithCodeJSON subResponse;
    return subResponse;
  }

  void AddSubResponse(unsigned long index,
                      const PKReadResponseWithCodeJSON &subResp) {
    if (index < result.size()) {
      result[index] = subResp;
    }
  }

  std::string to_string() const {
    std::string res = "[";
    for (size_t i = 0; i < result.size(); i++) {
      res += result[i].to_string();
      if (i < result.size() - 1) {
        res += ",";
      }
    }
    res += "]";
    return res;
  }
};
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_PK_DATA_STRUCTS_HPP_
