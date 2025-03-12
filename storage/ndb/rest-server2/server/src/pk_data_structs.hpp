/*
 * Copyright (C) 2023, 2025 Hopsworks AB
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
#include "rdrs_const.h"
#include <ndb_types.h>

#include <drogon/HttpTypes.h>
#include <string>
#include <vector>
#include <EventLogger.hpp>
#include <ArenaMalloc.hpp>
#include <libbase64.h>

std::string to_string(DataReturnType);
Uint32 decode_utf8_to_unicode(const std::string_view &, size_t &);
RS_Status validate_db_identifier(const std::string_view &);
RS_Status validate_operation_id(const std::string &);
RS_Status validate_db(const std::string_view);
RS_Status validate_table(const std::string_view);
RS_Status validate_column(const std::string_view);

class PKReadFilter {
 public:
  //std::string_view column;
  std::string_view column;
  std::vector<char> value;
};

class PKReadReadColumn {
 public:
  std::string_view column;
};

class PKReadPath {
 public:
  PKReadPath();
  PKReadPath(const std::string_view &, const std::string_view &);
  // json:"db" uri:"db"  binding:"required,min=1,max=64"
  std::string_view db;
  // Table *string `json:"table" uri:"table"  binding:"required,min=1,max=64"
  std::string table;
};

class PKReadParams {
 public:
  PKReadParams();
  explicit PKReadParams(const std::string_view &);
  explicit PKReadParams(PKReadPath &);
  PKReadParams(const std::string_view &, const std::string_view &);
  PKReadPath path;
  std::vector<PKReadFilter> filters;
  std::vector<PKReadReadColumn> readColumns;
  std::string operationId;
  std::string to_string();
  RS_Status validate();
  RS_Status validate_columns();
};

struct Column {
  std::string_view name;
  std::vector<char> value;  // Byte array for the value
};

struct ResultView {
  const char *name_ptr;
  const char *value_ptr;
  Uint32 name_len;
  Uint32 value_len;
  Uint32 data_type;
  bool quoted_flag;
};

class PKReadResponse {
 public:
  PKReadResponse()= default;
  virtual void init(Uint32, ResultView*) = 0;
  virtual void setOperationID(const char*, Uint32) = 0;
  virtual void setOperationID(std::string_view) = 0;
  virtual void setColumnData(Uint32 index,
                             const char *name,
                             Uint32 name_len,
                             const char *value,
                             Uint32 value_len,
                             bool quoted_flag,
                             Uint32 data_type) = 0;
  virtual ~PKReadResponse() = default;
};

class PKReadResponseJSON : public PKReadResponse {
 private:
  // json:"code"    form:"code"    binding:"required"
  drogon::HttpStatusCode code;
  // json:"operationId" form:"operation-id" binding:"omitempty"
  const char *opIdPtr;
  Uint32 opIdLen;
  Uint32 num_values;
  // json:"data" form:"data" binding:"omitempty"
  ResultView *result_view;
  size_t size_json;

 public:
  PKReadResponseJSON() : PKReadResponse() {
  }

  PKReadResponseJSON(const PKReadResponseJSON &other) : PKReadResponse() {
    code = other.code;
    num_values = other.num_values;
    opIdPtr = other.opIdPtr;
    opIdLen = other.opIdLen;
    result_view = other.result_view;
    size_json = other.size_json;
  }

  PKReadResponseJSON &operator=(const PKReadResponseJSON &other) {
    code = other.code;
    num_values = other.num_values;
    opIdPtr = other.opIdPtr;
    opIdLen = other.opIdLen;
    result_view = other.result_view;
    size_json = other.size_json;
    return *this;
  }

  void init(Uint32 numColumns,
            ResultView *in_result_view) override {
    code = drogon::HttpStatusCode::kUnknown;
    opIdPtr = nullptr;
    opIdLen = 0;
    num_values = numColumns;
    result_view = in_result_view;
    /**
     * First and last part + security
     */
    size_json = 55;
  }

  void setStatusCode(drogon::HttpStatusCode c) {
    code = c;
  }

  void setOperationID(const char *opId, Uint32 len) override {
    opIdPtr = opId;
    opIdLen = len;
    size_json += len;
  }

  void setOperationID(std::string_view str_view) override {
    opIdPtr = str_view.data();
    opIdLen = str_view.size();
    size_json += str_view.size();
  }

  void setColumnData(Uint32 index,
                     const char *name,
                     Uint32 name_len,
                     const char *value,
                     Uint32 value_len,
                     bool quoted,
                     Uint32 data_type) override {
    result_view[index].name_ptr = name;
    result_view[index].name_len = name_len;
    result_view[index].value_ptr = value;
    result_view[index].value_len = value_len;
    result_view[index].quoted_flag = quoted;
    result_view[index].data_type = data_type;
    Uint32 encoded_value_len = value_len;
    if (data_type == RDRS_BINARY_DATATYPE ||
        data_type == RDRS_BIT_DATATYPE) {
      encoded_value_len *= 4;
      encoded_value_len /= 3;
      encoded_value_len += 2;
    }
    // 8 includes quoting for value
    size_json += (8 + name_len + encoded_value_len);
  }

  drogon::HttpStatusCode getStatusCode() const {
    return code;
  }

  std::string_view getOperationID() const {
    std::string_view opId(opIdPtr, opIdLen);
    return opId;
  }

  std::string_view getName(Uint32 index) const {
    std::string_view name(result_view[index].name_ptr,
                          result_view[index].name_len);
    return name;
  }

  std::string getOperationIdString() const {
    std::string opId(opIdPtr, opIdLen);
    return opId;
  }

  Uint32 getNumValues() const {
    return num_values;
  }

  std::string getNameString(Uint32 index) const {
    std::string name(result_view[index].name_ptr,
                     result_view[index].name_len);
    return name;
  }

  std::string getValueString(Uint32 index) const {
    std::string value(result_view[index].value_ptr,
                      result_view[index].value_len);
    return value;
  }

  std::vector<char> getValueArray(Uint32 index) {
    const char *value_ptr = result_view[index].value_ptr;
    Uint32 value_len = result_view[index].value_len;
    bool quoted_flag = result_view[index].quoted_flag;
    if (result_view[index].data_type == RDRS_BINARY_DATATYPE ||
        result_view[index].data_type == RDRS_BIT_DATATYPE) {
      Uint32 encoded_len = ((value_len * 4) / 3) + 3 + 2;
      std::vector<char> vec(encoded_len);
      char* encode_ptr = &vec[1];
      assert(quoted_flag);
      size_t encode_len = 0;
      base64_encode(value_ptr, value_len, encode_ptr, &encode_len, 0);
      encode_len += 2;
      vec[0] = '\"';
      vec[encode_len - 1] = '\"';
      vec.resize(encode_len);
      return vec;
    } else if (quoted_flag) {
      std::vector<char> vec(value_len + 2);
      vec[0] = '\"';
      vec[value_len + 1] = '\"';
      std::copy(value_ptr, value_ptr + value_len, vec.begin() + 1);
      return vec;
    } else {
      std::vector<char> vec(value_ptr, value_ptr + value_len);
      return vec;
    }
  }

  std::vector<Uint8> getComplexValue(Uint32 index) {
    assert(result_view[index].quoted_flag);
    const char *ptr = result_view[index].value_ptr;
    Uint32 len = result_view[index].value_len;
    std::vector<Uint8> vec(ptr, ptr + len);
    return vec;
  }

  std::string_view getValue(Uint32 index) const {
    std::string_view value(result_view[index].value_ptr,
                           result_view[index].value_len);
    return value;
  }

  void addSizeJsonMessage() {
#ifdef VM_TRACE
    size_json += 20;
#else
    size_json += 8;
#endif
  }
  size_t getSizeJson() const { return size_json; }

  int to_string_single(char*,
                       ArenaMalloc *amalloc,
                       size_t & size_json) const;
  char* to_string_batch(char*, ArenaMalloc *amalloc) const;
  static int batch_to_string(const std::vector<PKReadResponseJSON> &,
                             char*,
                             ArenaMalloc *amalloc,
                             size_t & size_json);
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
    body = other.body;
  }

  PKReadResponseWithCodeJSON &operator=(
    const PKReadResponseWithCodeJSON &other) {
    message = other.message;
    body = other.body;
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

  void setOperationId(const char *opId, Uint32 opIdLen) {
    body.setOperationID(opId, opIdLen);
  }

  void setOperationId(std::string_view str_view) {
    body.setOperationID(str_view);
  }

  std::string getMessage() const {
    return message;
  }

  PKReadResponseJSON getBody() const {
    return body;
  }
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
};
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_PK_DATA_STRUCTS_HPP_
