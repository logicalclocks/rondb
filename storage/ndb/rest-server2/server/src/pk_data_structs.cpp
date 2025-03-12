/*
 * Copyright (c) 2023, 2025, Hopsworks and/or its affiliates.
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

#include "pk_data_structs.hpp"
#include "constants.hpp"
#include "config_structs.hpp"
#include "error_strings.h"
#include "rdrs_dal.hpp"
#include <util/require.h>
#include <libbase64.h>

#include <iostream>
#include <EventLogger.hpp>

extern EventLogger *g_eventLogger;

std::string to_string(DataReturnType drt) {
  switch (drt) {
  case DataReturnType::DEFAULT_DRT:
    return "default";
  default:
    return "unknown";
  }
}

Uint32 decode_utf8_to_unicode(const std::string_view &str, size_t &i) {
  Uint32 codepoint = 0;
  if ((str[i] & 0x80) == 0) {
    // 1-byte character
    codepoint = str[i];
  } else if ((str[i] & 0xE0) == 0xC0) {
    // 2-byte character
    codepoint = (str[i] & 0x1F) << 6 | (str[i + 1] & 0x3F);
    i += 1;
  } else if ((str[i] & 0xF0) == 0xE0) {
    // 3-byte character
    codepoint = (str[i] & 0x0F) << 12 | (str[i + 1] & 0x3F) << 6 |
                (str[i + 2] & 0x3F);
    i += 2;
  } else if ((str[i] & 0xF8) == 0xF0) {
    // 4-byte character
    codepoint = (str[i] & 0x07) << 18 | (str[i + 1] & 0x3F) << 12 |
                (str[i + 2] & 0x3F) << 6 | (str[i + 3] & 0x3F);
    i += 3;
  }
  return codepoint;
}

RS_Status validate_db_identifier(const std::string_view &identifier) {
  if (identifier.empty()) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
        ERROR_EMPTY_IDENTIFIER, std::string(rdrsErrorMessage(ERROR_EMPTY_IDENTIFIER))).status;
  }
  if (identifier.length() > 64) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
        ERROR_IDENTIFIER_TOO_LONG, (std::string(rdrsErrorMessage(ERROR_IDENTIFIER_TOO_LONG)) + 
          ": " + std::string(identifier)).c_str()).status;
  }

  // https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
  // ASCII: U+0001 .. U+007F
  // Extended: U+0080 .. U+FFFF
  for (size_t i = 0; i < identifier.length(); ++i) {
    Uint32 code = decode_utf8_to_unicode(identifier, i);

    if (code < 0x01 || code > 0xFFFF) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_INVALID_IDENTIFIER, (std::string(rdrsErrorMessage(ERROR_INVALID_IDENTIFIER)) + 
            ": " + std::to_string(code)).c_str()) .status;
    }
  }
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k200OK)).status;
}

RS_Status validate_db(const std::string_view db) {
  RS_Status status = validate_db_identifier(db);
  if (unlikely(status.http_code != static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k200OK))) {
    if (status.code == ERROR_EMPTY_IDENTIFIER) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_MIN_DB, (std::string(rdrsErrorMessage(ERROR_MIN_DB)) + 
            ": " + std::string(db)).c_str()).status;
    }
    if (status.code == ERROR_IDENTIFIER_TOO_LONG) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest), 
          ERROR_MAX_DB, (std::string(rdrsErrorMessage(ERROR_MAX_DB)) + 
            ": " + std::string(db)).c_str()).status;
    }
    if (status.code == ERROR_INVALID_IDENTIFIER) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_INVALID_IDENTIFIER,
          "database "+ std::string(rdrsErrorMessage(ERROR_INVALID_IDENTIFIER))).status;
      }
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
        ERROR_INVALID_DB_NAME,
        (std::string(rdrsErrorMessage(ERROR_INVALID_DB_NAME)) + 
         "; error: " + status.message).c_str()).status;
  }
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k200OK)).status;
}

RS_Status validate_table(const std::string_view table) {
  RS_Status status = validate_db_identifier(table);
  if (unlikely(status.http_code != static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k200OK))) {
    if (status.code == ERROR_EMPTY_IDENTIFIER)
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_MIN_TABLE, (std::string(rdrsErrorMessage(ERROR_MIN_TABLE)) + ": " +
          std::string(table)).c_str()).status;
    if (status.code == ERROR_IDENTIFIER_TOO_LONG)
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_MAX_TABLE, (std::string(rdrsErrorMessage(ERROR_MAX_TABLE)) + ": " +
          std::string(table)).c_str()).status;
    if (status.code == ERROR_INVALID_IDENTIFIER)
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_INVALID_IDENTIFIER,
          "tablename " + std::string(rdrsErrorMessage(ERROR_INVALID_IDENTIFIER))).status;
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
        ERROR_INVALID_TABLE_NAME,
        (std::string(rdrsErrorMessage(ERROR_INVALID_TABLE_NAME)) + "; error: " + status.message).c_str()).status;
  }
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k200OK)).status;
}

RS_Status validate_column(const std::string_view column) {
  // make sure column is valid
  RS_Status status = validate_db_identifier(column);
  if (unlikely(status.http_code != static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k200OK))) {
    if (status.code == ERROR_EMPTY_IDENTIFIER)
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_EMPTY_IDENTIFIER,
          (std::string(rdrsErrorMessage(ERROR_EMPTY_IDENTIFIER)) + 
           ": " + std::string(column)).c_str()).status;
    if (status.code == ERROR_IDENTIFIER_TOO_LONG)
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_IDENTIFIER_TOO_LONG,
          (std::string(rdrsErrorMessage(ERROR_IDENTIFIER_TOO_LONG)) + 
           ": " + std::string(column)).c_str()).status;
    if (status.code == ERROR_INVALID_IDENTIFIER)
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_INVALID_IDENTIFIER,
          "column name:" + std::string(rdrsErrorMessage(ERROR_INVALID_IDENTIFIER))).status;
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
        ERROR_INVALID_READ_COLUMN_NAME,
        (std::string(rdrsErrorMessage(ERROR_INVALID_READ_COLUMN_NAME)) + 
         "; error: " + status.message).c_str()).status;
  }
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k200OK)).status;
}

RS_Status validate_operation_id(const std::string &opId) {
  Uint32 operationIdMaxSize = AllConfigs::get_all().internal.operationIdMaxSize;
  if (static_cast<Uint32>(opId.length()) > operationIdMaxSize) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
        ERROR_MAX_IDENTIFIER_LENGTH,
       (std::string(rdrsErrorMessage(ERROR_MAX_IDENTIFIER_LENGTH)) + 
        " " + std::to_string(operationIdMaxSize)).c_str()).status;
  }
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k200OK)).status;
}

PKReadPath::PKReadPath() : db(), table() {
}

PKReadPath::PKReadPath(const std::string_view &db,
                       const std::string_view &table) : db(db), table(table) {
}

PKReadParams::PKReadParams() : path(),
                               filters(),
                               readColumns(),
                               operationId() {
}

PKReadParams::PKReadParams(PKReadPath &path)
    : path(path), filters(), readColumns(), operationId() {
}

PKReadParams::PKReadParams(const std::string_view &db,
                           const std::string_view &table)
    : path(db, table), filters(), readColumns(), operationId() {
}

std::string PKReadParams::to_string() {
  std::stringstream ss;
  ss << "PKReadParams:{path:{ db:" << path.db << ",table:" << path.table
     << "},filters:[";
  for (auto &filter : filters) {
    ss << "{column:" << filter.column;
    ss << ",value with each byte separately:";
    for (auto &byte : filter.value) {
      ss << byte << " ";
    }
    ss << "},";
  }
  ss << "],readColumns:[";
  for (auto &readColumn : readColumns) {
    ss << "{column:" << readColumn.column << "},";
  }
  ss << "],operationId:" << operationId << "}";
  return ss.str();
}

RS_Status PKReadParams::validate() {
  // std::cout << "Validating PKReadParams: " << to_string() << std::endl;

  RS_Status status = validate_db(path.db);
  if (unlikely(status.http_code != static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k200OK))) {
    return status;
  }
  status = validate_table(path.table);
  if (unlikely(status.http_code != static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k200OK))) {
    return status;
  }
  status = validate_operation_id(operationId);
  if (unlikely(status.http_code != static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k200OK)))
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
        ERROR_INVALID_OPERATION_ID,
        (std::string(rdrsErrorMessage(ERROR_INVALID_OPERATION_ID)) + 
         "; error: " + status.message).c_str()).status;

  // make sure read columns are valid
  for (auto &readColumn : readColumns) {
    status = validate_column(readColumn.column);
    if (unlikely(status.http_code != static_cast<HTTP_CODE>(
          drogon::HttpStatusCode::k200OK))) {
      return status;
    }
  }
  // make sure filter columns are valid
  for (auto &filter : filters) {
    status = validate_column(filter.column);
    if (unlikely(status.http_code != static_cast<HTTP_CODE>(
          drogon::HttpStatusCode::k200OK))) {
      return status;
    }
  }
  status = validate_columns();
  if (unlikely(status.http_code != static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k200OK))) {
    return status;
  }
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k200OK)).status;
}

RS_Status PKReadParams::validate_columns(void) {
  // make sure that the columns are unique
  std::unordered_map<std::string_view, bool> existingFilters;
  for (auto &filter : filters) {
    if (unlikely(existingFilters.find(filter.column) !=
        existingFilters.end())) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_UNIQUE_FILTER,
          (std::string(rdrsErrorMessage(ERROR_UNIQUE_FILTER)) + ": " +
          std::string(filter.column)).c_str()).status;
    }
    existingFilters[filter.column] = true;
  }
  // make sure that the filter columns and read columns do not overlap
  // and read cols are unique
  std::unordered_map<std::string_view, bool> existingCols;
  for (auto &readColumn : readColumns) {
    if (unlikely(existingFilters.find(readColumn.column) !=
        existingFilters.end())) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_INVALID_READ_COLUMNS,
          (std::string(rdrsErrorMessage(ERROR_INVALID_READ_COLUMNS)) + ". '" +
           std::string(readColumn.column) +
           "' already included in filter").c_str()).status;
    }
    if (unlikely(existingCols.find(readColumn.column) != existingCols.end())) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_UNIQUE_READ_COLUMN,
          (std::string(rdrsErrorMessage(ERROR_UNIQUE_READ_COLUMN)) + 
           ": " + std::string(readColumn.column)).c_str()).status;
    }
    existingCols[readColumn.column] = true;
  }
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k200OK)).status;
}

int PKReadResponseJSON::to_string_single(char *json_buf,
                                         ArenaMalloc *amalloc,
                                         size_t &ret_size) const {
  char *buf = json_buf;
  memcpy(buf, "{\"code\":", 8);
  buf += 8;

  char code_buf[10];
  int code_char_count = sprintf(&code_buf[0], "%d", code);
  char encode_buffer[MAX_TUPLE_SIZE_IN_BYTES_ENCODED];
  memcpy(buf, &code_buf[0], code_char_count);
  buf += code_char_count;
  memcpy(buf, ",\"operationId\":\"", 16);
  buf += 16;

  if (opIdPtr != nullptr) {
    memcpy(buf, opIdPtr, opIdLen);
    buf += opIdLen;
  }
  memcpy(buf, "\",\"data\":{", 10);
  buf += 10;

  for (Uint32 i = 0; i < num_values; i++) {
    if (i != 0) {
      *buf = ',';
      buf++;
    }
    *buf = '\n';
    buf++;
    *buf = '\"';
    buf++;
    ResultView res_view = result_view[i];
    memcpy(buf, res_view.name_ptr, res_view.name_len);
    buf += res_view.name_len;
    *buf = '\"';
    buf++;
    *buf = ':';
    buf++;
    if (res_view.quoted_flag) {
      *buf = '\"';
      buf++;
    }
    if (res_view.data_type == RDRS_BINARY_DATATYPE ||
        res_view.data_type == RDRS_BIT_DATATYPE) {
      size_t outlen = 0;
      char *encode_buf_ptr = &encode_buffer[0];
      if (res_view.value_len >= MAX_TUPLE_SIZE_IN_BYTES) {
        Uint32 value_len = res_view.value_len;
        Uint32 max_encoded_len = ((value_len * 4) / 3) + 3;
        encode_buf_ptr = (char*)amalloc->alloc_bytes(max_encoded_len, 8);
        if (unlikely(encode_buf_ptr == nullptr)) {
          return -1;
        }
      }
      base64_encode(res_view.value_ptr,
                    res_view.value_len,
                    (char *)encode_buf_ptr,
                    &outlen,
                    0);
      memcpy(buf, encode_buf_ptr, outlen);
      buf += outlen;
    } else {
      memcpy(buf, res_view.value_ptr, res_view.value_len);
      buf += res_view.value_len;
    }
    if (res_view.quoted_flag) {
      *buf = '\"';
      buf++;
    }
  }
  memcpy(buf, "\n}}", 3);
  buf += 3;
  *buf = '\0';
  size_t ret = (size_t)(buf - json_buf);
  ret_size = ret;
  return 0;
}

// Indent the JSON string by `indent` spaces.
char* PKReadResponseJSON::to_string_batch(char *buf,
                                          ArenaMalloc *amalloc) const {

  memcpy(buf, "{\"code\":", 8);
  buf += 8;

  char code_buf[10];
  int code_char_count = sprintf(&code_buf[0], "%d", code);
  char encode_buffer[MAX_TUPLE_SIZE_IN_BYTES_ENCODED];
  memcpy(buf, &code_buf[0], code_char_count);
  buf += code_char_count;

  memcpy(buf, ",\"body\":{\"operationId\":\"", 24);
  buf += 24;
  if (opIdPtr != nullptr) {
    memcpy(buf, opIdPtr, opIdLen);
    buf += opIdLen;
  }
  memcpy(buf, "\",\"data\":{", 10);
  buf += 10;
  if (code == drogon::HttpStatusCode::k200OK) {
    for (Uint32 i = 0; i < num_values; i++) {
      if (i != 0) {
        *buf = ',';
        buf++;
      }
      ResultView res_view = result_view[i];
      *buf = '\n';
      buf++;
      *buf = '\"';
      buf++;
      memcpy(buf, res_view.name_ptr, res_view.name_len);
      buf += res_view.name_len;
      *buf = '\"';
      buf++;
      *buf = ':';
      buf++;
      if (res_view.quoted_flag) {
        *buf = '\"';
        buf++;
      }
      if (res_view.data_type == RDRS_BINARY_DATATYPE ||
          res_view.data_type == RDRS_BIT_DATATYPE) {
        size_t outlen = 0;
        char *encode_buf_ptr = &encode_buffer[0];
        if (unlikely(res_view.value_len >= MAX_TUPLE_SIZE_IN_BYTES)) {
          Uint32 value_len = res_view.value_len;
          Uint32 max_encoded_len = ((value_len * 4) / 3) + 3;
          encode_buf_ptr = (char*)amalloc->alloc_bytes(max_encoded_len, 8);
          if (unlikely(encode_buf_ptr == nullptr)) {
            return nullptr;
          }
        }
        base64_encode(res_view.value_ptr,
                      res_view.value_len,
                      (char *)encode_buf_ptr,
                      &outlen,
                      0);
        memcpy(buf, encode_buf_ptr, outlen);
        buf += outlen;
      } else {
        memcpy(buf, res_view.value_ptr, res_view.value_len);
        buf += res_view.value_len;
      }
      if (res_view.quoted_flag) {
        *buf = '\"';
        buf++;
      }
    }
  }
  memcpy(buf, "\n}}}", 4);
  buf += 4;
  return buf;
}

int PKReadResponseJSON::batch_to_string(
  const std::vector<PKReadResponseJSON> &responses,
  char *json_buf,
  ArenaMalloc *amalloc,
  size_t &ret_size) {

  char *buf = json_buf; 
  memcpy(buf, "{\"result\":[", 11);
  buf+= 11;
  bool first = true;
  for (auto &response : responses) {
    if (!first) {
      *buf = ',';
      buf++;
    }
    first = false;
    *buf = '\n';
    buf++;
    buf = response.to_string_batch(buf, amalloc);
    if (unlikely(buf == nullptr)) {
      return -1;
    }
  }
  memcpy(buf, "\n]}\n", 4);
  buf += 4;
  *buf = '\0';
  ret_size = (size_t)(buf - json_buf);
  return 0;
}
