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

#include "pk_data_structs.hpp"
#include "constants.hpp"
#include "config_structs.hpp"
#include "error_strings.h"
#include "rdrs_dal.hpp"

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
        ERROR_CODE_EMPTY_IDENTIFIER, ERROR_038).status;
  }
  if (identifier.length() > 64) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
        ERROR_CODE_IDENTIFIER_TOO_LONG,
        (std::string(ERROR_039) + ": " +
        std::string(identifier)).c_str()).status;
  }

  // https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
  // ASCII: U+0001 .. U+007F
  // Extended: U+0080 .. U+FFFF
  for (size_t i = 0; i < identifier.length(); ++i) {
    Uint32 code = decode_utf8_to_unicode(identifier, i);

    if (code < 0x01 || code > 0xFFFF) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_CODE_INVALID_IDENTIFIER,
          (std::string(ERROR_040) + ": " + std::to_string(code)).c_str())
          .status;
    }
  }
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k200OK)).status;
}

RS_Status validate_db(const std::string_view db) {
  RS_Status status = validate_db_identifier(db);
  if (unlikely(status.http_code != static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k200OK))) {
    if (status.code == ERROR_CODE_EMPTY_IDENTIFIER)
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_CODE_EMPTY_IDENTIFIER,
          (std::string(ERROR_049) + ": " +
          std::string(db)).c_str()).status;
    if (status.code == ERROR_CODE_IDENTIFIER_TOO_LONG)
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_CODE_MAX_DB, (std::string(ERROR_050) + ": " +
          std::string(db)).c_str()).status;
    if (status.code == ERROR_CODE_INVALID_IDENTIFIER)
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_CODE_INVALID_IDENTIFIER,
          "database name contains invalid characters").status;
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
        ERROR_CODE_INVALID_DB_NAME,
        (std::string(ERROR_051) + "; error: " + status.message).c_str()).status;
  }
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k200OK)).status;
}

RS_Status validate_table(const std::string_view table) {
  RS_Status status = validate_db_identifier(table);
  if (unlikely(status.http_code != static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k200OK))) {
    if (status.code == ERROR_CODE_EMPTY_IDENTIFIER)
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_CODE_MIN_TABLE, (std::string(ERROR_052) + ": " +
          std::string(table)).c_str()).status;
    if (status.code == ERROR_CODE_IDENTIFIER_TOO_LONG)
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_CODE_MAX_TABLE, (std::string(ERROR_053) + ": " +
          std::string(table)).c_str()).status;
    if (status.code == ERROR_CODE_INVALID_IDENTIFIER)
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_CODE_INVALID_IDENTIFIER,
          "table name contains invalid characters").status;
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
        ERROR_CODE_INVALID_TABLE_NAME,
        (std::string(ERROR_054) + "; error: " + status.message).c_str()).status;
  }
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k200OK)).status;
}

RS_Status validate_column(const std::string_view column) {
  // make sure column is valid
  RS_Status status = validate_db_identifier(column);
  if (unlikely(status.http_code != static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k200OK))) {
    if (status.code == ERROR_CODE_EMPTY_IDENTIFIER)
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_CODE_EMPTY_IDENTIFIER,
          (std::string(ERROR_038) + ": " +
           std::string(column)).c_str()).status;
    if (status.code == ERROR_CODE_IDENTIFIER_TOO_LONG)
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_CODE_IDENTIFIER_TOO_LONG,
          (std::string(ERROR_039) + ": " +
           std::string(column)).c_str()).status;
    if (status.code == ERROR_CODE_INVALID_IDENTIFIER)
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_CODE_INVALID_IDENTIFIER,
          "column name: contains invalid characters").status;
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
        ERROR_CODE_INVALID_READ_COLUMN_NAME,
        (std::string(ERROR_059) + "; error: " +
         status.message).c_str()).status;
  }
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k200OK)).status;
}

RS_Status validate_operation_id(const std::string &opId) {
  Uint32 operationIdMaxSize = AllConfigs::get_all().internal.operationIdMaxSize;
  if (static_cast<Uint32>(opId.length()) > operationIdMaxSize) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
        ERROR_CODE_INVALID_IDENTIFIER_LENGTH,
       (std::string(ERROR_041) + " " +
        std::to_string(operationIdMaxSize)).c_str()).status;
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
  ss << "PKReadParams: { path: { db: " << path.db << ", table: " << path.table
     << " }, filters: [";
  for (auto &filter : filters) {
    ss << "{ column: " << filter.column;
    ss << ", value with each byte separately: ";
    for (auto &byte : filter.value) {
      ss << byte << " ";
    }
    ss << "}, ";
  }
  ss << "], readColumns: [";
  for (auto &readColumn : readColumns) {
    ss << "{ column: " << readColumn.column << " }, ";
  }
  ss << "], operationId: " << operationId << " }";
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
        ERROR_CODE_INVALID_OPERATION_ID,
        (std::string(ERROR_055) + "; error: " + status.message).c_str()).status;

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
          ERROR_CODE_UNIQUE_FILTER,
          (std::string(ERROR_057) + ": " +
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
          ERROR_CODE_INVALID_READ_COLUMNS,
          (std::string(ERROR_060) + ". '" +
           std::string(readColumn.column) +
           "' already included in filter").c_str()).status;
    }
    if (unlikely(existingCols.find(readColumn.column) != existingCols.end())) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
          ERROR_CODE_UNIQUE_READ_COLUMN,
          (std::string(ERROR_061) + ": " +
          std::string(readColumn.column)).c_str()).status;
    }
    existingCols[readColumn.column] = true;
  }
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k200OK)).status;
}

std::string PKReadResponseJSON::to_string() const {
  std::stringstream ss;
  ss << "{" << std::endl;
  ss << "  \"code\": " << static_cast<int>(code) << "," << std::endl;
  ss << "  \"operationId\": \"" << operationID << "\"," << std::endl;
  ss << "  \"data\": {";
  bool first = true;
  for (auto &[column, value] : data) {
    if (!first) {
      ss << ",";
    }
    first = false;
    ss << std::endl;
    ss << "    \"" << column << "\": ";
    if (value.empty()) {
      ss << "null";
    } else {
      ss << std::string(value.begin(), value.end());
    }
  }
  ss << std::endl << "  }" << std::endl;
  ss << "}" << std::endl;
  return ss.str();
}

// Indent the JSON string by `indent` spaces.
std::string PKReadResponseJSON::to_string(int indent, bool batch) const {
  std::stringstream ss;
  std::string indentStr(indent, ' ');
  std::string innerIndentStr     = indentStr + std::string(2, ' ');
  std::string innerMostIndentStr = innerIndentStr + std::string(2, ' ');

  if (batch) {
    ss << indentStr << "{" << std::endl;
    ss << innerIndentStr << "\"code\": " << static_cast<int>(code)
       << "," << std::endl;
    ss << innerIndentStr << "\"body\": {" << std::endl;
  } else {
    ss << indentStr << "{" << std::endl;
  }

  ss << (batch ? innerMostIndentStr : innerIndentStr) << "\"operationId\": \""
     << operationID << "\"," << std::endl;
  ss << (batch ? innerMostIndentStr : innerIndentStr) << "\"data\": {";

  bool first = true;
  for (auto &[column, value] : data) {
    if (!first) {
      ss << ",";
    }
    first = false;
    ss << std::endl;
    ss << (batch ? innerMostIndentStr + "  " : innerMostIndentStr)
       << "\"" << column << "\": ";
    if (value.empty()) {
      ss << "null";
    } else {
      ss << std::string(value.begin(), value.end());
    }
  }

  ss << std::endl << (batch ? innerMostIndentStr : innerIndentStr) << "}";
  ss << std::endl << (batch ? innerIndentStr : indentStr) << "}";

  if (batch) {
    ss << std::endl << indentStr << "}";
  }

  return ss.str();
}

std::string PKReadResponseJSON::batch_to_string(
  const std::vector<PKReadResponseJSON> &responses) {
  std::stringstream ss;
  ss << "{" << std::endl;
  ss << "  \"result\": [";
  bool first = true;
  for (auto &response : responses) {
    if (!first) {
      ss << ",";
    }
    first = false;
    ss << std::endl;
    ss << response.to_string(4, true);
  }
  ss << std::endl << "  ]" << std::endl;
  ss << "}" << std::endl;
  return ss.str();
}

std::string PKReadResponseWithCodeJSON::to_string() const {
  std::stringstream ss;
  ss << "{" << std::endl;
  ss << "  \"message\": \"" << message << "\"," << std::endl;
  ss << "  \"body\": " << body.to_string() << std::endl;
  ss << "}" << std::endl;
  return ss.str();
}
