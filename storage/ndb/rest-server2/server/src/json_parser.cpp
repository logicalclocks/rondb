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

#include "json_parser.hpp"
#include "feature_store_data_structs.hpp"
#include "pk_data_structs.hpp"
#include "constants.hpp"
#include "error_strings.h"
#include "config_structs.hpp"
#include "rdrs_dal.hpp"
#include <my_compiler.h>

#include <cstdint>
#include <cstddef>
#include <functional>
#include <memory>
#include <simdjson.h>
#include <EventLogger.hpp>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_REL_URL 1
#endif

#ifdef DEBUG_REL_URL
#define DEB_REL_URL(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_REL_URL(...) do { } while (0)
#endif

/*
 * Parsing utilities
 */

class ConfigParseError : public std::runtime_error {
public:
  ConfigParseError(std::string message) :
    std::runtime_error("ConfigParseError"), m_error_message(message) {}
  std::string m_error_message;
};

// Given datatypes for target and value and a function body, define parser
// functions for both lvalue and rvalue references for value.
//
// It is necessary to define two different functions; even though
// `const ValueDatatype&` can bind to both lvalue and rvalue, we cannot use a
// const parameter since parsing can change the object (progressing the point in
// the document).
//
// They are declared static (local to this compilation unit) in order to let the
// compiler prune unused ones more easily.
#define DEFINE_PARSER(TargetDatatype, ValueDatatype, ...) \
  [[maybe_unused]] static inline bool parse(TargetDatatype& target, \
                                            ValueDatatype& value) __VA_ARGS__ \
  [[maybe_unused]] static inline bool parse(TargetDatatype& target, \
                                            ValueDatatype&& value) __VA_ARGS__

// Usually, the value will be a simdjson value.
#define DEFINE_VALUE_PARSER(Datatype, ...) \
  DEFINE_PARSER(Datatype, \
                simdjson::ondemand::value, \
                __VA_ARGS__)

// Use simdjson built-in parsers. Return true on success. On failure due to null
// value, return false. (All other failures will result in an exception.)
#define USE_SIMDJSON_PARSER(Datatype) \
  DEFINE_VALUE_PARSER(Datatype, { \
    if (value.is_null()) \
      return false; \
    target = Datatype(value); \
    return true; \
  })

// Define a parser for std::vector of the given element type. Return true on
// success. On failure due to top-level null, return false. All other failures
// will result in an exception - null is acceptable, but [ null ] is not.
#define DEFINE_ARRAY_PARSER(ElementType) \
  DEFINE_VALUE_PARSER(std::vector<ElementType>, { \
    simdjson::ondemand::array array; \
    if (parse(array, value)) { \
      target.clear(); \
      for (simdjson::ondemand::value elementJson : array) { \
        ElementType element; \
        if(!parse(element, elementJson)) \
          throw ConfigParseError("Ill-formed array element"); \
        target.push_back(element); \
      } \
      return true; \
    } \
    return false; \
  })

// Define a parser for a struct. All elements will be optional. Will only accept
// keys that matches an element or begin with "#". Keys beginning with "#" are
// intended to be used for comments. All other keys will cause an exception.
#define DEFINE_STRUCT_PARSER(Datatype, ...) \
  DEFINE_PARSER(Datatype, simdjson::ondemand::object, { \
    for(simdjson::ondemand::field field : value) { \
      std::string_view fkey = field.unescaped_key(false); \
      simdjson::ondemand::value fval = field.value(); \
      __VA_ARGS__ \
      if (fkey[0] != '#') \
        throw ConfigParseError("Unexpected key"); \
    } \
    return true; \
  }) \
  DEFINE_VALUE_PARSER(Datatype, { \
    simdjson::ondemand::object obj; \
    if (parse(obj, value)) { \
      return parse(target, obj); \
    } \
    return false; \
  })
#define ELEMENT(TargetVar, SourceKey) \
  if (fkey == #SourceKey) { \
    parse(target.TargetVar, fval); \
    continue; \
  }

void assert_end_of_doc(simdjson::ondemand::document& doc) {
  switch(doc.current_location().error()) {
  case simdjson::error_code::OUT_OF_BOUNDS:
    // This is what we expect, after just having parsed one object from the
    // buffer.
    break;
  case simdjson::error_code::SUCCESS:
    throw ConfigParseError("Unexpected data after end of root-level object");
  default:
    // Should not happen
    throw ConfigParseError("Unexpected location error");
  }
}

RS_Status handle_parse_error(ConfigParseError& e,
                             const simdjson::padded_string_view& paddedJson,
                             const simdjson::ondemand::document* doc) {
  std::string message = e.m_error_message;
  const char *location = nullptr;
  if (doc != nullptr) {
    simdjson::error_code getLocationError = doc->current_location().get(location);
    if (getLocationError == simdjson::SUCCESS) {
      const char* bufferC = paddedJson.data();
#ifdef VM_TRACE
      const size_t bufferLength = paddedJson.length();
#endif
      assert(&bufferC[0] <= location && location < &bufferC[bufferLength]);
      int line = 1;
      int column = 0;
      for (const char* c = &bufferC[0]; c < location; c++) {
        if (*c == '\n') {
          line++;
          column = 0;
        } else {
          column++;
        }
      }
      message += " before/at line " + std::to_string(line) +
        ", column " + std::to_string(column);
    }
  }
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k400BadRequest),
    message).status;
}

/*
 * End of parsing utilities
 */

JSONParser* jsonParsers = nullptr;

JSONParser::JSONParser() {
  buffer = std::make_unique<char[]>(
    globalConfigs.internal.maxReqSize + simdjson::SIMDJSON_PADDING);
}

std::unique_ptr<char[]> &JSONParser::get_buffer() {
  return buffer;
}

RS_Status extract_db_and_table(const std::string_view &,
                               std::string_view &,
                               std::string&,
                               const std::string& expected_operation);
RS_Status handle_simdjson_error(const simdjson::error_code &,
                                simdjson::ondemand::document &,
                                const char *&);

// Is used to perform a primary key read operation.
RS_Status JSONParser::pk_parse(simdjson::padded_string_view reqBody,
                               PKReadParams &reqStruct) {
  const char *currentLocation = nullptr;

  simdjson::error_code error = parser.iterate(reqBody).get(doc);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }

  simdjson::ondemand::object reqObject;
  error = doc.get_object().get(reqObject);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }

  simdjson::ondemand::array filters;
  auto filtersVal = reqObject[FILTERS];
  if (unlikely(filtersVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(filtersVal.error(), doc, currentLocation);
  }
  if (unlikely(filtersVal.is_null())) {
    return CRS_Status(
      HTTP_CODE::CLIENT_ERROR, "the Field section is null").status;
  }
  error = filtersVal.get(filters);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }
  if (unlikely(filters.is_empty())) {
    return CRS_Status(
      HTTP_CODE::CLIENT_ERROR, "the Field section is empty").status;
  }
  for (auto filter : filters) {
    PKReadFilter pkReadFilter;
    simdjson::ondemand::object filterObj;
    error = filter.get(filterObj);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(error, doc, currentLocation);
    }
    std::string_view column;
    auto columnVal = filterObj[COLUMN];
    if (unlikely(columnVal.error() != simdjson::SUCCESS)) {
      return handle_simdjson_error(columnVal.error(), doc, currentLocation);
    } else if (unlikely(columnVal.is_null())) {
      return CRS_Status(
        HTTP_CODE::CLIENT_ERROR,
        "a Column name in the Field section is null").status;
    }
    error = columnVal.get(column);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(error, doc, currentLocation);
    }
    if (unlikely(column.size() == 0)) {
      return CRS_Status(
        HTTP_CODE::CLIENT_ERROR,
        "a Column name in the Field section is empty").status;
    }
    pkReadFilter.column = column;

    simdjson::ondemand::value value;
    std::vector<char> bytes;
    auto valueVal = filterObj[VALUE];
    error = valueVal.get(value);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(columnVal.error(), doc, currentLocation);
    } else if (unlikely(valueVal.is_null())) {
      return CRS_Status(
        HTTP_CODE::CLIENT_ERROR,
        "a Column in the Field section is null").status;
    }
    std::ostringstream oss;
    oss << value;
    std::string valueJson = oss.str();
    bytes = std::vector<char>(valueJson.begin(), valueJson.end());
    pkReadFilter.value = bytes;
    reqStruct.filters.emplace_back(pkReadFilter);
  }

  simdjson::ondemand::array readColumns;
  auto readColumnsVal = reqObject[READCOLUMNS];
  if (unlikely(readColumnsVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
    readColumns = {};
  } else if (unlikely(readColumnsVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(readColumnsVal.error(), doc, currentLocation);
  } else if (unlikely(readColumnsVal.is_null())) {
    readColumns = {};
  } else {
    error = readColumnsVal.get(readColumns);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(error, doc, currentLocation);
    } else if (unlikely(readColumns.is_empty())) {
      readColumns = {};
    } else {
      for (auto readColumn : readColumns) {
        PKReadReadColumn pkReadReadColumn;
        simdjson::ondemand::object readColumnObj;
        error = readColumn.get(readColumnObj);
        if (unlikely(error != simdjson::SUCCESS)) {
          return handle_simdjson_error(error, doc, currentLocation);
        }
        std::string_view column;
        auto columnVal = readColumnObj[COLUMN];
        if (unlikely(columnVal.error() ==
                     simdjson::error_code::NO_SUCH_FIELD)) {
          return CRS_Status(
            HTTP_CODE::CLIENT_ERROR,
             "a column to read is missing a name").status;
        } else if (unlikely(columnVal.error() != simdjson::SUCCESS)) {
          return handle_simdjson_error(columnVal.error(), doc, currentLocation);
        } else if (unlikely(columnVal.is_null())) {
          return CRS_Status(
            HTTP_CODE::CLIENT_ERROR,
            "a column to read is missing a name").status;
        } else {
          error = columnVal.get(column);
          if (unlikely(error != simdjson::SUCCESS)) {
            return handle_simdjson_error(error, doc, currentLocation);
          }
        }
        if (unlikely(column.size() == 0)) {
          return CRS_Status(
            HTTP_CODE::CLIENT_ERROR,
            "a column to read is missing a name").status;
        }
        pkReadReadColumn.column = column;
        std::string_view dataReturnType;
        auto dataReturnTypeVal = readColumnObj[DATA_RETURN_TYPE];
        if (dataReturnTypeVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
        } else if (unlikely(dataReturnTypeVal.error() != simdjson::SUCCESS)) {
          return handle_simdjson_error(
            dataReturnTypeVal.error(), doc, currentLocation);
        } else {
          if (unlikely(dataReturnTypeVal.is_null())) {
          } else {
            error = dataReturnTypeVal.get(dataReturnType);
            if (unlikely(error != simdjson::SUCCESS)) {
              return handle_simdjson_error(error, doc, currentLocation);
            }
            std::string_view check = "default";
            if (check != dataReturnType) {
              return CRS_Status(
                HTTP_CODE::CLIENT_ERROR,
                "default dataReturnType is the only supported").status;
            }
          }
        }
        reqStruct.readColumns.emplace_back(pkReadReadColumn);
      }
    }
  }

  std::string_view operationId;
  auto operationIdVal = reqObject[OPERATION_ID];
  if (operationIdVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    operationId = "";
  } else if (unlikely(operationIdVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(operationIdVal.error(), doc, currentLocation);
  } else {
    if (unlikely(operationIdVal.is_null())) {
      operationId = "";
    } else {
      error = operationIdVal.get(operationId);
      if (unlikely(error != simdjson::SUCCESS)) {
        return handle_simdjson_error(error, doc, currentLocation);
      }
    }
  }
  reqStruct.operationId = operationId;
  return CRS_Status::SUCCESS.status;
}

// This is used to perform batched primary key read operations.
// The body here is a list of arbitrary pk-reads under the key operations:
RS_Status JSONParser::batch_parse(simdjson::padded_string_view reqBody,
                                  std::vector<PKReadParams> &reqStructs) {
  const char *currentLocation = nullptr;

  simdjson::error_code error = parser.iterate(reqBody).get(doc);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }

  simdjson::ondemand::object reqObject;
  error = doc.get_object().get(reqObject);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }

  simdjson::ondemand::array operations;
  auto operationsVal = reqObject[OPERATIONS];
  if (unlikely(operationsVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
    return CRS_Status(
      HTTP_CODE::CLIENT_ERROR, "No operations defined").status;
    operations = {};
  } else if (unlikely(operationsVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(operationsVal.error(), doc, currentLocation);
  }
  if (unlikely(operationsVal.is_null())) {
    return CRS_Status(
      HTTP_CODE::CLIENT_ERROR, "No operations defined").status;
  }
  error = operationsVal.get(operations);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }
  if (unlikely(operations.is_empty())) {
    return CRS_Status(
      HTTP_CODE::CLIENT_ERROR, "No operations defined").status;
  }
  for (auto operation : operations) {
    PKReadParams reqStruct;
    simdjson::ondemand::object operationObj;
    error = operation.get(operationObj);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(error, doc, currentLocation);
    }
    std::string_view method;
    auto methodVal = operationObj[METHOD];
    if (unlikely(methodVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
    } else if (unlikely(methodVal.error() != simdjson::SUCCESS)) {
      return handle_simdjson_error(methodVal.error(), doc, currentLocation);
    } else if (methodVal.is_null()) {
      return CRS_Status(
        HTTP_CODE::CLIENT_ERROR, "the Method section should be POST").status;
    } else {
      error = methodVal.get(method);
      if (unlikely(error != simdjson::SUCCESS)) {
        return handle_simdjson_error(error, doc, currentLocation);
      }
      if (unlikely(method != POST)) {
        return CRS_Status(
          HTTP_CODE::CLIENT_ERROR, "the Method section should be POST").status;
      }
    }

    std::string_view relativeUrl;
    auto relativeUrlVal = operationObj[RELATIVE_URL];
    if (relativeUrlVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
      return CRS_Status(
        HTTP_CODE::CLIENT_ERROR, "the relativeUrl section is required").status;
    } else if (relativeUrlVal.error() != simdjson::SUCCESS) {
      return handle_simdjson_error(
        relativeUrlVal.error(), doc, currentLocation);
    } else if (unlikely(relativeUrlVal.is_null())) {
      return CRS_Status(
        HTTP_CODE::CLIENT_ERROR, "the relativeUrl section is required").status;
    } else {
      error = relativeUrlVal.get(relativeUrl);
      if (unlikely(error != simdjson::SUCCESS)) {
        return handle_simdjson_error(error, doc, currentLocation);
      }
    }
    RS_Status status =
      extract_db_and_table(relativeUrl,
                           reqStruct.path.db,
                           reqStruct.path.table,
                           PKREAD);
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
                             drogon::HttpStatusCode::k200OK)) {
      return status;
    }

    simdjson::ondemand::object bodyObject;
    auto bodyVal = operationObj[BODY];
    if (unlikely(bodyVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
        ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY))).status;
    }
    if (unlikely(bodyVal.error() != simdjson::SUCCESS)) {
      return handle_simdjson_error(bodyVal.error(), doc, currentLocation);
    } else if (unlikely(bodyVal.is_null())) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
        ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY))).status;
    }
    error = bodyVal.get(bodyObject);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(error, doc, currentLocation);
    }

    simdjson::ondemand::array filters;
    auto filtersVal = bodyObject[FILTERS];
    if (unlikely(filtersVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
      return CRS_Status(
        HTTP_CODE::CLIENT_ERROR, "the Field section is empty").status;
    } else if (unlikely(filtersVal.error() != simdjson::SUCCESS)) {
      return handle_simdjson_error(filtersVal.error(), doc, currentLocation);
    } else if (unlikely(filtersVal.is_null())) {
      return CRS_Status(
        HTTP_CODE::CLIENT_ERROR, "the Field section is null").status;
    } else {
      error = filtersVal.get(filters);
      if (unlikely(error != simdjson::SUCCESS)) {
        return handle_simdjson_error(error, doc, currentLocation);
      }
      if (unlikely(filters.is_empty())) {
        return CRS_Status(
          HTTP_CODE::CLIENT_ERROR, "the Field section is empty").status;
      }
    }
    for (auto filter : filters) {
      PKReadFilter pkReadFilter;
      simdjson::ondemand::object filterObj;
      error = filter.get(filterObj);
      if (unlikely(error != simdjson::SUCCESS)) {
        return handle_simdjson_error(error, doc, currentLocation);
      }
      std::string_view column;
      auto columnVal = filterObj[COLUMN];
      if (unlikely(columnVal.error() != simdjson::SUCCESS)) {
        return handle_simdjson_error(columnVal.error(), doc, currentLocation);
      } else if (unlikely(columnVal.is_null())) {
        return CRS_Status(
          HTTP_CODE::CLIENT_ERROR,
          "a Column name in the Field section is null").status;
      }
      error = columnVal.get(column);
      if (unlikely(error != simdjson::SUCCESS)) {
        return handle_simdjson_error(error, doc, currentLocation);
      }
      if (unlikely(column.size() == 0)) {
        return CRS_Status(
          HTTP_CODE::CLIENT_ERROR,
          "a Column name in the Field section is empty").status;
      }
      pkReadFilter.column = column;

      simdjson::ondemand::value value;
      std::vector<char> bytes;
      auto valueVal = filterObj[VALUE];
      error = valueVal.get(value);
      if (unlikely(error != simdjson::SUCCESS)) {
        return handle_simdjson_error(columnVal.error(), doc, currentLocation);
      } else if (unlikely(valueVal.is_null())) {
        return CRS_Status(
          HTTP_CODE::CLIENT_ERROR,
          "a Column in the Field section is null").status;
      }
      std::ostringstream oss;
      oss << value;
      std::string valueJson = oss.str();
      bytes = std::vector<char>(valueJson.begin(), valueJson.end());
      pkReadFilter.value = bytes;
      reqStruct.filters.emplace_back(pkReadFilter);
    }

    simdjson::ondemand::array readColumns;
    auto readColumnsVal = bodyObject[READCOLUMNS];
    if (unlikely(readColumnsVal.error() ==
                 simdjson::error_code::NO_SUCH_FIELD)) {
      readColumns = {};
    } else if (unlikely(readColumnsVal.error() != simdjson::SUCCESS)) {
      return handle_simdjson_error(
        readColumnsVal.error(), doc, currentLocation);
    } else if (unlikely(readColumnsVal.is_null())) {
      readColumns = {};
    } else {
      error = readColumnsVal.get(readColumns);
      if (unlikely(error != simdjson::SUCCESS)) {
        return handle_simdjson_error(error, doc, currentLocation);
      } else if (unlikely(readColumns.is_empty())) {
        readColumns = {};
      } else {
        for (auto readColumn : readColumns) {
          PKReadReadColumn pkReadReadColumn;
          simdjson::ondemand::object readColumnObj;
          error = readColumn.get(readColumnObj);
          if (unlikely(error != simdjson::SUCCESS)) {
            return handle_simdjson_error(error, doc, currentLocation);
          }
          std::string_view column;
          auto columnVal = readColumnObj[COLUMN];
          if (unlikely(columnVal.error() ==
              simdjson::error_code::NO_SUCH_FIELD)) {
            return CRS_Status(
            HTTP_CODE::CLIENT_ERROR,
             "a column to read is missing a name").status;
          } else if (unlikely(columnVal.error() != simdjson::SUCCESS)) {
            return handle_simdjson_error(
              columnVal.error(), doc, currentLocation);
          } else if (unlikely(columnVal.is_null())) {
            return CRS_Status(
              HTTP_CODE::CLIENT_ERROR,
               "a column to read is missing a name").status;
          } else {
            error = columnVal.get(column);
            if (unlikely(error != simdjson::SUCCESS)) {
              return handle_simdjson_error(error, doc, currentLocation);
            }
          }
          if (unlikely(column.size() == 0)) {
            return CRS_Status(
              HTTP_CODE::CLIENT_ERROR,
              "a column to read is missing a name").status;
          }
          pkReadReadColumn.column = column;
          std::string_view dataReturnType;
          auto dataReturnTypeVal = readColumnObj[DATA_RETURN_TYPE];
          if (unlikely(dataReturnTypeVal.error() ==
                       simdjson::error_code::NO_SUCH_FIELD)) {
            dataReturnType = "";
          } else if (unlikely(dataReturnTypeVal.error() != simdjson::SUCCESS)) {
            return handle_simdjson_error(dataReturnTypeVal.error(), doc,
                                         currentLocation);
          } else if (unlikely(dataReturnTypeVal.is_null())) {
            dataReturnType = "";
          } else {
            error = dataReturnTypeVal.get(dataReturnType);
            if (unlikely(error != simdjson::SUCCESS)) {
              return handle_simdjson_error(error, doc, currentLocation);
            }
          }
          reqStruct.readColumns.emplace_back(pkReadReadColumn);
        }
      }
    }

    std::string_view operationId;
    auto operationIdVal = bodyObject[OPERATION_ID];
    if (unlikely(operationIdVal.error() ==
                 simdjson::error_code::NO_SUCH_FIELD)) {
      operationId = "";
    } else if (unlikely(operationIdVal.error() != simdjson::SUCCESS)) {
      return handle_simdjson_error(
        operationIdVal.error(), doc, currentLocation);
    } else if (unlikely(operationIdVal.is_null())) {
      operationId = "";
    } else {
      error = operationIdVal.get(operationId);
      if (unlikely(error != simdjson::SUCCESS)) {
        return handle_simdjson_error(error, doc, currentLocation);
      }
    }
    reqStruct.operationId = operationId;
    reqStructs.push_back(reqStruct);
  }
  return CRS_Status::SUCCESS.status;
}

/*
 * Parsers for simple datatypes
 */

USE_SIMDJSON_PARSER(bool)
USE_SIMDJSON_PARSER(simdjson::ondemand::array)
USE_SIMDJSON_PARSER(simdjson::ondemand::object)
USE_SIMDJSON_PARSER(std::string_view)
USE_SIMDJSON_PARSER(uint64_t)
USE_SIMDJSON_PARSER(int64_t)

DEFINE_VALUE_PARSER(std::string, {
  std::string_view temp_target;
  if (!parse(temp_target, value)) {
    return false;
  }
  target = temp_target;
  return true;
})

DEFINE_VALUE_PARSER(Uint16, {
  int64_t temp_target;
  if (!parse(temp_target, value)) {
    return false;
  }
  if (temp_target < 0 || 65535 < temp_target) {
    throw ConfigParseError("16-bit unsigned integer out of range");
  }
  target = temp_target;
  return true;
})

DEFINE_VALUE_PARSER(Uint32, {
  int64_t temp_target;
  if (!parse(temp_target, value)) {
    return false;
  }
  if (temp_target < 0 || UINT32_MAX < temp_target) {
    throw ConfigParseError("32-bit unsigned integer out of range");
  }
  target = temp_target;
  return true;
})

DEFINE_VALUE_PARSER(int, {
  int64_t temp_target;
  if (!parse(temp_target, value)) {
    return false;
  }
  if (temp_target < INT_MIN || INT_MAX < temp_target) {
    throw ConfigParseError("32-bit signed integer out of range");
  }
  target = temp_target;
  return true;
})

/*
 * Parsers for the config structs.
 */

#define CLASS(NAME, ...) DEFINE_STRUCT_PARSER(NAME, __VA_ARGS__)
#define CM(DATATYPE, VARIABLENAME, JSONKEYNAME, INITEXPR, DOCSTRING) \
  ELEMENT(VARIABLENAME, JSONKEYNAME)
#define ALIAS(ACTUALVARIABLENAME, ACTUALJSONKEYNAME, ALIASJSONKEYNAME) \
  ELEMENT(ACTUALVARIABLENAME, ALIASJSONKEYNAME)
#define PROBLEM(CONDITION, MESSAGE)
#define CLASSDEFS(...)
#define VECTOR(DATATYPE) DEFINE_ARRAY_PARSER(DATATYPE)

#include "config_structs_def.hpp"

#undef CLASS
#undef CM
#undef ALIAS
#undef PROBLEM
#undef CLASSDEFS
#undef VECTOR

RS_Status JSONParser::config_parse(const std::string &configsBody,
                                   AllConfigs &configsStruct) noexcept {
  simdjson::padded_string paddedJson(configsBody);
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc;
  bool doc_initialized = false;
  try {
    try {
      doc = parser.iterate(paddedJson).value();
      doc_initialized = true;
      parse(configsStruct, doc.get_object());
      assert_end_of_doc(doc);
    }
    catch (simdjson::simdjson_error& e) {
      throw ConfigParseError(simdjson::error_message(e.error()));
    }
  }
  catch (ConfigParseError& e) {
    return handle_parse_error(e, paddedJson, doc_initialized ? &doc : nullptr);
  }
  return CRS_Status::SUCCESS.status;
}

// todo-ronsql Should we conform to the rest of RDRS by capitalizing
// these JSON keys?
DEFINE_STRUCT_PARSER(RonSQLParams,
                     ELEMENT(query,        query)
                     ELEMENT(database,     database)
                     ELEMENT(explainMode,  explainMode)
                     ELEMENT(outputFormat, outputFormat)
                     ELEMENT(operationId,  operationId)
                     )

RS_Status JSONParser::ronsql_parse(simdjson::padded_string_view reqBody,
                                   RonSQLParams &reqStruct) {
  bool doc_initialized = false;
  try {
    try {
      doc = parser.iterate(reqBody).value();
      doc_initialized = true;
      parse(reqStruct, doc.get_object());
      assert_end_of_doc(doc);
    }
    catch (simdjson::simdjson_error& e) {
      throw ConfigParseError(simdjson::error_message(e.error()));
    }
  }
  catch (ConfigParseError& e) {
    return handle_parse_error(e, reqBody, doc_initialized ? &doc : nullptr);
  }
  return CRS_Status::SUCCESS.status;
}

RS_Status
JSONParser::feature_store_parse(
  simdjson::padded_string_view reqBody,
  feature_store_data_structs::FeatureStoreRequest &reqStruct) {

  const char *currentLocation = nullptr;
  simdjson::error_code error = parser.iterate(reqBody).get(doc);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }

  simdjson::ondemand::object reqObject;
  error = doc.get_object().get(reqObject);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }

  std::string_view featureStoreName;
  auto featureStoreNameVal = reqObject[FEATURE_STORE_NAME];
  if (unlikely(featureStoreNameVal.error() ==
               simdjson::error_code::NO_SUCH_FIELD)) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(FEATURE_STORE_NAME)).status;
  }
  if (unlikely(featureStoreNameVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(
      featureStoreNameVal.error(), doc, currentLocation);
  }
  if (unlikely(featureStoreNameVal.is_null())) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(FEATURE_STORE_NAME)).status;
  }
  error = featureStoreNameVal.get(featureStoreName);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }
  if (unlikely(featureStoreName.size() == 0)) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(FEATURE_STORE_NAME)).status;
  }
  reqStruct.featureStoreName = featureStoreName;

  std::string_view featureViewName;
  auto featureViewNameVal = reqObject[FEATURE_VIEW_NAME];
  if (unlikely(featureViewNameVal.error() ==
               simdjson::error_code::NO_SUCH_FIELD)) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(FEATURE_VIEW_NAME)).status;
  }
  if (unlikely(featureViewNameVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(
      featureViewNameVal.error(), doc, currentLocation);
  }
  if (unlikely(featureViewNameVal.is_null())) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(FEATURE_VIEW_NAME)).status;
  }
  error = featureViewNameVal.get(featureViewName);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }
  if (unlikely(featureViewName.size() == 0)) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(FEATURE_VIEW_NAME)).status;
  }
  reqStruct.featureViewName = featureViewName;

  uint64_t featureViewVersion = 0;
  auto featureViewVersionVal  = reqObject[FEATURE_VIEW_VERSION];
  if (unlikely(featureViewVersionVal.error() ==
               simdjson::error_code::NO_SUCH_FIELD)) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(FEATURE_VIEW_VERSION)).status;
  }
  if (unlikely(featureViewVersionVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(
      featureViewVersionVal.error(), doc, currentLocation);
  }
  if (unlikely(featureViewVersionVal.is_null())) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(FEATURE_VIEW_VERSION)).status;
  }
  error = featureViewVersionVal.get(featureViewVersion);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }
  reqStruct.featureViewVersion = featureViewVersion;

  simdjson::ondemand::object passedFeatures;  // Optional
  auto passedFeaturesVal = reqObject[PASSED_FEATURES];
  if (passedFeaturesVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
  } else if (passedFeaturesVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(
      passedFeaturesVal.error(), doc, currentLocation);
  } else {
    if (likely(!passedFeaturesVal.is_null())) {
      error = passedFeaturesVal.get(passedFeatures);
      if (unlikely(error != simdjson::SUCCESS)) {
        return handle_simdjson_error(error, doc, currentLocation);
      }
      // Map of feature name as key and feature value as value.
      // This overwrites feature values in the response.
      for (auto feature : passedFeatures) {
        std::string_view featureName = feature.unescaped_key();
        simdjson::ondemand::value value;
        std::vector<char> bytes;
        auto valueVal = feature.value();
        if (unlikely(valueVal.error() ==
                     simdjson::error_code::NO_SUCH_FIELD)) {
          return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
            ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
            " " + std::string(featureName)).status;
        }
        if (unlikely(valueVal.error() != simdjson::SUCCESS)) {
          return handle_simdjson_error(valueVal.error(), doc, currentLocation);
        }
        if (unlikely(valueVal.is_null())) {
          return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
            ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
            " " + std::string(featureName)).status;
        }
        error = valueVal.get(value);
        if (unlikely(error != simdjson::SUCCESS)) {
          return handle_simdjson_error(error, doc, currentLocation);
        }
        std::ostringstream oss;
        oss << value;
        std::string valueJson = oss.str();
        bytes = std::vector<char>(valueJson.begin(), valueJson.end());
        reqStruct.passedFeatures[std::string(featureName)] = bytes;
      }
    }
  }

  simdjson::ondemand::object entries;
  //Map of serving key of feature view as key and value of serving key as value.
  // Serving key are a set of the primary key of feature groups which are
  // included in the feature
  // view query. If feature groups are joint with prefix, the primary key
  // needs to be attached with prefix.
  auto entriesVal = reqObject[ENTRIES];
  if (unlikely(entriesVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(ENTRIES)).status;
  } else if (unlikely(entriesVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(entriesVal.error(), doc, currentLocation);
  } else if (entriesVal.is_null()) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(ENTRIES)).status;
  }
  error = entriesVal.get(entries);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }
  for (auto entry : entries) {
    std::string_view servingKey = entry.unescaped_key();
    simdjson::ondemand::value value;
    std::vector<char> bytes;
    auto valueVal = entry.value();
    if (unlikely(valueVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
        ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
        " " + std::string(servingKey)).status;
    }
    if (unlikely(valueVal.error() != simdjson::SUCCESS)) {
      return handle_simdjson_error(valueVal.error(), doc, currentLocation);
    }
    if (unlikely(valueVal.is_null())) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
        ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
        " " + std::string(servingKey)).status;
    }
    error = valueVal.get(value);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(error, doc, currentLocation);
    }
    std::ostringstream oss;
    oss << value;
    std::string valueJson = oss.str();
    bytes = std::vector<char>(valueJson.begin(), valueJson.end());
    reqStruct.entries[std::string(servingKey)] = bytes;
  }

  simdjson::ondemand::object metaDataOptions;// Optional.
  // Map of metadataoption as key and boolean as value.
  // Default metadata option is false. Metadata is returned on request.
  // Metadata options available: 1. featureName 2. featureType
  auto metaDataOptionsVal = reqObject[METADATA_OPTIONS];
  if (metaDataOptionsVal.error() == simdjson::error_code::NO_SUCH_FIELD ||
      metaDataOptionsVal.is_null()) {
    // If the metadataOptions field is not present or is null,
    // set the optional fields to nullopt
    reqStruct.metadataRequest.featureName = std::nullopt;
    reqStruct.metadataRequest.featureType = std::nullopt;
  } else if (unlikely(metaDataOptionsVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(
      metaDataOptionsVal.error(), doc, currentLocation);
  } else {
    // If metadataOptions field is present and not null
    error = metaDataOptionsVal.get(metaDataOptions);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(error, doc, currentLocation);
    }
    for (auto option : metaDataOptions) {
      std::string_view optionKey = option.unescaped_key();
      if (optionKey == FEATURE_NAME) {
        bool optionValue = false;
        auto optionValueVal = option.value();
        if (unlikely(optionValueVal.error() != simdjson::SUCCESS)) {
          return handle_simdjson_error(
            optionValueVal.error(), doc, currentLocation);
        }
        if (likely(!optionValueVal.is_null())) {
          error = optionValueVal.get(optionValue);
          if (unlikely(error != simdjson::SUCCESS)) {
            return handle_simdjson_error(error, doc, currentLocation);
          }
          reqStruct.metadataRequest.featureName = optionValue;
        } else {
          reqStruct.metadataRequest.featureName = std::nullopt;
        }
      } else if (optionKey == FEATURE_TYPE) {
        bool optionValue = false;
        auto optionValueVal = option.value();
        if (unlikely(optionValueVal.error() != simdjson::SUCCESS)) {
          return handle_simdjson_error(
            optionValueVal.error(), doc, currentLocation);
        }
        if (likely(!optionValueVal.is_null())) {
          error = optionValueVal.get(optionValue);
          if (unlikely(error != simdjson::SUCCESS)) {
            return handle_simdjson_error(error, doc, currentLocation);
          }
          reqStruct.metadataRequest.featureType = optionValue;
        } else {
          reqStruct.metadataRequest.featureType = std::nullopt;
        }
      } else {
        return CRS_Status(static_cast<HTTP_CODE>(
          drogon::HttpStatusCode::k400BadRequest),
          ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
          " " + std::string(optionKey) +
          std::string(METADATA_OPTIONS)).status;
      }
    }
  }

  simdjson::ondemand::object options;
  // Optional. Map of option as key and boolean as value.
  // Default option is false.
  // Options available: 1. validatePassedFeatures 2. includeDetailedStatus
  auto optionsVal = reqObject[OPTIONS];
  if (optionsVal.error() == simdjson::error_code::NO_SUCH_FIELD ||
      optionsVal.is_null()) {
    // If the options field is not present or is null, set the optional
    // fields to nullopt
    reqStruct.optionsRequest.validatePassedFeatures = std::nullopt;
    reqStruct.optionsRequest.includeDetailedStatus  = std::nullopt;
  } else if (unlikely(optionsVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(optionsVal.error(), doc, currentLocation);
  } else {
    // If options field is present and not null
    error = optionsVal.get(options);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(error, doc, currentLocation);
    }
    for (auto option : options) {
      std::string_view optionKey = option.unescaped_key();
      if (optionKey == VALIDATE_PASSED_FEATURES) {
        bool optionValue = false;
        auto optionValueVal = option.value();
        if (unlikely(optionValueVal.error() != simdjson::SUCCESS)) {
          return handle_simdjson_error(
            optionValueVal.error(), doc, currentLocation);
        }
        if (likely(!optionValueVal.is_null())) {
          error = optionValueVal.get(optionValue);
          if (unlikely(error != simdjson::SUCCESS)) {
            return handle_simdjson_error(error, doc, currentLocation);
          }
          reqStruct.optionsRequest.validatePassedFeatures = optionValue;
        } else {
          reqStruct.optionsRequest.validatePassedFeatures = std::nullopt;
        }
      } else if (likely(optionKey == INCLUDE_DETAILED_STATUS)) {
        bool optionValue = false;
        auto optionValueVal = option.value();
        if (unlikely(optionValueVal.error() != simdjson::SUCCESS)) {
          return handle_simdjson_error(
            optionValueVal.error(), doc, currentLocation);
        }
        if (likely(!optionValueVal.is_null())) {
          error = optionValueVal.get(optionValue);
          if (unlikely(error != simdjson::SUCCESS)) {
            return handle_simdjson_error(error, doc, currentLocation);
          }
          reqStruct.optionsRequest.includeDetailedStatus = optionValue;
        } else {
          reqStruct.optionsRequest.includeDetailedStatus = std::nullopt;
        }
      } else {
        return CRS_Status(static_cast<HTTP_CODE>(
          drogon::HttpStatusCode::k400BadRequest),
          ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
          " " + std::string(optionKey) +
          std::string(OPTIONS)).status;
      }
    }
  }
  return CRS_Status::SUCCESS.status;
}

RS_Status JSONParser::batch_feature_store_parse(
    simdjson::padded_string_view reqBody,
    feature_store_data_structs::BatchFeatureStoreRequest &reqStruct) {
  const char *currentLocation = nullptr;

  simdjson::error_code error = parser.iterate(reqBody).get(doc);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }

  simdjson::ondemand::object reqObject;
  error = doc.get_object().get(reqObject);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }

  std::string_view featureStoreName;
  auto featureStoreNameVal = reqObject[FEATURE_STORE_NAME];
  if (featureStoreNameVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(FEATURE_STORE_NAME)).status;
  }
  if (unlikely(featureStoreNameVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(
      featureStoreNameVal.error(), doc, currentLocation);
  }
  if (unlikely(featureStoreNameVal.is_null())) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(FEATURE_STORE_NAME)).status;
  }
  error = featureStoreNameVal.get(featureStoreName);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }
  reqStruct.featureStoreName = featureStoreName;

  std::string_view featureViewName;
  auto featureViewNameVal = reqObject[FEATURE_VIEW_NAME];
  if (unlikely(featureViewNameVal.error() ==
               simdjson::error_code::NO_SUCH_FIELD)) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(FEATURE_VIEW_NAME)).status;
  }
  if (unlikely(featureViewNameVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(
      featureViewNameVal.error(), doc, currentLocation);
  }
  if (unlikely(featureViewNameVal.is_null())) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(FEATURE_VIEW_NAME)).status;
  }
  error = featureViewNameVal.get(featureViewName);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }
  reqStruct.featureViewName = featureViewName;

  uint64_t featureViewVersion = 0;
  auto featureViewVersionVal  = reqObject[FEATURE_VIEW_VERSION];
  if (unlikely(featureViewVersionVal.error() ==
               simdjson::error_code::NO_SUCH_FIELD)) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(FEATURE_VIEW_VERSION)).status;
  } else if (unlikely(featureViewVersionVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(
      featureViewVersionVal.error(), doc, currentLocation);
  }
  if (featureViewVersionVal.is_null()) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(FEATURE_VIEW_VERSION)).status;
  }
  error = featureViewVersionVal.get(featureViewVersion);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }
  reqStruct.featureViewVersion = featureViewVersion;

  simdjson::ondemand::array passedFeatures;  // Optional
  auto passedFeaturesVal = reqObject[PASSED_FEATURES];
  if (passedFeaturesVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
  } else if (unlikely(passedFeaturesVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(
      passedFeaturesVal.error(), doc, currentLocation);
  } else {
    if (likely(!passedFeaturesVal.is_null())) {
      error = passedFeaturesVal.get(passedFeatures);
      if (unlikely(error != simdjson::SUCCESS)) {
        return handle_simdjson_error(error, doc, currentLocation);
      }
      // Each item is a map of feature name as key and feature value as value.
      // This overwrites feature values in the response.
      // If provided, its size and order has to be equal to the size of entries.
      // Item can be null.
      for (auto feature : passedFeatures) {
        if (unlikely(feature.is_null())) {
          reqStruct.passedFeatures.push_back(
            std::unordered_map<std::string, std::vector<char>>());
          continue;
        }
        simdjson::ondemand::object featureObj;
        error = feature.get(featureObj);
        if (unlikely(error != simdjson::SUCCESS)) {
          return handle_simdjson_error(error, doc, currentLocation);
        }
        std::unordered_map<std::string, std::vector<char>> featureMap;
        for (auto featureItem : featureObj) {
          std::string_view featureName = featureItem.unescaped_key();
          simdjson::ondemand::value value;
          std::vector<char> bytes;
          auto valueVal = featureItem.value();
          if (unlikely(valueVal.error() ==
                       simdjson::error_code::NO_SUCH_FIELD)) {
            return CRS_Status(static_cast<HTTP_CODE>(
              drogon::HttpStatusCode::k400BadRequest),
              ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
              " " + std::string(featureName)).status;
          }
          if (unlikely(valueVal.error() != simdjson::SUCCESS)) {
            return handle_simdjson_error(
              valueVal.error(), doc, currentLocation);
          }

          if (unlikely(valueVal.is_null())) {
            return CRS_Status(static_cast<HTTP_CODE>(
              drogon::HttpStatusCode::k400BadRequest),
              ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
              " " + std::string(featureName)).status;
          }
          error = valueVal.get(value);
          if (unlikely(error != simdjson::SUCCESS)) {
            return handle_simdjson_error(error, doc, currentLocation);
          }
          std::ostringstream oss;
          oss << value;
          std::string valueJson = oss.str();
          bytes = std::vector<char>(valueJson.begin(), valueJson.end());
          featureMap[std::string(featureName)] = bytes;
        }
        reqStruct.passedFeatures.push_back(featureMap);
      }
    }
  }

  simdjson::ondemand::array entries;
  // Each item is a map of serving key of feature view as key and value
  // of serving key as value.
  // Serving key of feature view.
  auto entriesVal = reqObject[ENTRIES];
  if (unlikely(entriesVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(ENTRIES)).status;
  } else if (unlikely(entriesVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(entriesVal.error(), doc, currentLocation);
  } else if (entriesVal.is_null()) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
      " " + std::string(ENTRIES)).status;
  }
  error = entriesVal.get(entries);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }
  for (auto entry : entries) {
    simdjson::ondemand::object entryObj;
    error = entry.get(entryObj);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(error, doc, currentLocation);
    }
    std::unordered_map<std::string, std::vector<char>> entryMap;
    for (auto entryItem : entryObj) {
      std::string_view servingKey = entryItem.unescaped_key();
      simdjson::ondemand::value value;
      std::vector<char> bytes;
      auto valueVal = entryItem.value();
      if (unlikely(valueVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
        return CRS_Status(static_cast<HTTP_CODE>(
          drogon::HttpStatusCode::k400BadRequest),
          ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
          " " + std::string(servingKey)).status;
      }
      if (unlikely(valueVal.error() != simdjson::SUCCESS)) {
        return handle_simdjson_error(valueVal.error(), doc, currentLocation);
      }
      if (unlikely(valueVal.is_null())) {
        return CRS_Status(static_cast<HTTP_CODE>(
          drogon::HttpStatusCode::k400BadRequest),
          ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
          " " + std::string(servingKey)).status;
      }
      error = valueVal.get(value);
      if (unlikely(error != simdjson::SUCCESS)) {
        return handle_simdjson_error(error, doc, currentLocation);
      }
      std::ostringstream oss;
      oss << value;
      std::string valueJson = oss.str();
      bytes = std::vector<char>(valueJson.begin(), valueJson.end());
      entryMap[std::string(servingKey)] = bytes;
    }
    reqStruct.entries.push_back(entryMap);
  }

  simdjson::ondemand::object metaDataOptions;  // Optional.
  // Map of metadataoption as key and boolean as value.
  // Default metadata option is false. Metadata is returned on request.
  // Metadata options available: 1. featureName 2. featureType
  auto metaDataOptionsVal = reqObject[METADATA_OPTIONS];
  if (metaDataOptionsVal.error() == simdjson::error_code::NO_SUCH_FIELD ||
      metaDataOptionsVal.is_null()) {
    // If the metadataOptions field is not present or is null, set the
    // optional fields to nullopt
    reqStruct.metadataRequest.featureName = std::nullopt;
    reqStruct.metadataRequest.featureType = std::nullopt;
  } else if (metaDataOptionsVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(
      metaDataOptionsVal.error(), doc, currentLocation);
  } else {
    // If metadataOptions field is present and not null
    error = metaDataOptionsVal.get(metaDataOptions);
    if (error != simdjson::SUCCESS) {
      return handle_simdjson_error(error, doc, currentLocation);
    }
    for (auto option : metaDataOptions) {
      std::string_view optionKey = option.unescaped_key();
      if (optionKey == FEATURE_NAME) {
        bool optionValue    = false;
        auto optionValueVal = option.value();
        if (optionValueVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(
            optionValueVal.error(), doc, currentLocation);
        }
        if (!optionValueVal.is_null()) {
          error = optionValueVal.get(optionValue);
          if (error != simdjson::SUCCESS) {
            return handle_simdjson_error(error, doc, currentLocation);
          }
          reqStruct.metadataRequest.featureName = optionValue;
        } else {
          reqStruct.metadataRequest.featureName = std::nullopt;
        }
      } else if (optionKey == FEATURE_TYPE) {
        bool optionValue    = false;
        auto optionValueVal = option.value();
        if (optionValueVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(
            optionValueVal.error(), doc, currentLocation);
        }
        if (!optionValueVal.is_null()) {
          error = optionValueVal.get(optionValue);
          if (error != simdjson::SUCCESS) {
            return handle_simdjson_error(error, doc, currentLocation);
          }
          reqStruct.metadataRequest.featureType = optionValue;
        } else {
          reqStruct.metadataRequest.featureType = std::nullopt;
        }
      }
    }
  }

  simdjson::ondemand::object options;
  // Optional. Map of option as key and boolean as value.
  // Default option is false.
  // Options available: 1. validatePassedFeatures 2. includeDetailedStatus
  auto optionsVal = reqObject[OPTIONS];
  if (optionsVal.error() == simdjson::error_code::NO_SUCH_FIELD ||
      optionsVal.is_null()) {
    // If the options field is not present or is null, set the optional
    // fields to nullopt
    reqStruct.optionsRequest.validatePassedFeatures = std::nullopt;
    reqStruct.optionsRequest.includeDetailedStatus  = std::nullopt;
  } else if (optionsVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(optionsVal.error(), doc, currentLocation);
  } else {
    // If options field is present and not null
    error = optionsVal.get(options);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(error, doc, currentLocation);
    }
    for (auto option : options) {
      std::string_view optionKey = option.unescaped_key();
      if (optionKey == VALIDATE_PASSED_FEATURES) {
        bool optionValue = false;
        auto optionValueVal = option.value();
        if (unlikely(optionValueVal.error() != simdjson::SUCCESS)) {
          return handle_simdjson_error(
            optionValueVal.error(), doc, currentLocation);
        }
        if (likely(!optionValueVal.is_null())) {
          error = optionValueVal.get(optionValue);
          if (unlikely(error != simdjson::SUCCESS)) {
            return handle_simdjson_error(error, doc, currentLocation);
          }
          reqStruct.optionsRequest.validatePassedFeatures = optionValue;
        } else {
          reqStruct.optionsRequest.validatePassedFeatures = std::nullopt;
        }
      } else if (likely(optionKey == INCLUDE_DETAILED_STATUS)) {
        bool optionValue = false;
        auto optionValueVal = option.value();
        if (unlikely(optionValueVal.error() != simdjson::SUCCESS)) {
          return handle_simdjson_error(
            optionValueVal.error(), doc, currentLocation);
        }
        if (likely(!optionValueVal.is_null())) {
          error = optionValueVal.get(optionValue);
          if (unlikely(error != simdjson::SUCCESS)) {
            return handle_simdjson_error(error, doc, currentLocation);
          }
          reqStruct.optionsRequest.includeDetailedStatus = optionValue;
        } else {
          reqStruct.optionsRequest.includeDetailedStatus = std::nullopt;
        }
      } else {
        return CRS_Status(static_cast<HTTP_CODE>(
          drogon::HttpStatusCode::k400BadRequest),
          ERROR_INVALID_BODY, std::string(rdrsErrorMessage(ERROR_INVALID_BODY)) + 
          " " + std::string(optionKey) +
          std::string(OPTIONS)).status;
      }
    }
  }
  return CRS_Status::SUCCESS.status;
}

RS_Status extract_db_and_table(const std::string_view &relativeUrl,
                               std::string_view &db,
                               std::string &table,
                               const std::string& expected_op) {
  // Find the positions of the last three slashes
  std::string_view request_type;
  size_t len = relativeUrl.length();
  if (len < 11) {
    DEB_REL_URL(("1:relativeUrl bad: %s", relativeUrl.data()));
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_RELATIVE_URL,
      std::string(rdrsErrorMessage(ERROR_INVALID_RELATIVE_URL))).status;
  }
  const char *start_pos = relativeUrl.data();
  size_t end_pos = len - 1;
  size_t lastSlashPos = relativeUrl.find_last_of('/');
  while (lastSlashPos == end_pos) {
    /* Remove trailing slashes as is customary in URLs */
    len--;
    end_pos--;
    lastSlashPos = relativeUrl.find_last_of('/', lastSlashPos - 1);
  }
  if (len == 0) {
    DEB_REL_URL(("4:relativeUrl bad: %s", relativeUrl.data()));
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_RELATIVE_URL,
      std::string(rdrsErrorMessage(ERROR_INVALID_RELATIVE_URL))).status;
  }
  size_t firstSlashPos = relativeUrl.find_first_of('/');
  Uint32 leading_slashes = 0;
  while (firstSlashPos == leading_slashes) {
    /* Remove leading slashes as is customary in URLs */
    len--;
    lastSlashPos--;
    leading_slashes++;
    firstSlashPos = relativeUrl.find_first_of('/', leading_slashes);
  }
  std::string_view checkUrl(start_pos + leading_slashes, len);

  size_t secondLastSlashPos = lastSlashPos != std::string_view::npos ?
    checkUrl.find_last_of('/', lastSlashPos - 1) : std::string_view::npos;
  size_t thirdLastSlashPos  = secondLastSlashPos != std::string_view::npos ?
    checkUrl.find_last_of('/', secondLastSlashPos - 1)
      : std::string_view::npos;

  if (thirdLastSlashPos != std::string_view::npos ||
      secondLastSlashPos == std::string_view::npos) {
    DEB_REL_URL(("2:relativeUrl bad: %s", relativeUrl.data()));
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      ERROR_INVALID_RELATIVE_URL,
      std::string(rdrsErrorMessage(ERROR_INVALID_RELATIVE_URL))).status;
  }
  db = checkUrl.substr(0, secondLastSlashPos);
  table = checkUrl.substr(secondLastSlashPos + 1,
                          lastSlashPos - secondLastSlashPos - 1);
  request_type = checkUrl.substr(lastSlashPos + 1);
  if (request_type == expected_op &&
      db.length() > 0 &&
      table.length() > 0) {
    return CRS_Status::SUCCESS.status;
  }
  DEB_REL_URL(("3:relativeUrl bad: %s", relativeUrl.data()));
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k400BadRequest),
    ERROR_INVALID_RELATIVE_URL,
    std::string(rdrsErrorMessage(ERROR_INVALID_RELATIVE_URL))).status;
}

RS_Status handle_simdjson_error(const simdjson::error_code &error,
                                simdjson::ondemand::document &doc,
                                const char *&currentLocation) {
  simdjson::error_code getLocationError =
    doc.current_location().get(currentLocation);
  if (getLocationError != simdjson::SUCCESS) {
    return CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      error_message(error), "").status;
  }
  return CRS_Status(static_cast<HTTP_CODE>(
    drogon::HttpStatusCode::k400BadRequest),
    error_message(error), currentLocation).status;
}

/*** Scan Read ***/
bool parseGroup(std::string_view op, FilterNode::Group& out) {
  if (op == "AND") {
    out = FilterNode::Group::AND;
    return true;
  } else if (op == "OR") {
    out = FilterNode::Group::OR;
    return true;
  } else if (op == "NAND") {
    out = FilterNode::Group::NAND;
    return true;
  } else if (op == "NOR") {
    out = FilterNode::Group::NOR;
    return true;
  }
	return false;
}

RS_Status ParseJsonValue(simdjson::simdjson_result<simdjson::ondemand::value> v,
                         Node::ParsedValue& pv,
                         simdjson::ondemand::document &doc) {
  auto err = v.error();
  if (err != simdjson::SUCCESS) {
    const char *currentLocation = nullptr;
    return handle_simdjson_error(err, doc, currentLocation);
  }
  simdjson::ondemand::value value = v.value_unsafe();

  if (value.is_null()) {
    pv.kind = Node::ParsedValue::Kind::NULLVAL;
    return CRS_Status().status;
  }

  auto num_res = value.get_number();
  if (num_res.error() == simdjson::SUCCESS) {
    simdjson::ondemand::number num = num_res.value_unsafe();

    if (num.is_uint64()) {
      pv.kind = Node::ParsedValue::Kind::UINT64;
      pv.u64 = num.get_uint64();
    } else if (num.is_int64()) {
      pv.kind = Node::ParsedValue::Kind::INT64;
      pv.i64 = num.get_int64();
    } else if (num.is_double()) {
      pv.kind = Node::ParsedValue::Kind::DOUBLE;
      pv.d = num.get_double();
    } else {
      return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
          ERROR_SCAN_INVALID_VALUE,
          std::string(rdrsErrorMessage(ERROR_SCAN_INVALID_VALUE))).status;
    }
    return CRS_Status().status;
  }

  std::string_view sv;
  if (value.get(sv) == simdjson::SUCCESS) {
    pv.kind = Node::ParsedValue::Kind::STRING;
    pv.s.assign(sv);
    return CRS_Status().status;
  }

  return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
      ERROR_SCAN_INVALID_VALUE,
      std::string(rdrsErrorMessage(ERROR_SCAN_INVALID_VALUE))).status;
}

// Security limits for scan operations
static constexpr int MAX_FILTER_DEPTH = 32;
static constexpr size_t MAX_SCAN_RANGES = 64;

RS_Status parseScanFilter(
    simdjson::ondemand::document& doc,
    simdjson::ondemand::object obj,
    std::shared_ptr<FilterNode>& out,
    std::string& err,
    int depth = 0) {

  // Check recursion depth to prevent stack overflow
  if (depth > MAX_FILTER_DEPTH) {
    return CRS_Status(static_cast<HTTP_CODE>(
          drogon::HttpStatusCode::k400BadRequest),
        ERROR_SCAN_FILTER_TOO_DEEP,
        std::string(rdrsErrorMessage(ERROR_SCAN_FILTER_TOO_DEEP))).status;
  }

  const char *currentLocation = nullptr;
  auto opVal = obj["op"];
  if (unlikely(opVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
    return CRS_Status(static_cast<HTTP_CODE>(
          drogon::HttpStatusCode::k400BadRequest),
        ERROR_SCAN_FILTER_MISSING_OP,
        std::string(rdrsErrorMessage(ERROR_SCAN_FILTER_MISSING_OP))).status;
  } else if (unlikely(opVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(opVal.error(), doc, currentLocation);
  }
  std::string_view op;
  auto error = opVal.get(op);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }

  auto node = std::make_shared<FilterNode>();

  FilterNode::Group group;
  if (parseGroup(op, group)) {
    node->type = FilterNode::Type::LOGIC;
    node->group = group;

    auto argsVal = obj["args"];
    if (unlikely(argsVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
      return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
          ERROR_SCAN_FILTER_LOGIC_MISSING_FILTERS,
          std::string(rdrsErrorMessage(ERROR_SCAN_FILTER_LOGIC_MISSING_FILTERS))).status;
    } else if (unlikely(argsVal.error() != simdjson::SUCCESS)) {
      return handle_simdjson_error(argsVal.error(), doc, currentLocation);
    }
    simdjson::ondemand::array arr;
    error = argsVal.get(arr);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(error, doc, currentLocation);
    }

    for (simdjson::ondemand::value child : arr) {
      simdjson::ondemand::object childObj;
      error = child.get(childObj);
      if (error != simdjson::SUCCESS) {
        return CRS_Status(static_cast<HTTP_CODE>(
              drogon::HttpStatusCode::k400BadRequest),
            ERROR_SCAN_FILTER_LOGIC_INVALID_CHILD_TYPE,
            std::string(rdrsErrorMessage(ERROR_SCAN_FILTER_LOGIC_INVALID_CHILD_TYPE))).status;
      }

      std::shared_ptr<FilterNode> sub;
      RS_Status ret = parseScanFilter(doc, childObj, sub, err, depth + 1);
      if (ret.http_code != HTTP_CODE::SUCCESS) {
        return ret;
      }

      node->children.push_back(sub);
    }

    /* Binary logic tree */
    if (node->children.size() != 2) {
      return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
          ERROR_SCAN_FILTER_LOGIC_INVALID_CHILD_NUM,
          std::string(rdrsErrorMessage(ERROR_SCAN_FILTER_LOGIC_INVALID_CHILD_NUM))).status;
    }
    out = node;
    return CRS_Status().status;
  }

  if (op == "CMP") {
    node->type = FilterNode::Type::COMPARE;

    // column
    auto columnVal = obj["column"];
    if (unlikely(columnVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
      return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
          ERROR_SCAN_FILTER_CMP_MISSING_COLUMN,
          std::string(rdrsErrorMessage(ERROR_SCAN_FILTER_CMP_MISSING_COLUMN))).status;
    } else if (unlikely(columnVal.error() != simdjson::SUCCESS)) {
      return handle_simdjson_error(columnVal.error(), doc, currentLocation);
    }
    std::string_view col;
    error = columnVal.get(col);
    if (error != simdjson::SUCCESS) {
      return handle_simdjson_error(columnVal.error(), doc, currentLocation);
    }
    node->column = std::string(col);

    // cond
    auto condVal = obj["cond"];
    if (unlikely(condVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
      return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
          ERROR_SCAN_FILTER_CMP_MISSING_COND,
          std::string(rdrsErrorMessage(ERROR_SCAN_FILTER_CMP_MISSING_COND))).status;
    } else if (unlikely(condVal.error() != simdjson::SUCCESS)) {
      return handle_simdjson_error(condVal.error(), doc, currentLocation);
    }
    std::string_view cond_string;
    error = condVal.get(cond_string);
    if (error != simdjson::SUCCESS) {
      return handle_simdjson_error(condVal.error(), doc, currentLocation);
    }

    FilterNode::Condition cond = FilterNode::Condition::COND_LE;
    if (cond_string == "LE") {
      cond = FilterNode::Condition::COND_LE;
    } else if (cond_string == "LT") {
      cond = FilterNode::Condition::COND_LT;
    } else if (cond_string == "GE") {
      cond = FilterNode::Condition::COND_GE;
    } else if (cond_string == "GT") {
      cond = FilterNode::Condition::COND_GT;
    } else if (cond_string == "EQ") {
      cond = FilterNode::Condition::COND_EQ;
    } else if (cond_string == "NE") {
      cond = FilterNode::Condition::COND_NE;
    } else {
      return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
          ERROR_SCAN_FILTER_INVALID_COND,
          std::string(rdrsErrorMessage(ERROR_SCAN_FILTER_INVALID_COND))).status;
    }
    node->cond = cond;

    // value
    auto valueVal = obj["value"];
    if (unlikely(valueVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
      return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
          ERROR_SCAN_FILTER_CMP_MISSING_VALUE,
          std::string(rdrsErrorMessage(ERROR_SCAN_FILTER_CMP_MISSING_VALUE))).status;
    } else if (unlikely(valueVal.error() != simdjson::SUCCESS)) {
      return handle_simdjson_error(valueVal.error(), doc, currentLocation);
    }

    auto st = ParseJsonValue(valueVal, node->value, doc);
    if (st.http_code != HTTP_CODE::SUCCESS) {
      return st;
    }

    out = node;
    return CRS_Status().status;
  }

  if (op == "ISNULL") {
    node->type = FilterNode::Type::IS_NULL;

    auto columnVal = obj["column"];
    if (unlikely(columnVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
      return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
          ERROR_SCAN_FILTER_ISNULL_MISSING_COLUMN,
          std::string(rdrsErrorMessage(ERROR_SCAN_FILTER_ISNULL_MISSING_COLUMN))).status;
    } else if (unlikely(columnVal.error() != simdjson::SUCCESS)) {
      return handle_simdjson_error(columnVal.error(), doc, currentLocation);
    }

    std::string_view col;
    error = columnVal.get(col);
    if (error != simdjson::SUCCESS) {
      return handle_simdjson_error(columnVal.error(), doc, currentLocation);
    }

    node->column = std::string(col);
    out = node;
    return CRS_Status().status;
  }

  if (op == "ISNOTNULL") {
    node->type = FilterNode::Type::IS_NOT_NULL;

    auto columnVal = obj["column"];
    if (unlikely(columnVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
      return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
          ERROR_SCAN_FILTER_ISNOTNULL_MISSING_COLUMN,
          std::string(rdrsErrorMessage(ERROR_SCAN_FILTER_ISNOTNULL_MISSING_COLUMN))).status;
    } else if (unlikely(columnVal.error() != simdjson::SUCCESS)) {
      return handle_simdjson_error(columnVal.error(), doc, currentLocation);
    }

    std::string_view col;
    auto res = columnVal.get(col);
    if (res) {
      return handle_simdjson_error(columnVal.error(), doc, currentLocation);
    }

    node->column = std::string(col);
    out = node;
    return CRS_Status().status;
  }

  return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
      ERROR_SCAN_FILTER_UNKNOWN_OP,
      std::string(rdrsErrorMessage(ERROR_SCAN_FILTER_UNKNOWN_OP))).status;
}

RS_Status parseScanIndex(simdjson::ondemand::document& doc,
                         simdjson::ondemand::object indexObj,
                         ScanReadParams& out) {
  const char *currentLocation = nullptr;
  IndexScanParams index;

  // ---- name ----
  {
    std::string_view sv;
    auto res = indexObj["name"].get(sv);
    if (res == simdjson::error_code::NO_SUCH_FIELD) {
      return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
          ERROR_SCAN_INDEX_MISSING_NAME,
          std::string(rdrsErrorMessage(ERROR_SCAN_INDEX_MISSING_NAME))).status;
    }
    if (res != simdjson::SUCCESS) {
      return handle_simdjson_error(res, doc, currentLocation);
    }
    index.name.assign(sv);
  }

  // ---- columns ----
  {
    auto colsVal = indexObj["key_columns"];
    if (colsVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
      return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
          ERROR_SCAN_INDEX_MISSING_COLUMNS,
          std::string(rdrsErrorMessage(ERROR_SCAN_INDEX_MISSING_COLUMNS))).status;
    }
    if (colsVal.error() != simdjson::SUCCESS) {
      return handle_simdjson_error(colsVal.error(), doc,
                                   currentLocation);
    }

    simdjson::ondemand::array cols;
    if (colsVal.get(cols) != simdjson::SUCCESS) {
      return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
          ERROR_SCAN_INDEX_COLUMNS_NOT_ARRAY,
          std::string(rdrsErrorMessage(ERROR_SCAN_INDEX_COLUMNS_NOT_ARRAY))).status;
    }

    for (auto c : cols) {
      std::string_view sv;
      if (c.get(sv) != simdjson::SUCCESS) {
        return CRS_Status(static_cast<HTTP_CODE>(
              drogon::HttpStatusCode::k400BadRequest),
            ERROR_SCAN_INDEX_COLUMN_INVALID,
            std::string(rdrsErrorMessage(ERROR_SCAN_INDEX_COLUMN_INVALID))).status;
      }
      index.columns.emplace_back(sv);
    }

    if (index.columns.empty()) {
      return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
          ERROR_SCAN_INDEX_COLUMNS_EMPTY,
          std::string(rdrsErrorMessage(ERROR_SCAN_INDEX_COLUMNS_EMPTY))).status;
    }
  }

  // ---- order (optional) ----
  {
    auto ordVal = indexObj["order"];
    if (ordVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    } else if (ordVal.error() != simdjson::SUCCESS) {
      return handle_simdjson_error(ordVal.error(), doc,
                                   currentLocation);
    } else {
      std::string_view order;
      if (ordVal.get(order) != simdjson::SUCCESS) {
        return CRS_Status(static_cast<HTTP_CODE>(
              drogon::HttpStatusCode::k400BadRequest),
            ERROR_SCAN_INDEX_ORDER_INVALID,
            std::string(rdrsErrorMessage(ERROR_SCAN_INDEX_ORDER_INVALID))).status;
      }
      if (order == "asc") {
        index.order = IndexScanParams::Order::ASC;
      } else if (order == "desc") {
        index.order = IndexScanParams::Order::DESC;
      } else {
        return CRS_Status(static_cast<HTTP_CODE>(
              drogon::HttpStatusCode::k400BadRequest),
            ERROR_SCAN_INDEX_ORDER_INVALID,
            std::string(rdrsErrorMessage(ERROR_SCAN_INDEX_ORDER_INVALID))).status;
      }
    }
  }

  // ---- ranges ----
  {
    auto rangesVal = indexObj["ranges"];
    if (rangesVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
      return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
          ERROR_SCAN_INDEX_MISSING_RANGES,
          std::string(rdrsErrorMessage(ERROR_SCAN_INDEX_MISSING_RANGES))).status;
    }
    if (rangesVal.error() != simdjson::SUCCESS) {
      return handle_simdjson_error(rangesVal.error(), doc,
                                   currentLocation);
    }

    simdjson::ondemand::array ranges;
    if (rangesVal.get(ranges) != simdjson::SUCCESS) {
      return CRS_Status(static_cast<HTTP_CODE>(
            drogon::HttpStatusCode::k400BadRequest),
          ERROR_SCAN_INDEX_RANGES_NOT_ARRAY,
          std::string(rdrsErrorMessage(ERROR_SCAN_INDEX_RANGES_NOT_ARRAY))).status;
    }

    for (auto r : ranges) {
      simdjson::ondemand::object rangeObj;
      if (r.get(rangeObj) != simdjson::SUCCESS) {
        return CRS_Status(static_cast<HTTP_CODE>(
              drogon::HttpStatusCode::k400BadRequest),
            ERROR_SCAN_INDEX_RANGE_INVALID,
            std::string(rdrsErrorMessage(ERROR_SCAN_INDEX_RANGE_INVALID))).status;
      }

      IndexRange range;

      // ---- lower / upper ----
      for (auto side : {"lower", "upper"}) {
        auto boundVal = rangeObj[side];
        if (boundVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          continue;
        }
        if (boundVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(boundVal.error(), doc,
                                       currentLocation);
        }

        simdjson::ondemand::object boundObj;
        if (boundVal.get(boundObj) != simdjson::SUCCESS) {
          return CRS_Status(static_cast<HTTP_CODE>(
                drogon::HttpStatusCode::k400BadRequest),
              ERROR_SCAN_INDEX_BOUND_INVALID,
              std::string(rdrsErrorMessage(ERROR_SCAN_INDEX_BOUND_INVALID))).status;
        }

        IndexBound bound;

        // values
        auto valuesVal = boundObj["values"];
        if (valuesVal.error() != simdjson::SUCCESS) {
          return CRS_Status(static_cast<HTTP_CODE>(
                drogon::HttpStatusCode::k400BadRequest),
              ERROR_SCAN_INDEX_BOUND_MISSING_VALUES,
              std::string(rdrsErrorMessage(ERROR_SCAN_INDEX_BOUND_MISSING_VALUES))).status;
        }

        simdjson::ondemand::array values;
        if (valuesVal.get(values) != simdjson::SUCCESS) {
          return CRS_Status(static_cast<HTTP_CODE>(
                drogon::HttpStatusCode::k400BadRequest),
              ERROR_SCAN_INDEX_BOUND_VALUES_NOT_ARRAY,
              std::string(rdrsErrorMessage(ERROR_SCAN_INDEX_BOUND_VALUES_NOT_ARRAY))).status;
        }

        for (auto v : values) {
          BoundNode bound_node;
          auto st = ParseJsonValue(v, bound_node.value, doc);
          if (st.http_code != HTTP_CODE::SUCCESS) {
            return st;
          }
          bound.values.emplace_back(std::move(bound_node));
        }

        // inclusive (optional)
        auto incVal = boundObj["inclusive"];
        if (incVal.error() == simdjson::SUCCESS) {
          bool inc;
          if (incVal.get(inc) != simdjson::SUCCESS) {
            return CRS_Status(static_cast<HTTP_CODE>(
                  drogon::HttpStatusCode::k400BadRequest),
                ERROR_SCAN_INDEX_BOUND_INCLUSIVE_INVALID,
                std::string(rdrsErrorMessage(ERROR_SCAN_INDEX_BOUND_INCLUSIVE_INVALID))).status;
          }
          bound.inclusive = inc;
        }

        if (strcmp(side, "lower") == 0) {
          range.lower = std::move(bound);
        } else {
          range.upper = std::move(bound);
        }
      }

      // Check ranges limit before adding
      if (index.ranges.size() >= MAX_SCAN_RANGES) {
        return CRS_Status(static_cast<HTTP_CODE>(
              drogon::HttpStatusCode::k400BadRequest),
            ERROR_SCAN_TOO_MANY_RANGES,
            std::string(rdrsErrorMessage(ERROR_SCAN_TOO_MANY_RANGES))).status;
      }
      index.ranges.emplace_back(std::move(range));
    }
  }

  out.index = std::move(index);
  return CRS_Status().status;
}

RS_Status JSONParser::scan_parse(simdjson::padded_string_view reqBody,
                                  ScanReadParams& reqStruct) {
  const char *currentLocation = nullptr;

  simdjson::error_code error = parser.iterate(reqBody).get(doc);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }

  simdjson::ondemand::object reqObject;
  error = doc.get_object().get(reqObject);
  if (unlikely(error != simdjson::SUCCESS)) {
    return handle_simdjson_error(error, doc, currentLocation);
  }

  auto& bodyObject = reqObject;

  int64_t limit = -1;
  auto limitVal = bodyObject[LIMIT];
  if (unlikely(limitVal.error() ==
        simdjson::error_code::NO_SUCH_FIELD)) {
    return CRS_Status(static_cast<HTTP_CODE>(
          drogon::HttpStatusCode::k400BadRequest),
        ERROR_SCAN_MISSING_LIMIT, std::string(rdrsErrorMessage(ERROR_SCAN_MISSING_LIMIT))).status;
  } else if (unlikely(limitVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(
        limitVal.error(), doc, currentLocation);
  } else if (unlikely(limitVal.is_null())) {
    return CRS_Status(static_cast<HTTP_CODE>(
          drogon::HttpStatusCode::k400BadRequest),
        ERROR_SCAN_MISSING_LIMIT, std::string(rdrsErrorMessage(ERROR_SCAN_MISSING_LIMIT))).status;
  } else {
    error = limitVal.get(limit);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(error, doc, currentLocation);
    }
  }
  reqStruct.limit = limit;

  simdjson::ondemand::array readColumns;
  auto readColumnsVal = bodyObject[READCOLUMNS];
  if (unlikely(readColumnsVal.error() ==
        simdjson::error_code::NO_SUCH_FIELD)) {
    readColumns = {};
  } else if (unlikely(readColumnsVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(
        readColumnsVal.error(), doc, currentLocation);
  } else if (unlikely(readColumnsVal.is_null())) {
    readColumns = {};
  } else {
    error = readColumnsVal.get(readColumns);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(error, doc, currentLocation);
    } else if (unlikely(readColumns.is_empty())) {
      readColumns = {};
    } else {
      for (auto readColumn : readColumns) {
        ScanReadColumn ScanReadColumn;
        simdjson::ondemand::object readColumnObj;
        error = readColumn.get(readColumnObj);
        if (unlikely(error != simdjson::SUCCESS)) {
          return handle_simdjson_error(error, doc, currentLocation);
        }
        std::string_view column;
        auto columnVal = readColumnObj[COLUMN];
        if (unlikely(columnVal.error() ==
              simdjson::error_code::NO_SUCH_FIELD)) {
          return CRS_Status(
              HTTP_CODE::CLIENT_ERROR,
              "a column to read is missing a name").status;
        } else if (unlikely(columnVal.error() != simdjson::SUCCESS)) {
          return handle_simdjson_error(
              columnVal.error(), doc, currentLocation);
        } else if (unlikely(columnVal.is_null())) {
          return CRS_Status(
              HTTP_CODE::CLIENT_ERROR,
              "a column to read is missing a name").status;
        } else {
          error = columnVal.get(column);
          if (unlikely(error != simdjson::SUCCESS)) {
            return handle_simdjson_error(error, doc, currentLocation);
          }
        }
        if (unlikely(column.size() == 0)) {
          return CRS_Status(
              HTTP_CODE::CLIENT_ERROR,
              "a column to read is missing a name").status;
        }
        ScanReadColumn.column = column;
        std::string_view dataReturnType;
        auto dataReturnTypeVal = readColumnObj[DATA_RETURN_TYPE];
        if (unlikely(dataReturnTypeVal.error() ==
              simdjson::error_code::NO_SUCH_FIELD)) {
          dataReturnType = "";
        } else if (unlikely(dataReturnTypeVal.error() != simdjson::SUCCESS)) {
          return handle_simdjson_error(dataReturnTypeVal.error(), doc,
              currentLocation);
        } else if (unlikely(dataReturnTypeVal.is_null())) {
          dataReturnType = "";
        } else {
          error = dataReturnTypeVal.get(dataReturnType);
          if (unlikely(error != simdjson::SUCCESS)) {
            return handle_simdjson_error(error, doc, currentLocation);
          }
        }
        reqStruct.readColumns.emplace_back(ScanReadColumn);
      }
    }
  }

  simdjson::ondemand::object filtersObject;
  auto filtersVal = bodyObject[FILTERS];
  if (unlikely(filtersVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
  } else if (unlikely(filtersVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(filtersVal.error(), doc, currentLocation);
  } else if (unlikely(filtersVal.is_null())) {
  } else {
    error = filtersVal.get(filtersObject);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(error, doc, currentLocation);
    }
    if (unlikely(filtersObject.is_empty())) {
    } else {
      std::string err = "";
      std::shared_ptr<FilterNode> filters;
      RS_Status ret = parseScanFilter(doc, filtersObject, filters, err);
      if (ret.http_code != HTTP_CODE::SUCCESS) {
        return ret;
      }
      reqStruct.filterRoot = filters;
    }
  }

  simdjson::ondemand::object indexObject;
  auto indexVal = bodyObject[INDEX];
  if (unlikely(indexVal.error() == simdjson::error_code::NO_SUCH_FIELD)) {
  } else if (unlikely(indexVal.error() != simdjson::SUCCESS)) {
    return handle_simdjson_error(indexVal.error(), doc, currentLocation);
  } else if (unlikely(indexVal.is_null())) {
  } else {
    error = indexVal.get(indexObject);
    if (unlikely(error != simdjson::SUCCESS)) {
      return handle_simdjson_error(error, doc, currentLocation);
    }
    if (unlikely(indexObject.is_empty())) {
    } else {
      std::string err = "";
      RS_Status ret = parseScanIndex(doc, indexObject, reqStruct);
      if (ret.http_code != HTTP_CODE::SUCCESS) {
        return ret;
      }
    }
  }
  DEB_SCAN_BLOCK(
    std::cout << std::endl;
    std::cout << ">>>>>> Parsing LOGICAL Scan Filter: " << std::endl;
    reqStruct.DumpFilters(reqStruct.filterRoot);
    std::cout << "<<<<<<" << std::endl;
    std::cout << std::endl;
    std::cout << ">>>>>> Parsing LOGICAL Scan Index: " << std::endl;
    reqStruct.DumpIndex();
    std::cout << "<<<<<<" << std::endl;
    std::cout << std::endl;
  );

  return CRS_Status::SUCCESS.status;
}
