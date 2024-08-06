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

#include "json_parser.hpp"
#include "feature_store_data_structs.hpp"
#include "pk_data_structs.hpp"
#include "constants.hpp"
#include "error_strings.h"
#include "config_structs.hpp"
#include "rdrs_dal.hpp"

#include <cstdint>
#include <cstddef>
#include <functional>
#include <memory>
#include <simdjson.h>

/*
 * Parsing utilities
 */

class ConfigParseError : public std::runtime_error {
public:
  ConfigParseError(std::string message) : std::runtime_error("ConfigParseError"), m_error_message(message) {}
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
  static inline bool parse(TargetDatatype& target, ValueDatatype& value) __VA_ARGS__ \
  static inline bool parse(TargetDatatype& target, ValueDatatype&& value) __VA_ARGS__

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
// intended to be used for comments. All other keys will cause an exception. The
// body contains one or more ELEMENT(structMemberVariableName, JsonKey) without
// terminating semicolon.
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
                             const simdjson::padded_string& paddedJson,
                             const simdjson::ondemand::document& doc) {
  std::string message = e.m_error_message;
  const char *location = nullptr;
  simdjson::error_code getLocationError = doc.current_location().get(location);
  if (getLocationError == simdjson::SUCCESS) {
    const char* bufferC = paddedJson.data();
    const size_t bufferLength = paddedJson.length();
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
    message += " before/at line " + std::to_string(line) + ", column " + std::to_string(column);
  }
  return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                    message)
      .status;
}

/*
 * End of parsing utilities
 */

JSONParser jsonParser;

JSONParser::JSONParser() {
  // Allocate a buffer for each thread
  for (int i = 0; i < DEFAULT_NUM_THREADS; ++i) {
    buffers[i] = std::make_unique<char[]>(REQ_BUFFER_SIZE + simdjson::SIMDJSON_PADDING);
  }
}

std::unique_ptr<char[]> &JSONParser::get_buffer(size_t threadId) {
  return buffers[threadId];
}

RS_Status extract_db_and_table(const std::string &, std::string &, std::string &);
RS_Status handle_simdjson_error(const simdjson::error_code &, simdjson::ondemand::document &,
                                const char *&);

// Is used to perform a primary key read operation.
RS_Status JSONParser::pk_parse(size_t threadId, simdjson::padded_string_view reqBody,
                               PKReadParams &reqStruct) {
  const char *currentLocation = nullptr;

  simdjson::error_code error = parser[threadId].iterate(reqBody).get(doc[threadId]);
  if (error != simdjson::SUCCESS) {
    return handle_simdjson_error(error, doc[threadId], currentLocation);
  }

  simdjson::ondemand::object reqObject;
  error = doc[threadId].get_object().get(reqObject);
  if (error != simdjson::SUCCESS) {
    return handle_simdjson_error(error, doc[threadId], currentLocation);
  }

  simdjson::ondemand::array filters;

  auto filtersVal = reqObject[FILTERS];
  if (filtersVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    filters = {};
  } else if (filtersVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(filtersVal.error(), doc[threadId], currentLocation);
  } else {
    if (filtersVal.is_null()) {
      filters = {};
    } else {
      error = filtersVal.get(filters);
      if (error != simdjson::SUCCESS) {
        return handle_simdjson_error(error, doc[threadId], currentLocation);
      }
      for (auto filter : filters) {
        PKReadFilter pkReadFilter;
        simdjson::ondemand::object filterObj;
        error = filter.get(filterObj);
        if (error != simdjson::SUCCESS) {
          return handle_simdjson_error(error, doc[threadId], currentLocation);
        }

        std::string_view column;
        auto columnVal = filterObj[COLUMN];
        if (columnVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          column = "";
        } else if (columnVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(columnVal.error(), doc[threadId], currentLocation);
        } else {
          if (columnVal.is_null()) {
            column = "";
          } else {
            error = columnVal.get(column);
            if (error != simdjson::SUCCESS) {
              return handle_simdjson_error(error, doc[threadId], currentLocation);
            }
          }
        }
        pkReadFilter.column = column;

        simdjson::ondemand::value value;
        std::vector<char> bytes;
        auto valueVal = filterObj[VALUE];
        if (valueVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          bytes = {};
        } else if (valueVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(valueVal.error(), doc[threadId], currentLocation);
        } else {
          if (valueVal.is_null()) {
            bytes = {};
          } else {
            error = valueVal.get(value);
            if (error != simdjson::SUCCESS) {
              return handle_simdjson_error(error, doc[threadId], currentLocation);
            }
            std::ostringstream oss;
            oss << value;
            std::string valueJson = oss.str();
            bytes                 = std::vector<char>(valueJson.begin(), valueJson.end());
          }
        }
        pkReadFilter.value = bytes;

        reqStruct.filters.emplace_back(pkReadFilter);
      }
    }
  }

  simdjson::ondemand::array readColumns;

  auto readColumnsVal = reqObject[READCOLUMNS];
  if (readColumnsVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    readColumns = {};
  } else if (readColumnsVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(readColumnsVal.error(), doc[threadId], currentLocation);
  } else {
    if (readColumnsVal.is_null()) {
      readColumns = {};
    } else {
      error = readColumnsVal.get(readColumns);
      if (error != simdjson::SUCCESS) {
        return handle_simdjson_error(error, doc[threadId], currentLocation);
      }

      for (auto readColumn : readColumns) {
        PKReadReadColumn pkReadReadColumn;
        simdjson::ondemand::object readColumnObj;
        error = readColumn.get(readColumnObj);
        if (error != simdjson::SUCCESS) {
          return handle_simdjson_error(error, doc[threadId], currentLocation);
        }

        std::string_view column;
        auto columnVal = readColumnObj[COLUMN];
        if (columnVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          column = "";
        } else if (columnVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(columnVal.error(), doc[threadId], currentLocation);
        } else {
          if (columnVal.is_null()) {
            column = "";
          } else {
            error = columnVal.get(column);
            if (error != simdjson::SUCCESS) {
              return handle_simdjson_error(error, doc[threadId], currentLocation);
            }
          }
        }
        pkReadReadColumn.column = column;

        std::string_view dataReturnType;
        auto dataReturnTypeVal = readColumnObj[DATA_RETURN_TYPE];
        if (dataReturnTypeVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          dataReturnType = "";
        } else if (dataReturnTypeVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(dataReturnTypeVal.error(), doc[threadId], currentLocation);
        } else {
          if (dataReturnTypeVal.is_null()) {
            dataReturnType = "";
          } else {
            error = dataReturnTypeVal.get(dataReturnType);
            if (error != simdjson::SUCCESS) {
              return handle_simdjson_error(error, doc[threadId], currentLocation);
            }
          }
        }

        pkReadReadColumn.returnType = dataReturnType;

        reqStruct.readColumns.emplace_back(pkReadReadColumn);
      }
    }
  }

  std::string_view operationId;

  auto operationIdVal = reqObject[OPERATION_ID];
  if (operationIdVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    operationId = "";
  } else if (operationIdVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(operationIdVal.error(), doc[threadId], currentLocation);
  } else {
    if (operationIdVal.is_null()) {
      operationId = "";
    } else {
      error = operationIdVal.get(operationId);
      if (error != simdjson::SUCCESS) {
        return handle_simdjson_error(error, doc[threadId], currentLocation);
      }
    }
  }
  reqStruct.operationId = operationId;

  return CRS_Status::SUCCESS.status;
}

// This is used to perform batched primary key read operations.
// The body here is a list of arbitrary pk-reads under the key operations:
RS_Status JSONParser::batch_parse(size_t threadId, simdjson::padded_string_view reqBody,
                                  std::vector<PKReadParams> &reqStructs) {
  const char *currentLocation = nullptr;

  simdjson::error_code error = parser[threadId].iterate(reqBody).get(doc[threadId]);
  if (error != simdjson::SUCCESS) {
    return handle_simdjson_error(error, doc[threadId], currentLocation);
  }

  simdjson::ondemand::object reqObject;
  error = doc[threadId].get_object().get(reqObject);
  if (error != simdjson::SUCCESS) {
    return handle_simdjson_error(error, doc[threadId], currentLocation);
  }

  simdjson::ondemand::array operations;
  auto operationsVal = reqObject[OPERATIONS];
  if (operationsVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    operations = {};
  } else if (operationsVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(operationsVal.error(), doc[threadId], currentLocation);
  } else {
    if (operationsVal.is_null()) {
      operations = {};
    } else {
      error = operationsVal.get(operations);
      if (error != simdjson::SUCCESS) {
        return handle_simdjson_error(error, doc[threadId], currentLocation);
      }
      for (auto operation : operations) {
        PKReadParams reqStruct;
        simdjson::ondemand::object operationObj;
        error = operation.get(operationObj);
        if (error != simdjson::SUCCESS) {
          return handle_simdjson_error(error, doc[threadId], currentLocation);
        }

        std::string_view method;
        auto methodVal = operationObj[METHOD];
        if (methodVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          method = "";
        } else if (methodVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(methodVal.error(), doc[threadId], currentLocation);
        } else {
          if (methodVal.is_null()) {
            method = "";
          } else {
            error = methodVal.get(method);
            if (error != simdjson::SUCCESS) {
              return handle_simdjson_error(error, doc[threadId], currentLocation);
            }
          }
        }
        reqStruct.method = method;

        std::string_view relativeUrl;
        auto relativeUrlVal = operationObj[RELATIVE_URL];
        if (relativeUrlVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          relativeUrl = "";
        } else if (relativeUrlVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(relativeUrlVal.error(), doc[threadId], currentLocation);
        } else {
          if (relativeUrlVal.is_null()) {
            relativeUrl = "";
          } else {
            error = relativeUrlVal.get(relativeUrl);
            if (error != simdjson::SUCCESS) {
              return handle_simdjson_error(error, doc[threadId], currentLocation);
            }
          }
        }

        RS_Status status =
            extract_db_and_table(std::string(relativeUrl), reqStruct.path.db, reqStruct.path.table);
        if (static_cast<drogon::HttpStatusCode>(status.http_code) !=
            drogon::HttpStatusCode::k200OK) {
          return status;
        }

        simdjson::ondemand::object bodyObject;
        auto bodyVal = operationObj[BODY];
        if (bodyVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                            ERROR_CODE_INVALID_BODY, ERROR_064)
              .status;
        }
        if (bodyVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(bodyVal.error(), doc[threadId], currentLocation);
        } else {
          if (bodyVal.is_null()) {
            return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                              ERROR_CODE_INVALID_BODY, ERROR_064)
                .status;
          }
          error = bodyVal.get(bodyObject);
          if (error != simdjson::SUCCESS) {
            return handle_simdjson_error(error, doc[threadId], currentLocation);
          }

          simdjson::ondemand::array filters;

          auto filtersVal = bodyObject[FILTERS];
          if (filtersVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
            filters = {};
          } else if (filtersVal.error() != simdjson::SUCCESS) {
            return handle_simdjson_error(filtersVal.error(), doc[threadId], currentLocation);
          } else {
            if (filtersVal.is_null()) {
              filters = {};
            } else {
              error = filtersVal.get(filters);
              if (error != simdjson::SUCCESS) {
                return handle_simdjson_error(error, doc[threadId], currentLocation);
              }
              for (auto filter : filters) {
                PKReadFilter pkReadFilter;
                simdjson::ondemand::object filterObj;
                error = filter.get(filterObj);
                if (error != simdjson::SUCCESS) {
                  return handle_simdjson_error(error, doc[threadId], currentLocation);
                }

                std::string_view column;
                auto columnVal = filterObj[COLUMN];
                if (columnVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
                  column = "";
                } else if (columnVal.error() != simdjson::SUCCESS) {
                  return handle_simdjson_error(columnVal.error(), doc[threadId], currentLocation);
                } else {
                  if (columnVal.is_null()) {
                    column = "";
                  } else {
                    error = columnVal.get(column);
                    if (error != simdjson::SUCCESS) {
                      return handle_simdjson_error(error, doc[threadId], currentLocation);
                    }
                  }
                }
                pkReadFilter.column = column;

                simdjson::ondemand::value value;
                std::vector<char> bytes;
                auto valueVal = filterObj[VALUE];
                if (valueVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
                  bytes = {};
                } else if (valueVal.error() != simdjson::SUCCESS) {
                  return handle_simdjson_error(valueVal.error(), doc[threadId], currentLocation);
                } else {
                  if (valueVal.is_null()) {
                    bytes = {};
                  } else {
                    error = valueVal.get(value);
                    if (error != simdjson::SUCCESS) {
                      return handle_simdjson_error(error, doc[threadId], currentLocation);
                    }
                    std::ostringstream oss;
                    oss << value;
                    std::string valueJson = oss.str();
                    bytes                 = std::vector<char>(valueJson.begin(), valueJson.end());
                  }
                }
                pkReadFilter.value = bytes;

                reqStruct.filters.emplace_back(pkReadFilter);
              }
            }
          }

          simdjson::ondemand::array readColumns;

          auto readColumnsVal = bodyObject[READCOLUMNS];
          if (readColumnsVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
            readColumns = {};
          } else if (readColumnsVal.error() != simdjson::SUCCESS) {
            return handle_simdjson_error(readColumnsVal.error(), doc[threadId], currentLocation);
          } else {
            if (readColumnsVal.is_null()) {
              readColumns = {};
            } else {
              error = readColumnsVal.get(readColumns);
              if (error != simdjson::SUCCESS) {
                return handle_simdjson_error(error, doc[threadId], currentLocation);
              }

              for (auto readColumn : readColumns) {
                PKReadReadColumn pkReadReadColumn;
                simdjson::ondemand::object readColumnObj;
                error = readColumn.get(readColumnObj);
                if (error != simdjson::SUCCESS) {
                  return handle_simdjson_error(error, doc[threadId], currentLocation);
                }

                std::string_view column;
                auto columnVal = readColumnObj[COLUMN];
                if (columnVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
                  column = "";
                } else if (columnVal.error() != simdjson::SUCCESS) {
                  return handle_simdjson_error(columnVal.error(), doc[threadId], currentLocation);
                } else {
                  if (columnVal.is_null()) {
                    column = "";
                  } else {
                    error = columnVal.get(column);
                    if (error != simdjson::SUCCESS) {
                      return handle_simdjson_error(error, doc[threadId], currentLocation);
                    }
                  }
                }
                pkReadReadColumn.column = column;

                std::string_view dataReturnType;
                auto dataReturnTypeVal = readColumnObj[DATA_RETURN_TYPE];
                if (dataReturnTypeVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
                  dataReturnType = "";
                } else if (dataReturnTypeVal.error() != simdjson::SUCCESS) {
                  return handle_simdjson_error(dataReturnTypeVal.error(), doc[threadId],
                                               currentLocation);
                } else {
                  if (dataReturnTypeVal.is_null()) {
                    dataReturnType = "";
                  } else {
                    error = dataReturnTypeVal.get(dataReturnType);
                    if (error != simdjson::SUCCESS) {
                      return handle_simdjson_error(error, doc[threadId], currentLocation);
                    }
                  }
                }

                pkReadReadColumn.returnType = dataReturnType;

                reqStruct.readColumns.emplace_back(pkReadReadColumn);
              }
            }
          }

          std::string_view operationId;

          auto operationIdVal = bodyObject[OPERATION_ID];
          if (operationIdVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
            operationId = "";
          } else if (operationIdVal.error() != simdjson::SUCCESS) {
            return handle_simdjson_error(operationIdVal.error(), doc[threadId], currentLocation);
          } else {
            if (operationIdVal.is_null()) {
              operationId = "";
            } else {
              error = operationIdVal.get(operationId);
              if (error != simdjson::SUCCESS) {
                return handle_simdjson_error(error, doc[threadId], currentLocation);
              }
            }
          }
          reqStruct.operationId = operationId;
          reqStructs.push_back(reqStruct);
        }
      }
    }
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

DEFINE_VALUE_PARSER(uint16_t, {
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

DEFINE_VALUE_PARSER(uint32_t, {
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
 * Parsers for the config structs. Make sure these correspond exactly to the
 * DEFINE_STRUCT_PRINTER declarations in json_printer.cpp.
 */

DEFINE_STRUCT_PARSER(Internal,
                     ELEMENT(reqBufferSize,       ReqBufferSize)
                     ELEMENT(respBufferSize,      RespBufferSize)
                     ELEMENT(preAllocatedBuffers, PreAllocatedBuffers)
                     ELEMENT(batchMaxSize,        BatchMaxSize)
                     ELEMENT(operationIdMaxSize,  OperationIDMaxSize)
                     )

DEFINE_STRUCT_PARSER(REST,
                     ELEMENT(enable,     Enable)
                     ELEMENT(serverIP,   ServerIP)
                     ELEMENT(serverPort, ServerPort)
                     )

DEFINE_STRUCT_PARSER(GRPC,
                     ELEMENT(enable,     Enable)
                     ELEMENT(serverIP,   ServerIP)
                     ELEMENT(serverPort, ServerPort)
                     )

DEFINE_STRUCT_PARSER(Mgmd,
                     ELEMENT(IP,   IP)
                     ELEMENT(port, Port)
                     )

DEFINE_ARRAY_PARSER(Mgmd)

DEFINE_ARRAY_PARSER(uint32_t)

DEFINE_STRUCT_PARSER(RonDB,
                     ELEMENT(Mgmds,                         Mgmds)
                     ELEMENT(connectionPoolSize,            ConnectionPoolSize)
                     ELEMENT(nodeIDs,                       NodeIDs)
                     ELEMENT(connectionRetries,             ConnectionRetries)
                     ELEMENT(connectionRetryDelayInSec,     ConnectionRetryDelayInSec)
                     ELEMENT(opRetryOnTransientErrorsCount, OpRetryOnTransientErrorsCount)
                     ELEMENT(opRetryInitialDelayInMS,       OpRetryInitialDelayInMS)
                     ELEMENT(opRetryJitterInMS,             OpRetryJitterInMS)
                     )

DEFINE_STRUCT_PARSER(TestParameters,
                     ELEMENT(clientCertFile, ClientCertFile)
                     ELEMENT(clientKeyFile,  ClientKeyFile)
                     )

DEFINE_STRUCT_PARSER(TLS,
                     ELEMENT(enableTLS,                  EnableTLS)
                     ELEMENT(requireAndVerifyClientCert, RequireAndVerifyClientCert)
                     ELEMENT(certificateFile,            CertificateFile)
                     ELEMENT(privateKeyFile,             PrivateKeyFile)
                     ELEMENT(rootCACertFile,             RootCACertFile)
                     ELEMENT(testParameters,             TestParameters)
                     )

DEFINE_STRUCT_PARSER(APIKey,
                     ELEMENT(useHopsworksAPIKeys,          UseHopsworksAPIKeys)
                     ELEMENT(cacheRefreshIntervalMS,       CacheRefreshIntervalMS)
                     ELEMENT(cacheUnusedEntriesEvictionMS, CacheUnusedEntriesEvictionMS)
                     ELEMENT(cacheRefreshIntervalJitterMS, CacheRefreshIntervalJitterMS)
                     )

DEFINE_STRUCT_PARSER(Security,
                     ELEMENT(tls,    TLS)
                     ELEMENT(apiKey, APIKey)
                     )

DEFINE_STRUCT_PARSER(LogConfig,
                     ELEMENT(level,      Level)
                     ELEMENT(filePath,   FilePath)
                     ELEMENT(maxSizeMb,  MaxSizeMB)
                     ELEMENT(maxBackups, MaxBackups)
                     ELEMENT(maxAge,     MaxAge)
                     )

DEFINE_STRUCT_PARSER(MySQLServer,
                     ELEMENT(IP,   IP)
                     ELEMENT(port, Port)
                     )

DEFINE_ARRAY_PARSER(MySQLServer)

DEFINE_STRUCT_PARSER(MySQL,
                     ELEMENT(servers,  Servers)
                     ELEMENT(user,     User)
                     ELEMENT(password, Password)
                     )

DEFINE_STRUCT_PARSER(Testing,
                     ELEMENT(mySQL,                MySQL)
                     ELEMENT(mySQLMetadataCluster, MySQLMetadataCluster)
                     )

DEFINE_STRUCT_PARSER(AllConfigs,
                     ELEMENT(internal,             Internal)
                     ELEMENT(rest,                 REST)
                     ELEMENT(grpc,                 GRPC)
                     ELEMENT(pidfile,              PIDFile)
                     ELEMENT(ronDB,                RonDB)
                     ELEMENT(ronDbMetaDataCluster, RonDBMetadataCluster)
                     ELEMENT(security,             Security)
                     ELEMENT(log,                  Log)
                     ELEMENT(testing,              Testing)
                     )

RS_Status JSONParser::config_parse(const std::string &configsBody, AllConfigs &configsStruct) noexcept {
  simdjson::padded_string paddedJson(configsBody);
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc;
  try {
    try {
      doc = parser.iterate(paddedJson).value();
      parse(configsStruct, doc.get_object());
      assert_end_of_doc(doc);
    }
    catch (simdjson::simdjson_error& e) {
      throw ConfigParseError(simdjson::error_message(e.error()));
    }
  }
  catch (ConfigParseError& e) {
    return handle_parse_error(e, paddedJson, doc);
  }
  return CRS_Status::SUCCESS.status;
}

// todo-ronsql Should we conform to the rest of RDRS by capitalizing these JSON keys?
DEFINE_STRUCT_PARSER(RonSQLParams,
                     ELEMENT(query,        query)
                     ELEMENT(database,     database)
                     ELEMENT(explainMode,  explainMode)
                     ELEMENT(outputFormat, outputFormat)
                     ELEMENT(operationId,  operationId)
                     )

RS_Status JSONParser::ronsql_parse(size_t threadId, simdjson::padded_string_view reqBody,
                                   RonSQLParams &reqStruct) {
  try {
    try {
      doc[threadId] = parser[threadId].iterate(reqBody).value();
      parse(reqStruct, doc[threadId].get_object());
      assert_end_of_doc(doc[threadId]);
    }
    catch (simdjson::simdjson_error& e) {
      throw ConfigParseError(simdjson::error_message(e.error()));
    }
  }
  catch (ConfigParseError& e) {
    return handle_parse_error(e, reqBody, doc[threadId]);
  }
  return CRS_Status::SUCCESS.status;
}

RS_Status
JSONParser::feature_store_parse(size_t threadId, simdjson::padded_string_view reqBody,
                                feature_store_data_structs::FeatureStoreRequest &reqStruct) {
  const char *currentLocation = nullptr;

  simdjson::error_code error = parser[threadId].iterate(reqBody).get(doc[threadId]);
  if (error != simdjson::SUCCESS) {
    return handle_simdjson_error(error, doc[threadId], currentLocation);
  }

  simdjson::ondemand::object reqObject;
  error = doc[threadId].get_object().get(reqObject);
  if (error != simdjson::SUCCESS) {
    return handle_simdjson_error(error, doc[threadId], currentLocation);
  }

  std::string_view featureStoreName;
  auto featureStoreNameVal = reqObject[FEATURE_STORE_NAME];
  if (featureStoreNameVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_BODY,
                      std::string(ERROR_064) + " " + std::string(FEATURE_STORE_NAME))
        .status;
  }
  if (featureStoreNameVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(featureStoreNameVal.error(), doc[threadId], currentLocation);
  }
  if (featureStoreNameVal.is_null()) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_BODY,
                      std::string(ERROR_064) + " " + std::string(FEATURE_STORE_NAME))
        .status;
  }
  error = featureStoreNameVal.get(featureStoreName);
  if (error != simdjson::SUCCESS) {
    return handle_simdjson_error(error, doc[threadId], currentLocation);
  }

  reqStruct.featureStoreName = featureStoreName;

  std::string_view featureViewName;
  auto featureViewNameVal = reqObject[FEATURE_VIEW_NAME];
  if (featureViewNameVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_BODY,
                      std::string(ERROR_064) + " " + std::string(FEATURE_VIEW_NAME))
        .status;
  }
  if (featureViewNameVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(featureViewNameVal.error(), doc[threadId], currentLocation);
  }
  if (featureViewNameVal.is_null()) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_BODY,
                      std::string(ERROR_064) + " " + std::string(FEATURE_VIEW_NAME))
        .status;
  }
  error = featureViewNameVal.get(featureViewName);
  if (error != simdjson::SUCCESS) {
    return handle_simdjson_error(error, doc[threadId], currentLocation);
  }

  reqStruct.featureViewName = featureViewName;

  uint64_t featureViewVersion = 0;
  auto featureViewVersionVal  = reqObject[FEATURE_VIEW_VERSION];
  if (featureViewVersionVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_BODY,
                      std::string(ERROR_064) + " " + std::string(FEATURE_VIEW_VERSION))
        .status;
  }
  if (featureViewVersionVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(featureViewVersionVal.error(), doc[threadId], currentLocation);
  }
  if (featureViewVersionVal.is_null()) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_BODY,
                      std::string(ERROR_064) + " " + std::string(FEATURE_VIEW_VERSION))
        .status;
  }
  error = featureViewVersionVal.get(featureViewVersion);
  if (error != simdjson::SUCCESS) {
    return handle_simdjson_error(error, doc[threadId], currentLocation);
  }

  reqStruct.featureViewVersion = featureViewVersion;

  simdjson::ondemand::object passedFeatures;  // Optional
  auto passedFeaturesVal = reqObject[PASSED_FEATURES];
  if (passedFeaturesVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
  } else if (passedFeaturesVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(passedFeaturesVal.error(), doc[threadId], currentLocation);
  } else {
    if (!passedFeaturesVal.is_null()) {
      error = passedFeaturesVal.get(passedFeatures);
      if (error != simdjson::SUCCESS) {
        return handle_simdjson_error(error, doc[threadId], currentLocation);
      }
      // Map of feature name as key and feature value as value.
      // This overwrites feature values in the response.
      for (auto feature : passedFeatures) {
        std::string_view featureName = feature.unescaped_key();
        simdjson::ondemand::value value;
        std::vector<char> bytes;
        auto valueVal = feature.value();
        if (valueVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                            ERROR_CODE_INVALID_BODY,
                            std::string(ERROR_064) + " " + std::string(featureName))
              .status;
        }
        if (valueVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(valueVal.error(), doc[threadId], currentLocation);
        }
        if (valueVal.is_null()) {
          return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                            ERROR_CODE_INVALID_BODY,
                            std::string(ERROR_064) + " " + std::string(featureName))
              .status;
        }
        error = valueVal.get(value);
        if (error != simdjson::SUCCESS) {
          return handle_simdjson_error(error, doc[threadId], currentLocation);
        }
        std::ostringstream oss;
        oss << value;
        std::string valueJson = oss.str();
        bytes                 = std::vector<char>(valueJson.begin(), valueJson.end());

        reqStruct.passedFeatures[std::string(featureName)] = bytes;
      }
    }
  }

  simdjson::ondemand::object entries;
  // Map of serving key of feature view as key and value of serving key as value.
  // Serving key are a set of the primary key of feature groups which are included in the feature
  // view query. If feature groups are joint with prefix, the primary key needs to be attached with
  // prefix.
  auto entriesVal = reqObject[ENTRIES];
  if (entriesVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    // TODO: Handle missing field error
  } else if (entriesVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(entriesVal.error(), doc[threadId], currentLocation);
  } else {
    if (entriesVal.is_null()) {
      return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                        ERROR_CODE_INVALID_BODY,
                        std::string(ERROR_064) + " " + std::string(ENTRIES))
          .status;
    }
    error = entriesVal.get(entries);
    if (error != simdjson::SUCCESS) {
      return handle_simdjson_error(error, doc[threadId], currentLocation);
    }
    for (auto entry : entries) {
      std::string_view servingKey = entry.unescaped_key();
      simdjson::ondemand::value value;
      std::vector<char> bytes;
      auto valueVal = entry.value();
      if (valueVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
        return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                          ERROR_CODE_INVALID_BODY,
                          std::string(ERROR_064) + " " + std::string(servingKey))
            .status;
      }
      if (valueVal.error() != simdjson::SUCCESS) {
        return handle_simdjson_error(valueVal.error(), doc[threadId], currentLocation);
      }
      if (valueVal.is_null()) {
        return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                          ERROR_CODE_INVALID_BODY,
                          std::string(ERROR_064) + " " + std::string(servingKey))
            .status;
      }
      error = valueVal.get(value);
      if (error != simdjson::SUCCESS) {
        return handle_simdjson_error(error, doc[threadId], currentLocation);
      }
      std::ostringstream oss;
      oss << value;
      std::string valueJson = oss.str();
      bytes                 = std::vector<char>(valueJson.begin(), valueJson.end());

      reqStruct.entries[std::string(servingKey)] = bytes;
    }
  }

  simdjson::ondemand::object metaDataOptions;  // Optional.
  // Map of metadataoption as key and boolean as value.
  // Default metadata option is false. Metadata is returned on request.
  // Metadata options available: 1. featureName 2. featureType
  auto metaDataOptionsVal = reqObject[METADATA_OPTIONS];
  if (metaDataOptionsVal.error() == simdjson::error_code::NO_SUCH_FIELD ||
      metaDataOptionsVal.is_null()) {
    // If the metadataOptions field is not present or is null, set the optional fields to nullopt
    reqStruct.metadataRequest.featureName = std::nullopt;
    reqStruct.metadataRequest.featureType = std::nullopt;
  } else if (metaDataOptionsVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(metaDataOptionsVal.error(), doc[threadId], currentLocation);
  } else {
    // If metadataOptions field is present and not null
    error = metaDataOptionsVal.get(metaDataOptions);
    if (error != simdjson::SUCCESS) {
      return handle_simdjson_error(error, doc[threadId], currentLocation);
    }
    for (auto option : metaDataOptions) {
      std::string_view optionKey = option.unescaped_key();
      if (optionKey == FEATURE_NAME) {
        bool optionValue    = false;
        auto optionValueVal = option.value();
        if (optionValueVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(optionValueVal.error(), doc[threadId], currentLocation);
        }
        if (!optionValueVal.is_null()) {
          error = optionValueVal.get(optionValue);
          if (error != simdjson::SUCCESS) {
            return handle_simdjson_error(error, doc[threadId], currentLocation);
          }
          reqStruct.metadataRequest.featureName = optionValue;
        } else {
          reqStruct.metadataRequest.featureName = std::nullopt;
        }
      } else if (optionKey == FEATURE_TYPE) {
        bool optionValue    = false;
        auto optionValueVal = option.value();
        if (optionValueVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(optionValueVal.error(), doc[threadId], currentLocation);
        }
        if (!optionValueVal.is_null()) {
          error = optionValueVal.get(optionValue);
          if (error != simdjson::SUCCESS) {
            return handle_simdjson_error(error, doc[threadId], currentLocation);
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
  if (optionsVal.error() == simdjson::error_code::NO_SUCH_FIELD || optionsVal.is_null()) {
    // If the options field is not present or is null, set the optional fields to nullopt
    reqStruct.optionsRequest.validatePassedFeatures = std::nullopt;
    reqStruct.optionsRequest.includeDetailedStatus  = std::nullopt;
  } else if (optionsVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(optionsVal.error(), doc[threadId], currentLocation);
  } else {
    // If options field is present and not null
    error = optionsVal.get(options);
    if (error != simdjson::SUCCESS) {
      return handle_simdjson_error(error, doc[threadId], currentLocation);
    }
    for (auto option : options) {
      std::string_view optionKey = option.unescaped_key();
      if (optionKey == VALIDATE_PASSED_FEATURES) {
        bool optionValue = false;
        auto optionValueVal = option.value();
        if (optionValueVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(optionValueVal.error(), doc[threadId], currentLocation);
        }
        if (!optionValueVal.is_null()) {
          error = optionValueVal.get(optionValue);
          if (error != simdjson::SUCCESS) {
            return handle_simdjson_error(error, doc[threadId], currentLocation);
          }
          reqStruct.optionsRequest.validatePassedFeatures = optionValue;
        } else {
          reqStruct.optionsRequest.validatePassedFeatures = std::nullopt;
        }
      } else if (optionKey == INCLUDE_DETAILED_STATUS) {
        bool optionValue = false;
        auto optionValueVal = option.value();
        if (optionValueVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(optionValueVal.error(), doc[threadId], currentLocation);
        }
        if (!optionValueVal.is_null()) {
          error = optionValueVal.get(optionValue);
          if (error != simdjson::SUCCESS) {
            return handle_simdjson_error(error, doc[threadId], currentLocation);
          }
          reqStruct.optionsRequest.includeDetailedStatus = optionValue;
        } else {
          reqStruct.optionsRequest.includeDetailedStatus = std::nullopt;
        }
      }
    }
  }

  return CRS_Status::SUCCESS.status;
}

RS_Status JSONParser::batch_feature_store_parse(
    size_t threadId, simdjson::padded_string_view reqBody,
    feature_store_data_structs::BatchFeatureStoreRequest &reqStruct) {
  const char *currentLocation = nullptr;

  simdjson::error_code error = parser[threadId].iterate(reqBody).get(doc[threadId]);
  if (error != simdjson::SUCCESS) {
    return handle_simdjson_error(error, doc[threadId], currentLocation);
  }

  simdjson::ondemand::object reqObject;
  error = doc[threadId].get_object().get(reqObject);
  if (error != simdjson::SUCCESS) {
    return handle_simdjson_error(error, doc[threadId], currentLocation);
  }

  std::string_view featureStoreName;
  auto featureStoreNameVal = reqObject[FEATURE_STORE_NAME];
  if (featureStoreNameVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_BODY,
                      std::string(ERROR_064) + " " + std::string(FEATURE_STORE_NAME))
        .status;
  }
  if (featureStoreNameVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(featureStoreNameVal.error(), doc[threadId], currentLocation);
  }
  if (featureStoreNameVal.is_null()) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_BODY,
                      std::string(ERROR_064) + " " + std::string(FEATURE_STORE_NAME))
        .status;
  }
  error = featureStoreNameVal.get(featureStoreName);
  if (error != simdjson::SUCCESS) {
    return handle_simdjson_error(error, doc[threadId], currentLocation);
  }

  reqStruct.featureStoreName = featureStoreName;

  std::string_view featureViewName;
  auto featureViewNameVal = reqObject[FEATURE_VIEW_NAME];
  if (featureViewNameVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_BODY,
                      std::string(ERROR_064) + " " + std::string(FEATURE_VIEW_NAME))
        .status;
  }
  if (featureViewNameVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(featureViewNameVal.error(), doc[threadId], currentLocation);
  }
  if (featureViewNameVal.is_null()) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_BODY,
                      std::string(ERROR_064) + " " + std::string(FEATURE_VIEW_NAME))
        .status;
  }
  error = featureViewNameVal.get(featureViewName);
  if (error != simdjson::SUCCESS) {
    return handle_simdjson_error(error, doc[threadId], currentLocation);
  }

  reqStruct.featureViewName = featureViewName;

  uint64_t featureViewVersion = 0;
  auto featureViewVersionVal  = reqObject[FEATURE_VIEW_VERSION];
  if (featureViewVersionVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_BODY,
                      std::string(ERROR_064) + " " + std::string(FEATURE_VIEW_VERSION))
        .status;
  } else if (featureViewVersionVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(featureViewVersionVal.error(), doc[threadId], currentLocation);
  } else {
    if (featureViewVersionVal.is_null()) {
      return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                        ERROR_CODE_INVALID_BODY,
                        std::string(ERROR_064) + " " + std::string(FEATURE_VIEW_VERSION))
          .status;
    }
    error = featureViewVersionVal.get(featureViewVersion);
    if (error != simdjson::SUCCESS) {
      return handle_simdjson_error(error, doc[threadId], currentLocation);
    }
  }
  reqStruct.featureViewVersion = featureViewVersion;

  simdjson::ondemand::array passedFeatures;  // Optional
  auto passedFeaturesVal = reqObject[PASSED_FEATURES];

  if (passedFeaturesVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
  } else if (passedFeaturesVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(passedFeaturesVal.error(), doc[threadId], currentLocation);
  } else {
    if (!passedFeaturesVal.is_null()) {
      error = passedFeaturesVal.get(passedFeatures);
      if (error != simdjson::SUCCESS) {
        return handle_simdjson_error(error, doc[threadId], currentLocation);
      }
      // Each item is a map of feature name as key and feature value as value.
      // This overwrites feature values in the response.
      // If provided, its size and order has to be equal to the size of entries.
      // Item can be null.
      for (auto feature : passedFeatures) {
        if (feature.is_null()) {
          reqStruct.passedFeatures.push_back(std::unordered_map<std::string, std::vector<char>>());
          continue;
        }
        simdjson::ondemand::object featureObj;
        error = feature.get(featureObj);
        if (error != simdjson::SUCCESS) {
          return handle_simdjson_error(error, doc[threadId], currentLocation);
        }

        std::unordered_map<std::string, std::vector<char>> featureMap;
        for (auto featureItem : featureObj) {
          std::string_view featureName = featureItem.unescaped_key();
          simdjson::ondemand::value value;
          std::vector<char> bytes;
          auto valueVal = featureItem.value();
          if (valueVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
            return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                              ERROR_CODE_INVALID_BODY,
                              std::string(ERROR_064) + " " + std::string(featureName))
                .status;
          }
          if (valueVal.error() != simdjson::SUCCESS) {
            return handle_simdjson_error(valueVal.error(), doc[threadId], currentLocation);
          }

          if (valueVal.is_null()) {
            return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                              ERROR_CODE_INVALID_BODY,
                              std::string(ERROR_064) + " " + std::string(featureName))
                .status;
          }
          error = valueVal.get(value);
          if (error != simdjson::SUCCESS) {
            return handle_simdjson_error(error, doc[threadId], currentLocation);
          }
          std::ostringstream oss;
          oss << value;
          std::string valueJson = oss.str();
          bytes                 = std::vector<char>(valueJson.begin(), valueJson.end());
          featureMap[std::string(featureName)] = bytes;
        }
        reqStruct.passedFeatures.push_back(featureMap);
      }
    }
  }

  simdjson::ondemand::array entries;
  // Each item is a map of serving key of feature view as key and value of serving key as value.
  // Serving key of feature view.
  auto entriesVal = reqObject[ENTRIES];
  if (entriesVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    // TODO: Handle missing field error
  } else if (entriesVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(entriesVal.error(), doc[threadId], currentLocation);
  } else {
    if (entriesVal.is_null()) {
      return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                        ERROR_CODE_INVALID_BODY,
                        std::string(ERROR_064) + " " + std::string(ENTRIES))
          .status;
    }
    error = entriesVal.get(entries);
    if (error != simdjson::SUCCESS) {
      return handle_simdjson_error(error, doc[threadId], currentLocation);
    }
    for (auto entry : entries) {
      simdjson::ondemand::object entryObj;
      error = entry.get(entryObj);
      if (error != simdjson::SUCCESS) {
        return handle_simdjson_error(error, doc[threadId], currentLocation);
      }

      std::unordered_map<std::string, std::vector<char>> entryMap;
      for (auto entryItem : entryObj) {
        std::string_view servingKey = entryItem.unescaped_key();
        simdjson::ondemand::value value;
        std::vector<char> bytes;
        auto valueVal = entryItem.value();
        if (valueVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                            ERROR_CODE_INVALID_BODY,
                            std::string(ERROR_064) + " " + std::string(servingKey))
              .status;
        }
        if (valueVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(valueVal.error(), doc[threadId], currentLocation);
        }
        if (valueVal.is_null()) {
          return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                            ERROR_CODE_INVALID_BODY,
                            std::string(ERROR_064) + " " + std::string(servingKey))
              .status;
        }
        error = valueVal.get(value);
        if (error != simdjson::SUCCESS) {
          return handle_simdjson_error(error, doc[threadId], currentLocation);
        }
        std::ostringstream oss;
        oss << value;
        std::string valueJson = oss.str();
        bytes                 = std::vector<char>(valueJson.begin(), valueJson.end());

        entryMap[std::string(servingKey)] = bytes;
      }
      reqStruct.entries.push_back(entryMap);
    }
  }

    simdjson::ondemand::object metaDataOptions;  // Optional.
  // Map of metadataoption as key and boolean as value.
  // Default metadata option is false. Metadata is returned on request.
  // Metadata options available: 1. featureName 2. featureType
  auto metaDataOptionsVal = reqObject[METADATA_OPTIONS];
  if (metaDataOptionsVal.error() == simdjson::error_code::NO_SUCH_FIELD ||
      metaDataOptionsVal.is_null()) {
    // If the metadataOptions field is not present or is null, set the optional fields to nullopt
    reqStruct.metadataRequest.featureName = std::nullopt;
    reqStruct.metadataRequest.featureType = std::nullopt;
  } else if (metaDataOptionsVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(metaDataOptionsVal.error(), doc[threadId], currentLocation);
  } else {
    // If metadataOptions field is present and not null
    error = metaDataOptionsVal.get(metaDataOptions);
    if (error != simdjson::SUCCESS) {
      return handle_simdjson_error(error, doc[threadId], currentLocation);
    }
    for (auto option : metaDataOptions) {
      std::string_view optionKey = option.unescaped_key();
      if (optionKey == FEATURE_NAME) {
        bool optionValue    = false;
        auto optionValueVal = option.value();
        if (optionValueVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(optionValueVal.error(), doc[threadId], currentLocation);
        }
        if (!optionValueVal.is_null()) {
          error = optionValueVal.get(optionValue);
          if (error != simdjson::SUCCESS) {
            return handle_simdjson_error(error, doc[threadId], currentLocation);
          }
          reqStruct.metadataRequest.featureName = optionValue;
        } else {
          reqStruct.metadataRequest.featureName = std::nullopt;
        }
      } else if (optionKey == FEATURE_TYPE) {
        bool optionValue    = false;
        auto optionValueVal = option.value();
        if (optionValueVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(optionValueVal.error(), doc[threadId], currentLocation);
        }
        if (!optionValueVal.is_null()) {
          error = optionValueVal.get(optionValue);
          if (error != simdjson::SUCCESS) {
            return handle_simdjson_error(error, doc[threadId], currentLocation);
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
  if (optionsVal.error() == simdjson::error_code::NO_SUCH_FIELD || optionsVal.is_null()) {
    // If the options field is not present or is null, set the optional fields to nullopt
    reqStruct.optionsRequest.validatePassedFeatures = std::nullopt;
    reqStruct.optionsRequest.includeDetailedStatus  = std::nullopt;
  } else if (optionsVal.error() != simdjson::SUCCESS) {
    return handle_simdjson_error(optionsVal.error(), doc[threadId], currentLocation);
  } else {
    // If options field is present and not null
    error = optionsVal.get(options);
    if (error != simdjson::SUCCESS) {
      return handle_simdjson_error(error, doc[threadId], currentLocation);
    }
    for (auto option : options) {
      std::string_view optionKey = option.unescaped_key();
      if (optionKey == VALIDATE_PASSED_FEATURES) {
        bool optionValue = false;
        auto optionValueVal = option.value();
        if (optionValueVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(optionValueVal.error(), doc[threadId], currentLocation);
        }
        if (!optionValueVal.is_null()) {
          error = optionValueVal.get(optionValue);
          if (error != simdjson::SUCCESS) {
            return handle_simdjson_error(error, doc[threadId], currentLocation);
          }
          reqStruct.optionsRequest.validatePassedFeatures = optionValue;
        } else {
          reqStruct.optionsRequest.validatePassedFeatures = std::nullopt;
        }
      } else if (optionKey == INCLUDE_DETAILED_STATUS) {
        bool optionValue = false;
        auto optionValueVal = option.value();
        if (optionValueVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(optionValueVal.error(), doc[threadId], currentLocation);
        }
        if (!optionValueVal.is_null()) {
          error = optionValueVal.get(optionValue);
          if (error != simdjson::SUCCESS) {
            return handle_simdjson_error(error, doc[threadId], currentLocation);
          }
          reqStruct.optionsRequest.includeDetailedStatus = optionValue;
        } else {
          reqStruct.optionsRequest.includeDetailedStatus = std::nullopt;
        }
      }
    }
  }

  return CRS_Status::SUCCESS.status;
}

RS_Status extract_db_and_table(const std::string &relativeUrl, std::string &db,
                               std::string &table) {
  // Find the positions of the last three slashes
  size_t lastSlashPos       = relativeUrl.find_last_of('/');
  size_t secondLastSlashPos = lastSlashPos != std::string::npos
                                  ? relativeUrl.find_last_of('/', lastSlashPos - 1)
                                  : std::string::npos;
  size_t thirdLastSlashPos  = secondLastSlashPos != std::string::npos
                                  ? relativeUrl.find_last_of('/', secondLastSlashPos - 1)
                                  : std::string::npos;

  if (thirdLastSlashPos != std::string::npos && secondLastSlashPos != std::string::npos) {
    // If there are at least three slashes
    db    = relativeUrl.substr(thirdLastSlashPos + 1, secondLastSlashPos - thirdLastSlashPos - 1);
    table = relativeUrl.substr(secondLastSlashPos + 1, lastSlashPos - secondLastSlashPos - 1);
  } else if (secondLastSlashPos != std::string::npos) {
    // If there are only two slashes
    db    = relativeUrl.substr(0, secondLastSlashPos);
    table = relativeUrl.substr(secondLastSlashPos + 1, lastSlashPos - secondLastSlashPos - 1);
  } else {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_RELATIVE_URL, ERROR_063)
        .status;
  }

  return CRS_Status::SUCCESS.status;
}

RS_Status handle_simdjson_error(const simdjson::error_code &error,
                                simdjson::ondemand::document &doc, const char *&currentLocation) {
  simdjson::error_code getLocationError = doc.current_location().get(currentLocation);
  if (getLocationError != simdjson::SUCCESS) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      error_message(error), "")
        .status;
  }
  return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                    error_message(error), currentLocation)
      .status;
}
