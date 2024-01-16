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

#include "json_parser.hpp"
#include "pk_data_structs.hpp"
#include "src/error_strings.h"
#include <simdjson.h>

RS_Status extract_db_and_table(const std::string &, std::string &, std::string &);

// Is used to perform a primary key read operation.
RS_Status json_parser::parse(std::string_view &reqBody, PKReadParams &reqStruct) {
  simdjson::padded_string paddedJson(reqBody);

  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc;
  const char *current_location;

  simdjson::error_code error = parser.iterate(paddedJson).get(doc);
  if (error) {
    simdjson::error_code getLocationError = doc.current_location().get(current_location);
    if (getLocationError) {
      return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                       error_message(error), "");
    }
    return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                     error_message(error), current_location);
  }

  simdjson::ondemand::object reqObject;
  error = doc.get_object().get(reqObject);
  if (error) {
    simdjson::error_code getLocationError = doc.current_location().get(current_location);
    if (getLocationError) {
      return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                       error_message(error), "");
    }
    return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                     error_message(error), current_location);
  }

  simdjson::ondemand::array filters;

  auto filtersVal = reqObject["filters"];
  if (filtersVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    filters = {};
  } else if (filtersVal.error() != 0U) {
    simdjson::error_code getLocationError = doc.current_location().get(current_location);
    if (getLocationError) {
      return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                       error_message(error), "");
    }
    return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                     error_message(filtersVal.error()), current_location);
  } else {
    if (filtersVal.is_null()) {
      filters = {};
    } else {
      error = filtersVal.get(filters);
      if (error) {
        simdjson::error_code getLocationError = doc.current_location().get(current_location);
        if (getLocationError) {
          return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           error_message(error), "");
        }
        return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                         error_message(error), current_location);
      }
      for (auto filter : filters) {
        PKReadFilter pkReadFilter;
        simdjson::ondemand::object filterObj;
        error = filter.get(filterObj);
        if (error) {
          simdjson::error_code getLocationError = doc.current_location().get(current_location);
          if (getLocationError) {
            return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                             error_message(error), "");
          }
          return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           error_message(error), current_location);
        }

        std::string_view column;
        auto columnVal = filterObj["column"];
        if (columnVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          column = "";
        } else if (columnVal.error() != 0U) {
          simdjson::error_code getLocationError = doc.current_location().get(current_location);
          if (getLocationError) {
            return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                             error_message(error), "");
          }
          return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           error_message(columnVal.error()), current_location);
        } else {
          if (columnVal.is_null()) {
            column = "";
          } else {
            error = columnVal.get(column);
            if (error) {
              simdjson::error_code getLocationError = doc.current_location().get(current_location);
              if (getLocationError) {
                return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                                 error_message(error), "");
              }
              return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                               error_message(error), current_location);
            }
          }
        }
        pkReadFilter.column = column;

        simdjson::ondemand::value value;
        std::vector<char> bytes;
        auto valueVal = filterObj["value"];
        if (valueVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          bytes = {};
        } else if (valueVal.error() != 0U) {
          simdjson::error_code getLocationError = doc.current_location().get(current_location);
          if (getLocationError) {
            return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                             error_message(error), "");
          }
          return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           error_message(valueVal.error()), current_location);
        } else {
          if (valueVal.is_null()) {
            bytes = {};
          } else {
            error = valueVal.get(value);
            if (error) {
              simdjson::error_code getLocationError = doc.current_location().get(current_location);
              if (getLocationError) {
                return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                                 error_message(error), "");
              }
              return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                               error_message(error), current_location);
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

  auto readColumnsVal = reqObject["readColumns"];
  if (readColumnsVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    readColumns = {};
  } else if (readColumnsVal.error() != 0U) {
    simdjson::error_code getLocationError = doc.current_location().get(current_location);
    if (getLocationError) {
      return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                       error_message(error), "");
    }
    return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                     error_message(readColumnsVal.error()), current_location);
  } else {
    if (readColumnsVal.is_null()) {
      readColumns = {};
    } else {
      error = readColumnsVal.get(readColumns);
      if (error) {
        simdjson::error_code getLocationError = doc.current_location().get(current_location);
        if (getLocationError) {
          return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           error_message(error), "");
        }
        return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                         error_message(error), current_location);
      }

      for (auto readColumn : readColumns) {
        PKReadReadColumn pkReadReadColumn;
        simdjson::ondemand::object readColumnObj;
        error = readColumn.get(readColumnObj);
        if (error) {
          simdjson::error_code getLocationError = doc.current_location().get(current_location);
          if (getLocationError) {
            return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                             error_message(error), "");
          }
          return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           error_message(error), current_location);
        }

        std::string_view column;
        auto columnVal = readColumnObj["column"];
        if (columnVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          column = "";
        } else if (columnVal.error() != 0U) {
          simdjson::error_code getLocationError = doc.current_location().get(current_location);
          if (getLocationError) {
            return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                             error_message(error), "");
          }
          return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           error_message(columnVal.error()), current_location);
        } else {
          if (columnVal.is_null()) {
            column = "";
          } else {
            error = columnVal.get(column);
            if (error) {
              simdjson::error_code getLocationError = doc.current_location().get(current_location);
              if (getLocationError) {
                return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                                 error_message(error), "");
              }
              return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                               error_message(error), current_location);
            }
          }
        }
        pkReadReadColumn.column = column;

        std::string_view dataReturnType;
        auto dataReturnTypeVal = readColumnObj["dataReturnType"];
        if (dataReturnTypeVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          dataReturnType = "";
        } else if (dataReturnTypeVal.error() != 0U) {
          simdjson::error_code getLocationError = doc.current_location().get(current_location);
          if (getLocationError) {
            return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                             error_message(error), "");
          }
          return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           error_message(dataReturnTypeVal.error()), current_location);
        } else {
          if (dataReturnTypeVal.is_null()) {
            dataReturnType = "";
          } else {
            error = dataReturnTypeVal.get(dataReturnType);
            if (error) {
              simdjson::error_code getLocationError = doc.current_location().get(current_location);
              if (getLocationError) {
                return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                                 error_message(error), "");
              }
              return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                               error_message(error), current_location);
            }
          }
        }

        pkReadReadColumn.returnType = dataReturnType;

        reqStruct.readColumns.emplace_back(pkReadReadColumn);
      }
    }
  }

  std::string_view operationId;

  auto operationIdVal = reqObject["operationId"];
  if (operationIdVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    operationId = "";
  } else if (operationIdVal.error() != 0U) {
    simdjson::error_code getLocationError = doc.current_location().get(current_location);
    if (getLocationError) {
      return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                       error_message(error), "");
    }
    return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                     error_message(operationIdVal.error()), current_location);
  } else {
    if (operationIdVal.is_null()) {
      operationId = "";
    } else {
      error = operationIdVal.get(operationId);
      if (error) {
        simdjson::error_code getLocationError = doc.current_location().get(current_location);
        if (getLocationError) {
          return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           error_message(error), "");
        }
        return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                         error_message(error), current_location);
      }
    }
  }
  reqStruct.operationId = operationId;

  return RS_Status();
}

// This is used to perform batched primary key read operations.
// The body here is a list of arbitrary pk-reads under the key operations:
RS_Status json_parser::batch_parse(std::string_view &reqBody,
                                   std::vector<PKReadParams> &reqStructs) {
  simdjson::padded_string paddedJson(reqBody);

  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc;
  const char *current_location = nullptr;

  simdjson::error_code error = parser.iterate(paddedJson).get(doc);
  if (error) {
    simdjson::error_code getLocationError = doc.current_location().get(current_location);
    if (getLocationError) {
      return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                       error_message(error), "");
    }
    return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                     error_message(error), current_location);
  }

  simdjson::ondemand::object reqObject;
  error = doc.get_object().get(reqObject);
  if (error) {
    simdjson::error_code getLocationError = doc.current_location().get(current_location);
    if (getLocationError) {
      return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                       error_message(error), "");
    }
    return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                     error_message(error), current_location);
  }

  simdjson::ondemand::array operations;
  auto operationsVal = reqObject["operations"];
  if (operationsVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
    operations = {};
  } else if (operationsVal.error() != 0U) {
    simdjson::error_code getLocationError = doc.current_location().get(current_location);
    if (getLocationError) {
      return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                       error_message(error), "");
    }
    return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                     error_message(operationsVal.error()), current_location);
  } else {
    if (operationsVal.is_null()) {
      operations = {};
    } else {
      error = operationsVal.get(operations);
      if (error) {
        simdjson::error_code getLocationError = doc.current_location().get(current_location);
        if (getLocationError) {
          return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           error_message(error), "");
        }
        return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                         error_message(error), current_location);
      }
      for (auto operation : operations) {
        PKReadParams reqStruct;
        simdjson::ondemand::object operationObj;
        error = operation.get(operationObj);
        if (error) {
          simdjson::error_code getLocationError = doc.current_location().get(current_location);
          if (getLocationError) {
            return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                             error_message(error), "");
          }
          return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           error_message(error), current_location);
        }

        std::string_view method;
        auto methodVal = operationObj["method"];
        if (methodVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          method = "";
        } else if (methodVal.error() != 0U) {
          simdjson::error_code getLocationError = doc.current_location().get(current_location);
          if (getLocationError) {
            return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                             error_message(error), "");
          }
          return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           error_message(methodVal.error()), current_location);
        } else {
          if (methodVal.is_null()) {
            method = "";
          } else {
            error = methodVal.get(method);
            if (error) {
              simdjson::error_code getLocationError = doc.current_location().get(current_location);
              if (getLocationError) {
                return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                                 error_message(error), "");
              }
              return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                               error_message(error), current_location);
            }
          }
        }
        reqStruct.method = method;

        std::string_view relativeUrl;
        auto relativeUrlVal = operationObj["relative-url"];
        if (relativeUrlVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          relativeUrl = "";
        } else if (relativeUrlVal.error() != 0U) {
          simdjson::error_code getLocationError = doc.current_location().get(current_location);
          if (getLocationError) {
            return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                             error_message(error), "");
          }
          return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           error_message(relativeUrlVal.error()), current_location);
        } else {
          if (relativeUrlVal.is_null()) {
            relativeUrl = "";
          } else {
            error = relativeUrlVal.get(relativeUrl);
            if (error) {
              simdjson::error_code getLocationError = doc.current_location().get(current_location);
              if (getLocationError) {
                return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                                 error_message(error), "");
              }
              return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                               error_message(error), current_location);
            }
          }
        }

        RS_Status status =
            extract_db_and_table(std::string(relativeUrl), reqStruct.path.db, reqStruct.path.table);
        if (static_cast<drogon::HttpStatusCode>(status.http_code) !=
            drogon::HttpStatusCode::k200OK) {
          return status;
        }

        std::string_view bodyStr;
        auto bodyVal = operationObj["body"];
        if (bodyVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
          bodyStr = "";
        } else if (bodyVal.error() != 0U) {
          simdjson::error_code getLocationError = doc.current_location().get(current_location);
          if (getLocationError) {
            return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                             error_message(error), "");
          }
          return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           error_message(bodyVal.error()), current_location);
        } else {
          if (bodyVal.is_null()) {
            bodyStr = "";
          } else {
            bodyStr = simdjson::to_json_string(bodyVal);
          }
        }

        if (bodyStr.empty()) {
          return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           ERROR_CODE_INVALID_BODY, ERROR_064);
        }

        status = json_parser::parse(bodyStr, reqStruct);
        if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
          return status;
        }
        reqStructs.push_back(reqStruct);
      }
    }
  }

  return RS_Status();
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
    return RS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                     ERROR_CODE_INVALID_RELATIVE_URL, ERROR_063);
  }

  return RS_Status();
}