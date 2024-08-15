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
#include "constants.hpp"
#include "error_strings.h"
#include "config_structs.hpp"
#include "rdrs_dal.hpp"

#include <cstdint>
#include <cstddef>
#include <functional>
#include <memory>
#include <simdjson.h>

JSONParser jsonParser;

JSONParser::JSONParser() {
  // Allocate a buffer for each thread
  for (int i = 0; i < DEFAULT_NUM_THREADS; ++i) {
    buffers[i] = std::make_unique<char[]>(REQ_BUFFER_SIZE + simdjson::SIMDJSON_PADDING);
  }
}

std::unique_ptr<char[]> & JSONParser::get_buffer(size_t threadId) {
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

RS_Status JSONParser::config_parse(const std::string &configsBody,
                                   AllConfigs &configsStruct) {
  simdjson::padded_string paddedJson(configsBody);
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc;
  const char *currentLocation = nullptr;

  simdjson::error_code error = parser.iterate(paddedJson).get(doc);
  if (error != simdjson::SUCCESS) {
    return handle_simdjson_error(error, doc, currentLocation);
  }

  simdjson::ondemand::object configsObject;
  error = doc.get_object().get(configsObject);
  if (error != simdjson::SUCCESS) {
    return handle_simdjson_error(error, doc, currentLocation);
  }

  for (auto field : configsObject) {
    // parses and writes out the key, after unescaping it,
    // to a string buffer. It causes a performance penalty.
    std::string_view keyv = field.unescaped_key();
    if (keyv == GRPC_STR) {
      // We expect no grpc object here
      // Set the grpc.enable to be true
      // so that in main it exits
      configsStruct.grpc.enable = true;
      return CRS_Status::SUCCESS.status;
    }
    if (keyv == REST_STR) {
      simdjson::ondemand::object restObj;
      auto restVal = field.value();
      if (!restVal.is_null()) {
        error = restVal.get(restObj);
        if (error != simdjson::SUCCESS) {
          return handle_simdjson_error(error, doc, currentLocation);
        }
        auto portVal = restObj[SERVER_PORT];
        if (portVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
        } else if (portVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(portVal.error(), doc, currentLocation);
        } else {
          if (!portVal.is_null()) {
            uint64_t port = DEFAULT_REST_PORT;
            error         = portVal.get(port);
            if (error != simdjson::SUCCESS) {
              return handle_simdjson_error(error, doc, currentLocation);
            }
            configsStruct.rest.serverPort = port;
          }
        }
      }
    } else if (keyv == RONDB) {
      simdjson::ondemand::object rondbObj;
      auto rondbVal = field.value();
      if (!rondbVal.is_null()) {
        error = rondbVal.get(rondbObj);
        if (error != simdjson::SUCCESS) {
          return handle_simdjson_error(error, doc, currentLocation);
        }
        auto mgmdsVal = rondbObj[MGMDS];
        if (mgmdsVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
        } else if (mgmdsVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(mgmdsVal.error(), doc, currentLocation);
        } else {
          if (!mgmdsVal.is_null()) {
            simdjson::ondemand::array mgmdsArr;
            error = mgmdsVal.get(mgmdsArr);
            if (error != simdjson::SUCCESS) {
              return handle_simdjson_error(error, doc, currentLocation);
            }
            configsStruct.ronDB.Mgmds.clear();
            for (auto mgmd : mgmdsArr) {
              Mgmd mgmdStruct;
              simdjson::ondemand::object mgmdObj;
              error = mgmd.get(mgmdObj);
              if (error != simdjson::SUCCESS) {
                return handle_simdjson_error(error, doc, currentLocation);
              }

              std::string_view ip = LOCALHOST;
              auto ipVal          = mgmdObj[IP];
              if (ipVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
              } else if (ipVal.error() != simdjson::SUCCESS) {
                return handle_simdjson_error(ipVal.error(), doc, currentLocation);
              } else {
                if (!ipVal.is_null()) {
                  error = ipVal.get(ip);
                  if (error != simdjson::SUCCESS) {
                    return handle_simdjson_error(error, doc, currentLocation);
                  }
                }
              }
              mgmdStruct.IP = ip;

              uint64_t port = MGMD_DEFAULT_PORT;
              auto portVal  = mgmdObj[PORT];
              if (portVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
              } else if (portVal.error() != simdjson::SUCCESS) {
                return handle_simdjson_error(portVal.error(), doc, currentLocation);
              } else {
                if (!portVal.is_null()) {
                  error = portVal.get(port);
                  if (error != simdjson::SUCCESS) {
                    return handle_simdjson_error(error, doc, currentLocation);
                  }
                }
              }
              mgmdStruct.port = port;

              configsStruct.ronDB.Mgmds.push_back(mgmdStruct);
            }
          }
        }
      }
    } else if (keyv == SECURITY) {
      simdjson::ondemand::object securityObj;
      auto securityVal = field.value();
      if (!securityVal.is_null()) {
        error = securityVal.get(securityObj);
        if (error != simdjson::SUCCESS) {
          return handle_simdjson_error(error, doc, currentLocation);
        }
        auto tlsVal = securityObj[TLS_STR];
        if (tlsVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
        } else if (tlsVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(tlsVal.error(), doc, currentLocation);
        } else {
          if (!tlsVal.is_null()) {
            simdjson::ondemand::object tlsObj;
            error = tlsVal.get(tlsObj);
            if (error != simdjson::SUCCESS) {
              return handle_simdjson_error(error, doc, currentLocation);
            }
            auto enableTLSVal = tlsObj[ENABLE_TLS];
            if (enableTLSVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
            } else if (enableTLSVal.error() != simdjson::SUCCESS) {
              return handle_simdjson_error(enableTLSVal.error(), doc, currentLocation);
            } else {
              if (!enableTLSVal.is_null()) {
                bool enableTLS = false;
                error          = enableTLSVal.get(enableTLS);
                if (error != simdjson::SUCCESS) {
                  return handle_simdjson_error(error, doc, currentLocation);
                }
                configsStruct.security.tls.enableTLS = enableTLS;
              }
            }
            auto requireAndVerifyClientCertVal = tlsObj[REQUIRE_AND_VERIFY_CLIENT_CERT];
            if (requireAndVerifyClientCertVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
            } else if (requireAndVerifyClientCertVal.error() != simdjson::SUCCESS) {
              return handle_simdjson_error(requireAndVerifyClientCertVal.error(), doc,
                                           currentLocation);
            } else {
              if (!requireAndVerifyClientCertVal.is_null()) {
                bool requireAndVerifyClientCert = false;
                error = requireAndVerifyClientCertVal.get(requireAndVerifyClientCert);
                if (error != simdjson::SUCCESS) {
                  return handle_simdjson_error(error, doc, currentLocation);
                }
                configsStruct.security.tls.requireAndVerifyClientCert = requireAndVerifyClientCert;
              }
            }
          }
        }

        auto apiKeyVal = securityObj[API_KEY];
        if (apiKeyVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
        } else if (apiKeyVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(apiKeyVal.error(), doc, currentLocation);
        } else {
          if (!apiKeyVal.is_null()) {
            simdjson::ondemand::object apiKeyObj;
            error = apiKeyVal.get(apiKeyObj);
            if (error != simdjson::SUCCESS) {
              return handle_simdjson_error(error, doc, currentLocation);
            }
            auto useHopsworksAPIKeysVal = apiKeyObj[USE_HOPSWORKS_API_KEYS];
            if (useHopsworksAPIKeysVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
            } else if (useHopsworksAPIKeysVal.error() != simdjson::SUCCESS) {
              return handle_simdjson_error(useHopsworksAPIKeysVal.error(), doc,
                                           currentLocation);
            } else {
              if (!useHopsworksAPIKeysVal.is_null()) {
                bool useHopsworksAPIKeys = false;
                error                    = useHopsworksAPIKeysVal.get(useHopsworksAPIKeys);
                if (error != simdjson::SUCCESS) {
                  return handle_simdjson_error(error, doc, currentLocation);
                }
                configsStruct.security.apiKey.useHopsworksAPIKeys = useHopsworksAPIKeys;
              }
            }
          }
        }
      }
    } else if (keyv == LOG) {
      simdjson::ondemand::object logObj;
      auto logVal = field.value();
      if (!logVal.is_null()) {
        error = logVal.get(logObj);
        if (error != simdjson::SUCCESS) {
          return handle_simdjson_error(error, doc, currentLocation);
        }
        auto levelVal = logObj[LEVEL];
        if (levelVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
        } else if (levelVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(levelVal.error(), doc, currentLocation);
        } else {
          if (!levelVal.is_null()) {
            std::string_view level = INFO;
            error                  = levelVal.get(level);
            if (error != simdjson::SUCCESS) {
              return handle_simdjson_error(error, doc, currentLocation);
            }
            configsStruct.log.level = level;
          }
        }
      }
    } else if (keyv == TESTING) {
      simdjson::ondemand::object testingObj;
      auto testingVal = field.value();
      if (!testingVal.is_null()) {
        error = testingVal.get(testingObj);
        if (error != simdjson::SUCCESS) {
          return handle_simdjson_error(error, doc, currentLocation);
        }
        auto mySQLVal = testingObj[MYSQL];
        if (mySQLVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
        } else if (mySQLVal.error() != simdjson::SUCCESS) {
          return handle_simdjson_error(mySQLVal.error(), doc, currentLocation);
        } else {
          if (!mySQLVal.is_null()) {
            simdjson::ondemand::object mySQLObj;
            error = mySQLVal.get(mySQLObj);
            if (error != simdjson::SUCCESS) {
              return handle_simdjson_error(error, doc, currentLocation);
            }
            configsStruct.testing.mySQL.servers.clear();
            auto serversVal = mySQLObj[SERVERS];
            if (serversVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
            } else if (serversVal.error() != simdjson::SUCCESS) {
              return handle_simdjson_error(serversVal.error(), doc, currentLocation);
            } else {
              if (!serversVal.is_null()) {
                simdjson::ondemand::array serversArr;
                error = serversVal.get(serversArr);
                if (error != simdjson::SUCCESS) {
                  return handle_simdjson_error(error, doc, currentLocation);
                }
                for (auto server : serversArr) {
                  MySQLServer mySQLServer;
                  simdjson::ondemand::object serverObj;
                  error = server.get(serverObj);
                  if (error != simdjson::SUCCESS) {
                    return handle_simdjson_error(error, doc, currentLocation);
                  }

                  std::string_view ip = LOCALHOST;
                  auto ipVal          = serverObj[IP];
                  if (ipVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
                  } else if (ipVal.error() != simdjson::SUCCESS) {
                    return handle_simdjson_error(ipVal.error(), doc, currentLocation);
                  } else {
                    if (!ipVal.is_null()) {
                      error = ipVal.get(ip);
                      if (error != simdjson::SUCCESS) {
                        return handle_simdjson_error(error, doc, currentLocation);
                      }
                    }
                  }
                  mySQLServer.IP = ip;

                  uint64_t port = DEFAULT_MYSQL_PORT;
                  auto portVal  = serverObj[PORT];
                  if (portVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
                  } else if (portVal.error() != simdjson::SUCCESS) {
                    return handle_simdjson_error(portVal.error(), doc, currentLocation);
                  } else {
                    if (!portVal.is_null()) {
                      error = portVal.get(port);
                      if (error != simdjson::SUCCESS) {
                        return handle_simdjson_error(error, doc, currentLocation);
                      }
                    }
                  }
                  mySQLServer.port = port;

                  configsStruct.testing.mySQL.servers.push_back(mySQLServer);
                }
              }
            }

            auto userVal = mySQLObj[USER];
            if (userVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
            } else if (userVal.error() != simdjson::SUCCESS) {
              return handle_simdjson_error(userVal.error(), doc, currentLocation);
            } else {
              if (!userVal.is_null()) {
                std::string_view user = ROOT_STR;
                error                 = userVal.get(user);
                if (error != simdjson::SUCCESS) {
                  return handle_simdjson_error(error, doc, currentLocation);
                }
                configsStruct.testing.mySQL.user = user;
              }
            }

            auto passwordVal = mySQLObj[PASSWORD];
            if (passwordVal.error() == simdjson::error_code::NO_SUCH_FIELD) {
            } else if (passwordVal.error() != simdjson::SUCCESS) {
              return handle_simdjson_error(passwordVal.error(), doc, currentLocation);
            } else {
              if (!passwordVal.is_null()) {
                std::string_view password;
                error = passwordVal.get(password);
                if (error != simdjson::SUCCESS) {
                  return handle_simdjson_error(error, doc, currentLocation);
                }
                configsStruct.testing.mySQL.password = password;
              }
            }
          }
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
