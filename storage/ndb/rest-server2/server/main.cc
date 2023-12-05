#include <drogon/HttpTypes.h>
#include <drogon/drogon.h>

// #define NDEBUG
#define SIMDJSON_VERBOSE_LOGGING 1
#include <chrono>
#include <cmath>
#include <cstdio>
#include <iostream>
#include <memory>
#include <drogon/utils/string_view.h>
#include <thread>
#include "data-structs.h"
#include <simdjson.h>
#include "pk-data-structs.hpp"
#include "connection.hpp"
#include "config_structs.hpp"
#include "src/rdrs-dal.h"
#include "encoding.hpp"

using namespace drogon;
using namespace drogon::orm;
using namespace simdjson;

bool serialization = true;

#define BODY_MAX_SIZE 1024 * 1024 + SIMDJSON_PADDING

#define myprintf(...)     // printf(__VA_ARGS__)
#define mycout(msg, obj)  // std::cout << msg << obj << std::endl
#define MAX_THREADS 16

char buffers[MAX_THREADS][BODY_MAX_SIZE];

AllConfigs globalConfigs;

void create_dummy_batch_req(BatchOpRequest *req, int num_ops) {
  for (int i = 0; i < num_ops; i++) {
    BatchSubOp subOp{};
    subOp.Method = "POST";
    subOp.RelativeURL = "/testing/test";
    subOp.Body.OperationID = "opid";
    Filter filter;
    filter.Column = "filter_col";
    filter.Value = "filter_val";
    ReadColumn read_col;
    read_col.Column = "read_col";
    read_col.DataReturnType = "default";
    subOp.Body.Filters.push_back(filter);
    subOp.Body.ReadColumns.push_back(read_col);
    req->Operations.push_back(subOp);
  }
}

void work(int x) { std::this_thread::sleep_for(std::chrono::milliseconds(x)); }

void stressCPU(int durationInMs) {
  std::chrono::milliseconds startTime =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());
  const int numIterations = 1000;  // Adjust this number to control the workload

  while (true) {
    for (int i = 0; i < numIterations; ++i) {
      volatile double result = std::sqrt(i) * std::log(i + 1);
      result++;
    }

    std::chrono::milliseconds currentTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
    std::chrono::duration<double> elapsed = currentTime - startTime;
    if (elapsed.count() >= durationInMs) {
      break;
    }
  }
}
void simple_to_string(BatchOpRequest *req) {
  std::stringstream ss;
  ss << "{'operations':[ ";
  for (size_t i = 0; i < req->Operations.size(); i++) {
    BatchSubOp op = req->Operations[i];
    ss << "{ 'method': '" << op.Method<< "', ";
    ss << "'relative_url': '" << op.RelativeURL<<"', ";
    ss << "'body': {";

    ss << "'filters': [";
    for (size_t f = 0; f < op.Body.Filters.size(); f++) {
      Filter filter = op.Body.Filters[f];

      ss << "{'column': '" << filter.Column << "', 'value': '" << filter.Value << "'" << "},";
    }
    ss << "],";

    ss << "'readCoumns': [";
    for (size_t f = 0; f < op.Body.Filters.size(); f++) {
      ReadColumn rc = op.Body.ReadColumns[f];

      ss << "{'column': '" << rc.Column << "', 'dataType': " << rc.DataReturnType << "'  },";
    }
    ss << "],";

    ss<< "'operationid': '"<< op.Body.OperationID << "'";


    ss << "}}, ";
  }
  ss << "]} ";

  // std::cout<<ss.str()<<std::endl;
}

int json_parse(std::string_view &reqBody, PKReadParams &reqStruct, int threadId) {
  char* json = buffers[threadId];

  if (reqBody.size() >= (BODY_MAX_SIZE - SIMDJSON_PADDING)) {
    return simdjson::error_code::INSUFFICIENT_PADDING;
  }

  memcpy(json, reqBody.data(), reqBody.size());
  json[reqBody.size()] = 0;
  simdjson::padded_string padded_json(json, reqBody.size());

  ondemand::parser parser;
  ondemand::document doc;

  auto error = parser.iterate(padded_json).get(doc);
  if (error) {
    std::cout << "Parsing request failed. Error: " << error
      << " at " << doc.current_location() << std::endl;
    return error;
  }

  ondemand::object req_object;
  error = doc.get_object().get(req_object);
  if (error) {
    std::cout << "Parsing request failed. Error: " << error
      << " at " << doc.current_location() << std::endl;
    return error;
  }

  ondemand::array filters;
  error = req_object["filters"].get_array().get(filters);
  if (error) {
    std::cout << "Parsing request failed. Error: " << error
      << " at " << doc.current_location() << std::endl;
    return error;
  }

  for (auto filter: filters) {
    PKReadFilter pk_read_filter;
    ondemand::object filter_obj;
    error = filter.get(filter_obj);
    if (error) {
      std::cout << "Parsing request failed. Error: " << error
        << " at " << doc.current_location() << std::endl;
      return error;
    }

    std::string_view column;
    error = filter_obj["column"].get(column);
    if (error) {
      std::cout << "Parsing request failed. Error: " << error
        << " at " << doc.current_location() << std::endl;
      return error;
    }
    pk_read_filter.column = column;

    ondemand::value value;
    error = filter_obj["value"].get(value);
    if (error) {
      std::cout << "Parsing request failed. Error: " << error
        << " at " << doc.current_location() << std::endl;
      return error;
    }
    if (value.type() == ondemand::json_type::number) {
      double value_double = value.get_double();
      std::vector<char> bytes(sizeof(double));
      std::memcpy(bytes.data(), &value_double, sizeof(double));
      pk_read_filter.value = bytes;
    } else if (value.type() == ondemand::json_type::string) {
      std::string_view value_str = value.get_string();
      std::vector<char> bytes(value_str.begin(), value_str.end());
      pk_read_filter.value = bytes;
    } else {
      std::cout << "Parsing request failed. Error: " << error
        << " at " << doc.current_location() << std::endl;
      return error;
    }

    reqStruct.filters.emplace_back(pk_read_filter);
  }

  ondemand::array read_columns;
  error = req_object["readColumns"].get_array().get(read_columns);
  if (error) {
    std::cout << "Parsing request failed. Error: " << error
      << " at " << doc.current_location() << std::endl;
    return error;
  }

  for (auto read_column: read_columns) {
    PKReadReadColumn pk_read_read_column;
    ondemand::object read_column_obj;
    error = read_column.get(read_column_obj);
    if (error) {
      std::cout << "Parsing request failed. Error: " << error
        << " at " << doc.current_location() << std::endl;
      return error;
    }

    std::string_view column;
    error = read_column_obj["column"].get(column);
    if (error) {
      std::cout << "Parsing request failed. Error: " << error
        << " at " << doc.current_location() << std::endl;
      return error;
    }
    pk_read_read_column.column = column;

    std::string_view data_return_type;
    error = read_column_obj["dataReturnType"].get(data_return_type);
    if (error) {
      std::cout << "Parsing request failed. Error: " << error
        << " at " << doc.current_location() << std::endl;
      return error;
    }
    pk_read_read_column.returnType = data_return_type;

    reqStruct.readColumns.emplace_back(pk_read_read_column);
  }

  std::string_view operation_id;
  error = req_object["operationId"].get(operation_id);
  if (error) {
    std::cout << "Parsing request failed. Error: " << error
      << " at " << doc.current_location() << std::endl;
    return error;
  }

  reqStruct.operationId = operation_id;
  return simdjson::SUCCESS;
}

int batch_parse(std::basic_string_view<char> &req_body, BatchOpRequest &req_struct, int thradID, int *opCount) {
  char *json = buffers[thradID];

  if (req_body.size() >= (BODY_MAX_SIZE - SIMDJSON_PADDING)) {
    return simdjson::error_code::INSUFFICIENT_PADDING;
  }

  memcpy(json, req_body.data(), req_body.size());
  json[req_body.size()] = 0;

  ondemand::parser parser;
  ondemand::document doc;

  simdjson::error_code error;
  error = parser.iterate(json, req_body.size(), BODY_MAX_SIZE).get(doc);

  if (error) {
    myprintf("Parsing request failed. Error: %d. Line: %d \n", error, __LINE__);
    return error;
  } else {
    ondemand::array operations_arr;
    simdjson::error_code error =
        doc["operations"].get_array().get(operations_arr);
    if (error) {
      myprintf("Parsing request failed. Error: %d. Line: %d \n", error, __LINE__);
      return error;
    }

    for (auto arr_element : operations_arr) {
      opCount++;
      myprintf("\n------------\n");

      ondemand::object operation;
      simdjson::error_code error = arr_element.get(operation);
      if (error) {
        myprintf(
            "Parsing request failed Failed to read op. Error: %d. Line: %d \n",
            error, __LINE__);
        break;
      }

      std::string_view method;
      error = operation["method"].get(method);
      if (error) {
        myprintf(
            "Parsing request failed. Failed to read method. Error: %d. Line: "
            "%d \n",
            error, __LINE__);
        return error;
      } else {
        mycout("Method is: ", method);
      }

      std::string_view relative_url;
      error = operation["relative-url"].get(relative_url);
      if (error) {
        myprintf(
            "Parsing request failed. Failed to read relative url. Error: %d. "
            "Line: %d \n",
            error, __LINE__);
        return error;
      } else {
        mycout("Relateiv URL is: ", relative_url);
      }

      ondemand::object body;
      error = operation["body"].get(body);
      if (error) {
        myprintf(
            "Parsing request failed. Failed to read sub op body. Error: %d. "
            "Line: %d \n",
            error, __LINE__);
        return error;
      } else {
        // mycout("Body: ", body);

        ondemand::array filters_arr;
        error = body["filters"].get_array().get(filters_arr);
        if (error) {
          myprintf(
              "Parsing request failed. Failed to read filters. Error: %d. "
              "Line: %d \n",
              error, __LINE__);
          return error;
        } else {
          // list of filter objects
          for (auto arr_element : filters_arr) {
            ondemand::object filter;
            simdjson::error_code error = arr_element.get(filter);
            if (error) {
              myprintf(
                  "Parsing request failed Failed to read filter obj. Error: "
                  "%d. Line: "
                  "%d \n",
                  error, __LINE__);
              return error;
            }

            std::string_view column;
            error = filter["column"].get(column);
            if (error) {
              myprintf(
                  "Parsing request failed. Failed to read column. Error: %d. "
                  "Line: %d \n",
                  error, __LINE__);
            } else {
              mycout("Column: ", column);
            }

            std::string_view value;
            error = filter["value"].raw_json_token().get(value);
            if (error) {
              myprintf(
                  "Parsing request failed. Failed to read column. Error: %d. "
                  "Line: %d \n",
                  error, __LINE__);
            } else {
              mycout("Value: ", value);
            }
          }

          ondemand::array read_columns_arr;
          error = body["readColumns"].get_array().get(read_columns_arr);
          if (error) {
            myprintf("readColumns: null\n");
          } else {
            for (auto arr_element : read_columns_arr) {
              ondemand::object read_column;
              simdjson::error_code error = arr_element.get(read_column);
              if (error) {
                myprintf(
                    "Parsing request failed Failed to read read col obj. "
                    "Error: "
                    "%d. Line: "
                    "%d \n",
                    error, __LINE__);
                return error;
              }

              std::string_view column;
              error = read_column["column"].get(column);
              if (error) {
                myprintf(
                    "Parsing request failed. Failed to read column. Error: %d. "
                    "Line: %d \n",
                    error, __LINE__);
              } else {
                mycout("Column: ", column);
              }

              std::string_view dataReturnType;
              error = read_column["dataReturnType"].raw_json_token().get(
                  dataReturnType);
              if (error) {
                myprintf("dataReturnType: null\n");
              } else {
                mycout("Data Return type: ", dataReturnType);
              }
            }
          }
        }

        // operatin id
        std::string_view operation_id;
        error = body["operationId"].get(operation_id);
        if (error) {
          myprintf("OperationID: null\n");
        } else {
          mycout("OperationID: ", operation_id);
        }
      }
    }
  }
  return simdjson::SUCCESS;
}

int main() {
  app().registerHandler(
      "/ping",
      [](const HttpRequestPtr &,
         std::function<void(const HttpResponsePtr &)> &&callback) {
        auto resp = HttpResponse::newHttpResponse();
        resp->setBody("Hello, World!");
        resp->setStatusCode(drogon::HttpStatusCode::k200OK);
        callback(resp);
      },
      {Get});

  app().registerHandler(
      "/ping/delay/{time}",
      [](const HttpRequestPtr &,
         std::function<void(const HttpResponsePtr &)> &&callback,
         const std::string &time) {
        int t = std::stoi(time);
        work(t);
        auto resp = HttpResponse::newHttpResponse();
        resp->setBody("Hello, World!. With fixed delay");
        resp->setStatusCode(drogon::HttpStatusCode::k200OK);
        callback(resp);
      },
      {Get});

  app().registerHandler(
      "/ping/rand_delay/{time}",
      [](const HttpRequestPtr &,
         std::function<void(const HttpResponsePtr &)> &&callback,
         const std::string &time) {
        int t = std::stoi(time);
        if (t != 0) {
          t = rand() % t;
        }
        work(t);

        auto resp = HttpResponse::newHttpResponse();
        resp->setBody("Hello, World!. With random delay");
        resp->setStatusCode(drogon::HttpStatusCode::k200OK);
        callback(resp);
      },
      {Get});

  app().registerHandler(
      "/dbopsimd/{db}/{table}",
      [](const HttpRequestPtr &req,
         std::function<void(const HttpResponsePtr &)> &&callback
         //,const std::string &db, const std::string table) {
        ) {
        auto reqBody = req->getBody();
        // printf("Current thread %d\n",app().getCurrentThreadIndex());

        PKReadParams reqStruct;
        int error = json_parse(reqBody, reqStruct, app().getCurrentThreadIndex());

        if (error == simdjson::SUCCESS) {
          RS_BufferManager reqBuffManager = RS_BufferManager(globalConfigs.internal.bufferSize);
          RS_BufferManager respBuffManager = RS_BufferManager(globalConfigs.internal.bufferSize);
          RS_Buffer* reqBuff = reqBuffManager.getBuffer();
          RS_Buffer* respBuff = respBuffManager.getBuffer();

          create_native_request(reqStruct, reqBuff->buffer, respBuff->buffer);

          auto resp = HttpResponse::newHttpResponse();
          resp->setStatusCode(drogon::HttpStatusCode::k200OK);

          // pk_read
          pk_read(reqBuff, respBuff);
          // convert resp to json
          char* respData = respBuff->buffer;
          PKReadResponseJSON respJson;
          respJson.init();
          process_pkread_response(respData, respJson);
          resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
          resp->setBody(respJson.toString());
          callback(resp);
        } else {
          printf("Operation failed\n");
          auto resp = HttpResponse::newHttpResponse();
          resp->setBody("NACK");
          resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
          callback(resp);
        }
      },
      {Post});

  // connect to rondb
  globalConfigs = AllConfigs();
  RonDBConnection rondbConnection(globalConfigs.ronDB,
                                  globalConfigs.ronDbMetaDataCluster);

  if (globalConfigs.security.tls.enableTLS) {}

  if (globalConfigs.grpc.enable) {}

  if (globalConfigs.rest.enable) {
    printf("Server running on 0.0.0.0:4046\n");
    app().addListener("0.0.0.0", 4046);
    app().setThreadNum(MAX_THREADS);
    app().disableSession();
    app().run();
  }

  return 0;
}
