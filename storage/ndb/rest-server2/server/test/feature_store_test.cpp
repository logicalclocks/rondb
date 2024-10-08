/*
 * Copyright (C) 2024 Hopsworks AB
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

#include "../src/config_structs.hpp"
#include "connection.hpp"
#include "constants.hpp"
#include "feature_store_data_structs.hpp"
#include "feature_util.hpp"
#include "metadata.hpp"
#include "resources/embeddings.hpp"
#include "rdrs_dal.h"
#include "rdrs_hopsworks_dal.h"
#include "rdrs_rondb_connection.hpp"
#include "rdrs_rondb_connection_pool.hpp"
#include "base64.h"

#include <avro/ValidSchema.hh>
#include <drogon/HttpClient.h>
#include <drogon/HttpTypes.h>
#include <fstream>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <chrono>
#include <queue>
#include <mutex>
#include <mysql.h>
#include <tuple>
#include <unordered_map>
#include <vector>

MYSQL *CreateMySQLConnectionDataCluster() {
  auto conf = globalConfigs;
  auto connection_string =
    globalConfigs.testing.mySQL.generate_mysqld_connect_string();
  std::cout << "Connecting to data mysqld with connection string: "
            << connection_string
            << std::endl;
  MYSQL *dbConnection = mysql_init(nullptr);

  if (mysql_real_connect(
    dbConnection,
    globalConfigs.testing.mySQL.servers[0].IP.c_str(),
    globalConfigs.testing.mySQL.user.c_str(),
    globalConfigs.testing.mySQL.password.c_str(), nullptr,
    globalConfigs.testing.mySQL.servers[0].port, nullptr, 0) == nullptr) {
    std::cerr << "Failed to connect to data database: "
              << mysql_error(dbConnection) << std::endl;
    mysql_close(dbConnection);
    return nullptr;
  }
  return dbConnection;
}

MYSQL *CreateMySQLConnectionMetadataCluster() {
  auto conf = globalConfigs.get_all();
  // auto connection_string =
  // globalConfigs.testing.mySQLMetadataCluster.
  // generate_mysqld_connect_string();
  MYSQL *dbConnection = mysql_init(nullptr);

  if (!mysql_real_connect(
          dbConnection,
          globalConfigs.testing.mySQLMetadataCluster.servers[0].IP.c_str(),
          globalConfigs.testing.mySQL.user.c_str(),
          globalConfigs.testing.mySQL.password.c_str(),
          nullptr,
          globalConfigs.testing.mySQLMetadataCluster.servers[0].port,
          nullptr,
          0)) {
    std::cerr << "Failed to connect to metadata database: "
              << mysql_error(dbConnection)
              << std::endl;
    mysql_close(dbConnection);
    return nullptr;
  }
  return dbConnection;
}

std::tuple<std::vector<std::string>,
           std::vector<std::string>,
           std::vector<std::string>,
           RS_Status>
getColumnInfo(const std::string &dbName, const std::string &tableName) {
  auto *dbConn = CreateMySQLConnectionDataCluster();
  if (dbConn == nullptr) {
    return std::make_tuple(std::vector<std::string>(),
                           std::vector<std::string>(),
                           std::vector<std::string>(),
                           CRS_Status(HTTP_CODE::SERVER_ERROR,
                             "Failed to connect to data database").status);
  }

  std::vector<std::string> colTypes;
  std::vector<std::string> columns;
  std::vector<std::string> pks;

  std::string query =
    "SELECT DATA_TYPE, COLUMN_NAME, COLUMN_KEY FROM information_schema.COLUMNS "
    "WHERE TABLE_SCHEMA = '" + dbName + "' AND TABLE_NAME = '" +
    tableName + "'" + "ORDER BY ORDINAL_POSITION";
  if (mysql_query(dbConn, query.c_str()) != 0) {
    std::cerr << "Failed to execute query: "
              << mysql_error(dbConn)
              << std::endl;
    mysql_close(dbConn);
    return std::make_tuple(std::vector<std::string>(),
                           std::vector<std::string>(),
                           std::vector<std::string>(),
                           CRS_Status(HTTP_CODE::SERVER_ERROR,
                              "Failed to execute query").status);
  }

  MYSQL_RES *result = mysql_store_result(dbConn);
  if (result == nullptr) {
    std::cerr << "Failed to store result: "
              << mysql_error(dbConn)
              << std::endl;
    mysql_close(dbConn);
    return std::make_tuple(std::vector<std::string>(),
                           std::vector<std::string>(),
                           std::vector<std::string>(),
                           CRS_Status(HTTP_CODE::SERVER_ERROR,
                              "Failed to store result").status);
  }
  MYSQL_ROW row   = nullptr;
  auto num_fields = mysql_num_fields(result);
  if (num_fields < 3) {
    std::cerr << "Unexpected number of fields: " << num_fields << std::endl;
    mysql_free_result(result);
    mysql_close(dbConn);
    return std::make_tuple(std::vector<std::string>(),
                           std::vector<std::string>(),
                           std::vector<std::string>(),
                           CRS_Status(HTTP_CODE::SERVER_ERROR,
                              "Unexpected number of fields").status);
  }

  while ((row = mysql_fetch_row(result)) != nullptr) {
    std::string columnType = row[0];
    std::string columnName = row[1];
    std::string columnKey  = row[2];
    if (columnKey == "PRI") {
      pks.push_back(columnName);
    }
    colTypes.push_back(columnType);
    columns.push_back(columnName);
  }

  if (mysql_errno(dbConn) != 0) {
    std::cerr << "Failed to fetch row: " << mysql_error(dbConn) << std::endl;
    mysql_free_result(result);
    mysql_close(dbConn);
    return std::make_tuple(std::vector<std::string>(),
                           std::vector<std::string>(),
                           std::vector<std::string>(),
                           CRS_Status(HTTP_CODE::SERVER_ERROR,
                              "Failed to fetch row").status);
  }
  mysql_free_result(result);
  mysql_close(dbConn);
  return std::make_tuple(colTypes, columns, pks, CRS_Status::SUCCESS.status);
}

bool isColNumerical(const std::string &colType) {
  static const std::map<std::string, bool> numericalType = {
      {"TINYINT", true},
      {"SMALLINT", true},
      {"MEDIUMINT", true},
      {"INT", true},
      {"INTEGER", true},
      {"BIGINT", true},
      {"DECIMAL", true},
      {"FLOAT", true},
      {"DOUBLE", true},
      {"REAL", true}};
  return numericalType.find(colType) != numericalType.end();
}

std::tuple<std::vector<std::vector<std::vector<char>>>, RS_Status>
fetchRowsInt(const std::string &query,
             const std::vector<std::string> &colTypes,
             MYSQL *dbConn) {
  if (mysql_query(dbConn, query.c_str()) != 0) {
    std::cerr << "Query execution failed: "
              << mysql_error(dbConn)
              << std::endl;
    return std::make_tuple(std::vector<std::vector<std::vector<char>>>(),
                           CRS_Status(HTTP_CODE::SERVER_ERROR,
                             "Failed to execute query").status);
  }
  MYSQL_RES *result = mysql_store_result(dbConn);
  if (result == nullptr) {
    std::cerr << "Failed to store result: "
              << mysql_error(dbConn)
              << std::endl;
    return std::make_tuple(std::vector<std::vector<std::vector<char>>>(),
                           CRS_Status(HTTP_CODE::SERVER_ERROR,
                             "Failed to store result").status);
  } 
  std::vector<std::vector<std::vector<char>>> valueBatch;
  MYSQL_ROW row           = nullptr;
  unsigned int num_fields = mysql_num_fields(result);
  // MYSQL_FIELD* fields = mysql_fetch_fields(result);
  while ((row = mysql_fetch_row(result)) != nullptr) {
    unsigned long *lengths = mysql_fetch_lengths(result);
    std::vector<std::vector<char>> rawRow;

    for (unsigned int i = 0; i < num_fields; ++i) {
      std::vector<char> rawValue;
      if (row[i] != nullptr) {
        if (isColNumerical(colTypes[i])) {
          rawValue.insert(rawValue.end(), row[i], row[i] + lengths[i]);
        } else {
          rawValue.push_back('"');
          rawValue.insert(rawValue.end(), row[i], row[i] + lengths[i]);
          rawValue.push_back('"');
        }
      } else {
        rawValue = {'N', 'U', 'L', 'L'};
      }
      rawRow.push_back(rawValue);
    }
    valueBatch.push_back(rawRow);
  }

  if (mysql_errno(dbConn) != 0) {
    std::cerr << "Error while fetching rows: "
              << mysql_error(dbConn) << std::endl;
    mysql_free_result(result);
    return std::make_tuple(std::vector<std::vector<std::vector<char>>>(),
                           CRS_Status(HTTP_CODE::SERVER_ERROR,
                             "Error while fetching rows").status);
  }
  if (valueBatch.empty()) {
    mysql_free_result(result);
    return std::make_tuple(std::vector<std::vector<std::vector<char>>>(),
                           CRS_Status(HTTP_CODE::SERVER_ERROR,
                             "No sample data is fetched").status);
  }
  mysql_free_result(result);
  return std::make_tuple(valueBatch, CRS_Status::SUCCESS.status);
}

std::tuple<std::vector<std::vector<std::vector<char>>>, RS_Status>
fetchDataRows(const std::string &query, std::vector<std::string> colTypes) {
  auto *dbConn = CreateMySQLConnectionDataCluster();
  if (dbConn == nullptr) {
    return std::make_tuple(std::vector<std::vector<std::vector<char>>>(),
                           CRS_Status(HTTP_CODE::SERVER_ERROR,
                             "Failed to connect to data database").status);
  }
  auto [valueBatch, status] = fetchRowsInt(query, colTypes, dbConn);
  mysql_close(dbConn);
  return std::make_tuple(valueBatch, status);
}

std::tuple<std::vector<std::vector<std::vector<char>>>, RS_Status>
fetchMetadataRows(const std::string &query, std::vector<std::string> colTypes) {
  auto *dbConn = CreateMySQLConnectionMetadataCluster();
  if (dbConn == nullptr) {
    return std::make_tuple(std::vector<std::vector<std::vector<char>>>(),
                           CRS_Status(HTTP_CODE::SERVER_ERROR,
                             "Failed to connect to metadata database").status); 
  }
  auto [valueBatch, status] = fetchRowsInt(query, colTypes, dbConn);
  mysql_close(dbConn);
  return std::make_tuple(valueBatch, status);
}

std::tuple<std::vector<std::vector<std::vector<char>>>,
           std::vector<std::string>,
           std::vector<std::string>,
           RS_Status>
GetNSampleData(const std::string &database, const std::string &table, int n) {
  std::vector<std::string> columnNames;
  std::vector<std::string> pks;
  std::vector<std::string> colTypes;
  RS_Status status;
  tie(colTypes, columnNames, pks, status) = getColumnInfo(database, table);
  if (status.http_code !=
        static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    return std::make_tuple(std::vector<std::vector<std::vector<char>>>(),
                           std::vector<std::string>(),
                           std::vector<std::string>(),
                           status);
  }
  std::string query = "SELECT * FROM " + database + "." + table + " LIMIT " +
                      std::to_string(n);
  auto [valueBatch, fetchStatus] = fetchDataRows(query, colTypes);
  if (fetchStatus.http_code !=
         static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    return std::make_tuple(std::vector<std::vector<std::vector<char>>>(),
                           std::vector<std::string>(),
                           std::vector<std::string>(),
                           fetchStatus);
  }
  return std::make_tuple(valueBatch,
                         columnNames,
                         pks,
                         CRS_Status::SUCCESS.status);
}

std::tuple<std::vector<std::vector<std::vector<char>>>,
           std::vector<std::string>,
           std::vector<std::string>,
           RS_Status>
GetSampleData(const std::string &database, const std::string &table) {
  return GetNSampleData(database, table, 2);
}

feature_store_data_structs::FeatureStoreRequest
CreateFeatureStoreRequest(
  const std::string &fsName,
  const std::string &fvName,
  int fvVersion,
  const std::vector<std::string> &pks,
  const std::vector<std::vector<char>> &values,
  const std::vector<std::string> &passedFeaturesKey,
  const std::vector<std::vector<char>> &passedFeaturesValue) {

  std::unordered_map<std::string, std::vector<char>> entries;
  for (unsigned i = 0; i < pks.size(); i++) {
    entries[pks[i]] = values[i];
  }

  std::unordered_map<std::string, std::vector<char>> passedFeatures;
  for (unsigned i = 0; i < passedFeaturesKey.size(); i++) {
    passedFeatures[passedFeaturesKey[i]] = passedFeaturesValue[i];
  }

  feature_store_data_structs::FeatureStoreRequest request;
  request.featureStoreName = fsName;
  request.featureViewName = fvName;
  request.featureViewVersion = fvVersion;
  request.entries = entries;
  request.passedFeatures = passedFeatures;
  return request;
}

feature_store_data_structs::BatchFeatureStoreRequest CreateFeatureStoreRequest(
    const std::string &fsName,
    const std::string &fvName,
    int fvVersion,
    const std::vector<std::string> &pks,
    const std::vector<std::vector<std::vector<char>>> &batchValues,
    const std::vector<std::string> &passedFeaturesKey,
    const std::vector<std::vector<std::vector<char>>>
      &batchPassedFeaturesValue) {
  std::vector<std::unordered_map<std::string, std::vector<char>>> batchEntries;
  for (const auto &values : batchValues) {
    std::unordered_map<std::string, std::vector<char>> entries;
    for (unsigned i = 0; i < pks.size(); i++) {
      entries[pks[i]] = values[i];
    }
    batchEntries.push_back(entries);
  }

  std::vector<std::unordered_map<std::string,
                                 std::vector<char>>>
    batchPassedFeatures;
  for (const auto &values : batchPassedFeaturesValue) {
    std::unordered_map<std::string, std::vector<char>> passedFeatures;
    for (unsigned i = 0; i < passedFeaturesKey.size(); i++) {
      passedFeatures[passedFeaturesKey[i]] = values[i];
    }
    batchPassedFeatures.push_back(passedFeatures);
  }
  auto req = feature_store_data_structs::BatchFeatureStoreRequest();
  req.featureStoreName = fsName;
  req.featureViewName = fvName;
  req.featureViewVersion = fvVersion;
  req.entries = batchEntries;
  req.passedFeatures = batchPassedFeatures;
  return req;
}

int SendHttpRequestWithClient(
  const std::shared_ptr<drogon::HttpClient> &client,
  const std::string &httpVerb,
  const std::string &url,
  const std::string &body,
  const std::string &expectedErrMsg,
  const std::vector<int> &expectedStatus,
  std::vector<char> &responseBody) {
  auto req = drogon::HttpRequest::newHttpRequest();
  req->setPath(url);

  if (httpVerb == POST) {
    req->setMethod(drogon::Post);
    req->setBody(body);
    req->setContentTypeCode(drogon::CT_APPLICATION_JSON);
  } else if (httpVerb == GET) {
    req->setMethod(drogon::Get);
  } else {
    ADD_FAILURE() << ("HTTP verb '" + httpVerb + "' is not implemented");
    return -1;
  }

  auto [result, resp] = client->sendRequest(req);
  if (result != drogon::ReqResult::Ok) {
    ADD_FAILURE() << "failed to perform HTTP request towards url: '" +
                    url + "'\nrequest body:"
                  << body << "\nerror: " +
                    std::to_string(static_cast<int>(result));
    return -1;
  }

  int respCode = static_cast<int>(resp->getStatusCode());
  responseBody.assign(resp->body().begin(), resp->body().end());
  std::string respBody(responseBody.begin(), responseBody.end());

  auto idx = std::find(expectedStatus.begin(), expectedStatus.end(), respCode);
  if (idx == expectedStatus.end()) {
    ADD_FAILURE() << "Received unexpected status '" +
                       std::to_string(respCode) +
                      "' expected status: " +
                        std::to_string(expectedStatus[0]) + " url: '" +
                        url + "' body: '" + body + "' response body: " +
                        respBody;
    return respCode;
  }

  if (respCode != static_cast<int>(drogon::HttpStatusCode::k200OK) &&
      respBody.find(expectedErrMsg) == std::string::npos) {
    ADD_FAILURE() << "Response error body does not contain '" +
                       expectedErrMsg + "'; received response body: '" +
                       respBody + "'";
    return respCode;
  }
  return respCode;
}

int SendHttpRequest(
  const std::string &httpVerb,
  const std::string &url,
  const std::string &body,
  const std::string &expectedErrMsg,
  const std::vector<int> &expectedStatus,
  std::vector<char> &responseBody) {
  auto client = drogon::HttpClient::newHttpClient("http://localhost:8080");
  return SendHttpRequestWithClient(client,
                                   httpVerb,
                                   url,
                                   body,
                                   expectedErrMsg,
                                   expectedStatus,
                                   responseBody);
}

void appendURLProtocol(std::string &url) {
  auto conf = globalConfigs.get_all();
  if (conf.security.tls.enableTLS) {
    url = "https://" + url;
  } else {
    url = "http://" + url;
  }
}

std::string NewFeatureStoreURL() {
  auto url = globalConfigs.rest.serverIP + ":" +
             std::to_string(globalConfigs.rest.serverPort) +
             "/" + API_VERSION + FEATURE_STORE_OPERATION;
  appendURLProtocol(url);
  return url;
}

std::shared_ptr<feature_store_data_structs::FeatureStoreResponse>
GetFeatureStoreResponseWithDetail(
  const feature_store_data_structs::FeatureStoreRequest &req,
  const std::string &message,
  int status) {

  auto reqBody = req.to_string();
  std::vector<char> respBody;
  SendHttpRequest(POST,
                  NewFeatureStoreURL(),
                  reqBody,
                  message,
                  {status},
                  respBody);
  if (status == drogon::k200OK) {
    auto fsResp = feature_store_data_structs::FeatureStoreResponse();
    auto status =
      feature_store_data_structs::FeatureStoreResponse::
        parseFeatureStoreResponse(
        std::string(respBody.begin(),
        respBody.end()),
        fsResp);

    if (status.http_code != static_cast<HTTP_CODE>(drogon::k200OK)) {
      return nullptr;
    }
    return std::make_shared<feature_store_data_structs::FeatureStoreResponse>(
      fsResp);
  }
  return nullptr;
}

std::shared_ptr<feature_store_data_structs::BatchFeatureStoreResponse>
GetFeatureStoreResponseWithDetail(
  const feature_store_data_structs::BatchFeatureStoreRequest &req,
  const std::string &message,
  int status) {
  auto reqBody = req.to_string();
  std::vector<char> respBody;
  SendHttpRequest(POST,
                  NewFeatureStoreURL(),
                  reqBody,
                  message,
                  {status},
                  respBody);
  if (status == drogon::k200OK) {
    auto fsResp = feature_store_data_structs::BatchFeatureStoreResponse();
    auto status =
      feature_store_data_structs::BatchFeatureStoreResponse::
        parseBatchFeatureStoreResponse(
          std::string(respBody.begin(), respBody.end()),
          fsResp);

    if (status.http_code != static_cast<HTTP_CODE>(drogon::k200OK)) {
      return nullptr;
    }
    return std::make_shared<feature_store_data_structs::
      BatchFeatureStoreResponse>(fsResp);
  }
  return nullptr;
}

std::shared_ptr<feature_store_data_structs::FeatureStoreResponse>
GetFeatureStoreResponse(
  const feature_store_data_structs::FeatureStoreRequest &req) {
  return GetFeatureStoreResponseWithDetail(req, "", drogon::k200OK);
}

std::shared_ptr<feature_store_data_structs::BatchFeatureStoreResponse>
GetFeatureStoreResponse(
  const feature_store_data_structs::BatchFeatureStoreRequest &req) {
  return GetFeatureStoreResponseWithDetail(req, "", drogon::k200OK);
}

std::vector<std::vector<char>> GetPkValues(
  const std::vector<std::vector<char>> &row,
  const std::vector<std::string> &pks,
  const std::vector<std::string> &cols) {

  auto pkSet = std::unordered_map<std::string, bool>();
  for (const auto &pk : pks) {
    pkSet[pk] = true;
  }
  std::vector<std::vector<char>> pkValues;
  for (unsigned i = 0; i < cols.size(); i++) {
    if (pkSet.find(cols[i]) != pkSet.end()) {
      pkValues.push_back(row[i]);
    }
  }
  return pkValues;
}

std::vector<std::vector<std::vector<char>>>
GetPkValuesBatch(const std::vector<std::vector<std::vector<char>>> &batchRows,
                 const std::vector<std::string> &pks,
                 const std::vector<std::string> &cols) {
  std::vector<std::vector<std::vector<char>>> pkValuesBatch;
  for (const auto &row : batchRows) {
    pkValuesBatch.push_back(GetPkValues(row, pks, cols));
  }
  return pkValuesBatch;
}

std::tuple<std::vector<std::string>, std::vector<std::vector<char>>>
GetPkValuesExclude(const std::vector<std::vector<char>> &row,
                   const std::vector<std::string> &pks,
                   const std::vector<std::string> &cols,
                   const std::vector<std::string> &exclude) {

  auto pkSet = std::unordered_map<std::string, bool>();
  auto exSet = std::unordered_map<std::string, bool>();
  for (const auto &pk : pks) {
    pkSet[pk] = true;
  }
  for (const auto &ex : exclude) {
    exSet[ex] = true;
  }
  auto pkValue     = std::vector<std::vector<char>>();
  auto pksFiltered = std::vector<std::string>();
  for (unsigned i = 0; i < cols.size(); i++) {
    if (pkSet.find(cols[i]) !=
          pkSet.end() && exSet.find(cols[i]) == exSet.end()) {
      pkValue.push_back(row[i]);
      pksFiltered.push_back(cols[i]);
    }
  }
  return std::make_tuple(pksFiltered, pkValue);
}

std::tuple<std::vector<std::string>,
           std::vector<std::vector<std::vector<char>>>>
GetPkValuesBatchExclude(
  const std::vector<std::vector<std::vector<char>>> &batchRows,
  const std::vector<std::string> &pks,
  const std::vector<std::string> &cols,
  const std::vector<std::string> &exclude) {

  std::vector<std::vector<std::vector<char>>> pkValues;
  std::vector<std::string> pkFiltered;
  for (const auto &row : batchRows) {
    auto [pksFilteredRow, pkValueFiltered] =
      GetPkValuesExclude(row, pks, cols, exclude);
    pkFiltered = pksFilteredRow;
    pkValues.push_back(pkValueFiltered);
  }
  return std::make_tuple(pkFiltered, pkValues);
}

std::string removeQuotes(const std::string &input) {
  // Check if the string starts and ends with double quotes
  if (input.length() >= 2 && input.front() == '"' && input.back() == '"') {
    // Remove the first and last character (double quotes)
    return input.substr(1, input.length() - 2);
  }
  return input;  // Return unchanged if not quoted
}

std::string ConvertBinaryToJsonMessage(const std::vector<char> &data) {
  // string to base64string
  std::string dataStr(data.begin(), data.end());
  std::string unquotedStr = removeQuotes(dataStr);
  std::string base64Str = base64_decode(unquotedStr);
  std::string jsonStr = "\"" + base64Str + "\"";
  return jsonStr;
}

void ValidateResponseWithDataExcludeCols(
    const std::vector<std::vector<char>> &data,
    const std::vector<std::string> &cols,
    const std::unordered_map<std::string, bool> &exCols,
    const feature_store_data_structs::FeatureStoreResponse &resp) {

  auto status = data.empty() ?
    feature_store_data_structs::FeatureStatus::Error :
    feature_store_data_structs::FeatureStatus::Complete;

  int i = -1;
  for (size_t k = 0; k < data.size(); ++k) {
    const auto &_data = data[k];
    if (exCols.find(cols[k]) != exCols.end() && exCols.at(cols[k])) {
      continue;
    }
    ++i;
    const auto &gotRaw = resp.features[i];

    if (gotRaw.empty() && !_data.empty()) {
      FAIL() << "Got nil but expect "
             << std::string(_data.begin(), _data.end());
    } else if (!gotRaw.empty() && _data.empty()) {
      FAIL() << "Got " << std::string(gotRaw.begin(), gotRaw.end())
             << " but expect nil";
    } else if (gotRaw.empty() && _data.empty()) {
      status = feature_store_data_structs::FeatureStatus::Missing;
      continue;
    }

    simdjson::dom::element got;
    if (simdjson::dom::parser parser;
        parser.parse(std::string(gotRaw.begin(),
                                 gotRaw.end())).get(got) != simdjson::SUCCESS) {
      FAIL() << "Cannot parse gotRaw: "
             << std::string(gotRaw.begin(), gotRaw.end());
      continue;
    }
    simdjson::dom::element expectedJson;
    if (_data.empty()) {
      expectedJson = simdjson::dom::element();
    } else {
      if (simdjson::dom::parser parser;
          parser.parse(std::string(_data.begin(),
                       _data.end())).get(expectedJson) !=
            simdjson::SUCCESS) {
        FAIL() << "Cannot parse _data: "
               << std::string(_data.begin(), _data.end());
        continue;
      }
    }
    if (cols[i].find("binary") != std::string::npos) {
      std::string gotStr = std::string(got.get_string().value());
      std::string decodedStr = base64_decode(gotStr);
      simdjson::dom::parser parser;
      if (parser.parse(decodedStr).get(got) != simdjson::SUCCESS) {
        FAIL() << "Cannot parse decoded binary data: " << decodedStr;
        continue;
      }
    }
    std::string got_json = simdjson::to_string(got);
    std::string expected_json = simdjson::to_string(expectedJson);
    EXPECT_EQ(got_json, expected_json)
        << "col: " << cols[k] << "; Got " << got_json
        << " but expect " << expected_json;
  }
  if (resp.status != status) {
    FAIL() << "Got status "
           << toString(resp.status) << " but expect " << toString(status);
  }
}

void ValidateResponseWithData(
  const std::vector<std::vector<char>> &data,
  const std::vector<std::string> &cols,
  const feature_store_data_structs::FeatureStoreResponse &resp) {

  auto exCols = std::unordered_map<std::string, bool>();
  ValidateResponseWithDataExcludeCols(data, cols, exCols, resp);
}

void ValidateBatchResponseWithDataExcludeCols(
  const std::vector<std::vector<std::vector<char>>> &data,
  const std::vector<std::string> &cols,
  const std::unordered_map<std::string, bool> &exCols,
  const feature_store_data_structs::BatchFeatureStoreResponse &resp) {

  for (unsigned i = 0; i < data.size(); ++i) {
    auto fsResp = feature_store_data_structs::FeatureStoreResponse();
    fsResp.metadata = resp.metadata;
    fsResp.features = resp.features[i];
    fsResp.status = resp.status[i];
    if (exCols.empty()) {
      ValidateResponseWithData(data[i], cols, fsResp);
    } else {
      ValidateResponseWithDataExcludeCols(data[i], cols, exCols, fsResp);
    }
  }
}

void ValidateBatchResponseWithData(
  const std::vector<std::vector<std::vector<char>>> &data,
  const std::vector<std::string> &cols,
  const feature_store_data_structs::BatchFeatureStoreResponse &resp) {

  auto exCols = std::unordered_map<std::string, bool>();
  ValidateBatchResponseWithDataExcludeCols(data, cols, exCols, resp);
}

void ValidateResponseMetadataExCol(
  std::vector<feature_store_data_structs::FeatureMetadata> &metadata,
  const feature_store_data_structs::MetadataRequest &metadataRequest,
  const std::map<std::string, bool> &exCol,
  const std::string &fsName,
  const std::string &fvName,
    int fvVersion) {

  auto fetchRows = [](const std::string &query,
                      const std::vector<std::string> &colTypes) {
    auto [rows, status] = fetchMetadataRows(query, colTypes);
    if (status.http_code !=
          static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
      ADD_FAILURE() << "Failed to fetch rows with error: "
                    << status.message;
    }
    return rows;
  };
  auto rows = fetchRows(
    "SELECT id from hopsworks.feature_store where name = \"" + fsName +
    "\"", {"bigint"});
  int fsId  = std::stoi(std::string(rows[0][0].begin(), rows[0][0].end()));
  rows = fetchRows(
    "SELECT id from hopsworks.feature_view where feature_store_id = " +
      std::to_string(fsId) + " and name = \"" + fvName + "\" and version = " +
      std::to_string(fvVersion), {"bigint"});
  int fvId = std::stoi(std::string(rows[0][0].begin(),
                                   rows[0][0].end()));

  rows = fetchRows(
    "SELECT tdf.name, tdf.type, tdj.prefix from"
    " hopsworks.training_dataset_feature tdf "
    "inner join hopsworks.training_dataset_join tdj on tdf.td_join = tdj.id "
    "where tdf.feature_view_id = " + std::to_string(fvId) +
    " order by tdf.idx", {"varchar", "varchar", "varchar"});

  std::vector<feature_store_data_structs::FeatureMetadata> expected;
  for (const auto &row : rows) {
    feature_store_data_structs::FeatureMetadata meta;
    std::string prefix(row[2].begin(), row[2].end());
    prefix.erase(std::remove(prefix.begin(), prefix.end(), '\"'), prefix.end());
    std::string name(row[0].begin(), row[0].end());
    name.erase(std::remove(name.begin(), name.end(), '\"'), name.end());
    name = prefix.append(name);
    if (exCol.find(name) != exCol.end() && exCol.at(name)) {
      continue;
    }
    if (metadataRequest.featureName) {
      meta.name = name;
    }
    if (metadataRequest.featureType) {
      std::string type(row[1].begin(), row[1].end());
      type.erase(std::remove(type.begin(), type.end(), '\"'), type.end());
      meta.type = type;
    }
    expected.push_back(meta);
  }

  for (size_t i = 0; i < metadata.size(); ++i) {
    const auto &got = metadata[i];
    const auto &expect = expected[i];
    if (got != expect) {
      FAIL() << "Got " << got.toString()
             << " but expect " << expect.toString();
    }
  }
}

void ValidateResponseMetadata(
  std::vector<feature_store_data_structs::FeatureMetadata> &metadata,
  const feature_store_data_structs::MetadataRequest &metadataRequest,
  const std::string &fsName,
  const std::string &fvName,
  int fvVersion) {

  auto exCol = std::map<std::string, bool>();
  ValidateResponseMetadataExCol(metadata,
                                metadataRequest,
                                exCol,
                                fsName,
                                fvName,
                                fvVersion);
}

void testConvertAvroToJson(const std::string &schema,
                           const std::vector<Uint8> &data,
                           std::vector<char> expectedJson) {
  metadata::AvroDecoder decoder;
  try {
    decoder = metadata::AvroDecoder(schema);
  } catch (const std::exception &e) {
    FAIL() << "Failed to create avro decoder: " << e.what();
  }
  auto [native, _, err] = decoder.NativeFromBinary(data);
  if (err.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to convert avro to json with error: " << err.message;
  }
  auto [actual, status] = ConvertAvroToJson(native, decoder.getSchema());
  if (status.http_code !=
        static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    FAIL() << "Failed to convert avro to json with error: " << status.message;
  }
  std::string actualStr(actual.begin(), actual.end());
  std::string expectedStr(expectedJson.begin(), expectedJson.end());
  simdjson::dom::parser parser;
  simdjson::dom::element actualJson;
  simdjson::dom::element expectedJsonElem;
  auto error = parser.parse(actualStr).get(actualJson);
  if (error != 0U) {
    FAIL() << "Failed to parse actual JSON: " << simdjson::error_message(error);
  }
  error = parser.parse(expectedStr).get(expectedJsonElem);
  if (error != 0U) {
    FAIL() << "Failed to parse expected JSON: "
           << simdjson::error_message(error);
  }
  // Convert both elements back to strings for comparison
  std::string actualJsonStr   = simdjson::minify(actualJson);
  std::string expectedJsonStr = simdjson::minify(expectedJsonElem);
  EXPECT_EQ(actualJsonStr, expectedJsonStr)
      << "Expected " << expectedStr << " but got " << actualStr;
}

static std::vector<char> s2v(const char *str) {
  return std::vector<char>(str, str + strlen(str));
}

std::string datumToJson(const avro::GenericDatum &datum,
                        const avro::ValidSchema &schema) {
  std::ostringstream oss;
  auto encoder = avro::jsonEncoder(schema);
  auto outputStream = avro::ostreamOutputStream(oss);
  encoder->init(*outputStream);
  avro::encode(*encoder, datum);
  outputStream->flush();
  return oss.str();
}

class FeatureStoreTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    setenv("RUNNING_UNIT_TESTS", "1", 1);
  }
  static void TearDownTestSuite() {
    unsetenv("RUNNING_UNIT_TESTS");
  }
};

class MyEnvironment : public ::testing::Environment {
 public:
  ~MyEnvironment() override {}

  // Override this to define how to set up the environment.
  void SetUp() override
  {
    RS_Status status = RonDBConnection::init_rondb_connection(
      globalConfigs.ronDB,
      globalConfigs.ronDBMetadataCluster);
    if (status.http_code !=
          static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
      errno = status.http_code;
      exit(errno);
    }
  }

  // Override this to define how to tear down the environment.
  void TearDown() override
  {
    RS_Status status = RonDBConnection::shutdown_rondb_connection();
    if (status.http_code !=
          static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
      errno = status.http_code;
      exit(errno);
    }
  }
};

TEST_F(FeatureStoreTest, TestFeatureStoreMetaData) {
  metadata::FeatureViewMetadata *metadata;
  std::shared_ptr<RestErrorCode> errorCode;
  std::tie(metadata, errorCode) =
    metadata::GetFeatureViewMetadata(FSDB002, "sample_2", 1);
  EXPECT_EQ(errorCode, nullptr) << "Read FS metadata failed "
                                << errorCode->ToString();
}

TEST_F(FeatureStoreTest, TestMetadata_FsNotExist) {
  metadata::FeatureViewMetadata *metadata;
  std::shared_ptr<RestErrorCode> errorCode;
  std::tie(metadata, errorCode) =
    metadata::GetFeatureViewMetadata("NA", "sample_2", 1);
  EXPECT_NE(errorCode, nullptr) << "This should fail.";
  EXPECT_TRUE(errorCode->GetReason().find(FS_NOT_EXIST->GetReason()) !=
              std::string::npos)
      << "This should fail with error message: " << FS_NOT_EXIST->GetReason();
}

TEST_F(FeatureStoreTest, TestMetadata_ReadDeletedFg) {
  metadata::FeatureViewMetadata *metadata;
  std::shared_ptr<RestErrorCode> errorCode;
  std::tie(metadata, errorCode) =
    metadata::GetFeatureViewMetadata(FSDB001, "test_deleted_fg", 1);
  EXPECT_NE(errorCode, nullptr) << "This should fail.";
  EXPECT_TRUE(errorCode->GetReason().find(FG_NOT_EXIST->GetReason()) !=
              std::string::npos)
      << "This should fail with error message: "
      << FG_NOT_EXIST->GetReason()
      << ". But found: " << errorCode->GetReason();
}

TEST_F(FeatureStoreTest, TestMetadata_ReadDeletedJointFg) {
  metadata::FeatureViewMetadata *metadata;
  std::shared_ptr<RestErrorCode> errorCode;
  std::tie(metadata, errorCode) =
      metadata::GetFeatureViewMetadata(FSDB001, "test_deleted_joint_fg", 1);
  EXPECT_NE(errorCode, nullptr) << "This should fail.";
  EXPECT_TRUE(errorCode->GetReason().find(FG_NOT_EXIST->GetReason()) !=
              std::string::npos)
      << "This should fail with error message: "
      << FG_NOT_EXIST->GetReason()
      << ". But found: " << errorCode->GetReason();
}

TEST_F(FeatureStoreTest, TestMetadata_FvNotExist) {
  metadata::FeatureViewMetadata *metadata;
  std::shared_ptr<RestErrorCode> errorCode;
  std::tie(metadata, errorCode) =
    metadata::GetFeatureViewMetadata(FSDB002, "NA", 1);
  EXPECT_NE(errorCode, nullptr) << "This should fail.";
  EXPECT_TRUE(errorCode->GetReason().find(FV_NOT_EXIST->GetReason()) !=
              std::string::npos)
      << "This should fail with error message: " << FV_NOT_EXIST->GetReason();
}

namespace c {
struct cpx {
  double re;
  double im;
};

}  // namespace c

namespace avro {
template <> struct codec_traits<c::cpx> {
  static void encode(Encoder &e, const c::cpx &v) {
    avro::encode(e, v.re);
    avro::encode(e, v.im);
  }
  static void decode(Decoder &d, c::cpx &v) {
    avro::decode(d, v.re);
    avro::decode(d, v.im);
  }
};
}  // namespace avro

TEST_F(FeatureStoreTest, DISABLED_TestConvertAvroToJsonCpx) {
  std::string schemaJson = R"({
      "type": "record",
      "name": "cpx",
      "fields" : [
          {"name": "re", "type": "double"},
          {"name": "im", "type" : "double"}
      ]
  })";

  avro::ValidSchema cpxSchema = avro::compileJsonSchemaFromString(schemaJson);

  std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
  avro::EncoderPtr e                      = avro::binaryEncoder();
  e->init(*out);
  c::cpx c1;
  c1.re = 100.23;
  c1.im = 105.77;
  avro::encode(*e, c1);

  std::unique_ptr<avro::InputStream> in = avro::memoryInputStream(*out);
  avro::DecoderPtr d                    = avro::binaryDecoder();
  d->init(*in);

  avro::GenericDatum datum(cpxSchema);
  avro::decode(*d, datum);
  EXPECT_EQ(datum.type(), avro::AVRO_RECORD);
  const avro::GenericRecord &r = datum.value<avro::GenericRecord>();
  EXPECT_EQ(r.fieldCount(), 2);
  if (r.fieldCount() == 2) {
    const avro::GenericDatum &f0 = r.fieldAt(0);
    EXPECT_EQ(f0.type(), avro::AVRO_DOUBLE);
    EXPECT_EQ(f0.value<double>(), 100.23);

    const avro::GenericDatum &f1 = r.fieldAt(1);
    EXPECT_EQ(f1.type(), avro::AVRO_DOUBLE);
    EXPECT_EQ(f1.value<double>(), 105.77);
  }
}

TEST_F(FeatureStoreTest, DISABLED_TestConvertAvroToJsonSimple) {
  std::string schemaJson = R"({
      "type": "record",
      "name": "LongList",
      "fields" : [
        {"name": "next", "type": ["null", "LongList"], "default": null}
      ]
    })";

  metadata::AvroDecoder decoder(schemaJson);

  std::vector<Uint8> buf = {0x02, 0x02, 0x00};

  try {
    auto result = decoder.decode(buf);
    std::string expectedJson =
      R"({"next":{"LongList":{"next":{"LongList":{"next":null}}}}})";
    auto [resultBytes, status] = ConvertAvroToJson(result, decoder.getSchema());
    std::string resultJson =
      std::string(resultBytes.begin(), resultBytes.end());
    ASSERT_EQ(resultJson, expectedJson);
  } catch (const std::exception &e) {
    FAIL() << "Failed to decode avro: " << e.what();
  }
}

TEST_F(FeatureStoreTest, DISABLED_TestConvertAvroToJson) {
  // map
  testConvertAvroToJson(
      R"(["null",{"type":"record",
        "name":"r854762204",
        "namespace":"struct",
        "fields":[{"name":"int1",
        "type":["null",
        "long"]},{"name":"int2",
        "type":["null",
        "long"]}]}])",
      {0x02, 0x02, 0x56, 0x02, 0x88, 0x01}, s2v(R"({"int1":43,"int2":68})"));

  // array
  testConvertAvroToJson(R"(["null",{"type":"array","items":["null","long"]}])",
                        {0x02, 0x06, 0x02, 0x02, 0x00, 0x02, 0x06, 0x00},
                        s2v(R"([1,null,3])"));

  // array of map
  testConvertAvroToJson(
      R"(["null",{"type":"array",
        "items":["null",
        {"type":"record",
        "name":"r854762204",
        "namespace":"struct",
        "fields":[{"name":"int1",
        "type":["null","long"]},
        {"name":"int2",
        "type":["null",
        "long"]}]}]}])",
      {0x02, 0x06, 0x02, 0x02, 0x22, 0x02, 0x48,
       0x00, 0x02, 0x02, 0x24, 0x02, 0x4A, 0x00},
      s2v(R"([{"int1":17,"int2":36},null,{"int1":18,"int2":37}])"));

  // map of array
  testConvertAvroToJson(
      R"(["null",{"type":"record",
        "name":"r854762204","namespace":"struct",
        "fields":[{"name":"int1",
        "type":["null",
        {"type":"array",
        "items":["null","long"]}]},
        {"name":"int2","type":["null",
        {"type":"array",
        "items":["null",
        "long"]}]}]}])",
      {0x02, 0x02, 0x06, 0x02, 0x02, 0x02, 0x04, 0x02, 0x06,
       0x00, 0x02, 0x06, 0x02, 0x06, 0x00, 0x02, 0x0a, 0x00},
      s2v(R"({"int1":[1,2,3],"int2":[3,null,5]})"));
}

TEST_F(FeatureStoreTest, DISABLED_Test_GetFeatureVector_Success_ComplexType) {
  auto fsName = FSDB002;
  const auto *fvName = "sample_complex_type";
  auto fvVersion = 1;
  auto [rows, pks, cols, status] =
    GetSampleData(fsName, "sample_complex_type_1");
  ASSERT_EQ(status.http_code,
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
    << "Cannot get sample data with error: " << status.message;
  metadata::AvroDecoder mapDecoder;
  metadata::AvroDecoder arrayDecoder;
  try {
    mapDecoder = metadata::AvroDecoder(
      "[\"null\",{\"type\":\"record\",\"name\":\"r854762204\",\"namespace\":\"struct\","
      "\"fields\":[{\"name\":\"int1\",\"type\":[\"null\",\"long\"]},{\"name\":\"int2\",\"type\":["
      "\"null\",\"long\"]}]}]");
  } catch (const std::exception &e) {
    FAIL() << "Failed to create mapDecoder: " << e.what();
  }
  try {
    arrayDecoder =
        metadata::AvroDecoder(
          "[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"long\"]}]");
  } catch (const std::exception &e) {
    FAIL() << "Failed to create arrayDecoder: " << e.what();
  }
  for (auto &row : rows) {
    auto fsReq = CreateFeatureStoreRequest(fsName,
                                           fvName,
                                           fvVersion,
                                           pks,
                                           GetPkValues(row,
                                                       pks,
                                                       cols), {}, {});
    fsReq.metadataRequest = {true, true};
    auto fsResp = GetFeatureStoreResponse(fsReq);
    EXPECT_NE(fsResp, nullptr) << "Failed to get feature store response";
    // convert data to object in json format
    auto arrayJson = ConvertBinaryToJsonMessage(row[2]);
    auto [arrayPt, err1] = DeserialiseComplexFeature(
        std::vector<char>(arrayJson.begin(), arrayJson.end()), arrayDecoder);
    row[2] = arrayPt;
    if (err1 != nullptr) {
      FAIL() << "Cannot deserailize feature with error " << err1->GetReason();
    }
    // convert data to object in json format
    auto mapJson = ConvertBinaryToJsonMessage(row[3]);
    auto [mapPt, err2] =
        DeserialiseComplexFeature(
          std::vector<char>(mapJson.begin(), mapJson.end()), mapDecoder);
    row[3] = mapPt;
    if (err2 != nullptr) {
      FAIL() << "Cannot deserailize feature with error " << err2->GetReason();
    }

    // validate
    ValidateResponseWithData(row, cols, *fsResp);
    ValidateResponseMetadata(fsResp->metadata,
                             fsReq.metadataRequest,
                             fsName,
                             fvName,
                             fvVersion);
  }
}

class BatchFeatureStoreTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    setenv("RUNNING_UNIT_TESTS", "1", 1);
    RS_Status status = RonDBConnection::init_rondb_connection(
      globalConfigs.ronDB,
      globalConfigs.ronDBMetadataCluster);
    if (status.http_code !=
          static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
      errno = status.http_code;
      exit(errno);
    }
  }

  static void TearDownTestSuite() {
    unsetenv("RUNNING_UNIT_TESTS");
    RS_Status status = RonDBConnection::shutdown_rondb_connection();
    if (status.http_code !=
          static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
      errno = status.http_code;
      exit(errno);
    }
  }
};

// TODO RS_Status RunQueriesOnDataCluster(std::string sqlQueries)
// TODO TEST_F(FeatureStoreTest,
// Test_GetFeatureVector_Success_ComplexType_With_Schema_Change)

TEST_F(BatchFeatureStoreTest, DISABLED_Test_GetFeatureVector_Success_ComplexType) {
  auto fsName = FSDB002;
  const auto *fvName = "sample_complex_type";
  auto fvVersion = 1;
  auto [rows, pks, cols, status] =
    GetSampleData(fsName, "sample_complex_type_1");
  ASSERT_EQ(status.http_code,
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
    << "Cannot get sample data with error: " << status.message;
  metadata::AvroDecoder mapDecoder;
  metadata::AvroDecoder arrayDecoder;
  try {
    mapDecoder = metadata::AvroDecoder(
      "[\"null\",{\"type\":\"record\",\"name\":\"r854762204\",\"namespace\":\"struct\","
      "\"fields\":[{\"name\":\"int1\",\"type\":[\"null\",\"long\"]},{\"name\":\"int2\",\"type\":["
      "\"null\",\"long\"]}]}]");
  } catch (const std::exception &e) {
    FAIL() << "Failed to create mapDecoder: " << e.what();
  }
  try {
    arrayDecoder =
        metadata::AvroDecoder(
          "[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"long\"]}]");
  } catch (const std::exception &e) {
    FAIL() << "Failed to create arrayDecoder: " << e.what();
  }

  auto fsReq = CreateFeatureStoreRequest(
    fsName, fvName, fvVersion, pks, GetPkValuesBatch(rows, pks, cols), {}, {});
  fsReq.metadataRequest = {true, true};
  auto fsResp = GetFeatureStoreResponse(fsReq);
  for (auto row : rows) {
    // convert data to object in json format
    auto arrayJson = ConvertBinaryToJsonMessage(row[2]);
    auto [arrayPt, err1] = DeserialiseComplexFeature(
        std::vector<char>(arrayJson.begin(), arrayJson.end()), arrayDecoder);
    row[2] = arrayPt;
    if (err1 != nullptr) {
      FAIL() << "Cannot deserailize feature with error " << err1->GetReason();
    }
    // convert data to object in json format
    auto mapJson = ConvertBinaryToJsonMessage(row[3]);
    auto [mapPt, err2] =
        DeserialiseComplexFeature(
          std::vector<char>(mapJson.begin(), mapJson.end()), mapDecoder);
    row[3] = mapPt;
    if (err2 != nullptr) {
      FAIL() << "Cannot deserailize feature with error " << err2->GetReason();
    }
  }

  // validate
  ValidateBatchResponseWithData(rows, cols, *fsResp);
  ValidateResponseMetadata(fsResp->metadata,
                           fsReq.metadataRequest,
                           fsName,
                           fvName,
                           fvVersion);
}

int main(int argc, char **argv) {
  ndb_init();
  testing::InitGoogleTest(&argc, argv);
  testing::Environment* const my_env =
    testing::AddGlobalTestEnvironment(new MyEnvironment);
  (void)my_env;
  int rc = RUN_ALL_TESTS();
  ndb_end(0);
  return rc;
}
