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
#include "fs_cache.hpp"
#include "feature_store/feature_store.h"
#include "rdrs_rondb_connection_pool.hpp"
#include <NdbMutex.h>
#include <NdbSleep.h>

#include <drogon/HttpClient.h>
#include <drogon/HttpTypes.h>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <mysql.h>
#include <tuple>
#include <unordered_map>
#include <vector>

NdbMutex *globalConfigsMutex = nullptr;
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
  std::string base64Str;
  base64_decode(unquotedStr, base64Str);
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
      std::string decodedStr;
      base64_decode(gotStr, decodedStr);
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

class FeatureStoreTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
  }
  static void TearDownTestSuite() {
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
      globalConfigs.ronDBMetadataCluster,
      64);
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

TEST_F(FeatureStoreTest, TestFindAllFeatureViews) {
  Feature_View_Entry *entries = nullptr;
  int count = 0;
  RS_Status status = find_all_feature_views(&entries, &count);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    GTEST_SKIP() << "Skipping: hopsworks schema not available: " << status.message;
  }
  EXPECT_GT(count, 0) << "Expected at least one feature view in test data";
  free(entries);
}

TEST_F(FeatureStoreTest, TestPreload) {
  // Check if hopsworks schema is available
  Feature_View_Entry *entries = nullptr;
  int count = 0;
  RS_Status status = find_all_feature_views(&entries, &count);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    GTEST_SKIP() << "Skipping: hopsworks schema not available: " << status.message;
  }
  free(entries);

  // Start a fresh cache, preload, then verify entries are present
  start_fs_cache();
  ASSERT_NE(g_fs_metadata_cache, nullptr);

  g_fs_metadata_cache->preload_all_feature_views();

  // Verify a known feature view is now in cache and IS_VALID
  // fsdb002|sample_2|1 is used in TestFeatureStoreMetaData
  FSCacheEntry *entry = nullptr;
  auto *data = fs_metadata_cache_get(
    metadata::getFeatureViewCacheKey(FSDB002, "sample_2", 1), &entry);
  ASSERT_NE(entry, nullptr) << "Expected cache entry for fsdb002|sample_2|1";
  EXPECT_EQ(entry->m_state, FSCacheEntry::IS_VALID)
      << "Expected IS_VALID state for preloaded entry";
  EXPECT_NE(data, nullptr) << "Expected metadata for preloaded entry";
  if (data != nullptr) {
    EXPECT_EQ(data->featureViewName, "sample_2");
    EXPECT_EQ(data->featureViewVersion, 1);
  }
  fs_cache_dec_ref_count(reinterpret_cast<char*>(entry));

  stop_fs_cache();
}

TEST_F(FeatureStoreTest, TestPreloadIdempotent) {
  // Check if hopsworks schema is available
  Feature_View_Entry *entries = nullptr;
  int count = 0;
  RS_Status status = find_all_feature_views(&entries, &count);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    GTEST_SKIP() << "Skipping: hopsworks schema not available: " << status.message;
  }
  free(entries);

  start_fs_cache();
  ASSERT_NE(g_fs_metadata_cache, nullptr);

  g_fs_metadata_cache->preload_all_feature_views();

  // Get entry via a known feature view
  FSCacheEntry *entry1 = nullptr;
  auto *data1 = fs_metadata_cache_get(
    metadata::getFeatureViewCacheKey(FSDB002, "sample_2", 1), &entry1);
  ASSERT_NE(entry1, nullptr);
  ASSERT_NE(data1, nullptr);
  fs_cache_dec_ref_count(reinterpret_cast<char*>(entry1));

  // Preload again — should not create duplicates
  g_fs_metadata_cache->preload_all_feature_views();

  FSCacheEntry *entry2 = nullptr;
  auto *data2 = fs_metadata_cache_get(
    metadata::getFeatureViewCacheKey(FSDB002, "sample_2", 1), &entry2);
  ASSERT_NE(entry2, nullptr);
  EXPECT_EQ(entry1, entry2) << "Expected same cache entry after second preload";
  ASSERT_NE(data2, nullptr);
  fs_cache_dec_ref_count(reinterpret_cast<char*>(entry2));

  stop_fs_cache();
}

TEST_F(FeatureStoreTest, TestLazyLoadFallback) {
  // Check if hopsworks schema is available
  Feature_View_Entry *entries = nullptr;
  int count = 0;
  RS_Status status = find_all_feature_views(&entries, &count);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    GTEST_SKIP() << "Skipping: hopsworks schema not available: " << status.message;
  }
  free(entries);

  // Without preloading, verify lazy-load still works
  start_fs_cache();
  ASSERT_NE(g_fs_metadata_cache, nullptr);

  // Use the full cache-get path (which lazy-loads on miss)
  char *cache_entry_ptr = nullptr;
  auto [data, errorCode] = metadata::FeatureViewMetadataCache_Get(
    FSDB002, "sample_2", 1, &cache_entry_ptr);
  EXPECT_EQ(errorCode, nullptr) << "Lazy load should succeed: "
                                << (errorCode ? errorCode->ToString() : "");
  EXPECT_NE(data, nullptr) << "Expected metadata from lazy load";
  if (data != nullptr) {
    EXPECT_EQ(data->featureViewName, "sample_2");
  }
  if (cache_entry_ptr != nullptr) {
    fs_cache_dec_ref_count(cache_entry_ptr);
  }

  stop_fs_cache();
}

// ---- NDB helpers for feature_view event tests ----

extern RDRSRonDBConnectionPool *rdrsRonDBConnectionPool;

static void prepare_short_varchar(char *buf, const char *str, size_t len) {
  buf[0] = (char)len;
  memcpy(buf + 1, str, len);
}

static bool ndb_insert_feature_view(int id,
                                     const char *name,
                                     int feature_store_id,
                                     int creator,
                                     int version) {
  Ndb *ndb = nullptr;
  RS_Status rs = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb);
  if (rs.http_code != SUCCESS) return false;

  if (ndb->setDatabaseName(HOPSWORKS) != 0) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  const NdbDictionary::Table *tab = ndb->getDictionary()->getTable(FEATURE_VIEW);
  if (tab == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbTransaction *tx = ndb->startTransaction();
  if (tx == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbOperation *op = tx->getNdbOperation(tab);
  if (op == nullptr || op->insertTuple() != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (op->equal("id", id) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // name: varchar(63), short varchar (1-byte length prefix)
  char name_buf[FEATURE_VIEW_NAME_SIZE];
  memset(name_buf, 0, sizeof(name_buf));
  prepare_short_varchar(name_buf, name, strlen(name));
  if (op->setValue("name", name_buf) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (op->setValue("feature_store_id", feature_store_id) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (op->setValue("creator", creator) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (op->setValue("version", version) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // created: timestamp (4-byte seconds since epoch)
  Uint32 now = (Uint32)time(nullptr);
  if (op->setValue("created", now) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (tx->execute(NdbTransaction::Commit) != 0) {
    std::cerr << "NDB feature_view insert failed: " << tx->getNdbError().code
              << " " << tx->getNdbError().message << std::endl;
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  ndb->closeTransaction(tx);
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
  return true;
}

static bool ndb_delete_feature_view_by_id(int id) {
  Ndb *ndb = nullptr;
  RS_Status rs = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb);
  if (rs.http_code != SUCCESS) return false;

  if (ndb->setDatabaseName(HOPSWORKS) != 0) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  const NdbDictionary::Table *tab = ndb->getDictionary()->getTable(FEATURE_VIEW);
  if (tab == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbTransaction *tx = ndb->startTransaction();
  if (tx == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbOperation *op = tx->getNdbOperation(tab);
  if (op == nullptr || op->deleteTuple() != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (op->equal("id", id) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (tx->execute(NdbTransaction::Commit) != 0) {
    std::cerr << "NDB feature_view delete failed: " << tx->getNdbError().code
              << " " << tx->getNdbError().message << std::endl;
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  ndb->closeTransaction(tx);
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
  return true;
}

TEST_F(FeatureStoreTest, TestEventInsert) {
  Feature_View_Entry *entries = nullptr;
  int count = 0;
  RS_Status status = find_all_feature_views(&entries, &count);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    GTEST_SKIP() << "Skipping: hopsworks schema not available: " << status.message;
  }
  free(entries);

  start_fs_cache();
  ASSERT_NE(g_fs_metadata_cache, nullptr);

  const int test_id = 99999;
  const char *test_fv_name = "evt_insert_test";
  const int test_fs_id = 67;   // fsdb001
  const int test_creator = 10000;
  const int test_version = 1;

  // Cleanup leftover from any previous failed run
  ndb_delete_feature_view_by_id(test_id);

  g_fs_metadata_cache->preload_all_feature_views();
  g_fs_metadata_cache->start_event_watcher();

  // Give event watcher time to subscribe
  NdbSleep_MilliSleep(2000);

  std::string cacheKey = metadata::getFeatureViewCacheKey(
    FSDB001, test_fv_name, test_version);

  // Verify not in cache yet
  FSCacheEntry *entry_before = nullptr;
  auto *data_before = fs_metadata_cache_get(cacheKey, &entry_before);
  // This creates an IS_FILLING entry; complete it so cleanup doesn't hang
  if (entry_before != nullptr && data_before == nullptr) {
    fs_metadata_update_cache(nullptr, entry_before,
      FV_NOT_EXIST->NewMessage("entry created by test"));
    entry_before->m_ref_count--;
  }

  // Insert the row via NDB
  ASSERT_TRUE(ndb_insert_feature_view(test_id, test_fv_name, test_fs_id,
                                       test_creator, test_version))
      << "Failed to insert test feature view via NDB";

  // Wait for event watcher to detect the INSERT
  NdbSleep_MilliSleep(3000);

  // The event watcher detected the INSERT and called load_single_feature_view,
  // but the test FV has no training_dataset_join data, so the metadata load
  // failed.  Failed loads are NOT cached — only successful loads are added to
  // the cache.  Verify the entry is not in cache by checking that
  // fs_metadata_cache_get creates a fresh IS_FILLING entry.
  FSCacheEntry *entry_after = nullptr;
  auto *data_after = fs_metadata_cache_get(cacheKey, &entry_after);
  ASSERT_NE(entry_after, nullptr);
  (void)data_after;
  EXPECT_EQ(entry_after->m_state, FSCacheEntry::IS_FILLING)
      << "Event watcher should not cache errors from incomplete FVs; "
         "entry should be freshly created (IS_FILLING) by our get call";
  // Complete the IS_FILLING entry so cache cleanup doesn't hang
  fs_metadata_update_cache(nullptr, entry_after,
    FV_NOT_EXIST->NewMessage("entry created by test verification"));
  entry_after->m_ref_count--;

  // Cleanup
  ASSERT_TRUE(ndb_delete_feature_view_by_id(test_id))
      << "Failed to cleanup test feature view";

  stop_fs_cache();
}

TEST_F(FeatureStoreTest, TestEventDelete) {
  Feature_View_Entry *entries = nullptr;
  int count = 0;
  RS_Status status = find_all_feature_views(&entries, &count);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    GTEST_SKIP() << "Skipping: hopsworks schema not available: " << status.message;
  }
  free(entries);

  const int test_id = 99998;
  const char *test_fv_name = "evt_delete_test";
  const int test_fs_id = 67;   // fsdb001
  const int test_creator = 10000;
  const int test_version = 1;

  // Insert a test row that we'll delete (no child rows = no FK issues)
  ndb_delete_feature_view_by_id(test_id);  // cleanup any leftover
  ASSERT_TRUE(ndb_insert_feature_view(test_id, test_fv_name, test_fs_id,
                                       test_creator, test_version))
      << "Failed to insert test feature view for delete test";

  start_fs_cache();
  ASSERT_NE(g_fs_metadata_cache, nullptr);

  // Preload won't cache this test FV (no training_dataset_join data, so
  // metadata load fails and errors are not cached).  Instead, populate the
  // cache entry via the lazy-load path: get creates an IS_FILLING entry,
  // update_cache transitions it to IS_INVALID.
  std::string cacheKey = metadata::getFeatureViewCacheKey(
    FSDB001, test_fv_name, test_version);
  FSCacheEntry *entry = nullptr;
  auto *data = fs_metadata_cache_get(cacheKey, &entry);
  ASSERT_NE(entry, nullptr) << "get should create a new cache entry";
  fs_metadata_update_cache(nullptr, entry,
    FV_NOT_EXIST->NewMessage("test entry for delete test"));
  fs_cache_dec_ref_count(reinterpret_cast<char*>(entry));

  g_fs_metadata_cache->start_event_watcher();

  // Give event watcher time to subscribe
  NdbSleep_MilliSleep(2000);

  // Delete the row via NDB
  ASSERT_TRUE(ndb_delete_feature_view_by_id(test_id))
      << "Failed to delete feature view via NDB";

  // Wait for event watcher to detect the DELETE
  NdbSleep_MilliSleep(3000);

  // Verify the entry is evicted or marked IS_INVALID
  FSCacheEntry *entry_after = nullptr;
  auto *data_after = fs_metadata_cache_get(cacheKey, &entry_after);
  if (entry_after != nullptr) {
    if (data_after == nullptr &&
        entry_after->m_state == FSCacheEntry::IS_FILLING) {
      // Entry was newly created (IS_FILLING) because old one was evicted.
      // Complete it so cleanup doesn't hang.
      fs_metadata_update_cache(nullptr, entry_after,
        FV_NOT_EXIST->NewMessage("entry created by test"));
    }
    EXPECT_NE(entry_after->m_state, FSCacheEntry::IS_VALID)
        << "Entry should not be IS_VALID after DELETE event";
    fs_cache_dec_ref_count(reinterpret_cast<char*>(entry_after));
  }
  // entry_after == nullptr is also acceptable (fully evicted + cache stopped)

  stop_fs_cache();
}

static bool ndb_update_feature_view_description(int id,
                                                 const char *description) {
  Ndb *ndb = nullptr;
  RS_Status rs = rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb);
  if (rs.http_code != SUCCESS) return false;

  if (ndb->setDatabaseName(HOPSWORKS) != 0) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  const NdbDictionary::Table *tab = ndb->getDictionary()->getTable(FEATURE_VIEW);
  if (tab == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbTransaction *tx = ndb->startTransaction();
  if (tx == nullptr) {
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  NdbOperation *op = tx->getNdbOperation(tab);
  if (op == nullptr || op->updateTuple() != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (op->equal("id", id) != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  // description: varchar(10000), medium varchar (2-byte length prefix)
  size_t desc_len = strlen(description);
  char *desc_buf = (char *)calloc(1, desc_len + 3);
  desc_buf[0] = (char)(desc_len & 0xFF);
  desc_buf[1] = (char)((desc_len >> 8) & 0xFF);
  memcpy(desc_buf + 2, description, desc_len);
  int rc = op->setValue("description", desc_buf);
  free(desc_buf);
  if (rc != 0) {
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  if (tx->execute(NdbTransaction::Commit) != 0) {
    std::cerr << "NDB feature_view update failed: " << tx->getNdbError().code
              << " " << tx->getNdbError().message << std::endl;
    ndb->closeTransaction(tx);
    rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
    return false;
  }

  ndb->closeTransaction(tx);
  rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &rs);
  return true;
}

// Verify that TE_UPDATE events on feature_view do NOT invalidate the cache.
// Description changes are irrelevant to serving metadata.
TEST_F(FeatureStoreTest, TestEventUpdateIgnored) {
  Feature_View_Entry *entries = nullptr;
  int count = 0;
  RS_Status status = find_all_feature_views(&entries, &count);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    GTEST_SKIP() << "Skipping: hopsworks schema not available: " << status.message;
  }
  free(entries);

  start_fs_cache();
  ASSERT_NE(g_fs_metadata_cache, nullptr);

  g_fs_metadata_cache->preload_all_feature_views();
  g_fs_metadata_cache->start_event_watcher();

  // Give event watcher time to subscribe
  NdbSleep_MilliSleep(2000);

  // Verify fsdb002|sample_2|1 is cached and IS_VALID
  std::string cacheKey = metadata::getFeatureViewCacheKey(FSDB002, "sample_2", 1);
  FSCacheEntry *entry = nullptr;
  auto *data = fs_metadata_cache_get(cacheKey, &entry);
  ASSERT_NE(entry, nullptr);
  ASSERT_EQ(entry->m_state, FSCacheEntry::IS_VALID);
  ASSERT_NE(data, nullptr);
  int fv_id = data->featureViewId;
  fs_cache_dec_ref_count(reinterpret_cast<char*>(entry));

  // Update the description via NDB (triggers TE_UPDATE event)
  ASSERT_TRUE(ndb_update_feature_view_description(fv_id, "test_update_ignored"))
      << "Failed to update feature_view description";

  // Wait for any event processing
  NdbSleep_MilliSleep(3000);

  // Verify the entry is still IS_VALID (UPDATE should be ignored)
  FSCacheEntry *entry_after = nullptr;
  auto *data_after = fs_metadata_cache_get(cacheKey, &entry_after);
  ASSERT_NE(entry_after, nullptr);
  EXPECT_EQ(entry_after->m_state, FSCacheEntry::IS_VALID)
      << "Entry should still be IS_VALID after UPDATE event";
  EXPECT_NE(data_after, nullptr)
      << "Metadata should still be present after UPDATE event";
  EXPECT_EQ(entry, entry_after)
      << "Should be the same cache entry (not reloaded)";
  fs_cache_dec_ref_count(reinterpret_cast<char*>(entry_after));

  // Restore original description
  ndb_update_feature_view_description(fv_id, "");

  stop_fs_cache();
}

// Verify that evict_entry marks IS_INVALID (rather than deleting) when
// the entry has ref_count > 0.
TEST_F(FeatureStoreTest, TestEvictWhileInUse) {
  Feature_View_Entry *entries = nullptr;
  int count = 0;
  RS_Status status = find_all_feature_views(&entries, &count);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    GTEST_SKIP() << "Skipping: hopsworks schema not available: " << status.message;
  }
  free(entries);

  const int test_id = 99997;
  const char *test_fv_name = "evt_inuse_test";
  const int test_fs_id = 67;   // fsdb001
  const int test_creator = 10000;
  const int test_version = 1;

  // Insert a test row
  ndb_delete_feature_view_by_id(test_id);
  ASSERT_TRUE(ndb_insert_feature_view(test_id, test_fv_name, test_fs_id,
                                       test_creator, test_version))
      << "Failed to insert test feature view";

  start_fs_cache();
  ASSERT_NE(g_fs_metadata_cache, nullptr);

  // Preload won't cache this test FV (incomplete metadata).  Create the
  // cache entry via lazy-load and hold a reference to simulate an in-flight
  // request.
  std::string cacheKey = metadata::getFeatureViewCacheKey(
    FSDB001, test_fv_name, test_version);
  FSCacheEntry *entry = nullptr;
  fs_metadata_cache_get(cacheKey, &entry);
  ASSERT_NE(entry, nullptr);
  // Complete the IS_FILLING entry → IS_INVALID.  ref_count is still 1
  // (held by us, simulating an in-flight request).
  fs_metadata_update_cache(nullptr, entry,
    FV_NOT_EXIST->NewMessage("test entry for evict test"));

  g_fs_metadata_cache->start_event_watcher();
  NdbSleep_MilliSleep(2000);

  // Delete the row while we hold a reference
  ASSERT_TRUE(ndb_delete_feature_view_by_id(test_id))
      << "Failed to delete feature view";

  // Wait for event watcher to detect the DELETE
  NdbSleep_MilliSleep(3000);

  // Entry should be marked IS_INVALID (not deleted, because ref_count > 0)
  EXPECT_EQ(entry->m_state, FSCacheEntry::IS_INVALID)
      << "Entry should be IS_INVALID when evicted while in use";
  EXPECT_NE(entry->m_errorCode, nullptr)
      << "Error code should be set on evicted entry";

  // Release our reference
  fs_cache_dec_ref_count(reinterpret_cast<char*>(entry));

  stop_fs_cache();
}

class BatchFeatureStoreTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    RS_Status status = RonDBConnection::init_rondb_connection(
      globalConfigs.ronDB,
      globalConfigs.ronDBMetadataCluster,
      64);
    if (status.http_code !=
          static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
      errno = status.http_code;
      exit(errno);
    }
  }

  static void TearDownTestSuite() {
    RS_Status status = RonDBConnection::shutdown_rondb_connection();
    if (status.http_code !=
          static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
      errno = status.http_code;
      exit(errno);
    }
  }
};

// Test that preload_all_feature_views() picks up new entries inserted during
// an event gap (simulates what happens on event watcher reconnect).
TEST_F(FeatureStoreTest, TestReconnectPreloadPicksUpNewEntry) {
  Feature_View_Entry *entries = nullptr;
  int count = 0;
  RS_Status status = find_all_feature_views(&entries, &count);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    GTEST_SKIP() << "Skipping: hopsworks schema not available: " << status.message;
  }
  free(entries);

  start_fs_cache();
  ASSERT_NE(g_fs_metadata_cache, nullptr);

  // Initial preload (simulates startup)
  g_fs_metadata_cache->preload_all_feature_views();

  const int test_id = 99996;
  const char *test_fv_name = "reconnect_test";
  const int test_fs_id = 67;   // fsdb001
  const int test_creator = 10000;
  const int test_version = 1;

  // Cleanup leftover from previous runs
  ndb_delete_feature_view_by_id(test_id);

  std::string cacheKey = metadata::getFeatureViewCacheKey(
    FSDB001, test_fv_name, test_version);

  // Verify not in cache yet
  FSCacheEntry *entry_before = nullptr;
  auto *data_before = fs_metadata_cache_get(cacheKey, &entry_before);
  if (entry_before != nullptr && data_before == nullptr) {
    fs_metadata_update_cache(nullptr, entry_before,
      FV_NOT_EXIST->NewMessage("entry created by test"));
    fs_cache_dec_ref_count(reinterpret_cast<char*>(entry_before));
  }

  // Insert a new row (simulating INSERT during event gap)
  ASSERT_TRUE(ndb_insert_feature_view(test_id, test_fv_name, test_fs_id,
                                       test_creator, test_version))
      << "Failed to insert test feature view";

  // Preload again (this is what the event watcher does on reconnect)
  g_fs_metadata_cache->preload_all_feature_views();

  // The test FV has no training_dataset_join data, so the metadata load
  // fails.  Failed loads are not cached.  Verify that the entry is not in
  // cache (fs_metadata_cache_get creates a fresh IS_FILLING entry).
  FSCacheEntry *entry_after = nullptr;
  auto *data_after = fs_metadata_cache_get(cacheKey, &entry_after);
  ASSERT_NE(entry_after, nullptr);
  (void)data_after;
  EXPECT_EQ(entry_after->m_state, FSCacheEntry::IS_FILLING)
      << "Preload should not cache errors from incomplete FVs";
  fs_metadata_update_cache(nullptr, entry_after,
    FV_NOT_EXIST->NewMessage("entry created by test verification"));
  entry_after->m_ref_count--;

  // Cleanup
  ndb_delete_feature_view_by_id(test_id);
  stop_fs_cache();
}

// Test that preload on reconnect removes stale IS_INVALID entries.
// Simulates the scenario where:
//   1. A lazy-load request creates an IS_INVALID entry (metadata load failed)
//   2. Event watcher reconnects and calls preload_all_feature_views()
//   3. Preload removes the IS_INVALID entry and attempts a fresh load
//   4. Since the underlying metadata issue hasn't changed, the fresh load
//      also fails and nothing is cached (errors are not cached by preload)
TEST_F(FeatureStoreTest, TestReconnectPreloadRemovesInvalidEntry) {
  Feature_View_Entry *entries = nullptr;
  int count = 0;
  RS_Status status = find_all_feature_views(&entries, &count);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    GTEST_SKIP() << "Skipping: hopsworks schema not available: " << status.message;
  }
  free(entries);

  start_fs_cache();
  ASSERT_NE(g_fs_metadata_cache, nullptr);

  const int test_id = 99994;
  const char *test_fv_name = "reconnect_inv";
  const int test_fs_id = 67;   // fsdb001
  const int test_creator = 10000;
  const int test_version = 1;

  ndb_delete_feature_view_by_id(test_id);

  // Insert a test FV row (no training_dataset_join data, so metadata
  // load will fail).
  ASSERT_TRUE(ndb_insert_feature_view(test_id, test_fv_name, test_fs_id,
                                       test_creator, test_version))
      << "Failed to insert test feature view";

  std::string cacheKey = metadata::getFeatureViewCacheKey(
    FSDB001, test_fv_name, test_version);

  // Create an IS_INVALID entry via the lazy-load path (simulating a request
  // that failed to load metadata).
  FSCacheEntry *entry1 = nullptr;
  auto *data1 = fs_metadata_cache_get(cacheKey, &entry1);
  ASSERT_NE(entry1, nullptr) << "get should create a new cache entry";
  (void)data1;
  fs_metadata_update_cache(nullptr, entry1,
    FV_NOT_EXIST->NewMessage("simulated lazy-load failure"));
  fs_cache_dec_ref_count(reinterpret_cast<char*>(entry1));

  // Preload (simulates reconnect).  load_single_feature_view detects
  // the IS_INVALID entry, removes it, and attempts a fresh load.
  // The fresh load also fails (still no training_dataset_join data),
  // but errors are not cached by preload, so the entry is gone.
  g_fs_metadata_cache->preload_all_feature_views();

  // Verify the IS_INVALID entry was removed: fs_metadata_cache_get creates
  // a fresh IS_FILLING entry (proving the old IS_INVALID is gone).
  FSCacheEntry *entry2 = nullptr;
  auto *data2 = fs_metadata_cache_get(cacheKey, &entry2);
  ASSERT_NE(entry2, nullptr);
  (void)data2;
  EXPECT_EQ(entry2->m_state, FSCacheEntry::IS_FILLING)
      << "Old IS_INVALID entry should have been removed by preload";
  // Clean up the IS_FILLING entry
  fs_metadata_update_cache(nullptr, entry2,
    FV_NOT_EXIST->NewMessage("entry created by test verification"));
  entry2->m_ref_count--;

  // Cleanup
  ndb_delete_feature_view_by_id(test_id);
  stop_fs_cache();
}

// End-to-end reconnect test: forces the event watcher to tear down and
// reconnect, then verifies that a feature view inserted during the gap
// is picked up by the reconnect preload.
TEST_F(FeatureStoreTest, TestEndToEndReconnect) {
  Feature_View_Entry *entries = nullptr;
  int count = 0;
  RS_Status status = find_all_feature_views(&entries, &count);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    GTEST_SKIP() << "Skipping: hopsworks schema not available: " << status.message;
  }
  free(entries);

  start_fs_cache();
  ASSERT_NE(g_fs_metadata_cache, nullptr);

  g_fs_metadata_cache->preload_all_feature_views();
  g_fs_metadata_cache->start_event_watcher();

  // Wait for event watcher to subscribe
  NdbSleep_MilliSleep(2000);

  const int test_id = 99992;
  const char *test_fv_name = "e2e_reconnect";
  const int test_fs_id = 67;   // fsdb001
  const int test_creator = 10000;
  const int test_version = 1;

  ndb_delete_feature_view_by_id(test_id);

  // Force the event watcher to disconnect.  This triggers the full
  // err: path (tear down subscription → backoff sleep → retry:).
  g_fs_metadata_cache->force_reconnect();

  // Wait for the watcher to enter backoff sleep (1s), then insert during
  // the gap so the INSERT event is missed.
  NdbSleep_MilliSleep(500);

  ASSERT_TRUE(ndb_insert_feature_view(test_id, test_fv_name, test_fs_id,
                                       test_creator, test_version))
      << "Failed to insert test feature view";

  // Wait for: remaining backoff (0.5s) + reconnect setup + preload
  NdbSleep_MilliSleep(5000);

  std::string cacheKey = metadata::getFeatureViewCacheKey(
    FSDB001, test_fv_name, test_version);

  // The reconnect preload attempted to load the test FV but it has no
  // training_dataset_join data, so the metadata load failed.  Failed loads
  // are not cached.  Verify the entry is not in cache.
  FSCacheEntry *entry = nullptr;
  auto *data = fs_metadata_cache_get(cacheKey, &entry);
  ASSERT_NE(entry, nullptr);
  (void)data;
  EXPECT_EQ(entry->m_state, FSCacheEntry::IS_FILLING)
      << "Reconnect preload should not cache errors from incomplete FVs";
  // Complete the IS_FILLING entry so cache cleanup doesn't hang
  fs_metadata_update_cache(nullptr, entry,
    FV_NOT_EXIST->NewMessage("entry created by test verification"));
  entry->m_ref_count--;

  // Cleanup
  ndb_delete_feature_view_by_id(test_id);
  stop_fs_cache();
}

int main(int argc, char **argv) {
  ndb_init();
  globalConfigsMutex = NdbMutex_Create();

  // Load config from RDRS_CONFIG_FILE (needed for correct NDB connection settings)
  std::string configFile;
  const char *env_config_file_path = std::getenv("RDRS_CONFIG_FILE");
  if (env_config_file_path != nullptr) {
    configFile = env_config_file_path;
  }
  RS_Status status = AllConfigs::init(configFile);
  if (status.http_code !=
        static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    std::cerr << "Error loading config: " << status.message << std::endl;
    return 1;
  }

  testing::InitGoogleTest(&argc, argv);
  testing::Environment* const my_env =
    testing::AddGlobalTestEnvironment(new MyEnvironment);
  (void)my_env;
  int rc = RUN_ALL_TESTS();
  NdbMutex_Destroy(globalConfigsMutex);
  ndb_end(0);
  return rc;
}
