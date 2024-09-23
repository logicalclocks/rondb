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

#include "feature_store_ctrl.hpp"
#include "feature_store_data_structs.hpp"
#include "feature_store_error_code.hpp"
#include "feature_util.hpp"
#include "json_parser.hpp"
#include "encoding.hpp"
#include "buffer_manager.hpp"
#include "pk_data_structs.hpp"
#include "api_key.hpp"
#include "rdrs_dal.hpp"
#include "src/constants.hpp"
#include "metadata.hpp"
#include "error_strings.h"

#include <cstddef>
#include <cstring>
#include <drogon/HttpTypes.h>
#include <iostream>
#include <memory>
#include <regex>
#include <simdjson.h>
#include <string>
#include <unordered_map>
#include <vector>

metadata::FeatureViewMetaDataCache fvMetaCache;

std::shared_ptr<RestErrorCode>
ValidatePrimaryKey(const std::unordered_map<std::string, std::vector<char>> &entries,
                   const std::unordered_map<std::string, bool> &validPrimaryKeys) {
  // Data type check of primary key will be delegated to rondb.
  if (entries.empty()) {
    return std::make_shared<RestErrorCode>(*INCORRECT_PRIMARY_KEY->NewMessage("No entries found"));
  }

  for (const auto &entry : entries) {
    const std::string &featureName = entry.first;
    if (validPrimaryKeys.find(featureName) == validPrimaryKeys.end()) {
      return std::make_shared<RestErrorCode>(
          *INCORRECT_PRIMARY_KEY->NewMessage("Provided primary key `" + featureName +
                                             "` does not belong to the set of primary keys."));
    }
  }
  return nullptr;
}

std::shared_ptr<RestErrorCode> ValidatePassedFeatures(
    const std::unordered_map<std::string, std::vector<char>> &passedFeatures,
    const std::unordered_map<std::string, std::vector<metadata::FeatureMetadata>> &features) {
  for (const auto &passedFeature : passedFeatures) {
    const std::string &featureName = passedFeature.first;
    const std::vector<char> &value = passedFeature.second;
    auto featuresIt                = features.find(featureName);
    if (featuresIt == features.end()) {
      return std::make_shared<RestErrorCode>(
          *FEATURE_NOT_EXIST->NewMessage("Feature `" + featureName +
                                         "` does not exist in the feature view or it is a label "
                                         "which cannot be a passed feature."));
    }
    for (const auto &feature : featuresIt->second) {
      auto err = ValidateFeatureType(value, feature.type);
      if (err) {
        return std::make_shared<RestErrorCode>(err);
      }
    }
  }
  return nullptr;
}

std::shared_ptr<RestErrorCode> ValidateFeatureType(const std::vector<char> &feature,
                                                   const std::string &featureType) {
  std::string got;
  std::shared_ptr<RestErrorCode> err;
  std::tie(got, err) = getJsonType(feature);

  if (err) {
    return std::make_shared<RestErrorCode>(*INCORRECT_FEATURE_VALUE->NewMessage(
        "Provided value " + std::string(feature.begin(), feature.end()) +
        " is not in correct JSON format. " + err->ToString()));
  }
  std::string expected = mapFeatureTypeToJsonType(featureType);
  if (got != expected) {
    return std::make_shared<RestErrorCode>(*WRONG_DATA_TYPE->NewMessage(
        "Got: '" + got + "', expected: '" + expected + "' (offline type: " + featureType + ")"));
  }
  return nullptr;
}

std::string mapFeatureTypeToJsonType(const std::string &featureType) {
  if (featureType == "boolean") {
    return JSON_BOOLEAN;
  }
  if (featureType == "tinyint" || featureType == "int" || featureType == "smallint" ||
      featureType == "bigint" || featureType == "float" || featureType == "double" ||
      featureType == "decimal") {
    return JSON_NUMBER;
  }
  if (featureType == "date" || featureType == "string" || featureType == "binary" ||
      featureType == "timestamp") {
    return JSON_STRING;
  }
  return JSON_OTHER;
}

std::tuple<std::string, std::shared_ptr<RestErrorCode>>
getJsonType(const std::vector<char> &jsonString) {
  simdjson::dom::parser parser;
  simdjson::dom::element element;
  auto error = parser.parse(std::string(jsonString.begin(), jsonString.end())).get(element);
  if (error != 0U) {
    return std::make_tuple("", std::make_shared<RestErrorCode>(
                                   *INCORRECT_FEATURE_VALUE->NewMessage("Failed to parse JSON.")));
  }

  if (element.is<double>()) {
    return std::make_tuple(JSON_NUMBER, nullptr);
  }
  if (element.is<std::string_view>()) {
    return std::make_tuple(JSON_STRING, nullptr);
  }
  if (element.is<bool>()) {
    return std::make_tuple(JSON_BOOLEAN, nullptr);
  }
  if (element.is_null()) {
    return std::make_tuple(JSON_NIL, nullptr);
  }
  return std::make_tuple(JSON_OTHER, nullptr);
}

std::shared_ptr<RestErrorCode> checkRondbResponse(const BatchResponseJSON &rondbResp) {
  for (const auto &result : rondbResp.getResult()) {
    if (result.getBody().getStatusCode() != drogon::k200OK &&
        result.getBody().getStatusCode() != drogon::k404NotFound) {
      return TranslateRonDbError(static_cast<int>(result.getBody().getStatusCode()),
                                 result.getMessage());
    }
  }
  return nullptr;
}

std::vector<feature_store_data_structs::FeatureMetadata>
GetFeatureMetadata(const std::shared_ptr<metadata::FeatureViewMetadata> &metadata,
                   const feature_store_data_structs::MetadataRequest &metaRequest) {
  auto featureMetadataArray =
      std::vector<feature_store_data_structs::FeatureMetadata>(metadata->numOfFeatures);
  for (const auto &[featureKey, prefixFeaturesLookup] : metadata->prefixFeaturesLookup) {
    for (const auto &featureMetadata : prefixFeaturesLookup) {
      if (auto it = metadata->featureIndexLookup.find(GetFeatureIndexKeyByFeature(featureMetadata));
          it != metadata->featureIndexLookup.end()) {
        auto featureMetadataResp = feature_store_data_structs::FeatureMetadata();
        if (metaRequest.featureName.has_value() && metaRequest.featureName.value()) {
          featureMetadataResp.name = featureKey;
        }
        if (metaRequest.featureType.has_value() && metaRequest.featureType.value()) {
          featureMetadataResp.type = featureMetadata.type;
        }
        (featureMetadataArray)[it->second] = featureMetadataResp;
      }
    }
  }
  return featureMetadataArray;
}

std::shared_ptr<RestErrorCode> TranslateRonDbError(int code, const std::string &err) {
  std::shared_ptr<RestErrorCode> fsError;
  if (err.find(ERROR_015) != std::string::npos) {  // Wrong data type.
    std::regex regex(R"regex(Expecting (\w+)\. Column: (\w+))regex");
    std::smatch match;
    if (std::regex_search(err, match, regex)) {
      std::string dataType   = match[1].str();
      std::string columnName = match[2].str();
      fsError = WRONG_DATA_TYPE->NewMessage("Primary key '" + columnName + "' should be in '" +
                                            dataType + "' format.");
    } else {
      fsError = WRONG_DATA_TYPE;
    }
  } else if (err.find(ERROR_014) != std::string::npos ||  // "Wrong primay-key column."
             err.find(ERROR_012) != std::string::npos) {  // "Column does not exist."
    fsError = INCORRECT_PRIMARY_KEY->NewMessage(err);
  } else if (err.find(ERROR_013) != std::string::npos) {  // "Wrong number of primary-key columns.")
    fsError = nullptr;  // Missing entry can happen and users can fill up missing features by passed
                        // featues
  } else {
    if (code == drogon::k400BadRequest) {
      fsError = READ_FROM_DB_FAIL_BAD_INPUT->NewMessage(err);
    } else {
      fsError = READ_FROM_DB_FAIL->NewMessage(err);
    }
  }
  return fsError;
}

std::tuple<std::vector<std::vector<char>>, feature_store_data_structs::FeatureStatus,
           std::vector<feature_store_data_structs::DetailedStatus>, std::shared_ptr<RestErrorCode>>
GetFeatureValues(const std::vector<PKReadResponseWithCodeJSON> &ronDbResult,
                 const std::unordered_map<std::string, std::vector<char>> &entries,
                 const metadata::FeatureViewMetadata &featureView, bool includeDetailedStatus) {
  auto featureValues = std::vector<std::vector<char>>(featureView.numOfFeatures);
  feature_store_data_structs::FeatureStatus status =
      feature_store_data_structs::FeatureStatus::Complete;
  auto arrDetailedStatus = std::vector<feature_store_data_structs::DetailedStatus>();
  arrDetailedStatus.reserve(ronDbResult.size());
  std::shared_ptr<RestErrorCode> err;

  for (const auto &response : ronDbResult) {
    if (includeDetailedStatus) {
      auto fgInt                  = 0;
      auto operationId            = response.getBody().getOperationID();
      auto separatorPos           = operationId.find('|');
      auto splitOperationIdFirst  = operationId.substr(0, separatorPos);
      auto splitOperationIdSecond = operationId.substr(separatorPos + 1);
      try {
        fgInt = std::stoi(splitOperationIdSecond);
      } catch (...) {
        // TODO err logger
        fgInt = -1;
      }
      feature_store_data_structs::DetailedStatus tmp{};
      tmp.featureGroupId = fgInt;
      tmp.httpStatus     = response.getBody().getStatusCode();
      arrDetailedStatus.push_back(tmp);
    }
    if (response.getBody().getStatusCode() == drogon::k404NotFound) {
      status = feature_store_data_structs::FeatureStatus::Missing;
    } else if (response.getBody().getStatusCode() == drogon::k400BadRequest) {
      if (response.getMessage().find(ERROR_013) !=
          std::string::npos)  // "Wrong number of primary-key columns."
        status =
            feature_store_data_structs::FeatureStatus::Missing;  // Missing entry can happen and
                                                                 // users can fill up missing
                                                                 // features by passed featues
      else {
        status = feature_store_data_structs::FeatureStatus::Error;
      }
    } else if (response.getBody().getStatusCode() != drogon::k200OK) {
      status = feature_store_data_structs::FeatureStatus::Error;
    }

    for (const auto &[featureName, value] : response.getBody().getData()) {
      std::string featureIndexKey = metadata::GetFeatureIndexKeyByFgIndexKey(
          response.getBody().getOperationID(), featureName);
      if (auto it = featureView.featureIndexLookup.find(featureIndexKey);
          it != featureView.featureIndexLookup.end()) {
        if (auto decoderIt = featureView.complexFeatures.find(featureIndexKey);
            decoderIt != featureView.complexFeatures.end()) {
          auto deserResult = DeserialiseComplexFeature(value, decoderIt->second);
          if (std::get<0>(deserResult).empty()) {
            status = feature_store_data_structs::FeatureStatus::Error;
            err    = DESERIALISE_FEATURE_FAIL->NewMessage("Feature name: " + featureName + "; " +
                                                          std::get<1>(deserResult)->Error());
          } else {
            (featureValues)[it->second] = std::get<0>(deserResult);
          }
        } else {
          (featureValues)[it->second] = value;
        }
      }
    }
  }

  // Fill in primary key value from request into the vector
  // If multiple matched entries are found, the priority of the entry follows the order in
  // `GetBatchPkReadParams` i.e required entry > entry with prefix > entry without prefix Reason why
  // it has to loop all entries for each `*JoinKeyMap` is to make sure the value are filled in
  // according to the priority. Otherwise the lower priority one can overwrite the previous assigned
  // entry if the later entry exists only in the lower priority map. Check
  // `Test_GetFeatureVector_TestCorrectPkValue*` for more detail.
  for (const auto &entry : entries) {
    std::string featureName = entry.first;
    auto value              = entry.second;
    // Get all join key linked to the provided feature name without prefix
    if (auto joinKeysWithoutPrefixIt = featureView.joinKeyMap.find(featureName);
        joinKeysWithoutPrefixIt != featureView.joinKeyMap.end()) {
      // TODO debug logger
      FillPrimaryKey(featureView, featureValues, joinKeysWithoutPrefixIt->second, value);
    }
  }

  for (const auto &entry : entries) {
    std::string featureName = entry.first;
    auto value              = entry.second;
    // Get all join key linked to the provided feature name with prefix.
    // Entry with prefix is prioritised and the value overwrites the previous assignment if
    // available.
    if (auto joinKeysWithPrefixIt = featureView.prefixJoinKeyMap.find(featureName);
        joinKeysWithPrefixIt != featureView.prefixJoinKeyMap.end()) {
      // TODO debug logger
      FillPrimaryKey(featureView, featureValues, joinKeysWithPrefixIt->second, value);
    }
  }

  for (const auto &entry : entries) {
    std::string featureName = entry.first;
    auto value              = entry.second;
    // Get all join key linked to the provided feature name with prefix.
    // Entry in RequiredJoinKeyMap is prioritised and the value overwrites the previous assignment
    // if available.
    if (auto joinKeysRequiredIt = featureView.requiredJoinKeyMap.find(featureName);
        joinKeysRequiredIt != featureView.requiredJoinKeyMap.end()) {
      // TODO debug logger
      FillPrimaryKey(featureView, featureValues, joinKeysRequiredIt->second, value);
    }
  }
  return {featureValues, status, arrDetailedStatus, err};
}

void FillPrimaryKey(const metadata::FeatureViewMetadata &featureView,
                    std::vector<std::vector<char>> &featureValues,
                    const std::vector<std::string> &joinKeys, const std::vector<char> &value) {
  std::string indexKey;
  // Get all join key linked to the provided feature name
  // TODO debug logger
  for (const auto &joinKey : joinKeys) {
    // Check if the join key are valid feature
    if (auto prefixFeaturesLookupsIt = featureView.prefixFeaturesLookup.find(joinKey);
        prefixFeaturesLookupsIt != featureView.prefixFeaturesLookup.end()) {
      for (const auto &prefixFeaturesLookup : prefixFeaturesLookupsIt->second) {
        indexKey = metadata::GetFeatureIndexKeyByFeature(prefixFeaturesLookup);
        // Get the index of the feature
        auto it = featureView.featureIndexLookup.find(indexKey);
        if (it != featureView.featureIndexLookup.end()) {
          (featureValues)[it->second] = value;
          // TODO debug logger
        }
      }
    }
  }
}

std::vector<PKReadParams>
GetBatchPkReadParams(const metadata::FeatureViewMetadata &metadata,
                     const std::unordered_map<std::string, std::vector<char>> &entries) {
  auto batchReadParams = std::vector<PKReadParams>();

  for (const auto &fgFeature : metadata.featureGroupFeatures) {
    std::string testDb = fgFeature.featureStoreName;
    std::string testTable =
        fgFeature.featureGroupName + "_" + std::to_string(fgFeature.featureGroupVersion);
    std::vector<PKReadFilter> filters;
    std::vector<PKReadReadColumn> columns;

    for (const auto &feature : fgFeature.features) {
      if (metadata.primaryKeyMap.find(metadata::GetServingKey(feature.joinIndex, feature.name)) ==
          metadata.primaryKeyMap.end()) {
        PKReadReadColumn readCol;
        readCol.column     = feature.name;
        readCol.returnType = to_string(DataReturnType::DEFAULT_DRT);
        columns.push_back(readCol);
      }
    }

    for (const auto &servingKey : fgFeature.primaryKeyMap) {

      // Fill in value of required entry as original entry may not be required.
      std::string pkCol = servingKey.featureName;

      if (entries.find(servingKey.requiredEntry) != entries.end()) {
        PKReadFilter filter;
        filter.column = pkCol;
        filter.value  = entries.at(servingKey.requiredEntry);
        filters.push_back(filter);
        // TODO debug logger
      } else if (entries.find(servingKey.prefix + servingKey.featureName) != entries.end()) {
        // Also Fallback and use feature name with prefix.
        PKReadFilter filter;
        filter.column = pkCol;
        filter.value  = entries.at(servingKey.prefix + servingKey.featureName);
        filters.push_back(filter);
        // TODO debug logger
      } else if (entries.find(servingKey.featureName) != entries.end()) {
        // Fallback and use the raw feature name so as to be consistent with python client.
        // Also add feature name with prefix.
        PKReadFilter filter;
        filter.column = pkCol;
        filter.value  = entries.at(servingKey.featureName);
        filters.push_back(filter);
        // TODO debug logger
      }
    }

    std::string opId = metadata::GetFeatureGroupKeyByTDFeature(fgFeature);
    auto param       = PKReadParams();
    param.path.db    = testDb;
    param.path.table = testTable;

    param.filters     = filters;
    param.readColumns = columns;
    param.operationId = opId;
    batchReadParams.push_back(param);
  }
  return batchReadParams;
}

BatchResponseJSON getPkReadResponseJSON(const metadata::FeatureViewMetadata &metadata) {
  auto response = BatchResponseJSON();
  response.setResult(std::vector<PKReadResponseWithCodeJSON>(metadata.featureGroupFeatures.size()));
  return response;
}

void FillPassedFeatures(
    std::vector<std::vector<char>> &features,
    const std::unordered_map<std::string, std::vector<char>> &passedFeatures,
    std::unordered_map<std::string, std::vector<metadata::FeatureMetadata>> featureMetadataMap,
    const std::unordered_map<std::string, int> &indexLookup) {
  if (!passedFeatures.empty()) {
    for (const auto &[featureName, passFeature] : passedFeatures) {
      auto featureMetadatas = featureMetadataMap[featureName];
      for (const auto &featureMetadata : featureMetadatas) {
        std::string lookupKey = GetFeatureIndexKeyByFeature(featureMetadata);
        if (auto it = indexLookup.find(lookupKey); it != indexLookup.end()) {
          features[it->second] = passFeature;
        }
      }
    }
  }
}

RS_Status process_responses(std::vector<RS_Buffer> &respBuffs, BatchResponseJSON &response) {
  for (unsigned int i = 0; i < respBuffs.size(); i++) {
    auto pkReadResponseWithCode = BatchResponseJSON::CreateNewSubResponse();
    auto pkReadResponse         = pkReadResponseWithCode.getBody();

    auto subRespStatus = process_pkread_response(respBuffs[i].buffer, pkReadResponse);
    if (!(subRespStatus.err_file_name[0] == '\0')) {
      std::cerr << "Error: " << subRespStatus.err_file_name << std::endl;
      return subRespStatus;
    }

    pkReadResponseWithCode.setBody(pkReadResponse);
    pkReadResponseWithCode.setMessage(subRespStatus.message);

    response.AddSubResponse(i, pkReadResponseWithCode);
  }
  return CRS_Status::SUCCESS.status;
}

void FeatureStoreCtrl::featureStore(
    const drogon::HttpRequestPtr &req,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
  auto resp                 = drogon::HttpResponse::newHttpResponse();
  size_t currentThreadIndex = drogon::app().getCurrentThreadIndex();
  if (currentThreadIndex >= globalConfigs.rest.numThreads) {
    resp->setBody("Too many threads");
    resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
    callback(resp);
    return;
  }

  // Store it to the first string buffer
  const char *json_str = req->getBody().data();
  size_t length        = req->getBody().length();
  if (length > globalConfigs.internal.reqBufferSize) {
    auto resp = drogon::HttpResponse::newHttpResponse();
    resp->setBody("Request too large");
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  memcpy(jsonParser.get_buffer(currentThreadIndex).get(), json_str, length);

  feature_store_data_structs::FeatureStoreRequest reqStruct;
  RS_Status status = jsonParser.feature_store_parse(
      currentThreadIndex,
      simdjson::padded_string_view(jsonParser.get_buffer(currentThreadIndex).get(), length,
                                   globalConfigs.internal.reqBufferSize + simdjson::SIMDJSON_PADDING),
      reqStruct);

  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    resp->setBody(std::string(std::string("Error:") + status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  // Validate
  auto [metadata, err] = fvMetaCache.Get(reqStruct.featureStoreName, reqStruct.featureViewName,
                                         reqStruct.featureViewVersion);

  if (err != nullptr) {
    resp->setBody(err->Error());
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  // TODO logger

  auto err1 = ValidatePrimaryKey(reqStruct.entries, metadata->validPrimayKeys);
  if (err1 != nullptr) {
    resp->setBody(err1->Error());
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  if (reqStruct.GetOptions().validatePassedFeatures) {
    auto err2 = ValidatePassedFeatures(reqStruct.passedFeatures, metadata->prefixFeaturesLookup);
    if (err2 != nullptr) {
      resp->setBody(err2->Error());
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
  }

  // Authenticate
  if (globalConfigs.security.apiKey.useHopsworksAPIKeys) {
    auto api_key = req->getHeader(API_KEY_NAME_LOWER_CASE);
    if (err != nullptr) {
      resp->setBody(err->Error());
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
    // Validate access right to ALL feature stores including shared feature
    auto status = apiKeyCache->validate_api_key(api_key, metadata->featureStoreNames);
    if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode(drogon::HttpStatusCode::k401Unauthorized);
      callback(resp);
      return;
    }
  }

  // Execute
  if (static_cast<drogon::HttpStatusCode>(status.http_code) == drogon::HttpStatusCode::k200OK) {
    auto readParams = GetBatchPkReadParams(*metadata, reqStruct.entries);
    // Perform batch pk read
    auto noOps = readParams.size();
    if (noOps == 0U) {
      auto fsError = INCORRECT_PRIMARY_KEY->NewMessage("Feature store does not exist");
      resp->setBody(fsError->Error());
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
    // Validate
    for (auto readParam : readParams) {
      // std::cout << readParam.to_string() << std::endl;

      status = readParam.validate();
      if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
        auto fsError = TranslateRonDbError(drogon::k400BadRequest, status.message);
        resp->setBody(fsError->Error());
        resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
        callback(resp);
        return;
      }
    }
    auto dbResponseIntf = getPkReadResponseJSON(*metadata);

    std::vector<RS_Buffer> reqBuffs(noOps);
    std::vector<RS_Buffer> respBuffs(noOps);

    for (unsigned long i = 0; i < noOps; i++) {
      RS_Buffer reqBuff  = rsBufferArrayManager.get_req_buffer();
      RS_Buffer respBuff = rsBufferArrayManager.get_resp_buffer();

      reqBuffs[i]  = reqBuff;
      respBuffs[i] = respBuff;

      status = create_native_request(readParams[i], reqBuff.buffer, respBuff.buffer);
      if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
        resp->setBody(std::string(status.message));
        resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
        callback(resp);
        return;
      }
      uintptr_t length_ptr = reinterpret_cast<uintptr_t>(reqBuff.buffer) +
                             static_cast<uintptr_t>(PK_REQ_LENGTH_IDX) * ADDRESS_SIZE;
      uint32_t *length_ptr_casted = reinterpret_cast<uint32_t *>(length_ptr);
      reqBuffs[i].size            = *length_ptr_casted;
    }

    // pk_batch_read
    status = pk_batch_read(noOps, reqBuffs.data(), respBuffs.data());
    if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
      auto fsError = TranslateRonDbError(status.http_code, status.message);
      resp->setBody(fsError->Error());
      resp->setStatusCode(static_cast<drogon::HttpStatusCode>(fsError->GetStatus()));
      callback(resp);
      return;
    }
    // TODO debug logger

    status = process_responses(respBuffs, dbResponseIntf);
    if (status.err_file_name[0] != '\0') {
      auto fsError = TranslateRonDbError(status.http_code, status.message);
      resp->setBody(fsError->Error());
      resp->setStatusCode(static_cast<drogon::HttpStatusCode>(fsError->GetStatus()));
      callback(resp);
      return;
    }

    // convert resp to json
    std::vector<PKReadResponseWithCodeJSON> responses = dbResponseIntf.getResult();

    for (unsigned long i = 0; i < noOps; i++) {
      rsBufferArrayManager.return_resp_buffer(respBuffs[i]);
      rsBufferArrayManager.return_req_buffer(reqBuffs[i]);
    }

    auto rondbResp = std::make_shared<BatchResponseJSON>();
    rondbResp->setResult(responses);
    auto fsError = checkRondbResponse(*rondbResp);
    if (fsError != nullptr) {
      resp->setBody(fsError->Error());
      resp->setStatusCode(static_cast<drogon::HttpStatusCode>(fsError->GetStatus()));
      callback(resp);
      return;
    }

    auto [features, status, detailedStatus, fsErr] =
        GetFeatureValues(rondbResp->getResult(), reqStruct.entries, *metadata,
                         reqStruct.GetOptions().includeDetailedStatus);
    if (fsErr != nullptr) {
      resp->setBody(fsErr->Error());
      resp->setStatusCode(static_cast<drogon::HttpStatusCode>(fsError->GetStatus()));
      callback(resp);
      return;
    }
    auto fsResp   = feature_store_data_structs::FeatureStoreResponse();
    fsResp.status = status;
    FillPassedFeatures(features, reqStruct.passedFeatures, metadata->prefixFeaturesLookup,
                       metadata->featureIndexLookup);
    fsResp.features = features;
    if (reqStruct.metadataRequest.featureName || reqStruct.metadataRequest.featureType) {
      fsResp.metadata = GetFeatureMetadata(metadata, reqStruct.metadataRequest);
    }
    if (reqStruct.GetOptions().includeDetailedStatus) {
      fsResp.detailedStatus = detailedStatus;
    }

    resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
    resp->setBody(fsResp.to_string());
    resp->setStatusCode(drogon::HttpStatusCode::k200OK);
    callback(resp);
  }
}
