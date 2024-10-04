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

#include "batch_feature_store_ctrl.hpp"
#include "buffer_manager.hpp"
#include "constants.hpp"
#include "encoding.hpp"
#include "feature_store_ctrl.hpp"
#include "feature_store_data_structs.hpp"
#include "feature_store_error_code.hpp"
#include "json_parser.hpp"
#include "metadata.hpp"
#include "fs_cache.hpp"
#include "pk_data_structs.hpp"

#include <drogon/HttpTypes.h>
#include <memory>
#include <simdjson.h>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <EventLogger.hpp>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_BFS_CTRL 1
#endif

#ifdef DEBUG_BFS_CTRL
#define DEB_BFS_CTRL(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_BFS_CTRL(arglist) do { } while (0)
#endif

void BatchFeatureStoreCtrl::batch_featureStore(
    const drogon::HttpRequestPtr &req,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback) {

  /**
   * Processing a Feature Store REST API request goes through a pipeline.
   * It is very similar to the handling of non-batched requests, but has
   * some handling that requires handling multiple values.
   *
   * The pipeline steps are:
   * 1. Receive the HTTP request from Drogon
   * 2. Parse the JSON request using simdjson
   * 3. Get the Feature Store metadata from the FS metadata cache
   * 4. Validate the primary keys
   * 5. Validate the passed features (Optional)
   * 6. Validate the Hopsworks API key in API Key cache
   * 7. Get and validate the columns to read
   * 8. Prepare the HTTP response packet
   * 9. Perform the batched PK read
   * 10. Process the response
   * 11. Convert response to JSON
   * 12. Get Feature values
   * 13. Fill passed features into response
   * 14. Callback to Drogon to send response HTTP packet
   */

  auto resp = drogon::HttpResponse::newHttpResponse();
  size_t currentThreadIndex = drogon::app().getCurrentThreadIndex();
  if (unlikely(currentThreadIndex >= globalConfigs.rest.numThreads)) {
    resp->setBody("Too many threads");
    resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
    callback(resp);
    return;
  }
  JSONParser& jsonParser = jsonParsers[currentThreadIndex];

  // Store it to the first string buffer
  const char *json_str = req->getBody().data();
  DEB_BFS_CTRL(("\n\n JSON REQUEST: \n %s \n", json_str));
  size_t length = req->getBody().length();
  if (unlikely(length > globalConfigs.internal.reqBufferSize)) {
    auto resp = drogon::HttpResponse::newHttpResponse();
    resp->setBody("Request too large");
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  memcpy(jsonParser.get_buffer().get(), json_str, length);

  feature_store_data_structs::BatchFeatureStoreRequest reqStruct;
  auto status = jsonParser.batch_feature_store_parse(
      simdjson::padded_string_view(jsonParser.get_buffer().get(), length,
                                   globalConfigs.internal.reqBufferSize +
                                   simdjson::SIMDJSON_PADDING),
      reqStruct);

  if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
                 drogon::HttpStatusCode::k200OK)) {
    resp->setBody(std::string(std::string("Error:") + status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }
  // Validate
  // Complete validation is delegated to Execute()
  // check if requested fv and fs exist
  auto [metadata, err] =
    metadata::FeatureViewMetadataCache_Get(
      reqStruct.featureStoreName,
      reqStruct.featureViewName,
      reqStruct.featureViewVersion);
  if (unlikely(err != nullptr)) {
    resp->setBody(err->Error());
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  if (unlikely(reqStruct.entries.empty())) {
    resp->setBody(NO_PRIMARY_KEY_GIVEN->GetReason());
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  if (unlikely(!reqStruct.passedFeatures.empty() &&
      reqStruct.entries.size() != reqStruct.passedFeatures.size())) {
    resp->setBody(INCORRECT_PASSED_FEATURE->NewMessage(
      "Length of passed feature does not equal to that of the entries "
      "provided in the request.")
                      ->GetMessage());
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  // Authenticate
  if (likely(globalConfigs.security.apiKey.useHopsworksAPIKeys)) {
    auto api_key = req->getHeader(API_KEY_NAME_LOWER_CASE);
    if (unlikely(err != nullptr)) {
      resp->setBody(err->Error());
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
    // Validate access right to ALL feature stores including shared feature
    auto status = authenticate(api_key, metadata->featureStoreNames);
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
                   drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode((drogon::HttpStatusCode)status.http_code);
      callback(resp);
      return;
    }
  }

  // Execute
  auto featureStatus =
      std::vector<feature_store_data_structs::FeatureStatus>(
        reqStruct.entries.size());
  auto detailedStatus =
    std::vector<std::vector<feature_store_data_structs::DetailedStatus>>();

  if (reqStruct.GetOptions().includeDetailedStatus) {
    detailedStatus =
      std::vector<std::vector<feature_store_data_structs::DetailedStatus>>(
        reqStruct.entries.size());
  }
  auto numPassed = checkFeatureStatus(reqStruct, *metadata, featureStatus);
  auto readParams =
    getBatchPkReadParamsMultipleEntries(*metadata,
                                        reqStruct.entries,
                                        featureStatus);
  auto fsResp = feature_store_data_structs::BatchFeatureStoreResponse();

  auto features = std::vector<std::vector<std::vector<char>>>();
  auto noOps    = readParams.size();
  if (unlikely(noOps != 0)) {
    // Validate batch pkread
    for (auto readParam : readParams) {
      // The request is always a POST, but not set in request parameters
      status = readParam.validate(false);
      if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
                     drogon::HttpStatusCode::k200OK)) {
        resp->setBody(
            TranslateRonDbError(drogon::HttpStatusCode::k400BadRequest,
                                status.message)->Error());
        resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
        callback(resp);
        return;
      }
    }
    auto dbResponseIntf = getPkReadResponseJson(numPassed, *metadata);
    // Execute batch pkread
    std::vector<RS_Buffer> reqBuffs(noOps);
    std::vector<RS_Buffer> respBuffs(noOps);
    for (unsigned long i = 0; i < noOps; i++) {
      RS_Buffer reqBuff  = rsBufferArrayManager.get_req_buffer();
      RS_Buffer respBuff = rsBufferArrayManager.get_resp_buffer();

      reqBuffs[i]  = reqBuff;
      respBuffs[i] = respBuff;

      status = create_native_request(readParams[i],
                                     reqBuff.buffer,
                                     respBuff.buffer);
      if (static_cast<drogon::HttpStatusCode>(status.http_code) !=
            drogon::HttpStatusCode::k200OK) {
        resp->setBody(std::string(status.message));
        resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
        callback(resp);
        return;
      }
      UintPtr length_ptr =
        reinterpret_cast<UintPtr>(reqBuff.buffer) +
          static_cast<UintPtr>(PK_REQ_LENGTH_IDX) * ADDRESS_SIZE;
      Uint32 *length_ptr_casted = reinterpret_cast<Uint32*>(length_ptr);
      reqBuffs[i].size = *length_ptr_casted;
    }
    // pk_batch_read
    status = pk_batch_read(noOps, reqBuffs.data(), respBuffs.data());
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
                   drogon::HttpStatusCode::k200OK)) {
      auto fsError = TranslateRonDbError(status.http_code, status.message);
      resp->setBody(fsError->Error());
      resp->setStatusCode(
        static_cast<drogon::HttpStatusCode>(fsError->GetStatus()));
      callback(resp);
      return;
    }
    status = process_responses(respBuffs, dbResponseIntf);
    if (unlikely(status.err_file_name[0] != '\0')) {
      auto fsError = TranslateRonDbError(status.http_code, status.message);
      resp->setBody(fsError->Error());
      resp->setStatusCode(
        static_cast<drogon::HttpStatusCode>(fsError->GetStatus()));
      callback(resp);
      return;
    }
    for (unsigned long i = 0; i < noOps; i++) {
      rsBufferArrayManager.return_resp_buffer(respBuffs[i]);
      rsBufferArrayManager.return_req_buffer(reqBuffs[i]);
    }
    std::tie(features, err) = getFeatureValuesMultipleEntries(
        dbResponseIntf,
        reqStruct.entries,
        *metadata,
        featureStatus,
        detailedStatus,
        reqStruct.GetOptions().includeDetailedStatus);
    if (unlikely(err != nullptr)) {
      resp->setBody(err->Error());
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
  } else {
    auto emptyFeatures =
      std::vector<std::vector<std::vector<char>>>(reqStruct.entries.size());
    features = emptyFeatures;
  }
  fsResp.status = featureStatus;
  fillPassedFeaturesMultipleEntries(features,
                                    reqStruct.passedFeatures,
                                    metadata->prefixFeaturesLookup,
                                    metadata->featureIndexLookup,
                                    featureStatus);
  fsResp.features = features;
  fsResp.metadata = GetFeatureMetadata(metadata, reqStruct.metadataRequest);
  resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
  auto fsRespStr = fsResp.to_string();
  resp->setBody(fsRespStr);
  resp->setStatusCode(drogon::HttpStatusCode::k200OK);
  callback(resp);
}

unsigned checkFeatureStatus(
  const feature_store_data_structs::BatchFeatureStoreRequest &fsReq,
  const metadata::FeatureViewMetadata &metadata,
  std::vector<feature_store_data_structs::FeatureStatus> &status) {
  auto cnt = std::unordered_map<unsigned, bool>();
  for (unsigned i = 0; i < fsReq.entries.size(); ++i) {
    if (ValidatePrimaryKey(fsReq.entries[i],
                           metadata.validPrimayKeys) != nullptr) {
      status[i] = feature_store_data_structs::FeatureStatus::Error;
      cnt[i]    = true;
    }
  }
  if (!fsReq.passedFeatures.empty() &&
      fsReq.GetOptions().validatePassedFeatures) {
    for (unsigned i = 0; i < fsReq.passedFeatures.size(); ++i) {
      if (ValidatePassedFeatures(fsReq.passedFeatures[i],
                                 metadata.prefixFeaturesLookup) !=
          nullptr) {
        status[i] = feature_store_data_structs::FeatureStatus::Error;
        cnt[i]    = true;
      }
    }
  }
  return status.size() - cnt.size();
}

std::tuple<std::vector<std::vector<std::vector<char>>>,
                       std::shared_ptr<RestErrorCode>>
getFeatureValuesMultipleEntries(
  BatchResponseJSON &batchResponse,
  const std::vector<std::unordered_map<std::string,
  std::vector<char>>> &entries,
  const metadata::FeatureViewMetadata &featureView,
  std::vector<feature_store_data_structs::FeatureStatus> &batchStatus,
  std::vector<std::vector<
    feature_store_data_structs::DetailedStatus>> &detailedStatus,
  bool includeDetailedStatus) {
  auto rondbBatchResult =
    std::vector<std::vector<PKReadResponseWithCodeJSON>>(batchStatus.size());
  auto batchResult =
    std::vector<std::vector<std::vector<char>>>(batchStatus.size());
  for (auto &response : batchResponse.getResult()) {
    auto operationId = response.getBody().getOperationID();
    auto separatorPos = operationId.find(SEQUENCE_SEPARATOR);
    auto splitOperationIdFirst = operationId.substr(0, separatorPos);
    auto splitOperationIdSecond = operationId.substr(separatorPos + 1);
    int seqNum = 0;
    try {
      seqNum = std::stoi(splitOperationIdFirst);
    } catch (...) {
      return std::make_tuple(std::vector<std::vector<std::vector<char>>>(),
                             std::make_shared<RestErrorCode>(
                               "Failed to parse sequence number.",
                               static_cast<int>(
                                 drogon::k500InternalServerError)));
    }
    response.setOperationId(splitOperationIdSecond);
    rondbBatchResult[seqNum].push_back(response);
  }

  for (unsigned i = 0; i < rondbBatchResult.size(); ++i) {
    if (!rondbBatchResult[i].empty()) {
      auto result =
          GetFeatureValues(rondbBatchResult[i],
                           entries[i],
                           featureView,
                           includeDetailedStatus);
      batchResult[i] = std::get<0>(result);
      batchStatus[i] = std::get<1>(result);
      if (includeDetailedStatus) {
        detailedStatus[i] = std::get<2>(result);
      }
    }
  }
  return {batchResult, nullptr};
}

std::vector<PKReadParams> getBatchPkReadParamsMultipleEntries(
    const metadata::FeatureViewMetadata &metadata,
    const std::vector<std::unordered_map<std::string,
    std::vector<char>>> &entries,
    std::vector<feature_store_data_structs::FeatureStatus> &status) {
  auto batchReadParams = std::vector<PKReadParams>();
  batchReadParams.reserve(metadata.numOfFeatures * entries.size());
  for (unsigned i = 0; i < entries.size(); ++i) {
    if (status[i] != feature_store_data_structs::FeatureStatus::Error) {
      for (auto &param : GetBatchPkReadParams(metadata, entries[i])) {
        auto oid = std::to_string(i) + SEQUENCE_SEPARATOR + param.operationId;
        param.operationId = oid;
        batchReadParams.push_back(param);
      }
    }
  }
  return batchReadParams;
}

BatchResponseJSON
  getPkReadResponseJson(int numEntries,
                        const metadata::FeatureViewMetadata &metadata) {
  auto response = BatchResponseJSON();
  response.Init(metadata.featureGroupFeatures.size() * numEntries);
  return response;
}

void fillPassedFeaturesMultipleEntries(
    std::vector<std::vector<std::vector<char>>> &features,
    const std::vector<std::unordered_map<
      std::string, std::vector<char>>> &passedFeatures,
    std::unordered_map<std::string,
      std::vector<metadata::FeatureMetadata>> &featureMetadata,
    std::unordered_map<std::string, int> &indexLookup,
    const std::vector<feature_store_data_structs::FeatureStatus> &status) {
  if (!passedFeatures.empty() && !passedFeatures.empty()) {
    for (unsigned i = 0; i < features.size(); ++i) {
      if (status[i] != feature_store_data_structs::FeatureStatus::Error) {
        FillPassedFeatures(features[i],
                           passedFeatures[i],
                           featureMetadata,
                           indexLookup);
      }
    }
  }
}
