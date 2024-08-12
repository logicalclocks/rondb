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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_CTRL_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_CTRL_HPP_

#include "metadata.hpp"
#include "rdrs_dal.h"
#include "constants.hpp"
#include "feature_store_data_structs.hpp"
#include "operations_feature_store.hpp"
#include "feature_store_error_code.hpp"
#include "base_ctrl.hpp"

#include <drogon/drogon.h>
#include <drogon/HttpSimpleController.h>

const std::string JSON_NUMBER  = "NUMBER";
const std::string JSON_STRING  = "STRING";
const std::string JSON_BOOLEAN = "BOOLEAN";
const std::string JSON_NIL     = "NIL";
const std::string JSON_OTHER   = "OTHER";

class FeatureStoreCtrl : public drogon::HttpController<FeatureStoreCtrl> {
 public:
  METHOD_LIST_BEGIN
  ADD_METHOD_TO(FeatureStoreCtrl::featureStore, FEATURE_STORE_PATH, drogon::Post);
  METHOD_LIST_END

  static void featureStore(const drogon::HttpRequestPtr &req,
                           std::function<void(const drogon::HttpResponsePtr &)> &&callback);
};

extern metadata::FeatureViewMetaDataCache fvMetaCache;

std::shared_ptr<RestErrorCode>
ValidatePrimaryKey(const std::unordered_map<std::string, std::vector<char>> &entries,
                   const std::unordered_map<std::string, std::string> &features);
std::shared_ptr<RestErrorCode>
ValidatePassedFeatures(const std::unordered_map<std::string, std::vector<char>> &passedFeatures,
                       const std::unordered_map<std::string, metadata::FeatureMetadata> &features);
std::shared_ptr<RestErrorCode> ValidateFeatureType(const std::vector<char> &feature,
                                                   const std::string &featureType);
std::string mapFeatureTypeToJsonType(const std::string &featureType);
std::tuple<std::string, std::shared_ptr<RestErrorCode>>
getJsonType(const std::vector<char> &jsonString);
std::shared_ptr<RestErrorCode> checkRondbResponse(const BatchResponseJSON &rondbResp);
std::shared_ptr<std::vector<feature_store_data_structs::FeatureMetadata>>
GetFeatureMetadata(const metadata::FeatureViewMetadata &metadata,
                   const feature_store_data_structs::MetadataRequest &metaRequest);
std::shared_ptr<RestErrorCode> TranslateRonDbError(int code, const std::string &err);
std::tuple<std::vector<std::vector<char>>, feature_store_data_structs::FeatureStatus,
           std::shared_ptr<RestErrorCode>>
GetFeatureValues(const std::vector<PKReadResponseWithCodeJSON> &ronDbResult,
                 const std::unordered_map<std::string, std::vector<char>> &entries,
                 const metadata::FeatureViewMetadata &featureView);
std::vector<PKReadParams>
GetBatchPkReadParams(const metadata::FeatureViewMetadata &metadata,
                     const std::unordered_map<std::string, std::vector<char>> &entries);
void FillPassedFeatures(
std::vector<std::vector<char>> &features,
const std::unordered_map<std::string, std::vector<char>> &passedFeatures,
const std::unordered_map<std::string, metadata::FeatureMetadata> &featureMetadata,
const std::unordered_map<std::string, int> &indexLookup);

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_CTRL_HPP_
