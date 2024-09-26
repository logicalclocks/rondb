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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_BATCH_FEATURE_STORE_CTRL_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_BATCH_FEATURE_STORE_CTRL_HPP_

#include "api_key.hpp"
#include "constants.hpp"
#include "feature_store_error_code.hpp"
#include "metadata.hpp"
#include "feature_store_data_structs.hpp"

#include <drogon/drogon.h>
#include <drogon/HttpSimpleController.h>
#include <memory>
#include <tuple>
#include <vector>

class BatchFeatureStoreCtrl : public drogon::HttpController<BatchFeatureStoreCtrl> {
 public:
  METHOD_LIST_BEGIN
  ADD_METHOD_TO(BatchFeatureStoreCtrl::batch_featureStore, BATCH_FEATURE_STORE_PATH, drogon::Post);
  METHOD_LIST_END

  static void batch_featureStore(const drogon::HttpRequestPtr &req,
                                 std::function<void(const drogon::HttpResponsePtr &)> &&callback);
};

extern metadata::FeatureViewMetaDataCache batch_fvMetaCache;

unsigned checkFeatureStatus(const feature_store_data_structs::BatchFeatureStoreRequest &fsReq,
                            const metadata::FeatureViewMetadata &metadata,
                            std::vector<feature_store_data_structs::FeatureStatus> &status);
std::tuple<std::vector<std::vector<std::vector<char>>>, std::shared_ptr<RestErrorCode>>
getFeatureValuesMultipleEntries(
    BatchResponseJSON &batchResponse,
    const std::vector<std::unordered_map<std::string, std::vector<char>>> &entries,
    const metadata::FeatureViewMetadata &featureView,
    std::vector<feature_store_data_structs::FeatureStatus> &batchStatus,
    std::vector<std::vector<feature_store_data_structs::DetailedStatus>> &detailedStatus,
    bool includeDetailedStatus);
std::vector<PKReadParams> getBatchPkReadParamsMultipleEntries(
    const metadata::FeatureViewMetadata &metadata,
    const std::vector<std::unordered_map<std::string, std::vector<char>>> &entries,
    std::vector<feature_store_data_structs::FeatureStatus> &status);
BatchResponseJSON getPkReadResponseJson(int numEntries,
                                        const metadata::FeatureViewMetadata &metadata);
void fillPassedFeaturesMultipleEntries(
    std::vector<std::vector<std::vector<char>>> &features,
    const std::vector<std::unordered_map<std::string, std::vector<char>>> &passedFeatures,
    std::unordered_map<std::string, std::vector<metadata::FeatureMetadata>> &featureMetadata,
    std::unordered_map<std::string, int> &indexLookup,
    const std::vector<feature_store_data_structs::FeatureStatus> &status);
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_BATCH_FEATURE_STORE_CTRL_HPP_
