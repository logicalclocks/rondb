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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_OPERATIONS_FEATURE_STORE_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_OPERATIONS_FEATURE_STORE_HPP_

#include "rdrs_dal.h"
#include "rdrs_const.h"
#include "feature_store/feature_store.h"
#include "rdrs_dal.hpp"
#include <memory>
#include <stdlib.h>

#include <simdjson.h>
#include <drogon/HttpTypes.h>
#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <stdexcept>

struct TrainingDatasetFeature {
  int featureID;
  int trainingDataset;
  int featureGroupID;  // When FG Id is null in DB, the value here is 0. Fg Id starts with 1.
  std::string name;
  std::string type;
  int tdJoinID;
  int idx;
  int label;
  int featureViewID;
};

struct TrainingDatasetJoin {
  int id;
  std::string prefix;
  int index;
};

std::tuple<int, RS_Status> GetProjectID(const std::string &featureStoreName);

std::tuple<int, RS_Status> GetFeatureStoreID(const std::string &featureStoreName);

std::tuple<int, RS_Status> GetFeatureViewID(int featureStoreID, const std::string &featureViewName,
                                            int featureViewVersion);

std::tuple<std::vector<TrainingDatasetJoin>, RS_Status>
GetTrainingDatasetJoinData(int featureViewID);

struct FeatureGroup {
  std::string name;
  int featureStoreId;
  int version;
  bool onlineEnabled;
};

std::tuple<FeatureGroup, RS_Status> GetFeatureGroupData(int featureGroupID);
std::tuple<std::vector<TrainingDatasetFeature>, RS_Status>
GetTrainingDatasetFeature(int featureViewID);
std::tuple<std::string, RS_Status> GetFeatureStoreName(int fsId);

struct ServingKey {
  int featureGroupId;
  std::string featureName;
  std::string prefix;
  bool required;
  std::string joinOn;
  int joinIndex;
  std::string requiredEntry;
  std::string to_string() const {
    std::ostringstream oss;
    oss << "ServingKey {"
        << "\n  featureGroupId: " << featureGroupId << "\n  featureName: " << featureName
        << "\n  prefix: " << prefix << "\n  required: " << required << "\n  joinOn: " << joinOn
        << "\n  joinIndex: " << joinIndex << "\n  requiredEntry: " << requiredEntry << "\n}";
    return oss.str();
  }
};

std::tuple<std::vector<ServingKey>, RS_Status> GetServingKeys(int featureViewId);

struct AvroField {
  std::string name;  // json:"name"
  std::string type;  // json:"type"
};

struct FeatureGroupAvroSchema {
  std::string type;
  std::string name;
  std::string namespace_;
  std::vector<AvroField> fields;

  std::tuple<std::string, RS_Status> getSchemaByFeatureName(const std::string &featureName) const {
    for (const auto &field : fields) {
      if (field.name == featureName) {
        return {field.type, CRS_Status().status};
      }
    }
    return {"", CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                           std::string("Cannot find schema for feature ") + featureName)
                    .status};
  }

  // Parse from a simdjson document
  void from_json(const simdjson::dom::element &elem) {
    std::string_view type_view;
    std::string_view name_view;
    std::string_view namespace_view;
    // Parse each field from the JSON object
    elem["type"].get(type_view);
    elem["name"].get(name_view);
    elem["namespace"].get(namespace_view);
    type       = std::string(type_view);
    name       = std::string(name_view);
    namespace_ = std::string(namespace_view);

    // Parse the array of fields
    auto fields_array = elem["fields"];
    for (auto field : fields_array) {
      std::string_view field_name_view;
      std::string_view field_type_json_view;
      field["name"].get(field_name_view);
      field["type"].get(field_type_json_view);
      fields.push_back({std::string(field_name_view), std::string(field_type_json_view)});
    }
  }
};

std::tuple<FeatureGroupAvroSchema, RS_Status>
GetFeatureGroupAvroSchema(const std::string &fgName, int fgVersion, int projectId);

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_OPERATIONS_FEATURE_STORE_HPP_