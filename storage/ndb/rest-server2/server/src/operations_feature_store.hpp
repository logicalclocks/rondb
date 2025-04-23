/*
 * Copyright (C) 2024, 2025 Hopsworks AB
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
#include "feature_store/feature_store.h"
#include "rdrs_dal.hpp"
#include <stdlib.h>

#include <simdjson.h>
#include <drogon/HttpTypes.h>
#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <ndb_types.h>
#include "mystring.hpp"

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

std::tuple<int, RS_Status> GetProjectID(const int featureStoreID);

std::tuple<int, RS_Status> GetFeatureStoreID(
  const std::string &featureStoreName);

std::tuple<int, RS_Status> GetFeatureViewID(int featureStoreID,
                                            const std::string &featureViewName,
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
        << "\nfeatureGroupId: " << featureGroupId << "\nfeatureName: "
        << featureName << "\nprefix: " << prefix << "\nrequired: "
        << required << "\njoinOn: " << joinOn
        << "\njoinIndex: " << joinIndex << "\nrequiredEntry: "
        << requiredEntry << "\n}";
    return oss.str();
  }
};

std::tuple<std::vector<ServingKey>, RS_Status>
GetServingKeys(int featureViewId);

struct AvroField {
  std::string name;  
  std::string avroSchema;  
};

struct FeatureGroupAvroSchema {
  std::string type;
  std::string name;
  std::string namespace_;
  std::vector<AvroField> fields;

  std::tuple<std::string, RS_Status>
    getSchemaByFeatureName(const std::string &featureName) const {
    for (const AvroField &field : fields) {
      if (field.name == featureName) {
        return {field.avroSchema, CRS_Status::SUCCESS.status};
      }
    }
    return {"", CRS_Status(static_cast<HTTP_CODE>(
      drogon::HttpStatusCode::k400BadRequest),
      std::string("Cannot find schema for feature ") + featureName).status};
  }

  // Parse from a simdjson document
  RS_Status from_json(const simdjson::dom::element &elem) {
    std::string_view type_view;
    std::string_view name_view;
    std::string_view namespace_str_view;

    // Parse each field from the JSON object
    simdjson::error_code error = elem["type"].get(type_view);
    if (error != simdjson::SUCCESS) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
        "Failed to parse type from JSON").status;
    }

    error = elem["name"].get(name_view);
    if (error != simdjson::SUCCESS) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
        "Failed to parse name from JSON").status;
    }

    error = elem["namespace"].get(namespace_str_view);
    if (error != simdjson::SUCCESS) {
      return CRS_Status(static_cast<HTTP_CODE>(
        drogon::HttpStatusCode::k400BadRequest),
        "Failed to parse namespace from JSON").status;
    }

    // Parse the array of fields
    simdjson::dom::array fields_array = elem["fields"];
    for (simdjson::dom::element field : fields_array) {
      std::string_view field_name_view;
      simdjson::dom::element type_dom_elem;

      // entire field
      std::string field_str = simdjson::to_string(field);
      
      // col name
      error = field["name"].get(field_name_view);
      if (error != simdjson::SUCCESS) {
        return CRS_Status(static_cast<HTTP_CODE>(
          drogon::HttpStatusCode::k400BadRequest),
          "Failed to parse field name from JSON").status;
      }
    
      std::string field_avro_schema =
        getAvroSchema(std::string(type_view),
                      std::string(name_view),
                      std::string(namespace_str_view),
                      field_str);
      fields.push_back(AvroField{std::string(field_name_view),
                       field_avro_schema});
    }
    return CRS_Status::SUCCESS.status;
  }

  std::string getAvroSchema(
    std::string type,
    std::string name,
    std::string schemaNamespaces, 
    std::string fields) {

    std::ostringstream json;
    json << "{";
    json << "\"type\": \"" << escape_string(type) << "\",";
    json << "\"name\": \"" << escape_string(name) << "\",";
    json << "\"namespace\": \"" << escape_string(schemaNamespaces) << "\",";
    json << "\"fields\": [" << fields << "]";
    json << "}";

    return json.str();
  }
};

std::tuple<FeatureGroupAvroSchema, RS_Status>
GetFeatureGroupAvroSchema(const std::string &fgName,
                          int fgVersion,
                          int projectId);

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_OPERATIONS_FEATURE_STORE_HPP_
