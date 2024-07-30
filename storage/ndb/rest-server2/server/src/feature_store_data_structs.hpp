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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_DATA_STRUCTS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_DATA_STRUCTS_HPP_

#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include "rdrs_dal.h"

namespace feature_store_data_structs {

class MetadataRequest {
 public:
  bool featureName;  // json:"featureName" binding:"required"
  bool featureType;  // json:"featureType" binding:"required"
  std::string to_string() const {
    std::ostringstream oss;
    oss << "MetadataRequest {"
        << "\n  featureName: " << (featureName ? "true" : "false")
        << "\n  featureType: " << (featureType ? "true" : "false") << "\n}";
    return oss.str();
  }
};

class FeatureStoreRequest {
 public:
  std::string featureStoreName;  // json:"featureStoreName" binding:"required"
  std::string featureViewName;   // json:"featureViewName" binding:"required"
  int featureViewVersion;        // json:"featureViewVersion" binding:"required"
  std::unordered_map<std::string, std::vector<char>> passedFeatures;  // json:"passedFeatures"
  std::unordered_map<std::string, std::vector<char>> entries;  // json:"entries" binding:"required"
  MetadataRequest metadataRequest;                             // json:"metadataOptions"
  RS_Status validate();
  static RS_Status
  validate_primary_key(const std::unordered_map<std::string, std::vector<char>> &entries,
                       const std::unordered_map<std::string, std::vector<char>> &features);
  std::string to_string() const {
    std::ostringstream oss;
    oss << "FeatureStoreRequest {"
        << "\n  featureStoreName: " << featureStoreName
        << "\n  featureViewName: " << featureViewName
        << "\n  featureViewVersion: " << featureViewVersion << "\n  passedFeatures: {";
    for (const auto &[key, value] : passedFeatures) {
      oss << "\n    " << key << ": [";
      for (char c : value) {
        oss << c;
      }
      oss << "]";
    }
    oss << "\n  }"
        << "\n  entries: {";
    for (const auto &[key, value] : entries) {
      oss << "\n    " << key << ": [";
      for (char c : value) {
        oss << c;
      }
      oss << "]";
    }
    oss << "\n  }"
        << "\n  metadataRequest: " << metadataRequest.to_string() << "\n}";
    return oss.str();
  }
};

class BatchFeatureStoreRequest {
 public:
  std::string featureStoreName;  // json:"featureStoreName" binding:"required"
  std::string featureViewName;   // json:"featureViewName" binding:"required"
  int featureViewVersion;        // json:"featureViewVersion" binding:"required"
  std::vector<std::unordered_map<std::string, std::vector<char>>>
      passedFeatures;  // json:"passedFeatures"
  std::vector<std::unordered_map<std::string, std::vector<char>>>
      entities;                     // json:"entities" binding:"required"
  MetadataRequest metadataRequest;  // json:"metadataOptions"
};

class FeatureMetadata {
 public:
  std::string name;  // json:"featureName"
  std::string type;  // json:"featureType"
  bool operator==(const FeatureMetadata &rhs) const {
    return name == rhs.name && type == rhs.type;
  }
  bool operator!=(const FeatureMetadata &rhs) const {
    return !(rhs == *this);
  }
  std::string toString() const {
    std::ostringstream oss;
    oss << "FeatureMetadata {"
        << "\n  name: " << name << "\n  type: " << type << "\n}";
    return oss.str();
  }
};

enum class FeatureStatus {
  Complete,  // "COMPLETE"
  Missing,   // "MISSING"
  Error      // "ERROR"
};

const std::unordered_map<FeatureStatus, std::string> FeatureStatusToString{
    {FeatureStatus::Complete, "COMPLETE"},
    {FeatureStatus::Missing, "MISSING"},
    {FeatureStatus::Error, "ERROR"}};

const std::unordered_map<std::string, FeatureStatus> StringToFeatureStatus{
    {"COMPLETE", FeatureStatus::Complete},
    {"MISSING", FeatureStatus::Missing},
    {"ERROR", FeatureStatus::Error}};

std::string toString(FeatureStatus status);

FeatureStatus fromString(const std::string &status);

class FeatureStoreResponse {
 public:
  std::vector<std::vector<char>> features;  // json:"features"
  std::vector<FeatureMetadata> metadata;    // json:"metadata"
  FeatureStatus status;                     // json:"status"
  std::string to_string() const;
  static RS_Status parseFeatureStoreResponse(const std::string &respBody,
                                             FeatureStoreResponse &fsResp);
};

class BatchFeatureStoreResponse {
 public:
  std::vector<std::vector<std::string>> features;  // json:"features"
  std::vector<FeatureMetadata> metadata;           // json:"metadata"
  std::vector<FeatureStatus> status;               // json:"status"
};

}  // namespace feature_store_data_structs

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_DATA_STRUCTS_HPP_