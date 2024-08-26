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
#include <sys/_types/_int32_t.h>
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

class OptionsRequest {
 public:
  bool validatePassedFeatures;  // json:"validatePassedFeatures"
  bool includeDetailedStatus;   // json:"includeDetailedStatus"
  std::string to_string() const {
    std::ostringstream oss;
    oss << "OptionsRequest {"
        << "\n  validatePassedFeatures: " << (validatePassedFeatures ? "true" : "false")
        << "\n  includeDetailedStatus: " << (includeDetailedStatus ? "true" : "false") << "\n}";
    return oss.str();
  }
};

class Options {
 public:
  bool validatePassedFeatures;  // json:"validatePassedFeatures"
  bool includeDetailedStatus;   // json:"includeDetailedStatus"
};

// Request of multiple feature vectors and optional metadata
class BatchFeatureStoreRequest {
 public:
  std::string featureStoreName;  // json:"featureStoreName" binding:"required"
  std::string featureViewName;   // json:"featureViewName" binding:"required"
  int featureViewVersion;        // json:"featureViewVersion" binding:"required"
  // Client provided feature map for overwriting feature value
  std::vector<std::unordered_map<std::string, std::vector<char>>>
      passedFeatures;  // json:"passedFeatures"
  // Serving key of feature view
  std::vector<std::unordered_map<std::string, std::vector<char>>>
      entries;  // json:"entries" binding:"required"
  // Client requested metadata
  MetadataRequest metadataRequest;  // json:"metadataOptions"
  OptionsRequest optionsRequest;    // json:"options"
  std::string to_string() const {
    std::ostringstream oss;
    oss << "BatchFeatureStoreRequest {"
        << "\n  featureStoreName: " << featureStoreName
        << "\n  featureViewName: " << featureViewName
        << "\n  featureViewVersion: " << featureViewVersion << "\n  passedFeatures: {";
    for (const auto &passedFeature : passedFeatures) {
      oss << "\n    {";
      for (const auto &[key, value] : passedFeature) {
        oss << "\n      " << key << ": [";
        for (char c : value) {
          oss << c;
        }
        oss << "]";
      }
      oss << "\n    }";
    }
    oss << "\n  }"
        << "\n  entries: {";
    for (const auto &entry : entries) {
      oss << "\n    {";
      for (const auto &[key, value] : entry) {
        oss << "\n      " << key << ": [";
        for (char c : value) {
          oss << c;
        }
        oss << "]";
      }
      oss << "\n    }";
    }
    oss << "\n  }"
        << "\n  metadataRequest: " << metadataRequest.to_string();
    oss << "\n  }";
    oss << "\n  optionsRequest: " << optionsRequest.to_string() << "\n}";
    return oss.str();
  }
  Options GetOptions() const;
};

// Request of a signle feature vector and optional metadata
class FeatureStoreRequest {
 public:
  std::string featureStoreName;  // json:"featureStoreName" binding:"required"
  std::string featureViewName;   // json:"featureViewName" binding:"required"
  int featureViewVersion;        // json:"featureViewVersion" binding:"required"
  std::unordered_map<std::string, std::vector<char>> passedFeatures;  // json:"passedFeatures"
  std::unordered_map<std::string, std::vector<char>> entries;  // json:"entries" binding:"required"
  MetadataRequest metadataRequest;                             // json:"metadataOptions"
  OptionsRequest optionsRequest;                               // json:"options"
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
        << "\n  metadataRequest: " << metadataRequest.to_string();
    oss << "\n  }";
    oss << "\n  optionsRequest: " << optionsRequest.to_string() << "\n}";
    return oss.str();
  }
  Options GetOptions() const;
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

class DetailedStatus {
 public:
  int32_t httpStatus;  // json:"httpStatus"
  int featureGroupId;  // json:"featureGroupId"

  std::string to_string() const {
    std::ostringstream oss;
    oss << "{"
        << "\"httpStatus\": " << httpStatus << ", "
        << "\"featureGroupId\": " << featureGroupId << "}";
    return oss.str();
  }
};

class FeatureStoreResponse {
 public:
  std::vector<std::vector<char>> features;     // json:"features"
  std::vector<FeatureMetadata> metadata;       // json:"metadata"
  FeatureStatus status;                        // json:"status"
  std::vector<DetailedStatus> detailedStatus;  // json:"detailedStatus"
  std::string to_string() const;
  static RS_Status parseFeatureStoreResponse(const std::string &respBody,
                                             FeatureStoreResponse &fsResp);
};

class BatchFeatureStoreResponse {
 public:
  std::vector<std::vector<std::vector<char>>> features;     // json:"features"
  std::vector<FeatureMetadata> metadata;                    // json:"metadata"
  std::vector<FeatureStatus> status;                        // json:"status"
  std::vector<std::vector<DetailedStatus>> detailedStatus;  // json:"detailedStatus"
  std::string to_string() const;
  static RS_Status parseBatchFeatureStoreResponse(const std::string &respBody,
                                                  BatchFeatureStoreResponse &fsResp);
};

}  // namespace feature_store_data_structs

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_DATA_STRUCTS_HPP_