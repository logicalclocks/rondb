#include "feature_store_data_structs.hpp"
#include "rdrs_dal.hpp"
#include "json_parser.hpp"

#include <simdjson.h>
#include <drogon/HttpTypes.h>

namespace feature_store_data_structs {
std::string FeatureStoreResponse::to_string() const {
  std::string res = "{";

  // Features
  res += "\"features\": [";
  for (const auto &feature : features) {
    if (feature.empty())
      res += "null";
    else
      res += std::string(feature.begin(), feature.end());
    res += ",";
  }
  if (!features.empty()) {
    res.pop_back();
  }
  res += "],";

  // Metadata
  res += "\"metadata\": [";
  for (const auto &metadata : metadata) {
    res += "{";
    if (metadata.name.empty()) {
      res += "\"featureName\": null,";
    } else {
      res += "\"featureName\": \"" + metadata.name + "\",";
    }
    if (metadata.type.empty()) {
      res += "\"featureType\": null";
    } else {
      res += "\"featureType\": \"" + metadata.type + "\"";
    }
    res += "},";
  }
  if (!metadata.empty()) {
    res.pop_back();
  }
  res += "],";

  // Status
  res += "\"status\": \"" + toString(status) + "\",";

  // DetailedStatus
  res += "\"detailedStatus\": [";
  if (detailedStatus.empty()) {
    res.pop_back();
    res += "null";
  }
  for (const auto &detailed : detailedStatus) {
    res += "{";
    res += "\"featureGroupId\": " + std::to_string(detailed.featureGroupId) + ",";
    res += "\"httpStatus\": " + std::to_string(detailed.httpStatus);
    res += "},";
  }
  if (!detailedStatus.empty()) {
    res.pop_back();
    res += "]";
  }

  res += "}";
  return res;
}

RS_Status FeatureStoreResponse::parseFeatureStoreResponse(const std::string &respBody,
                                                          FeatureStoreResponse &fsResp) {
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  auto error = parser.parse(respBody).get(doc);
  if (error != 0U) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      error_message(error))
        .status;
  }

  // Parse features
  auto features_array = doc["features"];
  if (features_array.error() == simdjson::error_code::NO_SUCH_FIELD) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "Missing 'features' field in response")
        .status;
  }
  if (features_array.error() != simdjson::SUCCESS) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      error_message(features_array.error()))
        .status;
  }
  if (!features_array.is_null()) {
    for (simdjson::dom::element feature : features_array) {
      std::ostringstream oss;
      oss << feature;
      std::string valueJson = oss.str();
      fsResp.features.push_back(std::vector<char>(valueJson.begin(), valueJson.end()));
    }
  }

  // Parse metadata
  auto metadata_array = doc["metadata"];
  if (metadata_array.error() == simdjson::error_code::NO_SUCH_FIELD) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "Missing 'metadata' field in response")
        .status;
  }
  if (metadata_array.error() != simdjson::SUCCESS) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      error_message(metadata_array.error()))
        .status;
  }

  if (!metadata_array.is_null()) {
    for (auto metadata : metadata_array) {
      FeatureMetadata fm;
      std::string_view fmName;
      std::string_view fmType;
      error = metadata["featureName"].get(fmName);
      if (error != 0U) {
        return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                          error_message(error))
            .status;
      }
      fm.name = std::string(fmName);
      error   = metadata["featureType"].get(fmType);
      if (error != 0U) {
        return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                          error_message(error))
            .status;
      }
      fm.type = std::string(fmType);

      fsResp.metadata.push_back(fm);
    }
  }

  // Parse status
  std::string_view status_view;
  auto status_ = doc["status"];
  if (status_.error() == simdjson::error_code::NO_SUCH_FIELD) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "Missing 'status' field in response")
        .status;
  }
  if (status_.error() != simdjson::SUCCESS) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      error_message(status_.error()))
        .status;
  }

  error         = status_.get(status_view);
  fsResp.status = fromString(std::string(status_view));

  // Parse detailedStatus
  uint64_t featureGroupId    = 0;
  uint64_t httpStatus        = 0;
  auto detailed_status_array = doc["detailedStatus"];
  if (detailed_status_array.error() == simdjson::error_code::NO_SUCH_FIELD ||
      detailed_status_array.is_null()) {
    fsResp.detailedStatus.clear();
  } else {
    for (simdjson::dom::element detailed : detailed_status_array) {
      DetailedStatus ds{};
      error = detailed["featureGroupId"].get(featureGroupId);
      if (error != 0U) {
        return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                          error_message(error))
            .status;
      }
      ds.featureGroupId = featureGroupId;
      error             = detailed["httpStatus"].get(httpStatus);
      if (error != 0U) {
        return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                          error_message(error))
            .status;
      }
      ds.httpStatus = httpStatus;
      fsResp.detailedStatus.push_back(ds);
    }
  }

  return CRS_Status::SUCCESS.status;
}

RS_Status
BatchFeatureStoreResponse::parseBatchFeatureStoreResponse(const std::string &respBody,
                                                          BatchFeatureStoreResponse &fsResp) {
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  auto error = parser.parse(respBody).get(doc);
  if (error != 0U) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      error_message(error))
        .status;
  }

  // Parse features
  auto features_array = doc["features"];
  if (features_array.error() == simdjson::error_code::NO_SUCH_FIELD) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "Missing 'features' field in response")
        .status;
  }
  if (features_array.error() != simdjson::SUCCESS) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      error_message(features_array.error()))
        .status;
  }
  if (!features_array.is_null()) {
    for (simdjson::dom::element feature : features_array) {
      std::vector<std::vector<char>> feature_vector;
      for (simdjson::dom::element feature_inner : feature) {
        std::vector<char> feature_inner_vector;
        std::ostringstream oss;
        oss << feature_inner;
        std::string valueJson = oss.str();
        feature_inner_vector.assign(valueJson.begin(), valueJson.end());
        feature_vector.push_back(feature_inner_vector);
      }
      fsResp.features.push_back(feature_vector);
    }
  }

  // Parse metadata
  auto metadata_array = doc["metadata"];
  if (metadata_array.error() == simdjson::error_code::NO_SUCH_FIELD) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "Missing 'metadata' field in response")
        .status;
  }
  if (metadata_array.error() != simdjson::SUCCESS) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      error_message(metadata_array.error()))
        .status;
  }

  if (!metadata_array.is_null()) {
    for (auto metadata : metadata_array) {
      FeatureMetadata fm;
      std::string_view fmName;
      std::string_view fmType;
      error = metadata["featureName"].get(fmName);
      if (error != 0U) {
        return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                          error_message(error))
            .status;
      }
      fm.name = std::string(fmName);
      error   = metadata["featureType"].get(fmType);
      if (error != 0U) {
        return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                          error_message(error))
            .status;
      }
      fm.type = std::string(fmType);

      fsResp.metadata.push_back(fm);
    }
  }

  // Parse status
  auto status_array = doc["status"];
  if (status_array.error() == simdjson::error_code::NO_SUCH_FIELD) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "Missing 'status' field in response")
        .status;
  }
  if (status_array.error() != simdjson::SUCCESS) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      error_message(status_array.error()))
        .status;
  }

  if (!status_array.is_null()) {
    for (simdjson::dom::element status : status_array) {
      std::string_view status_view;
      error = status.get(status_view);
      fsResp.status.push_back(fromString(std::string(status_view)));
    }
  }

  // Parse detailedStatus
  uint64_t featureGroupId    = 0;
  uint64_t httpStatus        = 0;
  auto detailed_status_array = doc["detailedStatus"];
  if (detailed_status_array.error() == simdjson::error_code::NO_SUCH_FIELD ||
      detailed_status_array.is_null()) {
    // No detailedStatus field or it is null, leave fsResp.detailedStatus empty
    fsResp.detailedStatus.clear();
  } else {
    for (simdjson::dom::element detailed_group : detailed_status_array) {
      std::vector<DetailedStatus> detailed_status_vector;
      for (simdjson::dom::element detailed : detailed_group) {
        DetailedStatus ds{};
        error = detailed["featureGroupId"].get(featureGroupId);
        if (error != 0U) {
          return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                            error_message(error))
              .status;
        }
        ds.featureGroupId = featureGroupId;
        error             = detailed["httpStatus"].get(httpStatus);
        if (error != 0U) {
          return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                            error_message(error))
              .status;
        }
        ds.httpStatus = httpStatus;
        detailed_status_vector.push_back(ds);
      }
      fsResp.detailedStatus.push_back(detailed_status_vector);
    }
  }

  return CRS_Status::SUCCESS.status;
}

std::string toString(FeatureStatus status) {
  auto it = FeatureStatusToString.find(status);
  if (it != FeatureStatusToString.end()) {
    return it->second;
  }
  return "UNKNOWN";
}

FeatureStatus fromString(const std::string &status) {
  auto it = StringToFeatureStatus.find(status);
  if (it != StringToFeatureStatus.end()) {
    return it->second;
  }
  return FeatureStatus::Error;
}

std::string BatchFeatureStoreResponse::to_string() const {
  std::string res = "{";

  // Serialize features
  res += "\"features\": [";
  for (const auto &feature : features) {
    res += "[";
    for (const auto &entry : feature) {
      if (entry.empty())
        res += "null";
      else
        res += std::string(entry.begin(), entry.end());
      res += ",";
    }
    if (!feature.empty()) {
      res.pop_back();
    }
    res += "],";
  }
  if (!features.empty()) {
    res.pop_back();
  }
  res += "],";

  // Serialize metadata
  res += "\"metadata\": [";
  for (const auto &metadata : metadata) {
    res += "{";
    if (metadata.name.empty()) {
      res += "\"featureName\": null,";
    } else {
      res += "\"featureName\": \"" + metadata.name + "\",";
    }
    if (metadata.type.empty()) {
      res += "\"featureType\": null";
    } else {
      res += "\"featureType\": \"" + metadata.type + "\"";
    }
    res += "},";
  }
  if (!metadata.empty()) {
    res.pop_back();
  }
  res += "],";

  // Serialize status
  res += "\"status\": [";
  for (const auto &status : status) {
    res += "\"" + toString(status) + "\",";
  }
  if (!status.empty()) {
    res.pop_back();
  }
  res += "],";

  // Serialize detailedStatus
  res += "\"detailedStatus\": [";
  for (const auto &detailed_status_group : detailedStatus) {
    res += "[";
    if (detailed_status_group.empty()) {
      res.pop_back();
      res += "null";
    }

    for (const auto &detailed : detailed_status_group) {
      res += "{";
      res += "\"featureGroupId\": " + std::to_string(detailed.featureGroupId) + ",";
      res += "\"httpStatus\": " + std::to_string(detailed.httpStatus);
      res += "},";
    }
    if (!detailed_status_group.empty()) {
      res.pop_back();
      res += "],";
    } else {
      res += ",";
    }
  }
  if (!detailedStatus.empty()) {
    res.pop_back();
  }
  res += "]";

  res += "}";
  return res;
}

Options GetDefaultOptions() {
  auto defaultOptions                   = Options();
  defaultOptions.validatePassedFeatures = true;
  defaultOptions.includeDetailedStatus  = false;
  return defaultOptions;
}

Options FeatureStoreRequest::GetOptions() const {
  auto defaultOptions = GetDefaultOptions();

  if (optionsRequest.validatePassedFeatures.has_value()) {
    defaultOptions.validatePassedFeatures = optionsRequest.validatePassedFeatures.value();
  }

  if (optionsRequest.includeDetailedStatus.has_value()) {
    defaultOptions.includeDetailedStatus = optionsRequest.includeDetailedStatus.value();
  }

  return defaultOptions;
}

Options BatchFeatureStoreRequest::GetOptions() const {
  auto defaultOptions = GetDefaultOptions();

  if (optionsRequest.validatePassedFeatures.has_value()) {
    defaultOptions.validatePassedFeatures = optionsRequest.validatePassedFeatures.value();
  }

  if (optionsRequest.includeDetailedStatus.has_value()) {
    defaultOptions.includeDetailedStatus = optionsRequest.includeDetailedStatus.value();
  }

  return defaultOptions;
}

}  // namespace feature_store_data_structs
