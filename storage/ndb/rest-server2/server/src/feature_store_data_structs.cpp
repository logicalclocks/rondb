#include "feature_store_data_structs.hpp"
#include "rdrs_dal.hpp"
#include "json_parser.hpp"

#include <simdjson.h>
#include <drogon/HttpTypes.h>

namespace feature_store_data_structs {
std::string FeatureStoreResponse::to_string() const {
  std::string res = "{";
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
  res += "\"status\": \"" + toString(status) + "\"";
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

  return CRS_Status().status;
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

  return CRS_Status().status;
}

std::string toString(FeatureStatus status) {
  auto it = FeatureStatusToString.find(status);
  if (it != FeatureStatusToString.end()) {
    return it->second;
  }
  // TODO
}

FeatureStatus fromString(const std::string &status) {
  auto it = StringToFeatureStatus.find(status);
  if (it != StringToFeatureStatus.end()) {
    return it->second;
  }
  // TODO
}

std::string BatchFeatureStoreResponse::to_string() const {
  std::string res = "{";
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
  res += "\"status\": [";
  for (const auto &status : status) {
    res += "\"" + toString(status) + "\",";
  }
  if (!status.empty()) {
    res.pop_back();
  }
  res += "]";
  res += "}";
  return res;
}

}  // namespace feature_store_data_structs