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

#include "metadata.hpp"
#include "operations_feature_store.hpp"
#include "rdrs_dal.hpp"
#include "fs_cache.hpp"

#include <avro/Exception.hh>
#include <tuple>
#include <util/require.h>
#include <EventLogger.hpp>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_MD_CACHE 1
#endif

#ifdef DEBUG_MD_CACHE
#define DEB_MD_CACHE(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_MD_CACHE(arglist) do { } while (0)
#endif

namespace metadata {

bool FeatureMetadata::isComplex() const {
  static const std::unordered_map<std::string, bool> complexFeature{
      {"MAP", true}, {"ARRAY", true}, {"STRUCT", true}, {"UNIONTYPE", true}};
  std::string baseType = type.substr(0, type.find('<'));
  std::transform(baseType.begin(), baseType.end(), baseType.begin(), ::toupper);
  return complexFeature.count(baseType) > 0 && complexFeature.at(baseType);
}

AvroDecoder::AvroDecoder() = default;

AvroDecoder::AvroDecoder(const std::string &schemaJson) {
  schema = avro::compileJsonSchemaFromString(schemaJson);
}

avro::GenericDatum
  AvroDecoder::decode(const std::vector<uint8_t> &inData) const {
  auto inStream = avro::memoryInputStream(inData.data(), inData.size());
  avro::DecoderPtr decoder = avro::binaryDecoder();
  decoder->init(*inStream);
  avro::GenericDatum datum(schema);
  try {
    avro::decode(*decoder, datum);
    return datum;
  } catch (const std::exception &e) {
    throw std::runtime_error(std::string("Decoding failed: ") + e.what());
  }
}

std::tuple<avro::GenericDatum, std::vector<uint8_t>, RS_Status>
AvroDecoder::NativeFromBinary(const std::vector<uint8_t> &buf) {
  try {
    // std::cout << "Schema: " << schema.toJson() << std::endl;
    // std::cout << "Buffer size: " << buf.size() << std::endl;
    auto inStream = avro::memoryInputStream(buf.data(), buf.size());
    avro::DecoderPtr decoder = avro::binaryDecoder();
    decoder->init(*inStream);

    avro::GenericDatum datum(schema);
    // std::cout << "Starting decode..." << std::endl;
    avro::decode(*decoder, datum);
    auto bytesRead = inStream->byteCount();
    std::vector<uint8_t> remainingBytes(buf.begin() + bytesRead, buf.end());
    return {datum, remainingBytes, CRS_Status::SUCCESS.status};
  } catch (const std::exception &e) {
    return {avro::GenericDatum(),
            buf,
            CRS_Status(
              static_cast<HTTP_CODE>(
                drogon::HttpStatusCode::k400BadRequest), e.what()).status};
  }
}

std::string getFeatureGroupServingKey(int joinIndex, int featureGroupId) {
  std::ostringstream ss;
  ss << joinIndex << "|" << featureGroupId;
  return ss.str();
}

std::string GetServingKey(int joinIndex, const std::string &featureName) {
  std::ostringstream ss;
  ss << joinIndex << "|" << featureName;
  return ss.str();
}

std::string getFeatureGroupIndexKey(int joinIndex, int fgId) {
  std::ostringstream ss;
  ss << joinIndex << "|" << fgId;
  return ss.str();
}

std::string GetFeatureGroupKeyByFeature(const FeatureMetadata &feature) {
  return getFeatureGroupIndexKey(feature.joinIndex, feature.featureGroupId);
}

std::string GetFeatureGroupKeyByTDFeature(const FeatureGroupFeatures &feature) {
  return getFeatureGroupIndexKey(feature.joinIndex, feature.featureGroupId);
}

std::string GetFeatureIndexKeyByFgIndexKey(const std::string &fgKey,
                                           const std::string &featureName) {
  std::ostringstream ss;
  ss << fgKey << "|" << featureName;
  return ss.str();
}

std::string getFeatureIndexKey(int joinIndex, int fgId, const std::string &f) {
  return GetFeatureIndexKeyByFgIndexKey(getFeatureGroupIndexKey(joinIndex, fgId), f);
}

std::string GetFeatureIndexKeyByFeature(const FeatureMetadata &feature) {
  return getFeatureIndexKey(feature.joinIndex, feature.featureGroupId, feature.name);
}

std::tuple<FeatureViewMetadata, RS_Status>
newFeatureViewMetadata(const std::string &featureStoreName,
                       int featureStoreId,
                       const std::string &featureViewName,
                       int featureViewId,
                       int featureViewVersion,
                       const std::vector<FeatureMetadata> &features,
                       const std::vector<ServingKey> &servingKeys) {
  std::unordered_map<std::string, bool> validPrimaryKeysMap;
  std::unordered_map<std::string, ServingKey> primaryKeyMap;
  std::unordered_map<std::string, std::vector<ServingKey>> fgPrimaryKeyMap;
  std::unordered_map<std::string, std::vector<std::string>> prefixJoinKeyMap;
  std::unordered_map<std::string, std::vector<std::string>> joinKeyMap;
  std::unordered_map<std::string, std::vector<std::string>> requiredJoinKeyMap;
  // key: serving key prefix + fname
  std::unordered_map<std::string, ServingKey> servingKeyMap;

  for (const auto &key : servingKeys) {
    servingKeyMap[key.prefix + key.featureName] = key;
  }
  for (const auto &key : servingKeys) {
    // std::cout << key.to_string() << std::endl;
    std::string prefixFeatureName = key.prefix + key.featureName;
    validPrimaryKeysMap[prefixFeatureName] = true;
    validPrimaryKeysMap[key.featureName] = true;

    prefixJoinKeyMap[prefixFeatureName].emplace_back(prefixFeatureName);
    joinKeyMap[key.featureName].emplace_back(prefixFeatureName);

    if (key.required) {
      requiredJoinKeyMap[prefixFeatureName].push_back(prefixFeatureName);
    } else {
      if (!key.requiredEntry.empty()) {
        requiredJoinKeyMap[key.joinOn].push_back(prefixFeatureName);
      }
    }
    primaryKeyMap[GetServingKey(key.joinIndex, key.featureName)] = key;
    std::string fgKey =
      getFeatureGroupServingKey(key.joinIndex, key.featureGroupId);
    fgPrimaryKeyMap[fgKey].push_back(key);
  }
  std::unordered_map<std::string, std::vector<FeatureMetadata>> prefixColumns;
  std::unordered_map<std::string, std::vector<FeatureMetadata>> fgFeatures;
  for (const auto &feature : features) {
    if (feature.label)
      continue;

    auto prefixFeatureName = feature.prefix + feature.name;
    prefixColumns[prefixFeatureName].emplace_back(feature);
    std::string featureKey =
      getFeatureGroupIndexKey(feature.joinIndex, feature.featureGroupId);
    fgFeatures[featureKey].push_back(feature);
  }

  auto fgFeaturesArray = std::vector<FeatureGroupFeatures>();
  for (auto &kvp : fgFeatures) {
    auto featureValue = kvp.second;
    auto feature      = featureValue.front();

    auto fgFeature = FeatureGroupFeatures();
    fgFeature.featureStoreName = feature.featureStoreName;
    fgFeature.featureStoreId = featureStoreId;
    fgFeature.featureGroupName = feature.featureGroupName;
    fgFeature.featureGroupVersion = feature.featureGroupVersion;
    fgFeature.featureGroupId = feature.featureGroupId;
    fgFeature.features = featureValue;
    // std::cout << "fgFeature: " << fgFeature.to_string() << std::endl;

    auto fgServingKey =
      getFeatureGroupServingKey(feature.joinIndex, feature.featureGroupId);
    // std::cout << "fgServingKey: " << fgServingKey << std::endl;
    auto it = fgPrimaryKeyMap.find(fgServingKey);
    // std::cout << "fgPrimaryKeyMap: " << std::endl;
    // for (const auto &key : fgPrimaryKeyMap) {
    //   std::cout << key.first << " : ";
    //   for (const auto &k : key.second) {
    //     std::cout << k.to_string() << " ";
    //   }
    //   std::cout << std::endl;
    // }
    if (it != fgPrimaryKeyMap.end()) {
      fgFeature.primaryKeyMap = it->second;
      // std::cout << "fgFeature.primaryKeyMap: " <<
      // fgFeature.primaryKeyMap.size() << std::endl;
    }
    fgFeature.joinIndex = feature.joinIndex;
    fgFeaturesArray.push_back(fgFeature);
  }
  // Sort features by index and populate feature index lookup
  auto sortedFeatures = features;
  std::sort(sortedFeatures.begin(),
            sortedFeatures.end(),
            [](const FeatureMetadata &a,
            const FeatureMetadata &b) { return a.index < b.index; });

  auto featureIndex = std::unordered_map<std::string, int>();
  int featureCount  = 0;
  for (const auto &feature : sortedFeatures) {
    if (feature.label)
      continue;

    std::string featureIndexKey   = GetFeatureIndexKeyByFeature(feature);
    featureIndex[featureIndexKey] = featureCount++;
  }
  auto complexFeatures = std::unordered_map<std::string, AvroDecoder>();
  auto fgSchemaCache = std::unordered_map<int, FeatureGroupAvroSchema>();
  int projectId = 0;
  FeatureGroupAvroSchema newFgSchema;
  std::string schema;
  AvroDecoder codec;
  RS_Status status;
  for (const auto &fgFeature : fgFeaturesArray) {
    for (const auto &feature : fgFeature.features) {
      if (feature.isComplex()) {
        auto it = fgSchemaCache.find(feature.featureGroupId);
        if (it == fgSchemaCache.end()) {
          std::tie(projectId, status) =
            GetProjectID(feature.featureStoreName);
          if (status.http_code !=
              static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
            return {FeatureViewMetadata(), status};
          }
          // TODO logger
          std::tie(newFgSchema, status) = GetFeatureGroupAvroSchema(
              fgFeature.featureGroupName,
              fgFeature.featureGroupVersion,
              projectId);
          if (status.http_code !=
              static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
            return {FeatureViewMetadata(), status};
          }
          fgSchemaCache[feature.featureGroupId] = newFgSchema;
        }
        std::tie(schema, status) =
          fgSchemaCache[feature.featureGroupId].getSchemaByFeatureName(
            feature.name);
        if (status.http_code !=
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
          return {FeatureViewMetadata(), status};
        }
        try {
          codec = AvroDecoder(schema);
        } catch (avro::Exception &e) {
          return {FeatureViewMetadata(),
                  CRS_Status(static_cast<HTTP_CODE>(
                    drogon::HttpStatusCode::k400BadRequest),
                    "Failed to parse feature schema.").status};
        }
        auto featureIndexKey = GetFeatureIndexKeyByFeature(feature);
        complexFeatures[featureIndexKey] = codec;
      }
    }
  }
  auto fsNames = std::vector<std::string>();
  auto fsNameMap = std::unordered_map<std::string, bool>();
  for (const auto &fgf : fgFeaturesArray) {
    auto fgName = fgf.featureStoreName;
    if (!fsNameMap[fgName]) {
      const auto &fsName = fgName;
      fsNames.push_back(fsName);
      fsNameMap[fgName] = true;
    }
  }
  if (!fsNameMap[featureStoreName]) {
    const auto &fsName = featureStoreName;
    fsNames.push_back(fsName);
    fsNameMap[featureStoreName] = true;
  }
  auto numOfFeature = featureIndex.size();
  auto metadata = FeatureViewMetadata();

  metadata.featureStoreName = featureStoreName;
  metadata.featureStoreId = featureStoreId;
  metadata.featureViewName = featureViewName;
  metadata.featureViewId = featureViewId;
  metadata.featureViewVersion = featureViewVersion;
  metadata.prefixFeaturesLookup = prefixColumns;
  metadata.featureGroupFeatures = fgFeaturesArray;
  metadata.numOfFeatures = numOfFeature;
  metadata.featureIndexLookup = featureIndex;
  metadata.featureStoreNames = fsNames;
  metadata.primaryKeyMap = primaryKeyMap;
  metadata.validPrimayKeys = validPrimaryKeysMap;
  metadata.prefixJoinKeyMap = prefixJoinKeyMap;
  metadata.requiredJoinKeyMap = requiredJoinKeyMap;
  metadata.joinKeyMap = joinKeyMap;
  metadata.complexFeatures = complexFeatures;

  return {metadata, CRS_Status::SUCCESS.status};
}

std::string getFeatureViewCacheKey(const std::string &featureStoreName,
                                   const std::string &featureViewName,
                                   int featureViewVersion) {
  std::ostringstream ss;
  ss << featureStoreName << "|"
     << featureViewName << "|" << featureViewVersion;
  return ss.str();
}

std::tuple<std::shared_ptr<FeatureViewMetadata>, std::shared_ptr<RestErrorCode>>
  GetFeatureViewMetadata(const std::string &featureStoreName,
                         const std::string &featureViewName,
                         int featureViewVersion) {
  RS_Status status;
  int fsID = 0;
  std::tie(fsID, status) = GetFeatureStoreID(featureStoreName);
  if (status.http_code !=
      static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    if (std::string(status.message).find(ERROR_NOT_FOUND) !=
          std::string::npos) {
      return {nullptr, std::make_shared<RestErrorCode>(FS_NOT_EXIST)};
    }
    return
      {nullptr,
      std::make_shared<RestErrorCode>(
        FS_READ_FAIL->NewMessage(status.message))};
  }

  int fvID = 0;
  std::tie(fvID, status) =
    GetFeatureViewID(fsID, featureViewName, featureViewVersion);
  if (status.http_code !=
      static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    if (std::string(status.message).find(ERROR_NOT_FOUND) !=
          std::string::npos) {
      return {nullptr, std::make_shared<RestErrorCode>(FV_NOT_EXIST)};
    }
    return
      {nullptr, std::make_shared<RestErrorCode>(
                  FV_READ_FAIL->NewMessage(status.message))};
  }
  std::unordered_map<int, TrainingDatasetJoin> joinIdToJoin;
  std::vector<TrainingDatasetJoin> tdJoins;
  std::tie(tdJoins, status) = GetTrainingDatasetJoinData(fvID);
  if (status.http_code !=
        static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    if (std::string(status.message).find(ERROR_NOT_FOUND) !=
          std::string::npos) {
      return {nullptr,
              std::make_shared<RestErrorCode>(FG_NOT_EXIST->NewMessage(
                "Feature view may contain deleted feature groups."))};
    }
    return {nullptr,
            std::make_shared<RestErrorCode>(
              TD_JOIN_READ_FAIL->NewMessage(status.message))};
  }
  for (const auto &tdj : tdJoins) {
    joinIdToJoin[tdj.id] = tdj;
  }
  std::vector<TrainingDatasetFeature> tdfs;
  std::tie(tdfs, status) = GetTrainingDatasetFeature(fvID);
  if (status.http_code !=
        static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    if (std::string(status.message).find(ERROR_NOT_FOUND) !=
          std::string::npos) {
      return {nullptr,
              std::make_shared<RestErrorCode>(
                FG_NOT_EXIST->NewMessage(
                  "Feature view may contain deleted feature groups."))};
    }
    return {nullptr,
            std::make_shared<RestErrorCode>(
              TD_FEATURE_READ_FAIL->NewMessage(status.message))};
  }
  std::vector<FeatureMetadata> features(tdfs.size());
  std::unordered_map<int, std::string> fsIdToName;
  std::unordered_map<int, FeatureGroup> fgCache;
  for (size_t i = 0; i < tdfs.size(); ++i) {
    const auto &tdf = tdfs[i];
    if (tdf.featureGroupID == 0) {
      return {nullptr,
              std::make_shared<RestErrorCode>(FG_NOT_EXIST->NewMessage(
                "Cannot get the feature group of feature `" + tdf.name +
                           "`. Check if the feature group still exists."))};
    }
    FeatureGroup featureGroup;
    if (fgCache.find(tdf.featureGroupID) != fgCache.end()) {
      featureGroup = fgCache[tdf.featureGroupID];
    } else {
      std::tie(featureGroup, status) = GetFeatureGroupData(tdf.featureGroupID);
      if (status.http_code !=
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
        if (std::string(status.message).find(ERROR_NOT_FOUND)!=
              std::string::npos) {
          return {nullptr,
                  std::make_shared<RestErrorCode>(FG_NOT_EXIST)};
        }
        return {nullptr,
                std::make_shared<RestErrorCode>(
                  FG_READ_FAIL->NewMessage(status.message))};
      }
    }
    auto feature = FeatureMetadata();
    if (fsIdToName.find(featureGroup.featureStoreId) != fsIdToName.end()) {
      feature.featureStoreName = fsIdToName[featureGroup.featureStoreId];
    } else {
      std::tie(feature.featureStoreName, status) =
        GetFeatureStoreName(featureGroup.featureStoreId);
      fsIdToName[featureGroup.featureStoreId] = feature.featureStoreName;
      if (status.http_code !=
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
        if (std::string(status.message).find(ERROR_NOT_FOUND) !=
              std::string::npos) {
          return {nullptr, std::make_shared<RestErrorCode>(FS_NOT_EXIST)};
        }
        return {nullptr,
                std::make_shared<RestErrorCode>(
                  FS_READ_FAIL->NewMessage(status.message))};
      }
    }
    feature.featureGroupName = featureGroup.name;
    feature.featureGroupVersion = featureGroup.version;
    feature.featureGroupId = tdf.featureGroupID;
    feature.id = tdf.featureID;
    feature.name = tdf.name;
    feature.type = tdf.type;
    feature.index = tdf.idx;
    feature.label = tdf.label == 1;
    feature.prefix = joinIdToJoin[tdf.tdJoinID].prefix;
    feature.joinIndex = joinIdToJoin[tdf.tdJoinID].index;
    features[i] = feature;
  }
  std::vector<ServingKey> servingKeys;
  std::tie(servingKeys, status) = GetServingKeys(fvID);
  if (status.http_code !=
        static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    return {nullptr,
            std::make_shared<RestErrorCode>(
              FV_READ_FAIL->NewMessage("Failed to read serving keys."))};
  }
  FeatureViewMetadata featureViewMetadata;
  std::tie(featureViewMetadata, status) = newFeatureViewMetadata(
      featureStoreName,
      fsID,
      featureViewName,
      fvID,
      featureViewVersion,
      features,
      servingKeys);
  if (status.http_code !=
        static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    return {nullptr,
            std::make_shared<RestErrorCode>(
              FV_READ_FAIL->NewMessage(status.message))};
  }
  return {std::make_shared<FeatureViewMetadata>(featureViewMetadata), nullptr};
}

std::tuple<std::shared_ptr<FeatureViewMetadata>, std::shared_ptr<RestErrorCode>>
  FeatureViewMetadataCache_Get(const std::string &featureStoreName,
                               const std::string &featureViewName,
                               int featureViewVersion) {
  std::string fvCacheKey =
      getFeatureViewCacheKey(featureStoreName,
                             featureViewName,
                             featureViewVersion);
  FSCacheEntry *entry = nullptr;
  auto cache_metadata = fs_metadata_cache_get(fvCacheKey, &entry);
  if (entry == nullptr) {
    require(cache_metadata == nullptr);
    DEB_MD_CACHE(("Cached Key failed with FETCH_METADATA_FROM_CACHE_FAIL"));
    return {nullptr, FETCH_METADATA_FROM_CACHE_FAIL};
  } else if (entry->m_errorCode != nullptr) {
    DEB_MD_CACHE(("Cached Key %s failed with error: %s",
                  entry->m_key.c_str(),
                  entry->m_errorCode->ToString().c_str()));
    return {nullptr, entry->m_errorCode};
  }
  if (!cache_metadata) {
    std::shared_ptr<FeatureViewMetadata> metadata;
    std::shared_ptr<RestErrorCode> errorCode;
    std::tie(metadata, errorCode) =
        GetFeatureViewMetadata(featureStoreName,
                               featureViewName,
                               featureViewVersion);
    if (errorCode) {
      DEB_MD_CACHE(("Key %s failed with error: %s",
                    entry->m_key.c_str(),
                    errorCode->ToString().c_str()));
      fs_metadata_update_cache(nullptr, entry, errorCode);
      return {nullptr, errorCode};
    }
    fs_metadata_update_cache(metadata, entry, nullptr);
    return {metadata, nullptr};
  }
  return {cache_metadata, nullptr};
}
} // namespace metadata
