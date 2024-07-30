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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_METADATA_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_METADATA_HPP_

#include "operations_feature_store.hpp"
#include "constants.hpp"
#include "cache.hpp"
#include "feature_store_error_code.hpp"
#include "storage/ndb/rest-server2/extra/avro/avro-release-1.11.3/lang/c++/api/Compiler.hh"
#include "storage/ndb/rest-server2/extra/avro/avro-release-1.11.3/lang/c++/api/Decoder.hh"
#include "storage/ndb/rest-server2/extra/avro/avro-release-1.11.3/lang/c++/api/Encoder.hh"
#include "storage/ndb/rest-server2/extra/avro/avro-release-1.11.3/lang/c++/api/Generic.hh"
#include "storage/ndb/rest-server2/extra/avro/avro-release-1.11.3/lang/c++/api/GenericDatum.hh"
#include "storage/ndb/rest-server2/extra/avro/avro-release-1.11.3/lang/c++/api/Specific.hh"
#include "storage/ndb/rest-server2/extra/avro/avro-release-1.11.3/lang/c++/api/Stream.hh"
#include "storage/ndb/rest-server2/extra/avro/avro-release-1.11.3/lang/c++/api/ValidSchema.hh"

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <stdexcept>
#include <fstream>
#include <sstream>

namespace metadata {

struct FeatureMetadata {
  std::string featureStoreName;
  std::string featureGroupName;
  int featureGroupVersion;
  int featureGroupId;
  int id;
  std::string name;
  std::string type;
  int index;
  bool label;
  std::string prefix;
  int transformationFunctionId;
  int joinIndex;

  bool isComplex() const;
  std::string to_string() const {
    std::ostringstream oss;
    oss << "FeatureMetadata {"
        << "\n  featureStoreName: " << featureStoreName
        << "\n  featureGroupName: " << featureGroupName
        << "\n  featureGroupVersion: " << featureGroupVersion
        << "\n  featureGroupId: " << featureGroupId << "\n  id: " << id << "\n  name: " << name
        << "\n  type: " << type << "\n  index: " << index << "\n  label: " << label
        << "\n  prefix: " << prefix << "\n  transformationFunctionId: " << transformationFunctionId
        << "\n  joinIndex: " << joinIndex << "\n  isComplex: " << isComplex() << "\n}";
    return oss.str();
  }
};

struct FeatureGroupFeatures {
  std::string featureStoreName;
  int featureStoreId;
  std::string featureGroupName;
  int featureGroupVersion;
  int featureGroupId;
  int joinIndex;
  std::vector<FeatureMetadata> features;
  std::vector<ServingKey> primaryKeyMap;
  std::string to_string() const {
    std::ostringstream oss;
    oss << "FeatureGroupFeatures {"
        << "\n  featureStoreName: " << featureStoreName << "\n  featureStoreId: " << featureStoreId
        << "\n  featureGroupName: " << featureGroupName
        << "\n  featureGroupVersion: " << featureGroupVersion
        << "\n  featureGroupId: " << featureGroupId << "\n  joinIndex: " << joinIndex
        << "\n  features: [";
    for (const auto &feature : features) {
      oss << "\n    " << feature.to_string();
    }
    oss << "\n  ]"
        << "\n  primaryKeyMap: [";
    for (const auto &key : primaryKeyMap) {
      oss << "\n    " << key.to_string();
    }
    oss << "\n  ]"
        << "\n}";
    return oss.str();
  }
};

class AvroDecoder {
 private:
  avro::ValidSchema schema;

 public:
  AvroDecoder();

  explicit AvroDecoder(const std::string &schemaJson);

  avro::ValidSchema getSchema() const {
    return schema;
  }

  // Decode binary data into a GenericDatum
  avro::GenericDatum decode(const std::vector<uint8_t> &inData) const;

  std::tuple<avro::GenericDatum, std::vector<uint8_t>, RS_Status>
  NativeFromBinary(const std::vector<uint8_t> &buf);

  std::string to_string() const {
    std::ostringstream oss;
    oss << "AvroDecoder {"
        << "\n  schema: " << schema.toJson() << "\n}";
    return oss.str();
  }
};

struct FeatureViewMetadata {
  std::string featureStoreName;
  int featureStoreId;
  std::string featureViewName;
  int featureViewId;
  int featureViewVersion;
  std::unordered_map<std::string, FeatureMetadata>
      prefixFeaturesLookup;  // key: prefix + fName, label are excluded
  std::vector<FeatureGroupFeatures> featureGroupFeatures;  // label are excluded
  std::vector<std::string> featureStoreNames;  // List of all feature store used by feature view
                                               // including shared feature store
  int numOfFeatures;
  std::unordered_map<std::string, int>
      featureIndexLookup;  // key: joinIndex + fgId + fName, label are excluded. joinIndex is needed
                           // because of self-join
  // serving key doc:
  // https://hopsworks.atlassian.net/wiki/spaces/FST/pages/173342721/How+to+resolve+the+set+of+serving+key+in+get+feature+vector
  std::unordered_map<std::string, ServingKey>
      primaryKeyMap;  // key: join index + feature name. Used for constructing rondb request.
  std::unordered_map<std::string, std::string>
      prefixPrimaryKeyMap;  // key: serving-key-prefix + fName, value: feature name in feature
                            // group. Used for pk validation.
  std::unordered_map<std::string, std::vector<std::string>>
      joinKeyMap;  // key: serving-key-prefix + fName, value: list of feature which join on the key.
                   // Used for filling in pk value.
  std::unordered_map<std::string, AvroDecoder>
      complexFeatures;  // key: joinIndex + fgId + fName, label are excluded. joinIndex is needed
                        // because of self-join
  std::string to_string() const {
    std::ostringstream oss;
    oss << "FeatureViewMetadata {"
        << "\n  featureStoreName: " << featureStoreName << "\n  featureStoreId: " << featureStoreId
        << "\n  featureViewName: " << featureViewName << "\n  featureViewId: " << featureViewId
        << "\n  featureViewVersion: " << featureViewVersion << "\n  prefixFeaturesLookup: {";

    for (const auto &[key, value] : prefixFeaturesLookup) {
      oss << "\n    " << key << ": " << value.to_string();
    }

    oss << "\n  }"
        << "\n  featureGroupFeatures: [";

    for (const auto &fgf : featureGroupFeatures) {
      oss << "\n    " << fgf.to_string();
    }

    oss << "\n  ]"
        << "\n  featureStoreNames: [";

    for (const auto &name : featureStoreNames) {
      oss << "\n    " << name;
    }

    oss << "\n  ]"
        << "\n  numOfFeatures: " << numOfFeatures << "\n  featureIndexLookup: {";

    for (const auto &[key, value] : featureIndexLookup) {
      oss << "\n    " << key << ": " << value;
    }

    oss << "\n  }"
        << "\n  primaryKeyMap: {";

    for (const auto &[key, value] : primaryKeyMap) {
      oss << "\n    " << key << ": " << value.to_string();
    }

    oss << "\n  }"
        << "\n  prefixPrimaryKeyMap: {";

    for (const auto &[key, value] : prefixPrimaryKeyMap) {
      oss << "\n    " << key << ": " << value;
    }

    oss << "\n  }"
        << "\n  joinKeyMap: {";

    for (const auto &[key, value] : joinKeyMap) {
      oss << "\n    " << key << ": [";
      for (const auto &v : value) {
        oss << "\n      " << v;
      }
      oss << "\n    ]";
    }

    oss << "\n  }"
        << "\n  complexFeatures: {";

    for (const auto &[key, value] : complexFeatures) {
      oss << "\n    " << key << ": " << value.to_string();
    }

    oss << "\n  }"
        << "\n}";

    return oss.str();
  }
};

std::string getFeatureGroupServingKey(int joinIndex, int featureGroupId);
std::string GetServingKey(int joinIndex, const std::string &featureName);
std::string getFeatureGroupIndexKey(int joinIndex, int fgId);
std::string GetFeatureGroupKeyByFeature(const FeatureMetadata &feature);
std::string GetFeatureGroupKeyByTDFeature(const FeatureGroupFeatures &feature);
std::string GetFeatureIndexKeyByFgIndexKey(const std::string &fgKey,
                                           const std::string &featureName);
std::string getFeatureIndexKey(int joinIndex, int fgId, const std::string &f);
std::string GetFeatureIndexKeyByFeature(const FeatureMetadata &feature);

std::tuple<FeatureViewMetadata, RS_Status>
newFeatureViewMetadata(const std::string &featureStoreName, int featureStoreId,
                       const std::string &featureViewName, int featureViewId,
                       int featureViewVersion, const std::vector<FeatureMetadata> &features,
                       const std::vector<ServingKey> &servingKeys);

std::string getFeatureViewCacheKey(const std::string &featureStoreName,
                                   const std::string &featureViewName, int featureViewVersion);
std::tuple<std::shared_ptr<FeatureViewMetadata>, std::shared_ptr<RestErrorCode>>
GetFeatureViewMetadata(const std::string &featureStoreName, const std::string &featureViewName,
                       int featureViewVersion);

class FeatureViewMetaDataCache {
 public:
  Cache<std::shared_ptr<FeatureViewMetadata>> metadataCache;
  std::tuple<std::shared_ptr<FeatureViewMetadata>, std::shared_ptr<RestErrorCode>>
  Get(const std::string &featureStoreName, const std::string &featureViewName,
      int featureViewVersion);
};

}  // namespace metadata

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_METADATA_HPP_