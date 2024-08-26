#include "operations_feature_store.hpp"
#include "feature_store_error_code.hpp"
#include "rdrs_dal.hpp"
#include <memory>
#include <tuple>

std::tuple<int, RS_Status> GetProjectID(const std::string &featureStoreName) {
  int projectID = 0;

  auto ret = find_project_id(featureStoreName.c_str(), &projectID);

  if (ret.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    return {projectID, ret};
  }

  return {projectID, CRS_Status().status};
}

std::tuple<int, RS_Status> GetFeatureStoreID(const std::string &featureStoreName) {
  int projectID = 0;

  auto ret = find_feature_store_id(featureStoreName.c_str(), &projectID);

  if (ret.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    return {projectID, ret};
  }

  return {projectID, CRS_Status().status};
}

std::tuple<int, RS_Status> GetFeatureViewID(int featureStoreID, const std::string &featureViewName,
                                            int featureViewVersion) {
  int fsViewID = 0;

  auto ret =
      find_feature_view_id(featureStoreID, featureViewName.c_str(), featureViewVersion, &fsViewID);

  if (ret.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    return {fsViewID, ret};
  }

  return {fsViewID, CRS_Status().status};
}

std::tuple<std::vector<TrainingDatasetJoin>, RS_Status>
GetTrainingDatasetJoinData(int featureViewID) {
  int tdjsSize                = 0;
  Training_Dataset_Join *tdjs = nullptr;

  auto ret = find_training_dataset_join_data(featureViewID, &tdjs, &tdjsSize);

  if (ret.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    return {{}, ret};
  }

  std::vector<TrainingDatasetJoin> retTdjs(tdjsSize);
  for (int i = 0; i < tdjsSize; ++i) {
    retTdjs[i].id     = tdjs[i].id;
    retTdjs[i].prefix = tdjs[i].prefix;
    retTdjs[i].index  = tdjs[i].idx;
  }

  free(tdjs);
  return {retTdjs, CRS_Status().status};
}

std::tuple<FeatureGroup, RS_Status> GetFeatureGroupData(int featureGroupID) {
  Feature_Group fg;

  auto ret = find_feature_group_data(featureGroupID, &fg);

  if (ret.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    return {{}, ret};
  }

  return {{fg.name, fg.feature_store_id, fg.version, fg.online_enabled != 0}, CRS_Status().status};
}

std::tuple<std::vector<TrainingDatasetFeature>, RS_Status>
GetTrainingDatasetFeature(int featureViewID) {
  int tdfsSize                   = 0;
  Training_Dataset_Feature *tdfs = nullptr;

  auto ret = find_training_dataset_data(featureViewID, &tdfs, &tdfsSize);

  if (ret.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    return {{}, ret};
  }

  std::vector<TrainingDatasetFeature> retTdfs(tdfsSize);
  for (int i = 0; i < tdfsSize; ++i) {
    retTdfs[i] = {tdfs[i].feature_id,
                  tdfs[i].training_dataset,
                  tdfs[i].feature_group_id,
                  tdfs[i].name,
                  tdfs[i].data_type,
                  tdfs[i].td_join_id,
                  tdfs[i].idx,
                  tdfs[i].label,
                  tdfs[i].feature_view_id};
  }

  free(tdfs);

  std::sort(retTdfs.begin(), retTdfs.end(),
            [](const TrainingDatasetFeature &a, const TrainingDatasetFeature &b) {
              return a.idx < b.idx;
            });

  return {retTdfs, CRS_Status().status};
}

std::tuple<std::string, RS_Status> GetFeatureStoreName(int fsId) {
  char nameBuff[FEATURE_STORE_NAME_SIZE];

  auto ret = find_feature_store_data(fsId, nameBuff);

  if (ret.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    // TODO
  }

  return {std::string(nameBuff), CRS_Status().status};
}

std::tuple<std::vector<ServingKey>, RS_Status> GetServingKeys(int featureViewId) {
  int servingKeySize       = 0;
  Serving_Key *servingKeys = nullptr;

  auto ret = find_serving_key_data(featureViewId, &servingKeys, &servingKeySize);

  if (ret.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    return {{}, ret};
  }

  std::vector<ServingKey> retServingKeys(servingKeySize);
  for (int i = 0; i < servingKeySize; ++i) {
    ServingKey key{servingKeys[i].feature_group_id,
                   servingKeys[i].feature_name,
                   servingKeys[i].prefix,
                   servingKeys[i].required != 0,
                   servingKeys[i].join_on,
                   servingKeys[i].join_index,
                   std::string(servingKeys[i].prefix) + std::string(servingKeys[i].feature_name)};
    if (!key.required) {
      key.requiredEntry = key.joinOn;
    }
    retServingKeys[i] = key;
  }

  free(servingKeys);
  return {retServingKeys, CRS_Status().status};
}

std::tuple<FeatureGroupAvroSchema, RS_Status>
GetFeatureGroupAvroSchema(const std::string &fgName, int fgVersion, int projectId) {
  std::string subjectName = fgName + "_" + std::to_string(fgVersion);
  // TODO logger

  char *schemaBuff = static_cast<char *>(malloc(FEATURE_GROUP_SCHEMA_SIZE));

  auto ret = find_feature_group_schema(subjectName.c_str(), projectId, schemaBuff);

  if (ret.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    free(schemaBuff);
    return {{}, ret};
  }

  std::string schemaStr(schemaBuff);
  free(schemaBuff);

  simdjson::dom::parser parser;
  auto result = parser.parse(schemaStr);

  if (result.error()) {
    return {{}, CRS_Status(HTTP_CODE::SERVER_ERROR, "Failed to parse schema").status};
  }

  FeatureGroupAvroSchema avroSchema;
  avroSchema.from_json(result.value());

  return {avroSchema, CRS_Status().status};
}