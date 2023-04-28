/*
 * Copyright (C) 2022 Hopsworks AB
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

#include "src/feature_store/feature_store.h"
#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <unistd.h>
#include "NdbOut.hpp"
#include "NdbRecAttr.hpp"
#include "src/error-strings.h"
#include "src/logger.hpp"
#include "src/rdrs_rondb_connection.hpp"
#include "src/db-operations/pk/common.hpp"
#include "src/rdrs-const.h"
#include "src/retry_handler.hpp"
#include "src/ndb_api_helper.hpp"
#include "src/status.hpp"

// RonDB connection
extern RDRSRonDBConnection *rdrsRonDBConnection;

//-------------------------------------------------------------------------------------------------

RS_Status find_project_id_int(Ndb *ndb_object, const char *feature_store_name, Int32 *project_id) {
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scanOp;

  RS_Status status = select_table(ndb_object, "hopsworks", "project", &table_dict);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = start_transaction(ndb_object, &tx);
  if (status.http_code != SUCCESS) {
    return status;
  }

  std::string index_name = "projectname";
  status = get_index_scan_op(ndb_object, tx, table_dict, index_name.c_str(), &scanOp);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  status = read_tuples(ndb_object, scanOp);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  int col_id      = table_dict->getColumn("projectname")->getColumnNo();
  Uint32 col_size = (Uint32)table_dict->getColumn("projectname")->getSizeInBytes();
  assert(col_size == PROJECT_PROJECTNAME_SIZE);
  if (strlen(feature_store_name) >= col_size) {  // col_size include length byte(s)
    return RS_CLIENT_ERROR("Wrong length of the projectname");
  }

  char cmp_str[PROJECT_PROJECTNAME_SIZE];
  memcpy(cmp_str + 1, feature_store_name, PROJECT_PROJECTNAME_SIZE - 1);
  cmp_str[0] = static_cast<char>(strlen(feature_store_name));

  NdbScanFilter filter(scanOp);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, col_id, cmp_str, PROJECT_PROJECTNAME_SIZE) < 0 ||
      filter.end() < 0) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_031);
  }

  NdbRecAttr *id = scanOp->getValue("id");

  if (id == nullptr) {
    return RS_RONDB_SERVER_ERROR(err, ERROR_019);
  }

  if (tx->execute(NdbTransaction::NoCommit) != 0) {
    err = ndb_object->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_009);
  }

  bool check   = 0;
  Uint32 count = 0;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      if (count > 1) {
        return RS_SERVER_ERROR(ERROR_028 + std::string(" Expecting single ID"));
      }

      count++;
      *project_id = id->int32_value();
    } while ((check = scanOp->nextResult(false)) == 0);
  }

  // check for errors happened during the reading process
  NdbError error = scanOp->getNdbError();

  // As we are at the end we will first close the transaction and then deal with the error
  ndb_object->closeTransaction(tx);

  // storage/ndb/src/ndbapi/ndberror.cpp
  if (error.code != 4120 /*Scan already complete*/) {
    return RS_RONDB_SERVER_ERROR(error, "Failed Reading Project ID. Fn find_project_id_int");
  }

  if (count == 0) {
    return RS_CLIENT_404_ERROR();
  }

  return RS_OK;
}

//-------------------------------------------------------------------------------------------------

RS_Status find_project_id(const char *feature_store_name, Int32 *project_id) {

  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnection->GetNdbObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  RETRY_HANDLER(
    status = find_project_id_int(ndb_object, feature_store_name, project_id);
  )
  /* clang-format on */

  return status;
}

//-------------------------------------------------------------------------------------------------

RS_Status find_feature_store_id_int(Ndb *ndb_object, const char *feature_store_name,
                                    int *feature_store_id) {
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scanOp;

  RS_Status status = select_table(ndb_object, "hopsworks", "feature_store", &table_dict);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = start_transaction(ndb_object, &tx);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = get_scan_op(ndb_object, tx, "feature_store", &scanOp);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  status = read_tuples(ndb_object, scanOp);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  int col_id      = table_dict->getColumn("name")->getColumnNo();
  Uint32 col_size = (Uint32)table_dict->getColumn("name")->getSizeInBytes();
  assert(col_size == FEATURE_STORE_NAME_SIZE);
  if (strlen(feature_store_name) >= col_size) {  // col_size include length byte(s)
    return RS_CLIENT_ERROR("Wrong length of column name");
  }

  char cmp_str[FEATURE_STORE_NAME_SIZE];
  memcpy(cmp_str + 1, feature_store_name, FEATURE_STORE_NAME_SIZE - 1);
  cmp_str[0] = static_cast<char>(strlen(feature_store_name));

  NdbScanFilter filter(scanOp);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, col_id, cmp_str, FEATURE_STORE_NAME_SIZE) < 0 ||
      filter.end() < 0) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_031);
  }

  NdbRecAttr *id = scanOp->getValue("id");

  if (id == nullptr) {
    return RS_RONDB_SERVER_ERROR(err, ERROR_019);
  }

  if (tx->execute(NdbTransaction::NoCommit) != 0) {
    err = ndb_object->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_009);
  }

  bool check   = 0;
  Uint32 count = 0;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      if (count > 1) {
        return RS_SERVER_ERROR(ERROR_028 + std::string(" Expecting single ID"));
      }

      count++;
      *feature_store_id = id->int32_value();
    } while ((check = scanOp->nextResult(false)) == 0);
  }

  // check for errors happened during the reading process
  NdbError error = scanOp->getNdbError();

  // As we are at the end we will first close the transaction and then deal with the error
  ndb_object->closeTransaction(tx);

  // storage/ndb/src/ndbapi/ndberror.cpp
  if (error.code != 4120 /*Scan already complete*/) {
    return RS_RONDB_SERVER_ERROR(error, "Failed Reading Project ID. Fn find_feature_store_id_int");
  }

  if (count == 0) {
    return RS_CLIENT_404_ERROR();
  }

  return RS_OK;
}

//-------------------------------------------------------------------------------------------------

/**
 * Find feature store ID using the feature store name
 * SELECT id AS feature_store_id FROM feature_store WHERE _name = {feature_store_name}
 */
RS_Status find_feature_store_id(const char *feature_store_name, int *feature_store_id) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnection->GetNdbObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  RETRY_HANDLER(
    status = find_feature_store_id_int(ndb_object, feature_store_name, feature_store_id);
  )
  /* clang-format on */

  return status;
}

//-------------------------------------------------------------------------------------------------

RS_Status find_feature_view_id_int(Ndb *ndb_object, int feature_store_id,
                                   const char *feature_view_name, int feature_view_version,
                                   int *feature_view_id) {
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scanOp;

  RS_Status status = select_table(ndb_object, "hopsworks", "feature_view", &table_dict);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = start_transaction(ndb_object, &tx);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = get_scan_op(ndb_object, tx, "feature_view", &scanOp);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  status = read_tuples(ndb_object, scanOp);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  int name_col_id      = table_dict->getColumn("name")->getColumnNo();
  Uint32 name_col_size = (Uint32)table_dict->getColumn("name")->getSizeInBytes();
  assert(name_col_size == FEATURE_VIEW_NAME_SIZE);
  if (strlen(feature_view_name) >= name_col_size) {  // col_size include length byte(s)
    return RS_CLIENT_ERROR("Wrong length of column name");
  }

  char cmp_str[FEATURE_VIEW_NAME_SIZE];
  memcpy(cmp_str + 1, feature_view_name, FEATURE_VIEW_NAME_SIZE - 1);
  cmp_str[0] = static_cast<char>(strlen(feature_view_name));

  NdbScanFilter filter(scanOp);

  // name
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, name_col_id, cmp_str, FEATURE_VIEW_NAME_SIZE) < 0) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_031);
  }

  // version
  int version_col_id      = table_dict->getColumn("version")->getColumnNo();
  Uint32 version_col_size = (Uint32)table_dict->getColumn("version")->getSizeInBytes();

  if (filter.cmp(NdbScanFilter::COND_EQ, version_col_id, &feature_view_version, version_col_size) <
      0) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_031);
  }

  // feature store id
  int fs_id_col_id      = table_dict->getColumn("feature_store_id")->getColumnNo();
  Uint32 fs_id_col_size = (Uint32)table_dict->getColumn("feature_store_id")->getSizeInBytes();

  if (filter.cmp(NdbScanFilter::COND_EQ, fs_id_col_id, &feature_store_id, fs_id_col_size) < 0 ||
      filter.end() < 0) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_031);
  }

  NdbRecAttr *id = scanOp->getValue("id");
  if (id == nullptr) {
    return RS_RONDB_SERVER_ERROR(err, ERROR_019);
  }

  if (tx->execute(NdbTransaction::NoCommit) != 0) {
    err = ndb_object->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_009);
  }

  bool check   = 0;
  Uint32 count = 0;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      if (count > 1) {
        return RS_SERVER_ERROR(ERROR_028 + std::string(" Expecting single ID"));
      }

      count++;
      *feature_view_id = id->int32_value();
    } while ((check = scanOp->nextResult(false)) == 0);
  }

  // check for errors happened during the reading process
  NdbError error = scanOp->getNdbError();

  // As we are at the end we will first close the transaction and then deal with the error
  ndb_object->closeTransaction(tx);

  // storage/ndb/src/ndbapi/ndberror.cpp
  if (error.code != 4120 /*Scan already complete*/) {
    return RS_RONDB_SERVER_ERROR(error, "Failed Reading Project ID. Fn find_feature_store_id_int");
  }

  if (count == 0) {
    return RS_CLIENT_404_ERROR();
  }

  return RS_OK;
}

//-------------------------------------------------------------------------------------------------

/**
 * Find feature view ID using the feature store id
 * SELECT id AS feature_view_id FROM feature_view WHERE name = {feature_view_name} AND version =
 * {feature_view_version} AND feature_store_id = {feature_store_id}
 */
RS_Status find_feature_view_id(int feature_store_id, const char *feature_view_name,
                               int feature_view_version, int *feature_view_id) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnection->GetNdbObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  RETRY_HANDLER(
    status = find_feature_view_id_int(ndb_object, feature_store_id,
      feature_view_name, feature_view_version, feature_view_id);
  )
  /* clang-format on */

  return status;
}

//-------------------------------------------------------------------------------------------------

RS_Status find_training_dataset_join_data_int(Ndb *ndb_object, int feature_view_id, int *td_join_id,
                                              int *feature_group_id, char *prefix) {
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scanOp;

  RS_Status status = select_table(ndb_object, "hopsworks", "training_dataset_join", &table_dict);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = start_transaction(ndb_object, &tx);
  if (status.http_code != SUCCESS) {
    return status;
  }

  std::string index_name = "tdj_feature_view_fk";
  status = get_index_scan_op(ndb_object, tx, table_dict, index_name.c_str(), &scanOp);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  status = read_tuples(ndb_object, scanOp);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  int fv_id_col_id      = table_dict->getColumn("feature_view_id")->getColumnNo();
  Uint32 fv_id_col_size = (Uint32)table_dict->getColumn("feature_view_id")->getSizeInBytes();

  NdbScanFilter filter(scanOp);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, fv_id_col_id, &feature_view_id, fv_id_col_size) < 0 ||
      filter.end() < 0) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_031);
  }

  NdbRecAttr *td_join_id_attr       = scanOp->getValue("id");
  NdbRecAttr *feature_group_id_attr = scanOp->getValue("feature_group");
  NdbRecAttr *prefix_attr           = scanOp->getValue("prefix");
  assert(TRAINING_DATASET_JOIN_PREFIX_SIZE ==
         (Uint32)table_dict->getColumn("prefix")->getSizeInBytes());

  if (td_join_id_attr == nullptr || feature_group_id_attr == nullptr || prefix_attr == nullptr) {
    return RS_RONDB_SERVER_ERROR(err, ERROR_019);
  }

  if (tx->execute(NdbTransaction::NoCommit) != 0) {
    err = ndb_object->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_009);
  }

  bool check   = 0;
  Uint32 count = 0;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      if (count > 1) {
        return RS_SERVER_ERROR(ERROR_028 + std::string(" Expecting single row of data"));
      }

      count++;
      *td_join_id       = td_join_id_attr->int32_value();
      *feature_group_id = feature_group_id_attr->int32_value();

      // memory for "prefix" is allocated and freed by the go layer
      Uint32 prefix_attr_bytes;
      const char *prefix_data_start = nullptr;
      if (GetByteArray(prefix_attr, &prefix_data_start, &prefix_attr_bytes) != 0) {
        return RS_CLIENT_ERROR(ERROR_019);
      }

      memcpy(prefix, prefix_data_start, prefix_attr_bytes);
      prefix[prefix_attr_bytes] = 0;
    } while ((check = scanOp->nextResult(false)) == 0);
  }

  // check for errors happened during the reading process
  NdbError error = scanOp->getNdbError();

  // As we are at the end we will first close the transaction and then deal with the error
  ndb_object->closeTransaction(tx);

  // storage/ndb/src/ndbapi/ndberror.cpp
  if (error.code != 4120 /*Scan already complete*/) {
    return RS_RONDB_SERVER_ERROR(error, "Failed Reading Project ID. Fn find_project_id_int");
  }

  if (count == 0) {
    return RS_CLIENT_404_ERROR();
  }

  return RS_OK;
}

//-------------------------------------------------------------------------------------------------

/**
 * Find training dataset join data
 * SELECT id AS td_join_id, feature_group AS feature_group_id, prefix FROM training_dataset_join
 * WHERE feature_view_id = {feature_view_id}
 */
RS_Status find_training_dataset_join_data(int feature_view_id, int *td_join_id,
                                          int *feature_group_id, char *prefix) {

  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnection->GetNdbObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  RETRY_HANDLER(
    status = find_training_dataset_join_data_int(ndb_object,
      feature_view_id, td_join_id, feature_group_id, prefix);
  )
  /* clang-format on */

  return status;
}

//-------------------------------------------------------------------------------------------------

RS_Status find_feature_group_data_int(Ndb *ndb_object, int feature_group_id, char *name,
                                      int *online_enabled, int *feature_store_id) {

  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbOperation *ndbOp;

  RS_Status status = select_table(ndb_object, "hopsworks", "feature_group", &table_dict);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = start_transaction(ndb_object, &tx);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = get_op(ndb_object, tx, "feature_group", &ndbOp);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  status = read_tuple(ndb_object, ndbOp);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  if (ndbOp->equal("id", feature_group_id) != 0) {
    return RS_SERVER_ERROR(ERROR_023);
  }

  NdbRecAttr *nameAttr           = ndbOp->getValue("name", nullptr);
  NdbRecAttr *onlineEnabledAttr  = ndbOp->getValue("online_enabled", nullptr);
  NdbRecAttr *featureStoreIDAttr = ndbOp->getValue("feature_store_id", nullptr);
  assert(FEATURE_GROUP_NAME_SIZE == (Uint32)table_dict->getColumn("name")->getSizeInBytes());

  if (nameAttr == nullptr || onlineEnabledAttr == nullptr || featureStoreIDAttr == nullptr) {
    return RS_RONDB_SERVER_ERROR(err, ERROR_019);
  }

  if (tx->execute(NdbTransaction::Commit) != 0) {
    err = ndb_object->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_009);
  }

  if (ndbOp->getNdbError().classification == NdbError::NoDataFound) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_404_ERROR();
  }

  *online_enabled   = onlineEnabledAttr->u_8_value();
  *feature_store_id = featureStoreIDAttr->int32_value();

  Uint32 name_attr_bytes;
  const char *name_attr_start = nullptr;
  if (GetByteArray(nameAttr, &name_attr_start, &name_attr_bytes) != 0) {
    return RS_CLIENT_ERROR(ERROR_019);
  }

  memcpy(name, name_attr_start, name_attr_bytes);
  name[name_attr_bytes] = 0;

  // As we are at the end we will first close the transaction and then deal with the error
  ndb_object->closeTransaction(tx);

  return RS_OK;
}

//-------------------------------------------------------------------------------------------------

/**
 * Find feature group data
 * SELECT name, online_enabled, feature_store_id FROM feature_group WHERE id = {feature_group_id}
 */
RS_Status find_feature_group_data(int feature_group_id, char *name, int *online_enabled,
                                  int *feature_store_id) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnection->GetNdbObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  RETRY_HANDLER(
    status = find_feature_group_data_int(ndb_object,
      feature_group_id, name, online_enabled, feature_store_id);
  )
  /* clang-format on */

  return status;
}

//-------------------------------------------------------------------------------------------------

RS_Status find_training_dataset_data_int(Ndb *ndb_object, int feature_view_id,
                                         Training_Dataset_Feature **tdfs, int *tdfs_size) {
  NdbError err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scanOp;

  RS_Status status = select_table(ndb_object, "hopsworks", "training_dataset_feature", &table_dict);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = start_transaction(ndb_object, &tx);
  if (status.http_code != SUCCESS) {
    return status;
  }

  std::string index_name = "tdf_feature_view_fk";
  status = get_index_scan_op(ndb_object, tx, table_dict, index_name.c_str(), &scanOp);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  status = read_tuples(ndb_object, scanOp);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  int col_id      = table_dict->getColumn("feature_view_id")->getColumnNo();
  Uint32 col_size = (Uint32)table_dict->getColumn("feature_view_id")->getSizeInBytes();

  NdbScanFilter filter(scanOp);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, col_id, &feature_view_id, col_size) < 0 ||
      filter.end() < 0) {
    err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_031);
  }

  NdbRecAttr *feature_id_attr       = scanOp->getValue("id", nullptr);
  NdbRecAttr *training_dataset_attr = scanOp->getValue("training_dataset", nullptr);
  NdbRecAttr *feature_group_id_attr = scanOp->getValue("feature_group", nullptr);
  NdbRecAttr *name_attr             = scanOp->getValue("name", nullptr);
  NdbRecAttr *type_attr             = scanOp->getValue("type", nullptr);
  NdbRecAttr *td_join_id_attr       = scanOp->getValue("td_join", nullptr);
  NdbRecAttr *idx_attr              = scanOp->getValue("idx", nullptr);
  NdbRecAttr *label_attr            = scanOp->getValue("label", nullptr);
  NdbRecAttr *transformation_function_id_attr =
      scanOp->getValue("transformation_function", nullptr);
  NdbRecAttr *feature_view_id_attr = scanOp->getValue("feature_view_id", nullptr);

  if (feature_id_attr == nullptr || training_dataset_attr == nullptr ||
      feature_group_id_attr == nullptr || name_attr == nullptr || type_attr == nullptr ||
      td_join_id_attr == nullptr || idx_attr == nullptr || label_attr == nullptr ||
      transformation_function_id_attr == nullptr || feature_view_id_attr == nullptr) {
    return RS_RONDB_SERVER_ERROR(err, ERROR_019);
  }

  assert(TRAINING_DATASET_FEATURE_NAME_SIZE ==
         (Uint32)table_dict->getColumn("name")->getSizeInBytes());
  assert(TRAINING_DATASET_FEATURE_TYPE_SIZE ==
         (Uint32)table_dict->getColumn("type")->getSizeInBytes());

  if (tx->execute(NdbTransaction::NoCommit) != 0) {
    err = ndb_object->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(err, ERROR_009);
  }

  std::vector<Training_Dataset_Feature> tdfsv;
  bool check = 0;
  while ((check = scanOp->nextResult(true)) == 0) {
    do {
      Training_Dataset_Feature tdf;
      tdf.feature_id                 = feature_id_attr->int32_value();
      tdf.training_dataset           = training_dataset_attr->int32_value();
      tdf.feature_group_id           = feature_group_id_attr->int32_value();
      tdf.td_join_id                 = td_join_id_attr->int32_value();
      tdf.idx                        = idx_attr->int32_value();
      tdf.label                      = label_attr->int32_value();
      tdf.transformation_function_id = transformation_function_id_attr->int32_value();
      tdf.feature_view_id            = feature_group_id_attr->int32_value();

      // name
      Uint32 name_attr_bytes;
      const char *name_attr_start = nullptr;
      if (GetByteArray(name_attr, &name_attr_start, &name_attr_bytes) != 0) {
        return RS_CLIENT_ERROR(ERROR_019);
      }
      memcpy(tdf.name, name_attr_start, name_attr_bytes);
      tdf.name[name_attr_bytes] = 0;

      // type
      Uint32 type_attr_bytes;
      const char *type_attr_start = nullptr;
      if (GetByteArray(type_attr, &type_attr_start, &type_attr_bytes) != 0) {
        return RS_CLIENT_ERROR(ERROR_019);
      }
      memcpy(tdf.data_type, type_attr_start, type_attr_bytes);
      tdf.data_type[type_attr_bytes] = 0;

      tdfsv.push_back(tdf);
    } while ((check = scanOp->nextResult(false)) == 0);
  }

  // check for errors happened during the reading process
  NdbError error = scanOp->getNdbError();

  // As we are at the end we will first close the transaction and then deal with the error
  ndb_object->closeTransaction(tx);

  // storage/ndb/src/ndbapi/ndberror.cpp
  if (error.code != 4120 /*Scan already complete*/) {
    return RS_RONDB_SERVER_ERROR(error, "Failed Reading Project ID. Fn find_project_id_int");
  }

  if (tdfsv.size() == 0) {
    return RS_CLIENT_404_ERROR();
  }

  // freed by CGO
  *tdfs_size = tdfsv.size();
  void* ptr = (Training_Dataset_Feature *)malloc(tdfsv.size() * sizeof(Training_Dataset_Feature));
  *tdfs =(Training_Dataset_Feature *) ptr;
  for (Uint64 i = 0; i < tdfsv.size(); i++) {
    *(*tdfs + i) = tdfsv[i];
  }
  tdfsv.clear();

  return RS_OK;
}

/**
 * Find training_dataset_feature
 * SELECT * from training_dataset_feature  WHERE feature_view_id = {feature_view_id}
 */
RS_Status find_training_dataset_data(int feature_view_id, Training_Dataset_Feature **tdf,
                                     int *tdf_size) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnection->GetNdbObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  RETRY_HANDLER(
    status = find_training_dataset_data_int(ndb_object, feature_view_id, tdf, tdf_size); 
  )
  /* clang-format on */

  return status;
}
