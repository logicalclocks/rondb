/*
 * Copyright (C) 2023 Hopsworks AB
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

#include <unistd.h>

#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "NdbOut.hpp"
#include "NdbRecAttr.hpp"
#include "src/db-operations/pk/common.hpp"
#include "src/error-strings.h"
#include "src/logger.hpp"
#include "src/ndb_api_helper.hpp"
#include "src/rdrs-const.h"
#include "src/rdrs_rondb_connection.hpp"
#include "src/retry_handler.hpp"
#include "src/status.hpp"

// RonDB connection
extern RDRSRonDBConnection *rdrsRonDBConnection;

//-------------------------------------------------------------------------------------------------

RS_Status find_project_id_int(Ndb *ndb_object, const char *feature_store_name, Int32 *project_id) {
  NdbError ndb_error;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scan_op;

  RS_Status status = select_table(ndb_object, "hopsworks", "project", &table_dict);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = start_transaction(ndb_object, &tx);
  if (status.http_code != SUCCESS) {
    return status;
  }

  std::string index_name = "projectname";
  status = get_index_scan_op(ndb_object, tx, table_dict, index_name.c_str(), &scan_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  status = read_tuples(ndb_object, scan_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  int projectname_col_id      = table_dict->getColumn("projectname")->getColumnNo();
  Uint32 projectname_col_size = (Uint32)table_dict->getColumn("projectname")->getSizeInBytes();
  assert(projectname_col_size == PROJECT_PROJECTNAME_SIZE);

  size_t feature_store_name_len = strlen(feature_store_name);
  if (feature_store_name_len >
      (projectname_col_size - bytes_for_ndb_str_len(PROJECT_PROJECTNAME_SIZE))) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_ERROR("Wrong length of the projectname");
  }

  // Note: projectname is varchar column
  char cmp_str[PROJECT_PROJECTNAME_SIZE];
  memcpy(cmp_str + bytes_for_ndb_str_len(PROJECT_PROJECTNAME_SIZE), feature_store_name,
         feature_store_name_len);
  cmp_str[0] = static_cast<char>(feature_store_name_len);

  NdbScanFilter filter(scan_op);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, projectname_col_id, cmp_str, PROJECT_PROJECTNAME_SIZE) <
          0 ||
      filter.end() < 0) {
    ndb_error = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_031);
  }

  NdbRecAttr *id_attr = scan_op->getValue("id");
  if (id_attr == nullptr) {
    ndb_error = scan_op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_019);
  }

  if (tx->execute(NdbTransaction::NoCommit) != 0) {
    ndb_error = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_009);
  }

  bool check   = 0;
  Uint32 count = 0;
  while ((check = scan_op->nextResult(true)) == 0) {
    do {
      if (count > 1) {
        ndb_object->closeTransaction(tx);
        return RS_SERVER_ERROR(ERROR_028 + std::string(" Expecting single ID"));
      }

      count++;
      *project_id = id_attr->int32_value();
    } while ((check = scan_op->nextResult(false)) == 0);
  }

  // check for errors happened during the reading process
  NdbError error = scan_op->getNdbError();

  // As we are at the end we will first close the transaction and then deal with
  // the error
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

  rdrsRonDBConnection->ReturnNDBObjectToPool(ndb_object, &status);

  return status;
}

//-------------------------------------------------------------------------------------------------

RS_Status find_feature_store_id_int(Ndb *ndb_object, const char *feature_store_name,
                                    int *feature_store_id) {
  NdbError ndb_err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scan_op;

  RS_Status status = select_table(ndb_object, "hopsworks", "feature_store", &table_dict);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = start_transaction(ndb_object, &tx);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = get_scan_op(ndb_object, tx, "feature_store", &scan_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  status = read_tuples(ndb_object, scan_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  int name_col_id      = table_dict->getColumn("name")->getColumnNo();
  Uint32 name_col_size = (Uint32)table_dict->getColumn("name")->getSizeInBytes();
  assert(name_col_size == FEATURE_STORE_NAME_SIZE);

  size_t feature_store_name_len = strlen(feature_store_name);
  if (feature_store_name_len > (name_col_size - bytes_for_ndb_str_len(FEATURE_STORE_NAME_SIZE))) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_ERROR("Wrong length of column name");
  }

  // Note: name is varchar column
  char cmp_str[FEATURE_STORE_NAME_SIZE];
  memcpy(cmp_str + bytes_for_ndb_str_len(FEATURE_STORE_NAME_SIZE), feature_store_name,
         feature_store_name_len);
  cmp_str[0] = static_cast<char>(strlen(feature_store_name));

  NdbScanFilter filter(scan_op);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, name_col_id, cmp_str, FEATURE_STORE_NAME_SIZE) < 0 ||
      filter.end() < 0) {
    ndb_err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_err, ERROR_031);
  }

  NdbRecAttr *id = scan_op->getValue("id");
  if (id == nullptr) {
    ndb_err = scan_op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_err, ERROR_019);
  }

  if (tx->execute(NdbTransaction::NoCommit) != 0) {
    ndb_err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_err, ERROR_009);
  }

  bool check   = 0;
  Uint32 count = 0;
  while ((check = scan_op->nextResult(true)) == 0) {
    do {
      if (count > 1) {
        ndb_object->closeTransaction(tx);
        return RS_SERVER_ERROR(ERROR_028 + std::string(" Expecting single ID"));
      }

      count++;
      *feature_store_id = id->int32_value();
    } while ((check = scan_op->nextResult(false)) == 0);
  }

  // check for errors happened during the reading process
  NdbError error = scan_op->getNdbError();

  // As we are at the end we will first close the transaction and then deal with
  // the error
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
 * SELECT id AS feature_store_id FROM feature_store WHERE _name =
 * {feature_store_name}
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

  rdrsRonDBConnection->ReturnNDBObjectToPool(ndb_object, &status);

  return status;
}

//-------------------------------------------------------------------------------------------------

RS_Status find_feature_view_id_int(Ndb *ndb_object, int feature_store_id,
                                   const char *feature_view_name, int feature_view_version,
                                   int *feature_view_id) {
  NdbError ndb_error;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scan_op;

  RS_Status status = select_table(ndb_object, "hopsworks", "feature_view", &table_dict);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = start_transaction(ndb_object, &tx);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = get_scan_op(ndb_object, tx, "feature_view", &scan_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  status = read_tuples(ndb_object, scan_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  int name_col_id      = table_dict->getColumn("name")->getColumnNo();
  Uint32 name_col_size = (Uint32)table_dict->getColumn("name")->getSizeInBytes();
  assert(name_col_size == FEATURE_VIEW_NAME_SIZE);

  size_t feature_view_name_len = strlen(feature_view_name);
  if (feature_view_name_len >
      (name_col_size -
       bytes_for_ndb_str_len(FEATURE_VIEW_NAME_SIZE))) {  // col_size include length byte(s)
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_ERROR("Wrong length of column name");
  }

  // Note: feature_view is varchar column
  char cmp_str[FEATURE_VIEW_NAME_SIZE];
  memcpy(cmp_str + bytes_for_ndb_str_len(FEATURE_VIEW_NAME_SIZE), feature_view_name,
         feature_view_name_len);
  cmp_str[0] = static_cast<char>(strlen(feature_view_name));

  NdbScanFilter filter(scan_op);

  // name
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, name_col_id, cmp_str, FEATURE_VIEW_NAME_SIZE) < 0) {
    ndb_error = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_031);
  }

  // version
  int version_col_id      = table_dict->getColumn("version")->getColumnNo();
  Uint32 version_col_size = (Uint32)table_dict->getColumn("version")->getSizeInBytes();

  if (filter.cmp(NdbScanFilter::COND_EQ, version_col_id, &feature_view_version, version_col_size) <
      0) {
    ndb_error = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_031);
  }

  // feature store id
  int fs_id_col_id      = table_dict->getColumn("feature_store_id")->getColumnNo();
  Uint32 fs_id_col_size = (Uint32)table_dict->getColumn("feature_store_id")->getSizeInBytes();

  if (filter.cmp(NdbScanFilter::COND_EQ, fs_id_col_id, &feature_store_id, fs_id_col_size) < 0 ||
      filter.end() < 0) {
    ndb_error = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_031);
  }

  NdbRecAttr *id_attr = scan_op->getValue("id");
  if (id_attr == nullptr) {
    ndb_error = scan_op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_019);
  }

  if (tx->execute(NdbTransaction::NoCommit) != 0) {
    ndb_error = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_009);
  }

  bool check   = 0;
  Uint32 count = 0;
  while ((check = scan_op->nextResult(true)) == 0) {
    do {
      if (count > 1) {
        ndb_object->closeTransaction(tx);
        return RS_SERVER_ERROR(ERROR_028 + std::string(" Expecting single ID"));
      }

      count++;
      *feature_view_id = id_attr->int32_value();
    } while ((check = scan_op->nextResult(false)) == 0);
  }

  // check for errors happened during the reading process
  NdbError error = scan_op->getNdbError();

  // As we are at the end we will first close the transaction and then deal with
  // the error
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
 * SELECT id AS feature_view_id FROM feature_view WHERE name =
 * {feature_view_name} AND version = {feature_view_version} AND feature_store_id
 * = {feature_store_id}
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

  rdrsRonDBConnection->ReturnNDBObjectToPool(ndb_object, &status);

  return status;
}

//-------------------------------------------------------------------------------------------------

RS_Status find_training_dataset_join_data_int(Ndb *ndb_object, int feature_view_id,
                                              Training_Dataset_Join **tdjs, int *tdjs_size) {
  NdbError ndb_err;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scan_op;

  RS_Status status = select_table(ndb_object, "hopsworks", "training_dataset_join", &table_dict);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = start_transaction(ndb_object, &tx);
  if (status.http_code != SUCCESS) {
    return status;
  }

  std::string index_name = "tdj_feature_view_fk";
  status = get_index_scan_op(ndb_object, tx, table_dict, index_name.c_str(), &scan_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  status = read_tuples(ndb_object, scan_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  int fv_id_col_id      = table_dict->getColumn("feature_view_id")->getColumnNo();
  Uint32 fv_id_col_size = (Uint32)table_dict->getColumn("feature_view_id")->getSizeInBytes();

  NdbScanFilter filter(scan_op);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, fv_id_col_id, &feature_view_id, fv_id_col_size) < 0 ||
      filter.end() < 0) {
    ndb_err = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_err, ERROR_031);
  }

  NdbRecAttr *td_join_id_attr = scan_op->getValue("id", nullptr);
  NdbRecAttr *prefix_attr     = scan_op->getValue("prefix", nullptr);
  assert(TRAINING_DATASET_JOIN_PREFIX_SIZE ==
         (Uint32)table_dict->getColumn("prefix")->getSizeInBytes());
  NdbRecAttr *idx_attr = scan_op->getValue("idx", nullptr);

  if (td_join_id_attr == nullptr || prefix_attr == nullptr || idx_attr == nullptr) {
    ndb_err = scan_op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_err, ERROR_019);
  }

  if (tx->execute(NdbTransaction::NoCommit) != 0) {
    ndb_err = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_err, ERROR_009);
  }

  bool check = 0;
  std::vector<Training_Dataset_Join> tdjsv;
  while ((check = scan_op->nextResult(true)) == 0) {
    do {
      Training_Dataset_Join tdj;
      tdj.id  = td_join_id_attr->int32_value();
      tdj.idx = idx_attr->int32_value();

      if (prefix_attr->isNULL()) {
        tdj.prefix[0] = '\0';
      } else {
        Uint32 prefix_attr_bytes;
        const char *prefix_data_start = nullptr;
        if (GetByteArray(prefix_attr, &prefix_data_start, &prefix_attr_bytes) != 0) {
          ndb_object->closeTransaction(tx);
          return RS_CLIENT_ERROR(ERROR_019);
        }

        memcpy(tdj.prefix, prefix_data_start, prefix_attr_bytes);
        tdj.prefix[prefix_attr_bytes] = '\0';
      }

      tdjsv.push_back(tdj);
    } while ((check = scan_op->nextResult(false)) == 0);
  }

  // check for errors happened during the reading process
  NdbError error = scan_op->getNdbError();

  // As we are at the end we will first close the transaction and then deal with
  // the error
  ndb_object->closeTransaction(tx);

  // storage/ndb/src/ndbapi/ndberror.cpp
  if (error.code != 4120 /*Scan already complete*/) {
    return RS_RONDB_SERVER_ERROR(error, "Failed Reading Project ID. Fn find_project_id_int");
  }

  if (tdjsv.size() == 0) {
    return RS_CLIENT_404_ERROR();
  }

  // freed by CGO
  *tdjs_size = tdjsv.size();
  void *ptr  = (Training_Dataset_Join *)malloc(tdjsv.size() * sizeof(Training_Dataset_Join));
  *tdjs      = (Training_Dataset_Join *)ptr;
  for (Uint64 i = 0; i < tdjsv.size(); i++) {
    (*tdjs + i)->id  = tdjsv[i].id;
    (*tdjs + i)->idx = tdjsv[i].idx;
    memcpy((*tdjs + i)->prefix, tdjsv[i].prefix,
           strlen(tdjsv[i].prefix) + 1);  // +1 or '\0'
  }
  tdjsv.clear();

  return RS_OK;
}

//-------------------------------------------------------------------------------------------------

/**
 * Find training dataset join data
 * SELECT id AS td_join_id, feature_group AS feature_group_id, prefix FROM
 * training_dataset_join WHERE feature_view_id = {feature_view_id}
 */
RS_Status find_training_dataset_join_data(int feature_view_id, Training_Dataset_Join **tdjs,
                                          int *tdjs_size) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnection->GetNdbObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  RETRY_HANDLER(
    status = find_training_dataset_join_data_int(ndb_object,
      feature_view_id, tdjs, tdjs_size);
  )
  /* clang-format on */

  rdrsRonDBConnection->ReturnNDBObjectToPool(ndb_object, &status);

  return status;
}

//-------------------------------------------------------------------------------------------------

RS_Status find_feature_group_data_int(Ndb *ndb_object, int feature_group_id, Feature_Group *fg) {
  NdbError ndb_error;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbOperation *ndb_op;

  RS_Status status = select_table(ndb_object, "hopsworks", "feature_group", &table_dict);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = start_transaction(ndb_object, &tx);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = get_op(ndb_object, tx, "feature_group", &ndb_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  status = read_tuple(ndb_object, ndb_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  if (ndb_op->equal("id", feature_group_id) != 0) {
    ndb_error = ndb_op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_023);
  }

  NdbRecAttr *online_enabled_attr = nullptr;
  // In hopsworks 3.1, there is no column `online_enabled`. This is a workaround
  // for customer using hopsworks 3.1.
  if (table_dict->getColumn("online_enabled") != nullptr) {
    online_enabled_attr = ndb_op->getValue("online_enabled", nullptr);
    if (online_enabled_attr == nullptr) {
      ndb_error = ndb_op->getNdbError();
      ndb_object->closeTransaction(tx);
      return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_019);
    }
  }

  NdbRecAttr *feature_store_id_attr      = ndb_op->getValue("feature_store_id", nullptr);
  NdbRecAttr *feature_group_version_attr = ndb_op->getValue("version", nullptr);
  NdbRecAttr *on_demand_feature_group_id_attr =
      ndb_op->getValue("on_demand_feature_group_id", nullptr);
  NdbRecAttr *cached_feature_group_id_attr = ndb_op->getValue("cached_feature_group_id", nullptr);
  NdbRecAttr *stream_feature_group_id_attr = ndb_op->getValue("stream_feature_group_id", nullptr);

  NdbRecAttr *name_attr = ndb_op->getValue("name", nullptr);
  assert(FEATURE_GROUP_NAME_SIZE == (Uint32)table_dict->getColumn("name")->getSizeInBytes());

  if (name_attr == nullptr || feature_store_id_attr == nullptr ||
      feature_group_version_attr == nullptr || on_demand_feature_group_id_attr == nullptr ||
      cached_feature_group_id_attr == nullptr || stream_feature_group_id_attr == nullptr) {
    ndb_error = ndb_op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_019);
  }

  if (tx->execute(NdbTransaction::Commit) != 0) {
    ndb_error = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_009);
  }

  if (ndb_op->getNdbError().classification == NdbError::NoDataFound) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_404_ERROR();
  }

  if (online_enabled_attr == nullptr || online_enabled_attr->isNULL()) {
    // This is due to incompatible schema, assume online enabled.
    fg->online_enabled = 1;
  } else {
    fg->online_enabled = online_enabled_attr->u_8_value();
  }

  fg->feature_store_id = feature_store_id_attr->int32_value();
  fg->version          = feature_group_version_attr->int32_value();

  Uint32 name_attr_bytes;
  const char *name_attr_start = nullptr;
  if (GetByteArray(name_attr, &name_attr_start, &name_attr_bytes) != 0) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_ERROR(ERROR_019);
  }

  memcpy(fg->name, name_attr_start, name_attr_bytes);
  fg->name[name_attr_bytes] = '\0';

  ndb_object->closeTransaction(tx);

  return RS_OK;
}

//-------------------------------------------------------------------------------------------------

/**
 * Find feature group data
 * SELECT name, online_enabled, feature_store_id FROM feature_group WHERE id =
 * {feature_group_id}
 */
RS_Status find_feature_group_data(int feature_group_id, Feature_Group *fg) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnection->GetNdbObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  RETRY_HANDLER(
    status = find_feature_group_data_int(ndb_object, feature_group_id, fg);
  )
  /* clang-format on */

  rdrsRonDBConnection->ReturnNDBObjectToPool(ndb_object, &status);

  return status;
}

//-------------------------------------------------------------------------------------------------

RS_Status find_training_dataset_data_int(Ndb *ndb_object, int feature_view_id,
                                         Training_Dataset_Feature **tdfs, int *tdfs_size) {
  NdbError ndb_error;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scan_op;

  RS_Status status = select_table(ndb_object, "hopsworks", "training_dataset_feature", &table_dict);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = start_transaction(ndb_object, &tx);
  if (status.http_code != SUCCESS) {
    return status;
  }

  std::string index_name = "tdf_feature_view_fk";
  status = get_index_scan_op(ndb_object, tx, table_dict, index_name.c_str(), &scan_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  status = read_tuples(ndb_object, scan_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  int fv_id_col_id      = table_dict->getColumn("feature_view_id")->getColumnNo();
  Uint32 fv_id_col_size = (Uint32)table_dict->getColumn("feature_view_id")->getSizeInBytes();

  NdbScanFilter filter(scan_op);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, fv_id_col_id, &feature_view_id, fv_id_col_size) < 0 ||
      filter.end() < 0) {
    ndb_error = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_031);
  }

  assert(TRAINING_DATASET_FEATURE_NAME_SIZE ==
         (Uint32)table_dict->getColumn("name")->getSizeInBytes());
  assert(TRAINING_DATASET_FEATURE_TYPE_SIZE ==
         (Uint32)table_dict->getColumn("type")->getSizeInBytes());

  NdbRecAttr *feature_id_attr       = scan_op->getValue("id", nullptr);
  NdbRecAttr *training_dataset_attr = scan_op->getValue("training_dataset", nullptr);
  NdbRecAttr *feature_group_id_attr = scan_op->getValue("feature_group", nullptr);
  NdbRecAttr *name_attr             = scan_op->getValue("name", nullptr);
  NdbRecAttr *type_attr             = scan_op->getValue("type", nullptr);
  NdbRecAttr *td_join_id_attr       = scan_op->getValue("td_join", nullptr);
  NdbRecAttr *idx_attr              = scan_op->getValue("idx", nullptr);
  NdbRecAttr *label_attr            = scan_op->getValue("label", nullptr);
  NdbRecAttr *transformation_function_id_attr =
      scan_op->getValue("transformation_function", nullptr);
  NdbRecAttr *feature_view_id_attr = scan_op->getValue("feature_view_id", nullptr);

  if (feature_id_attr == nullptr || training_dataset_attr == nullptr ||
      feature_group_id_attr == nullptr || name_attr == nullptr || type_attr == nullptr ||
      td_join_id_attr == nullptr || idx_attr == nullptr || label_attr == nullptr ||
      transformation_function_id_attr == nullptr || feature_view_id_attr == nullptr) {
    ndb_error = scan_op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_019);
  }

  if (tx->execute(NdbTransaction::NoCommit) != 0) {
    ndb_error = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_009);
  }

  std::vector<Training_Dataset_Feature> tdfsv;
  bool check = 0;
  while ((check = scan_op->nextResult(true)) == 0) {
    do {
      Training_Dataset_Feature tdf;
      tdf.feature_id                 = feature_id_attr->int32_value();
      tdf.training_dataset           = training_dataset_attr->int32_value();
      tdf.feature_group_id           = feature_group_id_attr->int32_value();
      tdf.td_join_id                 = td_join_id_attr->int32_value();
      tdf.idx                        = idx_attr->int32_value();
      tdf.label                      = label_attr->int32_value();
      tdf.transformation_function_id = transformation_function_id_attr->int32_value();
      tdf.feature_view_id            = feature_view_id_attr->int32_value();

      // name
      Uint32 name_attr_bytes;
      const char *name_attr_start = nullptr;
      if (GetByteArray(name_attr, &name_attr_start, &name_attr_bytes) != 0) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(ERROR_019);
      }

      memcpy(tdf.name, name_attr_start, name_attr_bytes);
      tdf.name[name_attr_bytes] = '\0';

      // type
      Uint32 type_attr_bytes;
      const char *type_attr_start = nullptr;
      if (GetByteArray(type_attr, &type_attr_start, &type_attr_bytes) != 0) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(ERROR_019);
      }
      memcpy(tdf.data_type, type_attr_start, type_attr_bytes);
      tdf.data_type[type_attr_bytes] = '\0';

      tdfsv.push_back(tdf);
    } while ((check = scan_op->nextResult(false)) == 0);
  }

  // check for errors happened during the reading process
  NdbError error = scan_op->getNdbError();

  // As we are at the end we will first close the transaction and then deal with
  // the error
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
  void *ptr  = (Training_Dataset_Feature *)malloc(tdfsv.size() * sizeof(Training_Dataset_Feature));
  *tdfs      = (Training_Dataset_Feature *)ptr;
  for (Uint64 i = 0; i < tdfsv.size(); i++) {
    (*tdfs + i)->feature_id                 = tdfsv[i].feature_id;
    (*tdfs + i)->training_dataset           = tdfsv[i].training_dataset;
    (*tdfs + i)->feature_group_id           = tdfsv[i].feature_group_id;
    (*tdfs + i)->td_join_id                 = tdfsv[i].td_join_id;
    (*tdfs + i)->idx                        = tdfsv[i].idx;
    (*tdfs + i)->label                      = tdfsv[i].label;
    (*tdfs + i)->transformation_function_id = tdfsv[i].transformation_function_id;
    (*tdfs + i)->feature_view_id            = tdfsv[i].feature_view_id;
    memcpy((*tdfs + i)->name, tdfsv[i].name,
           strlen(tdfsv[i].name) + 1);  // +1 for '\0'
    memcpy((*tdfs + i)->data_type, tdfsv[i].data_type,
           strlen(tdfsv[i].data_type) + 1);  // +1 for '\0'
  }
  tdfsv.clear();

  return RS_OK;
}

//-------------------------------------------------------------------------------------------------

/**
 * Find training_dataset_feature
 * SELECT * from training_dataset_feature  WHERE feature_view_id =
 * {feature_view_id}
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

  rdrsRonDBConnection->ReturnNDBObjectToPool(ndb_object, &status);

  return status;
}

//-------------------------------------------------------------------------------------------------

RS_Status find_feature_store_data_int(Ndb *ndb_object, int feature_store_id, char *name) {
  NdbError ndb_error;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbOperation *ndb_op;

  RS_Status status = select_table(ndb_object, "hopsworks", "feature_store", &table_dict);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = start_transaction(ndb_object, &tx);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = get_op(ndb_object, tx, "feature_store", &ndb_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  status = read_tuple(ndb_object, ndb_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  if (ndb_op->equal("id", feature_store_id) != 0) {
    return RS_SERVER_ERROR(ERROR_023);
  }

  NdbRecAttr *name_attr = ndb_op->getValue("name", nullptr);
  assert(FEATURE_STORE_NAME_SIZE == (Uint32)table_dict->getColumn("name")->getSizeInBytes());

  if (name_attr == nullptr) {
    ndb_error = ndb_op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_019);
  }

  if (tx->execute(NdbTransaction::Commit) != 0) {
    ndb_error = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_009);
  }

  if (ndb_op->getNdbError().classification == NdbError::NoDataFound) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_404_ERROR();
  }

  Uint32 name_attr_bytes;
  const char *name_attr_start = nullptr;
  if (GetByteArray(name_attr, &name_attr_start, &name_attr_bytes) != 0) {
    ndb_object->closeTransaction(tx);
    return RS_CLIENT_ERROR(ERROR_019);
  }

  // this memory is created and freed by the golayer
  memcpy(name, name_attr_start, name_attr_bytes);
  name[name_attr_bytes] = '\0';

  ndb_object->closeTransaction(tx);

  return RS_OK;
}

//-------------------------------------------------------------------------------------------------

/**
 * Find feature store data
 * SELECT name FROM feature_store WHERE id = {feature_store_id}
 */
RS_Status find_feature_store_data(int feature_store_id, char *name) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnection->GetNdbObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  RETRY_HANDLER(
    status = find_feature_store_data_int(ndb_object, feature_store_id, name);
  )
  /* clang-format on */

  rdrsRonDBConnection->ReturnNDBObjectToPool(ndb_object, &status);

  return status;
}

//-------------------------------------------------------------------------------------------------

RS_Status find_serving_key_data_int(Ndb *ndb_object, int feature_view_id,
                                    Serving_Key **serving_keys, int *sk_size) {
  NdbError ndb_error;
  const NdbDictionary::Table *table_dict;
  NdbTransaction *tx;
  NdbScanOperation *scan_op;

  RS_Status status = select_table(ndb_object, "hopsworks", "serving_key", &table_dict);
  if (status.http_code != SUCCESS) {
    return status;
  }

  status = start_transaction(ndb_object, &tx);
  if (status.http_code != SUCCESS) {
    return status;
  }

  std::string index_name = "feature_view_id";
  status = get_index_scan_op(ndb_object, tx, table_dict, index_name.c_str(), &scan_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  status = read_tuples(ndb_object, scan_op);
  if (status.http_code != SUCCESS) {
    ndb_object->closeTransaction(tx);
    return status;
  }

  int fv_id_col_id      = table_dict->getColumn("feature_view_id")->getColumnNo();
  Uint32 fv_id_col_size = (Uint32)table_dict->getColumn("feature_view_id")->getSizeInBytes();

  NdbScanFilter filter(scan_op);
  if (filter.begin(NdbScanFilter::AND) < 0 ||
      filter.cmp(NdbScanFilter::COND_EQ, fv_id_col_id, &feature_view_id, fv_id_col_size) < 0 ||
      filter.end() < 0) {
    ndb_error = filter.getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_031);
  }

  assert(TRAINING_DATASET_FEATURE_NAME_SIZE ==
         (Uint32)table_dict->getColumn("feature_name")->getSizeInBytes());
  assert(TRAINING_DATASET_JOIN_PREFIX_SIZE ==
         (Uint32)table_dict->getColumn("prefix")->getSizeInBytes());
  assert(TRAINING_DATASET_FEATURE_NAME_SIZE ==
         (Uint32)table_dict->getColumn("join_on")->getSizeInBytes());

  NdbRecAttr *feature_group_id_attr = scan_op->getValue("feature_group_id", nullptr);
  NdbRecAttr *feature_name_attr     = scan_op->getValue("feature_name", nullptr);
  NdbRecAttr *prefix_attr           = scan_op->getValue("prefix", nullptr);
  NdbRecAttr *required_attr         = scan_op->getValue("required", nullptr);
  NdbRecAttr *join_on_attr          = scan_op->getValue("join_on", nullptr);
  NdbRecAttr *join_index_attr       = scan_op->getValue("join_index", nullptr);

  if (feature_group_id_attr == nullptr || feature_name_attr == nullptr || prefix_attr == nullptr ||
      required_attr == nullptr || join_on_attr == nullptr || join_index_attr == nullptr) {
    ndb_error = scan_op->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_019);
  }

  if (tx->execute(NdbTransaction::NoCommit) != 0) {
    ndb_error = tx->getNdbError();
    ndb_object->closeTransaction(tx);
    return RS_RONDB_SERVER_ERROR(ndb_error, ERROR_009);
  }

  std::vector<Serving_Key> serving_keys_vec;
  bool check = 0;
  while ((check = scan_op->nextResult(true)) == 0) {
    do {
      Serving_Key serving_key;
      serving_key.feature_group_id = feature_group_id_attr->int32_value();
      serving_key.required         = required_attr->int32_value();
      serving_key.join_index       = join_index_attr->int32_value();

      // feature name
      Uint32 feature_name_attr_bytes;
      const char *feature_name_attr_start = nullptr;
      if (GetByteArray(feature_name_attr, &feature_name_attr_start, &feature_name_attr_bytes) !=
          0) {
        ndb_object->closeTransaction(tx);
        return RS_CLIENT_ERROR(ERROR_019);
      }

      memcpy(serving_key.feature_name, feature_name_attr_start, feature_name_attr_bytes);
      serving_key.feature_name[feature_name_attr_bytes] = '\0';

      // prefix
      if (prefix_attr->isNULL()) {
        serving_key.prefix[0] = '\0';
      } else {
        Uint32 prefix_attr_bytes;
        const char *prefix_attr_start = nullptr;

        if (GetByteArray(prefix_attr, &prefix_attr_start, &prefix_attr_bytes) != 0) {
          ndb_object->closeTransaction(tx);
          return RS_CLIENT_ERROR(ERROR_019);
        }
        memcpy(serving_key.prefix, prefix_attr_start, prefix_attr_bytes);
        serving_key.prefix[prefix_attr_bytes] = '\0';
      }

      // prefix
      if (join_on_attr->isNULL()) {
        serving_key.join_on[0] = '\0';
      } else {
        Uint32 join_on_attr_bytes;
        const char *join_on_attr_start = nullptr;

        if (GetByteArray(join_on_attr, &join_on_attr_start, &join_on_attr_bytes) != 0) {
          ndb_object->closeTransaction(tx);
          return RS_CLIENT_ERROR(ERROR_019);
        }
        memcpy(serving_key.join_on, join_on_attr_start, join_on_attr_bytes);
        serving_key.join_on[join_on_attr_bytes] = '\0';
      }

      serving_keys_vec.push_back(serving_key);
    } while ((check = scan_op->nextResult(false)) == 0);
  }

  // check for errors happened during the reading process
  NdbError error = scan_op->getNdbError();

  // As we are at the end we will first close the transaction and then deal with
  // the error
  ndb_object->closeTransaction(tx);

  // storage/ndb/src/ndbapi/ndberror.cpp
  if (error.code != 4120 /*Scan already complete*/) {
    return RS_RONDB_SERVER_ERROR(error, "Failed Reading Serving Key. Fn find_serving_key_data_int");
  }

  if (serving_keys_vec.size() == 0) {
    return RS_CLIENT_404_ERROR();
  }

  // freed by CGO
  *sk_size      = serving_keys_vec.size();
  void *ptr     = (Serving_Key *)malloc(serving_keys_vec.size() * sizeof(Serving_Key));
  *serving_keys = (Serving_Key *)ptr;
  for (Uint64 i = 0; i < serving_keys_vec.size(); i++) {
    (*serving_keys + i)->feature_group_id = serving_keys_vec[i].feature_group_id;
    (*serving_keys + i)->required         = serving_keys_vec[i].required;
    (*serving_keys + i)->join_index       = serving_keys_vec[i].join_index;
    memcpy((*serving_keys + i)->feature_name, serving_keys_vec[i].feature_name,
           strlen(serving_keys_vec[i].feature_name) + 1);  // +1 for '\0'
    memcpy((*serving_keys + i)->prefix, serving_keys_vec[i].prefix,
           strlen(serving_keys_vec[i].prefix) + 1);  // +1 for '\0'
    memcpy((*serving_keys + i)->join_on, serving_keys_vec[i].join_on,
           strlen(serving_keys_vec[i].join_on) + 1);  // +1 for '\0'
  }
  serving_keys_vec.clear();

  return RS_OK;
}

//-------------------------------------------------------------------------------------------------

/**
 * Find serving_key_data
 * SELECT * from serving_key  WHERE feature_view_id = {feature_view_id}
 */
RS_Status find_serving_key_data(int feature_view_id, Serving_Key **serving_keys, int *sk_size) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnection->GetNdbObject(&ndb_object);
  if (status.http_code != SUCCESS) {
    return status;
  }

  /* clang-format off */
  RETRY_HANDLER(
    status = find_serving_key_data_int(ndb_object, feature_view_id, serving_keys, sk_size); 
  )
  /* clang-format on */

  rdrsRonDBConnection->ReturnNDBObjectToPool(ndb_object, &status);

  return status;
}

//-------------------------------------------------------------------------------------------------
