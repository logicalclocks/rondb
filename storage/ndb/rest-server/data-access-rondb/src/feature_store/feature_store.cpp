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
    err = ndb_object->getNdbError();
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

  // printf("feature store name is %s", feature_store_name, project_id);
  return status;
}
