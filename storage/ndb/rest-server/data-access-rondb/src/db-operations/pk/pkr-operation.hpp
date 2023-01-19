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
#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_DB_OPERATIONS_PK_PKR_OPERATION_HPP_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_DB_OPERATIONS_PK_PKR_OPERATION_HPP_

#include <stdint.h>
#include <string>
#include <unordered_map>
#include <vector>
#include <NdbApi.hpp>
#include "src/db-operations/pk/pkr-request.hpp"
#include "src/db-operations/pk/pkr-response.hpp"
#include "src/rdrs-dal.h"

class PKROperation {
 private:
  Uint32 no_ops;
  NdbTransaction *transaction = nullptr;
  Ndb *ndb_object             = nullptr;
  bool isBatch                = false;

  std::vector<PKRRequest *> requests;
  std::vector<PKRResponse *> responses;
  std::vector<NdbOperation *> operations;
  std::vector<std::vector<NdbRecAttr *>> all_recs;  // records that will be read from DB
  std::vector<const NdbDictionary::Table *> all_table_dicts;
  std::vector<std::unordered_map<std::string, const NdbDictionary::Column *>> all_non_pk_cols;
  std::vector<std::unordered_map<std::string, const NdbDictionary::Column *>> all_pk_cols;

 public:
  PKROperation(RS_Buffer *req_buff, RS_Buffer *resp_buff, Ndb *ndb_object);

  PKROperation(Uint32 noOps, RS_Buffer *req_buffs, RS_Buffer *resp_buffs, Ndb *ndb_object);

  ~PKROperation();

  /**
   * perform the operation
   */
  RS_Status PerformOperation();

 private:
  /**
   * start a transaction
   *
   * @return status
   */
  RS_Status SetupTransaction();

  /**
   * setup pk read operation
   * @returns status
   */
  RS_Status SetupReadOperation();

  /**
   * Set primary key column values
   * @returns status
   */
  RS_Status SetOperationPKCols();

  /**
   * Execute transaction
   *
   * @return status
   */
  RS_Status Execute();

  /**
   * Close transaction
   */
  void CloseTransaction();

  /**
   * abort operation
   */
  RS_Status Abort();

  /**
   * create response
   *
   * @return status
   */
  RS_Status CreateResponse();

  /**
   * initialize data structures
   * @return status
   */
  RS_Status Init();

  /**
   * Validate request
   * @return status
   */
  RS_Status ValidateRequest();

  /**
   * Append operation records to response buffer
   * @return status
   */
  RS_Status AppendOpRecs(PKRResponse *resp, std::vector<NdbRecAttr *> *recs);
};
#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_DB_OPERATIONS_PK_PKR_OPERATION_HPP_
