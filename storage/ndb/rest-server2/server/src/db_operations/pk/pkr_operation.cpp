/*
 * Copyright (C) 2023, 2024 Hopsworks AB
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

#include "pkr_operation.hpp"
#include "NdbBlob.hpp"
#include "NdbOperation.hpp"
#include "NdbRecAttr.hpp"
#include "NdbTransaction.hpp"
#include "src/db_operations/pk/common.hpp"
#include "src/db_operations/pk/pkr_request.hpp"
#include "src/db_operations/pk/pkr_response.hpp"
#include "src/error_strings.h"
#include "src/logger.hpp"
#include "src/rdrs_const.h"
#include "src/status.hpp"
#include "src/mystring.hpp"
#include "my_compiler.h"

#include <memory>
#include <mysql_time.h>
#include <algorithm>
#include <tuple>
#include <utility>
#include <my_base.h>
#include <storage/ndb/include/ndbapi/NdbDictionary.hpp>

PKROperation::PKROperation(RS_Buffer *reqBuff,
                           RS_Buffer *respBuff,
                           Ndb *ndbObject) {
  SubOpTuple pkOpTuple = SubOpTuple{};
  pkOpTuple.pkRequest = new PKRRequest(reqBuff);
  pkOpTuple.pkResponse = new PKRResponse(respBuff);
  pkOpTuple.ndbOperation = nullptr;
  pkOpTuple.tableDict = nullptr;
  pkOpTuple.primaryKeysCols = nullptr;
  pkOpTuple.primaryKeySizes = nullptr;
  this->subOpTuples.push_back(pkOpTuple);
  this->ndbObject = ndbObject;
  this->noOps = 1;
  this->isBatch = false;
}

PKROperation::PKROperation(Uint32 noOps,
                           RS_Buffer *reqBuffs,
                           RS_Buffer *respBuffs,
                           Ndb *ndbObject) {
  for (Uint32 i = 0; i < noOps; i++) {
    SubOpTuple pkOpTuple = SubOpTuple{};
    pkOpTuple.pkRequest = new PKRRequest(&reqBuffs[i]);
    pkOpTuple.pkResponse = new PKRResponse(&respBuffs[i]);
    pkOpTuple.ndbOperation = nullptr;
    pkOpTuple.tableDict = nullptr;
    pkOpTuple.primaryKeysCols = nullptr;
    pkOpTuple.primaryKeySizes = nullptr;
    this->subOpTuples.push_back(pkOpTuple);
  }
  this->ndbObject = ndbObject;
  this->noOps     = noOps;
  this->isBatch   = true;
}

PKROperation::~PKROperation() {
  for (size_t subOpIdx = 0; subOpIdx < subOpTuples.size(); subOpIdx++) {
    SubOpTuple subOp = subOpTuples[subOpIdx];
    int pkColsCount = subOp.pkRequest->PKColumnsCount();
    if (subOp.primaryKeysCols != nullptr) {
      for (int pkPtrIdx = 0; pkPtrIdx < pkColsCount; pkPtrIdx++) {
        if (subOp.primaryKeysCols[pkPtrIdx] == nullptr) {
          break;
        } else {
          free(subOp.primaryKeysCols[pkPtrIdx]);
        }
      }
      free(subOp.primaryKeysCols);
    }
    if (subOp.primaryKeySizes != nullptr) {
      free(subOp.primaryKeySizes);
    }
    // clear the ColRec Vector
    subOp.recs.clear();
    delete subOp.pkRequest;
    delete subOp.pkResponse;
  }
}

/**
 * start a transaction
 *
 * @return status
 */

RS_Status PKROperation::SetupTransaction() {
  const NdbDictionary::Table *table_dict = subOpTuples[0].tableDict;
  transaction = ndbObject->startTransaction(table_dict);
  if (unlikely(transaction == nullptr)) {
    return RS_RONDB_SERVER_ERROR(ndbObject->getNdbError(), ERROR_005);
  }
  return RS_OK;
}

/**
 * Set up read operation
 *
 * @return status
 */
RS_Status PKROperation::SetupReadOperation() {

start:
  for (size_t opIdx = 0; opIdx < noOps; opIdx++) {
    // this sub operation can not be processed
    if (unlikely(subOpTuples[opIdx].pkRequest->IsInvalidOp())) {
      continue;
    }
    PKRRequest *req = subOpTuples[opIdx].pkRequest;
    const NdbDictionary::Table *tableDict = subOpTuples[opIdx].tableDict;
    std::vector<std::shared_ptr<ColRec>> *recs = &subOpTuples[opIdx].recs;
    // cleaned by destructor
    Int8 **primaryKeysCols =
      (Int8 **)malloc(req->PKColumnsCount() * sizeof(Int8 *));
    Uint32 *primaryKeySizes =
      (Uint32 *)malloc(req->PKColumnsCount() * sizeof(Uint32));
    memset(primaryKeysCols, 0, req->PKColumnsCount() * sizeof(Int8 *));
    memset(primaryKeySizes, 0, req->PKColumnsCount() * sizeof(Uint32));
    subOpTuples[opIdx].primaryKeysCols = primaryKeysCols;
    subOpTuples[opIdx].primaryKeySizes = primaryKeySizes;
    for (Uint32 colIdx = 0; colIdx < req->PKColumnsCount(); colIdx++) {
      RS_Status status =
        SetOperationPKCol(tableDict->getColumn(req->PKName(colIdx)),
                          req,
                          colIdx,
                          &primaryKeysCols[colIdx],
                          &primaryKeySizes[colIdx]);
      if (status.http_code != SUCCESS) {
        if (isBatch) {
          subOpTuples[opIdx].pkRequest->MarkInvalidOp(status);
          goto start;
        } else {
          return status;
        }
      }
    }
    NdbOperation *operation = transaction->getNdbOperation(tableDict);
    if (unlikely(operation == nullptr)) {
      return RS_RONDB_SERVER_ERROR(transaction->getNdbError(), ERROR_007);
    }
    subOpTuples[opIdx].ndbOperation = operation;
    if (unlikely(operation->readTuple(NdbOperation::LM_CommittedRead) != 0)) {
      return RS_SERVER_ERROR(ERROR_022);
    }
    for (Uint32 colIdx = 0; colIdx < req->PKColumnsCount(); colIdx++) {
      int retVal =
        operation->equal(req->PKName(colIdx),
        (char *)primaryKeysCols[colIdx],
        primaryKeySizes[colIdx]);
      if (unlikely(retVal != 0)) {
        return RS_SERVER_ERROR(ERROR_023);
      }
    }
    if (req->ReadColumnsCount() > 0) {
      for (Uint32 i = 0; i < req->ReadColumnsCount(); i++) {
        RS_Status status = GetColValue(tableDict,
                                       operation,
                                       req->ReadColumnName(i),
                                       recs);
        if (unlikely(status.http_code != SUCCESS)) {
          return status;
        }
      }
    } else {
      std::unordered_map<std::string,
                         const NdbDictionary::Column *> *nonPKCols =
        &subOpTuples[opIdx].allNonPKCols;
      std::unordered_map<std::string,
                         const NdbDictionary::Column *>::const_iterator it =
        nonPKCols->begin();
      while (it != nonPKCols->end()) {
        RS_Status status = GetColValue(tableDict,
                                       operation,
                                       it->first.c_str(),
                                       recs);
        if (unlikely(status.http_code != SUCCESS)) {
          return status;
        }
        it++;
      }
    }
  }
  return RS_OK;
}

RS_Status PKROperation::GetColValue(const NdbDictionary::Table *tableDict,
                                    NdbOperation *ndbOperation,
                                    const char *colName,
                                    std::vector<std::shared_ptr<ColRec>> *recs) {
  NdbBlob *blob = nullptr;
  NdbRecAttr *ndbRecAttr = nullptr;
  if (tableDict->getColumn(colName)->getType() == NdbDictionary::Column::Blob ||
      tableDict->getColumn(colName)->getType() == NdbDictionary::Column::Text) {
    blob = ndbOperation->getBlobHandle(colName);
    // failed to read blob column
    if (unlikely(blob == nullptr)) {
      return RS_SERVER_ERROR(
        ERROR_037 + std::string(" Column: ") + std::string(colName));
    }
  }
  ndbRecAttr = ndbOperation->getValue(colName, nullptr);
  if (unlikely(ndbRecAttr == nullptr)) {
    return RS_SERVER_ERROR(
      ERROR_037 + std::string(" Column: ") + std::string(colName));
  }
  auto colRec = std::make_shared<ColRec>(ndbRecAttr, blob);
  recs->push_back(colRec);
  return RS_OK;
}

RS_Status PKROperation::Execute() {
  if (unlikely(transaction->execute(NdbTransaction::NoCommit) != 0)) {
    return RS_RONDB_SERVER_ERROR(transaction->getNdbError(), ERROR_009);
  }
  return RS_OK;
}

RS_Status PKROperation::CreateResponse() {
  bool found = true;
  for (size_t i = 0; i < noOps; i++) {
    PKRRequest *req = subOpTuples[i].pkRequest;
    PKRResponse *resp = subOpTuples[i].pkResponse;
    const NdbOperation *op = subOpTuples[i].ndbOperation;
    std::vector<std::shared_ptr<ColRec>> *recs = &subOpTuples[i].recs;
    resp->SetDB(req->DB());
    resp->SetTable(req->Table());
    resp->SetOperationID(req->OperationId());
    resp->SetNoOfColumns(recs->size());
    if (unlikely(req->IsInvalidOp())) {
      resp->SetStatus(req->GetError().http_code, req->GetError().message);
      resp->Close();
      continue;
    }
    found = true;
    if (likely(op->getNdbError().classification == NdbError::NoError)) {
      resp->SetStatus(SUCCESS, "OK");
    } else if (op->getNdbError().classification == NdbError::NoDataFound) {
      found = false;
      resp->SetStatus(NOT_FOUND, "NOT Found");
    } else {      
      //  immediately fail the entire batch
      resp->SetStatus(SERVER_ERROR, op->getNdbError().message);
      resp->Close();
      return RS_RONDB_SERVER_ERROR(
        op->getNdbError(), std::string("SubOperation ") +
        std::string(req->OperationId()) +
        std::string(" failed"));
    }
    if (likely(found)) {
      // iterate over all columns
      RS_Status ret = AppendOpRecs(resp, recs);
      if (ret.http_code != SUCCESS) {
        return ret;
      }
    }
    resp->Close();
  }
  if (unlikely(!found && !isBatch)) {
    return RS_CLIENT_404_ERROR();
  }
  return RS_OK;
}

RS_Status PKROperation::AppendOpRecs(
  PKRResponse *resp,
  std::vector<std::shared_ptr<ColRec>> *recs) {
  for (Uint32 i = 0; i < recs->size(); i++) {
    RS_Status status = WriteColToRespBuff((*recs)[i], resp);
    if (unlikely(status.http_code != SUCCESS)) {
      return status;
    }
  }
  return RS_OK;
}

RS_Status PKROperation::Init() {
  for (size_t i = 0; i < noOps; i++) {
    PKRRequest *req = subOpTuples[i].pkRequest;
    std::unordered_map<std::string, const NdbDictionary::Column *> *pkCols =
      &subOpTuples[i].allPKCols;
    std::unordered_map<std::string, const NdbDictionary::Column *> *nonPKCols =
      &subOpTuples[i].allNonPKCols;
    if (unlikely(ndbObject->setCatalogName(req->DB()) != 0)) {
      RS_Status error =
          RS_CLIENT_404_WITH_MSG_ERROR(
            ERROR_011 + std::string(" Database: ") +
            std::string(req->DB()) + " Table: " + req->Table());
      if (isBatch) {  // ignore this sub-operation and continue with the rest
        req->MarkInvalidOp(error);
        continue;
      } else {
        return error;
      }
    }
    const NdbDictionary::Dictionary *dict = ndbObject->getDictionary();
    const NdbDictionary::Table *tableDict = dict->getTable(req->Table());
    if (unlikely(tableDict == nullptr)) {
      RS_Status error =
          RS_CLIENT_404_WITH_MSG_ERROR(
            ERROR_011 + std::string(" Database: ") +
            std::string(req->DB()) + " Table: " + req->Table());
      if (isBatch) {  // ignore this sub-operation and continue with the rest
        req->MarkInvalidOp(error);
        continue;
      } else {
        return error;
      }
    }
    subOpTuples[i].tableDict = tableDict;
    // get all primary key columnns
    for (int i = 0; i < tableDict->getNoOfPrimaryKeys(); i++) {
      const char *priName = tableDict->getPrimaryKey(i);
      (*pkCols)[std::string(priName)] = tableDict->getColumn(priName);
    }
    // get all non primary key columnns
    for (int i = 0; i < tableDict->getNoOfColumns(); i++) {
      const NdbDictionary::Column *col = tableDict->getColumn(i);
      std::string colNameStr(col->getName());
      std::unordered_map<std::string,
                         const NdbDictionary::Column *>::const_iterator got =
        (*pkCols).find(colNameStr);
      if (got == pkCols->end()) {  // not found
        (*nonPKCols)[std::string(col->getName())] =
          tableDict->getColumn(col->getName());
      }
    }
  }
  return RS_OK;
}

RS_Status PKROperation::ValidateRequest() {
  // Check primary key columns
  for (size_t i = 0; i < noOps; i++) {
    PKRRequest *req = subOpTuples[i].pkRequest;
    if (unlikely(req->IsInvalidOp())) {
      // this sub-operation was previously marked invalid.
      continue;
    }
    std::unordered_map<std::string, const NdbDictionary::Column *> *pkCols =
      &subOpTuples[i].allPKCols;
    std::unordered_map<std::string, const NdbDictionary::Column *> *nonPKCols =
      &subOpTuples[i].allNonPKCols;
    if (unlikely(req->PKColumnsCount() != pkCols->size())) {
      RS_Status error =
          RS_CLIENT_ERROR(
            ERROR_013 + std::string(" Expecting: ") +
            std::to_string(pkCols->size()) +
            " Got: " + std::to_string(req->PKColumnsCount()));
      if (isBatch) {  // mark bad sub-operation
        req->MarkInvalidOp(error);
        continue;
      }
      return error;
    }
    for (Uint32 i = 0; i < req->PKColumnsCount(); i++) {
      std::unordered_map<std::string,
                         const NdbDictionary::Column *>::const_iterator got =
        pkCols->find(std::string(req->PKName(i)));
      // not found
      if (unlikely(got == pkCols->end())) {
        RS_Status error =
            RS_CLIENT_ERROR(
              ERROR_014 + std::string(" Column: ") +
              std::string(req->PKName(i)));
        if (isBatch) {  // mark bad sub-operation
          req->MarkInvalidOp(error);
          continue;
        }
        return error;
      }
    }
    // Check non primary key columns
    // check that all columns exist
    // check that data return type is supported
    // check for reading blob columns
    if (req->ReadColumnsCount() > 0) {
      for (Uint32 i = 0; i < req->ReadColumnsCount(); i++) {
        std::unordered_map<std::string,
                           const NdbDictionary::Column *>::const_iterator got =
          nonPKCols->find(std::string(req->ReadColumnName(i)));
        // not found
        if (unlikely(got == nonPKCols->end())) {
          RS_Status error = RS_CLIENT_ERROR(
            ERROR_012 + std::string(" Column: ") +
            std::string(req->ReadColumnName(i)));
          if (isBatch) {  // mark bad sub-operation
            req->MarkInvalidOp(error);
            continue;
          }
          return error;
        }
        // check that the data return type is supported
        // for now we only support DataReturnType.DEFAULT
        if (unlikely(req->ReadColumnReturnType(i) > __MAX_TYPE_NOT_A_DRT ||
            DEFAULT_DRT != req->ReadColumnReturnType(i))) {
          RS_Status error = RS_SERVER_ERROR(
            ERROR_025 + std::string(" Column: ") +
            std::string(req->ReadColumnName(i)));
          if (isBatch) {  // mark bad sub-operation
            req->MarkInvalidOp(error);
            continue;
          }
          return error;
        }
      }
    }
  }
  return RS_OK;
}

void PKROperation::CloseTransaction() {
  ndbObject->closeTransaction(transaction);
}

RS_Status PKROperation::PerformOperation() {
  RS_Status status = Init();
  if (unlikely(status.http_code != SUCCESS)) {
    this->HandleNDBError(status);
    return status;
  }
  status = ValidateRequest();
  if (unlikely(status.http_code != SUCCESS)) {
    this->HandleNDBError(status);
    return status;
  }
  status = SetupTransaction();
  if (unlikely(status.http_code != SUCCESS)) {
    this->HandleNDBError(status);
    return status;
  }
  status = SetupReadOperation();
  if (unlikely(status.http_code != SUCCESS)) {
    this->HandleNDBError(status);
    return status;
  }
  status = Execute();
  if (unlikely(status.http_code != SUCCESS)) {
    this->HandleNDBError(status);
    return status;
  }
  status = CreateResponse();
  if (unlikely(status.http_code != SUCCESS)) {
    this->HandleNDBError(status);
    return status;
  }
  CloseTransaction();
  return RS_OK;
}

RS_Status PKROperation::Abort() {
  if (likely(transaction != nullptr)) {
    NdbTransaction::CommitStatusType status = transaction->commitStatus();
    if (status == NdbTransaction::CommitStatusType::Started) {
      transaction->execute(NdbTransaction::Rollback);
    }
    ndbObject->closeTransaction(transaction);
  }
  return RS_OK;
}

RS_Status PKROperation::HandleNDBError(RS_Status status) {
  // schema errors
  if (UnloadSchema(status)) {
    // no idea which sub-operation threw the error
    // unload all tables used in this operation
    std::list<std::tuple<std::string, std::string>> tables;
    std::unordered_map<std::string, bool> tablesMap;
    for (size_t i = 0; i < noOps; i++) {
      PKRRequest *req = subOpTuples[i].pkRequest;
      const char *db = req->DB();
      const char *table = req->Table();
      std::string key(std::string(db) + "|" + std::string(table));
      if (tablesMap.count(key) == 0) {
        tables.push_back(std::make_tuple(std::string(db), std::string(table)));
        tablesMap[key] = true;
      }
    }
    HandleSchemaErrors(ndbObject, status, tables);
  }
  this->Abort();
  return RS_OK;
}
