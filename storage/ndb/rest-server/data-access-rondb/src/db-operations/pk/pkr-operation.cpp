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

#include "src/db-operations/pk/pkr-operation.hpp"
#include <mysql_time.h>
#include <algorithm>
#include <utility>
#include <my_base.h>
#include <storage/ndb/include/ndbapi/NdbDictionary.hpp>
#include "NdbTransaction.hpp"
#include "src/db-operations/pk/common.hpp"
#include "src/db-operations/pk/pkr-request.hpp"
#include "src/db-operations/pk/pkr-response.hpp"
#include "src/db-operations/pk/common.hpp"
#include "src/error-strings.h"
#include "src/logger.hpp"
#include "src/rdrs-const.h"
#include "src/status.hpp"
#include "src/mystring.hpp"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/beast/core/detail/base64.hpp>

PKROperation::PKROperation(RS_Buffer *req_buff, RS_Buffer *resp_buff, Ndb *ndb_object) {
  SubOpTuple pkOpTuple   = SubOpTuple{};
  pkOpTuple.pkRequest    = new PKRRequest(req_buff);
  pkOpTuple.pkResponse   = new PKRResponse(resp_buff);
  pkOpTuple.ndbOperation = nullptr;
  pkOpTuple.tableDict    = nullptr;
  this->subOpTuples.push_back(pkOpTuple);

  this->ndbObject = ndb_object;
  this->noOps     = 1;
  this->isBatch   = false;
}

PKROperation::PKROperation(Uint32 no_ops, RS_Buffer *req_buffs, RS_Buffer *resp_buffs,
                           Ndb *ndb_object) {
  for (Uint32 i = 0; i < no_ops; i++) {
    SubOpTuple pkOpTuple   = SubOpTuple{};
    pkOpTuple.pkRequest    = new PKRRequest(&req_buffs[i]);
    pkOpTuple.pkResponse   = new PKRResponse(&resp_buffs[i]);
    pkOpTuple.ndbOperation = nullptr;
    pkOpTuple.tableDict    = nullptr;
    this->subOpTuples.push_back(pkOpTuple);
  }

  this->ndbObject = ndb_object;
  this->noOps     = no_ops;
  this->isBatch   = true;
}

PKROperation::~PKROperation() {
  for (size_t i = 0; i < subOpTuples.size(); i++) {
    delete subOpTuples[i].pkRequest;
    delete subOpTuples[i].pkResponse;
  }
}

/**
 * start a transaction
 *
 * @return status
 */

RS_Status PKROperation::SetupTransaction() {
  const NdbDictionary::Table *table_dict = subOpTuples[0].tableDict;
  transaction                            = ndbObject->startTransaction(table_dict);
  if (transaction == nullptr) {
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

start:  for (size_t opIdx = 0; opIdx < noOps; opIdx++) {
    if (subOpTuples[opIdx].pkRequest->IsInvalidOp()) { // this sub operation can not be processed
      continue;
    }

    PKRRequest *req                        = subOpTuples[opIdx].pkRequest;
    const NdbDictionary::Table *tableDict = subOpTuples[opIdx].tableDict;
    std::vector<NdbRecAttr *> *recs        = &subOpTuples[opIdx].recs;

    NdbOperation *op = nullptr;
    for (Uint32 colIdx = 0; colIdx < req->PKColumnsCount(); colIdx++) {
      RS_Status status = SetOperationPKCol(tableDict->getColumn(req->PKName(colIdx)), transaction,
                                           tableDict, req, colIdx, &op);
      if (status.http_code != SUCCESS) {
        if (isBatch) {
          subOpTuples[opIdx].pkRequest->MarkInvalidOp(status);
          goto start;
        } else {
          return status;
        }
      }
      subOpTuples[opIdx].ndbOperation = op;
    }

    if (req->ReadColumnsCount() > 0) {
      for (Uint32 i = 0; i < req->ReadColumnsCount(); i++) {
        NdbRecAttr *rec = op->getValue(req->ReadColumnName(i), nullptr);
        recs->push_back(rec);
      }
    } else {
      std::unordered_map<std::string, const NdbDictionary::Column *> *nonPKCols =
          &subOpTuples[opIdx].allNonPKCols;
      std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator it =
          nonPKCols->begin();
      while (it != nonPKCols->end()) {
        NdbRecAttr *rec = op->getValue(it->first.c_str(), nullptr);
        it++;
        recs->push_back(rec);
      }
    }
  }

  return RS_OK;
}

RS_Status PKROperation::Execute() {
  if (transaction->execute(NdbTransaction::Commit) != 0) {
    return RS_RONDB_SERVER_ERROR(transaction->getNdbError(), ERROR_009);
  }

  return RS_OK;
}

RS_Status PKROperation::CreateResponse() {
  bool found = true;
  for (size_t i = 0; i < noOps; i++) {
    PKRRequest *req                 = subOpTuples[i].pkRequest;
    PKRResponse *resp               = subOpTuples[i].pkResponse;
    const NdbOperation *op          = subOpTuples[i].ndbOperation;
    std::vector<NdbRecAttr *> *recs = &subOpTuples[i].recs;

    resp->SetDB(req->DB());
    resp->SetTable(req->Table());
    resp->SetOperationID(req->OperationId());
    resp->SetNoOfColumns(recs->size());

    if (req->IsInvalidOp()){
      resp->SetStatus(CLIENT_ERROR, req->GetError().message);
      resp->Close();
      continue;
    }

    found = true;
    if (op->getNdbError().classification == NdbError::NoError) {
      resp->SetStatus(SUCCESS, "OK");
    } else if (op->getNdbError().classification == NdbError::NoDataFound) {
      found = false;
      resp->SetStatus(NOT_FOUND, "NOT Found");
    } else {
      //  immediately fail the entire batch
      resp->SetStatus(SERVER_ERROR, op->getNdbError().message);
      resp->Close();
      return RS_RONDB_SERVER_ERROR(op->getNdbError(), std::string("SubOperation ") +
                                                          std::string(req->OperationId()) +
                                                          std::string(" failed"));
    }

    if (found) {
      // iterate over all columns
      RS_Status ret = AppendOpRecs(resp, recs);
      if (ret.http_code != SUCCESS) {
        return ret;
      }
    }
    resp->Close();
  }

  if (!found && !isBatch) {
    return RS_CLIENT_404_ERROR();
  }
  return RS_OK;
}

RS_Status PKROperation::AppendOpRecs(PKRResponse *resp, std::vector<NdbRecAttr *> *recs) {
  for (Uint32 i = 0; i < recs->size(); i++) {
    RS_Status status = WriteColToRespBuff((*recs)[i], resp);
    if (status.http_code != SUCCESS) {
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

    if (ndbObject->setCatalogName(req->DB()) != 0) {
      RS_Status error =
          RS_CLIENT_404_WITH_MSG_ERROR(ERROR_011 + std::string(" Database: ") +
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

    if (tableDict == nullptr) {
      RS_Status error =
          RS_CLIENT_404_WITH_MSG_ERROR(ERROR_011 + std::string(" Database: ") +
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
      const char *priName             = tableDict->getPrimaryKey(i);
      (*pkCols)[std::string(priName)] = tableDict->getColumn(priName);
    }

    // get all non primary key columnns
    for (int i = 0; i < tableDict->getNoOfColumns(); i++) {
      const NdbDictionary::Column *col = tableDict->getColumn(i);
      std::string colNameStr(col->getName());
      std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator got =
          (*pkCols).find(colNameStr);
      if (got == pkCols->end()) {  // not found
        (*nonPKCols)[std::string(col->getName())] = tableDict->getColumn(col->getName());
      }
    }
  }
  return RS_OK;
}

RS_Status PKROperation::ValidateRequest() {
  // Check primary key columns

  for (size_t i = 0; i < noOps; i++) {
    PKRRequest *req = subOpTuples[i].pkRequest;
    if (req->IsInvalidOp()) {
      // this sub-operation was previously marked invalid.
      continue;
    }

    std::unordered_map<std::string, const NdbDictionary::Column *> *pkCols =
        &subOpTuples[i].allPKCols;
    std::unordered_map<std::string, const NdbDictionary::Column *> *nonPKCols =
        &subOpTuples[i].allNonPKCols;
    const NdbDictionary::Table *table_dict = subOpTuples[i].tableDict;

    if (req->PKColumnsCount() != pkCols->size()) {
      RS_Status error =
          RS_CLIENT_ERROR(ERROR_013 + std::string(" Expecting: ") + std::to_string(pkCols->size()) +
                          " Got: " + std::to_string(req->PKColumnsCount()));
      if (isBatch) {  // mark bad sub-operation
        req->MarkInvalidOp(error);
        continue;
      } else {
        return error;
      }
    }

    for (Uint32 i = 0; i < req->PKColumnsCount(); i++) {
      std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator got =
          pkCols->find(std::string(req->PKName(i)));
      if (got == pkCols->end()) {  // not found
        RS_Status error =
            RS_CLIENT_ERROR(ERROR_014 + std::string(" Column: ") + std::string(req->PKName(i)));
        if (isBatch) {  // mark bad sub-operation
          req->MarkInvalidOp(error);
          continue;
        } else {
          return error;
        }
      }
    }

    // Check non primary key columns
    // check that all columns exist
    // check that data return type is supported
    // check for reading blob columns
    if (req->ReadColumnsCount() > 0) {
      for (Uint32 i = 0; i < req->ReadColumnsCount(); i++) {
        std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator got =
            nonPKCols->find(std::string(req->ReadColumnName(i)));
        if (got == nonPKCols->end()) {  // not found
          RS_Status error = RS_CLIENT_ERROR(ERROR_012 + std::string(" Column: ") +
                                            std::string(req->ReadColumnName(i)));
          if (isBatch) {  // mark bad sub-operation
            req->MarkInvalidOp(error);
            continue;
          } else {
            return error;
          }
        }

        // check that the data return type is supported
        // for now we only support DataReturnType.DEFAULT
        if (req->ReadColumnReturnType(i) > __MAX_TYPE_NOT_A_DRT ||
            DEFAULT_DRT != req->ReadColumnReturnType(i)) {
          RS_Status error = RS_SERVER_ERROR(ERROR_025 + std::string(" Column: ") +
                                            std::string(req->ReadColumnName(i)));
          if (isBatch) {  // mark bad sub-operation
            req->MarkInvalidOp(error);
            continue;
          } else {
            return error;
          }
        }

        if (table_dict->getColumn(req->ReadColumnName(i))->getType() ==
                NdbDictionary::Column::Blob ||
            table_dict->getColumn(req->ReadColumnName(i))->getType() ==
                NdbDictionary::Column::Text) {
          RS_Status error = RS_SERVER_ERROR(ERROR_026 + std::string(" Column: ") +
                                            std::string(req->ReadColumnName(i)));
          if (isBatch) {  // mark bad sub-operation
            req->MarkInvalidOp(error);
            continue;
          } else {
            return error;
          }
        }
      }
    } else {
      // user wants to read all columns. make sure that we are not reading Blobs
      std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator it =
          nonPKCols->begin();
      while (it != nonPKCols->end()) {
        NdbDictionary::Column::Type type = it->second->getType();
        if (type == NdbDictionary::Column::Blob || type == NdbDictionary::Column::Text) {
          RS_Status error = RS_SERVER_ERROR(ERROR_026 + std::string(" Column: ") + it->first);
          if (isBatch) {  // mark bad sub-operation
            req->MarkInvalidOp(error);
            continue;
          } else {
            return error;
          }
        }
        it++;
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
  if (status.http_code != SUCCESS) {
    this->HandleNDBError(status);
    return status;
  }

  status = ValidateRequest();
  if (status.http_code != SUCCESS) {
    this->HandleNDBError(status);
    return status;
  }

  status = SetupTransaction();
  if (status.http_code != SUCCESS) {
    this->HandleNDBError(status);
    return status;
  }

  status = SetupReadOperation();
  if (status.http_code != SUCCESS) {
    this->HandleNDBError(status);
    return status;
  }

  status = Execute();
  if (status.http_code != SUCCESS) {
    this->HandleNDBError(status);
    return status;
  }

  status = CreateResponse();
  if (status.http_code != SUCCESS) {
    this->HandleNDBError(status);
    return status;
  }

  CloseTransaction();
  return RS_OK;
}

RS_Status PKROperation::Abort() {
  if (transaction != nullptr) {
    NdbTransaction::CommitStatusType status = transaction->commitStatus();
    if (status == NdbTransaction::CommitStatusType::Started) {
      transaction->execute(NdbTransaction::Rollback);
    }
    ndbObject->closeTransaction(transaction);
  }

  return RS_OK;
}

RS_Status PKROperation::HandleNDBError(RS_Status status) {
  if (UnloadSchema(status)) {
    // no idea which sub-operation threw the error
    // unload all tables used in this operation
    for (size_t i = 0; i < noOps; i++) {
      PKRRequest *req = subOpTuples[i].pkRequest;
      ndbObject->setCatalogName(req->DB());
      NdbDictionary::Dictionary *dict = ndbObject->getDictionary();
      dict->invalidateTable(req->Table());
      dict->removeCachedTable(req->Table());
      LOG_INFO("Unloading schema " + std::string(req->DB()) + "/" + std::string(req->Table()));
    }
  }

  this->Abort();

  return RS_OK;
}
