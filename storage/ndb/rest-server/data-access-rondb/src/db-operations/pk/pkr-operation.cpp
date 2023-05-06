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
  this->requests.push_back(new PKRRequest(req_buff));
  this->responses.push_back(new PKRResponse(resp_buff));
  this->ndb_object = ndb_object;
  this->no_ops     = 1;
  this->isBatch    = false;
}

PKROperation::PKROperation(Uint32 no_ops, RS_Buffer *req_buffs, RS_Buffer *resp_buffs,
                           Ndb *ndb_object) {
  this->no_ops = no_ops;
  for (Uint32 i = 0; i < no_ops; i++) {
    this->requests.push_back(new PKRRequest(&req_buffs[i]));
    this->responses.push_back(new PKRResponse(&resp_buffs[i]));
  }
  this->ndb_object = ndb_object;
  this->isBatch    = true;
}

PKROperation::~PKROperation() {
  for (size_t i = 0; i < no_ops; i++) {
    delete requests[i];
  }

  for (size_t i = 0; i < responses.size(); i++) {
    delete responses[i];
  }
}

/**
 * start a transaction
 *
 * @return status
 */

RS_Status PKROperation::SetupTransaction() {
  const NdbDictionary::Table *table_dict = all_table_dicts[0];
  transaction                            = ndb_object->startTransaction(table_dict);
  if (transaction == nullptr) {
    return RS_RONDB_SERVER_ERROR(ndb_object->getNdbError(), ERROR_005);
  }
  return RS_OK;
}

/**
 * Set up read operation
 *
 * @return status
 */
RS_Status PKROperation::SetupReadOperation() {
  if (operations.size() != 0) {
    return RS_CLIENT_ERROR(ERROR_006);
  }

  for (size_t i = 0; i < no_ops; i++) {
    PKRRequest *req                        = requests[i];
    const NdbDictionary::Table *table_dict = all_table_dicts[i];
    NdbOperation *op                       = transaction->getNdbOperation(table_dict);
    if (op == nullptr) {
      return RS_RONDB_SERVER_ERROR(transaction->getNdbError(), ERROR_007);
    } else {
      operations.push_back(op);
    }

    if (op->readTuple(NdbOperation::LM_CommittedRead) != 0) {
      return RS_SERVER_ERROR(ERROR_022);
    }

    for (Uint32 i = 0; i < req->PKColumnsCount(); i++) {
      RS_Status status = SetOperationPKCol(table_dict->getColumn(req->PKName(i)), op, req, i);
      if (status.http_code != SUCCESS) {
        return status;
      }
    }

    std::vector<NdbRecAttr *> recs;
    if (req->ReadColumnsCount() > 0) {
      for (Uint32 i = 0; i < req->ReadColumnsCount(); i++) {
        NdbRecAttr *rec = op->getValue(req->ReadColumnName(i), nullptr);
        recs.push_back(rec);
      }
    } else {
      std::unordered_map<std::string, const NdbDictionary::Column *> non_pk_cols =
          all_non_pk_cols[i];
      std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator it =
          non_pk_cols.begin();
      while (it != non_pk_cols.end()) {
        NdbRecAttr *rec = op->getValue(it->first.c_str(), nullptr);
        it++;
        recs.push_back(rec);
      }
    }
    all_recs.push_back(recs);
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
  for (size_t i = 0; i < no_ops; i++) {
    PKRRequest *req                = requests[i];
    PKRResponse *resp              = responses[i];
    const NdbOperation *op         = operations[i];
    std::vector<NdbRecAttr *> recs = all_recs[i];

    found = true;
    if (op->getNdbError().classification == NdbError::NoError) {
      resp->SetStatus(SUCCESS);
    } else if (op->getNdbError().classification == NdbError::NoDataFound) {
      found = false;
      resp->SetStatus(NOT_FOUND);
    } else {
      // immediately fail the entire batch
      return RS_RONDB_SERVER_ERROR(op->getNdbError(), std::string("SubOperation ") +
                                                          std::string(req->OperationId()) +
                                                          std::string(" failed"));
    }

    resp->SetDB(req->DB());
    resp->SetTable(req->Table());
    resp->SetOperationID(req->OperationId());
    resp->SetNoOfColumns(recs.size());

    if (found) {
      // iterate over all columns
      RS_Status ret = AppendOpRecs(resp, &recs);
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
  for (size_t i = 0; i < no_ops; i++) {
    PKRRequest *req = requests[i];
    std::unordered_map<std::string, const NdbDictionary::Column *> pk_cols;
    std::unordered_map<std::string, const NdbDictionary::Column *> non_pk_cols;
    if (ndb_object->setCatalogName(req->DB()) != 0) {
      return RS_CLIENT_404_WITH_MSG_ERROR(ERROR_011 + std::string(" Database: ") +
                                          std::string(req->DB()) + " Table: " + req->Table());
    }
    const NdbDictionary::Dictionary *dict  = ndb_object->getDictionary();
    const NdbDictionary::Table *table_dict = dict->getTable(req->Table());

    if (table_dict == nullptr) {
      return RS_CLIENT_404_WITH_MSG_ERROR(ERROR_011 + std::string(" Database: ") +
                                          std::string(req->DB()) + " Table: " + req->Table());
    }
    all_table_dicts.push_back(table_dict);

    // get all primary key columnns
    for (int i = 0; i < table_dict->getNoOfPrimaryKeys(); i++) {
      const char *priName           = table_dict->getPrimaryKey(i);
      pk_cols[std::string(priName)] = table_dict->getColumn(priName);
    }

    // get all non primary key columnns
    for (int i = 0; i < table_dict->getNoOfColumns(); i++) {
      const NdbDictionary::Column *col = table_dict->getColumn(i);
      std::string colNameStr(col->getName());
      std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator got =
          pk_cols.find(colNameStr);
      if (got == pk_cols.end()) {  // not found
        non_pk_cols[std::string(col->getName())] = table_dict->getColumn(col->getName());
      }
    }
    all_non_pk_cols.push_back(non_pk_cols);
    all_pk_cols.push_back(pk_cols);
  }
  return RS_OK;
}

RS_Status PKROperation::ValidateRequest() {
  // Check primary key columns

  for (size_t i = 0; i < no_ops; i++) {
    PKRRequest *req                                                            = requests[i];
    std::unordered_map<std::string, const NdbDictionary::Column *> pk_cols     = all_pk_cols[i];
    std::unordered_map<std::string, const NdbDictionary::Column *> non_pk_cols = all_non_pk_cols[i];
    const NdbDictionary::Table *table_dict                                     = all_table_dicts[i];

    if (req->PKColumnsCount() != pk_cols.size()) {
      return RS_CLIENT_ERROR(ERROR_013 + std::string(" Expecting: ") +
                             std::to_string(pk_cols.size()) +
                             " Got: " + std::to_string(req->PKColumnsCount()));
    }

    for (Uint32 i = 0; i < req->PKColumnsCount(); i++) {
      std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator got =
          pk_cols.find(std::string(req->PKName(i)));
      if (got == pk_cols.end()) {  // not found
        return RS_CLIENT_ERROR(ERROR_014 + std::string(" Column: ") + std::string(req->PKName(i)));
      }
    }

    // Check non primary key columns
    // check that all columns exist
    // check that data return type is supported
    // check for reading blob columns
    if (req->ReadColumnsCount() > 0) {
      for (Uint32 i = 0; i < req->ReadColumnsCount(); i++) {
        std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator got =
            non_pk_cols.find(std::string(req->ReadColumnName(i)));
        if (got == non_pk_cols.end()) {  // not found
          return RS_CLIENT_ERROR(ERROR_012 + std::string(" Column: ") +
                                 std::string(req->ReadColumnName(i)));
        }

        // check that the data return type is supported
        // for now we only support DataReturnType.DEFAULT
        if (req->ReadColumnReturnType(i) > __MAX_TYPE_NOT_A_DRT ||
            DEFAULT_DRT != req->ReadColumnReturnType(i)) {
          return RS_SERVER_ERROR(ERROR_025 + std::string(" Column: ") +
                                 std::string(req->ReadColumnName(i)));
        }

        if (table_dict->getColumn(req->ReadColumnName(i))->getType() ==
                NdbDictionary::Column::Blob ||
            table_dict->getColumn(req->ReadColumnName(i))->getType() ==
                NdbDictionary::Column::Text) {
          return RS_SERVER_ERROR(ERROR_026 + std::string(" Column: ") +
                                 std::string(req->ReadColumnName(i)));
        }
      }
    } else {
      // user wants to read all columns. make sure that we are not reading Blobs
      std::unordered_map<std::string, const NdbDictionary::Column *>::const_iterator it =
          non_pk_cols.begin();
      while (it != non_pk_cols.end()) {
        NdbDictionary::Column::Type type = it->second->getType();
        if (type == NdbDictionary::Column::Blob || type == NdbDictionary::Column::Text) {
          return RS_SERVER_ERROR(ERROR_026 + std::string(" Column: ") + it->first);
        }
        it++;
      }
    }
  }

  return RS_OK;
}

void PKROperation::CloseTransaction() {
  ndb_object->closeTransaction(transaction);
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
    ndb_object->closeTransaction(transaction);
  }

  return RS_OK;
}

RS_Status PKROperation::HandleNDBError(RS_Status status) {
  if (UnloadSchema(status)) {
    // no idea which sub-operation threw the error
    // unload all tables used in this operation
    for (size_t i = 0; i < no_ops; i++) {
      PKRRequest *req = requests[i];
      ndb_object->setCatalogName(req->DB());
      NdbDictionary::Dictionary *dict = ndb_object->getDictionary();
      dict->invalidateTable(req->Table());
      dict->removeCachedTable(req->Table());
      LOG_INFO("Unloading schema " + std::string(req->DB()) + "/" + std::string(req->Table()));
    }
  }

  this->Abort();

  return RS_OK;
}
