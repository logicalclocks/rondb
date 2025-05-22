/*
 * Copyright (C) 2023, 2025 Hopsworks AB
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
#include "src/rdrs_dal.h"
#include "encoding.hpp"

#include <memory>
#include <mysql_time.h>
#include <algorithm>
#include <tuple>
#include <utility>
#include <my_base.h>
#include <storage/ndb/include/ndbapi/NdbDictionary.hpp>
#include <kernel/ndb_limits.h>
#include <ArenaMalloc.hpp>
#include <util/require.h>
#include "my_byteorder.h"
#include <decimal_utils.hpp>
#include <my_time.h>
#include <libbase64.h>
#include <EventLogger.hpp>

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_NDB_BE 1
//#define DEBUG_NDB_BE_ERR 1
#endif

#ifdef DEBUG_NDB_BE
#define DEB_NDB_BE(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_NDB_BE(...) do { } while (0)
#endif

#ifdef DEBUG_NDB_BE_ERR
#define DEB_NDB_BE_ERR(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_NDB_BE_ERR(...) do { } while (0)
#endif

BatchKeyOperations::BatchKeyOperations() {
}

BatchKeyOperations::~BatchKeyOperations() {
  if (!m_isSuccess) {
    for (Uint32 i = 0; i < m_numOperations; i++) {
      KeyOperation *key_op = &m_key_ops[i];
      PKRRequest *req = &key_op->m_req;
      req->resetReadColumns();
    }
  }
}

RS_Status
BatchKeyOperations::init_batch_operations(ArenaMalloc *amalloc,
                                          Uint32 numOps,
                                          bool is_batch,
                                          RS_Buffer *reqBuffer,
                                          Ndb *ndb_object) {
  RS_Status status = RS_OK;
  m_isSuccess = false;
  m_isBatch = is_batch;
  m_ndb_object = ndb_object;
  m_numOperations = numOps;
  m_num_sent_operations = 0;
  m_single_transaction = globalConfigs.rest.useSingleTransaction;
  m_key_ops = amalloc->alloc<KeyOperation>(numOps);
  if (unlikely(m_key_ops == nullptr)) {
    RS_Status error = RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
    return error;
  }
  DEB_NDB_BE("m_key_ops: %p, sizeof(KeyOperation): %u",
             m_key_ops, (Uint32)sizeof(KeyOperation));
  for (Uint32 i = 0; i < numOps; i++) {
    KeyOperation *key_op = &m_key_ops[i];
    key_op->m_ndbTransaction = nullptr;
    PKRRequest *req = new (&key_op->m_req) PKRRequest(&reqBuffer[i]);
    if (unlikely(ndb_object->setCatalogName(req->DB()) != 0)) {
      RS_Status err = RS_CLIENT_404_WITH_MSG_ERROR(
        std::string(rdrsErrorMessage(ERROR_DB_TABLE_NOT_EXIST)) +
        std::string(" Database: ") +
        std::string(req->DB()) + " Table: " + req->Table());
      if (m_isBatch) {
        req->MarkInvalidOp(err);
        continue;
      }
      return err;
    }
    const NdbDictionary::Dictionary *dict = ndb_object->getDictionary();
    const NdbDictionary::Table *tableDict = dict->getTable(req->Table());
    DEB_NDB_BE("Request on DB: %s, Table: %s, op: %u, reqBuffer: %p",
      req->DB(), req->Table(), i, reqBuffer[i].buffer);
    if (unlikely(tableDict == nullptr)) {
      RS_Status err = RS_CLIENT_404_WITH_MSG_ERROR(
        std::string(rdrsErrorMessage(ERROR_DB_TABLE_NOT_EXIST)) + 
        std::string(" Database: ") + std::string(req->DB()) + 
        std::string(" Table: ") + req->Table());
      if (m_isBatch) {
        req->MarkInvalidOp(err);
        continue;
      }
      return err;
    }
    key_op->m_tableDict = tableDict;
    Uint32 numPrimaryKeys = (Uint32)tableDict->getNoOfPrimaryKeys();
    Uint32 numColumns = (Uint32)tableDict->getNoOfColumns();
    Uint32 numReadColumns = req->ReadColumnsCount();
    const NdbRecord *ndb_record = tableDict->getDefaultRecord();
    key_op->m_ndb_record = ndb_record;
    key_op->m_num_pk_columns = numPrimaryKeys;
    key_op->m_num_table_columns = numColumns;
    key_op->m_num_read_columns = numReadColumns;
    key_op->m_blob_handles = nullptr;
    if (unlikely(numPrimaryKeys != req->PKColumnsCount())) {
      DEB_NDB_BE("numPrimaryKeys: %u, reqPKKeys: %u",
        numPrimaryKeys, req->PKColumnsCount());
      RS_Status err =
        RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_WRONG_PRIMARY_KEY_COUNT)) + 
        std::string(" Expecting: ") + std::to_string(numPrimaryKeys) +
        " Got: " + std::to_string(req->PKColumnsCount()));
      if (m_isBatch) {
        req->MarkInvalidOp(err);
        continue;
      }
      return err;
    }
    if (unlikely(numColumns < req->ReadColumnsCount())) {
      status = RS_CLIENT_ERROR(
          std::string(rdrsErrorMessage(ERROR_TOO_MANY_COLUMNS)));
      req->MarkInvalidOp(status);
      return status;
    }
    Uint32 num_bitmap_words = (numColumns + 31) / 32;
    Uint32 num_bitmap_bytes = 4 * num_bitmap_words;
    Uint8* bitmap_words = (Uint8*)amalloc->alloc_bytes(num_bitmap_bytes, 4);
    key_op->m_bitmap_read_columns = bitmap_words;
    Uint32 row_len = NdbDictionary::getRecordRowLength(ndb_record);
    Uint32 row_len_aligned = ((row_len + 7) / 8) * 8;
    Uint8* row = (Uint8*)amalloc->alloc_bytes(row_len_aligned, 8);
    /* Ensure no halfwritten words distort the rows for pk and reading */
    memset(row, 0, row_len_aligned);
    key_op->m_row = row;
    const NdbDictionary::Column **pkCols = (const NdbDictionary::Column**)
      amalloc->alloc_bytes(numPrimaryKeys * sizeof(NdbDictionary::Column*), 8);
    key_op->m_pkColumns = pkCols;
    const NdbDictionary::Column **readCols = nullptr;
    if (numReadColumns != 0) {
      readCols = (const NdbDictionary::Column**)
        amalloc->alloc_bytes(numReadColumns *
          sizeof(NdbDictionary::Column*), 8);
    } else {
      readCols = (const NdbDictionary::Column**)
        amalloc->alloc_bytes(numColumns *
          sizeof(NdbDictionary::Column*), 8);
    }
    key_op->m_readColumns = readCols;
    if (unlikely(bitmap_words == nullptr ||
                 pkCols == nullptr ||
                 readCols == nullptr ||
                 row == nullptr)) {
      status = RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
      return status;
    }
    Uint32 pk_bitmap_words[MAX_ATTRIBUTES_IN_TABLE/32];
    memset(bitmap_words, 0, num_bitmap_bytes);
    memset(pk_bitmap_words, 0, num_bitmap_bytes);
    Uint32 failed = 0;
    Uint32 j = 0;
    DEB_NDB_BE("Start setting up %u primary keys", numPrimaryKeys);
    for (; j < numPrimaryKeys; j++) {
      std::string_view pk_name(req->PKName(j), req->PKNameLen(j));
      const NdbDictionary::Column *pk_col =
        tableDict->getColumn(pk_name);
      if (unlikely(pk_col == nullptr || !pk_col->getPrimaryKey())) {
        failed = 1;
        break;
      }
      Uint32 col_id = pk_col->getColumnNo();
      Uint32 col_word = col_id / 32;
      Uint32 col_bit = col_id & 31;
      Uint32 col_bit_value = (pk_bitmap_words[col_word] >> col_bit) & 1;
      if (unlikely(col_bit_value != 0)) {
        failed = 2;
      }
      Uint32 word = pk_bitmap_words[col_word];
      Uint32 bit_value = 1 << col_bit;
      word |= bit_value;
      pk_bitmap_words[col_word] = word;
      key_op->m_pkColumns[j] = pk_col;
    }
    if (unlikely(failed != 0)) {
      RS_Status err;
      if (failed == 1) {
        err = RS_CLIENT_ERROR(
          std::string(rdrsErrorMessage(ERROR_WRONG_PRIMARY_KEY_COLUMN)) + 
          " " + std::string(req->PKName(j)));
      } else {
        err = RS_CLIENT_ERROR(
          std::string(rdrsErrorMessage(ERROR_SET_PK_MULTIPLE)) + 
          std::string(req->PKName(j)));
      }
      if (m_isBatch) {
        req->MarkInvalidOp(err);
        continue;
      }
      return err;
    }
    bool use_blob_values = false;
    if (numReadColumns != 0) {
      j = 0;
      DEB_NDB_BE("Start reading numReadColumns: %u", numReadColumns);
      for (; j < numReadColumns; j++) {
        std::string_view col_name(req->ReadColumnName(j),
                                  req->ReadColumnNameLen(j));
        const NdbDictionary::Column *read_col =
          tableDict->getColumn(col_name);
        DEB_NDB_BE("Try to find column %s, with len: %u",
          col_name.data(), (Uint32)col_name.size());
        if (unlikely(read_col == nullptr)) {
          failed = 1;
          DEB_NDB_BE_ERR("Failed to find column %s", col_name.data());
          break;
        }
        Uint32 col_id = read_col->getColumnNo();
        Uint32 col_word = col_id / 8;
        Uint32 col_bit = col_id & 7;
        Uint32 col_bit_value = (bitmap_words[col_word] >> col_bit) & 1;
        if (unlikely(col_bit_value != 0)) {
          failed = 2;
          break;
        }
        Uint32 word = bitmap_words[col_word];
        Uint32 bit_value = 1 << col_bit;
        word |= bit_value;
        bitmap_words[col_word] = word;
        key_op->m_readColumns[j] = read_col;
        if (unlikely(use_blob_values == false &&
                     (read_col->getType() == NdbDictionary::Column::Blob ||
                      read_col->getType() == NdbDictionary::Column::Text))) {
          use_blob_values = true;
          m_single_transaction = true;
          key_op->m_blob_handles = (NdbBlob**)
            amalloc->alloc_bytes(sizeof(NdbBlob*) * numReadColumns, 8);
          if (unlikely(key_op->m_blob_handles == nullptr)) {
            return RS_SERVER_ERROR(
                std::string(
                  rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
          }
          DEB_NDB_BE("Allocating memory at %p for"
                     " m_key_ops[%u].m_blob_handles",
                     m_key_ops[i].m_blob_handles, i);
        }
      }
      if (unlikely(failed != 0)) {
        RS_Status err;
        if (failed == 1) {
          err = RS_CLIENT_404_WITH_MSG_ERROR(
            std::string(rdrsErrorMessage(ERROR_COLUMN_NOT_EXIST)) + 
            std::string(" Database: ") + std::string(req->DB()) + 
            std::string(" Table: ") + std::string(req->Table()) +
            std::string(" Column: ") + std::string(req->ReadColumnName(j)));
        } else {
          err = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) + 
            std::string(req->ReadColumnName(j)));
        }
        if (m_isBatch) {
          req->MarkInvalidOp(err);
          continue;
        }
        return err;
      }
    } else {
      /**
       * When we arrive here we have not received any column names from
       * the JSON request. It is too early to fill them in here since it
       * is possible that the schema changes between now and a retry
       * attempt. Thus we fill in this information in create_response
       * instead.
       */
      DEB_NDB_BE("Start reading all columns: %u", numColumns);
      bool use_blob_values = false;
      for (Uint32 k = 0; k < numColumns; k++) {
        const NdbDictionary::Column *read_col = tableDict->getColumn(k);
        key_op->m_readColumns[k] = read_col;
        DEB_NDB_BE("Read column, id: %u, name: %s", k, read_col->getName());
        if (unlikely(!use_blob_values &&
                     (read_col->getType() == NdbDictionary::Column::Blob ||
                      read_col->getType() == NdbDictionary::Column::Text))) {
          if (use_blob_values == false) {
            use_blob_values = true;
            m_single_transaction = true;
            key_op->m_blob_handles = (NdbBlob**)
              amalloc->alloc_bytes(sizeof(NdbBlob*) * numReadColumns, 8);
            if (unlikely(key_op->m_blob_handles == nullptr)) {
              return RS_SERVER_ERROR(
                  std::string(
                    rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
            }
            DEB_NDB_BE("(2)Allocating memory at %p for"
                       " m_key_ops[%u].m_blob_handles",
                       m_key_ops[i].m_blob_handles, i);
          }
        }
      }
      Uint32* bitmap_words32 = (Uint32*)bitmap_words;
      for (j = 0; j < num_bitmap_words; j++) {
        bitmap_words32[j] = 0xFFFFFFFF;
      }
      key_op->m_num_read_columns = numColumns;
    }
  }
  return status;
}

RS_Status BatchKeyOperations::setup_primary_keys() {
  for (Uint32 opIdx = 0; opIdx < m_numOperations; opIdx++) {
    // this sub operation can not be processed
    KeyOperation *key_op = &m_key_ops[opIdx];
    PKRRequest *req = &key_op->m_req;
    if (unlikely(req->IsInvalidOp())) {
      continue;
    }
    Uint32 numPrimaryKeys = key_op->m_num_pk_columns;
    for (Uint32 colIdx = 0; colIdx < numPrimaryKeys; colIdx++) {
      RS_Status status =
        set_operation_pk_col(key_op->m_pkColumns[colIdx],
                             req,
                             key_op->m_row,
                             key_op->m_ndb_record,
                             colIdx);
      DEB_NDB_BE("First words of row is: 0x%x, op: %u",
                 *(Uint32*)key_op->m_row, opIdx);
      if (unlikely(status.http_code != SUCCESS)) {
        if (m_isBatch) {
          req->MarkInvalidOp(status);
        } else {
          return status;
        }
      }
    }
  }
  return RS_OK;
}

RS_Status BatchKeyOperations::setup_transactions() {
  Uint32 tmp[MAX_KEY_SIZE_IN_WORDS * MAX_XFRM_MULTIPLY];
  char *buf = (char *)&tmp[0];
  if (unlikely(m_single_transaction)) {
    if (m_key_ops[0].m_ndbTransaction != nullptr) {
      /* Transaction started already in previous loop */
      return RS_OK;
    }
    KeyOperation *key_op = &m_key_ops[0];
    KeyOperation *first_key_op = nullptr;
    for (Uint32 i = m_first_key; i < m_last_key; i++) {
      first_key_op = &m_key_ops[i];
      PKRRequest *req = &first_key_op->m_req;
      if (req->IsInvalidOp()) {
        first_key_op = nullptr;
        continue;
      }
      break;
    }
    /* Check that at least one operation is valid */
    if (first_key_op != nullptr) {
      key_op->m_ndbTransaction = m_ndb_object->startTransaction(
        first_key_op->m_ndb_record,
        (const char*)first_key_op->m_row,
        buf,
        sizeof(tmp));
      if (unlikely(key_op->m_ndbTransaction == nullptr)) {
        return RS_RONDB_SERVER_ERROR(m_ndb_object->getNdbError(), 
          std::string(rdrsErrorMessage(ERROR_TRANSACTION_START_FAILED)));
      }
    }
  } else {
    for (Uint32 i = m_first_key; i < m_last_key; i++) {
      KeyOperation *key_op = &m_key_ops[i];
      PKRRequest *req = &key_op->m_req;
      if (req->IsInvalidOp()) {
        continue;
      }
      key_op->m_ndbTransaction = m_ndb_object->startTransaction(
        key_op->m_ndb_record,
        (const char*)key_op->m_row,
        buf,
        sizeof(tmp));
      if (unlikely(key_op->m_ndbTransaction == nullptr)) {
        return RS_RONDB_SERVER_ERROR(m_ndb_object->getNdbError(), 
            std::string(rdrsErrorMessage(ERROR_TRANSACTION_START_FAILED)));
      }
    }
  }
  return RS_OK;
}

/**
 * Set up read operation
 *
 * @return status
 */
RS_Status BatchKeyOperations::setup_read_operations() {
  for (Uint32 opIdx = m_first_key; opIdx < m_last_key; opIdx++) {
    KeyOperation *key_op = &m_key_ops[opIdx];
    PKRRequest *req = &key_op->m_req;
    if (unlikely(req->IsInvalidOp())) {
      continue;
    }
    NdbTransaction *trans = key_op->m_ndbTransaction;
    if (unlikely(m_single_transaction)) {
      trans = m_key_ops[0].m_ndbTransaction;
      require(trans);
    }
    DEB_NDB_BE("readTuple: read_columns[%u]: 0x%x,0x%x",
              opIdx,
              key_op->m_bitmap_read_columns[0],
              key_op->m_bitmap_read_columns[1]);
    NdbOperation::OperationOptions opts;
    std::memset(&opts, 0, sizeof(opts));
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_BATCH_SAFE_FLAG;
    const NdbOperation *operation = trans->readTuple(
      key_op->m_ndb_record,
      (const char*)key_op->m_row,
      key_op->m_ndb_record,
      (char*)key_op->m_row,
      NdbOperation::LM_CommittedRead,
      key_op->m_bitmap_read_columns,
      &opts,
      sizeof(opts));
    if (unlikely(operation == nullptr)) {
      return RS_RONDB_SERVER_ERROR(trans->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_READ_OPERATION_FAILED)));
    }
    key_op->m_ndbOperation = operation;
    if (unlikely(key_op->m_blob_handles != nullptr)) {
      for (Uint32 colIdx = 0;
           colIdx < key_op->m_num_read_columns;
           colIdx++) {
        const NdbDictionary::Column *col =
          key_op->m_readColumns[colIdx];
        if (unlikely(col->getType() == NdbDictionary::Column::Blob ||
                     col->getType() == NdbDictionary::Column::Text)) {
          key_op->m_blob_handles[colIdx] =
            operation->getBlobHandle(col->getName());
          DEB_NDB_BE("Blob handle for %s in op %u in col: %u is %p",
            col->getName(),
            opIdx,
            colIdx,
            key_op->m_blob_handles[colIdx]);
          if (unlikely(key_op->m_blob_handles[colIdx] == nullptr)) {
            return RS_SERVER_ERROR(std::string(
              rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
          }
        } else {
          DEB_NDB_BE("No Blob handle for %s in op %u in col: %u",
            col->getName(), opIdx, colIdx);
          key_op->m_blob_handles[colIdx] = nullptr;
        }
      }
    }
    if (likely(!m_single_transaction)) {
      trans->executeAsynchPrepare(NdbTransaction::Commit,
                                  nullptr,
                                  (void*)key_op);
    }
    m_num_sent_operations++;
  }
  return RS_OK;
}

RS_Status BatchKeyOperations::execute() {
  if (m_num_sent_operations > 0) {
    if (unlikely(m_single_transaction)) {
      NdbTransaction *trans = m_key_ops[0].m_ndbTransaction;
      if (unlikely(trans->execute(NdbTransaction::NoCommit) != 0)) {
        return RS_RONDB_SERVER_ERROR(trans->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_TRANSACTION_EXEC_FAILED)));
      }
    } else {
      if (m_ndb_object->sendPollNdb(
          WAITFOR_RESPONSE_TIMEOUT, m_num_sent_operations) <
            (int)m_num_sent_operations) {
        return RS_RONDB_SERVER_ERROR(
          m_ndb_object->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_TRANSACTION_EXEC_FAILED)));
      }
    }
  }
  return RS_OK;
}

RS_Status BatchKeyOperations::create_response(RS_Buffer *respBuffs) {
  bool found = true;
  Uint32 response_buffer_size = respBuffs[0].size;
  Uint32 response_buffer_limit = response_buffer_size / 2;
  Uint32 current_head = 0;
  Uint32 response_length = 0;
  Uint32 current_response_buffer_idx = 0;
  for (size_t i = 0; i < m_numOperations; i++) {
    current_head += response_length;
    if (i > 0)
      respBuffs[i] = getNextRespRS_Buffer(
        current_head,
        response_buffer_limit,
        respBuffs[current_response_buffer_idx],
        current_response_buffer_idx,
        i
      );
    KeyOperation *key_op = &m_key_ops[i];
    PKRResponse *resp =
      new (&key_op->m_resp) PKRResponse(&respBuffs[i]);
    PKRRequest *req = &key_op->m_req;
    const NdbOperation *op = key_op->m_ndbOperation;
    resp->SetOperationID(req->OperationId());
    if (unlikely(req->IsInvalidOp())) {
      resp->SetStatus(req->GetError().http_code, req->GetError().message);
      resp->Close(response_length);
      continue;
    }
    resp->SetNoOfColumns(key_op->m_num_read_columns);
    if (req->ReadColumnsCount() == 0) {
      DEB_NDB_BE("Build request when all columns requested");
      Uint32 numColumns = key_op->m_num_table_columns;
      if (unlikely(!req->addReadColumns(numColumns))) {
        return RS_SERVER_ERROR(std::string(
          rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
      }
      for (Uint32 k = 0; k < numColumns; k++) {
        const NdbDictionary::Column *read_col =
          key_op->m_tableDict->getColumn(k);
        if (unlikely(req->addReadColumnName(k,
                                   read_col->getName(),
                                   DEFAULT_DRT))) {
          return RS_SERVER_ERROR(std::string(
            rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
        }
      }
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
      resp->Close(response_length);
      return RS_RONDB_SERVER_ERROR(
        op->getNdbError(), std::string("SubOperation ") +
        std::string(req->OperationId()) +
        std::string(" failed"));
    }
    if (likely(found)) {
      // iterate over all columns
      RS_Status ret = key_op->append_op_recs(resp, req);
      if (unlikely(ret.http_code != SUCCESS)) {
        return ret;
      }
    }
    resp->Close(response_length);
  }
  if (unlikely(!found && !m_isBatch)) {
    return RS_CLIENT_404_ERROR();
  }
  return RS_OK;
}

RS_Status KeyOperation::append_op_recs(PKRResponse *resp,
                                       PKRRequest *req) {
  for (Uint32 colIdx = 0; colIdx < m_num_read_columns; colIdx++) {
    RS_Status ret = write_col_to_resp(colIdx, resp, req);
    if (unlikely(ret.http_code != SUCCESS)) {
      DEB_NDB_BE("Failed with colIdx: %u, code: %u, message: %s",
        colIdx, ret.http_code, ret.message);
      return ret;
    }
  }
  return RS_OK;
}

static inline void my_unpack_date(MYSQL_TIME *l_time, const void *d) {
  uchar b[4];
  memcpy(b, d, 3);
  b[3] = 0;
  uint w = (uint)uint3korr(b);
  l_time->day = (w & 31);
  w >>= 5;
  l_time->month = (w & 15);
  w >>= 4;
  l_time->year = w;
  l_time->time_type = MYSQL_TIMESTAMP_DATE;
}

RS_Status KeyOperation::write_col_to_resp(Uint32 colIdx,
                                          PKRResponse *response,
                                          PKRRequest *request) {
  const NdbDictionary::Column *col = m_readColumns[colIdx];
  const NdbRecord *ndb_record = m_ndb_record;
  const char *col_name = col->getName();
  Uint32 col_id = col->getColumnNo();
  Uint8 *row = m_row;
  {
    Uint32 null_byte_offset;
    Uint32 null_bit_in_byte;
    bool null_value = NdbDictionary::getNullBitOffset(
      ndb_record, col_id, null_byte_offset, null_bit_in_byte);
    if (null_value) {
      Uint8 null_byte = row[null_byte_offset];
      Uint8 null_bit_value = (null_byte >> null_bit_in_byte) & 1;
      if (null_bit_value) {
        DEB_NDB_BE("Column %s is null", col_name);
        return response->SetColumnDataNull();
      }
    }
  }
  Uint32 offset;
  bool ret = NdbDictionary::getOffset(ndb_record, col_id, offset);
  require(ret);
  Uint8 *col_ptr = row + offset;
  DEB_NDB_BE("Column %s with col_id: %u is not null, offset: %u, type: %u",
             col_name, col_id, offset, col->getType());
  switch (col->getType()) {
  case NdbDictionary::Column::Undefined: {
    ///< 4 bytes + 0-3 fraction
    return RS_CLIENT_ERROR(std::string(
      rdrsErrorMessage(ERROR_UNDEFINED_DATA_TYPE)) + 
        std::string(" Column: ") + std::string(col_name));
  }
  case NdbDictionary::Column::Tinyint: {
    ///< 8 bit. 1 byte signed integer, can be used in array
    return response->Append_i8(*(Int8*)col_ptr);
  }
  case NdbDictionary::Column::Tinyunsigned: {
    ///< 8 bit. 1 byte unsigned integer, can be used in array
    return response->Append_iu8(*(Uint8*)col_ptr);
  }
  case NdbDictionary::Column::Smallint: {
    ///< 16 bit. 2 byte signed integer, can be used in array
    Int16 i16;
    memcpy(&i16, col_ptr, sizeof(Int16));
    return response->Append_i16(i16);
  }
  case NdbDictionary::Column::Smallunsigned: {
    Uint16 u16;
    memcpy(&u16, col_ptr, sizeof(Uint16));
    ///< 16 bit. 2 byte unsigned integer, can be used in array
    return response->Append_iu16(u16);
  }
  case NdbDictionary::Column::Mediumint: {
    ///< 24 bit. 3 byte signed integer, can be used in array
    return response->Append_i24(sint3korr(col_ptr));
  }
  case NdbDictionary::Column::Mediumunsigned: {
    ///< 24 bit. 3 byte unsigned integer, can be used in array
    return response->Append_iu24(uint3korr(col_ptr));
  }
  case NdbDictionary::Column::Int: {
    ///< 32 bit. 4 byte signed integer, can be used in array
    Int32 i32;
    memcpy(&i32, col_ptr, sizeof(Int32));
    return response->Append_i32(i32);
  }
  case NdbDictionary::Column::Unsigned: {
    ///< 32 bit. 4 byte unsigned integer, can be used in array
    Uint32 u32;
    memcpy(&u32, col_ptr, sizeof(Uint32));
    return response->Append_iu32(u32);
  }
  case NdbDictionary::Column::Bigint: {
    ///< 64 bit. 8 byte signed integer, can be used in array
    Int64 i64;
    memcpy(&i64, col_ptr, sizeof(Int64));
    return response->Append_i64(i64);
  }
  case NdbDictionary::Column::Bigunsigned: {
    ///< 64 Bit. 8 byte signed integer, can be used in array
    Uint64 u64;
    memcpy(&u64, col_ptr, sizeof(Uint64));
    return response->Append_iu64(u64);
  }
  case NdbDictionary::Column::Float: {
    ///< 32-bit float. 4 bytes float, can be used in array
    float f32;
    memcpy(&f32, col_ptr, sizeof(float));
    return response->Append_f32(f32);
  }
  case NdbDictionary::Column::Double: {
    ///< 64-bit float. 8 byte float, can be used in array
    double d64;
    memcpy(&d64, col_ptr, sizeof(double));
    return response->Append_d64(d64);
  }
  case NdbDictionary::Column::Olddecimal: {
    ///< MySQL < 5.0 signed decimal,  Precision, Scale
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) + 
      std::string(" Column: ") + std::string(col_name) +
      " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Olddecimalunsigned: {
    ///< MySQL < 5.0 signed decimal,  Precision, Scale
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) +
      std::string(" Column: ") + std::string(col_name) +
      " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Decimal:
    ///< MySQL >= 5.0 signed decimal,  Precision, Scale
    [[fallthrough]];
  case NdbDictionary::Column::Decimalunsigned: {
    char decStr[DECIMAL_MAX_STR_LEN_IN_BYTES];
    int precision = col->getPrecision();
    int scale = col->getScale();
    void *bin = (void*)col_ptr;
    int binLen = col->getSizeInBytesForRecord();
    decimal_bin2str(bin,
                    binLen,
                    precision,
                    scale,
                    decStr,
                    DECIMAL_MAX_STR_LEN_IN_BYTES);
    DEB_NDB_BE("col_name: %s Decimal column, decStr: %s, binLen: %u",
               col_name, std::string(decStr).c_str(), binLen);
    return response->Append_string(std::string(decStr),
                                   RDRS_FLOAT_DATATYPE);
  }
  case NdbDictionary::Column::Char:
    ///< Len. A fixed array of 1-byte chars
    [[fallthrough]];
  case NdbDictionary::Column::Varchar:
    ///< Length bytes: 1, Max: 255
    [[fallthrough]];
  case NdbDictionary::Column::Longvarchar: {
    ///< Length bytes: 2, little-endian
    const char *dataStart = nullptr;
    const NdbDictionary::Column::ArrayType arrayType =
      col->getArrayType();
    Uint32 attrBytes = col->getLength();
    switch (arrayType) {
    case NdbDictionary::Column::ArrayTypeFixed:
      /**
       *  No prefix length is stored in aRef. Data starts from aRef's first byte
       *  data might be padded with blank or null bytes to fill the whole column
       */
      dataStart = (const char*)col_ptr;
      break;
    case NdbDictionary::Column::ArrayTypeShortVar:
      /**
       * First byte of aRef has the length of data stored
       *  Data starts from second byte of aRef
       */
      dataStart = (const char*)(col_ptr + 1);
      attrBytes = static_cast<Uint8>(col_ptr[0]);
      break;
    case NdbDictionary::Column::ArrayTypeMediumVar:
      /**
       * First two bytes of aRef has the length of data stored
       * Data starts from third byte of aRef
       */
      dataStart = (const char*)(col_ptr + 2);
      attrBytes = static_cast<Uint8>(col_ptr[1]) * 256 +
                  static_cast<Uint8>(col_ptr[0]);
      break;
    default:
      return RS_CLIENT_ERROR(std::string(
        rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
    }
    return response->Append_char(dataStart,
                                 attrBytes,
                                 col->getCharset(),
                                 col->getType() == NdbDictionary::Column::Char);
  }
  case NdbDictionary::Column::Binary:
    [[fallthrough]];
  case NdbDictionary::Column::Varbinary:
    ///< Length bytes: 1, Max: 255
    [[fallthrough]];
  case NdbDictionary::Column::Longvarbinary: {
    ///< Length bytes: 2, little-endian
    const char *dataStart = nullptr;
    const NdbDictionary::Column::ArrayType arrayType =
      col->getArrayType();
    Uint32 attrBytes = col->getLength();
    switch (arrayType) {
    case NdbDictionary::Column::ArrayTypeFixed:
      /**
       *  No prefix length is stored in aRef. Data starts from aRef's first byte
       *  data might be padded with blank or null bytes to fill the whole column
       */
      dataStart = (const char*)col_ptr;
      break;
    case NdbDictionary::Column::ArrayTypeShortVar:
      /**
       * First byte of aRef has the length of data stored
       *  Data starts from second byte of aRef
       */
      dataStart = (const char*)(col_ptr + 1);
      attrBytes = static_cast<Uint8>(col_ptr[0]);
      break;
    case NdbDictionary::Column::ArrayTypeMediumVar:
      /**
       * First two bytes of aRef has the length of data stored
       * Data starts from third byte of aRef
       */
      dataStart = (const char*)(col_ptr + 2);
      attrBytes = static_cast<Uint8>(col_ptr[1]) * 256 +
                  static_cast<Uint8>(col_ptr[0]);
      break;
    default:
      return RS_CLIENT_ERROR(std::string(
        rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
    }
    if (unlikely(attrBytes > MAX_TUPLE_SIZE_IN_BYTES)) {
      return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_TOO_LARGE_ROWS)) +
        std::string(" DB: ") + std::string(request->DB()) +
        " Table: " + std::string(request->Table()));
    }
    DEB_NDB_BE("dataStart: %p, len: %u", dataStart, attrBytes);
    return response->Append_bin(dataStart, attrBytes, RDRS_BINARY_DATATYPE);
  }
  case NdbDictionary::Column::Datetime: {
    ///< Precision down to 1 sec (sizeof(Datetime) == 8 bytes )
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) + 
      std::string(" Column: ") + std::string(col_name) +
      " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Date: {
    ///< Precision down to 1 day(sizeof(Date) == 4 bytes )
    MYSQL_TIME lTime;
    my_unpack_date(&lTime, (char*)col_ptr);
    char to[MAX_DATE_STRING_REP_LENGTH];
    my_date_to_str(lTime, to);
    return response->Append_string(std::string(to), RDRS_DATETIME_DATATYPE);
  }
  case NdbDictionary::Column::Blob: {
    ///< Binary large object (see NdbBlob)
    /// Treat it as binary data
    require(m_blob_handles[colIdx] != nullptr);
    NdbBlob *blobHandle = m_blob_handles[colIdx];
    Uint64 length = 0;
    int isNull = 0;
    if (unlikely(blobHandle->getNull(isNull) != 0)) {
      return RS_RONDB_SERVER_ERROR(
        blobHandle->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) + 
          std::string(" Failed to check NULL of ") +
          std::string(" Column: ") + std::string(col_name) +
          " Type: " + std::to_string(col->getType()));
    }
    if (isNull) {
      if (unlikely(blobHandle->getLength(length) != 0)) {
        return RS_RONDB_SERVER_ERROR(
          blobHandle->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) +
          std::string(" NULL column has size != 0 ") +
          std::string(" Column: ") + std::string(col_name) +
          " Type: " + std::to_string(col->getType()));
      }
      return response->SetColumnDataNull();
    }
    if (blobHandle->getLength(length) == -1) {
      return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) + 
        std::string(" Reading column length failed.") +
          std::string(" Column: ") + std::string(col_name) +
          " Type: " + std::to_string(col->getType()));
    }
    DEB_NDB_BE("Read col_name: %s, BLOB of length: %llu", col_name, length);
    // check for max length
    // (4 * ceil(input_size / 3))
    const size_t maxLength = length + 4;
    if (unlikely(response->GetRemainingCapacity() < maxLength)) {
      return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_RESPONSE_BUFFER_OVERFLOW)) + 
        std::string(" Buffer Remaining Capacity: ") +
        std::to_string(response->GetRemainingCapacity()) +
        " Required: " + std::to_string(maxLength));
    }
    Uint64 chunk = 0;
    Uint64 total_read = 0;
    char buffer[BLOB_MAX_FETCH_SIZE];

    for (chunk = 0; chunk < (length / (BLOB_MAX_FETCH_SIZE)) + 1; chunk++) {
      Uint64 pos = chunk * BLOB_MAX_FETCH_SIZE;
      // NOTE this is bytes to read and also bytes read.
      Uint32 bytes = BLOB_MAX_FETCH_SIZE;
      if (pos + bytes > length) {
        bytes = length - pos;
      }
      if (bytes != 0) {
        if (unlikely(-1 == blobHandle->setPos(pos))) {
          return RS_RONDB_SERVER_ERROR(
            blobHandle->getNdbError(),
            std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) + 
            std::string(" Failed to set read position.") +
            std::string(" Column: ") + std::string(col_name) +
            " Type: " + std::to_string(col->getType()));
        }
        if (blobHandle->readData(buffer,
                                 bytes /*to read, also bytes read*/) == -1) {
          return RS_RONDB_SERVER_ERROR(
            blobHandle->getNdbError(),
            std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) + 
            std::string(" Read data failed .") +
            std::string(" Column: ") + std::string(col_name) +
            " Type: " + std::to_string(col->getType()) +
            " Position: " + std::to_string(pos));
        }
        if (bytes > 0) {
          total_read += bytes;
          if (chunk == 0) {
            response->Append_string("", RDRS_BINARY_DATATYPE);
            // This adds a column to the response buffer. Right now the last
            // byte of the response buffer is '\0'. Remove the last byte and
            // start appending the base64 data
            response->AdvanceWritePointer(-1);
          }
          memcpy((char*)response->GetWritePointer(),
                 (const char*)buffer,
                 bytes);
          response->AdvanceWritePointer(bytes);
        }
      }
    }
    if (total_read != length) {
      return RS_RONDB_SERVER_ERROR(
        blobHandle->getNdbError(),
        std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) + 
        std::string(" Not all of the data was read.") +
        std::string(" Column: ") + std::string(col_name) +
        " Expected to read: " + std::to_string(length) +
        " bytes. Read: " + std::to_string(total_read));
    }
    response->SetBlobLen(total_read);
    DEB_NDB_BE("Written a blob of total len: %u",
      (Uint32)total_read);
    return RS_OK;
  }
  case NdbDictionary::Column::Text: {
    ///< Text blob
    require(m_blob_handles[colIdx] != nullptr);
    NdbBlob *blobHandle = m_blob_handles[colIdx];
    Uint64 length = 0;
    int isNull = 0;
    if (unlikely(blobHandle->getNull(isNull) != 0)) {
      return RS_RONDB_SERVER_ERROR(
        blobHandle->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) + 
          std::string(" Failed to check NULL of ") +
          std::string(" Column: ") + std::string(col_name) +
          " Type: " + std::to_string(col->getType()));
    }
    if (isNull) {
      if (unlikely(blobHandle->getLength(length) != 0)) {
        return RS_RONDB_SERVER_ERROR(
          blobHandle->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) + 
          std::string(" NULL column has size != 0 ") +
          std::string(" Column: ") + std::string(col_name) +
          " Type: " + std::to_string(col->getType()));
      }
      return response->SetColumnDataNull();
    }
    if (blobHandle->getLength(length) == -1) {
      return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) + 
        std::string(" Reading column length failed.") +
        std::string(" Column: ") + std::string(col_name) +
        " Type: " + std::to_string(col->getType()));
    }
    //+1 for null terminator
    if (unlikely(response->GetRemainingCapacity() < length + 1)) {
      return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_RESPONSE_BUFFER_OVERFLOW)) + 
        std::string(" Buffer Remaining Capacity: ") +
        std::to_string(response->GetRemainingCapacity()) +
        " Required: " + std::to_string(length + 1));
    }
    DEB_NDB_BE("Read col_name: %s, TEXT of length: %llu", col_name, length);
    // NOTE: we not allocating a tmp buffer to hold the data
    // Reusing the reponse buffer
    char *tmpBuffer = static_cast<char*>(response->GetWritePointer());
    Uint64 chunk = 0;
    Uint64 total_read = 0;
    for (chunk = 0; chunk < (length / (BLOB_MAX_FETCH_SIZE)) + 1; chunk++) {
      Uint64 pos   = chunk * BLOB_MAX_FETCH_SIZE;
      // NOTE this is bytes to read and also bytes read.
      Uint32 bytes = BLOB_MAX_FETCH_SIZE;
      if (pos + bytes > length) {
        bytes = length - pos;
      }
      if (bytes != 0) {
        if (-1 == blobHandle->setPos(pos)) {
          return RS_RONDB_SERVER_ERROR(
            blobHandle->getNdbError(),
            std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) + 
            std::string(" Failed to set read position.") +
            std::string(" Column: ") + std::string(col_name) +
            " Type: " + std::to_string(col->getType()));
        }
        if (blobHandle->readData(tmpBuffer,
                                 bytes /*to read, also bytes read*/) == -1) {
          return RS_RONDB_SERVER_ERROR(
            blobHandle->getNdbError(),
            std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) + 
            std::string(" Read data failed .") +
            std::string(" Column: ") + std::string(col_name) +
            " Type: " + std::to_string(col->getType()) +
            " Position: " + std::to_string(pos));
        }
        if (bytes > 0) {
          tmpBuffer += bytes;  // move the pointer forward
          total_read += bytes;
        }
      }
    }
    if (unlikely(total_read != length)) {
      return RS_RONDB_SERVER_ERROR(
        blobHandle->getNdbError(),
        std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) + 
        std::string(" Not all of the data was read.") +
        std::string(" Column: ") + std::string(col_name) +
        " Expected to read: " + std::to_string(length) +
        " bytes. Read: " + std::to_string(total_read));
    }
    return response->Append_char(
      static_cast<char *>(response->GetWritePointer()),
      length,
      col->getCharset(),
      false);
  }
  case NdbDictionary::Column::Bit: {
    Uint32 len = col->getLength();
    Uint32 words = len / 8;
    Uint32 bits_used_in_last_word = len % 8;
    Uint32 last_mask = 0xFF;
    if (bits_used_in_last_word != 0) {
      words += 1;
      last_mask = ((1 << bits_used_in_last_word) - 1);
    }
    require(words <= BIT_MAX_SIZE_IN_BYTES);
    // change endieness
    col_ptr[words - 1] &= last_mask;
    int i = 0;
    char reversed[BIT_MAX_SIZE_IN_BYTES];
    for (int j = words - 1; j >= 0; j--) {
      reversed[i++] = (char)col_ptr[j];
    }
    DEB_NDB_BE("BIT:col_name: %s, col_ptr[words - 1] = %x, outlen: %u",
               col_name,
               col_ptr[words - 1],
               Uint32(words));
    return response->Append_bin(reversed, words, RDRS_BIT_DATATYPE);
  }
  case NdbDictionary::Column::Time: {
    ///< Time without date
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) + 
      std::string(" Column: ") + std::string(col_name) +
      " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Year: {
    ///< Year 1901-2155 (1 byte)
    Int32 year = (uint)(1900 + col_ptr[0]);
    return response->Append_i32(year);
  }
  case NdbDictionary::Column::Timestamp: {
    ///< Unix time
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) + 
      std::string(" Column: ") +
      std::string(col_name) +
      " Type: " + std::to_string(col->getType()));
  }
  ///**
  // * Time types in MySQL 5.6 add microsecond fraction.
  // * One should use setPrecision(x) to set number of fractional
  // * digits (x = 0-6, default 0).  Data formats are as in MySQL
  // * and must use correct byte length.  NDB does not check data
  // * itself since any values can be compared as binary strings.
  // */
  case NdbDictionary::Column::Time2: {
    ///< 3 bytes + 0-3 fraction
    uint precision = col->getPrecision();
    longlong numericTime =
      my_time_packed_from_binary((const unsigned char *)col_ptr, precision);
    MYSQL_TIME lTime;
    TIME_from_longlong_time_packed(&lTime, numericTime);
    char to[MAX_DATE_STRING_REP_LENGTH];
    my_TIME_to_str(lTime, to, precision);
    return response->Append_string(std::string(to), RDRS_DATETIME_DATATYPE);
  }
  case NdbDictionary::Column::Datetime2: {
    ///< 5 bytes plus 0-3 fraction
    uint precision = col->getPrecision();
    longlong numericDate =
      my_datetime_packed_from_binary((const unsigned char *)col_ptr, precision);
    MYSQL_TIME lTime;
    TIME_from_longlong_datetime_packed(&lTime, numericDate);
    char to[MAX_DATE_STRING_REP_LENGTH];
    my_TIME_to_str(lTime, to, precision);
    return response->Append_string(std::string(to), RDRS_DATETIME_DATATYPE);
  }
  case NdbDictionary::Column::Timestamp2: {
    ///< 4 bytes + 0-3 fraction
    uint precision = col->getPrecision();
    my_timeval myTV{};
    my_timestamp_from_binary(&myTV, (const unsigned char *)col_ptr, precision);
    Int64 epochIn = myTV.m_tv_sec;
    time_t stdtime(epochIn);
    struct tm *time_info = gmtime(&stdtime);
    MYSQL_TIME lTime  = {};
    lTime.year        = time_info->tm_year + 1900;
    lTime.month       = time_info->tm_mon +1;
    lTime.day         = time_info->tm_mday;
    lTime.hour        = time_info->tm_hour; 
    lTime.minute      = time_info->tm_min; 
    lTime.second      = time_info->tm_sec; 
    lTime.second_part = myTV.m_tv_usec;
    lTime.time_type   = MYSQL_TIMESTAMP_DATETIME;
    char to[MAX_DATE_STRING_REP_LENGTH];
    my_TIME_to_str(lTime, to, precision);
    return response->Append_string(std::string(to), RDRS_DATETIME_DATATYPE);
  }
  }
  return RS_SERVER_ERROR(
    std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) + 
    std::string(" Column: ") + std::string(col_name) +
    " Type: " + std::to_string(col->getType()));
}

void BatchKeyOperations::close_transaction() {
  if (unlikely(m_single_transaction)) {
    if (m_key_ops[0].m_ndbTransaction != nullptr) {
      m_ndb_object->closeTransaction(m_key_ops[0].m_ndbTransaction);
    }
  } else {
    for (Uint32 i = 0; i < m_numOperations; i++) {
      if (m_key_ops[i].m_ndbTransaction != nullptr) {
        m_ndb_object->closeTransaction(m_key_ops[i].m_ndbTransaction);
      }
    }
  }
}

RS_Status BatchKeyOperations::perform_operation(
  ArenaMalloc *amalloc,
  Uint32 numOperations,
  bool is_batch,
  RS_Buffer *reqBuffer,
  RS_Buffer *respBuffer,
  Ndb *ndb_object) {

  DEB_NDB_BE("init_batch_operations");
  RS_Status status = init_batch_operations(
    amalloc,
    numOperations,
    is_batch,
    reqBuffer,
    ndb_object);
  if (unlikely(status.http_code != SUCCESS)) {
    handle_ndb_error(status);
    return status;
  }
  DEB_NDB_BE("setup_primary_keys");
  status = setup_primary_keys();
  if (unlikely(status.http_code != SUCCESS)) {
    handle_ndb_error(status);
    return status;
  }
  m_first_key = 0;
  Uint32 last_key = 0;
  Uint32 min_keys_for_last_loop = 50;
  Uint32 max_keys_in_one_loop = 1024;
  while (m_last_key < m_numOperations) {
    /**
     * Handle loop of operations.
     * 1. Use batchMaxSize as a suggested maximum batch size although not
     *    a hard limit. Preferrably this should be e.g. 128 and is configurable
     * 2. We are allowed to add up to 50 operations to this batch size for the
     *    last loop. The reason for this is to avoid a weird situation where we
     *    have batchMaxSize and need to send 129 operations. In this case it is
     *    preferrable to run all operations in one loop to avoid that the second
     *    loop only runs one operation. We set the minimum operations to handle
     *    in one loop to 50 as a hard coded constant here.
     * 3. NDB API can handle at most 1024 transactions in parallel and this is
     *    already way too much, we will set the maximum batchMaxSize to e.g.
     *    512 to avoid even coming close to this. Experiments have shown that
     *    optimal batch size is around 100 operations. Thus we set the default
     *    to 128.
     */
    Uint32 numOps = m_numOperations;
    Uint32 start = m_first_key;
    last_key = m_last_key;
    Uint32 max_keys = globalConfigs.internal.batchMaxSize;
    Uint32 keys_this_loop = std::min(max_keys, (numOps - start));
    last_key += keys_this_loop;
    Uint32 remaining_keys = numOps - last_key;
    if (last_key < numOps &&
        remaining_keys < 50) {
      last_key += remaining_keys;
    }
    m_last_key = last_key;

    DEB_NDB_BE("setup_transactions");
    status = setup_transactions();
    if (unlikely(status.http_code != SUCCESS)) {
      handle_ndb_error(status);
      return status;
    }
    DEB_NDB_BE("setup_read_operations");
    status = setup_read_operations();
    if (unlikely(status.http_code != SUCCESS)) {
      handle_ndb_error(status);
      return status;
    }
    DEB_NDB_BE("execute");
    status = execute();
    if (unlikely(status.http_code != SUCCESS)) {
      handle_ndb_error(status);
      return status;
    }
    m_first_key = m_last_key;
    m_num_sent_operations = 0;
  }
  DEB_NDB_BE("create_response");
  status = create_response(respBuffer);
  if (unlikely(status.http_code != SUCCESS)) {
    handle_ndb_error(status);
    return status;
  }
  DEB_NDB_BE("close_transaction");
  close_transaction();

  m_isSuccess = true;

  return RS_OK;
}

RS_Status BatchKeyOperations::handle_ndb_error(RS_Status status) {
  // schema errors
  if (UnloadSchema(status)) {
    // no idea which sub-operation threw the error
    // unload all tables used in this operation
    std::list<std::tuple<std::string, std::string>> tables;
    std::unordered_map<std::string, bool> tablesMap;
    for (Uint32 i = 0; i < m_numOperations; i++) {
      PKRRequest *req = &m_key_ops[i].m_req;
      const char *db = req->DB();
      const char *table = req->Table();
      std::string key(std::string(db) + "|" + std::string(table));
      if (tablesMap.count(key) == 0) {
        tables.push_back(std::make_tuple(std::string(db),
                         std::string(table)));
        tablesMap[key] = true;
      }
    }
    HandleSchemaErrors(m_ndb_object, status, tables);
  }
  close_transaction();

  return RS_OK;
}
