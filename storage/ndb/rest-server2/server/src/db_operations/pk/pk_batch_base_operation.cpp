/*
 * Copyright (C) 2023, 2026 Hopsworks AB
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

#include "pk_batch_base_operation.hpp"
#include "common.hpp"
#include "pkr_request.hpp"
#include "pkr_response.hpp"
#include "src/config_structs.hpp"
#include "src/encoding.hpp"
#include "src/error_strings.h"
#include "src/rdrs_const.h"
#include "src/rdrs_dal.h"
#include "src/status.hpp"
#include "my_compiler.h"

#include <list>
#include <tuple>
#include <string>
#include <unordered_map>
#include <storage/ndb/include/ndbapi/NdbDictionary.hpp>
#include <kernel/ndb_limits.h>
#include <util/require.h>
#include <EventLogger.hpp>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_BATCH_BASE 1
#endif

#ifdef DEBUG_BATCH_BASE
#define DEB_BATCH_BASE(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_BATCH_BASE(...) do { } while (0)
#endif

BaseBatchOperations::BaseBatchOperations() {
}

BaseBatchOperations::~BaseBatchOperations() {
  // Note: Cleanup of m_req.resetReadColumns() must be done in derived class
  // destructors since virtual get_key_op() cannot be called from base destructor
}

RS_Status
BaseBatchOperations::init_batch_operations(ArenaMalloc *amalloc,
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
  m_user_rate_limits = globalConfigs.rest.userRateLimits;

  // Allocate key operations array (derived class handles the type)
  status = allocate_key_ops(amalloc, numOps);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }

  for (Uint32 i = 0; i < numOps; i++) {
    BaseKeyOperation *key_op = get_key_op(i);
    key_op->m_ndbTransaction = nullptr;
    key_op->m_blob_handles = nullptr;
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
    DEB_BATCH_BASE("Request on DB: %s, Table: %s, op: %u, reqBuffer: %p",
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

    if (unlikely(numPrimaryKeys != req->PKColumnsCount())) {
      DEB_BATCH_BASE("numPrimaryKeys: %u, reqPKKeys: %u",
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

    // Extra validation for read operations
    if (supports_read_all_columns()) {
      if (unlikely(numColumns < req->ReadColumnsCount())) {
        status = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_TOO_MANY_COLUMNS)));
        req->MarkInvalidOp(status);
        return status;
      }
    }

    Uint32 num_bitmap_words = (numColumns + 31) / 32;
    Uint32 num_bitmap_bytes = 4 * num_bitmap_words;
    Uint8* bitmap_words = (Uint8*)amalloc->alloc_bytes(num_bitmap_bytes, 4);
    key_op->m_bitmap_read_columns = bitmap_words;
    Uint32 row_len = NdbDictionary::getRecordRowLength(ndb_record);
    Uint32 row_len_aligned = ((row_len + 7) / 8) * 8;
    Uint8* row = (Uint8*)amalloc->alloc_bytes(row_len_aligned, 8);
    /* Ensure no halfwritten words distort the rows for pk */
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
    } else if (supports_read_all_columns()) {
      // For read operations with no columns specified, allocate for all columns
      readCols = (const NdbDictionary::Column**)
        amalloc->alloc_bytes(numColumns *
          sizeof(NdbDictionary::Column*), 8);
    }
    key_op->m_readColumns = readCols;

    // Check memory allocation
    if (unlikely(bitmap_words == nullptr ||
                 pkCols == nullptr ||
                 row == nullptr)) {
      status = RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
      return status;
    }
    // For read operations, also check readCols
    if (supports_read_all_columns() && unlikely(readCols == nullptr)) {
      status = RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
      return status;
    }

    Uint32 pk_bitmap_words[MAX_ATTRIBUTES_IN_TABLE/32];
    memset(bitmap_words, 0, num_bitmap_bytes);
    memset(pk_bitmap_words, 0, num_bitmap_bytes);
    Uint32 failed = 0;
    Uint32 j = 0;
    DEB_BATCH_BASE("Start setting up %u primary keys", numPrimaryKeys);
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

    // Handle read columns validation
    if (numReadColumns != 0) {
      j = 0;
      DEB_BATCH_BASE("Validating numReadColumns: %u", numReadColumns);
      for (; j < numReadColumns; j++) {
        std::string_view col_name(req->ReadColumnName(j),
                                  req->ReadColumnNameLen(j));
        const NdbDictionary::Column *read_col =
          tableDict->getColumn(col_name);
        DEB_BATCH_BASE("Try to find column %s, with len: %u",
          col_name.data(), (Uint32)col_name.size());
        if (unlikely(read_col == nullptr)) {
          failed = 1;
          break;
        }
        // Reject BLOB/TEXT columns for operations that don't support blobs (e.g., delete)
        if (!supports_blobs() &&
            (read_col->getType() == NdbDictionary::Column::Blob ||
             read_col->getType() == NdbDictionary::Column::Text)) {
          failed = 3;
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
      }
      if (unlikely(failed != 0)) {
        RS_Status err;
        if (failed == 1) {
          err = RS_CLIENT_404_WITH_MSG_ERROR(
            std::string(rdrsErrorMessage(ERROR_COLUMN_NOT_EXIST)) +
            std::string(" Database: ") + std::string(req->DB()) +
            std::string(" Table: ") + std::string(req->Table()) +
            std::string(" Column: ") + std::string(req->ReadColumnName(j)));
        } else if (failed == 2) {
          err = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) +
            std::string(req->ReadColumnName(j)));
        } else {
          // failed == 3: BLOB/TEXT not supported for this operation
          err = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_UNSUPPORTED_BLOB_TEXT_READ)) +
            std::string(" Column: ") + std::string(req->ReadColumnName(j)));
        }
        if (m_isBatch) {
          req->MarkInvalidOp(err);
          continue;
        }
        return err;
      }
    } else if (supports_read_all_columns()) {
      // For read operations: when no columns specified, read all columns
      DEB_BATCH_BASE("Start reading all columns: %u", numColumns);
      for (Uint32 k = 0; k < numColumns; k++) {
        const NdbDictionary::Column *read_col = tableDict->getColumn(k);
        key_op->m_readColumns[k] = read_col;
        DEB_BATCH_BASE("Read column, id: %u, name: %s", k, read_col->getName());
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

RS_Status BaseBatchOperations::setup_primary_keys() {
  for (Uint32 opIdx = 0; opIdx < m_numOperations; opIdx++) {
    BaseKeyOperation *key_op = get_key_op(opIdx);
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
      DEB_BATCH_BASE("First words of row is: 0x%x, op: %u",
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

RS_Status BaseBatchOperations::set_user_id(PKRRequest *req,
                                           BaseKeyOperation *key_op,
                                           char *username_ptr) {
  /**
   * We need to concatenate database name and username here
   */
  const char *project_name = req->DB();
  Uint32 project_name_len =
    strnlen(project_name, PROJECT_PROJECTNAME_SIZE);
  Uint32 username_len = strnlen(username_ptr, USERNAME_SIZE);
  memmove(username_ptr + project_name_len,
          username_ptr,
          username_len + 1);
  memcpy(username_ptr, project_name, project_name_len);
  username_len += project_name_len;
  if (key_op->m_ndbTransaction->setUserId(username_ptr,
                                          username_len) != 0) {
    m_ndb_object->closeTransaction(key_op->m_ndbTransaction);
    key_op->m_ndbTransaction = nullptr;
    return RS_RONDB_SERVER_ERROR(m_ndb_object->getNdbError(), 
      std::string(rdrsErrorMessage(ERROR_TRANSACTION_START_FAILED)));
  }
  return RS_OK;
}

RS_Status BaseBatchOperations::setup_transactions(char *username_ptr) {
  Uint32 tmp[MAX_KEY_SIZE_IN_WORDS * MAX_XFRM_MULTIPLY];
  char *buf = (char *)&tmp[0];
  PKRRequest *req = nullptr;
  if (unlikely(m_single_transaction)) {
    BaseKeyOperation *key_op = get_key_op(0);
    if (key_op->m_ndbTransaction != nullptr) {
      /* Transaction started already in previous loop */
      return RS_OK;
    }
    BaseKeyOperation *first_key_op = nullptr;
    for (Uint32 i = m_first_key; i < m_last_key; i++) {
      first_key_op = get_key_op(i);
      req = &first_key_op->m_req;
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
      if (m_user_rate_limits && username_ptr) {
        RS_Status status = set_user_id(req, key_op, username_ptr);
        if (unlikely(status.http_code != SUCCESS)) {
          return status;
        }
      }
    }
  } else {
    for (Uint32 i = m_first_key; i < m_last_key; i++) {
      BaseKeyOperation *key_op = get_key_op(i);
      req = &key_op->m_req;
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
      if (m_user_rate_limits && username_ptr) {
        /**
         * We need to concatenate database name and username here
         */
        RS_Status status = set_user_id(req, key_op, username_ptr);
        if (unlikely(status.http_code != SUCCESS)) {
          return status;
        }
      }
    }
  }
  return RS_OK;
}

RS_Status BaseBatchOperations::execute() {
  if (m_num_sent_operations > 0) {
    if (unlikely(m_single_transaction)) {
      BaseKeyOperation *key_op = get_key_op(0);
      NdbTransaction *trans = key_op->m_ndbTransaction;
      // Use AO_IgnoreError to allow transaction to continue even if some
      // operations fail (e.g., 626 NoDataFound). Each operation's error
      // is checked individually in create_response().
      if (unlikely(trans->execute(get_single_transaction_exec_type(),
                                  NdbOperation::AO_IgnoreError) != 0)) {
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

RS_Status BaseBatchOperations::create_response(RS_Buffer *respBuffs) {
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
    BaseKeyOperation *key_op = get_key_op(i);
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
    // For read operations with no columns specified, fill in all column names
    if (supports_read_all_columns() && req->ReadColumnsCount() == 0) {
      DEB_BATCH_BASE("Build request when all columns requested");
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
    NdbError::Classification op_error = op->getNdbError().classification;
    // For multi-transaction mode, the error may be at transaction level rather
    // than operation level. Check transaction error if operation shows no error.
    // (In single transaction mode with AO_IgnoreError, operation errors are set
    // correctly for each operation.)
    if (unlikely(!m_single_transaction && op_error == NdbError::NoError)) {
      NdbTransaction *trans = key_op->m_ndbTransaction;
      if (trans->getNdbError().classification == NdbError::NoDataFound) {
        op_error = NdbError::NoDataFound;
      }
    }
    if (likely(op_error == NdbError::NoError)) {
      resp->SetStatus(SUCCESS, "OK");
    } else if (op_error == NdbError::NoDataFound) {
      found = false;
      resp->SetStatus(NOT_FOUND, "NOT Found");
    } else {
      //  immediately fail the entire batch
      resp->SetStatus(SERVER_ERROR, op->getNdbError().message);
      resp->Close(response_length);
      return RS_RONDB_SERVER_ERROR(
        op->getNdbError(), std::string("SubOperation ") +
        std::string(req->OperationId()
                    ? req->OperationId()
                    : "(Unidentified Operation)") +
        std::string(" failed"));
    }
    // Append column data if found and columns are requested
    if (likely(found) && key_op->m_num_read_columns > 0) {
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

void BaseBatchOperations::close_transaction() {
  if (unlikely(m_single_transaction)) {
    BaseKeyOperation *key_op = get_key_op(0);
    if (key_op->m_ndbTransaction != nullptr) {
      m_ndb_object->closeTransaction(key_op->m_ndbTransaction);
    }
  } else {
    for (Uint32 i = 0; i < m_numOperations; i++) {
      BaseKeyOperation *key_op = get_key_op(i);
      if (key_op->m_ndbTransaction != nullptr) {
        m_ndb_object->closeTransaction(key_op->m_ndbTransaction);
      }
    }
  }
}

RS_Status BaseBatchOperations::handle_ndb_error(RS_Status status) {
  // schema errors
  if (UnloadSchema(status)) {
    // no idea which sub-operation threw the error
    // unload all tables used in this operation
    std::list<std::tuple<std::string, std::string>> tables;
    std::unordered_map<std::string, bool> tablesMap;
    for (Uint32 i = 0; i < m_numOperations; i++) {
      BaseKeyOperation *key_op = get_key_op(i);
      PKRRequest *req = &key_op->m_req;
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
