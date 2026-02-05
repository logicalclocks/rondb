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

#include "pkw_operation.hpp"
#include "NdbBlob.hpp"
#include "NdbOperation.hpp"
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
#include <algorithm>
#include <tuple>
#include <utility>
#include <cstring>
#include <storage/ndb/include/ndbapi/NdbDictionary.hpp>
#include <kernel/ndb_limits.h>
#include <ArenaMalloc.hpp>
#include <util/require.h>
#include <EventLogger.hpp>
#include <libbase64.h>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_NDB_WRITE 1
#endif

#ifdef DEBUG_NDB_WRITE
#define DEB_NDB_WRITE(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_NDB_WRITE(...) do { } while (0)
#endif

BatchWriteOperations::BatchWriteOperations() : BaseBatchOperations() {
  m_key_ops = nullptr;
}

BatchWriteOperations::~BatchWriteOperations() {
  if (!m_isSuccess) {
    for (Uint32 i = 0; i < m_numOperations; i++) {
      m_key_ops[i].m_req.resetReadColumns();
    }
  }
}

RS_Status BatchWriteOperations::allocate_key_ops(ArenaMalloc* amalloc, Uint32 numOps) {
  // Use alloc instead of calloc because WriteKeyOperation contains non-trivial
  // types (PKRRequest, PKRResponse) that cannot be memset-initialized
  m_key_ops = amalloc->alloc<WriteKeyOperation>(numOps);
  if (unlikely(m_key_ops == nullptr)) {
    return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
  }
  // Initialize write-specific fields (base fields initialized in init_batch_operations)
  for (Uint32 i = 0; i < numOps; i++) {
    m_key_ops[i].m_writeColumns = nullptr;
    m_key_ops[i].m_num_write_columns = 0;
    m_key_ops[i].m_bitmap_write_columns = nullptr;
    m_key_ops[i].m_write_blob_handles = nullptr;
    m_key_ops[i].m_has_write_blobs = false;
    m_key_ops[i].m_write_op_type = RDRS_WRITE_OP_WRITE;
  }
  DEB_NDB_WRITE("m_key_ops: %p, sizeof(WriteKeyOperation): %u",
              m_key_ops, (Uint32)sizeof(WriteKeyOperation));
  return RS_OK;
}

/**
 * Setup write columns for write operations.
 * Reads the write columns from the request and looks up the column dictionary objects.
 * Also detects BLOB/TEXT columns and allocates blob handle arrays.
 */
RS_Status BatchWriteOperations::setup_write_columns(ArenaMalloc *amalloc) {
  for (Uint32 i = 0; i < m_numOperations; i++) {
    WriteKeyOperation *key_op = &m_key_ops[i];
    PKRRequest *req = &key_op->m_req;
    if (unlikely(req->IsInvalidOp())) {
      continue;
    }
    const NdbDictionary::Table *tableDict = key_op->m_tableDict;
    Uint32 numWriteColumns = req->WriteColumnsCount();
    Uint32 numTableColumns = key_op->m_num_table_columns;

    if (numWriteColumns == 0) {
      // No write columns specified - this is an error for write operations
      req->MarkInvalidOp(RS_CLIENT_ERROR(
        std::string("No write columns specified for write operation")));
      continue;
    }

    key_op->m_num_write_columns = numWriteColumns;
    key_op->m_has_write_blobs = false;
    key_op->m_write_op_type = req->WriteOperationType();
    DEB_NDB_WRITE("Op[%u] write_op_type: %u", i, key_op->m_write_op_type);

    // Allocate write columns array
    key_op->m_writeColumns = (const NdbDictionary::Column**)
      amalloc->alloc_bytes(numWriteColumns * sizeof(NdbDictionary::Column*), 8);
    if (unlikely(key_op->m_writeColumns == nullptr)) {
      return RS_SERVER_ERROR(
          std::string(rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
    }

    // Allocate bitmap for write columns
    Uint32 bitmapSize = (numTableColumns + 7) / 8;
    key_op->m_bitmap_write_columns = (Uint8*)amalloc->alloc_bytes(bitmapSize, 8);
    if (unlikely(key_op->m_bitmap_write_columns == nullptr)) {
      return RS_SERVER_ERROR(
          std::string(rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
    }
    memset(key_op->m_bitmap_write_columns, 0, bitmapSize);

    // First pass: look up columns and check for BLOBs
    bool hasBlobs = false;
    for (Uint32 j = 0; j < numWriteColumns; j++) {
      const char *colName = req->WriteColumnName(j);
      const NdbDictionary::Column *col = tableDict->getColumn(colName);
      if (unlikely(col == nullptr)) {
        req->MarkInvalidOp(RS_CLIENT_ERROR(
          std::string("Write column not found: ") + std::string(colName)));
        break;
      }

      // Check if this is a primary key column - we shouldn't allow writing to PK columns
      if (unlikely(col->getPrimaryKey())) {
        req->MarkInvalidOp(RS_CLIENT_ERROR(
          std::string("Cannot write to primary key column: ") + std::string(colName)));
        break;
      }

      key_op->m_writeColumns[j] = col;

      // Set bit in write bitmap
      Uint32 colNo = col->getColumnNo();
      key_op->m_bitmap_write_columns[colNo / 8] |= (1 << (colNo % 8));

      // Check if this is a BLOB/TEXT column
      if (col->getType() == NdbDictionary::Column::Blob ||
          col->getType() == NdbDictionary::Column::Text) {
        hasBlobs = true;
        DEB_NDB_WRITE("Write column[%u]: %s is BLOB/TEXT, colNo: %u",
                      j, colName, colNo);
      } else {
        DEB_NDB_WRITE("Write column[%u]: %s, colNo: %u", j, colName, colNo);
      }
    }

    if (unlikely(req->IsInvalidOp())) {
      continue;
    }

    // If we have BLOB columns, allocate blob handle array and set single transaction mode
    if (hasBlobs) {
      key_op->m_has_write_blobs = true;
      m_single_transaction = true;

      key_op->m_write_blob_handles = (NdbBlob**)
        amalloc->alloc_bytes(numWriteColumns * sizeof(NdbBlob*), 8);
      if (unlikely(key_op->m_write_blob_handles == nullptr)) {
        return RS_SERVER_ERROR(
            std::string(rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
      }
      // Initialize all to nullptr
      for (Uint32 j = 0; j < numWriteColumns; j++) {
        key_op->m_write_blob_handles[j] = nullptr;
      }
      DEB_NDB_WRITE("Allocated blob handles for op %u, m_single_transaction=true", i);
    }
  }
  return RS_OK;
}

/**
 * Set up write operation using writeTuple
 *
 * @return status
 */
RS_Status BatchWriteOperations::setup_write_operations() {
  for (Uint32 opIdx = m_first_key; opIdx < m_last_key; opIdx++) {
    WriteKeyOperation *key_op = &m_key_ops[opIdx];
    PKRRequest *req = &key_op->m_req;
    if (unlikely(req->IsInvalidOp())) {
      continue;
    }

    // Add primary key columns to the write bitmap.
    // writeTuple needs to know all columns with valid data in the row buffer.
    for (Uint32 j = 0; j < key_op->m_num_pk_columns; j++) {
      const NdbDictionary::Column *col = key_op->m_pkColumns[j];
      Uint32 colNo = col->getColumnNo();
      key_op->m_bitmap_write_columns[colNo / 8] |= (1 << (colNo % 8));
      DEB_NDB_WRITE("Added PK column to write bitmap: %s, colNo: %u",
                    col->getName(), colNo);
    }

    // Set non-BLOB write column values in the row buffer
    for (Uint32 j = 0; j < key_op->m_num_write_columns; j++) {
      const NdbDictionary::Column *col = key_op->m_writeColumns[j];

      // Skip BLOB/TEXT columns - they're handled separately via NdbBlob
      if (col->getType() == NdbDictionary::Column::Blob ||
          col->getType() == NdbDictionary::Column::Text) {
        DEB_NDB_WRITE("Skipping BLOB/TEXT column %s in row buffer setup",
                      col->getName());
        continue;
      }

      RS_Status status = set_operation_write_col(col, req, key_op->m_row,
                                                 key_op->m_ndb_record, j);
      if (unlikely(status.http_code != SUCCESS)) {
        req->MarkInvalidOp(status);
        break;
      }
    }

    if (unlikely(req->IsInvalidOp())) {
      continue;
    }

    NdbTransaction *trans = key_op->m_ndbTransaction;
    if (unlikely(m_single_transaction)) {
      trans = m_key_ops[0].m_ndbTransaction;
      require(trans);
    }

    DEB_NDB_WRITE("tuple op: op[%u], type: %u, num_write_columns: %u, has_blobs: %d",
                opIdx, key_op->m_write_op_type, key_op->m_num_write_columns,
                key_op->m_has_write_blobs);

    NdbOperation::OperationOptions opts;
    std::memset(&opts, 0, sizeof(opts));
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_BATCH_SAFE_FLAG;

    // Use the appropriate tuple method based on operation type
    const NdbOperation *operation = nullptr;
    switch (key_op->m_write_op_type) {
      case RDRS_WRITE_OP_UPDATE:
        // updateTuple - update only, fails if row doesn't exist
        operation = trans->updateTuple(
          key_op->m_ndb_record,
          (const char*)key_op->m_row,     // Primary key values
          key_op->m_ndb_record,
          (const char*)key_op->m_row,     // Write column values
          key_op->m_bitmap_write_columns, // Mask for columns to update
          &opts,
          sizeof(opts));
        break;
      case RDRS_WRITE_OP_INSERT:
        // insertTuple - insert only, fails if row already exists
        operation = trans->insertTuple(
          key_op->m_ndb_record,
          (const char*)key_op->m_row,     // All column values
          key_op->m_bitmap_write_columns, // Mask for columns to insert
          &opts,
          sizeof(opts));
        break;
      case RDRS_WRITE_OP_WRITE:
      default:
        // writeTuple - insert if row doesn't exist, update if it does
        operation = trans->writeTuple(
          key_op->m_ndb_record,
          (const char*)key_op->m_row,     // Primary key values
          key_op->m_ndb_record,
          (const char*)key_op->m_row,     // Write column values
          key_op->m_bitmap_write_columns, // Mask for columns to write
          &opts,
          sizeof(opts));
        break;
    }

    if (unlikely(operation == nullptr)) {
      const char *opName = (key_op->m_write_op_type == RDRS_WRITE_OP_UPDATE) ? "update" :
                           (key_op->m_write_op_type == RDRS_WRITE_OP_INSERT) ? "insert" : "write";
      return RS_RONDB_SERVER_ERROR(trans->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_WRITE_OPERATION_FAILED)) +
          " Operation type: " + opName);
    }
    key_op->m_ndbOperation = operation;

    // Handle BLOB columns: get blob handles and set values
    if (unlikely(key_op->m_has_write_blobs)) {
      for (Uint32 j = 0; j < key_op->m_num_write_columns; j++) {
        const NdbDictionary::Column *col = key_op->m_writeColumns[j];

        if (col->getType() == NdbDictionary::Column::Blob ||
            col->getType() == NdbDictionary::Column::Text) {
          // Get blob handle
          NdbBlob *blobHandle = operation->getBlobHandle(col->getName());
          if (unlikely(blobHandle == nullptr)) {
            return RS_RONDB_SERVER_ERROR(trans->getNdbError(),
                std::string(rdrsErrorMessage(ERROR_WRITE_OPERATION_FAILED)) +
                " Failed to get blob handle for column: " + col->getName());
          }
          key_op->m_write_blob_handles[j] = blobHandle;

          // Get the value from the request
          const char *valueCStr = req->WriteColumnValueCStr(j);
          Uint32 valueLen = req->WriteColumnValueLen(j);

          DEB_NDB_WRITE("Setting BLOB/TEXT column %s, valueLen: %u",
                        col->getName(), valueLen);

          if (col->getType() == NdbDictionary::Column::Text) {
            // TEXT column: value is the text directly
            if (blobHandle->setValue(valueCStr, valueLen) != 0) {
              return RS_RONDB_SERVER_ERROR(blobHandle->getNdbError(),
                  std::string(rdrsErrorMessage(ERROR_WRITE_OPERATION_FAILED)) +
                  " Failed to set TEXT value for column: " + col->getName());
            }
          } else {
            // BLOB column: value is base64 encoded, need to decode
            // Allocate buffer for decoded data (decoded size is at most 3/4 of encoded)
            size_t maxDecodedSize = (valueLen * 3) / 4 + 4;
            char *decodedBuffer = (char*)alloca(maxDecodedSize);
            size_t decodedLen = 0;

            int result = base64_decode(valueCStr, valueLen,
                                       decodedBuffer, &decodedLen, 0);
            if (unlikely(result == 0)) {
              return RS_CLIENT_ERROR(
                std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
                " Failed to decode base64 for BLOB column: " + col->getName());
            } else if (unlikely(result == -1)) {
              return RS_CLIENT_ERROR(
                std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
                " Base64 codec not available for BLOB column: " + col->getName());
            }

            if (blobHandle->setValue(decodedBuffer, decodedLen) != 0) {
              return RS_RONDB_SERVER_ERROR(blobHandle->getNdbError(),
                  std::string(rdrsErrorMessage(ERROR_WRITE_OPERATION_FAILED)) +
                  " Failed to set BLOB value for column: " + col->getName());
            }
            DEB_NDB_WRITE("BLOB column %s: decoded %zu bytes from %u encoded bytes",
                          col->getName(), decodedLen, valueLen);
          }
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

RS_Status BatchWriteOperations::perform_operation(
  ArenaMalloc *amalloc,
  Uint32 numOperations,
  bool is_batch,
  RS_Buffer *reqBuffer,
  RS_Buffer *respBuffer,
  Ndb *ndb_object,
  char *username_ptr) {

  DEB_NDB_WRITE("init_batch_operations");
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
  DEB_NDB_WRITE("setup_write_columns");
  status = setup_write_columns(amalloc);
  if (unlikely(status.http_code != SUCCESS)) {
    handle_ndb_error(status);
    return status;
  }
  DEB_NDB_WRITE("setup_primary_keys");
  status = setup_primary_keys();
  if (unlikely(status.http_code != SUCCESS)) {
    handle_ndb_error(status);
    return status;
  }
  m_first_key = 0;
  m_last_key = 0;
  Uint32 last_key = 0;
  static const Uint32 min_keys_for_last_loop = 50;
  while (m_last_key < m_numOperations) {
    Uint32 numOps = m_numOperations;
    Uint32 start = m_first_key;
    Uint32 max_keys = globalConfigs.internal.batchMaxSize;
    Uint32 keys_this_loop = std::min(max_keys, (numOps - start));
    last_key += keys_this_loop;
    Uint32 remaining_keys = numOps - last_key;
    if (last_key < numOps && remaining_keys < min_keys_for_last_loop) {
      last_key += remaining_keys;
    }
    m_last_key = last_key;

    DEB_NDB_WRITE("setup_transactions");
    status = setup_transactions(username_ptr);
    if (unlikely(status.http_code != SUCCESS)) {
      handle_ndb_error(status);
      return status;
    }
    DEB_NDB_WRITE("setup_write_operations");
    status = setup_write_operations();
    if (unlikely(status.http_code != SUCCESS)) {
      handle_ndb_error(status);
      return status;
    }
    DEB_NDB_WRITE("execute");
    status = execute();
    if (unlikely(status.http_code != SUCCESS)) {
      handle_ndb_error(status);
      return status;
    }
    m_first_key = m_last_key;
    m_num_sent_operations = 0;
  }
  DEB_NDB_WRITE("create_response");
  status = create_response(respBuffer);
  if (unlikely(status.http_code != SUCCESS)) {
    handle_ndb_error(status);
    return status;
  }
  DEB_NDB_WRITE("close_transaction");
  close_transaction();

  m_isSuccess = true;

  return RS_OK;
}
