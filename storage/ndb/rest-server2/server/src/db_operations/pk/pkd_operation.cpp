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

#include "pkd_operation.hpp"
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
#include <storage/ndb/include/ndbapi/NdbDictionary.hpp>
#include <kernel/ndb_limits.h>
#include <ArenaMalloc.hpp>
#include <util/require.h>
#include <EventLogger.hpp>

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_NDB_DEL 1
#endif

#ifdef DEBUG_NDB_DEL
#define DEB_NDB_DEL(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_NDB_DEL(...) do { } while (0)
#endif

BatchDeleteOperations::BatchDeleteOperations() : BaseBatchOperations() {
  m_key_ops = nullptr;
  m_has_blob_columns = false;
}

BatchDeleteOperations::~BatchDeleteOperations() {
  if (!m_isSuccess) {
    for (Uint32 i = 0; i < m_numOperations; i++) {
      m_key_ops[i].m_req.resetReadColumns();
    }
  }
}

RS_Status BatchDeleteOperations::allocate_key_ops(ArenaMalloc* amalloc, Uint32 numOps) {
  m_key_ops = amalloc->calloc<DeleteKeyOperation>(numOps);
  if (unlikely(m_key_ops == nullptr)) {
    return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
  }
  DEB_NDB_DEL("m_key_ops: %p, sizeof(DeleteKeyOperation): %u",
              m_key_ops, (Uint32)sizeof(DeleteKeyOperation));
  return RS_OK;
}

// init_batch_operations, setup_primary_keys, setup_transactions,
// close_transaction, and handle_ndb_error are inherited from BaseBatchOperations

/**
 * Setup blob handles for delete operations on tables with BLOB columns.
 * This detects BLOB/TEXT columns at the table level (not readColumns) and
 * prepares for automatic blob part deletion by NdbBlob.
 */
RS_Status BatchDeleteOperations::setup_table_blob_handles(ArenaMalloc *amalloc) {
  for (Uint32 i = 0; i < m_numOperations; i++) {
    DeleteKeyOperation *key_op = &m_key_ops[i];
    PKRRequest *req = &key_op->m_req;
    if (unlikely(req->IsInvalidOp())) {
      continue;
    }
    const NdbDictionary::Table *tableDict = key_op->m_tableDict;
    Uint32 numColumns = key_op->m_num_table_columns;

    // Count blob columns in the table
    Uint32 numBlobColumns = 0;
    for (Uint32 j = 0; j < numColumns; j++) {
      const NdbDictionary::Column *col = tableDict->getColumn(j);
      if (col->getType() == NdbDictionary::Column::Blob ||
          col->getType() == NdbDictionary::Column::Text) {
        numBlobColumns++;
      }
    }

    if (numBlobColumns > 0) {
      // Table has blob columns - allocate arrays
      m_has_blob_columns = true;
      m_single_transaction = true;
      key_op->m_num_blob_columns = numBlobColumns;

      key_op->m_blobColumns = (const NdbDictionary::Column**)
        amalloc->alloc_bytes(numBlobColumns * sizeof(NdbDictionary::Column*), 8);
      if (unlikely(key_op->m_blobColumns == nullptr)) {
        return RS_SERVER_ERROR(
            std::string(rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
      }

      key_op->m_blob_handles = (NdbBlob**)
        amalloc->alloc_bytes(numBlobColumns * sizeof(NdbBlob*), 8);
      if (unlikely(key_op->m_blob_handles == nullptr)) {
        return RS_SERVER_ERROR(
            std::string(rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
      }

      // Fill in blob column pointers
      Uint32 blobIdx = 0;
      for (Uint32 j = 0; j < numColumns; j++) {
        const NdbDictionary::Column *col = tableDict->getColumn(j);
        if (col->getType() == NdbDictionary::Column::Blob ||
            col->getType() == NdbDictionary::Column::Text) {
          key_op->m_blobColumns[blobIdx] = col;
          key_op->m_blob_handles[blobIdx] = nullptr;
          blobIdx++;
        }
      }
      DEB_NDB_DEL("Table %s has %u blob columns, op %u",
                  tableDict->getName(), numBlobColumns, i);
    }
  }
  return RS_OK;
}

/**
 * Set up delete operation using deleteTuple
 *
 * @return status
 */
RS_Status BatchDeleteOperations::setup_delete_operations() {
  for (Uint32 opIdx = m_first_key; opIdx < m_last_key; opIdx++) {
    DeleteKeyOperation *key_op = &m_key_ops[opIdx];
    PKRRequest *req = &key_op->m_req;
    if (unlikely(req->IsInvalidOp())) {
      continue;
    }
    NdbTransaction *trans = key_op->m_ndbTransaction;
    if (unlikely(m_single_transaction)) {
      trans = m_key_ops[0].m_ndbTransaction;
      require(trans);
    }
    DEB_NDB_DEL("deleteTuple: op[%u], num_read_columns: %u",
                opIdx, key_op->m_num_read_columns);
    NdbOperation::OperationOptions opts;
    std::memset(&opts, 0, sizeof(opts));
    opts.optionsPresent |= NdbOperation::OperationOptions::OO_BATCH_SAFE_FLAG;

    const NdbOperation *operation;
    if (key_op->m_num_read_columns > 0) {
      // Return deleted row data when readColumns is specified
      operation = trans->deleteTuple(
        key_op->m_ndb_record,
        (const char*)key_op->m_row,
        key_op->m_ndb_record,
        (char*)key_op->m_row,           // Result row buffer
        key_op->m_bitmap_read_columns,  // Result mask for columns to return
        &opts,
        sizeof(opts));
    } else {
      // No columns to return
      operation = trans->deleteTuple(
        key_op->m_ndb_record,
        (const char*)key_op->m_row,
        key_op->m_ndb_record,
        nullptr,  // No result row
        nullptr,  // No result mask
        &opts,
        sizeof(opts));
    }
    if (unlikely(operation == nullptr)) {
      return RS_RONDB_SERVER_ERROR(trans->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_DELETE_OPERATION_FAILED)));
    }
    key_op->m_ndbOperation = operation;

    // Get blob handles for tables with BLOB columns (activates NdbBlob for part deletion)
    if (unlikely(key_op->m_num_blob_columns > 0)) {
      for (Uint32 blobIdx = 0; blobIdx < key_op->m_num_blob_columns; blobIdx++) {
        const NdbDictionary::Column *col = key_op->m_blobColumns[blobIdx];
        key_op->m_blob_handles[blobIdx] = operation->getBlobHandle(col->getName());
        DEB_NDB_DEL("Blob handle for %s in delete op %u: %p",
                    col->getName(), opIdx, key_op->m_blob_handles[blobIdx]);
        if (unlikely(key_op->m_blob_handles[blobIdx] == nullptr)) {
          return RS_RONDB_SERVER_ERROR(trans->getNdbError(),
              std::string(rdrsErrorMessage(ERROR_DELETE_OPERATION_FAILED)) +
              " Failed to get blob handle for column: " + col->getName());
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

// execute() and create_response() are inherited from BaseBatchOperations
// append_op_recs and write_col_to_resp are inherited from BaseKeyOperation
// close_transaction and handle_ndb_error are inherited from BaseBatchOperations

RS_Status BatchDeleteOperations::perform_operation(
  ArenaMalloc *amalloc,
  Uint32 numOperations,
  bool is_batch,
  RS_Buffer *reqBuffer,
  RS_Buffer *respBuffer,
  Ndb *ndb_object) {

  DEB_NDB_DEL("init_batch_operations");
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
  DEB_NDB_DEL("setup_table_blob_handles");
  status = setup_table_blob_handles(amalloc);
  if (unlikely(status.http_code != SUCCESS)) {
    handle_ndb_error(status);
    return status;
  }
  DEB_NDB_DEL("setup_primary_keys");
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
    Uint32 max_keys = globalConfigs.internal.batchMaxSize;
    Uint32 keys_this_loop = std::min(max_keys, (numOps - start));
    last_key += keys_this_loop;
    Uint32 remaining_keys = numOps - last_key;
    if (last_key < numOps &&
        remaining_keys < min_keys_for_last_loop) {
      last_key += remaining_keys;
    }
    m_last_key = last_key;

    DEB_NDB_DEL("setup_transactions");
    status = setup_transactions();
    if (unlikely(status.http_code != SUCCESS)) {
      handle_ndb_error(status);
      return status;
    }
    DEB_NDB_DEL("setup_delete_operations");
    status = setup_delete_operations();
    if (unlikely(status.http_code != SUCCESS)) {
      handle_ndb_error(status);
      return status;
    }
    DEB_NDB_DEL("execute");
    status = execute();
    if (unlikely(status.http_code != SUCCESS)) {
      handle_ndb_error(status);
      return status;
    }
    m_first_key = m_last_key;
    m_num_sent_operations = 0;
  }
  DEB_NDB_DEL("create_response");
  status = create_response(respBuffer);
  if (unlikely(status.http_code != SUCCESS)) {
    handle_ndb_error(status);
    return status;
  }
  DEB_NDB_DEL("close_transaction");
  close_transaction();

  m_isSuccess = true;

  return RS_OK;
}
