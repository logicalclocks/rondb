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

BatchKeyOperations::BatchKeyOperations() : BaseBatchOperations() {
  m_key_ops = nullptr;
}

BatchKeyOperations::~BatchKeyOperations() {
  if (!m_isSuccess) {
    for (Uint32 i = 0; i < m_numOperations; i++) {
      m_key_ops[i].m_req.resetReadColumns();
    }
  }
}

RS_Status BatchKeyOperations::allocate_key_ops(ArenaMalloc* amalloc, Uint32 numOps) {
  m_key_ops = amalloc->alloc<KeyOperation>(numOps);
  if (unlikely(m_key_ops == nullptr)) {
    return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
  }
  DEB_NDB_BE("m_key_ops: %p, sizeof(KeyOperation): %u",
             m_key_ops, (Uint32)sizeof(KeyOperation));
  // m_blob_handles is initialized to nullptr in base class init_batch_operations
  return RS_OK;
}

// init_batch_operations, setup_primary_keys, setup_transactions,
// close_transaction, and handle_ndb_error are inherited from BaseBatchOperations

/**
 * Setup blob handles for read operations.
 * Called after base init_batch_operations to detect Blob/Text columns
 * and allocate blob handle arrays.
 */
RS_Status BatchKeyOperations::setup_blob_handles(ArenaMalloc *amalloc) {
  for (Uint32 i = 0; i < m_numOperations; i++) {
    KeyOperation *key_op = &m_key_ops[i];
    PKRRequest *req = &key_op->m_req;
    if (unlikely(req->IsInvalidOp())) {
      continue;
    }
    Uint32 numReadColumns = key_op->m_num_read_columns;
    for (Uint32 j = 0; j < numReadColumns; j++) {
      const NdbDictionary::Column *col = key_op->m_readColumns[j];
      if (col->getType() == NdbDictionary::Column::Blob ||
          col->getType() == NdbDictionary::Column::Text) {
        // Found a blob column - allocate blob handles array if not already done
        if (key_op->m_blob_handles == nullptr) {
          m_single_transaction = true;
          key_op->m_blob_handles = (NdbBlob**)
            amalloc->alloc_bytes(sizeof(NdbBlob*) * numReadColumns, 8);
          if (unlikely(key_op->m_blob_handles == nullptr)) {
            return RS_SERVER_ERROR(
                std::string(rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
          }
          DEB_NDB_BE("Allocating memory at %p for m_key_ops[%u].m_blob_handles",
                     key_op->m_blob_handles, i);
        }
        break;  // Only need to allocate once per operation
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

// execute() and create_response() are inherited from BaseBatchOperations
// append_op_recs and write_col_to_resp are now in BaseKeyOperation
// (see pk_base_operation.cpp)

// close_transaction and handle_ndb_error are inherited from BaseBatchOperations

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
  DEB_NDB_BE("setup_blob_handles");
  status = setup_blob_handles(amalloc);
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
