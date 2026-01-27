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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKD_OPERATION_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKD_OPERATION_HPP_

#include "pk_batch_base_operation.hpp"
#include "common.hpp"
#include "src/rdrs_dal.h"

#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <vector>
#include <NdbApi.hpp>
#include <ArenaMalloc.hpp>

/**
 * DeleteKeyOperation inherits from BaseKeyOperation.
 * For tables with BLOB columns, m_blob_handles is allocated in setup_table_blob_handles.
 * Note: m_blobColumns stores table's blob columns (for part deletion),
 * while m_readColumns stores requested read columns (may or may not include blobs).
 */
struct DeleteKeyOperation : public BaseKeyOperation {
  // Blob columns in the table (for automatic blob part deletion)
  const NdbDictionary::Column **m_blobColumns;
  Uint32 m_num_blob_columns;
};

class BatchDeleteOperations : public BaseBatchOperations {
 private:
  struct DeleteKeyOperation *m_key_ops;
  bool m_has_blob_columns;  // True if any table in batch has BLOB columns

  // Implementation of virtual methods from BaseBatchOperations
  BaseKeyOperation* get_key_op(Uint32 i) override { return &m_key_ops[i]; }
  RS_Status allocate_key_ops(ArenaMalloc* amalloc, Uint32 numOps) override;
  // Delete operations don't support reading blobs in response (keeps default false)
  // but we do handle blob part deletion at table level.
  // NdbBlob state machine runs internally during execute(Commit).

 public:
   BatchDeleteOperations();
   ~BatchDeleteOperations();
   RS_Status perform_operation(ArenaMalloc*,
                               Uint32 numOps,
                               bool is_batch,
                               RS_Buffer *reqBuffer,
                               RS_Buffer *respBuffer,
                               Ndb *ndb_object,
                               char *username_ptr);
   // Delete-specific methods
   RS_Status setup_table_blob_handles(ArenaMalloc *amalloc);
   RS_Status setup_delete_operations();
};
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKD_OPERATION_HPP_
