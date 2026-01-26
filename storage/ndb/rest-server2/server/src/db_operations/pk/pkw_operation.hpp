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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKW_OPERATION_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKW_OPERATION_HPP_

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
 * WriteKeyOperation inherits from BaseKeyOperation.
 * Contains additional fields for write columns (columns to update).
 */
struct WriteKeyOperation : public BaseKeyOperation {
  // Write columns - columns to be written/updated
  const NdbDictionary::Column **m_writeColumns;
  Uint32 m_num_write_columns;
  // Bitmap for write columns (which columns to write in writeTuple)
  Uint8 *m_bitmap_write_columns;
  // Blob handles for write columns (indexed by write column index)
  NdbBlob **m_write_blob_handles;
  // Flag indicating if any write column is a BLOB/TEXT
  bool m_has_write_blobs;
  // Write operation type: RDRS_WRITE_OP_WRITE, _UPDATE, or _INSERT
  Uint32 m_write_op_type;
};

class BatchWriteOperations : public BaseBatchOperations {
 private:
  struct WriteKeyOperation *m_key_ops;

  // Implementation of virtual methods from BaseBatchOperations
  BaseKeyOperation* get_key_op(Uint32 i) override { return &m_key_ops[i]; }
  RS_Status allocate_key_ops(ArenaMalloc* amalloc, Uint32 numOps) override;

 public:
   BatchWriteOperations();
   ~BatchWriteOperations();
   RS_Status perform_operation(ArenaMalloc*,
                               Uint32 numOps,
                               bool is_batch,
                               RS_Buffer *reqBuffer,
                               RS_Buffer *respBuffer,
                               Ndb *ndb_object,
                               char *username_ptr);
   // Write-specific methods
   RS_Status setup_write_columns(ArenaMalloc *amalloc);
   RS_Status setup_write_operations();
};
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKW_OPERATION_HPP_
