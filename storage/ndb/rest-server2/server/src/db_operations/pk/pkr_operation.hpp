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
 
#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKR_OPERATION_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKR_OPERATION_HPP_

#include "pk_batch_base_operation.hpp"
#include "pkr_request.hpp"
#include "pkr_response.hpp"
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
 * KeyOperation inherits from BaseKeyOperation.
 * For read operations, m_blob_handles is allocated in setup_blob_handles for ops with blobs.
 */
struct KeyOperation : public BaseKeyOperation {
  // No additional fields needed - m_blob_handles is in BaseKeyOperation
};

class BatchKeyOperations : public BaseBatchOperations {
 private:
  struct KeyOperation *m_key_ops;

  // Implementation of virtual methods from BaseBatchOperations
  BaseKeyOperation* get_key_op(Uint32 i) override { return &m_key_ops[i]; }
  RS_Status allocate_key_ops(ArenaMalloc* amalloc, Uint32 numOps) override;
  bool supports_blobs() const override { return true; }
  bool supports_read_all_columns() const override { return true; }
  NdbTransaction::ExecType get_single_transaction_exec_type() const override {
    return NdbTransaction::NoCommit;  // Read needs NoCommit for blob handling
  }

 public:
   BatchKeyOperations();
   ~BatchKeyOperations();
   RS_Status perform_operation(ArenaMalloc*,
                               Uint32 numOps,
                               bool is_batch,
                               RS_Buffer *reqBuffer,
                               RS_Buffer *respBuffer,
                               Ndb *ndb_object);
   // Read-specific methods
   RS_Status setup_blob_handles(ArenaMalloc *amalloc);
   RS_Status setup_read_operations();
};
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKR_OPERATION_HPP_
