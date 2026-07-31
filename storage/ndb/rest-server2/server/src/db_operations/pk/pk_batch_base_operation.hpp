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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PK_BATCH_BASE_OPERATION_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PK_BATCH_BASE_OPERATION_HPP_

#include "pk_base_operation.hpp"
#include "src/rdrs_dal.h"

#include <NdbApi.hpp>
#include <ArenaMalloc.hpp>

/**
 * Base class for batch operations (read and delete).
 * Contains common member variables and shared method implementations.
 */
class BaseBatchOperations {
 protected:
  Uint32 m_numOperations;
  Ndb *m_ndb_object;
  bool m_isBatch;
  bool m_single_transaction;
  bool m_user_rate_limits;
  Uint32 m_num_sent_operations;
  Uint32 m_first_key;
  Uint32 m_last_key;
  bool m_isSuccess;

  // Virtual methods that derived classes must implement
  virtual BaseKeyOperation* get_key_op(Uint32 i) = 0;
  virtual RS_Status allocate_key_ops(ArenaMalloc*, Uint32) = 0;
  virtual bool supports_blobs() const { return false; }
  virtual bool supports_read_all_columns() const { return false; }
  virtual NdbTransaction::ExecType get_single_transaction_exec_type() const {
    return NdbTransaction::Commit;  // Default for delete operations
  }

  // Shared implementations
  RS_Status init_batch_operations(ArenaMalloc *amalloc,
                                  Uint32 numOps,
                                  bool is_batch,
                                  RS_Buffer *reqBuffer,
                                  Ndb *ndb_object);

  RS_Status setup_primary_keys();
  RS_Status set_user_id(BaseKeyOperation *key_op,
                        const char *rate_limit_identity,
                        Uint32 rate_limit_identity_len);
  RS_Status setup_transactions(const char *rate_limit_identity,
                               Uint32 rate_limit_identity_len);
  RS_Status execute();
  RS_Status create_response(RS_Buffer *respBuffer);
  void close_transaction();
  RS_Status handle_ndb_error(RS_Status status);

 public:
  BaseBatchOperations();
  virtual ~BaseBatchOperations();
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PK_BATCH_BASE_OPERATION_HPP_
