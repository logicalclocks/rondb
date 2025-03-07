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
 
#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKR_OPERATION_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKR_OPERATION_HPP_

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

struct KeyOperation {
  Uint32 m_num_pk_columns;
  Uint32 m_num_read_columns;
  Uint32 m_num_table_columns;
  Uint8 *m_bitmap_read_columns;
  Uint8 *m_row;
  NdbTransaction *m_ndbTransaction;
  const NdbOperation *m_ndbOperation;
  const NdbDictionary::Table *m_tableDict;
  const NdbDictionary::Column **m_pkColumns;
  const NdbDictionary::Column **m_readColumns;
  NdbBlob **m_blob_handles;
  const NdbRecord *m_ndb_record;
  PKRRequest m_req;
  PKRResponse m_resp;
  RS_Status append_op_recs(PKRResponse *resp, PKRRequest *req);
  RS_Status write_col_to_resp(Uint32 colIdx,
                              PKRResponse *resp,
                              PKRRequest *req);
};

class BatchKeyOperations {
 private:
  Uint32 m_numOperations;
  Ndb *m_ndb_object;
  bool m_isBatch;
  bool m_single_transaction;
  struct KeyOperation *m_key_ops;
  Uint32 m_num_sent_operations;
 public:
   BatchKeyOperations();
   ~BatchKeyOperations();
   RS_Status perform_operation(ArenaMalloc*,
                               Uint32 numOps,
                               bool is_batch,
                               RS_Buffer *reqBuffer,
                               RS_Buffer *respBuffer,
                               Ndb *ndb_object);
   RS_Status init_batch_operations(ArenaMalloc*,
                                   Uint32,
                                   bool is_batch,
                                   RS_Buffer *reqBuffer,
                                   Ndb *ndb_object);
   RS_Status setup_transaction();
   RS_Status setup_read_operation();
   RS_Status execute();
   RS_Status create_response(RS_Buffer *respBuffer);
   RS_Status append_op_recs(Uint32);
   void close_transaction();
   RS_Status abort_request();
   RS_Status handle_ndb_error(RS_Status);
};
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKR_OPERATION_HPP_
