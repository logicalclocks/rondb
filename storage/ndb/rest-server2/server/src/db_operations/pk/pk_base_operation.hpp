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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PK_BASE_OPERATION_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PK_BASE_OPERATION_HPP_

#include "pkr_request.hpp"
#include "pkr_response.hpp"
#include "src/rdrs_dal.h"

#include <stdint.h>
#include <NdbApi.hpp>

/**
 * Base struct for key operations (read and delete).
 * Contains common fields and methods shared between KeyOperation and
 * DeleteKeyOperation.
 */
struct BaseKeyOperation {
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
  const NdbRecord *m_ndb_record;
  PKRRequest m_req;
  PKRResponse m_resp;
  NdbBlob **m_blob_handles;  // nullptr for delete operations

  // Shared methods for column data handling
  RS_Status append_op_recs(PKRResponse *resp, PKRRequest *req);
  RS_Status write_col_to_resp(Uint32 colIdx,
                              PKRResponse *resp,
                              PKRRequest *req);

  NdbBlob* get_blob_handle(Uint32 colIdx) {
    return m_blob_handles ? m_blob_handles[colIdx] : nullptr;
  }
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PK_BASE_OPERATION_HPP_
