/*
 * Copyright (C) 2022 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_DB_OPERATIONS_PK_COMMON_HPP_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_DB_OPERATIONS_PK_COMMON_HPP_

#include <NdbDictionary.hpp>
#include "src/rdrs-dal.h"
#include <my_time.h>
#include "src/db-operations/pk/pkr-request.hpp"
#include "src/db-operations/pk/pkr-response.hpp"

/**
 * Set up read operation
 * @param col information of column that we're querying
 * @param transaction
 * @param request the incoming request from the REST API server
 * @param colIdx
 * @param[out] primaryKeysCols
 * @param[out] primaryKeySizes
 * @return the REST API status of performing the operation
 *
 * @return status
 */
RS_Status SetOperationPKCol(const NdbDictionary::Column *col, PKRRequest *request, Uint32 colIdx,
                            Int8 **primaryKeyCol, Uint32 *primaryKeySize);

/**
 * it stores the data read from the DB into the response buffer
 */
RS_Status WriteColToRespBuff(const NdbOperation *op, const NdbRecAttr *attr, PKRResponse *response);

/**
 * return data for array columns
 *
 */
int GetByteArray(const NdbRecAttr *attr, const char **firstByte, Uint32 *bytes);

/**
 * Check if and operation can be retried
 */
bool CanRetryOperation(RS_Status status);

/**
 * Returns exponentially increasing delay with jitter
 */
Uint32 ExponentialDelayWithJitter(Uint32 retry, Uint32 initialDelayInMS, Uint32 jitterInMS);

/**
 * Check error if unload schema is needed
 */
bool UnloadSchema(RS_Status status);

#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_DB_OPERATIONS_PK_COMMON_HPP_
