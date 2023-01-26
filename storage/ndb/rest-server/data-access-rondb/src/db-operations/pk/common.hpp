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
 *
 * @param[in] table
 * @param[in] transaction
 * @param[out] operation
 *
 * @return status
 */
RS_Status SetOperationPKCol(const NdbDictionary::Column *col, NdbOperation *operation,
                            PKRRequest *request, Uint32 colIdx);

/**
 * it stores the data read from the DB into the response buffer
 */
RS_Status WriteColToRespBuff(const NdbRecAttr *attr, PKRResponse *response);

/**
 * return data for array columns
 *
 */
int GetByteArray(const NdbRecAttr *attr, const char **first_byte, Uint32 *bytes);

/*
 * Nullify microseconds which go beyond the maxPrecision
 * E.g. 123456 microseconds:
 *   --> currentPrecision = 6
 *   --> if maxPrecision == 2
 *     --> 120000 microseconds
*/
void TruncatePrecision(MYSQL_TIME *l_time, int currentPrecision, int maxPrecision);

#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_DB_OPERATIONS_PK_COMMON_HPP_
