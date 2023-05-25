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
#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_DB_OPERATIONS_PK_PKR_REQUEST_HPP_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_DB_OPERATIONS_PK_PKR_REQUEST_HPP_

#include <stdint.h>
#include <NdbApi.hpp>
#include "src/rdrs-dal.h"

class PKRRequest {
 private:
  const RS_Buffer *req;

  // Is the request bad
  bool isInvalidOp;
  // if the request is bad then `error` contains the details
  RS_Status error;

  /**
   * Get offset of nth primary key/value pair
   *
   * @param n nth key/value pair
   * @return offset
   */
  Uint32 PKTupleOffset(const int n);

 public:
  explicit PKRRequest(const RS_Buffer *request);

  /**
   * Opration type
   * @return Operation type
   */
  Uint32 OperationType();

  /**
   * Get length of the data
   * @return data length
   */
  Uint32 Length();

  /**
   * Get maximum capacity of the buffer
   * @return buffer capacity
   */
  Uint32 Capacity();

  /**
   * Get database name
   * @return database name
   */
  const char *DB();

  /**
   * Get table name
   * @return table name
   */
  const char *Table();

  /**
   * Get number of PK columns
   * @return number of PK Columns
   */
  Uint32 PKColumnsCount();

  /**
   * Get PK column name
   *
   * @param n. index
   * @return PK column name
   */
  const char *PKName(Uint32 n);

  /**
   * Get length of PK column value
   *
   * @param n. index
   * @return PK length of the string
   */
  Uint16 PKValueLen(Uint32 n);

  /**
   * Get PK column value.
   *
   * @param n. index
   * @return PK c-string for column value
   */
  const char *PKValueCStr(Uint32 n);

  /**
   * Get PK column value
   *
   * @param n[in]. index
   * @param col[in]. ndb column
   * @param data[out]. data
   * @return 0 if successfull
   */
  int PKValueNDBStr(Uint32 index, const NdbDictionary::Column *col, char **data);

  /**
   * Get number of read columns
   * @return number of read columns
   */
  Uint32 ReadColumnsCount();

  /**
   * Get read column name
   *
   * @param n. index
   * @return read column name
   */
  const char *ReadColumnName(const Uint32 n);

  /**
   * Get read column data return type
   *
   * @param n. index
   * @return return data type
   */
  DataReturnType ReadColumnReturnType(const Uint32 n);

  /**
   * Get operation ID
   *
   * @return operation ID
   */
  const char *OperationId();

  /**
   * The operation is invalid. set the error
   */
  void MarkInvalidOp(RS_Status error);

  /**
   * Get the error
   */
  RS_Status GetError(RS_Status error);

  /**
   * Is the operation invalid
   */
  bool IsInvalidOp();
};

#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_DB_OPERATIONS_PK_PKR_REQUEST_HPP_
