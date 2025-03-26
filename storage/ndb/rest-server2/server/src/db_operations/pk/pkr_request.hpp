/*
 * Copyright (C) 2023, 2024 Hopsworks AB
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
 
#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKR_REQUEST_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKR_REQUEST_HPP_

#include "src/rdrs_dal.h"

#include <stdint.h>
#include <NdbApi.hpp>

class PKRRequest {
 private:
  const RS_Buffer *req;

  // Is the request bad
  bool isInvalidOp;

  // if the request is bad then `error` contains the details
  RS_Status error;

  // Is request modified. 
  // When no read cols are specified then we build the list 
  // of possible columns that the user can read and this
  // information in stored in request buffer to save space. 
  // If the request fails then we have to revert the changes 
  // in the req buffer other wise upon retry the execution 
  // behaviour will be different. For example, upon first 
  // try we will see that no req columns are set but in the
  // second retry we will see that req cols are set. 
  bool isReqModified;
  Uint32 oldLen;

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
   * @param n index
   * @return PK column name
   */
  const char *PKName(Uint32 n);

  /**
   * Get PK column name length
   *
   * @param n index
   * @return PK column name length
   */
  Uint32 PKNameLen(Uint32 n);

  /**
   * Get length of PK column value
   *
   * @param n index
   * @return PK length of the string
   */
  Uint32 PKValueLen(Uint32 n);

  /**
   * Get PK column value.
   *
   * @param n index
   * @return PK c-string for column value
   */
  const char *PKValueCStr(Uint32 n);

  /**
   * Get number of read columns
   * @return number of read columns
   */
  Uint32 ReadColumnsCount();

  /**
   * Get read column name
   *
   * @param n index
   * @return read column name
   */
  const char *ReadColumnName(const Uint32 n);

  /**
   * Get read column name length
   *
   * @param n index
   * @return read column name length
   */
  Uint32 ReadColumnNameLen(const Uint32 n);

  /**
   * Get read column data return type
   *
   * @param n index
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
  RS_Status GetError();

  /**
   * Is the operation invalid
   */
  bool IsInvalidOp();

  /**
   * Add columns when user wanted to read all columns
   */
  bool addReadColumns(Uint32 numReadColumns);

  /**
   * Remove read columns from the req buff 
   */
  bool resetReadColumns();

  /**
   * Add a read column name
   */
  bool addReadColumnName(Uint32 index, const char *name, Uint32 data_type);
};
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKR_REQUEST_HPP_
