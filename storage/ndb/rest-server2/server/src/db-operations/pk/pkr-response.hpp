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
#ifndef STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_DB_OPERATIONS_PK_PKR_RESPONSE_HPP_
#define STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_DB_OPERATIONS_PK_PKR_RESPONSE_HPP_

#include <stdint.h>
#include <cstring>
#include <string>
#include "src/rdrs-dal.h"
#include "src/status.hpp"
#include "src/error-strings.h"

class PKRResponse {
 private:
  const RS_Buffer *resp;
  Uint32 writeHeader = 0;
  Uint32 colsWritten = 0;
  Uint32 colsToWrite = 0;

 public:
  explicit PKRResponse(const RS_Buffer *respBuff);

  /**
   * Write header fields.
   */
  RS_Status WriteHeaderField(Uint32 index, Uint32 value);

  /**
   * Set status
   */
  RS_Status SetStatus(Uint32 value, const char* message);

  /**
   * Close response and set the data lenght.
   */
  RS_Status Close();

  /**
   * Set Database Name
   */
  RS_Status SetDB(const char *db);

  /**
   * Set Table Name
   */
  RS_Status SetTable(const char *table);

  /**
   * Set Operation ID
   */
  RS_Status SetOperationID(const char *opID);

  /**
   * Set No of columns/values contained
   * in the response. This function must
   * be called before setting any columns/values
   * as this number is used to pre-allocate
   * space to store pointers for the columns/values
   */
  RS_Status SetNoOfColumns(Uint32 cols);

  /**
   * Set data to null for this column
   */
  RS_Status SetColumnDataNull(const char *colName);

  /**
   * Set column name and data
   */
  RS_Status SetColumnData(const char *colName, const char *value, Uint32 type);

  /**
   * Get remaining capacity of the response buffer
   *
   * @return remaining capacity
   */
  Uint32 GetRemainingCapacity();

  /**
   * Get response buffer
   */
  char *GetResponseBuffer();

  /**
   * Get write header location
   */
  Uint32 GetWriteHeader();

  /**
   * Get pointer to the writing end of the buffer
   */
  void *GetWritePointer();

  /**
   * Advance write pointer
   */
  void AdvanceWritePointer(Uint32 add);

  /**
   * Append to response buffer
   */
  RS_Status Append_iu32(const char *colName, Uint32 num);

  /**
   * Append to response buffer
   */
  RS_Status Append_i32(const char *colName, Int32 num);

  /**
   * Append to response buffer
   */
  RS_Status Append_i64(const char *colName, Int64 num);

  /**
   * Append to response buffer
   */
  RS_Status Append_iu64(const char *colName, Uint64 num);

  /**
   * Append to response buffer
   */
  RS_Status Append_i8(const char *colName, Int8 num);

  /**
   * Append to response buffer
   */
  RS_Status Append_iu8(const char *colName, Uint8 num);

  /**
   * Append to response buffer
   */
  RS_Status Append_i16(const char *colName, Int16 num);

  /**
   * Append to response buffer
   */
  RS_Status Append_iu16(const char *colName, Uint16 num);

  /**
   * Append to response buffer
   */
  RS_Status Append_i24(const char *colName, int num);

  /**
   * Append to response buffer
   */
  RS_Status Append_iu24(const char *colName, unsigned int num);

  /**
   * Append to response buffer
   */
  RS_Status Append_f32(const char *colName, float num);

  /**
   * Append to response buffer
   */
  RS_Status Append_d64(const char *colName, double num);

  /**
   * Append to response buffer
   */
  RS_Status Append_char(const char *colName, const char *from_buffer, Uint32 from_length,
                        CHARSET_INFO *from_cs);

  /**
   * Append to response buffer
   */
  RS_Status Append_string(const char *colName, std::string value, Uint32 type);

 private:
  /**
   * Set column name and data internal method
   */
  RS_Status SetColumnDataInt(const char *colName, const char *value, Uint32 type);

  /**
   * Check capacity if the buffer can hold the
   * data string
   */
  bool HasCapacity(char *str);

  /**
   * Get maximum capacity of the response buffer
   *
   * @return max capacity
   */
  Uint32 GetMaxCapacity();

  /**
   * write a c_string to the buffer
   *
   */
  RS_Status Append_cstring(const char *str);

  /**
   * write header field with string value
   *
   */
  RS_Status WriteStringHeaderField(Uint32 index, const char *str);
};

#endif  // STORAGE_NDB_REST_SERVER_DATA_ACCESS_RONDB_SRC_DB_OPERATIONS_PK_PKR_RESPONSE_HPP_
