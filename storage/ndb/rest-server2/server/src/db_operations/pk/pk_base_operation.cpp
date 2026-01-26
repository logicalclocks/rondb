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

#include "pk_base_operation.hpp"
#include "NdbBlob.hpp"
#include "src/error_strings.h"
#include "src/rdrs_const.h"
#include "src/status.hpp"
#include "my_compiler.h"

#include <mysql_time.h>
#include <my_base.h>
#include <my_byteorder.h>
#include <my_time.h>
#include <decimal_utils.hpp>
#include <storage/ndb/include/ndbapi/NdbDictionary.hpp>
#include <kernel/ndb_limits.h>
#include <util/require.h>
#include <EventLogger.hpp>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_BASE_OP 1
#endif

#ifdef DEBUG_BASE_OP
#define DEB_BASE_OP(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_BASE_OP(...) do { } while (0)
#endif

RS_Status BaseKeyOperation::append_op_recs(PKRResponse *resp,
                                           PKRRequest *req) {
  for (Uint32 colIdx = 0; colIdx < m_num_read_columns; colIdx++) {
    RS_Status ret = write_col_to_resp(colIdx, resp, req);
    if (unlikely(ret.http_code != SUCCESS)) {
      DEB_BASE_OP("Failed with colIdx: %u, code: %u, message: %s",
        colIdx, ret.http_code, ret.message);
      return ret;
    }
  }
  return RS_OK;
}

static inline void base_my_unpack_date(MYSQL_TIME *l_time, const void *d) {
  uchar b[4];
  memcpy(b, d, 3);
  b[3] = 0;
  uint w = (uint)uint3korr(b);
  l_time->day = (w & 31);
  w >>= 5;
  l_time->month = (w & 15);
  w >>= 4;
  l_time->year = w;
  l_time->time_type = MYSQL_TIMESTAMP_DATE;
}

RS_Status BaseKeyOperation::write_col_to_resp(Uint32 colIdx,
                                              PKRResponse *response,
                                              PKRRequest *request) {
  const NdbDictionary::Column *col = m_readColumns[colIdx];
  const NdbRecord *ndb_record = m_ndb_record;
  const char *col_name = col->getName();
  Uint32 col_id = col->getColumnNo();
  Uint8 *row = m_row;
  {
    Uint32 null_byte_offset;
    Uint32 null_bit_in_byte;
    bool null_value = NdbDictionary::getNullBitOffset(
      ndb_record, col_id, null_byte_offset, null_bit_in_byte);
    if (null_value) {
      Uint8 null_byte = row[null_byte_offset];
      Uint8 null_bit_value = (null_byte >> null_bit_in_byte) & 1;
      if (null_bit_value) {
        DEB_BASE_OP("Column %s is null", col_name);
        return response->SetColumnDataNull();
      }
    }
  }
  Uint32 offset;
  bool ret = NdbDictionary::getOffset(ndb_record, col_id, offset);
  require(ret);
  Uint8 *col_ptr = row + offset;
  DEB_BASE_OP("Column %s with col_id: %u is not null, offset: %u, type: %u",
              col_name, col_id, offset, col->getType());
  switch (col->getType()) {
  case NdbDictionary::Column::Undefined: {
    ///< 4 bytes + 0-3 fraction
    return RS_CLIENT_ERROR(std::string(
      rdrsErrorMessage(ERROR_UNDEFINED_DATA_TYPE)) +
        std::string(" Column: ") + std::string(col_name));
  }
  case NdbDictionary::Column::Tinyint: {
    ///< 8 bit. 1 byte signed integer, can be used in array
    return response->Append_i8(*(Int8*)col_ptr);
  }
  case NdbDictionary::Column::Tinyunsigned: {
    ///< 8 bit. 1 byte unsigned integer, can be used in array
    return response->Append_iu8(*(Uint8*)col_ptr);
  }
  case NdbDictionary::Column::Smallint: {
    ///< 16 bit. 2 byte signed integer, can be used in array
    Int16 i16;
    memcpy(&i16, col_ptr, sizeof(Int16));
    return response->Append_i16(i16);
  }
  case NdbDictionary::Column::Smallunsigned: {
    Uint16 u16;
    memcpy(&u16, col_ptr, sizeof(Uint16));
    ///< 16 bit. 2 byte unsigned integer, can be used in array
    return response->Append_iu16(u16);
  }
  case NdbDictionary::Column::Mediumint: {
    ///< 24 bit. 3 byte signed integer, can be used in array
    return response->Append_i24(sint3korr(col_ptr));
  }
  case NdbDictionary::Column::Mediumunsigned: {
    ///< 24 bit. 3 byte unsigned integer, can be used in array
    return response->Append_iu24(uint3korr(col_ptr));
  }
  case NdbDictionary::Column::Int: {
    ///< 32 bit. 4 byte signed integer, can be used in array
    Int32 i32;
    memcpy(&i32, col_ptr, sizeof(Int32));
    return response->Append_i32(i32);
  }
  case NdbDictionary::Column::Unsigned: {
    ///< 32 bit. 4 byte unsigned integer, can be used in array
    Uint32 u32;
    memcpy(&u32, col_ptr, sizeof(Uint32));
    return response->Append_iu32(u32);
  }
  case NdbDictionary::Column::Bigint: {
    ///< 64 bit. 8 byte signed integer, can be used in array
    Int64 i64;
    memcpy(&i64, col_ptr, sizeof(Int64));
    return response->Append_i64(i64);
  }
  case NdbDictionary::Column::Bigunsigned: {
    ///< 64 Bit. 8 byte signed integer, can be used in array
    Uint64 u64;
    memcpy(&u64, col_ptr, sizeof(Uint64));
    return response->Append_iu64(u64);
  }
  case NdbDictionary::Column::Float: {
    ///< 32-bit float. 4 bytes float, can be used in array
    float f32;
    memcpy(&f32, col_ptr, sizeof(float));
    return response->Append_f32(f32);
  }
  case NdbDictionary::Column::Double: {
    ///< 64-bit float. 8 byte float, can be used in array
    double d64;
    memcpy(&d64, col_ptr, sizeof(double));
    return response->Append_d64(d64);
  }
  case NdbDictionary::Column::Olddecimal: {
    ///< MySQL < 5.0 signed decimal,  Precision, Scale
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) +
      std::string(" Column: ") + std::string(col_name) +
      " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Olddecimalunsigned: {
    ///< MySQL < 5.0 signed decimal,  Precision, Scale
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) +
      std::string(" Column: ") + std::string(col_name) +
      " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Decimal:
    ///< MySQL >= 5.0 signed decimal,  Precision, Scale
    [[fallthrough]];
  case NdbDictionary::Column::Decimalunsigned: {
    char decStr[DECIMAL_MAX_STR_LEN_IN_BYTES];
    int precision = col->getPrecision();
    int scale = col->getScale();
    void *bin = (void*)col_ptr;
    int binLen = col->getSizeInBytesForRecord();
    decimal_bin2str(bin,
                    binLen,
                    precision,
                    scale,
                    decStr,
                    DECIMAL_MAX_STR_LEN_IN_BYTES);
    DEB_BASE_OP("col_name: %s Decimal column, decStr: %s, binLen: %u",
                col_name, std::string(decStr).c_str(), binLen);
    return response->Append_string(std::string(decStr),
                                   RDRS_FLOAT_DATATYPE);
  }
  case NdbDictionary::Column::Char:
    ///< Len. A fixed array of 1-byte chars
    [[fallthrough]];
  case NdbDictionary::Column::Varchar:
    ///< Length bytes: 1, Max: 255
    [[fallthrough]];
  case NdbDictionary::Column::Longvarchar: {
    ///< Length bytes: 2, little-endian
    const char *dataStart = nullptr;
    const NdbDictionary::Column::ArrayType arrayType =
      col->getArrayType();
    Uint32 attrBytes = col->getLength();
    switch (arrayType) {
    case NdbDictionary::Column::ArrayTypeFixed:
      /**
       *  No prefix length is stored in aRef. Data starts from aRef's first byte
       *  data might be padded with blank or null bytes to fill the whole column
       */
      dataStart = (const char*)col_ptr;
      break;
    case NdbDictionary::Column::ArrayTypeShortVar:
      /**
       * First byte of aRef has the length of data stored
       *  Data starts from second byte of aRef
       */
      dataStart = (const char*)(col_ptr + 1);
      attrBytes = static_cast<Uint8>(col_ptr[0]);
      break;
    case NdbDictionary::Column::ArrayTypeMediumVar:
      /**
       * First two bytes of aRef has the length of data stored
       * Data starts from third byte of aRef
       */
      dataStart = (const char*)(col_ptr + 2);
      attrBytes = static_cast<Uint8>(col_ptr[1]) * 256 +
                  static_cast<Uint8>(col_ptr[0]);
      break;
    default:
      return RS_CLIENT_ERROR(std::string(
        rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
    }
    return response->Append_char(dataStart,
                                 attrBytes,
                                 col->getCharset(),
                                 col->getType() == NdbDictionary::Column::Char);
  }
  case NdbDictionary::Column::Binary:
    [[fallthrough]];
  case NdbDictionary::Column::Varbinary:
    ///< Length bytes: 1, Max: 255
    [[fallthrough]];
  case NdbDictionary::Column::Longvarbinary: {
    ///< Length bytes: 2, little-endian
    const char *dataStart = nullptr;
    const NdbDictionary::Column::ArrayType arrayType =
      col->getArrayType();
    Uint32 attrBytes = col->getLength();
    switch (arrayType) {
    case NdbDictionary::Column::ArrayTypeFixed:
      /**
       *  No prefix length is stored in aRef. Data starts from aRef's first byte
       *  data might be padded with blank or null bytes to fill the whole column
       */
      dataStart = (const char*)col_ptr;
      break;
    case NdbDictionary::Column::ArrayTypeShortVar:
      /**
       * First byte of aRef has the length of data stored
       *  Data starts from second byte of aRef
       */
      dataStart = (const char*)(col_ptr + 1);
      attrBytes = static_cast<Uint8>(col_ptr[0]);
      break;
    case NdbDictionary::Column::ArrayTypeMediumVar:
      /**
       * First two bytes of aRef has the length of data stored
       * Data starts from third byte of aRef
       */
      dataStart = (const char*)(col_ptr + 2);
      attrBytes = static_cast<Uint8>(col_ptr[1]) * 256 +
                  static_cast<Uint8>(col_ptr[0]);
      break;
    default:
      return RS_CLIENT_ERROR(std::string(
        rdrsErrorMessage(ERROR_UNABLE_TO_READ_DATA)));
    }
    if (unlikely(attrBytes > MAX_TUPLE_SIZE_IN_BYTES)) {
      return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_TOO_LARGE_ROWS)) +
        std::string(" DB: ") + std::string(request->DB()) +
        " Table: " + std::string(request->Table()));
    }
    DEB_BASE_OP("dataStart: %p, len: %u", dataStart, attrBytes);
    return response->Append_bin(dataStart, attrBytes, RDRS_BINARY_DATATYPE);
  }
  case NdbDictionary::Column::Datetime: {
    ///< Precision down to 1 sec (sizeof(Datetime) == 8 bytes )
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) +
      std::string(" Column: ") + std::string(col_name) +
      " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Date: {
    ///< Precision down to 1 day(sizeof(Date) == 4 bytes )
    MYSQL_TIME lTime;
    base_my_unpack_date(&lTime, (char*)col_ptr);
    char to[MAX_DATE_STRING_REP_LENGTH];
    my_date_to_str(lTime, to);
    return response->Append_string(std::string(to), RDRS_DATETIME_DATATYPE);
  }
  case NdbDictionary::Column::Blob: {
    ///< Binary large object (see NdbBlob)
    /// Treat it as binary data
    NdbBlob *blobHandle = get_blob_handle(colIdx);
    if (unlikely(blobHandle == nullptr)) {
      // BLOB not supported (e.g., in delete operations)
      return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_UNSUPPORTED_BLOB_TEXT_READ)) +
        std::string(" Column: ") + std::string(col_name));
    }
    Uint64 length = 0;
    int isNull = 0;
    if (unlikely(blobHandle->getNull(isNull) != 0)) {
      return RS_RONDB_SERVER_ERROR(
        blobHandle->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) +
          std::string(" Failed to check NULL of ") +
          std::string(" Column: ") + std::string(col_name) +
          " Type: " + std::to_string(col->getType()));
    }
    if (isNull) {
      if (unlikely(blobHandle->getLength(length) != 0)) {
        return RS_RONDB_SERVER_ERROR(
          blobHandle->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) +
          std::string(" NULL column has size != 0 ") +
          std::string(" Column: ") + std::string(col_name) +
          " Type: " + std::to_string(col->getType()));
      }
      return response->SetColumnDataNull();
    }
    if (blobHandle->getLength(length) == -1) {
      return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) +
        std::string(" Reading column length failed.") +
          std::string(" Column: ") + std::string(col_name) +
          " Type: " + std::to_string(col->getType()));
    }
    DEB_BASE_OP("Read col_name: %s, BLOB of length: %llu", col_name, length);
    // check for max length
    // (4 * ceil(input_size / 3))
    const size_t maxLength = length + 4;
    if (unlikely(response->GetRemainingCapacity() < maxLength)) {
      return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_RESPONSE_BUFFER_OVERFLOW)) +
        std::string(" Buffer Remaining Capacity: ") +
        std::to_string(response->GetRemainingCapacity()) +
        " Required: " + std::to_string(maxLength));
    }
    Uint64 chunk = 0;
    Uint64 total_read = 0;
    char buffer[BLOB_MAX_FETCH_SIZE];

    for (chunk = 0; chunk < (length / (BLOB_MAX_FETCH_SIZE)) + 1; chunk++) {
      Uint64 pos = chunk * BLOB_MAX_FETCH_SIZE;
      // NOTE this is bytes to read and also bytes read.
      Uint32 bytes = BLOB_MAX_FETCH_SIZE;
      if (pos + bytes > length) {
        bytes = length - pos;
      }
      if (bytes != 0) {
        if (unlikely(-1 == blobHandle->setPos(pos))) {
          return RS_RONDB_SERVER_ERROR(
            blobHandle->getNdbError(),
            std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) +
            std::string(" Failed to set read position.") +
            std::string(" Column: ") + std::string(col_name) +
            " Type: " + std::to_string(col->getType()));
        }
        if (blobHandle->readData(buffer,
                                 bytes /*to read, also bytes read*/) == -1) {
          return RS_RONDB_SERVER_ERROR(
            blobHandle->getNdbError(),
            std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) +
            std::string(" Read data failed .") +
            std::string(" Column: ") + std::string(col_name) +
            " Type: " + std::to_string(col->getType()) +
            " Position: " + std::to_string(pos));
        }
        if (bytes > 0) {
          total_read += bytes;
          if (chunk == 0) {
            response->Append_string("", RDRS_BINARY_DATATYPE);
            // This adds a column to the response buffer. Right now the last
            // byte of the response buffer is '\0'. Remove the last byte and
            // start appending the base64 data
            response->AdvanceWritePointer(-1);
          }
          memcpy((char*)response->GetWritePointer(),
                 (const char*)buffer,
                 bytes);
          response->AdvanceWritePointer(bytes);
        }
      }
    }
    if (total_read != length) {
      return RS_RONDB_SERVER_ERROR(
        blobHandle->getNdbError(),
        std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) +
        std::string(" Not all of the data was read.") +
        std::string(" Column: ") + std::string(col_name) +
        " Expected to read: " + std::to_string(length) +
        " bytes. Read: " + std::to_string(total_read));
    }
    response->SetBlobLen(total_read);
    DEB_BASE_OP("Written a blob of total len: %u",
      (Uint32)total_read);
    return RS_OK;
  }
  case NdbDictionary::Column::Text: {
    ///< Text blob
    NdbBlob *blobHandle = get_blob_handle(colIdx);
    if (unlikely(blobHandle == nullptr)) {
      // TEXT not supported (e.g., in delete operations)
      return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_UNSUPPORTED_BLOB_TEXT_READ)) +
        std::string(" Column: ") + std::string(col_name));
    }
    Uint64 length = 0;
    int isNull = 0;
    if (unlikely(blobHandle->getNull(isNull) != 0)) {
      return RS_RONDB_SERVER_ERROR(
        blobHandle->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) +
          std::string(" Failed to check NULL of ") +
          std::string(" Column: ") + std::string(col_name) +
          " Type: " + std::to_string(col->getType()));
    }
    if (isNull) {
      if (unlikely(blobHandle->getLength(length) != 0)) {
        return RS_RONDB_SERVER_ERROR(
          blobHandle->getNdbError(),
          std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) +
          std::string(" NULL column has size != 0 ") +
          std::string(" Column: ") + std::string(col_name) +
          " Type: " + std::to_string(col->getType()));
      }
      return response->SetColumnDataNull();
    }
    if (blobHandle->getLength(length) == -1) {
      return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) +
        std::string(" Reading column length failed.") +
        std::string(" Column: ") + std::string(col_name) +
        " Type: " + std::to_string(col->getType()));
    }
    //+1 for null terminator
    if (unlikely(response->GetRemainingCapacity() < length + 1)) {
      return RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_RESPONSE_BUFFER_OVERFLOW)) +
        std::string(" Buffer Remaining Capacity: ") +
        std::to_string(response->GetRemainingCapacity()) +
        " Required: " + std::to_string(length + 1));
    }
    DEB_BASE_OP("Read col_name: %s, TEXT of length: %llu", col_name, length);
    // NOTE: we not allocating a tmp buffer to hold the data
    // Reusing the reponse buffer
    char *tmpBuffer = static_cast<char*>(response->GetWritePointer());
    Uint64 chunk = 0;
    Uint64 total_read = 0;
    for (chunk = 0; chunk < (length / (BLOB_MAX_FETCH_SIZE)) + 1; chunk++) {
      Uint64 pos   = chunk * BLOB_MAX_FETCH_SIZE;
      // NOTE this is bytes to read and also bytes read.
      Uint32 bytes = BLOB_MAX_FETCH_SIZE;
      if (pos + bytes > length) {
        bytes = length - pos;
      }
      if (bytes != 0) {
        if (-1 == blobHandle->setPos(pos)) {
          return RS_RONDB_SERVER_ERROR(
            blobHandle->getNdbError(),
            std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) +
            std::string(" Failed to set read position.") +
            std::string(" Column: ") + std::string(col_name) +
            " Type: " + std::to_string(col->getType()));
        }
        if (blobHandle->readData(tmpBuffer,
                                 bytes /*to read, also bytes read*/) == -1) {
          return RS_RONDB_SERVER_ERROR(
            blobHandle->getNdbError(),
            std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) +
            std::string(" Read data failed .") +
            std::string(" Column: ") + std::string(col_name) +
            " Type: " + std::to_string(col->getType()) +
            " Position: " + std::to_string(pos));
        }
        if (bytes > 0) {
          tmpBuffer += bytes;  // move the pointer forward
          total_read += bytes;
        }
      }
    }
    if (unlikely(total_read != length)) {
      return RS_RONDB_SERVER_ERROR(
        blobHandle->getNdbError(),
        std::string(rdrsErrorMessage(ERROR_COLUMN_READ_FAILED)) +
        std::string(" Not all of the data was read.") +
        std::string(" Column: ") + std::string(col_name) +
        " Expected to read: " + std::to_string(length) +
        " bytes. Read: " + std::to_string(total_read));
    }
    return response->Append_char(
      static_cast<char *>(response->GetWritePointer()),
      length,
      col->getCharset(),
      false);
  }
  case NdbDictionary::Column::Bit: {
    Uint32 len = col->getLength();
    Uint32 words = len / 8;
    Uint32 bits_used_in_last_word = len % 8;
    Uint32 last_mask = 0xFF;
    if (bits_used_in_last_word != 0) {
      words += 1;
      last_mask = ((1 << bits_used_in_last_word) - 1);
    }
    require(words <= BIT_MAX_SIZE_IN_BYTES);
    // change endieness
    col_ptr[words - 1] &= last_mask;
    int i = 0;
    char reversed[BIT_MAX_SIZE_IN_BYTES];
    for (int j = words - 1; j >= 0; j--) {
      reversed[i++] = (char)col_ptr[j];
    }
    DEB_BASE_OP("BIT:col_name: %s, col_ptr[words - 1] = %x, outlen: %u",
                col_name,
                col_ptr[words - 1],
                Uint32(words));
    return response->Append_bin(reversed, words, RDRS_BIT_DATATYPE);
  }
  case NdbDictionary::Column::Time: {
    ///< Time without date
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) +
      std::string(" Column: ") + std::string(col_name) +
      " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Year: {
    ///< Year 1901-2155 (1 byte)
    Int32 year = (uint)(1900 + col_ptr[0]);
    return response->Append_i32(year);
  }
  case NdbDictionary::Column::Timestamp: {
    ///< Unix time
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) +
      std::string(" Column: ") + std::string(col_name) +
      " Type: " + std::to_string(col->getType()));
  }
  ///**
  // * Time types in MySQL 5.6 add microsecond fraction.
  // * One should use setPrecision(x) to set number of fractional
  // * digits (x = 0-6, default 0).  Data formats are as in MySQL
  // * and must use correct byte length.  NDB does not check data
  // * itself since any values can be compared as binary strings.
  // */
  case NdbDictionary::Column::Time2: {
    ///< 3 bytes + 0-3 fraction
    uint precision = col->getPrecision();
    longlong numericTime =
      my_time_packed_from_binary((const unsigned char *)col_ptr, precision);
    MYSQL_TIME lTime;
    TIME_from_longlong_time_packed(&lTime, numericTime);
    char to[MAX_DATE_STRING_REP_LENGTH];
    my_TIME_to_str(lTime, to, precision);
    return response->Append_string(std::string(to), RDRS_DATETIME_DATATYPE);
  }
  case NdbDictionary::Column::Datetime2: {
    ///< 5 bytes plus 0-3 fraction
    uint precision = col->getPrecision();
    longlong numericDate =
      my_datetime_packed_from_binary((const unsigned char *)col_ptr, precision);
    MYSQL_TIME lTime;
    TIME_from_longlong_datetime_packed(&lTime, numericDate);
    char to[MAX_DATE_STRING_REP_LENGTH];
    my_TIME_to_str(lTime, to, precision);
    return response->Append_string(std::string(to), RDRS_DATETIME_DATATYPE);
  }
  case NdbDictionary::Column::Timestamp2: {
    ///< 4 bytes + 0-3 fraction
    uint precision = col->getPrecision();
    my_timeval myTV{};
    my_timestamp_from_binary(&myTV, (const unsigned char *)col_ptr, precision);
    Int64 epochIn = myTV.m_tv_sec;
    time_t stdtime(epochIn);
    struct tm *time_info = gmtime(&stdtime);
    MYSQL_TIME lTime  = {};
    lTime.year        = time_info->tm_year + 1900;
    lTime.month       = time_info->tm_mon +1;
    lTime.day         = time_info->tm_mday;
    lTime.hour        = time_info->tm_hour;
    lTime.minute      = time_info->tm_min;
    lTime.second      = time_info->tm_sec;
    lTime.second_part = myTV.m_tv_usec;
    lTime.time_type   = MYSQL_TIMESTAMP_DATETIME;
    char to[MAX_DATE_STRING_REP_LENGTH];
    my_TIME_to_str(lTime, to, precision);
    return response->Append_string(std::string(to), RDRS_DATETIME_DATATYPE);
  }
  }
  return RS_SERVER_ERROR(
    std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) +
    std::string(" Column: ") + std::string(col_name) +
    " Type: " + std::to_string(col->getType()));
}
