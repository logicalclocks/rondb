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

#include "common.hpp"
#include <my_base.h>
#include "NdbOut.hpp"
#include "ndb_types.h"
#include "storage/ndb/include/ndb_global.h"
#include "decimal.h"
#include "my_compiler.h"
#include "src/error_strings.h"
#include "src/status.hpp"
#include "src/mystring.hpp"
#include "src/rdrs_const.h"
#include "src/logger.hpp"

#include <decimal_utils.hpp>
#include <NdbError.hpp>
#include <my_time.h>
#include <ctime>
#include <sql_string.h>
#include <ndb_limits.h>
#include <libbase64.h>
#include <string>
#include <algorithm>
#include <utility>
#include <util/require.h>
#include <EventLogger.hpp>

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_NDB_BE 1
#endif

#ifdef DEBUG_NDB_BE
#define DEB_NDB_BE(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_NDB_BE(...) do { } while (0)
#endif

typedef unsigned char uchar;
typedef Uint32 uint32;
static inline uint32 uint3korr(const uchar *A) {
  return static_cast<uint32>((static_cast<uint32>(A[0])) +
                             ((static_cast<uint32>(A[1])) << 8) +
                             ((static_cast<uint32>(A[2])) << 16));
}

static inline void int3store(uchar *T, uint A) {
  *(T) = (uchar)(A);
  *(T + 1) = (uchar)(A >> 8);
  *(T + 2) = (uchar)(A >> 16);
}

inline void my_unpack_date(MYSQL_TIME *l_time, const void *d) {
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

RS_Status set_operation_pk_col(const NdbDictionary::Column *col,
                               PKRRequest *request,
                               Uint8 *row,
                               const NdbRecord *ndb_record,
                               Uint32 colIdx) {
  RS_Status error = RS_OK;
  Uint32 col_id = col->getColumnNo();
  Uint32 offset;
  bool ret = NdbDictionary::getOffset(ndb_record, col_id, offset);
  require(ret);
  Uint8* primaryKeyCol = row + offset;
  DEB_NDB_BE("Primary key column %s is at offset %u", col->getName(), offset);

  switch (col->getType()) {
  case NdbDictionary::Column::Undefined: {
    ///< 4 bytes + 0-3 fraction
    error = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_UNDEFINED_DATA_TYPE)) + 
                            std::string(" Column: ") + std::string(request->PKName(colIdx)));
    break;
  }
  case NdbDictionary::Column::Tinyint: {
    ///< 8 bit. 1 byte signed integer, can be used in array

    char *parsed = nullptr;
    errno = 0;
    Int64 parsedNumber = strtoll(request->PKValueCStr(colIdx), &parsed, 10);
    Int8 i = (Int8)parsedNumber;
    memcpy(primaryKeyCol, &i, sizeof(Int8));
    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(parsedNumber >= -128 && parsedNumber <= 127))) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) + 
        std::string(" Expecting TINYINT. Column: ") +
        std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Tinyunsigned: {
    ///< 8 bit. 1 byte unsigned integer, can be used in array
    char *parsed = nullptr;
    errno = 0;
    Uint64 parsedNumber = strtoull(request->PKValueCStr(colIdx), &parsed, 10);
    Uint8 u = (Uint8)parsedNumber;
    memcpy(primaryKeyCol, &u, sizeof(Uint8));
    if (unlikely(*parsed != '\0' ||
                 errno != 0 ||
                 !(parsedNumber <= 255))) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) + 
        std::string(" Expecting TINYINT UNSIGNED. Column: ") +
        std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Smallint: {
    ///< 16 bit. 2 byte signed integer, can be used in array
    char *parsed = nullptr;
    errno = 0;
    Int64 parsedNumber = strtoll(request->PKValueCStr(colIdx), &parsed, 10);
    Int16 i = (Int16)parsedNumber;
    memcpy(primaryKeyCol, &i, sizeof(Int16));
    if (unlikely(*parsed != '\0' ||
                 errno != 0 ||
                 !(parsedNumber >= -32768 && parsedNumber <= 32767))) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) + 
        std::string(" Expecting SMALLINT. Column: ") +
        std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Smallunsigned: {
    ///< 16 bit. 2 byte unsigned integer, can be used in array

    char *parsed = nullptr;
    errno = 0;
    Uint64 parsedNumber = strtoull(request->PKValueCStr(colIdx), &parsed, 10);
    Uint16 u = (Uint16)parsedNumber;
    memcpy(primaryKeyCol, &u, sizeof(Uint16));
    if (unlikely(*parsed != '\0' ||
                 errno != 0 ||
                 !(parsedNumber <= 65535))) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) + 
        std::string(" Expecting SMALLINT UNSIGNED. Column: ") +
        std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Mediumint: {
    ///< 24 bit. 3 byte signed integer, can be used in array

    char *parsed = nullptr;
    errno = 0;
    Int64 parsedNumber = strtoll(request->PKValueCStr(colIdx), &parsed, 10);
    Int32 i = (Int32)parsedNumber;
    int3store(primaryKeyCol, (uint)i);
    if (unlikely(*parsed != '\0' ||
                 errno != 0 ||
                 !(parsedNumber >= -8388608 && parsedNumber <= 8388607))) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) + 
        std::string(" Expecting MEDIUMINT. Column: ") +
        std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Mediumunsigned: {
    ///< 24 bit. 3 byte unsigned integer, can be used in array

    char *parsed = nullptr;
    errno = 0;
    Uint64 parsedNumber = strtoull(request->PKValueCStr(colIdx), &parsed, 10);
    Uint32 u = (Uint32)parsedNumber;
    int3store(primaryKeyCol, (uint)u);
    if (unlikely(*parsed != '\0' ||
                 errno != 0 ||
                 !(parsedNumber <= 16777215))) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) + 
        std::string(" Expecting MEDIUMINT UNSIGNED. Column: ") +
        std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Int: {
    ///< 32 bit. 4 byte signed integer, can be used in array
    char *parsed = nullptr;
    errno = 0;
    Int64 parsedNumber = strtoll(request->PKValueCStr(colIdx), &parsed, 10);
    Int32 i = (Int32)parsedNumber;
    memcpy(primaryKeyCol, &i, sizeof(Int32));
    if (unlikely(*parsed != '\0' ||
                 errno != 0 ||
                 !(parsedNumber >= -2147483648 &&
                   parsedNumber <= 2147483647))) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) + 
        std::string(" Expecting INT. Column: ") +
        std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Unsigned: {
    ///< 32 bit. 4 byte unsigned integer, can be used in array
    char *parsed = nullptr;
    errno = 0;
    Uint64 parsedNumber = strtoull(request->PKValueCStr(colIdx), &parsed, 10);
    Uint32 u = (Uint32)parsedNumber;
    memcpy(primaryKeyCol, &u, sizeof(Uint32));
    if (unlikely(*parsed != '\0' ||
                 errno != 0 ||
                 !(parsedNumber <= 4294967295))) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) + 
        std::string(" Expecting INT UNSIGNED. Column: ") +
        std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Bigint: {
    ///< 64 bit. 8 byte signed integer, can be used in array
    char *parsed = nullptr;
    errno = 0;
    Int64 parsedNumber = strtoll(request->PKValueCStr(colIdx), &parsed, 10);
    memcpy(primaryKeyCol, &parsedNumber, sizeof(Int64));
    if (unlikely(*parsed != '\0' || errno != 0)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) + 
        std::string(" Expecting BIGINT. Column: ") +
        std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Bigunsigned: {
    ///< 64 Bit. 8 byte signed integer, can be used in array
    char *parsed = nullptr;
    errno = 0;
    Uint64 parsedNumber = strtoull(request->PKValueCStr(colIdx), &parsed, 10);
    memcpy(primaryKeyCol, &parsedNumber, sizeof(Uint64));
    const std::string numStr = std::string(request->PKValueCStr(colIdx));
    if (unlikely(*parsed != '\0' ||
                 errno != 0 ||
                 numStr.find('-') != std::string::npos)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) + 
        std::string(" Expecting BIGINT UNSIGNED. Column: ") +
        std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Float: {
    ///< 32-bit float. 4 bytes float, can be used in array
    error = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_UNSUPPORTED_HASH_INDEX)) + 
        std::string(" Column: ") + std::string(request->PKName(colIdx)));
    break;
  }
  case NdbDictionary::Column::Double: {
    ///< 64-bit float. 8 byte float, can be used in array
    error = RS_CLIENT_ERROR(std::string(rdrsErrorMessage(ERROR_UNSUPPORTED_HASH_INDEX)) + 
        std::string(" Column: ") + std::string(request->PKName(colIdx)));
    break;
  }
  case NdbDictionary::Column::Olddecimal: {
    ///< MySQL < 5.0 signed decimal,  Precision, Scale
    error = RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) +
      std::string(" Column: ") + std::string(col->getName()) + 
      " Type: " + std::to_string(col->getType()));
    break;
  }
  case NdbDictionary::Column::Olddecimalunsigned: {
    error = RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) + 
      std::string(" Column: ") + std::string(col->getName()) +
      " Type: " + std::to_string(col->getType()));
    break;
  }
  case NdbDictionary::Column::Decimalunsigned: {
    ///< MySQL >= 5.0 signed decimal, Precision, Scale
    const std::string decStr = std::string(request->PKValueCStr(colIdx));
    if (unlikely(decStr.find('-') != std::string::npos)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) + 
        std::string(" Expecting DECIMAL UNSIGNED. Column: ") +
        std::string(request->PKName(colIdx)));
      break;
    }
    [[fallthrough]];
  }
  case NdbDictionary::Column::Decimal: {
    int precision = col->getPrecision();
    int scale = col->getScale();
    const char *decStr = request->PKValueCStr(colIdx);
    const int strLen = request->PKValueLen(colIdx);
    if (unlikely(decimal_str2bin(decStr,
                                 strLen,
                                 precision,
                                 scale,
                                 primaryKeyCol,
                                 DECIMAL_MAX_SIZE_IN_BYTES) != 0)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) + 
        std::string(" Expecting Decimal with Precision: ") +
        std::to_string(precision) + std::string(" and Scale: ") +
        std::to_string(scale));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Char: {
    /// A fix sized array of characters
    /// size of a character depends on encoding scheme
    const char *dataStr = request->PKValueCStr(colIdx);
    const int dataStrLen = request->PKValueLen(colIdx);
    const int colMaxLen = col->getSizeInBytes();
    if (unlikely(dataStrLen > colMaxLen)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
        " Data length is greater than column length. Column: " +
        std::string(col->getName()));
      break;
    }
    // operation->equal expects a zero-padded char string
    memcpy(primaryKeyCol, dataStr, dataStrLen);
    memset(primaryKeyCol + dataStrLen, 0, colMaxLen - dataStrLen);
    break;
  }
  case NdbDictionary::Column::Varchar:
    ///< Length bytes: 1, Max: 255
    [[fallthrough]];
  case NdbDictionary::Column::Longvarchar: {
    ///< Length bytes: 2, little-endian
    int additionalLen = 1;
    if (col->getType() == NdbDictionary::Column::Longvarchar) {
      additionalLen = 2;
    }
    Uint32 primaryKeySize = request->PKValueLen(colIdx);
    if (unlikely(primaryKeySize > (Uint32)col->getLength())) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
        " Data length is greater than column length. Data length:" +
        std::to_string(primaryKeySize) + "Column: " +
        std::string(col->getName()) +
        " Column length: " + std::to_string(col->getLength()));
      break;
    }
    memcpy(primaryKeyCol + additionalLen,
           request->PKValueCStr(colIdx),
           primaryKeySize);
    if (col->getType() == NdbDictionary::Column::Varchar) {
      ((Uint8 *)primaryKeyCol)[0] = (Uint8)(primaryKeySize);
    } else if (col->getType() == NdbDictionary::Column::Longvarchar) {
      ((Uint8 *)primaryKeyCol)[0] = (Uint8)(primaryKeySize % 256);
      ((Uint8 *)primaryKeyCol)[1] = (Uint8)(primaryKeySize / 256);
    } else {
      error = RS_SERVER_ERROR(std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Binary: {
    /// Binary data is sent as base64 string
    require(col->getLength() <= BINARY_MAX_SIZE_IN_BYTES);
    const char *encodedStr = request->PKValueCStr(colIdx);
    const size_t encodedStrLen = request->PKValueLen(colIdx);
    // The buffer in out has been allocated by the caller and is at least
    // 3/4 the size of the input.
    // Encoding takes 3 decoded bytes at a time and turns them into 4
    // encoded bytes. The encoded string is therefore always a multiple of 4.
    const size_t maxConversions =
      BINARY_MAX_SIZE_IN_BYTES / 3 +
        (BINARY_MAX_SIZE_IN_BYTES % 3 != 0);  // basically ceiling()
    const size_t maxEncodedSize = 4 * maxConversions;
    if (unlikely(encodedStrLen > maxEncodedSize)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) + " " +
        "Encoded data length is greater than 4/3 of maximum binary size." +
        " Column: " + std::string(col->getName()) +
        " Maximum binary size: " + std::to_string(BINARY_MAX_SIZE_IN_BYTES));
      break;
    }
    Uint32 primaryKeySize = col->getSizeInBytes();
    memset(primaryKeyCol, 0, primaryKeySize);
    size_t outlen = 0;
    int result = base64_decode(encodedStr,
                               encodedStrLen,
                               (char *)primaryKeyCol,
                               &outlen,
                               0);

    if (unlikely(result == 0)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) + " " +
        "Encountered error decoding base64. Column: " +
        std::string(col->getName()));
      break;
    } else if (unlikely(result == -1)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) + " " +
        "Encountered error decoding base64; Chosen codec is not part of"
        " current build. Column: " + std::string(col->getName()));
      break;
    }
    if (unlikely(outlen > primaryKeySize)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) + " " +
        "Decoded data length is greater than column length." +
        " Column: " + std::string(col->getName()) +
        " Length: " + std::to_string(col->getLength()));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Varbinary:
    ///< Length bytes: 1, Max: 255
    [[fallthrough]];
  case NdbDictionary::Column::Longvarbinary: {
    // Length bytes: 2, little-endian
    // Note: col->getLength() does not include the length bytes.

    // this includes size prefix
    size_t colDataLen = col->getLength();       // this is without size prefix
    const char *encodedStr = request->PKValueCStr(colIdx);
    const size_t encodedStrLen = request->PKValueLen(colIdx);
    // Encoding takes 3 decoded bytes at a time and turns them into 4
    // encoded bytes. The encoded string is therefore always a multiple of 4.
    const size_t maxConversions =
      colDataLen / 3 + (colDataLen % 3 != 0);  // basically ceiling()
    const size_t maxEncodedSize = 4 * maxConversions;
    if (unlikely(encodedStrLen > maxEncodedSize)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
        " Encoded data length is greater than 4/3 of maximum binary size." +
        " Column: " + std::string(col->getName()) +
        " Maximum binary size: " + std::to_string(colDataLen));
      break;
    }
    int additionalLen = 1;
    if (col->getType() == NdbDictionary::Column::Longvarbinary) {
      additionalLen = 2;
    }
    size_t outlen = 0;
    // leave first 1-2 bytes free for saving length bytes
    int result = base64_decode(encodedStr,
                               encodedStrLen,
                               (char *)(primaryKeyCol + additionalLen),
                               &outlen,
                               0);

    if (unlikely(result == 0)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) + " " +
       "Encountered error decoding base64. Column: " +
       std::string(col->getName()));
      break;
    } else if (unlikely(result == -1)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) + " " +
        "Encountered error decoding base64; Chosen codec is not part of"
        " current build. Column: " + std::string(col->getName()));
      break;
    }
    if (unlikely(outlen > colDataLen)) {
      // We should not get here as there is a check for it above.
      // This check is here just in case we get here due to
      // programming error.
      error =
          RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
            std::string(" Programming Error. Report Bug.") +
            " Decoded data length is greater than column length." +
            " Column: " + std::string(col->getName()) +
            " Length: " + std::to_string(col->getLength()));
      rdrs_logger::error(error.message);
      break;
    }
    // insert the length at the beginning of the array
    if (col->getType() == NdbDictionary::Column::Varbinary) {
      ((Uint8 *)primaryKeyCol)[0] = (Uint8)(outlen);
    } else if (col->getType() == NdbDictionary::Column::Longvarbinary) {
      ((Uint8 *)primaryKeyCol)[0] = (Uint8)(outlen % 256);
      ((Uint8 *)primaryKeyCol)[1] = (Uint8)(outlen / 256);
    } else {
      error = RS_SERVER_ERROR(std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Datetime: {
    ///< Precision down to 1 sec (sizeof(Datetime) == 8 bytes )
    error = RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) + 
      std::string(" Column: ") + std::string(col->getName()) +
      " Type: " + std::to_string(col->getType()));
    break;
  }
  case NdbDictionary::Column::Date: {
    ///< Precision down to 1 day(sizeof(Date) == 4 bytes )
    const char *dateStr = request->PKValueCStr(colIdx);
    size_t dateStrLen = request->PKValueLen(colIdx);
    MYSQL_TIME lTime;
    MYSQL_TIME_STATUS status;
    bool ret = str_to_datetime(dateStr, dateStrLen, &lTime, 0, &status);
    if (unlikely(ret != 0)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) + 
        std::string(" Column: ") +
        std::string(col->getName()));
      break;
    }
    if (unlikely(lTime.hour != 0 ||
                 lTime.minute != 0 ||
                 lTime.second != 0 ||
                 lTime.second_part != 0)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
        " Expecting only date data. Column: " + std::string(col->getName()));
      break;
    }
    my_date_to_binary(&lTime, (uchar *)primaryKeyCol);
    break;
  }
  case NdbDictionary::Column::Blob: {
    ///< Binary large object (see NdbBlob)
    error = RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) + 
      std::string(" Column: ") + std::string(col->getName()) +
      " Type: " + std::to_string(col->getType()));
    break;
  }
  case NdbDictionary::Column::Text: {
    ///< Text blob
    error = RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) + 
      std::string(" Column: ") + std::string(col->getName()) +
      " Type: " + std::to_string(col->getType()));
    break;
  }
  case NdbDictionary::Column::Bit: {
    ///< Bit, length specifies no of bits
    error = RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) + 
      std::string(" Column: ") + std::string(col->getName()) +
      " Type: " + std::to_string(col->getType()));
    break;
  }
  case NdbDictionary::Column::Time: {
    ///< Time without date
    error = RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) + 
        std::string(" Column: ") + std::string(col->getName()) +
        " Type: " + std::to_string(col->getType()));
    break;
  }
  case NdbDictionary::Column::Year: {
    ///< Year 1901-2155 (1 byte)
    char *parsed = nullptr;
    errno = 0;
    Int64 parsedNumber = strtoll(request->PKValueCStr(colIdx), &parsed, 10);
    if (unlikely(*parsed != '\0' ||
                 errno != 0 ||
                 !(parsedNumber >= 1901 && parsedNumber <= 2155))) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)) + 
        std::string(" Expecting YEAR column. Possible values"
        " [1901-2155]. Column: ") + std::string(request->PKName(colIdx)));
      break;
    }
    Int32 *year = reinterpret_cast<Int32 *>(primaryKeyCol);
    *year = static_cast<Int32>((parsedNumber - 1900));
    break;
  }
  case NdbDictionary::Column::Timestamp: {
    ///< Unix time
    error = RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUG)) + 
      std::string(" Column: ") + std::string(col->getName()) +
      " Type: " + std::to_string(col->getType()));
    break;
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
    require(col->getSizeInBytes() <= TIME2_MAX_SIZE_IN_BYTES);
    const char *timeStr = request->PKValueCStr(colIdx);
    size_t timeStrLen = request->PKValueLen(colIdx);
    MYSQL_TIME lTime;
    MYSQL_TIME_STATUS status;
    bool ret = str_to_time(timeStr, timeStrLen, &lTime, &status, 0);
    if (unlikely(ret != 0)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) + std::string(" Column: ") +
        std::string(col->getName()));
      break;
    }
    int precision = col->getPrecision();
    int warnings = 0;
    my_datetime_adjust_frac(&lTime, precision, &warnings, true);
    if (unlikely(warnings != 0)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) + std::string(" Column: ") +
        std::string(col->getName()));
      break;
    }
    longlong numeric_date_time = TIME_to_longlong_time_packed(lTime);
    my_time_packed_to_binary(numeric_date_time,
                             (uchar *)primaryKeyCol,
                             precision);
    break;
  }
  case NdbDictionary::Column::Datetime2: {
    ///< 5 bytes plus 0-3 fraction
    require(col->getSizeInBytes() <= DATETIME_MAX_SIZE_IN_BYTES);
    const char *dateStr = request->PKValueCStr(colIdx);
    size_t dateStrLen = request->PKValueLen(colIdx);
    MYSQL_TIME lTime;
    MYSQL_TIME_STATUS status;
    bool ret = str_to_datetime(dateStr, dateStrLen, &lTime, 0, &status);
    if (unlikely(ret != 0)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) + 
        std::string(" Column: ") +
        std::string(col->getName()));
      break;
    }
    int precision = col->getPrecision();
    int warnings = 0;
    my_datetime_adjust_frac(&lTime, precision, &warnings, true);
    if (unlikely(warnings != 0)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) + 
        std::string(" Column: ") + std::string(col->getName()));
      break;
    }
    longlong numericDateTime = TIME_to_longlong_datetime_packed(lTime);
    my_datetime_packed_to_binary(numericDateTime,
                                 (uchar *)primaryKeyCol,
                                 precision);
    break;
  }
  case NdbDictionary::Column::Timestamp2: {
    // epoch range 0 , 2147483647
    /// < 4 bytes + 0-3 fraction
    require(col->getSizeInBytes() <= TIMESTAMP2_MAX_SIZE_IN_BYTES);
    const char *tsStr = request->PKValueCStr(colIdx);
    size_t tsStrLen = request->PKValueLen(colIdx);
    uint precision = col->getPrecision();
    MYSQL_TIME lTime;
    MYSQL_TIME_STATUS status;
    bool ret = str_to_datetime(tsStr, tsStrLen, &lTime, 0, &status);
    if (unlikely(ret != 0)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) + 
        std::string(" Column: ") + std::string(col->getName()));
      break;
    }
    time_t epoch = 0;
    errno = 0;
    struct tm time_info;
    time_info.tm_year = lTime.year - 1900;  // tm_year is years since 1900
    time_info.tm_mon = lTime.month - 1;     // tm_mon is 0-based
    time_info.tm_mday = lTime.day;
    time_info.tm_hour = lTime.hour;
    time_info.tm_min = lTime.minute;
    time_info.tm_sec = lTime.second;
    time_info.tm_isdst = -1; // Daylight saving t
    epoch = timegm(&time_info);
    // 1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC.
    if (unlikely(epoch <= 0 || epoch > 2147483647)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) + std::string(" Column: ") +
        std::string(col->getName()));
      break;
    }
    // TODO(salman) 1 apply timezone changes
    // https://dev.mysql.com/doc/refman/8.0/en/datetime.html
    // iMySQL converts TIMESTAMP values from the current time zone to UTC
    // for storage, and back from UTC to the current time zone for retrieval.
    // (This does not occur for other types such as DATETIME.) By default,
    // the current time zone for each connection is the server's time. The
    // time zone can be set on a per-connection basis. As long as the time
    // zone setting remains constant, you get back the same value you store.
    // If you store a TIMESTAMP value, and then change the time zone and
    // retrieve the value, the retrieved value is different from the value
    // you stored. This occurs because the same time zone was not used for
    // conversion in both directions. The current time zone is available as
    // the value of the time_zone system variable.
    // For more information, see Section 5.1.15, “MySQL Server Time Zone
    // Support”.
    // TODO(salman) 2 Investigate how clusterj inserts time stamps. Does it
    // apply time zone changes
    // TODO(salman) how to deal with time zone setting in mysql server
    int warnings = 0;
    my_datetime_adjust_frac(&lTime, precision, &warnings, true);
    if (unlikely(warnings != 0)) {
      error = RS_CLIENT_ERROR(
        std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) + 
        std::string(" Column: ") + std::string(col->getName()));
      break;
    }
    // On Mac timeval.tv_usec is Int32 and on linux it is Int64.
    // Inorder to be compatible we cast l_time.second_part to Int32
    // This will not create problems as only six digit nanoseconds
    // are stored in Timestamp2
    my_timeval myTV{epoch, (Int32)lTime.second_part};
    my_timestamp_to_binary(&myTV, (uchar *)primaryKeyCol, precision);
    break;
  }
  }
  return error;
}

int GetByteArray(const NdbRecAttr *attr, const char **firstByte, Uint32 *bytes) {
  const NdbDictionary::Column::ArrayType arrayType =
    attr->getColumn()->getArrayType();
  const size_t attrBytes = attr->get_size_in_bytes();
  const char *aRef = attr->aRef();
  std::string result;
  switch (arrayType) {
  case NdbDictionary::Column::ArrayTypeFixed:
    /*
       No prefix length is stored in aRef. Data starts from aRef's first byte
       data might be padded with blank or null bytes to fill the whole column
       */
    *firstByte = aRef;
    *bytes = attrBytes;
    return 0;
  case NdbDictionary::Column::ArrayTypeShortVar:
    /*
       First byte of aRef has the length of data stored
       Data starts from second byte of aRef
       */
    *firstByte = aRef + 1;
    *bytes = static_cast<Uint8>(aRef[0]);
    return 0;
  case NdbDictionary::Column::ArrayTypeMediumVar:
    /*
       First two bytes of aRef has the length of data stored
       Data starts from third byte of aRef
       */
    *firstByte = aRef + 2;
    *bytes = static_cast<Uint8>(aRef[1]) * 256 + static_cast<Uint8>(aRef[0]);
    return 0;
  default:
    firstByte = nullptr;
    *bytes    = 0;
    return -1;
  }
}

bool CanRetryOperation(RS_Status status) {
  bool retry = false;
  if (status.http_code != SUCCESS) {
    if (status.classification == NdbError::TemporaryError) {
      retry = true;
    } else if (status.code == 245 /* many active scans */) {
      retry = true;
    } else if (UnloadSchema(status)) {
      retry = true;
    }
  }
  if (retry) {
    rdrs_logger::debug(std::string("Transient error. ") + status.message);
  }
  return retry;
}

bool UnloadSchema(RS_Status status) {
  bool unload = false;
  if (status.http_code != SUCCESS) {
    if (/*Invalid schema object version*/
        (status.mysql_code == HA_ERR_TABLE_DEF_CHANGED &&
         status.code == 241)) {
      unload = true;
    } else if (/*Table is being dropped*/
               (status.mysql_code == HA_ERR_NO_SUCH_TABLE &&
                status.code == 283)) {
      unload = true;
    } else if (/*Table not defined in transaction coordinator*/
               (status.mysql_code == HA_ERR_TABLE_DEF_CHANGED &&
                status.code == 284)) {
      unload = true;
    } else if (/*No such table existed*/
               (status.mysql_code == HA_ERR_NO_SUCH_TABLE &&
                status.code == 709)) {
      unload = true;
    } else if (/*No such table existed*/
               (status.mysql_code == HA_ERR_NO_SUCH_TABLE &&
                status.code == 723)) {
      unload = true;
    } else if (/*Table is being dropped*/
               (status.mysql_code == HA_ERR_NO_SUCH_TABLE &&
                status.code == 1226)) {
      unload = true;
    }
  }
  return unload;
}

Uint32 ExponentialDelayWithJitter(Uint32 retry,
                                  Uint32 initialDelayInMS,
                                  Uint32 jitterInMS) {
  Uint32 expoDelay  = initialDelayInMS * pow(2, retry);
  jitterInMS = std::min(jitterInMS, initialDelayInMS);
  Uint32 randJitter = rand() % jitterInMS;

  Uint32 delay = 0;
  if (rand() % 2 == 0) {
    delay = expoDelay + randJitter;
  } else {
    delay = expoDelay - randJitter;
  }
  return delay;
}

RS_Status HandleSchemaErrors(
  Ndb *ndbObject,
  RS_Status status,
  const std::list<std::tuple<std::string, std::string>> &tables) {

  if (unlikely(status.http_code != SUCCESS)) {
    if (UnloadSchema(status)) {
      for (const std::tuple<std::string, std::string> &table_tup : tables) {
        const char *db = std::get<0>(table_tup).c_str();
        const char *table = std::get<1>(table_tup).c_str();
        ndbObject->setCatalogName(db);
        NdbDictionary::Dictionary *dict = ndbObject->getDictionary();
        // invalidate indexes
        NdbDictionary::Dictionary::List indexes;
        dict->listIndexes(indexes, table);
        for (unsigned i = 0; i < indexes.count; i++) {
          dict->invalidateIndex(indexes.elements[i].name, table);
        }
        // invalidate table
        dict->invalidateTable(table);
        dict->removeCachedTable(table);
        rdrs_logger::info(
          "Unloading schema " + std::string(db) + "/" + std::string(table));
      }
    }
  }
  return RS_OK;
}
