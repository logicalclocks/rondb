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

#include "src/db-operations/pk/common.hpp"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/beast/core/detail/base64.hpp>
#include <decimal_utils.hpp>
#include <NdbError.hpp>
#include <my_time.h>
#include <sql_string.h>
#include <ndb_limits.h>
#include <libbase64.h>
#include <string>
#include <algorithm>
#include <utility>
#include <my_base.h>
#include "NdbOut.hpp"
#include "ndb_types.h"
#include "storage/ndb/include/ndb_global.h"
#include "decimal.h"
#include "my_compiler.h"
#include "src/error-strings.h"
#include "src/status.hpp"
#include "src/mystring.hpp"
#include "src/rdrs-const.h"
#include "src/logger.hpp"

typedef unsigned char uchar;
typedef Uint32 uint32;
static inline uint32 uint3korr(const uchar *A) {
  return static_cast<uint32>((static_cast<uint32>(A[0])) + ((static_cast<uint32>(A[1])) << 8) +
                             ((static_cast<uint32>(A[2])) << 16));
}

inline void my_unpack_date(MYSQL_TIME *l_time, const void *d) {
  uchar b[4];
  memcpy(b, d, 3);
  b[3]        = 0;
  uint w      = (uint)uint3korr(b);
  l_time->day = (w & 31);
  w >>= 5;
  l_time->month = (w & 15);
  w >>= 4;
  l_time->year      = w;
  l_time->time_type = MYSQL_TIMESTAMP_DATE;
}

RS_Status SetOperationPKCol(const NdbDictionary::Column *col, PKRRequest *request, Uint32 colIdx,
                            Int8 **primaryKeyCol, Uint32 *primaryKeySize) {
  RS_Status error = RS_OK;

  switch (col->getType()) {
  case NdbDictionary::Column::Undefined: {
    ///< 4 bytes + 0-3 fraction
    error = RS_CLIENT_ERROR(ERROR_018 + std::string(" Column: ") +
                            std::string(request->PKName(colIdx)));
    break;
  }
  case NdbDictionary::Column::Tinyint: {
    ///< 8 bit. 1 byte signed integer, can be used in array

    char *parsed        = nullptr;
    errno               = 0;
    *primaryKeyCol      = (Int8 *)malloc(sizeof(Int64));
    *primaryKeySize     = sizeof(Int64);
    Int64 *parsedNumber = reinterpret_cast<Int64 *>(*primaryKeyCol);
    *parsedNumber       = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(*parsedNumber >= -128 && *parsedNumber <= 127))) {
      error = RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting TINYINT. Column: ") +
                              std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Tinyunsigned: {
    ///< 8 bit. 1 byte unsigned integer, can be used in array
    char *parsed        = nullptr;
    errno               = 0;
    *primaryKeyCol      = (Int8 *)malloc(sizeof(Int64));
    *primaryKeySize     = sizeof(Int64);
    Int64 *parsedNumber = reinterpret_cast<Int64 *>(*primaryKeyCol);
    *parsedNumber       = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 || !(*parsedNumber >= 0 && *parsedNumber <= 255))) {
      error = RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting TINYINT UNSIGNED. Column: ") +
                              std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Smallint: {
    ///< 16 bit. 2 byte signed integer, can be used in array
    char *parsed        = nullptr;
    errno               = 0;
    *primaryKeyCol      = (Int8 *)malloc(sizeof(Int64));
    *primaryKeySize     = sizeof(Int64);
    Int64 *parsedNumber = reinterpret_cast<Int64 *>(*primaryKeyCol);
    *parsedNumber       = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(*parsedNumber >= -32768 && *parsedNumber <= 32767))) {
      error = RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting SMALLINT. Column: ") +
                              std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Smallunsigned: {
    ///< 16 bit. 2 byte unsigned integer, can be used in array

    char *parsed        = nullptr;
    errno               = 0;
    *primaryKeyCol      = (Int8 *)malloc(sizeof(Int64));
    *primaryKeySize     = sizeof(Int64);
    Int64 *parsedNumber = reinterpret_cast<Int64 *>(*primaryKeyCol);
    *parsedNumber       = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(*parsedNumber >= 0 && *parsedNumber <= 65535))) {
      error = RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting SMALLINT UNSIGNED. Column: ") +
                              std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Mediumint: {
    ///< 24 bit. 3 byte signed integer, can be used in array

    char *parsed        = nullptr;
    errno               = 0;
    *primaryKeyCol      = (Int8 *)malloc(sizeof(Int64));
    *primaryKeySize     = sizeof(Int64);
    Int64 *parsedNumber = reinterpret_cast<Int64 *>(*primaryKeyCol);
    *parsedNumber       = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(*parsedNumber >= -8388608 && *parsedNumber <= 8388607))) {
      error = RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting MEDIUMINT. Column: ") +
                              std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Mediumunsigned: {
    ///< 24 bit. 3 byte unsigned integer, can be used in array

    char *parsed        = nullptr;
    errno               = 0;
    *primaryKeyCol      = (Int8 *)malloc(sizeof(Int64));
    *primaryKeySize     = sizeof(Int64);
    Int64 *parsedNumber = reinterpret_cast<Int64 *>(*primaryKeyCol);
    *parsedNumber       = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(*parsedNumber >= 0 && *parsedNumber <= 16777215))) {
      error = RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting MEDIUMINT UNSIGNED. Column: ") +
                              std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Int: {
    ///< 32 bit. 4 byte signed integer, can be used in array
    char *parsed        = nullptr;
    errno               = 0;
    *primaryKeyCol      = (Int8 *)malloc(sizeof(Int64));
    *primaryKeySize     = sizeof(Int64);
    Int64 *parsedNumber = reinterpret_cast<Int64 *>(*primaryKeyCol);
    *parsedNumber       = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(*parsedNumber >= -2147483648 && *parsedNumber <= 2147483647))) {
      error = RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting INT. Column: ") +
                              std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Unsigned: {
    ///< 32 bit. 4 byte unsigned integer, can be used in array

    char *parsed        = nullptr;
    errno               = 0;
    *primaryKeyCol      = (Int8 *)malloc(sizeof(Int64));
    *primaryKeySize     = sizeof(Int64);
    Int64 *parsedNumber = reinterpret_cast<Int64 *>(*primaryKeyCol);
    *parsedNumber       = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(*parsedNumber >= 0 && *parsedNumber <= 4294967295))) {
      error = RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting INT UNSIGNED. Column: ") +
                              std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Bigint: {
    ///< 64 bit. 8 byte signed integer, can be used in array

    char *parsed        = nullptr;
    errno               = 0;
    *primaryKeyCol      = (Int8 *)malloc(sizeof(Int64));
    *primaryKeySize     = sizeof(Int64);
    Int64 *parsedNumber = reinterpret_cast<Int64 *>(*primaryKeyCol);
    *parsedNumber       = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0)) {
      error = RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting BIGINT. Column: ") +
                              std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Bigunsigned: {
    ///< 64 Bit. 8 byte signed integer, can be used in array
    char *parsed         = nullptr;
    errno                = 0;
    *primaryKeyCol       = (Int8 *)malloc(sizeof(Int64));
    *primaryKeySize      = sizeof(Int64);
    Uint64 *parsedNumber = reinterpret_cast<Uint64 *>(*primaryKeyCol);
    *parsedNumber        = strtoull(request->PKValueCStr(colIdx), &parsed, 10);

    const std::string numStr = std::string(request->PKValueCStr(colIdx));
    if (unlikely(*parsed != '\0' || errno != 0 || numStr.find('-') != std::string::npos)) {
      error = RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting BIGINT UNSIGNED. Column: ") +
                              std::string(request->PKName(colIdx)));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Float: {
    ///< 32-bit float. 4 bytes float, can be used in array
    error = RS_CLIENT_ERROR(ERROR_017 + std::string(" Column: ") +
                            std::string(request->PKName(colIdx)));
    break;
  }
  case NdbDictionary::Column::Double: {
    ///< 64-bit float. 8 byte float, can be used in array
    error = RS_CLIENT_ERROR(ERROR_017 + std::string(" Column: ") +
                            std::string(request->PKName(colIdx)));
    break;
  }
  case NdbDictionary::Column::Olddecimal: {
    ///< MySQL < 5.0 signed decimal,  Precision, Scale
    error = RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                            " Type: " + std::to_string(col->getType()));
    break;
  }
  case NdbDictionary::Column::Olddecimalunsigned: {
    error = RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                            " Type: " + std::to_string(col->getType()));
    break;
  }
  case NdbDictionary::Column::Decimalunsigned: {
    ///< MySQL >= 5.0 signed decimal,  Precision, Scale
    const std::string decStr = std::string(request->PKValueCStr(colIdx));
    if (unlikely(decStr.find('-') != std::string::npos)) {
      error = RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting DECIMAL UNSIGNED. Column: ") +
                              std::string(request->PKName(colIdx)));
      break;
    }
    [[fallthrough]];
  }
  case NdbDictionary::Column::Decimal: {
    int precision      = col->getPrecision();
    int scale          = col->getScale();
    const char *decStr = request->PKValueCStr(colIdx);

    *primaryKeyCol  = (Int8 *)malloc(DECIMAL_MAX_SIZE_IN_BYTES);
    *primaryKeySize = DECIMAL_MAX_SIZE_IN_BYTES;
    if (unlikely(decimal_str2bin(decStr, strlen(decStr), precision, scale, *primaryKeyCol,
                                 *primaryKeySize) != 0)) {
      error = RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting Decimal with Precision: ") +
                              std::to_string(precision) + std::string(" and Scale: ") +
                              std::to_string(scale));
      break;
    }
    break;
  }
  case NdbDictionary::Column::Char: {
    /// A fix sized array of characters
    /// size of a character depends on encoding scheme

    const char *dataStr  = request->PKValueCStr(colIdx);
    const int dataStrLen = request->PKValueLen(colIdx);
    const int colMaxLen  = col->getSizeInBytes();
    if (unlikely(dataStrLen > colMaxLen)) {
      error = RS_CLIENT_ERROR(
          std::string(ERROR_008) +
          " Data length is greater than column length. Column: " + std::string(col->getName()));
      break;
    }

    // operation->equal expects a zero-padded char string
    *primaryKeyCol  = (Int8 *)malloc(colMaxLen);
    *primaryKeySize = colMaxLen;
    memcpy(*primaryKeyCol, dataStr, dataStrLen);
    memset(*primaryKeyCol + dataStrLen, 0, colMaxLen - dataStrLen);

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

    *primaryKeySize = request->PKValueLen(colIdx);
    if (unlikely(*primaryKeySize > (Uint32)col->getLength())) {
      error = RS_CLIENT_ERROR(
          std::string(ERROR_008) + " Data length is greater than column length. Data length:" +
          std::to_string(*primaryKeySize) + "Column: " + std::string(col->getName()) +
          " Column length: " + std::to_string(col->getLength()));
      break;
    }

    *primaryKeyCol = (Int8 *)malloc((*primaryKeySize + additionalLen) * sizeof(Int8));
    memcpy(*primaryKeyCol + additionalLen, request->PKValueCStr(colIdx), *primaryKeySize);

    if (col->getType() == NdbDictionary::Column::Varchar) {
      ((Uint8 *)*primaryKeyCol)[0] = (Uint8)(*primaryKeySize);
    } else if (col->getType() == NdbDictionary::Column::Longvarchar) {
      ((Uint8 *)*primaryKeyCol)[0] = (Uint8)(*primaryKeySize % 256);
      ((Uint8 *)*primaryKeyCol)[1] = (Uint8)(*primaryKeySize / 256);
    } else {
      error = RS_SERVER_ERROR(ERROR_015);
      break;
    }

    break;
  }
  case NdbDictionary::Column::Binary: {
    /// Binary data is sent as base64 string
    require(col->getLength() <= BINARY_MAX_SIZE_IN_BYTES);
    const char *encodedStr     = request->PKValueCStr(colIdx);
    const size_t encodedStrLen = request->PKValueLen(colIdx);

    // The buffer in out has been allocated by the caller and is at least 3/4 the size of the input.

    // Encoding takes 3 decoded bytes at a time and turns them into 4 encoded bytes.
    // The encoded string is therefore always a multiple of 4.
    const size_t maxConversions =
        BINARY_MAX_SIZE_IN_BYTES / 3 + (BINARY_MAX_SIZE_IN_BYTES % 3 != 0);  // basically ceiling()
    const size_t maxEncodedSize = 4 * maxConversions;

    if (unlikely(encodedStrLen > maxEncodedSize)) {
      error = RS_CLIENT_ERROR(std::string(ERROR_008) + " " +
                              "Encoded data length is greater than 4/3 of maximum binary size." +
                              " Column: " + std::string(col->getName()) +
                              " Maximum binary size: " + std::to_string(BINARY_MAX_SIZE_IN_BYTES));
      break;
    }

    *primaryKeySize = col->getSizeInBytes();
    *primaryKeyCol  = (Int8 *)malloc(*primaryKeySize);
    memset(*primaryKeyCol, 0, *primaryKeySize);

    size_t outlen = 0;
    int result    = base64_decode(encodedStr, encodedStrLen, (char *)*primaryKeyCol, &outlen, 0);

    if (unlikely(result == 0)) {
      error = RS_CLIENT_ERROR(
          std::string(ERROR_008) + " " +
          "Encountered error decoding base64. Column: " + std::string(col->getName()));
      break;
    } else if (unlikely(result == -1)) {
      error = RS_CLIENT_ERROR(
          std::string(ERROR_008) + " " +
          "Encountered error decoding base64; Chosen codec is not part of current build. Column: " +
          std::string(col->getName()));
      break;
    }

    if (unlikely(outlen > *primaryKeySize)) {
      error = RS_CLIENT_ERROR(std::string(ERROR_008) + " " +
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

    const size_t maxColLen     = col->getSizeInBytes();  // this includes size prefix
    size_t colDataLen          = col->getLength();       // this is without size prefix
    const char *encodedStr     = request->PKValueCStr(colIdx);
    const size_t encodedStrLen = request->PKValueLen(colIdx);

    // Encoding takes 3 decoded bytes at a time and turns them into 4 encoded bytes.
    // The encoded string is therefore always a multiple of 4.
    const size_t maxConversions = colDataLen / 3 + (colDataLen % 3 != 0);  // basically ceiling()
    const size_t maxEncodedSize = 4 * maxConversions;

    if (unlikely(encodedStrLen > maxEncodedSize)) {
      error = RS_CLIENT_ERROR(std::string(ERROR_008) +
                              " Encoded data length is greater than 4/3 of maximum binary size." +
                              " Column: " + std::string(col->getName()) +
                              " Maximum binary size: " + std::to_string(colDataLen));
      break;
    }

    int additionalLen = 1;
    if (col->getType() == NdbDictionary::Column::Longvarbinary) {
      additionalLen = 2;
    }
    *primaryKeyCol = (Int8 *)malloc(maxColLen);

    size_t outlen = 0;
    // leave first 1-2 bytes free for saving length bytes
    int result = base64_decode(encodedStr, encodedStrLen, (char *)(*primaryKeyCol + additionalLen),
                               &outlen, 0);

    if (unlikely(result == 0)) {
      error = RS_CLIENT_ERROR(
          std::string(ERROR_008) + " " +
          "Encountered error decoding base64. Column: " + std::string(col->getName()));
      break;
    } else if (unlikely(result == -1)) {
      error = RS_CLIENT_ERROR(
          std::string(ERROR_008) + " " +
          "Encountered error decoding base64; Chosen codec is not part of current build. Column: " +
          std::string(col->getName()));
      break;
    }

    if (unlikely(outlen > colDataLen)) {
      // We should not get here as there is a check for it above.
      // This check is here just in case we get here due to
      // programming error.
      error =
          RS_CLIENT_ERROR(std::string(ERROR_008) + std::string(" Programming Error. Report Bug.") +
                          " Decoded data length is greater than column length." +
                          " Column: " + std::string(col->getName()) +
                          " Length: " + std::to_string(col->getLength()));

      LOG_ERROR(error.message);
      break;
    }

    // insert the length at the beginning of the array
    if (col->getType() == NdbDictionary::Column::Varbinary) {
      ((Uint8 *)*primaryKeyCol)[0] = (Uint8)(outlen);
    } else if (col->getType() == NdbDictionary::Column::Longvarbinary) {
      ((Uint8 *)*primaryKeyCol)[0] = (Uint8)(outlen % 256);
      ((Uint8 *)*primaryKeyCol)[1] = (Uint8)(outlen / 256);
    } else {
      error = RS_SERVER_ERROR(ERROR_015);
      break;
    }

    *primaryKeySize = outlen + additionalLen;
    break;
  }
  case NdbDictionary::Column::Datetime: {
    ///< Precision down to 1 sec (sizeof(Datetime) == 8 bytes )
    error = RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                            " Type: " + std::to_string(col->getType()));
    break;
  }
  case NdbDictionary::Column::Date: {
    ///< Precision down to 1 day(sizeof(Date) == 4 bytes )
    const char *dateStr = request->PKValueCStr(colIdx);
    size_t dateStrLen   = request->PKValueLen(colIdx);

    MYSQL_TIME lTime;
    MYSQL_TIME_STATUS status;
    bool ret = str_to_datetime(dateStr, dateStrLen, &lTime, 0, &status);
    if (unlikely(ret != 0)) {
      error = RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                              std::string(col->getName()));
      break;
    }

    if (unlikely(lTime.hour != 0 || lTime.minute != 0 || lTime.second != 0 ||
                 lTime.second_part != 0)) {
      error = RS_CLIENT_ERROR(std::string(ERROR_008) +
                              " Expecting only date data. Column: " + std::string(col->getName()));
      break;
    }

    *primaryKeyCol  = (Int8 *)malloc(DATE_MAX_SIZE_IN_BYTES);
    *primaryKeySize = col->getSizeInBytes();
    my_date_to_binary(&lTime, (uchar *)*primaryKeyCol);
    break;
  }
  case NdbDictionary::Column::Blob: {
    ///< Binary large object (see NdbBlob)
    error = RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                            " Type: " + std::to_string(col->getType()));
    break;
  }
  case NdbDictionary::Column::Text: {
    ///< Text blob
    error = RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                            " Type: " + std::to_string(col->getType()));
    break;
  }
  case NdbDictionary::Column::Bit: {
    ///< Bit, length specifies no of bits
    error = RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                            " Type: " + std::to_string(col->getType()));
    break;
  }
  case NdbDictionary::Column::Time: {
    ///< Time without date
    error = RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                            " Type: " + std::to_string(col->getType()));
    break;
  }
  case NdbDictionary::Column::Year: {
    ///< Year 1901-2155 (1 byte)

    char *parsed       = nullptr;
    errno              = 0;
    Int64 parsedNumber = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(parsedNumber >= 1901 && parsedNumber <= 2155))) {
      error = RS_CLIENT_ERROR(
          ERROR_015 + std::string(" Expecting YEAR column. Possible values [1901-2155]. Column: ") +
          std::string(request->PKName(colIdx)));
      break;
    }

    *primaryKeyCol  = (Int8 *)malloc(4);
    *primaryKeySize = 4;
    Int32 *year     = reinterpret_cast<Int32 *>(*primaryKeyCol);
    *year           = static_cast<Int32>((parsedNumber - 1900));
    break;
  }
  case NdbDictionary::Column::Timestamp: {
    ///< Unix time
    error = RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
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
    size_t timeStrLen   = request->PKValueLen(colIdx);

    MYSQL_TIME lTime;
    MYSQL_TIME_STATUS status;
    bool ret = str_to_time(timeStr, timeStrLen, &lTime, &status, 0);
    if (unlikely(ret != 0)) {
      error = RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                              std::string(col->getName()));
      break;
    }

    *primaryKeyCol  = (Int8 *)malloc(TIME2_MAX_SIZE_IN_BYTES);
    *primaryKeySize = col->getSizeInBytes();
    int precision   = col->getPrecision();

    int warnings = 0;
    my_datetime_adjust_frac(&lTime, precision, &warnings, true);
    if (unlikely(warnings != 0)) {
      error = RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                              std::string(col->getName()));
      break;
    }

    longlong numeric_date_time = TIME_to_longlong_time_packed(lTime);
    my_time_packed_to_binary(numeric_date_time, (uchar *)*primaryKeyCol, precision);

    break;
  }
  case NdbDictionary::Column::Datetime2: {
    ///< 5 bytes plus 0-3 fraction
    require(col->getSizeInBytes() <= DATETIME_MAX_SIZE_IN_BYTES);

    const char *dateStr = request->PKValueCStr(colIdx);
    size_t dateStrLen   = request->PKValueLen(colIdx);

    MYSQL_TIME lTime;
    MYSQL_TIME_STATUS status;
    bool ret = str_to_datetime(dateStr, dateStrLen, &lTime, 0, &status);
    if (unlikely(ret != 0)) {
      error = RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                              std::string(col->getName()));
      break;
    }

    int precision = col->getPrecision();
    int warnings  = 0;
    my_datetime_adjust_frac(&lTime, precision, &warnings, true);
    if (unlikely(warnings != 0)) {
      error = RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                              std::string(col->getName()));
      break;
    }

    longlong numericDateTime = TIME_to_longlong_datetime_packed(lTime);

    *primaryKeyCol  = (Int8 *)malloc(DATETIME_MAX_SIZE_IN_BYTES);
    *primaryKeySize = col->getSizeInBytes();
    my_datetime_packed_to_binary(numericDateTime, (uchar *)*primaryKeyCol, precision);

    break;
  }
  case NdbDictionary::Column::Timestamp2: {
    // epoch range 0 , 2147483647
    /// < 4 bytes + 0-3 fraction
    require(col->getSizeInBytes() <= TIMESTAMP2_MAX_SIZE_IN_BYTES);
    const char *tsStr = request->PKValueCStr(colIdx);
    size_t tsStrLen   = request->PKValueLen(colIdx);
    uint precision    = col->getPrecision();

    MYSQL_TIME lTime;
    MYSQL_TIME_STATUS status;
    bool ret = str_to_datetime(tsStr, tsStrLen, &lTime, 0, &status);
    if (unlikely(ret != 0)) {
      error = RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                              std::string(col->getName()));
      break;
    }

    time_t epoch = 0;
    errno        = 0;
    try {
      char btsStr[MAX_DATE_STRING_REP_LENGTH];
      snprintf(btsStr, MAX_DATE_STRING_REP_LENGTH, "%d-%d-%d %d:%d:%d", lTime.year, lTime.month,
               lTime.day, lTime.hour, lTime.minute, lTime.second);
      boost::posix_time::ptime bt(boost::posix_time::time_from_string(std::string(btsStr)));
      boost::posix_time::ptime start(boost::gregorian::date(1970, 1, 1));
      boost::posix_time::time_duration dur = bt - start;
      epoch                                = dur.total_seconds();
    } catch (...) {
      error = RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                              std::string(col->getName()));
      break;
    }

    // 1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC.
    if (unlikely(epoch <= 0 || epoch > 2147483647)) {
      error = RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                              std::string(col->getName()));
      break;
    }

    // TODO(salman) 1 apply timezone changes
    // https://dev.mysql.com/doc/refman/8.0/en/datetime.html
    // iMySQL converts TIMESTAMP values from the current time zone to UTC for storage, and back from
    // UTC to the current time zone for retrieval. (This does not occur for other types such as
    // DATETIME.) By default, the current time zone for each connection is the server's time. The
    // time zone can be set on a per-connection basis. As long as the time zone setting remains
    // constant, you get back the same value you store. If you store a TIMESTAMP value, and then
    // change the time zone and retrieve the value, the retrieved value is different from the value
    // you stored. This occurs because the same time zone was not used for conversion in both
    // directions. The current time zone is available as the value of the time_zone system variable.
    // For more information, see Section 5.1.15, “MySQL Server Time Zone Support”.
    // TODO(salman) 2 Investigate how clusterj inserts time stamps. Does it apply time zone changes
    // TODO(salman) how to deal with time zone setting in mysql server
    //

    int warnings = 0;
    my_datetime_adjust_frac(&lTime, precision, &warnings, true);
    if (unlikely(warnings != 0)) {
      error = RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                              std::string(col->getName()));
      break;
    }

    *primaryKeyCol  = (Int8 *)malloc(TIMESTAMP2_MAX_SIZE_IN_BYTES);
    *primaryKeySize = col->getSizeInBytes();

    // On Mac timeval.tv_usec is Int32 and on linux it is Int64.
    // Inorder to be compatible we cast l_time.second_part to Int32
    // This will not create problems as only six digit nanoseconds
    // are stored in Timestamp2
    timeval myTV{epoch, (Int32)lTime.second_part};
    my_timestamp_to_binary(&myTV, (uchar *)*primaryKeyCol, precision);

    break;
  }
  }

  return error;
}

RS_Status WriteColToRespBuff(const NdbRecAttr *attr, PKRResponse *response) {
  const NdbDictionary::Column *col = attr->getColumn();
  if (attr->isNULL()) {
    return response->SetColumnDataNull(attr->getColumn()->getName());
  }

  switch (col->getType()) {
  case NdbDictionary::Column::Undefined: {
    ///< 4 bytes + 0-3 fraction
    return RS_CLIENT_ERROR(ERROR_018 + std::string(" Column: ") + std::string(col->getName()));
  }
  case NdbDictionary::Column::Tinyint: {
    ///< 8 bit. 1 byte signed integer, can be used in array
    return response->Append_i8(attr->getColumn()->getName(), attr->int8_value());
  }
  case NdbDictionary::Column::Tinyunsigned: {
    ///< 8 bit. 1 byte unsigned integer, can be used in array
    return response->Append_iu8(attr->getColumn()->getName(), attr->u_8_value());
  }
  case NdbDictionary::Column::Smallint: {
    ///< 16 bit. 2 byte signed integer, can be used in array
    return response->Append_i16(attr->getColumn()->getName(), attr->short_value());
  }
  case NdbDictionary::Column::Smallunsigned: {
    ///< 16 bit. 2 byte unsigned integer, can be used in array
    return response->Append_iu16(attr->getColumn()->getName(), attr->u_short_value());
  }
  case NdbDictionary::Column::Mediumint: {
    ///< 24 bit. 3 byte signed integer, can be used in array
    return response->Append_i24(attr->getColumn()->getName(), attr->medium_value());
  }
  case NdbDictionary::Column::Mediumunsigned: {
    ///< 24 bit. 3 byte unsigned integer, can be used in array
    return response->Append_iu24(attr->getColumn()->getName(), attr->u_medium_value());
  }
  case NdbDictionary::Column::Int: {
    ///< 32 bit. 4 byte signed integer, can be used in array
    return response->Append_i32(attr->getColumn()->getName(), attr->int32_value());
  }
  case NdbDictionary::Column::Unsigned: {
    ///< 32 bit. 4 byte unsigned integer, can be used in array
    return response->Append_iu32(attr->getColumn()->getName(), attr->u_32_value());
  }
  case NdbDictionary::Column::Bigint: {
    ///< 64 bit. 8 byte signed integer, can be used in array
    return response->Append_i64(attr->getColumn()->getName(), attr->int64_value());
  }
  case NdbDictionary::Column::Bigunsigned: {
    ///< 64 Bit. 8 byte signed integer, can be used in array
    return response->Append_iu64(attr->getColumn()->getName(), attr->u_64_value());
  }
  case NdbDictionary::Column::Float: {
    ///< 32-bit float. 4 bytes float, can be used in array
    return response->Append_f32(attr->getColumn()->getName(), attr->float_value());
  }
  case NdbDictionary::Column::Double: {
    ///< 64-bit float. 8 byte float, can be used in array
    return response->Append_d64(attr->getColumn()->getName(), attr->double_value());
  }
  case NdbDictionary::Column::Olddecimal: {
    ///< MySQL < 5.0 signed decimal,  Precision, Scale
    return RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                           " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Olddecimalunsigned: {
    ///< MySQL < 5.0 signed decimal,  Precision, Scale
    return RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                           " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Decimal:
    ///< MySQL >= 5.0 signed decimal,  Precision, Scale
    [[fallthrough]];
  case NdbDictionary::Column::Decimalunsigned: {
    char decStr[DECIMAL_MAX_STR_LEN_IN_BYTES];
    int precision = attr->getColumn()->getPrecision();
    int scale     = attr->getColumn()->getScale();
    void *bin     = attr->aRef();
    int binLen    = attr->get_size_in_bytes();
    decimal_bin2str(bin, binLen, precision, scale, decStr, DECIMAL_MAX_STR_LEN_IN_BYTES);
    return response->Append_string(attr->getColumn()->getName(), std::string(decStr),
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
    Uint32 attrBytes;
    const char *dataStart = nullptr;
    if (unlikely(GetByteArray(attr, &dataStart, &attrBytes) != 0)) {
      return RS_CLIENT_ERROR(ERROR_019);
    } else {
      return response->Append_char(attr->getColumn()->getName(), dataStart, attrBytes,
                                   attr->getColumn()->getCharset());
    }
  }
  case NdbDictionary::Column::Binary:
    [[fallthrough]];
  case NdbDictionary::Column::Varbinary:
    ///< Length bytes: 1, Max: 255
    [[fallthrough]];
  case NdbDictionary::Column::Longvarbinary: {
    ///< Length bytes: 2, little-endian
    Uint32 attrBytes;
    const char *dataStart = nullptr;
    if (unlikely(GetByteArray(attr, &dataStart, &attrBytes) != 0)) {
      return RS_CLIENT_ERROR(ERROR_019);
    } else {
      require(attrBytes <= MAX_TUPLE_SIZE_IN_BYTES);
      char buffer[MAX_TUPLE_SIZE_IN_BYTES_ENCODED];

      size_t outlen = 0;
      base64_encode(dataStart, attrBytes, (char *)&buffer[0], &outlen, 0);

      return response->Append_string(attr->getColumn()->getName(), std::string(buffer, outlen),
                                     RDRS_BINARY_DATATYPE);
    }
  }
  case NdbDictionary::Column::Datetime: {
    ///< Precision down to 1 sec (sizeof(Datetime) == 8 bytes )
    return RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                           " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Date: {
    ///< Precision down to 1 day(sizeof(Date) == 4 bytes )
    MYSQL_TIME lTime;
    my_unpack_date(&lTime, attr->aRef());
    char to[MAX_DATE_STRING_REP_LENGTH];
    my_date_to_str(lTime, to);
    return response->Append_string(attr->getColumn()->getName(), std::string(to),
                                   RDRS_DATETIME_DATATYPE);
  }
  case NdbDictionary::Column::Blob: {
    ///< Binary large object (see NdbBlob)
    return RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                           " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Text: {
    ///< Text blob
    return RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                           " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Bit: {
    //< Bit, length specifies no of bits
    Uint32 words = attr->getColumn()->getLength() / 8;
    if (attr->getColumn()->getLength() % 8 != 0) {
      words += 1;
    }
    require(words <= BIT_MAX_SIZE_IN_BYTES);

    // change endieness
    int i = 0;
    char reversed[BIT_MAX_SIZE_IN_BYTES];
    for (int j = words - 1; j >= 0; j--) {
      reversed[i++] = attr->aRef()[j];
    }

    char buffer[BIT_MAX_SIZE_IN_BYTES_ENCODED];

    size_t outlen = 0;
    base64_encode(reversed, words, (char *)&buffer[0], &outlen, 0);

    return response->Append_string(attr->getColumn()->getName(), std::string(buffer, outlen),
                                   RDRS_BIT_DATATYPE);
  }
  case NdbDictionary::Column::Time: {
    ///< Time without date
    return RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                           " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Year: {
    ///< Year 1901-2155 (1 byte)
    Int32 year = (uint)(1900 + attr->aRef()[0]);
    return response->Append_i32(attr->getColumn()->getName(), year);
  }
  case NdbDictionary::Column::Timestamp: {
    ///< Unix time
    return RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
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
        my_time_packed_from_binary((const unsigned char *)attr->aRef(), precision);

    MYSQL_TIME lTime;
    TIME_from_longlong_time_packed(&lTime, numericTime);

    char to[MAX_DATE_STRING_REP_LENGTH];
    my_TIME_to_str(lTime, to, precision);

    return response->Append_string(attr->getColumn()->getName(), std::string(to),
                                   RDRS_DATETIME_DATATYPE);
  }
  case NdbDictionary::Column::Datetime2: {
    ///< 5 bytes plus 0-3 fraction
    uint precision = col->getPrecision();

    longlong numericDate =
        my_datetime_packed_from_binary((const unsigned char *)attr->aRef(), precision);

    MYSQL_TIME lTime;
    TIME_from_longlong_datetime_packed(&lTime, numericDate);

    char to[MAX_DATE_STRING_REP_LENGTH];
    my_TIME_to_str(lTime, to, precision);

    return response->Append_string(attr->getColumn()->getName(), std::string(to),
                                   RDRS_DATETIME_DATATYPE);
  }
  case NdbDictionary::Column::Timestamp2: {
    ///< 4 bytes + 0-3 fraction
    uint precision = col->getPrecision();

    timeval myTV{};
    my_timestamp_from_binary(&myTV, (const unsigned char *)attr->aRef(), precision);

    Int64 epochIn = myTV.tv_sec;
    std::time_t stdtime(epochIn);
    boost::posix_time::ptime ts = boost::posix_time::from_time_t(stdtime);

    MYSQL_TIME lTime  = {};
    lTime.year        = ts.date().year();
    lTime.month       = ts.date().month();
    lTime.day         = ts.date().day();
    lTime.hour        = ts.time_of_day().hours();
    lTime.minute      = ts.time_of_day().minutes();
    lTime.second      = ts.time_of_day().seconds();
    lTime.second_part = myTV.tv_usec;
    lTime.time_type   = MYSQL_TIMESTAMP_DATETIME;

    char to[MAX_DATE_STRING_REP_LENGTH];
    my_TIME_to_str(lTime, to, precision);

    return response->Append_string(attr->getColumn()->getName(), std::string(to),
                                   RDRS_DATETIME_DATATYPE);
  }
  }

  return RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                         " Type: " + std::to_string(col->getType()));
}

int GetByteArray(const NdbRecAttr *attr, const char **firstByte, Uint32 *bytes) {
  const NdbDictionary::Column::ArrayType arrayType = attr->getColumn()->getArrayType();
  const size_t attrBytes                           = attr->get_size_in_bytes();
  const char *aRef                                 = attr->aRef();
  std::string result;

  switch (arrayType) {
  case NdbDictionary::Column::ArrayTypeFixed:
    /*
       No prefix length is stored in aRef. Data starts from aRef's first byte
       data might be padded with blank or null bytes to fill the whole column
       */
    *firstByte = aRef;
    *bytes     = attrBytes;
    return 0;
  case NdbDictionary::Column::ArrayTypeShortVar:
    /*
       First byte of aRef has the length of data stored
       Data starts from second byte of aRef
       */
    *firstByte = aRef + 1;
    *bytes     = static_cast<Uint8>(aRef[0]);
    return 0;
  case NdbDictionary::Column::ArrayTypeMediumVar:
    /*
       First two bytes of aRef has the length of data stored
       Data starts from third byte of aRef
       */
    *firstByte = aRef + 2;
    *bytes     = static_cast<Uint8>(aRef[1]) * 256 + static_cast<Uint8>(aRef[0]);
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
    LOG_DEBUG(std::string("Transient error. ") + status.message);
  }
  return retry;
}

bool UnloadSchema(RS_Status status) {
  bool unload = false;
  if (status.http_code != SUCCESS) {
    if (/*Invalid schema object version*/
        (status.mysql_code == HA_ERR_TABLE_DEF_CHANGED && status.code == 241)) {
      unload = true;
    } else if (/*Table is being dropped*/
               (status.mysql_code == HA_ERR_NO_SUCH_TABLE && status.code == 283)) {
      unload = true;
    } else if (/*Table not defined in transaction coordinator*/
               (status.mysql_code == HA_ERR_TABLE_DEF_CHANGED && status.code == 284)) {
      unload = true;
    } else if (/*No such table existed*/
               (status.mysql_code == HA_ERR_NO_SUCH_TABLE && status.code == 709)) {
      unload = true;
    } else if (/*No such table existed*/
               (status.mysql_code == HA_ERR_NO_SUCH_TABLE && status.code == 723)) {
      unload = true;
    } else if (/*Table is being dropped*/
               (status.mysql_code == HA_ERR_NO_SUCH_TABLE && status.code == 1226)) {
      unload = true;
    }
  }
  return unload;
}

Uint32 ExponentialDelayWithJitter(Uint32 retry, Uint32 initialDelayInMS, Uint32 jitterInMS) {
  Uint32 expoDelay  = initialDelayInMS * pow(2, retry);
  jitterInMS        = std::min(jitterInMS, initialDelayInMS);
  Uint32 randJitter = rand() % jitterInMS;

  Uint32 delay = 0;
  if (rand() % 2 == 0) {
    delay = expoDelay + randJitter;
  } else {
    delay = expoDelay - randJitter;
  }
  return delay;
}
