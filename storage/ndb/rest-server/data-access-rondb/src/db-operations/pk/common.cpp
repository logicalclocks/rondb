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
#include <string>
#include <algorithm>
#include <utility>
#include <util/require.h>
#include <my_base.h>
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

/** Prepare the Operation object by setting the PK value that we are querying for
 *
 * @param col information of column that we're querying
 * @param operation the RonDB operation that we wish to prepare
 * @param request the incoming request from the REST API server
 * @param colIdx the scale
 * @return the REST API status of performing the operation
 */
RS_Status SetOperationPKCol(const NdbDictionary::Column *col, NdbOperation *operation,
                            PKRRequest *request, Uint32 colIdx) {
  switch (col->getType()) {
  case NdbDictionary::Column::Undefined: {
    ///< 4 bytes + 0-3 fraction
    return RS_CLIENT_ERROR(ERROR_018 + std::string(" Column: ") +
                           std::string(request->PKName(colIdx)));
  }
  case NdbDictionary::Column::Tinyint: {
    ///< 8 bit. 1 byte signed integer, can be used in array

    char *parsed        = nullptr;
    errno               = 0;
    Int64 parsed_number = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(parsed_number >= -128 && parsed_number <= 127))) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting TINYINT. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else if (unlikely(operation->equal(request->PKName(colIdx),
                                         static_cast<Int8>(parsed_number)) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Tinyunsigned: {
    ///< 8 bit. 1 byte unsigned integer, can be used in array
    char *parsed        = nullptr;
    errno               = 0;
    Int64 parsed_number = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 || !(parsed_number >= 0 && parsed_number <= 255))) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting TINYINT UNSIGNED. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else if (unlikely(operation->equal(request->PKName(colIdx),
                                         static_cast<Uint8>(parsed_number)) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Smallint: {
    ///< 16 bit. 2 byte signed integer, can be used in array
    char *parsed        = nullptr;
    errno               = 0;
    Int64 parsed_number = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(parsed_number >= -32768 && parsed_number <= 32767))) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting SMALLINT. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else if (unlikely(operation->equal(request->PKName(colIdx), (Int16)parsed_number) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Smallunsigned: {
    ///< 16 bit. 2 byte unsigned integer, can be used in array

    char *parsed        = nullptr;
    errno               = 0;
    Int64 parsed_number = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(parsed_number >= 0 && parsed_number <= 65535))) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting SMALLINT UNSIGNED. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else if (unlikely(operation->equal(request->PKName(colIdx), (Uint16)parsed_number) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }

    return RS_OK;
  }
  case NdbDictionary::Column::Mediumint: {
    ///< 24 bit. 3 byte signed integer, can be used in array

    char *parsed        = nullptr;
    errno               = 0;
    Int64 parsed_number = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(parsed_number >= -8388608 && parsed_number <= 8388607))) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting MEDIUMINT. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else if (unlikely(operation->equal(request->PKName(colIdx),
                                         static_cast<Int32>(parsed_number)) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Mediumunsigned: {
    ///< 24 bit. 3 byte unsigned integer, can be used in array

    char *parsed        = nullptr;
    errno               = 0;
    Int64 parsed_number = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(parsed_number >= 0 && parsed_number <= 16777215))) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting MEDIUMINT UNSIGNED. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else if (unlikely(operation->equal(request->PKName(colIdx),
                                         static_cast<Uint32>(parsed_number)) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Int: {
    ///< 32 bit. 4 byte signed integer, can be used in array
    char *parsed        = nullptr;
    errno               = 0;
    Int64 parsed_number = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(parsed_number >= -2147483648 && parsed_number <= 2147483647))) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting INT. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else if (unlikely(operation->equal(request->PKName(colIdx),
                                         static_cast<Int32>(parsed_number)) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Unsigned: {
    ///< 32 bit. 4 byte unsigned integer, can be used in array

    char *parsed        = nullptr;
    errno               = 0;
    Int64 parsed_number = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(parsed_number >= 0 && parsed_number <= 4294967295))) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting INT UNSIGNED. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else if (unlikely(operation->equal(request->PKName(colIdx),
                                         static_cast<Uint32>(parsed_number)) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Bigint: {
    ///< 64 bit. 8 byte signed integer, can be used in array

    char *parsed        = nullptr;
    errno               = 0;
    Int64 parsed_number = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0)) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting BIGINT. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else if (unlikely(operation->equal(request->PKName(colIdx), parsed_number) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Bigunsigned: {
    ///< 64 Bit. 8 byte signed integer, can be used in array
    char *parsed         = nullptr;
    errno                = 0;
    Uint64 parsed_number = strtoull(request->PKValueCStr(colIdx), &parsed, 10);

    const std::string numStr = std::string(request->PKValueCStr(colIdx));
    if (unlikely(*parsed != '\0' || errno != 0 || numStr.find('-') != std::string::npos)) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting BIGINT UNSIGNED. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else if (unlikely(operation->equal(request->PKName(colIdx), parsed_number) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Float: {
    ///< 32-bit float. 4 bytes float, can be used in array
    return RS_CLIENT_ERROR(ERROR_017 + std::string(" Column: ") +
                           std::string(request->PKName(colIdx)));
  }
  case NdbDictionary::Column::Double: {
    ///< 64-bit float. 8 byte float, can be used in array
    return RS_CLIENT_ERROR(ERROR_017 + std::string(" Column: ") +
                           std::string(request->PKName(colIdx)));
  }
  case NdbDictionary::Column::Olddecimal: {
    ///< MySQL < 5.0 signed decimal,  Precision, Scale
    return RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                           " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Olddecimalunsigned: {
    return RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                           " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Decimalunsigned: {
    ///< MySQL >= 5.0 signed decimal,  Precision, Scale
    const std::string decStr = std::string(request->PKValueCStr(colIdx));
    if (unlikely(decStr.find('-') != std::string::npos)) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting DECIMAL UNSIGNED. Column: ") +
                             std::string(request->PKName(colIdx)));
    }
    [[fallthrough]];
  }
  case NdbDictionary::Column::Decimal: {
    int precision      = col->getPrecision();
    int scale          = col->getScale();
    const char *decStr = request->PKValueCStr(colIdx);

    char decBin[DECIMAL_MAX_SIZE_IN_BYTES];
    if (unlikely(decimal_str2bin(decStr, strlen(decStr), precision, scale, decBin,
                                 DECIMAL_MAX_SIZE_IN_BYTES) != 0)) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting Decimal with Precision: ") +
                             std::to_string(precision) + std::string(" and Scale: ") +
                             std::to_string(scale));
    }

    if (unlikely(operation->equal(request->PKName(colIdx), decBin, DECIMAL_MAX_SIZE_IN_BYTES) !=
                 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Char: {
    /// A fix sized array of characters
    /// size of a character depends on encoding scheme

    const int data_len = request->PKValueLen(colIdx);
    if (unlikely(data_len > col->getLength())) {
      return RS_CLIENT_ERROR(
          std::string(ERROR_008) +
          " Data length is greater than column length. Column: " + std::string(col->getName()));
    }

    // operation->equal expects a zero-padded char string
    char pk[CHAR_MAX_SIZE_IN_BYTES];
    require(col->getLength() <= CHAR_MAX_SIZE_IN_BYTES);
    memset(pk, 0, col->getLength());

    const char *data_str = request->PKValueCStr(colIdx);
    memcpy(pk, data_str, data_len);

    if (unlikely(operation->equal(request->PKName(colIdx), pk, data_len) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Varchar:
    ///< Length bytes: 1, Max: 255
    [[fallthrough]];
  case NdbDictionary::Column::Longvarchar: {
    ///< Length bytes: 2, little-endian
    const int data_len = request->PKValueLen(colIdx);
    if (unlikely(data_len > col->getLength())) {
      return RS_CLIENT_ERROR(std::string(ERROR_008) +
                             " Data length is greater than column length. Data length:" +
                             std::to_string(data_len) + "Column: " + std::string(col->getName()) +
                             " Column length: " + std::to_string(col->getLength()));
    }
    char *charStr;
    if (unlikely(request->PKValueNDBStr(colIdx, col, &charStr) != 0)) {
      return RS_CLIENT_ERROR(ERROR_019);
    }

    if (unlikely(operation->equal(request->PKName(colIdx), charStr, data_len) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Binary: {
    /// Binary data is sent as base64 string
    require(col->getLength() <= BINARY_MAX_SIZE_IN_BYTES);
    const char *encoded_str      = request->PKValueCStr(colIdx);
    const size_t encoded_str_len = request->PKValueLen(colIdx);
    size_t col_len               = col->getLength();

    size_t decoded_size = boost::beast::detail::base64::decoded_size(encoded_str_len);
    if (unlikely(decoded_size > BINARY_MAX_SIZE_IN_BYTES_DECODED)) {
      return RS_CLIENT_ERROR(std::string(ERROR_008) + " " +
                             "Decoded data length is greater than column length. " +
                             "Column: " + std::string(col->getName()) +
                             " Length: " + std::to_string(col->getLength()));
    }

    char pk[BINARY_MAX_SIZE_IN_BYTES_DECODED];
    memset(pk, 0, col->getLength());

    std::pair<std::size_t, std::size_t> ret =
        boost::beast::detail::base64::decode(pk, encoded_str, encoded_str_len);
    // make sure everything was decoded. 1 or 2 bytes of padding is not included in ret.second
    require(ret.second >= encoded_str_len - 2 && ret.second <= encoded_str_len);
    if (unlikely(ret.first > col_len)) {
      return RS_CLIENT_ERROR(std::string(ERROR_008) + " " +
                             "Decoded data length is greater than column length. " +
                             "Column: " + std::string(col->getName()) +
                             " Length: " + std::to_string(col->getLength()));
    }

    if (unlikely(operation->equal(request->PKName(colIdx), pk, col->getLength()) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Varbinary:
    ///< Length bytes: 1, Max: 255
    [[fallthrough]];
  case NdbDictionary::Column::Longvarbinary: {
    // Length bytes: 2, little-endian
    // Note: col->getLength() does not include the length bytes.

    const size_t col_len         = col->getLength();
    const char *encoded_str      = request->PKValueCStr(colIdx);
    const size_t encoded_str_len = request->PKValueLen(colIdx);

    size_t decoded_size = boost::beast::detail::base64::decoded_size(encoded_str_len);

    if (unlikely(decoded_size > KEY_MAX_SIZE_IN_BYTES_DECODED)) {
      return RS_CLIENT_ERROR(std::string(ERROR_008) + " " +
                             "Decoded data length is greater than column length. " +
                             "Column: " + std::string(col->getName()) +
                             " Length: " + std::to_string(col->getLength()));
    }

    char pk[KEY_MAX_SIZE_IN_BYTES_DECODED];
    int additional_len = 1;
    if (col->getType() == NdbDictionary::Column::Longvarbinary) {
      additional_len = 2;
    }

    // leave first 1-2 bytes free for saving length bytes
    std::pair<std::size_t, std::size_t> ret =
        boost::beast::detail::base64::decode(pk + additional_len, encoded_str, encoded_str_len);
    // make sure everything was decoded. 1 or 2 bytes of padding which is not included in ret.second
    require(ret.second >= encoded_str_len - 2 && ret.second <= encoded_str_len);
    if (unlikely(ret.first > col_len)) {
      return RS_CLIENT_ERROR(std::string(ERROR_008) + " " +
                             "Decoded data length is greater than column length. " +
                             "Column: " + std::string(col->getName()) +
                             " Length: " + std::to_string(col->getLength()));
    }

    // insert the length at the beginning of the array
    if (col->getType() == NdbDictionary::Column::Varbinary) {
      pk[0] = (Uint8)ret.first;
    } else if (col->getType() == NdbDictionary::Column::Longvarbinary) {
      pk[0] = (Uint8)(ret.first % 256);
      pk[1] = (Uint8)(ret.first / 256);
    } else {
      return RS_SERVER_ERROR(ERROR_015);
    }

    if (unlikely(operation->equal(request->PKName(colIdx), pk, ret.first + additional_len) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Datetime: {
    ///< Precision down to 1 sec (sizeof(Datetime) == 8 bytes )
    return RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                           " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Date: {
    ///< Precision down to 1 day(sizeof(Date) == 4 bytes )
    const char *date_str = request->PKValueCStr(colIdx);
    size_t date_str_len  = request->PKValueLen(colIdx);

    MYSQL_TIME l_time;
    MYSQL_TIME_STATUS status;
    bool ret = str_to_datetime(date_str, date_str_len, &l_time, 0, &status);
    if (unlikely(ret != 0)) {
      return RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                             std::string(col->getName()))
    }

    if (unlikely(l_time.hour != 0 || l_time.minute != 0 || l_time.second != 0 ||
                 l_time.second_part != 0)) {
      return RS_CLIENT_ERROR(std::string(ERROR_008) +
                             " Expecting only date data. Column: " + std::string(col->getName()));
    }

    unsigned char packed[DATE_MAX_SIZE_IN_BYTES];
    my_date_to_binary(&l_time, packed);

    if (unlikely(operation->equal(request->PKName(colIdx), reinterpret_cast<char *>(packed),
                                  col->getSizeInBytes()) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
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
    ///< Bit, length specifies no of bits
    return RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                           " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Time: {
    ///< Time without date
    return RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                           " Type: " + std::to_string(col->getType()));
  }
  case NdbDictionary::Column::Year: {
    ///< Year 1901-2155 (1 byte)

    char *parsed        = nullptr;
    errno               = 0;
    Int64 parsed_number = strtoll(request->PKValueCStr(colIdx), &parsed, 10);

    if (unlikely(*parsed != '\0' || errno != 0 ||
                 !(parsed_number >= 1901 && parsed_number <= 2155))) {
      return RS_CLIENT_ERROR(
          ERROR_015 + std::string(" Expecting YEAR column. Possible values [1901-2155]. Column: ") +
          std::string(request->PKName(colIdx)));
    }

    Uint8 year = static_cast<Uint8>((parsed_number - 1900));
    if (unlikely(operation->equal(request->PKName(colIdx), year) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
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
    require(col->getSizeInBytes() <= TIME2_MAX_SIZE_IN_BYTES);
    const char *time_str = request->PKValueCStr(colIdx);
    size_t time_str_len  = request->PKValueLen(colIdx);

    MYSQL_TIME l_time;
    MYSQL_TIME_STATUS status;
    bool ret = str_to_time(time_str, time_str_len, &l_time, &status, 0);
    if (unlikely(ret != 0)) {
      return RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                             std::string(col->getName()))
    }

    size_t col_size = col->getSizeInBytes();
    int precision   = col->getPrecision();
    unsigned char packed[TIME2_MAX_SIZE_IN_BYTES];

    int warnings = 0;
    my_datetime_adjust_frac(&l_time, precision, &warnings, true);
    if (unlikely(warnings != 0)) {
      return RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                             std::string(col->getName()))
    }

    longlong numeric_date_time = TIME_to_longlong_time_packed(l_time);
    my_time_packed_to_binary(numeric_date_time, packed, precision);

    if (unlikely(operation->equal(request->PKName(colIdx), reinterpret_cast<char *>(packed),
                                  col_size) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Datetime2: {
    ///< 5 bytes plus 0-3 fraction
    require(col->getSizeInBytes() <= DATETIME_MAX_SIZE_IN_BYTES);

    const char *date_str = request->PKValueCStr(colIdx);
    size_t date_str_len  = request->PKValueLen(colIdx);

    MYSQL_TIME l_time;
    MYSQL_TIME_STATUS status;
    bool ret = str_to_datetime(date_str, date_str_len, &l_time, 0, &status);
    if (unlikely(ret != 0)) {
      return RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                             std::string(col->getName()))
    }

    size_t col_size = col->getSizeInBytes();
    int precision   = col->getPrecision();

    int warnings = 0;
    my_datetime_adjust_frac(&l_time, precision, &warnings, true);
    if (unlikely(warnings != 0)) {
      return RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                             std::string(col->getName()))
    }

    longlong numeric_date_time = TIME_to_longlong_datetime_packed(l_time);

    unsigned char packed[DATETIME_MAX_SIZE_IN_BYTES];
    my_datetime_packed_to_binary(numeric_date_time, packed, precision);

    if (unlikely(operation->equal(request->PKName(colIdx), reinterpret_cast<char *>(packed),
                                  col_size) != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Timestamp2: {
    // epoch range 0 , 2147483647
    /// < 4 bytes + 0-3 fraction
    require(col->getSizeInBytes() <= TIMESTAMP2_MAX_SIZE_IN_BYTES);
    const char *ts_str = request->PKValueCStr(colIdx);
    size_t ts_str_len  = request->PKValueLen(colIdx);
    unsigned char packed[TIMESTAMP2_MAX_SIZE_IN_BYTES];
    uint precision = col->getPrecision();

    MYSQL_TIME l_time;
    MYSQL_TIME_STATUS status;
    bool ret = str_to_datetime(ts_str, ts_str_len, &l_time, 0, &status);
    if (unlikely(ret != 0)) {
      return RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                             std::string(col->getName()))
    }

    time_t epoch = 0;
    errno        = 0;
    try {
      char bts_str[MAX_DATE_STRING_REP_LENGTH];
      snprintf(bts_str, MAX_DATE_STRING_REP_LENGTH, "%d-%d-%d %d:%d:%d", l_time.year, l_time.month,
               l_time.day, l_time.hour, l_time.minute, l_time.second);
      boost::posix_time::ptime bt(boost::posix_time::time_from_string(std::string(bts_str)));
      boost::posix_time::ptime start(boost::gregorian::date(1970, 1, 1));
      boost::posix_time::time_duration dur = bt - start;
      epoch                                = dur.total_seconds();
    } catch (...) {
      return RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                             std::string(col->getName()))
    }

    // 1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC.
    if (unlikely(epoch <= 0 || epoch > 2147483647)) {
      return RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                             std::string(col->getName()))
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
    my_datetime_adjust_frac(&l_time, precision, &warnings, true);
    if (unlikely(warnings != 0)) {
      return RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                             std::string(col->getName()))
    }

    // On Mac my_timeval.tv_usec is Int32 and on linux it is Int64.
    // Inorder to be compatible we cast l_time.second_part to Int32
    // This will not create problems as only six digit nanoseconds
    // are stored in Timestamp2
    my_timeval my_tv{epoch, (Int32)l_time.second_part};
    my_timestamp_to_binary(&my_tv, packed, precision);

    size_t col_size = col->getSizeInBytes();
    int exitCode =
        operation->equal(request->PKName(colIdx), reinterpret_cast<char *>(packed), col_size);
    if (unlikely(exitCode != 0)) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  }
  return RS_OK;
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
    int bin_len   = attr->get_size_in_bytes();
    decimal_bin2str(bin, bin_len, precision, scale, decStr, DECIMAL_MAX_STR_LEN_IN_BYTES);
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
    Uint32 attr_bytes;
    const char *data_start = nullptr;
    if (unlikely(GetByteArray(attr, &data_start, &attr_bytes) != 0)) {
      return RS_CLIENT_ERROR(ERROR_019);
    } else {
      return response->Append_char(attr->getColumn()->getName(), data_start, attr_bytes,
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
    Uint32 attr_bytes;
    const char *data_start = nullptr;
    if (unlikely(GetByteArray(attr, &data_start, &attr_bytes) != 0)) {
      return RS_CLIENT_ERROR(ERROR_019);
    } else {
      require(attr_bytes <= MAX_TUPLE_SIZE_IN_BYTES_ENCODED);
      char buffer[MAX_TUPLE_SIZE_IN_BYTES_ENCODED];
      size_t ret = boost::beast::detail::base64::encode(reinterpret_cast<void *>(buffer),
                                                        data_start, attr_bytes);
      return response->Append_string(attr->getColumn()->getName(), std::string(buffer, ret),
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
    MYSQL_TIME l_time;
    my_unpack_date(&l_time, attr->aRef());
    char to[MAX_DATE_STRING_REP_LENGTH];
    my_date_to_str(l_time, to);
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
    size_t ret =
        boost::beast::detail::base64::encode(reinterpret_cast<void *>(buffer), reversed, words);
    return response->Append_string(attr->getColumn()->getName(), std::string(buffer, ret),
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

    longlong numeric_time =
        my_time_packed_from_binary((const unsigned char *)attr->aRef(), precision);

    MYSQL_TIME l_time;
    TIME_from_longlong_time_packed(&l_time, numeric_time);

    char to[MAX_DATE_STRING_REP_LENGTH];
    my_TIME_to_str(l_time, to, precision);

    return response->Append_string(attr->getColumn()->getName(), std::string(to),
                                   RDRS_DATETIME_DATATYPE);
  }
  case NdbDictionary::Column::Datetime2: {
    ///< 5 bytes plus 0-3 fraction
    uint precision = col->getPrecision();

    longlong numeric_date =
        my_datetime_packed_from_binary((const unsigned char *)attr->aRef(), precision);

    MYSQL_TIME l_time;
    TIME_from_longlong_datetime_packed(&l_time, numeric_date);

    char to[MAX_DATE_STRING_REP_LENGTH];
    my_TIME_to_str(l_time, to, precision);

    return response->Append_string(attr->getColumn()->getName(), std::string(to),
                                   RDRS_DATETIME_DATATYPE);
  }
  case NdbDictionary::Column::Timestamp2: {
    ///< 4 bytes + 0-3 fraction
    uint precision = col->getPrecision();

    my_timeval my_tv{};
    my_timestamp_from_binary(&my_tv, (const unsigned char *)attr->aRef(), precision);

    Int64 epoch_in = my_tv.m_tv_sec;
    std::time_t stdtime(epoch_in);
    boost::posix_time::ptime ts = boost::posix_time::from_time_t(stdtime);

    MYSQL_TIME l_time  = {};
    l_time.year        = ts.date().year();
    l_time.month       = ts.date().month();
    l_time.day         = ts.date().day();
    l_time.hour        = ts.time_of_day().hours();
    l_time.minute      = ts.time_of_day().minutes();
    l_time.second      = ts.time_of_day().seconds();
    l_time.second_part = my_tv.m_tv_usec;
    l_time.time_type   = MYSQL_TIMESTAMP_DATETIME;

    char to[MAX_DATE_STRING_REP_LENGTH];
    my_TIME_to_str(l_time, to, precision);

    return response->Append_string(attr->getColumn()->getName(), std::string(to),
                                   RDRS_DATETIME_DATATYPE);
  }
  }

  return RS_SERVER_ERROR(ERROR_028 + std::string(" Column: ") + std::string(col->getName()) +
                         " Type: " + std::to_string(col->getType()));
}

int GetByteArray(const NdbRecAttr *attr, const char **first_byte, Uint32 *bytes) {
  const NdbDictionary::Column::ArrayType array_type = attr->getColumn()->getArrayType();
  const size_t attr_bytes                           = attr->get_size_in_bytes();
  const char *aRef                                  = attr->aRef();
  std::string result;

  switch (array_type) {
  case NdbDictionary::Column::ArrayTypeFixed:
    /*
       No prefix length is stored in aRef. Data starts from aRef's first byte
       data might be padded with blank or null bytes to fill the whole column
       */
    *first_byte = aRef;
    *bytes      = attr_bytes;
    return 0;
  case NdbDictionary::Column::ArrayTypeShortVar:
    /*
       First byte of aRef has the length of data stored
       Data starts from second byte of aRef
       */
    *first_byte = aRef + 1;
    *bytes      = static_cast<size_t>(aRef[0]);
    return 0;
  case NdbDictionary::Column::ArrayTypeMediumVar:
    /*
       First two bytes of aRef has the length of data stored
       Data starts from third byte of aRef
       */
    *first_byte = aRef + 2;
    *bytes      = static_cast<size_t>(aRef[1]) * 256 + static_cast<size_t>(aRef[0]);
    return 0;
  default:
    first_byte = nullptr;
    *bytes     = 0;
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
    DEBUG(std::string("Transient error. ") + status.message);
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

Uint32 ExponentialDelayWithJitter(Uint32 retry, Uint32 initial_delay_in_ms, Uint32 jitter_in_ms) {
  Uint32 expoDelay  = initial_delay_in_ms * pow(2, retry);
  jitter_in_ms      = std::min(jitter_in_ms, initial_delay_in_ms);
  Uint32 randJitter = rand() % jitter_in_ms;

  Uint32 delay = 0;
  if (rand() % 2 == 0) {
    delay = expoDelay + randJitter;
  } else {
    delay = expoDelay - randJitter;
  }
  return delay;
}
