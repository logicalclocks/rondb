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
#include <my_time.h>
#include <sql_string.h>
#include <ndb_limits.h>
#include <string>
#include <algorithm>
#include <utility>
#include "src/logger.hpp"
#include "src/error-strings.h"
#include "src/status.hpp"
#include "src/mystring.hpp"
#include "src/rdrs-const.h"

static const int MaxMySQLDecimalPrecision = 65;
static const int MaxDecimalStrLen         = MaxMySQLDecimalPrecision + 3;

static int howManyBytesNeeded[] = {
    0,  1,  1,  2,  2,  3,  3,  4,  4,  4,  5,  5,  6,  6,  7,  7,  8,  8,  8,  9,  9,  10,
    10, 11, 11, 12, 12, 12, 13, 13, 14, 14, 15, 15, 16, 16, 16, 17, 17, 18, 18, 19, 19, 20,
    20, 20, 21, 21, 22, 22, 23, 23, 24, 24, 24, 25, 25, 26, 26, 27, 27, 28, 28, 28, 29, 29};

/** Get the number of bytes needed in memory to represent the decimal number.
 *
 * @param precision the precision of the number
 * @param scale the scale
 * @return the number of bytes needed for the binary representation of the number
 */

inline int getDecimalColumnSpace(int precision, int scale) {
  int howManyBytesNeededForIntegral = howManyBytesNeeded[precision - scale];
  int howManyBytesNeededForFraction = howManyBytesNeeded[scale];
  int result                        = howManyBytesNeededForIntegral + howManyBytesNeededForFraction;
  return result;
}

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
    bool success = false;
    try {
      int num = std::stoi(request->PKValueCStr(colIdx));
      if (num >= -128 && num <= 127) {
        if (operation->equal(request->PKName(colIdx), static_cast<char>(num)) != 0) {
          return RS_SERVER_ERROR(ERROR_023);
        }
        success = true;
      }
    } catch (...) {
    }
    if (!success) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting TINYINT. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else {
      return RS_OK;
    }
  }
  case NdbDictionary::Column::Tinyunsigned: {
    ///< 8 bit. 1 byte unsigned integer, can be used in array
    bool success = false;
    try {
      int num = std::stoi(request->PKValueCStr(colIdx));
      if (num >= 0 && num <= 255) {
        if (operation->equal(request->PKName(colIdx), static_cast<char>(num)) != 0) {
          return RS_SERVER_ERROR(ERROR_023);
        }
        success = true;
      }
    } catch (...) {
    }
    if (!success) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting TINYINT. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else {
      return RS_OK;
    }
  }
  case NdbDictionary::Column::Smallint: {
    ///< 16 bit. 2 byte signed integer, can be used in array
    bool success = false;
    try {
      int num = std::stoi(request->PKValueCStr(colIdx));
      if (num >= -32768 && num <= 32767) {
        if (operation->equal(request->PKName(colIdx), (Int16)num) != 0) {
          return RS_SERVER_ERROR(ERROR_023);
        }
        success = true;
      }
    } catch (...) {
    }
    if (!success) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting SMALLINT. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else {
      return RS_OK;
    }
  }
  case NdbDictionary::Column::Smallunsigned: {
    ///< 16 bit. 2 byte unsigned integer, can be used in array
    bool success = false;
    try {
      int num = std::stoi(request->PKValueCStr(colIdx));
      if (num >= 0 && num <= 65535) {
        if (operation->equal(request->PKName(colIdx), (Uint16)num) != 0) {
          return RS_SERVER_ERROR(ERROR_023);
        }
        success = true;
      }
    } catch (...) {
    }
    if (!success) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting TINYINT UNSIGNED. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else {
      return RS_OK;
    }
  }
  case NdbDictionary::Column::Mediumint: {
    ///< 24 bit. 3 byte signed integer, can be used in array
    bool success = false;
    try {
      int num = std::stoi(request->PKValueCStr(colIdx));
      if (num >= -8388608 && num <= 8388607) {
        if (operation->equal(request->PKName(colIdx), static_cast<int>(num)) != 0) {
          return RS_SERVER_ERROR(ERROR_023);
        }
        success = true;
      }
    } catch (...) {
    }
    if (!success) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting MEDIUMINT. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else {
      return RS_OK;
    }
  }
  case NdbDictionary::Column::Mediumunsigned: {
    ///< 24 bit. 3 byte unsigned integer, can be used in array
    bool success = false;
    try {
      int num = std::stoi(request->PKValueCStr(colIdx));
      if (num >= 0 && num <= 16777215) {
        if (operation->equal(request->PKName(colIdx), (unsigned int)num)) {
          return RS_SERVER_ERROR(ERROR_023);
        }
        success = true;
      }
    } catch (...) {
    }
    if (!success) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting MEDIUMINT UNSIGNED. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else {
      return RS_OK;
    }
  }
  case NdbDictionary::Column::Int: {
    ///< 32 bit. 4 byte signed integer, can be used in array
    try {
      Int32 num = std::stoi(request->PKValueCStr(colIdx));
      if (operation->equal(request->PKName(colIdx), num) != 0) {
        return RS_SERVER_ERROR(ERROR_023);
      }
    } catch (...) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting Int. Column: ") +
                             std::string(request->PKName(colIdx)));
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Unsigned: {
    ///< 32 bit. 4 byte unsigned integer, can be used in array
    bool success = false;
    try {
      Int64 lresult = std::stoll(request->PKValueCStr(colIdx));
      Uint32 result = lresult;
      if (result == lresult) {
        if (operation->equal(request->PKName(colIdx), result) != 0) {
          return RS_SERVER_ERROR(ERROR_023);
        }
        success = true;
      }
    } catch (...) {
    }

    if (!success) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting Unsigned Int. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else {
      return RS_OK;
    }
  }
  case NdbDictionary::Column::Bigint: {
    ///< 64 bit. 8 byte signed integer, can be used in array
    try {
      Int64 num = std::stoll(request->PKValueCStr(colIdx));
      if (operation->equal(request->PKName(colIdx), num) != 0) {
        return RS_SERVER_ERROR(ERROR_023);
      }
    } catch (...) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting BIGINT. Column: ") +
                             std::string(request->PKName(colIdx)));
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Bigunsigned: {
    ///< 64 Bit. 8 byte signed integer, can be used in array
    bool success = false;
    try {
      const char *numCStr      = request->PKValueCStr(colIdx);
      const std::string numStr = std::string(numCStr);
      if (numStr.find('-') == std::string::npos) {
        Uint64 num = std::stoul(numCStr);
        if (operation->equal(request->PKName(colIdx), num) != 0) {
          return RS_SERVER_ERROR(ERROR_023);
        }
        success = true;
      }
    } catch (...) {
    }
    if (!success) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting BIGINT UNSIGNED. Column: ") +
                             std::string(request->PKName(colIdx)));
    } else {
      return RS_OK;
    }
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
    if (decStr.find('-') != std::string::npos) {
      return RS_CLIENT_ERROR(ERROR_015 +
                             std::string(" Expecting Decimalunsigned UNSIGNED. Column: ") +
                             std::string(request->PKName(colIdx)));
    }
    [[fallthrough]];
  }
  case NdbDictionary::Column::Decimal: {
    int precision      = col->getPrecision();
    int scale          = col->getScale();
    int bytesNeeded    = getDecimalColumnSpace(precision, scale);
    const char *decStr = request->PKValueCStr(colIdx);
    char decBin [MAX_KEY_SIZE_IN_WORDS*4];
    // std::fill (decBin, decBin + bytesNeeded, 0);

    if (decimal_str2bin(decStr, strlen(decStr), precision, scale, decBin, bytesNeeded) != 0) {
      return RS_CLIENT_ERROR(ERROR_015 + std::string(" Expecting Decimal with Precision: ") +
                             std::to_string(precision) + std::string(" and Scale: ") +
                             std::to_string(scale));
    }

    if (operation->equal(request->PKName(colIdx), decBin, bytesNeeded) != 0) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Char: {
    ///< Len. A fixed array of 1-byte chars

    const int data_len = request->PKValueLen(colIdx);
    if (data_len > col->getLength()) {
      return RS_CLIENT_ERROR(
          std::string(ERROR_008) +
          " Data length is greater than column length. Column: " + std::string(col->getName()));
    }

    // operation->equal expects a zero-padded char string
    char pk[MAX_KEY_SIZE_IN_WORDS*4];
    std::fill (pk, pk + col->getLength(), 0);
    
    const char *data_str = request->PKValueCStr(colIdx);
    memcpy(pk, data_str, data_len);

    if (operation->equal(request->PKName(colIdx), pk, data_len) != 0) {
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
    if (data_len > col->getLength()) {
      return RS_CLIENT_ERROR(
          std::string(ERROR_008) +
          " Data length is greater than column length. Column: " + std::string(col->getName()));
    }
    char *charStr;
    if (request->PKValueNDBStr(colIdx, col, &charStr) != 0) {
      return RS_CLIENT_ERROR(ERROR_019);
    }
    if (operation->equal(request->PKName(colIdx), charStr, data_len) != 0) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Binary: {
    ///< Len
    const char *encodedStr = request->PKValueCStr(colIdx);
    int data_len = request->PKValueLen(colIdx);

    /*
      Binary data will be sent as base64 strings.
      These use 4 ASCII characters to represent any 3 bytes.

      This means that any data which is not a multiple of 3 bytes will
      append the padding character "=" once or twice in the encoded string.

      E.g. the string "a"
        -> to byte array: [ 97 ]
        -> to base64 string: "YQ=="
      This uses "==" as padding, since we only have 1 byte to represent.

      Since the function decoded_size() does not read the string, it is not aware
      of whether it contains padding characters or not. PKValueLen() is also not
      aware of this. Thereby, any "==" will be intepreted as part of the data
      and therefore decoded_size() will always return a multiple of 3 bytes.

      E.g. given base64 string "YQ==" (4 ASCII characters)
        -> data_len == 4 bytes
        -> decoded_size == 3 bytes (since base64 always uses 4 bytes to represent 3 bytes of data)
    */
    size_t decoded_size = boost::beast::detail::base64::decoded_size(data_len);

    if (decoded_size > size_t(col->getLength())) {
      
      /*
        As described above, it may be the case that the decoded_size falsely interpretes
        1 or 2 padding bytes as part of the data. Since the decoded_size must be a
        multiple of 3, it may now only be the next multiple after col->getLength().

        E.g.: col->getLength() == 100; decoded_size is now only allowed to be 102
      */
      int base64Mod = col->getLength() % 3; // 100 % 3 == 1
      int base64Total = col->getLength() - base64Mod + 3; // 100 - 1 + 3 == 102

      if (decoded_size != size_t(base64Total)) {
        return RS_CLIENT_ERROR(
            std::string(ERROR_008) +
            " Decoded data length (" + std::to_string(decoded_size) +
            ") is greater than column length (" + std::to_string(col->getLength()) +
            "). Column: " + std::string(col->getName())
        );
      }
      /*
        It may now still be the case that the real decoded_size is 1 byte larger than
        the column length. But we cannot find out unless we decode the string.
      */
    }

    // operation->equal expects a zero-padded char string
    char pk[MAX_KEY_SIZE_IN_WORDS*4];
    std::fill (pk, pk + col->getLength(), 0);

    std::pair<std::size_t, std::size_t> ret = boost::beast::detail::base64::decode(pk, encodedStr, data_len);

    // ret.first does not interpret padding bytes as part of data
    if (ret.first > size_t(col->getLength())) {
        return RS_CLIENT_ERROR(
            std::string(ERROR_008) +
            " ret.first (" + std::to_string(ret.first) +
            ") is not equal to decoded_size (" + std::to_string(decoded_size)
        );
    }

    if (operation->equal(request->PKName(colIdx), pk, col->getLength()) != 0) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Varbinary:
    ///< Length bytes: 1, Max: 255
    [[fallthrough]];
  case NdbDictionary::Column::Longvarbinary: {
    ///< Length bytes: 2, little-endian

    /*
      In NDB, a row containing a varbinary column will use 11 bytes to store 
      10 bytes in this column. This is because the first byte will represent
      the length of the array. A longvarbinary column will use the first 2 bytes
      to represent the length.

      col->getLength() should not include the length bytes.
    */

    /*
      PKValueLen refers to data without prepended length bytes.
      The length bytes are only native to RonDB.
    */
    const int data_len = request->PKValueLen(colIdx); // data length of binary value

    /*
      Similarly to binary columns, the data in the request will be a base64 string.
      Once again, the data_len is unaware of base64's "=" used as padding. Therefore the
      decoded_size may be off by 1 or 2 bytes.
    */ 
    size_t decoded_size = boost::beast::detail::base64::decoded_size(data_len);

    if (decoded_size > size_t(col->getLength())) {

      /*
        As described above, it may be the case that the decoded_size falsely interpretes
        1-2 padding bytes as part of the data. If so, the decoded_size must be a
        multiple of 3 and it may only be the next multiple after col->getLength().

        E.g.: col->getLength() == 100; decoded_size may now only be 102
      */
      int base64Mod = col->getLength() % 3; // 100 % 3 == 1
      int base64Total = col->getLength() - base64Mod + 3; // 100 - 1 + 3 == 102

      if (decoded_size != size_t(base64Total)) {
        return RS_CLIENT_ERROR(
            std::string(ERROR_008) +
            " Decoded data length (" + std::to_string(decoded_size) +
            ") is greater than column length (" + std::to_string(col->getLength()) +
            "). Column: " + std::string(col->getName())
        );
      }
      // It can now still be the case that the true decoded length is
      // 101 or 102 bytes. We can only find out by decoding the string
    }

    int additional_len  = 1;
    if (col->getType() == NdbDictionary::Column::Longvarbinary) {
      additional_len = 2;
    }

    const char *encodedStr = request->PKValueCStr(colIdx);

    char pk[MAX_KEY_SIZE_IN_WORDS*4 + 2];
    // std::fill (pk, pk + col->getLength(), 0);  // What's column length in varbinary?

    // leave first 1-2 bytes free for saving length bytes
    std::pair<std::size_t, std::size_t> ret = boost::beast::detail::base64::decode(
        pk + additional_len, encodedStr, data_len);

    // ret.first does not interpret padding bytes as part of data
    if (ret.first > size_t(col->getLength())) {
        return RS_CLIENT_ERROR(
            std::string(ERROR_008) +
            " Size of decoded bytes (" + std::to_string(ret.first) +
            ") is greater than the varbinary column length (" + std::to_string(col->getLength())
        );
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

    if (operation->equal(request->PKName(colIdx), pk, ret.first + additional_len) != 0) {
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
    if (ret != 0) {
      return RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                             std::string(col->getName()))
    }

    if (l_time.hour != 0 || l_time.minute != 0 || l_time.second != 0 || l_time.second_part != 0) {
      return RS_CLIENT_ERROR(std::string(ERROR_008) +
                             " Expecting only date data. Column: " + std::string(col->getName()));
    }

    unsigned char packed[MAX_KEY_SIZE_IN_WORDS*4];
    // std::fill (pk, pk + col->getLength(), 0);
    my_date_to_binary(&l_time, packed);

    int exitCode = operation->equal(request->PKName(colIdx), reinterpret_cast<char *>(packed), col->getSizeInBytes());
    if ( exitCode != 0 ) {
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
    bool success = false;
    try {
      Int32 year = std::stoi(request->PKValueCStr(colIdx));
      if (year >= 1901 && year <= 2155) {
        Uint8 year_char = (year - 1900);
        if (operation->equal(request->PKName(colIdx), year_char) != 0) {
          return RS_SERVER_ERROR(ERROR_023);
        }
        success = true;
      }
    } catch (...) {
    }
    if (!success) {
      return RS_CLIENT_ERROR(
          ERROR_015 + std::string(" Expecting YEAR column. Possible values [1901-2155]. Column: ") +
          std::string(request->PKName(colIdx)));
    } else {
      return RS_OK;
    }
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
    const char *time_str = request->PKValueCStr(colIdx);
    size_t time_str_len  = request->PKValueLen(colIdx);

    MYSQL_TIME l_time;
    MYSQL_TIME_STATUS status;
    bool ret = str_to_time(time_str, time_str_len, &l_time, &status, 0);
    if (ret != 0) {
      return RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                             std::string(col->getName()))
    }

    size_t col_byte_size = col->getSizeInBytes();
    int precision     = col->getPrecision();
    unsigned char packed[MAX_KEY_SIZE_IN_WORDS*4];
    // std::fill (pk, pk + col->getLength(), 0);

    TruncatePrecision(&l_time, status.fractional_digits, precision);
    longlong numeric_date_time = TIME_to_longlong_time_packed(l_time);
    my_time_packed_to_binary(numeric_date_time, packed, precision);

    int exitCode = operation->equal(request->PKName(colIdx), reinterpret_cast<char *>(packed), col_byte_size);
    if ( exitCode != 0) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Datetime2: {
    ///< 5 bytes plus 0-3 fraction
    const char *date_str = request->PKValueCStr(colIdx);
    size_t date_str_len  = request->PKValueLen(colIdx);

    MYSQL_TIME l_time;
    MYSQL_TIME_STATUS status;
    bool ret = str_to_datetime(date_str, date_str_len, &l_time, 0, &status);
    if (ret != 0) {
      return RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                             std::string(col->getName()))
    }

    size_t col_byte_size = col->getSizeInBytes();
    int precision     = col->getPrecision();

    TruncatePrecision(&l_time, status.fractional_digits, precision);
    unsigned char packed[MAX_KEY_SIZE_IN_WORDS*4];
    // std::fill (pk, pk + col->getLength(), 0);

    longlong numeric_date_time = TIME_to_longlong_datetime_packed(l_time);

    my_datetime_packed_to_binary(numeric_date_time, packed, precision);

    int exitCode = operation->equal(request->PKName(colIdx), reinterpret_cast<char *>(packed), col_byte_size);
    if ( exitCode != 0 ) {
      return RS_SERVER_ERROR(ERROR_023);
    }
    return RS_OK;
  }
  case NdbDictionary::Column::Timestamp2: {
    // epoch range 0 , 2147483647
    /// < 4 bytes + 0-3 fraction
    const char *ts_str = request->PKValueCStr(colIdx);
    size_t ts_str_len  = request->PKValueLen(colIdx);
    // TODO: Check that col input is less than MAX_KEY_SIZE_IN_WORDS
    unsigned char packed[MAX_KEY_SIZE_IN_WORDS*4];
    // std::fill (pk, pk + col->getLength(), 0);
    uint precision = col->getPrecision();

    MYSQL_TIME l_time;
    MYSQL_TIME_STATUS status;
    bool ret = str_to_datetime(ts_str, ts_str_len, &l_time, 0, &status);
    if (ret != 0) {
      return RS_CLIENT_ERROR(std::string(ERROR_027) + std::string(" Column: ") +
                             std::string(col->getName()))
    }

    time_t epoch = 0;
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
    if (epoch <= 0 || epoch > 2147483647) {
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

    TruncatePrecision(&l_time, status.fractional_digits, precision);
    timeval my_tv{epoch, (Int64)l_time.second_part};
    my_timestamp_to_binary(&my_tv, packed, precision);

    size_t col_byte_size = col->getSizeInBytes();
    int exitCode = operation->equal(request->PKName(colIdx), reinterpret_cast<char *>(packed), col_byte_size);
    if ( exitCode != 0 ) {
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
    char decStr[MaxDecimalStrLen];
    int precision = attr->getColumn()->getPrecision();
    int scale     = attr->getColumn()->getScale();
    void *bin     = attr->aRef();
    int bin_len   = attr->get_size_in_bytes();
    decimal_bin2str(bin, bin_len, precision, scale, decStr, MaxDecimalStrLen);
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
    if (GetByteArray(attr, &data_start, &attr_bytes) != 0) {
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
    if (GetByteArray(attr, &data_start, &attr_bytes) != 0) {
      return RS_CLIENT_ERROR(ERROR_019);
    } else {
      char buffer[MAX_TUPLE_SIZE_IN_WORDS*4];
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
    Uint32 words     = attr->getColumn()->getLength() / 8;
    if (attr->getColumn()->getLength() % 8 != 0) {
      words += 1;
    }

    // change endieness
    int i = 0;
    char reversed [MAX_TUPLE_SIZE_IN_WORDS*4];
    for (int j = words - 1; j >= 0; j--) {
      reversed[i++] = attr->aRef()[j];
    }

    char buffer [MAX_TUPLE_SIZE_IN_WORDS*4];
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

    timeval my_tv{};
    my_timestamp_from_binary(&my_tv, (const unsigned char *)attr->aRef(), precision);

    Int64 epoch_in = my_tv.tv_sec;
    std::time_t stdtime(epoch_in);
    boost::posix_time::ptime ts = boost::posix_time::from_time_t(stdtime);

    MYSQL_TIME l_time  = {};
    l_time.year        = ts.date().year();
    l_time.month       = ts.date().month();
    l_time.day         = ts.date().day();
    l_time.hour        = ts.time_of_day().hours();
    l_time.minute      = ts.time_of_day().minutes();
    l_time.second      = ts.time_of_day().seconds();
    l_time.second_part = my_tv.tv_usec;
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

void TruncatePrecision(MYSQL_TIME *l_time, int currentPrecision, int maxPrecision) {
    int precisionDifference = currentPrecision - maxPrecision;
    if (precisionDifference > 0) {
      for (int i = 1; i <= precisionDifference; i++) {
        int rest = l_time->second_part % 10;
        l_time->second_part = l_time->second_part / 10;

        // round up the last digit
        if (i == precisionDifference && rest > 4) {
          l_time->second_part++;
        }
      }
      // Append zeros
      l_time->second_part = l_time->second_part * pow(10, precisionDifference);
    }
}
