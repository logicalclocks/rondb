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

#include "encoding_helper.hpp"
#include "mystring.hpp"
#include "rdrs_dal.hpp"
#include "buffer_manager.hpp"
#include "logger.hpp"

#include <cstring>
#include <tuple>

EN_Status copy_str_to_buffer(const std::string_view &src,
                             void *dst,
                             Uint32 offset) {
  if (dst == nullptr) {
    EN_Status status{};
    status.http_code = static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest);
    status.retValue  = 0;
    strncpy(status.message, "Destination buffer pointer is null", EN_STATUS_MSG_LEN - 1);
    status.message[EN_STATUS_MSG_LEN - 1] = '\0';
    return status;
  }
  Uint32 src_length = static_cast<Uint32>(src.size());
  memcpy(static_cast<char *>(dst) + offset, src.data(), src_length);
  static_cast<char *>(dst)[offset + src_length] = '\0';
  return EN_Status(offset + src_length + 1);
}

EN_Status copy_ndb_str_to_buffer(std::vector<char> &src, void *dst, Uint32 offset) {
  if (dst == nullptr) {
    return EN_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest), 0,
                     "Destination buffer pointer is null");
  }

  // Remove quotation marks from string, if it's a quoted string
  if (src.size() >= 2 && src.front() == '"' && src.back() == '"') {

    RS_Status status = Unquote(src);
    if (status.http_code != SUCCESS) {
      std::string msg =
        "Failed to unquote string. Error message: " + std::string(status.message);
      return EN_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest), 0,
                       msg.c_str());
    }
  }

  Uint32 src_length = static_cast<Uint32>(src.size());

  // Write immutable length of the string
  static const Uint32 MAX_BYTE_VALUE = 256;
  static_cast<char *>(dst)[offset] = static_cast<char>(src_length % MAX_BYTE_VALUE);
  static_cast<char *>(dst)[offset + 1] = static_cast<char>(src_length / MAX_BYTE_VALUE);
  offset += 2;

  static_cast<char *>(dst)[offset]     = 0;
  static_cast<char *>(dst)[offset + 1] = 0;
  offset += 2;

  memcpy(static_cast<char *>(dst) + offset, src.data(), src_length);

  static_cast<char *>(dst)[offset + src_length] = 0x00;

  return EN_Status(offset + src_length + 1);
}

std::vector<char> string_to_byte_array(std::string str) {
  std::vector<char> byte_array;
  byte_array.assign(std::make_move_iterator(str.begin()), std::make_move_iterator(str.end()));
  return byte_array;
}

std::vector<char> string_view_to_byte_array(const std::string_view &str_view) {
  return std::vector<char>(str_view.begin(), str_view.end());
}

Uint32 align_word(Uint32 head) {
  Uint32 a = head % 4;
  if (a != 0)
    head += (4 - a);
  return head;
}

Uint32 data_return_type(std::string_view drt) {
  if (drt == to_string(DataReturnType::DEFAULT_DRT)) {
    return DataReturnType::DEFAULT_DRT;
  }
  RDRSLogger::LOG_ERROR("Unknown data return type: " + std::string(drt));
  return UINT32_MAX;
}

/*
For both JSON and gRPC we need a way of letting the client know the datatype.

In JSON, strings are generally represented by using quotes (for numbers they are omitted).
For gRPC, we fixed our datatype to strings. So we pretend to have a JSON setup and also add
quotes when we are actually dealing with strings.

Since binary data is encoded as base64 strings, we also add quotes for these.
*/
std::string quote_if_string(Uint32 dataType, std::string value) {
  if (dataType == RDRS_INTEGER_DATATYPE || dataType == RDRS_FLOAT_DATATYPE) {
    return value;
  }
  return "\"" + value + "\"";
}

void printCharArray(const char *array, size_t length) {
  for (size_t i = 0; i < length; ++i) {
    std::cout << array[i];
  }
  std::cout << std::endl;
}

void printReqBuffer(const RS_Buffer *reqBuff) {
  char *reqData = reqBuff->buffer;
  std::cout << "Request buffer: " << std::endl;
  std::cout << "OP Type: " << std::hex << "0x" << ((Uint32 *)reqData)[0] << std::endl;
  std::cout << "Capacity: " << std::hex << "0x" << ((Uint32 *)reqData)[1] << std::endl;
  std::cout << "Length: " << std::hex << "0x" << ((Uint32 *)reqData)[2] << std::endl;
  std::cout << "Flags: " << std::hex << "0x" << ((Uint32 *)reqData)[3] << std::endl;
  std::cout << "DB Idx: " << std::hex << "0x" << ((Uint32 *)reqData)[4] << std::endl;
  std::cout << "Table Idx: " << std::hex << "0x" << ((Uint32 *)reqData)[5] << std::endl;
  std::cout << "PK Cols Idx: " << std::hex << "0x" << ((Uint32 *)reqData)[6] << std::endl;
  std::cout << "Read Cols Idx: " << std::hex << "0x" << ((Uint32 *)reqData)[7] << std::endl;
  std::cout << "OP ID Idx: " << std::hex << "0x" << ((Uint32 *)reqData)[8] << std::endl;
  std::cout << "DB: ";
  Uint32 dbIdx = ((Uint32 *)reqData)[4];
  std::cout << (char *)((UintPtr)reqData + dbIdx) << std::endl;
  std::cout << "Table: ";
  Uint32 tableIdx = ((Uint32 *)reqData)[5];
  std::cout << (char *)((UintPtr)reqData + tableIdx) << std::endl;
  Uint32 pkColsIdx = ((Uint32 *)reqData)[6];
  std::cout << "PK Cols Count: " << std::hex << "0x"
            << *((Uint32 *)((UintPtr)reqData + pkColsIdx)) << std::endl;
  for (Uint32 i = 0; i < *reinterpret_cast<Uint32 *>(reqData + pkColsIdx); i++) {
    int step = (i + 1) * ADDRESS_SIZE;
    std::cout << "KV pair " << i << " Idx: " << std::hex << "0x"
              << *((Uint32 *)((UintPtr)reqData + pkColsIdx + step)) << std::endl;
    Uint32 kvPairIdx = *((Uint32 *)((UintPtr)reqData + pkColsIdx + step));
    Uint32 keyIdx = ((Uint32 *)reqData)[kvPairIdx / ADDRESS_SIZE];
    std::cout << "Key idx: " << std::hex << "0x" << keyIdx << std::endl;
    std::cout << "Key " << i + 1 << ": ";
    std::cout << (char *)((UintPtr)reqData + keyIdx) << std::endl;
    Uint32 valueIdx = ((Uint32 *)reqData)[(kvPairIdx / ADDRESS_SIZE) + 1];
    std::cout << "Value idx: " << std::hex << "0x" << valueIdx << std::endl;
    std::cout << "Size " << i + 1 << ": ";
    std::cout << *((Uint16 *)((UintPtr)reqData + valueIdx)) << std::endl;
    std::cout << "Value " << i + 1 << ": ";
    std::cout << (char *)((UintPtr)reqData + valueIdx + ADDRESS_SIZE) << std::endl;
  }
  Uint32 readColsIdx = ((Uint32 *)reqData)[7];
  std::cout << "Read Cols Count: " << std::hex << "0x"
            << *((Uint32 *)((UintPtr)reqData + readColsIdx)) << std::endl;
  for (Uint32 i = 0;
       i < *reinterpret_cast<Uint32 *>(reinterpret_cast<UintPtr>(reqData) + readColsIdx); i++) {
    int step = (i + 1) * ADDRESS_SIZE;
    std::cout << "Read Col " << i << " Idx: " << std::hex << "0x"
              << *((Uint32 *)((UintPtr)reqData + readColsIdx + step)) << std::endl;
    Uint32 readColIdx = *((Uint32 *)((UintPtr)reqData + readColsIdx + step));
    Uint32 returnType = ((Uint32 *)reqData)[readColIdx / ADDRESS_SIZE];
    std::cout << "Return type: " << std::hex << "0x" << returnType << std::endl;
    std::cout << "Col " << i + 1 << ": ";
    std::cout << (char *)((UintPtr)reqData + readColIdx + ADDRESS_SIZE) << std::endl;
  }
  Uint32 opIDIdx = ((Uint32 *)reqData)[8];
  std::cout << "Op ID: ";
  std::cout << (char *)((UintPtr)reqData + opIDIdx) << std::endl;
}

void printStatus(RS_Status status) {
  std::cout << "Status: " << std::endl;
  std::cout << "http_code: " << status.http_code << std::endl;
  std::cout << "status: " << status.status << std::endl;
  std::cout << "classification: " << status.classification << std::endl;
  std::cout << "code: " << status.code << std::endl;
  std::cout << "mysql_code: " << status.mysql_code << std::endl;
  std::cout << "message: " << status.message << std::endl;
  std::cout << "err_line_no: " << status.err_line_no << std::endl;
  std::cout << "err_file_name: " << status.err_file_name << std::endl;
}

bool is_valid_utf8(std::string_view s) {
  size_t n = s.length();
  for (size_t i = 0; i < n;) {
    unsigned char c = s[i];

    // ASCII fast path
    if (c < 0x80) {
      ++i;
      continue;
    }

    int size;  // Size of the UTF-8 character
    if (c < 0xC2) {
      return false;  // Invalid UTF-8 header byte
    } else if (c < 0xE0) {
      size = 2;
    } else if (c < 0xF0) {
      size = 3;
    } else if (c <= 0xF4) {
      size = 4;
    } else {
      return false;  // Invalid UTF-8 header byte
    }

    if (i + size > n) {
      return false;  // Not enough bytes
    }

    for (int j = 1; j < size; ++j) {
      if ((s[i + j] & 0xC0) != 0x80) {
        return false;  // Invalid UTF-8 continuation byte
      }
    }

    i += size;
  }
  return true;
}

std::tuple<int, int> decode_rune_in_string(const std::string &s) {
  const int RuneError = 0xFFFD;  // Unicode replacement character
  size_t n            = s.length();

  if (n < 1) {
    return std::make_tuple(RuneError, 0);
  }

  unsigned char c = s[0];
  if (c < 0x80) {
    return std::make_tuple(c, 1);  // ASCII
  }

  int size;
  int rune;
  if (c < 0xE0) {
    size = 2;
    rune = c & 0x1F;
  } else if (c < 0xF0) {
    size = 3;
    rune = c & 0x0F;
  } else {
    size = 4;
    rune = c & 0x07;
  }

  if (n < static_cast<size_t>(size)) {
    return std::make_tuple(RuneError, 1);
  }

  for (int i = 1; i < size; ++i) {
    unsigned char cc = s[i];
    if ((cc & 0xC0) != 0x80) {
      return std::make_tuple(RuneError, 1);
    }
    rune = (rune << 6) | (cc & 0x3F);
  }

  return std::make_tuple(rune, size);
}

const int RuneError    = 0xFFFD;
const int rune1Max     = 0x7F;
const int rune2Max     = 0x7FF;
const int rune3Max     = 0xFFFF;
const int MaxRune      = 0x10FFFF;
const int surrogateMin = 0xD800;
const int surrogateMax = 0xDFFF;

// Utility constants for UTF-8 encoding
const uint8_t t2 = 0xC0, t3 = 0xE0, t4 = 0xF0, tx = 0x80, maskx = 0x3F;

int encode_rune(std::vector<char> &p, Uint32 r) {
  if (r <= rune1Max) {
    p.push_back(static_cast<char>(r));
    return 1;
  } else if (r <= rune2Max) {
    p.push_back(t2 | static_cast<char>(r >> 6));
    p.push_back(tx | (static_cast<char>(r) & maskx));
    return 2;
  } else if (r > MaxRune || (surrogateMin <= r && r <= surrogateMax)) {
    r = RuneError;
  }

  if (r <= rune3Max) {
    p.push_back(t3 | static_cast<char>(r >> 12));
    p.push_back(tx | (static_cast<char>(r >> 6) & maskx));
    p.push_back(tx | (static_cast<char>(r) & maskx));
    return 3;
  } else {
    p.push_back(t4 | static_cast<char>(r >> 18));
    p.push_back(tx | (static_cast<char>(r >> 12) & maskx));
    p.push_back(tx | (static_cast<char>(r >> 6) & maskx));
    p.push_back(tx | (static_cast<char>(r) & maskx));
    return 4;
  }
}

RS_Status unquote(std::vector<char> &str, bool unescape) {
  // if string to be unquoted is too short
  if (str.size() < 2) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "invalid syntax: too short string")
        .status;
  }

  char quote = str.front();
  auto end   = std::find(str.begin() + 1, str.end(), quote);

  // if no matching quote
  if (end == str.end()) {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "invalid syntax: no matching quote")
        .status;
  }

  auto end_pos =
      std::distance(str.begin(), end) +
      1;  // position after terminating quote; may be wrong if escape sequences are present

  std::string_view substring(str.data(), end_pos);

  switch (quote) {
  case '`': {
    if (!unescape) {
      // Keep the string as-is, including the quotes
      str.erase(str.begin() + end_pos);  // Remove everything after the quote
    } else {
      bool contains_cr = std::find(str.begin() + 1, end, '\r') != end;
      if (contains_cr) {
        // Remove carriage returns and quotes
        str.erase(std::remove(str.begin() + 1, end, '\r'), end);
      }
      // Remove the surrounding quotes
      str.erase(str.begin());
      str.erase(std::find(str.begin(), str.end(), quote));
    }
    break;
  }

  case '"':
  case '\'': {
    // Handle quoted strings without any escape sequences.
    std::string in(str.begin(), str.end());

    if (in.find('\\') == std::string::npos && in.find('\n') == std::string::npos) {
      bool valid = quote == '"' ? is_valid_utf8(in)
                                : static_cast<std::vector<char>::size_type>(
                                      std::get<1>(decode_rune_in_string(in))) == in.size();

      if (valid) {
        if (unescape) {
          str.erase(str.begin());                               // Remove the first quote
          str.erase(std::find(str.begin(), str.end(), quote));  // Remove the second quote
        }
      } else {
        // Invalid UTF-8 or improper single character in single quotes
        return CRS_Status(
          static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
          "invalid syntax: invalid UTF-8 or improper single character in single quotes").status;
      }
    } else {
      // Handle quoted strings with escape sequences
      if (unescape) {
        std::string unescaped =
            unescape_string(in.substr(1, in.size() - 2));  // Skip starting and ending quotes
        str.assign(unescaped.begin(), unescaped.end());
      }
    }
    break;
  }
  default: {
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      "invalid syntax: unknown quote type")
        .status;
  }
  }

  return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)).status;
}

RS_Status Unquote(std::vector<char> &str) {
  return unquote(str, true);
}
