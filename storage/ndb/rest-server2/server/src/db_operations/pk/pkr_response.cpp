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

#include "pkr_response.hpp"
#include "src/buffer_chain.hpp"
#include "src/mystring.hpp"
#include "src/rdrs_const.h"
#include "my_compiler.h"

#include <mysql.h>
#include <sql_string.h>
#include <iostream>
#include <sstream>
#include <cassert>
#include <memory>
#include <new>
#include <ndb_limits.h>
#include "buffer_sizes.hpp"

PKRResponse::PKRResponse(const RS_Buffer &home, RS_BufferChain *chain,
                         Uint32 op_index)
    : m_home(home), m_chain(chain), m_op_index(op_index) {
  this->writeHeader = PK_RESP_HEADER_END;
  this->WriteHeaderField(PK_RESP_OP_TYPE_IDX, RDRS_PK_RESP_ID);
  this->WriteHeaderField(PK_RESP_CAPACITY_IDX, m_home.size);
}

RS_Status PKRResponse::Overflow(const char *what, Uint64 required) {
  m_overflow = true;
  return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_RESPONSE_BUFFER_OVERFLOW)) + " " +
      what + ". Buffer Remaining Capacity: " +
      std::to_string(GetRemainingCapacity()) +
      " Required: " + std::to_string(required));
}

RS_Status PKRResponse::WriteHeaderField(Uint32 index, Uint32 value) {
  Uint32 *b = reinterpret_cast<Uint32 *>(m_home.buffer);
  b[index] = value;
  return RS_OK;
}

RS_Status PKRResponse::SetStatus(Uint32 value, const char *message) {
  this->WriteHeaderField(PK_RESP_OP_STATUS_IDX, value);
  Uint32 addr = this->writeHeader;
  RS_Status status = this->Append_cstring(message, strlen(message));
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  this->WriteHeaderField(PK_RESP_OP_MESSAGE_IDX, addr);
  return RS_OK;
}

RS_Status PKRResponse::Close(Uint32 &response_length) {
  /* The length field counts home view bytes only; spilled bytes are
   * accounted by their column entries. The reader's sanity check
   * (capacity >= length) therefore holds unchanged. */
  this->WriteHeaderField(PK_RESP_LENGTH_IDX, writeHeader);
  response_length = writeHeader;
  if (m_chain != nullptr) {
    m_chain->finish_op(m_op_index, writeHeader, m_spilled, m_tail_off);
  }
  return RS_OK;
}

RS_Status PKRResponse::SetOperationID(const char *opID) {
  if (opID == nullptr) {
    this->WriteHeaderField(PK_RESP_OP_ID_IDX, 0);
    return RS_OK;
  }
  Uint32 addr = this->writeHeader;
  RS_Status status = this->Append_cstring(opID, strlen(opID));
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  this->WriteHeaderField(PK_RESP_OP_ID_IDX, addr);
  return RS_OK;
}

RS_Status PKRResponse::Append_cstring(const char *str, Uint32 len) {
  Uint32 strl = len + 1;  // for null terminator
  if (unlikely(strl > GetRemainingCapacity())) {
    return Overflow("string", strl);
  }
  memcpy(m_home.buffer + writeHeader, str, len);
  m_home.buffer[writeHeader + len] = '\0';
  writeHeader += strl;
  return RS_OK;
}

RS_Status PKRResponse::SetNoOfColumns(Uint32 cols) {
  // 4 bytes alignment
  if (this->writeHeader % ADDRESS_SIZE != 0) {
    this->writeHeader += ADDRESS_SIZE - this->writeHeader % ADDRESS_SIZE;
  }
  // per column: value length, value address, isNULL, data type; plus the
  // column count in front
  Uint32 spaceNeeded4Pointers = 1 * ADDRESS_SIZE + (cols * ADDRESS_SIZE * 4);
  if (unlikely(spaceNeeded4Pointers > GetRemainingCapacity())) {
    return Overflow("column table", spaceNeeded4Pointers);
  }
  Uint32 colAddr = (this->writeHeader);
  WriteHeaderField(PK_RESP_COLS_IDX, colAddr);
  /* Zeroed so an entry no value was written for reads as a zero-length
   * value, never as garbage. */
  memset(m_home.buffer + colAddr, 0, spaceNeeded4Pointers);
  Uint32 *b = reinterpret_cast<Uint32 *>(m_home.buffer + colAddr);
  b[0] = cols;
  this->writeHeader = (this->writeHeader + spaceNeeded4Pointers);
  this->colsToWrite = cols;
  return RS_OK;
}

RS_Status PKRResponse::SetColumnDataNull() {
  return SetColumnDataInt(nullptr, RDRS_UNKNOWN_DATATYPE, 0);
}

RS_Status PKRResponse::SetColumnData(const char *value, Uint32 type,
                                     Uint32 len) {
  return this->SetColumnDataInt(value, type, len);
}

void PKRResponse::SetBlobLen(Uint32 slot, Uint32 len) {
  Uint32 *b = reinterpret_cast<Uint32 *>(m_home.buffer);
  Uint32 start = b[PK_RESP_COLS_IDX];
  start += ADDRESS_SIZE;  // skip the count
  int indexWritten = (start + (slot * 4 * ADDRESS_SIZE)) / ADDRESS_SIZE;
  b[indexWritten] = len;
}

RS_Status PKRResponse::SetColumnDataInt(const char *value, Uint32 type,
                                        Uint32 len) {
  // first index is for value length
  // second index is for value address
  // third index is for isNULL
  // fourth index is for data type, e.g., string, int, date etc
  Uint32 *b = reinterpret_cast<Uint32 *>(m_home.buffer);
  Uint32 start = b[PK_RESP_COLS_IDX];
  start += ADDRESS_SIZE;  // skip the count
  int indexWritten = (start + (m_slot * 4 * ADDRESS_SIZE)) / ADDRESS_SIZE;
  b[indexWritten] = len;
  if (value == nullptr) {
    b[indexWritten + 1] = 0;                      // value address not set
    b[indexWritten + 2] = 1;                      // isNULL
    b[indexWritten + 3] = RDRS_UNKNOWN_DATATYPE;  // data type
  } else {
    Uint32 valueAddress = this->writeHeader;
    RS_Status status = Append_cstring(value, len);
    if (unlikely(status.http_code != SUCCESS)) {
      return status;
    }
    b[indexWritten + 1] = valueAddress;  // value address
    b[indexWritten + 2] = 0;             // isNULL
    b[indexWritten + 3] = type;          // data type
  }
  return RS_OK;
}

Uint64 PKRResponse::CurrentVirtualOffset() const {
  if (m_spilled) {
    return static_cast<Uint64>(m_tail_base) + m_tail_off;
  }
  return writeHeader;
}

RS_Status PKRResponse::BeginRawColumn(Uint32 type) {
  const Uint64 offset = CurrentVirtualOffset();
  if (unlikely(offset > 0xFFFFFFFFULL)) {
    return Overflow("raw column start", offset);
  }
  Uint32 *b = reinterpret_cast<Uint32 *>(m_home.buffer);
  Uint32 start = b[PK_RESP_COLS_IDX];
  start += ADDRESS_SIZE;
  int indexWritten = (start + (m_slot * 4 * ADDRESS_SIZE)) / ADDRESS_SIZE;
  b[indexWritten] = 0;                             // length, set by SetBlobLen
  b[indexWritten + 1] = static_cast<Uint32>(offset);
  b[indexWritten + 2] = 0;                         // not NULL
  b[indexWritten + 3] = type;
  return RS_OK;
}

RS_Status PKRResponse::Spill() {
  if (m_chain == nullptr) {
    return Overflow("spill without a buffer chain", 0);
  }
  Uint32 owner_index = 0;
  RS_Status status = m_chain->take_continuation(&owner_index);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;  // 503, not an overflow
  }
  /* The virtual stream continues where the buffer just filled ended. */
  m_tail_base += m_spilled ? m_tail.size : m_home.size;
  m_tail = m_chain->owner(owner_index);
  m_tail_off = 0;
  m_spilled = true;
  return RS_OK;
}

RS_Status PKRResponse::AppendRawWithSpill(const char *src, Uint32 len) {
  Uint32 left = len;
  while (left > 0) {
    char *dst;
    Uint32 space;
    if (m_spilled) {
      dst = m_tail.buffer + m_tail_off;
      space = m_tail.size - m_tail_off;
    } else {
      dst = m_home.buffer + writeHeader;
      space = m_home.size - writeHeader;
    }
    const Uint32 take = left < space ? left : space;
    if (take > 0) {
      memcpy(dst, src, take);
      src += take;
      left -= take;
      if (m_spilled) {
        m_tail_off += take;
      } else {
        writeHeader += take;
      }
    }
    if (left > 0) {
      /* The current buffer is full to its last byte; continue in the next
       * one. Filling to the last byte is what lets the reader derive the
       * segments from the offset, the length and the chain alone. */
      RS_Status status = Spill();
      if (unlikely(status.http_code != SUCCESS)) {
        return status;
      }
    }
  }
  return RS_OK;
}

char *PKRResponse::GetResponseBuffer() {
  return m_home.buffer;
}

Uint32 PKRResponse::GetMaxCapacity() const {
  return m_home.size;
}

Uint32 PKRResponse::GetRemainingCapacity() const {
  return GetMaxCapacity() - GetWriteHeader();
}

Uint32 PKRResponse::GetWriteHeader() const {
  return this->writeHeader;
}

void *PKRResponse::GetWritePointer() {
  return m_home.buffer + writeHeader;
}

void PKRResponse::AdvanceWritePointer(Uint32 add) {
  writeHeader += add;
}

RS_Status PKRResponse::Append_string(std::string value, Uint32 type) {
  // +1 null terminator
  if (unlikely((value.length() + 1) > GetRemainingCapacity())) {
    return Overflow("string value", value.length() + 1);
  }
  return SetColumnData(value.c_str(), type, value.size());
}

RS_Status PKRResponse::Append_bin(const char *buffer, Uint32 value_len,
                                  Uint32 type) {
  if (unlikely(value_len + 1 > GetRemainingCapacity())) {
    return Overflow("binary value", static_cast<Uint64>(value_len) + 1);
  }
  return SetColumnData(buffer, type, value_len);
}

RS_Status PKRResponse::Append_i8(Int8 num) {
  return Append_i64(num);
}

RS_Status PKRResponse::Append_iu8(Uint8 num) {
  return Append_iu64(num);
}

RS_Status PKRResponse::Append_i16(Int16 num) {
  return Append_i64(num);
}

RS_Status PKRResponse::Append_iu16(Uint16 num) {
  return Append_iu64(num);
}

RS_Status PKRResponse::Append_i24(int num) {
  return Append_i64(num);
}

RS_Status PKRResponse::Append_iu24(Uint32 num) {
  return Append_iu64(num);
}

RS_Status PKRResponse::Append_iu32(Uint32 num) {
  return Append_iu64(num);
}

RS_Status PKRResponse::Append_i32(Int32 num) {
  return Append_i64(num);
}

RS_Status PKRResponse::Append_f32(float num) {
  return Append_d64(num);
}

RS_Status PKRResponse::Append_d64(double num) {
  try {
    std::stringstream ss;
    ss << num;
    return this->SetColumnData(ss.str().c_str(),
                               RDRS_FLOAT_DATATYPE,
                               ss.str().size());
  } catch (...) {
    return RS_SERVER_ERROR(std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)));
  }
}

RS_Status PKRResponse::Append_iu64(Uint64 num) {
  try {
    std::string numStr = std::to_string(num);
    return this->SetColumnData(numStr.c_str(),
                               RDRS_INTEGER_DATATYPE,
                               numStr.size());
  } catch (...) {
    return RS_SERVER_ERROR(std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)));
  }
}

RS_Status PKRResponse::Append_i64(Int64 num) {
  try {
    std::string numStr = std::to_string(num);
    return this->SetColumnData(numStr.c_str(),
                               RDRS_INTEGER_DATATYPE,
                               numStr.size());
  } catch (...) {
    return RS_SERVER_ERROR(std::string(rdrsErrorMessage(ERROR_WRONG_DATA_TYPE)));
  }
}

RS_Status PKRResponse::FormatChar(const char *fromBuff, Uint32 fromBuffLen,
                                  CHARSET_INFO *fromCS, bool trimSpaces,
                                  std::string &out) {
  //  from_buffer -> printable string  -> escaped string
  // allocate a buffer large enough to hold the formatted string
  const CHARSET_INFO* toCS = &my_charset_utf8mb4_bin;
  Uint64 estimated_bytes =
    (fromBuffLen / fromCS->mbminlen + 1) * toCS->mbmaxlen + 1;
  estimated_bytes = std::min(estimated_bytes, static_cast<Uint64>(UINT_MAX32));
  /* A TEXT value can make this large (RONDB-1144), so a failed allocation
   * is an error status, not an exception. */
  std::unique_ptr<char[]> tempBuff(new (std::nothrow) char[estimated_bytes]);
  if (unlikely(tempBuff == nullptr)) {
    return RS_SERVER_ERROR(std::string(
      rdrsErrorMessage(ERROR_MEMORY_ALLOCATION_FAILURE)));
  }
  const char *well_formed_error_pos = nullptr;
  const char *cannot_convert_error_pos = nullptr;
  const char *from_end_pos = nullptr;
  const char *error_pos = nullptr;
  size_t bytesFormed = well_formed_copy_nchars(toCS,
                                               tempBuff.get(),
                                               estimated_bytes,
                                               fromCS,
                                               fromBuff,
                                               fromBuffLen,
                                               UINT32_MAX,
                                               &well_formed_error_pos,
                                               &cannot_convert_error_pos,
                                               &from_end_pos);
  error_pos = well_formed_error_pos ?
    well_formed_error_pos : cannot_convert_error_pos;
  if (unlikely(error_pos)) {
    char printable_buff[32];
    convert_to_printable(printable_buff,
                         sizeof(printable_buff),
                         error_pos,
                         fromBuff + fromBuffLen - error_pos,
                         fromCS,
                         6);
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_INVALID_COLUMN_DATA)) +
      std::string(" Invalid string: ") +
      std::string(printable_buff));
  } else if (unlikely(from_end_pos < fromBuff + fromBuffLen)) {
    /*
      result is longer than UINT_MAX32 and doesn't fit into String
    */
    return RS_SERVER_ERROR(
      std::string(rdrsErrorMessage(ERROR_PROGRAMMING_BUFFER_TOO_SMALL)) +
      std::string(" Buffer size: ") +
      std::to_string(estimated_bytes) +
      std::string(". Bytes left to copy: ") +
      std::to_string((fromBuff + fromBuffLen) - from_end_pos));
  }
  std::string wellFormedString = std::string(tempBuff.get(), bytesFormed);
  if (trimSpaces) {
    // remove blank spaces that are padded to the string
    size_t endpos = wellFormedString.find_last_not_of(" ");
    if (std::string::npos != endpos) {
      wellFormedString = wellFormedString.substr(0, endpos + 1);
    }
  }
  out = escape_string(wellFormedString);
  return RS_OK;
}

RS_Status PKRResponse::Append_char(const char *fromBuff,
                                   Uint32 fromBuffLen,
                                   CHARSET_INFO *fromCS,
                                   bool trimSpaces) {
  std::string escapedstr;
  RS_Status status = FormatChar(fromBuff, fromBuffLen, fromCS, trimSpaces,
                                escapedstr);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  // +1 for null terminator
  if (unlikely((escapedstr.length() + 1) > GetRemainingCapacity())) {
    return Overflow("character value", escapedstr.length() + 1);
  }
  return this->SetColumnData(escapedstr.c_str(),
                             RDRS_STRING_DATATYPE,
                             escapedstr.size());
}
