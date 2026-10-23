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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKR_RESPONSE_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKR_RESPONSE_HPP_

#include "src/rdrs_dal.h"
#include "src/status.hpp"
#include "src/error_strings.h"

#include <stdint.h>
#include <cstring>
#include <string>

struct RS_BufferChain;

/*
 * Writes one operation's response: a fixed header, a column table, then
 * the values. Every address written is relative to the start of the home
 * view, so responses pack into a shared buffer.
 *
 * RONDB-1144: the fixed part (header, column table, every bounded value)
 * always lands in the home view. Blob and text values are written last
 * through BeginRawColumn/AppendRawWithSpill and may continue into
 * continuation buffers taken from the chain; their column entry carries the
 * total length and an offset in the response's virtual stream (home view
 * bytes first, then the continuation buffers in order). A failed capacity
 * check sets the overflow flag, which the caller uses to rewrite the
 * response in a fresh buffer.
 *
 * Arena-allocated and never destructed by its owner, so it holds copies of
 * the buffers it writes to and no std:: members.
 */
class PKRResponse {
 private:
  RS_Buffer m_home;             // the home view: buffer is the base, size the capacity
  RS_BufferChain *m_chain;      // owner of continuation buffers; may be null (no spill)
  Uint32 m_op_index;
  Uint32 writeHeader = 0;       // write offset in the home view
  Uint32 colsToWrite = 0;
  Uint32 m_slot = 0;            // column table entry the next value goes to
  bool m_overflow = false;
  bool m_spilled = false;
  RS_Buffer m_tail{};           // current continuation buffer, when spilled
  Uint32 m_tail_off = 0;        // write offset in m_tail
  Uint32 m_tail_base = 0;       // virtual stream offset of m_tail byte 0

  RS_Status Spill();
  RS_Status Append_cstring(const char *str, Uint32 len);
  RS_Status SetColumnDataInt(const char *value, Uint32 type, Uint32 len);
  RS_Status Overflow(const char *what, Uint64 required);

 public:
  PKRResponse(const RS_Buffer &home, RS_BufferChain *chain, Uint32 op_index);

  /* Column table entry the next SetColumnData, Append_xxx or
   * BeginRawColumn writes to. Entries keep request order while values may
   * be written in any order (bounded values first, blobs last). */
  void SetSlot(Uint32 slot) { m_slot = slot; }
  Uint32 GetSlot() const { return m_slot; }
  bool Overflowed() const { return m_overflow; }
  bool Spilled() const { return m_spilled; }

  RS_Status WriteHeaderField(Uint32 index, Uint32 value);
  RS_Status SetStatus(Uint32 value, const char* message);
  /* Writes the length of the home view and reports the write head to the
   * chain. length is the home view's length only. */
  RS_Status Close(Uint32 &length);
  RS_Status SetOperationID(const char *opID);
  /* Must be called before any column; reserves and zeroes the table. */
  RS_Status SetNoOfColumns(Uint32 cols);
  RS_Status SetColumnDataNull();
  RS_Status SetColumnData(const char *value, Uint32 type, Uint32 len);

  /* Start a value that is written piecewise with AppendRawWithSpill: the
   * entry at the current slot gets the virtual offset of its first byte;
   * SetBlobLen records its total length once it is complete. */
  RS_Status BeginRawColumn(Uint32 type);
  RS_Status AppendRawWithSpill(const char *src, Uint32 len);
  void SetBlobLen(Uint32 slot, Uint32 len);
  /* Virtual stream offset where the next raw byte goes. */
  Uint64 CurrentVirtualOffset() const;

  Uint32 GetMaxCapacity() const;
  Uint32 GetRemainingCapacity() const;
  char *GetResponseBuffer();
  Uint32 GetWriteHeader() const;
  void *GetWritePointer();
  void AdvanceWritePointer(Uint32 add);

  RS_Status Append_iu32(Uint32 num);
  RS_Status Append_i32(Int32 num);
  RS_Status Append_i64(Int64 num);
  RS_Status Append_iu64(Uint64 num);
  RS_Status Append_i8(Int8 num);
  RS_Status Append_iu8(Uint8 num);
  RS_Status Append_i16(Int16 num);
  RS_Status Append_iu16(Uint16 num);
  RS_Status Append_i24(int num);
  RS_Status Append_iu24(Uint32 num);
  RS_Status Append_f32(float num);
  RS_Status Append_d64(double num);
  RS_Status Append_string(std::string value, Uint32 type);
  RS_Status Append_bin(const char *buffer, Uint32 value_len, Uint32 type);
  /* Convert a CHAR/VARCHAR value to utf8mb4, trim it when asked, JSON-escape
   * it, and append it as one bounded value. */
  RS_Status Append_char(const char *from_buffer, Uint32 from_buffer_len,
                        CHARSET_INFO *from_cs, bool trim_spaces);
  /* The conversion, trimming and escaping of Append_char without the
   * capacity check: the escaped text in out. Used for TEXT values, which
   * are then written with AppendRawWithSpill. */
  RS_Status FormatChar(const char *from_buffer, Uint32 from_buffer_len,
                       CHARSET_INFO *from_cs, bool trim_spaces,
                       std::string &out);
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_DB_OPERATIONS_PK_PKR_RESPONSE_HPP_
