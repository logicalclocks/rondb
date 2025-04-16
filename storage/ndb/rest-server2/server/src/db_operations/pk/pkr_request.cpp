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

#include "pkr_request.hpp"
#include "ndb_types.h"
#include "src/logger.hpp"
#include "src/rdrs_const.h"
#include "src/status.hpp"
#include "my_compiler.h"
#include "src/encoding_helper.hpp"
#include <string_view>
#include <EventLogger.hpp>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_REQ 1
#endif

#ifdef DEBUG_REQ
#define DEB_REQ(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_REQ(...) do { } while (0)
#endif


PKRRequest::PKRRequest(const RS_Buffer *request) {
  this->req = request;
  this->isInvalidOp = false;
  this->isReqModified = false;
}

Uint32 PKRRequest::OperationType() {
  return (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_OP_TYPE_IDX];
}

Uint32 PKRRequest::Length() {
  return (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_LENGTH_IDX];
}

Uint32 PKRRequest::Capacity() {
  return (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_CAPACITY_IDX];
}

const char *PKRRequest::DB() {
  Uint32 dbOffset = (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_DB_IDX];
  return req->buffer + dbOffset + 4;
}

const char *PKRRequest::Table() {
  Uint32 tableOffset =
    (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_TABLE_IDX];
  return req->buffer + tableOffset + 4;
}

Uint32 PKRRequest::PKColumnsCount() {
  Uint32 offset = (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_PK_COLS_IDX];
  Uint32 count  =
    (reinterpret_cast<Uint32 *>(req->buffer))[offset / ADDRESS_SIZE];
  return count;
}

Uint32 PKRRequest::PKTupleOffset(const int n) {
  // [count][kv offset1]...[kv offset n] [k offset][v offset] [ bytes ... ]
  // [k offset][v offset]...
  //                                      ^
  //          ............................|                                 ^
  //                         ...............................................|
  //
  Uint32 offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_PK_COLS_IDX];
  // +1 for count
  Uint32 kvOffset =
    (reinterpret_cast<Uint32 *>(req->buffer))[(offset / ADDRESS_SIZE) + 1 + n];
  return kvOffset;
}

const char *PKRRequest::PKName(Uint32 index) {
  Uint32 kvOffset = PKTupleOffset(index);
  Uint32 kOffset  = (reinterpret_cast<Uint32 *>(req->buffer))[kvOffset / 4];
  return req->buffer + kOffset + ADDRESS_SIZE;
}

Uint32 PKRRequest::PKNameLen(Uint32 index) {
  Uint32 kvOffset = PKTupleOffset(index);
  Uint32 kOffset  = (reinterpret_cast<Uint32 *>(req->buffer))[kvOffset / 4];
  const char *ptr = req->buffer + kOffset;
  const Uint32 *len_ptr = reinterpret_cast<const Uint32*>(ptr);
  return *len_ptr;
}

const char *PKRRequest::PKValueCStr(Uint32 index) {
  Uint32 kvOffset = PKTupleOffset(index);
  Uint32 vOffset  =
    (reinterpret_cast<Uint32 *>(req->buffer))[(kvOffset / 4) + 1];
  // skip first 4 bytes that contain size of string
  return req->buffer + vOffset + 4;
}

/*
  PKValueLen refers to data without prepended length bytes.
  The length bytes are only native to RonDB.
*/
Uint32 PKRRequest::PKValueLen(Uint32 index) {
  Uint32 kvOffset = PKTupleOffset(index);
  Uint32 vOffset =
    (reinterpret_cast<Uint32 *>(req->buffer))[(kvOffset / 4) + 1];
  unsigned char *data_start = (unsigned char *)req->buffer + vOffset;
  Uint32 *len_ptr = reinterpret_cast<Uint32*>(data_start);
  return *len_ptr;
}

Uint32 PKRRequest::ReadColumnsCount() {
  Uint32 offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_READ_COLS_IDX];
  if (unlikely(offset == 0)) {
    return 0;
  } else {
    Uint32 count =
      (reinterpret_cast<Uint32 *>(req->buffer))[offset / ADDRESS_SIZE];
    return count;
  }
}

const char *PKRRequest::ReadColumnName(const Uint32 n) {
  // [count][rc offset1]...[rc offset n] [ return type ] [ bytes ... ]
  //   [ return type ] [ bytes ... ]
  //                                                         
  //          ......................................| 
  //                         .............................................|

  Uint32 offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_READ_COLS_IDX];
  // +1 for count
  Uint32 r_offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[(offset / ADDRESS_SIZE) + 1 + n];
  DEB_REQ("RCName:Col id: %u, offset: %u, r_offset: %u", n, offset, r_offset);
  return req->buffer + r_offset + (ADDRESS_SIZE * 2);
}

Uint32 PKRRequest::ReadColumnNameLen(const Uint32 n) {
  // [count][rc offset1]...[rc offset n] [ return type ] [ bytes ... ]
  //   [ return type ] [ bytes ... ]
  //                                                         
  //          ......................................| 
  //                         .............................................|

  Uint32 offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_READ_COLS_IDX];
  // +1 for count
  Uint32 r_offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[(offset / ADDRESS_SIZE) + 1 + n];
  const char *ptr = req->buffer + r_offset + ADDRESS_SIZE;
  const Uint32 *len_ptr = reinterpret_cast<const Uint32*>(ptr);
  DEB_REQ("RCLen:Colid: %u, offset: %u, r_offset: %u, len: %u",
    n, offset, r_offset, *len_ptr);
  return *len_ptr;
}

DataReturnType PKRRequest::ReadColumnReturnType(const Uint32 n) {
  // [count][rc offset1]...[rc offset n] [ return type ] [ bytes ... ]
  // [ return type ] [ bytes ... ]
  //                                      ^
  //          ............................|                                 ^
  //                         ...............................................|
  Uint32 offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_READ_COLS_IDX];
  // +1 for count
  Uint32 c_offset =
    (reinterpret_cast<Uint32 *>(req->buffer))[(offset / ADDRESS_SIZE) + 1 + n];
  Uint32 type =
    (reinterpret_cast<Uint32 *>(req->buffer))[c_offset / ADDRESS_SIZE];
  return static_cast<DataReturnType>(type);
}

const char *PKRRequest::OperationId() {
  Uint32 offset = (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_OP_ID_IDX];
  if (likely(offset != 0)) {
    return req->buffer + offset + 4;
  } else {
    return nullptr;
  }
}

bool PKRRequest::addReadColumns(Uint32 numColumns) {
  Uint32 head = reinterpret_cast<Uint32 *>(req->buffer)[PK_REQ_LENGTH_IDX];
  reinterpret_cast<Uint32 *>(req->buffer)[PK_REQ_READ_COLS_IDX] = head;
  reinterpret_cast<Uint32 *>(req->buffer + head)[0] = numColumns;
  DEB_REQ("ARCol:Num: %u, head: %u", numColumns, head);
  Uint32 newHead = head + ((numColumns + 1) * ADDRESS_SIZE);
  reinterpret_cast<Uint32 *>(req->buffer)[PK_REQ_LENGTH_IDX] = newHead;

  // the req buffer is modified. save info for reset
  isReqModified=true;
  oldLen = head; 

  return true;
}

bool PKRRequest::resetReadColumns() {
  if (isReqModified) {
    reinterpret_cast<Uint32 *>(req->buffer + oldLen)[0] = 0; 
    reinterpret_cast<Uint32 *>(req->buffer)[PK_REQ_LENGTH_IDX] = oldLen;
    isReqModified=false;
    oldLen=0;
    return true;
  }
  return false; 
}

bool PKRRequest::addReadColumnName(Uint32 index,
                                   const char *name,
                                   Uint32 data_type) {
  std::string_view name_view(name, strlen(name));
  Uint32 col_head =
    reinterpret_cast<Uint32 *>(req->buffer)[PK_REQ_READ_COLS_IDX];
  Uint32 head = reinterpret_cast<Uint32 *>(req->buffer)[PK_REQ_LENGTH_IDX];
  Uint32 *col_head_ptr = reinterpret_cast<Uint32 *>(req->buffer + col_head);
  col_head_ptr[index + 1] = head;
  EN_Status status = {};
  Uint32 *head_ptr = reinterpret_cast<Uint32 *>(req->buffer + head);
  head_ptr[0] = data_type;
  head += ADDRESS_SIZE;
  DEB_REQ("ARCName: id: %u, name: %s, name_len: %u, head: %u, col_head: %u",
    index, name_view.data(), (Uint32)name_view.size(), head, col_head);
  head = copy_str_to_buffer(name_view, (Uint32*)req->buffer, head, status);
  if (unlikely(head == 0)) {
    return true;
  }
  DEB_REQ("ARCName: new head: %u", head);
  reinterpret_cast<Uint32 *>(req->buffer)[PK_REQ_LENGTH_IDX] = head;
  return false;
}

void PKRRequest::MarkInvalidOp(RS_Status error) {
  this->error = error;
  this->isInvalidOp = true;
}

RS_Status PKRRequest::GetError() {
  return this->error;
}

bool PKRRequest::IsInvalidOp() {
  return this->isInvalidOp;
}
