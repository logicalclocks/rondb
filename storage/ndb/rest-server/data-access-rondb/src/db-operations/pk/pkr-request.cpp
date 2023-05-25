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

#include "src/db-operations/pk/pkr-request.hpp"
#include "src/logger.hpp"
#include "src/rdrs-const.h"
#include "src/status.hpp"

PKRRequest::PKRRequest(const RS_Buffer *request) {
  this->req         = request;
  this->isInvalidOp = false;
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
  return req->buffer + dbOffset;
}

const char *PKRRequest::Table() {
  Uint32 tableOffset = (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_TABLE_IDX];
  return req->buffer + tableOffset;
}

Uint32 PKRRequest::PKColumnsCount() {
  Uint32 offset = (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_PK_COLS_IDX];
  Uint32 count  = (reinterpret_cast<Uint32 *>(req->buffer))[offset / ADDRESS_SIZE];
  return count;
}

Uint32 PKRRequest::PKTupleOffset(const int n) {
  // [count][kv offset1]...[kv offset n] [k offset][v offset] [ bytes ... ] [k offset][v offset]...
  //                                      ^
  //          ............................|                                 ^
  //                         ...............................................|
  //

  Uint32 offset = (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_PK_COLS_IDX];
  Uint32 kvOffset =
      (reinterpret_cast<Uint32 *>(req->buffer))[(offset / ADDRESS_SIZE) + 1 + n];  // +1 for count
  return kvOffset;
}

const char *PKRRequest::PKName(Uint32 index) {
  Uint32 kvOffset = PKTupleOffset(index);
  Uint32 kOffset  = (reinterpret_cast<Uint32 *>(req->buffer))[kvOffset / 4];
  return req->buffer + kOffset;
}

const char *PKRRequest::PKValueCStr(Uint32 index) {
  Uint32 kvOffset = PKTupleOffset(index);
  Uint32 vOffset  = (reinterpret_cast<Uint32 *>(req->buffer))[(kvOffset / 4) + 1];

  return req->buffer + vOffset + 4;  // skip first 4 bytes that contain size of string
}

/*
  PKValueLen refers to data without prepended length bytes.
  The length bytes are only native to RonDB.
*/
Uint16 PKRRequest::PKValueLen(Uint32 index) {
  Uint32 kvOffset           = PKTupleOffset(index);
  Uint32 vOffset            = (reinterpret_cast<Uint32 *>(req->buffer))[(kvOffset / 4) + 1];
  unsigned char *data_start = (unsigned char *)req->buffer + vOffset;
  Uint16 len                = ((Uint16)data_start[1] * (Uint16)256) + (Uint16)data_start[0];
  return len;
}

/*
 * First two bytes are immutable containing the size of the string
 * next two bytes are mutable that also contain the size of the string.
 * Depending on the type of the column one or two bytes will be used for
 * storing size in the mutable size section
 *
 */
int PKRRequest::PKValueNDBStr(Uint32 index, const NdbDictionary::Column *col, char **data) {
  Uint32 kvOffset  = PKTupleOffset(index);
  Uint32 vOffset   = (reinterpret_cast<Uint32 *>(req->buffer))[(kvOffset / 4) + 1];
  char *data_start = req->buffer + vOffset;

  // The Go layer sets the length of the string in the first two bytes of the string
  const NdbDictionary::Column::ArrayType array_type = col->getArrayType();
  const size_t max_size                             = col->getSizeInBytes();
  const size_t user_size                            = this->PKValueLen(index);

  if (user_size > max_size) {
    *data = nullptr;
    return -1;
  }

  switch (array_type) {
  case NdbDictionary::Column::ArrayTypeFixed:
    // No prefix length is stored in string
    *data = data_start + 4;  // skip the first four bytes that contain the length of the string
    return 0;
  case NdbDictionary::Column::ArrayTypeShortVar:
    // one byte length
    data_start[3] = data_start[0];
    *data         = data_start + 3;
    return 0;
  case NdbDictionary::Column::ArrayTypeMediumVar:
    // two bytes length
    data_start[2] = data_start[0];
    data_start[3] = data_start[1];

    // skip the first two immuable length bytes
    *data = data_start + 2;
    return 0;
  default:
    *data = nullptr;
    return -1;
  }
}

Uint32 PKRRequest::ReadColumnsCount() {
  Uint32 offset = (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_READ_COLS_IDX];
  if (offset == 0) {
    return 0;
  } else {
    Uint32 count = (reinterpret_cast<Uint32 *>(req->buffer))[offset / ADDRESS_SIZE];
    return count;
  }
}

const char *PKRRequest::ReadColumnName(const Uint32 n) {
  // [count][rc offset1]...[rc offset n] [ return type ] [ bytes ... ] [ return type ] [ bytes ... ]
  //                                                         ^
  //          ...............................................|                                 ^
  //                         ..................................................................|

  Uint32 offset = (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_READ_COLS_IDX];
  Uint32 r_offset =
      (reinterpret_cast<Uint32 *>(req->buffer))[(offset / ADDRESS_SIZE) + 1 + n];  // +1 for count
  return req->buffer + r_offset + ADDRESS_SIZE;
}

DataReturnType PKRRequest::ReadColumnReturnType(const Uint32 n) {
  // [count][rc offset1]...[rc offset n] [ return type ] [ bytes ... ] [ return type ] [ bytes ... ]
  //                                      ^
  //          ............................|                                 ^
  //                         ...............................................|

  Uint32 offset = (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_READ_COLS_IDX];
  Uint32 c_offset =
      (reinterpret_cast<Uint32 *>(req->buffer))[(offset / ADDRESS_SIZE) + 1 + n];  // +1 for count
  Uint32 type = (reinterpret_cast<Uint32 *>(req->buffer))[c_offset / ADDRESS_SIZE];
  return static_cast<DataReturnType>(type);
}

const char *PKRRequest::OperationId() {
  Uint32 offset = (reinterpret_cast<Uint32 *>(req->buffer))[PK_REQ_OP_ID_IDX];
  if (offset != 0) {
    return req->buffer + offset;
  } else {
    return nullptr;
  }
}

void PKRRequest::MarkInvalidOp(RS_Status error) {
  this->error = error;
  this->isInvalidOp = true;
}

RS_Status PKRRequest::GetError(RS_Status error) {
  return this->error;
}

bool PKRRequest::IsInvalidOp() {
  return this->isInvalidOp;
}
