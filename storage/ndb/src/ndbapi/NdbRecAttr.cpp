/*
   Copyright (c) 2003, 2024, Oracle and/or its affiliates.
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include <NdbTCP.h>
#include <ndb_global.h>
#include <NdbBlob.hpp>
#include <NdbOut.hpp>
#include <NdbRecAttr.hpp>
#include "NdbDictionaryImpl.hpp"
#include <NdbTCP.h>
#include "AttributeHeader.hpp"

NdbRecAttr::NdbRecAttr(Ndb *) {
  init();
}

NdbRecAttr::~NdbRecAttr() { release(); }

int
NdbRecAttr::setup(const class NdbDictionary::Column* col,
                  char* aValue,
                  Uint32 aStartPos,
                  Uint32 aSize) {
  return setup(&(col->m_impl),
               aValue,
               aStartPos,
               aSize);
}

int
NdbRecAttr::setup(const NdbColumnImpl* anAttrInfo,
                  char* aValue,
                  Uint32 aStartPos,
                  Uint32 aSize) {
  release();
  Uint32 byteSize = 0;
  if (anAttrInfo != nullptr) {
    Uint32 tAttrSize = anAttrInfo->m_attrSize;
    Uint32 tArraySize = anAttrInfo->m_arraySize;
    byteSize = tAttrSize * tArraySize;
  
    m_column = anAttrInfo;

    theAttrId = anAttrInfo->m_attrId;
    theStartPos = aStartPos;
    theSize = aSize;
    m_size_in_bytes = -1; //UNDEFINED

    m_getVarValue = nullptr; // set in getVarValue() only
  } else {
    // Moz
    // Aggregation
    byteSize = MAX_AGG_RESULT_BATCH_BYTES;
    m_column = nullptr;
    theAttrId = AttributeHeader::AGG_RESULT;
    m_size_in_bytes = -1;
  }
  return setup(byteSize, aValue);
}

int
NdbRecAttr::setup(Uint32 byteSize, char* aValue)
{
  // Check if application provided pointer should be used
  // NOTE! Neither pointers alignment or length of attribute matters since
  // memcpy() will be used to copy received data there.
  if (aValue != nullptr) {
    theRef = (Uint64*)aValue;
    theMemorySource = EXT_MALLOC;
    return 0;
  }

  if (byteSize <= 16) {
    theStorage[0] = 0;
    theStorage[1] = 0;
    theRef = theStorage;
    theMemorySource = INT_STORAGE;
    return 0;
  }
  Uint32 tSize = (byteSize + 7) >> 3;
  Uint64 *tRef = new Uint64[tSize];
  if (tRef != nullptr) {
    memset(tRef, 0, 8 * tSize);
    theRef = tRef;
    theMemorySource = INT_MALLOC;
    return 0;
  }
  init();
  errno = ENOMEM;
  return -1;
}

NdbRecAttr *NdbRecAttr::clone() const {
  NdbRecAttr *ret = new NdbRecAttr(nullptr);
  if (ret == nullptr) {
    errno = ENOMEM;
    return nullptr;
  }
  ret->theAttrId = theAttrId;
  ret->m_size_in_bytes = m_size_in_bytes;
  ret->m_column = m_column;
  ret->theStartPos = theStartPos;
  ret->theSize = theSize;
  
  Uint32 n = m_size_in_bytes;
  if(n <= 16) {
    ret->theRef = ret->theStorage;
    ret->theMemorySource = INT_STORAGE;
  } else {
    if (Int32(n) != -1) {
      ret->theRef = new Uint64[((n + 7) >> 3)];
    } else {
      ret->theRef = nullptr;
    }
    if (ret->theRef == nullptr) {
      delete ret;
      errno = ENOMEM;
      return nullptr;
    }
    ret->theMemorySource = INT_MALLOC;
  }
  memcpy(ret->theRef, theRef, n);
  return ret;
}

bool NdbRecAttr::receive_data(const Uint32 *data32, Uint32 sz) {
  const unsigned char *data = (const unsigned char *)data32;
  if (sz) {
    if (unlikely(m_getVarValue != nullptr)) {
      // ONLY for blob V2 implementation
      assert(m_column->getType() == NdbDictionary::Column::Longvarchar ||
             m_column->getType() == NdbDictionary::Column::Longvarbinary);
      assert(sz >= 2);
      Uint32 len = data[0] + (data[1] << 8);
      assert(len == sz - 2);
      assert(len < (1 << 16));
      *m_getVarValue = len;
      data += 2;
      sz -= 2;
    }

    // Copy received data to destination pointer
    memcpy(theRef, data, sz);

    m_size_in_bytes = sz;
    return true;
  }

  return setNULL();
}

static const NdbRecordPrintFormat default_print_format;

NdbOut &ndbrecattr_print_formatted(NdbOut &out, const NdbRecAttr &r,
                                   const NdbRecordPrintFormat &f) {
  return NdbDictionary::printFormattedValue(
      out, f, r.getColumn(), r.isNULL() == 0 ? r.aRef() : nullptr);
}

NdbOut &operator<<(NdbOut &out, const NdbRecAttr &r) {
  return ndbrecattr_print_formatted(out, r, default_print_format);
}

Int64 NdbRecAttr::int64_value() const {
  Int64 val;
  memcpy(&val, theRef, 8);
  return val;
}

Uint64 NdbRecAttr::u_64_value() const {
  Uint64 val;
  memcpy(&val, theRef, 8);
  return val;
}

float NdbRecAttr::float_value() const {
  float val;
  memcpy(&val, theRef, sizeof(val));
  return val;
}

double NdbRecAttr::double_value() const {
  double val;
  memcpy(&val, theRef, sizeof(val));
  return val;
}

Int32 NdbRecAttr::medium_value() const {
  return sint3korr((unsigned char *)theRef);
}

Uint32 NdbRecAttr::u_medium_value() const {
  return uint3korr((unsigned char *)theRef);
}
