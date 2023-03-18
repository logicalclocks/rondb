/*
   Copyright (c) 2003, 2022, Oracle and/or its affiliates.
   Copyright (c) 2021, 2022, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#define DBTUP_C
#define DBTUP_TAB_DES_MAN_CPP
#include "Dbtup.hpp"
#include <RefConvert.hpp>
#include <ndb_limits.h>
#include <pc.hpp>

#define JAM_FILE_ID 412

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_TAB_MALLOC 1
#endif

#ifdef DEBUG_TAB_MALLOC
#define DEB_TAB_MALLOC(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_TAB_MALLOC(arglist) do { } while (0)
#endif

/*
 * TABLE DESCRIPTOR MEMORY MANAGER
 *
 * Each table has a descriptor which is a contiguous array of words.
 * Newer NDB versions also have additional "dynamic descriptors"
 * which are allocated separately using the same method.
 *
 * The descriptor is allocated from a global array using a buddy
 * algorithm.  Free lists exist for each power of 2 words.  Freeing
 * a piece first merges with free right and left neighbours and then
 * divides itself up into free list chunks.
 */

Uint32
Dbtup::getTabDescrOffsets(Uint32 noOfAttrs,
                          Uint32 noOfCharsets,
                          Uint32 noOfKeyAttr,
                          Uint32 extraColumns,
                          Uint32* offset)
{
  // belongs to configure.in
  unsigned sizeOfPointer = sizeof(CHARSET_INFO*);
  ndbrequire((sizeOfPointer & 0x3) == 0);
  sizeOfPointer = (sizeOfPointer >> 2);
  // do in layout order and return offsets (see DbtupMeta.cpp)
  Uint32 allocSize = 0;
  // magically aligned to 8 bytes
  allocSize = 0;
  offset[0] = 0;
  offset[1] = allocSize += noOfAttrs * sizeOfReadFunction();
  offset[2] = allocSize += noOfAttrs * sizeOfReadFunction();
  offset[3] = allocSize += noOfCharsets * sizeOfPointer;
  offset[4] = allocSize += noOfKeyAttr;
  offset[5] = allocSize += (noOfAttrs + extraColumns) * ZAD_SIZE;
  offset[6] = allocSize += (noOfAttrs+1) >> 1;  // real order
  // return number of words
  return allocSize;
}

Uint32
Dbtup::getDynTabDescrOffsets(Uint32 MaskSize, Uint32* offset)
{
  // do in layout order and return offsets (see DbtupMeta.cpp)
  offset[0] = 0;
  offset[1] = MaskSize;
  Uint32 allocSize = 2 * MaskSize;
  // return number of words
  return allocSize;
}

void
Dbtup::releaseTabDescr(Uint32* desc)
{
  if (desc != nullptr)
  {
    DEB_TAB_MALLOC(("(%u) releaseTabDescr(%p)",
                   instance(),
                   desc));
    lc_ndbd_pool_free(desc);
  }
}

Uint32*
Dbtup::allocTabDescr(Uint32 allocSize, Uint32 tableId)
{
  /* ---------------------------------------------------------------- */
  /*       ALWAYS ALLOCATE A MULTIPLE OF 16 WORDS                     */
  /* ---------------------------------------------------------------- */
  allocSize = (((allocSize - 1) >> 4) + 1) << 4;
  (void)tableId;
  Uint32* mem = (Uint32*)lc_ndbd_pool_malloc(allocSize * 4,
                                             RG_SCHEMA_MEMORY,
                                             getThreadId(),
                                             false);
  DEB_TAB_MALLOC(("(%u) tab(%u): allocTabDescr(%u), ret: %p"
                  "offset: %u, size: %u",
                 instance(),
                 tableId,
                 allocSize,
                 mem,
                 mem[-1],
                 mem[-2]));
  if (mem != nullptr)
  {
    return mem;
  }
  terrorCode = ZMEM_NOTABDESCR_ERROR;
  return nullptr;

}
