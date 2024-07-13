/*
<<<<<<< HEAD
   Copyright (c) 2003, 2023, Oracle and/or its affiliates.
   Copyright (c) 2021, 2023, Hopsworks and/or its affiliates.
=======
   Copyright (c) 2003, 2024, Oracle and/or its affiliates.
>>>>>>> 6dcee9fa4b19e67dea407787eba88e360dd679d9

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

#define DBTUP_C
#define DBTUP_TAB_DES_MAN_CPP
#include <ndb_limits.h>
#include <RefConvert.hpp>
#include <pc.hpp>
#include "Dbtup.hpp"

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

Uint32 Dbtup::getTabDescrOffsets(Uint32 noOfAttrs, Uint32 noOfCharsets,
                                 Uint32 noOfKeyAttr, Uint32 extraColumns,
                                 Uint32 *offset) {
  // belongs to configure.in
  unsigned sizeOfPointer = sizeof(CHARSET_INFO *);
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

Uint32 Dbtup::getDynTabDescrOffsets(Uint32 MaskSize, Uint32 *offset) {
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
<<<<<<< HEAD
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
=======
  Uint32 list =
      nextHigherTwoLog(allocSize - 1); /* CALCULATE WHICH LIST IT BELONGS TO */
  for (Uint32 i = list; i < 16; i++) {
    jam();
    if (cfreeTdList[i] != RNIL) {
      jam();
      reference = cfreeTdList[i];
      removeTdArea(reference, i); /* REMOVE THE AREA FROM THE FREELIST      */
      Uint32 retNo = (1 << i) - allocSize; /* CALCULATE THE DIFFERENCE */
      if (retNo >= ZTD_FREE_SIZE) {
        jam();
        // return unused words, of course without attempting left merge
        Uint32 retRef = reference + allocSize;
        freeTabDescr(retRef, retNo, false);
      } else {
        jam();
        allocSize = 1 << i;
      }  // if
      break;
    }  // if
  }    // for
  if (reference == RNIL) {
    jam();
    terrorCode = ZMEM_NOTABDESCR_ERROR;
    return RNIL;
  } else {
    jam();
    setTabDescrWord((reference + allocSize) - ZTD_TR_TYPE, ZTD_TYPE_NORMAL);
    setTabDescrWord(reference + ZTD_DATASIZE, allocSize);

    /* INITIALIZE THE TRAILER RECORD WITH TYPE AND SIZE     */
    /* THE TRAILER IS USED TO SIMPLIFY MERGE OF FREE AREAS  */

    setTabDescrWord(reference + ZTD_HEADER, ZTD_TYPE_NORMAL);
    setTabDescrWord((reference + allocSize) - ZTD_TR_SIZE, allocSize);
    return reference;
  }  // if
}  // Dbtup::allocTabDescr()

void Dbtup::freeTabDescr(Uint32 retRef, Uint32 retNo, bool normal) {
  itdaMergeTabDescr(retRef, retNo,
                    normal); /* MERGE WITH POSSIBLE NEIGHBOURS   */
  while (retNo >= ZTD_FREE_SIZE) {
    jam();
    Uint32 list = nextHigherTwoLog(retNo);

    if (list == 0 || list > 16) {
      verifytabdes();
      ndbabort();
    }

    list--; /* RETURN TO NEXT LOWER LIST    */
    Uint32 sizeOfChunk = 1 << list;
    insertTdArea(retRef, list);
    retRef += sizeOfChunk;

    if (retNo < sizeOfChunk) {
      verifytabdes();
      ndbabort();
    }

    retNo -= sizeOfChunk;
  }  // while
  ndbassert(retNo == 0);
}  // Dbtup::freeTabDescr()

Uint32 Dbtup::getTabDescrWord(Uint32 index) {
  ndbrequire(index < cnoOfTabDescrRec);
  return tableDescriptor[index].tabDescr;
}  // Dbtup::getTabDescrWord()

void Dbtup::setTabDescrWord(Uint32 index, Uint32 word) {
  ndbrequire(index < cnoOfTabDescrRec);
  tableDescriptor[index].tabDescr = word;
}  // Dbtup::setTabDescrWord()

void Dbtup::insertTdArea(Uint32 tabDesRef, Uint32 list) {
  if (tabDesRef >= cnoOfTabDescrRec) {
    verifytabdes();
    ndbabort();
  }
  if (list >= 16) {
    verifytabdes();
    ndbabort();
  }

  RSS_OP_FREE_X(cnoOfFreeTabDescrRec, 1 << list);
  setTabDescrWord(tabDesRef + ZTD_FL_HEADER, ZTD_TYPE_FREE);
  setTabDescrWord(tabDesRef + ZTD_FL_NEXT, cfreeTdList[list]);
  if (cfreeTdList[list] != RNIL) {
    jam(); /* PREVIOUSLY EMPTY SLOT     */
    setTabDescrWord(cfreeTdList[list] + ZTD_FL_PREV, tabDesRef);
  }                              // if
  cfreeTdList[list] = tabDesRef; /* RELINK THE LIST           */

  setTabDescrWord(tabDesRef + ZTD_FL_PREV, RNIL);
  setTabDescrWord(tabDesRef + ZTD_FL_SIZE, 1 << list);
  setTabDescrWord((tabDesRef + (1 << list)) - ZTD_TR_TYPE, ZTD_TYPE_FREE);
  setTabDescrWord((tabDesRef + (1 << list)) - ZTD_TR_SIZE, 1 << list);
}  // Dbtup::insertTdArea()

/*
 * Merge to-be-removed chunk (which need not be initialized with header
 * and trailer) with left and right buddies.  The start point retRef
 * moves to left and the size retNo increases to match the new chunk.
 */
void Dbtup::itdaMergeTabDescr(Uint32 &retRef, Uint32 &retNo, bool normal) {
  // merge right
  while ((retRef + retNo) < cnoOfTabDescrRec) {
    jam();
    Uint32 tabDesRef = retRef + retNo;
    Uint32 headerWord = getTabDescrWord(tabDesRef + ZTD_FL_HEADER);
    if (headerWord == ZTD_TYPE_FREE) {
      jam();
      Uint32 sizeOfMergedPart = getTabDescrWord(tabDesRef + ZTD_FL_SIZE);

      retNo += sizeOfMergedPart;
      Uint32 list = nextHigherTwoLog(sizeOfMergedPart - 1);
      removeTdArea(tabDesRef, list);
    } else {
      jam();
      break;
    }
  }
  // merge left
  const bool mergeLeft = normal;
  while (mergeLeft && retRef > 0) {
    jam();
    Uint32 trailerWord = getTabDescrWord(retRef - ZTD_TR_TYPE);
    if (trailerWord == ZTD_TYPE_FREE) {
      jam();
      Uint32 sizeOfMergedPart = getTabDescrWord(retRef - ZTD_TR_SIZE);
      ndbrequire(retRef >= sizeOfMergedPart);
      retRef -= sizeOfMergedPart;
      retNo += sizeOfMergedPart;
      Uint32 list = nextHigherTwoLog(sizeOfMergedPart - 1);
      removeTdArea(retRef, list);
    } else {
      jam();
      break;
    }
  }
  if ((retRef + retNo) > cnoOfTabDescrRec) {
    verifytabdes();
    ndbabort();
  }
}  // Dbtup::itdaMergeTabDescr()

/* ---------------------------------------------------------------- */
/* ------------------------ REMOVE_TD_AREA ------------------------ */
/* ---------------------------------------------------------------- */
/*                                                                  */
/* THIS ROUTINE REMOVES A TD CHUNK FROM THE POOL OF TD RECORDS      */
/*                                                                  */
/* INPUT:  TLIST          LIST TO USE                               */
/*         TAB_DESCR_PTR  POINTS TO THE CHUNK TO BE REMOVED         */
/*                                                                  */
/* SHORTNAME:   RMTA                                                */
/* -----------------------------------------------------------------*/
void Dbtup::removeTdArea(Uint32 tabDesRef, Uint32 list) {
  if (tabDesRef >= cnoOfTabDescrRec) {
    verifytabdes();
    ndbabort();
  }
  if (list >= 16) {
    verifytabdes();
    ndbabort();
  }

  RSS_OP_ALLOC_X(cnoOfFreeTabDescrRec, 1 << list);

  Uint32 tabDescrNextPtr = getTabDescrWord(tabDesRef + ZTD_FL_NEXT);
  Uint32 tabDescrPrevPtr = getTabDescrWord(tabDesRef + ZTD_FL_PREV);

  setTabDescrWord(tabDesRef + ZTD_HEADER, ZTD_TYPE_NORMAL);
  setTabDescrWord((tabDesRef + (1 << list)) - ZTD_TR_TYPE, ZTD_TYPE_NORMAL);

  if (tabDesRef == cfreeTdList[list]) {
    jam();
    cfreeTdList[list] = tabDescrNextPtr; /* RELINK THE LIST           */
  }                                      // if
  if (tabDescrNextPtr != RNIL) {
    jam();
    setTabDescrWord(tabDescrNextPtr + ZTD_FL_PREV, tabDescrPrevPtr);
  }  // if
  if (tabDescrPrevPtr != RNIL) {
    jam();
    setTabDescrWord(tabDescrPrevPtr + ZTD_FL_NEXT, tabDescrNextPtr);
  }  // if
}  // Dbtup::removeTdArea()

void Dbtup::verifytabdes() {
  struct WordType {
    short fl;  // free list 0-15
    short ti;  // table id
    short td;  // table descriptor area 0 or >0 for dynamic
    WordType() : fl(-1), ti(-1), td(-1) {}
  };
  WordType *wt = new WordType[cnoOfTabDescrRec];
  uint free_words = 0;
  uint free_frags = 0;
  uint used_words = 0;
  // free lists
>>>>>>> 6dcee9fa4b19e67dea407787eba88e360dd679d9
  {
    return mem;
  }
  terrorCode = ZMEM_NOTABDESCR_ERROR;
  return nullptr;

}
