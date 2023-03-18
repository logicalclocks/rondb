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

#ifndef PC_H
#define PC_H


#include "Emulator.hpp"
#include <NdbOut.hpp>
#include <ndb_limits.h>
#include <NdbThread.h>

#define JAM_FILE_ID 282

/* Jam buffer pointer. */
struct EmulatedJamBuffer;
extern thread_local EmulatedJamBuffer* NDB_THREAD_TLS_JAM;

/* Thread self pointer. */
struct thr_data;
extern thread_local thr_data* NDB_THREAD_TLS_THREAD;

#define qt_likely likely
#define qt_unlikely unlikely

#ifdef NDB_DEBUG_RES_OWNERSHIP

/* (Debug only) Shared resource owner. */
extern thread_local Uint32 NDB_THREAD_TLS_RES_OWNER;

#endif

/**
 * To enable jamDebug and its siblings in a production simply
 * remove the comment and get EXTRA_JAM defined.
 * It is enabled in builds using ERROR_INSERT to simplify tracing
 * of bugs from autotest.
 *
 * Similarly enable initialisation of global variables in a block
 * thread before executing each asynchronous signal by enabling
 * USE_INIT_GLOBAL_VARIABLES. This is also enabled in all builds
 * using ERROR_INSERT to ensure that we quickly discover failures
 * in using global variables.
 */
#if defined(ERROR_INSERT)
#define EXTRA_JAM 1
#define USE_INIT_GLOBAL_VARIABLES 1
#endif

#ifdef NO_EMULATED_JAM

#define _internal_thrjamLinenumber
#define _internal_thrjamData

#else

/**
 * Make an entry in the jamBuffer to record that execution reached a given point
 * in the source code (file and line number). For a description of how to
 * maintain and debug JAM_FILE_IDs, please refer to the comments for
 * jamFileNames in Emulator.cpp.
 */
#define _internal_thrjamLinenumber(jamBufferArg, lineNumber)  \
  do { \
    EmulatedJamBuffer* const jamBuffer = jamBufferArg; \
    /* Make sure both file and line number are known at compile-time. */ \
    constexpr Uint32 constJamFileId = (JAM_FILE_ID); \
    constexpr Uint32 constLineNumber = (lineNumber); \
    /* Statically check that file id fits in 14 bits. */ \
    static_assert((constJamFileId & 0x3fff) == constJamFileId); \
    /* Statically check that file id does not collide with Empty jam type. */ \
    static_assert(constJamFileId != 0x3fff); \
    /* Statically check that line number fits in 16 bits. */ \
    static_assert((constLineNumber & 0xffff) == constLineNumber); \
    /* Make sure the whole jam event is known at compile-time. */ \
    constexpr JamEvent newJamEvent = JamEvent(constJamFileId, constLineNumber, \
                                              true); \
    /* Insert the event */ \
    jamBuffer->insertJamEvent(newJamEvent); \
    /**
     * Occasionally, check at run-time that the jam buffer belongs to this
     * thread.
     */ \
    assert((jamBuffer->theEmulatedJamIndex & 3) != 0 || \
           jamBuffer == NDB_THREAD_TLS_JAM); \
    /* Statically check that jamFileNames[JAM_FILE_ID] matches __FILE__.*/ \
    static_assert(JamEvent::verifyId((JAM_FILE_ID), __FILE__)); \
  } while (0)

/**
 * Make an entry in the jamBuffer to record file number and up to 16 bits of
 * arbitrary data.
 */
#define _internal_thrjamData(jamBufferArg, data) \
  do { \
    EmulatedJamBuffer* const jamBuffer = jamBufferArg; \
    /* Make sure file number is known at compile-time. */ \
    constexpr Uint32 constJamFileId = (JAM_FILE_ID); \
    /* Statically check that file id fits in 14 bits */ \
    static_assert((constJamFileId & 0x3fff) == constJamFileId); \
    /* Statically check that file id does not collide with Empty jam type */ \
    static_assert(constJamFileId != 0x3fff); \
    jamBuffer->insertJamEvent(JamEvent(constJamFileId, (data), false)); \
    /**
     * Occasionally, check at run-time that the jam buffer belongs to this
     * thread.
     */ \
    assert((jamBuffer->theEmulatedJamIndex & 3) != 0 || \
           jamBuffer == NDB_THREAD_TLS_JAM); \
    /* Statically check that jamFileNames[JAM_FILE_ID] matches __FILE__.*/ \
    static_assert(JamEvent::verifyId((JAM_FILE_ID), __FILE__)); \
  } while (0)
#endif

/** The jam macro names have seven parts:
    1) "_internal_": Only for use in macro definitions.
       "":           For use in code.
    2) "thr":        The macro takes a jam buffer as an argument.
       "":           No jam buffer argument, it's inferred.
    3) "jam"
    4) "Block":      The macro takes a block object as argument
       "NoBlock":    Not called from a block. The jam buffer is NDB_THREAD_TLS_JAM.
       "":           No block object argument, it's inferred.
    5) "Entry":      Present for historical reasons. Now same as "".
       ""
    6) "Linenumber": The macro takes a line number argument.
       "Data":       The macro takes an argument with arbitrary data. Line
                     number is not registered.
       "Line":       Deprecated synonym for "Data".
       "":           No argument, line number is inferred.
    7) "Debug":      Turned off in production unless EXTRA_JAM is set.
       "":           Turned on unless NO_EMULATED_JAM is set.

    There are many possible combinations, so here we define only those that are
    actually used, as well as the "Data" macros since "Line" is deprecated.
*/

#define thrjamData(buf, data) _internal_thrjamData(buf, data)
#define thrjamLine(buf, data) thrjamData(buf, data)

#define jamBlockData(block, data) thrjamData(block->jamBuffer(), data)
#define jamBlockLine(block, data) jamBlockData(block, data)
#define _internal_jamBlockLinenumber(block, line) _internal_thrjamLinenumber(block->jamBuffer(), line)
#define jamBlock(block) _internal_jamBlockLinenumber((block), __LINE__)
#define jamData(data) jamBlockData(this, (data))
#define jamLine(data) jamData(data)
#define _internal_jamLinenumber(line) _internal_jamBlockLinenumber(this, (line))
#define jam() _internal_jamLinenumber(__LINE__)

#define jamEntry() jam()

#define _internal_jamNoBlockLinenumber(line) _internal_thrjamLinenumber(NDB_THREAD_TLS_JAM, line)
#define jamNoBlock() _internal_jamNoBlockLinenumber(__LINE__)

#define thrjam(buf) _internal_thrjamLinenumber(buf, __LINE__)
#define thrjamEntry(buf) thrjam(buf)

#if defined VM_TRACE || defined ERROR_INSERT || defined EXTRA_JAM
#define jamDebug() jam()
#define jamDataDebug(data) jamData(data)
#define jamLineDebug(data) jamLine(data)
#define jamEntryDebug() jamEntry()
#define thrjamEntryDebug(buf) thrjamEntry(buf)
#define thrjamDebug(buf) thrjam(buf)
#define thrjamDataDebug(buf, data) thrjamData(buf, data)
#define thrjamLineDebug(buf, data) thrjamLine(buf, data)
#else
#define jamDebug()
#define jamDataDebug(data)
#define jamLineDebug(data)
#define jamEntryDebug()
#define thrjamEntryDebug(buf)
#define thrjamDebug(buf)
#define thrjamDataDebug(buf, data)
#define thrjamLineDebug(buf, data)
#endif

#ifndef NDB_OPT
#define ptrCheck(ptr, limit, rec) if (ptr.i < (limit)) ptr.p = &rec[ptr.i]; else ptr.p = NULL

/**
 * Sets the p-value of a ptr-struct to be a pointer to record no i  
 * (where i is the i-value of the ptr-struct)
 *
 * @param ptr    ptr-struct with a set i-value  (the p-value in this gets set)
 * @param limit  max no of records in rec
 * @param rec    pointer to first record in an array of records
 */
#define ptrCheckGuardErr(ptr, limit, rec, error) {\
  UintR TxxzLimit; \
  TxxzLimit = (limit); \
  UintR TxxxPtr; \
  TxxxPtr = ptr.i; \
  ptr.p = &rec[TxxxPtr]; \
  if (TxxxPtr < (TxxzLimit)) { \
    ; \
  } else { \
    progError(__LINE__, error, __FILE__); \
  }}
#define ptrAss(ptr, rec) ptr.p = &rec[ptr.i]
#define ptrNull(ptr) ptr.p = NULL
#define ptrGuardErr(ptr, error) if (ptr.p == NULL) \
    progError(__LINE__, error, __FILE__)
#define arrGuardErr(ind, size, error) if ((ind) >= (size)) \
    progError(__LINE__, error, __FILE__)
#else
#define ptrCheck(ptr, limit, rec) ptr.p = &rec[ptr.i]
#define ptrCheckGuardErr(ptr, limit, rec, error) ptr.p = &rec[ptr.i]
#define ptrAss(ptr, rec) ptr.p = &rec[ptr.i]
#define ptrNull(ptr) ptr.p = NULL
#define ptrGuardErr(ptr, error)
#define arrGuardErr(ind, size, error)
#endif

#define ptrCheckGuard(ptr, limit, rec) \
  ptrCheckGuardErr(ptr, limit, rec, NDBD_EXIT_POINTER_NOTINRANGE)
#define ptrGuard(ptr) ptrGuardErr(ptr, NDBD_EXIT_POINTER_NOTINRANGE)
#define arrGuard(ind, size) arrGuardErr(ind, size, NDBD_EXIT_INDEX_NOTINRANGE)

// -------- ERROR INSERT MACROS -------
#ifdef ERROR_INSERT
#define ERROR_INSERT_VARIABLE mutable UintR cerrorInsert, c_error_insert_extra
#define ERROR_INSERTED(x) (unlikely(cerrorInsert == (x)))
#define ERROR_INSERTED_CLEAR(x) (cerrorInsert == (x) ? (cerrorInsert = 0, true) : false)
#define ERROR_INSERT_VALUE cerrorInsert
#define ERROR_INSERT_EXTRA c_error_insert_extra
#define SET_ERROR_INSERT_VALUE(x) cerrorInsert = x
#define SET_ERROR_INSERT_VALUE2(x,y) cerrorInsert = x; c_error_insert_extra = y
#define CLEAR_ERROR_INSERT_VALUE cerrorInsert = 0
#define CLEAR_ERROR_INSERT_EXTRA c_error_insert_extra = 0
#else
#define ERROR_INSERT_VARIABLE typedef void * cerrorInsert // Will generate compiler error if used
#define ERROR_INSERTED(x) false
#define ERROR_INSERTED_CLEAR(x) false
#define ERROR_INSERT_VALUE 0
#define ERROR_INSERT_EXTRA Uint32(0)
#define SET_ERROR_INSERT_VALUE(x) do { } while(0)
#define SET_ERROR_INSERT_VALUE2(x,y) do { } while(0)
#define CLEAR_ERROR_INSERT_VALUE do { } while(0)
#define CLEAR_ERROR_INSERT_EXTRA do { } while(0)
#endif

#define DECLARE_DUMP0(BLOCK, CODE, DESC) if (arg == CODE)

/* ------------------------------------------------------------------------- */
/*       COMMONLY USED CONSTANTS.                                            */
/* ------------------------------------------------------------------------- */
#define ZFALSE 0
#define ZTRUE 1
#define ZSET 1
#define ZOK 0
#define ZNOT_OK 1
#define ZCLOSE_FILE 2
#define ZNIL 0xffff
#define Z8NIL 255
#define UINT28_MAX ((1 << 28) - 1)

/* ------------------------------------------------------------------------- */
// Number of fragments stored per node. Should be settable on a table basis
// in future version since small tables want small value and large tables
// need large value.
/* ------------------------------------------------------------------------- */
#define NO_OF_FRAG_PER_NODE 1
#define MAX_FRAG_PER_LQH (4 * MAX_NDB_PARTITIONS + 16)

/* ---------------------------------------------------------------- */
// To avoid syncing too big chunks at a time we synch after writing
// a certain number of data/UNDO pages. (e.g. 2 MBytes).
/* ---------------------------------------------------------------- */
#define MAX_REDO_PAGES_WITHOUT_SYNCH 32

/* ------------------------------------------------------------------ */
// We have these constants to ensure that we can easily change the
// parallelism of node recovery and the amount of scan 
// operations needed for node recovery.
/* ------------------------------------------------------------------ */
#define MAX_NO_WORDS_OUTSTANDING_COPY_FRAGMENT 6000
#define MAGIC_CONSTANT 56
#define NODE_RECOVERY_SCAN_OP_RECORDS \
         (4 + ((4*MAX_NO_WORDS_OUTSTANDING_COPY_FRAGMENT)/ \
         ((MAGIC_CONSTANT + 2) * 5)))

#ifdef NO_CHECKPOINT
#define NO_LCP
#define NO_GCP
#endif
#define ZUNDEFINED_GCI_LIMIT 1
#define DEFAULT_SPIN_TIME 0
#define MEASURE_SPIN_TIME 60
#define MAX_SPIN_TIME 500
#define MAX_SPIN_OVERHEAD 10000
#define MIN_SPINTIME_PER_CALL 300
#define MAX_SPINTIME_PER_CALL 8000

/**
 * Ndb kernel blocks assertion handling
 *
 * Two type of assertions:
 * - ndbassert  - Only used when compiling VM_TRACE
 * - ndbrequire - Always checked
 *
 * If a ndbassert/ndbrequire fails, the system will 
 * shutdown and generate an error log
 *
 *
 * NOTE these may only be used within blocks
 */
#if defined(VM_TRACE) || defined(ERROR_INSERT)
#define ndbassert(check) \
  if(likely(check)){ \
  } else {     \
    jamNoBlock(); \
    progError(__LINE__, NDBD_EXIT_NDBASSERT, __FILE__, #check); \
  }
#else
#define ndbassert(check) do { } while(0)
#endif

#define ndbrequireErr(check, error) \
  if(likely(check)){ \
  } else {     \
    jamNoBlock(); \
    progError(__LINE__, error, __FILE__, #check); \
  }

#define ndbrequire(check) \
  ndbrequireErr(check, NDBD_EXIT_NDBREQUIRE)

#define ndbabort() \
  do { \
    jamNoBlock(); \
    progError(__LINE__, NDBD_EXIT_PRGERR, __FILE__, ""); \
  } while (false)

#define CRASH_INSERTION(errorType) \
  if (!ERROR_INSERTED((errorType))) { \
  } else { \
    jamNoBlock(); \
    progError(__LINE__, NDBD_EXIT_ERROR_INSERT, __FILE__); \
  }

#define CRASH_INSERTION2(errorNum, condition) \
  if (!(ERROR_INSERTED(errorNum) && condition)) { \
  } else { \
    jamNoBlock(); \
    progError(__LINE__, NDBD_EXIT_ERROR_INSERT, __FILE__); \
  }

#define CRASH_INSERTION3() \
  { \
    jamNoBlock(); \
    progError(__LINE__, NDBD_EXIT_ERROR_INSERT, __FILE__); \
  }
#define MEMCOPY_PAGE(to, from, page_size_in_bytes) \
  memcpy((void*)(to), (void*)(from), (size_t)(page_size_in_bytes));
#define MEMCOPY_NO_WORDS(to, from, no_of_words) \
  memcpy((to), (void*)(from), (size_t)((no_of_words) << 2));

// Get the jam buffer for the current thread.
inline EmulatedJamBuffer* getThrJamBuf()
{
  return NDB_THREAD_TLS_JAM;
}

#undef JAM_FILE_ID

#endif
