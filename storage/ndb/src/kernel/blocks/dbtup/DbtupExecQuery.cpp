/*
   Copyright (c) 2003, 2025, Oracle and/or its affiliates.
   Copyright (c) 2021, 2026, Hopsworks and/or its affiliates.

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
#include <ndb_limits.h>
#include <portlib/ndb_prefetch.h>
#include <AttributeDescriptor.hpp>
#include <AttributeHeader.hpp>
#include <Checksum.hpp>
#include <Interpreter.hpp>
#include <NdbSqlUtil.hpp>
#include <RefConvert.hpp>
#include <cstring>
#include <dblqh/Dblqh.hpp>
#include <pc.hpp>
#include <signaldata/AttrInfo.hpp>
#include <signaldata/LqhKey.hpp>
#include <signaldata/ScanFrag.hpp>
#include <signaldata/TransIdAI.hpp>
#include <signaldata/TupKey.hpp>
#include <signaldata/TuxMaint.hpp>
#include "../dblqh/Dblqh.hpp"
#include "AttributeOffset.hpp"
#include "Dbtup.hpp"
#include "AggInterpreter.hpp"
#include "JoinAggInterpreter.hpp"
#include "PushdownInterpreter.hpp"
#include "VecSearchInterpreter.hpp"
#include "dblqh/JoinAggregationState.hpp"
#include "my_time.h"
#include "my_systime.h"
#include <signaldata/AccLock.hpp>
#include "rondb_hash.hpp"
#include "../dbtux/Dbtux.hpp"
#include <my_byteorder.h>
#include <climits>
#include <cmath>

#define JAM_FILE_ID 422

#define TUP_NO_TUPLE_FOUND 626
#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_LCP 1
//#define DEBUG_REORG 1
//#define DEBUG_DELETE 1
//#define DEBUG_DELETE_NR 1
//#define DEBUG_LCP_LGMAN 1
//#define DEBUG_LCP_SKIP_DELETE 1
//#define DEBUG_DISK 1
//#define DEBUG_ELEM_COUNT 1
//#define DEBUG_COPY_TUPLE 1
//#define DEBUG_VARPART_EXPAND 1
//#define DEBUG_JOIN_AGG_TRACE 1
//#define DEBUG_STAR_AGG 1
#endif

#ifdef DEBUG_COPY_TUPLE
#define DEB_COPY_TUPLE(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_COPY_TUPLE(arglist) do { } while (0)
#endif

#ifdef DEBUG_REORG
#define DEB_REORG(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_REORG(arglist) do { } while (0)
#endif

#ifdef DEBUG_ELEM_COUNT
#define DEB_ELEM_COUNT(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_ELEM_COUNT(arglist) do { } while (0)
#endif

#ifdef DEBUG_STAR_AGG
#define DEB_STAR_AGG(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_STAR_AGG(arglist) do { } while (0)
#endif

#ifdef DEBUG_DISK
#define DEB_DISK(arglist)        \
  do {                           \
    g_eventLogger->info arglist; \
  } while (0)
#else
#define DEB_DISK(arglist) \
  do {                    \
  } while (0)
#endif

#ifdef DEBUG_LCP
#define DEB_LCP(arglist)         \
  do {                           \
    g_eventLogger->info arglist; \
  } while (0)
#else
#define DEB_LCP(arglist) \
  do {                   \
  } while (0)
#endif

#ifdef DEBUG_DELETE
#define DEB_DELETE(arglist)      \
  do {                           \
    g_eventLogger->info arglist; \
  } while (0)
#else
#define DEB_DELETE(arglist) \
  do {                      \
  } while (0)
#endif

#ifdef DEBUG_LCP_SKIP_DELETE
#define DEB_LCP_SKIP_DELETE(arglist) \
  do {                               \
    g_eventLogger->info arglist;     \
  } while (0)
#else
#define DEB_LCP_SKIP_DELETE(arglist) \
  do {                               \
  } while (0)
#endif

#ifdef DEBUG_DELETE_NR
#define DEB_DELETE_NR(arglist)   \
  do {                           \
    g_eventLogger->info arglist; \
  } while (0)
#else
#define DEB_DELETE_NR(arglist) \
  do {                         \
  } while (0)
#endif

#ifdef DEBUG_LCP_LGMAN
#define DEB_LCP_LGMAN(arglist)   \
  do {                           \
    g_eventLogger->info arglist; \
  } while (0)
#else
#define DEB_LCP_LGMAN(arglist) \
  do {                         \
  } while (0)
#endif

/**
 * DEBUG_JOIN_AGG_TRACE: Dump cinBuffer header and linked data extracted
 * from the subroutine section before passing to AggInterpreter.
 * Uncomment the define in the #if block above to activate.
 */
#ifdef DEBUG_JOIN_AGG_TRACE
#define DEB_JOIN_AGG(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_JOIN_AGG(arglist) do { } while (0)
#endif

#ifdef DEBUG_VARPART_EXPAND
#define DEB_VAR_EXPAND(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_VAR_EXPAND(arglist) do { } while (0)
#endif

//#define TRACE_INTERPRETER 1
//#define TRACE_INTERPRETER_REGISTERS 1

#define RET_NULL Uint32(~0)
#define EQUAL_MATCH 0
#define SMALLER_MATCH 1
#define LARGER_MATCH 2
#define SMALLER_EQUAL_MATCH 3
#define LARGER_EQUAL_MATCH 4

Uint32 binary_uint64_search_exact(Uint64 test_ordinal,
                                         const char *memory_ptr,
                                         Uint32 num_elems) {
  Uint32 start = 0;
  Uint32 end = num_elems;
  if (num_elems == 0) {
    return RET_NULL;
  }
  Uint64 value = 0;
  Uint32 test_position;
  while (start < end) {
    Uint32 mid_point = (start + end) / 2;
    test_position = mid_point * 8;
    memcpy(&value, memory_ptr + test_position, 8);
    if (value < test_ordinal) {
      start = mid_point + 1;
    } else {
      end = mid_point;
    }
  }
  if (start == num_elems) return RET_NULL;
  test_position = start * 8;
  memcpy(&value, memory_ptr + test_position, 8);
  if (value == test_ordinal)
    return start;
  return RET_NULL;
}

static Uint32 binary_uint64_search_smaller(Uint64 test_ordinal,
                                           const char *memory_ptr,
                                           Uint32 num_elems,
                                           bool not_include_equal,
                                           bool search_interval) {
  Uint32 start = 0;
  Uint32 end = num_elems;
  if (num_elems == 0) {
    return RET_NULL;
  }
  Uint64 value = 0;
  Uint32 test_position;
  while (start < end) {
    Uint32 mid_point = (start + end) / 2;
    test_position = mid_point * 8;
    memcpy(&value, memory_ptr + test_position, 8);
    if (value < test_ordinal) {
      start = mid_point + 1;
    } else {
      end = mid_point;
    }
  }
#ifdef TRACE_INTERPRETER
  g_eventLogger->info("Smaller: start: %u, end: %u, num_elems: %u, val: %llu,"
    " ord: %llu, flag: %u",
    start, end, num_elems, value, test_ordinal, not_include_equal);
#endif
  if (not_include_equal) {
    test_position = start * 8;
    memcpy(&value, memory_ptr + test_position, 8);
    if (value != test_ordinal) {
      if (start == 0) return RET_NULL;
      else return start - 1;
    } else if ((search_interval == true) &&
               ((start + 1) < num_elems)) {
      test_position = (start + 1) * 8;
      memcpy(&value, memory_ptr + test_position, 8);
      if (value == test_ordinal)
        return start + 1;
    }

  }
  return start;
}

static Uint32 binary_uint64_search_larger(Uint64 test_ordinal,
                                          const char *memory_ptr,
                                          Uint32 num_elems,
                                          bool not_include_equal,
                                          bool search_interval) {
  Uint32 start = 0;
  Uint32 end = num_elems;
  if (num_elems == 0) {
    return RET_NULL;
  }
  Uint64 value = 0;
  Uint32 test_position;
  while (start < end) {
    Uint32 mid_point = (start + end) / 2;
    test_position = mid_point * 8;
    memcpy(&value, memory_ptr + test_position, 8);
    if (value > test_ordinal) {
      end = mid_point;
    } else {
      start = mid_point + 1;
    }
  }
#ifdef TRACE_INTERPRETER
  g_eventLogger->info("Larger: start: %u, end: %u, num_elems: %u, val: %llu,"
    " ord: %llu, flag: %u",
    start, end, num_elems, value, test_ordinal, not_include_equal);
#endif

  if (not_include_equal == ZTRUE && end > 0) {
    test_position = (end - 1) * 8;
    memcpy(&value, memory_ptr + test_position, 8);
    if (value == test_ordinal) {
      if ((search_interval == false) ||
          (end == 1))
        return end - 1;
      test_position = (end - 2) * 8;
      memcpy(&value, memory_ptr + test_position, 8);
      if (value == test_ordinal)
        return end - 2;
      else
        return end - 1;
    }
  }
  return end;
}

static Uint32 binary_uint32_search_exact(Uint32 test_ordinal,
                                         const char *memory_ptr,
                                         Uint32 num_elems) {
  Uint32 start = 0;
  Uint32 end = num_elems;
  if (num_elems == 0) {
    return RET_NULL;
  }
  Uint32 value = 0;
  Uint32 test_position;
  while (start < end) {
    Uint32 mid_point = (start + end) / 2;
    test_position = mid_point * 4;
    memcpy(&value, memory_ptr + test_position, 4);
    if (value < test_ordinal) {
      start = mid_point + 1;
    } else {
      end = mid_point;
    }
  }
  if (start == num_elems) return RET_NULL;
  test_position = start * 4;
  memcpy(&value, memory_ptr + test_position, 4);
  if (value == test_ordinal)
    return start;
  return RET_NULL;
}

static Uint32 binary_uint32_search_smaller(Uint32 test_ordinal,
                                           const char *memory_ptr,
                                           Uint32 num_elems,
                                           bool not_include_equal,
                                           bool search_interval) {
  Uint32 start = 0;
  Uint32 end = num_elems;
  if (num_elems == 0) {
    return RET_NULL;
  }
  Uint32 value = 0;
  Uint32 test_position;
  while (start < end) {
    Uint32 mid_point = (start + end) / 2;
    test_position = mid_point * 4;
    memcpy(&value, memory_ptr + test_position, 4);
    if (value < test_ordinal) {
      start = mid_point + 1;
    } else {
      end = mid_point;
    }
  }
#ifdef TRACE_INTERPRETER
  g_eventLogger->info("Smaller: start: %u, end: %u, num_elems: %u, val: %u,"
    " ord: %u, flag: %u",
    start, end, num_elems, value, test_ordinal, not_include_equal);
#endif
  if (not_include_equal) {
    test_position = start * 4;
    memcpy(&value, memory_ptr + test_position, 4);
    if (value != test_ordinal) {
      if (start == 0) return RET_NULL;
      else return start - 1;
    } else if ((search_interval == true) &&
               ((start + 1) < num_elems)) {
      /**
       * This is a special case:
       * We might have something like (4,5,5,8). In this case we will have
       * start == 1 and would return 1 which would lead to NULL. But in
       * reality we should return 2 since 5 is also part of the next interval.
       */
      test_position = (start + 1) * 4;
      memcpy(&value, memory_ptr + test_position, 4);
      if (value == test_ordinal)
        return start + 1;
    }
  }
  return start;
}

static Uint32 binary_uint32_search_larger(Uint32 test_ordinal,
                                          const char *memory_ptr,
                                          Uint32 num_elems,
                                          bool not_include_equal,
                                          bool search_interval) {
  Uint32 start = 0;
  Uint32 end = num_elems;
  if (num_elems == 0) {
    return RET_NULL;
  }
  Uint32 value = 0;
  Uint32 test_position;
  while (start < end) {
    Uint32 mid_point = (start + end) / 2;
    test_position = mid_point * 4;
    memcpy(&value, memory_ptr + test_position, 4);
    if (value > test_ordinal) {
      end = mid_point;
    } else {
      start = mid_point + 1;
    }
  }
#ifdef TRACE_INTERPRETER
  g_eventLogger->info("Larger: start: %u, end: %u, num_elems: %u, val: %u,"
    " ord: %u, flag: %u",
    start, end, num_elems, value, test_ordinal, not_include_equal);
#endif
  if (not_include_equal && end > 0) {
    test_position = (end - 1) * 4;
    memcpy(&value, memory_ptr + test_position, 4);
    if (value == test_ordinal) {
      if ((search_interval == false) ||
          (end == 1))
        return end - 1;
      /**
       * Special case to handle, this could be e.g.
       * (2,3,3,5) and we are searching for 3, in this case we should
       * return end - 2 rather than end - 1.
       */
      test_position = (end - 2) * 4;
      memcpy(&value, memory_ptr + test_position, 4);
      if (value == test_ordinal)
        return end - 2;
      else
        return end - 1;
    }
  }
  return end;
}

Uint32 binary_uint16_search_exact(Uint16 test_ordinal,
                                  const char *memory_ptr,
                                  Uint32 num_elems) {
  Uint32 start = 0;
  Uint32 end = num_elems;
  if (num_elems == 0) {
    return RET_NULL;
  }
  Uint16 value = 0;
  Uint32 test_position;
  while (start < end) {
    Uint32 mid_point = (start + end) / 2;
    test_position = mid_point * 2;
    memcpy(&value, memory_ptr + test_position, 2);
    if (value < test_ordinal) {
      start = mid_point + 1;
    } else {
      end = mid_point;
    }
  }
  if (start == num_elems) return RET_NULL;
  test_position = start * 2;
  memcpy(&value, memory_ptr + test_position, 2);
  if (value == test_ordinal)
    return start;
  return RET_NULL;
}

static Uint32 binary_uint16_search_smaller(Uint16 test_ordinal,
                                           const char *memory_ptr,
                                           Uint32 num_elems,
                                           bool not_include_equal,
                                           bool search_interval) {
  Uint32 start = 0;
  Uint32 end = num_elems;
  if (num_elems == 0) {
    return RET_NULL;
  }
  Uint16 value = 0;
  Uint32 test_position;
  while (start < end) {
    Uint32 mid_point = (start + end) / 2;
    test_position = mid_point * 2;
    memcpy(&value, memory_ptr + test_position, 2);
    if (value < test_ordinal) {
      start = mid_point + 1;
    } else {
      end = mid_point;
    }
  }
  if (not_include_equal) {
    test_position = start * 2;
    memcpy(&value, memory_ptr + test_position, 2);
    if (value != test_ordinal) {
      if (start == 0) return RET_NULL;
      else return start - 1;
    } else if ((search_interval == true) &&
               ((start + 1) < num_elems)) {
      test_position = (start + 1) * 2;
      memcpy(&value, memory_ptr + test_position, 2);
      if (value == test_ordinal)
        return start + 1;
    }
  }
  return start;
}

static Uint32 binary_uint16_search_larger(Uint16 test_ordinal,
                                          const char *memory_ptr,
                                          Uint32 num_elems,
                                          bool not_include_equal,
                                          bool search_interval) {
  Uint32 start = 0;
  Uint32 end = num_elems;
  if (num_elems == 0) {
    return RET_NULL;
  }
  Uint16 value = 0;
  Uint32 test_position;
  while (start < end) {
    Uint32 mid_point = (start + end) / 2;
    test_position = mid_point * 2;
    memcpy(&value, memory_ptr + test_position, 2);
    if (value > test_ordinal) {
      end = mid_point;
    } else {
      start = mid_point + 1;
    }
  }
  if (not_include_equal && end > 0) {
    test_position = (end - 1) * 2;
    memcpy(&value, memory_ptr + test_position, 2);
    if (value == test_ordinal) {
      if ((search_interval == false) ||
          (end == 1))
        return end - 1;
      test_position = (end - 2) * 2;
      memcpy(&value, memory_ptr + test_position, 2);
      if (value == test_ordinal)
        return end - 2;
      else
        return end - 1;
    }
  }
  return end;
}

static Uint64
get_odd_sized_number(const char *memory_ptr,
                     Uint32 test_position,
                     Uint32 number_size) {
  Uint64 value = 0;
  const uchar *number_ptr = (const uchar*)(memory_ptr + test_position);
  switch (number_size) {
    case 1: {
      Uint8 val8 = *number_ptr;
      value = (Uint64)val8;
      break;
    }
    case 3: {
      Uint32 val32 = uint3korr(number_ptr);
      value = (Uint64)val32;
      break;
    }
    case 5: {
      value = uint5korr(number_ptr);
      break;
    }
    case 6: {
      value = uint6korr(number_ptr);
      break;
    }
    default: {
      require(false);
    }
  }
  return value;
}

static Uint32 binary_odd_search_exact(Uint64 test_ordinal,
                                      const char *memory_ptr,
                                      Uint32 num_elems,
                                      Uint32 number_size) {
  Uint32 start = 0;
  Uint32 end = num_elems;
  if (num_elems == 0) {
    return RET_NULL;
  }
  Uint64 value = 0;
  Uint32 test_position;
  while (start < end) {
    Uint32 mid_point = (start + end) / 2;
    test_position = mid_point * number_size;
    value = get_odd_sized_number(memory_ptr, test_position, number_size);
    if (value < test_ordinal) {
      start = mid_point + 1;
    } else {
      end = mid_point;
    }
  }
  if (start == num_elems) return RET_NULL;
  test_position = start * number_size;
  value = get_odd_sized_number(memory_ptr, test_position, number_size);
  if (value == test_ordinal)
    return start;
  return RET_NULL;
}

static Uint32 binary_odd_search_smaller(Uint64 test_ordinal,
                                        const char *memory_ptr,
                                        Uint32 num_elems,
                                        Uint32 number_size,
                                        bool not_include_equal,
                                        bool search_interval) {
  Uint32 start = 0;
  Uint32 end = num_elems;
  if (num_elems == 0) {
    return RET_NULL;
  }
  Uint64 value = 0;
  Uint32 test_position;
  while (start < end) {
    Uint32 mid_point = (start + end) / 2;
    test_position = mid_point * number_size;
    value = get_odd_sized_number(memory_ptr, test_position, number_size);
    if (value < test_ordinal) {
      start = mid_point + 1;
    } else {
      end = mid_point;
    }
  }
#ifdef TRACE_INTERPRETER
  g_eventLogger->info("Smaller: start: %u, end: %u, num_elems: %u,"
    " val: %llu, ord: %llu, flag: %u, size: %u",
    start, end, num_elems, value, test_ordinal, not_include_equal, number_size);
#endif
  if (not_include_equal) {
    test_position = start * number_size;
    value = get_odd_sized_number(memory_ptr, test_position, number_size);
    if (value != test_ordinal) {
      if (start == 0) return RET_NULL;
      else return (start - 1);
      return end - 1;
    } else if ((search_interval == true) &&
               ((start + 1) < num_elems)) {
      test_position = (start + 1) * number_size;
      value = get_odd_sized_number(memory_ptr, test_position, number_size);
      if (value == test_ordinal)
        return start + 1;
    }
  }
  return start;
}

static Uint32 binary_odd_search_larger(Uint64 test_ordinal,
                                       const char *memory_ptr,
                                       Uint32 num_elems,
                                       Uint32 number_size,
                                       bool not_include_equal,
                                       bool search_interval) {
  Uint32 start = 0;
  Uint32 end = num_elems;
  if (num_elems == 0) {
    return RET_NULL;
  }
  Uint64 value = 0;
  Uint32 test_position;
  while (start < end) {
    Uint32 mid_point = (start + end) / 2;
    test_position = mid_point * number_size;
    value = get_odd_sized_number(memory_ptr, test_position, number_size);
    if (value > test_ordinal) {
      end = mid_point;
    } else {
      start = mid_point + 1;
    }
  }
#ifdef TRACE_INTERPRETER
  g_eventLogger->info("Larger: start: %u, end: %u, num_elems: %u,"
    " val: %llu, ord: %llu, flag: %u, size: %u",
    start, end, num_elems, value, test_ordinal, not_include_equal, number_size);
#endif
  if (not_include_equal && end > 0) {
    test_position = (end - 1) * number_size;
    value = get_odd_sized_number(memory_ptr, test_position, number_size);
    if (value == test_ordinal) {
      if ((search_interval == false) ||
          (end == 1))
        return end - 1;
      test_position = (end - 2) * number_size;
      value = get_odd_sized_number(memory_ptr, test_position, number_size);
      if (value == test_ordinal)
        return end - 2;
      else
        return end - 1;
    }
  }
  return end;
}

static Uint32 string_search(const char *search_string,
                            Uint32 search_len,
                            const char *string_ptr,
                            Uint32 string_len) {
  Uint32 equal_len = 0;
  for (Uint32 i = 0; i < string_len; i++) {
    char c_search = search_string[equal_len];
    char c_string = string_ptr[i];
    if (c_search == c_string) {
      equal_len++;
      if (equal_len == search_len) {
        return (i - search_len) + 1;
      }
    } else {
      equal_len = 0;
    }
  }
  return RET_NULL;
}

static void compress_num32_array(char *memory_ptr,
                                 Uint32 elems,
                                 size_t number_size) {
  switch (number_size) {
    case 3: {
      for (size_t i = 0; i < elems; i++) {
        Uint32 val = 0;
        memcpy(&val, memory_ptr + (4 * i), 4);
        int3store(memory_ptr + (3 * i), (uint)val);
      }
      return;
    }
    default: {
      require(false);
    }
  }
  return;
}

static void compress_num64_array(char *memory_ptr,
                                 Uint32 elems,
                                 size_t number_size) {
  switch (number_size) {
    case 5: {
      for (size_t i = 0; i < elems; i++) {
        Uint64 val = 0;
        memcpy(&val, memory_ptr + (8 * i), 8);
        int5store(memory_ptr + (5 * i), (ulonglong)val);
      }
      return;
    }
    case 6: {
      for (size_t i = 0; i < elems; i++) {
        Uint64 val = 0;
        memcpy(&val, memory_ptr + (8 * i), 8);
        int6store(memory_ptr + (6 * i), (ulonglong)val);
      }
      return;
    }
    default: {
      require(false);
    }
  }
  return;
}

static int compare_8b(const void *left, const void *right) {
  ulonglong left_ulong = *(ulonglong*)left;
  ulonglong right_ulong = *(ulonglong*)right;
  if (left_ulong < right_ulong) return -1;
  if (left_ulong > right_ulong) return +1;
  return 0;
}

static int compare_6b(const void *left, const void *right) {
  const uchar *left_cmp = (const uchar*)left;
  const uchar *right_cmp = (const uchar*)right;
  ulonglong left_ulong = uint6korr(left_cmp);
  ulonglong right_ulong = uint6korr(right_cmp);
  if (left_ulong < right_ulong) return -1;
  if (left_ulong > right_ulong) return +1;
  return 0;
}

static int compare_5b(const void *left, const void *right) {
  const uchar *left_cmp = (const uchar*)left;
  const uchar *right_cmp = (const uchar*)right;
  ulonglong left_ulong = uint5korr(left_cmp);
  ulonglong right_ulong = uint5korr(right_cmp);
  if (left_ulong < right_ulong) return -1;
  if (left_ulong > right_ulong) return +1;
  return 0;
}

static int compare_4b(const void *left, const void *right) {
  const Uint32 left_cmp = *(const Uint32*)left;
  const Uint32 right_cmp = *(const Uint32*)right;
  if (left_cmp < right_cmp) return -1;
  if (left_cmp > right_cmp) return +1;
  return 0;
}

static int compare_3b(const void *left, const void *right) {
  const uchar *left_cmp = (const uchar*)left;
  const uchar *right_cmp = (const uchar*)right;
  uint32 left_uint32 = uint3korr(left_cmp);
  uint32 right_uint32 = uint3korr(right_cmp);
  if (left_uint32 < right_uint32) return -1;
  if (left_uint32 > right_uint32) return +1;
  return 0;
}

static int compare_2b(const void *left, const void *right) {
  const Uint16 left_cmp = *(const Uint16*)left;
  const Uint16 right_cmp = *(const Uint16*)right;
  if (left_cmp < right_cmp) return -1;
  if (left_cmp > right_cmp) return +1;
  return 0;
}

static int compare_1b(const void *left, const void *right) {
  const uchar left_cmp = *(const uchar*)left;
  const uchar right_cmp = *(const uchar*)right;
  if (left_cmp < right_cmp) return -1;
  if (left_cmp > right_cmp) return +1;
  return 0;
}

static void qsort_instr(const char *memory_ptr,
                        Uint32 elems,
                        Uint32 number_size) {
  switch (number_size) {
    case 1: {
      qsort((void*)(memory_ptr),
            elems,
            number_size,
            compare_1b);
      return;
    }
    case 2: {
      qsort((void*)(memory_ptr),
            elems,
            number_size,
            compare_2b);
      return;
    }
    case 3: {
      qsort((void*)(memory_ptr),
            elems,
            number_size,
            compare_3b);
      return;
    }
    case 4: {
      qsort((void*)(memory_ptr),
            elems,
            number_size,
            compare_4b);
      return;
    }
    case 5: {
      qsort((void*)(memory_ptr),
            elems,
            number_size,
            compare_5b);
      return;
    }
    case 6: {
      qsort((void*)(memory_ptr),
            elems,
            number_size,
            compare_6b);
      return;
    }
    case 8: {
      qsort((void*)(memory_ptr),
            elems,
            number_size,
             compare_8b);
      return;
    }
    default: {
      require(false);
      return;
    }
  }
  return;
}

/* For debugging */
static void dump_hex(const Uint32 *p, Uint32 len) {
  if (len > 2560) len = 160;
  if (len == 0) return;
  for (;;) {
    if (len >= 4)
      g_eventLogger->info("%8p %08X %08X %08X %08X", p, p[0], p[1], p[2], p[3]);
    else if (len >= 3)
      g_eventLogger->info("%8p %08X %08X %08X", p, p[0], p[1], p[2]);
    else if (len >= 2)
      g_eventLogger->info("%8p %08X %08X", p, p[0], p[1]);
    else
      g_eventLogger->info("%8p %08X", p, p[0]);
    if (len <= 4) break;
    len -= 4;
    p += 4;
  }
}

static inline
void
zero32(Uint8* dstPtr, const Uint32 len)
{
  Uint32 odd = len & 3;
  if (odd != 0)
  {
    Uint32 aligned = len & ~3;
    Uint8* dst = dstPtr+aligned;
    switch(odd){     /* odd is: {1..3} */
    case 1:
      dst[1] = 0;
      [[fallthrough]];
    case 2:
      dst[2] = 0;
      [[fallthrough]];
    default:         /* Known to be odd==3 */
      dst[3] = 0;
    }
  }
} 

Uint32 Dbtup::keyCopyAttrinfo(Uint32 expectedLen, Uint32 attrInfoIVal) {
  ndbassert(expectedLen > 0 || attrInfoIVal == RNIL);

  if (expectedLen > 0) {
    jamDebug();

    ndbassert(attrInfoIVal != RNIL);

    /* Check length in section is as we expect */
    SegmentedSectionPtr sectionPtr;
    getSection(sectionPtr, attrInfoIVal);

    ndbrequire(sectionPtr.sz == expectedLen);

    if (unlikely(sectionPtr.sz >= ZATTR_BUFFER_SIZE)) {
      jam();
      return ZTOO_MUCH_ATTRINFO_ERROR;
    }

    /* Copy attrInfo data into linear buffer */
    // TODO : Consider operating TUP out of first segment where
    // appropriate
    copy(cinBuffer, attrInfoIVal);
  }
  return 0;
}

Uint32 Dbtup::scanCopyAttrinfo(Uint32 storedProcId,
                               bool interpretedFlag,
                               bool first_call,
                               void* scan_rec)
{
  /* Get stored procedure */
  StoredProcPtr storedPtr;
  storedPtr.i = storedProcId;
  ndbrequire(c_storedProcPool.getValidPtr(storedPtr));

  const bool useCache = (storedPtr.p->copyAttrinfoCalled &&
                         storedPtr.p->cachedLinearAttrInfo != nullptr);
  Uint32 totalLen;
  /*
   * Fill cinBuffer with the full linearized section.
   * Either memcpy from cached linear buffer or read via SectionReader.
   */
  Uint32 paramAreaStart;
  if (useCache) {
    jamDebug();
    totalLen = storedPtr.p->cachedLinearLen;
    paramAreaStart = storedPtr.p->storedParamAreaStart;
    if (first_call) {
      memcpy(&cinBuffer[0],
             storedPtr.p->cachedLinearAttrInfo,
             totalLen * sizeof(Uint32));
    }
    if (unlikely(storedPtr.p->storedCode == ZCOPY_PROCEDURE)) {
      jamDebug();
      return totalLen;
    }
  } else {
    jamDebug();
    SectionReader reader(storedPtr.p->storedProcIVal,
                         getSectionSegmentPool());
    totalLen = reader.getSize();
    reader.getWords(&cinBuffer[0], totalLen);
    /* Cache the full linearized section on first call (scan procedures only) */
    jamDebug();
    ndbassert(storedPtr.p->storedCode == ZSCAN_PROCEDURE);
    if (unlikely(!cacheFromCinBuffer(storedPtr.p, totalLen))) {
      jam();
      return 0;
    }
    const Uint32 readLen = cinBuffer[0] + cinBuffer[1] +
      cinBuffer[2] + cinBuffer[3];
    paramAreaStart = 5 + readLen;
    storedPtr.p->storedParamAreaStart = paramAreaStart;
  }
  ndbrequire(storedPtr.p->storedCode == ZSCAN_PROCEDURE);

  /*
   * Setup cinBuffer for interpreted programs: select the right parameter
   * and initialize pushdown interpreter if present.
   */
  if (interpretedFlag) {
    jam();
    jamDataDebug(paramAreaStart);
    Uint32 paramLen = 0;
    if (cinBuffer[4] == 0) {
      jamDebug();
      // No parameters supplied in this attrInfo
    } else if (storedPtr.p->storedParamOffset == 0) {
      jamDebug();
      // First parameter — already in position
      paramLen = cinBuffer[4];
      ndbassert(paramAreaStart + paramLen <= totalLen);
    } else {
      jamDebug();
      // Jump directly to current parameter using cached offset
      const Uint32 paramOffset =
        paramAreaStart + storedPtr.p->storedParamOffset;
      ndbassert(paramOffset < totalLen);
      Uint32* paramBase = &cinBuffer[paramOffset];
      paramLen = *paramBase;
      ndbassert(paramOffset + paramLen <= totalLen);
      if (paramAreaStart + paramLen <= paramOffset) {
        memcpy(&cinBuffer[paramAreaStart], paramBase,
                paramLen * sizeof(Uint32));
      } else {
        memmove(&cinBuffer[paramAreaStart], paramBase,
                paramLen * sizeof(Uint32));
      }
      cinBuffer[4] = paramLen;
    }

    // Moz
    if (scan_rec != nullptr) {
      Dblqh::ScanRecord* scan_rec_ptr =
                        reinterpret_cast<Dblqh::ScanRecord*>(scan_rec);
      if (scan_rec_ptr->m_has_pushdown &&
          scan_rec_ptr->m_agg_interpreter == nullptr &&
          scan_rec_ptr->m_vs_interpreter == nullptr) {
        jam();
        Uint32 proc_start = paramAreaStart + paramLen;
        ndbrequire((cinBuffer[proc_start] >> 16) == 0x0721);
        Uint32 proc_len = cinBuffer[proc_start] & 0xFFFF;

        auto result = PushdownInterpreterFactory::Create(
            &cinBuffer[proc_start], proc_len,
            prepare_fragptr.p->fragTableId,
            prepare_fragptr.p->fragmentId,
            getThreadId());
        ndbrequire(result.agg != nullptr || result.vs != nullptr);
        scan_rec_ptr->m_agg_interpreter = result.agg;
        scan_rec_ptr->m_vs_interpreter = result.vs;
      }
    }
  } else {
    jamDebug();
    ndbassert(storedPtr.p->storedParamOffset == 0);
  }

  return totalLen;
}

void Dbtup::nextAttrInfoParam(Uint32 storedProcId) {
  jam();

  /* Get stored procedure */
  StoredProcPtr storedPtr;
  storedPtr.i = storedProcId;
  ndbrequire(c_storedProcPool.getValidPtr(storedPtr));
  ndbrequire(((storedPtr.p->storedCode == ZSCAN_PROCEDURE) ||
        (storedPtr.p->storedCode == ZCOPY_PROCEDURE)));

  /* Advance storedParamOffset past current param block.
   * Read block length from cached linear buffer, not cinBuffer,
   * since cinBuffer may have been overwritten by a real-time break.
   * Only advance when parameters exist (buf[4] > 0). For scans
   * without parameters, nextAttrInfoParam is still called (opExec
   * is set) but there is nothing to skip. */
  Uint32* buf = storedPtr.p->cachedLinearAttrInfo;
  ndbassert(buf != nullptr);
  if (buf[4] > 0) {
    jamDebug();
    const Uint32 paramAreaStart = storedPtr.p->storedParamAreaStart;
    ndbassert(paramAreaStart + storedPtr.p->storedParamOffset <
              storedPtr.p->cachedLinearLen);
    const Uint32 paramBlockLen =
      buf[paramAreaStart + storedPtr.p->storedParamOffset];
    storedPtr.p->storedParamOffset += paramBlockLen;
  }
}

bool Dbtup::cacheFromCinBuffer(storedProc* sp, Uint32 len) {
  sp->cachedLinearLen = len;
  sp->cachedLinearAttrInfo = static_cast<Uint32*>(
      lc_ndbd_pool_malloc(len * sizeof(Uint32),
                           RG_TRANSACTION_MEMORY,
                           getThreadId(),
                           false));
  if (sp->cachedLinearAttrInfo == nullptr) {
    jam();
    return false;
  }
  sp->copyAttrinfoCalled = true;
  memcpy(sp->cachedLinearAttrInfo,
         &cinBuffer[0],
         len * sizeof(Uint32));

  /* Release the segmented section — no longer needed */
  releaseSection(sp->storedProcIVal);
  sp->storedProcIVal = RNIL;
  return true;
}

void Dbtup::setInvalidChecksum(Tuple_header *tuple_ptr,
                               const Tablerec *regTabPtr) {
  if (regTabPtr->m_bits & Tablerec::TR_Checksum) {
    jam();
    /**
     * Set a magic checksum when tuple isn't supposed to be read.
     */
    tuple_ptr->m_checksum = 0x87654321;
  }
}

void Dbtup::updateChecksum(Tuple_header *tuple_ptr, const Tablerec *regTabPtr,
                           Uint32 old_header, Uint32 new_header) {
  /**
   * This function is used when only updating the header bits in row.
   * We start by XOR:ing the old header, this negates the impact of the
   * old header since old_header ^ old_header = 0. Next we XOR with new
   * header to get the new checksum and finally we store the new checksum.
   */
  if (regTabPtr->m_bits & Tablerec::TR_Checksum) {
    Uint32 checksum = tuple_ptr->m_checksum;
    jam();
    checksum ^= old_header;
    checksum ^= new_header;
    tuple_ptr->m_checksum = checksum;
  }
}

void Dbtup::setChecksum(Tuple_header *tuple_ptr, const Tablerec *regTabPtr) {
  if (regTabPtr->m_bits & Tablerec::TR_Checksum) {
    jamDebug();
    tuple_ptr->m_checksum = 0;
    tuple_ptr->m_checksum = calculateChecksum(tuple_ptr, regTabPtr);
  }
}

Uint32 Dbtup::calculateChecksum(Tuple_header *tuple_ptr,
                                const Tablerec *regTabPtr) {
  Uint32 checksum;
  Uint32 rec_size, *tuple_header;
  rec_size = regTabPtr->m_offsets[MM].m_fix_header_size;
  tuple_header = &tuple_ptr->m_header_bits;
  // includes tupVersion
  // printf("%p - ", tuple_ptr);

  /**
   * We include every except the first word of the Tuple header
   * which is only used on copy tuples. We do however include
   * the header bits.
   */
  checksum = computeXorChecksum(
      tuple_header, (rec_size-Tuple_header::HeaderSize) + 1);

  //printf("-> %.8x\n", checksum);

#if 0
  if (var_sized) {
    /*
       if (! req_struct->fix_var_together) {
       jam();
       checksum ^= tuple_header[rec_size];
       }
     */
    jam();
    var_data_part= req_struct->var_data_start;
    vsize_words= calculate_total_var_size(req_struct->var_len_array,
        regTabPtr->no_var_attr);
    ndbassert(req_struct->var_data_end >= &var_data_part[vsize_words]);
    checksum = computeXorChecksum(var_data_part,vsize_words,checksum);
  }
#endif
  return checksum;
}

int Dbtup::corruptedTupleDetected(KeyReqStruct *req_struct,
                                  Tablerec *regTabPtr) {
  Uint32 checksum = calculateChecksum(req_struct->m_tuple_ptr, regTabPtr);
  Uint32 header_bits = req_struct->m_tuple_ptr->m_header_bits;
  Uint32 tableId = req_struct->fragPtrP->fragTableId;
  Uint32 fragId = req_struct->fragPtrP->fragmentId;
  Uint32 page_id = req_struct->frag_page_id;
  Uint32 page_idx = prepare_page_idx;

  g_eventLogger->info(
      "Tuple corruption detected, checksum: 0x%x, header_bits: 0x%x"
      ", checksum word: 0x%x"
      ", tab(%u,%u), page(%u,%u)",
      checksum, header_bits, req_struct->m_tuple_ptr->m_checksum, tableId,
      fragId, page_id, page_idx);
  if (c_crashOnCorruptedTuple && !ERROR_INSERTED(4036)) {
    g_eventLogger->info(" Exiting.");
    ndbabort();
  }
  // Clear error 4036 caught in handleDeleteReq
  (void)ERROR_INSERTED_CLEAR(4036);
  terrorCode = ZTUPLE_CORRUPTED_ERROR;
  tupkeyErrorLab(req_struct);
  return -1;
}

/* ----------------------------------------------------------------- */
/* -----------       INSERT_ACTIVE_OP_LIST            -------------- */
/* ----------------------------------------------------------------- */
bool Dbtup::prepareActiveOpList(OperationrecPtr regOperPtr,
                                KeyReqStruct *req_struct) {
  /**
   * We are executing in the LDM thread since this is a write operation.
   * Thus we are protected from concurrent write activity from other
   * threads. We are however not protected against READ activities in the
   * query thread. Readers use the linked list of operations on the
   * row to find out which version of the row to use.
   *
   * We cannot publish our new row version until it is fully written,
   * thus it is ok to become the new leader of the write operations since
   * we are protected from other write row activity, but it is not ok to
   * change the linked list of operations on the row until we have completed
   * the write of the row.
   *
   * Therefore we divide insertActiveOpList into a prepareActiveOpList and
   * later call insertActiveOpList when the write is completed and we are
   * ready to insert ourselves into the linked list of operations on the
   * record.
   *
   * For initial inserts we place ourselves into the linked list immediately
   * since REFRESH operations are always performed with exclusive
   * access to the fragment and thus no interaction with query threads is
   * possible.
   */
  jam();
  OperationrecPtr prevOpPtr;
  ndbrequire(!regOperPtr.p->op_struct.bit_field.in_active_list);
  req_struct->prevOpPtr.i = prevOpPtr.i =
      req_struct->m_tuple_ptr->m_operation_ptr_i;
  regOperPtr.p->prevActiveOp = prevOpPtr.i;
  regOperPtr.p->m_undo_buffer_space = 0;
  ndbassert(!m_is_in_query_thread);
  if (likely(prevOpPtr.i == RNIL)) {
    return true;
  } else {
    jam();
    jamLineDebug(Uint16(prevOpPtr.i));
    ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(prevOpPtr));
    req_struct->prevOpPtr.p = prevOpPtr.p;

    regOperPtr.p->op_struct.bit_field.m_wait_log_buffer= 
      prevOpPtr.p->op_struct.bit_field.m_wait_log_buffer;
    regOperPtr.p->op_struct.bit_field.m_load_diskpage_on_commit= 
      prevOpPtr.p->op_struct.bit_field.m_load_diskpage_on_commit;
    regOperPtr.p->op_struct.bit_field.m_load_extra_diskpage_on_commit= 
      prevOpPtr.p->op_struct.bit_field.m_load_extra_diskpage_on_commit;
    regOperPtr.p->op_struct.bit_field.m_gci_written=
      prevOpPtr.p->op_struct.bit_field.m_gci_written;
    regOperPtr.p->op_struct.bit_field.m_tuple_existed_at_start=
      prevOpPtr.p->op_struct.bit_field.m_tuple_existed_at_start;
    regOperPtr.p->m_undo_buffer_space= prevOpPtr.p->m_undo_buffer_space;
    regOperPtr.p->m_uncommitted_used_space =
      prevOpPtr.p->m_uncommitted_used_space;
    // start with prev mask (matters only for UPD o UPD)

    regOperPtr.p->m_any_value = prevOpPtr.p->m_any_value;

    prevOpPtr.p->op_struct.bit_field.m_wait_log_buffer= 0;
    prevOpPtr.p->op_struct.bit_field.m_load_diskpage_on_commit= 0;
    prevOpPtr.p->op_struct.bit_field.m_load_extra_diskpage_on_commit= 0;

    if (prevOpPtr.p->tuple_state == TUPLE_PREPARED)
    {
      Uint32 op= regOperPtr.p->op_type;
      Uint32 prevOp= prevOpPtr.p->op_type;
      if (prevOp == ZDELETE)
      {
        if(op == ZINSERT)
        {
          // mark both
          prevOpPtr.p->op_struct.bit_field.delete_insert_flag= true;
          regOperPtr.p->op_struct.bit_field.delete_insert_flag= true;
          return true;
        }
        else if (op == ZREFRESH)
        {
          /* ZREFRESH after Delete - ok */
          return true;
        } else {
          terrorCode = ZTUPLE_DELETED_ERROR;
          return false;
        }
      } else if (op == ZINSERT && prevOp != ZDELETE) {
        terrorCode = ZINSERT_ERROR;
        return false;
      } else if (prevOp == ZREFRESH) {
        /* No operation after a ZREFRESH */
        terrorCode = ZOP_AFTER_REFRESH_ERROR;
        return false;
      }
      return true;
    } else {
      terrorCode = ZMUST_BE_ABORTED_ERROR;
      return false;
    }
  }
}

void
Dbtup::insertActiveOpList(OperationrecPtr regOperPtr,
                          KeyReqStruct* req_struct,
                          Tuple_header *tuple_ptr)
{
  /**
   * We have already prepared inserting ourselves into the list by
   * setting prevActiveOp to point to the previous leader.
   * We have not yet put ourselves last in the list, this is done
   * by updating the row operation pointer and by updating nextActiveOp
   * to point to us. We do this after performing the changes to ensure
   * that inserting us in the list happens after performing the changes
   * related to the operation.
   */
  jamDebug();
  jamDataDebug(regOperPtr.i);
  regOperPtr.p->op_struct.bit_field.in_active_list = true;
  tuple_ptr->m_operation_ptr_i = regOperPtr.i;
  if (unlikely(req_struct->prevOpPtr.i != RNIL)) {
    jam();
    req_struct->prevOpPtr.p->nextActiveOp = regOperPtr.i;
  }
}

bool
Dbtup::setup_read(KeyReqStruct *req_struct,
                  Operationrec* regOperPtr,
                  Tablerec* regTabPtr,
                  bool disk)
{
  OperationrecPtr currOpPtr;
  currOpPtr.i = req_struct->m_tuple_ptr->m_operation_ptr_i;
  const Uint32 bits = req_struct->m_tuple_ptr->m_header_bits;

  if (unlikely(req_struct->m_reorg != ScanFragReq::REORG_ALL)) {
    ndbassert(req_struct->m_reorg != ScanFragReq::REORG_MOVED_COPY);
    const Uint32 moved = bits & Tuple_header::REORG_MOVE;
    if (! ((req_struct->m_reorg == ScanFragReq::REORG_NOT_MOVED &&
            moved == 0) ||
          (req_struct->m_reorg == ScanFragReq::REORG_MOVED && moved != 0)))
    {
      /**
       * We're either scanning to only find moved rows (used when scanning
       * for rows to delete in reorg delete phase or we're scanning for
       * only non-moved rows and this happens also in reorg delete phase,
       * but it is done for normal scans in this phase.
       */
      jamDebug();
      terrorCode = ZTUPLE_DELETED_ERROR;
      return false;
    }
  }
  if (likely(currOpPtr.i == RNIL)) {
    jamDebug();
    if (regTabPtr->need_expand(disk)) {
      jamDebug();
      prepare_read(req_struct, regTabPtr, disk);
    }
    return true;
  }

  do {
    Uint32 savepointId = regOperPtr->savepointId;
    bool dirty = req_struct->dirty_op;
    Dblqh *ldm_lqh = nullptr;
    Dbtup *ldm_tup = this;

    /**
     * currOpPtr.i is an operation record in the LDM thread owning
     * the fragment. We could however be a query thread, we have
     * setup m_ldm_instance_used to always point to the owning
     * LDM threads block instance for DBLQH, DBTUP and DBACC.
     */
    currOpPtr.p = m_ldm_instance_used->getOperationPtrP(currOpPtr.i);
    ldm_lqh = c_lqh->m_ldm_instance_used;
    ldm_tup = m_ldm_instance_used;

    const bool sameTrans= ldm_lqh->is_same_trans(currOpPtr.p->userpointer,
        req_struct->trans_id1,
        req_struct->trans_id2);
    /**
     * Read committed in same trans reads latest copy
     */
    if (dirty && !sameTrans) {
      jamDebug();
      savepointId = 0;
    } else if (sameTrans) {
      // Use savepoint even in read committed mode
      jamDebug();
      dirty = false;
    }

    /* found == true indicates that savepoint is some state
     * within tuple's current transaction's uncommitted operations
     */
    const bool found = ldm_tup->find_savepoint(currOpPtr,
        savepointId,
        jamBuffer());

    const Uint32 currOp= currOpPtr.p->op_type;

    /* is_insert==true if tuple did not exist before its current
     * transaction
     */
    const bool is_insert = (bits & Tuple_header::ALLOC);

    /* If savepoint is in transaction, and post-delete-op
     *   OR
     * Tuple didn't exist before
     *      AND
     *   Read is dirty
     *           OR
     *   Savepoint is before-transaction
     *
     * Tuple does not exist in read's view
     */
    if((found && currOp == ZDELETE) || 
        ((dirty || !found) && is_insert))
    {
      /* Tuple not visible to this read operation */
      jamDebug();
      terrorCode = ZTUPLE_DELETED_ERROR;
      break;
    }

    if(dirty || !found)
    {
      /* Read existing committed tuple */
      jamDebug();
    } else {
      jamDebug();
      req_struct->m_tuple_ptr =
          get_copy_tuple(currOpPtr.p->m_copy_tuple_location);
    }

    if (regTabPtr->need_expand(disk)) {
      jamDebug();
      prepare_read(req_struct, regTabPtr, disk);
    }
    return true;
  } while(0);

  return false;
}

int
Dbtup::load_diskpage(Signal* signal,
                     Uint32 opRec,
                     Uint32 lkey1,
                     Uint32 lkey2,
                     Uint32 flags)
{
  Ptr<Operationrec> operPtr;

  operPtr.i = opRec;
  ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(operPtr));

  Operationrec *regOperPtr = operPtr.p;
  Fragrecord *regFragPtr = prepare_fragptr.p;
  Tablerec *regTabPtr = prepare_tabptr.p;

  if (Local_key::isInvalid(lkey1, lkey2)) {
    jam();
    regOperPtr->op_struct.bit_field.m_wait_log_buffer = 1;
    regOperPtr->op_struct.bit_field.m_load_diskpage_on_commit = 1;
    if (unlikely((flags & 7) == ZREFRESH)) {
      jam();
      /* Refresh of previously nonexistent DD tuple.
       * No diskpage to load at commit time
       */
      regOperPtr->op_struct.bit_field.m_wait_log_buffer = 0;
      regOperPtr->op_struct.bit_field.m_load_diskpage_on_commit = 0;
    }

    /* In either case return 1 for 'proceed' */
    return 1;
  }

  jam();
  ndbassert(Uint16(lkey2) == lkey2);
  Uint16 page_idx= Uint16(lkey2);
  Uint32 frag_page_id= lkey1;
  regOperPtr->m_tuple_location.m_page_no= getRealpid(regFragPtr,
      frag_page_id);
  regOperPtr->m_tuple_location.m_page_idx= page_idx;

  PagePtr page_ptr;
  Uint32 *tmp = get_ptr(&page_ptr, &regOperPtr->m_tuple_location, regTabPtr);
  Tuple_header *ptr = (Tuple_header *)tmp;

  if (((flags & 7) == ZREAD) &&
      ptr->m_header_bits & Tuple_header::DELETE_WAIT) {
    jam();
    /**
     * Tuple is already deleted and must not be read at this point in
     * time since when we come back from real-time break the row
     * will already be removed and invalidated.
     */
    return -(TUP_NO_TUPLE_FOUND);
  }
  if (ptr->m_operation_ptr_i != RNIL)
  {
    /**
     * There is a previous operation, we need to get the flag
     * m_load_extra_diskpage_on_commit from this operation
     * before proceeding with below code.
     *
     * This is handled in prepareActiveOpList, but this flag
     * is required to know whether to call load_extra_diskpage.
     */
    jam();
    OperationrecPtr prevOpPtr;
    prevOpPtr.i = ptr->m_operation_ptr_i;
    regOperPtr->prevActiveOp = prevOpPtr.i;
    ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(prevOpPtr));
    regOperPtr->op_struct.bit_field.m_load_extra_diskpage_on_commit =
      prevOpPtr.p->op_struct.bit_field.m_load_extra_diskpage_on_commit;
  }

  int res= 1;
  if (ptr->m_header_bits & Tuple_header::DISK_PART ||
      ptr->m_header_bits & Tuple_header::DISK_VAR_PART)
  {
    jam();
    /**
     * We retrieve the original disk row page when the transaction
     * started with an existing disk row (DISK_PART flag is set).
     * When we arrive here and DISK_PART isn't set, but DISK_VAR_PART
     * is set, this means that this is an operation that started with
     * an initial insert of a row. Any updates or re-inserts of this
     * row in the same transaction requires the page where the row is
     * allocated to be read before the operation is started. This is
     * necessary on variable sized disk rows since we need to check
     * if the row still fits on the page after performing this operation.
     */
    Page_cache_client::Request req;
    memcpy(&req.m_page, ptr->get_disk_ref_ptr(regTabPtr), sizeof(Local_key));
    req.m_table_id = regFragPtr->fragTableId;
    req.m_fragment_id = regFragPtr->fragmentId;
    req.m_callback.m_callbackData = opRec;
    req.m_callback.m_callbackFunction =
        safe_cast(&Dbtup::disk_page_load_callback);

    DEB_DISK(("(%u), load_diskpage, row(%u,%u), disk_row(%u,%u,%u)",
      instance(),
      frag_page_id,
      page_idx,
      req.m_page.m_file_no,
      req.m_page.m_page_no,
      req.m_page.m_page_idx));
#ifdef ERROR_INSERT
    if (ERROR_INSERTED(4022)) {
      flags |= Page_cache_client::DELAY_REQ;
      const NDB_TICKS now = NdbTick_getCurrentTicks();
      req.m_delay_until_time = NdbTick_AddMilliseconds(now, (Uint64)3000);
    }
    if (ERROR_INSERTED(4035) && (rand() % 13) == 0) {
      // Disk access have to randomly wait max 16ms for a diskpage
      Uint64 delay = (Uint64)(rand() % 16) + 1;
      flags |= Page_cache_client::DELAY_REQ;
      const NDB_TICKS now = NdbTick_getCurrentTicks();
      req.m_delay_until_time = NdbTick_AddMilliseconds(now, delay);
    }
#endif

    if (regOperPtr->op_struct.bit_field.m_load_extra_diskpage_on_commit)
    {
      /**
       * We will request 2 pages and need to ensure that the first page
       * isn't paged out while we are paging in the second page.
       */
      jamDebug();
      flags |= Page_cache_client::REF_REQ;
    }
    // Save the current flags
    regOperPtr->get_disk_page_flags = flags;
    Page_cache_client pgman(this, c_pgman);
    res = pgman.get_page(signal, req, flags);
  }

  switch(flags & 7)
  {
    case ZREAD:
    case ZREAD_EX:
      break;
    case ZDELETE:
    case ZUPDATE:
    case ZINSERT:
    case ZWRITE:
    case ZREFRESH:
      jam();
      regOperPtr->op_struct.bit_field.m_wait_log_buffer= 1;
      regOperPtr->op_struct.bit_field.m_load_diskpage_on_commit= 1;
  }
  if (res > 0)
  {
    jam();
    regOperPtr->m_disk_callback_page = res;
    regOperPtr->m_disk_extra_callback_page = RNIL;
    if (regOperPtr->op_struct.bit_field.m_load_extra_diskpage_on_commit)
    {
      jam();
      // No need to set BUSY state on the extra disk page
      flags &= ~Page_cache_client::REF_REQ;
      res = load_extra_diskpage(signal, opRec, flags);
    }
  }
  return res;
}

int
Dbtup::load_extra_diskpage(Signal *signal, Uint32 opRec, Uint32 flags)
{
  Fragrecord * regFragPtr = prepare_fragptr.p;
  Tablerec* regTabPtr = prepare_tabptr.p;
  Ptr<Operationrec> operPtr;
  operPtr.i = opRec;
  ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(operPtr));
  OperationrecPtr prevOpPtr;
  prevOpPtr.i = operPtr.p->prevActiveOp;
  ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(prevOpPtr));

  PagePtr page_ptr;
  ndbassert(prevOpPtr.p->m_copy_tuple_location != nullptr);
  Tuple_header *ptr = get_copy_tuple(prevOpPtr.p->m_copy_tuple_location);
  jamEntry();
  /**
   * We will never need an extra disk page if the first operation was an
   * INSERT operation. This means that DISK_PART must be set on the row.
   */
  ndbrequire(ptr->m_header_bits & Tuple_header::DISK_PART);
  Page_cache_client::Request req;
  memcpy(&req.m_page,
      ptr->get_disk_ref_ptr(regTabPtr),
      sizeof(Local_key));
  req.m_table_id = regFragPtr->fragTableId;
  req.m_fragment_id = regFragPtr->fragmentId;
  req.m_callback.m_callbackData= opRec;
  req.m_callback.m_callbackFunction= 
    safe_cast(&Dbtup::disk_page_load_extra_callback);

#ifdef DEBUG_DISK
  PagePtr deb_page_ptr;
  Uint32 *deb_tmp = get_ptr(&deb_page_ptr,
                            &operPtr.p->m_tuple_location,
                            regTabPtr);
  (void)deb_tmp;
  DEB_DISK(("(%u), load_diskpage, row(%u,%u), disk_row(%u,%u,%u)",
    instance(),
    ((Tup_varsize_page*)deb_page_ptr.p)->frag_page_id,
    operPtr.p->m_tuple_location.m_page_idx,
    req.m_page.m_file_no,
    req.m_page.m_page_no,
    req.m_page.m_page_idx));
#endif
  Page_cache_client pgman(this, c_pgman);
  int res = pgman.get_page(signal, req, flags);
  if (res != 0)
  {
    jam();
    if (res > 0)
      operPtr.p->m_disk_extra_callback_page = Uint32(res);
    deref_disk_page(signal,
      operPtr,
      regFragPtr,
      regTabPtr);
  }
  return res;
}

void
Dbtup::deref_disk_page(Signal *signal,
                       OperationrecPtr operPtr,
                       Fragrecord *regFragPtr,
                       Tablerec *regTabPtr)
{
  PagePtr page_ptr;
  Tuple_header* ptr;
  jamDebug();
  Uint32* tmp= get_ptr(&page_ptr, &operPtr.p->m_tuple_location, regTabPtr);
  ptr = (Tuple_header*)tmp;
  Page_cache_client::Request req;
  memcpy(&req.m_page, ptr->get_disk_ref_ptr(regTabPtr), sizeof(Local_key));
  req.m_table_id = regFragPtr->fragTableId;
  req.m_fragment_id = regFragPtr->fragmentId;
  req.m_callback.m_callbackData = operPtr.i;
  req.m_callback.m_callbackFunction= 
    safe_cast(&Dbtup::deref_disk_page_callback);
  Page_cache_client pgman(this, c_pgman);
  Uint32 flags = Page_cache_client::DEREF_REQ;
  int res = pgman.get_page(signal, req, flags);
  ndbrequire(res > 0);
}

void
Dbtup::deref_disk_page_callback(Signal *signal, Uint32 opRec, Uint32 page_id)
{
  (void)signal;
  (void)opRec;
  (void)page_id;
  ndbabort();
}

void
Dbtup::disk_page_load_callback(Signal* signal, Uint32 opRec, Uint32 page_id)
{
  Ptr<Operationrec> operPtr;
  jam();
  operPtr.i = opRec;
  jamData(opRec);
  ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(operPtr));
#ifdef DEBUG_DISK
  Ptr<GlobalPage> diskPagePtr;
  diskPagePtr.i = page_id;
  ndbrequire(m_global_page_pool.getPtr(diskPagePtr, page_id));
  g_eventLogger->info("(%u) disk_page_load_callback, disk_row(%u,%u)",
    instance(),
    ((Tup_varsize_page*)diskPagePtr.p)->m_file_no,
    ((Tup_varsize_page*)diskPagePtr.p)->m_page_no);
#endif
  if (operPtr.p->op_struct.bit_field.m_load_extra_diskpage_on_commit)
  {
    jam();
    c_lqh->setup_key_pointers(operPtr.p->userpointer);
    // Recover the saved flags
    Uint32 flags = operPtr.p->get_disk_page_flags;
    // No need to set BUSY state on the extra disk page
    flags &= ~Page_cache_client::REF_REQ;
    // Reset the saved flags
    operPtr.p->get_disk_page_flags = RNIL;
    int extra_page_id = load_extra_diskpage(signal, opRec, flags);
    if (extra_page_id == 0)
    {
      /* Save the disk callback page during real-time break. */
      operPtr.p->m_disk_callback_page = page_id;
      return;
    } else if (extra_page_id < 0) {
      /* Error happened when loading extra disk page */
      c_lqh->acckeyconf_load_diskpage_callback(signal, 
        operPtr.p->userpointer,
        0);
      return;
    }
  }
  else
  {
    /**
     * The m_disk_callback_page will be overwritten, thus we pass it
     * to DBLQH so that DBLQH can set it up.
     */
    ;
    jam();
    c_lqh->setup_key_pointers(operPtr.p->userpointer);
    operPtr.p->m_disk_callback_page = page_id;
  }
  c_lqh->acckeyconf_load_diskpage_callback(signal, 
      operPtr.p->userpointer,
      page_id);
}

void
Dbtup::disk_page_load_extra_callback(Signal* signal,
                                     Uint32 opRec,
                                     Uint32 extra_page_id)
{
  Ptr<Operationrec> operPtr;
  operPtr.i = opRec;
  jam();
  jamData(opRec);
  ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(operPtr));
#ifdef DEBUG_DISK
  Ptr<GlobalPage> diskPagePtr;
  diskPagePtr.i = extra_page_id;
  ndbrequire(m_global_page_pool.getPtr(diskPagePtr, extra_page_id));
  g_eventLogger->info("(%u) disk_page_load_extra_callback, disk_row(%u,%u)",
    instance(),
    ((Tup_varsize_page*)diskPagePtr.p)->m_file_no,
    ((Tup_varsize_page*)diskPagePtr.p)->m_page_no);
#endif
  operPtr.p->m_disk_extra_callback_page = extra_page_id;
  Uint32 page_id = operPtr.p->m_disk_callback_page;
  c_lqh->setup_key_pointers(operPtr.p->userpointer);
  operPtr.p->m_disk_callback_page = page_id;
  Fragrecord * regFragPtr = prepare_fragptr.p;
  Tablerec* regTabPtr = prepare_tabptr.p;
  deref_disk_page(signal,
      operPtr,
      regFragPtr,
      regTabPtr);
  c_lqh->acckeyconf_load_diskpage_callback(signal, 
      operPtr.p->userpointer,
      operPtr.p->m_disk_callback_page);
}

int
Dbtup::load_diskpage_scan(Signal* signal,
                          Uint32 opRec,
                          Uint32 lkey1,
                          Uint32 lkey2,
                          Uint32 tux_flag,
                          Uint32 disk_flag)
{
  Ptr<Operationrec> operPtr;
  operPtr.i = opRec;
  ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(operPtr));

  Operationrec *regOperPtr = operPtr.p;
  Fragrecord *regFragPtr = prepare_fragptr.p;
  Tablerec *regTabPtr = prepare_tabptr.p;

  jam();
  Uint32 page_idx = lkey2;
  if (likely(tux_flag)) {
    jamDebug();
    regOperPtr->m_tuple_location.m_page_no = lkey1;
  } else {
    jamDebug();
    Uint32 frag_page_id= lkey1;
    regOperPtr->m_tuple_location.m_page_no= getRealpid(regFragPtr,
        frag_page_id);
  }
  regOperPtr->m_tuple_location.m_page_idx= page_idx;
  regOperPtr->op_struct.bit_field.m_load_diskpage_on_commit= 0;

  PagePtr page_ptr;
  Uint32 *tmp = get_ptr(&page_ptr, &regOperPtr->m_tuple_location, regTabPtr);
  Tuple_header *ptr = (Tuple_header *)tmp;

  if (ptr->m_header_bits & Tuple_header::DELETE_WAIT) {
    jam();
    /**
     * Tuple is already deleted and must not be read at this point in
     * time since when we come back from real-time break the row
     * will already be removed and invalidated.
     */
    return -(TUP_NO_TUPLE_FOUND);
  }

  int res= 1;
  if (ptr->m_header_bits & Tuple_header::DISK_PART)
  {
    jam();
    Page_cache_client::Request req;
    memcpy(&req.m_page, ptr->get_disk_ref_ptr(regTabPtr), sizeof(Local_key));
    req.m_table_id = regFragPtr->fragTableId;
    req.m_fragment_id = regFragPtr->fragmentId;
    req.m_callback.m_callbackData= opRec;
    req.m_callback.m_callbackFunction= 
      safe_cast(&Dbtup::disk_page_load_scan_callback);

    Page_cache_client pgman(this, c_pgman);
    res= pgman.get_page(signal, req, disk_flag);
    if (res > 0)
    {
      regOperPtr->m_disk_callback_page = res;
    }
  }
  else
  {
    jam();
    /**
     * We need to set m_disk_callback_page to something different
     * than RNIL to indicate that we should be ready to read the
     * disk columns. At the same time there is no disk page, so
     * we set it to something that should crash if attempted to
     * be used as a page id.
     */
    regOperPtr->m_disk_callback_page = Uint32(~0);
  }
  regOperPtr->m_disk_extra_callback_page = RNIL;
  return res;
}

void
Dbtup::disk_page_load_scan_callback(Signal* signal, 
                                    Uint32 opRec,
                                    Uint32 page_id)
{
  Ptr<Operationrec> operPtr;
  operPtr.i = opRec;
  jam();
  jamData(opRec);
  ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(operPtr));
  c_lqh->next_scanconf_load_diskpage_callback(signal, 
      operPtr.p->userpointer,
      page_id);
}

/**
  This method is used to prepare for faster execution of TUPKEYREQ.
  It prepares the pointers to the fragment record, the table record,
  the page for the record and the tuple pointer to the record. In
  addition it also prefetches the cache lines of the fixed size part
  of the tuple.

  The calculations performed here have to be done when we arrive in
  execTUPKEYREQ, we perform them here to enable prefetching the
  cache lines of the fixed part of the tuple storage. In order to not
  do the same work twice we store the calculated information in
  block variables. Given that we can arrive in execTUPKEYREQ from
  multiple directions, we have added debug-code that verifies that we
  have passed through prepareTUPKEYREQ always before we reach
  execTUPKEYREQ.

  The access of the fixed size part of the tuple is an almost certain
  CPU cache miss and so performing this as early as possible will
  decrease the time for cache misses later in the process. Tests using
  Sysbench indicates that this prefetch gains about 5% in performance.

  See DblqhMain.cpp for more documentation of prepare_* methods.
 */

void Dbtup::prepare_tab_pointers_acc(Uint32 table_id, Uint32 frag_id) {
  TablerecPtr tablePtr;
  tablePtr.i = table_id;
  ptrCheckGuard(tablePtr, cnoOfTablerec, tablerec);
  FragrecordPtr fragPtr;
  getFragmentrec(fragPtr, frag_id, tablePtr.i);
  ndbrequire(fragPtr.i != RNIL64);
  prepare_fragptr = fragPtr;
  prepare_tabptr = tablePtr;
}

void Dbtup::prepareTUPKEYREQ(Uint32 page_id,
    Uint32 page_idx,
    Uint64 fragPtrI)
{
  FragrecordPtr fragptr;
  TablerecPtr tabptr;

  fragptr.i = fragPtrI;
  ndbrequire(c_fragment_pool.getPtr(fragptr));
  const Uint32 RnoOfTablerec= cnoOfTablerec;
  Tablerec * Rtablerec = tablerec;

  jamEntryDebug();
  tabptr.i = fragptr.p->fragTableId;
  ptrCheckGuard(tabptr, RnoOfTablerec, Rtablerec);
  prepare_tabptr = tabptr;
  prepare_fragptr = fragptr;
  prepare_scanTUPKEYREQ(page_id, page_idx);
}

void Dbtup::prepare_scanTUPKEYREQ(Uint32 page_id, Uint32 page_idx) {
  Local_key key;
  PagePtr pagePtr;
#ifdef VM_TRACE
  prepare_orig_local_key.m_page_no = page_id;
  prepare_orig_local_key.m_page_idx = page_idx;
#endif
  bool is_page_key = (!(Local_key::isInvalid(page_id, page_idx)));
  if (is_page_key) {
    Uint32 fixed_part_size_in_words =
      prepare_tabptr.p->m_offsets[MM].m_fix_header_size;
    acquire_frag_page_map_mutex_read(prepare_fragptr.p, jamBuffer());
    page_id = getRealpid(prepare_fragptr.p, page_id);
    release_frag_page_map_mutex_read(prepare_fragptr.p, jamBuffer());
    key.m_page_no = page_id;
    key.m_page_idx = page_idx;
    Uint32 *tuple_ptr = get_ptr(&pagePtr,
        &key,
        prepare_tabptr.p);
    jamDebug();
    prepare_pageptr = pagePtr;
    prepare_page_idx = page_idx;
    prepare_tuple_ptr = tuple_ptr;
    prepare_page_no = page_id;
    for (Uint32 i = 0; i < fixed_part_size_in_words; i += 16) {
      NDB_PREFETCH_WRITE(tuple_ptr + i);
    }
  }
}

void Dbtup::prepare_scan_tux_TUPKEYREQ(Uint32 page_id, Uint32 page_idx) {
  Local_key key;
  PagePtr pagePtr;
#ifdef VM_TRACE
  prepare_orig_local_key.m_page_no = page_id;
  prepare_orig_local_key.m_page_idx = page_idx;
#endif
  bool is_page_key = (!(Local_key::isInvalid(page_id, page_idx)));
  ndbrequire(is_page_key);
  {
    Uint32 fixed_part_size_in_words =
        prepare_tabptr.p->m_offsets[MM].m_fix_header_size;
    key.m_page_no = page_id;
    key.m_page_idx = page_idx;
    Uint32 *tuple_ptr = get_ptr(&pagePtr,
        &key,
        prepare_tabptr.p);
    jamDebug();
    prepare_pageptr = pagePtr;
    prepare_tuple_ptr = tuple_ptr;
    prepare_page_no = page_id;
    for (Uint32 i = 0; i < fixed_part_size_in_words; i += 16) {
      NDB_PREFETCH_WRITE(tuple_ptr + i);
    }
  }
}

bool Dbtup::execTUPKEYREQ(Signal* signal,
                          void *_lqhOpPtrP,
                          void *_lqhScanPtrP)
{
  Dblqh::TcConnectionrec *lqhOpPtrP = (Dblqh::TcConnectionrec*)_lqhOpPtrP;
  Dblqh::ScanRecord *lqhScanPtrP = (Dblqh::ScanRecord*)_lqhScanPtrP;

  TupKeyReq * tupKeyReq= (TupKeyReq *)signal->getDataPtr();
  Ptr<Operationrec> operPtr = prepare_oper_ptr;
  KeyReqStruct req_struct(this);

  jamEntryDebug();
  jamLineDebug(Uint16(prepare_oper_ptr.i));
  req_struct.m_lqh = c_lqh;

#ifdef VM_TRACE
  {
    bool error_found = false;
    Local_key key;
    key.m_page_no = tupKeyReq->keyRef1;
    key.m_page_idx = tupKeyReq->keyRef2;
    if (key.m_page_no != prepare_orig_local_key.m_page_no) {
      ndbout << "page_no = " << prepare_orig_local_key.m_page_no;
      ndbout << " keyRef1 = " << key.m_page_no << endl;
      error_found = true;
    }
    if (key.m_page_idx != prepare_orig_local_key.m_page_idx) {
      ndbout << "page_idx = " << prepare_orig_local_key.m_page_idx;
      ndbout << " keyRef2 = " << key.m_page_idx << endl;
      error_found = true;
    }
    if (error_found) {
      ndbout << flush;
    }
    ndbassert(prepare_orig_local_key.m_page_no == key.m_page_no);
    ndbassert(prepare_orig_local_key.m_page_idx == key.m_page_idx);
    FragrecordPtr fragPtr = prepare_fragptr;
    ndbrequire(c_fragment_pool.getPtr(fragPtr));
    ndbassert(prepare_fragptr.p == fragPtr.p);
  }
#endif

  /**
   * DESIGN PATTERN DESCRIPTION
   * --------------------------
   * The variable operPtr.p is located on the block object, it is located
   * there to ensure that we can easily access it in many methods such
   * that we don't have to transport it through method calls. There are
   * a number of references to structs that we store in this manner.
   * Oftentimes they refer to the operation object, the table object,
   * the fragment object and sometimes also a transaction object.
   *
   * Given that we both need access to the .i-value and the .p-value
   * of all of those objects we store them on the block object to
   * avoid the need of transporting them from function to function.
   * This is an optimisation and obviously requires that one keeps
   * track of which variables are alive and which are not.
   * The function clear_global_variables used in debug mode ensures
   * that all pointer variables are cleared before an asynchronous
   * signal is executed.
   *
   * When we need to access data through the .p-value many times
   * (more than one time), then it often pays off to declare a
   * stack variable such as below regOperPtr. This helps the compiler
   * to avoid having to constantly reload the .p-value from the
   * block object after each store operation through a pointer.
   *
   * One has to take care though when doing this to ensure that
   * one doesn't create a stack variable that creates too much
   * pressure on the register allocation in the method. This is
   * particularly important in large methods.
   *
   * The pattern is to define the variable as:
   * Operationrec * const regOperPtr = operPtr.p;
   * This helps the compiler to understand that we won't change the
   * pointer here.
   */
  Operationrec *const regOperPtr = operPtr.p;

  Dbtup::TransState trans_state = get_trans_state(regOperPtr);

  req_struct.signal = signal;
  req_struct.operPtrP = regOperPtr;
  regOperPtr->fragmentPtr = prepare_fragptr.i;
  regOperPtr->prevActiveOp = RNIL;
  regOperPtr->nextActiveOp = RNIL;
  req_struct.num_fired_triggers = 0;
  req_struct.no_exec_instructions = 0;
  req_struct.read_length = 0;
  req_struct.last_row = false;
  req_struct.m_is_lcp = false;
  // MOZ Aggregation batch
  req_struct.agg_curr_batch_size_rows = 0;
  req_struct.agg_curr_batch_size_bytes = 0;
  req_struct.agg_n_res_recs = 0;

  req_struct.ttl_purge_window_size = 0;

  if (unlikely(trans_state != TRANS_IDLE)) {
    TUPKEY_abort(&req_struct, 39);
    return false;
  }

  /* ----------------------------------------------------------------- */
  // Operation is ZREAD when we arrive here so no need to worry about the
  // abort process.
  /* ----------------------------------------------------------------- */
  /* -----------    INITIATE THE OPERATION RECORD       -------------- */
  /* ----------------------------------------------------------------- */
  Uint32 disable_fk_checks = 0;
  Uint32 deferred_constraints = 0;
  Uint32 flags = lqhOpPtrP->m_flags;
  if (lqhScanPtrP != nullptr) {
    Uint32 attrBufLen = lqhScanPtrP->scanAiLength;
    Uint32 dirtyOp = (lqhScanPtrP->scanLockHold == ZFALSE);
    Uint32 prioAFlag = lqhScanPtrP->prioAFlag;
    Uint32 opRef = lqhScanPtrP->scanApiOpPtr[lqhScanPtrP->scanApiOpPtr_index];
    Uint32 applRef = lqhScanPtrP->scanApiBlockref;
    Uint32 interpreted_exec = lqhOpPtrP->opExec;
    Uint32 interpreted_insert = lqhOpPtrP->m_interpreted_insert;

    req_struct.log_size = attrBufLen;
    req_struct.attrinfo_len = attrBufLen;
    req_struct.dirty_op = dirtyOp;
    req_struct.m_prio_a_flag = prioAFlag;
    req_struct.tc_operation_ptr = opRef;
    req_struct.rec_blockref = applRef;
    req_struct.interpreted_exec = interpreted_exec;
    req_struct.interpreted_insert = interpreted_insert;
    req_struct.m_nr_copy_or_redo = 0;
    req_struct.m_use_rowid = 0;
#ifdef ERROR_INSERT
    /* Insert garbage into rowid, should not be used */
    req_struct.m_row_id.m_page_no = RNIL;
    req_struct.m_row_id.m_page_idx = ZNIL;
#endif
    req_struct.scan_rec = lqhScanPtrP;
    req_struct.m_join_agg_state_key = lqhScanPtrP->m_join_agg_state_key;
    /*
     * TTL related
     */
    regOperPtr->ttl_ignore = lqhScanPtrP->m_ttl_ignore;
    if (lqhScanPtrP->m_ttl_ignore == 1 ||
        lqhScanPtrP->m_ttl_ignore_for_ral == 1) {
      regOperPtr->ttl_ignore = 1;
    } else {
      regOperPtr->ttl_ignore = 0;
    }
    regOperPtr->ttl_only_expired = lqhScanPtrP->m_ttl_only_expired;

    TTL_RONDB_TRACE(prepare_fragptr.p->fragTableId,
                    "Dbtup::execTUPKEYREQ(), Ignore TTL[%u, %u]: %u, "
                    "only expired: %u",
                    lqhScanPtrP->m_ttl_ignore,
                    lqhScanPtrP->m_ttl_ignore_for_ral,
                    regOperPtr->ttl_ignore,
                    regOperPtr->ttl_only_expired);
    /*
     * TTL related
     * TODO (Zhao)
     * double check here
     */
    if (lqhScanPtrP->m_ttl_ignore == 0 && lqhOpPtrP->ttl_ignore) {
      TTL_RONDB_TRACE(prepare_fragptr.p->fragTableId,
                      "Dbtup::execTUPKEYREQ(), Ignore TTL "
                      "for one operation in a normal scan");
      ndbrequire(false);
      regOperPtr->ttl_ignore = lqhOpPtrP->ttl_ignore;
    }
    req_struct.ttl_purge_window_size = lqhScanPtrP->m_ttl_purge_window_size;
  } else {
    Uint32 attrBufLen = lqhOpPtrP->totReclenAi;
    Uint32 dirtyOp = lqhOpPtrP->dirtyOp;
    Uint32 row_id = TupKeyReq::getRowidFlag(tupKeyReq->request);
    Uint32 interpreted_exec = TupKeyReq::getInterpretedFlag(tupKeyReq->request);
    Uint32 interpreted_insert =
      TupKeyReq::getInterpretedInsertFlag(tupKeyReq->request);
    Uint32 opRef = lqhOpPtrP->applOprec;
    Uint32 applRef = lqhOpPtrP->applRef;

    req_struct.dirty_op = dirtyOp;
    req_struct.m_use_rowid = row_id;
    req_struct.log_size = attrBufLen;
    req_struct.attrinfo_len = attrBufLen;
    req_struct.tc_operation_ptr = opRef;
    req_struct.rec_blockref = applRef;
    req_struct.interpreted_exec = interpreted_exec;
    req_struct.interpreted_insert = interpreted_insert;

    req_struct.m_prio_a_flag = 0;
    req_struct.m_nr_copy_or_redo =
        ((LqhKeyReq::getNrCopyFlag(lqhOpPtrP->reqinfo) |
          c_lqh->c_executing_redo_log) != 0);
    disable_fk_checks = ((flags & Dblqh::TcConnectionrec::OP_DISABLE_FK) != 0);
    deferred_constraints =
        ((flags & Dblqh::TcConnectionrec::OP_DEFERRED_CONSTRAINTS) != 0);
    const Uint32 row_id_page_no = tupKeyReq->m_row_id_page_no;
    const Uint32 row_id_page_idx = tupKeyReq->m_row_id_page_idx;
    req_struct.m_row_id.m_page_no = row_id_page_no;
    req_struct.m_row_id.m_page_idx = row_id_page_idx;
    req_struct.scan_rec = nullptr;
    req_struct.m_join_agg_state_key = lqhOpPtrP->m_join_agg_state_key;
    regOperPtr->ttl_ignore = lqhOpPtrP->ttl_ignore;
    regOperPtr->ttl_only_expired = lqhOpPtrP->ttl_only_expired;
#ifdef TTL_DEBUG
    if (regOperPtr->ttl_ignore) {
      TTL_RONDB_TRACE(prepare_fragptr.p->fragTableId,
                      "Dbtup::execTUPKEYREQ(), Ignore TTL "
                      "for one operation");
    }
#endif  // TTL_DEBUG
  }
  if (req_struct.ttl_purge_window_size != 0 && !regOperPtr->ttl_only_expired) {
    g_eventLogger->warning("reset ttl_purge_window_size from %u to 0 "
                        "since ttl_only_expired is not set",
                        req_struct.ttl_purge_window_size);
    req_struct.ttl_purge_window_size = 0;
  }
  req_struct.m_deferred_constraints = deferred_constraints;
  req_struct.m_disable_fk_checks = disable_fk_checks;
  {
    Operationrec::OpStruct op_struct;
    op_struct.op_bit_fields = regOperPtr->op_struct.op_bit_fields;
    op_struct.bit_field.m_disable_fk_checks = disable_fk_checks;
    op_struct.bit_field.m_deferred_constraints = deferred_constraints;

    const Uint32 triggers = (flags & Dblqh::TcConnectionrec::OP_NO_TRIGGERS)
                                ? TupKeyReq::OP_NO_TRIGGERS
                            : (lqhOpPtrP->seqNoReplica == 0)
                                ? TupKeyReq::OP_PRIMARY_REPLICA
                                : TupKeyReq::OP_BACKUP_REPLICA;
    op_struct.bit_field.delete_insert_flag = false;
    op_struct.bit_field.m_gci_written = 0;
    op_struct.bit_field.m_reorg = lqhOpPtrP->m_reorg;
    op_struct.bit_field.tupVersion = ZNIL;
    op_struct.bit_field.m_triggers = triggers;

    regOperPtr->m_copy_tuple_location = nullptr;
    regOperPtr->op_struct.op_bit_fields = op_struct.op_bit_fields;
    regOperPtr->m_refresh_case = 0;
  }
  {
    Uint32 reorg = lqhOpPtrP->m_reorg;
    Uint32 op = lqhOpPtrP->operation;
    Uint32 original_op = lqhOpPtrP->original_operation;

    req_struct.m_reorg = reorg;
    regOperPtr->op_type = op;
    /*
     * TTL related
     * We keep original operation type for regOperPtr,
     * so that we can handle HandleUpdateReq() correctly
     * in the future
     */
    regOperPtr->original_op_type = original_op;
    TTL_RONDB_TRACE(prepare_fragptr.p->fragTableId, "[TableId: %u]"
                    "Set Dbtup::Operationrec::original_op_type: %u, "
                    "current Dbtup::Operationrec::op_type: %u, "
                    "ignore TTL ?(%u), "
                    "only expired?(%u)",
                    prepare_fragptr.p->fragTableId,
                    regOperPtr->original_op_type,
                    regOperPtr->op_type,
                    regOperPtr->ttl_ignore,
                    regOperPtr->ttl_only_expired);
  }
  {
    /**
     * DESIGN PATTERN DESCRIPTION
     * --------------------------
     * This code segment is using a common design pattern in the
     * signal reception and signal sending code of performance
     * critical functions such as execTUPKEYREQ.
     * The idea is that at signal reception we need to transfer
     * data from the signal object to state variables relating to
     * the operation we are about to execute.
     * The normal manner to do this would be to write:
     * regOperPtr->savePointId = tupKeyReq->savePointId;
     *
     * This normal manner would however not work so well due to
     * that the compiler has to issue assembler code that does
     * a load operation immediately followed by a store operation.
     * Many modern CPUs can hide parts of this deficiency in the
     * code, but only to a certain extent.
     *
     * What we want to do here is instead to perform a series of
     * six loads followed by six stores. The delay after a load
     * is ready for a store operation is oftentimes 3 cycles. Many
     * CPUs can handle two loads per cycle. So by using 6 loads
     * we ensure that we execute at full speed as long as the data
     * is available in the first level CPU cache.
     *
     * The reason we don't want to use more than 6 loads before
     * we start storing is that CPUs have a limited amount of
     * CPU registers. The x86 have 16 CPU registers available.
     * Here is a short description of commonly used registers:
     * RIP: Instruction pointer, not available
     * RSP: Top of Stack pointer, not available for L/S
     * RBP: Current Stack frame pointer, not available for L/S
     * RDI: Usually this-pointer, reference to Dbtup object here
     *
     * In this particular example we also need to have a register
     * for storing:
     * tupKeyReq, req_struct, regOperPtr.
     *
     * The compiler also needs a few more registers to track some
     * of the other live variables such that not all of the live
     * variables have to be spilled to the stack.
     *
     * Thus the design pattern uses between 4 to 6 variables loaded
     * before storing them. Another commonly used manner is to locate
     * all initialisations to constants in one or more of those
     * initialisation code blocks as well.
     *
     * The naming pattern is to define the temporary variable as
     * const Uint32 name_of_variable_to_assign = x->name;
     * y->name_of_variable_to_assign = name_of_variable_to_assign.
     *
     * In the case where the receiver of the data is a signal object
     * we use the pattern:
     * const Uint32 sig0 = x->name;
     * signal->theData[0] = sig0;
     *
     * Finally if possible we should place this initialisation in a
     * separate code block by surrounding it with brackets, this is
     * to assist the compiler to understand that the variables used
     * are not needed after storing its value. Most compilers will
     * handle this well anyways, but it helps the compiler avoid
     * doing mistakes and it also clarifies for the reader of the
     * source code. As can be seen in code below this rule is
     * however not followed if it will remove other possibilities.
     */
    const Uint32 savePointId = lqhOpPtrP->savePointId;
    const Uint32 tcOpIndex = lqhOpPtrP->tcOprec;
    const Uint32 coordinatorTC = lqhOpPtrP->tcBlockref;

    regOperPtr->savepointId = savePointId;
    req_struct.TC_index = tcOpIndex;
    req_struct.TC_ref = coordinatorTC;
  }

  const Uint32 disk_page = regOperPtr->m_disk_callback_page;
  const Uint32 keyRef1 = tupKeyReq->keyRef1;
  const Uint32 keyRef2 = tupKeyReq->keyRef2;

  req_struct.m_disk_page_ptr.i = disk_page;
  /**
   * The pageid here is a page id of a row id except when we are
   * reading from an ordered index scan, in this case it is a
   * physical page id. We will only use this variable for LCP
   * scan reads and for inserts and refreshs. So it is not used
   * for TUX scans.
   */
  Uint32 pageid = regOperPtr->fragPageId = req_struct.frag_page_id = keyRef1;
  Uint32 pageidx = regOperPtr->m_tuple_location.m_page_idx = keyRef2;

  const Uint32 transId1 = lqhOpPtrP->transid[0];
  const Uint32 transId2 = lqhOpPtrP->transid[1];
  Tablerec *const regTabPtr = prepare_tabptr.p;

  /* Get AttrInfo section if this is a long TUPKEYREQ */
  Fragrecord *regFragPtr = prepare_fragptr.p;

  req_struct.trans_id1 = transId1;
  req_struct.trans_id2 = transId2;
  req_struct.tablePtrP = regTabPtr;
  req_struct.fragPtrP = regFragPtr;

  const Uint32 Roptype = regOperPtr->op_type;

  regOperPtr->m_any_value = 0;

  const Uint32 loc_prepare_page_id = prepare_page_no;
  /**
   * Check operation
   */
  if (likely(Roptype == ZREAD)) {
    jamDebug();
    regOperPtr->op_struct.bit_field.m_tuple_existed_at_start = 0;
    ndbassert(!Local_key::isInvalid(pageid, pageidx));

    if (unlikely(m_copy_tuple_used != nullptr)) {
      jamDebug();
      /**
       * Only LCP reads a copy-tuple "directly"
       */
      ndbassert(disk_page == RNIL);
      ndbassert(!m_is_query_block);
      setup_lcp_read_copy_tuple(&req_struct, regOperPtr, regTabPtr);
    } else {
      /**
       * Get pointer to tuple
       */
      jamDebug();
      regOperPtr->m_tuple_location.m_page_no = loc_prepare_page_id;
      setup_fixed_tuple_ref_opt(&req_struct);
      setup_fixed_part(&req_struct, regOperPtr, regTabPtr);
      /**
       * When coming here as a Query thread we must grab a mutex to ensure
       * that the row version we see is written properly, once we have
       * retrieved the row version we need no more protection since the
       * next change either comes through an ABORT or a COMMIT operation
       * and these are all exclusive access that first will ensure that no
       * query threads are executing on the fragment before proceeding.
       */
      acquire_frag_mutex_read(regFragPtr, pageid, jamBuffer());
      if (unlikely(req_struct.m_tuple_ptr->m_header_bits &
                   Tuple_header::FREE)) {
        jam();
        terrorCode = ZTUPLE_DELETED_ERROR;
        tupkeyErrorLab(&req_struct);
        release_frag_mutex_read(regFragPtr, pageid, jamBuffer());
        return false;
      }
      if (unlikely(setup_read(&req_struct, regOperPtr, regTabPtr,
                              disk_page != RNIL) == false)) {
        jam();
        tupkeyErrorLab(&req_struct);
        release_frag_mutex_read(regFragPtr, pageid, jamBuffer());
        return false;
      }
      /* Check checksum with mutex protection. */
      if (unlikely(
              ((regTabPtr->m_bits & Tablerec::TR_Checksum) &&
               (calculateChecksum(req_struct.m_tuple_ptr, regTabPtr) != 0)) ||
              ERROR_INSERTED(4036))) {
        jam();
        release_frag_mutex_read(regFragPtr, pageid, jamBuffer());
        corruptedTupleDetected(&req_struct, regTabPtr);
        return false;
      }
      release_frag_mutex_read(regFragPtr, pageid, jamBuffer());
    }
    if (handleReadReq(signal, regOperPtr, regTabPtr, &req_struct) != -1) {
      req_struct.log_size = 0;
      /* ---------------------------------------------------------------- */
      // Read Operations need not to be taken out of any lists.
      // We also do not need to wait for commit since there is no changes
      // to commit. Thus we
      // prepare the operation record already now for the next operation.
      // Write operations set the state to STARTED indicating that they
      // are waiting for the Commit or Abort decision.
      /* ---------------------------------------------------------------- */
      /**
       * We could release fragment access here for read key readers, but not
       * for scan operations.
       */
      returnTUPKEYCONF(signal, &req_struct, regOperPtr, TRANS_IDLE);
      return true;
    }
    jamDebug();
    return false;
  }
  /**
   * DBQTUP can come here when executing restore, but query thread should
   * not arrive here.
   */
  ndbassert(!m_is_in_query_thread);
  req_struct.changeMask.clear();
  Tuple_header *tuple_ptr = nullptr;

  if (!Local_key::isInvalid(pageid, pageidx)) {
    regOperPtr->op_struct.bit_field.m_tuple_existed_at_start = 1;
  } else {
    regOperPtr->op_struct.bit_field.in_active_list = false;
    regOperPtr->op_struct.bit_field.m_tuple_existed_at_start = 0;
    req_struct.prevOpPtr.i = RNIL;
    if (Roptype == ZINSERT) {
      // No tuple allocated yet
      jamDebug();
      goto do_insert;
    }
    if (Roptype == ZREFRESH) {
      // No tuple allocated yet
      jamDebug();
      goto do_refresh;
    }
    ndbabort();
  }
  /**
   * Get pointer to tuple
   */
  regOperPtr->m_tuple_location.m_page_no = loc_prepare_page_id;
  setup_fixed_tuple_ref_opt(&req_struct);
  setup_fixed_part(&req_struct, regOperPtr, regTabPtr);
  tuple_ptr = req_struct.m_tuple_ptr;

  if (prepareActiveOpList(operPtr, &req_struct)) {
    m_base_header_bits = tuple_ptr->m_header_bits;
    if (Roptype == ZINSERT) {
      jam();
    do_insert:
      Local_key accminupdate;
      Local_key *accminupdateptr = &accminupdate;
      if (unlikely(handleInsertReq(signal, operPtr, prepare_fragptr, regTabPtr,
                                   &req_struct, &accminupdateptr,
                                   false) == -1)) {
        return false;
      }

      if (tuple_ptr != nullptr) {
        jam();
        acquire_frag_mutex(regFragPtr, pageid, jamBuffer());
        /**
         * Updates of checksum needs to be protected during non-initial
         * INSERTs.
         */
        if (tuple_ptr->m_header_bits != m_base_header_bits) {
          /**
           * The checksum is invalid if the ALLOC flag is set in the
           * header bits, but there is no problem in recalculating a
           * new incorrect checksum. So we will perform this calculation
           * even when it isn't required to do it.
           *
           * The bits must still be updated as other threads can look
           * at some bits even before checksum has been set.
           * Updating header bits need always be protected by the TUP
           * fragment mutex.
           */
          Uint32 old_header = tuple_ptr->m_header_bits;
          tuple_ptr->m_header_bits = m_base_header_bits;
          updateChecksum(tuple_ptr, regTabPtr, old_header,
                         tuple_ptr->m_header_bits);
        }

#if defined(VM_TRACE) || defined(ERROR_INSERT)
        /**
         * Verify that we didn't mess up the checksum
         * If the ALLOC flag is set it means that the row hasn't been
         * committed yet, in this state the checksum isn't yet properly
         * set. Thus it makes no sense to verify it.
         */
        if (tuple_ptr != nullptr &&
            ((tuple_ptr->m_header_bits & Tuple_header::ALLOC) == 0) &&
            (regTabPtr->m_bits & Tablerec::TR_Checksum) &&
            (calculateChecksum(tuple_ptr, regTabPtr) != 0)) {
          ndbabort();
        }
#endif
        /**
         * Prepare of INSERT operations is different dependent on whether the
         * row existed before or not (it can exist before if we had a DELETE
         * operation before it in the same transaction). If the row didn't
         * exist then no one can see the row until we have filled in the
         * local key in DBACC which happens below in the call to accminupdate.
         *
         * If the row existed before we need to grab a mutex to ensure that
         * concurrent key readers see a consistent view of the row. We need
         * to update the row before we execute the TUX triggers since they
         * make use of the linked list of operations on the row and this
         * needs to be visible when executing the prepare insert triggers
         * on the TUX index.
         *
         * The INSERT is made visible to other read operations through the
         * call to insertActiveOpList, this includes making it visible to
         * trigger code. If the INSERT is aborted, the inserted row will
         * be visible to read operations from the same transaction for a
         * short time, but first of all reading rows concurrently with an
         * INSERT does not deliver guaranteed results in the first place
         * and second if the transaction aborts, it should not consider
         * the read value anyways. So it should be safe to release the
         * mutex and make the new row visible immediately after
         * completing the INSERT operation and before the actual trigger
         * execution happens that in a rare case could cause the operation
         * to be aborted.
         */
        insertActiveOpList(operPtr, &req_struct, tuple_ptr);
        release_frag_mutex(regFragPtr, pageid, jamBuffer());
      } else {
        /**
         * An initial INSERT operation requires no mutex, and it is
         * trivially already in the active list, even the flag is set
         * in the handleInsertReq method. The insert operation
         * is made visible through the call to execACCMINUPDATE later.
         */
        jam();
      }
      terrorCode = 0;
      checkImmediateTriggersAfterInsert(&req_struct, regOperPtr, regTabPtr,
                                        disk_page != RNIL);

      if (likely(terrorCode == 0)) {
        if (!regTabPtr->tuxCustomTriggers.isEmpty()) {
          jam();
          /**
           * Ensure that no concurrent scans happens while I am
           * updating the TUX indexes.
           *
           * It is vital that I don't hold any fragment mutex while making
           * this call since that could cause a deadlock if any of the
           * threads I am waiting on is requiring this lock to be able to
           * complete its operation before allowing write key access.
           */
          c_lqh->upgrade_to_write_key_frag_access();
          executeTuxInsertTriggers(signal, regOperPtr, regFragPtr, regTabPtr);
        }
      }

      if (unlikely(terrorCode != 0)) {
        jam();
        /*
         * TUP insert succeeded but immediate trigger firing or
         * add of TUX entries failed.
         * All TUX changes have been rolled back at this point.
         *
         * We will abort via tupkeyErrorLab() as usual.  This routine
         * however resets the operation to ZREAD.  The TUP_ABORTREQ
         * arriving later cannot then undo the insert.
         *
         * Therefore we call TUP_ABORTREQ already now.  Diskdata etc
         * should be in memory and timeslicing cannot occur.  We must
         * skip TUX abort triggers since TUX is already aborted.  We
         * will dealloc the fixed and var parts if necessary.
         */
        c_lqh->upgrade_to_exclusive_frag_access_no_return();
        signal->theData[0] = operPtr.i;
        do_tup_abortreq(signal, ZSKIP_TUX_TRIGGERS | ZABORT_DEALLOC);
        tupkeyErrorLab(&req_struct);
        return false;
      }
      /**
       * It is ok to release fragment access already here since the
       * call to ACCMINUPDATE will make the new row appear to other operations
       * in the same transaction, but this is protected by the ACC fragment
       * mutex and requires no special access to the table fragment. TUX
       * index readers get access to the row by the above call to
       * executeTuxInsertTriggers. Thus scanners get access to the new row
       * slightly ahead of read key readers, but this only matters for the
       * operations within the same transaction and we don't guarantee order
       * of those operations towards each other anyways.
       */
      c_lqh->release_frag_access();
      if (accminupdateptr) {
        /**
         * Update ACC local-key, once *everything* has completed successfully
         */
        c_lqh->accminupdate(signal, regOperPtr->userpointer, accminupdateptr);
      }
      returnTUPKEYCONF(signal, &req_struct, regOperPtr, TRANS_STARTED);
      return true;
    }

    ndbassert(!(req_struct.m_tuple_ptr->m_header_bits & Tuple_header::FREE));

    if (Roptype == ZUPDATE || Roptype == ZINSERT_TTL) {
      jamDebug();
      if (unlikely(handleUpdateReq(signal, regOperPtr, regFragPtr, regTabPtr,
                                   &req_struct, disk_page != RNIL) == -1)) {
        return false;
      }
      /*
       * Here we set op_type from ZINSERT_TTL to ZUPDATE to make
       * the following trigger stuffs regard this operation as a
       * normal update operation.
       */
      // TODO(Zhao): double check this solution
      // regOperPtr->op_type = ZUPDATE;
      /*
       * Update this TODO note. Since I added special handling logic for TTL
       * in the unique index (which is an extra internal table) of a TTL table:
       *
       * If its primary table is a TTL table, then in `exec_acckeyreq()`,
       * it will be treated as a TTL table even if there is no TTL information
       * on this extra table. Therefore, we don't need to convert `ZINSERT_TTL`
       * to `ZUPDATE` here.
       *
       * Let's keep it as is for now and observe.
       *
       * Update: if we don't convert it to ZUPDATE here, the foreign key trigger
       * will encounter an issue:
       * 1. CREATE parent table which has TTL
       * 2. CREATE child table which has foreign key references to the parent table
       * 3. Insert a row into the parent table and insert the related row into the
       *    child table
       * 4. Wait for the row in the parent table expires and then insert the row
       *    with the same primary key but different unique index key column
       * THEN we can get an error from the inserting caused by using TTL table
       * as the parent table. But we disable adding foreign key references to
       * a TTL table currently, so it won't happen.
       * TODO(Zhao)
       * However this issue requests fixing when we support this foreign key feature
       * in the future.
       */
      /**
       * The lock on the TUP fragment is required to update header info on
       * the base row, thus we use the variable m_base_header_bits in
       * handleUpdateReq and postpone updating the checksum under mutex
       * protection until after completing the call to handleUpdateReq.
       * This shortens the time we hold the mutex on the fragment part this
       * row belongs to.
       *
       * We can execute other key reads from the query thread concurrently,
       * thus we need to acquire a mutex while inserting the operation
       * into the linked list of operations on the row.
       *
       * Scans will not run in parallel with parallel updates. So the lock
       * on triggers is since we need to set the operation record in the
       * row header before executing the triggers. There is no need for the
       * lock though during execution of the immediate triggers.
       */
      jamDebug();
      acquire_frag_mutex(regFragPtr, pageid, jamBuffer());
      if (tuple_ptr->m_header_bits != m_base_header_bits)
      {
        jamDebug();
        Uint32 old_header = tuple_ptr->m_header_bits;
        tuple_ptr->m_header_bits = m_base_header_bits;
        updateChecksum(tuple_ptr, regTabPtr, old_header,
                       tuple_ptr->m_header_bits);
      }
      insertActiveOpList(operPtr, &req_struct, tuple_ptr);
#if defined(VM_TRACE) || defined(ERROR_INSERT)
      /* Verify that we didn't mess up the checksum */
      if (tuple_ptr != nullptr &&
          ((tuple_ptr->m_header_bits & Tuple_header::ALLOC) == 0) &&
          (regTabPtr->m_bits & Tablerec::TR_Checksum) &&
          (calculateChecksum(tuple_ptr, regTabPtr) != 0)) {
        ndbabort();
      }
#endif
      release_frag_mutex(regFragPtr, pageid, jamBuffer());
      terrorCode = 0;
      checkImmediateTriggersAfterUpdate(&req_struct, regOperPtr, regTabPtr,
                                        disk_page != RNIL);

      if (unlikely(terrorCode != 0)) {
        tupkeyErrorLab(&req_struct);
        return false;
      }

      if (!regTabPtr->tuxCustomTriggers.isEmpty()) {
        jam();
        c_lqh->upgrade_to_write_key_frag_access();
        if (unlikely(executeTuxUpdateTriggers(signal, regOperPtr, regFragPtr,
                                              regTabPtr) != 0)) {
          jam();
          /*
           * See insert case.
           */
          c_lqh->upgrade_to_exclusive_frag_access_no_return();
          signal->theData[0] = operPtr.i;
          do_tup_abortreq(signal, ZSKIP_TUX_TRIGGERS);
          tupkeyErrorLab(&req_struct);
          return false;
        }
      }
      c_lqh->release_frag_access();
      returnTUPKEYCONF(signal, &req_struct, regOperPtr, TRANS_STARTED);
      return true;
    } else if (Roptype == ZDELETE) {
      jam();
      req_struct.log_size = 0;
      if (unlikely(handleDeleteReq(signal, regOperPtr, regFragPtr, regTabPtr,
                                   &req_struct, disk_page != RNIL) == -1)) {
        return false;
      }

      terrorCode = 0;
      /**
       * Prepare of DELETE operations only use shared access to fragments,
       * thus we need to insert the DELETE operation into the list of
       * of operations in a safe way to ensure that there is a well defined
       * point where READ operations can see this row version.
       *
       * It is important to also hold mutex while calling tupkeyErrorLab in
       * case something goes wrong in checking triggers, this ensures that
       * we remove the tuple from the view of the readers before they get
       * access to it.
       */
      jamDebug();
      acquire_frag_mutex(regFragPtr, pageid, jamBuffer());
      insertActiveOpList(operPtr, &req_struct, tuple_ptr);
      release_frag_mutex(regFragPtr, pageid, jamBuffer());
      checkImmediateTriggersAfterDelete(&req_struct, regOperPtr, regTabPtr,
                                        disk_page != RNIL);

      if (unlikely(terrorCode != 0)) {
        tupkeyErrorLab(&req_struct);
        return false;
      }
      /*
       * TUX doesn't need to check for triggers at delete since entries in
       * the index are kept until commit time.
       */
#if defined(VM_TRACE) || defined(ERROR_INSERT)
      /* Verify that we didn't mess up the checksum */
      acquire_frag_mutex(regFragPtr, pageid, jamBuffer());
      if (tuple_ptr != nullptr &&
          ((tuple_ptr->m_header_bits & Tuple_header::ALLOC) == 0) &&
          (regTabPtr->m_bits & Tablerec::TR_Checksum) &&
          (calculateChecksum(tuple_ptr, regTabPtr) != 0)) {
        release_frag_mutex(regFragPtr, pageid, jamBuffer());
        ndbabort();
      }
      release_frag_mutex(regFragPtr, pageid, jamBuffer());
#endif
      c_lqh->release_frag_access();
      returnTUPKEYCONF(signal, &req_struct, regOperPtr, TRANS_STARTED);
      return true;
    } else if (Roptype == ZREFRESH) {
      /**
       * No TUX or immediate triggers, just detached triggers
       */
    do_refresh:
      jamDebug();
      c_lqh->upgrade_to_exclusive_frag_access_no_return();
      if (unlikely(handleRefreshReq(signal, operPtr, prepare_fragptr, regTabPtr,
                                    &req_struct, disk_page != RNIL) == -1)) {
        return false;
      }
      if (tuple_ptr) {
        jam();
        insertActiveOpList(operPtr, &req_struct, tuple_ptr);
      } else {
        jam();
        operPtr.p->op_struct.bit_field.in_active_list = true;
      }
#if defined(VM_TRACE) || defined(ERROR_INSERT)
      /* Verify that we didn't mess up the checksum */
      if (tuple_ptr != nullptr &&
          ((tuple_ptr->m_header_bits & Tuple_header::ALLOC) == 0) &&
          (regTabPtr->m_bits & Tablerec::TR_Checksum) &&
          (calculateChecksum(tuple_ptr, regTabPtr) != 0)) {
        ndbabort();
      }
#endif
      c_lqh->release_frag_access();
      returnTUPKEYCONF(signal, &req_struct, regOperPtr, TRANS_STARTED);
      return true;
    } else {
      ndbabort();  // Invalid op type
    }
  }
  tupkeyErrorLab(&req_struct);
  return false;
}

void Dbtup::setup_fixed_part(KeyReqStruct *req_struct, Operationrec *regOperPtr,
                             Tablerec *regTabPtr) {
  ndbassert(regOperPtr->op_type == ZINSERT ||
            (!(req_struct->m_tuple_ptr->m_header_bits & Tuple_header::FREE)));

  Uint32* tab_descr = regTabPtr->tabDescriptor;
  NDB_PREFETCH_READ((char *)tab_descr);
  Uint32 mm_check_offset = regTabPtr->get_check_offset(MM);
  Uint32 dd_check_offset = regTabPtr->get_check_offset(DD);

  req_struct->check_offset[MM] = mm_check_offset;
  req_struct->check_offset[DD] = dd_check_offset;
  req_struct->attr_descr = tab_descr;
}

void Dbtup::setup_lcp_read_copy_tuple(KeyReqStruct *req_struct,
                                      Operationrec *regOperPtr,
                                      Tablerec *regTabPtr) {
  Uint32 *copytuple = m_copy_tuple_used;
  m_copy_tuple_used = nullptr;

  Uint32* tab_descr = regTabPtr->tabDescriptor;
  Tuple_header *th = get_copy_tuple(copytuple);
  req_struct->m_page_ptr.setNull();
  req_struct->m_tuple_ptr = (Tuple_header *)th;
  th->m_operation_ptr_i = RNIL;
  ndbassert((th->m_header_bits & Tuple_header::COPY_TUPLE) != 0);

  req_struct->attr_descr = tab_descr;

  bool disk = false;
  if (regTabPtr->need_expand(disk)) {
    jam();
    prepare_read(req_struct, regTabPtr, disk);
  }
}

/* ---------------------------------------------------------------- */
/* ------------------------ CONFIRM REQUEST ----------------------- */
/* ---------------------------------------------------------------- */
inline void Dbtup::returnTUPKEYCONF(Signal *signal, KeyReqStruct *req_struct,
                                    Operationrec *regOperPtr,
                                    TransState trans_state) {
  /**
   * When we arrive here we have been executing read code and/or write
   * code to read/write the tuple. During this execution path we have
   * not accessed the regOperPtr object for a long time and we have
   * accessed lots of other data in the meantime. This prefetch was
   * shown useful by using the perf tool. So not an obvious prefetch.
   */
  NDB_PREFETCH_WRITE(regOperPtr);
  TupKeyConf *tupKeyConf = (TupKeyConf *)signal->getDataPtrSend();

  Uint32 Rcreate_rowid = req_struct->m_use_rowid;
  Uint32 RuserPointer = regOperPtr->userpointer;
  Uint32 RnumFiredTriggers = req_struct->num_fired_triggers;
  const Uint32 RnoExecInstructions = req_struct->no_exec_instructions;
  Uint32 log_size = req_struct->log_size;
  Uint32 read_length = req_struct->read_length;
  Uint32 last_row = req_struct->last_row;

  tupKeyConf->userPtr = RuserPointer;
  tupKeyConf->readLength = read_length;
  tupKeyConf->writeLength = log_size;
  tupKeyConf->numFiredTriggers = RnumFiredTriggers;
  tupKeyConf->lastRow = last_row;
  tupKeyConf->rowid = Rcreate_rowid;
  tupKeyConf->noExecInstructions = RnoExecInstructions;
  tupKeyConf->agg_batch_size_rows = req_struct->agg_curr_batch_size_rows;
  tupKeyConf->agg_batch_size_bytes = req_struct->agg_curr_batch_size_bytes;
  tupKeyConf->agg_n_res_recs = req_struct->agg_n_res_recs;
  set_tuple_state(regOperPtr, TUPLE_PREPARED);
  set_trans_state(regOperPtr, trans_state);
}

#define MAX_READ 524288

int Dbtup::checkTTL(Tablerec* regTabPtr,
                    KeyReqStruct *req_struct,
                    bool* has_error,
                    int* err_no) {
  Uint32 attrId = (regTabPtr->m_ttl_col_no);
  const Uint32* attrDescriptor = regTabPtr->tabDescriptor +
    (attrId * ZAD_SIZE);
  const Uint32 TattrDesc1 = attrDescriptor[0];
  const Uint32 type_id = AttributeDescriptor::getType(TattrDesc1);
  [[maybe_unused]] const Uint32 size =
                                AttributeDescriptor::getSize(TattrDesc1);
  [[maybe_unused]] const Uint32 size_in_bytes =
                                AttributeDescriptor::getSizeInBytes(TattrDesc1);
  [[maybe_unused]] const Uint32 size_in_words =
                                AttributeDescriptor::getSizeInWords(TattrDesc1);
  ndbrequire(type_id == NDB_TYPE_DATETIME2 || type_id == NDB_TYPE_TIMESTAMP2);
  TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                  "handleXXXReq TTL check, table_id: %u, "
                  "type_id: %u, size: %u, size_in_bytes: %u, "
                  "size_in_words: %u",
                  req_struct->fragPtrP->fragTableId, type_id, size,
                  size_in_bytes, size_in_words);
  /*
   * TTL related
   * Prepare correct attribute id format before passing it to readAttributes
   */
  attrId = attrId << 16;
  Uint32 out_buf[3];
  /*
   * TTL related
   * TODO (Zhao)
   * Double check whether it's safe to reuse req_struct here or not.
   */
  int ret = readAttributes(req_struct,
      &attrId,
      1,
      out_buf,
      3);
  AttributeHeader* ahOut = (AttributeHeader*)out_buf;
  TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                  "Get ttl column data, col_id: %u, "
                  "byte_size: %u, data_size: %u, is_null: %u",
                  ahOut->getAttributeId(), ahOut->getByteSize(),
                  ahOut->getDataSize(), ahOut->isNULL());
  ndbrequire(regTabPtr->m_ttl_col_no == ahOut->getAttributeId());

  int cmp_ret = 0;
  *has_error = false;
  if (ret >= 0) {
    if (!ahOut->isNULL()) {
      /*
       * TTL related
       * Just need to parse to second part.
       */
      MYSQL_TIME dt;
      if (type_id == NDB_TYPE_TIMESTAMP2) {
        my_timeval timeval;
        my_timestamp_from_binary(&timeval,
            reinterpret_cast<const unsigned char*>(
              ahOut->getDataPtr()), 0);
        struct tm tmp_tm;
        const time_t tmp_t = (time_t)timeval.m_tv_sec;
        gmtime_r(&tmp_t, &tmp_tm);
        // gmt_sec_to_TIME
        if (tmp_tm.tm_year <= 0) {  // Windows sets -1 if timestamp is too high.
          dt.year = 0;
          dt.month = 0;
          dt.day = 0;
          dt.hour = 0;
          dt.minute = 0;
          dt.second = 0;
          dt.second_part = 0;
          dt.time_type = MYSQL_TIMESTAMP_DATETIME;
        } else {
          localtime_to_TIME(&dt, &tmp_tm);
          dt.time_type = MYSQL_TIMESTAMP_DATETIME;
          if (dt.second == 60 || dt.second == 61) {
            dt.second = 59;
          }
        }
      } else {
        int64_t dt_bin = my_datetime_packed_from_binary(
            reinterpret_cast<const unsigned char*>(
              ahOut->getDataPtr()), 0);
        TIME_from_longlong_datetime_packed(&dt, dt_bin);
      }
      TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                      "Parsed TTL column data: "
                      "%u.%u.%u %u:%u:%u",
                      dt.year, dt.month, dt.day,
                      dt.hour, dt.minute, dt.second);
      Uint32 ttl_sec = regTabPtr->m_ttl_sec;
      ttl_sec += req_struct->ttl_purge_window_size;
      bool valid_future_dt = true;
      if (ttl_sec != 0) {
        Interval interval;
        memset(&interval, 0, sizeof(interval));
        interval.second = ttl_sec;
        bool add_ret = date_add_interval(&dt, INTERVAL_SECOND,
            interval, nullptr);
        if (add_ret) {
          g_eventLogger->warning("TTL column adds "
              "interval overflowing");
          valid_future_dt = false;
        }
      }
      if (valid_future_dt) {
        /*
         * TTL related
         * Get current utc time
         */
        MYSQL_TIME curr_dt;
        struct tm tmp_tm;
        time_t t_now = (time_t)my_micro_time() / 1000000; /* second */
        gmtime_r(&t_now, &tmp_tm);
        curr_dt.neg = false;
        curr_dt.second_part = 0;
        curr_dt.year = ((tmp_tm.tm_year + 1900) % 10000);
        curr_dt.month = tmp_tm.tm_mon + 1;
        curr_dt.day = tmp_tm.tm_mday;
        curr_dt.hour = tmp_tm.tm_hour;
        curr_dt.minute = tmp_tm.tm_min;
        curr_dt.second = tmp_tm.tm_sec;
        curr_dt.time_zone_displacement = 0;
        curr_dt.time_type = MYSQL_TIMESTAMP_DATETIME;
        if (curr_dt.second == 60 || curr_dt.second == 61) {
          curr_dt.second = 59;
        }

        /*
         * TTL related
         * Compare with TTL
         */
        TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                        "Get TTL [%u + (%u) = %u], "
                        "expired time: %u.%u.%u %u:%u:%u, "
                        "current time: %u.%u.%u %u:%u:%u",
                        regTabPtr->m_ttl_sec,
                        req_struct->ttl_purge_window_size, ttl_sec,
                        dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second,
                        curr_dt.year, curr_dt.month, curr_dt.day, curr_dt.hour,
                        curr_dt.minute, curr_dt.second);
        cmp_ret = my_time_compare(dt, curr_dt);
      } else {
        // future_dt overflows, we assume this row doesn't expire
        cmp_ret = 1;
      }
    } else {
      /*
       * TTL related
       * TODO (Zhao)
       * remove the warning log here.
       */
#ifdef TTL_DEBUG
      g_eventLogger->warning("Zard, Read a NULL TTL column");
#endif  // TTL_DEBUG
      ndbassert(*has_error == false);
      // NULL equals no TTL is set on the row
      cmp_ret = 1;
    }
  } else {
    jam();
    *has_error = true;
    *err_no = ret;
  }
  return cmp_ret;
}

void Dbtup::PrepareAccLockReq4RAL(void* scan_rec_ptr, Signal* signal) {
  Dblqh::ScanRecord* scan_rec =
             reinterpret_cast<Dblqh::ScanRecord*>(scan_rec_ptr);
  ndbassert(scan_rec != nullptr && scan_rec->scanBlock == this);
  ScanOpPtr scan_op_PTR;
  scan_op_PTR.i = scan_rec->scanAccPtr;
  ndbrequire(c_scanOpPool.getValidPtr(scan_op_PTR));
  ScanOp* scan_op = scan_op_PTR.p;
  ndbrequire(!(scan_op->m_bits & ScanOp::SCAN_LOCK));
  TTL_RONDB_TRACE(scan_op->m_tableId,
                  "Dbtup::PrepareAccLockReq4RAL, "
                  "ScanOp::m_tableId: %u, scanAccPtr: %u",
                  scan_op->m_tableId, scan_op_PTR.i);
  FragrecordPtr fragPtr;
  fragPtr.i = scan_op->m_fragPtrI;
  ndbrequire(c_fragment_pool.getPtr(fragPtr));
  Fragrecord& frag = *fragPtr.p;
  ndbrequire(fragPtr.p->fragTableId == scan_op->m_tableId);

  Uint32 *pkData = (Uint32 *)c_dataBuffer;
  unsigned pkSize = 0;
  jam();
  scan_op->m_last_seen = __LINE__;
  // read tuple key - use TUX routine
  const ScanPos& pos = scan_op->m_scanPos;
  const Local_key& key_mm = pos.m_key_mm;
  TablerecPtr tablePtr;
  tablePtr.i = fragPtr.p->fragTableId;
  ptrCheckGuard(tablePtr, cnoOfTablerec, tablerec);
  int ret = tuxReadPk((Uint32*)fragPtr.p,
      (Uint32*)tablePtr.p,
      pos.m_realpid_mm,
      key_mm.m_page_idx,
      pkData,
      /*hash=*/true);
  ndbrequire(ret > 0);
  pkSize = ret;
  AccLockReq* const lockReq = (AccLockReq*)signal->getDataPtrSend();
  lockReq->returnCode = RNIL;
  lockReq->requestInfo = AccLockReq::LockShared;
  lockReq->accOpPtr = RNIL;
  lockReq->userPtr = scan_op_PTR.i;
  lockReq->userRef = reference();
  lockReq->tableId = scan_op->m_tableId;
  lockReq->fragId = frag.fragmentId;
  lockReq->hashValue =
    rondb_calc_hash_val((const char*)pkData,
        pkSize,
        ((tablePtr.p->m_bits & Tablerec::TR_HashFunction) != 0));
  lockReq->page_id = key_mm.m_page_no;
  lockReq->page_idx = key_mm.m_page_idx;
  lockReq->transId1 = scan_op->m_transId1;
  lockReq->transId2 = scan_op->m_transId2;
  lockReq->isCopyFragScan = ((scan_op->m_bits & ScanOp::SCAN_COPY_FRAG) != 0);
}
/* ---------------------------------------------------------------- */
/* ----------------------------- READ  ---------------------------- */
/* ---------------------------------------------------------------- */
int Dbtup::handleReadReq(
    Signal *signal,
    // UNUSED, is member of req_struct
    Operationrec *_regOperPtr,
    Tablerec *regTabPtr, KeyReqStruct *req_struct) {
  Uint32 *dst;
  Uint32 dstLen;

  dst = &signal->theData[25];
  dstLen = (MAX_READ / 4) - 25;

  if (unlikely(_regOperPtr->ttl_ignore == 0 &&
               _regOperPtr->ttl_only_expired == 1)) {
    ndbassert(req_struct->fragPtrP != nullptr);
    if (!c_lqh->is_ttl_table(req_struct->fragPtrP->fragTableId)) {
      g_eventLogger->warning("(Read) Received an read request with "
                             "ttl_only_expired on a non-TTL table: %d",
                             req_struct->fragPtrP->fragTableId);
      // return Notfound
      terrorCode = 626;
      tupkeyErrorLab(req_struct);
      return -1;
    }
  }

  /*
   * TTL related
   * Here we check whether the row is expired
   */
  if (_regOperPtr->ttl_ignore == 1) {
    TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                    "(Read) Skip checking TTL since "
                    "ttl ignore is set");
  }

  if (_regOperPtr->ttl_ignore == 0 &&
      is_ttl_table(regTabPtr)) {
    bool has_error = false;
    int err_no = 0;
    int cmp_ret = 0;
    TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                    "(READ) handleReadReq TTL check");
    cmp_ret = checkTTL(regTabPtr, req_struct, &has_error, &err_no);
    if (!has_error) {
      if (_regOperPtr->ttl_only_expired == 0) {
        if (cmp_ret <= 0) {
          // Expired
          bool ttl_ignore_for_ral = false;
          if (req_struct->scan_rec != nullptr) {
            Dblqh::ScanRecord* scan_rec_ptr =
              reinterpret_cast<Dblqh::ScanRecord*>(req_struct->scan_rec);
            if (!scan_rec_ptr->scanLockMode /* X */ &&
                !scan_rec_ptr->scanLockHold /* S */) {
               /*
                * NOTICE:
                * Dbtc::fk_scanFromChildTable will break this assumption.
                * Something seems wrong when constructing the scan request
                * flag there.
                */
              // ndbrequire(scan_rec_ptr->readCommitted);
              if (scan_rec_ptr->scanBlock == this) {
                PrepareAccLockReq4RAL(req_struct->scan_rec, signal);
              } else {
                ndbrequire(reinterpret_cast<const void*>(c_lqh->get_c_tux()) ==
                    reinterpret_cast<const void*>(
                      scan_rec_ptr->scanBlock));
                reinterpret_cast<Dbtux*>(scan_rec_ptr->scanBlock)->
                  PrepareAccLockReq4RAL(
                      req_struct->scan_rec,
                      signal);
              }
              ttl_ignore_for_ral = c_acc->WhetherSkipTTL(signal);
              TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                              "Dbtup::handleReadReq() check whether needs "
                              "to ignore TTL: %d", ttl_ignore_for_ral);
            } else {
              TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                              "Dbtup::handleReadReq() skip TTL "
                              "checking for locking-scan on TTL "
                              "table");
            }
          }
          if (!ttl_ignore_for_ral) {
            TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId, "(READ) TTL expired");
            terrorCode = 626;
            tupkeyErrorLab(req_struct);
            return -1;
          }
        }
      } else {
        if (cmp_ret > 0) {
          TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                          "(READ) TTL skip non-expired row "
                          "since only_expired flag is set");
          terrorCode = 626;
          tupkeyErrorLab(req_struct);
          return -1;
        } else {
          TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                          "(READ) TTL return expired row "
                          "since only_expired flag is set");
        }
      }
    } else {
      jam();
      ndbrequire(err_no < 0);
      terrorCode = Uint32(-err_no);
      tupkeyErrorLab(req_struct);
      return -1;
    }
  }

  if (!req_struct->interpreted_exec)
  {
    jamDebug();
    if (req_struct->m_join_agg_state_key != RNIL) {
      jam();
      /*
       * Join aggregation without old interpreter: the aggregate
       * interpreter reads child-table columns directly via DBTUP
       * readAttributes.  No linked parent data is available in
       * the non-interpreted AttrInfo layout.
       */
      int res = handleJoinAggRow(req_struct, nullptr, 0);
      if (res != 0) {
        tupkeyErrorLab(req_struct);
        return -1;
      }
      return 0;
    }
    int ret = readAttributes(req_struct, &cinBuffer[0],
                             req_struct->attrinfo_len, dst, dstLen);
    if (likely(ret >= 0)) {
      /* -------------------------------------------------------------------------
       */
      // We have read all data into coutBuffer. Now send it to the API.
      /* -------------------------------------------------------------------------
       */
      jamDebug();
      const Uint32 TnoOfDataRead = (Uint32)ret;
      sendReadAttrinfo(signal, req_struct, TnoOfDataRead);
      return 0;
    } else {
      terrorCode = Uint32(-ret);
    }
  } else {
    return interpreterStartLab(signal, req_struct);
  }

  jam();
  tupkeyErrorLab(req_struct);
  return -1;
}

static Uint32 get_reorg_flag(Dbtup::KeyReqStruct *req_struct,
                             Dbtup::Fragrecord::FragState state) {
  Uint32 reorg = req_struct->m_reorg;
  switch (state) {
    case Dbtup::Fragrecord::FS_FREE:
    case Dbtup::Fragrecord::FS_REORG_NEW:
    case Dbtup::Fragrecord::FS_REORG_COMMIT_NEW:
    case Dbtup::Fragrecord::FS_REORG_COMPLETE_NEW:
      return 0;
    case Dbtup::Fragrecord::FS_REORG_COMMIT:
    case Dbtup::Fragrecord::FS_REORG_COMPLETE:
      if (reorg != ScanFragReq::REORG_NOT_MOVED) return 0;
      break;
    case Dbtup::Fragrecord::FS_ONLINE:
      if (reorg != ScanFragReq::REORG_MOVED &&
          reorg != ScanFragReq::REORG_MOVED_COPY)
        return 0;
      break;
    default:
      return 0;
  }

  return Dbtup::Tuple_header::REORG_MOVE;
}

/* ---------------------------------------------------------------- */
/* ---------------------------- UPDATE ---------------------------- */
/* ---------------------------------------------------------------- */
int Dbtup::handleUpdateReq(Signal* signal,
                           Operationrec* operPtrP,
                           Fragrecord* regFragPtr,
                           Tablerec* regTabPtr,
                           KeyReqStruct* req_struct,
                           bool disk) 
{
  if (unlikely(operPtrP->ttl_ignore == 0 &&
               operPtrP->ttl_only_expired == 1 &&
               operPtrP->original_op_type != ZWRITE)) {
    ndbassert(req_struct->fragPtrP != nullptr);
    if (!c_lqh->is_ttl_table(req_struct->fragPtrP->fragTableId)) {
      g_eventLogger->warning("(Update) Received an update request with "
                             "ttl_only_expired on a non-TTL table: %d",
                             req_struct->fragPtrP->fragTableId);
      // return Notfound
      terrorCode = 626;
      tupkeyErrorLab(req_struct);
      return -1;
    }
  }

#ifdef TTL_DEBUG
  /*
   * TTL related
   * Here we check whether the row is expired
   *
   * PRECONDITION:
   * If the original operation is ZWRITE, we skip checking
   * TTL
   */
  if (operPtrP->original_op_type == ZWRITE &&
      is_ttl_table(regTabPtr)) {
    TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                    "(UPDATE) Skip checking TTL since "
                    "the original operation is ZWRITE.");
  }
  if (operPtrP->ttl_ignore == 1) {
    TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                    "(Update) Skip checking TTL since "
                     "ttl ignore is set");
  }
#endif  // TTL_DEBUG
  if (operPtrP->ttl_ignore == 0 &&
      operPtrP->original_op_type != ZWRITE &&
      is_ttl_table(regTabPtr)) {
    bool has_error = false;
    int err_no = 0;
    int cmp_ret = 0;
    TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                    "(UPDATE) handleUpdateReq TTL check");
    cmp_ret = checkTTL(regTabPtr, req_struct, &has_error, &err_no);
    if (!has_error) {
      if (cmp_ret <= 0 && operPtrP->op_type != ZINSERT_TTL) {
        /*
         * TTL related
         * 1. Normal update on an already existing but expired row
         */
        TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId, "(UPDATE) TTL expired");
        terrorCode = 626; // HA_ERR_KEY_NOT_FOUND
        tupkeyErrorLab(req_struct);
        return -1;
      } else if (cmp_ret > 0 && operPtrP->op_type == ZINSERT_TTL) {
        /*
         * TTL related
         * 2. Insert an already existing but non-expired row
         */
        TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                        "(UPDATE) ZINSERT_TIL but already "
                        "existing row hasn't expired");
        terrorCode = 630; // HA_ERR_FOUND_DUPP_KEY
        tupkeyErrorLab(req_struct);
        return -1;
      }
      if (cmp_ret <= 0 && operPtrP->op_type == ZINSERT_TTL) {
        TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                        "(UPDATE) ZINSERT_TTL on an duplicated "
                        "expired row");
      }
    } else {
      g_eventLogger->warning("(UPDATE) Failed to read a TTL column");
      jam();
      ndbrequire(err_no < 0);
      terrorCode = Uint32(-err_no);
      tupkeyErrorLab(req_struct);
      return -1;
    }
  }
#ifdef TTL_DEBUG
  if (operPtrP->op_type == ZINSERT_TTL && !is_ttl_table(regTabPtr)) {
    TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                    "(UPDATE) ZINSERT_TTL on an duplicated "
                    "expired row on non-primary table");
  }
#endif  // TTL_DEBUG
  Tuple_header *dst;
  Tuple_header *base= req_struct->m_tuple_ptr, *org;
  ChangeMask * change_mask_ptr;
  if (unlikely((dst= alloc_copy_tuple(
    regTabPtr, &operPtrP->m_copy_tuple_location, false)) == 0))
  {
    terrorCode= ZNO_COPY_TUPLE_MEMORY_ERROR;
    goto error;
  }

  DEB_COPY_TUPLE(("(%u) alloc_copy_tuple: 0x%p, line: %u",
    instance(), operPtrP->m_copy_tuple_location, __LINE__));

  Uint32 tup_version;
  change_mask_ptr = get_change_mask_ptr(regTabPtr, dst);
  if (likely(operPtrP->is_first_operation())) {
    jamDebug();
    org = req_struct->m_tuple_ptr;
    tup_version = org->get_tuple_version();
    clear_change_mask_info(regTabPtr, change_mask_ptr);
  } else {
    jam();
    Operationrec* prevOp= req_struct->prevOpPtr.p;
    tup_version= prevOp->op_struct.bit_field.tupVersion;
    Uint32 * rawptr = prevOp->m_copy_tuple_location;
    org= get_copy_tuple(rawptr);
    copy_change_mask_info(regTabPtr,
        change_mask_ptr,
        get_change_mask_ptr(rawptr));
  }

  /**
   * Check consistency before update/delete
   * Any updates made to the row and checksum is performed by the LDM
   * thread and thus protected by being in the same thread.
   */
  req_struct->m_tuple_ptr = org;
  if (unlikely((regTabPtr->m_bits & Tablerec::TR_Checksum) &&
        (calculateChecksum(req_struct->m_tuple_ptr, regTabPtr) != 0)))
  {
    jam();
    return corruptedTupleDetected(req_struct, regTabPtr);
  }

  req_struct->m_tuple_ptr= dst;
  union {
    Uint32 sizes[4];
    Uint64 cmp[2];
  };

  disk = disk || (org->m_header_bits & Tuple_header::DISK_INLINE);
  if (regTabPtr->need_expand(disk)) {
    jamDebug();
    expand_tuple(req_struct, sizes, org, regTabPtr, disk);
    if (disk && operPtrP->m_undo_buffer_space == 0) {
      jam();
      operPtrP->op_struct.bit_field.m_wait_log_buffer = 1;
      operPtrP->op_struct.bit_field.m_load_diskpage_on_commit = 1;
      operPtrP->m_undo_buffer_space= 
        (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) + sizes[DD] - 1;
      jamDataDebug(operPtrP->m_undo_buffer_space);

      {
        D("Logfile_client - handleUpdateReq");
        Logfile_client lgman(this, c_lgman, regFragPtr->m_logfile_group_id);
        DEB_LCP_LGMAN(("(%u)alloc_log_space(%u): %u",
              instance(),
              __LINE__,
              operPtrP->m_undo_buffer_space));
        terrorCode= lgman.alloc_log_space(operPtrP->m_undo_buffer_space,
            true,
            !req_struct->m_nr_copy_or_redo,
            jamBuffer());
      }
      if (unlikely(terrorCode)) {
        jam();
        operPtrP->m_undo_buffer_space= 0;
        goto error;
      }
    }
  } else {
    memcpy(dst, org, 4 * regTabPtr->m_offsets[MM].m_fix_header_size);
    req_struct->m_tuple_ptr->m_header_bits |= Tuple_header::COPY_TUPLE;
  }

  tup_version= (tup_version + 1) & ZTUP_VERSION_MASK;
  operPtrP->op_struct.bit_field.tupVersion= tup_version;

  req_struct->optimize_options = 0;

  if (!req_struct->interpreted_exec)
  {
    jamDebug();

    if (unlikely(regTabPtr->m_bits & Tablerec::TR_ExtraRowAuthorBits)) {
      jam();
      Uint32 attrId =
          regTabPtr->getExtraAttrId<Tablerec::TR_ExtraRowAuthorBits>();

      store_extra_row_bits(attrId, regTabPtr, dst, /* default */ 0, false);
    }
    req_struct->m_write_log_memory_in_update = false;
    int retValue = updateAttributes(req_struct,
        &cinBuffer[0],
        req_struct->attrinfo_len);
    if (unlikely(retValue < 0))
    {
      terrorCode = Uint32(-retValue);
      goto error;
    }
    if (unlikely(req_struct->m_write_log_memory_in_update == true)) {
      /**
       * Send logMemory back to LQH for propagation to REDO log
       * and other replicas.
       */
      if (unlikely(sendLogAttrinfo(signal,
                                   req_struct,
                                   operPtrP) != 0)) {
        return TUPKEY_abort(req_struct, ZLOG_BUFFER_OVERFLOW_ERROR);
      }
    }
  } else {
    if (unlikely(interpreterStartLab(signal, req_struct) == -1)) return -1;
  }

  update_change_mask_info(regTabPtr,
      change_mask_ptr,
      req_struct->changeMask.rep.data);

  switch (req_struct->optimize_options) {
    case AttributeHeader::OPTIMIZE_MOVE_VARPART:
      /**
       * optimize varpart of tuple,  move varpart of tuple from
       * big-free-size page list into small-free-size page list
       */
      if (base->m_header_bits & Tuple_header::VAR_PART) {
        jam();
        optimize_var_part(req_struct,
                          base,
                          operPtrP,
                          regFragPtr,
                          regTabPtr);
      }
      break;
    case AttributeHeader::OPTIMIZE_MOVE_FIXPART:
      // TODO: move fix part of tuple
      break;
    default:
      break;
  }

  if (regTabPtr->need_shrink()) {
    jamDebug();
    shrink_tuple(req_struct, sizes+2, regTabPtr, disk);
    if (cmp[0] != cmp[1] &&
        ((handle_size_change_after_update(signal,
                                          req_struct,
                                          base,
                                          operPtrP,
                                          regFragPtr,
                                          regTabPtr,
                                          sizes)) != 0))
    {
      goto error;
    }
  }

  if (req_struct->m_reorg != ScanFragReq::REORG_ALL) {
    req_struct->m_tuple_ptr->m_header_bits |=
        get_reorg_flag(req_struct, regFragPtr->fragStatus);
  }

  req_struct->m_tuple_ptr->set_tuple_version(tup_version);

  /**
   * We haven't made the tuple available for readers yet, so no need
   * to protect this change.
   */
  setChecksum(req_struct->m_tuple_ptr, regTabPtr);
  set_tuple_state(operPtrP, TUPLE_PREPARED);

  return 0;

error:
  tupkeyErrorLab(req_struct);
  return -1;
}

/*
   expand_dyn_part - copy dynamic attributes to fully expanded size.

   Both variable-sized and fixed-size attributes are stored in the same way
   in the expanded form as variable-sized attributes (in expand_var_part()).

   This method is used for both mem and disk dynamic data.

   dst         Destination for expanded data
   tabPtrP     Table descriptor
   src         Pointer to the start of dynamic bitmap in source row
   row_len     Total number of 32-bit words in dynamic part of row
   tabDesc     Array of table descriptors
   order       Array of indexes into tabDesc, dynfix followed by dynvar
 */
static
Uint32*
expand_dyn_part(Dbtup::KeyReqStruct::Var_data *dst,
                const Uint32* src,
                Uint32 row_len,
                const Uint32 * tabDesc,
                const Uint16* order,
                Uint32 dynvar,
                Uint32 dynfix,
                Uint32 max_bmlen)
{
  /* Copy the bitmap, zeroing out any words not stored in the row. */
  Uint32 *dst_bm_ptr= (Uint32*)dst->m_dyn_data_ptr;
  Uint32 bm_len = row_len ? (* src & Dbtup::DYN_BM_LEN_MASK) : 0;

#ifdef VM_TRACE
  if (bm_len > max_bmlen) {
    g_eventLogger->info("bm_len: %u, max_bmlen: %u, row_len: %u"
                        ", *src = %x, mask: %x",
                        bm_len,
                        max_bmlen,
                        row_len,
                        *src,
                        Dbtup::DYN_BM_LEN_MASK);
  }
#endif
  assert(bm_len <= max_bmlen);

  if (bm_len > 0) memcpy(dst_bm_ptr, src, 4 * bm_len);
  if (bm_len < max_bmlen)
    std::memset(dst_bm_ptr + bm_len, 0, 4 * (max_bmlen - bm_len));

  /**
   * Store max_bmlen for homogenous code in DbtupRoutines
   */
  Uint32 tmp = (*dst_bm_ptr);
  *dst_bm_ptr = (tmp & ~(Uint32)Dbtup::DYN_BM_LEN_MASK) | max_bmlen;

  char *src_off_start = (char *)(src + bm_len);
  assert((UintPtr(src_off_start) & 3) == 0);
  Uint16 *src_off_ptr = (Uint16 *)src_off_start;

  /*
     Prepare the variable-sized dynamic attributes, copying out data from the
     source row for any that are not NULL.
   */
  Uint32 no_attr= dst->m_dyn_len_offset;
  Uint16* dst_off_ptr= dst->m_dyn_offset_arr_ptr;
  Uint16* dst_len_ptr= dst_off_ptr + no_attr;
  Uint16 this_src_off= row_len ? * src_off_ptr++ : 0;
  /* We need to reserve room for the offsets written by shrink_tuple+padding.*/
  Uint16 dst_off = 4 * (max_bmlen + ((dynvar + 2) >> 1));
  char *dst_ptr = (char *)dst_bm_ptr + dst_off;
  for (Uint32 i = 0; i < dynvar; i++) {
    Uint16 j = order[dynfix + i];
    Uint32 max_len = 4 * AttributeDescriptor::getSizeInWords(tabDesc[j]);
    Uint32 len;
    Uint32 pos = AttributeOffset::getNullFlagPos(tabDesc[j + 1]);
    if (bm_len > (pos >> 5) && BitmaskImpl::get(bm_len, src, pos)) {
      Uint16 next_src_off = *src_off_ptr++;
      len = next_src_off - this_src_off;
      memcpy(dst_ptr, src_off_start + this_src_off, len);
      this_src_off = next_src_off;
    } else {
      len = 0;
    }
    dst_off_ptr[i] = dst_off;
    dst_len_ptr[i] = dst_off + len;
    dst_off += max_len;
    dst_ptr += max_len;
  }
  /*
     The fixed-size data is stored 32-bit aligned after the variable-sized
     data.
   */
  char *src_ptr= src_off_start+this_src_off;
  src_ptr= (char *)(ALIGN_WORD(src_ptr));

  /*
     Prepare the fixed-size dynamic attributes, copying out data from the
     source row for any that are not NULL.
     Note that the fixed-size data is stored in reverse from the end of the
     dynamic part of the row. This is true both for the stored/shrunken and
     for the expanded form.
   */
  for(Uint32 i= dynfix; i>0; )
  {
    i--;
    Uint16 j = order[i];
    Uint32 fix_size = 4 * AttributeDescriptor::getSizeInWords(tabDesc[j]);
    dst_off_ptr[dynvar + i] = dst_off;
    /* len offset array is not used for fixed size. */
    Uint32 pos = AttributeOffset::getNullFlagPos(tabDesc[j + 1]);
    if (bm_len > (pos >> 5) && BitmaskImpl::get(bm_len, src, pos)) {
      assert((UintPtr(dst_ptr) & 3) == 0);
      memcpy(dst_ptr, src_ptr, fix_size);
      src_ptr += fix_size;
    }
    dst_off += fix_size;
    dst_ptr += fix_size;
  }

  return (Uint32 *)dst_ptr;
}

static Uint32 *shrink_dyn_part(Dbtup::KeyReqStruct::Var_data *dst,
                               Uint32 *dst_ptr, const Dbtup::Tablerec *tabPtrP,
                               const Uint32 *tabDesc, const Uint16 *order,
                               Uint32 dynvar, Uint32 dynfix, Uint32 ind) {
  /**
   * Now build the dynamic part, if any.
   * First look for any trailing all-NULL words of the bitmap; we do
   * not need to store those.
   */
  assert((UintPtr(dst->m_dyn_data_ptr) & 3) == 0);
  char *dyn_src_ptr = dst->m_dyn_data_ptr;
  Uint32 bm_len = tabPtrP->m_offsets[ind].m_dyn_null_words;  // In words

  /* If no dynamic variables, store nothing. */
  assert(bm_len);
  {
    /**
     * clear bm-len bits, so they won't incorrect indicate
     *   a non-zero map
     */
    *((Uint32 *)dyn_src_ptr) &= ~Uint32(Dbtup::DYN_BM_LEN_MASK);

    Uint32 *bm_ptr = (Uint32 *)dyn_src_ptr + bm_len - 1;
    while (*bm_ptr == 0) {
      bm_ptr--;
      bm_len--;
      if (bm_len == 0) break;
    }
  }

  if (bm_len) {
    /**
     * Copy the bitmap, counting the number of variable sized
     * attributes that are not NULL on the way.
     */
    Uint32 *dyn_dst_ptr = dst_ptr;
    Uint32 dyn_var_count = 0;
    const Uint32 *src_bm_ptr = (Uint32 *)(dyn_src_ptr);
    Uint32 *dst_bm_ptr = (Uint32 *)dyn_dst_ptr;

    /* ToDo: Put all of the dynattr code inside if(bm_len>0) { ... },
     * split to separate function. */
    Uint16 dyn_dst_data_offset = 0;
    const Uint32 *dyn_bm_var_mask_ptr = tabPtrP->dynVarSizeMask[ind];
    for (Uint16 i = 0; i < bm_len; i++) {
      Uint32 v = src_bm_ptr[i];
      dyn_var_count += BitmaskImpl::count_bits(v & *dyn_bm_var_mask_ptr++);
      dst_bm_ptr[i] = v;
    }

    Uint32 tmp = *dyn_dst_ptr;
    assert(bm_len <= Dbtup::DYN_BM_LEN_MASK);
    *dyn_dst_ptr = (tmp & ~(Uint32)Dbtup::DYN_BM_LEN_MASK) | bm_len;
    dyn_dst_ptr += bm_len;
    dyn_dst_data_offset = 2 * dyn_var_count + 2;

    Uint16 *dyn_src_off_array = dst->m_dyn_offset_arr_ptr;
    Uint16 *dyn_src_lenoff_array = dyn_src_off_array + dst->m_dyn_len_offset;
    Uint16 *dyn_dst_off_array = (Uint16 *)dyn_dst_ptr;

    /**
     * Copy over the variable sized not-NULL attributes.
     * Data offsets are counted from the start of the offset array, and
     * we store one additional offset to be able to easily compute the
     * data length as the difference between offsets.
     */
    Uint16 off_idx = 0;
    for (Uint32 i = 0; i < dynvar; i++) {
      /**
       * Note that we must use the destination (shrunken) bitmap here,
       * as the source (expanded) bitmap may have been already clobbered
       * (by offset data).
       */
      Uint32 attrDesc2 = tabDesc[order[dynfix + i] + 1];
      Uint32 pos = AttributeOffset::getNullFlagPos(attrDesc2);
      if (bm_len > (pos >> 5) && BitmaskImpl::get(bm_len, dst_bm_ptr, pos)) {
        dyn_dst_off_array[off_idx++] = dyn_dst_data_offset;
        Uint32 dyn_src_off = dyn_src_off_array[i];
        Uint32 dyn_len = dyn_src_lenoff_array[i] - dyn_src_off;
        memmove(((char *)dyn_dst_ptr) + dyn_dst_data_offset,
            dyn_src_ptr + dyn_src_off,
            dyn_len);
        dyn_dst_data_offset+= dyn_len;
      }
    }
    /* If all dynamic attributes are NULL, we store nothing. */
    dyn_dst_off_array[off_idx] = dyn_dst_data_offset;
    assert(dyn_dst_off_array + off_idx ==
           (Uint16 *)dyn_dst_ptr + dyn_var_count);

    char *dynvar_end_ptr = ((char *)dyn_dst_ptr) + dyn_dst_data_offset;
    char *dyn_dst_data_ptr = (char *)(ALIGN_WORD(dynvar_end_ptr));

    /**
     * Zero out any padding bytes. Might not be strictly necessary,
     * but seems cleaner than leaving random stuff in there.
     */
    std::memset(dynvar_end_ptr, 0, dyn_dst_data_ptr - dynvar_end_ptr);

    /* *
     * Copy over the fixed-sized not-NULL attributes.
     * Note that attributes are copied in reverse order; this is to avoid
     * overwriting not-yet-copied data, as the data is also stored in
     * reverse order.
     */
    for (Uint32 i = dynfix; i > 0;) {
      i--;
      Uint16 j = order[i];
      Uint32 attrDesc2 = tabDesc[j + 1];
      Uint32 pos = AttributeOffset::getNullFlagPos(attrDesc2);
      if(bm_len > (pos >>5 ) && BitmaskImpl::get(bm_len, dst_bm_ptr, pos))
      {
        Uint32 fixsize=
          4*AttributeDescriptor::getSizeInWords(tabDesc[j]);
        memmove(dyn_dst_data_ptr,
            dyn_src_ptr + dyn_src_off_array[dynvar+i],
            fixsize);
        dyn_dst_data_ptr += fixsize;
      }
    }
    dst_ptr = (Uint32 *)dyn_dst_data_ptr;
    assert((UintPtr(dst_ptr) & 3) == 0);
  }
  return (Uint32 *)dst_ptr;
}

/* ---------------------------------------------------------------- */
/* ----------------------------- INSERT --------------------------- */
/* ---------------------------------------------------------------- */
void
Dbtup::prepare_initial_insert(KeyReqStruct *req_struct, 
                              Operationrec* regOperPtr,
                              Tablerec* regTabPtr,
                              bool is_refresh)
{
  Uint32 disk_undo = ((regTabPtr->m_no_of_disk_attributes > 0) &&
      !is_refresh) ? 
    sizeof(Dbtup::Disk_undo::Alloc) >> 2 : 0;
  regOperPtr->m_undo_buffer_space= disk_undo;
  jamDebug();
  jamDataDebug(regOperPtr->m_undo_buffer_space);

  req_struct->check_offset[MM]= regTabPtr->get_check_offset(MM);
  req_struct->check_offset[DD]= regTabPtr->get_check_offset(DD);

  Uint16* order = regTabPtr->m_real_order_descriptor;
  Uint32 *tab_descr = regTabPtr->tabDescriptor;
  req_struct->attr_descr = tab_descr; 

  Uint32 bits = Tuple_header::COPY_TUPLE;
  bits |= disk_undo ? (Tuple_header::DISK_ALLOC|Tuple_header::DISK_INLINE) : 0;
  req_struct->m_tuple_ptr->m_header_bits= bits;

  Uint32 *ptr= req_struct->m_tuple_ptr->get_end_of_fix_part_ptr(regTabPtr);
  Var_part_ref* ref = req_struct->m_tuple_ptr->get_var_part_ref_ptr(regTabPtr);

  if (regTabPtr->m_bits & Tablerec::TR_ForceVarPart)
  {
    ref->m_page_no = RNIL; 
    ref->m_page_idx = Tup_varsize_page::END_OF_FREE_LIST;
  }

  for (Uint32 ind = 0; ind < 2; ind++)
  {
    const Uint32 num_fix = regTabPtr->m_attributes[ind].m_no_of_fixsize;
    const Uint32 num_vars= regTabPtr->m_attributes[ind].m_no_of_varsize;
    const Uint32 num_dyns= regTabPtr->m_attributes[ind].m_no_of_dynamic;

    if (ind == DD)
    {
      jamDebug();
      Uint32 disk_fix_header_size =
        regTabPtr->m_offsets[DD].m_fix_header_size;
      req_struct->m_disk_ptr= (Tuple_header*)ptr;
      ptr += disk_fix_header_size;
    }
    order += num_fix;
    jamDebug();
    jamDataDebug(num_fix);
    if (num_vars || num_dyns)
    {
      jam();
      /* Init Varpart_copy struct */
      Varpart_copy * cp = (Varpart_copy*)ptr;
      cp->m_len = 0;
      ptr += Varpart_copy::SZ32;

      /* Prepare empty varsize part. */
      KeyReqStruct::Var_data* dst= &req_struct->m_var_data[ind];

      if (num_vars)
      {
        jamDebug();
        dst->m_data_ptr= (char*)(((Uint16*)ptr)+num_vars+1);
        dst->m_offset_array_ptr= req_struct->var_pos_array[ind];
        dst->m_var_len_offset= num_vars;
        dst->m_max_var_offset= regTabPtr->m_offsets[ind].m_max_var_offset;

        Uint32 pos= 0;
        Uint16 *pos_ptr = req_struct->var_pos_array[ind];
        Uint16 *len_ptr = pos_ptr + num_vars;
        for (Uint32 i = 0; i < num_vars; i++)
        {
          * pos_ptr++ = pos;
          * len_ptr++ = pos;
          pos += AttributeDescriptor::getSizeInBytes(
              tab_descr[*order++]);
          jamDebug();
          jamDataDebug(pos);
        }

        // Disk/dynamic part is 32-bit aligned
        ptr = ALIGN_WORD(dst->m_data_ptr+pos);
        ndbassert(ptr == ALIGN_WORD(dst->m_data_ptr + 
              regTabPtr->m_offsets[ind].m_max_var_offset));
      }

      if (num_dyns)
      {
        const Uint32 num_dynvar= regTabPtr->m_attributes[ind].m_no_of_dyn_var;
        const Uint32 num_dynfix= regTabPtr->m_attributes[ind].m_no_of_dyn_fix;
        jam();
        /* Prepare empty dynamic part. */
        dst->m_dyn_data_ptr= (char *)ptr;
        dst->m_dyn_offset_arr_ptr= req_struct->var_pos_array[ind]+2*num_vars;
        dst->m_dyn_len_offset= num_dynvar+num_dynfix;
        dst->m_max_dyn_offset= regTabPtr->m_offsets[ind].m_max_dyn_offset;

        ptr = expand_dyn_part(dst,
            0,
            0,
            (Uint32*)tab_descr,
            order,
            num_dynvar,
            num_dynfix,
            regTabPtr->m_offsets[ind].m_dyn_null_words);
        order += (num_dynvar + num_dynfix);
      }

      ndbassert((UintPtr(ptr)&3) == 0);
    }
  }
  /**
   * The copy tuple will be copied directly into the rowid position of
   * the tuple. Since we use the GCI in this position to see if a row
   * has changed we need to ensure that the GCI value is initialised,
   * otherwise we will not count inserts as a changed row.
   */
  *req_struct->m_tuple_ptr->get_mm_gci(regTabPtr) = 0;

  // Set all null bits
  std::memset(req_struct->m_tuple_ptr->m_null_bits+
      regTabPtr->m_offsets[MM].m_null_offset, 0xFF,
      4*regTabPtr->m_offsets[MM].m_null_words);
  std::memset(req_struct->m_disk_ptr->m_null_bits+
      regTabPtr->m_offsets[DD].m_null_offset, 0xFF,
      4*regTabPtr->m_offsets[DD].m_null_words);
}

int Dbtup::handleInsertReq(Signal* signal,
    Ptr<Operationrec> regOperPtr,
    FragrecordPtr fragPtr,
    Tablerec* regTabPtr,
    KeyReqStruct *req_struct,
    Local_key ** accminupdateptr,
    bool is_refresh)
{
  Uint32 tup_version = 1;
  Fragrecord* regFragPtr = fragPtr.p;
  Uint32 *ptr = nullptr;
  Tuple_header *dst;
  Tuple_header *base = req_struct->m_tuple_ptr;
  Tuple_header *org = base;
  Tuple_header *tuple_ptr;

  bool disk = regTabPtr->m_no_of_disk_attributes > 0 && !is_refresh;
  bool mem_insert = regOperPtr.p->is_first_operation();
  bool disk_insert = mem_insert && disk;
  bool vardynsize = (regTabPtr->m_attributes[MM].m_no_of_varsize ||
      regTabPtr->m_attributes[MM].m_no_of_dynamic);
  bool varalloc = vardynsize || regTabPtr->m_bits & Tablerec::TR_ForceVarPart;
  bool rowid = req_struct->m_use_rowid;
  bool update_acc = false;
  Uint32 real_page_id = regOperPtr.p->m_tuple_location.m_page_no;
  Uint32 frag_page_id = req_struct->frag_page_id;

  union {
    Uint32 sizes[4];
    Uint64 cmp[2];
  };
  cmp[0] = cmp[1] = 0;

  if (ERROR_INSERTED(4014)) {
    dst = 0;
    goto trans_mem_error;
  }

  dst = alloc_copy_tuple(regTabPtr,
                         &regOperPtr.p->m_copy_tuple_location,
                         true);

  if (unlikely(dst == nullptr))
  {
    goto trans_mem_error;
  }

  DEB_COPY_TUPLE(("(%u) alloc_copy_tuple: 0x%p, line: %u",
    instance(), regOperPtr.p->m_copy_tuple_location, __LINE__));

  tuple_ptr= req_struct->m_tuple_ptr = dst;
  set_change_mask_info(regTabPtr, get_change_mask_ptr(regTabPtr, dst));

  if (mem_insert)
  {
    jamDebug();
    prepare_initial_insert(req_struct, regOperPtr.p, regTabPtr, is_refresh);
  } else {
    Operationrec *prevOp = req_struct->prevOpPtr.p;
    ndbassert(prevOp->op_type == ZDELETE);
    tup_version= prevOp->op_struct.bit_field.tupVersion + 1;

    if(unlikely(!prevOp->is_first_operation()))
    {
      jam();
      org = get_copy_tuple(prevOp->m_copy_tuple_location);
    } else {
      jamDebug();
    }
    if (regTabPtr->need_expand()) {
      jamDebug();
      expand_tuple(req_struct, sizes, org, regTabPtr, !disk_insert);
      std::memset(req_struct->m_disk_ptr->m_null_bits+
          regTabPtr->m_offsets[DD].m_null_offset, 0xFF,
          4*regTabPtr->m_offsets[DD].m_null_words);

      Uint32 bm_size_in_bytes = 4 * (regTabPtr->m_offsets[MM].m_dyn_null_words);
      if (bm_size_in_bytes) {
        Uint32 *ptr = (Uint32 *)req_struct->m_var_data[MM].m_dyn_data_ptr;
        std::memset(ptr, 0, bm_size_in_bytes);
        *ptr = bm_size_in_bytes >> 2;
      }
    } else {
      jamDebug();
      memcpy(dst, org, 4 * regTabPtr->m_offsets[MM].m_fix_header_size);
      tuple_ptr->m_header_bits |= Tuple_header::COPY_TUPLE;
    }
    std::memset(tuple_ptr->m_null_bits+
        regTabPtr->m_offsets[MM].m_null_offset, 0xFF,
        4*regTabPtr->m_offsets[MM].m_null_words);
  }

  int res;
  if (disk_insert) {
    jamDebug();
    if (ERROR_INSERTED(4015)) {
      terrorCode = 1501;
      goto log_space_error;
    }

    {
      D("Logfile_client - handleInsertReq");
      Logfile_client lgman(this, c_lgman, regFragPtr->m_logfile_group_id);
      DEB_LCP_LGMAN(("(%u)alloc_log_space(%u): %u",
            instance(),
            __LINE__,
            regOperPtr.p->m_undo_buffer_space));
      res= lgman.alloc_log_space(regOperPtr.p->m_undo_buffer_space,
          true,
          !req_struct->m_nr_copy_or_redo,
          jamBuffer());
      jamDebug();
      jamDataDebug(regOperPtr.p->m_undo_buffer_space);
    }
    if (unlikely(res)) {
      jam();
      terrorCode = res;
      goto log_space_error;
    }
  }

  regOperPtr.p->op_struct.bit_field.tupVersion=
    tup_version & ZTUP_VERSION_MASK;
  tuple_ptr->set_tuple_version(tup_version);

  if (ERROR_INSERTED(4016)) {
    terrorCode = ZAI_INCONSISTENCY_ERROR;
    goto update_error;
  }

  if (regTabPtr->m_bits & Tablerec::TR_ExtraRowAuthorBits) {
    jamDebug();
    Uint32 attrId =
        regTabPtr->getExtraAttrId<Tablerec::TR_ExtraRowAuthorBits>();

    store_extra_row_bits(attrId, regTabPtr, tuple_ptr, /* default */ 0, false);
  }

  req_struct->m_write_log_memory_in_update = false;
  if (!(is_refresh ||
        regTabPtr->m_default_value_location.isNull()))
  {
    jamDebug();
    Uint32 default_values_len;
    /* Get default values ptr + len for this table */
    Uint32 *default_values = get_default_ptr(regTabPtr, default_values_len);
    ndbrequire(default_values_len != 0 && default_values != NULL);
    /*
     * Update default values into row first,
     * next update with data received from the client.
     */
    if(unlikely((res = updateAttributes(req_struct, default_values,
              default_values_len)) < 0))
    {
      jam();
      terrorCode = Uint32(-res);
      goto update_error;
    }
  }
  if (unlikely(req_struct->interpreted_exec)) {
    if (likely(req_struct->interpreted_insert)) {
      /**
       * To support actions like INCR using Redis server and APPEND operations
       * it is important to be able to have a complex interpreted program using
       * Write operations. The interpreted program must in this be able to
       * handle writes where the column had no value before its execution.
       * This makes it possible to send efficient Attrinfo, other solutions
       * would require sending the column data in several different places.
       *
       * This is a new RonDB feature as of RonDB 24.10.
       *
       * To support this we have a new instruction that loads the operation
       * type into a register.
       */
      if (unlikely(interpreterStartLab(signal, req_struct) == -1)) return -1;
    } else {
      jam();

      req_struct->m_write_log_memory_in_update = true;
      req_struct->log_size = 0;

      /* Interpreted insert only processes the finalUpdate section */
      const Uint32 RinitReadLen = cinBuffer[0];
      const Uint32 RexecRegionLen = cinBuffer[1];
      const Uint32 RfinalUpdateLen = cinBuffer[2];
      jamData(RfinalUpdateLen);
      // const Uint32 RfinalRLen= cinBuffer[3];
      // const Uint32 RsubLen= cinBuffer[4];
  
      const Uint32 offset = 5 + RinitReadLen + RexecRegionLen;
  
      if (unlikely((res = updateAttributes(req_struct, &cinBuffer[offset],
                RfinalUpdateLen)) < 0)) {
        jam();
        terrorCode = Uint32(-res);
        goto update_error;
      }
    }
  } else {
    /* Normal insert */
    jamDebug();
    if (unlikely((res = updateAttributes(req_struct, &cinBuffer[0],
              req_struct->attrinfo_len)) < 0))
    {
      jam();
      terrorCode = Uint32(-res);
      goto update_error;
    }
  }
  /**
   * Send logMemory back to LQH for propagation to REDO log and replicas
   */
  if (unlikely(req_struct->m_write_log_memory_in_update)) {
    if (unlikely(sendLogAttrinfo(signal,
                                 req_struct,
                                 regOperPtr.p) != 0)) {
      jam();
      goto update_error;
    }
  }
  if (ERROR_INSERTED(4017)) {
    goto null_check_error;
  }
  if (unlikely(checkNullAttributes(req_struct,
          regTabPtr,
          is_refresh) == false))
  {
    goto null_check_error;
  }

  if (req_struct->m_is_lcp)
  {
    jam();
    sizes[2 + MM] = req_struct->m_lcp_varpart_len;
  } else if (regTabPtr->need_shrink()) {
    jamDebug();
    shrink_tuple(req_struct, sizes+2, regTabPtr, true);
  }

  if (ERROR_INSERTED(4025)) {
    goto mem_error;
  }

  if (ERROR_INSERTED(4026)) {
    CLEAR_ERROR_INSERT_VALUE;
    goto mem_error;
  }

  if (ERROR_INSERTED(4027) && (rand() % 100) > 25) {
    goto mem_error;
  }

  if (ERROR_INSERTED(4028) && (rand() % 100) > 25)
  {
    CLEAR_ERROR_INSERT_VALUE;
    goto mem_error;
  }

  /**
   * Alloc memory
   */
  if (mem_insert) {
    terrorCode = 0;
    if (!rowid)
    {
      if (ERROR_INSERTED(4018))
      {
        goto mem_error;
      }

      if (!varalloc)
      {
        jam();
        ptr= alloc_fix_rec(
            jamBuffer(),
            &terrorCode,
            regFragPtr,
            regTabPtr,
            &regOperPtr.p->m_tuple_location,
            &frag_page_id);
      } 
      else 
      {
        jam();
        regOperPtr.p->m_tuple_location.m_file_no= sizes[2+MM];
        ptr= alloc_var_row(
            signal,
            &terrorCode,
            regFragPtr, regTabPtr,
            sizes[2+MM],
            &regOperPtr.p->m_tuple_location,
            &frag_page_id,
            false);
      }
      if (unlikely(ptr == 0))
      {
        goto mem_error;
      }
      req_struct->m_use_rowid = true;
    } else {
      regOperPtr.p->m_tuple_location = req_struct->m_row_id;
      if (ERROR_INSERTED(4019))
      {
        terrorCode = ZROWID_ALLOCATED;
        goto alloc_rowid_error;
      }

      if (!varalloc)
      {
        jam();
        ptr= alloc_fix_rowid(&terrorCode,
            regFragPtr,
            regTabPtr,
            &regOperPtr.p->m_tuple_location,
            &frag_page_id);
      } 
      else 
      {
        jam();
        regOperPtr.p->m_tuple_location.m_file_no= sizes[2+MM];
        ptr= alloc_var_row(
            signal,
            &terrorCode,
            regFragPtr, regTabPtr,
            sizes[2+MM],
            &regOperPtr.p->m_tuple_location,
            &frag_page_id,
            true);
      }
      if (unlikely(ptr == 0))
      {
        jam();
        goto alloc_rowid_error;
      }
    }
    /**
     * Arriving here we have acquired the fragment page mutex in
     * either alloc_fix_rec (can be called from alloc_var_rec) or
     * alloc_fix_rowid (can be called from alloc_var_rowid).
     *
     * Thus as soon as we release the fragment mutex the row will
     * be visible to the TUP scan.
     */
    regOperPtr.p->fragPageId = frag_page_id;
    real_page_id = regOperPtr.p->m_tuple_location.m_page_no;
    update_acc = true; /* Will be updated later once success is known */

    base = (Tuple_header*)ptr;
    regOperPtr.p->op_struct.bit_field.in_active_list = true;
    base->m_operation_ptr_i = regOperPtr.i;
    ndbassert(!m_is_in_query_thread);

#ifdef DEBUG_DELETE
    char *insert_str;
    if (req_struct->m_is_lcp) {
      insert_str = (char *)"LCP_INSERT";
    } else {
      insert_str = (char *)"INSERT";
    }
    DEB_DELETE(("(%u)%s: tab(%u,%u) row(%u,%u)",
          instance(),
          insert_str,
          regFragPtr->fragTableId,
          regFragPtr->fragmentId,
          frag_page_id,
          regOperPtr.p->m_tuple_location.m_page_idx));
#endif

    /**
     * The LCP_SKIP and LCP_DELETE flags must be retained even when allocating
     * a new row since they record state for the rowid and not for the record
     * as such. So we need to know state of rowid in LCP scans.
     */
    Uint32 old_header_keep =
      base->m_header_bits &
      (Tuple_header::LCP_SKIP | Tuple_header::LCP_DELETE);
    base->m_header_bits= Tuple_header::ALLOC |
      (sizes[2+MM] > 0 ? Tuple_header::VAR_PART : 0) |
      old_header_keep;
    if ((regTabPtr->m_bits & Tablerec::TR_UseVarSizedDiskData) != 0)
    {
      jam();
      if (disk_insert)
      {
        jam();
        /**
         * If mem_insert is true and disk_insert isn't true, this means
         * that this a Refresh operation, in this case we will not create
         * any disk part. Thus we can avoid setting DISK_VAR_PART to
         * ensure we don't attempt to retrieve the disk page.
         */
        base->m_header_bits |= Tuple_header::DISK_VAR_PART;
      }
    }
    if (disk_insert)
    {
      Local_key tmp;
      Uint32 size =
        ((regTabPtr->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0) ?
        1 : (sizes[2+DD] + 1);

      jamDebug();
      jamDataDebug(size);

      if (ERROR_INSERTED(4021))
      {
        terrorCode = 1601;
        goto disk_prealloc_error;
      }


      int ret= disk_page_prealloc(signal,
          fragPtr,
          regTabPtr,
          &tmp,
          size);
      if (unlikely(ret < 0))
      {
        jam();
        terrorCode = -ret;
        goto disk_prealloc_error;
      }

      jamDebug();
      jamDataDebug(tmp.m_file_no);
      jamDataDebug(tmp.m_page_no);
      regOperPtr.p->op_struct.bit_field.m_disk_preallocated= 1;
      tmp.m_page_idx= size;
      /**
       * We update the disk row reference both in the allocated row and
       * in the allocated copy row. The allocated row reference is used
       * in load_diskpage if we do any further operations on the
       * row in the same transaction.
       */
      memcpy(tuple_ptr->get_disk_ref_ptr(regTabPtr), &tmp, sizeof(tmp));
      memcpy(base->get_disk_ref_ptr(regTabPtr), &tmp, sizeof(tmp));

      /**
       * Set ref from disk to mm
       */
      Local_key ref = regOperPtr.p->m_tuple_location;
      ref.m_page_no = frag_page_id;

      ndbrequire(ref.m_page_idx < Tup_page::DATA_WORDS);
      Tuple_header* disk_ptr= req_struct->m_disk_ptr;
      DEB_DISK(("(%u) set_base_record(%u,%u).%u on row(%u,%u)",
            instance(),
            tmp.m_file_no,
            tmp.m_page_no,
            tmp.m_page_idx,
            ref.m_page_no,
            ref.m_page_idx));

      disk_ptr->set_base_record_ref(ref);
    }

    if (req_struct->m_reorg != ScanFragReq::REORG_ALL) {
      req_struct->m_tuple_ptr->m_header_bits |=
          get_reorg_flag(req_struct, regFragPtr->fragStatus);
    }

    setChecksum(req_struct->m_tuple_ptr, regTabPtr);
    /**
     * At this point we hold the fragment mutex to ensure that TUP scans
     * don't see the rows until the row is ready for reading.
     *
     * After releasing the mutex here the row becomes visible to TUP scans
     * and thus checksum needs to be correct on the copy row, the checksum
     * on the row itself isn't checked before reading or updating unless it
     * is used for reading. So no need to update it already here. It will
     * be set when we commit the change.
     */
    release_frag_mutex(regFragPtr, frag_page_id, jamBuffer());
  }
  else 
  {
    /* ! mem_insert */
    if (ERROR_INSERTED(4020)) {
      c_lqh->upgrade_to_exclusive_frag_access();
      goto size_change_error;
    }

    if (regTabPtr->need_shrink() && cmp[0] != cmp[1] &&
        unlikely(handle_size_change_after_update(signal,
            req_struct,
            base,
            regOperPtr.p,
            regFragPtr,
            regTabPtr,
            sizes) != 0))
    {
      goto size_change_error;
    }
    req_struct->m_use_rowid = false;
    /**
     * The main row header bits are updated through a local variable
     * such that we can do the change under mutex protection after
     * finalizing the writes on the row.
     */
    m_base_header_bits &= ~(Uint32)Tuple_header::FREE;

    if (req_struct->m_reorg != ScanFragReq::REORG_ALL) {
      m_base_header_bits |= get_reorg_flag(req_struct, regFragPtr->fragStatus);
    }

    /**
     * Fragment-locking :
     *   No need to protect this checksum write, we only perform it here for
     *   non-first inserts since first insert operations are handled above
     *   while holding the mutex. For non-first operations the row is not
     *   visible to other threads at this time, copy rows are not even visible
     * to TUP scans, thus no need to protect it here. The row becomes visible
     *   when inserted into the active list after returning from this call.
     */
    setChecksum(req_struct->m_tuple_ptr, regTabPtr);
  }

  /* Have been successful with disk + mem, update ACC to point to
   * new record if necessary
   * Failures in disk alloc will skip this part
   */
  if (update_acc) {
    /* Acc stores the local key with the frag_page_id rather
     * than the real_page_id
     */
    jamDebug();
    ndbassert(regOperPtr.p->m_tuple_location.m_page_no == real_page_id);

    Local_key accKey = regOperPtr.p->m_tuple_location;
    accKey.m_page_no = frag_page_id;
    **accminupdateptr = accKey;
  } else {
    *accminupdateptr = 0;  // No accminupdate should be performed
  }

  set_tuple_state(regOperPtr.p, TUPLE_PREPARED);
  return 0;

size_change_error:
  jam();
  terrorCode = ZMEM_NOMEM_ERROR;
  goto exit_error;

trans_mem_error:
  jam();
  terrorCode = ZNO_COPY_TUPLE_MEMORY_ERROR;
  regOperPtr.p->m_undo_buffer_space = 0;
  if (mem_insert) regOperPtr.p->m_tuple_location.setNull();
  regOperPtr.p->m_copy_tuple_location = nullptr;
  tupkeyErrorLab(req_struct);
  return -1;

null_check_error:
  jam();
  terrorCode = ZNO_ILLEGAL_NULL_ATTR;
  goto update_error;

mem_error:
  jam();
  if (terrorCode == 0) {
    terrorCode = ZMEM_NOMEM_ERROR;
  }
  goto update_error;

log_space_error:
  jam();
  regOperPtr.p->m_undo_buffer_space = 0;
alloc_rowid_error:
  jam();
update_error:
  jam();
  if (mem_insert) {
    regOperPtr.p->m_tuple_location.setNull();
  }
exit_error:
  if (!regOperPtr.p->m_tuple_location.isNull()) {
    jam();
    /* Memory allocated, abort insert, releasing memory if appropriate */
    signal->theData[0] = regOperPtr.i;
    do_tup_abortreq(signal, ZSKIP_TUX_TRIGGERS | ZABORT_DEALLOC);
  }
  tupkeyErrorLab(req_struct);
  return -1;

disk_prealloc_error:
  jam();
  base->m_header_bits |= Tuple_header::FREE;
  setInvalidChecksum(base, regTabPtr);
  release_frag_mutex(regFragPtr, frag_page_id, jamBuffer());
  c_lqh->upgrade_to_exclusive_frag_access_no_return();
  goto exit_error;
}

/* ---------------------------------------------------------------- */
/* ---------------------------- DELETE ---------------------------- */
/* ---------------------------------------------------------------- */
int Dbtup::handleDeleteReq(Signal* signal,
                           Operationrec* regOperPtr,
                           Fragrecord* regFragPtr,
                           Tablerec* regTabPtr,
                           KeyReqStruct *req_struct,
                           bool disk)
{
  if (unlikely(regOperPtr->ttl_ignore == 0
               && regOperPtr->ttl_only_expired == 1)) {
    ndbassert(req_struct->fragPtrP != nullptr);
    if (!c_lqh->is_ttl_table(req_struct->fragPtrP->fragTableId)) {
      g_eventLogger->warning("(Delete) Received an delete request with "
                             "ttl_only_expired on a non-TTL table: %d",
                             req_struct->fragPtrP->fragTableId);
      // return Notfound
      terrorCode = 626;
      tupkeyErrorLab(req_struct);
      return -1;
    }
  }

  /*
   * TTL related
   * Here we check whether the row is expired
   */
  if (regOperPtr->ttl_ignore == 1) {
    TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                    "(Delete) Skip checking TTL since "
                    "ttl ignore is set");
  }

  if (regOperPtr->ttl_ignore == 0 &&
      is_ttl_table(regTabPtr)) {
    bool has_error = false;
    int err_no = 0;
    int cmp_ret = 0;
    TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                    "(DELETE) handleDeleteReq TTL check");
    cmp_ret = checkTTL(regTabPtr, req_struct, &has_error, &err_no);
    if (!has_error) {
      if (cmp_ret <= 0) {
        /*
         * TTL related
         * 1. Normal deletion on an already existing but expired row
         */
        TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId, "(DELETE) TTL expired");
        terrorCode = 626; // HA_ERR_KEY_NOT_FOUND
        tupkeyErrorLab(req_struct);
        return -1;
      }
    } else {
      jam();
      ndbrequire(err_no < 0);
      terrorCode = Uint32(-err_no);
      tupkeyErrorLab(req_struct);
      return -1;
    }
  }

#ifdef TTL_DEBUG
  if (!is_ttl_table(regTabPtr)) {
    TTL_RONDB_TRACE(req_struct->fragPtrP->fragTableId,
                    "(DELETE) delete a row on non-primary table %u",
                    req_struct->fragPtrP->fragTableId);
  }
#endif  // TTL_DEBUG
  Uint32 copy_bits = 0;
  Tuple_header* dst = alloc_copy_tuple(
    regTabPtr, &regOperPtr->m_copy_tuple_location, false);
  if (unlikely(dst == 0))
  {
    jam();
    terrorCode = ZNO_COPY_TUPLE_MEMORY_ERROR;
    goto error;
  }

  DEB_COPY_TUPLE(("(%u) alloc_copy_tuple: 0x%p. line: %u",
    instance(), regOperPtr->m_copy_tuple_location, __LINE__));

  // delete must set but not increment tupVersion
  if (unlikely(!regOperPtr->is_first_operation())) {
    jam();
    Operationrec *prevOp = req_struct->prevOpPtr.p;
    regOperPtr->op_struct.bit_field.tupVersion =
        prevOp->op_struct.bit_field.tupVersion;
    // make copy since previous op is committed before this one
    const Tuple_header *org = get_copy_tuple(prevOp->m_copy_tuple_location);
    Uint32 len = regTabPtr->total_rec_size -
      Uint32(((Uint32*)dst) - regOperPtr->m_copy_tuple_location);
    memcpy(dst, org, 4 * len);
    req_struct->m_tuple_ptr = dst;
    copy_bits = org->m_header_bits;
    if (regTabPtr->m_no_of_disk_attributes)
    {
      ndbrequire(disk);
      memcpy(dst->get_disk_ref_ptr(regTabPtr),
          req_struct->m_tuple_ptr->get_disk_ref_ptr(regTabPtr),
          sizeof(Local_key));
    }
  }
  else
  {
    regOperPtr->op_struct.bit_field.tupVersion=
      req_struct->m_tuple_ptr->get_tuple_version();
    dst->m_header_bits = req_struct->m_tuple_ptr->m_header_bits;
    copy_bits = dst->m_header_bits;
    if (regTabPtr->m_no_of_disk_attributes)
    {
      ndbrequire(disk);
      memcpy(dst->get_disk_ref_ptr(regTabPtr),
          req_struct->m_tuple_ptr->get_disk_ref_ptr(regTabPtr),
          sizeof(Local_key));
    }
  }
  req_struct->changeMask.set();
  set_change_mask_info(regTabPtr, get_change_mask_ptr(regTabPtr, dst));

  if (disk && regOperPtr->m_undo_buffer_space == 0) {
    jam();
    ndbrequire(!(copy_bits & Tuple_header::DISK_ALLOC));
    regOperPtr->op_struct.bit_field.m_wait_log_buffer = 1;
    regOperPtr->op_struct.bit_field.m_load_diskpage_on_commit = 1;
    /**
     * Arriving here we cannot have the flag DISK_ALLOC set since
     * this would require m_undo_buffer_space to be set > 0.
     *
     * The length of the disk part is retrieved in get_dd_info,
     * this method retrieves a few other things that we are not
     * interested in here, so use dummy variables for those.
     *
     * Even if we do multiple insert-delete pairs and even updates
     * in between if this DELETE becomes the final delete, it will
     * always use the UNDO information from the stored row, thus
     * even for multi-row operations we retrieve the size of the
     * UNDO log information from the stored row.
     *
     * We calculate the space to write into the UNDO log here. We
     * allocate space in the UNDO log files here to ensure that
     * there is space for the UNDO log in the files. At commit time
     * we need to allocate space in the log buffer before actually
     * writing the UNDO log. The actual write to the UNDO log happens
     * as a background task that writes from the UNDO log buffer.
     */
    Uint32 undo_len;
    if ((regTabPtr->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
    {
      jamDebug();
      undo_len = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) +
        (regTabPtr->m_offsets[DD].m_fix_header_size - 1);
    }
    else
    {
      jamDebug();
      Local_key key;
      const Uint32 *disk_ref = dst->get_disk_ref_ptr(regTabPtr);
      memcpy(&key, disk_ref, sizeof(key));
      key.m_page_no= req_struct->m_disk_page_ptr.i;
      jamData(key.m_page_idx);
      ndbrequire(key.m_page_idx < Tup_page::DATA_WORDS);
      Uint32 disk_len = 0;
      Uint32 *src_ptr = get_dd_info(&req_struct->m_disk_page_ptr,
          key,
          regTabPtr,
          disk_len);
      (void)src_ptr;
      undo_len = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) +
        (disk_len - 1);
    }
    regOperPtr->m_undo_buffer_space = undo_len;
    jamDebug();
    jamDataDebug(regOperPtr->m_undo_buffer_space);
    {
      D("Logfile_client - handleDeleteReq");
      Logfile_client lgman(this, c_lgman, regFragPtr->m_logfile_group_id);
      DEB_LCP_LGMAN(("(%u)alloc_log_space(%u): %u",
            instance(),
            __LINE__,
            regOperPtr->m_undo_buffer_space));
      terrorCode= lgman.alloc_log_space(regOperPtr->m_undo_buffer_space,
          true,
          !req_struct->m_nr_copy_or_redo,
          jamBuffer());
    }
    jamDataDebug(regOperPtr->m_undo_buffer_space);
    if (unlikely(terrorCode))
    {
      jam();
      regOperPtr->m_undo_buffer_space = 0;
      goto error;
    }
  }

  set_tuple_state(regOperPtr, TUPLE_PREPARED);

  if (req_struct->attrinfo_len == 0) {
    return 0;
  }

  if (regTabPtr->need_expand(disk)) {
    prepare_read(req_struct, regTabPtr, disk);
  }

  {
    /* Delete happens in LDM thread, so no need to protect it */
    if (unlikely(((regTabPtr->m_bits & Tablerec::TR_Checksum) &&
            (calculateChecksum(req_struct->m_tuple_ptr, regTabPtr) != 0)) ||
          ERROR_INSERTED(4036)))
    {
      jam();
      return corruptedTupleDetected(req_struct, regTabPtr);
    }
    return handleReadReq(signal, regOperPtr, regTabPtr, req_struct);
  }

error:
  tupkeyErrorLab(req_struct);
  return -1;
}

int
Dbtup::handleRefreshReq(Signal* signal,
                        Ptr<Operationrec> regOperPtr,
                        FragrecordPtr regFragPtr,
                        Tablerec* regTabPtr,
                        KeyReqStruct *req_struct,
                        bool disk)
{
  /* Here we setup the tuple so that a transition to its current
   * state can be observed by SUMA's detached triggers.
   *
   * If the tuple does not exist then we fabricate a tuple
   * so that it can appear to be 'deleted'.
   *   The fabricated tuple may have invalid NULL values etc.
   * If the tuple does exist then we fabricate a null-change
   * update to the tuple.
   *
   * The logic differs depending on whether there are already
   * other operations on the tuple in this transaction.
   * No other operations (including Refresh) are allowed after
   * a refresh.
   */
  Uint32 refresh_case;
  if (likely(regOperPtr.p->is_first_operation())) {
    jam();
    if (Local_key::isInvalid(req_struct->frag_page_id,
          regOperPtr.p->m_tuple_location.m_page_idx))
    {
      jam();
      refresh_case = Operationrec::RF_SINGLE_NOT_EXIST;
      /**
       * This is refresh of non-existing tuple...
       *   i.e "delete", reuse initial insert
       */
      Local_key accminupdate;
      Local_key *accminupdateptr = &accminupdate;

      /**
       * We don't need ...in this scenario
       * - disk
       * - default values
       *
       * We signal this to handleInsertReq with is_refresh flag
       * set to true.
       */
      regOperPtr.p->op_type = ZINSERT;

      int res = handleInsertReq(signal, regOperPtr, regFragPtr, regTabPtr,
          req_struct, &accminupdateptr, true);

      if (unlikely(res == -1)) {
        jam();
        return -1;
      }

      regOperPtr.p->op_type = ZREFRESH;

      if (accminupdateptr)
      {
        /**
         * Update ACC local-key, once *everything* has completed successfully
         */
        jamDebug();
        c_lqh->accminupdate(signal,
            regOperPtr.p->userpointer,
            accminupdateptr);
      }
    }
    else
    {
      refresh_case = Operationrec::RF_SINGLE_EXIST;
      // g_eventLogger->info("case 2");
      jam();

      Tuple_header *origTuple = req_struct->m_tuple_ptr;
      Uint32 tup_version_save = origTuple->get_tuple_version();
      {
        /* Set new row version and update the tuple header */
        Uint32 old_header = origTuple->m_header_bits;
        Uint32 new_tup_version = decr_tup_version(tup_version_save);
        origTuple->set_tuple_version(new_tup_version);
        Uint32 new_header = origTuple->m_header_bits;
        updateChecksum(origTuple, regTabPtr, old_header, new_header);
      }
      m_base_header_bits = origTuple->m_header_bits;
      int res = handleUpdateReq(signal, regOperPtr.p, regFragPtr.p,
          regTabPtr, req_struct, disk);

      /* Now we must reset the original tuple header back
       * to the original version.
       * The copy tuple will have the correct version due to
       * the update incrementing it.
       * On commit, the tuple becomes the copy tuple.
       * On abort, the original tuple remains.  If we don't
       * reset it here, then aborts cause the version to
       * decrease
       *
       * We also need to recalculate checksum since we're changing the
       * row here.
       */
      {
        origTuple->m_header_bits = m_base_header_bits;
        Uint32 old_header = m_base_header_bits;
        origTuple->set_tuple_version(tup_version_save);
        Uint32 new_header = origTuple->m_header_bits;
        updateChecksum(origTuple, regTabPtr, old_header, new_header);
        m_base_header_bits = origTuple->m_header_bits;
      }
      if (unlikely(res == -1)) {
        jam();
        return -1;
      }
    }
  } else {
    /* Not first operation on tuple in transaction */
    jam();

    Uint32 tup_version_save =
        req_struct->prevOpPtr.p->op_struct.bit_field.tupVersion;
    Uint32 new_tup_version = decr_tup_version(tup_version_save);
    req_struct->prevOpPtr.p->op_struct.bit_field.tupVersion = new_tup_version;

    int res;
    if (req_struct->prevOpPtr.p->op_type == ZDELETE) {
      refresh_case = Operationrec::RF_MULTI_NOT_EXIST;
      jam();
      /**
       * We don't need ...in this scenario
       * - default values
       *
       * We keep disk attributes to avoid issues with 'insert'
       * We signal this to handleInsertReq with is_refresh flag
       * set to true.
       */
      regOperPtr.p->op_type = ZINSERT;

      /**
       * This is multi-update + DELETE + REFRESH
       */
      Local_key * accminupdateptr = 0;
      res = handleInsertReq(signal,
          regOperPtr,
          regFragPtr,
          regTabPtr,
          req_struct,
          &accminupdateptr,
          true);
      if (unlikely(res == -1))
      {
        jam();
        return -1;
      }
      regOperPtr.p->op_type = ZREFRESH;
    } else {
      jam();
      refresh_case = Operationrec::RF_MULTI_EXIST;
      /**
       * This is multi-update + INSERT/UPDATE + REFRESH
       */
      res = handleUpdateReq(signal, regOperPtr.p, regFragPtr.p,
          regTabPtr, req_struct, disk);
    }
    req_struct->prevOpPtr.p->op_struct.bit_field.tupVersion = tup_version_save;
    if (unlikely(res == -1)) {
      jam();
      return -1;
    }
  }
  regOperPtr.p->m_refresh_case = refresh_case;
  return 0;
}

bool
Dbtup::checkNullAttributes(KeyReqStruct * req_struct,
                           Tablerec* regTabPtr,
                           bool is_refresh)
{
  // Implement checking of updating all not null attributes in an insert here.
  Bitmask<MAXNROFATTRIBUTESINWORDS> attributeMask;  
  /* 
   * The idea here is maybe that changeMask is not-null attributes
   * and must contain notNullAttributeMask.  But:
   *
   * 1. changeMask has all bits set on insert
   * 2. not-null is checked in each UpdateFunction
   * 3. the code below does not work except trivially due to 1.
   *
   * XXX remove or fix
   */
  attributeMask.clear();
  attributeMask.bitOR(req_struct->changeMask);
  if (unlikely(is_refresh)) {
    /**
     * Update notNullAttributeMask  to only include primary keys
     */
    Bitmask<MAXNROFATTRIBUTESINWORDS> tableMask;
    tableMask.clear();
    const Uint32 * primarykeys = regTabPtr->readKeyArray;
    for (Uint32 i = 0; i<regTabPtr->noOfKeyAttr; i++)
      tableMask.set(primarykeys[i] >> 16);
    attributeMask.bitAND(tableMask);
    attributeMask.bitXOR(tableMask);
  } else {
    attributeMask.bitAND(regTabPtr->notNullAttributeMask);
    attributeMask.bitXOR(regTabPtr->notNullAttributeMask);
  }
  if (!attributeMask.isclear()) {
    return false;
  }
  return true;
}

/* ---------------------------------------------------------------- */
/* THIS IS THE START OF THE INTERPRETED EXECUTION OF UPDATES. WE    */
/* START BY LINKING ALL ATTRINFO'S IN A DOUBLY LINKED LIST (THEY ARE*/
/* ALREADY IN A LINKED LIST). WE ALLOCATE A REGISTER MEMORY (EQUAL  */
/* TO AN ATTRINFO RECORD). THE INTERPRETER GOES THROUGH FOUR  PHASES*/
/* DURING THE FIRST PHASE IT IS ONLY ALLOWED TO READ ATTRIBUTES THAT*/
/* ARE SENT TO THE CLIENT APPLICATION. DURING THE SECOND PHASE IT IS*/
/* ALLOWED TO READ FROM ATTRIBUTES INTO REGISTERS, TO UPDATE        */
/* ATTRIBUTES BASED ON EITHER A CONSTANT VALUE OR A REGISTER VALUE, */
/* A DIVERSE SET OF OPERATIONS ON REGISTERS ARE AVAILABLE AS WELL.  */
/* IT IS ALSO POSSIBLE TO PERFORM JUMPS WITHIN THE INSTRUCTIONS THAT*/
/* BELONGS TO THE SECOND PHASE. ALSO SUBROUTINES CAN BE CALLED IN   */
/* THIS PHASE. THE THIRD PHASE IS TO AGAIN READ ATTRIBUTES AND      */
/* FINALLY THE FOURTH PHASE READS SELECTED REGISTERS AND SEND THEM  */
/* TO THE CLIENT APPLICATION.                                       */
/* THERE IS A FIFTH REGION WHICH CONTAINS SUBROUTINES CALLABLE FROM */
/* THE INTERPRETER EXECUTION REGION.                                */
/* THE FIRST FIVE WORDS WILL GIVE THE LENGTH OF THE FIVE REGIONS    */
/*                                                                  */
/* THIS MEANS THAT FROM THE APPLICATIONS POINT OF VIEW THE DATABASE */
/* CAN HANDLE SUBROUTINE CALLS WHERE THE CODE IS SENT IN THE REQUEST*/
/* THE RETURN PARAMETERS ARE FIXED AND CAN EITHER BE GENERATED      */
/* BEFORE THE EXECUTION OF THE ROUTINE OR AFTER.                    */
/*                                                                  */
/* IN LATER VERSIONS WE WILL ADD MORE THINGS LIKE THE POSSIBILITY   */
/* TO ALLOCATE MEMORY AND USE THIS AS LOCAL STORAGE. IT IS ALSO     */
/* IMAGINABLE TO HAVE SPECIAL ROUTINES THAT CAN PERFORM CERTAIN     */
/* OPERATIONS ON BLOB'S DEPENDENT ON WHAT THE BLOB REPRESENTS.      */
/*                                                                  */
/*                                                                  */
/*       -----------------------------------------                  */
/*       +   INITIAL READ REGION                 +                  */
/*       -----------------------------------------                  */
/*       +   INTERPRETED EXECUTE  REGION         +                  */
/*       -----------------------------------------                  */
/*       +   FINAL UPDATE REGION                 +                  */
/*       -----------------------------------------                  */
/*       +   FINAL READ REGION                   +                  */
/*       -----------------------------------------                  */
/*       +   SUBROUTINE REGION (or parameter)    +                  */
/*       -----------------------------------------                  */
/*                                                                  */
/* For read operations it only makes sense to first perform the     */
/* interpreted execution (this will perform condition pushdown      */
/* where we evaluate the conditions that are not evaluated by       */
/* ranges implied by the scan operation. These conditions pushed    */
/* down can essentially check any type of condition.                */
/*                                                                  */
/* Since it only makes sense to interpret before reading we delay   */
/* the initial read to after interpreted execution for read         */
/* operations. This is safe from a protocol point of view since the */
/* interpreted execution cannot generate Attrinfo data.             */
/*                                                                  */
/* For updates it still makes sense to handle initial read and      */
/* final read separately since we might want to read values before  */
/* and after changes, the interpreter can write column values.      */
/**
 * Extract linked column data from cinBuffer sub-region, reset
 * read_length, and call handleJoinAggRow.
 */
int Dbtup::prepareAndHandleJoinAggRow(KeyReqStruct *req_struct,
                                      Uint32 RsubLen) {
  const Uint32 *linked_data = nullptr;
  Uint32 linked_len = 0;
  if (RsubLen > 0) {
    const Uint32 *sub_start =
        &cinBuffer[5 + cinBuffer[0] + cinBuffer[1] +
                    cinBuffer[2] + cinBuffer[3]];
    // Skip the paramLen word prepended by DBSPJ T_ATTRINFO_CONSTRUCTED
    linked_data = sub_start + 1;
    linked_len = RsubLen - 1;
  }
#ifdef DEBUG_JOIN_AGG_TRACE
  DEB_JOIN_AGG(("(%u)DBTUP prepareAndHandleJoinAggRow: "
                 "cinBuffer header: initRead=%u interp=%u "
                 "finalUpd=%u finalRead=%u subLen=%u  "
                 "linked_len=%u",
                 instance(),
                 cinBuffer[0],
                 cinBuffer[1],
                 cinBuffer[2],
                 cinBuffer[3],
                 cinBuffer[4],
                 linked_len));
  if (linked_data && linked_len > 0) {
    const Uint32 *p = linked_data;
    const Uint32 *p_end = linked_data + linked_len;
    Uint32 idx = 0;
    while (p < p_end) {
      Uint32 hdr = *p;
      Uint32 attrId = AttributeHeader::getAttributeId(hdr);
      Uint32 dSz = AttributeHeader::getDataSize(hdr);
      DEB_JOIN_AGG(("(%u)DBTUP  linked[%u]: AttrHeader=0x%08x "
                     "(attrId=%u dataSize=%u)",
                     instance(), idx, hdr, attrId, dSz));
      p += 1 + dSz;
      idx++;
    }
  } else {
    DEB_JOIN_AGG(("(%u)DBTUP linked data: EMPTY (RsubLen=%u)",
      instance(), RsubLen));
  }
#endif
  // Reset read_length: the final-read projection (FLUSH_AI) may have
  // set it, but ProcessRec requires read_length == 0 on entry.
  req_struct->read_length = 0;
  return handleJoinAggRow(req_struct, linked_data, linked_len);
}

/**
 * handleJoinAggRow
 *
 * Feed a row into the join aggregation JoinAggInterpreter instead of
 * sending via TRANSID_AI.  Selects the correct interpreter based
 * on the concurrency strategy and increments m_completed_ops.
 *
 * If the interpreter returns AGG_EVICT_NEEDED (group map is full),
 * evicts one group by sending it via TRANSID_AI, then retries.
 *
 * Returns 0 on success, or the TUPKEY_abort return value on error.
 */
int Dbtup::handleJoinAggRow(KeyReqStruct *req_struct,
                            const Uint32 *linked_data,
                            Uint32 linked_len) {
  Uint32 encodedKey = req_struct->m_join_agg_state_key;
  Uint32 baseKey = JoinAggregationState::decodeBaseKey(encodedKey);
  Uint32 leafIndex = JoinAggregationState::decodeLeafIndex(encodedKey);

  JoinAggregationState *state = getJoinAggState(baseKey);
  ndbrequire(state != nullptr);
  ndbrequire(leafIndex < state->m_num_leaves);
  JoinAggInterpreter *interp = c_lqh->getJoinAggInterpreter(state);

  DEB_STAR_AGG(("(%u)DBTUP STAR_AGG handleJoinAggRow: encodedKey=0x%08x"
                " baseKey=%u leafIndex=%u num_leaves=%u n_gb_cols=%u"
                " linked_len=%u",
                instance(),
                encodedKey,
                baseKey,
                leafIndex,
                state->m_num_leaves,
                interp->n_gb_cols(),
                linked_len));

  // For multi-leaf, pass leaf program to processRecWithLinkedAttrs
  // which switches under mutex protection for MUTEX_BASED.
  const LeafProgram *leaf = nullptr;
  if (state->m_num_leaves > 1) {
    leaf = &state->m_leaf_programs[leafIndex];
    DEB_STAR_AGG(("(%u)DBTUP STAR_AGG leaf=%u progLen=%u "
                  "prog_start=%u acc_offset=%u n_agg=%u",
                  instance(),
                  leafIndex,
                  leaf->m_agg_program_len,
                  leaf->m_agg_prog_start_pos,
                  leaf->m_acc_offset,
                  leaf->m_n_agg_results));
  }

  Uint32 evict_count = 0;

retry:
  Int32 ret = interp->processRecWithLinkedAttrs(
      this, req_struct, linked_data, linked_len, leaf);
  if (ret == AGG_EVICT_NEEDED) {
    c_lqh->sendEvictedAggGroup(req_struct->signal, interp, state);
    evict_count++;
    goto retry;
  }
  if (ret != 0) {
    return TUPKEY_abort(req_struct, ret);
  }
  state->m_completed_ops.fetch_add(1, std::memory_order_relaxed);

#ifdef ERROR_INSERT
  if (ERROR_INSERTED(4040) &&
      interp->gb_map_mutable() != nullptr &&
      interp->gb_map_mutable()->size() > 2 &&
      (state->m_completed_ops.load(std::memory_order_relaxed) % 7) == 0) {
    c_lqh->sendEvictedAggGroup(req_struct->signal, interp, state);
    evict_count++;
  }
#endif

  req_struct->read_length = evict_count;
  return 0;
}

/* ---------------------------------------------------------------- */
/* ---------------------------------------------------------------- */
/* ----------------- INTERPRETED EXECUTION  ----------------------- */
/* ---------------------------------------------------------------- */
int Dbtup::interpreterStartLab(Signal *signal, KeyReqStruct *req_struct) {
  Operationrec *const regOperPtr = req_struct->operPtrP;
  int TnoDataRW;
  Uint32 RtotalLen, dstLen;
  Uint32 *dst;

  Uint32 RinitReadLen = cinBuffer[0];
  Uint32 RexecRegionLen = cinBuffer[1];
  Uint32 RfinalUpdateLen = cinBuffer[2];
  Uint32 RfinalRLen = cinBuffer[3];
  Uint32 RsubLen = cinBuffer[4];

  jamDebug();

  Uint32 RattrinbufLen = req_struct->attrinfo_len;

  dst = &signal->theData[25];
  dstLen = (MAX_READ / 4) - 25;

  RtotalLen= RinitReadLen;
  RtotalLen += RexecRegionLen;
  RtotalLen += RfinalUpdateLen;
  RtotalLen += RfinalRLen;
  RtotalLen += RsubLen;

  Uint32 RattroutCounter = 0;
  Uint32 RinstructionCounter = 5;

  /* All information to be logged/propagated to replicas
   * is generated from here on so reset the log word count
   *
   * Note that in case attrInfo contain multiple params for the
   * interpreterCode, we will only copy one of them into the cinBuffer[].
   * Thus, 'RtotalLen + 5' may be '<' than RattrinbufLen.
   */
  req_struct->log_size = 0;
  req_struct->m_write_log_memory_in_update = true;
  Uint32 op_type = regOperPtr->op_type;

  // bool debug_print = false;
  // if (req_struct->fragPtrP != nullptr &&
  //     PA_NEED_PRINT(true,
  //       req_struct->fragPtrP->fragTableId,
  //       req_struct->fragPtrP->fragmentId)) {
  //   debug_print = true;
  // }
  // if (debug_print) {
  //   g_eventLogger->info("Zhao interpreterStartLab, %u, %u, %u, %u, %u, %u, %u\n",
  //       RinitReadLen, RexecRegionLen, RfinalUpdateLen, RfinalRLen, RsubLen,
  //       RtotalLen, RattrinbufLen);
  // }
  if (likely(((RtotalLen + 5) <= RattrinbufLen) &&
        (RattrinbufLen >= 5) &&
        (RtotalLen + 5 < ZATTR_BUFFER_SIZE))) {
    /* ---------------------------------------------------------------- */
    // We start by checking consistency. We must have the first five
    // words of the ATTRINFO to give us the length of the regions. The
    // size of these regions must be the same as the total ATTRINFO
    // length and finally the total length must be within the limits.
    /* ---------------------------------------------------------------- */

    Uint32 inputParamLen = 0;
    if (unlikely(RinitReadLen > 0 &&
        (cinBuffer[5] >> 16) == 0xFFFF)) {
      inputParamLen = cinBuffer[5] & 0xFFFF;
#ifdef TRACE_INTERPRETER
      g_eventLogger->info("(%u) %u words for input parameters",
        instance(), inputParamLen);
#endif
      if (inputParamLen > RinitReadLen ||
          inputParamLen < 4 ||
          inputParamLen > (1 + MAX_INPUT_PARAMS * 3)) {
        jam();
        if (inputParamLen > RinitReadLen ||
            inputParamLen < 4)
          terrorCode = ZINCONSISTENCY_INPUT_PARAM;
        else
          terrorCode = ZTOO_MUCH_INPUT_PARAM;
        tupkeyErrorLab(req_struct);
        return -1;
      }
      int ret = setInputParameters(req_struct,
                                   &cinBuffer[5],
                                   inputParamLen);
      if (ret < 0) {
        terrorCode = Uint32(-ret);
        tupkeyErrorLab(req_struct);
        return -1;
      }
      RinitReadLen -= inputParamLen;
      RinstructionCounter += inputParamLen;
    }
    if (likely(RinitReadLen > 0)) {
      if (likely(op_type == ZREAD)) {
        jamDebug();
        RinstructionCounter += RinitReadLen;
      } else {
        jamDebug();
#ifdef TRACE_INTERPRETER
        g_eventLogger->info("(%u) %u words for initial read",
          instance(), RinitReadLen);
#endif
        /* ---------------------------------------------------------------- */
        // The first step that can be taken in the interpreter is to read
        // data of the tuple before any updates have been applied.
        /* ---------------------------------------------------------------- */
        TnoDataRW = readAttributes(req_struct,
                                   &cinBuffer[5 + inputParamLen],
                                   RinitReadLen,
                                   &dst[0],
                                   dstLen);
        if (TnoDataRW >= 0) {
          jamDebug();
          RattroutCounter = TnoDataRW;
          RinstructionCounter += RinitReadLen;
          RinitReadLen = 0;
        } else {
          jam();
          terrorCode = Uint32(-TnoDataRW);
          tupkeyErrorLab(req_struct);
          return -1;
        }
      }
    }
    if (RexecRegionLen > 0) {
      jamDebug();
#ifdef TRACE_INTERPRETER
      g_eventLogger->info("(%u) %u words for interpreter",
        instance(), RexecRegionLen);
#endif
      /* ---------------------------------------------------------------- */
      // The next step is the actual interpreted execution. This executes
      // a register-based virtual machine which can read and write attributes
      // to and from registers.
      /* ---------------------------------------------------------------- */
      Uint32 RsubPC= RinstructionCounter + RexecRegionLen 
        + RfinalUpdateLen + RfinalRLen;
      TnoDataRW= interpreterNextLab(signal,
          req_struct,
          &cinBuffer[RinstructionCounter],
          RexecRegionLen,
          &cinBuffer[RsubPC],
          RsubLen,
          &coutBuffer[0],
          sizeof(coutBuffer) / 4);
      if (TnoDataRW != -1)
      {
        jamDebug();
        RinstructionCounter += RexecRegionLen;
      } else {
        jamDebug();
        /**
         * TUPKEY REF is sent from within interpreter
         */
        return -1;
      }
    }

    if (((req_struct->log_size > 0) && (op_type != ZDELETE)) ||
         (RfinalUpdateLen > 0)) {
      jamDebug();
      /* Operation updates row,
       * reset author pseudo-col before update takes effect
       * This should probably occur only if the interpreted program
       * did not explicitly write the value, but that requires a bit
       * to record whether the value has been written.
       */
      Tablerec *regTabPtr = req_struct->tablePtrP;
      Tuple_header *dst = req_struct->m_tuple_ptr;

      if (unlikely(regTabPtr->m_bits & Tablerec::TR_ExtraRowAuthorBits)) {
        Uint32 attrId =
            regTabPtr->getExtraAttrId<Tablerec::TR_ExtraRowAuthorBits>();

        store_extra_row_bits(attrId, regTabPtr, dst, /* default */ 0, false);
      }
    }

    if (unlikely(RfinalUpdateLen > 0)) {
      /* ---------------------------------------------------------------- */
      // We can also apply a set of updates without any conditions as part
      // of the interpreted execution.
      /* ---------------------------------------------------------------- */
#ifdef TRACE_INTERPRETER
      g_eventLogger->info("(%u) %u words for final update",
        instance(), RfinalUpdateLen);
#endif
      if (op_type == ZUPDATE || op_type == ZINSERT) {
        jamDebug();
        TnoDataRW= updateAttributes(req_struct,
            &cinBuffer[RinstructionCounter],
            RfinalUpdateLen);
        if (TnoDataRW >= 0)
        {
          jamDebug();
          RinstructionCounter += RfinalUpdateLen;
        } else {
          jam();
          terrorCode = Uint32(-TnoDataRW);
          tupkeyErrorLab(req_struct);
          return -1;
        }
      } else {
        jamDebug();
        return TUPKEY_abort(req_struct, ZTRY_TO_UPDATE_ERROR);
      }
    }

    // VS related
    Uint32 vec_max_rec_size = 0;
    Uint32* vec_max_rec_size_ptr = nullptr;
    bool get_vec_max_rec_size = false;
    {
    if (req_struct->scan_rec != nullptr) {
      Dblqh::ScanRecord* scan_rec_ptr =
                    reinterpret_cast<Dblqh::ScanRecord*>(req_struct->scan_rec);
        if (unlikely(scan_rec_ptr->m_has_pushdown == true &&
            scan_rec_ptr->m_vs_interpreter != nullptr &&
            !scan_rec_ptr->m_vs_interpreter->IsCandidateBufAllocated())) {

          vec_max_rec_size_ptr = &vec_max_rec_size;
          get_vec_max_rec_size = true;
      }
    }
    }
    if (likely(RinitReadLen > 0)) {
      jamDebug();
#ifdef TRACE_INTERPRETER
      g_eventLogger->info("(%u) %u words for initial read after interpreter",
        instance(), RinitReadLen);
#endif
      // if (debug_print) {
      //   g_eventLogger->info("RinitReadLen %u, inputParamLen: %u, [%d], req_struct->out_buf_index: %u\n",
      //       RinitReadLen, inputParamLen, cinBuffer[5 + inputParamLen], req_struct->out_buf_index);
      // }
      TnoDataRW = readAttributes(req_struct,
                                 &cinBuffer[5 + inputParamLen],
                                 RinitReadLen,
                                 &dst[0],
                                 dstLen,
                                 vec_max_rec_size_ptr);
      // if (debug_print) {
      //   g_eventLogger->info("TnoDataRw %u, dst: %u %u\n", TnoDataRW, dst[0], dst[1]);
      // }
      if (TnoDataRW >= 0) {
        jamDebug();
        RattroutCounter = TnoDataRW;
      } else {
        jam();
        terrorCode = Uint32(-TnoDataRW);
        tupkeyErrorLab(req_struct);
        return -1;
      }
    }
    if (RfinalRLen > 0) {
      jamDebug();
#ifdef TRACE_INTERPRETER
      g_eventLogger->info("(%u) %u words for final read",
        instance(), RfinalRLen);
#endif
      /* ---------------------------------------------------------------- */
      // The final action is that we can also read the tuple after it has
      // been updated.
      /* ---------------------------------------------------------------- */
      TnoDataRW = readAttributes(req_struct, &cinBuffer[RinstructionCounter],
                                 RfinalRLen, &dst[RattroutCounter],
                                 (dstLen - RattroutCounter));
      if (TnoDataRW >= 0) {
        jamDebug();
        RattroutCounter += TnoDataRW;
      } else {
        jam();
        terrorCode = Uint32(-TnoDataRW);
        tupkeyErrorLab(req_struct);
        return -1;
      }
    }
    /* Add log words explicitly generated here to existing log size
     *  - readAttributes can generate log for ANYVALUE column
     *    It adds the words directly to req_struct->log_size
     *    This is used for ANYVALUE and interpreted delete.
     */
    if (req_struct->scan_rec != nullptr) {
      Dblqh::ScanRecord* scan_rec_ptr =
                    reinterpret_cast<Dblqh::ScanRecord*>(req_struct->scan_rec);
      // Moz
      if (scan_rec_ptr->m_has_pushdown == true) {
        ndbrequire(scan_rec_ptr->m_agg_interpreter != nullptr ||
                   scan_rec_ptr->m_vs_interpreter != nullptr);
        if (unlikely(get_vec_max_rec_size)) {
          /*
           * VS related
           * We’ve already calculated the theoretical maximum result record size.
           * Now it’s time to pass it to m_vs_interpreter to guide it in preallocating
           * the buffer for maintaining the top-k vector search results.
           */
          ndbrequire(scan_rec_ptr->m_vs_interpreter != nullptr);
          scan_rec_ptr->m_vs_interpreter->set_vec_max_rec_size(vec_max_rec_size);
        }
        /*
         * update req_struct->read_length here, which will update the
         * Dblqh::ScanRecord::m_curr_batch_size_bytes later in the
         * Dblqh::scanTupkeyConfLab, even we don’t use that variable
         * to decide whether reaches batch limitation. For aggregation,
         * we use Dblqh::ScanRecord::m_agg_curr_batch_size_bytes.
         * req_struct->read_length would be updated in ProcessRec().
         */
        bool vec_update_candidate = false;
        int ret = 0;
        if (scan_rec_ptr->m_agg_interpreter != nullptr) {
          ret = scan_rec_ptr->m_agg_interpreter->ProcessRec(this, req_struct);
        } else {
          ret = scan_rec_ptr->m_vs_interpreter->ProcessRec(this, req_struct,
                                                         &vec_update_candidate);
        }
        if (ret != 0) {
          return TUPKEY_abort(req_struct, ret);
        }
        if (scan_rec_ptr->m_agg_interpreter != nullptr) {
          /* PA path */
          Uint32 res_len = scan_rec_ptr->m_agg_interpreter->
            PrepareAggResIfNeeded(signal, false);
          if (res_len != 0) {
            ndbrequire(req_struct->agg_curr_batch_size_rows == 0);
            ndbrequire(req_struct->agg_curr_batch_size_bytes == 0);
            req_struct->agg_curr_batch_size_rows = 1;
            req_struct->agg_curr_batch_size_bytes = res_len * sizeof(Uint32);
            TransIdAI * transIdAI=  (TransIdAI *)signal->getDataPtrSend();
            transIdAI->connectPtr = req_struct->tc_operation_ptr;
            transIdAI->transId[0] = req_struct->trans_id1;
            transIdAI->transId[1] = req_struct->trans_id2;
            SendAggregationResult(signal, res_len, req_struct->rec_blockref);
          }
          req_struct->agg_n_res_recs = scan_rec_ptr->
            m_agg_interpreter->NumOfResRecords();
        } else if (vec_update_candidate) {
          /* VS path */
          int ret = scan_rec_ptr->m_vs_interpreter->
                             CopyVecCandidateFromSignal(signal,
                                              RattroutCounter);
          if (ret != 0) {
            return TUPKEY_abort(req_struct, ret);
          }
        }
        return 0;
      } else if (req_struct->m_join_agg_state_key != RNIL) {
        jamDebug();
        int res = prepareAndHandleJoinAggRow(req_struct, RsubLen);
        if (res != 0) return res;
      } else {
        sendReadAttrinfo(signal, req_struct, RattroutCounter);
      }
    } else {
      if (req_struct->m_join_agg_state_key != RNIL) {
        jamDebug();
        int res = prepareAndHandleJoinAggRow(req_struct, RsubLen);
        if (res != 0) return res;
      } else {
        sendReadAttrinfo(signal, req_struct, RattroutCounter);
      }
    }
    if (req_struct->log_size > 0)
    {
      jamDebug();
      return sendLogAttrinfo(signal, req_struct, regOperPtr);
    }
    return 0;
  } else {
    return TUPKEY_abort(req_struct, ZTOTAL_LEN_ERROR);
  }
}

void Dbtup::SendAggregationResult(Signal* signal, Uint32 res_len,
                                 BlockReference api_blockref) {
  ndbassert(refToMain(api_blockref) != 32770);
  const Uint32 nodeId = refToNode(api_blockref);

  bool connectedToNode = getNodeInfo(nodeId).m_connected;
  const Uint32 type = getNodeInfo(nodeId).m_type;
  const bool is_api = (type >= NodeInfo::API && type <= NodeInfo::MGM);
  ndbrequire(is_api);
  ndbrequire(nodeId != getOwnNodeId());
  ndbrequire(connectedToNode);

  LinearSectionPtr ptr[3];
  ptr[0].p = const_cast<Uint32*>(&signal->theData[25]);
  ptr[0].sz = res_len;
  if (res_len <= MAX_TRANSID_AI_SIZE) {
    sendSignal(api_blockref, GSN_TRANSID_AI, signal,
               TransIdAI::HeaderLength, JBB, ptr, 1);
  } else {
    TransIdAILong *const transIdAILong = (TransIdAILong*)signal->getDataPtr();
    transIdAILong->totalLen = res_len;
    sendBatchedFragmentedSignal(api_blockref, GSN_TRANSID_AI, signal,
       TransIdAILong::HeaderLength, JBB, ptr, 1);
  }
}

bool
Dbtup::writeLogMemory(KeyReqStruct *req_struct,
                      const char *input_ptr,
                      Uint32 byte_size) {
  Uint32 logSize = req_struct->log_size;
  Uint32 words = (byte_size + 3) / 4;
  if (unlikely((logSize + words) > MAX_LOG_RECORD_SIZE_WORDS)) {
    return false;
  }
  Uint8 *current_log_ptr = (Uint8*)&clogMemBuffer[logSize];
  memcpy(current_log_ptr,
         input_ptr,
         byte_size);
  zero32(current_log_ptr, byte_size);
  req_struct->log_size += words;
  jamDebug();
  jamDataDebug(req_struct->log_size);
  return true;
}

/* ---------------------------------------------------------------- */
/*       WHEN EXECUTION IS INTERPRETED WE NEED TO SEND SOME ATTRINFO*/
/*       BACK TO LQH FOR LOGGING AND SENDING TO BACKUP AND STANDBY  */
/*       NODES.                                                     */
/*       INPUT:  LOG_ATTRINFOPTR         WHERE TO FETCH DATA FROM   */
/*               TLOG_START              FIRST INDEX TO LOG         */
/*               TLOG_END                LAST INDEX + 1 TO LOG      */
/* ---------------------------------------------------------------- */
int Dbtup::sendLogAttrinfo(Signal* signal,
                           KeyReqStruct * req_struct,
                           Operationrec *  const regOperPtr)
{
  /* Copy from Log buffer to segmented section,
   * then attach to ATTRINFO and execute direct
   * to LQH
   */
  Uint32 TlogSize = req_struct->log_size;
  jamDebug();
  jamDataDebug(TlogSize);
  ndbrequire( TlogSize > 0 );
  ndbassert(!m_is_query_block);
  Uint32 longSectionIVal= RNIL;
  bool ok= appendToSection(longSectionIVal, 
      &clogMemBuffer[0],
      TlogSize);
  if (unlikely(!ok))
  {
    /* Resource error, abort transaction */
    terrorCode = ZSEIZE_ATTRINBUFREC_ERROR;
    tupkeyErrorLab(req_struct);
    return -1;
  }

  /* Send a TUP_ATTRINFO signal to LQH, which contains
   * the relevant user pointer and the attrinfo section's
   * IVAL
   */
  signal->theData[0] = regOperPtr->userpointer;
  signal->theData[1] = TlogSize;
  signal->theData[2] = longSectionIVal;

#ifdef TRACE_INTERPRETER
  g_eventLogger->info("(%u) log_size: %u, first words: %x,%x,%x",
    instance(),
    TlogSize,
    clogMemBuffer[0],
    clogMemBuffer[1],
    clogMemBuffer[2]);
#endif
  c_lqh->execTUP_ATTRINFO(signal);
  return 0;
}

inline Uint32 Dbtup::brancher(Uint32 TheInstruction, Uint32 TprogramCounter) {
  Uint32 TbranchDirection = TheInstruction >> 31;
  Uint32 TbranchLength = (TheInstruction >> 16) & 0x7fff;
  TprogramCounter--;
  if (TbranchDirection == 1) {
    jamDebug();
    /* ---------------------------------------------------------------- */
    /*       WE JUMP BACKWARDS.                                         */
    /* ---------------------------------------------------------------- */
    return (TprogramCounter - TbranchLength);
  } else {
    jamDebug();
    /* ---------------------------------------------------------------- */
    /*       WE JUMP FORWARD.                                           */
    /* ---------------------------------------------------------------- */
    return (TprogramCounter + TbranchLength);
  }
}

const Uint32 *Dbtup::lookupInterpreterParameter(Uint32 paramNo,
                                                const Uint32 *subptr) const {
  /**
   * The parameters are stored in the subroutine section.
   * Each entry has format: [tableId] [tableVersion] [AH] [data...]
   * where tableId and tableVersion identify the source table of the
   * linked attribute (for schema version validation).
   */
  const Uint32 sublen = *subptr;
  ndbassert(sublen > 0);

  Uint32 pos = 1;
  while (paramNo) {
    if (unlikely(pos + 2 >= sublen)) return nullptr;
    pos += 2;  // skip tableId + tableVersion
    const Uint32 len = AttributeHeader::getDataSize(*(subptr + pos));
    paramNo--;
    pos += 1 + len;  // skip AH + data
  }
  if (unlikely(pos + 2 >= sublen)) return nullptr;
  pos += 2;  // skip tableId + tableVersion of target param
  if (unlikely(pos >= sublen)) return nullptr;
  const Uint32 *head = subptr + pos;
  const Uint32 len = AttributeHeader::getDataSize(*head);
  if (unlikely(pos + 1 + len > sublen)) return nullptr;

  return head;
}

#define HEAP_MEMORY_SIZE_DWORDS 8200
#define MAX_HEAP_OFFSET 65535
#define NULL_INDICATOR 0
#define NOT_NULL_INDICATOR 1

/* ============================================================
 * Interpreter handler infrastructure (Phase A)
 *
 * The NDB interpreter's main loop (interpreterNextLab) has ~117 case
 * handlers in a large switch statement. To enable alternative dispatch
 * modes (e.g. CTE filter mode for CTE_LOOKUP_REQ / CTE_SCAN_REQ, which
 * run the interpreter against a virtual row with no backing DBTUP tuple),
 * each case body is extracted into a static inline handler function
 * taking an InterpreterContext& parameter.
 *
 * The main interpreter's switch still dispatches directly to these
 * handlers (inlined by the compiler — identical hot-path performance).
 * Alternative modes (interpreterFilterCte) use a function pointer table
 * that can selectively override handlers for instructions that are
 * unsafe without a real tuple (EXIT_REFUSE → return reject sentinel,
 * READ_ATTR_INTO_REG → return error, etc.).
 *
 * InterpreterContext and its handler methods are a nested struct of
 * Dbtup. Being a nested type, its members have access to Dbtup private
 * methods (brancher, tupkeyErrorLab, TUPKEY_abort, readAttributes, …)
 * via the ctx.tup pointer — C++11 and later give nested classes the
 * same access rights as other members of the enclosing class.
 * ============================================================ */

/* Handler return value convention (all handlers return int):
 *     0  (INTERP_CONTINUE)     — continue to next instruction
 *     1  (INTERP_EXIT)         — EXIT_OK / EXIT_OK_LAST — caller returns
 *                                req_struct->log_size. EXIT_OK_LAST sets
 *                                req_struct->last_row before returning.
 *    -(error_code)              — error: handler returns -ERROR_CODE (e.g.
 *                                -ZREGISTER_INIT_ERROR = -878). The main
 *                                interpreter dispatch macro wraps this in
 *                                TUPKEY_abort(req_struct, -_rc) which sets
 *                                terrorCode, records jam, calls
 *                                tupkeyErrorLab, and returns -1.
 *                                The CTE filter dispatch sets terrorCode
 *                                directly without calling tupkeyErrorLab.
 *    INTERPRETER_FILTER_REJECT  — EXIT_REFUSE in CTE filter mode (only
 *                                returned by the CTE filter table's
 *                                handleExitRefuseCte override). Propagated
 *                                to the caller of interpreterFilterCte.
 *
 * Handlers use thrjamDebug(ctx.tup->jamBuffer()) for jam tracing since
 * static member functions don't have `this` bound to Dbtup.
 */
static constexpr int INTERP_CONTINUE = 0;
static constexpr int INTERP_EXIT = 1;

/* Maximum opcode value: 0-63 for primary opcodes, 64-127 for
 * OVERFLOW_OPCODE variants (primary + 64). See Interpreter.hpp. */
static constexpr Uint32 INTERP_HANDLER_TABLE_SIZE = 128;

/* InterpreterContext — nested struct of Dbtup.
 *
 * Holds references to the loop-local state of interpreterNextLab so
 * that the extracted case handlers can access interpreter state
 * uniformly. Constructed once at the top of the interpreter loop via
 * aggregate initialization; references are bound to the caller's
 * locals. Handlers modify state via the context — updates propagate
 * to the loop locals automatically.
 */
struct Dbtup::InterpreterContext {
  Dbtup* tup;                       // block pointer (for member-fn calls)
  Signal* signal;                   // signal (for tupkeyErrorLab)
  Dbtup::KeyReqStruct* req_struct;

  // Current program state — bound to loop locals by reference
  Uint32*& TcurrentProgram;         // may be swapped to subroutineProg
  Uint32& TcurrentSize;             // program length of current program
  Uint32& TprogramCounter;          // position in current program

  // Current instruction decoded (updated by loop each iteration)
  Uint32& theInstruction;
  Uint32& theRegister;              // getReg1(theInstruction) << 2

  // Register buffer: 32 words = 8 registers (4 words each)
  Uint32* TregMemBuffer;            // pointer to caller's array[32]
  Uint32* TstackMemBuffer;          // pointer to caller's array[32]
  Uint32& RstackPtr;                // CALL/RETURN stack pointer

  // Heap memory (cheapMemory)
  char* TheapMemoryChar;

  // Loop instruction count (bound to req_struct->no_exec_instructions)
  Uint32& RnoOfInstructions;

  // Cache for BRANCH_ATTR_OP_* (last attrId read, avoids re-read)
  Uint32& tmpHabitant;

  // Main program — needed by RETURN to restore when stack becomes empty
  Uint32* mainProgram;
  Uint32 TmainProgLen;

  // Subroutine program (for CALL/RETURN, and parameter lookup)
  Uint32* subroutineProg;
  Uint32 TsubroutineLen;

  // Temp area for BRANCH_ATTR_OP_* attr reads
  Uint32* tmpArea;
  Uint32 tmpAreaSz;

  // Aggregation register import source.  Only set for aggregation
  // embedded interpreter calls; normal interpreter and CTE filters keep NULL.
  const Register* aggRegisters;

  /* ============================================================
   * Phase A handlers — extracted case bodies
   *
   * Each handler below is a static member function of InterpreterContext
   * reflecting the body of one case in the original interpreterNextLab
   * switch statement. The main switch calls these handlers (inlined by
   * the compiler, identical hot-path performance). Alternative dispatch
   * modes (e.g. CTE filter) reference these via function pointer tables.
   *
   * As a nested type of Dbtup, these handlers can access Dbtup private
   * members (brancher, tupkeyErrorLab, TUPKEY_abort, etc.) via the
   * ctx.tup pointer.
   *
   * Extraction progresses in batches of ~10 handlers.
   * ============================================================ */

  /* --- Batch 1 --- constant loads, simple branches, exits */

  /* LOAD_CONST_NULL — set register to NULL */
  static inline int handleLoadConstNull(InterpreterContext& ctx) {
    ctx.TregMemBuffer[ctx.theRegister] = NULL_INDICATOR;
    return INTERP_CONTINUE;
  }

  /* LOAD_CONST16 — load 16-bit immediate from instruction word into register */
  static inline int handleLoadConst16(InterpreterContext& ctx) {
    *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2) = ctx.theInstruction >> 16;
    ctx.TregMemBuffer[ctx.theRegister] = NOT_NULL_INDICATOR;
    return INTERP_CONTINUE;
  }

  /* LOAD_CONST32 — load 32-bit constant from next program word */
  static inline int handleLoadConst32(InterpreterContext& ctx) {
    ctx.TregMemBuffer[ctx.theRegister] = NOT_NULL_INDICATOR;
    *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2) =
      *(ctx.TcurrentProgram + ctx.TprogramCounter);
    ctx.TprogramCounter++;
    return INTERP_CONTINUE;
  }

  /* LOAD_CONST64 — load 64-bit constant from next two program words */
  static inline int handleLoadConst64(InterpreterContext& ctx) {
    ctx.TregMemBuffer[ctx.theRegister] = NOT_NULL_INDICATOR;
    ctx.TregMemBuffer[ctx.theRegister + 2] =
      *(ctx.TcurrentProgram + ctx.TprogramCounter++);
    ctx.TregMemBuffer[ctx.theRegister + 3] =
      *(ctx.TcurrentProgram + ctx.TprogramCounter++);
    return INTERP_CONTINUE;
  }

  /* LOAD_DOUBLE_CONST — Phase I.18: load IEEE-754 double immediate
   * from the next two program words into a register, marking it
   * REG_TYPE_DOUBLE.  Counterpart of LOAD_CONST64 for floats; signed
   * Int64 constants stay on LOAD_CONST64.  The two program words are
   * laid out low-word-first, matching LOAD_CONST64 — producers should
   * memcpy the double's 8-byte bit pattern into a Uint32[2] when
   * emitting the program. */
  static inline int handleLoadDoubleConst(InterpreterContext& ctx) {
    ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_DOUBLE;
    ctx.TregMemBuffer[ctx.theRegister + 2] =
      *(ctx.TcurrentProgram + ctx.TprogramCounter++);
    ctx.TregMemBuffer[ctx.theRegister + 3] =
      *(ctx.TcurrentProgram + ctx.TprogramCounter++);
    return INTERP_CONTINUE;
  }

  /* BRANCH — unconditional branch */
  static inline int handleBranch(InterpreterContext& ctx) {
    ctx.TprogramCounter =
        ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    return INTERP_CONTINUE;
  }

  /* BRANCH_REG_EQ_NULL — branch if register is NULL */
  static inline int handleBranchRegEqNull(InterpreterContext& ctx) {
    if (ctx.TregMemBuffer[ctx.theRegister] != NULL_INDICATOR) {
      thrjamDebug(ctx.tup->jamBuffer());
    } else {
      thrjamDebug(ctx.tup->jamBuffer());
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_REG_NE_NULL — branch if register is NOT NULL */
  static inline int handleBranchRegNeNull(InterpreterContext& ctx) {
    if (ctx.TregMemBuffer[ctx.theRegister] == NULL_INDICATOR) {
      thrjamDebug(ctx.tup->jamBuffer());
    } else {
      thrjamDebug(ctx.tup->jamBuffer());
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* EXIT_OK — normal exit from interpreter program */
  static inline int handleExitOk(InterpreterContext& /*ctx*/) {
    return INTERP_EXIT;
  }

  /* EXIT_OK_LAST — exit and mark this as the last row in the result set */
  static inline int handleExitOkLast(InterpreterContext& ctx) {
    ctx.req_struct->last_row = true;
    return INTERP_EXIT;
  }

  /* EXIT_REFUSE — reject this row (scan filter rejection).
   * Returns the negative of the client-supplied error code (which lives
   * in the upper 16 bits of the instruction). The main-interpreter
   * INTERP_DISPATCH macro wraps this in TUPKEY_abort, which records jam,
   * sets terrorCode, and calls tupkeyErrorLab. The CTE filter dispatch
   * table overrides this opcode with handleExitRefuseCte which returns
   * INTERPRETER_FILTER_REJECT so the caller can skip the row without
   * touching tuple state. */
  static inline int handleExitRefuse(InterpreterContext& ctx) {
    return -static_cast<int>(ctx.theInstruction >> 16);
  }

  /* --- Batch 2 --- register comparison branches (reg-reg and reg-const) */

  /* Phase I.18: type-aware register-vs-register three-way comparison.
   * Returns -1 / 0 / 1 by analogy with `memcmp`.  Both operands must
   * be non-NULL (caller checks via `(leftType & rightType) != 0`).
   *
   * Type-word layout from Interpreter.hpp:
   *   byte 0 = NOT_NULL flag (always 1 for non-NULL operands here)
   *   byte 1 = UNSIGNED flag
   *   byte 2 = FLOAT flag
   *
   * Single-byte loads keep the dispatch cheap on most CPUs. */
  static inline int compareTypedRegs(Uint32 leftType, Uint64 leftBits,
                                     Uint32 rightType, Uint64 rightBits) {
    const Uint8* lw = reinterpret_cast<const Uint8*>(&leftType);
    const Uint8* rw = reinterpret_cast<const Uint8*>(&rightType);
    bool leftFloat  = lw[2] != 0;
    bool rightFloat = rw[2] != 0;
    if (unlikely(leftFloat || rightFloat)) {
      double l;
      double r;
      if (leftFloat) {
        memcpy(&l, &leftBits, 8);
      } else if (lw[1] != 0) {
        l = static_cast<double>(leftBits);
      } else {
        l = static_cast<double>(static_cast<Int64>(leftBits));
      }
      if (rightFloat) {
        memcpy(&r, &rightBits, 8);
      } else if (rw[1] != 0) {
        r = static_cast<double>(rightBits);
      } else {
        r = static_cast<double>(static_cast<Int64>(rightBits));
      }
      return (l < r) ? -1 : (l > r) ? 1 : 0;
    }
    bool leftUnsigned  = lw[1] != 0;
    bool rightUnsigned = rw[1] != 0;
    if (likely(leftUnsigned == rightUnsigned)) {
      if (leftUnsigned) {
        return (leftBits < rightBits) ? -1 :
               (leftBits > rightBits) ?  1 : 0;
      }
      Int64 ls = static_cast<Int64>(leftBits);
      Int64 rs = static_cast<Int64>(rightBits);
      return (ls < rs) ? -1 : (ls > rs) ? 1 : 0;
    }
    /* Mixed signed / unsigned: a negative signed operand is strictly
     * less than any unsigned operand.  Otherwise both fit non-negative
     * in Uint64 so the unsigned comparison is exact. */
    if (leftUnsigned) {
      if (static_cast<Int64>(rightBits) < 0) return 1;
      return (leftBits < rightBits) ? -1 :
             (leftBits > rightBits) ?  1 : 0;
    }
    if (static_cast<Int64>(leftBits) < 0) return -1;
    return (leftBits < rightBits) ? -1 :
           (leftBits > rightBits) ?  1 : 0;
  }

  /* Phase I.18: type-aware register arithmetic.  Op is one of
   * '+' '-' '*' '/' '%'.  Promotion rules:
   *
   *   - either operand FLOAT  → double arithmetic, result REG_TYPE_DOUBLE.
   *     Div/Mod by 0.0 returns -ZDIV_BY_ZERO_ERROR.
   *   - else either UNSIGNED  → exact mixed signed/unsigned integer
   *     arithmetic.  Negative results are tagged REG_TYPE_INT when
   *     they fit Int64; non-negative results are tagged REG_TYPE_UINT
   *     when they fit Uint64.  Add/Sub/Mul reject overflow.  Div/Mod
   *     by 0 rejected.
   *   - else both signed      → Int64 arithmetic, result REG_TYPE_INT.
   *     Add/Sub/Mul reject overflow; Div/Mod reject only the
   *     divide-by-zero case.
   *
   * Returns 0 on success, negative error code on failure. */
  static inline int applyTypedArith(char op,
                                     Uint32 leftType, Uint64 leftBits,
                                     Uint32 rightType, Uint64 rightBits,
                                     Uint32* resultType,
                                     Uint64* resultBits) {
    const Uint8* lw = reinterpret_cast<const Uint8*>(&leftType);
    const Uint8* rw = reinterpret_cast<const Uint8*>(&rightType);
    bool leftFloat  = lw[2] != 0;
    bool rightFloat = rw[2] != 0;
    if (unlikely(leftFloat || rightFloat)) {
      double l;
      double r;
      if (leftFloat) {
        memcpy(&l, &leftBits, 8);
      } else if (lw[1] != 0) {
        l = static_cast<double>(leftBits);
      } else {
        l = static_cast<double>(static_cast<Int64>(leftBits));
      }
      if (rightFloat) {
        memcpy(&r, &rightBits, 8);
      } else if (rw[1] != 0) {
        r = static_cast<double>(rightBits);
      } else {
        r = static_cast<double>(static_cast<Int64>(rightBits));
      }
      double res;
      switch (op) {
        case '+': res = l + r; break;
        case '-': res = l - r; break;
        case '*': res = l * r; break;
        case '/':
          if (r == 0.0) return -ZDIV_BY_ZERO_ERROR;
          res = l / r;
          break;
        case '%':
          if (r == 0.0) return -ZDIV_BY_ZERO_ERROR;
          res = std::fmod(l, r);
          break;
        default: return -ZCALC_OVERFLOW_ERROR;
      }
      *resultType = Interpreter::REG_TYPE_DOUBLE;
      memcpy(resultBits, &res, 8);
      return 0;
    }
    bool leftUnsigned  = lw[1] != 0;
    bool rightUnsigned = rw[1] != 0;
    if (leftUnsigned || rightUnsigned) {
      __int128 l = leftUnsigned
          ? static_cast<__int128>(leftBits)
          : static_cast<__int128>(static_cast<Int64>(leftBits));
      __int128 r = rightUnsigned
          ? static_cast<__int128>(rightBits)
          : static_cast<__int128>(static_cast<Int64>(rightBits));
      __int128 res;
      switch (op) {
        case '+':
          res = l + r;
          break;
        case '-':
          res = l - r;
          break;
        case '*':
          res = l * r;
          break;
        case '/':
          if (r == 0) return -ZDIV_BY_ZERO_ERROR;
          res = l / r;
          break;
        case '%':
          if (r == 0) return -ZDIV_BY_ZERO_ERROR;
          res = l % r;
          break;
        default: return -ZCALC_OVERFLOW_ERROR;
      }
      if (res < 0) {
        if (unlikely(leftUnsigned && rightUnsigned)) {
          return -ZCALC_OVERFLOW_ERROR;
        }
        if (unlikely(res < static_cast<__int128>(LLONG_MIN))) {
          return -ZCALC_OVERFLOW_ERROR;
        }
        *resultType = Interpreter::REG_TYPE_INT;
        *resultBits = static_cast<Uint64>(static_cast<Int64>(res));
      } else {
        if (unlikely(static_cast<unsigned __int128>(res) >
                     static_cast<unsigned __int128>(UINT64_MAX))) {
          return -ZCALC_OVERFLOW_ERROR;
        }
        *resultType = Interpreter::REG_TYPE_UINT;
        *resultBits = static_cast<Uint64>(res);
      }
      return 0;
    }
    /* Both signed integer. */
    Int64 l = static_cast<Int64>(leftBits);
    Int64 r = static_cast<Int64>(rightBits);
    Int64 res;
    switch (op) {
      case '+':
        if (unlikely((r >= 0 && LLONG_MAX - r < l) ||
                     (r <  0 && LLONG_MIN - r > l))) {
          return -ZCALC_OVERFLOW_ERROR;
        }
        res = l + r;
        break;
      case '-':
        if (unlikely((r >= 0 && LLONG_MIN + r > l) ||
                     (r <  0 && LLONG_MAX + r < l))) {
          return -ZCALC_OVERFLOW_ERROR;
        }
        res = l - r;
        break;
      case '*':
      {
        __int128 wide =
            static_cast<__int128>(l) * static_cast<__int128>(r);
        if (unlikely(wide > static_cast<__int128>(LLONG_MAX) ||
                     wide < static_cast<__int128>(LLONG_MIN))) {
          return -ZCALC_OVERFLOW_ERROR;
        }
        res = static_cast<Int64>(wide);
        break;
      }
      case '/':
        if (r == 0) return -ZDIV_BY_ZERO_ERROR;
        res = l / r;
        break;
      case '%':
        if (r == 0) return -ZDIV_BY_ZERO_ERROR;
        res = l % r;
        break;
      default: return -ZCALC_OVERFLOW_ERROR;
    }
    *resultType = Interpreter::REG_TYPE_INT;
    *resultBits = static_cast<Uint64>(res);
    return 0;
  }

  /* Phase I.18: type-aware register bitwise / shift operations.
   * Op is one of:
   *   '&' '|' '^'  bitwise (binary)
   *   '~'          bitwise NOT (unary; rightBits is ignored)
   *   'L' 'R'      left / right shift (rightBits is shift count)
   *
   * Float operands rejected (returns -ZREGISTER_INIT_ERROR).  Result
   * type is REG_TYPE_UINT iff either operand is unsigned (matches
   * MySQL bitwise-promotion rules); RSHIFT on a signed operand is
   * arithmetic shift (sign-fill).  Shift count must be 0..63;
   * out-of-range returns -ZSHIFT_OPERAND_ERROR. */
  static inline int applyTypedBitwise(char op,
                                       Uint32 leftType, Uint64 leftBits,
                                       Uint32 rightType, Uint64 rightBits,
                                       Uint32* resultType,
                                       Uint64* resultBits) {
    const Uint8* lw = reinterpret_cast<const Uint8*>(&leftType);
    const Uint8* rw = reinterpret_cast<const Uint8*>(&rightType);
    if (unlikely(lw[2] != 0 || rw[2] != 0)) {
      return -ZREGISTER_INIT_ERROR;  /* bitwise / shift on float */
    }
    bool resultUnsigned = (lw[1] != 0) || (rw[1] != 0);
    Uint64 res;
    switch (op) {
      case '&': res = leftBits & rightBits; break;
      case '|': res = leftBits | rightBits; break;
      case '^': res = leftBits ^ rightBits; break;
      case '~':
        res = ~leftBits;
        /* Unary: inherit the source's signedness. */
        resultUnsigned = (lw[1] != 0);
        break;
      case 'L': {
        Int64 shift = static_cast<Int64>(rightBits);
        if (unlikely(shift < 0 || shift >= 64)) {
          return -ZSHIFT_OPERAND_ERROR;
        }
        res = leftBits << shift;
        break;
      }
      case 'R': {
        Int64 shift = static_cast<Int64>(rightBits);
        if (unlikely(shift < 0 || shift >= 64)) {
          return -ZSHIFT_OPERAND_ERROR;
        }
        /* Logical shift for unsigned, arithmetic shift for signed. */
        if (resultUnsigned) {
          res = leftBits >> shift;
        } else {
          res = static_cast<Uint64>(static_cast<Int64>(leftBits) >> shift);
        }
        break;
      }
      default:
        return -ZREGISTER_INIT_ERROR;
    }
    *resultType = resultUnsigned ? Interpreter::REG_TYPE_UINT
                                 : Interpreter::REG_TYPE_INT;
    *resultBits = res;
    return 0;
  }

  /* Phase I.18: returns true iff the register's type word marks a
   * floating-point value (byte 2 of the type word is non-zero).
   * Used by write-back handlers that reject float operands until the
   * float-to-integer coercion semantics are designed. */
  static inline bool reg_is_float(Uint32 typeWord) {
    return reinterpret_cast<const Uint8*>(&typeWord)[2] != 0;
  }

  /* Phase I.18: type-aware register-vs-immediate-const comparison.
   * The immediate is the 6-bit constant in bits 9..14 of the
   * instruction word (range 0..63), always non-negative — it always
   * fits in Int64, Uint64, and double identically. */
  static inline int compareTypedRegConst(Uint32 leftType, Uint64 leftBits,
                                         Uint32 rightConst) {
    const Uint8* lw = reinterpret_cast<const Uint8*>(&leftType);
    if (unlikely(lw[2] != 0)) {
      double l;
      memcpy(&l, &leftBits, 8);
      double r = static_cast<double>(rightConst);
      return (l < r) ? -1 : (l > r) ? 1 : 0;
    }
    if (lw[1] != 0) {
      Uint64 r = static_cast<Uint64>(rightConst);
      return (leftBits < r) ? -1 : (leftBits > r) ? 1 : 0;
    }
    Int64 ls = static_cast<Int64>(leftBits);
    Int64 rs = static_cast<Int64>(rightConst);
    return (ls < rs) ? -1 : (ls > rs) ? 1 : 0;
  }

  /* BRANCH_EQ_REG_REG — branch if reg == reg2 (both non-null)
   *
   * Phase I.18: type-aware compare via compareTypedRegs.  Existing
   * "both signed integer" pair cases fall through to the same
   * Int64 comparison as before; mixed signed / unsigned and
   * float-bearing operands now compare correctly. */
  static inline int handleBranchEqRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    if (unlikely((TrightType & TleftType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    if (compareTypedRegs(TleftType, leftBits, TrightType, rightBits) == 0) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_NE_REG_REG — branch if reg != reg2 */
  static inline int handleBranchNeRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    if (unlikely((TrightType & TleftType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    if (compareTypedRegs(TleftType, leftBits, TrightType, rightBits) != 0) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_LT_REG_REG — branch if reg < reg2 (type-aware) */
  static inline int handleBranchLtRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    if (unlikely((TrightType & TleftType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    if (compareTypedRegs(TleftType, leftBits, TrightType, rightBits) < 0) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_LE_REG_REG — branch if reg <= reg2 (type-aware) */
  static inline int handleBranchLeRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    if (unlikely((TrightType & TleftType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    if (compareTypedRegs(TleftType, leftBits, TrightType, rightBits) <= 0) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_GT_REG_REG — branch if reg > reg2 (type-aware) */
  static inline int handleBranchGtRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    if (unlikely((TrightType & TleftType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    if (compareTypedRegs(TleftType, leftBits, TrightType, rightBits) > 0) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_GE_REG_REG — branch if reg >= reg2 (type-aware) */
  static inline int handleBranchGeRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    if (unlikely((TrightType & TleftType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    if (compareTypedRegs(TleftType, leftBits, TrightType, rightBits) >= 0) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_EQ_REG_CONST — branch if reg == 6-bit immediate
   *
   * Phase I.18: type-aware compare via compareTypedRegConst.  The
   * 6-bit immediate is always non-negative (range 0..63) so it
   * fits identically in Int64 / Uint64 / double; only the
   * register's type word affects the comparison. */
  static inline int handleBranchEqRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 rightConst = (ctx.theInstruction >> 9) & 0x3F;
    if (compareTypedRegConst(TleftType, leftBits, rightConst) == 0) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_NE_REG_CONST — branch if reg != 6-bit immediate */
  static inline int handleBranchNeRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 rightConst = (ctx.theInstruction >> 9) & 0x3F;
    if (compareTypedRegConst(TleftType, leftBits, rightConst) != 0) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_LT_REG_CONST — branch if reg < 6-bit immediate (type-aware) */
  static inline int handleBranchLtRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 rightConst = (ctx.theInstruction >> 9) & 0x3F;
    if (compareTypedRegConst(TleftType, leftBits, rightConst) < 0) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_LE_REG_CONST — branch if reg <= 6-bit immediate (type-aware) */
  static inline int handleBranchLeRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 rightConst = (ctx.theInstruction >> 9) & 0x3F;
    if (compareTypedRegConst(TleftType, leftBits, rightConst) <= 0) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* --- Batch 3 --- remaining reg-const branches, subroutine, memory, arithmetic */

  /* BRANCH_GT_REG_CONST — branch if reg > 6-bit immediate (type-aware) */
  static inline int handleBranchGtRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 rightConst = (ctx.theInstruction >> 9) & 0x3F;
    if (compareTypedRegConst(TleftType, leftBits, rightConst) > 0) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_GE_REG_CONST — branch if reg >= 6-bit immediate (type-aware) */
  static inline int handleBranchGeRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 rightConst = (ctx.theInstruction >> 9) & 0x3F;
    if (compareTypedRegConst(TleftType, leftBits, rightConst) >= 0) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* CALL — push return address, jump to subroutine */
  static inline int handleCall(InterpreterContext& ctx) {
    ctx.RstackPtr++;
    if (ctx.RstackPtr < 32) {
      ctx.TstackMemBuffer[ctx.RstackPtr] = ctx.TprogramCounter;
      ctx.TprogramCounter = ctx.theInstruction >> 16;
      if (ctx.TprogramCounter < ctx.TsubroutineLen) {
        ctx.TcurrentProgram = ctx.subroutineProg;
        ctx.TcurrentSize = ctx.TsubroutineLen;
      } else {
        return -ZCALL_ERROR;
      }
    } else {
      return -ZSTACK_OVERFLOW_ERROR;
    }
    return INTERP_CONTINUE;
  }

  /* RETURN — pop return address, resume caller (or main program) */
  static inline int handleReturn(InterpreterContext& ctx) {
    if (ctx.RstackPtr > 0) {
      ctx.TprogramCounter = ctx.TstackMemBuffer[ctx.RstackPtr];
      ctx.RstackPtr--;
      if (ctx.RstackPtr == 0) {
        thrjamDebug(ctx.tup->jamBuffer());
        /* Back to the main program */
        ctx.TcurrentProgram = ctx.mainProgram;
        ctx.TcurrentSize = ctx.TmainProgLen;
      }
    } else {
      return -ZSTACK_UNDERFLOW_ERROR;
    }
    return INTERP_CONTINUE;
  }

  /* NOT_REG_REG — bitwise NOT of register
   *
   * Phase I.18: type-aware via applyTypedBitwise.  Float operand
   * rejected; result inherits the source's signedness. */
  static inline int handleNotRegReg(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedBitwise('~', TleftType, leftBits,
                               TleftType, 0,
                               &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* BZERO_MEM — zero a range of heap memory */
  static inline int handleBzeroMem(InterpreterContext& ctx) {
    Uint32 registerOffsetType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 registerSize = Interpreter::getReg2(ctx.theInstruction) << 2;
    Int64 Toffset = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Int64 Tsize = *(Int64*)(ctx.TregMemBuffer + registerSize + 2);
    Uint32 registerSizeType = ctx.TregMemBuffer[registerSize];
    if (unlikely(registerOffsetType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(registerSizeType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Int64 Tend = Toffset + Tsize;
    if (Toffset < 0 || Tsize < 0 || Tend > MAX_HEAP_OFFSET) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    Uint32* memory_ptr = (Uint32*)&ctx.TheapMemoryChar[Toffset];
    memset(memory_ptr, 0, Tsize);
    return INTERP_CONTINUE;
  }

  /* LOAD_CONST_MEM — copy inline constant bytes into heap memory */
  static inline int handleLoadConstMem(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 1;  // A bit heavier instruction
    Uint32 registerDestSize = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 registerOffsetType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 Tsize = ctx.theInstruction >> 16;
    Uint32 words = (Tsize + 3) / 4;
    Int64 Toffset = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    if (unlikely(registerOffsetType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(((Toffset + Int64(words << 2)) > MAX_HEAP_OFFSET) ||
                 (Toffset < Int64(0)))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    if (unlikely(Tsize > (MAX_VAR_SIZE_IN_WORDS * 4))) {
      return -ZLOAD_MEM_TOO_BIG_ERROR;
    }
    ctx.TregMemBuffer[registerDestSize] = NOT_NULL_INDICATOR;
    *(Int64*)(ctx.TregMemBuffer + registerDestSize + 2) = (Int64)Tsize;
    Uint32* memory_ptr = (Uint32*)&ctx.TheapMemoryChar[Toffset];
    memcpy(memory_ptr, &ctx.TcurrentProgram[ctx.TprogramCounter], Tsize);
    ctx.TprogramCounter += words;
    return INTERP_CONTINUE;
  }

  /* ADD_REG_REG — destReg = reg + reg2 (uses Reg4 as dest, legacy)
   *
   * Phase I.18: type-aware via applyTypedArith. */
  static inline int handleAddRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    Uint32 TdestRegister = Interpreter::getReg4(ctx.theInstruction) << 2;
    if (unlikely((TleftType & TrightType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedArith('+', TleftType, leftBits,
                             TrightType, rightBits,
                             &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* SUB_REG_REG — destReg = reg - reg2 (uses Reg4 as dest, legacy) */
  static inline int handleSubRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    Uint32 TdestRegister = Interpreter::getReg4(ctx.theInstruction) << 2;
    if (unlikely((TleftType & TrightType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedArith('-', TleftType, leftBits,
                             TrightType, rightBits,
                             &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* MUL_REG_REG — destReg = reg * reg2 */
  static inline int handleMulRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely((TleftType & TrightType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedArith('*', TleftType, leftBits,
                             TrightType, rightBits,
                             &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* --- Batch 4 --- arithmetic and shift ops (reg-const and remaining reg-reg) */

  /* ADD_REG_CONST — destReg = reg + 16-bit immediate
   *
   * Phase I.18: the instruction's 16-bit immediate is treated as a
   * signed Int64 (matches existing behaviour) and supplied to
   * applyTypedArith as a REG_TYPE_INT operand.  The register's type
   * word still drives the result type. */
  static inline int handleAddRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Int64 rhs = static_cast<Int64>(ctx.theInstruction >> 16);
    Uint64 rightBits = static_cast<Uint64>(rhs);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedArith('+', TleftType, leftBits,
                             Interpreter::REG_TYPE_INT, rightBits,
                             &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* SUB_REG_CONST — destReg = reg - 16-bit immediate */
  static inline int handleSubRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Int64 rhs = static_cast<Int64>(ctx.theInstruction >> 16);
    Uint64 rightBits = static_cast<Uint64>(rhs);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedArith('-', TleftType, leftBits,
                             Interpreter::REG_TYPE_INT, rightBits,
                             &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* MUL_REG_CONST — destReg = reg * 16-bit immediate */
  static inline int handleMulRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Int64 rhs = static_cast<Int64>(ctx.theInstruction >> 16);
    Uint64 rightBits = static_cast<Uint64>(rhs);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedArith('*', TleftType, leftBits,
                             Interpreter::REG_TYPE_INT, rightBits,
                             &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* DIV_REG_CONST — destReg = reg / 16-bit immediate (with div-by-zero check) */
  static inline int handleDivRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Int64 rhs = static_cast<Int64>(ctx.theInstruction >> 16);
    Uint64 rightBits = static_cast<Uint64>(rhs);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedArith('/', TleftType, leftBits,
                             Interpreter::REG_TYPE_INT, rightBits,
                             &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* DIV_REG_REG — destReg = reg / reg2 (with div-by-zero check) */
  static inline int handleDivRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely((TleftType & TrightType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedArith('/', TleftType, leftBits,
                             TrightType, rightBits,
                             &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* LSHIFT_REG_CONST — destReg = reg << 16-bit immediate (shift < 64) */
  static inline int handleLshiftRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits = static_cast<Uint64>(ctx.theInstruction >> 16);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedBitwise('L', TleftType, leftBits,
                               Interpreter::REG_TYPE_INT, rightBits,
                               &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* LSHIFT_REG_REG — destReg = reg << reg2 (shift < 64) */
  static inline int handleLshiftRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely((TleftType & TrightType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedBitwise('L', TleftType, leftBits,
                               TrightType, rightBits,
                               &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* RSHIFT_REG_CONST — destReg = reg >> 16-bit immediate (shift < 64) */
  static inline int handleRshiftRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits = static_cast<Uint64>(ctx.theInstruction >> 16);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedBitwise('R', TleftType, leftBits,
                               Interpreter::REG_TYPE_INT, rightBits,
                               &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* RSHIFT_REG_REG — destReg = reg >> reg2 (shift < 64) */
  static inline int handleRshiftRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely((TleftType & TrightType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedBitwise('R', TleftType, leftBits,
                               TrightType, rightBits,
                               &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* AND_REG_CONST — destReg = reg & 16-bit immediate */
  static inline int handleAndRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits = static_cast<Uint64>(ctx.theInstruction >> 16);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedBitwise('&', TleftType, leftBits,
                               Interpreter::REG_TYPE_INT, rightBits,
                               &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* --- Batch 5 --- remaining logical/arithmetic + attr-null branches + linked read */

  /* AND_REG_REG — destReg = reg & reg2 */
  static inline int handleAndRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely((TleftType & TrightType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedBitwise('&', TleftType, leftBits,
                               TrightType, rightBits,
                               &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* OR_REG_CONST — destReg = reg | 16-bit immediate */
  static inline int handleOrRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits = static_cast<Uint64>(ctx.theInstruction >> 16);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedBitwise('|', TleftType, leftBits,
                               Interpreter::REG_TYPE_INT, rightBits,
                               &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* OR_REG_REG — destReg = reg | reg2 */
  static inline int handleOrRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely((TleftType & TrightType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedBitwise('|', TleftType, leftBits,
                               TrightType, rightBits,
                               &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* XOR_REG_CONST — destReg = reg ^ 16-bit immediate */
  static inline int handleXorRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits = static_cast<Uint64>(ctx.theInstruction >> 16);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedBitwise('^', TleftType, leftBits,
                               Interpreter::REG_TYPE_INT, rightBits,
                               &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* XOR_REG_REG — destReg = reg ^ reg2 */
  static inline int handleXorRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely((TleftType & TrightType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedBitwise('^', TleftType, leftBits,
                               TrightType, rightBits,
                               &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* MOD_REG_CONST — destReg = reg % 16-bit immediate (with div-by-zero check) */
  static inline int handleModRegConst(InterpreterContext& ctx) {
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely(TleftType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Int64 rhs = static_cast<Int64>(ctx.theInstruction >> 16);
    Uint64 rightBits = static_cast<Uint64>(rhs);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedArith('%', TleftType, leftBits,
                             Interpreter::REG_TYPE_INT, rightBits,
                             &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* MOD_REG_REG — destReg = reg % reg2 (with div-by-zero check) */
  static inline int handleModRegReg(InterpreterContext& ctx) {
    Uint32 TrightRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TleftType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TrightType = ctx.TregMemBuffer[TrightRegister];
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    if (unlikely((TleftType & TrightType) == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint64 leftBits =
        *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint64 rightBits =
        *(Uint64*)(ctx.TregMemBuffer + TrightRegister + 2);
    Uint32 resultType;
    Uint64 resultBits;
    int rc = applyTypedArith('%', TleftType, leftBits,
                             TrightType, rightBits,
                             &resultType, &resultBits);
    if (unlikely(rc != 0)) return rc;
    ctx.TregMemBuffer[TdestRegister] = resultType;
    *(Uint64*)(ctx.TregMemBuffer + TdestRegister + 2) = resultBits;
    return INTERP_CONTINUE;
  }

  /* READ_LINKED_TO_MEM — read a linked (parent-table) column value from
   * req_struct->m_linked_attr_data into cheapMemory[0]. Format of the
   * linked buffer: [tableId, schemaVersion, AttrHeader, data...] per entry.
   * Position (bits 16..23) selects the Nth entry. If linked data is
   * unavailable or out-of-bounds, writes a NULL AttributeHeader at offset 0. */
  static inline int handleReadLinkedToMem(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3;
    Uint32 position = (ctx.theInstruction >> 16) & 0xFF;

    const Uint32* linked = ctx.req_struct->m_linked_attr_data;
    Uint32 linked_len = ctx.req_struct->m_linked_attr_len;
    Uint32* memory_ptr = (Uint32*)&ctx.TheapMemoryChar[0];

    if (unlikely(linked == nullptr)) {
      AttributeHeader null_ah(0, 0);
      memory_ptr[0] = null_ah.m_value;
      return INTERP_CONTINUE;
    }

    const Uint32* p = linked;
    const Uint32* p_end = linked + linked_len;
    Uint32 pos_count = 0;
    while (p < p_end) {
      if (pos_count == position) break;
      p += 2;  // skip tableId, schemaVersion
      p += 1 + AttributeHeader::getDataSize(*p);
      pos_count++;
    }
    if (unlikely(p >= p_end)) {
      AttributeHeader null_ah(0, 0);
      memory_ptr[0] = null_ah.m_value;
      return INTERP_CONTINUE;
    }

    // Skip tableId and schemaVersion, copy AttrHeader + data
    p += 2;
    Uint32 words = 1 + AttributeHeader::getDataSize(*p);
    memcpy(memory_ptr, p, words * sizeof(Uint32));
    return INTERP_CONTINUE;
  }

  /* BRANCH_ATTR_EQ_NULL — branch if tuple attribute is NULL */
  static inline int handleBranchAttrEqNull(InterpreterContext& ctx) {
    Uint32 ins2 = ctx.TcurrentProgram[ctx.TprogramCounter];
    Uint32 attrId = Interpreter::getBranchCol_AttrId(ins2) << 16;

    if (ctx.tmpHabitant != attrId) {
      Int32 TnoDataR = ctx.tup->readAttributes(
          ctx.req_struct, &attrId, 1, ctx.tmpArea, ctx.tmpAreaSz);
      if (unlikely(TnoDataR < 0)) {
        thrjam(ctx.tup->jamBuffer());
        return TnoDataR;  /* already negative of error code */
      }
      ctx.tmpHabitant = attrId;
    }

    AttributeHeader ah(ctx.tmpArea[0]);
    if (ah.isNULL()) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    } else {
      ctx.TprogramCounter++;
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_LINKED_EQ_NULL — branch if linked column at the most
   * recent READ_LINKED_TO_MEM position is NULL.  Examines the
   * AttributeHeader at cheapMemory[0]. */
  static inline int handleBranchLinkedEqNull(InterpreterContext& ctx) {
    const Uint32* memory_ptr = (const Uint32*)&ctx.TheapMemoryChar[0];
    AttributeHeader ah(memory_ptr[0]);
    if (ah.isNULL()) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_LINKED_NE_NULL — branch if linked column at the most
   * recent READ_LINKED_TO_MEM position is NOT NULL. */
  static inline int handleBranchLinkedNeNull(InterpreterContext& ctx) {
    const Uint32* memory_ptr = (const Uint32*)&ctx.TheapMemoryChar[0];
    AttributeHeader ah(memory_ptr[0]);
    if (!ah.isNULL()) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_ATTR_NE_NULL — branch if tuple attribute is NOT NULL */
  static inline int handleBranchAttrNeNull(InterpreterContext& ctx) {
    Uint32 ins2 = ctx.TcurrentProgram[ctx.TprogramCounter];
    Uint32 attrId = Interpreter::getBranchCol_AttrId(ins2) << 16;

    if (ctx.tmpHabitant != attrId) {
      Int32 TnoDataR = ctx.tup->readAttributes(
          ctx.req_struct, &attrId, 1, ctx.tmpArea, ctx.tmpAreaSz);
      if (unlikely(TnoDataR < 0)) {
        thrjam(ctx.tup->jamBuffer());
        return TnoDataR;  /* already negative of error code */
      }
      ctx.tmpHabitant = attrId;
    }

    AttributeHeader ah(ctx.tmpArea[0]);
    if (ah.isNULL()) {
      ctx.TprogramCounter++;
    } else {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    }
    return INTERP_CONTINUE;
  }

  /* --- Batch 6 --- memory read/write ops (size-prefixed loads/stores) */

  /* READ_UINT8_MEM_TO_REG — read 1 byte from heap offset (immediate) */
  static inline int handleReadUint8MemToReg(InterpreterContext& ctx) {
    Uint32 memoryOffset = ctx.theInstruction >> 16;
    if (unlikely(memoryOffset > MAX_HEAP_OFFSET)) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    Uint8 value = ctx.TheapMemoryChar[memoryOffset];
    /* Phase I.18: type-aware register write.  Source is unsigned so
     * register acquires REG_TYPE_UINT; payload zero-extended to 64
     * bits via Uint64 store. */
    ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_UINT;
    *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2) = (Uint64)value;
    return INTERP_CONTINUE;
  }

  /* READ_UINT16_MEM_TO_REG — read 2 bytes from heap offset (immediate) */
  static inline int handleReadUint16MemToReg(InterpreterContext& ctx) {
    Uint32 memoryOffset = ctx.theInstruction >> 16;
    Uint16 value;
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 1))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    memcpy(&value, &ctx.TheapMemoryChar[memoryOffset], 2);
    /* Phase I.18: REG_TYPE_UINT.  See handleReadUint8MemToReg. */
    ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_UINT;
    *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2) = (Uint64)value;
    return INTERP_CONTINUE;
  }

  /* READ_UINT32_MEM_TO_REG — read 4 bytes from heap offset (immediate) */
  static inline int handleReadUint32MemToReg(InterpreterContext& ctx) {
    Uint32 memoryOffset = ctx.theInstruction >> 16;
    Uint32 value;
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 3))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    memcpy(&value, &ctx.TheapMemoryChar[memoryOffset], 4);
    /* Phase I.18: REG_TYPE_UINT.  See handleReadUint8MemToReg. */
    ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_UINT;
    *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2) = (Uint64)value;
    return INTERP_CONTINUE;
  }

  /* READ_INT64_MEM_TO_REG — read 8 bytes from heap offset (immediate) */
  static inline int handleReadInt64MemToReg(InterpreterContext& ctx) {
    Uint32 memoryOffset = ctx.theInstruction >> 16;
    Int64 value;
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 7))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    memcpy(&value, &ctx.TheapMemoryChar[memoryOffset], 8);
    ctx.TregMemBuffer[ctx.theRegister] = NOT_NULL_INDICATOR;
    *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2) = value;
    return INTERP_CONTINUE;
  }

  /* READ_UINT8_REG_TO_REG — read 1 byte from heap offset stored in register */
  static inline int handleReadUint8RegToReg(InterpreterContext& ctx) {
    Uint32 memoryOffsetType = ctx.TregMemBuffer[ctx.theRegister];
    Int64 memoryOffset = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 destRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    if (unlikely(memoryOffsetType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 0))) {
      thrjam(ctx.tup->jamBuffer());
      return -ZMEMORY_OFFSET_ERROR;
    }
    Uint8 value = ctx.TheapMemoryChar[memoryOffset];
    /* Phase I.18: REG_TYPE_UINT on the destination. */
    *(Uint64*)(ctx.TregMemBuffer + destRegister + 2) = (Uint64)value;
    ctx.TregMemBuffer[destRegister] = Interpreter::REG_TYPE_UINT;
    return INTERP_CONTINUE;
  }

  /* READ_UINT16_REG_TO_REG — read 2 bytes from heap offset stored in register */
  static inline int handleReadUint16RegToReg(InterpreterContext& ctx) {
    Uint32 memoryOffsetType = ctx.TregMemBuffer[ctx.theRegister];
    Int64 memoryOffset = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 destRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint16 value;
    if (unlikely(memoryOffsetType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 1))) {
      thrjam(ctx.tup->jamBuffer());
      return -ZMEMORY_OFFSET_ERROR;
    }
    memcpy(&value, &ctx.TheapMemoryChar[memoryOffset], 2);
    /* Phase I.18: REG_TYPE_UINT on the destination. */
    *(Uint64*)(ctx.TregMemBuffer + destRegister + 2) = (Uint64)value;
    ctx.TregMemBuffer[destRegister] = Interpreter::REG_TYPE_UINT;
    return INTERP_CONTINUE;
  }

  /* READ_UINT32_REG_TO_REG — read 4 bytes from heap offset stored in register */
  static inline int handleReadUint32RegToReg(InterpreterContext& ctx) {
    Uint32 memoryOffsetType = ctx.TregMemBuffer[ctx.theRegister];
    Int64 memoryOffset = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 destRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 value;
    if (unlikely(memoryOffsetType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 3))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    memcpy(&value, &ctx.TheapMemoryChar[memoryOffset], 4);
    /* Phase I.18: REG_TYPE_UINT on the destination. */
    *(Uint64*)(ctx.TregMemBuffer + destRegister + 2) = (Uint64)value;
    ctx.TregMemBuffer[destRegister] = Interpreter::REG_TYPE_UINT;
    return INTERP_CONTINUE;
  }

  /* READ_INT64_REG_TO_REG — read 8 bytes from heap offset stored in register */
  static inline int handleReadInt64RegToReg(InterpreterContext& ctx) {
    Uint32 memoryOffsetType = ctx.TregMemBuffer[ctx.theRegister];
    Int64 memoryOffset = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 destRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Int64 value;
    if (unlikely(memoryOffsetType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 7))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    memcpy(&value, &ctx.TheapMemoryChar[memoryOffset], 8);
    *(Int64*)(ctx.TregMemBuffer + destRegister + 2) = value;
    ctx.TregMemBuffer[destRegister] = NOT_NULL_INDICATOR;
    return INTERP_CONTINUE;
  }

  /* WRITE_UINT8_REG_TO_MEM — write 1 byte from register to heap (immediate)
   *
   * Phase I.18: strict typing.  The opcode names UINT8, so the source
   * register must be REG_TYPE_UINT (rejects NULL, signed, and float
   * sources alike).  Use WRITE_REG_TO_MEM_ANY for type-agnostic
   * 8-byte memory writes. */
  static inline int handleWriteUint8RegToMem(InterpreterContext& ctx) {
    Uint32 TregType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 memoryOffset = ctx.theInstruction >> 16;
    Uint64 Tvalue = *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint8 val = (Uint8)Tvalue;
    if (unlikely(TregType != Interpreter::REG_TYPE_UINT)) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 0))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    memcpy(&ctx.TheapMemoryChar[memoryOffset], &val, 1);
    return INTERP_CONTINUE;
  }

  /* WRITE_UINT16_REG_TO_MEM — write 2 bytes from register to heap (immediate) */
  static inline int handleWriteUint16RegToMem(InterpreterContext& ctx) {
    Uint32 TregType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 memoryOffset = ctx.theInstruction >> 16;
    Uint64 Tvalue = *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint16 val = (Uint16)Tvalue;
    if (unlikely(TregType != Interpreter::REG_TYPE_UINT)) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 1))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    memcpy(&ctx.TheapMemoryChar[memoryOffset], &val, 2);
    return INTERP_CONTINUE;
  }

  /* --- Batch 7 --- remaining memory writes + I/O + size-convert */

  /* WRITE_UINT32_REG_TO_MEM — write 4 bytes from register to heap (immediate) */
  static inline int handleWriteUint32RegToMem(InterpreterContext& ctx) {
    Uint32 TregType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 memoryOffset = ctx.theInstruction >> 16;
    Uint64 Tvalue = *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 val = (Uint32)Tvalue;
    if (unlikely(TregType != Interpreter::REG_TYPE_UINT)) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 3))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    memcpy(&ctx.TheapMemoryChar[memoryOffset], &val, 4);
    return INTERP_CONTINUE;
  }

  /* WRITE_INT64_REG_TO_MEM — write 8 bytes from register to heap (immediate)
   *
   * Phase I.18: strict typing.  Source register must be REG_TYPE_INT
   * (signed Int64).  Unsigned / float / NULL sources rejected — use
   * WRITE_REG_TO_MEM_ANY for type-agnostic 8-byte writes. */
  static inline int handleWriteInt64RegToMem(InterpreterContext& ctx) {
    Uint32 TregType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 memoryOffset = ctx.theInstruction >> 16;
    Int64 Tvalue = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    if (unlikely(TregType != Interpreter::REG_TYPE_INT)) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 7))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    memcpy(&ctx.TheapMemoryChar[memoryOffset], &Tvalue, 8);
    return INTERP_CONTINUE;
  }

  /* WRITE_REG_TO_MEM_ANY — type-agnostic 8-byte register-to-heap
   * write (immediate offset).
   *
   * Phase I.18: copies the register's slots 2-3 verbatim (8 bytes)
   * regardless of source type — Int64, Uint64, and IEEE-754 double
   * already share a 64-bit canonical bit pattern in the register.
   * Only NULL source is rejected.  Producers that need to spill a
   * non-matching width through a typed-named opcode can switch to
   * this opcode instead. */
  static inline int handleWriteRegToMemAny(InterpreterContext& ctx) {
    Uint32 TregType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 memoryOffset = ctx.theInstruction >> 16;
    if (unlikely(TregType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 7))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    memcpy(&ctx.TheapMemoryChar[memoryOffset],
           ctx.TregMemBuffer + ctx.theRegister + 2,
           8);
    return INTERP_CONTINUE;
  }

  /* WRITE_UINT8_REG_TO_REG — write 1 byte from register to heap, offset in register
   *
   * Phase I.18: strict typing.  Source value register must be
   * REG_TYPE_UINT.  Memory-offset register must be non-NULL and
   * non-float (signed or unsigned offset both fine). */
  static inline int handleWriteUint8RegToReg(InterpreterContext& ctx) {
    Uint32 registerOffset = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TregType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 memoryOffsetType = ctx.TregMemBuffer[registerOffset];
    Int64 memoryOffset = *(Int64*)(ctx.TregMemBuffer + registerOffset + 2);
    Uint64 Tvalue = *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint8 val = (Uint8)Tvalue;
    if (unlikely(TregType != Interpreter::REG_TYPE_UINT ||
                 memoryOffsetType == NULL_INDICATOR ||
                 reg_is_float(memoryOffsetType))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 7))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    memcpy(&ctx.TheapMemoryChar[memoryOffset], &val, 1);
    return INTERP_CONTINUE;
  }

  /* WRITE_UINT16_REG_TO_REG — write 2 bytes from register, offset in register */
  static inline int handleWriteUint16RegToReg(InterpreterContext& ctx) {
    Uint32 registerOffset = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TregType = ctx.TregMemBuffer[ctx.theRegister];
    Int64 memoryOffset = *(Int64*)(ctx.TregMemBuffer + registerOffset + 2);
    Uint32 memoryOffsetType = ctx.TregMemBuffer[registerOffset];
    Uint64 Tvalue = *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint16 val = (Uint16)Tvalue;
    if (unlikely(TregType != Interpreter::REG_TYPE_UINT ||
                 memoryOffsetType == NULL_INDICATOR ||
                 reg_is_float(memoryOffsetType))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 7))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    memcpy(&ctx.TheapMemoryChar[memoryOffset], &val, 2);
    return INTERP_CONTINUE;
  }

  /* WRITE_UINT32_REG_TO_REG — write 4 bytes from register, offset in register */
  static inline int handleWriteUint32RegToReg(InterpreterContext& ctx) {
    Uint32 registerOffset = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TregType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 memoryOffsetType = ctx.TregMemBuffer[registerOffset];
    Int64 memoryOffset = *(Int64*)(ctx.TregMemBuffer + registerOffset + 2);
    Uint64 Tvalue = *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 val = (Uint32)Tvalue;
    if (unlikely(TregType != Interpreter::REG_TYPE_UINT ||
                 memoryOffsetType == NULL_INDICATOR ||
                 reg_is_float(memoryOffsetType))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 7))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    memcpy(&ctx.TheapMemoryChar[memoryOffset], &val, 4);
    return INTERP_CONTINUE;
  }

  /* WRITE_INT64_REG_TO_REG — write 8 bytes from register, offset in register
   *
   * Phase I.18: strict typing.  Source value register must be
   * REG_TYPE_INT.  Memory-offset register must be non-NULL and
   * non-float. */
  static inline int handleWriteInt64RegToReg(InterpreterContext& ctx) {
    Uint32 registerOffset = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TregType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 memoryOffsetType = ctx.TregMemBuffer[registerOffset];
    Int64 memoryOffset = *(Int64*)(ctx.TregMemBuffer + registerOffset + 2);
    Int64 Tvalue = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    if (unlikely(TregType != Interpreter::REG_TYPE_INT ||
                 memoryOffsetType == NULL_INDICATOR ||
                 reg_is_float(memoryOffsetType))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 7))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    memcpy(&ctx.TheapMemoryChar[memoryOffset], &Tvalue, 8);
    return INTERP_CONTINUE;
  }

  /* READ_INTERPRETER_INPUT — load value from an interpreter input slot
   *
   * Phase I.18: producer.  Interpreter input slots are opaque 8-byte
   * values; mark the destination register as REG_TYPE_INT (signed
   * Int64 is the historical default and bit-identical to
   * NOT_NULL_INDICATOR). */
  static inline int handleReadInterpreterInput(InterpreterContext& ctx) {
    Uint32 inputInx = ctx.theInstruction >> 16;
    Int64* value_ptr = (Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    if (unlikely(inputInx >= AttributeHeader::MaxInterpreterInputIndex)) {
      return -ZINPUT_OUTPUT_INDEX_ERROR;
    }
    memcpy(value_ptr, &ctx.tup->m_interpreter_input[inputInx], 8);
    ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_INT;
    return INTERP_CONTINUE;
  }

  /* WRITE_INTERPRETER_OUTPUT — store register into an interpreter output slot
   *
   * Phase I.18: type-agnostic writer (interpreter outputs hold any
   * 64-bit value).  Reject float and NULL; signed and unsigned both
   * fine. */
  static inline int handleWriteInterpreterOutput(InterpreterContext& ctx) {
    Uint32 valueType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 outputInx = ctx.theInstruction >> 16;
    Int64* value_ptr = (Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    if (unlikely(valueType == NULL_INDICATOR || reg_is_float(valueType))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(outputInx >= AttributeHeader::MaxInterpreterOutputIndex)) {
      return -ZINPUT_OUTPUT_INDEX_ERROR;
    }
    outputInx *= 2;
    memcpy(&ctx.tup->c_interpreter_output[outputInx], value_ptr, 8);
    return INTERP_CONTINUE;
  }

  /* READ_AGG_REG_TO_REG — aggregation-embedded-only register import */
  static inline int handleReadAggRegToReg(InterpreterContext& ctx) {
    if (ctx.aggRegisters == nullptr) {
      return -ZNO_INSTRUCTION_ERROR;
    }
    Uint32 aggReg = ctx.theInstruction >> 16;
    if (unlikely(aggReg >= kRegTotal)) {
      return -ZREGISTER_INIT_ERROR;
    }
    const Register& src = ctx.aggRegisters[aggReg];
    if (src.is_null) {
      ctx.TregMemBuffer[ctx.theRegister] = NULL_INDICATOR;
      ctx.TregMemBuffer[ctx.theRegister + 2] = 0;
      ctx.TregMemBuffer[ctx.theRegister + 3] = 0;
      return INTERP_CONTINUE;
    }
    if (src.type == NDB_TYPE_DOUBLE) {
      ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_DOUBLE;
      memcpy(ctx.TregMemBuffer + ctx.theRegister + 2,
             &src.value.val_double,
             8);
      return INTERP_CONTINUE;
    }
    if (unlikely(src.type != NDB_TYPE_BIGINT)) {
      return -ZREGISTER_INIT_ERROR;
    }
    /* Phase I.18: carry signedness through to the normal interpreter
     * register's type word. */
    ctx.TregMemBuffer[ctx.theRegister] =
        src.is_unsigned ? Interpreter::REG_TYPE_UINT
                        : Interpreter::REG_TYPE_INT;
    if (src.is_unsigned) {
      *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2) =
          src.value.val_uint64;
    } else {
      *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2) =
          src.value.val_int64;
    }
    return INTERP_CONTINUE;
  }

  /* READ_LINKED_COLUMN_TO_REG (Phase I.5 v5) — type-aware
   * linked-attr-buffer load.  Walks the linked buffer to the
   * requested 8-bit position, reads the AttributeHeader, and
   * decodes the value into a normal interpreter register according
   * to the supplied 8-bit NDB_TYPE_* code.  Replaces the existing
   * READ_LINKED_TO_MEM + READ_*_MEM_TO_REG_CONST sequence for the
   * integer family and adds correct sign extension for signed
   * sub-64-bit widths.
   *
   * Encoding: bits 6..8 dest reg, bits 16..23 position, bits
   * 24..31 NDB column type.  See Interpreter.hpp.
   *
   * Supported types: Tinyint, Tinyunsigned, Smallint,
   * Smallunsigned, Mediumint, Mediumunsigned, Int, Unsigned,
   * Bigint, Bigunsigned.  Other type codes return
   * -ZNO_INSTRUCTION_ERROR. */
  static inline int handleReadLinkedColumnToReg(InterpreterContext& ctx) {
    Uint32 position = (ctx.theInstruction >> 16) & 0xFF;
    Uint32 type     = (ctx.theInstruction >> 24) & 0xFF;

    /* Walk the linked-attr buffer to the requested position.  Same
     * loop shape as handleReadLinkedToMem (per-entry layout is
     * tableId, schemaVersion, AttrHeader, data). */
    const Uint32* linked = ctx.req_struct->m_linked_attr_data;
    Uint32 linked_len = ctx.req_struct->m_linked_attr_len;
    if (unlikely(linked == nullptr)) {
      ctx.TregMemBuffer[ctx.theRegister] = NULL_INDICATOR;
      return INTERP_CONTINUE;
    }
    const Uint32* p = linked;
    const Uint32* p_end = linked + linked_len;
    Uint32 pos_count = 0;
    while (p < p_end) {
      if (pos_count == position) break;
      p += 2;  /* skip tableId, schemaVersion */
      p += 1 + AttributeHeader::getDataSize(*p);
      pos_count++;
    }
    if (unlikely(p >= p_end)) {
      ctx.TregMemBuffer[ctx.theRegister] = NULL_INDICATOR;
      return INTERP_CONTINUE;
    }

    /* Skip tableId and schemaVersion. */
    p += 2;

    /* Inspect the AttributeHeader. */
    AttributeHeader ah(*p);
    if (ah.isNULL()) {
      ctx.TregMemBuffer[ctx.theRegister] = NULL_INDICATOR;
      return INTERP_CONTINUE;
    }

    /* Decode the value.  Data starts immediately after the
     * AttrHeader word, so &p[1] is the first data byte boundary
     * (aligned to a Uint32 word). */
    const char* data = reinterpret_cast<const char*>(p + 1);
    Int64 sval = 0;
    Uint64 uval = 0;
    bool is_unsigned = false;
    switch (type) {
      case NDB_TYPE_TINYINT:
        sval = *reinterpret_cast<const Int8*>(data);
        break;
      case NDB_TYPE_TINYUNSIGNED:
        uval = *reinterpret_cast<const Uint8*>(data);
        is_unsigned = true;
        break;
      case NDB_TYPE_SMALLINT:
        sval = sint2korr(data);
        break;
      case NDB_TYPE_SMALLUNSIGNED:
        uval = uint2korr(data);
        is_unsigned = true;
        break;
      case NDB_TYPE_MEDIUMINT:
        sval = sint3korr(data);
        break;
      case NDB_TYPE_MEDIUMUNSIGNED:
        uval = uint3korr(data);
        is_unsigned = true;
        break;
      case NDB_TYPE_INT:
        sval = sint4korr(data);
        break;
      case NDB_TYPE_UNSIGNED:
        uval = uint4korr(data);
        is_unsigned = true;
        break;
      case NDB_TYPE_BIGINT:
        memcpy(&sval, data, 8);
        break;
      case NDB_TYPE_BIGUNSIGNED:
        memcpy(&uval, data, 8);
        is_unsigned = true;
        break;
      case NDB_TYPE_FLOAT: {
        /* Phase I.5 v3: load 4-byte FLOAT, widen to double, tag
         * REG_TYPE_DOUBLE.  Early-return because the unified tail
         * below would mis-tag this as REG_TYPE_INT. */
        float fval;
        memcpy(&fval, data, 4);
        double dval = static_cast<double>(fval);
        ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_DOUBLE;
        memcpy(ctx.TregMemBuffer + ctx.theRegister + 2, &dval, 8);
        return INTERP_CONTINUE;
      }
      case NDB_TYPE_DOUBLE:
        ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_DOUBLE;
        memcpy(ctx.TregMemBuffer + ctx.theRegister + 2, data, 8);
        return INTERP_CONTINUE;
      default:
        return -ZNO_INSTRUCTION_ERROR;
    }

    /* Phase I.18: tag the register's type word so consumers know
     * whether the payload is signed or unsigned. */
    ctx.TregMemBuffer[ctx.theRegister] =
        is_unsigned ? Interpreter::REG_TYPE_UINT
                    : Interpreter::REG_TYPE_INT;
    if (is_unsigned) {
      *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2) = uval;
    } else {
      *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2) = sval;
    }
    return INTERP_CONTINUE;
  }

  /* CONVERT_SIZE — decode 2-byte little-endian length into destination register
   *
   * Phase I.18: source register is a memory offset (signed or
   * unsigned integer; reject float and NULL).  Destination is a
   * non-negative size value tagged REG_TYPE_INT. */
  static inline int handleConvertSize(InterpreterContext& ctx) {
    Uint32 offsetType = ctx.TregMemBuffer[ctx.theRegister];
    Int64 memoryOffset = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 TdestRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    if (unlikely(offsetType == NULL_INDICATOR || reg_is_float(offsetType))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 1) || memoryOffset < 0)) {
      thrjam(ctx.tup->jamBuffer());
      return -ZMEMORY_OFFSET_ERROR;
    }
    Uint32 low_byte = ctx.TheapMemoryChar[memoryOffset];
    Uint32 high_byte = ctx.TheapMemoryChar[memoryOffset + 1];
    Uint32 size_read = low_byte + (256 * high_byte);
    *(Int64*)(ctx.TregMemBuffer + TdestRegister + 2) = (Int64)size_read;
    ctx.TregMemBuffer[TdestRegister] = Interpreter::REG_TYPE_INT;
    return INTERP_CONTINUE;
  }

  /* WRITE_SIZE_MEM — encode 2-byte little-endian length from register to heap */
  static inline int handleWriteSizeMem(InterpreterContext& ctx) {
    Uint32 TsizeRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 offsetType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 sizeType = ctx.TregMemBuffer[TsizeRegister];
    Int64 memoryOffset = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    if (unlikely(offsetType == NULL_INDICATOR || sizeType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 1) || memoryOffset < 0)) {
      thrjam(ctx.tup->jamBuffer());
      return -ZMEMORY_OFFSET_ERROR;
    }
    Int64 size = *(Int64*)(ctx.TregMemBuffer + TsizeRegister + 2);
    if (unlikely(size <= 0 || size >= (MAX_VAR_SIZE_IN_WORDS * 4))) {
      return -ZPARTIAL_READ_ERROR;
    }
    Uint32 low_byte = size & 255;
    Uint32 high_byte = size >> 8;
    ctx.TheapMemoryChar[memoryOffset] = low_byte;
    ctx.TheapMemoryChar[memoryOffset + 1] = high_byte;
    return INTERP_CONTINUE;
  }

  /* --- Batch 8 --- attribute read/write ops and string conversion */

  /* LOAD_OP_TYPE — load current operation type (INSERT/UPDATE/DELETE/…) */
  static inline int handleLoadOpType(InterpreterContext& ctx) {
    Uint32 op_type = ctx.req_struct->operPtrP->op_type;
    ctx.TregMemBuffer[ctx.theRegister] = NOT_NULL_INDICATOR;
    *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2) = op_type & 7;
    return INTERP_CONTINUE;
  }

  /* READ_ATTR_INTO_REG — read tuple attribute into register
   *
   * Phase I.18: type-aware register write.  After readAttributes
   * returns the raw column data, the column descriptor is consulted
   * to apply correct sign extension on signed sub-Bigint integers,
   * and to tag the register with REG_TYPE_INT / REG_TYPE_UINT /
   * REG_TYPE_DOUBLE depending on the source type.  Pre-I.18 the
   * handler implicitly zero-extended every 32-bit data word into
   * the Int64 payload, which silently produced wrong values for
   * signed TINYINT / SMALLINT / MEDIUMINT / INT columns containing
   * negative values (this was the bug surfaced by the I.5 v5 MTR
   * fixture). */
  static inline int handleReadAttrIntoReg(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3;  // A bit heavier instruction
    Uint32 theAttrinfo = (ctx.theInstruction & 0xFFFF0000);
    int TnoDataRW = ctx.tup->readAttributes(
        ctx.req_struct, &theAttrinfo, (Uint32)1,
        &ctx.TregMemBuffer[ctx.theRegister], (Uint32)3);
    if (TnoDataRW == 2) {
      // 32-bit cell read.  TregMemBuffer[theRegister + 1] holds the
      // raw column data word (after the AttrHeader at slot 0).
      // Inspect the column descriptor to know how wide the actual
      // value is and whether it is signed.
      thrjamDebug(ctx.tup->jamBuffer());
      Uint32 attrId = theAttrinfo >> 16;
      const Uint32 attrDescIndex = attrId * ZAD_SIZE;
      Uint32 attrDesc1 =
          ctx.req_struct->tablePtrP->tabDescriptor[attrDescIndex];
      Uint32 typeId = AttributeDescriptor::getType(attrDesc1);
      const char* dataPtr =
          reinterpret_cast<const char*>(
              &ctx.TregMemBuffer[ctx.theRegister + 1]);
      switch (typeId) {
        case NDB_TYPE_TINYINT:
          *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2) =
              (Int64)*reinterpret_cast<const Int8*>(dataPtr);
          ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_INT;
          break;
        case NDB_TYPE_TINYUNSIGNED:
          *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2) =
              (Uint64)*reinterpret_cast<const Uint8*>(dataPtr);
          ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_UINT;
          break;
        case NDB_TYPE_SMALLINT:
          *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2) =
              (Int64)(Int16)sint2korr(dataPtr);
          ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_INT;
          break;
        case NDB_TYPE_SMALLUNSIGNED:
          *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2) =
              (Uint64)uint2korr(dataPtr);
          ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_UINT;
          break;
        case NDB_TYPE_MEDIUMINT:
          *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2) =
              (Int64)sint3korr(dataPtr);
          ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_INT;
          break;
        case NDB_TYPE_MEDIUMUNSIGNED:
          *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2) =
              (Uint64)uint3korr(dataPtr);
          ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_UINT;
          break;
        case NDB_TYPE_INT:
          *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2) =
              (Int64)sint4korr(dataPtr);
          ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_INT;
          break;
        case NDB_TYPE_UNSIGNED:
          *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2) =
              (Uint64)uint4korr(dataPtr);
          ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_UINT;
          break;
        case NDB_TYPE_FLOAT: {
          // Single-precision float in 4 bytes.  Promote to double
          // and store as REG_TYPE_DOUBLE.
          float fval;
          memcpy(&fval, dataPtr, 4);
          double dval = (double)fval;
          memcpy(ctx.TregMemBuffer + ctx.theRegister + 2, &dval, 8);
          ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_DOUBLE;
          break;
        }
        default:
          // Unknown / unsupported type for typed register loading
          // — preserve the historical zero-extension behaviour and
          // mark the register as a default signed integer so
          // callers that don't care about typed semantics keep
          // working.
          *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2) =
              ctx.TregMemBuffer[ctx.theRegister + 1];
          ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_INT;
          break;
      }
    } else if (TnoDataRW == 3) {
      // 64-bit cell read.  Two data words at slots 1 and 2 — repack
      // into the canonical Int64 / Uint64 / double layout in slots
      // 2-3.
      thrjamDebug(ctx.tup->jamBuffer());
      Uint32 lowWord  = ctx.TregMemBuffer[ctx.theRegister + 1];
      Uint32 highWord = ctx.TregMemBuffer[ctx.theRegister + 2];
      ctx.TregMemBuffer[ctx.theRegister + 2] = lowWord;
      ctx.TregMemBuffer[ctx.theRegister + 3] = highWord;
      Uint32 attrId = theAttrinfo >> 16;
      const Uint32 attrDescIndex = attrId * ZAD_SIZE;
      Uint32 attrDesc1 =
          ctx.req_struct->tablePtrP->tabDescriptor[attrDescIndex];
      Uint32 typeId = AttributeDescriptor::getType(attrDesc1);
      switch (typeId) {
        case NDB_TYPE_BIGUNSIGNED:
          ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_UINT;
          break;
        case NDB_TYPE_DOUBLE:
          ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_DOUBLE;
          break;
        case NDB_TYPE_BIGINT:
        default:
          // Bigint and any other 8-byte source defaults to signed
          // Int64.
          ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_INT;
          break;
      }
    } else if (TnoDataRW == 1) {
      // NULL value
      thrjamDebug(ctx.tup->jamBuffer());
      ctx.TregMemBuffer[ctx.theRegister] = NULL_INDICATOR;
      ctx.TregMemBuffer[ctx.theRegister + 2] = 0;
      ctx.TregMemBuffer[ctx.theRegister + 3] = 0;
    } else if (TnoDataRW < 0) {
      thrjamDebug(ctx.tup->jamBuffer());
      return TnoDataRW;  /* already negative of error code */
    } else {
      // Any other value is an unexpected read result — same as ndbabort()
      // but with an explicit block pointer (free function has no `this`).
      jamNoBlock();
      ctx.tup->progError(__LINE__, NDBD_EXIT_PRGERR, __FILE__, "");
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_MEM_OP_ARG — like BRANCH_ATTR_OP_ARG but reads column data from
   * heap memory (placed there by READ_LINKED_TO_MEM) instead of calling
   * readAttributes(). Uses tableId + schemaVersion from the instruction
   * stream to look up the parent table descriptor for type/charset info.
   *
   * Layout: [opcode+cond, attrId|argLen, tableId, schemaVer, data...]
   *
   * This is THE critical instruction for CTE filter mode — CTE rows are
   * loaded into heap memory as linked-attribute data and compared with
   * literal constants via this instruction. */
  static inline int handleBranchMemOpArg(InterpreterContext& ctx) {
    thrjamDebug(ctx.tup->jamBuffer());
    const Uint32 ins2 = ctx.TcurrentProgram[ctx.TprogramCounter];
    Uint32 attrId = Interpreter::getBranchCol_AttrId(ins2);
    Uint32 argLen = Interpreter::getBranchCol_Len(ins2);
    Uint32 tableId = ctx.TcurrentProgram[ctx.TprogramCounter + 1];
    Uint32 schemaVersion = ctx.TcurrentProgram[ctx.TprogramCounter + 2];

    // Look up the parent table by tableId for type/charset info.
    // Validate table exists, is DEFINED, and schemaVersion matches
    // (via DBLQH's table record which tracks schema versions).
    if (unlikely(tableId >= ctx.tup->cnoOfTablerec)) {
      thrjam(ctx.tup->jamBuffer());
      return -40;
    }
    Tablerec* parentTablePtrP = &ctx.tup->tablerec[tableId];
    if (unlikely(parentTablePtrP->tableStatus != DEFINED)) {
      thrjam(ctx.tup->jamBuffer());
      return -40;
    }
    if (unlikely(tableId >= ctx.tup->c_lqh->ctabrecFileSize ||
                 ctx.tup->c_lqh->tablerec[tableId].schemaVersion !=
                     schemaVersion)) {
      thrjam(ctx.tup->jamBuffer());
      return -40;
    }

    // Read column data from heap (written by READ_LINKED_TO_MEM)
    const Uint32* memData = (const Uint32*)&ctx.TheapMemoryChar[0];
    const AttributeHeader ah(memData[0]);

    // Get type info from the parent table's descriptor
    const Uint32* attrDescriptor =
        parentTablePtrP->tabDescriptor + (attrId * ZAD_SIZE);
    const Uint32 TattrDesc1 = attrDescriptor[0];
    const Uint32 TattrDesc2 = attrDescriptor[1];
    const Uint32 typeId = AttributeDescriptor::getType(TattrDesc1);
    const CHARSET_INFO* cs = nullptr;
    if (AttributeOffset::getCharsetFlag(TattrDesc2)) {
      const Uint32 pos = AttributeOffset::getCharsetPos(TattrDesc2);
      cs = parentTablePtrP->charsetArray[pos];
    }
    const NdbSqlUtil::Type& sqlType = NdbSqlUtil::getType(typeId);

    Uint32 attrLen = AttributeDescriptor::getSizeInBytes(TattrDesc1);
    const char* s1 = (const char*)&memData[1];
    // Inline constant starts after word 0 (ins2) + word 1 (tableId) +
    // word 2 (schemaVersion)
    const char* s2 = (const char*)&ctx.TcurrentProgram[ctx.TprogramCounter + 3];
    const Uint32 step = argLen;

    const bool r1_null = ah.isNULL();
    const bool r2_null = (argLen == 0);

    if (r1_null || r2_null) {
      const Uint32 nullSemantics =
          Interpreter::getNullSemantics(ctx.theInstruction);
      if (nullSemantics == Interpreter::IF_NULL_BREAK_OUT) {
        ctx.TprogramCounter =
            ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
        return INTERP_CONTINUE;
      }
      if (nullSemantics == Interpreter::IF_NULL_CONTINUE) {
        // Skip: 1(ins2) + 2(tableId,schemaVer) + data words
        const Uint32 tmp = ((step + 3) >> 2) + 3;
        ctx.TprogramCounter += tmp;
        return INTERP_CONTINUE;
      }
    }

    const Uint32 cond = Interpreter::getBinaryCondition(ctx.theInstruction);
    int res1;
    if (r1_null || r2_null) {
      res1 = r1_null && r2_null ? 0 : r1_null ? -1 : 1;
    } else {
      if (unlikely(sqlType.m_cmp == 0)) {
        return -40;
      }
      res1 = (*sqlType.m_cmp)(cs, s1, attrLen, s2, argLen);
    }

    bool res = false;
    switch (cond) {
      case Interpreter::EQ: res = (res1 == 0); break;
      case Interpreter::NE: res = (res1 != 0); break;
      case Interpreter::LT: res = (res1 > 0); break;   // inverted
      case Interpreter::LE: res = (res1 >= 0); break;
      case Interpreter::GT: res = (res1 < 0); break;
      case Interpreter::GE: res = (res1 <= 0); break;
      default: break;
    }

    if (res) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    } else {
      // Skip: 1(ins2) + 2(tableId,schemaVer) + data words
      Uint32 tmp = ((step + 3) >> 2) + 3;
      ctx.TprogramCounter += tmp;
    }
    return INTERP_CONTINUE;
  }

  /* BRANCH_MEM_OP_ARG_INLINE_TYPE — like BRANCH_MEM_OP_ARG, but the
   * column descriptor (type, length, charset) is encoded inline in
   * the program rather than looked up via tableId+schemaVersion in
   * tablerec[]. Used by the CTE filter interpreter to compare against
   * synthesized aggregate result values for which no real registered
   * NDB column exists (e.g. SUM produces Bigint; the synthetic CTE
   * virt table is not registered in DBTUP).
   *
   * Layout: [opcode+cond, typeId|argLen, columnSizeBytes<<16|csNumber,
   *          data...]
   */
  static inline int handleBranchMemOpArgInlineType(InterpreterContext& ctx) {
    thrjamDebug(ctx.tup->jamBuffer());
    const Uint32 ins2 = ctx.TcurrentProgram[ctx.TprogramCounter];
    const Uint32 typeId = Interpreter::getBranchCol_AttrId(ins2);
    const Uint32 argLen = Interpreter::getBranchCol_Len(ins2);
    const Uint32 meta   = ctx.TcurrentProgram[ctx.TprogramCounter + 1];
    const Uint32 attrLen  = (meta >> 16) & 0xFFFF;
    const Uint32 csNumber = meta & 0xFFFF;

    const NdbSqlUtil::Type& sqlType = NdbSqlUtil::getType(typeId);
    if (unlikely(sqlType.m_cmp == 0)) {
      thrjam(ctx.tup->jamBuffer());
      return -40;
    }

    const CHARSET_INFO* cs = nullptr;
    if (csNumber != 0) {
      if (unlikely(csNumber >= MY_ALL_CHARSETS_SIZE)) {
        thrjam(ctx.tup->jamBuffer());
        return -40;
      }
      cs = all_charsets[csNumber];
      if (unlikely(cs == nullptr)) {
        thrjam(ctx.tup->jamBuffer());
        return -40;
      }
    }

    // Read column data from heap (written by READ_LINKED_TO_MEM)
    const Uint32* memData = (const Uint32*)&ctx.TheapMemoryChar[0];
    const AttributeHeader ah(memData[0]);
    const char* s1 = (const char*)&memData[1];
    // Inline constant starts after word 0 (ins2) + word 1 (meta)
    const char* s2 = (const char*)&ctx.TcurrentProgram[ctx.TprogramCounter + 2];
    const Uint32 step = argLen;

    const bool r1_null = ah.isNULL();
    const bool r2_null = (argLen == 0);

    if (r1_null || r2_null) {
      const Uint32 nullSemantics =
          Interpreter::getNullSemantics(ctx.theInstruction);
      if (nullSemantics == Interpreter::IF_NULL_BREAK_OUT) {
        ctx.TprogramCounter =
            ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
        return INTERP_CONTINUE;
      }
      if (nullSemantics == Interpreter::IF_NULL_CONTINUE) {
        // Skip: 1(ins2) + 1(meta) + data words
        const Uint32 tmp = ((step + 3) >> 2) + 2;
        ctx.TprogramCounter += tmp;
        return INTERP_CONTINUE;
      }
    }

    const Uint32 cond = Interpreter::getBinaryCondition(ctx.theInstruction);
    int res1;
    if (r1_null || r2_null) {
      res1 = r1_null && r2_null ? 0 : r1_null ? -1 : 1;
    } else {
      res1 = (*sqlType.m_cmp)(cs, s1, attrLen, s2, argLen);
    }

    bool res = false;
    switch (cond) {
      case Interpreter::EQ: res = (res1 == 0); break;
      case Interpreter::NE: res = (res1 != 0); break;
      case Interpreter::LT: res = (res1 > 0); break;   // inverted
      case Interpreter::LE: res = (res1 >= 0); break;
      case Interpreter::GT: res = (res1 < 0); break;
      case Interpreter::GE: res = (res1 <= 0); break;
      default: break;
    }

    if (res) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    } else {
      // Skip: 1(ins2) + 1(meta) + data words
      Uint32 tmp = ((step + 3) >> 2) + 2;
      ctx.TprogramCounter += tmp;
    }
    return INTERP_CONTINUE;
  }

  /* WRITE_ATTR_FROM_REG — write register value to tuple attribute */
  static inline int handleWriteAttrFromReg(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3;  // A bit heavier instruction
    Uint32 TattrId = ctx.theInstruction >> 16;
    Uint32 TattrDescrIndex = (TattrId * ZAD_SIZE);
    Uint32 TregType = ctx.TregMemBuffer[ctx.theRegister];
    thrjamDebug(ctx.tup->jamBuffer());
    thrjamDataDebug(ctx.tup->jamBuffer(), TattrId);

    if (unlikely(TattrId >= ctx.req_struct->tablePtrP->m_no_of_attributes)) {
      return -ZATTRIBUTE_ID_ERROR;
    }
    /* Phase I.18: column write is type-agnostic on signed/unsigned
     * integers (the in-register bit pattern is the canonical
     * representation), but reject float-typed registers until
     * float-to-integer column-coercion semantics are designed. */
    if (unlikely(reg_is_float(TregType))) {
      return -ZREGISTER_INIT_ERROR;
    }
    Uint32 TattrDesc1 =
        ctx.req_struct->tablePtrP->tabDescriptor[TattrDescrIndex];
    Uint32 TattrNoOfWords = AttributeDescriptor::getSizeInWords(TattrDesc1);
    Uint32 Toptype = ctx.req_struct->operPtrP->op_type;
    Uint32 TdataForUpdate[3];
    Uint32 Tlen;

    AttributeHeader ah(TattrId, TattrNoOfWords << 2);
    TdataForUpdate[0] = ah.m_value;
    TdataForUpdate[1] = ctx.TregMemBuffer[ctx.theRegister + 2];
    TdataForUpdate[2] = ctx.TregMemBuffer[ctx.theRegister + 3];
    Tlen = TattrNoOfWords + 1;
    if (Toptype == ZUPDATE || Toptype == ZINSERT) {
      if (TattrNoOfWords <= 2) {
        if (TattrNoOfWords == 1) {
          thrjamDebug(ctx.tup->jamBuffer());
          Int64* tmp = new (&ctx.TregMemBuffer[ctx.theRegister + 2]) Int64;
          TdataForUpdate[1] = Uint32(*tmp);
          TdataForUpdate[2] = 0;
        }
        if (TregType == NULL_INDICATOR) {
          thrjamDebug(ctx.tup->jamBuffer());
          ah.setNULL();
          TdataForUpdate[0] = ah.m_value;
          Tlen = 1;
        }
        int TnoDataRW = ctx.tup->updateAttributes(
            ctx.req_struct, &TdataForUpdate[0], Tlen);
        if (TnoDataRW < 0) {
          return TnoDataRW;  /* already negative of error code */
        }
      } else {
        return -ZREGISTER_INIT_ERROR;
      }
    } else {
      return -ZTRY_TO_UPDATE_ERROR;
    }
    return INTERP_CONTINUE;
  }

  /* WRITE_ATTR_FROM_MEM — write heap memory content to tuple attribute */
  static inline int handleWriteAttrFromMem(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3;
    Uint32 attrId = ctx.theInstruction >> 16;
    thrjamDebug(ctx.tup->jamBuffer());
    thrjamDataDebug(ctx.tup->jamBuffer(), attrId);
    Uint32 attrDescrIndex = (attrId * ZAD_SIZE);
    Uint32 TsizeRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TregOffsetType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TregSizeType = ctx.TregMemBuffer[TsizeRegister];
    Int64 Toffset = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Int64 Tsize = *(Int64*)(ctx.TregMemBuffer + TsizeRegister + 2);
    Uint32 Toptype = ctx.req_struct->operPtrP->op_type;

    if (unlikely(attrId >= ctx.req_struct->tablePtrP->m_no_of_attributes)) {
      return -ZATTRIBUTE_ID_ERROR;
    }
    Uint32 attrDesc1 = ctx.req_struct->tablePtrP->tabDescriptor[attrDescrIndex];
    Uint32 attrNoOfBytes = AttributeDescriptor::getSizeInBytes(attrDesc1);
    if (unlikely((TregOffsetType == NULL_INDICATOR) ||
                 (TregSizeType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(((Toffset + Tsize) > MAX_HEAP_OFFSET) ||
                 ((Toffset & 3) != 0) || (Toffset < 0))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    if (unlikely((Tsize < 0) || (Tsize > attrNoOfBytes))) {
      return -ZWRITE_SIZE_TOO_BIG_ERROR;
    }
    if (unlikely(Toptype != ZUPDATE && Toptype != ZINSERT)) {
      return -ZTRY_TO_UPDATE_ERROR;
    }
    AttributeHeader ah(attrId, Tsize);
    Uint32* memory_ptr = (Uint32*)&ctx.TheapMemoryChar[Toffset];
    Uint32 words = 1 + (Tsize + 3) / 4;
    memory_ptr[0] = ah.m_value;
    int TnoDataRW = ctx.tup->updateAttributes(ctx.req_struct, memory_ptr, words);
    if (TnoDataRW < 0) {
      return TnoDataRW;  /* already negative of error code */
    }
    return INTERP_CONTINUE;
  }

  /* WRITE_PARTIAL_ATTR_FROM_MEM — partial update of a var-length column */
  static inline int handleWritePartialAttrFromMem(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3;
    Uint32 attrId = ctx.theInstruction >> 16;
    Uint32 TsizeRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TstartPosRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    Uint32 attrDescrIndex = (attrId * ZAD_SIZE);
    Uint32 TregOffsetType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TregSizeType = ctx.TregMemBuffer[TsizeRegister];
    Uint32 TstartPosType = ctx.TregMemBuffer[TstartPosRegister];
    Int64 Toffset = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Int64 Tsize = *(Int64*)(ctx.TregMemBuffer + TsizeRegister + 2);
    Int64 TstartPos = *(Int64*)(ctx.TregMemBuffer + TstartPosRegister + 2);
    Uint32 Toptype = ctx.req_struct->operPtrP->op_type;

    if (unlikely(attrId >= ctx.req_struct->tablePtrP->m_no_of_attributes)) {
      return -ZATTRIBUTE_ID_ERROR;
    }
    Uint32 attrDesc1 = ctx.req_struct->tablePtrP->tabDescriptor[attrDescrIndex];
    Uint32 attrNoOfBytes = AttributeDescriptor::getSizeInBytes(attrDesc1);
    Uint32 array = AttributeDescriptor::getArrayType(attrDesc1);
    if (unlikely((TregOffsetType == NULL_INDICATOR) ||
                 (TregSizeType == NULL_INDICATOR ||
                  (TstartPosType == NULL_INDICATOR)))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(((Toffset + Tsize) > MAX_HEAP_OFFSET) ||
                 ((Toffset & Int64(3)) != 0) || (Toffset < Int64(0)))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    if (array != NDB_ARRAYTYPE_MEDIUM_VAR && array != NDB_ARRAYTYPE_SHORT_VAR) {
      return -ZAPPEND_ON_FIXED_SIZE_COLUMN_ERROR;
    }
    if (unlikely((Tsize < Int64(0)) ||
                 ((Tsize + TstartPos) > Int64(attrNoOfBytes)))) {
      return -ZWRITE_SIZE_TOO_BIG_ERROR;
    }
    if (unlikely(Tsize == Int64(0))) {
      return -ZAPPEND_NULL_ERROR;
    }
    if (unlikely(Toptype != ZUPDATE && Toptype != ZINSERT)) {
      return -ZTRY_TO_UPDATE_ERROR;
    }
    AttributeHeader ah(AttributeHeader::SET_PARTIAL_COLUMN, Tsize);
    Uint32 extended_header = attrId + (TstartPos << 16);
    Uint32* memory_ptr = (Uint32*)&ctx.TheapMemoryChar[Toffset];
    Uint32 words = 2 + (Tsize + 3) / 4;
    memory_ptr[0] = ah.m_value;
    memory_ptr[1] = extended_header;
    int TnoDataRW = ctx.tup->updateAttributes(ctx.req_struct, memory_ptr, words);
    if (TnoDataRW < 0) {
      return TnoDataRW;
    }
    return INTERP_CONTINUE;
  }

  /* APPEND_ATTR_FROM_MEM — append heap memory to a var-length column */
  static inline int handleAppendAttrFromMem(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3;
    Uint32 attrId = ctx.theInstruction >> 16;
    Uint32 TsizeRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 attrDescrIndex = (attrId * ZAD_SIZE);
    Uint32 TregOffsetType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 TregSizeType = ctx.TregMemBuffer[TsizeRegister];
    Int64 Toffset = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Int64 Tsize = *(Int64*)(ctx.TregMemBuffer + TsizeRegister + 2);
    Uint32 Toptype = ctx.req_struct->operPtrP->op_type;

    if (unlikely(attrId >= ctx.req_struct->tablePtrP->m_no_of_attributes)) {
      return -ZATTRIBUTE_ID_ERROR;
    }
    Uint32 attrDesc1 = ctx.req_struct->tablePtrP->tabDescriptor[attrDescrIndex];
    Uint32 attrNoOfBytes = AttributeDescriptor::getSizeInBytes(attrDesc1);
    Uint32 array = AttributeDescriptor::getArrayType(attrDesc1);
    if (unlikely((TregOffsetType == NULL_INDICATOR) ||
                 (TregSizeType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(((Toffset + Tsize) > MAX_HEAP_OFFSET) ||
                 ((Toffset & Int64(3)) != 0) || (Toffset < Int64(0)))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    if (array != NDB_ARRAYTYPE_MEDIUM_VAR && array != NDB_ARRAYTYPE_SHORT_VAR) {
      return -ZAPPEND_ON_FIXED_SIZE_COLUMN_ERROR;
    }
    if (unlikely((Tsize < Int64(0)) || (Tsize > Int64(attrNoOfBytes)))) {
      return -ZWRITE_SIZE_TOO_BIG_ERROR;
    }
    if (unlikely(Tsize == Int64(0))) {
      return -ZAPPEND_NULL_ERROR;
    }
    if (unlikely(Toptype != ZUPDATE)) {
      return -ZTRY_TO_UPDATE_ERROR;
    }
    AttributeHeader ah(AttributeHeader::APPEND_COLUMN, Tsize);
    Uint32 extended_header = attrId;
    Uint32* memory_ptr = (Uint32*)&ctx.TheapMemoryChar[Toffset];
    Uint32 words = 2 + (Tsize + 3) / 4;
    memory_ptr[0] = ah.m_value;
    memory_ptr[1] = extended_header;
    int TnoDataRW = ctx.tup->updateAttributes(ctx.req_struct, memory_ptr, words);
    if (TnoDataRW < 0) {
      return TnoDataRW;
    }
    return INTERP_CONTINUE;
  }

  /* READ_PARTIAL_ATTR_TO_MEM — partial read of a var-length column to heap */
  static inline int handleReadPartialAttrToMem(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3;
    Uint32 ToffsetType = ctx.TregMemBuffer[ctx.theRegister];
    Int64 Toffset = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 TposRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 TsizeRegister = Interpreter::getReg4(ctx.theInstruction) << 2;
    Uint32 TposType = ctx.TregMemBuffer[TposRegister];
    Uint32 TsizeType = ctx.TregMemBuffer[TsizeRegister];
    if (unlikely((ToffsetType == NULL_INDICATOR) ||
                 (TposType == NULL_INDICATOR) ||
                 (TsizeType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(Toffset < 0 ||
                 (Toffset > ((HEAP_MEMORY_SIZE_DWORDS * 8) -
                             (MAX_VAR_SIZE_IN_WORDS * 4))) ||
                 ((Toffset & Int64(3)) != 0))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    Uint32 memory_offset = Uint32(Toffset);
    Int64 Tpos = *(Int64*)(ctx.TregMemBuffer + TposRegister + 2);
    if (unlikely(Tpos < 0 || Tpos >= (MAX_VAR_SIZE_IN_WORDS * 4))) {
      return -ZPARTIAL_READ_ERROR;
    }
    Uint32 read_pos = (Uint32)Tpos;
    Int64 Tsize = *(Int64*)(ctx.TregMemBuffer + TsizeRegister + 2);
    if (unlikely(Tsize <= 0 || Tsize >= (MAX_VAR_SIZE_IN_WORDS * 4))) {
      return -ZPARTIAL_READ_ERROR;
    }
    Uint32 read_size = (Uint32)Tsize;
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    Uint32 TattrId = ctx.theInstruction >> 19;
    AttributeHeader ah(TattrId, 0);
    ah.setPartialReadWriteFlag();
    Uint32 TdataForRead[2];
    TdataForRead[0] = ah.m_value;
    TdataForRead[1] = read_size | read_pos << 16;
    int TnoDataRW = ctx.tup->readAttributes(
        ctx.req_struct, &TdataForRead[0], (Uint32)2,
        (Uint32*)&ctx.TheapMemoryChar[memory_offset], (Uint32)MAX_VAR_SIZE_IN_WORDS);
    if (TnoDataRW < 0) {
      thrjamDebug(ctx.tup->jamBuffer());
      return TnoDataRW;
    }
    Uint32* memory_ptr = (Uint32*)&ctx.TheapMemoryChar[memory_offset];
    Uint32 header = *memory_ptr;
    AttributeHeader ah_read(header);
    if (ah_read.isNULL()) {
      ctx.TregMemBuffer[TdestRegister] = NULL_INDICATOR;
    } else {
      Uint32 read_len = ah_read.getByteSize();
      *(Int64*)(ctx.TregMemBuffer + TdestRegister + 2) = read_len;
      ctx.TregMemBuffer[TdestRegister] = NOT_NULL_INDICATOR;
    }
    return INTERP_CONTINUE;
  }

  /* READ_ATTR_TO_MEM — read an attribute into heap memory */
  static inline int handleReadAttrToMem(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3;
    Uint32 ToffsetType = ctx.TregMemBuffer[ctx.theRegister];
    Int64 Toffset = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 TdestRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    Uint32 TattrId = ctx.theInstruction >> 16;
    Uint32 theAttrinfo = (TattrId << 16);
    if (unlikely(ToffsetType == NULL_INDICATOR)) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(Toffset < 0 ||
                 (Toffset > ((HEAP_MEMORY_SIZE_DWORDS * 8) -
                             (MAX_VAR_SIZE_IN_WORDS * 4))) ||
                 ((Toffset & Int64(3)) != 0))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    Uint32 memory_offset = Uint32(Toffset);
    int TnoDataRW = ctx.tup->readAttributes(
        ctx.req_struct, &theAttrinfo, (Uint32)1,
        (Uint32*)&ctx.TheapMemoryChar[memory_offset], (Uint32)MAX_VAR_SIZE_IN_WORDS);
    if (TnoDataRW < 0) {
      thrjamDebug(ctx.tup->jamBuffer());
      return TnoDataRW;
    }
    Uint32* memory_ptr = (Uint32*)&ctx.TheapMemoryChar[memory_offset];
    Uint32 header = *memory_ptr;
    AttributeHeader ah(header);
    if (ah.isNULL()) {
      ctx.TregMemBuffer[TdestRegister] = NULL_INDICATOR;
    } else {
      Uint32 read_len = ah.getByteSize();
      *(Int64*)(ctx.TregMemBuffer + TdestRegister + 2) = read_len;
      ctx.TregMemBuffer[TdestRegister] = NOT_NULL_INDICATOR;
    }
    return INTERP_CONTINUE;
  }

  /* STR_TO_INT64 — parse a string in heap memory to 64-bit integer */
  static inline int handleStrToInt64(InterpreterContext& ctx) {
    Uint32 offsetType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 sizeReg = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 destValReg = Interpreter::getReg3(ctx.theInstruction) << 2;
    Uint32 sizeType = ctx.TregMemBuffer[sizeReg];
    Int64 memoryOffset = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Int64 size = *(Int64*)(ctx.TregMemBuffer + sizeReg + 2);
    if (unlikely((offsetType == NULL_INDICATOR) ||
                 (sizeType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(size > MAX_LONG_LONG_STRING)) {
      return -ZLONG_LONG_STRING_TOO_LONG;
    }
    if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - size) || (memoryOffset < 0))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    {
      char local_heap[MAX_LONG_LONG_STRING + 1];
      char* memory_start = &ctx.TheapMemoryChar[memoryOffset];
      memcpy(&local_heap[0], memory_start, size);
      char* memory_end = &local_heap[size];
      memory_end[0] = 0;
      char* end_ptr = nullptr;
      errno = 0;
      Int64 val = strtoll(&local_heap[0], &end_ptr, 10);
      if (unlikely(errno == EINVAL || errno == ERANGE ||
                   end_ptr != memory_end)) {
        return -ZINVALID_LONG_LONG_STRING;
      }
      *(Int64*)(ctx.TregMemBuffer + destValReg + 2) = val;
      ctx.TregMemBuffer[destValReg] = NOT_NULL_INDICATOR;
    }
    return INTERP_CONTINUE;
  }

  /* INT64_TO_STR — format a 64-bit integer as a string in heap memory */
  static inline int handleInt64ToStr(InterpreterContext& ctx) {
    Uint32 offsetType = ctx.TregMemBuffer[ctx.theRegister];
    Uint32 valueReg = Interpreter::getReg2(ctx.theInstruction) << 2;
    Uint32 destSizeReg = Interpreter::getReg3(ctx.theInstruction) << 2;
    Uint32 valueType = ctx.TregMemBuffer[valueReg];
    Int64 memOffset = *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Int64 value = *(Int64*)(ctx.TregMemBuffer + valueReg + 2);
    if (unlikely((offsetType == NULL_INDICATOR) ||
                 (valueType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (unlikely(memOffset > (MAX_HEAP_OFFSET - MAX_LONG_LONG_STRING) ||
                 (memOffset < 0))) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    int size = snprintf(&ctx.TheapMemoryChar[memOffset],
                        MAX_LONG_LONG_STRING, "%lld", value);
    if (size <= 0) {
      return -ZCONVERT_LONG_LONG_TO_STRING_ERROR;
    }
    *(Int64*)(ctx.TregMemBuffer + destSizeReg + 2) = size;
    ctx.TregMemBuffer[destSizeReg] = NOT_NULL_INDICATOR;
    return INTERP_CONTINUE;
  }

  /* BRANCH_ATTR_OP (ARG/PARAM/ATTR + OVERFLOW variants) — branch on an
   * attribute-vs-value/parameter/attribute comparison. Shared body for six
   * case labels that differ only in how the second operand is obtained. */
  static inline int handleBranchAttrOp(InterpreterContext& ctx) {
    const Uint32 ins2 = ctx.TcurrentProgram[ctx.TprogramCounter];
    Uint32 attrId = Interpreter::getBranchCol_AttrId(ins2) << 16;
    const Uint32 opCode =
        Interpreter::getOpCode(ctx.theInstruction) % OVERFLOW_OPCODE;

    if (ctx.tmpHabitant != attrId) {
      Int32 TnoDataR = ctx.tup->readAttributes(
          ctx.req_struct, &attrId, 1, ctx.tmpArea, ctx.tmpAreaSz);
      if (unlikely(TnoDataR < 0)) {
        thrjam(ctx.tup->jamBuffer());
        return TnoDataR;  /* already negative */
      }
      ctx.tmpHabitant = attrId;
    }

    // get type
    attrId >>= 16;
    const Uint32* attrDescriptor = ctx.req_struct->tablePtrP->tabDescriptor +
                                    (attrId * ZAD_SIZE);
    const Uint32 TattrDesc1 = attrDescriptor[0];
    const Uint32 TattrDesc2 = attrDescriptor[1];
    const Uint32 typeId = AttributeDescriptor::getType(TattrDesc1);
    const CHARSET_INFO* cs = nullptr;
    if (AttributeOffset::getCharsetFlag(TattrDesc2)) {
      const Uint32 pos = AttributeOffset::getCharsetPos(TattrDesc2);
      cs = ctx.req_struct->tablePtrP->charsetArray[pos];
    }
    const NdbSqlUtil::Type& sqlType = NdbSqlUtil::getType(typeId);

    // get data for 1st argument, always an ATTR.
    const AttributeHeader ah(ctx.tmpArea[0]);
    const char* s1 = (char*)&ctx.tmpArea[1];
    Uint32 attrLen = AttributeDescriptor::getSizeInBytes(TattrDesc1);
    if (unlikely(typeId == NDB_TYPE_BIT)) {
      Uint32 bitFieldAttrLen =
          (AttributeDescriptor::getArraySize(TattrDesc1) + 7) / 8;
      attrLen = bitFieldAttrLen;
    }

    // 2'nd argument, literal, parameter or another attribute
    Uint32 argLen = 0;
    Uint32 step = 0;
    const char* s2 = nullptr;

    if (likely(opCode == Interpreter::BRANCH_ATTR_OP_ARG)) {
      thrjamDebug(ctx.tup->jamBuffer());
      argLen = Interpreter::getBranchCol_Len(ins2);
      step = argLen;
      s2 = (char*)&ctx.TcurrentProgram[ctx.TprogramCounter + 1];
    } else if (opCode == Interpreter::BRANCH_ATTR_OP_PARAM) {
      thrjamDebug(ctx.tup->jamBuffer());
      assert(ctx.req_struct != nullptr);
      assert(ctx.req_struct->operPtrP != nullptr);
      const Uint32 paramNo = Interpreter::getBranchCol_ParamNo(ins2);
      const Uint32* paramPos = ctx.subroutineProg;
      const Uint32* paramptr =
          ctx.tup->lookupInterpreterParameter(paramNo, paramPos);
      if (unlikely(paramptr == nullptr)) {
        thrjam(ctx.tup->jamBuffer());
        return -99;  // TODO — unclear error code
      }
      argLen = AttributeHeader::getByteSize(*paramptr);
      step = 0;
      s2 = (char*)(paramptr + 1);
    } else if (opCode == Interpreter::BRANCH_ATTR_OP_ATTR) {
      thrjamDebug(ctx.tup->jamBuffer());
      Uint32 attr2Id = Interpreter::getBranchCol_AttrId2(ins2) << 16;

      // Attr2 to be read into tmpArea[] after Attr1.
      const Uint32 firstAttrWords = attrLen + 1;
      assert(ctx.tmpAreaSz >= 2 * firstAttrWords);
      Int32 TnoDataR = ctx.tup->readAttributes(
          ctx.req_struct, &attr2Id, 1, &ctx.tmpArea[firstAttrWords],
          ctx.tmpAreaSz - firstAttrWords);
      if (unlikely(TnoDataR < 0)) {
        thrjam(ctx.tup->jamBuffer());
        return TnoDataR;  /* already negative */
      }

      const AttributeHeader ah2(ctx.tmpArea[firstAttrWords]);
      if (!ah2.isNULL()) {
        attr2Id >>= 16;
        const Uint32* attr2Descriptor =
            ctx.req_struct->tablePtrP->tabDescriptor + (attr2Id * ZAD_SIZE);
        const Uint32 Tattr2Desc1 = attr2Descriptor[0];
        const Uint32 type2Id = AttributeDescriptor::getType(Tattr2Desc1);

        argLen = AttributeDescriptor::getSizeInBytes(Tattr2Desc1);
        if (unlikely(type2Id == NDB_TYPE_BIT)) {
          Uint32 bitFieldAttrLen =
              (AttributeDescriptor::getArraySize(Tattr2Desc1) + 7) / 8;
          argLen = bitFieldAttrLen;
        }
        s2 = (char*)&ctx.tmpArea[firstAttrWords + 1];
      }
      step = 0;
    }

    // Evaluate
    const bool r1_null = ah.isNULL();
    const bool r2_null = argLen == 0;
    if (r1_null || r2_null) {
      // There are NULL-valued operands, check the NullSemantics
      const Uint32 nullSemantics =
          Interpreter::getNullSemantics(ctx.theInstruction);
      if (nullSemantics == Interpreter::IF_NULL_BREAK_OUT) {
        ctx.TprogramCounter =
            ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
        return INTERP_CONTINUE;
      }
      if (nullSemantics == Interpreter::IF_NULL_CONTINUE) {
        const Uint32 tmp = ((step + 3) >> 2) + 1;
        ctx.TprogramCounter += tmp;
        return INTERP_CONTINUE;
      }
    }

    const Uint32 cond = Interpreter::getBinaryCondition(ctx.theInstruction);
    int res1;
    if (cond <= Interpreter::GE) {
      /* Inequality - EQ, NE, LT, LE, GT, GE */
      if (r1_null || r2_null) {
        res1 = r1_null && r2_null ? 0 : r1_null ? -1 : 1;
      } else {
        thrjamDebug(ctx.tup->jamBuffer());
        if (unlikely(sqlType.m_cmp == 0)) {
          return -40;
        }
        res1 = (*sqlType.m_cmp)(cs, s1, attrLen, s2, argLen);
      }
    } else {
      if ((cond == Interpreter::LIKE) ||
          (cond == Interpreter::NOT_LIKE)) {
        if (r1_null || r2_null) {
          res1 = r1_null && r2_null ? 0 : -1;
        } else {
          thrjam(ctx.tup->jamBuffer());
          if (unlikely(sqlType.m_like == 0)) {
            return -40;
          }
          res1 = (*sqlType.m_like)(cs, s1, attrLen, s2, argLen);
        }
      } else {
        /* AND_XX_MASK condition */
        assert(cond <= Interpreter::AND_NE_ZERO);
        if (unlikely(sqlType.m_mask == 0)) {
          return -40;
        }
        if (r1_null || r2_null) {
          res1 = 1;
        } else {
          bool cmpZero = (cond == Interpreter::AND_EQ_ZERO) ||
                         (cond == Interpreter::AND_NE_ZERO);
          res1 = (*sqlType.m_mask)(s1, attrLen, s2, argLen, cmpZero);
        }
      }
    }

    int res = 0;
    switch ((Interpreter::BinaryCondition)cond) {
      case Interpreter::EQ:       res = (res1 == 0); break;
      case Interpreter::NE:       res = (res1 != 0); break;
      case Interpreter::LT:       res = (res1 > 0); break;   // inverted
      case Interpreter::LE:       res = (res1 >= 0); break;
      case Interpreter::GT:       res = (res1 < 0); break;
      case Interpreter::GE:       res = (res1 <= 0); break;
      case Interpreter::LIKE:     res = (res1 == 0); break;
      case Interpreter::NOT_LIKE: res = (res1 == 1); break;
      case Interpreter::AND_EQ_MASK: res = (res1 == 0); break;
      case Interpreter::AND_NE_MASK: res = (res1 != 0); break;
      case Interpreter::AND_EQ_ZERO: res = (res1 == 0); break;
      case Interpreter::AND_NE_ZERO: res = (res1 != 0); break;
    }

    if (res) {
      ctx.TprogramCounter =
          ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
    } else {
      Uint32 tmp = ((step + 3) >> 2) + 1;
      ctx.TprogramCounter += tmp;
    }
    return INTERP_CONTINUE;
  }


  /* --- Batch 10 --- search interval, binary search, string/array ops (extraction complete) */

  /* SEARCH_INTERVAL_64 — binary search in sorted Uint64 array of interval ranges */
  static inline int handleSearchInterval64(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3; //A bit heavier instruction
    /**
     * This instruction does a binary search in a sorted array of
     * ranges. This means that each pair of numbers represents a
     * range. Thus input have an even number of elements in the
     * array.
     *
     * By using binary search with smaller or equal we get the result
     * that returning an even number means that the number is within
     * one of the ranges and odd numbers and NULL values means that
     * the value was not in a range.
     *
     * Input:
     *   Register 1:
     *     The number we are looking for
     *   Register 2:
     *     The offset in memory where sorted Uint64 array is stored
     *   Register 3:
     *     The number of elements in the array
     *   Enum 5:
     *     0: Left open and Right closed interval
     *     1: Left closed and Right open interval
     * Output:
     *   Register 4:
     *     The position of the found element
     *     NULL if no element found
     *
     */
    Int64 Tordinal = * (Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 TregOrdinalType = ctx.TregMemBuffer[ctx.theRegister];

    Uint32 ToffsetRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Int64 Toffset = * (Int64*)(ctx.TregMemBuffer + ToffsetRegister + 2);
    Uint32 TregOffsetType = ctx.TregMemBuffer[ToffsetRegister];

    Uint32 TnumElemsRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    Int64 TnumElems = * (Int64*)(ctx.TregMemBuffer + TnumElemsRegister + 2);
    Uint32 TregNumElemsType = ctx.TregMemBuffer[TnumElemsRegister];

    Uint32 TretElemsRegister = Interpreter::getReg4(ctx.theInstruction) << 2;
    Int64 end_pos = Toffset + (8 * TnumElems);
    Uint32 TleftOpen = Interpreter::enum5(ctx.theInstruction);

    if (unlikely((TregOffsetType == NULL_INDICATOR) ||
                 (TregOrdinalType == NULL_INDICATOR) ||
                 (TregNumElemsType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (Toffset < 0 || TnumElems < 0 || end_pos > MAX_HEAP_OFFSET) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    if (Tordinal < 0) {
      return -ZWRONG_INPUT_TO_BINARY_SEARCH;
    }
    if (TleftOpen > 1) {
      return -ZNO_SUCH_SEARCH_INTERVAL_METHOD;
    }
    Uint32 ret;
    Uint64 ordinal = Uint64(Tordinal);
    if (TleftOpen == 0) {
      ret = binary_uint64_search_smaller(ordinal,
                                         &ctx.TheapMemoryChar[Toffset],
                                         TnumElems,
                                         true,
                                         true);
      if (ret == RET_NULL || ((ret & 1) == 1)) {
        ctx.TregMemBuffer[TretElemsRegister] = NULL_INDICATOR;
      } else {
        ctx.TregMemBuffer[TretElemsRegister] = NOT_NULL_INDICATOR;
        *(Int64*)(ctx.TregMemBuffer + TretElemsRegister + 2) = ret;
      }
    } else {
      ret = binary_uint64_search_larger(ordinal,
                                        &ctx.TheapMemoryChar[Toffset],
                                        TnumElems,
                                        true,
                                        true);
      if ((ret & 1) == 0) {
        ctx.TregMemBuffer[TretElemsRegister] = NULL_INDICATOR;
      } else {
        ctx.TregMemBuffer[TretElemsRegister] = NOT_NULL_INDICATOR;
        *(Int64*)(ctx.TregMemBuffer + TretElemsRegister + 2) = ret - 1;
      }
    }
    return INTERP_CONTINUE;
  }

  /* SEARCH_INTERVAL_32 — binary search in sorted Uint32 array of interval ranges */
  static inline int handleSearchInterval32(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3; //A bit heavier instruction
    /* This is the 32-bit version of SEARCH_INTERVAL_64 */
    Int64 Tordinal = * (Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 TregOrdinalType = ctx.TregMemBuffer[ctx.theRegister];

    Uint32 ToffsetRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Int64 Toffset = * (Int64*)(ctx.TregMemBuffer + ToffsetRegister + 2);
    Uint32 TregOffsetType = ctx.TregMemBuffer[ToffsetRegister];

    Uint32 TnumElemsRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    Int64 TnumElems = * (Int64*)(ctx.TregMemBuffer + TnumElemsRegister + 2);
    Uint32 TregNumElemsType = ctx.TregMemBuffer[TnumElemsRegister];

    Uint32 TretElemsRegister = Interpreter::getReg4(ctx.theInstruction) << 2;
    Int64 end_pos = Toffset + (4 * TnumElems);
    Uint32 TleftOpen = Interpreter::enum5(ctx.theInstruction);

    if (unlikely((TregOffsetType == NULL_INDICATOR) ||
                 (TregOrdinalType == NULL_INDICATOR) ||
                 (TregNumElemsType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (Toffset < 0 || TnumElems < 0 || end_pos > MAX_HEAP_OFFSET) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    if (Tordinal < 0 ||
        Tordinal > Int64(std::numeric_limits<Uint32>::max())) {
      return -ZWRONG_INPUT_TO_BINARY_SEARCH;
    }
    if (TleftOpen > 1) {
      return -ZNO_SUCH_SEARCH_INTERVAL_METHOD;
    }
    Uint32 ret;
    Uint32 ordinal = Uint32(Tordinal);
    if (TleftOpen == 0) {
      ret = binary_uint32_search_smaller(ordinal,
                                         &ctx.TheapMemoryChar[Toffset],
                                         TnumElems,
                                         true,
                                         true);
      if (ret == RET_NULL || ((ret & 1) == 1)) {
        ctx.TregMemBuffer[TretElemsRegister] = NULL_INDICATOR;
      } else {
        ctx.TregMemBuffer[TretElemsRegister] = NOT_NULL_INDICATOR;
        *(Int64*)(ctx.TregMemBuffer + TretElemsRegister + 2) = ret;
      }
    } else {
      ret = binary_uint32_search_larger(ordinal,
                                        &ctx.TheapMemoryChar[Toffset],
                                        TnumElems,
                                        true,
                                        true);
      if ((ret & 1) == 0) {
        ctx.TregMemBuffer[TretElemsRegister] = NULL_INDICATOR;
      } else {
        ctx.TregMemBuffer[TretElemsRegister] = NOT_NULL_INDICATOR;
        *(Int64*)(ctx.TregMemBuffer + TretElemsRegister + 2) = ret - 1;
      }
    }
    return INTERP_CONTINUE;
  }

  /* SEARCH_INTERVAL_16 — binary search in sorted Uint16 array of interval ranges */
  static inline int handleSearchInterval16(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3; //A bit heavier instruction
    /* This is the 16-bit version of SEARCH_INTERVAL_64 */
    Int64 Tordinal = * (Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 TregOrdinalType = ctx.TregMemBuffer[ctx.theRegister];

    Uint32 ToffsetRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Int64 Toffset = * (Int64*)(ctx.TregMemBuffer + ToffsetRegister + 2);
    Uint32 TregOffsetType = ctx.TregMemBuffer[ToffsetRegister];

    Uint32 TnumElemsRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    Int64 TnumElems = * (Int64*)(ctx.TregMemBuffer + TnumElemsRegister + 2);
    Uint32 TregNumElemsType = ctx.TregMemBuffer[TnumElemsRegister];

    Uint32 TretElemsRegister = Interpreter::getReg4(ctx.theInstruction) << 2;
    Int64 end_pos = Toffset + (2 * TnumElems);
    Uint32 TleftOpen = Interpreter::enum5(ctx.theInstruction);

    if (unlikely((TregOffsetType == NULL_INDICATOR) ||
                 (TregOrdinalType == NULL_INDICATOR) ||
                 (TregNumElemsType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (Toffset < 0 || TnumElems < 0 || end_pos > MAX_HEAP_OFFSET) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    if (Tordinal < 0 ||
        Tordinal > Int64(std::numeric_limits<Uint16>::max())) {
      return -ZWRONG_INPUT_TO_BINARY_SEARCH;
    }
    if (TleftOpen > 1) {
      return -ZNO_SUCH_SEARCH_INTERVAL_METHOD;
    }
    Uint32 ret;
    Uint16 ordinal = Uint16(Tordinal);
    if (TleftOpen == 0) {
      ret = binary_uint16_search_smaller(ordinal,
                                         &ctx.TheapMemoryChar[Toffset],
                                         TnumElems,
                                         true,
                                         true);
      if (ret == RET_NULL || ((ret & 1) == 1)) {
        ctx.TregMemBuffer[TretElemsRegister] = NULL_INDICATOR;
      } else {
        ctx.TregMemBuffer[TretElemsRegister] = NOT_NULL_INDICATOR;
        *(Int64*)(ctx.TregMemBuffer + TretElemsRegister + 2) = ret;
      }
    } else {
      ret = binary_uint16_search_larger(ordinal,
                                        &ctx.TheapMemoryChar[Toffset],
                                        TnumElems,
                                        true,
                                        true);
      if ((ret & 1) == 0) {
        ctx.TregMemBuffer[TretElemsRegister] = NULL_INDICATOR;
      } else {
        ctx.TregMemBuffer[TretElemsRegister] = NOT_NULL_INDICATOR;
        *(Int64*)(ctx.TregMemBuffer + TretElemsRegister + 2) = ret - 1;
      }
    }
    return INTERP_CONTINUE;
  }

  /* SEARCH_INTERVAL_ODD — binary search in sorted array of arbitrary-size interval ranges */
  static inline int handleSearchIntervalOdd(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3; //A bit heavier instruction
    /* This is the odd number version of SEARCH_INTERVAL_64 */
    Int64 Tordinal = * (Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 TregOrdinalType = ctx.TregMemBuffer[ctx.theRegister];

    Uint32 ToffsetRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Int64 Toffset = * (Int64*)(ctx.TregMemBuffer + ToffsetRegister + 2);
    Uint32 TregOffsetType = ctx.TregMemBuffer[ToffsetRegister];

    Uint32 TnumElemsRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    Int64 TnumElems = * (Int64*)(ctx.TregMemBuffer + TnumElemsRegister + 2);
    Uint32 TregNumElemsType = ctx.TregMemBuffer[TnumElemsRegister];

    Uint32 TretElemsRegister = Interpreter::getReg4(ctx.theInstruction) << 2;
    Uint32 TleftOpen = Interpreter::enum5(ctx.theInstruction);
    Uint32 TnumberSize = Interpreter::enum6(ctx.theInstruction);
    Int64 end_pos = Toffset + (TnumberSize * TnumElems);

    if (unlikely((TregOffsetType == NULL_INDICATOR) ||
                 (TregOrdinalType == NULL_INDICATOR) ||
                 (TregNumElemsType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (Toffset < 0 || TnumElems < 0 || end_pos > MAX_HEAP_OFFSET) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    if (TnumberSize != 1 &&
        TnumberSize != 3 &&
        TnumberSize != 5 &&
        TnumberSize != 6) {
      return -ZNO_SUCH_NUMBER_SIZE_SUPPORTED;
    }
    Uint64 max_number = (Uint64(1) << (Uint64(TnumberSize * 8))) - 1;
    if (Tordinal < 0 ||
        Tordinal > Int64(max_number)) {
      return -ZWRONG_INPUT_TO_BINARY_SEARCH;
    }
    if (TleftOpen > 1) {
      return -ZNO_SUCH_SEARCH_INTERVAL_METHOD;
    }
    Uint32 ret;
    Uint64 ordinal = Uint64(Tordinal);
    if (TleftOpen == 0) {
      ret = binary_odd_search_smaller(ordinal,
                                      &ctx.TheapMemoryChar[Toffset],
                                      TnumElems,
                                      TnumberSize,
                                      true,
                                      true);
      if (ret == RET_NULL || ((ret & 1) == 1)) {
        ctx.TregMemBuffer[TretElemsRegister] = NULL_INDICATOR;
      } else {
        ctx.TregMemBuffer[TretElemsRegister] = NOT_NULL_INDICATOR;
        *(Int64*)(ctx.TregMemBuffer + TretElemsRegister + 2) = ret;
      }
    } else {
      ret = binary_odd_search_larger(ordinal,
                                     &ctx.TheapMemoryChar[Toffset],
                                     TnumElems,
                                     TnumberSize,
                                     true,
                                     true);
      if ((ret & 1) == 0) {
        ctx.TregMemBuffer[TretElemsRegister] = NULL_INDICATOR;
      } else {
        ctx.TregMemBuffer[TretElemsRegister] = NOT_NULL_INDICATOR;
        *(Int64*)(ctx.TregMemBuffer + TretElemsRegister + 2) = ret - 1;
      }
    }
    return INTERP_CONTINUE;
  }

  /* BINARY_SEARCH_64 — binary search in sorted Uint64 array */
  static inline int handleBinarySearch64(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3; //A bit heavier instruction
    /**
     * Input:
     *   Register 1:
     *     The number we are looking for
     *   Register 2:
     *     The offset in memory where sorted Uint64 array is stored
     *   Register 3:
     *     The number of elements in the array
     *   Enum 5:
     *     0 means exact match only
     *     1 means search for nearest that is smaller
     *       Another name for this is that it is a rank query.
     *       This query will never return NULL.
     *     2 means search for nearest that is larger or equal
     *       This query finds the successor element.
     *       This query will never return NULL.
     *     3 means search for nearest that is smaller or equal
     *     4 means search for nearest that is larger or equal
     * Output:
     *   Register 4:
     *     The position of the found element
     *     NULL if no element found
     *
     */
    Int64 Tordinal = * (Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 TregOrdinalType = ctx.TregMemBuffer[ctx.theRegister];

    Uint32 ToffsetRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Int64 Toffset = * (Int64*)(ctx.TregMemBuffer + ToffsetRegister + 2);
    Uint32 TregOffsetType = ctx.TregMemBuffer[ToffsetRegister];

    Uint32 TnumElemsRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    Int64 TnumElems = * (Int64*)(ctx.TregMemBuffer + TnumElemsRegister + 2);
    Uint32 TregNumElemsType = ctx.TregMemBuffer[TnumElemsRegister];

    Uint32 TretElemsRegister = Interpreter::getReg4(ctx.theInstruction) << 2;
    Uint32 TexactMatch = Interpreter::enum5(ctx.theInstruction);
    Int64 end_pos = Toffset + (8 * TnumElems);

    if (unlikely((TregOffsetType == NULL_INDICATOR) ||
                 (TregOrdinalType == NULL_INDICATOR) ||
                 (TregNumElemsType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (Toffset < 0 || TnumElems < 0 || end_pos > MAX_HEAP_OFFSET) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    if (Tordinal < 0) {
      return -ZWRONG_INPUT_TO_BINARY_SEARCH;
    }
    Uint32 ret;
    Uint64 ordinal = Uint64(Tordinal);
    switch (TexactMatch) {
    case EQUAL_MATCH: {
      ret = binary_uint64_search_exact(ordinal,
                                       &ctx.TheapMemoryChar[Toffset],
                                       TnumElems);
      break;
    }
    case SMALLER_MATCH: {
      ret = binary_uint64_search_smaller(ordinal,
                                         &ctx.TheapMemoryChar[Toffset],
                                         TnumElems,
                                         false,
                                         false);
      break;
    }
    case LARGER_MATCH: {
      ret = binary_uint64_search_larger(ordinal,
                                        &ctx.TheapMemoryChar[Toffset],
                                        TnumElems,
                                        false,
                                        false);
      break;
    }
    case SMALLER_EQUAL_MATCH: {
      ret = binary_uint64_search_smaller(ordinal,
                                         &ctx.TheapMemoryChar[Toffset],
                                         TnumElems,
                                         true,
                                         false);
      break;
    }
    case LARGER_EQUAL_MATCH: {
      ret = binary_uint64_search_larger(ordinal,
                                        &ctx.TheapMemoryChar[Toffset],
                                        TnumElems,
                                        true,
                                        false);
      break;
    }
    default: {
      return -ZNO_SUCH_BINARY_SEARCH_METHOD;
    }
    }
    if (ret == RET_NULL) {
      ctx.TregMemBuffer[TretElemsRegister] = NULL_INDICATOR;
    } else {
      ctx.TregMemBuffer[TretElemsRegister] = NOT_NULL_INDICATOR;
      *(Int64*)(ctx.TregMemBuffer + TretElemsRegister + 2) = ret;
    }
    return INTERP_CONTINUE;
  }

  /* BINARY_SEARCH_32 — binary search in sorted Uint32 array */
  static inline int handleBinarySearch32(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3; //A bit heavier instruction
    /* See BINARY_SEARCH_64, this is the 32-bit version */
    Int64 Tordinal = * (Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 TregOrdinalType = ctx.TregMemBuffer[ctx.theRegister];

    Uint32 ToffsetRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Int64 Toffset = * (Int64*)(ctx.TregMemBuffer + ToffsetRegister + 2);
    Uint32 TregOffsetType = ctx.TregMemBuffer[ToffsetRegister];

    Uint32 TnumElemsRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    Int64 TnumElems = * (Int64*)(ctx.TregMemBuffer + TnumElemsRegister + 2);
    Uint32 TregNumElemsType = ctx.TregMemBuffer[TnumElemsRegister];

    Uint32 TretElemsRegister = Interpreter::getReg4(ctx.theInstruction) << 2;
    Uint32 TexactMatch = Interpreter::enum5(ctx.theInstruction);
    Int64 end_pos = Toffset + (4 * TnumElems);

    if (unlikely((TregOffsetType == NULL_INDICATOR) ||
                 (TregOrdinalType == NULL_INDICATOR) ||
                 (TregNumElemsType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (Toffset < 0 || TnumElems < 0 || end_pos > MAX_HEAP_OFFSET) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    if (TexactMatch > LARGER_EQUAL_MATCH) {
      return -ZNO_SUCH_BINARY_SEARCH_METHOD;
    }
    if (Tordinal < 0 ||
        Tordinal > Int64(std::numeric_limits<Uint32>::max())) {
      return -ZWRONG_INPUT_TO_BINARY_SEARCH;
    }
    Uint32 ordinal = Uint32(Tordinal);
    Uint32 ret;
    switch (TexactMatch) {
    case EQUAL_MATCH: {
      ret = binary_uint32_search_exact(ordinal,
                                       &ctx.TheapMemoryChar[Toffset],
                                       TnumElems);
      break;
    }
    case SMALLER_MATCH: {
      ret = binary_uint32_search_smaller(ordinal,
                                         &ctx.TheapMemoryChar[Toffset],
                                         TnumElems,
                                         false,
                                         false);
      break;
    }
    case LARGER_MATCH: {
      ret = binary_uint32_search_larger(ordinal,
                                        &ctx.TheapMemoryChar[Toffset],
                                        TnumElems,
                                        false,
                                        false);
      break;
    }
    case SMALLER_EQUAL_MATCH: {
      ret = binary_uint32_search_smaller(ordinal,
                                         &ctx.TheapMemoryChar[Toffset],
                                         TnumElems,
                                         true,
                                         false);
      break;
    }
    case LARGER_EQUAL_MATCH: {
      ret = binary_uint32_search_larger(ordinal,
                                        &ctx.TheapMemoryChar[Toffset],
                                        TnumElems,
                                        true,
                                        false);
      break;
    }
    default: {
      return -ZNO_SUCH_BINARY_SEARCH_METHOD;
    }
    }
    if (ret == RET_NULL) {
      ctx.TregMemBuffer[TretElemsRegister] = NULL_INDICATOR;
    } else {
      ctx.TregMemBuffer[TretElemsRegister] = NOT_NULL_INDICATOR;
      *(Int64*)(ctx.TregMemBuffer + TretElemsRegister + 2) = ret;
    }
    return INTERP_CONTINUE;
  }

  /* BINARY_SEARCH_16 — binary search in sorted Uint16 array */
  static inline int handleBinarySearch16(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3; //A bit heavier instruction
    /* See BINARY_SEARCH_64, this is the 16-bit version */
    Int64 Tordinal = * (Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 TregOrdinalType = ctx.TregMemBuffer[ctx.theRegister];

    Uint32 ToffsetRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Int64 Toffset = * (Int64*)(ctx.TregMemBuffer + ToffsetRegister + 2);
    Uint32 TregOffsetType = ctx.TregMemBuffer[ToffsetRegister];

    Uint32 TnumElemsRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    Int64 TnumElems = * (Int64*)(ctx.TregMemBuffer + TnumElemsRegister + 2);
    Uint32 TregNumElemsType = ctx.TregMemBuffer[TnumElemsRegister];

    Uint32 TretElemsRegister = Interpreter::getReg4(ctx.theInstruction) << 2;
    Uint32 TexactMatch = Interpreter::enum5(ctx.theInstruction);
    Int64 end_pos = Toffset + (2 * TnumElems);

    if (unlikely((TregOffsetType == NULL_INDICATOR) ||
                 (TregOrdinalType == NULL_INDICATOR) ||
                 (TregNumElemsType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (Toffset < 0 || TnumElems < 0 || end_pos > MAX_HEAP_OFFSET) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    if (Tordinal < 0 ||
        Tordinal > Int64(std::numeric_limits<Uint16>::max())) {
      return -ZWRONG_INPUT_TO_BINARY_SEARCH;
    }
    Uint16 ordinal = Uint16(Tordinal);
    Uint32 ret;
    switch (TexactMatch) {
    case EQUAL_MATCH: {
      ret = binary_uint16_search_exact(ordinal,
                                       &ctx.TheapMemoryChar[Toffset],
                                       TnumElems);
      break;
    }
    case SMALLER_MATCH: {
      ret = binary_uint16_search_smaller(ordinal,
                                         &ctx.TheapMemoryChar[Toffset],
                                         TnumElems,
                                         false,
                                         false);
      break;
    }
    case LARGER_MATCH: {
      ret = binary_uint16_search_larger(ordinal,
                                        &ctx.TheapMemoryChar[Toffset],
                                        TnumElems,
                                        false,
                                        false);
      break;
    }
    case SMALLER_EQUAL_MATCH: {
      ret = binary_uint16_search_smaller(ordinal,
                                         &ctx.TheapMemoryChar[Toffset],
                                         TnumElems,
                                         true,
                                         false);
      break;
    }
    case LARGER_EQUAL_MATCH: {
      ret = binary_uint16_search_larger(ordinal,
                                        &ctx.TheapMemoryChar[Toffset],
                                        TnumElems,
                                        true,
                                        false);
      break;
    }
    default: {
      return -ZNO_SUCH_BINARY_SEARCH_METHOD;
    }
    }
    if (ret == RET_NULL) {
      ctx.TregMemBuffer[TretElemsRegister] = NULL_INDICATOR;
    } else {
      ctx.TregMemBuffer[TretElemsRegister] = NOT_NULL_INDICATOR;
      *(Int64*)(ctx.TregMemBuffer + TretElemsRegister + 2) = ret;
    }
    return INTERP_CONTINUE;
  }

  /* BINARY_SEARCH_ODD — binary search in sorted array of arbitrary-size elements */
  static inline int handleBinarySearchOdd(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3; //A bit heavier instruction
    /* See BINARY_SEARCH_64, this is the odd number version version */
    Int64 Tordinal = * (Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 TregOrdinalType = ctx.TregMemBuffer[ctx.theRegister];

    Uint32 ToffsetRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Int64 Toffset = * (Int64*)(ctx.TregMemBuffer + ToffsetRegister + 2);
    Uint32 TregOffsetType = ctx.TregMemBuffer[ToffsetRegister];

    Uint32 TnumElemsRegister = Interpreter::getReg3(ctx.theInstruction) << 2;
    Int64 TnumElems = * (Int64*)(ctx.TregMemBuffer + TnumElemsRegister + 2);
    Uint32 TregNumElemsType = ctx.TregMemBuffer[TnumElemsRegister];

    Uint32 TretElemsRegister = Interpreter::getReg4(ctx.theInstruction) << 2;
    Uint32 TexactMatch = Interpreter::enum5(ctx.theInstruction);
    Uint32 TnumberSize = Interpreter::enum6(ctx.theInstruction);
    Int64 end_pos = Toffset + (TnumberSize * TnumElems);

    if (unlikely((TregOffsetType == NULL_INDICATOR) ||
                 (TregOrdinalType == NULL_INDICATOR) ||
                 (TregNumElemsType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (Toffset < 0 || TnumElems < 0 || end_pos > MAX_HEAP_OFFSET) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    if (TnumberSize != 1 &&
        TnumberSize != 3 &&
        TnumberSize != 5 &&
        TnumberSize != 6) {
      return -ZNO_SUCH_NUMBER_SIZE_SUPPORTED;
    }
    Uint64 max_number = (Uint64(1) << (Uint64(TnumberSize * 8))) - 1;
    if (Tordinal < 0 ||
        Tordinal > Int64(max_number)) {
      return -ZWRONG_INPUT_TO_BINARY_SEARCH;
    }
    Uint64 ordinal = Uint64(Tordinal);
    Uint32 ret;
#ifdef TRACE_INTERPRETER
    g_eventLogger->info("ctx.theInstruction: %x, TexactMatch: %u, "
                        "Tordinal: %llu, Toffset: %lld, TnumElems: %lld, "
                        "TnumberSize: %u",
     ctx.theInstruction,
     TexactMatch,
     Tordinal,
     Toffset,
     TnumElems,
     TnumberSize);
#endif
    switch (TexactMatch) {
    case EQUAL_MATCH: {
      ret = binary_odd_search_exact(ordinal,
                                    &ctx.TheapMemoryChar[Toffset],
                                    TnumElems,
                                    TnumberSize);
      break;
    }
    case SMALLER_MATCH: {
      ret = binary_odd_search_smaller(ordinal,
                                      &ctx.TheapMemoryChar[Toffset],
                                      TnumElems,
                                      TnumberSize,
                                      false,
                                      false);
      break;
    }
    case LARGER_MATCH: {
      ret = binary_odd_search_larger(ordinal,
                                     &ctx.TheapMemoryChar[Toffset],
                                     TnumElems,
                                     TnumberSize,
                                     false,
                                     false);
      break;
    }
    case SMALLER_EQUAL_MATCH: {
      ret = binary_odd_search_smaller(ordinal,
                                      &ctx.TheapMemoryChar[Toffset],
                                      TnumElems,
                                      TnumberSize,
                                      true,
                                      false);
      break;
    }
    case LARGER_EQUAL_MATCH: {
      ret = binary_odd_search_larger(ordinal,
                                     &ctx.TheapMemoryChar[Toffset],
                                     TnumElems,
                                     TnumberSize,
                                     true,
                                     false);
      break;
    }
    default: {
      return -ZNO_SUCH_BINARY_SEARCH_METHOD;
    }
    }
    if (ret == RET_NULL) {
      ctx.TregMemBuffer[TretElemsRegister] = NULL_INDICATOR;
    } else {
      ctx.TregMemBuffer[TretElemsRegister] = NOT_NULL_INDICATOR;
      *(Int64*)(ctx.TregMemBuffer + TretElemsRegister + 2) = ret;
    }
    return INTERP_CONTINUE;
  }

  /* STRING_SEARCH — search for a substring in heap memory */
  static inline int handleStringSearch(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3; //A bit heavier instruction
    Int64 ToffsetString = * (Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 TregoffsetStringType = ctx.TregMemBuffer[ctx.theRegister];

    Uint32 TstringLenRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Int64 TstringLen = *(Int64*)(ctx.TregMemBuffer + TstringLenRegister + 2);
    Uint32 TregstringLenType = ctx.TregMemBuffer[TstringLenRegister];

    Uint32 ToffsetSearchRegister =
      Interpreter::getReg3(ctx.theInstruction) << 2;
    Int64 ToffsetSearch =
      *(Int64*)(ctx.TregMemBuffer + ToffsetSearchRegister + 2);
    Uint32 ToffsetSearchType = ctx.TregMemBuffer[ToffsetSearchRegister];

    Uint32 TsearchLenRegister = Interpreter::getReg4(ctx.theInstruction) << 2;
    Int64 TsearchLen = *(Int64*)(ctx.TregMemBuffer + TsearchLenRegister + 2);
    Uint32 TregsearchLenType = ctx.TregMemBuffer[TsearchLenRegister];

    Uint32 TretRegister = Interpreter::getReg5(ctx.theInstruction) << 2;

    if (unlikely((TregoffsetStringType == NULL_INDICATOR) ||
                 (TregstringLenType == NULL_INDICATOR) ||
                 (ToffsetSearchType == NULL_INDICATOR) ||
                 (TregsearchLenType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (ToffsetString < 0 ||
        ToffsetSearch < 0 ||
        TsearchLen < 0 ||
        TstringLen < 0 ||
        (ToffsetString + TstringLen) > MAX_HEAP_OFFSET ||
        (ToffsetSearch + TsearchLen) > MAX_HEAP_OFFSET) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    Uint32 ret;
    ret = string_search(&ctx.TheapMemoryChar[ToffsetSearch],
                        TsearchLen,
                        &ctx.TheapMemoryChar[ToffsetString],
                        TstringLen);
    if (ret == RET_NULL) {
      ctx.TregMemBuffer[TretRegister] = NULL_INDICATOR;
    } else {
      ctx.TregMemBuffer[TretRegister] = NOT_NULL_INDICATOR;
      *(Int64*)(ctx.TregMemBuffer + TretRegister + 2) = ret;
    }
    return INTERP_CONTINUE;
  }

  /* QSORT — quicksort a range in heap memory */
  static inline int handleQsort(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3; //A bit heavier instruction
    /**
     * This instruction sorts an array of unsigned integers of a
     * given size. The size can be 1,2,4,5,6 and 8 bytes.
     *
     * Input:
     *  Reg1: Offset of memory to be sorted
     *  Reg2: Number of elements in array to be sorted
     *  Enum5: Number size in bytes
     */
    Int64 Toffset = * (Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 TregoffsetType = ctx.TregMemBuffer[ctx.theRegister];

    Uint32 TnumElemsRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Int64 TnumElems = *(Int64*)(ctx.TregMemBuffer + TnumElemsRegister + 2);
    Uint32 TregnumElemsType = ctx.TregMemBuffer[TnumElemsRegister];

    Uint32 TnumberSize = Interpreter::enum5(ctx.theInstruction);

    if (unlikely((TregoffsetType == NULL_INDICATOR) ||
                 (TregnumElemsType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (Toffset < 0 ||
        TnumElems < 0 ||
        (Toffset + (TnumberSize * TnumElems)) > MAX_HEAP_OFFSET) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    if (TnumberSize != 1 &&
        TnumberSize != 2 &&
        TnumberSize != 3 &&
        TnumberSize != 4 &&
        TnumberSize != 5 &&
        TnumberSize != 6 &&
        TnumberSize != 8) {
      return -ZNO_SUCH_NUMBER_SIZE_SUPPORTED;
    }
    qsort_instr(&ctx.TheapMemoryChar[Toffset],
                TnumElems,
                TnumberSize);
    return INTERP_CONTINUE;
  }

  /* COMPRESS_NUM_ARRAY — remove duplicates from a sorted numeric array */
  static inline int handleCompressNumArray(InterpreterContext& ctx) {
    ctx.RnoOfInstructions += 3; //A bit heavier instruction
    /**
     * This instruction takes as input an array of Uint32 or Uint64
     * and converts it into a smaller array of 3, 5 or 6 bytes stored
     * in little-endian format. This can be used in combination with
     * binary search of odd sizes and similarly for search intervals.
     *
     * Input:
     *  Reg1: Offset of memory to be sorted
     *  Reg2: Number of elements in array to be sorted
     *  Enum5: Number size in bytes of input data (4 or 8)
     *  Enum6: Number size in bytes of output data (3, 5 or 6)
     */
    Int64 Toffset = * (Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2);
    Uint32 TregoffsetType = ctx.TregMemBuffer[ctx.theRegister];

    Uint32 TnumElemsRegister = Interpreter::getReg2(ctx.theInstruction) << 2;
    Int64 TnumElems = *(Int64*)(ctx.TregMemBuffer + TnumElemsRegister + 2);
    Uint32 TregnumElemsType = ctx.TregMemBuffer[TnumElemsRegister];

    Uint32 TnumberSizeIn = Interpreter::enum5(ctx.theInstruction);
    Uint32 TnumberSizeOut = Interpreter::enum6(ctx.theInstruction);

#ifdef TRACE_INTERPRETER
    g_eventLogger->info("Toffset: %lld, TnumElems: %lld, "
                        "TnumberSizeIn: %u, TnumberSizeOut: %u, "
                        "ctx.theInstruction: %x",
      Toffset,
      TnumElems,
      TnumberSizeIn,
      TnumberSizeOut,
      ctx.theInstruction);
#endif
    if (unlikely((TregoffsetType == NULL_INDICATOR) ||
                 (TregnumElemsType == NULL_INDICATOR))) {
      return -ZREGISTER_INIT_ERROR;
    }
    if (Toffset < 0 ||
        TnumElems < 0 ||
        (Toffset + (TnumberSizeIn * TnumElems)) > MAX_HEAP_OFFSET) {
      return -ZMEMORY_OFFSET_ERROR;
    }
    if (!((TnumberSizeIn == 4 && TnumberSizeOut == 3) ||
          (TnumberSizeIn == 8 &&
           (TnumberSizeOut == 5 || TnumberSizeOut == 6)))) {
      return -ZNO_SUCH_NUMBER_SIZE_SUPPORTED;
    }
    if (TnumberSizeIn == 4) {
      compress_num32_array(&ctx.TheapMemoryChar[Toffset],
                           TnumElems,
                           TnumberSizeOut);
    } else {
      compress_num64_array(&ctx.TheapMemoryChar[Toffset],
                           TnumElems,
                           TnumberSizeOut);
    }
    return INTERP_CONTINUE;
  }

  /* SPECIAL_INSTR — extension opcode dispatched on secondary opcode */
  static inline int handleSpecialInstr(InterpreterContext& ctx) {
    Uint32 extended_instruction = ctx.theInstruction >> 16;
    switch (extended_instruction)
    {
      default:
      {
#ifdef TRACE_INTERPRETER
        g_eventLogger->info("(%u) Extended Instruction %u doesn't exist",
                            instance(),
                            extended_instruction);
#endif
	      return -ZNO_INSTRUCTION_ERROR;
      }
    }
    return INTERP_CONTINUE;
  }

  /* ============================================================
   * CTE filter mode handlers — override specific behaviour
   * for interpreterFilterCte.
   * ============================================================ */

  /* EXIT_REFUSE override: return filter-reject sentinel rather than
   * calling tupkeyErrorLab (which dereferences operPtrP — CTE rows
   * have no Operationrec). */
  static inline int handleExitRefuseCte(InterpreterContext& /* ctx */) {
    return Dbtup::INTERPRETER_FILTER_REJECT;
  }

  /* Unsupported instruction in CTE filter mode: the instruction
   * depends on real-tuple state (operPtrP / tablePtrP / readAttributes).
   * Return a clean error without touching tuple state. */
  static inline int handleUnsupportedCte(InterpreterContext& ctx) {
    ctx.tup->terrorCode = ZNO_INSTRUCTION_ERROR;
    return -1;
  }

  /* Default handler slot for opcodes that haven't been extracted yet
   * (or aren't wired up). Should never be reached from the main
   * interpreter's switch path — it's only used as the default fill
   * for the dispatch tables. */
  static inline int handleTableNotPopulated(InterpreterContext& ctx) {
    ctx.tup->terrorCode = ZNO_INSTRUCTION_ERROR;
    return -1;
  }
};

/* Handler signature — declared in Dbtup.hpp as Dbtup::InterpreterHandler
 * so method signatures on Dbtup can name it; re-alias file-scope here
 * for the local dispatch tables and INTERP_DISPATCH macro. */
using InterpreterHandler = Dbtup::InterpreterHandler;

/* ------------------------------------------------------------------
 * s_cte_filter_handlers — dispatch table for interpreterFilterCte.
 *
 * Indexed by opcode [0..INTERP_HANDLER_TABLE_SIZE).  A nullptr slot
 * means the opcode is not accepted in CTE filter mode; the dispatch
 * loop detects it and raises ZNO_INSTRUCTION_ERROR.  EXIT_REFUSE is
 * redirected to handleExitRefuseCte so a user-visible filter reject
 * returns INTERPRETER_FILTER_REJECT rather than aborting the tuple
 * operation (CTE rows have no operPtrP to abort against).
 *
 * Opcode values come from Interpreter.hpp:60-248.  The low range
 * (0..63) is the primary opcode; the high range (64..127) is the
 * _CONST / overflow variant (opcode + OVERFLOW_OPCODE=64).  We must
 * list all 128 slots positionally because array designated
 * initialisers are a C99 extension outside the C++20 standard; GCC
 * 12 accepts them only with a warning flag.
 *
 * Accepted ops: constant loads, register arithmetic/bitwise, all
 * register branches, heap R/W on the register-indirect mem buffers,
 * EXIT_OK / EXIT_OK_LAST / CALL / RETURN, and the two CTE-critical
 * opcodes BRANCH_MEM_OP_ARG / READ_LINKED_TO_MEM that compare and
 * load linked (virtual-column) data from m_linked_attr_data.
 *
 * Rejected ops: everything that reads or writes a real tuple
 * attribute (READ_ATTR_INTO_REG, WRITE_ATTR_FROM_REG / _FROM_MEM,
 * APPEND, WRITE_PARTIAL, READ_ATTR_TO_MEM, the BRANCH_ATTR_* family,
 * LOAD_OP_TYPE, READ/WRITE_INTERPRETER_INPUT/OUTPUT); all the
 * BINARY_SEARCH / SEARCH_INTERVAL / STRING_SEARCH / QSORT /
 * COMPRESS_NUM_ARRAY / SPECIAL_INSTR instructions.
 * ------------------------------------------------------------------ */
static const InterpreterHandler
s_cte_filter_handlers[INTERP_HANDLER_TABLE_SIZE] = {
  /*   0  (unused)                */ nullptr,
  /*   1  READ_ATTR_INTO_REG      */ nullptr,
  /*   2  WRITE_ATTR_FROM_REG     */ nullptr,
  /*   3  LOAD_CONST_NULL         */ &Dbtup::InterpreterContext::handleLoadConstNull,
  /*   4  LOAD_CONST16            */ &Dbtup::InterpreterContext::handleLoadConst16,
  /*   5  LOAD_CONST32            */ &Dbtup::InterpreterContext::handleLoadConst32,
  /*   6  LOAD_CONST64            */ &Dbtup::InterpreterContext::handleLoadConst64,
  /*   7  ADD_REG_REG             */ &Dbtup::InterpreterContext::handleAddRegReg,
  /*   8  SUB_REG_REG             */ &Dbtup::InterpreterContext::handleSubRegReg,
  /*   9  BRANCH                  */ &Dbtup::InterpreterContext::handleBranch,
  /*  10  BRANCH_REG_EQ_NULL      */ &Dbtup::InterpreterContext::handleBranchRegEqNull,
  /*  11  BRANCH_REG_NE_NULL      */ &Dbtup::InterpreterContext::handleBranchRegNeNull,
  /*  12  BRANCH_EQ_REG_REG       */ &Dbtup::InterpreterContext::handleBranchEqRegReg,
  /*  13  BRANCH_NE_REG_REG       */ &Dbtup::InterpreterContext::handleBranchNeRegReg,
  /*  14  BRANCH_LT_REG_REG       */ &Dbtup::InterpreterContext::handleBranchLtRegReg,
  /*  15  BRANCH_LE_REG_REG       */ &Dbtup::InterpreterContext::handleBranchLeRegReg,
  /*  16  BRANCH_GT_REG_REG       */ &Dbtup::InterpreterContext::handleBranchGtRegReg,
  /*  17  BRANCH_GE_REG_REG       */ &Dbtup::InterpreterContext::handleBranchGeRegReg,
  /*  18  EXIT_OK                 */ &Dbtup::InterpreterContext::handleExitOk,
  /*  19  EXIT_REFUSE  (OVERRIDE) */ &Dbtup::InterpreterContext::handleExitRefuseCte,
  /*  20  CALL                    */ &Dbtup::InterpreterContext::handleCall,
  /*  21  RETURN                  */ &Dbtup::InterpreterContext::handleReturn,
  /*  22  EXIT_OK_LAST            */ &Dbtup::InterpreterContext::handleExitOkLast,
  /*  23  BRANCH_ATTR_OP_ARG      */ nullptr,
  /*  24  BRANCH_ATTR_EQ_NULL     */ nullptr,
  /*  25  BRANCH_ATTR_NE_NULL     */ nullptr,
  /*  26  BRANCH_ATTR_OP_PARAM    */ nullptr,
  /*  27  BRANCH_ATTR_OP_ATTR     */ nullptr,
  /*  28  LSHIFT_REG_REG          */ &Dbtup::InterpreterContext::handleLshiftRegReg,
  /*  29  RSHIFT_REG_REG          */ &Dbtup::InterpreterContext::handleRshiftRegReg,
  /*  30  MUL_REG_REG             */ &Dbtup::InterpreterContext::handleMulRegReg,
  /*  31  DIV_REG_REG             */ &Dbtup::InterpreterContext::handleDivRegReg,
  /*  32  AND_REG_REG             */ &Dbtup::InterpreterContext::handleAndRegReg,
  /*  33  OR_REG_REG              */ &Dbtup::InterpreterContext::handleOrRegReg,
  /*  34  XOR_REG_REG             */ &Dbtup::InterpreterContext::handleXorRegReg,
  /*  35  MOD_REG_REG             */ &Dbtup::InterpreterContext::handleModRegReg,
  /*  36  NOT_REG_REG             */ &Dbtup::InterpreterContext::handleNotRegReg,
  /*  37  STR_TO_INT64            */ &Dbtup::InterpreterContext::handleStrToInt64,
  /*  38  BRANCH_MEM_OP_ARG       */ &Dbtup::InterpreterContext::handleBranchMemOpArg,
  /*  39  READ_LINKED_TO_MEM      */ &Dbtup::InterpreterContext::handleReadLinkedToMem,
  /*  40  BRANCH_MEM_OP_ARG_INLINE_TYPE */ &Dbtup::InterpreterContext::handleBranchMemOpArgInlineType,
  /*  41  BRANCH_LINKED_EQ_NULL   */ &Dbtup::InterpreterContext::handleBranchLinkedEqNull,
  /*  42  BRANCH_LINKED_NE_NULL   */ &Dbtup::InterpreterContext::handleBranchLinkedNeNull,
  /*  43  READ_AGG_REG_TO_REG     */ nullptr,
  /*  44  READ_LINKED_COLUMN_TO_REG */ &Dbtup::InterpreterContext::handleReadLinkedColumnToReg,
  /*  45  LOAD_DOUBLE_CONST       */ &Dbtup::InterpreterContext::handleLoadDoubleConst,
  /*  46  (unused)                */ nullptr,
  /*  47  READ_PARTIAL_ATTR_TO_MEM*/ nullptr,
  /*  48  READ_ATTR_TO_MEM        */ nullptr,
  /*  49  READ_UINT8_MEM_TO_REG   */ &Dbtup::InterpreterContext::handleReadUint8MemToReg,
  /*  50  READ_UINT16_MEM_TO_REG  */ &Dbtup::InterpreterContext::handleReadUint16MemToReg,
  /*  51  READ_UINT32_MEM_TO_REG  */ &Dbtup::InterpreterContext::handleReadUint32MemToReg,
  /*  52  READ_INT64_MEM_TO_REG   */ &Dbtup::InterpreterContext::handleReadInt64MemToReg,
  /*  53  WRITE_UINT8_REG_TO_MEM  */ &Dbtup::InterpreterContext::handleWriteUint8RegToMem,
  /*  54  WRITE_UINT16_REG_TO_MEM */ &Dbtup::InterpreterContext::handleWriteUint16RegToMem,
  /*  55  WRITE_UINT32_REG_TO_MEM */ &Dbtup::InterpreterContext::handleWriteUint32RegToMem,
  /*  56  WRITE_INT64_REG_TO_MEM  */ &Dbtup::InterpreterContext::handleWriteInt64RegToMem,
  /*  57  WRITE_ATTR_FROM_MEM     */ nullptr,
  /*  58  APPEND_ATTR_FROM_MEM    */ nullptr,
  /*  59  LOAD_CONST_MEM          */ &Dbtup::InterpreterContext::handleLoadConstMem,
  /*  60  CONVERT_SIZE            */ &Dbtup::InterpreterContext::handleConvertSize,
  /*  61  LOAD_OP_TYPE            */ nullptr,
  /*  62  WRITE_REG_TO_MEM_ANY    */ &Dbtup::InterpreterContext::handleWriteRegToMemAny,
  /*  63  SPECIAL_INSTR           */ nullptr,

  /* --- overflow range 64..127 (opcode + OVERFLOW_OPCODE=64) ----- */
  /*  64  (unused)                */ nullptr,
  /*  65  BINARY_SEARCH_64        */ nullptr,
  /*  66  BINARY_SEARCH_32        */ nullptr,
  /*  67  BINARY_SEARCH_16        */ nullptr,
  /*  68  BINARY_SEARCH_ODD       */ nullptr,
  /*  69  SEARCH_INTERVAL_64      */ nullptr,
  /*  70  SEARCH_INTERVAL_32      */ nullptr,
  /*  71  ADD_REG_CONST           */ &Dbtup::InterpreterContext::handleAddRegConst,
  /*  72  SUB_REG_CONST           */ &Dbtup::InterpreterContext::handleSubRegConst,
  /*  73  SEARCH_INTERVAL_16      */ nullptr,
  /*  74  SEARCH_INTERVAL_ODD     */ nullptr,
  /*  75  STRING_SEARCH           */ nullptr,
  /*  76  BRANCH_EQ_REG_CONST     */ &Dbtup::InterpreterContext::handleBranchEqRegConst,
  /*  77  BRANCH_NE_REG_CONST     */ &Dbtup::InterpreterContext::handleBranchNeRegConst,
  /*  78  BRANCH_LT_REG_CONST     */ &Dbtup::InterpreterContext::handleBranchLtRegConst,
  /*  79  BRANCH_LE_REG_CONST     */ &Dbtup::InterpreterContext::handleBranchLeRegConst,
  /*  80  BRANCH_GT_REG_CONST     */ &Dbtup::InterpreterContext::handleBranchGtRegConst,
  /*  81  BRANCH_GE_REG_CONST     */ &Dbtup::InterpreterContext::handleBranchGeRegConst,
  /*  82  QSORT                   */ nullptr,
  /*  83  COMPRESS_NUM_ARRAY      */ nullptr,
  /*  84  (unused)                */ nullptr,
  /*  85  (unused)                */ nullptr,
  /*  86  (unused)                */ nullptr,
  /*  87  (unused)                */ nullptr,
  /*  88  (unused)                */ nullptr,
  /*  89  (unused)                */ nullptr,
  /*  90  (unused)                */ nullptr,
  /*  91  (unused)                */ nullptr,
  /*  92  LSHIFT_REG_CONST        */ &Dbtup::InterpreterContext::handleLshiftRegConst,
  /*  93  RSHIFT_REG_CONST        */ &Dbtup::InterpreterContext::handleRshiftRegConst,
  /*  94  MUL_REG_CONST           */ &Dbtup::InterpreterContext::handleMulRegConst,
  /*  95  DIV_REG_CONST           */ &Dbtup::InterpreterContext::handleDivRegConst,
  /*  96  AND_REG_CONST           */ &Dbtup::InterpreterContext::handleAndRegConst,
  /*  97  OR_REG_CONST            */ &Dbtup::InterpreterContext::handleOrRegConst,
  /*  98  XOR_REG_CONST           */ &Dbtup::InterpreterContext::handleXorRegConst,
  /*  99  MOD_REG_CONST           */ &Dbtup::InterpreterContext::handleModRegConst,
  /* 100  (NOT_REG_REG overflow)  */ nullptr,
  /* 101  INT64_TO_STR            */ &Dbtup::InterpreterContext::handleInt64ToStr,
  /* 102  (unused)                */ nullptr,
  /* 103  (unused)                */ nullptr,
  /* 104  (unused)                */ nullptr,
  /* 105  (unused)                */ nullptr,
  /* 106  (unused)                */ nullptr,
  /* 107  (unused)                */ nullptr,
  /* 108  (unused)                */ nullptr,
  /* 109  (unused)                */ nullptr,
  /* 110  (unused)                */ nullptr,
  /* 111  (unused)                */ nullptr,
  /* 112  (unused)                */ nullptr,
  /* 113  READ_UINT8_REG_TO_REG   */ &Dbtup::InterpreterContext::handleReadUint8RegToReg,
  /* 114  READ_UINT16_REG_TO_REG  */ &Dbtup::InterpreterContext::handleReadUint16RegToReg,
  /* 115  READ_UINT32_REG_TO_REG  */ &Dbtup::InterpreterContext::handleReadUint32RegToReg,
  /* 116  READ_INT64_REG_TO_REG   */ &Dbtup::InterpreterContext::handleReadInt64RegToReg,
  /* 117  WRITE_UINT8_REG_TO_REG  */ &Dbtup::InterpreterContext::handleWriteUint8RegToReg,
  /* 118  WRITE_UINT16_REG_TO_REG */ &Dbtup::InterpreterContext::handleWriteUint16RegToReg,
  /* 119  WRITE_UINT32_REG_TO_REG */ &Dbtup::InterpreterContext::handleWriteUint32RegToReg,
  /* 120  WRITE_INT64_REG_TO_REG  */ &Dbtup::InterpreterContext::handleWriteInt64RegToReg,
  /* 121  READ_INTERPRETER_INPUT  */ nullptr,
  /* 122  WRITE_PARTIAL_ATTR_FROM_MEM */ nullptr,
  /* 123  WRITE_INTERPRETER_OUTPUT*/ nullptr,
  /* 124  WRITE_SIZE_MEM          */ &Dbtup::InterpreterContext::handleWriteSizeMem,
  /* 125  BZERO_MEM               */ &Dbtup::InterpreterContext::handleBzeroMem,
  /* 126  (unused)                */ nullptr,
  /* 127  (unused)                */ nullptr,
};

/* ------------------------------------------------------------------
 * s_agg_interp_handlers — dispatch table for the aggregation
 * interpreter's embedded user-bytecode programs (WHERE / CASE
 * predicates inside an aggregation tree).  Consumed by
 * AggInterpreter::ProcessRec via Dbtup::interpreterJumpTable with
 * IFLAG_DISALLOW_BACKWARD_JUMPS.
 *
 * Accepted set mirrors the aggregation embedded-program validators.
 * Runs against a REAL tuple so it can include READ_ATTR_INTO_REG and
 * BRANCH_ATTR_*.  Join aggregation also supplies linked-attribute data,
 * so BRANCH_MEM_OP_ARG and BRANCH_MEM_OP_ARG_INLINE_TYPE are accepted
 * for CASE predicates over parent-table or CTE-linked columns.  This
 * differs from s_cte_filter_handlers, which must nullptr real tuple
 * attribute handlers because a CTE virtual row has no operPtrP /
 * tablePtrP.
 *
 * Deliberately omits CALL / RETURN (opcodes 20/21): combined with
 * IFLAG_DISALLOW_BACKWARD_JUMPS and forward-only branches this makes
 * every program provably terminating and lets the interpreter drop
 * its 16000-instruction fuse in this mode.  Do NOT add CALL/RETURN
 * here without also revisiting the fuse in interpreterJumpTable.
 * ------------------------------------------------------------------ */
static const InterpreterHandler
s_agg_interp_handlers[INTERP_HANDLER_TABLE_SIZE] = {
  /*   0  (unused)                */ nullptr,
  /*   1  READ_ATTR_INTO_REG      */ &Dbtup::InterpreterContext::handleReadAttrIntoReg,
  /*   2  WRITE_ATTR_FROM_REG     */ nullptr,
  /*   3  LOAD_CONST_NULL         */ &Dbtup::InterpreterContext::handleLoadConstNull,
  /*   4  LOAD_CONST16            */ &Dbtup::InterpreterContext::handleLoadConst16,
  /*   5  LOAD_CONST32            */ &Dbtup::InterpreterContext::handleLoadConst32,
  /*   6  LOAD_CONST64            */ &Dbtup::InterpreterContext::handleLoadConst64,
  /*   7  ADD_REG_REG             */ &Dbtup::InterpreterContext::handleAddRegReg,
  /*   8  SUB_REG_REG             */ &Dbtup::InterpreterContext::handleSubRegReg,
  /*   9  BRANCH                  */ &Dbtup::InterpreterContext::handleBranch,
  /*  10  BRANCH_REG_EQ_NULL      */ &Dbtup::InterpreterContext::handleBranchRegEqNull,
  /*  11  BRANCH_REG_NE_NULL      */ &Dbtup::InterpreterContext::handleBranchRegNeNull,
  /*  12  BRANCH_EQ_REG_REG       */ &Dbtup::InterpreterContext::handleBranchEqRegReg,
  /*  13  BRANCH_NE_REG_REG       */ &Dbtup::InterpreterContext::handleBranchNeRegReg,
  /*  14  BRANCH_LT_REG_REG       */ &Dbtup::InterpreterContext::handleBranchLtRegReg,
  /*  15  BRANCH_LE_REG_REG       */ &Dbtup::InterpreterContext::handleBranchLeRegReg,
  /*  16  BRANCH_GT_REG_REG       */ &Dbtup::InterpreterContext::handleBranchGtRegReg,
  /*  17  BRANCH_GE_REG_REG       */ &Dbtup::InterpreterContext::handleBranchGeRegReg,
  /*  18  EXIT_OK                 */ &Dbtup::InterpreterContext::handleExitOk,
  /*  19  EXIT_REFUSE             */ nullptr,
  /*  20  CALL                    */ nullptr,  /* termination proof */
  /*  21  RETURN                  */ nullptr,  /* termination proof */
  /*  22  EXIT_OK_LAST            */ nullptr,
  /*  23  BRANCH_ATTR_OP_ARG      */ &Dbtup::InterpreterContext::handleBranchAttrOp,
  /*  24  BRANCH_ATTR_EQ_NULL     */ &Dbtup::InterpreterContext::handleBranchAttrEqNull,
  /*  25  BRANCH_ATTR_NE_NULL     */ &Dbtup::InterpreterContext::handleBranchAttrNeNull,
  /*  26  BRANCH_ATTR_OP_PARAM    */ nullptr,
  /*  27  BRANCH_ATTR_OP_ATTR     */ nullptr,
  /*  28  LSHIFT_REG_REG          */ nullptr,
  /*  29  RSHIFT_REG_REG          */ nullptr,
  /*  30  MUL_REG_REG             */ &Dbtup::InterpreterContext::handleMulRegReg,
  /*  31  DIV_REG_REG             */ nullptr,
  /*  32  AND_REG_REG             */ nullptr,
  /*  33  OR_REG_REG              */ nullptr,
  /*  34  XOR_REG_REG             */ nullptr,
  /*  35  MOD_REG_REG             */ nullptr,
  /*  36  NOT_REG_REG             */ nullptr,
  /*  37  STR_TO_INT64            */ nullptr,
  /*  38  BRANCH_MEM_OP_ARG       */ &Dbtup::InterpreterContext::handleBranchMemOpArg,
  /*  39  READ_LINKED_TO_MEM      */ &Dbtup::InterpreterContext::handleReadLinkedToMem,
  /*  40  BRANCH_MEM_OP_ARG_INLINE_TYPE */ &Dbtup::InterpreterContext::handleBranchMemOpArgInlineType,
  /*  41  BRANCH_LINKED_EQ_NULL   */ nullptr,
  /*  42  BRANCH_LINKED_NE_NULL   */ nullptr,
  /*  43  READ_AGG_REG_TO_REG     */ &Dbtup::InterpreterContext::handleReadAggRegToReg,
  /*  44  READ_LINKED_COLUMN_TO_REG */ &Dbtup::InterpreterContext::handleReadLinkedColumnToReg,
  /*  45  LOAD_DOUBLE_CONST       */ &Dbtup::InterpreterContext::handleLoadDoubleConst,
  /*  46  (unused)                */ nullptr,
  /*  47  READ_PARTIAL_ATTR_TO_MEM*/ nullptr,
  /*  48  READ_ATTR_TO_MEM        */ nullptr,
  /*  49  READ_UINT8_MEM_TO_REG   */ &Dbtup::InterpreterContext::handleReadUint8MemToReg,
  /*  50  READ_UINT16_MEM_TO_REG  */ &Dbtup::InterpreterContext::handleReadUint16MemToReg,
  /*  51  READ_UINT32_MEM_TO_REG  */ &Dbtup::InterpreterContext::handleReadUint32MemToReg,
  /*  52  READ_INT64_MEM_TO_REG   */ &Dbtup::InterpreterContext::handleReadInt64MemToReg,
  /*  53  WRITE_UINT8_REG_TO_MEM  */ nullptr,
  /*  54  WRITE_UINT16_REG_TO_MEM */ nullptr,
  /*  55  WRITE_UINT32_REG_TO_MEM */ nullptr,
  /*  56  WRITE_INT64_REG_TO_MEM  */ nullptr,
  /*  57  WRITE_ATTR_FROM_MEM     */ nullptr,
  /*  58  APPEND_ATTR_FROM_MEM    */ nullptr,
  /*  59  LOAD_CONST_MEM          */ nullptr,
  /*  60  CONVERT_SIZE            */ nullptr,
  /*  61  LOAD_OP_TYPE            */ nullptr,
  /*  62  WRITE_REG_TO_MEM_ANY    */ nullptr,
  /*  63  SPECIAL_INSTR           */ nullptr,

  /* --- overflow range 64..127 — all rejected in agg mode ---------- */
  /*  64  (unused)                */ nullptr,
  /*  65  BINARY_SEARCH_64        */ nullptr,
  /*  66  BINARY_SEARCH_32        */ nullptr,
  /*  67  BINARY_SEARCH_16        */ nullptr,
  /*  68  BINARY_SEARCH_ODD       */ nullptr,
  /*  69  SEARCH_INTERVAL_64      */ nullptr,
  /*  70  SEARCH_INTERVAL_32      */ nullptr,
  /*  71  ADD_REG_CONST           */ nullptr,
  /*  72  SUB_REG_CONST           */ nullptr,
  /*  73  SEARCH_INTERVAL_16      */ nullptr,
  /*  74  SEARCH_INTERVAL_ODD     */ nullptr,
  /*  75  STRING_SEARCH           */ nullptr,
  /*  76  BRANCH_EQ_REG_CONST     */ nullptr,
  /*  77  BRANCH_NE_REG_CONST     */ nullptr,
  /*  78  BRANCH_LT_REG_CONST     */ nullptr,
  /*  79  BRANCH_LE_REG_CONST     */ nullptr,
  /*  80  BRANCH_GT_REG_CONST     */ nullptr,
  /*  81  BRANCH_GE_REG_CONST     */ nullptr,
  /*  82  QSORT                   */ nullptr,
  /*  83  COMPRESS_NUM_ARRAY      */ nullptr,
  /*  84  (unused)                */ nullptr,
  /*  85  (unused)                */ nullptr,
  /*  86  (unused)                */ nullptr,
  /*  87  (unused)                */ nullptr,
  /*  88  (unused)                */ nullptr,
  /*  89  (unused)                */ nullptr,
  /*  90  (unused)                */ nullptr,
  /*  91  (unused)                */ nullptr,
  /*  92  LSHIFT_REG_CONST        */ nullptr,
  /*  93  RSHIFT_REG_CONST        */ nullptr,
  /*  94  MUL_REG_CONST           */ nullptr,
  /*  95  DIV_REG_CONST           */ nullptr,
  /*  96  AND_REG_CONST           */ nullptr,
  /*  97  OR_REG_CONST            */ nullptr,
  /*  98  XOR_REG_CONST           */ nullptr,
  /*  99  MOD_REG_CONST           */ nullptr,
  /* 100  (NOT_REG_REG overflow)  */ nullptr,
  /* 101  INT64_TO_STR            */ nullptr,
  /* 102..112 (unused)            */ nullptr, nullptr, nullptr, nullptr,
                                    nullptr, nullptr, nullptr, nullptr,
                                    nullptr, nullptr, nullptr,
  /* 113  READ_UINT8_REG_TO_REG   */ nullptr,
  /* 114  READ_UINT16_REG_TO_REG  */ nullptr,
  /* 115  READ_UINT32_REG_TO_REG  */ nullptr,
  /* 116  READ_INT64_REG_TO_REG   */ nullptr,
  /* 117  WRITE_UINT8_REG_TO_REG  */ nullptr,
  /* 118  WRITE_UINT16_REG_TO_REG */ nullptr,
  /* 119  WRITE_UINT32_REG_TO_REG */ nullptr,
  /* 120  WRITE_INT64_REG_TO_REG  */ nullptr,
  /* 121  READ_INTERPRETER_INPUT  */ nullptr,
  /* 122  WRITE_PARTIAL_ATTR_FROM_MEM */ nullptr,
  /* 123  WRITE_INTERPRETER_OUTPUT*/ &Dbtup::InterpreterContext::handleWriteInterpreterOutput,
  /* 124  WRITE_SIZE_MEM          */ nullptr,
  /* 125  BZERO_MEM               */ nullptr,
  /* 126  (unused)                */ nullptr,
  /* 127  (unused)                */ nullptr,
};

int Dbtup::interpreterNextLab(Signal* signal,
                              KeyReqStruct* req_struct,
                              Uint32* mainProgram,
                              Uint32 TmainProgLen,
                              Uint32* subroutineProg,
                              Uint32 TsubroutineLen,
                              Uint32 * tmpArea,
                              Uint32 tmpAreaSz)
{
  Uint32 theRegister;
  Uint32 theInstruction;
  Uint32 TprogramCounter = 0;
  Uint32 *TcurrentProgram = mainProgram;
  Uint32 TcurrentSize = TmainProgLen;
  Uint32 RstackPtr = 0;
  char *TheapMemoryChar;
  union {
    Uint32 TregMemBuffer[32];
    Uint64 align[16];
  };
  (void)align;  // kill warning
  Uint32 TstackMemBuffer[32];

  TheapMemoryChar = (char*)&cheapMemory[0];

  Uint32 &RnoOfInstructions = req_struct->no_exec_instructions;
  ndbassert(RnoOfInstructions == 0);
  /* ---------------------------------------------------------------- */
  // Initialise all 8 registers to contain the NULL value.
  // In this version we can handle 32 and 64 bit unsigned integers.
  // They are handled as 64 bit values. Thus the 32 most significant
  // bits are zeroed for 32 bit values.
  /* ---------------------------------------------------------------- */
  TregMemBuffer[0] = NULL_INDICATOR;
  TregMemBuffer[4] = NULL_INDICATOR;
  TregMemBuffer[8] = NULL_INDICATOR;
  TregMemBuffer[12] = NULL_INDICATOR;
  TregMemBuffer[16] = NULL_INDICATOR;
  TregMemBuffer[20] = NULL_INDICATOR;
  TregMemBuffer[24] = NULL_INDICATOR;
  TregMemBuffer[28] = NULL_INDICATOR;
  Uint32 tmpHabitant = ~0;

  /* ---------------------------------------------------------------- */
  /* Build the InterpreterContext — references bind to the locals
   * above so extracted handler functions can read and mutate the
   * interpreter state just like the inline switch-case bodies used to.
   * Handlers are inlined back into the dispatch switch below, so the
   * resulting code is identical to the previous inline version.    */
  /* ---------------------------------------------------------------- */
  InterpreterContext ctx{
    this,                /* tup */
    signal,
    req_struct,
    TcurrentProgram,     /* Uint32*&  — program pointer, may swap on CALL */
    TcurrentSize,        /* Uint32&                                      */
    TprogramCounter,     /* Uint32&                                      */
    theInstruction,      /* Uint32&                                      */
    theRegister,         /* Uint32&                                      */
    &TregMemBuffer[0],   /* Uint32*  — array base                        */
    &TstackMemBuffer[0], /* Uint32*  — array base                        */
    RstackPtr,           /* Uint32&                                      */
    TheapMemoryChar,     /* char*                                        */
    RnoOfInstructions,   /* Uint32&  — bound to req_struct field         */
    tmpHabitant,         /* Uint32&                                      */
    mainProgram,         /* Uint32*  — immutable, for RETURN to restore  */
    TmainProgLen,        /* Uint32                                       */
    subroutineProg,      /* Uint32*                                      */
    TsubroutineLen,      /* Uint32                                       */
    tmpArea,             /* Uint32*                                      */
    tmpAreaSz,           /* Uint32                                       */
    nullptr,             /* const Register*                              */
  };

  /* Macro to dispatch a switch case to an extracted handler function.
   * The macro simply assigns the handler result to the loop-local _rc
   * variable. The return-value interpretation happens ONCE, after the
   * switch statement, avoiding the code duplication of inlining the
   * error/exit checks at every case label. The compiler still inlines
   * the handler (it is static inline in the same TU) so the resulting
   * code at each call site is just the inlined handler body plus a
   * store to _rc — and the post-switch check runs at most once per
   * loop iteration.
   *
   * Usage:
   *   case X:
   *     INTERP_DISPATCH(handleX);
   *     break;
   *
   * Handler return values interpreted after the switch:
   *   0 (INTERP_CONTINUE) → continue loop
   *   1 (INTERP_EXIT)      → return req_struct->log_size
   *   < 0                   → error: TUPKEY_abort(req_struct, -_rc)
   */
#define INTERP_DISPATCH(handler) \
  _rc = InterpreterContext::handler(ctx)

#ifdef TRACE_INTERPRETER
  g_eventLogger->info("(%u)Program size: %u", instance(), TcurrentSize);
#endif
  while (RnoOfInstructions < 16000) {
    /* ---------------------------------------------------------------- */
    /* EXECUTE THE NEXT INTERPRETER INSTRUCTION.                        */
    /* ---------------------------------------------------------------- */
    RnoOfInstructions++;
    theInstruction = TcurrentProgram[TprogramCounter];
    theRegister = Interpreter::getReg1(theInstruction) << 2;
#ifdef TRACE_INTERPRETER
    g_eventLogger->info(
        "(%u)Interpreter : Instruction: 0x%x"
        " RnoOfInstructions : %u.  TprogramCounter : %u.  Opcode : %u",
        instance(), theInstruction, RnoOfInstructions, TprogramCounter,
        Interpreter::getOpCode(theInstruction));
#endif

#ifdef TRACE_INTERPRETER_REGISTERS
    g_eventLogger->info(
      "REG0: %lld NULL: %u\n"
      "REG1: %lld NULL: %u\n"
      "REG2: %lld NULL: %u\n"
      "REG3: %lld NULL: %u\n"
      "REG4: %lld NULL: %u\n"
      "REG5: %lld NULL: %u\n"
      "REG6: %lld NULL: %u\n"
      "REG7: %lld NULL: %u\n",
      *(Int64*)(TregMemBuffer + 2),
      TregMemBuffer[0],
      *(Int64*)(TregMemBuffer + 6),
      TregMemBuffer[4],
      *(Int64*)(TregMemBuffer + 10),
      TregMemBuffer[8],
      *(Int64*)(TregMemBuffer + 14),
      TregMemBuffer[12],
      *(Int64*)(TregMemBuffer + 18),
      TregMemBuffer[16],
      *(Int64*)(TregMemBuffer + 22),
      TregMemBuffer[20],
      *(Int64*)(TregMemBuffer + 26),
      TregMemBuffer[24],
      *(Int64*)(TregMemBuffer + 30),
      TregMemBuffer[28]);
#endif
    if (TprogramCounter < TcurrentSize) {
      TprogramCounter++;
      Uint32 opCode = Interpreter::getOpCode(theInstruction);
      jamDebug();
      jamDataDebug(opCode);
      int _rc = INTERP_CONTINUE;
      switch (opCode) {
        case Interpreter::LOAD_OP_TYPE:
          INTERP_DISPATCH(handleLoadOpType);
          break;
        case Interpreter::READ_ATTR_INTO_REG:
          INTERP_DISPATCH(handleReadAttrIntoReg);
          break;
        case Interpreter::WRITE_ATTR_FROM_REG:
          INTERP_DISPATCH(handleWriteAttrFromReg);
          break;
        case Interpreter::WRITE_ATTR_FROM_MEM:
          INTERP_DISPATCH(handleWriteAttrFromMem);
          break;
        case Interpreter::WRITE_PARTIAL_ATTR_FROM_MEM:
          INTERP_DISPATCH(handleWritePartialAttrFromMem);
          break;
        case Interpreter::APPEND_ATTR_FROM_MEM:
          INTERP_DISPATCH(handleAppendAttrFromMem);
          break;

        case Interpreter::READ_PARTIAL_ATTR_TO_MEM:
          INTERP_DISPATCH(handleReadPartialAttrToMem);
          break;
        case Interpreter::READ_ATTR_TO_MEM:
          INTERP_DISPATCH(handleReadAttrToMem);
          break;

        case Interpreter::READ_LINKED_TO_MEM:
          INTERP_DISPATCH(handleReadLinkedToMem);
          break;
        case Interpreter::READ_AGG_REG_TO_REG:
          INTERP_DISPATCH(handleReadAggRegToReg);
          break;
        case Interpreter::READ_LINKED_COLUMN_TO_REG:
          INTERP_DISPATCH(handleReadLinkedColumnToReg);
          break;

        case Interpreter::READ_INTERPRETER_INPUT:
          INTERP_DISPATCH(handleReadInterpreterInput);
          break;
        case Interpreter::WRITE_INTERPRETER_OUTPUT:
          INTERP_DISPATCH(handleWriteInterpreterOutput);
          break;
        case Interpreter::CONVERT_SIZE:
          INTERP_DISPATCH(handleConvertSize);
          break;
        case Interpreter::WRITE_SIZE_MEM:
          INTERP_DISPATCH(handleWriteSizeMem);
          break;
        case Interpreter::READ_UINT8_MEM_TO_REG:
          INTERP_DISPATCH(handleReadUint8MemToReg);
          break;
        case Interpreter::READ_UINT16_MEM_TO_REG:
          INTERP_DISPATCH(handleReadUint16MemToReg);
          break;
        case Interpreter::READ_UINT32_MEM_TO_REG:
          INTERP_DISPATCH(handleReadUint32MemToReg);
          break;
        case Interpreter::READ_INT64_MEM_TO_REG:
          INTERP_DISPATCH(handleReadInt64MemToReg);
          break;
        case Interpreter::READ_UINT8_REG_TO_REG:
          INTERP_DISPATCH(handleReadUint8RegToReg);
          break;
        case Interpreter::READ_UINT16_REG_TO_REG:
          INTERP_DISPATCH(handleReadUint16RegToReg);
          break;
        case Interpreter::READ_UINT32_REG_TO_REG:
          INTERP_DISPATCH(handleReadUint32RegToReg);
          break;
        case Interpreter::READ_INT64_REG_TO_REG:
          INTERP_DISPATCH(handleReadInt64RegToReg);
          break;
        case Interpreter::WRITE_UINT8_REG_TO_MEM:
          INTERP_DISPATCH(handleWriteUint8RegToMem);
          break;
        case Interpreter::WRITE_UINT16_REG_TO_MEM:
          INTERP_DISPATCH(handleWriteUint16RegToMem);
          break;
        case Interpreter::WRITE_UINT32_REG_TO_MEM:
          INTERP_DISPATCH(handleWriteUint32RegToMem);
          break;
        case Interpreter::WRITE_INT64_REG_TO_MEM:
          INTERP_DISPATCH(handleWriteInt64RegToMem);
          break;
        case Interpreter::WRITE_REG_TO_MEM_ANY:
          INTERP_DISPATCH(handleWriteRegToMemAny);
          break;
        case Interpreter::WRITE_UINT8_REG_TO_REG:
          INTERP_DISPATCH(handleWriteUint8RegToReg);
          break;
        case Interpreter::WRITE_UINT16_REG_TO_REG:
          INTERP_DISPATCH(handleWriteUint16RegToReg);
          break;
        case Interpreter::WRITE_UINT32_REG_TO_REG:
          INTERP_DISPATCH(handleWriteUint32RegToReg);
          break;
        case Interpreter::WRITE_INT64_REG_TO_REG:
          INTERP_DISPATCH(handleWriteInt64RegToReg);
          break;
        case Interpreter::LOAD_CONST_NULL:
          INTERP_DISPATCH(handleLoadConstNull);
          break;
        case Interpreter::LOAD_CONST16:
          INTERP_DISPATCH(handleLoadConst16);
          break;
        case Interpreter::LOAD_CONST32:
          INTERP_DISPATCH(handleLoadConst32);
          break;
        case Interpreter::LOAD_CONST64:
          INTERP_DISPATCH(handleLoadConst64);
          break;
        case Interpreter::LOAD_DOUBLE_CONST:
          INTERP_DISPATCH(handleLoadDoubleConst);
          break;
        case Interpreter::BZERO_MEM:
          INTERP_DISPATCH(handleBzeroMem);
          break;
        case Interpreter::LOAD_CONST_MEM:
          INTERP_DISPATCH(handleLoadConstMem);
          break;
        case Interpreter::STR_TO_INT64:
          INTERP_DISPATCH(handleStrToInt64);
          break;
        case Interpreter::INT64_TO_STR:
          INTERP_DISPATCH(handleInt64ToStr);
          break;
        case Interpreter::ADD_REG_CONST:
          INTERP_DISPATCH(handleAddRegConst);
          break;
        case Interpreter::ADD_REG_REG:
          INTERP_DISPATCH(handleAddRegReg);
          break;
        case Interpreter::SUB_REG_CONST:
          INTERP_DISPATCH(handleSubRegConst);
          break;
        case Interpreter::SUB_REG_REG:
          INTERP_DISPATCH(handleSubRegReg);
          break;
        case Interpreter::LSHIFT_REG_CONST:
          INTERP_DISPATCH(handleLshiftRegConst);
          break;
        case Interpreter::LSHIFT_REG_REG:
          INTERP_DISPATCH(handleLshiftRegReg);
          break;
        case Interpreter::RSHIFT_REG_CONST:
          INTERP_DISPATCH(handleRshiftRegConst);
          break;
        case Interpreter::RSHIFT_REG_REG:
          INTERP_DISPATCH(handleRshiftRegReg);
          break;
        case Interpreter::MUL_REG_CONST:
          INTERP_DISPATCH(handleMulRegConst);
          break;
        case Interpreter::MUL_REG_REG:
          INTERP_DISPATCH(handleMulRegReg);
          break;
        case Interpreter::DIV_REG_CONST:
          INTERP_DISPATCH(handleDivRegConst);
          break;
        case Interpreter::DIV_REG_REG:
          INTERP_DISPATCH(handleDivRegReg);
          break;
        case Interpreter::AND_REG_CONST:
          INTERP_DISPATCH(handleAndRegConst);
          break;
        case Interpreter::AND_REG_REG:
          INTERP_DISPATCH(handleAndRegReg);
          break;
        case Interpreter::OR_REG_CONST:
          INTERP_DISPATCH(handleOrRegConst);
          break;
        case Interpreter::OR_REG_REG:
          INTERP_DISPATCH(handleOrRegReg);
          break;
        case Interpreter::XOR_REG_CONST:
          INTERP_DISPATCH(handleXorRegConst);
          break;
        case Interpreter::XOR_REG_REG:
          INTERP_DISPATCH(handleXorRegReg);
          break;
        case Interpreter::MOD_REG_CONST:
          INTERP_DISPATCH(handleModRegConst);
          break;
        case Interpreter::MOD_REG_REG:
          INTERP_DISPATCH(handleModRegReg);
          break;
        case Interpreter::NOT_REG_REG:
          INTERP_DISPATCH(handleNotRegReg);
          break;
        case Interpreter::BRANCH:
          INTERP_DISPATCH(handleBranch);
          break;
        case Interpreter::BRANCH_REG_EQ_NULL:
          INTERP_DISPATCH(handleBranchRegEqNull);
          break;
        case Interpreter::BRANCH_REG_NE_NULL:
          INTERP_DISPATCH(handleBranchRegNeNull);
          break;
        case Interpreter::BRANCH_EQ_REG_REG:
          INTERP_DISPATCH(handleBranchEqRegReg);
          break;
        case Interpreter::BRANCH_NE_REG_REG:
          INTERP_DISPATCH(handleBranchNeRegReg);
          break;
        case Interpreter::BRANCH_LT_REG_REG:
          INTERP_DISPATCH(handleBranchLtRegReg);
          break;
        case Interpreter::BRANCH_LE_REG_REG:
          INTERP_DISPATCH(handleBranchLeRegReg);
          break;
        case Interpreter::BRANCH_GT_REG_REG:
          INTERP_DISPATCH(handleBranchGtRegReg);
          break;
        case Interpreter::BRANCH_GE_REG_REG:
          INTERP_DISPATCH(handleBranchGeRegReg);
          break;
        case Interpreter::BRANCH_EQ_REG_CONST:
          INTERP_DISPATCH(handleBranchEqRegConst);
          break;
        case Interpreter::BRANCH_NE_REG_CONST:
          INTERP_DISPATCH(handleBranchNeRegConst);
          break;
        case Interpreter::BRANCH_LT_REG_CONST:
          INTERP_DISPATCH(handleBranchLtRegConst);
          break;
        case Interpreter::BRANCH_LE_REG_CONST:
          INTERP_DISPATCH(handleBranchLeRegConst);
          break;
        case Interpreter::BRANCH_GT_REG_CONST:
          INTERP_DISPATCH(handleBranchGtRegConst);
          break;
        case Interpreter::BRANCH_GE_REG_CONST:
          INTERP_DISPATCH(handleBranchGeRegConst);
          break;
        case Interpreter::BRANCH_ATTR_OP_ATTR:
        case Interpreter::BRANCH_ATTR_OP_ATTR + OVERFLOW_OPCODE:
        case Interpreter::BRANCH_ATTR_OP_ARG:
        case Interpreter::BRANCH_ATTR_OP_ARG + OVERFLOW_OPCODE:
        case Interpreter::BRANCH_ATTR_OP_PARAM:
        case Interpreter::BRANCH_ATTR_OP_PARAM + OVERFLOW_OPCODE:
          INTERP_DISPATCH(handleBranchAttrOp);
          break;

        case Interpreter::BRANCH_MEM_OP_ARG:
          INTERP_DISPATCH(handleBranchMemOpArg);
          break;
        case Interpreter::BRANCH_MEM_OP_ARG_INLINE_TYPE:
          INTERP_DISPATCH(handleBranchMemOpArgInlineType);
          break;

        case Interpreter::BRANCH_ATTR_EQ_NULL:
          INTERP_DISPATCH(handleBranchAttrEqNull);
          break;
        case Interpreter::BRANCH_ATTR_NE_NULL:
          INTERP_DISPATCH(handleBranchAttrNeNull);
          break;
        case Interpreter::BRANCH_LINKED_EQ_NULL:
          INTERP_DISPATCH(handleBranchLinkedEqNull);
          break;
        case Interpreter::BRANCH_LINKED_NE_NULL:
          INTERP_DISPATCH(handleBranchLinkedNeNull);
          break;
        case Interpreter::EXIT_OK:
          INTERP_DISPATCH(handleExitOk);
          break;  /* unreachable — handler returns INTERP_EXIT */
        case Interpreter::EXIT_OK_LAST:
          INTERP_DISPATCH(handleExitOkLast);
          break;  /* unreachable — handler returns INTERP_EXIT */
        case Interpreter::EXIT_REFUSE:
          /* This is a very common exit path, particularly for scans —
           * the row did not fulfil the search condition. */
          INTERP_DISPATCH(handleExitRefuse);
          break;  /* unreachable — handler returns -1 */
        case Interpreter::CALL:
          INTERP_DISPATCH(handleCall);
          break;
        case Interpreter::RETURN:
          INTERP_DISPATCH(handleReturn);
          break;
        case Interpreter::SEARCH_INTERVAL_64:
          INTERP_DISPATCH(handleSearchInterval64);
          break;
        case Interpreter::SEARCH_INTERVAL_32:
          INTERP_DISPATCH(handleSearchInterval32);
          break;
        case Interpreter::SEARCH_INTERVAL_16:
          INTERP_DISPATCH(handleSearchInterval16);
          break;
        case Interpreter::SEARCH_INTERVAL_ODD:
          INTERP_DISPATCH(handleSearchIntervalOdd);
          break;
        case Interpreter::BINARY_SEARCH_64:
          INTERP_DISPATCH(handleBinarySearch64);
          break;
        case Interpreter::BINARY_SEARCH_32:
          INTERP_DISPATCH(handleBinarySearch32);
          break;
        case Interpreter::BINARY_SEARCH_16:
          INTERP_DISPATCH(handleBinarySearch16);
          break;
        case Interpreter::BINARY_SEARCH_ODD:
          INTERP_DISPATCH(handleBinarySearchOdd);
          break;
        case Interpreter::STRING_SEARCH:
          INTERP_DISPATCH(handleStringSearch);
          break;
        case Interpreter::QSORT:
          INTERP_DISPATCH(handleQsort);
          break;
        case Interpreter::COMPRESS_NUM_ARRAY:
          INTERP_DISPATCH(handleCompressNumArray);
          break;
        case Interpreter::SPECIAL_INSTR:
          INTERP_DISPATCH(handleSpecialInstr);
          break;
        default:
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("(%u) Instruction with opCode %u doesn't exist",
                              instance(),
                              opCode);
#endif
	  return TUPKEY_abort(req_struct, ZNO_INSTRUCTION_ERROR);
      }
      /* Handler return-value interpretation — done ONCE after the switch
       * instead of inlined at every case via the INTERP_DISPATCH macro.
       *
       *   INTERP_CONTINUE (0) — fall through to the next loop iteration
       *   INTERP_EXIT     (1) — EXIT_OK / EXIT_OK_LAST: return log_size
       *   _rc < 0             — error: _rc is -(error_code). TUPKEY_abort
       *                         records jam, sets terrorCode, calls
       *                         tupkeyErrorLab, returns -1.
       */
      if (unlikely(_rc != INTERP_CONTINUE)) {
        if (_rc == INTERP_EXIT) return req_struct->log_size;
        return TUPKEY_abort(req_struct, -_rc);
      }
    } else {
      return TUPKEY_abort(req_struct, ZOUTSIDE_OF_PROGRAM_ERROR);
    }
  }
  return TUPKEY_abort(req_struct, ZTOO_MANY_INSTRUCTIONS_ERROR);
}

/**
 * interpreterJumpTable — third interpreter, dispatched via a
 * caller-supplied function-pointer table and mode flags.
 *
 * Used by two distinct call sites:
 *   1. CTE filter (execCTE_LOOKUP_REQ, cteScanAggFeed,
 *      cteScanEmitResults) via the Dbtup::interpreterFilterCte
 *      wrapper.  Table: s_cte_filter_handlers.  Flag:
 *      IFLAG_REJECT_RETURNS_NEG so EXIT_REFUSE surfaces as
 *      INTERPRETER_FILTER_REJECT rather than aborting a synthetic
 *      CTE row.
 *   2. Aggregation interpreter embedded programs
 *      (AggInterpreter::ProcessRec) — future Phase C.3.  Table:
 *      s_agg_interp_handlers.  Flag: IFLAG_DISALLOW_BACKWARD_JUMPS
 *      so programs are provably terminating (no CALL/RETURN in the
 *      table + forward-only branches) — the 16000-instruction fuse
 *      is dropped in that mode.
 *
 * The locals + InterpreterContext aggregate init mirror
 * interpreterNextLab line-for-line so the extracted handlers see
 * an identical execution environment.  The only difference is the
 * dispatch: handlerTable[opCode] instead of the big switch.
 *
 * Return values:
 *   >= 0                          — normal exit / accept
 *   INTERPRETER_FILTER_REJECT     — filter reject (IFLAG_REJECT_RETURNS_NEG)
 *   -1                            — interpreter error (terrorCode set)
 */
int Dbtup::interpreterJumpTable(Signal* signal,
                                KeyReqStruct* req_struct,
                                Uint32* mainProgram,
                                Uint32 TmainProgLen,
                                Uint32* subroutineProg,
                                Uint32 TsubroutineLen,
                                Uint32* tmpArea,
                                Uint32 tmpAreaSz,
                                const InterpreterHandler *handlerTable,
                                Uint32 flags,
                                const Register *aggRegisters)
{
  Uint32 theRegister;
  Uint32 theInstruction;
  Uint32 TprogramCounter = 0;
  Uint32 *TcurrentProgram = mainProgram;
  Uint32 TcurrentSize = TmainProgLen;
  Uint32 RstackPtr = 0;
  char *TheapMemoryChar;
  union {
    Uint32 TregMemBuffer[32];
    Uint64 align[16];
  };
  (void)align;  // kill warning
  Uint32 TstackMemBuffer[32];

  TheapMemoryChar = (char*)&cheapMemory[0];

  Uint32 &RnoOfInstructions = req_struct->no_exec_instructions;
  ndbassert(RnoOfInstructions == 0);
  /* Initialise 8 registers to NULL, as interpreterNextLab does. */
  TregMemBuffer[0]  = NULL_INDICATOR;
  TregMemBuffer[4]  = NULL_INDICATOR;
  TregMemBuffer[8]  = NULL_INDICATOR;
  TregMemBuffer[12] = NULL_INDICATOR;
  TregMemBuffer[16] = NULL_INDICATOR;
  TregMemBuffer[20] = NULL_INDICATOR;
  TregMemBuffer[24] = NULL_INDICATOR;
  TregMemBuffer[28] = NULL_INDICATOR;
  Uint32 tmpHabitant = ~0;

  InterpreterContext ctx{
    this,                /* tup */
    signal,
    req_struct,
    TcurrentProgram,     /* Uint32*&  — program pointer, may swap on CALL */
    TcurrentSize,        /* Uint32&                                      */
    TprogramCounter,     /* Uint32&                                      */
    theInstruction,      /* Uint32&                                      */
    theRegister,         /* Uint32&                                      */
    &TregMemBuffer[0],   /* Uint32*  — array base                        */
    &TstackMemBuffer[0], /* Uint32*  — array base                        */
    RstackPtr,           /* Uint32&                                      */
    TheapMemoryChar,     /* char*                                        */
    RnoOfInstructions,   /* Uint32&  — bound to req_struct field         */
    tmpHabitant,         /* Uint32&                                      */
    mainProgram,         /* Uint32*  — immutable, for RETURN to restore  */
    TmainProgLen,        /* Uint32                                       */
    subroutineProg,      /* Uint32*                                      */
    TsubroutineLen,      /* Uint32                                       */
    tmpArea,             /* Uint32*                                      */
    tmpAreaSz,           /* Uint32                                       */
    aggRegisters,        /* const Register*                              */
  };

  const bool noBackJumps = (flags & IFLAG_DISALLOW_BACKWARD_JUMPS) != 0;
  /* Instruction fuse only applies when backward jumps are allowed —
   * without them, every handler dispatch moves the PC strictly
   * forward and the program is provably terminating. */
  while (noBackJumps || RnoOfInstructions < 16000) {
    if (TprogramCounter < TcurrentSize) {
      RnoOfInstructions++;
      theInstruction = TcurrentProgram[TprogramCounter];
      theRegister    = Interpreter::getReg1(theInstruction) << 2;
      const Uint32 prevPC = TprogramCounter;
      TprogramCounter++;
      const Uint32 opCode = Interpreter::getOpCode(theInstruction);
      const InterpreterHandler h = handlerTable[opCode];
      if (unlikely(h == nullptr)) {
        jam();
        terrorCode = ZNO_INSTRUCTION_ERROR;
        return -1;
      }
      jamDebug();
      jamDataDebug(opCode);
      const int _rc = h(ctx);
      if (likely(_rc == INTERP_CONTINUE)) {
        if (noBackJumps && unlikely(TprogramCounter < prevPC)) {
          jam();
          terrorCode = ZBACKWARD_JUMP_NOT_ALLOWED;
          return -1;
        }
        continue;
      }
      if (_rc == INTERP_EXIT) return 0;                    /* accept  */
      if (_rc == Dbtup::INTERPRETER_FILTER_REJECT) {
        return _rc;                                        /* reject  */
      }
      /* Handler error: it either set terrorCode directly and returned
       * -1, or returned -(error_code) without setting it.  Mirror the
       * TUPKEY_abort path used by interpreterNextLab: derive
       * terrorCode from -_rc when the handler hasn't already set one. */
      if (_rc < -1 && terrorCode == 0) {
        terrorCode = (Uint32)(-_rc);
      }
      return -1;
    } else {
      jam();
      terrorCode = ZOUTSIDE_OF_PROGRAM_ERROR;
      return -1;
    }
  }
  jam();
  terrorCode = ZTOO_MANY_INSTRUCTIONS_ERROR;
  return -1;
}

/**
 * interpreterFilterCte — thin wrapper for Phase A/B callers that
 * dispatches via s_cte_filter_handlers with IFLAG_REJECT_RETURNS_NEG.
 */
int Dbtup::interpreterFilterCte(Signal* signal,
                                KeyReqStruct* req_struct,
                                Uint32* mainProgram,
                                Uint32 TmainProgLen,
                                Uint32* subroutineProg,
                                Uint32 TsubroutineLen,
                                Uint32* tmpArea,
                                Uint32 tmpAreaSz)
{
  return interpreterJumpTable(signal, req_struct,
                              mainProgram, TmainProgLen,
                              subroutineProg, TsubroutineLen,
                              tmpArea, tmpAreaSz,
                              s_cte_filter_handlers,
                              IFLAG_REJECT_RETURNS_NEG);
}

/**
 * interpreterAggEmbedded — Phase C.3 entry point for
 * AggInterpreter::ProcessRec's kOpEmbeddedInterp case.  Dispatches
 * the user's embedded program via s_agg_interp_handlers with
 * IFLAG_DISALLOW_BACKWARD_JUMPS — terminating by construction, so
 * the 16000-instruction fuse is off in this mode.
 */
int Dbtup::interpreterAggEmbedded(Signal* signal,
                                  KeyReqStruct* req_struct,
                                  Uint32* mainProgram,
                                  Uint32 TmainProgLen,
                                  Uint32* tmpArea,
                                  Uint32 tmpAreaSz)
{
  return interpreterAggEmbedded(signal, req_struct, mainProgram, TmainProgLen,
                                tmpArea, tmpAreaSz, nullptr);
}

int Dbtup::interpreterAggEmbedded(Signal* signal,
                                  KeyReqStruct* req_struct,
                                  Uint32* mainProgram,
                                  Uint32 TmainProgLen,
                                  Uint32* tmpArea,
                                  Uint32 tmpAreaSz,
                                  const Register *aggRegisters)
{
  return interpreterJumpTable(signal, req_struct,
                              mainProgram, TmainProgLen,
                              nullptr, 0,
                              tmpArea, tmpAreaSz,
                              s_agg_interp_handlers,
                              IFLAG_DISALLOW_BACKWARD_JUMPS,
                              aggRegisters);
}

/**
 * expand_var_part - copy packed variable attributes to fully expanded size
 *
 * dst:        where to start writing attribute data
 * dst_off_ptr where to write attribute offsets
 * src         pointer to packed attributes
 * tabDesc     array of attribute descriptors (used for getting max size)
 * order       Pointer to variable indicating which attributeId this is
 * num_vars    no of atributes to expand
 */
static
Uint32*
expand_var_part(Dbtup::KeyReqStruct::Var_data *dst, 
                const Uint32* src, 
                const Uint32 * tabDesc, 
                const Uint16* order,
                EmulatedJamBuffer *jamBuf)
{
  char* dst_ptr= dst->m_data_ptr;
  Uint32 num_vars = dst->m_var_len_offset;
  Uint16* dst_off_ptr= dst->m_offset_array_ptr;
  Uint16* dst_len_ptr= dst_off_ptr + num_vars;
  const Uint16* src_off_ptr= (const Uint16*)src;
  const char* src_ptr= (const char*)(src_off_ptr + num_vars + 1);

  Uint16 tmp= *src_off_ptr++, next_pos, len, max_len, dst_off= 0;
  for(Uint32 i = 0; i < num_vars; i++)
  {
    next_pos= *src_off_ptr++;
    len= next_pos - tmp;
    require(next_pos >= tmp);

    *dst_off_ptr++ = dst_off; 
    *dst_len_ptr++ = dst_off + len;
    memcpy(dst_ptr, src_ptr, len);
    src_ptr += len;

    max_len= AttributeDescriptor::getSizeInBytes(tabDesc[* order++]);
    thrjamDebug(jamBuf);
    thrjamDataDebug(jamBuf, max_len);
    dst_ptr += max_len; // Max size
    dst_off += max_len;

    tmp= next_pos;
  }
  return ALIGN_WORD(dst_ptr);
}

void
Dbtup::expand_tuple(KeyReqStruct* req_struct, 
                    Uint32 sizes[2],
                    Tuple_header* src, 
                    const Tablerec* tabPtrP,
                    bool disk,
                    bool from_lcp_keep)
{
  /**
   * The source tuple only touches the header parts. The updates of the
   * tuple is applied on the new copy tuple. We still need to ensure that
   * the checksum is correct on the tuple even after changing the header
   * parts since the header is part of the checksum. This is not covered
   * by setting checksum normally since mostly we don't touch the
   * original tuple.
   *
   * This updates the checksum of the source row which has already been
   * made available to the readers. Thus we need to ensure that this
   * write is protected.
   *
   * This updateChecksum seems to always be a NULL op.
   * Verified with ndbrequire
   * updateChecksum(src, tabPtrP, bits, src->m_header_bits);
   */
  Uint32 fix_size= tabPtrP->m_offsets[MM].m_fix_header_size;
  const Uint16 *order = tabPtrP->m_real_order_descriptor;
  Uint32 bits = src->m_header_bits;
  Tuple_header* ptr = req_struct->m_tuple_ptr;
  Uint32 *dst_ptr = ptr->get_end_of_fix_part_ptr(tabPtrP);

  order += tabPtrP->m_attributes[MM].m_no_of_fixsize;
  jamDebug();
  jamDataDebug(tabPtrP->m_attributes[MM].m_no_of_fixsize);
  const Uint32 *src_ptr= src->get_end_of_fix_part_ptr(tabPtrP);
  req_struct->is_expanded= true;

  // Copy in-memory fixed part
  memcpy(ptr, src, 4*fix_size);
  sizes[MM]= 0;
  sizes[DD]= 0;
  Uint32 step = 0; // in bytes

  for (Uint32 ind = 0; ind < 2; ind++)
  {
    Uint32 flex_len = 0;
    const Uint32 *flex_data = nullptr;
    Uint16 num_vars = tabPtrP->m_attributes[ind].m_no_of_varsize;
    Uint16 num_dyns = tabPtrP->m_attributes[ind].m_no_of_dynamic;
    KeyReqStruct::Var_data* dst= &req_struct->m_var_data[ind];
    if (num_vars || num_dyns)
    {
      jamDebug();
      /*
       * Reserve place for initial length word and offset array (with one extra
       * offset). This will be filled-in in later, in shrink_tuple().
       */
      dst_ptr += Varpart_copy::SZ32;
    }
    if (ind == DD)
    {
      if (disk == false || tabPtrP->m_no_of_disk_attributes == 0)
      {
        jamDebug();
        ptr->m_header_bits= (bits | Tuple_header::COPY_TUPLE);
        return;
      }
      jamDebug();
      Uint32 disk_fix_header_size = tabPtrP->m_offsets[DD].m_fix_header_size;
      jamDataDebug(disk_fix_header_size);
      jamDataDebug(tabPtrP->m_attributes[DD].m_no_of_fixsize);
      order += tabPtrP->m_attributes[DD].m_no_of_fixsize;
      Uint32 src_len = disk_fix_header_size;
      if (bits & Tuple_header::DISK_INLINE)
      {
        // Only on copy tuple
        jamDebug();
        ndbassert(bits & Tuple_header::COPY_TUPLE);
        /**
         * Need to set pointer to disk page as preparation for size
         * changes that might occur. In this case we need to check
         * free and used of the disk page to see if we need to select
         * a new disk page.
         */
        req_struct->m_disk_page_ptr.p =
          (Page*)m_global_page_pool.getPtr(req_struct->m_disk_page_ptr.i);
      }
      else
      {
        Local_key key;
        jamDebug();
        /**
         * Can still be a copy tuple if only updates so far without
         * updates of disk columns.
         */
        const Uint32 *disk_ref= src->get_disk_ref_ptr(tabPtrP);
        memcpy(&key, disk_ref, sizeof(key));
        jamDataDebug(key.m_file_no);
        jamDataDebug(key.m_page_no);
        key.m_page_no= req_struct->m_disk_page_ptr.i;
        ndbrequire(key.m_page_idx < Tup_page::DATA_WORDS);
        jamDataDebug(key.m_page_idx);
        jamDataDebug(src_len);
        src_ptr= get_dd_info(&req_struct->m_disk_page_ptr,
            key,
            tabPtrP,
            src_len);
        DEB_DISK(("(%u) disk_row(%u,%u), src_ptr: %p, src_len: %u",
              instance(),
              key.m_page_no,
              key.m_page_idx,
              src_ptr,
              src_len));
      }

      // Fix diskpart
      req_struct->m_disk_ptr = (Tuple_header*)dst_ptr;
      memcpy(dst_ptr, src_ptr, 4*disk_fix_header_size);
      sizes[DD] = src_len;
      src_ptr += disk_fix_header_size;
      dst_ptr += disk_fix_header_size;
      if (bits & Tuple_header::DISK_VAR_PART)
      {
        jamDebug();
        ndbrequire(tabPtrP->m_bits & Tablerec::TR_UseVarSizedDiskData);
        if ((num_vars + num_dyns) > 0)
        {
          if (! (bits & Tuple_header::DISK_INLINE))
          {
            jamDebug();
            PagePtr pagePtr;
            flex_len = src_len - disk_fix_header_size;
            flex_data = src_ptr;
            req_struct->m_varpart_page_ptr[DD] = req_struct->m_disk_page_ptr;
          }
          else
          {
            jamDebug();
            Varpart_copy* vp = (Varpart_copy*)src_ptr;
            flex_len = vp->m_len;
            flex_data = vp->m_data;
            req_struct->m_varpart_page_ptr[DD] = req_struct->m_page_ptr;
            sizes[DD] += flex_len;
          }
        }
      }
      else
      {
        bool var_part = (tabPtrP->m_bits & Tablerec::TR_UseVarSizedDiskData);
        ndbrequire(!var_part);
      }
      if (unlikely(req_struct->m_disk_ptr->m_base_record_page_idx >=
            Tup_page::DATA_WORDS))
      {
        Local_key key;
        const Uint32 *disk_ref= src->get_disk_ref_ptr(tabPtrP);
        memcpy(&key, disk_ref, sizeof(key));
        g_eventLogger->info("(%u) Crash on error in disk ref on row(%u,%u)"
            ", disk_page(%u,%u).%u, disk_page_ptr.i = %u"
            ", size: %u, disk_ptr: %p",
            instance(),
            req_struct->frag_page_id,
            req_struct->operPtrP->m_tuple_location.m_page_idx,
            key.m_file_no,
            key.m_page_no,
            key.m_page_idx,
            req_struct->m_disk_page_ptr.i,
            req_struct->m_disk_ptr->m_base_record_page_idx,
            req_struct->m_disk_ptr);
        ndbrequire(req_struct->m_disk_ptr->m_base_record_page_idx <
            Tup_page::DATA_WORDS);
      }
    }
    else
    {
      if (bits & Tuple_header::VAR_PART)
      {
        jamDebug();
        if (! (bits & Tuple_header::COPY_TUPLE))
        {
          jamDebug();
          /* This is for the initial expansion of a stored row. */
          const Var_part_ref* var_ref = src->get_var_part_ref_ptr(tabPtrP);
          Ptr<Page> var_page;
          flex_data= get_ptr(&var_page, *var_ref);
          flex_len= get_len(&var_page, *var_ref);
          DEB_VAR_EXPAND(("(%u) expand_tuple MM_READ: tab(%u,%u),"
                          " row(%u,%u), var_ref(%u,%u),"
                          " flex_len: %u, bits: 0x%x,"
                          " flex_data[0]: 0x%08x",
                          instance(),
                          req_struct->fragPtrP->fragTableId,
                          req_struct->fragPtrP->fragmentId,
                          req_struct->frag_page_id,
                          req_struct->operPtrP->
                            m_tuple_location.m_page_idx,
                          var_ref->m_page_no,
                          var_ref->m_page_idx,
                          flex_len,
                          bits,
                          flex_len > 0 ? flex_data[0] : 0));
          jam();
          /**
           * Coming here with MM_GROWN set is possible if we are coming here
           * from handle_lcp_keep_commit. In this case we are currently
           * performing a DELETE operation. This operation is the final
           * operation that will be committed. It could very well have
           * been preceeded by an UPDATE operation that did set the
           * MM_GROWN bit. In this case it is important to get the original
           * length from the end of the varsize part and not the page
           * entry length which is essentially the meaning of the MM_GROWN
           * bit.
           *
           * An original tuple can't have grown as we're expanding it...
           * else we would be "re-expanding". This is the case when coming
           * here as part of INSERT/UPDATE/REFRESH. We assert on that we
           * don't do any "re-expanding".
           */
          if (bits & Tuple_header::MM_GROWN)
          {
            jam();
            ndbrequire(from_lcp_keep);
            ndbassert(flex_len>0);
            flex_len= flex_data[flex_len-1];
          }
          sizes[MM]= flex_len;
          step= 0;
          req_struct->m_varpart_page_ptr[MM] = var_page;
        }
        else
        {
          /* This is for the re-expansion of a shrunken row (update2 ...) */
          Varpart_copy* vp = (Varpart_copy*)src_ptr;
          flex_len = vp->m_len;
          flex_data= vp->m_data;
          step = (Varpart_copy::SZ32 + flex_len); // 1+ is for extra word
          req_struct->m_varpart_page_ptr[MM] = req_struct->m_page_ptr;
          sizes[MM]= flex_len;
          DEB_VAR_EXPAND(("(%u) expand_tuple MM_COPY: tab(%u,%u),"
                          " row(%u,%u), flex_len: %u, bits: 0x%x,"
                          " flex_data[0]: 0x%08x",
                          instance(),
                          req_struct->fragPtrP->fragTableId,
                          req_struct->fragPtrP->fragmentId,
                          req_struct->frag_page_id,
                          req_struct->operPtrP->
                            m_tuple_location.m_page_idx,
                          flex_len,
                          bits,
                          flex_len > 0 ? flex_data[0] : 0));
          jamDebug();
          jamDataDebug(flex_len);
        }
      }
    }
    Uint32 dyn_len = flex_len;
    const Uint32 *dyn_data = flex_data;
    if (num_vars)
    {
      jamDebug();
      ndbrequire(flex_data != nullptr);
      const Uint32 *desc = req_struct->attr_descr;
      dst->m_data_ptr= (char*)(((Uint16*)dst_ptr)+num_vars+1);
      dst->m_offset_array_ptr= req_struct->var_pos_array[ind];
      dst->m_var_len_offset= num_vars;
      dst->m_max_var_offset= tabPtrP->m_offsets[ind].m_max_var_offset;

      dst_ptr= expand_var_part(dst, flex_data, desc, order, jamBuffer());
      order += num_vars;
      ndbassert(dst_ptr == ALIGN_WORD(dst->m_data_ptr + dst->m_max_var_offset));
      /**
       * Move to end of fix varpart
       */
      char* varstart = (char*)(((Uint16*)flex_data)+num_vars+1);
      Uint32 varlen = ((Uint16*)flex_data)[num_vars];
      Uint32 *dynstart = ALIGN_WORD(varstart + varlen);

      ndbassert((ptrdiff_t)flex_len >= (dynstart - flex_data));
      dyn_len -= Uint32(dynstart - flex_data);
      dyn_data = dynstart;
      DEB_VAR_EXPAND(("(%u) expand_tuple VAR_TO_DYN %s: tab(%u,%u),"
                      " row(%u,%u), num_vars: %u,"
                      " varlen: %u, flex_len: %u,"
                      " var_overhead: %u, dyn_len: %u",
                      instance(),
                      ind == MM ? "MM" : "DD",
                      req_struct->fragPtrP->fragTableId,
                      req_struct->fragPtrP->fragmentId,
                      req_struct->frag_page_id,
                      req_struct->operPtrP->
                        m_tuple_location.m_page_idx,
                      num_vars,
                      varlen,
                      flex_len,
                      Uint32(dynstart - flex_data),
                      dyn_len));
    }
    if (num_dyns)
    {
      jamDebug();
      Uint16 num_dynfix= tabPtrP->m_attributes[ind].m_no_of_dyn_fix;
      Uint16 num_dynvar= tabPtrP->m_attributes[ind].m_no_of_dyn_var;
      const Uint32 *desc = req_struct->attr_descr;
      /**
       * dynattr needs to be expanded even if no varpart existed before
       */
      dst->m_dyn_offset_arr_ptr= req_struct->var_pos_array[ind]+2*num_vars;
      dst->m_dyn_len_offset= num_dynvar+num_dynfix;
      dst->m_max_dyn_offset= tabPtrP->m_offsets[ind].m_max_dyn_offset;
      dst->m_dyn_data_ptr= (char*)dst_ptr;
      DEB_VAR_EXPAND(("(%u) expand_dyn_part %s: tab(%u,%u),"
                      " row(%u,%u), dyn_len: %u,"
                      " max_bmlen: %u, dyn_data: %p,"
                      " flex_data: %p, flex_len: %u,"
                      " dyn_data[0]: 0x%08x,"
                      " dyn_data[1]: 0x%08x",
                      instance(),
                      ind == MM ? "MM" : "DD",
                      req_struct->fragPtrP->fragTableId,
                      req_struct->fragPtrP->fragmentId,
                      req_struct->frag_page_id,
                      req_struct->operPtrP->
                        m_tuple_location.m_page_idx,
                      dyn_len,
                      tabPtrP->m_offsets[ind].m_dyn_null_words,
                      dyn_data,
                      flex_data,
                      flex_len,
                      dyn_len > 0 ? dyn_data[0] : 0,
                      dyn_len > 1 ? dyn_data[1] : 0));
      dst_ptr= expand_dyn_part(dst,
                               dyn_data,
                               dyn_len,
                               desc,
                               order,
                               num_dynvar, num_dynfix,
                               tabPtrP->m_offsets[ind].m_dyn_null_words);
      order += (num_dynvar + num_dynfix);
    }

    ndbassert((UintPtr(src_ptr) & 3) == 0);
    src_ptr = src_ptr + step;
  }
  ptr->m_header_bits = (bits |
                        Tuple_header::COPY_TUPLE |
                        Tuple_header::DISK_INLINE);
}

void Dbtup::dump_tuple(const KeyReqStruct *req_struct,
                       const Tablerec *tabPtrP) {
  Uint16 mm_vars = tabPtrP->m_attributes[MM].m_no_of_varsize;
  Uint16 mm_dyns = tabPtrP->m_attributes[MM].m_no_of_dynamic;
  // Uint16 dd_tot= tabPtrP->m_no_of_disk_attributes;
  const Tuple_header *ptr = req_struct->m_tuple_ptr;
  Uint32 bits = ptr->m_header_bits;
  const Uint32 *tuple_words = (Uint32 *)ptr;
  const Uint32 *fix_p;
  Uint32 fix_len;
  const Uint32 *var_p;
  Uint32 var_len;
  // const Uint32 *disk_p;
  // Uint32 disk_len;
  const char *typ;

  fix_p = tuple_words;
  fix_len = tabPtrP->m_offsets[MM].m_fix_header_size;
  if (req_struct->is_expanded) {
    typ = "expanded";
    var_p = ptr->get_end_of_fix_part_ptr(tabPtrP);
    var_len = 0;  // No dump of varpart in expanded
#if 0
    disk_p= (Uint32 *)req_struct->m_disk_ptr;
    disk_len= (dd_tot ? tabPtrP->m_offsets[DD].m_fix_header_size : 0);
#endif
  } else if (!(bits & Tuple_header::COPY_TUPLE)) {
    typ = "stored";
    if (mm_vars + mm_dyns) {
      // const KeyReqStruct::Var_data* dst= &req_struct->m_var_data[MM];
      const Var_part_ref *varref = ptr->get_var_part_ref_ptr(tabPtrP);
      Ptr<Page> tmp;
      var_p = get_ptr(&tmp, *varref);
      var_len = get_len(&tmp, *varref);
    } else {
      var_p = 0;
      var_len = 0;
    }
#if 0
    if(dd_tot)
    {
      Local_key key;
      memcpy(&key, ptr->get_disk_ref_ptr(tabPtrP), sizeof(key));
      key.m_page_no= req_struct->m_disk_page_ptr.i;
      disk_p= get_dd_len(&req_struct->m_disk_page_ptr,
                         &key,
                         tabPtrP,
                         &disk_len);
    }
    else
    {
      disk_p= var_p;
      disk_len= 0;
    }
#endif
  } else {
    typ = "shrunken";
    if (mm_vars + mm_dyns) {
      var_p = ptr->get_end_of_fix_part_ptr(tabPtrP);
      var_len = *((Uint16 *)var_p) + 1;
    } else {
      var_p = 0;
      var_len = 0;
    }
#if 0
    disk_p= (Uint32 *)(req_struct->m_disk_ptr);
    disk_len= (dd_tot ? tabPtrP->m_offsets[DD].m_fix_header_size : 0);
#endif
  }
  g_eventLogger->info("Fixed part[%s](%p len=%u words)", typ, fix_p, fix_len);
  dump_hex(fix_p, fix_len);
  g_eventLogger->info("Varpart part[%s](%p len=%u words)", typ, var_p, var_len);
  dump_hex(var_p, var_len);
#if 0
  g_eventLogger->info("Disk part[%s](%p len=%u words)", typ, disk_p, disk_len);
  dump_hex(disk_p, disk_len);
#endif
}

void
Dbtup::prepare_read(KeyReqStruct* req_struct, 
		    Tablerec* tabPtrP,
                    bool disk)
{
  Tuple_header* ptr = req_struct->m_tuple_ptr;
  Uint32 bits = ptr->m_header_bits;
  const Uint32 *src_ptr = ptr->get_end_of_fix_part_ptr(tabPtrP);
  req_struct->is_expanded= false;

  /**
   * We can have 0 varsized columns and a number of dynamic columns
   * that are all set to NULL values. In this case we can arrive
   * here and still have no var part. The flag VAR_PART indicates
   * there is an in-memory var part and DISK_VAR_PART indicates there
   * is a var part in the disk part.
   *
   * We also make use of the fact that if VAR_PART is set on a tuple
   * then definitely there is either a varsized or dynamic in-memory
   * column and similarly for the disk part.
   */
  for (Uint32 ind = 0; ind < 2; ind++)
  {
    KeyReqStruct::Var_data* dst= &req_struct->m_var_data[ind];
    Uint16 num_vars= tabPtrP->m_attributes[ind].m_no_of_varsize;
    Uint16 num_dyns= tabPtrP->m_attributes[ind].m_no_of_dynamic;
    /**
     * Pointer to and length of the dynamic part of the row, this
     * consists of the variable sized columns with fixed length
     * parts and the dynamic parts. We call the variable flex*
     * since they are the flexible part of the row.
     */
    Uint32 flex_len = 0;
    const Uint32 *flex_data = nullptr;

    if (ind == DD)
    {
      req_struct->m_disk_ptr = nullptr;
      if ((disk == false) || (tabPtrP->m_no_of_disk_attributes == 0))
      {
        thrjamDebug(req_struct->jamBuffer);
        return;
      }
      req_struct->m_disk_ptr = (Tuple_header*)src_ptr;
      Uint32 disk_fix_header_size = tabPtrP->m_offsets[DD].m_fix_header_size;
      if (! (bits & Tuple_header::DISK_INLINE))
      {
        thrjam(req_struct->jamBuffer);
        /**
         * We will read the disk part row from the disk page, no previous
         * updates of the disk columns have occurred in this transaction
         * so far. This means that for these reads we could fetch the
         * in-memory parts from the Copy row and the disk parts from
         * the disk page.
         */
        Local_key key;
        const Uint32 *disk_ref = ptr->get_disk_ref_ptr(tabPtrP);
        memcpy(&key, disk_ref, sizeof(key));
        key.m_page_no = req_struct->m_disk_page_ptr.i;
        ndbrequire(key.m_page_idx < Tup_page::DATA_WORDS);
        Uint32 disk_len = 0;
        src_ptr = get_dd_info(&req_struct->m_disk_page_ptr,
                              key,
                              tabPtrP,
                              disk_len);
        req_struct->m_disk_ptr = (Tuple_header*)src_ptr;
        flex_data = src_ptr + disk_fix_header_size;
        /**
         * Move past the fixed size columns to set src_ptr to point to
         * where the varsized columns start.
         */
        ndbrequire(disk_len >= disk_fix_header_size);
        flex_len = disk_len - disk_fix_header_size;
      }
      else
      {
        thrjam(req_struct->jamBuffer);
        /**
         * On COPY tuples the disk data columns comes immediately after
         * the in-memory columns. The address was calculated in the first
         * loop and thus src_ptr already points to the first set of disk
         * data columns.
         */
        ndbrequire(bits & Tuple_header::COPY_TUPLE);
        src_ptr += disk_fix_header_size;
        if (bits & Tuple_header::DISK_VAR_PART)
        {
          thrjam(req_struct->jamBuffer);
          Varpart_copy* vp = (Varpart_copy*)src_ptr;
          flex_len = vp->m_len;
          flex_data = vp->m_data;
          src_ptr++;
        }
      }
      if (unlikely(req_struct->m_disk_ptr->m_base_record_page_idx >=
                   Tup_page::DATA_WORDS))
      {
        Local_key key;
        const Uint32 *disk_ref = ptr->get_disk_ref_ptr(tabPtrP);
        memcpy(&key, disk_ref, sizeof(key));
        g_eventLogger->info(
          "Crash: page(%u,%u,%u,%u).%u, DISK_INLINE= %u, tab(%x,%x,%x)"
                       ", frag_page_id:%u, rowid_ref(%u,%u)",
                       instance(),
                       req_struct->m_disk_page_ptr.i,
                       req_struct->m_disk_page_ptr.p->m_file_no,
                       req_struct->m_disk_page_ptr.p->m_page_no,
                       key.m_page_idx,
                       bits & Tuple_header::DISK_INLINE ? 1 : 0,
                       req_struct->m_disk_page_ptr.p->m_table_id,
                       req_struct->m_disk_page_ptr.p->m_fragment_id,
                       req_struct->m_disk_page_ptr.p->m_create_table_version,
                       req_struct->frag_page_id,
                       req_struct->m_disk_ptr->m_base_record_page_no,
                       req_struct->m_disk_ptr->m_base_record_page_idx);
        ndbrequire(req_struct->m_disk_ptr->m_base_record_page_idx <
                   Tup_page::DATA_WORDS);
      }
      if (unlikely((bits & Tuple_header::DISK_VAR_PART) == 0))
      {
        thrjamDebug(req_struct->jamBuffer);
        ndbrequire((tabPtrP->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0);
        dst->m_max_var_offset = 0;
        dst->m_dyn_part_len = 0;
#if defined(VM_TRACE) || defined(ERROR_INSERT)
        std::memset(dst, 0, sizeof(* dst));
#endif
        return;
      }
    }
    else
    {
      /* ind == MM */
      if (num_vars == 0 && num_dyns == 0)
      {
        thrjamDebug(req_struct->jamBuffer);
        continue;
      }
      if (unlikely((bits & Tuple_header::VAR_PART) == 0))
      {
        thrjamDebug(req_struct->jamBuffer);
        dst->m_max_var_offset = 0;
        dst->m_dyn_part_len = 0;
#if defined(VM_TRACE) || defined(ERROR_INSERT)
        std::memset(dst, 0, sizeof(* dst));
#endif
        continue;
      }
      if (! (bits & Tuple_header::COPY_TUPLE))
      {
        thrjamDebug(req_struct->jamBuffer);
        Ptr<Page> tmp;
        Var_part_ref* var_ref = ptr->get_var_part_ref_ptr(tabPtrP);
        flex_data= get_ptr(&tmp, * var_ref);
        flex_len= get_len(&tmp, * var_ref);

        /* If the original tuple was grown,
         * the old size is stored at the end. */
        if (bits & Tuple_header::MM_GROWN)
        {
          /**
           * This is when triggers read before value of update
           *   when original has been reallocated due to grow
           */
          ndbassert(flex_len>0);
          thrjam(req_struct->jamBuffer);
          flex_len= flex_data[flex_len-1];
        }
      }
      else
      {
        thrjam(req_struct->jamBuffer); // Read Copy tuple
        Varpart_copy* vp = (Varpart_copy*)src_ptr;
        flex_len = vp->m_len;
        flex_data = vp->m_data;
        src_ptr++;
      }
      /* Set up src_ptr for DD loop */
      src_ptr += flex_len;
    }
    char *varstart;
    Uint32 varlen;
    const Uint32 *dynstart;
    if (num_vars)
    {
      varstart = (char *)(((Uint16 *)flex_data) + num_vars + 1);
      varlen = ((Uint16 *)flex_data)[num_vars];
      dynstart = ALIGN_WORD(varstart + varlen);
#ifdef TUP_DATA_VALIDATION
      thrjam(req_struct->jamBuffer);
      thrjamLine(req_struct->jamBuffer, num_vars);
      for (Uint16 i = 0; i < (num_vars + 1); i++)
        thrjamLine(req_struct->jamBuffer, ((Uint16*)flex_data)[i]);
#endif
    }
    else
    {
#ifdef TUP_DATA_VALIDATION
      thrjam(req_struct->jamBuffer);
#endif
      varstart = 0;
      varlen = 0;
      dynstart = flex_data;
    }

    dst->m_data_ptr= varstart;
    dst->m_offset_array_ptr = (Uint16*)flex_data;
    dst->m_var_len_offset = 1;
    dst->m_max_var_offset = varlen;

    Uint32 dynlen = Uint32(flex_len - (dynstart - flex_data));
    ndbassert((ptrdiff_t)flex_len >= (dynstart - flex_data));
    dst->m_dyn_data_ptr= (char*)dynstart;
    dst->m_dyn_part_len= dynlen;
  }
}

void Dbtup::shrink_tuple(KeyReqStruct *req_struct, Uint32 sizes[2],
                         const Tablerec *tabPtrP, bool disk) {
  ndbassert(tabPtrP->need_shrink());
  ndbassert(req_struct->is_expanded);
  Tuple_header* ptr= req_struct->m_tuple_ptr;
  ndbassert(ptr->m_header_bits & Tuple_header::COPY_TUPLE);
  
  const Uint16* order= tabPtrP->m_real_order_descriptor;
  const Uint32 * tabDesc = req_struct->attr_descr;
  
  Uint32 *dst_ptr= ptr->get_end_of_fix_part_ptr(tabPtrP);

  /**
   * shrink_tuple is called when there is disk attributes and/or
   * when there is a variable sized in-memory part. Thus we could
   * come here without a variable sized part.
   */
  sizes[MM] = 0;
  sizes[DD] = 0;

  /**
   * No need to copy the fixed size memory parts, those are
   * already in the correct position.
   */
  for (Uint32 ind = 0; ind < 2; ind++)
  {
    Uint16 num_fix= tabPtrP->m_attributes[ind].m_no_of_fixsize;
    Uint16 num_vars= tabPtrP->m_attributes[ind].m_no_of_varsize;
    Uint16 num_dyns= tabPtrP->m_attributes[ind].m_no_of_dynamic;
    if (ind == DD)
    {
      Uint16 dd_tot = tabPtrP->m_no_of_disk_attributes;
      if (!(disk && dd_tot))
      {
        jamDebug();
        break;
      }
      Uint32 * src_ptr = (Uint32*)req_struct->m_disk_ptr;
      req_struct->m_disk_ptr = (Tuple_header*)dst_ptr;
      Uint32 disk_fix_header_size = tabPtrP->m_offsets[DD].m_fix_header_size;
      sizes[DD] = disk_fix_header_size;
      memmove(dst_ptr, src_ptr, 4 * disk_fix_header_size);
      dst_ptr += disk_fix_header_size;
      if ((tabPtrP->m_bits & Tablerec::TR_UseVarSizedDiskData) != 0)
      {
        jamDebug();
        ptr->m_header_bits |= Tuple_header::DISK_VAR_PART;
      }
      else
      {
        jamDebug();
      }
    }
    order += num_fix;
    if (num_vars || num_dyns)
    {
      jamDebug();
      Varpart_copy* vp = (Varpart_copy*)dst_ptr;
      Uint32* varstart = vp->m_data;
      dst_ptr = vp->m_data;

      if (num_vars)
      {
        jamDebug();
        Uint16* src_off_ptr= req_struct->var_pos_array[ind];
        Uint16* dst_off_ptr= (Uint16*)dst_ptr;
        char*  dst_data_ptr= (char*)(dst_off_ptr + num_vars + 1);
        char* src_data_ptr = (char*)req_struct->m_var_data[ind].m_data_ptr;
        ndbassert((ind == DD) || (src_data_ptr == dst_data_ptr));
        Uint32 off= 0;
        for (Uint32 i = 0; i < num_vars; i++)
        {
          /**
           * var_pos_array has one index for main memory part and
           * the second is the disk columns part.
           *
           * Each var_pos_array has 2 parts, the first is index by the
           * index of the varsize column, the second is the index
           * plus the number of varsize columns. These were initialised
           * to the position of the start of the column (both of them),
           * starting at position 0.
           *
           * When updating the varsize column we set the index plus
           * num_vars to instead be position of the end of the varsize
           * column. Thus var_pos_array[num_vars + i] - var_pos_array[i]
           * is the length of the column. For NULL values both are set
           * to the position of the column and thus length is 0.
           *
           * Seems a bit complicated manner to calculate length, but it
           * means that we retain the position of the column.
           *
           * In the stored row we store the offset of each varsize column,
           * starting at 0, in addition we store the total length of all
           * varsize column as an extra length information.
           */
          const char* data_ptr= src_data_ptr + *src_off_ptr;
          Uint32 len= src_off_ptr[num_vars] - *src_off_ptr;
          * dst_off_ptr++= off;
          memmove(dst_data_ptr, data_ptr, len);
          off += len;
          src_off_ptr++;
          dst_data_ptr += len;
          jamDebug();
          jamDataDebug(len);
        }
        *dst_off_ptr= off;
        dst_ptr = ALIGN_WORD(dst_data_ptr);
        order += num_vars; // Point to first dynfix entry
      }

      if (num_dyns)
      {
        jamDebug();
        Uint16 num_dynvar= tabPtrP->m_attributes[ind].m_no_of_dyn_var;
        Uint16 num_dynfix= tabPtrP->m_attributes[ind].m_no_of_dyn_fix;
        KeyReqStruct::Var_data* dst = &req_struct->m_var_data[ind];
        dst_ptr = shrink_dyn_part(dst,
                                  dst_ptr,
                                  tabPtrP,
                                  tabDesc,
                                  order,
                                  num_dynvar,
                                  num_dynfix,
                                  ind);
        order += (num_dynfix + num_dynvar);
      }
      Uint32 varpart_len_words = Uint32(dst_ptr - varstart);
      ndbassert(varpart_len_words <= MAX_EXPANDED_TUPLE_SIZE_IN_WORDS);
      vp->m_len = varpart_len_words;
      DEB_VAR_EXPAND(("(%u) shrink_tuple %s: tab(%u,%u),"
                      " row(%u,%u), varpart_len: %u,"
                      " vp->m_data[0]: 0x%08x",
                      instance(),
                      ind == MM ? "MM" : "DD",
                      req_struct->fragPtrP->fragTableId,
                      req_struct->fragPtrP->fragmentId,
                      req_struct->frag_page_id,
                      req_struct->operPtrP->
                        m_tuple_location.m_page_idx,
                      varpart_len_words,
                      varpart_len_words > 0 ? vp->m_data[0] : 0));
      if (ind == MM)
      {
        sizes[MM] = varpart_len_words;
        if (varpart_len_words != 0)
        {
          jamDebug();
          jamDataDebug(varpart_len_words);
          ptr->m_header_bits |= Tuple_header::VAR_PART;
        }
        else if ((ptr->m_header_bits & Tuple_header::VAR_PART) == 0)
        {
          jamDebug();
          /*
           * No varpart present.
           * And this is not an update where the dynamic column is set to null.
           * So skip storing the var part altogether.
           */
          ndbassert(((Uint32*) vp) == ptr->get_end_of_fix_part_ptr(tabPtrP));
          dst_ptr= (Uint32*)vp;
        }
        else
        {
          jamDebug();
          /*
           * varpart_len is now 0, but tuple already had a varpart.
           * It will be released at commit time.
           */
        }
      }
      else
      {
        sizes[DD] += varpart_len_words;
        jamDebug();
        jamDataDebug(varpart_len_words);
      }
      ndbassert((UintPtr(ptr) & 3) == 0);
      ndbassert(varpart_len_words < 0x2000);
    }
  }
  req_struct->is_expanded= false;
}

void
Dbtup::validate_page(TablerecPtr regTabPtr, Var_page* p)
{
  /* ToDo: We could also do some checks here for any dynamic part. */
  Uint32 mm_vars= regTabPtr.p->m_attributes[MM].m_no_of_varsize;
  Uint32 fix_sz= regTabPtr.p->m_offsets[MM].m_fix_header_size + 
    Tuple_header::HeaderSize;
    
  if(mm_vars == 0)
    return;
  
  for(Uint32 F= 0; F < MAX_FRAG_PER_LQH; F++)
  {
    FragrecordPtr fragPtr;

    fragPtr.i = c_lqh->m_ldm_instance_used->getNextTupFragrec(regTabPtr.i, F);
    if (fragPtr.i == RNIL64)
      continue;

    ndbrequire(c_fragment_pool.getPtr(fragPtr));
    for(Uint32 P= 0; P<fragPtr.p->noOfPages; P++)
    {
      Uint32 real= getRealpid(fragPtr.p, P);
      Var_page* page= (Var_page*)c_page_pool.getPtr(real);

      for(Uint32 i=1; i<page->high_index; i++)
      {
	Uint32 idx= page->get_index_word(i);
	Uint32 len = (idx & Var_page::LEN_MASK) >> Var_page::LEN_SHIFT;
	if(!(idx & Var_page::FREE) && !(idx & Var_page::CHAIN))
	{
	  Tuple_header *ptr= (Tuple_header*)page->get_ptr(i);
	  Uint32 *part= ptr->get_end_of_fix_part_ptr(regTabPtr.p);
	  if(! (ptr->m_header_bits & Tuple_header::COPY_TUPLE))
	  {
	    ndbrequire(len == fix_sz + 1);
            Local_key tmp;
            Var_part_ref *vpart = reinterpret_cast<Var_part_ref *>(part);
            vpart->copyout(&tmp);
#if defined(VM_TRACE) || defined(ERROR_INSERT)
            ndbrequire(!"Looking for test coverage - found it!");
#endif
            Ptr<Page> tmpPage;
            part = get_ptr(&tmpPage, *vpart);
            len = ((Var_page *)tmpPage.p)->get_entry_len(tmp.m_page_idx);
            Uint32 sz = ((mm_vars + 1) << 1) + (((Uint16 *)part)[mm_vars]);
            ndbrequire(len >= ((sz + 3) >> 2));
          } else {
            Uint32 sz = ((mm_vars + 1) << 1) + (((Uint16 *)part)[mm_vars]);
            ndbrequire(len >= ((sz + 3) >> 2) + fix_sz);
          }
          if (ptr->m_operation_ptr_i != RNIL) {
            OperationrecPtr operPtr;
            operPtr.i = ptr->m_operation_ptr_i;
	    ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(operPtr));
	  }
	} 
	else if(!(idx & Var_page::FREE))
	{
	  /**
	   * Chain
	   */
	  Uint32 *part= page->get_ptr(i);
	  Uint32 sz= ((mm_vars + 1) << 1) + (((Uint16*)part)[mm_vars]);
	  ndbrequire(len >= ((sz + 3) >> 2));
	} 
	else 
	{
	  
	}
      }
      if (p == 0 && page->high_index > 1) page->reorg((Var_page *)ctemp_page);
    }
  }

  if (p == 0) {
    validate_page(regTabPtr, (Var_page *)1);
  }
}

int
Dbtup::handle_size_change_after_update(Signal *signal,
                                       KeyReqStruct* req_struct,
				       Tuple_header* org,
				       Operationrec* regOperPtr,
				       Fragrecord* regFragPtr,
				       Tablerec* regTabPtr,
				       Uint32 sizes[4])
{
  Uint32 bits = m_base_header_bits;
  Uint32 copy_bits= req_struct->m_tuple_ptr->m_header_bits;
  
  DEB_LCP(("(%u)size_change: tab(%u,%u), row_id(%u,%u), old: %u, new: %u",
          instance(),
          req_struct->fragPtrP->fragTableId,
          req_struct->fragPtrP->fragmentId,
          regOperPtr->m_tuple_location.m_page_no,
          regOperPtr->m_tuple_location.m_page_idx,
          sizes[2+MM],
          sizes[MM]));
  for (Uint32 ind = 0; ind < 2; ind++)
  {
    jamDebug();
    jamDataDebug(ind);
    if(sizes[2+ind] == sizes[ind])
    {
      jam();
      continue;
    }
    else if(sizes[2+ind] < sizes[ind])
    {
      jam();
      continue;
    }
    jam();
    if (ind == DD)
    {
      jamDebug();
      /**
       * We have updated the disk part row such that it has grown, this
       * means that we need to ensure that more space is allocated before
       * can proceed, we will attempt to use the current disk page. But if
       * this lacks space for the new size we will allocate space in
       * another page.
       *
       * We use the No Steal approach which means that we are not allow
       * to write dirty information to a page, thus we cannot touch the
       * disk page until we commit it. Thus we cannot follow the same
       * approach as for variable sized memory pages. We have to allocate
       * a new area for the row and ensure that there is space for the new
       * disk part row should the transaction finally commit.
       *
       * We need however to write some temporary information to the page.
       * This information doesn't affect any of the row data in the page.
       * It only affects the page header information. The variables we
       * can change here are:
       * 1) uncommitted_used_space
       *    We need this variable to preallocate area on the page.
       *    This variable is changed only when we increase the row size
       *    on the same page or dropping a row due to moving the row
       *    to a new page.
       * 2) m_restart_seq
       *    The number of the restart that the page was last written.
       *
       * This variable is affected by calls to disk_page_prealloc, this call
       * use the ALLOC_REQ flag to PGMAN, this will make the page dirty and
       * thus no special handling of this flag is required.
       *
       * When we read the page before an UPDATE/DELETE/WRITE we haven't
       * set anything that indicates that the page is dirty. If we update
       * uncommitted_used_space we thus need to indicate to PGMAN that
       * the page must be written to disk before cleaned out. We do this
       * through a get_page call using the DIRTY_HEADER flag.
       *
       * The free_space variable is read, but this is only updated during
       * commit of a transaction.
       */
      if ((regTabPtr->m_bits & Tablerec::TR_UseVarSizedDiskData) != 0)
      {
        PagePtr diskPagePtr = req_struct->m_disk_page_ptr;
        Uint32 free = diskPagePtr.p->free_space;
        Uint32 used = diskPagePtr.p->uncommitted_used_space;
        Uint32 new_size = sizes[2+DD];
        bool disk_alloc_flag = copy_bits & Tuple_header::DISK_ALLOC;
        bool disk_reorg_flag = bits & Tuple_header::DISK_REORG;
        DEB_REORG(("(%u) free: %u, used: %u, new_size: %u, size[DD]: %u"
                   ", alloc_flag: %u, reorg_flag: %u",
                   instance(),
                   free,
                   used,
                   new_size,
                   sizes[DD],
                   disk_alloc_flag,
                   disk_reorg_flag));
        jamDataDebug(free);
        jamDataDebug(used);
        if (unlikely(disk_alloc_flag || disk_reorg_flag))
        {
          jamDebug();
          ndbrequire(regOperPtr->m_uncommitted_used_space == 0);
          /**
           * disk_alloc_flag true:
           * We started the transaction without a row in this position.
           * Thus this must be a multi-row operation that either
           * re-inserts the row or updates the row with a new size.
           *
           * In this case we have allocated the row in a page which we
           * have access to, thus we can either change the size used by
           * this page or move the row to a new page.
           *
           * disk_reorg_flag true:
           * We have already previously moved to a new disk page.
           * Thus we need to check if the new page is large enough
           * handle also the new size of the row. This is very
           * similar to the handling of a row operation that starts
           * with an initial insert operation.
           *
           * One difference is that the size of the new page is found
           * in the copy row. Another difference is that diskPagePtr
           * refers to the original page and thus using
           * disk_page_abort_prealloc_callback_1 directly is replaced
           * by using disk_page_abort_prealloc. We still know that
           * the page is retrieved before reaching here, so we are
           * safe that no real-time break will happen here.
           */
          Local_key key;
          PagePtr used_pagePtr;
          const Uint32 *disk_ref =
            req_struct->m_tuple_ptr->get_disk_ref_ptr(regTabPtr);
          memcpy(&key, disk_ref, sizeof(key));
          jamDataDebug(key.m_file_no);
          jamDataDebug(key.m_page_no);
          if (disk_reorg_flag)
          {
            jamDebug();
            /**
             * We are not using diskPagePtr, need to recalculate
             * some variables and set use_pagePtr to new disk page.
             */
            used_pagePtr.i = regOperPtr->m_disk_extra_callback_page;
            used_pagePtr.p =
              (Page*)m_global_page_pool.getPtr(used_pagePtr.i);
            used = used_pagePtr.p->uncommitted_used_space;
            free = used_pagePtr.p->free_space;
          }
          else
          {
            jamDebug();
            used_pagePtr = diskPagePtr;
          }
          Uint32 curr_size = key.m_page_idx;
          Int32 add = new_size - curr_size;
          jamDataDebug(new_size);
          jamDataDebug(curr_size);
          DEB_REORG(("(%u) (file,page): (%u,%u), new_size: %u, curr_size: %u"
                     ", add: %d, free: %u, used: %u",
                     instance(),
                     key.m_file_no,
                     key.m_page_no,
                     new_size,
                     curr_size,
                     add,
                     free,
                     used));
          if ((used + add) <= free)
          {
            jamDebug();
            /**
             * The row fits in the page already used. We will either
             * add more to the uncommitted_used_space of the page or
             * decrease it.
             */
            if (add > 0)
            {
              jamDebug();
              /**
               * We also need to update the size allocated that is stored
               * in the local key of the disk row reference. When DISK_ALLOC
               * is set (an initial insert is the first operation on the row).
               * There is no need to set it in the disk row reference stored
               * in the in-memory row that will become the row. We set it
               * anyways for consistency.
               *
               * Need to set the checksum using the entire row, this happens
               * with exclusive access, so it is safe to set a new checksum.
              */
              disk_page_dirty_header(signal,
                                     regFragPtr,
                                     key,
                                     used_pagePtr,
                                     add);
              key.m_page_idx = new_size;
              memcpy(req_struct->m_tuple_ptr->get_disk_ref_ptr(regTabPtr),
                     &key,
                     sizeof(key));
              if (disk_alloc_flag)
              {
                jam();
                memcpy(org->get_disk_ref_ptr(regTabPtr),
                       &key,
                       sizeof(key));
                setChecksum(org, regTabPtr);
              }
            }
            else
            {
              jamDebug();
              /**
               * We follow the principle that we never return any preallocated
               * storage. This would create problems if we roll back one or
               * more operations. We cannot safely allocate storage later in
               * the process and we cannot abort already prepared operations.
               * Thus we avoid returning memory here.
               */
            }
          }
          else
          {
            jamDebug();
            ndbrequire(add > 0);
            /**
             * We grew out of the current page, we need to allocate a new page
             * and deallocate the current page.
             * We deallocate after allocating from a new page. Finally
             * we also need to update the disk reference in the copy row.
             */
            /* Add extra word to handle possible directory size increase.*/
            new_size++;
            Local_key new_key;
            jamDebug();
            int ret = disk_page_prealloc(signal,
                                         prepare_fragptr,
                                         regTabPtr,
                                         &new_key,
                                         new_size);
            if (unlikely(ret < 0))
            {
              jam();
              terrorCode = -ret;
              return ret;
            }
            jamDebug();
            jamDataDebug(new_key.m_file_no);
            jamDataDebug(new_key.m_page_no);
            new_key.m_page_idx = new_size;
            memcpy(req_struct->m_tuple_ptr->get_disk_ref_ptr(regTabPtr),
                   &new_key,
                   sizeof(new_key));
            if (disk_alloc_flag)
            {
              jam();
              /**
               * Update the disk row reference also in the row to be
               * committed to ensure that we handle any further row
               * operation on the same row in the same transaction
               * in a proper manner.
               *
               * Ensure that checksum is updated as well, we have exclusive
               * access, so no worries with that.
               */
              memcpy(org->get_disk_ref_ptr(regTabPtr),
                     &new_key,
                     sizeof(new_key));
              setChecksum(org, regTabPtr);
            }
            ndbrequire(used_pagePtr.p->m_restart_seq == globalData.m_restart_seq);
            disk_page_abort_prealloc_callback_1(signal,
                                                regFragPtr,
                                                used_pagePtr,
                                                curr_size,
                                                0);
          }
          return 0;
        }
        else
        {
          jamDebug();
          Local_key key;
          const Uint32 *disk_ref= org->get_disk_ref_ptr(regTabPtr);
          memcpy(&key, disk_ref, sizeof(key));
          Uint32 curr_size =
            ((Var_page*)diskPagePtr.p)->get_entry_len(key.m_page_idx);
          Int32 add = new_size - curr_size;
          add -= regOperPtr->m_uncommitted_used_space;
          jamDataDebug(new_size);
          jamDataDebug(curr_size);
          jamDataDebug(regOperPtr->m_uncommitted_used_space);
          DEB_REORG(("(%u) key(%u,%u,%u), new_size: %u, curr_size: %u"
                     ", add: %d, uncommitted_used_space: %u",
                     instance(),
                     key.m_file_no,
                     key.m_page_no,
                     key.m_page_idx,
                     new_size,
                     curr_size,
                     add,
                     regOperPtr->m_uncommitted_used_space));
          /**
           * curr_size is the size of the row before the transaction
           * started. new_size is the size after this operation is
           * completed. regOperPtr->m_uncommitted_used_space is the
           * size we already seized in the page, thus add will here
           * only be positive if we need to increase our allocation.
           */
          if (Int32(Int32(used) + add) <= Int32(free))
          {
            /**
             * Size of new row fits in the current disk page, no need
             * to move the row to another page.
             */
            if (add > 0)
            {
              jamDebug();
              jamDataDebug(Uint16(add));
              /* Size has grown, but we still fit in the original disk page. */
              disk_page_dirty_header(signal,
                                     regFragPtr,
                                     key,
                                     diskPagePtr,
                                     add);
              regOperPtr->m_uncommitted_used_space += add;
              jamDebug();
              jamDataDebug(Uint16(regOperPtr->m_uncommitted_used_space));
            }
            else
            {
              jamDebug();
              /**
               * The row has either shrunk or is of the same size, so no need
               * to do anything since we cannot shrink the original row until
               * commit time.
               *
               * We will never decrease m_uncommitted_used_space to allow for
               * various abort variants. Otherwise we might have to allocate
               * space in abort or commit which is not safe.
               */
            }
          }
          else
          {
            jamDebug();
            /**
             * The row will no longer fit in the original page. Thus we have
             * to move the row to a new page. We set the DISK_REORG flag.
             */
            jam();
            ndbrequire(add > 0);
            Uint32 undo_len = sizeof(Dbtup::Disk_undo::Alloc) >> 2;
            {
              Logfile_client lgman(this,
                                   c_lgman,
                                   regFragPtr->m_logfile_group_id);
              DEB_LCP_LGMAN(("(%u)alloc_log_space(%u): %u",
                             instance(),
                             __LINE__,
                             undo_len));
              terrorCode = lgman.alloc_log_space(
                undo_len,
                true,
                !req_struct->m_nr_copy_or_redo,
                jamBuffer());
            }
            if (unlikely(terrorCode))
            {
              jam();
              return -1;
            }
            jamDebug();
            /**
             * There are two ways to get here. The first is by starting with
             * a UPDATE that includes updating the disk part of the row. It
             * could be this operation or a previous operation. In both cases
             * we allocated log space for an UNDO of the UPDATE operation
             * as part of expand_tuple which happens before coming to this
             * part of the code which happens after completing the updates
             * on the row.
             *
             * The second variant is to get here starting with a DELETE
             * operation on the row followed by an INSERT of the row
             * again. This INSERT could also have been followed up with
             * an UPDATE operation. In all cases we will arrive here
             * with log space allocated in the DELETE operation that is
             * calculated based on the size of the row.
             *
             * Thus in both cases we have allocated log space for the UNDO
             * of the free of this disk row. The size of this UNDO will be
             * based on the size of the row at start of the transaction.
             * This is the size set at in initial UPDATE operation in
             * expand_tuple which is thus large enough. The size of the
             * allocated log space after an initial DELETE operation is
             * larger than required. We could free this space up here, but
             * it will only be of any help in extreme overload situations.
             *
             * The extra log space required for adding a new disk row is
             * always the same size and the state DISK_REORG will always
             * have an additional log space allocated for this purpose.
             * If we follow up this operation with a subsequent DELETE
             * operation we will free this extra log space and free the
             * new disk row and remove the DISK_REORG flag.
             */
            ndbrequire(regOperPtr->m_undo_buffer_space > 0);
            jamDataDebug(regOperPtr->m_undo_buffer_space);
            if (regOperPtr->m_uncommitted_used_space > 0)
            {
              /**
               * We have allocated space on the original row page.
               * We need to release this extra memory already now.
               * The memory used by the original row will be released
               * at commit time if the operation is committed.
               *
               * Normally we would keep the memory allocated extra on
               * the disk page until either a full abort or a commit
               * of the transaction. This is to avoid complex abort
               * handling. However changing to DISK_REORG is a
               * persistent operation lasting the rest of the transaction
               * and it is thus safe to deallocate the extra space used
               * by the old row since we have committed to using a new
               * row if we commit whatever type of operations happens
               * after this.
               */
              jam();
              Int32 overflow_space = -regOperPtr->m_uncommitted_used_space;
              ndbrequire(diskPagePtr.p->m_restart_seq == globalData.m_restart_seq);
              disk_page_dirty_header(signal,
                                     regFragPtr,
                                     key,
                                     diskPagePtr,
                                     overflow_space);
              regOperPtr->m_uncommitted_used_space = 0;
            }
            Local_key new_key;
            /* Add extra word to handle possible directory size increase.*/
            new_size++;
            int ret = disk_page_prealloc(signal,
                                         prepare_fragptr,
                                         regTabPtr,
                                         &new_key,
                                         new_size);
            if (unlikely(ret < 0))
            {
              jam();
              terrorCode = -ret;
              /**
               * Need to release log space since only recalled through
               * setting DISK_REORG bit. This bit is only set in a
               * successful prepare operation. Thus we need only abort
               * when the entire transaction is aborted.
               */
              Uint32 undo_insert_len = sizeof(Dbtup::Disk_undo::Alloc) >> 2;
              Logfile_client lgman(this,
                                   c_lgman,
                                   regFragPtr->m_logfile_group_id);
              lgman.free_log_space(undo_insert_len, jamBuffer());
              return ret;
            }
            jamDebug();
            jamDataDebug(new_key.m_file_no);
            jamDataDebug(new_key.m_page_no);
            bits |= Tuple_header::DISK_REORG;
            regOperPtr->op_struct.bit_field.m_load_extra_diskpage_on_commit= 1;
            m_base_header_bits = bits;
            new_key.m_page_idx = new_size;
	    void *ptr = (void*)req_struct->m_tuple_ptr->get_disk_ref_ptr(regTabPtr);
            memcpy(ptr, &new_key, sizeof(new_key));
            DEB_REORG(("(%u) REORG set: tab(%u,%u), row(%u,%u), disk_key:"
                       " (%u,%u), new_size: %u, undo_buffer_space: %u",
                       instance(),
                       req_struct->fragPtrP->fragTableId,
                       req_struct->fragPtrP->fragmentId,
                       regOperPtr->m_tuple_location.m_page_no,
                       regOperPtr->m_tuple_location.m_page_idx,
                       new_key.m_file_no,
                       new_key.m_page_no,
                       new_size,
                       regOperPtr->m_undo_buffer_space));
          }
        }
      }
      return 0;
    }
    Ptr<Page> pagePtr = req_struct->m_varpart_page_ptr[MM];
    Var_page* pageP= (Var_page*)pagePtr.p;
    Var_part_ref *refptr= org->get_var_part_ref_ptr(regTabPtr);
    ndbassert(! (bits & Tuple_header::COPY_TUPLE));

    Local_key ref;
    refptr->copyout(&ref);
    Uint32 alloc;
    Uint32 idx = ref.m_page_idx;
    if (bits & Tuple_header::VAR_PART) {
      jamDebug();
      if (copy_bits & Tuple_header::COPY_TUPLE) {
        jamDebug();
        ndbrequire(c_page_pool.getPtr(pagePtr, ref.m_page_no));
        pageP = (Var_page *)pagePtr.p;
      }
      alloc = pageP->get_entry_len(idx);
    } else {
      jamDebug();
      alloc = 0;
    }
    Uint32 orig_size = alloc;
    if (bits & Tuple_header::MM_GROWN) {
      jamDebug();
      /* Was grown before, so must fetch real original size from last word. */
      Uint32 *old_var_part = pageP->get_ptr(idx);
      ndbassert(alloc > 0);
      orig_size = old_var_part[alloc - 1];
    }

    if (alloc) {
      jamDebug();
#ifdef VM_TRACE
      if (!pageP->get_entry_chain(idx)) ndbout << *pageP << endl;
#endif
      ndbassert(pageP->get_entry_chain(idx));
    }

    Uint32 needed = sizes[2 + MM];

    if(needed <= alloc)
    {
      jam();
      continue;
    }
    /**
     * Reallocation of the variable sized part of the row is intruding
     * on all readers from the query thread. It reorganises the rows
     * visible to the readers and it can even reorganise an entire
     * page.
     *
     * This can be solved in a number of ways. One could use some kind of
     * read-write mutex on the TUP fragment in the same fashion as the
     * protection of the table fragment.
     *
     * This kind of rearrangement happens when one grows the total size
     * of the variable sized part of the row. In addition there should
     * not be any space at the end of the page. This should be rare
     * enough such that we can simply upgrade ourselves to use an
     * exclusive fragment access during the time we perform this
     * reallocation of the variable sized part.
     *
     * An alternative approach is to never touch the varsized pages until
     * we commit the operation. This would have the advantage that we need
     * no exclusive access, it would be sufficient to lock the fragment with
     * mutexes to increase the uncommitted used space or the allocation of a
     * new variable sized part.
     *
     * There is a slight advantage in such an approach in somewhat less
     * impact on concurrency. Thus if concurrency is deemed to be something
     * required to increase one could change the approach taken here.
     *
     * However the current approach has a more efficient use of memory.
     * The other approach using uncommitted used space requires holding on
     * to the old row plus allocating space for the new row in a new page.
     * Thus we could end up holding much more memory allocated during the
     * transaction compared to the approach of instantly moving the row
     * data to a new row. The current approach has more likelihood of
     * succeeding transactions in a memory constrained environment.
     *
     * The current approach will never have more memory space allocated
     * than will be required when the transaction commits. The other
     * approach if postponing the reorganisation until Commit time could
     * lead to extra memory being held during the transaction for all
     * rows that need to move to another page.
     *
     * A potential improvement here is to avoid reorganising the page
     * here and postpone this to the Commit phase. It should be sufficient
     * to remove space from the free space in the page and this should be
     * safe to do here without extra mutexes since only reads can execute
     * in parallel with this operation.
     *
     * The extra complexity required here is to track free space allocation
     * per operation. This is required to ensure that we can return the
     * free space if the operation aborts. As usual multi-operation
     * complicates matters a bit in this case since it is only the last
     * operation that worked on the row that should track the free space
     * usage.
     */
    Uint32 add = needed - alloc;
    Local_key oldref;
    refptr->copyout(&oldref);
    /**
     * Important to check alloc == 0 first since if this is true then
     * pageP is not initialised and points to garbage.
     */
    bool require_exclusive_access =
        alloc == 0 || pageP->free_space < add ||
        !pageP->is_space_behind_entry(oldref.m_page_idx, add);
    if (require_exclusive_access) {
      jam();
      DEB_ELEM_COUNT(("(%u) realloc_var_part tab(%u,%u), var_lkey: (%u,%u),"
                      " alloc: %u, needed: %u",
                      instance(),
                      regFragPtr->fragTableId,
                      regFragPtr->fragmentId,
                      oldref.m_page_no,
                      oldref.m_page_idx,
                      alloc,
                      needed));
      c_lqh->upgrade_to_exclusive_frag_access();
      Uint32 *new_var_part = realloc_var_part(&terrorCode,
                                              regFragPtr,
                                              regTabPtr,
                                              pagePtr,
                                              refptr,
                                              alloc,
                                              needed);
      if (unlikely(new_var_part == NULL)) {
        jam();
        c_lqh->reset_old_fragment_lock_status();
        return -1;
      }
      /* Mark the tuple grown, store the original length at the end. */
      DEB_LCP(("tab(%u,%u), row_id(%u,%u), set MM_GROWN",
              req_struct->fragPtrP->fragTableId,
              req_struct->fragPtrP->fragmentId,
              regOperPtr->m_tuple_location.m_page_no,
              regOperPtr->m_tuple_location.m_page_idx));
      org->m_header_bits= bits |
                          Tuple_header::MM_GROWN |
                          Tuple_header::VAR_PART;
      m_base_header_bits = org->m_header_bits;
      new_var_part[needed - 1] = orig_size;

      /**
       * Here we can change both header bits and the reference to the varpart,
       * this means that we need to completely recalculate the checksum here.
       *
       * The source row is changed, this requires protection against readers.
       *
       * When reading we acquire the pointers to the variable parts when we
       * call prepare_read, thus it is sufficient to protect this part with
       * a mutex, we need not hold the mutex during the entire read operation.
       * It is vital to not use the row reference to the variable part after
       * releasing the mutex in query threads.
       */
      setChecksum(org, regTabPtr);
      c_lqh->downgrade_from_exclusive_frag_access();
    } else {
      jamDebug();
      /**
       * Growing the entry in the page requires not exclusive access.
       * Only LDM threads are allowed to perform Updates that will
       * change the size of pages and the free area. Thus only one
       * thread can be active on the fragment for updates. Readers can
       * can execute in parallel with this on the fragment since
       * they will only read the pointer to the varsized row part and
       * it will be ok to see both the before value and the after
       * value of the write of the index value. The index value is
       * a 32-bit value which is atomic in that the CPU will see
       * either the before value or the after value.
       */
      Uint32 *new_var_part = pageP->get_ptr(oldref.m_page_idx);
      regFragPtr->m_varWordsFree -= pageP->free_space;
      pageP->grow_entry(oldref.m_page_idx, add);
      update_free_page_list(regFragPtr, pagePtr);
      m_base_header_bits= bits |
                          Tuple_header::MM_GROWN |
                          Tuple_header::VAR_PART;
      new_var_part[needed-1]= orig_size;
    }
  }
  return 0;
}

int Dbtup::optimize_var_part(KeyReqStruct* req_struct,
                             Tuple_header* org,
                             Operationrec* regOperPtr,
                             Fragrecord* regFragPtr,
                             Tablerec* regTabPtr) {
  jam();
  Var_part_ref *refptr = org->get_var_part_ref_ptr(regTabPtr);

  Local_key ref;
  refptr->copyout(&ref);
  Uint32 idx = ref.m_page_idx;

  Ptr<Page> pagePtr;
  ndbrequire(c_page_pool.getPtr(pagePtr, ref.m_page_no));

  Var_page *pageP = (Var_page *)pagePtr.p;
  Uint32 var_part_size = pageP->get_entry_len(idx);

  /**
   * if the size of page list_index is MAX_FREE_LIST,
   * we think it as full page, then need not optimize
   */
  if (pageP->list_index != MAX_FREE_LIST) {
    jam();
    /*
     * optimize var part of tuple by moving varpart,
     * then we possibly reclaim free pages
     */
    move_var_part(regFragPtr,
                  regTabPtr,
                  pagePtr,
                  refptr,
                  var_part_size,
                  org);
  }

  return 0;
}

int
Dbtup::nr_update_gci(Uint64 fragPtrI,
                     const Local_key* key,
                     Uint32 gci,
                     bool tuple_exists)
{
  FragrecordPtr fragPtr;
  fragPtr.i= fragPtrI;
  ndbrequire(c_fragment_pool.getPtr(fragPtr));
  TablerecPtr tablePtr;
  tablePtr.i = fragPtr.p->fragTableId;
  ptrCheckGuard(tablePtr, cnoOfTablerec, tablerec);

  /**
   * GCI on the row is mandatory since many versions back.
   * During restore we have temporarily disabled this
   * flag to avoid it being set other than when done
   * with a purpose to actually set it (happens in
   * DELETE BY PAGEID and DELETE BY ROWID).
   *
   * This code is called in restore for DELETE BY
   * ROWID and PAGEID. We want to set the GCI in
   * this specific case, but not for WRITEs and
   * INSERTs, so we make this condition always
   * true.
   *
   * We don't use query threads during Copy fragment phase, thus we
   * can skip mutex protection here.
   */
  jamDebug();
  ndbrequire(!m_is_in_query_thread);
  if (tablePtr.p->m_bits & Tablerec::TR_RowGCI || true) {
    Local_key tmp = *key;
    PagePtr pagePtr;

    pagePtr.i = getRealpidCheck(fragPtr.p, tmp.m_page_no);
    if (unlikely(pagePtr.i == RNIL)) {
      jam();
      ndbassert(!tuple_exists);
      return 0;
    }

    c_page_pool.getPtr(pagePtr);

    Tuple_header *ptr =
        (Tuple_header *)((Fix_page *)pagePtr.p)->get_ptr(tmp.m_page_idx, 0);

    if (tuple_exists) {
      ndbrequire(!(ptr->m_header_bits & Tuple_header::FREE));
    } else {
      ndbrequire(ptr->m_header_bits & Tuple_header::FREE);
    }
    update_gci(fragPtr.p, tablePtr.p, ptr, gci);
  }
  return 0;
}

int
Dbtup::nr_read_pk(Uint64 fragPtrI, 
		  const Local_key* key, Uint32* dst, bool& copy)
{
  
  FragrecordPtr fragPtr;
  fragPtr.i= fragPtrI;
  ndbrequire(c_fragment_pool.getPtr(fragPtr));
  TablerecPtr tablePtr;
  tablePtr.i = fragPtr.p->fragTableId;
  ptrCheckGuard(tablePtr, cnoOfTablerec, tablerec);

  Local_key tmp = *key;

  ndbrequire(!m_is_in_query_thread);
  PagePtr pagePtr;
  /* Mutex protection only required here for query threads. */
  pagePtr.i = getRealpidCheck(fragPtr.p, tmp.m_page_no);
  if (unlikely(pagePtr.i == RNIL)) {
    jam();
    dst[0] = 0;
    return 0;
  }

  c_page_pool.getPtr(pagePtr);
  KeyReqStruct req_struct(this);
  Uint32* ptr= ((Fix_page*)pagePtr.p)->get_ptr(key->m_page_idx, 0);
  
  req_struct.m_lqh = c_lqh;
  req_struct.m_page_ptr = pagePtr;
  req_struct.m_tuple_ptr = (Tuple_header *)ptr;
  Uint32 bits = req_struct.m_tuple_ptr->m_header_bits;

  int ret = 0;
  copy = false;
  if (!(bits & Tuple_header::FREE)) {
    if (bits & Tuple_header::ALLOC) {
      OperationrecPtr opPtr;
      opPtr.i = req_struct.m_tuple_ptr->m_operation_ptr_i;
      ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(opPtr));
      ndbassert(opPtr.p->m_copy_tuple_location != nullptr);
      req_struct.m_tuple_ptr = get_copy_tuple(opPtr.p->m_copy_tuple_location);
      copy = true;
    }
    Uint32 *tab_descr = tablePtr.p->tabDescriptor;
    req_struct.check_offset[MM] = tablePtr.p->get_check_offset(MM);
    req_struct.check_offset[DD] = tablePtr.p->get_check_offset(DD);

    req_struct.attr_descr = tab_descr;

    if (tablePtr.p->need_expand()) prepare_read(&req_struct, tablePtr.p, false);

    const Uint32 *attrIds = tablePtr.p->readKeyArray;
    const Uint32 numAttrs = tablePtr.p->noOfKeyAttr;
    // read pk attributes from original tuple

    req_struct.tablePtrP = tablePtr.p;
    req_struct.fragPtrP = fragPtr.p;

    // do it
    ret = readAttributes(&req_struct, attrIds, numAttrs, dst, ZNIL);

    // done
    if (likely(ret >= 0)) {
      // remove headers
      Uint32 n = 0;
      Uint32 i = 0;
      while (n < numAttrs) {
        const AttributeHeader ah(dst[i]);
        Uint32 size = ah.getDataSize();
        ndbrequire(size != 0);
        for (Uint32 j = 0; j < size; j++) {
          dst[i + j - n] = dst[i + j + 1];
        }
        n += 1;
        i += 1 + size;
      }
      ndbrequire((int)i == ret);
      ret -= numAttrs;
    } else {
      return ret;
    }
  }

  if (tablePtr.p->m_bits & Tablerec::TR_RowGCI) {
    dst[ret] = *req_struct.m_tuple_ptr->get_mm_gci(tablePtr.p);
  } else {
    dst[ret] = 0;
  }
  return ret;
}

int
Dbtup::nr_delete(Signal* signal, Uint32 senderData,
		 Uint64 fragPtrI, const Local_key* key, Uint32 gci)
{
  FragrecordPtr fragPtr;
  fragPtr.i= fragPtrI;
  ndbrequire(c_fragment_pool.getPtr(fragPtr));
  TablerecPtr tablePtr;
  tablePtr.i = fragPtr.p->fragTableId;
  ptrCheckGuard(tablePtr, cnoOfTablerec, tablerec);

  ndbrequire(!m_is_in_query_thread);
  /**
   * We execute this function as part of RESTORE operations and as part
   * of COPY fragment handling in the starting node. Thus there is no
   * concurrency from query threads that will bother us at this point in
   * time.
   */
  Local_key tmp = *key;
  tmp.m_page_no = getRealpid(fragPtr.p, tmp.m_page_no);

  PagePtr pagePtr;
  Tuple_header *ptr = (Tuple_header *)get_ptr(&pagePtr, &tmp, tablePtr.p);

  if (!tablePtr.p->tuxCustomTriggers.isEmpty()) {
    jam();
    TuxMaintReq *req = (TuxMaintReq *)signal->getDataPtrSend();
    req->tableId = fragPtr.p->fragTableId;
    req->fragId = fragPtr.p->fragmentId;
    req->pageId = tmp.m_page_no;
    req->pageIndex = tmp.m_page_idx;
    req->tupVersion = ptr->get_tuple_version();
    req->opInfo = TuxMaintReq::OpRemove;
    removeTuxEntries(signal, tablePtr.p);
  }

  Local_key disk;
  memcpy(&disk, ptr->get_disk_ref_ptr(tablePtr.p), sizeof(disk));

  Uint32 lcpScan_ptr_i = fragPtr.p->m_lcp_scan_op;
  Uint32 bits = ptr->m_header_bits;
  if (lcpScan_ptr_i != RNIL &&
      !(bits & (Tuple_header::LCP_SKIP | Tuple_header::LCP_DELETE |
                Tuple_header::ALLOC))) {
    /**
     * We are performing a node restart currently, at the same time we
     * are also running a LCP on the fragment. This can happen when the
     * UNDO log level becomes too high. In this case we can start a full
     * local LCP during the copy fragment process.
     *
     * Since we are about to delete a row now, we have to ensure that the
     * lcp keep list gets this row before we delete it. This will ensure
     * that the LCP becomes a consistent LCP based on what was there at the
     * start of the LCP.
     */
    jam();
    ScanOpPtr scanOp;
    scanOp.i = lcpScan_ptr_i;
    ndbrequire(c_scanOpPool.getValidPtr(scanOp));
    if (is_rowid_in_remaining_lcp_set(pagePtr.p, fragPtr.p, *key, *scanOp.p,
                                      0)) {
      KeyReqStruct req_struct(jamBuffer(), KRS_PREPARE);
      req_struct.m_lqh = c_lqh;
      req_struct.fragPtrP = fragPtr.p;
      Operationrec oprec;
      Tuple_header *copy;
      if ((copy = alloc_copy_tuple
         (tablePtr.p, &oprec.m_copy_tuple_location, false)) ==
          0) {
        /**
         * We failed to allocate the copy record, this is a critical error,
         * we will fail with an error message instruction to increase
         * SharedGlobalMemory.
         */
        char buf[256];
        BaseString::snprintf(buf, sizeof(buf),
                             "Out of memory when allocating copy tuple for"
                             " LCP keep list, increase SharedGlobalMemory");
        progError(__LINE__, NDBD_EXIT_RESOURCE_ALLOC_ERROR, buf);
      }

      DEB_COPY_TUPLE(("(%u) alloc_copy_tuple: 0x%p, line: %u",
        instance(), oprec.m_copy_tuple_location, __LINE__));

      req_struct.m_tuple_ptr = ptr;
      oprec.m_tuple_location = tmp;
      oprec.op_type = ZDELETE;
      DEB_LCP_SKIP_DELETE(
          ("(%u)nr_delete: tab(%u,%u), row(%u,%u),"
           " handle_lcp_keep_commit"
           ", set LCP_SKIP, bits: %x",
           instance(), fragPtr.p->fragTableId, fragPtr.p->fragmentId,
           key->m_page_no, key->m_page_idx, bits));
      handle_lcp_keep_commit(key,
                             &req_struct,
                             &oprec,
                             fragPtr.p,
                             tablePtr.p);
      jamDebug();
      acquire_frag_mutex(fragPtr.p, key->m_page_no, jamBuffer());
      ptr->m_header_bits |= Tuple_header::LCP_SKIP;
      /**
       * Updating checksum of stored row requires protection against
       * readers in other threads.
       */
      updateChecksum(ptr, tablePtr.p, bits, ptr->m_header_bits);
      release_frag_mutex(fragPtr.p, key->m_page_no, jamBuffer());
    }
  }

  /**
   * A row is deleted as part of Copy fragment or Restore
   * We need to keep track of the row count also during restore.
   * We increment number of changed rows, for restore this variable
   * will be cleared after completing the restore, but it is
   * important to count it while performing a COPY fragment
   * operation.
   */
  fragPtr.p->m_row_count--;
  fragPtr.p->m_lcp_changed_rows++;

  DEB_DELETE_NR((
      "(%u)nr_delete, tab(%u,%u) row(%u,%u), gci: %u"
      ", row_count: %llu",
      instance(), fragPtr.p->fragTableId, fragPtr.p->fragmentId, key->m_page_no,
      key->m_page_idx, *ptr->get_mm_gci(tablePtr.p), fragPtr.p->m_row_count));

  /**
   * No query threads active when restore and copy fragment process
   * is active. Thus no need to lock mutex here.
   */
  if (tablePtr.p->m_attributes[MM].m_no_of_varsize +
      tablePtr.p->m_attributes[MM].m_no_of_dynamic) {
    jam();
    free_var_rec(fragPtr.p, tablePtr.p, &tmp, pagePtr);
  } else {
    jam();
    free_fix_rec(fragPtr.p, tablePtr.p, &tmp, (Fix_page*)pagePtr.p);
  }

  if (tablePtr.p->m_no_of_disk_attributes) {
    jam();
    Ptr<GlobalPage> diskPagePtr;
    int res;
    Uint32 sz;
    Uint32 page_idx;
    Uint32 entry_len;
    Uint32 size_len;

    /**
     * 1) get page
     * 2) alloc log buffer
     * 3) get log buffer
     * 4) delete tuple
     */
    Page_cache_client::Request preq;
    preq.m_page = disk;
    preq.m_table_id = fragPtr.p->fragTableId;
    preq.m_fragment_id = fragPtr.p->fragmentId;
    preq.m_callback.m_callbackData = senderData;
    preq.m_callback.m_callbackFunction =
      safe_cast(&Dbtup::nr_delete_page_callback);
    int flags = Page_cache_client::COMMIT_REQ;

    DEB_DISK(("(%u), nr_delete, row(%u,%u), disk_row(%u,%u,%u)",
      instance(),
      key->m_page_no,
      key->m_page_idx,
      preq.m_page.m_file_no,
      preq.m_page.m_page_no,
      preq.m_page.m_page_idx));
#ifdef ERROR_INSERT
      if (ERROR_INSERTED(4023) || ERROR_INSERTED(4024)) {
        int rnd = rand() % 100;
        int slp = 0;
        if (ERROR_INSERTED(4024)) {
          slp = 3000;
        } else if (rnd > 90) {
          slp = 3000;
        } else if (rnd > 70) {
          slp = 100;
        }

        g_eventLogger->info("rnd: %d slp: %d", rnd, slp);

        if (slp) {
          flags |= Page_cache_client::DELAY_REQ;
          const NDB_TICKS now = NdbTick_getCurrentTicks();
          preq.m_delay_until_time = NdbTick_AddMilliseconds(now, (Uint64)slp);
        }
      }
#endif
      {
        Page_cache_client pgman(this, c_pgman);
        res = pgman.get_page(signal, preq, flags);
        diskPagePtr = pgman.m_ptr;
        if (res == 0) {
          goto timeslice;
        }
        /**
         * We are processing node recovery and need to process a disk
         * data page, if this fails we cannot proceed with node recovery.
         */
        ndbrequire(res > 0);

      if ((tablePtr.p->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
      {
        jam();
        sz = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) + 
          tablePtr.p->m_offsets[DD].m_fix_header_size - 1;
      }
      else
      {
        jam();
        page_idx = disk.m_page_idx;
        entry_len = ((Var_page*)diskPagePtr.p)->get_entry_len(page_idx);
        size_len = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2);
        sz = size_len + (entry_len - 1);

      }
      D("Logfile_client - nr_delete");
      {
        Logfile_client lgman(this, c_lgman, fragPtr.p->m_logfile_group_id);
        res = lgman.alloc_log_space(sz, false, false, jamBuffer());
        ndbrequire(res == 0);
        /* Complete work on LGMAN before setting page to dirty */
        CallbackPtr cptr;
        cptr.m_callbackIndex = NR_DELETE_LOG_BUFFER_CALLBACK;
        cptr.m_callbackData = senderData;
        res= lgman.get_log_buffer(signal, sz, &cptr);
      }
    } // Unlock the LGMAN lock

    PagePtr disk_page((Tup_page*)diskPagePtr.p, diskPagePtr.i);
    disk_page_set_dirty(disk_page, fragPtr.p);

    switch (res) {
      case 0:
        signal->theData[2] = disk_page.i;
        goto timeslice;
      case -1:
        ndbrequire("NOT YET IMPLEMENTED" == 0);
        break;
    }
    disk_page_free(signal,
                   tablePtr.p,
                   fragPtr.p,
                   disk,
                   *(PagePtr *)&disk_page,
                   gci,
                   key,
                   sz);
    return 0;
  }

  return 0;

timeslice:
  memcpy(signal->theData, &disk, sizeof(disk));
  return 1;
}

void Dbtup::nr_delete_page_callback(Signal *signal, Uint32 userpointer,
                                    Uint32 page_id)  // unused
{
  Ptr<GlobalPage> gpage;
  ndbrequire(m_global_page_pool.getPtr(gpage, page_id));
  PagePtr pagePtr((Tup_page*)gpage.p, gpage.i);
  Dblqh::Nr_op_info op;
  op.m_ptr_i = userpointer;
  jam();
  jamData(userpointer);
  op.m_disk_ref.m_page_no = pagePtr.p->m_page_no;
  op.m_disk_ref.m_file_no = pagePtr.p->m_file_no;
  c_lqh->get_nr_op_info(&op, page_id);

#ifdef DEBUG_DISK
  Ptr<GlobalPage> diskPagePtr;
  diskPagePtr.i = page_id;
  ndbrequire(m_global_page_pool.getPtr(diskPagePtr, page_id));
  g_eventLogger->info("(%u) nr_delete_page_callback, disk_row(%u,%u)",
    instance(),
    ((Tup_varsize_page*)diskPagePtr.p)->m_file_no,
    ((Tup_varsize_page*)diskPagePtr.p)->m_page_no);
#endif

  FragrecordPtr fragPtr;
  fragPtr.i= op.m_tup_frag_ptr_i;
  ndbrequire(c_fragment_pool.getPtr(fragPtr));
  disk_page_set_dirty(pagePtr, fragPtr.p);

  Ptr<Tablerec> tablePtr;
  tablePtr.i = fragPtr.p->fragTableId;
  ptrCheckGuard(tablePtr, cnoOfTablerec, tablerec);
  
  Uint32 sz;
  if ((tablePtr.p->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
  {
    sz = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) + 
      tablePtr.p->m_offsets[DD].m_fix_header_size - 1;
  }
  else
  {
    Uint32 page_idx = op.m_disk_ref.m_page_idx;
    Uint32 entry_len = ((Var_page*)pagePtr.p)->get_entry_len(page_idx);
    sz = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) +
           (entry_len - 1);
  }
  int res;
  {
    Logfile_client lgman(this, c_lgman, fragPtr.p->m_logfile_group_id);
    res = lgman.alloc_log_space(sz, false, false, jamBuffer());
    ndbrequire(res == 0);

    CallbackPtr cb;
    cb.m_callbackData = userpointer;
    cb.m_callbackIndex = NR_DELETE_LOG_BUFFER_CALLBACK;
    D("Logfile_client - nr_delete_page_callback");
    res= lgman.get_log_buffer(signal, sz, &cb);
  }
  switch (res) {
    case 0:
      jam();
      return;
    case -1:
      ndbrequire("NOT YET IMPLEMENTED" == 0);
      break;
  }
  jam();
  disk_page_free(signal,
                 tablePtr.p,
                 fragPtr.p,
                 op.m_disk_ref,
                 pagePtr,
                 op.m_gci_hi,
                 &op.m_row_id,
                 sz);

  c_lqh->nr_delete_complete(signal, &op);
  return;
}

void Dbtup::nr_delete_log_buffer_callback(Signal *signal, Uint32 userpointer,
                                          Uint32 unused) {
  Dblqh::Nr_op_info op;
  op.m_ptr_i = userpointer;
  jam();
  jamData(userpointer);
  c_lqh->get_nr_op_info(&op, RNIL);
  
  FragrecordPtr fragPtr;
  fragPtr.i= op.m_tup_frag_ptr_i;
  ndbrequire(c_fragment_pool.getPtr(fragPtr));

  Ptr<Tablerec> tablePtr;
  tablePtr.i = fragPtr.p->fragTableId;
  ptrCheckGuard(tablePtr, cnoOfTablerec, tablerec);

  Ptr<GlobalPage> gpage;
  ndbrequire(m_global_page_pool.getPtr(gpage, op.m_page_id));
  PagePtr pagePtr((Tup_page *)gpage.p, gpage.i);

  Uint32 sz;
  if ((tablePtr.p->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
  {
    jam();
    sz = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) + 
      tablePtr.p->m_offsets[DD].m_fix_header_size - 1;
  }
  else
  {
    jam();
    Uint32 page_idx = op.m_disk_ref.m_page_idx;
    Uint32 entry_len = ((Var_page*)pagePtr.p)->get_entry_len(page_idx);
    sz = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) +
           (entry_len - 1);
  }

  /**
   * reset page no
   */
  disk_page_free(signal,
                 tablePtr.p,
                 fragPtr.p,
                 op.m_disk_ref,
                 pagePtr,
                 op.m_gci_hi,
                 &op.m_row_id,
                 sz);
  
  c_lqh->nr_delete_complete(signal, &op);
}
