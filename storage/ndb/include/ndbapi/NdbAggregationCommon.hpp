/*
 * Copyright [2024] <Copyright Hopsworks AB>
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.

 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#ifndef NDBAGGREGATIONCOMMON_H_
#define NDBAGGREGATIONCOMMON_H_
#include <cstring>
#include <cstdint>
/*
 * MOZ
 * Turn off the MOZ_AGG_CHECK to stop validate aggregation
 * network package on both data node and API node
 * in DEBUG binary
 */
#define MOZ_AGG_CHECK 1

#define MAX_AGG_RESULT_BATCH_BYTES 8192
#define DEF_AGG_RESULT_BATCH_BYTES 4096
#define MAX_AGG_N_GROUPBY_COLS 128
#define MAX_AGG_N_RESULTS 256
#define MAX_AGG_PROGRAM_WORD_SIZE 1024
enum InterpreterOp {
  kOpUnknown = 0,
  kOpPlus,
  kOpMinus,
  kOpMul,
  kOpDiv,
  kOpMod,
  kOpLoadCol,
  kOpLoadConst,
  kOpMov,
  kOpSum,
  kOpMax,
  kOpMin,
  kOpCount,
  kOpTotal
};

enum InterpreterRegisters {
  kReg1 = 0,
  kReg2,
  kReg3,
  kReg4,
  kReg5,
  kReg6,
  kReg7,
  kReg8,
  kRegTotal
};

union DataValue {
  int64_t val_int64;
  uint64_t val_uint64;
  double val_double;
  void* val_ptr;
};

typedef uint32_t DataType;
struct Register {
  DataType type;
  DataValue value;
  bool is_unsigned;
  bool is_null;
};

typedef Register AggResItem;

struct GBColInfo {
  DataType type;
  bool is_unsigned;
};

struct GBHashEntry {
  char *ptr;
  uint32_t len;
};

struct GBHashEntryCmp {
  bool operator() (const GBHashEntry& n1, const GBHashEntry& n2) const {
    uint32_t len = n1.len > n2.len ?
                    n2.len : n1.len;

    int ret = memcmp(n1.ptr, n2.ptr, len);
    if (ret == 0) {
      return n1.len < n2.len;
    } else {
      return ret < 0;
    }
  }
};


#endif  // NDBAGGREGATIONCOMMON_H_
