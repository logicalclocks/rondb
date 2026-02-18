/*
 * Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.
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

struct CHARSET_INFO;
/*
 * MOZ
 * Turn off the PA_CHECK to stop validate aggregation
 * network package on both data node and API node
 * in DEBUG binary
 */
#define PA_CHECK 1

#define MAX_AGG_RESULT_BATCH_BYTES 8192
#define DEF_AGG_RESULT_BATCH_BYTES 4096
#define MAX_AGG_N_GROUPBY_COLS 128
#define MAX_AGG_N_RESULTS 256
#define MAX_AGG_PROGRAM_WORD_SIZE 1024
/*
 * VS related
 * Currently, we allocate only one page (32 KB) to store
 * the vector search (VS) program. This limits the maximum
 * program word size to 8192, which in turn determines the
 * upper limit of the vector dimension.
 * Therefore, we set the dimension slightly below 8192 — currently 8100.
 */
#define MAX_VEC_SEARCH_PROGRAM_WORD_SIZE 8192
#define MAX_VEC_DIMS 8100
#define MAX_CANDIDATE_SEGMENTS 256

/* Bit 15 in column ID: column is from parent table in a pushed join.
 * Used with GroupBy() and LoadColumn() to reference linked parent columns. */
#define AGG_LINKED_COL_FLAG 0x8000

/* GROUP BY column word encoding: (col_id << 16) | (col_type & 0xFF)
 * Lower 8 bits store the NdbDictionary::Column::Type enum value so that
 * linked GROUP BY columns are self-describing (the parent table's column
 * type is not otherwise available on the API result side). */
#define AGG_GB_COL_TYPE_MASK 0xFF

#define PUSHDOWN_AGGREGATION_VERSION 2
enum InterpreterOp {
  kOpUnknown = 0,
  kOpPlus,
  kOpMinus,
  kOpMul,
  kOpDiv,
  kOpDivInt,
  kOpMod,
  kOpLoadCol,
  kOpLoadConst,
  kOpMov,
  kOpSum,
  kOpMax,
  kOpMin,
  kOpCount,

  // Type-specific aggregations
  kOpSumBigint,       // Sum for BIGINT (handles signed/unsigned dynamically)
  kOpSumDouble,
  kOpMaxBigint,
  kOpMaxDouble,
  kOpMinBigint,
  kOpMinDouble,

  // Type-specific arithmetic operations
  kOpPlusBigint,      // Plus for BIGINT (handles signed/unsigned dynamically)
  kOpPlusDouble,
  kOpMinusBigint,
  kOpMinusDouble,
  kOpMulBigint,
  kOpMulDouble,
  kOpDivDouble,       // Floating point division (result is always double)
  kOpDivIntBigint,    // Integer division for BIGINT

  // Embedded interpreter support for CASE expressions
  kOpEmbeddedInterp,  // Invoke embedded old-interpreter code block
  kOpSkip,            // Unconditional forward skip in aggregation program

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
  Int64 val_int64;
  Uint64 val_uint64;
  double val_double;
  void* val_ptr;
};

typedef Uint32 DataType;
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
  Uint32 len;
};

struct GBColMeta {
  Uint32 typeId;
  const CHARSET_INFO *cs;
};

struct GBCmpContext {
  Uint32 n_cols;
  bool all_binary_cmp;
  GBColMeta col_meta[MAX_AGG_N_GROUPBY_COLS];
};

struct GBHashEntryCmp {
  GBCmpContext *ctx;

  GBHashEntryCmp() : ctx(nullptr) {}
  explicit GBHashEntryCmp(GBCmpContext *c) : ctx(c) {}

  bool operator() (const GBHashEntry& n1, const GBHashEntry& n2) const;
};

#endif  // NDBAGGREGATIONCOMMON_H_
