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
#include <cassert>
#include <ndb_types.h>
#include <ndb_constants.h>

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

#define PUSHDOWN_AGGREGATION_VERSION 3

/*
 * kOpEmbeddedInterp writes its control result through interpreter output
 * slot 0.  Normal CASE lowering writes a forward skip offset.  This
 * reserved value tells the aggregation interpreter to stop processing the
 * current row's aggregation program.  The embedded normal interpreter still
 * owns all comparison and branch semantics.
 */
#define AGG_EMBEDDED_INTERP_STOP_PROGRAM 0xFFFF

/**
 * CTE definitions marker in the KeyInfo agg section (Section 2).
 * Separates the main aggregation program from CTE definition data.
 * DBTC checks for this marker after parsing the main agg program.
 */
#define CTE_DEFS_MARKER 0xCDE00000

/*
 * Marker for the optional join aggregation column metadata container appended
 * after the main aggregation program and optional CTE definitions in the
 * API-to-DBTC aggregation payload. DBTC splits this container into the
 * per-aggregation ColumnMetaSectionNum section on JOIN_AGG_SETUP_REQ.
 */
#define JOIN_AGG_META_MARKER 0xA66D0000
#define JOIN_AGG_META_VERSION 1
#define JOIN_AGG_META_ENTRY_WORDS 12

#define JOIN_AGG_META_KIND_MAIN 0
#define JOIN_AGG_META_KIND_CTE 1

#define JOIN_AGG_META_SOURCE_LOCAL_COLUMN 0
#define JOIN_AGG_META_SOURCE_LINKED_COLUMN 1
#define JOIN_AGG_META_SOURCE_CTE_COLUMN 2

#define JOIN_AGG_META_FLAG_UNSIGNED 1
#define JOIN_AGG_META_FLAG_NULLABLE 2
#define JOIN_AGG_META_FLAG_GROUP_BY 4
#define JOIN_AGG_META_FLAG_LOAD_COLUMN 8
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
  kOpSetRegNull,      // Mark register NULL, preserving its value type

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

/**
 * Shared numeric partial-aggregate MERGE helpers — used by the kernel
 * cross-thread/cross-node merges (JoinAggInterpreter mergeAccumulators)
 * and the API-side result merge (NdbAggregator::ProcessRes), which
 * cannot link the kernel implementations.
 *
 * Partial accumulators from different LDM threads / data nodes may
 * legitimately disagree in BIGINT signedness — and, for CASE arms of
 * mixed numeric types, in BIGINT-vs-DOUBLE — because the per-row
 * kernels (AggInterpreterBase::Sum / Max / Min) type each accumulator
 * from the values it actually saw: e.g. SUM(CASE WHEN c THEN
 * bigunsigned_col ELSE 0 END) yields signed partials on workers whose
 * rows all took the signed-constant arm and unsigned partials
 * elsewhere (the big-06 finding — the old ProcessRes merge asserted
 * on exactly this).  These helpers apply the same promotion rules as
 * the per-row kernels: SUM's merged signedness is the OR of the
 * contributions, MAX/MIN compare in the value domain across all four
 * signedness arms, and any DOUBLE contribution promotes the slot to
 * DOUBLE.
 *
 * Caller contract: src.type and dst->type are NDB_TYPE_BIGINT or
 * NDB_TYPE_DOUBLE (never UNDEFINED or a string type) and both slots
 * are non-null — string, NULL and first-contribution handling stay at
 * the call sites.  COUNT merges by unsigned addition (the per-row
 * Count() increments by one per row and cannot be reused for
 * merging).
 *
 * Distributed SUM merging deliberately retains its existing behavior:
 * BIGINT uses modular 64-bit addition and DOUBLE follows IEEE arithmetic.
 * Overflow reporting is not introduced here because neither of the existing
 * distributed merge call chains propagates it reliably.  In particular, a
 * failed merge must never silently omit one partial result.
 */
static inline double aggSlotAsDouble(const AggResItem& v) {
  if (v.type == NDB_TYPE_DOUBLE) return v.value.val_double;
  return v.is_unsigned ? static_cast<double>(v.value.val_uint64)
                       : static_cast<double>(v.value.val_int64);
}

static inline void aggMergeSum(AggResItem* dst, const AggResItem& src) {
  assert(!src.is_null && !dst->is_null);

  /*
   * A signed BIGINT zero is both a value and type identity.  This is the
   * common big-06 case: workers that only execute ELSE 0 contribute a
   * signed-zero partial.  Avoid the general mixed-signedness merge entirely,
   * regardless of which partial arrived first.
   *
   * Do not apply this shortcut to unsigned zero: its unsignedness affects the
   * result domain under the existing signedness-OR rule.
   */
  if (src.type == NDB_TYPE_BIGINT &&
      dst->type == NDB_TYPE_BIGINT) {
    if (!src.is_unsigned && src.value.val_int64 == 0) {
      return;
    }
    if (!dst->is_unsigned && dst->value.val_int64 == 0) {
      *dst = src;
      return;
    }
  }

  if (src.type == NDB_TYPE_DOUBLE || dst->type == NDB_TYPE_DOUBLE) {
    dst->value.val_double =
        aggSlotAsDouble(*dst) + aggSlotAsDouble(src);
    dst->type = NDB_TYPE_DOUBLE;
    dst->is_unsigned = false;
    return;
  }

  assert(src.type == NDB_TYPE_BIGINT &&
         dst->type == NDB_TYPE_BIGINT);
  dst->value.val_uint64 += src.value.val_uint64;
  dst->is_unsigned = src.is_unsigned || dst->is_unsigned;
}

static inline void aggMergeMax(AggResItem* dst, const AggResItem& src) {
  assert(!src.is_null && !dst->is_null);
  if (src.type == NDB_TYPE_DOUBLE || dst->type == NDB_TYPE_DOUBLE) {
    const double a = aggSlotAsDouble(src);
    const double b = aggSlotAsDouble(*dst);
    dst->type = NDB_TYPE_DOUBLE;
    dst->is_unsigned = false;
    dst->value.val_double = a > b ? a : b;
    return;
  }
  if (!src.is_unsigned && !dst->is_unsigned) {
    if (src.value.val_int64 > dst->value.val_int64)
      dst->value.val_int64 = src.value.val_int64;
  } else if (src.is_unsigned && dst->is_unsigned) {
    if (src.value.val_uint64 > dst->value.val_uint64)
      dst->value.val_uint64 = src.value.val_uint64;
  } else if (src.is_unsigned && !dst->is_unsigned) {
    if (dst->value.val_int64 < 0 ||
        src.value.val_uint64 > static_cast<Uint64>(dst->value.val_int64))
      dst->value.val_uint64 = src.value.val_uint64;
    dst->is_unsigned = true;
  } else {  // src signed, dst unsigned
    if (src.value.val_int64 >= 0 &&
        static_cast<Uint64>(src.value.val_int64) > dst->value.val_uint64)
      dst->value.val_uint64 = static_cast<Uint64>(src.value.val_int64);
    /* src negative: the unsigned dst is already larger. */
  }
}

static inline void aggMergeMin(AggResItem* dst, const AggResItem& src) {
  assert(!src.is_null && !dst->is_null);
  if (src.type == NDB_TYPE_DOUBLE || dst->type == NDB_TYPE_DOUBLE) {
    const double a = aggSlotAsDouble(src);
    const double b = aggSlotAsDouble(*dst);
    dst->type = NDB_TYPE_DOUBLE;
    dst->is_unsigned = false;
    dst->value.val_double = a < b ? a : b;
    return;
  }
  if (!src.is_unsigned && !dst->is_unsigned) {
    if (src.value.val_int64 < dst->value.val_int64)
      dst->value.val_int64 = src.value.val_int64;
  } else if (src.is_unsigned && dst->is_unsigned) {
    if (src.value.val_uint64 < dst->value.val_uint64)
      dst->value.val_uint64 = src.value.val_uint64;
  } else if (src.is_unsigned && !dst->is_unsigned) {
    if (dst->value.val_int64 >= 0) {
      if (src.value.val_uint64 < static_cast<Uint64>(dst->value.val_int64))
        dst->value.val_uint64 = src.value.val_uint64;
      dst->is_unsigned = true;
    }
    /* dst negative: the signed dst is already smaller. */
  } else {  // src signed, dst unsigned
    if (src.value.val_int64 < 0) {
      dst->value.val_int64 = src.value.val_int64;
      dst->is_unsigned = false;
    } else if (static_cast<Uint64>(src.value.val_int64) <
               dst->value.val_uint64) {
      dst->value.val_uint64 = static_cast<Uint64>(src.value.val_int64);
    }
  }
}

static inline void aggMergeNumericSlot(AggResItem* dst,
                                       const AggResItem& src,
                                       Uint32 agg_op) {
  switch (agg_op) {
    case kOpCount:
      assert(src.type == NDB_TYPE_BIGINT &&
             dst->type == NDB_TYPE_BIGINT);
      assert(src.is_unsigned && dst->is_unsigned);
      dst->value.val_uint64 += src.value.val_uint64;
      return;
    case kOpSum:
    case kOpSumBigint:
    case kOpSumDouble:
      aggMergeSum(dst, src);
      return;
    case kOpMax:
    case kOpMaxBigint:
    case kOpMaxDouble:
      aggMergeMax(dst, src);
      return;
    case kOpMin:
    case kOpMinBigint:
    case kOpMinDouble:
      aggMergeMin(dst, src);
      return;
    default:
      assert(false);
      return;
  }
}

#endif  // NDBAGGREGATIONCOMMON_H_
