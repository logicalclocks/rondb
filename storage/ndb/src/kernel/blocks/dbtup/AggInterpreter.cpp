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

#include <cstdint>
#include <cstring>
#include <utility>

#define DBTUP_C
#include "signaldata/TransIdAI.hpp"
#include "include/my_byteorder.h"
#include "AggInterpreter.hpp"
#include "InterpreterCommonOp.hpp"
#include "decimal.h"
#include "Dbtup.hpp"
#include <NdbSqlUtil.hpp>

#include <simsimd/simsimd.h>

Uint32 AggInterpreter::g_buf_len_ = READ_BUF_WORD_SIZE;
Uint32 AggInterpreter::g_result_header_size_ = 3 * sizeof(Uint32);
Uint32 AggInterpreter::g_result_header_size_per_group_ = sizeof(Uint32);

/*
 * VS related
 * Since we allocate one page (32 KB) to accommodate the vector search
 * program — with a maximum size of MAX_VEC_SEARCH_PROGRAM_WORD_SIZE (8192 words) —
 * this effectively limits the maximum supported vector dimension.
 *
 * We also allocate a 1-page (32 KB) buffer for reading vector column values.
 * This buffer has a capacity of 8192 words, which is slightly larger than
 * the size of a vector with the current maximum dimension (MAX_VEC_DIMS = 8100).
 */
Uint32 AggInterpreter::g_vec_buf_len_ = MAX_VEC_SEARCH_PROGRAM_WORD_SIZE; /* float */

bool
GBHashEntryCmp::operator()(const GBHashEntry &n1,
                           const GBHashEntry &n2) const {
  if (ctx == nullptr || ctx->n_cols == 0 || ctx->all_binary_cmp) {
    /* Binary comparison: safe when all group-by columns are
       non-charset-aware, since binary identity equals semantic identity. */
    Uint32 len = n1.len < n2.len ? n1.len : n2.len;
    int ret = memcmp(n1.ptr, n2.ptr, len);
    if (ret == 0) {
      return n1.len < n2.len;
    }
    return ret < 0;
  }

  const char *p1 = n1.ptr;
  const char *p2 = n2.ptr;
  const char *end1 = n1.ptr + n1.len;
  const char *end2 = n2.ptr + n2.len;

  for (Uint32 i = 0; i < ctx->n_cols; i++) {
    assert(p1 + sizeof(Uint32) <= end1);
    assert(p2 + sizeof(Uint32) <= end2);
    const AttributeHeader ah1(*(const Uint32 *)p1);
    const AttributeHeader ah2(*(const Uint32 *)p2);

    bool null1 = ah1.isNULL();
    bool null2 = ah2.isNULL();
    if (null1 && null2) {
      p1 += sizeof(Uint32);
      p2 += sizeof(Uint32);
      continue;
    }
    if (null1) return true;   /* NULL < non-NULL */
    if (null2) return false;

    const char *data1 = p1 + sizeof(Uint32);
    const char *data2 = p2 + sizeof(Uint32);
    Uint32 byteSize1 = ah1.getByteSize();
    Uint32 byteSize2 = ah2.getByteSize();

    int ret = NdbSqlUtil::getType(ctx->col_meta[i].typeId).m_cmp(
                ctx->col_meta[i].cs, data1, byteSize1, data2, byteSize2);
    if (ret != 0) {
      return ret < 0;
    }

    /* Advance past header + word-aligned data */
    p1 += sizeof(Uint32) + ah1.getDataSize() * sizeof(Uint32);
    p2 += sizeof(Uint32) + ah2.getDataSize() * sizeof(Uint32);
  }
  return false;  /* All columns equal */
}

/*
 * PA related
 * Turn on the DEBUG_PA_INTERP
 * to trace AggInterpreter on partition DEBUG_PA_INTERP_PART_ID
 */
#undef DEBUG_PA_INTERP
// #define DEBUG_PA_INTERP 1
#define DEBUG_PA_INTERP_PART_ID 0
#ifdef DEBUG_PA_INTERP
#define PA_INTERP_TRACE(part_id, format, ...) \
  do {\
    if ((part_id == DEBUG_PA_INTERP_PART_ID)) {\
      g_eventLogger->info("[PA_INTERP_TRACE] " format, ##__VA_ARGS__); \
    }\
  } while (0)
#else
#define PA_INTERP_TRACE(part_id, format, ...) {}
#endif // DEBUG_PA_INTERP

/*
 * VS related
 * Turn on the DEBUG_VS_INTERP
 * to trace AggInterpreter on partition DEBUG_VS_INTERP_PART_ID
 */
#undef DEBUG_VS_INTERP
// #define DEBUG_VS_INTERP 1
#define DEBUG_VS_INTERP_TABLE_ID 17
#define DEBUG_VS_INTERP_PART_ID 0
#ifdef DEBUG_VS_INTERP
#define VS_INTERP_TRACE(table_id, part_id, format, ...) \
  do {\
    if ((table_id == DEBUG_VS_INTERP_TABLE_ID) && \
        (part_id == DEBUG_VS_INTERP_PART_ID)) {\
      g_eventLogger->info("[VS_INTERP_TRACE] " format, ##__VA_ARGS__); \
    }\
  } while (0)
#else
#define VS_INTERP_TRACE(table_id, part_id, format, ...) {}
#endif // DEBUG_VS_INTERP

bool AggInterpreter::Init(const Uint32* prog) {
  if (inited_) {
    return true;
  }

  /* 0. Prepare the buffer and copy the program */
#ifdef PA_MALLOC
  // TODO (Zhao)
  // VS related
	assert(prog_len_ <= MAX_VEC_SEARCH_PROGRAM_WORD_SIZE);
  if (prog_len_ <= MAX_AGG_PROGRAM_WORD_SIZE) {
    /*
		 * Use inline prog_buf_ for aggregation or
     * small-dimension vector search queries.
     */
    prog_ = prog_buf_;
  } else {
    /* Use external buf for large-dimension vector search queries. */
    void* page_ptr = lc_ndbd_pool_malloc(32 * 1024, RG_QUERY_MEMORY,
        thread_id_, false);
    if (page_ptr == nullptr) {
      g_eventLogger->error("Alloc mem for pushdown vector search interpreter failed");
      return false;
    }
    ext_prog_buf_ = static_cast<Uint32*>(page_ptr);
    prog_ = ext_prog_buf_;
  }
  alloc_len_ = 0;
#else
  prog_ = new Uint32[prog_len];
#endif // PA_MALLOC
  memcpy(prog_, prog, prog_len_ * sizeof(Uint32));
  memset(buf_, 0, READ_BUF_WORD_SIZE * sizeof(Uint32));
  memset(decimal_buf_, 0, sizeof(Int32) * DECIMAL_BUFF_LENGTH);
  decimal_.buf = decimal_buf_;
  decimal_.len = DECIMAL_BUFF_LENGTH;


  Uint32 value = 0;
  /*
   * 1. Double check the magic num and  total length of program.
   */
  value = prog_[cur_pos_++];
  assert(((value & 0xFFFF0000) >> 16) == 0x0721);
  assert((value & 0xFFFF) == prog_len_);

  /*
   * 2. Get num of columns for group by and num of aggregation results;
   */
  value = prog_[cur_pos_++];
  n_gb_cols_ = (value >> 16) & 0xFFFF;
  n_agg_results_ = value & 0xFFFF;

  Uint32 version = prog_[cur_pos_++];
  if (version > PUSHDOWN_AGGREGATION_VERSION) {
    g_eventLogger->warning("Pushdown aggregation program version(%u) is "
                           "not compatible with "
                           "the version (%u) on data node",
                           version, PUSHDOWN_AGGREGATION_VERSION);
    /*
     * Return with inited_ = false, and
     * ProcessRec() will handle this incompatible issue.
     */
    return true;
  }

  if (prog_[cur_pos_] & 0x80000000) {
    vec_search_ = true;
    assert((prog_[cur_pos_] & 0x7FFFFFFF) == 0);
    cur_pos_++;
  } else {
    assert(vec_search_ == false);
    // Skip the next 5 reserved Uint32 elements
    assert(prog_[cur_pos_] == 0);
    cur_pos_ += 5;
  }

  if (vec_search_) {
    value = prog_[cur_pos_++];
    vec_type_ = (value >> 24) & 0xFF;
    vec_metric_ = (value >> 16) & 0xFF;
    vec_dims_ = value & 0xFFFF;

    value = prog_[cur_pos_++];
    vec_top_n_ = (value) & 0xFFFF;
    vec_col_idx_ = (value >> 16) & 0xFFFF;
    value = prog_[cur_pos_++];
    vec_size_in_bytes_ = (value & 0xFFFFFFFF);
    // g_eventLogger->info("frag_id: %lld, type: %u, metric: %u, dims: %u, vec_col_idx: %u, vec_top_n: %u, size: %u\n",
    //     frag_id_, vec_type_, vec_metric_, vec_dims_, vec_col_idx_, vec_top_n_, vec_size_in_bytes_);

    vec_start_pos_ = cur_pos_;

#ifdef PA_MALLOC
    void* page_ptr = lc_ndbd_pool_malloc(32 * 1024, RG_QUERY_MEMORY,
        thread_id_, false);
    if (page_ptr == nullptr) {
      g_eventLogger->error("Alloc mem for pushdown vector search interpreter failed");
      return false;
    }
    vec_buf_ = static_cast<Uint32*>(page_ptr);
#else
    vec_buf_ = new Uint32[g_vec_buf_len_];
#endif // PA_MALLOC

    inited_ = true;
    return true;
  }

  /*
   * 3. Get all the group by columns id.
   */
  if (n_gb_cols_) {
#ifdef PA_MALLOC
    assert(n_gb_cols_ <= MAX_AGG_N_GROUPBY_COLS);
    gb_cols_ = gb_cols_buf_;
#else
    gb_cols_ = new Uint32[n_gb_cols_];
#endif // PA_MALLOC

    Uint32 i = 0;
    while (i < n_gb_cols_ && cur_pos_ < prog_len_) {
      gb_cols_[i++] = prog_[cur_pos_++];
    }
#ifdef PA_MALLOC
    gb_cmp_ctx_.n_cols = 0;
    gb_cmp_ctx_.all_binary_cmp = false;
    gb_map_buf_ = std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>(
                      GBHashEntryCmp(&gb_cmp_ctx_));
    gb_map_ = &gb_map_buf_;
#else
    gb_cmp_ctx_.n_cols = 0;
    gb_cmp_ctx_.all_binary_cmp = false;
    gb_map_ = new std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>(
                      GBHashEntryCmp(&gb_cmp_ctx_));
#endif // PA_MALLOC
  }

  /*
   * 4. Reset all aggregation results
   */
  if (n_agg_results_) {
#ifdef PA_MALLOC
    assert(n_agg_results_ <= MAX_AGG_N_RESULTS);
    agg_results_ = agg_results_buf_;
#else
    agg_results_ = new AggResItem[n_agg_results_];
#endif // PA_MALLOC
    Uint32 i = 0;
    while (i < n_agg_results_) {
      agg_results_[i].type = NDB_TYPE_UNDEFINED;
      agg_results_[i].value.val_int64 = 0;
      agg_results_[i].is_unsigned = false;
      agg_results_[i].is_null = true;
      i++;
    }
  }

  inited_ = true;
  agg_prog_start_pos_ = cur_pos_;
  memset(registers_, 0, sizeof(registers_));

  return true;
}


/**
 * OptimizeProgram - Analyze the aggregation program and replace generic opcodes
 * with type-specific ones based on static type analysis.
 *
 * This function walks through the program instructions, tracks the type of each
 * register, and replaces generic opcodes (kOpSum, kOpPlus, etc.) with their
 * type-specific variants (kOpSumBigint, kOpSumDouble, kOpPlusBigint, etc.).
 *
 * Since the aggregation program has no loops, the type of each register is
 * deterministic and can be computed in a single pass.
 *
 * Must be called after Init() and before the first ProcessRec().
 */
bool AggInterpreter::OptimizeProgram() {
  if (!inited_) {
    return false;
  }

  // Track the type of each register: NDB_TYPE_BIGINT, NDB_TYPE_DOUBLE, or NDB_TYPE_UNDEFINED
  DataType reg_types[kRegTotal];
  for (Uint32 i = 0; i < kRegTotal; i++) {
    reg_types[i] = NDB_TYPE_UNDEFINED;
  }

  // Single pass: analyze and rewrite the program
  Uint32 exec_pos = agg_prog_start_pos_;

  while (exec_pos < prog_len_) {
    Uint32 value = prog_[exec_pos];
    Uint8 op = (value & 0xFC000000) >> 26;
    Uint32 reg_index, reg_index2;
    DataType type;
    Uint8 new_op = op;

    switch (op) {
      case kOpLoadCol:
        type = (value & 0x03E00000) >> 21;
        reg_index = (value & 0x000F0000) >> 16;
        if (type == NDB_TYPE_FLOAT || type == NDB_TYPE_DOUBLE) {
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (type == NDB_TYPE_DECIMAL || type == NDB_TYPE_DECIMALUNSIGNED) {
          // Decimal could be either depending on scale - keep generic
          reg_types[reg_index] = NDB_TYPE_UNDEFINED;
          exec_pos++;  // Skip decimal info word
        } else {
          reg_types[reg_index] = NDB_TYPE_BIGINT;
        }
        break;

      case kOpLoadConst:
        type = (value & 0x03E00000) >> 21;
        reg_index = (value & 0x000F0000) >> 16;
        if (type == NDB_TYPE_DOUBLE) {
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else {
          reg_types[reg_index] = NDB_TYPE_BIGINT;
        }
        exec_pos += 2;  // Skip constant value words
        break;

      case kOpMov:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        reg_types[reg_index] = reg_types[reg_index2];
        break;

      case kOpPlus:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE ||
            reg_types[reg_index2] == NDB_TYPE_DOUBLE) {
          new_op = kOpPlusDouble;
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] != NDB_TYPE_UNDEFINED &&
                   reg_types[reg_index2] != NDB_TYPE_UNDEFINED) {
          new_op = kOpPlusBigint;
          reg_types[reg_index] = NDB_TYPE_BIGINT;
        } else {
          reg_types[reg_index] = NDB_TYPE_UNDEFINED;
        }
        prog_[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        break;

      case kOpMinus:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE ||
            reg_types[reg_index2] == NDB_TYPE_DOUBLE) {
          new_op = kOpMinusDouble;
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] != NDB_TYPE_UNDEFINED &&
                   reg_types[reg_index2] != NDB_TYPE_UNDEFINED) {
          new_op = kOpMinusBigint;
          reg_types[reg_index] = NDB_TYPE_BIGINT;
        } else {
          reg_types[reg_index] = NDB_TYPE_UNDEFINED;
        }
        prog_[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        break;

      case kOpMul:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE ||
            reg_types[reg_index2] == NDB_TYPE_DOUBLE) {
          new_op = kOpMulDouble;
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] != NDB_TYPE_UNDEFINED &&
                   reg_types[reg_index2] != NDB_TYPE_UNDEFINED) {
          new_op = kOpMulBigint;
          reg_types[reg_index] = NDB_TYPE_BIGINT;
        } else {
          reg_types[reg_index] = NDB_TYPE_UNDEFINED;
        }
        prog_[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        break;

      case kOpDiv:
        reg_index = (value >> 12) & 0x0F;
        // Division always produces double
        new_op = kOpDivDouble;
        reg_types[reg_index] = NDB_TYPE_DOUBLE;
        prog_[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        break;

      case kOpDivInt:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        if (reg_types[reg_index] != NDB_TYPE_UNDEFINED &&
            reg_types[reg_index2] != NDB_TYPE_UNDEFINED) {
          new_op = kOpDivIntBigint;
        }
        reg_types[reg_index] = NDB_TYPE_BIGINT;
        prog_[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        break;

      case kOpMod:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        // No type-specific version for Mod - just track types
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE ||
            reg_types[reg_index2] == NDB_TYPE_DOUBLE) {
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] == NDB_TYPE_UNDEFINED ||
                   reg_types[reg_index2] == NDB_TYPE_UNDEFINED) {
          reg_types[reg_index] = NDB_TYPE_UNDEFINED;
        } else {
          reg_types[reg_index] = NDB_TYPE_BIGINT;
        }
        break;

      case kOpSum:
        reg_index = (value & 0x000F0000) >> 16;
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE) {
          new_op = kOpSumDouble;
        } else if (reg_types[reg_index] == NDB_TYPE_BIGINT) {
          new_op = kOpSumBigint;
        }
        if (new_op != op) {
          prog_[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        }
        break;

      case kOpMax:
        reg_index = (value & 0x000F0000) >> 16;
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE) {
          new_op = kOpMaxDouble;
        } else if (reg_types[reg_index] == NDB_TYPE_BIGINT) {
          new_op = kOpMaxBigint;
        }
        if (new_op != op) {
          prog_[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        }
        break;

      case kOpMin:
        reg_index = (value & 0x000F0000) >> 16;
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE) {
          new_op = kOpMinDouble;
        } else if (reg_types[reg_index] == NDB_TYPE_BIGINT) {
          new_op = kOpMinBigint;
        }
        if (new_op != op) {
          prog_[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        }
        break;

      case kOpCount:
        // Count always produces BIGINT - no optimization needed
        break;

      default:
        break;
    }
    exec_pos++;
  }

  return true;
}

static bool TypeSupported(DataType type) {
  switch (type) {
    case NDB_TYPE_TINYINT:
    case NDB_TYPE_SMALLINT:
    case NDB_TYPE_MEDIUMINT:
    case NDB_TYPE_INT:
    case NDB_TYPE_BIGINT:

    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:

    case NDB_TYPE_FLOAT:
    case NDB_TYPE_DOUBLE:

    case NDB_TYPE_DECIMAL:
    case NDB_TYPE_DECIMALUNSIGNED:
      return true;
    default:
      return false;
  }
  return false;
}

static bool IsUnsigned(DataType type) {
  switch (type) {
    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:
    case NDB_TYPE_DECIMALUNSIGNED:
      return true;
    default:
      return false;
  }
  return false;
}

static DataType AlignedType(DataType type, int scale) {
  switch (type) {
    case NDB_TYPE_TINYINT:
    case NDB_TYPE_SMALLINT:
    case NDB_TYPE_MEDIUMINT:
    case NDB_TYPE_INT:
    case NDB_TYPE_BIGINT:

    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:

      return NDB_TYPE_BIGINT;
    case NDB_TYPE_FLOAT:
    case NDB_TYPE_DOUBLE:
      return NDB_TYPE_DOUBLE;
    case NDB_TYPE_DECIMAL:
    case NDB_TYPE_DECIMALUNSIGNED:
      return scale == 0 ? NDB_TYPE_BIGINT : NDB_TYPE_DOUBLE;
    default:
      assert(0);
  }
  return NDB_TYPE_UNDEFINED;
}

[[maybe_unused]] static void PrintValue(const AggResItem* res, char* log_buf) {
  if (res->type == NDB_TYPE_BIGINT) {
    if (res->is_unsigned) {
      sprintf(log_buf + strlen(log_buf), "[%llu, %d, %d, %d]",
          res->value.val_uint64, res->type, res->is_unsigned, res->is_null);
    } else {
      sprintf(log_buf + strlen(log_buf), "[%lld, %d, %d, %d]",
          res->value.val_int64, res->type, res->is_unsigned, res->is_null);
    }
  } else {
    assert(res->type == NDB_TYPE_DOUBLE);
    sprintf(log_buf + strlen(log_buf), "[%lf, %d, %d, %d]",
        res->value.val_double, res->type, res->is_unsigned, res->is_null);
  }
  g_eventLogger->info("[PA_INTERP_TRACE] %s", log_buf);
}

static Int32 Sum(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED) {
    // Agg result first initialized
    *res = a;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "Sum() init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  if (res->is_null) {
    assert(res->value.val_int64 == 0);
  }

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || res->type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT &&
          (res->type == NDB_TYPE_BIGINT || res->type == NDB_TYPE_UNDEFINED));
    res_type = NDB_TYPE_BIGINT;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    Int64 val0 = a.value.val_int64;
    Int64 val1 = res->value.val_int64;
    Int64 res_val = static_cast<Uint64>(val0) + static_cast<Uint64>(val1);
    bool res_unsigned = false;

    if (a.is_unsigned) {
      if (res->is_unsigned || val1 >= 0) {
        if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) {
          // overflows;
          return -1;
        } else {
          res_unsigned = true;
        }
      } else {
        if ((Uint64)val0 > (Uint64)(LLONG_MAX)) {
          res_unsigned = true;
        }
      }
    } else {
      if (res->is_unsigned) {
        if (val0 >= 0) {
          if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) {
            // overflows;
            return -1;
          } else {
            res_unsigned = true;
          }
        } else {
          if ((Uint64)val1 > (Uint64)(LLONG_MAX)) {
            res_unsigned = true;
          }
        }
      } else {
        if (val0 >= 0 && val1 >= 0) {
          res_unsigned = true;
        } else if (val0 < 0 && val1 < 0 && res_val >= 0) {
          // overflow
          return -1;
        }
      }
    }

    // Check if res_val is overflow
    bool unsigned_flag = (a.is_unsigned | res->is_unsigned);
    if ((unsigned_flag && !res_unsigned && res_val < 0) ||
        (!unsigned_flag && res_unsigned &&
         (Uint64)res_val > (Uint64)LLONG_MAX)) {
      return -1;
    } else {
      if (unsigned_flag) {
        res->value.val_uint64 = res_val;
      } else {
        res->value.val_int64 = res_val;
      }
    }
    res->is_unsigned = unsigned_flag;
  } else {
    double val0 = (a.type == NDB_TYPE_DOUBLE) ?
                     a.value.val_double :
                     ((a.is_unsigned == true) ?
                       static_cast<double>(a.value.val_uint64) :
                       static_cast<double>(a.value.val_int64));
    double val1 = (res->type == NDB_TYPE_DOUBLE) ?
                     res->value.val_double :
                     ((res->is_unsigned == true) ?
                       static_cast<double>(res->value.val_uint64) :
                       static_cast<double>(res->value.val_int64));
    double res_val = val0 + val1;
    if (std::isfinite(res_val)) {
      res->value.val_double = res_val;
    } else {
      // overflow
      return -1;
    }
    res->is_unsigned = false;
  }

  res->type = res_type;
  res->is_null = false;

#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "Sum(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP
  return 0;
}

/**
 * SumBigint - Sum for BIGINT (handles both signed and unsigned dynamically)
 */
static Int32 SumBigint(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) {
    return 1;
  }

  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_BIGINT;
    res->value.val_int64 = a.value.val_int64;
    res->is_unsigned = a.is_unsigned;
    res->is_null = false;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "SumBigint() init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }

  Int64 val0 = a.value.val_int64;
  Int64 val1 = res->value.val_int64;
  Int64 res_val = static_cast<Uint64>(val0) + static_cast<Uint64>(val1);
  bool res_unsigned = false;

  if (a.is_unsigned) {
    if (res->is_unsigned || val1 >= 0) {
      if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) {
        return -1;
      }
      res_unsigned = true;
    } else {
      if ((Uint64)val0 > (Uint64)(LLONG_MAX)) {
        res_unsigned = true;
      }
    }
  } else {
    if (res->is_unsigned) {
      if (val0 >= 0) {
        if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) {
          return -1;
        }
        res_unsigned = true;
      } else {
        if ((Uint64)val1 > (Uint64)(LLONG_MAX)) {
          res_unsigned = true;
        }
      }
    } else {
      if (val0 >= 0 && val1 >= 0) {
        res_unsigned = true;
      } else if (val0 < 0 && val1 < 0 && res_val >= 0) {
        return -1;
      }
    }
  }

  bool unsigned_flag = (a.is_unsigned | res->is_unsigned);
  if ((unsigned_flag && !res_unsigned && res_val < 0) ||
      (!unsigned_flag && res_unsigned &&
       (Uint64)res_val > (Uint64)LLONG_MAX)) {
    return -1;
  }

  if (unsigned_flag) {
    res->value.val_uint64 = res_val;
  } else {
    res->value.val_int64 = res_val;
  }
  res->is_unsigned = unsigned_flag;
#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "SumBigint(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP
  return 0;
}

/**
 * SumDouble - Sum for double precision floats
 */
static Int32 SumDouble(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) {
    return 1;
  }

  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_DOUBLE;
    res->value.val_double = a.value.val_double;
    res->is_unsigned = false;
    res->is_null = false;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "SumDouble() init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }

  double res_val = a.value.val_double + res->value.val_double;

  if (unlikely(!std::isfinite(res_val))) {
    return -1;
  }

  res->value.val_double = res_val;
#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "SumDouble(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP
  return 0;
}

static Int32 Max(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED || res->is_null) {
    // Agg result first initialized
    *res = a;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "Max(), init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  if (res->is_null) {
    assert(res->value.val_int64 == 0);
  }

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || res->type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT &&
          (res->type == NDB_TYPE_BIGINT || res->type == NDB_TYPE_UNDEFINED));
    res_type = NDB_TYPE_BIGINT;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    if (!a.is_unsigned && !res->is_unsigned) {
      res->value.val_int64 = (a.value.val_int64 > res->value.val_int64) ?
                              a.value.val_int64 : res->value.val_int64;
    } else if (a.is_unsigned && res->is_unsigned) {
      res->value.val_uint64 = (a.value.val_uint64 > res->value.val_uint64) ?
                              a.value.val_uint64 : res->value.val_uint64;
    } else if (a.is_unsigned && !res->is_unsigned) {
      if (res->value.val_int64 < 0) {
        res->value.val_uint64 = a.value.val_uint64;
      } else {
        res->value.val_uint64 = a.value.val_uint64 >
                static_cast<Uint64>(res->value.val_int64) ?
                a.value.val_uint64 :
                static_cast<Uint64>(res->value.val_int64);
      }
      res->is_unsigned = true;
    } else {
      assert(!a.is_unsigned && res->is_unsigned);
      if (a.value.val_int64 < 0) {
      } else {
        res->value.val_uint64 = static_cast<Uint64>(a.value.val_int64) >
                                res->value.val_uint64;
      }
    }
  } else {
    assert(res_type == NDB_TYPE_DOUBLE);
    res->value.val_double = (a.value.val_double > res->value.val_double) ?
                             a.value.val_double : res->value.val_double;
  }
  res->is_null = false;

#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "Max(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP

  return 0;
}

/**
 * MaxBigint - Max for BIGINT (handles both signed and unsigned dynamically)
 */
static Int32 MaxBigint(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) {
    return 1;
  }

  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_BIGINT;
    res->value.val_int64 = a.value.val_int64;
    res->is_unsigned = a.is_unsigned;
    res->is_null = false;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "MaxBigint() init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }

  if (!a.is_unsigned && !res->is_unsigned) {
    if (a.value.val_int64 > res->value.val_int64) {
      res->value.val_int64 = a.value.val_int64;
    }
  } else if (a.is_unsigned && res->is_unsigned) {
    if (a.value.val_uint64 > res->value.val_uint64) {
      res->value.val_uint64 = a.value.val_uint64;
    }
  } else if (a.is_unsigned && !res->is_unsigned) {
    if (res->value.val_int64 < 0) {
      res->value.val_uint64 = a.value.val_uint64;
      res->is_unsigned = true;
    } else {
      if (a.value.val_uint64 > static_cast<Uint64>(res->value.val_int64)) {
        res->value.val_uint64 = a.value.val_uint64;
        res->is_unsigned = true;
      }
    }
  } else {
    // !a.is_unsigned && res->is_unsigned
    if (a.value.val_int64 >= 0) {
      if (static_cast<Uint64>(a.value.val_int64) > res->value.val_uint64) {
        res->value.val_uint64 = static_cast<Uint64>(a.value.val_int64);
      }
    }
    // If a is negative and res is unsigned, res is already larger
  }
#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "MaxBigint(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP
  return 0;
}

/**
 * MaxDouble - Max for double precision floats
 */
static Int32 MaxDouble(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) {
    return 1;
  }

  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_DOUBLE;
    res->value.val_double = a.value.val_double;
    res->is_unsigned = false;
    res->is_null = false;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "MaxDouble() init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }

  if (a.value.val_double > res->value.val_double) {
    res->value.val_double = a.value.val_double;
  }
#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "MaxDouble(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP
  return 0;
}

static Int32 Min(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED || res->is_null) {
    // Agg result first initialized
    *res = a;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "Min(), init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  if (res->is_null) {
    assert(res->value.val_int64 == 0);
  }

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || res->type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT &&
          (res->type == NDB_TYPE_BIGINT || res->type == NDB_TYPE_UNDEFINED));
    res_type = NDB_TYPE_BIGINT;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    if (!a.is_unsigned && !res->is_unsigned) {
      res->value.val_int64 = (a.value.val_int64 < res->value.val_int64) ?
                              a.value.val_int64 : res->value.val_int64;
    } else if (a.is_unsigned && res->is_unsigned) {
      res->value.val_uint64 = (a.value.val_uint64 < res->value.val_uint64) ?
                              a.value.val_uint64 : res->value.val_uint64;
    } else if (a.is_unsigned && !res->is_unsigned) {
      if (res->value.val_int64 < 0) {
      } else {
        res->value.val_uint64 = a.value.val_uint64 <
                static_cast<Uint64>(res->value.val_int64) ?
                a.value.val_uint64 :
                static_cast<Uint64>(res->value.val_int64);
        res->is_unsigned = true;
      }
    } else {
      assert(!a.is_unsigned && res->is_unsigned);
      if (a.value.val_int64 < 0) {
        res->value.val_int64 = a.value.val_int64;
        res->is_unsigned = false;
      } else {
        res->value.val_uint64 = static_cast<Uint64>(a.value.val_int64) <
                                res->value.val_uint64 ?
                                static_cast<Uint64>(a.value.val_int64) :
                                res->value.val_uint64;
      }
    }
  } else {
    assert(res_type == NDB_TYPE_DOUBLE);
    res->value.val_double = (a.value.val_double < res->value.val_double) ?
                             a.value.val_double : res->value.val_double;
  }
  res->is_null = false;

#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "Min(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP

  return 0;
}

/**
 * MinBigint - Min for BIGINT (handles both signed and unsigned dynamically)
 */
static Int32 MinBigint(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) {
    return 1;
  }

  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_BIGINT;
    res->value.val_int64 = a.value.val_int64;
    res->is_unsigned = a.is_unsigned;
    res->is_null = false;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "MinBigint() init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }

  if (!a.is_unsigned && !res->is_unsigned) {
    if (a.value.val_int64 < res->value.val_int64) {
      res->value.val_int64 = a.value.val_int64;
    }
  } else if (a.is_unsigned && res->is_unsigned) {
    if (a.value.val_uint64 < res->value.val_uint64) {
      res->value.val_uint64 = a.value.val_uint64;
    }
  } else if (a.is_unsigned && !res->is_unsigned) {
    // a is unsigned, res is signed
    if (res->value.val_int64 < 0) {
      // res is negative, so res is smaller - keep res
    } else {
      if (a.value.val_uint64 < static_cast<Uint64>(res->value.val_int64)) {
        res->value.val_uint64 = a.value.val_uint64;
        res->is_unsigned = true;
      }
    }
  } else {
    // !a.is_unsigned && res->is_unsigned
    if (a.value.val_int64 < 0) {
      res->value.val_int64 = a.value.val_int64;
      res->is_unsigned = false;
    } else {
      if (static_cast<Uint64>(a.value.val_int64) < res->value.val_uint64) {
        res->value.val_uint64 = static_cast<Uint64>(a.value.val_int64);
      }
    }
  }
#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "MinBigint(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP
  return 0;
}

/**
 * MinDouble - Min for double precision floats
 */
static Int32 MinDouble(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) {
    return 1;
  }

  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_DOUBLE;
    res->value.val_double = a.value.val_double;
    res->is_unsigned = false;
    res->is_null = false;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "MinDouble() init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }

  if (a.value.val_double < res->value.val_double) {
    res->value.val_double = a.value.val_double;
  }
#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "MinDouble(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP
  return 0;
}

static Int32 Count(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED) {
    // Agg result first initialized
    res->type = NDB_TYPE_BIGINT;
    res->value.val_uint64 = 0;
    res->is_unsigned = true;
    res->is_null = false;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "Count(), init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  assert(res->type == NDB_TYPE_BIGINT &&
      res->is_null == false && res->is_unsigned == true);
  res->value.val_uint64 += 1;

#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "Count(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP

  return 0;
}

/*
 * Success: RETURN 0
 * Failure: RETURN 1860+ by aggregation interpreter
 *          Others returned by readAttributes
 */
Int32 AggInterpreter::ProcessRec(Dbtup* block_tup,
        Dbtup::KeyReqStruct* req_struct,
        bool* vec_update_candidate) {
  // assert(inited_);
  // assert(req_struct->read_length == 0);
  if (!inited_ || req_struct->read_length != 0) {
    g_eventLogger->debug("AggInterpreter::ProcessRec error, inited: %d, read_length: %u",
            inited_, req_struct->read_length);
    return ZAGG_OTHER_ERROR;
  }

  *vec_update_candidate = false;
  if (unlikely(vec_search_)) {
    Uint32 vec_col_idx = (vec_col_idx_ & 0x0000FFFF) << 16;
    int ret = block_tup->readAttributes(req_struct, &(vec_col_idx), 1,
                  vec_buf_ + vec_buf_pos_, g_vec_buf_len_ - vec_buf_pos_);
    if (ret < 0) {
      g_eventLogger->debug("read vector column error: %d", ret);
      return -ret;
    }
    AttributeHeader* header = nullptr;
    header = reinterpret_cast<AttributeHeader*>(vec_buf_ + vec_buf_pos_);
    const Uint32* attrDescriptor = req_struct->tablePtrP->tabDescriptor +
      (((vec_col_idx) >> 16) * ZAD_SIZE);
    const Uint32 TattrDesc1 = attrDescriptor[0];
    // const Uint32 TattrDesc2 = attrDescriptor[1];
    const Uint32 type_id = AttributeDescriptor::getType(TattrDesc1);
    const Uint32 array_type = AttributeDescriptor::getArrayType(TattrDesc1);

#ifdef DEBUG_VS_INTERP
    const Uint32 attributeId = header->getAttributeId();
    assert(attributeId == (vec_col_idx >> 16));
    const Uint32 size = AttributeDescriptor::getSize(TattrDesc1);
    const Uint32 size_in_bytes = AttributeDescriptor::getSizeInBytes(TattrDesc1);
    const Uint32 size_in_words = AttributeDescriptor::getSizeInWords(TattrDesc1);
    const Uint32 array_size = AttributeDescriptor::getArraySize(TattrDesc1);
    const Uint32 nullable = AttributeDescriptor::getNullable(TattrDesc1);
    const Uint32 distri_key = AttributeDescriptor::getDKey(TattrDesc1);
    const Uint32 primary_key = AttributeDescriptor::getPrimaryKey(TattrDesc1);
    const Uint32 dynamic = AttributeDescriptor::getDynamic(TattrDesc1);
    const Uint32 disk_based = AttributeDescriptor::getDiskBased(TattrDesc1);
    VS_INTERP_TRACE(table_id_, frag_id_,
         "AttributeDescriptor, attributeId: %u, type_id: %u, size: %u, "
         "size_in_bytes: %u, size_in_words: %u, array_type: %u, "
         "array_size: %u, nullable: %u, distri_key: %u, primary_key: %u "
         "dynamic: %u, disk_based: %u",
         attributeId, type_id, size, size_in_bytes, size_in_words, array_type,
         array_size, nullable, distri_key, primary_key, dynamic, disk_based);
#endif  // DEBUG_VS_INTERP

    if (type_id != NDB_TYPE_LONGVARBINARY) {
      g_eventLogger->debug("Unsupported vector column type: %u", type_id);
      return ZAGG_COL_TYPE_UNSUPPORTED;
    }

    Uint32 length_bytes = 0;
    if (array_type == NDB_ARRAYTYPE_SHORT_VAR) {
      length_bytes = 1;
    } else if (array_type == NDB_ARRAYTYPE_MEDIUM_VAR) {
      length_bytes = 2;
    } else {
      assert(0);
    }
    Uint32 dims = 0;
    if (!header->isNULL()) {
#if DEBUG
      Uint32 len = header->getByteSize();
      assert(len >= length_bytes);
#endif  // DEBUG
      if (length_bytes == 1) {
        dims = *((Uint8*)header->getDataPtr());
      } else {
        dims = *((Uint16*)header->getDataPtr());
        // assert((dims & 0x00008000) == 1);
        // dims = (dims & 0x00007FFF);
      }
      assert(vec_dims_ == dims / sizeof(float));
    } else {
      assert(0);
    }

    double distance = 0;
    float* target = (float* )&(prog_[vec_start_pos_]);
    float* current = (float* )((char*)header->getDataPtr() + length_bytes);
    simsimd_l2sq_f32(current, target, vec_dims_, &distance);
    // simsimd_l2sq_f32_serial(current, target, vec_dims_, &distance);
    curr_distance_ = distance;
    if (vec_top_n_ != 0 &&
        (vec_top_n_results_.size() < vec_top_n_ ||
        distance < vec_top_n_results_.top()->distance_)) {
      *vec_update_candidate = true;
    }

    return 0;
  }

  AggResItem* agg_res_ptr = nullptr;
  if (n_gb_cols_) {
    char* agg_rec = nullptr;

    AttributeHeader* header = nullptr;
    buf_pos_ = 0;
    for (Uint32 i = 0; i < n_gb_cols_; i++) {
      int ret = block_tup->readAttributes(req_struct, &(gb_cols_[i]), 1,
                    buf_ + buf_pos_, g_buf_len_ - buf_pos_);
      // assert(ret >= 0);
      if (ret < 0) {
        g_eventLogger->debug("read group by column error: %d", ret);
        return -ret;
      }
      header = reinterpret_cast<AttributeHeader*>(buf_ + buf_pos_);
      buf_pos_ += (1 + header->getDataSize());
    }

    if (!gb_cmp_inited_) {
      gb_cmp_ctx_.n_cols = n_gb_cols_;
      bool all_binary = true;
      for (Uint32 i = 0; i < n_gb_cols_; i++) {
        Uint32 attrId = gb_cols_[i] >> 16;
        const Uint32* attrDescriptor = req_struct->tablePtrP->tabDescriptor +
          (attrId * ZAD_SIZE);
        const Uint32 TattrDesc1 = attrDescriptor[0];
        const Uint32 TattrDesc2 = attrDescriptor[1];
        gb_cmp_ctx_.col_meta[i].typeId =
            AttributeDescriptor::getType(TattrDesc1);
        gb_cmp_ctx_.col_meta[i].cs = nullptr;
        if (AttributeOffset::getCharsetFlag(TattrDesc2)) {
          all_binary = false;
          const Uint32 pos = AttributeOffset::getCharsetPos(TattrDesc2);
          gb_cmp_ctx_.col_meta[i].cs =
              req_struct->tablePtrP->charsetArray[pos];
        }
      }
      gb_cmp_ctx_.all_binary_cmp = all_binary;
      gb_cmp_inited_ = true;
    }

    Uint32 len_in_char = buf_pos_ * sizeof(Uint32);
    GBHashEntry entry{reinterpret_cast<char*>(buf_), len_in_char};
    auto iter = gb_map_->find(entry);
    if (iter != gb_map_->end()) {
      header = reinterpret_cast<AttributeHeader*>(iter->first.ptr);
      agg_res_ptr = reinterpret_cast<AggResItem*>(iter->second.ptr);
      PA_INTERP_TRACE(frag_id_,
                      "Found GBHashEntry, id: %u, byte_size: %u, "
                      "data_size: %u, is_null: %u",
                      header->getAttributeId(), header->getByteSize(),
                      header->getDataSize(), header->isNULL());
    } else {
      /*
       * update req_struct->read_length here, which will update the
       * Dblqh::ScanRecord::m_curr_batch_size_bytes later in the
       * Dblqh::scanTupkeyConfLab, even we don't use that variable
       * to decide whether reaches batch limitation. Only increase
       * Dblqh::ScanRecord::m_curr_batch_size_bytes when new group
       * item is inserted into gb_map_.
       * For aggregation,
       * we use Dblqh::ScanRecord::m_agg_curr_batch_size_bytes to
       * indicate batch limitation
       */
      req_struct->read_length = (len_in_char +
                       n_agg_results_ * sizeof(AggResItem)) / sizeof(Int32);

      // we use result_size_ to decide whether need to send some aggregation
      // results to API.
      result_size_ += len_in_char +
                       n_agg_results_ * sizeof(AggResItem);
#ifdef PA_MALLOC
      agg_rec = MemAlloc(len_in_char +
                          n_agg_results_ * sizeof(AggResItem));
#else
      agg_rec = new char[len_in_char +
                        n_agg_results_ * sizeof(AggResItem)];
#endif // PA_MALLOC
      memset(agg_rec, 0, len_in_char +
                        n_agg_results_ * sizeof(AggResItem));
      memcpy(agg_rec, reinterpret_cast<char*>(buf_), len_in_char);
      GBHashEntry new_entry{agg_rec, len_in_char};

      gb_map_->insert(std::pair<GBHashEntry, GBHashEntry>(
                      new_entry,
            GBHashEntry{agg_rec + len_in_char,
            static_cast<Uint32>(n_agg_results_ * sizeof(AggResItem))}));
      n_groups_ = gb_map_->size();
      agg_res_ptr = reinterpret_cast<AggResItem*>(agg_rec + len_in_char);

      // Initialize the new group's aggregation slots
      assert(n_agg_results_ <= MAX_AGG_N_RESULTS);
      for (Uint32 i = 0; i < n_agg_results_; i++) {
        agg_res_ptr[i].type = NDB_TYPE_UNDEFINED;
        agg_res_ptr[i].value.val_int64 = 0;
        agg_res_ptr[i].is_unsigned = false;
        agg_res_ptr[i].is_null = true;
        assert(agg_res_ptr[i].type == agg_results_[i].type);
        assert(agg_res_ptr[i].value.val_int64 == agg_results_[i].value.val_int64);
        assert(agg_res_ptr[i].is_unsigned == agg_results_[i].is_unsigned);
        assert(agg_res_ptr[i].is_null == agg_results_[i].is_null);
      }
    }
  } else {
    agg_res_ptr = agg_results_;
  }

  Uint32 col_index;

  Uint32 value;
  DataType type;
  bool is_unsigned;
  Uint32 reg_index;

  Uint32 reg_index2;

  Uint32 agg_index;

  const Uint32* attrDescriptor = nullptr;

  Int32 decimal_info = 0;
  Int32 precision = 0;
  Int32 scale = 0;
  Int32 dec_ret = E_DEC_OK;
  Uint8* dec_buf_ptr = nullptr;
  double dec_val_dbl = 0;
  longlong dec_val_ll = 0;
  ulonglong dec_val_ull = 0;

  Uint32 exec_pos = agg_prog_start_pos_;
  bool debug_print = (frag_id_ == DEBUG_PA_INTERP_PART_ID);
  while (exec_pos < prog_len_) {
    value = prog_[exec_pos++];
    Uint8 op = (value & 0xFC000000) >> 26;
    int ret = 0;
    buf_pos_ = 0;
    AttributeHeader* header = nullptr;

    switch (op) {
      case kOpPlus:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegPlusReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[PLUS], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpMinus:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegMinusReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[MINUS], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpMul:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegMulReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[MUL], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpDiv:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegDivReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index], false);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[DIV], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpDivInt:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegDivReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index], true);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[DIVINT], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpMod:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegModReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[MOD], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      // Type-specific Plus operations
      case kOpPlusBigint:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegPlusBigint(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[PlusBigint], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      case kOpPlusDouble:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegPlusDouble(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[PlusDouble], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      // Type-specific Minus operations
      case kOpMinusBigint:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegMinusBigint(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[MinusBigint], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      case kOpMinusDouble:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegMinusDouble(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[MinusDouble], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      // Type-specific Multiply operations
      case kOpMulBigint:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegMulBigint(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[MulBigint], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      case kOpMulDouble:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegMulDouble(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[MulDouble], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      // Type-specific Division operations
      case kOpDivDouble:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegDivDouble(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[DivDouble], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      case kOpDivIntBigint:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegDivIntBigint(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[DivIntBigint], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      case kOpLoadCol:
        type = (value & 0x03E00000) >> 21;
        is_unsigned = IsUnsigned(type);
        reg_index = (value & 0x000F0000) >> 16;
        col_index = (value & 0x0000FFFF) << 16;

        ret = block_tup->readAttributes(req_struct, &(col_index), 1,
                  buf_ + buf_pos_, g_buf_len_ - buf_pos_);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("read column error: %d", ret);
          return -ret;
        }
        header = reinterpret_cast<AttributeHeader*>(buf_ + buf_pos_);
        attrDescriptor = req_struct->tablePtrP->tabDescriptor +
          (((col_index) >> 16) * ZAD_SIZE);
        assert(header->getAttributeId() == (col_index >> 16));

        assert(type == AttributeDescriptor::getType(attrDescriptor[0]));
        // assert(TypeSupported(type));
        if (!TypeSupported(type)) {
          g_eventLogger->debug("Unsupported column type: %u", type);
          return ZAGG_COL_TYPE_UNSUPPORTED;
        }

        if (type == NDB_TYPE_DECIMAL ||
            type == NDB_TYPE_DECIMALUNSIGNED) {
          if (unlikely(exec_pos >= prog_len_)) {
            g_eventLogger->warning("Pushdown aggregation program ended in the"
                                   " middle of an instruction.");
            return ZAGG_OTHER_ERROR;
          }
          decimal_info =
              sint4korr(reinterpret_cast<char*>(&prog_[exec_pos++]));
          precision = decimal_info >> 16;
          scale = decimal_info & 0xFFFF;
        } else {
          precision = 0;
          scale = 0;
        }

        ResetRegister(&registers_[reg_index]);
        registers_[reg_index].type = AlignedType(type, scale);
        registers_[reg_index].is_unsigned = is_unsigned;
        registers_[reg_index].is_null = header->isNULL();
        if (registers_[reg_index].is_null) {
          // Column has a null value
          PA_INTERP_TRACE(frag_id_,
                          "Load NULL, type: %u",
                          registers_[reg_index].type);
          registers_[reg_index].value.val_int64 = 0;
          break;
        }
        switch (type) {
          case NDB_TYPE_TINYINT:
            registers_[reg_index].value.val_int64 =
                *reinterpret_cast<Int8*>(&buf_[buf_pos_ + 1]);
            PA_INTERP_TRACE(frag_id_,
                            "Load NDB_TYPE_TINYINT %lld",
                            registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_SMALLINT:
            registers_[reg_index].value.val_int64 =
                sint2korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            PA_INTERP_TRACE(frag_id_,
                            "Load NDB_TYPE_SMALLINT %lld",
                            registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_MEDIUMINT:
            registers_[reg_index].value.val_int64 =
                sint3korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            PA_INTERP_TRACE(frag_id_,
                            "Load NDB_TYPE_MEDIUM %lld",
                            registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_INT:
            registers_[reg_index].value.val_int64 =
                sint4korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            PA_INTERP_TRACE(frag_id_,
                            "Load NDB_TYPE_INT %lld",
                            registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_BIGINT:
            registers_[reg_index].value.val_int64 =
                sint8korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            PA_INTERP_TRACE(frag_id_,
                            "Load NDB_TYPE_BIGINT %lld",
                            registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_TINYUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                *reinterpret_cast<Uint8*>(&buf_[buf_pos_ + 1]);
            PA_INTERP_TRACE(frag_id_,
                            "Load NDB_TYPE_TINYUNSIGNED %llu",
                            registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_SMALLUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                uint2korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            PA_INTERP_TRACE(frag_id_,
                            "Load NDB_TYPE_SMALLUNSIGNED %llu",
                            registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_MEDIUMUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                uint3korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            PA_INTERP_TRACE(frag_id_,
                            "Load NDB_TYPE_MEDIUMUNSIGNED %llu",
                            registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_UNSIGNED:
            registers_[reg_index].value.val_uint64 =
                uint4korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            PA_INTERP_TRACE(frag_id_,
                            "Load NDB_TYPE_UNSIGNED %llu",
                            registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_BIGUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                uint8korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            PA_INTERP_TRACE(frag_id_,
                            "Load NDB_TYPE_BIGUNSIGNED %llu",
                            registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_FLOAT:
            registers_[reg_index].value.val_double =
                floatget(reinterpret_cast<unsigned char*>(&buf_[buf_pos_ + 1]));
            PA_INTERP_TRACE(frag_id_,
                            "Load NDB_TYPE_FLOAT %lf",
                            registers_[reg_index].value.val_double);
            break;
          case NDB_TYPE_DOUBLE:
            registers_[reg_index].value.val_double =
                doubleget(reinterpret_cast<unsigned char*>(
                      &buf_[buf_pos_ + 1]));
            PA_INTERP_TRACE(frag_id_,
                            "Load NDB_TYPE_DOUBLE %lf",
                            registers_[reg_index].value.val_double);
            break;
          case NDB_TYPE_DECIMAL:
            assert(static_cast<Uint32>(decimal_bin_size(precision, scale)) ==
                AttributeDescriptor::getSizeInBytes(attrDescriptor[0]));
            // memset(decimal.buf, 0, sizeof(Int32) * DECIMAL_BUFF_LENGTH);
            dec_ret = bin2decimal(reinterpret_cast<const uchar*>(&buf_[buf_pos_ + 1]),
                      &decimal_, precision, scale);
            // assert(dec_ret == E_DEC_OK);
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&buf_[buf_pos_ + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while parsing decimal: ");
              for (Uint32 i = 0;
                  i < AttributeDescriptor::getSizeInBytes(attrDescriptor[0]); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              g_eventLogger->debug("%s", log_buf);
              if (dec_ret == E_DEC_OVERFLOW) {
                return ZAGG_DECIMAL_PARSE_OVERFLOW;
              } else {
                return ZAGG_DECIMAL_PARSE_ERROR;
              }
            }
            /*
             * Moz
             * convert from decimal to double or bigint.
             */
            assert(registers_[reg_index].is_unsigned == false);
            if (scale != 0) {
              assert(registers_[reg_index].type == NDB_TYPE_DOUBLE);
              dec_ret = decimal2double(&decimal_, &dec_val_dbl);
              registers_[reg_index].value.val_double = dec_val_dbl;
            } else {
              assert(registers_[reg_index].type == NDB_TYPE_BIGINT);
              dec_ret = decimal2longlong(&decimal_, &dec_val_ll);
              registers_[reg_index].value.val_int64 = dec_val_ll;
            }
            // assert(dec_ret == E_DEC_OK);
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&buf_[buf_pos_ + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while converting decimal: ");
              for (Uint32 i = 0;
                  i < AttributeDescriptor::getSizeInBytes(attrDescriptor[0]); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              g_eventLogger->debug("%s", log_buf);
              if (dec_ret == E_DEC_OVERFLOW) {
                return ZAGG_DECIMAL_CONV_OVERFLOW;
              } else {
                return ZAGG_DECIMAL_CONV_ERROR;
              }
            }
#ifdef DEBUG_PA_INTERP
            if (scale != 0) {
              PA_INTERP_TRACE(frag_id_,
                              "Load NDB_TYPE_DECIMAL[double] %lf",
                              registers_[reg_index].value.val_double);
            } else {
              PA_INTERP_TRACE(frag_id_,
                              "Load NDB_TYPE_DECIMAL[int64] %lld",
                              registers_[reg_index].value.val_int64);
            }
#endif // DEBUG_PA_INTERP
          break;
        case NDB_TYPE_DECIMALUNSIGNED:
            assert(static_cast<Uint32>(decimal_bin_size(precision, scale)) ==
                AttributeDescriptor::getSizeInBytes(attrDescriptor[0]));
            // memset(decimal.buf, 0, sizeof(Int32) * DECIMAL_BUFF_LENGTH);
            dec_ret = bin2decimal(reinterpret_cast<const uchar*>(&buf_[buf_pos_ + 1]),
                      &decimal_, precision, scale);
            // assert(dec_ret == E_DEC_OK);
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&buf_[buf_pos_ + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while parsing decimal: ");
              for (Uint32 i = 0;
                  i < AttributeDescriptor::getSizeInBytes(attrDescriptor[0]); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              g_eventLogger->debug("%s", log_buf);
              if (dec_ret == E_DEC_OVERFLOW) {
                return ZAGG_DECIMAL_PARSE_OVERFLOW;
              } else {
                return ZAGG_DECIMAL_PARSE_ERROR;
              }
            }
            /*
             * Moz
             * convert from decimal unsigned to double or bigint.
             */
            assert(registers_[reg_index].is_unsigned == true);
            if(unlikely(decimal_.sign)) {
              return ZAGG_DECIMAL_CONV_ERROR;
            }
            if (scale != 0) {
              assert(registers_[reg_index].type == NDB_TYPE_DOUBLE);
              dec_ret = decimal2double(&decimal_, &dec_val_dbl);
              registers_[reg_index].value.val_double = dec_val_dbl;
            } else {
              assert(registers_[reg_index].type == NDB_TYPE_BIGINT);
              dec_ret = decimal2ulonglong(&decimal_, &dec_val_ull);
              registers_[reg_index].value.val_uint64 = dec_val_ull;
            }
            // assert(dec_ret == E_DEC_OK);
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&buf_[buf_pos_ + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while converting decimal: ");
              for (Uint32 i = 0;
                  i < AttributeDescriptor::getSizeInBytes(attrDescriptor[0]); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              g_eventLogger->debug("%s", log_buf);
              if (dec_ret == E_DEC_OVERFLOW) {
                return ZAGG_DECIMAL_CONV_OVERFLOW;
              } else {
                return ZAGG_DECIMAL_CONV_ERROR;
              }
            }
#ifdef DEBUG_PA_INTERP
            if (scale != 0) {
              PA_INTERP_TRACE(frag_id_,
                              "Load NDB_TYPE_DECIMALUNSIGNED[double] %lf",
                              registers_[reg_index].value.val_double);
            } else {
              PA_INTERP_TRACE(frag_id_,
                              "Load NDB_TYPE_DECIMALUNSIGEND[uint64] %llu",
                              registers_[reg_index].value.val_uint64);
            }
#endif // DEBUG_PA_INTERP
          break;

          default:
            // assert(0);
            return ZAGG_LOAD_COL_WRONG_TYPE;
        }
        break;
      case kOpLoadConst:
        type = (value & 0x03E00000) >> 21;
        reg_index = (value & 0x000F0000) >> 16;
        assert(type == NDB_TYPE_BIGINT || type == NDB_TYPE_BIGUNSIGNED ||
               type == NDB_TYPE_DOUBLE);
        ResetRegister(&registers_[reg_index]);
        registers_[reg_index].type = AlignedType(type, 0);
        registers_[reg_index].is_unsigned = IsUnsigned(type);
        registers_[reg_index].is_null = false;
        if (unlikely(exec_pos + 2 > prog_len_)) {
          g_eventLogger->warning("Pushdown aggregation program ended in the"
                                 " middle of an instruction.");
          return ZAGG_OTHER_ERROR;
        }
        switch (type) {
          case NDB_TYPE_BIGINT:
            registers_[reg_index].value.val_int64 =
                sint8korr(reinterpret_cast<char*>(&prog_[exec_pos]));
              PA_INTERP_TRACE(frag_id_,
                              "LoadConst[%u] NDB_TYPE_BIGINT %lld",
                              reg_index, registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_BIGUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                uint8korr(reinterpret_cast<char*>(&prog_[exec_pos]));
              PA_INTERP_TRACE(frag_id_,
                              "LoadConst[%u] "
                              "NDB_TYPE_BIGUNSIGNED %llu",
                              reg_index, registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_DOUBLE:
            registers_[reg_index].value.val_double =
                doubleget(reinterpret_cast<unsigned char*>(
                      &prog_[exec_pos]));
              PA_INTERP_TRACE(frag_id_,
                              "LoadConst[%u] NDB_TYPE_DOUBLE %lf",
                              reg_index, registers_[reg_index].value.val_double);
            break;
          default:
            // assert(0);
            return ZAGG_LOAD_CONST_WRONG_TYPE;
        }
        exec_pos += 2;
        break;
      case kOpMov:
        reg_index = (value >> 12 ) & 0x0F;
        reg_index2 = (value >> 8 ) & 0x0F;

        registers_[reg_index] = registers_[reg_index2];
        PA_INTERP_TRACE(frag_id_,
                        "Move [%u]->[%u]",
                        reg_index2, reg_index);
        break;
      case kOpSum:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Sum(registers_[reg_index], &agg_res_ptr[agg_index], debug_print);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[SUM], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpMax:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Max(registers_[reg_index], &agg_res_ptr[agg_index], debug_print);
        // assert(ret >= 0);
        break;
      case kOpMin:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Min(registers_[reg_index], &agg_res_ptr[agg_index], debug_print);
        // assert(ret >= 0);
        break;
      case kOpCount:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Count(registers_[reg_index], &agg_res_ptr[agg_index], debug_print);
        // assert(ret >= 0);
        break;

      // Type-specific Sum operations
      case kOpSumBigint:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);
        ret = SumBigint(registers_[reg_index], &agg_res_ptr[agg_index], debug_print);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[SumBigint], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      case kOpSumDouble:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);
        ret = SumDouble(registers_[reg_index], &agg_res_ptr[agg_index], debug_print);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[SumDouble], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      // Type-specific Max operations
      case kOpMaxBigint:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);
        ret = MaxBigint(registers_[reg_index], &agg_res_ptr[agg_index], debug_print);
        break;

      case kOpMaxDouble:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);
        ret = MaxDouble(registers_[reg_index], &agg_res_ptr[agg_index], debug_print);
        break;

      // Type-specific Min operations
      case kOpMinBigint:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);
        ret = MinBigint(registers_[reg_index], &agg_res_ptr[agg_index], debug_print);
        break;

      case kOpMinDouble:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);
        ret = MinDouble(registers_[reg_index], &agg_res_ptr[agg_index], debug_print);
        break;

      default:
        // assert(0);
        return ZAGG_WRONG_OPERATION;
    }
  }
  processed_rows_++;
  return 0;
}

void AggInterpreter::Print() {
  char log_buf[1024];
  if (n_gb_cols_) {
    if (gb_map_) {
      sprintf(log_buf, "Group by columns: [");
      for (Uint32 i = 0; i < n_gb_cols_; i++) {
        if (i != n_gb_cols_ - 1) {
          sprintf(log_buf + strlen(log_buf), "%u ", gb_cols_[i] >> 16);
        } else {
          sprintf(log_buf + strlen(log_buf), "%u", gb_cols_[i] >> 16);
        }
      }
      sprintf(log_buf + strlen(log_buf), "]");
      g_eventLogger->info("%s", log_buf);
      log_buf[0] = '\0';

      g_eventLogger->info("Num of groups: %lu, Aggregation results:",
                          gb_map_->size());
      for (auto iter = gb_map_->begin(); iter != gb_map_->end(); iter++) {
        int pos = 0;
        sprintf(log_buf, "(");
        for (Uint32 i = 0; i < n_gb_cols_; i++) {
          if (i != n_gb_cols_ - 1) {
            sprintf(log_buf + strlen(log_buf), "%u: %p, ", i, iter->first.ptr + pos);
          } else {
            sprintf(log_buf + strlen(log_buf), "%u: %p): ", i, iter->first.ptr + pos);
          }
        }

        AggResItem* item = reinterpret_cast<AggResItem*>(iter->second.ptr);
        for (Uint32 i = 0; i < n_agg_results_; i++) {
          sprintf(log_buf + strlen(log_buf), "(%u, %u, %u)", item[i].type,
                  item[i].is_unsigned, item[i].is_null);
          if (item[i].is_null) {
            sprintf(log_buf + strlen(log_buf), "[NULL]");
          } else {
            switch (item[i].type) {
              case NDB_TYPE_BIGINT:
                sprintf(log_buf + strlen(log_buf), "[%15lld]", item[i].value.val_int64);
                break;
              case NDB_TYPE_DOUBLE:
                sprintf(log_buf + strlen(log_buf), "[%31.16f]", item[i].value.val_double);
                break;
              default:
                assert(0);
            }
          }
        }
        g_eventLogger->info("%s", log_buf);
      }
    }
  } else {
    AggResItem* item = agg_results_;
    log_buf[0] = '\0';
    for (Uint32 i = 0; i < n_agg_results_; i++) {
      sprintf(log_buf + strlen(log_buf), "(%u, %u, %u)", item[i].type,
              item[i].is_unsigned, item[i].is_null);
      if (item[i].is_null) {
        sprintf(log_buf + strlen(log_buf), "[NULL]");
      } else {
        switch (item[i].type) {
          case NDB_TYPE_BIGINT:
            sprintf(log_buf + strlen(log_buf), "[%15lld]", item[i].value.val_int64);
            break;
          case NDB_TYPE_DOUBLE:
            sprintf(log_buf + strlen(log_buf), "[%31.16f]", item[i].value.val_double);
            break;
          default:
            assert(0);
        }
      }
    }
    g_eventLogger->info("%s", log_buf);
  }
}

// NOTICE: Need to define agg_ops[] before using this func.
void AggInterpreter::MergePrint(const AggInterpreter* in1,
                                   const AggInterpreter* in2) {
  assert(in1 != nullptr && in2 != nullptr);
  assert(in1->n_agg_results_ == in2->n_agg_results_);
  auto iter1 = in1->gb_map_->begin();
  auto iter2 = in2->gb_map_->begin();
  char log_buf[1024];
  log_buf[0] = '\0';

  while (iter1 != in1->gb_map_->end() && iter2 != in2->gb_map_->end()) {
    Uint32 len1 = iter1->first.len;
    Uint32 len2 = iter2->first.len;
#ifdef NDEBUG
    (void)len2;
#endif // NDEBUG
    assert(len1 == len2);

    int ret = memcmp(iter1->first.ptr, iter2->first.ptr, len1);
    if (ret < 0) {
      int pos = 0;
      sprintf(log_buf + strlen(log_buf), "(");
      for (Uint32 i = 0; i < in1->n_gb_cols_; i++) {
        if (i != in1->n_gb_cols_ - 1) {
          sprintf(log_buf + strlen(log_buf), "%u: %p, ", i, iter1->first.ptr + pos);
        } else {
          sprintf(log_buf + strlen(log_buf), "%u: %p): ", i, iter1->first.ptr + pos);
        }
      }
      AggResItem* item = reinterpret_cast<AggResItem*>(iter1->second.ptr);
      for (Uint32 i = 0; i < in1->n_agg_results_; i++) {
        sprintf(log_buf + strlen(log_buf), "(%u, %u, %u)", item[i].type,
            item[i].is_unsigned, item[i].is_null);
        if (item[i].is_null) {
          sprintf(log_buf + strlen(log_buf), "[NULL]");
        } else {
          switch (item[i].type) {
            case NDB_TYPE_BIGINT:
              sprintf(log_buf + strlen(log_buf), "[%15lld]", item[i].value.val_int64);
              break;
            case NDB_TYPE_DOUBLE:
              sprintf(log_buf + strlen(log_buf), "[%31.16f]", item[i].value.val_double);
              break;
            default:
              assert(0);
          }
        }
      }
      g_eventLogger->info("%s", log_buf);
      log_buf[0] = '\0';
      iter1++;
    } else if (ret > 0) {
      int pos = 0;
      sprintf(log_buf + strlen(log_buf), "(");
      for (Uint32 i = 0; i < in2->n_gb_cols_; i++) {
        if (i != in2->n_gb_cols_ - 1) {
          sprintf(log_buf + strlen(log_buf), "%u: %p, ", i, iter2->first.ptr + pos);
        } else {
          sprintf(log_buf + strlen(log_buf), "%u: %p): ", i, iter2->first.ptr + pos);
        }
      }
      AggResItem* item = reinterpret_cast<AggResItem*>(iter2->second.ptr);
      for (Uint32 i = 0; i < in2->n_agg_results_; i++) {
        sprintf(log_buf + strlen(log_buf), "(%u, %u, %u)", item[i].type,
            item[i].is_unsigned, item[i].is_null);
        if (item[i].is_null) {
          sprintf(log_buf + strlen(log_buf), "[NULL]");
        } else {
          switch (item[i].type) {
            case NDB_TYPE_BIGINT:
              sprintf(log_buf + strlen(log_buf), "[%15lld]", item[i].value.val_int64);
              break;
            case NDB_TYPE_DOUBLE:
              sprintf(log_buf + strlen(log_buf), "[%31.16f]", item[i].value.val_double);
              break;
            default:
              assert(0);
          }
        }
      }
      g_eventLogger->info("%s", log_buf);
      log_buf[0] = '\0';
      iter2++;
    } else {
      int pos = 0;
      sprintf(log_buf + strlen(log_buf), "(");
      for (Uint32 i = 0; i < in1->n_gb_cols_; i++) {
        if (i != in1->n_gb_cols_ - 1) {
          sprintf(log_buf + strlen(log_buf), "%u: %p, ", i, iter1->first.ptr + pos);
        } else {
          sprintf(log_buf + strlen(log_buf), "%u: %p): ", i, iter1->first.ptr + pos);
        }
      }
      AggResItem* item1 = reinterpret_cast<AggResItem*>(iter1->second.ptr);
      AggResItem* item2 = reinterpret_cast<AggResItem*>(iter2->second.ptr);
      AggResItem result;
      // NOTICE: Need to define agg_ops[] first.
      Uint32 agg_ops[32];
      for (Uint32 i = 0; i < in1->n_agg_results_; i++) {
        assert(((item1[i].type == NDB_TYPE_BIGINT &&
                item1[i].is_unsigned == item2[i].is_unsigned) ||
                item1[i].type == NDB_TYPE_DOUBLE) &&
                item1[i].type == item2[i].type);
        if (item1[i].is_null) {
          result = item2[i];
        } else if (item2[i].is_null) {
          result = item1[i];
        } else {
          result.type = item1[i].type;
          result.is_unsigned = item1[i].is_unsigned;
          switch (agg_ops[i]) {
            case kOpSum:
              if (item1[i].type == NDB_TYPE_BIGINT) {
                if (item1[i].is_unsigned) {
                  result.value.val_uint64 = (item1[i].value.val_uint64 +
                                                 item2[i].value.val_uint64);
                } else {
                  result.value.val_int64 = (item1[i].value.val_int64 +
                                                 item2[i].value.val_int64);
                }
              } else {
                assert(item1[i].type == NDB_TYPE_DOUBLE);
                result.value.val_double = (item1[i].value.val_double +
                                               item2[i].value.val_double);
              }
              break;
            case kOpCount:
              assert(item1[i].type == NDB_TYPE_BIGINT);
              assert(item1[i].is_unsigned == 1);
              result.value.val_int64 = (item1[i].value.val_int64 +
                                             item2[i].value.val_int64);
              break;
            case kOpMax:
              if (item1[i].type == NDB_TYPE_BIGINT) {
                if (item1[i].is_unsigned) {
                  result.value.val_uint64 =
                    item1[i].value.val_uint64 >= item2[i].value.val_uint64 ?
                    item1[i].value.val_uint64 : item2[i].value.val_uint64;
                } else {
                  result.value.val_int64 =
                    item1[i].value.val_int64 >= item2[i].value.val_int64 ?
                    item1[i].value.val_int64 : item2[i].value.val_int64;
                }
              } else {
                assert(item1[i].type == NDB_TYPE_DOUBLE);
                result.value.val_double =
                  item1[i].value.val_double >= item2[i].value.val_double ?
                  item1[i].value.val_double : item2[i].value.val_double;
              }
              break;
            case kOpMin:
              if (item1[i].type == NDB_TYPE_BIGINT) {
                if (item1[i].is_unsigned) {
                  result.value.val_uint64 =
                    item1[i].value.val_uint64 <= item2[i].value.val_uint64 ?
                    item1[i].value.val_uint64 : item2[i].value.val_uint64;
                } else {
                  result.value.val_int64 =
                    item1[i].value.val_int64 <= item2[i].value.val_int64 ?
                    item1[i].value.val_int64 : item2[i].value.val_int64;
                }
              } else {
                assert(item1[i].type == NDB_TYPE_DOUBLE);
                result.value.val_double =
                  item1[i].value.val_double <= item2[i].value.val_double ?
                  item1[i].value.val_double : item2[i].value.val_double;
              }
              break;
            default:
              assert(0);
              break;
          }
        }
        sprintf(log_buf + strlen(log_buf), "(%u, %u, %u)", result.type,
            result.is_unsigned, result.is_null);
        if (result.is_null) {
          sprintf(log_buf + strlen(log_buf), "[NULL]");
        } else {
          switch (result.type) {
            case NDB_TYPE_BIGINT:
              sprintf(log_buf + strlen(log_buf), "[%15lld]", result.value.val_int64);
              break;
            case NDB_TYPE_DOUBLE:
              sprintf(log_buf + strlen(log_buf), "[%31.16f]", result.value.val_double);
              break;
            default:
              assert(0);
          }
        }
      }
      g_eventLogger->info("%s", log_buf);
      log_buf[0] = '\0';
      iter1++;
      iter2++;
    }
  }
  while (iter1 != in1->gb_map_->end()) {
    int pos = 0;
    sprintf(log_buf + strlen(log_buf), "(");
    for (Uint32 i = 0; i < in1->n_gb_cols_; i++) {
      if (i != in1->n_gb_cols_ - 1) {
        sprintf(log_buf + strlen(log_buf), "%u: %p, ", i, iter1->first.ptr + pos);
      } else {
        sprintf(log_buf + strlen(log_buf), "%u: %p): ", i, iter1->first.ptr + pos);
      }
    }
    AggResItem* item = reinterpret_cast<AggResItem*>(iter1->second.ptr);
    for (Uint32 i = 0; i < in1->n_agg_results_; i++) {
      sprintf(log_buf + strlen(log_buf), "(%u, %u, %u)", item[i].type,
          item[i].is_unsigned, item[i].is_null);
      if (item[i].is_null) {
        sprintf(log_buf + strlen(log_buf), "[NULL]");
      } else {
        switch (item[i].type) {
          case NDB_TYPE_BIGINT:
            sprintf(log_buf + strlen(log_buf), "[%15lld]", item[i].value.val_int64);
            break;
          case NDB_TYPE_DOUBLE:
            sprintf(log_buf + strlen(log_buf), "[%31.16f]", item[i].value.val_double);
            break;
          default:
            assert(0);
        }
      }
    }
    g_eventLogger->info("%s", log_buf);
    log_buf[0] = '\0';
  }
  while (iter2 != in2->gb_map_->end()) {
    int pos = 0;
    sprintf(log_buf + strlen(log_buf), "(");
    for (Uint32 i = 0; i < in2->n_gb_cols_; i++) {
      if (i != in2->n_gb_cols_ - 1) {
        sprintf(log_buf + strlen(log_buf), "%u: %p, ", i, iter2->first.ptr + pos);
      } else {
        sprintf(log_buf + strlen(log_buf), "%u: %p): ", i, iter2->first.ptr + pos);
      }
    }
    AggResItem* item = reinterpret_cast<AggResItem*>(iter2->second.ptr);
    for (Uint32 i = 0; i < in2->n_agg_results_; i++) {
      sprintf(log_buf + strlen(log_buf), "(%u, %u, %u)", item[i].type,
          item[i].is_unsigned, item[i].is_null);
      if (item[i].is_null) {
        sprintf(log_buf + strlen(log_buf), "[NULL]");
      } else {
        switch (item[i].type) {
          case NDB_TYPE_BIGINT:
            sprintf(log_buf + strlen(log_buf), "[%15lld]", item[i].value.val_int64);
            break;
          case NDB_TYPE_DOUBLE:
            sprintf(log_buf + strlen(log_buf), "[%31.16f]", item[i].value.val_double);
            break;
          default:
            assert(0);
        }
      }
    }
    g_eventLogger->info("%s", log_buf);
    log_buf[0] = '\0';
    iter2++;
  }
}


Uint32 AggInterpreter::PrepareAggResIfNeeded(Signal* signal, bool force) {
  // Limitation
  Uint32 total_size = result_size_ +
                  (gb_map_ ?
                   gb_map_->size() * g_result_header_size_per_group_ : 0) +
                  g_result_header_size_;
  if (!force && (gb_map_ == nullptr ||
        total_size < DEF_AGG_RESULT_BATCH_BYTES)) {
    return 0;
  }
  if (force &&
      (n_gb_cols_ != 0 && (gb_map_ == nullptr || gb_map_->size() == 0))) {
    assert(result_size_ == 0);
    return 0;
  }
  Uint32* data_buf = (&signal->theData[25]);
  Uint32 pos = 0;
  assert(n_gb_cols_ < 0xFFFF);
  assert(n_agg_results_ < 0xFFFF);

  if (n_gb_cols_) {
    data_buf[pos++] = AttributeHeader::AGG_RESULT << 16 | 0x0721;
    data_buf[pos++] = n_gb_cols_ << 16 | n_agg_results_;
    data_buf[pos++] = gb_map_->size();
    for (auto iter = gb_map_->begin(); iter != gb_map_->end();) {
      assert(iter->first.len % 4 == 0 && iter->first.len < 0xFFFF);
      assert(iter->second.len % 4 == 0 && iter->second.len < 0xFFFF);
      data_buf[pos++] = iter->first.len << 16 | iter->second.len;
      assert(iter->first.ptr + (iter->first.len + iter->second.len) ==
          iter->second.ptr + iter->second.len);
      MEMCOPY_NO_WORDS(&data_buf[pos], iter->first.ptr,
          (iter->first.len + iter->second.len) >> 2);
      pos += ((iter->first.len + iter->second.len) >> 2);
#ifndef PA_MALLOC
      delete[] iter->first.ptr;
#endif // !PA_MALLOC
      gb_map_->erase(iter++);
      result_size_ = 0;
    }
#ifdef PA_MALLOC
    alloc_len_ = 0;
#endif // PA_MALLOC
    assert(gb_map_->empty());
  } else {
    data_buf[pos++] = AttributeHeader::AGG_RESULT << 16 | 0x0721;
    data_buf[pos++] = n_gb_cols_ << 16 | n_agg_results_;
    data_buf[pos++] = 0;
    data_buf[pos++] = 0 << 16 | (n_agg_results_ * sizeof(AggResItem));
    assert(gb_map_ == nullptr);
    MEMCOPY_NO_WORDS(&data_buf[pos], agg_results_,
        (n_agg_results_ * sizeof(AggResItem)) >> 2);
    pos += ((n_agg_results_ * sizeof(AggResItem)) >> 2);
  }

#if defined(PA_CHECK) && !defined(NDEBUG)
  /*
   * PA related
   * Validation
   */
  Uint32 data_len = pos;
  Uint32 parse_pos = 0;

  while (parse_pos < data_len) {
    AttributeHeader agg_checker_ah(data_buf[parse_pos++]);
    assert(agg_checker_ah.getAttributeId() == AttributeHeader::AGG_RESULT &&
           agg_checker_ah.getByteSize() == 0x0721);
    Uint32 n_gb_cols = data_buf[parse_pos] >> 16;
    Uint32 n_agg_results = data_buf[parse_pos++] & 0xFFFF;
    Uint32 n_res_items = data_buf[parse_pos++];
    // g_eventLogger->info("Moz, GB cols: %u, AGG results: %u, RES items: %u",
    //         n_gb_cols, n_agg_results, n_res_items);

    if (n_gb_cols) {
      // char log_buf[128];
      for (Uint32 i = 0; i < n_res_items; i++) {
        Uint32 gb_cols_len = data_buf[parse_pos] >> 16;
        Uint32 agg_res_len = data_buf[parse_pos++] & 0xFFFF;
        // remove compile warnings
        (void)gb_cols_len;
        (void)agg_res_len;
        for (Uint32 j = 0; j < n_gb_cols; j++) {
          AttributeHeader ah(data_buf[parse_pos++]);
          // sprintf(log_buf,
          //     "[id: %u, sizeB: %u, sizeW: %u, gb_len: %u, "
          //     "res_len: %u, value: ",
          //     ah.getAttributeId(), ah.getByteSize(),
          //     ah.getDataSize(), gb_cols_len, agg_res_len);
          assert(ah.getDataPtr() != &data_buf[parse_pos]);
          // char* ptr = (char*)(&data_buf[parse_pos]);
          // for (Uint32 i = 0; i < ah.getByteSize(); i++) {
          //   sprintf(log_buf + strlen(log_buf), " %x", ptr[i]);
          // }
          parse_pos += ah.getDataSize();
          // sprintf(log_buf + strlen(log_buf), "]");
        }
        for (Uint32 i = 0; i < n_agg_results; i++) {
          // AggResItem* ptr = (AggResItem*)(&data_buf[parse_pos]);
          // sprintf(log_buf + strlen(log_buf), "(type: %u, is_unsigned: %u, is_null: %u, value: ",
          //         ptr->type, ptr->is_unsigned, ptr->is_null);
          // switch (ptr->type) {
          //   case NDB_TYPE_BIGINT:
          //     sprintf(log_buf + strlen(log_buf), "%15ld", ptr->value.val_int64);
          //     break;
          //   case NDB_TYPE_DOUBLE:
          //     sprintf(log_buf + strlen(log_buf), "%31.16f", ptr->value.val_double);
          //     break;
          //   default:
          //     assert(0);
          // }
          // sprintf(log_buf + strlen(log_buf), ")");
          parse_pos += (sizeof(AggResItem) >> 2);
        }
        // g_eventLogger->info("%s", log_buf);
      }
    } else {
      assert(n_gb_cols == 0);
      assert(n_agg_results == n_agg_results_);
      assert(n_res_items == 0);
      Uint32 gb_cols_len = data_buf[parse_pos] >> 16;
      Uint32 agg_res_len = data_buf[parse_pos++] & 0xFFFF;
      assert(gb_cols_len == 0);
      assert(agg_res_len == n_agg_results_ * sizeof(AggResItem));
      parse_pos += (agg_res_len >> 2);
    }
  }
  assert(parse_pos == data_len);
#endif // PA_CHECK && !NDEBUG
  return pos;
}

Uint32 AggInterpreter::NumOfResRecords(bool last_time) {
  /*
   * Moz
   * NumOfResRecords is called after PrepareAggResIfNeeded
   * to see if there's no result left in the interpreter.
   * we use this return value to stop Dblqh::scanTupkeyRefLab
   * to send scanfragconf to TC wrongly
   * see [MOZ-COMMENT] there.
   */

  if (!last_time) {
    /*
     * if it's not the last time PrepareAggResIfNeeded,
     * here we can return the real value.
     * NOTICE:
     * always return 1 even if gb_map_ is empty().
     * In this situation: pushdown aggregation with filter and
     * group by. 99% rows has been filtered out which means
     * gb_map_ has big chance to stay empty. In order to stop
     * Dblqh::scanTupkeyRefLab send scanfragconf before aggregation
     * scan finishes. here return 1 to stop that.
     */
    if (gb_map_) {
      return (gb_map_->empty() ? 1 : gb_map_->size());
    } else {
      /*
       * In non-groupby mode, before we send the result to API
       * at the last time. we always return 1.
       * NOTICE:
       * In non-groupby mode, we still need to stop scanTupkeyRefLab
       * send scanfragconf wrongly.
       */
      return 1;
    }
  } else {
    /*
     * This is the last time we call PrepareAggResIfNeeded, so the
     * aggregation is going to finish.
     * We assert all results have been sent and return 0 here.
     */
    if (gb_map_) {
      assert(gb_map_->empty());
    }
    return 0;
  }
}

#ifdef PA_MALLOC
char* AggInterpreter::MemAlloc(Uint32 len) {
  if (alloc_len_ + len >= MAX_AGG_RESULT_BATCH_BYTES) {
    return nullptr;
  } else {
    char* ptr = &(mem_buf_[alloc_len_]);
    alloc_len_ += len;
    return ptr;
  }
}

void AggInterpreter::Destruct(AggInterpreter* ptr) {
  if (ptr == nullptr) {
    return;
  }
  ptr->~AggInterpreter();
  lc_ndbd_pool_free(ptr);
}
#endif // PA_MALLOC
Int32 AggInterpreter::CopyVecCandidateFromSignal(Signal* signal,
                                                Uint32 ToutBufIndex) {
  if (candidate_allocator_ == nullptr) {
    VS_INTERP_TRACE(table_id_, frag_id_,
                    "CandidateAllocator pre-allocating memory, "
                    "top_n: %u, vec_max_rec_size: %u, actual_rec_size: %u",
                    vec_top_n_, vec_max_rec_size_, ToutBufIndex);
#ifdef PA_MALLOC
    void* ca_mem = lc_ndbd_pool_malloc(
        sizeof(CandidateAllocator), RG_QUERY_MEMORY, thread_id_, false);
    if (ca_mem == nullptr) {
      g_eventLogger->error("Alloc mem for CandidateAllocator failed");
      return ZAGG_ALLOC_MEM_FAILED;
    }
    candidate_allocator_ = new (ca_mem) CandidateAllocator(
        vec_top_n_, vec_max_rec_size_, table_id_, frag_id_);
#else
    candidate_allocator_ = new CandidateAllocator(
        vec_top_n_, vec_max_rec_size_, table_id_, frag_id_);
#endif
    int ret = candidate_allocator_ -> Init(thread_id_);
    if (ret != 0) {
      return ret;
    }
  }
  // The actual record size can't be larger than the vec_max_rec_size_
  // TODO (Zhao)
  // handle this error
  assert(ToutBufIndex <= vec_max_rec_size_);

  if (vec_top_n_results_.size() >= vec_top_n_) {
    Candidate* knockout = vec_top_n_results_.top();
    VS_INTERP_TRACE(table_id_, frag_id_,
                    "Picked the candidate with distance %lf, idx: %d as the next knockout",
        knockout->distance_,
        knockout->idx_in_allocator_);
    vec_top_n_results_.pop();
    candidate_allocator_->set_next_index(knockout->idx_in_allocator_);
    // No need to delete 'knockout' — it will be reused by the next selected candidate.
    // delete knockout;
  }

  Candidate* selected = candidate_allocator_->
      Allocate(curr_distance_, &(signal->theData[25]), ToutBufIndex);
  if (selected == nullptr) {
    return -1;
  }
  vec_top_n_results_.push(selected);
  VS_INTERP_TRACE(table_id_, frag_id_,
                  "Push one candidate with distance %lf, [%lu/%u], idx_in_allocator: %u",
                  curr_distance_, vec_top_n_results_.size(), vec_top_n_,
                  selected->idx_in_allocator_);
  return 0;
}

void AggInterpreter::PrepareVecCandidates() {
  vec_top_n_results_final_.clear();
  while (!vec_top_n_results_.empty()) {
    vec_top_n_results_final_.push_back(vec_top_n_results_.top());
    vec_top_n_results_.pop();
  }
  next_send_idx_ = vec_top_n_results_final_.size() - 1;
  VS_INTERP_TRACE(table_id_, frag_id_,
                  "PrepareVecCandidates %lu",
                  vec_top_n_results_final_.size());
}

Uint32 AggInterpreter::CopyOneVecCandidateToSignal(Signal* signal) {
  if (next_send_idx_ >= 0) {
    Candidate* next_send = vec_top_n_results_final_[next_send_idx_];
    if (next_send->actual_buf_len_ > 3) {
      // Fast path
      AttributeHeader header = *(AttributeHeader*)(next_send->buf_ + next_send->actual_buf_len_ - 3);
      if (header.getAttributeId() == AttributeHeader::VEC_DISTANCE &&
          *(double*)(next_send->buf_ + next_send->actual_buf_len_ - 2) == 721.721) {
        /* Fill in the vec_closes_ to the reserved area which contains the magic word 721.721*/
        *(double*)(next_send->buf_ + next_send->actual_buf_len_ - 2) = next_send->distance_;
      }
    } else {
      // TODO (Zhao)
      // It doesn’t seem to work with index scans, since in an index scan
      // there is an NdbRecord (READ_PACKED) packet at the beginning of the result.
      Uint32 pos = 0;
      while (pos < next_send->actual_buf_len_) {
        AttributeHeader header = *(AttributeHeader*)(next_send->buf_ + pos);
        if (header.getAttributeId() == AttributeHeader::VEC_DISTANCE) {
#ifdef DEBUG_VS_INTERP
          double value = *(double*)(next_send->buf_ + pos + 1);
          VS_INTERP_TRACE(table_id_, frag_id_,
                          "CopyOneToSignalForSending, attributeId: %d, value: %lf",
                          header.getAttributeId(), value);
#endif  // DEBUG_VS_INTERP
          *(double*)(next_send->buf_ + pos + 1) = next_send->distance_;
        }
        pos += header.getDataSize() + 1;
      }
    }
    Uint32 copy_len = next_send->actual_buf_len_;
    if (next_send != nullptr && next_send->actual_buf_len_ != 0) {
      memcpy((void*)(&signal->theData[25]), next_send->buf_,
          next_send->actual_buf_len_ * sizeof(Uint32));
      VS_INTERP_TRACE(table_id_, frag_id_,
                      "CopyOneToSignalForSending, len: %u, idx: %d",
                      next_send->actual_buf_len_, next_send_idx_);
      vec_n_candidates_sent_++;
      vec_size_candidates_sent_ += next_send->actual_buf_len_;
    }

    vec_top_n_results_final_[next_send_idx_] = nullptr;
    next_send_idx_--;
    /*
     * Don’t release the memory here — CandidateAllocator will handle it.
     */
    // delete next_send;
    return copy_len;
  } else {
    return 0;
  }
}

void AggInterpreter::set_vec_max_rec_size(Uint32 size) {
	/*
	 * vec_max_rec_size_ is only used to calculate the preallocated memory size
	 * for candidate_allocator_, so it doesn’t make sense to set it
	 * after candidate_allocator_ has already been initialized.
	 */
  if (!candidate_allocator_) {
    // 10% bigger
    vec_max_rec_size_ = static_cast<int>(std::ceil(size * 1.1));
    VS_INTERP_TRACE(table_id_, frag_id_, "Adjust vec_max_rec_size: %u -> %u",
                    size, vec_max_rec_size_);
  }
}

Uint32 AggInterpreter::CandidateAllocator::g_max_results_size = 100 * 1024 * 1024; /*100 MB*/
Uint32 AggInterpreter::CandidateAllocator::g_segment_size = 1 * 1024 * 1024; /*1 MB*/

static inline size_t highest_power_of_two_leq(size_t x) {
  // x > 0
  return size_t(1) << (8 * sizeof(size_t) - 1 - __builtin_clzl(x));
}

Int32 AggInterpreter::CandidateAllocator::Init(Uint32 thread_id) {
  if (init_) {
    return 0;
  }

  if (total_size_ > g_max_results_size) {
    g_eventLogger->warning(
        "Vector search result size %lu exceeds max pool %u",
        total_size_, g_max_results_size);
    return ZAGG_VS_TOO_BIG_RESULT;
  }

  if (slot_size_ > g_segment_size) {
    g_eventLogger->error(
        "slot_size %lu > segment_size %u, cannot allocate candidates safely",
        slot_size_, g_segment_size);
    return ZAGG_VS_TOO_BIG_RESULT;
  }

  size_t raw = g_segment_size / slot_size_;
  assert(raw > 0);
  slots_per_full_segment_ = highest_power_of_two_leq(raw);
  shift_k_ = __builtin_ctzl(slots_per_full_segment_);
  // slots_per_full_segment_ = g_segment_size / slot_size_;
  // assert(slots_per_full_segment_ > 0);

  size_t full_segments = max_candidates_ / slots_per_full_segment_;
  size_t last_slots = max_candidates_ % slots_per_full_segment_;
  size_t last_size = last_slots * slot_size_;

  size_t num_segments = full_segments + (last_slots > 0 ? 1 : 0);

  VS_INTERP_TRACE(table_id_, frag_id_,
      "Init CandidateAllocator: slot_size=%lu, slots_per_full_seg=%lu, "
      "full_segments=%lu, last_slots=%lu",
      slot_size_, slots_per_full_segment_, full_segments, last_slots);

#ifdef PA_MALLOC
  assert(num_segments <= MAX_CANDIDATE_SEGMENTS);
#else
  segments_.reserve(num_segments);
#endif

  for (size_t i = 0; i < full_segments; i++) {
#ifdef PA_MALLOC
    void* page_ptr = lc_ndbd_pool_malloc(g_segment_size, RG_QUERY_MEMORY,
        thread_id, false);
    if (!page_ptr) {
      g_eventLogger->error("Failed allocating segment %lu", i);
      return ZAGG_ALLOC_MEM_FAILED;
    }
    segments_[n_segments_++] = {(char*)page_ptr, g_segment_size};
#else
    char* buf = (char*)::operator new(g_segment_size);
    segments_.push_back({buf, g_segment_size});
#endif // PA_MALLOC
  }

  if (last_size > 0) {
#ifdef PA_MALLOC
    void* page_ptr = lc_ndbd_pool_malloc(last_size, RG_QUERY_MEMORY,
        thread_id, false);
    if (!page_ptr) {
      g_eventLogger->error("Failed allocating last segment");
      return ZAGG_ALLOC_MEM_FAILED;
    }
    segments_[n_segments_++] = {(char*)page_ptr, last_size};
#else
    char* buf = (char*)::operator new(last_size);
    segments_.push_back({buf, last_size});
#endif // PA_MALLOC
  }

  init_ = true;
  return 0;
}

AggInterpreter::Candidate* AggInterpreter::CandidateAllocator::Allocate(
    double distance, const Uint32* tuple, Uint32 actual_buf_len) {
  if (next_index_ >= max_candidates_) {
    return nullptr;
  }

  // size_t seg_id = next_index_ / slots_per_full_segment_;
  // size_t slot_off = next_index_ % slots_per_full_segment_;
	size_t seg_id   = next_index_ >> shift_k_;
	size_t slot_off = next_index_ & (slots_per_full_segment_ - 1);

#ifdef PA_MALLOC
  assert(seg_id < n_segments_);
#else
  assert(seg_id < segments_.size());
#endif

  char* base = segments_[seg_id].ptr;
  char* ptr = base + slot_off * slot_size_;

  assert(ptr + slot_size_ <= base + segments_[seg_id].size);

  VS_INTERP_TRACE(table_id_, frag_id_,
      "Alloc idx=%lu -> seg=%lu slot_off=%lu",
      next_index_, seg_id, slot_off);

  Candidate* c = new (ptr) Candidate(distance, actual_buf_len, next_index_);
  c->Init(tuple);

  if (!reuse_started_) {
    ++next_index_;
  }
  if (!reuse_started_ && next_index_ == max_candidates_) {
    VS_INTERP_TRACE(table_id_, frag_id_, "CandidateAllocator reuse started");
    next_index_ = 0;
    reuse_started_ = true;
  }

  return c;
}
