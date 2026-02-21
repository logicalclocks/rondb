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
#include "util/require.h"
#include "decimal.h"
#include "Dbtup.hpp"
#include "../dblqh/Dblqh.hpp"
#include <NdbSqlUtil.hpp>
#include <Interpreter.hpp>

#include <simsimd/simsimd.h>

Uint32 AggInterpreter::g_attr_read_buf_len_ = ATTR_READ_BUF_WORD_SIZE;
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
// Per-group header prepended by allocGroupData.
// Layout: [chunk_next (char*, 8)] [hash_next (char*, 8)] [key_len (Uint32, 4)] [chunk_offset (Uint32, 4)]
static const Uint32 GROUP_LINK_OVERHEAD = 24;

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
  [[maybe_unused]] const char *end1 = n1.ptr + n1.len;
  [[maybe_unused]] const char *end2 = n2.ptr + n2.len;

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
#if (defined(VM_TRACE) || defined(ERROR_INSERT))
#undef DEBUG_PA_INTERP
// #define DEBUG_PA_INTERP 1
#define DEBUG_AGG 1
#endif
#define DEBUG_PA_INTERP_PART_ID 0

#ifdef DEBUG_AGG
#define DEB_AGG(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_AGG(arglist) do { } while (0)
#endif

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

/**
 * validateEmbeddedProgram — validate an embedded old-interpreter program
 * at Init() time ("compile" step).
 *
 * Walks the instruction stream and checks:
 * 1. All opcodes are in the allowed whitelist (no CALL, RETURN, EXIT_REFUSE,
 *    WRITE_ATTR, heap memory ops)
 * 2. All branch offsets are forward-only (bit 31 = 0)
 * 3. All branch targets fall within the embedded program bounds
 * 4. Instruction lengths computed via getInstructionPreProcessingInfo are valid
 *
 * Returns true if the program is safe to execute in interpreterNextLab.
 */
bool AggInterpreter::validateEmbeddedProgram(
    const Uint32* emb_prog, Uint32 emb_len) {
  Uint32 pc = 0;
  while (pc < emb_len) {
    Uint32 instr = emb_prog[pc];
    Uint32 opCode = Interpreter::getOpCode(instr);

    /* Check opcode whitelist */
    switch (opCode) {
      /* Load/store register operations */
      case Interpreter::READ_ATTR_INTO_REG:
      case Interpreter::LOAD_CONST_NULL:
      case Interpreter::LOAD_CONST16:
      case Interpreter::LOAD_CONST32:
      case Interpreter::LOAD_CONST64:
      /* Arithmetic */
      case Interpreter::ADD_REG_REG:
      case Interpreter::SUB_REG_REG:
      /* Unconditional branch */
      case Interpreter::BRANCH:
      /* Null-check branches */
      case Interpreter::BRANCH_REG_EQ_NULL:
      case Interpreter::BRANCH_REG_NE_NULL:
      /* Register comparison branches */
      case Interpreter::BRANCH_EQ_REG_REG:
      case Interpreter::BRANCH_NE_REG_REG:
      case Interpreter::BRANCH_LT_REG_REG:
      case Interpreter::BRANCH_LE_REG_REG:
      case Interpreter::BRANCH_GT_REG_REG:
      case Interpreter::BRANCH_GE_REG_REG:
      /* Exit */
      case Interpreter::EXIT_OK:
      /* Column comparison branches */
      case Interpreter::BRANCH_ATTR_OP_ARG:
      case Interpreter::BRANCH_ATTR_EQ_NULL:
      case Interpreter::BRANCH_ATTR_NE_NULL:
      /* Output for skip_offset communication */
      case Interpreter::WRITE_INTERPRETER_OUTPUT:
        break;  /* Allowed */

      default:
        g_eventLogger->warning(
            "validateEmbeddedProgram: forbidden opcode %u at pc=%u",
            opCode, pc);
        return false;
    }

    /* Validate branch targets: forward-only, within bounds */
    bool is_branch = false;
    switch (opCode) {
      case Interpreter::BRANCH:
      case Interpreter::BRANCH_REG_EQ_NULL:
      case Interpreter::BRANCH_REG_NE_NULL:
      case Interpreter::BRANCH_EQ_REG_REG:
      case Interpreter::BRANCH_NE_REG_REG:
      case Interpreter::BRANCH_LT_REG_REG:
      case Interpreter::BRANCH_LE_REG_REG:
      case Interpreter::BRANCH_GT_REG_REG:
      case Interpreter::BRANCH_GE_REG_REG:
      case Interpreter::BRANCH_ATTR_OP_ARG:
      case Interpreter::BRANCH_ATTR_EQ_NULL:
      case Interpreter::BRANCH_ATTR_NE_NULL:
        is_branch = true;
        break;
      default:
        break;
    }

    if (is_branch) {
      Uint32 direction = instr >> 31;
      if (direction != 0) {
        g_eventLogger->warning(
            "validateEmbeddedProgram: backward branch at pc=%u", pc);
        return false;
      }
      Uint32 offset = (instr >> 16) & 0x7FFF;
      /* Target = pc + offset (brancher logic: TprogramCounter-- then + offset) */
      Uint32 target = pc + offset;
      if (target >= emb_len) {
        g_eventLogger->warning(
            "validateEmbeddedProgram: branch target %u out of bounds "
            "(emb_len=%u) at pc=%u", target, emb_len, pc);
        return false;
      }
    }

    /* Advance to next instruction using getInstructionPreProcessingInfo */
    Interpreter::InstructionPreProcessing processing;
    Uint32* next = Interpreter::getInstructionPreProcessingInfo(
        const_cast<Uint32*>(&emb_prog[pc]), processing);
    if (next == nullptr) {
      g_eventLogger->warning(
          "validateEmbeddedProgram: invalid instruction at pc=%u", pc);
      return false;
    }
    Uint32 instr_len = (Uint32)(next - &emb_prog[pc]);
    pc += instr_len;
  }

  return true;
}

bool AggInterpreter::Init(const Uint32* prog) {
  if (m_inited) {
    return true;
  }

  require(prog != nullptr);

  /*
   * Allocate all large buffers in a single block to keep
   * sizeof(AggInterpreter) small (must fit in MEM_CHUNK_SIZE).
   */
  if (m_buf_block == nullptr) {
    static const Uint32 BUF_BLOCK_SIZE =
      ATTR_READ_BUF_WORD_SIZE * sizeof(Uint32) +
      MAX_AGG_PROGRAM_WORD_SIZE * sizeof(Uint32) +
      MAX_AGG_N_GROUPBY_COLS * sizeof(Uint32) +
      MAX_AGG_N_RESULTS * sizeof(AggResItem) +
      sizeof(GBHashTable) +
      MAX_AGG_RESULT_BATCH_BYTES +
      MAX_AGG_N_GROUPBY_COLS * sizeof(GBColTypeInfo) +
      MAX_AGG_N_RESULTS * sizeof(Uint8);

    m_buf_block = lc_ndbd_pool_malloc(BUF_BLOCK_SIZE, RG_QUERY_MEMORY,
                                       m_thread_id, false);
    if (m_buf_block == nullptr) {
      g_eventLogger->error("Alloc mem for AggInterpreter buffers failed");
      return false;
    }

    char* p = static_cast<char*>(m_buf_block);
    m_attr_read_buf = reinterpret_cast<Uint32*>(p);
    p += ATTR_READ_BUF_WORD_SIZE * sizeof(Uint32);
    m_prog_buf = reinterpret_cast<Uint32*>(p);
    p += MAX_AGG_PROGRAM_WORD_SIZE * sizeof(Uint32);
    m_gb_cols_buf = reinterpret_cast<Uint32*>(p);
    p += MAX_AGG_N_GROUPBY_COLS * sizeof(Uint32);
    m_agg_results_buf = reinterpret_cast<AggResItem*>(p);
    p += MAX_AGG_N_RESULTS * sizeof(AggResItem);
    m_gb_map_buf = new (p) GBHashTable();
    p += sizeof(GBHashTable);
    m_mem_buf = p;
    p += MAX_AGG_RESULT_BATCH_BYTES;
    m_gb_types = reinterpret_cast<GBColTypeInfo*>(p);
    p += MAX_AGG_N_GROUPBY_COLS * sizeof(GBColTypeInfo);
    m_cached_agg_ops = reinterpret_cast<Uint8*>(p);
    memset(m_gb_types, 0, MAX_AGG_N_GROUPBY_COLS * sizeof(GBColTypeInfo));
  }

  /* 0. Prepare the buffer and copy the program */
  assert(m_prog_len <= MAX_VEC_SEARCH_PROGRAM_WORD_SIZE);
  if (m_prog_len <= MAX_AGG_PROGRAM_WORD_SIZE) {
    m_prog = m_prog_buf;
  } else {
    /* Use external buf for large-dimension vector search queries. */
    void* page_ptr = lc_ndbd_pool_malloc(32 * 1024, RG_QUERY_MEMORY,
        m_thread_id, false);
    if (page_ptr == nullptr) {
      g_eventLogger->error("Alloc mem for pushdown vector search interpreter failed");
      return false;
    }
    m_ext_prog_buf = static_cast<Uint32*>(page_ptr);
    m_prog = m_ext_prog_buf;
  }
  m_alloc_len = 0;
  memcpy(m_prog, prog, m_prog_len * sizeof(Uint32));
  memset(m_attr_read_buf, 0, ATTR_READ_BUF_WORD_SIZE * sizeof(Uint32));
  memset(m_decimal_buf, 0, sizeof(Int32) * DECIMAL_BUFF_LENGTH);
  m_decimal.buf = m_decimal_buf;
  m_decimal.len = DECIMAL_BUFF_LENGTH;


  Uint32 value = 0;
  /*
   * 1. Double check the magic num and  total length of program.
   */
  value = m_prog[m_cur_pos++];
  assert(((value & 0xFFFF0000) >> 16) == 0x0721);
  assert((value & 0xFFFF) == m_prog_len);

  /*
   * 2. Get num of columns for group by and num of aggregation results;
   */
  value = m_prog[m_cur_pos++];
  m_n_gb_cols = (value >> 16) & 0xFFFF;
  m_n_agg_results = value & 0xFFFF;

  Uint32 version = m_prog[m_cur_pos++];
  if (version > PUSHDOWN_AGGREGATION_VERSION) {
    g_eventLogger->warning("Pushdown aggregation program version(%u) is "
                           "not compatible with "
                           "the version (%u) on data node",
                           version, PUSHDOWN_AGGREGATION_VERSION);
    /*
     * Return with m_inited = false, and
     * ProcessRec() will handle this incompatible issue.
     */
    return true;
  }

  if (m_prog[m_cur_pos] & 0x80000000) {
    m_vec_search = true;
    assert((m_prog[m_cur_pos] & 0x7FFFFFFF) == 0);
    m_cur_pos++;
  } else {
    assert(m_vec_search == false);
    // Skip the next 5 reserved Uint32 elements
    assert(m_prog[m_cur_pos] == 0);
    m_cur_pos += 5;
  }

  if (m_vec_search) {
    value = m_prog[m_cur_pos++];
    m_vec_type = (value >> 24) & 0xFF;
    m_vec_metric = (value >> 16) & 0xFF;
    m_vec_dims = value & 0xFFFF;

    value = m_prog[m_cur_pos++];
    m_vec_top_n = (value) & 0xFFFF;
    m_vec_col_idx = (value >> 16) & 0xFFFF;
    value = m_prog[m_cur_pos++];
    m_vec_size_in_bytes = (value & 0xFFFFFFFF);
    // g_eventLogger->info("frag_id: %lld, type: %u, metric: %u, dims: %u, vec_col_idx: %u, vec_top_n: %u, size: %u\n",
    //     m_frag_id, m_vec_type, m_vec_metric, m_vec_dims, m_vec_col_idx, m_vec_top_n, m_vec_size_in_bytes);

    m_vec_start_pos = m_cur_pos;

    void* page_ptr = lc_ndbd_pool_malloc(32 * 1024, RG_QUERY_MEMORY,
        m_thread_id, false);
    if (page_ptr == nullptr) {
      g_eventLogger->error("Alloc mem for pushdown vector search interpreter failed");
      return false;
    }
    m_vec_buf = static_cast<Uint32*>(page_ptr);

    m_inited = true;
    return true;
  }

  /*
   * 3. Get all the group by columns id.
   */
  if (m_n_gb_cols) {
    assert(m_n_gb_cols <= MAX_AGG_N_GROUPBY_COLS);
    m_gb_cols = m_gb_cols_buf;

    Uint32 i = 0;
    while (i < m_n_gb_cols && m_cur_pos < m_prog_len) {
      m_gb_cols[i++] = m_prog[m_cur_pos++];
    }
    gb_cmp_ctx_.n_cols = 0;
    gb_cmp_ctx_.all_binary_cmp = false;
    m_gb_map_buf = std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>(
                     GBHashEntryCmp(&gb_cmp_ctx_));
    m_gb_map = m_gb_map_buf;
    m_gb_map->init(GB_HASH_BUCKET_COUNT);
  }

  /*
   * 4. Reset all aggregation results
   */
  if (m_n_agg_results) {
    assert(m_n_agg_results <= MAX_AGG_N_RESULTS);
    m_agg_results = m_agg_results_buf;
    Uint32 i = 0;
    while (i < m_n_agg_results) {
      m_agg_results[i].type = NDB_TYPE_UNDEFINED;
      m_agg_results[i].value.val_int64 = 0;
      m_agg_results[i].is_unsigned = false;
      m_agg_results[i].is_null = true;
      i++;
    }
  }

  m_inited = true;
  m_agg_prog_start_pos = m_cur_pos;
  memset(m_registers, 0, sizeof(m_registers));

  /*
   * 5. Validate any embedded interpreter blocks in the aggregation program.
   *    This "compiles" the embedded programs to ensure they will execute
   *    correctly in interpreterNextLab.
   */
  {
    Uint32 scan_pos = m_agg_prog_start_pos;
    while (scan_pos < m_prog_len) {
      Uint32 w = m_prog[scan_pos];
      Uint8 op = (w & 0xFC000000) >> 26;
      if (op == kOpEmbeddedInterp) {
        Uint32 emb_len = w & 0xFFFF;
        if (scan_pos + 1 + emb_len > m_prog_len ||
            !validateEmbeddedProgram(&m_prog[scan_pos + 1], emb_len)) {
          g_eventLogger->warning(
              "AggInterpreter::Init: embedded program validation failed "
              "at scan_pos=%u", scan_pos);
          m_inited = false;
          return false;
        }
        scan_pos += 1 + emb_len;  /* skip header + embedded words */
      } else if (op == kOpLoadConst) {
        scan_pos += 3;  /* header + 2 constant value words */
      } else if (op == kOpLoadCol) {
        Uint32 type = (w & 0x03E00000) >> 21;
        scan_pos += (type == NDB_TYPE_DECIMAL ||
                     type == NDB_TYPE_DECIMALUNSIGNED) ? 2 : 1;
      } else {
        scan_pos++;
      }
    }
  }

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
  if (!m_inited) {
    return false;
  }

  // Track the type of each register: NDB_TYPE_BIGINT, NDB_TYPE_DOUBLE, or NDB_TYPE_UNDEFINED
  DataType reg_types[kRegTotal];
  for (Uint32 i = 0; i < kRegTotal; i++) {
    reg_types[i] = NDB_TYPE_UNDEFINED;
  }

  // Single pass: analyze and rewrite the program
  Uint32 exec_pos = m_agg_prog_start_pos;

  while (exec_pos < m_prog_len) {
    Uint32 value = m_prog[exec_pos];
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
        m_prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
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
        m_prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
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
        m_prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        break;

      case kOpDiv:
        reg_index = (value >> 12) & 0x0F;
        // Division always produces double
        new_op = kOpDivDouble;
        reg_types[reg_index] = NDB_TYPE_DOUBLE;
        m_prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        break;

      case kOpDivInt:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        if (reg_types[reg_index] != NDB_TYPE_UNDEFINED &&
            reg_types[reg_index2] != NDB_TYPE_UNDEFINED) {
          new_op = kOpDivIntBigint;
        }
        reg_types[reg_index] = NDB_TYPE_BIGINT;
        m_prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
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
          m_prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
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
          m_prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
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
          m_prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        }
        break;

      case kOpCount:
        // Count always produces BIGINT - no optimization needed
        break;

      case kOpEmbeddedInterp:
      {
        Uint32 emb_len = value & 0xFFFF;
        exec_pos += emb_len;  /* skip embedded words; exec_pos++ below adds 1 */
        break;
      }

      case kOpSkip:
        break;  /* 1-word instruction, exec_pos++ below handles it */

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
  // assert(m_inited);
  // assert(req_struct->read_length == 0);
  if (!m_inited || req_struct->read_length != 0) {
    g_eventLogger->debug("AggInterpreter::ProcessRec ZAGG_OTHER_ERROR at entry: "
            "inited=%d, read_length=%u",
            m_inited, req_struct->read_length);
    return ZAGG_OTHER_ERROR;
  }

  *vec_update_candidate = false;
  if (unlikely(m_vec_search)) {
    Uint32 vec_col_idx = (m_vec_col_idx & 0x0000FFFF) << 16;
    int ret = block_tup->readAttributes(req_struct, &(vec_col_idx), 1,
                  m_vec_buf + m_vec_buf_pos, g_vec_buf_len_ - m_vec_buf_pos);
    if (ret < 0) {
      g_eventLogger->debug("read vector column error: %d", ret);
      return -ret;
    }
    AttributeHeader* header = nullptr;
    header = reinterpret_cast<AttributeHeader*>(m_vec_buf + m_vec_buf_pos);
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
    VS_INTERP_TRACE(m_table_id, m_frag_id,
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
      assert(m_vec_dims == dims / sizeof(float));
    } else {
      assert(0);
    }

    double distance = 0;
    float* target = (float* )&(m_prog[m_vec_start_pos]);
    float* current = (float* )((char*)header->getDataPtr() + length_bytes);
    simsimd_l2sq_f32(current, target, m_vec_dims, &distance);
    // simsimd_l2sq_f32_serial(current, target, m_vec_dims, &distance);
    m_curr_distance = distance;
    if (m_vec_top_n != 0 &&
        (m_vec_top_n_results.size() < m_vec_top_n ||
        distance < m_vec_top_n_results.top()->m_distance)) {
      *vec_update_candidate = true;
    }

    return 0;
  }

  AggResItem* agg_res_ptr = nullptr;
  if (m_n_gb_cols) {
    if (!m_gb_types_inited) {
      Int32 err = initGBTypes(block_tup, req_struct);
      if (unlikely(err != 0)) return err;
    }
    char* agg_rec = nullptr;

    AttributeHeader* header = nullptr;
    m_attr_read_pos = 0;
    for (Uint32 i = 0; i < m_n_gb_cols; i++) {
      Uint32 attr_id = m_gb_cols[i] >> 16;
      if ((attr_id & 0x8000) != 0 && m_linked_attr_data != nullptr) {
        /*
         * Linked GROUP BY column from a parent table in the join tree.
         * The lower 15 bits encode a 0-based position index into the
         * linked attribute buffer (not a table attrId, since attrIds
         * from different tables can collide in multi-table joins).
         */
        Uint32 position = attr_id & 0x7FFF;
        const Uint32* p = m_linked_attr_data;
        const Uint32* p_end = m_linked_attr_data + m_linked_attr_len;
        Uint32 pos_count = 0;
        while (p < p_end) {
          if (pos_count == position) break;
          p += 2;  // skip tableId + tableVersion
          p += 1 + AttributeHeader::getDataSize(*p);  // skip AH + data
          pos_count++;
        }
        if (p >= p_end) {
          g_eventLogger->debug("AggInterpreter::ProcessRec ZAGG_OTHER_ERROR: "
              "Linked GROUP BY position %u not found in linked buffer "
              "(linked_len=%u)", position, m_linked_attr_len);
          return ZAGG_OTHER_ERROR;
        }
        p += 2;  // skip tableId + tableVersion of found entry
        Uint32 words = 1 + AttributeHeader::getDataSize(*p);
        memcpy(m_attr_read_buf + m_attr_read_pos, p, words * sizeof(Uint32));
        header = reinterpret_cast<AttributeHeader*>(m_attr_read_buf + m_attr_read_pos);
        m_attr_read_pos += words;
      } else {
        int ret = block_tup->readAttributes(req_struct, &(m_gb_cols[i]), 1,
                      m_attr_read_buf + m_attr_read_pos, g_attr_read_buf_len_ - m_attr_read_pos);
        if (ret < 0) {
          DEB_AGG(("read group by column error: %d", ret));
          return -ret;
        }
        header = reinterpret_cast<AttributeHeader*>(m_attr_read_buf + m_attr_read_pos);
        m_attr_read_pos += (1 + header->getDataSize());
      }
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

    Uint32 len_in_char = m_attr_read_pos * sizeof(Uint32);
    char* found = m_gb_map->find(reinterpret_cast<char*>(m_attr_read_buf), len_in_char);
    if (found != nullptr) {
      header = reinterpret_cast<AttributeHeader*>(found);
      agg_res_ptr = reinterpret_cast<AggResItem*>(found + len_in_char);
      PA_INTERP_TRACE(m_frag_id,
                      "Found GBHashEntry, id: %u, byte_size: %u, "
                      "data_size: %u, is_null: %u",
                      header->getAttributeId(), header->getByteSize(),
                      header->getDataSize(), header->isNULL());
    } else {
      /* New group: check eviction limit before allocating. */
      if (m_max_groups > 0 && m_n_groups >= m_max_groups) {
        return AGG_EVICT_NEEDED;
      }
      /*
       * update req_struct->read_length here, which will update the
       * Dblqh::ScanRecord::m_curr_batch_size_bytes later in the
       * Dblqh::scanTupkeyConfLab, even we don't use that variable
       * to decide whether reaches batch limitation. Only increase
       * Dblqh::ScanRecord::m_curr_batch_size_bytes when new group
       * item is inserted into m_gb_map.
       * For aggregation,
       * we use Dblqh::ScanRecord::m_agg_curr_batch_size_bytes to
       * indicate batch limitation
       */
      req_struct->read_length = (len_in_char +
                       m_n_agg_results * sizeof(AggResItem)) / sizeof(Int32);

      // we use m_result_size to decide whether need to send some aggregation
      // results to API.
      m_result_size += len_in_char +
                       m_n_agg_results * sizeof(AggResItem);
      agg_rec = allocGroupData(len_in_char +
                               m_n_agg_results * sizeof(AggResItem),
                               len_in_char);
      if (agg_rec == nullptr) {
        return AGG_EVICT_NEEDED;
      }
      memset(agg_rec, 0, len_in_char +
                        m_n_agg_results * sizeof(AggResItem));
      memcpy(agg_rec, reinterpret_cast<char*>(m_attr_read_buf), len_in_char);

      m_gb_map->insert(agg_rec, len_in_char);
      m_n_groups = m_gb_map->size();
      agg_res_ptr = reinterpret_cast<AggResItem*>(agg_rec + len_in_char);

      // Initialize the new group's aggregation slots
      assert(m_n_agg_results <= MAX_AGG_N_RESULTS);
      for (Uint32 i = 0; i < m_n_agg_results; i++) {
        agg_res_ptr[i].type = NDB_TYPE_UNDEFINED;
        agg_res_ptr[i].value.val_int64 = 0;
        agg_res_ptr[i].is_unsigned = false;
        agg_res_ptr[i].is_null = true;
        assert(agg_res_ptr[i].type == m_agg_results[i].type);
        assert(agg_res_ptr[i].value.val_int64 == m_agg_results[i].value.val_int64);
        assert(agg_res_ptr[i].is_unsigned == m_agg_results[i].is_unsigned);
        assert(agg_res_ptr[i].is_null == m_agg_results[i].is_null);
      }
    }
  } else {
    agg_res_ptr = m_agg_results;
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

  Uint32 exec_pos = m_agg_prog_start_pos;
  bool debug_print = (m_frag_id == DEBUG_PA_INTERP_PART_ID);
  while (exec_pos < m_prog_len) {
    value = m_prog[exec_pos++];
    Uint8 op = (value & 0xFC000000) >> 26;
    int ret = 0;
    m_attr_read_pos = 0;
    AttributeHeader* header = nullptr;

    switch (op) {
      case kOpPlus:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegPlusReg(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) {
          DEB_AGG(("Overflow[PLUS], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpMinus:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegMinusReg(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) {
          DEB_AGG(("Overflow[MINUS], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpMul:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegMulReg(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) {
          DEB_AGG(("Overflow[MUL], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpDiv:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegDivReg(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index], false);
        if (ret < 0) {
          DEB_AGG(("Overflow[DIV], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpDivInt:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegDivReg(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index], true);
        if (ret < 0) {
          DEB_AGG(("Overflow[DIVINT], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpMod:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegModReg(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) {
          DEB_AGG(("Overflow[MOD], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      // Type-specific Plus operations
      case kOpPlusBigint:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegPlusBigint(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) {
          DEB_AGG(("Overflow[PlusBigint], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      case kOpPlusDouble:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegPlusDouble(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) {
          DEB_AGG(("Overflow[PlusDouble], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      // Type-specific Minus operations
      case kOpMinusBigint:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegMinusBigint(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) {
          DEB_AGG(("Overflow[MinusBigint], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      case kOpMinusDouble:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegMinusDouble(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) {
          DEB_AGG(("Overflow[MinusDouble], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      // Type-specific Multiply operations
      case kOpMulBigint:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegMulBigint(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) {
          DEB_AGG(("Overflow[MulBigint], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      case kOpMulDouble:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegMulDouble(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) {
          DEB_AGG(("Overflow[MulDouble], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      // Type-specific Division operations
      case kOpDivDouble:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegDivDouble(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) {
          DEB_AGG(("Overflow[DivDouble], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      case kOpDivIntBigint:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegDivIntBigint(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) {
          DEB_AGG(("Overflow[DivIntBigint], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      case kOpLoadCol:
        type = (value & 0x03E00000) >> 21;
        is_unsigned = IsUnsigned(type);
        reg_index = (value & 0x000F0000) >> 16;
        {
          Uint32 col_id_raw = value & 0x0000FFFF;
          if ((col_id_raw & 0x8000) != 0 && m_linked_attr_data != nullptr) {
            /*
             * Linked column from a parent table in the join tree.
             * The lower 15 bits encode a 0-based position index into the
             * linked attribute buffer (not a table attrId, since attrIds
             * from different tables can collide in multi-table joins).
             */
            Uint32 position = col_id_raw & 0x7FFF;
            const Uint32* p = m_linked_attr_data;
            const Uint32* p_end = m_linked_attr_data + m_linked_attr_len;
            Uint32 pos_count = 0;
            while (p < p_end) {
              if (pos_count == position) break;
              p += 2;  // skip tableId + tableVersion
              p += 1 + AttributeHeader::getDataSize(*p);  // skip AH + data
              pos_count++;
            }
            if (p >= p_end) {
              g_eventLogger->debug("AggInterpreter::ProcessRec ZAGG_OTHER_ERROR: "
                  "kOpLoadCol linked position %u not found in buffer "
                  "(linked_len=%u)", position, m_linked_attr_len);
              return ZAGG_OTHER_ERROR;
            }
            p += 2;  // skip tableId + tableVersion of found entry
            Uint32 words = 1 + AttributeHeader::getDataSize(*p);
            memcpy(m_attr_read_buf + m_attr_read_pos, p, words * sizeof(Uint32));
            header = reinterpret_cast<AttributeHeader*>(m_attr_read_buf + m_attr_read_pos);
            attrDescriptor = nullptr;
          } else {
            col_index = col_id_raw << 16;
            ret = block_tup->readAttributes(req_struct, &(col_index), 1,
                      m_attr_read_buf + m_attr_read_pos, g_attr_read_buf_len_ - m_attr_read_pos);
            if (ret < 0) {
              DEB_AGG(("read column error: %d", ret));
              return -ret;
            }
            header = reinterpret_cast<AttributeHeader*>(m_attr_read_buf + m_attr_read_pos);
            attrDescriptor = req_struct->tablePtrP->tabDescriptor +
                (((col_index) >> 16) * ZAD_SIZE);
            assert(header->getAttributeId() == (col_index >> 16));
            assert(type == AttributeDescriptor::getType(attrDescriptor[0]));
          }
        }
        if (!TypeSupported(type)) {
          DEB_AGG(("Unsupported column type: %u", type));
          return ZAGG_COL_TYPE_UNSUPPORTED;
        }

        if (type == NDB_TYPE_DECIMAL ||
            type == NDB_TYPE_DECIMALUNSIGNED) {
          if (unlikely(exec_pos >= m_prog_len)) {
            g_eventLogger->debug("AggInterpreter::ProcessRec ZAGG_OTHER_ERROR: "
                "kOpLoadCol DECIMAL overflow exec_pos=%u prog_len=%u",
                exec_pos, m_prog_len);
            return ZAGG_OTHER_ERROR;
          }
          decimal_info =
              sint4korr(reinterpret_cast<char*>(&m_prog[exec_pos++]));
          precision = decimal_info >> 16;
          scale = decimal_info & 0xFFFF;
        } else {
          precision = 0;
          scale = 0;
        }

        ResetRegister(&m_registers[reg_index]);
        m_registers[reg_index].type = AlignedType(type, scale);
        m_registers[reg_index].is_unsigned = is_unsigned;
        m_registers[reg_index].is_null = header->isNULL();
        if (m_registers[reg_index].is_null) {
          // Column has a null value
          PA_INTERP_TRACE(m_frag_id,
                          "Load NULL, type: %u",
                          m_registers[reg_index].type);
          m_registers[reg_index].value.val_int64 = 0;
          break;
        }
        switch (type) {
          case NDB_TYPE_TINYINT:
            m_registers[reg_index].value.val_int64 =
                *reinterpret_cast<Int8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_TINYINT %lld",
                            m_registers[reg_index].value.val_int64);
            break;
          case NDB_TYPE_SMALLINT:
            m_registers[reg_index].value.val_int64 =
                sint2korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_SMALLINT %lld",
                            m_registers[reg_index].value.val_int64);
            break;
          case NDB_TYPE_MEDIUMINT:
            m_registers[reg_index].value.val_int64 =
                sint3korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_MEDIUM %lld",
                            m_registers[reg_index].value.val_int64);
            break;
          case NDB_TYPE_INT:
            m_registers[reg_index].value.val_int64 =
                sint4korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_INT %lld",
                            m_registers[reg_index].value.val_int64);
            break;
          case NDB_TYPE_BIGINT:
            m_registers[reg_index].value.val_int64 =
                sint8korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_BIGINT %lld",
                            m_registers[reg_index].value.val_int64);
            break;
          case NDB_TYPE_TINYUNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                *reinterpret_cast<Uint8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_TINYUNSIGNED %llu",
                            m_registers[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_SMALLUNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                uint2korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_SMALLUNSIGNED %llu",
                            m_registers[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_MEDIUMUNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                uint3korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_MEDIUMUNSIGNED %llu",
                            m_registers[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_UNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                uint4korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_UNSIGNED %llu",
                            m_registers[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_BIGUNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                uint8korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_BIGUNSIGNED %llu",
                            m_registers[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_FLOAT:
            m_registers[reg_index].value.val_double =
                floatget(reinterpret_cast<unsigned char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_FLOAT %lf",
                            m_registers[reg_index].value.val_double);
            break;
          case NDB_TYPE_DOUBLE:
            m_registers[reg_index].value.val_double =
                doubleget(reinterpret_cast<unsigned char*>(
                      &m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_DOUBLE %lf",
                            m_registers[reg_index].value.val_double);
            break;
          case NDB_TYPE_DECIMAL:
            assert(static_cast<Uint32>(decimal_bin_size(precision, scale)) ==
                header->getByteSize());
            // memset(decimal.buf, 0, sizeof(Int32) * DECIMAL_BUFF_LENGTH);
            dec_ret = bin2decimal(reinterpret_cast<const uchar*>(&m_attr_read_buf[m_attr_read_pos + 1]),
                      &m_decimal, precision, scale);
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while parsing decimal: ");
              for (Uint32 i = 0;
                  i < header->getByteSize(); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              DEB_AGG(("%s", log_buf));
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
            assert(m_registers[reg_index].is_unsigned == false);
            if (scale != 0) {
              assert(m_registers[reg_index].type == NDB_TYPE_DOUBLE);
              dec_ret = decimal2double(&m_decimal, &dec_val_dbl);
              m_registers[reg_index].value.val_double = dec_val_dbl;
            } else {
              assert(m_registers[reg_index].type == NDB_TYPE_BIGINT);
              dec_ret = decimal2longlong(&m_decimal, &dec_val_ll);
              m_registers[reg_index].value.val_int64 = dec_val_ll;
            }
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while converting decimal: ");
              for (Uint32 i = 0;
                  i < header->getByteSize(); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              DEB_AGG(("%s", log_buf));
              if (dec_ret == E_DEC_OVERFLOW) {
                return ZAGG_DECIMAL_CONV_OVERFLOW;
              } else {
                return ZAGG_DECIMAL_CONV_ERROR;
              }
            }
#ifdef DEBUG_PA_INTERP
            if (scale != 0) {
              PA_INTERP_TRACE(m_frag_id,
                              "Load NDB_TYPE_DECIMAL[double] %lf",
                              m_registers[reg_index].value.val_double);
            } else {
              PA_INTERP_TRACE(m_frag_id,
                              "Load NDB_TYPE_DECIMAL[int64] %lld",
                              m_registers[reg_index].value.val_int64);
            }
#endif // DEBUG_PA_INTERP
          break;
        case NDB_TYPE_DECIMALUNSIGNED:
            assert(static_cast<Uint32>(decimal_bin_size(precision, scale)) ==
                header->getByteSize());
            // memset(decimal.buf, 0, sizeof(Int32) * DECIMAL_BUFF_LENGTH);
            dec_ret = bin2decimal(reinterpret_cast<const uchar*>(&m_attr_read_buf[m_attr_read_pos + 1]),
                      &m_decimal, precision, scale);
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while parsing decimal: ");
              for (Uint32 i = 0;
                  i < header->getByteSize(); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              DEB_AGG(("%s", log_buf));
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
            assert(m_registers[reg_index].is_unsigned == true);
            if(unlikely(m_decimal.sign)) {
              return ZAGG_DECIMAL_CONV_ERROR;
            }
            if (scale != 0) {
              assert(m_registers[reg_index].type == NDB_TYPE_DOUBLE);
              dec_ret = decimal2double(&m_decimal, &dec_val_dbl);
              m_registers[reg_index].value.val_double = dec_val_dbl;
            } else {
              assert(m_registers[reg_index].type == NDB_TYPE_BIGINT);
              dec_ret = decimal2ulonglong(&m_decimal, &dec_val_ull);
              m_registers[reg_index].value.val_uint64 = dec_val_ull;
            }
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while converting decimal: ");
              for (Uint32 i = 0;
                  i < header->getByteSize(); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              DEB_AGG(("%s", log_buf));
              if (dec_ret == E_DEC_OVERFLOW) {
                return ZAGG_DECIMAL_CONV_OVERFLOW;
              } else {
                return ZAGG_DECIMAL_CONV_ERROR;
              }
            }
#ifdef DEBUG_PA_INTERP
            if (scale != 0) {
              PA_INTERP_TRACE(m_frag_id,
                              "Load NDB_TYPE_DECIMALUNSIGNED[double] %lf",
                              m_registers[reg_index].value.val_double);
            } else {
              PA_INTERP_TRACE(m_frag_id,
                              "Load NDB_TYPE_DECIMALUNSIGEND[uint64] %llu",
                              m_registers[reg_index].value.val_uint64);
            }
#endif // DEBUG_PA_INTERP
          break;

          default:
            return ZAGG_LOAD_COL_WRONG_TYPE;
        }
        break;
      case kOpLoadConst:
        type = (value & 0x03E00000) >> 21;
        reg_index = (value & 0x000F0000) >> 16;
        assert(type == NDB_TYPE_BIGINT || type == NDB_TYPE_BIGUNSIGNED ||
               type == NDB_TYPE_DOUBLE);
        ResetRegister(&m_registers[reg_index]);
        m_registers[reg_index].type = AlignedType(type, 0);
        m_registers[reg_index].is_unsigned = IsUnsigned(type);
        m_registers[reg_index].is_null = false;
        if (unlikely(exec_pos + 2 > m_prog_len)) {
          g_eventLogger->debug("AggInterpreter::ProcessRec ZAGG_OTHER_ERROR: "
              "kOpLoadConst overflow exec_pos=%u prog_len=%u",
              exec_pos, m_prog_len);
          return ZAGG_OTHER_ERROR;
        }
        switch (type) {
          case NDB_TYPE_BIGINT:
            m_registers[reg_index].value.val_int64 =
                sint8korr(reinterpret_cast<char*>(&m_prog[exec_pos]));
              PA_INTERP_TRACE(m_frag_id,
                              "LoadConst[%u] NDB_TYPE_BIGINT %lld",
                              reg_index, m_registers[reg_index].value.val_int64);
            break;
          case NDB_TYPE_BIGUNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                uint8korr(reinterpret_cast<char*>(&m_prog[exec_pos]));
              PA_INTERP_TRACE(m_frag_id,
                              "LoadConst[%u] "
                              "NDB_TYPE_BIGUNSIGNED %llu",
                              reg_index, m_registers[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_DOUBLE:
            m_registers[reg_index].value.val_double =
                doubleget(reinterpret_cast<unsigned char*>(
                      &m_prog[exec_pos]));
              PA_INTERP_TRACE(m_frag_id,
                              "LoadConst[%u] NDB_TYPE_DOUBLE %lf",
                              reg_index, m_registers[reg_index].value.val_double);
            break;
          default:
            return ZAGG_LOAD_CONST_WRONG_TYPE;
        }
        exec_pos += 2;
        break;
      case kOpMov:
        reg_index = (value >> 12 ) & 0x0F;
        reg_index2 = (value >> 8 ) & 0x0F;

        m_registers[reg_index] = m_registers[reg_index2];
        PA_INTERP_TRACE(m_frag_id,
                        "Move [%u]->[%u]",
                        reg_index2, reg_index);
        break;
      case kOpSum:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Sum(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
        if (ret < 0) {
          DEB_AGG(("Overflow[SUM], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpMax:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Max(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
        break;
      case kOpMin:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Min(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
        break;
      case kOpCount:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Count(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
        break;

      // Type-specific Sum operations
      case kOpSumBigint:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);
        ret = SumBigint(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
        if (ret < 0) {
          DEB_AGG(("Overflow[SumBigint], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      case kOpSumDouble:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);
        ret = SumDouble(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
        if (ret < 0) {
          DEB_AGG(("Overflow[SumDouble], value is out of range"));
          return ZAGG_MATH_OVERFLOW;
        }
        break;

      // Type-specific Max operations
      case kOpMaxBigint:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);
        ret = MaxBigint(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
        break;

      case kOpMaxDouble:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);
        ret = MaxDouble(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
        break;

      // Type-specific Min operations
      case kOpMinBigint:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);
        ret = MinBigint(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
        break;

      case kOpMinDouble:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);
        ret = MinDouble(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
        break;

      case kOpSkip:
      {
        Uint32 skip_count = value & 0xFFFF;
        exec_pos += skip_count;
        break;
      }

      case kOpEmbeddedInterp:
      {
        Uint32 emb_len = value & 0xFFFF;
        if (exec_pos + emb_len > m_prog_len) {
          g_eventLogger->debug("AggInterpreter::ProcessRec ZAGG_OTHER_ERROR: "
              "embedded interp len overflow exec_pos=%u emb_len=%u "
              "prog_len=%u", exec_pos, emb_len, m_prog_len);
          return ZAGG_OTHER_ERROR;
        }

        /* Save and reset instruction counter (interpreterNextLab asserts 0) */
        Uint32 saved_instr_count = req_struct->no_exec_instructions;
        req_struct->no_exec_instructions = 0;

        /* Local output buffer — avoids corrupting outer interpreter coutBuffer */
        Uint32 local_tmpArea[16];

        int rc = block_tup->interpreterNextLab(
            req_struct->signal, req_struct,
            &m_prog[exec_pos], emb_len,   /* main program = embedded portion */
            nullptr, 0,                   /* no subroutines */
            local_tmpArea, 16);

        req_struct->no_exec_instructions = saved_instr_count;

        if (rc < 0) {
          return ZAGG_EMBEDDED_INTERP_ERROR;
        }

        /* Read skip_offset written by WRITE_INTERPRETER_OUTPUT in embedded prog */
        Uint32 skip_offset = block_tup->c_interpreter_output[0];
        exec_pos += emb_len + skip_offset;
        break;
      }

      default:
        return ZAGG_WRONG_OPERATION;
    }
  }
  m_processed_rows++;
  return 0;
}

void AggInterpreter::Print() {
  char log_buf[1024];
  if (m_n_gb_cols) {
    if (m_gb_map) {
      sprintf(log_buf, "Group by columns: [");
      for (Uint32 i = 0; i < m_n_gb_cols; i++) {
        if (i != m_n_gb_cols - 1) {
          sprintf(log_buf + strlen(log_buf), "%u ", m_gb_cols[i] >> 16);
        } else {
          sprintf(log_buf + strlen(log_buf), "%u", m_gb_cols[i] >> 16);
        }
      }
      sprintf(log_buf + strlen(log_buf), "]");
      g_eventLogger->info("%s", log_buf);
      log_buf[0] = '\0';

      g_eventLogger->info("Num of groups: %u, Aggregation results:",
                          m_gb_map->size());
      for (auto iter = m_gb_map->begin(); iter.valid(); m_gb_map->next(iter)) {
        int pos = 0;
        char* data = iter.data();
        Uint32 key_len = iter.keyLen();
        sprintf(log_buf, "(");
        for (Uint32 i = 0; i < m_n_gb_cols; i++) {
          if (i != m_n_gb_cols - 1) {
            sprintf(log_buf + strlen(log_buf), "%u: %p, ", i, data + pos);
          } else {
            sprintf(log_buf + strlen(log_buf), "%u: %p): ", i, data + pos);
          }
        }

        AggResItem* item = reinterpret_cast<AggResItem*>(data + key_len);
        for (Uint32 i = 0; i < m_n_agg_results; i++) {
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
    AggResItem* item = m_agg_results;
    log_buf[0] = '\0';
    for (Uint32 i = 0; i < m_n_agg_results; i++) {
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

/**
 * processRecWithLinkedAttrs - process a row for join aggregation.
 *
 * Sets m_linked_attr_data to point at linked_attr_data which contains
 * entries from parent tables in the join tree, each with format:
 *   [tableId] [tableVersion] [AH(attrId, dataSize)] [data...]
 * ProcessRec's kOpLoadCol and GROUP BY handlers resolve columns whose
 * bit 15 is set from this buffer (skipping the tableId/tableVersion
 * prefix per entry) instead of reading via DBTUP readAttributes.
 * The linked attr pointer is cleared after ProcessRec returns.
 */
Uint32 GBHashTable::hashKey(const char* key, Uint32 len) const {
  if (m_col_types == nullptr) {
    Uint64 h = rondb_xxhash_std(key, len);
    return static_cast<Uint32>(h) & m_bucket_mask;
  }

  // Type-aware path: hash column-by-column
  Uint64 hash = 0;
  const Uint32* p = reinterpret_cast<const Uint32*>(key);
  const Uint32* end = reinterpret_cast<const Uint32*>(key + len);
  for (Uint32 i = 0; i < m_n_gb_cols && p < end; i++) {
    AttributeHeader ah(*p);
    Uint32 dataSize = ah.getDataSize();
    if (m_col_types[i].cs != nullptr && dataSize > 0) {
      // Collation-aware: normalize via strnxfrm_hash, then hash
      const uchar* src = reinterpret_cast<const uchar*>(p + 1);
      Uint32 byteSize = ah.getByteSize();
      Uint32 lb, srcLen;
      NdbSqlUtil::get_var_length(m_col_types[i].typeId, src, byteSize,
                                 lb, srcLen);
      Uint32 maxBytes = m_col_types[i].maxBytes;
      Uint32 defLen = maxBytes - lb;
      int n = NdbSqlUtil::strnxfrm_hash(m_col_types[i].cs,
                                         m_col_types[i].typeId,
                                         m_xfrm_buf, m_xfrm_buf_len,
                                         src + lb, srcLen, defLen);
      if (n > 0) {
        Uint64 colHash = rondb_xxhash_std(reinterpret_cast<const char*>(m_xfrm_buf),
                                          n);
        hash ^= colHash + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
      }
    } else {
      // Non-collation or NULL: hash raw [AH + data] bytes
      Uint32 colWords = 1 + dataSize;
      Uint64 colHash = rondb_xxhash_std(reinterpret_cast<const char*>(p),
                                        colWords * sizeof(Uint32));
      hash ^= colHash + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
    }
    p += 1 + dataSize;
  }
  return static_cast<Uint32>(hash) & m_bucket_mask;
}

char* GBHashTable::findInBucket(Uint32 b, const char* key,
                                Uint32 key_len) const {
  if (m_col_types == nullptr) {
    // Raw comparison path (no type metadata)
    for (char* raw = m_buckets[b]; raw != nullptr;
         raw = hashNext(raw)) {
      char* d = raw + OVERHEAD;
      Uint32 kl = *reinterpret_cast<Uint32*>(raw + KEY_LEN_OFFSET);
      if (kl == key_len && memcmp(d, key, key_len) == 0) {
        return d;
      }
    }
    return nullptr;
  }

  // Type-aware comparison path: compare column-by-column using cmpFn
  for (char* raw = m_buckets[b]; raw != nullptr;
       raw = hashNext(raw)) {
    char* d = raw + OVERHEAD;
    Uint32 kl = *reinterpret_cast<Uint32*>(raw + KEY_LEN_OFFSET);
    if (kl != key_len) continue;

    const Uint32* p1 = reinterpret_cast<const Uint32*>(key);
    const Uint32* p2 = reinterpret_cast<const Uint32*>(d);
    const Uint32* p1_end = reinterpret_cast<const Uint32*>(key + key_len);
    bool match = true;
    for (Uint32 i = 0; i < m_n_gb_cols && p1 < p1_end; i++) {
      AttributeHeader ah1(*p1);
      AttributeHeader ah2(*p2);
      Uint32 ds1 = ah1.getDataSize();
      Uint32 ds2 = ah2.getDataSize();
      if (ds1 == 0 && ds2 == 0) {
        // Both NULL — considered equal
        p1 += 1;
        p2 += 1;
        continue;
      }
      if (ds1 != ds2) {
        match = false;
        break;
      }
      if (m_col_types[i].cs != nullptr) {
        // Collation-aware comparison
        Uint32 byteSize = ah1.getByteSize();
        int cmp = (*m_col_types[i].cmpFn)(
            m_col_types[i].cs,
            p1 + 1, byteSize,
            p2 + 1, byteSize);
        if (cmp != 0) {
          match = false;
          break;
        }
      } else {
        // Raw comparison for non-string types
        if (memcmp(p1 + 1, p2 + 1, ds1 * sizeof(Uint32)) != 0) {
          match = false;
          break;
        }
      }
      p1 += 1 + ds1;
      p2 += 1 + ds2;
    }
    if (match) return d;
  }
  return nullptr;
}

Int32 AggInterpreter::initGBTypes(Dbtup* block_tup,
                                  Dbtup::KeyReqStruct* req_struct) {
  for (Uint32 i = 0; i < m_n_gb_cols; i++) {
    Uint32 attr_id = m_gb_cols[i] >> 16;
    GBColTypeInfo &info = m_gb_types[i];

    if ((attr_id & 0x8000) != 0 && m_linked_attr_data != nullptr) {
      // Linked column: walk buffer to get (tableId, tableVersion, attrId)
      Uint32 position = attr_id & 0x7FFF;
      const Uint32* p = m_linked_attr_data;
      const Uint32* p_end = m_linked_attr_data + m_linked_attr_len;
      Uint32 pos_count = 0;
      while (p < p_end && pos_count < position) {
        p += 2;  // skip tableId + tableVersion
        p += 1 + AttributeHeader::getDataSize(*p);
        pos_count++;
      }
      if (unlikely(p + 2 >= p_end)) {
        g_eventLogger->debug("initGBTypes: linked buffer too short for "
            "position %u (linked_len=%u)", position, m_linked_attr_len);
        return ZAGG_OTHER_ERROR;
      }
      Uint32 tableId = p[0];
      Uint32 tableVersion = p[1];

      // Validate table version against DBLQH's tablerec
      Dblqh* lqh = block_tup->c_lqh;
      if (unlikely(tableId >= lqh->ctabrecFileSize)) {
        g_eventLogger->debug("initGBTypes: tableId %u out of range "
            "(max=%u)", tableId, lqh->ctabrecFileSize);
        return ZINVALID_SCHEMA_VERSION;
      }
      if (unlikely(table_version_major(tableVersion) !=
                   table_version_major(lqh->tablerec[tableId].schemaVersion))) {
        g_eventLogger->debug("initGBTypes: schema version mismatch for "
            "tableId %u: linked=%u, current=%u",
            tableId, tableVersion, lqh->tablerec[tableId].schemaVersion);
        return ZINVALID_SCHEMA_VERSION;
      }

      // Look up type metadata from DBTUP's tablerec
      if (unlikely(tableId >= block_tup->cnoOfTablerec)) {
        g_eventLogger->debug("initGBTypes: tableId %u out of range for "
            "DBTUP (max=%u)", tableId, block_tup->cnoOfTablerec);
        return ZINVALID_SCHEMA_VERSION;
      }
      Dbtup::Tablerec* tab = &block_tup->tablerec[tableId];
      Uint32 linkedAttrId = AttributeHeader(p[2]).getAttributeId();
      const Uint32* attrDesc = tab->tabDescriptor + linkedAttrId * ZAD_SIZE;
      info.typeId = AttributeDescriptor::getType(attrDesc[0]);
      info.maxBytes = AttributeDescriptor::getSizeInBytes(attrDesc[0]);
      info.cs = nullptr;
      if (AttributeOffset::getCharsetFlag(attrDesc[1])) {
        Uint32 csPos = AttributeOffset::getCharsetPos(attrDesc[1]);
        info.cs = tab->charsetArray[csPos];
      }
    } else {
      // Local column: look up from the leaf table
      const Uint32* attrDesc = req_struct->tablePtrP->tabDescriptor +
          attr_id * ZAD_SIZE;
      info.typeId = AttributeDescriptor::getType(attrDesc[0]);
      info.maxBytes = AttributeDescriptor::getSizeInBytes(attrDesc[0]);
      info.cs = nullptr;
      if (AttributeOffset::getCharsetFlag(attrDesc[1])) {
        Uint32 csPos = AttributeOffset::getCharsetPos(attrDesc[1]);
        info.cs = req_struct->tablePtrP->charsetArray[csPos];
      }
    }
    const NdbSqlUtil::Type &sqlType = NdbSqlUtil::getType(info.typeId);
    info.cmpFn = sqlType.m_cmp;
  }
  // Compute max xfrm buffer size needed for collation-aware hashing
  Uint32 max_xfrm_len = 0;
  for (Uint32 i = 0; i < m_n_gb_cols; i++) {
    if (m_gb_types[i].cs != nullptr) {
      Uint32 lb = 0;
      if (m_gb_types[i].typeId == NDB_TYPE_VARCHAR) lb = 1;
      else if (m_gb_types[i].typeId == NDB_TYPE_LONGVARCHAR) lb = 2;
      Uint32 defLen = m_gb_types[i].maxBytes - lb;
      Uint32 xfrm_len = NdbSqlUtil::strnxfrm_hash_len(m_gb_types[i].cs,
                                                        defLen);
      if (xfrm_len > max_xfrm_len) max_xfrm_len = xfrm_len;
    }
  }
  if (max_xfrm_len > 0) {
    void* p = lc_ndbd_pool_malloc(max_xfrm_len, RG_QUERY_MEMORY,
                                  m_thread_id, false);
    if (unlikely(p == nullptr)) {
      g_eventLogger->debug("initGBTypes: failed to allocate xfrm buffer "
          "(%u bytes)", max_xfrm_len);
      return ZAGG_OTHER_ERROR;
    }
    m_xfrm_buf = static_cast<uchar*>(p);
    m_xfrm_buf_len = max_xfrm_len;
  }

  m_gb_types_inited = true;
  m_gb_map->setTypeMeta(m_gb_types, m_n_gb_cols, m_xfrm_buf, m_xfrm_buf_len);
  return 0;
}

Int32 AggInterpreter::processRecWithLinkedAttrs(
    Dbtup* block_tup,
    Dbtup::KeyReqStruct* req_struct,
    const Uint32* linked_attr_data,
    Uint32 linked_attr_len) {
  std::unique_lock<std::mutex> lock(m_mutex, std::defer_lock);
  if (m_use_mutex) lock.lock();

  m_linked_attr_data = linked_attr_data;
  m_linked_attr_len = linked_attr_len;

  bool dummy = false;
  Int32 ret = ProcessRec(block_tup, req_struct, &dummy);

  m_linked_attr_data = nullptr;
  m_linked_attr_len = 0;
  return ret;
}

/**
 * evictOneGroup - remove one group from m_gb_map and serialize it.
 *
 * Called when ProcessRec returns AGG_EVICT_NEEDED because allocation
 * failed.  Targets the chunk with the fewest live_groups so that
 * repeated evictions are likely to free an entire chunk.  Pops the
 * head of the target chunk's per-group linked list for O(1) group
 * selection.  Falls back to m_gb_map->begin() when no chunk with a
 * non-empty group_list is found.  Returns -1 if the map is empty or
 * the buffer is too small.
 */
Int32 AggInterpreter::evictOneGroup(Uint32* buf, Uint32 buf_words,
                                    Uint32* words_written) {
  if (m_gb_map == nullptr || m_gb_map->empty()) {
    return -1;
  }

  // Find the chunk with the fewest live_groups that has a group_list.
  static const Uint32 MAX_CHUNK_SCAN = 10;
  MemChunk* target = nullptr;
  Uint32 scanned = 0;
  for (MemChunk* c = m_chunks; c != nullptr && scanned < MAX_CHUNK_SCAN;
       c = c->next, scanned++) {
    if (c->group_list != nullptr &&
        (target == nullptr || c->live_groups < target->live_groups)) {
      target = c;
      if (target->live_groups == 1) break;
    }
  }

  require(target != nullptr);

  // Pop head from the target chunk's per-group linked list.
  char* raw = target->group_list;
  target->group_list = *reinterpret_cast<char**>(raw);
  Uint32 key_len = *reinterpret_cast<Uint32*>(raw + 2 * sizeof(char*));
  char* data_ptr = raw + GROUP_LINK_OVERHEAD;
  Uint32 v_len = val_len();
  const Uint32 data_words = (key_len + v_len) >> 2;
  const Uint32 total_words = 4 + data_words;

  if (total_words > buf_words) {
    return -1;
  }

  Uint32 pos = 0;
  buf[pos++] = AttributeHeader::AGG_RESULT << 16 | 0x0721;
  buf[pos++] = m_n_gb_cols << 16 | m_n_agg_results;
  buf[pos++] = 1;
  buf[pos++] = key_len << 16 | v_len;
  memcpy(&buf[pos], data_ptr, key_len + v_len);
  pos += data_words;

  *words_written = pos;

  m_result_size -= (key_len + v_len);
  m_n_groups--;

  m_gb_map->erase(data_ptr, key_len);
  freeGroupData(data_ptr);

  return 0;
}

/**
 * finalizeResults - post-process accumulated aggregation results.
 *
 * Called after all rows have been processed (and after mergeFrom in the
 * multi-thread case) and before getResultData.
 *
 * All current aggregate operations (SUM, COUNT, MAX, MIN) accumulate
 * their final value directly during ProcessRec, so no arithmetic
 * post-processing is required. AVG is computed at the SQL layer from
 * the pushed-down SUM and COUNT.
 */
Int32 AggInterpreter::finalizeResults() {
  return 0;
}

/**
 * getResultData - serialize finalized aggregation results into a buffer.
 *
 * Writes results in the same TRANSID_AI-compatible format used by
 * PrepareAggResIfNeeded but into a caller-provided buffer (buffer_size
 * in bytes).  Non-destructive: does not erase entries from m_gb_map.
 * Returns -1 if the buffer is too small.
 *
 * Wire format:
 *   Word 0: AttributeHeader::AGG_RESULT << 16 | 0x0721  (magic)
 *   Word 1: m_n_gb_cols << 16 | m_n_agg_results
 *   Word 2: number of groups (0 when no group-by)
 *   Per group (or once when no group-by):
 *     Word: key_len << 16 | value_len  (byte lengths)
 *     Words: key data + accumulator data (contiguous, word-aligned)
 */
Int32 AggInterpreter::getResultData(Uint32* buffer, Uint32 buffer_size,
                                    Uint32* bytes_written) {
  Uint32 pos = 0;
  assert(m_n_gb_cols < 0xFFFF);
  assert(m_n_agg_results < 0xFFFF);

  if (m_n_gb_cols) {
    if (m_gb_map == nullptr || m_gb_map->empty()) {
      if (3 * sizeof(Uint32) > buffer_size) {
        *bytes_written = 0;
        return -1;
      }
      buffer[pos++] = AttributeHeader::AGG_RESULT << 16 | 0x0721;
      buffer[pos++] = m_n_gb_cols << 16 | m_n_agg_results;
      buffer[pos++] = 0;
      *bytes_written = pos * sizeof(Uint32);
      return 0;
    }
    if (3 * sizeof(Uint32) > buffer_size) {
      *bytes_written = 0;
      return -1;
    }
    buffer[pos++] = AttributeHeader::AGG_RESULT << 16 | 0x0721;
    buffer[pos++] = m_n_gb_cols << 16 | m_n_agg_results;
    const Uint32 v_len = val_len();
    buffer[pos++] = m_gb_map->size();
    for (auto iter = m_gb_map->begin(); iter.valid(); m_gb_map->next(iter)) {
      Uint32 key_len = iter.keyLen();
      assert(key_len % 4 == 0 && key_len < 0xFFFF);
      assert(v_len % 4 == 0 && v_len < 0xFFFF);
      Uint32 data_words = (key_len + v_len) >> 2;
      if ((pos + 1 + data_words) * sizeof(Uint32) > buffer_size) {
        *bytes_written = 0;
        return -1;
      }
      buffer[pos++] = key_len << 16 | v_len;
      MEMCOPY_NO_WORDS(&buffer[pos], iter.data(), data_words);
      pos += data_words;
    }
  } else {
    Uint32 agg_bytes = m_n_agg_results * sizeof(AggResItem);
    Uint32 agg_words = agg_bytes >> 2;
    if ((4 + agg_words) * sizeof(Uint32) > buffer_size) {
      *bytes_written = 0;
      return -1;
    }
    buffer[pos++] = AttributeHeader::AGG_RESULT << 16 | 0x0721;
    buffer[pos++] = m_n_gb_cols << 16 | m_n_agg_results;
    buffer[pos++] = 0;
    buffer[pos++] = 0 << 16 | agg_bytes;
    MEMCOPY_NO_WORDS(&buffer[pos], m_agg_results, agg_words);
    pos += agg_words;
  }

  *bytes_written = pos * sizeof(Uint32);
  return 0;
}

/**
 * mergeAccumulators - merge src accumulator array into dst.
 * Rules: SUM += SUM, COUNT += COUNT, MAX = max(), MIN = min().
 * agg_ops[i] contains the opcode (kOpSum/kOpCount/kOpMax/kOpMin or
 * type-specific variant) for accumulator slot i.
 */
static void mergeAccumulators(AggResItem* dst, const AggResItem* src,
                              Uint32 n_agg_results,
                              const Uint8* agg_ops) {
  for (Uint32 i = 0; i < n_agg_results; i++) {
    if (src[i].type == NDB_TYPE_UNDEFINED) {
      continue;
    }
    if (dst[i].type == NDB_TYPE_UNDEFINED) {
      dst[i] = src[i];
      continue;
    }
    if (src[i].is_null) {
      continue;
    }
    if (dst[i].is_null) {
      dst[i] = src[i];
      continue;
    }
    switch (agg_ops[i]) {
      case kOpSum:
      case kOpSumBigint:
      case kOpSumDouble:
        if (dst[i].type == NDB_TYPE_BIGINT) {
          if (dst[i].is_unsigned) {
            dst[i].value.val_uint64 += src[i].value.val_uint64;
          } else {
            dst[i].value.val_int64 += src[i].value.val_int64;
          }
        } else {
          dst[i].value.val_double += src[i].value.val_double;
        }
        break;
      case kOpCount:
        dst[i].value.val_uint64 += src[i].value.val_uint64;
        break;
      case kOpMax:
      case kOpMaxBigint:
      case kOpMaxDouble:
        if (dst[i].type == NDB_TYPE_BIGINT) {
          if (dst[i].is_unsigned) {
            if (src[i].value.val_uint64 > dst[i].value.val_uint64)
              dst[i].value.val_uint64 = src[i].value.val_uint64;
          } else {
            if (src[i].value.val_int64 > dst[i].value.val_int64)
              dst[i].value.val_int64 = src[i].value.val_int64;
          }
        } else {
          if (src[i].value.val_double > dst[i].value.val_double)
            dst[i].value.val_double = src[i].value.val_double;
        }
        break;
      case kOpMin:
      case kOpMinBigint:
      case kOpMinDouble:
        if (dst[i].type == NDB_TYPE_BIGINT) {
          if (dst[i].is_unsigned) {
            if (src[i].value.val_uint64 < dst[i].value.val_uint64)
              dst[i].value.val_uint64 = src[i].value.val_uint64;
          } else {
            if (src[i].value.val_int64 < dst[i].value.val_int64)
              dst[i].value.val_int64 = src[i].value.val_int64;
          }
        } else {
          if (src[i].value.val_double < dst[i].value.val_double)
            dst[i].value.val_double = src[i].value.val_double;
        }
        break;
      default:
        assert(0);
        break;
    }
  }
}

/**
 * extractAggOps - scan the aggregation program to determine which operation
 * (SUM, COUNT, MAX, MIN) applies to each accumulator slot.
 */
static void extractAggOps(const Uint32* prog, Uint32 prog_len,
                          Uint32 agg_prog_start_pos,
                          Uint8* agg_ops, Uint32 n_agg_results) {
  memset(agg_ops, 0, n_agg_results);
  Uint32 exec_pos = agg_prog_start_pos;
  while (exec_pos < prog_len) {
    Uint32 value = prog[exec_pos++];
    Uint8 op = (value & 0xFC000000) >> 26;
    Uint32 agg_index;
    switch (op) {
      case kOpSum:
      case kOpSumBigint:
      case kOpSumDouble:
      case kOpMax:
      case kOpMaxBigint:
      case kOpMaxDouble:
      case kOpMin:
      case kOpMinBigint:
      case kOpMinDouble:
      case kOpCount:
        agg_index = value & 0x0000FFFF;
        if (agg_index < n_agg_results) {
          agg_ops[agg_index] = op;
        }
        break;
      case kOpLoadCol: {
        Uint32 type = (value & 0x03E00000) >> 21;
        if (type == NDB_TYPE_DECIMAL || type == NDB_TYPE_DECIMALUNSIGNED) {
          exec_pos++;  // Skip decimal info word
        }
        break;
      }
      case kOpLoadConst:
        exec_pos += 2;  // Skip constant value words
        break;
      case kOpEmbeddedInterp: {
        Uint32 emb_len = value & 0xFFFF;
        exec_pos += emb_len;  // Skip embedded words
        break;
      }
      case kOpSkip:
        break;  // 1-word instruction, already advanced by exec_pos++
      default:
        break;
    }
  }
}

Uint32 AggInterpreter::mergeFrom(AggInterpreter* other,
                                  Uint32 max_groups) {
  assert(other != nullptr);
  assert(m_n_agg_results == other->m_n_agg_results);

  // Cache agg_ops on first call to avoid recomputing per CONTINUEB batch.
  if (!m_agg_ops_cached) {
    extractAggOps(m_prog, m_prog_len, m_agg_prog_start_pos,
                  m_cached_agg_ops, m_n_agg_results);
    m_agg_ops_cached = true;
  }

  // No group-by case: merge the flat accumulator arrays
  if (m_n_gb_cols == 0) {
    if (other->m_agg_results != nullptr) {
      mergeAccumulators(m_agg_results, other->m_agg_results,
                        m_n_agg_results, m_cached_agg_ops);
    }
    m_processed_rows += other->m_processed_rows;
    return 0;
  }

  // Group-by case: lockstep bucket merge from other into this
  if (other->m_gb_map == nullptr || other->m_gb_map->empty()) {
    m_processed_rows += other->m_processed_rows;
    return 0;
  }

  const Uint32 v_len = val_len();
  const Uint32 nbuckets = m_gb_map->bucketCount();
  Uint32 count = 0;
  for (Uint32 b = 0; b < nbuckets; b++) {
    while (!other->m_gb_map->bucketEmpty(b)) {
      char* other_data = other->m_gb_map->popBucketHead(b);
      Uint32 other_key_len =
        *reinterpret_cast<Uint32*>(other_data - GBHashTable::OVERHEAD +
                                   GBHashTable::KEY_LEN_OFFSET);

      char* my_data = m_gb_map->findInBucket(b, other_data, other_key_len);
      if (my_data != nullptr) {
        // Group exists in both: merge accumulators, free other's copy
        const AggResItem *other_items =
          reinterpret_cast<const AggResItem *>(other_data + other_key_len);
        AggResItem *my_items =
          reinterpret_cast<AggResItem *>(my_data + other_key_len);
        mergeAccumulators(my_items, other_items, m_n_agg_results,
                          m_cached_agg_ops);
        other->freeGroupData(other_data);
      } else {
        // Group only in other: move pointer into this map (no alloc/copy)
        m_gb_map->insertRaw(other_data);
        m_result_size += other_key_len + v_len;
      }
      count++;

      if (max_groups > 0 && count >= max_groups &&
          !other->m_gb_map->empty()) {
        m_n_groups = m_gb_map->size();
        return other->m_gb_map->size();
      }
    }
  }

  // All groups processed — transfer chunks from other to this interpreter.
  // Moved groups live in other's chunks, so we take ownership.
  // O(1) splice via tail pointer.
  if (other->m_chunks != nullptr) {
    other->m_chunks_tail->next = m_chunks;
    if (m_chunks != nullptr) {
      m_chunks->prev = other->m_chunks_tail;
    } else {
      m_chunks_tail = other->m_chunks_tail;
    }
    m_chunks = other->m_chunks;
    m_chunks->prev = nullptr;
    m_total_chunk_bytes += other->m_total_chunk_bytes;
    other->m_chunks = nullptr;
    other->m_chunks_tail = nullptr;
    other->m_current_chunk = nullptr;
    other->m_total_chunk_bytes = 0;
  }

  m_processed_rows += other->m_processed_rows;
  m_n_groups = m_gb_map->size();
  return 0;
}

// Dead code — retained for reference but sorted dual-iteration no longer applies
// with hash table. Print each interpreter's groups independently.
void AggInterpreter::MergePrint(const AggInterpreter* in1,
                                   const AggInterpreter* in2) {
  assert(in1 != nullptr && in2 != nullptr);
  assert(in1->m_n_agg_results == in2->m_n_agg_results);
  g_eventLogger->info("MergePrint: in1 groups=%u, in2 groups=%u",
                      in1->m_gb_map ? in1->m_gb_map->size() : 0,
                      in2->m_gb_map ? in2->m_gb_map->size() : 0);
}


Uint32 AggInterpreter::PrepareAggResIfNeeded(Signal* signal, bool force) {
  // Limitation
  Uint32 total_size = m_result_size +
                  (m_gb_map ?
                   m_gb_map->size() * g_result_header_size_per_group_ : 0) +
                  g_result_header_size_;
  if (!force && (m_gb_map == nullptr ||
        total_size < DEF_AGG_RESULT_BATCH_BYTES)) {
    return 0;
  }
  if (force &&
      (m_n_gb_cols != 0 && (m_gb_map == nullptr || m_gb_map->size() == 0))) {
    assert(m_result_size == 0);
    return 0;
  }
  Uint32* data_buf = (&signal->theData[25]);
  Uint32 pos = 0;
  assert(m_n_gb_cols < 0xFFFF);
  assert(m_n_agg_results < 0xFFFF);

  if (m_n_gb_cols) {
    const Uint32 max_pos = MAX_AGG_RESULT_BATCH_BYTES / sizeof(Uint32);
    data_buf[pos++] = AttributeHeader::AGG_RESULT << 16 | 0x0721;
    data_buf[pos++] = m_n_gb_cols << 16 | m_n_agg_results;
    Uint32 n_groups_pos = pos++;
    const Uint32 v_len = val_len();
    Uint32 n_groups = 0;
    for (auto iter = m_gb_map->begin(); iter.valid();) {
      Uint32 key_len = iter.keyLen();
      assert(key_len % 4 == 0 && key_len < 0xFFFF);
      assert(v_len % 4 == 0 && v_len < 0xFFFF);
      Uint32 group_words = 1 + ((key_len + v_len) >> 2);
      if (pos + group_words > max_pos && n_groups > 0) break;
      data_buf[pos++] = key_len << 16 | v_len;
      MEMCOPY_NO_WORDS(&data_buf[pos], iter.data(),
          (key_len + v_len) >> 2);
      pos += ((key_len + v_len) >> 2);
      char* data = iter.data();
      m_gb_map->eraseAndNext(iter);
      freeGroupData(data);
      n_groups++;
    }
    data_buf[n_groups_pos] = n_groups;
    m_result_size = 0;
  } else {
    data_buf[pos++] = AttributeHeader::AGG_RESULT << 16 | 0x0721;
    data_buf[pos++] = m_n_gb_cols << 16 | m_n_agg_results;
    data_buf[pos++] = 0;
    data_buf[pos++] = 0 << 16 | (m_n_agg_results * sizeof(AggResItem));
    assert(m_gb_map == nullptr);
    MEMCOPY_NO_WORDS(&data_buf[pos], m_agg_results,
        (m_n_agg_results * sizeof(AggResItem)) >> 2);
    pos += ((m_n_agg_results * sizeof(AggResItem)) >> 2);
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
      assert(n_agg_results == m_n_agg_results);
      assert(n_res_items == 0);
      Uint32 gb_cols_len = data_buf[parse_pos] >> 16;
      Uint32 agg_res_len = data_buf[parse_pos++] & 0xFFFF;
      assert(gb_cols_len == 0);
      assert(agg_res_len == m_n_agg_results * sizeof(AggResItem));
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
     * always return 1 even if m_gb_map is empty().
     * In this situation: pushdown aggregation with filter and
     * group by. 99% rows has been filtered out which means
     * m_gb_map has big chance to stay empty. In order to stop
     * Dblqh::scanTupkeyRefLab send scanfragconf before aggregation
     * scan finishes. here return 1 to stop that.
     */
    if (m_gb_map) {
      return (m_gb_map->empty() ? 1 : m_gb_map->size());
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
    if (m_gb_map) {
      assert(m_gb_map->empty());
    }
    return 0;
  }
}

void AggInterpreter::Destruct(AggInterpreter* ptr) {
  if (ptr == nullptr) {
    return;
  }
  ptr->~AggInterpreter();
  lc_ndbd_pool_free(ptr);
}

Int32 AggInterpreter::CopyVecCandidateFromSignal(Signal* signal,
                                                Uint32 ToutBufIndex) {
  if (m_candidate_allocator == nullptr) {
    VS_INTERP_TRACE(m_table_id, m_frag_id,
                    "CandidateAllocator pre-allocating memory, "
                    "top_n: %u, vec_max_rec_size: %u, actual_rec_size: %u",
                    m_vec_top_n, m_vec_max_rec_size, ToutBufIndex);
    void* ca_mem = lc_ndbd_pool_malloc(
        sizeof(CandidateAllocator), RG_QUERY_MEMORY, m_thread_id, false);
    if (ca_mem == nullptr) {
      g_eventLogger->error("Alloc mem for CandidateAllocator failed");
      return ZAGG_ALLOC_MEM_FAILED;
    }
    m_candidate_allocator = new (ca_mem) CandidateAllocator(
        m_vec_top_n, m_vec_max_rec_size, m_table_id, m_frag_id);
    int ret = m_candidate_allocator -> Init(m_thread_id);
    if (ret != 0) {
      return ret;
    }
  }
  // The actual record size can't be larger than the m_vec_max_rec_size
  // TODO (Zhao)
  // handle this error
  assert(ToutBufIndex <= m_vec_max_rec_size);

  if (m_vec_top_n_results.size() >= m_vec_top_n) {
    Candidate* knockout = m_vec_top_n_results.top();
    VS_INTERP_TRACE(m_table_id, m_frag_id,
                    "Picked the candidate with distance %lf, idx: %d as the next knockout",
        knockout->m_distance,
        knockout->m_idx_in_allocator);
    m_vec_top_n_results.pop();
    m_candidate_allocator->set_next_index(knockout->m_idx_in_allocator);
    // No need to delete 'knockout' — it will be reused by the next selected candidate.
    // delete knockout;
  }

  Candidate* selected = m_candidate_allocator->
      Allocate(m_curr_distance, &(signal->theData[25]), ToutBufIndex);
  if (selected == nullptr) {
    return -1;
  }
  m_vec_top_n_results.push(selected);
  VS_INTERP_TRACE(m_table_id, m_frag_id,
                  "Push one candidate with distance %lf, [%lu/%u], idx_in_allocator: %u",
                  m_curr_distance, m_vec_top_n_results.size(), m_vec_top_n,
                  selected->m_idx_in_allocator);
  return 0;
}

void AggInterpreter::PrepareVecCandidates() {
  m_vec_top_n_results_final.clear();
  while (!m_vec_top_n_results.empty()) {
    m_vec_top_n_results_final.push_back(m_vec_top_n_results.top());
    m_vec_top_n_results.pop();
  }
  m_next_send_idx = m_vec_top_n_results_final.size() - 1;
  VS_INTERP_TRACE(m_table_id, m_frag_id,
                  "PrepareVecCandidates %lu",
                  m_vec_top_n_results_final.size());
}

Uint32 AggInterpreter::CopyOneVecCandidateToSignal(Signal* signal) {
  if (m_next_send_idx >= 0) {
    Candidate* next_send = m_vec_top_n_results_final[m_next_send_idx];
    if (next_send->m_actual_buf_len > 3) {
      // Fast path
      AttributeHeader header = *(AttributeHeader*)(next_send->m_buf + next_send->m_actual_buf_len - 3);
      if (header.getAttributeId() == AttributeHeader::VEC_DISTANCE &&
          *(double*)(next_send->m_buf + next_send->m_actual_buf_len - 2) == 721.721) {
        /* Fill in the vec_closes_ to the reserved area which contains the magic word 721.721*/
        *(double*)(next_send->m_buf + next_send->m_actual_buf_len - 2) = next_send->m_distance;
      }
    } else {
      // TODO (Zhao)
      // It doesn’t seem to work with index scans, since in an index scan
      // there is an NdbRecord (READ_PACKED) packet at the beginning of the result.
      Uint32 pos = 0;
      while (pos < next_send->m_actual_buf_len) {
        AttributeHeader header = *(AttributeHeader*)(next_send->m_buf + pos);
        if (header.getAttributeId() == AttributeHeader::VEC_DISTANCE) {
#ifdef DEBUG_VS_INTERP
          double value = *(double*)(next_send->m_buf + pos + 1);
          VS_INTERP_TRACE(m_table_id, m_frag_id,
                          "CopyOneToSignalForSending, attributeId: %d, value: %lf",
                          header.getAttributeId(), value);
#endif  // DEBUG_VS_INTERP
          *(double*)(next_send->m_buf + pos + 1) = next_send->m_distance;
        }
        pos += header.getDataSize() + 1;
      }
    }
    Uint32 copy_len = next_send->m_actual_buf_len;
    if (next_send != nullptr && next_send->m_actual_buf_len != 0) {
      memcpy((void*)(&signal->theData[25]), next_send->m_buf,
          next_send->m_actual_buf_len * sizeof(Uint32));
      VS_INTERP_TRACE(m_table_id, m_frag_id,
                      "CopyOneToSignalForSending, len: %u, idx: %d",
                      next_send->m_actual_buf_len, m_next_send_idx);
      m_vec_n_candidates_sent++;
      m_vec_size_candidates_sent += next_send->m_actual_buf_len;
    }

    m_vec_top_n_results_final[m_next_send_idx] = nullptr;
    m_next_send_idx--;
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
   * m_vec_max_rec_size is only used to calculate the preallocated memory size
   * for m_candidate_allocator, so it doesn’t make sense to set it
   * after m_candidate_allocator has already been initialized.
   */
  if (!m_candidate_allocator) {
    // 10% bigger
    m_vec_max_rec_size = static_cast<int>(std::ceil(size * 1.1));
    VS_INTERP_TRACE(m_table_id, m_frag_id, "Adjust vec_max_rec_size: %u -> %u",
                    size, m_vec_max_rec_size);
  }
}

Uint32 AggInterpreter::CandidateAllocator::g_max_results_size = 100 * 1024 * 1024; /*100 MB*/
Uint32 AggInterpreter::CandidateAllocator::g_segment_size = 1 * 1024 * 1024; /*1 MB*/

static inline size_t highest_power_of_two_leq(size_t x) {
  // x > 0
  return size_t(1) << (8 * sizeof(size_t) - 1 - __builtin_clzl(x));
}

Int32 AggInterpreter::CandidateAllocator::Init(Uint32 thread_id) {
  if (m_init) {
    return 0;
  }

  if (m_total_size > g_max_results_size) {
    g_eventLogger->warning(
        "Vector search result size %lu exceeds max pool %u",
        m_total_size, g_max_results_size);
    return ZAGG_VS_TOO_BIG_RESULT;
  }

  if (m_slot_size > g_segment_size) {
    g_eventLogger->error(
        "slot_size %lu > segment_size %u, cannot allocate candidates safely",
        m_slot_size, g_segment_size);
    return ZAGG_VS_TOO_BIG_RESULT;
  }

  size_t raw = g_segment_size / m_slot_size;
  assert(raw > 0);
  m_slots_per_full_segment = highest_power_of_two_leq(raw);
  m_shift_k = __builtin_ctzl(m_slots_per_full_segment);
  // m_slots_per_full_segment = g_segment_size / m_slot_size;
  // assert(m_slots_per_full_segment > 0);

  size_t full_segments = m_max_candidates / m_slots_per_full_segment;
  size_t last_slots = m_max_candidates % m_slots_per_full_segment;
  size_t last_size = last_slots * m_slot_size;

  [[maybe_unused]] size_t num_segments = full_segments + (last_slots > 0 ? 1 : 0);

  VS_INTERP_TRACE(m_table_id, m_frag_id,
      "Init CandidateAllocator: slot_size=%lu, slots_per_full_seg=%lu, "
      "full_segments=%lu, last_slots=%lu",
      m_slot_size, m_slots_per_full_segment, full_segments, last_slots);

  assert(num_segments <= MAX_CANDIDATE_SEGMENTS);

  for (size_t i = 0; i < full_segments; i++) {
    void* page_ptr = lc_ndbd_pool_malloc(g_segment_size, RG_QUERY_MEMORY,
        thread_id, false);
    if (!page_ptr) {
      g_eventLogger->error("Failed allocating segment %lu", i);
      return ZAGG_ALLOC_MEM_FAILED;
    }
    m_segments[m_n_segments++] = {(char*)page_ptr, g_segment_size};
  }

  if (last_size > 0) {
    void* page_ptr = lc_ndbd_pool_malloc(last_size, RG_QUERY_MEMORY,
        thread_id, false);
    if (!page_ptr) {
      g_eventLogger->error("Failed allocating last segment");
      return ZAGG_ALLOC_MEM_FAILED;
    }
    m_segments[m_n_segments++] = {(char*)page_ptr, last_size};
  }

  m_init = true;
  return 0;
}

AggInterpreter::Candidate* AggInterpreter::CandidateAllocator::Allocate(
    double distance, const Uint32* tuple, Uint32 actual_buf_len) {
  if (m_next_index >= m_max_candidates) {
    return nullptr;
  }

  // size_t seg_id = m_next_index / m_slots_per_full_segment;
  // size_t slot_off = m_next_index % m_slots_per_full_segment;
	size_t seg_id   = m_next_index >> m_shift_k;
	size_t slot_off = m_next_index & (m_slots_per_full_segment - 1);

  assert(seg_id < m_n_segments);

  char* base = m_segments[seg_id].ptr;
  char* ptr = base + slot_off * m_slot_size;

  assert(ptr + m_slot_size <= base + m_segments[seg_id].size);

  VS_INTERP_TRACE(m_table_id, m_frag_id,
      "Alloc idx=%lu -> seg=%lu slot_off=%lu",
      m_next_index, seg_id, slot_off);

  Candidate* c = new (ptr) Candidate(distance, actual_buf_len, m_next_index);
  c->Init(tuple);

  if (!m_reuse_started) {
    ++m_next_index;
  }
  if (!m_reuse_started && m_next_index == m_max_candidates) {
    VS_INTERP_TRACE(m_table_id, m_frag_id, "CandidateAllocator reuse started");
    m_next_index = 0;
    m_reuse_started = true;
  }

  return c;
  lc_ndbd_pool_free(ptr);
}

void AggInterpreter::initChunkAllocator(Uint32 thread_id,
                                        Uint32 budget_pages,
                                        Uint32 available_pages) {
  m_thread_id = thread_id;
  m_memory_budget = budget_pages * MEM_CHUNK_SIZE;
  m_budget_increment = m_memory_budget;
  m_total_available = available_pages * MEM_CHUNK_SIZE;
  m_chunks = nullptr;
  m_chunks_tail = nullptr;
  m_current_chunk = nullptr;
  m_total_chunk_bytes = 0;
}

bool AggInterpreter::bookMoreMemory() {
  Uint32 new_budget = m_memory_budget + m_budget_increment;
  if (new_budget > m_total_available) {
    return false;
  }
  m_memory_budget = new_budget;
  return true;
}

MemChunk* AggInterpreter::allocNewChunk() {
  if (m_total_chunk_bytes + MEM_CHUNK_SIZE > m_memory_budget) {
    if (!bookMoreMemory()) {
      return nullptr;
    }
  }
  void* page = lc_ndbd_pool_malloc(MEM_CHUNK_SIZE, RG_QUERY_MEMORY,
                                   m_thread_id, false);
  if (page == nullptr) {
    return nullptr;
  }
  MemChunk* chunk = static_cast<MemChunk*>(page);
  chunk->data = static_cast<char*>(page) + sizeof(MemChunk);
  chunk->capacity = MEM_CHUNK_SIZE - sizeof(MemChunk);
  chunk->used = 0;
  chunk->live_groups = 0;
  chunk->group_list = nullptr;
  chunk->next = m_chunks;
  chunk->prev = nullptr;
  if (m_chunks != nullptr) {
    m_chunks->prev = chunk;
  } else {
    m_chunks_tail = chunk;
  }
  m_chunks = chunk;
  m_total_chunk_bytes += MEM_CHUNK_SIZE;
  return chunk;
}

char* AggInterpreter::allocGroupData(Uint32 len, Uint32 key_len) {
  Uint32 total = ((GROUP_LINK_OVERHEAD + len) + 7) & ~7u;
  MemChunk* chunk = m_current_chunk;
  if (chunk == nullptr || chunk->used + total > chunk->capacity) {
    chunk = allocNewChunk();
    if (chunk == nullptr) {
      return nullptr;
    }
    m_current_chunk = chunk;
    if (total > chunk->capacity) {
      return nullptr;
    }
  }
  Uint32 offset = chunk->used;
  char* raw = chunk->data + offset;
  chunk->used += total;
  chunk->live_groups++;

  // Link into chunk's per-group list (LIFO).
  // Layout: [chunk_next (8)] [hash_next (8)] [key_len (4)] [chunk_offset (4)]
  *reinterpret_cast<char**>(raw) = chunk->group_list;
  *reinterpret_cast<char**>(raw + sizeof(char*)) = nullptr;  // hash_next
  *reinterpret_cast<Uint32*>(raw + 2 * sizeof(char*)) = key_len;
  *reinterpret_cast<Uint32*>(raw + 2 * sizeof(char*) + sizeof(Uint32)) = offset;
  chunk->group_list = raw;

  return raw + GROUP_LINK_OVERHEAD;
}

void AggInterpreter::freeGroupData(char* ptr) {
  char* raw = ptr - GROUP_LINK_OVERHEAD;
  Uint32 offset = *reinterpret_cast<Uint32*>(raw + 2 * sizeof(char*) + sizeof(Uint32));
  MemChunk* chunk = reinterpret_cast<MemChunk*>(raw - offset - sizeof(MemChunk));
  chunk->live_groups--;
  if (chunk->live_groups == 0) {
    // O(1) unlink via doubly-linked list.
    if (chunk->prev != nullptr) {
      chunk->prev->next = chunk->next;
    } else {
      m_chunks = chunk->next;
    }
    if (chunk->next != nullptr) {
      chunk->next->prev = chunk->prev;
    } else {
      m_chunks_tail = chunk->prev;
    }
    if (m_current_chunk == chunk) {
      m_current_chunk = m_chunks;
    }
    m_total_chunk_bytes -= MEM_CHUNK_SIZE;
    lc_ndbd_pool_free(chunk);
  }
}

void AggInterpreter::freeAllChunks() {
  MemChunk* chunk = m_chunks;
  while (chunk != nullptr) {
    MemChunk* next = chunk->next;
    lc_ndbd_pool_free(chunk);
    chunk = next;
  }
  m_chunks = nullptr;
  m_chunks_tail = nullptr;
  m_current_chunk = nullptr;
  m_total_chunk_bytes = 0;
}
