/*
 * Copyright (c) 2024, 2026, Hopsworks and/or its affiliates.
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
#include <NdbSqlUtil.hpp>
#include <Interpreter.hpp>

Uint32 AggInterpreter::g_attr_read_buf_len_ = ATTR_READ_BUF_WORD_SIZE;
Uint32 AggInterpreter::g_result_header_size_ = 3 * sizeof(Uint32);
Uint32 AggInterpreter::g_result_header_size_per_group_ = sizeof(Uint32);

/*
 * GBHashEntryCmp::operator() — charset-aware comparison for std::map ordering.
 * Duplicated from NdbAggregator.cpp (kernel and API are separate link targets).
 */
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

  /* 0. Prepare the buffer and copy the program */
  assert(m_prog_len <= MAX_AGG_PROGRAM_WORD_SIZE);
  m_prog = m_prog_buf;
  memcpy(m_prog, prog, m_prog_len * sizeof(Uint32));
  memset(m_attr_read_buf, 0, sizeof(m_attr_read_buf));
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

  /* Skip the next 5 reserved Uint32 elements (word 3 = VS flag, not set for PA) */
  assert((m_prog[m_cur_pos] & 0x80000000) == 0);
  assert(m_prog[m_cur_pos] == 0);
  m_cur_pos += 5;

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
    m_gb_cmp_ctx.n_cols = 0;
    m_gb_cmp_ctx.all_binary_cmp = false;
    m_gb_map_buf = std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>(
                      GBHashEntryCmp(&m_gb_cmp_ctx));
    m_gb_map = &m_gb_map_buf;
    m_alloc_len = 0;
  }

  /*
   * 4. Reset all aggregation results
   */
  if (m_n_agg_results) {
    assert(m_n_agg_results <= MAX_AGG_N_RESULTS);
    m_agg_results = m_agg_results_buf;
    for (Uint32 i = 0; i < m_n_agg_results; i++) {
      m_agg_results[i].type = NDB_TYPE_UNDEFINED;
      m_agg_results[i].value.val_int64 = 0;
      m_agg_results[i].is_unsigned = false;
      m_agg_results[i].is_null = true;
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
  OptimizeProgramBuffer(m_prog, m_prog_len, m_agg_prog_start_pos);
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
        Dbtup::KeyReqStruct* req_struct) {
  if (!m_inited || req_struct->read_length != 0) {
    g_eventLogger->debug("AggInterpreter::ProcessRec ZAGG_OTHER_ERROR at entry: "
            "inited=%d, read_length=%u",
            m_inited, req_struct->read_length);
    return ZAGG_OTHER_ERROR;
  }

  AggResItem* agg_res_ptr = nullptr;
  if (m_n_gb_cols) {
    if (!m_gb_cmp_inited) {
      initGBCmpCtx(block_tup, req_struct);
    }

    AttributeHeader* header = nullptr;
    m_attr_read_pos = 0;
    for (Uint32 i = 0; i < m_n_gb_cols; i++) {
      int ret = block_tup->readAttributes(req_struct, &(m_gb_cols[i]), 1,
                    m_attr_read_buf + m_attr_read_pos, g_attr_read_buf_len_ - m_attr_read_pos);
      if (ret < 0) {
        DEB_AGG(("read group by column error: %d", ret));
        return -ret;
      }
      header = reinterpret_cast<AttributeHeader*>(m_attr_read_buf + m_attr_read_pos);
      m_attr_read_pos += (1 + header->getDataSize());
    }

    Uint32 len_in_char = m_attr_read_pos * sizeof(Uint32);
    GBHashEntry lookup_key;
    lookup_key.ptr = reinterpret_cast<char*>(m_attr_read_buf);
    lookup_key.len = len_in_char;
    auto it = m_gb_map->find(lookup_key);
    if (it != m_gb_map->end()) {
      agg_res_ptr = reinterpret_cast<AggResItem*>(it->second.ptr);
      PA_INTERP_TRACE(m_frag_id,
                      "Found GBHashEntry, len: %u", len_in_char);
    } else {
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
      Uint32 total_alloc = len_in_char +
                           m_n_agg_results * sizeof(AggResItem);
      char* agg_rec = MemAlloc(total_alloc);
      if (agg_rec == nullptr) {
        return ZAGG_OTHER_ERROR;
      }
      memset(agg_rec, 0, total_alloc);
      memcpy(agg_rec, reinterpret_cast<char*>(m_attr_read_buf), len_in_char);

      GBHashEntry map_key;
      map_key.ptr = agg_rec;
      map_key.len = len_in_char;
      GBHashEntry map_val;
      map_val.ptr = agg_rec + len_in_char;
      map_val.len = m_n_agg_results * sizeof(AggResItem);
      m_gb_map->insert(std::make_pair(map_key, map_val));
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
          col_index = (value & 0x0000FFFF) << 16;
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

      case kOpBranchRegLt:
      case kOpBranchRegLe:
      case kOpBranchRegGt:
      case kOpBranchRegGe:
      case kOpBranchRegEq:
      case kOpBranchRegNe:
      {
        Uint32 ra = (value >> 20) & 0x0F;
        Uint32 rb = (value >> 16) & 0x0F;
        Uint32 skip_count = value & 0xFFFF;
        double va = 0, vb = 0;
        if (m_registers[ra].type == NDB_TYPE_BIGINT) {
          va = m_registers[ra].is_unsigned
              ? (double)m_registers[ra].value.val_uint64
              : (double)m_registers[ra].value.val_int64;
        } else if (m_registers[ra].type == NDB_TYPE_DOUBLE) {
          va = m_registers[ra].value.val_double;
        }
        if (m_registers[rb].type == NDB_TYPE_BIGINT) {
          vb = m_registers[rb].is_unsigned
              ? (double)m_registers[rb].value.val_uint64
              : (double)m_registers[rb].value.val_int64;
        } else if (m_registers[rb].type == NDB_TYPE_DOUBLE) {
          vb = m_registers[rb].value.val_double;
        }
        bool do_skip = false;
        if (m_registers[ra].is_null || m_registers[rb].is_null) {
          do_skip = true;
        } else {
          switch (op) {
          case kOpBranchRegLt: do_skip = (va < vb); break;
          case kOpBranchRegLe: do_skip = (va <= vb); break;
          case kOpBranchRegGt: do_skip = (va > vb); break;
          case kOpBranchRegGe: do_skip = (va >= vb); break;
          case kOpBranchRegEq: do_skip = (va == vb); break;
          case kOpBranchRegNe: do_skip = (va != vb); break;
          default: break;
          }
        }
        if (do_skip) exec_pos += skip_count;
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

      g_eventLogger->info("Num of groups: %zu, Aggregation results:",
                          m_gb_map->size());
      for (auto iter = m_gb_map->begin(); iter != m_gb_map->end(); ++iter) {
        int pos = 0;
        char* data = iter->first.ptr;
        sprintf(log_buf, "(");
        for (Uint32 i = 0; i < m_n_gb_cols; i++) {
          if (i != m_n_gb_cols - 1) {
            sprintf(log_buf + strlen(log_buf), "%u: %p, ", i, data + pos);
          } else {
            sprintf(log_buf + strlen(log_buf), "%u: %p): ", i, data + pos);
          }
        }

        AggResItem* item = reinterpret_cast<AggResItem*>(iter->second.ptr);
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


void AggInterpreter::initGBCmpCtx(Dbtup* block_tup,
                                  Dbtup::KeyReqStruct* req_struct) {
  m_gb_cmp_ctx.n_cols = m_n_gb_cols;
  m_gb_cmp_ctx.all_binary_cmp = true;
  for (Uint32 i = 0; i < m_n_gb_cols; i++) {
    Uint32 attr_id = m_gb_cols[i] >> 16;
    GBColMeta &meta = m_gb_cmp_ctx.col_meta[i];

    const Uint32* attrDesc = req_struct->tablePtrP->tabDescriptor +
        attr_id * ZAD_SIZE;
    meta.typeId = AttributeDescriptor::getType(attrDesc[0]);
    meta.cs = nullptr;
    if (AttributeOffset::getCharsetFlag(attrDesc[1])) {
      Uint32 csPos = AttributeOffset::getCharsetPos(attrDesc[1]);
      meta.cs = req_struct->tablePtrP->charsetArray[csPos];
      m_gb_cmp_ctx.all_binary_cmp = false;
    }
  }
  m_gb_cmp_inited = true;
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
    data_buf[pos++] = AttributeHeader::AGG_RESULT << 16 | 0x0721;
    data_buf[pos++] = m_n_gb_cols << 16 | m_n_agg_results;
    Uint32 n_groups_pos = pos++;
    const Uint32 v_len = val_len();
    Uint32 n_groups = 0;
    for (auto iter = m_gb_map->begin(); iter != m_gb_map->end();) {
      Uint32 key_len = iter->first.len;
      assert(key_len % 4 == 0 && key_len < 0xFFFF);
      assert(v_len % 4 == 0 && v_len < 0xFFFF);
      data_buf[pos++] = key_len << 16 | v_len;
      MEMCOPY_NO_WORDS(&data_buf[pos], iter->first.ptr,
          key_len >> 2);
      MEMCOPY_NO_WORDS(&data_buf[pos + (key_len >> 2)], iter->second.ptr,
          v_len >> 2);
      pos += ((key_len + v_len) >> 2);
      iter = m_gb_map->erase(iter);
      n_groups++;
    }
    data_buf[n_groups_pos] = n_groups;
    m_alloc_len = 0;
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

char* AggInterpreter::MemAlloc(Uint32 len) {
  if (m_alloc_len + len > MAX_AGG_RESULT_BATCH_BYTES) {
    return nullptr;
  }
  char* ptr = &(m_mem_buf[m_alloc_len]);
  m_alloc_len += len;
  return ptr;
}
