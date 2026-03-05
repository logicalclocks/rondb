/*
 * Copyright (c) 2025, 2026, Hopsworks and/or its affiliates.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.
 *
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
#include "JoinAggInterpreter.hpp"
#include "InterpreterCommonOp.hpp"
#include "util/require.h"
#include "decimal.h"
#include "Dbtup.hpp"
#include "../dblqh/Dblqh.hpp"
#include <NdbSqlUtil.hpp>
#include <Interpreter.hpp>

#define ATTR_READ_BUF_WORD_SIZE 2048

Uint32 JoinAggInterpreter::g_attr_read_buf_len_ = ATTR_READ_BUF_WORD_SIZE;
Uint32 JoinAggInterpreter::g_result_header_size_ = 3 * sizeof(Uint32);
Uint32 JoinAggInterpreter::g_result_header_size_per_group_ = sizeof(Uint32);

// Per-group header prepended by allocGroupData.
static const Uint32 GROUP_LINK_OVERHEAD = 24;

/*
 * Debug macros
 */
#if (defined(VM_TRACE) || defined(ERROR_INSERT))
#undef DEBUG_PA_INTERP
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
#endif

/*
 * Static helper functions — same as in AggInterpreter.cpp
 */
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

/* Aggregate functions — identical to AggInterpreter.cpp */
static Int32 Sum(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED) {
    *res = a;
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }
  if (a.is_null) return 1;
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
        if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) return -1;
        else res_unsigned = true;
      } else {
        if ((Uint64)val0 > (Uint64)(LLONG_MAX)) res_unsigned = true;
      }
    } else {
      if (res->is_unsigned) {
        if (val0 >= 0) {
          if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) return -1;
          else res_unsigned = true;
        } else {
          if ((Uint64)val1 > (Uint64)(LLONG_MAX)) res_unsigned = true;
        }
      } else {
        if (val0 >= 0 && val1 >= 0) res_unsigned = true;
        else if (val0 < 0 && val1 < 0 && res_val >= 0) return -1;
      }
    }
    bool unsigned_flag = (a.is_unsigned | res->is_unsigned);
    if ((unsigned_flag && !res_unsigned && res_val < 0) ||
        (!unsigned_flag && res_unsigned &&
         (Uint64)res_val > (Uint64)LLONG_MAX)) {
      return -1;
    } else {
      if (unsigned_flag) res->value.val_uint64 = res_val;
      else res->value.val_int64 = res_val;
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
    if (std::isfinite(res_val)) res->value.val_double = res_val;
    else return -1;
    res->is_unsigned = false;
  }
  res->type = res_type;
  res->is_null = false;
  return 0;
}

static Int32 SumBigint(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) return 1;
  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_BIGINT;
    res->value.val_int64 = a.value.val_int64;
    res->is_unsigned = a.is_unsigned;
    res->is_null = false;
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }
  Int64 val0 = a.value.val_int64;
  Int64 val1 = res->value.val_int64;
  Int64 res_val = static_cast<Uint64>(val0) + static_cast<Uint64>(val1);
  bool res_unsigned = false;
  if (a.is_unsigned) {
    if (res->is_unsigned || val1 >= 0) {
      if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) return -1;
      res_unsigned = true;
    } else {
      if ((Uint64)val0 > (Uint64)(LLONG_MAX)) res_unsigned = true;
    }
  } else {
    if (res->is_unsigned) {
      if (val0 >= 0) {
        if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) return -1;
        res_unsigned = true;
      } else {
        if ((Uint64)val1 > (Uint64)(LLONG_MAX)) res_unsigned = true;
      }
    } else {
      if (val0 >= 0 && val1 >= 0) res_unsigned = true;
      else if (val0 < 0 && val1 < 0 && res_val >= 0) return -1;
    }
  }
  bool unsigned_flag = (a.is_unsigned | res->is_unsigned);
  if ((unsigned_flag && !res_unsigned && res_val < 0) ||
      (!unsigned_flag && res_unsigned &&
       (Uint64)res_val > (Uint64)LLONG_MAX)) {
    return -1;
  }
  if (unsigned_flag) res->value.val_uint64 = res_val;
  else res->value.val_int64 = res_val;
  res->is_unsigned = unsigned_flag;
  return 0;
}

static Int32 SumDouble(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) return 1;
  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_DOUBLE;
    res->value.val_double = a.value.val_double;
    res->is_unsigned = false;
    res->is_null = false;
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }
  double res_val = a.value.val_double + res->value.val_double;
  if (unlikely(!std::isfinite(res_val))) return -1;
  res->value.val_double = res_val;
  return 0;
}

static Int32 Max(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED || res->is_null) {
    *res = a;
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }
  if (a.is_null) return 1;
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
  return 0;
}

static Int32 MaxBigint(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) return 1;
  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_BIGINT;
    res->value.val_int64 = a.value.val_int64;
    res->is_unsigned = a.is_unsigned;
    res->is_null = false;
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }
  if (!a.is_unsigned && !res->is_unsigned) {
    if (a.value.val_int64 > res->value.val_int64)
      res->value.val_int64 = a.value.val_int64;
  } else if (a.is_unsigned && res->is_unsigned) {
    if (a.value.val_uint64 > res->value.val_uint64)
      res->value.val_uint64 = a.value.val_uint64;
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
    if (a.value.val_int64 >= 0) {
      if (static_cast<Uint64>(a.value.val_int64) > res->value.val_uint64)
        res->value.val_uint64 = static_cast<Uint64>(a.value.val_int64);
    }
  }
  return 0;
}

static Int32 MaxDouble(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) return 1;
  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_DOUBLE;
    res->value.val_double = a.value.val_double;
    res->is_unsigned = false;
    res->is_null = false;
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }
  if (a.value.val_double > res->value.val_double)
    res->value.val_double = a.value.val_double;
  return 0;
}

static Int32 Min(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED || res->is_null) {
    *res = a;
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }
  if (a.is_null) return 1;
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
  return 0;
}

static Int32 MinBigint(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) return 1;
  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_BIGINT;
    res->value.val_int64 = a.value.val_int64;
    res->is_unsigned = a.is_unsigned;
    res->is_null = false;
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }
  if (!a.is_unsigned && !res->is_unsigned) {
    if (a.value.val_int64 < res->value.val_int64)
      res->value.val_int64 = a.value.val_int64;
  } else if (a.is_unsigned && res->is_unsigned) {
    if (a.value.val_uint64 < res->value.val_uint64)
      res->value.val_uint64 = a.value.val_uint64;
  } else if (a.is_unsigned && !res->is_unsigned) {
    if (res->value.val_int64 < 0) {
    } else {
      if (a.value.val_uint64 < static_cast<Uint64>(res->value.val_int64)) {
        res->value.val_uint64 = a.value.val_uint64;
        res->is_unsigned = true;
      }
    }
  } else {
    if (a.value.val_int64 < 0) {
      res->value.val_int64 = a.value.val_int64;
      res->is_unsigned = false;
    } else {
      if (static_cast<Uint64>(a.value.val_int64) < res->value.val_uint64)
        res->value.val_uint64 = static_cast<Uint64>(a.value.val_int64);
    }
  }
  return 0;
}

static Int32 MinDouble(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) return 1;
  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_DOUBLE;
    res->value.val_double = a.value.val_double;
    res->is_unsigned = false;
    res->is_null = false;
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }
  if (a.value.val_double < res->value.val_double)
    res->value.val_double = a.value.val_double;
  return 0;
}

static Int32 Count(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED) {
    res->type = NDB_TYPE_BIGINT;
    res->value.val_uint64 = 0;
    res->is_unsigned = true;
    res->is_null = false;
  }
  if (a.is_null) return 1;
  assert(res->type == NDB_TYPE_BIGINT &&
      res->is_null == false && res->is_unsigned == true);
  res->value.val_uint64 += 1;
  return 0;
}

/**
 * validateEmbeddedProgram — same as AggInterpreter version
 */
bool JoinAggInterpreter::validateEmbeddedProgram(
    const Uint32* emb_prog, Uint32 emb_len) {
  Uint32 pc = 0;
  while (pc < emb_len) {
    Uint32 instr = emb_prog[pc];
    Uint32 opCode = Interpreter::getOpCode(instr);

    switch (opCode) {
      case Interpreter::READ_ATTR_INTO_REG:
      case Interpreter::LOAD_CONST_NULL:
      case Interpreter::LOAD_CONST16:
      case Interpreter::LOAD_CONST32:
      case Interpreter::LOAD_CONST64:
      case Interpreter::ADD_REG_REG:
      case Interpreter::SUB_REG_REG:
      case Interpreter::BRANCH:
      case Interpreter::BRANCH_REG_EQ_NULL:
      case Interpreter::BRANCH_REG_NE_NULL:
      case Interpreter::BRANCH_EQ_REG_REG:
      case Interpreter::BRANCH_NE_REG_REG:
      case Interpreter::BRANCH_LT_REG_REG:
      case Interpreter::BRANCH_LE_REG_REG:
      case Interpreter::BRANCH_GT_REG_REG:
      case Interpreter::BRANCH_GE_REG_REG:
      case Interpreter::EXIT_OK:
      case Interpreter::BRANCH_ATTR_OP_ARG:
      case Interpreter::BRANCH_ATTR_EQ_NULL:
      case Interpreter::BRANCH_ATTR_NE_NULL:
      case Interpreter::WRITE_INTERPRETER_OUTPUT:
        break;
      default:
        g_eventLogger->warning(
            "validateEmbeddedProgram: forbidden opcode %u at pc=%u",
            opCode, pc);
        return false;
    }

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
      Uint32 target = pc + offset;
      if (target >= emb_len) {
        g_eventLogger->warning(
            "validateEmbeddedProgram: branch target %u out of bounds "
            "(emb_len=%u) at pc=%u", target, emb_len, pc);
        return false;
      }
    }

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

bool JoinAggInterpreter::Init(const Uint32* prog) {
  if (m_inited) {
    return true;
  }

  require(prog != nullptr);

  /*
   * Allocate all large buffers in a single block.
   */
  if (m_buf_block == nullptr) {
    static const Uint32 BUF_BLOCK_SIZE =
      ATTR_READ_BUF_WORD_SIZE * sizeof(Uint32) +
      MAX_AGG_PROGRAM_WORD_SIZE * sizeof(Uint32) +
      MAX_AGG_N_GROUPBY_COLS * sizeof(Uint32) +
      MAX_AGG_N_RESULTS * sizeof(AggResItem) +
      sizeof(JoinGBHashTable) +
      MAX_AGG_N_GROUPBY_COLS * sizeof(GBColTypeInfo) +
      MAX_AGG_N_RESULTS * sizeof(Uint8);

    m_buf_block = lc_ndbd_pool_malloc(BUF_BLOCK_SIZE, RG_QUERY_MEMORY,
                                       m_thread_id, false);
    if (m_buf_block == nullptr) {
      g_eventLogger->error("Alloc mem for JoinAggInterpreter buffers failed");
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
    m_gb_map_buf = new (p) JoinGBHashTable();
    p += sizeof(JoinGBHashTable);
    m_gb_types = reinterpret_cast<GBColTypeInfo*>(p);
    p += MAX_AGG_N_GROUPBY_COLS * sizeof(GBColTypeInfo);
    m_cached_agg_ops = reinterpret_cast<Uint8*>(p);
    memset(m_gb_types, 0, MAX_AGG_N_GROUPBY_COLS * sizeof(GBColTypeInfo));
  }

  assert(m_prog_len <= MAX_AGG_PROGRAM_WORD_SIZE);
  m_prog = m_prog_buf;
  memcpy(m_prog, prog, m_prog_len * sizeof(Uint32));
  memset(m_attr_read_buf, 0, ATTR_READ_BUF_WORD_SIZE * sizeof(Uint32));
  memset(m_decimal_buf, 0, sizeof(Int32) * DECIMAL_BUFF_LENGTH);
  m_decimal.buf = m_decimal_buf;
  m_decimal.len = DECIMAL_BUFF_LENGTH;

  Uint32 value = 0;
  value = m_prog[m_cur_pos++];
  assert(((value & 0xFFFF0000) >> 16) == 0x0721);
  assert((value & 0xFFFF) == m_prog_len);

  value = m_prog[m_cur_pos++];
  m_n_gb_cols = (value >> 16) & 0xFFFF;
  m_n_agg_results = value & 0xFFFF;

  Uint32 version = m_prog[m_cur_pos++];
  if (version > PUSHDOWN_AGGREGATION_VERSION) {
    g_eventLogger->warning("Pushdown aggregation program version(%u) is "
                           "not compatible with "
                           "the version (%u) on data node",
                           version, PUSHDOWN_AGGREGATION_VERSION);
    return true;
  }

  assert((m_prog[m_cur_pos] & 0x80000000) == 0);
  assert(m_prog[m_cur_pos] == 0);
  m_cur_pos += 5;

  if (m_n_gb_cols) {
    assert(m_n_gb_cols <= MAX_AGG_N_GROUPBY_COLS);
    m_gb_cols = m_gb_cols_buf;

    Uint32 i = 0;
    while (i < m_n_gb_cols && m_cur_pos < m_prog_len) {
      m_gb_cols[i++] = m_prog[m_cur_pos++];
    }
    m_gb_map_buf->clear();
    m_gb_map = m_gb_map_buf;
    m_gb_map->init(JOIN_AGG_HASH_BUCKET_COUNT);
  }

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

  /* Validate embedded interpreter blocks */
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
              "JoinAggInterpreter::Init: embedded program validation failed "
              "at scan_pos=%u", scan_pos);
          m_inited = false;
          return false;
        }
        scan_pos += 1 + emb_len;
      } else if (op == kOpLoadConst) {
        scan_pos += 3;
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

bool JoinAggInterpreter::OptimizeProgram() {
  if (!m_inited) {
    return false;
  }

  DataType reg_types[kRegTotal];
  for (Uint32 i = 0; i < kRegTotal; i++) {
    reg_types[i] = NDB_TYPE_UNDEFINED;
  }

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
          reg_types[reg_index] = NDB_TYPE_UNDEFINED;
          exec_pos++;
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
        exec_pos += 2;
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
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE) new_op = kOpSumDouble;
        else if (reg_types[reg_index] == NDB_TYPE_BIGINT) new_op = kOpSumBigint;
        if (new_op != op)
          m_prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        break;

      case kOpMax:
        reg_index = (value & 0x000F0000) >> 16;
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE) new_op = kOpMaxDouble;
        else if (reg_types[reg_index] == NDB_TYPE_BIGINT) new_op = kOpMaxBigint;
        if (new_op != op)
          m_prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        break;

      case kOpMin:
        reg_index = (value & 0x000F0000) >> 16;
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE) new_op = kOpMinDouble;
        else if (reg_types[reg_index] == NDB_TYPE_BIGINT) new_op = kOpMinBigint;
        if (new_op != op)
          m_prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        break;

      case kOpCount:
        break;

      case kOpEmbeddedInterp:
      {
        Uint32 emb_len = value & 0xFFFF;
        exec_pos += emb_len;
        break;
      }

      case kOpSkip:
        break;

      default:
        break;
    }
    exec_pos++;
  }

  return true;
}

/*
 * ProcessRec for join aggregation — includes linked attribute resolution
 */
Int32 JoinAggInterpreter::ProcessRec(Dbtup* block_tup,
        Dbtup::KeyReqStruct* req_struct) {
  if (!m_inited) {
    g_eventLogger->debug("AggInterpreter::ProcessRec ZAGG_OTHER_ERROR: not inited");
    return ZAGG_OTHER_ERROR;
  }
  if (!m_null_local_columns) {
    if (req_struct->read_length != 0) {
      g_eventLogger->debug("AggInterpreter::ProcessRec ZAGG_OTHER_ERROR at entry: "
              "read_length=%u", req_struct->read_length);
      return ZAGG_OTHER_ERROR;
    }
  }

  AggResItem* agg_res_ptr = nullptr;
  if (m_n_gb_cols) {
    if (!m_gb_types_inited) {
      if (m_null_local_columns) {
        initGBTypesForNullLocal(block_tup);
      } else {
        Int32 err = initGBTypes(block_tup, req_struct);
        if (unlikely(err != 0)) return err;
      }
    }
    char* agg_rec = nullptr;

    AttributeHeader* header = nullptr;
    m_attr_read_pos = 0;
    for (Uint32 i = 0; i < m_n_gb_cols; i++) {
      Uint32 attr_id = m_gb_cols[i] >> 16;
      if ((attr_id & 0x8000) != 0 && m_linked_attr_data != nullptr) {
        Uint32 position = attr_id & 0x7FFF;
        const Uint32* p = m_linked_attr_data;
        const Uint32* p_end = m_linked_attr_data + m_linked_attr_len;
        Uint32 pos_count = 0;
        while (p < p_end) {
          if (pos_count == position) break;
          p += 2;
          p += 1 + AttributeHeader::getDataSize(*p);
          pos_count++;
        }
        if (p >= p_end) {
          g_eventLogger->debug("JoinAggInterpreter::ProcessRec ZAGG_OTHER_ERROR: "
              "Linked GROUP BY position %u not found in linked buffer "
              "(linked_len=%u)", position, m_linked_attr_len);
          return ZAGG_OTHER_ERROR;
        }
        p += 2;
        Uint32 words = 1 + AttributeHeader::getDataSize(*p);
        memcpy(m_attr_read_buf + m_attr_read_pos, p, words * sizeof(Uint32));
        header = reinterpret_cast<AttributeHeader*>(m_attr_read_buf + m_attr_read_pos);
        m_attr_read_pos += words;
      } else {
        if (m_null_local_columns) {
          AttributeHeader null_ah(attr_id, 0);
          m_attr_read_buf[m_attr_read_pos] = null_ah.m_value;
          header = reinterpret_cast<AttributeHeader*>(
              m_attr_read_buf + m_attr_read_pos);
          m_attr_read_pos += 1;
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
    }

    Uint32 len_in_char = m_attr_read_pos * sizeof(Uint32);
    char* found = m_gb_map->find(reinterpret_cast<char*>(m_attr_read_buf), len_in_char);
    if (found != nullptr) {
      header = reinterpret_cast<AttributeHeader*>(found);
      agg_res_ptr = reinterpret_cast<AggResItem*>(found + len_in_char);
    } else {
      if (m_max_groups > 0 && m_n_groups >= m_max_groups) {
        return AGG_EVICT_NEEDED;
      }
      if (req_struct != nullptr) {
        req_struct->read_length = (len_in_char +
                         m_n_agg_results * sizeof(AggResItem)) / sizeof(Int32);
      }

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

      assert(m_n_agg_results <= MAX_AGG_N_RESULTS);
      for (Uint32 i = 0; i < m_n_agg_results; i++) {
        agg_res_ptr[i].type = NDB_TYPE_UNDEFINED;
        agg_res_ptr[i].value.val_int64 = 0;
        agg_res_ptr[i].is_unsigned = false;
        agg_res_ptr[i].is_null = true;
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
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;
      case kOpMinus:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegMinusReg(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;
      case kOpMul:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegMulReg(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;
      case kOpDiv:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegDivReg(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index], false);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;
      case kOpDivInt:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegDivReg(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index], true);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;
      case kOpMod:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegModReg(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;

      case kOpPlusBigint:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegPlusBigint(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;
      case kOpPlusDouble:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegPlusDouble(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;
      case kOpMinusBigint:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegMinusBigint(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;
      case kOpMinusDouble:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegMinusDouble(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;
      case kOpMulBigint:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegMulBigint(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;
      case kOpMulDouble:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegMulDouble(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;
      case kOpDivDouble:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegDivDouble(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;
      case kOpDivIntBigint:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        ret = RegDivIntBigint(m_registers[reg_index], m_registers[reg_index2],
                  &m_registers[reg_index]);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;

      case kOpLoadCol:
        type = (value & 0x03E00000) >> 21;
        is_unsigned = IsUnsigned(type);
        reg_index = (value & 0x000F0000) >> 16;
        {
          Uint32 col_id_raw = value & 0x0000FFFF;
          if ((col_id_raw & 0x8000) != 0 && m_linked_attr_data != nullptr) {
            Uint32 position = col_id_raw & 0x7FFF;
            const Uint32* p = m_linked_attr_data;
            const Uint32* p_end = m_linked_attr_data + m_linked_attr_len;
            Uint32 pos_count = 0;
            while (p < p_end) {
              if (pos_count == position) break;
              p += 2;
              p += 1 + AttributeHeader::getDataSize(*p);
              pos_count++;
            }
            if (p >= p_end) {
              g_eventLogger->debug("JoinAggInterpreter::ProcessRec ZAGG_OTHER_ERROR: "
                  "kOpLoadCol linked position %u not found in buffer "
                  "(linked_len=%u)", position, m_linked_attr_len);
              return ZAGG_OTHER_ERROR;
            }
            p += 2;
            Uint32 words = 1 + AttributeHeader::getDataSize(*p);
            memcpy(m_attr_read_buf + m_attr_read_pos, p, words * sizeof(Uint32));
            header = reinterpret_cast<AttributeHeader*>(m_attr_read_buf + m_attr_read_pos);
            attrDescriptor = nullptr;
          } else {
            if (m_null_local_columns) {
              AttributeHeader null_ah(col_id_raw, 0);
              m_attr_read_buf[m_attr_read_pos] = null_ah.m_value;
              header = reinterpret_cast<AttributeHeader*>(
                  m_attr_read_buf + m_attr_read_pos);
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
        }
        if (!TypeSupported(type)) {
          DEB_AGG(("Unsupported column type: %u", type));
          return ZAGG_COL_TYPE_UNSUPPORTED;
        }

        if (type == NDB_TYPE_DECIMAL ||
            type == NDB_TYPE_DECIMALUNSIGNED) {
          if (unlikely(exec_pos >= m_prog_len)) {
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
          m_registers[reg_index].value.val_int64 = 0;
          break;
        }
        switch (type) {
          case NDB_TYPE_TINYINT:
            m_registers[reg_index].value.val_int64 =
                *reinterpret_cast<Int8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
            break;
          case NDB_TYPE_SMALLINT:
            m_registers[reg_index].value.val_int64 =
                sint2korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            break;
          case NDB_TYPE_MEDIUMINT:
            m_registers[reg_index].value.val_int64 =
                sint3korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            break;
          case NDB_TYPE_INT:
            m_registers[reg_index].value.val_int64 =
                sint4korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            break;
          case NDB_TYPE_BIGINT:
            m_registers[reg_index].value.val_int64 =
                sint8korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            break;
          case NDB_TYPE_TINYUNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                *reinterpret_cast<Uint8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
            break;
          case NDB_TYPE_SMALLUNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                uint2korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            break;
          case NDB_TYPE_MEDIUMUNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                uint3korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            break;
          case NDB_TYPE_UNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                uint4korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            break;
          case NDB_TYPE_BIGUNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                uint8korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            break;
          case NDB_TYPE_FLOAT:
            m_registers[reg_index].value.val_double =
                floatget(reinterpret_cast<unsigned char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            break;
          case NDB_TYPE_DOUBLE:
            m_registers[reg_index].value.val_double =
                doubleget(reinterpret_cast<unsigned char*>(
                      &m_attr_read_buf[m_attr_read_pos + 1]));
            break;
          case NDB_TYPE_DECIMAL:
            assert(static_cast<Uint32>(decimal_bin_size(precision, scale)) ==
                header->getByteSize());
            dec_ret = bin2decimal(reinterpret_cast<const uchar*>(&m_attr_read_buf[m_attr_read_pos + 1]),
                      &m_decimal, precision, scale);
            if (dec_ret != E_DEC_OK) {
              if (dec_ret == E_DEC_OVERFLOW) return ZAGG_DECIMAL_PARSE_OVERFLOW;
              else return ZAGG_DECIMAL_PARSE_ERROR;
            }
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
              if (dec_ret == E_DEC_OVERFLOW) return ZAGG_DECIMAL_CONV_OVERFLOW;
              else return ZAGG_DECIMAL_CONV_ERROR;
            }
          break;
        case NDB_TYPE_DECIMALUNSIGNED:
            assert(static_cast<Uint32>(decimal_bin_size(precision, scale)) ==
                header->getByteSize());
            dec_ret = bin2decimal(reinterpret_cast<const uchar*>(&m_attr_read_buf[m_attr_read_pos + 1]),
                      &m_decimal, precision, scale);
            if (dec_ret != E_DEC_OK) {
              if (dec_ret == E_DEC_OVERFLOW) return ZAGG_DECIMAL_PARSE_OVERFLOW;
              else return ZAGG_DECIMAL_PARSE_ERROR;
            }
            assert(m_registers[reg_index].is_unsigned == true);
            if(unlikely(m_decimal.sign)) return ZAGG_DECIMAL_CONV_ERROR;
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
              if (dec_ret == E_DEC_OVERFLOW) return ZAGG_DECIMAL_CONV_OVERFLOW;
              else return ZAGG_DECIMAL_CONV_ERROR;
            }
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
        if (unlikely(exec_pos + 2 > m_prog_len)) return ZAGG_OTHER_ERROR;
        switch (type) {
          case NDB_TYPE_BIGINT:
            m_registers[reg_index].value.val_int64 =
                sint8korr(reinterpret_cast<char*>(&m_prog[exec_pos]));
            break;
          case NDB_TYPE_BIGUNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                uint8korr(reinterpret_cast<char*>(&m_prog[exec_pos]));
            break;
          case NDB_TYPE_DOUBLE:
            m_registers[reg_index].value.val_double =
                doubleget(reinterpret_cast<unsigned char*>(
                      &m_prog[exec_pos]));
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
        break;

      case kOpSum:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);
        ret = Sum(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
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

      case kOpSumBigint:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);
        ret = SumBigint(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;
      case kOpSumDouble:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);
        ret = SumDouble(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
        if (ret < 0) return ZAGG_MATH_OVERFLOW;
        break;
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
        if (exec_pos + emb_len > m_prog_len) return ZAGG_OTHER_ERROR;

        if (m_null_local_columns) {
          /*
           * Null-extended row: can't run embedded interpreter without
           * req_struct. Skip the embedded program (take THEN path).
           * All local column reads return NULL, so THEN-path aggregations
           * will correctly handle NULL inputs (SUM/MIN/MAX skip NULLs).
           */
          exec_pos += emb_len;
          break;
        }

        Uint32 saved_instr_count = req_struct->no_exec_instructions;
        req_struct->no_exec_instructions = 0;

        Uint32 local_tmpArea[16];
        int rc = block_tup->interpreterNextLab(
            req_struct->signal, req_struct,
            &m_prog[exec_pos], emb_len,
            nullptr, 0,
            local_tmpArea, 16);

        req_struct->no_exec_instructions = saved_instr_count;

        if (rc < 0) return ZAGG_EMBEDDED_INTERP_ERROR;

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

Int32 JoinAggInterpreter::processRecWithLinkedAttrs(
    Dbtup* block_tup,
    Dbtup::KeyReqStruct* req_struct,
    const Uint32* linked_attr_data,
    Uint32 linked_attr_len) {
  std::unique_lock<std::mutex> lock(m_mutex, std::defer_lock);
  if (m_use_mutex) lock.lock();

  m_linked_attr_data = linked_attr_data;
  m_linked_attr_len = linked_attr_len;

  Int32 ret = ProcessRec(block_tup, req_struct);

  m_linked_attr_data = nullptr;
  m_linked_attr_len = 0;
  return ret;
}

Int32 JoinAggInterpreter::evictOneGroup(Uint32* buf, Uint32 buf_words,
                                         Uint32* words_written) {
  if (m_gb_map == nullptr || m_gb_map->empty()) {
    return -1;
  }

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

Int32 JoinAggInterpreter::finalizeResults() {
  return 0;
}

Int32 JoinAggInterpreter::getResultData(Uint32* buffer, Uint32 buffer_size,
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

static void mergeAccumulators(AggResItem* dst, const AggResItem* src,
                              Uint32 n_agg_results,
                              const Uint8* agg_ops) {
  for (Uint32 i = 0; i < n_agg_results; i++) {
    if (src[i].type == NDB_TYPE_UNDEFINED) continue;
    if (dst[i].type == NDB_TYPE_UNDEFINED) { dst[i] = src[i]; continue; }
    if (src[i].is_null) continue;
    if (dst[i].is_null) { dst[i] = src[i]; continue; }
    switch (agg_ops[i]) {
      case kOpSum: case kOpSumBigint: case kOpSumDouble:
        if (dst[i].type == NDB_TYPE_BIGINT) {
          if (dst[i].is_unsigned) dst[i].value.val_uint64 += src[i].value.val_uint64;
          else dst[i].value.val_int64 += src[i].value.val_int64;
        } else {
          dst[i].value.val_double += src[i].value.val_double;
        }
        break;
      case kOpCount:
        dst[i].value.val_uint64 += src[i].value.val_uint64;
        break;
      case kOpMax: case kOpMaxBigint: case kOpMaxDouble:
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
      case kOpMin: case kOpMinBigint: case kOpMinDouble:
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
      case kOpSum: case kOpSumBigint: case kOpSumDouble:
      case kOpMax: case kOpMaxBigint: case kOpMaxDouble:
      case kOpMin: case kOpMinBigint: case kOpMinDouble:
      case kOpCount:
        agg_index = value & 0x0000FFFF;
        if (agg_index < n_agg_results) agg_ops[agg_index] = op;
        break;
      case kOpLoadCol: {
        Uint32 type = (value & 0x03E00000) >> 21;
        if (type == NDB_TYPE_DECIMAL || type == NDB_TYPE_DECIMALUNSIGNED)
          exec_pos++;
        break;
      }
      case kOpLoadConst:
        exec_pos += 2;
        break;
      case kOpEmbeddedInterp: {
        Uint32 emb_len = value & 0xFFFF;
        exec_pos += emb_len;
        break;
      }
      case kOpSkip:
        break;
      default:
        break;
    }
  }
}

Uint32 JoinAggInterpreter::mergeFrom(JoinAggInterpreter* other,
                                      Uint32 max_groups) {
  assert(other != nullptr);
  assert(m_n_agg_results == other->m_n_agg_results);

  if (!m_agg_ops_cached) {
    extractAggOps(m_prog, m_prog_len, m_agg_prog_start_pos,
                  m_cached_agg_ops, m_n_agg_results);
    m_agg_ops_cached = true;
  }

  if (m_n_gb_cols == 0) {
    if (other->m_agg_results != nullptr) {
      mergeAccumulators(m_agg_results, other->m_agg_results,
                        m_n_agg_results, m_cached_agg_ops);
    }
    m_processed_rows += other->m_processed_rows;
    return 0;
  }

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
        *reinterpret_cast<Uint32*>(other_data - JoinGBHashTable::OVERHEAD +
                                   JoinGBHashTable::KEY_LEN_OFFSET);

      char* my_data = m_gb_map->findInBucket(b, other_data, other_key_len);
      if (my_data != nullptr) {
        const AggResItem *other_items =
          reinterpret_cast<const AggResItem *>(other_data + other_key_len);
        AggResItem *my_items =
          reinterpret_cast<AggResItem *>(my_data + other_key_len);
        mergeAccumulators(my_items, other_items, m_n_agg_results,
                          m_cached_agg_ops);
        other->freeGroupData(other_data);
      } else {
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

Int32 JoinAggInterpreter::initGBTypes(Dbtup* block_tup,
                                       Dbtup::KeyReqStruct* req_struct) {
  for (Uint32 i = 0; i < m_n_gb_cols; i++) {
    Uint32 attr_id = m_gb_cols[i] >> 16;
    GBColTypeInfo &info = m_gb_types[i];

    if ((attr_id & 0x8000) != 0 && m_linked_attr_data != nullptr) {
      Uint32 position = attr_id & 0x7FFF;
      const Uint32* p = m_linked_attr_data;
      const Uint32* p_end = m_linked_attr_data + m_linked_attr_len;
      Uint32 pos_count = 0;
      while (p < p_end && pos_count < position) {
        p += 2;
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

void JoinAggInterpreter::initGBTypesForNullLocal(Dbtup* block_tup) {
  /*
   * Called when the first row is a null-extended row (m_null_local_columns).
   * Linked columns: resolve type from DBTUP tablerec (same as initGBTypes).
   * Local columns: use NDB_TYPE_UNSIGNED as placeholder — all values will
   * be NULL (data size 0), so the actual type doesn't affect comparison.
   * If a matched row arrives later, types are already initialized.
   */
  for (Uint32 i = 0; i < m_n_gb_cols; i++) {
    Uint32 attr_id = m_gb_cols[i] >> 16;
    GBColTypeInfo &info = m_gb_types[i];

    if ((attr_id & 0x8000) != 0 && m_linked_attr_data != nullptr) {
      Uint32 position = attr_id & 0x7FFF;
      const Uint32* p = m_linked_attr_data;
      const Uint32* p_end = m_linked_attr_data + m_linked_attr_len;
      Uint32 pos_count = 0;
      while (p < p_end && pos_count < position) {
        p += 2;
        p += 1 + AttributeHeader::getDataSize(*p);
        pos_count++;
      }
      if (p + 2 < p_end && block_tup != nullptr) {
        Uint32 tableId = p[0];
        if (tableId < block_tup->cnoOfTablerec) {
          Dbtup::Tablerec* tab = &block_tup->tablerec[tableId];
          Uint32 linkedAttrId = AttributeHeader(p[2]).getAttributeId();
          const Uint32* attrDesc = tab->tabDescriptor +
              linkedAttrId * ZAD_SIZE;
          info.typeId = AttributeDescriptor::getType(attrDesc[0]);
          info.maxBytes = AttributeDescriptor::getSizeInBytes(attrDesc[0]);
          info.cs = nullptr;
          if (AttributeOffset::getCharsetFlag(attrDesc[1])) {
            Uint32 csPos = AttributeOffset::getCharsetPos(attrDesc[1]);
            info.cs = tab->charsetArray[csPos];
          }
        } else {
          info.typeId = NDB_TYPE_UNSIGNED;
          info.maxBytes = 4;
          info.cs = nullptr;
        }
      } else {
        info.typeId = NDB_TYPE_UNSIGNED;
        info.maxBytes = 4;
        info.cs = nullptr;
      }
    } else {
      info.typeId = NDB_TYPE_UNSIGNED;
      info.maxBytes = 4;
      info.cs = nullptr;
    }
    const NdbSqlUtil::Type &sqlType = NdbSqlUtil::getType(info.typeId);
    info.cmpFn = sqlType.m_cmp;
  }
  m_gb_types_inited = true;
  m_gb_map->setTypeMeta(m_gb_types, m_n_gb_cols, m_xfrm_buf, m_xfrm_buf_len);
}

Int32 JoinAggInterpreter::processNullExtendedRow(
    const Uint32* linked_attr_data,
    Uint32 linked_attr_len) {
  std::unique_lock<std::mutex> lock(m_mutex, std::defer_lock);
  if (m_use_mutex) lock.lock();

  m_linked_attr_data = linked_attr_data;
  m_linked_attr_len = linked_attr_len;
  m_null_local_columns = true;

  Int32 ret = ProcessRec(nullptr, nullptr);

  m_null_local_columns = false;
  m_linked_attr_data = nullptr;
  m_linked_attr_len = 0;
  return ret;
}

void JoinAggInterpreter::initChunkAllocator(Uint32 thread_id,
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

bool JoinAggInterpreter::bookMoreMemory() {
  Uint32 new_budget = m_memory_budget + m_budget_increment;
  if (new_budget > m_total_available) {
    return false;
  }
  m_memory_budget = new_budget;
  return true;
}

MemChunk* JoinAggInterpreter::allocNewChunk() {
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

char* JoinAggInterpreter::allocGroupData(Uint32 len, Uint32 key_len) {
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

  *reinterpret_cast<char**>(raw) = chunk->group_list;
  *reinterpret_cast<char**>(raw + sizeof(char*)) = nullptr;
  *reinterpret_cast<Uint32*>(raw + 2 * sizeof(char*)) = key_len;
  *reinterpret_cast<Uint32*>(raw + 2 * sizeof(char*) + sizeof(Uint32)) = offset;
  chunk->group_list = raw;

  return raw + GROUP_LINK_OVERHEAD;
}

void JoinAggInterpreter::freeGroupData(char* ptr) {
  char* raw = ptr - GROUP_LINK_OVERHEAD;
  Uint32 offset = *reinterpret_cast<Uint32*>(raw + 2 * sizeof(char*) + sizeof(Uint32));
  MemChunk* chunk = reinterpret_cast<MemChunk*>(raw - offset - sizeof(MemChunk));
  chunk->live_groups--;
  if (chunk->live_groups == 0) {
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

void JoinAggInterpreter::freeAllChunks() {
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
