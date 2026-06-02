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

/*
 * AggInterpreterBase.cpp — shared numeric / type aggregation kernels.
 *
 * Step 1 of the interpreter unification
 * (claude_files/pushdown_join_aggregation/agg_interpreter_unification_plan.md):
 * these were previously duplicated verbatim as file-static functions in both
 * AggInterpreter.cpp and JoinAggInterpreter.cpp.  They are pure functions over
 * Register / AggResItem and were verified logically identical between the two
 * copies (differing only in cosmetics and the debug-only DEBUG_PA_INTERP trace
 * blocks, which are retained here).
 *
 * Include set and ordering mirror AggInterpreter.cpp (whence these bodies
 * came) so the kernels see exactly the symbols they did before.
 */
#include <cstdint>
#include <cstring>
#include <utility>
#include <cmath>     // std::isfinite
#include <climits>   // LLONG_MAX
#include <cstdio>    // sprintf
#include <cassert>   // assert

#define DBTUP_C
#include "signaldata/TransIdAI.hpp"
#include "include/my_byteorder.h"
#include "AggInterpreterBase.hpp"
#include "Dbtup.hpp"
#include "InterpreterCommonOp.hpp"
#include "util/require.h"
#include "decimal.h"
#include <NdbSqlUtil.hpp>
#include <Interpreter.hpp>
#include <CteLinkedAttr.hpp>
#include "my_sys.h"
#include "../dblqh/Dblqh.hpp"

/*
 * DEBUG_PA_INTERP / DEBUG_AGG machinery (off by default).  The kernels'
 * debug-only trace blocks reference DEBUG_PA_INTERP / PrintValue, and
 * the executeStandardOpcode body references DEB_AGG for overflow
 * traces — both must compile out cleanly in production builds and be
 * re-enableable for tracing.  Keep these in sync with the matching
 * blocks in AggInterpreter.cpp and JoinAggInterpreter.cpp.
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

/**
 * validateEmbeddedProgram — strict embedded-program sanity check.
 *
 * Step 1.2 of the interpreter unification.  The JoinAggInterpreter copy
 * was strictly more rigorous than the AggInterpreter copy: in addition
 * to bounds-checking branch targets, it enforced an opcode allow-list
 * and rejected backward branches.  The stricter form is adopted here
 * for both code paths.  Pure function over arguments.
 */
bool AggInterpreterBase::validateEmbeddedProgram(
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
      case Interpreter::LOAD_DOUBLE_CONST:
      case Interpreter::ADD_REG_REG:
      case Interpreter::SUB_REG_REG:
      case Interpreter::MUL_REG_REG:
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
      case Interpreter::BRANCH_MEM_OP_ARG:
      case Interpreter::BRANCH_MEM_OP_ARG_INLINE_TYPE:
      case Interpreter::BRANCH_ATTR_EQ_NULL:
      case Interpreter::BRANCH_ATTR_NE_NULL:
      case Interpreter::READ_LINKED_TO_MEM:
      case Interpreter::READ_UINT8_MEM_TO_REG:
      case Interpreter::READ_UINT16_MEM_TO_REG:
      case Interpreter::READ_UINT32_MEM_TO_REG:
      case Interpreter::READ_INT64_MEM_TO_REG:
      case Interpreter::READ_AGG_REG_TO_REG:
      case Interpreter::READ_LINKED_COLUMN_TO_REG:
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
      case Interpreter::BRANCH_MEM_OP_ARG:
      case Interpreter::BRANCH_MEM_OP_ARG_INLINE_TYPE:
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

/**
 * OptimizeProgram — guard + delegate to the shared OptimizeProgramBuffer.
 *
 * Step 1.2 of the interpreter unification.  Previously a byte-identical
 * 7-line method on each subclass; now a single definition that both
 * subclasses inherit.  Uses m_inited / m_prog_len from PushdownInterpreter
 * and m_prog / m_agg_prog_start_pos from this class (fields lifted from
 * the subclasses in 1.2).
 */
bool AggInterpreterBase::OptimizeProgram() {
  if (!m_inited) {
    return false;
  }
  OptimizeProgramBuffer(m_prog, m_prog_len, m_agg_prog_start_pos);
  return true;
}

bool AggInterpreterBase::TypeSupported(DataType type) {
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

    // Phase I.6 (F.2): MIN/MAX over CHAR / VARCHAR / Longvarchar.
    // Sum is rejected separately (see Sum()).  Count is
    // type-agnostic and works for any column type.  String
    // value handling lives in MinString / MaxString and the
    // m_string_results sidecar — see cte_filter_phase_i6_varchar.md.
    case NDB_TYPE_CHAR:
    case NDB_TYPE_VARCHAR:
    case NDB_TYPE_LONGVARCHAR:
      return true;
    default:
      return false;
  }
  return false;
}

bool AggInterpreterBase::IsUnsigned(DataType type) {
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

DataType AggInterpreterBase::AlignedType(DataType type, int scale) {
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

    // Phase I.6 (F.2): string MIN/MAX preserves the source type —
    // wire format stays as the source's [length_prefix][payload].
    case NDB_TYPE_CHAR:
    case NDB_TYPE_VARCHAR:
    case NDB_TYPE_LONGVARCHAR:
      return type;
    default:
      assert(0);
  }
  return NDB_TYPE_UNDEFINED;
}

void AggInterpreterBase::PrintValue(const AggResItem* res, char* log_buf) {
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

Int32 AggInterpreterBase::Sum(const Register& a, AggResItem* res, bool print) {
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
Int32 AggInterpreterBase::SumBigint(const Register& a, AggResItem* res, bool print) {
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
Int32 AggInterpreterBase::SumDouble(const Register& a, AggResItem* res, bool print) {
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

Int32 AggInterpreterBase::Max(const Register& a, AggResItem* res, bool print) {
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
Int32 AggInterpreterBase::MaxBigint(const Register& a, AggResItem* res, bool print) {
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
Int32 AggInterpreterBase::MaxDouble(const Register& a, AggResItem* res, bool print) {
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

Int32 AggInterpreterBase::Min(const Register& a, AggResItem* res, bool print) {
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
Int32 AggInterpreterBase::MinBigint(const Register& a, AggResItem* res, bool print) {
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
Int32 AggInterpreterBase::MinDouble(const Register& a, AggResItem* res, bool print) {
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

Int32 AggInterpreterBase::Count(const Register& a, AggResItem* res, bool print) {
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
 * Phase I.6 (F.2-K.5) string MIN/MAX helpers — bodies that used to live
 * verbatim in both AggInterpreter.cpp and JoinAggInterpreter.cpp.  Now
 * a single canonical copy on AggInterpreterBase; subclasses inherit
 * them.  See header for the per-method contract.  Step 1.3 of the
 * interpreter unification.
 */

// Phase I.6 (F.2-K.4e): release one group's string val_ptr buffers.
// Called from the per-batch drain (AggInterpreter) or from
// evictOneGroup (JoinAggInterpreter) after the wire-format emit has
// consumed the payload.  Cheap no-op when m_string_results is
// unallocated (program has no string MIN/MAX).
void AggInterpreterBase::freeGroupStringSlots(AggResItem* slots) {
  if (m_string_results == nullptr) {
    return;
  }
  for (Uint32 i = 0; i < m_n_agg_results; i++) {
    DataType t = slots[i].type;
    if ((t == NDB_TYPE_CHAR || t == NDB_TYPE_VARCHAR ||
         t == NDB_TYPE_LONGVARCHAR) &&
        slots[i].value.val_ptr != nullptr) {
      lc_ndbd_pool_free(slots[i].value.val_ptr);
      slots[i].value.val_ptr = nullptr;
    }
  }
}

// Phase I.6 (F.2-K.5): bytes that one group's appended string-payload
// region will consume on the wire.  Walks the slot array, summing the
// size contribution of each string slot:
// `[Uint32 byte_size][prefix+payload, Uint32-padded]`.  Non-string
// slots and null string slots contribute zero bytes.
Uint32 AggInterpreterBase::stringPayloadSize(const AggResItem* slots) const {
  if (m_string_results == nullptr) {
    return 0;
  }
  Uint32 total = 0;
  for (Uint32 i = 0; i < m_n_agg_results; i++) {
    DataType t = slots[i].type;
    if ((t == NDB_TYPE_CHAR || t == NDB_TYPE_VARCHAR ||
         t == NDB_TYPE_LONGVARCHAR) &&
        !slots[i].is_null && slots[i].value.val_ptr != nullptr) {
      const char* buf = static_cast<const char*>(slots[i].value.val_ptr);
      const Uint16 payload_len = *reinterpret_cast<const Uint16*>(buf);
      const Uint32 prefix = m_string_results[i].prefix_bytes;
      const Uint32 byte_size = prefix + payload_len;
      total += sizeof(Uint32);
      total += (byte_size + 3) & ~3U;
    }
  }
  return total;
}

// Phase I.6 (F.2-K.5): write one group's appended string-payload
// region into `dst`.  Caller must size `dst` from stringPayloadSize.
Uint32 AggInterpreterBase::encodeStringPayload(const AggResItem* slots,
                                                char* dst) const {
  if (m_string_results == nullptr) {
    return 0;
  }
  char* p = dst;
  for (Uint32 i = 0; i < m_n_agg_results; i++) {
    DataType t = slots[i].type;
    if ((t == NDB_TYPE_CHAR || t == NDB_TYPE_VARCHAR ||
         t == NDB_TYPE_LONGVARCHAR) &&
        !slots[i].is_null && slots[i].value.val_ptr != nullptr) {
      const char* buf = static_cast<const char*>(slots[i].value.val_ptr);
      const Uint16 payload_len = *reinterpret_cast<const Uint16*>(buf);
      const Uint32 prefix = m_string_results[i].prefix_bytes;
      const Uint32 byte_size = prefix + payload_len;
      *reinterpret_cast<Uint32*>(p) = byte_size;
      p += sizeof(Uint32);
      memcpy(p, buf + 4, byte_size);
      p += byte_size;
      const Uint32 pad = ((byte_size + 3) & ~3U) - byte_size;
      if (pad > 0) {
        memset(p, 0, pad);
        p += pad;
      }
    }
  }
  return static_cast<Uint32>(p - dst);
}

// Phase I.6 (F.2-K.4c): per-(group, slot) MIN/MAX string update.
// See header for layout and contract.
Int32 AggInterpreterBase::minMaxString(Uint32 reg_index, Uint32 agg_index,
                                       AggResItem* agg_res_ptr,
                                       bool is_max) {
  const Register& src_reg = m_registers[reg_index];
  if (src_reg.is_null) {
    return 0;
  }
  // Lazy-allocate the slot metadata array on the first string
  // MIN/MAX touch — non-string queries pay zero memory cost.
  if (m_string_results == nullptr) {
    Uint32 nbytes = m_n_agg_results * sizeof(StringResult);
    m_string_results = static_cast<StringResult*>(
        lc_ndbd_pool_malloc(nbytes, RG_QUERY_MEMORY,
                            m_current_thread_id, true));
    if (m_string_results == nullptr) {
      return ZAGG_ALLOC_MEM_FAILED;
    }
  }
  const StringResult& src = m_register_string_data[reg_index];
  StringResult& slot = m_string_results[agg_index];
  if (slot.declared_size == 0) {
    slot.charset = src.charset;
    slot.prefix_bytes = src.prefix_bytes;
    slot.declared_size = src.declared_size;
  }
  AggResItem& dst = agg_res_ptr[agg_index];
  const Uint32 needed_payload = src.prefix_bytes + src.length;
  Uint32 alloc_size = (4 + needed_payload + 15) & ~15U;
  if (alloc_size < 16) alloc_size = 16;

  if (dst.value.val_ptr == nullptr) {
    char* buf = static_cast<char*>(
        lc_ndbd_pool_malloc(alloc_size, RG_QUERY_MEMORY,
                            m_current_thread_id, false));
    if (buf == nullptr) {
      return ZAGG_ALLOC_MEM_FAILED;
    }
    Uint16* hdr = reinterpret_cast<Uint16*>(buf);
    hdr[0] = src.length;
    hdr[1] = static_cast<Uint16>(alloc_size - 4);
    if (needed_payload > 0) {
      memcpy(buf + 4, src.ptr, needed_payload);
    }
    dst.type = src_reg.type;
    dst.value.val_ptr = buf;
    dst.is_unsigned = false;
    dst.is_null = false;
    return 0;
  }

  char* old_buf = static_cast<char*>(dst.value.val_ptr);
  const Uint16* old_hdr = reinterpret_cast<const Uint16*>(old_buf);
  const Uint16 old_payload_len = old_hdr[0];
  const Uint16 old_capacity = old_hdr[1];
  const unsigned n_new = src.prefix_bytes + src.length;
  const unsigned n_old = slot.prefix_bytes + old_payload_len;
  const void* v_new = src.ptr;
  const void* v_old = old_buf + 4;
  const Uint32 type_id =
      (slot.prefix_bytes == 0) ? NDB_TYPE_CHAR :
      (slot.prefix_bytes == 1) ? NDB_TYPE_VARCHAR : NDB_TYPE_LONGVARCHAR;
  const NdbSqlUtil::Type& sqlType = NdbSqlUtil::getType(type_id);
  int cmp = (*sqlType.m_cmp)(slot.charset, v_new, n_new, v_old, n_old);
  const bool replace = is_max ? (cmp > 0) : (cmp < 0);
  if (!replace) return 0;

  if (needed_payload <= old_capacity) {
    Uint16* h = reinterpret_cast<Uint16*>(old_buf);
    h[0] = src.length;
    if (needed_payload > 0) {
      memcpy(old_buf + 4, src.ptr, needed_payload);
    }
  } else {
    // Allocate-then-free order keeps the existing winner intact on OOM.
    char* new_buf = static_cast<char*>(
        lc_ndbd_pool_malloc(alloc_size, RG_QUERY_MEMORY,
                            m_current_thread_id, false));
    if (new_buf == nullptr) {
      return ZAGG_ALLOC_MEM_FAILED;
    }
    Uint16* h = reinterpret_cast<Uint16*>(new_buf);
    h[0] = src.length;
    h[1] = static_cast<Uint16>(alloc_size - 4);
    if (needed_payload > 0) {
      memcpy(new_buf + 4, src.ptr, needed_payload);
    }
    dst.value.val_ptr = new_buf;
    lc_ndbd_pool_free(old_buf);
  }
  return 0;
}

/*
 * Step 1.4 — shared opcode handler.
 *
 * Covers the 28 opcode arms that were byte-identical between the two
 * subclasses' ProcessRec dispatches.  See AggInterpreterBase.hpp for
 * the parameter contract and the list of opcodes handled.  The two
 * divergent opcodes (kOpLoadCol, kOpEmbeddedInterp) are still handled
 * in each subclass's own switch.
 */
Int32 AggInterpreterBase::executeStandardOpcode(
    Uint8 op, Uint32 value, Uint32& exec_pos,
    AggResItem* agg_res_ptr, bool debug_print,
    bool* handled) {
  *handled = true;
  Uint32 reg_index;
  Uint32 reg_index2;
  Uint32 agg_index;
  Uint32 type;
  int ret;

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
      return 0;

    case kOpMinus:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegMinusReg(m_registers[reg_index], m_registers[reg_index2],
                        &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[MINUS], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpMul:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegMulReg(m_registers[reg_index], m_registers[reg_index2],
                      &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[MUL], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpDiv:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegDivReg(m_registers[reg_index], m_registers[reg_index2],
                      &m_registers[reg_index], false);
      if (ret < 0) {
        DEB_AGG(("Overflow[DIV], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpDivInt:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegDivReg(m_registers[reg_index], m_registers[reg_index2],
                      &m_registers[reg_index], true);
      if (ret < 0) {
        DEB_AGG(("Overflow[DIVINT], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpMod:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegModReg(m_registers[reg_index], m_registers[reg_index2],
                      &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[MOD], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

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
      return 0;

    case kOpPlusDouble:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegPlusDouble(m_registers[reg_index], m_registers[reg_index2],
                          &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[PlusDouble], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

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
      return 0;

    case kOpMinusDouble:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegMinusDouble(m_registers[reg_index], m_registers[reg_index2],
                           &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[MinusDouble], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

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
      return 0;

    case kOpMulDouble:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegMulDouble(m_registers[reg_index], m_registers[reg_index2],
                         &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[MulDouble], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

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
      return 0;

    case kOpDivIntBigint:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegDivIntBigint(m_registers[reg_index], m_registers[reg_index2],
                            &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[DivIntBigint], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

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
        g_eventLogger->debug("AggInterpreterBase::executeStandardOpcode "
            "ZAGG_OTHER_ERROR: kOpLoadConst overflow exec_pos=%u "
            "prog_len=%u", exec_pos, m_prog_len);
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
                          "LoadConst[%u] NDB_TYPE_BIGUNSIGNED %llu",
                          reg_index, m_registers[reg_index].value.val_uint64);
          break;
        case NDB_TYPE_DOUBLE:
          m_registers[reg_index].value.val_double =
              doubleget(reinterpret_cast<unsigned char*>(&m_prog[exec_pos]));
          PA_INTERP_TRACE(m_frag_id,
                          "LoadConst[%u] NDB_TYPE_DOUBLE %lf",
                          reg_index, m_registers[reg_index].value.val_double);
          break;
        default:
          return ZAGG_LOAD_CONST_WRONG_TYPE;
      }
      exec_pos += 2;
      return 0;

    case kOpMov:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      m_registers[reg_index] = m_registers[reg_index2];
      PA_INTERP_TRACE(m_frag_id,
                      "Move [%u]->[%u]",
                      reg_index2, reg_index);
      return 0;

    case kOpSetRegNull:
      reg_index = (value & 0x000F0000) >> 16;
      if (m_registers[reg_index].type == NDB_TYPE_UNDEFINED) {
        m_registers[reg_index].type = NDB_TYPE_BIGINT;
        m_registers[reg_index].is_unsigned = false;
        m_registers[reg_index].value.val_int64 = 0;
      }
      m_registers[reg_index].is_null = true;
      PA_INTERP_TRACE(m_frag_id, "SetRegNull[%u]", reg_index);
      return 0;

    case kOpSum:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      ret = Sum(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      if (ret < 0) {
        DEB_AGG(("Overflow[SUM], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpMax:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      if (m_registers[reg_index].type == NDB_TYPE_CHAR ||
          m_registers[reg_index].type == NDB_TYPE_VARCHAR ||
          m_registers[reg_index].type == NDB_TYPE_LONGVARCHAR) {
        /* minMaxString returns 0 on success or ZAGG_ALLOC_MEM_FAILED;
         * propagate as-is. */
        return minMaxString(reg_index, agg_index, agg_res_ptr, /*is_max=*/true);
      }
      /* Max returns 1 on "first row" / null short-circuit, 0 on a
       * normal update, never an error code.  Original dispatch
       * discarded the return value via `ret = ...; break;` — keep that
       * behavior here so a positive return doesn't surface as an
       * agg-interp failure. */
      Max(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      return 0;

    case kOpMin:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      if (m_registers[reg_index].type == NDB_TYPE_CHAR ||
          m_registers[reg_index].type == NDB_TYPE_VARCHAR ||
          m_registers[reg_index].type == NDB_TYPE_LONGVARCHAR) {
        return minMaxString(reg_index, agg_index, agg_res_ptr, /*is_max=*/false);
      }
      /* See kOpMax: Min's positive return is the null/first-row
       * short-circuit, not an error. */
      Min(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      return 0;

    case kOpCount:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      Count(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      return 0;

    // Type-specific Sum operations
    case kOpSumBigint:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      ret = SumBigint(m_registers[reg_index], &agg_res_ptr[agg_index],
                      debug_print);
      if (ret < 0) {
        DEB_AGG(("Overflow[SumBigint], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpSumDouble:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      ret = SumDouble(m_registers[reg_index], &agg_res_ptr[agg_index],
                      debug_print);
      if (ret < 0) {
        DEB_AGG(("Overflow[SumDouble], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    // Type-specific Max operations
    case kOpMaxBigint:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      MaxBigint(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      return 0;

    case kOpMaxDouble:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      MaxDouble(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      return 0;

    // Type-specific Min operations
    case kOpMinBigint:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      MinBigint(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      return 0;

    case kOpMinDouble:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      MinDouble(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      return 0;

    case kOpSkip: {
      Uint32 skip_count = value & 0xFFFF;
      exec_pos += skip_count;
      return 0;
    }

    default:
      *handled = false;
      return 0;
  }
}

/*
 * Step 2a — chunk allocator (lifted from JoinAggInterpreter).
 *
 * Per-group records live in MEM_CHUNK_SIZE (32 KB) pages allocated from
 * RG_QUERY_MEMORY.  Each page has a `MemChunk` header at offset 0 with
 * a singly-linked list of live groups carved from `data`; the doubly-
 * linked chunk list `m_chunks` / `m_chunks_tail` lets `freeGroupData`
 * unlink the page in O(1) once `live_groups` hits zero.
 *
 * No behavior change: both subclasses end up invoking this code via
 * inherited name lookup.  In Step 2a only JoinAggInterpreter actually
 * uses these methods; AggInterpreter still runs on its `std::map` +
 * inline `m_mem_buf` allocator until Step 2b switches it over.
 */
void AggInterpreterBase::initChunkAllocator(Uint32 thread_id,
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

bool AggInterpreterBase::bookMoreMemory() {
  Uint32 new_budget = m_memory_budget + m_budget_increment;
  if (new_budget > m_total_available) {
    return false;
  }
  m_memory_budget = new_budget;
  return true;
}

MemChunk* AggInterpreterBase::allocNewChunk() {
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

char* AggInterpreterBase::allocGroupData(Uint32 len, Uint32 key_len) {
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

void AggInterpreterBase::freeGroupData(char* ptr) {
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

void AggInterpreterBase::freeAllChunks() {
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

/*
 * Step 2b — shared GROUP BY type-metadata initializer.
 *
 * Lifted from JoinAggInterpreter.  Resolves each GB column's type
 * info (typeId / maxBytes / charset / cmpFn) into m_gb_types[],
 * allocates m_xfrm_buf if any column has a charset (sized for the
 * widest strnxfrm_hash output), and publishes the metadata to
 * m_gb_map via setTypeMeta.
 *
 * linked_attr_data / linked_attr_len are the per-row linked-attr
 * buffer JoinAgg passes for join queries (kOpLoadCol-equivalent
 * linked-GB columns).  AggInterpreter (normal scan) passes
 * nullptr / 0; the linked branches below are dead code on that
 * path because the attr_id 0x8000 bit never appears in a
 * normal-scan GB column.
 */
Int32 AggInterpreterBase::initGBTypes(
    Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct,
    const Uint32* linked_attr_data, Uint32 linked_attr_len) {
  for (Uint32 i = 0; i < m_n_gb_cols; i++) {
    Uint32 attr_id = m_gb_cols[i] >> 16;
    GBColTypeInfo &info = m_gb_types[i];

    if ((attr_id & 0x8000) != 0) {
      if (unlikely(linked_attr_data == nullptr)) {
        g_eventLogger->debug(
            "initGBTypes: linked GB col %u (attr_id=0x%x) but "
            "linked_attr_data is NULL — API likely missing "
            "addLinkedProjection for the position", i, attr_id);
        return ZAGG_OTHER_ERROR;
      }
      Uint32 position = attr_id & 0x7FFF;
      const Uint32* p = linked_attr_data;
      const Uint32* p_end = linked_attr_data + linked_attr_len;
      Uint32 pos_count = 0;
      while (p < p_end && pos_count < position) {
        p += 2;
        p += 1 + AttributeHeader::getDataSize(*p);
        pos_count++;
      }
      if (unlikely(p + 2 >= p_end)) {
        g_eventLogger->debug("initGBTypes: linked buffer too short for "
            "position %u (linked_len=%u)", position, linked_attr_len);
        return ZAGG_OTHER_ERROR;
      }
      Uint32 word0 = p[0];
      Uint32 word1 = p[1];

      if (CteLinkedAttr::isCteMarker(word0)) {
        info.typeId = CteLinkedAttr::decodeTypeId(word0);
        info.maxBytes = CteLinkedAttr::decodeMaxBytes(word0);
        info.cs = nullptr;
        Uint32 csNumber = CteLinkedAttr::decodeCsNumber(word1);
        if (csNumber != 0) {
          info.cs = all_charsets[csNumber];
        }
      } else {
        Uint32 tableId = word0;
        Uint32 tableVersion = word1;
        require(tableId != 0);

        Dblqh* lqh = block_tup->c_lqh;
        if (unlikely(tableId >= lqh->ctabrecFileSize)) {
          g_eventLogger->debug("initGBTypes: tableId %u out of range "
              "(max=%u)", tableId, lqh->ctabrecFileSize);
          return ZINVALID_SCHEMA_VERSION;
        }
        if (unlikely(table_version_major(tableVersion) !=
                     table_version_major(
                         lqh->tablerec[tableId].schemaVersion))) {
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
