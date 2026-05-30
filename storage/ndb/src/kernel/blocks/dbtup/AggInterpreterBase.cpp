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

/*
 * DEBUG_PA_INTERP machinery (off by default).  The kernels' debug-only
 * trace blocks reference DEBUG_PA_INTERP / PrintValue; keep the guard so
 * they compile out in normal builds and can be re-enabled for tracing.
 */
#if (defined(VM_TRACE) || defined(ERROR_INSERT))
#undef DEBUG_PA_INTERP
// #define DEBUG_PA_INTERP 1
#endif

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
