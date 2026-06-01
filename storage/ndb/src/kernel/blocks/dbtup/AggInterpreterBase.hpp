/*
 * Copyright (c) 2026, Hopsworks and/or its affiliates.
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

#ifndef AGGINTERPRETERBASE_H_
#define AGGINTERPRETERBASE_H_

#include "PushdownInterpreter.hpp"
#include "NdbAggregationCommon.hpp"

/**
 * AggInterpreterBase — shared base for AggInterpreter (normal-scan
 * aggregation) and JoinAggInterpreter (join / CTE aggregation).
 *
 * Step 1 of the interpreter unification (see
 * claude_files/pushdown_join_aggregation/agg_interpreter_unification_plan.md):
 * holds the numeric / type aggregation kernels that were previously
 * duplicated verbatim as file-static functions in both AggInterpreter.cpp
 * and JoinAggInterpreter.cpp.  They were verified to be logically identical
 * (differing only in cosmetics and the debug-only DEBUG_PA_INTERP trace
 * blocks), so a single canonical copy lives here.
 *
 * These kernels are pure functions over Register / AggResItem and carry no
 * interpreter state, so they are protected static methods; both subclasses
 * call them via ordinary inherited name lookup from their ProcessRec opcode
 * loops (every call site is inside a member function, so no qualification is
 * needed).
 *
 * Later steps of the plan will lift the shared fields, the string MIN/MAX
 * sidecar, and the opcode executor into this base as well.
 */
class AggInterpreterBase : public PushdownInterpreter {
 public:
  AggInterpreterBase(PushdownType type, Uint32 prog_len,
                     Int64 table_id, Int64 frag_id, Uint32 thread_id)
    : PushdownInterpreter(type, prog_len, table_id, frag_id, thread_id),
      m_prog(nullptr), m_agg_prog_start_pos(0) {}

  /**
   * OptimizeProgram — guard + delegate to OptimizeProgramBuffer.
   *
   * Step 1.2 of the interpreter unification: this used to live as a
   * byte-identical 7-line method in each subclass.  Both subclasses now
   * inherit the single definition here (non-virtual; callers reach it
   * through an AggInterpreter or JoinAggInterpreter pointer via ordinary
   * inheritance).
   */
  bool OptimizeProgram();

 protected:
  /**
   * validateEmbeddedProgram — sanity-check an embedded program at
   * decode time.
   *
   * Step 1.2 of the interpreter unification: previously duplicated in
   * each subclass with subtly different rigor (AggInterpreter only
   * bounds-checked branch targets; JoinAggInterpreter additionally
   * enforced an opcode allow-list and rejected backward branches).  The
   * stricter JoinAgg form is adopted here for both — the allow-list
   * covers every opcode either path emits, and the backward-branch
   * reject closes a potential infinite-loop class.  Pure function over
   * arguments; no instance state needed.
   */
  static bool validateEmbeddedProgram(const Uint32* emb_prog, Uint32 emb_len);

  /* Shared aggregation kernels — definitions in AggInterpreterBase.cpp.
   * `print` is consumed only inside DEBUG_PA_INTERP debug-trace blocks. */
  static bool TypeSupported(DataType type);
  static bool IsUnsigned(DataType type);
  static DataType AlignedType(DataType type, int scale);
  [[maybe_unused]] static void PrintValue(const AggResItem* res, char* log_buf);
  static Int32 Sum(const Register& a, AggResItem* res, bool print);
  static Int32 SumBigint(const Register& a, AggResItem* res, bool print);
  static Int32 SumDouble(const Register& a, AggResItem* res, bool print);
  static Int32 Max(const Register& a, AggResItem* res, bool print);
  static Int32 MaxBigint(const Register& a, AggResItem* res, bool print);
  static Int32 MaxDouble(const Register& a, AggResItem* res, bool print);
  static Int32 Min(const Register& a, AggResItem* res, bool print);
  static Int32 MinBigint(const Register& a, AggResItem* res, bool print);
  static Int32 MinDouble(const Register& a, AggResItem* res, bool print);
  static Int32 Count(const Register& a, AggResItem* res, bool print);

  /* Fields lifted from the subclasses in Step 1.2 to support the shared
   * OptimizeProgram.  Total sizeof is unchanged — same fields, moved up
   * the class hierarchy — so both static_asserts on subclass sizeof
   * still hold.  Plan's 1.3/1.4 will lift more shared fields. */
  Uint32* m_prog;
  Uint32 m_agg_prog_start_pos;
};

#endif  // AGGINTERPRETERBASE_H_
