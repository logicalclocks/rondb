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

#ifndef PUSHDOWNINTERPRETER_H_
#define PUSHDOWNINTERPRETER_H_

#include "ndb_types.h"

class AggInterpreter;
class JoinAggInterpreter;
class VecSearchInterpreter;

enum class PushdownType : Uint32 {
  AGGREGATION = 0,
  VECTOR_SEARCH = 1
};

class PushdownInterpreter {
 public:
  PushdownInterpreter(PushdownType type, Uint32 prog_len,
                      Int64 table_id, Int64 frag_id, Uint32 thread_id)
    : m_type(type), m_prog_len(prog_len),
      m_table_id(table_id), m_frag_id(frag_id),
      m_thread_id(thread_id), m_inited(false) {}

  virtual ~PushdownInterpreter() = default;

  PushdownType type() const { return m_type; }
  bool is_vec_search() const { return m_type == PushdownType::VECTOR_SEARCH; }
  bool is_aggregation() const { return m_type == PushdownType::AGGREGATION; }
  Int64 table_id() const { return m_table_id; }
  Int64 frag_id() const { return m_frag_id; }
  Uint32 thread_id() const { return m_thread_id; }
  bool inited() const { return m_inited; }

  static void Destruct(PushdownInterpreter* ptr);

  /**
   * OptimizeProgramBuffer — static type analysis on an aggregation program.
   *
   * Replaces generic opcodes (kOpSum, kOpPlus, etc.) with type-specific
   * variants (kOpSumBigint, kOpSumDouble, etc.) based on a single-pass
   * analysis of register types.  Modifies the program buffer in-place.
   *
   * Can be called on any writable program buffer — does not require an
   * interpreter instance.  Used by DblqhProxy to optimize leaf programs
   * in JoinAggregationState before creating interpreters.
   */
  static void OptimizeProgramBuffer(Uint32* prog, Uint32 prog_len,
                                    Uint32 start_pos);

 protected:
  PushdownType m_type;
  Uint32 m_prog_len;
  Int64 m_table_id;
  Int64 m_frag_id;
  Uint32 m_thread_id;
  bool m_inited;
};

struct PushdownCreateResult {
  AggInterpreter* agg;
  VecSearchInterpreter* vs;
};

class PushdownInterpreterFactory {
 public:
  static PushdownType DetectType(const Uint32* prog, Uint32 prog_len);
  static PushdownCreateResult Create(const Uint32* prog, Uint32 prog_len,
                                     Int64 table_id, Int64 frag_id,
                                     Uint32 thread_id);
};

#endif  // PUSHDOWNINTERPRETER_H_
