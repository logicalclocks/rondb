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

struct CHARSET_INFO;

/**
 * StringResult — sidecar storage for per-slot string MIN/MAX results.
 *
 * Used by AggInterpreter and JoinAggInterpreter to hold the running
 * winner of a MIN/MAX over CHAR / VARCHAR / Longvarchar.  The buffer
 * at *ptr stores [length_prefix][payload] in the wire format of the
 * source column type, sized to a multiple of 16 bytes; `length` is
 * the payload length (excluding any prefix); `size` is the allocated
 * capacity.  Backed by lc_ndbd_pool_malloc / free.
 *
 * `prefix_bytes` is 1 for VARCHAR, 2 for Longvarchar, 0 for CHAR.
 * `declared_size` is CHAR's fixed width (used for space-padding on
 * emit) or VARCHAR/Longvarchar's max byte length.  `charset` is the
 * column's collation, populated on the first row that touches the
 * slot via the existing AttributeOffset::getCharsetFlag /
 * tablePtrP->charsetArray[] path.  All four metadata fields are
 * stable across rows once captured — avoids re-walking the
 * AttributeDescriptor for every row.
 *
 * See cte_filter_phase_i6_varchar.md (Phase I.6 finish, F.2).
 */
struct StringResult {
  char* ptr;
  Uint16 length;
  Uint16 size;
  Uint16 prefix_bytes;
  Uint16 declared_size;
  const CHARSET_INFO* charset;
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
