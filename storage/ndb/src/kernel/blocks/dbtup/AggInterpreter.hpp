/*
 * Copyright (c) 2024, 2026, Hopsworks and/or its affiliates.
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

#ifndef AGGINTERPRETER_H_
#define AGGINTERPRETER_H_

#include <math.h>
#include <cstring>
#include "AggInterpreterBase.hpp"
#include "PushdownInterpreter.hpp"
#include "Dbtup.hpp"
#include "AggHashTable.hpp"  // for MEM_CHUNK_SIZE, AGG_EVICT_NEEDED

// ATTR_READ_BUF_WORD_SIZE and DECIMAL_BUFF_LENGTH live in
// AggInterpreterBase.hpp (Steps 1.3 / 3a-A).

/**
 * AggInterpreter — aggregation interpreter for normal scan pushdown.
 *
 * Step 3a-B: shares JoinAggInterpreter's memory model entirely —
 * `m_buf_block` carves the per-instance buffers from RG_QUERY_MEMORY
 * (right-sized to the program's actual prog_len / n_gb_cols /
 * n_agg_results), and group records live in MEM_CHUNK_SIZE pages
 * lazily allocated on first insert.  The per-batch streaming drain
 * (PrepareAggResIfNeeded) frees each group via freeGroupData after
 * emit, keeping resident footprint near one chunk for
 * low-cardinality queries.
 *
 * AggInterpreter remains a thin subclass: it owns the streaming
 * drain (no merge / eviction / mutex / linked-attr / multi-leaf
 * machinery), and it skips the m_gb_map_buf carve when n_gb_cols == 0.
 */
class AggInterpreter : public AggInterpreterBase {
 public:
  AggInterpreter(Uint32 prog_len,
                 Int64 table_id, Int64 frag_id,
                 Uint32 thread_id):
    AggInterpreterBase(PushdownType::AGGREGATION, prog_len,
                       table_id, frag_id, thread_id) {
    /* All scratch fields and buffer pointers initialised by the base
     * ctor (Steps 3a-A / 3a-B). */
  }
  /* ~AggInterpreter() default — AggInterpreterBase's destructor does
   * release_string_results, freeAllChunks, frees m_xfrm_buf and
   * m_buf_block. */

  bool Init(const Uint32* prog);

  Int32 ProcessRec(Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct,
                   Uint32 thread_id);

  Uint32 PrepareAggResIfNeeded(Signal* signal, bool force);
  Uint32 NumOfResRecords(bool last_time = false);
  /* gb_map / val_len / n_gb_cols / n_agg_results / agg_results /
   * processed_rows accessors lifted to AggInterpreterBase in Step 3b. */
};

#endif  // AGGINTERPRETER_H_
