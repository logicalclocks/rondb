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
#include <map>
#include "AggInterpreterBase.hpp"
#include "PushdownInterpreter.hpp"
#include "Dbtup.hpp"
#include "AggHashTable.hpp"  // for MEM_CHUNK_SIZE, AGG_EVICT_NEEDED

#define ATTR_READ_BUF_WORD_SIZE 2048
// DECIMAL_BUFF_LENGTH now lives in AggInterpreterBase.hpp (Step 1.3)

/**
 * AggInterpreter — aggregation interpreter for normal scan pushdown.
 *
 * Step 2b unification: uses the JoinGBHashTable (1024-bucket) + chunk
 * allocator on AggInterpreterBase, same memory model as
 * JoinAggInterpreter.  Group data lives in MEM_CHUNK_SIZE pages
 * allocated lazily on first insert; the per-batch streaming drain
 * (PrepareAggResIfNeeded) frees each group via freeGroupData after
 * emit, keeping resident footprint near one chunk for low-cardinality
 * queries.  All AggInterpreter-specific buffers (prog / gb_cols /
 * agg_results / hash-table buckets) are inline so the object still
 * fits in MEM_CHUNK_SIZE.
 */
class AggInterpreter : public AggInterpreterBase {
 public:
  AggInterpreter(Uint32 prog_len,
                 Int64 table_id, Int64 frag_id,
                 Uint32 thread_id):
    AggInterpreterBase(PushdownType::AGGREGATION, prog_len,
                       table_id, frag_id, thread_id),
    m_cur_pos(0),
    m_attr_read_pos(0),
    m_processed_rows(0),
    m_result_size(0) {
      memset(m_attr_read_buf, 0, sizeof(m_attr_read_buf));
  }
  ~AggInterpreter() override {
    release_string_results();
    freeAllChunks();
  }

  bool Init(const Uint32* prog);

  Int32 ProcessRec(Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct,
                   Uint32 thread_id);

  void Print();
  Uint32 PrepareAggResIfNeeded(Signal* signal, bool force);
  Uint32 NumOfResRecords(bool last_time = false);
  const JoinGBHashTable* gb_map() const {
    return m_gb_map;
  }
  Uint32 val_len() const {
    return m_n_agg_results * sizeof(AggResItem);
  }
  Uint32 n_gb_cols() const { return m_n_gb_cols; }
  Uint32 n_agg_results() const { return m_n_agg_results; }
  const AggResItem* agg_results() const { return m_agg_results; }
  Uint64 processed_rows() const { return m_processed_rows; }

 private:
  Uint32 m_cur_pos;
  // m_registers / m_register_string_data / m_n_agg_results / m_agg_results
  // / m_n_gb_cols / m_gb_cols / m_gb_map / m_n_groups all on
  // AggInterpreterBase (Steps 1.3 / 2b).

  Uint32 m_attr_read_pos;
  static Uint32 g_attr_read_buf_len_;
  Uint64 m_processed_rows;
  Uint32 m_result_size;
  static Uint32 g_result_header_size_;
  static Uint32 g_result_header_size_per_group_;

  // m_string_results / minMaxString / freeGroupStringSlots /
  // stringPayloadSize / encodeStringPayload / hasStringSlots /
  // m_current_thread_id / m_gb_types / m_xfrm_buf all lifted to
  // AggInterpreterBase (Steps 1.3 / 2a / 2b).  release_string_results
  // stays per-class — it iterates the GBHashTable plus the scalar
  // m_agg_results, sharing freeGroupStringSlots per group.
  void release_string_results();

  // All buffers inline so AggInterpreter fits a single 32KB page.
  Uint32 m_attr_read_buf[ATTR_READ_BUF_WORD_SIZE];       //  8,192 B
  Uint32 m_prog_buf[MAX_AGG_PROGRAM_WORD_SIZE];           //  4,096 B
  Uint32 m_gb_cols_buf[MAX_AGG_N_GROUPBY_COLS];           //    512 B
  AggResItem m_agg_results_buf[MAX_AGG_N_RESULTS];        //  6,144 B
  GBColTypeInfo m_gb_types_buf[MAX_AGG_N_GROUPBY_COLS];   // ~3,072 B
  JoinGBHashTable m_gb_map_buf;                            // ~8,200 B (1024 buckets)
};

/*
 * AggInterpreter packs all buffers inline to avoid extra allocations.
 * Step 2b replaces the std::map + m_mem_buf bump pool with the shared
 * JoinGBHashTable (~8 KB buckets) + chunk allocator (chunks live
 * out-of-line via lc_ndbd_pool_malloc).  m_gb_types_buf is a small
 * inline GROUP BY type-metadata array, replacing the old
 * GBCmpContext::col_meta carry.
 *
 * If any of these inline buffers is enlarged or a new field is added,
 * the static_assert below fires — review whether any buffer can be
 * shrunk or moved to an external allocation.
 */
static_assert(sizeof(AggInterpreter) <= MEM_CHUNK_SIZE,
              "AggInterpreter has exceeded the MEM_CHUNK_SIZE (32KB) page "
              "limit.  All inline buffers (prog_buf, attr_read_buf, hash buckets, "
              "etc.) must fit together on a single page.  Shrink a buffer or "
              "move it to an external allocation.");

#endif  // AGGINTERPRETER_H_
