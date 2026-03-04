/*
 * Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.
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
#include "PushdownInterpreter.hpp"
#include "Dbtup.hpp"
#include "AggHashTable.hpp"  // for MEM_CHUNK_SIZE, AGG_EVICT_NEEDED

#define ATTR_READ_BUF_WORD_SIZE 2048
#define DECIMAL_BUFF_LENGTH 9

/**
 * AggInterpreter — lean aggregation interpreter for normal scan pushdown.
 *
 * All buffers are inline so that sizeof(AggInterpreter) fits in a single
 * 32KB page.  Uses std::map with an inline bump allocator for group data —
 * no chunk allocator, no xfrm buffer, no dynamic allocation beyond
 * std::map tree nodes.
 */
class AggInterpreter : public PushdownInterpreter {
 public:
  AggInterpreter(Uint32 prog_len,
                 Int64 table_id, Int64 frag_id,
                 Uint32 thread_id):
    PushdownInterpreter(PushdownType::AGGREGATION, prog_len,
                        table_id, frag_id, thread_id),
    m_prog(nullptr), m_cur_pos(0),
    m_n_gb_cols(0), m_gb_cols(nullptr),
    m_n_agg_results(0),
    m_agg_results(nullptr), m_agg_prog_start_pos(0),
    m_gb_map(nullptr), m_n_groups(0),
    m_attr_read_pos(0),
    m_processed_rows(0),
    m_result_size(0),
    m_gb_cmp_inited(false),
    m_alloc_len(0) {
      memset(m_decimal_buf, 0, sizeof(decimal_digit_t) * DECIMAL_BUFF_LENGTH);
      m_decimal.buf = m_decimal_buf;
      m_decimal.len = DECIMAL_BUFF_LENGTH;
      memset(m_attr_read_buf, 0, sizeof(m_attr_read_buf));
  }
  ~AggInterpreter() override {}

  bool OptimizeProgram();

  bool Init(const Uint32* prog);

  Int32 ProcessRec(Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct);

  void Print();
  Uint32 PrepareAggResIfNeeded(Signal* signal, bool force);
  Uint32 NumOfResRecords(bool last_time = false);
  const std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>* gb_map() const {
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
  Uint32* m_prog;
  Uint32 m_cur_pos;
  Register m_registers[kRegTotal];

  Uint32 m_n_gb_cols;
  Uint32* m_gb_cols;
  Uint32 m_n_agg_results;
  AggResItem* m_agg_results;
  Uint32 m_agg_prog_start_pos;

  std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>* m_gb_map;
  Uint32 m_n_groups;
  Uint32 m_attr_read_pos;
  static Uint32 g_attr_read_buf_len_;
  Uint64 m_processed_rows;
  Uint32 m_result_size;
  static Uint32 g_result_header_size_;
  static Uint32 g_result_header_size_per_group_;

  decimal_t m_decimal;
  decimal_digit_t m_decimal_buf[DECIMAL_BUFF_LENGTH];

  // Embedded interpreter validation
  bool validateEmbeddedProgram(const Uint32* emb_prog, Uint32 emb_len);

  // Per-column comparison context for type-aware GROUP BY
  GBCmpContext m_gb_cmp_ctx;
  bool m_gb_cmp_inited;
  void initGBCmpCtx(Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct);

  // Inline bump allocator
  char* MemAlloc(Uint32 len);
  Uint32 m_alloc_len;

  // All buffers inline — no separate allocation needed
  Uint32 m_attr_read_buf[ATTR_READ_BUF_WORD_SIZE];       //  8,192 B
  Uint32 m_prog_buf[MAX_AGG_PROGRAM_WORD_SIZE];           //  4,096 B
  Uint32 m_gb_cols_buf[MAX_AGG_N_GROUPBY_COLS];           //    512 B
  AggResItem m_agg_results_buf[MAX_AGG_N_RESULTS];        //  6,144 B
  std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp> m_gb_map_buf;  // ~48 B
  char m_mem_buf[MAX_AGG_RESULT_BATCH_BYTES];             //  8,192 B
};

/*
 * AggInterpreter packs all buffers inline to avoid extra allocations:
 *   m_attr_read_buf  : ATTR_READ_BUF_WORD_SIZE  * 4  =  8,192 B
 *   m_prog_buf       : MAX_AGG_PROGRAM_WORD_SIZE * 4  =  4,096 B
 *   m_gb_cols_buf    : MAX_AGG_N_GROUPBY_COLS    * 4  =    512 B
 *   m_agg_results_buf: MAX_AGG_N_RESULTS * sizeof(R)  = ~6,144 B
 *   m_mem_buf        : MAX_AGG_RESULT_BATCH_BYTES     =  8,192 B
 *
 * If any of these constants are increased or new fields are added, this
 * assert fires — review whether any buffer can be shrunk or moved to an
 * external allocation.
 */
static_assert(sizeof(AggInterpreter) <= MEM_CHUNK_SIZE,
              "AggInterpreter has exceeded the MEM_CHUNK_SIZE (32KB) page "
              "limit.  All inline buffers (prog_buf, attr_read_buf, mem_buf, "
              "etc.) must fit together on a single page.  Shrink a buffer or "
              "move it to an external allocation.");

#endif  // AGGINTERPRETER_H_
