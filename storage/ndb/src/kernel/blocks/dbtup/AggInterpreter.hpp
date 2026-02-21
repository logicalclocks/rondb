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
    m_prog(nullptr), m_prog_len(prog_len), m_cur_pos(0),
    m_inited(false), m_n_gb_cols(0), m_gb_cols(nullptr),
    gb_cmp_inited_(false),
    m_n_agg_results(0),
    m_agg_results(nullptr), m_agg_prog_start_pos(0),
    m_gb_map(nullptr), m_n_groups(0),
    m_attr_read_buf(nullptr), m_attr_read_pos(0),
    m_processed_rows(0),
    m_result_size(0), m_table_id(table_id), m_frag_id(frag_id),
    m_linked_attr_data(nullptr), m_linked_attr_len(0),
    m_use_mutex(false), m_max_groups(0),
    m_chunks(nullptr), m_chunks_tail(nullptr),
    m_current_chunk(nullptr), m_total_chunk_bytes(0),
    m_memory_budget(0), m_budget_increment(0),
    m_total_available(0), m_thread_id(thread_id),
    m_cached_agg_ops(nullptr), m_agg_ops_cached(false),
    m_gb_types(nullptr), m_gb_types_inited(false),
    m_xfrm_buf(nullptr), m_xfrm_buf_len(0),
    m_prog_buf(nullptr), m_gb_cols_buf(nullptr),
    m_agg_results_buf(nullptr), m_gb_map_buf(nullptr),
    m_mem_buf(nullptr), m_alloc_len(0), m_buf_block(nullptr),
    m_vec_search(false), m_vec_dims(0), m_vec_type(0),
    m_vec_metric(0), m_vec_col_idx(0), m_vec_top_n(0),
    m_vec_size_in_bytes(0), m_vec_buf(nullptr),
    m_vec_buf_pos(0), m_vec_start_pos(0),
    m_vec_max_rec_size(0),
    m_candidate_allocator(nullptr),
    m_curr_distance(std::numeric_limits<double>::max()),
    m_vec_n_candidates_sent(0),
    m_vec_size_candidates_sent(0),
    m_vec_search_scan_done(false),
    m_next_send_idx(-1), m_ext_prog_buf(nullptr) {
      memset(m_decimal_buf, 0, sizeof(decimal_digit_t) * DECIMAL_BUFF_LENGTH);
      m_decimal.buf = m_decimal_buf;
      m_decimal.len = DECIMAL_BUFF_LENGTH;
  }
  ~AggInterpreter() {
    freeAllChunks();
    if (m_xfrm_buf != nullptr) {
      lc_ndbd_pool_free(m_xfrm_buf);
      m_xfrm_buf = nullptr;
    }
    if (m_buf_block != nullptr) {
      lc_ndbd_pool_free(m_buf_block);
      m_buf_block = nullptr;
    }
  }
  ~AggInterpreter() override {}

  bool OptimizeProgram();
  void FreeMemForVectorSearch() {
    if (m_ext_prog_buf) {
      lc_ndbd_pool_free(m_ext_prog_buf);
      m_ext_prog_buf = nullptr;
    }
    if (m_vec_buf) {
      lc_ndbd_pool_free(m_vec_buf);
      m_vec_buf = nullptr;
    }

    if (m_candidate_allocator) {
      m_candidate_allocator->~CandidateAllocator();
      lc_ndbd_pool_free(m_candidate_allocator);
      m_candidate_allocator = nullptr;
    }
  }

  bool Init(const Uint32* prog);

  Int32 ProcessRec(Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct);

  void Print();
  Uint32 PrepareAggResIfNeeded(Signal* signal, bool force);
  Uint32 NumOfResRecords(bool last_time = false);
  static void MergePrint(const AggInterpreter* in1, const AggInterpreter* in2);
  const GBHashTable* gb_map() const {
    return m_gb_map;
  }
  Int64 table_id() {
    return m_table_id;
  }
  GBHashTable* gb_map_mutable() {
    return m_gb_map;
  }
  Uint32 val_len() const {
    return m_n_agg_results * sizeof(AggResItem);
  }
  Uint32 n_gb_cols() const { return m_n_gb_cols; }
  Uint32 n_agg_results() const { return m_n_agg_results; }
  const AggResItem* agg_results() const { return m_agg_results; }
  Uint64 processed_rows() const { return m_processed_rows; }
  void setUseMutex(bool v) { m_use_mutex = v; }
  void setMaxGroups(Uint32 v) { m_max_groups = v; }
  Uint32 maxGroups() const { return m_max_groups; }
  Int32 evictOneGroup(Uint32* buf, Uint32 buf_words,
                      Uint32* words_written);
  void initChunkAllocator(Uint32 thread_id, Uint32 budget_pages,
                          Uint32 available_pages);
  bool bookMoreMemory();
  char* allocGroupData(Uint32 len, Uint32 key_len);
  void freeGroupData(char* ptr);
  void freeAllChunks();
  Int64 frag_id() {
    return m_frag_id;
  }
  static void Destruct(AggInterpreter* ptr);
  /* For using Ndbd_mem_manager*/
  /*
  Ndbd_mem_manager* mm() {
    return mm_;
  }
  Uint32 page_ref() {
    return page_ref_;
  }
  */
  bool vec_search() {
    return m_vec_search;
  }
  bool HasAnyVecResult() {
    return m_vec_search && !m_vec_top_n_results.empty();
  }
  Uint32 NoofVecResults() {
    return m_vec_top_n_results.size();
  }
  bool IsCandidateBufAllocated() {
    return m_candidate_allocator != nullptr;
  }
  Int32 CopyVecCandidateFromSignal(Signal* signal, Uint32 ToutBufIndex);
  Uint32 CopyVecCandidateToSignal(Signal* signal);
  void PrepareVecCandidates();
  Uint32 CopyOneVecCandidateToSignal(Signal* signal);
  void set_vec_max_rec_size(Uint32 size);
  void set_vec_search_scan_done(bool value) {
    m_vec_search_scan_done = value;
  }
  bool vec_search_scan_done() {
    return m_vec_search_scan_done;
  }
  Int32 next_send_idx() {
    return m_next_send_idx;
  }
  Uint32 n_gb_cols() const { return m_n_gb_cols; }
  Uint32 n_agg_results() const { return m_n_agg_results; }
  const AggResItem* agg_results() const { return m_agg_results; }
  Uint64 processed_rows() const { return m_processed_rows; }

 private:
  Uint32* m_prog;
  Uint32 m_prog_len;
  Uint32 m_cur_pos;
  bool m_inited;
  Register m_registers[kRegTotal];

  Uint32 m_n_gb_cols;
  Uint32* m_gb_cols;
  GBCmpContext gb_cmp_ctx_;
  bool gb_cmp_inited_;
  Uint32 m_n_agg_results;
  AggResItem* m_agg_results;
  Uint32 m_agg_prog_start_pos;

  GBHashTable* m_gb_map;
  Uint32 m_n_groups;
  Uint32* m_attr_read_buf;
  Uint32 m_attr_read_pos;
  static Uint32 g_attr_read_buf_len_;
  Uint64 m_processed_rows;
  Uint32 m_result_size;
  static Uint32 g_result_header_size_;
  static Uint32 g_result_header_size_per_group_;

  Int64 m_table_id;
  Int64 m_frag_id;
  decimal_t m_decimal;
  decimal_digit_t m_decimal_buf[DECIMAL_BUFF_LENGTH];

  // Linked attribute buffer for join aggregation
  const Uint32* m_linked_attr_data;   // Points to current row's linked attrs
  Uint32 m_linked_attr_len;           // Current length in words

  // MUTEX_BASED locking: protects m_gb_map and accumulators during
  // concurrent access from multiple LDM threads.
  bool m_use_mutex;                   // true for MUTEX_BASED strategy
  std::mutex m_mutex;

  // Group eviction: when m_max_groups > 0 and m_gb_map reaches this
  // limit, processRecWithLinkedAttrs returns AGG_EVICT_NEEDED so the
  // caller can evict a group before retrying.
  Uint32 m_max_groups;                // 0 = unlimited

  // Chunk-based allocator for group data.
  // Allocates from 32KB chunks via lc_ndbd_pool_malloc,
  // with per-chunk reference counting so eviction can free memory.
  MemChunk* m_chunks;                 // doubly-linked list head
  MemChunk* m_chunks_tail;            // doubly-linked list tail
  MemChunk* m_current_chunk;          // chunk currently bump-allocating from
  Uint32 m_total_chunk_bytes;         // total bytes across all chunks
  Uint32 m_memory_budget;             // current budget (bytes), grows via bookMoreMemory
  Uint32 m_budget_increment;          // bytes added per bookMoreMemory call
  Uint32 m_total_available;           // total available at setup (bytes), booking cap
  Uint32 m_thread_id;                 // for lc_ndbd_pool_malloc calls

  MemChunk* allocNewChunk();

  // Embedded interpreter validation (called at Init time)
  bool validateEmbeddedProgram(const Uint32* emb_prog, Uint32 emb_len);

  // Per-column comparison context for type-aware GROUP BY
  GBCmpContext m_gb_cmp_ctx;
  bool m_gb_cmp_inited;
  void initGBCmpCtx(Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct);

  // Inline bump allocator
  char* MemAlloc(Uint32 len);
  Uint32 m_alloc_len;

  Uint32* m_prog_buf;
  Uint32* m_gb_cols_buf;
  AggResItem* m_agg_results_buf;
  GBHashTable* m_gb_map_buf;

  char* m_mem_buf;
  Uint32 m_alloc_len;

  // Single allocation block for all dynamically allocated buffers above.
  // Allocated in Init(), freed in destructor.
  void* m_buf_block;

  /* Vector */
  bool m_vec_search;
  Uint32 m_vec_dims;
  Uint32 m_vec_type;
  Uint32 m_vec_metric;
  Uint32 m_vec_col_idx;
  Uint32 m_vec_top_n;
  Uint32 m_vec_size_in_bytes;
  Uint32* m_vec_buf;
  Uint32 m_vec_buf_pos;
  Uint32 m_vec_start_pos;
  static Uint32 g_vec_buf_len_;

  class Candidate {
   public:
    Candidate(double distance, Uint32 tuple_len, Int32 idx) :
      m_distance(distance),
      m_actual_buf_len(tuple_len),
      m_idx_in_allocator(idx),
      m_buf(nullptr) {
      m_buf = reinterpret_cast<Uint32*>(this + 1);
    }
    void Init(const Uint32* tuple) {
      memcpy(m_buf, tuple, m_actual_buf_len * sizeof(Uint32));
    }
    ~Candidate() {
      /*
       * No need to release m_buf — it comes from the pool_ of CandidateAllocator,
       * which will be released automatically at the end.
       */
    }
    double m_distance;
    Uint32 m_actual_buf_len;
    Int32 m_idx_in_allocator;
    Uint32* m_buf;
  };
  struct ByDistance {
    bool operator()(const Candidate* lhs, const Candidate* rhs) {
      return lhs->m_distance < rhs->m_distance ? true : false;
    }
  };

  class CandidateAllocator {
   public:
     CandidateAllocator(size_t max_candidates, size_t max_buf_len,
                        Int64 table_id, Int64 frag_id)
       : m_init(false), m_max_candidates(max_candidates),
       m_max_buf_len(max_buf_len),
       m_next_index(0), m_reuse_started(false),
       m_table_id(table_id), m_frag_id(frag_id),
       m_slots_per_full_segment(0),
       m_shift_k(0)
       , m_n_segments(0)
       {
         m_total_size = m_max_candidates * (sizeof(Candidate) + m_max_buf_len * sizeof(Uint32));
         m_slot_size = sizeof(Candidate) + m_max_buf_len * sizeof(Uint32);
       }

     ~CandidateAllocator() {
       for (size_t i = 0; i < m_n_segments; i++) {
         auto& seg = m_segments[i];
         if (seg.ptr) {
           lc_ndbd_pool_free(seg.ptr);
         }
       }
     }

     Int32 Init(Uint32 thread_id);

     static Uint32 g_max_results_size;
     static Uint32 g_segment_size;

     Candidate* Allocate(double distance, const Uint32* tuple, Uint32 actual_buf_len);
     void set_next_index(size_t next_index) {
       assert(m_reuse_started);
       m_next_index = next_index;
     }

   private:
     struct Segment {
       char* ptr;
       size_t size;
     };
     bool m_init;
     size_t m_max_candidates;
     size_t m_max_buf_len;
     size_t m_next_index;
     size_t m_total_size;
     bool m_reuse_started;
     Int64 m_table_id;
     Int64 m_frag_id;
     size_t m_slot_size;
     size_t m_slots_per_full_segment;
     size_t m_shift_k;
     Segment m_segments[MAX_CANDIDATE_SEGMENTS];
     size_t m_n_segments;
  };

  Uint32 m_vec_max_rec_size; // in words
  CandidateAllocator* m_candidate_allocator;
  double m_curr_distance;
  Uint32 m_vec_n_candidates_sent;
  Uint32 m_vec_size_candidates_sent;
  std::priority_queue<Candidate*,
    std::vector<Candidate*>,
    ByDistance> m_vec_top_n_results;
  bool m_vec_search_scan_done;
  Int32 m_next_send_idx;
  std::vector<Candidate*> m_vec_top_n_results_final;
  Uint32* m_ext_prog_buf;
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
