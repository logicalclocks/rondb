/*
 * Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.
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

#ifndef AGGINTERPRETER_H_
#define AGGINTERPRETER_H_

#include <math.h>
#include <map>
#include <mutex>
#include "Dbtup.hpp"
#include "NdbAggregationCommon.hpp"

#include <simsimd/simsimd.h>
#include <queue>

#define READ_BUF_WORD_SIZE 2048
#define DECIMAL_BUFF_LENGTH 9
#define AGG_EVICT_NEEDED 1
#define MEM_CHUNK_SIZE 32768

struct MemChunk {
  char* data;
  Uint32 capacity;
  Uint32 used;
  Uint32 live_groups;
  MemChunk* next;
  MemChunk* prev;
  char* group_list;         // singly-linked list of live groups in this chunk
};

class AggInterpreter {
 public:
  AggInterpreter(const Uint32* prog, Uint32 prog_len,
                 Int64 table_id, Int64 frag_id,
                 Uint32 thread_id):
    prog_len_(prog_len), cur_pos_(0),
    inited_(false),
    n_gb_cols_(0), gb_cols_(nullptr),
    gb_cmp_inited_(false),
    n_agg_results_(0),
    agg_results_(nullptr), agg_prog_start_pos_(0),
    gb_map_(nullptr), n_groups_(0),
    buf_pos_(0), processed_rows_(0),
    result_size_(0), table_id_(table_id), frag_id_(frag_id),
    thread_id_(thread_id), alloc_len_(0),
    vec_search_(false), vec_dims_(0), vec_type_(0),
    vec_metric_(0), vec_col_idx_(0), vec_top_n_(0),
    vec_size_in_bytes_(0), vec_buf_(nullptr),
    vec_buf_pos_(0), vec_start_pos_(0),
    vec_max_rec_size_(0),
    candidate_allocator_(nullptr),
    curr_distance_(std::numeric_limits<double>::max()),
    vec_n_candidates_sent_(0),
    vec_size_candidates_sent_(0),
    vec_search_scan_done_(false),
    next_send_idx_(-1), ext_prog_buf_(nullptr),
    m_linked_attr_data(nullptr), m_linked_attr_len(0),
    m_use_mutex(false), m_max_groups(0),
    m_chunks(nullptr), m_chunks_tail(nullptr),
    m_current_chunk(nullptr), m_total_chunk_bytes(0),
    m_memory_budget(0), m_budget_increment(0),
    m_total_available(0), m_thread_id(0),
    m_agg_ops_cached(false) {
      assert(prog_len_ <= MAX_AGG_PROGRAM_WORD_SIZE);
      prog_ = prog_buf_;
      memcpy(prog_, prog, prog_len * sizeof(Uint32));
      memset(buf_, 0, READ_BUF_WORD_SIZE * sizeof(Uint32));
      memset(decimal_buf_, 0, sizeof(decimal_digit_t) * DECIMAL_BUFF_LENGTH);
      decimal_.buf = decimal_buf_;
      decimal_.len = DECIMAL_BUFF_LENGTH;
  }
  ~AggInterpreter() {
    freeAllChunks();
  }

  bool Init();
  bool OptimizeProgram();
  void FreeMemForVectorSearch() {
#ifdef PA_MALLOC
    if (ext_prog_buf_) {
      lc_ndbd_pool_free(ext_prog_buf_);
      ext_prog_buf_ = nullptr;
    }
    if (vec_buf_) {
      lc_ndbd_pool_free(vec_buf_);
      vec_buf_ = nullptr;
    }
#else
    if (vec_buf_) {
      delete[] vec_buf_;
      vec_buf_ = nullptr;
    }
#endif // PA_MALLOC

    if (candidate_allocator_) {
      candidate_allocator_->~CandidateAllocator();
      lc_ndbd_pool_free(candidate_allocator_);
      candidate_allocator_ = nullptr;
    }
  }

  bool Init(const Uint32* prog);

  Int32 ProcessRec(Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct,
                   bool* vec_update_candidate);

  Int32 processRecWithLinkedAttrs(
      Dbtup* block_tup,
      Dbtup::KeyReqStruct* req_struct,
      const Uint32* linked_attr_data,
      Uint32 linked_attr_len);
  Int32 finalizeResults();
  Int32 getResultData(Uint32* buffer, Uint32 buffer_size,
                      Uint32* bytes_written);
  Uint32 mergeFrom(AggInterpreter* other, Uint32 max_groups);

  void Print();
  Uint32 PrepareAggResIfNeeded(Signal* signal, bool force);
  Uint32 NumOfResRecords(bool last_time = false);
  static void MergePrint(const AggInterpreter* in1, const AggInterpreter* in2);
  const std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>* gb_map() {
    return gb_map_;
  }
  Int64 table_id() {
    return table_id_;
  }
  std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>* gb_map_mutable() {
    return gb_map_;
  }
  Uint32 n_gb_cols() const { return n_gb_cols_; }
  Uint32 n_agg_results() const { return n_agg_results_; }
  const AggResItem* agg_results() const { return agg_results_; }
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
    return frag_id_;
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
    return vec_search_;
  }
  bool HasAnyVecResult() {
    return vec_search_ && !vec_top_n_results_.empty();
  }
  Uint32 NoofVecResults() {
    return vec_top_n_results_.size();
  }
  bool IsCandidateBufAllocated() {
    return candidate_allocator_ != nullptr;
  }
  Int32 CopyVecCandidateFromSignal(Signal* signal, Uint32 ToutBufIndex);
  Uint32 CopyVecCandidateToSignal(Signal* signal);
  void PrepareVecCandidates();
  Uint32 CopyOneVecCandidateToSignal(Signal* signal);
  void set_vec_max_rec_size(Uint32 size);
  void set_vec_search_scan_done(bool value) {
    vec_search_scan_done_ = value;
  }
  bool vec_search_scan_done() {
    return vec_search_scan_done_;
  }
  Int32 next_send_idx() {
    return next_send_idx_;
  }

 private:
  Uint32* prog_;
  Uint32 prog_len_;
  Uint32 cur_pos_;
  bool inited_;
  Register registers_[kRegTotal];

  Uint32 n_gb_cols_;
  Uint32* gb_cols_;
  GBCmpContext gb_cmp_ctx_;
  bool gb_cmp_inited_;
  Uint32 n_agg_results_;
  AggResItem* agg_results_;
  Uint32 agg_prog_start_pos_;

  std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>* gb_map_;
  Uint32 n_groups_;
  Uint32 buf_[READ_BUF_WORD_SIZE];
  Uint32 buf_pos_;
  static Uint32 g_buf_len_;
  Uint64 processed_rows_;
  Uint32 result_size_;
  static Uint32 g_result_header_size_;
  static Uint32 g_result_header_size_per_group_;

  Int64 table_id_;
  Int64 frag_id_;
  decimal_t decimal_;
  decimal_digit_t decimal_buf_[DECIMAL_BUFF_LENGTH];
  Uint32 thread_id_;

  // Linked attribute buffer for join aggregation
  const Uint32* m_linked_attr_data;   // Points to current row's linked attrs
  Uint32 m_linked_attr_len;           // Current length in words

  // MUTEX_BASED locking: protects gb_map_ and accumulators during
  // concurrent access from multiple LDM threads.
  bool m_use_mutex;                   // true for MUTEX_BASED strategy
  std::mutex m_mutex;

  // Group eviction: when m_max_groups > 0 and gb_map_ reaches this
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

  // Cached agg ops for merge (avoids recomputing per CONTINUEB batch)
  Uint8 m_cached_agg_ops[MAX_AGG_N_RESULTS];
  bool m_agg_ops_cached;

  Uint32 prog_buf_[MAX_AGG_PROGRAM_WORD_SIZE];
  Uint32 gb_cols_buf_[MAX_AGG_N_GROUPBY_COLS];
  AggResItem agg_results_buf_[MAX_AGG_N_RESULTS];
  std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp> gb_map_buf_;

  char mem_buf_[MAX_AGG_RESULT_BATCH_BYTES];
  Uint32 alloc_len_;
  char* MemAlloc(Uint32 len);

  /* Vector */
  bool vec_search_;
  Uint32 vec_dims_;
  Uint32 vec_type_;
  Uint32 vec_metric_;
  Uint32 vec_col_idx_;
  Uint32 vec_top_n_;
  Uint32 vec_size_in_bytes_;
  Uint32* vec_buf_;
  Uint32 vec_buf_pos_;
  Uint32 vec_start_pos_;
  static Uint32 g_vec_buf_len_;

  class Candidate {
   public:
    Candidate(double distance, Uint32 tuple_len, Int32 idx) :
      distance_(distance),
      actual_buf_len_(tuple_len),
      idx_in_allocator_(idx),
      buf_(nullptr) {
      buf_ = reinterpret_cast<Uint32*>(this + 1);
    }
    void Init(const Uint32* tuple) {
      memcpy(buf_, tuple, actual_buf_len_ * sizeof(Uint32));
    }
    ~Candidate() {
      /*
       * No need to release buf_ — it comes from the pool_ of CandidateAllocator,
       * which will be released automatically at the end.
       */
    }
    double distance_;
    Uint32 actual_buf_len_;
    Int32 idx_in_allocator_;
    Uint32* buf_;
  };
  struct ByDistance {
    bool operator()(const Candidate* lhs, const Candidate* rhs) {
      return lhs->distance_ < rhs->distance_ ? true : false;
    }
  };

  class CandidateAllocator {
   public:
     CandidateAllocator(size_t max_candidates, size_t max_buf_len,
                        Int64 table_id, Int64 frag_id)
       : init_(false), max_candidates_(max_candidates),
       max_buf_len_(max_buf_len),
       next_index_(0), reuse_started_(false),
       table_id_(table_id), frag_id_(frag_id),
       slots_per_full_segment_(0),
       shift_k_(0)
       , n_segments_(0)
       {
         total_size_ = max_candidates_ * (sizeof(Candidate) + max_buf_len_ * sizeof(Uint32));
         slot_size_ = sizeof(Candidate) + max_buf_len_ * sizeof(Uint32);
       }

     ~CandidateAllocator() {
       for (size_t i = 0; i < n_segments_; i++) {
         auto& seg = segments_[i];
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
       assert(reuse_started_);
       next_index_ = next_index;
     }

   private:
     struct Segment {
       char* ptr;
       size_t size;
     };
     bool init_;
     size_t max_candidates_;
     size_t max_buf_len_;
     size_t next_index_;
     size_t total_size_;
     bool reuse_started_;
     Int64 table_id_;
     Int64 frag_id_;
     size_t slot_size_;
     size_t slots_per_full_segment_;
     size_t shift_k_;
     Segment segments_[MAX_CANDIDATE_SEGMENTS];
     size_t n_segments_;
  };

  Uint32 vec_max_rec_size_; // in words
  CandidateAllocator* candidate_allocator_;
  double curr_distance_;
  Uint32 vec_n_candidates_sent_;
  Uint32 vec_size_candidates_sent_;
  std::priority_queue<Candidate*,
    std::vector<Candidate*>,
    ByDistance> vec_top_n_results_;
  bool vec_search_scan_done_;
  Int32 next_send_idx_;
  std::vector<Candidate*> vec_top_n_results_final_;
  Uint32* ext_prog_buf_;
};
#endif  // AGGINTERPRETER_H_
