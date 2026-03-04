/*
 * Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.
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

#ifndef VECSEARCHINTERPRETER_H_
#define VECSEARCHINTERPRETER_H_

#include <cstring>
#include <limits>
#include <queue>
#include <vector>

#include "PushdownInterpreter.hpp"
#include "Dbtup.hpp"
#include "NdbAggregationCommon.hpp"

#include <simsimd/simsimd.h>

#define MEM_CHUNK_SIZE 32768

class VecSearchInterpreter : public PushdownInterpreter {
 public:
  VecSearchInterpreter(Uint32 prog_len,
                       Int64 table_id, Int64 frag_id,
                       Uint32 thread_id)
    : PushdownInterpreter(PushdownType::VECTOR_SEARCH, prog_len,
                          table_id, frag_id, thread_id),
      m_prog(nullptr),
      m_vec_dims(0), m_vec_type(0),
      m_vec_metric(0), m_vec_col_idx(0), m_vec_top_n(0),
      m_vec_size_in_bytes(0), m_vec_buf(nullptr),
      m_vec_buf_pos(0), m_vec_start_pos(0),
      m_vec_max_rec_size(0),
      m_candidate_allocator(nullptr),
      m_curr_distance(std::numeric_limits<double>::max()),
      m_vec_n_candidates_sent(0),
      m_vec_size_candidates_sent(0),
      m_vec_search_scan_done(false),
      m_next_send_idx(-1), m_ext_prog_buf(nullptr) {}

  ~VecSearchInterpreter() override {
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

  Int32 ProcessRec(Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct,
                   bool* vec_update_candidate);

  bool HasAnyVecResult() {
    return !m_vec_top_n_results.empty();
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

 private:
  Uint32* m_prog;

  /* Vector search fields */
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

static_assert(sizeof(VecSearchInterpreter) <= MEM_CHUNK_SIZE,
              "VecSearchInterpreter must fit in MEM_CHUNK_SIZE allocation");

/*
 * Safety check: the Init() inline-program optimisation stores the program in
 * the leftover space on the object's 32KB page.  If the object grows so large
 * that the max realistic VS program no longer fits inline, this assert fires
 * at compile time — reminding the developer to either shrink the object or
 * acknowledge that the external-page fallback in Init() will now be the
 * normal path.
 *
 * Max realistic program: header (8 words) + MAX_VEC_DIMS (8100) = 8108 words.
 */
static_assert(
    MEM_CHUNK_SIZE - sizeof(VecSearchInterpreter) >=
        (8 + MAX_VEC_DIMS) * sizeof(Uint32),
    "VecSearchInterpreter has grown too large: the max VS program no longer "
    "fits inline on the object's 32KB page.  The external-page fallback in "
    "Init() will handle this correctly, but review whether the object can be "
    "trimmed.  To silence this, update the threshold.");

#endif  // VECSEARCHINTERPRETER_H_
