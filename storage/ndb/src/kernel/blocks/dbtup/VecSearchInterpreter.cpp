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

#include <cstring>

#define DBTUP_C
#include "VecSearchInterpreter.hpp"
#include "signaldata/TransIdAI.hpp"
#include "ndbd_malloc.hpp"
#include "../record_types.hpp"
#include "util/require.h"

#include <simsimd/simsimd.h>

/*
 * VS related
 * Turn on the DEBUG_VS_INTERP
 * to trace VecSearchInterpreter on partition DEBUG_VS_INTERP_PART_ID
 */
#undef DEBUG_VS_INTERP
// #define DEBUG_VS_INTERP 1
#define DEBUG_VS_INTERP_TABLE_ID 17
#define DEBUG_VS_INTERP_PART_ID 0
#ifdef DEBUG_VS_INTERP
#define VS_INTERP_TRACE(table_id, part_id, format, ...) \
  do {\
    if ((table_id == DEBUG_VS_INTERP_TABLE_ID) && \
        (part_id == DEBUG_VS_INTERP_PART_ID)) {\
      g_eventLogger->info("[VS_INTERP_TRACE] " format, ##__VA_ARGS__); \
    }\
  } while (0)
#else
#define VS_INTERP_TRACE(table_id, part_id, format, ...) {}
#endif // DEBUG_VS_INTERP

/*
 * Since we allocate one page (32 KB) to accommodate the vector search
 * program — with a maximum size of MAX_VEC_SEARCH_PROGRAM_WORD_SIZE (8192 words) —
 * this effectively limits the maximum supported vector dimension.
 *
 * We also allocate a 1-page (32 KB) buffer for reading vector column values.
 * This buffer has a capacity of 8192 words, which is slightly larger than
 * the size of a vector with the current maximum dimension (MAX_VEC_DIMS = 8100).
 */
Uint32 VecSearchInterpreter::g_vec_buf_len_ = MAX_VEC_SEARCH_PROGRAM_WORD_SIZE;

Uint32 VecSearchInterpreter::CandidateAllocator::g_max_results_size = 100 * 1024 * 1024; /*100 MB*/
Uint32 VecSearchInterpreter::CandidateAllocator::g_segment_size = 1 * 1024 * 1024; /*1 MB*/

bool VecSearchInterpreter::Init(const Uint32* prog) {
  if (m_inited) {
    return true;
  }

  require(prog != nullptr);

  /*
   * Copy the program into a local buffer.
   *
   * The VecSearchInterpreter object is placement-new'd at the start of a
   * MEM_CHUNK_SIZE (32KB) page.  If the program fits in the remaining space
   * after the object, we store it inline — avoiding a separate 32KB page
   * allocation.  This covers all realistic VS programs (max realistic
   * program = header + MAX_VEC_DIMS = ~8108 words).  Fall back to an
   * external page only when the object grows too large in future development.
   */
  assert(m_prog_len <= MAX_VEC_SEARCH_PROGRAM_WORD_SIZE);
  static constexpr Uint32 INLINE_AVAIL_WORDS =
      (MEM_CHUNK_SIZE - sizeof(VecSearchInterpreter)) / sizeof(Uint32);
  if (m_prog_len <= INLINE_AVAIL_WORDS) {
    /* Use the leftover space on the object's own 32KB page. */
    m_prog = reinterpret_cast<Uint32*>(
        reinterpret_cast<char*>(this) + sizeof(VecSearchInterpreter));
  } else {
    /* External page for very large programs or if the object grows. */
    void* page_ptr = lc_ndbd_pool_malloc(MEM_CHUNK_SIZE, RG_QUERY_MEMORY,
        m_thread_id, false);
    if (page_ptr == nullptr) {
      g_eventLogger->error("Alloc mem for pushdown vector search interpreter failed");
      return false;
    }
    m_ext_prog_buf = static_cast<Uint32*>(page_ptr);
    m_prog = m_ext_prog_buf;
  }

  memcpy(m_prog, prog, m_prog_len * sizeof(Uint32));

  Uint32 cur_pos = 0;
  Uint32 value = 0;

  /*
   * 1. Double check the magic num and total length of program.
   */
  value = m_prog[cur_pos++];
  assert(((value & 0xFFFF0000) >> 16) == 0x0721);
  assert((value & 0xFFFF) == m_prog_len);

  /*
   * 2. Get num of columns for group by and num of aggregation results;
   *    (For VS these are not used but we still parse the header.)
   */
  value = m_prog[cur_pos++];

  Uint32 version = m_prog[cur_pos++];
  if (version > PUSHDOWN_AGGREGATION_VERSION) {
    g_eventLogger->warning("Pushdown aggregation program version(%u) is "
                           "not compatible with "
                           "the version (%u) on data node",
                           version, PUSHDOWN_AGGREGATION_VERSION);
    return true;
  }

  /* Word 3: bit 31 must be set for VS */
  assert(m_prog[cur_pos] & 0x80000000);
  assert((m_prog[cur_pos] & 0x7FFFFFFF) == 0);
  cur_pos++;

  /* Parse VS-specific header */
  value = m_prog[cur_pos++];
  m_vec_type = (value >> 24) & 0xFF;
  m_vec_metric = (value >> 16) & 0xFF;
  m_vec_dims = value & 0xFFFF;

  value = m_prog[cur_pos++];
  m_vec_top_n = (value) & 0xFFFF;
  m_vec_col_idx = (value >> 16) & 0xFFFF;
  value = m_prog[cur_pos++];
  m_vec_size_in_bytes = (value & 0xFFFFFFFF);

  m_vec_start_pos = cur_pos;

  /* Allocate buffer for reading vector column values */
  void* page_ptr = lc_ndbd_pool_malloc(32 * 1024, RG_QUERY_MEMORY,
      m_thread_id, false);
  if (page_ptr == nullptr) {
    g_eventLogger->error("Alloc mem for pushdown vector search interpreter failed");
    return false;
  }
  m_vec_buf = static_cast<Uint32*>(page_ptr);

  m_inited = true;
  return true;
}

/*
 * Success: RETURN 0
 * Failure: RETURN error code
 */
Int32 VecSearchInterpreter::ProcessRec(Dbtup* block_tup,
        Dbtup::KeyReqStruct* req_struct,
        bool* vec_update_candidate) {
  if (!m_inited || req_struct->read_length != 0) {
    g_eventLogger->debug("VecSearchInterpreter::ProcessRec error at entry: "
            "inited=%d, read_length=%u",
            m_inited, req_struct->read_length);
    return ZAGG_OTHER_ERROR;
  }

  *vec_update_candidate = false;

  const Uint32 vec_col_idx = m_vec_col_idx & 0x0000FFFF;
  int ret = block_tup->readSingleAttribute(
      req_struct, vec_col_idx,
      m_vec_buf + m_vec_buf_pos, g_vec_buf_len_ - m_vec_buf_pos);
  if (ret < 0) {
    g_eventLogger->debug("read vector column error: %d", ret);
    return -ret;
  }
  AttributeHeader* header = nullptr;
  header = reinterpret_cast<AttributeHeader*>(m_vec_buf + m_vec_buf_pos);
  const Uint32* attrDescriptor = req_struct->tablePtrP->tabDescriptor +
    (vec_col_idx * ZAD_SIZE);
  const Uint32 TattrDesc1 = attrDescriptor[0];
  const Uint32 type_id = AttributeDescriptor::getType(TattrDesc1);
  const Uint32 array_type = AttributeDescriptor::getArrayType(TattrDesc1);

#ifdef DEBUG_VS_INTERP
  const Uint32 attributeId = header->getAttributeId();
  assert(attributeId == vec_col_idx);
  const Uint32 size = AttributeDescriptor::getSize(TattrDesc1);
  const Uint32 size_in_bytes = AttributeDescriptor::getSizeInBytes(TattrDesc1);
  const Uint32 size_in_words = AttributeDescriptor::getSizeInWords(TattrDesc1);
  const Uint32 array_size = AttributeDescriptor::getArraySize(TattrDesc1);
  const Uint32 nullable = AttributeDescriptor::getNullable(TattrDesc1);
  const Uint32 distri_key = AttributeDescriptor::getDKey(TattrDesc1);
  const Uint32 primary_key = AttributeDescriptor::getPrimaryKey(TattrDesc1);
  const Uint32 dynamic = AttributeDescriptor::getDynamic(TattrDesc1);
  const Uint32 disk_based = AttributeDescriptor::getDiskBased(TattrDesc1);
  VS_INTERP_TRACE(m_table_id, m_frag_id,
       "AttributeDescriptor, attributeId: %u, type_id: %u, size: %u, "
       "size_in_bytes: %u, size_in_words: %u, array_type: %u, "
       "array_size: %u, nullable: %u, distri_key: %u, primary_key: %u "
       "dynamic: %u, disk_based: %u",
       attributeId, type_id, size, size_in_bytes, size_in_words, array_type,
       array_size, nullable, distri_key, primary_key, dynamic, disk_based);
#endif  // DEBUG_VS_INTERP

  if (type_id != NDB_TYPE_LONGVARBINARY) {
    g_eventLogger->debug("Unsupported vector column type: %u", type_id);
    return ZAGG_COL_TYPE_UNSUPPORTED;
  }

  Uint32 length_bytes = 0;
  if (array_type == NDB_ARRAYTYPE_SHORT_VAR) {
    length_bytes = 1;
  } else if (array_type == NDB_ARRAYTYPE_MEDIUM_VAR) {
    length_bytes = 2;
  } else {
    assert(0);
  }
  Uint32 dims = 0;
  if (!header->isNULL()) {
#if DEBUG
    Uint32 len = header->getByteSize();
    assert(len >= length_bytes);
#endif  // DEBUG
    if (length_bytes == 1) {
      dims = *((Uint8*)header->getDataPtr());
    } else {
      dims = *((Uint16*)header->getDataPtr());
    }
    assert(m_vec_dims == dims / sizeof(float));
  } else {
    assert(0);
  }

  double distance = 0;
  float* target = (float* )&(m_prog[m_vec_start_pos]);
  float* current = (float* )((char*)header->getDataPtr() + length_bytes);
  simsimd_l2sq_f32(current, target, m_vec_dims, &distance);
  m_curr_distance = distance;
  if (m_vec_top_n != 0 &&
      (m_vec_top_n_results.size() < m_vec_top_n ||
      distance < m_vec_top_n_results.top()->m_distance)) {
    *vec_update_candidate = true;
  }

  return 0;
}

Int32 VecSearchInterpreter::CopyVecCandidateFromSignal(Signal* signal,
                                                Uint32 ToutBufIndex) {
  if (m_candidate_allocator == nullptr) {
    VS_INTERP_TRACE(m_table_id, m_frag_id,
                    "CandidateAllocator pre-allocating memory, "
                    "top_n: %u, vec_max_rec_size: %u, actual_rec_size: %u",
                    m_vec_top_n, m_vec_max_rec_size, ToutBufIndex);
    void* ca_mem = lc_ndbd_pool_malloc(
        sizeof(CandidateAllocator), RG_QUERY_MEMORY, m_thread_id, false);
    if (ca_mem == nullptr) {
      g_eventLogger->error("Alloc mem for CandidateAllocator failed");
      return ZAGG_ALLOC_MEM_FAILED;
    }
    m_candidate_allocator = new (ca_mem) CandidateAllocator(
        m_vec_top_n, m_vec_max_rec_size, m_table_id, m_frag_id);
    int ret = m_candidate_allocator -> Init(m_thread_id);
    if (ret != 0) {
      return ret;
    }
  }
  // The actual record size can't be larger than the m_vec_max_rec_size
  // TODO (Zhao)
  // handle this error
  assert(ToutBufIndex <= m_vec_max_rec_size);

  if (m_vec_top_n_results.size() >= m_vec_top_n) {
    Candidate* knockout = m_vec_top_n_results.top();
    VS_INTERP_TRACE(m_table_id, m_frag_id,
                    "Picked the candidate with distance %lf, idx: %d as the next knockout",
        knockout->m_distance,
        knockout->m_idx_in_allocator);
    m_vec_top_n_results.pop();
    m_candidate_allocator->set_next_index(knockout->m_idx_in_allocator);
    // No need to delete 'knockout' — it will be reused by the next selected candidate.
  }

  Candidate* selected = m_candidate_allocator->
      Allocate(m_curr_distance, &(signal->theData[25]), ToutBufIndex);
  if (selected == nullptr) {
    return -1;
  }
  m_vec_top_n_results.push(selected);
  VS_INTERP_TRACE(m_table_id, m_frag_id,
                  "Push one candidate with distance %lf, [%lu/%u], idx_in_allocator: %u",
                  m_curr_distance, m_vec_top_n_results.size(), m_vec_top_n,
                  selected->m_idx_in_allocator);
  return 0;
}

void VecSearchInterpreter::PrepareVecCandidates() {
  m_vec_top_n_results_final.clear();
  while (!m_vec_top_n_results.empty()) {
    m_vec_top_n_results_final.push_back(m_vec_top_n_results.top());
    m_vec_top_n_results.pop();
  }
  m_next_send_idx = m_vec_top_n_results_final.size() - 1;
  VS_INTERP_TRACE(m_table_id, m_frag_id,
                  "PrepareVecCandidates %lu",
                  m_vec_top_n_results_final.size());
}

Uint32 VecSearchInterpreter::CopyOneVecCandidateToSignal(Signal* signal) {
  if (m_next_send_idx >= 0) {
    Candidate* next_send = m_vec_top_n_results_final[m_next_send_idx];
    if (next_send->m_actual_buf_len > 3) {
      // Fast path
      AttributeHeader header = *(AttributeHeader*)(next_send->m_buf + next_send->m_actual_buf_len - 3);
      if (header.getAttributeId() == AttributeHeader::VEC_DISTANCE &&
          *(double*)(next_send->m_buf + next_send->m_actual_buf_len - 2) == 721.721) {
        /* Fill in the vec_closes_ to the reserved area which contains the magic word 721.721*/
        *(double*)(next_send->m_buf + next_send->m_actual_buf_len - 2) = next_send->m_distance;
      }
    } else {
      // TODO (Zhao)
      // It doesn't seem to work with index scans, since in an index scan
      // there is an NdbRecord (READ_PACKED) packet at the beginning of the result.
      Uint32 pos = 0;
      while (pos < next_send->m_actual_buf_len) {
        AttributeHeader header = *(AttributeHeader*)(next_send->m_buf + pos);
        if (header.getAttributeId() == AttributeHeader::VEC_DISTANCE) {
#ifdef DEBUG_VS_INTERP
          double value = *(double*)(next_send->m_buf + pos + 1);
          VS_INTERP_TRACE(m_table_id, m_frag_id,
                          "CopyOneToSignalForSending, attributeId: %d, value: %lf",
                          header.getAttributeId(), value);
#endif  // DEBUG_VS_INTERP
          *(double*)(next_send->m_buf + pos + 1) = next_send->m_distance;
        }
        pos += header.getDataSize() + 1;
      }
    }
    Uint32 copy_len = next_send->m_actual_buf_len;
    if (next_send != nullptr && next_send->m_actual_buf_len != 0) {
      memcpy((void*)(&signal->theData[25]), next_send->m_buf,
          next_send->m_actual_buf_len * sizeof(Uint32));
      VS_INTERP_TRACE(m_table_id, m_frag_id,
                      "CopyOneToSignalForSending, len: %u, idx: %d",
                      next_send->m_actual_buf_len, m_next_send_idx);
      m_vec_n_candidates_sent++;
      m_vec_size_candidates_sent += next_send->m_actual_buf_len;
    }

    m_vec_top_n_results_final[m_next_send_idx] = nullptr;
    m_next_send_idx--;
    /*
     * Don't release the memory here — CandidateAllocator will handle it.
     */
    return copy_len;
  } else {
    return 0;
  }
}

void VecSearchInterpreter::set_vec_max_rec_size(Uint32 size) {
  /*
   * m_vec_max_rec_size is only used to calculate the preallocated memory size
   * for m_candidate_allocator, so it doesn't make sense to set it
   * after m_candidate_allocator has already been initialized.
   */
  if (!m_candidate_allocator) {
    // 10% bigger
    m_vec_max_rec_size = static_cast<int>(std::ceil(size * 1.1));
    VS_INTERP_TRACE(m_table_id, m_frag_id, "Adjust vec_max_rec_size: %u -> %u",
                    size, m_vec_max_rec_size);
  }
}

static inline size_t highest_power_of_two_leq(size_t x) {
  // x > 0
  return size_t(1) << (8 * sizeof(size_t) - 1 - __builtin_clzl(x));
}

Int32 VecSearchInterpreter::CandidateAllocator::Init(Uint32 thread_id) {
  if (m_init) {
    return 0;
  }

  if (m_total_size > g_max_results_size) {
    g_eventLogger->warning(
        "Vector search result size %lu exceeds max pool %u",
        m_total_size, g_max_results_size);
    return ZAGG_VS_TOO_BIG_RESULT;
  }

  if (m_slot_size > g_segment_size) {
    g_eventLogger->error(
        "slot_size %lu > segment_size %u, cannot allocate candidates safely",
        m_slot_size, g_segment_size);
    return ZAGG_VS_TOO_BIG_RESULT;
  }

  size_t raw = g_segment_size / m_slot_size;
  assert(raw > 0);
  m_slots_per_full_segment = highest_power_of_two_leq(raw);
  m_shift_k = __builtin_ctzl(m_slots_per_full_segment);

  size_t full_segments = m_max_candidates / m_slots_per_full_segment;
  size_t last_slots = m_max_candidates % m_slots_per_full_segment;
  size_t last_size = last_slots * m_slot_size;

  [[maybe_unused]] size_t num_segments = full_segments + (last_slots > 0 ? 1 : 0);

  VS_INTERP_TRACE(m_table_id, m_frag_id,
      "Init CandidateAllocator: slot_size=%lu, slots_per_full_seg=%lu, "
      "full_segments=%lu, last_slots=%lu",
      m_slot_size, m_slots_per_full_segment, full_segments, last_slots);

  assert(num_segments <= MAX_CANDIDATE_SEGMENTS);

  for (size_t i = 0; i < full_segments; i++) {
    void* page_ptr = lc_ndbd_pool_malloc(g_segment_size, RG_QUERY_MEMORY,
        thread_id, false);
    if (!page_ptr) {
      g_eventLogger->error("Failed allocating segment %lu", i);
      return ZAGG_ALLOC_MEM_FAILED;
    }
    m_segments[m_n_segments++] = {(char*)page_ptr, g_segment_size};
  }

  if (last_size > 0) {
    void* page_ptr = lc_ndbd_pool_malloc(last_size, RG_QUERY_MEMORY,
        thread_id, false);
    if (!page_ptr) {
      g_eventLogger->error("Failed allocating last segment");
      return ZAGG_ALLOC_MEM_FAILED;
    }
    m_segments[m_n_segments++] = {(char*)page_ptr, last_size};
  }

  m_init = true;
  return 0;
}

VecSearchInterpreter::Candidate* VecSearchInterpreter::CandidateAllocator::Allocate(
    double distance, const Uint32* tuple, Uint32 actual_buf_len) {
  if (m_next_index >= m_max_candidates) {
    return nullptr;
  }

  size_t seg_id   = m_next_index >> m_shift_k;
  size_t slot_off = m_next_index & (m_slots_per_full_segment - 1);

  assert(seg_id < m_n_segments);

  char* base = m_segments[seg_id].ptr;
  char* ptr = base + slot_off * m_slot_size;

  assert(ptr + m_slot_size <= base + m_segments[seg_id].size);

  VS_INTERP_TRACE(m_table_id, m_frag_id,
      "Alloc idx=%lu -> seg=%lu slot_off=%lu",
      m_next_index, seg_id, slot_off);

  Candidate* c = new (ptr) Candidate(distance, actual_buf_len, m_next_index);
  c->Init(tuple);

  if (!m_reuse_started) {
    ++m_next_index;
  }
  if (!m_reuse_started && m_next_index == m_max_candidates) {
    VS_INTERP_TRACE(m_table_id, m_frag_id, "CandidateAllocator reuse started");
    m_next_index = 0;
    m_reuse_started = true;
  }

  return c;
}
