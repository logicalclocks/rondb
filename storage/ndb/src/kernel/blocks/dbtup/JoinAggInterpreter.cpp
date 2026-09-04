/*
 * Copyright (c) 2025, 2026, Hopsworks and/or its affiliates.
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

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <utility>

#define DBTUP_C
#include "signaldata/TransIdAI.hpp"
#include "include/my_byteorder.h"
#include "JoinAggInterpreter.hpp"
#include "InterpreterCommonOp.hpp"
#include "util/require.h"
#include "decimal.h"
#include "Dbtup.hpp"
#include <CteLinkedAttr.hpp>
#include "my_sys.h"
#include "../dblqh/Dblqh.hpp"
#include "../dblqh/JoinAggregationState.hpp"
#include <NdbSqlUtil.hpp>
#include <Interpreter.hpp>

#define JAM_FILE_ID 568
// ATTR_READ_BUF_WORD_SIZE + g_attr_read_buf_len_ /
// g_result_header_size_ / g_result_header_size_per_group_ moved to
// AggInterpreterBase in Step 3a-A.

/*
 * Debug macros
 */
#if (defined(VM_TRACE) || defined(ERROR_INSERT))
#undef DEBUG_PA_INTERP
#define DEBUG_AGG 1
#define DEBUG_CTE 1
#endif
#define DEBUG_PA_INTERP_PART_ID 0

#ifdef DEBUG_CTE
#define DEB_CTE(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_CTE(arglist) do { } while (0)
#endif

#ifdef DEBUG_AGG
#define DEB_AGG(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_AGG(arglist) do { } while (0)
#endif

#ifdef DEBUG_PA_INTERP
#define PA_INTERP_TRACE(part_id, format, ...) \
  do {\
    if ((part_id == DEBUG_PA_INTERP_PART_ID)) {\
      g_eventLogger->info("[PA_INTERP_TRACE] " format, ##__VA_ARGS__); \
    }\
  } while (0)
#else
#define PA_INTERP_TRACE(part_id, format, ...) {}
#endif

/*
 * Numeric / type aggregation kernels (TypeSupported, IsUnsigned,
 * AlignedType, PrintValue, Sum/SumBigint/SumDouble, Max/MaxBigint/MaxDouble,
 * Min/MinBigint/MinDouble, Count) live in the shared base class
 * AggInterpreterBase (AggInterpreterBase.{hpp,cpp}) and are reached here via
 * inherited name lookup.  See agg_interpreter_unification_plan.md, Step 1.
 */

bool JoinAggInterpreter::Init(const Uint32* prog) {
  if (m_inited) {
    return true;
  }
  require(prog != nullptr);

  /* Step 3 Cand-A: peek the program header.  Version mismatch returns
   * true without setting m_inited so ProcessRec rejects on entry. */
  bool compatible = true;
  peekProgramHeader(prog, &compatible);
  if (!compatible) return true;

  /* m_buf_block carve.  m_prog_buf right-sized to m_prog_len,
   * m_gb_cols_buf / m_gb_types right-sized to m_n_gb_cols (skipped
   * when n_gb_cols == 0).  m_agg_results_buf and m_cached_agg_ops
   * stay MAX-sized: setTotalAggResults can override m_n_agg_results
   * upward after Init for multi-leaf queries, and the buffers must
   * still fit.  m_gb_map_buf is always allocated (JoinAgg needs
   * scalar-CTE redistribute paths to land entries even with
   * n_gb_cols == 0). */
  if (m_buf_block == nullptr) {
    /* Tail layout: [Uint16 avg-hidden map, MAX entries] then
     * [Uint8 cached-agg-ops, MAX entries].  The Uint16 array comes
     * first so its 2-byte alignment is inherited from the block. */
    char* tail = initBufBlock(
        /*prog_words=*/m_prog_len,
        /*n_gb_cols_alloc=*/m_n_gb_cols,
        /*n_agg_results_alloc=*/MAX_AGG_N_RESULTS,
        /*alloc_gb_map=*/true,
        /*extra_tail_bytes=*/MAX_AGG_N_RESULTS *
            (sizeof(Uint16) + sizeof(Uint8)));
    if (tail == nullptr) {
      g_eventLogger->error("Alloc mem for JoinAggInterpreter buffers failed");
      return false;
    }
    require((reinterpret_cast<uintptr_t>(tail) & 1) == 0);
    m_avg_hidden_map = reinterpret_cast<Uint16*>(tail);
    m_cached_agg_ops = reinterpret_cast<Uint8*>(
        tail + MAX_AGG_N_RESULTS * sizeof(Uint16));
  }

  /* Common post-allocation steps. */
  initSharedAfterAlloc(prog);

  /* kOpAvg hidden-slot assignment (cte_avg_plan.md).  Walk the agg
   * program once: each kOpAvg's visible dst slot gets a hidden COUNT
   * companion appended AFTER the header's slot count, so visible
   * positions stay stable for every position-indexed consumer.
   * m_n_agg_results becomes the TOTAL (layout / merge / redistribute
   * width); m_n_visible_results (set by initSharedAfterAlloc) keeps
   * the header count for the CTE emission paths. */
  for (Uint32 i = 0; i < MAX_AGG_N_RESULTS; i++) {
    m_avg_hidden_map[i] = AVG_NO_HIDDEN;
  }
  {
    Uint32 n_avg = 0;
    Uint32 scan_pos = m_agg_prog_start_pos;
    bool bad_avg = false;
    while (scan_pos < m_prog_len) {
      Uint32 word = m_prog[scan_pos++];
      Uint8 op = (word & 0xFC000000) >> 26;
      switch (op) {
        case kOpAvg: {
          Uint32 dst = word & 0x0000FFFF;
          if (dst >= m_n_visible_results ||
              m_avg_hidden_map[dst] != AVG_NO_HIDDEN ||
              m_n_visible_results + n_avg >= MAX_AGG_N_RESULTS) {
            bad_avg = true;
            break;
          }
          m_avg_hidden_map[dst] = (Uint16)(m_n_visible_results + n_avg);
          n_avg++;
          break;
        }
        case kOpOrderBy: {
          /* ORDER BY trailer entry (cte_orderby_limit_plan.md). */
          if (m_n_order_cols >= MAX_ORDER_COLS) {
            bad_avg = true;
            break;
          }
          OrderCol& oc = m_order_spec[m_n_order_cols];
          oc.is_agg = (word >> 25) & 1;
          oc.desc = (word >> 24) & 1;
          oc.idx = (Uint16)(word & 0xFFFF);
          if ((oc.is_agg && oc.idx >= m_n_visible_results) ||
              (!oc.is_agg && oc.idx >= m_n_gb_cols)) {
            bad_avg = true;
            break;
          }
          m_n_order_cols++;
          break;
        }
        case kOpLimit:
          /* LIMIT trailer entry.  Last one wins (the API emits one). */
          m_limit = word & 0x03FFFFFF;
          m_has_limit = true;
          break;
        case kOpLoadCol: {
          Uint32 type = decodeLoadColType(word);
          if (type == NDB_TYPE_DECIMAL ||
              type == NDB_TYPE_DECIMALUNSIGNED) scan_pos++;
          break;
        }
        case kOpLoadConst:
          scan_pos += 2;
          break;
        case kOpEmbeddedInterp: {
          Uint32 emb_len = word & 0xFFFF;
          scan_pos += emb_len;
          break;
        }
        default:
          break;
      }
      if (bad_avg) break;
    }
    if (bad_avg) {
      g_eventLogger->error(
          "JoinAggInterpreter::Init: invalid kOpAvg program "
          "(dst out of range, duplicate dst, or slot cap exceeded)");
      return false;
    }
    if (n_avg > 0) {
      m_n_hidden_slots = n_avg;
      /* Extend the slot array with the hidden companions, initialised
       * like initSharedAfterAlloc does for the visible ones.  The
       * buffers are MAX-sized, so no reallocation. */
      for (Uint32 i = m_n_agg_results;
           i < m_n_agg_results + n_avg; i++) {
        m_agg_results[i].type = NDB_TYPE_UNDEFINED;
        m_agg_results[i].value.val_int64 = 0;
        m_agg_results[i].is_unsigned = false;
        m_agg_results[i].is_null = true;
      }
      m_n_agg_results += n_avg;
    }
  }

  /* Phase I.17: scalar aggregate (no GROUP BY) over empty input
   * must emit COUNT = 0 (not NULL) per MySQL semantics.  The
   * Count() handler at JoinAggInterpreter.cpp:522 lazy-initialises
   * a COUNT slot on the first row, which never runs on empty
   * input.  Pre-initialise every COUNT slot here so the scalar
   * emit path in Dblqh::cteScanEmitResults sees value=0,
   * is_null=false even when no rows were processed.  SUM / MIN /
   * MAX slots stay is_null=true to surface NULL on empty input.
   *
   * The walk below is intentionally targeted at kOpCount only —
   * cheaper than the full extractAggOps cache and runs once per
   * Init.  Other opcodes' length encoding mirrors extractAggOps. */
  if (m_n_gb_cols == 0 && m_n_agg_results > 0) {
    Uint32 scan_pos = m_agg_prog_start_pos;
    while (scan_pos < m_prog_len) {
      Uint32 word = m_prog[scan_pos++];
      Uint8 op = (word & 0xFC000000) >> 26;
      switch (op) {
        case kOpCount: {
          Uint32 agg_index = word & 0x0000FFFF;
          if (agg_index < m_n_agg_results) {
            m_agg_results[agg_index].type = NDB_TYPE_BIGINT;
            m_agg_results[agg_index].value.val_uint64 = 0;
            m_agg_results[agg_index].is_unsigned = true;
            m_agg_results[agg_index].is_null = false;
          }
          break;
        }
        case kOpAvg: {
          /* Scalar AVG over empty input: pre-init the hidden COUNT
           * companion to 0 so the finalize divide sees count == 0 and
           * yields NULL (MySQL AVG-over-empty).  The visible SUM slot
           * stays NULL like other SUM slots. */
          Uint32 dst = word & 0x0000FFFF;
          if (dst < m_n_visible_results &&
              m_avg_hidden_map[dst] != AVG_NO_HIDDEN) {
            AggResItem* c = &m_agg_results[m_avg_hidden_map[dst]];
            c->type = NDB_TYPE_BIGINT;
            c->value.val_uint64 = 0;
            c->is_unsigned = true;
            c->is_null = false;
          }
          break;
        }
        case kOpLoadCol: {
          Uint32 type = decodeLoadColType(word);
          if (type == NDB_TYPE_DECIMAL ||
              type == NDB_TYPE_DECIMALUNSIGNED) scan_pos++;
          break;
        }
        case kOpLoadConst:
          scan_pos += 2;
          break;
        case kOpEmbeddedInterp: {
          Uint32 emb_len = word & 0xFFFF;
          scan_pos += emb_len;
          break;
        }
        default:
          break;
      }
    }
  }

  /* Validate embedded interpreter blocks (Step 3b — shared helper). */
  if (!scanAndValidateEmbeddedPrograms("JoinAggInterpreter")) {
    return false;
  }
  return true;
}

/**
 * setTotalAggResults — override m_n_agg_results for multi-leaf.
 *
 * Must be called after Init() but before any rows are processed.
 * Sets the total accumulator count across all leaves so hash map entries
 * and the non-GROUP-BY accumulator array are sized for the full combined
 * layout. Also re-initializes the non-GROUP-BY accumulator slots.
 */
void JoinAggInterpreter::setTotalAggResults(Uint32 total) {
  require(m_inited);
  require(m_processed_rows == 0);
  require(total <= MAX_AGG_N_RESULTS);
  /* Multi-leaf and kOpAvg hidden slots do not combine (multi-leaf is
   * the merged select-list-subquery path; CTE programs are single-leaf).
   * The override would clobber the hidden-slot layout. */
  require(m_n_hidden_slots == 0);
  m_n_agg_results = total;
  m_n_visible_results = total;

  // Re-initialize the non-GROUP-BY accumulator array for the new total
  m_agg_results = m_agg_results_buf;
  for (Uint32 i = 0; i < m_n_agg_results; i++) {
    m_agg_results[i].type = NDB_TYPE_UNDEFINED;
    m_agg_results[i].value.val_int64 = 0;
    m_agg_results[i].is_unsigned = false;
    m_agg_results[i].is_null = true;
  }
}

/* Divide one slot array's AVG pairs in place (cte_avg_plan.md).
 * Sum slots accumulate as BIGINT (signed/unsigned) or DOUBLE; the
 * result is always DOUBLE.  count == 0 / NULL, or a NULL sum, yields
 * NULL — MySQL's AVG over empty or all-NULL input. */
static void finalizeAvgSlotArray(AggResItem* slots,
                                 const Uint16* avg_hidden_map,
                                 Uint32 n_visible_results) {
  for (Uint32 dst = 0; dst < n_visible_results; dst++) {
    const Uint16 hid = avg_hidden_map[dst];
    if (hid == AggInterpreterBase::AVG_NO_HIDDEN) continue;
    const AggResItem& cnt = slots[hid];
    AggResItem* sum = &slots[dst];
    double result = 0.0;
    bool is_null = true;
    if (!cnt.is_null && cnt.value.val_uint64 > 0 && !sum->is_null) {
      double s;
      if (sum->type == NDB_TYPE_DOUBLE) {
        s = sum->value.val_double;
      } else if (sum->is_unsigned) {
        s = static_cast<double>(sum->value.val_uint64);
      } else {
        s = static_cast<double>(sum->value.val_int64);
      }
      result = s / static_cast<double>(cnt.value.val_uint64);
      is_null = false;
    }
    sum->type = NDB_TYPE_DOUBLE;
    sum->value.val_double = result;
    sum->is_unsigned = false;
    sum->is_null = is_null;
  }
}

bool JoinAggInterpreter::finalizeAvgSlotsSlice(Uint32 max_groups) {
  if (m_n_hidden_slots == 0 || m_avg_finalized) {
    return true;
  }

  if (m_n_gb_cols == 0) {
    /* Scalar: a single record — no slicing needed. */
    if (m_agg_results != nullptr) {
      finalizeAvgSlotArray(m_agg_results, m_avg_hidden_map,
                           m_n_visible_results);
    }
    m_avg_finalized = true;
    m_avg_finalizing = false;
    return true;
  }

  if (m_gb_map == nullptr) {
    m_avg_finalized = true;
    m_avg_finalizing = false;
    return true;
  }

  /* Resume from the saved cursor, or start from the beginning.  The
   * hash table is immutable for the whole FINAL_REP..CTE_READY window,
   * so iteratorAt is safe across CONTINUEB slices. */
  JoinGBHashTable::Iterator it;
  if (!m_avg_finalizing) {
    m_avg_finalizing = true;
    it = m_gb_map->begin();
  } else {
    it = m_gb_map->iteratorAt(m_avg_fin_bucket, m_avg_fin_raw);
  }

  Uint32 processed = 0;
  while (it.valid()) {
    if (processed >= max_groups) {
      /* Save the cursor; the caller schedules the next slice. */
      m_avg_fin_bucket = it.bucket();
      m_avg_fin_raw = it.raw();
      return false;
    }
    AggResItem* slots =
        reinterpret_cast<AggResItem*>(it.data() + it.keyLen());
    finalizeAvgSlotArray(slots, m_avg_hidden_map, m_n_visible_results);
    processed++;
    m_gb_map->next(it);
  }

  m_avg_finalized = true;
  m_avg_finalizing = false;
  return true;
}

/* Compare one GROUP BY key entry per side.  Entries are
 * AttributeHeader-framed ([AH word][data words]); walk to entry
 * `col_idx` on each side independently (VARCHAR entries differ in
 * size between groups). */
static const Uint32* keyEntryAt(const char* data, Uint32 key_len,
                                Uint32 col_idx) {
  const Uint32* p = reinterpret_cast<const Uint32*>(data);
  const Uint32* end = p + (key_len >> 2);
  Uint32 i = 0;
  while (p < end) {
    if (i == col_idx) return p;
    p += 1 + AttributeHeader::getDataSize(*p);
    i++;
  }
  return nullptr;
}

int JoinAggInterpreter::compareGroupsByOrderSpec(
    const char* a_data, Uint32 a_key_len,
    const char* b_data, Uint32 b_key_len) const {
  for (Uint32 c = 0; c < m_n_order_cols; c++) {
    const OrderCol& oc = m_order_spec[c];
    int r = 0;
    if (!oc.is_agg) {
      const Uint32* ea = keyEntryAt(a_data, a_key_len, oc.idx);
      const Uint32* eb = keyEntryAt(b_data, b_key_len, oc.idx);
      if (ea == nullptr || eb == nullptr) continue;  /* defensive */
      const AttributeHeader aha(*ea);
      const AttributeHeader ahb(*eb);
      const bool na = aha.isNULL();
      const bool nb = ahb.isNULL();
      if (na || nb) {
        /* MySQL: NULLs order first ascending. */
        r = (na && nb) ? 0 : (na ? -1 : 1);
      } else if (m_gb_types_inited && oc.idx < m_n_gb_cols &&
                 m_gb_types[oc.idx].cmpFn != nullptr) {
        r = (*m_gb_types[oc.idx].cmpFn)(
            m_gb_types[oc.idx].cs,
            ea + 1, aha.getByteSize(),
            eb + 1, ahb.getByteSize());
      } else {
        /* Types not initialised (no rows processed) — nothing to
         * order; treat as equal. */
        r = 0;
      }
    } else {
      const AggResItem* sa = reinterpret_cast<const AggResItem*>(
          a_data + a_key_len) + oc.idx;
      const AggResItem* sb = reinterpret_cast<const AggResItem*>(
          b_data + b_key_len) + oc.idx;
      const bool na = sa->is_null || sa->type == NDB_TYPE_UNDEFINED;
      const bool nb = sb->is_null || sb->type == NDB_TYPE_UNDEFINED;
      if (na || nb) {
        r = (na && nb) ? 0 : (na ? -1 : 1);
      } else if (sa->type == NDB_TYPE_DOUBLE ||
                 sb->type == NDB_TYPE_DOUBLE) {
        const double va = (sa->type == NDB_TYPE_DOUBLE)
            ? sa->value.val_double
            : (sa->is_unsigned
                   ? static_cast<double>(sa->value.val_uint64)
                   : static_cast<double>(sa->value.val_int64));
        const double vb = (sb->type == NDB_TYPE_DOUBLE)
            ? sb->value.val_double
            : (sb->is_unsigned
                   ? static_cast<double>(sb->value.val_uint64)
                   : static_cast<double>(sb->value.val_int64));
        r = (va < vb) ? -1 : (va > vb) ? 1 : 0;
      } else if (sa->is_unsigned == sb->is_unsigned) {
        if (sa->is_unsigned) {
          r = (sa->value.val_uint64 < sb->value.val_uint64) ? -1
              : (sa->value.val_uint64 > sb->value.val_uint64) ? 1 : 0;
        } else {
          r = (sa->value.val_int64 < sb->value.val_int64) ? -1
              : (sa->value.val_int64 > sb->value.val_int64) ? 1 : 0;
        }
      } else {
        /* Mixed signedness (SUM can flip per group): a negative
         * signed side orders below any unsigned value. */
        const AggResItem* su = sa->is_unsigned ? sa : sb;
        const AggResItem* si = sa->is_unsigned ? sb : sa;
        int ru;
        if (si->value.val_int64 < 0) {
          ru = 1;  /* unsigned > negative signed */
        } else {
          const Uint64 ui = static_cast<Uint64>(si->value.val_int64);
          ru = (su->value.val_uint64 > ui) ? 1
               : (su->value.val_uint64 < ui) ? -1 : 0;
        }
        r = (su == sa) ? ru : -ru;
      }
    }
    if (oc.desc) r = -r;
    if (r != 0) return r;
  }
  return 0;
}

/* Bounded candidate heap for the LIMIT select phase: the WORST kept
 * group sits at the root, so a better-ordering arrival replaces the
 * root and sifts down.  "a worse than b" == compare(a, b) > 0. */
void JoinAggInterpreter::limitHeapSiftDown(Uint32 i) {
  const Uint32 n = m_lim_cand_count;
  while (true) {
    Uint32 largest = i;
    const Uint32 l = 2 * i + 1;
    const Uint32 rgt = 2 * i + 2;
    /* keyLen per entry: recompute from the record's link header — the
     * candidate pointers are group DATA pointers; key length is
     * stored in the group link header (GBHashTable layout). */
    if (l < n &&
        compareGroupsByOrderSpec(
            m_lim_cand[l], JoinGBHashTable::dataKeyLen(m_lim_cand[l]),
            m_lim_cand[largest],
            JoinGBHashTable::dataKeyLen(m_lim_cand[largest])) > 0) {
      largest = l;
    }
    if (rgt < n &&
        compareGroupsByOrderSpec(
            m_lim_cand[rgt], JoinGBHashTable::dataKeyLen(m_lim_cand[rgt]),
            m_lim_cand[largest],
            JoinGBHashTable::dataKeyLen(m_lim_cand[largest])) > 0) {
      largest = rgt;
    }
    if (largest == i) return;
    char* tmp = m_lim_cand[i];
    m_lim_cand[i] = m_lim_cand[largest];
    m_lim_cand[largest] = tmp;
    i = largest;
  }
}

static int cmpPtr(const void* a, const void* b) {
  const char* pa = *static_cast<char* const*>(a);
  const char* pb = *static_cast<char* const*>(b);
  return (pa < pb) ? -1 : (pa > pb) ? 1 : 0;
}

int JoinAggInterpreter::finalizeLimitSlice(Uint32 max_groups,
                                           Uint32 thread_id) {
  if (!m_has_limit || m_limit_finalized) {
    return 1;
  }
  if (m_n_gb_cols == 0 || m_gb_map == nullptr) {
    /* Scalar / keyless states: RonSQL only emits LIMIT trailers for
     * grouped CTEs; nothing to truncate. */
    m_limit_finalized = true;
    m_limit_finalizing = false;
    return 1;
  }
  if (!m_limit_finalizing) {
    if (m_limit >= m_gb_map->size()) {
      /* Limit covers every group — no work. */
      m_limit_finalized = true;
      return 1;
    }
    m_limit_finalizing = true;
    m_lim_select_done = false;
    m_lim_cand_count = 0;
    if (m_limit > 0) {
      m_lim_cand = static_cast<char**>(lc_ndbd_pool_malloc(
          sizeof(char*) * m_limit, RG_QUERY_MEMORY, thread_id, false));
      if (m_lim_cand == nullptr) {
        m_limit_finalizing = false;
        return -1;
      }
    }
    m_lim_bucket = 0;
    m_lim_raw = nullptr;
  }

  Uint32 processed = 0;

  if (!m_lim_select_done) {
    /* Phase 1: bounded top-N selection over a sliced walk. */
    JoinGBHashTable::Iterator it;
    if (m_lim_raw == nullptr) {
      it = m_gb_map->begin();
    } else {
      it = m_gb_map->iteratorAt(m_lim_bucket, m_lim_raw);
    }
    while (it.valid()) {
      if (processed >= max_groups) {
        m_lim_bucket = it.bucket();
        m_lim_raw = it.raw();
        return 0;
      }
      char* data = it.data();
      const Uint32 klen = it.keyLen();
      if (m_limit > 0) {
        if (m_lim_cand_count < m_limit) {
          m_lim_cand[m_lim_cand_count++] = data;
          if (m_lim_cand_count == m_limit) {
            /* Heapify once full (small N: sift each from the middle). */
            for (Int32 i = (Int32)(m_lim_cand_count / 2) - 1; i >= 0; i--) {
              limitHeapSiftDown((Uint32)i);
            }
          }
        } else if (compareGroupsByOrderSpec(
                       data, klen, m_lim_cand[0],
                       JoinGBHashTable::dataKeyLen(m_lim_cand[0])) < 0) {
          /* Better than the worst kept — replace the root. */
          m_lim_cand[0] = data;
          limitHeapSiftDown(0);
        }
      }
      processed++;
      m_gb_map->next(it);
    }
    /* Selection complete: sort candidates by pointer for the
     * membership test in the truncation walk. */
    if (m_lim_cand_count > 1) {
      qsort(m_lim_cand, m_lim_cand_count, sizeof(char*), cmpPtr);
    }
    m_lim_select_done = true;
    m_lim_bucket = 0;
    m_lim_raw = nullptr;
    /* Fall through into the truncation phase with the remaining
     * slice budget. */
  }

  /* Phase 2: erase every group not in the kept set. */
  {
    JoinGBHashTable::Iterator it;
    if (m_lim_raw == nullptr) {
      it = m_gb_map->begin();
    } else {
      it = m_gb_map->iteratorAt(m_lim_bucket, m_lim_raw);
    }
    while (it.valid()) {
      if (processed >= max_groups) {
        m_lim_bucket = it.bucket();
        m_lim_raw = it.raw();
        return 0;
      }
      char* data = it.data();
      bool kept = false;
      if (m_lim_cand_count > 0) {
        kept = (bsearch(&data, m_lim_cand, m_lim_cand_count,
                        sizeof(char*), cmpPtr) != nullptr);
      }
      processed++;
      if (kept) {
        m_gb_map->next(it);
      } else {
        AggResItem* slots =
            reinterpret_cast<AggResItem*>(data + it.keyLen());
        if (hasStringSlots()) {
          freeGroupStringSlots(slots);
        }
        m_gb_map->eraseAndNext(it);
        freeGroupData(data);
        if (m_n_groups > 0) m_n_groups--;
      }
    }
  }

  if (m_lim_cand != nullptr) {
    lc_ndbd_pool_free(m_lim_cand);
    m_lim_cand = nullptr;
  }
  m_limit_finalized = true;
  m_limit_finalizing = false;
  return 1;
}

/**
 * switchProgram — swap the active aggregation program for multi-leaf.
 *
 * Points to a different leaf's program and sets the accumulator offset.
 * The hash map, group rows, and all other interpreter state are unchanged.
 * Called before each processRecWithLinkedAttrs() to select the correct
 * leaf's program.
 *
 * @param prog             Pointer to the leaf's program words (must remain
 *                         valid for the lifetime of the interpreter)
 * @param prog_len         Program length in words
 * @param agg_prog_start   Instruction start offset within the program
 * @param acc_offset       Accumulator offset for this leaf (0 for leaf 0)
 */
// switchProgram removed — leaf program switching is now done inside
// processRecWithLinkedAttrs / processNullExtendedRow under mutex.

/**
 * cacheMultiLeafAggOps — pre-build combined agg_ops for multi-leaf merge.
 *
 * For MUTEX_FREE merge, mergeFrom() needs to know the aggregation opcode
 * for each accumulator slot. With multi-leaf, different slots belong to
 * different leaf programs. This method extracts ops from ALL leaf programs
 * into the combined m_cached_agg_ops array, with each leaf's ops placed
 * at its acc_offset.
 *
 * Must be called after Init() + setTotalAggResults(), before any merge.
 */
void JoinAggInterpreter::cacheMultiLeafAggOps(const LeafProgram* leaves,
                                               Uint32 num_leaves) {
  require(m_inited);
  require(m_cached_agg_ops != nullptr);
  memset(m_cached_agg_ops, 0, m_n_agg_results);

  for (Uint32 leaf = 0; leaf < num_leaves; leaf++) {
    // Extract ops from this leaf's program, writing at leaf's offset
    Uint32 exec_pos = leaves[leaf].m_agg_prog_start_pos;
    const Uint32* prog = leaves[leaf].m_agg_program;
    Uint32 prog_len = leaves[leaf].m_agg_program_len;
    Uint32 acc_offset = leaves[leaf].m_acc_offset;

    while (exec_pos < prog_len) {
      Uint32 value = prog[exec_pos++];
      Uint8 op = (value & 0xFC000000) >> 26;
      Uint32 agg_index;
      switch (op) {
        case kOpSum: case kOpSumBigint: case kOpSumDouble:
        case kOpMax: case kOpMaxBigint: case kOpMaxDouble:
        case kOpMin: case kOpMinBigint: case kOpMinDouble:
        case kOpCount:
          agg_index = (value & 0x0000FFFF) + acc_offset;
          if (agg_index < m_n_agg_results) m_cached_agg_ops[agg_index] = op;
          break;
        case kOpLoadCol: {
          Uint32 type = decodeLoadColType(value);
          if (type == NDB_TYPE_DECIMAL || type == NDB_TYPE_DECIMALUNSIGNED)
            exec_pos++;
          break;
        }
        case kOpLoadConst:
          exec_pos += 2;
          break;
        case kOpEmbeddedInterp: {
          Uint32 emb_len = value & 0xFFFF;
          exec_pos += emb_len;
          break;
        }
        case kOpSkip:
        case kOpSetRegNull:
          break;
        default:
          break;
      }
    }
  }
  m_agg_ops_cached = true;
}

/*
 * ProcessRec for join aggregation — includes linked attribute resolution
 */
Int32 JoinAggInterpreter::ProcessRec(Dbtup* block_tup,
        Dbtup::KeyReqStruct* req_struct,
        Uint32 thread_id,
        EmulatedJamBuffer *jamBuf,
        uchar* xfrm_buf,
        Uint32 xfrm_buf_len,
        Uint32* attr_read_buf) {
  m_current_thread_id = thread_id;
  /*
   * Normal row processing binds m_attr_read_buf from DBTUP.  Null-extended
   * rows have no tuple to read and pass the per-LDM scratch buffer directly.
   */
  if (attr_read_buf != nullptr) {
    m_attr_read_buf = attr_read_buf;
  } else {
    require(block_tup != nullptr);
    m_attr_read_buf = block_tup->getAggAttrReadBuf();
  }
  if (!m_inited) {
    g_eventLogger->debug("AggInterpreter::ProcessRec ZAGG_OTHER_ERROR: not inited");
    return ZAGG_OTHER_ERROR;
  }
  if (!m_null_local_columns) {
    thrjam(jamBuf);
    if (req_struct->read_length != 0) {
      g_eventLogger->debug("AggInterpreter::ProcessRec ZAGG_OTHER_ERROR at entry: "
              "read_length=%u", req_struct->read_length);
      return ZAGG_OTHER_ERROR;
    }
  }

  AggResItem* agg_res_ptr = nullptr;
  if (m_n_gb_cols) {
    if (!m_gb_types_inited) {
      if (m_null_local_columns) {
        thrjam(jamBuf);
        Int32 err = initGBTypesForNullLocal(jamBuf);
        if (unlikely(err != 0)) return err;
      } else {
        thrjam(jamBuf);
        Int32 err = initGBTypes(block_tup,
                                req_struct,
                                m_linked_attr_data,
                                m_linked_attr_len,
                                /*requireMetadata=*/true,
                                jamBuf);
        if (unlikely(err != 0)) return err;
      }
    }
    char* agg_rec = nullptr;

    AttributeHeader* header = nullptr;
    m_attr_read_pos = 0;
    for (Uint32 i = 0; i < m_n_gb_cols; i++) {
      thrjamDebug(jamBuf);
      Uint32 attr_id = m_gb_cols[i] >> 16;
      if ((attr_id & 0x8000) != 0) {
        thrjamDebug(jamBuf);
        /* Linked GROUP BY column — must have a linked-attr buffer.
         * If the attr_id has the linked flag set but
         * m_linked_attr_data is null, the API caller didn't
         * addLinkedProjection() for the position the aggregator
         * references — fail cleanly rather than falling through to
         * the local-column path, which would read tabDescriptor at
         * attr_id=0x8000+pos and crash. */
        if (unlikely(m_linked_attr_data == nullptr)) {
          g_eventLogger->debug(
              "JoinAggInterpreter::ProcessRec: linked GB col %u "
              "(attr_id=0x%x) but m_linked_attr_data is NULL — "
              "API likely missing addLinkedProjection for the "
              "position", i, attr_id);
          return ZAGG_OTHER_ERROR;
        }
        Uint32 position = attr_id & 0x7FFF;
        thrjamDataDebug(jamBuf, position);
        thrjamDataDebug(jamBuf, m_linked_attr_len);
        const Uint32* p = m_linked_attr_data;
        const Uint32* p_end = m_linked_attr_data + m_linked_attr_len;
        Uint32 pos_count = 0;
        while (p < p_end) {
          thrjamDebug(jamBuf);
          if (pos_count == position) break;
          p += 2;
          Uint32 data_size = AttributeHeader::getDataSize(*p);
          p += (1 + data_size);
          pos_count++;
          thrjamDataDebug(jamBuf, data_size);
        }
        if (p >= p_end) {
          g_eventLogger->debug("JoinAggInterpreter::ProcessRec ZAGG_OTHER_ERROR: "
              "Linked GROUP BY position %u not found in linked buffer "
              "(linked_len=%u)", position, m_linked_attr_len);
          return ZAGG_OTHER_ERROR;
        }
        p += 2;
        Uint32 words = 1 + AttributeHeader::getDataSize(*p);
        memcpy(m_attr_read_buf + m_attr_read_pos, p, words * sizeof(Uint32));
        header = reinterpret_cast<AttributeHeader*>(m_attr_read_buf + m_attr_read_pos);
        m_attr_read_pos += words;
      } else {
        if (m_null_local_columns) {
          thrjam(jamBuf);
          AttributeHeader null_ah(attr_id, 0);
          m_attr_read_buf[m_attr_read_pos] = null_ah.m_value;
          header = reinterpret_cast<AttributeHeader*>(
              m_attr_read_buf + m_attr_read_pos);
          m_attr_read_pos += 1;
        } else {
          thrjam(jamBuf);
          /* Normal (non-linked) GROUP BY column.  Only a real scanned-table
           * request supplies a valid tablePtrP; a CTE agg feed sets it to
           * nullptr.  Abort rather than read a table column that does not
           * exist in this context (see initGBTypes for the rationale). */
          if (unlikely(req_struct == nullptr ||
                       req_struct->tablePtrP == nullptr)) {
            g_eventLogger->debug(
                "JoinAggInterpreter::ProcessRec: normal GROUP BY column %u "
                "referenced in a CTE agg-feed with no scanned table — "
                "aborting query", m_gb_cols[i] >> 16);
            return ZAGG_OTHER_ERROR;
          }
          int ret = block_tup->readSingleAttribute(
              req_struct, m_gb_cols[i] >> 16,
              m_attr_read_buf + m_attr_read_pos,
              g_attr_read_buf_len_ - m_attr_read_pos);
          if (ret < 0) {
            DEB_AGG(("read group by column error: %d", ret));
            return -ret;
          }
          header = reinterpret_cast<AttributeHeader*>(m_attr_read_buf + m_attr_read_pos);
          m_attr_read_pos += Uint32(ret);
        }
      }
    }

    /* CTE mode: normalize each GB column's AttributeHeader attrId to
     * its column position (0..N-1) so stored keys match the virtual
     * CTE attrIds that DBSPJ uses to build CTE_LOOKUP keys. Preserves
     * byteSize/flags in the low 16 bits; only the attrId bits change. */
    if (m_cte_mode) {
      Uint32* p = m_attr_read_buf;
      Uint32* end = m_attr_read_buf + m_attr_read_pos;
      for (Uint32 i = 0; i < m_n_gb_cols && p < end; i++) {
        AttributeHeader ah(*p);
        *p = (i << 16) | (*p & 0x0000FFFF);
        p += 1 + ah.getDataSize();
      }
    }

    Uint32 len_in_char = m_attr_read_pos * sizeof(Uint32);
    char* found = m_gb_map->find(reinterpret_cast<char*>(m_attr_read_buf),
                                 len_in_char, xfrm_buf, xfrm_buf_len);
    if (found != nullptr) {
      header = reinterpret_cast<AttributeHeader*>(found);
      agg_res_ptr = reinterpret_cast<AggResItem*>(found + len_in_char);
      agg_res_ptr += m_acc_offset;
    } else {
      if (m_max_groups > 0 && m_n_groups >= m_max_groups) {
        return AGG_EVICT_NEEDED;
      }
      if (req_struct != nullptr) {
        req_struct->read_length = (len_in_char +
                         m_n_agg_results * sizeof(AggResItem)) / sizeof(Int32);
      }

      m_result_size += len_in_char +
                       m_n_agg_results * sizeof(AggResItem);
      agg_rec = allocGroupData(len_in_char +
                               m_n_agg_results * sizeof(AggResItem),
                               len_in_char);
      if (agg_rec == nullptr) {
        return AGG_EVICT_NEEDED;
      }
      memset(agg_rec, 0, len_in_char +
                        m_n_agg_results * sizeof(AggResItem));
      memcpy(agg_rec, reinterpret_cast<char*>(m_attr_read_buf), len_in_char);

      m_gb_map->insert(agg_rec, len_in_char, xfrm_buf, xfrm_buf_len);
      m_n_groups = m_gb_map->size();
      agg_res_ptr = reinterpret_cast<AggResItem*>(agg_rec + len_in_char);

      assert(m_n_agg_results <= MAX_AGG_N_RESULTS);
      for (Uint32 i = 0; i < m_n_agg_results; i++) {
        agg_res_ptr[i].type = NDB_TYPE_UNDEFINED;
        agg_res_ptr[i].value.val_int64 = 0;
        agg_res_ptr[i].is_unsigned = false;
        agg_res_ptr[i].is_null = true;
      }
      agg_res_ptr += m_acc_offset;
    }
  } else {
    agg_res_ptr = m_agg_results + m_acc_offset;
  }

  Uint32 value;
  DataType type;
  bool is_unsigned;
  Uint32 reg_index;
  /* reg_index2 / agg_index used only by shared opcodes — moved to base
   * helper in Step 1.4. */
  const Uint32* attrDescriptor = nullptr;
  Uint32 linked_word0 = 0;
  Uint32 linked_word1 = 0;
  bool linked_cte_attr = false;
  /* decimal_info / precision / scale / dec_ret / dec_val_dbl /
   * dec_val_ll / dec_val_ull moved into
   * AggInterpreterBase::loadColumnTypedFromBuf in Step 3 Cand-B. */

  Uint32 exec_pos = m_agg_prog_start_pos;
  bool debug_print = (m_frag_id == DEBUG_PA_INTERP_PART_ID);
  while (exec_pos < m_prog_len) {
    const Uint32 load_program_offset =
        ((m_acc_offset & 0xFFFF) << 16) | (exec_pos & 0xFFFF);
    value = m_prog[exec_pos++];
    Uint8 op = (value & 0xFC000000) >> 26;
    int ret = 0;
    m_attr_read_pos = 0;
    AttributeHeader* header = nullptr;

    switch (op) {
      case kOpLoadCol: {
        type = decodeLoadColType(value);
        is_unsigned = IsUnsigned(type);
        reg_index = (value & 0x000F0000) >> 16;
        linked_word0 = 0;
        linked_word1 = 0;
        linked_cte_attr = false;
        Uint32 col_id_raw = value & 0x0000FFFF;
        if ((col_id_raw & 0x8000) != 0 && m_linked_attr_data != nullptr) {
          Uint32 position = col_id_raw & 0x7FFF;
          const Uint32* p = m_linked_attr_data;
          const Uint32* p_end = m_linked_attr_data + m_linked_attr_len;
          Uint32 pos_count = 0;
          while (p < p_end) {
            if (pos_count == position) break;
            p += 2;
            p += 1 + AttributeHeader::getDataSize(*p);
            pos_count++;
          }
          if (p + 2 >= p_end) {
            g_eventLogger->debug("JoinAggInterpreter::ProcessRec ZAGG_OTHER_ERROR: "
                "kOpLoadCol linked position %u not found in buffer "
                "(linked_len=%u)", position, m_linked_attr_len);
            return ZAGG_OTHER_ERROR;
          }
          linked_word0 = p[0];
          linked_word1 = p[1];
          linked_cte_attr = CteLinkedAttr::isCteMarker(linked_word0);
          if (!linked_cte_attr) {
            const Uint32 linked_attr_id =
                AttributeHeader(p[2]).getAttributeId();
            const ColumnMeta *column_meta =
                findColumnMeta(linked_word0, linked_word1, linked_attr_id);
            if (column_meta != nullptr) {
              linked_word0 = CteLinkedAttr::encodeWord0(column_meta->typeId,
                                                        column_meta->maxBytes);
              linked_word1 = CteLinkedAttr::encodeWord1(column_meta->csNumber);
              linked_cte_attr = true;
            } else {
              const LoadColumnMeta *meta =
                  findLoadColumnMeta(load_program_offset);
              if (meta != nullptr) {
                linked_word0 = CteLinkedAttr::encodeWord0(meta->typeId,
                                                          meta->maxBytes);
                linked_word1 = CteLinkedAttr::encodeWord1(meta->csNumber);
                linked_cte_attr = true;
              }
            }
            if (!linked_cte_attr) {
              return ZAGG_OTHER_ERROR;
            }
          }
          p += 2;
          Uint32 words = 1 + AttributeHeader::getDataSize(*p);
          memcpy(m_attr_read_buf + m_attr_read_pos, p, words * sizeof(Uint32));
          header = reinterpret_cast<AttributeHeader*>(m_attr_read_buf + m_attr_read_pos);
          attrDescriptor = nullptr;
        } else if (m_null_local_columns) {
          AttributeHeader null_ah(col_id_raw, 0);
          m_attr_read_buf[m_attr_read_pos] = null_ah.m_value;
          header = reinterpret_cast<AttributeHeader*>(
              m_attr_read_buf + m_attr_read_pos);
          attrDescriptor = nullptr;
        } else {
          /* Normal (non-linked) column load.  The value still comes from the
           * scanned tuple, but type/charset metadata must come from the
           * JOIN_AGG_SETUP_REQ metadata cache. */
          const LoadColumnMeta *meta =
              findLoadColumnMeta(load_program_offset);
          if (unlikely(meta == nullptr)) {
            return ZAGG_OTHER_ERROR;
          }
          linked_word0 = CteLinkedAttr::encodeWord0(meta->typeId,
                                                    meta->maxBytes);
          linked_word1 = CteLinkedAttr::encodeWord1(meta->csNumber);
          linked_cte_attr = true;
          if (unlikely(req_struct == nullptr ||
                       req_struct->tablePtrP == nullptr)) {
            return ZAGG_OTHER_ERROR;
          }
          ret = block_tup->readSingleAttribute(
              req_struct, col_id_raw,
              m_attr_read_buf + m_attr_read_pos,
              g_attr_read_buf_len_ - m_attr_read_pos);
          if (ret < 0) {
            DEB_AGG(("read column error: %d", ret));
            return -ret;
          }
          header = reinterpret_cast<AttributeHeader*>(m_attr_read_buf + m_attr_read_pos);
          attrDescriptor = nullptr;
          assert(header->getAttributeId() == col_id_raw);
        }
        Int32 lret = loadColumnTypedFromBuf(
            type, is_unsigned, reg_index, header, attrDescriptor,
            linked_cte_attr, linked_word0, linked_word1,
            req_struct, exec_pos, "JoinAggInterpreter");
        if (lret != 0) return lret;
        break;
      }
      case kOpEmbeddedInterp:
      {
        Uint32 emb_len = value & 0xFFFF;
        if (exec_pos + emb_len > m_prog_len) return ZAGG_OTHER_ERROR;

        if (m_null_local_columns) {
          /*
           * Null-extended row: can't run embedded interpreter without
           * req_struct. Skip the embedded program (take THEN path).
           * All local column reads return NULL, so THEN-path aggregations
           * will correctly handle NULL inputs (SUM/MIN/MAX skip NULLs).
           */
          exec_pos += emb_len;
          break;
        }

        Uint32 saved_instr_count = req_struct->no_exec_instructions;
        req_struct->no_exec_instructions = 0;

        // Make linked attr data available to the NDB interpreter for
        // READ_LINKED_TO_MEM / BRANCH_MEM_OP_ARG instructions.
        req_struct->m_linked_attr_data = m_linked_attr_data;
        req_struct->m_linked_attr_len = m_linked_attr_len;

        Uint32 local_tmpArea[16];
        int rc = block_tup->interpreterAggEmbedded(
            req_struct->signal, req_struct,
            &m_prog[exec_pos], emb_len,
            local_tmpArea, 16,
            m_registers);

        req_struct->no_exec_instructions = saved_instr_count;
        req_struct->m_linked_attr_data = nullptr;
        req_struct->m_linked_attr_len = 0;

        if (rc < 0) return ZAGG_EMBEDDED_INTERP_ERROR;

        Uint32 skip_offset = block_tup->c_interpreter_output[0];
        if (skip_offset == AGG_EMBEDDED_INTERP_STOP_PROGRAM) {
          exec_pos = m_prog_len;
        } else {
          exec_pos += emb_len + skip_offset;
        }
        break;
      }

      default: {
        /* Step 1.4: arithmetic / aggregate / mov / setnull / skip /
         * loadconst are handled by the shared base helper.  Per-class
         * arms above stay for kOpLoadCol (linked-attr / CTE / NULL
         * injection) and kOpEmbeddedInterp (req_struct linked-attr
         * setup). */
        bool op_handled = false;
        Int32 op_ret = executeStandardOpcode(op, value, exec_pos,
                                              agg_res_ptr, debug_print,
                                              &op_handled);
        if (!op_handled) return ZAGG_WRONG_OPERATION;
        if (op_ret != 0) return op_ret;
        break;
      }
    }
  }
  m_processed_rows++;
#ifdef DEBUG_CTE
  if ((m_processed_rows % 128) == 0) {
    DEB_CTE(("(0x%p)->m_processed_rows = %llu, ProcessRec",
      this, m_processed_rows));
  }
#endif
  return 0;
}

Int32 JoinAggInterpreter::processRecWithLinkedAttrs(
    Dbtup* block_tup,
    Dbtup::KeyReqStruct* req_struct,
    const Uint32* linked_attr_data,
    Uint32 linked_attr_len,
    Uint32 thread_id,
    EmulatedJamBuffer *jamBuf,
    const LeafProgram* leaf) {
  std::unique_lock<std::mutex> lock(m_mutex, std::defer_lock);
  if (m_use_mutex) lock.lock();

  // Switch to leaf program under mutex protection.
  // For single-leaf queries, leaf is nullptr — no switch needed.
  if (leaf != nullptr) {
    thrjam(jamBuf);
    m_prog = const_cast<Uint32*>(leaf->m_agg_program);
    m_prog_len = leaf->m_agg_program_len;
    m_agg_prog_start_pos = leaf->m_agg_prog_start_pos;
    m_acc_offset = leaf->m_acc_offset;
  }

  m_linked_attr_data = linked_attr_data;
  m_linked_attr_len = linked_attr_len;

  // When called without a table reference (CTE_LOOKUP agg feed),
  // treat local columns as NULL to avoid nullptr dereference in ProcessRec.
  if (block_tup == nullptr) {
    thrjam(jamBuf);
    m_null_local_columns = true;
  }

  // D26: the build path runs on this LDM thread (block_tup is its own Dbtup),
  // so the group-key hash uses that thread's private xfrm scratch.
  Int32 ret = ProcessRec(block_tup, req_struct, thread_id, jamBuf,
                         block_tup->getAggXfrmBuf(),
                         block_tup->getAggXfrmBufLen());

  m_null_local_columns = false;
  m_linked_attr_data = nullptr;
  m_linked_attr_len = 0;
  return ret;
}

Int32 JoinAggInterpreter::evictOneGroup(Uint32* buf, Uint32 buf_words,
                                         Uint32* words_written,
                                         uchar* xfrm_buf,
                                         Uint32 xfrm_buf_len) {
  if (m_gb_map == nullptr || m_gb_map->empty()) {
    return -1;
  }

  static const Uint32 MAX_CHUNK_SCAN = 10;
  MemChunk* target = nullptr;
  Uint32 scanned = 0;
  for (MemChunk* c = m_chunks; c != nullptr && scanned < MAX_CHUNK_SCAN;
       c = c->next, scanned++) {
    if (c->group_list != nullptr &&
        (target == nullptr || c->live_groups < target->live_groups)) {
      target = c;
      if (target->live_groups == 1) break;
    }
  }

  require(target != nullptr);

  char* raw = target->group_list;
  target->group_list = *reinterpret_cast<char**>(raw);
  Uint32 key_len = *reinterpret_cast<Uint32*>(raw + 2 * sizeof(char*));
  char* data_ptr = raw + GROUP_LINK_OVERHEAD;
  Uint32 v_len_base = val_len();
  AggResItem* slots = reinterpret_cast<AggResItem*>(data_ptr + key_len);
  // Phase I.6 (F.2-K.5): include any appended string-payload region
  // in the wire-size budget; switch marker to AGG_CHAR_RESULT.
  const bool has_strings = hasStringSlots();
  const Uint32 marker = has_strings
      ? AttributeHeader::AGG_CHAR_RESULT
      : AttributeHeader::AGG_RESULT;
  const Uint32 payload_bytes = has_strings ? stringPayloadSize(slots) : 0;
  const Uint32 v_len_total = v_len_base + payload_bytes;
  const Uint32 data_words = (key_len + v_len_total) >> 2;
  const Uint32 total_words = 4 + data_words;

  if (total_words > buf_words) {
    return -1;
  }

  Uint32 pos = 0;
  buf[pos++] = marker << 16 | 0x0721;
  buf[pos++] = m_n_gb_cols << 16 | m_n_agg_results;
  buf[pos++] = 1;
  buf[pos++] = key_len << 16 | v_len_total;
  memcpy(&buf[pos], data_ptr, key_len + v_len_base);
  if (payload_bytes > 0) {
    encodeStringPayload(slots, reinterpret_cast<char*>(
        &buf[pos + ((key_len + v_len_base) >> 2)]));
  }
  pos += data_words;

  *words_written = pos;

  m_result_size -= (key_len + v_len_base);
  m_n_groups--;

  // Phase I.6 (F.2-K.4e): release per-(group, slot) string winner
  // buffers before the group leaves the local hash table.  K.5
  // wire-format emit has already substituted payload into the
  // outbound packet above, so val_ptr is safe to free here.
  freeGroupStringSlots(slots);
  m_gb_map->erase(data_ptr, key_len, xfrm_buf, xfrm_buf_len);
  freeGroupData(data_ptr);

  return 0;
}

Int32 JoinAggInterpreter::finalizeResults() {
  return 0;
}


static bool isStringAggType(DataType type) {
  return type == NDB_TYPE_CHAR ||
         type == NDB_TYPE_VARCHAR ||
         type == NDB_TYPE_LONGVARCHAR;
}

static Uint32 stringPrefixBytes(DataType type) {
  return type == NDB_TYPE_CHAR ? 0 :
         type == NDB_TYPE_VARCHAR ? 1 : 2;
}

static bool isMaxAggOp(Uint8 op) {
  return op == kOpMax || op == kOpMaxBigint || op == kOpMaxDouble;
}

static void freeStringAggSlot(AggResItem* slot) {
  if (isStringAggType(slot->type) && slot->value.val_ptr != nullptr) {
    lc_ndbd_pool_free(slot->value.val_ptr);
    slot->value.val_ptr = nullptr;
  }
}

static Int32 copyStringAggSlot(AggResItem* dst,
                               const AggResItem* src,
                               const StringResult* string_results,
                               Uint32 agg_index,
                               Uint32 thread_id) {
  const char* src_buf = static_cast<const char*>(src->value.val_ptr);
  if (src_buf == nullptr) {
    return ZAGG_OTHER_ERROR;
  }
  const Uint16 payload_len = *reinterpret_cast<const Uint16*>(src_buf);
  const Uint32 prefix = (string_results != nullptr) ?
      string_results[agg_index].prefix_bytes : stringPrefixBytes(src->type);
  const Uint32 byte_size = prefix + payload_len;
  Uint32 alloc_size = (4 + byte_size + 15) & ~15U;
  if (alloc_size < 16) alloc_size = 16;
  char* dst_buf = static_cast<char*>(
      lc_ndbd_pool_malloc(alloc_size, RG_QUERY_MEMORY, thread_id, false));
  if (dst_buf == nullptr) {
    return ZAGG_ALLOC_MEM_FAILED;
  }
  Uint16* hdr = reinterpret_cast<Uint16*>(dst_buf);
  hdr[0] = payload_len;
  hdr[1] = static_cast<Uint16>(alloc_size - 4);
  if (byte_size > 0) {
    memcpy(dst_buf + 4, src_buf + 4, byte_size);
  }
  *dst = *src;
  dst->value.val_ptr = dst_buf;
  return 0;
}

static Int32 assignStringAggSlot(AggResItem* dst,
                                 AggResItem* src,
                                 const StringResult* string_results,
                                 Uint32 agg_index,
                                 Uint32 thread_id,
                                 bool move_src) {
  freeStringAggSlot(dst);
  if (move_src) {
    *dst = *src;
    src->value.val_ptr = nullptr;
    src->is_null = true;
    src->type = NDB_TYPE_UNDEFINED;
    return 0;
  }
  return copyStringAggSlot(dst, src, string_results, agg_index, thread_id);
}

static Int32 mergeStringAccumulator(AggResItem* dst,
                                    AggResItem* src,
                                    const StringResult* string_results,
                                    Uint32 agg_index,
                                    Uint8 op,
                                    Uint32 thread_id,
                                    bool move_src) {
  if (src->type == NDB_TYPE_UNDEFINED || src->is_null) {
    return 0;
  }
  if (dst->type == NDB_TYPE_UNDEFINED || dst->is_null) {
    return assignStringAggSlot(dst, src, string_results, agg_index,
                               thread_id, move_src);
  }
  const char* src_buf = static_cast<const char*>(src->value.val_ptr);
  const char* dst_buf = static_cast<const char*>(dst->value.val_ptr);
  if (src_buf == nullptr || dst_buf == nullptr) {
    return ZAGG_OTHER_ERROR;
  }
  const Uint16 src_payload_len = *reinterpret_cast<const Uint16*>(src_buf);
  const Uint16 dst_payload_len = *reinterpret_cast<const Uint16*>(dst_buf);
  const Uint32 prefix = (string_results != nullptr) ?
      string_results[agg_index].prefix_bytes : stringPrefixBytes(src->type);
  const Uint32 src_len = prefix + src_payload_len;
  const Uint32 dst_len = prefix + dst_payload_len;
  const NdbSqlUtil::Type& sqlType = NdbSqlUtil::getType(src->type);
  const CHARSET_INFO* charset = (string_results != nullptr) ?
      string_results[agg_index].charset : nullptr;
  const int cmp = (*sqlType.m_cmp)(charset,
                                   src_buf + 4, src_len,
                                   dst_buf + 4, dst_len);
  const bool replace = isMaxAggOp(op) ? (cmp > 0) : (cmp < 0);
  if (!replace) {
    return 0;
  }
  return assignStringAggSlot(dst, src, string_results, agg_index,
                             thread_id, move_src);
}

static Int32 mergeAccumulators(AggResItem* dst, AggResItem* src,
                               Uint32 n_agg_results,
                               const Uint8* agg_ops,
                               const StringResult* string_results,
                               Uint32 thread_id,
                               bool move_src_strings) {
  for (Uint32 i = 0; i < n_agg_results; i++) {
    if (src[i].type == NDB_TYPE_UNDEFINED) continue;
    if (isStringAggType(src[i].type)) {
      Int32 ret = mergeStringAccumulator(&dst[i], &src[i],
                                         string_results, i, agg_ops[i],
                                         thread_id, move_src_strings);
      if (ret != 0) {
        return ret;
      }
      continue;
    }
    if (dst[i].type == NDB_TYPE_UNDEFINED) {
      dst[i] = src[i];
      continue;
    }
    if (src[i].is_null) continue;
    if (dst[i].is_null) { dst[i] = src[i]; continue; }
    /* Both slots non-null and numeric — merge with the shared
     * signedness/promotion-correct helper (NdbAggregationCommon.hpp).
     * The old per-op code here keyed every compare and add on
     * dst.is_unsigned alone.  Numeric overflow retains the legacy
     * distributed-merge behavior; this function's error return remains
     * reserved for errors that its callers already propagate. */
    aggMergeNumericSlot(&dst[i], src[i], agg_ops[i]);
  }
  return 0;
}

static Int32 decodeRedistributionStringSlots(
    AggResItem* slots,
    Uint32 n_agg_results,
    const char* appended,
    Uint32 appended_len,
    const StringResult* string_results,
    Uint32 thread_id) {
  const char* p = appended;
  const char* end = appended + appended_len;
  for (Uint32 i = 0; i < n_agg_results; i++) {
    if (!isStringAggType(slots[i].type) ||
        slots[i].is_null ||
        slots[i].value.val_ptr == nullptr) {
      continue;
    }
    if (p + sizeof(Uint32) > end) {
      return ZAGG_OTHER_ERROR;
    }
    const Uint32 byte_size = *reinterpret_cast<const Uint32*>(p);
    p += sizeof(Uint32);
    const Uint32 padded = (byte_size + 3) & ~3U;
    if (p + padded > end) {
      return ZAGG_OTHER_ERROR;
    }
    const Uint32 prefix = (string_results != nullptr) ?
        string_results[i].prefix_bytes : stringPrefixBytes(slots[i].type);
    if (byte_size < prefix) {
      return ZAGG_OTHER_ERROR;
    }
    const Uint32 payload_len = byte_size - prefix;
    Uint32 alloc_size = (4 + byte_size + 15) & ~15U;
    if (alloc_size < 16) alloc_size = 16;
    char* dst_buf = static_cast<char*>(
        lc_ndbd_pool_malloc(alloc_size, RG_QUERY_MEMORY, thread_id, false));
    if (dst_buf == nullptr) {
      return ZAGG_ALLOC_MEM_FAILED;
    }
    Uint16* hdr = reinterpret_cast<Uint16*>(dst_buf);
    hdr[0] = static_cast<Uint16>(payload_len);
    hdr[1] = static_cast<Uint16>(alloc_size - 4);
    if (byte_size > 0) {
      memcpy(dst_buf + 4, p, byte_size);
    }
    slots[i].value.val_ptr = dst_buf;
    p += padded;
  }
  return 0;
}

static void extractAggOps(const Uint32* prog, Uint32 prog_len,
                          Uint32 agg_prog_start_pos,
                          Uint8* agg_ops, Uint32 n_agg_results,
                          const Uint16* avg_hidden_map,
                          Uint32 n_visible_results) {
  memset(agg_ops, 0, n_agg_results);
  Uint32 exec_pos = agg_prog_start_pos;
  while (exec_pos < prog_len) {
    Uint32 value = prog[exec_pos++];
    Uint8 op = (value & 0xFC000000) >> 26;
    Uint32 agg_index;
    switch (op) {
      case kOpSum: case kOpSumBigint: case kOpSumDouble:
      case kOpMax: case kOpMaxBigint: case kOpMaxDouble:
      case kOpMin: case kOpMinBigint: case kOpMinDouble:
      case kOpCount:
        agg_index = value & 0x0000FFFF;
        if (agg_index < n_agg_results) agg_ops[agg_index] = op;
        break;
      case kOpAvg:
        /* The visible dst merges as a SUM, the hidden companion as a
         * COUNT — both commutative, so the merge machinery treats an
         * AVG pair as two ordinary slots (cte_avg_plan.md). */
        agg_index = value & 0x0000FFFF;
        if (agg_index < n_visible_results) {
          agg_ops[agg_index] = kOpSum;
          if (avg_hidden_map != nullptr &&
              avg_hidden_map[agg_index] !=
                  AggInterpreterBase::AVG_NO_HIDDEN &&
              avg_hidden_map[agg_index] < n_agg_results) {
            agg_ops[avg_hidden_map[agg_index]] = kOpCount;
          }
        }
        break;
      case kOpLoadCol: {
        Uint32 type = AggInterpreterBase::decodeLoadColType(value);
        if (type == NDB_TYPE_DECIMAL || type == NDB_TYPE_DECIMALUNSIGNED)
          exec_pos++;
        break;
      }
      case kOpLoadConst:
        exec_pos += 2;
        break;
      case kOpEmbeddedInterp: {
        Uint32 emb_len = value & 0xFFFF;
        exec_pos += emb_len;
        break;
      }
      case kOpSkip:
      case kOpSetRegNull:
        break;
      default:
        break;
    }
  }
}

Uint32 JoinAggInterpreter::mergeFrom(JoinAggInterpreter* other,
                                      Uint32 max_groups,
                                      uchar* xfrm_buf,
                                      Uint32 xfrm_buf_len) {
  assert(other != nullptr);
  assert(m_n_agg_results == other->m_n_agg_results);

  if (ensureStringResultsFrom(other->m_string_results) != 0) {
    return 0;
  }

  if (!m_agg_ops_cached) {
    extractAggOps(m_prog, m_prog_len, m_agg_prog_start_pos,
                  m_cached_agg_ops, m_n_agg_results,
                  m_avg_hidden_map, m_n_visible_results);
    m_agg_ops_cached = true;
  }

  if (m_n_gb_cols == 0) {
    if (other->m_agg_results != nullptr) {
      Int32 ret = mergeAccumulators(m_agg_results, other->m_agg_results,
                                    m_n_agg_results, m_cached_agg_ops,
                                    m_string_results, m_thread_id, true);
      if (ret != 0) {
        g_eventLogger->debug("mergeFrom scalar accumulator merge failed: %d",
                             ret);
        return 0;
      }
    }
    m_processed_rows += other->m_processed_rows;
    DEB_CTE(("(0x%p)->m_processed_rows = %llu, other: 0x%p, cols=0",
      this, m_processed_rows, other));
    return 0;
  }

  if (other->m_gb_map == nullptr || other->m_gb_map->empty()) {
    m_processed_rows += other->m_processed_rows;
    DEB_CTE(("(0x%p)->m_processed_rows = %llu, other: 0x%p, empty map",
      this, m_processed_rows, other));
    return 0;
  }

  const Uint32 v_len = val_len();
  const Uint32 nbuckets = m_gb_map->bucketCount();
  Uint32 count = 0;
  for (Uint32 b = 0; b < nbuckets; b++) {
    while (!other->m_gb_map->bucketEmpty(b)) {
      char* other_data = other->m_gb_map->popBucketHead(b);
      Uint32 other_key_len =
        *reinterpret_cast<Uint32*>(other_data - JoinGBHashTable::OVERHEAD +
                                   JoinGBHashTable::KEY_LEN_OFFSET);

      char* my_data = m_gb_map->findInBucket(b, other_data, other_key_len);
      if (my_data != nullptr) {
        AggResItem *other_items =
          reinterpret_cast<AggResItem *>(other_data + other_key_len);
        AggResItem *my_items =
          reinterpret_cast<AggResItem *>(my_data + other_key_len);
        Int32 ret = mergeAccumulators(my_items, other_items, m_n_agg_results,
                                      m_cached_agg_ops, m_string_results,
                                      m_thread_id, true);
        if (ret != 0) {
          g_eventLogger->debug("mergeFrom group accumulator merge failed: %d",
                               ret);
          other->freeGroupData(other_data);
          return 0;
        }
        other->freeGroupData(other_data);
      } else {
        m_gb_map->insertRaw(other_data, xfrm_buf, xfrm_buf_len);
        m_result_size += other_key_len + v_len;
      }
      count++;

      if (max_groups > 0 && count >= max_groups &&
          !other->m_gb_map->empty()) {
        m_n_groups = m_gb_map->size();
        return other->m_gb_map->size();
      }
    }
  }

  if (other->m_chunks != nullptr) {
    other->m_chunks_tail->next = m_chunks;
    if (m_chunks != nullptr) {
      m_chunks->prev = other->m_chunks_tail;
    } else {
      m_chunks_tail = other->m_chunks_tail;
    }
    m_chunks = other->m_chunks;
    m_chunks->prev = nullptr;
    m_total_chunk_bytes += other->m_total_chunk_bytes;
    other->m_chunks = nullptr;
    other->m_chunks_tail = nullptr;
    other->m_current_chunk = nullptr;
    other->m_total_chunk_bytes = 0;
  }

  m_processed_rows += other->m_processed_rows;
  m_n_groups = m_gb_map->size();
  DEB_CTE(("(0x%p)->m_processed_rows = %llu, other: 0x%p",
    this, m_processed_rows, other));
  return 0;
}

Int32 JoinAggInterpreter::mergeOneGroup(const char* key, Uint32 keyLen,
                                         const char* accumulators,
                                         Uint32 accLen,
                                         uchar* xfrm_buf,
                                         Uint32 xfrm_buf_len) {
  /* Phase I.17e: scalar (no GROUP BY) redistribute reuses this entry
   * point with keyLen == 0 — dispatch to the accumulator-only merge. */
  if (keyLen == 0) {
    return mergeScalarAccumulators(accumulators, accLen);
  }
  if (m_gb_map == nullptr) return -1;

  if (!m_agg_ops_cached) {
    extractAggOps(m_prog, m_prog_len, m_agg_prog_start_pos,
                  m_cached_agg_ops, m_n_agg_results,
                  m_avg_hidden_map, m_n_visible_results);
    m_agg_ops_cached = true;
  }

  const Uint32 v_len = val_len();
  if (accLen < v_len) return -1;
  const Uint32 payload_len = accLen - v_len;
  AggResItem local_items[MAX_AGG_N_RESULTS];
  const AggResItem* src_const_items =
      reinterpret_cast<const AggResItem*>(accumulators);
  if (payload_len > 0) {
    if (m_n_agg_results > MAX_AGG_N_RESULTS) return -1;
    memcpy(local_items, src_const_items, v_len);
    Int32 ret = ensureStringResultsFromRedistribution(
        local_items, accumulators + v_len, payload_len);
    if (ret != 0) {
      return ret;
    }
    ret = decodeRedistributionStringSlots(
        local_items, m_n_agg_results, accumulators + v_len, payload_len,
        m_string_results, m_thread_id);
    if (ret != 0) {
      for (Uint32 i = 0; i < m_n_agg_results; i++) {
        freeStringAggSlot(&local_items[i]);
      }
      return ret;
    }
    src_const_items = local_items;
  }

  /* Look up key in local hash table */
  char* found = m_gb_map->find(key, keyLen, xfrm_buf, xfrm_buf_len);

  if (found != nullptr) {
    /* Key exists — merge accumulators */
    AggResItem *my_items =
      reinterpret_cast<AggResItem *>(found + keyLen);
    AggResItem *src_items = const_cast<AggResItem*>(src_const_items);
    Int32 ret = mergeAccumulators(my_items, src_items, m_n_agg_results,
                                  m_cached_agg_ops, m_string_results,
                                  m_thread_id, false);
    if (ret != 0) {
      if (payload_len > 0) {
        for (Uint32 i = 0; i < m_n_agg_results; i++) {
          freeStringAggSlot(&local_items[i]);
        }
      }
      return ret;
    }
    if (payload_len > 0) {
      for (Uint32 i = 0; i < m_n_agg_results; i++) {
        freeStringAggSlot(&local_items[i]);
      }
    }
  } else {
    /* New key — allocate and insert */
    char *new_group = allocGroupData(keyLen + v_len, keyLen);
    if (new_group == nullptr) return -1;  /* Memory allocation failure */

    memcpy(new_group, key, keyLen);
    if (payload_len > 0) {
      AggResItem* dst_items = reinterpret_cast<AggResItem*>(
          new_group + keyLen);
      memcpy(dst_items, src_const_items, v_len);
      for (Uint32 i = 0; i < m_n_agg_results; i++) {
        if (isStringAggType(dst_items[i].type) &&
            !dst_items[i].is_null &&
            dst_items[i].value.val_ptr != nullptr) {
          dst_items[i].value.val_ptr = nullptr;
          Int32 ret = copyStringAggSlot(&dst_items[i], &src_const_items[i],
                                        m_string_results, i, m_thread_id);
          if (ret != 0) {
            for (Uint32 j = 0; j < m_n_agg_results; j++) {
              freeStringAggSlot(&dst_items[j]);
            }
            freeGroupData(new_group);
            for (Uint32 j = 0; j < m_n_agg_results; j++) {
              freeStringAggSlot(&local_items[j]);
            }
            return ret;
          }
        }
      }
      for (Uint32 i = 0; i < m_n_agg_results; i++) {
        freeStringAggSlot(&local_items[i]);
      }
    } else {
      memcpy(new_group + keyLen, accumulators, v_len);
    }

    m_gb_map->insert(new_group, keyLen, xfrm_buf, xfrm_buf_len);
    m_n_groups = m_gb_map->size();
    m_result_size += keyLen + v_len;
  }
  return 0;
}

Uint32 JoinAggInterpreter::redistributionValueLen(
    const AggResItem* slots) const {
  return val_len() + (hasStringSlots() ? stringPayloadSize(slots) : 0);
}

Int32 JoinAggInterpreter::mergeScalarAccumulators(const char* accumulators,
                                                   Uint32 accLen) {
  if (m_n_gb_cols != 0) return -1;
  if (m_agg_results == nullptr) return -1;

  if (!m_agg_ops_cached) {
    extractAggOps(m_prog, m_prog_len, m_agg_prog_start_pos,
                  m_cached_agg_ops, m_n_agg_results,
                  m_avg_hidden_map, m_n_visible_results);
    m_agg_ops_cached = true;
  }

  const Uint32 v_len = val_len();
  if (accLen < v_len) return -1;

  const Uint32 payload_len = accLen - v_len;
  AggResItem local_items[MAX_AGG_N_RESULTS];
  const AggResItem* src_const_items =
      reinterpret_cast<const AggResItem*>(accumulators);
  if (payload_len > 0) {
    if (m_n_agg_results > MAX_AGG_N_RESULTS) return -1;
    memcpy(local_items, src_const_items, v_len);
    Int32 ret = ensureStringResultsFromRedistribution(
        local_items, accumulators + v_len, payload_len);
    if (ret != 0) {
      return ret;
    }
    ret = decodeRedistributionStringSlots(
        local_items, m_n_agg_results, accumulators + v_len, payload_len,
        m_string_results, m_thread_id);
    if (ret != 0) {
      for (Uint32 i = 0; i < m_n_agg_results; i++) {
        freeStringAggSlot(&local_items[i]);
      }
      return ret;
    }
    src_const_items = local_items;
  }
  AggResItem* src_items = const_cast<AggResItem*>(src_const_items);
  Int32 ret = mergeAccumulators(m_agg_results, src_items, m_n_agg_results,
                                m_cached_agg_ops, m_string_results,
                                m_thread_id, false);
  if (payload_len > 0) {
    for (Uint32 i = 0; i < m_n_agg_results; i++) {
      freeStringAggSlot(&local_items[i]);
    }
  }
  if (ret == 0) {
    /* Single-feeder rule companion (dtw-19b): a peer contributed, so
     * the cteScanAggFeed scalar gate (processed_rows() > 0) must pass
     * on this owner even when it scanned no local rows itself. */
    m_processed_rows++;
  }
  return ret;
}
Int32 JoinAggInterpreter::initGBTypesForNullLocal(EmulatedJamBuffer *jamBuf) {
  /*
   * Called when the first row is a null-extended row (m_null_local_columns).
   * JOIN_AGG_SETUP_REQ normally initializes GROUP BY metadata before any row
   * is processed.  This fallback only handles typed linked data that arrives
   * in the NULL-row request itself; otherwise metadata must already be cached.
   */
  for (Uint32 i = 0; i < m_n_gb_cols; i++) {
    Uint32 attr_id = m_gb_cols[i] >> 16;
    thrjamDebug(jamBuf);
    thrjamDataDebug(jamBuf, attr_id);
    GBColTypeInfo &info = m_gb_types[i];

    if ((attr_id & 0x8000) != 0) {
      thrjamDebug(jamBuf);
      if (unlikely(m_linked_attr_data == nullptr)) {
        return ZAGG_OTHER_ERROR;
      }
      Uint32 position = attr_id & 0x7FFF;
      const Uint32* p = m_linked_attr_data;
      const Uint32* p_end = m_linked_attr_data + m_linked_attr_len;
      Uint32 pos_count = 0;
      while (p < p_end && pos_count < position) {
        p += 2;
        p += 1 + AttributeHeader::getDataSize(*p);
        pos_count++;
      }
      if (p + 2 < p_end) {
        Uint32 word0 = p[0];
        Uint32 word1 = p[1];
        if (CteLinkedAttr::isCteMarker(word0)) {
          thrjamDebug(jamBuf);
          info.typeId = CteLinkedAttr::decodeTypeId(word0);
          info.maxBytes = CteLinkedAttr::decodeMaxBytes(word0);
          info.cs = nullptr;
          Uint32 csNumber = CteLinkedAttr::decodeCsNumber(word1);
          if (csNumber != 0) {
            thrjamDebug(jamBuf);
            if (unlikely(csNumber >= NDB_ARRAY_SIZE(all_charsets) ||
                         all_charsets[csNumber] == nullptr)) {
              return ZAGG_OTHER_ERROR;
            }
            info.cs = all_charsets[csNumber];
          }
        } else {
          thrjamDebug(jamBuf);
          Uint32 tableId = word0;
          Uint32 tableVersion = word1;
          Uint32 linkedAttrId = AttributeHeader(p[2]).getAttributeId();
          if (unlikely(tableId == 0 || tableId == RNIL)) {
            g_eventLogger->debug("initGBTypesForNullLocal: linked GROUP BY "
                "column has untyped synthetic metadata prefix tableId=%u "
                "schemaVersion=%u", tableId, tableVersion);
            return ZAGG_OTHER_ERROR;
          }
          const ColumnMeta *meta =
              findColumnMeta(tableId, tableVersion, linkedAttrId);
          if (meta != nullptr) {
            info.typeId = meta->typeId;
            info.maxBytes = meta->maxBytes;
            info.cs = nullptr;
            if (meta->csNumber != 0) {
              thrjamDebug(jamBuf);
              info.cs = all_charsets[meta->csNumber];
            }
          } else {
            g_eventLogger->debug("initGBTypesForNullLocal: missing metadata "
                "for linked GROUP BY column tableId=%u schemaVersion=%u "
                "columnId=%u", tableId, tableVersion, linkedAttrId);
            return ZAGG_OTHER_ERROR;
          }
        }
      } else {
        return ZAGG_OTHER_ERROR;
      }
    } else {
      g_eventLogger->debug("initGBTypesForNullLocal: missing metadata "
          "for local GROUP BY column attr_id=%u", attr_id);
      return ZAGG_OTHER_ERROR;
    }
    const NdbSqlUtil::Type &sqlType = NdbSqlUtil::getType(info.typeId);
    info.cmpFn = sqlType.m_cmp;
  }
  return publishGBTypes(jamBuf);
}

Int32 JoinAggInterpreter::processNullExtendedRow(
    Uint32* attr_read_buf,
    const Uint32* linked_attr_data,
    Uint32 linked_attr_len,
    Uint32 thread_id,
    EmulatedJamBuffer *jamBuf,
    uchar* xfrm_buf,
    Uint32 xfrm_buf_len,
    const LeafProgram* leaf) {
  std::unique_lock<std::mutex> lock(m_mutex, std::defer_lock);
  if (m_use_mutex) lock.lock();

  if (leaf != nullptr) {
    thrjam(jamBuf);
    m_prog = const_cast<Uint32*>(leaf->m_agg_program);
    m_prog_len = leaf->m_agg_program_len;
    m_agg_prog_start_pos = leaf->m_agg_prog_start_pos;
    m_acc_offset = leaf->m_acc_offset;
  }

  m_linked_attr_data = linked_attr_data;
  m_linked_attr_len = linked_attr_len;
  m_null_local_columns = true;

  /*
   * Null-extended rows have no local tuple to read.  m_null_local_columns
   * drives kOpLoadCol to synthesize NULL AttributeHeaders into attr_read_buf.
   */
  Int32 ret = ProcessRec(nullptr, nullptr, thread_id, jamBuf,
                         xfrm_buf, xfrm_buf_len, attr_read_buf);

  m_null_local_columns = false;
  m_linked_attr_data = nullptr;
  m_linked_attr_len = 0;
  return ret;
}

// release_string_results body lifted to AggInterpreterBase in Step 3a-A.

Int32 JoinAggInterpreter::ensureStringResultsFrom(
    const StringResult* source) {
  if (m_string_results != nullptr || source == nullptr) {
    return 0;
  }
  const Uint32 nbytes = m_n_agg_results * sizeof(StringResult);
  m_string_results = static_cast<StringResult*>(
      lc_ndbd_pool_malloc(nbytes, RG_QUERY_MEMORY, m_thread_id, true));
  if (m_string_results == nullptr) {
    return ZAGG_ALLOC_MEM_FAILED;
  }
  memcpy(m_string_results, source, nbytes);
  for (Uint32 i = 0; i < m_n_agg_results; i++) {
    m_string_results[i].ptr = nullptr;
    m_string_results[i].length = 0;
    m_string_results[i].size = 0;
  }
  return 0;
}

Int32 JoinAggInterpreter::ensureStringResultsFromRedistribution(
    const AggResItem* slots,
    const char* appended,
    Uint32 appended_len) {
  if (m_string_results != nullptr) {
    return 0;
  }
  const Uint32 nbytes = m_n_agg_results * sizeof(StringResult);
  m_string_results = static_cast<StringResult*>(
      lc_ndbd_pool_malloc(nbytes, RG_QUERY_MEMORY, m_thread_id, true));
  if (m_string_results == nullptr) {
    return ZAGG_ALLOC_MEM_FAILED;
  }
  const char* p = appended;
  const char* end = appended + appended_len;
  for (Uint32 i = 0; i < m_n_agg_results; i++) {
    if (!isStringAggType(slots[i].type) ||
        slots[i].is_null ||
        slots[i].value.val_ptr == nullptr) {
      continue;
    }
    if (p + sizeof(Uint32) > end) {
      return ZAGG_OTHER_ERROR;
    }
    const Uint32 byte_size = *reinterpret_cast<const Uint32*>(p);
    p += sizeof(Uint32);
    const Uint32 prefix = stringPrefixBytes(slots[i].type);
    if (byte_size < prefix) {
      return ZAGG_OTHER_ERROR;
    }
    StringResult& sr = m_string_results[i];
    sr.ptr = nullptr;
    sr.length = 0;
    sr.size = 0;
    sr.prefix_bytes = static_cast<Uint16>(prefix);
    sr.declared_size = static_cast<Uint16>(byte_size - prefix);
    sr.charset = nullptr;
    p += (byte_size + 3) & ~3U;
    if (p > end) {
      return ZAGG_OTHER_ERROR;
    }
  }
  return 0;
}
