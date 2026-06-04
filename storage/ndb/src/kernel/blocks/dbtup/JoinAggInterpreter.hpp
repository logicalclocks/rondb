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

#ifndef JOINAGGINTERPRETER_H_
#define JOINAGGINTERPRETER_H_

#include <math.h>
#include <cstring>
#include <mutex>
#include "AggInterpreterBase.hpp"
#include "PushdownInterpreter.hpp"
#include "Dbtup.hpp"
#include "AggHashTable.hpp"

// DECIMAL_BUFF_LENGTH now lives in AggInterpreterBase.hpp (Step 1.3).

/**
 * JoinAggInterpreter — aggregation interpreter for join pushdown.
 *
 * Holds all join-specific state: mutex, linked attribute buffer pointers,
 * chunk-based group eviction, large GBHashTable (1024 buckets), merge cache,
 * and type-aware hashing.  Allocated via a single m_buf_block allocation.
 *
 * Separated from AggInterpreter so that normal scan aggregation (SELECT
 * COUNT(*) FROM t) stays at 1 page (32KB) with all buffers inline.
 */
class JoinAggInterpreter : public AggInterpreterBase {
 public:
  JoinAggInterpreter(Uint32 prog_len,
                     Int64 table_id, Int64 frag_id,
                     Uint32 thread_id):
    AggInterpreterBase(PushdownType::AGGREGATION, prog_len,
                       table_id, frag_id, thread_id),
    /* m_cur_pos / m_attr_read_pos / m_processed_rows / m_result_size lifted
     * to base in Step 3a-A; m_n_gb_cols / m_gb_cols / m_gb_map / m_n_groups
     * lifted in Step 2b; m_attr_read_buf lifted in Step 3a-B. */
    m_acc_offset(0),
    m_linked_attr_data(nullptr), m_linked_attr_len(0),
    m_null_local_columns(false),
    m_use_mutex(false), m_max_groups(0), m_cte_mode(false),
    /* Chunk allocator state + GB type metadata lifted to
     * AggInterpreterBase in Step 2a; base ctor initializes them. */
    m_cached_agg_ops(nullptr), m_agg_ops_cached(false) {
    /* m_attr_read_buf / m_prog_buf / m_gb_cols_buf / m_agg_results_buf /
     * m_gb_map_buf / m_buf_block initialised by the base ctor
     * (Step 3a-B). */
  }
  /* ~JoinAggInterpreter() default — base destructor handles
   * release_string_results, freeAllChunks, and frees both m_xfrm_buf
   * and m_buf_block. */

  bool Init(const Uint32* prog);

  Int32 processRecWithLinkedAttrs(
      Dbtup* block_tup,
      Dbtup::KeyReqStruct* req_struct,
      const Uint32* linked_attr_data,
      Uint32 linked_attr_len,
      Uint32 thread_id,
      EmulatedJamBuffer *jamBuf,
      const struct LeafProgram* leaf = nullptr);
  Int32 finalizeResults();
  Int32 processNullExtendedRow(
      Dbtup* block_tup,
      const Uint32* linked_attr_data,
      Uint32 linked_attr_len,
      Uint32 thread_id,
      EmulatedJamBuffer *jamBuf,
      const struct LeafProgram* leaf = nullptr);

  Uint32 mergeFrom(JoinAggInterpreter* other, Uint32 max_groups);

  /* gb_map / val_len / n_gb_cols / n_agg_results / agg_results /
   * processed_rows lifted to AggInterpreterBase in Step 3b. */
  JoinGBHashTable* gb_map_mutable() {
    return m_gb_map;
  }
  /**
   * Compute the full Uint64 distribution hash for a GROUP BY key.
   * When character set columns are present, uses per-column normalization
   * via hashKeyFull. Otherwise uses rondb_xxhash_std on raw key bytes,
   * which is consistent with DBSPJ's CTE_LOOKUP routing hash.
   */
  Uint64 hashGroupKey(const char* key, Uint32 keyLen) const {
    if (m_gb_types_inited) {
      for (Uint32 i = 0; i < m_n_gb_cols; i++) {
        if (m_gb_types[i].cs != nullptr) {
          return m_gb_map->hashKeyFull(key, keyLen);
        }
      }
    }
    return rondb_xxhash_std(key, keyLen);
  }
  /* Per-column GROUP BY type info, populated by initGBTypes on first
   * processed row.  Returns nullptr until initialized.  Used by
   * Dblqh::buildCteLinkedBuffer / cteScanAggFeed to encode CTE
   * virt-column markers in linked-attr entries (cte_filter_phase_e1k.md). */
  const GBColTypeInfo* gb_types() const {
    return m_gb_types_inited ? m_gb_types : nullptr;
  }

  /**
   * Look up a single group by key in the hash table.
   * Returns pointer to group data (key + accumulators) or nullptr if not found.
   * The returned pointer points past the 24-byte group link header.
   * Layout: [key_data (keyLen bytes)] [accumulator_data (val_len() bytes)]
   */
  const char* lookupGroup(const char* key, Uint32 keyLen) const {
    return m_gb_map ? m_gb_map->find(key, keyLen) : nullptr;
  }

  /**
   * Merge a single incoming group into the hash table.
   * If the key exists locally, merges accumulators using cached agg ops.
   * If the key is new, inserts a new group with the incoming data.
   * For Phase I.17e scalar (no GROUP BY) redistribute, callers can pass
   * keyLen == 0 and the call dispatches to mergeScalarAccumulators.
   * Returns 0 on success, negative on error (e.g., memory allocation failure).
   */
  Int32 mergeOneGroup(const char* key, Uint32 keyLen,
                      const char* accumulators, Uint32 accLen);
  Uint32 redistributionValueLen(const AggResItem* slots) const;

  /**
   * Phase I.17e: merge an inbound scalar accumulator payload into this
   * interpreter's m_agg_results.  Used by execJOIN_AGG_REDISTRIBUTE_REQ
   * for n_gb_cols == 0 CTE materialization, where every node packages
   * its local m_agg_results and ships it to the DBTC-co-located owner
   * node.  The owner repeatedly calls this method (one call per
   * non-owner peer) to fold every node's contribution into its own
   * accumulators.  Returns 0 on success, negative on error.
   */
  Int32 mergeScalarAccumulators(const char* accumulators, Uint32 accLen);

  void setUseMutex(bool v) { m_use_mutex = v; }
  void setMaxGroups(Uint32 v) { m_max_groups = v; }
  Uint32 maxGroups() const { return m_max_groups; }

  /**
   * CTE mode: rewrite each stored GROUP BY column's AttributeHeader
   * attrId to the column's position (0..N-1) before hash insert.
   *
   * Why: DBSPJ builds CTE_LOOKUP keys with virtual CTE attrIds (0..N-1),
   * but rows read from the source table carry source attrIds (e.g.
   * attrId=1 for "grp"). Without normalization, the raw-byte hash and
   * memcmp of the stored key diverge from the lookup key, so
   * CTE_LOOKUP_REQ and cross-node hash routing fail to find the group.
   */
  void setCteMode(bool v) { m_cte_mode = v; }

  /**
   * Multi-leaf aggregation support.
   *
   * setTotalAggResults() overrides m_n_agg_results to the combined count
   * across all leaves. Must be called after Init() but before any rows
   * are processed. This ensures hash map entries are sized for the full
   * combined accumulator layout.
   *
   * Leaf program switching is done inside processRecWithLinkedAttrs()
   * and processNullExtendedRow() under mutex protection (MUTEX_BASED).
   * Pass a non-null LeafProgram* to switch before processing.
   */
  void setTotalAggResults(Uint32 total);
  void cacheMultiLeafAggOps(const struct LeafProgram* leaves,
                            Uint32 num_leaves);
  Int32 evictOneGroup(Uint32* buf, Uint32 buf_words,
                      Uint32* words_written);
  /* initChunkAllocator / bookMoreMemory / allocGroupData / freeGroupData /
   * freeAllChunks lifted to AggInterpreterBase in Step 2a. */

 private:
  Int32 ProcessRec(Dbtup* block_tup,
                   Dbtup::KeyReqStruct* req_struct,
                   Uint32 thread_id,
                   EmulatedJamBuffer *jamBuf);

  // Phase I.6 (F.2-K.4): running thread id for the in-flight ProcessRec
  // call.  Set on entry to processRecWithLinkedAttrs /
  // processNullExtendedRow and consumed by MaxString / MinString
  // m_current_thread_id, minMaxString, freeGroupStringSlots,
  // stringPayloadSize, encodeStringPayload, hasStringSlots,
  // string_results lifted to AggInterpreterBase in Step 1.3.
  Int32 ensureStringResultsFrom(const StringResult* source);
  Int32 ensureStringResultsFromRedistribution(const AggResItem* slots,
                                              const char* appended,
                                              Uint32 appended_len);

  // m_cur_pos / m_attr_read_pos / m_processed_rows / m_result_size /
  // m_registers / m_register_string_data / m_n_agg_results / m_agg_results /
  // m_n_gb_cols / m_gb_cols / m_gb_map / m_n_groups / m_attr_read_buf /
  // static wire-header constants all lifted to AggInterpreterBase in
  // Steps 1.3 / 2b / 3a-A / 3a-B.

  // Multi-leaf: accumulator offset applied after group lookup/creation.
  // agg_res_ptr is shifted by m_acc_offset so the leaf's 0-based program
  // indices map to the correct physical slots in the combined row.
  Uint32 m_acc_offset;

  // m_decimal, m_decimal_buf lifted to AggInterpreterBase in Step 1.3.

  // Linked attribute buffer for join aggregation
  const Uint32* m_linked_attr_data;// Points to current row's linked attrs
  Uint32 m_linked_attr_len;        // Current length in words
  bool m_null_local_columns;       // When true, local column read NULL

  // MUTEX_BASED locking: protects m_gb_map and accumulators during
  // concurrent access from multiple LDM threads.
  bool m_use_mutex;                   // true for MUTEX_BASED strategy
  std::mutex m_mutex;

  // Group eviction: when m_max_groups > 0 and m_gb_map reaches this
  // limit, processRecWithLinkedAttrs returns AGG_EVICT_NEEDED so the
  // caller can evict a group before retrying.
  Uint32 m_max_groups;                // 0 = unlimited
  bool m_cte_mode;                    // see setCteMode()

  /* Chunk-based allocator state (m_chunks / m_chunks_tail /
   * m_current_chunk / m_total_chunk_bytes / m_memory_budget /
   * m_budget_increment / m_total_available) lifted to
   * AggInterpreterBase in Step 2a. */

  // Cached agg ops for merge (avoids recomputing per CONTINUEB batch).
  // Lives in the extra-tail region of m_buf_block (Step 3a-B).
  Uint8* m_cached_agg_ops;
  bool m_agg_ops_cached;

  /* Per-column GROUP BY type metadata (m_gb_types, m_gb_types_inited,
   * m_xfrm_buf, m_xfrm_buf_len) lifted to AggInterpreterBase in Step
   * 2a; initGBTypes lifted in Step 2b (parametrized on linked-attr
   * args).  initGBTypesForNullLocal stays per-class (JoinAgg-only:
   * triggered only by m_null_local_columns from outer-join NULL
   * extension). */
  void initGBTypesForNullLocal(Dbtup* block_tup,
                               EmulatedJamBuffer *jamBuf);

  // m_prog_buf / m_gb_cols_buf / m_agg_results_buf / m_gb_map_buf /
  // m_buf_block / m_string_results / release_string_results lifted to
  // AggInterpreterBase in Steps 1.3 / 3a-A / 3a-B.
};

#endif  // JOINAGGINTERPRETER_H_
