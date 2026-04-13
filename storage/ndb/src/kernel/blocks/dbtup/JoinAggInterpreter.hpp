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
#include "PushdownInterpreter.hpp"
#include "Dbtup.hpp"
#include "AggHashTable.hpp"

#define DECIMAL_BUFF_LENGTH 9

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
class JoinAggInterpreter : public PushdownInterpreter {
 public:
  JoinAggInterpreter(Uint32 prog_len,
                     Int64 table_id, Int64 frag_id,
                     Uint32 thread_id):
    PushdownInterpreter(PushdownType::AGGREGATION, prog_len,
                        table_id, frag_id, thread_id),
    m_prog(nullptr), m_cur_pos(0),
    m_n_gb_cols(0), m_gb_cols(nullptr),
    m_n_agg_results(0),
    m_agg_results(nullptr), m_agg_prog_start_pos(0),
    m_gb_map(nullptr), m_n_groups(0),
    m_attr_read_buf(nullptr), m_attr_read_pos(0),
    m_acc_offset(0),
    m_processed_rows(0),
    m_result_size(0),
    m_linked_attr_data(nullptr), m_linked_attr_len(0),
    m_null_local_columns(false),
    m_use_mutex(false), m_max_groups(0),
    m_chunks(nullptr), m_chunks_tail(nullptr),
    m_current_chunk(nullptr), m_total_chunk_bytes(0),
    m_memory_budget(0), m_budget_increment(0),
    m_total_available(0),
    m_cached_agg_ops(nullptr), m_agg_ops_cached(false),
    m_gb_types(nullptr), m_gb_types_inited(false),
    m_xfrm_buf(nullptr), m_xfrm_buf_len(0),
    m_prog_buf(nullptr), m_gb_cols_buf(nullptr),
    m_agg_results_buf(nullptr), m_gb_map_buf(nullptr),
    m_buf_block(nullptr) {
      memset(m_decimal_buf, 0, sizeof(decimal_digit_t) * DECIMAL_BUFF_LENGTH);
      m_decimal.buf = m_decimal_buf;
      m_decimal.len = DECIMAL_BUFF_LENGTH;
  }
  ~JoinAggInterpreter() override {
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

  bool OptimizeProgram();

  bool Init(const Uint32* prog);

  Int32 processRecWithLinkedAttrs(
      Dbtup* block_tup,
      Dbtup::KeyReqStruct* req_struct,
      const Uint32* linked_attr_data,
      Uint32 linked_attr_len,
      const struct LeafProgram* leaf = nullptr);
  Int32 finalizeResults();
  Int32 processNullExtendedRow(
      const Uint32* linked_attr_data,
      Uint32 linked_attr_len,
      const struct LeafProgram* leaf = nullptr);

  Int32 getResultData(Uint32* buffer, Uint32 buffer_size,
                      Uint32* bytes_written);
  Uint32 mergeFrom(JoinAggInterpreter* other, Uint32 max_groups);

  const JoinGBHashTable* gb_map() const {
    return m_gb_map;
  }
  JoinGBHashTable* gb_map_mutable() {
    return m_gb_map;
  }
  Uint32 val_len() const {
    return m_n_agg_results * sizeof(AggResItem);
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
  Uint32 n_gb_cols() const { return m_n_gb_cols; }
  Uint32 n_agg_results() const { return m_n_agg_results; }
  const AggResItem* agg_results() const { return m_agg_results; }
  Uint64 processed_rows() const { return m_processed_rows; }

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
   * Returns 0 on success, negative on error (e.g., memory allocation failure).
   */
  Int32 mergeOneGroup(const char* key, Uint32 keyLen,
                      const char* accumulators, Uint32 accLen);

  void setUseMutex(bool v) { m_use_mutex = v; }
  void setMaxGroups(Uint32 v) { m_max_groups = v; }
  Uint32 maxGroups() const { return m_max_groups; }

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
  void initChunkAllocator(Uint32 thread_id, Uint32 budget_pages,
                          Uint32 available_pages);
  bool bookMoreMemory();
  char* allocGroupData(Uint32 len, Uint32 key_len);
  void freeGroupData(char* ptr);
  void freeAllChunks();

 private:
  Int32 ProcessRec(Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct);

  Uint32* m_prog;
  Uint32 m_cur_pos;
  Register m_registers[kRegTotal];

  Uint32 m_n_gb_cols;
  Uint32* m_gb_cols;
  Uint32 m_n_agg_results;
  AggResItem* m_agg_results;
  Uint32 m_agg_prog_start_pos;

  JoinGBHashTable* m_gb_map;
  Uint32 m_n_groups;
  Uint32* m_attr_read_buf;
  Uint32 m_attr_read_pos;

  // Multi-leaf: accumulator offset applied after group lookup/creation.
  // agg_res_ptr is shifted by m_acc_offset so the leaf's 0-based program
  // indices map to the correct physical slots in the combined row.
  Uint32 m_acc_offset;
  static Uint32 g_attr_read_buf_len_;
  Uint64 m_processed_rows;
  Uint32 m_result_size;
  static Uint32 g_result_header_size_;
  static Uint32 g_result_header_size_per_group_;

  decimal_t m_decimal;
  decimal_digit_t m_decimal_buf[DECIMAL_BUFF_LENGTH];

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

  // Chunk-based allocator for group data.
  MemChunk* m_chunks;
  MemChunk* m_chunks_tail;
  MemChunk* m_current_chunk;
  Uint32 m_total_chunk_bytes;
  Uint32 m_memory_budget;
  Uint32 m_budget_increment;
  Uint32 m_total_available;

  MemChunk* allocNewChunk();

  // Embedded interpreter validation (called at Init time)
  bool validateEmbeddedProgram(const Uint32* emb_prog, Uint32 emb_len);

  // Cached agg ops for merge (avoids recomputing per CONTINUEB batch)
  Uint8* m_cached_agg_ops;
  bool m_agg_ops_cached;

  // Per-column type info for type-aware GROUP BY hashing and comparison
  GBColTypeInfo* m_gb_types;
  bool m_gb_types_inited;
  uchar *m_xfrm_buf;
  Uint32 m_xfrm_buf_len;
  Int32 initGBTypes(Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct);
  void initGBTypesForNullLocal(Dbtup* block_tup);

  Uint32* m_prog_buf;
  Uint32* m_gb_cols_buf;
  AggResItem* m_agg_results_buf;
  JoinGBHashTable* m_gb_map_buf;

  // Single allocation block for all dynamically allocated buffers above.
  void* m_buf_block;
};

static_assert(sizeof(JoinAggInterpreter) <= MEM_CHUNK_SIZE,
              "JoinAggInterpreter must fit in MEM_CHUNK_SIZE allocation");

#endif  // JOINAGGINTERPRETER_H_
