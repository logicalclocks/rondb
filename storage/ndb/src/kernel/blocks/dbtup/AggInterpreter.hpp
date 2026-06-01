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
#define DECIMAL_BUFF_LENGTH 9

/**
 * AggInterpreter — lean aggregation interpreter for normal scan pushdown.
 *
 * All buffers are inline so that sizeof(AggInterpreter) fits in a single
 * 32KB page.  Uses std::map with an inline bump allocator for group data —
 * no chunk allocator, no xfrm buffer, no dynamic allocation beyond
 * std::map tree nodes.
 */
class AggInterpreter : public AggInterpreterBase {
 public:
  AggInterpreter(Uint32 prog_len,
                 Int64 table_id, Int64 frag_id,
                 Uint32 thread_id):
    AggInterpreterBase(PushdownType::AGGREGATION, prog_len,
                       table_id, frag_id, thread_id),
    m_cur_pos(0),
    m_n_gb_cols(0), m_gb_cols(nullptr),
    m_n_agg_results(0),
    m_agg_results(nullptr),
    m_gb_map(nullptr), m_n_groups(0),
    m_attr_read_pos(0),
    m_processed_rows(0),
    m_result_size(0),
    m_gb_cmp_inited(false),
    m_alloc_len(0),
    m_string_results(nullptr) {
      memset(m_decimal_buf, 0, sizeof(decimal_digit_t) * DECIMAL_BUFF_LENGTH);
      m_decimal.buf = m_decimal_buf;
      m_decimal.len = DECIMAL_BUFF_LENGTH;
      memset(m_attr_read_buf, 0, sizeof(m_attr_read_buf));
  }
  ~AggInterpreter() override {
    release_string_results();
  }

  bool Init(const Uint32* prog);

  Int32 ProcessRec(Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct,
                   Uint32 thread_id);

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
  Uint32 m_cur_pos;
  Register m_registers[kRegTotal];

  // Phase I.6 (F.2-K.4a): per-register string scratch.  When a
  // kOpLoadCol arm reads a CHAR / VARCHAR / Longvarchar column, it
  // also stashes (ptr-into-m_attr_read_buf, length, prefix_bytes,
  // declared_size, charset) here for the matching register so that a
  // subsequent kOpMin / kOpMax can compare and copy without
  // re-walking the AttributeDescriptor.  `size` is unused at register
  // scope — only the read-only view fields apply.  192 B inline.
  StringResult m_register_string_data[kRegTotal];

  Uint32 m_n_gb_cols;
  Uint32* m_gb_cols;
  Uint32 m_n_agg_results;
  AggResItem* m_agg_results;

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

  // Per-column comparison context for type-aware GROUP BY
  GBCmpContext m_gb_cmp_ctx;
  bool m_gb_cmp_inited;
  void initGBCmpCtx(Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct);

  // Inline bump allocator
  char* MemAlloc(Uint32 len);
  Uint32 m_alloc_len;

  // Phase I.6 (F.2): per-slot string MIN/MAX sidecar.  Lazily
  // allocated (via lc_ndbd_pool_malloc) on the first row that
  // populates a string-typed slot; sized to m_n_agg_results
  // entries.  Stays nullptr for programs with no string MIN/MAX,
  // so non-string queries pay zero memory cost.  Freed via
  // release_string_results() in the destructor — entries' own
  // ptrs are freed first, then the array itself.
  StringResult* m_string_results;
  void release_string_results();

  // Phase I.6 (F.2-K.4c): per-(group, slot) MIN/MAX string update.
  // `agg_res_ptr[agg_index].value.val_ptr` holds a per-group buffer
  // laid out as `[Uint16 payload_len][Uint16 capacity]
  // [prefix_bytes + payload]` (rounded up to a multiple of 16, min 16
  // bytes).  Compares the row's source bytes (read into
  // m_register_string_data[reg_index] by kOpLoadCol) against the
  // current winner using NdbSqlUtil::cmpChar / cmpVarchar /
  // cmpLongvarchar; replaces in place when capacity allows, otherwise
  // allocates a larger buffer (allocate-then-free order so the old
  // winner survives an OOM).  Slot-level metadata
  // (charset / prefix_bytes / declared_size) is captured into
  // m_string_results[agg_index] on the first call for that slot.
  // Returns 0 on success, ZAGG_ALLOC_MEM_FAILED on OOM.
  Int32 minMaxString(Uint32 reg_index, Uint32 agg_index,
                     AggResItem* agg_res_ptr, bool is_max);

  // Phase I.6 (F.2-K.4e): free per-(group, slot) string winner
  // buffers for one group's AggResItem array.  Called from the
  // m_gb_map drain path so per-group val_ptr buffers are released
  // when the group leaves the local hash map.  No-op when no string
  // slots are present (m_string_results == nullptr).  Wire-format
  // emit (Phase I.6 F.2-K.5) consumes val_ptr before this runs.
  void freeGroupStringSlots(AggResItem* slots);

 public:
  // Phase I.6 (F.2-K.5): true when at least one string MIN/MAX slot
  // has been touched in this interpreter — equivalently, when
  // m_string_results has been lazy-allocated.  Drives marker
  // selection at emit time: AGG_CHAR_RESULT when true, AGG_RESULT
  // otherwise.
  bool hasStringSlots() const { return m_string_results != nullptr; }

  // Phase I.6 (F.2-K.5): bytes the appended string-payload region
  // for one group will occupy on the wire.  For each slot whose
  // type is CHAR / VARCHAR / Longvarchar AND is non-null, contributes
  // 4 bytes (Uint32 byte_size) plus a Uint32-padded copy of the
  // [prefix + payload] bytes from the slot's val_ptr buffer.
  // Slots whose type is non-string or whose value is null contribute
  // zero bytes.
  Uint32 stringPayloadSize(const AggResItem* slots) const;

  // Phase I.6 (F.2-K.5): write one group's appended string-payload
  // region into `dst`.  Layout mirrors the size returned by
  // stringPayloadSize.  Returns total bytes written.  `dst` must
  // have at least stringPayloadSize(slots) bytes available.
  Uint32 encodeStringPayload(const AggResItem* slots, char* dst) const;
 private:

  // Phase I.6 (F.2-K.4): running thread id for the in-flight ProcessRec
  // call.  AggInterpreter is constructed once on the LDM thread that
  // creates it but ProcessRec runs from whichever thread executes the
  // scan; lc_ndbd_pool_malloc requires the *running* thread id.  Set
  // by ProcessRec() on entry; read by MaxString / MinString helpers
  // when allocating per-(group, slot) string buffers via val_ptr.
  Uint32 m_current_thread_id = 0;

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
