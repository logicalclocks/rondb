/*
 * Copyright (c) 2026, Hopsworks and/or its affiliates.
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

#ifndef AGGINTERPRETERBASE_H_
#define AGGINTERPRETERBASE_H_

#include <cstring>
#include "PushdownInterpreter.hpp"
#include "NdbAggregationCommon.hpp"
#include <AttributeHeader.hpp>   // AggHashTable's template methods use this
#include "AggHashTable.hpp"      // MemChunk, GBColTypeInfo, MEM_CHUNK_SIZE
#include "Dbtup.hpp"             // Dbtup::KeyReqStruct (nested) — needed by initGBTypes
#include "decimal.h"

/* Step 3a-A — shared by both interpreters; was duplicated as `#define`
 * in each subclass header. */
#ifndef ATTR_READ_BUF_WORD_SIZE
#define ATTR_READ_BUF_WORD_SIZE 2048
#endif

/**
 * AggInterpreterBase — shared base for AggInterpreter (normal-scan
 * aggregation) and JoinAggInterpreter (join / CTE aggregation).
 *
 * Step 1 of the interpreter unification (see
 * claude_files/pushdown_join_aggregation/agg_interpreter_unification_plan.md):
 * holds the numeric / type aggregation kernels that were previously
 * duplicated verbatim as file-static functions in both AggInterpreter.cpp
 * and JoinAggInterpreter.cpp.  They were verified to be logically identical
 * (differing only in cosmetics and the debug-only DEBUG_PA_INTERP trace
 * blocks), so a single canonical copy lives here.
 *
 * These kernels are pure functions over Register / AggResItem and carry no
 * interpreter state, so they are protected static methods; both subclasses
 * call them via ordinary inherited name lookup from their ProcessRec opcode
 * loops (every call site is inside a member function, so no qualification is
 * needed).
 *
 * Later steps of the plan will lift the shared fields, the string MIN/MAX
 * sidecar, and the opcode executor into this base as well.
 */
class AggInterpreterBase : public PushdownInterpreter {
 public:
  /* Shared between AggInterpreter and JoinAggInterpreter — both
   * subclasses used to #define DECIMAL_BUFF_LENGTH 9 in their own
   * headers; centralized here as an in-class constexpr in Step 1.3.
   * Not a macro: my_decimal.h declares its own `static constexpr int
   * DECIMAL_BUFF_LENGTH{9}` and a macro of the same name would
   * collide where both headers are transitively included. */
  static constexpr Uint32 AGG_DECIMAL_BUFF_LENGTH = 9;

  /* Step 2a — chunk-allocator overhead per group record.
   * `GBHashTable<...>::OVERHEAD` defines the same constant; centralized
   * here for the chunk-allocator side that doesn't take a template
   * parameter for bucket count. */
  static constexpr Uint32 GROUP_LINK_OVERHEAD = 24;

  /**
   * Step 3a-B — central teardown: release per-(group, slot) string
   * winner buffers (release_string_results), then free the chunk pages
   * (freeAllChunks), the xfrm scratch buffer, and m_buf_block.  Order
   * matters: the chunk allocator owns the memory that backs the
   * GBHashTable's stored entries, and m_buf_block backs the bucket
   * array itself, so anything that walks live groups must run first.
   */
  ~AggInterpreterBase() override;

  AggInterpreterBase(PushdownType type, Uint32 prog_len,
                     Int64 table_id, Int64 frag_id, Uint32 thread_id)
    : PushdownInterpreter(type, prog_len, table_id, frag_id, thread_id),
      m_prog(nullptr), m_agg_prog_start_pos(0),
      m_n_agg_results(0), m_agg_results(nullptr),
      m_string_results(nullptr), m_current_thread_id(0),
      m_cur_pos(0), m_attr_read_pos(0),
      m_processed_rows(0), m_result_size(0),
      m_n_gb_cols(0), m_gb_cols(nullptr),
      m_gb_map(nullptr), m_n_groups(0),
      m_attr_read_buf(nullptr), m_prog_buf(nullptr),
      m_gb_cols_buf(nullptr), m_agg_results_buf(nullptr),
      m_gb_map_buf(nullptr), m_buf_block(nullptr),
      m_chunks(nullptr), m_chunks_tail(nullptr),
      m_current_chunk(nullptr), m_total_chunk_bytes(0),
      m_memory_budget(0), m_budget_increment(0),
      m_total_available(0),
      m_gb_types(nullptr), m_gb_types_inited(false),
      m_xfrm_buf(nullptr), m_xfrm_buf_len(0) {
    memset(m_decimal_buf, 0,
           sizeof(decimal_digit_t) * AGG_DECIMAL_BUFF_LENGTH);
    m_decimal.buf = m_decimal_buf;
    m_decimal.len = AGG_DECIMAL_BUFF_LENGTH;
  }

  /**
   * Step 3a-B — single-allocation buffer block setup.
   *
   * Allocates m_buf_block from RG_QUERY_MEMORY and carves it into the
   * six per-interpreter buffers (m_attr_read_buf / m_prog_buf /
   * m_gb_cols_buf / m_agg_results_buf / m_gb_map_buf / m_gb_types) in
   * the layout JoinAggInterpreter already used.  Subclasses pass their
   * own sizing:
   *
   *   prog_words           -- m_prog_buf word count (per-program; both
   *                            subclasses right-size to m_prog_len)
   *   n_gb_cols_alloc      -- m_gb_cols_buf / m_gb_types element count
   *                            (right-sized to m_n_gb_cols; pass 0 to
   *                            skip both)
   *   n_agg_results_alloc  -- m_agg_results_buf element count
   *                            (AggInterpreter right-sizes to
   *                            m_n_agg_results; JoinAgg passes
   *                            MAX_AGG_N_RESULTS to keep
   *                            setTotalAggResults flexibility)
   *   alloc_gb_map         -- true to carve a JoinGBHashTable for the
   *                            hash buckets, false to skip (no GB)
   *   extra_tail_bytes     -- additional bytes the subclass wants
   *                            appended (JoinAgg uses this for
   *                            m_cached_agg_ops at MAX size)
   *
   * On success returns the pointer to the start of the extra-tail
   * region (or one-past-end when extra_tail_bytes == 0); on
   * allocation failure returns nullptr.  m_buf_block / m_attr_read_buf
   * / m_prog_buf / m_gb_cols_buf / m_agg_results_buf / m_gb_map_buf /
   * m_gb_types are populated as a side effect.
   */
  char* initBufBlock(Uint32 prog_words,
                     Uint32 n_gb_cols_alloc,
                     Uint32 n_agg_results_alloc,
                     bool alloc_gb_map,
                     Uint32 extra_tail_bytes);

  /* Step 2a — chunk allocator (lifted from JoinAggInterpreter).
   * Group records live in MEM_CHUNK_SIZE pages allocated from
   * RG_QUERY_MEMORY; allocGroupData carves from the current chunk,
   * freeGroupData decrements the chunk's live count and releases the
   * page when it hits zero.  Same shape both interpreters end up using
   * in Step 2b. */
  void initChunkAllocator(Uint32 thread_id, Uint32 budget_pages,
                          Uint32 available_pages);
  bool bookMoreMemory();
  MemChunk* allocNewChunk();
  char* allocGroupData(Uint32 len, Uint32 key_len);
  void freeGroupData(char* ptr);
  void freeAllChunks();

  /**
   * OptimizeProgram — guard + delegate to OptimizeProgramBuffer.
   *
   * Step 1.2 of the interpreter unification: this used to live as a
   * byte-identical 7-line method in each subclass.  Both subclasses now
   * inherit the single definition here (non-virtual; callers reach it
   * through an AggInterpreter or JoinAggInterpreter pointer via ordinary
   * inheritance).
   */
  bool OptimizeProgram();

  /**
   * executeStandardOpcode — shared opcode handler for the
   * container-independent arms of the ProcessRec dispatch loop.
   *
   * Step 1.4 of the interpreter unification.  Handles 28 opcodes
   * whose bodies were byte-identical between AggInterpreter and
   * JoinAggInterpreter:
   *   - Generic arithmetic: kOpPlus / Minus / Mul / Div / DivInt / Mod
   *   - Typed arithmetic: kOpPlusBigint / PlusDouble / MinusBigint /
   *     MinusDouble / MulBigint / MulDouble / DivDouble / DivIntBigint
   *   - Aggregate-accumulate: kOpSum / SumBigint / SumDouble /
   *     Max / MaxBigint / MaxDouble / Min / MinBigint / MinDouble / Count
   *   - Misc: kOpLoadConst / kOpMov / kOpSetRegNull / kOpSkip
   *
   * The two divergent opcodes — `kOpLoadCol` (linked-attr / CTE /
   * NULL-injection in JoinAgg) and `kOpEmbeddedInterp` (req_struct
   * linked-attr setup in JoinAgg) — stay in each subclass's own
   * dispatch.  Per the maintainer: AggInterpreter must remain free
   * of linked-column machinery; subclasses use "different jump
   * tables" rather than a shared dispatch with dead branches.
   *
   * Verbose `DEB_AGG(...)` and `PA_INTERP_TRACE(...)` traces from
   * AggInterpreter's original form are preserved (JoinAgg gains
   * debug coverage in debug builds; production builds compile both
   * out via the standard DEB_AGG guard).
   *
   * Parameters
   *   op            (in)     decoded opcode byte
   *   value         (in)     the instruction word that op came from
   *   exec_pos      (in/out) program counter — bumped by kOpLoadConst
   *                          (+2 words) and kOpSkip (variable)
   *   agg_res_ptr   (in)     aggregate-slot base for the current row
   *   debug_print   (in)     PA_INTERP_TRACE gate from the caller
   *   handled       (out)    true if `op` matched one of the 28 arms
   * Returns 0 on success, a positive `ZAGG_*` error code on error.
   * The return value is meaningful only when `*handled == true`.
   */
  Int32 executeStandardOpcode(Uint8 op, Uint32 value, Uint32& exec_pos,
                               AggResItem* agg_res_ptr, bool debug_print,
                               bool* handled);

  /* Phase I.6 (F.2-K.5) string MIN/MAX helpers — bodies in
   * AggInterpreterBase.cpp.  Step 1.3 of the interpreter unification.
   * Operate on the lifted m_string_results / m_register_string_data /
   * m_registers fields below; the per-class destructor still owns the
   * top-level release_string_results because the group container
   * iteration differs (std::map vs GBHashTable).
   *
   * Public because DBLQH (DblqhMain.cpp wire-format emit + group eviction
   * paths) calls hasStringSlots / stringPayloadSize / encodeStringPayload
   * / string_results through a JoinAggInterpreter pointer; the internal
   * helpers (minMaxString, freeGroupStringSlots) are also exposed here
   * for symmetry — every call site is inside an aggregation-aware
   * caller. */
  Int32 minMaxString(Uint32 reg_index, Uint32 agg_index,
                     AggResItem* agg_res_ptr, bool is_max);
  void freeGroupStringSlots(AggResItem* slots);
  /* Step 3a-A: walk m_agg_results (scalar) + every group in m_gb_map,
   * freeing per-(group, slot) string winner buffers, then free the
   * m_string_results metadata array.  Adopts JoinAgg's defensive
   * `m_agg_results != nullptr` check.  Called by each subclass'
   * destructor before the group container itself is destroyed. */
  void release_string_results();
  Uint32 stringPayloadSize(const AggResItem* slots) const;
  Uint32 encodeStringPayload(const AggResItem* slots, char* dst) const;
  bool hasStringSlots() const { return m_string_results != nullptr; }
  const StringResult* string_results() const { return m_string_results; }

 protected:

  /**
   * validateEmbeddedProgram — sanity-check an embedded program at
   * decode time.
   *
   * Step 1.2 of the interpreter unification: previously duplicated in
   * each subclass with subtly different rigor (AggInterpreter only
   * bounds-checked branch targets; JoinAggInterpreter additionally
   * enforced an opcode allow-list and rejected backward branches).  The
   * stricter JoinAgg form is adopted here for both — the allow-list
   * covers every opcode either path emits, and the backward-branch
   * reject closes a potential infinite-loop class.  Pure function over
   * arguments; no instance state needed.
   */
  static bool validateEmbeddedProgram(const Uint32* emb_prog, Uint32 emb_len);

  /* Shared aggregation kernels — definitions in AggInterpreterBase.cpp.
   * `print` is consumed only inside DEBUG_PA_INTERP debug-trace blocks. */
  static bool TypeSupported(DataType type);
  static bool IsUnsigned(DataType type);
  static DataType AlignedType(DataType type, int scale);
  [[maybe_unused]] static void PrintValue(const AggResItem* res, char* log_buf);
  static Int32 Sum(const Register& a, AggResItem* res, bool print);
  static Int32 SumBigint(const Register& a, AggResItem* res, bool print);
  static Int32 SumDouble(const Register& a, AggResItem* res, bool print);
  static Int32 Max(const Register& a, AggResItem* res, bool print);
  static Int32 MaxBigint(const Register& a, AggResItem* res, bool print);
  static Int32 MaxDouble(const Register& a, AggResItem* res, bool print);
  static Int32 Min(const Register& a, AggResItem* res, bool print);
  static Int32 MinBigint(const Register& a, AggResItem* res, bool print);
  static Int32 MinDouble(const Register& a, AggResItem* res, bool print);
  static Int32 Count(const Register& a, AggResItem* res, bool print);

  /* Fields lifted from the subclasses in Step 1.2 to support the shared
   * OptimizeProgram.  Total sizeof is unchanged — same fields, moved up
   * the class hierarchy — so both static_asserts on subclass sizeof
   * still hold. */
  Uint32* m_prog;
  Uint32 m_agg_prog_start_pos;

  /* Fields lifted in Step 1.3 to support the shared string MIN/MAX
   * helpers (minMaxString / freeGroupStringSlots / stringPayloadSize /
   * encodeStringPayload) and the decimal scratch buffer used by the
   * Init / ProcessRec decimal paths.  Same total sizeof as before. */
  Uint32 m_n_agg_results;
  AggResItem* m_agg_results;
  Register m_registers[kRegTotal];

  // Phase I.6 (F.2-K.4a): per-register string scratch.  When a
  // kOpLoadCol arm reads a CHAR / VARCHAR / Longvarchar column, it
  // also stashes (ptr-into-m_attr_read_buf, length, prefix_bytes,
  // declared_size, charset) here for the matching register so that a
  // subsequent kOpMin / kOpMax can compare and copy without
  // re-walking the AttributeDescriptor.
  StringResult m_register_string_data[kRegTotal];

  // Phase I.6 (F.2): per-slot string MIN/MAX sidecar.  Lazily
  // allocated (via lc_ndbd_pool_malloc) on the first row that
  // populates a string-typed slot; sized to m_n_agg_results entries.
  // Stays nullptr for programs with no string MIN/MAX so non-string
  // queries pay zero memory cost.  Freed via the subclass'
  // release_string_results() in the destructor — entries' own ptrs
  // are freed first via the shared freeGroupStringSlots helper, then
  // the array itself.
  StringResult* m_string_results;

  // Per-thread context for lc_ndbd_pool_malloc; set on each ProcessRec
  // entry by the subclass before the helpers can run.
  Uint32 m_current_thread_id;

  // Decimal decode scratch — used by kOpLoadCol DECIMAL arms in both
  // subclasses' opcode executors (the shared executor lands in 1.4).
  decimal_t m_decimal;
  decimal_digit_t m_decimal_buf[AGG_DECIMAL_BUFF_LENGTH];

  /* Step 3a-A — shared per-row + per-program scratch / accounting.
   * Both subclasses used these fields identically; lifted in 3a-A as
   * a zero-behavior-change consolidation. */
  Uint32 m_cur_pos;          // program-counter during Init parsing
  Uint32 m_attr_read_pos;    // position in m_attr_read_buf during GB key read
  Uint64 m_processed_rows;   // rows seen by ProcessRec
  Uint32 m_result_size;      // bytes accumulated since last drain — drives
                             // the per-batch streaming flush

  /* Step 3a-A — wire-format header sizes used by both interpreters.
   * Definitions live in AggInterpreterBase.cpp (each subclass used to
   * carry its own identical copies). */
  static Uint32 g_attr_read_buf_len_;
  static Uint32 g_result_header_size_;
  static Uint32 g_result_header_size_per_group_;

  /* Step 2b — shared GROUP BY column metadata + group store.
   * Both subclasses use the 1024-bucket JoinGBHashTable variant; the
   * storage backing `m_gb_map` is owned by the subclass (inline
   * buffer in AggInterpreter, m_buf_block-carved in
   * JoinAggInterpreter) and the pointer is set up at Init time. */
  Uint32 m_n_gb_cols;
  Uint32* m_gb_cols;
  JoinGBHashTable* m_gb_map;
  Uint32 m_n_groups;

  /* Step 2b — shared GROUP BY type-metadata initializer.
   * Resolves AttributeDescriptor / linked-attr metadata for each GB
   * column into m_gb_types[], allocates m_xfrm_buf if any column has
   * a charset, and publishes the metadata to m_gb_map via setTypeMeta.
   *
   * linked_attr_data / linked_attr_len are the per-row linked-attr
   * buffer JoinAgg passes in for join queries.  AggInterpreter
   * (normal scan) passes nullptr / 0; the linked-attr resolution
   * branches are dead code on that path (the JoinAgg-only attr_id
   * bit 0x8000 never appears in normal-scan GB cols). */
  Int32 initGBTypes(Dbtup* block_tup, Dbtup::KeyReqStruct* req_struct,
                    const Uint32* linked_attr_data,
                    Uint32 linked_attr_len);

  /* Step 3a-B — m_buf_block-resident pointer buffers.
   * `initBufBlock` allocates one combined block from RG_QUERY_MEMORY
   * and carves it into these slots.  The subclass-specific sizing
   * lives in each subclass' Init; the layout (`m_attr_read_buf →
   * m_prog_buf → m_gb_cols_buf → m_agg_results_buf → m_gb_map_buf →
   * m_gb_types → extra-tail`) is shared.  AggInterpreter doesn't use
   * a tail; JoinAgg appends its `m_cached_agg_ops` merge cache. */
  Uint32* m_attr_read_buf;
  Uint32* m_prog_buf;
  Uint32* m_gb_cols_buf;
  AggResItem* m_agg_results_buf;
  JoinGBHashTable* m_gb_map_buf;
  void* m_buf_block;

  /* Step 2a — chunk allocator state lifted from JoinAggInterpreter.
   * MEM_CHUNK_SIZE pages are allocated lazily on first allocGroupData;
   * groups within a chunk form a singly-linked list via the chunk's
   * group_list head for O(1) eviction.  AggInterpreter doesn't engage
   * the chunk allocator until Step 2b — these fields stay
   * zero-initialized for normal-scan aggregation until then. */
  MemChunk* m_chunks;
  MemChunk* m_chunks_tail;
  MemChunk* m_current_chunk;
  Uint32 m_total_chunk_bytes;
  Uint32 m_memory_budget;
  Uint32 m_budget_increment;
  Uint32 m_total_available;

  /* Step 2a — GROUP BY per-column type metadata.  Populated by
   * initGBTypes (still per-class in 2a; lifted in 2b).  The
   * GBHashTable uses these for charset-aware hash + comparison. */
  GBColTypeInfo* m_gb_types;
  bool m_gb_types_inited;
  uchar* m_xfrm_buf;       // scratch buffer for strnxfrm_hash
  Uint32 m_xfrm_buf_len;   // size in bytes
};

#endif  // AGGINTERPRETERBASE_H_
