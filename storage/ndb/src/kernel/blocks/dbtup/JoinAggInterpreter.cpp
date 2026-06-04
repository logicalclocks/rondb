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
    char* tail = initBufBlock(
        /*prog_words=*/m_prog_len,
        /*n_gb_cols_alloc=*/m_n_gb_cols,
        /*n_agg_results_alloc=*/MAX_AGG_N_RESULTS,
        /*alloc_gb_map=*/true,
        /*extra_tail_bytes=*/MAX_AGG_N_RESULTS * sizeof(Uint8));
    if (tail == nullptr) {
      g_eventLogger->error("Alloc mem for JoinAggInterpreter buffers failed");
      return false;
    }
    m_cached_agg_ops = reinterpret_cast<Uint8*>(tail);
  }

  /* Common post-allocation steps. */
  initSharedAfterAlloc(prog);

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
        case kOpLoadCol: {
          Uint32 type = (word & 0x03E00000) >> 21;
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
  m_n_agg_results = total;

  // Re-initialize the non-GROUP-BY accumulator array for the new total
  m_agg_results = m_agg_results_buf;
  for (Uint32 i = 0; i < m_n_agg_results; i++) {
    m_agg_results[i].type = NDB_TYPE_UNDEFINED;
    m_agg_results[i].value.val_int64 = 0;
    m_agg_results[i].is_unsigned = false;
    m_agg_results[i].is_null = true;
  }
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
          Uint32 type = (value & 0x03E00000) >> 21;
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
        EmulatedJamBuffer *jamBuf) {
  m_current_thread_id = thread_id;
  /* Step 3 Cand-C: bind m_attr_read_buf to the calling LDM thread's
   * Dbtup scratch buffer.  processNullExtendedRow / processRecWithLinkedAttrs
   * both pass block_tup; the buffer is per-thread so MUTEX_BASED's
   * shared interpreter sees the calling thread's buffer on each entry. */
  require(block_tup != nullptr);
  m_attr_read_buf = block_tup->getAggAttrReadBuf();
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
        initGBTypesForNullLocal(block_tup, jamBuf);
      } else {
        thrjam(jamBuf);
        Int32 err = initGBTypes(block_tup,
                                req_struct,
                                m_linked_attr_data,
                                m_linked_attr_len,
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
    char* found = m_gb_map->find(reinterpret_cast<char*>(m_attr_read_buf), len_in_char);
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

      m_gb_map->insert(agg_rec, len_in_char);
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
    value = m_prog[exec_pos++];
    Uint8 op = (value & 0xFC000000) >> 26;
    int ret = 0;
    m_attr_read_pos = 0;
    AttributeHeader* header = nullptr;

    switch (op) {
      case kOpLoadCol: {
        type = (value & 0x03E00000) >> 21;
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
          if (p >= p_end) {
            g_eventLogger->debug("JoinAggInterpreter::ProcessRec ZAGG_OTHER_ERROR: "
                "kOpLoadCol linked position %u not found in buffer "
                "(linked_len=%u)", position, m_linked_attr_len);
            return ZAGG_OTHER_ERROR;
          }
          linked_word0 = p[0];
          linked_word1 = p[1];
          linked_cte_attr = CteLinkedAttr::isCteMarker(linked_word0);
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
          /* Normal (non-linked) column load.  A CTE agg feed has no scanned
           * table (tablePtrP == nullptr); reaching here means the
           * aggregation program references a table column that cannot be
           * supplied — abort cleanly (see initGBTypes). */
          if (unlikely(req_struct == nullptr ||
                       req_struct->tablePtrP == nullptr)) {
            g_eventLogger->debug(
                "JoinAggInterpreter::ProcessRec kOpLoadCol: normal column %u "
                "referenced in a CTE agg-feed with no scanned table — "
                "aborting query", col_id_raw);
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
          attrDescriptor =
              req_struct->tablePtrP->tabDescriptor + (col_id_raw * ZAD_SIZE);
          assert(header->getAttributeId() == col_id_raw);
          assert(type == AttributeDescriptor::getType(attrDescriptor[0]));
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
  DEB_CTE(("(0x%p)->m_processed_rows = %llu, ProcessRec",
    this, m_processed_rows));
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

  Int32 ret = ProcessRec(block_tup, req_struct, thread_id, jamBuf);

  m_null_local_columns = false;
  m_linked_attr_data = nullptr;
  m_linked_attr_len = 0;
  return ret;
}

Int32 JoinAggInterpreter::evictOneGroup(Uint32* buf, Uint32 buf_words,
                                         Uint32* words_written) {
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
  m_gb_map->erase(data_ptr, key_len);
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
    switch (agg_ops[i]) {
      case kOpSum: case kOpSumBigint: case kOpSumDouble:
        if (dst[i].type == NDB_TYPE_BIGINT) {
          if (dst[i].is_unsigned) dst[i].value.val_uint64 += src[i].value.val_uint64;
          else dst[i].value.val_int64 += src[i].value.val_int64;
        } else {
          dst[i].value.val_double += src[i].value.val_double;
        }
        break;
      case kOpCount:
        dst[i].value.val_uint64 += src[i].value.val_uint64;
        break;
      case kOpMax: case kOpMaxBigint: case kOpMaxDouble:
        if (dst[i].type == NDB_TYPE_BIGINT) {
          if (dst[i].is_unsigned) {
            if (src[i].value.val_uint64 > dst[i].value.val_uint64)
              dst[i].value.val_uint64 = src[i].value.val_uint64;
          } else {
            if (src[i].value.val_int64 > dst[i].value.val_int64)
              dst[i].value.val_int64 = src[i].value.val_int64;
          }
        } else {
          if (src[i].value.val_double > dst[i].value.val_double)
            dst[i].value.val_double = src[i].value.val_double;
        }
        break;
      case kOpMin: case kOpMinBigint: case kOpMinDouble:
        if (dst[i].type == NDB_TYPE_BIGINT) {
          if (dst[i].is_unsigned) {
            if (src[i].value.val_uint64 < dst[i].value.val_uint64)
              dst[i].value.val_uint64 = src[i].value.val_uint64;
          } else {
            if (src[i].value.val_int64 < dst[i].value.val_int64)
              dst[i].value.val_int64 = src[i].value.val_int64;
          }
        } else {
          if (src[i].value.val_double < dst[i].value.val_double)
            dst[i].value.val_double = src[i].value.val_double;
        }
        break;
      default:
        assert(0);
        break;
    }
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
                          Uint8* agg_ops, Uint32 n_agg_results) {
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
      case kOpLoadCol: {
        Uint32 type = (value & 0x03E00000) >> 21;
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
                                      Uint32 max_groups) {
  assert(other != nullptr);
  assert(m_n_agg_results == other->m_n_agg_results);

  if (ensureStringResultsFrom(other->m_string_results) != 0) {
    return 0;
  }

  if (!m_agg_ops_cached) {
    extractAggOps(m_prog, m_prog_len, m_agg_prog_start_pos,
                  m_cached_agg_ops, m_n_agg_results);
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
        m_gb_map->insertRaw(other_data);
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
                                         Uint32 accLen) {
  /* Phase I.17e: scalar (no GROUP BY) redistribute reuses this entry
   * point with keyLen == 0 — dispatch to the accumulator-only merge. */
  if (keyLen == 0) {
    return mergeScalarAccumulators(accumulators, accLen);
  }
  if (m_gb_map == nullptr) return -1;

  if (!m_agg_ops_cached) {
    extractAggOps(m_prog, m_prog_len, m_agg_prog_start_pos,
                  m_cached_agg_ops, m_n_agg_results);
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
  char* found = m_gb_map->find(key, keyLen);

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

    m_gb_map->insert(new_group, keyLen);
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
                  m_cached_agg_ops, m_n_agg_results);
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
  return ret;
}
void JoinAggInterpreter::initGBTypesForNullLocal(Dbtup* block_tup,
                                                 EmulatedJamBuffer *jamBuf) {
  /*
   * Called when the first row is a null-extended row (m_null_local_columns).
   * Linked columns: resolve type from DBTUP tablerec (same as initGBTypes).
   * Local columns: use NDB_TYPE_UNSIGNED as placeholder — all values will
   * be NULL (data size 0), so the actual type doesn't affect comparison.
   * If a matched row arrives later, types are already initialized.
   */
  for (Uint32 i = 0; i < m_n_gb_cols; i++) {
    Uint32 attr_id = m_gb_cols[i] >> 16;
    thrjamDebug(jamBuf);
    thrjamDataDebug(jamBuf, attr_id);
    GBColTypeInfo &info = m_gb_types[i];

    if ((attr_id & 0x8000) != 0 && m_linked_attr_data != nullptr) {
      thrjamDebug(jamBuf);
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
            info.cs = all_charsets[csNumber];
          }
        } else if (block_tup != nullptr) {
          thrjamDebug(jamBuf);
          Uint32 tableId = word0;
          if (tableId != 0 && tableId < block_tup->cnoOfTablerec) {
            thrjamDebug(jamBuf);
            Dbtup::Tablerec* tab = &block_tup->tablerec[tableId];
            Uint32 linkedAttrId = AttributeHeader(p[2]).getAttributeId();
            const Uint32* attrDesc = tab->tabDescriptor +
                linkedAttrId * ZAD_SIZE;
            info.typeId = AttributeDescriptor::getType(attrDesc[0]);
            info.maxBytes = AttributeDescriptor::getSizeInBytes(attrDesc[0]);
            info.cs = nullptr;
            if (AttributeOffset::getCharsetFlag(attrDesc[1])) {
              thrjamDebug(jamBuf);
              Uint32 csPos = AttributeOffset::getCharsetPos(attrDesc[1]);
              info.cs = tab->charsetArray[csPos];
            }
          } else {
            thrjamDebug(jamBuf);
            info.typeId = NDB_TYPE_UNSIGNED;
            info.maxBytes = 4;
            info.cs = nullptr;
          }
        } else {
          thrjamDebug(jamBuf);
          info.typeId = NDB_TYPE_UNSIGNED;
          info.maxBytes = 4;
          info.cs = nullptr;
        }
      } else {
        thrjamDebug(jamBuf);
        info.typeId = NDB_TYPE_UNSIGNED;
        info.maxBytes = 4;
        info.cs = nullptr;
      }
    } else {
      thrjamDebug(jamBuf);
      info.typeId = NDB_TYPE_UNSIGNED;
      info.maxBytes = 4;
      info.cs = nullptr;
    }
    const NdbSqlUtil::Type &sqlType = NdbSqlUtil::getType(info.typeId);
    info.cmpFn = sqlType.m_cmp;
  }
  m_gb_types_inited = true;
  m_gb_map->setTypeMeta(m_gb_types, m_n_gb_cols, m_xfrm_buf, m_xfrm_buf_len);
}

Int32 JoinAggInterpreter::processNullExtendedRow(
    Dbtup* block_tup,
    const Uint32* linked_attr_data,
    Uint32 linked_attr_len,
    Uint32 thread_id,
    EmulatedJamBuffer *jamBuf,
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

  /* Step 3 Cand-C: block_tup is needed inside ProcessRec to bind
   * m_attr_read_buf to the per-LDM-thread scratch on Dbtup.  The
   * null-extended row path still passes req_struct=nullptr (no row
   * data to read) — m_null_local_columns drives kOpLoadCol to
   * synthesise NULL AttributeHeaders into m_attr_read_buf instead. */
  Int32 ret = ProcessRec(block_tup, nullptr, thread_id, jamBuf);

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


