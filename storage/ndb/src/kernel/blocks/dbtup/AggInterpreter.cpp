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

#include <cstdint>
#include <cstring>
#include <utility>

#define DBTUP_C
#include "signaldata/TransIdAI.hpp"
#include "include/my_byteorder.h"
#include "AggInterpreter.hpp"
#include "InterpreterCommonOp.hpp"
#include "util/require.h"
#include "decimal.h"
#include "Dbtup.hpp"
#include <NdbSqlUtil.hpp>
#include <Interpreter.hpp>

// g_attr_read_buf_len_ / g_result_header_size_ /
// g_result_header_size_per_group_ definitions moved to
// AggInterpreterBase.cpp in Step 3a-A.

/*
 * PA related
 * Turn on the DEBUG_PA_INTERP
 * to trace AggInterpreter on partition DEBUG_PA_INTERP_PART_ID
 */
#if (defined(VM_TRACE) || defined(ERROR_INSERT))
#undef DEBUG_PA_INTERP
// #define DEBUG_PA_INTERP 1
#define DEBUG_AGG 1
#endif
#define DEBUG_PA_INTERP_PART_ID 0

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
#endif // DEBUG_PA_INTERP

/*
 * VS related
 * Turn on the DEBUG_VS_INTERP
 * to trace AggInterpreter on partition DEBUG_VS_INTERP_PART_ID
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

bool AggInterpreter::Init(const Uint32* prog) {
  if (m_inited) {
    return true;
  }

  require(prog != nullptr);

  /* Step 3a-B: peek header words (magic / prog_len / n_gb_cols /
   * n_agg_results / version / 5 reserved) directly from the input
   * `prog` so we can right-size m_buf_block before copying.  Match
   * the original header parse exactly. */
  Uint32 hdr0 = prog[0];
  assert(((hdr0 & 0xFFFF0000) >> 16) == 0x0721);
  assert((hdr0 & 0xFFFF) == m_prog_len);

  Uint32 hdr1 = prog[1];
  m_n_gb_cols = (hdr1 >> 16) & 0xFFFF;
  m_n_agg_results = hdr1 & 0xFFFF;

  Uint32 version = prog[2];
  if (version > PUSHDOWN_AGGREGATION_VERSION) {
    g_eventLogger->warning("Pushdown aggregation program version(%u) is "
                           "not compatible with "
                           "the version (%u) on data node",
                           version, PUSHDOWN_AGGREGATION_VERSION);
    /* Return with m_inited = false; ProcessRec rejects on entry. */
    return true;
  }
  assert((prog[3] & 0x80000000) == 0);
  assert(prog[3] == 0);

  assert(m_prog_len <= MAX_AGG_PROGRAM_WORD_SIZE);
  assert(m_n_gb_cols <= MAX_AGG_N_GROUPBY_COLS);
  assert(m_n_agg_results <= MAX_AGG_N_RESULTS);

  /* Right-sized buffer-block carve.  Skip the m_gb_map_buf slot when
   * n_gb_cols == 0 — scalar aggregation never touches a hash table.
   * On success initBufBlock returns the one-past-end pointer (we don't
   * append a tail); on allocation failure it returns nullptr. */
  if (initBufBlock(/*prog_words=*/m_prog_len,
                   /*n_gb_cols_alloc=*/m_n_gb_cols,
                   /*n_agg_results_alloc=*/m_n_agg_results,
                   /*alloc_gb_map=*/m_n_gb_cols > 0,
                   /*extra_tail_bytes=*/0) == nullptr) {
    g_eventLogger->error("AggInterpreter::Init: m_buf_block allocation failed");
    return false;
  }

  m_prog = m_prog_buf;
  memcpy(m_prog, prog, m_prog_len * sizeof(Uint32));
  memset(m_attr_read_buf, 0, ATTR_READ_BUF_WORD_SIZE * sizeof(Uint32));
  memset(m_decimal_buf, 0, sizeof(Int32) * AGG_DECIMAL_BUFF_LENGTH);
  m_decimal.buf = m_decimal_buf;
  m_decimal.len = AGG_DECIMAL_BUFF_LENGTH;

  /* Header is 8 words (magic, n_gb_cols/n_agg_results, version, 5 reserved). */
  m_cur_pos = 8;

  if (m_n_gb_cols) {
    m_gb_cols = m_gb_cols_buf;
    Uint32 i = 0;
    while (i < m_n_gb_cols && m_cur_pos < m_prog_len) {
      m_gb_cols[i++] = m_prog[m_cur_pos++];
    }
    /* m_gb_map_buf was placement-new'd by initBufBlock. */
    m_gb_map_buf->clear();
    m_gb_map = m_gb_map_buf;
    m_gb_map->init(JOIN_AGG_HASH_BUCKET_COUNT);
    /* Chunk allocator budget: start small and let bookMoreMemory grow it
     * if a high-cardinality GROUP BY needs more.  available_pages is
     * generous — query memory pool enforces the real cap. */
    initChunkAllocator(/*thread_id=*/0,
                       /*budget_pages=*/1,
                       /*available_pages=*/4096);
  }

  if (m_n_agg_results) {
    m_agg_results = m_agg_results_buf;
    for (Uint32 i = 0; i < m_n_agg_results; i++) {
      m_agg_results[i].type = NDB_TYPE_UNDEFINED;
      m_agg_results[i].value.val_int64 = 0;
      m_agg_results[i].is_unsigned = false;
      m_agg_results[i].is_null = true;
    }
  }

  m_inited = true;
  m_agg_prog_start_pos = m_cur_pos;
  memset(m_registers, 0, sizeof(m_registers));

  /* Validate embedded interpreter blocks (Step 3b — shared helper). */
  if (!scanAndValidateEmbeddedPrograms("AggInterpreter")) {
    return false;
  }
  return true;
}


/*
 * Numeric / type aggregation kernels (TypeSupported, IsUnsigned,
 * AlignedType, PrintValue, Sum/SumBigint/SumDouble, Max/MaxBigint/MaxDouble,
 * Min/MinBigint/MinDouble, Count) live in the shared base class
 * AggInterpreterBase (AggInterpreterBase.{hpp,cpp}) and are reached here via
 * inherited name lookup.  See agg_interpreter_unification_plan.md, Step 1.
 */

/*
 * Success: RETURN 0
 * Failure: RETURN 1860+ by aggregation interpreter
 *          Others returned by readAttributes
 */
Int32 AggInterpreter::ProcessRec(Dbtup* block_tup,
        Dbtup::KeyReqStruct* req_struct,
        Uint32 thread_id) {
  m_current_thread_id = thread_id;
  if (!m_inited || req_struct->read_length != 0) {
    g_eventLogger->debug("AggInterpreter::ProcessRec ZAGG_OTHER_ERROR at entry: "
            "inited=%d, read_length=%u",
            m_inited, req_struct->read_length);
    return ZAGG_OTHER_ERROR;
  }

  AggResItem* agg_res_ptr = nullptr;
  if (m_n_gb_cols) {
    /* Step 2b: resolve GROUP BY column type metadata once.  Normal-scan
     * never has linked-attr columns, so pass nullptr / 0. */
    if (!m_gb_types_inited) {
      Int32 err = initGBTypes(block_tup, req_struct,
                              /*linked_attr_data=*/nullptr,
                              /*linked_attr_len=*/0);
      if (unlikely(err != 0)) return err;
    }

    AttributeHeader* header = nullptr;
    m_attr_read_pos = 0;
    for (Uint32 i = 0; i < m_n_gb_cols; i++) {
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

    /* Step 2b: hash-table find / insert via the shared chunk allocator,
     * mirroring JoinAggInterpreter's group prologue minus the linked /
     * CTE / multi-leaf branches. */
    Uint32 len_in_char = m_attr_read_pos * sizeof(Uint32);
    char* found = m_gb_map->find(
        reinterpret_cast<char*>(m_attr_read_buf), len_in_char);
    if (found != nullptr) {
      agg_res_ptr = reinterpret_cast<AggResItem*>(found + len_in_char);
      PA_INTERP_TRACE(m_frag_id,
                      "Found group, len: %u", len_in_char);
    } else {
      /* read_length update keeps Dblqh::ScanRecord::m_curr_batch_size_bytes
       * tracking accurate even though aggregation uses its own batch
       * accounting via m_agg_curr_batch_size_bytes. */
      req_struct->read_length = (len_in_char +
                       m_n_agg_results * sizeof(AggResItem)) / sizeof(Int32);
      m_result_size += len_in_char +
                       m_n_agg_results * sizeof(AggResItem);
      char* agg_rec = allocGroupData(
          len_in_char + m_n_agg_results * sizeof(AggResItem),
          len_in_char);
      if (agg_rec == nullptr) {
        return ZAGG_OTHER_ERROR;
      }
      memset(agg_rec, 0, len_in_char + m_n_agg_results * sizeof(AggResItem));
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
    }
  } else {
    agg_res_ptr = m_agg_results;
  }

  Uint32 value;
  DataType type;
  bool is_unsigned;
  Uint32 reg_index;
  /* reg_index2 / agg_index used only by shared opcodes — moved to base
   * helper in Step 1.4. */

  const Uint32* attrDescriptor = nullptr;

  Int32 decimal_info = 0;
  Int32 precision = 0;
  Int32 scale = 0;
  Int32 dec_ret = E_DEC_OK;
  Uint8* dec_buf_ptr = nullptr;
  double dec_val_dbl = 0;
  longlong dec_val_ll = 0;
  ulonglong dec_val_ull = 0;

  Uint32 exec_pos = m_agg_prog_start_pos;
  bool debug_print = (m_frag_id == DEBUG_PA_INTERP_PART_ID);
  while (exec_pos < m_prog_len) {
    value = m_prog[exec_pos++];
    Uint8 op = (value & 0xFC000000) >> 26;
    int ret = 0;
    m_attr_read_pos = 0;
    AttributeHeader* header = nullptr;

    switch (op) {
      case kOpLoadCol:
        type = (value & 0x03E00000) >> 21;
        is_unsigned = IsUnsigned(type);
        reg_index = (value & 0x000F0000) >> 16;
        {
          const Uint32 col_id = value & 0x0000FFFF;
          ret = block_tup->readSingleAttribute(
              req_struct, col_id,
              m_attr_read_buf + m_attr_read_pos,
              g_attr_read_buf_len_ - m_attr_read_pos);
          if (ret < 0) {
            DEB_AGG(("read column error: %d", ret));
            return -ret;
          }
          header = reinterpret_cast<AttributeHeader*>(m_attr_read_buf + m_attr_read_pos);
          attrDescriptor =
              req_struct->tablePtrP->tabDescriptor + (col_id * ZAD_SIZE);
          assert(header->getAttributeId() == col_id);
          assert(type == AttributeDescriptor::getType(attrDescriptor[0]));
        }
        if (!TypeSupported(type)) {
          DEB_AGG(("Unsupported column type: %u", type));
          return ZAGG_COL_TYPE_UNSUPPORTED;
        }

        if (type == NDB_TYPE_DECIMAL ||
            type == NDB_TYPE_DECIMALUNSIGNED) {
          if (unlikely(exec_pos >= m_prog_len)) {
            g_eventLogger->debug("AggInterpreter::ProcessRec ZAGG_OTHER_ERROR: "
                "kOpLoadCol DECIMAL overflow exec_pos=%u prog_len=%u",
                exec_pos, m_prog_len);
            return ZAGG_OTHER_ERROR;
          }
          decimal_info =
              sint4korr(reinterpret_cast<char*>(&m_prog[exec_pos++]));
          precision = decimal_info >> 16;
          scale = decimal_info & 0xFFFF;
        } else {
          precision = 0;
          scale = 0;
        }

        ResetRegister(&m_registers[reg_index]);
        m_registers[reg_index].type = AlignedType(type, scale);
        m_registers[reg_index].is_unsigned = is_unsigned;
        m_registers[reg_index].is_null = header->isNULL();
        if (m_registers[reg_index].is_null) {
          // Column has a null value
          PA_INTERP_TRACE(m_frag_id,
                          "Load NULL, type: %u",
                          m_registers[reg_index].type);
          m_registers[reg_index].value.val_int64 = 0;
          break;
        }
        switch (type) {
          case NDB_TYPE_TINYINT:
            m_registers[reg_index].value.val_int64 =
                *reinterpret_cast<Int8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_TINYINT %lld",
                            m_registers[reg_index].value.val_int64);
            break;
          case NDB_TYPE_SMALLINT:
            m_registers[reg_index].value.val_int64 =
                sint2korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_SMALLINT %lld",
                            m_registers[reg_index].value.val_int64);
            break;
          case NDB_TYPE_MEDIUMINT:
            m_registers[reg_index].value.val_int64 =
                sint3korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_MEDIUM %lld",
                            m_registers[reg_index].value.val_int64);
            break;
          case NDB_TYPE_INT:
            m_registers[reg_index].value.val_int64 =
                sint4korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_INT %lld",
                            m_registers[reg_index].value.val_int64);
            break;
          case NDB_TYPE_BIGINT:
            m_registers[reg_index].value.val_int64 =
                sint8korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_BIGINT %lld",
                            m_registers[reg_index].value.val_int64);
            break;
          case NDB_TYPE_TINYUNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                *reinterpret_cast<Uint8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_TINYUNSIGNED %llu",
                            m_registers[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_SMALLUNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                uint2korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_SMALLUNSIGNED %llu",
                            m_registers[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_MEDIUMUNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                uint3korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_MEDIUMUNSIGNED %llu",
                            m_registers[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_UNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                uint4korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_UNSIGNED %llu",
                            m_registers[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_BIGUNSIGNED:
            m_registers[reg_index].value.val_uint64 =
                uint8korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_BIGUNSIGNED %llu",
                            m_registers[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_FLOAT:
            m_registers[reg_index].value.val_double =
                floatget(reinterpret_cast<unsigned char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_FLOAT %lf",
                            m_registers[reg_index].value.val_double);
            break;
          case NDB_TYPE_DOUBLE:
            m_registers[reg_index].value.val_double =
                doubleget(reinterpret_cast<unsigned char*>(
                      &m_attr_read_buf[m_attr_read_pos + 1]));
            PA_INTERP_TRACE(m_frag_id,
                            "Load NDB_TYPE_DOUBLE %lf",
                            m_registers[reg_index].value.val_double);
            break;
          case NDB_TYPE_DECIMAL:
            assert(static_cast<Uint32>(decimal_bin_size(precision, scale)) ==
                header->getByteSize());
            // memset(decimal.buf, 0, sizeof(Int32) * DECIMAL_BUFF_LENGTH);
            dec_ret = bin2decimal(reinterpret_cast<const uchar*>(&m_attr_read_buf[m_attr_read_pos + 1]),
                      &m_decimal, precision, scale);
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while parsing decimal: ");
              for (Uint32 i = 0;
                  i < header->getByteSize(); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              DEB_AGG(("%s", log_buf));
              if (dec_ret == E_DEC_OVERFLOW) {
                return ZAGG_DECIMAL_PARSE_OVERFLOW;
              } else {
                return ZAGG_DECIMAL_PARSE_ERROR;
              }
            }
            /*
             * Moz
             * convert from decimal to double or bigint.
             */
            assert(m_registers[reg_index].is_unsigned == false);
            if (scale != 0) {
              assert(m_registers[reg_index].type == NDB_TYPE_DOUBLE);
              dec_ret = decimal2double(&m_decimal, &dec_val_dbl);
              m_registers[reg_index].value.val_double = dec_val_dbl;
            } else {
              assert(m_registers[reg_index].type == NDB_TYPE_BIGINT);
              dec_ret = decimal2longlong(&m_decimal, &dec_val_ll);
              m_registers[reg_index].value.val_int64 = dec_val_ll;
            }
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while converting decimal: ");
              for (Uint32 i = 0;
                  i < header->getByteSize(); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              DEB_AGG(("%s", log_buf));
              if (dec_ret == E_DEC_OVERFLOW) {
                return ZAGG_DECIMAL_CONV_OVERFLOW;
              } else {
                return ZAGG_DECIMAL_CONV_ERROR;
              }
            }
#ifdef DEBUG_PA_INTERP
            if (scale != 0) {
              PA_INTERP_TRACE(m_frag_id,
                              "Load NDB_TYPE_DECIMAL[double] %lf",
                              m_registers[reg_index].value.val_double);
            } else {
              PA_INTERP_TRACE(m_frag_id,
                              "Load NDB_TYPE_DECIMAL[int64] %lld",
                              m_registers[reg_index].value.val_int64);
            }
#endif // DEBUG_PA_INTERP
          break;
        case NDB_TYPE_DECIMALUNSIGNED:
            assert(static_cast<Uint32>(decimal_bin_size(precision, scale)) ==
                header->getByteSize());
            // memset(decimal.buf, 0, sizeof(Int32) * DECIMAL_BUFF_LENGTH);
            dec_ret = bin2decimal(reinterpret_cast<const uchar*>(&m_attr_read_buf[m_attr_read_pos + 1]),
                      &m_decimal, precision, scale);
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while parsing decimal: ");
              for (Uint32 i = 0;
                  i < header->getByteSize(); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              DEB_AGG(("%s", log_buf));
              if (dec_ret == E_DEC_OVERFLOW) {
                return ZAGG_DECIMAL_PARSE_OVERFLOW;
              } else {
                return ZAGG_DECIMAL_PARSE_ERROR;
              }
            }
            /*
             * Moz
             * convert from decimal unsigned to double or bigint.
             */
            assert(m_registers[reg_index].is_unsigned == true);
            if(unlikely(m_decimal.sign)) {
              return ZAGG_DECIMAL_CONV_ERROR;
            }
            if (scale != 0) {
              assert(m_registers[reg_index].type == NDB_TYPE_DOUBLE);
              dec_ret = decimal2double(&m_decimal, &dec_val_dbl);
              m_registers[reg_index].value.val_double = dec_val_dbl;
            } else {
              assert(m_registers[reg_index].type == NDB_TYPE_BIGINT);
              dec_ret = decimal2ulonglong(&m_decimal, &dec_val_ull);
              m_registers[reg_index].value.val_uint64 = dec_val_ull;
            }
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while converting decimal: ");
              for (Uint32 i = 0;
                  i < header->getByteSize(); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              DEB_AGG(("%s", log_buf));
              if (dec_ret == E_DEC_OVERFLOW) {
                return ZAGG_DECIMAL_CONV_OVERFLOW;
              } else {
                return ZAGG_DECIMAL_CONV_ERROR;
              }
            }
#ifdef DEBUG_PA_INTERP
            if (scale != 0) {
              PA_INTERP_TRACE(m_frag_id,
                              "Load NDB_TYPE_DECIMALUNSIGNED[double] %lf",
                              m_registers[reg_index].value.val_double);
            } else {
              PA_INTERP_TRACE(m_frag_id,
                              "Load NDB_TYPE_DECIMALUNSIGEND[uint64] %llu",
                              m_registers[reg_index].value.val_uint64);
            }
#endif // DEBUG_PA_INTERP
          break;

          case NDB_TYPE_CHAR:
          case NDB_TYPE_VARCHAR:
          case NDB_TYPE_LONGVARCHAR: {
            // Phase I.6 (F.2-K.4b): stash a read-only view into
            // m_attr_read_buf for a subsequent kOpMin / kOpMax to
            // compare and copy without re-walking the AttributeDescriptor.
            // The register's val_int64 is unused on the string path —
            // dispatch goes through m_registers[reg].type to the
            // MaxString / MinString helpers.
            const Uint32 TattrDesc1 = attrDescriptor[0];
            const Uint32 TattrDesc2 = attrDescriptor[1];
            const CHARSET_INFO* cs = nullptr;
            if (AttributeOffset::getCharsetFlag(TattrDesc2)) {
              const Uint32 pos = AttributeOffset::getCharsetPos(TattrDesc2);
              cs = req_struct->tablePtrP->charsetArray[pos];
            }
            const Uint32 declared =
                AttributeDescriptor::getSizeInBytes(TattrDesc1);
            const Uint16 prefix =
                (type == NDB_TYPE_CHAR) ? 0 :
                (type == NDB_TYPE_VARCHAR) ? 1 : 2;
            char* base = reinterpret_cast<char*>(
                &m_attr_read_buf[m_attr_read_pos + 1]);
            Uint16 payload_len;
            if (type == NDB_TYPE_CHAR) {
              payload_len = static_cast<Uint16>(declared);
            } else if (type == NDB_TYPE_VARCHAR) {
              payload_len = static_cast<Uint16>(
                  static_cast<Uint8>(base[0]));
            } else {
              payload_len = static_cast<Uint16>(
                  static_cast<Uint8>(base[0]) |
                  (static_cast<Uint16>(static_cast<Uint8>(base[1])) << 8));
            }
            StringResult& sr = m_register_string_data[reg_index];
            sr.ptr = base;
            sr.length = payload_len;
            sr.size = 0;
            sr.prefix_bytes = prefix;
            sr.declared_size = static_cast<Uint16>(declared);
            sr.charset = cs;
            m_registers[reg_index].value.val_int64 = 0;
            // Advance m_attr_read_pos past the AttributeHeader + string
            // bytes so the next kOpLoadCol doesn't overwrite the
            // captured ptr.  Numeric paths leave m_attr_read_pos at 0
            // (they extract into the register's 8-byte union and the
            // buffer is then disposable); strings need the buffer to
            // stay live until kOpMax / kOpMin runs minMaxString.
            {
              const Uint32 string_bytes = prefix + payload_len;
              const Uint32 words_consumed = 1 /*AttributeHeader*/ +
                                            ((string_bytes + 3) >> 2);
              if (unlikely(m_attr_read_pos + words_consumed >
                           g_attr_read_buf_len_)) {
                g_eventLogger->debug("AggInterpreter::ProcessRec "
                    "ZAGG_OTHER_ERROR: string attr buffer overflow "
                    "pos=%u words=%u buf_words=%u",
                    m_attr_read_pos, words_consumed, g_attr_read_buf_len_);
                return ZAGG_OTHER_ERROR;
              }
              m_attr_read_pos += words_consumed;
            }
            break;
          }
          default:
            return ZAGG_LOAD_COL_WRONG_TYPE;
        }
        break;
      case kOpEmbeddedInterp:
      {
        Uint32 emb_len = value & 0xFFFF;
        if (exec_pos + emb_len > m_prog_len) {
          g_eventLogger->debug("AggInterpreter::ProcessRec ZAGG_OTHER_ERROR: "
              "embedded interp len overflow exec_pos=%u emb_len=%u "
              "prog_len=%u", exec_pos, emb_len, m_prog_len);
          return ZAGG_OTHER_ERROR;
        }

        /* Save and reset instruction counter (interpreterJumpTable
         * asserts it is 0 on entry). */
        Uint32 saved_instr_count = req_struct->no_exec_instructions;
        req_struct->no_exec_instructions = 0;

        /* Local tmp buffer — unused by the accepted agg-interp opcodes
         * (handleBranchAttrOp reads via readAttributes into tmpArea),
         * but sized for safety in case handlers use it. */
        Uint32 local_tmpArea[16];

        /* Phase C.3: delegate the embedded user bytecode to the
         * generalised jump-table interpreter with the aggregation
         * handler table and a backward-jump guard.  The old path
         * called interpreterNextLab, which also accepted opcodes the
         * agg-interp whitelist rejects — the new dispatch raises
         * ZNO_INSTRUCTION_ERROR for any non-whitelisted opcode at
         * runtime, so the invariants AggInterpreter relies on hold by
         * construction. */
        int rc = block_tup->interpreterAggEmbedded(
            req_struct->signal, req_struct,
            &m_prog[exec_pos], emb_len,   /* main program = embedded portion */
            local_tmpArea, 16,
            m_registers);

        req_struct->no_exec_instructions = saved_instr_count;

        if (rc < 0) {
          return ZAGG_EMBEDDED_INTERP_ERROR;
        }

        /* Read skip_offset written by WRITE_INTERPRETER_OUTPUT in embedded prog */
        Uint32 skip_offset = block_tup->c_interpreter_output[0];
        if (skip_offset == AGG_EMBEDDED_INTERP_STOP_PROGRAM) {
          exec_pos = m_prog_len;
        } else {
          exec_pos += emb_len + skip_offset;
        }
        break;
      }

      default: {
        /* Step 1.4: every opcode other than kOpLoadCol /
         * kOpEmbeddedInterp is handled by the shared base helper.
         * AggInterpreter's normal-scan dispatch stays free of any
         * linked-attribute or CTE-specific machinery — those live in
         * JoinAggInterpreter's per-class arms only. */
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
  return 0;
}

Uint32 AggInterpreter::PrepareAggResIfNeeded(Signal* signal, bool force) {
  // Limitation
  Uint32 total_size = m_result_size +
                  (m_gb_map ?
                   m_gb_map->size() * g_result_header_size_per_group_ : 0) +
                  g_result_header_size_;
  if (!force && (m_gb_map == nullptr ||
        total_size < DEF_AGG_RESULT_BATCH_BYTES)) {
    return 0;
  }
  if (force &&
      (m_n_gb_cols != 0 && (m_gb_map == nullptr || m_gb_map->size() == 0))) {
    assert(m_result_size == 0);
    return 0;
  }
  Uint32* data_buf = (&signal->theData[25]);
  Uint32 pos = 0;
  assert(m_n_gb_cols < 0xFFFF);
  assert(m_n_agg_results < 0xFFFF);

  // Phase I.6 (F.2-K.5): when at least one string MIN/MAX slot has
  // been touched, switch the wire marker to AGG_CHAR_RESULT and
  // append a per-group string-payload region after the AggResItem
  // array.  Numeric-only queries continue to emit AGG_RESULT
  // unchanged.  Per-group val_len grows by stringPayloadSize(slots).
  // Mixed numeric+string queries: numeric slots keep their existing
  // AggResItem encoding; only string slots contribute to the
  // appended region.
  const bool has_strings = hasStringSlots();
  const Uint32 marker = has_strings
      ? AttributeHeader::AGG_CHAR_RESULT
      : AttributeHeader::AGG_RESULT;
  if (m_n_gb_cols) {
    data_buf[pos++] = marker << 16 | 0x0721;
    data_buf[pos++] = m_n_gb_cols << 16 | m_n_agg_results;
    Uint32 n_groups_pos = pos++;
    const Uint32 v_len_base = val_len();
    Uint32 n_groups = 0;
    /* Step 2b: iterate the JoinGBHashTable, emit each group, then
     * erase + freeGroupData.  Same emit→erase→free shape as
     * JoinAggInterpreter::evictOneGroup but generalized to drain
     * every currently-resident group. */
    for (auto iter = m_gb_map->begin(); iter.valid();) {
      Uint32 key_len = iter.keyLen();
      char* key_ptr = iter.data();
      AggResItem* slots =
          reinterpret_cast<AggResItem*>(iter.data() + key_len);
      Uint32 payload_bytes = has_strings ? stringPayloadSize(slots) : 0;
      Uint32 v_len_total = v_len_base + payload_bytes;
      assert(key_len % 4 == 0 && key_len < 0xFFFF);
      assert(v_len_total % 4 == 0 && v_len_total < 0xFFFF);
      data_buf[pos++] = key_len << 16 | v_len_total;
      MEMCOPY_NO_WORDS(&data_buf[pos], key_ptr, key_len >> 2);
      MEMCOPY_NO_WORDS(&data_buf[pos + (key_len >> 2)], slots,
                       v_len_base >> 2);
      if (payload_bytes > 0) {
        encodeStringPayload(slots, reinterpret_cast<char*>(
            &data_buf[pos + ((key_len + v_len_base) >> 2)]));
      }
      pos += ((key_len + v_len_total) >> 2);
      /* Free per-group string val_ptr buffers (Phase I.6 F.2-K.4e) —
       * payload bytes already substituted into the wire above. */
      freeGroupStringSlots(slots);
      /* eraseAndNext removes the entry from the bucket chain and
       * advances iter past it; freeGroupData then releases the
       * group's chunk slot.  Hold key_ptr before erase so we still
       * have a valid pointer for freeGroupData. */
      m_gb_map->eraseAndNext(iter);
      freeGroupData(key_ptr);
      n_groups++;
    }
    data_buf[n_groups_pos] = n_groups;
    m_n_groups = m_gb_map->size();
    m_result_size = 0;
  } else {
    const Uint32 v_len_base = m_n_agg_results * sizeof(AggResItem);
    const Uint32 payload_bytes =
        has_strings ? stringPayloadSize(m_agg_results) : 0;
    const Uint32 v_len_total = v_len_base + payload_bytes;
    data_buf[pos++] = marker << 16 | 0x0721;
    data_buf[pos++] = m_n_gb_cols << 16 | m_n_agg_results;
    data_buf[pos++] = 0;
    data_buf[pos++] = 0 << 16 | v_len_total;
    assert(m_gb_map == nullptr);
    MEMCOPY_NO_WORDS(&data_buf[pos], m_agg_results, v_len_base >> 2);
    if (payload_bytes > 0) {
      encodeStringPayload(m_agg_results, reinterpret_cast<char*>(
          &data_buf[pos + (v_len_base >> 2)]));
    }
    pos += (v_len_total >> 2);
  }

#if defined(PA_CHECK) && !defined(NDEBUG)
  /*
   * PA related
   * Validation
   */
  Uint32 data_len = pos;
  Uint32 parse_pos = 0;

  while (parse_pos < data_len) {
    AttributeHeader agg_checker_ah(data_buf[parse_pos++]);
    const Uint32 marker_id = agg_checker_ah.getAttributeId();
    assert((marker_id == AttributeHeader::AGG_RESULT ||
            marker_id == AttributeHeader::AGG_CHAR_RESULT) &&
           agg_checker_ah.getByteSize() == 0x0721);
    const bool wire_has_strings =
        (marker_id == AttributeHeader::AGG_CHAR_RESULT);
    Uint32 n_gb_cols = data_buf[parse_pos] >> 16;
    Uint32 n_agg_results = data_buf[parse_pos++] & 0xFFFF;
    Uint32 n_res_items = data_buf[parse_pos++];
    // g_eventLogger->info("Moz, GB cols: %u, AGG results: %u, RES items: %u",
    //         n_gb_cols, n_agg_results, n_res_items);

    if (n_gb_cols) {
      // char log_buf[128];
      for (Uint32 i = 0; i < n_res_items; i++) {
        Uint32 gb_cols_len = data_buf[parse_pos] >> 16;
        Uint32 agg_res_len = data_buf[parse_pos++] & 0xFFFF;
        // remove compile warnings
        (void)gb_cols_len;
        (void)agg_res_len;
        // For AGG_CHAR_RESULT, agg_res_len includes the appended
        // string-payload region past the AggResItem array.  Capture
        // the absolute end position so we can skip the appended
        // region after walking the per-slot AggResItem entries.
        const Uint32 group_val_end =
            parse_pos + ((gb_cols_len + agg_res_len) >> 2);
        for (Uint32 j = 0; j < n_gb_cols; j++) {
          AttributeHeader ah(data_buf[parse_pos++]);
          // sprintf(log_buf,
          //     "[id: %u, sizeB: %u, sizeW: %u, gb_len: %u, "
          //     "res_len: %u, value: ",
          //     ah.getAttributeId(), ah.getByteSize(),
          //     ah.getDataSize(), gb_cols_len, agg_res_len);
          assert(ah.getDataPtr() != &data_buf[parse_pos]);
          // char* ptr = (char*)(&data_buf[parse_pos]);
          // for (Uint32 i = 0; i < ah.getByteSize(); i++) {
          //   sprintf(log_buf + strlen(log_buf), " %x", ptr[i]);
          // }
          parse_pos += ah.getDataSize();
          // sprintf(log_buf + strlen(log_buf), "]");
        }
        for (Uint32 i = 0; i < n_agg_results; i++) {
          // AggResItem* ptr = (AggResItem*)(&data_buf[parse_pos]);
          // sprintf(log_buf + strlen(log_buf), "(type: %u, is_unsigned: %u, is_null: %u, value: ",
          //         ptr->type, ptr->is_unsigned, ptr->is_null);
          // switch (ptr->type) {
          //   case NDB_TYPE_BIGINT:
          //     sprintf(log_buf + strlen(log_buf), "%15ld", ptr->value.val_int64);
          //     break;
          //   case NDB_TYPE_DOUBLE:
          //     sprintf(log_buf + strlen(log_buf), "%31.16f", ptr->value.val_double);
          //     break;
          //   default:
          //     assert(0);
          // }
          // sprintf(log_buf + strlen(log_buf), ")");
          parse_pos += (sizeof(AggResItem) >> 2);
        }
        // After the AggResItem array, skip any appended string
        // payload region (only present for AGG_CHAR_RESULT).
        assert(parse_pos <= group_val_end);
        parse_pos = group_val_end;
        // g_eventLogger->info("%s", log_buf);
      }
    } else {
      assert(n_gb_cols == 0);
      assert(n_agg_results == m_n_agg_results);
      assert(n_res_items == 0);
      Uint32 gb_cols_len = data_buf[parse_pos] >> 16;
      Uint32 agg_res_len = data_buf[parse_pos++] & 0xFFFF;
      assert(gb_cols_len == 0);
      // Numeric-only path: agg_res_len exactly fits the AggResItem
      // array.  AGG_CHAR_RESULT path: includes the appended
      // string-payload region; advance by the declared length.
      assert(wire_has_strings ||
             agg_res_len == m_n_agg_results * sizeof(AggResItem));
      parse_pos += (agg_res_len >> 2);
    }
  }
  assert(parse_pos == data_len);
#endif // PA_CHECK && !NDEBUG
  return pos;
}

Uint32 AggInterpreter::NumOfResRecords(bool last_time) {
  /*
   * Moz
   * NumOfResRecords is called after PrepareAggResIfNeeded
   * to see if there's no result left in the interpreter.
   * we use this return value to stop Dblqh::scanTupkeyRefLab
   * to send scanfragconf to TC wrongly
   * see [MOZ-COMMENT] there.
   */

  if (!last_time) {
    /*
     * if it's not the last time PrepareAggResIfNeeded,
     * here we can return the real value.
     * NOTICE:
     * always return 1 even if m_gb_map is empty().
     * In this situation: pushdown aggregation with filter and
     * group by. 99% rows has been filtered out which means
     * m_gb_map has big chance to stay empty. In order to stop
     * Dblqh::scanTupkeyRefLab send scanfragconf before aggregation
     * scan finishes. here return 1 to stop that.
     */
    if (m_gb_map) {
      return (m_gb_map->empty() ? 1 : m_gb_map->size());
    } else {
      /*
       * In non-groupby mode, before we send the result to API
       * at the last time. we always return 1.
       * NOTICE:
       * In non-groupby mode, we still need to stop scanTupkeyRefLab
       * send scanfragconf wrongly.
       */
      return 1;
    }
  } else {
    /*
     * This is the last time we call PrepareAggResIfNeeded, so the
     * aggregation is going to finish.
     * We assert all results have been sent and return 0 here.
     */
    if (m_gb_map) {
      assert(m_gb_map->empty());
    }
    return 0;
  }
}

// release_string_results body lifted to AggInterpreterBase in Step 3a-A.

