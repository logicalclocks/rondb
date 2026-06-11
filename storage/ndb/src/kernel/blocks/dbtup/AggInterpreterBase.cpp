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

/*
 * AggInterpreterBase.cpp — shared numeric / type aggregation kernels.
 *
 * Step 1 of the interpreter unification
 * (claude_files/pushdown_join_aggregation/agg_interpreter_unification_plan.md):
 * these were previously duplicated verbatim as file-static functions in both
 * AggInterpreter.cpp and JoinAggInterpreter.cpp.  They are pure functions over
 * Register / AggResItem and were verified logically identical between the two
 * copies (differing only in cosmetics and the debug-only DEBUG_PA_INTERP trace
 * blocks, which are retained here).
 *
 * Include set and ordering mirror AggInterpreter.cpp (whence these bodies
 * came) so the kernels see exactly the symbols they did before.
 */
#include <cstdint>
#include <cstring>
#include <utility>
#include <cmath>     // std::isfinite
#include <climits>   // LLONG_MAX
#include <cstdio>    // sprintf
#include <cassert>   // assert
#include <new>       // placement new for initBufBlock

#define DBTUP_C
#include "signaldata/TransIdAI.hpp"
#include "include/my_byteorder.h"
#include "AggInterpreterBase.hpp"
#include "Dbtup.hpp"
#include "InterpreterCommonOp.hpp"
#include "util/require.h"
#include "decimal.h"
#include <NdbSqlUtil.hpp>
#include <Interpreter.hpp>
#include <CteLinkedAttr.hpp>
#include "my_sys.h"
#include "../dblqh/Dblqh.hpp"

#define JAM_FILE_ID 566

/*
 * DEBUG_PA_INTERP / DEBUG_AGG machinery (off by default).  The kernels'
 * debug-only trace blocks reference DEBUG_PA_INTERP / PrintValue, and
 * the executeStandardOpcode body references DEB_AGG for overflow
 * traces — both must compile out cleanly in production builds and be
 * re-enableable for tracing.  Keep these in sync with the matching
 * blocks in AggInterpreter.cpp and JoinAggInterpreter.cpp.
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

/**
 * Step 3 Candidate B — shared post-prelude of the kOpLoadCol arm.
 *
 * See AggInterpreterBase.hpp for the contract.  Each subclass arm
 * sets up `header` / `attrDescriptor` / linked-attr state, then calls
 * this helper for the bulk of the work (~270 lines pre-3-B): type
 * supported check, DECIMAL precision/scale word read (advances
 * exec_pos), register init with NULL early-return, big switch on
 * NDB_TYPE_*.  For string types, captures into
 * m_register_string_data and bumps m_attr_read_pos so subsequent
 * column reads don't overwrite the captured pointer.
 *
 * Verbose DEB_AGG / PA_INTERP_TRACE traces preserved from
 * AggInterpreter's original arm (debug-only; JoinAgg gains the trace
 * coverage in VM_TRACE builds).  The CTE linked-attr branch in the
 * string case is a JoinAgg-only path — AggInterpreter never enters
 * it because its prelude always sets attrDescriptor non-null.
 */
Int32 AggInterpreterBase::loadColumnTypedFromBuf(
    DataType type, bool is_unsigned, Uint32 reg_index,
    AttributeHeader* header, const Uint32* attrDescriptor,
    bool linked_cte_attr, Uint32 linked_word0, Uint32 linked_word1,
    Dbtup::KeyReqStruct* req_struct, Uint32& exec_pos,
    const char* class_name) {
  if (!TypeSupported(type)) {
    DEB_AGG(("Unsupported column type: %u", type));
    return ZAGG_COL_TYPE_UNSUPPORTED;
  }

  Int32 decimal_info = 0;
  Int32 precision = 0;
  Int32 scale = 0;
  Int32 dec_ret = E_DEC_OK;
  Uint8* dec_buf_ptr = nullptr;
  double dec_val_dbl = 0;
  longlong dec_val_ll = 0;
  ulonglong dec_val_ull = 0;

  if (type == NDB_TYPE_DECIMAL ||
      type == NDB_TYPE_DECIMALUNSIGNED) {
    if (unlikely(exec_pos >= m_prog_len)) {
      g_eventLogger->debug("%s::ProcessRec ZAGG_OTHER_ERROR: "
          "kOpLoadCol DECIMAL overflow exec_pos=%u prog_len=%u",
          class_name, exec_pos, m_prog_len);
      return ZAGG_OTHER_ERROR;
    }
    decimal_info =
        sint4korr(reinterpret_cast<char*>(&m_prog[exec_pos++]));
    precision = decimal_info >> 16;
    scale = decimal_info & 0xFFFF;
  }

  ResetRegister(&m_registers[reg_index]);
  m_registers[reg_index].type = AlignedType(type, scale);
  m_registers[reg_index].is_unsigned = is_unsigned;
  m_registers[reg_index].is_null = header->isNULL();
  if (m_registers[reg_index].is_null) {
    PA_INTERP_TRACE(m_frag_id,
                    "Load NULL, type: %u",
                    m_registers[reg_index].type);
    m_registers[reg_index].value.val_int64 = 0;
    return 0;
  }

  switch (type) {
    case NDB_TYPE_TINYINT:
      m_registers[reg_index].value.val_int64 =
          *reinterpret_cast<Int8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
      PA_INTERP_TRACE(m_frag_id,
                      "Load NDB_TYPE_TINYINT %lld",
                      m_registers[reg_index].value.val_int64);
      return 0;
    case NDB_TYPE_SMALLINT:
      m_registers[reg_index].value.val_int64 =
          sint2korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
      PA_INTERP_TRACE(m_frag_id,
                      "Load NDB_TYPE_SMALLINT %lld",
                      m_registers[reg_index].value.val_int64);
      return 0;
    case NDB_TYPE_MEDIUMINT:
      m_registers[reg_index].value.val_int64 =
          sint3korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
      PA_INTERP_TRACE(m_frag_id,
                      "Load NDB_TYPE_MEDIUM %lld",
                      m_registers[reg_index].value.val_int64);
      return 0;
    case NDB_TYPE_INT:
      m_registers[reg_index].value.val_int64 =
          sint4korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
      PA_INTERP_TRACE(m_frag_id,
                      "Load NDB_TYPE_INT %lld",
                      m_registers[reg_index].value.val_int64);
      return 0;
    case NDB_TYPE_BIGINT:
      m_registers[reg_index].value.val_int64 =
          sint8korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
      PA_INTERP_TRACE(m_frag_id,
                      "Load NDB_TYPE_BIGINT %lld",
                      m_registers[reg_index].value.val_int64);
      return 0;
    case NDB_TYPE_TINYUNSIGNED:
      m_registers[reg_index].value.val_uint64 =
          *reinterpret_cast<Uint8*>(&m_attr_read_buf[m_attr_read_pos + 1]);
      PA_INTERP_TRACE(m_frag_id,
                      "Load NDB_TYPE_TINYUNSIGNED %llu",
                      m_registers[reg_index].value.val_uint64);
      return 0;
    case NDB_TYPE_SMALLUNSIGNED:
      m_registers[reg_index].value.val_uint64 =
          uint2korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
      PA_INTERP_TRACE(m_frag_id,
                      "Load NDB_TYPE_SMALLUNSIGNED %llu",
                      m_registers[reg_index].value.val_uint64);
      return 0;
    case NDB_TYPE_MEDIUMUNSIGNED:
      m_registers[reg_index].value.val_uint64 =
          uint3korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
      PA_INTERP_TRACE(m_frag_id,
                      "Load NDB_TYPE_MEDIUMUNSIGNED %llu",
                      m_registers[reg_index].value.val_uint64);
      return 0;
    case NDB_TYPE_UNSIGNED:
      m_registers[reg_index].value.val_uint64 =
          uint4korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
      PA_INTERP_TRACE(m_frag_id,
                      "Load NDB_TYPE_UNSIGNED %llu",
                      m_registers[reg_index].value.val_uint64);
      return 0;
    case NDB_TYPE_BIGUNSIGNED:
      m_registers[reg_index].value.val_uint64 =
          uint8korr(reinterpret_cast<char*>(&m_attr_read_buf[m_attr_read_pos + 1]));
      PA_INTERP_TRACE(m_frag_id,
                      "Load NDB_TYPE_BIGUNSIGNED %llu",
                      m_registers[reg_index].value.val_uint64);
      return 0;
    case NDB_TYPE_FLOAT:
      m_registers[reg_index].value.val_double =
          floatget(reinterpret_cast<unsigned char*>(
                &m_attr_read_buf[m_attr_read_pos + 1]));
      PA_INTERP_TRACE(m_frag_id,
                      "Load NDB_TYPE_FLOAT %lf",
                      m_registers[reg_index].value.val_double);
      return 0;
    case NDB_TYPE_DOUBLE:
      m_registers[reg_index].value.val_double =
          doubleget(reinterpret_cast<unsigned char*>(
                &m_attr_read_buf[m_attr_read_pos + 1]));
      PA_INTERP_TRACE(m_frag_id,
                      "Load NDB_TYPE_DOUBLE %lf",
                      m_registers[reg_index].value.val_double);
      return 0;
    case NDB_TYPE_DECIMAL:
      assert(static_cast<Uint32>(decimal_bin_size(precision, scale)) ==
          header->getByteSize());
      dec_ret = bin2decimal(reinterpret_cast<const uchar*>(
                    &m_attr_read_buf[m_attr_read_pos + 1]),
                &m_decimal, precision, scale);
      if (dec_ret != E_DEC_OK) {
        dec_buf_ptr = reinterpret_cast<Uint8*>(
            &m_attr_read_buf[m_attr_read_pos + 1]);
        char log_buf[128];
        sprintf(log_buf, "Error while parsing decimal: ");
        for (Uint32 i = 0; i < header->getByteSize(); i++) {
          sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
        }
        DEB_AGG(("%s", log_buf));
        if (dec_ret == E_DEC_OVERFLOW) {
          return ZAGG_DECIMAL_PARSE_OVERFLOW;
        } else {
          return ZAGG_DECIMAL_PARSE_ERROR;
        }
      }
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
        dec_buf_ptr = reinterpret_cast<Uint8*>(
            &m_attr_read_buf[m_attr_read_pos + 1]);
        char log_buf[128];
        sprintf(log_buf, "Error while converting decimal: ");
        for (Uint32 i = 0; i < header->getByteSize(); i++) {
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
#endif
      return 0;

    case NDB_TYPE_DECIMALUNSIGNED:
      assert(static_cast<Uint32>(decimal_bin_size(precision, scale)) ==
          header->getByteSize());
      dec_ret = bin2decimal(reinterpret_cast<const uchar*>(
                    &m_attr_read_buf[m_attr_read_pos + 1]),
                &m_decimal, precision, scale);
      if (dec_ret != E_DEC_OK) {
        dec_buf_ptr = reinterpret_cast<Uint8*>(
            &m_attr_read_buf[m_attr_read_pos + 1]);
        char log_buf[128];
        sprintf(log_buf, "Error while parsing decimal: ");
        for (Uint32 i = 0; i < header->getByteSize(); i++) {
          sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
        }
        DEB_AGG(("%s", log_buf));
        if (dec_ret == E_DEC_OVERFLOW) {
          return ZAGG_DECIMAL_PARSE_OVERFLOW;
        } else {
          return ZAGG_DECIMAL_PARSE_ERROR;
        }
      }
      assert(m_registers[reg_index].is_unsigned == true);
      if (unlikely(m_decimal.sign)) {
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
        dec_buf_ptr = reinterpret_cast<Uint8*>(
            &m_attr_read_buf[m_attr_read_pos + 1]);
        char log_buf[128];
        sprintf(log_buf, "Error while converting decimal: ");
        for (Uint32 i = 0; i < header->getByteSize(); i++) {
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
#endif
      return 0;

    case NDB_TYPE_CHAR:
    case NDB_TYPE_VARCHAR:
    case NDB_TYPE_LONGVARCHAR: {
      /* Phase I.6 (F.2-K.4b): stash a read-only view into
       * m_attr_read_buf for a subsequent kOpMin / kOpMax to compare and
       * copy without re-walking the AttributeDescriptor.  CTE linked-
       * attr branch (Phase I.6 F.4-K.3, JoinAgg-only) reads the type
       * metadata from the two linked-attr header words.  AggInterpreter
       * never enters that branch (attrDescriptor is always non-null on
       * its prelude path). */
      const CHARSET_INFO* cs = nullptr;
      Uint32 declared = 0;
      if (attrDescriptor != nullptr) {
        const Uint32 TattrDesc1 = attrDescriptor[0];
        const Uint32 TattrDesc2 = attrDescriptor[1];
        if (AttributeOffset::getCharsetFlag(TattrDesc2)) {
          const Uint32 pos = AttributeOffset::getCharsetPos(TattrDesc2);
          cs = req_struct->tablePtrP->charsetArray[pos];
        }
        declared = AttributeDescriptor::getSizeInBytes(TattrDesc1);
      } else if (linked_cte_attr) {
        const Uint32 linked_type = CteLinkedAttr::decodeTypeId(linked_word0);
        if (linked_type != type) {
          return ZAGG_LOAD_COL_WRONG_TYPE;
        }
        declared = CteLinkedAttr::decodeMaxBytes(linked_word0);
        const Uint32 csNumber = CteLinkedAttr::decodeCsNumber(linked_word1);
        if (csNumber != 0) {
          cs = all_charsets[csNumber];
        }
      } else {
        return ZAGG_LOAD_COL_WRONG_TYPE;
      }
      const Uint16 prefix =
          (type == NDB_TYPE_CHAR) ? 0 :
          (type == NDB_TYPE_VARCHAR) ? 1 : 2;
      char* base = reinterpret_cast<char*>(
          &m_attr_read_buf[m_attr_read_pos + 1]);
      Uint16 payload_len;
      if (type == NDB_TYPE_CHAR) {
        payload_len = static_cast<Uint16>(
            attrDescriptor != nullptr ? declared :
            header->getByteSize());
      } else if (type == NDB_TYPE_VARCHAR) {
        payload_len = static_cast<Uint16>(static_cast<Uint8>(base[0]));
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
      /* Advance m_attr_read_pos past the AttributeHeader + string bytes
       * so the next kOpLoadCol doesn't overwrite the captured ptr.
       * Numeric paths leave m_attr_read_pos at 0 (they extract into the
       * register's 8-byte union and the buffer is then disposable);
       * strings need the buffer to stay live until kOpMax / kOpMin runs
       * minMaxString. */
      const Uint32 string_bytes = prefix + payload_len;
      const Uint32 words_consumed = 1 /*AttributeHeader*/ +
                                    ((string_bytes + 3) >> 2);
      if (unlikely(m_attr_read_pos + words_consumed >
                   g_attr_read_buf_len_)) {
        g_eventLogger->debug("%s::ProcessRec "
            "ZAGG_OTHER_ERROR: string attr buffer overflow "
            "pos=%u words=%u buf_words=%u",
            class_name, m_attr_read_pos, words_consumed,
            g_attr_read_buf_len_);
        return ZAGG_OTHER_ERROR;
      }
      m_attr_read_pos += words_consumed;
      return 0;
    }
    default:
      return ZAGG_LOAD_COL_WRONG_TYPE;
  }
}

/**
 * Step 3 Candidate A — peek and validate the program header from the
 * input prog buffer before m_buf_block is allocated.  Bodies in both
 * subclasses' Inits used to do this identically (after sed-collapsing
 * trivial comment differences).
 */
void AggInterpreterBase::peekProgramHeader(const Uint32* prog,
                                            bool* compatible) {
  *compatible = true;
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
    *compatible = false;
    return;
  }
  assert((prog[3] & 0x80000000) == 0);
  assert(prog[3] == 0);

  assert(m_prog_len <= MAX_AGG_PROGRAM_WORD_SIZE);
  assert(m_n_gb_cols <= MAX_AGG_N_GROUPBY_COLS);
  assert(m_n_agg_results <= MAX_AGG_N_RESULTS);
}

/**
 * Step 3 Candidate A — common Init steps that must run after the
 * subclass has called initBufBlock with its own sizing.  Copies the
 * program into m_prog_buf, configures m_gb_map for GROUP BY, inits
 * m_agg_results, sets m_inited / m_agg_prog_start_pos, and zeroes the
 * register file.
 */
void AggInterpreterBase::initSharedAfterAlloc(const Uint32* prog) {
  m_prog = m_prog_buf;
  memcpy(m_prog, prog, m_prog_len * sizeof(Uint32));
  /* m_attr_read_buf is now the Dbtup-instance scratch buffer
   * (Step 3 Cand-C); ProcessRec binds it on entry.  No init-time
   * memset needed — m_attr_read_pos resets on each opcode iteration. */
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
}

/**
 * Step 3b — shared validate-embedded-programs scan.  Both subclass
 * Inits used to walk m_prog from m_agg_prog_start_pos and invoke
 * validateEmbeddedProgram on every kOpEmbeddedInterp arm.  Bodies
 * were byte-identical except for the class name in the warning.
 */
bool AggInterpreterBase::scanAndValidateEmbeddedPrograms(
    const char* class_name) {
  Uint32 scan_pos = m_agg_prog_start_pos;
  while (scan_pos < m_prog_len) {
    Uint32 w = m_prog[scan_pos];
    Uint8 op = (w & 0xFC000000) >> 26;
    if (op == kOpEmbeddedInterp) {
      Uint32 emb_len = w & 0xFFFF;
      if (scan_pos + 1 + emb_len > m_prog_len ||
          !validateEmbeddedProgram(&m_prog[scan_pos + 1], emb_len)) {
        g_eventLogger->warning(
            "%s::Init: embedded program validation failed "
            "at scan_pos=%u", class_name, scan_pos);
        m_inited = false;
        return false;
      }
      scan_pos += 1 + emb_len;  /* header + embedded words */
    } else if (op == kOpLoadConst) {
      scan_pos += 3;  /* header + 2 constant value words */
    } else if (op == kOpLoadCol) {
      Uint32 type = (w & 0x03E00000) >> 21;
      scan_pos += (type == NDB_TYPE_DECIMAL ||
                   type == NDB_TYPE_DECIMALUNSIGNED) ? 2 : 1;
    } else {
      scan_pos++;
    }
  }
  return true;
}

/**
 * validateEmbeddedProgram — strict embedded-program sanity check.
 *
 * Step 1.2 of the interpreter unification.  The JoinAggInterpreter copy
 * was strictly more rigorous than the AggInterpreter copy: in addition
 * to bounds-checking branch targets, it enforced an opcode allow-list
 * and rejected backward branches.  The stricter form is adopted here
 * for both code paths.  Pure function over arguments.
 */
bool AggInterpreterBase::validateEmbeddedProgram(
    const Uint32* emb_prog, Uint32 emb_len) {
  Uint32 pc = 0;
  while (pc < emb_len) {
    Uint32 instr = emb_prog[pc];
    Uint32 opCode = Interpreter::getOpCode(instr);

    switch (opCode) {
      case Interpreter::READ_ATTR_INTO_REG:
      case Interpreter::LOAD_CONST_NULL:
      case Interpreter::LOAD_CONST16:
      case Interpreter::LOAD_CONST32:
      case Interpreter::LOAD_CONST64:
      case Interpreter::LOAD_DOUBLE_CONST:
      case Interpreter::ADD_REG_REG:
      case Interpreter::SUB_REG_REG:
      case Interpreter::MUL_REG_REG:
      case Interpreter::BRANCH:
      case Interpreter::BRANCH_REG_EQ_NULL:
      case Interpreter::BRANCH_REG_NE_NULL:
      case Interpreter::BRANCH_EQ_REG_REG:
      case Interpreter::BRANCH_NE_REG_REG:
      case Interpreter::BRANCH_LT_REG_REG:
      case Interpreter::BRANCH_LE_REG_REG:
      case Interpreter::BRANCH_GT_REG_REG:
      case Interpreter::BRANCH_GE_REG_REG:
      case Interpreter::EXIT_OK:
      case Interpreter::BRANCH_ATTR_OP_ARG:
      case Interpreter::BRANCH_MEM_OP_ARG:
      case Interpreter::BRANCH_MEM_OP_ARG_INLINE_TYPE:
      case Interpreter::BRANCH_ATTR_EQ_NULL:
      case Interpreter::BRANCH_ATTR_NE_NULL:
      case Interpreter::READ_LINKED_TO_MEM:
      case Interpreter::READ_UINT8_MEM_TO_REG:
      case Interpreter::READ_UINT16_MEM_TO_REG:
      case Interpreter::READ_UINT32_MEM_TO_REG:
      case Interpreter::READ_INT64_MEM_TO_REG:
      case Interpreter::READ_AGG_REG_TO_REG:
      case Interpreter::READ_LINKED_COLUMN_TO_REG:
      case Interpreter::WRITE_INTERPRETER_OUTPUT:
        break;
      default:
        g_eventLogger->warning(
            "validateEmbeddedProgram: forbidden opcode %u at pc=%u",
            opCode, pc);
        return false;
    }

    bool is_branch = false;
    switch (opCode) {
      case Interpreter::BRANCH:
      case Interpreter::BRANCH_REG_EQ_NULL:
      case Interpreter::BRANCH_REG_NE_NULL:
      case Interpreter::BRANCH_EQ_REG_REG:
      case Interpreter::BRANCH_NE_REG_REG:
      case Interpreter::BRANCH_LT_REG_REG:
      case Interpreter::BRANCH_LE_REG_REG:
      case Interpreter::BRANCH_GT_REG_REG:
      case Interpreter::BRANCH_GE_REG_REG:
      case Interpreter::BRANCH_ATTR_OP_ARG:
      case Interpreter::BRANCH_MEM_OP_ARG:
      case Interpreter::BRANCH_MEM_OP_ARG_INLINE_TYPE:
      case Interpreter::BRANCH_ATTR_EQ_NULL:
      case Interpreter::BRANCH_ATTR_NE_NULL:
        is_branch = true;
        break;
      default:
        break;
    }

    if (is_branch) {
      Uint32 direction = instr >> 31;
      if (direction != 0) {
        g_eventLogger->warning(
            "validateEmbeddedProgram: backward branch at pc=%u", pc);
        return false;
      }
      Uint32 offset = (instr >> 16) & 0x7FFF;
      Uint32 target = pc + offset;
      if (target >= emb_len) {
        g_eventLogger->warning(
            "validateEmbeddedProgram: branch target %u out of bounds "
            "(emb_len=%u) at pc=%u", target, emb_len, pc);
        return false;
      }
    }

    Interpreter::InstructionPreProcessing processing;
    Uint32* next = Interpreter::getInstructionPreProcessingInfo(
        const_cast<Uint32*>(&emb_prog[pc]), processing);
    if (next == nullptr) {
      g_eventLogger->warning(
          "validateEmbeddedProgram: invalid instruction at pc=%u", pc);
      return false;
    }
    Uint32 instr_len = (Uint32)(next - &emb_prog[pc]);
    pc += instr_len;
  }
  return true;
}

/**
 * OptimizeProgram — guard + delegate to the shared OptimizeProgramBuffer.
 *
 * Step 1.2 of the interpreter unification.  Previously a byte-identical
 * 7-line method on each subclass; now a single definition that both
 * subclasses inherit.  Uses m_inited / m_prog_len from PushdownInterpreter
 * and m_prog / m_agg_prog_start_pos from this class (fields lifted from
 * the subclasses in 1.2).
 */
bool AggInterpreterBase::OptimizeProgram() {
  if (!m_inited) {
    return false;
  }
  OptimizeProgramBuffer(m_prog, m_prog_len, m_agg_prog_start_pos);
  return true;
}

bool AggInterpreterBase::TypeSupported(DataType type) {
  switch (type) {
    case NDB_TYPE_TINYINT:
    case NDB_TYPE_SMALLINT:
    case NDB_TYPE_MEDIUMINT:
    case NDB_TYPE_INT:
    case NDB_TYPE_BIGINT:

    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:

    case NDB_TYPE_FLOAT:
    case NDB_TYPE_DOUBLE:

    case NDB_TYPE_DECIMAL:
    case NDB_TYPE_DECIMALUNSIGNED:

    // Phase I.6 (F.2): MIN/MAX over CHAR / VARCHAR / Longvarchar.
    // Sum is rejected separately (see Sum()).  Count is
    // type-agnostic and works for any column type.  String
    // value handling lives in MinString / MaxString and the
    // m_string_results sidecar — see cte_filter_phase_i6_varchar.md.
    case NDB_TYPE_CHAR:
    case NDB_TYPE_VARCHAR:
    case NDB_TYPE_LONGVARCHAR:
      return true;
    default:
      return false;
  }
  return false;
}

bool AggInterpreterBase::IsUnsigned(DataType type) {
  switch (type) {
    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:
    case NDB_TYPE_DECIMALUNSIGNED:
      return true;
    default:
      return false;
  }
  return false;
}

DataType AggInterpreterBase::AlignedType(DataType type, int scale) {
  switch (type) {
    case NDB_TYPE_TINYINT:
    case NDB_TYPE_SMALLINT:
    case NDB_TYPE_MEDIUMINT:
    case NDB_TYPE_INT:
    case NDB_TYPE_BIGINT:

    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:

      return NDB_TYPE_BIGINT;
    case NDB_TYPE_FLOAT:
    case NDB_TYPE_DOUBLE:
      return NDB_TYPE_DOUBLE;
    case NDB_TYPE_DECIMAL:
    case NDB_TYPE_DECIMALUNSIGNED:
      return scale == 0 ? NDB_TYPE_BIGINT : NDB_TYPE_DOUBLE;

    // Phase I.6 (F.2): string MIN/MAX preserves the source type —
    // wire format stays as the source's [length_prefix][payload].
    case NDB_TYPE_CHAR:
    case NDB_TYPE_VARCHAR:
    case NDB_TYPE_LONGVARCHAR:
      return type;
    default:
      assert(0);
  }
  return NDB_TYPE_UNDEFINED;
}

void AggInterpreterBase::PrintValue(const AggResItem* res, char* log_buf) {
  if (res->type == NDB_TYPE_BIGINT) {
    if (res->is_unsigned) {
      sprintf(log_buf + strlen(log_buf), "[%llu, %d, %d, %d]",
          res->value.val_uint64, res->type, res->is_unsigned, res->is_null);
    } else {
      sprintf(log_buf + strlen(log_buf), "[%lld, %d, %d, %d]",
          res->value.val_int64, res->type, res->is_unsigned, res->is_null);
    }
  } else {
    assert(res->type == NDB_TYPE_DOUBLE);
    sprintf(log_buf + strlen(log_buf), "[%lf, %d, %d, %d]",
        res->value.val_double, res->type, res->is_unsigned, res->is_null);
  }
  g_eventLogger->info("[PA_INTERP_TRACE] %s", log_buf);
}

Int32 AggInterpreterBase::Sum(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED) {
    // Agg result first initialized
    *res = a;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "Sum() init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  if (res->is_null) {
    assert(res->value.val_int64 == 0);
  }

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || res->type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT &&
          (res->type == NDB_TYPE_BIGINT || res->type == NDB_TYPE_UNDEFINED));
    res_type = NDB_TYPE_BIGINT;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    Int64 val0 = a.value.val_int64;
    Int64 val1 = res->value.val_int64;
    Int64 res_val = static_cast<Uint64>(val0) + static_cast<Uint64>(val1);
    bool res_unsigned = false;

    if (a.is_unsigned) {
      if (res->is_unsigned || val1 >= 0) {
        if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) {
          // overflows;
          return -1;
        } else {
          res_unsigned = true;
        }
      } else {
        if ((Uint64)val0 > (Uint64)(LLONG_MAX)) {
          res_unsigned = true;
        }
      }
    } else {
      if (res->is_unsigned) {
        if (val0 >= 0) {
          if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) {
            // overflows;
            return -1;
          } else {
            res_unsigned = true;
          }
        } else {
          if ((Uint64)val1 > (Uint64)(LLONG_MAX)) {
            res_unsigned = true;
          }
        }
      } else {
        if (val0 >= 0 && val1 >= 0) {
          res_unsigned = true;
        } else if (val0 < 0 && val1 < 0 && res_val >= 0) {
          // overflow
          return -1;
        }
      }
    }

    // Check if res_val is overflow
    bool unsigned_flag = (a.is_unsigned | res->is_unsigned);
    if ((unsigned_flag && !res_unsigned && res_val < 0) ||
        (!unsigned_flag && res_unsigned &&
         (Uint64)res_val > (Uint64)LLONG_MAX)) {
      return -1;
    } else {
      if (unsigned_flag) {
        res->value.val_uint64 = res_val;
      } else {
        res->value.val_int64 = res_val;
      }
    }
    res->is_unsigned = unsigned_flag;
  } else {
    double val0 = (a.type == NDB_TYPE_DOUBLE) ?
                     a.value.val_double :
                     ((a.is_unsigned == true) ?
                       static_cast<double>(a.value.val_uint64) :
                       static_cast<double>(a.value.val_int64));
    double val1 = (res->type == NDB_TYPE_DOUBLE) ?
                     res->value.val_double :
                     ((res->is_unsigned == true) ?
                       static_cast<double>(res->value.val_uint64) :
                       static_cast<double>(res->value.val_int64));
    double res_val = val0 + val1;
    if (std::isfinite(res_val)) {
      res->value.val_double = res_val;
    } else {
      // overflow
      return -1;
    }
    res->is_unsigned = false;
  }

  res->type = res_type;
  res->is_null = false;

#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "Sum(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP
  return 0;
}

/**
 * SumBigint - Sum for BIGINT (handles both signed and unsigned dynamically)
 */
Int32 AggInterpreterBase::SumBigint(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) {
    return 1;
  }

  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_BIGINT;
    res->value.val_int64 = a.value.val_int64;
    res->is_unsigned = a.is_unsigned;
    res->is_null = false;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "SumBigint() init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }

  Int64 val0 = a.value.val_int64;
  Int64 val1 = res->value.val_int64;
  Int64 res_val = static_cast<Uint64>(val0) + static_cast<Uint64>(val1);
  bool res_unsigned = false;

  if (a.is_unsigned) {
    if (res->is_unsigned || val1 >= 0) {
      if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) {
        return -1;
      }
      res_unsigned = true;
    } else {
      if ((Uint64)val0 > (Uint64)(LLONG_MAX)) {
        res_unsigned = true;
      }
    }
  } else {
    if (res->is_unsigned) {
      if (val0 >= 0) {
        if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) {
          return -1;
        }
        res_unsigned = true;
      } else {
        if ((Uint64)val1 > (Uint64)(LLONG_MAX)) {
          res_unsigned = true;
        }
      }
    } else {
      if (val0 >= 0 && val1 >= 0) {
        res_unsigned = true;
      } else if (val0 < 0 && val1 < 0 && res_val >= 0) {
        return -1;
      }
    }
  }

  bool unsigned_flag = (a.is_unsigned | res->is_unsigned);
  if ((unsigned_flag && !res_unsigned && res_val < 0) ||
      (!unsigned_flag && res_unsigned &&
       (Uint64)res_val > (Uint64)LLONG_MAX)) {
    return -1;
  }

  if (unsigned_flag) {
    res->value.val_uint64 = res_val;
  } else {
    res->value.val_int64 = res_val;
  }
  res->is_unsigned = unsigned_flag;
#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "SumBigint(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP
  return 0;
}

/**
 * SumDouble - Sum for double precision floats
 */
Int32 AggInterpreterBase::SumDouble(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) {
    return 1;
  }

  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_DOUBLE;
    res->value.val_double = a.value.val_double;
    res->is_unsigned = false;
    res->is_null = false;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "SumDouble() init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }

  double res_val = a.value.val_double + res->value.val_double;

  if (unlikely(!std::isfinite(res_val))) {
    return -1;
  }

  res->value.val_double = res_val;
#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "SumDouble(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP
  return 0;
}

Int32 AggInterpreterBase::Max(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED || res->is_null) {
    // Agg result first initialized
    *res = a;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "Max(), init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  if (res->is_null) {
    assert(res->value.val_int64 == 0);
  }

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || res->type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT &&
          (res->type == NDB_TYPE_BIGINT || res->type == NDB_TYPE_UNDEFINED));
    res_type = NDB_TYPE_BIGINT;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    if (!a.is_unsigned && !res->is_unsigned) {
      res->value.val_int64 = (a.value.val_int64 > res->value.val_int64) ?
                              a.value.val_int64 : res->value.val_int64;
    } else if (a.is_unsigned && res->is_unsigned) {
      res->value.val_uint64 = (a.value.val_uint64 > res->value.val_uint64) ?
                              a.value.val_uint64 : res->value.val_uint64;
    } else if (a.is_unsigned && !res->is_unsigned) {
      if (res->value.val_int64 < 0) {
        res->value.val_uint64 = a.value.val_uint64;
      } else {
        res->value.val_uint64 = a.value.val_uint64 >
                static_cast<Uint64>(res->value.val_int64) ?
                a.value.val_uint64 :
                static_cast<Uint64>(res->value.val_int64);
      }
      res->is_unsigned = true;
    } else {
      assert(!a.is_unsigned && res->is_unsigned);
      if (a.value.val_int64 < 0) {
      } else {
        res->value.val_uint64 = static_cast<Uint64>(a.value.val_int64) >
                                res->value.val_uint64;
      }
    }
  } else {
    assert(res_type == NDB_TYPE_DOUBLE);
    res->value.val_double = (a.value.val_double > res->value.val_double) ?
                             a.value.val_double : res->value.val_double;
  }
  res->is_null = false;

#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "Max(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP

  return 0;
}

/**
 * MaxBigint - Max for BIGINT (handles both signed and unsigned dynamically)
 */
Int32 AggInterpreterBase::MaxBigint(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) {
    return 1;
  }

  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_BIGINT;
    res->value.val_int64 = a.value.val_int64;
    res->is_unsigned = a.is_unsigned;
    res->is_null = false;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "MaxBigint() init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }

  if (!a.is_unsigned && !res->is_unsigned) {
    if (a.value.val_int64 > res->value.val_int64) {
      res->value.val_int64 = a.value.val_int64;
    }
  } else if (a.is_unsigned && res->is_unsigned) {
    if (a.value.val_uint64 > res->value.val_uint64) {
      res->value.val_uint64 = a.value.val_uint64;
    }
  } else if (a.is_unsigned && !res->is_unsigned) {
    if (res->value.val_int64 < 0) {
      res->value.val_uint64 = a.value.val_uint64;
      res->is_unsigned = true;
    } else {
      if (a.value.val_uint64 > static_cast<Uint64>(res->value.val_int64)) {
        res->value.val_uint64 = a.value.val_uint64;
        res->is_unsigned = true;
      }
    }
  } else {
    // !a.is_unsigned && res->is_unsigned
    if (a.value.val_int64 >= 0) {
      if (static_cast<Uint64>(a.value.val_int64) > res->value.val_uint64) {
        res->value.val_uint64 = static_cast<Uint64>(a.value.val_int64);
      }
    }
    // If a is negative and res is unsigned, res is already larger
  }
#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "MaxBigint(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP
  return 0;
}

/**
 * MaxDouble - Max for double precision floats
 */
Int32 AggInterpreterBase::MaxDouble(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) {
    return 1;
  }

  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_DOUBLE;
    res->value.val_double = a.value.val_double;
    res->is_unsigned = false;
    res->is_null = false;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "MaxDouble() init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }

  if (a.value.val_double > res->value.val_double) {
    res->value.val_double = a.value.val_double;
  }
#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "MaxDouble(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP
  return 0;
}

Int32 AggInterpreterBase::Min(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED || res->is_null) {
    // Agg result first initialized
    *res = a;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "Min(), init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  if (res->is_null) {
    assert(res->value.val_int64 == 0);
  }

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || res->type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT &&
          (res->type == NDB_TYPE_BIGINT || res->type == NDB_TYPE_UNDEFINED));
    res_type = NDB_TYPE_BIGINT;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    if (!a.is_unsigned && !res->is_unsigned) {
      res->value.val_int64 = (a.value.val_int64 < res->value.val_int64) ?
                              a.value.val_int64 : res->value.val_int64;
    } else if (a.is_unsigned && res->is_unsigned) {
      res->value.val_uint64 = (a.value.val_uint64 < res->value.val_uint64) ?
                              a.value.val_uint64 : res->value.val_uint64;
    } else if (a.is_unsigned && !res->is_unsigned) {
      if (res->value.val_int64 < 0) {
      } else {
        res->value.val_uint64 = a.value.val_uint64 <
                static_cast<Uint64>(res->value.val_int64) ?
                a.value.val_uint64 :
                static_cast<Uint64>(res->value.val_int64);
        res->is_unsigned = true;
      }
    } else {
      assert(!a.is_unsigned && res->is_unsigned);
      if (a.value.val_int64 < 0) {
        res->value.val_int64 = a.value.val_int64;
        res->is_unsigned = false;
      } else {
        res->value.val_uint64 = static_cast<Uint64>(a.value.val_int64) <
                                res->value.val_uint64 ?
                                static_cast<Uint64>(a.value.val_int64) :
                                res->value.val_uint64;
      }
    }
  } else {
    assert(res_type == NDB_TYPE_DOUBLE);
    res->value.val_double = (a.value.val_double < res->value.val_double) ?
                             a.value.val_double : res->value.val_double;
  }
  res->is_null = false;

#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "Min(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP

  return 0;
}

/**
 * MinBigint - Min for BIGINT (handles both signed and unsigned dynamically)
 */
Int32 AggInterpreterBase::MinBigint(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) {
    return 1;
  }

  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_BIGINT;
    res->value.val_int64 = a.value.val_int64;
    res->is_unsigned = a.is_unsigned;
    res->is_null = false;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "MinBigint() init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }

  if (!a.is_unsigned && !res->is_unsigned) {
    if (a.value.val_int64 < res->value.val_int64) {
      res->value.val_int64 = a.value.val_int64;
    }
  } else if (a.is_unsigned && res->is_unsigned) {
    if (a.value.val_uint64 < res->value.val_uint64) {
      res->value.val_uint64 = a.value.val_uint64;
    }
  } else if (a.is_unsigned && !res->is_unsigned) {
    // a is unsigned, res is signed
    if (res->value.val_int64 < 0) {
      // res is negative, so res is smaller - keep res
    } else {
      if (a.value.val_uint64 < static_cast<Uint64>(res->value.val_int64)) {
        res->value.val_uint64 = a.value.val_uint64;
        res->is_unsigned = true;
      }
    }
  } else {
    // !a.is_unsigned && res->is_unsigned
    if (a.value.val_int64 < 0) {
      res->value.val_int64 = a.value.val_int64;
      res->is_unsigned = false;
    } else {
      if (static_cast<Uint64>(a.value.val_int64) < res->value.val_uint64) {
        res->value.val_uint64 = static_cast<Uint64>(a.value.val_int64);
      }
    }
  }
#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "MinBigint(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP
  return 0;
}

/**
 * MinDouble - Min for double precision floats
 */
Int32 AggInterpreterBase::MinDouble(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (unlikely(a.is_null)) {
    return 1;
  }

  if (unlikely(res->type == NDB_TYPE_UNDEFINED || res->is_null)) {
    res->type = NDB_TYPE_DOUBLE;
    res->value.val_double = a.value.val_double;
    res->is_unsigned = false;
    res->is_null = false;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "MinDouble() init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 0;
  }

  if (a.value.val_double < res->value.val_double) {
    res->value.val_double = a.value.val_double;
  }
#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "MinDouble(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP
  return 0;
}

Int32 AggInterpreterBase::Count(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED) {
    // Agg result first initialized
    res->type = NDB_TYPE_BIGINT;
    res->value.val_uint64 = 0;
    res->is_unsigned = true;
    res->is_null = false;
#ifdef DEBUG_PA_INTERP
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "Count(), init AggRes to ");
      PrintValue(res, log_buf);
    }
#endif // DEBUG_PA_INTERP
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  assert(res->type == NDB_TYPE_BIGINT &&
      res->is_null == false && res->is_unsigned == true);
  res->value.val_uint64 += 1;

#ifdef DEBUG_PA_INTERP
  if (print) {
    char log_buf[128];
    sprintf(log_buf, "Count(), update AggRes to ");
    PrintValue(res, log_buf);
  }
#endif // DEBUG_PA_INTERP

  return 0;
}

/*
 * Phase I.6 (F.2-K.5) string MIN/MAX helpers — bodies that used to live
 * verbatim in both AggInterpreter.cpp and JoinAggInterpreter.cpp.  Now
 * a single canonical copy on AggInterpreterBase; subclasses inherit
 * them.  See header for the per-method contract.  Step 1.3 of the
 * interpreter unification.
 */

// Phase I.6 (F.2-K.4e): release one group's string val_ptr buffers.
// Called from the per-batch drain (AggInterpreter) or from
// evictOneGroup (JoinAggInterpreter) after the wire-format emit has
// consumed the payload.  Cheap no-op when m_string_results is
// unallocated (program has no string MIN/MAX).
void AggInterpreterBase::freeGroupStringSlots(AggResItem* slots) {
  if (m_string_results == nullptr) {
    return;
  }
  for (Uint32 i = 0; i < m_n_agg_results; i++) {
    DataType t = slots[i].type;
    if ((t == NDB_TYPE_CHAR || t == NDB_TYPE_VARCHAR ||
         t == NDB_TYPE_LONGVARCHAR) &&
        slots[i].value.val_ptr != nullptr) {
      lc_ndbd_pool_free(slots[i].value.val_ptr);
      slots[i].value.val_ptr = nullptr;
    }
  }
}

// Phase I.6 (F.2-K.5): bytes that one group's appended string-payload
// region will consume on the wire.  Walks the slot array, summing the
// size contribution of each string slot:
// `[Uint32 byte_size][prefix+payload, Uint32-padded]`.  Non-string
// slots and null string slots contribute zero bytes.
Uint32 AggInterpreterBase::stringPayloadSize(const AggResItem* slots) const {
  if (m_string_results == nullptr) {
    return 0;
  }
  Uint32 total = 0;
  for (Uint32 i = 0; i < m_n_agg_results; i++) {
    DataType t = slots[i].type;
    if ((t == NDB_TYPE_CHAR || t == NDB_TYPE_VARCHAR ||
         t == NDB_TYPE_LONGVARCHAR) &&
        !slots[i].is_null && slots[i].value.val_ptr != nullptr) {
      const char* buf = static_cast<const char*>(slots[i].value.val_ptr);
      const Uint16 payload_len = *reinterpret_cast<const Uint16*>(buf);
      const Uint32 prefix = m_string_results[i].prefix_bytes;
      const Uint32 byte_size = prefix + payload_len;
      total += sizeof(Uint32);
      total += (byte_size + 3) & ~3U;
    }
  }
  return total;
}

// Phase I.6 (F.2-K.5): write one group's appended string-payload
// region into `dst`.  Caller must size `dst` from stringPayloadSize.
Uint32 AggInterpreterBase::encodeStringPayload(const AggResItem* slots,
                                                char* dst) const {
  if (m_string_results == nullptr) {
    return 0;
  }
  char* p = dst;
  for (Uint32 i = 0; i < m_n_agg_results; i++) {
    DataType t = slots[i].type;
    if ((t == NDB_TYPE_CHAR || t == NDB_TYPE_VARCHAR ||
         t == NDB_TYPE_LONGVARCHAR) &&
        !slots[i].is_null && slots[i].value.val_ptr != nullptr) {
      const char* buf = static_cast<const char*>(slots[i].value.val_ptr);
      const Uint16 payload_len = *reinterpret_cast<const Uint16*>(buf);
      const Uint32 prefix = m_string_results[i].prefix_bytes;
      const Uint32 byte_size = prefix + payload_len;
      *reinterpret_cast<Uint32*>(p) = byte_size;
      p += sizeof(Uint32);
      memcpy(p, buf + 4, byte_size);
      p += byte_size;
      const Uint32 pad = ((byte_size + 3) & ~3U) - byte_size;
      if (pad > 0) {
        memset(p, 0, pad);
        p += pad;
      }
    }
  }
  return static_cast<Uint32>(p - dst);
}

// Phase I.6 (F.2-K.4c): per-(group, slot) MIN/MAX string update.
// See header for layout and contract.
Int32 AggInterpreterBase::minMaxString(Uint32 reg_index, Uint32 agg_index,
                                       AggResItem* agg_res_ptr,
                                       bool is_max) {
  const Register& src_reg = m_registers[reg_index];
  if (src_reg.is_null) {
    return 0;
  }
  // Lazy-allocate the slot metadata array on the first string
  // MIN/MAX touch — non-string queries pay zero memory cost.
  if (m_string_results == nullptr) {
    Uint32 nbytes = m_n_agg_results * sizeof(StringResult);
    m_string_results = static_cast<StringResult*>(
        lc_ndbd_pool_malloc(nbytes, RG_QUERY_MEMORY,
                            m_current_thread_id, true));
    if (m_string_results == nullptr) {
      return ZAGG_ALLOC_MEM_FAILED;
    }
  }
  const StringResult& src = m_register_string_data[reg_index];
  StringResult& slot = m_string_results[agg_index];
  if (slot.declared_size == 0) {
    slot.charset = src.charset;
    slot.prefix_bytes = src.prefix_bytes;
    slot.declared_size = src.declared_size;
  }
  AggResItem& dst = agg_res_ptr[agg_index];
  const Uint32 needed_payload = src.prefix_bytes + src.length;
  Uint32 alloc_size = (4 + needed_payload + 15) & ~15U;
  if (alloc_size < 16) alloc_size = 16;

  if (dst.value.val_ptr == nullptr) {
    char* buf = static_cast<char*>(
        lc_ndbd_pool_malloc(alloc_size, RG_QUERY_MEMORY,
                            m_current_thread_id, false));
    if (buf == nullptr) {
      return ZAGG_ALLOC_MEM_FAILED;
    }
    Uint16* hdr = reinterpret_cast<Uint16*>(buf);
    hdr[0] = src.length;
    hdr[1] = static_cast<Uint16>(alloc_size - 4);
    if (needed_payload > 0) {
      memcpy(buf + 4, src.ptr, needed_payload);
    }
    dst.type = src_reg.type;
    dst.value.val_ptr = buf;
    dst.is_unsigned = false;
    dst.is_null = false;
    return 0;
  }

  char* old_buf = static_cast<char*>(dst.value.val_ptr);
  const Uint16* old_hdr = reinterpret_cast<const Uint16*>(old_buf);
  const Uint16 old_payload_len = old_hdr[0];
  const Uint16 old_capacity = old_hdr[1];
  const unsigned n_new = src.prefix_bytes + src.length;
  const unsigned n_old = slot.prefix_bytes + old_payload_len;
  const void* v_new = src.ptr;
  const void* v_old = old_buf + 4;
  const Uint32 type_id =
      (slot.prefix_bytes == 0) ? NDB_TYPE_CHAR :
      (slot.prefix_bytes == 1) ? NDB_TYPE_VARCHAR : NDB_TYPE_LONGVARCHAR;
  const NdbSqlUtil::Type& sqlType = NdbSqlUtil::getType(type_id);
  int cmp = (*sqlType.m_cmp)(slot.charset, v_new, n_new, v_old, n_old);
  const bool replace = is_max ? (cmp > 0) : (cmp < 0);
  if (!replace) return 0;

  if (needed_payload <= old_capacity) {
    Uint16* h = reinterpret_cast<Uint16*>(old_buf);
    h[0] = src.length;
    if (needed_payload > 0) {
      memcpy(old_buf + 4, src.ptr, needed_payload);
    }
  } else {
    // Allocate-then-free order keeps the existing winner intact on OOM.
    char* new_buf = static_cast<char*>(
        lc_ndbd_pool_malloc(alloc_size, RG_QUERY_MEMORY,
                            m_current_thread_id, false));
    if (new_buf == nullptr) {
      return ZAGG_ALLOC_MEM_FAILED;
    }
    Uint16* h = reinterpret_cast<Uint16*>(new_buf);
    h[0] = src.length;
    h[1] = static_cast<Uint16>(alloc_size - 4);
    if (needed_payload > 0) {
      memcpy(new_buf + 4, src.ptr, needed_payload);
    }
    dst.value.val_ptr = new_buf;
    lc_ndbd_pool_free(old_buf);
  }
  return 0;
}

/*
 * Step 1.4 — shared opcode handler.
 *
 * Covers the 28 opcode arms that were byte-identical between the two
 * subclasses' ProcessRec dispatches.  See AggInterpreterBase.hpp for
 * the parameter contract and the list of opcodes handled.  The two
 * divergent opcodes (kOpLoadCol, kOpEmbeddedInterp) are still handled
 * in each subclass's own switch.
 */
Int32 AggInterpreterBase::executeStandardOpcode(
    Uint8 op, Uint32 value, Uint32& exec_pos,
    AggResItem* agg_res_ptr, bool debug_print,
    bool* handled) {
  *handled = true;
  Uint32 reg_index;
  Uint32 reg_index2;
  Uint32 agg_index;
  Uint32 type;
  int ret;

  switch (op) {
    case kOpPlus:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegPlusReg(m_registers[reg_index], m_registers[reg_index2],
                       &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[PLUS], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpMinus:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegMinusReg(m_registers[reg_index], m_registers[reg_index2],
                        &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[MINUS], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpMul:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegMulReg(m_registers[reg_index], m_registers[reg_index2],
                      &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[MUL], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpDiv:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegDivReg(m_registers[reg_index], m_registers[reg_index2],
                      &m_registers[reg_index], false);
      if (ret < 0) {
        DEB_AGG(("Overflow[DIV], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpDivInt:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegDivReg(m_registers[reg_index], m_registers[reg_index2],
                      &m_registers[reg_index], true);
      if (ret < 0) {
        DEB_AGG(("Overflow[DIVINT], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpMod:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegModReg(m_registers[reg_index], m_registers[reg_index2],
                      &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[MOD], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    // Type-specific Plus operations
    case kOpPlusBigint:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegPlusBigint(m_registers[reg_index], m_registers[reg_index2],
                          &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[PlusBigint], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpPlusDouble:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegPlusDouble(m_registers[reg_index], m_registers[reg_index2],
                          &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[PlusDouble], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    // Type-specific Minus operations
    case kOpMinusBigint:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegMinusBigint(m_registers[reg_index], m_registers[reg_index2],
                           &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[MinusBigint], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpMinusDouble:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegMinusDouble(m_registers[reg_index], m_registers[reg_index2],
                           &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[MinusDouble], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    // Type-specific Multiply operations
    case kOpMulBigint:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegMulBigint(m_registers[reg_index], m_registers[reg_index2],
                         &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[MulBigint], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpMulDouble:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegMulDouble(m_registers[reg_index], m_registers[reg_index2],
                         &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[MulDouble], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    // Type-specific Division operations
    case kOpDivDouble:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegDivDouble(m_registers[reg_index], m_registers[reg_index2],
                         &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[DivDouble], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpDivIntBigint:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      ret = RegDivIntBigint(m_registers[reg_index], m_registers[reg_index2],
                            &m_registers[reg_index]);
      if (ret < 0) {
        DEB_AGG(("Overflow[DivIntBigint], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpLoadConst:
      type = (value & 0x03E00000) >> 21;
      reg_index = (value & 0x000F0000) >> 16;
      assert(type == NDB_TYPE_BIGINT || type == NDB_TYPE_BIGUNSIGNED ||
             type == NDB_TYPE_DOUBLE);
      ResetRegister(&m_registers[reg_index]);
      m_registers[reg_index].type = AlignedType(type, 0);
      m_registers[reg_index].is_unsigned = IsUnsigned(type);
      m_registers[reg_index].is_null = false;
      if (unlikely(exec_pos + 2 > m_prog_len)) {
        g_eventLogger->debug("AggInterpreterBase::executeStandardOpcode "
            "ZAGG_OTHER_ERROR: kOpLoadConst overflow exec_pos=%u "
            "prog_len=%u", exec_pos, m_prog_len);
        return ZAGG_OTHER_ERROR;
      }
      switch (type) {
        case NDB_TYPE_BIGINT:
          m_registers[reg_index].value.val_int64 =
              sint8korr(reinterpret_cast<char*>(&m_prog[exec_pos]));
          PA_INTERP_TRACE(m_frag_id,
                          "LoadConst[%u] NDB_TYPE_BIGINT %lld",
                          reg_index, m_registers[reg_index].value.val_int64);
          break;
        case NDB_TYPE_BIGUNSIGNED:
          m_registers[reg_index].value.val_uint64 =
              uint8korr(reinterpret_cast<char*>(&m_prog[exec_pos]));
          PA_INTERP_TRACE(m_frag_id,
                          "LoadConst[%u] NDB_TYPE_BIGUNSIGNED %llu",
                          reg_index, m_registers[reg_index].value.val_uint64);
          break;
        case NDB_TYPE_DOUBLE:
          m_registers[reg_index].value.val_double =
              doubleget(reinterpret_cast<unsigned char*>(&m_prog[exec_pos]));
          PA_INTERP_TRACE(m_frag_id,
                          "LoadConst[%u] NDB_TYPE_DOUBLE %lf",
                          reg_index, m_registers[reg_index].value.val_double);
          break;
        default:
          return ZAGG_LOAD_CONST_WRONG_TYPE;
      }
      exec_pos += 2;
      return 0;

    case kOpMov:
      reg_index = (value >> 12) & 0x0F;
      reg_index2 = (value >> 8) & 0x0F;
      m_registers[reg_index] = m_registers[reg_index2];
      PA_INTERP_TRACE(m_frag_id,
                      "Move [%u]->[%u]",
                      reg_index2, reg_index);
      return 0;

    case kOpSetRegNull:
      reg_index = (value & 0x000F0000) >> 16;
      if (m_registers[reg_index].type == NDB_TYPE_UNDEFINED) {
        m_registers[reg_index].type = NDB_TYPE_BIGINT;
        m_registers[reg_index].is_unsigned = false;
        m_registers[reg_index].value.val_int64 = 0;
      }
      m_registers[reg_index].is_null = true;
      PA_INTERP_TRACE(m_frag_id, "SetRegNull[%u]", reg_index);
      return 0;

    case kOpSum:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      ret = Sum(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      if (ret < 0) {
        DEB_AGG(("Overflow[SUM], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpMax:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      if (m_registers[reg_index].type == NDB_TYPE_CHAR ||
          m_registers[reg_index].type == NDB_TYPE_VARCHAR ||
          m_registers[reg_index].type == NDB_TYPE_LONGVARCHAR) {
        /* minMaxString returns 0 on success or ZAGG_ALLOC_MEM_FAILED;
         * propagate as-is. */
        return minMaxString(reg_index, agg_index, agg_res_ptr, /*is_max=*/true);
      }
      /* Max returns 1 on "first row" / null short-circuit, 0 on a
       * normal update, never an error code.  Original dispatch
       * discarded the return value via `ret = ...; break;` — keep that
       * behavior here so a positive return doesn't surface as an
       * agg-interp failure. */
      Max(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      return 0;

    case kOpMin:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      if (m_registers[reg_index].type == NDB_TYPE_CHAR ||
          m_registers[reg_index].type == NDB_TYPE_VARCHAR ||
          m_registers[reg_index].type == NDB_TYPE_LONGVARCHAR) {
        return minMaxString(reg_index, agg_index, agg_res_ptr, /*is_max=*/false);
      }
      /* See kOpMax: Min's positive return is the null/first-row
       * short-circuit, not an error. */
      Min(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      return 0;

    case kOpCount:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      Count(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      return 0;

    // Type-specific Sum operations
    case kOpSumBigint:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      ret = SumBigint(m_registers[reg_index], &agg_res_ptr[agg_index],
                      debug_print);
      if (ret < 0) {
        DEB_AGG(("Overflow[SumBigint], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    case kOpSumDouble:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      ret = SumDouble(m_registers[reg_index], &agg_res_ptr[agg_index],
                      debug_print);
      if (ret < 0) {
        DEB_AGG(("Overflow[SumDouble], value is out of range"));
        return ZAGG_MATH_OVERFLOW;
      }
      return 0;

    // Type-specific Max operations
    case kOpMaxBigint:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      MaxBigint(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      return 0;

    case kOpMaxDouble:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      MaxDouble(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      return 0;

    // Type-specific Min operations
    case kOpMinBigint:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      MinBigint(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      return 0;

    case kOpMinDouble:
      reg_index = (value & 0x000F0000) >> 16;
      agg_index = (value & 0x0000FFFF);
      MinDouble(m_registers[reg_index], &agg_res_ptr[agg_index], debug_print);
      return 0;

    case kOpSkip: {
      Uint32 skip_count = value & 0xFFFF;
      exec_pos += skip_count;
      return 0;
    }

    default:
      *handled = false;
      return 0;
  }
}

/*
 * Step 2a — chunk allocator (lifted from JoinAggInterpreter).
 *
 * Per-group records live in MEM_CHUNK_SIZE (32 KB) pages allocated from
 * RG_QUERY_MEMORY.  Each page has a `MemChunk` header at offset 0 with
 * a singly-linked list of live groups carved from `data`; the doubly-
 * linked chunk list `m_chunks` / `m_chunks_tail` lets `freeGroupData`
 * unlink the page in O(1) once `live_groups` hits zero.
 *
 * No behavior change: both subclasses end up invoking this code via
 * inherited name lookup.  In Step 2a only JoinAggInterpreter actually
 * uses these methods; AggInterpreter still runs on its `std::map` +
 * inline `m_mem_buf` allocator until Step 2b switches it over.
 */
void AggInterpreterBase::initChunkAllocator(Uint32 thread_id,
                                             Uint32 budget_pages,
                                             Uint32 available_pages) {
  m_thread_id = thread_id;
  m_memory_budget = budget_pages * MEM_CHUNK_SIZE;
  m_budget_increment = m_memory_budget;
  m_total_available = available_pages * MEM_CHUNK_SIZE;
  m_chunks = nullptr;
  m_chunks_tail = nullptr;
  m_current_chunk = nullptr;
  m_total_chunk_bytes = 0;
}

bool AggInterpreterBase::bookMoreMemory() {
  Uint32 new_budget = m_memory_budget + m_budget_increment;
  if (new_budget > m_total_available) {
    return false;
  }
  m_memory_budget = new_budget;
  return true;
}

MemChunk* AggInterpreterBase::allocNewChunk() {
  if (m_total_chunk_bytes + MEM_CHUNK_SIZE > m_memory_budget) {
    if (!bookMoreMemory()) {
      return nullptr;
    }
  }
  void* page = lc_ndbd_pool_malloc(MEM_CHUNK_SIZE, RG_QUERY_MEMORY,
                                   m_thread_id, false);
  if (page == nullptr) {
    return nullptr;
  }
  MemChunk* chunk = static_cast<MemChunk*>(page);
  chunk->data = static_cast<char*>(page) + sizeof(MemChunk);
  chunk->capacity = MEM_CHUNK_SIZE - sizeof(MemChunk);
  chunk->used = 0;
  chunk->live_groups = 0;
  chunk->group_list = nullptr;
  chunk->next = m_chunks;
  chunk->prev = nullptr;
  if (m_chunks != nullptr) {
    m_chunks->prev = chunk;
  } else {
    m_chunks_tail = chunk;
  }
  m_chunks = chunk;
  m_total_chunk_bytes += MEM_CHUNK_SIZE;
  return chunk;
}

char* AggInterpreterBase::allocGroupData(Uint32 len, Uint32 key_len) {
  Uint32 total = ((GROUP_LINK_OVERHEAD + len) + 7) & ~7u;
  MemChunk* chunk = m_current_chunk;
  if (chunk == nullptr || chunk->used + total > chunk->capacity) {
    chunk = allocNewChunk();
    if (chunk == nullptr) {
      return nullptr;
    }
    m_current_chunk = chunk;
    if (total > chunk->capacity) {
      return nullptr;
    }
  }
  Uint32 offset = chunk->used;
  char* raw = chunk->data + offset;
  chunk->used += total;
  chunk->live_groups++;

  *reinterpret_cast<char**>(raw) = chunk->group_list;
  *reinterpret_cast<char**>(raw + sizeof(char*)) = nullptr;
  *reinterpret_cast<Uint32*>(raw + 2 * sizeof(char*)) = key_len;
  *reinterpret_cast<Uint32*>(raw + 2 * sizeof(char*) + sizeof(Uint32)) = offset;
  chunk->group_list = raw;

  return raw + GROUP_LINK_OVERHEAD;
}

void AggInterpreterBase::freeGroupData(char* ptr) {
  char* raw = ptr - GROUP_LINK_OVERHEAD;
  Uint32 offset = *reinterpret_cast<Uint32*>(raw + 2 * sizeof(char*) + sizeof(Uint32));
  MemChunk* chunk = reinterpret_cast<MemChunk*>(raw - offset - sizeof(MemChunk));
  chunk->live_groups--;
  if (chunk->live_groups == 0) {
    if (chunk->prev != nullptr) {
      chunk->prev->next = chunk->next;
    } else {
      m_chunks = chunk->next;
    }
    if (chunk->next != nullptr) {
      chunk->next->prev = chunk->prev;
    } else {
      m_chunks_tail = chunk->prev;
    }
    if (m_current_chunk == chunk) {
      m_current_chunk = m_chunks;
    }
    m_total_chunk_bytes -= MEM_CHUNK_SIZE;
    lc_ndbd_pool_free(chunk);
  }
}

void AggInterpreterBase::freeAllChunks() {
  MemChunk* chunk = m_chunks;
  while (chunk != nullptr) {
    MemChunk* next = chunk->next;
    lc_ndbd_pool_free(chunk);
    chunk = next;
  }
  m_chunks = nullptr;
  m_chunks_tail = nullptr;
  m_current_chunk = nullptr;
  m_total_chunk_bytes = 0;
}

/*
 * Step 2b — shared GROUP BY type-metadata initializer.
 *
 * Lifted from JoinAggInterpreter.  Resolves each GB column's type
 * info (typeId / maxBytes / charset / cmpFn) into m_gb_types[],
 * allocates m_xfrm_buf if any column has a charset (sized for the
 * widest strnxfrm_hash output), and publishes the metadata to
 * m_gb_map via setTypeMeta.
 *
 * linked_attr_data / linked_attr_len are the per-row linked-attr
 * buffer JoinAgg passes for join queries (kOpLoadCol-equivalent
 * linked-GB columns).  AggInterpreter (normal scan) passes
 * nullptr / 0; the linked branches below are dead code on that
 * path because the attr_id 0x8000 bit never appears in a
 * normal-scan GB column.
 */
Int32 AggInterpreterBase::initGBTypes(
    Dbtup* block_tup,
    Dbtup::KeyReqStruct* req_struct,
    const Uint32* linked_attr_data,
    Uint32 linked_attr_len,
    EmulatedJamBuffer *jamBuf) {
  for (Uint32 i = 0; i < m_n_gb_cols; i++) {
    thrjamDebug(jamBuf);
    Uint32 attr_id = m_gb_cols[i] >> 16;
    thrjamDataDebug(jamBuf, attr_id);
    GBColTypeInfo &info = m_gb_types[i];

    if ((attr_id & 0x8000) != 0) {
      thrjam(jamBuf);
      if (unlikely(linked_attr_data == nullptr)) {
        g_eventLogger->debug(
            "initGBTypes: linked GB col %u (attr_id=0x%x) but "
            "linked_attr_data is NULL — API likely missing "
            "addLinkedProjection for the position", i, attr_id);
        return ZAGG_OTHER_ERROR;
      }
      Uint32 position = attr_id & 0x7FFF;
      const Uint32* p = linked_attr_data;
      const Uint32* p_end = linked_attr_data + linked_attr_len;
      Uint32 pos_count = 0;
      while (p < p_end && pos_count < position) {
        p += 2;
        p += 1 + AttributeHeader::getDataSize(*p);
        pos_count++;
      }
      if (unlikely(p + 2 >= p_end)) {
        g_eventLogger->debug("initGBTypes: linked buffer too short for "
            "position %u (linked_len=%u)", position, linked_attr_len);
        return ZAGG_OTHER_ERROR;
      }
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
      } else {
        thrjamDebug(jamBuf);
        Uint32 tableId = word0;
        Uint32 tableVersion = word1;
        Uint32 linkedAttrId = AttributeHeader(p[2]).getAttributeId();
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
          const NdbSqlUtil::Type &sqlType = NdbSqlUtil::getType(info.typeId);
          info.cmpFn = sqlType.m_cmp;
          continue;
        }
        require(tableId != 0);

        Dblqh* lqh = block_tup->c_lqh;
        if (unlikely(tableId >= lqh->ctabrecFileSize)) {
          g_eventLogger->debug("initGBTypes: tableId %u out of range "
              "(max=%u)", tableId, lqh->ctabrecFileSize);
          return ZINVALID_SCHEMA_VERSION;
        }
        if (unlikely(table_version_major(tableVersion) !=
                     table_version_major(
                         lqh->tablerec[tableId].schemaVersion))) {
          g_eventLogger->debug("initGBTypes: schema version mismatch for "
              "tableId %u: linked=%u, current=%u",
              tableId, tableVersion, lqh->tablerec[tableId].schemaVersion);
          return ZINVALID_SCHEMA_VERSION;
        }

        if (unlikely(tableId >= block_tup->cnoOfTablerec)) {
          g_eventLogger->debug("initGBTypes: tableId %u out of range for "
              "DBTUP (max=%u)", tableId, block_tup->cnoOfTablerec);
          return ZINVALID_SCHEMA_VERSION;
        }
        Dbtup::Tablerec* tab = &block_tup->tablerec[tableId];
        const Uint32* attrDesc = tab->tabDescriptor + linkedAttrId * ZAD_SIZE;
        info.typeId = AttributeDescriptor::getType(attrDesc[0]);
        info.maxBytes = AttributeDescriptor::getSizeInBytes(attrDesc[0]);
        info.cs = nullptr;
        if (AttributeOffset::getCharsetFlag(attrDesc[1])) {
          thrjamDebug(jamBuf);
          Uint32 csPos = AttributeOffset::getCharsetPos(attrDesc[1]);
          info.cs = tab->charsetArray[csPos];
        }
      }
    } else {
      thrjam(jamBuf);
      /* Normal (non-linked) GROUP BY column.  Resolving its type needs a
       * valid tablePtrP, which only a real scanned-table request supplies.
       * A CTE_LOOKUP / CTE_SCAN agg feed has no scanned table (the row comes
       * from the linked buffer) and sets tablePtrP == nullptr, so reaching
       * here means the aggregator references a normal column in a context
       * that cannot supply one.  Abort the query cleanly rather than
       * dereference a null/poisoned tablePtrP. */
      if (unlikely(req_struct == nullptr ||
                   req_struct->tablePtrP == nullptr)) {
        g_eventLogger->debug(
            "initGBTypes: normal GROUP BY column attr_id=%u referenced in a "
            "CTE agg-feed with no scanned table — aborting query", attr_id);
        return ZAGG_OTHER_ERROR;
      }
      const Uint32* attrDesc = req_struct->tablePtrP->tabDescriptor +
          attr_id * ZAD_SIZE;
      info.typeId = AttributeDescriptor::getType(attrDesc[0]);
      info.maxBytes = AttributeDescriptor::getSizeInBytes(attrDesc[0]);
      info.cs = nullptr;
      if (AttributeOffset::getCharsetFlag(attrDesc[1])) {
        thrjamDebug(jamBuf);
        Uint32 csPos = AttributeOffset::getCharsetPos(attrDesc[1]);
        info.cs = req_struct->tablePtrP->charsetArray[csPos];
      }
    }
    const NdbSqlUtil::Type &sqlType = NdbSqlUtil::getType(info.typeId);
    info.cmpFn = sqlType.m_cmp;
  }
  return publishGBTypes(jamBuf);
}

Int32 AggInterpreterBase::initGBTypesFromMetadata(
    const Uint32* metadata,
    Uint32 metadataLen,
    EmulatedJamBuffer *jamBuf) {
  if (metadata == nullptr || metadataLen == 0) {
    return 0;
  }
  if (unlikely(metadataLen < 3 ||
               metadata[0] != JOIN_AGG_META_MARKER ||
               metadata[1] != JOIN_AGG_META_VERSION)) {
    return ZAGG_OTHER_ERROR;
  }

  const Uint32 entryCount = metadata[2];
  if (unlikely(entryCount == 0 ||
               entryCount > (metadataLen - 3) / JOIN_AGG_META_ENTRY_WORDS ||
               metadataLen != 3 + entryCount * JOIN_AGG_META_ENTRY_WORDS)) {
    return ZAGG_OTHER_ERROR;
  }

  bool found[MAX_AGG_N_GROUPBY_COLS];
  memset(found, 0, sizeof(found));
  Uint32 foundCount = 0;
  bool haveGroupByMetadata = false;
  const Uint32* entry = metadata + 3;
  for (Uint32 e = 0; e < entryCount; e++) {
    const Uint32 sourceKind = entry[0];
    const Uint32 tableId = entry[4];
    const Uint32 tableVersion = entry[5];
    const Uint32 columnId = entry[6];
    const Uint32 slotIndex = entry[3];
    const Uint32 typeId = entry[7];
    const Uint32 maxBytes = entry[8];
    const Uint32 csNumber = entry[9];
    const Uint32 flags = entry[11];
    const Uint32 knownFlags = JOIN_AGG_META_FLAG_UNSIGNED |
                              JOIN_AGG_META_FLAG_NULLABLE |
                              JOIN_AGG_META_FLAG_GROUP_BY |
                              JOIN_AGG_META_FLAG_LOAD_COLUMN;
    const bool isGroupBy =
        (flags & JOIN_AGG_META_FLAG_GROUP_BY) != 0;
    const bool isLoadColumn =
        (flags & JOIN_AGG_META_FLAG_LOAD_COLUMN) != 0;

    if (unlikely(sourceKind != JOIN_AGG_META_SOURCE_LOCAL_COLUMN &&
                 sourceKind != JOIN_AGG_META_SOURCE_LINKED_COLUMN &&
                 sourceKind != JOIN_AGG_META_SOURCE_CTE_COLUMN)) {
      return ZAGG_OTHER_ERROR;
    }
    if (unlikely((flags & ~knownFlags) != 0 ||
                 isGroupBy == isLoadColumn)) {
      return ZAGG_OTHER_ERROR;
    }
    Int32 ret = initColumnMetaFromMetadata(tableId, tableVersion, columnId,
                                           typeId, maxBytes, csNumber,
                                           entryCount);
    if (unlikely(ret != 0)) {
      return ret;
    }

    if (isGroupBy) {
      haveGroupByMetadata = true;
      if (m_gb_types_inited) {
        entry += JOIN_AGG_META_ENTRY_WORDS;
        continue;
      }
      if (unlikely(m_gb_map == nullptr ||
                   slotIndex >= m_n_gb_cols ||
                   slotIndex >= MAX_AGG_N_GROUPBY_COLS ||
                   found[slotIndex])) {
        return ZAGG_OTHER_ERROR;
      }

      GBColTypeInfo &info = m_gb_types[slotIndex];
      info.typeId = typeId;
      info.maxBytes = maxBytes;
      info.cs = nullptr;
      if (csNumber != 0) {
        if (unlikely(csNumber >= NDB_ARRAY_SIZE(all_charsets) ||
                     all_charsets[csNumber] == nullptr)) {
          return ZAGG_OTHER_ERROR;
        }
        info.cs = all_charsets[csNumber];
      }
      const NdbSqlUtil::Type &sqlType = NdbSqlUtil::getType(info.typeId);
      info.cmpFn = sqlType.m_cmp;
      found[slotIndex] = true;
      foundCount++;
    }
    if (isLoadColumn) {
      ret = initLoadColumnMetaFromMetadata(entry[2], typeId, maxBytes,
                                           csNumber, entryCount);
      if (unlikely(ret != 0)) {
        return ret;
      }
      ret = initStringAggSlotFromMetadata(slotIndex, typeId, maxBytes,
                                          csNumber);
      if (unlikely(ret != 0)) {
        return ret;
      }
    }
    entry += JOIN_AGG_META_ENTRY_WORDS;
  }

  if (m_gb_types_inited || !haveGroupByMetadata) {
    return 0;
  }
  if (unlikely(foundCount != m_n_gb_cols)) {
    return ZAGG_OTHER_ERROR;
  }

  return publishGBTypes(jamBuf);
}

Int32 AggInterpreterBase::publishGBTypes(EmulatedJamBuffer *jamBuf) {
  if (m_gb_types_inited || m_n_gb_cols == 0) {
    return 0;
  }
  if (unlikely(m_gb_map == nullptr)) {
    return ZAGG_OTHER_ERROR;
  }

  Uint32 max_xfrm_len = 0;
  for (Uint32 i = 0; i < m_n_gb_cols; i++) {
    thrjamDebug(jamBuf);
    if (m_gb_types[i].cs != nullptr) {
      Uint32 lb = 0;
      if (m_gb_types[i].typeId == NDB_TYPE_VARCHAR) lb = 1;
      else if (m_gb_types[i].typeId == NDB_TYPE_LONGVARCHAR) lb = 2;
      if (unlikely(m_gb_types[i].maxBytes < lb)) {
        return ZAGG_OTHER_ERROR;
      }
      Uint32 defLen = m_gb_types[i].maxBytes - lb;
      Uint32 xfrm_len = NdbSqlUtil::strnxfrm_hash_len(m_gb_types[i].cs,
                                                       defLen);
      if (xfrm_len > max_xfrm_len) max_xfrm_len = xfrm_len;
    }
  }
  if (max_xfrm_len > 0) {
    void* p = lc_ndbd_pool_malloc(max_xfrm_len, RG_QUERY_MEMORY,
                                  m_thread_id, false);
    if (unlikely(p == nullptr)) {
      g_eventLogger->debug("publishGBTypes: failed to allocate xfrm buffer "
          "(%u bytes)", max_xfrm_len);
      return ZAGG_OTHER_ERROR;
    }
    m_xfrm_buf = static_cast<uchar*>(p);
    m_xfrm_buf_len = max_xfrm_len;
  }

  m_gb_types_inited = true;
  m_gb_map->setTypeMeta(m_gb_types, m_n_gb_cols, m_xfrm_buf, m_xfrm_buf_len);
  return 0;
}

Int32 AggInterpreterBase::initStringAggSlotFromMetadata(
    Uint32 aggIndex,
    Uint32 typeId,
    Uint32 maxBytes,
    Uint32 csNumber) {
  if (typeId != NDB_TYPE_CHAR &&
      typeId != NDB_TYPE_VARCHAR &&
      typeId != NDB_TYPE_LONGVARCHAR) {
    return 0;
  }
  if (unlikely(aggIndex >= m_n_agg_results ||
               aggIndex >= MAX_AGG_N_RESULTS)) {
    return ZAGG_OTHER_ERROR;
  }

  const Uint32 prefix =
      typeId == NDB_TYPE_CHAR ? 0 :
      typeId == NDB_TYPE_VARCHAR ? 1 : 2;
  if (unlikely(maxBytes < prefix || maxBytes > UINT16_MAX)) {
    return ZAGG_OTHER_ERROR;
  }

  const CHARSET_INFO *cs = nullptr;
  if (csNumber != 0) {
    if (unlikely(csNumber >= NDB_ARRAY_SIZE(all_charsets) ||
                 all_charsets[csNumber] == nullptr)) {
      return ZAGG_OTHER_ERROR;
    }
    cs = all_charsets[csNumber];
  }

  if (m_string_results == nullptr) {
    Uint32 nbytes = m_n_agg_results * sizeof(StringResult);
    m_string_results = static_cast<StringResult*>(
        lc_ndbd_pool_malloc(nbytes, RG_QUERY_MEMORY, m_thread_id, true));
    if (m_string_results == nullptr) {
      return ZAGG_ALLOC_MEM_FAILED;
    }
  }

  StringResult &slot = m_string_results[aggIndex];
  if (unlikely(slot.ptr != nullptr)) {
    return ZAGG_OTHER_ERROR;
  }
  slot.length = 0;
  slot.size = 0;
  slot.prefix_bytes = static_cast<Uint16>(prefix);
  slot.declared_size = static_cast<Uint16>(maxBytes);
  slot.charset = cs;
  return 0;
}

Int32 AggInterpreterBase::initLoadColumnMetaFromMetadata(
    Uint32 programOffset,
    Uint32 typeId,
    Uint32 maxBytes,
    Uint32 csNumber,
    Uint32 entryCapacity) {
  if (unlikely(programOffset == RNIL)) {
    return ZAGG_OTHER_ERROR;
  }
  if (csNumber != 0) {
    if (unlikely(csNumber >= NDB_ARRAY_SIZE(all_charsets) ||
                 all_charsets[csNumber] == nullptr)) {
      return ZAGG_OTHER_ERROR;
    }
  }

  for (Uint32 i = 0; i < m_load_column_meta_count; i++) {
    LoadColumnMeta &meta = m_load_column_meta[i];
    if (meta.programOffset == programOffset) {
      if (unlikely(meta.typeId != typeId ||
                   meta.maxBytes != maxBytes ||
                   meta.csNumber != csNumber)) {
        return ZAGG_OTHER_ERROR;
      }
      return 0;
    }
  }

  if (m_load_column_meta == nullptr) {
    if (unlikely(entryCapacity == 0)) {
      return ZAGG_OTHER_ERROR;
    }
    m_load_column_meta = static_cast<LoadColumnMeta*>(
        lc_ndbd_pool_malloc(entryCapacity * sizeof(LoadColumnMeta),
                            RG_QUERY_MEMORY, m_thread_id, false));
    if (m_load_column_meta == nullptr) {
      return ZAGG_ALLOC_MEM_FAILED;
    }
    m_load_column_meta_capacity = entryCapacity;
    m_load_column_meta_count = 0;
  }
  if (unlikely(m_load_column_meta_count >= m_load_column_meta_capacity)) {
    return ZAGG_OTHER_ERROR;
  }

  LoadColumnMeta &meta = m_load_column_meta[m_load_column_meta_count++];
  meta.programOffset = programOffset;
  meta.typeId = typeId;
  meta.maxBytes = maxBytes;
  meta.csNumber = csNumber;
  return 0;
}

static Uint32 joinAggColumnMetaHash(Uint32 tableId,
                                    Uint32 tableVersion,
                                    Uint32 columnId) {
  Uint32 h = tableId * 2654435761U;
  h ^= table_version_major(tableVersion) * 2246822519U;
  h ^= columnId * 3266489917U;
  h ^= h >> 16;
  return h;
}

static Uint32 joinAggColumnMetaHashSize(Uint32 entryCapacity) {
  Uint32 hashSize = 8;
  while (hashSize < entryCapacity * 2) {
    hashSize <<= 1;
  }
  return hashSize;
}

Int32 AggInterpreterBase::initColumnMetaFromMetadata(
    Uint32 tableId,
    Uint32 tableVersion,
    Uint32 columnId,
    Uint32 typeId,
    Uint32 maxBytes,
    Uint32 csNumber,
    Uint32 entryCapacity) {
  if (tableId == RNIL || tableId == 0 || tableVersion == 0) {
    return 0;
  }
  if (csNumber != 0) {
    if (unlikely(csNumber >= NDB_ARRAY_SIZE(all_charsets) ||
                 all_charsets[csNumber] == nullptr)) {
      return ZAGG_OTHER_ERROR;
    }
  }

  if (m_column_meta == nullptr) {
    if (unlikely(entryCapacity == 0)) {
      return ZAGG_OTHER_ERROR;
    }
    const Uint32 hashSize = joinAggColumnMetaHashSize(entryCapacity);
    m_column_meta = static_cast<ColumnMeta*>(
        lc_ndbd_pool_malloc(entryCapacity * sizeof(ColumnMeta),
                            RG_QUERY_MEMORY, m_thread_id, false));
    if (m_column_meta == nullptr) {
      return ZAGG_ALLOC_MEM_FAILED;
    }
    m_column_meta_hash = static_cast<Uint32*>(
        lc_ndbd_pool_malloc(hashSize * sizeof(Uint32),
                            RG_QUERY_MEMORY, m_thread_id, false));
    if (m_column_meta_hash == nullptr) {
      lc_ndbd_pool_free(m_column_meta);
      m_column_meta = nullptr;
      return ZAGG_ALLOC_MEM_FAILED;
    }
    for (Uint32 i = 0; i < hashSize; i++) {
      m_column_meta_hash[i] = RNIL;
    }
    m_column_meta_capacity = entryCapacity;
    m_column_meta_hash_size = hashSize;
    m_column_meta_count = 0;
  }

  const Uint32 hash =
      joinAggColumnMetaHash(tableId, tableVersion, columnId) &
      (m_column_meta_hash_size - 1);
  Uint32 metaIndex = m_column_meta_hash[hash];
  while (metaIndex != RNIL) {
    ColumnMeta &meta = m_column_meta[metaIndex];
    if (meta.tableId == tableId &&
        table_version_major(meta.tableVersion) ==
            table_version_major(tableVersion) &&
        meta.columnId == columnId) {
      if (unlikely(meta.typeId != typeId ||
                   meta.maxBytes != maxBytes ||
                   meta.csNumber != csNumber)) {
        return ZAGG_OTHER_ERROR;
      }
      return 0;
    }
    metaIndex = meta.nextIndex;
  }

  if (unlikely(m_column_meta_count >= m_column_meta_capacity)) {
    return ZAGG_OTHER_ERROR;
  }
  ColumnMeta &meta = m_column_meta[m_column_meta_count];
  meta.tableId = tableId;
  meta.tableVersion = tableVersion;
  meta.columnId = columnId;
  meta.typeId = typeId;
  meta.maxBytes = maxBytes;
  meta.csNumber = csNumber;
  meta.nextIndex = m_column_meta_hash[hash];
  m_column_meta_hash[hash] = m_column_meta_count;
  m_column_meta_count++;
  return 0;
}

const AggInterpreterBase::LoadColumnMeta*
AggInterpreterBase::findLoadColumnMeta(Uint32 programOffset) const {
  for (Uint32 i = 0; i < m_load_column_meta_count; i++) {
    if (m_load_column_meta[i].programOffset == programOffset) {
      return &m_load_column_meta[i];
    }
  }
  return nullptr;
}

const AggInterpreterBase::ColumnMeta*
AggInterpreterBase::findColumnMeta(Uint32 tableId,
                                   Uint32 tableVersion,
                                   Uint32 columnId) const {
  if (m_column_meta_hash == nullptr ||
      m_column_meta_hash_size == 0 ||
      tableId == RNIL ||
      tableId == 0 ||
      tableVersion == 0) {
    return nullptr;
  }
  const Uint32 hash =
      joinAggColumnMetaHash(tableId, tableVersion, columnId) &
      (m_column_meta_hash_size - 1);
  Uint32 metaIndex = m_column_meta_hash[hash];
  while (metaIndex != RNIL) {
    const ColumnMeta &meta = m_column_meta[metaIndex];
    if (meta.tableId == tableId &&
        table_version_major(meta.tableVersion) ==
            table_version_major(tableVersion) &&
        meta.columnId == columnId) {
      return &meta;
    }
    metaIndex = meta.nextIndex;
  }
  return nullptr;
}

/*
 * Step 3a-A — wire-format header sizes used by both interpreters.
 * Definitions previously duplicated in each subclass .cpp.
 */
Uint32 AggInterpreterBase::g_attr_read_buf_len_ =
    Dbtup::AGG_ATTR_READ_BUF_WORD_SIZE;
Uint32 AggInterpreterBase::g_result_header_size_ = 3 * sizeof(Uint32);
Uint32 AggInterpreterBase::g_result_header_size_per_group_ = sizeof(Uint32);

/*
 * Step 4 — chunked teardown.  Drains up to max_groups groups from
 * m_gb_map per call.  When the map is empty, finishes the
 * per-instance cleanup (scalar string slots, m_string_results,
 * m_xfrm_buf, freeAllChunks) and returns true.  Subsequent
 * Destruct/~AggInterpreterBase() only needs to release m_buf_block —
 * the destructor body is nullptr-guarded throughout, so a clean
 * tearDownChunk run leaves it with no real work.
 *
 * Returning false means: at least one more tearDownChunk(N) call is
 * required.  Caller schedules a CONTINUEB and re-enters.
 *
 * Idempotent re-entry on a fully drained interpreter returns true
 * immediately (no work).
 */
bool AggInterpreterBase::tearDownChunk(Uint32 max_count) {
  /* Phase 1: drain up to max_count groups from m_gb_map.  Each
   * freeGroupData call releases the underlying chunk slot, so chunks
   * are freed incrementally as their last live group leaves. */
  if (m_gb_map != nullptr && !m_gb_map->empty()) {
    Uint32 count = 0;
    auto iter = m_gb_map->begin();
    while (iter.valid() && count < max_count) {
      Uint32 key_len = iter.keyLen();
      char* key_ptr = iter.data();
      if (m_string_results != nullptr) {
        AggResItem* slots =
            reinterpret_cast<AggResItem*>(key_ptr + key_len);
        freeGroupStringSlots(slots);
      }
      m_gb_map->eraseAndNext(iter);
      freeGroupData(key_ptr);
      count++;
    }
    if (!m_gb_map->empty()) {
      return false;  /* More groups remain — caller re-schedules. */
    }
  }
  /* Phase 2: scalar (no-GROUP-BY) string winners + m_string_results
   * metadata array.  One-shot; idempotent on re-entry. */
  if (m_agg_results != nullptr && m_string_results != nullptr) {
    freeGroupStringSlots(m_agg_results);
  }
  if (m_string_results != nullptr) {
    lc_ndbd_pool_free(m_string_results);
    m_string_results = nullptr;
  }
  /* Phase 3: drain chunks.  In normal flow Phase 1 has already
   * released every chunk via freeGroupData (each chunk auto-frees
   * when its last live_groups hits zero), so m_chunks is nullptr by
   * the time we get here and this loop is a no-op.  The bounded walk
   * is defense-in-depth for cases where the live_groups accounting
   * leaves chunks behind — at the 10 µs budget per signal we don't
   * want a 4096-chunk fallback walk to run synchronously. */
  if (m_chunks != nullptr) {
    Uint32 count = 0;
    while (m_chunks != nullptr && count < max_count) {
      MemChunk* chunk = m_chunks;
      m_chunks = chunk->next;
      lc_ndbd_pool_free(chunk);
      count++;
    }
    if (m_chunks != nullptr) {
      return false;  /* More chunks remain — caller re-schedules. */
    }
    m_chunks_tail = nullptr;
    m_current_chunk = nullptr;
    m_total_chunk_bytes = 0;
  }
  /* Phase 4: xfrm scratch (one-shot, idempotent). */
  if (m_xfrm_buf != nullptr) {
    lc_ndbd_pool_free(m_xfrm_buf);
    m_xfrm_buf = nullptr;
  }
  if (m_load_column_meta != nullptr) {
    lc_ndbd_pool_free(m_load_column_meta);
    m_load_column_meta = nullptr;
    m_load_column_meta_count = 0;
    m_load_column_meta_capacity = 0;
  }
  if (m_column_meta != nullptr) {
    lc_ndbd_pool_free(m_column_meta);
    m_column_meta = nullptr;
    m_column_meta_count = 0;
    m_column_meta_capacity = 0;
  }
  if (m_column_meta_hash != nullptr) {
    lc_ndbd_pool_free(m_column_meta_hash);
    m_column_meta_hash = nullptr;
    m_column_meta_hash_size = 0;
  }
  return true;
}

/*
 * Step 4 — central teardown.  The destructor expects an invariant
 * that no unbounded iteration is needed:
 *
 *   - m_gb_map is nullptr or empty — release_string_results' map walk
 *     iterates 0 times.
 *   - m_chunks is nullptr — freeAllChunks iterates 0 times.
 *
 * These hold when reaching the destructor via:
 *   (a) tearDownChunk → ... → true (CONTINUEB path).  Both m_gb_map
 *       and m_chunks are drained; m_string_results / m_xfrm_buf are
 *       already freed in tearDownChunk's tail; the destructor's
 *       remaining work is just m_buf_block.
 *   (b) the synchronous "empty map" fast path in
 *       init_release_scanrec.  By contract the periodic / end-of-scan
 *       drain has emptied the map; freeGroupData's per-group chunk
 *       release ensures m_chunks is nullptr in lock-step.  m_string_results
 *       and m_xfrm_buf may be present — both are O(1) frees here.
 *   (c) an interpreter that never processed rows (Init failure,
 *       version mismatch) — m_gb_map / m_chunks / m_string_results /
 *       m_xfrm_buf are all nullptr.
 *
 * Anything else means the caller bypassed the CONTINUEB protocol
 * while still holding live group state; the destructor would otherwise
 * walk thousands of groups synchronously and blow the 10 µs
 * block-thread budget (true even during graceful shutdown — other
 * blocks coordinate via signals and watchdogs).  Fail fast with
 * ndbrequire rather than silently freeze.
 */
AggInterpreterBase::~AggInterpreterBase() {
  ndbrequire(m_gb_map == nullptr || m_gb_map->empty());
  ndbrequire(m_chunks == nullptr);
  /* m_string_results / m_xfrm_buf may be present; both are O(1).
   * release_string_results' scalar slot walk is bounded by
   * m_n_agg_results ≤ MAX_AGG_N_RESULTS = 256, also O(1). */
  release_string_results();
  if (m_xfrm_buf != nullptr) {
    lc_ndbd_pool_free(m_xfrm_buf);
    m_xfrm_buf = nullptr;
  }
  if (m_load_column_meta != nullptr) {
    lc_ndbd_pool_free(m_load_column_meta);
    m_load_column_meta = nullptr;
  }
  if (m_column_meta != nullptr) {
    lc_ndbd_pool_free(m_column_meta);
    m_column_meta = nullptr;
  }
  if (m_column_meta_hash != nullptr) {
    lc_ndbd_pool_free(m_column_meta_hash);
    m_column_meta_hash = nullptr;
  }
  if (m_buf_block != nullptr) {
    lc_ndbd_pool_free(m_buf_block);
    m_buf_block = nullptr;
  }
}

/*
 * Step 3a-B — single-allocation buffer-block carve.
 *
 * Allocates one block from RG_QUERY_MEMORY sized for the caller's
 * actual needs and slices it into the six per-interpreter buffers.
 * Layout matches JoinAggInterpreter's pre-3a Init carve so the call
 * sites converge on a single helper.
 *
 * Right-sizing: callers pass right-sized counts, except where setter
 * APIs (e.g. JoinAgg's setTotalAggResults) require MAX-sized
 * over-allocation.  Pass n_gb_cols_alloc == 0 to skip the GB-col +
 * GB-types carve; pass alloc_gb_map == false to skip the
 * JoinGBHashTable carve (e.g. scalar aggregation, n_gb_cols == 0).
 *
 * Returns the start of the extra-tail region (or one-past-end when
 * extra_tail_bytes == 0).  nullptr on allocation failure.
 */
char* AggInterpreterBase::initBufBlock(Uint32 prog_words,
                                       Uint32 n_gb_cols_alloc,
                                       Uint32 n_agg_results_alloc,
                                       bool alloc_gb_map,
                                       Uint32 extra_tail_bytes) {
  require(m_buf_block == nullptr);

  /* Step 3 Cand-C: m_attr_read_buf is no longer carved here — it lives
   * on the Dbtup block instance (per LDM thread) and is set on each
   * ProcessRec entry from block_tup->getAggAttrReadBuf().  The
   * m_buf_block carve is correspondingly smaller. */
  const Uint32 prog_bytes = prog_words * sizeof(Uint32);
  const Uint32 gb_cols_bytes = n_gb_cols_alloc * sizeof(Uint32);
  const Uint32 agg_results_bytes = n_agg_results_alloc * sizeof(AggResItem);
  const Uint32 gb_map_bytes = alloc_gb_map ? sizeof(JoinGBHashTable) : 0;
  const Uint32 gb_types_bytes = n_gb_cols_alloc * sizeof(GBColTypeInfo);

  const Uint32 total = prog_bytes + gb_cols_bytes +
                       agg_results_bytes + gb_map_bytes + gb_types_bytes +
                       extra_tail_bytes;

  m_buf_block = lc_ndbd_pool_malloc(total, RG_QUERY_MEMORY,
                                    m_thread_id, false);
  if (m_buf_block == nullptr) {
    return nullptr;
  }

  char* p = static_cast<char*>(m_buf_block);
  m_prog_buf = reinterpret_cast<Uint32*>(p);
  p += prog_bytes;
  if (gb_cols_bytes > 0) {
    m_gb_cols_buf = reinterpret_cast<Uint32*>(p);
    p += gb_cols_bytes;
  } else {
    m_gb_cols_buf = nullptr;
  }
  m_agg_results_buf = reinterpret_cast<AggResItem*>(p);
  p += agg_results_bytes;
  if (alloc_gb_map) {
    m_gb_map_buf = new (p) JoinGBHashTable();
    p += gb_map_bytes;
  } else {
    m_gb_map_buf = nullptr;
  }
  if (gb_types_bytes > 0) {
    m_gb_types = reinterpret_cast<GBColTypeInfo*>(p);
    memset(m_gb_types, 0, gb_types_bytes);
    p += gb_types_bytes;
  } else {
    m_gb_types = nullptr;
  }
  return p;  /* start of extra-tail region, or one-past-end */
}

/*
 * Step 3a-A — release per-(group, slot) string MIN/MAX winner buffers
 * plus the slot-level metadata array.  Walks m_agg_results (the
 * no-GROUP-BY scalar slot array) and every live group in m_gb_map.
 * Now that both subclasses use the same JoinGBHashTable container,
 * the bodies are byte-identical except for JoinAgg's defensive
 * `m_agg_results != nullptr` check; adopted here for both.
 *
 * Called by each subclass' destructor before the group container
 * itself is torn down.
 */
void AggInterpreterBase::release_string_results() {
  if (m_string_results == nullptr) {
    return;
  }
  if (m_agg_results != nullptr) {
    freeGroupStringSlots(m_agg_results);
  }
  if (m_gb_map != nullptr) {
    for (auto iter = m_gb_map->begin(); iter.valid(); m_gb_map->next(iter)) {
      Uint32 key_len = iter.keyLen();
      AggResItem* slots =
          reinterpret_cast<AggResItem*>(iter.data() + key_len);
      freeGroupStringSlots(slots);
    }
  }
  lc_ndbd_pool_free(m_string_results);
}
