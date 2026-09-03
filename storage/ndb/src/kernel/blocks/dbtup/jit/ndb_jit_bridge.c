/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
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

/*
 * RONDB-1056 Phase 4 — NDB aggregation bytecode bridge.
 *
 * NDB wire format (per AggInterpreter.cpp's decode loop):
 *
 *   - Each instruction starts with a Uint32 word.
 *   - Top 6 bits (bits 31-26) = opcode (kOp* value).
 *   - Lower 26 bits = operand fields, layout per opcode.
 *   - Some opcodes (notably kOpLoadConst BIGINT) have additional
 *     trailing words carrying the immediate value.
 *
 * Per-opcode operand layouts we care about:
 *
 *   kOpLoadConst (3 words: 1 instr + 2 value):
 *     bits 25-21 (5 bits): type   (NDB_TYPE_*)
 *     bits 19-16 (4 bits): reg_index
 *     next 2 words: 64-bit little-endian value
 *
 *   kOpLoadCol (1 word for non-DECIMAL types):
 *     bits 25-21: type
 *     bits 19-16: reg_index
 *     bits 15-0:  col_index (we use this directly)
 *
 *   kOpMov (1 word):
 *     bits 15-12: dst reg_index
 *     bits 11-8:  src reg_index
 *
 *   kOpPlusBigint / kOpMinusBigint / kOpMulBigint (1 word, 2-operand):
 *     bits 15-12: lhs/dst reg_index
 *     bits 11-8:  rhs reg_index
 *     Semantics: regs[lhs] = regs[lhs] op regs[rhs].
 *     We translate to our 3-operand form: a=lhs, b=lhs, c=rhs.
 *
 *   kOpSumBigint (1 word):
 *     bits 19-16: src reg_index
 *     bits 15-0:  agg_index (16 bits — but we only have BC_MAX_ACCS
 *                 slots, currently 32)
 *
 * Program-end: implicit when the word stream is exhausted. The
 * bridge appends an OP_EXIT instruction.
 *
 * The bridge does NOT call jit1_compile or touch any JIT engine
 * state. It produces a Program; the caller chains to jit1_compile.
 */

#define NDB_JIT_BRIDGE_TESTING 1
#include "ndb_jit_bridge.h"

#include <stdio.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* NDB opcode + type constants (mirrored locally — we don't include  */
/* the NDB headers here, the bridge stays pure C).                   */
/* ------------------------------------------------------------------ */

/* kOp* values from include/ndbapi/NdbAggregationCommon.hpp.
 * Keep this in sync — the audit (Phase 5+) will assert it. */
#define BR_kOpUnknown        0
#define BR_kOpPlus           1
#define BR_kOpMinus          2
#define BR_kOpMul            3
#define BR_kOpDiv            4
#define BR_kOpDivInt         5
#define BR_kOpMod            6
#define BR_kOpLoadCol        7
#define BR_kOpLoadConst      8
#define BR_kOpMov            9
#define BR_kOpSum           10
#define BR_kOpMax           11
#define BR_kOpMin           12
#define BR_kOpCount         13
#define BR_kOpSumBigint     14
#define BR_kOpSumDouble     15
#define BR_kOpMaxBigint     16
#define BR_kOpMaxDouble     17
#define BR_kOpMinBigint     18
#define BR_kOpMinDouble     19
#define BR_kOpPlusBigint    20
#define BR_kOpPlusDouble    21
#define BR_kOpMinusBigint   22
#define BR_kOpMinusDouble   23
#define BR_kOpMulBigint     24
#define BR_kOpMulDouble     25
#define BR_kOpDivDouble     26
#define BR_kOpDivIntBigint  27
#define BR_kOpEmbeddedInterp 28
#define BR_kOpSkip          29
#define BR_kOpSetRegNull    30

/* From storage/ndb/include/ndb_constants.h. */
#define BR_NDB_TYPE_TINYINT          1
#define BR_NDB_TYPE_TINYUNSIGNED     2
#define BR_NDB_TYPE_SMALLINT         3
#define BR_NDB_TYPE_SMALLUNSIGNED    4
#define BR_NDB_TYPE_MEDIUMINT        5
#define BR_NDB_TYPE_MEDIUMUNSIGNED   6
#define BR_NDB_TYPE_INT              7
#define BR_NDB_TYPE_UNSIGNED         8
#define BR_NDB_TYPE_BIGINT      9
#define BR_NDB_TYPE_BIGUNSIGNED 10
#define BR_NDB_TYPE_DATE            19
#define BR_NDB_TYPE_YEAR            26
#define BR_NDB_TYPE_TIME2           31
#define BR_NDB_TYPE_DATETIME2       32
#define BR_NDB_TYPE_TIMESTAMP2      33
#define BR_NDB_TYPE_FLOAT       11
#define BR_NDB_TYPE_DOUBLE      12
#define BR_NDB_TYPE_DECIMAL         29
#define BR_NDB_TYPE_DECIMALUNSIGNED 30
#define BR_NDB_TYPE_CHAR        14
#define BR_NDB_TYPE_VARCHAR     15
#define BR_NDB_TYPE_LONGVARCHAR 23

/* ------------------------------------------------------------------ */
/* Embedded-interpreter opcodes (mirror of                            */
/* storage/ndb/include/kernel/Interpreter.hpp constants — Phase 5.0   */
/* admits only a small subset).                                       */
/* ------------------------------------------------------------------ */

/* These must equal the Interpreter:: enum values (see
 * include/kernel/Interpreter.hpp) — the bridge decodes the embedded
 * opcode via Interpreter::getOpCode and switches on it directly. */
#define BR_EMB_LOAD_CONST_NULL       3   /* reg := SQL NULL (rejects) */
/* ronsql_jit slice 2 item 3: the unconditional forward jump
 * (Interpreter.hpp BRANCH = 9 — the old define said 3, which is
 * LOAD_CONST_NULL; it was only ever used in the diagnostic name
 * table, so nothing mistranslated). RonSQL's DNF filter trellis and
 * NdbScanFilter's branch_label wiring are made of these. */
#define BR_EMB_BRANCH                9
#define BR_EMB_LOAD_CONST16          4
/* Phase 5A: the SQL planner's CASE-condition family (see
 * ha_ndbcluster_push_agg.cc emit_int_comparison_branch): column into
 * register, 64-bit constant into register, register-register compare. */
#define BR_EMB_READ_ATTR             1   /* READ_ATTR_INTO_REG */
#define BR_EMB_LOAD_CONST64          6   /* value in next 2 words */
#define BR_EMB_BRANCH_REG_EQ_NULL   10   /* null guard (5D-3 fusion / fold) */
#define BR_EMB_BRANCH_REG_NE_NULL   11   /* always-taken fold (slice 2 item 2) */
/* ronsql_jit slice 2 item 2: the GREATEST/LEAST pair-op import —
 * copies an OUTER aggregation-interpreter register into an embedded
 * register (the kernel's handleReadAggRegToReg). In the JIT both
 * files live in regs_i64 (outer 0-7, embedded 8-15), so this lowers
 * to OP_MOV_INT_INT — admitted only when the outer register's
 * tracked type makes the embedded SIGNED compares exact (I64/NNC, or
 * U64 with the u63-safe bound). */
#define BR_EMB_READ_AGG_REG_TO_REG  43
/* ronsql_jit slice 2 item 4: type-aware linked-attr-buffer load into
 * an embedded register (the kernel's handleReadLinkedColumnToReg).
 * Wire: bits 6-8 dst reg, 16-23 buffer position, 24-31 NDB type.
 * Lowers to OP_LOAD_LINKED_COL (cold call; NULL → per-row fallback).
 * Same signed-i64 admission as the op-43 import: signed widths and
 * narrow unsigned are exact; BIGUNSIGNED / FLOAT / DOUBLE reject. */
#define BR_EMB_READ_LINKED_COL_TO_REG 44
#define BR_EMB_BRANCH_EQ_REG_REG    12
#define BR_EMB_BRANCH_NE_REG_REG    13
#define BR_EMB_BRANCH_LT_REG_REG    14
#define BR_EMB_BRANCH_LE_REG_REG    15
#define BR_EMB_BRANCH_GT_REG_REG    16
#define BR_EMB_BRANCH_GE_REG_REG    17
#define BR_EMB_EXIT_OK              18
#define BR_EMB_EXIT_REFUSE          19
#define BR_EMB_EXIT_OK_LAST         22
/* BRANCH_ATTR_OP_ARG (Interpreter.hpp = 23). cond≤GE keeps bit 15 clear,
 * so getOpCode decodes it as 23 (Phase 7 comparison predicates). */
#define BR_EMB_BRANCH_ATTR_OP_ARG   23
/* BRANCH_ATTR_OP_PARAM (Interpreter.hpp = 26) — WHERE col <op> ?param. Same
 * word0/word1 layout as OP_ARG but no inline literal (2-word instruction);
 * the value lives in the subroutine/param region, resolved at runtime by
 * the helper via lookupInterpreterParameter. */
#define BR_EMB_BRANCH_ATTR_OP_PARAM 26
/* BRANCH_ATTR_OP_ATTR (Interpreter.hpp = 27) — WHERE col <op> col2 (two
 * columns of the same row). Same word0/word1 layout as OP_ARG but word1's
 * low 16 bits are the 2nd column's attrId and there is no inline literal
 * (2-word instruction); the helper reads the 2nd column from the row. */
#define BR_EMB_BRANCH_ATTR_OP_ATTR  27
/* ronsql_jit slice 2 item 5: the CTE-filter compares of a
 * cheapMemory[0] value (pre-loaded by READ_LINKED_TO_MEM) against an
 * inline literal. 38 resolves type/charset via tablerec[tableId] +
 * schemaVersion (2 extra header words); 40 carries them inline
 * (1 extra header word). Both lower to OP_BRANCH_MEM_OP_ARG. */
#define BR_EMB_BRANCH_MEM_OP_ARG        38
#define BR_EMB_BRANCH_MEM_OP_ARG_INLINE 40
/* ronsql_jit slice 2 item 6: F64 literal + heap-memory reads. */
#define BR_EMB_LOAD_DOUBLE_CONST        45   /* IEEE double in next 2 words */
#define BR_EMB_READ_UINT8_MEM_TO_REG    49
#define BR_EMB_READ_UINT16_MEM_TO_REG   50
#define BR_EMB_READ_UINT32_MEM_TO_REG   51
#define BR_EMB_READ_INT64_MEM_TO_REG    52
/* ronsql_jit slice 2 item 7: embedded WHERE arithmetic. */
#define BR_EMB_ADD_REG_REG               7
#define BR_EMB_SUB_REG_REG               8
#define BR_EMB_MUL_REG_REG              30
/* Highest BinaryCondition the MEM_OP_ARG family lowers (EQ..GE):
 * evalBranchMemForJit has no LIKE arm; LIKE / mask reject there. */
#define BR_EMB_MAX_BINARY_COND       5
/* ronsql_jit item 12: the ATTR_OP_ARG / _PARAM / _ATTR family also
 * lowers LIKE (6) and NOT_LIKE (7) — evalBranchColForJit carries the
 * interpreter's m_like arm. AND_*_MASK (8+) still reject. */
#define BR_EMB_MAX_ATTR_COND         7
/* WRITE_INTERPRETER_OUTPUT = LOAD_CONST_MEM(59) + OVERFLOW_OPCODE(64). */
#define BR_EMB_WRITE_INTERP_OUTPUT 123
#define BR_EMB_BRANCH_ATTR_EQ_NULL  24
#define BR_EMB_BRANCH_ATTR_NE_NULL  25
/* Phase 5.1a: READ_LINKED_TO_MEM populates cheapMemory[0] from the
 * preceding-table-row's linked-attr buffer. Subsequent
 * BRANCH_LINKED_*_NULL instructions inspect that. */
#define BR_EMB_READ_LINKED_TO_MEM   39
#define BR_EMB_BRANCH_LINKED_EQ_NULL 41
#define BR_EMB_BRANCH_LINKED_NE_NULL 42

/* Cap on embedded-block length. Larger blocks reject and the program
 * falls back to the interpreter. The cap bounds two stack arrays:
 * `emb_pc_to_op_idx[BR_EMB_MAX_LEN]` (1 byte/entry — output Op index
 * for each embedded pc) and the per-branch fixup machinery, both
 * sized at compile time. */
#define BR_EMB_MAX_LEN              1024
#define BR_MAX_LOCAL_ATTR_ID        4095

/* ------------------------------------------------------------------ */
/* Helpers.                                                           */
/* ------------------------------------------------------------------ */

static inline void set_err(JitBridgeError *err,
                           JitBridgeReason r,
                           uint32_t off, uint32_t op) {
  if (err == NULL) return;
  err->reason         = r;
  err->offending_word = off;
  err->offending_op   = op;
}

const char *ndb_jit_bridge_reason_name(JitBridgeReason reason) {
  switch (reason) {
    case JIT_BRIDGE_OK:
      return "ok";
    case JIT_BRIDGE_UNSUPPORTED_OP:
      return "unsupported_op";
    case JIT_BRIDGE_NON_BIGINT:
      return "non_bigint";
    case JIT_BRIDGE_PROG_TOO_LARGE:
      return "prog_too_large";
    case JIT_BRIDGE_MALFORMED:
      return "malformed";
    case JIT_BRIDGE_REG_OUT_OF_RANGE:
      return "reg_out_of_range";
    case JIT_BRIDGE_EMBEDDED_TOO_LARGE:
      return "embedded_too_large";
    case JIT_BRIDGE_EMBEDDED_BACKWARD:
      return "embedded_backward";
    case JIT_BRIDGE_TYPE_MISMATCH:
      return "type_mismatch";
    default:
      return "unknown";
  }
}

const char *ndb_jit_bridge_agg_op_name(uint32_t op) {
  switch (op) {
    case BR_kOpUnknown:        return "kOpUnknown";
    case BR_kOpPlus:           return "kOpPlus";
    case BR_kOpMinus:          return "kOpMinus";
    case BR_kOpMul:            return "kOpMul";
    case BR_kOpDiv:            return "kOpDiv";
    case BR_kOpDivInt:         return "kOpDivInt";
    case BR_kOpMod:            return "kOpMod";
    case BR_kOpLoadCol:        return "kOpLoadCol";
    case BR_kOpLoadConst:      return "kOpLoadConst";
    case BR_kOpMov:            return "kOpMov";
    case BR_kOpSum:            return "kOpSum";
    case BR_kOpMax:            return "kOpMax";
    case BR_kOpMin:            return "kOpMin";
    case BR_kOpCount:          return "kOpCount";
    case BR_kOpSumBigint:      return "kOpSumBigint";
    case BR_kOpSumDouble:      return "kOpSumDouble";
    case BR_kOpMaxBigint:      return "kOpMaxBigint";
    case BR_kOpMaxDouble:      return "kOpMaxDouble";
    case BR_kOpMinBigint:      return "kOpMinBigint";
    case BR_kOpMinDouble:      return "kOpMinDouble";
    case BR_kOpPlusBigint:     return "kOpPlusBigint";
    case BR_kOpPlusDouble:     return "kOpPlusDouble";
    case BR_kOpMinusBigint:    return "kOpMinusBigint";
    case BR_kOpMinusDouble:    return "kOpMinusDouble";
    case BR_kOpMulBigint:      return "kOpMulBigint";
    case BR_kOpMulDouble:      return "kOpMulDouble";
    case BR_kOpDivDouble:      return "kOpDivDouble";
    case BR_kOpDivIntBigint:   return "kOpDivIntBigint";
    case BR_kOpEmbeddedInterp: return "kOpEmbeddedInterp";
    case BR_kOpSkip:           return "kOpSkip";
    case BR_kOpSetRegNull:     return "kOpSetRegNull";
    default:                   return "kOp?";
  }
}

const char *ndb_jit_bridge_emb_op_name(uint32_t op) {
  switch (op) {
    case BR_EMB_LOAD_CONST_NULL:       return "LOAD_CONST_NULL";
    case BR_EMB_BRANCH:                return "BRANCH";
    case BR_EMB_READ_ATTR:             return "READ_ATTR_INTO_REG";
    case BR_EMB_LOAD_CONST16:          return "LOAD_CONST16";
    case BR_EMB_LOAD_CONST64:          return "LOAD_CONST64";
    case BR_EMB_BRANCH_REG_EQ_NULL:    return "BRANCH_REG_EQ_NULL";
    case BR_EMB_BRANCH_REG_NE_NULL:    return "BRANCH_REG_NE_NULL";
    case BR_EMB_BRANCH_EQ_REG_REG:     return "BRANCH_EQ_REG_REG";
    case BR_EMB_BRANCH_NE_REG_REG:     return "BRANCH_NE_REG_REG";
    case BR_EMB_BRANCH_LT_REG_REG:     return "BRANCH_LT_REG_REG";
    case BR_EMB_BRANCH_LE_REG_REG:     return "BRANCH_LE_REG_REG";
    case BR_EMB_BRANCH_GT_REG_REG:     return "BRANCH_GT_REG_REG";
    case BR_EMB_BRANCH_GE_REG_REG:     return "BRANCH_GE_REG_REG";
    case BR_EMB_EXIT_OK:               return "EXIT_OK";
    case BR_EMB_EXIT_REFUSE:           return "EXIT_REFUSE";
    case BR_EMB_EXIT_OK_LAST:          return "EXIT_OK_LAST";
    case BR_EMB_WRITE_INTERP_OUTPUT:   return "WRITE_INTERPRETER_OUTPUT";
    case BR_EMB_BRANCH_ATTR_EQ_NULL:   return "BRANCH_ATTR_EQ_NULL";
    case BR_EMB_BRANCH_ATTR_NE_NULL:   return "BRANCH_ATTR_NE_NULL";
    case BR_EMB_BRANCH_ATTR_OP_ARG:    return "BRANCH_ATTR_OP_ARG";
    case BR_EMB_BRANCH_ATTR_OP_PARAM:  return "BRANCH_ATTR_OP_PARAM";
    case BR_EMB_BRANCH_ATTR_OP_ATTR:   return "BRANCH_ATTR_OP_ATTR";
    case BR_EMB_READ_LINKED_TO_MEM:    return "READ_LINKED_TO_MEM";
    case BR_EMB_BRANCH_LINKED_EQ_NULL: return "BRANCH_LINKED_EQ_NULL";
    case BR_EMB_BRANCH_LINKED_NE_NULL: return "BRANCH_LINKED_NE_NULL";
    case BR_EMB_READ_LINKED_COL_TO_REG:return "READ_LINKED_COLUMN_TO_REG";
    case BR_EMB_ADD_REG_REG:           return "ADD_REG_REG";
    case BR_EMB_SUB_REG_REG:           return "SUB_REG_REG";
    case BR_EMB_MUL_REG_REG:           return "MUL_REG_REG";
    case BR_EMB_LOAD_DOUBLE_CONST:     return "LOAD_DOUBLE_CONST";
    case BR_EMB_READ_UINT8_MEM_TO_REG: return "READ_UINT8_MEM_TO_REG";
    case BR_EMB_READ_UINT16_MEM_TO_REG:return "READ_UINT16_MEM_TO_REG";
    case BR_EMB_READ_UINT32_MEM_TO_REG:return "READ_UINT32_MEM_TO_REG";
    case BR_EMB_READ_INT64_MEM_TO_REG: return "READ_INT64_MEM_TO_REG";
    case BR_EMB_BRANCH_MEM_OP_ARG:     return "BRANCH_MEM_OP_ARG";
    case BR_EMB_BRANCH_MEM_OP_ARG_INLINE:
      return "BRANCH_MEM_OP_ARG_INLINE_TYPE";
    default:                           return "EMB_OP?";
  }
}

const char *ndb_jit_bridge_jit_op_name(uint8_t kind) {
  switch (kind) {
    case OP_LOAD_CONST_INT:       return "load_const_int";
    case OP_LOAD_COL_INT:         return "load_col_int";
    case OP_MOV_INT_INT:          return "mov_int_int";
    case OP_ADD_INT_INT:          return "add_int_int";
    case OP_SUM_BIGINT:           return "sum_bigint";
    case OP_BRANCH_LT_INT_INT:    return "branch_lt_int_int";
    case OP_BRANCH_LE_INT_INT:    return "branch_le_int_int";
    case OP_BRANCH_EQ_INT_INT:    return "branch_eq_int_int";
    case OP_BRANCH_GT_INT_INT:    return "branch_gt_int_int";
    case OP_BRANCH_GE_INT_INT:    return "branch_ge_int_int";
    case OP_BRANCH_NE_INT_INT:    return "branch_ne_int_int";
    case OP_SKIP:                 return "skip";
    case OP_EXIT:                 return "exit";
    case OP_MINUS_INT_INT:        return "minus_int_int";
    case OP_MUL_INT_INT:          return "mul_int_int";
    case OP_LOAD_COL_NDB:         return "load_col_ndb";
    case OP_LOAD_CONST_UINT16:    return "load_const_uint16";
    case OP_LOAD_CONST_INT16:     return "load_const_int16";
    case OP_LOAD_CONST_UINT32:    return "load_const_uint32";
    case OP_LOAD_CONST_INT32:     return "load_const_int32";
    case OP_BRANCH_ATTR_EQ_NULL:  return "branch_attr_eq_null";
    case OP_BRANCH_ATTR_NE_NULL:  return "branch_attr_ne_null";
    case OP_BRANCH_ATTR_OP_ARG:   return "branch_attr_op_arg";
    case OP_LOAD_LINKED_TO_MEM:   return "load_linked_to_mem";
    case OP_LOAD_LINKED_COL:      return "load_linked_col";
    case OP_BRANCH_MEM_OP_ARG:    return "branch_mem_op_arg";
    case OP_BRANCH_F64:           return "branch_f64";
    case OP_READ_MEM_TO_REG:      return "read_mem_to_reg";
    case OP_ARITH_FB:             return "arith_fb";
    case OP_BRANCH_LINKED_EQ_NULL:return "branch_linked_eq_null";
    case OP_BRANCH_LINKED_NE_NULL:return "branch_linked_ne_null";
    case OP_ADD_INT_INT_CHECKED:  return "add_int_int_checked";
    case OP_MINUS_INT_INT_CHECKED:return "minus_int_int_checked";
    case OP_MUL_INT_INT_CHECKED:  return "mul_int_int_checked";
    case OP_SUM_BIGINT_CHECKED:   return "sum_bigint_checked";
    case OP_OVERFLOW_EXIT:        return "overflow_exit";
    case OP_JUMP:                 return "jump";
    case OP_FILTER_REJECT_EXIT:   return "filter_reject_exit";
    case OP_COUNT_BIGINT:         return "count_bigint";
    case OP_MIN_BIGINT:           return "min_bigint";
    case OP_MAX_BIGINT:           return "max_bigint";
    case OP_LOAD_COL_NDB_F64:     return "load_col_ndb_f64";
    case OP_ADD_F64:              return "add_f64";
    case OP_MINUS_F64:            return "minus_f64";
    case OP_MUL_F64:              return "mul_f64";
    case OP_DIV_F64:              return "div_f64";
    case OP_SUM_F64:              return "sum_f64";
    case OP_MIN_F64:              return "min_f64";
    case OP_MAX_F64:              return "max_f64";
    case OP_LOAD_COL_NDB_U64:     return "load_col_ndb_u64";
    case OP_SUM_U64_CHECKED:      return "sum_u64_checked";
    case OP_MIN_U64:              return "min_u64";
    case OP_MAX_U64:              return "max_u64";
    case OP_LOAD_COL_NDB_NB:      return "load_col_ndb_nb";
    case OP_LOAD_COL_NDB_F64_NB:  return "load_col_ndb_f64_nb";
    case OP_LOAD_COL_NDB_U64_NB:  return "load_col_ndb_u64_nb";
    case OP_ADD_U64_CHECKED:      return "add_u64_checked";
    case OP_MINUS_U64_CHECKED:    return "minus_u64_checked";
    case OP_MUL_U64_CHECKED:      return "mul_u64_checked";
    case OP_LOAD_COL_NDB_DEC:     return "load_col_ndb_dec";
    case OP_MINMAX_STR_NDB:       return "minmax_str_ndb";
    case OP_DIV_INT_CHECKED:      return "div_int_checked";
    case OP_MOD_INT:              return "mod_int";
    case OP_DIV_U64:              return "div_u64";
    case OP_MOD_U64:              return "mod_u64";
    case OP_DIV_CONV_F64:         return "div_conv_f64";
    case OP_ARITH_CONV_F64:       return "arith_conv_f64";
    case OP_DIVMOD_CONV:          return "divmod_conv";
    case OP_SET_REG_NULL_FB:      return "set_reg_null_fb";
    default:                      return "jit_op?";
  }
}

static void dump_line(NdbJitBridgeDumpFn dump, void *ctx,
                      const char *line) {
  if (dump != NULL) {
    dump(ctx, line);
  }
}

void ndb_jit_bridge_dump_input(const uint32_t *header,
                               uint32_t       header_words,
                               const uint32_t *body,
                               uint32_t       body_words,
                               NdbJitBridgeDumpFn dump,
                               void          *ctx) {
  char line[192];
  snprintf(line, sizeof(line),
           "[RONDB-1056] agg program: header=%u body=%u total=%u words",
           (unsigned)header_words, (unsigned)body_words,
           (unsigned)(header_words + body_words));
  dump_line(dump, ctx, line);

  for (uint32_t i = 0; i < header_words; i++) {
    uint32_t w = header != NULL ? header[i] : 0;
    snprintf(line, sizeof(line),
             "[RONDB-1056]   hdr[%u] = 0x%08x",
             (unsigned)i, (unsigned)w);
    dump_line(dump, ctx, line);
  }

  for (uint32_t i = 0; i < body_words; i++) {
    uint32_t w = body != NULL ? body[i] : 0;
    uint32_t op = (w & 0xFC000000u) >> 26;
    snprintf(line, sizeof(line),
             "[RONDB-1056]   bc[%u] = 0x%08x  top6=%u  %s",
             (unsigned)i, (unsigned)w, (unsigned)op,
             ndb_jit_bridge_agg_op_name(op));
    dump_line(dump, ctx, line);
  }
}

void ndb_jit_bridge_dump_program(const Program *prog,
                                 NdbJitBridgeDumpFn dump,
                                 void *ctx) {
  char line[192];
  if (prog == NULL) {
    dump_line(dump, ctx, "[RONDB-1056] jit program: <null>");
    return;
  }

  snprintf(line, sizeof(line),
           "[RONDB-1056] jit program: %u ops", (unsigned)prog->n_ops);
  dump_line(dump, ctx, line);

  for (uint16_t pc = 0; pc < prog->n_ops; pc++) {
    const Op *op = &prog->ops[pc];
    snprintf(line, sizeof(line),
             "[RONDB-1056]   op[%u] %-24s kind=%u a=%u b=%u c=%u d=%u imm=%lld",
             (unsigned)pc, ndb_jit_bridge_jit_op_name(op->kind),
             (unsigned)op->kind, (unsigned)op->a, (unsigned)op->b,
             (unsigned)op->c, (unsigned)op->d, (long long)op->imm);
    dump_line(dump, ctx, line);
  }
}

/* sint8korr equivalent: read 8 little-endian bytes as int64.
 * NDB's bytecode stores the 64-bit immediate as two consecutive
 * Uint32 words in little-endian order (low word first). */
static inline int64_t read_int64_le(const uint32_t *prog, uint32_t pos) {
  uint64_t lo = (uint64_t)prog[pos];
  uint64_t hi = (uint64_t)prog[pos + 1];
  return (int64_t)((hi << 32) | lo);
}

/* Append an Op to out_prog, returning 1 on success, 0 if the
 * program is full. */
static inline int emit_op_d(Program *out, uint8_t kind, uint8_t a,
                            uint16_t b, uint16_t c, uint16_t d, int64_t imm) {
  if (out->n_ops >= BC_MAX_OPS) return 0;
  Op *op = &out->ops[out->n_ops++];
  op->kind = kind;
  op->a    = a;
  op->b    = b;
  op->c    = c;
  op->d    = d;
  op->imm  = imm;
  return 1;
}

static inline int emit_op(Program *out, uint8_t kind,
                          uint8_t a, uint16_t b, uint16_t c, int64_t imm) {
  return emit_op_d(out, kind, a, b, c, 0, imm);
}

static inline int embedded_filters_enabled(void) {
  /* Aggregation embedded filters lower EXIT_REFUSE to OP_EXIT, which
   * returns before accumulator opcodes can set JitState::value_updated[].
   * Standalone scan-filter translation uses OP_FILTER_REJECT_EXIT instead. */
  return 1;
}

/* ------------------------------------------------------------------ */
/* Embedded normal-interpreter block translation (Phase 5.0+).        */
/*                                                                    */
/* Walks the inner NDB-bytecode words emitted after kOpEmbeddedInterp's*/
/* header word. The JIT admits a deliberately narrow opcode set:       */
/*                                                                    */
/*   BRANCH_ATTR_EQ_NULL  → OP_BRANCH_ATTR_EQ_NULL                    */
/*   BRANCH_ATTR_NE_NULL  → OP_BRANCH_ATTR_NE_NULL                    */
/*   READ_LINKED_TO_MEM   → OP_LOAD_LINKED_TO_MEM (reject if          */
/*                          !allow_linked_ops — scan-filter path)     */
/*   BRANCH_LINKED_*_NULL → OP_BRANCH_LINKED_*_NULL (likewise)        */
/*   LOAD_CONST16         → stage row-disposition skip_offset          */
/*   WRITE_INTERP_OUTPUT  → no Op for skip_offset 0, OP_JUMP otherwise*/
/*   EXIT_OK              → no Op (fall through to outer program)     */
/*   EXIT_OK_LAST         → reject (JIT can't set req_struct last_row)*/
/*   EXIT_REFUSE          → caller-selected reject terminator          */
/*                                                                    */
/* Every other embedded opcode rejects the WHOLE program. The         */
/* admission contract: if Phase 5.0 can't handle every opcode in the  */
/* embedded block, it doesn't try to handle any — partial coverage    */
/* would be unsafe (we'd silently miss filter conditions).            */
/*                                                                    */
/* Branch target translation: the embedded block has its own pc-      */
/* space (0..emb_len-1). We track each instruction's output Op index  */
/* in emb_pc_to_op_idx[]. After the linear walk, a fixup pass walks   */
/* the emitted Ops and replaces their temporary `c` (= target embedded*/
/* pc) with the corresponding output Op index. CASE OP_JUMP targets   */
/* are resolved later by the outer translator because their skip       */
/* offsets name positions in the outer aggregation word stream.        */
/* ------------------------------------------------------------------ */

/* Sentinel for `pending_target_emb_pc[]` slots that don't need
 * fixup (i.e., the emitted Op is not a branch from this embedded
 * block). 0xFFFF can never be a real emb_pc because BR_EMB_MAX_LEN
 * is 1024. */
#define BR_EMB_NO_PENDING_FIXUP     0xFFFFu

typedef struct {
  uint16_t op_idx;
  uint32_t target_word_pos;
  /* GL Part A (2026-09-01) merge-point guard: the OUTER register
   * tracker (regs 0..BC_EMB_REG_BASE-1) as it stood at the jump's
   * SOURCE. An embedded output-block jump's source state is the
   * state at block entry (embedded code cannot write outer
   * registers); an outer kOpSkip's is the state at the Skip.
   * has_snap == 0 where there is no outer register file (standalone
   * scan filters, the embedded-only test wrapper) and for edges that
   * never land on an outer word (the arith fallback edge to the tail
   * exit) — those entries take no part in the guard. */
  uint8_t  has_snap;
  uint8_t  reg_type_snap[BC_EMB_REG_BASE];
  uint8_t  reg_u63_snap[BC_EMB_REG_BASE];
  uint8_t  reg_pack_snap[BC_EMB_REG_BASE];   /* GL Part B */
} PendingCaseJump;

/* ronsql_jit slice 2 item 9: WRITE_INTERPRETER_OUTPUT's skip value
 * 0xFFFF is AGG_EMBEDDED_INTERP_STOP_PROGRAM ("skip the rest of this
 * ROW's outer program" — the interpreter sets exec_pos = prog_len).
 * The bridge marks such jumps with this sentinel and the resolver
 * points them at the tail OP_EXIT. Unambiguous: real skip offsets are
 * bounded by the ≤1024-word program. */
#define BR_CASE_JUMP_STOP 0xFFFFFFFFu

/* exit_ok_kind sentinel: "emit no Op for EXIT_OK; the accepted row falls
 * through to whatever the outer translation emits next." Used by the
 * aggregation-embedded path, where accept continues into accumulator ops.
 * The standalone scan-filter path passes OP_EXIT instead (see the EXIT_OK
 * case for why). 0xFF is not a valid JIT op kind. */
#define BR_EXIT_OK_FALLTHROUGH 0xFFu

/* out_exit_refuse_code sentinel: "no EXIT_REFUSE seen yet". A real refuse
 * code is `inst >> 16`, i.e. at most 16 bits, so a >16-bit sentinel can't
 * collide. Callers that capture the code init their local to this before
 * calling translate_embedded_block. */
#define BR_NO_REFUSE_CODE 0xFFFFFFFFu

/* Phase 5D-3: null-path safety scan for the READ_ATTR +
 * BRANCH_REG_EQ_NULL fusion.
 *
 * The fused OP_LOAD_COL_NDB_NB leaves the destination register with an
 * UNDEFINED value on its taken (null) edge, where the interpreter's
 * register would be tagged null (and raise ZREGISTER_INIT_ERROR if
 * compared). Fusion is therefore only sound if no path from the
 * guard's target can READ the register before overwriting it. Planner
 * programs satisfy this by construction (the target is either the next
 * condition — which starts by re-loading the same register — or an
 * ELSE/THEN output block that never touches it); this scan proves it
 * for the program at hand so a hand-built API program can't diverge.
 *
 * Walks every path from start_pc (forward-only branches, visited set →
 * terminates). A path is safe when it overwrites `reg`, reaches a
 * terminal EXIT, or leaves the embedded block (the outer translator
 * resets every register to UNKNOWN after an embedded block, so no
 * admitted outer op can read the stale register). Reading `reg`,
 * a backward branch, or an opcode this scan doesn't know → unsafe
 * (conservative: pass 1 rejects such programs anyway).
 * Returns 1 = safe to fuse, 0 = not. */
static int emb_null_path_reg_safe(const uint32_t *emb_prog, uint32_t emb_len,
                                  uint32_t start_pc, uint32_t reg) {
  uint8_t  visited[BR_EMB_MAX_LEN];
  uint32_t stack[BR_EMB_MAX_LEN];
  uint32_t sp = 0;
  memset(visited, 0, sizeof(visited));
  stack[sp++] = start_pc;
  while (sp > 0) {
    uint32_t pc = stack[--sp];
    /* Follow one path; pc = emb_len ends it (off-end / overwritten /
     * terminal all funnel here — see the header comment for why
     * leaving the block is safe). */
    while (pc < emb_len && !visited[pc]) {
      visited[pc] = 1;
      uint32_t inst = emb_prog[pc];
      uint8_t  scan_op = (uint8_t)((inst & 0x3Fu) |
                                   (((inst >> 15) & 0x1u) << 6));
      switch (scan_op) {
        case BR_EMB_READ_ATTR:
        case BR_EMB_LOAD_CONST16:
        /* ronsql_jit slice 2 item 2: the READ_AGG import writes its
         * embedded dst like a load — an overwrite ends the tracked
         * path safely; it reads only OUTER registers, never the
         * embedded reg under scan. Item 4: the linked-column load
         * writes its dst the same way and reads no registers. */
        case BR_EMB_READ_AGG_REG_TO_REG:
        case BR_EMB_READ_LINKED_COL_TO_REG:
          if (((inst >> 6) & 0x7u) == reg) { pc = emb_len; break; }
          pc += 1;
          break;
        case BR_EMB_LOAD_CONST64:
          if (((inst >> 6) & 0x7u) == reg) { pc = emb_len; break; }
          pc += 3;
          break;
        /* ronsql_jit slice 2 item 6: double const writes like
         * LOAD_CONST64; the heap-memory reads write their dst and
         * read no registers. */
        case BR_EMB_LOAD_DOUBLE_CONST:
          if (((inst >> 6) & 0x7u) == reg) { pc = emb_len; break; }
          pc += 3;
          break;
        case BR_EMB_READ_UINT8_MEM_TO_REG:
        case BR_EMB_READ_UINT16_MEM_TO_REG:
        case BR_EMB_READ_UINT32_MEM_TO_REG:
        case BR_EMB_READ_INT64_MEM_TO_REG:
          if (((inst >> 6) & 0x7u) == reg) { pc = emb_len; break; }
          pc += 1;
          break;
        /* ronsql_jit slice 2 item 7: arithmetic reads both sources
         * (a read of the scanned reg is unsafe); a dst overwrite
         * ends the path. */
        case BR_EMB_ADD_REG_REG:
        case BR_EMB_SUB_REG_REG:
        case BR_EMB_MUL_REG_REG:
          if (((inst >> 6) & 0x7u) == reg ||
              ((inst >> 9) & 0x7u) == reg) return 0;
          /* dst: Add/Sub bits 16-18, Mul bits 12-14 (see the
           * translate case). */
          if (((scan_op == BR_EMB_MUL_REG_REG) ? (inst >> 12) & 0x7u
                                               : (inst >> 16) & 0x7u) ==
              reg) { pc = emb_len; break; }
          pc += 1;
          break;
        /* ronsql_jit slice 2 item 3: the unconditional jump has ONE
         * successor — the target. (A zero offset stalls on visited[]
         * and ends the path; translate rejects it as MALFORMED before
         * any scan verdict matters.) */
        case BR_EMB_BRANCH:
          if ((inst >> 31) != 0) return 0;             /* backward */
          pc += (inst >> 16) & 0x7FFFu;
          break;
        case BR_EMB_BRANCH_REG_EQ_NULL:
        case BR_EMB_BRANCH_REG_NE_NULL:
          if (((inst >> 6) & 0x7u) == reg) return 0;   /* reads reg */
          if ((inst >> 31) != 0) return 0;             /* backward */
          if (sp >= BR_EMB_MAX_LEN) return 0;
          stack[sp++] = pc + ((inst >> 16) & 0x7FFFu);
          pc += 1;
          break;
        case BR_EMB_BRANCH_EQ_REG_REG:
        case BR_EMB_BRANCH_NE_REG_REG:
        case BR_EMB_BRANCH_LT_REG_REG:
        case BR_EMB_BRANCH_LE_REG_REG:
        case BR_EMB_BRANCH_GT_REG_REG:
        case BR_EMB_BRANCH_GE_REG_REG:
          if (((inst >> 6) & 0x7u) == reg ||
              ((inst >> 9) & 0x7u) == reg) return 0;   /* reads reg */
          if ((inst >> 31) != 0) return 0;
          if (sp >= BR_EMB_MAX_LEN) return 0;
          stack[sp++] = pc + ((inst >> 16) & 0x7FFFu);
          pc += 1;
          break;
        case BR_EMB_BRANCH_ATTR_EQ_NULL:
        case BR_EMB_BRANCH_ATTR_NE_NULL:
          if ((inst >> 31) != 0) return 0;
          if (sp >= BR_EMB_MAX_LEN) return 0;
          stack[sp++] = pc + ((inst >> 16) & 0x7FFFu);
          pc += 2;
          break;
        case BR_EMB_BRANCH_ATTR_OP_ARG:
        case BR_EMB_BRANCH_ATTR_OP_PARAM:
        case BR_EMB_BRANCH_ATTR_OP_ATTR: {
          if ((inst >> 31) != 0) return 0;
          if (pc + 2 > emb_len) return 0;
          uint32_t words = 2u;
          if (scan_op == BR_EMB_BRANCH_ATTR_OP_ARG) {
            words = 2u + (((emb_prog[pc + 1] & 0xFFFFu) + 3u) >> 2);
          }
          if (sp >= BR_EMB_MAX_LEN) return 0;
          stack[sp++] = pc + ((inst >> 16) & 0x7FFFu);
          pc += words;
          break;
        }
        /* ronsql_jit slice 2 item 5: the mem-compare branches fork
         * like ATTR_OP_ARG; 38 has 4 header words, 40 has 3. */
        case BR_EMB_BRANCH_MEM_OP_ARG:
        case BR_EMB_BRANCH_MEM_OP_ARG_INLINE: {
          if ((inst >> 31) != 0) return 0;
          if (pc + 2 > emb_len) return 0;
          uint32_t hdr = (scan_op == BR_EMB_BRANCH_MEM_OP_ARG) ? 4u : 3u;
          uint32_t words = hdr + (((emb_prog[pc + 1] & 0xFFFFu) + 3u) >> 2);
          if (sp >= BR_EMB_MAX_LEN) return 0;
          stack[sp++] = pc + ((inst >> 16) & 0x7FFFu);
          pc += words;
          break;
        }
        case BR_EMB_READ_LINKED_TO_MEM:
          pc += 1;
          break;
        case BR_EMB_BRANCH_LINKED_EQ_NULL:
        case BR_EMB_BRANCH_LINKED_NE_NULL:
          if ((inst >> 31) != 0) return 0;
          if (sp >= BR_EMB_MAX_LEN) return 0;
          stack[sp++] = pc + ((inst >> 16) & 0x7FFFu);
          pc += 1;
          break;
        case BR_EMB_WRITE_INTERP_OUTPUT:
          if (((inst >> 6) & 0x7u) == reg) return 0;   /* reads reg */
          pc += 1;
          break;
        case BR_EMB_EXIT_OK:
        case BR_EMB_EXIT_OK_LAST:
        case BR_EMB_EXIT_REFUSE:
          pc = emb_len;                                /* terminal */
          break;
        default:
          return 0;
      }
    }
  }
  return 1;
}

/* Register-type tracker values (Phase 5C, hoisted to file scope in
 * ronsql_jit slice 2 item 2 so translate_embedded_block's
 * READ_AGG_REG_TO_REG import can consult the outer walk's tracker).
 * Full per-value docs at the tracker declaration site in
 * ndb_jit_bridge_translate. */
enum { BR_REG_UNKNOWN = 0, BR_REG_I64 = 1, BR_REG_F64 = 2,
       BR_REG_U64 = 3, BR_REG_NNC = 4, BR_REG_STR = 5 };

/* GL Part A (2026-09-01): the bit-REINTERPRETATION class of a track —
 * the property two control-flow paths must agree on where they merge.
 * I64 / U64 / NNC are all i64 bit patterns (signedness stays the
 * linear tracker's call, exactly as before); F64 and STR registers are
 * reinterpreted by their consumers (SUM_F64, the F64 branches, the
 * string min/max), so a merge whose paths disagree on the class hands
 * a typed consumer the wrong bits on one of them. 0 = unknown. */
static uint8_t br_track_class(uint8_t track) {
  switch (track) {
    case BR_REG_UNKNOWN: return 0;
    case BR_REG_F64:     return 2;
    case BR_REG_STR:     return 3;
    default:             return 1;
  }
}

/* Records the outer tracker at a CASE-disposition jump's source into
 * its PendingCaseJump (see the struct). reg_type == NULL = no outer
 * register file on this path → the entry opts out of the guard. */
static void br_snapshot_outer_tracker(PendingCaseJump *j,
                                      const uint8_t *reg_type,
                                      const uint8_t *reg_u63,
                                      const uint8_t *reg_pack) {
  if (reg_type == NULL) {
    j->has_snap = 0;
    return;
  }
  j->has_snap = 1;
  memcpy(j->reg_type_snap, reg_type, BC_EMB_REG_BASE);
  if (reg_u63 != NULL) {
    memcpy(j->reg_u63_snap, reg_u63, BC_EMB_REG_BASE);
  } else {
    memset(j->reg_u63_snap, 0, BC_EMB_REG_BASE);
  }
  if (reg_pack != NULL) {
    memcpy(j->reg_pack_snap, reg_pack, BC_EMB_REG_BASE);
  } else {
    memset(j->reg_pack_snap, 0, BC_EMB_REG_BASE);
  }
}

static JitBridgeReason translate_embedded_block(
    const uint32_t *emb_prog, uint32_t emb_len,
    Program *out_prog, JitBridgeError *out_err,
    uint32_t outer_word_pos, /* for error reporting */
    uint32_t outer_after_emb_pos,
    uint8_t exit_refuse_kind,
    uint8_t exit_ok_kind,
    int allow_linked_ops,
    int allow_attr_op_arg,
    uint32_t attr_op_arg_base,
    int allow_reg_ops,
    PendingCaseJump *pending_case_jumps,
    uint16_t *n_pending_case_jumps,
    uint32_t *out_exit_refuse_code,
    /* ronsql_jit slice 2 item 2: the OUTER walk's register-type
     * tracker (+ the u63-safe flags), consulted by the
     * READ_AGG_REG_TO_REG import. NULL on paths with no outer
     * register file (scan filters, the embedded-only test wrapper) —
     * the import rejects there. */
    const uint8_t *outer_reg_type,
    const uint8_t *outer_reg_u63,
    /* GL Part B: per-outer-register "packed temporal" flag (DATETIME2 /
     * TIMESTAMP2 loads) — such U64 registers are never imported. NULL
     * wherever outer_reg_type is NULL. */
    const uint8_t *outer_reg_pack) {

  if (emb_len == 0) {
    /* Empty embedded block — equivalent to "always pass". */
    return JIT_BRIDGE_OK;
  }
  if (emb_len > BR_EMB_MAX_LEN) {
    if (out_err) {
      out_err->reason         = JIT_BRIDGE_EMBEDDED_TOO_LARGE;
      out_err->offending_word = outer_word_pos;
      out_err->offending_op   = BR_kOpEmbeddedInterp;
    }
    return JIT_BRIDGE_EMBEDDED_TOO_LARGE;
  }

  /* emb_pc → out_op_idx mapping. Initialised to 0xFF (= "no op
   * emitted at this pc" — happens for embedded-pcs strictly inside
   * a multi-word instruction's body, and for EXIT_OK which emits no
   * Op). The stored value is bounded by BC_MAX_OPS so 1 byte is
   * enough; the array is indexed by emb_pc so it scales with
   * BR_EMB_MAX_LEN. */
  uint8_t emb_pc_to_op_idx[BR_EMB_MAX_LEN];
  memset(emb_pc_to_op_idx, 0xFF, sizeof(emb_pc_to_op_idx));

  /* Side-array carrying the target embedded-pc for each Op this
   * block emits. Indexed by absolute output Op index (0..n_ops-1).
   * Slots set to BR_EMB_NO_PENDING_FIXUP for non-branch Ops or Ops
   * emitted before this block — pass 2 ignores those. We store the
   * full uint16_t emb_pc instead of stuffing it into op->c, so
   * target pcs ≥ 128 don't collide with any reserved bit (bug fixed
   * 2026-05: prior code OR'd 0x80 into op->c, losing bit 7 for any
   * target_emb_pc ≥ 128). */
  uint16_t pending_target_emb_pc[BC_MAX_OPS];
  for (uint16_t i = 0; i < BC_MAX_OPS; ++i) {
    pending_target_emb_pc[i] = BR_EMB_NO_PENDING_FIXUP;
  }
  uint16_t first_op_idx_at_entry = (uint16_t)out_prog->n_ops;
  uint16_t const16_by_reg[8];
  uint8_t const16_valid[8];
  memset(const16_by_reg, 0, sizeof(const16_by_reg));
  memset(const16_valid, 0, sizeof(const16_valid));

  /* Phase 5D-3: READ_ATTR + BRANCH_REG_EQ_NULL fusion state. Records
   * the immediately preceding instruction when it was a READ_ATTR so
   * the guard case can rewrite its emitted load into the
   * null-branching form. Any other instruction clears it (captured
   * into prev_* at the top of each iteration). */
  uint8_t  read_attr_pending = 0;
  uint8_t  read_attr_dst = 0;
  uint16_t read_attr_op_idx = 0;

  /* ronsql_jit slice 2 item 6: per-embedded-reg F64 tracking.
   * emb_reg_f64[r] — the reg holds double BITS (LOAD_DOUBLE_CONST,
   * or a READ_ATTR retroactively converted to the F64 load).
   * emb_reg_read_attr[r] — output-op index of the plain
   * OP_LOAD_COL_NDB that last defined r (-1 otherwise); lets a
   * later F64 compare convert that load in place. Every reg write
   * updates both. */
  uint8_t emb_reg_f64[8];
  int16_t emb_reg_read_attr[8];
  memset(emb_reg_f64, 0, sizeof(emb_reg_f64));
  /* GL Part B (2026-09-03): emb_reg_u64[r] — the reg holds an UNSIGNED
   * 64-bit value that is not provably < 2^63 (a BIGUNSIGNED import or
   * linked load). A compare with such a side takes the typed helper's
   * unsigned arms (per-side flags 0x40 / 0x80); arithmetic over it
   * rejects like F64. Every reg write updates it alongside
   * emb_reg_f64. */
  uint8_t emb_reg_u64[8];
  memset(emb_reg_u64, 0, sizeof(emb_reg_u64));
  for (int tr = 0; tr < 8; ++tr) emb_reg_read_attr[tr] = -1;

  /* Pass 1: linear walk, emit Ops with target_emb_pc in c. */
  uint32_t emb_pc = 0;
  while (emb_pc < emb_len) {
    uint8_t  prev_read_attr_pending = read_attr_pending;
    uint8_t  prev_read_attr_dst     = read_attr_dst;
    uint16_t prev_read_attr_op_idx  = read_attr_op_idx;
    read_attr_pending = 0;
    uint32_t inst = emb_prog[emb_pc];
    /* NDB normal-interpreter opcode encoding (different from the
     * aggregation interpreter's bits 31..26 layout): bits 5..0 +
     * (bit 15 << 6), per Interpreter::getOpCode. Branch direction
     * and length stay at bit 31 and bits 30..16. */
    uint8_t  emb_op = (uint8_t)((inst & 0x3Fu) |
                                (((inst >> 15) & 0x1u) << 6));
    uint8_t  out_op_idx = (uint8_t)out_prog->n_ops;

    emb_pc_to_op_idx[emb_pc] = out_op_idx;

    switch (emb_op) {
      case BR_EMB_READ_ATTR: {
        /* Phase 5A: READ_ATTR_INTO_REG — column value into an
         * interpreter register (the SQL planner's CASE condition starts
         * with this). Lowers to the OP_LOAD_COL_NDB cold-call: the
         * helper reads the column via readSingleAttributeForJit into
         * regs_i64[dst]. A NULL value sets JitState::row_fallback — the
         * row is re-run on the interpreter, which reproduces the exact
         * null-register semantics (SUM/COUNT null-skip,
         * ZREGISTER_INIT_ERROR on null comparisons).
         *
         * Phase 5D-3: when the NEXT instruction is the planner's
         * BRANCH_REG_EQ_NULL null guard for this register, the guard
         * case rewrites this load into OP_LOAD_COL_NDB_NB — NULL rows
         * then take the guard's edge on the JIT instead of the per-row
         * fallback. */
        if (!allow_reg_ops) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        uint32_t dst_reg = (inst >> 6) & 0x7u;
        uint32_t attr_id = inst >> 16;
        if (attr_id > BR_MAX_LOCAL_ATTR_ID) {
          /* Local table columns only — linked/pseudo out of scope. */
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_REG_OUT_OF_RANGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        if (!emit_op(out_prog, OP_LOAD_COL_NDB,
                     (uint8_t)(BC_EMB_REG_BASE + dst_reg), 0,
                     (uint16_t)attr_id, 0)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        /* The register no longer holds a LOAD_CONST16 value usable as
         * a WRITE_INTERPRETER_OUTPUT skip offset. */
        const16_valid[dst_reg] = 0;
        /* Arm the 5D-3 fusion for a following BRANCH_REG_EQ_NULL. */
        read_attr_pending = 1;
        read_attr_dst = (uint8_t)dst_reg;
        read_attr_op_idx = out_op_idx;
        emb_reg_f64[dst_reg] = 0;
        emb_reg_u64[dst_reg] = 0;
        emb_reg_read_attr[dst_reg] = (int16_t)out_op_idx;
        emb_pc += 1;
        break;
      }

      case BR_EMB_BRANCH: {
        /* ronsql_jit slice 2 item 3: the unconditional forward jump.
         * RonSQL's DNF filter trellis wires every condition block to
         * the shared ACCEPT/REJECT exits with it (NdbScanFilter's
         * branch_label), which is why it dominated the scan-filter
         * reject census. No registers involved, so it lowers on BOTH
         * the scan-filter and aggregation-embedded paths: OP_JUMP to
         * whatever op the target pc resolves to (the same pending
         * fixup as every embedded branch; forward-only like all of
         * them). A ZERO length is rejected as MALFORMED — the
         * interpreter's brancher would re-execute the same word
         * forever, so no emitter produces it; lowering it would hand
         * the JIT a guaranteed infinite self-jump. */
        if ((inst >> 31) != 0) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_EMBEDDED_BACKWARD;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_EMBEDDED_BACKWARD;
        }
        uint32_t branch_length = (inst >> 16) & 0x7FFFu;
        uint32_t target_emb_pc = emb_pc + branch_length;
        if (branch_length == 0 || target_emb_pc >= emb_len) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        pending_target_emb_pc[out_op_idx] = (uint16_t)target_emb_pc;
        if (!emit_op(out_prog, OP_JUMP, 0, 0, /*c=*/0, 0)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        emb_pc += 1;
        break;
      }

      case BR_EMB_BRANCH_REG_EQ_NULL: {
        /* Phase 5D-3: the planner's null guard for a nullable
         * CASE-condition column — always emitted directly after the
         * READ_ATTR it guards, targeting "this condition failed"
         * (the next WHEN's first word, or the ELSE output block for a
         * simple-CASE search register).
         *
         * Fuse the READ_ATTR + guard PAIR into one OP_LOAD_COL_NDB_NB:
         * the null-branching load's taken edge IS the guard's edge, so
         * NULL rows stay on the JIT (pre-5D-3 the plain load's helper
         * took the per-row interpreter fallback for every NULL). The
         * guard itself emits no Op; emb_pc_to_op_idx for its pc was
         * recorded at the loop top and resolves to the next emitted Op
         * (planner programs never branch to a guard).
         *
         * A guard NOT immediately after a READ_ATTR of the same
         * register has no lowering (JIT registers carry no null
         * state) → reject, program falls back. Likewise when the
         * null path could read the register before overwriting it
         * (emb_null_path_reg_safe) — the fused load leaves the
         * register UNDEFINED on the taken edge where the interpreter
         * would hold a null tag. */
        if (!allow_reg_ops) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        uint32_t direction = inst >> 31;
        if (direction != 0) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_EMBEDDED_BACKWARD;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_EMBEDDED_BACKWARD;
        }
        uint32_t branch_length = (inst >> 16) & 0x7FFFu;
        uint32_t target_emb_pc = emb_pc + branch_length;
        if (target_emb_pc >= emb_len) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        uint32_t guard_reg = (inst >> 6) & 0x7u;
        if (!prev_read_attr_pending || prev_read_attr_dst != guard_reg ||
            !emb_null_path_reg_safe(emb_prog, emb_len, target_emb_pc,
                                    guard_reg)) {
          /* ronsql_jit slice 2 item 2: NOT the 5D-3 fusion pattern —
           * FOLD the guard to nothing instead of rejecting. On any
           * row the JIT completes, no register ever holds SQL-NULL:
           * plain loads take the per-row fallback on NULL, NB-fused
           * loads branch their null edge away, constants are non-null,
           * the READ_AGG import's sources are the outer registers
           * (same invariant), and kOpSetRegNull rows fall back
           * entirely. An EQ_NULL branch is therefore never taken on a
           * completing row; emb_pc_to_op_idx for this pc (recorded at
           * the loop top) resolves any branch INTO it to the next
           * emitted op, exactly like the fused guard. */
          emb_pc += 1;
          break;
        }
        /* Rewrite the just-emitted load into the null-branching form.
         * Operand relayout: OP_LOAD_COL_NDB carries col_id in c;
         * OP_LOAD_COL_NDB_NB carries a=dst, b=col_id, c=branch target
         * (the engine patches HK_BRANCH_TAKE displacement from op->c). */
        {
          Op *load_op = &out_prog->ops[prev_read_attr_op_idx];
          load_op->kind = OP_LOAD_COL_NDB_NB;
          load_op->b = load_op->c;
          load_op->c = 0;   /* pass-2 fixup */
          pending_target_emb_pc[prev_read_attr_op_idx] =
              (uint16_t)target_emb_pc;
        }
        emb_pc += 1;
        break;
      }

      case BR_EMB_BRANCH_REG_NE_NULL: {
        /* ronsql_jit slice 2 item 2: by the completing-row invariant
         * (see the EQ_NULL fold above) the register is never NULL, so
         * NE_NULL is ALWAYS taken — lower to an unconditional
         * OP_JUMP to the branch target. */
        if (!allow_reg_ops) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        if ((inst >> 31) != 0) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_EMBEDDED_BACKWARD;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_EMBEDDED_BACKWARD;
        }
        uint32_t branch_length = (inst >> 16) & 0x7FFFu;
        uint32_t target_emb_pc = emb_pc + branch_length;
        if (target_emb_pc >= emb_len) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        pending_target_emb_pc[out_op_idx] = (uint16_t)target_emb_pc;
        if (!emit_op(out_prog, OP_JUMP, 0, 0, /*c=*/0, 0)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        emb_pc += 1;
        break;
      }

      case BR_EMB_READ_AGG_REG_TO_REG: {
        /* ronsql_jit slice 2 item 2 — the GREATEST/LEAST pair-op
         * import: OUTER register (m_registers in the interpreter) into
         * an EMBEDDED register. Both files live in regs_i64 (outer
         * 0-7, embedded 8-15) so this is OP_MOV_INT_INT — admitted
         * only when the outer walk proved the source's track makes
         * the embedded SIGNED i64 compares exact: I64 or NNC always;
         * U64 only when u63-safe (narrow unsigned / DATE / YEAR /
         * TIME2 sources — Bigunsigned and the sign-bit-set
         * DATETIME2/TIMESTAMP2 packs would misorder); F64 would
         * bit-compare doubles (wrong for negatives), STR/UNKNOWN have
         * no compare semantics — reject, program falls back whole.
         * NULL sources need no runtime handling: every outer-register
         * null path already left the JIT (per-row fallback /
         * kOpSetRegNull fallback), so completing rows import real
         * values only. */
        if (!allow_reg_ops || outer_reg_type == NULL) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        uint32_t src_outer = inst >> 16;
        uint32_t dst_emb   = (inst >> 6) & 0x7u;
        if (src_outer >= BC_EMB_REG_BASE) {
          /* The interpreter's outer file has 8 registers. */
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_REG_OUT_OF_RANGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        uint8_t track = outer_reg_type[src_outer];
        /* GL Part A (2026-09-01): F64 outer sources are ADMITTED —
         * the MOV is bit-preserving and the embedded dst is marked
         * F64, so the GL body's compare takes the OP_BRANCH_F64 arm
         * (double compare with runtime int→double promotion of any
         * non-F64 side — the interpreter's compareTypedRegs float
         * arm). Only BIGUNSIGNED-unsafe U64 / STR / UNKNOWN still
         * reject (Part B territory). */
        int import_f64 = (track == BR_REG_F64);
        /* GL Part B (2026-09-03): a U64 outer source that is NOT u63-safe
         * (BIGUNSIGNED column / constant, DECIMALUNSIGNED, u64
         * arithmetic) is admitted too and marked U64 in the embedded
         * tracker, so a compare takes the typed helper's unsigned arm.
         * Packed temporals (DATETIME2 / TIMESTAMP2 — outer_reg_pack)
         * stay rejected: the ordering of the loaded word under an
         * unsigned compare is unproven for them. */
        int u63_ok = (outer_reg_u63 != NULL && outer_reg_u63[src_outer]);
        int import_u64 = (track == BR_REG_U64 && !u63_ok &&
                          outer_reg_pack != NULL &&
                          !outer_reg_pack[src_outer]);
        int admissible =
            (track == BR_REG_I64 || track == BR_REG_NNC ||
             import_f64 || import_u64 ||
             (track == BR_REG_U64 && u63_ok));
        if (!admissible) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_TYPE_MISMATCH;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_TYPE_MISMATCH;
        }
        if (!emit_op(out_prog, OP_MOV_INT_INT,
                     (uint8_t)(BC_EMB_REG_BASE + dst_emb),
                     (uint16_t)src_outer, 0, 0)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        const16_valid[dst_emb] = 0;
        emb_reg_f64[dst_emb] = (uint8_t)import_f64;
        emb_reg_u64[dst_emb] = (uint8_t)import_u64;
        emb_reg_read_attr[dst_emb] = -1;
        emb_pc += 1;
        break;
      }

      case BR_EMB_READ_LINKED_COL_TO_REG: {
        /* ronsql_jit slice 2 item 4 — type-aware linked load into an
         * embedded register. The wire carries the NDB type, so
         * admission is static: signed widths sign-extend and narrow
         * unsigned zero-extend to exact signed i64; GL Part A
         * (2026-09-01): FLOAT/DOUBLE are ADMITTED too — the helper
         * stores the value's double BITS and the dst is marked F64,
         * routing any compare through the OP_BRANCH_F64 arm.
         * GL Part B (2026-09-03): BIGUNSIGNED is admitted as well — the
         * helper stores the raw 8 bytes and the dst is marked U64 for
         * the typed compare's unsigned arm. Anything else (strings,
         * temporals …) rejects TYPE_MISMATCH. At runtime a NULL / missing-buffer /
         * out-of-range read takes the per-row fallback (the folded
         * reg-null guards' path replays on the interpreter). Gated
         * like every linked op: the buffer only exists on the
         * join-agg path (scan filters reject → interpreter). */
        if (!allow_linked_ops || !allow_reg_ops) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        uint32_t dst_emb  = (inst >> 6) & 0x7u;
        uint32_t position = (inst >> 16) & 0xFFu;
        uint32_t type_id  = (inst >> 24) & 0xFFu;
        int admissible;
        int linked_f64 = 0;
        int linked_u64 = 0;
        switch (type_id) {
          case BR_NDB_TYPE_TINYINT:
          case BR_NDB_TYPE_TINYUNSIGNED:
          case BR_NDB_TYPE_SMALLINT:
          case BR_NDB_TYPE_SMALLUNSIGNED:
          case BR_NDB_TYPE_MEDIUMINT:
          case BR_NDB_TYPE_MEDIUMUNSIGNED:
          case BR_NDB_TYPE_INT:
          case BR_NDB_TYPE_UNSIGNED:
          case BR_NDB_TYPE_BIGINT:
            admissible = 1;
            break;
          case BR_NDB_TYPE_FLOAT:
          case BR_NDB_TYPE_DOUBLE:
            admissible = 1;
            linked_f64 = 1;
            break;
          case BR_NDB_TYPE_BIGUNSIGNED:
            admissible = 1;
            linked_u64 = 1;
            break;
          default:
            admissible = 0;
            break;
        }
        if (!admissible) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_TYPE_MISMATCH;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_TYPE_MISMATCH;
        }
        if (!emit_op(out_prog, OP_LOAD_LINKED_COL,
                     (uint8_t)(BC_EMB_REG_BASE + dst_emb),
                     (uint16_t)((position << 8) | type_id), 0, 0)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        const16_valid[dst_emb] = 0;
        emb_reg_f64[dst_emb] = (uint8_t)linked_f64;
        emb_reg_u64[dst_emb] = (uint8_t)linked_u64;
        emb_reg_read_attr[dst_emb] = -1;
        emb_pc += 1;
        break;
      }

      case BR_EMB_LOAD_CONST64: {
        /* Phase 5A: 64-bit constant into a register; the value lives in
         * the next 2 words, low word first (the emitter memcpy's an
         * Int64 on a little-endian host). Lowers to OP_LOAD_CONST_INT
         * (imm64 hole). The 2 data-word pcs keep the 0xFF "no op here"
         * mapping — a branch may not legally target the middle of an
         * instruction. */
        if (!allow_reg_ops) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        if (emb_pc + 3 > emb_len) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        uint32_t dst_reg = (inst >> 6) & 0x7u;
        uint64_t lo = emb_prog[emb_pc + 1];
        uint64_t hi = emb_prog[emb_pc + 2];
        int64_t  imm = (int64_t)(lo | (hi << 32));
        if (!emit_op(out_prog, OP_LOAD_CONST_INT,
                     (uint8_t)(BC_EMB_REG_BASE + dst_reg), 0, 0,
                     imm)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        const16_valid[dst_reg] = 0;
        emb_reg_f64[dst_reg] = 0;
        emb_reg_u64[dst_reg] = 0;
        emb_reg_read_attr[dst_reg] = -1;
        emb_pc += 3;
        break;
      }

      case BR_EMB_ADD_REG_REG:
      case BR_EMB_SUB_REG_REG:
      case BR_EMB_MUL_REG_REG: {
        /* ronsql_jit slice 2 item 7: WHERE-clause arithmetic. Wire:
         * src1 bits 6-8, src2 bits 9-11, dst bits 16-18. Lowers to
         * the OP_ARITH_FB cold call — signed-i64 math whose overflow
         * (and negative-SUB-result, see the op-kind doc) edge takes
         * the per-row fallback and exits, replaying the row on the
         * interpreter's typed semantics. F64-tracked operands reject
         * (double WHERE-arithmetic is a future item). */
        if (!allow_reg_ops) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        uint32_t src1 = (inst >> 6) & 0x7u;
        uint32_t src2 = (inst >> 9) & 0x7u;
        /* Destination field differs per op (Interpreter.hpp encoders):
         * Add/Sub put it at bits 16-18 (getReg4); Mul at bits 12-14
         * (getReg3). Decoding Mul's dst from bits 16-18 read 0 and
         * the product landed in the wrong register — the
         * subquery_agg_ext M11 wrong-result. */
        uint32_t dst  = (emb_op == BR_EMB_MUL_REG_REG)
                            ? (inst >> 12) & 0x7u
                            : (inst >> 16) & 0x7u;
        if (emb_reg_f64[src1] || emb_reg_f64[src2] ||
            emb_reg_u64[src1] || emb_reg_u64[src2]) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_TYPE_MISMATCH;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_TYPE_MISMATCH;
        }
        uint32_t code;
        switch (emb_op) {
          case BR_EMB_ADD_REG_REG: code = 0; break;
          case BR_EMB_SUB_REG_REG: code = 1; break;
          default:                 code = 2; break;
        }
        uint32_t arg = (code << 12) |
                       ((BC_EMB_REG_BASE + dst)  << 8) |
                       ((BC_EMB_REG_BASE + src1) << 4) |
                       (BC_EMB_REG_BASE + src2);
        /* Fallback edge: op->c tail-calls the tail OP_EXIT, resolved
         * via the pending-case-jump STOP sentinel; src2 rides in
         * op->d (c is the branch target for bc_op_is_branch kinds). */
        if (*n_pending_case_jumps >= BC_MAX_OPS) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        pending_case_jumps[*n_pending_case_jumps] = (PendingCaseJump){
          .op_idx = out_prog->n_ops,
          .target_word_pos = BR_CASE_JUMP_STOP,
        };
        (*n_pending_case_jumps)++;
        if (!emit_op(out_prog, OP_ARITH_FB,
                     (uint8_t)(BC_EMB_REG_BASE + dst),
                     (uint16_t)(BC_EMB_REG_BASE + src1),
                     /*c=*/0,
                     (int64_t)arg)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        out_prog->ops[out_prog->n_ops - 1].d =
            (uint16_t)(BC_EMB_REG_BASE + src2);
        const16_valid[dst] = 0;
        emb_reg_f64[dst] = 0;
        emb_reg_u64[dst] = 0;
        emb_reg_read_attr[dst] = -1;
        emb_pc += 1;
        break;
      }

      case BR_EMB_LOAD_DOUBLE_CONST: {
        /* ronsql_jit slice 2 item 6: IEEE double immediate (2 data
         * words, low first — same shape as LOAD_CONST64). F64 values
         * live bit-cast in regs_i64, so this IS OP_LOAD_CONST_INT
         * with the double's bit pattern; the F64 tracking makes a
         * following REG_REG compare take the OP_BRANCH_F64 arm. */
        if (!allow_reg_ops) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        if (emb_pc + 3 > emb_len) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        uint32_t dst_reg = (inst >> 6) & 0x7u;
        uint64_t lo = emb_prog[emb_pc + 1];
        uint64_t hi = emb_prog[emb_pc + 2];
        int64_t  imm = (int64_t)(lo | (hi << 32));
        if (!emit_op(out_prog, OP_LOAD_CONST_INT,
                     (uint8_t)(BC_EMB_REG_BASE + dst_reg), 0, 0,
                     imm)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        const16_valid[dst_reg] = 0;
        emb_reg_f64[dst_reg] = 1;
        emb_reg_u64[dst_reg] = 0;
        emb_reg_read_attr[dst_reg] = -1;
        emb_pc += 3;
        break;
      }

      case BR_EMB_READ_UINT8_MEM_TO_REG:
      case BR_EMB_READ_UINT16_MEM_TO_REG:
      case BR_EMB_READ_UINT32_MEM_TO_REG:
      case BR_EMB_READ_INT64_MEM_TO_REG: {
        /* ronsql_jit slice 2 item 6: heap-memory read at a constant
         * byte offset (RonSQL's legacy linked-value reads after
         * READ_LINKED_TO_MEM). Zero-extension for 1/2/4 bytes and
         * raw Int64 for 8 mirror the interpreter handlers; both are
         * exact under the signed-i64 compare model (u32 < 2^33).
         * The offset is a wire constant, so the interpreter's
         * runtime MAX_HEAP_OFFSET check becomes a compile-time
         * bound: out-of-range programs reject (the interpreter
         * would error the query — the fallback reproduces that). */
        if (!allow_reg_ops) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        uint32_t dst_reg = (inst >> 6) & 0x7u;
        uint32_t mem_off = inst >> 16;
        uint32_t width_code;
        switch (emb_op) {
          case BR_EMB_READ_UINT8_MEM_TO_REG:  width_code = 0; break;
          case BR_EMB_READ_UINT16_MEM_TO_REG: width_code = 1; break;
          case BR_EMB_READ_UINT32_MEM_TO_REG: width_code = 2; break;
          default:                            width_code = 3; break;
        }
        if (mem_off + (1u << width_code) - 1u > 65535u) {
          /* MAX_HEAP_OFFSET (DbtupExecQuery.cpp). */
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        if (!emit_op(out_prog, OP_READ_MEM_TO_REG,
                     (uint8_t)(BC_EMB_REG_BASE + dst_reg),
                     (uint16_t)mem_off,
                     (uint16_t)((width_code << 8) |
                                (BC_EMB_REG_BASE + dst_reg)),
                     0)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        const16_valid[dst_reg] = 0;
        emb_reg_f64[dst_reg] = 0;
        emb_reg_u64[dst_reg] = 0;
        emb_reg_read_attr[dst_reg] = -1;
        emb_pc += 1;
        break;
      }

      case BR_EMB_BRANCH_EQ_REG_REG:
      case BR_EMB_BRANCH_NE_REG_REG:
      case BR_EMB_BRANCH_LT_REG_REG:
      case BR_EMB_BRANCH_LE_REG_REG:
      case BR_EMB_BRANCH_GT_REG_REG:
      case BR_EMB_BRANCH_GE_REG_REG: {
        /* Phase 5A: register-register comparison branch — lowers to the
         * Phase 1 hot branch stencils. Word 0: opcode | left_reg<<6 |
         * right_reg<<9 | direction<<31 | branch_length<<16 (forward
         * only, like every embedded branch). Null registers cannot
         * reach the compare through the JIT: the only register writers
         * are LOAD_CONST16/64 (never null) and READ_ATTR, whose helper
         * flags NULL rows for interpreter fallback — or, when fused
         * with a BRANCH_REG_EQ_NULL guard (5D-3), branches around the
         * compare on NULL — so the hot compare on raw i64 values is
         * exact for every row the JIT completes. */
        if (!allow_reg_ops) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        uint32_t direction = inst >> 31;
        if (direction != 0) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_EMBEDDED_BACKWARD;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_EMBEDDED_BACKWARD;
        }
        uint32_t branch_length = (inst >> 16) & 0x7FFFu;
        uint32_t target_emb_pc = emb_pc + branch_length;
        if (target_emb_pc >= emb_len) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        uint32_t left_reg  = (inst >> 6) & 0x7u;
        uint32_t right_reg = (inst >> 9) & 0x7u;
        /* ronsql_jit slice 2 item 6: F64 arm. If either side is
         * F64-tracked, the compare is a double compare (the
         * interpreter's compareTypedRegs promotes the other side to
         * double). A non-F64 side whose defining op is a plain
         * READ_ATTR load is retroactively converted to the F64 load
         * (same a/c operand layout; its helper per-row-falls-back on
         * non-FLOAT/DOUBLE declared types, so a mistyped program
         * degrades per row instead of miscomparing); any other int
         * side (consts, imports, mem reads) converts signed-i64 →
         * double at runtime via the helper flags. */
        /* GL Part B (2026-09-03): a U64-marked side routes through the
         * same typed helper with the per-side unsigned flags (0x40 left,
         * 0x80 right) — the helper is compareTypedRegs' full lattice
         * (double arm, both-unsigned, both-signed, mixed: negative
         * signed < any unsigned). The READ_ATTR → F64-load conversion
         * below only applies when a DOUBLE side is present. */
        int any_f64 = (emb_reg_f64[left_reg] || emb_reg_f64[right_reg]);
        int any_u64 = (emb_reg_u64[left_reg] || emb_reg_u64[right_reg]);
        if (any_f64 || any_u64) {
          uint32_t sides[2] = { left_reg, right_reg };
          for (int si = 0; any_f64 && si < 2; ++si) {
            uint32_t r = sides[si];
            if (!emb_reg_f64[r] && !emb_reg_u64[r] &&
                emb_reg_read_attr[r] >= 0 &&
                out_prog->ops[emb_reg_read_attr[r]].kind ==
                    OP_LOAD_COL_NDB) {
              out_prog->ops[emb_reg_read_attr[r]].kind =
                  OP_LOAD_COL_NDB_F64;
              emb_reg_f64[r] = 1;
            }
          }
          uint32_t cond;
          switch (emb_op) {
            case BR_EMB_BRANCH_EQ_REG_REG: cond = 0; break;
            case BR_EMB_BRANCH_NE_REG_REG: cond = 1; break;
            case BR_EMB_BRANCH_LT_REG_REG: cond = 2; break;
            case BR_EMB_BRANCH_LE_REG_REG: cond = 3; break;
            case BR_EMB_BRANCH_GT_REG_REG: cond = 4; break;
            default:                       cond = 5; break;
          }
          uint32_t arg = cond |
              (emb_reg_f64[left_reg]  ? 0x10u : 0u) |
              (emb_reg_f64[right_reg] ? 0x20u : 0u) |
              (emb_reg_u64[left_reg]  ? 0x40u : 0u) |
              (emb_reg_u64[right_reg] ? 0x80u : 0u) |
              ((BC_EMB_REG_BASE + left_reg)  << 8) |
              ((BC_EMB_REG_BASE + right_reg) << 12);
          pending_target_emb_pc[out_op_idx] = (uint16_t)target_emb_pc;
          if (!emit_op(out_prog, OP_BRANCH_F64,
                       (uint8_t)(BC_EMB_REG_BASE + left_reg),
                       (uint16_t)(BC_EMB_REG_BASE + right_reg),
                       /*c=*/0, (int64_t)arg)) {
            if (out_err) {
              out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
              out_err->offending_word = outer_word_pos + 1 + emb_pc;
              out_err->offending_op   = emb_op;
            }
            return JIT_BRIDGE_PROG_TOO_LARGE;
          }
          emb_pc += 1;
          break;
        }
        uint8_t out_kind;
        switch (emb_op) {
          case BR_EMB_BRANCH_EQ_REG_REG: out_kind = OP_BRANCH_EQ_INT_INT; break;
          case BR_EMB_BRANCH_NE_REG_REG: out_kind = OP_BRANCH_NE_INT_INT; break;
          case BR_EMB_BRANCH_LT_REG_REG: out_kind = OP_BRANCH_LT_INT_INT; break;
          case BR_EMB_BRANCH_LE_REG_REG: out_kind = OP_BRANCH_LE_INT_INT; break;
          case BR_EMB_BRANCH_GT_REG_REG: out_kind = OP_BRANCH_GT_INT_INT; break;
          default:                       out_kind = OP_BRANCH_GE_INT_INT; break;
        }
        pending_target_emb_pc[out_op_idx] = (uint16_t)target_emb_pc;
        if (!emit_op(out_prog, out_kind,
                     (uint8_t)(BC_EMB_REG_BASE + left_reg),
                     (uint16_t)(BC_EMB_REG_BASE + right_reg),
                     /*c=*/0, 0)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        emb_pc += 1;
        break;
      }

      case BR_EMB_BRANCH_ATTR_EQ_NULL:
      case BR_EMB_BRANCH_ATTR_NE_NULL: {
        /* 2-word instruction. Word 0 has branch_offset in
         * bits 30..16; bit 31 = direction (0=forward; we
         * reject backward). Word 1 has attrId in bits 31..16. */
        if (emb_pc + 2 > emb_len) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        uint32_t direction = inst >> 31;
        if (direction != 0) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_EMBEDDED_BACKWARD;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_EMBEDDED_BACKWARD;
        }
        uint32_t branch_length = (inst >> 16) & 0x7FFFu;
        uint32_t target_emb_pc = emb_pc + branch_length;
        if (target_emb_pc >= emb_len) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        uint32_t inst2 = emb_prog[emb_pc + 1];
        uint32_t attr_id = (inst2 >> 16) & 0xFFFFu;
        if (attr_id > BR_MAX_LOCAL_ATTR_ID) {
          /* Phase 5.0 narrow scope: BRANCH_ATTR_*_NULL reads local
           * table columns only. Linked attributes are handled by
           * READ_LINKED_TO_MEM + BRANCH_LINKED_*_NULL, and pseudo
           * columns are outside this lowering. */
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_REG_OUT_OF_RANGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc + 1;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        uint8_t out_kind =
            (emb_op == BR_EMB_BRANCH_ATTR_EQ_NULL)
                ? OP_BRANCH_ATTR_EQ_NULL
                : OP_BRANCH_ATTR_NE_NULL;
        /* Record the target_emb_pc in the side-array indexed by
         * the Op's absolute output index. Pass 2 will remap to the
         * final output Op index. op->c is left at 0 here — it gets
         * populated in pass 2. */
        pending_target_emb_pc[out_op_idx] = (uint16_t)target_emb_pc;
        if (!emit_op(out_prog, out_kind, /*a=*/0,
                     (uint16_t)attr_id, /*c=*/0, 0)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        emb_pc += 2;
        break;
      }

      case BR_EMB_BRANCH_ATTR_OP_ARG:
      case BR_EMB_BRANCH_ATTR_OP_PARAM:
      case BR_EMB_BRANCH_ATTR_OP_ATTR: {
        /* Phase 7: WHERE col <op> literal (OP_ARG), ?param (OP_PARAM), or
         * col2 (OP_ATTR). All lower to OP_BRANCH_ATTR_OP_ARG — the JIT helper
         * reads the whole instruction from the program buffer by offset and
         * resolves the 2nd operand (inline literal / param-region lookup /
         * 2nd-column read) from the decoded opcode. We record the
         * instruction's offset (emb_pc) in op->b and the branch target in
         * op->c, then advance past the instruction (OP_ARG is
         * variable-length; OP_PARAM and OP_ATTR are 2 words). Only the
         * scan-filter path is wired (ctx->prog_buf / param_buf set); other
         * paths reject (fall back to interp). */
        if (!allow_attr_op_arg) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        /* word 0 = inst (cond/nulls/offset); word 1 = (attrId<<16)|argLen. */
        if (emb_pc + 2 > emb_len) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        /* EQ..GE (0..5) and, since ronsql_jit item 12, LIKE / NOT_LIKE
         * (6/7) lower — the helper's kernel eval has the interpreter's
         * m_like arm and returns the interpreter's error (40) for a
         * column type without one, so admission needs no type
         * knowledge (the scan-filter cache is keyed on bytecode alone
         * and cannot have any). AND_*_MASK conditions (8+) reject. */
        uint32_t cond = (inst >> 12) & 0xFu;
        if (cond > BR_EMB_MAX_ATTR_COND) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        uint32_t direction = inst >> 31;
        if (direction != 0) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_EMBEDDED_BACKWARD;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_EMBEDDED_BACKWARD;
        }
        uint32_t branch_length = (inst >> 16) & 0x7FFFu;
        uint32_t target_emb_pc = emb_pc + branch_length;
        if (target_emb_pc >= emb_len) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        uint32_t inst2 = emb_prog[emb_pc + 1];
        uint32_t attr_id = (inst2 >> 16) & 0xFFFFu;
        if (attr_id > BR_MAX_LOCAL_ATTR_ID) {
          /* Local table columns only — linked/pseudo are out of scope. */
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_REG_OUT_OF_RANGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc + 1;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        /* OP_ARG carries an inline literal (argLen bytes => ceil(argLen/4)
         * words after word 1). OP_PARAM and OP_ATTR have no inline data
         * (word 1's low 16 bits are a paramNo / the 2nd column's attrId, and
         * the value comes from the param region / a 2nd-column read), so both
         * are fixed 2-word instructions. For OP_ATTR the 2nd attrId must also
         * be a local column. */
        uint32_t inst_words;
        if (emb_op == BR_EMB_BRANCH_ATTR_OP_ARG) {
          uint32_t arg_len_bytes = inst2 & 0xFFFFu;
          inst_words = 2u + ((arg_len_bytes + 3u) >> 2);
        } else {
          inst_words = 2u;
          if (emb_op == BR_EMB_BRANCH_ATTR_OP_ATTR) {
            uint32_t attr2_id = inst2 & 0xFFFFu;
            if (attr2_id > BR_MAX_LOCAL_ATTR_ID) {
              if (out_err) {
                out_err->reason         = JIT_BRIDGE_REG_OUT_OF_RANGE;
                out_err->offending_word = outer_word_pos + 1 + emb_pc + 1;
                out_err->offending_op   = emb_op;
              }
              return JIT_BRIDGE_REG_OUT_OF_RANGE;
            }
          }
        }
        if (emb_pc + inst_words > emb_len) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        /* op->b = the instruction's word offset within ctx->prog_buf.
         * The BASE differs per path (the string-CASE unblock): the
         * scan filter's prog_buf IS the embedded words (base 0); the
         * aggregation path's prog_buf is the whole program past the
         * GROUP BY metadata, so its embedded words sit at
         * outer_word_pos + 1 — the caller passes attr_op_arg_base
         * accordingly. Both bases + emb_pc stay ≤
         * MAX_AGG_PROGRAM_WORD_SIZE (1024), well inside the 16-bit
         * op->b / narrow-MOVZ hole. op->c is the branch target
         * (pass-2 fixup). */
        if (attr_op_arg_base + emb_pc > 0xFFFFu) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        pending_target_emb_pc[out_op_idx] = (uint16_t)target_emb_pc;
        if (!emit_op(out_prog, OP_BRANCH_ATTR_OP_ARG, /*a=*/0,
                     /*b=*/(uint16_t)(attr_op_arg_base + emb_pc),
                     /*c=*/0, /*imm=*/0)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        emb_pc += inst_words;
        break;
      }

      case BR_EMB_BRANCH_MEM_OP_ARG:
      case BR_EMB_BRANCH_MEM_OP_ARG_INLINE: {
        /* ronsql_jit slice 2 item 5: the CTE-filter compare over the
         * cheapMemory[0] value. Same runtime model as ATTR_OP_ARG —
         * op->b records the instruction's word offset in
         * ctx->prog_buf and the helper re-decodes everything there
         * (dispatching 38 vs 40 on the opcode), so the bridge only
         * validates shape: condition EQ..GE, forward in-range
         * target, and the instruction's word count (38 = 4 header
         * words + literal, 40 = 3 + literal). Linked-gated: the
         * compared value comes from READ_LINKED_TO_MEM, which only
         * the join-agg path wires up. */
        if (!allow_attr_op_arg || !allow_linked_ops) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        if (emb_pc + 2 > emb_len) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        uint32_t cond = (inst >> 12) & 0xFu;
        if (cond > BR_EMB_MAX_BINARY_COND) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        uint32_t direction = inst >> 31;
        if (direction != 0) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_EMBEDDED_BACKWARD;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_EMBEDDED_BACKWARD;
        }
        uint32_t branch_length = (inst >> 16) & 0x7FFFu;
        uint32_t target_emb_pc = emb_pc + branch_length;
        if (target_emb_pc >= emb_len) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        uint32_t inst2 = emb_prog[emb_pc + 1];
        uint32_t arg_len_bytes = inst2 & 0xFFFFu;
        uint32_t header_words =
            (emb_op == BR_EMB_BRANCH_MEM_OP_ARG) ? 4u : 3u;
        uint32_t inst_words = header_words + ((arg_len_bytes + 3u) >> 2);
        if (emb_pc + inst_words > emb_len) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        if (attr_op_arg_base + emb_pc > 0xFFFFu) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        pending_target_emb_pc[out_op_idx] = (uint16_t)target_emb_pc;
        if (!emit_op(out_prog, OP_BRANCH_MEM_OP_ARG, /*a=*/0,
                     /*b=*/(uint16_t)(attr_op_arg_base + emb_pc),
                     /*c=*/0, /*imm=*/0)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        emb_pc += inst_words;
        break;
      }

      case BR_EMB_READ_LINKED_TO_MEM: {
        /* Linked-attr ops depend on the join-aggregation linked buffer
         * (the cold-call helpers reach it through JitState.ctx->join_agg).
         * A standalone scan filter has no join_agg, so reject these for
         * the scan-filter path rather than compile a program whose helper
         * would dereference a null context at runtime. Linked attrs only
         * arise from pushdown joins (JOIN_AGG), never plain SCAN_FRAGREQ
         * filters, so this should never fire in practice — it's a safety
         * guard. */
        if (!allow_linked_ops) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        /* Phase 5.1a: 1-word instruction. position is in bits
         * 16..23 of word 0 (8-bit field — sufficient for NDB's
         * linked-attr buffer position range). Translates to
         * OP_LOAD_LINKED_TO_MEM (a pure cold-call stencil — no
         * branch). */
        uint32_t position = (inst >> 16) & 0xFFu;
        if (position > 255) {
          /* Defensive — the bit-field is already 8-bit. */
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_REG_OUT_OF_RANGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        if (!emit_op(out_prog, OP_LOAD_LINKED_TO_MEM, /*a=*/0,
                     (uint8_t)position, /*c=*/0, 0)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        emb_pc += 1;
        break;
      }

      case BR_EMB_BRANCH_LINKED_EQ_NULL:
      case BR_EMB_BRANCH_LINKED_NE_NULL: {
        /* Reject on the scan-filter path — see READ_LINKED_TO_MEM above.
         * (A well-formed program reaches this only after a
         * READ_LINKED_TO_MEM, which would already have been rejected, but
         * guard here too for defence in depth.) */
        if (!allow_linked_ops) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        /* Phase 5.1a: 1-word instruction. branch_offset in bits
         * 30..16, direction in bit 31. No operand word — the
         * cheapMemory[0] state from the preceding
         * READ_LINKED_TO_MEM is implicit. */
        uint32_t direction = inst >> 31;
        if (direction != 0) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_EMBEDDED_BACKWARD;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_EMBEDDED_BACKWARD;
        }
        uint32_t branch_length = (inst >> 16) & 0x7FFFu;
        uint32_t target_emb_pc = emb_pc + branch_length;
        if (target_emb_pc >= emb_len) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_MALFORMED;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_MALFORMED;
        }
        uint8_t out_kind =
            (emb_op == BR_EMB_BRANCH_LINKED_EQ_NULL)
                ? OP_BRANCH_LINKED_EQ_NULL
                : OP_BRANCH_LINKED_NE_NULL;
        pending_target_emb_pc[out_op_idx] = (uint16_t)target_emb_pc;
        if (!emit_op(out_prog, out_kind, /*a=*/0, /*b=*/0,
                     /*c=*/0, 0)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        emb_pc += 1;
        break;
      }

      case BR_EMB_LOAD_CONST16: {
        /* Stages the accept-path skip_offset for a following
         * WRITE_INTERPRETER_OUTPUT (the JIT only models constants written
         * to output slot 0, the aggregation row-disposition slot).
         *
         * Phase 5A: in reg-ops mode the register is ALSO materialized
         * (OP_LOAD_CONST_UINT16) — a following BRANCH_*_REG_REG may
         * compare it: the SQL planner's CASE conditions load their
         * constant with LOAD_CONST16 whenever it fits 16 bits. The
         * staged copy still serves skip offsets; on a pure
         * row-disposition path the materialized load is a dead register
         * store (one mov per row — accepted). Branches targeting this pc
         * then resolve to the load itself, which is executed and
         * continues — semantically identical to the interpreter. */
        uint32_t reg = (inst >> 6) & 0x7u;
        uint32_t const16 = (inst >> 16) & 0xFFFFu;
        if (allow_reg_ops) {
          if (!emit_op(out_prog, OP_LOAD_CONST_UINT16,
                       (uint8_t)(BC_EMB_REG_BASE + reg), 0, 0,
                       (int64_t)const16)) {
            if (out_err) {
              out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
              out_err->offending_word = outer_word_pos + 1 + emb_pc;
              out_err->offending_op   = emb_op;
            }
            return JIT_BRIDGE_PROG_TOO_LARGE;
          }
        }
        const16_by_reg[reg] = (uint16_t)const16;
        const16_valid[reg] = 1;
        emb_reg_f64[reg] = 0;
        emb_reg_u64[reg] = 0;
        emb_reg_read_attr[reg] = -1;
        emb_pc += 1;
        break;
      }

      case BR_EMB_WRITE_INTERP_OUTPUT: {
        /* Writes the staged skip_offset to interpreter output slot 0. A
         * zero value is the natural fall-through to the next outer
         * aggregate instruction. A non-zero value is CASE row-disposition:
         * branch to the later outer instruction selected by the skip
         * offset. Other output slots are not part of aggregation row
         * disposition and still fall back to the interpreter. */
        uint32_t reg = (inst >> 6) & 0x7u;
        uint32_t out_slot = (inst >> 16) & 0xFFFFu;
        if (out_slot != 0 || !const16_valid[reg]) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        uint16_t skip_offset = const16_by_reg[reg];
        uint32_t case_target =
            (skip_offset == 0xFFFFu /* AGG_EMBEDDED_INTERP_STOP_PROGRAM */)
                ? BR_CASE_JUMP_STOP
                : outer_after_emb_pos + (uint32_t)skip_offset;
        /* Phase 5A fix: emit the disposition jump for skip_offset 0 TOO.
         * "Emit nothing, fall through to the outer ops" is only correct
         * when this output block is the LAST thing in the embedded
         * stream. A multi-arm CASE lays out several LC16/WRITE/EXIT_OK
         * blocks back to back — a zero-skip block in the middle would
         * fall through into the NEXT block's materialized LC16 + jump
         * and take the wrong arm (ndb_join_pushdown_agg Test 42:
         * rows for arm 0 landed in arm 1). The jump targets
         * outer_after_emb_pos + skip_offset, exactly like the nonzero
         * path; for a final block that's a jump to the next op — one
         * wasted jmp, always correct. (Both paths assume nothing
         * meaningful executes between WRITE and its EXIT_OK — true for
         * every emitter: output blocks are exactly LC16/WRITE/EXIT_OK.) */
        {
          uint16_t jump_op_idx = out_prog->n_ops;
          if (!emit_op(out_prog, OP_JUMP, 0, 0, 0, 0)) {
            if (out_err) {
              out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
              out_err->offending_word = outer_word_pos + 1 + emb_pc;
              out_err->offending_op   = emb_op;
            }
            return JIT_BRIDGE_PROG_TOO_LARGE;
          }
          if (*n_pending_case_jumps >= BC_MAX_OPS) {
            if (out_err) {
              out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
              out_err->offending_word = outer_word_pos + 1 + emb_pc;
              out_err->offending_op   = emb_op;
            }
            return JIT_BRIDGE_PROG_TOO_LARGE;
          }
          pending_case_jumps[*n_pending_case_jumps] = (PendingCaseJump){
            .op_idx = jump_op_idx,
            .target_word_pos = case_target,
          };
          /* GL Part A: the outer walk's merge-point guard needs the
           * tracker as it stands at this block's entry (= now: the
           * block cannot write outer registers). */
          br_snapshot_outer_tracker(
              &pending_case_jumps[*n_pending_case_jumps],
              outer_reg_type, outer_reg_u63, outer_reg_pack);
          (*n_pending_case_jumps)++;
        }
        emb_pc += 1;
        break;
      }

      case BR_EMB_EXIT_OK_LAST: {
        /* EXIT_OK_LAST = "row accepted AND terminate the fragment scan"
         * — the interpreter's handleExitOkLast sets
         * req_struct->last_row before exiting. The JIT has no channel
         * to signal last-row back to the scan layer, and lowering this
         * as a plain accept keeps the scan running past the row. That
         * is not hypothetical: ndb_get_table_statistics' stats scan
         * (interpret_exit_last_row — this opcode as the whole program)
         * then returns every row per fragment instead of one, each
         * carrying fragment-level ROW_COUNT, inflating stats.records
         * and thus COUNT(*). Reject → interpreter fallback. Known
         * emitters run the program once per fragment, so JIT'ing them
         * would win nothing anyway. */
        if (out_err) {
          out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
          out_err->offending_word = outer_word_pos + 1 + emb_pc;
          out_err->offending_op   = emb_op;
        }
        return JIT_BRIDGE_UNSUPPORTED_OP;
      }

      case BR_EMB_EXIT_OK: {
        /* EXIT_OK = "row accepted".
         *
         * Aggregation-embedded model (exit_ok_kind ==
         * BR_EXIT_OK_FALLTHROUGH): accept must continue into the outer
         * program's accumulator updates, so emit no Op.
         * emb_pc_to_op_idx for this pc points at the next emitted Op
         * (the outer agg's first instruction), so a branch landing on
         * the accept path resolves to the aggregation rather than this
         * no-op pc.
         *
         * Standalone scan-filter model (exit_ok_kind == OP_EXIT): there
         * are no following accumulator ops, and a scan filter typically
         * lays out EXIT_OK *before* the EXIT_REFUSE it guards
         * (BRANCH_ATTR_EQ_NULL -> EXIT_OK -> EXIT_REFUSE). Emitting no Op
         * there would let a fall-through accept run straight into the
         * following OP_FILTER_REJECT_EXIT and reject every accepted row.
         * So emit OP_EXIT — the function-return terminator that leaves
         * JitState::row_filter_rejected == 0 (accept). A branch that
         * targets EXIT_OK then resolves to this OP_EXIT, also correct. */
        if (exit_ok_kind != BR_EXIT_OK_FALLTHROUGH) {
          if (!emit_op(out_prog, exit_ok_kind, 0, 0, 0, 0)) {
            if (out_err) {
              out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
              out_err->offending_word = outer_word_pos + 1 + emb_pc;
              out_err->offending_op   = emb_op;
            }
            return JIT_BRIDGE_PROG_TOO_LARGE;
          }
        }
        emb_pc += 1;
        break;
      }

      case BR_EMB_EXIT_REFUSE: {
        /* Capture the refuse code (theInstruction >> 16 — same field the
         * interpreter's handleExitRefuse reads). The scan-filter runtime
         * TUPKEY_aborts with this code instead of a hardcoded one, so the
         * JIT reject behaves exactly like the interpreter (the LQH scan
         * layer decides skip-row vs abort-scan from the code). A boolean
         * WHERE filter rejects with one uniform code across the program;
         * a single per-program value can't represent differing codes, so
         * if two EXIT_REFUSE words disagree we reject the program (it
         * falls back to the interpreter). out_exit_refuse_code is NULL for
         * the aggregation/test paths, which don't need the code. */
        if (out_exit_refuse_code != NULL) {
          uint32_t refuse_code = inst >> 16;
          if (*out_exit_refuse_code != BR_NO_REFUSE_CODE &&
              *out_exit_refuse_code != refuse_code) {
            if (out_err) {
              out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
              out_err->offending_word = outer_word_pos + 1 + emb_pc;
              out_err->offending_op   = emb_op;
            }
            return JIT_BRIDGE_UNSUPPORTED_OP;
          }
          *out_exit_refuse_code = refuse_code;
        }
        /* Phase 5.0: EXIT_REFUSE = "row rejected" = early-
         * terminate the JIT'd function (skip accumulators).
         * OP_EXIT's stencil is the function-return sequence,
         * so executing it stops row processing immediately. */
        if (!emit_op(out_prog, exit_refuse_kind, 0, 0, 0, 0)) {
          if (out_err) {
            out_err->reason         = JIT_BRIDGE_PROG_TOO_LARGE;
            out_err->offending_word = outer_word_pos + 1 + emb_pc;
            out_err->offending_op   = emb_op;
          }
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        emb_pc += 1;
        break;
      }

      default:
        if (out_err) {
          out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
          out_err->offending_word = outer_word_pos + 1 + emb_pc;
          out_err->offending_op   = emb_op;
        }
        return JIT_BRIDGE_UNSUPPORTED_OP;
    }
  }

  /* Pass 2: fix up branch targets. Only walk Ops emitted by THIS
   * embedded block (i.e., from first_op_idx_at_entry to n_ops).
   * pending_target_emb_pc[] tells us which slots need fixup. */
  for (uint16_t i = first_op_idx_at_entry; i < out_prog->n_ops; ++i) {
    uint16_t target_emb_pc = pending_target_emb_pc[i];
    if (target_emb_pc == BR_EMB_NO_PENDING_FIXUP) continue;
    if (target_emb_pc >= emb_len) {
      /* Caught at admission, but defensive. */
      if (out_err) {
        out_err->reason         = JIT_BRIDGE_MALFORMED;
        out_err->offending_word = outer_word_pos;
        out_err->offending_op   = BR_kOpEmbeddedInterp;
      }
      return JIT_BRIDGE_MALFORMED;
    }
    uint8_t target_op_idx = emb_pc_to_op_idx[target_emb_pc];
    if (target_op_idx == 0xFF) {
      /* Branch into a non-emitted pc (e.g., the unused EXIT_OK
       * slot). For Phase 5.0 we treat this as "fall-through to
       * outer program" — the target becomes the next Op the
       * outer translation emits after returning from the
       * embedded block. We can't know that index yet, so set
       * c to 0 and the JIT engine treats this as a fall-through
       * via the trailing tail. */
      /* TODO Phase 5.1: handle this more cleanly. For Phase
       * 5.0's narrow scope (BRANCH_ATTR_NE_NULL → EXIT_REFUSE)
       * this case shouldn't arise. Reject defensively. */
      if (out_err) {
        out_err->reason         = JIT_BRIDGE_UNSUPPORTED_OP;
        out_err->offending_word = outer_word_pos;
        out_err->offending_op   = BR_kOpEmbeddedInterp;
      }
      return JIT_BRIDGE_UNSUPPORTED_OP;
    }
    out_prog->ops[i].c = target_op_idx;
  }
  return JIT_BRIDGE_OK;
}

JitBridgeReason ndb_jit_bridge_translate_embedded_for_test(
    const uint32_t *emb_prog,
    uint32_t       emb_len,
    Program       *out_prog,
    JitBridgeError *out_err,
    uint32_t       outer_word_pos) {
  memset(out_prog, 0, sizeof(*out_prog));
  if (out_err != NULL) {
    out_err->reason = JIT_BRIDGE_OK;
    out_err->offending_word = 0;
    out_err->offending_op = 0;
  }
  PendingCaseJump pending_case_jumps[BC_MAX_OPS];
  uint16_t n_pending_case_jumps = 0;
  return translate_embedded_block(emb_prog, emb_len, out_prog, out_err,
                                  outer_word_pos, outer_word_pos + 1 + emb_len,
                                  OP_EXIT,
                                  BR_EXIT_OK_FALLTHROUGH,
                                  /*allow_linked_ops=*/1,
                                  /*allow_attr_op_arg=*/1,
                                  /*attr_op_arg_base=*/outer_word_pos + 1,
                                  /*allow_reg_ops=*/1,
                                  pending_case_jumps,
                                  &n_pending_case_jumps,
                                  /*out_exit_refuse_code=*/NULL,
                                  /*outer_reg_type=*/NULL,
                                  /*outer_reg_u63=*/NULL,
                                  /*outer_reg_pack=*/NULL);
}

JitBridgeReason ndb_jit_bridge_translate_scan_filter(
    const uint32_t *filter_prog,
    uint32_t       n_words,
    Program       *out_prog,
    JitBridgeError *out_err,
    uint32_t       *out_reject_code) {
  memset(out_prog, 0, sizeof(*out_prog));
  if (out_err != NULL) {
    out_err->reason = JIT_BRIDGE_OK;
    out_err->offending_word = 0;
    out_err->offending_op = 0;
  }
  if (out_reject_code != NULL) {
    *out_reject_code = 0;
  }

  PendingCaseJump pending_case_jumps[BC_MAX_OPS];
  uint16_t n_pending_case_jumps = 0;
  uint32_t refuse_code = BR_NO_REFUSE_CODE;
  JitBridgeReason rc =
      translate_embedded_block(filter_prog, n_words, out_prog, out_err,
                               /*outer_word_pos=*/0,
                               /*outer_after_emb_word_pos=*/n_words,
                               OP_FILTER_REJECT_EXIT,
                               /*exit_ok_kind=*/OP_EXIT,
                               /*allow_linked_ops=*/0,
                               /*allow_attr_op_arg=*/1,
                               /*attr_op_arg_base=*/0,
                               /*allow_reg_ops=*/0,
                               pending_case_jumps,
                               &n_pending_case_jumps,
                               &refuse_code,
                               /*outer_reg_type=*/NULL,
                               /*outer_reg_u63=*/NULL,
                                  /*outer_reg_pack=*/NULL);
  if (rc != JIT_BRIDGE_OK) {
    return rc;
  }

  /* Publish the program's (uniform) EXIT_REFUSE code. If the filter has
   * no reject path (no EXIT_REFUSE), leave it 0 — the runtime never
   * rejects such a program, so the value is unused. The DBTUP glue maps
   * this to the TUPKEY_abort code on the reject path. */
  if (out_reject_code != NULL && refuse_code != BR_NO_REFUSE_CODE) {
    *out_reject_code = refuse_code;
  }

  /* WRITE_INTERPRETER_OUTPUT skip-offset handling is meaningful only
   * when an embedded filter is embedded inside an outer aggregation
   * program. A standalone scan filter has no outer aggregate word stream
   * to skip through, so reject those programs until Phase 7 grows a
   * dedicated standalone CASE disposition model. */
  if (n_pending_case_jumps != 0) {
    set_err(out_err, JIT_BRIDGE_UNSUPPORTED_OP, 0,
            BR_EMB_WRITE_INTERP_OUTPUT);
    return JIT_BRIDGE_UNSUPPORTED_OP;
  }

  if (out_prog->n_ops >= BC_MAX_OPS) {
    set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, n_words, 0);
    return JIT_BRIDGE_PROG_TOO_LARGE;
  }
  emit_op(out_prog, OP_EXIT, 0, 0, 0, 0);
  return JIT_BRIDGE_OK;
}

/* ------------------------------------------------------------------ */
/* Phase 5D-1 — null-branch conversion pass.                          */
/*                                                                    */
/* Rewrites eligible outer OP_LOAD_COL_NDB ops to OP_LOAD_COL_NDB_NB, */
/* whose taken edge (a NULL column value) skips the loaded register's */
/* whole consumer chain — reproducing the interpreter kernels'        */
/* null-skip (no accumulator update, no writeback-mask marks, no      */
/* arithmetic on garbage). Loads whose skip range cannot be proven    */
/* safe simply KEEP the row-fallback stencil — per-load degradation,  */
/* never a program-level reject.                                      */
/* ------------------------------------------------------------------ */

/* Register read-set / written-register classification for the op     */
/* kinds that can appear in the translated stream. Kinds not listed   */
/* read and write no REGISTERS (helper branches, jumps, terminators). */
static int nb_op_reads_reg(const Op *op, uint8_t reg) {
  switch (op->kind) {
    case OP_MOV_INT_INT:
      return op->b == reg;
    case OP_ADD_INT_INT:
    case OP_MINUS_INT_INT:
    case OP_MUL_INT_INT:
    case OP_ADD_INT_INT_CHECKED:
    case OP_MINUS_INT_INT_CHECKED:
    case OP_MUL_INT_INT_CHECKED:
    case OP_ADD_F64:
    case OP_MINUS_F64:
    case OP_MUL_F64:
    case OP_DIV_F64:
    case OP_ADD_U64_CHECKED:
    case OP_MINUS_U64_CHECKED:
    case OP_MUL_U64_CHECKED:
    case OP_DIV_INT_CHECKED:
    case OP_MOD_INT:
    case OP_DIV_U64:
    case OP_MOD_U64:
      return op->b == reg || op->c == reg;
    /* Conversion divide/arith (5E-3/5I): a = dst (also the lhs),
     * b = src; c is the packed helper argument, NOT a register. */
    case OP_DIV_CONV_F64:
    case OP_ARITH_CONV_F64:
    case OP_DIVMOD_CONV:
      return op->a == reg || op->b == reg;
    /* Accumulators read their source register (op->b). COUNT is
     * listed deliberately: its stencil ignores the register, but the
     * INTERPRETER's Count kernel skips null registers — treating
     * COUNT as a reader is exactly what gives COUNT(nullable_col)
     * its null-skip via the branch. */
    case OP_SUM_BIGINT:
    case OP_SUM_BIGINT_CHECKED:
    case OP_SUM_U64_CHECKED:
    case OP_SUM_F64:
    case OP_MIN_BIGINT:
    case OP_MAX_BIGINT:
    case OP_MIN_U64:
    case OP_MAX_U64:
    case OP_MIN_F64:
    case OP_MAX_F64:
    case OP_COUNT_BIGINT:
      return op->b == reg;
    case OP_BRANCH_LT_INT_INT:
    case OP_BRANCH_LE_INT_INT:
    case OP_BRANCH_EQ_INT_INT:
    case OP_BRANCH_GT_INT_INT:
    case OP_BRANCH_GE_INT_INT:
    case OP_BRANCH_NE_INT_INT:
      return op->a == reg || op->b == reg;
    /* ronsql_jit slice 2 item 2: SetRegNull is conservatively a
     * READER of its register — an NB null-skip chain containing it
     * must degrade the load to the plain per-row-fallback form (the
     * interpreter's null semantics for the skipped chain are not
     * representable around it). */
    case OP_SET_REG_NULL_FB:
      return op->a == reg;
    /* ronsql_jit slice 2 item 6: the F64 compare reads both regs. */
    case OP_BRANCH_F64:
      return op->a == reg || op->b == reg;
    /* ronsql_jit slice 2 item 7: arith reads both sources (src2 in
     * op->d -- c is the fallback branch target). */
    case OP_ARITH_FB:
      return op->b == reg || op->d == reg;
    default:
      return 0;
  }
}

/* Returns the register the op writes, or -1. */
static int nb_op_written_reg(const Op *op) {
  switch (op->kind) {
    case OP_LOAD_CONST_INT:
    case OP_LOAD_CONST_UINT16:
    case OP_LOAD_CONST_INT16:
    case OP_LOAD_CONST_UINT32:
    case OP_LOAD_CONST_INT32:
    case OP_LOAD_COL_INT:
    case OP_LOAD_COL_NDB:
    case OP_LOAD_COL_NDB_F64:
    case OP_LOAD_COL_NDB_U64:
    case OP_LOAD_COL_NDB_NB:
    case OP_LOAD_COL_NDB_F64_NB:
    case OP_LOAD_COL_NDB_U64_NB:
    case OP_LOAD_COL_NDB_DEC:
    case OP_LOAD_LINKED_COL:
    case OP_READ_MEM_TO_REG:
    case OP_ARITH_FB:
    case OP_MOV_INT_INT:
    case OP_ADD_INT_INT:
    case OP_MINUS_INT_INT:
    case OP_MUL_INT_INT:
    case OP_ADD_INT_INT_CHECKED:
    case OP_MINUS_INT_INT_CHECKED:
    case OP_MUL_INT_INT_CHECKED:
    case OP_ADD_F64:
    case OP_MINUS_F64:
    case OP_MUL_F64:
    case OP_DIV_F64:
    case OP_ADD_U64_CHECKED:
    case OP_MINUS_U64_CHECKED:
    case OP_MUL_U64_CHECKED:
    case OP_DIV_INT_CHECKED:
    case OP_MOD_INT:
    case OP_DIV_U64:
    case OP_MOD_U64:
    case OP_DIV_CONV_F64:
    case OP_ARITH_CONV_F64:
    case OP_DIVMOD_CONV:
      return op->a;
    default:
      return -1;
  }
}

static int nb_op_is_accumulator(uint8_t kind) {
  switch (kind) {
    case OP_SUM_BIGINT:
    case OP_SUM_BIGINT_CHECKED:
    case OP_SUM_U64_CHECKED:
    case OP_SUM_F64:
    case OP_MIN_BIGINT:
    case OP_MAX_BIGINT:
    case OP_MIN_U64:
    case OP_MAX_U64:
    case OP_MIN_F64:
    case OP_MAX_F64:
    case OP_COUNT_BIGINT:
    /* Phase 5F-1: the fused string MIN/MAX mutates its AggResItem —
     * a side effect that must never be skipped for unrelated NULLs. */
    case OP_MINMAX_STR_NDB:
      return 1;
    default:
      return 0;
  }
}

static int nb_op_is_terminator(uint8_t kind) {
  return kind == OP_EXIT || kind == OP_SKIP ||
         kind == OP_OVERFLOW_EXIT || kind == OP_FILTER_REJECT_EXIT;
}

/* True when `reg`, written by an op inside a candidate's skip range,
 * is dead after the range: rewritten by another register-writing op
 * before any read, or never touched again. */
static int nb_reg_dead_after(const Program *prog, uint16_t from,
                             uint8_t reg) {
  for (uint16_t k = from; k < prog->n_ops; k++) {
    const Op *op = &prog->ops[k];
    if (nb_op_is_terminator(op->kind)) return 1;
    if (nb_op_reads_reg(op, reg)) return 0;
    if (nb_op_written_reg(op) == (int)reg) return 1;
  }
  return 1;
}

static void nb_convert_loads(Program *prog, const uint8_t *op_from_emb) {
  uint8_t dep[BC_MAX_OPS];
  for (uint16_t i = 0; i < prog->n_ops; i++) {
    /* 5D-1: i64 loads; 5D-2: their f64/u64 siblings. */
    uint8_t nb_kind;
    switch (prog->ops[i].kind) {
      case OP_LOAD_COL_NDB:     nb_kind = OP_LOAD_COL_NDB_NB;     break;
      case OP_LOAD_COL_NDB_F64: nb_kind = OP_LOAD_COL_NDB_F64_NB; break;
      case OP_LOAD_COL_NDB_U64: nb_kind = OP_LOAD_COL_NDB_U64_NB; break;
      default: continue;
    }
    if (op_from_emb[i]) continue;

    /* Pass 1: taint walk — find the last op that (transitively)
     * depends on the loaded register, recording per-op dependence. */
    uint8_t taint[BC_MAX_REGS];
    memset(taint, 0, sizeof(taint));
    memset(dep, 0, sizeof(dep));
    taint[prog->ops[i].a] = 1;
    uint16_t last_dep = i;
    for (uint16_t j = (uint16_t)(i + 1); j < prog->n_ops; j++) {
      const Op *op = &prog->ops[j];
      if (nb_op_is_terminator(op->kind)) break;
      int reads_taint = 0;
      for (uint8_t r = 0; r < BC_MAX_REGS; r++) {
        if (taint[r] && nb_op_reads_reg(op, r)) { reads_taint = 1; break; }
      }
      int wr = nb_op_written_reg(op);
      if (reads_taint) {
        dep[j] = 1;
        last_dep = j;
        if (wr >= 0) taint[wr] = 1;
      } else if (wr >= 0 && taint[wr]) {
        taint[wr] = 0;   /* overwritten with an untainted value */
      }
    }
    if (last_dep == i) continue;   /* dead load — leave it alone */
    uint16_t target = (uint16_t)(last_dep + 1);

    /* Pass 2: every op in (i, last_dep] must be safe to skip. */
    int ok = 1;
    for (uint16_t j = (uint16_t)(i + 1); j <= last_dep && ok; j++) {
      const Op *op = &prog->ops[j];
      if (op_from_emb[j]) { ok = 0; break; }
      if (dep[j]) continue;        /* dependent — skipping IS the goal */
      /* Independent op inside the range. An earlier-converted NB load
       * is fine if its own null branch stays within our skip range. */
      if (op->kind == OP_LOAD_COL_NDB_NB ||
          op->kind == OP_LOAD_COL_NDB_F64_NB ||
          op->kind == OP_LOAD_COL_NDB_U64_NB) {
        if (op->c > target) ok = 0;
        continue;
      }
      if (bc_op_is_branch(op->kind)) { ok = 0; break; }
      if (nb_op_is_accumulator(op->kind)) { ok = 0; break; }
      int wr = nb_op_written_reg(op);
      if (wr < 0) { ok = 0; break; }   /* unknown side effects */
      /* A pure register write is skippable only if nothing after the
       * range reads the value it would have produced. */
      if (!nb_reg_dead_after(prog, target, (uint8_t)wr)) { ok = 0; break; }
    }
    if (!ok) continue;               /* keep the row-fallback load */

    /* Convert: col_id moves c -> b, target rides c (the engine patches
     * HK_BRANCH_TAKE displacement from op->c). */
    prog->ops[i].kind = nb_kind;
    prog->ops[i].b = prog->ops[i].c;
    prog->ops[i].c = target;
  }
}

/* ------------------------------------------------------------------ */
/* Main translation.                                                  */
/* ------------------------------------------------------------------ */

JitBridgeReason ndb_jit_bridge_translate(const uint32_t *ndb_prog,
                                          uint32_t       n_words,
                                          Program       *out_prog,
                                          JitBridgeError *out_err) {
  /* Defensive zero — caller may have left junk in here. */
  memset(out_prog, 0, sizeof(*out_prog));
  if (out_err != NULL) {
    out_err->reason = JIT_BRIDGE_OK;
    out_err->offending_word = 0;
    out_err->offending_op   = 0;
  }

  /* Phase 5C-1: linear register-type tracker. The kernel-side
   * OptimizeProgramBuffer type-specializes the bytecode before every
   * compile (both the Create and DblqhProxy paths), so the bridge only
   * sees typed opcodes — this tracker validates that the operand
   * registers actually carry the type the opcode claims (defense in
   * depth against optimizer bugs, and the safety fence that keeps an
   * f64 bit pattern out of an i64 op once 5C-2 lands).
   *
   * Rules (documented invariants, not a dataflow lattice):
   * - Producers set their destination's type (loads/consts from their
   *   explicit type field; Mov copies; Bigint arithmetic yields I64).
   * - I64-consuming ops reject F64 and U64 operands but TOLERATE
   *   UNKNOWN: everything producible today — including the 5A
   *   embedded ops, which write registers this linear walk cannot
   *   see — is i64 (the embedded READ_ATTR per-row-falls-back on
   *   BIGUNSIGNED and FLOAT/DOUBLE columns, so neither u64 nor f64
   *   bits ever enter a register from an embedded path). Revisit
   *   that tolerance if an embedded op ever produces one.
   * - F64/U64-consuming ops (5C-2/5C-3) REQUIRE the proven type —
   *   their stencils reinterpret the register bits, so an UNKNOWN
   *   (embedded-invalidated) register must not reach them.
   * - An embedded block invalidates every register (the optimizer
   *   itself skips embedded blocks, and the planner re-loads outer
   *   registers after them — same convention). */
  /* Register-track enum hoisted to file scope (ronsql_jit slice 2
   * item 2) — translate_embedded_block's READ_AGG_REG_TO_REG import
   * consults the same track values. See BR_REG_* above
   * translate_embedded_block. */
  uint8_t reg_type[BC_MAX_REGS];
  memset(reg_type, BR_REG_UNKNOWN, sizeof(reg_type));
  /* ronsql_jit slice 2 item 2: per-register "u64 value provably
   * < 2^63" flag, set alongside BR_REG_U64 for loads whose source
   * type is width-bounded (narrow unsigned ints, DATE/YEAR/TIME2
   * packed values) and cleared otherwise. Consulted ONLY by the
   * embedded READ_AGG_REG_TO_REG import: the embedded compare
   * stencils are SIGNED i64, which orders u64 values correctly iff
   * they stay below 2^63 (Bigunsigned and the 8-byte big-endian
   * DATETIME2/TIMESTAMP2 packs — whose sign bit is set for valid
   * dates — do not qualify). */
  uint8_t reg_u63_safe[BC_MAX_REGS];
  memset(reg_u63_safe, 0, sizeof(reg_u63_safe));
  /* GL Part B (2026-09-03): per-register "packed temporal" flag — set
   * by a DATETIME2 / TIMESTAMP2 LoadCol (U64 track, not u63-safe),
   * cleared by the other register-defining loads / consts, propagated
   * by kOpMov, OR-merged at CASE-jump targets. The embedded op-43
   * import admits a non-u63 U64 register only when this is clear:
   * whether the loaded word of a big-endian temporal pack orders
   * correctly under an unsigned compare is unproven, and RonSQL never
   * asks for GREATEST over a DATETIME2 anyway. Arithmetic results keep
   * the dst's previous flag (conservative). */
  uint8_t reg_u64_pack[BC_MAX_REGS];
  memset(reg_u64_pack, 0, sizeof(reg_u64_pack));
#define BR_REQUIRE_F64(r)                                          \
  do {                                                             \
    if (reg_type[(r)] != BR_REG_F64) {                             \
      set_err(out_err, JIT_BRIDGE_TYPE_MISMATCH, this_pos, op);    \
      return JIT_BRIDGE_TYPE_MISMATCH;                             \
    }                                                              \
  } while (0)

  /* Phase 5C-3: uniform signedness per accumulator slot. A multi-arm
   * CASE emits several aggregate ops targeting the SAME agg_index; if
   * the arms' value families differed (signed vs unsigned vs double),
   * the per-row writeback masks (value_unsigned / value_double) would
   * reflect whichever arm ran LAST rather than the accumulated
   * value's true family — the interpreter mutates AggResItem
   * signedness dynamically per row, which the JIT cannot represent.
   * Reject mixed-family programs; they fall back whole. */
  enum { BR_ACC_UNSET = 0, BR_ACC_I64 = 1, BR_ACC_F64 = 2,
         BR_ACC_U64 = 3, BR_ACC_STR = 4 };
  uint8_t acc_family[BC_MAX_ACCS];
  memset(acc_family, BR_ACC_UNSET, sizeof(acc_family));
#define BR_CLAIM_ACC_FAMILY(slot, family)                          \
  do {                                                             \
    if (acc_family[(slot)] == BR_ACC_UNSET) {                      \
      acc_family[(slot)] = (family);                               \
    } else if (acc_family[(slot)] != (family)) {                   \
      set_err(out_err, JIT_BRIDGE_TYPE_MISMATCH, this_pos, op);    \
      return JIT_BRIDGE_TYPE_MISMATCH;                             \
    }                                                              \
  } while (0)

  uint32_t pos = 0;
  uint16_t checked_arith_ops[BC_MAX_OPS];
  uint16_t n_checked_arith_ops = 0;
  PendingCaseJump pending_case_jumps[BC_MAX_OPS];
  uint16_t n_pending_case_jumps = 0;
  uint32_t outer_word_pos[BC_MAX_OPS];
  uint16_t outer_op_idx[BC_MAX_OPS];
  uint16_t n_outer_map = 0;
  /* Phase 5D-1: which output ops were emitted by an embedded block —
   * the null-branch conversion pass must neither convert those loads
   * nor skip over embedded ops. */
  uint8_t op_from_emb[BC_MAX_OPS];
  memset(op_from_emb, 0, sizeof(op_from_emb));
  /* GL Part A: set by kOpSkip — the fall-through into the next word
   * does not execute; cleared when a pending jump lands. */
  int linear_dead = 0;
  while (pos < n_words) {
    uint32_t word = ndb_prog[pos];
    uint8_t  op   = (uint8_t)((word & 0xFC000000u) >> 26);
    uint32_t this_pos = pos;

    if (n_outer_map >= BC_MAX_OPS) {
      set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
      return JIT_BRIDGE_PROG_TOO_LARGE;
    }
    outer_word_pos[n_outer_map] = this_pos;
    outer_op_idx[n_outer_map] = out_prog->n_ops;
    n_outer_map++;

    /* GL Part A (2026-09-01): control-flow MERGE guard for the linear
     * register tracker. CASE-disposition jumps (embedded output blocks,
     * outer kOpSkip) make some outer words reachable on more than one
     * path, and the tracker only ever followed the linear one. Every
     * pending jump landing HERE carries the tracker as it stood at its
     * source; it must agree with the linear state on each register's
     * bit-reinterpretation class wherever both know it — otherwise a
     * typed consumer downstream reinterprets the bits of the path the
     * tracker never saw. The motivating shape is the GREATEST/LEAST
     * trellis: after `Mov(dest, src)` the tracker says dest has src's
     * type, but the output=1 path SKIPS the Mov and arrives with dest's
     * ORIGINAL type. Same-class pairs were always sound; Part A's F64
     * admission made a DOUBLE-vs-BIGINT pair compilable, for which
     * `SumDouble(dest)` would add int bits as a double on output=1
     * rows (the interpreter's linear OptimizeProgramBuffer has the
     * same blind spot — the JIT must not add a second wrong answer).
     * Same-class disagreements (I64 / U64 / NNC) keep the linear value
     * exactly as before; the u63 proof ANDs. A DEAD linear predecessor
     * (previous word was kOpSkip) is not compared: the first arriving
     * jump's state IS the state — a CASE arm may reuse a register the
     * previous arm typed differently, which must not false-reject. */
    {
      int landed = 0;
      for (uint16_t j = 0; j < n_pending_case_jumps; j++) {
        const PendingCaseJump *pj = &pending_case_jumps[j];
        if (!pj->has_snap || pj->target_word_pos != this_pos) continue;
        if (linear_dead && !landed) {
          memcpy(reg_type, pj->reg_type_snap, BC_EMB_REG_BASE);
          memcpy(reg_u63_safe, pj->reg_u63_snap, BC_EMB_REG_BASE);
          memcpy(reg_u64_pack, pj->reg_pack_snap, BC_EMB_REG_BASE);
        } else {
          for (uint8_t r = 0; r < BC_EMB_REG_BASE; r++) {
            uint8_t lc = br_track_class(reg_type[r]);
            uint8_t jc = br_track_class(pj->reg_type_snap[r]);
            if (lc == 0 || jc == 0) continue;
            if (lc != jc) {
              set_err(out_err, JIT_BRIDGE_TYPE_MISMATCH, this_pos, op);
              return JIT_BRIDGE_TYPE_MISMATCH;
            }
            reg_u63_safe[r] =
                (uint8_t)(reg_u63_safe[r] & pj->reg_u63_snap[r]);
            /* A pack on either path keeps the register un-importable. */
            reg_u64_pack[r] =
                (uint8_t)(reg_u64_pack[r] | pj->reg_pack_snap[r]);
          }
        }
        landed = 1;
      }
      if (landed) linear_dead = 0;
    }

    switch (op) {
      case BR_kOpLoadConst: {
        /* 1 instr word + 2 value words = 3 words total. */
        if (pos + 3 > n_words) {
          set_err(out_err, JIT_BRIDGE_MALFORMED, this_pos, op);
          return JIT_BRIDGE_MALFORMED;
        }
        uint8_t type      = (uint8_t)((word >> 21) & 0x1Fu);
        uint8_t reg_index = (uint8_t)((word >> 16) & 0x0Fu);
        /* Phase 5C-2/5C-3: DOUBLE and BIGUNSIGNED constants are
         * admitted — bits are bits. The two value words carry the
         * bit pattern, which rides the same imm64 path as a signed
         * constant; only the tracker type differs. */
        if (type != BR_NDB_TYPE_BIGINT && type != BR_NDB_TYPE_DOUBLE &&
            type != BR_NDB_TYPE_BIGUNSIGNED) {
          set_err(out_err, JIT_BRIDGE_NON_BIGINT, this_pos, op);
          return JIT_BRIDGE_NON_BIGINT;
        }
        /* OUTER register bound = the interpreter's 8-register file
         * (BC_EMB_REG_BASE). BC_MAX_REGS is 16 only because the JIT
         * renames EMBEDDED registers into slots 8-15 (ronsql_jit
         * slice 2 item 2) — no wire program may name them. */
        if (reg_index >= BC_EMB_REG_BASE) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        int64_t value = read_int64_le(ndb_prog, pos + 1);
        /* Phase 4.7: dispatch to the smallest-fitting LoadConst
         * variant. Smallest first so each fast path exits early.
         * Most NDB query literals are small non-negative integers
         * and route to the 8 B uint16 stencil. */
        OpKind kind;
        if (value >= 0 && value <= 0xFFFFLL) {
          kind = OP_LOAD_CONST_UINT16;        /*  8 B */
        } else if (value >= -32768 && value <= -1) {
          kind = OP_LOAD_CONST_INT16;         /* 12 B (negative-only) */
        } else if (value >= 0 && value <= 0xFFFFFFFFLL) {
          kind = OP_LOAD_CONST_UINT32;        /* 12 B */
        } else if ((int64_t)(int32_t)value == value) {
          kind = OP_LOAD_CONST_INT32;         /* 16 B (negative int32) */
        } else {
          kind = OP_LOAD_CONST_INT;           /* 20 B (full int64) */
        }
        if (!emit_op(out_prog, kind, reg_index, 0, 0, value)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        reg_type[reg_index] =
            (type == BR_NDB_TYPE_DOUBLE)      ? BR_REG_F64 :
            (type == BR_NDB_TYPE_BIGUNSIGNED) ? BR_REG_U64 :
            (value >= 0)                      ? BR_REG_NNC :
                                                BR_REG_I64;
        /* GL Part B: a BIGUNSIGNED constant is u63-safe iff its top bit
         * is clear (previously the flag went stale here); never a pack. */
        reg_u63_safe[reg_index] =
            (uint8_t)(type == BR_NDB_TYPE_BIGUNSIGNED && value >= 0);
        reg_u64_pack[reg_index] = 0;
        pos += 3;
        break;
      }

      case BR_kOpLoadCol: {
        /* NDB → JIT mapping: kOpLoadCol is too complex to inline as
         * a pure stencil — it must call into NDB to read the column
         * from the row buffer. So we emit OP_LOAD_COL_NDB which is
         * a cold-call stencil; the helper ndb_jit_h_load_col is
         * registered by DbtupJitGlue at engine init.
         *
         * Op layout for OP_LOAD_COL_NDB:
         *   a = dst register slot (4 bits in NDB's bits 19-16)
         *   c = NDB col_id        (16 bits in NDB's bits 15-0)
         *
         * Operand width (RONDB-1056 Test 27): col_id is admitted up to
         * BR_MAX_LOCAL_ATTR_ID (4095), matching NDB's
         * MAX_ATTRIBUTES_IN_TABLE (4096 columns) and the embedded
         * BRANCH_ATTR_*_NULL attr_id path. op->c is uint16_t and the
         * aarch64 narrow MOVZ / x86_64 imm32 operand holes both carry the
         * full 16-bit value, so no engine change is needed — only this
         * admission bound and dropping the former (uint8_t) cast. */
        /* Full 6-bit type decode, mirroring decodeLoadColType (bit 20
         * extends the 5-bit field for DATETIME2/TIMESTAMP2). Neither
         * of those is admitted, but decoding them as their real values
         * keeps the reject reason honest. */
        uint8_t  type      = (uint8_t)(((word >> 21) & 0x1Fu) |
                                       (((word >> 20) & 0x1u) << 5));
        uint8_t  reg_index = (uint8_t)((word >> 16) & 0x0Fu);
        uint16_t col_index = (uint16_t)(word & 0xFFFFu);
        /* Phase 5C-2/5C-3: declared FLOAT/DOUBLE columns load through
         * the f64 cold-call variant (FLOAT promotes to double in the
         * helper, matching the interpreter's floatget path), and
         * declared BIGUNSIGNED columns through the u64 variant. The
         * wire type is the column's DECLARED type, so this admission
         * is static; each helper's own descriptor check is defense in
         * depth (and keeps a schema drift from feeding one contract's
         * bits into another's consumers). */
        int is_f64 =
            (type == BR_NDB_TYPE_DOUBLE || type == BR_NDB_TYPE_FLOAT);
        /* Narrow-int admission (post-5D-3, census probe int_sum): the
         * SIGNED widths sign-extend into the i64 track and the
         * UNSIGNED widths zero-extend into the u64 track — the same
         * split the interpreter makes (IsUnsigned(type) tags the
         * register, and the Sum/Min/Max kernels then pick signed vs
         * unsigned accumulation/compare and set the result's
         * is_unsigned). Narrow-unsigned must NOT ride the i64 track:
         * the interpreter accumulates unsigned registers as u64 (SUM
         * up to 2^64-1 with a u64 overflow check) and marks the
         * result unsigned — a signed SUM would overflow-exit at 2^63
         * and drop the metadata. The load helpers already decode
         * every width; this admission is the only gate. */
        int is_i64 = (type == BR_NDB_TYPE_BIGINT ||
                      type == BR_NDB_TYPE_TINYINT ||
                      type == BR_NDB_TYPE_SMALLINT ||
                      type == BR_NDB_TYPE_MEDIUMINT ||
                      type == BR_NDB_TYPE_INT);
        /* Phase 5K: TEMPORAL columns ride the u64 track — the
         * interpreter loads each type's native packed value as an
         * unsigned integer (DATE/YEAR little-endian, the *2 types
         * big-endian memcmp-ordered bytes), tags the register
         * unsigned BIGINT, and unsigned MIN/MAX over the packed value
         * IS temporal MIN/MAX. The API rejects SUM/AVG over temporals
         * at program build; any other consumer sees exactly the
         * interpreter's register semantics. RonSQL decodes the packed
         * result for display (mysqld never pushes temporals). */
        int is_u64 = (type == BR_NDB_TYPE_BIGUNSIGNED ||
                      type == BR_NDB_TYPE_TINYUNSIGNED ||
                      type == BR_NDB_TYPE_SMALLUNSIGNED ||
                      type == BR_NDB_TYPE_MEDIUMUNSIGNED ||
                      type == BR_NDB_TYPE_UNSIGNED ||
                      type == BR_NDB_TYPE_DATE ||
                      type == BR_NDB_TYPE_YEAR ||
                      type == BR_NDB_TYPE_TIME2 ||
                      type == BR_NDB_TYPE_DATETIME2 ||
                      type == BR_NDB_TYPE_TIMESTAMP2);
        int is_dec = (type == BR_NDB_TYPE_DECIMAL ||
                      type == BR_NDB_TYPE_DECIMALUNSIGNED);
        int is_str = (type == BR_NDB_TYPE_CHAR ||
                      type == BR_NDB_TYPE_VARCHAR ||
                      type == BR_NDB_TYPE_LONGVARCHAR);
        if (!is_i64 && !is_f64 && !is_u64 && !is_dec && !is_str) {
          set_err(out_err, JIT_BRIDGE_NON_BIGINT, this_pos, op);
          return JIT_BRIDGE_NON_BIGINT;
        }
        /* Phase 5F-2: bit 15 marks a LINKED column (join aggregation's
         * parent-table / CTE attribute at position col & 0x7FFF) — the
         * load helpers route it through the JoinAggInterpreter's
         * linked-buffer walk, all type families incl. the fused string
         * MIN/MAX. Local columns keep the MAX_ATTRIBUTES_IN_TABLE
         * bound. */
        if (reg_index >= BC_EMB_REG_BASE ||
            ((col_index & 0x8000u) == 0 &&
             col_index > BR_MAX_LOCAL_ATTR_ID)) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        if (is_str) {
          /* Phase 5F-1: FUSE the string load with its consecutive
           * kOpMin/kOpMax consumers — one OP_MINMAX_STR_NDB per
           * consumer, each covering load + collation compare +
           * winner-buffer update via the interpreter's own kernel
           * (jitMinMaxStringCol). ronsql_jit item 13 adds kOpCount
           * consumers: COUNT reads no bits, only the column's
           * NULL-ness, so the load becomes a PRESENCE-ONLY
           * OP_LOAD_COL_NDB (col_id | NDB_JIT_COL_PRESENCE_FLAG — the
           * helper checks the AttributeHeader and stores 0), followed
           * by the plain OP_COUNT_BIGINT ops; nb_convert_loads then
           * turns it into the null-branching form that skips the
           * COUNTs on NULL, exactly the interpreter's Count null-skip.
           * Emission order is presence load, COUNTs, then the MINMAX
           * ops — the null branch targets the first op after the last
           * reader of the register, and MINMAX (which re-reads the
           * column itself and handles NULL in its kernel) is not a
           * reader, so it must not sit between the load and a COUNT.
           * Any other consumer (or none) keeps the whole-program
           * fallback. The register is poisoned (BR_REG_STR) so nothing
           * else can read it. */
          if ((col_index & NDB_JIT_COL_PRESENCE_FLAG) != 0) {
            set_err(out_err, JIT_BRIDGE_UNSUPPORTED_OP, this_pos, op);
            return JIT_BRIDGE_UNSUPPORTED_OP;
          }
          uint32_t look = pos + 1;
          uint16_t n_count = 0;
          uint16_t n_minmax = 0;
          while (look < n_words) {
            uint32_t cword = ndb_prog[look];
            uint8_t  cop   = (uint8_t)((cword & 0xFC000000u) >> 26);
            if (cop != BR_kOpMin && cop != BR_kOpMax &&
                cop != BR_kOpCount) break;
            uint8_t c_reg = (uint8_t)((cword >> 16) & 0x0Fu);
            if (c_reg != reg_index) break;
            uint16_t c_agg = (uint16_t)(cword & 0xFFFFu);
            if (c_agg >= BC_MAX_ACCS) {
              set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, look, cop);
              return JIT_BRIDGE_REG_OUT_OF_RANGE;
            }
            if (cop == BR_kOpCount) n_count++; else n_minmax++;
            look++;
          }
          if (n_count == 0 && n_minmax == 0) {
            set_err(out_err, JIT_BRIDGE_UNSUPPORTED_OP, this_pos, op);
            return JIT_BRIDGE_UNSUPPORTED_OP;
          }
          uint32_t end = look;
          if (n_count != 0) {
            if (!emit_op(out_prog, OP_LOAD_COL_NDB, reg_index, 0,
                         (uint16_t)(col_index | NDB_JIT_COL_PRESENCE_FLAG),
                         0)) {
              set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
              return JIT_BRIDGE_PROG_TOO_LARGE;
            }
          }
          /* Pass A: the COUNT consumers, right after the presence load. */
          for (uint32_t w = pos + 1; w < end && n_count != 0; w++) {
            uint32_t cword = ndb_prog[w];
            uint8_t  cop   = (uint8_t)((cword & 0xFC000000u) >> 26);
            if (cop != BR_kOpCount) continue;
            uint16_t c_agg = (uint16_t)(cword & 0xFFFFu);
            if (!emit_op(out_prog, OP_COUNT_BIGINT, (uint8_t)c_agg,
                         reg_index, c_agg, 0)) {
              set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, w, cop);
              return JIT_BRIDGE_PROG_TOO_LARGE;
            }
            if (n_outer_map >= BC_MAX_OPS) {
              set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, w, cop);
              return JIT_BRIDGE_PROG_TOO_LARGE;
            }
            outer_word_pos[n_outer_map] = w;
            outer_op_idx[n_outer_map] = (uint16_t)(out_prog->n_ops - 1);
            n_outer_map++;
          }
          /* Pass B: the fused MIN/MAX consumers. */
          for (uint32_t w = pos + 1; w < end && n_minmax != 0; w++) {
            uint32_t cword = ndb_prog[w];
            uint8_t  cop   = (uint8_t)((cword & 0xFC000000u) >> 26);
            if (cop != BR_kOpMin && cop != BR_kOpMax) continue;
            uint16_t c_agg = (uint16_t)(cword & 0xFFFFu);
            BR_CLAIM_ACC_FAMILY(c_agg, BR_ACC_STR);
            uint16_t packed = (uint16_t)(
                ((cop == BR_kOpMax) ? 0x100u : 0u) | c_agg);
            if (!emit_op(out_prog, OP_MINMAX_STR_NDB, (uint8_t)c_agg,
                         col_index, packed, 0)) {
              set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, w, cop);
              return JIT_BRIDGE_PROG_TOO_LARGE;
            }
            /* Map the consumed word so CASE-jump resolution keeps
             * working (a jump landing on the Min word lands on its
             * fused op — the fused op IS the min). */
            if (n_outer_map >= BC_MAX_OPS) {
              set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, w, cop);
              return JIT_BRIDGE_PROG_TOO_LARGE;
            }
            outer_word_pos[n_outer_map] = w;
            outer_op_idx[n_outer_map] = (uint16_t)(out_prog->n_ops - 1);
            n_outer_map++;
          }
          reg_type[reg_index] = BR_REG_STR;
          reg_u63_safe[reg_index] = 0;
          reg_u64_pack[reg_index] = 0;
          pos = end;
          break;
        }
        if (is_dec) {
          /* Phase 5G: DECIMAL loads are TWO wire words — the extra
           * word carries decimal_info = precision << 16 | scale.
           * scale == 0 lands in the BIGINT track (unsigned for
           * DECIMALUNSIGNED), scale > 0 in the DOUBLE track — the
           * interpreter's AlignedType routing, known statically here.
           * The helper gets (is_unsigned << 15) | (precision << 8) |
           * scale packed into op->b (free in the load layout). */
          if (pos + 2 > n_words) {
            set_err(out_err, JIT_BRIDGE_MALFORMED, this_pos, op);
            return JIT_BRIDGE_MALFORMED;
          }
          int32_t dec_info = (int32_t)ndb_prog[pos + 1];
          int32_t precision = dec_info >> 16;
          int32_t scale     = dec_info & 0xFFFF;
          int dec_uns = (type == BR_NDB_TYPE_DECIMALUNSIGNED);
          if (precision < 1 || precision > 65 ||
              scale < 0 || scale > 30 || scale > precision) {
            set_err(out_err, JIT_BRIDGE_MALFORMED, this_pos, op);
            return JIT_BRIDGE_MALFORMED;
          }
          uint16_t pinfo = (uint16_t)((dec_uns ? 0x8000u : 0u) |
                                      ((uint32_t)precision << 8) |
                                      (uint32_t)scale);
          if (!emit_op(out_prog, OP_LOAD_COL_NDB_DEC,
                       reg_index, pinfo, col_index, 0)) {
            set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
            return JIT_BRIDGE_PROG_TOO_LARGE;
          }
          reg_type[reg_index] = (scale != 0) ? BR_REG_F64
                              : dec_uns      ? BR_REG_U64
                                             : BR_REG_I64;
          /* GL Part B: a DECIMALUNSIGNED(scale 0) value is a plain
           * integer, not provably < 2^63, never a pack. */
          reg_u63_safe[reg_index] = 0;
          reg_u64_pack[reg_index] = 0;
          pos += 2;
          break;
        }
        if (!emit_op(out_prog,
                     is_f64 ? OP_LOAD_COL_NDB_F64 :
                     is_u64 ? OP_LOAD_COL_NDB_U64 : OP_LOAD_COL_NDB,
                     reg_index, 0, col_index, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        reg_type[reg_index] = is_f64 ? BR_REG_F64 :
                              is_u64 ? BR_REG_U64 : BR_REG_I64;
        reg_u63_safe[reg_index] = (uint8_t)(is_u64 &&
            (type == BR_NDB_TYPE_TINYUNSIGNED ||
             type == BR_NDB_TYPE_SMALLUNSIGNED ||
             type == BR_NDB_TYPE_MEDIUMUNSIGNED ||
             type == BR_NDB_TYPE_UNSIGNED ||
             type == BR_NDB_TYPE_DATE ||
             type == BR_NDB_TYPE_YEAR ||
             type == BR_NDB_TYPE_TIME2));
        reg_u64_pack[reg_index] = (uint8_t)(is_u64 &&
            (type == BR_NDB_TYPE_DATETIME2 ||
             type == BR_NDB_TYPE_TIMESTAMP2));
        pos += 1;
        break;
      }

      case BR_kOpSetRegNull: {
        /* ronsql_jit slice 2 item 2: the interpreter marks the
         * register SQL-NULL preserving its value type; JIT registers
         * carry no null state, so a row that EXECUTES this op takes
         * the per-row fallback (OP_SET_REG_NULL_FB sets row_fallback
         * and continues; the interpreter re-run applies the exact
         * semantics). GREATEST/LEAST routes only NULL-input rows
         * here via the skip trellis — non-null rows never execute it
         * and stay native. Tracker state: unchanged (the op preserves
         * the value type, and rows that ran it are discarded). */
        uint8_t nreg = (uint8_t)((word >> 16) & 0x0Fu);
        if (nreg >= BC_EMB_REG_BASE) {
          /* Outer registers only (0-7 on the wire). */
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        if (!emit_op(out_prog, OP_SET_REG_NULL_FB, nreg, 0, 0, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        pos += 1;
        break;
      }

      case BR_kOpMov: {
        uint8_t dst = (uint8_t)((word >> 12) & 0x0Fu);
        uint8_t src = (uint8_t)((word >> 8)  & 0x0Fu);
        if (dst >= BC_EMB_REG_BASE || src >= BC_EMB_REG_BASE) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        if (!emit_op(out_prog, OP_MOV_INT_INT, dst, src, 0, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        /* A 64-bit register move is type-preserving (bits are bits). */
        reg_type[dst] = reg_type[src];
        reg_u63_safe[dst] = reg_u63_safe[src];
        reg_u64_pack[dst] = reg_u64_pack[src];
        pos += 1;
        break;
      }

      case BR_kOpDivInt:
      case BR_kOpDivIntBigint:
      case BR_kOpMod: {
        /* Phase 5E-2: integer division / modulo. kOpDivIntBigint is
         * the optimizer's typed rewrite of kOpDivInt (both operands
         * statically BIGINT to it); kOpDivInt itself arrives when an
         * operand was not (DECIMAL loads), and kOpMod is never
         * rewritten. All three lower identically off the tracker's
         * proof. Kernel semantics (RegDivIntBigint / RegModReg
         * integer paths): C truncating division / remainder; divisor
         * 0 → NULL result (the stencil takes the per-row fallback);
         * DIV's INT64_MIN / -1 → overflow exit; DIV's result
         * signedness is a|b, MOD's follows the DIVIDEND only. f64 /
         * STR / UNKNOWN operands (the double trunc-DIV and fmod
         * paths — durable negatives) and u64 mixed with a signed
         * VARIABLE have no lowering → UNSUPPORTED. */
        uint8_t dst = (uint8_t)((word >> 12) & 0x0Fu);
        uint8_t src = (uint8_t)((word >> 8)  & 0x0Fu);
        if (dst >= BC_EMB_REG_BASE || src >= BC_EMB_REG_BASE) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        int dst_int = (reg_type[dst] == BR_REG_I64 ||
                       reg_type[dst] == BR_REG_U64 ||
                       reg_type[dst] == BR_REG_NNC);
        int src_int = (reg_type[src] == BR_REG_I64 ||
                       reg_type[src] == BR_REG_U64 ||
                       reg_type[src] == BR_REG_NNC);
        if (!dst_int || !src_int) {
          /* Phase 5L: a DOUBLE-track operand on the GENERIC forms
           * (kOpDivInt / kOpMod — the optimizer leaves those untyped)
           * lowers to the conversion cold call: trunc-DIV retypes dst
           * into the SIGNED i64 track (the kernel truncates into a
           * signed BIGINT result), fmod stays F64. The TYPED
           * kOpDivIntBigint with an f64 operand is a producer bug —
           * keep rejecting. STR/UNKNOWN keep the fallback. */
          int dst_known = (dst_int || reg_type[dst] == BR_REG_F64);
          int src_known = (src_int || reg_type[src] == BR_REG_F64);
          if (op == BR_kOpDivIntBigint || !dst_known || !src_known) {
            set_err(out_err, JIT_BRIDGE_UNSUPPORTED_OP, this_pos, op);
            return JIT_BRIDGE_UNSUPPORTED_OP;
          }
          uint16_t sel = (op == BR_kOpMod) ? 1u : 0u;
          uint16_t flags =
              (uint16_t)(((reg_type[dst] == BR_REG_F64) ? 0x1u : 0u) |
                         ((reg_type[dst] == BR_REG_U64) ? 0x2u : 0u) |
                         ((reg_type[src] == BR_REG_F64) ? 0x4u : 0u) |
                         ((reg_type[src] == BR_REG_U64) ? 0x8u : 0u));
          uint16_t packed = (uint16_t)((sel << 12) | (flags << 8) |
                                       ((uint16_t)dst << 4) | src);
          if (!emit_op(out_prog, OP_DIVMOD_CONV, dst, src, packed, 0)) {
            set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
            return JIT_BRIDGE_PROG_TOO_LARGE;
          }
          reg_type[dst] = (op == BR_kOpMod) ? BR_REG_F64 : BR_REG_I64;
          pos += 1;
          break;
        }
        int arith_unsigned =
            (reg_type[dst] == BR_REG_U64 || reg_type[src] == BR_REG_U64);
        if (arith_unsigned) {
          int dst_ok = (reg_type[dst] == BR_REG_U64 ||
                        reg_type[dst] == BR_REG_NNC);
          int src_ok = (reg_type[src] == BR_REG_U64 ||
                        reg_type[src] == BR_REG_NNC);
          if (!dst_ok || !src_ok) {
            set_err(out_err, JIT_BRIDGE_UNSUPPORTED_OP, this_pos, op);
            return JIT_BRIDGE_UNSUPPORTED_OP;
          }
        }
        uint8_t our_kind;
        int is_checked = 0;
        if (op == BR_kOpMod) {
          our_kind = arith_unsigned ? OP_MOD_U64 : OP_MOD_INT;
          /* Result signedness follows the DIVIDEND (kernel uses
           * a_unsigned only): a u64 dividend stays u64; an NNC
           * dividend with a u64 divisor yields a small non-negative
           * value — signed track. The unsigned stencil still computes
           * it (urem of the zero-extended magnitudes = the kernel's
           * magnitude dance for non-negative operands). */
          reg_type[dst] = (reg_type[dst] == BR_REG_U64) ? BR_REG_U64
                                                        : BR_REG_I64;
        } else {
          our_kind = arith_unsigned ? OP_DIV_U64 : OP_DIV_INT_CHECKED;
          is_checked = !arith_unsigned;
          reg_type[dst] = arith_unsigned ? BR_REG_U64 : BR_REG_I64;
        }
        if (!emit_op(out_prog, our_kind, dst, dst, src, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        if (is_checked) {
          checked_arith_ops[n_checked_arith_ops++] =
              (uint16_t)(out_prog->n_ops - 1);
        }
        pos += 1;
        break;
      }

      case BR_kOpPlus:
      case BR_kOpMinus:
      case BR_kOpMul: {
        /* Phase 5E-1: GENERIC arithmetic — RonSQL's NdbAggregator::
         * Add/Minus/Mul emit these untyped forms (the mysqld planner
         * emits the typed kOp*Bigint / kOp*Double families below).
         * The generic kernels (RegPlusReg et al.) ARE the typed
         * kernels once operand types are known: uniform-BIGINT
         * operands run the exact signed/unsigned dance the 5C-4
         * classifier models, and double operands run the F64
         * semantics (double op + isfinite → overflow exit). So lower
         * by the TRACKER's proof:
         *   - both proven F64      → the F64 stencils;
         *   - known integer tracks → the 5C-4 classifier → checked
         *     signed / u64 stencils;
         *   - anything else — UNKNOWN (an untyped op over an
         *     unproven register could be int or double), STR, mixed
         *     int/double (the kernel converts inline; the JIT has no
         *     conversion op), or u64 mixed with a signed VARIABLE →
         *     UNSUPPORTED, whole-program fallback. Deliberately not
         *     TYPE_MISMATCH: the kernel handles these shapes — the
         *     LOWERING doesn't. */
        uint8_t dst = (uint8_t)((word >> 12) & 0x0Fu);
        uint8_t src = (uint8_t)((word >> 8)  & 0x0Fu);
        if (dst >= BC_EMB_REG_BASE || src >= BC_EMB_REG_BASE) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        int both_f64 = (reg_type[dst] == BR_REG_F64 &&
                        reg_type[src] == BR_REG_F64);
        int dst_int = (reg_type[dst] == BR_REG_I64 ||
                       reg_type[dst] == BR_REG_U64 ||
                       reg_type[dst] == BR_REG_NNC);
        int src_int = (reg_type[src] == BR_REG_I64 ||
                       reg_type[src] == BR_REG_U64 ||
                       reg_type[src] == BR_REG_NNC);
        uint8_t our_kind;
        if (both_f64) {
          switch (op) {
            case BR_kOpPlus:  our_kind = OP_ADD_F64;   break;
            case BR_kOpMinus: our_kind = OP_MINUS_F64; break;
            default:          our_kind = OP_MUL_F64;   break;
          }
          reg_type[dst] = BR_REG_F64;
        } else if (dst_int && src_int) {
          int arith_unsigned =
              (reg_type[dst] == BR_REG_U64 || reg_type[src] == BR_REG_U64);
          if (arith_unsigned) {
            int dst_ok = (reg_type[dst] == BR_REG_U64 ||
                          reg_type[dst] == BR_REG_NNC);
            int src_ok = (reg_type[src] == BR_REG_U64 ||
                          reg_type[src] == BR_REG_NNC);
            if (!dst_ok || !src_ok) {
              set_err(out_err, JIT_BRIDGE_UNSUPPORTED_OP, this_pos, op);
              return JIT_BRIDGE_UNSUPPORTED_OP;
            }
          }
          switch (op) {
            case BR_kOpPlus:
              our_kind = arith_unsigned ? OP_ADD_U64_CHECKED
                                        : OP_ADD_INT_INT_CHECKED;
              break;
            case BR_kOpMinus:
              our_kind = arith_unsigned ? OP_MINUS_U64_CHECKED
                                        : OP_MINUS_INT_INT_CHECKED;
              break;
            default:
              our_kind = arith_unsigned ? OP_MUL_U64_CHECKED
                                        : OP_MUL_INT_INT_CHECKED;
              break;
          }
          reg_type[dst] = arith_unsigned ? BR_REG_U64 : BR_REG_I64;
        } else if ((dst_int || reg_type[dst] == BR_REG_F64) &&
                   (src_int || reg_type[src] == BR_REG_F64)) {
          /* Phase 5I: MIXED int/double operands — the kernel's double
           * arm converts the integer side with a PLAIN cast (no ±2^53
           * guard, unlike division) and isfinite-checks the result.
           * One conversion cold call mirrors it exactly; errors ride
           * the per-row fallback, so no overflow-target fixup. This
           * closes the TPC-H Q9 shape (NNC const ⊕ scaled DECIMAL). */
          uint16_t sel = (op == BR_kOpPlus)  ? 0u
                       : (op == BR_kOpMinus) ? 1u : 2u;
          uint16_t flags =
              (uint16_t)(((reg_type[dst] == BR_REG_F64) ? 0x1u : 0u) |
                         ((reg_type[dst] == BR_REG_U64) ? 0x2u : 0u) |
                         ((reg_type[src] == BR_REG_F64) ? 0x4u : 0u) |
                         ((reg_type[src] == BR_REG_U64) ? 0x8u : 0u));
          uint16_t packed = (uint16_t)((sel << 12) | (flags << 8) |
                                       ((uint16_t)dst << 4) | src);
          if (!emit_op(out_prog, OP_ARITH_CONV_F64, dst, src, packed,
                       0)) {
            set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
            return JIT_BRIDGE_PROG_TOO_LARGE;
          }
          reg_type[dst] = BR_REG_F64;
          pos += 1;
          break;
        } else {
          set_err(out_err, JIT_BRIDGE_UNSUPPORTED_OP, this_pos, op);
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        if (!emit_op(out_prog, our_kind, dst, dst, src, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        checked_arith_ops[n_checked_arith_ops++] =
            (uint16_t)(out_prog->n_ops - 1);
        pos += 1;
        break;
      }

      case BR_kOpPlusBigint:
      case BR_kOpMinusBigint:
      case BR_kOpMulBigint: {
        /* NDB's 2-operand form: regs[dst] = regs[dst] op regs[src].
         * Our 3-operand form: regs[a] = regs[b] op regs[c].
         * Translate by setting a = dst, b = dst, c = src. */
        uint8_t dst = (uint8_t)((word >> 12) & 0x0Fu);
        uint8_t src = (uint8_t)((word >> 8)  & 0x0Fu);
        if (dst >= BC_EMB_REG_BASE || src >= BC_EMB_REG_BASE) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        /* Phase 5C-4 signed/unsigned classifier. F64 operands always
         * mismatch. If EITHER operand is proven u64, the op lowers
         * UNSIGNED — and then BOTH operands must be u64-compatible
         * (U64 or a non-negative BIGINT constant; the kernels' mixed
         * unsigned/nonneg-signed paths reduce exactly to u64
         * arithmetic). A u64 mixed with a signed VARIABLE keeps the
         * whole-program fallback (the kernel's sign-dependent mixed
         * logic has no lowering). Otherwise the op lowers SIGNED with
         * the pre-5C-4 rules (I64/NNC/UNKNOWN operands). */
        if (reg_type[dst] == BR_REG_F64 || reg_type[src] == BR_REG_F64 ||
            reg_type[dst] == BR_REG_STR || reg_type[src] == BR_REG_STR) {
          set_err(out_err, JIT_BRIDGE_TYPE_MISMATCH, this_pos, op);
          return JIT_BRIDGE_TYPE_MISMATCH;
        }
        int arith_unsigned =
            (reg_type[dst] == BR_REG_U64 || reg_type[src] == BR_REG_U64);
        if (arith_unsigned) {
          int dst_ok = (reg_type[dst] == BR_REG_U64 ||
                        reg_type[dst] == BR_REG_NNC);
          int src_ok = (reg_type[src] == BR_REG_U64 ||
                        reg_type[src] == BR_REG_NNC);
          if (!dst_ok || !src_ok) {
            set_err(out_err, JIT_BRIDGE_TYPE_MISMATCH, this_pos, op);
            return JIT_BRIDGE_TYPE_MISMATCH;
          }
        }
        uint8_t our_kind;
        switch (op) {
          case BR_kOpPlusBigint:
            our_kind = arith_unsigned ? OP_ADD_U64_CHECKED
                                      : OP_ADD_INT_INT_CHECKED;
            break;
          case BR_kOpMinusBigint:
            our_kind = arith_unsigned ? OP_MINUS_U64_CHECKED
                                      : OP_MINUS_INT_INT_CHECKED;
            break;
          default:
            our_kind = arith_unsigned ? OP_MUL_U64_CHECKED
                                      : OP_MUL_INT_INT_CHECKED;
            break;
        }
        if (!emit_op(out_prog, our_kind, dst, dst, src, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        checked_arith_ops[n_checked_arith_ops++] =
            (uint16_t)(out_prog->n_ops - 1);
        reg_type[dst] = arith_unsigned ? BR_REG_U64 : BR_REG_I64;
        pos += 1;
        break;
      }

      case BR_kOpPlusDouble:
      case BR_kOpMinusDouble:
      case BR_kOpMulDouble:
      case BR_kOpDivDouble:
      case BR_kOpDiv: {
        /* Phase 5C-2: double arithmetic — same 2-operand wire form as
         * the Bigint family. Both operands must be PROVEN f64 (the
         * stencils reinterpret the register bits as doubles).
         *
         * kOpDiv is the GENERIC division: the optimizer never rewrites
         * it (it only marks dst DOUBLE), and its kernel converts i64
         * operands to double at runtime. The bridge lowers only the
         * all-double case — conversion ops don't exist yet — and calls
         * anything else UNSUPPORTED (an unimplemented conversion, not
         * a type bug). For the explicitly typed kOp*Double ops a
         * non-f64 operand IS a type bug → TYPE_MISMATCH.
         *
         * All four carry the interpreter kernels' non-finite check →
         * overflow exit (same d-target fixup list as checked int
         * arithmetic). OP_DIV_F64 additionally takes the per-row
         * fallback on divisor == 0 (kernel: result register becomes
         * NULL) — handled inside the stencil, no extra target. */
        uint8_t dst = (uint8_t)((word >> 12) & 0x0Fu);
        uint8_t src = (uint8_t)((word >> 8)  & 0x0Fu);
        if (dst >= BC_EMB_REG_BASE || src >= BC_EMB_REG_BASE) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        if (op == BR_kOpDiv &&
            (reg_type[dst] != BR_REG_F64 || reg_type[src] != BR_REG_F64)) {
          /* Phase 5E-3: generic '/' with integer operand(s) — the
           * conversion cold call (RegDivReg's ±2^53 guards + divide;
           * every edge rides the per-row fallback). Both-proven-F64
           * stays on the hot OP_DIV_F64 path below; STR / UNKNOWN
           * operands keep the whole-program fallback. */
          int dst_known = (reg_type[dst] == BR_REG_I64 ||
                           reg_type[dst] == BR_REG_U64 ||
                           reg_type[dst] == BR_REG_NNC ||
                           reg_type[dst] == BR_REG_F64);
          int src_known = (reg_type[src] == BR_REG_I64 ||
                           reg_type[src] == BR_REG_U64 ||
                           reg_type[src] == BR_REG_NNC ||
                           reg_type[src] == BR_REG_F64);
          if (!dst_known || !src_known) {
            set_err(out_err, JIT_BRIDGE_UNSUPPORTED_OP, this_pos, op);
            return JIT_BRIDGE_UNSUPPORTED_OP;
          }
          uint16_t flags =
              (uint16_t)(((reg_type[dst] == BR_REG_F64) ? 0x1u : 0u) |
                         ((reg_type[dst] == BR_REG_U64) ? 0x2u : 0u) |
                         ((reg_type[src] == BR_REG_F64) ? 0x4u : 0u) |
                         ((reg_type[src] == BR_REG_U64) ? 0x8u : 0u));
          uint16_t packed = (uint16_t)((flags << 8) |
                                       ((uint16_t)dst << 4) | src);
          if (!emit_op(out_prog, OP_DIV_CONV_F64, dst, src, packed, 0)) {
            set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
            return JIT_BRIDGE_PROG_TOO_LARGE;
          }
          reg_type[dst] = BR_REG_F64;
          pos += 1;
          break;
        }
        BR_REQUIRE_F64(dst);
        BR_REQUIRE_F64(src);
        uint8_t our_kind;
        switch (op) {
          case BR_kOpPlusDouble:  our_kind = OP_ADD_F64;   break;
          case BR_kOpMinusDouble: our_kind = OP_MINUS_F64; break;
          case BR_kOpMulDouble:   our_kind = OP_MUL_F64;   break;
          default:                our_kind = OP_DIV_F64;   break;
        }
        if (!emit_op(out_prog, our_kind, dst, dst, src, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        checked_arith_ops[n_checked_arith_ops++] =
            (uint16_t)(out_prog->n_ops - 1);
        reg_type[dst] = BR_REG_F64;
        pos += 1;
        break;
      }

      case BR_kOpSumBigint: {
        /* Phase 5A fix: op->c (the value_updated/value_unsigned mask
         * index) must be the wire agg_index — the interpreter writes
         * agg_res_ptr[agg_index], i.e. agg_index IS the AggResItem
         * index, and the writeback glue pairs acc_i64[i] with result i.
         * The old per-op ordinal only coincided with agg_index for
         * shapes with one aggregate op per result; a multi-arm CASE
         * emits several Sum ops targeting the SAME agg_index and the
         * ordinal mis-marked the mask. */
        uint8_t  reg_index = (uint8_t)((word >> 16) & 0x0Fu);
        uint16_t agg_index = (uint16_t)(word & 0xFFFFu);
        if (reg_index >= BC_EMB_REG_BASE || agg_index >= BC_MAX_ACCS) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        /* Phase 5C-3: the optimizer types BIGUNSIGNED columns into
         * the same BIGINT track (the kernels dispatch on the
         * register's is_unsigned flag at runtime), so kOpSumBigint
         * over a proven-u64 register lowers to the UNSIGNED sum —
         * u64 add with carry check, matching SumBigint's uniform-
         * unsigned path. F64 still rejects. */
        if (reg_type[reg_index] == BR_REG_F64 ||
            reg_type[reg_index] == BR_REG_STR) {
          set_err(out_err, JIT_BRIDGE_TYPE_MISMATCH, this_pos, op);
          return JIT_BRIDGE_TYPE_MISMATCH;
        }
        /* ronsql_jit item 11: a NON-NEGATIVE CONSTANT (NNC) is exact in
         * either integer family, so it ADOPTS the family the slot has
         * already claimed — the CASE shape `THEN unsigned_col ELSE 0`
         * (column arm first, then the constant arm) no longer trips
         * the 5C-3 family conflict. A constant-FIRST order still claims
         * I64 and a later U64 arm rejects (would need a pre-scan; not
         * a corpus shape). */
        int sum_is_u64 = (reg_type[reg_index] == BR_REG_U64) ||
                         (reg_type[reg_index] == BR_REG_NNC &&
                          acc_family[agg_index] == BR_ACC_U64);
        BR_CLAIM_ACC_FAMILY(agg_index,
                            sum_is_u64 ? BR_ACC_U64 : BR_ACC_I64);
        if (!emit_op(out_prog,
                     sum_is_u64 ? OP_SUM_U64_CHECKED
                                : OP_SUM_BIGINT_CHECKED,
                     (uint8_t)agg_index, reg_index, agg_index, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        checked_arith_ops[n_checked_arith_ops++] =
            (uint16_t)(out_prog->n_ops - 1);
        pos += 1;
        break;
      }

      case BR_kOpCount: {
        /* Phase 8 GROUP BY lift: COUNT accumulator. Same wire layout as
         * kOpSumBigint (reg in bits 16..19, acc slot in the low 16).
         * The register operand exists only for the interpreter's
         * null-register skip; the admitted LoadCol contract is non-null
         * columns, so the JIT counts every row that reaches the op —
         * b carries the reg for diagnostics but the stencil ignores it.
         * Unchecked: Int64 row counts cannot realistically overflow. */
        uint8_t  reg_index = (uint8_t)((word >> 16) & 0x0Fu);
        uint16_t agg_index = (uint16_t)(word & 0xFFFFu);
        if (reg_index >= BC_EMB_REG_BASE || agg_index >= BC_MAX_ACCS) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        /* NO type check on the register: the COUNT stencil never reads
         * its bits (acc += 1; the interpreter's null-register skip is
         * covered by the non-null load contract), so any register type
         * — i64, f64, unknown — is fine. AVG(double_col) relies on
         * this: it decomposes into kOpSumDouble + kOpCount over the
         * SAME f64 register. */
        /* c = agg_index (the AggResItem index) — see the kOpSumBigint
         * comment on the Phase 5A mask-index fix. */
        if (!emit_op(out_prog, OP_COUNT_BIGINT,
                     (uint8_t)agg_index, reg_index, agg_index, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        pos += 1;
        break;
      }

      case BR_kOpMinBigint:
      case BR_kOpMaxBigint: {
        /* Phase 5B: MIN/MAX accumulators. Same wire layout as
         * kOpSumBigint (reg in bits 19..16, acc slot in the low 16 —
         * the optimizer's typed rewrite preserves the operand bits).
         * First-row-initialize semantics come from the
         * value_initialized input mask (set per row by the dispatch
         * glue); c = agg_index, the AggResItem index, like every
         * aggregate op. Unchecked — no arithmetic. */
        uint8_t  reg_index = (uint8_t)((word >> 16) & 0x0Fu);
        uint16_t agg_index = (uint16_t)(word & 0xFFFFu);
        if (reg_index >= BC_EMB_REG_BASE || agg_index >= BC_MAX_ACCS) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        /* Phase 5C-3: proven-u64 sources lower to the UNSIGNED
         * variants — a signed compare would order values >= 2^63 as
         * negative. F64 still rejects (kOpMin/MaxDouble is the typed
         * route for doubles). */
        if (reg_type[reg_index] == BR_REG_F64 ||
            reg_type[reg_index] == BR_REG_STR) {
          set_err(out_err, JIT_BRIDGE_TYPE_MISMATCH, this_pos, op);
          return JIT_BRIDGE_TYPE_MISMATCH;
        }
        /* ronsql_jit item 11: NNC adopts an already-claimed U64 slot
         * (see kOpSumBigint) — dd_bigquery's `MAX(CASE WHEN .. THEN
         * biguns_col ELSE 0 END)`. */
        int mm_is_u64 = (reg_type[reg_index] == BR_REG_U64) ||
                        (reg_type[reg_index] == BR_REG_NNC &&
                         acc_family[agg_index] == BR_ACC_U64);
        BR_CLAIM_ACC_FAMILY(agg_index,
                            mm_is_u64 ? BR_ACC_U64 : BR_ACC_I64);
        uint8_t out_kind;
        if (op == BR_kOpMinBigint) {
          out_kind = mm_is_u64 ? OP_MIN_U64 : OP_MIN_BIGINT;
        } else {
          out_kind = mm_is_u64 ? OP_MAX_U64 : OP_MAX_BIGINT;
        }
        if (!emit_op(out_prog, out_kind,
                     (uint8_t)agg_index, reg_index, agg_index, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        pos += 1;
        break;
      }

      case BR_kOpSum:
      case BR_kOpMin:
      case BR_kOpMax: {
        /* Phase 5G: GENERIC aggregates. The optimizer leaves these
         * untyped when the source register's track is not statically
         * BIGINT/DOUBLE to IT — notably DECIMAL loads, whose register
         * becomes runtime-BIGINT (scale 0) or runtime-DOUBLE
         * (scale > 0). The bridge's tracker DOES know the track (the
         * DECIMAL load's wire word carries the scale), so a generic
         * aggregate over a proven register lowers exactly like its
         * typed sibling; UNKNOWN keeps the whole-program fallback.
         * (Generic MIN/MAX over STRING registers never gets here —
         * string loads are not admitted.) */
        uint8_t  reg_index = (uint8_t)((word >> 16) & 0x0Fu);
        uint16_t agg_index = (uint16_t)(word & 0xFFFFu);
        if (reg_index >= BC_EMB_REG_BASE || agg_index >= BC_MAX_ACCS) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        uint8_t out_kind;
        uint8_t fam;
        int     is_checked;
        /* ronsql_jit item 11: NNC adopts an already-claimed U64 slot
         * (see kOpSumBigint). */
        uint8_t eff_track = reg_type[reg_index];
        if (eff_track == BR_REG_NNC &&
            acc_family[agg_index] == BR_ACC_U64) {
          eff_track = BR_REG_U64;
        }
        switch (eff_track) {
          case BR_REG_F64:
            out_kind = (op == BR_kOpSum) ? OP_SUM_F64
                     : (op == BR_kOpMin) ? OP_MIN_F64 : OP_MAX_F64;
            fam = BR_ACC_F64;
            is_checked = (op == BR_kOpSum);
            break;
          case BR_REG_U64:
            out_kind = (op == BR_kOpSum) ? OP_SUM_U64_CHECKED
                     : (op == BR_kOpMin) ? OP_MIN_U64 : OP_MAX_U64;
            fam = BR_ACC_U64;
            is_checked = (op == BR_kOpSum);
            break;
          case BR_REG_I64:
          case BR_REG_NNC:
            out_kind = (op == BR_kOpSum) ? OP_SUM_BIGINT_CHECKED
                     : (op == BR_kOpMin) ? OP_MIN_BIGINT : OP_MAX_BIGINT;
            fam = BR_ACC_I64;
            is_checked = (op == BR_kOpSum);
            break;
          default:
            set_err(out_err, JIT_BRIDGE_UNSUPPORTED_OP, this_pos, op);
            return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        BR_CLAIM_ACC_FAMILY(agg_index, fam);
        if (!emit_op(out_prog, out_kind,
                     (uint8_t)agg_index, reg_index, agg_index, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        if (is_checked) {
          checked_arith_ops[n_checked_arith_ops++] =
              (uint16_t)(out_prog->n_ops - 1);
        }
        pos += 1;
        break;
      }

      case BR_kOpSumDouble:
      case BR_kOpMinDouble:
      case BR_kOpMaxDouble: {
        /* Phase 5C-2: double accumulators. Same wire layout as their
         * Bigint siblings; the source register must be PROVEN f64
         * (these stencils reinterpret its bits as a double). c =
         * agg_index — the AggResItem index (5A mask-index fix). All
         * three mark value_double so the writeback glue produces a
         * DOUBLE result; SUM additionally overflow-exits on a
         * non-finite update, joining the same d-target fixup list. */
        uint8_t  reg_index = (uint8_t)((word >> 16) & 0x0Fu);
        uint16_t agg_index = (uint16_t)(word & 0xFFFFu);
        if (reg_index >= BC_EMB_REG_BASE || agg_index >= BC_MAX_ACCS) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        BR_REQUIRE_F64(reg_index);
        BR_CLAIM_ACC_FAMILY(agg_index, BR_ACC_F64);
        uint8_t out_kind;
        switch (op) {
          case BR_kOpSumDouble: out_kind = OP_SUM_F64; break;
          case BR_kOpMinDouble: out_kind = OP_MIN_F64; break;
          default:              out_kind = OP_MAX_F64; break;
        }
        if (!emit_op(out_prog, out_kind,
                     (uint8_t)agg_index, reg_index, agg_index, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        if (op == BR_kOpSumDouble) {
          checked_arith_ops[n_checked_arith_ops++] =
              (uint16_t)(out_prog->n_ops - 1);
        }
        pos += 1;
        break;
      }

      case BR_kOpSkip: {
        /* Phase 5A: unconditional forward skip over outer words — the
         * planner emits one after each CASE arm to jump past the
         * remaining arms (interpreter: exec_pos += skip_count, plus the
         * loop's own +1 for this word). Lowers to OP_JUMP; the target
         * outer-word position is resolved by the same pass that
         * resolves embedded CASE skip offsets: end-of-program maps to
         * the tail OP_EXIT, a target inside a multi-word instruction is
         * MALFORMED, and backward targets reject. */
        uint32_t skip_count = word & 0xFFFFu;
        uint16_t jump_op_idx = (uint16_t)out_prog->n_ops;
        if (!emit_op(out_prog, OP_JUMP, 0, 0, 0, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        if (n_pending_case_jumps >= BC_MAX_OPS) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        pending_case_jumps[n_pending_case_jumps] = (PendingCaseJump){
          .op_idx = jump_op_idx,
          .target_word_pos = this_pos + 1u + skip_count,
        };
        br_snapshot_outer_tracker(&pending_case_jumps[n_pending_case_jumps],
                                  reg_type, reg_u63_safe, reg_u64_pack);
        n_pending_case_jumps++;
        /* The linear walk past an unconditional jump is DEAD until the
         * next word some pending jump lands on (the merge guard at the
         * loop head adopts that jump's state rather than comparing
         * against this arm's leftovers). */
        linear_dead = 1;
        pos += 1;
        break;
      }

      case BR_kOpEmbeddedInterp: {
        if (!embedded_filters_enabled()) {
          set_err(out_err, JIT_BRIDGE_UNSUPPORTED_OP, this_pos, op);
          return JIT_BRIDGE_UNSUPPORTED_OP;
        }
        /* Phase 5.0: recurse into the embedded block. Header word
         * has emb_len in low 16 bits; emb_len words of NDB normal-
         * interpreter bytecode follow. */
        uint32_t emb_len = word & 0xFFFFu;
        if (pos + 1 + emb_len > n_words) {
          set_err(out_err, JIT_BRIDGE_MALFORMED, this_pos, op);
          return JIT_BRIDGE_MALFORMED;
        }
        uint16_t emb_first_op = out_prog->n_ops;
        JitBridgeReason rc = translate_embedded_block(
            ndb_prog + pos + 1, emb_len, out_prog, out_err, this_pos,
            pos + 1 + emb_len, OP_EXIT, BR_EXIT_OK_FALLTHROUGH,
            /*allow_linked_ops=*/1, /*allow_attr_op_arg=*/1,
            /*attr_op_arg_base=*/this_pos + 1,
            /*allow_reg_ops=*/1,
            pending_case_jumps,
            &n_pending_case_jumps, /*out_exit_refuse_code=*/NULL,
            reg_type, reg_u63_safe, reg_u64_pack);
        if (rc != JIT_BRIDGE_OK) return rc;
        for (uint16_t m = emb_first_op; m < out_prog->n_ops; m++) {
          op_from_emb[m] = 1;
        }
        /* Phase 5C-1 invalidated the whole tracker here; since the
         * ronsql_jit slice-2 register split (outer 0-7, embedded
         * 8-15) an embedded block CANNOT write an outer register, so
         * outer tracking survives the block — which GREATEST/LEAST
         * requires: its trailing outer Mov/aggregates consume
         * registers that were live BEFORE the pair-op's embedded
         * body. (The interpreter's outer register file was never
         * touched by embedded code either — the invalidation was
         * pure conservatism.) */
        pos += 1 + emb_len;
        break;
      }

      /* Everything else — genuinely unknown / future opcodes.
       * (kOpSetRegNull lowered in ronsql_jit slice 2 item 2; the
       * deliberate-fallback canaries now use an agg slot >=
       * BC_MAX_ACCS, which rejects above with REG_OUT_OF_RANGE.)
       * Reject the entire program. */
      default:
        set_err(out_err, JIT_BRIDGE_UNSUPPORTED_OP, this_pos, op);
        return JIT_BRIDGE_UNSUPPORTED_OP;
    }
  }

  /* Implicit program-end → append OP_EXIT. */
  if (!emit_op(out_prog, OP_EXIT, 0, 0, 0, 0)) {
    set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, n_words, 0);
    return JIT_BRIDGE_PROG_TOO_LARGE;
  }
  uint16_t tail_exit_pc = (uint16_t)(out_prog->n_ops - 1);

  for (uint16_t i = 0; i < n_pending_case_jumps; i++) {
    uint32_t target_word = pending_case_jumps[i].target_word_pos;
    uint16_t target_op = UINT16_MAX;
    if (target_word == n_words || target_word == BR_CASE_JUMP_STOP) {
      /* End-of-program, or the STOP_PROGRAM row-disposition sentinel
       * (item 9) — both mean "this row is done": the tail OP_EXIT. */
      target_op = tail_exit_pc;
    } else {
      for (uint16_t m = 0; m < n_outer_map; m++) {
        if (outer_word_pos[m] == target_word) {
          target_op = outer_op_idx[m];
          break;
        }
      }
    }
    if (target_op == UINT16_MAX) {
      set_err(out_err, JIT_BRIDGE_MALFORMED, target_word,
              BR_kOpEmbeddedInterp);
      return JIT_BRIDGE_MALFORMED;
    }
    if (target_op <= pending_case_jumps[i].op_idx) {
      set_err(out_err, JIT_BRIDGE_EMBEDDED_BACKWARD, target_word,
              BR_kOpEmbeddedInterp);
      return JIT_BRIDGE_EMBEDDED_BACKWARD;
    }
    out_prog->ops[pending_case_jumps[i].op_idx].c = target_op;
  }

  if (n_checked_arith_ops != 0) {
    uint16_t overflow_exit_pc = out_prog->n_ops;
    if (!emit_op(out_prog, OP_OVERFLOW_EXIT, 0, 0, 0, 0)) {
      set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, n_words, 0);
      return JIT_BRIDGE_PROG_TOO_LARGE;
    }
    for (uint16_t i = 0; i < n_checked_arith_ops; i++) {
      out_prog->ops[checked_arith_ops[i]].d = overflow_exit_pc;
    }
  }

  /* Phase 5D-1: convert eligible outer loads to null-branching form
   * (runs last — targets are indexes into the final op stream). */
  nb_convert_loads(out_prog, op_from_emb);

  return JIT_BRIDGE_OK;
#undef BR_REQUIRE_F64
#undef BR_CLAIM_ACC_FAMILY
}
