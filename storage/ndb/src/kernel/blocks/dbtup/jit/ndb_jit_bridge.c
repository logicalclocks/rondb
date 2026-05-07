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
 *                 slots, currently 4)
 *
 * Program-end: implicit when the word stream is exhausted. The
 * bridge appends an OP_EXIT instruction.
 *
 * The bridge does NOT call jit1_compile or touch any JIT engine
 * state. It produces a Program; the caller chains to jit1_compile.
 */

#include "ndb_jit_bridge.h"

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
#define BR_NDB_TYPE_BIGINT  9

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
static inline int emit_op(Program *out, uint8_t kind,
                          uint8_t a, uint8_t b, uint8_t c, int64_t imm) {
  if (out->n_ops >= BC_MAX_OPS) return 0;
  Op *op = &out->ops[out->n_ops++];
  op->kind = kind;
  op->a    = a;
  op->b    = b;
  op->c    = c;
  op->imm  = imm;
  return 1;
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

  uint32_t pos = 0;
  while (pos < n_words) {
    uint32_t word = ndb_prog[pos];
    uint8_t  op   = (uint8_t)((word & 0xFC000000u) >> 26);
    uint32_t this_pos = pos;

    switch (op) {
      case BR_kOpLoadConst: {
        /* 1 instr word + 2 value words = 3 words total. */
        if (pos + 3 > n_words) {
          set_err(out_err, JIT_BRIDGE_MALFORMED, this_pos, op);
          return JIT_BRIDGE_MALFORMED;
        }
        uint8_t type      = (uint8_t)((word >> 21) & 0x1Fu);
        uint8_t reg_index = (uint8_t)((word >> 16) & 0x0Fu);
        if (type != BR_NDB_TYPE_BIGINT) {
          set_err(out_err, JIT_BRIDGE_NON_BIGINT, this_pos, op);
          return JIT_BRIDGE_NON_BIGINT;
        }
        if (reg_index >= BC_MAX_REGS) {
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
        } else {
          /* (Day 4 will insert UINT32 + INT32 variants here.) */
          kind = OP_LOAD_CONST_INT;           /* 20 B fallback */
        }
        if (!emit_op(out_prog, kind, reg_index, 0, 0, value)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
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
         * Phase 4 narrow scope: col_id ≤ 255 (fits in op->c uint8).
         * Larger col_ids would need an extended hole. */
        uint8_t  type      = (uint8_t)((word >> 21) & 0x1Fu);
        uint8_t  reg_index = (uint8_t)((word >> 16) & 0x0Fu);
        uint16_t col_index = (uint16_t)(word & 0xFFFFu);
        if (type != BR_NDB_TYPE_BIGINT) {
          set_err(out_err, JIT_BRIDGE_NON_BIGINT, this_pos, op);
          return JIT_BRIDGE_NON_BIGINT;
        }
        if (reg_index >= BC_MAX_REGS || col_index > 255) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        if (!emit_op(out_prog, OP_LOAD_COL_NDB,
                     reg_index, 0, (uint8_t)col_index, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        pos += 1;
        break;
      }

      case BR_kOpMov: {
        uint8_t dst = (uint8_t)((word >> 12) & 0x0Fu);
        uint8_t src = (uint8_t)((word >> 8)  & 0x0Fu);
        if (dst >= BC_MAX_REGS || src >= BC_MAX_REGS) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        if (!emit_op(out_prog, OP_MOV_INT_INT, dst, src, 0, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
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
        if (dst >= BC_MAX_REGS || src >= BC_MAX_REGS) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        uint8_t our_kind;
        switch (op) {
          case BR_kOpPlusBigint:  our_kind = OP_ADD_INT_INT;   break;
          case BR_kOpMinusBigint: our_kind = OP_MINUS_INT_INT; break;
          default:                our_kind = OP_MUL_INT_INT;   break;
        }
        if (!emit_op(out_prog, our_kind, dst, dst, src, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        pos += 1;
        break;
      }

      case BR_kOpSumBigint: {
        uint8_t  reg_index = (uint8_t)((word >> 16) & 0x0Fu);
        uint16_t agg_index = (uint16_t)(word & 0xFFFFu);
        if (reg_index >= BC_MAX_REGS || agg_index >= BC_MAX_ACCS) {
          set_err(out_err, JIT_BRIDGE_REG_OUT_OF_RANGE, this_pos, op);
          return JIT_BRIDGE_REG_OUT_OF_RANGE;
        }
        if (!emit_op(out_prog, OP_SUM_BIGINT,
                     (uint8_t)agg_index, reg_index, 0, 0)) {
          set_err(out_err, JIT_BRIDGE_PROG_TOO_LARGE, this_pos, op);
          return JIT_BRIDGE_PROG_TOO_LARGE;
        }
        pos += 1;
        break;
      }

      /* Everything else — including kOpEmbeddedInterp, kOpDiv*,
       * kOpMod, all double / max / min / count variants, generic
       * untyped kOpPlus / kOpSum, kOpSetRegNull, kOpSkip — is
       * unsupported in Phase 4. Reject the entire program. */
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

  return JIT_BRIDGE_OK;
}
