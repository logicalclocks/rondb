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
#define BR_NDB_TYPE_BIGINT  9

/* ------------------------------------------------------------------ */
/* Embedded-interpreter opcodes (mirror of                            */
/* storage/ndb/include/kernel/Interpreter.hpp constants — Phase 5.0   */
/* admits only a small subset).                                       */
/* ------------------------------------------------------------------ */

#define BR_EMB_BRANCH                3   /* unconditional forward jump */
#define BR_EMB_EXIT_OK               5
#define BR_EMB_EXIT_REFUSE           6
#define BR_EMB_EXIT_OK_LAST         22
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
    case BR_EMB_BRANCH:                return "BRANCH";
    case BR_EMB_EXIT_OK:               return "EXIT_OK";
    case BR_EMB_EXIT_REFUSE:           return "EXIT_REFUSE";
    case BR_EMB_EXIT_OK_LAST:          return "EXIT_OK_LAST";
    case BR_EMB_BRANCH_ATTR_EQ_NULL:   return "BRANCH_ATTR_EQ_NULL";
    case BR_EMB_BRANCH_ATTR_NE_NULL:   return "BRANCH_ATTR_NE_NULL";
    case BR_EMB_READ_LINKED_TO_MEM:    return "READ_LINKED_TO_MEM";
    case BR_EMB_BRANCH_LINKED_EQ_NULL: return "BRANCH_LINKED_EQ_NULL";
    case BR_EMB_BRANCH_LINKED_NE_NULL: return "BRANCH_LINKED_NE_NULL";
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
    case OP_LOAD_LINKED_TO_MEM:   return "load_linked_to_mem";
    case OP_BRANCH_LINKED_EQ_NULL:return "branch_linked_eq_null";
    case OP_BRANCH_LINKED_NE_NULL:return "branch_linked_ne_null";
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
             "[RONDB-1056]   op[%u] %-24s kind=%u a=%u b=%u c=%u imm=%lld",
             (unsigned)pc, ndb_jit_bridge_jit_op_name(op->kind),
             (unsigned)op->kind, (unsigned)op->a, (unsigned)op->b,
             (unsigned)op->c, (long long)op->imm);
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
/* Embedded normal-interpreter block translation (Phase 5.0).         */
/*                                                                    */
/* Walks the inner NDB-bytecode words emitted after kOpEmbeddedInterp's*/
/* header word. Phase 5.0 admits a very narrow opcode set:             */
/*                                                                    */
/*   BRANCH_ATTR_EQ_NULL  → OP_BRANCH_ATTR_EQ_NULL                    */
/*   BRANCH_ATTR_NE_NULL  → OP_BRANCH_ATTR_NE_NULL                    */
/*   EXIT_OK / EXIT_OK_LAST → no Op (fall through to outer program)   */
/*   EXIT_REFUSE          → OP_EXIT (early-terminate the JIT'd row)   */
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
/* pc) with the corresponding output Op index.                        */
/* ------------------------------------------------------------------ */

/* Sentinel for `pending_target_emb_pc[]` slots that don't need
 * fixup (i.e., the emitted Op is not a branch from this embedded
 * block). 0xFFFF can never be a real emb_pc because BR_EMB_MAX_LEN
 * is 1024. */
#define BR_EMB_NO_PENDING_FIXUP     0xFFFFu

static JitBridgeReason translate_embedded_block(
    const uint32_t *emb_prog, uint32_t emb_len,
    Program *out_prog, JitBridgeError *out_err,
    uint32_t outer_word_pos /* for error reporting */) {

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

  /* Pass 1: linear walk, emit Ops with target_emb_pc in c. */
  uint32_t emb_pc = 0;
  while (emb_pc < emb_len) {
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
        if (attr_id > 255) {
          /* Phase 5.0 narrow scope — same restriction as
           * op_load_col_ndb. attr_ids ≥ 256 reject the program. */
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
                     (uint8_t)attr_id, /*c=*/0, 0)) {
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

      case BR_EMB_READ_LINKED_TO_MEM: {
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

      case BR_EMB_EXIT_OK:
      case BR_EMB_EXIT_OK_LAST: {
        /* Phase 5.0: EXIT_OK = "row passes the filter" =
         * fall through to outer program's accumulator updates.
         * Emit no Op. The mapping entry stays at 0xFF — but
         * since no later branch can target this pc (we're at a
         * fall-through point), that's fine. */
        emb_pc += 1;
        break;
      }

      case BR_EMB_EXIT_REFUSE: {
        /* Phase 5.0: EXIT_REFUSE = "row rejected" = early-
         * terminate the JIT'd function (skip accumulators).
         * OP_EXIT's stencil is the function-return sequence,
         * so executing it stops row processing immediately. */
        if (!emit_op(out_prog, OP_EXIT, 0, 0, 0, 0)) {
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

      case BR_kOpEmbeddedInterp: {
        /* Phase 5.0: recurse into the embedded block. Header word
         * has emb_len in low 16 bits; emb_len words of NDB normal-
         * interpreter bytecode follow. */
        uint32_t emb_len = word & 0xFFFFu;
        if (pos + 1 + emb_len > n_words) {
          set_err(out_err, JIT_BRIDGE_MALFORMED, this_pos, op);
          return JIT_BRIDGE_MALFORMED;
        }
        JitBridgeReason rc = translate_embedded_block(
            ndb_prog + pos + 1, emb_len, out_prog, out_err, this_pos);
        if (rc != JIT_BRIDGE_OK) return rc;
        pos += 1 + emb_len;
        break;
      }

      /* Everything else — kOpDiv*, kOpMod, all double / max / min /
       * count variants, generic untyped kOpPlus / kOpSum,
       * kOpSetRegNull, kOpSkip — is unsupported. Reject the entire
       * program. */
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
