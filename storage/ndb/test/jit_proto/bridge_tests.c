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
 */

/*
 * RONDB-1056 Phase 4 Day 1 PM — bridge unit tests.
 *
 * Hand-built NDB-format Uint32 programs exercise
 * ndb_jit_bridge_translate() on accept and reject paths. Each
 * case asserts:
 *
 *   - Expected return value (JIT_BRIDGE_OK / specific reject reason).
 *   - On accept: out_prog->n_ops + key field values.
 *   - On reject: out_err->offending_word + offending_op.
 *
 * Independent of jit1.c — exercises just the bridge.
 */

#define NDB_JIT_BRIDGE_TESTING 1

#include "bytecode1.h"
#include "ndb_jit_bridge.h"

#include <inttypes.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(__GNUC__) && !defined(__clang__)
#define JIT_PROTO_PRINTF_ATTR(fmt_pos, arg_pos) \
  __attribute__((format(gnu_printf, fmt_pos, arg_pos)))
#else
#define JIT_PROTO_PRINTF_ATTR(fmt_pos, arg_pos) \
  __attribute__((format(printf, fmt_pos, arg_pos)))
#endif

/* ------------------------------------------------------------------ */
/* NDB wire-format helpers (mirrored locally — bridge tests have to   */
/* construct programs in the format the bridge expects).              */
/* ------------------------------------------------------------------ */

#define kOpLoadCol        7
#define kOpLoadConst      8
#define kOpMov            9
#define kOpSumBigint     14
#define kOpPlusBigint    20
#define kOpMinusBigint   22
#define kOpMulBigint     24
#define kOpEmbeddedInterp 28
#define kOpDiv            4
#define kOpSetRegNull    30

#define NDB_TYPE_BIGINT  9
#define NDB_TYPE_DOUBLE  18

/* Encode opcode + operands into a single Uint32. */
static uint32_t enc_op(uint32_t op, uint32_t lower) {
  return ((op & 0x3Fu) << 26) | (lower & 0x03FFFFFFu);
}

/* kOpLoadConst: type in bits 25-21, reg_index in bits 19-16. */
static uint32_t enc_load_const(uint32_t type, uint32_t reg_index) {
  return enc_op(kOpLoadConst,
                ((type & 0x1Fu) << 21) | ((reg_index & 0x0Fu) << 16));
}

/* kOpLoadCol: type bits 25-21, reg bits 19-16, col bits 15-0. */
static uint32_t enc_load_col(uint32_t type, uint32_t reg_index,
                              uint32_t col_index) {
  return enc_op(kOpLoadCol,
                ((type & 0x1Fu) << 21) | ((reg_index & 0x0Fu) << 16) |
                (col_index & 0xFFFFu));
}

/* kOpMov / arithmetic: dst bits 15-12, src bits 11-8. */
static uint32_t enc_2reg(uint32_t op, uint32_t dst, uint32_t src) {
  return enc_op(op, ((dst & 0x0Fu) << 12) | ((src & 0x0Fu) << 8));
}

/* kOpSumBigint: reg bits 19-16, agg bits 15-0. */
static uint32_t enc_sum(uint32_t reg_index, uint32_t agg_index) {
  return enc_op(kOpSumBigint,
                ((reg_index & 0x0Fu) << 16) | (agg_index & 0xFFFFu));
}

/* ------------------------------------------------------------------ */
/* Harness.                                                           */
/* ------------------------------------------------------------------ */

static int n_pass = 0;
static int n_fail = 0;

static void mark_pass(const char *name) {
  printf("  PASS  %s\n", name);
  n_pass++;
}
static void mark_fail(const char *name, const char *fmt, ...)
    JIT_PROTO_PRINTF_ATTR(2, 3);
static void mark_fail(const char *name, const char *fmt, ...) {
  printf("  FAIL  %s — ", name);
  va_list ap;
  va_start(ap, fmt);
  vprintf(fmt, ap);
  va_end(ap);
  printf("\n");
  n_fail++;
}

static void assert_accepted(const char *name,
                             const uint32_t *prog, uint32_t n_words,
                             uint16_t expected_n_ops) {
  Program p;
  JitBridgeError err;
  JitBridgeReason r = ndb_jit_bridge_translate(prog, n_words, &p, &err);
  if (r != JIT_BRIDGE_OK) {
    mark_fail(name, "expected OK, got reason=%d (offending_word=%u op=%u)",
              r, err.offending_word, err.offending_op);
    return;
  }
  if (p.n_ops != expected_n_ops) {
    mark_fail(name, "n_ops=%u, want %u", (unsigned)p.n_ops,
              (unsigned)expected_n_ops);
    return;
  }
  mark_pass(name);
}

static void assert_rejected(const char *name,
                             const uint32_t *prog, uint32_t n_words,
                             JitBridgeReason want_reason,
                             uint32_t want_word, uint32_t want_op) {
  Program p;
  JitBridgeError err;
  JitBridgeReason r = ndb_jit_bridge_translate(prog, n_words, &p, &err);
  if (r != want_reason) {
    mark_fail(name, "reason=%d, want %d", r, want_reason);
    return;
  }
  if (want_word != UINT32_MAX && err.offending_word != want_word) {
    mark_fail(name, "offending_word=%u, want %u",
              err.offending_word, want_word);
    return;
  }
  if (want_op != UINT32_MAX && err.offending_op != want_op) {
    mark_fail(name, "offending_op=%u, want %u",
              err.offending_op, want_op);
    return;
  }
  mark_pass(name);
}

static int expect_op_field(const char *name, const Program *p,
                           uint16_t op_idx, const char *field,
                           unsigned got, unsigned want) {
  if (got != want) {
    mark_fail(name, "op[%u].%s=%u, want %u",
              (unsigned)op_idx, field, got, want);
    return 0;
  }
  return 1;
}

static int assert_embedded_accepted(const char *name,
                                    const uint32_t *emb_prog,
                                    uint32_t emb_len,
                                    Program *out_prog,
                                    uint16_t expected_n_ops) {
  JitBridgeError err;
  JitBridgeReason r = ndb_jit_bridge_translate_embedded_for_test(
      emb_prog, emb_len, out_prog, &err, /*outer_word_pos=*/0);
  if (r != JIT_BRIDGE_OK) {
    mark_fail(name, "expected OK, got reason=%d (offending_word=%u op=%u)",
              r, err.offending_word, err.offending_op);
    return 0;
  }
  if (out_prog->n_ops != expected_n_ops) {
    mark_fail(name, "n_ops=%u, want %u", (unsigned)out_prog->n_ops,
              (unsigned)expected_n_ops);
    return 0;
  }
  return 1;
}

static void assert_embedded_rejected(const char *name,
                                     const uint32_t *emb_prog,
                                     uint32_t emb_len,
                                     JitBridgeReason want_reason,
                                     uint32_t want_word,
                                     uint32_t want_op) {
  Program p;
  JitBridgeError err;
  JitBridgeReason r = ndb_jit_bridge_translate_embedded_for_test(
      emb_prog, emb_len, &p, &err, /*outer_word_pos=*/0);
  if (r != want_reason) {
    mark_fail(name, "reason=%d, want %d", r, want_reason);
    return;
  }
  if (want_word != UINT32_MAX && err.offending_word != want_word) {
    mark_fail(name, "offending_word=%u, want %u",
              err.offending_word, want_word);
    return;
  }
  if (want_op != UINT32_MAX && err.offending_op != want_op) {
    mark_fail(name, "offending_op=%u, want %u",
              err.offending_op, want_op);
    return;
  }
  mark_pass(name);
}

/* ------------------------------------------------------------------ */
/* Test cases.                                                        */
/* ------------------------------------------------------------------ */

/* T1: empty program — accept, just OP_EXIT. */
static void test_empty_accept(void) {
  uint32_t prog[1] = {0};
  assert_accepted("T1 empty_accept", prog, 0, /*expected_n_ops=*/1);
}

/* T2: SELECT SUM(c1) shape:
 *   load_col r0, c1
 *   sum_bigint acc[0], r0
 *   (end → exit) */
static void test_simple_sum_accept(void) {
  uint32_t prog[2] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  assert_accepted("T2 simple_sum_accept", prog, 2, /*expected_n_ops=*/3);
}

/* T3: SELECT SUM(c1+c2-c3*c4) shape — full arithmetic battery.
 *   load_col r0, c0
 *   load_col r1, c1
 *   plus r0, r1            (r0 = c0 + c1)
 *   load_col r2, c2
 *   load_col r3, c3
 *   mul r2, r3             (r2 = c2 * c3)
 *   minus r0, r2           (r0 = (c0+c1) - (c2*c3))
 *   sum_bigint acc[0], r0
 *   exit */
static void test_arithmetic_battery_accept(void) {
  uint32_t prog[8] = {
    enc_load_col(NDB_TYPE_BIGINT, 0, 0),
    enc_load_col(NDB_TYPE_BIGINT, 1, 1),
    enc_2reg(kOpPlusBigint, 0, 1),
    enc_load_col(NDB_TYPE_BIGINT, 2, 2),
    enc_load_col(NDB_TYPE_BIGINT, 3, 3),
    enc_2reg(kOpMulBigint, 2, 3),
    enc_2reg(kOpMinusBigint, 0, 2),
    enc_sum(0, 0),
  };
  assert_accepted("T3 arithmetic_battery_accept", prog, 8,
                   /*expected_n_ops=*/9);
}

/* T4: load_const + sum. Verify the 3-word LoadConst layout works. */
static void test_load_const_accept(void) {
  uint32_t prog[5];
  uint32_t pos = 0;
  prog[pos++] = enc_load_const(NDB_TYPE_BIGINT, /*reg=*/0);
  /* 64-bit immediate value 0x1234_5678_9ABC_DEF0 (little-endian
   * across two Uint32 words: low word first). */
  prog[pos++] = 0x9ABCDEF0u;
  prog[pos++] = 0x12345678u;
  prog[pos++] = enc_sum(0, 0);
  /* (program ends; bridge appends exit) */
  Program p;
  JitBridgeError err;
  JitBridgeReason r = ndb_jit_bridge_translate(prog, pos, &p, &err);
  if (r != JIT_BRIDGE_OK) {
    mark_fail("T4 load_const_accept",
              "expected OK, got reason=%d", r);
    return;
  }
  if (p.n_ops != 3) {
    mark_fail("T4 load_const_accept", "n_ops=%u, want 3", (unsigned)p.n_ops);
    return;
  }
  if (p.ops[0].imm != (int64_t)0x123456789ABCDEF0LL) {
    mark_fail("T4 load_const_accept",
              "decoded imm=0x%" PRIx64 ", want 0x123456789ABCDEF0",
              (uint64_t)p.ops[0].imm);
    return;
  }
  mark_pass("T4 load_const_accept");
}

/* T5: kOpLoadCol still has the Phase 5.1a narrow column-id policy.
 * The largest admitted value is 255; larger column ids must fall
 * back until the stencil holes and bridge operands are widened end to
 * end. */
static void test_load_col_255_accept(void) {
  uint32_t prog[2] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/255),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };

  Program p;
  JitBridgeError err;
  JitBridgeReason r = ndb_jit_bridge_translate(prog, 2, &p, &err);
  if (r != JIT_BRIDGE_OK) {
    mark_fail("T5 load_col_255_accept",
              "expected OK, got reason=%d (offending_word=%u op=%u)",
              r, err.offending_word, err.offending_op);
    return;
  }
  if (p.n_ops != 3 ||
      p.ops[0].kind != OP_LOAD_COL_NDB ||
      p.ops[0].c != 255) {
    mark_fail("T5 load_col_255_accept",
              "unexpected translated column op");
    return;
  }
  mark_pass("T5 load_col_255_accept");
}

static void test_load_col_256_reject(void) {
  uint32_t prog[1] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/256),
  };
  assert_rejected("T6 load_col_256_reject", prog, 1,
                   JIT_BRIDGE_REG_OUT_OF_RANGE, 0, kOpLoadCol);
}

/* T7: kOpEmbeddedInterp with an unsupported embedded opcode —
 * must reject. Phase 5.0 admits BRANCH_ATTR_EQ/NE_NULL + EXIT_OK
 * + EXIT_REFUSE only; opcode 0 (kOpUnknown) inside the embedded
 * block must reject the program. */
static void test_embedded_interp_reject(void) {
  /* emb header word + 1 embedded word = unsupported opcode 0. */
  uint32_t prog[2] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/1),
    0u,                                /* embedded opcode 0 — bogus */
  };
  assert_rejected("T7 embedded_interp_reject", prog, 2,
                   JIT_BRIDGE_UNSUPPORTED_OP, /*offending_word=*/0,
                   /*offending_op=*/kOpEmbeddedInterp);
}

/* T8: kOpDiv — must reject (no division support in Phase 4). */
static void test_div_reject(void) {
  uint32_t prog[1] = { enc_2reg(kOpDiv, 0, 1) };
  assert_rejected("T8 div_reject", prog, 1,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpDiv);
}

/* T9: kOpLoadConst with NDB_TYPE_DOUBLE — must reject (non-bigint). */
static void test_load_const_double_reject(void) {
  uint32_t prog[3] = {
    enc_load_const(NDB_TYPE_DOUBLE, 0),
    0x00000000u, 0x40000000u,   /* doesn't matter */
  };
  assert_rejected("T9 load_const_double_reject", prog, 3,
                   JIT_BRIDGE_NON_BIGINT, 0, kOpLoadConst);
}

/* T10: kOpLoadConst truncated — only 1 word where 3 are needed. */
static void test_load_const_truncated_reject(void) {
  uint32_t prog[1] = { enc_load_const(NDB_TYPE_BIGINT, 0) };
  assert_rejected("T10 load_const_truncated_reject", prog, 1,
                   JIT_BRIDGE_MALFORMED, 0, kOpLoadConst);
}

/* T11: register index out of range. */
static void test_reg_oor_reject(void) {
  /* BC_MAX_REGS is 8; reg index 9 is out of range. */
  uint32_t prog[1] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/9, /*col=*/0)
  };
  assert_rejected("T11 reg_oor_reject", prog, 1,
                   JIT_BRIDGE_REG_OUT_OF_RANGE, 0, kOpLoadCol);
}

/* T12: kOpSetRegNull — Phase 4 doesn't admit nullable handling. */
static void test_set_reg_null_reject(void) {
  uint32_t prog[1] = { enc_op(kOpSetRegNull, 0) };
  assert_rejected("T12 set_reg_null_reject", prog, 1,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpSetRegNull);
}

/* ------------------------------------------------------------------ */
/* Phase 5.0 — embedded interpreter call admission.                   */
/* ------------------------------------------------------------------ */

/* NDB normal-interpreter opcodes (mirror of Interpreter.hpp). */
#define EMB_BRANCH_ATTR_EQ_NULL  24
#define EMB_BRANCH_ATTR_NE_NULL  25
#define EMB_EXIT_REFUSE           6
#define EMB_READ_LINKED_TO_MEM   39
#define EMB_BRANCH_LINKED_EQ_NULL 41
#define EMB_BRANCH_LINKED_NE_NULL 42

#define EMB_LOCAL_ATTR_MAX       4095
#define EMB_LINKED_ATTR_FIRST    0x8000
#define EMB_LINKED_ATTR_LAST     0x80ff
#define EMB_PSEUDO_ATTR_FIRST    0xFC00

/* Encode an embedded NDB normal-interpreter instruction.
 * Opcode encoding (different from the aggregation interpreter):
 *   bits 5..0 = opcode low 6 bits
 *   bit 15    = opcode bit 6
 * Branch encoding (BRANCH_ATTR_EQ_NULL etc.):
 *   bit 31         = direction (0=forward)
 *   bits 30..16    = branch_length
 *   word 1 (next): attrId in bits 31..16 */
static uint32_t enc_emb_op_word(uint32_t op, uint32_t lower) {
  return (op & 0x3Fu) | (((op >> 6) & 0x1u) << 15) | lower;
}
static uint32_t enc_emb_branch_attr_null(uint32_t op, uint32_t branch_length) {
  /* Direction bit cleared (forward); branch_length in bits 30..16. */
  return enc_emb_op_word(op, (branch_length & 0x7FFFu) << 16);
}
static uint32_t enc_emb_branch_linked_null(uint32_t op,
                                           uint32_t branch_length) {
  /* Direction bit cleared (forward); branch_length in bits 30..16. */
  return enc_emb_op_word(op, (branch_length & 0x7FFFu) << 16);
}
static uint32_t enc_emb_attr_id(uint32_t attrId) {
  return (attrId & 0xFFFFu) << 16;
}
static uint32_t enc_emb_read_linked_to_mem(uint32_t position) {
  return enc_emb_op_word(EMB_READ_LINKED_TO_MEM,
                         (position & 0xFFu) << 16);
}

/* T13: embedded blocks currently fall back. The JIT per-row ABI has no
 * row-reject signal yet, so compiling EXIT_REFUSE filters would return
 * rows that the SQL layer's pushed_cond recheck rejects. */
static void test_embedded_empty_accept(void) {
  uint32_t prog[1] = { enc_op(kOpEmbeddedInterp, /*emb_len=*/0) };
  assert_rejected("T13 embedded_empty_reject", prog, 1,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpEmbeddedInterp);
}

/* T14: minimal `WHERE col IS NULL` shape:
 *   embedded block:
 *     0: BRANCH_ATTR_NE_NULL +2  (skip to EXIT_REFUSE)
 *     1:   (operand word — attrId)
 *     2: EXIT_REFUSE             (skip row)
 *     -- fall-through after embedded block: row passes
 * Expected: 2 JIT Ops (BRANCH_ATTR_NE_NULL + OP_EXIT) + trailing OP_EXIT
 *           = 3 total. The branch's c field points at the EXIT op. */
static void test_embedded_attr_ne_null_accept(void) {
  uint32_t prog[4] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/3),
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_NE_NULL, /*offset=*/2),
    enc_emb_attr_id(/*attrId=*/1),
    /* EXIT_REFUSE = single-word opcode in the embedded space. */
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_rejected("T14 embedded_attr_ne_null_reject", prog, 4,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpEmbeddedInterp);
}

/* T15: backward branch in embedded block — must reject.
 * Direction bit (bit 31 of word 0) = 1. */
static void test_embedded_backward_reject(void) {
  uint32_t prog[3] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/2),
    /* opcode (low 6 bits + bit 15) | direction<<31 | branch_length<<16. */
    enc_emb_op_word(EMB_BRANCH_ATTR_EQ_NULL,
                    (1u << 31) | (1u << 16)),
    enc_emb_attr_id(/*attrId=*/1),
  };
  assert_rejected("T15 embedded_backward_reject", prog, 3,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpEmbeddedInterp);
}

/* T16/T17: full-program embedded translation is still gated before
 * the embedded BRANCH_ATTR_*_NULL operands are inspected. */
static void test_embedded_attr_gate_reject(void) {
  uint32_t prog[4] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/3),
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_EQ_NULL, /*offset=*/2),
    enc_emb_attr_id(/*attrId=*/EMB_LOCAL_ATTR_MAX),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_rejected("T16 embedded_attr_gate_reject", prog, 4,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpEmbeddedInterp);
}

static void test_embedded_attr_pseudo_gate_reject(void) {
  uint32_t prog[4] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/3),
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_EQ_NULL, /*offset=*/2),
    enc_emb_attr_id(/*attrId=*/EMB_PSEUDO_ATTR_FIRST),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_rejected("T17 embedded_attr_pseudo_gate_reject", prog, 4,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpEmbeddedInterp);
}

/* T18: embedded BRANCH_ATTR_*_NULL must reject linked-flag attr ids
 * under the current local-attribute-only policy. */
static void test_embedded_attr_linked_flag_reject(void) {
  uint32_t prog[4] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/3),
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_EQ_NULL, /*offset=*/2),
    enc_emb_attr_id(/*attrId=*/EMB_LINKED_ATTR_FIRST),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_rejected("T18 embedded_attr_linked_flag_reject", prog, 4,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpEmbeddedInterp);
}

/* T19: embedded block too large — must reject. */
static void test_embedded_too_large_reject(void) {
  /* Allocate 1025 words of embedded body (exceeds BR_EMB_MAX_LEN=1024).
   * The bridge rejects before scanning the body, so word values
   * don't matter. */
  static uint32_t prog[1 + 1025];
  memset(prog, 0, sizeof(prog));
  prog[0] = enc_op(kOpEmbeddedInterp, /*emb_len=*/1025);
  assert_rejected("T19 embedded_too_large_reject",
                   prog, sizeof(prog) / sizeof(prog[0]),
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpEmbeddedInterp);
}

/* T20: Phase 5.1a linked null-check shape:
 *   embedded block:
 *     0: READ_LINKED_TO_MEM position 0
 *     1: BRANCH_LINKED_NE_NULL +1  (skip to EXIT_REFUSE)
 *     2: EXIT_REFUSE               (skip row)
 * Expected: LOAD_LINKED_TO_MEM, BRANCH_LINKED_NE_NULL, OP_EXIT,
 * trailing OP_EXIT. */
static void test_embedded_linked_ne_null_accept(void) {
  uint32_t prog[4] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/3),
    enc_emb_read_linked_to_mem(/*position=*/0),
    enc_emb_branch_linked_null(EMB_BRANCH_LINKED_NE_NULL, /*offset=*/1),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };

  assert_rejected("T20 embedded_linked_ne_null_reject", prog, 4,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpEmbeddedInterp);
}

/* T21: Phase 5.1a linked EQ-null branch accept path. */
static void test_embedded_linked_eq_null_accept(void) {
  uint32_t prog[4] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/3),
    enc_emb_read_linked_to_mem(/*position=*/255),
    enc_emb_branch_linked_null(EMB_BRANCH_LINKED_EQ_NULL, /*offset=*/1),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };

  assert_rejected("T21 embedded_linked_eq_null_reject", prog, 4,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpEmbeddedInterp);
}

/* T22: linked null-check backward branch must reject. */
static void test_embedded_linked_backward_reject(void) {
  uint32_t prog[3] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/2),
    enc_emb_read_linked_to_mem(/*position=*/0),
    enc_emb_op_word(EMB_BRANCH_LINKED_EQ_NULL,
                    (1u << 31) | (1u << 16)),
  };
  assert_rejected("T22 embedded_linked_backward_reject", prog, 3,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpEmbeddedInterp);
}

/* T23: local-attribute branch target outside the embedded block must
 * reject before any JIT admission/patching can see the malformed
 * target. */
static void test_embedded_attr_target_oor_reject(void) {
  uint32_t prog[4] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/3),
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_EQ_NULL, /*offset=*/3),
    enc_emb_attr_id(/*attrId=*/1),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_rejected("T23 embedded_attr_target_oor_reject", prog, 4,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpEmbeddedInterp);
}

/* T24: linked-null branch target outside the embedded block must
 * reject with the same malformed-program reason. */
static void test_embedded_linked_target_oor_reject(void) {
  uint32_t prog[4] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/3),
    enc_emb_read_linked_to_mem(/*position=*/0),
    enc_emb_branch_linked_null(EMB_BRANCH_LINKED_NE_NULL, /*offset=*/2),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_rejected("T24 embedded_linked_target_oor_reject", prog, 4,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpEmbeddedInterp);
}

/* T25: direct embedded translator coverage for the linked NE-null
 * lowering hidden behind the current kOpEmbeddedInterp runtime gate. */
static void test_embedded_direct_linked_ne_null_lowering(void) {
  uint32_t emb_prog[3] = {
    enc_emb_read_linked_to_mem(/*position=*/0),
    enc_emb_branch_linked_null(EMB_BRANCH_LINKED_NE_NULL, /*offset=*/1),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  Program p;
  const char *name = "T25 embedded_direct_linked_ne_null_lowering";
  if (!assert_embedded_accepted(name, emb_prog, 3, &p,
                                /*expected_n_ops=*/3)) return;
  if (!expect_op_field("T25 embedded_direct_linked_ne_null_lowering", &p, 0,
                       "kind", p.ops[0].kind, OP_LOAD_LINKED_TO_MEM)) return;
  if (!expect_op_field("T25 embedded_direct_linked_ne_null_lowering", &p, 0,
                       "b", p.ops[0].b, 0)) return;
  if (!expect_op_field("T25 embedded_direct_linked_ne_null_lowering", &p, 1,
                       "kind", p.ops[1].kind,
                       OP_BRANCH_LINKED_NE_NULL)) return;
  if (!expect_op_field("T25 embedded_direct_linked_ne_null_lowering", &p, 1,
                       "c", p.ops[1].c, 2)) return;
  if (!expect_op_field("T25 embedded_direct_linked_ne_null_lowering", &p, 2,
                       "kind", p.ops[2].kind, OP_EXIT)) return;
  mark_pass(name);
}

/* T26: linked EQ-null lowering preserves the 8-bit linked-buffer
 * position field. This is not a column id; it indexes the linked
 * attribute data buffer staged by the query executor. */
static void test_embedded_direct_linked_eq_null_255_lowering(void) {
  uint32_t emb_prog[3] = {
    enc_emb_read_linked_to_mem(/*position=*/255),
    enc_emb_branch_linked_null(EMB_BRANCH_LINKED_EQ_NULL, /*offset=*/1),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  Program p;
  const char *name = "T26 embedded_direct_linked_eq_null_255_lowering";
  if (!assert_embedded_accepted(name, emb_prog, 3, &p,
                                /*expected_n_ops=*/3)) return;
  if (!expect_op_field("T26 embedded_direct_linked_eq_null_255_lowering", &p,
                       0, "b", p.ops[0].b, 255)) return;
  if (!expect_op_field("T26 embedded_direct_linked_eq_null_255_lowering", &p,
                       1, "kind", p.ops[1].kind,
                       OP_BRANCH_LINKED_EQ_NULL)) return;
  if (!expect_op_field("T26 embedded_direct_linked_eq_null_255_lowering", &p,
                       1, "c", p.ops[1].c, 2)) return;
  mark_pass(name);
}

/* T27/T28: direct tests verify linked branch diagnostics even while
 * full-program embedded translation remains gated off. */
static void test_embedded_direct_linked_backward_reject(void) {
  uint32_t emb_prog[2] = {
    enc_emb_read_linked_to_mem(/*position=*/0),
    enc_emb_op_word(EMB_BRANCH_LINKED_EQ_NULL, (1u << 31) | (1u << 16)),
  };
  assert_embedded_rejected("T27 embedded_direct_linked_backward_reject",
                           emb_prog, 2, JIT_BRIDGE_EMBEDDED_BACKWARD,
                           2, EMB_BRANCH_LINKED_EQ_NULL);
}

static void test_embedded_direct_linked_target_oor_reject(void) {
  uint32_t emb_prog[3] = {
    enc_emb_read_linked_to_mem(/*position=*/0),
    enc_emb_branch_linked_null(EMB_BRANCH_LINKED_NE_NULL, /*offset=*/2),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_embedded_rejected("T28 embedded_direct_linked_target_oor_reject",
                           emb_prog, 3, JIT_BRIDGE_MALFORMED,
                           2, EMB_BRANCH_LINKED_NE_NULL);
}

/* T29/T30: direct local-attribute boundary tests complement T16/T17,
 * which intentionally cover only the top-level embedded gate. */
static void test_embedded_direct_attr_4095_lowering(void) {
  uint32_t emb_prog[3] = {
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_EQ_NULL, /*offset=*/2),
    enc_emb_attr_id(/*attrId=*/EMB_LOCAL_ATTR_MAX),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  Program p;
  const char *name = "T29 embedded_direct_attr_4095_lowering";
  if (!assert_embedded_accepted(name, emb_prog, 3, &p,
                                /*expected_n_ops=*/2)) return;
  if (!expect_op_field("T29 embedded_direct_attr_4095_lowering", &p, 0,
                       "kind", p.ops[0].kind,
                       OP_BRANCH_ATTR_EQ_NULL)) return;
  if (!expect_op_field("T29 embedded_direct_attr_4095_lowering", &p, 0,
                       "b", p.ops[0].b, EMB_LOCAL_ATTR_MAX)) return;
  if (!expect_op_field("T29 embedded_direct_attr_4095_lowering", &p, 0,
                       "c", p.ops[0].c, 1)) return;
  mark_pass(name);
}

static void test_embedded_direct_attr_4096_reject(void) {
  uint32_t emb_prog[3] = {
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_EQ_NULL, /*offset=*/2),
    enc_emb_attr_id(/*attrId=*/EMB_LOCAL_ATTR_MAX + 1),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_embedded_rejected("T30 embedded_direct_attr_4096_reject",
                           emb_prog, 3, JIT_BRIDGE_REG_OUT_OF_RANGE,
                           2, EMB_BRANCH_ATTR_EQ_NULL);
}

static void test_embedded_direct_attr_linked_flag_reject(void) {
  uint32_t emb_prog[3] = {
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_EQ_NULL, /*offset=*/2),
    enc_emb_attr_id(/*attrId=*/EMB_LINKED_ATTR_FIRST),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_embedded_rejected("T31 embedded_direct_attr_linked_flag_reject",
                           emb_prog, 3, JIT_BRIDGE_REG_OUT_OF_RANGE,
                           2, EMB_BRANCH_ATTR_EQ_NULL);
}

static void test_embedded_direct_attr_linked_last_reject(void) {
  uint32_t emb_prog[3] = {
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_EQ_NULL, /*offset=*/2),
    enc_emb_attr_id(/*attrId=*/EMB_LINKED_ATTR_LAST),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_embedded_rejected("T32 embedded_direct_attr_linked_last_reject",
                           emb_prog, 3, JIT_BRIDGE_REG_OUT_OF_RANGE,
                           2, EMB_BRANCH_ATTR_EQ_NULL);
}

static void test_embedded_direct_attr_pseudo_reject(void) {
  uint32_t emb_prog[3] = {
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_EQ_NULL, /*offset=*/2),
    enc_emb_attr_id(/*attrId=*/EMB_PSEUDO_ATTR_FIRST),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_embedded_rejected("T33 embedded_direct_attr_pseudo_reject",
                           emb_prog, 3, JIT_BRIDGE_REG_OUT_OF_RANGE,
                           2, EMB_BRANCH_ATTR_EQ_NULL);
}

static void test_sum_result_indexes_accept(void) {
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/1, /*col=*/1),
    enc_sum(/*reg=*/0, /*agg=*/0),
    enc_sum(/*reg=*/1, /*agg=*/1),
  };

  Program p;
  JitBridgeError err;
  JitBridgeReason r = ndb_jit_bridge_translate(prog, 4, &p, &err);
  if (r != JIT_BRIDGE_OK) {
    mark_fail("T34 sum_result_indexes_accept",
              "expected OK, got reason=%d", r);
    return;
  }
  if (p.n_ops != 5) {
    mark_fail("T34 sum_result_indexes_accept",
              "n_ops=%u, want 5", (unsigned)p.n_ops);
    return;
  }
  if (!expect_op_field("T34 sum_result_indexes_accept", &p, 2,
                       "c", p.ops[2].c, 0)) return;
  if (!expect_op_field("T34 sum_result_indexes_accept", &p, 3,
                       "c", p.ops[3].c, 1)) return;
  mark_pass("T34 sum_result_indexes_accept");
}

int main(void) {
  printf("RONDB-1056 Phase 4 — bridge_tests\n");
  printf("=================================\n");

  test_empty_accept();
  test_simple_sum_accept();
  test_arithmetic_battery_accept();
  test_load_const_accept();
  test_load_col_255_accept();
  test_load_col_256_reject();
  test_embedded_interp_reject();
  test_div_reject();
  test_load_const_double_reject();
  test_load_const_truncated_reject();
  test_reg_oor_reject();
  test_set_reg_null_reject();
  test_embedded_empty_accept();
  test_embedded_attr_ne_null_accept();
  test_embedded_backward_reject();
  test_embedded_attr_gate_reject();
  test_embedded_attr_pseudo_gate_reject();
  test_embedded_attr_linked_flag_reject();
  test_embedded_too_large_reject();
  test_embedded_linked_ne_null_accept();
  test_embedded_linked_eq_null_accept();
  test_embedded_linked_backward_reject();
  test_embedded_attr_target_oor_reject();
  test_embedded_linked_target_oor_reject();
  test_embedded_direct_linked_ne_null_lowering();
  test_embedded_direct_linked_eq_null_255_lowering();
  test_embedded_direct_linked_backward_reject();
  test_embedded_direct_linked_target_oor_reject();
  test_embedded_direct_attr_4095_lowering();
  test_embedded_direct_attr_4096_reject();
  test_embedded_direct_attr_linked_flag_reject();
  test_embedded_direct_attr_linked_last_reject();
  test_embedded_direct_attr_pseudo_reject();
  test_sum_result_indexes_accept();

  printf("\nbridge_tests: %d/%d passed\n", n_pass, n_pass + n_fail);
  return n_fail == 0 ? 0 : 1;
}
