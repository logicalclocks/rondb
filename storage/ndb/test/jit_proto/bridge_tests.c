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

#define kOpPlus           1
#define kOpMinus          2
#define kOpMul            3
#define kOpDivInt         5
#define kOpMod            6
#define kOpDivIntBigint  27
#define kOpLoadCol        7
#define kOpLoadConst      8
#define kOpMov            9
#define kOpSum           10
#define kOpMax           11
#define kOpMin           12
#define kOpCount         13
#define kOpSumBigint     14
#define kOpMaxBigint     16
#define kOpMinBigint     18
#define kOpPlusBigint    20
#define kOpMinusBigint   22
#define kOpMulBigint     24
#define kOpEmbeddedInterp 28
#define kOpDiv            4
#define kOpSkip          29
#define kOpSetRegNull    30
/* Phase 5C-2: the optimizer's DOUBLE-track rewrites. */
#define kOpSumDouble     15
#define kOpMaxDouble     17
#define kOpMinDouble     19
#define kOpPlusDouble    21
#define kOpMinusDouble   23
#define kOpMulDouble     25
#define kOpDivDouble     26

#define NDB_TYPE_TINYINT          1
#define NDB_TYPE_TINYUNSIGNED     2
#define NDB_TYPE_SMALLINT         3
#define NDB_TYPE_SMALLUNSIGNED    4
#define NDB_TYPE_MEDIUMINT        5
#define NDB_TYPE_MEDIUMUNSIGNED   6
#define NDB_TYPE_INT              7
#define NDB_TYPE_UNSIGNED         8
#define NDB_TYPE_BIGINT      9
#define NDB_TYPE_BIGUNSIGNED 10
#define NDB_TYPE_DATE            19
#define NDB_TYPE_YEAR            26
#define NDB_TYPE_TIME2           31
#define NDB_TYPE_DATETIME2       32
#define NDB_TYPE_TIMESTAMP2      33
#define NDB_TYPE_FLOAT       11
#define NDB_TYPE_DOUBLE      12
#define NDB_TYPE_DECIMAL         29
#define NDB_TYPE_DECIMALUNSIGNED 30
#define NDB_TYPE_CHAR            14
#define NDB_TYPE_VARCHAR         15
#define NDB_TYPE_LONGVARCHAR     23

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

/* kOpCount: same wire layout as kOpSumBigint. */
static uint32_t enc_count(uint32_t reg_index, uint32_t agg_index) {
  return enc_op(kOpCount,
                ((reg_index & 0x0Fu) << 16) | (agg_index & 0xFFFFu));
}

/* Generic aggregate encoder (Sum/Min/Max families all share the
 * kOpSumBigint wire layout). */
static uint32_t enc_agg(uint32_t op, uint32_t reg_index,
                        uint32_t agg_index) {
  return enc_op(op, ((reg_index & 0x0Fu) << 16) | (agg_index & 0xFFFFu));
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

static int expect_accepted(const char *name,
                           const uint32_t *prog, uint32_t n_words,
                           Program *out_prog,
                           uint16_t expected_n_ops) {
  JitBridgeError err;
  JitBridgeReason r = ndb_jit_bridge_translate(prog, n_words, out_prog, &err);
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

static int assert_scan_filter_accepted(const char *name,
                                       const uint32_t *filter_prog,
                                       uint32_t n_words,
                                       Program *out_prog,
                                       uint16_t expected_n_ops) {
  JitBridgeError err;
  JitBridgeReason r = ndb_jit_bridge_translate_scan_filter(
      filter_prog, n_words, out_prog, &err, /*out_reject_code=*/NULL);
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

static void assert_scan_filter_rejected(const char *name,
                                        const uint32_t *filter_prog,
                                        uint32_t n_words,
                                        JitBridgeReason want_reason,
                                        uint32_t want_word,
                                        uint32_t want_op) {
  Program p;
  JitBridgeError err;
  JitBridgeReason r = ndb_jit_bridge_translate_scan_filter(
      filter_prog, n_words, &p, &err, /*out_reject_code=*/NULL);
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
  assert_accepted("T2 simple_sum_accept", prog, 2, /*expected_n_ops=*/4);
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
  Program p;
  if (!expect_accepted("T3 arithmetic_battery_accept", prog, 8,
                       &p, /*expected_n_ops=*/10)) return;
  if (!expect_op_field("T3 arithmetic_battery_accept", &p, 2,
                       "kind", p.ops[2].kind,
                       OP_ADD_INT_INT_CHECKED)) return;
  if (!expect_op_field("T3 arithmetic_battery_accept", &p, 5,
                       "kind", p.ops[5].kind,
                       OP_MUL_INT_INT_CHECKED)) return;
  if (!expect_op_field("T3 arithmetic_battery_accept", &p, 6,
                       "kind", p.ops[6].kind,
                       OP_MINUS_INT_INT_CHECKED)) return;
  if (!expect_op_field("T3 arithmetic_battery_accept", &p, 7,
                       "kind", p.ops[7].kind,
                       OP_SUM_BIGINT_CHECKED)) return;
  for (uint16_t pc = 2; pc <= 7; pc++) {
    if (pc == 3 || pc == 4) continue;
    if (!expect_op_field("T3 arithmetic_battery_accept", &p, pc,
                         "d", p.ops[pc].d, 9)) return;
  }
  mark_pass("T3 arithmetic_battery_accept");
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
  if (p.n_ops != 4) {
    mark_fail("T4 load_const_accept", "n_ops=%u, want 4", (unsigned)p.n_ops);
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

/* T5/T6: kOpLoadCol column-id width policy (RONDB-1056 Test 27).
 * The bridge admits col_id 0..BR_MAX_LOCAL_ATTR_ID (4095), matching
 * NDB's MAX_ATTRIBUTES_IN_TABLE (4096 columns); 4096 and above reject.
 * op->c is uint16_t and the operand holes (aarch64 narrow MOVZ /
 * x86_64 imm32) carry the full value, so these compile end to end. */
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
  /* Phase 5D-1: the outer load converts to the null-branching form —
   * col_id rides b, c is the null-branch target (past the SUM). */
  if (p.n_ops != 4 ||
      p.ops[0].kind != OP_LOAD_COL_NDB_NB ||
      p.ops[0].b != 255 || p.ops[0].c != 2) {
    mark_fail("T5 load_col_255_accept",
              "unexpected translated column op");
    return;
  }
  mark_pass("T5 load_col_255_accept");
}

/* T6: col_id=256 now ACCEPTS (was reject under the old 255 cap). */
static void test_load_col_256_accept(void) {
  uint32_t prog[2] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/256),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };

  Program p;
  JitBridgeError err;
  JitBridgeReason r = ndb_jit_bridge_translate(prog, 2, &p, &err);
  if (r != JIT_BRIDGE_OK) {
    mark_fail("T6 load_col_256_accept",
              "expected OK, got reason=%d (offending_word=%u op=%u)",
              r, err.offending_word, err.offending_op);
    return;
  }
  if (p.n_ops != 4 ||
      p.ops[0].kind != OP_LOAD_COL_NDB_NB ||
      p.ops[0].b != 256 || p.ops[0].c != 2) {
    mark_fail("T6 load_col_256_accept",
              "unexpected translated column op (b=%u)", p.ops[0].b);
    return;
  }
  mark_pass("T6 load_col_256_accept");
}

/* T6b: the maximum admitted column id, BR_MAX_LOCAL_ATTR_ID=4095. */
static void test_load_col_4095_accept(void) {
  uint32_t prog[2] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/4095),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };

  Program p;
  JitBridgeError err;
  JitBridgeReason r = ndb_jit_bridge_translate(prog, 2, &p, &err);
  if (r != JIT_BRIDGE_OK) {
    mark_fail("T6b load_col_4095_accept",
              "expected OK, got reason=%d (offending_word=%u op=%u)",
              r, err.offending_word, err.offending_op);
    return;
  }
  if (p.n_ops != 4 ||
      p.ops[0].kind != OP_LOAD_COL_NDB_NB ||
      p.ops[0].b != 4095 || p.ops[0].c != 2) {
    mark_fail("T6b load_col_4095_accept",
              "unexpected translated column op (b=%u)", p.ops[0].b);
    return;
  }
  mark_pass("T6b load_col_4095_accept");
}

/* T6c: one past the maximum (4096) must reject — beyond NDB's
 * MAX_ATTRIBUTES_IN_TABLE. */
static void test_load_col_4096_reject(void) {
  uint32_t prog[1] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/4096),
  };
  assert_rejected("T6c load_col_4096_reject", prog, 1,
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
                   JIT_BRIDGE_UNSUPPORTED_OP, /*offending_word=*/1,
                   /*offending_op=*/0);
}

/* T8: generic kOpDiv over registers NOT proven f64 — must reject
 * (the kernel's runtime i64→double conversion has no JIT lowering;
 * only the all-double case lowers — see T50b). Both regs are UNKNOWN
 * here, so this rejects as an unimplemented conversion, not a type
 * bug. */
static void test_div_reject(void) {
  uint32_t prog[1] = { enc_2reg(kOpDiv, 0, 1) };
  assert_rejected("T8 div_reject", prog, 1,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpDiv);
}

/* T9 (flipped in Phase 5C-2): kOpLoadConst with NDB_TYPE_DOUBLE now
 * ACCEPTS — the two value words carry the double's bit pattern, which
 * rides the normal imm64 path (bits are bits), and the tracker marks
 * the register f64 so it can feed kOpSumDouble.
 * Value 2.0 = 0x4000000000000000. */
static void test_load_const_double_accept(void) {
  const char *name = "T9 load_const_double_accept";
  uint32_t prog[4] = {
    enc_load_const(NDB_TYPE_DOUBLE, 0),
    0x00000000u, 0x40000000u,   /* 2.0, low word first */
    enc_agg(kOpSumDouble, /*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* ops: [0]LOAD_CONST_INT (bit pattern > 2^32) [1]SUM_F64
   *      [2]tail EXIT [3]OVERFLOW_EXIT (SUM_F64 is checked). */
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/4)) return;
  if (p.ops[0].imm != (int64_t)0x4000000000000000LL) {
    mark_fail(name, "decoded imm=0x%" PRIx64 ", want 0x4000000000000000",
              (uint64_t)p.ops[0].imm);
    return;
  }
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind, OP_SUM_F64))
    return;
  if (!expect_op_field(name, &p, 1, "d", p.ops[1].d, 3)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_OVERFLOW_EXIT)) return;
  mark_pass(name);
}

/* T9b: kOpLoadConst with a type that is neither BIGINT nor DOUBLE —
 * must reject. 18 = NDB_TYPE_VARCHAR territory; any non-numeric-const
 * type takes this path. */
static void test_load_const_unsupported_type_reject(void) {
  uint32_t prog[3] = {
    enc_load_const(/*type=*/18, 0),
    0x00000000u, 0x40000000u,   /* doesn't matter */
  };
  assert_rejected("T9b load_const_unsupported_type_reject", prog, 3,
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
#define EMB_BRANCH_ATTR_OP_ARG   23   /* Phase 7 comparison branch */
#define EMB_BRANCH_ATTR_OP_PARAM 26   /* Phase 7 col-vs-param branch */
#define EMB_BRANCH_ATTR_OP_ATTR  27   /* Phase 7 col-vs-col branch */
#define EMB_EXIT_REFUSE          19
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
/* Accept-path opcodes (row-disposition model). */
#define EMB_EXIT_OK              18
#define EMB_EXIT_OK_LAST         22
#define EMB_LOAD_CONST16          4
#define EMB_WRITE_INTERP_OUTPUT 123
/* Phase 5A embedded reg-ops (the SQL planner's CASE condition family). */
#define EMB_READ_ATTR             1
#define EMB_LOAD_CONST64          6
#define EMB_BRANCH_EQ_REG_REG    12
#define EMB_BRANCH_NE_REG_REG    13
#define EMB_BRANCH_GT_REG_REG    16

static uint32_t enc_emb_read_attr(uint32_t attr_id, uint32_t reg) {
  return (attr_id << 16) | ((reg & 0x7u) << 6) | EMB_READ_ATTR;
}
static uint32_t enc_emb_load_const64_hdr(uint32_t reg) {
  return ((reg & 0x7u) << 6) | EMB_LOAD_CONST64;
}
/* left_reg bits 6..8, right_reg bits 9..11, forward offset bits 30..16
 * (mirrors ha_ndbcluster_push_agg.cc's emission). */
static uint32_t enc_emb_branch_reg_reg(uint32_t op, uint32_t left,
                                       uint32_t right, uint32_t offset) {
  return op | ((left & 0x7u) << 6) | ((right & 0x7u) << 9) |
         ((offset & 0x7FFFu) << 16);
}
static uint32_t enc_emb_load_const16(uint32_t reg, uint32_t val) {
  return EMB_LOAD_CONST16 | ((reg & 0x7u) << 6) | ((val & 0xFFFFu) << 16);
}
static uint32_t enc_emb_write_output(uint32_t reg, uint32_t out_idx) {
  /* WRITE_INTERPRETER_OUTPUT = LOAD_CONST_MEM(59) | (1<<15) → opcode 123. */
  return 59u | (1u << 15) | ((reg & 0x7u) << 6) | ((out_idx & 0xFFFFu) << 16);
}

/* T13: empty embedded block is a no-op. */
static void test_embedded_empty_accept(void) {
  uint32_t prog[1] = { enc_op(kOpEmbeddedInterp, /*emb_len=*/0) };
  assert_accepted("T13 embedded_empty_accept", prog, 1,
                  /*expected_n_ops=*/1);
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
  Program p;
  if (!expect_accepted("T14 embedded_attr_ne_null_accept", prog, 4,
                       &p, /*expected_n_ops=*/3)) return;
  if (!expect_op_field("T14 embedded_attr_ne_null_accept", &p, 0,
                       "kind", p.ops[0].kind,
                       OP_BRANCH_ATTR_NE_NULL)) return;
  if (!expect_op_field("T14 embedded_attr_ne_null_accept", &p, 0,
                       "c", p.ops[0].c, 1)) return;
  mark_pass("T14 embedded_attr_ne_null_accept");
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
                   JIT_BRIDGE_EMBEDDED_BACKWARD, 1,
                   EMB_BRANCH_ATTR_EQ_NULL);
}

/* T16: largest local attr id is accepted. */
static void test_embedded_attr_max_accept(void) {
  uint32_t prog[4] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/3),
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_EQ_NULL, /*offset=*/2),
    enc_emb_attr_id(/*attrId=*/EMB_LOCAL_ATTR_MAX),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  Program p;
  if (!expect_accepted("T16 embedded_attr_max_accept", prog, 4,
                       &p, /*expected_n_ops=*/3)) return;
  if (!expect_op_field("T16 embedded_attr_max_accept", &p, 0,
                       "b", p.ops[0].b, EMB_LOCAL_ATTR_MAX)) return;
  mark_pass("T16 embedded_attr_max_accept");
}

static void test_embedded_attr_pseudo_reject(void) {
  uint32_t prog[4] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/3),
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_EQ_NULL, /*offset=*/2),
    enc_emb_attr_id(/*attrId=*/EMB_PSEUDO_ATTR_FIRST),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_rejected("T17 embedded_attr_pseudo_reject", prog, 4,
                   JIT_BRIDGE_REG_OUT_OF_RANGE, 2,
                   EMB_BRANCH_ATTR_EQ_NULL);
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
                   JIT_BRIDGE_REG_OUT_OF_RANGE, 2,
                   EMB_BRANCH_ATTR_EQ_NULL);
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
                   JIT_BRIDGE_EMBEDDED_TOO_LARGE, 0,
                   kOpEmbeddedInterp);
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

  Program p;
  if (!expect_accepted("T20 embedded_linked_ne_null_accept", prog, 4,
                       &p, /*expected_n_ops=*/4)) return;
  if (!expect_op_field("T20 embedded_linked_ne_null_accept", &p, 0,
                       "kind", p.ops[0].kind,
                       OP_LOAD_LINKED_TO_MEM)) return;
  if (!expect_op_field("T20 embedded_linked_ne_null_accept", &p, 1,
                       "kind", p.ops[1].kind,
                       OP_BRANCH_LINKED_NE_NULL)) return;
  if (!expect_op_field("T20 embedded_linked_ne_null_accept", &p, 1,
                       "c", p.ops[1].c, 2)) return;
  mark_pass("T20 embedded_linked_ne_null_accept");
}

/* T21: Phase 5.1a linked EQ-null branch accept path. */
static void test_embedded_linked_eq_null_accept(void) {
  uint32_t prog[4] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/3),
    enc_emb_read_linked_to_mem(/*position=*/255),
    enc_emb_branch_linked_null(EMB_BRANCH_LINKED_EQ_NULL, /*offset=*/1),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };

  Program p;
  if (!expect_accepted("T21 embedded_linked_eq_null_accept", prog, 4,
                       &p, /*expected_n_ops=*/4)) return;
  if (!expect_op_field("T21 embedded_linked_eq_null_accept", &p, 0,
                       "b", p.ops[0].b, 255)) return;
  if (!expect_op_field("T21 embedded_linked_eq_null_accept", &p, 1,
                       "kind", p.ops[1].kind,
                       OP_BRANCH_LINKED_EQ_NULL)) return;
  mark_pass("T21 embedded_linked_eq_null_accept");
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
                   JIT_BRIDGE_EMBEDDED_BACKWARD, 2,
                   EMB_BRANCH_LINKED_EQ_NULL);
}

/* T22b: Phase 5.1 linked filter with an explicit ACCEPT path (the full
 * row-disposition model). The non-NULL branch lands on the accept
 * sequence (LOAD_CONST16 0 + WRITE_INTERPRETER_OUTPUT 0 + EXIT_OK).
 * Phase 5A: LOAD_CONST16 now ALSO materializes its register (a
 * following REG_REG compare may read it), so the accept branch
 * resolves to that load and falls through to the outer LoadCol.
 *   embedded(emb_len=6):
 *     0: READ_LINKED_TO_MEM 0
 *     1: BRANCH_LINKED_NE_NULL +2   (non-NULL -> accept @3)
 *     2: EXIT_REFUSE 626            (NULL -> skip)
 *     3: LOAD_CONST16 r2, 0
 *     4: WRITE_INTERPRETER_OUTPUT r2, 0
 *     5: EXIT_OK
 *   outer: kOpLoadCol, kOpSumBigint
 * Expected Ops: [0]LOAD_LINKED_TO_MEM [1]BRANCH_LINKED_NE_NULL(c=3)
 *   [2]OP_EXIT(reject) [3]OP_LOAD_CONST_UINT16
 *   [4]OP_JUMP(c=5 — the zero-skip disposition jump to the outer
 *   continuation; required so mid-stream output blocks can't fall
 *   through into a following block) [5]OP_LOAD_COL_NDB
 *   [6]OP_SUM_BIGINT_CHECKED [7]OP_EXIT(tail) [8]OP_OVERFLOW_EXIT. */
static void test_embedded_linked_accept_path(void) {
  uint32_t prog[9] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/6),
    enc_emb_read_linked_to_mem(/*position=*/0),
    enc_emb_branch_linked_null(EMB_BRANCH_LINKED_NE_NULL, /*offset=*/2),
    enc_emb_op_word(EMB_EXIT_REFUSE, (626u << 16)),
    enc_emb_load_const16(/*reg=*/2, /*val=*/0),
    enc_emb_write_output(/*reg=*/2, /*out_idx=*/0),
    enc_emb_op_word(EMB_EXIT_OK, 0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };

  Program p;
  if (!expect_accepted("T22b embedded_linked_accept_path", prog, 9,
                       &p, /*expected_n_ops=*/9)) return;
  if (!expect_op_field("T22b embedded_linked_accept_path", &p, 1,
                       "kind", p.ops[1].kind,
                       OP_BRANCH_LINKED_NE_NULL)) return;
  /* Accept branch resolves to the materialized LOAD_CONST16 at op 3. */
  if (!expect_op_field("T22b embedded_linked_accept_path", &p, 1,
                       "c", p.ops[1].c, 3)) return;
  if (!expect_op_field("T22b embedded_linked_accept_path", &p, 2,
                       "kind", p.ops[2].kind, OP_EXIT)) return;
  if (!expect_op_field("T22b embedded_linked_accept_path", &p, 3,
                       "kind", p.ops[3].kind, OP_LOAD_CONST_UINT16)) return;
  if (!expect_op_field("T22b embedded_linked_accept_path", &p, 4,
                       "kind", p.ops[4].kind, OP_JUMP)) return;
  if (!expect_op_field("T22b embedded_linked_accept_path", &p, 4,
                       "c", p.ops[4].c, 5)) return;
  /* Phase 5D-1: the outer load converts to the null-branching form. */
  if (!expect_op_field("T22b embedded_linked_accept_path", &p, 5,
                       "kind", p.ops[5].kind, OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field("T22b embedded_linked_accept_path", &p, 5,
                       "c", p.ops[5].c, 7)) return;
  if (!expect_op_field("T22b embedded_linked_accept_path", &p, 6,
                       "kind", p.ops[6].kind, OP_SUM_BIGINT_CHECKED)) return;
  if (!expect_op_field("T22b embedded_linked_accept_path", &p, 6,
                       "d", p.ops[6].d, 8)) return;
  mark_pass("T22b embedded_linked_accept_path");
}

/* T22c: CASE-style non-zero skip_offset accepts by emitting OP_JUMP. The
 * accept arm below skips the first outer LoadCol/SUM pair and lands on the
 * second pair.
 *   embedded(emb_len=6):
 *     0: READ_LINKED_TO_MEM 0
 *     1: BRANCH_LINKED_NE_NULL +2   (non-NULL -> accept @3)
 *     2: EXIT_REFUSE 626            (NULL -> skip)
 *     3: LOAD_CONST16 r2, 2         (skip 2 outer words)
 *     4: WRITE_INTERPRETER_OUTPUT r2, 0
 *     5: EXIT_OK
 *   outer: kOpLoadCol(0), kOpSum(0), kOpLoadCol(1), kOpSum(1)
 * Phase 5A: LOAD_CONST16 materializes its register too, so the op list
 * gains one load before the OP_JUMP and the jump target shifts.
 * Expected jump target: outer word pos 9 -> JIT op index 7. */
static void test_embedded_case_skip_offset_accept(void) {
  uint32_t prog[11] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/6),
    enc_emb_read_linked_to_mem(/*position=*/0),
    enc_emb_branch_linked_null(EMB_BRANCH_LINKED_NE_NULL, /*offset=*/2),
    enc_emb_op_word(EMB_EXIT_REFUSE, (626u << 16)),
    enc_emb_load_const16(/*reg=*/2, /*val=*/2),
    enc_emb_write_output(/*reg=*/2, /*out_idx=*/0),
    enc_emb_op_word(EMB_EXIT_OK, 0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_sum(/*reg=*/0, /*agg=*/0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/1, /*col=*/1),
    enc_sum(/*reg=*/1, /*agg=*/1),
  };

  Program p;
  if (!expect_accepted("T22c embedded_case_skip_offset_accept", prog, 11,
                       &p, /*expected_n_ops=*/11)) return;
  if (!expect_op_field("T22c embedded_case_skip_offset_accept", &p, 1,
                       "c", p.ops[1].c, 3)) return;
  if (!expect_op_field("T22c embedded_case_skip_offset_accept", &p, 3,
                       "kind", p.ops[3].kind, OP_LOAD_CONST_UINT16)) return;
  if (!expect_op_field("T22c embedded_case_skip_offset_accept", &p, 4,
                       "kind", p.ops[4].kind, OP_JUMP)) return;
  if (!expect_op_field("T22c embedded_case_skip_offset_accept", &p, 4,
                       "c", p.ops[4].c, 7)) return;
  if (!expect_op_field("T22c embedded_case_skip_offset_accept", &p, 5,
                       "kind", p.ops[5].kind, OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field("T22c embedded_case_skip_offset_accept", &p, 5,
                       "c", p.ops[5].c, 7)) return;
  if (!expect_op_field("T22c embedded_case_skip_offset_accept", &p, 6,
                       "kind", p.ops[6].kind, OP_SUM_BIGINT_CHECKED)) return;
  if (!expect_op_field("T22c embedded_case_skip_offset_accept", &p, 7,
                       "kind", p.ops[7].kind, OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field("T22c embedded_case_skip_offset_accept", &p, 7,
                       "c", p.ops[7].c, 9)) return;
  if (!expect_op_field("T22c embedded_case_skip_offset_accept", &p, 8,
                       "kind", p.ops[8].kind, OP_SUM_BIGINT_CHECKED)) return;
  if (!expect_op_field("T22c embedded_case_skip_offset_accept", &p, 6,
                       "d", p.ops[6].d, 10)) return;
  if (!expect_op_field("T22c embedded_case_skip_offset_accept", &p, 8,
                       "d", p.ops[8].d, 10)) return;
  mark_pass("T22c embedded_case_skip_offset_accept");
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
                   JIT_BRIDGE_MALFORMED, 1,
                   EMB_BRANCH_ATTR_EQ_NULL);
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
                   JIT_BRIDGE_MALFORMED, 2,
                   EMB_BRANCH_LINKED_NE_NULL);
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
  if (p.n_ops != 6) {
    mark_fail("T34 sum_result_indexes_accept",
              "n_ops=%u, want 6", (unsigned)p.n_ops);
    return;
  }
  if (!expect_op_field("T34 sum_result_indexes_accept", &p, 2,
                       "c", p.ops[2].c, 0)) return;
  if (!expect_op_field("T34 sum_result_indexes_accept", &p, 3,
                       "c", p.ops[3].c, 1)) return;
  if (!expect_op_field("T34 sum_result_indexes_accept", &p, 2,
                       "d", p.ops[2].d, 5)) return;
  if (!expect_op_field("T34 sum_result_indexes_accept", &p, 3,
                       "d", p.ops[3].d, 5)) return;
  mark_pass("T34 sum_result_indexes_accept");
}

/* T35: Phase 7 scan-filter bridge accepts the same local attr-null
 * shape as embedded aggregation filters and appends a fall-through
 * OP_EXIT for accepted rows. EXIT_REFUSE lowers to OP_FILTER_REJECT_EXIT
 * so runtime callers can distinguish rejected rows from accepted rows. */
static void test_scan_filter_attr_ne_null_accept(void) {
  uint32_t filter_prog[3] = {
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_NE_NULL, /*offset=*/2),
    enc_emb_attr_id(/*attrId=*/1),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  Program p;
  const char *name = "T35 scan_filter_attr_ne_null_accept";
  if (!assert_scan_filter_accepted(name, filter_prog, 3, &p,
                                   /*expected_n_ops=*/3)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_BRANCH_ATTR_NE_NULL)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 1)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_FILTER_REJECT_EXIT)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_EXIT)) return;
  mark_pass(name);
}

/* T36: linked-attr ops are REJECTED on the standalone scan-filter path.
 * They depend on the join-aggregation linked buffer (the cold-call
 * helpers reach it via JitState.ctx->join_agg), which a scan filter has
 * no instance of. Linked attrs only arise from pushdown joins (JOIN_AGG),
 * never plain SCAN_FRAGREQ filters, so rejecting them at translate time is
 * a safety guard against compiling a program whose helper would
 * dereference a null context at runtime. READ_LINKED_TO_MEM is the first
 * op walked, so it is the one reported. (The aggregation-embedded path
 * still admits these — see T20/T21.) */
static void test_scan_filter_linked_reject(void) {
  uint32_t filter_prog[3] = {
    enc_emb_read_linked_to_mem(/*position=*/7),
    enc_emb_branch_linked_null(EMB_BRANCH_LINKED_NE_NULL, /*offset=*/1),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_scan_filter_rejected("T36 scan_filter_linked_reject",
                              filter_prog, 3,
                              JIT_BRIDGE_UNSUPPORTED_OP,
                              /*want_word=*/UINT32_MAX,
                              EMB_READ_LINKED_TO_MEM);
}

/* T37: WRITE_INTERPRETER_OUTPUT skip-offset is aggregation-embedded
 * row-disposition machinery. Standalone scan filters have no outer
 * aggregation word stream, so the Phase 7 bridge rejects it for now. */
static void test_scan_filter_write_output_reject(void) {
  uint32_t filter_prog[3] = {
    enc_emb_load_const16(/*reg=*/2, /*val=*/2),
    enc_emb_write_output(/*reg=*/2, /*out_idx=*/0),
    enc_emb_op_word(EMB_EXIT_OK, 0),
  };
  assert_scan_filter_rejected("T37 scan_filter_write_output_reject",
                              filter_prog, 3,
                              JIT_BRIDGE_UNSUPPORTED_OP,
                              0, EMB_WRITE_INTERP_OUTPUT);
}

/* T38: scan filter with an explicit EXIT_OK *before* the EXIT_REFUSE it
 * guards — the real NdbScanFilter shape for `col IS NOT NULL`:
 *
 *     0: BRANCH_ATTR_EQ_NULL col -> 3   (if NULL, jump to the refuse)
 *     2: EXIT_OK                        (not NULL -> accept)
 *     3: EXIT_REFUSE                    (NULL    -> reject)
 *
 * Regression test for the all-rows-rejected bug: a standalone scan
 * filter must lower EXIT_OK to OP_EXIT (the accept terminator), NOT to
 * nothing. If EXIT_OK is elided (the aggregation-embedded behaviour), a
 * fall-through accept runs straight into the following
 * OP_FILTER_REJECT_EXIT and every accepted row is wrongly rejected.
 * Expected lowering (4 ops):
 *     op0 OP_BRANCH_ATTR_EQ_NULL (target -> op2, the reject)
 *     op1 OP_EXIT                (the EXIT_OK accept)
 *     op2 OP_FILTER_REJECT_EXIT  (the EXIT_REFUSE)
 *     op3 OP_EXIT                (trailing accept terminator)
 * Before the fix this was 3 ops with op1 == OP_FILTER_REJECT_EXIT. */
static void test_scan_filter_exit_ok_before_refuse_accept(void) {
  uint32_t filter_prog[4] = {
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_EQ_NULL, /*offset=*/3),
    enc_emb_attr_id(/*attrId=*/1),
    enc_emb_op_word(EMB_EXIT_OK, 0),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  Program p;
  const char *name = "T38 scan_filter_exit_ok_before_refuse_accept";
  if (!assert_scan_filter_accepted(name, filter_prog, 4, &p,
                                   /*expected_n_ops=*/4)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_BRANCH_ATTR_EQ_NULL)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_EXIT)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_FILTER_REJECT_EXIT)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_EXIT)) return;
  mark_pass(name);
}

/* T39: the scan-filter bridge captures the program's EXIT_REFUSE code
 * (theInstruction >> 16) into out_reject_code, so the runtime can
 * TUPKEY_abort with the program's actual code instead of a hardcoded one.
 * Same IS-NOT-NULL shape as T38 but with refuse code 899. */
static void test_scan_filter_captures_refuse_code(void) {
  const char *name = "T39 scan_filter_captures_refuse_code";
  uint32_t filter_prog[4] = {
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_EQ_NULL, /*offset=*/3),
    enc_emb_attr_id(/*attrId=*/1),
    enc_emb_op_word(EMB_EXIT_OK, 0),
    enc_emb_op_word(EMB_EXIT_REFUSE, 899u << 16),
  };
  Program p;
  JitBridgeError err;
  uint32_t reject_code = 0;
  JitBridgeReason r = ndb_jit_bridge_translate_scan_filter(
      filter_prog, 4, &p, &err, &reject_code);
  if (r != JIT_BRIDGE_OK) {
    mark_fail(name, "expected OK, got reason=%d", r);
    return;
  }
  if (reject_code != 899u) {
    mark_fail(name, "reject_code=%u, want 899", (unsigned)reject_code);
    return;
  }
  mark_pass(name);
}

/* T40: a program whose EXIT_REFUSE words carry differing codes can't be
 * represented by a single per-program reject code, so the scan-filter
 * bridge rejects it (it falls back to the interpreter). Shape:
 *     0: BRANCH_ATTR_NE_NULL col -> 3   (not NULL -> reject with 626)
 *     2: EXIT_REFUSE 899                (NULL     -> reject with 899)
 *     3: EXIT_REFUSE 626
 * The walk captures 899 at pc 2, then sees 626 at pc 3 -> mismatch. */
static void test_scan_filter_mixed_refuse_codes_reject(void) {
  uint32_t filter_prog[4] = {
    enc_emb_branch_attr_null(EMB_BRANCH_ATTR_NE_NULL, /*offset=*/3),
    enc_emb_attr_id(/*attrId=*/1),
    enc_emb_op_word(EMB_EXIT_REFUSE, 899u << 16),
    enc_emb_op_word(EMB_EXIT_REFUSE, 626u << 16),
  };
  assert_scan_filter_rejected("T40 scan_filter_mixed_refuse_codes_reject",
                              filter_prog, 4,
                              JIT_BRIDGE_UNSUPPORTED_OP,
                              /*want_word=*/UINT32_MAX,
                              EMB_EXIT_REFUSE);
}

/* T41: BRANCH_ATTR_OP_ARG (WHERE col <op> literal) lowers to a one-operand
 * cold-call branch. word0 = opcode | nulls(IF_NULL_CONTINUE=3) | cond(GT=4)
 * | (branch_offset=5 << 16); word1 = (attrId=1 << 16) | argLen(8 bytes);
 * words 2-3 = the 8-byte literal; emb_pc 4 = EXIT_OK; emb_pc 5 = EXIT_REFUSE
 * (the branch target). Expected lowering (4 ops):
 *   op0 OP_BRANCH_ATTR_OP_ARG  b = inst offset 0, c = target op2
 *   op1 OP_EXIT                (EXIT_OK accept)
 *   op2 OP_FILTER_REJECT_EXIT  (EXIT_REFUSE)
 *   op3 OP_EXIT                (trailing accept) */
static void test_scan_filter_attr_op_arg_lower(void) {
  const char *name = "T41 scan_filter_attr_op_arg_lower";
  uint32_t filter_prog[6] = {
    enc_emb_op_word(EMB_BRANCH_ATTR_OP_ARG,
                    (3u << 6) | (4u << 12) | (5u << 16)),
    (1u << 16) | 8u,
    0x00000005u,
    0x00000000u,
    enc_emb_op_word(EMB_EXIT_OK, 0),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  Program p;
  if (!assert_scan_filter_accepted(name, filter_prog, 6, &p,
                                   /*expected_n_ops=*/4)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_BRANCH_ATTR_OP_ARG)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 0)) return;  // inst offset
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 2)) return;  // target -> op2
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind, OP_EXIT)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_FILTER_REJECT_EXIT)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind, OP_EXIT)) return;
  mark_pass(name);
}

/* T42: BRANCH_ATTR_OP_ARG with a non-inequality condition (LIKE=6) is not
 * JIT-admitted — only EQ..GE (0..5) lower; the rest fall back to the
 * interpreter. */
static void test_scan_filter_attr_op_arg_like_reject(void) {
  uint32_t filter_prog[6] = {
    enc_emb_op_word(EMB_BRANCH_ATTR_OP_ARG,
                    (3u << 6) | (6u << 12) | (5u << 16)),  // cond = LIKE(6)
    (1u << 16) | 8u,
    0x00000005u,
    0x00000000u,
    enc_emb_op_word(EMB_EXIT_OK, 0),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_scan_filter_rejected("T42 scan_filter_attr_op_arg_like_reject",
                              filter_prog, 6,
                              JIT_BRIDGE_UNSUPPORTED_OP,
                              /*want_word=*/UINT32_MAX,
                              EMB_BRANCH_ATTR_OP_ARG);
}

/* T43: BRANCH_ATTR_OP_PARAM (WHERE col <op> ?param) lowers to the same
 * OP_BRANCH_ATTR_OP_ARG cold-call branch as OP_ARG, but the instruction is
 * 2 words (no inline literal — the value lives in the param region, looked
 * up at runtime). word0 = opcode | nulls(3) | cond(GT=4) | (offset=3<<16);
 * word1 = (attrId=1 << 16) | paramNo(0); emb_pc 2 = EXIT_OK; emb_pc 3 =
 * EXIT_REFUSE (the branch target). Expected lowering (4 ops):
 *   op0 OP_BRANCH_ATTR_OP_ARG  b = inst offset 0, c = target op2
 *   op1 OP_EXIT                (EXIT_OK accept)
 *   op2 OP_FILTER_REJECT_EXIT  (EXIT_REFUSE)
 *   op3 OP_EXIT                (trailing accept) */
static void test_scan_filter_attr_op_param_lower(void) {
  const char *name = "T43 scan_filter_attr_op_param_lower";
  uint32_t filter_prog[4] = {
    enc_emb_op_word(EMB_BRANCH_ATTR_OP_PARAM,
                    (3u << 6) | (4u << 12) | (3u << 16)),
    (1u << 16) | 0u,                      // attrId=1, paramNo=0
    enc_emb_op_word(EMB_EXIT_OK, 0),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  Program p;
  if (!assert_scan_filter_accepted(name, filter_prog, 4, &p,
                                   /*expected_n_ops=*/4)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_BRANCH_ATTR_OP_ARG)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 0)) return;  // inst offset
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 2)) return;  // target -> op2
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind, OP_EXIT)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_FILTER_REJECT_EXIT)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind, OP_EXIT)) return;
  mark_pass(name);
}

/* T44: BRANCH_ATTR_OP_ATTR (WHERE col1 <op> col2) lowers to the same
 * OP_BRANCH_ATTR_OP_ARG cold-call branch — a 2-word instruction (no inline
 * literal; the 2nd operand is another column read at runtime). word0 =
 * opcode | nulls(3) | cond(LT=2) | (offset=3<<16); word1 = (attrId1=0 << 16)
 * | attrId2(1); emb_pc 2 = EXIT_OK; emb_pc 3 = EXIT_REFUSE (target).
 * Expected lowering matches T43 (4 ops). */
static void test_scan_filter_attr_op_attr_lower(void) {
  const char *name = "T44 scan_filter_attr_op_attr_lower";
  uint32_t filter_prog[4] = {
    enc_emb_op_word(EMB_BRANCH_ATTR_OP_ATTR,
                    (3u << 6) | (2u << 12) | (3u << 16)),
    (0u << 16) | 1u,                     // attrId1=0, attrId2=1
    enc_emb_op_word(EMB_EXIT_OK, 0),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  Program p;
  if (!assert_scan_filter_accepted(name, filter_prog, 4, &p,
                                   /*expected_n_ops=*/4)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_BRANCH_ATTR_OP_ARG)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 0)) return;  // inst offset
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 2)) return;  // target -> op2
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind, OP_EXIT)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_FILTER_REJECT_EXIT)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind, OP_EXIT)) return;
  mark_pass(name);
}

/* T45: EXIT_OK_LAST is REJECTED on the scan-filter path. It means "accept
 * this row AND terminate the fragment scan" — the interpreter's
 * handleExitOkLast sets req_struct->last_row — and the JIT has no way to
 * signal last-row, so lowering it as a plain accept keeps the scan
 * running. The one-instruction program below is exactly what
 * ndb_get_table_statistics' interpret_exit_last_row() emits; mis-lowering
 * it made the stats scan return every row per fragment instead of one,
 * inflating stats.records and thus implicit COUNT(*) (the
 * ndb_pushdown_agg 44-vs-12 regression). */
static void test_scan_filter_exit_ok_last_reject(void) {
  uint32_t filter_prog[1] = {
    enc_emb_op_word(EMB_EXIT_OK_LAST, 0),
  };
  assert_scan_filter_rejected("T45 scan_filter_exit_ok_last_reject",
                              filter_prog, 1,
                              JIT_BRIDGE_UNSUPPORTED_OP,
                              /*want_word=*/UINT32_MAX,
                              EMB_EXIT_OK_LAST);
}

/* T46: kOpCount lowers to OP_COUNT_BIGINT (Phase 8 GROUP BY lift).
 * SELECT COUNT(c0), SUM(c0) shape:
 *   load_col r0, c0
 *   count acc[0], r0
 *   sum_bigint acc[1], r0
 * Expected lowering (5 ops): load_col_ndb, COUNT (unchecked — no
 * overflow branch), SUM_BIGINT_CHECKED, exit, overflow_exit (from the
 * checked SUM). The mask index (op->c) equals the wire agg_index (the
 * AggResItem index — Phase 5A fix): COUNT agg 0 -> c=0, SUM agg 1 ->
 * c=1. */
static void test_count_lowering_accept(void) {
  const char *name = "T46 count_lowering_accept";
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_count(/*reg=*/0, /*agg=*/0),
    enc_sum(/*reg=*/0, /*agg=*/1),
  };
  Program p;
  if (!expect_accepted(name, prog, 3, &p, /*expected_n_ops=*/5)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_COUNT_BIGINT)) return;
  if (!expect_op_field(name, &p, 1, "a", p.ops[1].a, 0)) return;
  if (!expect_op_field(name, &p, 1, "c", p.ops[1].c, 0)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_SUM_BIGINT_CHECKED)) return;
  if (!expect_op_field(name, &p, 2, "a", p.ops[2].a, 1)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c, 1)) return;
  mark_pass(name);
}

/* T46b: kOpCount with an out-of-range acc slot rejects. */
static void test_count_slot_oor_reject(void) {
  uint32_t prog[2] = {
    enc_load_col(NDB_TYPE_BIGINT, 0, 0),
    enc_count(/*reg=*/0, /*agg=*/BC_MAX_ACCS),
  };
  assert_rejected("T46b count_slot_oor_reject", prog, 2,
                  JIT_BRIDGE_REG_OUT_OF_RANGE,
                  /*want_word=*/1, kOpCount);
}

/* T47: Phase 5A — the SQL planner's CASE condition family lowers.
 * Embedded shape (ha_ndbcluster_push_agg.cc emit_int_comparison_branch
 * + the single-arm accept path):
 *   emb(7): 0 READ_ATTR attr7 -> r0
 *           1 LOAD_CONST16 r1, 115
 *           2 BRANCH_GT_REG_REG r0, r1, +2   (true -> accept @4)
 *           3 EXIT_REFUSE 626
 *           4 LOAD_CONST16 r2, 0
 *           5 WRITE_INTERPRETER_OUTPUT r2, 0
 *           6 EXIT_OK
 *   outer: kOpLoadCol(r0, c0), kOpSum(r0, agg0)
 * Expected ops (10): [0]LOAD_COL_NDB(r0<-attr7) [1]LOAD_CONST_UINT16(r1)
 *   [2]BRANCH_GT_INT_INT(a=0,b=1,c=4) [3]OP_EXIT(refuse)
 *   [4]LOAD_CONST_UINT16(r2, materialized accept marker)
 *   [5]OP_JUMP(c=6 — zero-skip disposition jump)
 *   [6]outer LOAD_COL_NDB [7]SUM_CHECKED(c=0,d=9) [8]OP_EXIT(tail)
 *   [9]OVERFLOW_EXIT. */
static void test_case_condition_family_accept(void) {
  const char *name = "T47 case_condition_family_accept";
  uint32_t prog[10] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/7),
    enc_emb_read_attr(/*attr_id=*/7, /*reg=*/0),
    enc_emb_load_const16(/*reg=*/1, /*val=*/115),
    enc_emb_branch_reg_reg(EMB_BRANCH_GT_REG_REG, /*left=*/0, /*right=*/1,
                           /*offset=*/2),
    enc_emb_op_word(EMB_EXIT_REFUSE, (626u << 16)),
    enc_emb_load_const16(/*reg=*/2, /*val=*/0),
    enc_emb_write_output(/*reg=*/2, /*out_idx=*/0),
    enc_emb_op_word(EMB_EXIT_OK, 0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  if (!expect_accepted(name, prog, 10, &p, /*expected_n_ops=*/10)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 7)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_LOAD_CONST_UINT16)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_BRANCH_GT_INT_INT)) return;
  if (!expect_op_field(name, &p, 2, "a", p.ops[2].a, 0)) return;
  if (!expect_op_field(name, &p, 2, "b", p.ops[2].b, 1)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c, 4)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind, OP_EXIT)) return;
  if (!expect_op_field(name, &p, 4, "kind", p.ops[4].kind,
                       OP_LOAD_CONST_UINT16)) return;
  if (!expect_op_field(name, &p, 5, "kind", p.ops[5].kind, OP_JUMP)) return;
  if (!expect_op_field(name, &p, 5, "c", p.ops[5].c, 6)) return;
  /* Phase 5D-1: the outer load converts (the embedded READ_ATTR at
   * op 0 stays OP_LOAD_COL_NDB — embedded loads are never converted). */
  if (!expect_op_field(name, &p, 6, "kind", p.ops[6].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 6, "c", p.ops[6].c, 8)) return;
  if (!expect_op_field(name, &p, 7, "kind", p.ops[7].kind,
                       OP_SUM_BIGINT_CHECKED)) return;
  if (!expect_op_field(name, &p, 7, "c", p.ops[7].c, 0)) return;
  mark_pass(name);
}

/* T47b: embedded LOAD_CONST64 lowers to OP_LOAD_CONST_INT with the
 * 64-bit immediate reassembled low-word-first. */
static void test_load_const64_lowering(void) {
  const char *name = "T47b load_const64_lowering";
  const int64_t want = (int64_t)0x1122334455667788LL;
  uint32_t prog[7] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/6),
    enc_emb_read_attr(/*attr_id=*/3, /*reg=*/0),
    enc_emb_load_const64_hdr(/*reg=*/1),
    (uint32_t)(0x55667788u),              /* low word  */
    (uint32_t)(0x11223344u),              /* high word */
    enc_emb_branch_reg_reg(EMB_BRANCH_NE_REG_REG, 0, 1, /*offset=*/1),
    enc_emb_op_word(EMB_EXIT_REFUSE, (626u << 16)),
  };
  Program p;
  /* ops: [0]LOAD_COL_NDB [1]LOAD_CONST_INT [2]BRANCH_NE(c=3)
   *      [3]OP_EXIT(refuse) [4]tail OP_EXIT. No aggregation ops, so no
   *      OVERFLOW_EXIT. */
  if (!expect_accepted(name, prog, 7, &p, /*expected_n_ops=*/5)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_LOAD_CONST_INT)) return;
  if (p.ops[1].imm != want) {
    mark_fail(name, "imm=%lld, want %lld",
              (long long)p.ops[1].imm, (long long)want);
    return;
  }
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_BRANCH_NE_INT_INT)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c, 3)) return;
  mark_pass(name);
}

/* T47c: the reg-ops family stays REJECTED on the scan-filter path
 * (allow_reg_ops=0 there — NdbScanFilter never emits it and the
 * per-row fallback plumbing is aggregation-only today). */
static void test_scan_filter_reg_ops_reject(void) {
  uint32_t filter_prog[2] = {
    enc_emb_branch_reg_reg(EMB_BRANCH_EQ_REG_REG, 0, 1, /*offset=*/1),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_scan_filter_rejected("T47c scan_filter_reg_ops_reject",
                              filter_prog, 2,
                              JIT_BRIDGE_UNSUPPORTED_OP,
                              /*want_word=*/UINT32_MAX,
                              EMB_BRANCH_EQ_REG_REG);
}

/* T47d: a backward REG_REG branch (direction bit 31) rejects the whole
 * program — same forward-only contract as every embedded branch. */
static void test_reg_reg_backward_reject(void) {
  uint32_t prog[3] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/2),
    enc_emb_branch_reg_reg(EMB_BRANCH_EQ_REG_REG, 0, 1, /*offset=*/1) |
        0x80000000u,
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_rejected("T47d reg_reg_backward_reject", prog, 3,
                  JIT_BRIDGE_EMBEDDED_BACKWARD,
                  /*want_word=*/UINT32_MAX, EMB_BRANCH_EQ_REG_REG);
}

/* T49: kOpMinBigint / kOpMaxBigint lower (Phase 5B). Same wire layout
 * as kOpSumBigint; unchecked (no OVERFLOW_EXIT when the program has no
 * checked arithmetic); mask index c = agg_index. */
static void test_min_max_lowering_accept(void) {
  const char *name = "T49 min_max_lowering_accept";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_op(kOpMinBigint, (0u << 16) | 0u),   /* min acc0, r0 */
    enc_op(kOpMaxBigint, (0u << 16) | 1u),   /* max acc1, r0 */
    enc_count(/*reg=*/0, /*agg=*/2),
  };
  Program p;
  /* ops: [0]LOAD_COL [1]MIN [2]MAX [3]COUNT [4]tail EXIT — all
   * unchecked, so no OVERFLOW_EXIT. */
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/5)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_MIN_BIGINT)) return;
  if (!expect_op_field(name, &p, 1, "a", p.ops[1].a, 0)) return;
  if (!expect_op_field(name, &p, 1, "b", p.ops[1].b, 0)) return;
  if (!expect_op_field(name, &p, 1, "c", p.ops[1].c, 0)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_MAX_BIGINT)) return;
  if (!expect_op_field(name, &p, 2, "a", p.ops[2].a, 1)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c, 1)) return;
  if (!expect_op_field(name, &p, 4, "kind", p.ops[4].kind, OP_EXIT)) return;
  mark_pass(name);
}

/* T49b: MIN with an out-of-range acc slot rejects. */
static void test_min_slot_oor_reject(void) {
  uint32_t prog[2] = {
    enc_load_col(NDB_TYPE_BIGINT, 0, 0),
    enc_op(kOpMinBigint, (0u << 16) | BC_MAX_ACCS),
  };
  assert_rejected("T49b min_slot_oor_reject", prog, 2,
                  JIT_BRIDGE_REG_OUT_OF_RANGE,
                  /*want_word=*/1, kOpMinBigint);
}

/* T48: outer kOpSkip lowers to OP_JUMP (Phase 5A — the planner emits a
 * Skip after each CASE arm to jump past the remaining arms). Two-arm
 * shape, both arms feeding the SAME aggregate (RepeatAgg re-emits the
 * aggregate opcode with the same agg id):
 *   0: kOpLoadCol r0,c0   1: kOpSum r0,agg0
 *   2: kOpSkip 2          (target word 5 = end -> tail OP_EXIT)
 *   3: kOpLoadCol r1,c1   4: kOpSum r1,agg0
 * Expected ops (7): [0]LOAD_COL [1]SUM_CHECKED(c=0) [2]OP_JUMP(c=5)
 *   [3]LOAD_COL [4]SUM_CHECKED(c=0) [5]OP_EXIT(tail) [6]OVERFLOW_EXIT. */
static void test_skip_lowers_to_jump(void) {
  const char *name = "T48 skip_lowers_to_jump";
  uint32_t prog[5] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_sum(/*reg=*/0, /*agg=*/0),
    enc_op(kOpSkip, /*skip_count=*/2),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/1, /*col=*/1),
    enc_sum(/*reg=*/1, /*agg=*/0),
  };
  Program p;
  if (!expect_accepted(name, prog, 5, &p, /*expected_n_ops=*/7)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind, OP_JUMP)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c, 5)) return;
  if (!expect_op_field(name, &p, 1, "c", p.ops[1].c, 0)) return;
  if (!expect_op_field(name, &p, 4, "c", p.ops[4].c, 0)) return;
  if (!expect_op_field(name, &p, 5, "kind", p.ops[5].kind, OP_EXIT)) return;
  mark_pass(name);
}

/* ------------------------------------------------------------------ */
/* Phase 5C-2 — the DOUBLE family.                                     */
/* ------------------------------------------------------------------ */

/* T50: full double lowering battery. DOUBLE and FLOAT columns both
 * enter the f64 track (FLOAT promotes in the helper); all four
 * arithmetic ops lower with the shared overflow target; the three
 * accumulators lower with c = agg_index; SUM is checked, MIN/MAX are
 * not. */
static void test_double_family_lowering_accept(void) {
  const char *name = "T50 double_family_lowering_accept";
  uint32_t prog[9] = {
    enc_load_col(NDB_TYPE_DOUBLE, /*reg=*/0, /*col=*/0),
    enc_load_col(NDB_TYPE_FLOAT,  /*reg=*/1, /*col=*/1),
    enc_2reg(kOpPlusDouble,  0, 1),
    enc_2reg(kOpMulDouble,   0, 0),
    enc_2reg(kOpMinusDouble, 0, 1),
    enc_2reg(kOpDivDouble,   0, 1),
    enc_agg(kOpSumDouble, /*reg=*/0, /*agg=*/0),
    enc_agg(kOpMinDouble, /*reg=*/1, /*agg=*/1),
    enc_agg(kOpMaxDouble, /*reg=*/1, /*agg=*/2),
  };
  Program p;
  /* ops: [0][1]LOAD_COL_NDB_F64 [2]ADD [3]MUL [4]MINUS [5]DIV
   *      [6]SUM_F64 [7]MIN_F64 [8]MAX_F64 [9]tail EXIT
   *      [10]OVERFLOW_EXIT. */
  if (!expect_accepted(name, prog, 9, &p, /*expected_n_ops=*/11)) return;
  /* Phase 5D-2: load [1] (r1) converts — its chain runs through every
   * arithmetic op to MAX at [8], so its null branch targets 9. Load
   * [0] (r0) DEGRADES by design: its range ends at the SUM ([6]) with
   * the untainted r1 load inside it, and r1 is read after the range
   * ([7]/[8]) — a NULL in r0's column keeps the per-row fallback. */
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_F64)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_LOAD_COL_NDB_F64_NB)) return;
  if (!expect_op_field(name, &p, 1, "c", p.ops[1].c, 9)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind, OP_ADD_F64))
    return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind, OP_MUL_F64))
    return;
  if (!expect_op_field(name, &p, 4, "kind", p.ops[4].kind, OP_MINUS_F64))
    return;
  if (!expect_op_field(name, &p, 5, "kind", p.ops[5].kind, OP_DIV_F64))
    return;
  if (!expect_op_field(name, &p, 6, "kind", p.ops[6].kind, OP_SUM_F64))
    return;
  if (!expect_op_field(name, &p, 7, "kind", p.ops[7].kind, OP_MIN_F64))
    return;
  if (!expect_op_field(name, &p, 8, "kind", p.ops[8].kind, OP_MAX_F64))
    return;
  /* Checked ops all point at the tail OVERFLOW_EXIT (pc 10). */
  for (uint16_t pc = 2; pc <= 6; pc++) {
    if (!expect_op_field(name, &p, pc, "d", p.ops[pc].d, 10)) return;
  }
  /* MIN/MAX are unchecked. */
  if (!expect_op_field(name, &p, 7, "d", p.ops[7].d, 0)) return;
  if (!expect_op_field(name, &p, 8, "d", p.ops[8].d, 0)) return;
  /* Accumulator operand shape: a = c = agg_index, b = src reg. */
  if (!expect_op_field(name, &p, 6, "a", p.ops[6].a, 0)) return;
  if (!expect_op_field(name, &p, 6, "b", p.ops[6].b, 0)) return;
  if (!expect_op_field(name, &p, 6, "c", p.ops[6].c, 0)) return;
  if (!expect_op_field(name, &p, 7, "a", p.ops[7].a, 1)) return;
  if (!expect_op_field(name, &p, 7, "b", p.ops[7].b, 1)) return;
  if (!expect_op_field(name, &p, 8, "a", p.ops[8].a, 2)) return;
  mark_pass(name);
}

/* T50b: generic kOpDiv over two PROVEN-f64 registers lowers to
 * OP_DIV_F64 (the kernel's runtime conversion is a no-op when both
 * operands are already double). */
static void test_generic_div_f64_lowering_accept(void) {
  const char *name = "T50b generic_div_f64_lowering_accept";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_DOUBLE, /*reg=*/0, /*col=*/0),
    enc_load_col(NDB_TYPE_DOUBLE, /*reg=*/1, /*col=*/1),
    enc_2reg(kOpDiv, 0, 1),
    enc_agg(kOpSumDouble, /*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0][1]LOAD_COL_NDB_F64 [2]DIV_F64 [3]SUM_F64 [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind, OP_DIV_F64))
    return;
  if (!expect_op_field(name, &p, 2, "d", p.ops[2].d, 5)) return;
  mark_pass(name);
}

/* T51: the 5C-1 tracker's rejects, now constructible with f64
 * producers in the opcode set. */
static void test_type_mismatch_f64_into_i64_sum(void) {
  /* SUM_BIGINT over an f64 register — optimizer-bug fence. */
  uint32_t prog[2] = {
    enc_load_col(NDB_TYPE_DOUBLE, 0, 0),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  assert_rejected("T51a type_mismatch_f64_into_sum_bigint", prog, 2,
                  JIT_BRIDGE_TYPE_MISMATCH, 1, kOpSumBigint);
}

static void test_type_mismatch_i64_into_f64_sum(void) {
  uint32_t prog[2] = {
    enc_load_col(NDB_TYPE_BIGINT, 0, 0),
    enc_agg(kOpSumDouble, /*reg=*/0, /*agg=*/0),
  };
  assert_rejected("T51b type_mismatch_i64_into_sum_double", prog, 2,
                  JIT_BRIDGE_TYPE_MISMATCH, 1, kOpSumDouble);
}

static void test_type_mismatch_f64_into_i64_arith(void) {
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_BIGINT, 0, 0),
    enc_load_col(NDB_TYPE_DOUBLE, 1, 1),
    enc_2reg(kOpPlusBigint, 0, 1),
  };
  assert_rejected("T51c type_mismatch_f64_into_plus_bigint", prog, 3,
                  JIT_BRIDGE_TYPE_MISMATCH, 2, kOpPlusBigint);
}

static void test_type_mismatch_i64_into_f64_arith(void) {
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_DOUBLE, 0, 0),
    enc_load_col(NDB_TYPE_BIGINT, 1, 1),
    enc_2reg(kOpPlusDouble, 0, 1),
  };
  assert_rejected("T51d type_mismatch_i64_into_plus_double", prog, 3,
                  JIT_BRIDGE_TYPE_MISMATCH, 2, kOpPlusDouble);
}

/* T51e: kOpMov is type-preserving — a moved f64 register still feeds
 * kOpSumDouble. */
static void test_mov_preserves_f64_accept(void) {
  const char *name = "T51e mov_preserves_f64_accept";
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_DOUBLE, 0, 0),
    enc_2reg(kOpMov, /*dst=*/1, /*src=*/0),
    enc_agg(kOpSumDouble, /*reg=*/1, /*agg=*/0),
  };
  Program p;
  /* [0]LOAD_COL_NDB_F64 [1]MOV [2]SUM_F64 [3]EXIT [4]OVF. */
  if (!expect_accepted(name, prog, 3, &p, /*expected_n_ops=*/5)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind, OP_SUM_F64))
    return;
  mark_pass(name);
}

/* T51g: the AVG(double_col) decomposition — kOpSumDouble + kOpCount
 * over the SAME f64 register — must accept. COUNT never reads the
 * register's bits (acc += 1), so its operand type is irrelevant; a
 * type check there would knock out every double AVG. */
static void test_avg_double_shape_accept(void) {
  const char *name = "T51g avg_double_shape_accept";
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_DOUBLE, 0, 0),
    enc_agg(kOpSumDouble, /*reg=*/0, /*agg=*/0),
    enc_count(/*reg=*/0, /*agg=*/1),
  };
  Program p;
  /* [0]LOAD_COL_NDB_F64 [1]SUM_F64 [2]COUNT [3]EXIT [4]OVF. */
  if (!expect_accepted(name, prog, 3, &p, /*expected_n_ops=*/5)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_COUNT_BIGINT)) return;
  mark_pass(name);
}

/* T51f: an embedded block invalidates the tracker — an f64 register
 * from before the block may not feed an f64 consumer after it (the
 * planner re-loads outer registers after embedded blocks, so real
 * programs never hit this; an optimizer/emitter bug would). */
static void test_embedded_invalidates_f64_reject(void) {
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_DOUBLE, 0, 0),
    enc_op(kOpEmbeddedInterp, /*emb_len=*/0),
    enc_agg(kOpSumDouble, /*reg=*/0, /*agg=*/0),
  };
  assert_rejected("T51f embedded_invalidates_f64_reject", prog, 3,
                  JIT_BRIDGE_TYPE_MISMATCH, 2, kOpSumDouble);
}

/* ------------------------------------------------------------------ */
/* Phase 5C-3 — unsigned BIGINT.                                       */
/* ------------------------------------------------------------------ */

/* T52: u64 lowering battery. A declared-BIGUNSIGNED load enters the
 * u64 track; the optimizer's kOpSumBigint / kOpMin/MaxBigint (the
 * BIGINT track covers unsigned — the kernels dispatch on the
 * register's is_unsigned at runtime) lower to the UNSIGNED variants. */
static void test_u64_lowering_accept(void) {
  const char *name = "T52 u64_lowering_accept";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_BIGUNSIGNED, /*reg=*/0, /*col=*/0),
    enc_sum(/*reg=*/0, /*agg=*/0),
    enc_agg(kOpMinBigint, /*reg=*/0, /*agg=*/1),
    enc_agg(kOpMaxBigint, /*reg=*/0, /*agg=*/2),
  };
  Program p;
  /* [0]LOAD_COL_NDB_U64 [1]SUM_U64_CHECKED [2]MIN_U64 [3]MAX_U64
   * [4]tail EXIT [5]OVERFLOW_EXIT. */
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/6)) return;
  /* Phase 5D-2: the u64 load converts to its null-branching form. */
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_U64_NB)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 0)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 4)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_SUM_U64_CHECKED)) return;
  if (!expect_op_field(name, &p, 1, "d", p.ops[1].d, 5)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind, OP_MIN_U64))
    return;
  if (!expect_op_field(name, &p, 2, "d", p.ops[2].d, 0)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind, OP_MAX_U64))
    return;
  if (!expect_op_field(name, &p, 1, "a", p.ops[1].a, 0)) return;
  if (!expect_op_field(name, &p, 2, "a", p.ops[2].a, 1)) return;
  if (!expect_op_field(name, &p, 3, "a", p.ops[3].a, 2)) return;
  mark_pass(name);
}

/* T52b: a BIGUNSIGNED constant is admitted (bits ride the imm64 path)
 * and tracks u64 — feeding kOpMinBigint lowers unsigned. Value
 * 2^63 + 5: as a signed imm it is negative; the tracker, not the
 * value, decides the variant. */
static void test_u64_const_accept(void) {
  const char *name = "T52b u64_const_accept";
  uint32_t prog[4] = {
    enc_load_const(NDB_TYPE_BIGUNSIGNED, 0),
    0x00000005u, 0x80000000u,   /* 0x8000000000000005, low word first */
    enc_agg(kOpMinBigint, /*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]LOAD_CONST_INT [1]MIN_U64 [2]tail EXIT. */
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/3)) return;
  if (p.ops[0].imm != (int64_t)0x8000000000000005LL) {
    mark_fail(name, "decoded imm=0x%" PRIx64 ", want 0x8000000000000005",
              (uint64_t)p.ops[0].imm);
    return;
  }
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind, OP_MIN_U64))
    return;
  mark_pass(name);
}

/* T53a: u64 into signed checked arithmetic rejects — unsigned
 * arithmetic has no JIT lowering yet (SUM(u_col + 1) falls back
 * whole). */
static void test_u64_into_arith_reject(void) {
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_BIGUNSIGNED, 0, 0),
    enc_load_col(NDB_TYPE_BIGINT, 1, 1),
    enc_2reg(kOpPlusBigint, 1, 0),
  };
  assert_rejected("T53a u64_into_arith_reject", prog, 3,
                  JIT_BRIDGE_TYPE_MISMATCH, 2, kOpPlusBigint);
}

/* T53b: mixed accumulator families reject — two Sum ops targeting
 * the SAME agg slot from signed and unsigned arms would leave the
 * per-row value_unsigned mask reflecting whichever arm ran last. */
static void test_u64_mixed_acc_family_reject(void) {
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_BIGINT, 0, 0),
    enc_load_col(NDB_TYPE_BIGUNSIGNED, 1, 1),
    enc_sum(/*reg=*/0, /*agg=*/0),
    enc_sum(/*reg=*/1, /*agg=*/0),
  };
  assert_rejected("T53b u64_mixed_acc_family_reject", prog, 4,
                  JIT_BRIDGE_TYPE_MISMATCH, 3, kOpSumBigint);
}

/* T53c: COUNT over a u64 register accepts — COUNT never reads the
 * register's bits (same rationale as the f64 T51g case). */
static void test_u64_count_accept(void) {
  const char *name = "T53c u64_count_accept";
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_BIGUNSIGNED, 0, 0),
    enc_sum(/*reg=*/0, /*agg=*/0),
    enc_count(/*reg=*/0, /*agg=*/1),
  };
  Program p;
  /* [0]LOAD_COL_NDB_U64 [1]SUM_U64_CHECKED [2]COUNT [3]EXIT [4]OVF. */
  if (!expect_accepted(name, prog, 3, &p, /*expected_n_ops=*/5)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_COUNT_BIGINT)) return;
  mark_pass(name);
}

/* T53d: kOpMov preserves the u64 track. */
static void test_mov_preserves_u64_accept(void) {
  const char *name = "T53d mov_preserves_u64_accept";
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_BIGUNSIGNED, 0, 0),
    enc_2reg(kOpMov, /*dst=*/1, /*src=*/0),
    enc_agg(kOpMaxBigint, /*reg=*/1, /*agg=*/0),
  };
  Program p;
  /* [0]LOAD_COL_NDB_U64 [1]MOV [2]MAX_U64 [3]EXIT. */
  if (!expect_accepted(name, prog, 3, &p, /*expected_n_ops=*/4)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind, OP_MAX_U64))
    return;
  mark_pass(name);
}

/* ------------------------------------------------------------------ */
/* Phase 5D-1 — null-branching load conversion.                        */
/* ------------------------------------------------------------------ */

/* T55a: expression chain — SUM(a + c). BOTH loads convert; both null
 * branches target past the SUM (either operand NULL means the row
 * contributes nothing — the interpreter's null propagation + kernel
 * skip). The second load sits inside the first's skip range as an
 * already-converted NB whose own target stays within bounds. */
static void test_nb_expression_chain(void) {
  const char *name = "T55a nb_expression_chain";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/1, /*col=*/1),
    enc_2reg(kOpPlusBigint, 0, 1),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]NB [1]NB [2]ADD_CHECKED [3]SUM_CHECKED [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 0)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 4)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 1, "b", p.ops[1].b, 1)) return;
  if (!expect_op_field(name, &p, 1, "c", p.ops[1].c, 4)) return;
  mark_pass(name);
}

/* T55b: per-load degradation — an INDEPENDENT accumulator inside the
 * skip range (interleaved chains) must not be skipped, so that load
 * keeps the row-fallback form; the well-formed chain still converts. */
static void test_nb_interleaved_degradation(void) {
  const char *name = "T55b nb_interleaved_degradation";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/1, /*col=*/1),
    enc_sum(/*reg=*/1, /*agg=*/0),
    enc_sum(/*reg=*/0, /*agg=*/1),
  };
  Program p;
  /* [0]LOAD (degraded: r1's SUM sits untainted inside r0's range)
   * [1]NB(c=3) [2]SUM [3]SUM [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 0)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 1, "c", p.ops[1].c, 3)) return;
  mark_pass(name);
}

/* T55c: a branch (outer kOpSkip's OP_JUMP) inside the skip range
 * degrades the load — control flow inside a skipped region is not
 * analyzed. */
static void test_nb_branch_in_range_degradation(void) {
  const char *name = "T55c nb_branch_in_range_degradation";
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_op(kOpSkip, /*skip_count=*/0),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]LOAD (degraded) [1]JUMP(c=2) [2]SUM [3]EXIT [4]OVF. */
  if (!expect_accepted(name, prog, 3, &p, /*expected_n_ops=*/5)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind, OP_JUMP)) return;
  mark_pass(name);
}

/* T55d: COUNT joins the skip range — the interpreter's Count kernel
 * skips null registers, so COUNT(nullable_col) must skip too. */
static void test_nb_count_in_range(void) {
  const char *name = "T55d nb_count_in_range";
  uint32_t prog[2] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_count(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]NB(c=2) [1]COUNT [2]EXIT — COUNT is unchecked, no OVF tail. */
  if (!expect_accepted(name, prog, 2, &p, /*expected_n_ops=*/3)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 2)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_COUNT_BIGINT)) return;
  mark_pass(name);
}

/* T55e: two independent chains on the SAME register — each load's
 * range ends where the register is re-loaded; both convert. */
static void test_nb_reload_same_register(void) {
  const char *name = "T55e nb_reload_same_register";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_sum(/*reg=*/0, /*agg=*/0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/1),
    enc_sum(/*reg=*/0, /*agg=*/1),
  };
  Program p;
  /* [0]NB(c=2) [1]SUM [2]NB(c=4) [3]SUM [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 2)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c, 4)) return;
  mark_pass(name);
}

/* T56a: f64 null-branching load — simple chain converts. */
static void test_nb_f64_simple(void) {
  const char *name = "T56a nb_f64_simple";
  uint32_t prog[2] = {
    enc_load_col(NDB_TYPE_DOUBLE, /*reg=*/0, /*col=*/3),
    enc_agg(kOpSumDouble, /*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]F64_NB(c=2) [1]SUM_F64 [2]EXIT [3]OVF. */
  if (!expect_accepted(name, prog, 2, &p, /*expected_n_ops=*/4)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_F64_NB)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 3)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 2)) return;
  mark_pass(name);
}

/* T56b: u64 null-branching load — simple chain converts. */
static void test_nb_u64_simple(void) {
  const char *name = "T56b nb_u64_simple";
  uint32_t prog[2] = {
    enc_load_col(NDB_TYPE_BIGUNSIGNED, /*reg=*/0, /*col=*/5),
    enc_agg(kOpMinBigint, /*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]U64_NB(c=2) [1]MIN_U64 [2]EXIT. */
  if (!expect_accepted(name, prog, 2, &p, /*expected_n_ops=*/3)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_U64_NB)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 5)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 2)) return;
  mark_pass(name);
}

/* ------------------------------------------------------------------ */
/* Phase 5C-4 — unsigned checked arithmetic.                           */
/* ------------------------------------------------------------------ */

/* T57a: the census shape — SUM(u + 1). The BIGINT constant 1 is a
 * NON-NEGATIVE constant (NNC), u64-compatible, so the addition lowers
 * UNSIGNED and the result register carries u64 into an unsigned SUM. */
static void test_u64_arith_mixed_const(void) {
  const char *name = "T57a u64_arith_mixed_const";
  uint32_t prog[6];
  prog[0] = enc_load_col(NDB_TYPE_BIGUNSIGNED, /*reg=*/0, /*col=*/0);
  prog[1] = enc_load_const(NDB_TYPE_BIGINT, /*reg=*/1);
  prog[2] = 1u;          /* value 1, low word */
  prog[3] = 0u;          /* high word */
  prog[4] = enc_2reg(kOpPlusBigint, 0, 1);
  prog[5] = enc_sum(/*reg=*/0, /*agg=*/0);
  Program p;
  /* [0]U64_NB(c=4) [1]CONST [2]ADD_U64 [3]SUM_U64 [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 6, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_U64_NB)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_ADD_U64_CHECKED)) return;
  if (!expect_op_field(name, &p, 2, "d", p.ops[2].d, 5)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_SUM_U64_CHECKED)) return;
  mark_pass(name);
}

/* T57b: const-first form (1 + u) — the NNC destination becomes u64. */
static void test_u64_arith_const_first(void) {
  const char *name = "T57b u64_arith_const_first";
  uint32_t prog[6];
  prog[0] = enc_load_const(NDB_TYPE_BIGINT, /*reg=*/0);
  prog[1] = 2u;
  prog[2] = 0u;
  prog[3] = enc_load_col(NDB_TYPE_BIGUNSIGNED, /*reg=*/1, /*col=*/0);
  prog[4] = enc_2reg(kOpPlusBigint, 0, 1);
  prog[5] = enc_sum(/*reg=*/0, /*agg=*/0);
  Program p;
  /* [0]CONST [1]U64_NB(c=4) [2]ADD_U64 [3]SUM_U64 [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 6, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_ADD_U64_CHECKED)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_SUM_U64_CHECKED)) return;
  mark_pass(name);
}

/* T57c: a NEGATIVE constant mixed with u64 keeps the fallback — the
 * kernel's sign-dependent mixed logic has no lowering. */
static void test_u64_arith_negative_const_reject(void) {
  uint32_t prog[5];
  prog[0] = enc_load_const(NDB_TYPE_BIGINT, /*reg=*/0);
  prog[1] = (uint32_t)(int32_t)-5;   /* low word of -5 */
  prog[2] = 0xFFFFFFFFu;             /* high word (sign extension) */
  prog[3] = enc_load_col(NDB_TYPE_BIGUNSIGNED, /*reg=*/1, /*col=*/0);
  prog[4] = enc_2reg(kOpPlusBigint, 1, 0);
  assert_rejected("T57c u64_arith_negative_const_reject", prog, 5,
                  JIT_BRIDGE_TYPE_MISMATCH, 4, kOpPlusBigint);
}

/* T57d: NNC-only arithmetic stays SIGNED (no u64 operand). */
static void test_nnc_arith_stays_signed(void) {
  const char *name = "T57d nnc_arith_stays_signed";
  uint32_t prog[8];
  prog[0] = enc_load_const(NDB_TYPE_BIGINT, /*reg=*/0);
  prog[1] = 7u;
  prog[2] = 0u;
  prog[3] = enc_load_const(NDB_TYPE_BIGINT, /*reg=*/1);
  prog[4] = 8u;
  prog[5] = 0u;
  prog[6] = enc_2reg(kOpPlusBigint, 0, 1);
  prog[7] = enc_sum(/*reg=*/0, /*agg=*/0);
  Program p;
  /* [0]CONST [1]CONST [2]ADD_INT_CHECKED [3]SUM_BIGINT_CHECKED
   * [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 8, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_ADD_INT_INT_CHECKED)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_SUM_BIGINT_CHECKED)) return;
  mark_pass(name);
}

/* ------------------------------------------------------------------ */
/* Phase 5G — DECIMAL loads + generic-aggregate lowering.              */
/* ------------------------------------------------------------------ */

/* Two-word DECIMAL load: word0 like enc_load_col, word1 =
 * precision << 16 | scale. */
static uint32_t enc_dec_info(uint32_t precision, uint32_t scale) {
  return (precision << 16) | scale;
}

/* T58a: DECIMAL(9,0) — scale 0 lands in the BIGINT track; the GENERIC
 * kOpSum lowers via the tracker to the signed checked sum. */
static void test_dec_scale0_sum(void) {
  const char *name = "T58a dec_scale0_sum";
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_DECIMAL, /*reg=*/0, /*col=*/2),
    enc_dec_info(9, 0),
    enc_agg(kOpSum, /*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]LCD [1]SUM_BIGINT_CHECKED [2]EXIT [3]OVF. */
  if (!expect_accepted(name, prog, 3, &p, /*expected_n_ops=*/4)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_DEC)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, (9u << 8))) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 2)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_SUM_BIGINT_CHECKED)) return;
  if (!expect_op_field(name, &p, 1, "d", p.ops[1].d, 3)) return;
  mark_pass(name);
}

/* T58b: DECIMAL(9,2) — scale > 0 lands in the DOUBLE track; generic
 * Sum/Min/Max lower to the f64 accumulators. */
static void test_dec_scale2_aggregates(void) {
  const char *name = "T58b dec_scale2_aggregates";
  uint32_t prog[5] = {
    enc_load_col(NDB_TYPE_DECIMAL, /*reg=*/0, /*col=*/2),
    enc_dec_info(9, 2),
    enc_agg(kOpSum, /*reg=*/0, /*agg=*/0),
    enc_agg(kOpMin, /*reg=*/0, /*agg=*/1),
    enc_agg(kOpMax, /*reg=*/0, /*agg=*/2),
  };
  Program p;
  /* [0]LCD [1]SUM_F64 [2]MIN_F64 [3]MAX_F64 [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 5, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b,
                       (9u << 8) | 2u)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind, OP_SUM_F64))
    return;
  if (!expect_op_field(name, &p, 1, "d", p.ops[1].d, 5)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind, OP_MIN_F64))
    return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind, OP_MAX_F64))
    return;
  mark_pass(name);
}

/* T58c: DECIMALUNSIGNED(20,0) — scale 0 unsigned lands in the u64
 * track; generic Sum lowers to the carry-checked u64 sum, and the
 * pinfo word carries the signedness bit. */
static void test_dec_unsigned_scale0_sum(void) {
  const char *name = "T58c dec_unsigned_scale0_sum";
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_DECIMALUNSIGNED, /*reg=*/0, /*col=*/2),
    enc_dec_info(20, 0),
    enc_agg(kOpSum, /*reg=*/0, /*agg=*/0),
  };
  Program p;
  if (!expect_accepted(name, prog, 3, &p, /*expected_n_ops=*/4)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b,
                       0x8000u | (20u << 8))) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_SUM_U64_CHECKED)) return;
  mark_pass(name);
}

/* T58d: DECIMAL load truncated (missing the decimal_info word). */
static void test_dec_truncated_reject(void) {
  uint32_t prog[1] = {
    enc_load_col(NDB_TYPE_DECIMAL, /*reg=*/0, /*col=*/2),
  };
  assert_rejected("T58d dec_truncated_reject", prog, 1,
                  JIT_BRIDGE_MALFORMED, 0, kOpLoadCol);
}

/* T58e: generic Sum over an UNPROVEN register keeps the fallback. */
static void test_generic_sum_unknown_reject(void) {
  uint32_t prog[1] = {
    enc_agg(kOpSum, /*reg=*/0, /*agg=*/0),
  };
  assert_rejected("T58e generic_sum_unknown_reject", prog, 1,
                  JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpSum);
}

/* T58f: nonsense precision/scale rejects (scale > precision). */
static void test_dec_bad_info_reject(void) {
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_DECIMAL, /*reg=*/0, /*col=*/2),
    enc_dec_info(5, 9),
    enc_agg(kOpSum, /*reg=*/0, /*agg=*/0),
  };
  assert_rejected("T58f dec_bad_info_reject", prog, 3,
                  JIT_BRIDGE_MALFORMED, 0, kOpLoadCol);
}

/* ------------------------------------------------------------------ */
/* Phase 5F-1 — fused string MIN/MAX.                                  */
/* ------------------------------------------------------------------ */

/* T59a: single string MIN fuses the load + kernel into one op. */
static void test_str_min_fusion(void) {
  const char *name = "T59a str_min_fusion";
  uint32_t prog[2] = {
    enc_load_col(NDB_TYPE_VARCHAR, /*reg=*/0, /*col=*/3),
    enc_agg(kOpMin, /*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]MINMAX_STR(b=3, c=agg0|min) [1]tail EXIT. */
  if (!expect_accepted(name, prog, 2, &p, /*expected_n_ops=*/2)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_MINMAX_STR_NDB)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 3)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 0)) return;
  mark_pass(name);
}

/* T59b: MIN + MAX on the same string column, single load — two fused
 * ops, the MAX carrying the is_max bit. */
static void test_str_min_max_fusion(void) {
  const char *name = "T59b str_min_max_fusion";
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_CHAR, /*reg=*/0, /*col=*/4),
    enc_agg(kOpMin, /*reg=*/0, /*agg=*/0),
    enc_agg(kOpMax, /*reg=*/0, /*agg=*/1),
  };
  Program p;
  /* [0]MINMAX_STR(min agg0) [1]MINMAX_STR(max agg1) [2]EXIT. */
  if (!expect_accepted(name, prog, 3, &p, /*expected_n_ops=*/3)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_MINMAX_STR_NDB)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 0)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_MINMAX_STR_NDB)) return;
  if (!expect_op_field(name, &p, 1, "b", p.ops[1].b, 4)) return;
  if (!expect_op_field(name, &p, 1, "c", p.ops[1].c, 0x101)) return;
  mark_pass(name);
}

/* T59c: mixed program — the string aggregate fuses, the integer part
 * lowers hot (NB load + checked sum). */
static void test_str_mixed_program(void) {
  const char *name = "T59c str_mixed_program";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_sum(/*reg=*/0, /*agg=*/0),
    enc_load_col(NDB_TYPE_LONGVARCHAR, /*reg=*/1, /*col=*/5),
    enc_agg(kOpMin, /*reg=*/1, /*agg=*/1),
  };
  Program p;
  /* [0]LOAD_NB(c=2) [1]SUM_CHECKED [2]MINMAX_STR [3]EXIT [4]OVF. */
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/5)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_SUM_BIGINT_CHECKED)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_MINMAX_STR_NDB)) return;
  if (!expect_op_field(name, &p, 2, "b", p.ops[2].b, 5)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c, 1)) return;
  mark_pass(name);
}

/* T59d: a string load whose consumer is NOT Min/Max keeps the
 * whole-program fallback (the planner never emits string Sum/Count
 * through this path). */
static void test_str_non_minmax_consumer_reject(void) {
  uint32_t prog[2] = {
    enc_load_col(NDB_TYPE_VARCHAR, /*reg=*/0, /*col=*/3),
    enc_count(/*reg=*/0, /*agg=*/0),
  };
  assert_rejected("T59d str_non_minmax_consumer_reject", prog, 2,
                  JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpLoadCol);
}

/* T59e: a dangling string load (no consumer at all) rejects. */
static void test_str_dangling_load_reject(void) {
  uint32_t prog[1] = {
    enc_load_col(NDB_TYPE_VARCHAR, /*reg=*/0, /*col=*/3),
  };
  assert_rejected("T59e str_dangling_load_reject", prog, 1,
                  JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpLoadCol);
}

/* ------------------------------------------------------------------ */
/* Phase 5D-3: READ_ATTR + BRANCH_REG_EQ_NULL fusion (nullable CASE   */
/* condition columns). The planner emits the guard directly after the */
/* READ_ATTR it protects; the pair fuses into one OP_LOAD_COL_NDB_NB  */
/* whose taken edge is the guard's edge, keeping NULL rows on the JIT.*/
/* ------------------------------------------------------------------ */
#define EMB_BRANCH_REG_EQ_NULL   10
static uint32_t enc_emb_branch_reg_eq_null(uint32_t reg, uint32_t offset) {
  return EMB_BRANCH_REG_EQ_NULL | ((reg & 0x7u) << 6) |
         ((offset & 0x7FFFu) << 16);
}

/* T60a: single guarded condition, guard targets the refuse word
 * (condition failed). Planner shape for one nullable integer WHEN.
 *   emb(8): 0 READ_ATTR attr7 -> r0
 *           1 BRANCH_REG_EQ_NULL r0, +3   (-> 4 EXIT_REFUSE)
 *           2 LOAD_CONST16 r1, 115
 *           3 BRANCH_GT_REG_REG r0, r1, +2  (-> 5 accept)
 *           4 EXIT_REFUSE 626
 *           5 LOAD_CONST16 r2, 0
 *           6 WRITE_INTERPRETER_OUTPUT r2, 0
 *           7 EXIT_OK
 *   outer: kOpLoadCol(r0, c0), kOpSum(r0, agg0)
 * Expected ops (10): [0]LOAD_COL_NDB_NB(a=0,b=7,c=3 — the fused pair)
 *   [1]LC16 [2]BRANCH_GT(c=4) [3]EXIT(refuse) [4]LC16 [5]JUMP(c=6)
 *   [6]outer NB load [7]SUM_CHECKED [8]tail EXIT [9]OVERFLOW_EXIT. */
static void test_nb_case_guard_fusion(void) {
  const char *name = "T60a nb_case_guard_fusion";
  uint32_t prog[11] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/8),
    enc_emb_read_attr(/*attr_id=*/7, /*reg=*/0),
    enc_emb_branch_reg_eq_null(/*reg=*/0, /*offset=*/3),
    enc_emb_load_const16(/*reg=*/1, /*val=*/115),
    enc_emb_branch_reg_reg(EMB_BRANCH_GT_REG_REG, 0, 1, /*offset=*/2),
    enc_emb_op_word(EMB_EXIT_REFUSE, (626u << 16)),
    enc_emb_load_const16(/*reg=*/2, /*val=*/0),
    enc_emb_write_output(/*reg=*/2, /*out_idx=*/0),
    enc_emb_op_word(EMB_EXIT_OK, 0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  if (!expect_accepted(name, prog, 11, &p, /*expected_n_ops=*/10)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 0, "a", p.ops[0].a, 0)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 7)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 3)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_BRANCH_GT_INT_INT)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c, 4)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind, OP_EXIT)) return;
  if (!expect_op_field(name, &p, 5, "kind", p.ops[5].kind, OP_JUMP)) return;
  if (!expect_op_field(name, &p, 5, "c", p.ops[5].c, 6)) return;
  if (!expect_op_field(name, &p, 6, "kind", p.ops[6].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 7, "kind", p.ops[7].kind,
                       OP_SUM_BIGINT_CHECKED)) return;
  mark_pass(name);
}

/* T60b: guard target INSIDE an accept output block (the simple-CASE
 * "NULL search value -> ELSE block" shape). The null path walks
 * LC16(r2) + WRITE(r2) + EXIT_OK without touching r0 — safe. */
static void test_nb_case_guard_into_output_block(void) {
  const char *name = "T60b nb_case_guard_into_output_block";
  uint32_t prog[11] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/8),
    enc_emb_read_attr(/*attr_id=*/3, /*reg=*/0),
    enc_emb_branch_reg_eq_null(/*reg=*/0, /*offset=*/4),  /* -> 5 */
    enc_emb_load_const16(/*reg=*/1, /*val=*/10),
    enc_emb_branch_reg_reg(EMB_BRANCH_EQ_REG_REG, 0, 1, /*offset=*/2),
    enc_emb_op_word(EMB_EXIT_REFUSE, (626u << 16)),
    enc_emb_load_const16(/*reg=*/2, /*val=*/0),
    enc_emb_write_output(/*reg=*/2, /*out_idx=*/0),
    enc_emb_op_word(EMB_EXIT_OK, 0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  if (!expect_accepted(name, prog, 11, &p, /*expected_n_ops=*/10)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 3)) return;
  /* Both the guard (emb pc 5) and the compare (emb pc 3 -> +2 = 5)
   * resolve to the accept block's LC16 at op 4. */
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 4)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c, 4)) return;
  mark_pass(name);
}

/* T60c: a guard NOT immediately after a READ_ATTR has no lowering
 * (JIT registers carry no null state) — reject. */
static void test_nb_case_guard_no_read_attr_reject(void) {
  uint32_t prog[4] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/3),
    enc_emb_load_const16(/*reg=*/0, /*val=*/5),
    enc_emb_branch_reg_eq_null(/*reg=*/0, /*offset=*/1),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_rejected("T60c nb_case_guard_no_read_attr_reject", prog, 4,
                  JIT_BRIDGE_UNSUPPORTED_OP, 2, EMB_BRANCH_REG_EQ_NULL);
}

/* T60d: guard on a DIFFERENT register than the READ_ATTR — reject. */
static void test_nb_case_guard_reg_mismatch_reject(void) {
  uint32_t prog[4] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/3),
    enc_emb_read_attr(/*attr_id=*/1, /*reg=*/0),
    enc_emb_branch_reg_eq_null(/*reg=*/1, /*offset=*/1),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_rejected("T60d nb_case_guard_reg_mismatch_reject", prog, 4,
                  JIT_BRIDGE_UNSUPPORTED_OP, 2, EMB_BRANCH_REG_EQ_NULL);
}

/* T60e: the null path READS the register before overwriting it — the
 * fused load would compare an UNDEFINED value where the interpreter
 * holds a null tag (ZREGISTER_INIT_ERROR). emb_null_path_reg_safe
 * must veto the fusion -> reject. Guard target +1 = the fall-through
 * continuation, whose BRANCH_EQ reads r0. */
static void test_nb_case_guard_null_path_reads_reject(void) {
  uint32_t prog[6] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/5),
    enc_emb_read_attr(/*attr_id=*/1, /*reg=*/0),
    enc_emb_branch_reg_eq_null(/*reg=*/0, /*offset=*/1),
    enc_emb_load_const16(/*reg=*/1, /*val=*/5),
    enc_emb_branch_reg_reg(EMB_BRANCH_EQ_REG_REG, 0, 1, /*offset=*/1),
    enc_emb_op_word(EMB_EXIT_REFUSE, 0),
  };
  assert_rejected("T60e nb_case_guard_null_path_reads_reject", prog, 6,
                  JIT_BRIDGE_UNSUPPORTED_OP, 2, EMB_BRANCH_REG_EQ_NULL);
}

/* T60f: TWO guarded conditions — the planner's multi-WHEN shape. The
 * first guard targets the SECOND condition, whose READ_ATTR overwrites
 * r0 (the scanner's overwrite rule); the second guard targets the
 * refuse word (terminal rule).
 *   emb(12): 0 READ_ATTR attr7 r0    1 GUARD r0 +3 (->4)
 *            2 LC16 r1 115           3 BR_GT r0,r1 +6 (->9)
 *            4 READ_ATTR attr9 r0    5 GUARD r0 +3 (->8)
 *            6 LC16 r1 42            7 BR_EQ r0,r1 +2 (->9)
 *            8 EXIT_REFUSE           9 LC16 r2 0
 *           10 WRITE r2 0           11 EXIT_OK
 * Expected ops (13): [0]NB(b=7,c=3) [1]LC16 [2]BR_GT(c=7)
 *   [3]NB(b=9,c=6) [4]LC16 [5]BR_EQ(c=7) [6]EXIT [7]LC16 [8]JUMP(c=9)
 *   [9]outer NB [10]SUM_CHECKED [11]tail EXIT [12]OVERFLOW_EXIT. */
static void test_nb_case_guard_two_conditions(void) {
  const char *name = "T60f nb_case_guard_two_conditions";
  uint32_t prog[15] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/12),
    enc_emb_read_attr(/*attr_id=*/7, /*reg=*/0),
    enc_emb_branch_reg_eq_null(/*reg=*/0, /*offset=*/3),
    enc_emb_load_const16(/*reg=*/1, /*val=*/115),
    enc_emb_branch_reg_reg(EMB_BRANCH_GT_REG_REG, 0, 1, /*offset=*/6),
    enc_emb_read_attr(/*attr_id=*/9, /*reg=*/0),
    enc_emb_branch_reg_eq_null(/*reg=*/0, /*offset=*/3),
    enc_emb_load_const16(/*reg=*/1, /*val=*/42),
    enc_emb_branch_reg_reg(EMB_BRANCH_EQ_REG_REG, 0, 1, /*offset=*/2),
    enc_emb_op_word(EMB_EXIT_REFUSE, (626u << 16)),
    enc_emb_load_const16(/*reg=*/2, /*val=*/0),
    enc_emb_write_output(/*reg=*/2, /*out_idx=*/0),
    enc_emb_op_word(EMB_EXIT_OK, 0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  if (!expect_accepted(name, prog, 15, &p, /*expected_n_ops=*/13)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 7)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 3)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c, 7)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 3, "b", p.ops[3].b, 9)) return;
  if (!expect_op_field(name, &p, 3, "c", p.ops[3].c, 6)) return;
  if (!expect_op_field(name, &p, 5, "c", p.ops[5].c, 7)) return;
  if (!expect_op_field(name, &p, 6, "kind", p.ops[6].kind, OP_EXIT)) return;
  if (!expect_op_field(name, &p, 8, "kind", p.ops[8].kind, OP_JUMP)) return;
  if (!expect_op_field(name, &p, 8, "c", p.ops[8].c, 9)) return;
  if (!expect_op_field(name, &p, 9, "kind", p.ops[9].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  mark_pass(name);
}

/* ------------------------------------------------------------------ */
/* Narrow-int admission (census probe int_sum). SIGNED widths         */
/* sign-extend into the i64 track, UNSIGNED widths zero-extend into   */
/* the u64 track — the interpreter's IsUnsigned(type) split, so the   */
/* kernels' accumulation signedness and is_unsigned result metadata   */
/* match exactly. The load helpers already decoded every width; the   */
/* bridge admission was the only gate.                                */
/* ------------------------------------------------------------------ */

/* T61a: INT column -> i64 track (signed checked SUM); the load
 * null-branch-converts like any i64 load. */
static void test_narrow_int_sum(void) {
  const char *name = "T61a narrow_int_sum";
  uint32_t prog[2] = {
    enc_load_col(NDB_TYPE_INT, /*reg=*/0, /*col=*/3),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]NB load [1]SUM_CHECKED [2]tail EXIT [3]OVERFLOW_EXIT. */
  if (!expect_accepted(name, prog, 2, &p, /*expected_n_ops=*/4)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 3)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 2)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_SUM_BIGINT_CHECKED)) return;
  mark_pass(name);
}

/* T61b: SMALLUNSIGNED -> u64 track: generic Sum/Min/Max lower to the
 * unsigned accumulators and the load converts to the u64 NB form. */
static void test_narrow_unsigned_aggregates(void) {
  const char *name = "T61b narrow_unsigned_aggregates";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_SMALLUNSIGNED, /*reg=*/0, /*col=*/4),
    enc_sum(/*reg=*/0, /*agg=*/0),
    enc_agg(kOpMin, /*reg=*/0, /*agg=*/1),
    enc_agg(kOpMax, /*reg=*/0, /*agg=*/2),
  };
  Program p;
  /* [0]U64_NB [1]SUM_U64 [2]MIN_U64 [3]MAX_U64 [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_U64_NB)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 4)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 4)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_SUM_U64_CHECKED)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind, OP_MIN_U64))
    return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind, OP_MAX_U64))
    return;
  mark_pass(name);
}

/* T61c: TINYINT MIN/MAX -> signed compares (a negative TINYINT must
 * order below 0 — the u64 track would misorder it). */
static void test_narrow_signed_min_max(void) {
  const char *name = "T61c narrow_signed_min_max";
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_TINYINT, /*reg=*/0, /*col=*/1),
    enc_agg(kOpMin, /*reg=*/0, /*agg=*/0),
    enc_agg(kOpMax, /*reg=*/0, /*agg=*/1),
  };
  Program p;
  /* [0]NB load [1]MIN_BIGINT [2]MAX_BIGINT [3]tail EXIT — no checked
   * arithmetic, so no OVERFLOW_EXIT. */
  if (!expect_accepted(name, prog, 3, &p, /*expected_n_ops=*/4)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_MIN_BIGINT)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_MAX_BIGINT)) return;
  mark_pass(name);
}

/* T61d: narrow-unsigned + narrow-signed VARIABLE arithmetic keeps the
 * whole-program fallback — same mixed-track rule as the BIGUNSIGNED
 * family (the interpreter's mixed kernels are not lowered). */
static void test_narrow_mixed_arith_reject(void) {
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_MEDIUMUNSIGNED, 0, 2),
    enc_load_col(NDB_TYPE_INT, 1, 3),
    enc_2reg(kOpPlusBigint, 1, 0),
  };
  assert_rejected("T61d narrow_mixed_arith_reject", prog, 3,
                  JIT_BRIDGE_TYPE_MISMATCH, 2, kOpPlusBigint);
}

/* T61e: narrow-signed arithmetic chain -> checked signed adds, exactly
 * like BIGINT (both operands sign-extended i64). */
static void test_narrow_signed_arith(void) {
  const char *name = "T61e narrow_signed_arith";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_SMALLINT, /*reg=*/0, /*col=*/1),
    enc_load_col(NDB_TYPE_INT, /*reg=*/1, /*col=*/2),
    enc_2reg(kOpPlusBigint, 0, 1),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* Both loads DEGRADE to the row-fallback form? No: load r0's skip
   * range ends at the SUM while load r1 sits inside it and r1 is dead
   * after the ADD — per-load conversion applies as in T50:
   * [0]NB(r0) degrades or converts per the 5D-1 rules. Assert only
   * the kinds that matter: the ADD is checked-signed and the SUM is
   * checked-signed. */
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_ADD_INT_INT_CHECKED)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_SUM_BIGINT_CHECKED)) return;
  mark_pass(name);
}

/* T61f: narrow-unsigned + non-negative BIGINT constant -> u64 checked
 * arithmetic (the census unsigned_arith shape at narrow width). */
static void test_narrow_unsigned_const_arith(void) {
  const char *name = "T61f narrow_unsigned_const_arith";
  uint32_t prog[6];
  prog[0] = enc_load_col(NDB_TYPE_UNSIGNED, /*reg=*/0, /*col=*/0);
  prog[1] = enc_load_const(NDB_TYPE_BIGINT, /*reg=*/1);
  prog[2] = 1u;          /* value 1, low word */
  prog[3] = 0u;          /* high word */
  prog[4] = enc_2reg(kOpPlusBigint, 0, 1);
  prog[5] = enc_sum(/*reg=*/0, /*agg=*/0);
  Program p;
  /* [0]U64_NB(c=4) [1]CONST [2]ADD_U64 [3]SUM_U64 [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 6, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_U64_NB)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_ADD_U64_CHECKED)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_SUM_U64_CHECKED)) return;
  mark_pass(name);
}

/* ------------------------------------------------------------------ */
/* Phase 5E-1: GENERIC arithmetic (RonSQL's NdbAggregator::Add/Minus/ */
/* Mul emit untyped kOpPlus/kOpMinus/kOpMul). Lowered by the          */
/* tracker's proof: both-f64 -> F64 stencils; integer tracks -> the   */
/* 5C-4 classifier; UNKNOWN / STR / mixed int-double / u64-with-      */
/* signed-variable -> UNSUPPORTED (missing conversion, not a type     */
/* bug — the kernel handles those shapes, the lowering doesn't).      */
/* ------------------------------------------------------------------ */

/* T62a: generic +,-,* over two BIGINT loads -> checked signed. */
static void test_generic_arith_i64(void) {
  const char *name = "T62a generic_arith_i64";
  uint32_t prog[6] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/1, /*col=*/1),
    enc_2reg(kOpPlus, 0, 1),
    enc_2reg(kOpMinus, 0, 1),
    enc_2reg(kOpMul, 0, 1),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]load r0 [1]load r1 [2]ADD [3]MINUS [4]MUL [5]SUM [6]EXIT
   * [7]OVERFLOW_EXIT (loads convert/degrade per the 5D-1 rules). */
  if (!expect_accepted(name, prog, 6, &p, /*expected_n_ops=*/8)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_ADD_INT_INT_CHECKED)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_MINUS_INT_INT_CHECKED)) return;
  if (!expect_op_field(name, &p, 4, "kind", p.ops[4].kind,
                       OP_MUL_INT_INT_CHECKED)) return;
  if (!expect_op_field(name, &p, 5, "kind", p.ops[5].kind,
                       OP_SUM_BIGINT_CHECKED)) return;
  mark_pass(name);
}

/* T62b: generic +,* and generic '/' over two DOUBLE loads -> the F64
 * stencils (kOpDiv-all-double was already lowered; the +,* forms are
 * new). Generic Sum over the f64 result -> SUM_F64. */
static void test_generic_arith_f64(void) {
  const char *name = "T62b generic_arith_f64";
  uint32_t prog[6] = {
    enc_load_col(NDB_TYPE_DOUBLE, /*reg=*/0, /*col=*/2),
    enc_load_col(NDB_TYPE_DOUBLE, /*reg=*/1, /*col=*/3),
    enc_2reg(kOpPlus, 0, 1),
    enc_2reg(kOpMul, 0, 1),
    enc_2reg(kOpDiv, 0, 1),
    enc_agg(kOpSum, /*reg=*/0, /*agg=*/0),
  };
  Program p;
  if (!expect_accepted(name, prog, 6, &p, /*expected_n_ops=*/8)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_ADD_F64)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_MUL_F64)) return;
  if (!expect_op_field(name, &p, 4, "kind", p.ops[4].kind,
                       OP_DIV_F64)) return;
  if (!expect_op_field(name, &p, 5, "kind", p.ops[5].kind,
                       OP_SUM_F64)) return;
  mark_pass(name);
}

/* T62c: generic + over u64 column and non-negative constant -> u64
 * checked (the 5C-4 NNC rule through the generic opcode). */
static void test_generic_arith_u64_const(void) {
  const char *name = "T62c generic_arith_u64_const";
  uint32_t prog[6];
  prog[0] = enc_load_col(NDB_TYPE_BIGUNSIGNED, /*reg=*/0, /*col=*/0);
  prog[1] = enc_load_const(NDB_TYPE_BIGINT, /*reg=*/1);
  prog[2] = 5u;
  prog[3] = 0u;
  prog[4] = enc_2reg(kOpPlus, 0, 1);
  prog[5] = enc_sum(/*reg=*/0, /*agg=*/0);
  Program p;
  /* [0]U64_NB [1]CONST [2]ADD_U64 [3]SUM_U64 [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 6, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_ADD_U64_CHECKED)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_SUM_U64_CHECKED)) return;
  mark_pass(name);
}

/* T62d: generic + over mixed int/double operands — REJECTED until
 * Phase 5I, now the conversion cold call (T66a-c carry the detailed
 * packed-operand assertions; this pins the former reject boundary). */
static void test_generic_arith_mixed(void) {
  const char *name = "T62d generic_arith_mixed";
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_BIGINT, 0, 0),
    enc_load_col(NDB_TYPE_DOUBLE, 1, 2),
    enc_2reg(kOpPlus, 0, 1),
  };
  Program p;
  /* [0]load [1]load [2]ARITH_CONV [3]EXIT. */
  if (!expect_accepted(name, prog, 3, &p, /*expected_n_ops=*/4)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_ARITH_CONV_F64)) return;
  mark_pass(name);
}

/* T62e: generic + over u64 and a signed VARIABLE -> UNSUPPORTED
 * (same mixed-track rule as the typed family, softer reason). */
static void test_generic_arith_u64_signed_reject(void) {
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_BIGUNSIGNED, 0, 0),
    enc_load_col(NDB_TYPE_BIGINT, 1, 1),
    enc_2reg(kOpPlus, 0, 1),
  };
  assert_rejected("T62e generic_arith_u64_signed_reject", prog, 3,
                  JIT_BRIDGE_UNSUPPORTED_OP, 2, kOpPlus);
}

/* T62f: generic + over UNKNOWN registers (invalidated by an embedded
 * block) -> UNSUPPORTED. An untyped op over an unproven register
 * could be int or double — the typed families tolerate UNKNOWN
 * because their opcode IS the type proof; the generic ones cannot. */
static void test_generic_arith_unknown_reject(void) {
  uint32_t prog[5] = {
    enc_load_col(NDB_TYPE_BIGINT, 0, 0),
    enc_load_col(NDB_TYPE_BIGINT, 1, 1),
    enc_op(kOpEmbeddedInterp, /*emb_len=*/0),
    enc_2reg(kOpPlus, 0, 1),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  assert_rejected("T62f generic_arith_unknown_reject", prog, 5,
                  JIT_BRIDGE_UNSUPPORTED_OP, 3, kOpPlus);
}

/* T62g: THE LIVE PATH for the generic case — DECIMAL operands. The
 * data node's OptimizeProgramBuffer rewrites generic arith to the
 * typed kOp*Bigint/kOp*Double forms BEFORE compilation whenever both
 * operand types are statically known — but it types DECIMAL loads as
 * UNDEFINED, so decimal arithmetic reaches the bridge GENERIC. The
 * bridge's 5G decimal loads know better (I64/U64/F64 by scale), so
 * the generic case lowers what the optimizer could not type:
 * scale>0 pair -> F64 arith; scale-0 pair -> checked signed arith. */
static void test_generic_arith_decimal(void) {
  const char *name = "T62g generic_arith_decimal";
  uint32_t prog[7] = {
    enc_load_col(NDB_TYPE_DECIMAL, /*reg=*/0, /*col=*/2),
    enc_dec_info(9, 2),
    enc_load_col(NDB_TYPE_DECIMAL, /*reg=*/1, /*col=*/3),
    enc_dec_info(9, 2),
    enc_2reg(kOpMul, 0, 1),
    enc_2reg(kOpPlus, 0, 1),
    enc_agg(kOpSum, /*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]LCD [1]LCD [2]MUL_F64 [3]ADD_F64 [4]SUM_F64 [5]EXIT [6]OVF. */
  if (!expect_accepted(name, prog, 7, &p, /*expected_n_ops=*/7)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_MUL_F64)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_ADD_F64)) return;
  if (!expect_op_field(name, &p, 4, "kind", p.ops[4].kind,
                       OP_SUM_F64)) return;
  mark_pass(name);
}

/* T62h: scale-0 DECIMAL pair -> the generic op lands in the checked
 * signed integer track. */
static void test_generic_arith_decimal_scale0(void) {
  const char *name = "T62h generic_arith_decimal_scale0";
  uint32_t prog[6] = {
    enc_load_col(NDB_TYPE_DECIMAL, /*reg=*/0, /*col=*/2),
    enc_dec_info(9, 0),
    enc_load_col(NDB_TYPE_DECIMAL, /*reg=*/1, /*col=*/3),
    enc_dec_info(9, 0),
    enc_2reg(kOpPlus, 0, 1),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]LCD [1]LCD [2]ADD_CHECKED [3]SUM_CHECKED [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 6, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_ADD_INT_INT_CHECKED)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_SUM_BIGINT_CHECKED)) return;
  mark_pass(name);
}

/* ------------------------------------------------------------------ */
/* Phase 5E-2: integer DIV / MOD. kOpDivIntBigint is the optimizer's  */
/* typed rewrite of kOpDivInt; kOpMod is never rewritten. Signed DIV  */
/* carries the INT64_MIN/-1 overflow target in d; MOD and the u64     */
/* pair have none. Divisor 0 is a runtime per-row fallback inside the */
/* stencils — invisible to admission.                                 */
/* ------------------------------------------------------------------ */

/* T63a: signed DIV -> OP_DIV_INT_CHECKED with the d-target fixup. */
static void test_div_int_signed(void) {
  const char *name = "T63a div_int_signed";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/1, /*col=*/1),
    enc_2reg(kOpDivInt, 0, 1),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]load [1]load [2]DIV_INT_CHECKED [3]SUM_CHECKED [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_DIV_INT_CHECKED)) return;
  if (!expect_op_field(name, &p, 2, "a", p.ops[2].a, 0)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c, 1)) return;
  if (!expect_op_field(name, &p, 2, "d", p.ops[2].d, 5)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_SUM_BIGINT_CHECKED)) return;
  mark_pass(name);
}

/* T63b: the typed kOpDivIntBigint lowers identically. */
static void test_div_int_bigint_typed(void) {
  const char *name = "T63b div_int_bigint_typed";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/1, /*col=*/1),
    enc_2reg(kOpDivIntBigint, 0, 1),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_DIV_INT_CHECKED)) return;
  if (!expect_op_field(name, &p, 2, "d", p.ops[2].d, 5)) return;
  mark_pass(name);
}

/* T63c: signed MOD -> OP_MOD_INT, no overflow target. */
static void test_mod_int_signed(void) {
  const char *name = "T63c mod_int_signed";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/1, /*col=*/1),
    enc_2reg(kOpMod, 0, 1),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_MOD_INT)) return;
  if (!expect_op_field(name, &p, 2, "d", p.ops[2].d, 0)) return;
  mark_pass(name);
}

/* T63d: u64 dividend, NNC divisor -> OP_DIV_U64 (no overflow) and
 * OP_MOD_U64; the u64 result feeds SUM_U64. */
static void test_div_mod_u64(void) {
  const char *name = "T63d div_mod_u64";
  uint32_t prog[6];
  prog[0] = enc_load_col(NDB_TYPE_BIGUNSIGNED, /*reg=*/0, /*col=*/0);
  prog[1] = enc_load_const(NDB_TYPE_BIGINT, /*reg=*/1);
  prog[2] = 7u;
  prog[3] = 0u;
  prog[4] = enc_2reg(kOpDivInt, 0, 1);
  prog[5] = enc_agg(kOpSum, /*reg=*/0, /*agg=*/0);
  Program p;
  /* [0]U64_NB [1]CONST [2]DIV_U64 [3]SUM_U64 [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 6, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_DIV_U64)) return;
  if (!expect_op_field(name, &p, 2, "d", p.ops[2].d, 0)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_SUM_U64_CHECKED)) return;
  mark_pass(name);
}

/* T63e: MOD's result signedness follows the DIVIDEND — an NNC
 * dividend with a u64 divisor computes on the unsigned stencil but
 * lands the result in the SIGNED track (kernel: a_unsigned only). */
static void test_mod_dividend_signedness(void) {
  const char *name = "T63e mod_dividend_signedness";
  uint32_t prog[6];
  prog[0] = enc_load_const(NDB_TYPE_BIGINT, /*reg=*/0);
  prog[1] = 100u;
  prog[2] = 0u;
  prog[3] = enc_load_col(NDB_TYPE_BIGUNSIGNED, /*reg=*/1, /*col=*/1);
  prog[4] = enc_2reg(kOpMod, 0, 1);
  prog[5] = enc_agg(kOpSum, /*reg=*/0, /*agg=*/0);
  Program p;
  /* [0]CONST [1]U64_NB [2]MOD_U64 [3]SUM_BIGINT_CHECKED [4]EXIT
   * [5]OVF — the SUM kind is the assertion: dividend-signed result. */
  if (!expect_accepted(name, prog, 6, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_MOD_U64)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_SUM_BIGINT_CHECKED)) return;
  mark_pass(name);
}

/* T63f: DIV/MOD over doubles (trunc-DIV / fmod) stay unsupported. */
static void test_div_mod_f64_reject(void) {
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_DOUBLE, 0, 2),
    enc_load_col(NDB_TYPE_DOUBLE, 1, 3),
    enc_2reg(kOpMod, 0, 1),
  };
  assert_rejected("T63f div_mod_f64_reject", prog, 3,
                  JIT_BRIDGE_UNSUPPORTED_OP, 2, kOpMod);
}

/* T63g: u64 mixed with a signed VARIABLE keeps the fallback. */
static void test_div_u64_signed_reject(void) {
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_BIGUNSIGNED, 0, 0),
    enc_load_col(NDB_TYPE_BIGINT, 1, 1),
    enc_2reg(kOpDivInt, 0, 1),
  };
  assert_rejected("T63g div_u64_signed_reject", prog, 3,
                  JIT_BRIDGE_UNSUPPORTED_OP, 2, kOpDivInt);
}

/* ------------------------------------------------------------------ */
/* Phase 5E-3: GENERIC '/' with integer operand(s) — OP_DIV_CONV_F64  */
/* (cold call; ±2^53 conversion guards + divide, edges via per-row    */
/* fallback). Both-proven-F64 stays on the hot OP_DIV_F64.            */
/* packed(op->c) = (flags << 8) | (dst << 4) | src.                   */
/* ------------------------------------------------------------------ */

/* T64a: i64 / i64 -> conversion divide, result lands F64 (generic
 * Sum picks SUM_F64). */
static void test_div_conv_i64(void) {
  const char *name = "T64a div_conv_i64";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/1, /*col=*/1),
    enc_2reg(kOpDiv, 0, 1),
    enc_agg(kOpSum, /*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]load [1]load [2]DIV_CONV [3]SUM_F64 [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_DIV_CONV_F64)) return;
  if (!expect_op_field(name, &p, 2, "a", p.ops[2].a, 0)) return;
  if (!expect_op_field(name, &p, 2, "b", p.ops[2].b, 1)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c,
                       (0x0 << 8) | (0 << 4) | 1)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_SUM_F64)) return;
  mark_pass(name);
}

/* T64b: u64 / NNC -> flags mark the u64 dividend (bit1). */
static void test_div_conv_u64_const(void) {
  const char *name = "T64b div_conv_u64_const";
  uint32_t prog[5];
  prog[0] = enc_load_col(NDB_TYPE_BIGUNSIGNED, /*reg=*/0, /*col=*/0);
  prog[1] = enc_load_const(NDB_TYPE_BIGINT, /*reg=*/1);
  prog[2] = 3u;
  prog[3] = 0u;
  prog[4] = enc_2reg(kOpDiv, 0, 1);
  Program p;
  /* [0]U64 load (stays row-fallback form: no consumer chain to skip
   * matters here) [1]CONST [2]DIV_CONV [3]EXIT. */
  if (!expect_accepted(name, prog, 5, &p, /*expected_n_ops=*/4)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_DIV_CONV_F64)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c,
                       (0x2 << 8) | (0 << 4) | 1)) return;
  mark_pass(name);
}

/* T64c: mixed f64 / i64 -> conversion divide with the f64 flag on
 * the dividend (previously UNSUPPORTED). */
static void test_div_conv_mixed(void) {
  const char *name = "T64c div_conv_mixed";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_DOUBLE, /*reg=*/0, /*col=*/2),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/1, /*col=*/1),
    enc_2reg(kOpDiv, 0, 1),
    enc_agg(kOpSum, /*reg=*/0, /*agg=*/0),
  };
  Program p;
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_DIV_CONV_F64)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c,
                       (0x1 << 8) | (0 << 4) | 1)) return;
  mark_pass(name);
}

/* T64d: UNKNOWN operand (post-embedded) still rejects. */
static void test_div_conv_unknown_reject(void) {
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_BIGINT, 0, 0),
    enc_load_col(NDB_TYPE_BIGINT, 1, 1),
    enc_op(kOpEmbeddedInterp, /*emb_len=*/0),
    enc_2reg(kOpDiv, 0, 1),
  };
  assert_rejected("T64d div_conv_unknown_reject", prog, 4,
                  JIT_BRIDGE_UNSUPPORTED_OP, 3, kOpDiv);
}

/* T64e: both-F64 keeps the HOT OP_DIV_F64 (regression pin). */
static void test_div_conv_both_f64_hot(void) {
  const char *name = "T64e div_conv_both_f64_hot";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_DOUBLE, /*reg=*/0, /*col=*/2),
    enc_load_col(NDB_TYPE_DOUBLE, /*reg=*/1, /*col=*/3),
    enc_2reg(kOpDiv, 0, 1),
    enc_agg(kOpSum, /*reg=*/0, /*agg=*/0),
  };
  Program p;
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_DIV_F64)) return;
  mark_pass(name);
}

/* ------------------------------------------------------------------ */
/* Phase 5F-2: LINKED columns (join aggregation's parent-table / CTE  */
/* attributes) — bit 15 of the col field marks them; the bridge now   */
/* admits every type family with the flag, and the load helpers walk  */
/* the linked buffer at runtime.                                      */
/* ------------------------------------------------------------------ */

/* T65a: linked BIGINT load + Sum — the flagged col id rides the same
 * 16-bit operand into the (NB-converted) load. */
static void test_linked_i64_load(void) {
  const char *name = "T65a linked_i64_load";
  uint32_t prog[2] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0x8003),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  if (!expect_accepted(name, prog, 2, &p, /*expected_n_ops=*/4)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_NB)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 0x8003)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_SUM_BIGINT_CHECKED)) return;
  mark_pass(name);
}

/* T65b: linked DOUBLE, DECIMAL (2-word) and BIGUNSIGNED loads route
 * to their family stencils with the flag intact. */
static void test_linked_families(void) {
  const char *name = "T65b linked_families";
  uint32_t prog[7];
  prog[0] = enc_load_col(NDB_TYPE_DOUBLE, /*reg=*/0, /*col=*/0x8001);
  prog[1] = enc_load_col(NDB_TYPE_DECIMAL, /*reg=*/1, /*col=*/0x8002);
  prog[2] = enc_dec_info(9, 2);
  prog[3] = enc_load_col(NDB_TYPE_BIGUNSIGNED, /*reg=*/2, /*col=*/0x8004);
  prog[4] = enc_agg(kOpSum, /*reg=*/0, /*agg=*/0);
  prog[5] = enc_agg(kOpSum, /*reg=*/1, /*agg=*/1);
  prog[6] = enc_agg(kOpSum, /*reg=*/2, /*agg=*/2);
  Program p;
  /* [0]F64 load [1]LCD [2]U64 load [3]SUM_F64 [4]SUM_F64(dec s2)
   * [5]SUM_U64 [6]EXIT [7]OVF. The three consumer chains INTERLEAVE,
   * so each load's 5D-1 skip range contains another chain's live ops
   * — the loads correctly DEGRADE to their row-fallback forms (T50's
   * interleaved-degradation rule), whose operand layout carries the
   * flagged col in c. */
  if (!expect_accepted(name, prog, 7, &p, /*expected_n_ops=*/8)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_F64)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 0x8001)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_LOAD_COL_NDB_DEC)) return;
  if (!expect_op_field(name, &p, 1, "c", p.ops[1].c, 0x8002)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_LOAD_COL_NDB_U64)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c, 0x8004)) return;
  mark_pass(name);
}

/* T65c: linked STRING MIN/MAX fuses with the flag in the fused op's
 * col operand — the helper routes to the linked facade at runtime. */
static void test_linked_string_fusion(void) {
  const char *name = "T65c linked_string_fusion";
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_VARCHAR, /*reg=*/0, /*col=*/0x8002),
    enc_agg(kOpMin, /*reg=*/0, /*agg=*/0),
    enc_agg(kOpMax, /*reg=*/0, /*agg=*/1),
  };
  Program p;
  /* [0]MINMAX_STR(b=0x8002,c=0x000) [1]MINMAX_STR(c=0x101) [2]EXIT. */
  if (!expect_accepted(name, prog, 3, &p, /*expected_n_ops=*/3)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_MINMAX_STR_NDB)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 0x8002)) return;
  if (!expect_op_field(name, &p, 1, "b", p.ops[1].b, 0x8002)) return;
  if (!expect_op_field(name, &p, 1, "c", p.ops[1].c, 0x101)) return;
  mark_pass(name);
}

/* ------------------------------------------------------------------ */
/* Phase 5I: MIXED int/double GENERIC plus/minus/mul: OP_ARITH_CONV_F64  */
/* (cold call; plain casts + op + isfinite, errors via per-row        */
/* fallback). packed(op->c) = (sel<<12)|(flags<<8)|(dst<<4)|src.      */
/* ------------------------------------------------------------------ */

/* T66a: i64 + f64 kOpPlus -> conversion arith; generic Sum lands
 * SUM_F64 (result register is F64). */
static void test_arith_conv_mixed_plus(void) {
  const char *name = "T66a arith_conv_mixed_plus";
  uint32_t prog[4] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_load_col(NDB_TYPE_DOUBLE, /*reg=*/1, /*col=*/2),
    enc_2reg(kOpPlus, 0, 1),
    enc_agg(kOpSum, /*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]load [1]load [2]ARITH_CONV [3]SUM_F64 [4]EXIT [5]OVF. */
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_ARITH_CONV_F64)) return;
  if (!expect_op_field(name, &p, 2, "a", p.ops[2].a, 0)) return;
  if (!expect_op_field(name, &p, 2, "b", p.ops[2].b, 1)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c,
                       (0u << 12) | (0x4u << 8) | (0 << 4) | 1)) return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind,
                       OP_SUM_F64)) return;
  mark_pass(name);
}

/* T66b: the TPC-H Q9 shape — NNC const MINUS a scaled-DECIMAL (F64)
 * register, then MUL by another decimal. Both mixed ops convert. */
static void test_arith_conv_q9_shape(void) {
  const char *name = "T66b arith_conv_q9_shape";
  uint32_t prog[9];
  prog[0] = enc_load_const(NDB_TYPE_BIGINT, /*reg=*/0);
  prog[1] = 1u;
  prog[2] = 0u;
  prog[3] = enc_load_col(NDB_TYPE_DECIMAL, /*reg=*/1, /*col=*/2);
  prog[4] = enc_dec_info(9, 2);
  prog[5] = enc_2reg(kOpMinus, 0, 1);   /* NNC - F64 -> mixed conv */
  prog[6] = enc_load_col(NDB_TYPE_DECIMAL, /*reg=*/2, /*col=*/3);
  prog[7] = enc_dec_info(9, 2);
  prog[8] = enc_2reg(kOpMul, 0, 2);     /* F64 * F64 -> HOT MUL_F64 */
  Program p;
  /* [0]CONST [1]LCD [2]ARITH_CONV(sub) [3]LCD [4]MUL_F64 [5]EXIT
   * [6]OVF. */
  if (!expect_accepted(name, prog, 9, &p, /*expected_n_ops=*/7)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_ARITH_CONV_F64)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c,
                       (1u << 12) | (0x4u << 8) | (0 << 4) | 1)) return;
  if (!expect_op_field(name, &p, 4, "kind", p.ops[4].kind,
                       OP_MUL_F64)) return;
  mark_pass(name);
}

/* T66c: u64 * f64 kOpMul — the unsigned flag on the dividend side. */
static void test_arith_conv_u64_mul(void) {
  const char *name = "T66c arith_conv_u64_mul";
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_BIGUNSIGNED, /*reg=*/0, /*col=*/0),
    enc_load_col(NDB_TYPE_DOUBLE, /*reg=*/1, /*col=*/2),
    enc_2reg(kOpMul, 0, 1),
  };
  Program p;
  /* [0]load [1]load [2]ARITH_CONV [3]EXIT. */
  if (!expect_accepted(name, prog, 3, &p, /*expected_n_ops=*/4)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_ARITH_CONV_F64)) return;
  if (!expect_op_field(name, &p, 2, "c", p.ops[2].c,
                       (2u << 12) | (0x6u << 8) | (0 << 4) | 1)) return;
  mark_pass(name);
}

/* ------------------------------------------------------------------ */
/* String CASE conditions on the AGGREGATION path: embedded           */
/* BRANCH_ATTR_OP_ARG is now admitted with its op->b as the ABSOLUTE  */
/* word offset (embedded base = header pos + 1), matching the         */
/* dispatch's ctx->prog_buf = program + agg_prog_start_pos.           */
/* ------------------------------------------------------------------ */

/* T67a: embedded string condition at program start — b = 0 + 1 + 0. */
static void test_agg_attr_op_arg_start(void) {
  const char *name = "T67a agg_attr_op_arg_start";
  uint32_t prog[9] = {
    enc_op(kOpEmbeddedInterp, /*emb_len=*/6),
    enc_emb_op_word(EMB_BRANCH_ATTR_OP_ARG,
                    (3u << 6) | (0u << 12) | (5u << 16)),  /* EQ -> +5 */
    (1u << 16) | 8u,           /* attrId 1, byte len 8 (2 data words) */
    0x00000005u,
    0x00000000u,
    enc_emb_op_word(EMB_EXIT_REFUSE, (626u << 16)),
    enc_emb_op_word(EMB_EXIT_OK, 0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_sum(/*reg=*/0, /*agg=*/0),
  };
  Program p;
  /* [0]BRANCH_ATTR_OP_ARG(b=1,c=2) [1]EXIT(refuse) [2..]outer. */
  if (!expect_accepted(name, prog, 9, &p, /*expected_n_ops=*/6)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_BRANCH_ATTR_OP_ARG)) return;
  if (!expect_op_field(name, &p, 0, "b", p.ops[0].b, 1)) return;
  if (!expect_op_field(name, &p, 0, "c", p.ops[0].c, 2)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind, OP_EXIT)) return;
  mark_pass(name);
}

/* T67b: embedded block mid-program — the base tracks the block's
 * position (header at word 2 -> b = 3 + emb_pc). */
static void test_agg_attr_op_arg_mid(void) {
  const char *name = "T67b agg_attr_op_arg_mid";
  uint32_t prog[11] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/0, /*col=*/0),
    enc_sum(/*reg=*/0, /*agg=*/0),
    enc_op(kOpEmbeddedInterp, /*emb_len=*/6),
    enc_emb_op_word(EMB_BRANCH_ATTR_OP_ARG,
                    (3u << 6) | (0u << 12) | (5u << 16)),
    (2u << 16) | 8u,
    0x00000005u,
    0x00000000u,
    enc_emb_op_word(EMB_EXIT_REFUSE, (626u << 16)),
    enc_emb_op_word(EMB_EXIT_OK, 0),
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/1, /*col=*/1),
    enc_sum(/*reg=*/1, /*agg=*/1),
  };
  Program p;
  /* [0]load [1]SUM [2]BRANCH_ATTR_OP_ARG(b=3) [3]EXIT [4]load [5]SUM
   * [6]EXIT [7]OVF. */
  if (!expect_accepted(name, prog, 11, &p, /*expected_n_ops=*/8)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind,
                       OP_BRANCH_ATTR_OP_ARG)) return;
  if (!expect_op_field(name, &p, 2, "b", p.ops[2].b, 3)) return;
  mark_pass(name);
}

/* ------------------------------------------------------------------ */
/* Phase 5K: TEMPORAL MIN/MAX — DATE/YEAR/TIME2/DATETIME2/TIMESTAMP2  */
/* ride the u64 track (packed values, unsigned order = chronological  */
/* order). The >= 32 ids exercise the wire's 6-bit type encoding      */
/* (bit 20 extension).                                                */
/* ------------------------------------------------------------------ */
static uint32_t enc_load_col_wide(uint32_t type, uint32_t reg_index,
                                  uint32_t col_index) {
  return enc_op(kOpLoadCol,
                ((type & 0x1Fu) << 21) | (((type >> 5) & 0x1u) << 20) |
                ((reg_index & 0x0Fu) << 16) | (col_index & 0xFFFFu));
}

/* T68a: DATE MIN/MAX -> the unsigned accumulators over the packed
 * value; the load rides the u64 family (NB-converted). */
static void test_temporal_date_min_max(void) {
  const char *name = "T68a temporal_date_min_max";
  uint32_t prog[3] = {
    enc_load_col(NDB_TYPE_DATE, /*reg=*/0, /*col=*/1),
    enc_agg(kOpMin, /*reg=*/0, /*agg=*/0),
    enc_agg(kOpMax, /*reg=*/0, /*agg=*/1),
  };
  Program p;
  /* [0]U64_NB [1]MIN_U64 [2]MAX_U64 [3]EXIT. */
  if (!expect_accepted(name, prog, 3, &p, /*expected_n_ops=*/4)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_U64_NB)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind, OP_MIN_U64))
    return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind, OP_MAX_U64))
    return;
  mark_pass(name);
}

/* T68b: DATETIME2 (type id 32 — the bit-20 wide encoding) and
 * TIMESTAMP2 (33) both route u64. */
static void test_temporal_wide_types(void) {
  const char *name = "T68b temporal_wide_types";
  uint32_t prog[4] = {
    enc_load_col_wide(NDB_TYPE_DATETIME2, /*reg=*/0, /*col=*/2),
    enc_load_col_wide(NDB_TYPE_TIMESTAMP2, /*reg=*/1, /*col=*/3),
    enc_agg(kOpMin, /*reg=*/0, /*agg=*/0),
    enc_agg(kOpMax, /*reg=*/1, /*agg=*/1),
  };
  Program p;
  /* [0]u64 load [1]u64 load [2]MIN_U64 [3]MAX_U64 [4]EXIT — the
   * interleaved chains degrade the loads to row-fallback forms. */
  if (!expect_accepted(name, prog, 4, &p, /*expected_n_ops=*/5)) return;
  if (!expect_op_field(name, &p, 0, "kind", p.ops[0].kind,
                       OP_LOAD_COL_NDB_U64)) return;
  if (!expect_op_field(name, &p, 1, "kind", p.ops[1].kind,
                       OP_LOAD_COL_NDB_U64)) return;
  if (!expect_op_field(name, &p, 2, "kind", p.ops[2].kind, OP_MIN_U64))
    return;
  if (!expect_op_field(name, &p, 3, "kind", p.ops[3].kind, OP_MAX_U64))
    return;
  mark_pass(name);
}

int main(void) {
  printf("RONDB-1056 Phase 4 — bridge_tests\n");
  printf("=================================\n");

  test_empty_accept();
  test_simple_sum_accept();
  test_arithmetic_battery_accept();
  test_load_const_accept();
  test_load_col_255_accept();
  test_load_col_256_accept();
  test_load_col_4095_accept();
  test_load_col_4096_reject();
  test_embedded_interp_reject();
  test_div_reject();
  test_load_const_double_accept();
  test_load_const_unsupported_type_reject();
  test_load_const_truncated_reject();
  test_reg_oor_reject();
  test_set_reg_null_reject();
  test_embedded_empty_accept();
  test_embedded_attr_ne_null_accept();
  test_embedded_backward_reject();
  test_embedded_attr_max_accept();
  test_embedded_attr_pseudo_reject();
  test_embedded_attr_linked_flag_reject();
  test_embedded_too_large_reject();
  test_embedded_linked_ne_null_accept();
  test_embedded_linked_eq_null_accept();
  test_embedded_linked_backward_reject();
  test_embedded_linked_accept_path();
  test_embedded_case_skip_offset_accept();
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
  test_scan_filter_attr_ne_null_accept();
  test_scan_filter_linked_reject();
  test_scan_filter_write_output_reject();
  test_scan_filter_exit_ok_before_refuse_accept();
  test_scan_filter_captures_refuse_code();
  test_scan_filter_mixed_refuse_codes_reject();
  test_scan_filter_attr_op_arg_lower();
  test_scan_filter_attr_op_arg_like_reject();
  test_scan_filter_attr_op_param_lower();
  test_scan_filter_attr_op_attr_lower();
  test_scan_filter_exit_ok_last_reject();
  test_count_lowering_accept();
  test_count_slot_oor_reject();
  test_case_condition_family_accept();
  test_load_const64_lowering();
  test_scan_filter_reg_ops_reject();
  test_reg_reg_backward_reject();
  test_skip_lowers_to_jump();
  test_min_max_lowering_accept();
  test_min_slot_oor_reject();
  test_double_family_lowering_accept();
  test_generic_div_f64_lowering_accept();
  test_type_mismatch_f64_into_i64_sum();
  test_type_mismatch_i64_into_f64_sum();
  test_type_mismatch_f64_into_i64_arith();
  test_type_mismatch_i64_into_f64_arith();
  test_mov_preserves_f64_accept();
  test_avg_double_shape_accept();
  test_embedded_invalidates_f64_reject();
  test_u64_lowering_accept();
  test_u64_const_accept();
  test_u64_into_arith_reject();
  test_u64_mixed_acc_family_reject();
  test_u64_count_accept();
  test_mov_preserves_u64_accept();
  test_nb_expression_chain();
  test_nb_interleaved_degradation();
  test_nb_branch_in_range_degradation();
  test_nb_count_in_range();
  test_nb_reload_same_register();
  test_nb_f64_simple();
  test_nb_u64_simple();
  test_u64_arith_mixed_const();
  test_u64_arith_const_first();
  test_u64_arith_negative_const_reject();
  test_nnc_arith_stays_signed();
  test_dec_scale0_sum();
  test_dec_scale2_aggregates();
  test_dec_unsigned_scale0_sum();
  test_dec_truncated_reject();
  test_generic_sum_unknown_reject();
  test_dec_bad_info_reject();
  test_str_min_fusion();
  test_str_min_max_fusion();
  test_str_mixed_program();
  test_str_non_minmax_consumer_reject();
  test_str_dangling_load_reject();
  test_nb_case_guard_fusion();
  test_nb_case_guard_into_output_block();
  test_nb_case_guard_no_read_attr_reject();
  test_nb_case_guard_reg_mismatch_reject();
  test_nb_case_guard_null_path_reads_reject();
  test_nb_case_guard_two_conditions();
  test_narrow_int_sum();
  test_narrow_unsigned_aggregates();
  test_narrow_signed_min_max();
  test_narrow_mixed_arith_reject();
  test_narrow_signed_arith();
  test_narrow_unsigned_const_arith();
  test_generic_arith_i64();
  test_generic_arith_f64();
  test_generic_arith_u64_const();
  test_generic_arith_mixed();
  test_generic_arith_u64_signed_reject();
  test_generic_arith_unknown_reject();
  test_generic_arith_decimal();
  test_generic_arith_decimal_scale0();
  test_div_int_signed();
  test_div_int_bigint_typed();
  test_mod_int_signed();
  test_div_mod_u64();
  test_mod_dividend_signedness();
  test_div_mod_f64_reject();
  test_div_u64_signed_reject();
  test_div_conv_i64();
  test_div_conv_u64_const();
  test_div_conv_mixed();
  test_div_conv_unknown_reject();
  test_div_conv_both_f64_hot();
  test_linked_i64_load();
  test_linked_families();
  test_linked_string_fusion();
  test_arith_conv_mixed_plus();
  test_arith_conv_q9_shape();
  test_arith_conv_u64_mul();
  test_agg_attr_op_arg_start();
  test_agg_attr_op_arg_mid();
  test_temporal_date_min_max();
  test_temporal_wide_types();

  printf("\nbridge_tests: %d/%d passed\n", n_pass, n_pass + n_fail);
  return n_fail == 0 ? 0 : 1;
}
