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

#include "bytecode1.h"
#include "ndb_jit_bridge.h"

#include <inttypes.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

/* T5: kOpEmbeddedInterp — must reject. */
static void test_embedded_interp_reject(void) {
  uint32_t prog[1] = { enc_op(kOpEmbeddedInterp, 0) };
  assert_rejected("T5 embedded_interp_reject", prog, 1,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpEmbeddedInterp);
}

/* T6: kOpDiv — must reject (no division support in Phase 4). */
static void test_div_reject(void) {
  uint32_t prog[1] = { enc_2reg(kOpDiv, 0, 1) };
  assert_rejected("T6 div_reject", prog, 1,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpDiv);
}

/* T7: kOpLoadConst with NDB_TYPE_DOUBLE — must reject (non-bigint). */
static void test_load_const_double_reject(void) {
  uint32_t prog[3] = {
    enc_load_const(NDB_TYPE_DOUBLE, 0),
    0x00000000u, 0x40000000u,   /* doesn't matter */
  };
  assert_rejected("T7 load_const_double_reject", prog, 3,
                   JIT_BRIDGE_NON_BIGINT, 0, kOpLoadConst);
}

/* T8: kOpLoadConst truncated — only 1 word where 3 are needed. */
static void test_load_const_truncated_reject(void) {
  uint32_t prog[1] = { enc_load_const(NDB_TYPE_BIGINT, 0) };
  assert_rejected("T8 load_const_truncated_reject", prog, 1,
                   JIT_BRIDGE_MALFORMED, 0, kOpLoadConst);
}

/* T9: register index out of range. */
static void test_reg_oor_reject(void) {
  /* BC_MAX_REGS is 8; reg index 9 is out of range. */
  uint32_t prog[1] = {
    enc_load_col(NDB_TYPE_BIGINT, /*reg=*/9, /*col=*/0)
  };
  assert_rejected("T9 reg_oor_reject", prog, 1,
                   JIT_BRIDGE_REG_OUT_OF_RANGE, 0, kOpLoadCol);
}

/* T10: kOpSetRegNull — Phase 4 doesn't admit nullable handling. */
static void test_set_reg_null_reject(void) {
  uint32_t prog[1] = { enc_op(kOpSetRegNull, 0) };
  assert_rejected("T10 set_reg_null_reject", prog, 1,
                   JIT_BRIDGE_UNSUPPORTED_OP, 0, kOpSetRegNull);
}

int main(void) {
  printf("RONDB-1056 Phase 4 — bridge_tests\n");
  printf("=================================\n");

  test_empty_accept();
  test_simple_sum_accept();
  test_arithmetic_battery_accept();
  test_load_const_accept();
  test_embedded_interp_reject();
  test_div_reject();
  test_load_const_double_reject();
  test_load_const_truncated_reject();
  test_reg_oor_reject();
  test_set_reg_null_reject();

  printf("\nbridge_tests: %d/%d passed\n", n_pass, n_pass + n_fail);
  return n_fail == 0 ? 0 : 1;
}
