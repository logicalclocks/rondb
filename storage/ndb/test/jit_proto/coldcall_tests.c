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
 * RONDB-1056 Phase 4 Day 5 — cold-call mechanism unit tests.
 *
 * Exercises the HK_COLDCALL hole + helper registry end-to-end at
 * the JIT layer. No DBTUP / signal-flow involvement — these tests
 * register a mock helper under the production helper name
 * "ndb_jit_h_load_col", build small programs that use
 * OP_LOAD_COL_NDB, compile via jit1_compile, and run the resulting
 * native code with a hand-built JitState.
 *
 * This validates:
 *   - The HK_COLDCALL patcher computes correct PC-relative
 *     displacements (using ndb_jit_arena_exec_addr).
 *   - The helper registry resolves names → function pointers.
 *   - JitState.ctx round-trips through the JIT'd code into the
 *     helper's hands.
 *   - Cold-call values flow back into JitState.regs_i64 and on
 *     into the rest of the bytecode chain (e.g., OP_SUM_BIGINT).
 *
 * What it does NOT validate:
 *   - SETUP-signal flow (Day 6 / MTR-canary work).
 *   - real readAttributes integration (covered by ndbmtd build
 *     + future MTR test).
 */

#include "bytecode1.h"
#include "jit1.h"
#include "jit_arena.h"
#include "jit_codemem.h"

#include <errno.h>
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
/* Mock helper + per-test context.                                    */
/*                                                                    */
/* The mock has the same signature as DbtupJitGlue's                  */
/* ndb_jit_h_load_col so registering it under that name causes the    */
/* JIT'd code's cold-call site to land here at runtime.               */
/* ------------------------------------------------------------------ */

typedef struct {
  uint32_t n_calls;
  /* Last-call snapshots for caller assertions. */
  uint32_t last_col_id;
  uint32_t last_dst_reg;
  /* Phase 5D-1: makes mock_load_col_nb report NULL. */
  uint32_t nb_null;
  /* Phase 5G: last pinfo seen by mock_load_col_dec. */
  uint32_t last_pinfo;
} MockCtx;

/* Mock-A: writes col_id * 10 into dst_reg, bumps call counter. */
static void mock_load_col(JitState *s, uint32_t col_id, uint32_t dst_reg) {
  MockCtx *ctx = (MockCtx *)s->ctx;
  ctx->n_calls++;
  ctx->last_col_id = col_id;
  ctx->last_dst_reg = dst_reg;
  s->regs_i64[dst_reg] = (int64_t)col_id * 10;
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

/* Build a minimal program: OP_LOAD_COL_NDB(dst=r0, col=COL),
 * OP_SUM_BIGINT(slot=0, src=r0), OP_EXIT.
 * (Bridge isn't involved — we hand-build the Op array directly.)  */
static void build_load_col_then_sum(Program *p, uint8_t col_id) {
  memset(p, 0, sizeof(*p));
  p->n_ops = 3;
  /* OP_LOAD_COL_NDB layout: a = dst register, c = col_id. */
  p->ops[0] = (Op){ .kind = OP_LOAD_COL_NDB, .a = 0, .c = col_id };
  /* OP_SUM_BIGINT layout: a = acc slot, b = src register, c = result index. */
  p->ops[1] = (Op){ .kind = OP_SUM_BIGINT, .a = 0, .b = 0, .c = 0 };
  p->ops[2] = (Op){ .kind = OP_EXIT };
}

/* ------------------------------------------------------------------ */
/* Tests.                                                             */
/* ------------------------------------------------------------------ */

/* T1: helper-not-registered.
 *
 * Run with NO helpers registered; jit1_compile must return NULL
 * with errno=ENOENT. The arena's used-bytes high-water mark may
 * have grown (admission passes — the program is well-formed —
 * and pass-1 / pass-2 emit the bytes before failing on the
 * unresolved cold-call hole), but no NULL deref / crash. */
static void test_no_helper_registered(void) {
  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Program p;
  build_load_col_then_sum(&p, /*col_id=*/3);

  errno = 0;
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp != NULL) {
    mark_fail("T1 no_helper_registered",
              "jit1_compile returned non-NULL");
    ndb_jit_codemem_destroy(arena);
    return;
  }
  if (errno != ENOENT) {
    mark_fail("T1 no_helper_registered",
              "errno=%d, want ENOENT", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  mark_pass("T1 no_helper_registered");
  ndb_jit_codemem_destroy(arena);
}

/* T2: single-row invocation.
 *
 * Register the mock under the production helper name. Compile
 * the program. Run the JIT entry once with a fresh JitState.
 * Verify the helper was called with the expected args and its
 * value flowed into the accumulator. */
static void test_single_row(void) {
  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Program p;
  build_load_col_then_sum(&p, /*col_id=*/7);

  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail("T2 single_row",
              "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitEntry entry = jit1_entry(jp);

  MockCtx ctx = {0};
  JitState s;
  memset(&s, 0, sizeof(s));
  s.ctx = &ctx;
  entry(&s);

  if (ctx.n_calls != 1) {
    mark_fail("T2 single_row",
              "n_calls=%u, want 1", ctx.n_calls);
  } else if (ctx.last_col_id != 7) {
    mark_fail("T2 single_row",
              "last_col_id=%u, want 7", ctx.last_col_id);
  } else if (ctx.last_dst_reg != 0) {
    mark_fail("T2 single_row",
              "last_dst_reg=%u, want 0", ctx.last_dst_reg);
  } else if (s.acc_i64[0] != 70) {
    mark_fail("T2 single_row",
              "acc[0]=%" PRId64 ", want 70 (col_id 7 * 10)",
              s.acc_i64[0]);
  } else if (s.value_updated[0] != 1) {
    mark_fail("T2 single_row",
              "value_updated[0]=%" PRIu64 ", want 1", s.value_updated[0]);
  } else {
    mark_pass("T2 single_row");
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T3: multi-row accumulation.
 *
 * Run the entry 100 times with the same JitState. acc carries
 * across rows; regs reset each row (we memset them). Verify the
 * helper was called 100 times and acc holds the cumulative sum. */
static void test_multi_row(void) {
  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Program p;
  build_load_col_then_sum(&p, /*col_id=*/4);

  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail("T3 multi_row",
              "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitEntry entry = jit1_entry(jp);

  MockCtx ctx = {0};
  JitState s;
  memset(&s, 0, sizeof(s));
  s.ctx = &ctx;
  for (int i = 0; i < 100; ++i) {
    /* Per-row: clear regs but carry acc. */
    memset(s.regs_i64, 0, sizeof(s.regs_i64));
    memset(s.value_updated, 0, sizeof(s.value_updated));
    entry(&s);
  }

  if (ctx.n_calls != 100) {
    mark_fail("T3 multi_row", "n_calls=%u, want 100", ctx.n_calls);
  } else if (s.acc_i64[0] != 100 * 40) {
    mark_fail("T3 multi_row",
              "acc[0]=%" PRId64 ", want %d (100 rows * 40/row)",
              s.acc_i64[0], 100 * 40);
  } else if (s.value_updated[0] != 1) {
    mark_fail("T3 multi_row",
              "value_updated[0]=%" PRIu64 ", want 1", s.value_updated[0]);
  } else {
    mark_pass("T3 multi_row");
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T4: cold-call combined with hot arithmetic.
 *
 * Program:
 *   load_col_ndb r0, col=5      ; r0 = 50
 *   load_col_ndb r1, col=7      ; r1 = 70
 *   add_int_int  r2, r0, r1     ; r2 = 120
 *   sum_bigint   acc[0], r2     ; acc += 120
 *   exit
 *
 * Verifies that the cold-call result feeds into the next stencil
 * via JitState.regs_i64 — i.e., the value the helper writes is
 * the same value subsequent stencils read. */
static void test_coldcall_plus_arith(void) {
  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);

  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 5;
  p.ops[0] = (Op){ .kind = OP_LOAD_COL_NDB, .a = 0, .c = 5 };
  p.ops[1] = (Op){ .kind = OP_LOAD_COL_NDB, .a = 1, .c = 7 };
  p.ops[2] = (Op){ .kind = OP_ADD_INT_INT,  .a = 2, .b = 0, .c = 1 };
  p.ops[3] = (Op){ .kind = OP_SUM_BIGINT,   .a = 0, .b = 2, .c = 0 };
  p.ops[4] = (Op){ .kind = OP_EXIT };

  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail("T4 coldcall_plus_arith",
              "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitEntry entry = jit1_entry(jp);

  MockCtx ctx = {0};
  JitState s;
  memset(&s, 0, sizeof(s));
  s.ctx = &ctx;
  entry(&s);

  if (ctx.n_calls != 2) {
    mark_fail("T4 coldcall_plus_arith",
              "n_calls=%u, want 2", ctx.n_calls);
  } else if (s.acc_i64[0] != 120) {
    mark_fail("T4 coldcall_plus_arith",
              "acc[0]=%" PRId64 ", want 120 (50 + 70)",
              s.acc_i64[0]);
  } else if (s.value_updated[0] != 1) {
    mark_fail("T4 coldcall_plus_arith",
              "value_updated[0]=%" PRIu64 ", want 1", s.value_updated[0]);
  } else {
    mark_pass("T4 coldcall_plus_arith");
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T5: aggregation result update index is independent of the accumulator
 * slot operand. This matches DBTUP writeback, which is ordered by result
 * value rather than by whatever temporary slot the program used. */
static void test_sum_marks_result_index(void) {
  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);

  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 2;
  p.ops[0] = (Op){ .kind = OP_SUM_BIGINT, .a = 0, .b = 1, .c = 2 };
  p.ops[1] = (Op){ .kind = OP_EXIT };

  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail("T5 sum_marks_result_index",
              "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitEntry entry = jit1_entry(jp);

  JitState s;
  memset(&s, 0, sizeof(s));
  s.regs_i64[1] = 17;
  entry(&s);

  if (s.acc_i64[0] != 17) {
    mark_fail("T5 sum_marks_result_index",
              "acc[0]=%" PRId64 ", want 17", s.acc_i64[0]);
  } else if (s.value_updated[0] != 0 || s.value_updated[1] != 0 ||
             s.value_updated[2] != 1 || s.value_updated[3] != 0) {
    mark_fail("T5 sum_marks_result_index",
              "updated={%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64
              "}, want {0,0,1,0}",
              s.value_updated[0], s.value_updated[1],
              s.value_updated[2], s.value_updated[3]);
  } else {
    mark_pass("T5 sum_marks_result_index");
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T6: a row that exits before any aggregate opcode must not mark any
 * result value as updated. */
static void test_exit_does_not_mark_result(void) {
  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);

  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 1;
  p.ops[0] = (Op){ .kind = OP_EXIT };

  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail("T6 exit_does_not_mark_result",
              "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitEntry entry = jit1_entry(jp);

  JitState s;
  memset(&s, 0, sizeof(s));
  entry(&s);

  for (uint32_t i = 0; i < BC_MAX_ACCS; i++) {
    if (s.value_updated[i] != 0) {
      mark_fail("T6 exit_does_not_mark_result",
                "value_updated[%u]=%" PRIu64 ", want 0",
                i, s.value_updated[i]);
      jit1_free(jp);
      ndb_jit_codemem_destroy(arena);
      return;
    }
  }
  mark_pass("T6 exit_does_not_mark_result");
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T7: registry idempotency + miss.
 *
 * Re-register the same helper under the same name → no-op (rc=0).
 * Lookup an unregistered helper → NULL. */
static void test_registry_basics(void) {
  /* Same helper, same name — should succeed. */
  if (jit1_register_helper("ndb_jit_h_load_col",
                            (JitHelperFn)&mock_load_col) != 0) {
    mark_fail("T7 registry_basics",
              "re-register same name+fn returned non-zero");
    return;
  }
  if (jit1_lookup_helper("does_not_exist") != NULL) {
    mark_fail("T7 registry_basics",
              "lookup of unregistered name returned non-NULL");
    return;
  }
  if (jit1_lookup_helper("ndb_jit_h_load_col") !=
      (JitHelperFn)&mock_load_col) {
    mark_fail("T7 registry_basics",
              "lookup of registered name returned wrong fn");
    return;
  }
  mark_pass("T7 registry_basics");
}

static void test_checked_add_no_overflow(void) {
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 5;
  p.ops[0] = (Op){ .kind = OP_LOAD_CONST_UINT16, .a = 0, .imm = 40 };
  p.ops[1] = (Op){ .kind = OP_LOAD_CONST_UINT16, .a = 1, .imm = 2 };
  p.ops[2] = (Op){ .kind = OP_ADD_INT_INT_CHECKED,
                   .a = 2, .b = 0, .c = 1, .d = 4 };
  p.ops[3] = (Op){ .kind = OP_EXIT };
  p.ops[4] = (Op){ .kind = OP_OVERFLOW_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  if (arena == NULL) {
    mark_fail("T8 checked_add_no_overflow", "arena_create failed");
    return;
  }
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail("T8 checked_add_no_overflow",
              "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitState s;
  memset(&s, 0, sizeof(s));
  jit1_entry(jp)(&s);
  if (s.row_overflowed != 0 || s.regs_i64[2] != 42) {
    mark_fail("T8 checked_add_no_overflow",
              "row_overflowed=%u reg2=%lld, want 0/42",
              s.row_overflowed, (long long)s.regs_i64[2]);
    jit1_free(jp);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
  mark_pass("T8 checked_add_no_overflow");
}

static void test_checked_add_overflow(void) {
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 5;
  p.ops[0] = (Op){ .kind = OP_LOAD_CONST_INT, .a = 0, .imm = INT64_MAX };
  p.ops[1] = (Op){ .kind = OP_LOAD_CONST_UINT16, .a = 1, .imm = 1 };
  p.ops[2] = (Op){ .kind = OP_ADD_INT_INT_CHECKED,
                   .a = 2, .b = 0, .c = 1, .d = 4 };
  p.ops[3] = (Op){ .kind = OP_EXIT };
  p.ops[4] = (Op){ .kind = OP_OVERFLOW_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  if (arena == NULL) {
    mark_fail("T9 checked_add_overflow", "arena_create failed");
    return;
  }
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail("T9 checked_add_overflow",
              "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitState s;
  memset(&s, 0, sizeof(s));
  jit1_entry(jp)(&s);
  if (s.row_overflowed != 1 || s.regs_i64[2] != 0) {
    mark_fail("T9 checked_add_overflow",
              "row_overflowed=%u reg2=%lld, want 1/0",
              s.row_overflowed, (long long)s.regs_i64[2]);
    jit1_free(jp);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
  mark_pass("T9 checked_add_overflow");
}

static void test_jump_skips_sum(void) {
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 4;
  p.ops[0] = (Op){ .kind = OP_LOAD_CONST_UINT16, .a = 0, .imm = 5 };
  p.ops[1] = (Op){ .kind = OP_JUMP, .c = 3 };
  p.ops[2] = (Op){ .kind = OP_SUM_BIGINT, .a = 0, .b = 0, .c = 0 };
  p.ops[3] = (Op){ .kind = OP_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  if (arena == NULL) {
    mark_fail("T10 jump_skips_sum", "arena_create failed");
    return;
  }
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail("T10 jump_skips_sum",
              "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitState s;
  memset(&s, 0, sizeof(s));
  jit1_entry(jp)(&s);
  if (s.regs_i64[0] != 5 || s.acc_i64[0] != 0 ||
      s.value_updated[0] != 0) {
    mark_fail("T10 jump_skips_sum",
              "reg0=%lld acc0=%lld updated0=%" PRIu64 ", want 5/0/0",
              (long long)s.regs_i64[0], (long long)s.acc_i64[0],
              s.value_updated[0]);
    jit1_free(jp);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
  mark_pass("T10 jump_skips_sum");
}

static void test_filter_reject_exit_sets_state(void) {
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 1;
  p.ops[0] = (Op){ .kind = OP_FILTER_REJECT_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  if (arena == NULL) {
    mark_fail("T11 filter_reject_exit_sets_state", "arena_create failed");
    return;
  }
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail("T11 filter_reject_exit_sets_state",
              "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitState s;
  memset(&s, 0, sizeof(s));
  jit1_entry(jp)(&s);
  if (s.row_filter_rejected != 1 || s.row_overflowed != 0) {
    mark_fail("T11 filter_reject_exit_sets_state",
              "row_filter_rejected=%u row_overflowed=%u, want 1/0",
              s.row_filter_rejected, s.row_overflowed);
    jit1_free(jp);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
  mark_pass("T11 filter_reject_exit_sets_state");
}

/* T12: crash-diagnosis registry — jit1_describe_pc resolves any PC
 * inside a live blob to a JIT-CRASH line and stops resolving once the
 * program is freed; jit1_registry_dump walks the same registry.
 *
 * The dump assertions are relative (live == freed + 1 lines) rather
 * than absolute so a leaked program from another test can never break
 * this one. The describe assertions rely on registry insertion order:
 * the newest program sits at the list head, so our live blob always
 * shadows any stale leaked entry whose (dangling) range an mmap reuse
 * might alias. */
static void count_dump_line(void *arg, const char *line) {
  (void)line;
  (*(unsigned *)arg)++;
}

static void test_describe_pc_registry(void) {
  const char *name = "T12 describe_pc_registry";
  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  if (arena == NULL) {
    mark_fail(name, "arena_create failed");
    return;
  }
  Program p;
  build_load_col_then_sum(&p, /*col_id=*/7);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  const uint8_t *entry = (const uint8_t *)jit1_entry(jp);
  size_t emitted = jit1_emitted_size(jp);
  char buf[256];

  unsigned lines_live = 0;
  unsigned lines_freed = 0;
  jit1_registry_dump(count_dump_line, &lines_live);

  int ok = 1;
  if (!jit1_describe_pc(entry, buf, sizeof(buf))) {
    mark_fail(name, "entry pc not resolved");
    ok = 0;
  } else if (strstr(buf, "JIT-CRASH:") == NULL) {
    mark_fail(name, "describe line missing JIT-CRASH prefix: %s", buf);
    ok = 0;
  } else if (!jit1_describe_pc(entry + emitted - 1, buf, sizeof(buf))) {
    mark_fail(name, "last blob byte not resolved");
    ok = 0;
  } else if (jit1_describe_pc(entry + emitted, buf, sizeof(buf))) {
    mark_fail(name, "one-past-end pc wrongly resolved");
    ok = 0;
  } else if (jit1_describe_pc(&p, buf, sizeof(buf))) {
    mark_fail(name, "non-code (stack) pc wrongly resolved");
    ok = 0;
  }

  jit1_free(jp);
  if (ok && jit1_describe_pc(entry, buf, sizeof(buf))) {
    mark_fail(name, "freed blob pc still resolved");
    ok = 0;
  }
  jit1_registry_dump(count_dump_line, &lines_freed);
  if (ok && lines_live != lines_freed + 1) {
    mark_fail(name, "dump lines live=%u freed=%u, want live == freed+1",
              lines_live, lines_freed);
    ok = 0;
  }
  if (ok) {
    mark_pass(name);
  }
  ndb_jit_codemem_destroy(arena);
}

/* T13: OP_COUNT_BIGINT (Phase 8 GROUP BY lift) — acc += 1 per
 * invocation, marks BOTH value_updated and value_unsigned for its
 * result index (COUNT is an unsigned BIGINT in the interpreter), and
 * leaves other results' masks untouched. Program:
 *   count   acc[0] -> result 0
 *   sum     acc[1] += r1 -> result 1
 *   exit
 * Run 3 rows with r1 = 5: acc[0] = 3 (count), acc[1] = 15 (sum);
 * result 0 unsigned, result 1 signed. */
static void test_count_bigint(void) {
  const char *name = "T13 count_bigint";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 3;
  p.ops[0] = (Op){ .kind = OP_COUNT_BIGINT, .a = 0, .b = 0, .c = 0 };
  p.ops[1] = (Op){ .kind = OP_SUM_BIGINT,   .a = 1, .b = 1, .c = 1 };
  p.ops[2] = (Op){ .kind = OP_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  if (arena == NULL) {
    mark_fail(name, "arena_create failed");
    return;
  }
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d) — stencils regenerated?",
              errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitEntry entry = jit1_entry(jp);

  JitState s;
  memset(&s, 0, sizeof(s));
  for (int i = 0; i < 3; ++i) {
    memset(s.regs_i64, 0, sizeof(s.regs_i64));
    memset(s.value_updated, 0, sizeof(s.value_updated));
    memset(s.value_unsigned, 0, sizeof(s.value_unsigned));
    s.regs_i64[1] = 5;
    entry(&s);
  }

  if (s.acc_i64[0] != 3 || s.acc_i64[1] != 15) {
    mark_fail(name, "acc0=%lld acc1=%lld, want 3/15",
              (long long)s.acc_i64[0], (long long)s.acc_i64[1]);
  } else if (s.value_updated[0] != 1 || s.value_updated[1] != 1) {
    mark_fail(name, "value_updated={%" PRIu64 ",%" PRIu64 "}, want {1,1}",
              s.value_updated[0], s.value_updated[1]);
  } else if (s.value_unsigned[0] != 1 || s.value_unsigned[1] != 0) {
    mark_fail(name, "value_unsigned={%" PRIu64 ",%" PRIu64 "}, want {1,0}",
              s.value_unsigned[0], s.value_unsigned[1]);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* main.                                                              */
/* ------------------------------------------------------------------ */

/* T14: OP_MIN_BIGINT / OP_MAX_BIGINT (Phase 5B) — first-row
 * initialization semantics. The accumulator starts with garbage (777)
 * and value_initialized = 0: the first reaching row must OVERWRITE it
 * (a min of 0/garbage must never win). Later rows compare. The test
 * emulates the dispatch glue: value_initialized is set once the slot
 * holds a real value.
 *   ops: min acc0<-r1 (result 0), max acc1<-r1 (result 1), exit
 *   rows: 50, 10, 99  ->  min 10, max 99. */
static void test_min_max_first_row_init(void) {
  const char *name = "T14 min_max_first_row_init";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 3;
  p.ops[0] = (Op){ .kind = OP_MIN_BIGINT, .a = 0, .b = 1, .c = 0 };
  p.ops[1] = (Op){ .kind = OP_MAX_BIGINT, .a = 1, .b = 1, .c = 1 };
  p.ops[2] = (Op){ .kind = OP_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  if (arena == NULL) {
    mark_fail(name, "arena_create failed");
    return;
  }
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d) — stencils regenerated?",
              errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitEntry entry = jit1_entry(jp);

  JitState s;
  memset(&s, 0, sizeof(s));
  /* Fresh accumulators hold garbage; value_initialized = 0. */
  s.acc_i64[0] = 777;
  s.acc_i64[1] = -777;
  const int64_t rows[3] = {50, 10, 99};
  for (int i = 0; i < 3; ++i) {
    memset(s.regs_i64, 0, sizeof(s.regs_i64));
    memset(s.value_updated, 0, sizeof(s.value_updated));
    s.regs_i64[1] = rows[i];
    entry(&s);
    /* The stencil set value_initialized itself; verify after row 1. */
    if (i == 0 && (s.value_initialized[0] != 1 ||
                   s.value_initialized[1] != 1)) {
      mark_fail(name, "value_initialized={%" PRIu64 ",%" PRIu64
                "} after first row, want {1,1}",
                s.value_initialized[0], s.value_initialized[1]);
      jit1_free(jp);
      ndb_jit_codemem_destroy(arena);
      return;
    }
  }

  if (s.acc_i64[0] != 10 || s.acc_i64[1] != 99) {
    mark_fail(name, "min=%lld max=%lld, want 10/99 (first-row init must "
              "overwrite the garbage 777/-777)",
              (long long)s.acc_i64[0], (long long)s.acc_i64[1]);
  } else if (s.value_updated[0] != 1 || s.value_updated[1] != 1) {
    mark_fail(name, "value_updated={%" PRIu64 ",%" PRIu64 "}, want {1,1}",
              s.value_updated[0], s.value_updated[1]);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Phase 5C-2 — the DOUBLE family. f64 values live bit-cast in the    */
/* i64 register file / accumulators.                                  */
/* ------------------------------------------------------------------ */

static int64_t f64_bits(double d) {
  int64_t v;
  memcpy(&v, &d, sizeof(v));
  return v;
}

static double bits_f64(int64_t v) {
  double d;
  memcpy(&d, &v, sizeof(d));
  return d;
}

/* Mock for ndb_jit_h_load_col_f64: writes (double)col_id * 0.5. */
static void mock_load_col_f64(JitState *s, uint32_t col_id,
                              uint32_t dst_reg) {
  MockCtx *ctx = (MockCtx *)s->ctx;
  ctx->n_calls++;
  ctx->last_col_id = col_id;
  ctx->last_dst_reg = dst_reg;
  s->regs_i64[dst_reg] = f64_bits((double)col_id * 0.5);
}

/* T15: SUM/MIN/MAX_F64 battery with first-row initialization. The
 * accumulators start with garbage and value_initialized = 0; the
 * first row must overwrite all three (for SUM too — the kernel
 * stores the first value verbatim). Rows 1.5, -2.25, 4.0 →
 * sum 3.25, min -2.25, max 4.0. All three mark value_double. */
static void test_f64_accumulator_battery(void) {
  const char *name = "T15 f64_accumulator_battery";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 5;
  p.ops[0] = (Op){ .kind = OP_SUM_F64, .a = 0, .b = 1, .c = 0, .d = 4 };
  p.ops[1] = (Op){ .kind = OP_MIN_F64, .a = 1, .b = 1, .c = 1 };
  p.ops[2] = (Op){ .kind = OP_MAX_F64, .a = 2, .b = 1, .c = 2 };
  p.ops[3] = (Op){ .kind = OP_EXIT };
  p.ops[4] = (Op){ .kind = OP_OVERFLOW_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  if (arena == NULL) {
    mark_fail(name, "arena_create failed");
    return;
  }
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d) — stencils regenerated?",
              errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitEntry entry = jit1_entry(jp);

  JitState s;
  memset(&s, 0, sizeof(s));
  s.acc_i64[0] = 777;    /* garbage — must be overwritten, not added */
  s.acc_i64[1] = 777;
  s.acc_i64[2] = -777;
  const double rows[3] = {1.5, -2.25, 4.0};
  for (int i = 0; i < 3; ++i) {
    memset(s.regs_i64, 0, sizeof(s.regs_i64));
    memset(s.value_updated, 0, sizeof(s.value_updated));
    memset(s.value_double, 0, sizeof(s.value_double));
    s.regs_i64[1] = f64_bits(rows[i]);
    entry(&s);
  }

  if (bits_f64(s.acc_i64[0]) != 3.25 ||
      bits_f64(s.acc_i64[1]) != -2.25 ||
      bits_f64(s.acc_i64[2]) != 4.0) {
    mark_fail(name, "sum=%f min=%f max=%f, want 3.25/-2.25/4.0",
              bits_f64(s.acc_i64[0]), bits_f64(s.acc_i64[1]),
              bits_f64(s.acc_i64[2]));
  } else if (s.value_updated[0] != 1 || s.value_updated[1] != 1 ||
             s.value_updated[2] != 1) {
    mark_fail(name, "value_updated={%" PRIu64 ",%" PRIu64 ",%" PRIu64
              "}, want {1,1,1}",
              s.value_updated[0], s.value_updated[1], s.value_updated[2]);
  } else if (s.value_double[0] != 1 || s.value_double[1] != 1 ||
             s.value_double[2] != 1) {
    mark_fail(name, "value_double={%" PRIu64 ",%" PRIu64 ",%" PRIu64
              "}, want {1,1,1}",
              s.value_double[0], s.value_double[1], s.value_double[2]);
  } else if (s.row_overflowed != 0) {
    mark_fail(name, "row_overflowed=%u, want 0", s.row_overflowed);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T15b: single-row SUM_F64(-0.0) preserves the sign bit — the init
 * path stores the value verbatim (0.0 + -0.0 would yield +0.0). */
static void test_f64_sum_negative_zero_init(void) {
  const char *name = "T15b f64_sum_negative_zero_init";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 3;
  p.ops[0] = (Op){ .kind = OP_SUM_F64, .a = 0, .b = 0, .c = 0, .d = 2 };
  p.ops[1] = (Op){ .kind = OP_EXIT };
  p.ops[2] = (Op){ .kind = OP_OVERFLOW_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitState s;
  memset(&s, 0, sizeof(s));
  s.acc_i64[0] = 777;                        /* garbage */
  s.regs_i64[0] = f64_bits(-0.0);
  jit1_entry(jp)(&s);

  if ((uint64_t)s.acc_i64[0] != UINT64_C(0x8000000000000000)) {
    mark_fail(name, "acc bits=0x%" PRIx64 ", want 0x8000000000000000 "
              "(the -0.0 sign bit must survive first-row init)",
              (uint64_t)s.acc_i64[0]);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T16: f64 arithmetic chain — r2=r0+r1, r3=r2*r0, r4=r3/r1, then
 * SUM. r0=3.0, r1=2.0 → 5.0, 15.0, 7.5 → acc 7.5. */
static void test_f64_arith_chain(void) {
  const char *name = "T16 f64_arith_chain";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 6;
  p.ops[0] = (Op){ .kind = OP_ADD_F64, .a = 2, .b = 0, .c = 1, .d = 5 };
  p.ops[1] = (Op){ .kind = OP_MUL_F64, .a = 3, .b = 2, .c = 0, .d = 5 };
  p.ops[2] = (Op){ .kind = OP_DIV_F64, .a = 4, .b = 3, .c = 1, .d = 5 };
  p.ops[3] = (Op){ .kind = OP_SUM_F64, .a = 0, .b = 4, .c = 0, .d = 5 };
  p.ops[4] = (Op){ .kind = OP_EXIT };
  p.ops[5] = (Op){ .kind = OP_OVERFLOW_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitState s;
  memset(&s, 0, sizeof(s));
  s.regs_i64[0] = f64_bits(3.0);
  s.regs_i64[1] = f64_bits(2.0);
  jit1_entry(jp)(&s);

  if (bits_f64(s.regs_i64[2]) != 5.0 ||
      bits_f64(s.regs_i64[3]) != 15.0 ||
      bits_f64(s.regs_i64[4]) != 7.5 ||
      bits_f64(s.acc_i64[0]) != 7.5) {
    mark_fail(name, "r2=%f r3=%f r4=%f acc=%f, want 5/15/7.5/7.5",
              bits_f64(s.regs_i64[2]), bits_f64(s.regs_i64[3]),
              bits_f64(s.regs_i64[4]), bits_f64(s.acc_i64[0]));
  } else if (s.row_overflowed != 0 || s.row_fallback != 0) {
    mark_fail(name, "overflowed=%u fallback=%u, want 0/0",
              s.row_overflowed, s.row_fallback);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T17: non-finite result → overflow exit. DBL_MAX * 2.0 = inf; the
 * store is skipped and row_overflowed is set (⇒ ZAGG_MATH_OVERFLOW
 * in the glue). */
static void test_f64_mul_overflow(void) {
  const char *name = "T17 f64_mul_overflow";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 3;
  p.ops[0] = (Op){ .kind = OP_MUL_F64, .a = 2, .b = 0, .c = 1, .d = 2 };
  p.ops[1] = (Op){ .kind = OP_EXIT };
  p.ops[2] = (Op){ .kind = OP_OVERFLOW_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitState s;
  memset(&s, 0, sizeof(s));
  s.regs_i64[0] = f64_bits(1.7976931348623157e308);   /* DBL_MAX */
  s.regs_i64[1] = f64_bits(2.0);
  jit1_entry(jp)(&s);

  if (s.row_overflowed != 1) {
    mark_fail(name, "row_overflowed=%u, want 1", s.row_overflowed);
  } else if (s.regs_i64[2] != 0) {
    mark_fail(name, "dst reg written on overflow (bits=0x%" PRIx64 ")",
              (uint64_t)s.regs_i64[2]);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T18: divisor == 0 → per-row fallback, NOT overflow. The stencil
 * stores 0.0, sets row_fallback and CONTINUES — the downstream SUM
 * still runs (its result is discarded by the glue on fallback). */
static void test_f64_div_by_zero_fallback(void) {
  const char *name = "T18 f64_div_by_zero_fallback";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 4;
  p.ops[0] = (Op){ .kind = OP_DIV_F64, .a = 2, .b = 0, .c = 1, .d = 3 };
  p.ops[1] = (Op){ .kind = OP_SUM_F64, .a = 0, .b = 2, .c = 0, .d = 3 };
  p.ops[2] = (Op){ .kind = OP_EXIT };
  p.ops[3] = (Op){ .kind = OP_OVERFLOW_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitState s;
  memset(&s, 0, sizeof(s));
  s.regs_i64[0] = f64_bits(5.0);
  s.regs_i64[1] = f64_bits(0.0);
  jit1_entry(jp)(&s);

  if (s.row_fallback != 1) {
    mark_fail(name, "row_fallback=%u, want 1", s.row_fallback);
  } else if (s.row_overflowed != 0) {
    mark_fail(name, "row_overflowed=%u, want 0 (x/0 is fallback, "
              "not overflow)", s.row_overflowed);
  } else if (bits_f64(s.regs_i64[2]) != 0.0) {
    mark_fail(name, "dst=%f, want 0.0", bits_f64(s.regs_i64[2]));
  } else if (s.value_updated[0] != 1) {
    mark_fail(name, "downstream SUM did not run (value_updated[0]=%"
              PRIu64 ") — the blob must run to completion on fallback",
              s.value_updated[0]);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T19: OP_LOAD_COL_NDB_F64 cold-call — the helper's bits land in the
 * register file and flow into SUM_F64. col 6 → 3.0 via the mock. */
static void test_f64_load_col_coldcall(void) {
  const char *name = "T19 f64_load_col_coldcall";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 4;
  p.ops[0] = (Op){ .kind = OP_LOAD_COL_NDB_F64, .a = 0, .c = 6 };
  p.ops[1] = (Op){ .kind = OP_SUM_F64, .a = 0, .b = 0, .c = 0, .d = 3 };
  p.ops[2] = (Op){ .kind = OP_EXIT };
  p.ops[3] = (Op){ .kind = OP_OVERFLOW_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d) — f64 helper "
              "registered?", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  MockCtx ctx;
  memset(&ctx, 0, sizeof(ctx));
  JitState s;
  memset(&s, 0, sizeof(s));
  s.ctx = &ctx;
  jit1_entry(jp)(&s);

  if (ctx.n_calls != 1 || ctx.last_col_id != 6 || ctx.last_dst_reg != 0) {
    mark_fail(name, "helper calls=%u col=%u dst=%u, want 1/6/0",
              ctx.n_calls, ctx.last_col_id, ctx.last_dst_reg);
  } else if (bits_f64(s.acc_i64[0]) != 3.0) {
    mark_fail(name, "acc=%f, want 3.0", bits_f64(s.acc_i64[0]));
  } else if (s.value_double[0] != 1) {
    mark_fail(name, "value_double[0]=%" PRIu64 ", want 1",
              s.value_double[0]);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Phase 5C-3 — unsigned BIGINT.                                      */
/* ------------------------------------------------------------------ */

/* Mock for ndb_jit_h_load_col_u64: writes 2^63 + col_id (a value a
 * signed pipeline would misinterpret as negative). */
static void mock_load_col_u64(JitState *s, uint32_t col_id,
                              uint32_t dst_reg) {
  MockCtx *ctx = (MockCtx *)s->ctx;
  ctx->n_calls++;
  ctx->last_col_id = col_id;
  ctx->last_dst_reg = dst_reg;
  s->regs_i64[dst_reg] =
      (int64_t)(UINT64_C(0x8000000000000000) + col_id);
}

/* Phase 5D-1 mock for ndb_jit_h_load_col_nb: MockCtx.nb_null drives
 * the return (1 = NULL, take the branch); non-null writes col_id*10
 * like mock_load_col. */
static int mock_load_col_nb(JitState *s, uint32_t col_id,
                            uint32_t dst_reg) {
  MockCtx *ctx = (MockCtx *)s->ctx;
  ctx->n_calls++;
  ctx->last_col_id = col_id;
  ctx->last_dst_reg = dst_reg;
  if (ctx->nb_null) {
    s->regs_i64[dst_reg] = 0;
    return 1;
  }
  s->regs_i64[dst_reg] = (int64_t)col_id * 10;
  return 0;
}

/* Phase 5D-2 mocks: f64 sibling writes bits of col_id * 0.5; u64
 * sibling writes 2^63 + col_id. Both honor MockCtx.nb_null. */
static int mock_load_col_f64_nb(JitState *s, uint32_t col_id,
                                uint32_t dst_reg) {
  MockCtx *ctx = (MockCtx *)s->ctx;
  ctx->n_calls++;
  ctx->last_col_id = col_id;
  ctx->last_dst_reg = dst_reg;
  if (ctx->nb_null) {
    s->regs_i64[dst_reg] = 0;
    return 1;
  }
  double d = (double)col_id * 0.5;
  int64_t bits;
  memcpy(&bits, &d, sizeof(bits));
  s->regs_i64[dst_reg] = bits;
  return 0;
}

static int mock_load_col_u64_nb(JitState *s, uint32_t col_id,
                                uint32_t dst_reg) {
  MockCtx *ctx = (MockCtx *)s->ctx;
  ctx->n_calls++;
  ctx->last_col_id = col_id;
  ctx->last_dst_reg = dst_reg;
  if (ctx->nb_null) {
    s->regs_i64[dst_reg] = 0;
    return 1;
  }
  s->regs_i64[dst_reg] =
      (int64_t)(UINT64_C(0x8000000000000000) + col_id);
  return 0;
}

/* T20: MIN/MAX_U64 ordering across the signed boundary. Rows
 * {2^63+5, 3, 2^64-1}: unsigned order gives min 3, max 2^64-1; a
 * signed compare would call 2^63+5 the minimum (negative as i64) and
 * 3 the maximum. First-row init from garbage, like T14. */
static void test_u64_min_max_ordering(void) {
  const char *name = "T20 u64_min_max_ordering";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 3;
  p.ops[0] = (Op){ .kind = OP_MIN_U64, .a = 0, .b = 1, .c = 0 };
  p.ops[1] = (Op){ .kind = OP_MAX_U64, .a = 1, .b = 1, .c = 1 };
  p.ops[2] = (Op){ .kind = OP_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d) — stencils regenerated?",
              errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitEntry entry = jit1_entry(jp);

  JitState s;
  memset(&s, 0, sizeof(s));
  s.acc_i64[0] = 777;    /* garbage; value_initialized = 0 */
  s.acc_i64[1] = 777;
  const uint64_t rows[3] = {
    UINT64_C(0x8000000000000005),   /* 2^63 + 5 */
    3,
    UINT64_C(0xFFFFFFFFFFFFFFFF),   /* 2^64 - 1 */
  };
  for (int i = 0; i < 3; ++i) {
    memset(s.regs_i64, 0, sizeof(s.regs_i64));
    memset(s.value_updated, 0, sizeof(s.value_updated));
    memset(s.value_unsigned, 0, sizeof(s.value_unsigned));
    s.regs_i64[1] = (int64_t)rows[i];
    entry(&s);
  }

  if ((uint64_t)s.acc_i64[0] != 3 ||
      (uint64_t)s.acc_i64[1] != UINT64_C(0xFFFFFFFFFFFFFFFF)) {
    mark_fail(name, "min=%" PRIu64 " max=0x%" PRIx64
              ", want 3/0xffffffffffffffff (unsigned compares)",
              (uint64_t)s.acc_i64[0], (uint64_t)s.acc_i64[1]);
  } else if (s.value_unsigned[0] != 1 || s.value_unsigned[1] != 1) {
    mark_fail(name, "value_unsigned={%" PRIu64 ",%" PRIu64 "}, want {1,1}",
              s.value_unsigned[0], s.value_unsigned[1]);
  } else if (s.value_updated[0] != 1 || s.value_updated[1] != 1) {
    mark_fail(name, "value_updated={%" PRIu64 ",%" PRIu64 "}, want {1,1}",
              s.value_updated[0], s.value_updated[1]);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T21: SUM_U64_CHECKED — accumulates past the SIGNED boundary without
 * error (3 × 2^62 = 0xC000... > 2^63, where a signed checked add
 * would overflow), then a genuine u64 carry sets row_overflowed. */
static void test_u64_sum_carry(void) {
  const char *name = "T21 u64_sum_carry";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 3;
  p.ops[0] = (Op){ .kind = OP_SUM_U64_CHECKED, .a = 0, .b = 1, .c = 0,
                   .d = 2 };
  p.ops[1] = (Op){ .kind = OP_EXIT };
  p.ops[2] = (Op){ .kind = OP_OVERFLOW_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitEntry entry = jit1_entry(jp);

  JitState s;
  memset(&s, 0, sizeof(s));
  for (int i = 0; i < 3; ++i) {
    memset(s.value_updated, 0, sizeof(s.value_updated));
    s.regs_i64[1] = (int64_t)(UINT64_C(1) << 62);
    entry(&s);
  }
  if ((uint64_t)s.acc_i64[0] != UINT64_C(0xC000000000000000) ||
      s.row_overflowed != 0) {
    mark_fail(name, "acc=0x%" PRIx64 " overflowed=%u, want "
              "0xc000000000000000/0 (past the signed boundary is fine)",
              (uint64_t)s.acc_i64[0], s.row_overflowed);
    jit1_free(jp);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  if (s.value_unsigned[0] != 1) {
    mark_fail(name, "value_unsigned[0]=%" PRIu64 ", want 1",
              s.value_unsigned[0]);
    jit1_free(jp);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  /* Two more 2^62 additions: the first lands exactly at 2^64 → carry. */
  entry(&s);
  entry(&s);
  if (s.row_overflowed != 1) {
    mark_fail(name, "row_overflowed=%u after u64 carry, want 1",
              s.row_overflowed);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T22: OP_LOAD_COL_NDB_U64 cold-call — the helper's above-signed-range
 * bits land in the register file and flow through MAX_U64. */
static void test_u64_load_col_coldcall(void) {
  const char *name = "T22 u64_load_col_coldcall";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 3;
  p.ops[0] = (Op){ .kind = OP_LOAD_COL_NDB_U64, .a = 0, .c = 9 };
  p.ops[1] = (Op){ .kind = OP_MAX_U64, .a = 0, .b = 0, .c = 0 };
  p.ops[2] = (Op){ .kind = OP_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d) — u64 helper "
              "registered?", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  MockCtx ctx;
  memset(&ctx, 0, sizeof(ctx));
  JitState s;
  memset(&s, 0, sizeof(s));
  s.ctx = &ctx;
  jit1_entry(jp)(&s);

  if (ctx.n_calls != 1 || ctx.last_col_id != 9 || ctx.last_dst_reg != 0) {
    mark_fail(name, "helper calls=%u col=%u dst=%u, want 1/9/0",
              ctx.n_calls, ctx.last_col_id, ctx.last_dst_reg);
  } else if ((uint64_t)s.acc_i64[0] != UINT64_C(0x8000000000000009)) {
    mark_fail(name, "acc=0x%" PRIx64 ", want 0x8000000000000009",
              (uint64_t)s.acc_i64[0]);
  } else if (s.value_unsigned[0] != 1) {
    mark_fail(name, "value_unsigned[0]=%" PRIu64 ", want 1",
              s.value_unsigned[0]);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T23: OP_LOAD_COL_NDB_NB — a NULL row takes the branch past the
 * accumulator (no acc update, no mask marks: the kernels' null-skip),
 * a non-null row proceeds normally. Program:
 *   [0] NB load r0 <- col 7, null target 2
 *   [1] SUM_BIGINT acc0 += r0 (unchecked — no OVF tail needed)
 *   [2] EXIT */
static void test_nb_load_null_skips_accumulator(void) {
  const char *name = "T23 nb_load_null_skips_accumulator";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 3;
  p.ops[0] = (Op){ .kind = OP_LOAD_COL_NDB_NB, .a = 0, .b = 7, .c = 2 };
  p.ops[1] = (Op){ .kind = OP_SUM_BIGINT, .a = 0, .b = 0, .c = 0 };
  p.ops[2] = (Op){ .kind = OP_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d) — nb helper "
              "registered / stencils regenerated?", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitEntry entry = jit1_entry(jp);

  MockCtx ctx;
  memset(&ctx, 0, sizeof(ctx));
  JitState s;
  memset(&s, 0, sizeof(s));
  s.ctx = &ctx;

  /* Row 1: NULL — branch taken, nothing accumulates or marks. */
  ctx.nb_null = 1;
  entry(&s);
  if (ctx.n_calls != 1 || ctx.last_col_id != 7 || ctx.last_dst_reg != 0) {
    mark_fail(name, "helper calls=%u col=%u dst=%u, want 1/7/0",
              ctx.n_calls, ctx.last_col_id, ctx.last_dst_reg);
    jit1_free(jp);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  if (s.acc_i64[0] != 0 || s.value_updated[0] != 0 ||
      s.row_fallback != 0) {
    mark_fail(name, "NULL row leaked: acc=%lld updated=%" PRIu64
              " fallback=%u, want 0/0/0",
              (long long)s.acc_i64[0], s.value_updated[0],
              s.row_fallback);
    jit1_free(jp);
    ndb_jit_codemem_destroy(arena);
    return;
  }

  /* Row 2: non-null — normal accumulate (col 7 -> 70). */
  ctx.nb_null = 0;
  entry(&s);
  if (s.acc_i64[0] != 70 || s.value_updated[0] != 1) {
    mark_fail(name, "non-null row: acc=%lld updated=%" PRIu64
              ", want 70/1",
              (long long)s.acc_i64[0], s.value_updated[0]);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T24: OP_LOAD_COL_NDB_F64_NB — a NULL row skips SUM_F64, leaving
 * the accumulator and ALL masks (updated/initialized/double)
 * untouched; a non-null row accumulates (col 6 -> 3.0). */
static void test_nb_f64_load_null_skip(void) {
  const char *name = "T24 nb_f64_load_null_skip";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 4;
  p.ops[0] = (Op){ .kind = OP_LOAD_COL_NDB_F64_NB, .a = 0, .b = 6,
                   .c = 2 };
  p.ops[1] = (Op){ .kind = OP_SUM_F64, .a = 0, .b = 0, .c = 0, .d = 3 };
  p.ops[2] = (Op){ .kind = OP_EXIT };
  p.ops[3] = (Op){ .kind = OP_OVERFLOW_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitEntry entry = jit1_entry(jp);

  MockCtx ctx;
  memset(&ctx, 0, sizeof(ctx));
  JitState s;
  memset(&s, 0, sizeof(s));
  s.ctx = &ctx;

  ctx.nb_null = 1;
  entry(&s);
  if (s.acc_i64[0] != 0 || s.value_updated[0] != 0 ||
      s.value_initialized[0] != 0 || s.value_double[0] != 0) {
    mark_fail(name, "NULL row leaked: acc=0x%" PRIx64 " upd=%" PRIu64
              " init=%" PRIu64 " dbl=%" PRIu64 ", want all 0",
              (uint64_t)s.acc_i64[0], s.value_updated[0],
              s.value_initialized[0], s.value_double[0]);
    jit1_free(jp);
    ndb_jit_codemem_destroy(arena);
    return;
  }

  ctx.nb_null = 0;
  entry(&s);
  if (bits_f64(s.acc_i64[0]) != 3.0 || s.value_updated[0] != 1 ||
      s.value_double[0] != 1) {
    mark_fail(name, "non-null row: acc=%f upd=%" PRIu64 " dbl=%" PRIu64
              ", want 3.0/1/1",
              bits_f64(s.acc_i64[0]), s.value_updated[0],
              s.value_double[0]);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T25: OP_LOAD_COL_NDB_U64_NB — a NULL row skips MAX_U64 (no masks,
 * no init); a non-null row stores 2^63 + 9 with the unsigned mark. */
static void test_nb_u64_load_null_skip(void) {
  const char *name = "T25 nb_u64_load_null_skip";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 3;
  p.ops[0] = (Op){ .kind = OP_LOAD_COL_NDB_U64_NB, .a = 0, .b = 9,
                   .c = 2 };
  p.ops[1] = (Op){ .kind = OP_MAX_U64, .a = 0, .b = 0, .c = 0 };
  p.ops[2] = (Op){ .kind = OP_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitEntry entry = jit1_entry(jp);

  MockCtx ctx;
  memset(&ctx, 0, sizeof(ctx));
  JitState s;
  memset(&s, 0, sizeof(s));
  s.ctx = &ctx;

  ctx.nb_null = 1;
  entry(&s);
  if (s.acc_i64[0] != 0 || s.value_updated[0] != 0 ||
      s.value_initialized[0] != 0 || s.value_unsigned[0] != 0) {
    mark_fail(name, "NULL row leaked: acc=0x%" PRIx64 " upd=%" PRIu64
              " init=%" PRIu64 " uns=%" PRIu64 ", want all 0",
              (uint64_t)s.acc_i64[0], s.value_updated[0],
              s.value_initialized[0], s.value_unsigned[0]);
    jit1_free(jp);
    ndb_jit_codemem_destroy(arena);
    return;
  }

  ctx.nb_null = 0;
  entry(&s);
  if ((uint64_t)s.acc_i64[0] != UINT64_C(0x8000000000000009) ||
      s.value_updated[0] != 1 || s.value_unsigned[0] != 1) {
    mark_fail(name, "non-null row: acc=0x%" PRIx64 " upd=%" PRIu64
              " uns=%" PRIu64 ", want 0x8000000000000009/1/1",
              (uint64_t)s.acc_i64[0], s.value_updated[0],
              s.value_unsigned[0]);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* Phase 5G mock for ndb_jit_h_load_col_dec: records pinfo, writes
 * col_id * 100 (an already-converted scale-0 value). */
static void mock_load_col_dec(JitState *s, uint32_t col_id,
                              uint32_t dst_reg, uint32_t pinfo) {
  MockCtx *ctx = (MockCtx *)s->ctx;
  ctx->n_calls++;
  ctx->last_col_id = col_id;
  ctx->last_dst_reg = dst_reg;
  ctx->last_pinfo = pinfo;
  s->regs_i64[dst_reg] = (int64_t)col_id * 100;
}

/* T30: OP_LOAD_COL_NDB_DEC cold call — all three operand holes
 * (col, dst, pinfo) reach the helper and the value flows into the
 * accumulator. */
static void test_dec_load_col_coldcall(void) {
  const char *name = "T30 dec_load_col_coldcall";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 3;
  p.ops[0] = (Op){ .kind = OP_LOAD_COL_NDB_DEC, .a = 0,
                   .b = (9u << 8) | 2u, .c = 5 };
  p.ops[1] = (Op){ .kind = OP_SUM_BIGINT, .a = 0, .b = 0, .c = 0 };
  p.ops[2] = (Op){ .kind = OP_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d) — dec helper "
              "registered / stencils regenerated?", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  MockCtx ctx;
  memset(&ctx, 0, sizeof(ctx));
  JitState s;
  memset(&s, 0, sizeof(s));
  s.ctx = &ctx;
  jit1_entry(jp)(&s);

  if (ctx.n_calls != 1 || ctx.last_col_id != 5 ||
      ctx.last_dst_reg != 0 || ctx.last_pinfo != ((9u << 8) | 2u)) {
    mark_fail(name, "helper calls=%u col=%u dst=%u pinfo=0x%x, "
              "want 1/5/0/0x902",
              ctx.n_calls, ctx.last_col_id, ctx.last_dst_reg,
              ctx.last_pinfo);
  } else if (s.acc_i64[0] != 500 || s.value_updated[0] != 1) {
    mark_fail(name, "acc=%lld updated=%" PRIu64 ", want 500/1",
              (long long)s.acc_i64[0], s.value_updated[0]);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T26: unsigned checked arithmetic battery — values past the SIGNED
 * boundary flow without spurious overflow: r2 = 2^62 + 3,
 * r3 = r2 - 3 = 2^62, r4 = r3 * 3 = 3*2^62 (> 2^63). */
static void test_u64_arith_battery(void) {
  const char *name = "T26 u64_arith_battery";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 5;
  p.ops[0] = (Op){ .kind = OP_ADD_U64_CHECKED,   .a = 2, .b = 0, .c = 1,
                   .d = 4 };
  p.ops[1] = (Op){ .kind = OP_MINUS_U64_CHECKED, .a = 3, .b = 2, .c = 1,
                   .d = 4 };
  p.ops[2] = (Op){ .kind = OP_MUL_U64_CHECKED,   .a = 4, .b = 3, .c = 1,
                   .d = 4 };
  p.ops[3] = (Op){ .kind = OP_EXIT };
  p.ops[4] = (Op){ .kind = OP_OVERFLOW_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d) — stencils "
              "regenerated?", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitState s;
  memset(&s, 0, sizeof(s));
  s.regs_i64[0] = (int64_t)(UINT64_C(1) << 62);
  s.regs_i64[1] = 3;
  jit1_entry(jp)(&s);

  if ((uint64_t)s.regs_i64[2] != (UINT64_C(1) << 62) + 3 ||
      (uint64_t)s.regs_i64[3] != (UINT64_C(1) << 62) ||
      (uint64_t)s.regs_i64[4] != UINT64_C(3) << 62 ||
      s.row_overflowed != 0) {
    mark_fail(name, "r2=0x%" PRIx64 " r3=0x%" PRIx64 " r4=0x%" PRIx64
              " ovf=%u", (uint64_t)s.regs_i64[2],
              (uint64_t)s.regs_i64[3], (uint64_t)s.regs_i64[4],
              s.row_overflowed);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T27: unsigned subtraction borrow (1 - 2) → overflow exit; the
 * destination is not written. */
static void test_u64_minus_borrow(void) {
  const char *name = "T27 u64_minus_borrow";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 3;
  p.ops[0] = (Op){ .kind = OP_MINUS_U64_CHECKED, .a = 2, .b = 0, .c = 1,
                   .d = 2 };
  p.ops[1] = (Op){ .kind = OP_EXIT };
  p.ops[2] = (Op){ .kind = OP_OVERFLOW_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitState s;
  memset(&s, 0, sizeof(s));
  s.regs_i64[0] = 1;
  s.regs_i64[1] = 2;
  jit1_entry(jp)(&s);
  if (s.row_overflowed != 1 || s.regs_i64[2] != 0) {
    mark_fail(name, "ovf=%u r2=0x%" PRIx64 ", want 1/0",
              s.row_overflowed, (uint64_t)s.regs_i64[2]);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

/* T28: unsigned multiplication overflow (2^32 * 2^32) → overflow exit. */
static void test_u64_mul_overflow(void) {
  const char *name = "T28 u64_mul_overflow";
  Program p;
  memset(&p, 0, sizeof(p));
  p.n_ops = 3;
  p.ops[0] = (Op){ .kind = OP_MUL_U64_CHECKED, .a = 2, .b = 0, .c = 1,
                   .d = 2 };
  p.ops[1] = (Op){ .kind = OP_EXIT };
  p.ops[2] = (Op){ .kind = OP_OVERFLOW_EXIT };

  NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
  Jit1Prog *jp = jit1_compile(arena, &p, NULL);
  if (jp == NULL) {
    mark_fail(name, "jit1_compile failed (errno=%d)", errno);
    ndb_jit_codemem_destroy(arena);
    return;
  }
  JitState s;
  memset(&s, 0, sizeof(s));
  s.regs_i64[0] = (int64_t)(UINT64_C(1) << 32);
  s.regs_i64[1] = (int64_t)(UINT64_C(1) << 32);
  jit1_entry(jp)(&s);
  if (s.row_overflowed != 1) {
    mark_fail(name, "row_overflowed=%u, want 1", s.row_overflowed);
  } else {
    mark_pass(name);
  }
  jit1_free(jp);
  ndb_jit_codemem_destroy(arena);
}

int main(void) {
  printf("RONDB-1056 Phase 4 — coldcall_tests\n");
  printf("===================================\n");

  /* T1 must run BEFORE registration (it asserts ENOENT). */
  test_no_helper_registered();

  /* Register the mocks for the rest of the tests. */
  if (jit1_register_helper("ndb_jit_h_load_col",
                            (JitHelperFn)&mock_load_col) != 0 ||
      jit1_register_helper("ndb_jit_h_load_col_f64",
                            (JitHelperFn)&mock_load_col_f64) != 0 ||
      jit1_register_helper("ndb_jit_h_load_col_u64",
                            (JitHelperFn)&mock_load_col_u64) != 0 ||
      jit1_register_helper("ndb_jit_h_load_col_nb",
                            (JitHelperFn)&mock_load_col_nb) != 0 ||
      jit1_register_helper("ndb_jit_h_load_col_f64_nb",
                            (JitHelperFn)&mock_load_col_f64_nb) != 0 ||
      jit1_register_helper("ndb_jit_h_load_col_u64_nb",
                            (JitHelperFn)&mock_load_col_u64_nb) != 0 ||
      jit1_register_helper("ndb_jit_h_load_col_dec",
                            (JitHelperFn)&mock_load_col_dec) != 0) {
    fprintf(stderr, "FATAL: jit1_register_helper failed\n");
    return 2;
  }

  test_single_row();
  test_multi_row();
  test_coldcall_plus_arith();
  test_sum_marks_result_index();
  test_exit_does_not_mark_result();
  test_registry_basics();
  test_checked_add_no_overflow();
  test_checked_add_overflow();
  test_jump_skips_sum();
  test_filter_reject_exit_sets_state();
  test_describe_pc_registry();
  test_count_bigint();
  test_min_max_first_row_init();
  test_f64_accumulator_battery();
  test_f64_sum_negative_zero_init();
  test_f64_arith_chain();
  test_f64_mul_overflow();
  test_f64_div_by_zero_fallback();
  test_f64_load_col_coldcall();
  test_u64_min_max_ordering();
  test_u64_sum_carry();
  test_u64_load_col_coldcall();
  test_nb_load_null_skips_accumulator();
  test_nb_f64_load_null_skip();
  test_nb_u64_load_null_skip();
  test_u64_arith_battery();
  test_u64_minus_borrow();
  test_u64_mul_overflow();
  test_dec_load_col_coldcall();

  printf("\ncoldcall_tests: %d/%d passed\n", n_pass, n_pass + n_fail);
  return n_fail == 0 ? 0 : 1;
}
