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
 * RONDB-1056 Phase 3 Day 1 PM — admission walk unit tests.
 *
 * Hand-built programs exercise jit1_compile's admission walk on
 * accept and reject paths. Each case asserts:
 *
 *   - The expected return-value polarity (handle / NULL).
 *   - The expected jit1_last_admit_error()->reason on reject.
 *   - The expected sidecar fields (offending_pc, offending_target,
 *     offending_kind) where the reason carries them.
 *
 * Plus a pair-test (T_NO_LEAK_ON_REJECT) showing that a reject leaves
 * the arena's high-water mark unchanged — the admission walk really
 * does run before any allocation.
 *
 * Independent of the microbench / regen pipeline. Runs straight off
 * libndb_jit1.a + libndb_jit_arena.a.
 */

#include "bytecode1.h"
#include "jit1.h"
#include "jit_arena.h"

#include <errno.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Harness state.                                                     */
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

/* Helper: check that jit1_compile(prog) is rejected with the given
 * reason and (optionally) offending_pc/target/kind. Pass UINT16_MAX /
 * UINT8_MAX as "don't care" sentinels for the optional fields. */
#define DONTCARE_U16 ((uint16_t)0xFFFF)
#define DONTCARE_U8  ((uint8_t)0xFF)

static void assert_rejected(NdbJitArena *arena,
                             const Program *prog,
                             const char *test_name,
                             Jit1AdmitReason want_reason,
                             uint16_t want_pc,
                             uint16_t want_target,
                             uint8_t  want_kind)
{
  errno = 0;
  Jit1Prog *result = jit1_compile(arena, prog, NULL);
  const Jit1AdmitError *err = jit1_last_admit_error();

  if (result != NULL) {
    mark_fail(test_name, "jit1_compile returned non-NULL on reject");
    return;
  }
  if (errno != EINVAL) {
    mark_fail(test_name, "errno=%d, want EINVAL", errno);
    return;
  }
  if (err->reason != want_reason) {
    mark_fail(test_name, "reason=%d, want %d", err->reason, want_reason);
    return;
  }
  if (want_pc != DONTCARE_U16 && err->offending_pc != want_pc) {
    mark_fail(test_name, "offending_pc=%u, want %u",
              (unsigned)err->offending_pc, (unsigned)want_pc);
    return;
  }
  if (want_target != DONTCARE_U16 && err->offending_target != want_target) {
    mark_fail(test_name, "offending_target=%u, want %u",
              (unsigned)err->offending_target, (unsigned)want_target);
    return;
  }
  if (want_kind != DONTCARE_U8 && err->offending_kind != want_kind) {
    mark_fail(test_name, "offending_kind=%u, want %u",
              (unsigned)err->offending_kind, (unsigned)want_kind);
    return;
  }
  mark_pass(test_name);
}

static void assert_accepted(NdbJitArena *arena,
                             const Program *prog,
                             const char *test_name)
{
  errno = 0;
  Jit1Prog *result = jit1_compile(arena, prog, NULL);
  const Jit1AdmitError *err = jit1_last_admit_error();

  if (result == NULL) {
    mark_fail(test_name, "jit1_compile returned NULL (errno=%d, reason=%d)",
              errno, err->reason);
    return;
  }
  if (err->reason != JIT_ADMIT_OK) {
    mark_fail(test_name, "reason=%d, want OK", err->reason);
    return;
  }
  mark_pass(test_name);
}

/* ------------------------------------------------------------------ */
/* Hand-built program builders.                                       */
/* ------------------------------------------------------------------ */

/* The shortest valid program: load a constant, exit. */
static void build_minimal_accept(Program *p) {
  memset(p, 0, sizeof(*p));
  p->n_ops = 2;
  p->ops[0] = (Op){ .kind = OP_LOAD_CONST_INT, .a = 0, .imm = 42 };
  p->ops[1] = (Op){ .kind = OP_EXIT };
}

/* 5-op program with one forward branch (pc=2 → pc=4). */
static void build_one_branch_accept(Program *p) {
  memset(p, 0, sizeof(*p));
  p->n_ops = 5;
  p->ops[0] = (Op){ .kind = OP_LOAD_CONST_INT,    .a = 0, .imm = 5 };
  p->ops[1] = (Op){ .kind = OP_LOAD_CONST_INT,    .a = 1, .imm = 10 };
  p->ops[2] = (Op){ .kind = OP_BRANCH_LT_INT_INT, .a = 0, .b = 1, .c = 4 };
  p->ops[3] = (Op){ .kind = OP_ADD_INT_INT,       .a = 2, .b = 0, .c = 1 };
  p->ops[4] = (Op){ .kind = OP_EXIT };
}

/* 7-op forked program: two forward branches creating three control-
 * flow segments. pc=2 forward to pc=4; pc=4 forward to pc=6. */
static void build_forked_accept(Program *p) {
  memset(p, 0, sizeof(*p));
  p->n_ops = 7;
  p->ops[0] = (Op){ .kind = OP_LOAD_CONST_INT,    .a = 0, .imm = 5 };
  p->ops[1] = (Op){ .kind = OP_LOAD_CONST_INT,    .a = 1, .imm = 10 };
  p->ops[2] = (Op){ .kind = OP_BRANCH_LT_INT_INT, .a = 0, .b = 1, .c = 4 };
  p->ops[3] = (Op){ .kind = OP_ADD_INT_INT,       .a = 2, .b = 0, .c = 1 };
  p->ops[4] = (Op){ .kind = OP_BRANCH_LT_INT_INT, .a = 1, .b = 0, .c = 6 };
  p->ops[5] = (Op){ .kind = OP_SUM_BIGINT,        .a = 0, .b = 0     };
  p->ops[6] = (Op){ .kind = OP_EXIT };
}

/* Self-branch (op->c == pc). */
static void build_backward_self_branch(Program *p) {
  memset(p, 0, sizeof(*p));
  p->n_ops = 3;
  p->ops[0] = (Op){ .kind = OP_LOAD_CONST_INT,    .a = 0, .imm = 5 };
  p->ops[1] = (Op){ .kind = OP_BRANCH_LT_INT_INT, .a = 0, .b = 0, .c = 1 };  /* op.c == pc */
  p->ops[2] = (Op){ .kind = OP_EXIT };
}

/* Backward branch (op->c < pc). */
static void build_backward_branch(Program *p) {
  memset(p, 0, sizeof(*p));
  p->n_ops = 4;
  p->ops[0] = (Op){ .kind = OP_LOAD_CONST_INT,    .a = 0, .imm = 5 };
  p->ops[1] = (Op){ .kind = OP_LOAD_CONST_INT,    .a = 1, .imm = 10 };
  p->ops[2] = (Op){ .kind = OP_BRANCH_LT_INT_INT, .a = 0, .b = 1, .c = 0 };  /* op.c < pc */
  p->ops[3] = (Op){ .kind = OP_EXIT };
}

/* Out-of-range branch — target == n_ops. */
static void build_branch_oor_eq(Program *p) {
  memset(p, 0, sizeof(*p));
  p->n_ops = 3;
  p->ops[0] = (Op){ .kind = OP_LOAD_CONST_INT,    .a = 0, .imm = 5 };
  p->ops[1] = (Op){ .kind = OP_BRANCH_LT_INT_INT, .a = 0, .b = 0, .c = 3 };  /* op.c == n_ops */
  p->ops[2] = (Op){ .kind = OP_EXIT };
}

/* Out-of-range branch — target > n_ops. */
static void build_branch_oor_gt(Program *p) {
  memset(p, 0, sizeof(*p));
  p->n_ops = 3;
  p->ops[0] = (Op){ .kind = OP_LOAD_CONST_INT,    .a = 0, .imm = 5 };
  p->ops[1] = (Op){ .kind = OP_BRANCH_LT_INT_INT, .a = 0, .b = 0, .c = 99 };  /* op.c > n_ops */
  p->ops[2] = (Op){ .kind = OP_EXIT };
}

/* Cold-call null-check branches must also pass admission target
 * validation — their stencils carry HK_BRANCH_TAKE so a malformed
 * op->c would fault during patching otherwise. One builder per
 * kind, all using backward (op.c < pc) which the simplest reject
 * shape. */
static void build_backward_branch_attr_eq(Program *p) {
  memset(p, 0, sizeof(*p));
  p->n_ops = 2;
  p->ops[0] = (Op){ .kind = OP_BRANCH_ATTR_EQ_NULL, .a = 0, .b = /*attr*/0, .c = 0 };
  p->ops[1] = (Op){ .kind = OP_EXIT };
}

static void build_backward_branch_attr_ne(Program *p) {
  memset(p, 0, sizeof(*p));
  p->n_ops = 2;
  p->ops[0] = (Op){ .kind = OP_BRANCH_ATTR_NE_NULL, .a = 0, .b = /*attr*/0, .c = 0 };
  p->ops[1] = (Op){ .kind = OP_EXIT };
}

static void build_backward_branch_linked_eq(Program *p) {
  memset(p, 0, sizeof(*p));
  p->n_ops = 2;
  p->ops[0] = (Op){ .kind = OP_BRANCH_LINKED_EQ_NULL, .c = 0 };
  p->ops[1] = (Op){ .kind = OP_EXIT };
}

static void build_backward_branch_linked_ne(Program *p) {
  memset(p, 0, sizeof(*p));
  p->n_ops = 2;
  p->ops[0] = (Op){ .kind = OP_BRANCH_LINKED_NE_NULL, .c = 0 };
  p->ops[1] = (Op){ .kind = OP_EXIT };
}

/* Op with kind=0 (admission rejects). */
static void build_invalid_kind_zero(Program *p) {
  memset(p, 0, sizeof(*p));
  p->n_ops = 2;
  p->ops[0] = (Op){ .kind = 0 };       /* invalid */
  p->ops[1] = (Op){ .kind = OP_EXIT };
}

/* Op with kind > OP_KIND_MAX (admission rejects). */
static void build_invalid_kind_too_large(Program *p) {
  memset(p, 0, sizeof(*p));
  p->n_ops = 2;
  p->ops[0] = (Op){ .kind = (uint8_t)(OP_KIND_MAX + 1) };
  p->ops[1] = (Op){ .kind = OP_EXIT };
}

/* Empty program. */
static void build_empty(Program *p) {
  memset(p, 0, sizeof(*p));
  p->n_ops = 0;
}

/* Oversized program — n_ops > BC_MAX_OPS. We don't actually fill
 * BC_MAX_OPS+1 entries (the array is fixed-size), we just set the
 * count. The admission walk's first check should reject before
 * reading ops[]. */
static void build_oversized(Program *p) {
  memset(p, 0, sizeof(*p));
  p->n_ops = (uint16_t)(BC_MAX_OPS + 1);
  /* Fill the visible slots so any out-of-bounds access shows up
   * as a clear poison value rather than zero. */
  for (uint16_t i = 0; i < BC_MAX_OPS; ++i) {
    p->ops[i] = (Op){ .kind = OP_EXIT };
  }
}

/* ------------------------------------------------------------------ */
/* Tests.                                                             */
/* ------------------------------------------------------------------ */

static void test_minimal_accept(NdbJitArena *arena) {
  Program p;
  build_minimal_accept(&p);
  assert_accepted(arena, &p, "T1 minimal_accept");
}

static void test_one_branch_accept(NdbJitArena *arena) {
  Program p;
  build_one_branch_accept(&p);
  assert_accepted(arena, &p, "T2 one_branch_accept");
}

static void test_forked_accept(NdbJitArena *arena) {
  Program p;
  build_forked_accept(&p);
  assert_accepted(arena, &p, "T3 forked_accept");
}

static void test_empty_reject(NdbJitArena *arena) {
  Program p;
  build_empty(&p);
  assert_rejected(arena, &p, "T4 empty_reject",
                   JIT_ADMIT_EMPTY_PROG,
                   DONTCARE_U16, DONTCARE_U16, DONTCARE_U8);
}

static void test_oversized_reject(NdbJitArena *arena) {
  Program p;
  build_oversized(&p);
  assert_rejected(arena, &p, "T5 oversized_reject",
                   JIT_ADMIT_PROG_TOO_LARGE,
                   DONTCARE_U16, DONTCARE_U16, DONTCARE_U8);
}

static void test_invalid_kind_zero(NdbJitArena *arena) {
  Program p;
  build_invalid_kind_zero(&p);
  assert_rejected(arena, &p, "T6 invalid_kind_zero",
                   JIT_ADMIT_INVALID_KIND,
                   /*pc=*/0, DONTCARE_U16, /*kind=*/0);
}

static void test_invalid_kind_too_large(NdbJitArena *arena) {
  Program p;
  build_invalid_kind_too_large(&p);
  assert_rejected(arena, &p, "T7 invalid_kind_too_large",
                   JIT_ADMIT_INVALID_KIND,
                   /*pc=*/0, DONTCARE_U16,
                   /*kind=*/(uint8_t)(OP_KIND_MAX + 1));
}

static void test_backward_self_branch(NdbJitArena *arena) {
  Program p;
  build_backward_self_branch(&p);
  assert_rejected(arena, &p, "T8 backward_self_branch",
                   JIT_ADMIT_BACKWARD_BRANCH,
                   /*pc=*/1, /*target=*/1, /*kind=*/OP_BRANCH_LT_INT_INT);
}

static void test_backward_branch(NdbJitArena *arena) {
  Program p;
  build_backward_branch(&p);
  assert_rejected(arena, &p, "T9 backward_branch",
                   JIT_ADMIT_BACKWARD_BRANCH,
                   /*pc=*/2, /*target=*/0, /*kind=*/OP_BRANCH_LT_INT_INT);
}

static void test_branch_oor_eq(NdbJitArena *arena) {
  Program p;
  build_branch_oor_eq(&p);
  assert_rejected(arena, &p, "T10 branch_oor_eq",
                   JIT_ADMIT_BRANCH_OOR,
                   /*pc=*/1, /*target=*/3, /*kind=*/OP_BRANCH_LT_INT_INT);
}

static void test_branch_oor_gt(NdbJitArena *arena) {
  Program p;
  build_branch_oor_gt(&p);
  assert_rejected(arena, &p, "T11 branch_oor_gt",
                   JIT_ADMIT_BRANCH_OOR,
                   /*pc=*/1, /*target=*/99, /*kind=*/OP_BRANCH_LT_INT_INT);
}

static void test_backward_branch_attr_eq(NdbJitArena *arena) {
  Program p;
  build_backward_branch_attr_eq(&p);
  assert_rejected(arena, &p, "T13 backward_branch_attr_eq",
                   JIT_ADMIT_BACKWARD_BRANCH,
                   /*pc=*/0, /*target=*/0,
                   /*kind=*/OP_BRANCH_ATTR_EQ_NULL);
}

static void test_backward_branch_attr_ne(NdbJitArena *arena) {
  Program p;
  build_backward_branch_attr_ne(&p);
  assert_rejected(arena, &p, "T14 backward_branch_attr_ne",
                   JIT_ADMIT_BACKWARD_BRANCH,
                   /*pc=*/0, /*target=*/0,
                   /*kind=*/OP_BRANCH_ATTR_NE_NULL);
}

static void test_backward_branch_linked_eq(NdbJitArena *arena) {
  Program p;
  build_backward_branch_linked_eq(&p);
  assert_rejected(arena, &p, "T15 backward_branch_linked_eq",
                   JIT_ADMIT_BACKWARD_BRANCH,
                   /*pc=*/0, /*target=*/0,
                   /*kind=*/OP_BRANCH_LINKED_EQ_NULL);
}

static void test_backward_branch_linked_ne(NdbJitArena *arena) {
  Program p;
  build_backward_branch_linked_ne(&p);
  assert_rejected(arena, &p, "T16 backward_branch_linked_ne",
                   JIT_ADMIT_BACKWARD_BRANCH,
                   /*pc=*/0, /*target=*/0,
                   /*kind=*/OP_BRANCH_LINKED_NE_NULL);
}

/* The expensive one: prove a reject doesn't grow the arena. We need
 * a fresh arena for this so the "before" reading is the empty-arena
 * baseline (a previous accept would muddy it). */
static void test_no_leak_on_reject(void) {
  NdbJitArena *arena = ndb_jit_arena_create(64 * 1024);
  if (!arena) {
    mark_fail("T12 no_leak_on_reject", "arena create failed");
    return;
  }

  size_t used_before = ndb_jit_arena_used(arena);

  Program p;
  build_backward_branch(&p);
  errno = 0;
  Jit1Prog *result = jit1_compile(arena, &p, NULL);

  size_t used_after = ndb_jit_arena_used(arena);

  if (result != NULL) {
    mark_fail("T12 no_leak_on_reject", "expected NULL on backward branch");
  } else if (used_after != used_before) {
    mark_fail("T12 no_leak_on_reject",
              "arena grew %zu -> %zu bytes despite reject",
              used_before, used_after);
  } else {
    mark_pass("T12 no_leak_on_reject");
  }

  ndb_jit_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* main.                                                              */
/* ------------------------------------------------------------------ */

int main(void) {
  printf("RONDB-1056 Phase 3 — admission_tests\n");
  printf("====================================\n");

  /* Most cases share an arena to verify the sidecar gets reset
   * properly across calls (a stale reason from a prior reject
   * shouldn't leak into a subsequent accept). */
  NdbJitArena *arena = ndb_jit_arena_create(64 * 1024);
  if (!arena) {
    fprintf(stderr, "FATAL: arena create failed\n");
    return 2;
  }

  test_minimal_accept(arena);
  test_one_branch_accept(arena);
  test_forked_accept(arena);
  test_empty_reject(arena);
  test_oversized_reject(arena);
  test_invalid_kind_zero(arena);
  test_invalid_kind_too_large(arena);
  test_backward_self_branch(arena);
  test_backward_branch(arena);
  test_branch_oor_eq(arena);
  test_branch_oor_gt(arena);
  test_backward_branch_attr_eq(arena);
  test_backward_branch_attr_ne(arena);
  test_backward_branch_linked_eq(arena);
  test_backward_branch_linked_ne(arena);

  ndb_jit_arena_destroy(arena);

  /* T12 wants its own arena. */
  test_no_leak_on_reject();

  printf("\nadmission_tests: %d/%d passed\n", n_pass, n_pass + n_fail);
  return n_fail == 0 ? 0 : 1;
}
