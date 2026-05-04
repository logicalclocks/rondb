/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license as bytecode1.h.)
 */

/*
 * RONDB-1056 Phase 1 — copy-and-patch microbench.
 *
 * Builds the canonical 30-op program, runs it through both the
 * stripped-down interpreter and the JIT'd entry, asserts bit-
 * identical accumulator values, and reports compile time and
 * per-row times against `plan.md` §7's three thresholds:
 *
 *   - compile_ns < 5000          (Linux x86_64 only)
 *   - jit_ns_per_row * 2 <=
 *       interp_ns_per_row        (≥ 2× speedup)
 *   - break_even_rows < 5000
 *
 * Output is one CSV line per run plus a "PASS"/"FAIL" verdict line.
 *
 * Phase 1 design: the JIT path runs only on x86_64 (the only arch
 * with extracted stencil bytes today). On other architectures the
 * binary still builds; jit1_compile returns NULL with errno=ENOTSUP
 * and we report INFO instead of running the JIT path. macOS Apple
 * Silicon hits this branch — it's correctness-only as far as
 * Phase 1 is concerned.
 */

#include "bytecode1.h"
#include "jit1.h"
#include "jit_arena.h"
#include "microbench_interp.h"
#include "microbench_program.h"

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ------------------------------------------------------------------ */
/* Timing helper.                                                     */
/* ------------------------------------------------------------------ */

#if defined(CLOCK_MONOTONIC_RAW)
#define BENCH_CLOCK CLOCK_MONOTONIC_RAW
#else
#define BENCH_CLOCK CLOCK_MONOTONIC
#endif

static uint64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(BENCH_CLOCK, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* ------------------------------------------------------------------ */
/* JIT row-driver: runs the compiled program over `nrows` rows,       */
/* threading the row pointer in JitState. Returns final acc[0].       */
/* ------------------------------------------------------------------ */

#if defined(__x86_64__)
static int64_t jit_run(JitEntry entry, const Row *rows, size_t nrows) {
  JitState s;
  memset(&s, 0, sizeof(s));
  for (size_t i = 0; i < nrows; ++i) {
    s.row_cols_i64 = rows[i].cols;
    /* Per-row register file fresh; accumulators carry. */
    memset(s.regs_i64, 0, sizeof(s.regs_i64));
    entry(&s);
  }
  return s.acc_i64[0];
}
#endif

/* ------------------------------------------------------------------ */
/* main.                                                              */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv) {
  size_t nrows = (argc > 1) ? strtoull(argv[1], NULL, 10) : 100000;
  uint64_t seed = (argc > 2) ? strtoull(argv[2], NULL, 10) : 1;

  Row *rows = mb_generate_rows(nrows, seed);
  if (!rows) {
    fprintf(stderr, "FAIL row generator OOM\n");
    return 2;
  }
  Program prog;
  mb_build_30op_program(&prog);

  /* ---------------- Interpreted run ---------------- */
  int64_t acc_interp = 0;
  uint64_t t_i0 = now_ns();
  interp_run(&prog, rows, nrows, &acc_interp);
  uint64_t t_i1 = now_ns();
  double interp_ns_per_row = (double)(t_i1 - t_i0) / (double)nrows;

#if !defined(__x86_64__)
  /* Phase 1 has no aarch64 stencils. Report interpreter numbers and
   * a clear "JIT path skipped" note; the macOS Apple Silicon dev
   * gets correctness-only signal from proto_interp_only.c instead. */
  printf("INFO proto_microbench: JIT path not available on this arch "
         "(Phase 1 is x86_64-only).\n");
  printf("interp_ns_per_row,jit_ns_per_row,compile_ns,speedup,break_even_rows\n");
  printf("%.2f,n/a,n/a,n/a,n/a\n", interp_ns_per_row);
  free(rows);
  return 0;
#else

  /* ---------------- JIT compile ---------------- */
  /* Generous arena: 30 ops × ~30 bytes = 900 bytes, plus alignment. */
  NdbJitArena *arena = ndb_jit_arena_create(64 * 1024);
  if (!arena) {
    perror("ndb_jit_arena_create");
    free(rows);
    return 3;
  }

  uint64_t t_c0 = now_ns();
  Jit1Prog *jp = jit1_compile(arena, &prog);
  uint64_t t_c1 = now_ns();
  if (!jp) {
    fprintf(stderr, "FAIL jit1_compile errno=%d\n", errno);
    ndb_jit_arena_destroy(arena);
    free(rows);
    return 4;
  }
  double compile_ns = (double)(t_c1 - t_c0);

  /* ---------------- JIT run ---------------- */
  JitEntry entry = jit1_entry(jp);
  if (!entry) {
    fprintf(stderr, "FAIL jit1_entry\n");
    ndb_jit_arena_destroy(arena);
    free(rows);
    return 5;
  }

  uint64_t t_j0 = now_ns();
  int64_t acc_jit = jit_run(entry, rows, nrows);
  uint64_t t_j1 = now_ns();
  double jit_ns_per_row = (double)(t_j1 - t_j0) / (double)nrows;

  /* ---------------- Correctness ---------------- */
  if (acc_interp != acc_jit) {
    fprintf(stderr, "FAIL acc mismatch: interp=%" PRId64
                    " jit=%" PRId64 "\n",
            acc_interp, acc_jit);
    ndb_jit_arena_destroy(arena);
    free(rows);
    return 1;
  }

  /* ---------------- Numbers + verdict ---------------- */
  double speedup = interp_ns_per_row / jit_ns_per_row;
  double per_row_savings = interp_ns_per_row - jit_ns_per_row;
  double break_even_rows = (per_row_savings > 0.0)
                              ? compile_ns / per_row_savings
                              : 1e9;

  printf("nrows=%zu seed=%" PRIu64 " n_ops=%u emitted=%zu acc=%" PRId64 "\n",
         nrows, seed, (unsigned)prog.n_ops,
         jit1_emitted_size(jp), acc_jit);
  printf("interp_ns_per_row,jit_ns_per_row,compile_ns,speedup,break_even_rows\n");
  printf("%.2f,%.2f,%.0f,%.2fx,%.0f\n",
         interp_ns_per_row, jit_ns_per_row,
         compile_ns, speedup, break_even_rows);

  int compile_ok    = (compile_ns < 5000.0);
  int speedup_ok    = (speedup    >= 2.0);
  int breakeven_ok  = (break_even_rows < 5000.0);
  int all_ok        = compile_ok && speedup_ok && breakeven_ok;

  printf("%s compile<5us=%s 2xspeedup=%s breakeven<5k=%s\n",
         all_ok ? "PASS" : "FAIL",
         compile_ok    ? "yes" : "NO",
         speedup_ok    ? "yes" : "NO",
         breakeven_ok  ? "yes" : "NO");

  ndb_jit_arena_destroy(arena);
  free(rows);
  return all_ok ? 0 : 6;
#endif
}
