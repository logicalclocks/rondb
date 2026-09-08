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
#include "jit_codemem.h"
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

/* ------------------------------------------------------------------ */
/* main.                                                              */
/* ------------------------------------------------------------------ */

/* ------------------------------------------------------------------ */
/* qsort comparator + percentile helpers.                             */
/* ------------------------------------------------------------------ */

/* Used only in the x86_64 JIT path. The non-x86 builds get a
 * `-Wunused-function` warning otherwise. */
__attribute__((unused))
static int dbl_cmp(const void *a, const void *b) {
  double x = *(const double *)a, y = *(const double *)b;
  return (x > y) - (x < y);
}

__attribute__((unused))
static double dbl_pct(const double *sorted, size_t n, double p) {
  size_t idx = (size_t)((double)(n - 1) * p);
  return sorted[idx];
}

static int run_informational_bench(const char *title,
                                   const Program *prog,
                                   const Row *rows,
                                   size_t nrows,
                                   int repeats) {
  int64_t acc_interp = 0;
  uint64_t t_i0 = now_ns();
  interp_run(prog, rows, nrows, &acc_interp);
  uint64_t t_i1 = now_ns();
  double interp_ns_per_row = (double)(t_i1 - t_i0) / (double)nrows;

  double *compile_samples =
      (double *)calloc((size_t)repeats, sizeof(double));
  double *jit_per_row_samples =
      (double *)calloc((size_t)repeats, sizeof(double));
  if (!compile_samples || !jit_per_row_samples) {
    fprintf(stderr, "FAIL %s: OOM samples\n", title);
    free(compile_samples);
    free(jit_per_row_samples);
    return 0;
  }

  int64_t acc_jit = 0;
  size_t emitted = 0;
  for (int it = 0; it < repeats; ++it) {
    NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
    if (!arena) {
      fprintf(stderr, "FAIL %s: arena create failed\n", title);
      free(compile_samples);
      free(jit_per_row_samples);
      return 0;
    }

    uint64_t t_c0 = now_ns();
    Jit1Prog *jp = jit1_compile(arena, prog, NULL);
    uint64_t t_c1 = now_ns();
    if (!jp) {
      const Jit1AdmitError *err = jit1_last_admit_error();
      fprintf(stderr,
              "FAIL %s: jit1_compile failed (errno=%d, reason=%d, iter=%d)\n",
              title, errno, err->reason, it);
      ndb_jit_codemem_destroy(arena);
      free(compile_samples);
      free(jit_per_row_samples);
      return 0;
    }
    compile_samples[it] = (double)(t_c1 - t_c0);
    if (it == 0) emitted = jit1_emitted_size(jp);

    JitEntry entry = jit1_entry(jp);
    uint64_t t_j0 = now_ns();
    int64_t acc_this = jit_run(entry, rows, nrows);
    uint64_t t_j1 = now_ns();
    jit_per_row_samples[it] = (double)(t_j1 - t_j0) / (double)nrows;

    if (it == 0) {
      acc_jit = acc_this;
    } else if (acc_this != acc_jit) {
      fprintf(stderr, "FAIL %s: acc drift across iters: %" PRId64
                      " -> %" PRId64 " at iter=%d\n",
              title, acc_jit, acc_this, it);
      ndb_jit_codemem_destroy(arena);
      free(compile_samples);
      free(jit_per_row_samples);
      return 0;
    }

    ndb_jit_codemem_destroy(arena);
  }

  if (acc_interp != acc_jit) {
    fprintf(stderr, "FAIL %s: acc mismatch: interp=%" PRId64
                    " jit=%" PRId64 "\n",
            title, acc_interp, acc_jit);
    free(compile_samples);
    free(jit_per_row_samples);
    return 0;
  }

  size_t warm_n = (size_t)(repeats - 1);
  double cold_compile_ns = compile_samples[0];
  double *warm_compile = compile_samples + 1;
  qsort(warm_compile, warm_n, sizeof(double), dbl_cmp);
  qsort(jit_per_row_samples, (size_t)repeats, sizeof(double), dbl_cmp);

  double warm_compile_med = warm_compile[warm_n / 2];
  double warm_compile_min = warm_compile[0];
  double warm_compile_p99 = dbl_pct(warm_compile, warm_n, 0.99);
  double jit_ns_per_row_med = jit_per_row_samples[(size_t)repeats / 2];
  double jit_ns_per_row_min = jit_per_row_samples[0];
  double jit_ns_per_row_p99 =
      dbl_pct(jit_per_row_samples, (size_t)repeats, 0.99);
  double speedup = interp_ns_per_row / jit_ns_per_row_med;

  printf("%s\n", title);
  printf("========================================\n");
  printf("  Program        : %u ops, %zu bytes emitted\n",
         (unsigned)prog->n_ops, emitted);
  printf("  Aggregate      : %" PRId64 "  (interp == jit, ok)\n", acc_jit);
  printf("  Interpreter    : %7.2f ns/row\n", interp_ns_per_row);
  printf("  JIT'd (median) : %7.2f ns/row   (min %.2f, p99 %.2f)\n",
         jit_ns_per_row_med, jit_ns_per_row_min, jit_ns_per_row_p99);
  printf("  Speedup        : %7.2fx\n", speedup);
  printf("  Compile (cold) : %7.2f us      (informational)\n",
         cold_compile_ns / 1000.0);
  printf("  Compile (warm) : %7.2f us      (min %.2f, p99 %.2f us)\n",
         warm_compile_med / 1000.0,
         warm_compile_min / 1000.0,
         warm_compile_p99 / 1000.0);
  printf("\n");

  free(compile_samples);
  free(jit_per_row_samples);
  return 1;
}

int main(int argc, char **argv) {
  size_t   nrows   = (argc > 1) ? strtoull(argv[1], NULL, 10) : 100000;
  uint64_t seed    = (argc > 2) ? strtoull(argv[2], NULL, 10) : 1;
  int      repeats = (argc > 3) ? (int)strtol(argv[3], NULL, 10) : 11;
  if (repeats < 2) repeats = 2;

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

  /* ---------------- JIT: multi-iteration measurement ---------------- */
  /*
   * One-shot wall-clock measurements have wide variance from cold
   * cache + CPU frequency-scaling + scheduler artefacts. We do
   * `repeats` iterations of (fresh arena, compile, run rows, destroy
   * arena), report:
   *   - first compile separately (the cold-cache first-compile
   *     baseline; relevant for the very first query a node sees);
   *   - median / min / p99 of the remaining "warm" compiles, which
   *     reflect what production sees once the icache is hot;
   *   - median / min / p99 of per-row JIT times across all iterations.
   *
   * The interpreter run above is a single shot — its variance is
   * already averaged out across nrows rows, so a multi-iter loop on
   * it would be noise.
   */

  double *compile_samples = (double *)calloc((size_t)repeats, sizeof(double));
  double *jit_per_row_samples = (double *)calloc((size_t)repeats, sizeof(double));
  /* Per-phase timing samples — same length as compile_samples,
   * indexed by iteration. */
  double *pass1_samples  = (double *)calloc((size_t)repeats, sizeof(double));
  double *alloc_samples  = (double *)calloc((size_t)repeats, sizeof(double));
  double *emit_samples   = (double *)calloc((size_t)repeats, sizeof(double));
  double *seal_samples   = (double *)calloc((size_t)repeats, sizeof(double));
  double *handle_samples = (double *)calloc((size_t)repeats, sizeof(double));
  size_t  emitted = 0;
  int64_t acc_jit = 0;
  if (!compile_samples || !jit_per_row_samples || !pass1_samples ||
      !alloc_samples || !emit_samples || !seal_samples || !handle_samples) {
    fprintf(stderr, "FAIL OOM samples\n");
    free(compile_samples);   free(jit_per_row_samples);
    free(pass1_samples);     free(alloc_samples);
    free(emit_samples);      free(seal_samples);
    free(handle_samples);    free(rows);
    return 3;
  }

  for (int it = 0; it < repeats; ++it) {
    NdbJitCodeMem *arena = ndb_jit_codemem_create(0);
    if (!arena) {
      perror("ndb_jit_codemem_create");
      free(compile_samples);   free(jit_per_row_samples);
      free(pass1_samples);     free(alloc_samples);
      free(emit_samples);      free(seal_samples);
      free(handle_samples);    free(rows);
      return 3;
    }

    Jit1Timing t = {0};
    uint64_t t_c0 = now_ns();
    Jit1Prog *jp = jit1_compile(arena, &prog, &t);
    uint64_t t_c1 = now_ns();
    if (!jp) {
      fprintf(stderr, "FAIL jit1_compile errno=%d (iter=%d)\n", errno, it);
      ndb_jit_codemem_destroy(arena);
      free(compile_samples);   free(jit_per_row_samples);
      free(pass1_samples);     free(alloc_samples);
      free(emit_samples);      free(seal_samples);
      free(handle_samples);    free(rows);
      return 4;
    }
    compile_samples[it] = (double)(t_c1 - t_c0);
    pass1_samples[it]  = (double)t.pass1_ns;
    alloc_samples[it]  = (double)t.alloc_ns;
    emit_samples[it]   = (double)t.emit_ns;
    seal_samples[it]   = (double)t.seal_ns;
    handle_samples[it] = (double)t.handle_ns;
    if (it == 0) emitted = jit1_emitted_size(jp);

    JitEntry entry = jit1_entry(jp);
    uint64_t t_j0 = now_ns();
    int64_t acc_this = jit_run(entry, rows, nrows);
    uint64_t t_j1 = now_ns();
    jit_per_row_samples[it] = (double)(t_j1 - t_j0) / (double)nrows;

    if (it == 0) {
      acc_jit = acc_this;
    } else if (acc_this != acc_jit) {
      fprintf(stderr, "FAIL acc drift across iters: %" PRId64
                      " -> %" PRId64 " at iter=%d\n",
              acc_jit, acc_this, it);
      ndb_jit_codemem_destroy(arena);
      free(compile_samples);   free(jit_per_row_samples);
      free(pass1_samples);     free(alloc_samples);
      free(emit_samples);      free(seal_samples);
      free(handle_samples);    free(rows);
      return 1;
    }

    ndb_jit_codemem_destroy(arena);
  }

  /* ---------------- Correctness ---------------- */
  if (acc_interp != acc_jit) {
    fprintf(stderr, "FAIL acc mismatch: interp=%" PRId64
                    " jit=%" PRId64 "\n",
            acc_interp, acc_jit);
    free(compile_samples);   free(jit_per_row_samples);
    free(pass1_samples);     free(alloc_samples);
    free(emit_samples);      free(seal_samples);
    free(handle_samples);    free(rows);
    return 1;
  }

  /* ---------------- Distil the distributions ---------------- */
  double cold_compile_ns = compile_samples[0];
  double cold_pass1_ns   = pass1_samples[0];
  double cold_alloc_ns   = alloc_samples[0];
  double cold_emit_ns    = emit_samples[0];
  double cold_seal_ns    = seal_samples[0];
  double cold_handle_ns  = handle_samples[0];

  /* Warm = iterations 1..N-1 (drop the cold first one). */
  size_t warm_n = (size_t)(repeats - 1);
  double *warm_compile = compile_samples + 1;
  qsort(warm_compile, warm_n, sizeof(double), dbl_cmp);
  double warm_compile_min = warm_compile[0];
  double warm_compile_med = warm_compile[warm_n / 2];
  double warm_compile_p99 = dbl_pct(warm_compile, warm_n, 0.99);

  /* Per-phase warm medians + p99. We sort each phase's warm samples
   * independently — the sum of phase medians won't exactly equal
   * total_compile_med because each phase's median may come from a
   * different iteration. Phase numbers below are nonetheless
   * representative of "typical" cost per phase. */
  qsort(pass1_samples  + 1, warm_n, sizeof(double), dbl_cmp);
  qsort(alloc_samples  + 1, warm_n, sizeof(double), dbl_cmp);
  qsort(emit_samples   + 1, warm_n, sizeof(double), dbl_cmp);
  qsort(seal_samples   + 1, warm_n, sizeof(double), dbl_cmp);
  qsort(handle_samples + 1, warm_n, sizeof(double), dbl_cmp);
  double warm_pass1_med  = pass1_samples [1 + warm_n / 2];
  double warm_alloc_med  = alloc_samples [1 + warm_n / 2];
  double warm_emit_med   = emit_samples  [1 + warm_n / 2];
  double warm_seal_med   = seal_samples  [1 + warm_n / 2];
  double warm_handle_med = handle_samples[1 + warm_n / 2];
  double warm_pass1_p99  = dbl_pct(pass1_samples  + 1, warm_n, 0.99);
  double warm_alloc_p99  = dbl_pct(alloc_samples  + 1, warm_n, 0.99);
  double warm_emit_p99   = dbl_pct(emit_samples   + 1, warm_n, 0.99);
  double warm_seal_p99   = dbl_pct(seal_samples   + 1, warm_n, 0.99);
  double warm_handle_p99 = dbl_pct(handle_samples + 1, warm_n, 0.99);
  double phase_sum_med   = warm_pass1_med + warm_alloc_med +
                           warm_emit_med + warm_seal_med + warm_handle_med;

  qsort(jit_per_row_samples, (size_t)repeats, sizeof(double), dbl_cmp);
  double jit_ns_per_row_med = jit_per_row_samples[(size_t)repeats / 2];
  double jit_ns_per_row_min = jit_per_row_samples[0];
  double jit_ns_per_row_p99 =
      dbl_pct(jit_per_row_samples, (size_t)repeats, 0.99);

  /* For the verdict we use the median warm compile and the median
   * per-row JIT time — these reflect the steady-state production
   * cost. The cold first compile is reported separately because it
   * matters for "first query the node ever sees" but not for any
   * subsequent execution. */
  double compile_ns = warm_compile_med;
  double jit_ns_per_row = jit_ns_per_row_med;

  /* ---------------- Verdict against §7 thresholds ---------------- */
  double speedup = interp_ns_per_row / jit_ns_per_row;
  double per_row_savings = interp_ns_per_row - jit_ns_per_row;
  double break_even_rows = (per_row_savings > 0.0)
                              ? compile_ns / per_row_savings
                              : 1e9;

  /* Phase 1 thresholds were tuned for x86_64. aarch64 stencils are
   * ~3x larger (movz/movk chains) which inflates emit+patch cost; the
   * compile budget and speedup floor are relaxed accordingly. Phase 5
   * will tune aarch64 perf properly — Phase 2's bar on aarch64 is
   * correctness (jit aggregate == interp aggregate), not absolute
   * compile time / speedup. */
#if defined(__x86_64__)
  const double compile_threshold_ns = 5000.0;
  const double speedup_threshold    = 2.0;
#else
  const double compile_threshold_ns = 15000.0;
  const double speedup_threshold    = 1.5;   /* sanity floor; above 1x means JIT is faster than interp */
#endif
  int compile_ok    = (compile_ns < compile_threshold_ns);
  int speedup_ok    = (speedup    >= speedup_threshold);
  int breakeven_ok  = (break_even_rows < 5000.0);
  int all_ok        = compile_ok && speedup_ok && breakeven_ok;

  const char *mark_ok   = "[ok]";
  const char *mark_fail = "[FAIL]";

  printf("\n");
  printf("Phase 1 microbench  copy-and-patch JIT vs interpreter\n");
  printf("=====================================================\n");
  printf("  Program        : %u ops, %zu bytes emitted (seed=%" PRIu64 ")\n",
         (unsigned)prog.n_ops, emitted, seed);
  printf("  Rows           : %zu\n", nrows);
  printf("  Iterations     : %d  (1 cold + %zu warm)\n", repeats, warm_n);
  printf("  Aggregate      : %" PRId64 "  (interp == jit, ok)\n", acc_jit);
  printf("\n");
  printf("  Per-row dispatch                target          result\n");
  printf("  ----------------                ------          ------\n");
  printf("  Interpreter    : %7.2f ns/row\n", interp_ns_per_row);
  printf("  JIT'd (median) : %7.2f ns/row   (min %.2f, p99 %.2f)\n",
         jit_ns_per_row_med, jit_ns_per_row_min, jit_ns_per_row_p99);
  char speedup_target_str[32];
  snprintf(speedup_target_str, sizeof(speedup_target_str),
           ">= %.2fx", speedup_threshold);
  printf("  Speedup        : %7.2fx       %-15s %s\n",
         speedup, speedup_target_str, speedup_ok ? mark_ok : mark_fail);
  printf("\n");
  printf("  Compile cost                    target          result\n");
  printf("  ------------                    ------          ------\n");
  printf("  Compile (cold) : %7.2f us      (1st compile, cold cache; "
         "informational)\n",
         cold_compile_ns / 1000.0);
  char compile_target_str[32];
  snprintf(compile_target_str, sizeof(compile_target_str),
           "< %.2f us", compile_threshold_ns / 1000.0);
  printf("  Compile (warm) : %7.2f us      %-15s %s\n",
         warm_compile_med / 1000.0, compile_target_str,
         compile_ok ? mark_ok : mark_fail);
  printf("                   (min %.2f, p99 %.2f us)\n",
         warm_compile_min / 1000.0, warm_compile_p99 / 1000.0);
  printf("  Break-even     : %7.0f rows    %-15s %s\n",
         break_even_rows, "< 5000 rows",
         breakeven_ok ? mark_ok : mark_fail);
  printf("\n");

  /* ---------------- Compile breakdown ---------------- */
  /* Per-phase warm medians + p99. Total may differ slightly from
   * the sum because each phase's median is independent. */
  printf("  Compile breakdown — warm median (%% of warm-median total):\n");
  printf("  ---------------------------------------------------------\n");
  printf("  pass1 (size walk)    : %5.0f ns   (%4.1f%%)   p99 %.0f ns\n",
         warm_pass1_med,
         100.0 * warm_pass1_med / phase_sum_med,
         warm_pass1_p99);
  printf("  arena alloc          : %5.0f ns   (%4.1f%%)   p99 %.0f ns\n",
         warm_alloc_med,
         100.0 * warm_alloc_med / phase_sum_med,
         warm_alloc_p99);
  printf("  emit + patch         : %5.0f ns   (%4.1f%%)   p99 %.0f ns\n",
         warm_emit_med,
         100.0 * warm_emit_med / phase_sum_med,
         warm_emit_p99);
  printf("  arena seal           : %5.0f ns   (%4.1f%%)   p99 %.0f ns\n",
         warm_seal_med,
         100.0 * warm_seal_med / phase_sum_med,
         warm_seal_p99);
  printf("  malloc handle        : %5.0f ns   (%4.1f%%)   p99 %.0f ns\n",
         warm_handle_med,
         100.0 * warm_handle_med / phase_sum_med,
         warm_handle_p99);
  printf("  -- sum of phase meds : %5.0f ns\n", phase_sum_med);
  printf("\n");
  printf("  Cold first-compile breakdown (informational):\n");
  printf("  pass1=%4.0f ns  alloc=%4.0f ns  emit=%4.0f ns  "
         "seal=%4.0f ns  handle=%4.0f ns\n",
         cold_pass1_ns, cold_alloc_ns, cold_emit_ns,
         cold_seal_ns, cold_handle_ns);
  printf("\n");
  printf("VERDICT: %s\n",
         all_ok        ? "PASS - all three Phase 1 thresholds cleared"
       : speedup_ok && breakeven_ok && !compile_ok
                       ? "PARTIAL - speedup and break-even cleared; "
                         "compile time over budget"
       : !speedup_ok   ? "FAIL - speedup target missed; reassess approach"
                       : "FAIL - one or more thresholds missed");
  printf("\n");

  /* Also emit a CSV summary line for scripted parsing. */
  printf("CSV  nrows,iters,interp_ns_row,jit_ns_row_med,jit_ns_row_min,"
         "compile_cold_ns,compile_warm_med_ns,compile_warm_min_ns,"
         "compile_warm_p99_ns,speedup,break_even_rows\n");
  printf("CSV  %zu,%d,%.2f,%.2f,%.2f,%.0f,%.0f,%.0f,%.0f,%.2fx,%.0f\n",
         nrows, repeats,
         interp_ns_per_row, jit_ns_per_row_med, jit_ns_per_row_min,
         cold_compile_ns, warm_compile_med, warm_compile_min,
         warm_compile_p99,
         speedup, break_even_rows);

  free(compile_samples);   free(jit_per_row_samples);
  free(pass1_samples);     free(alloc_samples);
  free(emit_samples);      free(seal_samples);
  free(handle_samples);

  /* ---------------------------------------------------------------- */
  /* Phase 3 — forked-program differential test.                      */
  /*                                                                  */
  /* No perf gates here; the goal is "JIT result == interp result"    */
  /* on a program that exercises three new branch opcodes (LE/EQ/GT)  */
  /* and the multi-fixup queue (3 simultaneous pending fixups).       */
  /*                                                                  */
  /* The aggregate is acc[0]+acc[1]+acc[2] across all rows.           */
  /* ---------------------------------------------------------------- */

  Program forked_prog;
  mb_build_forked_program(&forked_prog);

  /* Interpreter run on the forked program. */
  int64_t acc_forked_interp = 0;
  uint64_t t_fi0 = now_ns();
  /* interp_run only returns acc[0]; for the forked program we need
   * acc[0]+acc[1]+acc[2]. Wrap the interpreter loop ourselves. */
  {
    JitState s;
    memset(&s, 0, sizeof(s));
    for (size_t i = 0; i < nrows; ++i) {
      s.row_cols_i64 = rows[i].cols;
      memset(s.regs_i64, 0, sizeof(s.regs_i64));
      /* Inline-interpret one row of the forked program. */
      for (uint16_t pc = 0; pc < forked_prog.n_ops; /*per-case advance*/) {
        const Op *op = &forked_prog.ops[pc];
        switch (op->kind) {
          case OP_LOAD_CONST_INT:
            s.regs_i64[op->a] = op->imm; ++pc; break;
          case OP_LOAD_COL_INT:
            s.regs_i64[op->a] = s.row_cols_i64[op->b]; ++pc; break;
          case OP_SUM_BIGINT:
            s.acc_i64[op->a] += s.regs_i64[op->b]; ++pc; break;
          case OP_BRANCH_LE_INT_INT:
            pc = (s.regs_i64[op->a] <= s.regs_i64[op->b]) ? op->c : (uint16_t)(pc + 1);
            break;
          case OP_BRANCH_EQ_INT_INT:
            pc = (s.regs_i64[op->a] == s.regs_i64[op->b]) ? op->c : (uint16_t)(pc + 1);
            break;
          case OP_BRANCH_GT_INT_INT:
            pc = (s.regs_i64[op->a] >  s.regs_i64[op->b]) ? op->c : (uint16_t)(pc + 1);
            break;
          case OP_SKIP:
          case OP_EXIT:
            goto forked_row_done;
          default:
            goto forked_row_done;
        }
      }
    forked_row_done: ;
    }
    acc_forked_interp = s.acc_i64[0] + s.acc_i64[1] + s.acc_i64[2];
  }
  uint64_t t_fi1 = now_ns();
  double forked_interp_ns_per_row = (double)(t_fi1 - t_fi0) / (double)nrows;

  /* JIT-compile + run the forked program. */
  NdbJitCodeMem *arena_f = ndb_jit_codemem_create(0);
  if (!arena_f) {
    fprintf(stderr, "FAIL forked: arena create failed\n");
    free(rows);
    return 7;
  }
  Jit1Prog *fp = jit1_compile(arena_f, &forked_prog, NULL);
  if (!fp) {
    const Jit1AdmitError *err = jit1_last_admit_error();
    fprintf(stderr, "FAIL forked: jit1_compile failed (errno=%d, reason=%d)\n",
            errno, err->reason);
    ndb_jit_codemem_destroy(arena_f);
    free(rows);
    return 7;
  }
  JitEntry fentry = jit1_entry(fp);

  int64_t acc_forked_jit = 0;
  uint64_t t_fj0 = now_ns();
  {
    JitState s;
    memset(&s, 0, sizeof(s));
    for (size_t i = 0; i < nrows; ++i) {
      s.row_cols_i64 = rows[i].cols;
      memset(s.regs_i64, 0, sizeof(s.regs_i64));
      fentry(&s);
      acc_forked_jit += 0;  /* per-row accumulators carry across rows already */
      (void)acc_forked_jit;
    }
    acc_forked_jit = s.acc_i64[0] + s.acc_i64[1] + s.acc_i64[2];
  }
  uint64_t t_fj1 = now_ns();
  double forked_jit_ns_per_row = (double)(t_fj1 - t_fj0) / (double)nrows;
  ndb_jit_codemem_destroy(arena_f);

  int forked_ok = (acc_forked_interp == acc_forked_jit);

  printf("Phase 3 forked program — differential test\n");
  printf("==========================================\n");
  printf("  Program        : %u ops, 3 forward branches (LE, EQ, GT)\n",
         (unsigned)forked_prog.n_ops);
  printf("  Aggregate (interp): %" PRId64 "\n", acc_forked_interp);
  printf("  Aggregate (jit)   : %" PRId64 "  %s\n",
         acc_forked_jit, forked_ok ? "[ok]" : "[MISMATCH]");
  printf("  Interpreter    : %7.2f ns/row\n", forked_interp_ns_per_row);
  printf("  JIT'd          : %7.2f ns/row   (informational; no perf gate)\n",
         forked_jit_ns_per_row);
  printf("\n");

  Program checked_prog;
  mb_build_checked_30op_program(&checked_prog);
  int checked_ok = run_informational_bench(
      "Checked arithmetic normal-path microbench",
      &checked_prog, rows, nrows, repeats);

  free(rows);
  return (all_ok && forked_ok && checked_ok) ? 0 : 6;
}
