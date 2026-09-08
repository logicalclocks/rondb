/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license as bytecode1.h.)
 */

/*
 * RONDB-1056 Phase 1 — Day 1 sanity test: interpreter only.
 *
 * Builds the canonical 30-op program, runs it through the
 * stripped-down interpreter over a fixed-seed batch of rows, and
 * asserts the expected accumulator value. No JIT path involved.
 * Validates program builder + interpreter before introducing any
 * stencil / patch logic.
 *
 * Exit codes:
 *   0  PASS
 *   1  unexpected accumulator value
 *   2  row generator failed
 *
 * The expected value is computed once at first run with NROWS=10000
 * SEED=1, then hard-coded. If the program shape or row generator
 * changes, update the expectation in this file.
 */

#include "microbench_interp.h"
#include "microbench_program.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

/* Compute the same aggregate independently of the program/interp
 * — a reference oracle. The 30-op program is exactly:
 *
 *   for each row:
 *     if col[0] < 1000:    skip
 *     else:                acc[0] += col[1] + col[2] + col[3]
 *                                    + col[1] + col[2]      (second sum pair)
 *
 * Anything else in the program is mov/add filler that doesn't
 * touch acc[0]. We compute the same thing here in C and compare. */
static int64_t reference_aggregate(const Row *rows, size_t nrows) {
  int64_t acc = 0;
  for (size_t i = 0; i < nrows; ++i) {
    if (rows[i].cols[0] < 1000) continue;
    acc += rows[i].cols[1];
    acc += rows[i].cols[2];
    acc += rows[i].cols[3];
    /* Second sum pair against col[1] and col[2]. */
    acc += rows[i].cols[1];
    acc += rows[i].cols[2];
  }
  return acc;
}

int main(int argc, char **argv) {
  size_t nrows = 10000;
  uint64_t seed = 1;
  if (argc > 1) nrows = (size_t)strtoull(argv[1], NULL, 10);
  if (argc > 2) seed  = strtoull(argv[2], NULL, 10);

  Row *rows = mb_generate_rows(nrows, seed);
  if (!rows) {
    fprintf(stderr, "FAIL row generator OOM\n");
    return 2;
  }

  Program prog;
  mb_build_30op_program(&prog);

  int64_t want = reference_aggregate(rows, nrows);

  int64_t got = 0;
  interp_run(&prog, rows, nrows, &got);

  if (got != want) {
    fprintf(stderr, "FAIL nrows=%zu seed=%" PRIu64
                    " interp_acc=%" PRId64 " want=%" PRId64 "\n",
            nrows, seed, got, want);
    free(rows);
    return 1;
  }

  printf("PASS proto_interp_only nrows=%zu seed=%" PRIu64
         " n_ops=%u acc=%" PRId64 "\n",
         nrows, seed, (unsigned)prog.n_ops, got);
  free(rows);
  return 0;
}
