/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.

 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

/*
 * RONDB-1056 Phase 0 — I-cache flush microbenchmark.
 *
 * Measures the wall-clock cost of ndb_jit_arena_seal as a proxy for
 * the I-cache invalidation cost on each platform/arch. Output is CSV
 * to stdout, ready to paste verbatim into phase_0_arena.md:
 *
 *     size,median_ns,p99_ns
 *     16,...,...
 *     64,...,...
 *     ...
 *
 * On x86_64 the result is dominated by call/return overhead and the
 * timer resolution; we still print the numbers so reviewers see
 * "near-zero" rather than wonder if the test ran. The decision-
 * relevant values are the ARM64 ones — specifically p99 at 1024 B,
 * which represents a typical 30-op JIT'd program.
 */

#include "jit_arena.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Picks CLOCK_MONOTONIC_RAW on Linux (not subject to NTP slew); falls
 * back to CLOCK_MONOTONIC elsewhere (macOS exposes _RAW too as of
 * 10.12, but we keep the fallback for portability). */
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

static int u64_cmp(const void *a, const void *b) {
  uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
  return (x > y) - (x < y);
}

/* Run `reps` iterations of seal-on-`bytes`-bytes and return median
 * + p99 in `*out_median`/`*out_p99` (ns). Each iteration uses a
 * fresh arena to avoid amortising the first-touch page-fault cost
 * across measurements. */
static void measure_one_size(size_t bytes, int reps,
                             uint64_t *out_median,
                             uint64_t *out_p99) {
  uint64_t *samples = (uint64_t *)calloc((size_t)reps, sizeof(uint64_t));
  if (!samples) {
    fprintf(stderr, "FAIL: oom samples\n");
    *out_median = 0;
    *out_p99 = 0;
    return;
  }

  /* Pad payload up to 4 KiB so any size fits inside one arena page. */
  uint8_t *payload = (uint8_t *)calloc(1, 4096);
  if (!payload) {
    free(samples);
    fprintf(stderr, "FAIL: oom payload\n");
    *out_median = 0;
    *out_p99 = 0;
    return;
  }
  /* Fill with NOPs / scratch so the bytes are not all zero. The
   * value doesn't matter — we never execute these. */
  memset(payload, 0x90, 4096);

  for (int r = 0; r < reps; ++r) {
    NdbJitArena *a = ndb_jit_arena_create(4096);
    if (!a) {
      samples[r] = 0;
      continue;
    }
    void *rw = ndb_jit_arena_alloc(a, bytes, 16);
    if (!rw) {
      ndb_jit_arena_destroy(a);
      samples[r] = 0;
      continue;
    }
    memcpy(rw, payload, bytes);

    uint64_t t0 = now_ns();
    (void)ndb_jit_arena_seal(a, rw, bytes);
    uint64_t t1 = now_ns();
    samples[r] = t1 - t0;

    ndb_jit_arena_destroy(a);
  }

  qsort(samples, (size_t)reps, sizeof(uint64_t), u64_cmp);
  *out_median = samples[reps / 2];
  /* p99: index ceil(0.99 * reps) - 1, clamped to last. */
  int p99_idx = (int)((double)reps * 0.99);
  if (p99_idx >= reps) p99_idx = reps - 1;
  *out_p99 = samples[p99_idx];

  free(samples);
  free(payload);
}

int main(void) {
#if defined(__x86_64__)
  printf("INFO x86_64: I/D cache coherent, "
         "__builtin___clear_cache is a no-op. "
         "Numbers below capture call/timer overhead only.\n");
#endif

  static const size_t sizes[] = {16, 64, 256, 1024, 4096};
  static const int reps = 10000;

  printf("size,median_ns,p99_ns\n");
  for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); ++i) {
    uint64_t med = 0, p99 = 0;
    measure_one_size(sizes[i], reps, &med, &p99);
    printf("%zu,%" PRIu64 ",%" PRIu64 "\n",
           sizes[i], med, p99);
  }
  return 0;
}
