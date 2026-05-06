/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license as bytecode1.h.)
 */

/*
 * RONDB-1056 Phase 1 — program builder + row generator API.
 *
 * Pure C11. Used by the microbench to construct a deterministic
 * 30-op aggregation-shaped program and the synthetic rows it
 * processes.
 */

#ifndef NDB_JIT_MICROBENCH_PROGRAM_H
#define NDB_JIT_MICROBENCH_PROGRAM_H

#include "bytecode1.h"

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Build the canonical 30-op program. The shape (described in
 * phase_1_implementation.md §4) is:
 *
 *   r0 = const 1000             ; threshold
 *   r1 = const 0                ; constant 0
 *   r2 = col[0]                 ; filter key
 *   if r2 < r0 -> label_skip    ; forward branch
 *   r3 = col[1]; acc[0] += r3
 *   r3 = col[2]; acc[0] += r3
 *   r3 = col[3]; acc[0] += r3
 *   ... mov/add filler to grow op count to ~30 ...
 *   exit
 * label_skip:
 *   skip                        ; forward jump to row_end
 *
 * Returns a Program with n_ops set; ops[] beyond n_ops is
 * untouched (caller must zero-init the struct first if it cares). */
void mb_build_30op_program(Program *out);

/* Build the Phase 3 forked program. ~17 ops, three forward branches
 * (BRANCH_LE, BRANCH_EQ, BRANCH_GT) creating four control-flow
 * segments. Exercises:
 *
 *   - Multi-fixup queue: branches at pc=3, 4, 6 with targets at
 *     pcs ~8, ~11, ~14, so 3 fixups are pending simultaneously
 *     between pc=6 and pc=8.
 *   - Multiple new branch opcodes (LE, EQ, GT) — not just BLT.
 *   - Different accumulators per segment, so the final aggregate
 *     is data-dependent on which branch fires.
 *
 * Uses the same row layout as mb_build_30op_program (col[0..3]).
 * Final aggregate sums acc[0] + acc[1] + acc[2] (acc[3] unused). */
void mb_build_forked_program(Program *out);

/* Generate `nrows` synthetic rows. Deterministic given a seed.
 * Rows are allocated with malloc; caller free()s.
 *
 * Column layout for the 30-op program:
 *   col[0]  in [0, 2000)   — filter key, ~half land below threshold 1000
 *   col[1]  in [1, 100)    — sum source 1
 *   col[2]  in [1, 100)    — sum source 2
 *   col[3]  in [1, 100)    — sum source 3
 *   col[4..7]  zero        — unused
 */
Row *mb_generate_rows(size_t nrows, uint64_t seed);

#ifdef __cplusplus
}
#endif

#endif /* NDB_JIT_MICROBENCH_PROGRAM_H */
