/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license as bytecode1.h.)
 */

/*
 * RONDB-1056 Phase 1 — interpreter API for the microbench.
 *
 * Stripped-down dispatch loop modelled on
 * AggInterpreter::ProcessRec. Phase 1 doesn't model NULLs, type
 * promotion, or accumulator-slot resolution — the program never
 * exercises any of that. What's modelled is the *dispatch shape*
 * (switch on opcode kind, per-row inner loop), which is what
 * copy-and-patch is meant to beat.
 */

#ifndef NDB_JIT_MICROBENCH_INTERP_H
#define NDB_JIT_MICROBENCH_INTERP_H

#include "bytecode1.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Run the program over `nrows` rows. Writes the final value of
 * acc[0] into *out_acc. */
void interp_run(const Program *prog,
                const Row *rows,
                size_t nrows,
                int64_t *out_acc);

#ifdef __cplusplus
}
#endif

#endif /* NDB_JIT_MICROBENCH_INTERP_H */
