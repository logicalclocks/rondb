/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license as bytecode1.h.)
 */

/*
 * RONDB-1056 Phase 1 — copy-and-patch JIT engine API.
 *
 * Pure C11. Uses the Phase 0 arena (jit_arena.h) for code memory.
 * Compiles a Program (bytecode1.h) into native code by stitching
 * hand-extracted stencils together and patching each operand hole
 * with the actual operand value for that bytecode instruction.
 *
 * Phase 1 has exactly one stencil per opcode kind, no type prop,
 * no admission walk, no fallback: all 8 stencils support the 30-op
 * program, full stop. Phase 3+ generalises.
 */

#ifndef NDB_JIT1_H
#define NDB_JIT1_H

#include "bytecode1.h"
#include "jit_arena.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* JIT execution state. Identical layout in stencils_src.c and the
 * bench so clang's struct-offset calculations match the patcher's
 * runtime use. Phase 4 replaces this with the real
 * AggInterpreter::ProcessRec state. */
typedef struct JitState {
  int64_t   regs_i64[BC_MAX_REGS];   /* per-row register file */
  int64_t   acc_i64[BC_MAX_ACCS];    /* program-level accumulators */
  const int64_t *row_cols_i64;       /* current row's columns */
} JitState;

/* Per-row entry function — produced by jit1_compile.
 *
 * preserve_none calling convention: state pointer goes in r12 on
 * x86_64 (NOT rdi as in the standard ABI). The attribute is applied
 * directly in this single typedef rather than chained through an
 * intermediate alias, because some clang versions drop the attribute
 * when typedef-aliasing twice.
 *
 * Calling a preserve_none function from a default-convention caller
 * (the microbench's jit_run) is fine: clang at the call site sees
 * the attribute on the typedef and emits the correct `mov r12, ...`
 * sequence, and saves/restores r12 around the call (since it's
 * callee-saved in the regular ABI but not in preserve_none). */
typedef __attribute__((preserve_none)) void (*JitEntry)(JitState *);

/* Compiled program handle. Opaque outside jit1.c. */
typedef struct Jit1Prog Jit1Prog;

/* Compile `prog` into `arena`. Returns NULL on arena OOM (the only
 * failure mode in Phase 1). The returned handle remains valid until
 * the arena is destroyed. */
Jit1Prog *jit1_compile(NdbJitArena *arena, const Program *prog);

/* Get the per-row entry pointer. Must be called after compile;
 * always succeeds for a non-NULL prog. */
JitEntry jit1_entry(const Jit1Prog *prog);

/* Total emitted size in bytes (for diagnostics). */
size_t jit1_emitted_size(const Jit1Prog *prog);

#ifdef __cplusplus
}
#endif

#endif /* NDB_JIT1_H */
