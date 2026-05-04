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
 * REGULAR x86_64 calling convention: state pointer in rdi.
 *
 * The compiled blob's first 5 bytes are a `push r12; mov r12, rdi`
 * preamble emitted by jit1_compile. The preamble:
 *   - saves the caller's r12 (callee-saved in regular ABI),
 *   - moves rdi into r12 so the stencils — which use r12 internally
 *     under their preserve_none calling convention — find the state
 *     pointer where they expect.
 *
 * Every row terminator (op_exit, op_skip) is overridden to `pop r12;
 * ret` (3 bytes) so the caller's r12 is restored on the way out
 * before returning, honouring the regular ABI's callee-saved
 * contract.
 *
 * This decouples the C call site from clang's preserve_none
 * attribute propagation, which has shown to be unreliable in some
 * clang versions when the attributed function-pointer type round-
 * trips through a typedef. Calls to entry() are plain C calls. */
typedef void (*JitEntry)(JitState *);

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
