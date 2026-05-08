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
 * runtime use. Phase 4 adds `ctx` for the cold-call mechanism: C++
 * helpers cast it to a context struct that carries NDB-side
 * pointers (block_tup, req_struct, the AggInterpreter instance).
 * Microbench code keeps ctx == nullptr; pure stencils never read
 * ctx. */
typedef struct JitState {
  int64_t   regs_i64[BC_MAX_REGS];   /* per-row register file */
  int64_t   acc_i64[BC_MAX_ACCS];    /* program-level accumulators */
  const int64_t *row_cols_i64;       /* current row's columns (microbench path) */
  void     *ctx;                     /* opaque per-call context for cold-call helpers */
} JitState;

/* Per-row entry function — produced by jit1_compile.
 *
 * REGULAR x86_64 calling convention: state pointer in rdi.
 *
 * The compiled blob starts with a `push r12; sub rsp,8; mov r12,rdi`
 * preamble emitted by jit1_compile. The preamble:
 *   - saves the caller's r12 (callee-saved in regular ABI),
 *   - keeps the stack parity that clang's extracted cold-call
 *     stencils expect before they call regular C helpers,
 *   - moves rdi into r12 so the stencils — which use r12 internally
 *     under their preserve_none calling convention — find the state
 *     pointer where they expect.
 *
 * Every row terminator (op_exit, op_skip) is overridden to
 * `add rsp,8; pop r12; ret` so the caller's stack and r12 are restored
 * on the way out, honouring the regular ABI.
 *
 * This decouples the C call site from clang's preserve_none
 * attribute propagation, which has shown to be unreliable in some
 * clang versions when the attributed function-pointer type round-
 * trips through a typedef. Calls to entry() are plain C calls. */
typedef void (*JitEntry)(JitState *);

/* Compiled program handle. Opaque outside jit1.c. */
typedef struct Jit1Prog Jit1Prog;

/* Optional per-phase timing breakdown of jit1_compile. Caller may
 * pass NULL. All values in nanoseconds, computed via
 * CLOCK_MONOTONIC_RAW (or CLOCK_MONOTONIC where unavailable).
 *
 * Phases sum to total_ns minus measurement overhead (~6× the cost
 * of a clock_gettime call, which is ~10-15 ns per sample on x86_64
 * — small relative to the phase totals). */
typedef struct {
  uint64_t total_ns;          /* jit1_compile entry to exit */
  uint64_t pass1_ns;          /* bytecode walk + size compute */
  uint64_t alloc_ns;          /* arena bump-pointer alloc */
  uint64_t emit_ns;           /* memcpy stencils + hole patch + branch fixups */
  uint64_t seal_ns;           /* arena seal (icache flush, RW->RX translate) */
  uint64_t handle_ns;         /* Jit1Prog malloc + populate */
} Jit1Timing;

/* Admission verdicts. jit1_compile runs an admission walk (a single
 * forward linear pass over prog->ops) before any arena allocation.
 * On reject, jit1_compile returns NULL with errno=EINVAL and the
 * thread-local sidecar at jit1_last_admit_error() carries the
 * specific reason + offending PC for the caller to log.
 *
 * Append-only: future opcodes / failure modes add new values at the
 * end so existing callers' switch statements stay valid. */
typedef enum {
  JIT_ADMIT_OK              = 0,
  JIT_ADMIT_EMPTY_PROG      = 1,   /* n_ops == 0 */
  JIT_ADMIT_PROG_TOO_LARGE  = 2,   /* n_ops > BC_MAX_OPS */
  JIT_ADMIT_INVALID_KIND    = 3,   /* op kind out of OpKind range or zero */
  JIT_ADMIT_UNSUPPORTED_OP  = 4,   /* opcode kind has no stencil */
  JIT_ADMIT_BACKWARD_BRANCH = 5,   /* op->c <= pc */
  JIT_ADMIT_BRANCH_OOR      = 6,   /* op->c >= n_ops */
} Jit1AdmitReason;

typedef struct {
  Jit1AdmitReason reason;
  uint16_t        offending_pc;     /* meaningful when reason refers to one */
  uint16_t        offending_target; /* meaningful for branch-related reasons */
  uint8_t         offending_kind;   /* meaningful for INVALID_KIND / UNSUPPORTED_OP */
} Jit1AdmitError;

/* Compile `prog` into `arena`. Returns NULL on:
 *   - admission rejection (errno=EINVAL; jit1_last_admit_error()
 *     carries the reason)
 *   - arena OOM (errno=ENOMEM)
 * The returned handle remains valid until the arena is destroyed.
 *
 * If `out_timing` is non-NULL, the per-phase breakdown is written
 * there. Pass NULL when timing isn't needed (no measurement
 * overhead is paid in that case). */
Jit1Prog *jit1_compile(NdbJitArena *arena,
                       const Program *prog,
                       Jit1Timing *out_timing);

/* Read-only view of the most recent admission failure on the
 * calling thread. Returns a pointer to thread-local storage; the
 * pointed-to data is only meaningful immediately after a NULL
 * return from jit1_compile and only when errno was EINVAL.
 * On accept, the returned struct's reason is JIT_ADMIT_OK. */
const Jit1AdmitError *jit1_last_admit_error(void);

/* Get the per-row entry pointer. Must be called after compile;
 * always succeeds for a non-NULL prog. */
JitEntry jit1_entry(const Jit1Prog *prog);

/* Total emitted size in bytes (for diagnostics). */
size_t jit1_emitted_size(const Jit1Prog *prog);

/* ------------------------------------------------------------------ */
/* Phase 4 RONDB-1056 — cold-call helper registry.                    */
/*                                                                    */
/* Some opcodes (kOpLoadCol, future kOpDiv NULL fixup, kOpStringSearch,*/
/* etc.) are too complex to inline as pure stencils — their stencils  */
/* emit a regular C function call to an extern helper, then continue  */
/* with TAIL_NEXT. The engine's compile-time patcher resolves the    */
/* helper's address through this registry and writes the call target  */
/* at the HK_COLDCALL hole (absolute on x86_64, PC-relative on        */
/* aarch64).                                                         */
/*                                                                    */
/* Registration is one-shot at engine init (e.g., from                */
/* DbtupJitGlue::dbtup_jit_register_helpers). Call before any         */
/* jit1_compile that targets a stencil with cold-call holes —         */
/* otherwise compile fails ENOENT.                                    */
/* ------------------------------------------------------------------ */

/* Generic callable type. Helpers cast the JitState* / extra args
 * to their own signatures; the registry doesn't enforce a shape. */
typedef void (*JitHelperFn)(void);

/* Register a helper. Returns 0 on success, -1 if the registry is
 * full (raise the static cap in jit1.c). Re-registering the same
 * name with the same fn is a no-op; same name with different fn
 * returns -1 (defensive — name collisions are bugs). */
int jit1_register_helper(const char *name, JitHelperFn fn);

/* Look up a helper by name. Returns NULL if not registered. */
JitHelperFn jit1_lookup_helper(const char *name);

#ifdef __cplusplus
}
#endif

#endif /* NDB_JIT1_H */
