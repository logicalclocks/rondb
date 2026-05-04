/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license as bytecode1.h.)
 */

/*
 * RONDB-1056 Phase 1 — hand-written stencil source for
 * copy-and-patch JIT.
 *
 * Compiled with clang to produce stencils.o, which is *not* linked
 * into the bench. Instead we run `objdump -d -r` on stencils.o and
 * manually copy the per-symbol byte ranges + relocation offsets
 * into stencils_x86_64.h, per phase_1_implementation.md §7.
 *
 * Each stencil is tagged `__attribute__((preserve_none))` so clang
 * skips caller-saved register save/restore, and ends in
 * `[[clang::musttail]] return next(state)` (or an indirect tail
 * call for branch). The trailing tail-call instruction is stripped
 * during extraction so stencil bytes flow directly into each other
 * in the emitted blob.
 *
 * The "holes" — operand placeholders — are taken as the *addresses*
 * of extern uint64_t symbols. clang treats those addresses as
 * unknown 64-bit constants and emits `movabs reg, imm64` with an
 * R_X86_64_64 relocation against the symbol. The patcher overwrites
 * the 8-byte imm64 at JIT time with the actual operand value
 * (a register index, an immediate, an accumulator slot, etc.).
 *
 * Hole naming convention: HOLE_<short-stencil-tag>_<role>. Names
 * are unique per stencil so `objdump -r` can identify them
 * unambiguously. Across stencils, semantically-identical holes can
 * use the same suffix — they will produce distinct relocation
 * entries because the symbols are distinct.
 */

#include "jit1.h"

#include <stdint.h>

/* The "next stencil" trampoline. Declared but never defined. clang
 * emits a tail-call to this symbol at the end of every stencil
 * (except branch / skip / exit, which have their own terminators).
 * The 5-byte `jmp rel32` instruction emitted by clang is stripped
 * during extraction; the next stencil's bytes are appended in its
 * place by the JIT.
 *
 * preserve_none must match the stencils — musttail rejects mismatched
 * calling conventions. */
extern __attribute__((preserve_none)) void next(JitState *);

/* Internal stencil function-pointer type. preserve_none, distinct
 * from the public JitEntry typedef (which is regular ABI — see
 * jit1.h). The branch stencil's tail-call to HOLE_BLT_TGT is
 * cast through this type so musttail's calling-convention match
 * is satisfied. */
typedef __attribute__((preserve_none)) void (*StencilTailFn)(JitState *);

/* preserve_none + noinline: the calling convention has minimal
 * register-save overhead, and we never want clang to inline
 * stencils into one another (we want each to be an extractable
 * function). */
#define STENCIL static __attribute__((preserve_none, noinline)) void
#define TAIL_NEXT(state) [[clang::musttail]] return next(state)

/* ------------------------------------------------------------------ */
/* op_load_const_int : regs_i64[DST] = VAL                            */
/* Holes:                                                             */
/*   HOLE_LCI_DST  — 8-byte: register index (0..7)                    */
/*   HOLE_LCI_VAL  — 8-byte: i64 constant value                       */
/* ------------------------------------------------------------------ */
extern uint64_t HOLE_LCI_DST;
extern uint64_t HOLE_LCI_VAL;
STENCIL op_load_const_int(JitState *s) {
  s->regs_i64[(uint64_t)&HOLE_LCI_DST] = (int64_t)(uint64_t)&HOLE_LCI_VAL;
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_load_col_int : regs_i64[DST] = row_cols_i64[COL]                */
/* Holes:                                                             */
/*   HOLE_LRC_DST  — 8-byte: register index (0..7)                    */
/*   HOLE_LRC_COL  — 8-byte: column index (0..7)                      */
/* ------------------------------------------------------------------ */
extern uint64_t HOLE_LRC_DST;
extern uint64_t HOLE_LRC_COL;
STENCIL op_load_col_int(JitState *s) {
  s->regs_i64[(uint64_t)&HOLE_LRC_DST] =
      s->row_cols_i64[(uint64_t)&HOLE_LRC_COL];
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_mov_int_int : regs_i64[DST] = regs_i64[SRC]                     */
/* Holes:                                                             */
/*   HOLE_MV_DST   — 8-byte: register index                           */
/*   HOLE_MV_SRC   — 8-byte: register index                           */
/* ------------------------------------------------------------------ */
extern uint64_t HOLE_MV_DST;
extern uint64_t HOLE_MV_SRC;
STENCIL op_mov_int_int(JitState *s) {
  s->regs_i64[(uint64_t)&HOLE_MV_DST] = s->regs_i64[(uint64_t)&HOLE_MV_SRC];
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_add_int_int : regs_i64[DST] = regs_i64[A] + regs_i64[B]         */
/* Holes:                                                             */
/*   HOLE_ADD_DST  — 8-byte: register index                           */
/*   HOLE_ADD_A    — 8-byte: register index                           */
/*   HOLE_ADD_B    — 8-byte: register index                           */
/* ------------------------------------------------------------------ */
extern uint64_t HOLE_ADD_DST;
extern uint64_t HOLE_ADD_A;
extern uint64_t HOLE_ADD_B;
STENCIL op_add_int_int(JitState *s) {
  s->regs_i64[(uint64_t)&HOLE_ADD_DST] =
      s->regs_i64[(uint64_t)&HOLE_ADD_A] +
      s->regs_i64[(uint64_t)&HOLE_ADD_B];
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_sum_bigint : acc_i64[SLOT] += regs_i64[SRC]                     */
/* Holes:                                                             */
/*   HOLE_SUM_SLOT — 8-byte: accumulator slot index                   */
/*   HOLE_SUM_SRC  — 8-byte: register index                           */
/* ------------------------------------------------------------------ */
extern uint64_t HOLE_SUM_SLOT;
extern uint64_t HOLE_SUM_SRC;
STENCIL op_sum_bigint(JitState *s) {
  s->acc_i64[(uint64_t)&HOLE_SUM_SLOT] +=
      s->regs_i64[(uint64_t)&HOLE_SUM_SRC];
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_branch_lt_int_int : if regs_i64[A] < regs_i64[B] goto TGT       */
/* Holes:                                                             */
/*   HOLE_BLT_A    — 8-byte: register index                           */
/*   HOLE_BLT_B    — 8-byte: register index                           */
/*   HOLE_BLT_TGT  — 8-byte: absolute address of branch target        */
/*                  stencil within the emitted blob                   */
/*                                                                    */
/* The "fall-through" path tail-calls next(); the "taken" path        */
/* tail-calls through the patched HOLE_BLT_TGT pointer.               */
/* ------------------------------------------------------------------ */
extern uint64_t HOLE_BLT_A;
extern uint64_t HOLE_BLT_B;
extern uint64_t HOLE_BLT_TGT;
STENCIL op_branch_lt_int_int(JitState *s) {
  if (s->regs_i64[(uint64_t)&HOLE_BLT_A] <
      s->regs_i64[(uint64_t)&HOLE_BLT_B]) {
    [[clang::musttail]] return ((StencilTailFn)(uintptr_t)&HOLE_BLT_TGT)(s);
  }
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_skip : row terminator (forward-jumps to row-end).               */
/*                                                                    */
/* In Phase 1's program shape, op_skip always jumps to the row-end    */
/* sentinel which is op_exit's bytes. Both op_skip and op_exit are    */
/* implemented as a `ret` from the row entry function — the JIT just  */
/* emits the terminator stencil and execution returns to the caller. */
/* No holes.                                                          */
/* ------------------------------------------------------------------ */
STENCIL op_skip(JitState *s) {
  (void)s;
  /* No tail-call: the trailing jmp to next() is what would be
   * emitted otherwise; we explicitly return so clang emits a `ret`,
   * terminating the row. The extractor preserves the ret as the
   * stencil's only instruction. */
  return;
}

/* ------------------------------------------------------------------ */
/* op_exit : row terminator. Identical shape to op_skip.              */
/* ------------------------------------------------------------------ */
STENCIL op_exit(JitState *s) {
  (void)s;
  return;
}

/* ------------------------------------------------------------------ */
/* Force-keep symbols so the linker doesn't strip them out of         */
/* stencils.o. They are static so they would normally be discarded,   */
/* but `used` keeps them. clang also needs the address taken          */
/* somewhere visible for the relocations to be emitted.               */
/*                                                                    */
/* This array is referenced by an external symbol so the link-time    */
/* dead-code-eliminator can't drop the stencils.                      */
/* ------------------------------------------------------------------ */
__attribute__((used))
const StencilTailFn g_stencil_anchor[] = {
    op_load_const_int,
    op_load_col_int,
    op_mov_int_int,
    op_add_int_int,
    op_sum_bigint,
    op_branch_lt_int_int,
    op_skip,
    op_exit,
};
