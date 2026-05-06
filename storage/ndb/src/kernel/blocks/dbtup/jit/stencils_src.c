/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license as bytecode1.h.)
 */

/*
 * RONDB-1056 — hand-written stencil source for copy-and-patch JIT.
 *
 * Compiled with the pinned upstream LLVM clang (see plan.md §2)
 * by the regen-stencils CMake target. The compiled .o is consumed
 * by extract_stencils.c, which emits stencils_x86_64.h /
 * stencils_arm64.h with per-stencil byte arrays + Hole tables.
 *
 * Each stencil is tagged `__attribute__((preserve_none))` so clang
 * skips caller-saved register save/restore, and ends in
 * `[[clang::musttail]] return next(state)` (or an indirect tail
 * call for the branch stencil). The trailing tail-call instruction
 * is stripped during extraction so stencil bytes flow directly
 * into each other in the emitted blob.
 *
 * Two arches need DIFFERENT hole materialisation strategies:
 *
 *   x86_64 — `extern uint64_t HOLE_*` placeholder pattern. clang
 *            with -fno-pic + small code model lowers
 *            `(uint64_t)&HOLE_*` to `mov reg32, imm32` with an
 *            R_X86_64_32 relocation. The extractor records the
 *            relocation site as the hole; the engine patches the
 *            4-byte immediate at JIT time.
 *
 *   aarch64 — volatile uint64_t = MAGIC sentinel pattern. clang
 *             materialises the constant via a `movz/movk/movk/movk`
 *             chain (4 instructions × 4 bytes = 16 bytes). The
 *             extractor scans the stencil's byte range for each
 *             magic constant from kHoleMagicTable[] and records the
 *             chain's byte offset as the hole; the engine patches
 *             the 4 immediate fields at JIT time.
 *
 *             The aarch64 fallback is needed because the
 *             `extern uint64_t` pattern compiles to GOT-indirect
 *             addressing on aarch64 (verified Phase 1) — there's
 *             no inline immediate the patcher can rewrite.
 *
 * The DECLARE_HOLE / HOLE() macros below paper over the difference.
 */

#include "jit1.h"
#include "hole_kinds.h"

#include <stdint.h>

/* The "next stencil" trampoline. Declared but never defined. clang
 * emits a tail-call to this symbol at the end of every stencil
 * (except branch / skip / exit, which have their own terminators).
 * The trailing `jmp rel32` (x86_64) or `b rel26` (aarch64) emitted
 * by clang is stripped during extraction; the next stencil's bytes
 * are appended in its place by the JIT.
 *
 * preserve_none must match the stencils — musttail rejects
 * mismatched calling conventions. */
extern __attribute__((preserve_none)) void next(JitState *);

/* Internal stencil function-pointer type. preserve_none, distinct
 * from the public JitEntry typedef (which is regular ABI — see
 * jit1.h). The branch stencil's tail-call to HOLE_BLT_TGT is cast
 * through this type so musttail's calling-convention match is
 * satisfied. */
typedef __attribute__((preserve_none)) void (*StencilTailFn)(JitState *);

/* preserve_none + noinline: the calling convention has minimal
 * register-save overhead, and we never want clang to inline
 * stencils into one another (we want each to be an extractable
 * function). */
#define STENCIL static __attribute__((preserve_none, noinline)) void
#define TAIL_NEXT(state) [[clang::musttail]] return next(state)

/* ------------------------------------------------------------------ */
/* HOLE() — produces a uint64_t whose value the patcher rewrites at  */
/* JIT time. Declaration + use macros are different per arch:        */
/*                                                                    */
/*   x86_64:                                                         */
/*     DECLARE_HOLE(LCI_DST)  -> extern uint64_t HOLE_LCI_DST;        */
/*     HOLE(LCI_DST)          -> (uint64_t)&HOLE_LCI_DST              */
/*                               (a relocatable, becomes mov imm32)  */
/*                                                                    */
/*   aarch64:                                                        */
/*     DECLARE_HOLE(LCI_DST)  -> (no top-level declaration needed)   */
/*     HOLE(LCI_DST)          -> aarch64_hole_(MAGIC_LCI_DST)         */
/*                               (a volatile-load that compiles to a */
/*                                movz/movk/movk/movk chain)         */
/* ------------------------------------------------------------------ */

#if defined(__x86_64__)
#  define DECLARE_HOLE(name)  extern uint64_t HOLE_##name
#  define HOLE(name)          ((uint64_t)(uintptr_t)&HOLE_##name)
#elif defined(__aarch64__)
#  define DECLARE_HOLE(name)  /* nothing — magic constant from hole_kinds.h */

/* The volatile load forces clang to actually materialise the
 * constant via a movz/movk chain; without volatile, it would
 * constant-fold and we'd have nothing to patch. Inlining is
 * essential — we want the chain inline at the use site, not
 * hidden behind a function call. */
__attribute__((always_inline))
static inline uint64_t aarch64_hole_(uint64_t magic) {
  volatile uint64_t v = magic;
  return v;
}
#  define HOLE(name)          aarch64_hole_(MAGIC_##name)
#else
#  error "unsupported architecture for stencil source"
#endif

/* ------------------------------------------------------------------ */
/* op_load_const_int : regs_i64[DST] = VAL                            */
/* ------------------------------------------------------------------ */
DECLARE_HOLE(LCI_DST);
DECLARE_HOLE(LCI_VAL);
STENCIL op_load_const_int(JitState *s) {
  s->regs_i64[HOLE(LCI_DST)] = (int64_t)HOLE(LCI_VAL);
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_load_col_int : regs_i64[DST] = row_cols_i64[COL]                */
/* ------------------------------------------------------------------ */
DECLARE_HOLE(LRC_DST);
DECLARE_HOLE(LRC_COL);
STENCIL op_load_col_int(JitState *s) {
  s->regs_i64[HOLE(LRC_DST)] = s->row_cols_i64[HOLE(LRC_COL)];
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_mov_int_int : regs_i64[DST] = regs_i64[SRC]                     */
/* ------------------------------------------------------------------ */
DECLARE_HOLE(MV_DST);
DECLARE_HOLE(MV_SRC);
STENCIL op_mov_int_int(JitState *s) {
  s->regs_i64[HOLE(MV_DST)] = s->regs_i64[HOLE(MV_SRC)];
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_add_int_int : regs_i64[DST] = regs_i64[A] + regs_i64[B]         */
/* ------------------------------------------------------------------ */
DECLARE_HOLE(ADD_DST);
DECLARE_HOLE(ADD_A);
DECLARE_HOLE(ADD_B);
STENCIL op_add_int_int(JitState *s) {
  s->regs_i64[HOLE(ADD_DST)] = s->regs_i64[HOLE(ADD_A)] + s->regs_i64[HOLE(ADD_B)];
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_sum_bigint : acc_i64[SLOT] += regs_i64[SRC]                     */
/* ------------------------------------------------------------------ */
DECLARE_HOLE(SUM_SLOT);
DECLARE_HOLE(SUM_SRC);
STENCIL op_sum_bigint(JitState *s) {
  s->acc_i64[HOLE(SUM_SLOT)] += s->regs_i64[HOLE(SUM_SRC)];
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_branch_lt_int_int : if regs_i64[A] < regs_i64[B] goto TGT       */
/*                                                                    */
/* The "fall-through" path tail-calls next(); the "taken" path        */
/* tail-calls through HOLE_BLT_TGT (the patched branch target).       */
/*                                                                    */
/* HOLE_BLT_TGT is a tail-call target, NOT an operand value, so on    */
/* both arches it's left as an extern symbol — the linker emits a     */
/* PC-relative branch relocation (R_X86_64_PLT32 on x86_64,           */
/* R_AARCH64_CALL26 on aarch64) which the extractor records as        */
/* HK_BRANCH_TAKE. The patcher rewrites the displacement at JIT time. */
/* ------------------------------------------------------------------ */
DECLARE_HOLE(BLT_A);
DECLARE_HOLE(BLT_B);
extern __attribute__((preserve_none)) void HOLE_BLT_TGT(JitState *);
STENCIL op_branch_lt_int_int(JitState *s) {
  if (s->regs_i64[HOLE(BLT_A)] < s->regs_i64[HOLE(BLT_B)]) {
    [[clang::musttail]] return HOLE_BLT_TGT(s);
  }
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_branch_*_int_int — comparison siblings (Phase 3).               */
/*                                                                    */
/* Identical structure to op_branch_lt_int_int with the comparison    */
/* operator flipped. Same hole shape (HK_OP_A, HK_OP_B for operands;  */
/* HK_BRANCH_FALL for the fall-through; HK_BRANCH_TAKE for the taken  */
/* tail-call). The extractor handles them identically — no extractor */
/* changes required.                                                  */
/* ------------------------------------------------------------------ */

DECLARE_HOLE(BLE_A);
DECLARE_HOLE(BLE_B);
extern __attribute__((preserve_none)) void HOLE_BLE_TGT(JitState *);
STENCIL op_branch_le_int_int(JitState *s) {
  if (s->regs_i64[HOLE(BLE_A)] <= s->regs_i64[HOLE(BLE_B)]) {
    [[clang::musttail]] return HOLE_BLE_TGT(s);
  }
  TAIL_NEXT(s);
}

DECLARE_HOLE(BEQ_A);
DECLARE_HOLE(BEQ_B);
extern __attribute__((preserve_none)) void HOLE_BEQ_TGT(JitState *);
STENCIL op_branch_eq_int_int(JitState *s) {
  if (s->regs_i64[HOLE(BEQ_A)] == s->regs_i64[HOLE(BEQ_B)]) {
    [[clang::musttail]] return HOLE_BEQ_TGT(s);
  }
  TAIL_NEXT(s);
}

DECLARE_HOLE(BGT_A);
DECLARE_HOLE(BGT_B);
extern __attribute__((preserve_none)) void HOLE_BGT_TGT(JitState *);
STENCIL op_branch_gt_int_int(JitState *s) {
  if (s->regs_i64[HOLE(BGT_A)] > s->regs_i64[HOLE(BGT_B)]) {
    [[clang::musttail]] return HOLE_BGT_TGT(s);
  }
  TAIL_NEXT(s);
}

DECLARE_HOLE(BGE_A);
DECLARE_HOLE(BGE_B);
extern __attribute__((preserve_none)) void HOLE_BGE_TGT(JitState *);
STENCIL op_branch_ge_int_int(JitState *s) {
  if (s->regs_i64[HOLE(BGE_A)] >= s->regs_i64[HOLE(BGE_B)]) {
    [[clang::musttail]] return HOLE_BGE_TGT(s);
  }
  TAIL_NEXT(s);
}

DECLARE_HOLE(BNE_A);
DECLARE_HOLE(BNE_B);
extern __attribute__((preserve_none)) void HOLE_BNE_TGT(JitState *);
STENCIL op_branch_ne_int_int(JitState *s) {
  if (s->regs_i64[HOLE(BNE_A)] != s->regs_i64[HOLE(BNE_B)]) {
    [[clang::musttail]] return HOLE_BNE_TGT(s);
  }
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_minus_int_int : regs_i64[DST] = regs_i64[A] - regs_i64[B]       */
/* op_mul_int_int   : regs_i64[DST] = regs_i64[A] * regs_i64[B]       */
/*                                                                    */
/* Phase 4 hot-arithmetic siblings of OP_ADD_INT_INT — same hole      */
/* shape (a=dst, b=lhs, c=rhs). Bridge maps NDB's kOpMinusBigint /    */
/* kOpMulBigint to these.                                             */
/* ------------------------------------------------------------------ */

DECLARE_HOLE(MINUS_DST);
DECLARE_HOLE(MINUS_A);
DECLARE_HOLE(MINUS_B);
STENCIL op_minus_int_int(JitState *s) {
  s->regs_i64[HOLE(MINUS_DST)] = s->regs_i64[HOLE(MINUS_A)] - s->regs_i64[HOLE(MINUS_B)];
  TAIL_NEXT(s);
}

DECLARE_HOLE(MUL_DST);
DECLARE_HOLE(MUL_A);
DECLARE_HOLE(MUL_B);
STENCIL op_mul_int_int(JitState *s) {
  s->regs_i64[HOLE(MUL_DST)] = s->regs_i64[HOLE(MUL_A)] * s->regs_i64[HOLE(MUL_B)];
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_skip / op_exit : row terminators.                               */
/*                                                                    */
/* Bare returns; the extractor overrides the bytes entirely with      */
/* the engine-required terminator sequence (see                       */
/* extract_stencils.c — kX86Terminator on x86_64, the equivalent      */
/* aarch64 sequence on arm64).                                        */
/* ------------------------------------------------------------------ */
STENCIL op_skip(JitState *s) {
  (void)s;
  return;
}

STENCIL op_exit(JitState *s) {
  (void)s;
  return;
}

/* ------------------------------------------------------------------ */
/* Force-keep symbols so the linker doesn't strip them out of         */
/* stencils.o. They are static so they would normally be discarded,   */
/* but `used` keeps them.                                             */
/* ------------------------------------------------------------------ */
__attribute__((used))
const StencilTailFn g_stencil_anchor[] = {
    op_load_const_int,
    op_load_col_int,
    op_mov_int_int,
    op_add_int_int,
    op_sum_bigint,
    op_branch_lt_int_int,
    op_branch_le_int_int,
    op_branch_eq_int_int,
    op_branch_gt_int_int,
    op_branch_ge_int_int,
    op_branch_ne_int_int,
    op_skip,
    op_exit,
    op_minus_int_int,
    op_mul_int_int,
};
