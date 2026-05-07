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
#  define DECLARE_HOLE(name)         extern uint64_t HOLE_##name
#  define DECLARE_NARROW_HOLE(name)  DECLARE_HOLE(name)
#  define DECLARE_FOLD_HOLE(name)    DECLARE_HOLE(name)
#  define HOLE(name)                 ((uint64_t)(uintptr_t)&HOLE_##name)
/* On x86_64, narrow / fold holes share the same extern symbol +
 * R_X86_64_32 relocation pattern as wide holes. The 32-bit
 * immediate is wide enough for any operand value already; no
 * compaction is possible. HOLE_NARROW / HOLE_LOAD_REG /
 * HOLE_STORE_REG fall back to the same `regs_i64[HOLE(...)]`
 * idiom — clang lowers the index into rip-relative addressing. */
#  define HOLE_NARROW(name)          HOLE(name)
#  define HOLE_32(name)              HOLE(name)
#  define HOLE_LOAD_REG(name, state)  \
      ((state)->regs_i64[HOLE(name)])
#  define HOLE_STORE_REG(name, state, value)  \
      ((state)->regs_i64[HOLE(name)] = (value))
#  define HOLE_LOAD_ACC(name, state)  \
      ((state)->acc_i64[HOLE(name)])
#  define HOLE_STORE_ACC(name, state, value)  \
      ((state)->acc_i64[HOLE(name)] = (value))
#  define HOLE_LOAD_COL(name, state)  \
      ((state)->row_cols_i64[HOLE(name)])
#elif defined(__aarch64__)
#  define DECLARE_HOLE(name)         /* nothing — magic constant from hole_kinds.h */
#  define DECLARE_NARROW_HOLE(name)  /* nothing — same */
#  define DECLARE_FOLD_HOLE(name)    /* nothing — same */

/* Phase 4.6: inline asm with `"n"` immediate constraint. The
 * older Phase 4.5 form used `volatile uint{16,64}_t v = magic;
 * return v;` to force clang to materialise the constant — but
 * that *also* forced a STR/LDR (or STRH/LDRH) round-trip via a
 * stack slot, which in turn required a `sub sp / add sp`
 * stack-frame pair. The volatile semantics are stronger than we
 * need.
 *
 * `__asm__ volatile` pins the side effect to its lexical
 * position (clang doesn't CSE two HOLE() calls to the same
 * magic) without demanding a memory round-trip. `=r` lets clang
 * pick any free register; `"n"` substitutes the magic as a
 * compile-time integer immediate directly into the asm string.
 * Net effect: the MOVZ chain bytes are byte-identical to Phase
 * 4.5, but the surrounding spill/reload + prologue/epilogue
 * disappear. ~50% byte saving across the stencil set.
 *
 * See phase_4_6_implementation.md for the spike findings,
 * including the two adjustments below (mask `& 0xFFFFu` per
 * slice; uint64_t return on the narrow helper). */
__attribute__((always_inline))
static inline uint64_t aarch64_hole_(uint64_t magic) {
  uint64_t v;
  __asm__ volatile (
    "movz %[out], %[a]\n\t"
    "movk %[out], %[b], lsl #16\n\t"
    "movk %[out], %[c], lsl #32\n\t"
    "movk %[out], %[d], lsl #48"
    : [out] "=r" (v)
    : [a] "n" ( magic        & 0xFFFFu),
      [b] "n" ((magic >> 16) & 0xFFFFu),
      [c] "n" ((magic >> 32) & 0xFFFFu),
      [d] "n" ((magic >> 48) & 0xFFFFu)
  );
  return v;
}

/* Narrow helper — single MOVZ. Parameter is uint32_t (not
 * uint16_t) so the `& 0xFFFFu` constraint expression compiles
 * without sign-formatting issues; magics with the high bit set
 * (e.g., 0xfc24) would otherwise be printed as signed integers
 * and rejected. Return is uint64_t so clang trusts the MOVZ
 * zero-extension and doesn't emit a redundant
 * `and rN, rN, #0xffff` after each MOVZ. */
__attribute__((always_inline))
static inline uint64_t aarch64_hole_narrow_(uint32_t magic) {
  uint64_t v;
  __asm__ volatile (
    "movz %w[out], %[m]"
    : [out] "=r" (v)
    : [m]   "n"  (magic & 0xFFFFu)
  );
  return v;
}

/* Phase 4.7 32-bit chain helper — 2-slot W-form MOVZ + MOVK.
 * Used by op_load_const_uint32 / op_load_const_int32. The magic
 * is a 32-bit value with both 16-bit halves non-zero so clang
 * emits a full 2-slot chain (rather than collapsing to MOVZ + 0
 * if the upper half is zero). The extractor's pass-2 detector
 * recognizes the 2-slot W-form chain via the chain_len=2 entry
 * in kHoleMagicTable. */
__attribute__((always_inline))
__attribute__((unused))
static inline uint64_t aarch64_hole32_(uint32_t magic) {
  uint64_t v;
  __asm__ volatile (
    "movz %w[out], %[a]\n\t"
    "movk %w[out], %[b], lsl #16"
    : [out] "=r" (v)
    : [a] "n" ( magic        & 0xFFFFu),
      [b] "n" ((magic >> 16) & 0xFFFFu)
  );
  return v;
}

/* Phase 4.7: imm12-fold helpers for register-file LDR / STR.
 *
 * Replaces the `regs_i64[HOLE_NARROW(idx)]` two-instruction
 * pattern (MOVZ + indexed-LDR/STR) with a single LDR/STR
 * (immediate, unsigned offset, X-form) whose imm12 field
 * carries the patched register index. Saves one MOVZ per
 * memory access — typically 1-3 instructions per stencil.
 *
 * The magic in the source is a *byte offset* (= magic_idx * 8).
 * The assembler divides by 8 to encode imm12 = magic_idx into
 * bits 21..10 of the LDR/STR. The extractor's pass-4 walker
 * scans LDR/STR instructions and matches imm12 against
 * kHoleFoldMagicTable; the patcher rewrites bits 21..10 with
 * the operand value at JIT compile time.
 *
 * The `& 0x7FF8u` constraint mask keeps the value within the
 * imm12 range (12 bits × scale 8 = 32760 max byte offset). */
__attribute__((always_inline))
__attribute__((unused))   /* used once stencils migrate in Day 2+ */
static inline int64_t aarch64_load_reg_(uint32_t magic_byte_off,
                                         const JitState *state) {
  int64_t v;
  __asm__ volatile (
    "ldr %[out], [%[base], %[off]]"
    : [out] "=r" (v)
    : [base] "r"  (state),
      [off]  "n"  (magic_byte_off & 0x7FF8u)
  );
  return v;
}

__attribute__((always_inline))
__attribute__((unused))
static inline void aarch64_store_reg_(uint32_t magic_byte_off,
                                       JitState *state,
                                       int64_t value) {
  __asm__ volatile (
    "str %[v], [%[base], %[off]]"
    :
    : [v]    "r"  (value),
      [base] "r"  (state),
      [off]  "n"  (magic_byte_off & 0x7FF8u)
    : "memory"
  );
}

/* acc_i64[] variants. acc_i64 lives at offset 64 within JitState
 * (after regs_i64[BC_MAX_REGS=8]). To keep the imm12 hole carrying
 * just the slot index (not slot + array_base / 8), we pass
 * state->acc_i64 as the base register — clang emits an
 * `add x_base, x20, #64` once per stencil and reuses it for both
 * load and store via the imm12 fold. */
__attribute__((always_inline))
__attribute__((unused))
static inline int64_t aarch64_load_acc_(uint32_t magic_byte_off,
                                         JitState *state) {
  int64_t v;
  __asm__ volatile (
    "ldr %[out], [%[base], %[off]]"
    : [out] "=r" (v)
    : [base] "r"  (state->acc_i64),
      [off]  "n"  (magic_byte_off & 0x7FF8u)
  );
  return v;
}

__attribute__((always_inline))
__attribute__((unused))
static inline void aarch64_store_acc_(uint32_t magic_byte_off,
                                       JitState *state,
                                       int64_t value) {
  __asm__ volatile (
    "str %[v], [%[base], %[off]]"
    :
    : [v]    "r"  (value),
      [base] "r"  (state->acc_i64),
      [off]  "n"  (magic_byte_off & 0x7FF8u)
    : "memory"
  );
}

/* row_cols_i64 variant. row_cols_i64 is a *pointer* member of
 * JitState (offset 96), not an embedded array — clang loads the
 * pointer once into a register, then uses it as the imm12 base. */
__attribute__((always_inline))
__attribute__((unused))
static inline int64_t aarch64_load_col_(uint32_t magic_byte_off,
                                         const JitState *state) {
  int64_t v;
  __asm__ volatile (
    "ldr %[out], [%[base], %[off]]"
    : [out] "=r" (v)
    : [base] "r"  (state->row_cols_i64),
      [off]  "n"  (magic_byte_off & 0x7FF8u)
  );
  return v;
}

#  define HOLE(name)         aarch64_hole_(MAGIC_##name)
#  define HOLE_NARROW(name)  aarch64_hole_narrow_(MAGIC_##name##_NARROW)
#  define HOLE_32(name)      aarch64_hole32_(MAGIC_##name##_32)
#  define HOLE_LOAD_REG(name, state)         \
      aarch64_load_reg_(MAGIC_##name##_FOLD * 8u, (state))
#  define HOLE_STORE_REG(name, state, value) \
      aarch64_store_reg_(MAGIC_##name##_FOLD * 8u, (state), (value))
#  define HOLE_LOAD_ACC(name, state)         \
      aarch64_load_acc_(MAGIC_##name##_FOLD * 8u, (state))
#  define HOLE_STORE_ACC(name, state, value) \
      aarch64_store_acc_(MAGIC_##name##_FOLD * 8u, (state), (value))
#  define HOLE_LOAD_COL(name, state)         \
      aarch64_load_col_(MAGIC_##name##_FOLD * 8u, (state))
#else
#  error "unsupported architecture for stencil source"
#endif

/* ------------------------------------------------------------------ */
/* op_load_const_int : regs_i64[DST] = VAL                            */
/*                                                                    */
/* Phase 4.5: LCI_DST is a register index (≤8 bits) — narrow hole.    */
/* LCI_VAL is the int64 immediate value — stays wide.                 */
/* ------------------------------------------------------------------ */
DECLARE_FOLD_HOLE(LCI_DST);
DECLARE_HOLE(LCI_VAL);
STENCIL op_load_const_int(JitState *s) {
  HOLE_STORE_REG(LCI_DST, s, (int64_t)HOLE(LCI_VAL));
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_load_col_int : regs_i64[DST] = row_cols_i64[COL]                */
/*                                                                    */
/* Phase 4.5: both holes are indices (≤8 bits) — narrow.              */
/* ------------------------------------------------------------------ */
DECLARE_FOLD_HOLE(LRC_DST);
DECLARE_FOLD_HOLE(LRC_COL);
STENCIL op_load_col_int(JitState *s) {
  HOLE_STORE_REG(LRC_DST, s, HOLE_LOAD_COL(LRC_COL, s));
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_mov_int_int : regs_i64[DST] = regs_i64[SRC]                     */
/*                                                                    */
/* Phase 4.5: both holes are register indices — narrow.               */
/* ------------------------------------------------------------------ */
DECLARE_FOLD_HOLE(MV_DST);
DECLARE_FOLD_HOLE(MV_SRC);
STENCIL op_mov_int_int(JitState *s) {
  HOLE_STORE_REG(MV_DST, s, HOLE_LOAD_REG(MV_SRC, s));
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_add_int_int : regs_i64[DST] = regs_i64[A] + regs_i64[B]         */
/* ------------------------------------------------------------------ */
DECLARE_FOLD_HOLE(ADD_DST);
DECLARE_FOLD_HOLE(ADD_A);
DECLARE_FOLD_HOLE(ADD_B);
STENCIL op_add_int_int(JitState *s) {
  HOLE_STORE_REG(ADD_DST, s,
                 HOLE_LOAD_REG(ADD_A, s) + HOLE_LOAD_REG(ADD_B, s));
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_sum_bigint : acc_i64[SLOT] += regs_i64[SRC]                     */
/* ------------------------------------------------------------------ */
DECLARE_FOLD_HOLE(SUM_SLOT);
DECLARE_FOLD_HOLE(SUM_SRC);
STENCIL op_sum_bigint(JitState *s) {
  HOLE_STORE_ACC(SUM_SLOT, s,
                 HOLE_LOAD_ACC(SUM_SLOT, s) + HOLE_LOAD_REG(SUM_SRC, s));
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
DECLARE_FOLD_HOLE(BLT_A);
DECLARE_FOLD_HOLE(BLT_B);
extern __attribute__((preserve_none)) void HOLE_BLT_TGT(JitState *);
STENCIL op_branch_lt_int_int(JitState *s) {
  if (HOLE_LOAD_REG(BLT_A, s) < HOLE_LOAD_REG(BLT_B, s)) {
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

DECLARE_FOLD_HOLE(BLE_A);
DECLARE_FOLD_HOLE(BLE_B);
extern __attribute__((preserve_none)) void HOLE_BLE_TGT(JitState *);
STENCIL op_branch_le_int_int(JitState *s) {
  if (HOLE_LOAD_REG(BLE_A, s) <= HOLE_LOAD_REG(BLE_B, s)) {
    [[clang::musttail]] return HOLE_BLE_TGT(s);
  }
  TAIL_NEXT(s);
}

DECLARE_FOLD_HOLE(BEQ_A);
DECLARE_FOLD_HOLE(BEQ_B);
extern __attribute__((preserve_none)) void HOLE_BEQ_TGT(JitState *);
STENCIL op_branch_eq_int_int(JitState *s) {
  if (HOLE_LOAD_REG(BEQ_A, s) == HOLE_LOAD_REG(BEQ_B, s)) {
    [[clang::musttail]] return HOLE_BEQ_TGT(s);
  }
  TAIL_NEXT(s);
}

DECLARE_FOLD_HOLE(BGT_A);
DECLARE_FOLD_HOLE(BGT_B);
extern __attribute__((preserve_none)) void HOLE_BGT_TGT(JitState *);
STENCIL op_branch_gt_int_int(JitState *s) {
  if (HOLE_LOAD_REG(BGT_A, s) > HOLE_LOAD_REG(BGT_B, s)) {
    [[clang::musttail]] return HOLE_BGT_TGT(s);
  }
  TAIL_NEXT(s);
}

DECLARE_FOLD_HOLE(BGE_A);
DECLARE_FOLD_HOLE(BGE_B);
extern __attribute__((preserve_none)) void HOLE_BGE_TGT(JitState *);
STENCIL op_branch_ge_int_int(JitState *s) {
  if (HOLE_LOAD_REG(BGE_A, s) >= HOLE_LOAD_REG(BGE_B, s)) {
    [[clang::musttail]] return HOLE_BGE_TGT(s);
  }
  TAIL_NEXT(s);
}

DECLARE_FOLD_HOLE(BNE_A);
DECLARE_FOLD_HOLE(BNE_B);
extern __attribute__((preserve_none)) void HOLE_BNE_TGT(JitState *);
STENCIL op_branch_ne_int_int(JitState *s) {
  if (HOLE_LOAD_REG(BNE_A, s) != HOLE_LOAD_REG(BNE_B, s)) {
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

DECLARE_FOLD_HOLE(MINUS_DST);
DECLARE_FOLD_HOLE(MINUS_A);
DECLARE_FOLD_HOLE(MINUS_B);
STENCIL op_minus_int_int(JitState *s) {
  HOLE_STORE_REG(MINUS_DST, s,
                 HOLE_LOAD_REG(MINUS_A, s) - HOLE_LOAD_REG(MINUS_B, s));
  TAIL_NEXT(s);
}

DECLARE_FOLD_HOLE(MUL_DST);
DECLARE_FOLD_HOLE(MUL_A);
DECLARE_FOLD_HOLE(MUL_B);
STENCIL op_mul_int_int(JitState *s) {
  HOLE_STORE_REG(MUL_DST, s,
                 HOLE_LOAD_REG(MUL_A, s) * HOLE_LOAD_REG(MUL_B, s));
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_load_col_ndb : NDB cold-call variant of LoadCol.                */
/*                                                                    */
/* The stencil emits a regular C call to ndb_jit_h_load_col(), then   */
/* tail-calls next. The helper consults JitState.ctx (set by          */
/* DbtupJitGlue per row) to find the AggInterpreter / Dbtup /         */
/* KeyReqStruct, calls block_tup->readAttributes(), and writes the    */
/* int64 value into s->regs_i64[dst_reg].                             */
/*                                                                    */
/* The non-tail call is deliberate: the helper has its own stack      */
/* frame and may invoke ndbrequire / signal-block primitives that     */
/* expect a normal frame above them.                                  */
/*                                                                    */
/* Operand holes:                                                     */
/*   HOLE(LCN_COL) — col_id (uint32_t passed to helper)               */
/*   HOLE(LCN_DST) — dst register slot (uint32_t passed to helper)    */
/*                                                                    */
/* Cold-call hole:                                                    */
/*   The `call` / `bl` instruction's PC-rel32 / imm26 displacement    */
/*   is patched at JIT compile time via the helper registry.          */
/*   Symbol name "ndb_jit_h_load_col" resolves through                */
/*   jit1_lookup_helper().                                            */
/* ------------------------------------------------------------------ */
DECLARE_NARROW_HOLE(LCN_COL);
DECLARE_NARROW_HOLE(LCN_DST);
extern void ndb_jit_h_load_col(JitState *s, uint32_t col_id, uint32_t dst_reg);
STENCIL op_load_col_ndb(JitState *s) {
  ndb_jit_h_load_col(s, (uint32_t)HOLE_NARROW(LCN_COL), (uint32_t)HOLE_NARROW(LCN_DST));
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_load_const_uint16 / op_load_const_int16                         */
/*                                                                    */
/* Phase 4.7 narrow LoadConst variants. The bridge picks the          */
/* smallest-fitting variant per constant value:                       */
/*   value ∈ [0,    65535]  → op_load_const_uint16  ( 8 B post-strip) */
/*   value ∈ [-32768,  -1]  → op_load_const_int16   (12 B post-strip) */
/*                                                                    */
/* uint16 form: MOVZ + STR (no sign-extend; W-form MOVZ zero-fills    */
/*              the upper 48 bits of the X-register).                 */
/* int16 form:  MOVZ + SXTH + STR (sign-extend the 16-bit signed      */
/*              value before storing to int64 regs_i64 slot).         */
/*                                                                    */
/* The destination register index uses the imm12 fold pattern (one    */
/* STR with patched imm12). The const value uses the existing narrow- */
/* MOVZ pattern (one MOVZ with patched imm16 carrying the bit         */
/* pattern of the 16-bit value).                                      */
/* ------------------------------------------------------------------ */
DECLARE_FOLD_HOLE(LCU16_DST);
DECLARE_NARROW_HOLE(LCU16_VAL);
STENCIL op_load_const_uint16(JitState *s) {
  HOLE_STORE_REG(LCU16_DST, s, (int64_t)HOLE_NARROW(LCU16_VAL));
  TAIL_NEXT(s);
}

DECLARE_FOLD_HOLE(LCI16_DST);
DECLARE_NARROW_HOLE(LCI16_VAL);
STENCIL op_load_const_int16(JitState *s) {
  HOLE_STORE_REG(LCI16_DST, s,
                 (int64_t)(int16_t)HOLE_NARROW(LCI16_VAL));
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_load_const_uint32 / op_load_const_int32                         */
/*                                                                    */
/* Phase 4.7 wider LoadConst variants. Bridge dispatch:               */
/*   value ∈ [0, 2^32-1]            → op_load_const_uint32 (12 B)    */
/*   value ∈ [INT32_MIN, -32769]    → op_load_const_int32  (16 B)    */
/*                                                                    */
/* uint32 form: MOVZ + MOVK + STR (no sign-extend; W-form 2-slot      */
/*              chain zero-fills upper 32 bits of the X-register).    */
/* int32 form:  MOVZ + MOVK + SXTW + STR (sign-extend the 32-bit      */
/*              signed value to int64 before storing).                */
/* ------------------------------------------------------------------ */
DECLARE_FOLD_HOLE(LCU32_DST);
DECLARE_HOLE(LCU32_VAL);
STENCIL op_load_const_uint32(JitState *s) {
  HOLE_STORE_REG(LCU32_DST, s, (int64_t)HOLE_32(LCU32_VAL));
  TAIL_NEXT(s);
}

DECLARE_FOLD_HOLE(LCI32_DST);
DECLARE_HOLE(LCI32_VAL);
STENCIL op_load_const_int32(JitState *s) {
  HOLE_STORE_REG(LCI32_DST, s,
                 (int64_t)(int32_t)HOLE_32(LCI32_VAL));
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_branch_attr_eq_null / op_branch_attr_ne_null                    */
/*                                                                    */
/* Phase 5.0 cold-call branches translated from the embedded NDB     */
/* normal-interpreter's BRANCH_ATTR_EQ_NULL / BRANCH_ATTR_NE_NULL    */
/* opcodes. The 3-hole pattern combines:                             */
/*   - 1× HK_OP_B narrow MOVZ for attr_id (8-bit, ≤255).            */
/*   - 1× HK_COLDCALL bl/call for the helper.                       */
/*   - 1× HK_BRANCH_TAKE for the taken-musttail to HOLE_*_TGT.      */
/*   - 1× implicit trailing tail to next_ (HK_BRANCH_FALL,           */
/*     stripped by extractor as for hot branches).                   */
/*                                                                    */
/* Both variants share one helper (ndb_jit_h_branch_attr_null) with  */
/* a runtime want_null flag — eq=1, ne=0. The helper reads the       */
/* attribute via the existing readAttributeForJit path, checks       */
/* AttributeHeader::isNULL(), and returns 1 if the row should take   */
/* the branch (filter says "skip this row" via the embedded          */
/* EXIT_REFUSE landing pad), 0 to fall through.                      */
/* ------------------------------------------------------------------ */
extern int ndb_jit_h_branch_attr_null(JitState *s,
                                       uint32_t attr_id,
                                       uint32_t want_null);

DECLARE_NARROW_HOLE(BAEN_ATTR);
extern __attribute__((preserve_none)) void HOLE_BAEN_TGT(JitState *);
STENCIL op_branch_attr_eq_null(JitState *s) {
  if (ndb_jit_h_branch_attr_null(s,
                                  (uint32_t)HOLE_NARROW(BAEN_ATTR),
                                  /*want_null=*/1)) {
    [[clang::musttail]] return HOLE_BAEN_TGT(s);
  }
  TAIL_NEXT(s);
}

DECLARE_NARROW_HOLE(BANN_ATTR);
extern __attribute__((preserve_none)) void HOLE_BANN_TGT(JitState *);
STENCIL op_branch_attr_ne_null(JitState *s) {
  if (ndb_jit_h_branch_attr_null(s,
                                  (uint32_t)HOLE_NARROW(BANN_ATTR),
                                  /*want_null=*/0)) {
    [[clang::musttail]] return HOLE_BANN_TGT(s);
  }
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
    op_load_col_ndb,
    op_load_const_uint16,
    op_load_const_int16,
    op_load_const_uint32,
    op_load_const_int32,
    op_branch_attr_eq_null,
    op_branch_attr_ne_null,
};
