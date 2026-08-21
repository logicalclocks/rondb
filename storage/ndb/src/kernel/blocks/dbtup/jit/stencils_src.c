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
#  define HOLE_STORE_VALUE_UPDATED(name, state, value)  \
      ((state)->value_updated[HOLE(name)] = (uint64_t)(value))
#  define HOLE_STORE_VALUE_UNSIGNED(name, state, value)  \
      ((state)->value_unsigned[HOLE(name)] = (uint64_t)(value))
#  define HOLE_LOAD_VALUE_INITIALIZED(name, state)  \
      ((state)->value_initialized[HOLE(name)])
#  define HOLE_STORE_VALUE_INITIALIZED(name, state, value)  \
      ((state)->value_initialized[HOLE(name)] = (uint64_t)(value))
#  define HOLE_STORE_VALUE_DOUBLE(name, state, value)  \
      ((state)->value_double[HOLE(name)] = (uint64_t)(value))
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

__attribute__((always_inline))
__attribute__((unused))
static inline void aarch64_store_value_updated_(uint32_t magic_byte_off,
                                                JitState *state,
                                                uint64_t value) {
  __asm__ volatile (
    "str %[v], [%[base], %[off]]"
    :
    : [v]    "r"  (value),
      [base] "r"  (state->value_updated),
      [off]  "n"  (magic_byte_off & 0x7FF8u)
    : "memory"
  );
}

__attribute__((always_inline))
__attribute__((unused))
static inline void aarch64_store_value_unsigned_(uint32_t magic_byte_off,
                                                 JitState *state,
                                                 uint64_t value) {
  __asm__ volatile (
    "str %[v], [%[base], %[off]]"
    :
    : [v]    "r"  (value),
      [base] "r"  (state->value_unsigned),
      [off]  "n"  (magic_byte_off & 0x7FF8u)
    : "memory"
  );
}

__attribute__((always_inline))
__attribute__((unused))
static inline uint64_t aarch64_load_value_initialized_(uint32_t magic_byte_off,
                                                       const JitState *state) {
  uint64_t v;
  __asm__ volatile (
    "ldr %[out], [%[base], %[off]]"
    : [out] "=r" (v)
    : [base] "r"  (state->value_initialized),
      [off]  "n"  (magic_byte_off & 0x7FF8u)
  );
  return v;
}

__attribute__((always_inline))
__attribute__((unused))
static inline void aarch64_store_value_initialized_(uint32_t magic_byte_off,
                                                    JitState *state,
                                                    uint64_t value) {
  __asm__ volatile (
    "str %[v], [%[base], %[off]]"
    :
    : [v]    "r"  (value),
      [base] "r"  (state->value_initialized),
      [off]  "n"  (magic_byte_off & 0x7FF8u)
    : "memory"
  );
}

__attribute__((always_inline))
__attribute__((unused))
static inline void aarch64_store_value_double_(uint32_t magic_byte_off,
                                               JitState *state,
                                               uint64_t value) {
  __asm__ volatile (
    "str %[v], [%[base], %[off]]"
    :
    : [v]    "r"  (value),
      [base] "r"  (state->value_double),
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
#  define HOLE_STORE_VALUE_UPDATED(name, state, value) \
      aarch64_store_value_updated_(MAGIC_##name##_FOLD * 8u, (state), (value))
#  define HOLE_STORE_VALUE_UNSIGNED(name, state, value) \
      aarch64_store_value_unsigned_(MAGIC_##name##_FOLD * 8u, (state), (value))
#  define HOLE_LOAD_VALUE_INITIALIZED(name, state) \
      aarch64_load_value_initialized_(MAGIC_##name##_FOLD * 8u, (state))
#  define HOLE_STORE_VALUE_INITIALIZED(name, state, value) \
      aarch64_store_value_initialized_(MAGIC_##name##_FOLD * 8u, (state), (value))
#  define HOLE_STORE_VALUE_DOUBLE(name, state, value) \
      aarch64_store_value_double_(MAGIC_##name##_FOLD * 8u, (state), (value))
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

extern __attribute__((preserve_none)) void HOLE_ADD_OVF_TGT(JitState *);
STENCIL op_add_int_int_checked(JitState *s) {
  int64_t result;
  if (__builtin_add_overflow(HOLE_LOAD_REG(ADD_A, s),
                             HOLE_LOAD_REG(ADD_B, s),
                             &result)) {
    [[clang::musttail]] return HOLE_ADD_OVF_TGT(s);
  }
  HOLE_STORE_REG(ADD_DST, s, result);
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_sum_bigint : acc_i64[SLOT] += regs_i64[SRC]                     */
/* ------------------------------------------------------------------ */
DECLARE_FOLD_HOLE(SUM_SLOT);
DECLARE_FOLD_HOLE(SUM_SRC);
DECLARE_FOLD_HOLE(SUM_RESULT);
STENCIL op_sum_bigint(JitState *s) {
  HOLE_STORE_ACC(SUM_SLOT, s,
                 HOLE_LOAD_ACC(SUM_SLOT, s) + HOLE_LOAD_REG(SUM_SRC, s));
  HOLE_STORE_VALUE_UPDATED(SUM_RESULT, s, 1u);
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_min_bigint / op_max_bigint : conditional accumulators.          */
/*                                                                    */
/* Phase 5B. Interpreter first-row semantics (MinBigint/MaxBigint):   */
/* the first reaching row INITIALIZES the result — a fresh            */
/* accumulator's copy-in value of 0 must never win the comparison.    */
/* The value_initialized INPUT mask (set per row by the dispatch glue */
/* from AggResItem::type != UNDEFINED && !is_null) carries that       */
/* state; the stencil stores 1 after any reaching row. Signed i64     */
/* compares only — the kernel's unsigned branches are unreachable     */
/* for admitted (declared-signed BIGINT track) programs. Marks        */
/* value_updated on every reaching row (result exists once any row    */
/* reaches the op). Unchecked: no arithmetic.                         */
/* Holes shared by both stencils (like ADD's checked/unchecked pair). */
/* ------------------------------------------------------------------ */
DECLARE_FOLD_HOLE(MM_SLOT);
DECLARE_FOLD_HOLE(MM_SRC);
DECLARE_FOLD_HOLE(MM_RESULT);
STENCIL op_min_bigint(JitState *s) {
  int64_t v = HOLE_LOAD_REG(MM_SRC, s);
  if (!HOLE_LOAD_VALUE_INITIALIZED(MM_RESULT, s) ||
      v < HOLE_LOAD_ACC(MM_SLOT, s)) {
    HOLE_STORE_ACC(MM_SLOT, s, v);
  }
  HOLE_STORE_VALUE_INITIALIZED(MM_RESULT, s, 1u);
  HOLE_STORE_VALUE_UPDATED(MM_RESULT, s, 1u);
  TAIL_NEXT(s);
}

STENCIL op_max_bigint(JitState *s) {
  int64_t v = HOLE_LOAD_REG(MM_SRC, s);
  if (!HOLE_LOAD_VALUE_INITIALIZED(MM_RESULT, s) ||
      v > HOLE_LOAD_ACC(MM_SLOT, s)) {
    HOLE_STORE_ACC(MM_SLOT, s, v);
  }
  HOLE_STORE_VALUE_INITIALIZED(MM_RESULT, s, 1u);
  HOLE_STORE_VALUE_UPDATED(MM_RESULT, s, 1u);
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_count_bigint : acc_i64[SLOT] += 1 (COUNT accumulator)           */
/*                                                                    */
/* Phase 8 GROUP BY lift. Mirrors the interpreter's Count kernel for  */
/* the admitted (non-null LoadCol) contract: every row that reaches   */
/* this op counts — the interpreter's null-register skip can only     */
/* fire for rows the JIT path would have aborted at the load. COUNT   */
/* results are unsigned BIGINT, so the RESULT index marks both the    */
/* value_updated and value_unsigned masks; the writeback glue mirrors */
/* the latter into AggResItem::is_unsigned. Unchecked: Int64 row      */
/* counts cannot realistically overflow (interpreter is unchecked     */
/* here too).                                                         */
/* ------------------------------------------------------------------ */
DECLARE_FOLD_HOLE(COUNT_SLOT);
DECLARE_FOLD_HOLE(COUNT_RESULT);
STENCIL op_count_bigint(JitState *s) {
  HOLE_STORE_ACC(COUNT_SLOT, s, HOLE_LOAD_ACC(COUNT_SLOT, s) + 1);
  HOLE_STORE_VALUE_UPDATED(COUNT_RESULT, s, 1u);
  HOLE_STORE_VALUE_UNSIGNED(COUNT_RESULT, s, 1u);
  TAIL_NEXT(s);
}

extern __attribute__((preserve_none)) void HOLE_SUM_OVF_TGT(JitState *);
STENCIL op_sum_bigint_checked(JitState *s) {
  int64_t result;
  if (__builtin_add_overflow(HOLE_LOAD_ACC(SUM_SLOT, s),
                             HOLE_LOAD_REG(SUM_SRC, s),
                             &result)) {
    [[clang::musttail]] return HOLE_SUM_OVF_TGT(s);
  }
  HOLE_STORE_ACC(SUM_SLOT, s, result);
  HOLE_STORE_VALUE_UPDATED(SUM_RESULT, s, 1u);
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

extern __attribute__((preserve_none)) void HOLE_MINUS_OVF_TGT(JitState *);
STENCIL op_minus_int_int_checked(JitState *s) {
  int64_t result;
  if (__builtin_sub_overflow(HOLE_LOAD_REG(MINUS_A, s),
                             HOLE_LOAD_REG(MINUS_B, s),
                             &result)) {
    [[clang::musttail]] return HOLE_MINUS_OVF_TGT(s);
  }
  HOLE_STORE_REG(MINUS_DST, s, result);
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

extern __attribute__((preserve_none)) void HOLE_MUL_OVF_TGT(JitState *);
STENCIL op_mul_int_int_checked(JitState *s) {
  int64_t result;
  if (__builtin_mul_overflow(HOLE_LOAD_REG(MUL_A, s),
                             HOLE_LOAD_REG(MUL_B, s),
                             &result)) {
    [[clang::musttail]] return HOLE_MUL_OVF_TGT(s);
  }
  HOLE_STORE_REG(MUL_DST, s, result);
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
/*   The helper call target is patched at JIT compile time via the    */
/*   helper registry. The x86_64 extractor expands clang's rel32 call */
/*   into an absolute indirect call; aarch64 keeps a BL displacement. */
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
/* op_branch_attr_op_arg (Phase 7)                                    */
/*                                                                    */
/* Cold-call comparison branch — WHERE col <op> literal. Same 1-hole  */
/* shape as op_branch_attr_eq_null: a single narrow operand (the      */
/* instruction's word offset within ctx->prog_buf, ≤ BR_EMB_MAX_LEN   */
/* so it fits a single MOVZ) + the HK_BRANCH_TAKE target. The helper  */
/* reads the whole instruction (cond / nulls / attrId + inline        */
/* literal) from the program buffer and returns 1 to take the branch, */
/* 0 to fall through (it aborts on a fatal decode/compare error). */
extern int ndb_jit_h_branch_attr_op_arg(JitState *s, uint32_t inst_word_off);

DECLARE_NARROW_HOLE(BAOA_OFF);
extern __attribute__((preserve_none)) void HOLE_BAOA_TGT(JitState *);
STENCIL op_branch_attr_op_arg(JitState *s) {
  if (ndb_jit_h_branch_attr_op_arg(s,
                                   (uint32_t)HOLE_NARROW(BAOA_OFF))) {
    [[clang::musttail]] return HOLE_BAOA_TGT(s);
  }
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_load_linked_to_mem (Phase 5.1a)                                 */
/*                                                                    */
/* Cold-call (no branch) — populates ctx->block_tup->cheapMemory[0]   */
/* from the row's linked-attr buffer at the patched position. The     */
/* helper delegates to NDB's existing READ_LINKED_TO_MEM logic.       */
/* ------------------------------------------------------------------ */
extern void ndb_jit_h_read_linked_to_mem(JitState *s, uint32_t position);

DECLARE_NARROW_HOLE(LLM_POS);
STENCIL op_load_linked_to_mem(JitState *s) {
  ndb_jit_h_read_linked_to_mem(s, (uint32_t)HOLE_NARROW(LLM_POS));
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_branch_linked_eq_null / op_branch_linked_ne_null (Phase 5.1a)   */
/*                                                                    */
/* Cold-call branches that null-check the AttributeHeader at          */
/* cheapMemory[0] (populated by a preceding op_load_linked_to_mem).   */
/* No per-row attr_id operand — the bridge ensures the preceding      */
/* LOAD_LINKED instruction set up the right entry. Both variants      */
/* share one helper via runtime want_null (eq=1, ne=0).               */
/* ------------------------------------------------------------------ */
extern int ndb_jit_h_branch_linked_null(JitState *s, uint32_t want_null);

extern __attribute__((preserve_none)) void HOLE_BLEN_TGT(JitState *);
STENCIL op_branch_linked_eq_null(JitState *s) {
  if (ndb_jit_h_branch_linked_null(s, /*want_null=*/1)) {
    [[clang::musttail]] return HOLE_BLEN_TGT(s);
  }
  TAIL_NEXT(s);
}

extern __attribute__((preserve_none)) void HOLE_BLNN_TGT(JitState *);
STENCIL op_branch_linked_ne_null(JitState *s) {
  if (ndb_jit_h_branch_linked_null(s, /*want_null=*/0)) {
    [[clang::musttail]] return HOLE_BLNN_TGT(s);
  }
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* op_jump : unconditional forward branch.                            */
/* Used by embedded CASE accept paths after WRITE_INTERPRETER_OUTPUT   */
/* selects a non-zero skip_offset in the outer aggregation program.    */
/* ------------------------------------------------------------------ */
extern __attribute__((preserve_none)) void HOLE_JMP_TGT(JitState *);
STENCIL op_jump(JitState *s) {
  [[clang::musttail]] return HOLE_JMP_TGT(s);
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
/* op_overflow_exit : checked arithmetic overflow terminator.          */
/* The extractor overrides this return with the engine terminator,     */
/* matching op_skip/op_exit.                                           */
/* ------------------------------------------------------------------ */
STENCIL op_overflow_exit(JitState *s) {
  s->row_overflowed = 1u;
  return;
}

STENCIL op_filter_reject_exit(JitState *s) {
  s->row_filter_rejected = 1u;
  return;
}

/* ------------------------------------------------------------------ */
/* Phase 5C-2 — the DOUBLE family.                                    */
/*                                                                    */
/* f64 values live BIT-CAST in the existing regs_i64 / acc_i64 slots: */
/* the accessors below load the i64 bits through the same imm12-fold  */
/* holes as the integer stencils and reinterpret via __builtin_memcpy */
/* (clang lowers to a plain fmov/movq — no memory round-trip, no      */
/* strict-aliasing UB). No new register-file accessor machinery.      */
/*                                                                    */
/* Finiteness checks are done on the BIT PATTERN                      */
/* ((bits & ~sign) >= exponent-mask ⇔ inf or NaN) instead of          */
/* __builtin_isfinite: the latter compares against an FP constant     */
/* that clang materialises in .rodata — a relocation class the        */
/* extractor doesn't support. The integer immediates below lower to   */
/* movabs (x86_64) / AND-bitmask-immediate + shifted MOVZ (aarch64),  */
/* both inline. (0x7ff0'0000'0000'0000 is MOVZ #0x7ff0, lsl #48 —     */
/* imm16 slot 0x7ff0 collides with no narrow magic.)                  */
/*                                                                    */
/* Non-finite results branch to the shared HOLE_F64_OVF_TGT           */
/* (HK_OVERFLOW_TAKE, patched from op->d) — the interpreter kernels   */
/* return ZAGG_MATH_OVERFLOW on !isfinite. Divisor == 0 in            */
/* op_div_f64 does NOT branch: the kernel NULLs the result register   */
/* (SQL x/0 = NULL) which the JIT cannot represent until 5D, so the   */
/* stencil sets JitState::row_fallback INLINE (plain baked-offset     */
/* field store, the 5A helper convention) and keeps executing — the   */
/* glue discards the row and the interpreter re-runs it.              */
/* ------------------------------------------------------------------ */

__attribute__((always_inline))
static inline double f64_from_bits_(int64_t bits) {
  double d;
  __builtin_memcpy(&d, &bits, sizeof(d));
  return d;
}

__attribute__((always_inline))
static inline int64_t f64_to_bits_(double d) {
  int64_t bits;
  __builtin_memcpy(&bits, &d, sizeof(bits));
  return bits;
}

__attribute__((always_inline))
static inline int f64_bits_nonfinite_(int64_t bits) {
  return (bits & INT64_C(0x7FFFFFFFFFFFFFFF)) >=
         INT64_C(0x7FF0000000000000);
}

/* op_add_f64 / op_minus_f64 / op_mul_f64 / op_div_f64 — same operand
 * layout as the checked int arithmetic (a=dst, b=lhs, c=rhs,
 * d=overflow-exit pc), shared FAR_* holes (like the MM_* pair). */
DECLARE_FOLD_HOLE(FAR_DST);
DECLARE_FOLD_HOLE(FAR_A);
DECLARE_FOLD_HOLE(FAR_B);
extern __attribute__((preserve_none)) void HOLE_F64_OVF_TGT(JitState *);

STENCIL op_add_f64(JitState *s) {
  int64_t rb = f64_to_bits_(f64_from_bits_(HOLE_LOAD_REG(FAR_A, s)) +
                            f64_from_bits_(HOLE_LOAD_REG(FAR_B, s)));
  if (f64_bits_nonfinite_(rb)) {
    [[clang::musttail]] return HOLE_F64_OVF_TGT(s);
  }
  HOLE_STORE_REG(FAR_DST, s, rb);
  TAIL_NEXT(s);
}

STENCIL op_minus_f64(JitState *s) {
  int64_t rb = f64_to_bits_(f64_from_bits_(HOLE_LOAD_REG(FAR_A, s)) -
                            f64_from_bits_(HOLE_LOAD_REG(FAR_B, s)));
  if (f64_bits_nonfinite_(rb)) {
    [[clang::musttail]] return HOLE_F64_OVF_TGT(s);
  }
  HOLE_STORE_REG(FAR_DST, s, rb);
  TAIL_NEXT(s);
}

STENCIL op_mul_f64(JitState *s) {
  int64_t rb = f64_to_bits_(f64_from_bits_(HOLE_LOAD_REG(FAR_A, s)) *
                            f64_from_bits_(HOLE_LOAD_REG(FAR_B, s)));
  if (f64_bits_nonfinite_(rb)) {
    [[clang::musttail]] return HOLE_F64_OVF_TGT(s);
  }
  HOLE_STORE_REG(FAR_DST, s, rb);
  TAIL_NEXT(s);
}

STENCIL op_div_f64(JitState *s) {
  double divisor = f64_from_bits_(HOLE_LOAD_REG(FAR_B, s));
  int64_t rb;
  if (divisor == 0.0) {
    /* Kernel semantics: result register becomes NULL. Take the per-row
     * interpreter fallback; store a harmless 0.0 so downstream ops of
     * this (discarded) run stay finite. One store site + one TAIL_NEXT
     * for the whole stencil — the extractor strips exactly one
     * trailing tail-call. */
    s->row_fallback = 1u;
    rb = 0;
  } else {
    rb = f64_to_bits_(f64_from_bits_(HOLE_LOAD_REG(FAR_A, s)) / divisor);
    if (f64_bits_nonfinite_(rb)) {
      [[clang::musttail]] return HOLE_F64_OVF_TGT(s);
    }
  }
  HOLE_STORE_REG(FAR_DST, s, rb);
  TAIL_NEXT(s);
}

/* op_sum_f64 — SumDouble's shape: first-row-initialize via the
 * value_initialized input mask (matters even for SUM — the kernel
 * stores the first value verbatim, and 0.0 + -0.0 would flip the sign
 * of a single-row SUM(-0.0)); non-finite update → overflow exit (the
 * init path is unchecked, like the kernel). Marks value_updated,
 * value_initialized AND value_double. a=slot, b=src, c=result,
 * d=overflow-exit pc. */
DECLARE_FOLD_HOLE(FSUM_SLOT);
DECLARE_FOLD_HOLE(FSUM_SRC);
DECLARE_FOLD_HOLE(FSUM_RESULT);
STENCIL op_sum_f64(JitState *s) {
  int64_t src_bits = HOLE_LOAD_REG(FSUM_SRC, s);
  if (!HOLE_LOAD_VALUE_INITIALIZED(FSUM_RESULT, s)) {
    HOLE_STORE_ACC(FSUM_SLOT, s, src_bits);
  } else {
    int64_t rb =
        f64_to_bits_(f64_from_bits_(HOLE_LOAD_ACC(FSUM_SLOT, s)) +
                     f64_from_bits_(src_bits));
    if (f64_bits_nonfinite_(rb)) {
      [[clang::musttail]] return HOLE_F64_OVF_TGT(s);
    }
    HOLE_STORE_ACC(FSUM_SLOT, s, rb);
  }
  HOLE_STORE_VALUE_INITIALIZED(FSUM_RESULT, s, 1u);
  HOLE_STORE_VALUE_UPDATED(FSUM_RESULT, s, 1u);
  HOLE_STORE_VALUE_DOUBLE(FSUM_RESULT, s, 1u);
  TAIL_NEXT(s);
}

/* op_min_f64 / op_max_f64 — the 5B MIN/MAX shape with double compares
 * (plain < / >, matching MinDouble/MaxDouble; NaN cannot reach them —
 * column values are finite by storage contract and every arithmetic
 * producer overflow-exits on non-finite). Marks value_double alongside
 * updated/initialized. Shared FMM_* holes. Unchecked — no arithmetic. */
DECLARE_FOLD_HOLE(FMM_SLOT);
DECLARE_FOLD_HOLE(FMM_SRC);
DECLARE_FOLD_HOLE(FMM_RESULT);
STENCIL op_min_f64(JitState *s) {
  int64_t vb = HOLE_LOAD_REG(FMM_SRC, s);
  if (!HOLE_LOAD_VALUE_INITIALIZED(FMM_RESULT, s) ||
      f64_from_bits_(vb) < f64_from_bits_(HOLE_LOAD_ACC(FMM_SLOT, s))) {
    HOLE_STORE_ACC(FMM_SLOT, s, vb);
  }
  HOLE_STORE_VALUE_INITIALIZED(FMM_RESULT, s, 1u);
  HOLE_STORE_VALUE_UPDATED(FMM_RESULT, s, 1u);
  HOLE_STORE_VALUE_DOUBLE(FMM_RESULT, s, 1u);
  TAIL_NEXT(s);
}

STENCIL op_max_f64(JitState *s) {
  int64_t vb = HOLE_LOAD_REG(FMM_SRC, s);
  if (!HOLE_LOAD_VALUE_INITIALIZED(FMM_RESULT, s) ||
      f64_from_bits_(vb) > f64_from_bits_(HOLE_LOAD_ACC(FMM_SLOT, s))) {
    HOLE_STORE_ACC(FMM_SLOT, s, vb);
  }
  HOLE_STORE_VALUE_INITIALIZED(FMM_RESULT, s, 1u);
  HOLE_STORE_VALUE_UPDATED(FMM_RESULT, s, 1u);
  HOLE_STORE_VALUE_DOUBLE(FMM_RESULT, s, 1u);
  TAIL_NEXT(s);
}

/* op_load_col_ndb_f64 — cold-call load for declared FLOAT/DOUBLE
 * columns; same shape as op_load_col_ndb. The helper decodes by the
 * column's declared type (DOUBLE: bit copy; FLOAT: promote to double)
 * and stores the double's bit pattern into regs_i64[dst]; NULL or an
 * unexpected declared type takes the per-row fallback. */
DECLARE_NARROW_HOLE(LCF_COL);
DECLARE_NARROW_HOLE(LCF_DST);
extern void ndb_jit_h_load_col_f64(JitState *s, uint32_t col_id,
                                   uint32_t dst_reg);
STENCIL op_load_col_ndb_f64(JitState *s) {
  ndb_jit_h_load_col_f64(s, (uint32_t)HOLE_NARROW(LCF_COL),
                         (uint32_t)HOLE_NARROW(LCF_DST));
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* Phase 5C-3 — unsigned BIGINT.                                      */
/*                                                                    */
/* u64 values live bit-cast in regs_i64 / acc_i64. The bridge admits  */
/* these ops only for statically-proven-unsigned sources and enforces */
/* uniform signedness per accumulator slot, so the interpreter        */
/* kernels' mixed signed/unsigned branches are unreachable for        */
/* admitted programs — the stencils implement only the uniform-       */
/* unsigned paths: u64 add with carry check for SUM (kernel:          */
/* TestIfSumOverflowsUint64 ⇒ ZAGG_MATH_OVERFLOW), plain u64          */
/* compares for MIN/MAX. All three mark value_unsigned alongside      */
/* value_updated (the kernels produce is_unsigned = true), and reuse  */
/* the SUM_* / MM_* holes — shared across signedness variants the     */
/* same way ADD_* is shared across checked/unchecked.                 */
/* ------------------------------------------------------------------ */

STENCIL op_sum_u64_checked(JitState *s) {
  uint64_t result;
  if (__builtin_add_overflow((uint64_t)HOLE_LOAD_ACC(SUM_SLOT, s),
                             (uint64_t)HOLE_LOAD_REG(SUM_SRC, s),
                             &result)) {
    [[clang::musttail]] return HOLE_SUM_OVF_TGT(s);
  }
  HOLE_STORE_ACC(SUM_SLOT, s, (int64_t)result);
  HOLE_STORE_VALUE_UPDATED(SUM_RESULT, s, 1u);
  HOLE_STORE_VALUE_UNSIGNED(SUM_RESULT, s, 1u);
  TAIL_NEXT(s);
}

STENCIL op_min_u64(JitState *s) {
  uint64_t v = (uint64_t)HOLE_LOAD_REG(MM_SRC, s);
  if (!HOLE_LOAD_VALUE_INITIALIZED(MM_RESULT, s) ||
      v < (uint64_t)HOLE_LOAD_ACC(MM_SLOT, s)) {
    HOLE_STORE_ACC(MM_SLOT, s, (int64_t)v);
  }
  HOLE_STORE_VALUE_INITIALIZED(MM_RESULT, s, 1u);
  HOLE_STORE_VALUE_UPDATED(MM_RESULT, s, 1u);
  HOLE_STORE_VALUE_UNSIGNED(MM_RESULT, s, 1u);
  TAIL_NEXT(s);
}

STENCIL op_max_u64(JitState *s) {
  uint64_t v = (uint64_t)HOLE_LOAD_REG(MM_SRC, s);
  if (!HOLE_LOAD_VALUE_INITIALIZED(MM_RESULT, s) ||
      v > (uint64_t)HOLE_LOAD_ACC(MM_SLOT, s)) {
    HOLE_STORE_ACC(MM_SLOT, s, (int64_t)v);
  }
  HOLE_STORE_VALUE_INITIALIZED(MM_RESULT, s, 1u);
  HOLE_STORE_VALUE_UPDATED(MM_RESULT, s, 1u);
  HOLE_STORE_VALUE_UNSIGNED(MM_RESULT, s, 1u);
  TAIL_NEXT(s);
}

/* Phase 5C-4 — unsigned checked arithmetic. u64 add/sub/mul with
 * unsigned overflow/borrow checks, matching the kernels' uniform-
 * unsigned (and unsigned x nonneg-const) paths: any u64 overflow or
 * borrow ⇒ ZAGG_MATH_OVERFLOW via the per-family overflow targets.
 * Reuses the ADD / MINUS / MUL fold-hole families and OVF symbols
 * the same way op_sum_u64_checked reuses the SUM ones. */
STENCIL op_add_u64_checked(JitState *s) {
  uint64_t result;
  if (__builtin_add_overflow((uint64_t)HOLE_LOAD_REG(ADD_A, s),
                             (uint64_t)HOLE_LOAD_REG(ADD_B, s),
                             &result)) {
    [[clang::musttail]] return HOLE_ADD_OVF_TGT(s);
  }
  HOLE_STORE_REG(ADD_DST, s, (int64_t)result);
  TAIL_NEXT(s);
}

STENCIL op_minus_u64_checked(JitState *s) {
  uint64_t result;
  if (__builtin_sub_overflow((uint64_t)HOLE_LOAD_REG(MINUS_A, s),
                             (uint64_t)HOLE_LOAD_REG(MINUS_B, s),
                             &result)) {
    [[clang::musttail]] return HOLE_MINUS_OVF_TGT(s);
  }
  HOLE_STORE_REG(MINUS_DST, s, (int64_t)result);
  TAIL_NEXT(s);
}

STENCIL op_mul_u64_checked(JitState *s) {
  uint64_t result;
  if (__builtin_mul_overflow((uint64_t)HOLE_LOAD_REG(MUL_A, s),
                             (uint64_t)HOLE_LOAD_REG(MUL_B, s),
                             &result)) {
    [[clang::musttail]] return HOLE_MUL_OVF_TGT(s);
  }
  HOLE_STORE_REG(MUL_DST, s, (int64_t)result);
  TAIL_NEXT(s);
}

/* op_load_col_ndb_u64 — cold-call load for declared BIGUNSIGNED
 * columns; same shape as op_load_col_ndb. The helper stores the u64
 * bits into regs_i64[dst]; NULL or an unexpected declared type takes
 * the per-row fallback. Kept separate from the signed helper so a
 * schema drift cannot feed u64 bits into a signed-contract site. */
DECLARE_NARROW_HOLE(LU64_COL);
DECLARE_NARROW_HOLE(LU64_DST);
extern void ndb_jit_h_load_col_u64(JitState *s, uint32_t col_id,
                                   uint32_t dst_reg);
STENCIL op_load_col_ndb_u64(JitState *s) {
  ndb_jit_h_load_col_u64(s, (uint32_t)HOLE_NARROW(LU64_COL),
                         (uint32_t)HOLE_NARROW(LU64_DST));
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* Phase 5D-1 — op_load_col_ndb_nb: NULL-BRANCHING load.              */
/*                                                                    */
/* A cold-call BRANCH in op_branch_attr_eq_null's shape: the helper   */
/* loads like ndb_jit_h_load_col but RETURNS "value was NULL", and    */
/* the taken edge (HOLE_LCNB_TGT, patched from op->c) skips the       */
/* loaded register's whole consumer chain — the bridge computes the   */
/* target with a taint walk. This reproduces the interpreter          */
/* kernels' null-skip exactly, keeping NULL rows on the JIT instead   */
/* of the per-row interpreter fallback. Read errors / declared-type   */
/* mismatches still take the row_fallback defense inside the helper   */
/* (return 0, blob continues, glue discards the row).                 */
/* ------------------------------------------------------------------ */
DECLARE_NARROW_HOLE(LCNB_COL);
DECLARE_NARROW_HOLE(LCNB_DST);
extern int ndb_jit_h_load_col_nb(JitState *s, uint32_t col_id,
                                 uint32_t dst_reg);
extern __attribute__((preserve_none)) void HOLE_LCNB_TGT(JitState *);
STENCIL op_load_col_ndb_nb(JitState *s) {
  if (ndb_jit_h_load_col_nb(s, (uint32_t)HOLE_NARROW(LCNB_COL),
                            (uint32_t)HOLE_NARROW(LCNB_DST))) {
    [[clang::musttail]] return HOLE_LCNB_TGT(s);
  }
  TAIL_NEXT(s);
}

/* Phase 5D-2: f64/u64 siblings of the null-branching load — same
 * shape, decoding like their void row-fallback counterparts (FLOAT
 * promotes to double; strict declared-type contracts). */
DECLARE_NARROW_HOLE(LFNB_COL);
DECLARE_NARROW_HOLE(LFNB_DST);
extern int ndb_jit_h_load_col_f64_nb(JitState *s, uint32_t col_id,
                                     uint32_t dst_reg);
extern __attribute__((preserve_none)) void HOLE_LFNB_TGT(JitState *);
STENCIL op_load_col_ndb_f64_nb(JitState *s) {
  if (ndb_jit_h_load_col_f64_nb(s, (uint32_t)HOLE_NARROW(LFNB_COL),
                                (uint32_t)HOLE_NARROW(LFNB_DST))) {
    [[clang::musttail]] return HOLE_LFNB_TGT(s);
  }
  TAIL_NEXT(s);
}

DECLARE_NARROW_HOLE(LUNB_COL);
DECLARE_NARROW_HOLE(LUNB_DST);
extern int ndb_jit_h_load_col_u64_nb(JitState *s, uint32_t col_id,
                                     uint32_t dst_reg);
extern __attribute__((preserve_none)) void HOLE_LUNB_TGT(JitState *);
STENCIL op_load_col_ndb_u64_nb(JitState *s) {
  if (ndb_jit_h_load_col_u64_nb(s, (uint32_t)HOLE_NARROW(LUNB_COL),
                                (uint32_t)HOLE_NARROW(LUNB_DST))) {
    [[clang::musttail]] return HOLE_LUNB_TGT(s);
  }
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* Phase 5G — op_load_col_ndb_dec: DECIMAL column load (cold call).   */
/*                                                                    */
/* The helper mirrors the interpreter's bin2decimal + conversion:     */
/* scale 0 stores an i64 (or u64 for DECIMALUNSIGNED), scale > 0 the  */
/* double's bit pattern. pinfo packs (is_unsigned << 15) |            */
/* (precision << 8) | scale. NULL and every conversion error take    */
/* the per-row interpreter fallback inside the helper.                */
/* ------------------------------------------------------------------ */
DECLARE_NARROW_HOLE(LCD_COL);
DECLARE_NARROW_HOLE(LCD_DST);
DECLARE_NARROW_HOLE(LCD_INFO);
extern void ndb_jit_h_load_col_dec(JitState *s, uint32_t col_id,
                                   uint32_t dst_reg, uint32_t pinfo);
STENCIL op_load_col_ndb_dec(JitState *s) {
  ndb_jit_h_load_col_dec(s, (uint32_t)HOLE_NARROW(LCD_COL),
                         (uint32_t)HOLE_NARROW(LCD_DST),
                         (uint32_t)HOLE_NARROW(LCD_INFO));
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* Phase 5F-1 — op_minmax_str_ndb: FUSED string MIN/MAX (cold call).  */
/*                                                                    */
/* One call covers load + collation compare + winner-buffer update    */
/* via the interpreter's own kernel (see bytecode1.h). packed =       */
/* (is_max << 8) | agg_index.                                         */
/* ------------------------------------------------------------------ */
DECLARE_NARROW_HOLE(MMS_COL);
DECLARE_NARROW_HOLE(MMS_ARG);
extern void ndb_jit_h_minmax_str(JitState *s, uint32_t col_id,
                                 uint32_t packed);
STENCIL op_minmax_str_ndb(JitState *s) {
  ndb_jit_h_minmax_str(s, (uint32_t)HOLE_NARROW(MMS_COL),
                       (uint32_t)HOLE_NARROW(MMS_ARG));
  TAIL_NEXT(s);
}

/* ------------------------------------------------------------------ */
/* Integer division / modulo (Phase 5E-2).                            */
/*                                                                    */
/* Kernel equivalences: RegDivIntBigint / RegModReg integer paths.    */
/* Divisor 0 → the interpreter NULLs the result register — take the   */
/* per-row fallback (op_div_f64's zero-divisor pattern; the stored 0  */
/* keeps this discarded run's downstream ops well-defined). The       */
/* signed pair guard dividend == INT64_MIN && divisor == -1 BEFORE    */
/* dividing: x86-64 idiv TRAPS on that input (DIV's magnitude 2^63    */
/* overflows → OVF target; MOD's result is 0 — a remainder is always  */
/* smaller in magnitude than the divisor, so MOD needs no overflow    */
/* hole). One store site + one TAIL_NEXT each.                        */
/* ------------------------------------------------------------------ */
DECLARE_FOLD_HOLE(DIV_DST);
DECLARE_FOLD_HOLE(DIV_A);
DECLARE_FOLD_HOLE(DIV_B);
extern __attribute__((preserve_none)) void HOLE_DIV_OVF_TGT(JitState *);
/* All four bodies are BRANCHLESS past their (at most one) musttail
 * branch: a divisor==0 path with its own store + TAIL_NEXT tempts
 * clang into tail-duplicating the exit, and a duplicated jmp-to-next
 * is exactly what the STRIP_TAIL extractor cannot strip (seen live
 * on arm64: "unrecognised relocation target=next" mid-body). Instead
 * the zero case divides by a SAFE divisor of 1 and csel's the result
 * to 0, with row_fallback OR-ed in unconditionally. */
STENCIL op_div_int_checked(JitState *s) {
  int64_t divisor  = HOLE_LOAD_REG(DIV_B, s);
  int64_t dividend = HOLE_LOAD_REG(DIV_A, s);
  if (dividend == INT64_MIN && divisor == -1) {
    [[clang::musttail]] return HOLE_DIV_OVF_TGT(s);
  }
  uint32_t zero = (divisor == 0);
  s->row_fallback |= zero;
  /* Past the overflow guard, divisor == -1 implies dividend !=
   * INT64_MIN — the unconditional divide below cannot trap. */
  int64_t safe = zero ? 1 : divisor;
  int64_t result = dividend / safe;
  HOLE_STORE_REG(DIV_DST, s, zero ? 0 : result);
  TAIL_NEXT(s);
}

STENCIL op_div_u64(JitState *s) {
  uint64_t divisor  = (uint64_t)HOLE_LOAD_REG(DIV_B, s);
  uint64_t dividend = (uint64_t)HOLE_LOAD_REG(DIV_A, s);
  uint32_t zero = (divisor == 0);
  s->row_fallback |= zero;
  uint64_t safe = zero ? 1u : divisor;
  uint64_t result = dividend / safe;
  HOLE_STORE_REG(DIV_DST, s, zero ? 0 : (int64_t)result);
  TAIL_NEXT(s);
}

DECLARE_FOLD_HOLE(MOD_DST);
DECLARE_FOLD_HOLE(MOD_A);
DECLARE_FOLD_HOLE(MOD_B);
STENCIL op_mod_int(JitState *s) {
  int64_t divisor  = HOLE_LOAD_REG(MOD_B, s);
  int64_t dividend = HOLE_LOAD_REG(MOD_A, s);
  uint32_t zero = (divisor == 0);
  s->row_fallback |= zero;
  int64_t safe = zero ? 1 : divisor;
  /* x % -1 == 0 for EVERY x, so forcing the dividend to 0 when the
   * divisor is -1 is exact — and removes the INT64_MIN % -1 input
   * that traps x86-64's idiv. */
  int64_t safe_dividend = (safe == -1) ? 0 : dividend;
  int64_t result = safe_dividend % safe;
  HOLE_STORE_REG(MOD_DST, s, zero ? 0 : result);
  TAIL_NEXT(s);
}

STENCIL op_mod_u64(JitState *s) {
  uint64_t divisor  = (uint64_t)HOLE_LOAD_REG(MOD_B, s);
  uint64_t dividend = (uint64_t)HOLE_LOAD_REG(MOD_A, s);
  uint32_t zero = (divisor == 0);
  s->row_fallback |= zero;
  uint64_t safe = zero ? 1u : divisor;
  uint64_t result = dividend % safe;
  HOLE_STORE_REG(MOD_DST, s, zero ? 0 : (int64_t)result);
  TAIL_NEXT(s);
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
    op_count_bigint,
    op_min_bigint,
    op_max_bigint,
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
    op_load_linked_to_mem,
    op_branch_linked_eq_null,
    op_branch_linked_ne_null,
    op_add_int_int_checked,
    op_minus_int_int_checked,
    op_mul_int_int_checked,
    op_sum_bigint_checked,
    op_overflow_exit,
    op_jump,
    op_filter_reject_exit,
    op_branch_attr_op_arg,
    op_add_f64,
    op_minus_f64,
    op_mul_f64,
    op_div_f64,
    op_sum_f64,
    op_min_f64,
    op_max_f64,
    op_load_col_ndb_f64,
    op_sum_u64_checked,
    op_min_u64,
    op_max_u64,
    op_load_col_ndb_u64,
    op_load_col_ndb_nb,
    op_load_col_ndb_f64_nb,
    op_load_col_ndb_u64_nb,
    op_add_u64_checked,
    op_minus_u64_checked,
    op_mul_u64_checked,
    op_load_col_ndb_dec,
    op_minmax_str_ndb,
    op_div_int_checked,
    op_mod_int,
    op_div_u64,
    op_mod_u64,
};
