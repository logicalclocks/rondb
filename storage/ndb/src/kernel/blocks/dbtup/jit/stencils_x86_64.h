/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license as bytecode1.h.)
 */

/*
 * RONDB-1056 Phase 1 — hand-extracted x86_64 stencil bytes.
 *
 * Hand-extracted from stencils_src.c per phase_1_implementation.md
 * §7. The Phase 2 extractor will replace this with a generated
 * artefact; for Phase 1 the bytes live here so we can validate the
 * technique before sinking effort into tooling.
 *
 * Extraction recipe (reproduce):
 *
 *     clang --target=x86_64-pc-linux-gnu \
 *           -O2 -fno-asynchronous-unwind-tables -ffreestanding \
 *           -fno-stack-protector -fno-pic -std=c11 \
 *           -c stencils_src.c -o stencils.o
 *     objdump -d -M intel stencils.o
 *     objdump -r stencils.o
 *
 * Source toolchain at extraction time:
 *   - Apple clang 17.0.0 (clang-1700.0.13.5), cross-compiling to
 *     x86_64-pc-linux-gnu.
 *
 * If a different clang produces different bytes:
 *   - Most likely changes are nop-padding sizes between stencils
 *     (irrelevant — we read each stencil's bytes individually) or
 *     instruction reordering inside a stencil. The latter shifts
 *     hole offsets; update both `bytes_*` and `holes_*` together.
 *   - The relocation TYPES should remain stable across clang
 *     versions (R_X86_64_32 / R_X86_64_32S for index/imm holes,
 *     R_X86_64_PLT32 for PC-relative branch displacements).
 *
 * All offsets and bytes below were captured from a verified
 * disassembly + relocation dump; see /tmp/dis.txt during the
 * Phase 1 Day 2 implementation session for the source.
 */

#ifndef NDB_JIT_STENCILS_X86_64_H
#define NDB_JIT_STENCILS_X86_64_H

#if !defined(__x86_64__)
#error "stencils_x86_64.h is only valid on x86_64 build targets. " \
       "Phase 1 does not provide stencils for other architectures."
#endif

#include <stddef.h>
#include <stdint.h>

#include "bytecode1.h"

#ifdef __cplusplus
extern "C" {
#endif

/* What value gets baked into the patch site at JIT time. The engine
 * (jit1.c) reads the corresponding field from the Op record. */
typedef enum {
  HK_OP_A,             /* op->a (register / accumulator slot) */
  HK_OP_B,             /* op->b (register / column index) */
  HK_OP_C,             /* op->c (register) */
  HK_OP_IMM,           /* op->imm (signed int — fits 32 bits sign-extended) */
  HK_BRANCH_FALL,      /* PC-relative disp from patch site to next stencil */
  HK_BRANCH_TAKE,      /* PC-relative disp from patch site to op.c bytecode pc */
} HoleKind;

typedef struct {
  uint16_t byte_offset;   /* into the stencil's bytes_*[] */
  uint8_t  kind;          /* HoleKind */
  uint8_t  width;         /* 4 — Phase 1 uses only 4-byte holes on x86_64 */
} Hole;

typedef struct {
  const uint8_t *bytes;
  uint16_t       n_bytes;
  const Hole    *holes;
  uint8_t        n_holes;
} Stencil;

/* ------------------------------------------------------------------ */
/* op_load_const_int (OpKind = OP_LOAD_CONST_INT, 13 bytes)            */
/*                                                                    */
/* Source: stencils.o offset 0x00..0x0c (trailing 5-byte jmp to       */
/* `next` at 0x0d-0x11 stripped).                                     */
/*                                                                    */
/*   00: b8 00 00 00 00               mov  eax, HOLE_LCI_DST          */
/*   05: 49 c7 04 c4 00 00 00 00      mov  qword [r12 + 8*rax],       */
/*                                          HOLE_LCI_VAL (sign-ext)  */
/*                                                                    */
/* Holes:                                                             */
/*   off=1  width=4 R_X86_64_32   HOLE_LCI_DST  → patch with op->a    */
/*   off=9  width=4 R_X86_64_32S  HOLE_LCI_VAL  → patch with op->imm  */
/* ------------------------------------------------------------------ */
static const uint8_t bytes_op_load_const_int[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00,
  0x49, 0xc7, 0x04, 0xc4, 0x00, 0x00, 0x00, 0x00,
};
static const Hole holes_op_load_const_int[] = {
  { .byte_offset = 1, .kind = HK_OP_A,   .width = 4 },
  { .byte_offset = 9, .kind = HK_OP_IMM, .width = 4 },
};

/* ------------------------------------------------------------------ */
/* op_load_col_int (OpKind = OP_LOAD_COL_INT, 23 bytes)               */
/*                                                                    */
/* Source: stencils.o offset 0x20..0x36 (jmp at 0x37-0x3b stripped).   */
/*                                                                    */
/*   00: 49 8b 44 24 60                mov  rax, [r12 + 0x60]         */
/*                                          ; load row_cols_i64 ptr  */
/*   05: b9 00 00 00 00                mov  ecx, HOLE_LRC_COL         */
/*   0a: 48 8b 04 c8                   mov  rax, [rax + 8*rcx]        */
/*   0e: b9 00 00 00 00                mov  ecx, HOLE_LRC_DST         */
/*   13: 49 89 04 cc                   mov  [r12 + 8*rcx], rax        */
/*                                                                    */
/* Holes:                                                             */
/*   off=6  width=4 R_X86_64_32   HOLE_LRC_COL  → patch with op->b    */
/*   off=15 width=4 R_X86_64_32   HOLE_LRC_DST  → patch with op->a    */
/* ------------------------------------------------------------------ */
static const uint8_t bytes_op_load_col_int[] = {
  0x49, 0x8b, 0x44, 0x24, 0x60,
  0xb9, 0x00, 0x00, 0x00, 0x00,
  0x48, 0x8b, 0x04, 0xc8,
  0xb9, 0x00, 0x00, 0x00, 0x00,
  0x49, 0x89, 0x04, 0xcc,
};
static const Hole holes_op_load_col_int[] = {
  { .byte_offset =  6, .kind = HK_OP_B, .width = 4 },
  { .byte_offset = 15, .kind = HK_OP_A, .width = 4 },
};

/* ------------------------------------------------------------------ */
/* op_mov_int_int (OpKind = OP_MOV_INT_INT, 18 bytes)                 */
/*                                                                    */
/* Source: stencils.o offset 0x40..0x51 (jmp at 0x52-0x56 stripped).   */
/*                                                                    */
/*   00: b8 00 00 00 00                mov  eax, HOLE_MV_SRC          */
/*   05: 49 8b 04 c4                   mov  rax, [r12 + 8*rax]        */
/*   09: b9 00 00 00 00                mov  ecx, HOLE_MV_DST          */
/*   0e: 49 89 04 cc                   mov  [r12 + 8*rcx], rax        */
/*                                                                    */
/* Holes:                                                             */
/*   off=1  width=4 R_X86_64_32   HOLE_MV_SRC   → patch with op->b    */
/*   off=10 width=4 R_X86_64_32   HOLE_MV_DST   → patch with op->a    */
/* ------------------------------------------------------------------ */
static const uint8_t bytes_op_mov_int_int[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00,
  0x49, 0x8b, 0x04, 0xc4,
  0xb9, 0x00, 0x00, 0x00, 0x00,
  0x49, 0x89, 0x04, 0xcc,
};
static const Hole holes_op_mov_int_int[] = {
  { .byte_offset =  1, .kind = HK_OP_B, .width = 4 },
  { .byte_offset = 10, .kind = HK_OP_A, .width = 4 },
};

/* ------------------------------------------------------------------ */
/* op_add_int_int (OpKind = OP_ADD_INT_INT, 27 bytes)                 */
/*                                                                    */
/* Source: stencils.o offset 0x60..0x7a (jmp at 0x7b-0x7f stripped).   */
/*                                                                    */
/*   00: b8 00 00 00 00                mov  eax, HOLE_ADD_A           */
/*   05: b9 00 00 00 00                mov  ecx, HOLE_ADD_B           */
/*   0a: 49 8b 0c cc                   mov  rcx, [r12 + 8*rcx]        */
/*   0e: 49 03 0c c4                   add  rcx, [r12 + 8*rax]        */
/*   12: b8 00 00 00 00                mov  eax, HOLE_ADD_DST         */
/*   17: 49 89 0c c4                   mov  [r12 + 8*rax], rcx        */
/*                                                                    */
/* Holes:                                                             */
/*   off=1  width=4 R_X86_64_32   HOLE_ADD_A    → patch with op->b    */
/*   off=6  width=4 R_X86_64_32   HOLE_ADD_B    → patch with op->c    */
/*   off=19 width=4 R_X86_64_32   HOLE_ADD_DST  → patch with op->a    */
/* ------------------------------------------------------------------ */
static const uint8_t bytes_op_add_int_int[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00,
  0xb9, 0x00, 0x00, 0x00, 0x00,
  0x49, 0x8b, 0x0c, 0xcc,
  0x49, 0x03, 0x0c, 0xc4,
  0xb8, 0x00, 0x00, 0x00, 0x00,
  0x49, 0x89, 0x0c, 0xc4,
};
static const Hole holes_op_add_int_int[] = {
  { .byte_offset =  1, .kind = HK_OP_B, .width = 4 },
  { .byte_offset =  6, .kind = HK_OP_C, .width = 4 },
  { .byte_offset = 19, .kind = HK_OP_A, .width = 4 },
};

/* ------------------------------------------------------------------ */
/* op_sum_bigint (OpKind = OP_SUM_BIGINT, 19 bytes)                   */
/*                                                                    */
/* Source: stencils.o offset 0x80..0x92 (jmp at 0x93-0x97 stripped).   */
/*                                                                    */
/*   00: b8 00 00 00 00                mov  eax, HOLE_SUM_SRC         */
/*   05: 49 8b 04 c4                   mov  rax, [r12 + 8*rax]        */
/*   09: b9 00 00 00 00                mov  ecx, HOLE_SUM_SLOT        */
/*   0e: 49 01 44 cc 40                add  [r12 + 8*rcx + 0x40], rax */
/*                                                                    */
/* Note the +0x40 byte displacement — that's the offset of acc_i64    */
/* within JitState (regs_i64 is 8*8 = 64 bytes = 0x40). If JitState   */
/* layout changes, this byte will shift; the engine doesn't need to   */
/* know — it's part of the stencil.                                   */
/*                                                                    */
/* Holes:                                                             */
/*   off=1  width=4 R_X86_64_32   HOLE_SUM_SRC  → patch with op->b    */
/*   off=10 width=4 R_X86_64_32   HOLE_SUM_SLOT → patch with op->a    */
/* ------------------------------------------------------------------ */
static const uint8_t bytes_op_sum_bigint[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00,
  0x49, 0x8b, 0x04, 0xc4,
  0xb9, 0x00, 0x00, 0x00, 0x00,
  0x49, 0x01, 0x44, 0xcc, 0x40,
};
static const Hole holes_op_sum_bigint[] = {
  { .byte_offset =  1, .kind = HK_OP_B, .width = 4 },
  { .byte_offset = 10, .kind = HK_OP_A, .width = 4 },
};

/* ------------------------------------------------------------------ */
/* op_branch_lt_int_int (OpKind = OP_BRANCH_LT_INT_INT, 29 bytes)     */
/*                                                                    */
/* Source: stencils.o offset 0xa0..0xbc (no trailing jmp to strip —   */
/* the branch keeps both its jge and its jmp).                        */
/*                                                                    */
/*   00: b8 00 00 00 00                mov  eax, HOLE_BLT_A           */
/*   05: 49 8b 04 c4                   mov  rax, [r12 + 8*rax]        */
/*   09: b9 00 00 00 00                mov  ecx, HOLE_BLT_B           */
/*   0e: 49 3b 04 cc                   cmp  rax, [r12 + 8*rcx]        */
/*   12: 0f 8d 00 00 00 00             jge  <next>      ; fall-through*/
/*   18: e9 00 00 00 00                jmp  HOLE_BLT_TGT ; taken     */
/*                                                                    */
/* The C source is `if (A < B) musttail HOLE_BLT_TGT; else next;`.   */
/* clang inverts to: `if (A >= B) musttail next; (else fall through */
/* to:) musttail HOLE_BLT_TGT;`.                                      */
/*                                                                    */
/* The two trailing jumps are BOTH meaningful and BOTH require        */
/* displacement patching at JIT time:                                 */
/*   - The jge fall-through goes to the next bytecode pc's stencil    */
/*     (i.e., immediately after this stencil's bytes end).            */
/*   - The jmp taken goes to op->c's bytecode pc's stencil (a forward */
/*     branch fixup queued during emission, resolved when the target  */
/*     is emitted).                                                   */
/*                                                                    */
/* Holes:                                                             */
/*   off=1  width=4 R_X86_64_32    HOLE_BLT_A   → patch with op->a   */
/*   off=10 width=4 R_X86_64_32    HOLE_BLT_B   → patch with op->b   */
/*   off=20 width=4 R_X86_64_PLT32 jge          → patch with         */
/*                                  PC-relative disp to next stencil */
/*   off=25 width=4 R_X86_64_PLT32 HOLE_BLT_TGT → patch with         */
/*                                  PC-relative disp to op->c        */
/* ------------------------------------------------------------------ */
static const uint8_t bytes_op_branch_lt_int_int[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00,                  /* mov  eax, HOLE_BLT_A */
  0x49, 0x8b, 0x04, 0xc4,                        /* mov  rax, [r12+8*rax]*/
  0xb9, 0x00, 0x00, 0x00, 0x00,                  /* mov  ecx, HOLE_BLT_B */
  0x49, 0x3b, 0x04, 0xcc,                        /* cmp  rax, [r12+8*rcx]*/
  0x0f, 0x8d, 0x00, 0x00, 0x00, 0x00,            /* jge  <next>          */
  0xe9, 0x00, 0x00, 0x00, 0x00,                  /* jmp  HOLE_BLT_TGT    */
};
static const Hole holes_op_branch_lt_int_int[] = {
  { .byte_offset =  1, .kind = HK_OP_A,         .width = 4 },
  { .byte_offset = 10, .kind = HK_OP_B,         .width = 4 },
  { .byte_offset = 20, .kind = HK_BRANCH_FALL,  .width = 4 },
  { .byte_offset = 25, .kind = HK_BRANCH_TAKE,  .width = 4 },
};

/* ------------------------------------------------------------------ */
/* op_skip / op_exit  (3 bytes each: `pop r12; ret`)                   */
/*                                                                    */
/* Phase 1 row terminators. Both restore the caller's r12 (saved by   */
/* the preamble at offset 0 of the compiled blob) and return.         */
/*                                                                    */
/*   00: 41 5c                         pop  r12                       */
/*   02: c3                            ret                            */
/*                                                                    */
/* These bytes are HAND-WRITTEN, not extracted from stencils_src.c    */
/* (which still defines op_skip/op_exit as bare returns). The         */
/* preamble/terminator pair is what makes the JIT'd entry callable    */
/* via the standard C ABI — see JitEntry typedef in jit1.h.           */
/* ------------------------------------------------------------------ */
static const uint8_t bytes_op_skip[] = { 0x41, 0x5c, 0xc3 };
static const uint8_t bytes_op_exit[] = { 0x41, 0x5c, 0xc3 };
/* No Hole[] arrays for the no-hole stencils — the table entry uses
 * NULL + 0 (see STENCIL_NOHOLES macro below). */

/* ------------------------------------------------------------------ */
/* Top-level table: indexed by OpKind. Phase 1 has exactly one        */
/* stencil per opcode kind.                                           */
/* ------------------------------------------------------------------ */

#define STENCIL_(name) \
  { .bytes = bytes_##name, \
    .n_bytes = (uint16_t)sizeof(bytes_##name), \
    .holes = holes_##name, \
    .n_holes = (uint8_t)(sizeof(holes_##name) / sizeof(holes_##name[0])) }

#define STENCIL_NOHOLES(name) \
  { .bytes = bytes_##name, \
    .n_bytes = (uint16_t)sizeof(bytes_##name), \
    .holes = NULL, \
    .n_holes = 0 }

static const Stencil g_stencils[OP_KIND_MAX + 1] = {
  [OP_LOAD_CONST_INT]    = STENCIL_(op_load_const_int),
  [OP_LOAD_COL_INT]      = STENCIL_(op_load_col_int),
  [OP_MOV_INT_INT]       = STENCIL_(op_mov_int_int),
  [OP_ADD_INT_INT]       = STENCIL_(op_add_int_int),
  [OP_SUM_BIGINT]        = STENCIL_(op_sum_bigint),
  [OP_BRANCH_LT_INT_INT] = STENCIL_(op_branch_lt_int_int),
  [OP_SKIP]              = STENCIL_NOHOLES(op_skip),
  [OP_EXIT]              = STENCIL_NOHOLES(op_exit),
};

#ifdef __cplusplus
}
#endif

#endif /* NDB_JIT_STENCILS_X86_64_H */
