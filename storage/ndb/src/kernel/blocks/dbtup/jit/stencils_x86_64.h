/* GENERATED FILE. Do not edit.
 * Source: stencils_src.c
 * Toolchain: clang version 20.1.8 (pinned)
 * Extractor: extract_stencils.c (RONDB-1056 Phase 2)
 *
 * Re-generate via:
 *   cmake --build . --target regen-stencils -DRONDB_REGEN_STENCILS=ON
 */

#ifndef NDB_JIT_STENCILS_X86_64_H
#define NDB_JIT_STENCILS_X86_64_H

#if !defined(__x86_64__)
#error "NDB_JIT_STENCILS_X86_64_H is only valid on __x86_64__ build targets."
#endif

#include <stddef.h>
#include <stdint.h>

#include "bytecode1.h"
#include "hole_kinds.h"

/* op_load_const_int — 13 bytes, 2 holes */
static const uint8_t bytes_op_load_const_int[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00, 0x49, 0xc7, 0x04, 0xc4, 0x00, 0x00, 0x00,
  0x00, 
};
static const Hole holes_op_load_const_int[] = {
  { .byte_offset = 1, .kind = HK_OP_A, .width = 4 },
  { .byte_offset = 9, .kind = HK_OP_IMM, .width = 4 },
};

/* op_load_col_int — 23 bytes, 2 holes */
static const uint8_t bytes_op_load_col_int[] = {
  0x49, 0x8b, 0x44, 0x24, 0x60, 0xb9, 0x00, 0x00, 0x00, 0x00, 0x48, 0x8b,
  0x04, 0xc8, 0xb9, 0x00, 0x00, 0x00, 0x00, 0x49, 0x89, 0x04, 0xcc, 
};
static const Hole holes_op_load_col_int[] = {
  { .byte_offset = 6, .kind = HK_OP_B, .width = 4 },
  { .byte_offset = 15, .kind = HK_OP_A, .width = 4 },
};

/* op_mov_int_int — 18 bytes, 2 holes */
static const uint8_t bytes_op_mov_int_int[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00, 0x49, 0x8b, 0x04, 0xc4, 0xb9, 0x00, 0x00,
  0x00, 0x00, 0x49, 0x89, 0x04, 0xcc, 
};
static const Hole holes_op_mov_int_int[] = {
  { .byte_offset = 1, .kind = HK_OP_B, .width = 4 },
  { .byte_offset = 10, .kind = HK_OP_A, .width = 4 },
};

/* op_add_int_int — 27 bytes, 3 holes */
static const uint8_t bytes_op_add_int_int[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00, 0xb9, 0x00, 0x00, 0x00, 0x00, 0x49, 0x8b,
  0x0c, 0xcc, 0x49, 0x03, 0x0c, 0xc4, 0xb8, 0x00, 0x00, 0x00, 0x00, 0x49,
  0x89, 0x0c, 0xc4, 
};
static const Hole holes_op_add_int_int[] = {
  { .byte_offset = 1, .kind = HK_OP_B, .width = 4 },
  { .byte_offset = 6, .kind = HK_OP_C, .width = 4 },
  { .byte_offset = 19, .kind = HK_OP_A, .width = 4 },
};

/* op_sum_bigint — 19 bytes, 2 holes */
static const uint8_t bytes_op_sum_bigint[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00, 0x49, 0x8b, 0x04, 0xc4, 0xb9, 0x00, 0x00,
  0x00, 0x00, 0x49, 0x01, 0x44, 0xcc, 0x40, 
};
static const Hole holes_op_sum_bigint[] = {
  { .byte_offset = 1, .kind = HK_OP_B, .width = 4 },
  { .byte_offset = 10, .kind = HK_OP_A, .width = 4 },
};

/* op_branch_lt_int_int — 29 bytes, 4 holes */
static const uint8_t bytes_op_branch_lt_int_int[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00, 0x49, 0x8b, 0x04, 0xc4, 0xb9, 0x00, 0x00,
  0x00, 0x00, 0x49, 0x3b, 0x04, 0xcc, 0x0f, 0x8d, 0x00, 0x00, 0x00, 0x00,
  0xe9, 0x00, 0x00, 0x00, 0x00, 
};
static const Hole holes_op_branch_lt_int_int[] = {
  { .byte_offset = 1, .kind = HK_OP_A, .width = 4 },
  { .byte_offset = 10, .kind = HK_OP_B, .width = 4 },
  { .byte_offset = 20, .kind = HK_BRANCH_FALL, .width = 4 },
  { .byte_offset = 25, .kind = HK_BRANCH_TAKE, .width = 4 },
};

/* op_branch_le_int_int — 29 bytes, 4 holes */
static const uint8_t bytes_op_branch_le_int_int[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00, 0x49, 0x8b, 0x04, 0xc4, 0xb9, 0x00, 0x00,
  0x00, 0x00, 0x49, 0x3b, 0x04, 0xcc, 0x0f, 0x8f, 0x00, 0x00, 0x00, 0x00,
  0xe9, 0x00, 0x00, 0x00, 0x00, 
};
static const Hole holes_op_branch_le_int_int[] = {
  { .byte_offset = 1, .kind = HK_OP_A, .width = 4 },
  { .byte_offset = 10, .kind = HK_OP_B, .width = 4 },
  { .byte_offset = 20, .kind = HK_BRANCH_FALL, .width = 4 },
  { .byte_offset = 25, .kind = HK_BRANCH_TAKE, .width = 4 },
};

/* op_branch_eq_int_int — 29 bytes, 4 holes */
static const uint8_t bytes_op_branch_eq_int_int[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00, 0x49, 0x8b, 0x04, 0xc4, 0xb9, 0x00, 0x00,
  0x00, 0x00, 0x49, 0x3b, 0x04, 0xcc, 0x0f, 0x85, 0x00, 0x00, 0x00, 0x00,
  0xe9, 0x00, 0x00, 0x00, 0x00, 
};
static const Hole holes_op_branch_eq_int_int[] = {
  { .byte_offset = 1, .kind = HK_OP_A, .width = 4 },
  { .byte_offset = 10, .kind = HK_OP_B, .width = 4 },
  { .byte_offset = 20, .kind = HK_BRANCH_FALL, .width = 4 },
  { .byte_offset = 25, .kind = HK_BRANCH_TAKE, .width = 4 },
};

/* op_branch_gt_int_int — 29 bytes, 4 holes */
static const uint8_t bytes_op_branch_gt_int_int[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00, 0x49, 0x8b, 0x04, 0xc4, 0xb9, 0x00, 0x00,
  0x00, 0x00, 0x49, 0x3b, 0x04, 0xcc, 0x0f, 0x8e, 0x00, 0x00, 0x00, 0x00,
  0xe9, 0x00, 0x00, 0x00, 0x00, 
};
static const Hole holes_op_branch_gt_int_int[] = {
  { .byte_offset = 1, .kind = HK_OP_A, .width = 4 },
  { .byte_offset = 10, .kind = HK_OP_B, .width = 4 },
  { .byte_offset = 20, .kind = HK_BRANCH_FALL, .width = 4 },
  { .byte_offset = 25, .kind = HK_BRANCH_TAKE, .width = 4 },
};

/* op_branch_ge_int_int — 29 bytes, 4 holes */
static const uint8_t bytes_op_branch_ge_int_int[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00, 0x49, 0x8b, 0x04, 0xc4, 0xb9, 0x00, 0x00,
  0x00, 0x00, 0x49, 0x3b, 0x04, 0xcc, 0x0f, 0x8c, 0x00, 0x00, 0x00, 0x00,
  0xe9, 0x00, 0x00, 0x00, 0x00, 
};
static const Hole holes_op_branch_ge_int_int[] = {
  { .byte_offset = 1, .kind = HK_OP_A, .width = 4 },
  { .byte_offset = 10, .kind = HK_OP_B, .width = 4 },
  { .byte_offset = 20, .kind = HK_BRANCH_FALL, .width = 4 },
  { .byte_offset = 25, .kind = HK_BRANCH_TAKE, .width = 4 },
};

/* op_branch_ne_int_int — 29 bytes, 4 holes */
static const uint8_t bytes_op_branch_ne_int_int[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00, 0x49, 0x8b, 0x04, 0xc4, 0xb9, 0x00, 0x00,
  0x00, 0x00, 0x49, 0x3b, 0x04, 0xcc, 0x0f, 0x84, 0x00, 0x00, 0x00, 0x00,
  0xe9, 0x00, 0x00, 0x00, 0x00, 
};
static const Hole holes_op_branch_ne_int_int[] = {
  { .byte_offset = 1, .kind = HK_OP_A, .width = 4 },
  { .byte_offset = 10, .kind = HK_OP_B, .width = 4 },
  { .byte_offset = 20, .kind = HK_BRANCH_FALL, .width = 4 },
  { .byte_offset = 25, .kind = HK_BRANCH_TAKE, .width = 4 },
};

/* op_skip — 3 bytes, 0 holes */
static const uint8_t bytes_op_skip[] = {
  0x41, 0x5c, 0xc3, 
};

/* op_exit — 3 bytes, 0 holes */
static const uint8_t bytes_op_exit[] = {
  0x41, 0x5c, 0xc3, 
};

/* op_minus_int_int — 27 bytes, 3 holes */
static const uint8_t bytes_op_minus_int_int[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00, 0x49, 0x8b, 0x04, 0xc4, 0xb9, 0x00, 0x00,
  0x00, 0x00, 0x49, 0x2b, 0x04, 0xcc, 0xb9, 0x00, 0x00, 0x00, 0x00, 0x49,
  0x89, 0x04, 0xcc, 
};
static const Hole holes_op_minus_int_int[] = {
  { .byte_offset = 1, .kind = HK_OP_B, .width = 4 },
  { .byte_offset = 10, .kind = HK_OP_C, .width = 4 },
  { .byte_offset = 19, .kind = HK_OP_A, .width = 4 },
};

/* op_mul_int_int — 28 bytes, 3 holes */
static const uint8_t bytes_op_mul_int_int[] = {
  0xb8, 0x00, 0x00, 0x00, 0x00, 0xb9, 0x00, 0x00, 0x00, 0x00, 0x49, 0x8b,
  0x0c, 0xcc, 0x49, 0x0f, 0xaf, 0x0c, 0xc4, 0xb8, 0x00, 0x00, 0x00, 0x00,
  0x49, 0x89, 0x0c, 0xc4, 
};
static const Hole holes_op_mul_int_int[] = {
  { .byte_offset = 1, .kind = HK_OP_B, .width = 4 },
  { .byte_offset = 6, .kind = HK_OP_C, .width = 4 },
  { .byte_offset = 20, .kind = HK_OP_A, .width = 4 },
};

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
  [OP_LOAD_CONST_INT] = STENCIL_(op_load_const_int),
  [OP_LOAD_COL_INT] = STENCIL_(op_load_col_int),
  [OP_MOV_INT_INT] = STENCIL_(op_mov_int_int),
  [OP_ADD_INT_INT] = STENCIL_(op_add_int_int),
  [OP_SUM_BIGINT] = STENCIL_(op_sum_bigint),
  [OP_BRANCH_LT_INT_INT] = STENCIL_(op_branch_lt_int_int),
  [OP_BRANCH_LE_INT_INT] = STENCIL_(op_branch_le_int_int),
  [OP_BRANCH_EQ_INT_INT] = STENCIL_(op_branch_eq_int_int),
  [OP_BRANCH_GT_INT_INT] = STENCIL_(op_branch_gt_int_int),
  [OP_BRANCH_GE_INT_INT] = STENCIL_(op_branch_ge_int_int),
  [OP_BRANCH_NE_INT_INT] = STENCIL_(op_branch_ne_int_int),
  [OP_SKIP] = STENCIL_NOHOLES(op_skip),
  [OP_EXIT] = STENCIL_NOHOLES(op_exit),
  [OP_MINUS_INT_INT] = STENCIL_(op_minus_int_int),
  [OP_MUL_INT_INT] = STENCIL_(op_mul_int_int),
};

#endif /* NDB_JIT_STENCILS_X86_64_H */
