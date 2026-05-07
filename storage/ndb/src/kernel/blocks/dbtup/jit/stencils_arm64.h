/* GENERATED FILE. Do not edit.
 * Source: stencils_src.c
 * Toolchain: clang version 20.1.8 (pinned)
 * Extractor: extract_stencils.c (RONDB-1056 Phase 2)
 *
 * Re-generate via:
 *   cmake --build . --target regen-stencils -DRONDB_REGEN_STENCILS=ON
 */

#ifndef NDB_JIT_STENCILS_ARM64_H
#define NDB_JIT_STENCILS_ARM64_H

#if !defined(__aarch64__)
#error "NDB_JIT_STENCILS_ARM64_H is only valid on __aarch64__ build targets."
#endif

#include <stddef.h>
#include <stdint.h>

#include "bytecode1.h"
#include "hole_kinds.h"

/* op_load_const_int — 24 bytes, 5 holes */
static const uint8_t bytes_op_load_const_int[] = {
  0x68, 0x2a, 0x93, 0xd2, 0x08, 0x24, 0xb1, 0xf2, 0x68, 0xba, 0xce, 0xf2,
  0x28, 0x47, 0xe4, 0xf2, 0x89, 0x84, 0x9f, 0x52, 0x88, 0x7a, 0x29, 0xf8,
  
};
static const Hole holes_op_load_const_int[] = {
  { .byte_offset = 0, .kind = HK_OP_IMM, .width = 4 },
  { .byte_offset = 4, .kind = HK_OP_IMM, .width = 4 },
  { .byte_offset = 8, .kind = HK_OP_IMM, .width = 4 },
  { .byte_offset = 12, .kind = HK_OP_IMM, .width = 4 },
  { .byte_offset = 16, .kind = HK_OP_A, .width = 2 },
};

/* op_load_col_int — 20 bytes, 2 holes */
static const uint8_t bytes_op_load_col_int[] = {
  0x88, 0x32, 0x40, 0xf9, 0xc9, 0xc2, 0x94, 0x52, 0x08, 0x79, 0x69, 0xf8,
  0xa9, 0x8d, 0x81, 0x52, 0x88, 0x7a, 0x29, 0xf8, 
};
static const Hole holes_op_load_col_int[] = {
  { .byte_offset = 4, .kind = HK_OP_B, .width = 2 },
  { .byte_offset = 12, .kind = HK_OP_A, .width = 2 },
};

/* op_mov_int_int — 16 bytes, 2 holes */
static const uint8_t bytes_op_mov_int_int[] = {
  0xc8, 0x75, 0x97, 0x52, 0x88, 0x7a, 0x68, 0xf8, 0x89, 0x1d, 0x9a, 0x52,
  0x88, 0x7a, 0x29, 0xf8, 
};
static const Hole holes_op_mov_int_int[] = {
  { .byte_offset = 0, .kind = HK_OP_B, .width = 2 },
  { .byte_offset = 8, .kind = HK_OP_A, .width = 2 },
};

/* op_add_int_int — 28 bytes, 3 holes */
static const uint8_t bytes_op_add_int_int[] = {
  0x28, 0x7b, 0x8f, 0x52, 0x88, 0x7a, 0x68, 0xf8, 0xa9, 0xba, 0x92, 0x52,
  0x89, 0x7a, 0x69, 0xf8, 0x6a, 0x5d, 0x82, 0x52, 0x28, 0x01, 0x08, 0x8b,
  0x88, 0x7a, 0x2a, 0xf8, 
};
static const Hole holes_op_add_int_int[] = {
  { .byte_offset = 0, .kind = HK_OP_B, .width = 2 },
  { .byte_offset = 8, .kind = HK_OP_C, .width = 2 },
  { .byte_offset = 16, .kind = HK_OP_A, .width = 2 },
};

/* op_sum_bigint — 28 bytes, 2 holes */
static const uint8_t bytes_op_sum_bigint[] = {
  0xe8, 0x63, 0x9e, 0x52, 0x88, 0x7a, 0x68, 0xf8, 0xc9, 0xb2, 0x81, 0x52,
  0x89, 0x0e, 0x09, 0x8b, 0x2a, 0x21, 0x40, 0xf9, 0x48, 0x01, 0x08, 0x8b,
  0x28, 0x21, 0x00, 0xf9, 
};
static const Hole holes_op_sum_bigint[] = {
  { .byte_offset = 0, .kind = HK_OP_B, .width = 2 },
  { .byte_offset = 8, .kind = HK_OP_A, .width = 2 },
};

/* op_branch_lt_int_int — 32 bytes, 4 holes */
static const uint8_t bytes_op_branch_lt_int_int[] = {
  0xe8, 0x86, 0x9e, 0x52, 0x88, 0x7a, 0x68, 0xf8, 0x49, 0xaf, 0x8d, 0x52,
  0x89, 0x7a, 0x69, 0xf8, 0x1f, 0x01, 0x09, 0xeb, 0x4a, 0x00, 0x00, 0x54,
  0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x14, 
};
static const Hole holes_op_branch_lt_int_int[] = {
  { .byte_offset = 0, .kind = HK_OP_A, .width = 2 },
  { .byte_offset = 8, .kind = HK_OP_B, .width = 2 },
  { .byte_offset = 24, .kind = HK_BRANCH_TAKE, .width = 4 },
  { .byte_offset = 28, .kind = HK_BRANCH_FALL, .width = 4 },
};

/* op_branch_le_int_int — 32 bytes, 4 holes */
static const uint8_t bytes_op_branch_le_int_int[] = {
  0xa8, 0xb8, 0x83, 0x52, 0x88, 0x7a, 0x68, 0xf8, 0xc9, 0x90, 0x9c, 0x52,
  0x89, 0x7a, 0x69, 0xf8, 0x1f, 0x01, 0x09, 0xeb, 0x4d, 0x00, 0x00, 0x54,
  0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x14, 
};
static const Hole holes_op_branch_le_int_int[] = {
  { .byte_offset = 0, .kind = HK_OP_A, .width = 2 },
  { .byte_offset = 8, .kind = HK_OP_B, .width = 2 },
  { .byte_offset = 24, .kind = HK_BRANCH_FALL, .width = 4 },
  { .byte_offset = 28, .kind = HK_BRANCH_TAKE, .width = 4 },
};

/* op_branch_eq_int_int — 32 bytes, 4 holes */
static const uint8_t bytes_op_branch_eq_int_int[] = {
  0xc8, 0x95, 0x8e, 0x52, 0x88, 0x7a, 0x68, 0xf8, 0x69, 0xd0, 0x91, 0x52,
  0x89, 0x7a, 0x69, 0xf8, 0x1f, 0x01, 0x09, 0xeb, 0x41, 0x00, 0x00, 0x54,
  0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x14, 
};
static const Hole holes_op_branch_eq_int_int[] = {
  { .byte_offset = 0, .kind = HK_OP_A, .width = 2 },
  { .byte_offset = 8, .kind = HK_OP_B, .width = 2 },
  { .byte_offset = 24, .kind = HK_BRANCH_TAKE, .width = 4 },
  { .byte_offset = 28, .kind = HK_BRANCH_FALL, .width = 4 },
};

/* op_branch_gt_int_int — 32 bytes, 4 holes */
static const uint8_t bytes_op_branch_gt_int_int[] = {
  0x48, 0x52, 0x83, 0x52, 0x88, 0x7a, 0x68, 0xf8, 0xa9, 0x7b, 0x95, 0x52,
  0x89, 0x7a, 0x69, 0xf8, 0x1f, 0x01, 0x09, 0xeb, 0x4d, 0x00, 0x00, 0x54,
  0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x14, 
};
static const Hole holes_op_branch_gt_int_int[] = {
  { .byte_offset = 0, .kind = HK_OP_A, .width = 2 },
  { .byte_offset = 8, .kind = HK_OP_B, .width = 2 },
  { .byte_offset = 24, .kind = HK_BRANCH_TAKE, .width = 4 },
  { .byte_offset = 28, .kind = HK_BRANCH_FALL, .width = 4 },
};

/* op_branch_ge_int_int — 32 bytes, 4 holes */
static const uint8_t bytes_op_branch_ge_int_int[] = {
  0x28, 0xdd, 0x82, 0x52, 0x88, 0x7a, 0x68, 0xf8, 0xc9, 0x63, 0x95, 0x52,
  0x89, 0x7a, 0x69, 0xf8, 0x1f, 0x01, 0x09, 0xeb, 0x4a, 0x00, 0x00, 0x54,
  0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x14, 
};
static const Hole holes_op_branch_ge_int_int[] = {
  { .byte_offset = 0, .kind = HK_OP_A, .width = 2 },
  { .byte_offset = 8, .kind = HK_OP_B, .width = 2 },
  { .byte_offset = 24, .kind = HK_BRANCH_FALL, .width = 4 },
  { .byte_offset = 28, .kind = HK_BRANCH_TAKE, .width = 4 },
};

/* op_branch_ne_int_int — 32 bytes, 4 holes */
static const uint8_t bytes_op_branch_ne_int_int[] = {
  0x28, 0xd2, 0x91, 0x52, 0x88, 0x7a, 0x68, 0xf8, 0x09, 0x09, 0x94, 0x52,
  0x89, 0x7a, 0x69, 0xf8, 0x1f, 0x01, 0x09, 0xeb, 0x41, 0x00, 0x00, 0x54,
  0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x14, 
};
static const Hole holes_op_branch_ne_int_int[] = {
  { .byte_offset = 0, .kind = HK_OP_A, .width = 2 },
  { .byte_offset = 8, .kind = HK_OP_B, .width = 2 },
  { .byte_offset = 24, .kind = HK_BRANCH_FALL, .width = 4 },
  { .byte_offset = 28, .kind = HK_BRANCH_TAKE, .width = 4 },
};

/* op_skip — 8 bytes, 0 holes */
static const uint8_t bytes_op_skip[] = {
  0xf4, 0x7b, 0xc1, 0xa8, 0xc0, 0x03, 0x5f, 0xd6, 
};

/* op_exit — 8 bytes, 0 holes */
static const uint8_t bytes_op_exit[] = {
  0xf4, 0x7b, 0xc1, 0xa8, 0xc0, 0x03, 0x5f, 0xd6, 
};

/* op_minus_int_int — 28 bytes, 3 holes */
static const uint8_t bytes_op_minus_int_int[] = {
  0xa8, 0x8c, 0x85, 0x52, 0x88, 0x7a, 0x68, 0xf8, 0x69, 0x53, 0x82, 0x52,
  0x89, 0x7a, 0x69, 0xf8, 0xea, 0x1b, 0x8e, 0x52, 0x08, 0x01, 0x09, 0xcb,
  0x88, 0x7a, 0x2a, 0xf8, 
};
static const Hole holes_op_minus_int_int[] = {
  { .byte_offset = 0, .kind = HK_OP_B, .width = 2 },
  { .byte_offset = 8, .kind = HK_OP_C, .width = 2 },
  { .byte_offset = 16, .kind = HK_OP_A, .width = 2 },
};

/* op_mul_int_int — 28 bytes, 3 holes */
static const uint8_t bytes_op_mul_int_int[] = {
  0xa8, 0x97, 0x98, 0x52, 0x88, 0x7a, 0x68, 0xf8, 0x89, 0x37, 0x8e, 0x52,
  0x89, 0x7a, 0x69, 0xf8, 0x28, 0x7d, 0x08, 0x9b, 0xa9, 0x58, 0x9b, 0x52,
  0x88, 0x7a, 0x29, 0xf8, 
};
static const Hole holes_op_mul_int_int[] = {
  { .byte_offset = 0, .kind = HK_OP_B, .width = 2 },
  { .byte_offset = 8, .kind = HK_OP_C, .width = 2 },
  { .byte_offset = 20, .kind = HK_OP_A, .width = 2 },
};

/* op_load_col_ndb — 28 bytes, 3 holes */
static const uint8_t bytes_op_load_col_ndb[] = {
  0xfd, 0x7b, 0xbf, 0xa9, 0xe1, 0x1e, 0x81, 0x52, 0x62, 0xea, 0x83, 0x52,
  0xe0, 0x03, 0x14, 0xaa, 0xfd, 0x03, 0x00, 0x91, 0x00, 0x00, 0x00, 0x94,
  0xfd, 0x7b, 0xc1, 0xa8, 
};
static const Hole holes_op_load_col_ndb[] = {
  { .byte_offset = 4, .kind = HK_OP_C, .width = 2 },
  { .byte_offset = 8, .kind = HK_OP_A, .width = 2 },
  { .byte_offset = 20, .kind = HK_COLDCALL, .width = 4, .helper_name = "ndb_jit_h_load_col" },
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
  [OP_LOAD_COL_NDB] = STENCIL_(op_load_col_ndb),
};

#endif /* NDB_JIT_STENCILS_ARM64_H */
