/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.

 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

/*
 * RONDB-1056 Phase 1 — bytecode representation for the
 * copy-and-patch microbench.
 *
 * Pure C11. No NDB / kernel dependencies. Used identically by
 * the interpreter (microbench_interp.c) and the JIT engine
 * (jit1.c).
 *
 * The opcode set is the smallest one that lets us express a
 * representative aggregation hot path:
 *
 *   load_const_int  rN, imm
 *   load_col_int    rN, col
 *   mov_int_int     rDST, rSRC
 *   add_int_int     rDST, rA, rB
 *   sum_bigint      acc[S], rN          ; acc[S] += rN
 *   branch_lt_int   rA, rB, label       ; if rA < rB then pc=label
 *   skip                                ; jump to row_end
 *   exit                                ; row terminator
 *
 * Each Op has up to four operand slots (a, b, c, imm). Which slots
 * are meaningful depends on `kind`:
 *
 *   load_const_int       a=dst,        imm=value
 *   load_col_int         a=dst, b=col
 *   mov_int_int          a=dst, b=src
 *   add_int_int          a=dst, b=lhs, c=rhs
 *   sum_bigint           a=acc_slot, b=src
 *   branch_lt_int_int    a=lhs,  b=rhs, c=target_pc
 *   skip                 (no operands)
 *   exit                 (no operands)
 *
 * Phase 1 is forward-only: branch targets always point to a higher
 * pc. The program builder enforces this by construction.
 */

#ifndef NDB_JIT_BYTECODE1_H
#define NDB_JIT_BYTECODE1_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  OP_LOAD_CONST_INT     = 1,
  OP_LOAD_COL_INT       = 2,
  OP_MOV_INT_INT        = 3,
  OP_ADD_INT_INT        = 4,
  OP_SUM_BIGINT         = 5,
  OP_BRANCH_LT_INT_INT  = 6,
  /* Phase 3 branch-comparison siblings — same operand layout as
   * BRANCH_LT_INT_INT (a=lhs, b=rhs, c=target_pc). Inserted before
   * OP_SKIP / OP_EXIT to keep the existing kind values stable. */
  OP_BRANCH_LE_INT_INT  = 7,
  OP_BRANCH_EQ_INT_INT  = 8,
  OP_BRANCH_GT_INT_INT  = 9,
  OP_BRANCH_GE_INT_INT  = 10,
  OP_BRANCH_NE_INT_INT  = 11,
  OP_SKIP               = 12,
  OP_EXIT               = 13,
  /* Phase 4 hot-arithmetic siblings of OP_ADD_INT_INT — same
   * operand layout (a=dst, b=lhs, c=rhs). Bridge maps NDB's
   * kOpMinusBigint / kOpMulBigint to these; kOpPlusBigint maps to
   * the existing OP_ADD_INT_INT (Phase 1's name kept for stability).
   *
   * These follow OP_EXIT in numeric order — append-only so existing
   * stencils' g_stencils[kind] indices don't shift. OP_KIND_MAX
   * tracks the highest valid kind. */
  OP_MINUS_INT_INT      = 14,
  OP_MUL_INT_INT        = 15,
  /* Phase 4: NDB cold-call variant of LoadCol. The stencil emits a
   * regular function call to ndb_jit_h_load_col(state, col_id,
   * dst_reg) which calls into NDB's readAttributes via the
   * JitState.ctx pointer. Bridge maps kOpLoadCol to this opcode;
   * the existing OP_LOAD_COL_INT (pure stencil, reads a flat
   * row_cols_i64[] array) stays unchanged for microbench tests.
   * Operand layout: a=dst_reg, c=col_id (16-bit, fits in op->c
   * after Phase 4's ≤255 col_id restriction). */
  OP_LOAD_COL_NDB       = 16,
  OP_KIND_MAX           = OP_LOAD_COL_NDB
} OpKind;

/* Inline predicate — true for any opcode that takes a forward branch
 * via op->c. Phase 3 set is the six BRANCH_*_INT_INT siblings. */
static inline int bc_op_is_branch(uint8_t kind) {
  switch (kind) {
    case OP_BRANCH_LT_INT_INT:
    case OP_BRANCH_LE_INT_INT:
    case OP_BRANCH_EQ_INT_INT:
    case OP_BRANCH_GT_INT_INT:
    case OP_BRANCH_GE_INT_INT:
    case OP_BRANCH_NE_INT_INT:
      return 1;
    default:
      return 0;
  }
}

typedef struct {
  uint8_t  kind;       /* OpKind */
  uint8_t  a;          /* register or accumulator slot */
  uint8_t  b;          /* register, column index, or unused */
  uint8_t  c;          /* register or branch target pc */
  int64_t  imm;        /* immediate (load_const_int) — 0 otherwise */
} Op;

/* Phase 1 hard limits — generously sized for the 30-op program. */
#define BC_MAX_OPS   64
#define BC_MAX_REGS  8
#define BC_MAX_ACCS  4
#define BC_MAX_COLS  8

typedef struct {
  Op       ops[BC_MAX_OPS];
  uint16_t n_ops;
} Program;

/* A row is just a fixed-width column array; Phase 1 doesn't model
 * per-row schemas. The interpreter / JIT both index into
 * cols[0..BC_MAX_COLS) directly. */
typedef struct {
  int64_t cols[BC_MAX_COLS];
} Row;

/* Diagnostic — used only by error paths and tests. */
const char *bc_op_name(uint8_t kind);

#ifdef __cplusplus
}
#endif

#endif /* NDB_JIT_BYTECODE1_H */
