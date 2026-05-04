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
  OP_SKIP               = 7,
  OP_EXIT               = 8,
  OP_KIND_MAX           = OP_EXIT
} OpKind;

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
