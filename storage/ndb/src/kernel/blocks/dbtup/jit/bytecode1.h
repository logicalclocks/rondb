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
  /* Operand layout: a=accumulator slot, b=source register,
   * c=aggregation-result index. */
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
   * Operand layout: a=dst_reg, c=col_id (16-bit in op->c; the bridge
   * admits col_id up to BR_MAX_LOCAL_ATTR_ID=4095, matching NDB's
   * MAX_ATTRIBUTES_IN_TABLE — see RONDB-1056 Test 27). */
  OP_LOAD_COL_NDB       = 16,
  /* Phase 4.7 narrow LoadConst variants. The bridge picks the
   * smallest-fitting variant per constant value. Operand layout:
   * a=dst_reg, imm=constant value (the patcher writes only the
   * 16-bit MOVZ slice). */
  OP_LOAD_CONST_UINT16  = 17,   /* 0..65535,        MOVZ + STR        ( 8 B) */
  OP_LOAD_CONST_INT16   = 18,   /* -32768..-1,      MOVZ + SXTH + STR (12 B) */
  OP_LOAD_CONST_UINT32  = 19,   /* 0..2^32-1,       MOVZ+MOVK + STR   (12 B) */
  OP_LOAD_CONST_INT32   = 20,   /* INT32_MIN..-2^15-1, +SXTW           (16 B) */
  /* Phase 5.0 cold-call branches from embedded normal-interpreter
   * blocks. Operand layout: a=0 (unused); b=attr_id (≤255);
   * c=branch target pc (after bridge fixup). Two variants share
   * one helper; eq/ne discrimination via the want_null flag. */
  OP_BRANCH_ATTR_EQ_NULL = 21,
  OP_BRANCH_ATTR_NE_NULL = 22,
  /* Phase 5.1a: linked-column variants. READ_LINKED_TO_MEM is a
   * cold-call (no branch) that populates ctx->block_tup->cheapMemory[0]
   * from the linked-attr buffer. BRANCH_LINKED_EQ/NE_NULL is a 3-hole
   * cold-call branch that null-checks cheapMemory[0]'s AttributeHeader.
   * Operand layout (LOAD_LINKED): b=position (≤255). Branch variants:
   * a/b unused, c=branch target pc; eq/ne via want_null flag. */
  OP_LOAD_LINKED_TO_MEM   = 23,
  OP_BRANCH_LINKED_EQ_NULL = 24,
  OP_BRANCH_LINKED_NE_NULL = 25,
  /* Overflow-checked signed arithmetic variants. Same operand layout as
   * their unchecked siblings for a/b/c; d carries the overflow-exit pc. */
  OP_ADD_INT_INT_CHECKED   = 26,
  OP_MINUS_INT_INT_CHECKED = 27,
  OP_MUL_INT_INT_CHECKED   = 28,
  OP_SUM_BIGINT_CHECKED    = 29,
  /* Terminator reached only from checked arithmetic overflow branches. */
  OP_OVERFLOW_EXIT         = 30,
  /* Unconditional forward jump. Used for embedded CASE accept paths whose
   * WRITE_INTERPRETER_OUTPUT skip_offset selects a later aggregate op. */
  OP_JUMP                  = 31,
  /* Scan-filter reject terminator. Sets JitState::row_filter_rejected before
   * returning so scan-filter callers can distinguish reject from accept. */
  OP_FILTER_REJECT_EXIT    = 32,
  /* Phase 7: cold-call comparison branch (WHERE col <op> literal). The
   * helper reads the whole instruction from the program buffer, so the
   * stencil carries just one operand hole (the instruction's word offset,
   * ≤ BR_EMB_MAX_LEN so it fits the 16-bit narrow hole) plus the branch
   * target — the same shape as OP_BRANCH_ATTR_EQ_NULL. Operand layout:
   * b = inst word offset into ctx->prog_buf; c = branch target pc (after
   * bridge fixup); a/imm unused. */
  OP_BRANCH_ATTR_OP_ARG    = 33,
  /* Phase 8 GROUP BY lift: COUNT accumulator. acc_i64[a] += 1; marks
   * value_updated[c] AND value_unsigned[c] (the interpreter's Count
   * kernel produces an unsigned BIGINT result — the writeback glue
   * mirrors is_unsigned from the mask). The interpreter's per-row
   * null-register skip is not lowered: the admitted LoadCol contract
   * is non-null columns, so a row that reaches this op always counts.
   * b carries the bytecode's register operand for diagnostics only. */
  OP_COUNT_BIGINT          = 34,
  /* Phase 5B: MIN/MAX accumulators. First-row initialization comes
   * from the value_initialized input mask (set by the dispatch glue
   * from AggResItem::type != UNDEFINED && !is_null, per row — the
   * grouped path makes this per-group state): uninitialized -> store
   * the row's value, else compare-and-conditionally-store (signed i64;
   * the unsigned kernel branches are unreachable for admitted
   * programs). Marks value_updated AND value_initialized on every
   * reaching row. a = acc slot, b = src register, c = result index
   * (== a, the AggResItem index). Unchecked — no arithmetic. */
  OP_MIN_BIGINT            = 35,
  OP_MAX_BIGINT            = 36,
  /* Phase 5C-2: the DOUBLE family. f64 values live BIT-CAST in the
   * same regs_i64 / acc_i64 arrays (bits are bits — copy-in/copy-out
   * and OP_MOV_INT_INT are type-agnostic); the bridge's register-type
   * tracker (5C-1) keeps i64 and f64 consumers apart statically.
   *
   * OP_LOAD_COL_NDB_F64: cold-call load for declared FLOAT/DOUBLE
   * columns (helper ndb_jit_h_load_col_f64; FLOAT promotes to double,
   * NULL / unexpected type takes the per-row fallback). Same operand
   * layout as OP_LOAD_COL_NDB (a=dst_reg, c=col_id).
   *
   * Arithmetic (a=dst, b=lhs, c=rhs, d=overflow-exit pc): fadd/fsub/
   * fmul/fdiv with the interpreter kernels' non-finite check routed
   * to OP_OVERFLOW_EXIT (⇒ ZAGG_MATH_OVERFLOW). OP_DIV_F64 handles
   * divisor == 0 by setting JitState::row_fallback inline (the kernel
   * NULLs the result register — SQL semantics the JIT reproduces by
   * re-running the row on the interpreter) and continuing.
   *
   * Accumulators (a=acc slot, b=src reg, c=result index, SUM also
   * d=overflow-exit pc): first-row-initialize via value_initialized
   * (SumDouble/MinDouble/MaxDouble all initialize on first value —
   * needed even for SUM: 0.0 + -0.0 flips the sign of a single-row
   * SUM(-0.0)); all three mark value_updated AND value_double so the
   * writeback glue produces a DOUBLE AggResItem. */
  OP_LOAD_COL_NDB_F64      = 37,
  OP_ADD_F64               = 38,
  OP_MINUS_F64             = 39,
  OP_MUL_F64               = 40,
  OP_DIV_F64               = 41,
  OP_SUM_F64               = 42,
  OP_MIN_F64               = 43,
  OP_MAX_F64               = 44,
  /* Phase 5C-3: unsigned BIGINT. u64 values live bit-cast in
   * regs_i64 / acc_i64 like everything else; the bridge admits these
   * only for statically-proven-unsigned sources (declared BIGUNSIGNED
   * loads / BIGUNSIGNED constants), and enforces uniform signedness
   * per accumulator slot, so the interpreter kernels' mixed
   * signed/unsigned branches are unreachable for admitted programs.
   *
   * OP_LOAD_COL_NDB_U64: cold-call load for declared BIGUNSIGNED
   * columns (helper ndb_jit_h_load_col_u64; NULL / unexpected type →
   * per-row fallback). Same operand layout as OP_LOAD_COL_NDB.
   *
   * OP_SUM_U64_CHECKED: u64 add with carry check → overflow-exit pc
   * in d (the kernel returns ZAGG_MATH_OVERFLOW on u64 overflow). No
   * first-row init mask needed — u64 0 + x = x, like the signed SUM.
   *
   * OP_MIN_U64 / OP_MAX_U64: the 5B MIN/MAX shape with UNSIGNED
   * compares (a signed compare would order values >= 2^63 as
   * negative) and first-row init via value_initialized.
   *
   * All three accumulators mark value_unsigned alongside
   * value_updated — the writeback glue mirrors it into
   * AggResItem::is_unsigned (the kernels produce is_unsigned = true
   * for uniform-unsigned inputs). */
  OP_LOAD_COL_NDB_U64      = 45,
  OP_SUM_U64_CHECKED       = 46,
  OP_MIN_U64               = 47,
  OP_MAX_U64               = 48,
  /* Phase 5D-1: null-branching load — a cold-call BRANCH in
   * op_branch_attr_eq_null's shape. The helper
   * (ndb_jit_h_load_col_nb) loads like OP_LOAD_COL_NDB but RETURNS
   * "value was NULL"; the taken edge skips the loaded register's
   * whole consumer chain (bridge-computed by a taint walk),
   * reproducing the interpreter kernels' null-skip exactly: no
   * accumulator update, no writeback-mask marks (per-group SQL NULL
   * preserved), and the chain's arithmetic never runs on garbage (no
   * spurious overflow). NULL rows thus stay on the JIT instead of
   * taking the per-row interpreter fallback. Operand layout differs
   * from OP_LOAD_COL_NDB: a = dst reg, b = col_id (moved from c —
   * the engine patches branch displacement from op->c),
   * c = null-branch target pc. Read errors / declared-type
   * mismatches keep the row_fallback defense inside the helper. */
  OP_LOAD_COL_NDB_NB       = 49,
  /* Phase 5D-2: f64/u64 siblings of the null-branching load — same
   * operand layout (a = dst, b = col_id, c = null-branch target) and
   * semantics, decoding like OP_LOAD_COL_NDB_F64 / _U64 respectively
   * (FLOAT promotes to double; strict declared-type contracts). */
  OP_LOAD_COL_NDB_F64_NB   = 50,
  OP_LOAD_COL_NDB_U64_NB   = 51,
  /* Phase 5C-4 (census-driven): unsigned checked arithmetic. Same
   * operand layout as the signed checked family (a=dst, b=lhs,
   * c=rhs, d=overflow-exit pc), but u64 add/sub/mul with unsigned
   * overflow/borrow checks. Admitted by the bridge only for
   * operands proven u64 or NON-NEGATIVE BIGINT constants — the
   * kernel's mixed unsigned/nonneg-signed paths reduce exactly to
   * u64 arithmetic (RegPlus/Minus/MulBigint audits in
   * phase_5c_plan.md §5C-4); genuinely mixed unsigned/signed-
   * variable shapes keep the whole-program fallback. */
  OP_ADD_U64_CHECKED       = 52,
  OP_MINUS_U64_CHECKED     = 53,
  OP_MUL_U64_CHECKED       = 54,
  /* Phase 5G (census-driven): DECIMAL column load. Cold call that
   * mirrors the interpreter's bin2decimal + decimal2{longlong,
   * ulonglong,double} conversion: scale == 0 lands in the BIGINT
   * track (unsigned for DECIMALUNSIGNED), scale > 0 in the DOUBLE
   * track — the same AlignedType routing the interpreter uses, known
   * STATICALLY by the bridge from the instruction's decimal_info
   * word. Operand layout: a = dst reg, c = col_id,
   * b = packed (is_unsigned << 15) | (precision << 8) | scale.
   * NULL values and every conversion error take the per-row
   * interpreter fallback (which reproduces the interpreter's exact
   * ZAGG_DECIMAL_* error for the error cases). */
  OP_LOAD_COL_NDB_DEC      = 55,
  /* Phase 5F-1: FUSED string MIN/MAX — one cold call per string
   * aggregate covering load + collation compare + winner-buffer
   * update. The helper delegates to
   * AggInterpreterBase::jitMinMaxStringCol, which reuses the
   * interpreter's own load path and public minMaxString kernel — the
   * kernel mutates the group's AggResItem DIRECTLY and the helper
   * never sets value_updated, so the glue's masked writeback leaves
   * string slots alone. NULL values are skipped by the kernel (no
   * fallback of any kind); kernel errors (alloc failure) take the
   * per-row fallback so the interpreter surfaces the exact ZAGG
   * error. Operand layout: b = col_id,
   * c = (is_max << 8) | agg_index; a = agg_index (diagnostics). */
  OP_MINMAX_STR_NDB        = 56,
  OP_KIND_MAX           = OP_MINMAX_STR_NDB
} OpKind;

/* Inline predicate — true for any opcode that takes a forward branch
 * via op->c. Includes the six BRANCH_*_INT_INT siblings (Phase 3) and
 * the four cold-call null-check branches (Phase 5.0/5.1a) whose
 * stencils carry an HK_BRANCH_TAKE hole. Admission validates op->c
 * is forward-and-in-range for every kind listed here. */
static inline int bc_op_is_branch(uint8_t kind) {
  switch (kind) {
    case OP_BRANCH_LT_INT_INT:
    case OP_BRANCH_LE_INT_INT:
    case OP_BRANCH_EQ_INT_INT:
    case OP_BRANCH_GT_INT_INT:
    case OP_BRANCH_GE_INT_INT:
    case OP_BRANCH_NE_INT_INT:
    case OP_BRANCH_ATTR_EQ_NULL:
    case OP_BRANCH_ATTR_NE_NULL:
    case OP_BRANCH_LINKED_EQ_NULL:
    case OP_BRANCH_LINKED_NE_NULL:
    case OP_BRANCH_ATTR_OP_ARG:
    case OP_JUMP:
    case OP_LOAD_COL_NDB_NB:
    case OP_LOAD_COL_NDB_F64_NB:
    case OP_LOAD_COL_NDB_U64_NB:
      return 1;
    default:
      return 0;
  }
}

typedef struct {
  uint8_t  kind;       /* OpKind */
  uint8_t  a;          /* register or accumulator slot (≤ BC_MAX_REGS) */
  uint16_t b;          /* register, column/attr id, or unused.
                        * 16-bit so it can carry NDB column/attr IDs
                        * including PSEUDO_FLAG (0x8000+). */
  uint16_t c;          /* register, column id, or branch target pc.
                        * 16-bit for the same reason as b. */
  uint16_t d;          /* overflow branch target pc for checked arithmetic;
                        * unused by existing opcodes. */
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
