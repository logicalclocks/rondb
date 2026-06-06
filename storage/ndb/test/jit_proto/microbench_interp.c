/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license as bytecode1.h.)
 */

/*
 * RONDB-1056 Phase 1 — switch-on-opcode interpreter.
 *
 * Models AggInterpreter::ProcessRec's dispatch shape: per-row outer
 * loop, per-op inner loop with switch dispatch. No object
 * construction, no signal handling, no kernel dependencies.
 */

#include "microbench_interp.h"

#include <stddef.h>
#include <string.h>

typedef struct {
  int64_t  regs_i64[BC_MAX_REGS];
  int64_t  acc_i64[BC_MAX_ACCS];
  const int64_t *row_cols_i64;   /* width BC_MAX_COLS */
} InterpState;

void interp_run(const Program *prog,
                const Row *rows,
                size_t nrows,
                int64_t *out_acc) {
  InterpState s;
  memset(&s, 0, sizeof(s));

  for (size_t r = 0; r < nrows; ++r) {
    s.row_cols_i64 = rows[r].cols;
    /* Per-row register file is fresh — accumulators carry over. */
    memset(s.regs_i64, 0, sizeof(s.regs_i64));

    for (size_t pc = 0; pc < prog->n_ops; /* incremented per case */) {
      const Op *op = &prog->ops[pc];
      switch (op->kind) {
        case OP_LOAD_CONST_INT:
          s.regs_i64[op->a] = op->imm;
          ++pc;
          break;

        case OP_LOAD_COL_INT:
          s.regs_i64[op->a] = s.row_cols_i64[op->b];
          ++pc;
          break;

        case OP_MOV_INT_INT:
          s.regs_i64[op->a] = s.regs_i64[op->b];
          ++pc;
          break;

        case OP_ADD_INT_INT:
          s.regs_i64[op->a] = s.regs_i64[op->b] + s.regs_i64[op->c];
          ++pc;
          break;

        case OP_MINUS_INT_INT:
          s.regs_i64[op->a] = s.regs_i64[op->b] - s.regs_i64[op->c];
          ++pc;
          break;

        case OP_MUL_INT_INT:
          s.regs_i64[op->a] = s.regs_i64[op->b] * s.regs_i64[op->c];
          ++pc;
          break;

        case OP_SUM_BIGINT:
          s.acc_i64[op->a] += s.regs_i64[op->b];
          ++pc;
          break;

        case OP_ADD_INT_INT_CHECKED: {
          int64_t result;
          int overflow = __builtin_add_overflow(s.regs_i64[op->b],
                                                s.regs_i64[op->c], &result);
          if (overflow) {
            pc = op->d;
          } else {
            s.regs_i64[op->a] = result;
            ++pc;
          }
          break;
        }

        case OP_MINUS_INT_INT_CHECKED: {
          int64_t result;
          int overflow = __builtin_sub_overflow(s.regs_i64[op->b],
                                                s.regs_i64[op->c], &result);
          if (overflow) {
            pc = op->d;
          } else {
            s.regs_i64[op->a] = result;
            ++pc;
          }
          break;
        }

        case OP_MUL_INT_INT_CHECKED: {
          int64_t result;
          int overflow = __builtin_mul_overflow(s.regs_i64[op->b],
                                                s.regs_i64[op->c], &result);
          if (overflow) {
            pc = op->d;
          } else {
            s.regs_i64[op->a] = result;
            ++pc;
          }
          break;
        }

        case OP_SUM_BIGINT_CHECKED: {
          int64_t result;
          int overflow = __builtin_add_overflow(s.acc_i64[op->a],
                                                s.regs_i64[op->b], &result);
          if (overflow) {
            pc = op->d;
          } else {
            s.acc_i64[op->a] = result;
            ++pc;
          }
          break;
        }

        case OP_BRANCH_LT_INT_INT:
          if (s.regs_i64[op->a] < s.regs_i64[op->b]) {
            pc = op->c;
          } else {
            ++pc;
          }
          break;

        case OP_BRANCH_LE_INT_INT:
          if (s.regs_i64[op->a] <= s.regs_i64[op->b]) {
            pc = op->c;
          } else {
            ++pc;
          }
          break;

        case OP_BRANCH_EQ_INT_INT:
          if (s.regs_i64[op->a] == s.regs_i64[op->b]) {
            pc = op->c;
          } else {
            ++pc;
          }
          break;

        case OP_BRANCH_GT_INT_INT:
          if (s.regs_i64[op->a] > s.regs_i64[op->b]) {
            pc = op->c;
          } else {
            ++pc;
          }
          break;

        case OP_BRANCH_GE_INT_INT:
          if (s.regs_i64[op->a] >= s.regs_i64[op->b]) {
            pc = op->c;
          } else {
            ++pc;
          }
          break;

        case OP_BRANCH_NE_INT_INT:
          if (s.regs_i64[op->a] != s.regs_i64[op->b]) {
            pc = op->c;
          } else {
            ++pc;
          }
          break;

        case OP_SKIP:
          /* Forward jump to row_end — represented here by exiting
           * the per-row pc loop. */
          goto row_done;

        case OP_EXIT:
          goto row_done;

        case OP_OVERFLOW_EXIT:
          goto row_done;

        default:
          /* Unknown opcode — Phase 1 has no fallback; in production
           * this is the JIT's whole-program fallback trigger.
           * Here we just stop the row to avoid undefined behaviour. */
          goto row_done;
      }
    }
  row_done: ;
  }

  *out_acc = s.acc_i64[0];
}
