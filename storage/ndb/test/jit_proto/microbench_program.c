/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license as bytecode1.h.)
 */

/*
 * RONDB-1056 Phase 1 — program builder + row generator.
 */

#include "microbench_program.h"

#include <stdlib.h>
#include <string.h>

/* xorshift64 — a small deterministic PRNG. We don't need
 * cryptographic quality, just reproducible row data. */
static uint64_t xs64(uint64_t *st) {
  uint64_t x = *st;
  x ^= x << 13;
  x ^= x >> 7;
  x ^= x << 17;
  *st = x;
  return x;
}

static void emit_(Program *p, OpKind k, uint8_t a, uint8_t b,
                  uint8_t c, int64_t imm) {
  Op *op = &p->ops[p->n_ops++];
  op->kind = (uint8_t)k;
  op->a = a;
  op->b = b;
  op->c = c;
  op->imm = imm;
}

void mb_build_30op_program(Program *out) {
  memset(out, 0, sizeof(*out));

  /* The branch target is filled in after we know the label_skip pc.
   * We emit the branch with c=0 first, patch later. */
  size_t branch_pc;

  /* r0 = 1000 (threshold) */
  emit_(out, OP_LOAD_CONST_INT, 0, 0, 0, 1000);
  /* r1 = 0 */
  emit_(out, OP_LOAD_CONST_INT, 1, 0, 0, 0);
  /* r2 = col[0] */
  emit_(out, OP_LOAD_COL_INT,   2, 0, 0, 0);
  /* if r2 < r0 -> label_skip */
  branch_pc = out->n_ops;
  emit_(out, OP_BRANCH_LT_INT_INT, 2, 0, /* target=*/0, 0);

  /* Three load+sum pairs against col[1..3]. */
  emit_(out, OP_LOAD_COL_INT, 3, 1, 0, 0);  /* r3 = col[1] */
  emit_(out, OP_SUM_BIGINT,   0, 3, 0, 0);  /* acc[0] += r3 */
  emit_(out, OP_LOAD_COL_INT, 3, 2, 0, 0);  /* r3 = col[2] */
  emit_(out, OP_SUM_BIGINT,   0, 3, 0, 0);  /* acc[0] += r3 */
  emit_(out, OP_LOAD_COL_INT, 3, 3, 0, 0);  /* r3 = col[3] */
  emit_(out, OP_SUM_BIGINT,   0, 3, 0, 0);  /* acc[0] += r3 */

  /* Filler: mov/add chain to grow op count toward 30. Carries data
   * through r4/r5/r6/r7 to keep the chain real (no dead code that
   * the C compiler might learn to fold — though clang doesn't see
   * our bytecode anyway). */
  emit_(out, OP_MOV_INT_INT, 4, 2, 0, 0);   /* r4 = r2 */
  emit_(out, OP_ADD_INT_INT, 5, 4, 1, 0);   /* r5 = r4 + r1 */
  emit_(out, OP_MOV_INT_INT, 6, 5, 0, 0);   /* r6 = r5 */
  emit_(out, OP_ADD_INT_INT, 7, 6, 4, 0);   /* r7 = r6 + r4 */
  emit_(out, OP_ADD_INT_INT, 4, 7, 1, 0);   /* r4 = r7 + r1 */
  emit_(out, OP_ADD_INT_INT, 5, 4, 7, 0);   /* r5 = r4 + r7 */
  emit_(out, OP_MOV_INT_INT, 6, 5, 0, 0);   /* r6 = r5 */
  emit_(out, OP_ADD_INT_INT, 7, 6, 1, 0);   /* r7 = r6 + r1 */

  /* A second sum pair to keep the aggregate non-trivial. */
  emit_(out, OP_LOAD_COL_INT, 3, 1, 0, 0);  /* r3 = col[1] */
  emit_(out, OP_SUM_BIGINT,   0, 3, 0, 0);  /* acc[0] += r3 */
  emit_(out, OP_LOAD_COL_INT, 3, 2, 0, 0);
  emit_(out, OP_SUM_BIGINT,   0, 3, 0, 0);

  /* More mov/add filler to push us to 30 ops. */
  emit_(out, OP_MOV_INT_INT, 4, 7, 0, 0);
  emit_(out, OP_ADD_INT_INT, 5, 4, 7, 0);
  emit_(out, OP_MOV_INT_INT, 6, 5, 0, 0);
  emit_(out, OP_ADD_INT_INT, 7, 6, 1, 0);
  emit_(out, OP_MOV_INT_INT, 4, 7, 0, 0);
  emit_(out, OP_ADD_INT_INT, 5, 4, 1, 0);

  /* Final exit. */
  emit_(out, OP_EXIT, 0, 0, 0, 0);

  /* label_skip: */
  size_t label_skip = out->n_ops;
  emit_(out, OP_SKIP, 0, 0, 0, 0);

  /* Patch the branch target. */
  out->ops[branch_pc].c = (uint8_t)label_skip;
}

void mb_build_checked_30op_program(Program *out) {
  mb_build_30op_program(out);
  uint16_t overflow_pc = out->n_ops;
  emit_(out, OP_OVERFLOW_EXIT, 0, 0, 0, 0);

  for (uint16_t pc = 0; pc < overflow_pc; pc++) {
    switch (out->ops[pc].kind) {
      case OP_ADD_INT_INT:
        out->ops[pc].kind = OP_ADD_INT_INT_CHECKED;
        out->ops[pc].d = overflow_pc;
        break;
      case OP_SUM_BIGINT:
        out->ops[pc].kind = OP_SUM_BIGINT_CHECKED;
        out->ops[pc].d = overflow_pc;
        break;
      default:
        break;
    }
  }
}

void mb_build_forked_program(Program *out) {
  memset(out, 0, sizeof(*out));
  size_t br_le_pc, br_eq_pc, br_gt_pc;

  /* Constants. r0=50, r1=100, r3=1000 (loaded later). */
  emit_(out, OP_LOAD_CONST_INT, 0, 0, 0, 50);
  emit_(out, OP_LOAD_CONST_INT, 1, 0, 0, 100);
  emit_(out, OP_LOAD_COL_INT,   2, 0, 0, 0);   /* r2 = col[0] */

  /* if r2 <= 50 -> label_low (~2.5% of rows) */
  br_le_pc = out->n_ops;
  emit_(out, OP_BRANCH_LE_INT_INT, 2, 0, /*target=*/0, 0);

  /* if r2 == 100 -> label_eq (~0.05% of rows) */
  br_eq_pc = out->n_ops;
  emit_(out, OP_BRANCH_EQ_INT_INT, 2, 1, /*target=*/0, 0);

  /* r3 = 1000 ; if r2 > r3 -> label_high (~50% of rows) */
  emit_(out, OP_LOAD_CONST_INT, 3, 0, 0, 1000);
  br_gt_pc = out->n_ops;
  emit_(out, OP_BRANCH_GT_INT_INT, 2, 3, /*target=*/0, 0);

  /* Mid-segment: r2 in (50, 100) ∪ (100, 1000].
   * Sum col[1] + col[2] into acc[0]. */
  emit_(out, OP_LOAD_COL_INT, 4, 1, 0, 0);
  emit_(out, OP_SUM_BIGINT,   0, 4, 0, 0);
  emit_(out, OP_LOAD_COL_INT, 4, 2, 0, 0);
  emit_(out, OP_SUM_BIGINT,   0, 4, 0, 0);
  emit_(out, OP_EXIT, 0, 0, 0, 0);

  /* label_high: r2 > 1000 -> sum col[3] into acc[1]. */
  size_t label_high = out->n_ops;
  emit_(out, OP_LOAD_COL_INT, 4, 3, 0, 0);
  emit_(out, OP_SUM_BIGINT,   1, 4, 0, 0);
  emit_(out, OP_EXIT, 0, 0, 0, 0);

  /* label_eq: r2 == 100 exactly -> sum col[2] into acc[2]. */
  size_t label_eq = out->n_ops;
  emit_(out, OP_LOAD_COL_INT, 4, 2, 0, 0);
  emit_(out, OP_SUM_BIGINT,   2, 4, 0, 0);
  emit_(out, OP_EXIT, 0, 0, 0, 0);

  /* label_low: r2 <= 50 -> skip (no contribution). */
  size_t label_low = out->n_ops;
  emit_(out, OP_SKIP, 0, 0, 0, 0);

  /* Patch the three branch targets. */
  out->ops[br_le_pc].c = (uint8_t)label_low;
  out->ops[br_eq_pc].c = (uint8_t)label_eq;
  out->ops[br_gt_pc].c = (uint8_t)label_high;
}

Row *mb_generate_rows(size_t nrows, uint64_t seed) {
  Row *rows = (Row *)calloc(nrows, sizeof(Row));
  if (!rows) return NULL;
  uint64_t st = seed ? seed : 0xa5a5a5a5a5a5a5a5ull;
  for (size_t i = 0; i < nrows; ++i) {
    /* col[0]: filter key in [0, 2000) — about half are < threshold 1000 */
    rows[i].cols[0] = (int64_t)(xs64(&st) % 2000ull);
    /* col[1..3]: small positives in [1, 100) so the sum stays
     * predictable but non-trivial */
    rows[i].cols[1] = (int64_t)(xs64(&st) % 99ull) + 1;
    rows[i].cols[2] = (int64_t)(xs64(&st) % 99ull) + 1;
    rows[i].cols[3] = (int64_t)(xs64(&st) % 99ull) + 1;
    /* col[4..7]: zero */
  }
  return rows;
}

const char *bc_op_name(uint8_t kind) {
  switch (kind) {
    case OP_LOAD_CONST_INT:    return "load_const_int";
    case OP_LOAD_COL_INT:      return "load_col_int";
    case OP_MOV_INT_INT:       return "mov_int_int";
    case OP_ADD_INT_INT:       return "add_int_int";
    case OP_SUM_BIGINT:        return "sum_bigint";
    case OP_BRANCH_LT_INT_INT: return "branch_lt_int_int";
    case OP_BRANCH_LE_INT_INT: return "branch_le_int_int";
    case OP_BRANCH_EQ_INT_INT: return "branch_eq_int_int";
    case OP_BRANCH_GT_INT_INT: return "branch_gt_int_int";
    case OP_BRANCH_GE_INT_INT: return "branch_ge_int_int";
    case OP_BRANCH_NE_INT_INT: return "branch_ne_int_int";
    case OP_SKIP:              return "skip";
    case OP_EXIT:              return "exit";
    case OP_MINUS_INT_INT:     return "minus_int_int";
    case OP_MUL_INT_INT:       return "mul_int_int";
    case OP_LOAD_COL_NDB:      return "load_col_ndb";
    case OP_ADD_INT_INT_CHECKED:   return "add_int_int_checked";
    case OP_MINUS_INT_INT_CHECKED: return "minus_int_int_checked";
    case OP_MUL_INT_INT_CHECKED:   return "mul_int_int_checked";
    case OP_SUM_BIGINT_CHECKED:    return "sum_bigint_checked";
    case OP_OVERFLOW_EXIT:         return "overflow_exit";
    case OP_JUMP:                  return "jump";
    default:                   return "?";
  }
}
