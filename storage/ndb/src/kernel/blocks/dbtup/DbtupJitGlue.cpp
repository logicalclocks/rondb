/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#include "DbtupJitGlue.hpp"
#include "JoinAggInterpreter.hpp"
#include "AggInterpreter.hpp"   /* for AlignedType / IsUnsigned helpers */

#include <ndb_global.h>
#include <my_byteorder.h>      /* sint8korr */

#include <cstring>

/* ------------------------------------------------------------------ */
/* Cold-call helpers.                                                 */
/* ------------------------------------------------------------------ */

extern "C" void
ndb_jit_h_load_col(JitState *s, uint32_t col_id, uint32_t dst_reg) {
  auto *ctx = static_cast<dbtup_jit_call_ctx *>(s->ctx);
  /* Phase 4 admission guarantees ctx is set. If it isn't, the
   * dispatch path is broken — fail fast. ndbrequire requires a
   * JAM context only available inside blocks, so we use direct
   * null-checks + abort here. */
  if (ctx == nullptr || ctx->block_tup == nullptr ||
      ctx->req_struct == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_load_col: JitState.ctx is malformed (col_id=%u)",
        col_id);
    abort();
  }

  /* Read buffer: 1 word AttributeHeader + up to 2 words (8 bytes)
   * for a BIGINT. 4 words gives breathing room for alignment.
   *
   * readAttributes is private on Dbtup (friended only to
   * AggInterpreter / JoinAggInterpreter). The glue calls into
   * ctx->agg->readAttributeForJit which forwards through the
   * friend access. */
  Uint32 read_buf[4];

  int ret = ctx->agg->readAttributeForJit(ctx->block_tup,
                                            ctx->req_struct,
                                            col_id, read_buf,
                                            sizeof(read_buf) / sizeof(Uint32));
  if (ret < 0) {
    /* Column read failed — Phase 4 panics. Phase 5 wires this into
     * the JoinAggInterpreter error path so the row can be skipped
     * cleanly. */
    g_eventLogger->error(
        "ndb_jit_h_load_col: readAttributes failed for col_id=%u (rc=%d)",
        col_id, ret);
    abort();
  }

  AttributeHeader *header =
      reinterpret_cast<AttributeHeader *>(&read_buf[0]);
  if (header->isNULL()) {
    /* Bridge admission was supposed to reject programs touching
     * nullable columns. If a NULL surfaced anyway, schema /
     * admission has a bug. */
    g_eventLogger->error(
        "ndb_jit_h_load_col: unexpected NULL value for col_id=%u "
        "(admission should have rejected the program)", col_id);
    abort();
  }

  /* Decode the BIGINT value (8 bytes, little-endian). */
  s->regs_i64[dst_reg] =
      sint8korr(reinterpret_cast<char *>(&read_buf[1]));
}

/* ndb_jit_h_branch_attr_null — Phase 5.0 cold-call branch helper.
 *
 * Used by both op_branch_attr_eq_null (want_null=1) and
 * op_branch_attr_ne_null (want_null=0). Reads the column's
 * AttributeHeader via the same readAttributeForJit path as
 * ndb_jit_h_load_col, checks isNULL(), and returns:
 *   1 → take the branch (e.g., for IS NULL: column is null AND
 *       want_null=1, so the embedded EXIT_REFUSE landing pad —
 *       sorry actually the semantics are flipped: see the bridge
 *       — `WHERE c IS NULL` emits BRANCH_ATTR_NE_NULL +offset to
 *       EXIT_REFUSE, so taking the branch here means rejecting
 *       the row).
 *   0 → fall through.
 *
 * Lifetime: ctx is stack-local in dbtup_jit_invoke; this helper
 * runs synchronously inside that invocation, so ctx is always
 * valid. */
extern "C" int
ndb_jit_h_branch_attr_null(JitState *s,
                            uint32_t attr_id,
                            uint32_t want_null) {
  auto *ctx = static_cast<dbtup_jit_call_ctx *>(s->ctx);
  if (ctx == nullptr || ctx->block_tup == nullptr ||
      ctx->req_struct == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_branch_attr_null: JitState.ctx is malformed "
        "(attr_id=%u)", attr_id);
    abort();
  }

  /* Read just the AttributeHeader; the value bytes that follow
   * don't matter for a null check. 4 words still gives breathing
   * room for the readAttributes path's worst-case header
   * size. */
  Uint32 read_buf[4];
  int ret = ctx->agg->readAttributeForJit(ctx->block_tup,
                                            ctx->req_struct,
                                            attr_id, read_buf,
                                            sizeof(read_buf) /
                                                sizeof(Uint32));
  if (ret < 0) {
    /* Column read failed — same Phase 4 policy: panic. Phase 5+
     * wires this into JoinAggInterpreter's error path so a row
     * can be skipped cleanly. */
    g_eventLogger->error(
        "ndb_jit_h_branch_attr_null: readAttributes failed for "
        "attr_id=%u (rc=%d)", attr_id, ret);
    abort();
  }

  AttributeHeader *header =
      reinterpret_cast<AttributeHeader *>(&read_buf[0]);
  bool is_null = header->isNULL();
  return (is_null == (want_null != 0)) ? 1 : 0;
}

/* ------------------------------------------------------------------ */
/* Helper registration.                                               */
/* ------------------------------------------------------------------ */

extern "C" void dbtup_jit_register_helpers(void) {
  /* The cast through (void(*)(void)) is safe — the helper-registry
   * stores generic JitHelperFn pointers and the engine's
   * HK_COLDCALL patcher only uses the function's address, not its
   * signature. The stencil source's extern declarations are what
   * enforce the call ABI at codegen time. */
  jit1_register_helper("ndb_jit_h_load_col",
                        reinterpret_cast<JitHelperFn>(&ndb_jit_h_load_col));
  jit1_register_helper("ndb_jit_h_branch_attr_null",
                        reinterpret_cast<JitHelperFn>(&ndb_jit_h_branch_attr_null));
}

/* ------------------------------------------------------------------ */
/* Per-row dispatch.                                                  */
/* ------------------------------------------------------------------ */

Int32 dbtup_jit_invoke(JoinAggInterpreter * /*agg*/,
                       Dbtup *block_tup,
                       Dbtup::KeyReqStruct *req_struct,
                       JitEntry            entry_fn,
                       AggResItem         *agg_res_ptr,
                       Uint32              n_agg_results) {
  /* Build the per-row context on the stack. JitState.ctx points
   * at this; helpers consult it during the JIT'd code's execution
   * and never retain pointers into it. */
  dbtup_jit_call_ctx ctx;
  ctx.agg        = nullptr;   /* unused on Phase 4's cold-call path */
  ctx.block_tup  = block_tup;
  ctx.req_struct = req_struct;

  JitState s;
  std::memset(&s, 0, sizeof(s));
  s.ctx = &ctx;

  /* Read accumulators into s.acc_i64. Phase 4 narrow: non-null
   * BIGINT only. Cap at BC_MAX_ACCS — bridge admission rejects
   * programs that would exceed this. */
  if (n_agg_results > BC_MAX_ACCS) n_agg_results = BC_MAX_ACCS;
  for (Uint32 i = 0; i < n_agg_results; i++) {
    s.acc_i64[i] = agg_res_ptr[i].value.val_int64;
  }

  /* Run the JIT'd program. */
  entry_fn(&s);

  /* Write accumulators back. The metadata fields (type,
   * is_unsigned, is_null) are set even on first row so downstream
   * result-emit code sees the right shape. */
  for (Uint32 i = 0; i < n_agg_results; i++) {
    agg_res_ptr[i].type        = NDB_TYPE_BIGINT;
    agg_res_ptr[i].is_unsigned = false;
    agg_res_ptr[i].is_null     = false;
    agg_res_ptr[i].value.val_int64 = s.acc_i64[i];
  }

  return 0;
}
