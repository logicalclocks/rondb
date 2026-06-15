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
#include "AggInterpreterBase.hpp"
#include "JoinAggInterpreter.hpp"
#include "AggInterpreter.hpp"   /* for AlignedType / IsUnsigned helpers */

#include <ndb_global.h>
#include <my_byteorder.h>      /* sint8korr */

#include <cstring>

extern "C" {
#include "jit/ndb_jit_bridge.h"   /* ndb_jit_bridge_translate_scan_filter */
}

#ifndef ZAGG_MATH_OVERFLOW
#define ZAGG_MATH_OVERFLOW 1860
#endif

#ifdef ERROR_INSERT
static bool dbtup_jit_trace_start(AggInterpreterBase *agg,
                                  Dbtup *block_tup,
                                  Uint32 *row_no,
                                  Uint32 *limit) {
  if (agg == nullptr ||
      !agg->jitTraceEnabledForJit(block_tup, limit)) {
    return false;
  }
  Uint64 processed = agg->processed_rows();
  Uint32 current = processed > ~Uint32(0)
                     ? ~Uint32(0)
                     : static_cast<Uint32>(processed);
  if (current > *limit) {
    return false;
  }
  *row_no = current;
  return true;
}

static void dbtup_jit_trace_accs(const char *stage,
                                 Uint32 row_no,
                                 const int64_t *accs,
                                 Uint32 n_accs) {
  for (Uint32 i = 0; i < n_accs; i++) {
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u %s acc[%u]=%lld",
        row_no, stage, i, (long long)accs[i]);
  }
}
#endif

/* ------------------------------------------------------------------ */
/* Cold-call helpers.                                                 */
/* ------------------------------------------------------------------ */

extern "C" void
ndb_jit_h_load_col(JitState *s, uint32_t col_id, uint32_t dst_reg) {
  dbtup_jit_call_ctx *ctx =
      static_cast<dbtup_jit_call_ctx *>(s->ctx);
  /* Admission guarantees ctx is set. If it isn't, the dispatch path
   * is broken — fail fast. ndbrequire requires a JAM context only
   * available inside blocks, so we use direct null-checks + abort
   * here. ctx->agg is NOT required: the scan-filter path
   * (dbtup_jit_invoke_scan_filter) leaves it null and reaches the row
   * through block_tup + req_struct, same as the aggregation path. */
  if (ctx == nullptr ||
      ctx->block_tup == nullptr || ctx->req_struct == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_load_col: JitState.ctx is malformed (col_id=%u)",
        col_id);
    abort();
  }

  /* Read buffer: 1 word AttributeHeader + up to 2 words (8 bytes)
   * for a BIGINT. 4 words gives breathing room for alignment.
   *
   * readSingleAttribute is private on Dbtup; the glue reaches it via
   * the public Dbtup::readSingleAttributeForJit forwarder (the same
   * call AggInterpreterBase::readAttributeForJit makes), so this
   * helper no longer depends on an AggInterpreter instance. */
  Uint32 read_buf[4];

  int ret = ctx->block_tup->readSingleAttributeForJit(
      ctx->req_struct, col_id, read_buf,
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

#ifdef ERROR_INSERT
  if (ctx->trace_enabled) {
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u helper=load_col col=%u dst=r%u "
        "value=%lld",
        ctx->trace_row_no, col_id, dst_reg,
        (long long)s->regs_i64[dst_reg]);
  }
#endif
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
  dbtup_jit_call_ctx *ctx =
      static_cast<dbtup_jit_call_ctx *>(s->ctx);
  /* ctx->agg is NOT required — see ndb_jit_h_load_col. The scan-filter
   * path leaves agg null and reads through block_tup + req_struct. */
  if (ctx == nullptr ||
      ctx->block_tup == nullptr || ctx->req_struct == nullptr) {
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
  int ret = ctx->block_tup->readSingleAttributeForJit(
      ctx->req_struct, attr_id, read_buf,
      sizeof(read_buf) / sizeof(Uint32));
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
  int take_branch = (is_null == (want_null != 0)) ? 1 : 0;
#ifdef ERROR_INSERT
  if (ctx->trace_enabled) {
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u helper=branch_attr_null attr=%u "
        "want_null=%u is_null=%u take=%u",
        ctx->trace_row_no, attr_id, want_null, is_null ? 1 : 0,
        take_branch);
  }
#endif
  return take_branch;
}

/* ndb_jit_h_branch_attr_op_arg — Phase 7 cold-call branch helper for
 * BRANCH_ATTR_OP_ARG (WHERE col <op> literal). The whole instruction is
 * read from the program buffer: ctx->prog_buf + inst_word_off points at the
 * instruction's word 0, and Dbtup::evalBranchColLiteralForJit decodes it
 * (cond / nulls / attrId / inline literal), reads the column, and compares
 * via the type's NdbSqlUtil comparator — mirroring the interpreter's
 * handleBranchAttrOp. Returns 1 to take the branch, 0 to fall through. */
extern "C" int
ndb_jit_h_branch_attr_op_arg(JitState *s, uint32_t inst_word_off) {
  dbtup_jit_call_ctx *ctx =
      static_cast<dbtup_jit_call_ctx *>(s->ctx);
  if (ctx == nullptr || ctx->block_tup == nullptr ||
      ctx->req_struct == nullptr || ctx->prog_buf == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_branch_attr_op_arg: JitState.ctx is malformed "
        "(inst_word_off=%u)", inst_word_off);
    abort();
  }
  int rc = ctx->block_tup->evalBranchColLiteralForJit(
      ctx->req_struct, ctx->prog_buf + inst_word_off);
  if (rc < 0) {
    /* Read failure / unsupported type or condition. The bridge's admission
     * is meant to keep these off the JIT path; if one surfaces, fail fast
     * (same policy as the other helpers) rather than silently mis-filter. */
    g_eventLogger->error(
        "ndb_jit_h_branch_attr_op_arg: evalBranchColLiteralForJit failed "
        "(inst_word_off=%u, rc=%d)", inst_word_off, rc);
    abort();
  }
#ifdef ERROR_INSERT
  if (ctx->trace_enabled) {
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u helper=branch_attr_op_arg off=%u take=%d",
        ctx->trace_row_no, inst_word_off, rc);
  }
#endif
  return rc;
}

/* ndb_jit_h_read_linked_to_mem — Phase 5.1a cold-call helper.
 *
 * Used by op_load_linked_to_mem to populate
 * ctx->block_tup->cheapMemory[0] from the row's linked-attr buffer
 * at the patched position. Delegates to Dbtup::readLinkedToMemBuffer
 * so the buffer-walk logic is shared one-to-one with NDB's
 * interpreter READ_LINKED_TO_MEM handler — no drift risk. */
extern "C" void
ndb_jit_h_read_linked_to_mem(JitState *s, uint32_t position) {
  dbtup_jit_call_ctx *ctx =
      static_cast<dbtup_jit_call_ctx *>(s->ctx);
  if (ctx == nullptr || ctx->join_agg == nullptr ||
      ctx->block_tup == nullptr || ctx->req_struct == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_read_linked_to_mem: JitState.ctx is malformed "
        "(position=%u)", position);
    abort();
  }
  /* Routes through JoinAggInterpreter::readLinkedToMemForJit since
   * Dbtup::cheapMemory is private — JoinAggInterpreter is friend of
   * Dbtup so it can reach the buffer + the static walk routine. */
  JoinAggInterpreter *join_agg = ctx->join_agg;
  join_agg->readLinkedToMemForJit(ctx->block_tup, ctx->req_struct,
                                  position);
#ifdef ERROR_INSERT
  if (ctx->trace_enabled) {
    AttributeHeader ah(join_agg->cheapMemoryHeaderForJit(ctx->block_tup));
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u helper=read_linked_to_mem "
        "position=%u is_null=%u bytes=%u",
        ctx->trace_row_no, position, ah.isNULL() ? 1 : 0,
        ah.getByteSize());
  }
#endif
}

/* ndb_jit_h_branch_linked_null — Phase 5.1a cold-call branch helper.
 *
 * Returns 1 to take the branch, 0 to fall through. Both
 * op_branch_linked_eq_null (want_null=1) and op_branch_linked_ne_null
 * (want_null=0) share this helper. Inspects the AttributeHeader at
 * cheapMemory[0] which a preceding op_load_linked_to_mem populated. */
extern "C" int
ndb_jit_h_branch_linked_null(JitState *s, uint32_t want_null) {
  dbtup_jit_call_ctx *ctx =
      static_cast<dbtup_jit_call_ctx *>(s->ctx);
  if (ctx == nullptr || ctx->join_agg == nullptr ||
      ctx->block_tup == nullptr) {
    g_eventLogger->error(
        "ndb_jit_h_branch_linked_null: JitState.ctx is malformed");
    abort();
  }
  JoinAggInterpreter *join_agg = ctx->join_agg;
  AttributeHeader ah(join_agg->cheapMemoryHeaderForJit(ctx->block_tup));
  bool is_null = ah.isNULL();
  int take_branch = (is_null == (want_null != 0)) ? 1 : 0;
#ifdef ERROR_INSERT
  if (ctx->trace_enabled) {
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u helper=branch_linked_null "
        "want_null=%u is_null=%u take=%u",
        ctx->trace_row_no, want_null, is_null ? 1 : 0,
        take_branch);
  }
#endif
  return take_branch;
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
  jit1_register_helper("ndb_jit_h_branch_attr_op_arg",
                        reinterpret_cast<JitHelperFn>(&ndb_jit_h_branch_attr_op_arg));
  jit1_register_helper("ndb_jit_h_read_linked_to_mem",
                        reinterpret_cast<JitHelperFn>(&ndb_jit_h_read_linked_to_mem));
  jit1_register_helper("ndb_jit_h_branch_linked_null",
                        reinterpret_cast<JitHelperFn>(&ndb_jit_h_branch_linked_null));
}

/* ------------------------------------------------------------------ */
/* Per-row dispatch.                                                  */
/* ------------------------------------------------------------------ */

Int32 dbtup_jit_invoke(AggInterpreterBase *agg,
                       Dbtup *block_tup,
                       Dbtup::KeyReqStruct *req_struct,
                       JitEntry            entry_fn,
                       AggResItem         *agg_res_ptr,
                       Uint32              n_agg_results,
                       JoinAggInterpreter *join_agg) {
  /* Build the per-row context on the stack. JitState.ctx points
   * at this; helpers consult it during the JIT'd code's execution
   * and never retain pointers into it. */
  dbtup_jit_call_ctx ctx;
  ctx.agg        = agg;
  ctx.join_agg   = join_agg;
  ctx.block_tup  = block_tup;
  ctx.req_struct = req_struct;
  ctx.prog_buf   = nullptr;  /* aggregation path emits no BRANCH_ATTR_OP_ARG */
#ifdef ERROR_INSERT
  ctx.trace_enabled = dbtup_jit_trace_start(agg, block_tup,
                                            &ctx.trace_row_no,
                                            &ctx.trace_limit);
#endif

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

#ifdef ERROR_INSERT
  if (ctx.trace_enabled) {
    g_eventLogger->info(
        "ERROR_INSERT 4063: row=%u/%u jit invoke entry=%p "
        "n_agg_results=%u",
        ctx.trace_row_no, ctx.trace_limit,
        reinterpret_cast<void *>(entry_fn), n_agg_results);
    dbtup_jit_trace_accs("before", ctx.trace_row_no,
                         s.acc_i64, n_agg_results);
  }
#endif

  /* Run the JIT'd program. */
  entry_fn(&s);

  if (s.row_overflowed != 0) {
    return ZAGG_MATH_OVERFLOW;
  }

#ifdef ERROR_INSERT
  if (ctx.trace_enabled) {
    dbtup_jit_trace_accs("after", ctx.trace_row_no,
                         s.acc_i64, n_agg_results);
    for (Uint32 i = 0; i < BC_MAX_REGS; i++) {
      g_eventLogger->info(
          "ERROR_INSERT 4063: row=%u after reg[%u]=%lld",
          ctx.trace_row_no, i, (long long)s.regs_i64[i]);
    }
  }
#endif

  /* Write accumulators back only for aggregate results updated by this
   * row. Rejected rows leave NULL metadata intact for SUM/MIN/MAX
   * semantics over an empty input. */
  for (Uint32 i = 0; i < n_agg_results; i++) {
    if (s.value_updated[i] != 0) {
      agg_res_ptr[i].type        = NDB_TYPE_BIGINT;
      agg_res_ptr[i].is_unsigned = false;
      agg_res_ptr[i].is_null     = false;
      agg_res_ptr[i].value.val_int64 = s.acc_i64[i];
    }
  }

  return 0;
}

/* ------------------------------------------------------------------ */
/* Phase 7 — scan-filter compile + per-row invoke.                    */
/* ------------------------------------------------------------------ */

void *dbtup_jit_compile_scan_filter(NdbJitArena *arena,
                                    const Uint32 *filter_prog,
                                    Uint32        n_words,
                                    Uint32       *out_reject_code) {
  if (out_reject_code != nullptr) {
    *out_reject_code = 0;
  }
  if (arena == nullptr || filter_prog == nullptr || n_words == 0) {
    return nullptr;
  }

  /* Stage 1: NDB scan-filter wire format -> normalized Program. The
   * scan-filter bridge admits only the embedded attr-NULL-branch subset
   * (BRANCH_ATTR_*_NULL / EXIT_OK / EXIT_REFUSE); linked ops and anything
   * else return != JIT_BRIDGE_OK and we leave the scan on the interpreter.
   * reject_code captures the program's EXIT_REFUSE code so the caller can
   * TUPKEY_abort with the program's actual code, not a hardcoded one. */
  Program p;
  JitBridgeError berr;
  Uint32 reject_code = 0;
  JitBridgeReason brc = ndb_jit_bridge_translate_scan_filter(
      filter_prog, n_words, &p, &berr, &reject_code);
  if (brc != JIT_BRIDGE_OK) {
    return nullptr;
  }

  /* Stage 2: NDB-agnostic compile into the per-DBTUP arena. */
  Jit1Prog *jp = jit1_compile(arena, &p, /*timing=*/nullptr);
  if (jp == nullptr) {
    return nullptr;
  }
  if (out_reject_code != nullptr) {
    *out_reject_code = reject_code;
  }
  return reinterpret_cast<void *>(jit1_entry(jp));
}

bool dbtup_jit_invoke_scan_filter(Dbtup *block_tup,
                                  Dbtup::KeyReqStruct *req_struct,
                                  JitEntry             entry_fn,
                                  const Uint32        *prog_buf) {
  /* Per-row context: no aggregation instance. The cold-call helpers
   * (ndb_jit_h_load_col / ndb_jit_h_branch_attr_null /
   * ndb_jit_h_branch_attr_op_arg) reach the row through
   * block_tup->readSingleAttributeForJit, so agg / join_agg stay null.
   * prog_buf is the exec-region base so the OP_ARG helper can read the
   * instruction + its inline literal by offset. */
  dbtup_jit_call_ctx ctx;
  ctx.agg        = nullptr;
  ctx.join_agg   = nullptr;
  ctx.block_tup  = block_tup;
  ctx.req_struct = req_struct;
  ctx.prog_buf   = prog_buf;
#ifdef ERROR_INSERT
  ctx.trace_enabled = false;   /* 4063 row trace is aggregation-only for now */
  ctx.trace_row_no  = 0;
  ctx.trace_limit   = 0;
#endif

  JitState s;
  std::memset(&s, 0, sizeof(s));
  s.ctx = &ctx;

  entry_fn(&s);

  /* A scan filter keeps the row unless OP_FILTER_REJECT_EXIT set the
   * reject flag. The admitted NULL-branch subset performs no
   * arithmetic, so row_overflowed cannot legitimately be set here;
   * treat any unexpected overflow defensively as "reject" so a
   * miscompiled program can never leak a row past the filter. */
  if (s.row_overflowed != 0) {
    return false;
  }
  return s.row_filter_rejected == 0;
}
