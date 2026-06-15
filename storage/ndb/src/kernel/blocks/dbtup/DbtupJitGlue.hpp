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

/*
 * RONDB-1056 Phase 4 — DBTUP/JIT C++ glue.
 *
 * Bridges between NDB's C++ interpreter context (Dbtup,
 * KeyReqStruct, AggInterpreter / JoinAggInterpreter) and the JIT
 * engine's pure-C JitState. Two responsibilities:
 *
 *   1. Cold-call helpers (ndb_jit_h_*) — extern "C" functions
 *      called from JIT-compiled code via HK_COLDCALL holes. The
 *      helper consults JitState.ctx (a dbtup_jit_call_ctx*) to
 *      find the NDB-side context, performs the operation that's
 *      too complex to inline as a stencil (e.g., readAttributes
 *      for kOpLoadCol), and writes the result back into JitState.
 *
 *   2. Per-row dispatch glue — invoked from
 *      JoinAggInterpreter::ProcessRec when a JIT'd entry is
 *      cached. Sets up JitState, copies registers/accumulators,
 *      calls the entry, writes accumulators back.
 *
 *   3. Helper registration — dbtup_jit_register_helpers() is
 *      called once at engine init from Dbtup's constructor or
 *      block-init path. Registers every ndb_jit_h_* function
 *      with jit1_register_helper so the engine can resolve
 *      cold-call holes at compile time.
 */

#ifndef DBTUP_JIT_GLUE_HPP_
#define DBTUP_JIT_GLUE_HPP_

#include "ndb_types.h"
#include "Dbtup.hpp"
#include "ndbapi/NdbAggregationCommon.hpp"   /* for AggResItem */

class AggInterpreterBase;
class JoinAggInterpreter;

extern "C" {
#include "jit/jit1.h"
}

/* Per-call context passed to cold-call helpers via JitState.ctx.
 * The dispatch glue (dbtup_jit_invoke_load_col_path) populates
 * one of these on the stack per row, points JitState.ctx at it,
 * then calls the JIT entry. Helpers cast s->ctx back to this
 * struct.
 *
 * Lifetime: stack-local in the dispatch function. Helpers must
 * not retain pointers into the ctx beyond the helper's return —
 * which they don't, since helpers return immediately. */
struct dbtup_jit_call_ctx {
  AggInterpreterBase *agg;          /* the interpreter instance */
  JoinAggInterpreter *join_agg;     /* non-null only for join-only helpers */
  Dbtup              *block_tup;    /* DBTUP block context for readAttributes */
  Dbtup::KeyReqStruct *req_struct;  /* row position / linked-attr context */
  /* Phase 7: program buffer base for the scan-filter
   * BRANCH_ATTR_OP_ARG helper, which reads the instruction (and its inline
   * literal) by word offset. Points at the exec region passed to
   * interpreterStartLab (&cinBuffer[RinstructionCounter]); the offset the
   * helper adds is the bridge's emb_pc within that region. nullptr on the
   * aggregation path (which does not emit OP_BRANCH_ATTR_OP_ARG). */
  const Uint32       *prog_buf;
#ifdef ERROR_INSERT
  bool                trace_enabled;
  Uint32              trace_row_no;
  Uint32              trace_limit;
#endif
};

extern "C" {

/* ndb_jit_h_load_col — kOpLoadCol cold-call helper.
 *
 * Reads a single non-null BIGINT column from the current row via
 * Dbtup::readAttributes, decodes it as int64, and stores the
 * value into JitState.regs_i64[dst_reg].
 *
 * Phase 4 narrow scope: the bridge admits only programs with
 * BIGINT non-null columns, so we don't handle null or
 * type-promotion here. If a runtime mismatch surfaces (column
 * actually NULL, or non-BIGINT), the helper aborts — that's a
 * bug elsewhere (admission, schema, or query-rewrite) that
 * Phase 5 wires proper error propagation for. */
void ndb_jit_h_load_col(JitState *s, uint32_t col_id, uint32_t dst_reg);

/* ndb_jit_h_branch_attr_null — Phase 5.0 cold-call branch helper.
 *
 * Used by op_branch_attr_eq_null (want_null=1) and
 * op_branch_attr_ne_null (want_null=0) — embedded normal-
 * interpreter BRANCH_ATTR_*_NULL opcodes lowered to JIT.
 *
 * Returns 1 to take the branch, 0 to fall through. The caller
 * stencil branches on the int return value via cbz / test rax,
 * rax. */
int ndb_jit_h_branch_attr_null(JitState *s, uint32_t attr_id,
                                 uint32_t want_null);

/* ndb_jit_h_branch_attr_op_arg — Phase 7 cold-call branch helper for
 * BRANCH_ATTR_OP_ARG (WHERE col <op> literal). inst_word_off is the
 * instruction's word offset within ctx->prog_buf. Forwards to
 * Dbtup::evalBranchColLiteralForJit, which reads the column, compares it
 * against the inline literal via the type's NdbSqlUtil comparator, and
 * returns the take-branch decision. Returns 1 to take the branch, 0 to
 * fall through; aborts on a fatal error (read failure / unsupported type
 * or condition — never expected for the admitted integer scope). */
int ndb_jit_h_branch_attr_op_arg(JitState *s, uint32_t inst_word_off);

/* ndb_jit_h_read_linked_to_mem — Phase 5.1a cold-call helper.
 *
 * Populates ctx->block_tup->cheapMemory[0] from the row's linked-
 * attr buffer at the patched position. Delegates to
 * Dbtup::readLinkedToMemBuffer (shared with NDB's READ_LINKED_TO_MEM
 * interpreter handler). Subsequent op_branch_linked_*_null
 * instructions inspect cheapMemory[0]. */
void ndb_jit_h_read_linked_to_mem(JitState *s, uint32_t position);

/* ndb_jit_h_branch_linked_null — Phase 5.1a cold-call branch helper.
 *
 * Returns 1 to take the branch, 0 to fall through. Inspects the
 * AttributeHeader at ctx->block_tup->cheapMemory[0] (populated by
 * a preceding op_load_linked_to_mem). want_null=1 for the eq
 * variant, 0 for ne — both stencils share this one helper. */
int ndb_jit_h_branch_linked_null(JitState *s, uint32_t want_null);

/* Register every Phase 4 cold-call helper with the JIT engine.
 * Call once at engine init (Dbtup ctor / block-init path).
 * Idempotent — re-registering the same fn is a no-op. */
void dbtup_jit_register_helpers(void);

} /* extern "C" */

/* Per-row dispatch entry — invoked from AggInterpreter::ProcessRec or
 * JoinAggInterpreter::ProcessRec when m_jit_entry != nullptr AND
 * m_n_gb_cols == 0. Returns 0 on success; the existing ProcessRec
 * error codes otherwise.
 *
 * Sets up JitState with the cold-call ctx, copies accumulators
 * into JitState.acc_i64, calls the JIT entry, and writes the
 * (possibly updated) accumulators back into agg_res_ptr. */
Int32 dbtup_jit_invoke(AggInterpreterBase *agg,
                       Dbtup *block_tup,
                       Dbtup::KeyReqStruct *req_struct,
                       JitEntry            entry_fn,
                       AggResItem         *agg_res_ptr,
                       Uint32              n_agg_results,
                       JoinAggInterpreter *join_agg = nullptr);

/* RONDB-1056 Phase 7 — SCAN_FRAGREQ scan-filter compile + invoke.
 *
 * dbtup_jit_compile_scan_filter translates a scan-filter interpreter
 * program (NDB wire format — the RexecRegionLen region of a scan's
 * interpreted program) via ndb_jit_bridge_translate_scan_filter and
 * compiles it with jit1_compile. Returns the JIT entry as a void*
 * (the caller casts to JitEntry), or nullptr if the program is not
 * JIT-eligible or compilation failed — in which case the caller runs
 * the interpreter (interpreterNextLab) as before. Called once per
 * prepared scan program at scan setup.
 *
 * out_reject_code (nullable) receives the program's EXIT_REFUSE code so
 * the per-row path can TUPKEY_abort with the program's actual code
 * instead of a hardcoded one (matching the interpreter). Set to 0 on any
 * failure/early return, or for a filter with no reject path. */
void *dbtup_jit_compile_scan_filter(NdbJitArena *arena,
                                    const Uint32 *filter_prog,
                                    Uint32        n_words,
                                    Uint32       *out_reject_code);

/* dbtup_jit_invoke_scan_filter runs a compiled scan filter against the
 * current row. Returns true to keep the row, false to reject it. The
 * caller maps a rejected row to TUPKEY_abort with
 * ZUSER_SEARCH_CONDITION_FALSE_CODE, matching the interpreter's
 * EXIT_REFUSE disposition for scans. No aggregation accumulators are
 * involved; ctx.agg / ctx.join_agg stay null and column reads go
 * through Dbtup::readSingleAttributeForJit. */
bool dbtup_jit_invoke_scan_filter(Dbtup *block_tup,
                                  Dbtup::KeyReqStruct *req_struct,
                                  JitEntry             entry_fn,
                                  const Uint32        *prog_buf);

#endif /* DBTUP_JIT_GLUE_HPP_ */
