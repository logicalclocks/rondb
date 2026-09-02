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
  /* Phase 7: subroutine/param region base, for BRANCH_ATTR_OP_PARAM
   * (WHERE col <op> ?) where the parameter value lives. Passed to
   * lookupInterpreterParameter by evalBranchColForJit. nullptr when the
   * scan has no param region (or on the aggregation path). */
  const Uint32       *param_buf;
  /* Phase 5F-1: the row's aggregate-result array (the same pointer
   * dbtup_jit_invoke writes back through) — the fused string MIN/MAX
   * helper hands it to AggInterpreterBase::jitMinMaxStringCol, whose
   * kernel mutates the string slots DIRECTLY (those slots never set
   * value_updated, so the masked writeback leaves them alone).
   * nullptr on the scan-filter path. */
  AggResItem         *agg_res_ptr;
  /* ronsql_jit item 12: a cold-call helper whose kernel eval returned
   * the interpreter's negative error (e.g. -40 "no comparator for this
   * condition / type") records -(rc) here and raises row_fallback
   * instead of aborting the node. Aggregation paths replay the row on
   * the interpreter, which surfaces the same error; the scan-filter
   * invoke hands the code back to interpreterStartLab, which
   * TUPKEY_aborts with it — exactly interpreterNextLab's disposition.
   * 0 = no error. */
  int                 error_code;
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
 * Dbtup::evalBranchColForJit, which reads the column, compares it
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

/* Per-row dispatch entry — invoked from AggInterpreter::ProcessRec
 * (gate: m_jit_entry != nullptr) or JoinAggInterpreter::ProcessRec
 * (gate: m_jit_entry != nullptr && !m_null_local_columns — outer-join
 * NULL-extended rows stay on the interpreter). GROUP BY programs run
 * the JIT too since the Phase 8 gate lift: the caller's group
 * prologue resolves agg_res_ptr to the row's group slots before each
 * dispatch. Returns 0 on success; the existing ProcessRec
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

/* RONDB-1056 Phase 7/8 — SCAN_FRAGREQ scan-filter compile + invoke.
 *
 * dbtup_jit_compile_scan_filter acquires the compiled form of a
 * scan-filter interpreter program (NDB wire format — the RexecRegionLen
 * region) from the node-global program-reuse cache, keyed on the exact
 * bytecode words: identical filters share one compiled blob. On a miss
 * the cache translates via ndb_jit_bridge_translate_scan_filter and
 * compiles with jit1_compile into the code-memory manager. Returns the
 * JIT entry as a void* (the caller casts to JitEntry), or nullptr if the
 * program is not JIT-eligible / compilation failed / OOM — in which case
 * the caller runs the interpreter (interpreterNextLab) as before. Called
 * once per stored procedure at scan setup.
 *
 * out_reject_code (nullable) receives the program's EXIT_REFUSE code so
 * the per-row path can TUPKEY_abort with the program's actual code
 * instead of a hardcoded one (matching the interpreter). Set to 0 on any
 * failure/early return, or for a filter with no reject path.
 *
 * out_cache_handle (nullable) receives the opaque cache handle (NjpEntry*)
 * the caller must pass to dbtup_jit_release_scan_filter when the stored
 * procedure is freed. Set to nullptr on failure. */
void *dbtup_jit_compile_scan_filter(const Uint32 *filter_prog,
                                    Uint32        n_words,
                                    Uint32       *out_reject_code,
                                    void        **out_cache_handle);

/* Release a scan-filter program acquired by dbtup_jit_compile_scan_filter.
 * Drops the program's reuse-cache refcount; the compiled blob and its
 * code-memory slot are freed when the last holder releases. Safe with
 * nullptr. */
void dbtup_jit_release_scan_filter(void *cache_handle);

/* RONDB-1056 Phase 8 — standalone aggregation program reuse.
 *
 * dbtup_jit_compile_agg acquires the compiled form of a per-row
 * aggregation program (NDB wire format) from the node-global agg
 * reuse cache, keyed on the exact bytecode words: identical scalar
 * aggregations across scans share one compiled blob. On a miss the
 * cache translates via ndb_jit_bridge_translate and compiles with
 * jit1_compile into the code-memory manager. Returns the JIT entry as a
 * void* (cast to JitEntry), or nullptr if not JIT-eligible / OOM.
 * out_cache_handle (nullable) receives the handle to pass to
 * dbtup_jit_release_agg when the interpreter is torn down. Serves
 * scalar and GROUP BY programs alike since the Phase 8 gate lift (the
 * dispatcher resolves per-group accumulator slots before each
 * invocation). `pinned` (from the program's
 * AGG_PROG_FLAG_REUSABLE header bit — RonSQL / prepared statements)
 * keeps the entry cached at refcount 0 so a re-sent identical program
 * hits instead of recompiling (Phase 8 Slice 4); sticky on an existing
 * entry, never downgraded. */
void *dbtup_jit_compile_agg(const Uint32 *agg_prog, Uint32 n_words,
                            void **out_cache_handle, bool pinned);

/* Release an agg program acquired by dbtup_jit_compile_agg. Drops the
 * reuse-cache refcount; the blob + code-memory slot are freed when the
 * last holder releases. Safe with nullptr. */
void dbtup_jit_release_agg(void *cache_handle);

/* RONDB-1056 Phase 8 — node-global JIT statistics for NDBINFO.
 * A snapshot of the (node-global) code-memory manager + both program
 * reuse caches + the execution/compile counters; identical from any
 * block instance on the node. */
struct NdbJitStats {
  Uint64 code_reserved_bytes;  // executable memory mmap'd by the manager
  Uint64 code_used_bytes;      // memory held by live compiled-program slots
  Uint64 programs_compiled;    // total compiles (scan-filter + agg misses)
  Uint64 programs_reused;      // total reuse-cache hits (compiles avoided)
  Uint64 programs_fallback;    // compile attempts that produced no program
  Uint64 rows_executed;        // rows run through JIT entry points
  Uint64 compile_ns_total;     // total ns spent in jit1_compile
  Uint32 code_slots_live;      // live code-memory slots
  Uint32 programs_cached;      // compiled programs currently cached
};

/* Fill *out with a node-global JIT stats snapshot. Cheap (takes the
 * managers' locks briefly); for NDBINFO reporting, not the per-row path. */
void dbtup_jit_get_stats(NdbJitStats *out);

/* RONDB-1056 Phase 8 — crash diagnosis glue.
 *
 * dbtup_jit_install_crash_handler installs a process-wide SA_SIGINFO
 * interposer for SIGSEGV/SIGBUS/SIGILL/SIGFPE that maps a faulting PC
 * through jit1_describe_pc, logs the "JIT-CRASH:" line, and chains to
 * the handler ndbd installed at startup (so the normal ErrorReporter
 * crash path is unchanged). Idempotent (pthread_once); called lazily
 * from every JIT compile site, so a node that never compiles — or runs
 * CompiledInterpreter=OFF — never touches signal handling. No-op on
 * Windows. */
void dbtup_jit_install_crash_handler();

/* Log one "JIT-DUMP:" line per live JIT program (blob address range,
 * size, op kinds) plus a count line via g_eventLogger. Takes the
 * registry mutex — for DUMP diagnostics (TupDumpJitPrograms), not the
 * per-row path or signal handlers. */
void dbtup_jit_dump_programs();

/* RONDB-1056 Phase 8 — fallback / compile-time accounting.
 *
 * dbtup_jit_note_fallback records one "compile attempt produced no JIT
 * program" event (bumps the node-global programs_fallback counter) and
 * rate-limit-logs it via g_eventLogger — at most one line per period,
 * because implicit scans (e.g. the EXIT_OK_LAST table-stats scan) are
 * deliberate fallbacks that recur on every occurrence. `path` names
 * the compile site + stage (e.g. "scan-filter bridge"); reason/detail
 * are the site's reject enums, logged as numbers.
 *
 * dbtup_jit_note_compile_ns adds one successful compile's duration
 * (Jit1Timing.total_ns) to the node-global compile_ns_total counter.
 *
 * Both are compile-frequency paths (never per-row); DblqhProxy's
 * inline join-agg compile site uses them too. */
void dbtup_jit_note_fallback(const char *path, int reason, Uint32 detail,
                             Uint32 word, const Uint32 *prog,
                             Uint32 prog_len);
void dbtup_jit_note_compile_ns(Uint64 ns);

/* RONDB-1056 Phase 8 — CompiledInterpreter config gate (node-global).
 * dbtup_jit_set_mode is called once at config read with the
 * NDB_COMPILED_INTERPRETER_* value; every compile site consults
 * dbtup_jit_enabled() and produces no JIT program when the mode is OFF
 * (so the program runs on the interpreter). Default is enabled (AUTO). */
void dbtup_jit_set_mode(Uint32 mode);
bool dbtup_jit_enabled();

/* Sentinel return of dbtup_jit_invoke: the row hit a condition the JIT
 * cannot represent (JitState::row_fallback — e.g. a NULL column value).
 * The JIT run was discarded (no accumulator writeback); the caller must
 * re-run THIS ROW through the interpreter loop, which reproduces the
 * interpreter's exact semantics. Negative and far outside the ZAGG_*
 * error space so it can never collide with a real error code. */
constexpr Int32 NDB_JIT_ROW_FALLBACK = -7157;

/* dbtup_jit_invoke_scan_filter runs a compiled scan filter against the
 * current row. Returns true to keep the row, false to reject it. The
 * caller maps a rejected row to TUPKEY_abort with the program's own
 * refuse code, matching the interpreter's EXIT_REFUSE disposition for
 * scans. *out_error is 0 on a verdict; non-zero (ronsql_jit item 12)
 * when a helper's kernel eval failed with that error code — then the
 * verdict is meaningless and the caller must TUPKEY_abort(*out_error),
 * exactly as interpreterNextLab does for a negative handler return. No
 * aggregation accumulators are involved; ctx.agg / ctx.join_agg stay
 * null and column reads go through Dbtup::readSingleAttributeForJit. */
bool dbtup_jit_invoke_scan_filter(Dbtup *block_tup,
                                  Dbtup::KeyReqStruct *req_struct,
                                  JitEntry             entry_fn,
                                  const Uint32        *prog_buf,
                                  const Uint32        *param_buf,
                                  int                 *out_error);

#endif /* DBTUP_JIT_GLUE_HPP_ */
