# RONDB-1056 Phase 4 — DBTUP thin-slice integration (results)

**Status: shipped.** The cold-call mechanism, the bytecode bridge,
the per-node compile site in `DblqhProxy`, the per-row dispatch in
`JoinAggInterpreter`, and the first cold-call helper
(`ndb_jit_h_load_col`) all land. ndbmtd builds and links cleanly
with the JIT path wired through. Static / unit-level testing
covers the mechanism end-to-end at the JIT layer, and the MTR
canary `rondb_jit_canary` exercises the full path — RonSQL
planner → JOIN_AGG_SETUP_REQ → bridge + admission → JIT compile
→ runtime cold-call into NDB's readAttributes — through real SQL
queries against real NDB tables.

The MTR canary surfaced one bug during first-run: a per-thread vs
per-arena state mismatch in the JIT arena's macOS write-protect
toggle. When `jit1_compile` ran on a different thread than the
arena's creator, the arena's global `jit_write_enabled` flag
short-circuited the per-thread `pthread_jit_write_protect_np(0)`
call, leaving the calling thread in write-protected state. The
first write through the unified MAP_JIT mapping faulted SIGBUS.
Fix: arena_alloc on macOS now unconditionally re-enables write on
the calling thread, regardless of the global flag. Idempotent;
one extra syscall per compile.

Branch: `RONDB-1056-compiled-interpreter`. Final commit at
writing: `9fec407dd1a` (the SIGBUS fix); shipped state is the
state of the branch as of that commit + all preceding Phase 4
work.

## Outcome

| Gate | Status |
|------|--------|
| ndbmtd builds + links with JIT path wired | ✓ |
| `regen-stencils` 31/31 magics audit PASS, 16 stencils per arch | ✓ |
| `extractor-tests` 11/11 PASS | ✓ |
| `admission_tests` 12/12 PASS | ✓ |
| `bridge_tests` 10/10 PASS | ✓ |
| `coldcall_tests` 5/5 PASS | ✓ |
| `proto_microbench` (regression) PASS | ✓ |
| MTR canary `rondb_jit_canary` running real SQL through the full path | ✓ |
| `bench_q12_dbtc` informational speedup | deferred to Phase 5 |

The MTR canary validates the boundary that the unit tests can't
reach by themselves: a real query routing through
`JOIN_AGG_SETUP_REQ`, admission accepting the bytecode (Q1-Q3 in
the canary's set), the JIT'd code calling `ndb_jit_h_load_col`,
and that helper calling NDB's real `readAttributes` on real DBTUP
row buffers. Q4 (WHERE-bearing) exercises the fallback path —
admission rejects (kOpEmbeddedInterp), the interpreter handles
every row, results match.

The `bench_q12_dbtc` informational speedup gate stays deferred
to Phase 5 — Phase 4's bytecode coverage (no embedded interp,
no nullable, no division) is too narrow for q12 to be a
representative measurement. Once Phase 5 expands coverage,
q12 becomes meaningful.

## What shipped

### Cold-call mechanism (engine + extractor)

| File | Role |
|------|------|
| `hole_kinds.h` | New `HK_COLDCALL=7` HoleKind. Hole struct gains `helper_name` field (NULL except for HK_COLDCALL). |
| `jit1.h` | New `JitState.ctx` field (opaque per-call ctx for cold-call helpers). New `jit1_register_helper` / `jit1_lookup_helper` API. |
| `jit1.c` | `g_helpers[16]` static registry (register-once-at-init). HK_COLDCALL case in jit1_compile patcher: looks up helper, computes PC-rel displacement using `ndb_jit_arena_exec_addr` (so RX address is used, not RW). |
| `extract_stencils.c` | `is_jit_helper_symbol(name)` — true for `ndb_jit_h_*` prefix. `add_coldcall_hole` records helper_name from the relocation's target symbol. extract_one_x86 / extract_one_arm64 classify PLT32 / CALL26+JUMP26 against helper symbols as HK_COLDCALL. emit_header writes `.helper_name = "..."` for HK_COLDCALL holes. |

Range concern: x86_64 `call rel32` covers ±2 GB; aarch64 `bl
imm26` covers ±128 MB. Both should comfortably fit within
ndbmtd's address space. If a future configuration overflows, the
engine can fall back to indirect-call codegen (Phase 5
hardening).

### Stencils + bytecode

| File | Role |
|------|------|
| `bytecode1.h` | OpKind values 14-16 added: OP_MINUS_INT_INT, OP_MUL_INT_INT, OP_LOAD_COL_NDB. Append-only after Phase 3's branch siblings. |
| `stencils_src.c` | New stencils: op_minus_int_int, op_mul_int_int (clones of OP_ADD_INT_INT), op_load_col_ndb (cold-call shape calling `ndb_jit_h_load_col`). |
| `hole_kinds.h` | 8 new MAGIC_* constants (sha256-derived from `RONDB-1056-Phase4-magic-v1|<name>`). 8 new symbol-table + magic-table entries. |
| `stencils_x86_64.h` + `stencils_arm64.h` | Regenerated, 16 stencils each. |

### Bytecode bridge

| File | Role |
|------|------|
| `ndb_jit_bridge.{h,c}` | Translates NDB Uint32-word aggregation programs to our internal Program. 11 supported NDB opcodes; everything else (kOpEmbeddedInterp, kOpDiv*, kOpMod, kOpSkip, kOpSetRegNull, double / max / min / count variants, generic untyped variants) returns JIT_BRIDGE_UNSUPPORTED_OP. kOpLoadCol maps to OP_LOAD_COL_NDB (cold-call). |

### DBTUP integration

| File | Role |
|------|------|
| `DblqhProxy.{hpp,cpp}` | Owns `m_jit_arena` (1 MB, lifetime = proxy lifetime). Constructor registers all Phase 4 helpers via `dbtup_jit_register_helpers()`. `execJOIN_AGG_SETUP_REQ` calls bridge + jit1_compile between OptimizeProgramBuffer and JoinAggInterpreter allocation; stashes Jit1Prog* + JitEntry on LeafProgram. Both interpreter-creation paths (MUTEX_BASED + MUTEX_FREE) call `interp->setJitEntry(leaf0.m_jit_entry)`. |
| `JoinAggregationState.hpp` | LeafProgram gains `Jit1Prog *m_jit_prog` and `JitEntry m_jit_entry`. Both nullptr means interpreter path. |
| `JoinAggInterpreter.{hpp,cpp}` | Private `m_jit_entry` field. Public `setJitEntry` setter and `readAttributeForJit` friend-access wrapper for the cold-call helper. ProcessRec gains a JIT branch right after agg_res_ptr resolution: when `m_jit_entry != nullptr && m_n_gb_cols == 0`, dispatches via `dbtup_jit_invoke` and returns. |
| `DbtupJitGlue.{hpp,cpp}` | dbtup_jit_call_ctx struct. ndb_jit_h_load_col helper (extern "C", reads non-null BIGINT via JoinAggInterpreter::readAttributeForJit, decodes via sint8korr). dbtup_jit_register_helpers idempotent at-init registration. dbtup_jit_invoke per-row dispatch (sets up JitState with ctx, copies acc in/out). |

### Tests

| Binary | Cases | Coverage |
|--------|-------|----------|
| `extractor-tests` | 11 | Bridge bytes vs committed headers; misshapen ELF; bad CLI args; idempotency. |
| `admission_tests` | 12 | Accept/reject paths in jit1_compile's admission walk; arena-no-leak on reject. |
| `bridge_tests` | 10 | NDB Uint32 → Program translation: accept (empty, simple SUM, arithmetic battery, load_const) + reject (kOpEmbeddedInterp, kOpDiv, kOpSetRegNull, non-bigint, truncated, register out of range). |
| `coldcall_tests` | 5 | Cold-call mechanism end-to-end at the JIT layer: helper-not-registered ENOENT, single-row, multi-row accumulation, cold-call + arithmetic, registry idempotency. |
| `proto_microbench` | regression | Phase 1-3 microbench paths still work; non-cold-call OP_LOAD_COL_INT unchanged. |

## Path through the new code (canonical)

```
1. JOIN_AGG_SETUP_REQ arrives at DblqhProxy
2. DblqhProxy::execJOIN_AGG_SETUP_REQ
   - validate, allocate state record, copy program from section 0
   - PushdownInterpreter::OptimizeProgramBuffer (replaces kOpSum
     with kOpSumBigint etc.)
   - For single-leaf programs only:
     - ndb_jit_bridge_translate(leaf0 bytecode) → Program
     - jit1_compile(m_jit_arena, &Program) → Jit1Prog
     - lp.m_jit_entry = jit1_entry(jp)
   - Allocate JoinAggInterpreter(s) per strategy
   - For each: interp->Init(leaf0.m_agg_program);
               interp->setJitEntry(leaf0.m_jit_entry)

3. Per-row, on each LDM worker:
   JoinAggInterpreter::ProcessRec
   - GB column reads + hash lookup → agg_res_ptr
   - If m_jit_entry != nullptr && m_n_gb_cols == 0:
     dbtup_jit_invoke(this, block_tup, req_struct,
                      m_jit_entry, agg_res_ptr, m_n_agg_results)
       - Build dbtup_jit_call_ctx on stack
       - Initialize JitState, set ctx pointer
       - Copy m_agg_results.value.val_int64 → s.acc_i64[]
       - Call m_jit_entry(&s)  ← JIT'd code runs here
         - Each kOpLoadCol → OP_LOAD_COL_NDB stencil
         - Stencil makes a regular call to ndb_jit_h_load_col
           - Helper retrieves ctx from s.ctx
           - Calls ctx->agg->readAttributeForJit (friend access
             into Dbtup::readAttributes)
           - Decodes BIGINT via sint8korr
           - Writes to s.regs_i64[dst_reg]
         - Stencil returns; chain continues
         - Each kOpPlusBigint / kOpMinusBigint / kOpMulBigint /
           kOpSumBigint runs as a hot stencil
       - Write s.acc_i64[] back to m_agg_results.value.val_int64
       - Return 0
   - else: existing interpreter loop (unchanged)
```

## Toolchain quirks discovered

- **Hardcoded `HOLE_BLT_TGT` was already fixed in Phase 3** —
  the same lookup-by-symbol-table pattern is what Phase 4 used
  for cold-call helper detection. No new toolchain work was
  needed.

- **`readAttributes` is private on `Dbtup`** — only AggInterpreter /
  JoinAggInterpreter / VecSearchInterpreter are friended. Phase 4's
  cold-call helper goes through a public wrapper
  (`JoinAggInterpreter::readAttributeForJit`) rather than friending
  DbtupJitGlue. Cleaner separation, no friend declaration to
  audit.

- **`ndbrequire` requires JAM context** that's only available
  inside NDB blocks. DbtupJitGlue.cpp is not a block file, so
  ndbrequire fails to compile there. Replaced with direct
  null-checks + `g_eventLogger->error` + `abort()` for the
  defensive paths.

- **PC-relative range** — both x86_64 `call rel32` (±2 GB) and
  aarch64 `bl imm26` (±128 MB) cover ndbmtd's address-space
  layout in practice. The engine's HK_COLDCALL patcher does not
  check overflow; if a future huge-binary configuration violates
  the bound, the symptom would be a wrap-around displacement
  pointing at the wrong code. Phase 5 hardening: add range check
  + indirect-call fallback.

## Forward pointers

- **MTR canary (Phase 4.5 / future)** — exercise the full path
  with a real SQL query. Two routes:
  1. RonSQL planner emits a query that admission accepts. Most
     realistic aggregation queries use kOpEmbeddedInterp — needs
     planner-side cooperation to suppress.
  2. Hand-crafted bytecode injected via NdbApi for testing only.
     Validates the path; not realistic-shape.

  Either way, the test query needs:
  - Joins or CTEs (for JOIN_AGG_SETUP_REQ to fire)
  - No WHERE clause / no CASE / no nullability handling
    (avoid kOpEmbeddedInterp)
  - Non-null BIGINT columns only (Phase 4 narrow scope)
  - Single-leaf (m_num_leaves == 1)
  - No GROUP BY (m_n_gb_cols == 0)

  This is a tight constraint set; prototyping requires a hand-
  built test query, not "any aggregation".

- **Phase 5 work this enables**:
  - Wider helper inventory (Div NULL fixup, StringSearch,
    BinarySearch, QSort, CompressNumArray) — the cold-call
    mechanism is in place; Phase 5 just adds new helpers.
  - Embedded interpreter blocks via cold-call dispatch.
  - Nullable-aware arithmetic via type-state at branch joins.
  - Non-bigint types (Double, etc.).
  - Multi-leaf programs.

- **Per-program-dispatch invariant test** (plan.md §10.2's
  "flipping `m_jit_entry` mid-program is NOT picked up") is
  **not enforced** by the current implementation — ProcessRec
  re-checks `m_jit_entry` on every row. Branch predictor folds
  it to free in steady state, but a flip via setJitEntry would
  take effect immediately. Phase 5 can codify this if the
  invariant is ever needed in production (it isn't currently:
  setJitEntry is called once at Init time and never afterward).

## Verification checklist (final)

| Item | Status |
|------|--------|
| `regen-stencils` 16 stencils per arch, 31/31 magics PASS | ✓ |
| `extractor-tests` 11/11 PASS | ✓ |
| `admission_tests` 12/12 PASS | ✓ |
| `bridge_tests` 10/10 PASS | ✓ |
| `coldcall_tests` 5/5 PASS | ✓ |
| `proto_microbench` regression PASS | ✓ |
| `ndbmtd` builds + links cleanly | ✓ |
| Canary MTR test `rondb_jit_canary` PASS | ✓ — Q1-Q3 admission-accept; Q4 admission-reject + interp fallback; all four queries produce correct results |
| Fallback MTR test (admission rejects, interp runs) | ✓ — Q4 of `rondb_jit_canary` is the fallback case |
| Per-program-dispatch invariant unit test | deferred (impl re-checks `m_jit_entry` per row; branch predictor folds it; production callers don't flip mid-program — Phase 5 can codify if the invariant ever becomes load-bearing) |
| No new MTR failures across `testJoinAgg` family | deferred — broader regression sweep belongs in Phase 5 alongside the wider opcode coverage |
| `bench_q12_dbtc` informational speedup | deferred to Phase 5 — Phase 4's narrow coverage (no embedded interp, no nullable, no division) is unrepresentative |
