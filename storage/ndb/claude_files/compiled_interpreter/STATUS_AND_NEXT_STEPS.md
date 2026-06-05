# RONDB-1056 Compiled Interpreter — Status & Next Steps

**Updated: 2026-06-05.** Single entry point for resuming work. Branch:
`RONDB-1056-compiled-interpreter`.

> ⚠️ **Docs-vs-reality note.** `plan.md`'s header still says
> *"Status: planning. Code: not started."* That is stale. The JIT is
> implemented and wired end-to-end (~34 real code commits across
> Phases 0–5.0, plus Phase 5.1a). Treat `plan.md` / `phase_*.md` as
> *design intent*; treat the source tree as ground truth.

## Merge verification — RONDB-1066 AggInterpreter refactor (2026-06-05)

The `RONDB-1066-refactor` (PR #953) unified `AggInterpreter` and
`JoinAggInterpreter` under a shared base **`AggInterpreterBase`**. State and
shared kernels were lifted into the base: **`m_jit_entry`**
(`AggInterpreterBase.hpp:405`), `m_n_gb_cols` (~470), `m_prog`,
`executeStandardOpcode`, `validateEmbeddedProgram` /
`scanAndValidateEmbeddedPrograms`, `loadColumnTypedFromBuf`. `ProcessRec`
stays **per-subclass and non-virtual**; the JIT per-row dispatch
(`if (m_jit_entry != nullptr && m_n_gb_cols == 0) dbtup_jit_invoke(...)`)
still lives **only** in `JoinAggInterpreter::ProcessRec` — now
~`JoinAggInterpreter.cpp:484-500` (was ~1116; ProcessRec was compressed by
the refactor, dispatch happens before the interpreter loop and returns).
`m_linked_attr_data` / `m_linked_attr_len` stayed in `JoinAggInterpreter`
(`.hpp:287-288`).

**Reconciliation edits (UNCOMMITTED in the working tree — commit these):**
- `AggInterpreterBase.hpp` (+7): moved the `struct JitState; typedef void
  (*JitEntry)(JitState*);` forward-decl up to the base header (because
  `m_jit_entry` now lives there).
- `JoinAggInterpreter.hpp` (−6): removed that same forward-decl from the
  subclass header.
- `JoinAggInterpreter.cpp` (−2): removed an orphaned `Uint32 col_index;`
  local (zero remaining uses after the refactor) + a trailing blank line.

These are the minimal, correct adaptation of the JIT hook to the field
lift — no behavior change. All six JIT integration surfaces were
statically re-verified intact (dispatch site, lifted members in scope via
inheritance, `JitState.value_updated[]` writeback mask at
`DbtupJitGlue.cpp:342-349`, `dbtup_jit_invoke`/`ndb_jit_h_*` signatures,
allow-list + `s_agg_interp_handlers[19/41/42]`, DblqhProxy compile path).

**Verification status of the merge:**
- **Build: GREEN.** `ndbmtd` relinked 15:57 and `JoinAggInterpreter.cpp.o`
  15:56 from the reconciled source — the refactored kernel + JIT
  integration compiles and links cleanly (a broken member-move would have
  failed to compile). This is the most important gate and it has passed.
- **Host JIT unit layer: GREEN.** `bridge_tests` 36/36, `admission_tests`
  16/16, `coldcall_tests` 7/7, `proto_interp_only` PASS. NB these exercise
  the JIT engine/bridge **in isolation** — they do NOT link the refactored
  kernel classes, so they confirm "JIT engine unbroken" but NOT the
  integration with the unified interpreter.
- **Data-node layer: NOT YET RUN — this is the real merge gate.** Only the
  live-cluster tests exercise the refactored `ProcessRec` + JIT dispatch AND
  the unified interpreter path (the class merge changed the *interpreter*,
  not just JIT, so the regression risk is broader than the JIT canaries).
  Recommended run (operator to execute), from `debug_build/mysql-test`:
  ```sh
  ./mtr --suite=ndb_push_agg --force --nowarnings \
    rondb_jit_canary rondb_jit_embedded_canary rondb_jit_must_compile \
    rondb_jit_ndbapi_must_compile rondb_jit_ndbapi_null_sum \
    rondb_jit_ndbapi_linked_null \
    testJoinAggNdbApi testInterpreterTypedRegs testVarcharMinMax testCaseAgg \
    testJoinAgg testJoinAggSpj testStarJoinAgg testMultiOuterJoinAggNdbApi \
    ndb_join_pushdown_agg ndb_join_pushdown_agg_linked
  ```
  Rationale for the non-canary picks: `testInterpreterTypedRegs` →
  refactored `loadColumnTypedFromBuf`; `testVarcharMinMax` → shared string
  MIN/MAX helpers lifted to the base; `testCaseAgg` → CASE/embedded path;
  the `testJoinAgg*` / `testStarJoinAgg*` / `ndb_join_pushdown_agg*` family
  → the unified interpreter dispatch end-to-end.

**Cleanup item (low priority, not a JIT risk):** the comment at
`AggInterpreterBase.hpp:407-410` claims "both static_asserts on subclass
sizeof still hold," but no `static_assert(sizeof(...))` on the subclasses
remains in the tree (removed/relaxed during the refactor). The
placement-new sizing at `DblqhProxy.cpp:~2876` no longer has that
compile-time guard — either re-add the asserts or drop the stale comment.

## Where we actually are

### Implemented and wired (verified in source)

- **Engine (Phases 0–3):** copy-and-patch JIT in
  `storage/ndb/src/kernel/blocks/dbtup/jit/` — `jit1.c/.h`,
  `bytecode1.h`, `jit_arena*`, `ndb_jit_bridge.c/.h`,
  `stencils_{x86_64,arm64}.h`, `stencils_src.c`, `hole_kinds.h`,
  `extract_stencils/` (extractor + audit_magics). Forward-only
  admission walk, dual-mapping W^X arena, two host tools.
- **Data-node integration (Phase 4):** JIT is **already wired** —
  there is no separate `interpreterExec`; the dispatch point is
  `JoinAggInterpreter::ProcessRec` (`JoinAggInterpreter.cpp:~484-500`
  post-RONDB-1066; was ~1116-1121):
  when `m_jit_entry != nullptr && m_n_gb_cols == 0` it calls
  `dbtup_jit_invoke()`. Compile happens at
  `DblqhProxy::execJOIN_AGG_SETUP_REQ` → `ndb_jit_bridge_translate()`
  → `jit1_compile()` → `jit1_entry()` sets `m_jit_entry`. Glue in
  `DbtupJitGlue.{cpp,hpp}` (cold-call helpers + `dbtup_jit_invoke`).
- **Phase 4.5–4.7:** narrow-hole encoding, inline-asm imm constraint,
  addr-mode fold / narrow LoadConst (aarch64).
- **Phase 5.0:** embedded normal-interpreter calls, first slice
  (BRANCH_ATTR_*_NULL via 3-hole cold-call branch pattern).
- **Phase 5.1a (partial):** LINKED_*_NULL + READ_LINKED_TO_MEM
  cold-call helpers exist in `DbtupJitGlue` (`ndb_jit_h_branch_linked_null`,
  `ndb_jit_h_read_linked_to_mem`); embedded filters enabled in the
  bridge (commit `5688274`).
- **`row_accumulated` correctness fix: LANDED.** `value_updated[]` /
  per-aggregate mask added to `JitState` (commit `5c8169e`); writeback
  in `DbtupJitGlue` is now gated so an all-rejected SUM stays SQL NULL
  instead of 0. (See `phase_5_1_row_accumulated.md` for the design;
  confirm the shipped form matches before relying on it.)
- **Diagnostics:** error inserts `4060` (fallback-fatal canary),
  `4061` (dump program+translation), `4062` (setup-compile fatal),
  `4063` (bounded row trace), gated (commit `16feda17b`).
- **Tests/bench (host binaries, run directly — no ctest):**
  `proto_microbench` (Phase 1 PASS: 2.66× warm speedup, 1.82µs warm
  compile, 93-row break-even), `admission_tests`, `bridge_tests`,
  `coldcall_tests`, `proto_interp_only`, under
  `debug_build/storage/ndb/test/jit_proto/`.
- **NDB API canary harness:** `storage/ndb/block_unit_test/testJoinAggNdbApi.cpp`
  (MTR wrapper `mysql-test/suite/ndb_push_agg/t/testJoinAggNdbApi.test`).
  - **Test 23** — "JIT must compile SUM local attr" (`testJitMustCompileSum`,
    line ~807; commit `1a557ed`). Done.
  - **Test 24** — "JIT all-rejected SUM returns NULL"
    (`testJitAllRejectedSumNull`, line ~970); the last 4 commits
    (`5c8169e`, `5688274`, `9c0e87f`, `8102eab`) stabilized it (now run
    as a child aggregation, filter shape fixed). Verify green at HEAD.
    NB: the implemented Test 24 is the *all-rejected → NULL* shape, not
    the mixed-accept SUM=900 shape sketched in the additions doc.
  - **Linked path is further along than the additions doc assumes.**
    Helpers `ndb_jit_h_read_linked_to_mem` + `ndb_jit_h_branch_linked_null`
    are implemented and registered (`DbtupJitGlue.cpp:201,234,271-274`);
    interpreter handlers exist; the bridge admits the shape and
    **`bridge_tests.c` already has T20** (READ_LINKED_TO_MEM +
    BRANCH_LINKED_NE_NULL accept). So the unit-test layer for linked
    NULL is done — the gap is the end-to-end NDB API canary.

### Not done yet

- **Test 25** — linked NULL-branch canary: **✅ DONE & VERIFIED.** Test 25
  (SUM=900) + Test 24 (SUM=NULL) pass; bridge_tests T22b/T22c + Test 23 +
  full binary (Tests 1-25) all green. Uncovered + fixed a 5-layer stack of
  real gaps — the linked-NULL embedded filter was never actually runnable
  through JIT before this:
  1. *1869 at setup* — `validateEmbeddedProgram` allow-list + is_branch
     switch missing opcodes 41/42. Fixed in `JoinAggInterpreter.cpp` +
     `AggInterpreter.cpp`.
  2. *SUM=NULL (all rejected)* — (a) `s_agg_interp_handlers[41]/[42]`
     were `nullptr` so only JIT could run linked-NULL branches; wired to
     `handleBranchLinkedEqNull/NeNull`. (b) **EXIT opcode encoding bug**:
     bridge `BR_EMB_EXIT_OK/REFUSE` were `5/6` but the real `Interpreter`
     enum is `18/19`; Test 24 passed only via a validator decode quirk.
     Fixed bridge → 18/19; updated `bridge_tests.c` + Test 24 to use 19.
  3. *EXIT_REFUSE reject semantics* — `s_agg_interp_handlers[19]` was
     `nullptr` (→1869). Added `handleExitRefuseAgg`: filter codes
     (626/899/6000-6999/0) ⇒ `INTERPRETER_FILTER_REJECT` (skip row), else
     hard error (NdbInterpretedCode convention). `ProcessRec` (JoinAgg +
     Agg) maps that sentinel to skip-row, matching the JIT's OP_EXIT.
  4. *Still SUM=NULL — JIT not populating linked buffer:* the JIT
     dispatch in `ProcessRec` (~line 1121) never copied
     `m_linked_attr_data`/`m_linked_attr_len` into `req_struct`. Fixed:
     set the two fields around `dbtup_jit_invoke`, clear after. (4063
     trace then showed read/branch CORRECT — NULL→is_null=1→reject,
     non-NULL→is_null=0→fall-through.)
  5. *STILL SUM=NULL — the actual final cause: NO ACCEPT PATH.* The
     **embedded-program row-disposition model** (per Mikael): a per-row
     decision MUST terminate explicitly — (1) `EXIT_REFUSE` skip-code →
     skip row, (2) `EXIT_REFUSE` error-code → abort, (3)
     `LOAD_CONST16 skip_offset; WRITE_INTERPRETER_OUTPUT 0; EXIT_OK` →
     **use** the row (slot 0 selects which agg instruction runs next;
     0 = the next one; non-zero implements CASE multi-way aggregation).
     My 3-word block had only the reject path — accepted (non-NULL) rows
     fell into `EXIT_REFUSE` and never reached SUM. There is NO
     "fall off the end = accept"; accept is `WRITE_INTERPRETER_OUTPUT`.
     The JIT **bridge never implemented the accept path** (no
     `LOAD_CONST16`/`WRITE_INTERPRETER_OUTPUT` cases) — Test 25 is the
     first pass-some/reject-some filter forced through JIT (23=no filter,
     24=all-reject), so it was never exercised. Fix:
     - **Block (6 words):** `READ_LINKED_TO_MEM 0;
       BRANCH_LINKED_NE_NULL +2 (→accept @3); EXIT_REFUSE 626;
       LOAD_CONST16 r2,0; WRITE_INTERPRETER_OUTPUT r2,0; EXIT_OK`.
     - **Bridge** (`ndb_jit_bridge.c`): handle `LOAD_CONST16`(4) +
       `WRITE_INTERPRETER_OUTPUT`(123) in embedded blocks as no-ops when
       skip_offset==0 / slot==0 (plain filter); reject non-zero (CASE →
       interpreter fallback). Because `emb_pc_to_op_idx` records
       emb_pc→next-op at the top of the loop, the no-op accept pcs map to
       the outer LoadCol, so the NE_NULL branch resolves to it — no
       pass-2 change needed.
     - Both paths verified → SUM=900. bridge_tests T22b (accept-path) +
       T22c (non-zero skip_offset reject) added.
  - **Multi-file kernel change → rebuild `ndbmtd`** (+ test binary), then
    `rondb_jit_ndbapi_linked_null` / `--only 25 -v`. Files: `ndb_jit_bridge.c`
    (EXIT consts + LOAD_CONST16/WRITE_INTERPRETER_OUTPUT accept-path),
    `JoinAggInterpreter.cpp` (validator + handler wiring + ProcessRec
    reject-skip + linked-attr req_struct plumbing), `AggInterpreter.cpp`,
    `DbtupExecQuery.cpp` (handlers[19]/[41]/[42]), `bridge_tests.c`,
    `testJoinAggNdbApi.cpp` (+ .result + MTR wrapper).
  - **Deferred:** CASE-style non-zero skip_offset in JIT (rejects to
    interpreter for now). A JIT-off differential Test 25 variant would be
    a good follow-up (same bytecode, proves 900 both ways).
- **Test 26** — negative/unsupported-program fallback canary.
- **Test 27** — operand-width boundary canary — **blocked on a policy
  decision** (see Open Decisions).
- **Overflow parity (`phase_5_1_overflow_parity.md`)** — NOT landed.
  JIT silently wraps signed overflow where the interpreter returns
  `ZAGG_MATH_OVERFLOW`. Stage 1 = admission flag + canary + docs (~1d);
  Stage 2 = overflow-checked stencils (~5–7d).
- **Phase 5 (full)** — type-state lattice + stencil picker, full
  ~70–75 stencil matrix, full embedded-branch family (ATTR_OP_ATTR /
  OP_PARAM / OP_ARG, MEM family). `phase_5_implementation.md`.
- **Phase 6** — cross-branch always-JIT (`--force-jit`) differential
  testing. **Phase 7** — SCAN_FRAGREQ scan filters. **Phase 8** —
  production readiness (NDBINFO counters, config param, SIGSEGV
  sidecar, GROUP BY — i.e. lifting the `m_n_gb_cols == 0` gate).

## Chosen next focus: finish the Phase 5.1 canary suite

(Per `phase_5_1_ndbapi_test_additions.md` + `phase_5_1_debug_test_strategy.md`.)

Recommended order — develop each with `4061`/`4062`, then lock with `4060`:

1. **Confirm Test 24 is green at HEAD** (no error inserts → correct
   result; then `4060` → JIT required; all-rejected → SUM NULL).
   Re-run `bridge_tests`, `admission_tests`, `coldcall_tests`,
   `proto_microbench` for no regression.
2. **Shared dump/decode helpers first** (cheap, used by everything):
   `ndb_jit_bridge_dump_input/_program/_reject_reason`; make the
   existing DBLQH full dump conditional on `4061`.
3. **Bridge unit tests for the linked path** (`bridge_tests.c`):
   READ_LINKED_TO_MEM + BRANCH_LINKED_EQ/NE_NULL admission, before
   any data-node run.
4. **Test 25 — linked NULL canary** (NDB API): parent projects a
   nullable linked col via `addLinkedProjection`; child aggregates;
   embedded READ_LINKED_TO_MEM + BRANCH_LINKED_*_NULL. Expect
   SUM(non-NULL marker rows). Verify with `4061`, trace with `4063`,
   lock with `4060`.
5. **Test 26 — unsupported-program fallback** (negative): pick an
   unadmitted shape (e.g. `LoadLinkedColumn` in arithmetic, or an
   opcode the bridge rejects); verify clean interpreter fallback.
   Do not enable `4060`; a developer-only `4062` run confirms the
   reject reason.
6. **Test 27 — operand-width boundaries** — only after the width
   policy is decided (below). Add bridge unit tests first, then table
   tests if high-attr-id tables are practical.

Optional but recommended alongside: cheap runtime counters (compile
attempts/successes, bridge/admission rejects, JIT vs fallback rows,
helper failures) so tests can distinguish "never reached setup" from
"rejected" from "compiled but never ran".

## Open decisions (need a call before parts of the suite)

1. **Operand width policy (gates Test 27) — now an *asymmetry* to
   resolve, not a from-scratch decision.** `emit_op`'s `b`/`c` operands
   are already `uint16_t`, and embedded `BRANCH_ATTR_*_NULL` already
   admits up to `BR_MAX_LOCAL_ATTR_ID = 4095` (`ndb_jit_bridge.c:135,468`).
   But `BR_kOpLoadCol` still rejects `col_index > 255` (packs into a
   `uint8_t`, line ~752,757) and `READ_LINKED_TO_MEM` rejects
   `position > 255` (line ~509). Decide: (a) widen LoadCol (+ position)
   to 4095 to match the branch path and test 255/256/4095-accept,
   4096-reject; or (b) keep LoadCol at 255 deliberately and document the
   asymmetry. Test 27 then proves whichever line is chosen.
2. ~~`ERROR_INSERT 4060` + null `block_tup`.~~ **Already handled** —
   `JoinAggInterpreter.cpp:1131` guards with
   `block_tup != nullptr && block_tup->jit_error_inserted(4060)`. No
   action needed; left here for the record.
3. **Overflow parity stance.** Ship Stage-1 guardrail (admission flag
   + documented divergence) now, or schedule Stage-2 checked stencils?
   Affects whether overflow canaries can join the suite.

## How to build & run (from the docs)

```sh
cmake --build debug_build --target testJoinAggNdbApi -j 4
debug_build/runtime_output_directory/testJoinAggNdbApi -c localhost:1186 -m 3306 --only 23 -v
./mysql-test/mtr --suite=ndb_push_agg testJoinAggNdbApi
# host unit binaries (run directly, no ctest):
debug_build/storage/ndb/test/jit_proto/{admission_tests,bridge_tests,coldcall_tests,proto_microbench}
```

Keep `debug_build/` and `prod_build/` out of commits. Stencil regen
needs upstream LLVM clang **20.1.8** (macOS: `/opt/homebrew/opt/llvm@20/bin/clang`;
Apple clang is rejected by the version check).

## Source map (entry points)

| What | Path |
|---|---|
| Per-row JIT dispatch | `dbtup/JoinAggInterpreter.cpp:~484-500` (post-RONDB-1066; was ~1116) |
| Lifted JIT members | `m_jit_entry`/`m_n_gb_cols` now in `AggInterpreterBase.hpp:405,~470` |
| Glue + cold-call helpers | `dbtup/DbtupJitGlue.{cpp,hpp}` |
| Compile-time setup | `dblqh/DblqhProxy.cpp` (`execJOIN_AGG_SETUP_REQ`, ~2336; compile ~2763-2800) |
| Bytecode→Program bridge | `dbtup/jit/ndb_jit_bridge.c` |
| Copy-and-patch engine | `dbtup/jit/jit1.c`, `bytecode1.h`, `hole_kinds.h` |
| Stencils | `dbtup/jit/stencils_src.c`, `stencils_{x86_64,arm64}.h` |
| NDB API canaries | `block_unit_test/testJoinAggNdbApi.cpp` |
| Host unit tests/bench | `test/jit_proto/` |

## Design docs (intent)

`plan.md` (master, 1479 lines), `phase_5_implementation.md` (full Phase 5),
`phase_5_1_implementation.md` (5.1a/b/c branch families),
`phase_5_1_row_accumulated.md` (acc mask — landed),
`phase_5_1_overflow_parity.md` (not landed),
`phase_5_1_debug_test_strategy.md` (diagnostics + test strategy),
`phase_5_1_ndbapi_test_additions.md` (Tests 23–27, detailed).
