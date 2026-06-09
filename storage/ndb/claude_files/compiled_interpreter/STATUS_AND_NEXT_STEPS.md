# RONDB-1056 Compiled Interpreter — Status & Next Steps

**Updated: 2026-06-09.** Single entry point for resuming work. Branch:
`RONDB-1056-compiled-interpreter`.

> ⚠️ **Docs-vs-reality note.** `plan.md`'s header still says
> *"Status: planning. Code: not started."* That is stale. The JIT is
> implemented and wired end-to-end (~34 real code commits across
> Phases 0–5.0, plus Phase 5.1a). Treat `plan.md` / `phase_*.md` as
> *design intent*; treat the source tree as ground truth.

## Latest implementation — standalone aggregation JIT (2026-06-08)

Commit `55dab872ef4` (`RONDB-1056 JIT standalone aggregation`) extends the
JIT beyond *join* aggregation to **standalone pushed aggregation**
(`AggInterpreter`, the non-join path). Previously only
`JoinAggInterpreter::ProcessRec` dispatched to the JIT; now
`AggInterpreter::ProcessRec` carries the same scalar-aggregation dispatch
(`if (m_jit_entry != nullptr && m_n_gb_cols == 0) dbtup_jit_invoke(...)`).
GROUP BY still stays on the interpreter (the `m_n_gb_cols == 0` gate is
unchanged).

- **New compile/setup hook:** `PushdownInterpreterFactory::Create()` now
  takes a `NdbJitArena *jit_arena`; for an admitted aggregation program it
  runs `ndb_jit_bridge_translate()` → `jit1_compile()` → `setJitEntry()`.
  This is the standalone analogue of the join path's
  `DblqhProxy::execJOIN_AGG_SETUP_REQ` compile. Touched
  `PushdownInterpreter.{cpp,hpp}`, `Dbtup.hpp`, `DbtupGen.cpp`,
  `DbtupExecQuery.cpp`, `DbtupJitGlue.{cpp,hpp}`, `AggInterpreterBase.hpp`,
  `AggInterpreter.cpp`, `JoinAggInterpreter.cpp`.
- **Diagnostics:** `ERROR_INSERT 4060` now also guards
  `AggInterpreter::ProcessRec` — a standalone program that reaches the
  interpreter loop instead of the JIT aborts under the canary.
- **New MTR canary:** `rondb_jit_standalone_canary` (ndb_push_agg suite):
  `SUM(v)`, `SUM(v+pk)`, `SUM(v-pk+pk*0)` forced through JIT via
  `all error 4060`, plus a `WHERE`-clause fallback smoke query (Q4).
- **Verification: DONE** (per Mikael) — host unit binaries and the
  `ndb_push_agg` JIT set, including `rondb_jit_standalone_canary`, pass.

## Phase 7 groundwork — scan-filter reject state (2026-06-08)

Commit `c955005048b` (`RONDB-1056 Add scan filter reject JIT state`) lands
the *translation + engine* half of SCAN_FRAGREQ scan-filter JIT, but the
path is **inert** — nothing in DBTUP calls it at runtime yet.

- **New translate API:** `ndb_jit_bridge_translate_scan_filter()` reuses the
  embedded-interpreter subset (`BRANCH_ATTR_*_NULL`, `READ_LINKED_TO_MEM`,
  `BRANCH_LINKED_*_NULL`, `EXIT_OK`, `EXIT_REFUSE`). It lowers `EXIT_REFUSE`
  to the new **`OP_FILTER_REJECT_EXIT`** (via a `exit_refuse_kind` param now
  threaded through `translate_embedded_block`) and appends a trailing
  `OP_EXIT` for the fall-through accepted-row path. Standalone CASE
  skip-offsets (`WRITE_INTERPRETER_OUTPUT` with non-zero slot 0, i.e.
  `n_pending_case_jumps != 0`) are **rejected for now** — there is no outer
  aggregate word stream to skip through.
- **New opcode + stencils:** `OP_FILTER_REJECT_EXIT` with x86_64 + arm64
  stencils (`stencils_src.c`, `stencils_{x86_64,arm64}.h`,
  `extract_stencils.c`); it sets `JitState::row_filter_rejected` (new field
  in `jit1.h`) and returns.
- **Tests:** `coldcall_tests.c` T11 (`filter_reject_exit_sets_state`) proves
  native execution sets `row_filter_rejected=1`; `bridge_tests.c` adds
  scan-filter translate/admission coverage.
- **Verification: DONE** (per Mikael) — host unit binaries pass.
- **Explicitly NOT done (the next feature):** DBTUP runtime scan-filter
  invocation glue — a setup/compile hook for SCAN_FRAGREQ filters and the
  per-row call that branches on `row_filter_rejected` instead of
  `interpreterNextLab()`. See "Next focus" below.

## Latest verification — checked overflow stencils (2026-06-06)

Checked arithmetic stencils for ADD/MINUS/MUL/SUM are implemented, wired
through the bridge, and committed as `7d0498410be` (`RONDB-1056 Add checked
JIT overflow stencils`). The wide-column regression that originally tripped
the one-byte column-id assertion now passes, and Mikael reported that all tests
in the `ndb_push_agg` suite pass after the change.

Host-layer verification before commit:
- `regen-stencils`: PASS.
- `bridge_tests`: 38/38 passed.
- `admission_tests`: 16/16 passed.
- `coldcall_tests`: 9/9 passed.
- `proto_microbench`: PASS, including the checked-arithmetic normal-path
  informational run.

## Latest implementation — CASE skip offsets (2026-06-06)

CASE-style embedded accept paths with non-zero
`WRITE_INTERPRETER_OUTPUT slot 0` skip offsets are now implemented in the JIT.
The bridge lowers such accept paths to a new unconditional forward `OP_JUMP`;
the jump target is resolved from the outer aggregation word position to the
corresponding JIT op after the full outer program has been translated. Output
slots other than 0 still reject to interpreter fallback.

Host-layer verification:
- `regen-stencils`: PASS; generated headers now contain 31 stencils.
- `bridge_tests`: 38/38 passed, including `T22c
  embedded_case_skip_offset_accept`.
- `admission_tests`: 17/17 passed, including malformed `OP_JUMP` admission.
- `coldcall_tests`: 10/10 passed, including native `OP_JUMP` execution.
- `proto_microbench`: PASS.
- After rebuild, Mikael reported that all tests in the `ndb_push_agg` suite
  pass with the CASE skip-offset implementation.
- Added Test 28 (`JIT CASE skip offset`) as an NDB API/MTR canary; Mikael
  reported `testJoinAggNdbApi` passing after the addition.
- Mikael then reported the dedicated `rondb_jit_ndbapi_case_skip` MTR wrapper
  and the full `ndb_push_agg` suite passing with Test 28 included.

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
- **Data-node layer: GREEN (2026-06-05).** The live-cluster MTR sweep below
  (JIT canaries + the broader unified-interpreter regression set) was run and
  **all tests passed** — the real merge gate is closed. These exercise the
  refactored `ProcessRec` + JIT dispatch AND the unified interpreter path
  (the class merge changed the *interpreter*, not just JIT). Command used,
  from `debug_build/mysql-test`:
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

**Cleanup item (RESOLVED 2026-06-09):** the comment at
`AggInterpreterBase.hpp:~440` claimed "both static_asserts on subclass
sizeof still hold," but no `static_assert(sizeof(...))` on the subclasses
remains. Investigation showed the asserts were **deliberately removed** in
RONDB-1066 Step 3a-B (`d22ccf510d8`) when the ~30 KB inline buffers moved
out to an externally carved, right-sized `m_buf_block` — the placement-new'd
object header (`PushdownInterpreter.cpp:280`, into a 32 KB `MEM_CHUNK_SIZE`
chunk) is now only a few hundred bytes, so the guard was no longer
meaningful. Fixed as a documentation correction: the comment now describes
the post-3a-B allocation reality (no assert re-added — that would be a new,
very loose guard, not a restoration).

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
- **Standalone aggregation (2026-06-08, `55dab872ef4`):** the same
  scalar-aggregation dispatch is now ALSO in `AggInterpreter::ProcessRec`
  (non-join path). Standalone compile happens in
  `PushdownInterpreterFactory::Create()` (now takes `NdbJitArena *jit_arena`)
  → `ndb_jit_bridge_translate()` → `jit1_compile()` → `setJitEntry()`.
  Same `m_n_gb_cols == 0` gate; canary `rondb_jit_standalone_canary`.
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
  The microbench also includes an informational checked-arithmetic
  normal-path variant (canonical 30-op program with checked ADD/SUM and a
  hidden `OP_OVERFLOW_EXIT`): last run on 2026-06-06 reported 31 ops,
  668 emitted bytes, 11.06 ns/row JIT median, 4.50 us warm compile, and
  4.87x speedup on the local arm64 debug build.
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
       `WRITE_INTERPRETER_OUTPUT`(123) in embedded blocks. `skip_offset==0`
       / slot 0 remains a no-op plain-filter accept path; non-zero slot-0
       skip offsets now emit `OP_JUMP` and resolve to the selected later
       outer aggregation instruction.
     - Both paths verified → SUM=900. bridge_tests T22b (accept-path) +
       T22c (CASE non-zero skip_offset accept) added.
  - **Multi-file kernel change → rebuild `ndbmtd`** (+ test binary), then
    `rondb_jit_ndbapi_linked_null` / `--only 25 -v`. Files: `ndb_jit_bridge.c`
    (EXIT consts + LOAD_CONST16/WRITE_INTERPRETER_OUTPUT accept-path),
    `JoinAggInterpreter.cpp` (validator + handler wiring + ProcessRec
    reject-skip + linked-attr req_struct plumbing), `AggInterpreter.cpp`,
    `DbtupExecQuery.cpp` (handlers[19]/[41]/[42]), `bridge_tests.c`,
    `testJoinAggNdbApi.cpp` (+ .result + MTR wrapper).
  - **CASE non-zero skip_offset:** implemented on 2026-06-06 via `OP_JUMP`
    and verified in host tests. A JIT-off differential Test 25 variant would
    still be a useful follow-up (same bytecode, proves 900 both ways).
- **Test 26** — unsupported-program fallback canary: **IMPLEMENTED
  (2026-06-05), pending test run.** Shape: `MAX(amount)` over the Test 23
  `jagg_parent`/`jagg_child` tables (no GROUP BY → a JIT-eligible shape).
  `MAX` lowers to `kOpMaxBigint`, which the bridge's main switch does not
  emit — it hits the `default` → `JIT_BRIDGE_UNSUPPORTED_OP`
  (`ndb_jit_bridge.c:901-903`), so the program is rejected at setup,
  `m_jit_entry` stays nullptr, and `JoinAggInterpreter::ProcessRec` runs the
  interpreter. The canary asserts the query succeeds and returns the correct
  `MAX` (=500) — proving the reject path falls back cleanly. **No error
  inserts** (4060/4062 would abort the very path under test), so it also runs
  in production builds. Files: `testJoinAggNdbApi.cpp`
  (`testJitUnsupportedFallback` + Test 26 dispatch), `r/testJoinAggNdbApi.result`
  (added line), new MTR wrapper `t/rondb_jit_ndbapi_unsupported_fallback.test`
  + `r/...result`. **Verify:** build `testJoinAggNdbApi`, then
  `--only 26 -v` against a cluster, then
  `./mtr --suite=ndb_push_agg rondb_jit_ndbapi_unsupported_fallback testJoinAggNdbApi`.
  Developer-only: a one-off `4062` run should make it fail at setup with a
  `kOpMaxBigint` UNSUPPORTED_OP reason (kept out of MTR). Durability: if a
  later phase lowers MAX, switch the program to a still-unsupported op
  (DivInt/Mod) to retain fallback coverage.
- **Test 27** — operand-width boundary canary: **IMPLEMENTED
  (2026-06-06), pending test run.** Policy decided (option (a)): widen the
  real-column path to the full 0..4095 range (4096 columns, =
  `MAX_ATTRIBUTES_IN_TABLE`); linked `position` stays at 255 because it is
  an 8-bit field in NDB's own Interpreter wire format
  (`(inst>>16)&0xFF`), not an arbitrary JIT cap — and a query never has
  thousands of linked projections.
  - **Audit result (all interpreter commands):** the only operand that is
    a real table column id is `kOpLoadCol`'s `col_index`; every other
    operand is a register (≤`BC_MAX_REGS`=8), accumulator slot
    (≤`BC_MAX_ACCS`=4), branch offset/length, or emb length — bounded by
    JIT resources, not table width. The engine was *already* wide: `Op.b`/
    `Op.c` are `uint16_t`, the cold-call helper args are `uint32_t`
    (`ndb_jit_h_load_col(JitState*, uint32_t col_id, uint32_t)`), and the
    operand holes carry the full value (x86_64 `imm32`; aarch64 narrow
    `movz` writes a 16-bit imm with no 255 clamp — same patcher the
    already-4095 `BRANCH_ATTR_*_NULL` attr_id uses).
  - **Fix (1 functional line):** `ndb_jit_bridge.c` `kOpLoadCol` — change
    `col_index > 255` → `> BR_MAX_LOCAL_ATTR_ID` and drop the
    `(uint8_t)col_index` cast (value goes into the `uint16_t` `c`). No
    engine/stencil/helper change. Stale `≤255` comments updated in
    `ndb_jit_bridge.c` + `bytecode1.h`.
  - **Tests:** `bridge_tests.c` — T5 (255 accept) kept; T6 flipped
    256→accept; added T6b (4095 accept) + T6c (4096 reject). NDB API
    `testJitWideColumn` (Test 27): 260-column child table, aggregate
    `c260` (column id 260 > 255) with `4060` (JIT required) → SUM=1500;
    self-checks `c260`'s column id is actually > 255. New MTR wrapper
    `t/rondb_jit_ndbapi_wide_column.test` (+result) + line in the
    consolidated `testJoinAggNdbApi.result`. **Verify:** rebuild
    `bridge_tests`, `testJoinAggNdbApi`, `ndbmtd` (the bridge is kernel
    code), then `./mtr --suite=ndb_push_agg rondb_jit_ndbapi_wide_column
    testJoinAggNdbApi` and run `bridge_tests` directly.
- **Overflow parity (`phase_5_1_overflow_parity.md`)** — NOT landed;
  **chosen path is overflow-checked stencils.** JIT silently wraps
  signed overflow where the interpreter returns `ZAGG_MATH_OVERFLOW`.
  Do not ship the Stage-1 fallback/SQL-var guardrail as the solution; implement
  checked add/sub/mul/sum stencils and make the JIT return the interpreter's
  overflow error directly.
- **Phase 5 (full)** — type-state lattice + stencil picker, full
  ~70–75 stencil matrix, full embedded-branch family (ATTR_OP_ATTR /
  OP_PARAM / OP_ARG, MEM family). `phase_5_implementation.md`.
- **Phase 6** — cross-branch always-JIT (`--force-jit`) differential
  testing. **Phase 7** — SCAN_FRAGREQ scan filters: **translate + engine
  half LANDED 2026-06-08** (`ndb_jit_bridge_translate_scan_filter` +
  `OP_FILTER_REJECT_EXIT`, see the Phase 7 groundwork section above); the
  **runtime DBTUP invocation glue is the next feature** (see "Next focus").
  **Phase 8** — production readiness (NDBINFO counters, config param,
  SIGSEGV sidecar, GROUP BY — i.e. lifting the `m_n_gb_cols == 0` gate;
  note standalone *and* join scalar aggregation now both JIT under that
  gate as of 2026-06-08).

## Next focus: Phase 7 — SCAN_FRAGREQ scan-filter runtime glue

The Phase 5.1 canary suite (Tests 23–28) is **complete and verified**, and
standalone aggregation now JITs (2026-06-08). The translate + engine half of
Phase 7 scan filters also landed (2026-06-08). The remaining gap — and the
recommended next implementation chunk — is the **DBTUP runtime side** that
makes `ndb_jit_bridge_translate_scan_filter()` actually run:

1. **Setup/compile hook for SCAN_FRAGREQ filters** — the scan-filter
   analogue of `PushdownInterpreterFactory::Create()` /
   `DblqhProxy::execJOIN_AGG_SETUP_REQ`: detect a JIT-eligible scan filter,
   call `ndb_jit_bridge_translate_scan_filter()` → `jit1_compile()`, stash
   the resulting `jit1_entry()` so the per-row path can find it.
2. **Per-row invocation glue** — replace/short-circuit `interpreterNextLab()`
   for the supported subset: call the compiled entry, then accept/reject the
   row based on `JitState::row_filter_rejected` (set by
   `OP_FILTER_REJECT_EXIT`). Fall back to `interpreterNextLab()` for any
   filter the bridge rejected at setup.
3. **Canary** — an MTR / NDB API test that forces a scan filter through JIT
   (e.g. `4060`-style) and checks the row set matches the interpreter.
4. **(Deferred)** standalone CASE disposition model — the translate API
   currently rejects `WRITE_INTERPRETER_OUTPUT` skip-offsets
   (`n_pending_case_jumps != 0`); only needed if scan filters require
   multi-way CASE.

### Historical: the Phase 5.1 canary suite (DONE)

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
5. **Test 26 — unsupported-program fallback** (negative): **DONE
   (2026-06-05).** Shipped shape is `MAX(amount)` (→ `kOpMaxBigint`,
   rejected by the bridge default), no `4060`/`4062` in MTR. See the
   Test 26 entry above.
6. **Test 27 — operand-width boundaries** — **DONE (2026-06-06).** Policy
   = option (a): `kOpLoadCol` widened to 4095; linked `position` stays at
   255 (8-bit wire format). Bridge unit tests (255/256/4095 accept, 4096
   reject) + NDB API canary (260-col table, col_id 260, `4060`). See the
   Test 27 entry above.

Optional but recommended alongside: cheap runtime counters (compile
attempts/successes, bridge/admission rejects, JIT vs fallback rows,
helper failures) so tests can distinguish "never reached setup" from
"rejected" from "compiled but never ran".

## Open decisions (need a call before parts of the suite)

1. ~~**Operand width policy (gates Test 27).**~~ **RESOLVED 2026-06-06 —
   option (a).** `BR_kOpLoadCol` now admits `col_index` up to
   `BR_MAX_LOCAL_ATTR_ID = 4095` (was 255), matching the
   `BRANCH_ATTR_*_NULL` path and NDB's `MAX_ATTRIBUTES_IN_TABLE = 4096`;
   the `(uint8_t)` cast was dropped. `READ_LINKED_TO_MEM` `position`
   deliberately stays at 255 — it is an 8-bit field in NDB's Interpreter
   wire encoding, not an arbitrary JIT cap (widening it would require an
   NdbAggregator/Interpreter wire-format change, and linked-projection
   counts never approach 4096). See the Test 27 entry above for the audit
   and the shipped change.
2. ~~`ERROR_INSERT 4060` + null `block_tup`.~~ **Already handled** —
   `JoinAggInterpreter.cpp:1131` guards with
   `block_tup != nullptr && block_tup->jit_error_inserted(4060)`. No
   action needed; left here for the record.
3. ~~**Overflow parity stance.**~~ **RESOLVED — checked stencils.** Implement
   overflow-checked stencils for signed add/sub/mul/SUM and return
   `ZAGG_MATH_OVERFLOW` from the JIT path. The Stage-1 fallback/SQL-var
   guardrail is not the chosen product path.

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
| Per-row JIT dispatch (join) | `dbtup/JoinAggInterpreter.cpp:~484-500` (post-RONDB-1066; was ~1116) |
| Per-row JIT dispatch (standalone) | `dbtup/AggInterpreter.cpp::ProcessRec` (added 2026-06-08, `55dab872ef4`) |
| Lifted JIT members | `m_jit_entry`/`m_n_gb_cols` now in `AggInterpreterBase.hpp:405,~470` |
| Glue + cold-call helpers | `dbtup/DbtupJitGlue.{cpp,hpp}` |
| Compile-time setup (join) | `dblqh/DblqhProxy.cpp` (`execJOIN_AGG_SETUP_REQ`, ~2336; compile ~2763-2800) |
| Compile-time setup (standalone) | `dbtup/PushdownInterpreter.cpp` (`PushdownInterpreterFactory::Create`, `jit_arena` param) |
| Bytecode→Program bridge | `dbtup/jit/ndb_jit_bridge.c` |
| Scan-filter translate (Phase 7, inert) | `ndb_jit_bridge_translate_scan_filter` + `OP_FILTER_REJECT_EXIT` (`c955005048b`) |
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
