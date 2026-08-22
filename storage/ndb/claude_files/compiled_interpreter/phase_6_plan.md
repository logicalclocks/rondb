# Phase 6 — post-merge integration: fleet conformance, CTE safety, star-join JIT

**Status: PLANNED (2026-08-22). Awaiting approval; execute as
sequential slices 6-0 → 6-4 with the usual
implement → verify → commit cycle per slice. 6-5 is parked
(demand-driven); 6-6 threads through every slice.**

## 0. Context — what Phase 6 has become

The original Phase 6 declaration (`plan.md` §12, written before any
code existed) was: merge the parallel pushdown-join + CTE branch, add
a `--force-jit` switch, and run that branch's interpreter test
programs as an always-JIT conformance harness. Three things have
changed since:

1. **The merge already happened, via 26.04-main.** The branch now sits
   directly on `26.04-main` (`3ac91d6a023`) after the 2026-08-22
   rebase; the only conflict was `testJoinAggNdbApi` test renumbering
   (our JIT canaries moved from Tests 24–29 to 25–30, fixed in
   `469edceaff1`). The parallel branch's whole test fleet is in the
   tree: `block_unit_test/` carries
   `testCte{Dbtc,Lookup,NdbApi,NdbApiFilter,NdbApiOuterJoin,Phase6}`,
   `testInterpreterTypedRegs`, `testJoinAgg{,Spj,NdbApi,ScanScan,Idempotency}`,
   `testOuterJoinAgg{,NdbApi}`, `testMultiOuterJoinAggNdbApi`,
   `testStarJoinAgg{,NdbApi,Spj}`, `testCaseAgg`, `testVarcharMinMax`,
   plus the bench binaries — each with a one-line MTR wrapper in
   `suite/ndb_push_agg` (multi-node variants in `ndb_push_agg_dist`),
   and the RonSQL CTE surface runs as 25 shared test bodies × 5
   topology suites (`ronsql_cte`, `_ng1r3`, `_ng2r2`, `_ng2r3`,
   `_ng4r2`).
2. **Phase 6.5 and the neighbors are already done.** Standalone
   `AggInterpreter` dispatch (`AggInterpreter.cpp:259`), the Phase 7
   scan-filter slice, and Phase 8 (GROUP BY gate lift, code-memory
   manager, ndbinfo.jit, crash registry) all shipped during Phase 5.
3. **No `--force-jit` flag is needed.** The levers already exist:
   ERROR_INSERT **4060** is the fail-loud "must run on the JIT" switch
   (test-armed, per-query), ERROR_INSERT **5119/5120** are the
   compile-time dump/fatal probes, and the cluster config
   `CompiledInterpreter = OFF | AUTO | ON` gives the differential axis
   (MTR `.result` baselines are config-independent, so "suite green
   under OFF" + "suite green under AUTO" *is* the differential run).
   `ON` stays reserved.

**Naming caution:** `testCtePhase6` is phase 6 *of the CTE plan* (full
CTE lifecycle in DBLQH) — unrelated to this document.

So Phase 6 is no longer a merge exercise. It is: **prove the JIT over
the merged fleet, close the integration gaps the fleet exposes, and
pin the result so it cannot silently regress.**

## 1. Findings from the 2026-08-22 post-rebase survey

The sub-phases below are derived from these; file:line references are
current as of `469edceaff1`.

- **F1 — CTE aggregation already reaches the JIT, by omission, not
  design.** The dispatch gate is `m_jit_entry != nullptr &&
  !m_null_local_columns` (`JoinAggInterpreter.cpp:610`) and the
  compile gate is `dbtup_jit_enabled() && m_num_leaves == 1`
  (`DblqhProxy.cpp:2833`) — neither checks `m_cte_mode`. CTE
  *producer* rows are real scanned tuples and behave exactly like
  join-agg rows (correct and desirable). CTE *consumer* feeds
  (`Dblqh::cteLookupAggFeed` / `cteScanAggFeed`,
  `DblqhMain.cpp:19801/20686`) materialize groups into a linked
  buffer and call `processRecWithLinkedAttrs` with the **real**
  `c_tup` but a **stack `KeyReqStruct` whose `tablePtrP` is
  deliberately nullptr** (`DblqhMain.cpp:19855-19870`). Because
  `block_tup != nullptr`, `m_null_local_columns` stays false and the
  JIT gate is satisfied. The `m_cte_mode` attrId normalization for
  GROUP BY keys runs in the prologue *before* dispatch
  (`JoinAggInterpreter.cpp:526-533`), so CTE key semantics are
  preserved on the JIT path.
- **F2 — the interpreter's null-`tablePtrP` guards have no JIT
  counterpart.** The interpreter aborts cleanly with
  `ZAGG_OTHER_ERROR` when a *local* column is referenced on a CTE
  consumer feed (`JoinAggInterpreter.cpp:500-506` GROUP BY arm,
  `:718-721` `kOpLoadCol` arm). The JIT local-column helpers
  dereference `tablePtrP` unconditionally: `jit_load_col_read`'s
  local arm (`DbtupJitGlue.cpp:196, 204-206`),
  `Dbtup::readSingleAttribute` (`Dbtup.hpp:3452-3454`),
  `ndb_jit_h_branch_attr_null` (`DbtupJitGlue.cpp:1087`), and
  `evalBranchColForJit` (`Dbtup.hpp:3350`). In practice a CTE-fed
  leaf program should reference only linked (0x8000-flagged) columns,
  so no crash has been observed — but nothing enforces it, and the
  interpreter's clean-abort contract is silently a null-deref on the
  JIT path.
- **F3 — multi-leaf (star schema) is interpreter-only, and the safety
  is load-bearing in exactly one place.** Only
  `m_leaf_programs[0]` is ever compiled, and only when
  `m_num_leaves == 1` (`DblqhProxy.cpp:2833-2892`); the else arm logs
  "JIT skipped (multi-leaf)" and ERROR_INSERT 5120 makes it fatal
  (`:2964-2977`). `LeafProgram` already carries per-leaf
  `m_jit_prog`/`m_jit_entry` fields, explicitly nulled
  (`JoinAggregationState.hpp:66-97`, `DblqhProxy.cpp:2686-2687`) —
  but the interpreter holds a **single** `m_jit_entry`
  (`AggInterpreterBase.hpp:481`), set once from leaf 0, and the
  per-row leaf switch in `processRecWithLinkedAttrs`
  (`JoinAggInterpreter.cpp:836-841`) swaps `m_prog`/`m_prog_len`/
  `m_agg_prog_start_pos`/`m_acc_offset` **but not `m_jit_entry`**.
  Lifting the compile gate without moving the entry into that switch
  would run leaf 0's code against leaf N's accumulator layout. (The
  `DblqhProxy.cpp:2803-2812` comment "each leaf is independently
  JIT-compiled" is aspirational — the code never was.)
- **F4 — join-agg compiles bypass BOTH the reuse cache and the
  ndbinfo counters.** The join-agg path calls `jit1_compile` directly
  (`DblqhProxy.cpp:2888`); `dbtup_jit_compile_agg` (the cached path,
  `DbtupJitGlue.cpp:1560`) has exactly one caller —
  `PushdownInterpreterFactory::Create` (`PushdownInterpreter.cpp:296`,
  the standalone/RonSQL scan-agg path). Consequences: (a) identical
  join/CTE programs recompile on every `JOIN_AGG_SETUP_REQ`, per CTE
  stage, per node — no reuse, no `AGG_PROG_FLAG_REUSABLE` pinning;
  (b) `ndbinfo.jit.programs_compiled/reused` sum only the two
  progcaches (`DbtupJitGlue.cpp:1618-1624`), so **join-agg compiles
  are invisible to the counter methodology** — only
  `compile_ns_total`, the fallback counter, DEB_JIT logs, and
  4060/5119/5120 observe them today.
- **F5 — the linked-column JIT path is CTE-ready and CTE-optimal.**
  `readLinkedAttrIntoBuf`'s resolution order tries `isCteMarker`
  first (`JoinAggInterpreter.cpp:317-341`), so CTE-marked entries
  always resolve inline; the branch that degrades to per-row fallback
  (`load_program_offset < 0` skipping `findLoadColumnMeta`) can only
  bite parent-table columns whose `ColumnMeta` lookup misses.
- **F6 — CTE WHERE filters are structurally un-JITtable today.**
  `runCteFilter` (`DblqhMain.cpp:19748`) always calls
  `interpreterFilterCte` (the `s_cte_filter_handlers` jump table with
  real-tuple opcodes nulled); the scan-filter JIT is only reachable
  through `req_struct->scan_rec` on a real SCAN_FRAGREQ, which a CTE
  virtual row does not have. Coherent coverage gap, not a bug; these
  filters run per fed group, not per scanned row.
- **F7 — the pushdown/CTE documentation is entirely JIT-unaware, and
  two in-flight plans rewrite the JIT's hook points.**
  `pushdown_join_aggregation/local_execution_mode_plan.md` (LOCAL
  mode, `JOIN_AGG_FEED_REQ`, node-local SETUP) and
  `aggregation_treenode_alternative_plan.md` (`QN_AGGREGATE`
  TreeNode) both move the setup/feed sites where the JIT compiles and
  dispatches. `next_steps.md` Phases 15-20 (AVG, DECIMAL precision,
  expression GROUP BY, post-aggregation expressions) will grow the
  aggregation bytecode the bridge must admit or reject.
- **F8 — stale comments.** `DbtupJitGlue.hpp:159-161` and `:216-218`
  still describe the pre-Phase-8 `m_n_gb_cols == 0` dispatch gate;
  `DblqhProxy.cpp:2803-2812` describes per-leaf compilation that does
  not exist.
- **F9 — the standalone RONDB-733 mysqld fixes are still
  branch-local** after the rebase (`c7e8dd1ff89` nullable-CASE,
  `f0a3d38d451` unsigned SUM/AVG, `3c2c228535f` DECIMAL MIN/MAX) —
  they are JIT-independent and want their own PR to 26.04-main.

## 2. Sub-phases

### 6-0 — Post-rebase verification + fleet coverage census

*Measure first — the census-driven method that ordered Phase 5.*

**The problem today.** Two unknowns. First, the rebase is only
*textually* clean — nothing but test renumbering conflicted, but only
a build + full test run proves the JIT still works on the new base.
Second, the rebase delivered ~20 new test binaries plus the 5 RonSQL
CTE topology suites, and we do not know, for any of them, what the
JIT does with their programs: compiled? rejected by the bridge
(silent interpreter fallback)? never offered to the compiler at all
(multi-leaf skip, CTE filter, lookup-rooted)?

**The change.** No production code.
- Run the host tests and the full MTR sweep **twice**: once with the
  JIT on (default `AUTO`), once with `CompiledInterpreter=OFF`.
  MTR `.result` baselines are config-independent, so "green under
  both arms" *is* the always-JIT differential the original Phase 6
  asked for: same queries, same answers, JIT vs interpreter.
- Build a census table (committed into this doc) recording, per
  fleet test: compiled / fell back (reason) / never reaches the JIT.
  Sources: `ndbinfo.jit` deltas where the cached paths are involved;
  `compile_ns_total` + DEB_JIT / 5119 output for join-agg compiles
  (which today bypass the counters — F4); 5120 spot-probes for
  "does this shape compile".
- Extend `rondb_jit_fallback_census` with the shapes the merged work
  makes pushable: CTE producer aggregation, CTE consumer aggregation,
  star-join (multi-leaf) aggregation, outer-join aggregation — each
  as a counted probe or a documented "never reaches the JIT because
  ..." comment row, same style as the existing rows.

**What it brings.** Confidence the rebase is sound, plus a *measured*
map of JIT coverage over the new territory. That map scopes and ranks
6-2/6-3 by data instead of guesswork.

**Verification (Mikael runs).**
- Host: `bridge_tests` (141), `coldcall_tests` (34).
- Full `ndb_push_agg` sweep (all `rondb_jit_*` canaries + the fleet
  wrappers) and `ndb_push_agg_dist`, under the default
  `CompiledInterpreter=AUTO` — this is the JIT arm of the
  differential.
- The same sweep once with `CompiledInterpreter=OFF` (suite `my.cnf`
  edit for the run, not committed) — the interpreter arm. Identical
  `.result` baselines passing under both arms is the always-JIT
  differential the original Phase 6 asked for.
- One representative RonSQL CTE topology suite (`ronsql_cte`) under
  both arms; the other four topologies under AUTO only.

**Exit.** Both arms green; census table committed; 6-2/6-3 scope
confirmed or re-ranked from the measurements.

### 6-1 — CTE-consumer safety: null-`tablePtrP` hardening

**The problem today (F2).** When a query stage consumes a CTE's
materialized result, DBLQH feeds the groups to the aggregation
interpreter as *virtual rows*: no scanned tuple exists behind them,
and the row context is built with `tablePtrP = nullptr` on purpose.
The interpreter knows this — a program reading a normal local-table
column on such a row aborts the query cleanly with
`ZAGG_OTHER_ERROR`. The JIT does **not**: nothing in its gates
excludes CTE mode (these programs compile and dispatch natively), and
its local-column helpers dereference `tablePtrP` unchecked. No crash
observed — consumer programs in practice read only linked columns,
which the JIT handles perfectly — but nothing enforces that. One
wrong program shape turns a clean query error into a data-node
segfault.

**The change.** A null check at the top of the three JIT helpers that
touch local columns:
- `jit_load_col_read`'s local arm (`DbtupJitGlue.cpp:~196`):
  `req_struct->tablePtrP == nullptr` → return -1 → `row_fallback` —
  the standard Phase 5A mechanism: the JIT run of that row is
  discarded, the row re-runs on the interpreter, whose own guard then
  fails the query with exactly today's error.
- Same guard atop `ndb_jit_h_branch_attr_null` and
  `ndb_jit_h_branch_attr_op_arg` (both reach
  `readSingleAttribute`/`evalBranchColForJit`, which deref
  unconditionally).
- Coldcall test: mock ctx with null `tablePtrP`, assert `row_fallback`
  (not a crash) for the local-load, branch-null, and branch-op-arg
  helpers; linked (0x8000) loads keep working.
- Fold in F8: fix the three stale comments (`DbtupJitGlue.hpp:159-161`,
  `:216-218`, `DblqhProxy.cpp:2803-2812`).

**What it brings.** Crash-proofing only — zero behavior change for
any query that works today. The JIT path can no longer downgrade
"clean query error" into "node crash" on CTE-consumer rows. The
smallest slice of the phase.

**Verification.** Host tests; `testCteNdbApi` / `testCteNdbApiFilter` /
`testCteLookup` wrappers re-run (behavior must be unchanged — today's
consumer programs are linked-only, the guard is armor, not a
behavior change); full sweep.

**Exit.** Coldcall green with the new tests; CTE fleet unchanged; no
JIT code path can deref a null `tablePtrP`.

### 6-2 — Conformance pinning: the fleet as a standing must-JIT harness

**The problem today.** JIT fallback is *silent and correct by design*
— a query that stops compiling still returns right answers, just
slower. Right for production, worst-case for testing: if a future
change (ours or the pushdown team's) makes the bridge reject a
program family, every test stays green and the performance loss
surfaces months later as an unexplained regression. Our
`rondb_jit_*` canaries pin the mysqld/RonSQL shapes against this;
the merged fleet's hand-crafted NDB-API programs are completely
unpinned.

**The change.** Convert 6-0's measurements into permanent tripwires:
- For fleet binaries/sections 6-0 classified as fully JIT-covered:
  arm 4060 ("any fallback = abort the data node") around them —
  wrapper-level `--exec $NDB_MGM -e "all error 4060"` where the whole
  binary qualifies, or dedicated 4060-armed variants in the
  `rondb_jit_ndbapi_*` style where only sections qualify.
- For SQL/RonSQL-reachable CTE and join shapes: must-JIT canary
  queries under 4060 (CTE producer + consumer aggregation via the
  `ronsql_cte` body patterns; outer-join aggregation with the
  NULL-extended-row exemption documented).
- A committed exemption list in this doc, each entry with its reason:
  programs that intentionally use unsupported opcodes
  (`kOpSetRegNull` canaries), per-row-fallback shapes (div-by-zero,
  NULL probes), NULL-extended rows, multi-leaf (until 6-3), CTE
  filters (until/unless 6-5).

**What it brings.** Coverage regressions become loud test failures
*at the commit that introduces them*. This is the durable replacement
for the one-off `--force-jit` run the original plan imagined — not a
mode someone remembers to run, but the standing state of the suite.

**Verification.** The armed fleet + canaries green; deliberately
reverting one Phase-5 admission (local experiment) must turn the
harness red.

**Exit.** Any future admission or dispatch regression on a pinned
shape aborts a data node in the test run instead of silently falling
back.

### 6-3 — Multi-leaf (star schema) JIT

*The main implementation slice: retire the last program-shape
exclusion.*

**The problem today (F3).** A star/snowflake join aggregation ships
one aggregation program *per leaf table* to the data node. Per row,
the interpreter switches to that row's leaf program and accumulates
into that leaf's slice of one shared accumulator array (shifted by
`m_acc_offset`). The JIT compiles **nothing** when there is more than
one leaf (`m_num_leaves == 1` gate) — for a sound reason: the
interpreter holds a *single* `m_jit_entry` (from leaf 0), and the
per-row leaf switch swaps the bytecode but has no way to swap the
compiled code. Naively lifting the gate would run leaf 0's machine
code for every row regardless of leaf — silently wrong results into
wrong accumulator slots. So star-join aggregation runs 100%
interpreted: the last whole program category excluded from the JIT.

**The change.** Four coordinated pieces:
- **Compile every leaf:** turn the leaf-0 block
  (`DblqhProxy.cpp:2833-2892`) into a per-leaf loop storing into each
  `LeafProgram.m_jit_prog/m_jit_entry` (the fields already exist,
  today always nulled). Per-leaf independence: a leaf that fails
  admission leaves only its own entry null — mixed JIT/interpreter
  execution is safe because each row runs exactly one leaf program.
  Teardown already frees per-leaf (verify `tearDownChunk` handles N
  entries).
- **Switch the entry with the program:** the per-row leaf switch
  (`JoinAggInterpreter.cpp:836-841`) additionally selects the current
  leaf's entry (`m_jit_entry = leaf.m_jit_entry` alongside the
  existing `m_prog`/`m_acc_offset` swap); `processNullExtendedRow`
  stays interpreter-only (unchanged `!m_null_local_columns` gate).
- **Leaf-local invoke glue:** `dbtup_jit_invoke`'s writeback must be
  leaf-local — `agg_res_ptr + m_acc_offset` with the leaf's own
  `m_n_agg_results`, `value_initialized`/writeback masks, and (for 5J
  embedded blocks) the leaf's own `agg_program() +
  agg_prog_start_pos()` as `prog_buf`. This is the audit-heavy part:
  find every place the glue assumes "the one program" — entry, masks,
  embedded `prog_buf`, fallback re-run.
- **ERROR_INSERT semantics flip:** 5120's multi-leaf-skip abort arm
  (`DblqhProxy.cpp:2964-2977`) inverts — with the gate lifted it
  becomes "any leaf failed to compile is fatal"; 4060 needs no
  change (a null per-leaf entry that dispatches now aborts naturally).

Notably **no new stencils and no bridge changes** — each leaf program
is an ordinary single program from the compiler's viewpoint;
multi-leaf-ness is purely a runtime placement concern.

**What it brings.** Star-schema aggregation — the TPC-H-style
fact-plus-dimensions pattern, the `testStarJoinAgg*` fleet, the
snowflake/star CTE bodies — runs native. After this slice, every
aggregation program shape the planner can push is JIT-eligible; the
exclusion list shrinks to per-row conditions only.

**Verification.** Host tests (bridge unchanged, coldcall for any new
glue seams); `testStarJoinAgg`, `testStarJoinAggNdbApi`,
`testStarJoinAggSpj`, `testMultiOuterJoinAggNdbApi` wrappers, then
under 4060 per 6-2; snowflake/star `ronsql_cte` bodies; census
star probes flip from "never reaches the JIT" to counted; full sweep
both differential arms.

**Exit.** Star fleet green with per-leaf JIT proven by 4060; the
"JIT skipped (multi-leaf)" log line is dead code and removed.

### 6-4 — Join-agg/CTE compiles through the reuse cache

**The problem today (F4).** Three compile entry points exist, and
only two use the program-reuse cache. Standalone/RonSQL scan
aggregation and scan filters go through it: identical programs
compile once, later setups reuse the machine-code blob, and the cache
feeds `ndbinfo.jit`'s `programs_compiled`/`programs_reused`. The
join-agg/CTE path calls `jit1_compile` directly. Two consequences:
(a) a CTE query recompiles its identical per-stage programs on
**every execution, on every node** — pure wasted setup latency, worst
exactly where programs repeat most (each CTE stage is its own
setup); (b) join-agg compiles are *invisible* to the ndbinfo
counters — the counter-delta methodology our canaries use cannot see
this path at all (those canaries lean on 4060 instead).

**The change.**
- Route the DblqhProxy per-leaf compile through
  `dbtup_jit_compile_agg` (or a shared acquire/compile/release entry
  extracted from it) against `agg_cache()`: acquire keyed by program
  bytes, release at teardown instead of `jit1_free`.
- CTE producers/consumers benefit most (one program per CTE stage ×
  per node × per setup today).
- Verify canary counter expectations: `programs_compiled + reused`
  sums stay monotone; join canaries that previously showed no
  counter movement will start counting — re-record where a canary
  pinned exact deltas.

**What it brings.** Repeated queries skip compilation (lower setup
latency, less code-memory churn — most visible on CTE-heavy and
high-QPS workloads), and one uniform observability story:
`ndbinfo.jit` finally reports the truth for all three compile paths,
closing the census blind spot 6-0 had to work around.

**Verification.** Host tests; repeated identical join query shows
`programs_reused` movement; full sweep; a CTE-heavy `ronsql_cte`
body shows reduced `compile_ns_total` growth.

**Exit.** One compile pipeline, one counter story; no join-agg blind
spot in `ndbinfo.jit`.

### 6-5 — CTE WHERE-filter JIT (PARKED — demand-driven)

**The problem today (F6).** A CTE consumer can filter the
materialized CTE rows before aggregating them. Those filters run on
virtual rows through the dedicated `s_cte_filter_handlers` jump table
and *structurally cannot* reach the scan-filter JIT — that path
requires a real scan record a virtual row does not have. So CTE
filters are always interpreted.

**Why parked rather than scheduled.** Scan filters run per scanned
row — millions of executions, big JIT win. CTE filters run once per
*fed group* — typically orders of magnitude fewer executions, so the
ceiling on the win is low. The work is well understood if demand
appears: admissible opcodes are exactly `s_cte_filter_handlers`'
non-null slots (register ops + linked ops), which the bridge's
embedded translator mostly covers; the new part is the invoke
context (no scan_rec, no real tuple; reject =
`INTERPRETER_FILTER_REJECT` via the `handleExitRefuseCte`
convention). If 6-0's census + fleet timings show these filters hot,
the slice mirrors Phase 7's scan-filter shape: compile once at CTE
setup, dispatch in `runCteFilter`, 4060-armed canary. Until then the
value is a *documented, deliberate* boundary instead of an
accidental one.

### 6-6 — Documentation + cross-branch coordination (threads throughout)

**The problem today (F7).** The pushdown-join/CTE documentation
contains **zero** mentions of the JIT — the people evolving that
machinery cannot know their changes land on JIT hook points.
Concretely dangerous right now: two of their in-flight plans
(`local_execution_mode_plan.md` — moves aggregation setup to a new
node-local feed signal; `aggregation_treenode_alternative_plan.md` —
turns aggregation into an explicit query-tree node) would relocate
the exact signal-handling sites where the JIT compiles and
dispatches. Landed uncoordinated, either one silently stops the JIT
compiling join aggregation. (6-2's harness would catch it loudly —
prevention still beats detection.)

**The change.**
- Author the missing cross-references: a short "compiled interpreter
  touchpoints" section in `pushdown_join_aggregation/CLAUDE.md`
  naming the compile hook (`DblqhProxy::execJOIN_AGG_SETUP_REQ`), the
  dispatch gate (`JoinAggInterpreter.cpp:610`), the null-extended and
  CTE-consumer rules, and this plan.
- Add coordination notes **into** `local_execution_mode_plan.md` and
  `aggregation_treenode_alternative_plan.md`: whichever lands first
  must carry the compile hook and the dispatch gate with it, and
  re-run the 6-2 harness as its acceptance gate.
- Record the future-opcode pipeline (AVG, post-aggregation
  expressions, expression GROUP BY from `next_steps.md` 15-20) as
  named census probes to add when they land.
- Bookkeeping (F9): the three standalone RONDB-733 fixes go to
  26.04-main in their own PR.
- Update `STATUS_AND_NEXT_STEPS.md` per slice, as always.

**What it brings.** The two workstreams stop being mutually
invisible, and Phase 6's exit state survives contact with the next
months of pushdown development.

## 3. Ordering and risks

**Order: 6-0 → 6-1 → 6-2 → 6-3 → 6-4**, re-ranked after 6-0's census
if the data disagrees (the Phase 5 rule). 6-1 before 6-2 so the
harness never pins an unsafe path; 6-3 before 6-4 so the cache
routing is written once against the per-leaf loop rather than
rewritten by it.

**Risks.**
- *In-flight pushdown plans* (F7) can move the hook points mid-phase
  — 6-6's coordination notes are the mitigation, and 6-2's harness
  is the detector.
- *Multi-leaf accumulator placement* (6-3) is the one genuinely
  subtle change: every glue assumption about "the one program" must
  be found, and the leaf-switch/entry-switch pairing has a
  silent-wrong-results failure mode if missed. The 6-2 harness plus
  `testStarJoinAgg*` differential runs are the guard.
- *Counter re-records* (6-4): canaries that pinned exact
  compiled/reused deltas may need re-recording; use sums and `>=`
  where possible.
- *OFF-arm mechanics* (6-0): `CompiledInterpreter=OFF` needs a
  cluster-config edit for the run; keep it a documented local edit,
  not a committed suite change.
