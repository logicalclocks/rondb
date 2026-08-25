# Phase 6 — post-merge integration: fleet conformance, CTE safety, star-join JIT

**Status: 6-0 DONE & VERIFIED (2026-08-25 — all tests passed: host
tests green; suite `ndb_push_agg` green under CompiledInterpreter=OFF
and its full mirror `ndb_push_agg_jit` green under ON = the
structural always-JIT differential; `ronsql_cte_jit_census` baseline
recorded with both asserts holding after the EXIT_OK_LAST
counter-hygiene fix; fallback-log harvest recorded in §6-0).
6-1 DONE & VERIFIED (2026-08-25 — all tests passed with zero baseline
movement, exactly the armor-only expectation: host tests at 141/34,
both differential arms green, CTE census still reject-free; the four
null-`tablePtrP` guards, the scan-filter row_fallback fail-fast, and
the F8 comment fixes are in).
6-2 DONE & VERIFIED (2026-08-25 — ndb_push_agg_jit green twice with
the 25 recorded delta pins stable, ndb_push_agg green, the upgraded
must-JIT CTE census green with all four asserts under 4060 after the
unsigned-marker fix, the new outer-join canary green with
compile-activity proof, and the full ronsql_cte suite green as the
marker-change regression guard. The census's second catch was a REAL
cross-engine bug — unsigned CTE aggregate slots marked signed, JIT
consumer rows silently interpreter-bound, latent ≥2^63 interpreter
misread — fixed in DblqhMain.cpp as a standalone commit. Full-suite
runs at --parallel=10 hit a machine-wide connect blip (4 workers, 4
ports, one 11 s window: curl status 7 / mgm connect retries) —
infra, not engine; --parallel=6 was clean. Use reduced parallelism
for the ronsql_cte suites on this machine.)
Remaining slices 6-3 → 6-4 execute with the usual
implement → verify → commit cycle; 6-5 is parked (demand-driven);
6-6 threads through every slice.**

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

**Implemented — the structural differential (2026-08-22, Mikael's
design): suites `ndb_push_agg` (OFF) / `ndb_push_agg_jit` (ON).**
Instead of a manual my.cnf edit per run, the differential is now
permanent suite structure, following the `ronsql_cte` ×5 precedent:
- `suite/ndb_push_agg/my.cnf` pins `CompiledInterpreter=OFF` — the
  interpreter arm. (The config default is AUTO = JIT on, so without
  this line the "normal" suite was silently running the JIT.)
- NEW `suite/ndb_push_agg_jit/`: `my.cnf` includes the base suite's
  and overrides `CompiledInterpreter=ON` (mysqltest resolves
  `!include` root-relative; later section values win). All 24
  `rondb_jit_*` canaries moved here (`git mv`, plus a copy of
  `have_ndb_error_insert.inc` — mysqltest resolves bare `--source`
  names against the current file's directory first,
  `mysqltest.cc:3181`). Every non-bench functional test is mirrored
  as a one-line `--source suite/ndb_push_agg/t/<name>.test` wrapper
  with a copied `.result` — test logic lives only in the original,
  and identical baselines passing under both suites IS the always-JIT
  differential. `bench*` tests are deliberately unmirrored;
  `ndb_push_agg_dist` is untouched for now (flipping it OFF without a
  mirror would lose multinode JIT coverage — open item).
- **`testJoinAggNdbApi` split**: its former Tests 25–30 (the JIT
  canaries, which arm 4060 and would abort every data node under
  CompiledInterpreter=OFF) moved to the NEW binary `testJoinAggJit`
  (block_unit_test, own CMake target), renumbered Tests 1–6 with
  dedicated tables renamed `jit3_*`/`jit5_*`/`jit6_*`. The parent
  binary now ends at Test 24 and carries no JIT dependency. Wrappers:
  `testJoinAggJit.test` (whole binary) is new in the jit suite; the
  six `rondb_jit_ndbapi_*` wrappers now exec `testJoinAggJit
  --only 1..6`. Three latent defects found and fixed by the split:
  (a) `rondb_jit_ndbapi_null_sum` had run `--only 25` (= must-compile,
  not null-sum) since the merge renumbering — its recorded baseline
  matched, so the wrong-test run was invisible; now `--only 2`;
  (b) `fakeOkLineForErrorInsertTest`'s JIT entries still carried
  pre-merge numbers (24–29) — production builds would have printed
  wrong/no fake-OK lines; the moved table is renumbered 1–6;
  (c) `createT30Tables` dropped the Test-29 tables instead of its own
  — fixed in the split copy (`createT6Tables` drops `jit6_*`).

**Implemented probes (2026-08-22).**
- `rondb_jit_fallback_census` gained a "JOIN-AGGREGATION compile
  path" section (every pre-existing row ran the STANDALONE path
  only): `join_sum` (inner join, canonical JOIN_AGG_SETUP shape),
  `outer_join_sum` (LEFT JOIN under
  `ndb_join_pushdown_aggregate_outer_join=ON`; NULL-extended rows
  documented as per-row-invisible), `star_two_leaf` (two child
  tables; documents the multi-leaf blind spot — the compile-gate
  skip bumps no counter). Hand-authored `.result`.
- NEW `suite/ronsql_cte/t/ronsql_cte_jit_census.test` — the CTE
  pipeline probes live in the `ronsql_cte` suite because CTE queries
  execute only through the RDRS arm (every CTE body sets
  `$suppress_ronsql_cli`). Two literally-recorded-green queries
  copied from `body_agg.inc` (agg-04 form A: CTE_LOOKUP join +
  re-aggregation; agg-18 form B: CTE_SCAN root), each through the
  strict-diff compare harness (mysqld server-side compute = ground
  truth) and each bracketed by an `include/assert.inc` on the
  `programs_fallback` delta — a reject in either the producer or the
  consumer JOIN_AGG_SETUP fails the test even under `--record`.
  Baseline to be recorded on the first run.

**First census catch (2026-08-22): phantom scan-filter fallbacks from
internal EXIT_OK_LAST scans — FIXED.** The first
`ronsql_cte_jit_census` run failed its cte-jit-1 assert with a
fallback delta of 16 — but the CTE aggregation pipeline was clean
(zero join-agg fallbacks, empty strict diff). The ndbd fallback log
showed `scan-filter bridge rejected (reason=1 detail=22)`: opcode 22
= `EXIT_OK_LAST`. Source: internal scans — `NdbIndexStat.cpp:177`
(index-stat sample scans) and `NdbDictionaryImpl.cpp:7013`
(listEvents) — attach the classic 1-word `EXIT_OK_LAST` program
("accept every row, close the scan"). The Phase 7 scan-filter path
attempted to compile it once per fragment's storedProc (8
partitions × 2 nodes = 16), and each bridge reject bumped the
node-global `programs_fallback` — timing noise that could pollute
ANY counter bracket, in any census. Fix (`DbtupExecQuery.cpp`,
`scanCopyAttrinfo` eligibility gate): the 1-word `EXIT_OK_LAST`
program is classified INELIGIBLE before reaching the bridge — it has
no per-row filtering work to speed up, so "fallback" was the wrong
category. A LONGER program containing `EXIT_OK_LAST` still reaches
the bridge and counts its reject (a real Phase 7 coverage note: the
scan-filter bridge lowers `EXIT_OK` but not `EXIT_OK_LAST`, whose
accept-and-close disposition the invoke contract cannot express —
durable negative unless demand appears).

**Fleet census (fill the Measured column from the 6-0 run —
`programs_fallback` deltas, the fallback log, and where needed 5119
dumps):**

| Fleet test(s) | Compile path | Pre-run analysis | Measured |
|---|---|---|---|
| `testJoinAgg{,NdbApi,Spj,ScanScan,Idempotency}` | join-agg single-leaf | compile (the deliberate-fallback canary is now testJoinAggJit Test 4) | green ×2 arms; the expected kOpSetRegNull reject (r=1 d=30) observed |
| `testOuterJoinAgg{,NdbApi}`, `testMultiOuterJoinAggNdbApi` | join-agg single-leaf | compile; NULL-extended rows interpreter per-row | green ×2 arms; no attributed rejects |
| `testStarJoinAgg{,NdbApi,Spj}` | multi-leaf | compile-gate skip, SILENT (no counter) — 6-3 territory | green ×2 arms; skip stays invisible until 6-3/6-4 |
| `testCte{Dbtc,Lookup,NdbApi,NdbApiFilter,NdbApiOuterJoin,Phase6}` | CTE producer + consumer setups | compile (linked-column programs); CTE WHERE filters never JIT (structural, 6-5) | green ×2 arms; unattributed fleet rejects (see harvest) to pin in 6-2 |
| `testCaseAgg` | join-agg embedded CASE | compile (5A/5J coverage) | green ×2 arms |
| `testVarcharMinMax` | string MIN/MAX | compile (5F-1) | green ×2 arms |
| `testInterpreterTypedRegs` | normal interpreter | mostly outside the agg JIT; scan-filter eligibility only | green ×2 arms |
| `ronsql_cte` ×5 suites | CTE pipeline | compile; probes committed (`ronsql_cte_jit_census`) | census asserts GREEN: producer + consumer setups reject-free, strict diff empty |
| `benchJoinAgg`, `bench_*` | — | perf only, out of census scope | — |

**Fallback-log harvest (2026-08-25, surviving parallel-worker ndbd
logs of the verified JIT-arm sweeps — all suites green, so every line
below is benign; per-test attribution is 6-2's job):**
- `join-agg bridge reason=1 detail=30` (kOpSetRegNull) — the
  permanent deliberate-fallback canary shape. Expected.
- `join-agg bridge reason=1 detail=43` — an EMBEDDED block containing
  normal-interpreter opcode 43 = `READ_AGG_REG_TO_REG`, which the
  embedded translator does not lower. New coverage note from the
  fleet; candidate 6-2 exemption or future lowering.
- `aggregation bridge reason=5 detail=14/12` and `join-agg bridge
  reason=5 detail=13` — `JIT_BRIDGE_REG_OUT_OF_RANGE` at
  kOpSumBigint/kOpMin/kOpCount: fleet programs use register indices
  past the bridge's cap (`BC_MAX_REGS`). A register-count admission
  boundary the SQL planner never crosses; candidate 6-2 exemption or
  cap raise if demand shows.
- `scan-filter bridge reason=1 detail=1` (`READ_ATTR_INTO_REG`) —
  the known Phase 7 v1 subset limit (NULL-branch + comparison
  predicates only). Durable note.
- One worker logged 25–27 cumulative fallbacks — the 10 s rate
  limiter hides multiplicity; counters, not log lines, are counts.

**What it brings.** Confidence the rebase is sound, plus a *measured*
map of JIT coverage over the new territory. That map scopes and ranks
6-2/6-3 by data instead of guesswork.

**Verification (Mikael runs).**
- Build (the new `testJoinAggJit` binary compiles; `testJoinAggNdbApi`
  still compiles after the split).
- Host: `bridge_tests` (141), `coldcall_tests` (34).
- Record the CTE census baseline:
  `./mtr --record --suite=ronsql_cte ronsql_cte_jit_census`
  (the embedded asserts fail loudly on any fallback even while
  recording; the two `== Diff ==` sections must record empty).
- **Interpreter arm**: `./mtr --suite=ndb_push_agg` — now pinned
  `CompiledInterpreter=OFF`. Everything must stay green (proves the
  fleet + functional baselines are JIT-independent, and that
  `testJoinAggNdbApi` post-split runs clean without the JIT).
- **JIT arm**: `./mtr --suite=ndb_push_agg_jit` — the same tests
  mirrored under `CompiledInterpreter=ON`, plus all 24 `rondb_jit_*`
  canaries and the new `testJoinAggJit` (Tests 1–6, incl. the
  repaired null-sum coverage). Identical mirror baselines green under
  both suites is the always-JIT differential.
- `./mtr --suite=ndb_push_agg_dist` unchanged (still JIT-on via the
  AUTO default; the OFF/mirror question there is an open item).
- One RonSQL CTE topology suite: `./mtr --suite=ronsql_cte` (JIT on
  via AUTO default) — CTE-pipeline arm.
- After the JIT-arm sweeps, harvest the fallback log for the census
  table: `grep -h "JIT fallback"
  <build>/mysql-test/var/**/ndbd*/ndbd.log` (reason/detail per
  line; rate-limited to one line per 10s — the counters, not the
  log, are the counts).

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

**Implemented (2026-08-25).** Four guards + one fail-fast:
- `jit_load_col_read`'s local arm returns -1 (→ per-row fallback) on
  null `tablePtrP` BEFORE `readSingleAttributeForJit` and the
  descriptor lookup — one guard covers every load helper via the
  shared prologue.
- `ndb_jit_h_branch_attr_null` and `ndb_jit_h_branch_attr_op_arg`
  set `s->row_fallback = 1` and return "fall through" (their contract
  returns a branch bool, so the -1 convention doesn't apply; the
  row's JIT run is discarded and the interpreter re-run surfaces the
  clean error — the div-by-zero inline-fallback pattern).
- `ndb_jit_h_minmax_str`'s LOCAL arm takes the same inline fallback
  (its linked arm never needs the tuple).
- `dbtup_jit_invoke_scan_filter` now FAIL-FASTS on an unexpected
  `row_fallback` (previously silently ignored): no per-row fallback
  exists on that path and the flag is unreachable for a real scanned
  tuple — if it fires, a helper contract broke, and guessing an
  accept/reject verdict would silently corrupt results either way.
- F8 comment fixes: the two `DbtupJitGlue.hpp` blocks describing the
  pre-Phase-8 `m_n_gb_cols == 0` gate, and `DblqhProxy.cpp`'s
  "each leaf is independently JIT-compiled" (now states the leaf-0
  reality and points at 6-3's entry-switch requirement).

**Deviation from the planned test.** The planned "coldcall test with
a mocked null `tablePtrP`" is not buildable: `coldcall_tests.c`
registers MOCK helpers to exercise stencil call plumbing — the real
helpers are kernel C++ in `DbtupJitGlue.cpp` and are not host-
linkable. The guards are pure armor (unreachable by any program
RonSQL emits today — consumer programs are linked-column-only), so
verification is behavioral no-change across the full suite matrix,
plus the scan-filter fail-fast for the cannot-happen case. A true
end-to-end negative (a hand-crafted CTE consumer program with a
local-column read, expecting a clean query error) needs signal-level
CTE machinery — noted as a candidate `testCte*` addition for 6-2.

**Verification.** Build; host tests (bridge 141, coldcall 34 —
unchanged counts); `./mtr --suite=ndb_push_agg` (OFF arm) and
`./mtr --suite=ndb_push_agg_jit` (ON arm) — all green, no baseline
movement; `./mtr --suite=ronsql_cte ronsql_cte_jit_census` (the CTE
pipeline still reject-free).

**Exit.** Suites unchanged; no JIT code path can deref a null
`tablePtrP`; an impossible `row_fallback` on the scan-filter path
aborts instead of guessing.

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

**Implemented (2026-08-25).** Three pinning instruments, chosen after
the attribution pass showed whole-binary 4060 arming is unsafe (the
10 s rate limiter makes the 6-0 fallback-log harvest INCOMPLETE — a
binary could submit a rejecting program the log never showed, and one
unattributed reject under 4060 = node abort = suite failure):
1. **`ronsql_cte_jit_census` upgraded to must-JIT**: both queries run
   under 4060 (any `m_jit_entry==0` dispatch in the CTE pipeline
   aborts — including a silent multi-leaf compile-gate skip), plus a
   `compile_ns_total`-delta assert per query: join-agg compiles
   bypass the reuse cache, so a pushed pipeline ALWAYS bumps it —
   delta 0 means "not pushed at all", which neither 4060 nor the
   fallback assert can detect. Re-check both compile asserts when
   6-4's cache lands.
2. **NEW `rondb_jit_outer_join_canary`** (ndb_push_agg_jit): the
   outer-join path (RONDB-1035) pinned under 4060 + the same
   compile-activity proof — scalar and grouped shapes copied from the
   proven-pushed `ndb_join_pushdown_agg` Tests 38/40/41, each with a
   mysqld ground-truth run (outer-join pushdown OFF) first.
   NULL-extended rows are 4060-exempt by design (null `block_tup`).
   Inner joins were already pinned by `rondb_jit_canary` Q1-Q3.
3. **Recorded fallback-delta pins on all 25 mirror wrappers**: each
   jit-suite mirror brackets its `--source` with a
   `SUM(programs_fallback)` delta emitted into the recorded baseline
   — the recorded value IS the pin (MTR's diff enforces it forever),
   with no abort risk and no guessed expectations. A delta change in
   either direction fails the test; re-recording is a deliberate
   coverage decision. The recorded values also complete the census
   attribution the logs could not (e.g. which binaries carry the
   REG_OUT_OF_RANGE programs).

**The 6-2 exemption list** (shapes that legitimately do not run on
the JIT; each pinned by the instruments above rather than 4060):

| Shape | Where | Why | Disposition |
|---|---|---|---|
| `kOpSetRegNull` program | `testJoinAggJit` Test 4, `rondb_jit_ndbapi_unsupported_fallback` | THE permanent deliberate-fallback canary | permanent |
| Embedded `READ_AGG_REG_TO_REG` (op 43) | `testCteNdbApi.cpp:~5717` — consumer program comparing two CTE aggregate outputs | embedded translator doesn't lower acc→reg | future-lowering candidate (`JitState.acc_i64` makes it representable); pinned via the recorded delta |
| Register index ≥ `BC_MAX_REGS` (8) | "aggregation bridge reason=5" in the harvest; exact tests fall out of the recorded deltas | bridge admission boundary | raise the cap if the deltas show real demand |
| Scan filters beyond the Phase 7 v1 subset (e.g. `READ_ATTR_INTO_REG`) | fleet + mysqld WHERE shapes | Phase 7 v1 lowers NULL-branch + comparison predicates only | durable note; INELIGIBLE state is 4060-exempt |
| NULL-extended outer-join rows | any outer-join aggregation | only the interpreter can synthesize NULL loads for the missing tuple; 4060-exempt via null `block_tup` | permanent (per-row) |
| Multi-leaf (star) setups | `testStarJoinAgg*`, star/snowflake CTE bodies | compile-gate skip; `m_jit_entry==0` would abort under 4060 — never arm 4060 near star shapes | 6-3 removes |
| CTE WHERE filters | consumer-side filtering | structural: no `scan_rec`, jump-table interpreter only | 6-5 (parked) |
| 1-word `EXIT_OK_LAST` internal scans | NdbIndexStat / listEvents | INELIGIBLE since 6-0 (nothing to speed up; not counted as fallback) | permanent |

**Recorded delta pins (2026-08-25 first recording — now the standing
baselines; nonzero = the test's programs that legitimately stay on
the interpreter):**

| Test | Delta | Reading |
|---|---|---|
| `testInterpreterTypedRegs` | **1888** | the typed-register interpreter matrix probes the Phase 7 scan-filter subset boundary per storedProc per fragment — the dominant reject source by far (the harvest's `REG_OUT_OF_RANGE` + unsupported-opcode families) |
| `ndb_pushdown_agg` | 16 | mysqld standalone-agg shapes still rejecting — worth a targeted census look at WHICH queries (future lowering candidates) |
| `ndb_push_agg_case_null` | 8 | some RONDB-733 nullable-CASE shapes fall back — candidate follow-up |
| `testVarcharMinMax` | 8 | string MIN/MAX shapes beyond 5F-1's fused lowering |
| `ndb_join_pushdown_agg_types` | 6 | typed-column join-agg shapes |
| `ndb_join_pushdown_agg` | 3 | |
| `ndb_join_pushdown_agg_evict` | 2 | |
| `testCteNdbApi` | 2 | the embedded `READ_AGG_REG_TO_REG` consumer program (op 43, `testCteNdbApi.cpp:~5717`) — one per node |
| everything else (17 tests) | 0 | incl. ALL star tests — the multi-leaf skip counts nothing (the known blind spot, 6-3/6-4) — and the whole CTE fleet minus NdbApi |

**Second census catch (2026-08-25): unsigned CTE aggregate slots were
marked SIGNED — the JIT consumer path never actually ran, and the
interpreter consumer carried a latent ≥2^63 correctness bug. FIXED.**
The first must-JIT run of the upgraded CTE census aborted both data
nodes under 4060: `JoinAggInterpreter` (consumer, `m_n_gb_cols=0`)
reached the interpreter loop with a NON-null `m_jit_entry` — a
per-row fallback, on every row. Chain: `AggResItem` stores
unsignedness as a separate `is_unsigned` flag (`type` stays
`NDB_TYPE_BIGINT`); `emitCteLinkedAggSlot` (`DblqhMain.cpp`) encoded
`typeId = item.type`, dropping the flag — the CTE marker said BIGINT
while RonSQL's virt table correctly widens unsigned aggregates to
Bigunsigned, so the consumer program's wire type compiled the u64
load helper, whose runtime type check hit its `default:` fallback arm
on every row. Results stayed correct (interpreter re-run) and NO
counter recorded it — 6-0's fallback census was structurally blind to
it, which is exactly why 6-2 added the 4060 + compile-activity layer.
Worse, the same wrong marker made the INTERPRETER consumer tag the
register signed: MIN/MAX misorder and SUM misreads for values ≥ 2^63
(latent — suite data never crosses it; the same defect class as the
RONDB-733 `m_pushed_value_is_unsigned` fix). Fix: the marker now
encodes `BIGUNSIGNED` when `item.is_unsigned` (one site;
`emitCteOutputAggSlot` emits no marker, GB columns carry true source
types, DBSPJ's synthetic NULLs are never value-decoded).

**Bring-up fix (first run):** the new canary failed MTR's
check-testcase — `>> $NDB_TOOLS_OUTPUT` CREATED the shared
`var/tmp/ndb_testrun.log` on its worker (the pre-existing canaries
all created AND removed it, a self-consistent closed set; the two new
tests appended without removing and broke the invariant). Fixed by
converting ALL 18 arming tests (16 canaries + outer-join canary +
CTE census) to a per-test `$MYSQL_TMP_DIR/<name>_mgm.log` with
`--remove_file` at the end — self-consistent AND immune to any other
writer of the shared file (removing a shared file an earlier test
left behind fails check-testcase just as surely as creating one).

**Deferred within 6-2:** the 6-1 end-to-end negative (a hand-crafted
CTE consumer program with a local-column read, expecting a clean
query error) still needs signal-level CTE machinery — carried as a
candidate `testCte*` addition, not blocking.

**Verification (Mikael runs).**
- Build not required beyond the current binaries (test-only slice).
- Record the 25 delta pins (ONLY the mirrors — the canaries must NOT
  be recorded, their expectations are hand-authored):
  `./mtr --record --suite=ndb_push_agg_jit ndb_join_pushdown_agg
  ndb_join_pushdown_agg_evict ndb_join_pushdown_agg_linked
  ndb_join_pushdown_agg_types ndb_push_agg_case_null
  ndb_push_agg_decimal_minmax ndb_push_agg_unsigned ndb_pushdown_agg
  testCaseAgg testCteDbtc testCteLookup testCteNdbApi
  testCteNdbApiFilter testCteNdbApiOuterJoin testCtePhase6
  testInterpreterTypedRegs testJoinAgg testJoinAggNdbApi
  testJoinAggScanScan testJoinAggSpj testMultiOuterJoinAggNdbApi
  testStarJoinAgg testStarJoinAggNdbApi testStarJoinAggSpj
  testVarcharMinMax`
- Then full normal runs: `./mtr --suite=ndb_push_agg_jit` (twice —
  the second run proves the recorded deltas are stable),
  `./mtr --suite=ndb_push_agg` (OFF arm untouched by this slice),
  `./mtr --suite=ronsql_cte ronsql_cte_jit_census` (now must-JIT).

**Exit (revised).** The census + canaries hold under 4060 with
compile-activity proof; the 25 recorded deltas are stable across two
runs and become the standing coverage pins; the exemption list above
is the committed record of what legitimately stays on the
interpreter.

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
- *Mirror-suite maintenance* (6-0): every re-record of a mirrored
  test must be done in BOTH `ndb_push_agg` and `ndb_push_agg_jit`;
  the one-line `--source` mirrors prevent logic drift, and result
  drift fails loudly.
