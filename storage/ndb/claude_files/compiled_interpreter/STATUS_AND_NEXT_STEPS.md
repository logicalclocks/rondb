# RONDB-1056 Compiled Interpreter — Status & Next Steps

**Updated: 2026-08-22.** Single entry point for resuming work. Branch:
`RONDB-1056-compiled-interpreter`.

> ⚠️ **Docs-vs-reality note.** `plan.md`'s header still says
> *"Status: planning. Code: not started."* That is stale. The JIT is
> implemented and wired end-to-end (~34 real code commits across
> Phases 0–5.0, plus Phase 5.1a). Treat `plan.md` / `phase_*.md` as
> *design intent*; treat the source tree as ground truth.

## Session 2026-08-22 — 26.04-main rebase + Phase 6 plan

The branch was rebased onto latest `26.04-main` (`3ac91d6a023`); the
only conflict was `testJoinAggNdbApi` renumbering (our JIT canaries
shifted +1 again, Tests 24–29 → **25–30**, fixed in `469edceaff1`).
The rebase delivered the parallel pushdown-join + CTE branch's whole
test fleet (`testCte*`, `testStarJoinAgg*`, `testOuterJoinAgg*`,
`testInterpreterTypedRegs`, … + the `ronsql_cte` ×5 topology suites)
— the harness the original Phase 6 was waiting for. **Phase 6 is now
planned: see `phase_6_plan.md`** (post-rebase verification + fleet
census 6-0, CTE-consumer null-`tablePtrP` hardening 6-1, must-JIT
conformance pinning 6-2, multi-leaf/star-join JIT 6-3, join-agg
reuse-cache routing 6-4; CTE-filter JIT 6-5 parked; docs/coordination
6-6). Key survey findings behind it: CTE aggregation already reaches
the JIT (consumer feeds carry a null `tablePtrP` the JIT derefs
unguarded), multi-leaf is compile-gated to leaf 0 with a
single-`m_jit_entry` hazard in the per-row leaf switch, and join-agg
compiles bypass both the reuse cache and the `ndbinfo.jit` compile
counters (direct `jit1_compile` in `DblqhProxy.cpp:2888`). The three
standalone RONDB-733 mysqld fixes (`c7e8dd1ff89`, `f0a3d38d451`,
`3c2c228535f`) survived the rebase and remain branch-local — separate
PR to 26.04-main pending.

**6-0 restructure (Mikael's design): the differential is now suite
structure.** `ndb_push_agg` pins `CompiledInterpreter=OFF` (the
interpreter arm — the config default is AUTO, so it had been running
the JIT); NEW suite `ndb_push_agg_jit` (`my.cnf` !includes the base +
`CompiledInterpreter=ON`) holds all 24 moved `rondb_jit_*` canaries
plus one-line `--source` mirrors of every non-bench functional test
with copied baselines. `testJoinAggNdbApi`'s JIT Tests 25–30 split
out into the NEW binary `testJoinAggJit` (Tests 1–6, tables
`jit3_*`/`jit5_*`/`jit6_*`; the parent binary now ends at Test 24 and
runs under OFF). The split flushed three latent defects: the
`null_sum` wrapper ran `--only 25` (must-compile, not null-sum) since
the merge; the fake-OK table still had pre-merge numbers; and
`createT30Tables` dropped the Test-29 tables. Full record in
`phase_6_plan.md` §6-0. `ndb_push_agg_dist` untouched (open item:
OFF + mirror vs leave JIT-on).

**6-0 DONE & VERIFIED (2026-08-25 — all tests passed):** host tests
green; `ndb_push_agg` green under OFF and the full `ndb_push_agg_jit`
mirror green under ON (the structural always-JIT differential);
`ronsql_cte_jit_census` baseline recorded with both asserts holding.
The census's first catch was COUNTER NOISE, fixed at the source: the
internal 1-word EXIT_OK_LAST scans (NdbIndexStat samples, listEvents)
were being bridge-rejected once per fragment's storedProc, polluting
`programs_fallback` (16 phantom counts in one bracket) — now
classified INELIGIBLE before reaching the bridge
(`DbtupExecQuery.cpp` scanCopyAttrinfo). Fallback-log harvest +
filled fleet census table in `phase_6_plan.md` §6-0 (notables for
6-2: embedded `READ_AGG_REG_TO_REG` rejects, `REG_OUT_OF_RANGE` on
fleet programs past `BC_MAX_REGS`, scan-filter `READ_ATTR_INTO_REG`
= the Phase 7 v1 subset limit).

**6-1 DONE & VERIFIED (2026-08-25 — all tests passed, zero baseline
movement):** the JIT can no longer deref a CTE-consumer feed's null
`tablePtrP`. Four guards, all mapping to the per-row-fallback
convention so the interpreter re-run surfaces the clean
ZAGG_OTHER_ERROR: `jit_load_col_read`'s local arm (returns -1 —
covers every load helper via the shared prologue),
`ndb_jit_h_branch_attr_null` / `ndb_jit_h_branch_attr_op_arg` /
`ndb_jit_h_minmax_str`'s local arm (inline `row_fallback = 1`, the
div-by-zero pattern — their contracts return values, not rcs). Plus
a soundness fix: `dbtup_jit_invoke_scan_filter` FAIL-FASTS on an
unexpected `row_fallback` (previously ignored; no per-row fallback
exists on that path, and guessing accept/reject would silently
corrupt results). F8 stale comments fixed (the two pre-Phase-8
`m_n_gb_cols == 0` gate descriptions in `DbtupJitGlue.hpp`,
DblqhProxy's aspirational "each leaf independently compiled").
Planned coldcall test recorded as NOT BUILDABLE (real helpers are
kernel C++, coldcall_tests uses mocks) — armor verified by
behavioral no-change; end-to-end negative noted as a 6-2 candidate.

**6-2 DONE & VERIFIED (2026-08-25 — all arms green):** conformance
pinning via three instruments (whole-binary 4060 arming rejected —
the rate-limited fallback log makes attribution incomplete):
(1) `ronsql_cte_jit_census` upgraded to MUST-JIT — 4060 + a
compile_ns_total-delta assert per query (join-agg compiles bypass
the cache, so pushed = compiled; delta 0 = not pushed — re-check at
6-4); (2) NEW `rondb_jit_outer_join_canary` (RONDB-1035 shapes under
4060 + compile proof; NULL-extended rows 4060-exempt by design);
(3) recorded fallback-delta pins on all 25 jit-suite mirrors — the
recorded value IS the pin; attribution complete
(testInterpreterTypedRegs 1888 = Phase 7 subset boundary;
ndb_pushdown_agg 16, case_null 8, VarcharMinMax 8 = future-lowering
candidates; testCteNdbApi 2 = embedded READ_AGG_REG_TO_REG; 17
tests at 0). Exemption list committed in `phase_6_plan.md` §6-2.
**The must-JIT census caught a REAL cross-engine bug on its first
armed run**: `emitCteLinkedAggSlot` dropped `AggResItem.is_unsigned`
— unsigned CTE aggregate slots marked BIGINT while RonSQL's virt
table says Bigunsigned, so the JIT's u64 load helper per-row-fell-
back on EVERY consumer row (invisible to all counters, results
correct via interpreter re-run) and the interpreter consumer carried
a latent ≥2^63 sign bug. Fixed (standalone commit, DblqhMain.cpp);
census now proves the CTE pipeline runs native. Also: 18 arming
tests converted to per-test mgm log files (check-testcase state);
ronsql_cte full-suite runs on this machine need reduced parallelism
(--parallel=6 clean; 10 workers hit a machine-wide connect blip).
NEXT: 6-3 (multi-leaf/star-join JIT — the main implementation
slice).

## Session 2026-08-18/19 — crash registry, upstream merge, test renumbering

Three things happened after the 2026-06-22 state described below. **The
post-merge verification checklist at the end of this section has NOT been
run yet — it is the immediate next action.**

1. **Phase 8 item #4 (crash diagnosis) — first half LANDED**
   (`65d9c2fa9ee`, jit1.c only). At compile time each `Jit1Prog` now
   captures a debug sidecar (per-opcode kind + byte-offset table) and
   registers itself in a node-global intrusive list of live programs
   (mutex on compile/free; lock-free traversal, safe from a signal
   handler). `jit1_describe_pc(pc, buf, buflen)` maps a faulting PC to
   the owning blob, its byte offset, and the bytecode opcode. **Still
   inert:** not declared in `jit1.h` and has zero callers. The remaining
   half = export it, call it from the data node's SIGSEGV/error-reporter
   path (`JIT-CRASH:`-prefixed `g_eventLogger` output per plan.md §14),
   add a debug-build DUMP command for live programs, and a host unit test.
2. **Upstream merge** — `26.04-main` (~874 commits) merged;
   `dab718f802c` "Fixed merge issues" resolved committed conflict markers
   and renumbered collisions:
   - `JIT_TABLEID` **55 → 59** (upstream took 55–58 for
     TRANSPORTER_ACTIVITY/RDMA_TRANSPORTERS/SECURITY_VIOLATION*).
   - `CFG_DB_COMPILED_INTERPRETER` **705 → 708** (upstream took 705–707).
   - The `testJoinAggNdbApi` JIT canaries shifted **+1 to Tests 24–29**
     (upstream added "Test 23: fragsPerWorker result equality"):
     must-compile=24, null-sum=25, linked-null=26, unsupported-fallback=27,
     wide-column=28, case-skip=29. The consolidated
     `testJoinAggNdbApi.result` was updated in the merge fix.
   - Source scanned 2026-08-19: no leftover conflict markers, no stale
     references to table id 55 / param 705.
3. **MTR wrapper fix (2026-08-19):** the six `rondb_jit_ndbapi_*`
   wrappers + their `.result` files still used the pre-merge numbers
   (`--only 23…28` + old "Test N:" lines) → all six would have failed.
   Fixed: `--only` bumped to 24…29, result lines + wrapper comments
   renumbered. No other file in the suite uses `--only`.

### 2026-08-19 — EXIT_OK_LAST bug found by the sweep (FIXED, pending verify)

The first post-merge MTR run failed `ndb_push_agg.ndb_pushdown_agg`
(implicit `COUNT(*)` = 44 instead of 12, with SUM/MIN/MAX correct and
wrong under pushdown OFF **and** ON). Root cause — a genuine Phase 7
scan-filter JIT bug, latent since the slice landed:

- mysqld answers implicit no-WHERE `COUNT(*)` from
  `ha_ndbcluster::records()` → `ndb_get_table_statistics`
  (`ndb_table_stats.cc`), whose interpreted scan program is a single
  `interpret_exit_last_row()` = `EXIT_OK_LAST` (opcode 22) plus a
  pseudo-column read region. Semantics: accept the row **and terminate
  the fragment scan** (`handleExitOkLast` sets `req_struct->last_row`)
  → one stats row per fragment.
- The bridge lowered `EXIT_OK_LAST` identically to `EXIT_OK` (plain
  accept, shared case label) — the JIT never signals last-row, so the
  stats scan returned *every* row per fragment, each carrying
  fragment-level ROW_COUNT; the summed `stats.records` was inflated →
  COUNT(*) wrong wherever MySQL substitutes `ha_records()` (Tests
  11–12; Tests 1–10 GROUP BY and Test 13 WHERE unaffected). Optimizer
  row estimates were silently inflated too.

**Fix (in tree):**
1. `ndb_jit_bridge.c`: `BR_EMB_EXIT_OK_LAST` split into its own case →
   `JIT_BRIDGE_UNSUPPORTED_OP` (interpreter fallback). Right ship
   state, not a stopgap: the stats program runs once per fragment, so
   JIT'ing it wins nothing. `bridge_tests.c` T45
   (`scan_filter_exit_ok_last_reject`) locks it in.
2. **Stale-entry bug (found while fixing):**
   `ScanRecord::m_jit_filter_entry` was never reset on scan-record
   reuse (`init_release_scanrec` resets the agg fields but not the JIT
   fields; only pool-release runs the ctor) and `scanCopyAttrinfo` only
   *conditionally* copied it — a reused record could carry a stale
   entry whose code slot was already freed at `deleteScanProcedure`
   (use-after-free of executable memory) into a scan whose own filter
   was bridge-rejected. Fixed: reset `m_jit_filter_entry` /
   `_reject_code` / `_ineligible` in `init_release_scanrec` + the
   copy-fragment setup block, and `scanCopyAttrinfo` now always
   resets-then-publishes.
3. **4060 guard narrowed:** new `ScanRecord::m_jit_filter_ineligible`
   (set when the bridge deliberately rejected the filter) exempts
   deliberate fallbacks from the ERROR_INSERT 4060 abort — necessary
   because mysqld issues stats scans on its own, so a bridge-rejected
   EXIT_OK_LAST scan can appear while a canary has 4060 armed
   cluster-wide. Genuine eligible-but-failed filters still abort.

Files: `jit/ndb_jit_bridge.c`, `test/jit_proto/bridge_tests.c`,
`dblqh/Dblqh.hpp`, `dblqh/DblqhMain.cpp`, `dbtup/DbtupExecQuery.cpp`.
Verify: rebuild `ndbmtd` + `bridge_tests`; run `bridge_tests` (T45),
`./mtr --suite=ndb_push_agg ndb_pushdown_agg` and the JIT canaries
(`rondb_jit_scan_filter_canary` exercises the same translate path and
the 4060 guard).

**Post-merge verification checklist (Mikael runs; nothing built or run
since the merge):**

- Rebuild `ndbmtd`, `testJoinAggNdbApi`, and the host unit binaries.
- Host units (run directly): `bridge_tests`, `admission_tests`,
  `coldcall_tests`, `codemem_tests`, `progcache_tests`, `proto_microbench`
  under `debug_build/storage/ndb/test/jit_proto/`.
- MTR: the `ndb_push_agg` JIT set (the six fixed `rondb_jit_ndbapi_*`
  wrappers + `rondb_jit_canary` + `rondb_jit_embedded_canary` +
  `rondb_jit_must_compile` + `rondb_jit_scan_filter_canary` +
  `rondb_jit_standalone_canary` + `testJoinAggNdbApi`).
- `--record` still outstanding (flagged in the original commits, never
  done): config-enumeration tests (`ndb_config` / config-defaults — new
  `CompiledInterpreter` param) and ndbinfo metadata tests that enumerate
  tables/columns (new `jit` table, id 59).

## NEXT UP: Phase 8 — production readiness (start here after compaction)

**Phase 7 is complete.** The scan-filter JIT runs the full
column-vs-value comparison-predicate family — `col IS [NOT] NULL`,
`col <op> const|?|col2`, integer **and** string — plus standalone +
join aggregation (narrow shapes). All verified (bridge_tests T1–T44 +
`rondb_jit_scan_filter_canary` Q1–Q12). See
`phase_7_comparison_predicates.md` + `phase_7_implementation.md`. Last
commits: `c3e4eacc6eb`/`90f1ae60829` (strings). Branch pushed, green.

**Phase 8 = make it shippable** (full design: `plan.md` §14, line ~1298;
code-memory manager detail: `phase_8_code_memory_manager.md`).
Recommended order:

1. **JIT code-memory manager — the one true ship blocker. IN PROGRESS.**
   Was a **monotonic bump allocator that never frees** (`jit/jit_arena.c`):
   per-DBTUP `ndb_jit_arena_create(1 MB)` at `DbtupGen.cpp:~269` + per-
   `DblqhProxy` arena → a long-running node with many distinct prepared
   statements exhausts it and silently stops JITting forever. **Decided
   (2026-06-17): node-global, two layers, striped locks + a reuse cache.**
   - **Slice 1a — `jit_codemem.{h,c}` DONE (inert, host-tested).** Free-
     capable slot allocator over the kept W^X substrate: size classes
     256 B–8 KB, **each class its own free list + mutex** (striped), slabs =
     `NdbJitArena`s carved into fixed slots, free-list links in heap (so
     `free` never touches code mem / needs no macOS write-toggle), 16 MB
     node cap → OOM returns −1 → interpreter fallback, lock-free execution.
     `+ndb_jit_arena_prepare_write` substrate call. Unit test
     `test/jit_proto/codemem_tests.c` (class selection, alloc/seal/execute,
     reuse-after-free, accounting, cap OOM, validation, global singleton).
     **NOT yet wired into `jit1_compile` or DBTUP.**
   - **Slice 1b — `jit_progcache.{h,c}` DONE (inert, host-tested).** Sharded
     refcounted hash keyed on exact bytecode words (16 shards × 64 buckets,
     striped mutexes, hash+memcmp so no false reuse) → identical programs
     share one blob (sound: blob is pure fn of bytecode; OP_PARAM binds read
     per-row via `param_buf`). `acquire(key,len,pinned,&item)` / `release`
     with a `pinned` retain-at-refcount-0 hint = the RonSQL **PREPARE** reuse
     hook. Decoupled from codemem/NDB via compile/destroy callbacks.
     Diagnostics: live/compiles/hits. Test `progcache_tests.c` (mock-compiler
     cases + a capstone integration test on the real `codemem`: reuse → one
     slot, release-to-zero → slot freed). **NOT yet wired into DBTUP.**
   - **Slice 2 — `jit1_compile` on `codemem` DONE (first live-path change).**
     `jit1_compile(NdbJitCodeMem*, …)` reserves/seals a slot instead of the
     per-block bump arena; `Jit1Prog` records the slot + manager; new
     `jit1_free(prog)` returns it; post-reserve failures `goto fail` and
     free the slot (no leak on compile error). aarch64 cold-call fixup uses
     the slot's rx alias (set at alloc). Kernel call sites (DblqhProxy,
     PushdownInterpreter, DbtupJitGlue) compile via `ndb_jit_codemem_global()`
     — per-block `m_jit_arena`s now dead (removed Slice 3). Host tests
     (admission/coldcall/microbench) migrated. **`jit1_free` not yet called
     by the kernel** → programs still accumulate (in the shared 16 MB pool)
     until Slice 3 wires the lifecycle. Regression net: existing
     `coldcall_tests`/`admission_tests`/`bridge_tests`.
   - **Slice 3a — scan filter through `progcache` DONE (first path that
     frees code memory).** `DbtupJitGlue` owns a node-global scan-filter
     cache (compile cb = bridge translate + `jit1_compile`; destroy cb =
     `jit1_free`; product carries `reject_code`). `dbtup_jit_compile_scan_filter`
     `acquire`s (keyed on filter bytecode words, identical filters dedup),
     dropped its `arena` param, returns the handle;
     `dbtup_jit_release_scan_filter` releases. `storedProc.m_jit_filter_cache_handle`
     set at compile, reset at `scanProcedure`, **released at
     `deleteScanProcedure`** (scan finished). `ndb_jit_progcache` linked into
     `ndbblocks`. Test: `rondb_jit_scan_filter_canary`.
   - **Slice 3b — free join-agg leaf programs DONE.** `DblqhProxy`
     `jit1_free`s each `LeafProgram::m_jit_prog` at both `m_leaf_programs`
     free sites (setup-error + RELEASE), iterating `m_num_leaves`. No
     progcache (the proxy already dedups by `aggStateKey`); the win is
     freeing. Workers are done by RELEASE so no blob is executing.
   - **Slice 3c — standalone agg through `progcache` DONE (last leaking
     path closed).** Second node-global cache (`agg_cache`) in `DbtupJitGlue`
     (`dbtup_jit_compile_agg`/`_release_agg`). `Create` acquires only when
     `n_gb_cols()==0` (GROUP BY never uses the JIT entry), stores the handle
     via `AggInterpreterBase::setJitCacheHandle`, released in
     `~AggInterpreterBase` (runs on both fast + chunked teardown). nullptr
     for join-agg → no double-free with 3b. Test: `rondb_jit_standalone_canary`.
   - **Slice 3d — remove dead per-block `m_jit_arena`s DONE.** Deleted
     `Dbtup::m_jit_arena`/`getJitArena`, `DblqhProxy::m_jit_arena`, the
     `Create` `jit_arena` param + the `NdbJitArena` fwd-decls; join-agg gate
     simplified to `m_num_leaves == 1`. The `ndb_jit_arena` *library* stays
     (substrate under `jit_codemem`); only per-block instances are gone.
     **Phase 8 item #1 COMPLETE — the ship blocker is fully resolved.**
   - **Slice 4 — RonSQL reusable-program pinning DONE (2026-08-19,
     pending verify).** The reserved aggregation-header word `prog[3]`
     became a flags word: `AGG_PROG_FLAG_REUSABLE` (bit 0,
     `NdbAggregationCommon.hpp`) = "client re-sends this program".
     `NdbAggregator::set_reusable_program(true)` sets it (RonSQLPreparer
     marks its aggregators; mysqld never does → ad-hoc SQL unpinned);
     `peekProgramHeader` parses it; `PushdownInterpreterFactory::Create`
     passes it as the agg-cache acquire's `pinned` → the entry survives
     refcount 0 and re-sent identical RonSQL programs cache-hit instead
     of recompiling. Old release-build nodes ignore the bit (only
     DetectType's bit 31 is read); the join-agg proxy path has no cache
     and ignores it. Full detail: `phase_8_code_memory_manager.md`
     Slice 4. **Verify:** rebuild (API + kernel: NdbAggregator,
     RonSQLPreparer, ndbmtd), run the same RonSQL aggregation twice via
     `ronsql_cli`/RDRS with pauses between, check `ndbinfo.jit`:
     `programs_cached` stays ≥ 1 after the first run completes and
     `programs_reused` bumps on the second; a plain mysqld
     `SELECT SUM(...)` still drops `programs_cached` back after the
     scan ends (unpinned). Host `progcache_tests` already covers
     pinned-survives-refcount-0.
2. **Config param `CompiledInterpreter` — DONE (2026-06-22).** New
   `[DB]` config.ini parameter `CompiledInterpreter` (`CI_ENUM`,
   `CFG_DB_COMPILED_INTERPRETER=708` post-merge, was 705): **OFF** =
   interpreter only;
   **AUTO** (default) = JIT every eligible program (current behaviour);
   **ON** = reserved (== AUTO today). Named `CompiledInterpreter` (not the
   plan's `JoinAggCompiledInterpreter`) since the JIT now covers scan
   filters + standalone + join aggregation. `dbtup_jit_set_mode` /
   `dbtup_jit_enabled` (node-global flag in `DbtupJitGlue`, default AUTO)
   read once in `DblqhProxy::execREAD_CONFIG_REQ`; all three compile sites
   (scan filter, standalone agg, join agg) return no JIT program when OFF
   → interpreter. Startup param (effective at node start; no runtime MGM
   SET wired). typelib + def in `ConfigInfo.cpp`, key + value `#define`s in
   `mgmapi_config_parameters.h`. **NOTE:** config-enumeration tests
   (`ndb_config` / config-defaults `.result`) need `--record` (one new
   param). Default AUTO keeps the existing JIT canaries green.
3. **NDBINFO counters — DONE (2026-06-22).** New node-global ndbinfo table
   `ndbinfo.jit` (kernel `JIT_TABLEID=59` post-merge, was 55), one row per
   data node:
   `code_reserved_bytes`, `code_used_bytes`, `code_slots_live` (from the
   code-memory manager) + `programs_compiled`, `programs_reused`,
   `programs_cached` (summed over the scan-filter + agg reuse caches). New
   `dbtup_jit_get_stats(NdbJitStats*)` in `DbtupJitGlue` reads the node-
   global codemem/progcache diagnostics; emitted from
   `Dbtup::execDBINFO_SCANREQ` (`DbtupDebug.cpp`) gated to `instance()==1`
   (one row/node). Table def + array entry in `NdbinfoTables.cpp`; enum in
   `Ndbinfo.hpp`. **Correction (2026-08-19):** only the base table
   `ndb$jit` is auto-generated from the kernel metadata — the clean-named
   `ndbinfo.jit` VIEW needed a manual entry in the (name-sorted, asserted)
   `views[]` list in `plugin/ha_ndbinfo_sql.cc`; it was missing until the
   compound-canary run failed with "Table 'ndbinfo.jit' doesn't exist".
   Added 2026-08-19. NOTE: this re-invalidates the freshly recorded
   ndbinfo metadata results (`1a8c3a3b257`) — one more `--record` needed
   after rebuilding mysqld (new view in the listings).
   **Deferred counters — DONE (2026-08-19, pending verify).** The three
   deferred columns are wired: `ndbinfo.jit` grew `programs_fallback`
   (compile attempts that produced no program — bumped at all six
   reject points across the scan-filter/agg compile callbacks and
   DblqhProxy's join-agg site; NB deliberate fallbacks like the
   EXIT_OK_LAST stats scan recur per occurrence, so the counter runs
   hot by design), `rows_executed` (per-row — counted via
   cache-line-padded per-thread slots with plain load/store, NOT a
   shared atomic, so the ~11 ns/row JIT budget is untouched; slots
   summed at read), and `compile_ns_total` (Jit1Timing.total_ns now
   requested at all three `jit1_compile` sites). Table is now 10
   columns (`DECLARE_NDBINFO_TABLE(JIT, 10)`); `NdbJitStats` reordered
   Uint64s-first. Plus plan.md §14 "Error reporting":
   `dbtup_jit_note_fallback` rate-limit-logs one
   "RONDB-1056 JIT fallback: <site> rejected (reason/detail)" line per
   10 s via `g_eventLogger` (CAS-guarded timestamp). NOTE: the ndbinfo
   metadata `--record` must happen AFTER this lands too (3 new
   columns + the view, one batch). Verify: rebuild `ndbmtd` + `mysqld`,
   run a JIT query, `SELECT * FROM ndbinfo.jit` — rows_executed > 0,
   compile_ns_total > 0; a stats-scan-heavy workload moves
   programs_fallback; check the node log for the rate-limited line.
   **NOTE:** ndbinfo metadata MTR tests that enumerate all tables/columns
   need `--record` (one new table). Not yet wired: `runs` / `fallbacks` /
   `compile-ns` (need per-row + compile-time instrumentation; deferred).
4. **SIGSEGV sidecar — IMPLEMENTED (2026-08-19, pending verify).**
   Registry + sidecar + `jit1_describe_pc` (`65d9c2fa9ee`) now wired:
   `jit1_describe_pc` + `jit1_registry_dump` exported in `jit1.h`;
   `DbtupJitGlue` owns a process-wide SA_SIGINFO interposer for
   SIGSEGV/SIGBUS/SIGILL/SIGFPE that extracts the faulting PC from the
   ucontext (Linux + macOS, x86_64 + arm64), logs the `JIT-CRASH:` line
   (raw `write` to stderr first, then `g_eventLogger`), and chains to
   ndbd's `handler_error` so the normal ErrorReporter crash path is
   unchanged. Installed lazily at the first JIT compile (pthread_once;
   all three compile sites call it) — `CompiledInterpreter=OFF` never
   touches signal handling. New `DUMP 2383` (`TupDumpJitPrograms`,
   works in all builds, `instance()==1`-gated) logs one `JIT-DUMP:`
   line per live program via `dbtup_jit_dump_programs`. Host coverage:
   `coldcall_tests` T12 (describe resolves live blob PCs, stops after
   `jit1_free`, dump line count drops by one); T2–T11 also got the
   missing `jit1_free` calls (they leaked registry entries with
   dangling rx ranges). Files: `jit/jit1.{h,c}`,
   `DbtupJitGlue.{hpp,cpp}`, `DblqhProxy.cpp`, `DumpStateOrd.hpp`,
   `DbtupDebug.cpp`, `test/jit_proto/coldcall_tests.c`. **Verify:**
   rebuild `ndbmtd` + `coldcall_tests`; run `coldcall_tests` (T12);
   optionally `ndb_mgm -e "all dump 2383"` against a live cluster
   after a JIT query, and check the node log for `JIT-DUMP:` lines.
   Deviations from plan.md §14: DUMP works in production builds too
   (read-only, harmless, more useful); the type-state snapshot is not
   in the sidecar (Phase 5's lattice doesn't exist yet); per-register
   state and the 16-byte hex window are not dumped (the byte offset +
   op kind + blob range cover the diagnosis need).
5. **GROUP BY gate lift — DONE & FULLY VERIFIED (2026-08-19).**
   `rondb_jit_groupby_canary` passes post-regen (grouped SUM/COUNT JIT
   under 4060 + counter deltas; 500-group differential; grouped
   join-agg; CASE-falls-back and >4-aggregates negatives), **and the
   full `ndb_push_agg` suite passes** — the existing grouped tests
   dispatching through the JIT are the broad regression net.
   **Phase 8 is complete.** All six slices in tree; the
   generated stencil headers were regenerated for `OP_COUNT_BIGINT` +
   the `JitState::value_unsigned[]` unsigned-result mask. Shipped: (1) `op_count_bigint`
   stencil + `BR_kOpCount` bridge case + `value_unsigned` writeback
   (`is_unsigned` mirrored per result — COUNT is unsigned BIGINT);
   (2) compile gate dropped in `Create`; (3)+(4) both dispatch gates
   now `m_jit_entry != nullptr` only; (5) `rondb_jit_groupby_canary`
   (Q1 grouped SUM, Q2 COUNT+SUM, Q3 CASE per-group NULL, Q4 500-group
   differential, Q5 grouped join-agg — 4060-only proof, the proxy path
   has no progcache; Q6 >4-aggregates negative); host tests
   bridge_tests T46/T46b + coldcall_tests T13. Verify order + full
   notes in `phase_8_groupby_gate_lift.md`'s status header. Full plan:
   `phase_8_groupby_gate_lift.md`. Key finding from
   the planning analysis: the lift is much smaller than this item
   historically assumed — the group prologue (GB column reads, xfrm
   hash find/insert, group-record alloc) is block-level C++ *before*
   the interpreter loop, not bytecode; `dbtup_jit_invoke` already takes
   `agg_res_ptr` as a parameter; the glue's `value_updated` writeback
   already gives per-group SQL-NULL semantics; and the compiled/cached
   region (`agg_prog + bc_off`) already excludes GB metadata (blob
   sharing across different GROUP BY column sets is sound). Slices:
   (1) lower `kOpCount` — the bridge has no case for it today, so the
   canonical `grp, COUNT, SUM` query would still reject (the one
   stencil-regen item; MIN/MAX stay Phase 5 — Test 27/28 depends on
   MAX rejecting); (2) lift the compile gate in
   `PushdownInterpreterFactory::Create`; (3)+(4) lift the two dispatch
   gates (standalone + join-agg, keeping `m_num_leaves == 1`);
   (5) `rondb_jit_groupby_canary` (4060 + ndbinfo counter deltas +
   high-cardinality differential + >4-aggregates negative); (6) docs +
   bench (honest expectation: smaller win than scalar — the hash find
   stays interpreted).

**Phase 5A — DONE & VERIFIED (2026-08-19; full ndb_push_agg sweep
green after three post-first-green debugging rounds — see
`phase_5_roadmap.md` §5A findings: untyped READ_ATTR decode fixed
(INT columns read as 8 bytes → garbage compares), kOpSkip lowered
(outer CASE-arm jumps), zero-skip output blocks now emit their
disposition jump (mid-stream fall-through), and the proxy error
inserts renumbered 5119/5120 — 4xxx never routed to DBLQH).** SQL CASE
aggregation now JITs: embedded `READ_ATTR`/`LOAD_CONST64`/
`BRANCH_*_REG_REG` lowered (agg mode only, reusing the Phase 1 hot
branch stencils — **no regen**), `LOAD_CONST16` also materializes its
register, and TWO latent bugs fixed along the way: (1) **production
crash** — the load helper abort()ed on NULL column values, so
`SUM(nullable_col)` with a NULL row crashed the node; replaced by a
per-row interpreter fallback (`JitState::row_fallback` →
`NDB_JIT_ROW_FALLBACK` → the row re-runs on the interpreter, exact
null semantics; new `rondb_jit_nullable_canary` incl. all-NULL-group
per-group NULL); (2) the aggregate `value_updated` mask index was a
per-op ordinal instead of the wire `agg_index` — wrong for multi-arm
CASE (several Sum ops, same aggregate). Groupby-canary Q3 flipped to
must-JIT. Details: `phase_5_roadmap.md` §5A. Verify: rebuild ndbmtd +
host tools; bridge_tests (T47 family + updated T22b/c), coldcall;
mtr groupby + nullable canaries + full ndb_push_agg + testCaseAgg.

**Phase 5C — COMPLETE: 5C-1, 5C-2 AND 5C-3 all DONE & VERIFIED
(2026-08-20; regen clean, double + unsigned canaries green, full
ndb_push_agg sweeps to completion). `phase_5c_plan.md`.**

**Phase 5D — 5D-1 DONE & VERIFIED (2026-08-20; regen clean, bridge
79/79 + coldcall 24/24, nullable canary green under 4060 incl. the
nullable-expression query, full sweep to completion —
`phase_5d_plan.md` carries the implementation record: 1 stencil
`op_load_col_ndb_nb` + helper + the bridge's `nb_convert_loads` taint
post-pass with per-load degradation). 5D-2 (f64/u64 null-branching
siblings) DONE & VERIFIED (2026-08-20; regen clean, bridge 81/81 +
coldcall 26/26, double canary Q9 + unsigned canary Q6 green under
4060, full sweep to completion). 5D-3 (embedded READ_ATTR) deferred
until fallback data — otherwise Phase 5D is complete. Remaining
Phase 5 sub-phases RE-RANKED by the fallback census (2026-08-20, see
`rondb_jit_fallback_census` + roadmap): 5C-4 unsigned arithmetic
DONE & VERIFIED (2026-08-20; regen clean, bridge 85/85 + coldcall
29/29, unsigned canary Q5 green as 4060 must-JIT, census
unsigned_arith flipped to 0, full sweep to completion — 3 u64
checked-arith stencils reusing the ADD/MINUS/MUL holes, BR_REG_NNC
nonneg-constant tracker state, signed/unsigned arith classifier).
Phase 5G DECIMAL loads DONE & VERIFIED (2026-08-20; regen clean,
bridge 91/91 + coldcall 30/30, decimal canary green on all three
tracks, census decimal_sum flipped to 0, double canary Q7 flipped
from negative control to must-JIT, full sweep — 1 cold-call stencil +
bin2decimal helper routing scale statically into the BIGINT/u64/
DOUBLE tracks, plus GENERIC kOpSum/kOpMin/kOpMax lowering via the
tracker). Verification uncovered and FIXED a second RONDB-733-class
mysqld bug: pushed MIN/MAX over DECIMAL printed NULL —
Item_sum_hybrid::val_str's pushed branch fell through to the
never-populated value cache for non-string values; fixed +
backportable test ndb_push_agg_decimal_minmax (see roadmap §5G).
→ 5F-1 string MIN/MAX DONE & VERIFIED (2026-08-20 — 1 fused
cold-call stencil + jitMinMaxStringCol facade reusing minMaxString
exactly; bridge fusion + BR_REG_STR/BR_ACC_STR; bridge 96/96,
coldcall 31/31; rondb_jit_string_canary with collation-discriminating
values green; census string_min flipped to 0; mixed programs stay
hot; nullable strings 4060-safe day one; full sweep passed). The
census scoreboard is now FULLY GREEN for every shape the SQL planner
pushes. → 5D-3 DONE & VERIFIED (2026-08-20 — NO regen; bridge 102/102,
all canaries + census + full sweep green:
found + fixed two pre-existing mysqld bugs for nullable CASE
condition columns (integer conditions ERRORED with NDB 1872 on NULL
rows — no null guard; string NE counted NULL as matching —
NULL_CMP_EQUAL): planner now emits BRANCH_REG_EQ_NULL guards
(backportable standalone commit + portable ndb_push_agg_case_null
test) and the bridge fuses READ_ATTR + guard into the existing
OP_LOAD_COL_NDB_NB with a null-path safety scan; bridge 102/102,
census case_nullable green / case_string documents the string-
condition gap, new rondb_jit_case_nullable_canary). The canary's
first run ALSO surfaced that the OUTER kOpLoadCol admission is
BIGINT-only — every narrow-INT-column aggregate is a whole-program
fallback although the load helpers already decode every width
(census probe int_sum; likely the highest-demand gap left and a
small bridge-admission slice). → 5H narrow-int admission DONE &
VERIFIED (2026-08-21 — NO regen, NO mysqld change: signed
widths → i64 track, unsigned widths → u64 track per the
interpreter's IsUnsigned split; u64 helpers decode all unsigned
widths; bridge 108/108; new rondb_jit_int_canary with boundary
values on all 8 widths; census int_sum flipped to 0, full
sweep passed — see phase_5_roadmap.md §5H). → 5E PLANNED
(2026-08-21, `phase_5e_plan.md`): scoping against RonSQL — the
platform that emits these opcodes — found the REAL gap: RonSQL emits
GENERIC kOpPlus/kOpMinus/kOpMul/kOpDiv (NdbAggregator::Add etc.), and
the bridge only lowers the typed families, so EVERY RonSQL query with
any arithmetic falls back wholesale today. 5E-1 = generic-arith
lowering through the existing classifiers (NO regen) + the RonSQL
test platform (RONSQL_CLI in mtr, rondb_jit_ronsql_canary with 4060 +
mysqld differentials); 5E-2 = integer DIV/MOD (4 hot stencils, regen;
div-by-zero → per-row fallback like op_div_f64, LLONG_MIN/-1 →
overflow-exit with explicit pre-divide guards for the x86 idiv trap;
repoint Test 27 to kOpSetRegNull); 5E-3 = generic '/' over int
operands (1 cold-call stencil implementing RegDivReg's ±2^53
conversion guards; edges → per-row fallback). 5E-1 DONE & VERIFIED
(2026-08-21 — NO regen, bridge 116/116 + canary green: generic-arith bridge case +
RONSQL_CLI in mtr + rondb_jit_ronsql_canary, bridge 116/116.
CORRECTION found in review: the data node's OptimizeProgramBuffer
already rewrites generic arith to typed BEFORE compilation when
operand types are inferable, so RonSQL's int/double arithmetic
already compiled — the LIVE 5E-1 win is DECIMAL arithmetic, which
the optimizer leaves untyped but the bridge's 5G decimal typing can
lower (canary Q7/Q8, T62g/h); Q1-Q5 are pipeline regression
canaries). → 5E-2 DONE & VERIFIED (2026-08-21 — regen clean,
bridge 123/123, canaries + census + full sweep green; 4 stencils: OP_DIV_INT_CHECKED / OP_MOD_INT / OP_DIV_U64 /
OP_MOD_U64; divisor-0 → per-row fallback, INT64_MIN/-1 pre-divide
guards (x86 idiv trap), MOD result-signedness follows the dividend;
bridge 123/123; Test 27 repointed to kOpSetRegNull (permanent);
RonSQL canary Q9-Q13 incl. negative-operand truncation, div-by-zero
NULL semantics, and the overflow error under 4060). → 5E-3 DONE & VERIFIED
(2026-08-21 — regen clean, all tests green; 1 cold-call stencil
OP_DIV_CONV_F64 + ctx-free helper: RegDivReg's ±2^53 conversion
guards, zero-divisor NULL, non-finite — all edges per-row fallback;
both-F64 stays hot; bridge 128/128, coldcall 32/32; RonSQL canary
Q14/Q15). PHASE 5E COMPLETE — the RonSQL/API opcode space is
covered except the documented durable negatives (string CASE
conditions, double trunc-DIV/fmod, mixed int/double +/-/*,
kOpSetRegNull). → 5F-2 DONE & VERIFIED (2026-08-21 —
NO regen, bridge 131/131, linked canary + full sweep green: scoping found the bridge rejected AGG_LINKED_COL_FLAG
entirely, so ALL parent-table/CTE-column aggregates in pushed joins
fell back, every type — not just strings. JoinAggInterpreter's
linked walk extracted into a shared core + JIT facades; one shared
glue prologue routes all 7 load helpers local-vs-linked; string
fusion routes to jitMinMaxStringLinked; bridge admits flagged cols
for every family. new rondb_jit_linked_canary). PHASE 5F COMPLETE. With 5E and 5F
both closed, every parked coverage item is done — remaining
unlowered surface is only the documented durable negatives; → 5I mixed
int/double arithmetic DONE & VERIFIED (2026-08-21 — regen clean,
all tests green; 1 cold-call stencil OP_ARITH_CONV_F64: the kernels'
guard-free casts + op + isfinite, errors via per-row fallback;
closes the TPC-H Q9 mixed-decimal shape; bridge 134/134, coldcall
33/33; RonSQL canary Q6 flips to must-JIT; census mixed_arith
probe; new rondb_jit_mixed_canary). → 5J string CASE
conditions DONE & VERIFIED (2026-08-21 — NO regen, all green: the
block was plumbing — embedded BRANCH_ATTR_OP_ARG now carries the
ABSOLUTE instruction offset (attr_op_arg_base) and dbtup_jit_invoke
points ctx->prog_buf at program + start_pos; bridge 136/136;
case-nullable canary Q4/Q5 upgrade to 4060 must-JIT; census
case_string flips to 0 — the census is then FULLY green for every
planner-pushed shape, no exceptions). → 5K temporal MIN/MAX
DONE & VERIFIED (2026-08-21 — NO regen, all green: the five
temporal ids join the u64 track, helpers grow the interpreter's
exact packed-value decode arms incl. the *2 big-endian fold; bridge
138/138; RonSQL canary Q16-Q18 with a UTC-vs-session-tz caveat on
TIMESTAMP). → 5L double trunc-DIV/fmod
DONE & VERIFIED (2026-08-21 — regen clean, all tests green; 1
cold-call stencil OP_DIVMOD_CONV: trunc-DIV retypes into the signed
i64 track with out-of-int64 truncations falling back, fmod stays
F64; typed kOpDivIntBigint-with-f64 stays rejected; bridge 141/141,
coldcall 34/34; RonSQL canary Q19/Q20 incl. negative
toward-zero truncation). After 5L the ONLY unlowered opcode is
kOpSetRegNull — the permanent unsupported-fallback canary, by
design. Next milestone: Phase 6 (the pushdown-join + CTE branch
merge). NEXT → 5F-2 stays
parked pending demand data; string CASE conditions
(BRANCH_ATTR_OP_ARG per-block prog_buf plumbing) noted as future
work; Phase 6 when merged with the pushdown-join + CTE branch.** Key planning
finding: no null-tracking matrix needed — the null test FUSES into
the load's cold call (helper returns "was null", the load stencil
becomes a cold-call branch in op_branch_attr_eq_null's proven shape)
whose taken edge skips the register's consumer chain, reproducing the
kernels' null-skip exactly. Bridge computes the skip range by a taint
walk with PER-LOAD graceful degradation to today's row-fallback load
(non-contiguous range / embedded block) — no caller-side nullability
metadata, no regression possible. Slices: 5D-1 i64 (1 stencil;
nullable canary upgraded to 4060 must-JIT), 5D-2 f64/u64 siblings,
5D-3 embedded READ_ATTR (deferred until fallback data shows demand).
**5C-3**: aggregation over BIGINT UNSIGNED columns JITs. 4 stencils
(**regen required**): u64 load cold-call (new helper, descriptor-
BIGUNSIGNED-only), carry-checked u64 SUM (accumulates past 2^63 where
a signed add would falsely overflow), u64 MIN/MAX (unsigned compares —
signed ones would order values >= 2^63 as negative) — reusing the
SUM_*/MM_* fold holes; writeback via the existing value_unsigned mask.
Bridge: tracker grows BR_REG_U64 (BIGUNSIGNED loads/consts); the
BIGINT-track aggregates dispatch signed-vs-unsigned on the PROVEN
register type (the optimizer folds BIGUNSIGNED into the BIGINT track;
kernels dispatch on register is_unsigned at runtime); signed arith
rejects u64 (SUM(u+1) falls back whole); new acc-family guard rejects
mixed signed/unsigned/double arms feeding one agg slot (per-row
writeback masks can't represent the interpreter's dynamic per-row
signedness). Tests: bridge 74, coldcall 23, new
`rondb_jit_unsigned_canary` (boundary values 2^63+5 / 2^64-1 for
MIN/MAX, SUM past 2^63 — verification uncovered a PRE-EXISTING
pushdown bug, not JIT-related: mysqld's pushed-SUM/AVG consumption
routed Bigunsigned results through a signed int64 with no unsigned
flag, printing sums past 2^63 signed-wrapped under pushdown with
interpreter and JIT alike; **FIXED in the standalone backportable
commit `d815f2c1d18`** — sql-layer `m_pushed_value_is_unsigned` +
unsigned setters, honored by every converting reader; JIT-free test
`ndb_push_agg_unsigned`; details in `phase_5c_plan.md` §5C-3). The sweep also flushed
out a LATENT node crash (since the outer-join merge, not 5C-3's
doing): the join-agg JIT dispatch deref'd the nullptr req_struct on
outer-join NULL-EXTENDED rows — fixed by never dispatching such rows
to the JIT (`!m_null_local_columns`; the interpreter alone can
synthesize NULL loads for the missing tuple). Verify:
regen-stencils, rebuild, host tests, unsigned canary, full sweep. Key
planning finding: the original design's full type-state lattice is
unnecessary — `OptimizeProgramBuffer` pre-types the bytecode on both
compile paths, so a linear bridge-side register-type tracker suffices.
**5C-1**: `reg_type[8]` tracker in the outer translate walk with the
new `JIT_BRIDGE_TYPE_MISMATCH` reject reason (i64 consumers reject
proven-f64 operands; embedded blocks invalidate; behavior-neutral
until 5C-2's f64 producers). **5C-2**: the DOUBLE family —
aggregation over DOUBLE/FLOAT columns JITs. 8 new stencils
(**regen-stencils required**): `op_load_col_ndb_f64` (new helper;
FLOAT promotes, NULL → per-row fallback), `op_{add,minus,mul,div}_f64`
(bit-pattern finiteness check → the shared `HOLE_F64_OVF_TGT` /
`OP_OVERFLOW_EXIT`; **no** `.rodata` FP constants — extractor can't
relocate them), `op_{sum,min,max}_f64` (first-row init via
value_initialized — preserves single-row SUM(-0.0)'s sign — plus the
new `JitState::value_double[]` writeback mask → `NDB_TYPE_DOUBLE`
results). f64 values live BIT-CAST in regs_i64/acc_i64 (no new
accessor machinery — memcpy reinterpret in the stencil source).
Div-by-zero (kernel: result register → NULL) sets `row_fallback`
INLINE and continues — the planned OP_ROW_FALLBACK_EXIT terminator
proved unnecessary. Generic `kOpDiv` lowers when both operands are
proven f64 (the optimizer never rewrites it to kOpDivDouble). One
5C-1 correction: COUNT's operand type check removed —
`AVG(double_col)` is SumDouble + Count over the SAME f64 register and
COUNT never reads the bits (bridge T51g). Tests: bridge 68, coldcall
20, new `rondb_jit_double_canary` (Q1–Q8; all five must-JIT asserts
held — the planner pushes every double shape the canary assumes).
Verification findings: the extractor's `classify_tail` needed the five
f64 overflow-branch stencils listed for KEEP_ALL (they lack the
`_checked` name pattern the policy keyed off), and the div-by-zero
canary query's "Division by 0" warnings are suppressed (count is an
evaluation-frequency implementation detail).

**Phase 5B — DONE & VERIFIED (2026-08-19; full ndb_push_agg sweep
green post-regen).**
MIN/MAX BIGINT: two new stencils (**regen-stencils required**) with
first-row-init via the new `JitState::value_initialized[]` input mask
(glue sets it per row from `AggResItem::type`; per-group on the grouped
path), bridge cases for the optimizer's typed kOpMin/MaxBigint
rewrites, the Test 27/28 fallback canary repointed to
`SUM(amount % amount)` (kOpMod, durable until 5E), and canary/host
coverage (bridge T49/b, coldcall T14 garbage-accumulator init test,
groupby Q7, nullable Q2+MIN). Details + verify order:
`phase_5_roadmap.md` §5B.

**Roadmap decided 2026-08-19 (Mikael):** GROUP BY gate lift ✅ done →
**Phase 5, NOW CURRENT — sub-phased plan in `phase_5_roadmap.md`**
(supersedes the monolithic sequencing of `phase_5_implementation.md`,
which stays the stencil-level reference): 5A embedded
READ_ATTR/LOAD_CONST64/BRANCH_REG_REG (unblocks SQL CASE — mostly
bridge work reusing the Phase 1 REG_REG branch stencils; flips
groupby-canary Q3), 5B MIN/MAX BIGINT (first-row-init input mask;
repoint the Test 27/28 fallback canary to kOpMod), 5C type-state
lattice + DOUBLE family (the big one), 5D nullable columns + register
null-tracking, 5E int div/mod (NULL-on-zero, needs 5D), 5F string
MIN/MAX. Order after 5A is provisional — re-rank between sub-phases
from `programs_fallback` + the fallback log on real workloads. **Phase 6** (`--force-jit` differential
conformance) is handled when this branch merges with the parallel
pushdown-join + CTE work that carries the interpreter test-program
harness — not scheduled independently.

**Other open tracks** (for reference): **Phase 6**
cross-branch always-JIT `--force-jit` differential testing (high
confidence; needs the parallel interpreter-test-program branch merged);
**Phase 5 (full)** type-state lattice + ~70–75 aggregation stencil matrix
(broader aggregation expressions; needs regen). **Cheap leftover —
DONE & VERIFIED (2026-08-19):** compound scan predicates JIT. MTR
`rondb_jit_compound_canary` (ndb_push_agg) **passed** against the
hand-written `.result`: Q1 `a > 5 AND b < 'x'`, Q2 int range AND, Q3
int OR, Q4 `IS NULL OR >`, Q5 3-way mixed AND — each proven two ways,
ERROR_INSERT 4060 (compiled ⇒ must execute JIT; the
`m_jit_filter_ineligible` exemption means 4060 alone can no longer
prove compilation) **plus** an `ndbinfo.jit`
`programs_compiled + programs_reused` delta ≥ 1 (the progcache counts
only successful compiles, so rejected acquires — e.g. stats scans —
never pollute it). Findings: the OR shapes (Q3/Q4) compile too — the
bridge admits NdbScanFilter's full AND/OR forward-branch layout — and
Q8 confirmed LIKE stays on the interpreter with exactly-0 counter
movement, validating the counter methodology. The run also flushed out
that the `ndbinfo.jit` VIEW was missing (see the correction in item 3
above).

## Latest implementation — Phase 7 scan-filter runtime glue (2026-06-10)

First vertical slice of the SCAN_FRAGREQ scan-filter runtime wiring —
makes the inert `ndb_jit_bridge_translate_scan_filter` + `OP_FILTER_REJECT_EXIT`
groundwork actually run. A pushed-down `WHERE` filter is now compiled once
per stored procedure at scan setup and executed natively per row instead of
`interpreterNextLab()`. **Implemented + verified 2026-06-10** — Mikael
reported `bridge_tests` (incl. T38) and the `rondb_jit_scan_filter_canary`
MTR all passing after two bring-up fixes (reject code 626 not 899; EXIT_OK
lowering — see below). Full design + file map in `phase_7_implementation.md`.

- **Compile-once on storedProc:** `struct storedProc` gains
  `m_jit_filter_state` (untried/compiled/ineligible) + `m_jit_filter_entry`;
  reset in the ZSCAN_PROCEDURE init so a pooled record can't reuse a stale
  program's filter. `Dbtup::scanCopyAttrinfo` compiles from
  `cachedLinearAttrInfo` (exec region = `cache[5+cache[0]]`, len `cache[1]`)
  in a new `!m_has_pushdown` branch, then copies the entry onto
  `Dblqh::ScanRecord::m_jit_filter_entry` for fast per-row access.
- **Per-row dispatch:** `Dbtup::interpreterStartLab` runs the JIT filter
  when the scan record carries an entry; reject ⇒
  `TUPKEY_abort(req_struct, TUP_NO_TUPLE_FOUND=626)` (the scan "row
  filtered" disposition — Dblqh.hpp's recommended filter-reject code over
  the legacy 899), accept ⇒ advance past the exec region and fall through
  to the normal projection read. Null entry ⇒ unchanged interpreter path.
- **Glue:** `dbtup_jit_compile_scan_filter` / `dbtup_jit_invoke_scan_filter`
  in `DbtupJitGlue`; cold-call helpers `ndb_jit_h_load_col` /
  `ndb_jit_h_branch_attr_null` rewired to read via the new public
  `Dbtup::readSingleAttributeForJit` (no AggInterpreter dependency).
- **Eligibility (v1):** `RexecRegionLen>0 && RfinalUpdateLen==0 &&
  RsubLen==0`; a final-read/projection region is allowed.
- **Diagnostics/canary:** ERROR_INSERT 4060 made fallback-fatal for filtered
  scans; MTR `rondb_jit_scan_filter_canary` (IS NULL / IS NOT NULL under
  4060 + differential) — **passing** (committed `.result` matched).
- **Two bring-up fixes (both in the tree):** (1) reject code → 626
  (`TUP_NO_TUPLE_FOUND`); 899 isn't visible in `DbtupExecQuery.cpp` and 626
  is Dblqh.hpp's recommended filter code anyway. (2) **EXIT_OK lowering** —
  the first run rejected *every* row because the aggregation-embedded bridge
  lowers `EXIT_OK` to a no-op (accept = fall through to accumulators), but a
  scan filter's `EXIT_OK` precedes its `EXIT_REFUSE`, so accepted rows fell
  into `OP_FILTER_REJECT_EXIT`. Fixed via an `exit_ok_kind` param
  (scan→`OP_EXIT`, aggregation→fall-through); `bridge_tests` T38 regresses it.
- **Limitation:** only the NULL-branch subset compiles today
  (`WHERE col IS [NOT] NULL`); richer predicates need Phase 5's full
  embedded-branch family and stay on the interpreter until then.

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
  `4061` (dump program+translation, DBTUP-side paths), `4063` (bounded
  row trace), gated (commit `16feda17b`). **Renumbered 2026-08-19:**
  the DblqhProxy compile-time diagnostics are `5119` (dump program +
  translation at JOIN_AGG_SETUP) and `5120` (compile failure fatal) —
  they were 4061/4062, which `all error N` can never arm on the proxy
  because Cmvmi::execTAMPER_ORD routes 4xxx to DBTUP and 5xxx to DBLQH
  (found while debugging the 5A join-CASE regression: the proxy dump
  silently never fired).
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
Phase 7 scan filters landed 2026-06-08; the **DBTUP runtime side** —
setup/compile hook + per-row dispatch + canary — was implemented 2026-06-10
(first vertical slice; see the "Latest implementation" section above and
`phase_7_implementation.md`). What was the next chunk is now:

1. ~~Setup/compile hook for SCAN_FRAGREQ filters~~ **DONE** — compile-once
   on the stored procedure in `Dbtup::scanCopyAttrinfo`, entry copied onto
   `Dblqh::ScanRecord`.
2. ~~Per-row invocation glue~~ **DONE** — `Dbtup::interpreterStartLab` runs
   the compiled entry and maps `row_filter_rejected` →
   `TUPKEY_abort(TUP_NO_TUPLE_FOUND=626)`, else falls through; null entry →
   `interpreterNextLab()` unchanged.
3. ~~Canary~~ **DONE (pending run)** — `rondb_jit_scan_filter_canary`
   (4060-forced IS NULL / IS NOT NULL + differential). `.result` is
   best-effort; `--record` if it diffs.
4. **Now next:** ~~(a) verify the slice builds + the canary passes~~ **DONE
   2026-06-10** (bridge_tests incl. T38 + the MTR canary all pass).
   ~~(b) tighten `translate_scan_filter` to reject linked ops~~ **DONE
   2026-06-10** (`allow_linked_ops` flag; bridge_tests T36 flipped to a
   reject test). ~~(c) capture the `EXIT_REFUSE` code instead of assuming
   626~~ **DONE 2026-06-10** — bridge returns `out_reject_code` (the
   program's uniform `EXIT_REFUSE` code), cached on storedProc + scan_rec
   and used in the per-row `TUPKEY_abort`; mixed-code programs fall back to
   the interpreter (bridge_tests T39/T40). (d) ~~**the big one** — real
   comparison predicates (`col > 5`)~~ **FIRST SLICE DONE & VERIFIED
   2026-06-15** — integer `BRANCH_ATTR_OP_ARG` (EQ/NE/LT/LE/GT/GE vs an
   inline literal) now JITs via a new `op_branch_attr_op_arg` cold-call
   stencil whose helper reads the instruction from the program buffer and
   reuses the interpreter's `m_cmp` (commits `fa5464df51f` + `d1a710abb92`;
   `phase_7_comparison_predicates.md`). **OP_PARAM also DONE & VERIFIED
   2026-06-16** — `col <op> ?` (bound parameter, `cmp_param`) reuses the same
   stencil (no regen); the helper resolves the operand from the param region
   via `lookupInterpreterParameter` (commit `51f0f6a5718`). **OP_ATTR also
   DONE & VERIFIED 2026-06-16** — `col1 <op> col2` (column-vs-column,
   `cmp(cond,field1,field2)`) reuses the same stencil/helper; the eval reads
   the 2nd column and compares with the 1st column's comparator (commit
   `2f35cc1771c`). **String/VARCHAR also DONE & VERIFIED 2026-06-16** — the
   eval reads columns into the large `coutBuffer` (not a stack buffer) so
   wide strings can't overflow; `m_cmp`+charset already handled the compare
   (commit `c3e4eacc6eb`). **(d) — the comparison-predicate family is now
   complete:** `col <op> const | ? | col2`, integer and string. Only
   `LIKE`/mask conditions stay on the interpreter (bridge rejects cond > GE),
   by design.
5. **(Deferred)** standalone CASE disposition model — the translate API
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
debug_build/runtime_output_directory/testJoinAggNdbApi -c localhost:1186 -m 3306 --only 24 -v
# NB: JIT canaries are Tests 24-29 post-merge (upstream Test 23 = fragsPerWorker)
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
