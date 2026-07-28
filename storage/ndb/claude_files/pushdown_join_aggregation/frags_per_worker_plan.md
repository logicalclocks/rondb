# Configurable fragsPerWorker (1/2/4) for JoinAgg pushed queries — incl. CTE queries

**Status: Phases 1-8 implemented (wire protocol + version gate @ 26.04.2;
NdbQueryOptions::setFragsPerWorker; prepareSend + SCAN_TABREQ population;
DBTC chunked SPJ-instance assignment; DBSPJ defensive comment; CTE
enablement with scanCte carve-out; RonSQL `FRAGS_PER_WORKER = <n>` syntax;
tests). Phase 8 test files exist but the 5 MTR result files still need a
first `--record` run. Manually verified live: fs_batch at K=2/4 returns
correct results; K=4 measured ~10% faster than K=1. Queries containing a
scanCte (CTE_SCAN) operation stay pinned to K=1 (virtual-fragment mapping,
see Phase 6 Discovery 1).**

## Context

Aggregate (JoinAgg) pushed queries force `m_fragsPerWorker = 1`
(NdbQueryOperation.cpp:3689-3756, agg arm at :3701-3708): every root fragment
gets its own SCAN_FRAGREQ, its own DBSPJ request (QueryTree parse + TreeNode
build), its own API-side worker/receiver/result-stream, and its own conf
envelope. `next_steps.md` "Reduce Fixed Overhead for Small-Range CTE Queries"
item 3 attributes part of the ~2x CPU gap vs MySQL on `fs_batch` to this
fan-out fixed cost.

This plan makes the value configurable per query (at least 1, 2, 4) for
**aggregate queries only**, including CTE compound queries. Normal pushed scans
keep today's behavior (all node-local fragments bundled when MultiFrag).
Default stays 1 (opt-in); flipping the default is decided after benchmarking.
Surfaces: NDB API `NdbQueryOptions` (QueryDef level) + explicit RonSQL
statement syntax.

**Verified groundwork that makes this small:**
- DBTC bundling is purely primaryBlockRef-driven: the MultiFrag block
  (DbtcMain.cpp:17672-17725) assigns ONE `spjInstance` for the whole scan, so all
  node-local fragments collapse into one SCAN_FRAGREQ. If a node's fragments
  carried K distinct instances, the existing qsort + bundling loop (break at
  :19556) would emit ⌈frags/K⌉ SCAN_FRAGREQs per node — the kernel change
  surface is only the instance-assignment block.
- JoinAgg + MultiFrag section wiring `[AttrInfo, aggStateKeys, fragIdList]`
  already exists from Phase 7 (DbtcMain.cpp:19494-19497; DBSPJ strips fragIdList
  :1525-1540 then aggKeys :1565-1568).
- DBSPJ accepts arbitrary root fragment subsets (scanFrag_build :10818-10877);
  JoinAgg row accounting is per-request (`m_aggNodes`-gated, :13237-13241);
  batch division is by `m_parallelism` (:12308-12312). The only
  `m_rootFragCnt == 1` assert (:3707) is on the T_SORTED_ORDER path, never taken
  by aggregate queries.
- API downstream already scales by `m_fragsPerWorker`: buffer sizing
  (:1415-1423), 12-bit correlation cap 4096/K (:5921-5926), QN_ScanFragParameters
  batch multiply-up (:6157-6159). The 990-row CTE batch fits under 4096/4 = 1024.
- DBTC's CTE-phase barrier is already **per worker**, not per fragment:
  `registerCteScanFragHandle` (:19371) registers one CteScanFragHandle per
  ScanFragRec and increments `m_cteScanReportsExpected` (:19421) per handle.
- Motivation and analyses: `next_steps.md`, `scan_parallelism_analysis.md`,
  `scan_scan_batch_analysis.md`.

**Note on "as part of QueryTree":** DBTC does not parse the QueryTree, so the
value cannot ride in QN_ScanFragParameters; it is configured at the
QueryDef/QueryTree-building level (`NdbQueryOptions`) and travels to DBTC as
log2(K) in **bits 16-17 of `SCAN_TABREQ::storedProcId`** (the extended-flags
area — `requestInfo` is full; long-SCAN_TABREQ senders put 0xFFFF in
storedProcId so the upper half reads as zero from old versions). Chosen over
an optional trailing word per maintainer direction — only a few bits are
needed.

## Design overview

`K = fragsPerWorker`, query-global. Flow:
RonSQL `FRAGS_PER_WORKER = K` → `NdbQueryOptions::setFragsPerWorker(K)` on the
root scan op def → `prepareSend` computes effective `m_fragsPerWorker = K`
(clamped) and `m_workerCount = rootFragments / K` → SCAN_TABREQ carries
`scanParallelism = m_workerCount` (value-identical to today when K=1),
MultiFragFlag (auto-set when K>1, :4567-4569), and log2(K) in storedProcId
bits 16-17 (`ScanTabReq::setFragsPerWorker`) → DBTC seizes `workerCount`
ScanFragRecs and assigns a fresh
SPJ instance per chunk of K node-local fragments → existing bundling emits
`workerCount` SCAN_FRAGREQs of K fragIds each → DBSPJ: main root already
handles fragment subsets; CTE roots extended to scan all K listed fragments.

Aggregation state (per-LDM JoinAggregationState, JOIN_AGG_SETUP/COMPLETE,
redistribute) is per-node and unaffected: DBSPJ still sends per-fragment scans
to the owning LDMs; K only reduces the number of DBSPJ requests/TC-thread
envelopes and API workers.

## Phase 1 — Wire protocol + version gate (IMPLEMENTED)

Files: `storage/ndb/include/kernel/signaldata/ScanTab.hpp`,
`storage/ndb/include/ndb_version.h.in`,
`storage/ndb/src/common/debugger/signaldata/ScanTab.cpp`, `MYSQL_VERSION`.

- ScanTab.hpp: `SCAN_FRAGS_PER_WORKER_SHIFT (16)` / `SCAN_FRAGS_PER_WORKER_MASK
  (3)` + inline `ScanTabReq::getFragsPerWorker(storedProcId)` /
  `setFragsPerWorker(storedProcId, frags)` — 2-bit **log2 encoding** in
  storedProcId bits 16-17: 0⇒1, 1⇒2, 2⇒4, 3⇒8. Setter asserts power-of-two.
  Getter decodes to the actual K. Valid only when the JoinAgg flag is set;
  zero (old senders / K=1) decodes to 1, keeping the wire image bit-identical.
- MYSQL_VERSION bumped to 26.04.2;
  `ndbd_support_joinagg_frags_per_worker(Uint32 x)` added next to
  `ndbd_support_agg_wide_type` with a 26.04.2 floor.
- `printSCANTABREQ` prints the decoded `fragsPerWorker` when the JoinAgg flag
  is set.

## Phase 2 — NDB API option (IMPLEMENTED)

Files: `storage/ndb/src/ndbapi/NdbQueryBuilder.hpp`, `NdbQueryBuilder.cpp`,
`NdbQueryBuilderImpl.hpp`.

- `NdbQueryOptions::setFragsPerWorker(Uint32 frags)` declared next to
  `setMaxRows`; hint semantics documented: aggregate pushed scans only,
  0 = unset, silently ignored on non-aggregate queries / lookups / ordered /
  pruned scans (matches setMaxRows hint semantics).
- The setter **normalizes** the value to what the 2-bit log2 wire encoding can
  carry: rounds down to a power of two and caps at 8. The remaining clamp
  (divisor of fragments-per-node, version gate) happens in prepareSend
  (Phase 3), so the stored option is always encodable.
- Impl: `NdbQueryOptionsImpl::m_fragsPerWorker` (init 0 in ctor, copied in the
  deep-copy ctor), accessor `getFragsPerWorker()` next to `getMaxRows()`.

## Phase 3 — prepareSend + SCAN_TABREQ population (IMPLEMENTED)

File: `storage/ndb/src/ndbapi/NdbQueryOperation.cpp`.

- The node-count loop is factored into a `countDataNodes` lambda (captures
  `rootTable` + `rootFragments`, returns 0 on the fragment-without-node error)
  shared by the aggregate and non-aggregate branches of the fragsPerWorker
  cascade.
- The `m_hasAggregation` branch keeps `m_fragsPerWorker = 1` as the default
  and opts in when: option > 1 (`rootOp.getQueryOperationDef().getOptions()
  .getFragsPerWorker()`), `getQueryDef().getNumCtes() == 0` (pinned until
  Phase 6), and `ndbd_support_joinagg_frags_per_worker(minDbNodeVersion)`.
  Clamp: if `(rootFragments % cnt) != 0` stay at 1; else **halve** K until
  `K <= fragsPerNode && fragsPerNode % K == 0` (K stays a power of two for
  the 2-bit log2 wire encoding; halving terminates at 1).
  Pruned/ordered/mixed-version already forced 1 by the preceding cascade.
- SCAN_TABREQ agg block: `scanTabReq->scanParallelism = m_workerCount`
  (semantics: number of SPJ workers; bit-identical to the old
  `m_workerCount * m_fragsPerWorker` when K=1). K encoded via
  `ScanTabReq::setFragsPerWorker(scanTabReq->storedProcId, m_fragsPerWorker)`
  (storedProcId was set to 0xFFFF earlier in the function) — no
  signal-length change (still StaticLength+5 with scanParallelism).
- Hardening after the `m_workerCount` computation:
  `assert(m_workerCount * m_fragsPerWorker == rootFragments)` (all cascade
  paths pick a divisor; a remainder would leave fragments without a worker).
- No other API edits: receiver section (= m_workerCount, :4667-4674),
  MultiFragFlag, result-stream sizing, corrId cap, QN params batch multiply-up,
  `result_bufsize` (:7064-7098), `m_numAggReceivers = m_workerCount` (:4134) are
  already keyed off the two members. Verified: `isAggReceiveComplete()`
  (:2251-2258) counts confs/rows, not workers; agg routing uses only
  `m_aggReceivers[0]->getId()` (:4657); nothing assumes workerCount ==
  rootFragments.

## Phase 4 — DBTC: fragsPerWorker field + chunked SPJ-instance assignment (IMPLEMENTED)

Files: `storage/ndb/src/kernel/blocks/dbtc/Dbtc.hpp`, `DbtcMain.cpp`.

- `ScanRecord::m_fragsPerWorker` (Uint32, next to the Phase 7 JoinAgg fields):
  init 1 in `initScanrec`; when the JoinAgg flag is set, decoded there via
  `ScanTabReq::getFragsPerWorker(scanTabReq->storedProcId)` (old senders have
  zeros in the upper half → decodes to 1). `scanStoredProcId` is now stored
  masked to the low 16 bits (extended flags stripped; it only feeds a DUMP
  infoEvent).
- Chunked assignment in the MultiFrag block of `sendDihGetNodesLab`, gated on
  `chunkedJoinAgg = scanP->m_joinAgg && m_fragsPerWorker > 1` (non-agg
  MultiFrag keeps the single-instance code verbatim):
  1. The fill loop writes **instance-0** refs when chunking (the shared
     `spjInstance` local is 0 in that case).
  2. The existing qsort then groups fragments per node.
  3. Chunk walk over the sorted array: a fresh `(cspjInstanceRR++ % 120) + 1`
     instance per chunk, new chunk on node change or when `inChunk == K`;
     both primary and preferred refs get the same instance (preserves the
     same-primary⇒same-preferred invariant the `ndbrequire` in
     `sendScanFragReq` needs). Run detection uses the pre-rewrite ref
     (`prevRef`), since entries are rewritten in place. Chunks stay
     contiguous ⇒ no re-sort, zero bundling-code change.
  4. Existing write-back loop unchanged.
- Budget guard (implemented ahead of the walk): `totalChunks = Σ_nodes
  ⌈fragsOnNode / K⌉` counted from the sorted array; budget is
  `scanP->m_booked_fragments_count` (== the API's scanParallelism here — set
  at the end of initScanrec, first decremented in sendFragScansLab, which runs
  after this block). If `totalChunks > budget`: jam + fall back to legacy
  single-instance assignment (minimal bundle count = one per node; excess
  ScanFragRecs absorbed by the empty-conf path). This matters because a
  **deficit hangs JoinAgg**: the API sets PassAllConfsFlag so DBTC's
  ScanFragRec-reuse path is dead and the API never sends SCAN_NEXTREQ —
  DBTC must emit ≤ scanParallelism bundles. A mismatch can only arise from a
  metadata race (API and DBTC compute from the same distribution).
- No changes to: seize loop, the `scanParallel = tfragCount` overwrite
  (checked dependents :13077, :18696, :18971, `sendJoinAggSetupReqs` — all
  per-node or count-independent), `sendFragScansLab`, bundling loop, JoinAgg
  setup/complete/failure handling. Receiver binding stays 1:1: for JoinAgg
  the API sends m_workerCount receiver IDs and scanParallelism ==
  m_workerCount, so `apiPtr[i % numReceiverIds]` is the identity.

## Phase 5 — DBSPJ: defensive only (non-CTE part) (IMPLEMENTED)

File: `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`.

- Comment added at the T_SORTED_ORDER `ndbassert(m_rootFragCnt == 1)` refill
  site: a sorted-order root is never a MultiFrag bundle — the API forces
  fragsPerWorker = 1 for ordered scans, and JoinAgg queries (the only scans
  that may bundle > 1 root fragment per request) never set T_SORTED_ORDER.
- Nothing else: fragment-subset roots, per-request corrId space (guard
  :12473-12481), JoinAgg accounting and `handleJoinAggNextBatch` self-continue
  (:3718-3724) verified fragment-count-agnostic. Batch sizing needs no change:
  QN params carry batch × K (API :6157-6159), DBSPJ divides by `m_parallelism`.
- No temporary CTE `ndbrequire` in DBSPJ: a rogue CTE + K>1 sender is already
  aborted by DBTC's CTE coverage guard (`scanParallel < tfragCount`), since
  scanParallelism carries the (smaller) worker count. Phase 6 relaxes that
  guard and adds the real multi-fragment CTE-root support.

## Phase 6 — CTE compound query enablement (IMPLEMENTED, with scanCte carve-out)

Without this phase K>1 on a CTE query was rejected: DBTC aborts CTE scans when
`scanParallel < tfragCount` ("CTE compound queries require one DBSPJ instance
per fragment"), and the DBSPJ CTE-root branch scanned exactly one fragment
(`m_rootFragId`) — coverage would drop to 1/K. Implemented:

- **DBSPJ CTE-root multi-fragment** (`scanFrag_build` CTE branch): when the
  request's root REQ has MultiFragFlag, one ScanFragHandle is seized per fragId
  in the request's fragment list, mirroring the main-root MultiFrag branch in
  the same function. The root REQ + unpacked fragId list (`theData[25..]`) are
  read via `ctx.m_start_signal` (ndbassert non-NULL): CTE subtrees build before
  the main root consumes it. `data.m_fragCount = K`; the set-once
  `m_rootFragCnt` now takes `data.m_fragCount` instead of hardcoded 1 (the
  main root later sets the same count from the same list). Non-MultiFrag keeps
  the single-`m_rootFragId` path.
- **DBTC coverage-check relaxation**: abort condition is now
  `scanParallel * m_fragsPerWorker < tfragCount` (workers × K must cover all
  fragments).
- **API pin narrowed** (prepareSend): the blanket `getNumCtes() == 0`
  condition is replaced by a walk of the query def rejecting only queries
  containing a **CteScan (scanCte) operation**. CTE_LOOKUP-probed CTE queries
  (the fs_batch shape) now bundle.

**Discovery 1 — scanCte stays pinned to K=1.** `cte_scan_start` implements a
virtual-fragment → data-node mapping: only requests with
`m_rootFragId < m_numDataNodes` scan a node's CTE partition
(`targetNodeId = m_dataNodeList[m_rootFragId]`), higher fragIds complete
immediately. With bundling, request rootFragIds become "first fragId of each
chunk", a set NOT guaranteed to cover 0..numDataNodes-1 → CTE partitions would
be silently skipped. Hence the API-side CteScan pin (covers scanCte as main
root, aggregate leaf, and CTE-reads-CTE). Debug tripwire
`ndbassert(m_rootFragCnt <= 1)` added in `cte_scan_start`. Lifting this needs
a per-request fragment-set-aware node mapping — future work, only relevant if
scanCte-root shapes become fixed-cost-bound.

**Discovery 3 (found by first live K=2 run, crashed both data nodes) —
JoinAgg + MultiFrag + KeyInfo needs 4 sections but a signal carries 3.**
The first fs_batch run with `FRAGS_PER_WORKER = 2` died on
`SimulatedBlock.cpp getSections` assert `secCount=4`: SCAN_FRAGREQ carried
AttrInfo + KeyInfo (the INDEX_SCAN root's bounds) + aggKeys + fragIdList.
The "Phase 7 3-section wiring" only ever covered the no-KeyInfo case and was
dead code besides (JoinAgg never co-occurred with MultiFrag until now).
**Fix**: when both flags are set, DBTC packs the two lists into ONE trailing
section `[fragCount, fragIds..., aggKeys copy...]` (`sendScanFragReq`: fragIds
collected into a stack array, count word prepended, aggKeys content copied in
via SectionReader; the shared aggKeys section is no longer attached in this
mode and is explicitly released at the last send). DBSPJ `execSCAN_FRAGREQ`
unpacks the combined layout when both flags are set (count word → fragIds →
`aggKeysReadOffset` for the aggKeys SectionReader; `aggKeysPtrI` stays RNIL so
the section is released exactly once). Pure-MultiFrag and pure-JoinAgg keep
the old layouts; the combined format needs no version gate of its own since
JoinAgg+MultiFrag is only produced behind the fragsPerWorker gate.

**Discovery 2 — root CTE_LOOKUP exactly-once depends on rootFragId 0.**
`cte_lookup_start` and `lookup_start` (lookup main root on a CTE query)
execute exactly once cluster-wide by firing only on the request with
`m_rootFragId == 0` (all other requests complete the node immediately). The
qsort in DBTC ordered fragments only by blockRef, so within a node run the
fragId order was unspecified — chunking could have produced NO bundle whose
first fragId (→ `fragmentNoKeyLen` → `m_rootFragId`) is 0, hanging scalar /
root-lookupCte shapes. **Fix**: `compareFragLocation` now tiebreaks on fragId,
making node runs ascending; fragment 0 is the global minimum so its chunk
keeps it first, preserving exactly-once. (Also makes normal MultiFrag bundle
order deterministic — membership unchanged, harmless.)

- **Audits (verified, no code needed):**
  - CTE completion barrier: per-worker already (`registerCteScanFragHandle`
    registers one handle per ScanFragRec; `m_cteScanReportsExpected` counts
    handles), so K-fragment bundles report once per worker.
  - Child CTE_LOOKUP probing (fs_batch shape): fired per parent row via
    `cte_lookup_send`, group-hash-routed to the owner (Step 4d) — no
    rootFragId dependence, fragment-count independent.
  - CTE body multi-batch continuation: body scans use the ordinary scanFrag
    machinery over `m_fragments` (same as main-root MultiFrag, proven path).
  - MUTEX_FREE build + redistribute: feeds are per-LDM-scan (unchanged);
    feeds-done-before-COMPLETE barrier is the per-worker report above.
  - Child-scan skew offset `(m_rootFragId * m_rootFragCnt) % fragCount`
    changes value with bundling — load-balancing only, benign.

## Phase 7 — RonSQL syntax (IMPLEMENTED)

Files: `storage/ndb/src/ronsql/Keywords.hpp`, `RonSQLParser.y`,
`RonSQLPreparer.hpp/.cpp`, `RonSQLCommon.hpp`. No manual parser regen needed:
CMake regenerates via bison 3.8 (`build_parser.sh` custom command depends on
the .y).

Syntax: `[EXPLAIN] [FRAGS_PER_WORKER = <int>] [WITH …] SELECT …`
(statement-head slot, same shape the LOCAL-mode plan reserves).

- Keywords.hpp: `kwdef(FRAGS_PER_WORKER)` between FORCE and FROM — verified
  strcmp order for the lexer binary search; 16 chars ≤ the 18-char lexer cap;
  underscores survive the `& 0xdf` uppercase. Reserved in RonSQL only (NOT
  added to `keywords_defined_in_mysql`).
- RonSQLParser.y: `%token T_FRAGS_PER_WORKER`, `%type<bival>
  frags_per_worker_opt` (`%empty {$$=0;}` | `T_FRAGS_PER_WORKER T_EQUALS
  T_INT`), inserted in `selectstatement` after `explain_opt` ($n renumbered).
  Values < 1 raise new `ErrState::INVALID_FRAGS_PER_WORKER`
  ("FRAGS_PER_WORKER must be a positive integer.") via the
  set_err_state + YYERROR pattern.
- RonSQLCommon.hpp `SelectStatement`: `Int64 frags_per_worker = 0;`.
  **Gotcha**: `alloc_exc` is raw arena allocation — no default member
  initializers run — so the cte_def and subquery grammar actions explicitly
  zero the field on their arena-allocated SelectStatements (ast_root is a
  normally-constructed Context member and always assigned by the rule).
- RonSQLPreparer.cpp: applied in `emit_root_op` right after
  `NdbQueryOptions rootOpts;` — that object is shared with
  `emit_index_scan_root`, covering CTE_SCAN/lookupCte/readTuple/TABLE_SCAN/
  INDEX_SCAN root shapes. CTE-body emission untouched (K is query-global via
  SCAN_TABREQ). EXPLAIN (`print()`) leads with `FRAGS_PER_WORKER = N
  (requested; …)` when set.

## Phase 8 — Tests and benchmarks (IMPLEMENTED; needs first --record run)

- **Block test** — `testJoinAggNdbApi.cpp` **Test 23**
  (`testFragsPerWorker` + `runTest23Query`): reuses the Test 18 tables
  (2000 rows, `FOR_RP_BY_LDM_X_2` = 16 fragments) and runs the SUM/COUNT
  GROUP BY join with `setFragsPerWorker` on the root scan at
  unset / 1 / 2 / 4 / 8, verifying the closed-form expected results
  (group g → SUM = 1000·g + 990000, COUNT = 100) for every run. 8
  exercises the API halving clamp on topologies with < 8 frags/node. No
  ERROR_INSERT → no fake-OK entry, runs in all build types. Run:
  `make -j$(sysctl -n hw.ncpu) testJoinAggNdbApi` then
  `./storage/ndb/block_unit_test/testJoinAggNdbApi -c <connect_string> -m <mysql_port> --verbose`
  (or `--only 23`).
- **MTR** — `suite/ronsql_cte/include/body_frags_per_worker.inc` + thin
  wrapper `ronsql_cte_dd_frags_per_worker.test` in `ronsql_cte` and the 4
  topology siblings (ng1r3/ng2r2/ng2r3/ng4r2). Cases: fpw-1..4 fs_batch
  shape at K = unset/1/2/4 (each strict-diffed against the same bare-SQL
  MySQL baseline → cross-K equality is transitive); fpw-5 non-CTE join agg
  K=4; fpw-6 root-lookup CTE query K=4 (rootFragId-0 exactly-once path);
  fpw-7 scanCte root K=4 (pin path, must still be correct); fpw-8 EXPLAIN
  header grep (fatal — our own stable output); fpw-9 `= 0` parse error.
  **`ronsql_compare.inc` gained an optional `$RONSQL_PREFIX` variable**:
  RonSQL-only statement-head syntax prepended on the RDRS side only
  (requires `$suppress_ronsql_cli=yes`, reset after each use) — reusable
  for the LOCAL-mode plan. First run needs `--record` to create the 5
  result files:
  `./mtr --record --suite=ronsql_cte ronsql_cte_dd_frags_per_worker`
  (repeat for `ronsql_cte_ng1r3` / `ng2r2` / `ng2r3` / `ng4r2`), then a
  plain re-run to confirm stability.
- **Benchmarks** — after Phase 6, `fs_batch` / `offline_fs_*` via rondb-cli
  `.bench_ronsql` with `FRAGS_PER_WORKER = 2/4` variants (registry
  `tools/rondb-cli/internal/shell/ronsql_bench.go`, `RonSQLPrefix` field
  applied on the RonSQL REST paths only). Default-flip decision comes from
  these numbers.
- **First results (2026-07-09, fs_batch, 2-node MTR cluster, 8 frags/node)**:
  `FRAGS_PER_WORKER = 4` ~10% faster than un-hinted (K=1) and ~5% faster
  than K=2. Confirms the DBSPJ-request fan-out share of the fixed-cost floor
  from next_steps.md item 3; the fs_batch registry entry now defaults to
  K=4 via `RonSQLPrefix`. The kernel-side default remains 1 (opt-in) —
  revisit a global default after `offline_fs_*` and larger-range runs.

## Risks

1. **Bundle/receiver deficit hangs JoinAgg** (PassAllConfs disables reuse; API
   never sends NEXTREQ). Mitigated: API only enables K>1 on even distributions;
   DBTC precomputes chunk count and falls back to legacy assignment over budget.
2. **corrId budget**: 4096 ids shared across K fragments per request — API cap
   (:5921-5926) handles correctness; large K shrinks per-fragment batches →
   keep K ∈ {2,4} initially.
3. **CTE root multi-frag** is the highest-risk piece (wrong-results if a
   fragment is missed) — the MTR CTE result-equality cases across the 5
   topology suites are the net.
4. **storedProcId extended bits are a shared, unsigned-off namespace**: the
   LOCAL-mode plan notes bits 30/31 are claimed by in-flight work and wants
   "the next free extended-requestInfo bit" — bits 16-17 are now taken by
   fragsPerWorker; keep the allocation documented in ScanTab.hpp as the
   single source of truth.
5. **Grammar**: FRAGS_PER_WORKER becomes reserved; `$n` renumbering in
   `selectstatement` is error-prone — full ronsql MTR sweep after regen.
6. **Mixed versions**: covered by `ndbd_support_joinagg_frags_per_worker`; K=1
   wire image is bit-identical to today.
