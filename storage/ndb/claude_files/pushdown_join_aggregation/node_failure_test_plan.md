# Node-failure, state-machine and parking test plan (RONDB-1120 hardening)

**Status: ACCEPTED (2026-09-11), Phase 0 in progress.** Covers the node-failure hardening committed
on branch RONDB-1120 between `e2dad705492` and `dd1df0db64a`. All existing
suites pass on that range; none of them exercises a node failure while a
CTE / join-aggregation query is in flight, and none exercises the identity
parking mechanism deterministically beyond the two existing hooks (5127,
8310). This plan adds the hooks, the tests, and the pool-leak checks that
make the new code observable.

Companion docs: `joinagg_setup_overlap_plan.md` (identity table, parking,
10 ms sweeper), `coordinator_research.md` / `coordinator_implementation.md`
(DBTC SETUP/COMPLETE/RELEASE), `cte_dag_scheduler_plan.md`,
`test_benchmark_extension_plan.md` (MTR suite layout).

---

## 0. Deliverables at a glance

| Item | Count | Where |
|---|---|---|
| New error inserts | 14 (DBLQH 5128-5139, DBTC 8311-8313, DBSPJ 17532) | kernel blocks |
| New DUMP codes (leak checks) | 4 (LQH 2362-2363, TC 2560, SPJ new handler + 1 code) | kernel blocks |
| NDBT node-failure cases | 12 | `testNodeRestart` (new `testCteNodeFail` if the binary grows too large) |
| Block-level protocol / state-machine cases | 14 | `block_unit_test/testCteDbtc`, `testJoinAgg`, new `testCteProtocol` |
| Parking cases | 8 | `block_unit_test/testCteProtocol` + 2 in `testNodeRestart` |
| MTR cases | 6 test files, developed inside Phases 1-3 (maintainer direction) | `mysql-test/suite/ronsql_cte` (4-node variant `ronsql_cte_ng2r2`) |
| Autotest registration | all NDBT cases | `storage/ndb/test/run-test/daily-basic--0N-tests.txt` |

---

## 1. What changed and what a test must be able to observe

Each row is a mechanism added or fixed in this range, the failure it
prevents, and the symptom a test can assert on if it regresses.

| Commit | Mechanism | Symptom if broken |
|---|---|---|
| `c7faa193ac2` | DBTC `checkScanActiveInFailedLqh` rechecks close completion for a CLOSING_SCAN whose last close reply was owed by the failed node | ScanRecord + ApiConnectRecord parked forever; API never gets EndOfData; later API-node failure handling stalls |
| `55e99285ebb` | DBLQH identity-table sweep aborts CTE states listing a failed peer; `m_cte_complete_reply_sent` exactly-once COMPLETE reply | COMPLETE_REQ never answered (DBTC hangs); duplicate COMPLETE_REF |
| `6c7fa88dcaa` | DBSPJ CTE_LOOKUP abort / NODE_FAILREP, per-node outstanding counts | request `m_outstanding` never reaches 0 after node death; batch hang |
| `f718b5be5d6` | `numRowsToSpj` in CTE_SCAN_CONF/REF; DBSPJ per-source slot model (batch obligation until reply AND rows) | CTE scan node never completes when projection leaves no residual; close sent with unsettled iterator |
| `c3ce0732890` | DBLQH `CteScanIterState` sweep on requester failure; token validation | iterator pool records leak per failure; continuation with recycled token |
| `ae810a9dc73`, `0f983279eb6` | Coordinator death: continuations mark NODE_FAIL_ABORT, CTE owners mark in sweep, proxy reclaims CTE_READY | join-agg state pool (256 default) exhausted after coordinator failures during CTE probing / paused redistribution |
| `c13a662bc93`, `cebefb196f6`, `5efa38683b1` | coordinatorRef carried in CTE_SCAN_REQ / CTE_LOOKUP_REQ / NULL_ROW_REQ; agg-feed continuation stops on coordinator death; NF completion waits for it | continuation touches reclaimed state; DBLQH dereferences freed interpreter |
| `84f15ad6454` | `execJOIN_AGG_NULL_ROW_REF` retires the reply, `checkBatchComplete` | aborted request never completes when the last reply is a REF |
| `b30c78c0be0` | `m_release_started`: one teardown chain per record | double `beginTeardown`, double pool release |
| `10b32423001`, `cc86a7c2043`, `dd1df0db64a` | Identity validation (transid + identWord / SETUP requestId) on REDISTRIBUTE_REQ/CONF/REF, FINAL_REP, RELEASE_REQ, RELEASE_CONF | a stranger's state merged, finalized, aborted or released after slot reuse |
| `35f1ce95afa` | Live-query redistribution pages freed by a local chain, not the proxy's state-releasing chain | aggregation state released mid-query when a drain leaves > 256 pages |
| `ffb6f7f136a` | Iterator sweep queued behind existing continuations | continuation runs after NF completion permitted reclaim |

Observable invariants used throughout (see §3 for the hooks that expose
them):

- **I1 Query outcome**: the query fails with 286 (ZNODEFAIL_BEFORE_COMMIT)
  or 1251 (ZJOIN_AGG_STATE_NOT_FOUND), or succeeds if the failure landed
  after its completion; it never hangs (bounded wait, 60 s).
- **I2 Cluster health**: only the intended victim leaves; every survivor is
  still started (`NdbRestarter::waitClusterStarted` after the victim
  restarts); no `ndbrequire` in survivors (checked through the node logs
  the way `runFailRepBeforeJoin` does).
- **I3 NF completion**: the victim's failure handling completes on every
  survivor (the victim can rejoin; `waitClusterStarted` succeeds).
- **I4 No leak**: after the victim rejoins, `DUMP 2361` (join-agg state
  pool), new `DUMP 2362` (iterator pool), `DUMP 2363` (identity entries +
  park records), `DUMP 2560` (DBTC aggregation records, CTE handle pool,
  scan states) and the new DBSPJ dump all report empty.
- **I5 Follow-up query**: the same query succeeds after recovery with the
  correct result.

---

## 2. Test infrastructure (verified)

### 2.1 Layer A - NDBT with node kills (`storage/ndb/test/ndbapi/testNodeRestart.cpp`)

Existing cases: `JoinAggNodeRestart` (crash inserts 5121 SETUP_REQ,
5122 COMPLETE_REQ, 5123 RELEASE_REQ on one data node while a join-agg
query runs through the NDB API, then `waitNodesNoStart` / `startNodes` /
`waitClusterStarted`, post-restart query, `DUMP 2361`), and
`JoinAggErrorInsert` (5124 COMPLETE_REF, 5125 SETUP_REF). Both use
`runJoinAggQuery(ndb, tab)`, a plain join aggregation, not a CTE.

Available primitives (`storage/ndb/test/include/NdbRestarter.hpp`):
`insertErrorInNode`, `insertError2InNode`, `insertErrorInAllNodes`,
`restartOneDbNode(node, initial, nostart, abort)`, `waitNodesNoStart`,
`startNodes`, `waitClusterStarted`, `dumpStateOneNode`,
`dumpStateAllNodes`, `getMasterNodeId`, `getNextMasterNodeId`,
`getRandomNodeOtherNodeGroup`.

Gap: no NDBT helper builds a CTE query. `block_unit_test/testCteNdbApi.cpp`
builds CTE queries through the NDB API query builder; its construction
helpers must be moved into a shared header (`block_unit_test/CteQueryUtil.hpp`
or `storage/ndb/test/include/HugoCteQueries.hpp`) so `testNodeRestart` can
issue CTE queries. This is the first task of Phase 1.

Topology: peer-failure and coordinator-not-requester scenarios need
4 data nodes in 2 node groups (kill one node without losing data and still
have a surviving peer and a distinct coordinator). Cases must `[SKIPPED]`
on smaller clusters the way `runFailRepBeforeJoin` does.

### 2.2 Layer B - block unit tests (`storage/ndb/block_unit_test/`)

SignalSender-driven, link `NDBTEST NDBCLIENT MYSQLCLIENT`, so
`NdbRestarter` is available (already used by `testCteDbtc`, `testJoinAgg`,
`testCteLookup`, `testCtePhase6`). Build with `make testCteDbtc` in the
build dir (see `TESTING_GUIDE.md`, "Building").

- `testCteDbtc`: drives DBTC with hand-built SCAN_TABREQ + CTE trees
  (Tests 1-11: two CTEs, CTE_LOOKUP main select, empty table, negatives).
  Right layer for DBTC-visible protocol outcomes (COMPLETE/RELEASE
  accounting, CLOSING_SCAN behaviour) combined with error inserts.
- `testCteLookup` / `testCtePhase6`: hand-built CTE_LOOKUP_REQ to DBLQH
  and SCAN_FRAGREQ to DBSPJ with the test's own reference as sender.
  Right layer for identity-mismatch and token-validation tests, since the
  test controls every word of the request.
- `testJoinAgg` / `testCaseAgg`: SETUP -> operations -> COMPLETE -> RELEASE
  driven directly against the proxy (fixed FAKE_REQUEST_ID /
  FAKE_TRANS_ID). Right layer for the teardown-idempotence and RELEASE
  identity tests.
- `testCteNdbApi*`, `testVarcharMinMax`: NDB API end to end, CTE scan
  nodes included; the only tests that build QN_CTE_SCAN today.

Constraint: a test cannot spoof a DBLQH reply to DBSPJ. The reply's
sender node is taken from the signal header, which is the test's API
node, so DBSPJ's per-source slot lookup fails. DBSPJ state-machine
transitions are therefore driven through DBLQH error inserts against a
real cluster, not by injecting CONF/REF from the test.

### 2.3 Layer C - MTR (`mysql-test/suite/ronsql_cte*`)

`ronsql_cte` runs 2 data nodes, NoOfReplicas=2; `ronsql_cte_ng2r2` runs 4.
Queries go through RonSQL (rdrs). Data-node control idioms already used
in `mysql-test/suite/ndb/t` (e.g. `ndb_TCtakeover_stall.test`,
`ndb_activate_before_qmgr_phase1.test`, `ndb_backup_nodefail.test`):

```
--source include/have_ndb_error_insert.inc
--exec $NDB_MGM -e "2 ERROR 5128" >> $NDB_TOOLS_OUTPUT
--exec $NDB_MGM -e "2 RESTART -n" >> $NDB_TOOLS_OUTPUT
--exec $NDB_WAITER --nowait-nodes=1 --not-started >> $NDB_TOOLS_OUTPUT
--exec $NDB_MGM -e "2 START" >> $NDB_TOOLS_OUTPUT
--exec $NDB_WAITER >> $NDB_TOOLS_OUTPUT
```

Leak checks from MTR: `ndbinfo.ndb$pools` already lists DBTC's
"CTE Scan Fragment Handle" pool; the DBLQH transient pools (including the
iterator pool) are only printed by the debug-build
`ZLQH_TRANSIENT_POOL_STAT` timer. MTR tests will use `DUMP` through
`$NDB_MGM -e "ALL DUMP 2361"` and check the node logs for the crash the
dump raises on a leak.

### 2.4 Existing error inserts in these paths

| Code | Block / function | Effect |
|---|---|---|
| 5120 | Proxy `execJOIN_AGG_SETUP_REQ` | JIT compile failure path |
| 5121 | Proxy `execJOIN_AGG_SETUP_REQ` | crash node on SETUP_REQ |
| 5122 | DBLQH `execJOIN_AGG_COMPLETE_REQ` | crash node on COMPLETE_REQ |
| 5123 | Proxy `execJOIN_AGG_RELEASE_REQ` | crash node on RELEASE_REQ |
| 5124 | DBLQH `execJOIN_AGG_COMPLETE_REQ` | REF once (state not found) |
| 5125 | Proxy `execJOIN_AGG_SETUP_REQ` | SETUP_REF once (allocation failure) |
| 5126 | Proxy `execJOIN_AGG_SETUP_REQ` | max 3 groups per interpreter (eviction) |
| 5127 | Proxy `execJOIN_AGG_SETUP_REQ` | hold ONE SETUP_REQ 20 ms (parking of LQHKEYREQ / SCAN_FRAGREQ feeds) |
| 8310 | DBTC `execJOIN_AGG_SETUP_CONF` | delay ONE SETUP_CONF 20 ms (identity-addressed COMPLETE with RNIL key) |
| 8098 | DBTC `execNODE_FAILREP` | pre-existing NF hook, unrelated |
| 17530, 17531 | DBSPJ | in use (CTE); 17532+ free |

Free ranges verified: DBLQH 5128-5145, DBTC 8311-8330, DBSPJ 17532-17560,
DUMP 2362-2369 (LQH), 2560-2569 (TC). DBSPJ has no `execDUMP_STATE_ORD`
at all today.

---

## 3. Phase 0 - hooks (prerequisite for everything else)

### 3.1 Error inserts

All are `ERROR_INSERTED(n)` (self-clearing where "once" is stated, via
`CLEAR_ERROR_INSERT_VALUE`), guarded by `#ifdef ERROR_INSERT`, and each
gets a one-line entry in `TESTING_GUIDE.md`, "ERROR_INSERT for Testing".

| Code | Where | Effect | Window it opens |
|---|---|---|---|
| 5128 | DBLQH `cteScanEmitResults`, before the CONF send | send the batch's TRANSID_AI rows, then drop the CTE_SCAN_CONF once | DBSPJ slot has rows but no reply: kill the DBLQH node (slot drain by NODE_FAILREP), or let it time out |
| 5129 | DBLQH `cteScanReqImpl`, continuation path | answer a continuation REQ with CTE_SCAN_REF(ZJOIN_AGG_STATE_NOT_FOUND) once, releasing the token | DBSPJ REF path with rows already counted |
| 5130 | DBLQH `cteScanAggFeed` | force `CTE_SCAN_AGG_FEED_BATCH` = 1 while set | long agg-feed continuation chain: kill requester or coordinator mid-chain |
| 5131 | DBLQH `cteLookupReqImpl` | delay the CTE_LOOKUP_CONF/REF 50 ms once (park in a CONTINUEB) | lookup reply in flight when the target node is killed |
| 5132 | DBLQH `joinAggNullRowReqImpl` | REF once with ZJOIN_AGG_INTERPRETER_ERROR | `execJOIN_AGG_NULL_ROW_REF` accounting |
| 5133 | DBLQH `execJOIN_AGG_REDISTRIBUTE_REQ` | delay the flow-control CONF 200 ms while set | sender paused in CTE_REDISTRIBUTING (the "paused redistribution" window for coordinator kill) |
| 5134 | DBLQH `redistAlloc` | `REDIST_PAGE_SIZE` effectively 512 bytes while set | > 256 pages after a drain (page-free chain split) |
| 5135 | DBLQH `execSCAN_NEXTREQ`, close of a join-agg / CTE scan | ignore the close once (no SCAN_FRAGCONF) | DBTC CLOSING_SCAN with one close owed: kill this node |
| 5136 | Proxy `execJOIN_AGG_RELEASE_REQ` | re-send the same RELEASE to itself once | duplicate release during teardown |
| 5137 | Proxy `continueJoinAggTeardown` | `JOIN_AGG_TEARDOWN_GROUPS_PER_BATCH` = 1 while set | long teardown chain overlapping NF reclaim / duplicate release |
| 5138 | Proxy `execJOIN_AGG_SETUP_REQ` | hold EVERY SETUP_REQ until cleared (not 20 ms) | all consumers park; sweeper (10 ms) fires; node kills while parked |
| 5139 | Proxy `execJOIN_AGG_SETUP_REQ` | drop the SETUP_REQ once (no CONF, no REF) | placeholder never filled: sweeper REF path for every parked GSN |
| 8311 | DBTC `sendJoinAggCompleteReqs` | send COMPLETE_REQ with aggStateKey RNIL for one node even if the key is known | identity-addressed COMPLETE parks or resolves |
| 8312 | DBTC `sendJoinAggReleaseReqs` / `releaseJoinAggResources` | CRASH_INSERTION right after the RELEASE_REQs are sent | coordinator dies with releases in flight: reclaim vs teardown overlap |
| 8313 | DBTC `execJOIN_AGG_SETUP_CONF` | drop ONE SETUP_CONF for good (not 20 ms) | stale-SETUP reclaim path (`sendStaleSetupReclaim`) and RELEASE identity with zero transid |
| 17532 | DBSPJ `cte_scan_sendReq` | after sending, crash-insert the local node once the second batch is requested | multi-batch CTE scan with paused sources on the requester side |

Effort: 1.5 days including guide entries.

### 3.2 Leak-check DUMP codes

Modelled on `LqhDumpJoinAggStates` (2361): log every leaked record with
enough identity to triage, then `ndbabort()` so the leak shows as a node
crash in autotest.

| Code | Block | Checks |
|---|---|---|
| 2362 `LqhDumpCteIterStates` | DBLQH, every LDM and query instance | `c_cteScanIterStatePool` used == 0 (log senderNodeId, coordinatorNodeId, aggFeed) |
| 2363 `LqhDumpJoinAggIdentity` | DBLQH instance 1 (shared table) | identity table has no entries and no placeholders; park pool has no records (`s_jaiFreeHead` chain length == JAI_MAX_PARK) |
| 2560 `TcDumpJoinAggRecords` | DBTC | `AggCompleteRecord` pool, CTE scan-fragment handle pool and `m_joinAggNodes` allocations are empty; no ScanRecord in WAIT_JOIN_AGG_* or CLOSING_SCAN |
| new `SpjDumpRequests` | DBSPJ (add `execDUMP_STATE_ORD`, register in `DbspjInit.cpp`) | request pool and tree-node pool empty; log any request with its state and outstanding count |

Every node-failure test ends with all four dumps on all nodes (`I4`).
A leftover redistribution page list is covered by 2361: an allocated
state at test end already fails that dump. Effort: 1 day.

---

## 4. Phase 1 - node-failure cases (Layer A, `testNodeRestart`)

Common skeleton (extends `runJoinAggNodeRestart`): load a table, start
the CTE query on a worker thread through the shared CTE query helper,
arm the window with an error insert, kill the chosen node with
`restartOneDbNode(node, false, true, true)` or a crash insert, assert I1
on the query, `waitNodesNoStart` + `startNodes` + `waitClusterStarted`
(I2, I3), run the dumps (I4), re-run the query (I5). Roles below: **R** =
requester (the node whose DBSPJ runs the query root), **C** = coordinator
(the DBTC node), **P** = a CTE peer / owner node, **W** = a DBLQH worker.
On a 4-node cluster the test picks R != C by connecting the NDB API to a
node other than the TC master when possible, and skips otherwise.

| ID | Case name | Window (insert) | Victim | Primary invariant beyond I1-I5 |
|---|---|---|---|---|
| NF-1 | `CteCloseOwedByFailedNode` | 5135 on W: one close reply swallowed | W | DBTC ScanRecord leaves CLOSING_SCAN; API receives EndOfData; `TcDumpJoinAggRecords` clean (`c7faa193ac2`) |
| NF-2 | `CtePeerDiesDuringRedistribute` | 5133 on P: sender paused on CONF | P | survivors' CTE states go ERROR / NODE_FAIL_ABORT via identity sweep; COMPLETE_REF reaches DBTC exactly once (`55e99285ebb`) |
| NF-3 | `CteLookupTargetDies` | 5131 on P: lookup reply held | P | DBSPJ drains `m_nodeOutstanding[P]`; request completes with 286 (`6c7fa88dcaa`) |
| NF-4 | `CteScanSourceDiesMidBatch` | 5128 on P: rows sent, CONF dropped | P | slot retired by `cte_scan_execNODE_FAILREP`; request completes; 2362 clean on survivors (`f718b5be5d6`) |
| NF-5 | `CteRequesterDiesPausedScan` | small batch size so the scan pauses between batches | R | iterator records for R released by `handleCteScanNodeFailure`; 2362 clean (`c3ce0732890`) |
| NF-6 | `CteRequesterDiesAggFeed` | 5130 on P (long feed chain) | R | continuation stops on ZNODE_DOWN(R), no REF sent, NF completion not stalled (`c3ce0732890`) |
| NF-7 | `CteCoordinatorDiesAggFeed` | 5130 on P, C != R | C | continuation REFs the live requester; states reclaimed; 2361 clean (`c13a662bc93`) |
| NF-8 | `CteCoordinatorDiesReady` | query holding a CTE_READY state while probing (long lookup phase) | C | CTE_READY states reclaimed by proxy; 2361 clean (`ae810a9dc73`) |
| NF-9 | `CteCoordinatorDiesPausedRedist` | 5133 on P | C | paused CTE_REDISTRIBUTING state marked in owner sweep and reclaimed (`0f983279eb6`) |
| NF-10 | `CoordinatorDiesReleaseInFlight` | 8312 on C | C (crash insert) | no double teardown on survivors (`b30c78c0be0`); 2361 clean |
| NF-11 | `CteRequesterDiesParked` | 5138 on P, kill R while its consumers are parked | R | sweeper REFs go to a dead node harmlessly; placeholders cleaned; 2363 clean |
| NF-12 | `CteCoordinatorDiesParked` | 5138 on P, kill C | C | parked NULL_ROW / COMPLETE replay hits the coordinator check and REFs; 2363 clean (`5efa38683b1`) |

Each case runs 3 iterations to shake timing. Registration in
`daily-basic--01-tests.txt` next to the existing `JoinAggNodeRestart`
entry, `max-time: 1800`, happens in Phase 5.

MTR deliverables of this phase (see §7 for the shared idioms):
`cte_nodefail_peer.test` and `cte_nodefail_coordinator.test` in
`ronsql_cte_ng2r2`, written as soon as NF-2 and NF-7 pass in NDBT so the
same windows are covered end to end through RonSQL.

Effort: 4.5 days including the shared CTE query helper (1 day) and the
two MTR files (0.5 day).

---

## 5. Phase 2 - protocol and state-machine cases (Layer B)

New binary `block_unit_test/testCteProtocol.cpp` (SignalSender, links
NDBTEST) plus additions to `testJoinAgg` and `testCteDbtc`. Deterministic:
no node kills, error inserts only where a real DBLQH reply must be shaped.

### 5.1 DBSPJ CTE scan slot model (via DBTC harness + DBLQH inserts)

Driven from `testCteDbtc` (which already observes SCAN_TABCONF /
SCAN_TABREF and result rows):

| ID | Sequence forced | Expected |
|---|---|---|
| SM-1 | rows before CONF (normal), batchSize 2, 10 groups | five CONFs, obligation released only after reply and rows each time; final EndOfData; `SpjDumpRequests` clean |
| SM-2 | 5129: REF on the second continuation | SCAN_TABREF 1251; slot ended without close; 2362 clean (token released by DBLQH) |
| SM-3 | close during in-flight batch: API closes the scan (SCAN_NEXTREQ close) while 5128 holds one CONF | `close_pending` set, close sent only after the batch drains; close CONF has numRowsToSpj 0 |
| SM-4 | multi-source scan (`m_cteScanAllNodes`) where one source returns EndOfData first | slot ended; other slots continue; completion only when all ended |
| SM-5 | 5128 then timeout instead of node kill | request stays incomplete (documents that the SPJ side relies on DBTC's timeout); DBTC scan timeout aborts; dumps clean afterwards |

### 5.2 CTE lookup accounting

| ID | Sequence | Expected |
|---|---|---|
| LK-1 | 5131 (reply held) + API close during the hold | reply drained after abort; request completes |
| LK-2 | outer-join CTE lookup with a NULL key + 5132 | `execJOIN_AGG_NULL_ROW_REF` retires the reply; request aborts with the REF error, no hang (`84f15ad6454`) |

### 5.3 Proxy teardown and RELEASE identity (`testJoinAgg`, direct SETUP/RELEASE)

| ID | Sequence | Expected |
|---|---|---|
| TD-1 | SETUP, then RELEASE twice back to back (noReply 0) | two CONFs, one teardown; 2361 clean |
| TD-2 | 5137 (slow teardown) + second RELEASE mid-chain | second RELEASE CONFs and starts nothing; 2361 clean after the chain |
| TD-3 | RELEASE with wrong requestId | ignored, CONF sent, state still alive (a COMPLETE afterwards succeeds); then correct RELEASE |
| TD-4 | RELEASE with wrong senderRef (another SignalSender) | ignored + CONF |
| TD-5 | RELEASE with zero transid and correct requestId (stale-reclaim form) | accepted |
| TD-6 | 5136 (proxy re-sends to itself) | exactly one teardown; 2361 clean |

### 5.4 Identity validation on keyed peer signals (`testCteProtocol`, hand-built signals)

Set up a real two-node CTE through `testCteDbtc`-style flow, capture the
keys from SETUP_CONF, then send forged signals to the owner instance:

| ID | Signal | Forgery | Expected |
|---|---|---|---|
| ID-1 | REDISTRIBUTE_REQ | wrong identWord, valid key | dropped, REF(1251) to the test; the real query still completes |
| ID-2 | REDISTRIBUTE_CONF | wrong transid | ignored; sender (real) unaffected |
| ID-3 | REDISTRIBUTE_REF | wrong identWord | ignored; no abort of the real state |
| ID-4 | FINAL_REP | wrong identWord | ignored; owner does not finalize early |
| ID-5 | CTE_SCAN_REQ continuation | recycled / invalid scanIterI, wrong sender node | CTE_SCAN_REF 1251; no crash |
| ID-6 | CTE_SCAN_REQ / CTE_LOOKUP_REQ / NULL_ROW_REQ | coordinatorRef of a node marked down (use a node restarted with `-n`) | REF 286 before any state access |
| ID-7 | any of the above | shorter-than-required signal length | node asserts (documents the `>=` contract; run only in a throwaway cluster, or skip) |

### 5.5 Page-free chain

| ID | Sequence | Expected |
|---|---|---|
| PG-1 | 5134 (tiny pages) + CTE with > 300 queued groups arriving during finalization | drain completes, state stays alive (query returns correct rows), pages freed (2364 clean) (`35f1ce95afa`) |

MTR deliverables of this phase: `cte_redist_pages.test` (5134 with a
wide GROUP BY CTE, result must equal the baseline; covers PG-1 through
RonSQL) and `cte_scan_batches.test` (a CTE scan forced through several
batches, once clean and once with `ALL ERROR 5129` expecting error 1251;
covers SM-1 and SM-2 through RonSQL), both in `ronsql_cte`.

Effort: 5.5 days (new binary 2 days, cases 3 days, MTR 0.5 day).

---

## 6. Phase 3 - parking cases

Mechanism recap (verified in code): a consumer signal that arrives before
the local SETUP resolves by identity; a miss creates a placeholder entry
with a waiter chain and schedules a 10 ms sweeper on the originally
addressed instance (`ZCONTINUE_JOIN_AGG_PARK_SWEEP`). SETUP's identity
insert returns the waiters and the proxy re-dispatches them
(`ZCONTINUE_JOIN_AGG_FLUSH_PARKED`, `joinAggFlushParked` restores length,
sender and sections, then re-enters the exec handler). The sweeper answers
COMPLETE_REQ, REDISTRIBUTE_REQ, NULL_ROW_REQ and LQHKEYREQ with
ZJOIN_AGG_STATE_NOT_FOUND REFs and drops FINAL_REP. Parkable GSNs:
LQHKEYREQ, SCAN_FRAGREQ, JOIN_AGG_COMPLETE_REQ, JOIN_AGG_NULL_ROW_REQ,
JOIN_AGG_REDISTRIBUTE_REQ, JOIN_AGG_FINAL_REP. Limits: 16384 identity
entries, 16384 park records, 25-word park buffer.

| ID | Case | Hook | Expected |
|---|---|---|---|
| PK-1 | every GSN parks and replays | 5127 (one SETUP held 20 ms) on a 4-node CTE query with an outer-join lookup | LQHKEYREQ, SCAN_FRAGREQ, NULL_ROW_REQ, REDISTRIBUTE_REQ, FINAL_REP each observed parked (add a debug counter per GSN exposed by 2363) and the query result is correct |
| PK-2 | identity-addressed COMPLETE | 8310 (delay SETUP_CONF) and 8311 (force RNIL key) | COMPLETE resolves or parks, then replays; result correct |
| PK-3 | sweeper REF path | 5139 (drop one SETUP) | query fails with 1251 within ~50 ms; every parked GSN got its REF or drop; placeholder removed; 2363 clean |
| PK-4 | park pool exhaustion | debug insert that caps effective JAI_MAX_PARK at 4 (5140) with 5138 holding SETUP | consumer error path (LQHKEYREF / SCAN_FRAGREF with resource error), no crash; 2363 clean after SETUP released |
| PK-5 | identity table exhaustion | debug insert capping JAI_MAX_ENTRIES at N (5141) | SETUP_REF OutOfQueryMemory; query fails cleanly |
| PK-6 | RELEASE before flush / stale SETUP_CONF | 8313 (drop one SETUP_CONF) so the scan aborts and the CONF is stale | `sendStaleSetupReclaim` releases the state with zero transid; 2361 clean |
| PK-7 | duplicate identity | two SETUPs with the same transid + queryTag from `testJoinAgg` | second gets SETUP_REF InvalidRequest (release builds); first still releasable |
| PK-8 | parked replay after coordinator death | NF-12 above | covered in Phase 1; listed here for the parking matrix |

PK-1..PK-7 live in `testCteProtocol` (PK-1, PK-2 need a running CTE
query: reuse the NDB API helper).

MTR deliverables of this phase: `cte_park_basic.test` (`ALL ERROR 5127`
around the filter and outer-join bodies from `body_filter.inc`, results
identical to the baseline) and `cte_park_sweeper.test` (`ALL ERROR 5139`,
one query expected to fail with 1251, then a clean re-run), both in
`ronsql_cte`.

Effort: 3.5 days including the two capacity inserts and the MTR files.

---

## 7. MTR cases (developed inside Phases 1-3)

Maintainer direction: MTR coverage is written phase by phase, next to the
NDBT or block-level case that establishes each window, not as a trailing
phase. Six files in `mysql-test/suite/ronsql_cte*/t`, each with
`--source include/have_ndb_error_insert.inc` and a recorded `.result`:

| File | Phase | What it does |
|---|---|---|
| `cte_nodefail_peer.test` (suite `ronsql_cte_ng2r2`) | 1 | long multi-node CTE query in a `--send`, `2 ERROR 5133`, `2 RESTART -n` while paused, `--reap` expects error, `ndb_waiter`, re-run query, `ALL DUMP 2361/2362/2363/2560` |
| `cte_nodefail_coordinator.test` (`ronsql_cte_ng2r2`) | 1 | same with the TC node of the rdrs connection killed (`8312` on that node) |
| `cte_redist_pages.test` (`ronsql_cte`) | 2 | `ALL ERROR 5134`, wide GROUP BY CTE redistributed across nodes, result equals baseline |
| `cte_scan_batches.test` (`ronsql_cte`) | 2 | CTE scan over several batches, clean run then `ALL ERROR 5129` expecting 1251 |
| `cte_park_basic.test` (`ronsql_cte`) | 3 | `ALL ERROR 5127`, filter and outer-join bodies from `body_filter.inc`, results identical to baseline; `ALL ERROR 0`; `ALL DUMP 2363` |
| `cte_park_sweeper.test` (`ronsql_cte`) | 3 | `ALL ERROR 5139`, one query fails with 1251, clean re-run, `ALL DUMP 2363` |

MTR cannot read a DUMP's output, so the leak dumps rely on the crash they
raise; each test asserts the cluster is still up afterwards
(`$NDB_WAITER`).

---

## 8. Phase 5 - registration and soak

- Add every new NDBT case to `daily-basic--01-tests.txt` (format `cmd:` /
  `args: -n <Case> T1` / `max-time:`); 4-node cases also to
  `16node-tests.txt`.
- Run each node-failure case 50 times in a loop on a 4-node dev cluster
  before enabling in autotest; timing windows that never trigger in 50
  runs get a wider insert (e.g. raise 5133's delay) rather than a sleep in
  the test.
- Effort: 1 day.

---

## 9. Order of work and estimates

| Phase | Days | Depends on |
|---|---|---|
| 0 hooks | 2.5 | - |
| 1 NDBT node-failure + 2 MTR files | 4.5 | 0, CTE query helper |
| 2 protocol / state machines + 2 MTR files | 5.5 | 0 |
| 3 parking + 2 MTR files | 3.5 | 0, helper |
| 5 registration + soak | 1 | 1-3 |
| **Total** | **17** | |

There is no separate MTR phase (the former Phase 4); its files are listed
in §7 with the phase that produces them. Phases 1, 2 and 3 are
independent once Phase 0 is in and can be split between people.

---

## 10. Acceptance criteria

1. Every row in §1 has at least one test whose failure mode is the
   symptom in that row (traceability column in each test's header comment
   names the commit).
2. All four leak dumps are run at the end of every node-failure case and
   never fire in 50 consecutive runs.
3. No new case takes longer than 180 s on a 4-node cluster; node-kill
   cases under 600 s including recovery.
4. `ronsql_cte*` suites and the existing `JoinAggNodeRestart` /
   `JoinAggErrorInsert` cases still pass with all new inserts compiled in
   and cleared.

---

## 11. Risks and open points

- **Timing windows.** Peer-failure windows depend on a paused
  redistribution or held reply; the inserts in §3.1 make them
  deterministic on the DBLQH side, but the kill itself still races the
  10 ms sweeper and the 20 ms SETUP hold. The 3-iteration loop plus the
  50-run soak is the mitigation; cases must not fail when the window is
  missed, only skip the assertion that depends on it and log it.
- **Topology.** Coordinator-not-requester and peer cases need 4 data
  nodes; the 2-node default suite cannot run them. They skip on 2 nodes.
- **DBSPJ dump handler does not exist.** Adding `execDUMP_STATE_ORD` to
  DBSPJ is small but touches `DbspjInit.cpp` signal registration.
- **CTE query helper.** Extracting the NDB API CTE builder from
  `testCteNdbApi.cpp` is the one refactor on the critical path; keep it
  header-only to avoid a new library.
- **Not deterministically testable.** The CTE lookup drain-by-full-mask
  path for a target that was connected but not yet in `c_alive_nodes` at
  build time needs a node in an early start phase during the query; left
  to the soak loop with a starting node in the cluster (NF-3 variant,
  optional).
- **Error-insert numbering.** Codes above were checked free on
  2026-09-11; re-check before landing Phase 0.
