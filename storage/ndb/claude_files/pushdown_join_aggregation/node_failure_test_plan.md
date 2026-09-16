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
| New error inserts | 28 (DBLQH 5128-5150, DBTC 8311-8313, DBSPJ 17532-17533) | kernel blocks |
| New DUMP codes (leak checks) | 5 (LQH 2362-2364, TC 2560, SPJ new handler + 1 code) + LQH 2365 (park statistics, not a check) | kernel blocks |
| NDBT node-failure cases | 12 + PK-8 | `testNodeRestart` (new `testCteNodeFail` if the binary grows too large) |
| Block-level protocol / state-machine cases | 14 | `block_unit_test/testCteDbtc`, `testJoinAgg`, new `testCteProtocol` |
| Parking cases | 8 | PK-1..PK-7 in `block_unit_test/testCteProtocol`, PK-8 (a node kill) in `testNodeRestart` beside NF-11 / NF-12 |
| MTR cases | 7 RonSQL test files developed inside Phases 1-3 (maintainer direction) + 13 wrappers of the NDBT cases - **done** | `mysql-test/suite/ronsql_cte` (4-node variant `ronsql_cte_ng2r2`), wrappers in `ndb_cte` / `ndb_cte_ng2r2` |
| Autotest registration | all NDBT cases - **done** | `storage/ndb/test/run-test/daily-basic--16-tests.txt` (next to `JoinAggNodeRestart`), 3- and 4-node cases also in `16node-tests.txt` |

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

### 2.3 Layer C - MTR (`mysql-test/suite/ronsql_cte*` and the new `suite/ndb_cte`)

Every NDBT case also gets a one-file MTR wrapper in the new suite
`mysql-test/suite/ndb_cte` (same cluster config as suite `ndb`, kept
separate so that suite does not grow further), so a case runs as
`./mtr --suite=ndb_cte <file>` with MTR owning the cluster. The wrapper
follows `suite/ndb/t/ndb_lcp_scanned_bit_churn.test`, not
`run_ndbapitest.inc`: in this tree a table created through the NDB API
crashes a live mysqld, so both mysqlds are shut down around the NDBT run
and the API-side tables are dropped before they restart. The wrapper
needs `include/have_ndb_debug.inc` for the error inserts. The RonSQL-driven files below
stay in `ronsql_cte*`.

`ronsql_cte` runs 2 data nodes, NoOfReplicas=2; `ronsql_cte_ng2r2` runs 4.
Cases that need a CTE partition scanned from another node than its owner
(NF-4) get their wrapper in `suite/ndb_cte_ng2r2`, the 4-data-node copy of
`suite/ndb_cte`.
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
All codes from 5128 on are armed on the query-thread LQH instances as
well as on the LDMs (DblqhProxy forwards them to DbqlqhProxy; F-8), since
TRPMAN may run the hooked request on either kind of instance.

| Code | Where | Effect | Window it opens |
|---|---|---|---|
| 5128 | DBLQH `cteScanEmitResults`, before the CONF send | send the batch's TRANSID_AI rows, then drop the CTE_SCAN_CONF once | DBSPJ slot has rows but no reply: kill the DBLQH node (slot drain by NODE_FAILREP), or let it time out |
| 5129 | DBLQH `cteScanReqImpl`, continuation path | answer a continuation REQ with CTE_SCAN_REF(ZJOIN_AGG_STATE_NOT_FOUND) once, releasing the token | DBSPJ REF path with rows already counted |
| 5130 | DBLQH `cteScanAggFeed` | force `CTE_SCAN_AGG_FEED_BATCH` = 1 while set | long agg-feed continuation chain: kill requester or coordinator mid-chain |
| 5131 | DBLQH `cteLookupReqImpl` | delay the CTE_LOOKUP_CONF/REF 50 ms once (park in a CONTINUEB) | lookup reply in flight when the target node is killed |
| 5132 | DBLQH `joinAggNullRowReqImpl` | REF once with ZJOIN_AGG_INTERPRETER_ERROR | `execJOIN_AGG_NULL_ROW_REF` accounting |
| 5133 | DBLQH `execJOIN_AGG_REDISTRIBUTE_REQ` | delay the flow-control CONF 200 ms while set | sender paused in CTE_REDISTRIBUTING (the "paused redistribution" window for coordinator kill) |
| 5134 | DBLQH `execJOIN_AGG_REDISTRIBUTE_REQ` | queue every incoming redistribution row, one entry per page (minimum 512 bytes), until the owner's own redistribution and all declared peer requests are complete; the extra value is the cookie of the detached chain's `[CTE_REDIST_PAGES_FREED node=N pages=P cookie=C]` event | a drain that leaves far more than 256 pages behind, deterministically (page-free chain split, PG-1) |
| 5135 | DBLQH `execSCAN_NEXTREQ`, close of a join-agg / CTE scan | ignore the close once (no SCAN_FRAGCONF) | DBTC CLOSING_SCAN with one close owed: kill this node |
| 5136 | Proxy `execJOIN_AGG_RELEASE_REQ` | re-send the same RELEASE to itself once | duplicate release during teardown |
| 5137 | Proxy `continueJoinAggTeardown` | `JOIN_AGG_TEARDOWN_GROUPS_PER_BATCH` = 1 while set | long teardown chain overlapping NF reclaim / duplicate release |
| 5138 | Proxy `execJOIN_AGG_SETUP_REQ` + DBLQH `joinAggParkSweep` | hold the selected SETUP_REQs (extra: 0 all, 0xFFFF main, 0xFFFE none, else cteIndex + 1) and every placeholder sweeper until cleared | switch extra to 0xFFFE to release SETUP while sweepers remain held; clear after replay (PK-1) |
| 5139 | Proxy `execJOIN_AGG_SETUP_REQ` | leave one identity unfilled, send SETUP_REF 1251 after 200 ms | placeholder never filled: sweeper REF path for every parked GSN |
| 5140 | DBLQH `execJOIN_AGG_REDISTRIBUTE_REQ` | hold inbound redistribute requests with RI_NEED_CONF, 200 ms at a time, until cleared; other rows proceed without timer entries; `[CTE_NF2_CONF_HELD node=P iteration=I requester=R]` on first arrival and, for a matching state owned by this worker, `[CTE_RONSQL_REDIST_HELD node=P cookie=I requester=R coordinator=C]` | senders paused in CTE_REDISTRIBUTING for as long as the kill needs (NF-2); the RonSQL peer test picks its victim from the second event |
| 5141 | DBLQH `cteLookupReqImpl` | hold every inbound CTE lookup, 200 ms at a time, until cleared; one event per instance for the first remote probe (extra bit 30: for the first probe from any node) | DBSPJ workers on the other nodes keep probes charged to this node for as long as the kill needs (NF-3); LK-1 closes during the hold |
| 5142 | DBLQH `cteScanEmitResults` | rows sent, then swallow the CTE_SCAN_CONF of every remote requester while set; one event per instance | a remote DBSPJ worker holds this source's batch without a reply for as long as the kill needs (NF-4) |
| 5143 | DBLQH `cteScanEmitResults` | report each saved iterator for a remote requester as `[CTE_NF5_SCAN_PAUSED node=S iteration=I requester=R]`; rows and CONF delivered normally | identifies the actual requester of a paused remote scan before the kill (NF-5); cleared by the test |
| 5144 | DBLQH `cteScanAggFeed` | hold every aggregation feed continuation between rounds, 20 ms at a time, until cleared (after the requester / coordinator down checks); `[CTE_AGG_FEED_HELD node=S iteration=I requester=R]` once per instance for a remote requester and `[CTE_RONSQL_FEED_HELD node=S cookie=I requester=R coordinator=C]` once per instance on a non-coordinator source (extra: low 29 bits = iteration / cookie, bit 29 = clear on NODE_FAILREP so RonSQL retries do not stall behind the hold, top two bits = event flags) | the feed is still running when its requester (NF-6) or the coordinator (NF-7, the RonSQL coordinator test) is killed; cleared by the test, or by NODE_FAILREP with bit 29 |
| 5145 | Proxy `execJOIN_AGG_SETUP_REQ` + DBLQH `joinAggParkSweep` / `parkJoinAggConsumer` | hold every SETUP while its coordinator lives; LDM/query instances hold placeholder sweepers until NODE_FAILREP clears their local insert; `[CTE_NF11_PARKED node=P iteration=I requester=R]` per instance and requester | consumers stay parked until the requester (NF-11) or the coordinator (NF-12) is killed; NF-11 observes the sweep, NF-12 observes late CTE SETUP rejection before state allocation; cleared by the test |
| 5146 | Proxy release / teardown / node-failure reclaim | hold teardown for remote coordinators until cleared; report held and skipped states with iteration, coordinator and pool key | NF-10 kills the coordinator after the hold event and requires reclaim to skip the same key before clearing |
| 5147 | DBLQH `cteScanEmitResults` | hold the CTE_SCAN_CONF of every local requester after its rows went out (batches short of EndOfData), 100 ms at a time via CONTINUEB, until cleared; `[CTE_SCAN_CONF_HELD node=S iteration=I requester=R]` per held reply | the API closes while DBSPJ's slot still owes the batch, so the close must wait on `close_pending` (SM-3) |
| 5148 | Proxy + DBLQH park paths | the 5138 hold of every SETUP and sweeper, with extra capping the park pool: a consumer is refused once `extra` records are in use | park pool exhaustion with the pool far from full (PK-4) |
| 5149 | Proxy `execJOIN_AGG_SETUP_REQ` | refuse the SETUP with OutOfQueryMemory once the identity table holds `extra` entries | identity table exhaustion (PK-5) |
| 5150 | Proxy `execJOIN_AGG_SETUP_REQ` + DBLQH `joinAggFlushParked` / `joinAggParkSweep` | hold every SETUP until a NULL_ROW_REQ has parked on its identity (sweepers held meanwhile), let it succeed, then hold the replay of the parked consumers on each instance behind one re-check timer until NODE_FAILREP clears the instance's insert; `[CTE_PK8_REPLAY_HELD node=P iteration=I instance=B park=R coordinator=C]` selects one held NULL_ROW per instance and arming; its synchronous replay into the coordinator guard reports `[JOIN_AGG_NULL_ROW_REJECTED node=P failed=C iteration=I instance=B park=R]`; the test requires this matching pair | consumers parked before a successful SETUP are replayed only after their coordinator failed (PK-8) |
| 8311 | DBTC `sendJoinAggCompleteReqs` | send COMPLETE_REQ with aggStateKey RNIL for one node even if the key is known | identity-addressed COMPLETE parks or resolves |
| 8312 | DBTC `sendJoinAggReleaseReqs` / `releaseJoinAggResources` | CRASH_INSERTION right after the RELEASE_REQs are sent | coordinator dies with releases in flight: reclaim vs teardown overlap |
| 8313 | DBTC `execJOIN_AGG_SETUP_CONF` | delay ONE SETUP_CONF 5 s and emit the hold event | stale-SETUP reclaim path (`sendStaleSetupReclaim`) and RELEASE identity with zero transid |
| 17532 | DBSPJ `cte_scan_sendReq` | after sending, crash-insert the local node once the second batch is requested | multi-batch CTE scan with paused sources on the requester side |
| 17533 | DBSPJ `execSCAN_NEXTREQ`, close from DBTC | swallow the close once, request left waiting (logs when it fires) | DBTC CLOSING_SCAN with a worker's close reply owed: kill this node (NF-1) |

Effort: 1.5 days including guide entries.

### 3.2 Leak-check DUMP codes

Modelled on `LqhDumpJoinAggStates` (2361): log every leaked record with
enough identity to triage, then `ndbabort()` so the leak shows as a node
crash in autotest.

| Code | Block | Checks |
|---|---|---|
| 2362 `LqhDumpCteIterStates` | DBLQH, every LDM and query instance | `c_cteScanIterStatePool` used == 0 (log senderNodeId, coordinatorNodeId, aggFeed); two-word form emits the JOIN_AGG_LEAK_CHECK_OK cookie event like 2361 |
| 2363 `LqhDumpJoinAggIdentity` | DBLQH instance 1 (shared table) | identity table has no entries and no placeholders; park pool has no records (`s_jaiFreeHead` chain length == JAI_MAX_PARK) |
| 2364 `LqhDumpCteRedistPages` | DBLQH instance 1 (debug builds) | node-wide redistribution page count is zero, including lists detached from released states; two-word form emits the cookie event (run after asynchronous cleanup) |
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
| NF-1 | `CteCloseOwedByFailedNode` | 17533 on W: DBTC's close swallowed by every DBSPJ worker there | W | DBTC ScanRecord leaves CLOSING_SCAN; API receives EndOfData; `TcDumpJoinAggRecords` clean (`c7faa193ac2`) |
| NF-2 | `CtePeerDiesDuringRedistribute` | 5140 on P: inbound redistribute requests with RI_NEED_CONF held, their senders paused on its CONF | P | survivors' CTE states go ERROR / NODE_FAIL_ABORT via identity sweep; COMPLETE_REF reaches DBTC exactly once (`55e99285ebb`) |
| NF-3 | `CteLookupTargetDies` | 5141 on P: every inbound CTE lookup held, the requesters' probes outstanding | P | DBSPJ drains `m_nodeOutstanding[P]`; request completes with 286 / 20016 (`6c7fa88dcaa`) |
| NF-4 | `CteScanSourceDiesMidBatch` | 5142 on P: rows sent, the CONF to every remote worker swallowed (scanCte main query; P = an owner scanned from another node, skips when there is none) | P | slot retired by `cte_scan_execNODE_FAILREP`; request completes with 286 / 20016; 2362 clean on survivors (`f718b5be5d6`) |
| NF-5 | `CteRequesterDiesPausedScan` | 5143 on S reports a saved iterator and its actual remote requester R; API holds scanCte after its first row; choose S with both S and the coordinator outside the routing replica set | R | S logs `CTE_SCAN_ITER_RELEASED` for R; close returns; 2362 clean (`c3ce0732890`) |
| NF-6 | `CteRequesterDiesAggFeed` | 5144 on S holds the aggregation feed (FeedChain: CTE 1 over scanCte(CTE 0)) and names its remote requester R; S chosen with S and the coordinator outside the routing replica set | R | S logs `CTE_AGG_FEED_ABANDONED` for R (continuation stopped on ZNODE_DOWN, no REF); R rejoins (NF completion not stalled); query fails 286 / 20016; 2362 clean (`c3ce0732890`) |
| NF-7 | `CteCoordinatorDiesAggFeed` | 5144 on S as in NF-6; C chosen up front (startTransaction hint) so that an isolated source S != C exists; R named by the event | C | S logs `CTE_AGG_FEED_REFUSED` for C (continuation REFs the live requester); states reclaimed, 2361 clean; query fails 286 / 20016 / API 4010, 4025, 4028, 4031 (`c13a662bc93`) |
| NF-8 | `CteCoordinatorDiesReady` | 5141 on P holds every probe, the query sits on CTE_READY states | C | P logs `JOIN_AGG_RELEASES_QUEUED` for C (proxy queued releases; leak checks verify completed reclamation); query fails 286 / 20016 / API 4010, 4025, 4028, 4031; 2361 clean (`ae810a9dc73`) |
| NF-9 | `CteCoordinatorDiesPausedRedist` | 5140 on P holds redistribute requests with RI_NEED_CONF (one group per row); wait for a held sender other than C, proving a surviving owner is paused on P's CONF | C | P logs `JOIN_AGG_RELEASES_QUEUED` for C (proxy queued releases; leak checks verify completed reclamation); query fails 286 / 20016 / API 4010, 4025, 4028, 4031; 2361 clean (`0f983279eb6`) |
| NF-10 | `CoordinatorDiesReleaseInFlight` | 5146 on P holds teardown after RELEASE takes ownership; wait for CTE_NF10_TEARDOWN_HELD naming C and a pool key | C | P reports CTE_NF10_RECLAIM_SKIPPED for the same iteration, C and key; query returns every row or fails with a node-failure error after the kill; clear 5146, restart C and require leak checks clean (`b30c78c0be0`) |
| NF-11 | `CteRequesterDiesParked` | 5145 on P with the cross-node leaf (feeds of every node park at P); R = a reported parked requester other than C (3 nodes) | R | P logs `JOIN_AGG_PARK_SWEPT` naming R (sweeper REFs to the dead node dropped, live requesters aborted, placeholder cleaned); query fails 286 / 20016 / 1251; confirmed hold, kill and sweep required; 2363 clean |
| NF-12 | `CteCoordinatorDiesParked` | 5145 on P with the cross-node leaf; C killed once a parked requester was reported (C's own allowed) | C | the SETUP hold releases on C's disconnect, P logs `JOIN_AGG_SETUP_REJECTED` (CTE owner-list validation rejected the late SETUP before state allocation); query fails 286 / 20016 / API 4010, 4025, 4028, 4031; after clearing the insert and restarting C, 2363 and 2361 clean |

NF-2 uses one distinct group per source row to cross the 64 KiB
redistribution flow-control threshold. Its killer subscribes before the
query is armed and waits for the victim's `CTE_NF2_CONF_HELD` InfoEvent
matching its node and iteration, emitted only for a held remote
`RI_NEED_CONF` request. It then
kills the victim immediately. Only errors 286 and 20016 pass; TC and API
timeouts fail the test. A missing hold event fails without issuing the kill.
NF-3 uses the same held-signal driver (`runCteNfHoldQuery` /
`runCteNfHoldKiller` in `testNodeRestart.cpp`, parameterised by a
`CteNfHoldCase`): 5141 with `CTE_NF3_LOOKUP_HELD`, emitted once per DBLQH
instance for the first remote probe it holds, default groups.
NF-4 uses it with the ScanRoot shape (main = scanCte) and 5142 with
`CTE_NF4_CONF_HELD`. Its victim is derived from the registered
`cte_nf_virtual` table's placement: the worker for root fragment K runs
on a replica of that fragment and scans the K-th owner in ascending node
order. Choose an owner outside the entire replica set returned by
`getFragmentNodes`, excluding the coordinator, so its requester survives
regardless of dynamic primary selection or read-backup routing. Skip if
no owner qualifies. The 2-node cluster has no such owner; the wrapper uses
`ndb_cte_ng2r2` (2 node groups x 2 replicas, otherwise suite `ndb_cte`).
NF-5 arms 5143 on a source outside its routing fragment's replica set,
also requiring the coordinator to be outside that set. Its event
`[CTE_NF5_SCAN_PAUSED node=S iteration=I requester=R]` identifies the
actual requester after a scan iterator is saved. Rows and CONF are
delivered normally. The API holds the query after its first row, and the
pre-close hook waits for the event before killing R. It then requires
`[CTE_SCAN_ITER_RELEASED node=S failed=R count=N]` from the same source.
The source's diagnostic hook is cleared after the query closes.
After restarting the victim, reject close errors other than node-failure
reports and require close to finish within 30 s (5 s with the reduced
API protocol timeout debug flag). The duration check also catches a
timeout that close subsequently clears while draining the current batch.

NF-6 goes back to the held-signal driver with two additions: the picked
node is the armed source (5144, same isolation rule as NF-5 but on
`cte_nf_src`, the routing table of the FeedChain shape), the event names
the requester and that requester is killed (`CTE_NF_KILL_EVENT_REQUESTER`),
and after the kill the driver requires a post-kill marker from the armed
node (`postKillTag`): `[CTE_AGG_FEED_ABANDONED node=S failed=R]`, which
`cteScanAggFeed` now emits, in every build, when it stops on the
requester's ZNODE_DOWN. On success, the armed node's insert is cleared
after the post-kill marker and query completion checks. A cleanup guard
also clears it on error exits; the query step handles failed arming and
a stop during the handoff to the killer. The driver's baseline and
post-recovery checks run the case's own shape.
NF-7 reuses the held feed with the kill target set to the coordinator
(`CTE_NF_KILL_COORDINATOR`): the case chooses the coordinator before the
transaction starts (`Options::tcNodeId`, `startTransaction(node, 0)`) as
a node for which an isolated source other than itself exists, the event
still names the requester (checked distinct from both), and the post-kill
marker is `[CTE_AGG_FEED_REFUSED node=S failed=C requester=R]`, emitted
by `cteScanAggFeed` when it stops on the coordinator's ZNODE_DOWN and
REFs the requester. The accepted query errors add the NDB API's own
node-failure aborts (4010, 4025, 4028, 4031) for coordinator kills.
NF-8 and NF-9 are the coordinator kills of the NF-3 and NF-2 holds (5141
and 5140 on a peer) with the same kill target. The 5140 event names the
redistribution sender (`eventNamesRequester` true); NF-9 ignores events
from the coordinator and waits for a surviving sender within the same
30 s deadline. It skips clusters with fewer than three data nodes.
The 5141 event has no requester field. Their post-kill marker is
`[JOIN_AGG_RELEASES_QUEUED node=P
failed=C count=N]`, which `DblqhProxy::execJOIN_AGG_NODE_FAIL_REP` now
emits, in every build, after queueing release requests for a failed
coordinator's states. The count is queued releases, not completed teardown;
the later leak checks verify that reclamation finished.
NF-8 runs on 2 nodes; NF-9's wrapper is in the 4-node suite so paused
states exist on surviving peers.
NF-11 and NF-12 use the held-signal driver with insert 5145, which
combines the SETUP hold (proxy half, released once the coordinator is no
longer connected), the placeholder sweeper hold (LDM/query instances
clear their local insert on NODE_FAILREP) and the parked-requester event.
The hold stays off for the rest of the iteration, so requests still
arriving from surviving nodes cannot create another held placeholder.
The test explicitly clears the peer's remaining insert, including the proxy.
The cross-node leaf (`CteQueryUtil::Options::crossNodeLeaf`: CTE0's leaf
looks up pk = grp) makes every node's feeds land on remote nodes, so
consumers of P park from every other node. NF-11 kills a reported
requester and waits for `[JOIN_AGG_PARK_SWEPT node=P failed=R count=N]`,
which `joinAggParkSweep` now emits in every build. NF-11 also accepts
1251: the sweep sends STATE_NOT_FOUND to live requesters, and this error
can reach DBTC before its node-failure error. This allowance is local to
NF-11 and still requires the confirmed hold, kill and sweep.
NF-12 kills the
coordinator (its own parked requests are allowed, `allowCoordinatorRequester`)
and waits for `[JOIN_AGG_SETUP_REJECTED node=P failed=C]`, emitted by
the proxy after rejecting the held CTE SETUP. The original owner list
includes C, so owner-list validation rejects that SETUP once C is
disconnected, before any state allocation or parked-request flush.
The marker proves rejection; the leak dumps after clearing the insert
and restarting C verify that parked requests and placeholders were freed.
NULL_ROW replay after coordinator failure (`5efa38683b1`) is the
separate case PK-8 (`CteCoordinatorDiesParkedReplay`, section 6); NF-12
does not exercise that handler.
NF-10 arms 5146 on a peer after subscribing to management events.
RELEASE removes the identity and acknowledges normally, but the peer
holds the teardown and pool record. The killer waits up to 30 s for
CTE_NF10_TEARDOWN_HELD naming the query's coordinator, then kills that
coordinator. It requires CTE_NF10_RECLAIM_SKIPPED for the same peer,
iteration, coordinator and key within 30 s. The held key cannot be
recycled, so this proves reclaim encountered the original teardown.
The query may return every row before the kill; otherwise only a
node-failure error after the confirmed hold and kill is accepted.
Receive and close timeouts fail. Once the matching skip and query
completion are observed, clear the peer's insert, restart the coordinator
and verify the pools are clean. The insert guard also clears the hold
on failure. Runs on 2 nodes; crash insert 8312 and the restart-policy
override are no longer needed by this case.

Each case runs 3 iterations to shake timing. Registration in
`daily-basic--01-tests.txt` next to the existing `JoinAggNodeRestart`
entry, `max-time: 1800`, happens in Phase 5.

MTR deliverables of this phase (see §7 for the shared idioms):
`cte_nodefail_peer.test` and `cte_nodefail_coordinator.test` in
`ronsql_cte_ng2r2` use a background `ronsql_cli` under mysqltest's Perl
driver. Cookie-matched cluster-log holds identify a redistribution peer
(5140) or the coordinator of an aggregation feed (5144). The latter does
not require the requester to survive; NF-7 covers that three-role kernel
case. The CLI coordinator test sets extra bit 29 of 5144: every worker
clears its hold in NODE_FAILREP, allowing retries to progress even while
management-side cleanup is pending. NF-6/NF-7 do not set that option.
Both tests require bounded completion, an exact result after retry
or a recognized node-failure error with no output, a clean recovery query,
and cookie-matched leak-check acknowledgements. They cover CLI integration,
not the RDRS HTTP retry loop.

Effort: 4.5 days including the shared CTE query helper (1 day) and the
two MTR files (0.5 day).

---

## 5. Phase 2 - protocol and state-machine cases (Layer B)

New binary `block_unit_test/testCteProtocol.cpp` (SignalSender, links
NDBTEST) plus additions to `testJoinAgg` and `testCteDbtc`. Deterministic:
no node kills, error inserts only where a real DBLQH reply must be shaped.
Status: `testCteProtocol` exists with sections 5.3 (TD-1 .. TD-8) and
5.4 (ID-1 .. ID-6), MTR wrapper `suite/ndb_push_agg/t/testCteProtocol.test`
(the suite that runs the other block tests; `NDB_PUSH_AGG_DIR` locates the
binary). Section 5.3 drives one data node: SETUP (legacy or full length),
scan feeds to populate groups, COMPLETE with the result stream, RELEASE
variants. Section 5.4 builds a CTE state on every data node (CTE-mode
SETUP, fragment scans, COMPLETE with the per-node key triples, so it
works on one node as well as on the multi-node suites) and forges the
peer and consumer signals against it. COMPLETE collection requires one
reply from each expected owner, with matching requestId and senderData;
duplicate replies cannot substitute for a missing participant.
ID-5 opens a second cluster
connection so a token can be presented from another API node id (the
sub-case is skipped when no node id is free). After every case DUMP
2361 / 2362 / 2363 run on every node after a settle time for the
teardown chain. Each dump carries a cookie and must produce its matching
JOIN_AGG_LEAK_CHECK_OK event. Both management return values are checked;
all data nodes must remain STARTED with unchanged connection counters
across verification, so an automatic restart cannot hide a leak crash.

Sections 5.1 and 5.2 live in `testCteDbtc` (Tests 12 to 15), which
drives DBTC with hand-built SCAN_TABREQs and therefore controls every
SCAN_NEXTREQ: it can acknowledge one batch at a time and close while a
batch is in flight, which no NDB API client can do. Test 12 to 14 use
a new scanCte main root tree (QN_CTE_SCAN, the pass-through shape RonSQL
emits for `SELECT ... FROM cte`) with the batch size carried in
SCAN_TABREQ; rows are awaited per CONF the way the NDB API does, since
they travel from DBLQH to the API on their own path. The leak
verification (`JoinAggTestUtil.hpp`, shared with `testCteProtocol`)
covers 2361 / 2362 / 2363 / 2560 / 2650, each with the cookie event.

### 5.1 DBSPJ CTE scan slot model (via DBTC harness + DBLQH inserts)

Driven from `testCteDbtc` (which already observes SCAN_TABCONF /
SCAN_TABREF and result rows):

| ID | Sequence forced | Expected |
|---|---|---|
| SM-1 | scanCte main root (hand-built QN_CTE_SCAN tree, Test 12) paced at batchSize 2 over 10 groups; the API acknowledges each batch | no fragment op declares more than 2 rows, five row batches on one data node (at least five when spread), every group once, EndOfData; all five leak dumps clean - **done** |
| SM-2 | 5129: REF on the first continuation (Test 13) | SCAN_TABREF 1251 with closeNeeded, the close completes; 2362 clean (token released by DBLQH), 2650 / 2560 clean - **done** |
| SM-3 | close during an in-flight batch (Test 14): 5147 is armed before starting the scan and holds the first nonfinal local batch's CTE_SCAN_CONF after its rows went out (5128 drops it for good, which nothing but node failure recovers, see F-4), the API closes after the hold event | the close does not complete while the reply is held (`close_pending`), completes after the insert is cleared; the deferred close request releases the token, 2362 clean - **done** |
| SM-4 | multi-source scan (`m_cteScanAllNodes`) where one source returns EndOfData first | slot ended; other slots continue; completion only when all ended. **Not run:** the mode is DBTC's decision when a node has fewer DBSPJ instances than the cluster has data nodes (`CTE_SCAN_ALL_NODES` in the aggKeys section); no request-side switch exists and the MTR clusters never take it |
| SM-5 | 5128 then timeout instead of node kill | **Not run**, see F-4: DBTC's fragment timeout only re-issues the close, DBSPJ cannot answer it while the batch obligation is open, so the scan stays CLOSING_SCAN and the request pending until the node fails (NF-4 covers the failure) |

### 5.2 CTE lookup accounting

| ID | Sequence | Expected |
|---|---|---|
| LK-1 | 5141 holds every lookup (extra bit 30 reports the first held local request), the CTE_LOOKUP main select of Test 5, API close after the hold event (Test 15) | the close does not complete while the replies are held, completes once they drain after the insert is cleared; 2650 clean - **done** |
| LK-2 | `testCteNdbApiOuterJoin` Test 7: aggregating `scanTable(oj_rhs_nk) LEFT JOIN lookupCte` over a nullable join column (one NULL key row, one miss) with 5132 on every data node | control COUNT=4 SUM=80; with the insert the query fails with 1253 instead of hanging (`execJOIN_AGG_NULL_ROW_REF` retires the reply, `84f15ad6454`), all five leak dumps clean, the control succeeds again - **done** |

### 5.3 Proxy teardown and RELEASE identity (`testJoinAgg`, direct SETUP/RELEASE)

| ID | Sequence | Expected |
|---|---|---|
| TD-1 | SETUP, then RELEASE twice back to back (noReply 0) | two CONFs, one teardown; 2361 clean - **done** |
| TD-2 | arm 5146 with checked management results; RELEASE, require the matching hold event, then second RELEASE mid-chain | second RELEASE CONFs and starts nothing; cleanup guard clears the insert on failure too; 2361 clean after clearing - **done** |
| TD-3 | RELEASE with wrong requestId | ignored, CONF sent, state still alive (a COMPLETE afterwards returns the scanned groups); then correct RELEASE - **done** |
| TD-4 | populate groups, RELEASE with wrong senderRef (another SignalSender), then COMPLETE from the owner | ignored + CONF to the other sender; COMPLETE returns the scanned groups, then the owner releases the state - **done** |
| TD-5 | RELEASE with zero transid and correct requestId (stale-reclaim form) | accepted - **done** |
| TD-6 | 5136 (proxy re-sends to itself) | two CONFs, exactly one teardown; 2361 clean - **done** |
| TD-7 | SETUP_REQ (full length) whose `setupNodes` lists a node id that is not a connected data node; then the same request naming exactly the data nodes | JOIN_AGG_SETUP_REF 286, no state seized, 2363 clean; the second accepted (`cte_owner_list.md`) - **done** |
| TD-8 | CTE-mode SETUP_REQ of `SignalLength_v1` (13 words) | accepted; owner list = the receiver's connected data nodes (block-test fallback) - **done** |

### 5.4 Identity validation on keyed peer signals (`testCteProtocol`, hand-built signals)

Set up a CTE state on every data node with the direct SETUP / scan /
COMPLETE flow (the keys and owner instances come from SETUP_CONF), then
send forged signals to the owner instance; every forgery names the
state's real pool key, the slot-reuse case the identity checks exist for:

| ID | Signal | Forgery | Expected |
|---|---|---|---|
| ID-1 | REDISTRIBUTE_REQ | wrong identWord, then wrong transid, valid key | REF 1251 to the test echoing its senderAggStateKey, identWord and transid; the state still reaches CTE_READY and a scan returns every group - **done** |
| ID-2 | REDISTRIBUTE_CONF | wrong transid, then wrong identWord, the state's key as senderAggStateKey | ignored (an accepted CONF would resume a redistribution never started); the state completes normally - **done** |
| ID-3 | REDISTRIBUTE_REF | wrong identWord | ignored, the state completes normally; control: the correct identity aborts the state and its COMPLETE answers COMPLETE_REF 1251 - **done** |
| ID-4 | FINAL_REP | wrong identWord, then wrong transid, each with success and error reports | ignored; the state completes normally; accepting an error report aborts even a single-node state - **done** |
| ID-5 | CTE_SCAN_REQ continuation | a token no record had; a paused scan's live token from another API node id; the token after EndOfData; a token released by a close request | CTE_SCAN_REF 1251 each, no rows; the legitimate continuations return every group; 2362 clean - **done** |
| ID-6 | CTE_SCAN_REQ / CTE_LOOKUP_REQ / NULL_ROW_REQ | coordinatorRef = DBTC on a node id that never started (hostRecord keeps it ZNODE_DOWN, the same test a `-n` restart leaves behind) | REF 286 before any state access; the same requests naming DBTC on a live data node and an unknown key answer 1251 - **done** |
| ID-7 | any of the above | shorter-than-required signal length | node asserts (documents the `>=` contract); not run, it would crash the node |

### 5.5 Page-free chain

| ID | Sequence | Expected |
|---|---|---|
| PG-1 | 5134 queues all incoming groups until local redistribution and declared peer requests complete, one entry per page | query matches MySQL; cookie-matched completed page chains from every node, at least one exceeding 256 pages; DUMP 2364 acknowledged by every node; clean query repeated - implemented in `cte_redist_pages.test`, validation pending |

DUMP 2364 checks a debug-only node-wide count of redistribution pages.
Unlike 2361, it includes detached lists whose aggregation state has
already been released. Allocation and all three free paths update the
count. Run it after asynchronous cleanup has finished; the optional
second word is a cookie acknowledged with JOIN_AGG_LEAK_CHECK_OK.

MTR deliverables of this phase, both in `ronsql_cte`:
`cte_redist_pages.test` (`ALL ERROR 5134` around a wide GROUP BY CTE over
lineitem, 1500 groups with 8 aggregate slots joined into a GROUP BY main
query; result equals the MySQL baseline, completed multi-batch freeing
and DUMP 2364 acknowledgements are required before the other five leak
dumps, then the query is repeated with normal pages; covers PG-1) and
`cte_scan_batches.test` (scanCte pass-through root over 1500 groups,
three batches per source, compared with MySQL; then `ALL ERROR 5129`
and the same query through `ronsql_cli`, which must exit 1 with NDB
error 1251 in its output; leak dumps; the clean scan again; covers SM-1
and SM-2). Both need `--record` on first run and the debug build.

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
| PK-1 | every GSN parks and replays | 5138 holds the selected SETUPs and the sweepers until cleared; DUMP 2365 counts parks per GSN. (a) direct: an identity-addressed COMPLETE, a redistributed row and a FINAL_REP parked before the SETUP; (b) NDB API: LookupMain with CTE0's SETUP held, ScanAggMain and OuterAggMain with the main SETUP held | (a) the flushed COMPLETE streams its result, the flushed row is in the CTE scan; (b) LQHKEYREQ, SCAN_FRAGREQ, LQHKEYREQ + NULL_ROW_REQ observed parked, every result correct after the release; a held SETUP blocks the feeds, so REDISTRIBUTE / FINAL_REP park only in (a) - **done** |
| PK-2 | identity-addressed COMPLETE | 8310 (delay SETUP_CONF) and 8311 (force RNIL key) on the coordinator | COMPLETE resolves by identity; result correct - **done** |
| PK-3 | sweeper REF path | 5139 (leave identity unfilled, delayed SETUP_REF drains DBTC accounting) | query fails with 1251 within 3 s; `JOIN_AGG_PARK_SWEPT` with failed=0; placeholder removed; all dumps clean - **done** |
| PK-4 | park pool exhaustion | 5148 (the 5138 hold with extra = 4 capping the park pool) | the fifth consumer takes the resource-error path, the query fails with 1251; the four parked are replayed into the aborting request after the release; dumps clean - **done** |
| PK-5 | identity table exhaustion | 5149 (extra = 0: every SETUP refused) | SETUP_REF OutOfQueryMemory, the query fails with 20008 - **done** |
| PK-6 | stale SETUP_CONF | 8313 (one SETUP_CONF delayed 5 s); a real state names DBTC with scan RNIL | require hold and stale-reclaim events for that request, then clean pools; no API query can release the state normally - **done** |
| PK-7 | duplicate identity | two SETUPs with the same transid + queryTag (`testCteProtocol`) | second gets SETUP_REF InvalidRequest 20002 in every build (the debug assert is gone: a REF is the safe answer); identity-addressed COMPLETE still returns the first state's scanned groups before release - **done** |
| PK-8 | parked replay after coordinator death | `testNodeRestart -n CteCoordinatorDiesParkedReplay`: 5150 on a peer P (OuterAggMain, so P's own DBSPJ injects NULL rows) lets the SETUP succeed once a NULL_ROW_REQ has parked, then holds the replay of the parked consumers per instance; C killed after `CTE_PK8_REPLAY_HELD` | the replays run after P's NODE_FAILREP: the NULL_ROW replays hit the failed-coordinator guard (`5efa38683b1`, P reports `JOIN_AGG_NULL_ROW_REJECTED` naming C and the same iteration, instance and park record as the held NULL_ROW), LQHKEYREQ replays abort with 286 or are swept once the state is reclaimed; query fails 286 / 20016 / API 4010, 4025, 4028, 4031; after clearing the insert and restarting C, 2361 / 2363 clean; MTR wrapper `cte_nodefail_coordinator_replay.test` (`ndb_cte`) - **done** |

PK-1..PK-7 live in `testCteProtocol`; PK-8 kills a node and lives in
`testNodeRestart` on the shared hold driver of section 4 (its query shape
is `OuterAggMain`, its hold event names no requester, its post-kill
marker is the guard's event). The NDB API queries use the
`CteQueryUtil.hpp` shapes (tables created through MySQL, since the test
runs beside live mysqlds) on a second thread and Ndb object, so the test
can poll DUMP 2365 and release the hold while the query is in flight.
Two shapes were added for the parking cases: `ScanAggMain` (a root scan
followed by an equality scan of the SQL-created PRIMARY index on the
same pk; the child is the aggregate leaf, its SCAN_FRAGREQ feeds) and
`OuterAggMain` (LEFT
JOIN readTuple on the nullable `nk` column, one NULL key in four, so
NULL_ROW_REQ feeds). A held SETUP also holds that state's feeds, so the
scan phase never completes while it is held: COMPLETE, REDISTRIBUTE and
FINAL_REP cannot park during an API query and are parked by hand in
PK-1 (a) instead. Its REDISTRIBUTE / FINAL replay portion requires at
least two data nodes: SETUP and COMPLETE run on every participant and
the scans must find the injected group exactly once across all owners.
A single-node run explicitly skips that portion; parked COMPLETE replay
still runs.

MTR deliverables of this phase, both in `ronsql_cte` - **done**:
`cte_park_basic.test` (`ALL ERROR 5127` before a probed CTE, an outer
join onto a CTE with NULL and missing keys, and a pass-through scanCte
root; every result equals the MySQL baseline whether the parked
consumers were flushed or swept and retried; leak dumps) and
`cte_park_sweeper.test` (`ALL ERROR 5139`; `ronsql_cli`'s first attempt
fails with 1251 (stderr) before any row was delivered, so RonSQL retries
and the retried result is diffed against the mysql client; leak dumps;
a clean comparison). Both need `--record` on first run and the debug
build.

Effort: 3.5 days including the two capacity inserts and the MTR files.

---

## 7. MTR cases (developed inside Phases 1-3)

Maintainer direction: MTR coverage is written phase by phase, next to the
NDBT or block-level case that establishes each window, not as a trailing
phase. Six files in `mysql-test/suite/ronsql_cte*/t`, each with
`--source include/have_ndb_error_insert.inc` and a recorded `.result`:

| File | Phase | What it does |
|---|---|---|
| `cte_nodefail_close.test` (suite `ndb_cte`) | 1 | wrapper: `testNodeRestart -n CteCloseOwedByFailedNode T1` (NF-1) - **done** |
| `cte_nodefail_peer_redist.test` (suite `ndb_cte`) | 1 | wrapper: `testNodeRestart -n CtePeerDiesDuringRedistribute T1` (NF-2) - **done** |
| `cte_nodefail_lookup_target.test` (suite `ndb_cte`) | 1 | wrapper: `testNodeRestart -n CteLookupTargetDies T1` (NF-3) - **done** |
| `cte_nodefail_scan_source.test` (suite `ndb_cte_ng2r2`, 4 data nodes) | 1 | wrapper: `testNodeRestart -n CteScanSourceDiesMidBatch T1` (NF-4) - **done** |
| `cte_nodefail_requester_paused.test` (suite `ndb_cte_ng2r2`, 4 data nodes) | 1 | wrapper: `testNodeRestart -n CteRequesterDiesPausedScan T1` (NF-5) - **done** |
| `cte_nodefail_requester_feed.test` (suite `ndb_cte_ng2r2`, 4 data nodes) | 1 | wrapper: `testNodeRestart -n CteRequesterDiesAggFeed T1` (NF-6) - **done** |
| `cte_nodefail_coordinator_feed.test` (suite `ndb_cte_ng2r2`, 4 data nodes) | 1 | wrapper: `testNodeRestart -n CteCoordinatorDiesAggFeed T1` (NF-7) - **done** |
| `cte_nodefail_coordinator_ready.test` (suite `ndb_cte`) | 1 | wrapper: `testNodeRestart -n CteCoordinatorDiesReady T1` (NF-8) - **done** |
| `cte_nodefail_coordinator_redist.test` (suite `ndb_cte_ng2r2`, 4 data nodes) | 1 | wrapper: `testNodeRestart -n CteCoordinatorDiesPausedRedist T1` (NF-9) - **done** |
| `cte_nodefail_coordinator_release.test` (suite `ndb_cte`) | 1 | wrapper: `testNodeRestart -n CoordinatorDiesReleaseInFlight T1` (NF-10) - **done** |
| `cte_nodefail_requester_parked.test` (suite `ndb_cte_ng2r2`, 4 data nodes) | 1 | wrapper: `testNodeRestart -n CteRequesterDiesParked T1` (NF-11) - **done** |
| `cte_nodefail_coordinator_parked.test` (suite `ndb_cte`) | 1 | wrapper: `testNodeRestart -n CteCoordinatorDiesParked T1` (NF-12: late SETUP rejection and parked-request cleanup) - **done** |
| `cte_nodefail_coordinator_replay.test` (suite `ndb_cte`) | 3 | wrapper: `testNodeRestart -n CteCoordinatorDiesParkedReplay T1` (PK-8: replay of consumers parked before a successful SETUP after the coordinator failed) - **done** |
| `cte_nodefail_peer.test` (suite `ronsql_cte_ng2r2`) | 1 | background CLI, 5140 hold, cookie-matched peer selection, abort restart, checked retry/error, recovery query and leak acknowledgements (shared driver `ronsql_cte/include/cte_nodefail.inc`) - **done** |
| `cte_nodefail_coordinator.test` (`ronsql_cte_ng2r2`) | 1 | chained CTE through background CLI, 5144 feed hold naming the actual coordinator (extra bit 29 clears the hold on NODE_FAILREP), abort restart, checked retry/error, recovery query and leak acknowledgements - **done** |
| `cte_redist_pages.test` (`ronsql_cte`) | 2 | `ALL ERROR 5134`, wide GROUP BY CTE redistributed across nodes, result equals baseline, leak dumps, clean re-run - **done** |
| `cte_scan_batches.test` (`ronsql_cte`) | 2 | CTE scan over several batches compared with MySQL, then `ALL ERROR 5129` with `ronsql_cli` exiting 1 on NDB error 1251, leak dumps, clean re-run - **done** |
| `cte_park_basic.test` (`ronsql_cte`) | 3 | `ALL ERROR 5127` before a probed CTE, an outer join onto a CTE with NULL and missing keys and a pass-through root; results identical to the MySQL baseline; `ALL ERROR 0`; leak dumps - **done** |
| `cte_park_sweeper.test` (`ronsql_cte`) | 3 | `ALL ERROR 5139`, the first `ronsql_cli` attempt fails with 1251 and the retry matches the mysql client; leak dumps; clean re-run - **done** |

MTR cannot read a DUMP's output, so the leak dumps rely on the crash they
raise; each test asserts the cluster is still up afterwards
(`$NDB_WAITER`).

---

## 8. Phase 5 - registration and soak

- Every new NDBT case is registered in `daily-basic--16-tests.txt` next
  to the existing `JoinAggNodeRestart` / `JoinAggErrorInsert` entries
  (format `cmd:` / `args: -n <Case> T1` / `max-time:`; 600 s for the
  2-node cases, 900 s for those that need 3 or 4 nodes and skip
  otherwise); the 3- and 4-node cases (NF-4, NF-5, NF-6, NF-7, NF-9,
  NF-11) are also in `16node-tests.txt` - **done**.
- Run each node-failure case 50 times in a loop on a 4-node dev cluster
  before enabling in autotest; timing windows that never trigger in 50
  runs get a wider insert (e.g. raise 5133's delay) rather than a sleep in
  the test - **open** (the user's soak run).
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

## 11. Findings ledger (bugs the new tests exposed)

| ID | Found by | Symptom | Cause | Status |
|---|---|---|---|---|
| F-1 | NF-1 post-recovery check (2026-09-11) | After a data node rejoined, a CTE lookup query returned 3108 of 4096 rows with no error | DBSPJ's ordered data-node list (`m_dataNodeList`) is rebuilt only at STTOR and NODE_FAILREP, never when a node reconnects or is included, while DBLQH builds each query's owner list from the connected nodes at SETUP; both map owner = hash % count, so a surviving SPJ with a one-entry list sent every probe to itself and the groups owned by the rejoined node missed silently | fixed: DBSPJ rebuilds the list on demand in `cte_scan_start`, `cte_scan_build`, `cte_lookup_build` and on INCL_NODEREQ; follow-up done 2026-09-11: the owner list is decided once per query by DBTC from its connected data nodes and carried to every DBLQH in `JoinAggSetupReq::setupNodes` and to every DBSPJ worker in the aggKeys section; DBSPJ's private list is gone (`cte_owner_list.md`) |
| F-2 | NF-1 first run | Node crashed in `checkInitGlobalVariables` (fragment lock held) | test hook 5135 returned from SCAN_NEXTREQ without `release_frag_access` | fixed in the hook |
| F-3 | NF-1 third run | Every iteration reported "window missed" with rc=0, whether or not the close had been held | two test defects: (a) the killer published the kill only after `waitNodesNoStart`, so a close completed by DBTC's node-failure handling was checked before the flag existed; (b) the swallow sat in DBLQH, where it depends on the victim's LQH scan being mid-batch when the close arrives, and it left no trace when it fired | fixed: the killer publishes `CteNfKillIssued` before issuing the kill and the close is timed (`Result::closeMillis`); the swallow moved to DBSPJ `execSCAN_NEXTREQ` (17533) where every worker on the victim holds DBTC's close regardless of LQH state; the main scan runs with a 64-row batch so no worker has finished at the first row; both hooks log when they fire |
| F-6 | SM-2 through RonSQL, `cte_scan_batches.test` first run (2026-09-15) | Under `ALL ERROR 5129` (once per node) `ronsql_cli` exited 0 and printed 2015 rows for a 1500-group CTE scan | RonSQL's pass-through drains stream rows to `out_stream` as they arrive, and both `execute_passthrough_drain` and the single-table drain (through the generic NDB-status classification) turned the mid-drain failure into `RonSQLRetryableError`; the retry succeeded once the insert had cleared, but neither `ronsql_cli` (stdout) nor RDRS (its per-request response buffer) can rewind the rows already written, so the response held the partial first attempt plus the complete second one | fixed in RonSQL: `m_output_started` is set when a pass-through header or row is written; after that a drain failure is `RonSQLPermanentError` in every classification path (direct, NDB temporary status, stale-schema reload). Aggregating queries print after the drain and keep their retry. `cte_scan_batches.test` csb-2 pins the exit code, the 1251 on stderr and the short output |
| F-5 | LK-2 control run, `testCteNdbApiOuterJoin` Test 7 (2026-09-15) | `SELECT COUNT(*), SUM(cte.total) FROM t LEFT JOIN cte ON cte.grp = t.nullable_col` returned COUNT=3 for 4 rows: the row whose join key is NULL was dropped from the aggregation (the miss row was counted) | `Dbspj::cte_lookup_send` skipped a NULL key outright ("no match possible"), which is right for pass-through queries (the API NULL-fills the CTE columns of the delivered parent row) but wrong for an aggregating outer join, where the readTuple (`lookup_send`) and scanFrag (`scanFrag_parent_row`) arms feed the NULL-extended row through JOIN_AGG_NULL_ROW_REQ | fixed: the CTE lookup arm injects the NULL row for an outer-join aggregate leaf (own columns marked NULL, the CTE_LOOKUP_REF miss form) and propagates it for an aggregate ancestor; pinned by Test 7 and `ronsql_cte/cte_null_key_outer.test` |
| F-4 | SM-5 design review (2026-09-15) | A CTE_SCAN_CONF lost without a node failure (5128) leaves the scan unrecoverable: DBTC's fragment timeout (`timeOutFoundFragLab`, LQH_ACTIVE) calls `scanError`, which sends SCAN_TABREF with closeNeeded and a close to DBSPJ, but DBSPJ keeps the slot's batch obligation until the reply arrives, so the close never completes; the fragment times out again every `TransactionDeadlockDetectionTimeout` and re-issues the close, and the ApiConnectRecord stays in CLOSING_SCAN with the DBSPJ request pending until the node fails | by design: a reply is lost only by node failure, which NODE_FAILREP handles (NF-4); DBTC's timeout is not a recovery path for a live DBSPJ worker | documented; SM-5 not run, no test may leave such a scan behind |
| F-8 | PK-8 first run, `ndb_cte.cte_nodefail_coordinator_replay` (2026-09-16) | With 5150 armed on the peer, the query failed with 1251 after 12 ms and the peer logged `JOIN_AGG_PARK_SWEPT ... count=1020`: the sweeper ran although every LDM instance held it | CMVMI addresses DBLQH error inserts to DblqhProxy, which forwards them to its LDM workers only; the query-thread LQH instances (DBQLQH, behind DbqlqhProxy) receive nothing but the clear. TRPMAN routes V_QUERY-addressed LQHKEYREQ, SCAN_FRAGREQ, CTE_LOOKUP_REQ and JOIN_AGG_NULL_ROW_REQ to either kind of instance, so a NULL_ROW parked on an unarmed query-thread instance, whose 10 ms sweeper then removed the shared placeholder and REFed all 1020 waiters. Every hook that acts on the instance running the request had the same exposure: the 5138 / 5145 / 5148 sweeper holds (PK-1b, PK-4, NF-11, NF-12), the 5141 lookup hold (NF-3, LK-1), 5131 / 5132 (NULL_ROW) and the CTE scan hooks, all of which held only when TRPMAN happened to pick an LDM | fixed: `DblqhProxy::execNDB_TAMPER` forwards codes from `ZFIRST_QUERY_THREAD_ERROR_INSERT` (5128) on to `DBQLQH_REF`; codes below stay LDM-only so legacy DBLQH inserts keep their behaviour |
| F-7 | Phase 3 parking run, `testCteProtocol` section 6 (2026-09-15) | After a JoinAgg close had been deferred (SETUP / COMPLETE replies still pending, the scan left RUNNING), a further worker failure sent a second SCAN_TABREF, which interrupted the API's wait for the close confirmation | `Dbtc::scanError` reported every failure to the API unless the API itself had failed; it did not remember that a REF had already been sent, nor that the API had ordered the close and was owed only its confirmation | fixed: `ScanRecord::m_scan_error_sent` (reset in `initScanrec`); `scanError` still advances cleanup through `close_scan_req` on every call but sends SCAN_TABREF only for the first failure and never after the API requested the close (the decision is taken before `close_scan_req`, which may release the records) |

## 12. Risks and open points

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
