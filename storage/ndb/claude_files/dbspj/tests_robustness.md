# DBSPJ robustness: error, abort, resource-exhaustion and node-failure paths

Tree: /Users/mikael/mysql_trees/rondb_2610_main (branch RONDB-1124-spj-fixes).
Line numbers are **HEAD** line numbers of `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`
unless stated otherwise. In the working tree they shift by up to about 100 lines because of the new hooks.
Nothing was built or run. Every finding below comes from reading the code, and every test is written so that it fails with a message naming the weakness.

## 1. Files created / modified

| File | Change |
|---|---|
| `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp` | Seven new error-insert hooks, 17535-17541. Each does nothing unless its insert is set, and each clears itself when it fires (section 3). |
| `storage/ndb/src/kernel/blocks/ERROR_codes.txt` | 17535-17541 registered. "Next DBSPJ" is now 17542 (17530-17541 RonDB). |
| `storage/ndb/test/ndbapi/testSpj.cpp` | `SpjLeakCheck` (ndbinfo through `NdbInfo`), plus `spjDumpLeakCheck` (DUMP 2650/2560 and a node-liveness check). New TESTCASEs `SpjFaultLeakCheck`, `SpjNewFaultLeakCheck` and `NF_JoinLeakCheck`. FI_END is now 17541. |
| `storage/ndb/test/run-test/daily-basic--14-tests.txt` | Not changed: `SpjFaultLeakCheck` and `SpjNewFaultLeakCheck` are expected to fail while R-2..R-6/S-2/P-19 are open, so they are added to daily-basic--14 only once those are fixed. |
| `storage/ndb/test/run-test/daily-basic--16-tests.txt` | Adds `NF_JoinLeakCheck T6`. |
| `mysql-test/suite/ndb_spj/t/ndb_spj_robust_dih_scan_leak.test` | R-2 / S-3 (17012) |
| `mysql-test/suite/ndb_spj/t/ndb_spj_robust_getnodes_errcode.test` | R-6 (17538) |
| `mysql-test/suite/ndb_spj/t/ndb_spj_robust_getnodes_continueb.test` | R-3 (17539; 17131 as control) |
| `mysql-test/suite/ndb_spj/t/ndb_spj_robust_first_batch_abort.test` | R-4 / R-5 / S-19, large fragments (17535) |
| `mysql-test/suite/ndb_spj/t/ndb_spj_robust_first_batch_abort_small.test` | R-4b / S-19, small fragments, node stop (17535) |
| `mysql-test/suite/ndb_spj/t/ndb_spj_robust_joinagg_match_oom.test` | R-1 (17536) |
| `mysql-test/suite/ndb_spj/t/ndb_spj_robust_joinagg_null_row.test` | R-1b (17537; the lookup-leaf case is the control) |
| `mysql-test/suite/ndb_spj/t/ndb_spj_robust_dih_scan_ref.test` | S-2 (17541) |
| `mysql-test/suite/ndb_spj/t/ndb_spj_robust_exec_plan_oom.test` | P-19 (17540; scan request and lookup request) |
| `mysql-test/suite/ndb_spj/t/ndb_spj_robust_nodefail.test` | A data node is killed during a pushed scan join (DBLQH 5112 + DBTC 8317), then leak checks |
| `mysql-test/suite/ndb_spj/t/ndb_spj_robust_fault_matrix.test` | 51 existing 17xxx inserts against 4 query shapes. Expected-error checks, result checks and leak checks. |

No `.result` files exist yet; the user records them (section 6).

### How the MTR tests check themselves (shared pattern)

Each MTR test does the following:

- **Pushdown is verified first.** Each test checks that its query really is pushed, using the
  `Ndb_pushed_queries_executed` delta or, for aggregation, `EXPLAIN ... Using pushed join aggregation` and `type = ref`.
  If it is not pushed, the test dies with "Test setup: ...".
- **The faulted query.**
  - The insert is armed with `ALL ERROR n` and cleared with `ALL ERROR 0`.
  - The query runs with `--disable_abort_on_error`/`--disable_result_log`.
  - The NDB error code is parsed out of `SHOW WARNINGS`.
  - Queries that may hang are run with `send` on `con1`, while the default connection polls
    `performance_schema.threads`. A hang becomes `--die` after 30 s.
- **Leak checks.** Each one polls with a deadline:
  - DIH scan registrations (`ndbinfo.ndb$table_distribution_status_all.current_scan_count + scan_count_wait`, restricted to test tables and their indexes through `dict_obj_info`).
  - Open DBLQH fragment scans (`ndbinfo.operations_per_fragment.conc_frag_scans`).
  - `suite/ronsql_large/include/mem_leak_dumps.inc`: DUMP 2650 DBSPJ requests and DUMP 2560 DBTC; on debug builds also 2361-2364 for DBLQH join-agg. Each dump aborts a node that finds a leak, and the include dies if any acknowledgement is missing.
- **Recovery.** The query is run again and must give the reference result (string compare, or `--diff_files` for the aggregation queries).
- **Output.** It is deterministic when the test passes. When a check fails, the diagnostic rows are printed before `--die`.

## 2. Error-insert coverage matrix

Legend for the coverage columns:

- **LJE / SJE** = testSpj `LookupJoinError` / `ScanJoinError`. These arm the insert in one random node, accept any outcome (`return NDBT_FAILED` is commented out) and check nothing afterwards.
- **MAT** = new `ndb_spj_robust_fault_matrix.test`. Shapes: 0 = lookup→lookup, 1 = scan→lookup, 2 = scan→scan(ref), 3 = scan→scan→lookup. It checks the expected error, the result and leaks.
- **SFL** = new testSpj `SpjFaultLeakCheck` (Hugo scan and lookup joins, with the same checks through `NdbInfo`).
- "–" = none.

| Insert | Where (HEAD) | Simulates | Shapes that can hit it | Existing coverage | New coverage |
|---|---|---|---|---|---|
| 17001 | execLQHKEYREQ 1253 | Request seize OOM (20008) | lookup root | LJE | MAT, SFL |
| 17002 | execSCAN_FRAGREQ 1622 | Request seize OOM (20008) | scan root | SJE | MAT, SFL |
| 17003 | allocPage 5882 | Row page OOM (20015) | buffered rows, only if the free page list is empty | LJE, SJE | MAT, SFL |
| 17004 | scanFrag_build 11806.. | Root ScanFragHandle seize (20008) | scan root | SJE | MAT, SFL |
| 17005 | createNode 3525 | OutOfOperations 20000 | all | LJE, SJE | MAT, SFL |
| 17006 | build 2188 | UnknowQueryOperation 20003 | all | LJE, SJE | MAT, SFL |
| 17008 | appendToPattern 15386 | OOM 20008 | linked keys / attrs | LJE, SJE | MAT, SFL |
| 17009 / 17010 | appendParam(Head)ToPattern 15409/15447 | OOM 20008 | parameters | **none** (in neither list) | MAT, SFL |
| 17012 | execDIH_SCAN_TAB_CONF 12380 | First child ScanFragHandle seize (20008) | child scans | LJE, SJE (no check) | MAT, SFL, **dedicated R-2 test** |
| 17013 | LQHKEYREQ/SCAN_FRAGREQ 1331/1934 | InvalidRequest 20002 | all | LJE, SJE | MAT, SFL |
| 17014 | 1225/1589/9631/13870 | ndbrequire: the request must be local (read-local assertion) | – | MTR ndb_read_local.inc | – (not an error path) |
| 17020-17022 | lookup_send 9655 | NodeFailure 20016 | lookup children | LJE, SJE | MAT, SFL |
| 17030-17032 | lookup_send 9608 | LQHKEYREF 1227 (schema version) | lookups | LJE, SJE | MAT, SFL |
| 17040-17043 | lookup_parent_row 10038 | OOM 20008 | lookup children | LJE, SJE | MAT, SFL |
| 17050-17053 | parseDA 16099 | OutOfSectionMemory 20006 | all | LJE, SJE | MAT, SFL |
| 17060-17063 | scanFrag_parent_row 12771 | OOSM 20006 | child scans | LJE, SJE | MAT, SFL |
| 17070-17072 | lookup_send 9565 | dupSection OOSM 20006 | lookups | LJE, SJE | MAT, SFL |
| 17080-17082 | lookup_parent_row 10200 | OOSM 20006 | lookup children | LJE, SJE | MAT, SFL |
| 17090-17093 | scanFrag_send 13700 | OOSM 20006. Only checked in the `releaseAtSend \|\| T_ATTRINFO_CONSTRUCTED` branch, so for a non-pruned child only on the last fragment | scans | SJE | MAT, SFL |
| 17110-17112 | scanFrag_send 13880 | SCAN_FRAGREF 1227 from DBLQH | scans | SJE | MAT, SFL |
| 17120-17122 | execTRANSID_AI 5506 | OutOfRowMemory 20015 | buffered rows | LJE, SJE | MAT, SFL |
| 17123 | execSCAN_FRAGCONF 5071 | Delays SCAN_FRAGCONF by 0.5 s for ever | scans | MTR ndb_scan_protocol_timeout | – (never clears, R-10) |
| 17130 | scanFrag_sendDihGetNodesReq 12543 | OOSM 20006 when m_outstanding==1. The insert is **consumed even when it does not fire** (R-9). | child scans | LJE, SJE | MAT, SFL |
| 17131 | scanFrag_sendDihGetNodesReq 12517 | One DIGETNODESREQ per round (CONTINUEB) | child scans | MixedJoin17131 | MAT (must succeed), getnodes_continueb control |
| 17510 | appendToSection 15426 | Every 13th section append fails | all | LJE, SJE | MAT, SFL |
| 17520 / 17521 | checkTableError 5968 | NoSuchTable 20019 | all | LJE, SJE | MAT, SFL |
| 17530 | lookup_send 9640 | OOSM when the API is not connected | NF restart | bug#23049170 | – |
| 17531 | sendConf 4401 | Drops the next long signal (SIGNAL_DROPPED_REP) | all | bug#23048816 (**disabled in ATR: "never stable"**; see R-7) | – |
| 17532 | cte_scan_sendReq 8251 | Crash | CTE | **none** | – (CTE, not in scope) |
| 17533 | execSCAN_NEXTREQ 5195 | Swallows one close | CTE NF | cte_nodefail_close, testNodeRestart | – |
| 17534 | alloc_request_node_arrays 1377 | OOM 20008 | all | LJE, SJE (autotest only) | MAT, SFL |
| 17100 | – | listed in SJE, **does not exist** in DBSPJ (dead entry) | – | – | – |
| **17535** (new) | scanFrag_send loop | OOSM before the 2nd SCAN_FRAGREQ of a child's first batch (node still TN_INACTIVE) | child scans | – | first_batch_abort(_small), SpjNewFaultLeakCheck |
| **17536** (new) | mergeAggMatchBitmask | OOM merging the matched bitmask of the last outstanding fragment | outer-join agg scan leaf | – | joinagg_match_oom |
| **17537** (new) | sendJoinAggNullRow | OOSM before JOIN_AGG_NULL_ROW_REQ | outer-join agg (lookup and scan leaf) | – | joinagg_null_row |
| **17538** (new) | scanFrag_sendDihGetNodesReq | DIGETNODESREQ of the first child fragment fails DIH-style (zero=1, 311) | child scans | – | getnodes_errcode, SpjNewFaultLeakCheck |
| **17539** (new) | scanFrag_sendDihGetNodesReq | As 17131, and the first DIGETNODESREQ of a CONTINUEB round fails (311) | child scans | – | getnodes_continueb, SpjNewFaultLeakCheck |
| **17540** (new) | planParallelExec / appendTreeNode | Appending to the exec plan fails (OOM) | all | – | exec_plan_oom, SpjNewFaultLeakCheck |
| **17541** (new) | scanFrag_prepare | DIH_SCAN_TAB_REQ refused (ErroneousTableState), nothing registered | child scans | – | dih_scan_ref, SpjNewFaultLeakCheck |

**Gaps before this work.**

- No test checked any outcome or leak after an SPJ error insert.
- No SPJ error insert was ever used with join aggregation.
- MTR coverage was limited to 17014/17123.
- 17009/17010 were never exercised, 17100 is dead, and 17532 is unused.
- Node-failure tests (NF_Join, bug#23049170) only check that queries keep working, not that the survivors are leak-free.

## 3. New hooks (minimal, inert unless set)

Every hook is `if (ERROR_INSERTED(n) ...) { CLEAR_ERROR_INSERT_VALUE; g_eventLogger->info(...); <fail as the real failure would>; }`:

- **17535**: `scanFrag_send` (inner), right after the `SFH_NOT_STARTED` skip. It applies when `requestsSent > 0 && m_parentPtrI != RNIL && m_state == TN_INACTIVE`, and sets `err = OutOfSectionMemory; break;`.
- **17536**: `mergeAggMatchBitmask`, before the allocation. It applies when `data.m_frags_outstanding == 0` and returns `OutOfQueryMemory`.
- **17537**: `sendJoinAggNullRow`, before `expand()`. It returns `OutOfSectionMemory`.
- **17538 / 17539**: `scanFrag_sendDihGetNodesReq`, after `EXECUTE_DIRECT(DIGETNODESREQ)`. It rewrites `theData[0]=1, theData[1]=311` exactly as DBDIH reports a failure.
  - 17539 also chunks like 17131. It uses a local `fiContinuedRound` flag: a fragment resolved by an earlier round has m_ref != 0.
- **17540**: `appendTreeNode` (planSequentialExec) and `planParallelExec`, before `execList.append`. It returns `OutOfQueryMemory`.
- **17541**: `scanFrag_prepare`. It replaces `EXECUTE_DIRECT(DIH_SCAN_TAB_REQ)` with a DIH-style REF (senderData=1, ErroneousTableState), so nothing is registered in DBDIH.

## 4. Weaknesses

Confidence is about the code reading; "reach" is how likely the path is without an error insert.

### R-1: join-agg matched-bitmask OOM makes the request hang for ever. (high)
- **Where**:
  - `scanFrag_execSCAN_FRAGCONF` 14438-14445: `mergeAggMatchBitmask` fails, then `abort(); return;`.
  - `mergeAggMatchBitmask` 10492.
- **Description**:
  - `data.m_frags_outstanding` was already decremented, but the per-tree-node `Request::m_outstanding--` (inside `if (data.m_frags_outstanding == 0)`, 14467-14630) is skipped.
  - If this was the last outstanding fragment, `m_outstanding` sticks at ≥1. Nothing completes the request, and a DBTC close is ignored because the request is RS_ABORTING and checkBatchComplete sees outstanding > 0.
  - The DBSPJ request, the DBTC scan and the join-agg states (no RELEASE) leak.
  - In debug builds `scanFrag_abort` can additionally hit `ndbassert(m_frags_not_started > 0)` when all leaf fragments were in flight.
- **Trigger**:
  - 17536 with a pushed LEFT JOIN aggregation whose leaf is a ref scan.
  - The real trigger is a QUERY_MEMORY allocation failure. In the first leaf batch of an aggregation query, `m_rootFragCnt`=1, so parallelism is 1.
- **Test**: `ndb_spj_robust_joinagg_match_oom`.
- **Expected vs suspected**:
  - Expected: NDB 20008 promptly, no leak.
  - Suspected: the query hangs (die after 30 s), or fails with DBTC time-out 274 after about 3 s; DUMP 2650/2560 then abort a node.

### R-1b: join-agg null-row injection failure on a scan leaf hangs the request. (high)
- **Where**: `scanFrag_execSCAN_FRAGCONF` 14598-14621. A failure of `handleAggLeafScanComplete` or `handleScanAggAncestorComplete` leads to `abort(); return;` inside the `m_frags_outstanding == 0` block, before `m_outstanding--`.
- **Description**: the same leak as R-1, reached through `sendJoinAggNullRow` (OOSM in `expand`/`appendToSection`).
- **Trigger**: 17537 on a LEFT JOIN aggregation with a scan leaf and unmatched parents. Control: 17537 on a lookup leaf with a NULL join key goes through `lookup_parent_row`, then abort, then the caller's checkBatchComplete. It must pass.
- **Test**: `ndb_spj_robust_joinagg_null_row` (control first).
- **Expected vs suspected**: expected 20006; suspected a hang or 274.

### R-2 (= S-3): DIH scan count leaked when the child scan fails before any ScanFragHandle exists. (high)
- **Where**:
  - `execDIH_SCAN_TAB_CONF` 12316: `checkTableError` at 12371, or ScanFragHandle seize at 12380, fails before `m_fragments` gets an entry.
  - `scanFrag_complete` 14839 only sends `DIH_SCAN_TAB_COMPLETE_REP` `if (!data.m_fragments.isEmpty())`.
- **Description**:
  - `scanFrag_prepare` already registered the scan in DBDIH (`start_scan_on_table`: `m_scan_count[0]++`). It is never completed.
  - `ndb$table_distribution_status_all.current_scan_count` stays > 0.
  - A later online reorganisation (`wait_old_scan`) waits for ever, logging "Waiting %us for %u scans to complete".
- **Trigger**: 17012 (deterministic: it fails the first fragment); 17520/17521 when they hit that `checkTableError`.
- **Test**: `ndb_spj_robust_dih_scan_leak`; also MAT (17012, shapes 2/3) and SpjFaultLeakCheck.
- **Expected vs suspected**: expected 20008 and current_scan_count back to 0; suspected that it stays ≥1 for ever.

### R-3: a DIGETNODESREQ error in a CONTINUEB round is ignored, which can stop the data node. (high; reach low to medium)
- **Where**:
  - `execCONTINUEB` case 3, 1081: the return value of `scanFrag_sendDihGetNodesReq` is dropped.
  - `scanFrag_sendDihGetNodesReq` 12541-12553: on an error it sets `m_frags_outstanding=0`, marks the node TN_INACTIVE and decrements `m_outstanding`.
- **Description**:
  - The query starts with child fragments whose `m_ref == 0`.
  - The child's first batch then reaches `scanFrag_send` with `nodeId = refToNode(0) = 0` and calls `getNodeInfo(0)`. That fails the `ndbrequire(nodeId > 0)`, which stops the node.
  - The alternative outcome is that fragments are silently skipped, giving a wrong result.
- **Reach**: tables with more than MAX_DIGETNODESREQS fragments per child scan, plus a DIH error during resolution.
- **Trigger**: 17539. Control: 17131 must succeed.
- **Test**: `ndb_spj_robust_getnodes_continueb`; also SpjNewFaultLeakCheck.
- **Expected vs suspected**: expected NDB 311 and all nodes still started; suspected a data node stops.

### R-4 (= S-19): abort in the first child batch leaves started DBLQH fragment scans open for ever. (high)
- **Where**:
  - `scanFrag_send` (inner) 14075-14078 calls `abort()` after the loop sent ≥1 SCAN_FRAGREQ.
  - The outer `scanFrag_send` (13331) only sets TN_ACTIVE after the inner call returns.
  - `scanFrag_abort` 14853-14870 returns at once for TN_INACTIVE, so the started fragments are never closed.
- **Description**:
  - A started fragment that answers `done=0` stays SFH_WAIT_NEXTREQ.
  - The request is then completed through the batchComplete fix-up (R-5) and released.
  - The DBLQH scan record waits for a SCAN_NEXTREQ that never comes. DBLQH has no idle-scan time-out.
  - The visible effects:
    - `conc_frag_scans` stays ≥ 1.
    - DROP TABLE waits.
    - Repeating it exhausts scan records (488/489).
- **Reach**: OutOfSectionMemory in `dupSection`/`appendReaderToSection` part-way through the first child batch. With `T_ATTRINFO_CONSTRUCTED`, existing 17091/17093 can also trigger it.
- **Trigger**: 17535. The test uses a child with 1000 matching rows per fragment (8 fragments) and a MultiFrag root with first-batch parallelism 4.
- **Test**: `ndb_spj_robust_first_batch_abort`; also SpjNewFaultLeakCheck.
- **Expected vs suspected**: expected 20006 and conc_frag_scans back to 0; suspected that it stays ≥1.

### R-4b (= S-19, second half): more fragments started for an aborted request, node stop. (high)
- **Where**:
  - `scanFrag_execSCAN_FRAGCONF` 14547-14585: "batch not full, start more fragments". It checks neither RS_ABORTING nor the node state.
  - Completion tests at 14450-14455: `m_frags_complete == m_fragCount || (RS_ABORTING && fragCount == complete + not_started)`.
- **Description**:
  - After R-4 the started fragment completes (`done=1`, small fragments). The abort rule then decrements `m_cnt_active` and sets TN_INACTIVE.
  - Next, `all_started_completed && not_started > 0` with `state == SFH_SCANNING` (never closed) leads to a `scanFrag_send` of the remaining fragments for the aborted request.
  - When the last of them completes, `m_frags_complete == m_fragCount`, and the node is "completed" a second time. `ndbrequire(m_cnt_active)` then stops the node; if another node is still active, the count is corrupted instead.
  - If fewer fragments were restarted, they may stay open as in R-4.
- **Trigger**: 17535 with 2 matching rows per fragment.
- **Test**: `ndb_spj_robust_first_batch_abort_small`.
- **Expected vs suspected**: expected 20006 with no node stop; suspected a data node stops.

### R-5: the abort fix-up in batchComplete skips the complete() phase, so DIH scans leak. (high on code; it shows up together with R-4)
- **Where**: `batchComplete` 3858-3890.
- **Description**:
  - With RS_ABORTING and `!is_complete`, batchComplete marks the remaining TN_ACTIVE nodes inactive, sets `is_complete = true`, then calls `sendConf` and `cleanup`.
  - It never runs `complete()`, even though `RT_NEED_COMPLETE` is set, so `scanFrag_complete` never sends `DIH_SCAN_TAB_COMPLETE_REP` for any child scan of the request.
  - The fix-up exists exactly for "nodes left TN_ACTIVE after abort", which is the R-4 situation.
- **Trigger**: 17535, large-fragment variant.
- **Test**: `ndb_spj_robust_first_batch_abort` (DIH check after the R-4 check), and the DIH check in SpjNewFaultLeakCheck.
- **Expected vs suspected**: expected current_scan_count back to 0; suspected that it stays ≥1.

### R-6: the error code of a failed DIGETNODESREQ is wrong. (high; minor impact)
- **Where**: `scanFrag_sendDihGetNodesReq` 12541 uses `errCode = signal->theData[0]`.
- **Description**:
  - On error DBDIH sets `DiGetNodesConf::zero = 1` and puts the code in `theData[1]`. DBTC (18764) and `Dbspj::getNodes` (11538) read `theData[1]`.
  - DBSPJ instead aborts the query with NDB error **1** ("Unknown error code") instead of e.g. 311 or 1224. The temporary/permanent classification is lost, so clients cannot retry correctly.
- **Trigger**: 17538.
- **Test**: `ndb_spj_robust_getnodes_errcode`; also SpjNewFaultLeakCheck.
- **Expected vs suspected**: expected 311; suspected 1.

### S-2: DIH_SCAN_TAB_REF stalls the request until DBTC's scan time-out. (high; reach low)
- **Where**: `execDIH_SCAN_TAB_REF` 12300-12313 decrements `m_outstanding` and calls `abort()`, but not checkPrepareComplete/checkBatchComplete.
- **Description**:
  - When this was the last prepare, nothing completes the request. DBTC's fragment time-out then closes it, and the API gets 274.
  - Unlike R-1 it is not permanent: `execSCAN_NEXTREQ`(close) → checkBatchComplete → `m_outstanding==0` → completes.
  - It also passes DIH's `ErroneousTableState` (1) on as the NDB error code. DBTC's own `execDIH_SCAN_TAB_REF` (18527) does the same, so this is a shared quirk.
- **Reach**: DBDIH refuses only when the table is not TS_ACTIVE, and DBSPJ's own `checkTableError` usually catches that first.
- **Trigger**: 17541.
- **Test**: `ndb_spj_robust_dih_scan_ref`. It fails if the error is 274/296/297/4008/4012 or if the query took ≥ 2.5 s.

### P-19: errors from buildExecPlan() are ignored. (high on code; reach low)
- **Where**: `build()` 2520 calls `buildExecPlan(requestPtr);` without checking the result.
- **Description**:
  - An OutOfQueryMemory in `planParallelExec` (lookup requests, multi-leaf aggregation) or `appendTreeNode`/`planSequentialExec` (scan requests) leaves a tree node out of every `m_next_nodes`. It never runs.
  - The likely result is a silently wrong (inner join) result, or an API waiting for leaf results.
- **Trigger**: 17540 (both functions).
- **Test**: `ndb_spj_robust_exec_plan_oom` (scan request and lookup request); also SpjNewFaultLeakCheck.
- **Expected vs suspected**: expected 20008 promptly; suspected success with a wrong result, or a hang.

### R-7: SIGNAL_DROPPED_REP(TRANSID_AI) has no checkBatchComplete and miscounts. (medium; reach low)
- **Where**: `execSIGNAL_DROPPED_REP` 432-450 calls `m_countSignal` and then `abort()`, but not `checkBatchComplete`.
- **Description**:
  - If the dropped TRANSID_AI was the last outstanding signal, the request stalls. Scans recover by DBTC time-out; lookup requests leak until DUMP 2650.
  - It also counts the signal even for tree nodes without `T_EXPECT_TRANSID_AI`. `execTRANSID_AI` (5312) deliberately ignores those, because they are not in `m_outstanding`, so here `m_outstanding` can underflow.
  - This may explain why `bug#23048816` (17531) "has never been stable" and is disabled in daily-basic--14.
- **Test**: none. It cannot be made deterministic.

### R-8: execTRANSID_AI returns without checkBatchComplete after an aggregation-ancestor error. (medium; reach low)
- **Where**: `execTRANSID_AI` 5566-5571: `handleAggAncestorComplete` fails, then `abort(); return;`.
- **Description**: the request stalls if this TRANSID_AI was the last outstanding signal. It rarely is, because DBLQH sends TRANSID_AI before LQHKEYCONF from the same thread. The LQHKEYCONF/REF paths are fine because the outer exec calls checkBatchComplete.
- **Test**: none. It is not deterministic.

### S-4: handleJoinAggNextBatch has no completion re-check. (medium; reach low)
- **Where**: 3954-4054. If no tree node sends a NEXTREQ and nothing is aborted, `m_outstanding` stays 0, `ndbassert(m_outstanding > 0)` (4053) fires in debug, and the request stalls until 274 in release.
- **Why 17521 does not trigger it**: when `checkTableError` fails in `scanFrag_execSCAN_NEXTREQ`, `abort()` → `scanFrag_abort` closes the cursor node's SFH_WAIT_NEXTREQ fragments and does `m_outstanding++`, so the close CONFs complete the request.
- **What does reach it**: only a `scanFrag_execSCAN_NEXTREQ` that returns early without aborting, i.e. `batchRows == 0` when the estimated record size is larger than the batch byte size.
- **Test**: none. It is not deterministic.

### P-12: overwrite of a root key section. (medium on code; not reachable from the NDB API)
- **Where**: `execLQHKEYREQ` 1316 and `execSCAN_FRAGREQ` 1920 overwrite `m_send.m_keyInfoPtrI` that `parseDA` (16310) may have set for NI_KEY_PARAMS/CONSTS. That leaks one section.
- **Why the API cannot reach it**: `NdbQueryLookupOperationDefImpl::appendKeyPattern` and `appendBoundPattern` return 0 for opNo 0 (NdbQueryBuilder.cpp 2905, 3282), so the API never sends a key pattern for the root. Only crafted signals reach it, which is the testSpjMalformed area.
- **Test**: none.

### R-9 (test infrastructure): the 17130 hook consumes itself.
- **Where**: 12543. `ERROR_INSERTED_CLEAR(17130) && m_outstanding == 1` clears the insert even when `m_outstanding != 1`, so it usually never fires.

### R-10 (test infrastructure): 17123 drops sections and makes the delayed signal look local.
- **Where**: 5071-5077.
- **Description**: 17123 re-sends SCAN_FRAGCONF with `sendSignalWithDelay(reference())`. That drops any section (the join-agg matched bitmask), and the delayed signal then appears to come from the local node, so NODE_FAILREP cannot filter it.

### R-11 (test gap): what LookupJoinError/ScanJoinError miss.
- They never check that the query fails (`return NDBT_FAILED` is commented out), nor the error code or any leak.
- They arm the insert in a single random node.
- 17009/17010 are missing, and 17100 does not exist.
- These gaps are covered by the new `SpjFaultLeakCheck` and `ndb_spj_robust_fault_matrix`.

### Node failure
`ndb_spj_robust_nodefail` and testSpj `NF_JoinLeakCheck` add leak checks after a node failure:
- `nodeFail` (4608), with `RT_REQUESTER_FAILED` and RS_ABORTED tombstones.
- `scanFrag_execNODE_FAILREP` (14954).

No new NF bug was confirmed by reading.

Note: `scanFrag_execNODE_FAILREP` returns 1 for TN_PREPARING/TN_INACTIVE nodes, so any data-node failure aborts every scan request that has an inactive child scan, even when it does not involve the failed node. That is conservative by design.

## 5. Weaknesses → tests (quick map)

| ID | Test(s) | Expected failure message if the bug is present |
|---|---|---|
| R-1 | ndb_spj_robust_joinagg_match_oom | "R-1: ... hung" or "R-1: ... did not fail with NDB error 20008" |
| R-1b | ndb_spj_robust_joinagg_null_row | "R-1b: ..." |
| R-2/S-3 | ndb_spj_robust_dih_scan_leak, fault_matrix (17012), SpjFaultLeakCheck | "R-2: DBSPJ leaked a DIH scan ..." |
| R-3 | ndb_spj_robust_getnodes_continueb, SpjNewFaultLeakCheck | "R-3: a data node stopped ..." |
| R-4/S-19 | ndb_spj_robust_first_batch_abort, SpjNewFaultLeakCheck | "R-4: a child fragment scan ... was never closed" |
| R-4b/S-19 | ndb_spj_robust_first_batch_abort_small | "R-4b: a data node stopped ..." |
| R-5 | ndb_spj_robust_first_batch_abort (after R-4 is fixed) | "R-5: a DIH scan leaked ..." |
| R-6 | ndb_spj_robust_getnodes_errcode, SpjNewFaultLeakCheck | "R-6: ... did not reach the API as NDB error 311" |
| S-2 | ndb_spj_robust_dih_scan_ref, SpjNewFaultLeakCheck | "S-2: ... time-out error" or "took more than 2.5 s" |
| P-19 | ndb_spj_robust_exec_plan_oom, SpjNewFaultLeakCheck | "P-19: ..." |
| NF | ndb_spj_robust_nodefail, NF_JoinLeakCheck | leak or "did not terminate" messages |
| all existing inserts | ndb_spj_robust_fault_matrix, SpjFaultLeakCheck | a list of (insert, shape, what) |

## 6. Commands (the user runs them)

**Build.** Use a data node build with error inserts (debug, or ERROR_INSERT defined). From the build directory, either do a full build or build at least `ndbmtd`, `ndbd` and `testSpj`:

```
make -j$(nproc)                      # or: make -j$(nproc) ndbmtd ndbd testSpj
```

**MTR.** These are new tests and have no `.result` yet. Run each with `--record`. A test that dies does not record, and its die message names the weakness.

```
cd mysql-test
./mtr --suite=ndb_spj --record ndb_spj_robust_fault_matrix
./mtr --suite=ndb_spj --record ndb_spj_robust_dih_scan_leak
./mtr --suite=ndb_spj --record ndb_spj_robust_getnodes_errcode
./mtr --suite=ndb_spj --record ndb_spj_robust_getnodes_continueb
./mtr --suite=ndb_spj --record ndb_spj_robust_first_batch_abort
./mtr --suite=ndb_spj --record ndb_spj_robust_first_batch_abort_small
./mtr --suite=ndb_spj --record ndb_spj_robust_joinagg_match_oom
./mtr --suite=ndb_spj --record ndb_spj_robust_joinagg_null_row
./mtr --suite=ndb_spj --record ndb_spj_robust_dih_scan_ref
./mtr --suite=ndb_spj --record ndb_spj_robust_exec_plan_oom
./mtr --suite=ndb_spj --record ndb_spj_robust_nodefail
# all of them, continuing after a failure:
./mtr --suite=ndb_spj --force --record --do-test=ndb_spj_robust_
```

- The fault matrix runs 204 queries plus polls, and should take a few minutes. The node-failure test restarts data node 2.
- If a test fails on "Test setup: ..." (e.g. the plan is not pushed, or an insert did not fire), the test needs adjusting, not DBSPJ.

**ATR / NDB API.** Only `NF_JoinLeakCheck` is in an ATR list (daily-basic--16). Add `SpjFaultLeakCheck T1 I3` and `SpjNewFaultLeakCheck T1 I3` to daily-basic--14 once the findings they target are fixed. To run by hand against a running cluster with ≥ 2 data nodes and `NDB_CONNECTSTRING` set:

```
testSpj -n SpjFaultLeakCheck T1 I3
testSpj -n SpjNewFaultLeakCheck T1 I3
testSpj -n NF_JoinLeakCheck T6
```

`SpjNewFaultLeakCheck` is expected to fail as long as R-3/R-4/R-5/R-6/S-2/P-19 are open. With R-3 present the 17539 run stops a data node.
