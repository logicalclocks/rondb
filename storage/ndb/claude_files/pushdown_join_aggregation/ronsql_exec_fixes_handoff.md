# RONDB-1124 exec fixes: status and outstanding work

Date: 2026-11-27. Branch `RONDB-1124-exec-fixes` on `origin`
(https://github.com/mronstro/rondb), pushed at `2ef4f179545`. PR #1231 was
merged into 26.10-main on 2026-11-18; PR #1241 (logicalclocks/rondb, base
26.10-main, open) holds the work after it. The open analysis bugs 8, 9, 11
and 12 and three gaps of §4.2 were fixed on 2026-11-24, one commit each
(§2, §3). On 2026-11-25 most of what those fixes left open was fixed too
(§3, §4), the JOIN_AGG and CTE signals got signal scopes (§3.5), and a
collision of error insert 5160 was resolved (§3.6). One candidate cause of
the `ronsql_join` Test 3 abort in the NDB API is fixed (§3.7). On
2026-11-26 and 2026-11-27 a review of the executor closed most of the
remaining raw-API gaps of §3.4, §4.1, §4.2 and §4.3, the other two
candidates of §3.7, a race in the eviction of a join aggregation group, and
four test bugs (§3.8); §4.4 lists what is still open.

This file is the starting point for whoever continues the work. It records:
- what the analysis in `dblqh_ronsql_analysis.md` found;
- what has been fixed, and in which commits;
- what is still open, with current code locations.

That report's line numbers are from `50eb491eb3f` and have moved; use the
locations given here. The status column of its table is not updated; the
table in §2 below is the current status.

Since then the RonSQL executor review fixes (F-1, C-2, D-1 and O-1, branch
`RONDB-1124-lqh-fix2`) have been rebased onto this branch, and one session
continues both lists. Their status is in `../ronsql_executor/README.md`.

## 1. How to work on this

- **Builds and tests.** The maintainer runs every build and test. Do not run
  `cmake --build`, `make`, `mtr`, `ctest` or test binaries yourself. Instead,
  give the exact commands and say what to look for in the results.
- **One change at a time.** New features and new tests: build each in a
  worktree under `rondb_2610_main/.claude/worktrees`, show it with its diff
  and an explanation, and commit it to the branch only after the maintainer
  approves it ("approve, but change X" means make X and commit without a
  second review). A fix for a failure the maintainer has hit (a pasted test
  failure or crash) goes straight into the tree as a commit so that it can
  be rebuilt and tested, and its diff goes in the reply. Every change is
  shown. Push only when the maintainer says so.
- **Commits.** Subjects use the `BLOCK: description` style (e.g.
  `DBLQH: ...`, `DBTUP/NDBAPI: ...`, `RonSQL: ...`). The body explains the bug,
  the fix and the new test, and ends with the `Co-Authored-By:` line from the
  session's instructions. Make one commit per bug, with its tests.
- **Error inserts.** These numbers are free next, as recorded on the "Next"
  lines in `storage/ndb/src/kernel/blocks/ERROR_codes.txt`:
  - DBTUP 4048;
  - DBLQH 5165. A DBLQH number can also be taken by an entry outside
    the RONDB-1124 block of `ERROR_codes.txt`: check with
    `git grep -w <n> storage/ndb/src/kernel` before taking one;
  - DBTC 8135 / 8321 (RonDB);
  - DBSPJ 17542.

  Update the "Next" line in the same commit as the new insert.
- **Never commit** `debug_build/`.
- **MTR conventions:**
  - A RonSQL test compares RonSQL with MySQL through
    `suite/ronsql/include/ronsql_compare.inc`, usually with
    `--let $strict_diff=yes`.
  - Every new `ronsql`, `ronsql_cte` or `ndb_push_agg` test also gets a
    mirror in the matching `_jit` suite. The mirror is a short `.test`
    that sources the original and pins the JIT fallback delta; see
    `mysql-test/suite/ronsql_jit/t/ronsql_cte_minmax_string_redist.test`.
  - Raw-signal hardening tests go in
    `storage/ndb/test/ndbapi/testSpjMalformed.cpp` (cases P1..P60, Q1..Q5,
    S1..S2), each with an MTR wrapper in `mysql-test/suite/ndb_spj/t/`
    (`test_spj_malformed_pNN.test`, `test_tc_queue_flag_qN.test`,
    `test_signal_scope_sN.test`). A table a case needs besides T1 is
    created by its MTR wrapper through mysqld (`CREATE TABLE ...
    ENGINE=NDB`, as `test_spj_malformed_p53` does) and looked up by the
    program, never created through the NDB API.
  - A test that runs `ndb_mgm` writes its output to its own
    `$MYSQLTEST_VARDIR/tmp/<test>_mgm.log` and removes it, never to the
    shared `$NDB_TOOLS_OUTPUT` (`ndb_testrun.log`): check-testcase fails
    the test that creates that file, or the one that removes it after an
    earlier test left it.
  - DBLQH protocol tests go in `testCteProtocol` and `testNodeRestart`
    (NF-*, PK-*, ID-*, TD-*).
- **Regression set** after a kernel change in this area:
  ```
  ./mtr --suite=ndb_spj
  ./mtr --suite=ndb_push_agg,ndb_push_agg_dist,ndb_push_agg_jit
  ./mtr --suite=ronsql,ronsql_jit,ronsql_cte,ronsql_cte_jit,ronsql_cte_ng1r3,ronsql_cte_ng2r2
  ./mtr ndb.ndbapi_agg_test
  ```
  Add `ronsql_cte_ng2r3,ronsql_cte_ng4r2` and `ronsql_large` for
  redistribution or finalize changes. Add `ronsql_cte_jit_census` for
  anything that touches the JIT glue.

## 2. Status of the analysis bugs (`dblqh_ronsql_analysis.md` §1)

| # | Bug | Status | Commit(s) |
|---|---|---|---|
| 1, 1b, 1c | CASE arms of different types share a typed slot; conditional Mov; string/numeric CASE MIN/MAX crash | Fixed | `f5768de93da` (+ `c57d796fafa`, `ed759e4eb0f` found by its test) |
| 2 | GREATEST/LEAST on a LEFT JOIN NULL-extended row takes the THEN path | Fixed | `f3374e1ee59` |
| 3 | Eviction retry fails the `read_length` entry check | Fixed | `6b932cc61ef` |
| 4 | CTE_SCAN drops rows that overflow the output buffer | Fixed | `055d717c40c` |
| 5 | `const_cast` on a const `CteLookupReq` | Fixed | `5a34a12df76` |
| 6 | 16-bit `key_len<<16 \| val_len` packing unchecked | Fixed | `aa79a8b37e5`, then `b8116dc6e4b` and `9346d8ecc48` (fragmented sends) |
| 7 | COMPLETE merge failure bypasses `abortCteRedistribution` | Fixed | `58156514912` |
| 8 | Coordinator-liveness rule differs between sites | Fixed | `cdbadd5edd7` |
| 9 | `JOIN_AGG_SEND_CONF` has no identity/phase check | Fixed | `e5a36af6839` |
| 10 | Scalar CTE_SCAN feed retries eviction only once | Fixed | `e49a3cfd846` (+ `c1a9d9c9b88`, which rebuilds the feed row after an eviction) |
| 11 | FINAL_REP dropped when the park pool is exhausted | Fixed | `5a6a60f37de` |
| 12 | Linked-buffer walks with uneven bounds checks | Fixed | `5bbc88fc296` |
| 13 | Single-table pushed scan stops the node when `Init` fails | Fixed | `d9aac889f01` |
| 14 | Aggregation programs from the API not validated | Fixed | `8878794d1fd` |
| 15 | Multi-leaf string MIN/MAX side table indexed by the leaf-local slot | Fixed | `6c79ad0f0e4` |
| 16 | Null charset on string slots created by redistribution | Fixed | `9a0fffe7475` |

New error codes and inserts added by these fixes:
- error insert DBTUP 4046 (bug 13): fails the pushed interpreter's `Init` as
  if QueryMemory ran out;
- 1256 `ZJOIN_AGG_RESULT_TOO_LARGE`, now only for the 16-bit length limit;
- `kErrMixedStringCaseArms` in `NdbAggregator`;
- error insert DBLQH 5161 (bug 8): PK-8b, 5150 for a parked SCAN_FRAGREQ
  (5160 until `af5143543a0`, §3.6);
- 1873 `ZAGG_GROUP_TOO_LARGE` (`66f0cea92cd`): a group whose key and slots
  do not fit one 32 KB query memory chunk, a permanent error;
- `kErrTooBigGroupKey` in `NdbAggregator`: a GROUP BY key over 65535 bytes;
- error insert DBLQH 5162 (`97adebdcf15`): ID-10b, the send batch byte
  limit from the extra value;
- error insert DBLQH 5164 (`71440369013`): a CTE group's key taken one
  word short in `buildCteLinkedBuffer` (testCteProtocol OV-3);
- error insert DBLQH 5163 (`8ce1fe1b401`): one aggregate group counted in
  the COMPLETE_CONF but not sent (testJoinAggNdbApi Test 25);
- error insert DBTUP 4047 (`bb2930940bb`): read the parameter area of a
  scan that is not from DBSPJ as per-range blocks all the same (P46);
- error insert DBTC 8320 (`81e0f764713`): send the pure EndOfData of a
  join aggregation twice, as DBTC does when a close crosses it
  (testJoinAggNdbApi Test 26);
- `kErrStringArithmetic` in `NdbAggregator` (`3e163568ac7`) and the RonSQL
  refusal "Arithmetic on string columns is not supported." (`25c152abe89`);
- `JIT_BRIDGE_EMBEDDED_NO_OUTPUT` (10, `917434677ee`): an embedded block
  with a path that ends without output 0 runs on the interpreter;
- a missing interpreter parameter fails with 876
  (`ZOUTSIDE_OF_PROGRAM_ERROR`, `75b7d80d4ba`), where it gave 99.

Bug 14 adds `AggInterpreterBase::checkProgram`, which runs:
- in `DblqhProxy::execJOIN_AGG_SETUP_REQ` for every leaf, before the
  optimizer and the JIT;
- in `Init`.

Programs that fail it are refused:
- a join aggregation setup with `JOIN_AGG_SETUP_REF` 20002;
- a single-table scan with 1868.

Bug 14 also added a per-row DECIMAL size check, which returns 1863.

## 3. Analysis bugs fixed on 2026-11-24, and what they left

Each fix has its own commit and test. What the fixes found and left open,
and what of that was fixed on 2026-11-25:

### 3.1 Bug 8 (`cdbadd5edd7`): one coordinator-liveness rule
- `Dblqh::joinAggCoordinatorDown(ref)` is the only liveness check, at
  seven sites (the analysis missed `cteScanAggFeed`). A DBTC coordinator is
  down when this instance's host record says ZNODE_DOWN, which is ordered
  with the reclaim, or when its transporter is not connected. Non-DBTC
  references (block tests) are never down; an out-of-range DBTC node id
  is down instead of failing `ptrCheckGuard`.
- Test: PK-8b (`testNodeRestart -n CteCoordinatorDiesParkedScanReplay`,
  `ndb_cte.cte_nodefail_coordinator_scan_replay`, error insert 5161). PK-8
  now also ignores `m_connected` for its replay.
- Open: the LQHKEYREQ guard uses the same helper but has no deterministic
  test with `m_connected` ignored.

### 3.2 Bug 9 (`e5a36af6839`): SEND_CONF accepted only for the paused send
- `execJOIN_AGG_SEND_CONF` drops a CONF unless the state is
  WAITING_SEND_CONF in its owner LDM (not a query block) and senderRef,
  senderData and requestId match the recorded COMPLETE.
  `continueJoinAggSend` returns on a released or aborting state. No signal
  change. Test: ID-9 in `testCteProtocol`.
- Fixed on 2026-11-25:
  - `97b0ad31b41`: the flow control counted every group of the send
    (`num_result_rows`, which also holds the groups evicted during the
    scan), so after the first batch every group cost a SEND_REQ/CONF
    round trip. A batch now counts its own groups, `send_batch_rows`,
    which the COMPLETE_REQ and each accepted SEND_CONF start at 0.
    Test: ID-10.
  - `97adebdcf15`: a batch also ends at MAX_SCAN_BATCH_SIZE (512 KB) of
    group records, `send_batch_bytes`, also started at 0 with each batch.
    resultBytes in SEND_REQ and COMPLETE_CONF is the bytes of a batch.
    Test: ID-10b (error insert 5162 sets the limit).
  - `773bc784a0e`: a keyed COMPLETE_REQ is checked against the state's
    transid and identWord (the CANCEL_REQ rule): another query's gets
    COMPLETE_REF 1251, a duplicate of the state's own query is dropped,
    and the ndbassert is gone. The owner forward keeps the received
    signal length. Test: ID-11.
- Open: `m_cte_complete_*` are reset at seize for CTE states only;
  harmless today.

### 3.3 Bug 11 (`5a6a60f37de`): a FINAL_REP that cannot park is answered
- The receiver has no state to abort, so it answers the sender with an
  error FINAL_REP (1275) by identity; the sender aborts and its
  COMPLETE_REF fails the query. An error report is never answered back.
  Test: PK-4b in `testCteProtocol` (two data nodes,
  `ndb_push_agg_dist.testCteProtocol_pk4b`); `testCteProtocol -n <case>`
  runs one case.
- Fixed on 2026-11-25 (`e553094416b`): the placeholder sweeper
  (`Dblqh::joinAggParkSweep`) dropped a parked success FINAL_REP. It now
  answers it as the park failure does, with 1274; both use
  `Dblqh::failUndeliveredJoinAggFinalRep`. The sweep delay went from
  10 ms to 200 ms (`JOIN_AGG_PARK_SWEEP_DELAY_MS`), so a SETUP that is
  only late no longer fails the query; 5139 now sends its SETUP_REF after
  1 s. Test: PK-4c (`ndb_push_agg_dist.testCteProtocol_pk4c`).
- Open: with DBTC, the late node's own COMPLETE usually parks on the same
  placeholder and its COMPLETE_REF already fails the query; PK-4c drives
  the other order by hand. Node-failure tests may now see 1274 from this
  source (NF-11, NF-12 and the 14359 case accept it).

### 3.4 Bug 12 (`5bbc88fc296`): one checked linked-buffer walk
- Not only defence in depth: an NI_ATTR_LINKED pattern may hold P_DATA
  tokens, which DBSPJ copies verbatim into the linked buffer, so an API
  node chose it word for word and the GROUP BY walk copied past the
  18000-word read buffer. `CteLinkedAttr::linkedEntryAH()` serves all nine
  walks; `Dblqh::parseCteAttrInfo()` the five AttrInfo header parses.
  Test: testSpjMalformed P35.
- Fixed on 2026-11-25, all reachable from an API node:
  - `519404769c7`: `handleReadLinkedColumnToReg` and
    `ndb_jit_h_load_linked_col` read an entry shorter than its type
    (`CteLinkedAttr::linkedColumnBytes`) as NULL, or take the per-row
    fallback. Test: P40.
  - `215d89048a5`: `buildCteLinkedBuffer` checks every GROUP BY key entry
    and every slot against the end of `cevictBuffer` and returns false
    (1265 / 1268) instead of writing past it or the `ndbrequire`.
    Test: OV-2 in `testCteProtocol`.
  - `27ee6ce3ca3`: `interpreterStartLab` sums the five header lengths in
    64 bits; a wrapped header fails with 882. Test: P41.
- What those three fixes left was done as well:
  - `4a8d69b4948`: the NULL-row evaluator (`evalEmbeddedForNullRow`)
    reads a short entry as NULL too, by the same
    `CteLinkedAttr::linkedColumnBytes` rule. Test: P48 (an outer join's
    NULL-extended rows).
  - `823e7cdd38d`: the JIT half of `519404769c7` is tested: P48's compare
    of a linked BIGINT with a constant compiles, a 4-byte entry falls
    back to the interpreter, reads NULL and fails the row (1872), where
    the JIT used to decode 8 bytes and count it.
    `ndb_spj.test_spj_malformed_p48_jit` checks `ndbinfo.jit`.
  - `71440369013`: OV-3 reaches the GROUP BY key check of `215d89048a5`
    (a CTE grouped by two BIGINTs with no aggregate slot, so the key
    ends the row; error insert 5164 makes a key entry claim more than
    the key holds).
  - `26554f04acf`: `Dbtup::scanCopyAttrinfo` and `nextAttrInfoParam`
    (rewritten for RONDB-1044, not upstream code) checked the parameter
    blocks of a multi-range interpreted scan with `ndbassert` only, and
    summed the header in 32 bits: at each new range
    (`Dblqh::accScanConfScanLab`) a release node could read or memmove
    outside `cinBuffer`, reachable by any API node with an interpreted
    range scan of two or more ranges (or through DBSPJ). Now the header
    is summed in 64 bits, every block must lie within the parameter
    area, and the scan fails with 882. Test: P46.
- Fixed on 2026-11-26:
  - `bb2930940bb`: an NDB API multi-range scan whose program has
    subroutines had its subroutine code read as parameter blocks from
    range 2 on (so did upstream WL#14388, within the section) and failed
    with 882. DBTUP now takes per-range blocks only for a SCAN_FRAGREQ
    from DBSPJ; any other scan runs every range with the whole area.
    Test: P49; P46 keeps its block checks under error insert 4047.
  - `1b5a0262a58`: DBSPJ refuses API words for header word 4 (the
    subroutine part of a PI_ATTR_INTERPRET program, 20005; a pattern of
    an NI_ATTR_INTERPRET node without NI_ATTR_LINKED, 20004), which hit
    `ndbassert(false)` in a debug node and an `ndbrequire` in a release
    node for an NI_ATTR_LINKED leaf. Test: P50.
- Open: an outer-joined aggregation lookup leaf sent as raw signals
  (P48's first half) has not been seen to run.

### 3.5 Signal scopes for the JOIN_AGG and CTE signals (`9ff48bcde6a`)
- None of the 29 JOIN_AGG_* and CTE_* GSNs had an entry in
  `SignalScopes.hpp`, so a release data node ran their handlers for an
  API node too (the FINAL_REP `ndbrequire(senderNodeId <
  ABS_MAX_NDB_NODES)`, bug 9, the keyed COMPLETE_REQ, direct CTE_LOOKUP /
  CTE_SCAN). Now: Local for JOIN_AGG_NODE_FAIL_REP and
  JOIN_AGG_NULL_ROW_CONF / _REF; Remote for the replies to DBTC and DBSPJ
  and DBTC <-> DBSPJ; NDB_SCOPE_REMOTE_EXCEPT_DEBUG for the twelve the
  block unit tests send from an API node. A release node drops these from
  an API node (violation 32, Tier A). Tests: S1, S2 (S2 release only).
- A new block test that sends another of these from an API node needs
  its entry changed to NDB_SCOPE_REMOTE_EXCEPT_DEBUG.

### 3.6 Error insert 5160 was taken twice (`af5143543a0`)
- RONDB-1146 (`db2236f6e87`) took DBLQH 5160 (a delayed COMMIT, testNdbApi
  `UkCommittedReadCommitRace`) outside the RONDB-1124 block of
  `ERROR_codes.txt` without moving its "Next DBLQH" line, and bug 8's
  PK-8b took 5160 again. PK-8b now uses 5161. Check a number with
  `git grep -w` before taking one (§1).

### 3.7 `ronsql_join` Test 3: `assert(m_finalWorkers < getWorkerCount())`
- The abort (2026-09-14, once in 21 runs, no trace kept) is a second
  termination of the pushed scan in the API. A join aggregation ends with
  the aggregate SCAN_TABCONF, which promises the aggregate rows, and the
  pure EndOfData, which makes every worker final; the application thread
  then waits for any promised rows still missing.
- Fixed (`8ce1fe1b401`), candidate 1: when that wait ended in a timeout
  or a node failure, `NdbQueryImpl::awaitMoreResults` called
  `setFetchTerminated`, which requires a worker that is not final, so a
  debug API aborted. Now it only records the error (4008 / 4028). Test:
  testJoinAggNdbApi Test 25 (`ndb_push_agg.testJoinAggNdbApiMissingRows`,
  error insert 5163 plus `ndb_reduced_api_protocol_timeout`).
- The other two candidates are fixed too (2026-11-26):
  - `2ce87f392be`: `closeTcCursor`'s two waits called
    `setFetchTerminated` on a failed wait even with every worker final;
    `NdbQueryImpl::setWaitFailed` now holds the rule of `8ce1fe1b401` for
    all waits. No test: nothing can make the TC node's disconnect fall
    into the poll that delivers the final EndOfData.
  - `81e0f764713`: a close that crosses DBTC's own end of the scan gets a
    second pure EndOfData ("Confirming scan close"). With every worker
    final it is dropped (`execCLOSE_SCAN_REP` returns false), `close()`
    detaches the query under a PollGuard, a scan transaction with no
    scanning query drops it instead of asserting, and a close sent after
    a receive timeout force-releases the scan transaction, so the stale
    EndOfData cannot end the next scan on a reused connection (the RDRS
    candidate). Test: testJoinAggNdbApi Test 26
    (`ndb_push_agg.testJoinAggNdbApiDupEndOfData`, debug only, error
    insert 8320); the reused-connection case has no test.
- Open: which candidate caused the 2026-09-14 abort is not known; all
  three are fixed. If it comes back, the way to find out is a trace of
  the API's scan events per query (confs, EndOfData, REFs, waits,
  closes) dumped before the assert, while Test 3 runs a few hundred
  times.

### 3.8 Fixes of 2026-11-26 and 2026-11-27

One commit each, with its test. Kernel hardening against raw API input
(all tested by testSpjMalformed with every data node started, MTR wrapper
`ndb_spj.test_spj_malformed_pNN`):
- `1b5a0262a58`, `bb2930940bb`: §3.4.
- `e226cead16a`: a GROUP BY key whose VARCHAR / LONGVARCHAR length prefix
  runs past its value (linked entries, CTE_LOOKUP keys) is refused before
  `hashKeyFull`, `findInBucket`, `singleRowCteProbe` or the CTE ORDER BY
  top-N reads past it or fails a `require` (1869, CTE_LOOKUP_REF 1265,
  DBSPJ 20002). Tests: P52, `gb_hash_table-t` (`b2fe392c06f`).
- `db7f5dce700`: `handleBranchAttrOp` and its JIT mirror refuse a compare
  value that `check_compare_values` refuses (a VARCHAR prefix past the
  value, a NaN FLOAT / DOUBLE, a wrong size) with 40. Test: P53.
- `10fa2b555cd`: `checkAggSlotKinds` refuses arithmetic, SUM, AVG or a
  typed MIN / MAX of a string register and two different aggregates of
  one slot; per row, `minMaxString` refuses a value of another string
  type than its slot's, `kOpMov` copies the string capture,
  `kOpSetRegNull` zeroes the value, `loadColumnTypedFromBuf` refuses a
  short value or a prefix past the value (1866), and AggInterpreter
  refuses a `kOpLoadCol` of another type than the column's. Tests:
  P54 (a-c), P55, P56.
- `b306c658e03`: a column id past the table in an aggregation program
  (kOpLoadCol, GROUP BY, a JIT scan filter's attribute) fails with 879
  instead of an ndbassert, and `initGBTypes` no longer reads the
  descriptor array past the table. Test: P54 (d).
- `9d013e4d714`: `publishGBTypes` refuses a GROUP BY column whose type
  has no compare function (BLOB, TEXT, undefined, unknown) with 1869;
  `checkGroupKey` and `compareGroupsByOrderSpec` pick their columns with
  one helper, `orderGroupByType`. Tests: P59, NdbSqlUtil-t.
- `a35f780b3f0`: `validateEmbeddedProgram` refuses an embedded branch of
  offset 0 (a branch to itself, which held the LDM thread), and
  `interpreterJumpTable` refuses a program counter that does not move
  forward. Test: P57, bridge_tests T72f-g.
- `aa81e153e77`: `validateEmbeddedProgram` refuses a branch into the
  operand words of an instruction and an instruction past the end of the
  program, for every opcode; the JIT bridge refuses such a branch
  (`JIT_BRIDGE_MALFORMED`); `interpreterNextLab` checks the program
  counter before it fetches. Tests: P58, bridge_tests T72h-k.
- `75b7d80d4ba`: every multi-word interpreter instruction checks its
  operand words against the end of its program, and
  `lookupInterpreterParameter` is bounded by the subroutine section, both
  failing with 876. Tests: P60 (a-m), bridge_tests T87a-j.

JIT (`ndb_jit_bridge.c`; each refused program runs on the interpreter):
- `917434677ee`: §4.3, an embedded block with a path that ends without
  output 0. Tests: bridge_tests T86a-e, P40 with the JIT
  (`test_spj_malformed_p40_jit`).
- `d950f587d15`: an embedded read of a register that a path did not write
  (`JIT_BRIDGE_REG_NOT_LOADED`). Tests: T86f-i, P51 and
  `test_spj_malformed_p51_jit`.
- `0860e7af007`: a branch onto an output block's WRITE, or between its
  LOAD_CONST16 and the WRITE. Tests: T86j-k.

NDB API and RonSQL:
- `2ce87f392be`, `81e0f764713`: §3.7.
- `3e163568ac7`: `NdbAggregator` refuses arithmetic on a register whose
  last load is a string (`kErrStringArithmetic`), and the constant loads
  refuse a register number of 8 or more. Test: NdbAggregatorMerge-t.
- `25c152abe89`: RonSQL refuses arithmetic on a string column while
  planning. Test: `ronsql.ronsql_string_arithmetic` (no JIT mirror: no
  program reaches a data node).
- `8de5c290363`: the ScanTabReq flag setters clear the bit when passed 0
  (P46's control had gone to DBSPJ).

Found by test runs:
- `b6f0f91c2c2` (DBLQH): with MUTEX_BASED feeders,
  `sendEvictedAggGroup` evicted a group and counted `m_rows_sent`
  without the interpreter's lock, so an eviction could free a group
  another LDM thread was accumulating into. It now holds
  `lockFeederMutex()` around both. Every eviction was affected, not only
  the error inserts. Found by `ronsql_large.ronsql_large_mem_leak_errors`.
- `27f1c908f75`: Test 26 executed a query definition whose tables
  `runAndVerifyT14Query` had invalidated (SIGSEGV in the test program).
- `10c4f5f628b`, `b1500fc831d`: `testJoinAggNdbApiDupEndOfData`,
  `ndb_join_pushdown_agg_evict`, `ndb_pushdown_agg_drain` and
  `ndb_pushdown_agg_init_fail` keep their `ndb_mgm` output out of
  `ndb_testrun.log` (§1).
- `2ef4f179545`: TPC-H Q9 is skipped again in `rdrs2-ronsqltpch`: its
  aggregation reads sibling branches that are not ancestors of the
  aggregation leaf, which RonSQL refuses at prepare. Supporting it is the
  next phase, F-fanout in `cte_fix_plan.md`.
- `e54961447c6`: P53's table is created through mysqld;
  `75ae88908de`: clang's `-Wstring-concatenation` in testSpjMalformed.

Also on the branch, outside this list: `aa73f997980` (NdbReceiver calloc
argument order, GCC 14), `d72c9437333` (aarch64 JIT veneer for a far
helper), `0a84882d297` (TRPMAN state for CLOSE_COMREQ before READ_CONFIG).

## 4. Gaps left by the fixes

### 4.1 RonSQL (the next session's area)
- **WHERE arithmetic on LEFT JOIN NULL-extended rows: fixed.** A filter
  over two INNER-joined tables above a LEFT-joined leaf, e.g.
  `WHERE a.x + 1 > b.y` (each side reads one table), runs as an embedded
  program in the leaf's aggregation program. On a NULL-extended row,
  `evalEmbeddedForNullRow` returned `EMB_NULL_ROW_UNSUPPORTED` for
  ADD/SUB/MUL_REG_REG, and `ProcessRec` took the THEN path, so the row was
  counted even when the WHERE was false. The evaluator now computes them as
  DBTUP's handlers do: `compareTypedRegs` and `applyTypedArith` moved from
  `Dbtup::InterpreterContext` to the new `dbtup/TypedRegisterOps.hpp`,
  shared by both (the evaluator's copy `nullRowCompareRegs` is gone). Test:
  `ronsql_left_join_cross_arith` (+ `ronsql_jit` mirror) ar-0..ar-7: ADD,
  MUL, SUB, a nullable operand, OR, unsigned, no GROUP BY, an index-scan
  LEFT child.
  - Still `EMB_NULL_ROW_UNSUPPORTED` (THEN path): READ_*_MEM_TO_REG, which
    no API emits in an embedded program, and BRANCH_MEM_OP_ARG without
    `meta_tup`.
- **`is_null_rejecting` and OR: fixed** by C-2 (`6c52f4ee9b0`, RonSQL
  executor review, `../ronsql_executor/README.md`). `is_null_rejecting` now
  takes the LEFT-joined table and counts only its columns as NULL, so
  `c LEFT JOIN o ... WHERE o.amt > 5 OR c.fl = 30` is no longer promoted. A
  WHERE conjunct that stays on a LEFT-joined op is refused ("WHERE on a LEFT
  JOIN's right side that holds for NULL is not supported") until post-join
  filtering exists. Test: `ronsql_left_join_where` lj-5, lj-6, lj-r5..r7.
- **CASE MIN/MAX whose arms are all strings fails to print: fixed.** RonSQL
  refuses mixed arms (bug 1). All-string arms passed, but
  `ResultPrinter::aggregate_arg_charset` only followed a direct column Load,
  so printing and ORDER BY on the aggregate failed with "Could not find
  charset for string aggregation result". It now takes the charset of the
  THEN arm's column, or the ELSE arm's, which `RepeatAgg` requires to be one
  collation. CHAR arms of different sizes then stopped a debug RDRS:
  `NdbSqlUtil::cmpChar` asserts that both CHAR values have one length, and
  the merge compared a CHAR(4) and a CHAR(6) partial result. String MIN/MAX
  results now compare with `NdbSqlUtil::getStringResultCmp`, which for CHAR
  is `cmpCharAnyLength` (cmpChar without the length check; strnncollsp
  pads, NO_PAD strips trailing spaces): `AggInterpreterBase::minMaxString`,
  `JoinAggInterpreter`'s string slot merge, `NdbAggregator::compareStringSlots`
  and `ResultPrinter`'s ORDER BY. Test: `ronsql_case_minmax_string`
  (+ `ronsql_jit` mirror) cm-1..cm-6: VARCHAR(10)/VARCHAR(20) and
  CHAR(4)/CHAR(6) arms, no GROUP BY, ORDER BY the MIN, join aggregation,
  ORDER BY the MAX of the CHAR arms. A CTE output over a CASE aggregate is
  still refused ("CTE aggregate over complex expression").
  - Fixed in `32374dc4f82`: `NdbAggregator::Finalize` counted a string
    MIN/MAX over a CASE at its first arm only. That was an overflow, not
    only an under-estimate: the API copied a record with a wider later arm
    past its fixed 16 KB receive buffer. Finalize now counts the widest
    arm, and since `66f0cea92cd` the receive buffers grow (§4.2).
- **The data node guard against bug 1c: fixed** (`3e71092b7d6`). A program
  that mixes a string MIN/MAX and a number on one slot was refused by
  `NdbAggregator::RepeatAgg` and by mysqld, but not by the data node, and
  the numeric aggregate overwrote the string pointer in the slot.
  `PushdownInterpreter::checkAggSlotKinds`, last in `checkProgram`, walks
  the program's jumps with the possible kind of each register (string,
  other, not loaded) and refuses a slot that gets both (20002 / 1868). It
  reads the skips of an embedded program with `embeddedSkips`, which
  replaced the optimizer's `addEmbeddedJoinPoints`: it follows
  LOAD_CONST16 values to the output-0 write also through a branch that
  lands on it (RonSQL's guarded COUNT(*)), so the bug 1 optimizer may now
  type more opcodes after such programs. Tests: P38, P39,
  `testVarcharMinMax`.
  - Fixed on 2026-11-26 (`10fa2b555cd`, §3.8): SUM or arithmetic on a
    string register; a `kOpLoadCol` whose declared type is not the
    column's; `kOpMov` of a string did not copy its string data. The API
    and RonSQL refuse string arithmetic too (`3e163568ac7`,
    `25c152abe89`).

### 4.2 Kernel
- **An aggregate can read a register the row never loaded (K-1 in
  `../ronsql_executor/README.md`): fixed (K-1b).** Bug 14's
  `checkProgram` covers the rest of K-1, but not this. The skip after an embedded program is
  computed per row, so no check before the program runs can see it. Sum,
  Max, Min, Count and the typed variants only
  `assert(a.type != NDB_TYPE_UNDEFINED)`. Registers are zeroed once in
  Init, so in a release build the aggregate reads an UNDEFINED register or
  the previous row's value. RonSQL reached it through D-1 (fixed in
  `b0371bb91ef`). Before that fix, `ronsql_subquery_filter_decimal`
  aborted both data nodes in a debug build.
  - Fix: `AggInterpreterBase::m_loaded_regs`, a per-row bitmask of
    loaded registers cleared at row start. A load, a constant or
    kOpSetRegNull sets its register's bit; kOpMov and the arithmetic ops
    need their inputs set and set their result's. An aggregate, kOpMov or
    arithmetic op on a register not in the mask fails the row with 1868;
    READ_AGG_REG_TO_REG fails with 878 in DBTUP
    (`InterpreterContext::aggRegistersLoaded`) and in
    `evalEmbeddedForNullRow`. The JIT zeroes its registers per row and
    would have aggregated 0, so `ndb_jit_bridge.c` tracks the registers
    loaded on every path (AND-merged where CASE jumps land) and refuses a
    program that may read another (`JIT_BRIDGE_REG_NOT_LOADED`); it runs
    on the interpreter. Tests: testSpjMalformed P32 (also with the JIT,
    `test_spj_malformed_p32_jit`), bridge_tests T85a-e.
  - Fixed (K-1a): `peekProgramHeader` checked the magic word, `prog_len`,
    the reserved flag bits and the 1024-word limit with `assert` only, so
    a raw NDB API client could abort a debug data node with a bad header
    word. `AggInterpreterBase::checkProgramHeader` now refuses them, in
    `checkProgram` (JOIN_AGG_SETUP_REF 20002) and in `peekProgramHeader`,
    which fails Init and sets `program_rejected()` so the callers report a
    rejected program, not a lack of query memory. Tests: testSpjMalformed
    P30 / P31.
- **Bug 13 leftovers:**
  - `ndbrequire(regTcPtr->opExec)` in `execSCAN_FRAGREQ`: fixed in
    `01e72fb8dbb` (SCAN_FRAGREF 1868, testSpjMalformed P33). DBTC and DBSPJ
    never send the combination, and a release 26.10 node takes SCAN_FRAGREQ
    only from data nodes, so only a debug node was reachable. 26.02 and
    26.05 have no signal scopes: there any API node can reach it, and
    other `ndbrequire`s of that handler too.
  - The primary-key read path (`handleAggReadRow`) has no MTR test for 4046,
    because RonSQL retries temporary errors. A block or NDB API test is needed.
  - The next-range path needs no change: the interpreter exists by then.
- **Bug 16 leftover: fixed** in `e238f66561f`. An inline-type compare
  with charset 0 on a string type reached `cmpChar` with a null collation
  (reachable through the NDB API); a non-string type with a charset failed
  `cmpBinary`'s assert. `NdbSqlUtil::get_compare_collation` checks it at
  all five decode points, and `validateEmbeddedProgram` refuses such an
  embedded program at SETUP. Tests: testSpjMalformed P36 / P37,
  testCteNdbApiFilter Test 33. Fixed on 2026-11-25:
  - `56ca313347a`: GROUP BY metadata whose collation does not fit its
    type (a string with charset 0, which reached the ORDER BY top-N
    compare with a null collation; a non-string with a charset) is
    refused by `AggInterpreterBase::initGBColType`: at SETUP with 20002,
    from a marker or column metadata with 1869. GROUP BY keys compare
    with `getStringResultCmp` (CHAR padded). Tests: P43, P44.
  - `a0aa4bfc8c4`: the five compare sites clamp the length to what was
    staged and check the values with `NdbSqlUtil::check_compare_values`
    (error 40); CHAR values of different lengths compare padded. RonSQL
    itself sends CHAR(8) vs CHAR(12), which stopped a debug node, and
    `cmpVarchar`'s `require` stopped a release node too. Tests: P45,
    testCteNdbApiFilter Tests 34 / 35, NdbSqlUtil-t.
  - Fixed on 2026-11-26 (§3.8): the GROUP BY key compare sites
    (`findInBucket`, `singleRowCteProbe`, `compareGroupsByOrderSpec`)
    trusted a VARCHAR length prefix, and `hashKeyFull` and DBSPJ
    `cte_lookup_hash_key` ignored `get_var_length`'s result
    (`e226cead16a`); upstream `handleBranchAttrOp` had the same `require`
    on API constants (`db7f5dce700`).
- **Single-table pushed aggregation results: not a gap.** DBTUP has sent
  a record over one message fragmented since RONDB-874, and the API
  reassembles it. The real limit was the API's fixed 16 KB receive buffer,
  guarded by `NdbAggregator::Finalize`'s estimate, which counted a string
  MIN/MAX over a CASE at its first arm only: fixed in `32374dc4f82`
  (counted at its widest arm). `66f0cea92cd` then replaced the fixed
  buffer: receive buffers start at a typical record size from Finalize
  and grow on demand; Finalize's limit is the record format's, 65535 bytes
  each for the GROUP BY key and the values; and DBTUP fails a group whose
  key and slots do not fit one 32 KB query memory chunk with the
  permanent error 1873 (`ZAGG_GROUP_TOO_LARGE`) instead of the temporary
  1870.
- **`evictOneGroup` (JoinAggInterpreter.cpp:1992) pops the group before it
  checks the size.** If the size check refuses the group, it is already off
  the chunk list. This is minor.
- **Mixed versions.** An API from before `9346d8ecc48` that gets fragmented
  join-aggregation records from an upgraded data node uses the old receive
  path, which mixes interleaved fragments. There is no version gate (alpha
  precedent); mention it in the release notes.
- **Missing test hooks:**
  - a second eviction in the scalar CTE_SCAN feed (bug 10's loop). Error
    insert 5126 caps a target at 3 groups, so one eviction always makes room.
    A new insert must refuse the first allocation after an eviction;
  - error inserts that make the bug 3 (allocation failure on the retry) and
    bug 7 (COMPLETE merge failure) tests deterministic.

### 4.3 JIT
- **JIT and interpreter differed on an embedded program that exits
  without writing output 0** (found by `823e7cdd38d`): fixed in
  `917434677ee`. The bridge refuses such a program
  (`JIT_BRIDGE_EMBEDDED_NO_OUTPUT`), so it runs on the interpreter. The
  fold of `BRANCH_REG_EQ_NULL` stays, but only for a register every path
  wrote (`d950f587d15`); a branch onto an output block's WRITE is refused
  (`0860e7af007`).

Optional, performance only:
- Fuse compare-then-jump pairs in scan filters into one inverted compare
  (EQ↔NE, LT↔GE, LE↔GT). This halves the op count of the AND/OR trellis that
  `NdbScanFilter` emits. NULL handling must match the interpreter for each
  condition.
- The program cache does not remember refusals (`jit_progcache.c:285`), so a
  refused filter is retranslated and counted on every scan. Caching refusals
  would change every fallback pin in the `_jit` suites.

### 4.4 Open after 2026-11-27

- **Next phase: F-fanout and TPC-H Q9** (`cte_fix_plan.md`, "Deferred
  feature work"). An aggregation that reads columns of sibling branches
  that are not ancestors of its leaf is refused at prepare
  (`linked_source_is_leaf_ancestor`). Chain INNER-join PK / unique-key
  lookup siblings into a line when the query aggregates, as the planner
  does for sibling CTE_LOOKUPs, then clear Q9's skip in
  `ronsqltpch/handler_test.go`.
- **Untested paths of the 2026-11-26 fixes:**
  - the CTE_LOOKUP probe and CTE ORDER BY paths of `e226cead16a`, and
    the single-table paths of `b306c658e03` (a GROUP BY column id past
    the table) and `9d013e4d714` (a TEXT GROUP BY column):
    testSpjMalformed cannot build a CTE and its table has no TEXT
    column;
  - `2ce87f392be` and the reused-connection case of `81e0f764713`
    (timing that no test can force).
- **NdbAggregator follows no CASE arms:** a program in which a string
  reaches arithmetic on one path only passes the builder; the data node
  refuses it (`checkAggSlotKinds`).
- **Tables created through the NDB API in tests:** every testSpjMalformed
  MTR wrapper has the program create `TEST_DB.T1` (`--stop-on-error T1`),
  and P46 / P49 create an ordered index on it, through the NDB API,
  against the rule in §1. Move them to the wrappers (mysqld) as P53 does.
- **Watch items:**
  - a RonSQL CTE output column of undefined type used as an outer
    GROUP BY key could now get 1869 from `publishGBTypes`
    (`9d013e4d714`); the `ronsql_cte` suites would show it;
  - `ndb_scan_protocol_timeout`: the "Leaks" lines of its results decide
    whether the force-release on a close after a receive timeout
    (`81e0f764713` (c)) stays;
  - `evictOneGroup` still pops the group before it checks the size
    (§4.2).

## 5. Rest of the analysis report (not started)

- §1 "Smaller items": NULL string slot encoding in `emitCteLinkedAggSlot`,
  `MaxBigint`/`MinBigint` unsigned flag, `sendJoinAggSetupRef` field
  zeroing, `LLONG_MIN DIV 1`, DIV with DOUBLE operands, the DATE arm in the
  JIT helper, stale comments and logs, and the CONTINUEB pointer packing order.
- §2b/§2c: the missing tests. Those for bugs 1, 2, 13, 14 and 15 now exist;
  the DBLQH signal-layer gaps in §2c are still open.
- §2d/§2e: error inserts that no test sets, stale doc references, and dead code.
- §3: deduplication (~2 500 lines). Start with the DBLQH REF/CONF builders as
  cold noinline senders taking `callerLine` (§3b-1). The same change covers
  hot-path items L3/L6/L7/L8.
- §4: moving error handling off hot paths. These need a release build and
  before/after numbers (§6 of the report).
