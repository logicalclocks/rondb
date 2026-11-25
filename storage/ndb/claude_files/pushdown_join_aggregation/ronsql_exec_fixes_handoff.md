# RONDB-1124 exec fixes: status and outstanding work

Date: 2026-11-25. Branch `RONDB-1124-exec-fixes` on `origin`
(https://github.com/mronstro/rondb). PR #1231 was merged into 26.10-main on
2026-11-18; PR #1241 (logicalclocks/rondb, base 26.10-main) holds the work
after it. The open analysis bugs 8, 9, 11 and 12 and three gaps of §4.2
were fixed on 2026-11-24, one commit each (§2, §3). On 2026-11-25 most of
what those fixes left open was fixed too (§3, §4), the JOIN_AGG and CTE
signals got signal scopes (§3.5), and a collision of error insert 5160
was resolved (§3.6). One candidate cause of the `ronsql_join` Test 3
abort in the NDB API is fixed (§3.7).

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
- **One change at a time.** Propose one fix, wait for approval, then apply it.
  After the maintainer reports that the tests pass, commit and push.
- **Commits.** Subjects use the `BLOCK: description` style (e.g.
  `DBLQH: ...`, `DBTUP/NDBAPI: ...`, `RonSQL: ...`). The body explains the bug,
  the fix and the new test, and ends with the `Co-Authored-By:` line from the
  session's instructions. Make one commit per bug, with its tests.
- **Error inserts.** These numbers are free next, as recorded on the "Next"
  lines in `storage/ndb/src/kernel/blocks/ERROR_codes.txt`:
  - DBTUP 4047;
  - DBLQH 5165. A DBLQH number can also be taken by an entry outside
    the RONDB-1124 block of `ERROR_codes.txt`: check with
    `git grep -w <n> storage/ndb/src/kernel` before taking one;
  - DBTC 8135 / 8320 (RonDB);
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
    `storage/ndb/test/ndbapi/testSpjMalformed.cpp` (cases P1..P48, Q1..Q5,
    S1..S2), each with an MTR wrapper in `mysql-test/suite/ndb_spj/t/`
    (`test_spj_malformed_pNN.test`, `test_tc_queue_flag_qN.test`,
    `test_signal_scope_sN.test`).
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
  the COMPLETE_CONF but not sent (testJoinAggNdbApi Test 25).

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
- Open:
  - an NDB API multi-range scan whose program has subroutines gets its
    subroutine code read as parameter blocks from range 2 on (so did
    upstream WL#14388, within the section); it now fails with 882. To
    make it work, take per-range blocks only from DBSPJ.
  - DBSPJ (debug builds) hits `ndbassert(false)` when an API's
    PI_ATTR_INTERPRET / NI_ATTR_INTERPRET words land in header word 4.
  - an outer-joined aggregation lookup leaf sent as raw signals (P48's
    first half) has not been seen to run.

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
- Open: whether this was the cause. It needs a promised row that never
  arrives and a stall of 3 × the receive timeout (about 6 minutes in
  MTR). The other candidates: a close reply that crosses the EndOfData
  after an earlier error (`closeTcCursor` → `sendClose` while a worker is
  not final, delivered before `close()` clears `m_scanningQuery`), and a
  late terminator of an earlier scan on a reused scan transaction (RDRS
  only). The way to find out is a trace of the API's scan events per
  query (confs, EndOfData, REFs, waits, closes) dumped before the assert,
  while Test 3 runs a few hundred times.

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
  - Still open (raw API only, debug asserts): SUM or arithmetic on a
    string register; a `kOpLoadCol` whose declared type is not the
    column's; `kOpMov` of a string does not copy its string data.

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
  - Still open: the GROUP BY key compare sites (`findInBucket`,
    `singleRowCteProbe`, `compareGroupsByOrderSpec`) trust a VARCHAR
    length prefix from the metadata; `hashKeyFull` and DBSPJ
    `cte_lookup_hash_key` ignore `get_var_length`'s result; upstream
    `handleBranchAttrOp` has the same `require` on API constants.
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
- **JIT and interpreter differ on an embedded program that exits
  without writing output 0** (found by `823e7cdd38d`). The interpreter
  fails such a row with 1872; the bridge (`ndb_jit_bridge.c`) treats
  `EXIT_OK` without output as accept and counts the row. RonSQL and
  mysqld always write output 0, so only a hand-built NDB API program
  gets different results (no crash). Fix: the bridge refuses such a
  program, so it runs on the interpreter. Also, the bridge folds a
  `BRANCH_REG_EQ_NULL` on a linked register away rather than refusing
  it.

Optional, performance only:
- Fuse compare-then-jump pairs in scan filters into one inverted compare
  (EQ↔NE, LT↔GE, LE↔GT). This halves the op count of the AND/OR trellis that
  `NdbScanFilter` emits. NULL handling must match the interpreter for each
  condition.
- The program cache does not remember refusals (`jit_progcache.c:285`), so a
  refused filter is retranslated and counted on every scan. Caching refusals
  would change every fallback pin in the `_jit` suites.

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
