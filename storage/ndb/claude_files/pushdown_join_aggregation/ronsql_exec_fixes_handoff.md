# RONDB-1124 exec fixes: status and outstanding work

Date: 2026-11-24. Branch `RONDB-1124-exec-fixes` on `origin`
(https://github.com/mronstro/rondb). PR #1231 was merged into 26.10-main on
2026-11-18; PR #1241 (logicalclocks/rondb, base 26.10-main) holds the work
after it. The open analysis bugs 8, 9, 11 and 12 and three gaps of §4.2
were fixed on 2026-11-24, one commit each (§2, §3).

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
  - DBLQH 5162. A DBLQH number can also be taken by an entry outside
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
    `storage/ndb/test/ndbapi/testSpjMalformed.cpp` (cases P1..P29), each
    with an MTR wrapper `mysql-test/suite/ndb_spj/t/test_spj_malformed_pNN.test`.
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
- error insert DBLQH 5161 (bug 8): PK-8b, 5150 for a parked SCAN_FRAGREQ.

Bug 14 adds `AggInterpreterBase::checkProgram`, which runs:
- in `DblqhProxy::execJOIN_AGG_SETUP_REQ` for every leaf, before the
  optimizer and the JIT;
- in `Init`.

Programs that fail it are refused:
- a join aggregation setup with `JOIN_AGG_SETUP_REF` 20002;
- a single-table scan with 1868.

Bug 14 also added a per-row DECIMAL size check, which returns 1863.

## 3. Analysis bugs fixed on 2026-11-24, and what they left

Each fix has its own commit and test. What the fixes found and left open:

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
- Open:
  - performance: the flow-control check `num_result_rows >=
    m_max_batch_rows` uses a cumulative count, so after the first 256
    groups every group costs a SEND_REQ/CONF round trip;
  - a keyed COMPLETE_REQ is not identity-checked; a stale or duplicate one
    hits the duplicate-REQ `ndbassert` in a debug build.

### 3.3 Bug 11 (`5a6a60f37de`): a FINAL_REP that cannot park is answered
- The receiver has no state to abort, so it answers the sender with an
  error FINAL_REP (1275) by identity; the sender aborts and its
  COMPLETE_REF fails the query. An error report is never answered back.
  Test: PK-4b in `testCteProtocol` (two data nodes,
  `ndb_push_agg_dist.testCteProtocol_pk4b`); `testCteProtocol -n <case>`
  runs one case.
- Open: the placeholder sweeper (`Dblqh::joinAggParkSweep`) still drops a
  parked success FINAL_REP silently. If a SETUP is only late (more than
  10 ms), the same hang may be possible; the same answer, with 1274,
  would close it.

### 3.4 Bug 12 (`5bbc88fc296`): one checked linked-buffer walk
- Not only defence in depth: an NI_ATTR_LINKED pattern may hold P_DATA
  tokens, which DBSPJ copies verbatim into the linked buffer, so an API
  node chose it word for word and the GROUP BY walk copied past the
  18000-word read buffer. `CteLinkedAttr::linkedEntryAH()` serves all nine
  walks; `Dblqh::parseCteAttrInfo()` the five AttrInfo header parses.
  Test: testSpjMalformed P35.
- Open:
  - `handleReadLinkedColumnToReg` and `ndb_jit_h_load_linked_col` decode
    up to 8 bytes without checking the entry's byte size against the type
    width (the NULL-row evaluator checks it);
  - `buildCteLinkedBuffer` copies the GROUP BY key into `cevictBuffer`
    without a capacity check;
  - DBTUP `interpreterStartLab` sums the five header lengths in 32 bits,
    which can wrap, and `prepareAndHandleJoinAggRow` relies on that sum.

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
  - Still open: `NdbAggregator::Finalize` estimates string results from the
    first arm's column only. The estimate only guards
    `MAX_AGG_RESULT_BATCH_BYTES`, and each value is copied at its own
    length, so a longer ELSE arm under-estimates and is not an overflow.
- **The data node has no guard against bug 1c.** A program that mixes string
  and numeric MIN/MAX arms on one slot is refused by `NdbAggregator::RepeatAgg`
  and by mysqld, but not by the data node. Bug 14's `checkProgram` checks
  structure, not slot types. A raw NDB API client can still send one, and the
  THEN arm stores a string pointer that the ELSE arm's `MinBigint` overwrites,
  which can crash the node. Fix: in `checkProgram` (or the optimizer's
  per-slot type pass from bug 1), refuse a slot whose writers mix string and
  non-string. Add a testSpjMalformed case.

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
  testCteNdbApiFilter Test 33. Still open:
  - GROUP BY metadata with charset 0 for a string column leaves the
    collation null; the ORDER BY top-N compare
    (`JoinAggInterpreter.cpp` ~495) calls `cmpFn(nullptr)`;
  - `cmpChar` asserts equal lengths, which a CHAR(8) vs CHAR(12)
    `BRANCH_LINKED_OP_LINKED` can fail, and
    `handleBranchMemOpArgInlineType` does not clamp `attrLen` to the
    staged entry's size (debug asserts / `require`).
- **Single-table pushed aggregation results: not a gap.** DBTUP has sent
  a record over one message fragmented since RONDB-874, and the API
  reassembles it. The real limit was the API's fixed 16 KB receive buffer,
  guarded by `NdbAggregator::Finalize`'s estimate, which counted a string
  MIN/MAX over a CASE at its first arm only: fixed in `32374dc4f82`
  (counted at its widest arm). Receive buffers sized from the estimate
  and grown on demand, with the record format's 64 KB key / value limits
  instead of 16 KB, are the next change.
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

### 4.3 JIT (optional, performance only)
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
