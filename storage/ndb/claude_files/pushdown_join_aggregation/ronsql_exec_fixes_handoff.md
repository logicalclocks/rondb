# RONDB-1124 exec fixes: status and outstanding work

Date: 2026-11-12. Branch `RONDB-1124-exec-fixes` @ `8878794d1fd`, pushed to
`origin` (https://github.com/mronstro/rondb). It is 7 commits ahead of
`origin/26.10-main` and 0 behind, so a PR targets **26.10-main**. No PR is
open yet.

This file is the starting point for whoever continues the work. It records:
- what the analysis in `dblqh_ronsql_analysis.md` found;
- what has been fixed, and in which commits;
- what is still open, with current code locations.

That report's line numbers are from `50eb491eb3f` and have moved; use the
locations given here. The status column of its table is not updated; the
table in §2 below is the current status.

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
  - DBTUP 4045;
  - DBLQH 5155;
  - DBTC 8135 / 8318 (RonDB);
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
| 8 | Coordinator-liveness rule differs between sites | **Open** | §3.1 |
| 9 | `JOIN_AGG_SEND_CONF` has no identity/phase check | **Open** | §3.2 |
| 10 | Scalar CTE_SCAN feed retries eviction only once | Fixed | `e49a3cfd846` (+ `c1a9d9c9b88`, which rebuilds the feed row after an eviction) |
| 11 | FINAL_REP dropped when the park pool is exhausted | **Open** | §3.3 |
| 12 | Linked-buffer walks with uneven bounds checks | **Open** | §3.4 |
| 13 | Single-table pushed scan stops the node when `Init` fails | Fixed | `d9aac889f01` |
| 14 | Aggregation programs from the API not validated | Fixed | `8878794d1fd` |
| 15 | Multi-leaf string MIN/MAX side table indexed by the leaf-local slot | Fixed | `6c79ad0f0e4` |
| 16 | Null charset on string slots created by redistribution | Fixed | `9a0fffe7475` |

New error codes and inserts added by these fixes:
- error insert DBTUP 4044 (bug 13): fails the pushed interpreter's `Init` as
  if QueryMemory ran out;
- 1256 `ZJOIN_AGG_RESULT_TOO_LARGE`, now only for the 16-bit length limit;
- `kErrMixedStringCaseArms` in `NdbAggregator`.

Bug 14 adds `AggInterpreterBase::checkProgram`, which runs:
- in `DblqhProxy::execJOIN_AGG_SETUP_REQ` for every leaf, before the
  optimizer and the JIT;
- in `Init`.

Programs that fail it are refused:
- a join aggregation setup with `JOIN_AGG_SETUP_REF` 20002;
- a single-table scan with 1868.

Bug 14 also added a per-row DECIMAL size check, which returns 1863.

## 3. Open analysis bugs

These are "Reported" findings from the analysis: they have not been
re-verified against the current code. Re-read the code first.

### 3.1 Bug 8: one coordinator-liveness rule
- **Sites** (all in `DblqhMain.cpp`):
  - `checkJoinAggNodeFailed` (20462) uses `!m_connected || ZNODE_DOWN`;
  - `isJoinAggCoordinatorFailed` (20366) uses `ZNODE_DOWN` only. Its callers
    are at 20479, 20616, 22535 and 23662;
  - the LQHKEYREQ and `initScanrec` guards use `m_connected` only. The report
    gives their old lines as 10399-10405 and 28019-28025; search for
    `m_connected` near the join-aggregation flag checks.
- **Risk.** The NF-14 comment says `m_connected` can lag the node-failure
  protocol. A late feed can then enter a state that
  `JOIN_AGG_NODE_FAIL_REP` is reclaiming, which risks a use-after-free.
- **Fix.** One `joinAggCoordinatorDown(ref)` helper used at every site.
- **Test.** A PK-8 variant that parks a SCAN_FRAGREQ under 5150 and expects
  SCAN_FRAGREF 286. Error insert 5154 also has to bypass `m_connected`
  in these guards (report §2c row 1).

### 3.2 Bug 9: SEND_CONF identity and phase check
- **Sites:** `Dblqh::execJOIN_AGG_SEND_CONF` (DblqhMain.cpp:21332) and
  `continueJoinAggSend` (21147).
- **Bug.** The handler checks neither which query nor which phase the CONF
  belongs to, and it does `ndbrequire(state)` with no `isAborting()` check.
  A stale or duplicate SEND_CONF restarts the send chain on a reused or
  torn-down state.
- **Fix.** Add the same identity and phase check that REDISTRIBUTE_CONF got
  (ID-2). The identity test is
  `JoinAggregationState::packIdentWord(m_queryTag, m_cte_index, 0)` plus
  the transid, as in `execJOIN_AGG_FINAL_REP` at 25363.
- **Test.** An ID-9 case: a duplicate or stale SEND_CONF, and a
  COMPLETE_REQ with a non-RNIL key.

### 3.3 Bug 11: FINAL_REP dropped when the park pool is exhausted
- **Site:** `execJOIN_AGG_FINAL_REP` (DblqhMain.cpp:25304). When
  `joinAggResolveOrParkGeneric` returns neither PARKED nor RESOLVED (around
  25348), the handler logs "FINAL_REP park FAILED" and returns. The owner
  never counts that sender and sits in CTE_REDISTRIBUTING until DBTC times
  out.
- **What the other GSNs do.** COMPLETE and NULL_ROW REF with
  `ZJOIN_AGG_PARK_POOL_EXHAUSTED` (20139, 20713). REDISTRIBUTE_REQ is
  around 24729.
- **Fix.** FINAL_REP has no REF, so abort the state instead of
  dropping the signal: resolve the owner's state by identity when it
  exists, then call `abortCteRedistribution`, or mark the sender failed.
- **Test.** PK-4b: error insert 5148 with a small cap. Expect REF 1275, not
  a hang.

### 3.4 Bug 12: one checked linked-buffer walk
- **Sites.** There are 9 copies of the "seek linked position" walk:
  - `JoinAggInterpreter.cpp`: `readLinkedAttrIntoBuf` (795) and the GROUP BY
    walk;
  - `DbtupExecQuery.cpp` (two);
  - `DbtupJitGlue.cpp`;
  - `Dblqh::buildCteLinkedBuffer` (DblqhMain.cpp:21511), which never checks
    `subStart + subLen <= attrInfoLen`.
- **Bug.** The weakest copy reads `p[2]` and copies `1 + dataSize` words
  with no check against the end of the source or `g_attr_read_buf_len_`.
  The data comes from DBSPJ, so this is defence in depth.
- **Fix.** One checked `linkedEntryAH()` (the body of `nullRowLinkedEntry`,
  JoinAggInterpreter.cpp:894) and one `parseCteAttrInfo()`. Each caller still
  decides between a REF and a fallback. This also removes duplication §3a-4
  and §3b-12 of the report.

## 4. Gaps left by the fixes

### 4.1 RonSQL (the next session's area)
- **WHERE arithmetic on LEFT JOIN NULL-extended rows (wrong results).**
  Take a filter that compares columns of two tables above a LEFT-joined table,
  e.g. `WHERE b.x + c.y > 5`. On a NULL-extended row, `evalEmbeddedForNullRow`
  returns `EMB_NULL_ROW_UNSUPPORTED` for arithmetic, and `ProcessRec` then
  takes the THEN path (JoinAggInterpreter.cpp:1842). The row is counted even
  when the WHERE is false. Fixes:
  - teach the NULL-row evaluator the arithmetic opcodes;
  - or, for these shapes, have RonSQL emit a NULL guard, or refuse the shape.
- **`is_null_rejecting` and OR (wrong results).** In `RonSQLPreparer.cpp:3895`,
  OR counts as null-rejecting when both branches are comparisons, but not
  every branch has to reference the LEFT-joined table. For example,
  `c LEFT JOIN o ... WHERE o.amt > 5 OR c.fl = 30` is promoted to INNER and
  drops rows that MySQL keeps. Fix: each OR branch must reject NULL for a
  column of the RHS table. That needs the table identity, so it does not fit
  the current context-free function.
- **CASE MIN/MAX whose arms are all strings fails to print.** RonSQL refuses
  mixed arms (bug 1). All-string arms pass, but
  `ResultPrinter::aggregate_arg_charset` (ResultPrinter.cpp:2685) only follows
  a direct column Load, so printing fails with "Could not find charset for
  string aggregation result". Fix: take the charset of the arms, which
  `RepeatAgg` already requires to be one collation.
  - Related: `NdbAggregator::Finalize` sizes string results from the first
    arm only.
  - The `9a0fffe7475` printer fix covers chained CTE columns
    (`build_result_column_metadata`), not CASE.
- **The data node has no guard against bug 1c.** A program that mixes string
  and numeric MIN/MAX arms on one slot is refused by `NdbAggregator::RepeatAgg`
  and by mysqld, but not by the data node. Bug 14's `checkProgram` checks
  structure, not slot types. A raw NDB API client can still send one, and the
  THEN arm stores a string pointer that the ELSE arm's `MinBigint` overwrites,
  which can crash the node. Fix: in `checkProgram` (or the optimizer's
  per-slot type pass from bug 1), refuse a slot whose writers mix string and
  non-string. Add a testSpjMalformed case.

### 4.2 Kernel
- **Bug 13 leftovers:**
  - `ndbrequire(regTcPtr->opExec)` in `execSCAN_FRAGREQ` (DblqhMain.cpp:25882)
    still stops the node when SCAN_FRAGREQ has the aggregation flag but no
    interpreted program (API-controlled). Fix: refuse it with SCAN_FRAGREF.
  - The primary-key read path (`handleAggReadRow`) has no MTR test for 4044,
    because RonSQL retries temporary errors. A block or NDB API test is needed.
  - The next-range path needs no change: the interpreter exists by then.
- **Bug 16 leftover.** In the NULL-row evaluator,
  `evalEmbeddedForNullRow`'s inline-type compare (JoinAggInterpreter.cpp:1263)
  leaves `cs = nullptr` when `csNumber == 0`, even for a string type. Check
  whether a CHAR/VARCHAR compare can then reach `m_cmp` with a null charset,
  here and in DBTUP's `BRANCH_*_INLINE_TYPE` handlers. If it can, refuse
  string types with charset 0.
- **Single-table pushed aggregation results are not sent fragmented.**
  `Dbtup::SendAggResToAPI` (DbtupBuffer.cpp:542) is the single-table
  counterpart of `9346d8ecc48`. A result record over one transporter message
  fails there.
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
