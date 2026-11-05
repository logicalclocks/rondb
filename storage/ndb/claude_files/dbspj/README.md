# DBSPJ analysis (RONDB-1124)

An analysis of DBSPJ and of how DBTC and DBLQH affect it: three architecture
documents, the weaknesses found while writing them, and the tests added to
reveal those weaknesses. Everything was found by reading the code at
6598f4a65e3, except P-24, which a test run found. Findings that the first
test runs (2026-10-28) reproduced are marked **reproduced**.

## Documents

| File | Contents |
|------|----------|
| `dbspj_attrinfo_protocol.html` | Attribute-information protocol into DBSPJ: QueryTree and QueryParameters encoding (node specs, `NI_*`/`PI_*` bits, QueryPattern), parsing and limits, how DBSPJ builds KeyInfo/AttrInfo for DBLQH from parent rows, correlation ids, join-aggregation/CTE extensions, a worked 3-table example. Findings P-1..P-23. |
| `dbspj_state_machine.html` | Request/TreeNode/ScanFragHandle states and transitions, signal sequences with DBTC and DBLQH (lookup, scan, scan-scan, close, errors, node failure, time-out, join aggregation), ordering assumptions, resource lifecycle, the DBTC/DBLQH contract. Findings S-1..S-20. |
| `dbspj_query_execution.html` | Execution architecture, the query shapes DBSPJ can run and the pushability rules (with the "can't push" table), join aggregation across DBLQH/DBSPJ/DBTC, performance, test landscape. Findings E-1..E-30. |
| `tests_robustness.md` | Error-insert coverage matrix and the robustness findings R-1..R-11, with their tests. |
| `tests_query_shapes.md` | Coverage map of the pushed-join MTR tests and the correctness findings Q-1..Q-9, with their tests. |

Several findings were reported by more than one review under different IDs:
S-3 = R-2, S-19 = R-4/R-4b, S-15 = E-14, S-7 = E-27 = Q-4, E-5 = Q-3,
E-15 = Q-2, and E-19 is the P family.

## Most important findings

Findings marked **verified** were re-checked in the code. The others have the
confidence the reviewer gave.

### Data node stops, reachable by normal clients
- **S-1 (verified):** in `scanFrag_execSCAN_FRAGCONF`, the maxRows early close is sent after `m_frags_outstanding--` and is never counted. Its done=1 CONF then fails `ndbrequire(data.m_frags_outstanding)`. RonSQL reaches this through the CTE-body MIN/MAX optimization (`setMaxRows(1)`) on any fragment with more than one batch (990 rows). Test: `ronsql_cte/spj_state_s1_minmax_maxrows`.
- **R-4/R-4b = S-19 (reproduced):** an error part-way through the first batch of a child scan aborts while the node is still `TN_INACTIVE`. `scanFrag_abort` skips it, so started DBLQH scans are never closed, and with small fragments `ndbrequire(m_cnt_active)` fails. Insert 17535. `ndb_spj_robust_first_batch_abort` left a scan of index `ib` fragment 0 open; `ndb_spj_robust_first_batch_abort_small` stopped a data node at the `ndbrequire(requestPtr.p->m_cnt_active)` in `scanFrag_execSCAN_FRAGCONF`.
- **R-3 (reproduced):** a DIGETNODESREQ error in a CONTINUEB round is ignored, then `getNodeInfo(0)`. Insert 17539. `ndb_spj_robust_getnodes_continueb` stopped a data node at `ndbrequire(nodeId > 0 && nodeId < MAX_NODES)` in `SimulatedBlock::getNodeInfo`.
- **S-6:** a NULL-extended row from inside a CTE body always goes to the main aggregator; with no main aggregation an `ndbrequire` fails.
- **E-18:** a node included after DBTC's SETUP snapshot can be chosen during a long aggregate query, failing `ndbrequire(m_aggNodes.get(nodeId))`. No test (needs node-restart timing).
- **E-23:** the CTE redistribution receiver requires the group key to fit in 1023 words; the sender doesn't check. No test.

### Hangs and leaks
- **R-1 / R-1b:** in a pushed LEFT JOIN aggregation, a failed match-bitmask merge or null-row injection returns before the outstanding count is decremented, so the request hangs. Inserts 17536/17537. R-1b was reproduced (time-out 274); R-1 was not (the query failed promptly with 20008).
- **R-2 = S-3, R-5:** DIH scan registrations leak on two abort paths, and a later `REORGANIZE PARTITION` waits for ever.
- **S-2, S-4, P-19:** missing completion re-checks stall the request until DBTC's time-out (274), hiding the real error. Inserts 17541, 17540.
- **S-15 = E-14:** evictions from a lookup aggregate leaf produce a mid-fragment CONF: error 1270 with CTEs, a hang without.
- **S-17:** a close sent to a join-agg SCAN_FRAGREQ that DBLQH has parked is dropped.

### Wrong results
- **E-1 (verified):** `NdbAggregator::initForResults` decodes the data words of constants as opcodes. With a `1.0` constant, the high word `0x3FF00000` makes aggregate slot 0 merge as a double SUM across nodes.
- **Q-1 (verified):** `NdbReceiver` strips 3 words from any reassembled fragmented row of a query operation whose third-to-last word looks like a `CORR_FACTOR64` header.
- **E-2:** join-agg self-continuation doesn't reset a child scan's batch and correlation counters, so scan → scan → leaf with a large root silently skips child scans.
- **E-3, E-4, E-6, E-7, E-8, E-9:** outer-join aggregation cases: NULL rows lost across root batches, CASE on NULL rows taking THEN, filter-rejected lookup leaves, multi-leaf NULL rows, join nests, and the forced chain re-parenting siblings.
- **E-5 = Q-3:** GROUP BY values that are equal under the collation split into several groups (kernel compares lengths first; the API merges by bytes).
- **E-10, E-11, E-12:** in mysqld join-aggregation pushdown, an unpushable root condition is evaluated after aggregation, lookup filters of 64 words or more are dropped, and a range/MRR root stops after one row.
- **E-15 = Q-2:** a `<=>` ref child is pushed, and DBSPJ drops NULL keys.
- **S-5:** CTE_LOOKUP CONF/REF never call `handleTreeNodeComplete()`, so a sequenced sibling scan can be skipped.

### Malformed requests (crafted NDB API clients)
- **P-1, P-2, P-3, P-6, P-7, P-8, P-9, P-10, P-11, P-14 (reproduced), P-4, P-5:** unchecked lengths, indexes and `ndbrequire`/`ndbabort` on API-supplied words in `Dbspj::build`, `parseDA`, `expand`, `computeHash` and DBTC's `parseJoinAggKeyInfo`. P-1 is a ~16 GB `memmove`; P-3 is a stack overflow. The data-node security policy expects `reportMaliciousSignal()` here instead.
- **P-24 (reproduced):** `checkSignalSender` lets an `External` GSN through to any block instance, whether or not it has a handler. SCAN_TABREQ sent by an API to DBTC instance 0 (the DbtcProxy on ndbmtd) stopped the node with "Illegal signal received (GSN 32 not added)". Any API node can stop a data node this way. Not specific to DBSPJ; found because an early `testSpjMalformed` sent to plain `DBTC`.

### Other
- **E-27 = S-7 = Q-4 (verified):** `handleJoinAggNextBatch` writes a "TEMPORARY" `AGGT` info line to the cluster log for every self-continued batch.
- **E-30:** a query with HAVING is never pushed. **E-28:** after 256 groups, every group costs a JOIN_AGG_SEND round trip. **E-29:** RonSQL/CTE roots are never pruned.

## Tests added

| Tests | Suite | Targets |
|-------|-------|---------|
| `testSpjMalformed` (new NDB API program, raw signals) + `test_spj_malformed_<case>` (one MTR test per case) | `ndb_spj` | Positive control, P-1, P-2, P-3, P-6, P-7, P-8, P-9, P-10, P-11, P-14, P-24 |
| `ndb_spj_robust_*` (11 tests) | `ndb_spj` | R-1, R-1b, R-2, R-3, R-4, R-4b, R-5, R-6, S-2, P-19, node failure, all existing 17xxx inserts with leak checks |
| `testSpj` `SpjFaultLeakCheck`, `SpjNewFaultLeakCheck`, `NF_JoinLeakCheck` | ATR | Same as above through the NDB API |
| `ndb_spj_shapes_{nulls,keys,outer,fanout,wide,variants,multi_dml}` | `ndb_opt` | Q-1, Q-2, Q-7, Q-8, Q-9, Q-10, E-15, E-26, NULL/outer/fan-out/key-type shapes (pushdown ON vs OFF) |
| `ndb_spj_shapes_agg` | `ndb_push_agg_dist` | E-1..E-6, E-8..E-12, E-30, Q-3, Q-5, Q-6 (aggregation ON vs OFF) |
| `ndb_spj_shapes_agg_exact` (disabled) | `ndb_push_agg_dist` | Q-6a/b, the shapes of `ndb_spj_shapes_agg` that still fail. `ndb_spj_shapes_agg_outer` (E-3, E-6, E-8, E-9, split out on 2026-10-31) is gone: its shapes moved back on 2026-11-03 |
| `spj_state_*` (5 tests) | `ronsql_cte` | S-1, S-5, S-6, S-15 (with and without CTE) |
| `spj_ronsql_*` (12 tests, `spj_ronsql_q6_decimal_sum` disabled) | `ronsql_cte` | The open findings RonSQL reaches: E-3, E-4, E-5, E-6, E-7, E-8, E-17, E-23 (two tests), S-6b scan leaf, X-1; Q-6/E-16 |

Kernel support: error inserts 17535–17541 in `DbspjMain.cpp`, documented in
`ERROR_codes.txt`. Each does nothing unless set and clears itself when it
fires.

The differential tests (`ndb_spj_shapes_*`, `spj_state_*`) compare against
unpushed execution inside the test and die naming the finding. The robustness
tests also check that the query was really pushed, and check for leaks
afterwards (ndbinfo, DUMP 2650/2560/2361).

`SpjFaultLeakCheck`, `SpjNewFaultLeakCheck` and `testSpjMalformed` are expected
to fail until the findings they target are fixed, so they are not in any
`daily-*-tests.txt`. Only `NF_JoinLeakCheck` was added (daily-basic--16).

## First run of the `ndb_spj` suite (2026-10-28, debug build, ndbmtd)

| Test | Result | Finding |
|------|--------|---------|
| `ndb_spj_robust_first_batch_abort_small` | data node stopped at `ndbrequire(requestPtr.p->m_cnt_active)` in `scanFrag_execSCAN_FRAGCONF` | R-4b reproduced |
| `ndb_spj_robust_getnodes_continueb` | data node stopped at `ndbrequire(nodeId > 0 ...)` in `getNodeInfo` | R-3 reproduced |
| `ndb_spj_robust_first_batch_abort` | query failed with 20006, a scan of index `ib` fragment 0 stayed open | R-4 reproduced |
| `ndb_spj_robust_dih_scan_leak` | query failed with 20008, a DIH scan stayed registered | R-2 reproduced |
| `ndb_spj_robust_fault_matrix` | all other inserts and shapes passed; 17012 on shapes 2 and 3 leaked a DIH scan | R-2 reproduced |
| `ndb_spj_robust_dih_scan_ref` | 274 (time-out) instead of the DIH_SCAN_TAB_REF error | S-2 reproduced |
| `ndb_spj_robust_getnodes_errcode` | NDB error 1 instead of 311: DBSPJ reports `DiGetNodesConf::zero` (1) instead of `theData[1]` | R-6 reproduced |
| `ndb_spj_robust_joinagg_null_row` | lookup-leaf control failed with 20006 as expected; scan-leaf query failed with 274 | R-1b reproduced |
| `ndb_spj_robust_exec_plan_oom` | died with a P-19 message on the scan request | P-19 reproduced |
| `ndb_spj_robust_joinagg_match_oom` | passed: 17536 fired, the query failed promptly with 20008, nothing leaked | R-1 **not** reproduced |
| `ndb_spj_robust_nodefail` | passed | |
| `test_spj_malformed` | data node stopped in the positive control: harness bug (requests sent to DBTC instance 0), fixed | found P-24 |
| `test_spj_malformed` (second run, fixed harness) | positive control passed; P-1 stopped a data node with SIGSEGV in `Dbspj::build()` called from `execSCAN_FRAGREQ` | P-1 reproduced |

One MTR test per malformed case (2026-10-29). The positive control passed
in each; every crash is at the site the finding names:

| Test | Data node stopped at | Finding |
|------|----------------------|---------|
| `test_spj_malformed_p1` | SIGSEGV in `Dbspj::build()` | P-1 reproduced |
| `test_spj_malformed_p2` | `CArray::getPtr` assertion in `scanFrag_build` (SCAN_FRAGREQ) | P-2 reproduced |
| `test_spj_malformed_p3` | `ndbassert(ptr.sz <= MAX_KEY_SIZE_IN_WORDS)` in `computeHash` (debug build; a release build overflows `tmp32` on the stack) | P-3 reproduced |
| `test_spj_malformed_p6` | `ndbabort()` for a NULL fixed key in `parseDA` | P-6 reproduced |
| `test_spj_malformed_p10` | `ndbrequire(nodeId < MAX_NDB_NODES)` reading aggKeys in `execSCAN_FRAGREQ` | P-10 reproduced |
| `test_spj_malformed_p11` | `ndbrequire(handle.getSection(attrPtr, ...))` in `execLQHKEYREQ` | P-11 reproduced |
| `test_spj_malformed_p14` | `ndbrequire(reader.getWordsPtr(...))` in DBTC `parseJoinAggKeyInfo`: the wrapped length check passed | P-14 reproduced |
| `test_spj_malformed_p24` | "Illegal signal received (GSN 32 not added), block: 245, instance: 0" | P-24 reproduced |
| `test_spj_malformed_p7` | `ndbrequire(m_treenode_pool.getPtr(...))` walking the P_PARENT depth past the root in `build` | P-7 reproduced |
| `test_spj_malformed_p8` | `ArrayPool::getPtr` assertion in `lookup_parent_row` on the first parent row (no key section) | P-8 reproduced |
| `test_spj_malformed_p9` | passed: `P_COL(300)` only reads garbage inside the `execTRANSID_AI` stack buffer, which the test cannot see. Changed to `P_UNQ_PK(0)` on the INT parent column, which fails `ndbrequire(len > 1)` in `appendPkColToSection`; rerun stopped a data node there | P-9 reproduced |

An earlier run of the fault matrix died with "didn't return a result set",
a test bug that five other tests shared; all six now run the query through
`suite/ndb_spj/include/spj_run_query.inc`. The single `test_spj_malformed`
test stopped at the first crash (P-1), so the cases are now one MTR test
each (`test_spj_malformed_p1` .. `_p24`).

RonSQL CTE tests (`ronsql_cte`, 2026-10-29). All five failed on the first
attempt and on the retry with the same symptom. S-1, S-5 and S-6 need no
error insert: each is an ordinary RonSQL query. The S-15 tests force group
evictions with DBTUP 4041; without it DBTUP evicts when a group map is
full (`AGG_EVICT_NEEDED`), so a query with enough groups hits S-15 too.

| Test | Result | Finding |
|------|--------|---------|
| `spj_state_s1_minmax_maxrows` | **both** data nodes stopped. Node 1: `Failed spjCheck (data.m_frags_outstanding == (frags_outstanding_scan + frags_outstanding_close))` with `frags_outstanding 0` in two DBSPJ instances, then `ndbabort` in `checkRequest`. Node 2: `ndbassert` in `scanFrag_send_NEXTREQ` (a fragment still waiting for its close). Both are debug checks; a release build is expected to stop at `ndbrequire(data.m_frags_outstanding)` instead (not verified) | S-1 reproduced |
| `spj_state_s5_cte_lookup_successor` | RonSQL returned 15 of 100 rows (customers 1, 13, 17) on the first attempt and 10 (customers 1, 13) on the retry, no error: how many rows are lost depends on timing | S-5 reproduced |
| `spj_state_s6_cte_body_null_row` | COUNT(\*) 5, SUM(cnt) 3 instead of 2, 6: the three NULL-extended rows went to the main aggregation instead of `cte1` | S-6 reproduced (wrong result, no stop) |
| `spj_state_s15_eviction_cte` | control (s15c-1) matched; with DBTUP 4041 forcing evictions (s15c-2) the query failed with 1270 "CTE join aggregation scan received an unexpected intermediate batch boundary", also on retry | S-15 (CTE form) reproduced |
| `spj_state_s15_eviction_join` | control (s15j-1) matched; with DBTUP 4041 (s15j-2) the RonSQL request hung until curl's 120 s limit (exit 28), also on retry | S-15 (join form) reproduced |

Pushed-join shape tests (`ndb_opt`, 2026-10-29). **The first run is not
evidence for or against the result findings:** every result comparison
compared two empty lists. `ndb_spj_shapes_check.inc` exported the result
files as `SPJ_OFF_FILE`/`SPJ_ON_FILE`, but mysqltest looks variables up
without regard to case (`charset_info`, `mysqltest.cc:9541`), so
`let SPJ_OFF_FILE=` updated `$spj_off_file` and exported it as
`spj_off_file` (all 237 Perl checks printed `rows=0`). Fixed: the
variables are now `SPJ_CMP_*`, and the Perl block dies if one is missing.
The EXPLAIN, error and crash checks were real:

| Test | Result | Finding |
|------|--------|---------|
| `ndb_spj_shapes_wide` | mysqld stopped at the first check (W1, pushed lookup query, wide child row): `Assertion failed: (aLength >= len), function unpackRow, NdbReceiver.cpp:1226`, from `NdbQueryOperation::firstResult` | Q-1 reproduced (debug assertion; a release build unpacks a row three words short) |
| `ndb_spj_shapes_variants` | mysqld stopped at D3, a multi-table UPDATE whose pushed join has the scan child `dc` on the ordered index `ix_b`: `Assertion failed: (use_cursor \|\| idx == MAX_KEY \|\| ...HA_NOSAME), function get_read_set, ha_ndbcluster.cc:4224`, from `create_pushed_join` | new **Q-10** |
| `ndb_spj_shapes_nulls` | N18 (`<=>` ref in a LEFT JOIN) is not pushed; the test required it. N17 (inner `<=>`) is pushed | test expectation, fixed (N18 only checks the result) |
| `ndb_spj_shapes_keys`, `_outer`, `_fanout` | ran to the end (vacuous comparisons) | rerun needed |

**Q-10:** `ha_ndbcluster::create_pushed_join` calls
`get_read_set(false, active_index)` for every member of the pushed join
(`ha_ndbcluster.cc:15962`). In a multi-table UPDATE or DELETE every table
of the statement counts as updated, and `get_read_set` asserts that a
non-cursor read uses the primary key or a unique index. A pushed scan child
on an ordered index is a cursor read, so a debug mysqld stops. In a release
build the code after the assertion only adds the PK columns to the read set
of a table that is only read, so the effect there is an extra read, not a
wrong result. Test: `ndb_spj_shapes_multi_dml` (D3).

Test changes after this run: the multi-table UPDATE/DELETE moved from
`ndb_spj_shapes_variants` to `ndb_spj_shapes_multi_dml` (DELETE first), and
the Q-1 lookup shapes (W1-W4) of `ndb_spj_shapes_wide` run last, so that a
mysqld crash no longer hides the shapes after it.

Join aggregation shapes (`ndb_push_agg_dist`, `ndb_spj_shapes_agg`,
2026-10-29, first run with working comparisons). Reference: plain MySQL
execution; checked run: `ndb_join_pushdown`, `ndb_join_pushdown_aggregate`
and `_outer_join` ON (all three aggregation options default to OFF in
mysqld; RonSQL always pushes its aggregation). The baseline G0 and the
controls matched, so the comparison itself works.

| Shapes | With pushdown | Finding |
|--------|---------------|---------|
| E-1a, E-1f | `MAX(f.qty)` in slot 0 merged as a SUM across the 2 nodes: 98 instead of 49 | E-1 reproduced |
| E-1c | `SUM(f.qty)` in slot 0 (constant `2`, high word 0x40000000) merged as a MAX: the larger node's partial sum | E-1 reproduced |
| E-5d | GROUP BY an ai_ci leaf column: groups split and counts wrong | E-5/Q-3 reproduced (root-column variants E-5a..c matched) |
| Q-7a, Q-7b | GROUP BY DOUBLE with -0.0 and 0.0: COUNT 2333 instead of 3332 for one group | Q-7 reproduced |
| N4, N6 | scalar aggregate over an empty input (root filter rejects every row): **no row** instead of one row (COUNT 0, NULLs) | new **Q-11** |
| Q-6a, Q-6b | SUM of BIGINT beyond 64 bits: error 1860 "arithmetic operation results overflow" instead of the DECIMAL sum | Q-6 reproduced |
| Q-6c | SUM of DECIMAL(38,6): rounded to about 16 digits (computed as a double) | Q-6 reproduced |
| E-4a, E-4b | `SUM(CASE WHEN outer.v > 5 ...)` counts NULL-extended rows (41 instead of 0) | E-4 reproduced |
| E-6a | LEFT JOIN lookup leaf with a pushed ON filter: rows the filter rejects lose their NULL-extended row (88 instead of 100) | E-6 reproduced (scan form E-6b matched) |
| E-3a, E-3b, E-3c | LEFT-LEFT lookup chain: NULL-extended rows lost, worst with a large root (25 instead of 2000) | E-3 reproduced |
| E-9a | two outer-joined lookup siblings | E-9 reproduced (E-9b matched) |
| E-8a, E-8b | inner nest inside an outer nest; inner join after an outer join: groups lost or added | E-8 reproduced |
| E-11b, E-11c | IN-list of 20 / 40 values on a lookup leaf: the filter is dropped (59 rows instead of 12) | E-11 reproduced (5 values, E-11a, matched) |
| E-12a, E-12b, E-12c | range or IN-list root: **no rows** | E-12 reproduced (predicted: one row) |
| E-10a | root condition that cannot be pushed (`d.k + 0 > 5000`): **no rows** | E-10 reproduced |
| E-10b | same condition with single-table aggregation pushdown: condition ignored (100 instead of 52) | E-10 reproduced (single table) |
| E-15 | `<=>` ref child, **join pushed, aggregation not** (`ndb_join_pushdown` is ON by default): wrong counts | E-15/Q-2 reproduced |
| E-2 | both data nodes stopped at `ndbassert(bs_rows <= bs_bytes)` in `scanFrag_send`: the child scan's `m_totalBytes`/`m_totalRows` are not reset by self-continuation, so `batch_size_bytes - m_totalBytes` shrinks (and can underflow in a release build) | E-2 reproduced |
| E-1b, E-1d, E-1e, M1, M2, N1..N3, N5, Q-6d, E-6b, E-9b, Q-5, E-11a, E-30 | matched | |
| X1 | COUNT(DISTINCT): the join is not pushed, the test required it | test expectation, fixed |

**Q-11:** a scalar join aggregation (no GROUP BY) whose pushed root filter
rejects every row returns no row; MySQL returns one row (COUNT 0, other
aggregates NULL). `dbspj_query_execution.html` describes the API
synthesizing that row, so the gap is in that path. Tests: N4 (inner join),
N6 (LEFT JOIN) in `ndb_spj_shapes_agg`.

Test changes after this run: X1 no longer requires a pushed join, E-15 no
longer requires pushed aggregation (it checks the pushed join).

## Fixes (2026-10-29, not yet built or run)

Three agents worked on files nobody else touched: DBTC and signal
dispatch, the NDB API, and mysqld. DBSPJ was fixed directly.

New violation types, all Tier B (log only), in `ViolationType.hpp`:
`VT_SIGNAL_NO_HANDLER` (35), `VT_SPJ_MALFORMED_REQUEST` (36),
`VT_JOINAGG_KEYINFO_MALFORMED` (37); `VT_UNKNOWN` moves to 38. The catalog
grows to 39 rows, so `ndb/r/ndbinfo_plans.result` now says 78 rows for
`ndb$security_violations` (2 data nodes).

| Finding | Fix | Where |
|---------|-----|-------|
| S-1 | The maxRows close counts in `m_frags_outstanding`; its done=1 CONF completes the fragment normally (not as an abort); `scanFrag_abort` counts a fragment waiting for it as outstanding | `scanFrag_execSCAN_FRAGCONF`, `scanFrag_abort` |
| P-1 | v1 parameter block shorter than its header rejected before the memmove | `build` |
| P-2, P-18 | `checkTableError` before the table id indexes the key descriptors (scan and lookup nodes) | `scanFrag_build`, `lookup_build` |
| P-3 | Key longer than `MAX_KEY_SIZE_IN_WORDS` returns an error | `computeHash`, `computePartitionHash` |
| P-4 | Parameter values must end inside the parameter block (`buildParamRowHeader`); every column index is checked against the row | `expand`, `append*ToSection`, `append*ToPattern` |
| P-5 | P_DATA length and the prune header checked against the node | `expand`, `parseScanFrag` |
| P-6 | NULL or empty constant key/prune key is an error, not `ndbabort` | `parseDA`, `parseScanFrag` |
| P-7 | P_PARENT depth and the token after it checked at build | `expand` (pattern store) |
| P-8 | Child lookup needs a linked or fixed key, child scan a linked key; empty linked patterns rejected | `lookup_build`, `scanFrag_build`, `parseDA`, `parseScanFrag` |
| P-9 | P_UNQ_PK on a non-NDB$PK column returns an error | `appendPkColToSection` |
| P-10 | DBSPJ: request without the MultiFrag/aggKeys section is refused; aggKeys parsing returns errors. DBTC: JoinAgg needs ViaSPJ, three sections, KeyInfo and a receiver id | `execSCAN_FRAGREQ`; `Dbtc::execSCAN_TABREQ` |
| P-11 | Lookup without AttrInfo refused | `execLQHKEYREQ` |
| P-12 | A key built by `parseDA` is released before the root KeyInfo replaces it | `execLQHKEYREQ`, `execSCAN_FRAGREQ` |
| P-13 | CTE subtree requires the CTE keys block | `cte_subtree_build` |
| P-14 | All lengths checked against the words left; every error path releases the section and the scan record | `Dbtc::parseJoinAggKeyInfo` |
| P-15 | `numResultCols` checked before doubling; NULL start signal handled; CTE scan with a parent rejected; CTE column index checked | `cte_scan_build`, `appendCteAttrinfoWithVirtMeta` |
| P-16 | FLUSH_AI in an API read list or linked list rejected (list walked as DBTUP reads it) | `parseDA` (`readListHasFlushAI`) |
| P-17 | Row header bounded by the stack array | `buildRowHeader`, `execTRANSID_AI` |
| P-19 | `buildExecPlan` error returned | `build` |
| P-20 | Peek results and the tree length checked | `build`, both entry points |
| P-23 | Scan node in a lookup request rejected | `scanFrag_build` |
| P-24 | Signal from an API/MGM node to a block instance without a handler (or GSN > MAX_GSN) is dropped and reported; data-node and local senders still stop the node | `SimulatedBlock::handle_execute_error(gsn, signal)` |
| R-1, R-1b | An error while handling a SCAN_FRAGCONF is aborted after the CONF's counters are updated | `scanFrag_execSCAN_FRAGCONF` |
| R-2 | `scanFrag_complete` reports to DBDIH whenever DBDIH registered the scan (`m_dihScanRegistered`) | `execDIH_SCAN_TAB_CONF`, `scanFrag_complete` |
| R-3 | CONTINUEB DIGETNODESREQ error aborts the request | `execCONTINUEB` case 3 |
| R-4, R-4b | Inner `scanFrag_send` returns its error; callers abort after the node is TN_ACTIVE and counted; no new fragments for an aborted request | `scanFrag_send` (both), `scanFrag_execSCAN_NEXTREQ`, `scanFrag_execSCAN_FRAGCONF` |
| R-5 | Abort fix-up in `batchComplete` runs the complete phase | `batchComplete` |
| R-6 | Error code read from `theData[1]` | `scanFrag_sendDihGetNodesReq` |
| R-8 | `checkBatchComplete` after the aggregation-ancestor error | `execTRANSID_AI` |
| S-2 | `checkPrepareComplete` after DIH_SCAN_TAB_REF; error `NoSuchTable` instead of DIH's own code | `execDIH_SCAN_TAB_REF` |
| S-4 | Self-continuation that sent nothing fails with InternalError instead of waiting for 274 | `handleJoinAggNextBatch` |
| S-5 | CTE_LOOKUP CONF/REF that completes the node calls `handleTreeNodeComplete` | `execCTE_LOOKUP_CONF/REF` |
| S-6 | NULL row from a CTE body goes to that CTE's aggregation (key and identity word with the cteId) | `sendJoinAggNullRow` |
| S-15 | Evicted groups of a lookup aggregate leaf no longer count as batch rows (DBLQH counts them in JOIN_AGG_COMPLETE_CONF) | `lookup_execLQHKEYCONF` |
| S-17 | CONF(done=0) for a closing fragment resends the close (DBLQH ignores a duplicate close) | `scanFrag_execSCAN_FRAGCONF` |
| E-2 | Self-continuation resets the child scans' batch counters (`cleanupBatch(done=true)`) | `handleJoinAggNextBatch` |
| E-27 | DBSPJ "AGGT" info line removed (DBTC's are compiled out) | `handleJoinAggNextBatch` |
| E-4 | Partly: the CASE condition is evaluated for NULL-extended rows when it reads only the outer table's columns; programs that read parent columns keep the old behaviour | `JoinAggInterpreter::ProcessRec` |
| Q-1 | Correlation trailer stripped when the query is a scan query, not by looking at the data | `NdbReceiver::execTRANSID_AI` |
| E-1 | Shared aggregation-program decoder; `initForResults` and the multi-leaf combiner step over operand words | `NdbAggregationCommon.hpp`, `NdbAggregator`, `NdbQueryOperation.cpp` |
| Q-7 | FLOAT/DOUBLE group keys compare numerically (-0.0 = 0.0) | `NdbAggregator::setupGroupByCompare` |
| E-5 | Pushed-join results merge string keys with the collation (as `Finalize` already did) | `NdbAggregator::initForResults` |
| Q-10 | `get_read_set(!isLookup, ...)` per pushed member | `create_pushed_join` |
| Q-8 | Unordered scan without aggregation: bundle the root fragments per node only when they divide evenly over the nodes, otherwise one fragment per worker (as the aggregation path does). Added 2026-10-30, missed in the first round | `NdbQueryImpl::prepareSend` |
| E-26 | `is_pushable_as_child` counts the key parts that refer to a table before the root (they become `paramValue()`s in `build_key()`) and does not push a child that would take the total over `MAX_REFERRED_FIELDS` (16); it was a statement error before. Added 2026-10-30 | `is_param_refs_pushable` (`ha_ndbcluster_push_agg.cc`), `m_param_refs` |
| E-15 | A REF child on a nullable non-null-rejecting `<=>` key part is not pushed | `is_null_safe_key_pushable` (`ha_ndbcluster_push_agg.cc`) |
| E-10 | Root conditions prepared before the aggregation decision; aggregation pushed only when every table condition is fully pushed (join and single table) | `ndbcluster_push_to_engine`, `ndb_push_aggregation_to_engine` |
| E-11 | Aggregation not pushed when a lookup filter is 64 words or more; runtime guard | same |
| E-12 | Single-range root goes through the default MRR; multi-range roots are not pushed with join aggregation | `multi_range_read_init`, `ndb_aggregate_reads_ranges_separately` |
| E-8, E-9 | Added 2026-11-03. Join aggregation is not pushed for a table inner joined within an outer join nest (E-8a), nor when the forced chain would put a table after an outer-joined table that is not its parent (E-9a, E-8b); the join stays pushed | `ndb_push_aggregation` |
| Q-6 | SUM/AVG of DECIMAL with scale > 0, wide MIN/MAX and DECIMAL expressions are not pushed. SUM(BIGINT) overflow is still pushed and fails with 1860, on purpose | `is_exact_aggregate_arg` |
| E-20 | A `computeHash`/`getNodes` error for a CTE query's main-root lookup aborts the request instead of failing an `ndbrequire` (made with the P-18 change; noticed 2026-10-31) | `lookup_start` |

DBTC also: `initScanrec` rejects ordered `scanParallel > numReceiverIds`.
`assembleFragmentsSlow` and `handle_sender_error` could stop the node on a
fragmented malformed signal (reporting with sections or fragment info still
set; a double release; a dropped train left in the hash); fixed in
`SimulatedBlock.cpp`. `fragmented_signal_security.md` does not describe this
yet.

### Not fixed

Later rounds fixed E-3, E-6, E-7, the kernel part of E-5, E-17, E-23 and
the S-6b scan sibling (kernel), and the RonSQL side of E-4 and E-8: see
"Next round: RonSQL", steps 4 and later.

- **E-3, E-6, E-7, E-8, E-9** (outer-join aggregation): need design work.
  E-3, E-6 and E-7 were fixed later; E-8 and E-9 are no longer pushed
  (2026-11-03, see step 5 of the RonSQL round), pushing them needs design
  work. The join-nest structure is not sent to the kernel (E-8), the forced
  chain re-parents siblings (E-9), buffered root rows are released before deferred
  NULL injection (E-3), a filter-rejected outer lookup leaf gives a TUPKEYREF
  that DBLQH cannot turn into a NULL row in place (E-6), and NULL rows carry no
  leaf index (E-7). mysqld only pushes these with
  `ndb_join_pushdown_aggregate_outer_join` ON (default OFF).
- **E-4** for conditions on parent columns: fixed 2026-11-02 in
  `evalEmbeddedForNullRow` (see the RonSQL round below); RonSQL's
  `NULL_CMP_EQUAL` was fixed in the RonSQL round. mysqld's lowering is not
  checked.
- **E-5** in the kernel: CTE and CTE_LOOKUP consumers never reach the API
  merge, so `findInBucket` (length first) still splits their groups.
- **Q-6a/b, E-16**: exact integer/DECIMAL accumulation in the kernel and API.
- **Q-11**: the API does synthesize the row; the cause is probably the E-12
  MRR path, so the E-12 fix may cover it. Check N4/N6.
- **R-7** (SIGNAL_DROPPED_REP), **P-21**, **P-22**.
- New, from the fixing work:
  - TRANSID_AI is not classified in `SignalScopes.hpp`, so an API can send it to DBSPJ, where `getGuardedPtr` stops the node.
  - An aggregation-program branch with offset 0 is accepted by `validateEmbeddedProgram` and loops an LDM thread.
  - `mt.cpp` `block2ThreadId` only asserts on an unknown receiver block.
  - `linkSegments` "Bad head segment size" for misaligned fragments.
  - SCAN_TABREQ checks that `apiConnectPtr` is valid, not that the sender owns it.
  - CTE `depMask` bits are not checked (time-out, no stop).
  - `close_scan()` clears `m_agg_join`, so a re-executed pushed join aggregation loses aggregate mode.
  - `store_group_column` has no DECIMAL case (unverified).
  - Suspected, sibling of S-6b: for an outer-joined *scan* aggregate leaf, `validateAggregateFlags` still sets match tracking (`T_BUFFER_MATCH`) on the scan ancestor only under `RT_AGGREGATE`. In a CTE body under a main query that does not aggregate, unmatched parent rows would then get no NULL row in the CTE's aggregation (a wrong result, not a stop). No test covers it yet. Moving that loop also moves `RT_AGG_ANCESTOR_MATCH` and the batch-size cap, so it was left as is.

### Effects on existing tests

- `ndb_spj_shapes_nulls` N17/N19 and `ndb_spj_shapes_agg` E-15, E-11b/c,
  E-12b/c, Q-6c expect "not pushed" now. E-1a/c/f (SUM over DECIMAL
  expressions) are no longer pushed through mysqld, so only E-1b/d test E-1
  there; REAL variants would restore the coverage.
- `ndb_push_agg_jit`: `rondb_jit_decimal_canary` q2/q4,
  `rondb_jit_double_canary` q7, `rondb_jit_linked_canary` q5 and
  `rondb_jit_mixed_canary` q2 are no longer pushed (DECIMAL scale > 0).
- GROUP BY a string (join aggregation) or FLOAT/DOUBLE now returns groups in
  collation/numeric order: recorded results without ORDER BY or
  `--sorted_result` may differ in row order.
- EXPLAIN output changes where a recorded plan pushed a refused shape.
- `NdbAggregator` has a new member: rebuild everything linking ndbclient.

### What to run, in this order

```
# unit test (build target NdbAggregatorMerge-t)
ctest -R NdbAggregatorMerge-t --output-on-failure
# from mysql-test/
./mtr --suite=ndb_spj --force --do-test=test_spj_malformed_
./mtr --suite=ndb_spj --force --do-test=ndb_spj_robust_
./mtr --suite=ronsql_cte --force spj_state_s1_minmax_maxrows spj_state_s5_cte_lookup_successor \
      spj_state_s6_cte_body_null_row spj_state_s15_eviction_cte spj_state_s15_eviction_join
./mtr --suite=ndb_opt --force --do-test=ndb_spj_shapes_
./mtr --suite=ndb_push_agg_dist ndb_spj_shapes_agg
# regressions
./mtr --suite=ndb --force ndb_security ndb_security_enforce ndb_security_scope ndbinfo_plans \
      ndb_index_unique ndb_multi_update_delete ndb_read_multi_range ndb_partition_hash_fanout
./mtr --suite=ndb_opt --force
./mtr --suite=ndb_push_agg,ndb_push_agg_dist,ndb_push_agg_jit --force
./mtr --suite=ronsql,ronsql_cte --force
./mtr --suite=ndb_spj --force
```

ATR: `testSpj -n MixedJoin T3` (fragmented scan-query rows, Q-1) and
`testSpj -n SpjFaultLeakCheck`, `-n SpjNewFaultLeakCheck`.

### Results with the fixes

| Test | Result | Cause and fix |
|------|--------|---------------|
| `test_spj_malformed_p2` | Data node stops: `DLHashTable::remove` assertion in `scanFrag_cleanup` from `cleanup` (SCAN_FRAGREQ) | Caused by the P-2 fix. `checkTableError` failed before `parseScanFrag`, which is where `ScanFragData` was constructed, so cleanup walked an uninitialized fragment list. `scanFrag_build` now constructs `ScanFragData` right after `createNode`. The other node types are not affected: their cleanup only calls `cleanup_common`. Not yet rerun |
| `ndb_spj_robust_dih_scan_leak` | Output as intended; fails only because the `.result` file is missing. 17012 fails the query with 20008 and no DIH scan stays registered | R-2 fixed. Record with `--record` |
| `ndb_spj_robust_fault_matrix` | Every run failed as expected or returned the reference result; nothing leaked | R-2 fixed on all shapes |
| `ndb_spj_robust_first_batch_abort` | 20006, no fragment scan left open, no DIH scan left | R-4 fixed |
| `ndb_spj_robust_first_batch_abort_small` | 20006, all data nodes still started, nothing left | R-4b fixed |
| `ndb_spj_robust_getnodes_continueb` | 311, all data nodes still started | R-3 fixed |
| `ndb_spj_robust_getnodes_errcode` | 311 (was 1) | R-6 fixed |
| `ndb_spj_robust_dih_scan_ref` | Fails promptly with an error that is not a time-out (was 274) | S-2 fixed |
| `ndb_spj_robust_exec_plan_oom` | 20008 on both the scan and the lookup request, then the reference results | P-19 fixed |
| `ndb_spj_robust_joinagg_null_row` | Both queries return the reference results afterwards, nothing left (the scan-leaf query timed out with 274 before) | R-1b fixed |
| `ndb_spj_robust_joinagg_match_oom` | 20008, nothing left (passed before the fix too) | R-1 |
| `ndb_spj_robust_nodefail` | The pushed scan join ends after node 2 is killed, node 2 restarts, nothing left, reference result afterwards | Passes with the fixes (passed before them too) |
| `spj_state_s1_minmax_maxrows` | All three queries match; DUMP 2650 finds no leak. Fails only on the missing `.result` file | S-1 fixed |
| `spj_state_s5_cte_lookup_successor` | All three queries match, also with 5131 holding the CTE lookups. Missing `.result` file only | S-5 fixed |
| `spj_state_s15_eviction_join`, `spj_state_s15_eviction_cte` | Results match with forced evictions (4041). Missing `.result` file only | S-15 fixed |
| `spj_state_s6_cte_body_null_row` | s6-1 (main query aggregates) now right: 2, 6, 150. s6-2 (main query only projects) stopped both data nodes at `ndbassert(parentPtr.p->m_bits & T_BUFFER_ROW)` in `execCTE_LOOKUP_REF` | S-6 fixed for s6-1. The s6-2 stop is a second bug (S-6b): `validateAggregateFlags` set row buffering on an outer-joined CTE_LOOKUP leaf's parent only under `RT_AGGREGATE`, which only main-query aggregation sets. That loop now runs for every request. Rerun (2026-10-30): s6-2 returns (1, 3, 80), (2, 3, 70), as MySQL does; S-6b fixed |
| `ndb_spj_shapes_variants` (`ndb_opt`, 2026-10-30) | All shapes before Q-8 matched. mysqld stopped at Q-8: `assert((rootFragments % cnt) == 0)` in `prepareSend`, `NdbQueryOperation.cpp:3840` | Q-8 reproduced. It was missing from the first fixing round; fixed now (see the fix table). Rerun and recorded (2026-10-30): Q-8a-c are pushed and match; Q-8d is result-only by design (`$spj_min_pushed= 0`) |
| `ndb_spj_shapes_multi_dml` | D4 (DELETE) matched. D3 (UPDATE) now runs pushed without the Q-10 stop, but 343 of 400 rows differed | Test bug: a multi-table UPDATE changes each row once, with the first matching joined row; `SET v = v + dc.v` depended on the order of the 4 matching `dc` rows, which the pushed join changes. Now `SET v = v + dc.b + 1` (`dc.b` = `dp.b` for every match) and `dc.v > 10` (80 rows unmatched). Rerun and recorded (2026-10-30): D3 and D4 match; D4 leaves 243 rows, as the data gives |
| `ndb_spj_shapes_variants`, recorded result | `Warning 1296 ... error 4500 'Comment contains non-supported fragment count type'` before the pbal24 shapes | Test bug: `FOR_RP_BY_LDM_X_3` does not exist (RP has X_2, 4, 6, 8, 16), so the table got the default 8 fragments and pbal24 repeated the plain variant. Now pbal16 (`FOR_RP_BY_LDM_X_2`), and `ndb_spj_shapes_variant.inc` takes `$vt_frags` and stops if `ndbinfo.operations_per_fragment` shows another fragment count (set for pbal16 and pbal1). Rerun (2026-10-30): the fragment checks passed (16 and 1), the warning is gone, all pbal16 shapes match; only the labels differ from the old result, so re-record |
| `ndb_spj_shapes_variants`, E-26 | Both E-26 shapes, the control with 16 references included, showed `pushed=0`: neither built a pushed join, so `build_key()` and MAX_REFERRED_FIELDS were not reached. The check was result-only, so it passed | Test gap, now fixed. Cause (from `ha_query_plan.cc`): `drv18 STRAIGHT_JOIN r26` has no join condition, so `r26` is the build side of a hash join, which gets its own "hash-bucket" scope built before `drv18`; `drv18` is then not in the root's scope and the references to it cannot be pushed. Both queries now carry `/*+ NO_BNL(r26) */` and require the push: the control 3 tables, E-26 2 (`r26`, `c26a`; `c26b` would take the references to 18). Without the E-26 code fix the E-26 query fails. Recorded (2026-10-30): E-26 `rows=30 pushed=2 exec=1`, control `rows=30 pushed=3 exec=1`; E-26 fixed |
| `ndb_spj_shapes_agg` (`ndb_push_agg_dist`, 2026-10-30) | 11 of 56 shapes fail, all known: Q-6a/b (error 1860 on SUM(BIGINT) overflow, kept on purpose), E-6a and E-2c-p1/p2 (E-6: a LEFT JOIN lookup leaf whose ON filter rejects the row loses the NULL-extended row; in E-2c the whole NULL group of 73980 rows), E-3a/b/c, E-9a, E-8a/b (outer-join aggregation, same numbers as the first run) | Now pass: E-1 (a, c, f no longer pushed), E-5d, Q-7a/b, N4/N6 (Q-11, fixed by the E-12 MRR change), Q-6c (not pushed), E-4a/b, E-10a/b, E-11b/c (not pushed), E-12a/b/c, E-15 (not pushed), E-2a/b in both passes (E-2 fixed; the first run stopped the data nodes there). E-2c is E-6, not E-2 |
| All 11 `ndb_spj_robust_*` | Besides the missing `.result` files, `check-testcase` failed whichever test first created `$MYSQLTEST_VARDIR/tmp/ndb_testrun.log` (`$NDB_TOOLS_OUTPUT`); after a restart, a different test each time | Test bug. Each test now sends its `ndb_mgm`/`ndb_waiter` output to `$MYSQLTEST_VARDIR/tmp/<test>_mgm.log` and removes it at the end, as `ronsql_cte_jit_census` does |
| `ndb_spj_shapes_agg`, rerun (2026-10-31) | The same 11 shapes failed with the same numbers | Split out so that the test can be recorded: the outer-join shapes went to `ndb_spj_shapes_agg_outer` (E-3a-c, E-6a, E-8a/b, E-9a, and E-2c, renamed E-6c because it is the E-6 shape over the large root), Q-6a/b to `ndb_spj_shapes_agg_exact`. Both are disabled in `collections/disabled_ndb.def` (ndb suites cannot go in `disabled.def`). The controls (E-6b, E-9b, Q-5, Q-6c/d) stay. The tables moved to `suite/ndb_push_agg_dist/include/spj_shapes_agg_*.inc`. Record `ndb_spj_shapes_agg` |

Recorded (2026-10-30): the 11 `ndb_spj_robust_*` and the 5 `spj_state_*`
`.result` files. Each one prints the outcome the test checks for (error
code, nothing left behind, reference result afterwards); nothing printed
depends on timing. The nine single-insert robust tests stop if their error
insert did not fire or the query did not fail with the expected error.
`fault_matrix` also accepts a run in which an insert did not fire, if
the result is right. `nodefail` checks that the query was still running
when node 2 was killed. Each
`spj_state` test checks its plan with `grep -qF` on the EXPLAIN output,
and orders the MySQL reference rows with `--sorted_result`. The
`test_spj_malformed_*.result` files were written by hand for the passing
outcome (positive control and case both "Test program success!"). They
have not been rerun since the fixes.

## Next round: RonSQL (plan, 2026-10-31)

No RonSQL test fails with the fixes, but few RonSQL tests aim at the open
findings. Reading `RonSQLPreparer.cpp` (not yet confirmed by a test run),
RonSQL reaches these:

| Finding | How RonSQL reaches it | Fix in |
|---------|-----------------------|--------|
| E-3 | LEFT-LEFT lookup chain (`QueryPlanner.cpp` parents each table on the table its ON refers to); existing tests use roots of 5 and 30 rows | Kernel |
| E-6 | Only through a SELECT-list subquery correlated on the inner table's PK: its filter goes on the LEFT lookup leaf. Main-query ON conditions are equality-only | Kernel |
| E-7 | Multi-leaf SELECT-list subqueries; only NULL rows that DBSPJ sends itself carry leaf 0 (a DBLQH 626 miss passes the right leaf), visible with COUNT(\*) | Kernel |
| X-1 (new) | `(SELECT COUNT(*) FROM b WHERE b.eid = e.id)` counts its own NULL row: 1 where MySQL gives 0 | RonSQL |
| E-4 | CASE over a LEFT JOIN: RonSQL emits `NULL_CMP_EQUAL` for every CASE comparison (NULL < x is true), so NULL can take THEN | RonSQL; kernel for parent columns |
| E-8 | Main ON chains are promoted (`promote_left_joins`). Gaps: the WHERE promotion (`promote_left_to_inner_for_where`) promotes only the named table, not the tables above it; CTE bodies are never promoted | RonSQL |
| S-6b, scan sibling | CTE body with a LEFT JOIN index-scan leaf under a main query that only projects | Kernel (`validateAggregateFlags`) |
| E-5 | CTE GROUP BY on strings whose values differ in word count ('abc' vs 'ábc'); the API merge does not see CTE groups | Kernel (`findInBucket`) |
| E-17 | Every CTE query; error insert 8128 (undocumented in `ERROR_codes.txt`) forces it | Kernel (DBTC) |
| E-23 | CTE GROUP BY key over 1023 words (RonSQL allows about 4000); also `ndbrequire(keyPtr.sz <= MAX_KEY_SIZE_IN_WORDS)` in `cte_lookup_send` | Kernel, plus a RonSQL prepare check |
| Q-6 / E-16 | SUM/AVG/MIN/MAX of DECIMAL with scale > 0 computed as DOUBLE (RonSQL findings F5/F21, open) | Kernel + API; own round |

Not reachable or by design: E-9 (RonSQL keeps the two lookups as siblings
or refuses the query), SUM(BIGINT) overflow 1860 (RonSQL finding F6,
expected). E-28 and E-29 are performance only.

Steps:
1. Done (2026-10-31): the failing mysqld shapes moved to the disabled
   `ndb_spj_shapes_agg_outer` and `ndb_spj_shapes_agg_exact`.
2. Done (2026-10-31): `./mtr --suite=ronsql,ronsql_cte,ronsql_jit --force`
   passed with the fixes.
3. Run (2026-10-31): one `ronsql_cte` test per row above, in the
   `spj_state_s6` style (`ronsql_compare.inc` against MySQL, the plan
   pinned with `grep -qF` on the EXPLAIN output, DUMP 2650 at the end).
   Each header gives the MySQL result and the result before a fix. The run
   stopped after 10 failures (mtr's `--max-test-fail` default), so
   `spj_ronsql_s6b_scan_leaf` did not run.

   | Test | Finding | Before a fix (observed) |
   |------|---------|-------------------------|
   | `spj_ronsql_e3_left_chain` | E-3 | the 5 groups whose root rows all miss `e3_m` are gone, except 8 rows of the last root batch |
   | `spj_ronsql_e4_case_null` | E-4 | every CASE true on NULL rows, also `o.x > 5` on the root column |
   | `spj_ronsql_e5_cte_collation` | E-5 kernel | (3, 8, 3) instead of (2, 8, 6) |
   | `spj_ronsql_e6_subquery_filter` | E-6 | not reached: X-2 |
   | `spj_ronsql_e7_multi_leaf_count` | E-7 | not reached: X-2 |
   | `spj_ronsql_e8_promotion_gaps` | E-8 | e8-1: 30 instead of 10; e8-2, e8-3 not reached |
   | `spj_ronsql_e17_cte_scan_start` | E-17 | both data nodes stop on `ndbassert(false)` in `execJOIN_AGG_COMPLETE_REQ` (duplicate REQ): DBTC sent `JOIN_AGG_COMPLETE_REQ` twice |
   | `spj_ronsql_e23_wide_group_key` | E-23 receiver | both data nodes stop on `ndbrequire(keyWords <= MAX_KEY_SIZE_IN_WORDS)` (`DblqhMain.cpp`) |
   | `spj_ronsql_e23_wide_lookup_key` | E-23 `cte_lookup_send` | data node 1 stops on the `ndbrequire` at `DbspjMain.cpp:7308` |
   | `spj_ronsql_s6b_scan_leaf` | S-6b scan leaf | not run |
   | `spj_ronsql_x1_count_star_empty` | X-1 | not reached: X-2 |
   | `spj_ronsql_q6_decimal_sum` (disabled) | Q-6/E-16 | not run |

   New finding X-2: RDRS stops on SIGSEGV for an EXPLAIN (`explainMode`
   FORCE, or `EXPLAIN SELECT`) of a query whose only aggregates are
   SELECT-list subqueries. `compile()` turns each subquery into an
   `AGGREGATE` output with `arg == NULL`, computed by its leaf's own
   aggregator, and `RonSQLPreparer::print()` called `m_main_scope.agg`,
   which is NULL then. The queries themselves run (no EXPLAIN there). Any
   client of the REST server can send such a request.

   `spj_ronsql_q6_decimal_sum` is in `suite/ronsql/disabled.def` (imported
   by `collections/disabled_ndb.def`) until the exact DECIMAL round.
4. RonSQL fixes, written 2026-10-31, not yet built or run:

   | Finding | Fix (`storage/ndb/src/ronsql/RonSQLPreparer.cpp`) | Test after the fix |
   |---------|------------------------------------------------|--------------------|
   | X-2 | `print()` prints a SELECT-list subquery output as `SUBQUERY LEAF op N slot S: FN(alias.col)` from `m_select_subquery_leaves`; any other `AGGREGATE` output without a main aggregator is an error, not a crash | e6, e7, x1 get past EXPLAIN |
   | E-4 RonSQL part | `generate_embedded_condition`: the `BRANCH_*_OP_ARG` compares use `IF_NULL_CONTINUE` (OR) or `IF_NULL_BREAK_OUT` (AND) instead of `NULL_CMP_EQUAL`; the register compares (col-vs-col, float constant) get a `BRANCH_REG_EQ_NULL` guard per operand, as `emit_embedded_filter_expr` already has. A NULL operand is UNKNOWN, so the CASE takes ELSE, also for a real row with a NULL value | e4-1, e4-2 pass; e4-3 (parent column, kernel) still fails |
   | X-1 | A SELECT-list `COUNT(*)` counts the correlation column, or else a primary key column (integers first, then CHAR/VARCHAR), which is NULL only on the NULL-extended row; `LoadUint64(1)` only when there is no such column | x1 passes (x1-4 string keys, x1-5 with a cross-table filter added) |
   | E-8 | `promote_left_joins(JoinClause*)` also runs on each CTE body before it is planned; `promote_left_to_inner_for_where` promotes the LEFT JOIN key sources of every INNER op (one backward pass) | e8 passes |
   | E-23 | `check_cte_group_key_size`: a CTE's GROUP BY key (a header word and the padded value per column, at the declared sizes) must fit in `MAX_KEY_SIZE_IN_WORDS` words, else a limit error. CTE join keys are declared alike, so this bounds the `CTE_LOOKUP` key too | both e23 tests now expect the limit error (e23g-1 is a 4000-byte control) |

   With the X-1 fix the misdirected NULL rows of E-7 count nothing from
   RonSQL any more (`COUNT(*)` loads a column), so
   `spj_ronsql_e7_multi_leaf_count` passes without the kernel fix. E-7,
   E-23 and E-17 need NDB API tests (`testSpj`) in the kernel round, as
   other clients still reach them.

   Not done: a null-rejecting cross-table WHERE (`n.x > c.y`) promotes
   only `n`, not `c`; SUM over a DECIMAL column in a SELECT-list subquery
   with a cross-table filter skips 2 words where `LoadColumn` emitted 3
   (to check).

   Then the kernel (S-6b scan sibling, E-23, E-17, E-7, E-5, E-6, E-3);
   then E-4 for parent columns.

   Kernel fixes, written 2026-11-02 (first run the same day: E-3, E-17
   and E-23 pass, the others were not reached, see below):

   | Finding | Fix | Test after the fix |
   |---------|-----|--------------------|
   | E-17 | DBTC: `ScanRecord::m_cteFragScansSent`. `execCTE_PHASE_COMPLETE_REP` does not start a CTE's redistribution until `sendFragScansLab` has sent every `SCAN_FRAGREQ` (`m_cteScanReportsExpected` is final then); `cteFragScansSent` starts the CTEs that reached the threshold meanwhile. Error insert 8128 is now in `ERROR_codes.txt` | `spj_ronsql_e17_cte_scan_start` e17-2 gives (40, 4000) |
   | E-23 | New error 20023 (`DbspjErr::CteKeyTooLong`). DBLQH `continueJoinAggRedistribute` aborts the CTE for a group key over 4092 bytes (every group, before the owner test); `execJOIN_AGG_REDISTRIBUTE_REQ` REFs instead of `ndbrequire` (`redistributeKeyTooLong`); DBSPJ `cte_lookup_send` fails the query instead of `ndbrequire` | RonSQL refuses first. Kernel: `testCteNdbApiFilter` Tests 30 (redistribution) and 31 (`cte_lookup_send`) expect 20023 and check that the data nodes stay up; they need two data nodes (`ndb_push_agg_dist`) and print SKIPPED on one. The receiver's REF has no test (the sender refuses first) |
   | S-6b | DBSPJ `validateAggregateFlags`: match tracking for outer-joined scan aggregate leaves moved out of the `RT_AGGREGATE` branch, as for the CTE_LOOKUP leaves (S-6). Added after the first run: `scanFrag_send` sets `OuterJoinAggFlag` and sends the range count also for an outer-joined aggregate leaf of a CTE body (`T_CTE_SCAN`); without it DBLQH sent no match bitmask and no NULL row was sent even with `RT_AGGREGATE` | `spj_ronsql_s6b_scan_leaf` s6b-1 gives (7, 12, 165), s6b-2 has (6, 1, NULL) and (7, 1, NULL) |
   | E-8b | Found by e8-2 once RonSQL promotes the CTE body to all INNER: every real-table node of a CTE body is `T_CTE_SCAN`, so `lookup_send` set `JoinAggFlag` on the intermediate lookup of "t JOIN c JOIN n" and its row went through n's aggregation program (error 1869). The build now marks the real-table nodes between the root and the aggregate leaf `T_CTE_INDIRECT_FEED`, as the root already was, and `lookup_send` checks it as `scanFrag_send` does | `spj_ronsql_e8_promotion_gaps` e8-2 gives s = 10 |
   | E-7 | DBSPJ `sendJoinAggNullRow` puts the leaf index into the identity word of `JOIN_AGG_NULL_ROW_REQ`; DBLQH runs the NULL row through that leaf's program | `testStarJoinAggNdbApi` Test 16: LEFT OUTER star, NULL `fk_b` on leaf 1; per group (count_a, sum_a, count_b, sum_b) = (5, 150, 5, 9000) and (7, 630, 7, 24000); before the fix leaf 1's NULL rows were counted by leaf 0 |
   | E-5 | `GBHashTable::findInBucket` compares string columns with the collation only, not the key length or data size; every caller locates the accumulators after the stored key (`dataKeyLen`), and a CTE_LOOKUP continues with the stored key's length | `spj_ronsql_e5_cte_collation` gives (2, 8, 6) and (1, 6) (2, 6) (3, 2) |
   | E-6 | DBLQH `continueAfterLogAbortWriteLab`: a 626 or 899 from TUP (the pushed filter rejected the row) on an outer-joined aggregate lookup leaf feeds the NULL-extended row and sends `LQHKEYCONF`, as a key not found in ACC does (`feedOuterJoinAggNullRow`, shared with `handleOuterJoinAggKeyNotFound`) | `spj_ronsql_e6_subquery_filter` e6-1 has (1, NULL) and (3, NULL) |
   | E-3 | DBSPJ `handleAggAncestorComplete` sweeps a lookup intermediate as soon as it and its predecessors have completed the batch, not only after the scan ancestor's last fragment; `handleAggScanBatchComplete` sweeps when the scan ancestor completes the batch last; a row that got its NULL row is marked (`m_matched |= m_coverage`), so no later sweep sends it again | `spj_ronsql_e3_left_chain` matches MySQL |

   E-4 for parent columns, written 2026-11-02 after the round above was
   committed: `JoinAggInterpreter::evalEmbeddedForNullRow` evaluates
   `READ_LINKED_TO_MEM`, `READ_LINKED_COLUMN_TO_REG`,
   `BRANCH_LINKED_EQ/NE_NULL`, `BRANCH_MEM_OP_ARG`,
   `BRANCH_MEM_OP_ARG_INLINE_TYPE`, `BRANCH_LINKED_OP_LINKED` and register
   compares of two non-NULL registers on the row's linked data, mirroring
   the DBTUP handlers. `BRANCH_MEM_OP_ARG` takes the parent column's type
   from the table descriptor through the new
   `Dbtup::getBranchMemColumnType` (the checks of `handleBranchMemOpArg`);
   DBLQH passes `c_tup` to `processNullExtendedRow` (the query-thread
   NULL-row path already borrows the table records). Programs with
   aggregation registers or arithmetic still take THEN. Tests:
   `spj_ronsql_e4_case_null` e4-3 (memory compare, both NULL-row paths),
   e4-4 (DOUBLE parent column, register path) and e4-5 (two parent
   columns). Recorded 2026-11-03: all five steps match MySQL. The
   regression run after E-4 (2026-11-03) passed.

   First run, 2026-11-02 (a regression run that stopped after 10
   failures, so e5, e6, e7, e8, s6b and x1 were not reached):
   - `spj_ronsql_e3_left_chain`, `e17_cte_scan_start`,
     `e23_wide_group_key` and `e23_wide_lookup_key` gave the expected
     output and failed only for want of a `.result` file; their logs are
     now the `.result` files.
   - `spj_ronsql_e4_case_null` fails at e4-3 only, as expected.
   - Second run (`--do-test=spj_ronsql_`): e5, e6, e7 and x1 gave the
     expected output; their logs are now the `.result` files. s6b failed
     at s6b-1 and e8 at e8-2, which gave the S-6b addition and E-8b in
     the table above. Outer-joined lookup intermediates of a CTE body
     still get no NULL rows: `T_AGGREGATE_ANCESTOR` is set only on the
     main query's nodes (not tested yet).
   - Regression run after the S-6b addition and E-8b (2026-11-02, suites
     ronsql, ronsql_cte, ronsql_jit, ronsql_cte_jit, ronsql_fs,
     ndb_push_agg, ndb_push_agg_dist, ndb_push_agg_jit, ndb_spj): 373
     tests, only `spj_ronsql_e4_case_null` fails (e4-3). s6b and e8 are
     recorded and match MySQL.
   - `test_spj_malformed_p24`: the DbtcProxy dropped the request as it
     should, but mysqld stopped on a dictionary assertion in the metadata
     change monitor, which found the NDBT database `TEST_DB` with
     `lower_case_table_names=2` (macOS) during the 10 s the test waits for
     a reply. `suite/ndb_spj/my.cnf` now sets `ndb_metadata_check = false`.
   - `ndb_push_agg_jit` canaries: `rondb_jit_decimal_canary`,
     `double_canary` and `mixed_canary` asserted JIT compiles of SUM over
     scale-2 DECIMALs and of a DECIMAL expression that mysqld no longer
     pushes (Q-6 / E-16). They now use MIN / MAX, which mysqld still
     pushes, and the DECIMAL TPC-H Q9 shape moved to
     `rondb_jit_ronsql_canary` as Q7b. That canary failed on its alias
     `s3`, which RonSQL rejects as a MySQL keyword since ca769348db6; the
     aliases are now a1, a2, a3.
5. `./mtr --suite=ndb_push_agg_dist --enable-disabled ndb_spj_shapes_agg_outer`
   after the E-3 and E-6 kernel fixes; move the shapes that pass back to
   `ndb_spj_shapes_agg`. E-8 (no nest information from mysqld), E-9 (the
   forced chain in `ha_ndbcluster_push_agg.cc`) and Q-6a/b stay for the
   storage-engine round.

   Run 2026-11-03: E-6a and E-3a-c pass; E-9a, E-8a and E-8b fail as
   before. E-6c-p1/p2 now have the NULL group with the right counts
   (73980, 0), but its key showed 10 instead of NULL: a mysqld bug (N-1),
   found now because the kernel used to lose the group. `e2_t3.g` is
   NOT NULL, and `store_group_column` called `Field::set_null()`, which
   does nothing on a NOT NULL field; MySQL marks the NULL-complemented
   row of an outer-joined table with `TABLE::set_null_row()`, which
   `Field::is_null()` reads, so the field kept a stale value. N3 groups
   by the nullable `f.y` and did not show it. Fix: `store_group_column`
   sets the table's NULL row for a NULL value of a NOT NULL field (an
   error if the table is not nullable), and `ndb_fetch_next_aggregate_row`
   clears the mark on every group column's table before storing the next
   group. E-6a, E-3a-c and E-6c moved back to `ndb_spj_shapes_agg`, with
   new shapes N7-N10 (GROUP BY a NOT NULL column of an outer-joined
   lookup or scan leaf, a nullable and a NOT NULL column of one table, a
   root and a leaf column) and E-6d (E-6a grouped by the leaf's NOT NULL
   column). `ndb_spj_shapes_agg_outer` keeps E-9a, E-8a and E-8b.
   Recorded 2026-11-03: all 56 shapes of `ndb_spj_shapes_agg` pass
   (N9's two-column GROUP BY is pushed too); `ndb_spj_shapes_agg_outer`
   fails on E-9a, E-8a and E-8b only, with the numbers of the first run.
   E-8b's SUM(l2.v) with pushdown equals E-9a's (315, 354, 328 for
   groups 0, 1, 10): in both, l2 is only looked up for rows where l1
   matched, as under the forced chain of E-9.
   Regression run after the mysqld fix (2026-11-03, suites ndb_push_agg,
   ndb_push_agg_dist, ndb_push_agg_jit, ndb_spj): all tests pass.

   E-8 and E-9, 2026-11-03: DBSPJ only learns whether a node is inner or
   outer joined to its parent (`NI_INNER_JOIN`); `setFirstInnerJoin` /
   `setUpperJoin` stay in the NDB API. So the data nodes cannot evaluate
   an inner join inside an outer join nest (E-8a: a nest row without a
   match drops the root row), nor a table that the forced chain puts
   after an outer-joined table that is not its parent (E-9a, E-8b: the
   table is not joined when that table has no match). Pushing them needs
   the nests in the DBSPJ protocol and, for E-9, aggregation over sibling
   leaves in mysqld. Until then `ndb_push_aggregation` refuses them
   before forcing the chain: 1) a table outer joined to the root that is
   not the first table of its join nest, 2) a table with an outer-joined
   table between its parent and itself. E-9a, E-8a and E-8b moved back to
   `ndb_spj_shapes_agg` with `$spj_expect_agg= 0`, and
   `ndb_spj_shapes_agg_outer` is removed.

## How to run

Build everything (data nodes with error-insert support, mysqld, `testSpj`,
`testSpjMalformed`), then from `mysql-test/`:

```
./mtr --suite=ndb_spj --force --do-test=ndb_spj_robust_
./mtr --suite=ndb_spj --force --do-test=test_spj_malformed_
./mtr --suite=ndb_opt --force ndb_spj_shapes_nulls ndb_spj_shapes_keys ndb_spj_shapes_outer \
      ndb_spj_shapes_fanout ndb_spj_shapes_wide ndb_spj_shapes_variants
./mtr --suite=ndb_push_agg_dist ndb_spj_shapes_agg
./mtr --suite=ronsql_cte --force spj_state_s1_minmax_maxrows spj_state_s5_cte_lookup_successor \
      spj_state_s6_cte_body_null_row spj_state_s15_eviction_cte spj_state_s15_eviction_join
./mtr --suite=ronsql_cte --force --max-test-fail=0 --do-test=spj_ronsql_
./mtr --suite=ronsql_cte --enable-disabled spj_ronsql_q6_decimal_sum
```

The `test_spj_malformed_*` tests, the five `spj_state_*` tests above and
`spj_ronsql_e3_left_chain`, `e4_case_null`, `e5_cte_collation`, `e6_subquery_filter`,
`e7_multi_leaf_count`, `e17_cte_scan_start`, `e23_wide_group_key`,
`e23_wide_lookup_key` and `x1_count_star_empty` have `.result` files. Record each of the others with
`--record` once it passes. A test that hits a weakness dies before recording.
A failure message starting with "Test setup:" means the test needs adjusting,
not DBSPJ.
