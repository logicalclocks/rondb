# DBLQH / JoinAggInterpreter analysis — coverage, duplication, hot-path error handling

Date: 2026-11-09. Branch 26.10-main @ 50eb491eb3f. Static analysis only: nothing was built, run or edited.

> **Status (2026-11-12):** bugs 1–7, 10 and 13–16 are fixed. Bugs 8, 9, 11 and 12 and the later sections are
> still open. See `ronsql_exec_fixes_handoff.md` for the fix commits, current code locations and the gaps the
> fixes left. The status column and line numbers below are as of the analysis and are not updated.

Scope: `dblqh/DblqhMain.cpp` (join-agg/CTE regions), `dblqh/DblqhProxy.cpp` (SETUP/RELEASE/teardown),
`dblqh/JoinAggregationState.hpp`, `vm/SimulatedBlock.cpp` (state pool, identity hash, parking),
`dbtup/JoinAggInterpreter.*`, `dbtup/AggInterpreterBase.*`, `dbtup/AggInterpreter.*`, `dbtup/AggHashTable.hpp`,
`dbtup/InterpreterCommonOp.hpp`, `dbtup/PushdownInterpreter.cpp`, the JoinAgg feed in `dbtup/DbtupExecQuery.cpp`.

Status legend: **Verified** = I re-read the code and confirmed the mechanism. **Reported** = a subagent's
finding that I did not independently re-check. Line numbers are current as of the commit above.

---

## 1. Correctness bugs found during the analysis

| # | Status | Where | Bug | Fix direction |
|---|---|---|---|---|
| 1 | **Verified** (mechanism); needs a test to show the wrong output | `PushdownInterpreter.cpp:200-221` (`OptimizeProgramBuffer` kOpSum/Max/Min), `AggInterpreterBase.cpp:737` `SumBigint`, `:822` `SumDouble` (and Max/Min Bigint/Double); lowering in `ronsql/AggregationAPICompiler.cpp:566-622` | CASE is lowered as `EmbeddedInterp / THEN-arm + agg(slot k) / Skip / ELSE-arm + AggRepeat(slot k)`. The optimizer types registers in a straight pass that ignores control flow, so the two arms are specialised independently: `SUM(CASE WHEN c THEN dbl_col ELSE 0 END)` becomes `kOpSumDouble` in THEN and `kOpSumBigint` in ELSE on the **same slot**. The typed kernels never check or re-type `res->type`. If a group's first row takes ELSE, the slot is BIGINT, then `SumDouble` writes `val_double` bits into a slot still typed BIGINT, and the result is garbage. The same applies to MIN/MAX with ELSE ≠ 0, and to DECIMAL THEN arms (generic op) with ELSE 1 (`SumBigint` adds to the double bits). The JIT refuses mixed slot types and falls back to the interpreter, so `_jit` suites are affected too. Reported: the mysqld pushdown path (`ha_ndbcluster_push_agg.cc:189-237`) emits the same shape. | Specialise a slot only if every writer of that slot has the same proven type (a pre-pass collecting per-slot types), and reset register types at jump targets. A cheaper alternative: the typed kernels fall back to the generic kernel when `res->type` doesn't match (one predictable compare). |
| 1b | Reported | `PushdownInterpreter.cpp:97-101` (`kOpMov`) | Same root cause. GREATEST/LEAST lowers to a *conditional* Mov, but the optimizer treats it as unconditional. CTE operands skip the integer-only restriction (`RonSQLPreparer.cpp:18540`). | Same fix as #1. |
| 1c | Reported (suspected crash) | `AggInterpreterBase.cpp:1189-1191` vs `:1296-1335` | `MIN(CASE WHEN c THEN char_col ELSE 5 END)`: the THEN arm stores a string pointer in the slot, then the ELSE arm's `MinBigint` overwrites it with integer 5, which is later read and freed as a pointer. RonSQL only rejects SUM/AVG over strings. | Reject mixed string/numeric CASE arms at prepare time, and also guard in the kernel. |
| 2 | Verified fallthrough; wrong result Reported | `JoinAggInterpreter.cpp:1807-1814` together with `evalEmbeddedForNullRow` default arm `:1383-1385` | On an outer-join NULL-extended row, an embedded program containing `READ_AGG_REG_TO_REG` (every GREATEST/LEAST lowering) returns `EMB_NULL_ROW_UNSUPPORTED`, and the code then **takes the THEN path unconditionally**. `GREATEST(inner.col, outer.col)` on a miss then yields `outer.col` instead of NULL. Argument order decides whether the result happens to be right. | Teach the NULL-row evaluator `READ_AGG_REG_TO_REG`, or make UNSUPPORTED produce NULL for GREATEST/LEAST shapes. Add a LEFT JOIN + GREATEST/LEAST test. |
| 3 | **Verified** | `JoinAggInterpreter.cpp:1569-1581`; reset only at `DbtupExecQuery.cpp:5714`, retry label at `:5771` | In the new-group path, `req_struct->read_length` and `m_result_size` are set **before** `allocGroupData`. If the allocation fails, the code returns `AGG_EVICT_NEEDED`, `handleJoinAggRow` evicts and `goto retry`, but `read_length` is not reset inside the loop. The retry then fails the entry check at `:1425` with `ZAGG_OTHER_ERROR`. `m_result_size` is also double-counted, which is harmless because it is write-only in JoinAgg. Only non-CTE mode reaches this. | Set both only after a successful allocation, or reset `read_length` at `retry:`. Remove the write-only `m_result_size` from JoinAgg. |
| 4 | **Verified** (found by 2 agents independently) | `DblqhMain.cpp:23276-23294` (grouped), `:23398-23415` (scalar) in `cteScanEmitResults` | `emitCteGroupOutput` returns -1 on overflow (8 sites, `:21862-21955`), but CTE_SCAN tests only `outPos > 0`. The overflow is dropped silently, possibly after a FLUSH_AI chunk was already sent, so the API sees a truncated row. CTE_LOOKUP handles the same case with `ZCTE_LOOKUP_OUTPUT_OVERFLOW` (`:22033-22042`). The legacy path also silently skips a group on key overflow (`:23301-23305`), while an aggregate overflow a few lines later sends a REF. | Check `outPos < 0` and send a CTE_SCAN REF (1266) after releasing the iterator state. |
| 5 | **Verified** | `DblqhMain.cpp:22336` (`const CteLookupReq req`), writes at `:22556`, `:22579`, `:22588` | `const_cast<CteLookupReq&>(req).keyLen = …` modifies a `const` object, which is undefined behaviour. The compiler may keep using the old `keyLen`. | Make `req` non-const, or carry an `effKeyLen` local. Do this before outlining any helper that takes `const CteLookupReq&`. |
| 6 | **Verified** unchecked | `JoinAggInterpreter.cpp:2001`; `DblqhMain.cpp:21084`, `:21181`; debug-only `assert` at `AggInterpreter.cpp:533-534` | `key_len << 16 \| v_len_total` is packed with no 16-bit check. Several wide string MIN/MAX payloads in one group can exceed 64 KB and spill into `key_len`. The existing `ndbrequire` bounds `cevictBuffer` (32 784 words), not 64 KB. | Put one encoder helper in the base class with a release-mode check, and REF with a clean error. |
| 7 | Verified that the state is set as described; the double REF is plausible | `DblqhMain.cpp:20880-20896` (merge failure); `m_cte_complete_senderRef` set at `:20251`; `abortCteRedistribution` `:23900-23956` | The merge-failure COMPLETE_REF sets ERROR but neither sets `m_cte_complete_reply_sent` nor (in CTE mode) goes through `abortCteRedistribution`. Peers get no FINAL_REP error, and a later `abortCteRedistribution` (peer FINAL_REP error, REDISTRIBUTE_REF, node failure) sends a **second** COMPLETE_REF. The single-row arm (`:20962-20984`) does set the flag. | Add `failJoinAggComplete(signal, state, err, …)`: in CTE mode go through `abortCteRedistribution`, otherwise set ERROR, mark the reply sent and send the REF. |
| 8 | Reported (2 agents) | `DblqhMain.cpp:20442` (`checkJoinAggNodeFailed`: `!m_connected \|\| ZNODE_DOWN`), `:10399-10405` (LQHKEYREQ) and `:28019-28025` (`initScanrec`, both `m_connected` only), `:20346` (`isJoinAggCoordinatorFailed`, `ZNODE_DOWN` only) | The coordinator-liveness rule differs between sites. The NF-14 comment says `m_connected` can lag the whole node-failure protocol, so a late feed could enter a state that JOIN_AGG_NODE_FAIL_REP is reclaiming (use-after-free risk). The SCAN_FRAGREQ branch is untested. | One `joinAggCoordinatorDown(ref)` used everywhere. Add a PK-8 variant that parks a SCAN_FRAGREQ. |
| 9 | Reported | `DblqhMain.cpp:21305` `execJOIN_AGG_SEND_CONF`; `continueJoinAggSend` | No phase or identity check, and `ndbrequire(state)` with no `isAborting()` check. A stale or duplicate SEND_CONF restarts the send chain on a reused or torn-down state. REDISTRIBUTE_CONF got this check (ID-2); SEND_CONF did not. | Add the identity and phase check plus an ID-9 test. |
| 10 | Reported | `DblqhMain.cpp:23090-23125` (scalar scan feed) | Retries eviction only **once**, so a second `AGG_EVICT_NEEDED` is reported as `ZJOIN_AGG_INTERPRETER_ERROR` rather than OutOfQueryMemory. The other feeds loop. A missing target also gives different errors in the lookup feed (`:21649`, NOT_READY) and the scan feed (`:22846`, NOT_FOUND). | Use one `feedCteLinkedRow` helper (see the duplication section). |
| 11 | Reported | `DblqhMain.cpp:25320` (FINAL_REP), `:24713`, `:20071`, `:20678` | When the park pool is exhausted, the FINAL_REP is **dropped with only a log line**. The owner never counts that sender and sits in CTE_REDISTRIBUTING until DBTC times out. | REF or abort the state instead of dropping. Add PK-4b. |
| 12 | Reported | Linked-buffer walks: `JoinAggInterpreter.cpp:1477-1499` (GROUP BY), `:798-841` (`readLinkedAttrIntoBuf`); `DbtupExecQuery.cpp:8746-8762`, `:9306-9320`; `DbtupJitGlue.cpp:1497-1511`; AttrInfo parsing in `buildCteLinkedBuffer` `DblqhMain.cpp:21499-21519` | 9 copies of the "seek linked position" walk with 4 different levels of bounds checking. The weakest copy reads `p[2]` and copies `1+dataSize` words with no check against the source end or `g_attr_read_buf_len_`. `buildCteLinkedBuffer` never checks `subStart+subLen <= attrInfoLen`. The data comes from DBSPJ, so this is defence in depth. | One checked `linkedEntryAH()` (the `nullRowLinkedEntry` body) and one `parseCteAttrInfo()`. |
| 13 | Reported | `DbtupExecQuery.cpp:1074-1082`, `PushdownInterpreter.cpp:277-288` | In a single-table pushdown **scan**, an `AggInterpreter::Init` failure (OOM in `initBufBlock`, or an embedded-program check) hits `ndbrequire`/`require`, which is a **node crash**. The key-read path (`CreateAggForRead`) fails gracefully. | Return a scan error. Add an error insert that fails `initBufBlock`. |
| 14 | Reported | `AggInterpreterBase.cpp:315-348`, `:388-396`, `:1519-1871`; `JoinAggInterpreter.cpp:121-127`; `DblqhProxy.cpp:3638/3712` | Program validation on malformed API input:<br>• `n_agg_results` (≤65535) is never checked against the 256-slot buffer.<br>• `agg_index` is never checked against the slot count.<br>• The 4-bit `reg_index` indexes 8 registers.<br>• DECIMAL precision/scale checks are `assert` only.<br>• In multi-leaf programs only leaf 0 goes through `Init`, so the embedded programs of leaves 1..N are never validated. | Validate every leaf in the proxy at SETUP, and add testSpjMalformed P15+. |
| 15 | Reported | `AggInterpreterBase.cpp:1389` vs `JoinAggInterpreter.cpp:1564/1605` | Multi-leaf `minMaxString` indexes the string side-table with the leaf-local index, but encoding uses the combined index, so a leaf>0 VARCHAR gets the wrong length prefix. | Pass `m_acc_offset` through. Add a star-join test with `MAX(varchar)` on leaf 1. |
| 16 | Verified code; reachability unknown | `JoinAggInterpreter.cpp:2768-2808` | `ensureStringResultsFromRedistribution` sets `charset = nullptr`, whereas `ensureStringResultsFrom` copies it. If neither a local row nor setup metadata (`initStringAggSlotFromMetadata`) created the string slots, the redistributed collated MIN/MAX compares with a null charset (`AggInterpreterBase.cpp:1432`). | Check whether setup always pre-creates the slots. If not, copy the charset from metadata. |

Smaller items, all reported:
- `emitCteLinkedAggSlot` (`DblqhMain.cpp:21361`) encodes a NULL string slot as `encodeWord0(typeId, 8)` with charset 0.
- `MaxBigint`/`MinBigint` leave `is_unsigned=false` where `Max`/`Min`/`aggMerge*` set it to true. The value is the same.
- `DblqhProxy` `sendJoinAggSetupRef` does not zero `m_column_meta_len`, `m_num_leaves` or `m_numReceiverIds`, although its comment says it nulls every freed field. `execJOIN_AGG_RELEASE_REQ` does zero them.
- `LLONG_MIN DIV 1` reports a false overflow (`InterpreterCommonOp.hpp:1150-1153`).
- Integer DIV with DOUBLE operands converts to Int64 without a range check (`:563-566`, `:612-621`).
- The DATE arm of the linked typed decode is missing in the JIT helper and bridge (`ndb_jit_bridge.c:1292-1314`), so DATE-watermark filters never JIT. This is a performance issue only.
- Bridge comment `:174` is wrong about BIGUNSIGNED/FLOAT/DOUBLE.
- Log text at `JoinAggInterpreter.cpp:1420/1426` says "AggInterpreter::ProcessRec".
- The 64-bit pointer packing order in CONTINUEB is inconsistent: the feed puts low in `[6]` and high in `[7]`, while teardown and page-free put high in `[1]` and low in `[2]`.

---

## 2. Coverage gaps (no gcov data; mapped statically against MTR suites, testSpj*, testNodeRestart, block_unit_test)

### 2a. What is already well covered
- **Interpreter:** eviction, OOM teardown, SUM-64 overflow (per row and at merge), aggregation on key reads, CTE AVG and ORDER BY/LIMIT, DIV/MOD, temporal/utf8mb4/unsigned types.
- **DBLQH signals:**
  - the main path of every handler and most CONTINUEB cases;
  - 31 of the 35 DBLQH inserts 5120–5154;
  - parking and replay for all 6 GSN classes (PK-1);
  - NF-1..14 and PK-8;
  - ID-1..8 and TD-1..8;
  - single-row/single-group violations, and sender-side CteKeyTooLong.

### 2b. Missing tests — interpreter (highest value first)
| Test | Catches |
|---|---|
| `SELECT g, SUM(CASE WHEN k>5 THEN d ELSE 0 END), MAX(CASE WHEN k>5 THEN d ELSE 7 END), SUM(CASE WHEN k>5 THEN p ELSE 1 END), MIN(CASE WHEN k>5 THEN p ELSE 999 END) FROM t GROUP BY g` with `d DOUBLE`, `p DECIMAL(10,2)`, both arms taken and some groups whose first row takes ELSE. Run via RonSQL, and in mysqld with pushdown on vs off. (Today no test has a non-INT THEN arm; DECIMAL THEN arms only appear with `ELSE 0`, which happens to leave the bits unchanged.) | Bug 1 |
| `WITH s AS (SELECT g, SUM(p) t FROM x GROUP BY g) SELECT SUM(GREATEST(s.t,5)), SUM(GREATEST(5,s.t)) FROM s` | Bug 1b |
| Negative test: `MIN(CASE WHEN c THEN char_col ELSE 5 END)`, expecting a prepare-time error | Bug 1c |
| `SELECT c.id, SUM(GREATEST(o.amt,c.floor)), SUM(GREATEST(c.floor,o.amt)), COUNT(LEAST(o.amt,c.floor)) FROM c LEFT JOIN o ON o.cid=c.id GROUP BY c.id`, with customers that have no orders; nullable and NOT NULL `o.amt` | Bug 2 |
| testSpjMalformed P15+: `n_agg_results=300`, register 9, `agg_index` past the slot count, a backward branch in leaf 1's embedded program | Bug 14 |
| An error insert that fails `initBufBlock` on a single-table scan; expect a scan error, not a node crash | Bug 13 |
| A star/subquery shape with `MAX(varchar)` on leaf ≥1 | Bug 15 |
| `DECIMAL(30,0)` holding 10^20 with `SUM`, pushed in mysqld and via RonSQL; expect 1864. Errors 1861–1867 are never asserted anywhere. | DECIMAL error paths |
| `SUM(b DIV 1)` with `b = INT64_MIN`; `SUM(d DIV 1)` with `d = 1e30` | ICO DIV edge cases |

### 2c. Missing tests — DBLQH signal layer
| # | Path (DblqhMain.cpp unless noted) | Suggested test |
|---|---|---|
| 1 | Consumer coordinator guard, `m_connected` only (10401, 28022) | Make 5154 also bypass `m_connected` in these guards. Add a PK-8 variant that parks a SCAN_FRAGREQ under 5150 and expects SCAN_FRAGREF 286. |
| 2 | Sweeper REF for COMPLETE/REDISTRIBUTE/FINAL/NULL_ROW/SCAN_FRAGREQ (9632-9690); PK-3 only sweeps LQHKEYREQ | PK-3b in testCteProtocol: hand-park the owner-plane signals, run under 5139, and assert each REF is 1274 with the correct echo. A wrong echo is silently ignored by the receiver, so the query would hang. |
| 3 | Park pool exhausted for owner-plane signals (25320, 24713, 20071, 20678) | PK-4b with 5148 and a small cap. Expect REF 1275, not a hang (bug 11). |
| 4 | Outer-join null-row feed fails or evicts (16635, 16711-16725, 20771) | 5126 with a LEFT JOIN lookup missing into more than 3 groups, both non-CTE (eviction) and inside a CTE body (expect 1870). Risk if wrong: a double abort or a leaked op record. |
| 5 | Stale/duplicate SEND_CONF and COMPLETE_REQ with a non-RNIL key (21305, ~20112) | ID-9 (bug 9). |
| 6 | Coordinator and a peer fail together (17651) | testNodeRestart ng2r2 NRRF_ABORT while 5140 holds redistribution. |
| 7 | CANCEL in FINALIZING / SENDING_RESULTS (20413) | Extend 5154 (or add a new 5155) to hold owner merges, then DBTC 8130/8131. |
| 8 | AVG/LIMIT finalize across continuations (>1024 groups per owner); LIMIT finalize OOM (25405-25467, 25540-25600) | `ronsql_large` AVG and ORDER BY…LIMIT CTE over 20000 groups, plus an error insert for the `finalizeLimitSlice` allocation. |
| 9 | Eviction into the target of a CTE scan or lookup feed (22947/22956, 21717/21727) | 5126 with chained CTEs. |
| 10 | CteScanIterState seize failure / filter-overflow malloc failure (23483, 23756, 23778) | DUMP 2356 5 <n>, then DUMP 2362. |
| 11 | DblqhProxy SETUP failure cleanup (2941, 2906, 3131-3360, 3628-3740) | JoinAggStatePoolSize=2 with 3 concurrent queries; malformed SETUP via `sendSetupRaw`. |
| 12 | Identity hash chains longer than 1 (SimulatedBlock.cpp 6830-6880) | A VM_TRACE switch that forces a constant `jaiHash`, then rerun PK-1/PK-3/TD-1 with DUMP 2363. |
| 13 | `mergeFrom` error / redistribution queue OOM (20877, 24951, 25140) | Pin INT64_MAX rows on two LDMs and run `--sum-overflow` under 5134. |
| 14 | CTE output > ZATTR_BUFFER_SIZE (1266) and filter execution error (1268) | A CTE with many `MAX(VARCHAR(4000))` outputs. **This would also catch bug 4.** |

### 2d. Orphan error inserts and stale documentation
- **Set by no test:**
  - 5127 (DEBUG_JIT builds only)
  - 5128 (SM-5, "not run")
  - 5133
  - 5135 (superseded by DBSPJ 17533)
  - DBTC 8312
  - DBSPJ 17532
  - DUMP 2356 (pool index 5), 2366, 2367
- **Stale references:**
  - Error insert 5116 (now 5126) is still referenced in `storage/ndb/block_unit_test/CLAUDE.md:28`, `next_steps.md`, `local_database_implementation.md` and `cte_mutex_free_build_plan.md`.
  - `node_failure_test_plan.md:589-590` says PK-3/PK-4 fail with 1251; the code and tests use 1274/1275.
  - The Phase M checklist box for "CASE aggregation still uses kOpEmbeddedInterp" is satisfied but still unchecked.

### 2e. Dead or unreachable code
- `kOpDivDouble` / `RegDivDouble` has no producer: NdbAggregator emits only `kOpDiv`, and the optimizer never rewrites it. Only `bridge_tests.c` builds it. Delete it, or have the optimizer emit it.
- `processRecWithLinkedAttrs` `:1936-1943` tests `block_tup == nullptr` and then calls `block_tup->getAggXfrmBuf()` anyway. All callers pass non-null, so the branch is dead today but would crash if ever reached.
- `initGBTypesForNullLocal` (`JoinAggInterpreter.cpp:2612-2698`) is behaviourally equal to `initGBTypes(nullptr, nullptr, linked…, true, jamBuf)`, so it can be deleted (about 105 lines).
- `m_result_size` is write-only in JoinAgg.
- The `interp == nullptr` checks in the AVG/LIMIT finalize code are dead, because the getter already `ndbrequire`s.
- The FINAL_REP identWord length fallback (~25290) is dead after `ndbrequire(len == SignalLength)`.
- `ndbassert(false)` guards at 20187, 20931, 24239, 24246, 25481.
- The receiver-side `redistributeKeyTooLong` is reachable only from a hostile sender.

---

## 3. Duplicated code (~2 500 lines removable in total)

The `agg_interpreter_unification_plan.md` Steps 1–4 are done. What remains is duplication *inside* the code that
was moved to the base, and between the interpreters and their mirrors in DbtupExecQuery, the JIT glue and the
NULL-row evaluator. The shared helpers `aggInstructionWords`, `aggProgramOp`, `aggLoadColType`, `aggIsSlotOp` and
`aggSlotAsDouble` (`NdbAggregationCommon.hpp:204-336`) already exist but are used only by the NDB API.

### 3a. Interpreter side (~1 700 lines; all can be codegen-neutral)
| # | What | Copies | Consolidation | ~Lines |
|---|---|---|---|---|
| 1 | Checked BIGINT add/sub/mul/div decision trees | generic `RegPlusReg`… (`InterpreterCommonOp.hpp:77-586`) vs typed `RegPlusBigint`… (`:759-1171`); `Sum` vs `SumBigint`. The BIGINT→double ternary appears 16 times and is exactly `aggSlotAsDouble`. | force-inline by-value kernels such as `i64AddChecked(v0,u0,v1,u1,&out,&uout)` | 300 |
| 2 | MAX/MIN mirror pairs plus 18 `DEBUG_PA_INTERP` blocks | `Max/Min`, `MaxBigint/MinBigint`, `MaxDouble/MinDouble`, `aggMergeMax/Min` | `template<bool kMax>` core in the shared header, and one debug macro | 250 |
| 3 | Integer-width column decode switch | 9 copies: `AggColumnLoad.hpp:217` (canonical), JIT glue ×5, DbtupExecQuery ×2, NULL-row evaluator ×1 | `template<Uint64 kAdmitMask> aggDecodeIntegral` | 150 |
| 4 | Linked-buffer walk and typed decode | 9 and 3 copies with different hardening (bug 12) | checked `linkedEntryAH()` and `decodeLinkedTyped()` | 150 |
| 5 | `executeStandardOpcode` arm boilerplate | 14 arithmetic arms of 11 identical lines, 10 aggregate arms (`AggInterpreterBase.cpp:1520-1865`) | X-macro | 140 |
| 6 | Per-row prologue (find-or-insert group, embedded-program call, JIT dispatch, slot reset) | `AggInterpreter.cpp` vs `JoinAggInterpreter.cpp` | `template<bool kLinked>`. The maintainer previously chose separate arms, so check first. | 130 |
| 7 | `evalEmbeddedForNullRow` re-implements DBTUP semantics | `nullRowCompareRegs` vs `compareTypedRegs`; the inverted-branch mapping ×8; the filter-reject code set {0,626,899,6000-6999} ×2 | shared static-inline `branchCondHolds()` and `isAggFilterRejectCode()` | 120 |
| 8 | String-slot and merge helpers | pad+alloc+header+copy ×4; string-type test ×11; redistribution decode prologue ×2; "free local_items" loop ×6 | `aggIsStringType`, `aggStringPrefixBytes`, `ensureAggOpsCached`, RAII free | 120 |
| 9 | Program walkers re-implementing `aggInstructionWords` | 6 walkers; `decodeLoadColType` is identical to `aggLoadColType` | use the shared helpers, and merge the two `Init` walks into one | 110 |
| 10 | `initGBTypesForNullLocal`, and the collation/compare-function fill ×5 | — | delete it; add `fillGBTypeInfo()` | 105 |
| 11 | Result-record encoder | 5 copies (`AggInterpreter.cpp:513`, `JoinAggInterpreter.cpp:1984`, `DblqhMain.cpp:21068`, `:21160`) | base `encodeResultGroup()`, which **fixes bug 6 in one place** | 45 |

### 3b. DBLQH side (~800 lines)
| # | What | Copies | Consolidation | ~Lines |
|---|---|---|---|---|
| 1 | REF/CONF builders | `JoinAggRedistributeRef` ×6, `JoinAggNullRowRef` ×6, `JoinAggCompleteRef` ×6, `JoinAggCompleteConf` ×5, EndOfData `CteScanConf` ×3, bare `CteLookupConf` ×2 | `__attribute__((cold, noinline))` senders taking `callerLine`, with request fields captured before `getDataPtrSend()` aliases them. **The same change as hot-path items L3/L6/L7/L8**, so do it once. | 190 |
| 2 | DblqhProxy SETUP error exits and teardown | about 25 `releaseSections; sendJoinAggSetupRef(…, __LINE__, …); return;`; 5 redundant `allProgsBuf` frees; SETUP_REF vs RELEASE free sequences (already diverged); `continueJoinAggTeardown` phases 1 and 2; CONTINUEB packing ×5; page-free loops ×3; `REDIST_PAGES_PER_FREE_BATCH` defined twice | a local lambda, `freeJoinAggSetupBuffers(state)`, `teardownInterpStep()` | 110 |
| 3 | `cteScanEmitResults` grouped vs scalar arms | TRANSID_AI-to-SPJ ×5, `CteOutputParams` setup ×3, legacy emit ×2 | `sendCteRowToSpj()` and `emitOneCteRow()`. Fix bug 4 first. | 90 |
| 4 | Linked-row feeds | `cteLookupAggFeed`, grouped scan feed, scalar scan feed (diverged: bug 10); null-row feed ×2 | `resolveCteFeedTarget()`, `feedCteLinkedRow()`, `feedNullExtendedRow()` | 90 |
| 5 | Resolve-or-park prologue | COMPLETE, NULL_ROW, REDISTRIBUTE, FINAL_REP | fold the fast lookup into `joinAggResolveOrParkGeneric`. The trigger and the FAILED action differ per GSN, so this is medium risk. | 70 |
| 6 | Redistribution senders | `sendScalarRedistributeReq` re-implements `sendRedistributeReq`; owner-ref computation ×3; FINAL_REP fill ×2 | pack, then the common sender; `cteRemoteOwnerRef()` | 50 |
| 7 | AVG vs LIMIT finalize | `continueCteAvgFinalize`, `continueCteLimitFinalize`, two blocks in `checkCteReady` | `stepCteFinalize(kind)`. Keep the AVG-before-LIMIT order. | 45 |
| 8 | CTE_LOOKUP/CTE_SCAN request preamble and query-thread borrow | 22420-22465 vs 23667-23722; borrow bracket ×4 | `cteCheckReadyState()`; RAII guard | 40 |
| 9 | SimulatedBlock identity hash | bucket-find ×6, alloc ×2, free ×4 | `jaiAllocEntry`, `jaiFreeEntry`, `jaiFindLink`. Never nest the free and partition mutexes. | 35 |
| 10 | Park record setup | `parkJoinAggConsumer` vs `joinAggResolveOrParkGeneric` | `seizeParkRec`, `scheduleJoinAggParkSweep` | 30 |
| 11 | Identity match test | ×7 | `JoinAggregationState::matchesIdentity(transid, identWord)` | 25 |
| 12 | 5-word AttrInfo header parse | ×5 with 4 different hardening levels (bug 12) | `parseCteAttrInfo()`; each caller still chooses REF or fallback | 25 |

**Across blocks:** DBSPJ `cte_lookup_hash_key` (`DbspjMain.cpp:7043-7104`) re-implements `GBHashTable::hashKeyFull`
(`AggHashTable.hpp:676-730`). The two must stay bit-identical for CTE_LOOKUP routing, so a shared static function
removes the risk of them drifting apart.

---

## 4. Moving error handling out of hot paths

### 4a. What is hot, and what the tooling gives us
- **Per row:** `Dbtup::interpreterStartLab` → `handleJoinAggRow` (`DbtupExecQuery.cpp:5731`) → `processRecWithLinkedAttrs` → `ProcessRec` (`JoinAggInterpreter.cpp:1401`). With JIT on, the interpreter loop (`:1674-1886`) runs only for fallback rows, but `ProcessRec`'s prologue, the group-key build and the JIT dispatch run for every row.
- **Per row in DBLQH:**
  - `execLQHKEYREQ`, which every key operation runs;
  - `scanTupkeyConfLab` / `scanTupkeyRefLab`;
  - the CTE_LOOKUP chain (one per probe);
  - `joinAggNullRowReqImpl` and `feedOuterJoinAggNullRow`;
  - `getJoinAggState`, a cross-file call that cannot be inlined without LTO.
- **Per group:** `cteScanAggFeed`, `cteScanEmitResults`, the send/redistribute loops, `processRedistQueue`, `execJOIN_AGG_REDISTRIBUTE_REQ`.
- **Cold:** parking and the sweep, `routeCteLookup` (only compiled with VM_TRACE/ERROR_INSERT), `abortCteRedistribution`, node failure, the scalar arms.
- **Already free in release:**
  - `ERROR_INSERTED`;
  - all `DEB_*` and `AGGT` macros (only `DEB_AGG` is live, and only in VM_TRACE/ERROR_INSERT builds);
  - `jamDebug`;
  - `ndbassert`;
  - `ndbrequire`'s failure path, which is already cold and noinline.
- **Not free in release:**
  - `jam()` / `thrjam()`;
  - `g_eventLogger->debug/info`, a virtual variadic call that takes a mutex even when debug logging is off;
  - `sprintf` into stack buffers, which brings the stack protector.
- **No cold macro exists** (`ATTRIBUTE_NOINLINE` means noinline only). The established house pattern is a raw `__attribute__((cold, noinline))` helper taking a `Uint16 callerLine` and calling `jamLine(callerLine)` (`Dblqh.hpp:3859-3869`, `4255-4283`; implementations at `DblqhMain.cpp:6117-6138`). **None** of the join-agg/CTE helpers use it. Consider adding `ATTRIBUTE_COLD` to `ndb_compiler.h`.

### 4b. Interpreter (DBTUP side), ranked
| # | Where | Today | Proposal | Benefit / risk |
|---|---|---|---|---|
| H1 | `JoinAggInterpreter.cpp:1674-1886`, `AggInterpreter.cpp:323-483` | The fallback interpreter loop is inline in `ProcessRec`, so every JIT row pays for its frame: `local_tmpArea[16]`, address-taken locals (`exec_pos`, `op_handled`, `header`, …), a stack canary and callee-saved spills. | a noinline `runProgram(...)` | A few ns per row out of a ~11 ns JIT budget. A pure move. |
| H2 | `AggInterpreterBase::loadColumnTypedFromBuf` `:206-222`, `:175-179`, `:293-297` | The decimal error branch has `char log_buf[128]` plus a `sprintf` loop that survives release, so every call gets a stack canary and a 128-byte frame. It also has two inline `g_eventLogger->debug` calls, and a `class_name` parameter (12 arguments, ~6 on the stack) that exists only for logging. | a cold `decimalLoadError()` helper; drop `class_name` | High for interpreter rows; also runs per row on the JIT string MIN/MAX path. |
| H3 | `executeStandardOpcode(..., Uint32& exec_pos, ..., bool* handled)` | Escaped addresses force `exec_pos` into memory, so it is reloaded after every call. `handled` exists only for an invalid opcode. | The default arm returns `ZAGG_WRONG_OPERATION` directly (identical behaviour); return the new `exec_pos` or update it in the caller; pass the decimal word by value. | Per opcode. |
| H4 | `JoinAggInterpreter::ProcessRec` `:1419-1520` | Five inline `g_eventLogger->debug` calls (three without `unlikely`), the first-row `initGBTypes*` block, and the same in `AggInterpreter::ProcessRec` `:165-170` and `readLinkedAttrIntoBuf` `:807-811`. | one cold `processRecError(site,a,b)`; move the first-row init to a cold helper | Medium; per row. |
| H5 | `:1755-1759`, `:816-836`, `AggInterpreterBase.cpp:254-258` | Per-row lookups of values fixed for the whole query: a linear `findLoadColumnMeta` scan per local load (k² per row), a `findColumnMeta` hash lookup per linked load, `get_collation` per string load. | Validate or cache at setup; keep a per-offset pointer to the metadata. | Medium. |
| H6 | `:1790`, `AggInterpreterBase.cpp:1687-1715`, `:166`, `:175`, `:1796-1800` | Program-structure checks repeated for every instruction on every row. | Validate in `Init`, then make the runtime checks `ndbassert`. **Only after bug 14 is fixed** (leaves 1..N are not validated today, so these runtime checks are their only guard). | Low-medium. |
| H7 | `thrjam` at `:1424`, `:1509`, `:1916`; `jam()` at `DbtupExecQuery.cpp:6504` | These run per row in release. | Change them to `*Debug`, matching the neighbouring code. | About 5 instructions and 2 stores each. |
| H8 | `AggHashTable.hpp:496-653` | `growToHint`, `addSegment` and `splitOne` are defined in the class, so they can be inlined into both `ProcessRec`s. | a noinline `growSlow()` (not cold: splits are frequent while the table grows) | Per new group. |
| H9 | `AggInterpreter.cpp:254-281`; `JoinAggInterpreter.cpp:1792-1823`; `AggInterpreterBase.cpp:1379-1387`; `DbtupExecQuery.cpp:5780-5793` | The memory-pressure refresh, the NULL-extended embedded block, and lazy `m_string_results` allocation are inline; the eviction checks lack `unlikely`. | noinline or cold helpers; add `unlikely` | Low. |

**Leave as they are:**
- the overflow returns in `InterpreterCommonOp.hpp` (data-dependent and tiny);
- `AggColumnLoad.hpp` (it returns status codes);
- the arithmetic arms;
- `hashKeyFull` / `findInBucket`;
- `readSingleAttribute`.

### 4c. DBLQH side, ranked
| # | Where | Today | Proposal | Benefit / risk |
|---|---|---|---|---|
| L1 | `execLQHKEYREQ` `:10336-10409` | 74 lines with no branch hint in DBLQH's hottest function: an identity lookup (mutex), the park tri-state, a 15-line parked unwind, two `earlyKeyReqAbort_simple` calls, and a per-row `m_connected` check. | `if (getJoinAggFlag) { if (!lqhKeyReqJoinAgg(...)) return; }`, noinline, with the parked unwind in a cold helper inside it | Helps **all** key traffic, not only join-agg. Pass `nextPos` by reference; the caller must return immediately after a park. |
| L2 | `sendJoinAggCompleteHeartbeat` in the per-group loops `:21167`, `:24348`, `:25131` | `getHighResTimer()` returns the thread's cached tick (`*m_pHighResTimer`, verified). Within one signal, every call after the first returns early, but each still costs a call, two branches and a 64-bit divide. | Call it once per slice; 21148 and 24258 already do this. | About 30-60 cycles per group, with no behaviour change. |
| L3 | CTE_LOOKUP probe (`cteLookupReqImpl` `:22335-22719`, `cteLookupAggFeed` `:21639-21760`) | `sendCteLookupRef` (18 sites) serves both real errors and the normal GROUP_NOT_FOUND miss. There are two inline log+REF blocks (`:21656`, `:21734`), a redundant `getJoinAggState` at `:21647` (the caller already has `state`; compare keys instead), and outer-chain/anti-join rare arms interleaved with the hit path. | a cold `sendCteLookupRefCold(…, callerLine)` for the error cases only; a cold `cteLookupAggFeedFail()`; `unlikely` on the rare flags | Per probe. Keep "release sections, then REF". |
| L4 | `emitCteGroupOutput` `:21766-21965` (per column) | The G2b capture block (~33 lines), the route through DBTC (`TRANSID_AI_R`) and the NULL-row arms are all inside the per-column loop. | noinline cold helpers; `unlikely` or a separate NULL-row loop | Per probe and per scanned group. |
| L5 | `scanTupkeyConfLab` `:27284-27295`; `check_send_scan_hb_rep` `:27272`, `:27558` | The range-overflow branch has `g_eventLogger->info` plus state writes. The heartbeat check is a full out-of-line function per row. | Validate `range_no` once in `copyNextRange` (`:26484`); split the heartbeat into an inline fast check plus a noinline sender. | Per scanned row. |
| L6 | `joinAggNullRowReqImpl` | five inline `JoinAggNullRowRef` builds of about 12 lines each | a cold sender (shared with the sweeper at `:9666`) | about 55 lines out of a per-row function |
| L7 | `execJOIN_AGG_REDISTRIBUTE_REQ` | four inline REF builds, one inside the per-group loop (`:24973`); a 4 KB `keyBuf` used only by the single-group form; `isAborting()` unhinted | a cold `sendRedistributeRef`; move the single-group form to a helper | per batch |
| L8 | `cteScanAggFeed` / `cteScanEmitResults` | 26 "release iterator, then REF" pairs; logging at `:22856`/`:22968`; the per-query scalar arm inline; `CteOutputParams` rebuilt per group; the loop not unswitched on `haveFinalR` | a cold `cteScanFail()`; a separate scalar function; hoist and unswitch | per group |
| L9 | `execSCAN_FRAGREQ` park block `:25909-25953` | inline, but under `unlikely` | an outlined cold helper | small |
| L10 | `parkJoinAggConsumer`, `joinAggResolveOrParkGeneric`, `redistributeKeyTooLong`, `send_scan_fragref`, `sendCteScanRef` (cold); `handleOuterJoinAggKeyNotFound`, `feedOuterJoinAggNullRow` (noinline) | not annotated | annotate them | This fixes the missing branch hints at every caller for free. |

Also:
- per-column and per-group `jam()` calls survive release and could become `jamDebug()`;
- the identity lookup takes a mutex per LQHKEYREQ row (a per-thread last-hit cache is possible, but needs a slot-validity check).

---

## 5. Suggested order of work
1. **Fix the verified correctness bugs and add their regression tests:**
   - bugs 1/1b/1c: CASE slot typing (`OptimizeProgramBuffer`);
   - bug 2: outer join with GREATEST/LEAST;
   - bug 3: eviction retry;
   - bug 4: CTE_SCAN overflow;
   - bug 5: const_cast UB;
   - bug 6: 16-bit length packing.
2. **Hardening:**
   - bug 7: double COMPLETE_REF;
   - bug 8: liveness rule;
   - bug 9: SEND_CONF identity check;
   - bug 11: dropped FINAL_REP;
   - bug 12: bounds, via `linkedEntryAH` and `parseCteAttrInfo`;
   - bug 13: scan Init crash;
   - bug 14: validate every leaf at SETUP.
3. **DBLQH REF/CONF helpers, written as cold noinline senders with `callerLine`.** This is a single mechanical change that covers duplication §3b-1 and hot-path L3/L6/L7/L8, and makes the bug-7 and bug-10 fixes one-liners.
4. **Hot-path restructuring:** L1 (`execLQHKEYREQ`), L2 (heartbeat), H1–H3 (interpreter frame and out-params), H5 (per-row lookups), H7 (`jam`).
5. **Interpreter deduplication** §3a items 1–5, with the JIT suites as the safety net.

## 6. What to run (for you to run)
- **Correctness:**
  - `./mtr --suite=ronsql,ronsql_cte,ronsql_jit,ronsql_cte_jit,ndb_push_agg,ndb_push_agg_jit,ndb_push_agg_dist`
  - the multi-node CTE topologies `ronsql_cte_ng1r3,ronsql_cte_ng2r2,ronsql_cte_ng2r3,ronsql_cte_ng4r2`
  - `ronsql_large` for the finalize slices
  - `testSpjMalformed`, `testNodeRestart` (NF-*, PK-*), and the `testCteProtocol` block unit tests
  - `./mtr ronsql_cte_jit_census` (ERROR_INSERT 4060) for anything that touches the JIT glue
- **Hot-path changes:** do a release build (`WITH_ERROR_INSERT=OFF`) and compare before and after:
  - `nm -S --size-sort` on ndbmtd for `JoinAggInterpreter::ProcessRec`, `AggInterpreterBase::loadColumnTypedFromBuf`, `executeStandardOpcode` and `Dblqh::execLQHKEYREQ`;
  - `__stack_chk` references and call counts in their disassembly;
  - per-row time on a RonSQL GROUP BY benchmark with JIT on and off.
