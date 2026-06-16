# CTE Fix Plan — remediation of the test-driven findings (D1–D22)

Companion to `cte_test_driven_findings.md`. Sequenced per maintainer direction:
**(1) hangs → (2) crashes → (3) wrong results → (4) remaining errors.**

Every item below carries: the repro (which disabled test case + its
`# NEXT-PHASE` marker in `mysql-test/suite/ronsql_cte/include/body_<family>.inc`),
the symptom, the suspected area, and the **re-enable** step. Re-enable is always
the same shape: restore the disabled query at its marker, run
`../debug_build/mysql-test/mtr --suite=ronsql_cte <family> --record`, confirm the
diff is clean, then replicate to the topology suites (`cp` the wrapper+result, or
re-add the omitted `agg` to `ronsql_cte_ng2r3` for D22).

A hang is the worst operational failure (no fail-fast, ties up a worker), which
is why hangs lead. Each phase ends GREEN by un-disabling its cases.

---

## Phase 1 — HANGS (do first)

Likely a shared root in the CTE_LOOKUP / non-aggregating delivery path: a
request that never reaches its terminating SCAN_NEXTREQ/CONF, so DBSPJ/DBLQH
waits forever. Recommend attaching `gdb`/`DUMP` to a wedged data node on each
repro and inspecting the outstanding `ScanFragRec` / `requestPtr` state, and
turning on `DEB_CONT_SCAN` / the CTE trace macros around `cteLookupEmitResult`
and `Dblqh::continueJoinAgg*`.

- **H1 — D3: projection-only main SELECT over a CTE_LOOKUP hangs — ✅ FIXED
  (2026-06-08).** Root cause: `RonSQLPreparer::execute_passthrough_drain`
  registered no `getValue` on the main ROOT op when every projected column came
  from a CTE child, so the root scan was never set up to receive rows and
  `nextResult(true)` blocked on the first call. Fix: when no output reads the
  root op, register a throwaway `getValue` on the root's first column before
  `execute` (mirrors `testCteNdbApi.cpp` Test 17). Regression test:
  `ronsql_cte_dd_d3_hang.test` (drains 5 rows → scanComplete). Generalized to
  also fix NDB 4826 "empty projection" on a real-table CHILD op (cte JOIN real
  selecting only CTE columns). Re-enabled D3 cases mainmode MM1–MM6/13/14/16 are
  green across all topologies. **NEW residual crash D23 (Phase 2):** the related
  sub-case — projection-only with a CTE_LOOKUP child that has NO user-selected
  columns — SIGSEGVs in the kernel (MM17, disabled; the API registers all CTE
  virt cols, so it's a kernel CTE_LOOKUP-emit bug in the projection-only path).
  Original notes:
  Repro: `ronsql_cte/t/ronsql_cte_dd_d3_hang.test` (dedicated minimal repro:
  region JOIN nat, projection-only); also `body_mainmode.inc` MM1–MM6/13/14/16.
  **DBSPJ RULED OUT (2026-06-06, via the DbspjMain.cpp counter trace):** on the
  repro, DBSPJ activity is *bounded* (8 `sendConf`, all `is_complete=1`, 5
  `cte_lookup_send` all matched by `CTE_LOOKUP_CONF`, `m_outstanding` reaches 0,
  `m_cnt_active` reaches 0) and finishes in a ~1 ms burst, then goes idle until
  the timeout kill. No request left in `RS_WAITING`. So the kernel completes the
  query correctly; **the hang is on the consumer side — RonSQL's
  `execute_passthrough_drain` `nextResult(true)` loop at
  `RonSQLPreparer.cpp:6455`** (the NdbQuery carries a CTE subtree +
  CTE_LOOKUP child; `nextResult` evidently blocks even though DBSPJ already sent
  `is_complete=1`). Next step: instrument `execute_passthrough_drain` around
  `m_trans->execute(NoCommit)` (6434) and each `nextResult` iteration (rebuild
  `rdrs2`), and diff the NdbQuery operation/scan state against the WORKING
  projection-only-over-CTE_SCAN-**root** path (`ronsql_cte_scan.test`) — the
  delta (CTE_LOOKUP child vs CTE_SCAN root, or an un-closed CTE-subtree scan op)
  is the bug. Fix this first — several other "hangs" may share this consumer-side
  completion gap.
- **H2 — D2: `COUNT(<column>)` in a CTE body — ✅ RESOLVED by the D3 fix
  (2026-06-08).** As predicted ("interacts with H1"), the agg-02 repro was
  projection-only over a CTE_LOOKUP — i.e. D3, not a COUNT(col) bug. With D3
  fixed, `COUNT(<column>)` is correct (counts non-NULLs, matches MySQL) in both
  aggregating and projection-only main. Regression test:
  `ronsql_cte_dd_d2_countcol.test` (D2a aggregating + D2b projection-only).
- **H3 — D4 + D12: CTE-body signed-int col-vs-col WHERE — ✅ REJECTED
  (2026-06-08).** Root cause was two distinct failures, split by whether the
  left column is indexed:
    - *D12 (orders `o_custkey < o_orderkey`, indexed left):* the conjunct was
      mis-classified as an index bound by `build_scan_config_candidates`, so the
      INDEX_SCAN bound-extraction at `RonSQLPreparer.cpp:6033` called
      `encode_constant()` on the **column** RHS (`o_orderkey`), which throws the
      retryable `RonSQLMaybeStaleSchema` ("Only integer literals are supported")
      → RDRS retried 10× (hang-like).
    - *D4 (lineitem `l_quantity < l_partkey`, non-indexed left):* TABLE_SCAN body
      → `apply_filter_cmp`'s col-vs-col branch (`:8595`) emitted an `NdbScanFilter`
      attr-vs-attr program that **hangs** the data node.
  Per maintainer decision (permanent-error for now, feature deferred), both are
  rejected up front: new static `check_no_cte_body_col_vs_col` walks each CTE
  body's WHERE in `build_cte_scopes` (after `classify_where_by_table`, before
  scan-config selection + emit) and throws a clean `RonSQLPermanentError` when a
  comparison has two column operands. Main-query col-vs-col does not flow through
  `build_cte_scopes` and is unaffected. Re-enabled as rejection-asserts:
  `body_filter.inc` filter-12/13 + standalone `ronsql_cte_dd_d4_colvscol.test`.
  **Later phase:** actually *support* col-vs-col in a CTE-body filter — see
  "Deferred feature work" below.
- **H4 — D19: `real JOIN cte` with a main-query WHERE on a parent column — ✅
  NOT-REPRODUCIBLE (2026-06-08).** Does not reproduce on the current build: the
  query executes correctly and matches MySQL. EXPLAIN (base vs indexed-parent-
  WHERE vs non-indexed-parent-WHERE) showed the parent WHERE leaves the plan
  structurally identical — ROOT TABLE_SCAN customer + INNER CTE_LOOKUP cust
  (agg leaf), just with a root-scan filter added — and all variants run green.
  The recorded hang was a **stale rdrs2/ndbmtd captured during parallel suite
  authoring** (the only RonSQL changes since suite creation are the D3 + D4
  fixes, neither of which touches D19's aggregating path; the CTE join-agg
  routing/teardown hardening in `934ef2`/`985059` was already in HEAD).
  Re-enabled green: `body_joins.inc` J18 + standalone `ronsql_cte_dd_d19_hang`.
  **Action item: re-verify the remaining disabled hangs (H5/D20, H6/D5) against
  freshly-built binaries before diagnosing — they may also be stale-binary
  artifacts.**
- **H5 — D20: multi-key complete-key CTE lookup — ✅ NOT-REPRODUCIBLE as a hang
  (2026-06-08); but later found to harbor a flaky data race — see D26 (W4).**
  Executes (mostly) correctly on the current build in both predicate orders
  (verified J15 reversed; J14 in-order re-enabled). The original hang was a
  stale-binary artifact like D19, BUT J14/J15 subsequently surfaced an
  intermittent wrong-COUNT under multi-node-group stress (the shared
  strnxfrm-buffer race, **D26 / W5** below), now fixed. Re-enabled: `body_joins.inc`
  J14 + J15.
- **H6 — D5: 3-table chain `real JOIN cte JOIN real` — ✅ REJECTED (was a CRASH,
  not a hang; 2026-06-08).** Re-verification crashed the RDRS server
  (`NdbQueryBuilder.cpp:3020` `appendLinkedOperand` assert / null deref), not a
  hang. The pushed aggregation needs columns from BOTH children of `customer`
  (SUM over CTE child `cust` + GROUP BY real child `nation`) — a fan-out whose
  aggregation references a NON-ANCESTOR sibling. The NDB API serializes linked
  operands by walking the agg leaf's ancestor chain, and a sibling source walks
  off the root → abort. Only CTE_LOOKUP siblings are linearized into the
  ancestor chain (`QueryPlanner.cpp` `tree_parent_op_idx` loop); a real-table
  sibling never is. RonSQL now rejects cleanly at prepare via the
  `linked_source_is_leaf_ancestor` guard in `emit_child_ops` (mirrors the NDB
  API walk). Re-enabled as rejection-asserts: `body_joins.inc` J16/J17. The
  guard cannot regress any working query: in a debug build a non-ancestor
  linked projection already asserts, so no green test relied on it. **Later
  phase:** support fan-out aggregation across a real-table/CTE sibling (extend
  `tree_parent_op_idx` linearization to real-table siblings, with kernel SPJ
  support for the topology) — see "Deferred feature work".

Exit criteria: re-enable agg-02/03, filter-12/13, mainmode projection cases,
joins J14/J15/J18 green + J16/J17 rejection-asserts → all green on the base
suite + topologies.

---

## Phase 2 — CRASHES

- **C1 — D6: DBSPJ ndbassert `Error 2343, DbspjMain.cpp:7825,
  requestPtr.p->m_cnt_active == 0` — ✅ FIXED.** Two distinct root causes,
  both resolved:
    1. *Premature CTE completion.* A multi-batch CTE materialisation scan
       reached `m_outstanding==0` at every batch boundary while its scan node was
       still TN_ACTIVE; `batchComplete` reported `CTE_PHASE_COMPLETE_REP` per
       batch (truncating the scan / leaving an active node at teardown). DBSPJ
       now restarts the next batch directly (`handleCtePhaseNextBatch`, driving
       `SCAN_NEXTREQ` to DBLQH) and only reports completion at genuine EndOfData
       (`m_cnt_active==0`); a `SCAN_HBREP` heartbeat keeps DBTC's scan-frag timer
       alive during the multi-batch completion (`RONDB-1072: heartbeat during CTE
       aggregation completion`).
    2. *Cross-node GROUP BY hash inconsistency (the agg-16 / redistribution-hash
       bug, a.k.a. D24-class).* `hashGroupKey` chose charset-aware vs raw xxhash
       per the result interpreter's `m_gb_types_inited` state; a node whose
       result interpreter never processed a CTE row (so never ran the per-row
       `initGBTypes`) hashed CHAR/VARCHAR keys with raw xxhash while type-aware
       nodes used the charset-aware hash → the same group routed to two owners →
       split groups / over-count (and contributed to teardown accounting bugs).
       Fixed by **publishing GROUP BY (and linked / load) column type metadata
       through `JOIN_AGG_SETUP`** and initialising the interpreter from it
       (`initGBTypesFromMetadata`) instead of lazily on the first row — so every
       node's hash is charset-aware and identical regardless of data
       distribution. Landed as the metadata commit series
       (`…emit/carry/consume/validate/centralize … metadata`,
       `use setup metadata for join aggregation columns`,
       `use typed NULL metadata for CTE linked columns`).
  Follow-up (deferred): re-enable the disabled `body_agg.inc` cases (agg-06
  lookup form; agg-19/20/21 CTE_SCAN-root form), `--record`, and re-run all
  topologies to confirm green; then retire the untracked probes
  (`ronsql_cte_dd_d6_crash.test`, `ronsql_cte_ng4r2/…_agg16_probe.test`).
  **Re-verify D22 (W1)** against the metadata fix — it is the same CHAR-key
  (`p_brand`) cross-node redistribution shape and may already be resolved.
- **C2 — D18: data-node crash `NDB Error 6000 / Signal 6 abort`** when the main
  query RE-AGGREGATES a string (CHAR/VARCHAR) MIN/MAX CTE output — **✅ FIXED by
  the metadata series.** A string aggregate consumed as a *linked* input to the
  main aggregator was mis-sized/mis-pointed because the linked-column type was
  inferred per-row rather than supplied; publishing typed (incl. typed-NULL)
  CTE linked-column metadata through `JOIN_AGG_SETUP` and consuming it via
  `initGBTypesFromMetadata` gives the linked string aggregate the correct
  type/charset/size up front.  Verified: `ronsql_cte_dd_d18_probe` D18a (CHAR(1)
  `o_orderstatus`) → `F, P, 1500` (RonSQL == MySQL, no crash).  Closing
  follow-up: re-enable `body_agg.inc` agg-11..15 to confirm the remaining string
  types (CHAR(10) `p_brand`, CHAR(1) `l_returnflag` via CTE_SCAN root,
  VARCHAR(12) `c_mktsegment`, VARCHAR(40) `p_name`), `--record`, re-run
  topologies.
- **C3 — D23: SIGSEGV (`NDB Error 6000 / Signal 11`)** in the kernel CTE_LOOKUP
  emit/projection path for a **projection-only** main SELECT that joins a
  CTE_LOOKUP child with **no user-selected columns** from the CTE — **✅ FIXED by
  the metadata series.** The pre-fix trace crashed in the linked-attr (`0x8000`)
  `ProcessRec` path while handling a `JOIN_AGG_NULL_ROW_REQ`; supplying validated
  linked-column metadata up front (vs per-row inference with no projected CTE
  columns) fixed the segfault.  Verified: `ronsql_cte_dd_d23_probe` D23a →
  `c_custkey` 1..20 (RonSQL == MySQL, no crash).  Closing follow-up: re-enable
  `body_mainmode.inc` MM17, `--record`, re-run topologies.

**D6, D18, D23 fixed on the base / single-node-group topology** by the metadata
series (GB/linked/load type metadata published through `JOIN_AGG_SETUP` +
`initGBTypesFromMetadata`) plus the CTE multi-batch completion fix (batch restart
in DBSPJ + EndOfData-only `CTE_PHASE_COMPLETE_REP` + heartbeat).  Re-enabling the
disabled cases (item 1) recorded green on base + ng1r3 (1 node group) but
surfaced **D25** below on ≥2 node groups.

- **C4 — D25: DBLQH ndbassert `Error 2343, DblqhMain.cpp:21519,
  state->m_owner_instance == instance()`** in `execJOIN_AGG_REDISTRIBUTE_CONF`,
  on a **high-cardinality CTE redistributed across ≥2 node groups**.  Latent
  until now: D6 had disabled the only high-cardinality cross-NG cases, so the
  `RI_NEED_CONF` batched redistribution round-trip was never exercised on
  multi-NG.  Root cause: the `JOIN_AGG_REDISTRIBUTE_REQ` carries only the
  **destination** node's pool key (`req->aggStateKey = dstKey =
  m_cte_remote_aggKeys[ownerNode]`); the receiver echoes that key in the CONF
  (`execJOIN_AGG_REDISTRIBUTE_REQ` → `conf->aggStateKey = aggStateKey`); the
  sender's `execJOIN_AGG_REDISTRIBUTE_CONF` then does `getJoinAggState(dstKey)`
  — looking up its **own** waiting state by the *destination's* pool index,
  landing on the wrong `JoinAggregationState` (whose `m_owner_instance` ≠ the
  current instance).  **Fix:** carry the sender's own `aggStateKey` in
  `JoinAggRedistributeReq` (room in `SignalLength`), have the receiver echo it
  in `JoinAggRedistributeConf`, and resume via that sender key in the CONF/REF
  handlers.  Repro (now enabled): `body_agg.inc` agg-06 on any ≥2-NG suite
  (ng2r2 / ng2r3 / ng4r2).  Evidence:
  `var/log/ronsql_cte_ng2r2.ronsql_cte_dd_agg/.../ndb_3_error.log`.  Likely
  shares the multi-NG-redistribution neighbourhood with D22 (W1) but is a
  distinct (crash vs wrong-result) bug.  **NEXT (blocks item-1 re-record on
  multi-NG).**

Closing follow-up once D25 is fixed: `--record` agg + mainmode on all
topologies, retire the untracked `*_probe` / `d6_crash` files, and re-verify
D22 (W1).

---

## Phase 3 — WRONG RESULTS / semantics

- **W1 — D22: wrong COUNT under the 2 node-group × 3-replica (6-node) topology
  only — ✅ FIXED (resolved by the metadata + D25 redistribution work).**
  `SUM(prt.n)` (a COUNT fan-in over the CHAR-key `p_brand` CTE_LOOKUP, agg-05)
  returned e.g. BRAND#44=220/BRAND#55=240 instead of 400 only on 2NG×3rep — the
  same charset-key cross-NG redistribution shape that the GROUP BY type-metadata
  fix (consistent `hashGroupKey` across nodes) and the D25 CONF-correlation fix
  addressed.  Verified: restored `ronsql_cte_ng2r3/t/ronsql_cte_dd_agg.test`,
  recorded green; agg-05 now `SUM(prt.n)=400` for every brand and the ng2r3 agg
  result is byte-identical to base.  `README_D22.txt` removed.
- **W2 — D16: scalar `COUNT(*)` over EMPTY input returns NULL, not 0 — ✅ FIXED.**
  Repro: `body_index.inc` index-17, `body_chain_scalar.inc` cs20.  Symptom was
  precise: RonSQL emitted exactly one scalar row, MIN/MAX/SUM correctly NULL, but
  the COUNT slot read NULL instead of 0 (`NULL NULL NULL NULL` vs MySQL
  `NULL NULL NULL 0`).  Root cause is API-side, not kernel: `NdbAggregator`'s
  scalar (no-GROUP-BY) result record is the pre-initialised `agg_results_` array
  (every slot `is_null=true`), and `FetchResultRecord` always returns exactly one
  scalar record from it.  For a main scalar aggregation reading an EMPTY CTE_SCAN
  (the CTE materialised to zero rows) the kernel sends no scalar group, so the
  COUNT slot's NULL survives.  Non-CTE scalar works only because the kernel
  actively sends `COUNT=0` (its `JoinAggInterpreter::Init` pre-zeroes COUNT, but
  that interpreter never runs on a never-fed empty CTE_SCAN main agg).  The
  GROUP BY path already carried the RONDB-831 COUNT-null→0 fixup; the scalar path
  was missing it.  Fix: `NdbAggregator::PrepareResults` mirrors that fixup for the
  scalar case — any `kOpCount` slot still NULL/UNDEFINED at finalize becomes
  BIGINT-unsigned 0; SUM/MIN/MAX stay NULL.  Mathematically COUNT is never NULL,
  so the fixup is unconditionally correct and a no-op for the already-green
  non-CTE scalar path.  Re-enabled index-17 + cs20; recorded green ×5 topologies.
- **W3 — D15: scale-2 DECIMAL MIN/MAX output drops trailing zeros — ✅ FIXED
  (RonSQL output formatting).**  `print_float_or_double` already uses MySQL's
  `my_fcvt_compact`, so true DOUBLE/FLOAT columns already matched; the divergence
  was only DECIMAL MIN/MAX (widened to DOUBLE via F.1) losing scale (`20055` vs
  `20055.00`).  Fix: carry the source DECIMAL scale onto the virt column
  (`build_cte_virtual_tables` `setScale`), thread it into `PRINT_AGGREGATE`
  (`aggregate_arg_scale`), and format `Double`-with-`scale>0` as `%.*f` in both
  `print_aggregate_result` (re-aggregation path) and `print_passthrough_value`
  (pass-through projection).  Re-enabled: agg-08/09 (MIN/MAX o_totalprice /
  c_acctbal), cs09/cs11 (scalar-CTE pass-through).  **J3 stays disabled — it is
  `SUM(MIN(o_totalprice))`, i.e. SUM over a DECIMAL-derived DOUBLE = the separate
  D1 limitation, not formatting.**  Verified base + (pending) all topologies +
  `ronsql` regression.
- **W4 — D21: partial-key / wrong-column-bound multi-key CTE lookup — ✅
  RESOLVED (re-verify only, no code change).**  Re-verified against current
  binaries (the I.16a/I.16c/I.20 partial-key coverage + the D5 fan-out guard all
  landed AFTER D21 was first recorded), splitting the two halves:
    - **Partial key, root-eligible parent (J19):** `JOIN x ON x.k1 = c.c_custkey`
      binds a strict SUBSET of x's virtual PK (k1,k2), and the bound parent alias
      (`customer c`) is the original query root.  The **I.16c auto-rewrite**
      promotes x to a CTE_SCAN root, sums `x.t` across all k2 values per customer,
      and linearizes the would-be fan-out (x → customer → nation).  RDRS output is
      **byte-identical to MySQL** (verified, empty strict-diff).  So this is
      *supported*, not a bug — re-enabled as a green compare case.
    - **Wrong column (J20):** `JOIN x ON x.k1 = o2.o_custkey AND x.n =
      o2.o_shippriority` binds k1 + the aggregate output `x.n` (not the PK column
      k2).  A non-PK bind can't be a key lookup nor the I.16c rewrite, so RonSQL
      **rejects cleanly at prepare time**: "CTE lookup key references a CTE output
      column that is not part of the virtual primary key."  Re-enabled as a
      `--error 1` rejection-assert.
  D21's original concern ("not rejected, correctness unverified") is answered:
  the partial-key shape is correct via auto-rewrite, the wrong-column shape
  rejects.  Re-enabled `body_joins.inc` J19 (green) + J20 (rejection-assert),
  recorded green ×5 topologies.
- **W5 — D26: flaky wrong COUNT on a multi-node-group composite CHAR key — ✅
  FIXED** (commit `94527c480f3`): a cross-thread data race on the strnxfrm hash
  scratch buffer.  J14/J15 (`GROUP BY o_custkey, o_orderstatus` re-aggregated
  through a multi-key CTE_LOOKUP) failed ~3-6/200 on `ng2r3`, off by one group
  (`25→20`), failing custkey varying per run.  The CTE `JoinAggInterpreter` /
  `AggHashTable` is shared across LDM threads, but `hashKeyFull`'s `strnxfrm_hash`
  wrote into the interpreter's single `m_xfrm_buf` — used concurrently by the
  mutex-free lookup path and the DBSPJ TC-thread routing hash → corrupted bucket
  hash → `find()` miss → INNER join drops a row.  Only multi-col charset keys
  affected (int-only keys use raw `xxhash`).  Fix: `m_xfrm_buf` removed; every
  hash method REQUIRES a per-LDM-thread buffer (`Dbtup::getAggXfrmBuf`,
  compiler-enforced, no default); DBSPJ routes via `cte_lookup_hash_key` using
  block-local `m_buffer0`.  Stress-green 200×/100× on ng2r3/ng2r2/ng4r2.  See
  `cte_test_driven_findings.md` (D26) for the full write-up.

---

## Phase 4 — REMAINING errors / not-yet-implemented

Lower priority; some may be intentional scope limits — confirm before building.

- **E1 — D1: `SUM(<DECIMAL col>)` in a CTE body — ✅ FIXED.**  The SUM
  type-switch in `build_cte_virtual_tables` (and `resolve_chained_column_type`
  for chained CTEs) had no Decimal arm and threw "SUM over this column type in
  CTE not yet supported".  Fix mirrors the existing MIN/MAX DECIMAL widening
  (F.1): scale==0 → BIGINT / Bigunsigned (exact), scale>0 → DOUBLE (best-effort —
  the kernel already widens DECIMAL on load via `AggInterpreter::AlignedType`, so
  SUM accumulates the widened value; no exact DECIMAL accumulation), with the
  `decimal_minmax_fits_64bit` guard.  Display: the source DECIMAL scale must
  reach the printer, which required two more edits beyond the type widening —
  `resolve_cte_output_columns_for_scope` now plumbs SUM's source `dict_column`
  (it previously skipped SUM/COUNT), and `aggregate_arg_scale`/`precision` accept
  `T_SUM` — so a scale>0 SUM prints with the source scale (`15051277.50`) via
  D15's `%.*f` path instead of the compact formatter dropping trailing zeros.
  Re-enabled `body_agg.inc` agg-d1a (scale-2 `SUM(o_totalprice)`), agg-d1b
  (flagship COUNT + scale-2 SUM grouped), agg-d1c (scale-0 `SUM(s_margin)` —
  exact BIGINT) and `body_joins.inc` J3 (SUM of DECIMAL-derived MIN/MAX);
  recorded green ×5 topologies.
- **E2 — D10: `IS NULL` / `IS NOT NULL` in a CTE-body WHERE — ✅ FIXED.**
  `apply_filter`'s switch had no `case T_IS:` arm, so `o_clerk IS NULL` in a
  CTE body (or any single-table scan WHERE) fell to the default throw
  "Non-boolean term in WHERE condition".  Added a `T_IS` arm + `apply_filter_isnull`
  helper that resolves the operand to a stored-table column and emits
  `NdbScanFilter::isnull` / `isnotnull` (the high-level scan filter the existing
  comparison cases already use; `isnull`/`isnotnull` are unaffected by the
  inverted-inequality gotcha that only touches `cmp` LT/LE/GT/GE).  The
  main-query CTE_LOOKUP-output IS NULL path (Phase I.1 `branch_linked_isnull`)
  is unchanged.  Re-enabled `body_filter.inc` filter-10a (CTE-body IS NOT NULL)
  + filter-11a (CTE-body IS NULL), recorded green ×5 topologies.
- **E3 — D11: `GREATEST` / `LEAST` in a CTE-body WHERE term — ✅ FIXED.**
  The grammar previously permitted GREATEST/LEAST only in the top-level SELECT
  scalar position (`arith_expr`), so a WHERE term hit a hard parser "Syntax
  error".  Fix: (1) two `cond_expr` rules (`T_GREATEST/T_LEAST T_LEFT in_list
  T_RIGHT`) building a generic n-ary node — distinct from the `arith_expr`
  aggregation-program rules; (2) a `simplify_ce` boolean rewrite of
  `GREATEST/LEAST(...) <cmp> const`: `GREATEST > / >=` and `LEAST < / <=`
  expand to OR (with per-column `IS NOT NULL` guards, since GREATEST/LEAST is
  NULL when any arg is NULL); the opposite directions expand to AND (already
  NULL-rejecting).  Constant arguments are folded (`GREATEST(col,1) > 2` drops
  the `1 > 2` arm); `=`/`!=` and non-constant comparands are rejected with a
  clean permanent error (the latter avoids generating an unsupported
  column-vs-column CTE-body filter).  Allocation uses the non-throwing
  `ArenaMalloc::alloc` (one batched call, nullptr → caller leaves the node
  unchanged), per the move away from `alloc_exc`.  Re-enabled `body_filter.inc`
  filter-14 (GREATEST `>`, OR + const fold), filter-14b (GREATEST `<=`, AND,
  3-arg), filter-15 (LEAST `<`, OR, two cols), filter-15b (LEAST `>`, AND,
  3-arg); recorded green ×5 topologies.
- **E4 — D17: `MIN`/`MAX` over a DATE column in a CTE — DEFERRED to its own
  phase (NOT contained).**  Investigation (2026-06) found DATE is unsupported at
  *every* layer, not just the agg-program writer:
    - Kernel `AggInterpreterBase::loadColumnTypedFromBuf` has no `NDB_TYPE_DATE`
      arm → returns `ZAGG_LOAD_COL_WRONG_TYPE`; `AlignedType` has no Date mapping.
    - `NdbAggregator::TypeSupported` rejects `Date` → `LoadColumn` returns false →
      the "Failed writing aggregation program" error.
    - No DATE result formatting (a loaded value would print as a number).
  So this is a full new-type feature across kernel + NDB API + RonSQL (needs an
  **ndbmtd rebuild** + 5-topology record), comparable to the string MIN/MAX phase
  (D18 / I.6).  **Integer-day shortcut** (recommended when picked up): add a
  `NDB_TYPE_DATE` arm to `loadColumnTypedFromBuf` that reads the 3-byte packed
  Date as an unsigned int, map `AlignedType(Date)→Bigunsigned` (the packed
  encoding is monotonic, so MIN/MAX reuse the BIGINT compare + wire path),
  accept Date in `TypeSupported`, tag the MIN/MAX output virt-column as DATE in
  `build_cte_virtual_tables`, and unpack the packed integer to `YYYY-MM-DD` in
  `ResultPrinter` (encoding-sensitive — must match NDB's 3-byte Date layout).
  Repro: `body_index.inc` index-9 (main-query MIN/MAX over a DATE virt-column) +
  `body_agg.inc` (MIN/MAX over a DATE column in the CTE body).

---

## Deferred feature work (not bugs — capabilities to add later)

- **F-colvscol — support col-vs-col in a CTE-body WHERE.** Currently rejected
  permanently (H3 fix, `check_no_cte_body_col_vs_col`). To support it:
    1. Stop `build_scan_config_candidates` from treating a comparison whose RHS
       is a column (not a constant) as an index bound — route such conjuncts to
       the residual InterpretedCode filter instead (guards the `encode_constant`
       error path at `RonSQLPreparer.cpp:6033`).
    2. Make the CTE-body scan's `NdbScanFilter` attr-vs-attr program
       (`apply_filter_cmp:8595`) actually terminate on the data node — the D4
       hang showed the attr-vs-attr filter inside a CTE-body materialisation scan
       does not complete, unlike the same emit on a top-level main-query scan.
       Investigate the CTE-body scan / self-join leaf interaction (kernel side).
    3. Remove the `check_no_cte_body_col_vs_col` reject and convert filter-12/13 +
       `ronsql_cte_dd_d4_colvscol` from rejection-asserts back to value-compare
       cases (`ronsql_compare.inc`), re-record across all five topologies.

- **F-fanout — support fan-out aggregation across a real-table/CTE sibling
  (D5).** Currently rejected permanently (H6 fix, `linked_source_is_leaf_ancestor`
  guard).  Today only CTE_LOOKUP siblings are linearized into the agg leaf's
  ancestor chain (`QueryPlanner.cpp` `tree_parent_op_idx` loop, ~:293); a
  real-table sibling under the same parent (e.g. `customer JOIN cust(CTE) JOIN
  nation`) stays a sibling, so an aggregation reading from both children walks
  off the root in `NdbQueryBuilder` `appendLinkedOperand` and aborts.  To
  support it: extend the `tree_parent_op_idx` linearization to also re-parent a
  real-table sibling under the deepest CTE_LOOKUP (or vice versa) so every
  agg-referenced op is on one ancestor chain, and confirm the kernel SPJ
  protocol accepts the resulting topology (key source on the original join
  parent, tree parent on the sibling — the same split already used for chained
  CTE_LOOKUPs).  Then convert `body_joins.inc` J16/J17 from rejection-asserts
  back to value-compare cases and re-record.  (Relates to the RONDB-1044
  star-schema fan-out work.)

---

## Working method

1. Fix one item, re-enable its case(s), `--record`, confirm green on base.
2. Re-run all four topology suites for that family (multi-node regressions like
   D22 only show at ≥6 nodes / specific NG×replica ratios).
3. Update `cte_test_driven_findings.md` (move the row to a "Fixed" section) and
   the `# NEXT-PHASE` marker (delete it once the case is live).
4. Prefer fixing H1 (D3) and E1 (D1) early — they unblock the largest set of
   natural query shapes and may dissolve dependent findings.
