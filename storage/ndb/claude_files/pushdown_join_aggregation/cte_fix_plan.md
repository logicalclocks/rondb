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
- **H5 — D20: multi-key complete-key CTE lookup — ✅ NOT-REPRODUCIBLE
  (2026-06-08).** Executes correctly on the current build in both predicate
  orders (verified J15 reversed; J14 in-order re-enabled). Another stale-binary
  artifact like D19. Re-enabled green: `body_joins.inc` J14 + J15.
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

## Phase 2 — CRASHES (next)

Both have preserved evidence; reproduce under a debug data node and walk the
trace.

- **C1 — D6: DBSPJ ndbassert `Error 2343, DbspjMain.cpp:7825,
  requestPtr.p->m_cnt_active == 0`** when a CTE keyed by a high-cardinality key
  (l_orderkey/1500) is used as a CTE_LOOKUP child of a large `orders` root scan,
  OR as a high-cardinality CTE_SCAN root. The near-identical low-cardinality
  shape (o_custkey/300 → small `customer`) is GREEN, so the trigger is
  cardinality / multi-batch teardown. Repro: `body_agg.inc` agg-06 marker (lookup
  form) and agg-19/20/21 marker (CTE_SCAN-root form). Evidence:
  `findings/crash_artifacts/ndb_{1,2}_error.log`. Start at `DbspjMain.cpp:7825`
  and the active-operation accounting on request teardown when child batches span
  SCAN_NEXTREQ cycles.
- **C2 — D18: data-node crash `NDB Error 6000 / Signal 6 abort`** when the main
  query RE-AGGREGATES a string (CHAR/VARCHAR) MIN/MAX CTE output (numeric MIN/MAX
  re-aggregation is GREEN). Repro: `body_agg.inc` agg-11..15 marker. Suspect: the
  linked string-aggregate delivery/merge path (Phase I.6 F.4 —
  `cte_filter_phase_i6_string_minmax_f4.md`: `Dblqh::buildCteLinkedBuffer` /
  `emitCteGroupOutput`, `JoinAggInterpreter::kOpLoadCol` string load, string-slot
  redistribution). A string aggregate consumed as a *linked* input to the main
  aggregator likely mis-sizes or mis-points the payload.

Exit criteria: re-enable agg-06/11–15/19–21 → green; re-run all topologies.

---

## Phase 3 — WRONG RESULTS / semantics

- **W1 — D22: wrong COUNT under the 2 node-group × 3-replica (6-node) topology
  only.** `SUM(prt.n)` (a COUNT fan-in) returns e.g. BRAND#44=220/BRAND#55=240
  instead of 400; passes on 2/3/4/8 nodes. Repro: `ronsql_cte/include/body_agg.inc`
  agg-05 (currently omitted from `ronsql_cte_ng2r3`). Suspect: CTE_LOOKUP fan-out
  / `continueJoinAggRedistribute` under the specific node-group×replica geometry
  — a per-fragment/owner accounting that's only wrong when NG≠replicas in this
  ratio. Reproduce by running the base agg suite against a 2NG×3rep cluster.
  Re-enable: restore `ronsql_cte_ng2r3/t/ronsql_cte_dd_agg.test` + `.result`,
  delete `README_D22.txt`.
- **W2 — D16: scalar `COUNT(*)` over EMPTY input returns NULL, not 0.** Repro:
  `body_index.inc` index-17, `body_chain_scalar.inc` cs20 markers. SQL requires
  COUNT=0 (SUM/MIN/MAX=NULL) on empty. Likely the empty-group/no-rows finalize in
  the aggregation interpreter emits NULL uniformly. Smaller, well-scoped fix.
- **W3 — D15: scale-2 DECIMAL MIN/MAX output drops trailing zeros** (`20055.00`
  → `20055`). Values are correct — only display scale is lost. Cheapest win:
  carry the source DECIMAL scale through the virt-table type so the result
  printer formats with fixed scale (mirrors the F.1 widening metadata). Repro:
  `body_agg.inc` agg-01/08/09, `body_joins.inc` J3, `body_chain_scalar.inc`
  cs09/cs11. (Alternatively the suite can canonicalize, but fixing the format is
  the right outcome.)
- **W4 — D21: partial-key / wrong-column-bound multi-key CTE lookup is NOT
  rejected** (it executed and returned rows) — value-correctness vs MySQL is
  unverified. Repro: `body_joins.inc` J19/J20 (removed). Decide: either reject
  cleanly (as `ronsql_cte_partial_key.test` does for the simple case) or support
  it and verify correctness. Couple this with H5/D20.

---

## Phase 4 — REMAINING errors / not-yet-implemented

Lower priority; some may be intentional scope limits — confirm before building.

- **E1 — D1: `SUM(<DECIMAL col>)` in a CTE body** — `RonSQLPreparer.cpp:7069`
  SUM type switch has no Decimal/Decimalunsigned arm (falls to the throw at
  :7090). Add arms widening to Bigint/Bigunsigned/Double mirroring
  `AggInterpreter::AlignedType` and the F.1 DECIMAL widening. Unblocks the most
  natural CTE backbone (`SUM(price)`). Repro: `body_agg.inc` D1 marker.
- **E2 — D10: `IS NULL` / `IS NOT NULL` in a CTE-body WHERE** —
  `RonSQLPreparer.cpp:5934` `apply_filter` switch has no `case T_IS:` (hits
  :8507). Add a `T_IS` arm emitting `branch_col_eq_null` / `branch_col_ne_null`.
  Already supported on the main-query CTE_LOOKUP output, so reuse that lowering.
  Repro: `body_filter.inc` D10 marker.
- **E3 — D11: `GREATEST` / `LEAST` in a CTE-body WHERE term** — grammar permits
  them only in the top-level SELECT scalar position (`RonSQLParser.y`). Extend
  the conditional-expression grammar + the WHERE codegen. Repro: `body_filter.inc`
  D11 marker.
- **E4 — D17: `MIN`/`MAX` over a DATE column in a CTE** — "Failed writing
  aggregation program." Add DATE handling to the CTE aggregation-program writer
  (treat as the underlying integer day value, preserve DATE type out). Repro:
  `body_index.inc` D17 marker (and the `body_agg.inc` DATE MIN/MAX probe).

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
