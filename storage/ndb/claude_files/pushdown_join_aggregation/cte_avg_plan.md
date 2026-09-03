# AVG in CTE Outputs (SUM+COUNT slots, redistribute, finalize-divide)

**Status: V1-V5 SHIPPED (`8b8e69d89d4` on RONDB-1107-step4; Test 25
green with Tests 1-24 regression after the single-node completion-arm
fix); Test 26 + the V6 MTR family AUTHORED (pending user build +
ndb_push_agg/_dist runs + first `--record` ×5); docs pending.**

V5/V6 additions (September 2026):
- **Test 26** (testCteNdbApiFilter): AVG at scale — 1500 groups × 2
  rows with exact-in-double averages (avg = g+1); exceeds
  ZCTE_AVG_FINALIZE_BATCH so the owner finalize takes ≥2 CONTINUEB
  slices, and in ndb_push_agg_dist the groups reach the owner through
  redistribute + inbound merge first.  Thresholds pin both
  slice-visited regions (avg>1000 ⇒ COUNT=1002; avg<100 ⇒ COUNT=196);
  restores the canonical 5-row cte_src afterwards.
- **MTR family `body_avg.inc`** (`ronsql_cte_dd_avg` ×5 topology
  suites): avg-01..10 + avg-P1..P3 — pass-through display of AVG(int)
  scale-4 / AVG(DECIMAL) scale-6 / nullable-with-COUNT / mixed-aggregate
  position stability / two-AVG CTEs; aggregate-path consumption via the
  CTE_LOOKUP filter (int and DOUBLE args — the l_tax threshold sits at
  0.045, between the 0.04/0.05 value points, and each group's two
  l_tax rows are identical so double summation order cannot matter);
  scalar all-NULL ⇒ NULL; the AVG watermark comma join (real INT vs
  CTE DOUBLE via typed regs); SUM/MIN/MAX re-aggregation over the avg;
  string/temporal/expression rejection probes.
- Deliberate first-record scope choice: direct display rides
  PASS-THROUGH shapes (CTE_SCAN root, the proven filter-45/gc-9
  envelope) — a main-query `GROUP BY a.av` (grouping by a CTE
  AGGREGATE output) is unproven and is left as a follow-up probe
  rather than gambling a new family's record on it.

Test 25 first-run finding (FIXED): `avg>30` returned COUNT=0 — the
finalize never ran because the SINGLE-NODE completion arm
(`m_cte_num_nodes <= 1`, DblqhMain.cpp ~19519) skips redistribution AND
transitions to CTE_READY inline, bypassing checkCteReady entirely (the
same bypass the single-row-violation check had to duplicate).
Unfinalized BIGINT sums read as doubles are denormals ~1e-322, so the
DOUBLE filter rejected every group — precisely COUNT=0.  Fix: the
single-node success path now saves the sender info into the
`m_cte_complete_*` state fields (as the multi-node path does), sets
`m_cte_redistribution_done`, and calls checkCteReady — whose per-node
loop is a no-op for one node, so it degenerates to the same transition
+ CONF plus the finalize divide with its CONTINUEB slicing.
Preconditions verified: SETUP populates m_cte_node_list with the own
node (loop skips it), Phase L routes COMPLETE to the owner instance
(the ndbassert holds), and no probe can arrive before the CONF so the
SENDING_RESULTS window during a sliced chain is invisible.

V4 outcome notes (checklist C1-C9 all done):
- C1: `X(Avg)` in FORALL_AGGS + `ucasestr_Avg` — all six compiler
  switches extended by the name-generic AGG_CASE macros as predicted.
- C2: CTE-scope registration → single `agg->Avg(arg)` slot
  (`agg_index_count` aliased; main scope keeps Sum+Count+PRINT_AVG).
- C3/C4: `programAggregator_join` Avg → `aggregator->Avg(dest, src)`;
  single-table translation gets a report-a-bug arm.
- C5: virt-table AVG arm → DOUBLE, nullable; display metadata
  scale/precision per MySQL's rule (int ⇒ 4/15; DECIMAL(p,s) ⇒
  min(s+4,30)/min(p+4,65) — the printer's `ndbrequire(scale<=30)` and
  `<=65` bounds made the caps load-bearing, not cosmetic); plain-column
  arg gate, I.22 64-bit guard on the hidden sum, clean string/temporal
  rejections.
- C6: CTE_LOOKUP filter accept arm for Type::AVG (finalized DOUBLE via
  the inline path; virt-type require as belt-and-braces).
- C7: `resolve_chained_column_type` AVG arm (same typing/scale rules;
  unsupported types return false — C5 raises the clean messages).
- C8: both GROUP-BY-column Double print arms (buffered + streaming)
  gained the fixed-scale `%.*f` treatment (gated `sc>0 && 0<pr<=15`,
  mirroring print_passthrough_value which already had it);
  `build_result_column_metadata` computes AVG metadata from the
  resolved chained type when `ref.dict_column` is NULL (the
  source-column plumb-back is deliberately NOT used for AVG — its
  display scale is argument-DERIVED, not the source's).  Main
  aggregates over an avg output pick up scale 4 via the existing
  `aggregate_arg_scale` metadata reader for free.
- C9: folded into C5 (arg gate, I.22, string/temporal); scalar-CTE
  candidacy (:5158) already counted AVG as an aggregate; the
  column-ref marker and the main-scope EXPLAIN "CLIENT-SIDE
  CALCULATION" print were already AVG-aware (main-only).
- `raw_word_size` needs nothing (Avg = 1 word, default arm).

V1-V3 outcome notes:
- `kOpAvg` appended to the InterpreterOp enum (wire-stable).  Exec arm
  in `AggInterpreterBase::executeStandardOpcode`: Sum kernel into the
  visible dst + Count kernel into the hidden companion via
  `m_avg_hidden_map` (base members `m_n_visible_results` /
  `m_n_hidden_slots` / `m_avg_finalized`; map nullptr on
  AggInterpreter ⇒ clean ZAGG_OTHER_ERROR reject).
- `JoinAggInterpreter::Init`: program walk assigns hidden slots at the
  END of the slot array (m_n_agg_results becomes TOTAL — layout, merge,
  redistribute, teardown cover hidden slots automatically;
  `n_visible_results()` keeps the header count), validates dst
  range/duplicates/MAX cap; map lives in the buf-block tail ahead of
  `m_cached_agg_ops` (extra_tail_bytes bumped).  Scalar empty-input
  pre-init walk gained a kOpAvg arm (hidden count ⇒ 0 ⇒ finalize NULL,
  MySQL AVG-over-empty).  `extractAggOps` marks dst=kOpSum +
  hidden=kOpCount so `aggMergeNumericSlot` needs zero changes;
  `setTotalAggResults` (multi-leaf) requires no hidden slots.
- Finalize (**CONTINUEB-sliced per maintainer direction** — the group
  hash can hold millions of groups, so the walk must yield at
  real-time breaks; safe because the table is immutable in the
  FINAL_REP..CTE_READY window and the chain stays on the owner
  instance): file-static `finalizeAvgSlotArray` +
  `JoinAggInterpreter::finalizeAvgSlotsSlice(max_groups)` (scalar
  array in one shot; grouped walk resumes via the
  `GBHashTable::iteratorAt` cursor saved in
  `m_avg_fin_bucket/m_avg_fin_raw`; BIGINT signed/unsigned/DOUBLE sums
  ⇒ DOUBLE; count==0/NULL ⇒ NULL).  `Dblqh::checkCteReady` runs the
  first slice after the per-node FINAL_REP barrier and before the
  CTE_READY store; incomplete slices chain through
  `ZCONTINUE_CTE_AVG_FINALIZE` (52) → `continueCteAvgFinalize`
  (state-key lookup with aborted/CTE_READY drop guards,
  `ZCTE_AVG_FINALIZE_BATCH` = 1024 groups/slice) which re-calls
  checkCteReady on completion; `avgFinalizing()` makes re-entrant
  checkCteReady invocations (duplicate FINAL_REP) return while a chain
  is in flight.
- Visible-count sweep: `buildCteLinkedBuffer`, `cteLookupEmitResult`,
  `cteScanEmitResults` (grouped + scalar arms) flip to
  `n_visible_results()`; redistribute serializers stay `val_len()`
  (TOTAL — hidden slots ship to the owner by design); the main-agg API
  drain (`continueJoinAggMerge/Send`) stays TOTAL — main programs carry
  no kOpAvg in v1 (RonSQL uses Sum+Count + PRINT_AVG there).
  `OptimizeProgramBuffer` passes kOpAvg through untouched (default arm,
  single word) — verified, no change.
- API: `NdbAggregator::Avg(agg_id, reg_id)` with Sum's string/temporal
  rejections; the API-side agg_ops_ consumers are benign for kOpAvg
  (string merge never sees numeric AVG; RONDB-831 COUNT-fixup skips it,
  correctly leaving AVG NULL over empty).
- Block test: testCteNdbApiFilter **Test 25** — GROUP BY grp, AVG(val)
  over the 5-row cte_src (avgs 15/35/50), CTE_LOOKUP filter on the
  finalized DOUBLE via the inline const path, two thresholds pinning
  both sides of the divide (avg>30 ⇒ COUNT=3, avg<20 ⇒ COUNT=2).

Adds `AVG(col)` as a CTE output (and, as a cheap companion, main-level
AVG on the join path).  Today rejected at `build_cte_virtual_tables`
(RonSQLPreparer.cpp:10104, "AVG in CTE output not yet supported.") and
excluded from the CTE_LOOKUP filter accept-list (:10873).

## Design (the maintainer's sketch, confirmed against the machinery)

AVG is not commutative, but SUM and COUNT are — so an AVG output is
carried as **two slots through the entire distributed pipeline** and
divided exactly once, after all merging is complete:

1. **Per-row**: `AVG(x)` executes as plain SUM(x) + COUNT(x) using the
   existing numeric kernels (COUNT skips NULL x, the D2-verified
   behavior), into one *visible* slot (the SUM) and one *hidden*
   companion slot (the COUNT).
2. **Merge + redistribute**: both slots ride the existing machinery
   unchanged — thread-merge (`mergeFrom`, mutex-free early inbound
   merges), `JoinAggRedistributeReq` (including the scalar
   `keyLen == 0` I.17e variant), `aggMergeNumericSlot` — because they
   are ordinary SUM/COUNT slots and both are commutative.
3. **Finalize (the "last step")**: on the owner, at `checkCteReady`
   time — after the FINAL_REP barrier says every node's redistribute
   contribution has arrived, before CTE_READY is reported — a one-time
   O(groups) pass divides: visible slot ← `double(sum) / double(count)`
   typed DOUBLE, or NULL when count == 0 (AVG over all-NULL values,
   matching MySQL).

After finalize, every downstream consumer sees a single ordinary
DOUBLE slot and needs **zero changes**: CTE_LOOKUP probes, the
jump-table filter (const-vs-col via the inline DOUBLE path;
col-vs-col via typed registers — `t.col > cte.avg` watermarks work for
free), `kOpLoadCol` linked loads, `buildCteLinkedBuffer`,
`emitCteGroupOutput`, scalar emission, and re-aggregation by main
SUM/MIN/MAX over the avg output.

### The vehicle: one new aggregation opcode `kOpAvg(dst_slot, reg)`

Self-contained, no separate program-metadata section:
- **Init** (program scan): allocates the hidden COUNT companion at the
  END of the slot array — `total_slots = n_agg_results + n_hidden` —
  so visible slot indices stay stable for every position-indexed
  consumer (virt columns, filter positions, linked-load positions,
  redistribution layout).  Registers the (sum_slot, count_slot) pair on
  the interpreter's finalize list.
- **Per-row**: executes the existing Sum and Count kernels on the same
  register.
- Validator accept-list entry; JoinAggInterpreter only in v1 (the
  normal-scan single-table path already supports AVG end-to-end via the
  old AggregationAPICompiler decomposition + `PRINT_AVG` client-side
  division — untouched).

No version gate (26.04 alpha precedent).

### What already exists (audit)

- RonSQL's front end **already decomposes AVG**: `Outputs::Type::AVG`
  with `avg.agg_index_sum` / `avg.agg_index_count`
  (RonSQLPreparer.cpp:563 main scope, :888 CTE scope) and the
  `ResultPrinter` has `PRINT_AVG` (sum/count division at print).  The
  old single-table path supports AVG today; only the pushed
  join/CTE path (NdbAggregator programs) rejects it.
- MySQL-handler AVG (`next_steps.md` Phase 15) uses the same
  decomposition with handler-side division — the API-side twin of this
  design; unaffected here but unblocked by the same NdbAggregator work
  if it ever pushes AVG.

### Result type and display parity (the known risk)

Kernel result type is DOUBLE.  MySQL displays `AVG(exact-type)` (INT,
DECIMAL) as DECIMAL with scale+4 computed in exact arithmetic, and
`AVG(FLOAT/DOUBLE)` as double.  RonSQL will tag AVG outputs
(`ColumnMetadata::is_avg` + source-arg exactness, the D17 `is_date`
precedent) and format exact-arg AVG with 4 fraction digits from the
double value.  Double division can differ from MySQL's exact decimal
arithmetic in the last digit for large sums — the same artifact class
as the retired D7–D9 SUM-over-DECIMAL findings.  Mitigation: test data
in friendly value domains; `canonicalization_*` hooks where needed;
documented as an accepted v1 deviation (full DECIMAL arithmetic is
Phase 16, out of scope).

## V4 research findings (September 2026) — implementation checklist

The compiler is macro-generated end to end, so the RonSQL wiring is far
smaller than expected:

- **C1 `AggregationAPICompiler`**: add `X(Avg)` to `FORALL_AGGS` —
  this auto-generates `AggType::Avg`, `SVMInstrType::Avg`, the public
  `Uint32 Avg(Expr*)` builder, AND extends all six internal switches
  (svm_execute validation, compile() dest-ordering check, pushInstr
  AggType→SVMInstrType mapping, dead-code elimination, the
  `print_aggregate` "Avg(...)" form, the SVM program printer) because
  every one uses a name-generic `AGG_CASE` macro over `FORALL_AGGS`.
  One manual companion: `static const char* ucasestr_Avg = "AVG";`
  (:1211-1214 block) for the SVM printer's `ucasestr_##Name`.
- **C2 CTE registration (:888)**: `analyze_ctes`' AVG arm switches from
  `Sum(arg)+Count(arg)` (two slots — the exact reason the virt-table
  build had to reject AVG: two slots per one output column) to a single
  `Avg(arg)`; `agg_index_count` aliased to `agg_index_sum` (its
  PRINT_AVG readers are main-scope-only).  Main scope (:563) keeps
  Sum+Count + PRINT_AVG — that's the already-working single-table AVG
  and never generates an SVM Avg instr.
- **C3/C4 translation switches**: `programAggregator_join` (:13494)
  gains `case SVMInstrType::Avg → aggregator->Avg(dest, src)`;
  `programAggregator` (:12939, single-table path) gains a defensive
  unreachable arm (main scope never emits Avg instrs).
- **C5 virt table (:10103)**: replace the AVG throw with
  `derived_type = Double` (nullable, standard attr-size tail — the I.7
  canonical pattern already applies to all arms).  Slot mapping stays
  1:1 automatically: one agg index per output, allocated in outputs
  order like SUM.
- **C6 filter accept-list (:10873)**: add `fun == T_AVG` (the DOUBLE
  inline path already accepted for the virt type).
- **C7 typing/scale**: `resolve_chained_column_type` gains an AVG arm →
  Double with `out_scale` = MySQL's display scale (int arg ⇒ 4,
  DECIMAL(p,s) arg ⇒ s+4, FLOAT/DOUBLE arg ⇒ 0).  KEY FINDING:
  `print_aggregate_result` ALREADY formats `Double + scale>0` as
  `%.*f` (the F.1 DECIMAL-widening path) — AVG display parity rides
  the existing scale plumbing, no new metadata field needed.
- **C8 print gaps**: `print_passthrough_value` / the GROUP-BY-column
  Double print arms ignore metadata scale — add the `%.*f` treatment
  there (also closes a latent F.1 gap: pass-through printing of
  DECIMAL-widened MIN/MAX outputs).
- **C9 validation**: AVG's arg needs the same plain-column CTE-body
  gate as SUM (previously the virt-table throw masked it), the
  I.22 scale-zero DECIMAL 64-bit range guard extends to AVG's hidden
  sum (same overflow), and string/temporal args get clean prepare
  rejections ahead of the NdbAggregator::Avg API errors.

## Work items

- **V1 — kernel opcode + hidden slots.**  `kOpAvg` in
  `NdbAggregationCommon.hpp` + `AggInterpreterBase` (Init allocation,
  per-row arm in `executeStandardOpcode` or a JoinAgg-local arm,
  `validateEmbeddedProgram`/`scanAndValidateEmbeddedPrograms` accept).
  **Slot-count audit** — the critical correctness sweep, same bug class
  as the D6/D25 series: every site sized by "number of aggregate
  results" must be classified total-slots (group record allocation /
  chunk sizes, merge loops, redistribute serialization both keyed and
  scalar, teardown) vs visible-only (API result emission,
  `buildCteLinkedBuffer`, `emitCteGroupOutput`, probe/filter/linked
  positions, string-slot handling).
- **V2 — finalize pass.**  `finalizeAvgSlots(state)` walking the
  owner's group hash at `checkCteReady` (post-FINAL_REP, pre-CTE_READY):
  divide into the visible slot as DOUBLE, count==0 ⇒ NULL flag.
  Idempotency with Phase L's checkCteReady re-entry protection
  (finalize exactly once — hang the flag off the state).  Scalar CTEs:
  same pass over the single record.
- **V3 — NdbAggregator API.**  `Avg(result_index, reg)` builder
  emitting `kOpAvg`; `TypeSupported` reuse of the Sum envelope
  (DECIMAL widened per F.1, Date rejected like Sum-over-Date); result
  parse unchanged (finalized slot arrives as an ordinary DOUBLE).
- **V4 — RonSQL.**  Lift the :10104 rejection (virt column DOUBLE +
  `is_avg` metadata plumbed through
  `resolve_cte_output_columns_for_scope`); `programAggregator_join`
  emits `Avg` for `Type::AVG` CTE outputs; the :10873 filter
  accept-list gains T_AVG (DOUBLE inline path); ResultPrinter
  formatting arm (exact-arg ⇒ 4 fraction digits, approx-arg ⇒ double).
  Main-level AVG on the join path (over real leaves or CTE outputs):
  emit Sum+Count slots and reuse `PRINT_AVG` — API-side division, no
  kernel dependency; include if the program-builder change is as small
  as it looks, else split out.
- **V5 — block tests.**  testCteNdbApi/testJoinAggNdbApi AVG cases:
  multi-node redistribute correctness (the reason this design exists),
  all-NULL group ⇒ NULL, scalar AVG CTE, AVG output consumed by a
  CTE_LOOKUP filter and by a main aggregate.
- **V6 — MTR ×5 topologies.**  `body_agg.inc` (or a new family) —
  AVG(INT), AVG(DECIMAL) with formatting pins, AVG(DOUBLE),
  AVG(nullable col) incl. an all-NULL group, AVG + GROUP BY re-joined
  and re-aggregated (`SUM(cte.avg)`), avg in main WHERE (const compare
  + watermark + avg-vs-avg col-vs-col), scalar AVG CTE cross-join,
  multi-key CTE with AVG, D2-style COUNT semantics cross-check.
  Display-parity strategy per the risk note.

## Deliberately out of scope

- Exact DECIMAL AVG arithmetic (Phase 16 territory).
- MySQL-handler AVG pushdown (next_steps Phase 15 — same building
  blocks, separate consumer).
- AVG over strings/temporals (rejected like Sum today; MySQL's
  AVG(date) numeric coercion is out of envelope).
- Normal-scan (non-join) kernel AVG — already served by the old path.
