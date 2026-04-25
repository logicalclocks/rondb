# RonSQL wiring for NDB-API CTE features

> **Portable plan file.** This document is the canonical plan for the RonSQL
> CTE wiring work on branch `RONDB-1050-cte-filter`. It mirrors the local
> plan-mode file at `~/.claude/plans/purring-prancing-robin.md` but lives in
> the tree so the session can be resumed on any machine after a `git pull`.
>
> Related documents in the same directory:
> - `cte_filter_plan.md` + `cte_filter_phase_{a,b,c}.md` — NDB-API layer
>   (DBLQH filter / interpreter) that this plan consumes.
> - `cte_nextreq_plan.md` + `cte_nextreq_phase_{1,2,3,4}.md` —
>   SCAN_NEXTREQ multi-batch flow for CTEs.
> - `cte_outer_join_plan.md` + `cte_outer_join_phase_{1..5}.md` —
>   CTE outer-join support at the NDB-API/DBSPJ/DBLQH layer.
>
> This plan covers the RonSQL client-side wiring only. When in doubt about
> NDB-API-level behavior, those referenced documents and the
> `storage/ndb/block_unit_test/testCteNdbApi*.cpp` tests are authoritative.

## Context

RonDB (branch `RONDB-1050-cte-filter`) has landed five new CTE capabilities in the
NDB API, DBSPJ, and DBLQH: `scanCte` / `lookupCte` operation kinds, interpreted-code
filter on CTE rows, CTE in LEFT OUTER joins (via `MatchAll`), `SCAN_NEXTREQ`
multi-batch iteration, and aggregation-over-CTE via `NdbQueryOptions::setAggregation`.
Reference skeletons live in `storage/ndb/block_unit_test/testCteNdbApi*.cpp`.

RonSQL already has the upstream plumbing for CTEs: parser (`RonSQLParser.y:274-295`),
AST (`RonSQLCommon.hpp:254-258`), planner (`JoinOp::CTE_LOOKUP` in
`QueryPlanner.hpp:43`), semantic checks (`analyze_ctes()`,
`RonSQLPreparer.cpp:2564-2614`), virtual-table construction
(`RonSQLPreparer.cpp:4361-4395`), and `lookupCte` emit
(`RonSQLPreparer.cpp:4533-4542`). But the guard at
`RonSQLPreparer.cpp:4032-4037` throws `"CTE execution is not yet implemented"`
before any CTE query can run, and four NDB-API calls are never emitted anywhere
in RonSQL: `beginCteSubtree`, `endCteSubtree`, `defineCte`, `scanCte`
(verified by grep).

This plan wires RonSQL through to the new NDB-API CTE primitives and adds
matching test coverage.

## Current status (2026-04-24)

- **Phase A (subtree emit, CTE bodies, virt-table schema): DONE** — commits
  `aebf69d6c2b` … `67a83950c16`.
- **Phase A0 (guard main-query aggregation over CTE output): DONE** —
  `702a356d520`; effectively relaxed for shapes the NDB API supports.
- **Phase B.1 (first green MTR): DONE** — commit `5cd9600c6c4` lands all
  four Tests 1–4 in `ronsql_cte_basic.test` green with three fixes:
  (1) CTE-output linked-position offset in `programAggregator_join`,
  (2) `cteVirtualTables[]` lifetime until after `print_result`,
  (3) new `build_cte_linked_projections()` for CTE-scope parent-linked
  GBs. Result file recorded.
- **Phase B.2a (two independent CTEs): DONE** — three fixes:
  (1) non-leaf CTE Load dispatch in `programAggregator_join` —
  `column_map[src]` is NULL for CTE output columns, so resolve the
  descriptor from `cteVirtualTables[src_op_idx]`;
  (2) symmetric non-leaf CTE GroupBy dispatch;
  (3) topology fix — new `JoinOp::tree_parent_op_idx` field; planner
  chains sibling CTE_LOOKUPs under the same non-CTE parent (later CTE
  gets `tree_parent_op_idx = previous_cte_idx`, `parent_op_idx` /
  key source unchanged); emit calls `opts.setParent(opDefs[tree_parent])`
  on the override so the main aggregator on the deepest CTE reads
  earlier CTE outputs as ancestor-linked projections.
  Materialization stays parallel (`defineCte` `depMask` untouched).
- **Phase B.2b (CTE-body WHERE): DONE** — fix in the single-table
  CTE body emit path (`cp.num_ops == 1` branch, ~4579). That path
  hand-rolls `scanTable(srcTab)` + self-lookup `readTuple` and skipped
  attaching `cs.join_where_ce[0]` (which `build_cte_scopes` had
  already populated via `classify_where_by_table`). `emit_root_op`
  handles this for multi-op bodies; the single-table path now builds
  the `NdbScanFilter` + `NdbInterpretedCode` inline and passes
  `rootOpts.setInterpretedCode` to `scanTable`.
- **Phase B.2c (VARCHAR GB column in CTE body): DONE** — fix is
  a new `resolve_cte_output_columns()` pass after `build_cte_scopes`
  that fills `m_main_scope.column_map` for CTE output references
  (`load_join` leaves them NULL because CTE cols have no real-table
  column). For `Outputs::Type::COLUMN` and `Outputs::Type::AGGREGATE`
  MIN/MAX, it plumbs the CTE body's source column through so
  ResultPrinter can read charset/precision/scale. SUM/COUNT outputs
  stay NULL (charset-irrelevant for numeric types;
  ResultPrinter's existing fallback handles it).
- **Phase B.2d (COUNT(*) + SUM(cte.col) in main): DONE** — no emit
  changes required. `COUNT(*)` slot interleaves cleanly with
  `LoadLinkedColumn(cte_base_pos + ..., vtcol)` for the CTE-backed
  SUM; Phase B.1's `cte_base_pos` offset did not disturb non-linked
  slot numbering.
- **Phase B.2 COMPLETE** — all four shapes (B.2a/b/c/d) green in
  `ronsql_cte_basic.test`.
- **Phase C (CTE_LOOKUP filter, column-only): DONE** — new
  `emit_cte_lookup_filter` helper builds an `NdbInterpretedCode`
  using `branch_linked_mem_*`. Filter pushdown applies to CTE
  outputs that are direct column projections
  (`Outputs::Type::COLUMN`); for those we resolve the SOURCE real
  column from the CTE body's scope so DBTUP can look up the
  type descriptor via `tablerec[tableId]`. Aggregate outputs
  (SUM/MIN/MAX/COUNT) produce synthesized types whose descriptors
  aren't registered in NDB — those filter shapes throw a clear
  `require_prm` error and are deferred to the follow-up
  inline-type-opcode phase (see below).
- **Phase C-followup (inline-type opcode for CTE filters):
  PLANNED.** Add a new DBTUP opcode that carries
  `[typeId, length, charsetPos]` inline rather than
  `[tableId, schemaVersion, attrId]`. Lifts Phase C's
  column-only restriction; lets RonSQL filter on synthesized
  aggregate outputs. One new handler in `s_cte_filter_handlers`
  + one new client-side emitter on `NdbInterpretedCode`.
- **Phase D (CTE_LOOKUP in LEFT OUTER JOIN): DONE** — no code
  changes. Test 10 in `ronsql_cte_basic.test` adds a 4th customer
  (Dave / c_id=400) with no orders and runs `LEFT JOIN sums ON
  sums.k = c.c_id GROUP BY sums.k`. The unmatched row produces a
  k=NULL group with SUM=NULL — confirms agg-feed NULL injection
  (server-side commit `47d81b43903`) flows through RonSQL's
  per-column Load dispatch (Phase B.1) without further wiring.
- **Phase C–H, Phase P-GB: NOT STARTED.** Phase C has the virtual-table
  prerequisite; Phase P-GB (DBLQH `buildCteLinkedBuffer` fix to uniformly
  prefix step-1 parent-linked entries) should land before Phase D so
  `LEFT JOIN cte` can SELECT parent columns.

Working tree: clean (Phase B.1 push done).

## Scope (confirmed)

**In scope.** CTE NDB-API features only: CTE materialization (subtree + defineCte),
`lookupCte` with filter, `lookupCte` in LEFT OUTER JOIN, `scanCte` as main-query
root, SCAN_NEXTREQ multi-batch, and aggregation-over-CTE where the NDB API
supports it today.

**Out of scope (reject cleanly with `RonSQLPermanentError`):**
- CTE_SCAN as outer-join child — NDB API Phase 3 (= `next_steps.md` 6b) dropped.

CTE_LOOKUP agg-feed NULL injection inside an enclosing CTE agg subtree
was the other shape historically flagged as out of scope, but it was
shipped on this branch in commit `47d81b43903` (Phase 5 /
`cte_outer_join_phase_5.md`). RonSQL can support this shape without a
client-side guard.

**Out of scope (independent tracks, untouched):** AVG, DECIMAL precision,
expression GROUP BY, post-aggregation expressions (next_steps.md Phases 15-20);
Phase 7 open subquery steps 42, 43, 45.

## Architectural decisions

1. **CTE inner body is a full SELECT.** `analyze_ctes()`
   (`RonSQLPreparer.cpp:2564-2617`) only enforces GROUP BY + ≥1 aggregate +
   FROM + unique name. It does NOT restrict the CTE's body to a single table.
   A CTE's `stmt` is a full `SelectStatement` with `join_list`, WHERE,
   HAVING, etc. The NDB API matches this: `beginCteSubtree`/`endCteSubtree`
   wraps an arbitrary multi-op sub-tree
   (`NdbQueryBuilder.cpp:1217-1269`), with the CTE's aggregator on the
   designated leaf.
2. **CTEs can reference other CTEs in their FROM.** Chained CTEs are
   supported: `b`'s subtree may contain a `lookupCte(a)` or `scanCte(a)` as
   one of its ops (`NdbQueryBuilder.cpp:1188-1193`). No extra NDB-API work
   beyond correct build-order (declaration order).
3. **Reuse QueryPlanner + emit recursively per CTE.** For each CTE:
   (a) plan its inner stmt with `QueryPlanner::plan()`, producing a
   sub-`JoinPlan`;
   (b) inside `beginCteSubtree(cteId)` / `endCteSubtree()`, emit that
   sub-plan using the same op-emit logic the main query uses (scan root +
   child loop with linked keys + filters + `CTE_LOOKUP`/`CTE_SCAN` children
   when chained);
   (c) attach `setAggregation(cteAgg)` on the sub-plan's `agg_leaf_idx` op;
   (d) call `defineCte(cteId, srcTab, cteAgg)`.
   To do this cleanly, factor the existing child-loop + root-scan emit
   in `execute_join()` (`RonSQLPreparer.cpp:4246-4548`) into a helper
   that takes a `JoinPlan&` and an optional `NdbAggregator*`, callable from
   both main-query and CTE-subtree contexts. Likewise factor per-op state
   (`m_join_where_ce`, `m_column_table_idx`, etc.) so per-CTE scopes can
   carry their own arrays.
4. **Virtual table schema.** Today the virtual table at
   `RonSQLPreparer.cpp:4377-4391` carries only the parent PK columns for
   linked-key binding. Extend with the CTE's result columns (GROUP BY
   outputs + aggregate outputs) so WHERE filters on CTE columns (Phase C)
   and `scanCte` root columns (Phase E) can reference them. Same virtual
   table is reused for `lookupCte` (as child) and `scanCte` (as root of
   main or as child inside another CTE).
5. **Main aggregator over CTE leaf.** `plan.ops[agg_leaf_idx].table` is `NULL`
   for CTE ops (`QueryPlanner.hpp:47`); `RonSQLPreparer.cpp:4050` will
   NULL-deref the instant the guard is removed. Guard the construction and
   reject re-aggregation over CTE output in Phase B with
   `"main-query aggregation over CTE output not yet supported"`; revisit
   in Phase E for the scanCte-root + main-aggregate case.
6. **Reject-cleanly location.** All NDB-un-shipped-shape rejections go into
   a new `validate_cte_execution_shapes()` pass called from `prepare()`
   right after `analyze_ctes()`.
7. **No batch-size tuning.** Leave NDB API default batch size; multi-batch
   correctness is a test concern, not a RonSQL emit concern.

## Phases

### Phase A — CTE materialization subtree emit (multi-table + chained)

**Goal.** For each CTE, recursively plan its inner stmt and emit the
resulting sub-tree inside `beginCteSubtree`/`endCteSubtree`, with the CTE's
aggregator on the designated leaf, then call `defineCte`. Handles
multi-table joins inside the CTE and chained CTEs (b's body references a).

**Files.**
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` — refactor `execute_join()` to
  factor the root-scan emit block (lines 4246-4358) and the child-loop
  (lines 4398-4548) into helpers that take a `JoinPlan&` and optional
  `NdbAggregator*` aggLeafAgg, callable from both main-query and
  CTE-subtree contexts. Candidate signatures:
  - `emit_root_op(NdbQueryBuilder*, JoinPlan&, NdbQueryOperationDef** opDefs,
    NdbAggregator* rootAgg)` — emits the root scan/index-scan (or
    `scanCte` from Phase E).
  - `emit_child_ops(NdbQueryBuilder*, JoinPlan&, NdbQueryOperationDef** opDefs,
    NdbAggregator* leafAgg, ConditionalExpression** perOpWhere,
    NdbDictionary::Table** cteVirtualTables)` — emits child
    `readTuple`/`scanIndex`/`lookupCte`/`scanCte` ops with filters and
    linked keys.
- `storage/ndb/src/ronsql/QueryPlanner.cpp` — add `plan_for_cte(const
  CteDefinition*, JoinPlan& out)` (or equivalent) that runs the existing
  planner logic over a CTE's inner stmt, producing a standalone
  sub-`JoinPlan`. Must already be able to resolve CTE references in the
  inner stmt's FROM (for chained CTEs — see Phase E's planner change; that
  change must be general, not main-query-only).
- New helper `programCteAggregator(const CteDefinition*, NdbAggregator&)`
  that emits `GroupBy` + aggregate loads + `Finalize` over the CTE's
  output list, reusing `AggregationAPICompiler` logic.
- Per-CTE column/WHERE analysis: each CTE needs its own
  `column_table_idx` / `join_where_ce` arrays since they reference the
  inner stmt's tables, not the outer. Keep these in a stack/vector of
  per-scope contexts.

**Key emit (pseudocode, per CTE in declaration order):**
```
for (cte, cteId) in ast_root.cte_list:
    CteContext ctx
    QueryPlanner::plan_for_cte(cte, ctx.plan)
    classify_where_by_table(cte.stmt->where_expression, ctx)
    NdbAggregator cteAgg(ctx.plan.ops[ctx.plan.agg_leaf_idx].table)
    programCteAggregator(cte, cteAgg)

    qb->beginCteSubtree(cteId)
    emit_root_op(qb, ctx.plan, ctx.opDefs, /*rootAgg=*/nullptr)
    emit_child_ops(qb, ctx.plan, ctx.opDefs, &cteAgg,
                   ctx.join_where_ce, ctx.cteVirtualTables)
    qb->endCteSubtree()

    // srcTab for defineCte is the CTE's root table — or a virtual table if
    // the CTE's root is itself a CTE_SCAN (chained case).
    srcTab = (ctx.plan.ops[0].type == JoinOp::CTE_SCAN)
             ? cteVirtualTables[ctx.plan.ops[0].cte_def_idx]
             : ctx.plan.ops[0].table
    qb->defineCte(cteId, srcTab, cteAgg)
```

**Chained CTE handling.** When a CTE `b`'s inner stmt has `FROM a` (where
`a` is another CTE), `plan_for_cte(b, ...)` produces a sub-plan with
`ops[0].type == JoinOp::CTE_SCAN` (or `CTE_LOOKUP` on a joined CTE). The
`emit_child_ops`/`emit_root_op` helpers must dispatch on this and call
`qb->scanCte(a.cte_def_idx, ...)` or `qb->lookupCte(...)` correctly
inside `b`'s subtree. Because `defineCte(a)` was emitted before `b`'s
subtree began (declaration order), the reference resolves.

**Prerequisite A0.** Guard `RonSQLPreparer.cpp:4050`
(`NdbAggregator singleAgg(plan.ops[plan.agg_leaf_idx].table)`) against CTE
ops: throw `"main-query aggregation over CTE output not yet supported"`
when the main query's agg leaf is a CTE op. (Relaxed in Phase E for the
scanCte-root + main-aggregate case.)

**Tests.** No MTR yet (guard still in place). First green test lands in
Phase B. A focused debug-build sanity check: compile and verify EXPLAIN
output for a multi-table CTE names the expected ops inside the CTE scope.

---

### Phase B — Remove execution guard + end-to-end inner-join CTE

**Status:** Phase A + A0 emit landed; guard removed; first test
`ronsql_cte_basic.test` written and runs end-to-end up to a data-node SIGSEGV
in `JoinAggInterpreter::initGBTypes` (`+1476`). Phase B.1 fixes the
outer-aggregator emit bug so Test 1 of `ronsql_cte_basic.test` produces
green output; subsequent Phase B.2 records baselines for the remaining tests
in the file.

---

#### Phase B.1 — Fix CTE-leaf aggregator local/linked dispatch (crash root cause)

**Crash.**
```
JoinAggInterpreter::initGBTypes + 1476          ← SIGSEGV
JoinAggInterpreter::ProcessRec + 320
JoinAggInterpreter::processRecWithLinkedAttrs + 244
Dbtup::handleJoinAggRow + 696
```
Test query: Test 1 in `ronsql_cte_basic.test`.
```sql
WITH sums AS (SELECT o_custkey AS k, SUM(o_amt) AS t
              FROM cte_orders GROUP BY o_custkey)
SELECT c.c_name, SUM(sums.t)
FROM cte_customer AS c
JOIN sums ON sums.k = c.c_id
GROUP BY c.c_name;
```

**Diagnosis.** Three facts, confirmed against the code:

1. **`NdbAggregator` encodes local vs linked via bit `AGG_LINKED_COL_FLAG`
   (`0x8000`)** — `NdbAggregator.cpp:910-971,633-666`.
   `GroupBy(attrId)` / `LoadColumn(attrId, ...)` leave the bit clear → LOCAL
   (served by `req_struct->tablePtrP` in `initGBTypes`).
   `GroupByLinked(pos, col)` / `LoadLinkedColumn(pos, reg, col)` set the bit
   → LINKED (served by walking `m_linked_attr_data` to position `pos`). A
   single aggregator program may freely mix the two.

2. **CTE_LOOKUP agg-feed linked buffer layout** —
   `DblqhMain.cpp:18913-18998`:
   ```
     [Step 1] parent-linked projections (raw LQHKEYREQ subroutine format:
              [AttrHeader][data] per entry, *no* tableId/version prefix)
     [Step 2] CTE GB keys — each as [tableId=0][schemaVersion=0][AttrHeader][data]
     [Step 3] CTE agg results — same prefix format as step 2
   ```
   **Format inconsistency:** step 1 has no 2-word table prefix, but
   `JoinAggInterpreter::initGBTypes:1900-1903` walks the buffer assuming
   every entry has 2 prefix words. This means any attempt by the outer
   aggregator to reference a parent-linked projection via `GroupByLinked`
   or `LoadLinkedColumn` walks the buffer incorrectly and crashes (SEGV
   at line 1963 `strnxfrm_hash_len` on misaligned charset pointer).

   For a CTE body `SELECT k, SUM(v) AS t GROUP BY k` with no parent
   projections, the buffer is `[k][t]` and positions are `k=0, t=1`,
   matching the virt-table column indices set up in
   `build_cte_virtual_tables()`.

3. **Reference pattern in NDB API** — `testCteNdbApiFilter.cpp:1026-1034` and
   `testCteNdbApi.cpp:1319-1330`: when the outer aggregator is attached to
   `lookupCte(cteId, numResultCols, virtTab, cteKey, &opts)` via
   `opts.setAggregation(mainAgg)`, it uses `GroupByLinked(pos, vtCol)` /
   `LoadLinkedColumn(pos, reg, vtCol)` **with positions matching the
   virt-table column indices**. No test mixes LOCAL parent columns with
   LINKED CTE columns, because all known NDB-API tests put the aggregator
   on a purely CTE source. The encoding supports the mixed shape RonSQL
   needs — local `GroupBy(customer_c_name_attrId)` + linked
   `LoadLinkedColumn(1, reg, virt_t_col)` — the server-side interpreter is
   agnostic.

**Why the emit crashed (two bugs, one fixed, one deferred).**

- **(a) Wrong buffer position for CTE-output columns (FIXED).**
  `programAggregator_join()` emitted `LoadLinkedColumn(cte_col_idx, …)` and
  `GroupByLinked(cte_col_idx, …)` using the virt-table column index, but
  the linked-attr buffer prepends parent linked projections first. The fix
  offsets CTE-output positions by `scope.join_plan.num_linked_projs`.
  Applied in the working tree via `cte_base_pos`.

- **(b) Parent GB column + CTE agg leaf (DEFERRED).** When the outer
  aggregator's `GROUP BY` is on a parent-table column (e.g. `c.c_name`),
  RonSQL's existing GB ELSE branch emits
  `GroupByLinked(linked_proj_pos, col)` — relying on the parent column
  being present in the CTE_LOOKUP agg-feed buffer as a parent linked
  projection. But the DBLQH buffer walker (`initGBTypes:1900-1903`) does
  not handle step-1 parent projections correctly (see Fact 2 above), so
  any parent GB column over a CTE agg leaf SEGVs regardless of RonSQL's
  emit. The Phase B test suite works around this by grouping on a CTE
  output column instead; full parent-GB support requires a DBLQH fix and
  is tracked as a separate phase (likely Phase E or its own phase
  before H).

**Fix — per-column local/linked dispatch.**

Introduce two helpers on `QueryScope` (or as free functions over the scope):

- `bool is_cte_leaf_column(const QueryScope& scope, Uint32 col_idx)` —
  returns true iff `scope.column_table_idx[col_idx] == scope.plan.agg_leaf_idx`
  **and** `scope.plan.ops[agg_leaf_idx].type ∈ { CTE_LOOKUP, CTE_SCAN }`.

- `Uint32 cte_virt_col_idx(const CteDefinition* cte, Uint32 main_col_idx)` —
  walks `cte->stmt->outputs`, matching by output alias (already available
  on `Outputs::output_name`) against the column's name captured at analysis
  time. Emits position `0..N-1` in output-list order. Fail closed with
  `ndbrequire(false)` if not found (planner should have rejected unresolved
  references earlier).

Rewrite the GB case:
```cpp
for (Uint32 k = 0; k < scope.plan.num_groupby_cols; k++) {
  Uint32 col_idx = scope.plan.groupby_cols[k];
  if (is_cte_leaf_column(scope, col_idx)) {
    Uint32 pos = cte_virt_col_idx(scope.plan.ops[scope.plan.agg_leaf_idx].cte_def,
                                  col_idx);
    const NdbDictionary::Column* vtcol = cteLeafVirtTab->getColumn((int)pos);
    programAggregator_do_or_fail(aggregator->GroupByLinked(pos, vtcol));
  } else {
    programAggregator_do_or_fail(
        aggregator->GroupBy(scope.column_attrId_map[col_idx]));
  }
}
```
Same dispatch in the LoadColumn path — CTE-leaf col → `LoadLinkedColumn(pos,
reg, vtcol)`, parent col → `LoadColumn(attrId, reg, ...)`.

**Aggregator-anchor table.** Verify that
`NdbAggregator mainAgg(anchor)` uses the **right** anchor table. For the
mixed case the NDB API test pattern anchors on the virt table. When mixing
local parent columns, the anchor still needs to describe the local-column
namespace (`req_struct->tablePtrP` at execution time is the parent scan's
tablePtrP — the customer table — so local resolution works regardless of
what we pass to `NdbAggregator` constructor, as long as `GroupBy`/`LoadColumn`
bypass it). Quick check: the NDB API validates `GroupBy(attrId)` against the
anchor's column count. If the anchor is the virt table and parent-column
attrIds exceed virt-table col count, validation fails at prepare time.
**Use the parent-scan table as anchor** (`scope.table` in RonSQL-speak) to
side-step this. Revisit if NDB API client-side validation rejects.

**Remove the debug fprintf.** The `fprintf(stderr, "RONSQL_DEBUG ...")` block
in `programAggregator_join()` is WIP scaffolding — drop it.

**CTE-body aggregator path (already correct).** The `cteAgg` built via
`programCteAggregator` references CTE-body columns only (all local to the
CTE-body leaf table). No dispatch needed there; keep as-is.

**`rows=0` in DBSPJ log (not a bug).** The `scanFrag_CONF ... rows=0` in
`ndbd.1/ndbd.log` is the CTE-materialization phase's per-fragment count;
data is partitioned across the two data nodes and each fragment contributes
its slice. Not a materialization bug — it's just a symptom of the crash
firing before the second node's results aggregate.

**Files.**
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
  - `programAggregator_join()` — per-column dispatch in the GB loop and the
    Load loop. Remove the `fprintf(stderr, ...)` scaffolding.
  - New helpers `is_cte_leaf_column()` and `cte_virt_col_idx()` at file
    scope or as static helpers.
  - Main aggregator anchor (`NdbAggregator mainAgg(...)` construction, near
    current `RonSQLPreparer.cpp:4457-4469` area) — make sure it's anchored
    on the parent table, not the virt table, for mixed local/linked shapes.

**Verification.** `./mtr --record --suite=ronsql ronsql_cte_basic` must:
- Run Test 1 without SIGSEGV.
- Produce identical row set to the `== Expected result ==` block when the
  `--strict_diff=yes` harness compares RDRS vs MySQL.
- Leave both data nodes running (`ndbd.1/ndb_1_error.log` unchanged after
  the run; `ndbd.2` likewise).

---

#### Phase B.2 — Extend CTE test coverage (delivered in commit 5cd9600c6c4 landing all 4 baseline tests; now tackles multi-CTE and CTE-body-WHERE)

**Context.** Phase B.1 (`5cd9600c6c4`) already landed all four Tests 1–4 in
`ronsql_cte_basic.test` green in a single commit (single-table body,
multi-table body, MIN/MAX, COUNT — all A0 shape with GB on a CTE-output
column). Phase B.2 is now the natural follow-on: broaden the shape
coverage inside `ronsql_cte_basic.test` to expose any remaining emit bugs
before moving on to Phase C (filter) and Phase D (LEFT JOIN).

**Goal.** Four additional shapes land green in `ronsql_cte_basic.test`,
with `.result` re-recorded.

---

**Shape B.2a — Two independent CTEs in one query. DONE.**

Surfaced three RonSQL gaps, landed all three. What the test exposed:
1. The main aggregator hit "Failed writing aggregation program" on a GB
   or Load where the CTE-output column had a NULL `column_map` entry
   (CTE output cols are stored as `(attrId=cte_col_idx, column=NULL)`
   in `load_join`). Fix: dispatch on `src_is_cte` + non-leaf in both
   the GB and Load branches of `programAggregator_join`, resolving
   `vtcol` from `cteVirtualTables[src_op_idx]`.
2. With the emit fixed, `NdbQueryBuilder::prepare()` asserted at
   `appendParamConstructor` NdbQueryBuilder.cpp:2987: the main tree was
   being built with the two CTE_LOOKUPs as siblings under `c`, and SPJ
   linked projections require the source to be an ancestor. Reference
   topology in `testCrossJoinTwoScalarCtes` / `testGreatestViaCaseAgg`:
   chain the CTE_LOOKUPs in the main tree (scanCte/lookupCte →
   lookupCte → ... → lookupCte with aggregator on deepest one);
   materialization stays parallel. Fix: add
   `JoinOp::tree_parent_op_idx`, post-process in `QueryPlanner::plan`
   to chain sibling CTE_LOOKUPs, emit `opts.setParent` when override
   differs from key-source parent.
3. Implicit linkWithParent already handles the grandparent case —
   when `setParent(sums)` is set and keys reference grandparent `c`,
   `linkWithParent(c)` sees `cnt.isChildOf(c)` via `m_parent=sums →
   sums.m_parent=c` and returns 0 (already linked). No
   QRY_MULTIPLE_PARENTS.
```sql
WITH
  sums AS (SELECT o_custkey AS k, SUM(o_amt) AS t
           FROM cte_orders GROUP BY o_custkey),
  cnt  AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM cte_orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t), SUM(cnt.n)
FROM cte_customer AS c
JOIN sums ON sums.k = c.c_id
JOIN cnt  ON cnt.k  = c.c_id
GROUP BY sums.k;
```
Expected risk: each CTE scope allocates its own virt table, and the main
aggregator's `Load` / `GroupBy` dispatch has to pick the right
`cteLeafVirtTab`. Today `cteLeafVirtTab = cteVirtualTables[leaf_idx]` uses
the agg-leaf op index — which is only one op, so at most one CTE feeds the
outer aggregator. Two CTEs as inner-join children means only one is the
agg leaf; the other's data reaches the outer aggregator via parent-linked
projections. **Verify:** does the NDB API allow one `NdbAggregator` to
reference two CTE-LOOKUP children simultaneously? If not, the Load path
needs `cteVirtualTables[scope.column_table_idx[src]]` instead of
`cteVirtualTables[leaf_idx]`.

**Shape B.2b — WHERE clause inside CTE body (pre-GROUP BY filter). DONE.**

Exposed one gap: the single-table CTE body emit path
(`cp.num_ops == 1` in `execute_join`) skipped the WHERE filter.
`build_cte_scopes` already classifies `cte->stmt->where_expression`
into `cs.join_where_ce`, and `emit_root_op` handles it for the
multi-op path. The single-table self-lookup path now builds an
`NdbScanFilter` against `cs.join_where_ce[0]` and attaches it via
`rootOpts.setInterpretedCode` on `scanTable`. Filter evaluates
pre-GB as expected; aggregator on the readTuple leaf sees only
rows that passed the filter.
```sql
WITH big_orders AS (
  SELECT o_custkey AS k, SUM(o_amt) AS t
  FROM cte_orders
  WHERE o_amt > 50
  GROUP BY o_custkey)
SELECT big_orders.k, SUM(big_orders.t)
FROM cte_customer AS c
JOIN big_orders ON big_orders.k = c.c_id
GROUP BY big_orders.k;
```
Risk: `plan_index_and_filter` runs on `m_main_scope` only; per-CTE scope
filter pushdown goes through a separate path in `resolve_columns_for_cte_scope`
and `emit_child_ops`. Verify the interpreted-code for the CTE-body scan
compiles and executes (single-table filter on `cte_orders.o_amt`).

**Shape B.2c — CHAR/VARCHAR GB column in CTE body. DONE.**

Exposed one gap: `ResultPrinter` threw `"Could not find charset for
VARCHAR column"` when printing a CHAR/VARCHAR CTE-output column.
`build_cte_virtual_tables` sets charset correctly on the virt-table
column at execute time, but `ResultPrinter` is compiled at prepare
time from `m_main_scope.column_map`, which `load_join` set to NULL
for CTE output refs. Fix: new `resolve_cte_output_columns()` pass
after `build_cte_scopes` resolves CTE-output column_idx → source
real column (from the CTE's own scope) and fills `column_map` for
the COLUMN / MIN / MAX output kinds. SUM/COUNT remain NULL (no
charset needed). An alternative grammar note: SQL identifier `code`
is parsed as a reserved keyword — test uses `rcode`.
```sql
CREATE TABLE cte_regions (
  r_id INT NOT NULL,
  r_code VARCHAR(8) NOT NULL,
  PRIMARY KEY USING HASH (r_id)
) ENGINE=NDB;
-- seed 3-4 rows across 2 distinct r_code values
WITH by_code AS (
  SELECT r_code AS code, COUNT(*) AS n
  FROM cte_regions GROUP BY r_code)
SELECT by_code.code, SUM(by_code.n)
FROM cte_regions AS r
JOIN by_code ON by_code.code = r.r_code
GROUP BY by_code.code;
```
Risk: VARCHAR virt-table column metadata may need `setCharset()` populated
from the source. `build_cte_virtual_tables()` already copies charset from
`src_col->getCharset()` for COLUMN / MIN / MAX derived types — verify it
also fires for GB output and that the NDB API's `NdbAggregator` consumes
it correctly. Note the join uses `by_code.code = r.r_code`, a VARCHAR
linked key — exercises charset-aware key binding in the CTE_LOOKUP
emit.

**Shape B.2d — COUNT(*) in the MAIN query over CTE output. DONE.**

No emit fixes needed. `COUNT(*)` interleaves cleanly with
`LoadLinkedColumn(cte_base_pos + cte_col_idx, reg, vtcol)` for a
CTE-backed SUM — Phase B.1's `cte_base_pos` offset applies only to
linked loads, non-linked slots (LoadUint64 + Count) keep their
register IDs independently.
```sql
WITH sums AS (SELECT o_custkey AS k, SUM(o_amt) AS t
              FROM cte_orders GROUP BY o_custkey)
SELECT sums.k, COUNT(*), SUM(sums.t)
FROM cte_customer AS c
JOIN sums ON sums.k = c.c_id
GROUP BY sums.k;
```
Risk: `COUNT(*)` emits `LoadUint64(1, reg) + Count(slot, reg)` — no
column reference, so dispatch isn't exercised. But slot numbering must
survive mixing with `SUM(sums.t)` (which uses the new `LoadLinkedColumn`
path). Ensures Phase B.1's `cte_base_pos` offset didn't shift non-linked
slots.

---

**Files.**
- `mysql-test/suite/ronsql/t/ronsql_cte_basic.test` — append four new
  test blocks (B.2a–B.2d). Add the `cte_regions` table to the setup
  section; drop it in the cleanup section.
- `mysql-test/suite/ronsql/r/ronsql_cte_basic.result` — re-record.
- RonSQL emit fixes as exposed by the tests, likely:
  - `programAggregator_join()` Load branch: choose
    `cteVirtualTables[scope.column_table_idx[src]]` (per-op) instead of
    `cteVirtualTables[leaf_idx]` (agg-leaf only) if B.2a fails on the
    non-leaf CTE child.
  - `build_cte_virtual_tables()` charset plumbing if B.2c surfaces a
    type gap.
  - `build_cte_linked_projections()` iteration if multi-CTE exposes
    scope-bookkeeping gaps.

**Verification.**
```
cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
cd mysql-test && ./mtr --record --suite=ronsql ronsql_cte_basic
./mtr --suite=ronsql            # full suite — no regressions
```

**Out of scope for B.2** (tracked separately):
- WHERE on a CTE-output column in the main query — Phase C.
- LEFT JOIN with a CTE — Phase D.
- Parent-table GB over a CTE agg-leaf (e.g. `GROUP BY c.c_name`) —
  needs a DBLQH fix. The step-1 parent-linked entries in
  `cteLookupAggFeed`'s buffer are emitted in LQHKEYREQ subroutine format
  (raw `[AttrHeader][data]`, no `[tableId=0][schemaVersion=0]` prefix),
  but `JoinAggInterpreter::initGBTypes` walks the buffer assuming the
  prefixed format universally. Fix: in
  `DblqhMain.cpp:buildCteLinkedBuffer`, iterate the parent-linked
  section attr-by-attr and prepend the two zero words so every entry
  shares the CTE-data format. Tracked as **Phase P-GB** — land before
  Phase D so LEFT JOIN coverage can use parent-column SELECT lists
  naturally.

---

### Phase C — Filter on CTE_LOOKUP child

**Goal.** Wire `setInterpretedCode` for CTE_LOOKUP children over the CTE-safe
opcode subset (Phase A of `cte_filter_plan.md`).

**Status (column-only): DONE.** Test 9 in `ronsql_cte_basic.test`
exercises `WHERE sums.k > 100` on a CTE output that's a direct column
projection. Implementation:
- `emit_cte_lookup_filter()` builds a raw `NdbInterpretedCode` using
  `branch_linked_mem_*` (mirrors the testCteNdbApiFilter pattern).
- Server-side `BRANCH_MEM_OP_ARG` resolves type/charset via
  `tablerec[tableId]`, which RonSQL's synthetic virt tables do not
  populate. Workaround: walk through the CTE body's scope to find the
  SOURCE real column for each filter conjunct, and pass its real
  table + attrId. The linked-buffer slot at `cte_col_idx` stores the
  source-column-typed value for `Outputs::Type::COLUMN` outputs, so
  the descriptor matches the data.
- Constrained to single-table CTE bodies and AND-combined column-vs-
  constant comparisons. OR, column-vs-column, expressions, and
  filters on aggregate outputs (SUM/MIN/MAX/COUNT — type doesn't match
  any registered NDB column) throw a clear "not yet supported" error
  via `require_prm`.
- Inversion table for the `branch_linked_mem_*` family follows the
  project-wide convention (see CLAUDE.md): `branch_linked_mem_le`
  branches on `col >= val`, etc. The helper double-inverts so the
  sense matches the SQL operator that should still be REJECTED.

**Followup phase.** Aggregate-output filter pushdown needs a new DBTUP
opcode that carries `[typeId, length, charsetPos]` inline rather than
indirecting through `[tableId, schemaVersion, attrId]`. One new entry
in `s_cte_filter_handlers` + one new client-side
`branch_linked_mem_typed_*` family on `NdbInterpretedCode`. Lifts the
column-only restriction; until then RonSQL rejects filters on
SUM/COUNT/AVG outputs with a clear error.

**Files.**
- `RonSQLPreparer.cpp` — find the `op.type != JoinOp::CTE_LOOKUP` guard in
  the apply_filter path (current file, around line ~4911 per explorer report)
  and remove it so interpreted code is emitted for CTE_LOOKUP children.
  Positions of filter column references into the virt table use the same
  `cte_virt_col_idx()` helper introduced in Phase B.1.
- Server-side validation (`QRY_INTERPRETED_CODE_NOT_CTE_SAFE`) already
  propagates through `NdbQueryOperation.cpp` — no RonSQL change; surface the
  error text cleanly.

**Tests (MTR).** Extend `ronsql_cte_basic.test` with:
```sql
WITH sums AS (SELECT o_custkey k, SUM(o_amt) t FROM cte_orders GROUP BY o_custkey)
SELECT c.c_name, sums.t FROM cte_customer AS c JOIN sums ON sums.k = c.c_id
  WHERE sums.t > 100 AND c.c_region = 1;
```
Add CHAR-comparison variant (exercises charset descriptors on linked cols).

---

### Phase D — CTE_LOOKUP in LEFT OUTER JOIN

**Goal.** Confirm MatchAll-default path for CTE_LOOKUP under `LEFT JOIN` and
cover it end-to-end.

**Status: DONE.** Test 10 in `ronsql_cte_basic.test`. No code changes
required — RonSQL already maps `JoinOp::LEFT_OUTER` to MatchAll-default
by omitting `setMatchType()`, and Phase B.1's per-column Load dispatch
resolves CTE-leaf positions correctly for NULL-injected rows. The
fixture now has a 4th customer (Dave / c_id=400) with no orders, so
`LEFT JOIN sums ON sums.k = c.c_id GROUP BY sums.k` produces a NULL
group whose `SUM(sums.t)` is NULL. INNER JOIN tests (1-9) are
unaffected because Dave's c_id doesn't match any sums.k. Anti-join
patterns (`WHERE sums.t IS NULL`) and parent-table-column GROUP BY
under LEFT JOIN remain unexplored — they would land on the same
DBLQH parent-GB issue tracked in Phase B notes.

**Tests (MTR).** Append to `ronsql_cte_basic.test`:
```sql
-- Unmatched parents get NULL-filled CTE cols
WITH sums AS (SELECT o_custkey AS k, SUM(o_amt) AS t FROM cte_orders GROUP BY o_custkey)
SELECT c.c_name, sums.t
  FROM cte_customer AS c LEFT JOIN sums ON sums.k = c.c_id;
-- Anti-join pattern
SELECT c.c_name FROM cte_customer AS c LEFT JOIN sums ON sums.k = c.c_id
 WHERE sums.t IS NULL;
```
Seed data so at least one customer has no orders (already true in the
existing test fixture — `cte_customer` has 3 rows, `cte_orders` has 5
spread across the 3 customers, but include one customer with none for the
anti-join to produce a row).

---

### Phase E — scanCte as main-query root + CTE-child-of-CTE

**Goal.** Recognize `FROM <cte_name>` in any FROM clause (main or inside
another CTE); emit `scanCte` (as root) or `lookupCte`/`scanCte` (as child).
The planner change must be general — used by both main-query root
(Phase E) and chained-CTE inner planning (Phase A).

**Files.**
- `QueryPlanner.hpp:43` — add `CTE_SCAN` to `JoinOp::Type` enum (alongside
  existing `CTE_LOOKUP`).
- `QueryPlanner.cpp:80-111` — before `dict->getTable(root_name)`, look up
  `root_name` in the current scope's `cte_list`. If matched, set
  `rootOp.type = CTE_SCAN`, `rootOp.table = NULL`,
  `rootOp.cte_def = <cte>`, `rootOp.cte_def_idx = <idx>`; skip dict lookup.
  Same dispatch for child FROMs (existing CTE_LOOKUP planning path
  already handles CTE children — extend parity to `CTE_SCAN` child when
  the join topology calls for a scan rather than a lookup).
- `RonSQLPreparer.cpp` root-emit helper (introduced in Phase A): when
  `plan.ops[0].type == JoinOp::CTE_SCAN`, build the CTE's result-schema
  virtual table, call `qb->scanCte(cte_def_idx, numResultCols, virtTab,
  &rootOpts)`. Skip PK-bound / index-scan paths. Filter via
  `rootOpts.setInterpretedCode(code)` works (scanCte supports it).
- `RonSQLPreparer.cpp` child-emit helper (introduced in Phase A): when a
  child op has `type == CTE_SCAN`, call `qb->scanCte(...)` inside the
  current subtree scope (this handles chained CTEs during Phase A emit,
  and the mixed main-query shape `FROM cte JOIN real_table` during
  Phase E).
- `RonSQLPreparer.cpp:4457-4469` — main aggregator attach: when root is
  CTE_SCAN with no real-table leaf, attach the main aggregator to the
  CTE_SCAN root (scanCte supports `setAggregation` per
  `NdbQueryOperation.cpp:3368`). If that path isn't ready, fall back to
  pass-through and reject re-aggregation with
  `"main-query aggregation over CTE_SCAN not yet supported"`.
- `RonSQLPreparer.cpp:3309` — allow `ResultPrinter` for the scan-passthrough
  path.
- Column resolution (`analyze_columns` / `m_column_qualifiers`) — branch
  for CTE aliases when any scope's SELECT / ON / WHERE references
  `<cte>.col`.

**Tests (MTR).** New `mysql-test/suite/ronsql/t/ronsql_cte_scan.test`:
```sql
-- Scan CTE as main root, pass-through
WITH sums AS (SELECT o_custkey k, SUM(o_amt) t FROM orders GROUP BY o_custkey)
SELECT k, t FROM sums ORDER BY k;

-- Scan + WHERE filter
SELECT k, t FROM sums WHERE t > 50;

-- Scan CTE joined to real table
WITH sums AS (...)
SELECT s.k, s.t, c.c_name FROM sums s JOIN customer c ON c.c_id = s.k;

-- Chained CTE: b references a (requires scanCte inside CTE subtree)
WITH a AS (SELECT g, SUM(v) AS s FROM t GROUP BY g),
     b AS (SELECT g, MAX(s) AS m FROM a GROUP BY g)
SELECT g, m FROM b;
```

---

### Phase F — SCAN_NEXTREQ multi-batch (folded into E)

**Goal.** Confirm multi-batch iteration is correct end-to-end.

**Files.** None in RonSQL. The default batch size (DBSPJ hardcoded 256) is
fine; `nextResult(true)` already drains batches.

**Tests (MTR).** Large-dataset variant in `ronsql_cte_scan.test`: insert >600
distinct groups, scan via `scanCte` root, assert full result set.

---

### Phase G — Reject-cleanly guard for the one NDB-un-shipped shape

**Goal.** Emit `RonSQLPermanentError` for the single CTE shape the NDB API
does not yet support.

**Files.**
- `RonSQLPreparer.cpp` — new `validate_cte_execution_shapes()` called from
  `prepare()` right after `analyze_ctes()`:
  1. **CTE_SCAN as outer-join child.** If any outer SELECT join maps a
     CTE-named child under LEFT JOIN and the planner selects CTE_SCAN for
     that child (not CTE_LOOKUP), throw
     `"CTE_SCAN as outer-join child not yet supported by NDB"`.
     This is NDB Phase 3 / `next_steps.md` 6b — still deferred.

Note: `CTE_LOOKUP` agg-feed NULL injection inside an enclosing CTE agg
subtree was previously listed here but is now supported end-to-end
(branch commit `47d81b43903`, `cte_outer_join_phase_5.md`). No guard
needed; rely on the agg-feed NULL-injection path to fire for `LEFT JOIN
cte` inside a CTE body.

**Tests (MTR).** New `mysql-test/suite/ronsql/t/ronsql_cte_reject.test` with
`--error` expectation for the CTE_SCAN outer-join-child guard. Also add a
positive test for the ex-6a shape — `WITH a AS (...GROUP BY...), b AS
(SELECT COUNT(*), SUM(a.total) FROM t LEFT JOIN a ON ...)
SELECT * FROM b;` — confirming NDB's Phase 5 fix flows through RonSQL.

---

### Phase H — Test matrix consolidation + C++ coverage

**MTR files (final):**
- `ronsql_cte_basic.test` — Phase B + C + D (inner + filter + left join).
- `ronsql_cte_scan.test` — Phase E + F (scanCte root, multi-batch, chained).
- `ronsql_cte_reject.test` — Phase G rejections.
- `ronsql_cte_agg_ext.test` — COUNT, MIN, MAX, mixed aggregate types in CTE
  inner SELECT.

Each `.test` uses `--source suite/ronsql/include/ronsql_compare.inc` with
`--let $suppress_ronsql_cli=yes` (schema-cache convention).

**C++ block test.** New `storage/ndb/block_unit_test/testRonsqlCteWiring.cpp`,
registered in the same dir's `CMakeLists.txt` via `NDB_ADD_EXECUTABLE(...)`,
following the `testCteNdbApi*.cpp` pattern. Covers what MTR cannot reach:
- Virtual-table schema correctness (PK-only vs PK+result-cols) for CTE_LOOKUP.
- `defineCte` declaration-order emission for chained CTEs, `depMask` check.
- CTE-safe interpreter rejection surfaces as `RonSQLPermanentError` rather than
  crash.

## Files to modify

- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` — core: CTE subtree emit, guard
  removal, filter/agg wiring, scanCte root, shape validation.
- `storage/ndb/src/ronsql/QueryPlanner.cpp` — CTE-name-as-root resolution.
- `storage/ndb/src/ronsql/QueryPlanner.hpp` — `JoinOp::CTE_SCAN` enum.
- `storage/ndb/block_unit_test/CMakeLists.txt` — register new C++ test.
- `storage/ndb/block_unit_test/testRonsqlCteWiring.cpp` — new C++ test.
- `mysql-test/suite/ronsql/t/ronsql_cte_basic.test` — new.
- `mysql-test/suite/ronsql/t/ronsql_cte_scan.test` — new.
- `mysql-test/suite/ronsql/t/ronsql_cte_reject.test` — new.
- `mysql-test/suite/ronsql/t/ronsql_cte_agg_ext.test` — new.
- `mysql-test/suite/ronsql/r/ronsql_cte_*.result` — recorded after first run.

## Reference (reuse, don't re-create)

- `storage/ndb/src/ronsql/RonSQLPreparer.cpp:4361-4395` — existing virtual-table
  construction to extend.
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp:4533-4542` — existing `lookupCte`
  emit.
- `storage/ndb/src/ronsql/AggregationAPICompiler.cpp` — aggregator program
  compilation logic to reuse for `programCteAggregator`.
- `storage/ndb/block_unit_test/testCteNdbApi.cpp:285` — canonical
  subtree + `defineCte` + `setAggregation` skeleton.
- `storage/ndb/block_unit_test/testCteNdbApiFilter.cpp:642-670` — filter-on-CTE
  skeleton.
- `storage/ndb/block_unit_test/testCteNdbApiOuterJoin.cpp` — LEFT JOIN shape.

## Verification

Per-phase: after edits land, build and run the owning MTR file.

```
cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
cd debug_build/mysql-test && ./mtr --suite=ronsql ronsql_cte_basic
./mtr --suite=ronsql ronsql_cte_scan
./mtr --suite=ronsql ronsql_cte_reject
./mtr --suite=ronsql ronsql_cte_agg_ext
./mtr --suite=ronsql            # full suite — no regressions
```

C++ block test:
```
cd debug_build && make -j$(sysctl -n hw.ncpu) testRonsqlCteWiring
./storage/ndb/block_unit_test/testRonsqlCteWiring -c localhost:13010 -m 13011 -v
```

Re-record goldens on first landing:
```
./mtr --record --suite=ronsql ronsql_cte_basic
./mtr --record --suite=ronsql ronsql_cte_scan
./mtr --record --suite=ronsql ronsql_cte_reject
./mtr --record --suite=ronsql ronsql_cte_agg_ext
```

Full regression:
```
./mtr --suite=ronsql ronsql_basic ronsql_join ronsql_join_agg \
  ronsql_left_join ronsql_chained_left_join ronsql_subquery_agg
```
