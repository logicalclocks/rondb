# CTE-Body Column-vs-Column WHERE Support (F-colvscol)

**Status: SHIPPED (2026-09-01) — recorded green ×5 topology suites
(`ronsql_cte_dd_filter` + the `ronsql_cte_dd_d4_colvscol` standalone);
W5 docs done (rondb-docs PR #104 commit `48d46cd`, fix-plan F-colvscol
closed, findings D4/D12 rows flipped to SUPPORTED).**

Outcome log:
- **W0**: the D4 hang does NOT reproduce on current binaries — probe ran
  the exact D4 query (plus an isolated no-join variant and the D12
  shape) through RDRS + ronsql_cli with MySQL value compares, all green.
  Stale-binary artifact, D19/D20 precedent.  Body WHERE confirmed to
  execute in the main interpreter (`BRANCH_ATTR_OP_ATTR` →
  `handleBranchAttrOp`); the nullptr slot-27 dispatch tables are the CTE
  jump-table and embedded-agg interpreters, which never see body WHERE
  programs.
- **W1**: already fixed by the join-root non-constant-RHS bound
  hardening — the D12 body plans as `TABLE_SCAN` + residual filter
  (probe EXPLAIN + correct filtered counts).
- **W2**: `check_no_cte_body_col_vs_col` →
  `RonSQLPreparer::check_cte_body_col_vs_col(scope, ce)`; accepts
  StoredColumn pairs on the body root op passing
  `NdbDictionary::Column::isBindable` — which turned out to be PUBLIC
  API, so gate and emit literally share the predicate (the drift risk is
  gone, the belt-and-braces 4557 rethrow was not needed).  Two refined
  permanent errors (non-root-op pair; mixed-type pair).
- **W3**: zero code, as predicted — the existing `apply_filter_cmp`
  attr-vs-attr arm serves both body paths.
- **W4**: `body_filter.inc` Group 4 converted to value compares
  (filter-12/13) + new Group 8 matrix (filter-32..39b: nullable INT,
  OR/DNF, nullable-in-OR, scalar-body residual, CHAR/DATE/DECIMAL pairs
  on the new local `cvc` table, degenerate self-compare, =/<> sweep,
  both-nullable pair) + rejection probes filter-P1..P4 (mixed INT/SMALLINT
  UNSIGNED, FLOAT/DOUBLE, DECIMAL precision mismatch, non-root-op pair);
  `ronsql_cte_dd_d4_colvscol.test` rewritten as the standalone value-
  compare regression.  The temporary W0 probe test is deleted.
- **W5**: rondb-docs updated (CTE chapter restriction passage → support
  statement with both refined rejection messages, "Not supported" bullet
  and restrictions chapter narrowed; commit `48d46cd` on PR #104);
  `cte_fix_plan.md` F-colvscol closed; `cte_test_driven_findings.md`
  D4/D12 rows flipped to SUPPORTED.  Recorded green ×5 topology suites
  on first record.

Closes the `cte_fix_plan.md` deferred-feature item **F-colvscol**: support
`WHERE colA < colB` (all six comparison operators) inside a CTE body,
currently rejected permanently by `check_no_cte_body_col_vs_col`
(H3 fix for findings D4 + D12).

## Background

### The two original failures (June 2026, both `INT NOT NULL` pairs)

- **D12** (`orders`, `o_custkey < o_orderkey`, indexed left column): the
  conjunct was mis-classified as an index bound by
  `build_scan_config_candidates`, so bound extraction called
  `encode_constant()` on the **column** RHS → retryable
  `RonSQLMaybeStaleSchema` → RDRS retried 10× (hang-like at the client).
- **D4** (`lineitem`, `l_quantity < l_partkey`, non-indexed left): the
  TABLE_SCAN body emitted a **valid identical-type** NdbScanFilter
  attr-vs-attr program that **hung the data node**.

Both were closed by rejecting up front: `check_no_cte_body_col_vs_col`
(RonSQLPreparer.cpp:4126) walks each CTE body's WHERE from
`build_cte_scopes` (:5257) and throws a permanent error on any comparison
with two identifier operands. Main-query col-vs-col does not flow through
`build_cte_scopes` and reportedly works on top-level scans.

### What exists today (audit results, September 2026)

- **`apply_filter_cmp` already has a col-vs-col arm** (:11895): when the
  RHS is an identifier it calls `NdbScanFilter::cmp(cond, attrId1,
  attrId2)` → `cond_col_col` → `NdbInterpretedCode::branch_col_col` →
  legacy opcode **`BRANCH_ATTR_OP_ATTR` (27)**. This is the arm D4
  exercised. The same arm serves the TABLE_SCAN body filter, the
  INDEX_SCAN residual filter, and top-level main-query scan filters.
- **API-side validation**: `branch_col_col` requires
  `col1->isBindable(*col2)` — same type, precision, length, scale and
  charset; BLOB/TEXT excluded (error 4557 otherwise). So the legacy path
  is **identical-type only**, but within that it covers integers,
  CHAR/VARCHAR (charset-aware kernel compare), DATE and even DECIMAL
  with matching precision/scale.
- **NULL semantics**: every RonSQL scan filter calls
  `setSqlCmpSemantics()` — a NULL operand makes the comparison UNKNOWN
  and rejects the row in an AND group. SQL-correct by construction
  (needs a nullable-column test; the suite's shared tables are all
  NOT NULL).
- **Kernel dispatch**: the main interpreter
  (`interpreterNextLab` INTERP_DISPATCH switch, DbtupExecQuery.cpp:10645)
  handles `BRANCH_ATTR_OP_ATTR` via `handleBranchAttrOp`. The CTE
  jump-table dispatch (:10009) and the embedded-aggregation dispatch
  (:10165) both have **nullptr** at slot 27 — but neither of those should
  execute a body-scan WHERE program (the body WHERE is a normal
  PI_ATTR_INTERPRET program on the SCAN_FRAGREQ; the aggregation
  interpreter runs afterwards via `handleJoinAggRow`).
- **Bounds hardening since D12**: `join_root_index_scan_plan.md` shipped
  "generator hardened to reject non-constant right sides as bounds
  (latent col-vs-col `encode_constant` throw, also on the single-table
  path)". The D12 misclassification path is therefore *probably* already
  dead on the shared generator — needs verification on the CTE-body
  candidate path (`select_cte_body_scan_config`).
- **Stale-binary precedent**: D19 and D20's "hangs" both turned out to be
  stale rdrs2/ndbmtd binaries from parallel suite authoring. D4's hang
  was recorded on June-2026 binaries; the interpreter has since been
  rebuilt end to end (Phase I.18 typed-register retrofit touched every
  producer/consumer handler, plus the INTERP_DISPATCH rework). The hang
  may no longer exist.

## Design

Two-tier type envelope; v1 ships tier (a) only.

**Tier (a) — identical-type pairs (v1)**: both operands are stored
columns of the *same* single-table body, and the pair is bindable
(same type/precision/length/scale/charset, non-BLOB/TEXT). Emit through
the existing `apply_filter_cmp` attr-vs-attr arm — **zero new kernel or
API code**, zero wire change (opcode 27 is ancient, no version gate).
This covers both original repros (INT vs INT), plus CHAR-vs-CHAR,
DATE-vs-DATE and same-precision DECIMAL — a *broader* type list than the
main-path CTE_LOOKUP col-vs-col filter (which is typed-register-based
and has no strings/DECIMAL), at the cost of the identical-type
restriction.

**Tier (b) — mixed pairs (deferred follow-up)**: mixed integer
widths/signedness and INT/FLOAT/DOUBLE/DATE mixes need typed registers
in the main interpreter: a typed leaf-local column load (either fix the
queued v5 follow-up — `READ_ATTR_INTO_REG` zero-extends signed
sub-Bigint and has no FLOAT/DOUBLE/DATE typing — or add a
`READ_COLUMN_TO_REG` twin of `READ_LINKED_COLUMN_TO_REG` with the 6-bit
type field) plus `compareTypedRegs`-style emission. Until then, mixed
pairs get a clear permanent error in sc-P4 style: *"CTE-body
column-vs-column: only columns of identical type, precision, length and
charset can be compared — cast one side or compare against a constant."*

**Scope line for v1**: single-table CTE bodies only (the body scope has
exactly one op). Multi-op (join) bodies keep the blanket rejection —
cross-table col-vs-col there belongs to the embedded cross-table filter
machinery and is a separate feature. Single-row CTE bodies (full-PK
residuals) and scalar bodies are in scope — same root-op filter path.

## Work items

- **W0 — re-verify the D4 hang on current binaries (gates everything).**
  Bypass `check_no_cte_body_col_vs_col` locally and run the D4 shape
  (`WITH x AS (SELECT SUM(...) FROM lineitem WHERE l_quantity <
  l_partkey) SELECT ...`) against freshly built data nodes; the
  block-test route (a `testCteNdbApi`-style case building the body scan
  with an attr-vs-attr filter program) pins it below RonSQL if MTR-level
  behaviour is ambiguous.
  - If it does **not** reproduce → stale-binary artifact like D19/D20;
    proceed to W1.
  - If it reproduces → trace-file diagnosis (`trace_file_analysis.md`);
    likely suspects are the agg-scan continuation path around a
    mid-program EXIT (batch accounting) rather than the compare itself,
    since the identical program works on non-agg scans. Fix the kernel
    defect first; everything downstream is unchanged.
  - Either way, confirm by code reading that the body-scan WHERE program
    executes in the main interpreter on the agg-feed path (not the
    jump-table or embedded dispatch, whose slot 27 is nullptr).
- **W1 — bounds hygiene (the D12 half; expected zero code).** Verify the
  shipped non-constant-RHS bound hardening covers
  `select_cte_body_scan_config`'s candidate generation: an indexed-left
  col-vs-col conjunct must neither become an index bound nor count
  toward index-selection scoring, and must land in the residual filter.
  Add an EXPLAIN-pinned probe (D12 shape) proving `FILTER:` routing.
- **W2 — gate rework.** Replace the blanket
  `check_no_cte_body_col_vs_col` with a typed acceptance check run at
  the same point (build_cte_scopes, after classify_where_by_table,
  before scan-config selection):
  - accept when the body is single-table, both operands resolve to
    `StoredColumn` on the body table, and the pair passes a bindability
    predicate replicating `NdbColumnImpl::isBindable` (same
    type/precision/length/scale/charset, non-BLOB) — check whether the
    public dictionary API exposes this; if not, replicate it in RonSQL
    so the **same predicate** gates and emits (predicate drift would
    surface as emit-time 4557 → `require_sch` → retryable
    `RonSQLMaybeStaleSchema` → a D12-style retry storm; add a
    belt-and-braces catch that rethrows 4557 as permanent);
  - reject mixed pairs, cross-table pairs and multi-op bodies with the
    refined permanent messages above.
- **W3 — emit (expected zero code).** The `apply_filter_cmp` col-vs-col
  arm already serves both body call sites (TABLE_SCAN filter :7146,
  INDEX_SCAN residual :7292/:7623). Verify EXPLAIN prints the conjunct
  in the body CONDITIONS line on both paths.
- **W4 — MTR (×5 topology suites, `--record`).**
  - Convert `body_filter.inc` filter-12/13 and
    `ronsql_cte_dd_d4_colvscol.test` from rejection-asserts back to
    value compares (`ronsql_compare.inc`), per the fix-plan re-enable
    step.
  - Enable the NEXT-PHASE string case at body_filter.inc:868 (the
    degenerate self-compare `l_returnflag < l_returnflag` pins the
    always-false/empty result) and add a genuine two-column CHAR compare.
  - New cases: D12 shape with EXPLAIN `FILTER:` pin (indexed left);
    DATE-vs-DATE; identical-DECIMAL pair; nullable pair on a local table
    (NULL row rejected — none of the shared tables are nullable);
    col-vs-col inside body DNF/OR (or-body family interaction);
    col-vs-col residual in a single-row CTE body (`WHERE pk = const AND
    a < b`) and in a scalar body; `!=`-heavy program (the
    `finalise()` non-idempotency regression area).
  - Rejection probes: INT vs BIGINT, INT vs FLOAT, CHAR pairs with
    different lengths/charsets, cross-table pair in a join body,
    TEXT operand.
- **W5 — docs + bookkeeping.** rondb-docs `ronsql_cte.md` (the :149
  "not supported" statement and the "Not supported" bullet) +
  `ronsql_limitations.md` CTE bullet; close F-colvscol in
  `cte_fix_plan.md`; update the findings file and the directory
  CLAUDE.md entry; note tier (b) as the follow-up with its typed-load
  dependency.

## Risks and open questions

- **The D4 hang is undiagnosed.** The plan deliberately front-loads W0;
  no RonSQL relaxation lands until the kernel behaviour is proven green
  (crash/hang beats a missing feature).
- **isBindable availability** on the public `NdbDictionary::Column` for
  W2's gate — replicate if private, with a comment tying the two sites.
- **Retryable-vs-permanent drift** between gate and emit (the 4557
  belt-and-braces above).
- **Main-query col-vs-col has no MTR pin** despite being a working path;
  W4's families cover the body paths — add one main-WHERE case to the
  same family while at it (cheap, same emit arm).
- Tier (b) interacts with the v5 leaf-side sign-extension follow-up and
  the I.26 typed envelope; when it ships, the gate predicate widens from
  "bindable" to "bindable OR typed-reg-loadable pair", and the mixed-pair
  rejection message retires.
