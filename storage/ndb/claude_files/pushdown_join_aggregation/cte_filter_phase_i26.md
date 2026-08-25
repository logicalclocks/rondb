# Phase i26 — real-table vs CTE-output comparisons (`t.col > s.m`)

**Status: SHIPPED (2026-08-25, recorded green ×5 topology suites +
regression pass after two first-record fixes — the sc-17 GB-first
projection-ordering crash and the stale ronsql_cte_colvscol Test 9
pin, both described below).**
Ships the watermark shape pinned by the former sc-P1: a WHERE conjunct
comparing a real-table column against a CTE_LOOKUP's output —
`WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders)
SELECT cu.c_custkey FROM customer cu, s WHERE cu.c_custkey > s.m` —
on BOTH the pass-through and aggregate paths.  Also upgrades the
Phase I.3 Bigint-only CTE col-vs-col filter to the full typed
envelope (flipping rpr-P1 → rpr-16/16b) and fixes a latent
wrong-results base-offset bug.

## Mechanism (direction (a) of `non_aggregate_phase_4.md`'s sketch)

The predicate routes to the CTE_LOOKUP op's jump-table filter with the
real column delivered as a **linked parent projection** in the
filter's linked-attr buffer.  One mechanism serves both paths because
`Dblqh::runCteFilter` gates the CTE lookup before either the agg feed
or the row emit.  Almost all machinery pre-existed:

- `Dblqh::buildCteLinkedBuffer` already PREPENDS parent linked columns
  (from the AttrInfo subroutine section) ahead of the CTE outputs; the
  buffer layout is `[parent projs 0..P-1][GB keys][agg results]`.
- DBSPJ already expanded the per-parent-row `m_attrParamPattern` for
  CTE_LOOKUP sends — but only on the agg-feed branch.  **The single
  kernel change (W1)**: `cte_lookup_send` now expands whenever
  `T_ATTRINFO_CONSTRUCTED` is set, non-agg sends included (~10 lines;
  the base-AttrInfo and release branches merge accordingly).
- The NDB API serializes `addLinkedProjection` for CTE_LOOKUP nodes
  unconditionally; **W2 adds the one missing emit surface**:
  `NdbInterpretedCode::read_linked_column_to_reg(reg, position,
  ndb_type)` — a one-word wrapper for the Phase I.5 v5
  READ_LINKED_COLUMN_TO_REG opcode (kernel handler + jump-table slot
  already existed).
- I.18's typed registers make `BRANCH_*_REG_REG` compare correctly
  across mixed signedness and int-vs-double (`compareTypedRegs`).

## RonSQL changes

- **W3 — base offset (latent-bug fix)**: `emit_cte_lookup_filter`
  computes `linked_base = num_linked_projs` when projections attach to
  the op (single-leaf aggregator with linked projections, or the new
  filter projections) and offsets every CTE-side buffer position.
  Before this, an aggregate query combining GROUP BY linked
  projections with a filter on the CTE's own outputs silently compared
  the wrong buffer entries (no MTR case combined the two; sc-17 pins
  the three-way position agreement now).  Root CTE ops keep base 0 (a
  root has no ancestors).
- **W2 — typed col-vs-col**: the col-vs-col arm resolves each side to
  (buffer position, NDB type) — CTE output or tree-ancestor real
  column — and emits NULL guards + `read_linked_column_to_reg` +
  inverted typed reg-vs-reg branch.  Envelope: the 10 integer widths +
  Float + Double; DECIMAL / string / temporal rejected with a clear
  message (sc-P4).
- **W4 — classification**: `classify_where_by_table`'s cross-table arm
  routes a conjunct to the CTE op's filter when the scope is the main
  scope (CTE-body scopes deferred), the higher op is a CTE_LOOKUP, the
  other op is a TREE ANCESTOR of it, every atom side is a bare
  identifier or constant, and projection bookkeeping is safe (no
  multi-leaf aggregation; any single-leaf aggregator sits on the CTE
  op itself — the plan's linked_projs list is shared, so an aggregator
  elsewhere would inherit the filter's projections and its positions
  would shift).  Classification only MARKS
  (`JoinOp::needs_parent_linked_attrs`) and merges the CE; the
  ancestor columns' projections are registered later by
  `register_cte_filter_linked_projs`, after load_join's GROUP BY
  linked-projection block — **first-record finding (sc-17 data-node
  crash)**: the aggregation program's `GroupByLinked` positions are a
  sequential counter over the GROUP BY list, so GB projections must
  occupy the FIRST slots of `plan.linked_projs`; registering filter
  columns at classification time (which runs before the GB block)
  shifted them, and the target aggregator read an INT entry as its
  VARCHAR group key (`NdbSqlUtil::cmpVarchar` length `require` in
  `GBHashTable::find` via `cteLookupAggFeed`).  The GB-first invariant
  is now documented at the GB block; every other producer dedups via
  `find_or_add_linked_proj`.  Everything else falls through to the
  cross-table machinery unchanged (sibling branches: sc-P1).
- **W5 — emit**: `emit_child_ops` attaches the plan's linked
  projections to a filtered CTE op that has no aggregator (the
  aggregator branch already attached them otherwise).
- **W6 — parent-side atoms**: `emit_cte_lookup_filter`'s
  col-vs-const arm gained an ancestor-column arm — position = the
  projection index, and the entry is a real-table
  `[tableId][schemaVersion]`-prefixed value, exactly what the
  `branch_linked_mem_*` family resolves; constants encode against the
  real column descriptor.
- NULL semantics: `branch_linked_isnull` guards both operands before
  any typed load — UNKNOWN rejects the row/disjunct (mandatory: a NULL
  register would raise ZREGISTER_INIT_ERROR instead of rejecting).
  A NULL watermark (MAX over empty) therefore yields an empty result /
  COUNT(*)=0, matching MySQL (sc-13).

## MTR (×5 topology suites; user runs `--record`)

`body_passthrough_scalar_cte.inc`: sc-11 = the exact former sc-P1
probe (seeded to pass rows); sc-12a/b/c operator sweep; sc-13 NULL
watermark → empty; sc-14 signed INT vs Bigunsigned COUNT; sc-15 mixed
ancestor + CTE-vs-const atoms; sc-16 aggregate twin; sc-17 GROUP BY
parent col + mixed atoms (the base-offset pin); sc-18 snowflake +
root-column watermark; new sc-P1 = sibling-branch (non-ancestor)
rejection; sc-P4 = DECIMAL operand rejection.
`body_root_pk_residual.inc`: rpr-P1 flips to rpr-16 (rejecting group →
empty) + rpr-16b (accepting).  `suite/ronsql/ronsql_cte_colvscol`
(second first-record finding, expected): its Test 9 pinned the old
Bigint-only rejection for Bigunsigned col-vs-col — flipped to a
positive mixed-signedness compare; Test 10's expected-error text
updated to the new "only integer, FLOAT and DOUBLE operands" message
(strings stay rejected).  That baseline needs a re-record (single
suite).  NOT affected: mri-9 (real-table root col-vs-col residual —
normal-interpreter path) and the D4/D12 CTE-body col-vs-col
rejection-asserts (`check_no_cte_body_col_vs_col` unchanged).

Regression: full `ronsql_cte` ×5 (W1 touches every CTE_LOOKUP send) +
`ronsql` + `ndb_push_agg`.  The multi-node topologies (ng2r2/ng4r2)
are the real proof of the new non-agg DBSPJ expansion flow.

## Risks / notes

- The non-agg expansion is the one genuinely new kernel flow
  (per-parent-row AttrInfo expansion feeding `runCteFilter` then
  `cteLookupEmitResult`); the final-read region precedes the
  subroutine section, so the row emit is transparent to it.  Watch
  `ZATTR_BUFFER_SIZE` headroom.
- Position accounting has three writers (aggregation program's
  cte_base_pos, the filter emission's linked_base, DBSPJ's prepend
  order) — sc-17 pins their agreement.
- Float/Double coverage is thin; if a Double case misbehaves on
  record, shrink the whitelist to integers-only (one edit in
  `is_typed_reg_loadable`) without touching the architecture.

## Deferred sub-shapes (pinned where noted)

- Non-ancestor (sibling-branch) parent columns — needs planner
  re-parenting (sc-P1).
- CTE-root with real-table DESCENDANT comparisons (`FROM cf JOIN cu
  ... WHERE cu.x > cf.n`) — the real op is the child; needs the SPJ
  `branch_col_*_param` route or the cross-table machinery.
- Cross-CTE comparisons (`a.m > b.n`); arithmetic on either side;
  3+-table conjuncts; DECIMAL/string/temporal operands (sc-P4);
  CTE-body scopes.
