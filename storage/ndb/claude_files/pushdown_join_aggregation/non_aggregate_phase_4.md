# Non-Aggregate Pushdown — Phase 4 Detailed Plan

**Status: IMPLEMENTED (August 2026) — pending user build + MTR
--record.**

**Kernel fix found by sc-6 on first record**: the scalar CTE_LOOKUP
arm in `DblqhMain.cpp` REFed `GROUP_NOT_FOUND` on
`processed_rows() == 0`, dropping every cross-join parent row of an
empty-input scalar CTE (MatchNonNull) — on the AGGREGATE path too
(silently wrong COUNTs; sc-10 pins the twin).  The guard predated
I.17's empty-input semantics; the lookup arm now emits the same
Init-prepared `m_agg_results` (COUNT = 0, others NULL) that the
scalar CTE_SCAN emit path already produces for empty input.  This is
an `ndbmtd` change — rebuild the kernel, not just ronsql.

Implementation notes: landed as planned — W1's two gate changes
(conditionless comma joins admitted only for scalar CTE operands;
ScalarDummy added to the coverage accept-list) plus the rejection-text
update; W2's one-line `setParent(tree_parent_op_idx)` fix; W3 is
`body_passthrough_scalar_cte.inc` (sc-1..9 + sc-5b + sc-P1..P3) ×5
suites + `findings/passthrough_scalar_cte.md`.  sc-2's root is a
scalar `scanCte` (the no-WHERE scalar-root path) with a ScalarDummy
child — the Test-6 topology under a pass-through drain; sc-8 is the
first-ever 3-CTE chain on either path.
**Parent:** `non_aggregate_pushdown_plan.md` (Phase 4 overview).
**Predecessors:** Phases 0-3 + nest-semantics Parts A/B.

## Goal

Scalar (single-row, no GROUP BY) aggregating CTEs joined into
projection-only main queries:

```sql
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders)
SELECT cu.c_custkey, s.m FROM customer AS cu, s WHERE cu.c_custkey < 6;
```

## How the research reshaped the parent plan's sketch

1. **Scalar-CTE joins are comma cross-joins, full stop.**  The grammar
   has exactly four join productions; the only `conditions == NULL`
   form is `T_COMMA table_ref` (a bare `JOIN stats` is a syntax
   error), and the comma form is the documented Test-20
   dummy-key-lookup pattern.  A scalar CTE always produces exactly one
   row (the kernel emits COUNT→0 / MAX→NULL even over empty input), so
   the cross join is a semantics-preserving 1-row multiplier — MySQL
   agrees.  `LEFT JOIN <scalar cte>` is not expressible (LEFT requires
   ON), so the parent plan's "LEFT JOIN scalar CTE" item is void.
2. **Real-vs-scalar comparisons (`t.col > s.m`) are a NEW feature, not
   gate acceptance** — they classify as cross-table filters, which the
   non-aggregate path rejects at `compile()` and which even the
   AGGREGATE path's embedded-filter machinery rejects
   (`require_prm(kind == StoredColumn)` in
   `emit_embedded_filter_expr`; no test anywhere exercises it).  The
   parent plan's 4b is therefore **deferred** as its own follow-up
   (extend `emit_cte_lookup_filter` to linked parent columns, or the
   cross-table machinery to CTE operands — both paths at once).
   Predicates on the scalar CTE's **own** outputs (`WHERE s.m > 5`)
   already route to the CTE op and emit through the
   aggregation-independent `emit_cte_lookup_filter` arm.
3. **Emit, drain, printer need zero changes.**  The ScalarDummy
   `emit_child_ops` arm has no aggregator precondition; the
   pass-through drain registers all virt columns generically; the
   scalar virt PK is a *nullable* primary key (I.21 metadata
   decoupling) so NULL scalars flow into the printer's existing NULL
   handling; column metadata (temporal/DECIMAL decode) plumbs through
   `resolve_cte_output_columns_for_scope` kind-agnostically.
4. **One latent bug in scope**: with three or more comma-joined scalar
   CTEs, the planner chains the sibling CTE_LOOKUPs via
   `tree_parent_op_idx` (SPJ rejects un-chained sibling CTEs) and
   `emit_child_ops` honours it — but the ScalarDummy arm's
   `setParent(opDefs[op.parent_op_idx])` then overwrites the option
   with the root, clobbering the chain.  Two CTEs are unaffected
   (`parent == tree_parent`).  Fix: `setParent` on
   `tree_parent_op_idx`.  Affects the aggregate path identically.

## Work items

### W1 — parse gate (two independent changes)

In the `projection_only_join_chain` walk:

1. Accept `join->conditions == NULL` **only** when the operand is a
   scalar CTE comma cross-join (`find_cte_definition` hit with
   `groupby_columns == NULL`, `join_type == INNER_JOIN` — the comma
   production's type).  Real-table comma joins and grouped-CTE comma
   joins stay rejected.  The per-condition loops no-op naturally;
   `any_parent_under_left` stays false (a 1-row cross join is
   LEFT-safe); the alias bookkeeping still pushes the CTE alias.
2. Accept `CteKeyCoverage::ScalarDummy` alongside
   `ExactOrdered`/`ExactPermuted` — inherently restricted to the
   conditionless-scalar case by its producer
   (`num_pk_cols == 0 && num_keys == 0`).

Update the rejection text to name scalar-CTE cross-joins.

### W2 — ScalarDummy `setParent` chain fix

`opts.setParent(opDefs[op.parent_op_idx])` →
`opts.setParent(opDefs[op.tree_parent_op_idx])` in the ScalarDummy
arm.  Identical for every existing shape (the indices differ only for
3+ sibling CTEs, where the current value re-creates the topology SPJ
rejects); a 3-CTE test case probes the fixed shape on both this and
the aggregate path.

### W3 — MTR family

`body_passthrough_scalar_cte.inc` + wrappers ×5 suites (sc-N):

| # | Case |
|---|---|
| sc-1 | real root + scalar CTE: `customer, s` — the never-before-exercised `TABLE_SCAN` root + ScalarDummy child in the pass-through drain |
| sc-2 | two scalar CTEs, projection-only (`SELECT a.m, b.n FROM a, b` — the cs-probe-7 shape, previously "Not an aggregate query") |
| sc-3 | scalar CTE appended to a snowflake chain (`orders JOIN customer ON ..., s`) |
| sc-4 | scalar CTE appended to a star (self-join star + `, s`) |
| sc-5 | WHERE on the scalar output, accepting + rejecting variants (the reject empties the whole cross join) |
| sc-6 | NULL scalar (`MAX` over an empty subset) printed as NULL on every row |
| sc-7 | `COUNT(*)` over an empty subset prints 0 (kernel pre-zeroing) |
| sc-8 | three scalar CTEs (`FROM a, b, c`) — the W2 chain-fix probe |
| sc-9 | multi-output scalar CTE with temporal + DECIMAL outputs (`MIN(o_orderdate)`, `MAX(o_totalprice)`) — 0b metadata decode through the scalar path |
| sc-P1 | rejection: `WHERE cu.c_acctbal > s.m` — pins the deferred real-vs-scalar comparison (cross-table rejection) |
| sc-P2 | rejection: comma cross-join of real tables |
| sc-P3 | rejection: grouped CTE comma-joined (Partial coverage) |

### W4 — docs + findings

`findings/passthrough_scalar_cte.md` carries the deferred
real-vs-scalar comparison (both paths) and notes cs-probe-7 in
`body_chain_scalar.inc` is now supportable (left disabled there to
avoid re-recording that family; covered here as sc-2).

## Deliberately out of scope (tracked)

- ~~**Real-table vs scalar-CTE comparisons** (`t.col > s.m`)~~ —
  **SHIPPED by `cte_filter_phase_i26.md`** (August 2026): routed to
  the CTE_LOOKUP jump-table filter with the real column as a linked
  parent projection; sc-11..18 cover it, the new sc-P1 pins the
  non-ancestor rejection and sc-P4 the DECIMAL-operand rejection.
- `LEFT JOIN <scalar cte>` — not expressible in the grammar.
- Scalar CTE with ON conditions — `Partial` coverage, rejected (same
  as the aggregate path).

## Verification (user-run)

- Rebuild (ronsql), `./mtr --record --suite=ronsql_cte
  ronsql_cte_dd_passthrough_scalar_cte` + 4 siblings; full
  `ronsql_cte` + `ronsql` regression (W2 touches the aggregate
  scalar-CTE emit — cs14/cs15 and the ronsql_cte_scalar tests are the
  anchors, all 2-CTE shapes where the fix is identity).
- Watch sc-8 (first-ever 3-CTE chain) and sc-6/7 (NULL/zero scalars
  through the pass-through printer).
