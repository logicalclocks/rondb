# passthrough_scalar_cte family — findings

**Kernel defect found by sc-6 on first record (FIXED)**: the scalar
CTE_LOOKUP arm in DblqhMain.cpp REFed `GROUP_NOT_FOUND` when
`processed_rows() == 0` ("Empty table — no result to return"),
dropping every cross-join parent row via MatchNonNull — RonSQL
returned zero rows where MySQL returns the parents NULL-extended by
the scalar's one empty-input row.  The guard predated I.17's
empty-input semantics: the scalar CTE_SCAN emit path deliberately
emits one row for empty input (Init pre-zeroes COUNT slots, leaves the
rest is_null), and the lookup path now emits the same `m_agg_results`.
The AGGREGATE path had the identical silent bug (a COUNT over the
cross join returned 0 instead of the parent count) — pinned by sc-10.

Phase 4 of `non_aggregate_pushdown_plan.md` (detailed plan:
`non_aggregate_phase_4.md`): scalar aggregating CTEs comma-cross-joined
into projection-only queries.  Cases sc-1..9 lock in the supported
envelope; sc-P1..P3 pin the rejections.

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| Real column vs scalar-CTE output | `WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders) SELECT cu.c_custkey FROM customer AS cu, s WHERE cu.c_custkey > s.m;` | rejection-assert (sc-P1) — **deferred feature, missing on BOTH paths**: classifies cross-table; the non-agg path rejects at compile(), and even the aggregate embedded-filter machinery requires `StoredColumn` operands (no test anywhere exercises it).  Fix directions: extend `emit_cte_lookup_filter` to linked parent columns, or the cross-table machinery to CTE operands | non_aggregate_phase_4.md "out of scope" | body_passthrough_scalar_cte.inc |
| Comma cross-join of real tables | `SELECT ... FROM customer AS cu, orders AS o;` | rejection-assert (sc-P2) | conditionless joins accepted only for scalar CTE operands | body_passthrough_scalar_cte.inc |
| Grouped CTE comma-joined | `WITH g AS (... GROUP BY ...) SELECT ... FROM customer AS cu, g;` | rejection-assert (sc-P3) | Partial coverage — same envelope as the aggregate path | body_passthrough_scalar_cte.inc |
| 3+ sibling scalar CTEs | `SELECT a.m, b.n, c.r FROM a, b, c` | covered (sc-8) — first-ever 3-CTE chain on either path; enabled by the ScalarDummy `setParent(tree_parent_op_idx)` fix (the old `setParent(root)` clobbered the planner's sibling chain, re-creating the topology SPJ rejects) | non_aggregate_phase_4.md W2 | body_passthrough_scalar_cte.inc |
| cs-probe-7 in body_chain_scalar.inc | `SELECT a.m, b.n FROM a, b` | now supportable — covered here as sc-2; left disabled in body_chain_scalar.inc to avoid re-recording that family | — | body_chain_scalar.inc |
