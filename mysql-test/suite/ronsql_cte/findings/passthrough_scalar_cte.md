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
envelope; sc-11..18 add the Phase i26 real-vs-CTE comparisons;
sc-P1..P4 pin the remaining rejections.

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| Real column vs scalar-CTE output | `WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders) SELECT cu.c_custkey FROM customer AS cu, s WHERE cu.c_custkey > s.m;` | **SHIPPED** (sc-11..18, `cte_filter_phase_i26.md`): routed to the CTE_LOOKUP jump-table filter — real column rides the linked-attr buffer as a linked parent projection (DBSPJ non-agg expansion + typed-register comparisons), both paths.  The route also fixed a latent base-offset bug (agg linked projections + CTE-output filter compared wrong buffer entries — sc-17 pins) and closed rpr-P1 (typed col-vs-col beyond Bigint, now rpr-16/16b) | cte_filter_phase_i26.md | body_passthrough_scalar_cte.inc |
| Sibling-branch real column vs scalar output | `... o JOIN cu ..., s WHERE cu.c_custkey > s.m` | rejection-assert (new sc-P1) — the routed shape requires the real column's op to be a tree ancestor of the CTE op | cte_filter_phase_i26.md deferred | body_passthrough_scalar_cte.inc |
| DECIMAL operand in real-vs-CTE compare | `... WHERE cu.c_acctbal > s.m` | rejection-assert (sc-P4) — typed-register envelope is integers + FLOAT + DOUBLE | cte_filter_phase_i26.md deferred | body_passthrough_scalar_cte.inc |
| Comma cross-join of real tables | `SELECT ... FROM customer AS cu, orders AS o;` | rejection-assert (sc-P2) | conditionless joins accepted only for scalar CTE operands | body_passthrough_scalar_cte.inc |
| Grouped CTE comma-joined | `WITH g AS (... GROUP BY ...) SELECT ... FROM customer AS cu, g;` | rejection-assert (sc-P3) | Partial coverage — same envelope as the aggregate path | body_passthrough_scalar_cte.inc |
| 3+ sibling scalar CTEs | `SELECT a.m, b.n, c.r FROM a, b, c` | covered (sc-8) — first-ever 3-CTE chain on either path; enabled by the ScalarDummy `setParent(tree_parent_op_idx)` fix (the old `setParent(root)` clobbered the planner's sibling chain, re-creating the topology SPJ rejects) | non_aggregate_phase_4.md W2 | body_passthrough_scalar_cte.inc |
| cs-probe-7 in body_chain_scalar.inc | `SELECT a.m, b.n FROM a, b` | now supportable — covered here as sc-2; left disabled in body_chain_scalar.inc to avoid re-recording that family | — | body_chain_scalar.inc |
