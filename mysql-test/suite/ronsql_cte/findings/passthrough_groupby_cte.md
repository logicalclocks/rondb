# passthrough_groupby_cte family — findings

Phase 5 of `non_aggregate_pushdown_plan.md` (detailed plan:
`non_aggregate_phase_5.md`): grouped (GROUP BY) aggregating CTEs
joined anywhere into projection-only snowflake/star trees.  Zero code
changes — the gate (I.8 heritage), planner sibling re-chaining, emit
tree-parent pinning, drain and printer were all already general.
Cases gc-1..13 lock in the Phase 5 envelope; gc-14..20 add the Phase 6
(`non_aggregate_phase_6.md`) partial-key-rewrite and per-key-parent
shapes; gc-P1a/b/c, gc-P2b and gc-P3 pin the remaining rejections.  gc-7/8/9 are first-ever-on-either-path topology probes
(different-parent CTE branches; real PK lookup below a CTE_LOOKUP;
CTE_SCAN root with a two-level real chain) — **all three passed on
first record ×5 topologies**: SPJ accepts these shapes with no kernel,
API, or RonSQL change.

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| CTE-branch + real-branch star | gc-3 (`customer JOIN nation JOIN pc`) | covered — the D5 fan-out topology, legal on the pass-through path because there is no aggregation reading across sibling branches (the `linked_source_is_leaf_ancestor` guard only fires for agg linked projections) | cte_fix_plan.md H6 / F-fanout | body_passthrough_groupby_cte.inc |
| Partial-key join to a multi-key CTE | `... JOIN pt ON pt.k = g.g_a` (1 of 2 keys) | **SHIPPED** (gc-14..16, `non_aggregate_phase_6.md`): the I.16b/c root-rewrite runs pre-gate for non-aggregate queries — CTE promoted to CTE_SCAN root, original root demoted to keyed child.  Residual partial shapes (LEFT on the partial join gc-P1a, non-root parent gc-P1b, wrong-column keys gc-P1c) throw the clean I.16a/I.20-mirror messages instead of the generic gate text | cte_filter_phase_i16.md | body_passthrough_groupby_cte.inc |
| Multi-key CTE keyed off two aliases | `... JOIN pt ON pt.k = o.o_custkey AND pt.cl = cu.c_custkey` | **SHIPPED** (gc-17..20, `non_aggregate_phase_6.md`): per-key parent sources — QueryPlanner resolves every ON condition's parent, effective parent = deepest key source (all sources must lie on one ancestor chain, mirroring linkWithParent), emit links each key from its own source op; applies to the aggregate path too (gc-20).  Sibling-branch key sources stay rejected with the clean "single ancestor chain" planner error (gc-P2b) | QueryPlanner.cpp parent resolution | body_passthrough_groupby_cte.inc |
| gc-18 empty-result proof | the former gc-P2 probe joins `pt.cl = cu.c_custkey` with `cu.c_custkey = o.o_custkey` | no (custkey, clerk) group has clerk == custkey: clerk = MOD(n,40)+1, custkey = MOD(n-1,300)+1 ⇒ m ≡ c-1 (mod 300) ∧ m ≡ c-2 (mod 40) ⇒ 20j ≡ 39 (mod 40), unsolvable | data note | body_passthrough_groupby_cte.inc |
| Non-aggregating CTE body | `WITH v AS (SELECT o_orderkey AS k FROM orders) ...` | rejection-assert (gc-P3) — rejects at the parse gate (a no-GROUP-BY keyed CTE fails `cte_key_coverage`, generic message) before `analyze_ctes`'s "CTE bodies must aggregate" is reached; the deeper restriction stands either way and lifting it is a separate kernel-facing feature | non_aggregate_pushdown_plan.md kept restrictions | body_passthrough_groupby_cte.inc |
