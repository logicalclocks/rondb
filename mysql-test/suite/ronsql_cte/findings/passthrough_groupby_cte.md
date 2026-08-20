# passthrough_groupby_cte family — findings

Phase 5 of `non_aggregate_pushdown_plan.md` (detailed plan:
`non_aggregate_phase_5.md`): grouped (GROUP BY) aggregating CTEs
joined anywhere into projection-only snowflake/star trees.  Zero code
changes — the gate (I.8 heritage), planner sibling re-chaining, emit
tree-parent pinning, drain and printer were all already general.
Cases gc-1..13 lock in the supported envelope; gc-P1..P3 pin the
rejections.  gc-7/8/9 are first-ever-on-either-path topology probes
(different-parent CTE branches; real PK lookup below a CTE_LOOKUP;
CTE_SCAN root with a two-level real chain) — **all three passed on
first record ×5 topologies**: SPJ accepts these shapes with no kernel,
API, or RonSQL change.

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| CTE-branch + real-branch star | gc-3 (`customer JOIN nation JOIN pc`) | covered — the D5 fan-out topology, legal on the pass-through path because there is no aggregation reading across sibling branches (the `linked_source_is_leaf_ancestor` guard only fires for agg linked projections) | cte_fix_plan.md H6 / F-fanout | body_passthrough_groupby_cte.inc |
| Partial-key join to a multi-key CTE | `... JOIN pt ON pt.k = g.g_a` (1 of 2 keys) | rejection-assert (gc-P1) — generic gate message; evaluating the I.16b/c root-rewrite for non-aggregate queries is a follow-up | cte_filter_phase_i16.md | body_passthrough_groupby_cte.inc |
| Multi-key CTE keyed off two aliases | `... JOIN pt ON pt.k = o.o_custkey AND pt.cl = cu.c_custkey` | rejection-assert (gc-P2) — clean planner error ("All ON conditions in a single JOIN must reference the same parent table"); per-key parent sources are feasible later (`linkedValue` accepts any ancestor) but deferred | QueryPlanner.cpp parent resolution | body_passthrough_groupby_cte.inc |
| Non-aggregating CTE body | `WITH v AS (SELECT o_orderkey AS k FROM orders) ...` | rejection-assert (gc-P3) — rejects at the parse gate (a no-GROUP-BY keyed CTE fails `cte_key_coverage`, generic message) before `analyze_ctes`'s "CTE bodies must aggregate" is reached; the deeper restriction stands either way and lifting it is a separate kernel-facing feature | non_aggregate_pushdown_plan.md kept restrictions | body_passthrough_groupby_cte.inc |
