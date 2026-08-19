# bigquery family — findings

Larger / combined CTE queries on the shared realistic schema: 12 MAIN
cases (big-01..big-12) that compose shapes proven individually by the
other families. See `include/body_bigquery.inc` and
`test_benchmark_extension_plan.md` (Phase T2).

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|-------|--------------------|-------------|----------------------------|----------|
| Anti-join + positive INNER CTE join in one main (big-07) | `customer LEFT JOIN hiprio ON k=c_custkey JOIN lifetime ON k2=c_custkey WHERE hiprio.cnt IS NULL GROUP BY c_nationkey` | NEXT-PHASE-disabled | **HANG** (NDB 274 deadlock timeout ×10 retries, 2026-08-19). Anti-join alone green (Phase K); LEFT+INNER without IS NULL green. Suspect: ANTI_JOIN promotion + second agg-feed CTE_LOOKUP in the JoinAgg completion protocol | body_bigquery.inc big-07 |
| GREATEST/LEAST body WHERE + CASE main aggregate at 8 nodes (big-06) | (full query in body_bigquery.inc big-06 probe; bisect halves included) | NEXT-PHASE-disabled | **RDRS PROCESS CRASH on ng4r2 only** (curl exit 52, 2026-08-19); green on default/ng1r3/ng2r2/ng2r3. Crash is in RDRS, so suspect API-side handling of the 8-node result stream for this shape; rdrs crash trace pins the abort site | body_bigquery.inc big-06 |
| High-cardinality CTE key under a large root scan | (canonical D6 repro lives in the agg family findings) | NEXT-PHASE-disabled (not duplicated) | D6 DBSPJ assert 2343, cte_fix_plan.md Phase 2, unfixed | body_bigquery.inc probe block |
| String MIN/MAX re-aggregation via ordered-index child | (canonical D18 repro lives in the discovery log) | NEXT-PHASE-disabled (not duplicated) | D18 Error 6000, cte_fix_plan.md Phase 2, unfixed | body_bigquery.inc probe block |

Notes:

- big-01 scales sibling CTEs from the proven 2 to 4, mixing entity-keyed
  (o_custkey), nation-keyed (c_nationkey) and string-keyed
  (c_mktsegment) lookups in one main.
- big-04 crosses the 256-group API batch boundary at BOTH levels (300
  CTE groups, 300 main GROUP BY groups) with 7 aggregate outputs.
- big-12's ORDER BY has heavy ties by construction (total_spr takes 5
  distinct values); the `c_custkey ASC` tiebreak keeps the LIMIT 10 set
  deterministic.
- D22 caution (wrong COUNT under 6/8-node redistribution) applies to
  the ng2r3 / ng4r2 recordings: a divergence seen only on those
  topologies should be recorded here as a finding + the case disabled
  with a `# NEXT-PHASE` marker, not force-recorded.
