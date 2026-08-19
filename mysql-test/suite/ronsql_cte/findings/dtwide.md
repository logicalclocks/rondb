# dtwide family — findings

Data-type width × signedness × boundary matrix over a self-contained
wide-type table (`dtw`, 244 rows) + dimension table (`dtw_dim`).
20 MAIN cases (dtw-01..dtw-20). See `include/body_dtwide.inc` and
`test_benchmark_extension_plan.md` (Phase T1).

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|-------|--------------------|-------------|----------------------------|----------|
| Scalar-CTE MIN, any topology (dtw-19b) | `WITH t AS (SELECT MIN(d_bi) AS mn FROM dtw) SELECT MAX(t.mn) FROM t` | NEXT-PHASE-disabled | **WRONG RESULTS, 2026-08-19, all topologies**: the MIN arm of the scalar cross-node merge appears polarity-inverted (keeps the LARGER of the local minima) — correct only when the global min is scanned on the owner node. Three data points: ng4r2 full-table lost Int64-min; default topology with `d_id <= 240` returned row 3's value instead of row 1's; default full-table was right by row placement. MAX merges correctly everywhere; GROUPED MIN (dtw-05/16) correct everywhere. Suspect: MIN arm of mergeScalarAccumulators (I.17e keyLen==0 redistribute). Audit other scalar-CTE MIN users (chain_scalar family, offline_fs_scalar, cte_tpch_q15) — possibly green by placement luck | body_dtwide.inc dtw-19b probe |
| SUM over FLOAT / DOUBLE | `WITH t AS (SELECT d_grp AS g, SUM(d_f) AS sf FROM dtw GROUP BY d_grp) SELECT SUM(t.sf) FROM dtw_dim AS m JOIN t ON t.g = m.g_id` | NEXT-PHASE-disabled | Floating-point summation is non-associative; RonSQL partial-sum order differs from mysql in low-order ULPs — not strict-diff-testable (same as body_agg.inc) | body_dtwide.inc probe block |
| CTE body GROUP BY a NULLABLE typed key | `WITH t AS (SELECT d_ti AS g, COUNT(*) AS n FROM dtw GROUP BY d_ti) SELECT SUM(t.n) FROM t` | NEXT-PHASE-disabled | NULL group hashing/merge semantics for the width matrix unproven; existing families group on NOT NULL keys | body_dtwide.inc probe block |
| MIN/MAX over TIMESTAMP | — | not repeated | Covered by body_agg.inc agg-d17i/j under time_zone='+00:00' | body_agg.inc |

Notes:

- SUM cases restrict the CTE body to `d_id <= 240` so no Int64/Uint64
  accumulator can overflow under ANY kernel merge order; the boundary
  rows (Int64 min/max, Uint64 max, per-width extremes) are exercised by
  MIN/MAX cases only (dtw-01..05, dtw-16, dtw-19).
- FLOAT/DOUBLE columns use exact binary fractions (quarters / eighths)
  so MIN/MAX display is engine-independent.
- The empty-string and multi-byte (`åäö`) rows in dtw-10 pin
  charset-aware string MIN/MAX (Phase I.6 S.2).
