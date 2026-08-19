# dtwide family — findings

Data-type width × signedness × boundary matrix over a self-contained
wide-type table (`dtw`, 244 rows) + dimension table (`dtw_dim`).
20 MAIN cases (dtw-01..dtw-20). See `include/body_dtwide.inc` and
`test_benchmark_extension_plan.md` (Phase T1).

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|-------|--------------------|-------------|----------------------------|----------|
| Scalar-CTE duplicate feed (dtw-19/19b) | `WITH t AS (SELECT MIN(d_bi) AS mn FROM dtw) SELECT MAX(t.mn) FROM t` | **FIXED** — re-enabled as live cases dtw-19 (full table, boundary rows) + dtw-19b (WHERE d_id <= 240, the 2-node reproducer) | Root cause was NOT a merge bug: cteScanAggFeed's scalar branch had no single-feeder rule, so every node fed its LOCAL pre-redistribute accumulators into the main aggregator alongside the DBTC-node owner's merged ones — the main MAX over those duplicate rows computed max-of-per-node-minima. Fixed by the m_cte_scalar_shipped gate (set when a peer ships its scalar accumulators in the I.17e redistribute) + inbound scalar merges bumping processed_rows so a zero-local-rows owner still feeds | body_dtwide.inc dtw-19/dtw-19b |
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
