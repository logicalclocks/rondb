# mainmode family — findings

Family focus: one aggregating CTE (`cust`, `GROUP BY o_custkey` → 300 groups)
feeding the two main-SELECT modes the user cares about — (a) normal-join
PROJECTION-ONLY main SELECTs (no main aggregation) and (b) AGGREGATING main
SELECTs (P-GB: GROUP BY a parent-table column over a CTE aggregate leaf) — plus
multi-batch (>256-group) redistribution stress.

## HEADLINE FINDING — projection-only main SELECTs over a CTE_LOOKUP HANG (D3)

The whole point of this family was to contrast the two main-SELECT modes. The
recording pass made that contrast stark: **mode (a) — projection-only main
SELECTs over a CTE_LOOKUP — HANG (never return)** on this build, while **mode
(b) — the SAME CTE wrapped in a main aggregate (P-GB) — is GREEN.** This is the
single most important mainmode result.

- Repro (smallest): `WITH nat AS (SELECT n_regionkey AS rk, MIN(n_nationkey) AS
  mn, MAX(n_nationkey) AS mx, COUNT(*) AS n FROM nation GROUP BY n_regionkey)
  SELECT nat.rk, nat.mn, nat.mx, nat.n FROM region AS r JOIN nat ON nat.rk =
  r.r_regionkey;` → HANG (discovery log D3).
- Same class as D2 (`COUNT(<col>)` in a CTE feeding a projection-only main join
  also hangs). Projecting CTE_LOOKUP outputs without a wrapping main aggregate
  never returns; INNER vs LEFT, bounded vs unbounded (multi-batch), single vs
  multiple CTE columns, and CTE_SCAN-root-vs-child all hang identically.
- Workaround (and the supported shape): wrap the CTE outputs in a main
  aggregate and GROUP BY a parent column or scalar — i.e. mode (b) / P-GB.

Consequently every projection-only main SELECT in this family is DISABLED as a
`# NEXT-PHASE` probe; only the aggregating mains stay enabled.

## MAIN (green, aggregating) cases — 7

| Case | Mode | Shape |
|------|------|-------|
| MM7  | (b) aggregating | P-GB: `SUM(cust.t), SUM(cust.n)` GROUP BY `c_mktsegment` (INNER) |
| MM8  | (b) aggregating | P-GB LEFT join with unmatched parents (filtered CTE), SUM ignores NULL |
| MM9  | (b) aggregating | P-GB GROUP BY `c_nationkey`, `SUM(cust.t), COUNT(*)` |
| MM10 | (b) aggregating | P-GB `MIN/MAX` over CTE agg cols, GROUP BY `c_mktsegment` |
| MM11 | (b) aggregating | scalar (no GROUP BY) `SUM(cust.t), SUM(cust.n), COUNT(*)` over CTE leaf |
| MM12 | (b) aggregating | `cte JOIN real_table` (CTE parent), GROUP BY parent col |
| MM15 | multi-batch (b) | aggregating GROUP BY parent `c_custkey` over CTE leaf → 300 groups |

All seven wrap the CTE outputs in a main aggregate (SUM/COUNT/MIN/MAX) and
GROUP BY a parent column (or scalar in MM11). They should be GREEN after the
global `SUM(<decimal>)` fix (the CTE body uses only integer SUM —
`SUM(o_shippriority)` — plus `COUNT(*)` / MIN / MAX). MM15 is the
redistribution-stress case: `GROUP BY c_custkey` → 300 output groups, crossing
the 256-row API batch boundary and exercising multi-node redistribution. The
harness sorts output, so row order does not matter.

## PROBES (recorded + disabled) — 13

All probes are written COMMENTED OUT under `# NEXT-PHASE:` markers and are NOT
executed (form 2 in the guide — genuine gap / hang / not-yet-implemented, not a
clean permanent rejection). None of this family's deferred shapes are
clean-reject candidates, so there are no `rejection-assert` probes here.

### Projection-only-over-CTE_LOOKUP hang (D3) — 9 probes

These were the family's original MAIN cases (MM1–MM6, MM13, MM14, MM16);
recording showed they all HANG, so they are demoted to probes. They are the
direct demonstration of the headline finding.

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|-------|---------------------|-------------|----------------------------|----------|
| proj-only INNER join, complete-key lookup, bounded | `SELECT c.c_custkey, c.c_mktsegment, cust.t, cust.n FROM customer AS c JOIN cust ON cust.k=c.c_custkey WHERE c.c_custkey<=20` | NEXT-PHASE-disabled | projection-only over CTE_LOOKUP hang (D3/D2) | body_mainmode.inc PROBES block |
| proj-only LEFT join, complete-key lookup, bounded | `... LEFT JOIN cust ON cust.k=c.c_custkey WHERE c.c_custkey<=20` | NEXT-PHASE-disabled | D3/D2 | body_mainmode.inc PROBES block |
| proj-only LEFT join, unmatched parents (filtered CTE) | `WHERE o_orderkey<=150 GROUP BY o_custkey) ... LEFT JOIN cust ... WHERE c.c_custkey<=200` | NEXT-PHASE-disabled | D3/D2 | body_mainmode.inc PROBES block |
| proj-only INNER join, single MAX agg col | `MAX(o_totalprice) AS mx ... SELECT c.c_custkey, cust.mx ... JOIN cust ... WHERE c.c_custkey<=15` | NEXT-PHASE-disabled | D3/D2 | body_mainmode.inc PROBES block |
| proj-only INNER join + WHERE on CTE agg output | `... JOIN cust ... WHERE c.c_custkey<=60 AND cust.n>=5` | NEXT-PHASE-disabled | D3/D2 | body_mainmode.inc PROBES block |
| proj-only cte JOIN real_table (CTE_SCAN root) | `SELECT cust.k, cust.t, c.c_mktsegment FROM cust JOIN customer AS c ON c.c_custkey=cust.k WHERE cust.k<=25` | NEXT-PHASE-disabled | D3/D2 | body_mainmode.inc PROBES block |
| multi-batch proj-only over full 300-group CTE | `SELECT cust.k, cust.t FROM customer AS c JOIN cust ON cust.k=c.c_custkey` (~300 rows) | NEXT-PHASE-disabled | D3/D2 | body_mainmode.inc PROBES block |
| multi-batch proj-only CTE_SCAN root | `SELECT cust.k, cust.t, cust.n FROM cust JOIN customer AS c ON c.c_custkey=cust.k` (~300 rows) | NEXT-PHASE-disabled | D3/D2 | body_mainmode.inc PROBES block |
| multi-batch LEFT join proj-only, all 300 custkeys | `SELECT c.c_custkey, cust.t, cust.n FROM customer AS c LEFT JOIN cust ON cust.k=c.c_custkey` (~300 rows) | NEXT-PHASE-disabled | D3/D2 | body_mainmode.inc PROBES block |

### Clause-level deferrals (matrix) — 4 probes

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|-------|---------------------|-------------|----------------------------|----------|
| ORDER BY on a CTE query | `... GROUP BY c.c_mktsegment ORDER BY SUM(cust.t) DESC` | NEXT-PHASE-disabled | broader LIMIT/ORDER BY pushdown (matrix; cf. cte_filter_phase_n.md I.14) | body_mainmode.inc PROBES block |
| LIMIT on a CTE query | `... GROUP BY c.c_mktsegment LIMIT 3` | NEXT-PHASE-disabled | broader LIMIT/ORDER BY pushdown (matrix; cf. I.14) | body_mainmode.inc PROBES block |
| DISTINCT in CTE / main SELECT | `SELECT DISTINCT c.c_mktsegment FROM customer AS c JOIN cust ...` | NEXT-PHASE-disabled | DISTINCT deferred (matrix) | body_mainmode.inc PROBES block |
| HAVING on a CTE query | `... GROUP BY c.c_mktsegment HAVING SUM(cust.t) > 1000000` | NEXT-PHASE-disabled | HAVING deferred (matrix) | body_mainmode.inc PROBES block |

## Notes

- The ORDER BY / LIMIT probes were rebuilt onto an AGGREGATING main (GROUP BY +
  SUM) rather than the old projection-only forms, so that if these clauses are
  ever enabled the underlying join shape is itself supported (mode (b)).
- All table aliases use explicit `AS`; CTE names (`cust`) are referenced
  without an alias, per the parser constraint.
- The same `cust` CTE body (integer `SUM(o_shippriority)` + `COUNT(*)`, or
  scalar `MAX(o_totalprice)`) is reused across cases on purpose, to isolate the
  difference to the main-SELECT mode rather than the CTE shape. No
  `SUM(<decimal>)` is used (globally fixed; not reintroduced).
- Data semantics relied on: `o_custkey = ((orderkey-1)%300)+1` (custkey c first
  appears at orderkey c) is the basis for the MM8 unmatched-parent bound `WHERE
  o_orderkey <= 150`. `GROUP BY o_custkey` → 300 groups underpins MM15 (the
  enabled redistribution-stress case) and the disabled multi-batch probes.
