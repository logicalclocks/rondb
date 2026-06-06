# chain_scalar family — findings

Family focus: chained CTE-of-CTE and scalar (no-GROUP-BY) CTEs at realistic
scale. The base CTE `SELECT o_custkey, SUM(o_shippriority) ... GROUP BY
o_custkey` produces 300 groups (>256), so every chained roll-up phase crosses
the 256-row API batch boundary and exercises multi-batch redistribution.
Scalar CTEs materialise a single virtual-table row including for empty input.

Body include: `suite/ronsql_cte/include/body_chain_scalar.inc`
Wrapper:      `suite/ronsql_cte/t/ronsql_cte_dd_chain_scalar.test`

## Corrected-envelope rework (post first recording pass)

The first recording pass (BEFORE the global SUM-over-decimal fix) showed:

- cs01-06, cs19, cs20 errored "CTE aggregate references unresolved source
  column" — the inner CTE's SUM was over DECIMAL (`o_totalprice`). The suite
  was globally fixed so the inner SUM is now over the integer column
  `o_shippriority`; that error is expected to clear. All eight cases are KEPT
  ENABLED. Each chained case's outer/main SELECT now AGGREGATES — cs01 and
  cs03 (which previously projected a scalar tip) were converted to scalar-agg
  mains over a grouped tip; cs20 was converted from projecting a scalar tip to
  scalar-aggregating a grouped empty intermediate. Chained inner CTEs use only
  COUNT(*) / SUM(int) / MIN / MAX.
- cs07, cs10, cs11, cs16, cs17, cs18 errored SUM-over-decimal — now fixed
  (all use `SUM(o_shippriority)`), KEPT ENABLED.
- cs08 errored "Not an aggregate query" (scalar comma cross-join, projection-
  only) — DISABLED + recorded as cs-probe-7 (D8).
- cs14, cs15 errored "Failed to create child operation" (watermark
  GREATEST/LEAST over two scalar CTEs, comma cross-join) — DISABLED + recorded
  as cs-probe-8 / cs-probe-9 (D8, genuinely unsupported).
- cs09, cs12, cs13 had value diffs at record time — these are single scalar
  CTEs consumed by a single-table main (no cross-join), which is GREEN per the
  corrected envelope; kept enabled for the orchestrator's re-record.

## MAIN (green) cases — 14

| id   | Shape |
|------|-------|
| cs01 | 2-level chain a->b, main scalar-aggregates b (300-group base, multi-batch) |
| cs02 | 2-level chain a->b, both grouped, main grouped (3 GROUP-BY phases at 300 groups) |
| cs03 | 3-level chain a->b->c, main scalar-aggregates c |
| cs04 | 3-level chain a->b->c, all grouped, main grouped |
| cs05 | chained CTE with WHERE filter on the intermediate aggregate output |
| cs06 | chained CTE feeding a real-table (customer) join + main aggregation |
| cs07 | sibling CTEs (sums + cnts) both joined to customer, main re-aggregates |
| cs10 | scalar CTE COUNT/SUM(int)/MIN variants over a populated table |
| cs12 | scalar CTE MAX over EMPTY input -> NULL |
| cs13 | scalar CTE COUNT(*) over EMPTY input -> 0 |
| cs16 | main GROUP BY on a CTE_SCAN root col + agg (300 groups, multi-batch) |
| cs17 | main scalar agg over a CTE_SCAN root (300 groups -> 1 row) |
| cs18 | skewed CTE — WHERE narrows to a single group, joined to customer |
| cs19 | empty-intermediate chain (a always empty -> b empty), grouped main |

### DISABLED MAIN cases

| id   | Shape | Disposition |
|------|-------|-------------|
| cs09 | scalar CTE MAX over a populated table (DECIMAL output) | NEXT-PHASE-disabled (WRONG, D15) |
| cs11 | scalar CTE with multiple aggregate outputs projected together (DECIMAL MIN/MAX) | NEXT-PHASE-disabled (WRONG, D15) |
| cs20 | empty grouped intermediate feeding a scalar-aggregating main | NEXT-PHASE-disabled (WRONG, D16) |

cs09/cs11 — DECIMAL MIN/MAX output drops scale (RonSQL `20055`, MySQL
`20055.00`); RonSQL strips trailing zeros from whole-number DECIMAL aggregate
output (D15). cs20 — scalar/chained COUNT(*) over EMPTY input returns NULL
(RonSQL) vs 0 (MySQL) (D16). All three recorded commented-out under NEXT-PHASE
markers in body_chain_scalar.inc.

## PROBES — 8 (6 NEXT-PHASE-disabled, 2 rejection-asserts)

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|-------|---------------------|-------------|----------------------------|----------|
| Nullable scalar CTE output feeding top-level GREATEST (scalar comma cross-join) | `WITH a AS (SELECT MAX(o_clerk) AS m FROM orders WHERE o_orderkey = 7), b AS (SELECT MIN(o_clerk) AS n FROM orders WHERE o_orderkey = 7) SELECT GREATEST(a.m, b.n) AS g FROM a, b;` | NEXT-PHASE-disabled | I.21 nullable-first-output / GREATEST NULL semantics + D8 scalar cross-join | body cs-probe-1 |
| Chained CTE feeding a multi-key CTE complete-key lookup | `WITH a AS (SELECT o_custkey AS k, o_orderstatus AS st, SUM(o_shippriority) AS t FROM orders GROUP BY o_custkey, o_orderstatus), b AS (SELECT k, st, MAX(t) AS m FROM a GROUP BY k, st) SELECT b.k, b.st, b.m FROM customer AS c JOIN b ON b.k = c.c_custkey;` | NEXT-PHASE-disabled | I.16/I.20 multi-key lookup over a chained intermediate | body cs-probe-2 |
| Post-aggregation expression in main SELECT over a CTE aggregate | `WITH a AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t FROM orders GROUP BY o_custkey) SELECT k, SUM(t) + 1 FROM a GROUP BY k;` | NEXT-PHASE-disabled | DEFERRED — SELECT of a post-aggregation expression (matrix) | body cs-probe-3 |
| AVG in a scalar CTE body | `WITH x AS (SELECT AVG(o_totalprice) AS a FROM orders) SELECT a FROM x;` | NEXT-PHASE-disabled | DEFERRED — AVG (matrix) | body cs-probe-4 |
| Scalar-CTE comma cross-join, project both outputs (was cs08) | `WITH a AS (SELECT MAX(o_totalprice) AS m FROM orders), b AS (SELECT MIN(o_totalprice) AS n FROM orders) SELECT a.m, b.n FROM a, b;` | NEXT-PHASE-disabled | D8 — "Failed to create child operation" / "Not an aggregate query" | body cs-probe-7 |
| Watermark GREATEST over two scalar CTE outputs (was cs14) | `WITH a AS (SELECT MAX(o_totalprice) AS m FROM orders), b AS (SELECT MIN(o_totalprice) AS n FROM orders) SELECT GREATEST(a.m, b.n) AS biggest FROM a, b;` | NEXT-PHASE-disabled | D8 — scalar comma cross-join "Failed to create child operation" | body cs-probe-8 |
| Watermark LEAST over two scalar CTE outputs (was cs15) | `WITH a AS (SELECT MAX(o_totalprice) AS m FROM orders), b AS (SELECT MIN(o_totalprice) AS n FROM orders) SELECT LEAST(a.m, b.n) AS smallest FROM a, b;` | NEXT-PHASE-disabled | D8 — scalar comma cross-join "Failed to create child operation" | body cs-probe-9 |
| Non-aggregate column in a scalar CTE body (no GROUP BY) | `WITH x AS (SELECT o_totalprice, MAX(o_totalprice) AS m FROM orders) SELECT o_totalprice, m FROM x;` | rejection-assert | Clean prepare-time reject (no CTE pass-through); cf. ronsql_cte_scalar.test Test 5 | body cs-probe-5 |
| Top-level GREATEST over an ordinary table column | `SELECT GREATEST(o_totalprice, 1) AS g FROM orders;` | rejection-assert | Implicit top-level GREATEST/LEAST wrapper valid only for scalar CTE outputs; cf. ronsql_cte_scalar.test Test 14 | body cs-probe-6 |

## Notes

- D8 — scalar-CTE comma cross-join / watermark over two scalar CTEs — is the
  dominant gap for this family. cs08, cs14, cs15 all cross-join two scalar
  CTEs and were demoted to probes (cs-probe-7/8/9). The single-scalar-CTE
  shape (one scalar CTE, single-table main, project or aggregate its output)
  is GREEN and covered by cs09-cs13.
- The chained CTE-of-CTE cases (cs01-06, cs19, cs20) are the highest-confidence
  risk in this rework: they originally errored "CTE aggregate references
  unresolved source column" purely (we believe) because the inner SUM was over
  DECIMAL. With the inner SUM now over the integer `o_shippriority` the error
  should clear, but this has NOT been re-recorded here (no mtr per the
  constraint). If any of cs01-06/cs19/cs20 still errors "unresolved source
  column", demote it to a NEXT-PHASE probe (D7).
- cs01/cs03/cs20 were specifically reshaped so the chained tip is a GROUPED
  intermediate and the main does the final scalar aggregation, satisfying the
  "outer/main SELECT must aggregate" rule without leaving a projection-only
  main over a CTE tip.
- The two rejection-asserts (cs-probe-5, cs-probe-6) mirror the established
  rejections in `suite/ronsql/t/ronsql_cte_scalar.test` (Tests 5 and 14)
  re-targeted at the data-driven suite's `orders` table. They stay ENABLED
  because rejection happens at prepare time, so the join dictionary-cache
  caveat does not apply to ronsql_cli here.
- The empty-intermediate cases (cs19, cs20) rely on `o_totalprice` being
  strictly positive in the fixture (`o_totalprice = orderkey*13.37`), so
  `WHERE o_totalprice < 0` is a guaranteed-empty filter — exercising the
  empty-gb_map fast path on every node without depending on data that drifts.
- No SUM(<decimal>) anywhere: every CTE SUM is over `o_shippriority` (SMALLINT
  UNSIGNED). MIN/MAX over DECIMAL (`o_totalprice`) is retained (cs09-cs13) as
  that is supported.
- No projection-only-over-CTE_LOOKUP/CTE_SCAN main SELECT remains; all 17 MAIN
  mains aggregate (scalar agg or GROUP BY + agg) or project a single scalar
  CTE's aggregate output (cs09-cs13, single-table, no cross-join).
- All table aliases use explicit `AS` (customer AS c); CTE names are referenced
  without an alias (JOIN b ON b.k = ...), per the suite constraint.
