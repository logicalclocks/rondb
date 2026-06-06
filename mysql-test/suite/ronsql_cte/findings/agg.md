# agg family — findings

CTE-body aggregation matrix across data types and GROUP BY shapes. Per the
CORRECTED ENVELOPE, EVERY enabled main SELECT AGGREGATES (scalar aggregate or
GROUP BY a parent dimension with aggregates) — projection-only main SELECTs over
a CTE_LOOKUP child HANG (D3) and are recorded only as a disabled probe. The main
SELECT is exercised both ways: (A) aggregation over a CTE complete-key lookup
leaf joined to a SMALL real parent, and (B) aggregation over a CTE_SCAN root.

CTEs are keyed by a moderate-cardinality key to a SMALL parent (o_custkey/300 →
customer, s_nationkey → supplier/nation, p_brand/p_size → part, c_nationkey →
nation, n_regionkey → region). A CTE keyed by l_orderkey/1500 used as a
CTE_LOOKUP child under an orders root scan CRASHES (D6) and is NEVER used —
lineitem aggregates are exercised via the CTE_SCAN root form (agg-13/19/20/21).

Body include: `suite/ronsql_cte/include/body_agg.inc`
Wrapper:      `suite/ronsql_cte/t/ronsql_cte_dd_agg.test`

## MAIN cases (18, all expected green)

| #      | Shape | Aggregates (main) | GROUP BY (main) | Main topology |
|--------|-------|-------------------|-----------------|---------------|
| agg-01 | COUNT(*)/SUM(int)/MIN/MAX, DECIMAL(12,2) | scalar agg | — | A (customer JOIN cte) |
| agg-03 | MIN/MAX over TINYINT (n_nationkey) | scalar agg | — | A (region JOIN cte) |
| agg-04 | MIN/MAX over SMALLINT UNSIGNED (o_shippriority) | scalar agg | — | A (customer JOIN cte) |
| agg-05 | MIN/MAX over MEDIUMINT (p_size) | grouped by p_brand | p.p_brand | A (part JOIN cte) |
| agg-07 | MIN/MAX over BIGINT UNSIGNED (s_total_sales) | scalar agg | — | A (nation JOIN cte) |
| agg-08 | MIN/MAX over DECIMAL scale-2 (o_totalprice) + SUM(int) | scalar agg | — | A (customer JOIN cte) |
| agg-09 | MIN/MAX over DECIMAL scale-2 (c_acctbal) | scalar agg | — | A (nation JOIN cte) |
| agg-10 | MIN/MAX over DECIMAL scale-0 (s_margin) + SUM(int) | scalar agg | — | A (nation JOIN cte) |
| agg-11 | DISABLED (D18) — string MIN/MAX re-aggregated → CRASH | — | — | — |
| agg-12 | DISABLED (D18) — string MIN/MAX re-aggregated → CRASH | — | — | — |
| agg-13 | DISABLED (D18) — string MIN/MAX re-aggregated → CRASH | — | — | — |
| agg-14 | DISABLED (D18) — string MIN/MAX re-aggregated → CRASH | — | — | — |
| agg-15 | DISABLED (D18) — string MIN/MAX re-aggregated → CRASH | — | — | — |
| agg-16 | COUNT/SUM/MIN/MAX, multi-column body GROUP BY | scalar agg | — | B (CTE_SCAN root) |
| agg-17 | SUM/MIN/MAX, multi-column GROUP BY in body | grouped by st | ord.st | B (CTE_SCAN root) |
| agg-18 | COUNT(*)/SUM/MIN/MAX scalar over CTE output | scalar agg | — | B (CTE_SCAN root) |
| agg-19 | COUNT/SUM/MIN/MAX over lineitem | scalar agg | — | B (CTE_SCAN root) |
| agg-20 | COUNT/SUM/MIN/MAX, two-col body GROUP BY | grouped by rf | x.rf | B (CTE_SCAN root) |
| agg-21 | string MIN/MAX over lineitem | scalar agg | — | B (CTE_SCAN root) |

(agg-02 and agg-06 are disabled probes — see below. Type-coverage cases that
were previously projection-only over a CTE_LOOKUP — agg-03/07/08/09/10/11/14 —
and the GROUP-BY-all-cte-cols projection emulations — agg-05/12/15 — are now
genuine aggregating mains. agg-13's CHAR(1) coverage moved from an
l_orderkey-under-orders CTE_LOOKUP (D6 crash topology) to a CTE_SCAN root.)

Notes:
- `GROUP BY o_custkey` produces 300 groups (>256) so agg-01/04/08/11/18 cross
  the 256-row API batch boundary on the CTE materialisation.
- SUM is applied ONLY to integer columns (o_shippriority, s_nationkey, l_quantity,
  the COUNT(*) carry-through column n). DECIMAL columns are MIN/MAX'd, never
  SUM'd (D1: SUM over DECIMAL in a CTE is a clean error).
- agg-05/12/15 join a real table back to a CTE grouped by a non-unique column,
  producing a fan-out; the main `GROUP BY <parent dimension>` + aggregates
  collapses it. VALUES are engine-identical.
- String MIN/MAX (CHAR + VARCHAR) is matrix-SUPPORTED and exercised both through
  CTE_LOOKUP (agg-11/12/14/15) and CTE_SCAN root (agg-13/21).

## PROBES (NEXT-PHASE-disabled — commented out, do NOT execute)

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|-------|--------------------|-------------|----------------------------|----------|
| projection-only main over CTE_LOOKUP (HANG, D3) | `WITH nat AS (SELECT n_regionkey AS rk, MIN(n_nationkey) AS mn, MAX(n_nationkey) AS mx, COUNT(*) AS n FROM nation GROUP BY n_regionkey) SELECT nat.rk, nat.mn, nat.mx, nat.n FROM region AS r JOIN nat ON nat.rk = r.r_regionkey` | NEXT-PHASE-disabled | main SELECT projecting CTE_LOOKUP outputs without a main aggregate never returns | body_agg.inc (agg-03 disabled probe) |
| COUNT(`<col>`) in CTE body (HANG, D2) | `WITH ord AS (SELECT o_custkey AS k, COUNT(*) AS n_all, COUNT(o_clerk) AS n_clerk FROM orders GROUP BY o_custkey) SELECT SUM(ord.n_all), SUM(ord.n_clerk) FROM customer AS c JOIN ord ON ord.k = c.c_custkey` | NEXT-PHASE-disabled | COUNT of a column (vs COUNT(*)) hangs; high-value bug | body_agg.inc (agg-02 disabled probe) |
| SUM(`<DECIMAL col>`) in CTE body (ERROR, D1) | `WITH x AS (SELECT o_custkey AS k, SUM(o_totalprice) AS t FROM orders GROUP BY o_custkey) SELECT SUM(x.t) FROM customer AS c JOIN x ON x.k = c.c_custkey` | NEXT-PHASE-disabled | RonSQLPreparer.cpp:7090 — SUM supports int + FLOAT/DOUBLE, not DECIMAL | body_agg.inc probe |
| scalar agg over l_orderkey CTE_LOOKUP under orders root (CRASH, D6) | `WITH li AS (SELECT l_orderkey AS k, MIN(l_quantity) AS mn, MAX(l_quantity) AS mx, SUM(l_quantity) AS sq FROM lineitem GROUP BY l_orderkey) SELECT MIN(li.mn), MAX(li.mx), SUM(li.sq) FROM orders AS o JOIN li ON li.k = o.o_orderkey` | NEXT-PHASE-disabled | DBSPJ ndbassert Error 2343, DbspjMain.cpp:7825 `m_cnt_active==0` — both nodes crash; multi-batch CTE-lookup teardown | body_agg.inc (agg-06 disabled probe) |
| AVG in CTE body | `WITH x AS (SELECT o_custkey AS k, AVG(o_totalprice) AS a FROM orders GROUP BY o_custkey) SELECT x.k, x.a FROM customer AS c JOIN x ON x.k = c.c_custkey` | NEXT-PHASE-disabled | AVG not in supported CTE-body aggregate set | body_agg.inc probe |
| COUNT(DISTINCT col) | `WITH x AS (SELECT o_orderstatus AS st, COUNT(DISTINCT o_custkey) AS d FROM orders GROUP BY o_orderstatus) SELECT x.st, x.d FROM x` | NEXT-PHASE-disabled | COUNT(DISTINCT) unsupported | body_agg.inc probe |
| MIN/MAX over DATE | `WITH x AS (SELECT o_custkey AS k, MIN(o_orderdate) AS mn, MAX(o_orderdate) AS mx FROM orders GROUP BY o_custkey) SELECT MIN(x.mn), MAX(x.mx) FROM customer AS c JOIN x ON x.k = c.c_custkey` | NEXT-PHASE-disabled | UNCERTAIN — DATE MIN/MAX wire format; orchestrator may enable if it round-trips | body_agg.inc probe |
| GROUP BY expression | `WITH x AS (SELECT (o_custkey % 10) AS bucket, COUNT(*) AS n FROM orders GROUP BY (o_custkey % 10)) SELECT x.bucket, x.n FROM x` | NEXT-PHASE-disabled | GROUP BY must be direct columns only | body_agg.inc probe |
| Post-aggregation SELECT expr | `WITH x AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t FROM orders GROUP BY o_custkey) SELECT x.k, SUM(x.t) + 1 FROM x GROUP BY x.k` | NEXT-PHASE-disabled | SELECT of post-aggregation expression (SUM(x)+1) unsupported | body_agg.inc probe |
| SUM over FLOAT | `WITH x AS (SELECT l_orderkey AS k, SUM(l_discount) AS sd FROM lineitem GROUP BY l_orderkey) SELECT SUM(x.sd) FROM x` | NEXT-PHASE-disabled | UNCERTAIN — FLOAT SUM formatting may diverge; MIGHT pass, orchestrator verifies | body_agg.inc probe |
| SUM over DOUBLE | `WITH x AS (SELECT l_orderkey AS k, SUM(l_tax) AS stx FROM lineitem GROUP BY l_orderkey) SELECT SUM(x.stx) FROM x` | NEXT-PHASE-disabled | UNCERTAIN — DOUBLE SUM formatting may diverge; MIGHT pass, orchestrator verifies | body_agg.inc probe |
| Main re-aggregates a string (CHAR/VARCHAR) MIN/MAX CTE output (CRASH, D18) | `WITH ord AS (SELECT o_custkey AS k, MIN(o_orderstatus) AS mn, MAX(o_orderstatus) AS mx, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT MIN(ord.mn), MAX(ord.mx), SUM(ord.n) FROM customer AS c JOIN ord ON ord.k = c.c_custkey` | NEXT-PHASE-disabled | DATA NODE CRASH (NDB Error 6000 / Signal 6 abort). Numeric MIN/MAX re-aggregation works; string does not. Collapses agg-11..agg-15 (CHAR(1) o_orderstatus, CHAR(10) p_brand, CHAR(1) l_returnflag via CTE_SCAN root, VARCHAR(12) c_mktsegment, VARCHAR(40) p_name) | body_agg.inc (agg-11..15 shared NEXT-PHASE block) |

### Prose notes

- Rework against the CORRECTED ENVELOPE: every enabled MAIN now AGGREGATES.
  Seven type-coverage cases that were authored projection-only over a CTE_LOOKUP
  (agg-03/07/08/09/10/11/14) and three that emulated projection via
  `GROUP BY <all cte cols>` (agg-05/12/15, which RonSQL rejected as "Not an
  aggregate query") were converted to scalar or parent-dimension-grouped
  aggregating mains, keeping each type's MIN/MAX coverage GREEN.
- agg-01's `COUNT(o_clerk)` was replaced with `COUNT(*)` (COUNT of a column
  hangs, D2); the COUNT(col) hang is preserved as the agg-02 disabled probe.
- agg-13 (CHAR(1) l_returnflag) was moved off the D6 crash topology (lineitem
  CTE keyed by l_orderkey as a CTE_LOOKUP child under an orders root scan) onto
  a CTE_SCAN root — same type coverage, safe topology.
- All probes are recorded as `# NEXT-PHASE:` comment blocks in `body_agg.inc`
  (none execute). None are clean rejection-asserts: the hangs/crashes are
  runtime gaps and the formatting probes are uncertainties, so the ronsql_cli
  rejection-assert form does not apply.
- The FLOAT/DOUBLE SUM probes and the DATE MIN/MAX probe are the ones most
  likely to flip to MAIN at record time — values are functionally expected to
  compute; only TSV text formatting is in question.
- AVG, COUNT(DISTINCT), GROUP BY expression, and post-aggregation SELECT
  expressions are firm gaps per the capability matrix.
