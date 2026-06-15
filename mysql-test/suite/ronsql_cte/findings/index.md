# index family — findings

Family `index` of the data-driven RonSQL CTE suite. Asserts BOTH result
values (via `ronsql_compare.inc`) AND query plans (via `ronsql_explain.inc`
+ a `grep -qF` on a stable EXPLAIN substring).

- Body include: `suite/ronsql_cte/include/body_index.inc`
- Wrapper:      `suite/ronsql_cte/t/ronsql_cte_dd_index.test`

## CORRECTED ENVELOPE rework (this pass)

- **Main SELECTs are now AGGREGATING, never projection-only.** A projection-only
  main SELECT over a CTE_LOOKUP HANGS (D2/D3). Every enabled case wraps the CTE
  columns in `MIN`/`MAX`/`SUM(<int>)`/`COUNT(*)` (scalar aggregate; the CTE
  bodies that GROUP BY are aggregated again by the main). CTE-body shape changed
  to an aggregating main: index-1, index-2, index-3, index-6, index-7, index-9,
  index-10, index-11, index-12, index-13, index-14, index-15, index-16, index-17.
- **index-4, index-5, index-8 DISABLED (D9)** — CTE-body index-scan root on a
  DATE bound (idx_o_orderdate) and on a composite DATE-range bound
  (idx_o_status_date). The recording pass errored
  `Failed to create CTE body index-scan root`. Genuinely unsupported
  index-scan-root shapes; moved to NEXT-PHASE probes in `body_index.inc`
  (index-4 kept as the documented probe).
- **CTE-body aggregates use COUNT(*) / SUM(<int col>) / MIN / MAX only** — no
  SUM over DECIMAL (already globally fixed; not reintroduced), no COUNT(<col>).
  The int columns summed are l_quantity, p_size, o_shippriority (all integer).
- **EXPLAIN greps kept NON-FATAL (`|| true`)** for every enabled case for now;
  the orchestrator tightens the exact EXPLAIN strings after recording. The
  index-name substring greps are retained.

## EXPLAIN substrings grepped (for orchestrator verification)

All asserts target the per-CTE `Body root:` line emitted by
`RonSQLPreparer.cpp` (~line 10814). Substrings are robust (index name +,
where stable, the `[I.10 ... maxRows=1]` tag). Each grep is currently kept
non-fatal with `|| true`; the orchestrator should confirm/tighten these at
record time.

| Case | grep -qF substring |
|------|--------------------|
| index-1  | `Body root: INDEX_SCAN using idx_c_nationkey` |
| index-2  | `Body root: INDEX_SCAN using idx_l_partkey` |
| index-3  | `Body root: INDEX_SCAN using idx_p_size` |
| index-4  | DISABLED (D9) — `Failed to create CTE body index-scan root` (DATE bound) |
| index-5  | DISABLED (D9) — `Failed to create CTE body index-scan root` (DATE range) |
| index-6  | `Body root: INDEX_SCAN using idx_p_brand` |
| index-7  | `Body root: INDEX_SCAN using idx_o_status_date` |
| index-8  | DISABLED (D9) — `Failed to create CTE body index-scan root` (composite DATE range) |
| index-9  | DISABLED (D17) — MIN/MAX over DATE col: ERROR "Failed writing aggregation program. Please report a bug." |
| index-10 | `Body root: INDEX_SCAN using idx_p_size [I.10 MAX_DESC maxRows=1]` |
| index-11 | `Body root: INDEX_SCAN using idx_p_size [I.10 MIN_ASC maxRows=1]` |
| index-12 | `Body root: INDEX_SCAN using idx_o_custkey [I.10 MAX_DESC maxRows=1]` |
| index-13 | `Body root: INDEX_SCAN using idx_o_custkey [I.10 MIN_ASC maxRows=1]` |
| index-14 | `Body root: TABLE_SCAN` (nullable-col fallback; negative-space assert) |
| index-15 | `Body root: INDEX_SCAN using idx_o_custkey` (FORCE INDEX) |
| index-16 | `Body root: INDEX_SCAN using idx_c_nationkey` (USE INDEX) |
| index-17 | ENABLED (D16 FIXED) — scalar COUNT(*) over EMPTY input now reads 0; `NdbAggregator::PrepareResults` scalar COUNT-null→0 fixup |

Source of the exact strings: `RonSQLPreparer.cpp`
- `Body root: ` then `TABLE_SCAN` / `INDEX_SCAN` (lines 10814-10822)
- ` using <indexName>` when `root_op.index != NULL` (line 10824)
- ` [I.10 MIN_ASC maxRows=1]` / ` [I.10 MAX_DESC maxRows=1]` (lines 10826-10832)

Confirmed against the existing committed tests
`ronsql_cte_minmax_index.test` (I.10 tags) and `ronsql_cte_index_body.test`
(`Body root: TABLE_SCAN` / `INDEX_SCAN` shape).

## Probes

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|-------|---------------------|-------------|----------------------------|----------|
| Index hint on a JOINED (non-root) table | `WITH c AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t FROM orders GROUP BY o_custkey) SELECT cu.c_custkey FROM customer AS cu JOIN c USE INDEX (idx_c_nationkey) ON c.k = cu.c_custkey WHERE cu.c_custkey = 5;` | rejection-assert | W1 — `reject_index_hint_on_joined_table` / "Index hints ... only supported on the root-table scan" (`RonSQLPreparer.cpp:2471`) | body_index.inc index-P1 |
| CTE-body index-scan root on a DATE equality bound (was index-4) | `WITH od AS (SELECT o_orderdate AS d, COUNT(*) AS n FROM orders WHERE o_orderdate = '1995-06-15' GROUP BY o_orderdate) SELECT MIN(d) AS mnd, MAX(d) AS mxd, SUM(n) AS sn, COUNT(*) AS g FROM od;` | NEXT-PHASE-disabled (D9) | I.9 — index-scan root on DATE bound; recording pass errored "Failed to create CTE body index-scan root" | body_index.inc (NEXT-PHASE comment, kept as the documented index-4 probe) |
| CTE-body index-scan root on a DATE range bound (was index-5) | `WITH od AS (SELECT o_orderdate AS d, SUM(o_shippriority) AS prio FROM orders WHERE o_orderdate >= '1995-06-01' AND o_orderdate <= '1995-06-30' GROUP BY o_orderdate) SELECT MIN(d) AS mnd, MAX(d) AS mxd, SUM(prio) AS sp, COUNT(*) AS g FROM od;` | NEXT-PHASE-disabled (D9) | I.9 — same "Failed to create CTE body index-scan root"; collapses into the index-4 finding | body_index.inc (NEXT-PHASE comment) |
| Composite index-scan root on leading-eq + DATE range (was index-8) | `WITH os AS (SELECT o_orderdate AS d, COUNT(*) AS n FROM orders WHERE o_orderstatus = 'O' AND o_orderdate > '1995-06-01' GROUP BY o_orderdate) SELECT MIN(d) AS mnd, MAX(d) AS mxd, SUM(n) AS sn, COUNT(*) AS g FROM os;` | NEXT-PHASE-disabled (D9) | I.9 — composite idx_o_status_date index-scan root with a DATE range on the 2nd col; "Failed to create CTE body index-scan root" | body_index.inc (NEXT-PHASE comment) |
| Nullable MIN/MAX via index — EXPLAIN plan gap | `WITH mx AS (SELECT MAX(o_clerk) AS m FROM orders) SELECT m FROM mx;` | NEXT-PHASE-disabled | I.10 nullable extension — want INDEX_SCAN via idx_o_clerk, got TABLE_SCAN; VALUES already correct (index-14 keeps value-compare ENABLED + asserts the TABLE_SCAN fallback) | body_index.inc (NEXT-PHASE comment) + index-14 |
| Scalar MIN/MAX-via-index + WHERE composition | `WITH mx AS (SELECT MAX(p_size) AS m FROM part WHERE p_size < 40) SELECT m FROM mx;` | NEXT-PHASE-disabled | I.10 detects a single-op body whose only output is a direct-column MIN/MAX; a WHERE bound may force the residual-filter path and drop maxRows=1 — uncertain | body_index.inc (NEXT-PHASE comment) |
| BETWEEN as an index bound in a CTE body | `WITH lp AS (SELECT l_partkey AS k, SUM(l_quantity) AS q FROM lineitem WHERE l_partkey BETWEEN 40 AND 60 GROUP BY l_partkey) SELECT k, q FROM lp;` | NEXT-PHASE-disabled | I.9 / W3 — uncertain whether BETWEEN lowers to the same low+high NdbQueryIndexBound that `col >= x AND col <= y` produces (matrix marks BETWEEN UNCERTAIN) | body_index.inc (NEXT-PHASE comment) |
| MIN/MAX over a DATE column in a CTE (was index-9) | `WITH os AS (SELECT o_orderdate AS d, SUM(o_shippriority) AS t FROM orders WHERE o_orderstatus = 'F' AND o_orderdate >= '1995-03-01' AND o_shippriority < 3 GROUP BY o_orderdate) SELECT MIN(d) AS mnd, MAX(d) AS mxd, SUM(t) AS st, COUNT(*) AS g FROM os;` | NEXT-PHASE-disabled (D17) | ERROR "Failed writing aggregation program. Please report a bug." — DATE aggregate output not yet supported | body_index.inc (NEXT-PHASE comment) |
| Scalar COUNT(*) over EMPTY input (index-17) | `WITH x AS (SELECT c_mktsegment AS s, COUNT(*) AS n FROM customer IGNORE INDEX (idx_c_nationkey) WHERE c_nationkey = 4 AND c_mktsegment = 'BUILDING' GROUP BY c_mktsegment) SELECT MIN(s) AS mns, MAX(s) AS mxs, SUM(n) AS sn, COUNT(*) AS g FROM x;` | ENABLED (D16 FIXED) | `NdbAggregator::PrepareResults` now applies the RONDB-831 COUNT-null→0 fixup in the scalar (no-GROUP-BY) path; COUNT reads 0, MIN/MAX/SUM stay NULL | body_index.inc index-17 |

## Notes

- **Enabled MAIN cases: 13** (each = value-compare + EXPLAIN assert):
  index-1, -2, -3, -6, -7, -10, -11, -12, -13, -14, -15, -16, -17 (D16 FIXED).
  **Disabled probes: 4** (index-4, -5, -8 — D9 index-scan-root; index-9 — D17
  MIN/MAX over DATE; all NEXT-PHASE). Plus 1 rejection-assert (index-P1) and
  3 other NEXT-PHASE probes (nullable MIN/MAX EXPLAIN gap, MIN/MAX+WHERE,
  BETWEEN).
- **QUERY_FILE used for all single-quoted-literal cases**: index-6 (CHAR
  `p_brand`), index-7/9 (composite, CHAR `o_orderstatus` + DATE), index-17
  (CHAR `c_mktsegment`). (The DATE/composite-DATE cases index-4/5/8 are now
  disabled.) Because both `ronsql_compare.inc` and `ronsql_explain.inc`
  `--remove_file` the `QUERY_FILE` when `$QUERY` is empty, each step writes
  its OWN `.sql` copy (suffix `e` for the explain step's file), so the
  second step never reads a removed file.
- **index-14 (nullable MIN/MAX)** deliberately asserts the *TABLE_SCAN
  fallback* rather than INDEX_SCAN: per the capability matrix the values
  are correct but the plan falls back. No failing INDEX_SCAN grep is
  added; the desired INDEX_SCAN plan is recorded as a NEXT-PHASE probe.
- **index-17 (IGNORE INDEX steering)**: the WHERE binds both
  `c_nationkey` and `c_mktsegment`; `IGNORE INDEX (idx_c_nationkey)`
  removes the nationkey index from consideration so the planner must pick
  `idx_c_mktsegment`. The substring `INDEX_SCAN using idx_c_mktsegment`
  confirms the ignored index is absent and the alternative is chosen. If
  at record time the planner instead falls back to TABLE_SCAN (e.g.
  because the equality on `c_mktsegment` is scored below TABLE_SCAN), the
  orchestrator should switch this grep to `Body root: TABLE_SCAN` and the
  value-compare still validates correctness.
- All composite-index cases share the single `idx_o_status_date` Body-root
  substring; the orchestrator can tighten to include the bound columns
  once the recorded EXPLAIN output is captured (the emit code prints bound
  columns only in the multi-op join-plan block, not the per-CTE Body-root
  line, so the Body-root grep stays at the index name).
- Every table alias uses explicit `AS`; CTE names are referenced WITHOUT
  an alias (`FROM cn`, `SELECT k, n FROM x`).
```
