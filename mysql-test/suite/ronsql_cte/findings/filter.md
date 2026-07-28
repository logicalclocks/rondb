# Findings — filter family (data-driven CTE suite)

WHERE-filter coverage in both the CTE body (pre-GROUP BY) and the main query
(on the CTE GROUP BY key + on the CTE aggregate outputs).  Backbone is the
customer/orders join (CTE grouped by `o_custkey`, joined to `customer` on
`c_custkey`), matching `ronsql_cte_basic.test`.  String-literal filters use the
QUERY_FILE form.  Per the corrected envelope every enabled case has an
AGGREGATING main SELECT (scalar agg or GROUP BY + aggregates) over a CTE keyed
by a moderate-cardinality key joined to a SMALL parent; none is projection-only.

Body include: `suite/ronsql_cte/include/body_filter.inc`
Wrapper: `suite/ronsql_cte/t/ronsql_cte_dd_filter.test`

## MAIN (green) cases — 27

| Cases | Coverage |
|-------|----------|
| filter-01..04 | CTE-body WHERE `> >= < <=` on `o_totalprice` (DECIMAL); main aggregates `SUM(o_shippriority)` (int) / `COUNT(*)` |
| filter-05..06 | CTE-body WHERE `= !=` on `o_shippriority` |
| filter-07 | CTE-body WHERE range `>= AND <=` on `o_custkey` (two predicates) |
| filter-08 | CTE-body WHERE AND of two predicates on different columns |
| filter-09 | CTE-body WHERE OR/DNF (`o_shippriority = 0 OR o_shippriority = 4`) |
| filter-10 | main WHERE `MAX(o_clerk) IS NOT NULL` on the CTE aggregate output (Phase I.1 path; o_clerk NULL-semantics) |
| filter-11 | main WHERE `MAX(o_clerk) IS NULL` on the CTE aggregate output (Phase I.1 path; o_clerk NULL-semantics) |
| filter-16 | CTE-body WHERE `o_orderstatus = 'O'` (CHAR(1), QUERY_FILE) |
| filter-17 | CTE-body WHERE `c_mktsegment = 'BUILDING'` (VARCHAR, QUERY_FILE) |
| filter-18 | CTE-body WHERE `l_returnflag = 'R'` (CHAR(1), lineitem keyed by l_orderkey, P-GB main by o_custkey — empty-diff GREEN at record time) |
| filter-19..24 | main WHERE `= != < <= > >=` on the CTE GROUP BY key (`sums.k`); filter-23 crosses the 256-group batch boundary |
| filter-25..30 | main WHERE `= != < <= > >=` on the CTE aggregate output (`cnt.n` / `sums.t`) |
| filter-31 | main WHERE multi-conjunct AND combining a key predicate and an aggregate predicate |

Note: SMALLINT UNSIGNED (`o_shippriority`) is unsigned, so it is NOT used as a
col-vs-col operand; the col-vs-col case (filter-12) uses signed INT columns only
(`o_custkey`, `o_orderkey`).  SUM in MAIN cases is over the **integer**
`o_shippriority` / `l_quantity` (the prior `SUM(o_totalprice)` DECIMAL form was
globally removed — D1; do not reintroduce).

## PROBES (recorded, NOT executed — commented under NEXT-PHASE markers)

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|-------|---------------------|-------------|----------------------------|----------|
| IS NOT NULL in a **CTE body** WHERE (o_clerk) | `WHERE o_clerk IS NOT NULL` inside the CTE materialization | ENABLED (D10 FIXED) | `apply_filter` now has a `case T_IS:` arm (`apply_filter_isnull`) lowering to `NdbScanFilter::isnull`/`isnotnull`. filter-10a | body_filter.inc Group 3 |
| IS NULL in a **CTE body** WHERE (o_clerk) | `WHERE o_clerk IS NULL` inside the CTE materialization | ENABLED (D10 FIXED) | Same `T_IS` arm — this was the originally-confirmed "Non-boolean term in WHERE condition" case. filter-11a | body_filter.inc Group 3 |
| GREATEST in a CTE-body WHERE | `WHERE GREATEST(o_shippriority, 1) > 2` | ENABLED (D11 FIXED) | `cond_expr` grammar rules + `simplify_ce` boolean rewrite (GREATEST `>` → OR + `IS NOT NULL` guards; const arms folded). filter-14 (OR) + filter-14b (AND, n-ary) | body_filter.inc Group 5 |
| LEAST(col, col) in a CTE-body WHERE | `WHERE LEAST(l_quantity, l_partkey) < 10` | ENABLED (D11 FIXED) | Same rewrite (LEAST `<` → OR; LEAST `>` → AND). filter-15 (OR) + filter-15b (AND, n-ary) | body_filter.inc Group 5 |
| CTE-body signed-int col-vs-col on orders feeding aggregating main | `WHERE o_custkey < o_orderkey` (orders keyed by o_custkey → customer P-GB) | NEXT-PHASE-disabled | ERROR — RonSQLRetryableError after 10 attempts ("Integer type column..."). Together with the filter-13 HANG, col-vs-col in a CTE body is broadly broken. Was filter-12; D12 | body_filter.inc Group 4 (NEXT-PHASE) |
| CTE-body signed-int col-vs-col over lineitem feeding P-GB | `WHERE l_quantity < l_partkey` (lineitem keyed by l_orderkey, P-GB main by o_custkey) | NEXT-PHASE-disabled | HANG — col-vs-col exec on an orderkey-keyed lineitem CTE under the orders root scan. Was filter-13; D4 | body_filter.inc Group 4 (NEXT-PHASE) |
| BETWEEN in CTE-body WHERE | `WHERE o_totalprice BETWEEN 5000 AND 6000` | NEXT-PHASE-disabled | BETWEEN listed UNCERTAIN in matrix; not yet emitted | body_filter.inc PROBES block |
| IN-list in CTE-body WHERE | `WHERE o_shippriority IN (0,2,4)` | NEXT-PHASE-disabled | IN(...) listed UNCERTAIN in matrix | body_filter.inc PROBES block |
| LIKE on VARCHAR in CTE-body WHERE | `WHERE c_mktsegment LIKE 'A%'` | NEXT-PHASE-disabled | LIKE listed UNCERTAIN in matrix | body_filter.inc PROBES block |
| String column-vs-column in CTE-body WHERE | `WHERE l_returnflag < l_returnflag` (two CHAR cols) | NEXT-PHASE-disabled | only signed-int col-vs-col supported (matrix) | body_filter.inc PROBES block |
| OR in the MAIN-query CTE filter | `WHERE sums.k = 100 OR sums.t > 50000` | NEXT-PHASE-disabled | `emit_cte_lookup_filter` accepts top-level AND conjuncts; top-level OR across key + agg-output predicates not yet exercised from SQL | body_filter.inc PROBES block |
| CASE over a CTE AGGREGATE output | `WHERE CASE WHEN sums.t > 50000 THEN 1 ELSE 0 END = 1` | NEXT-PHASE-disabled | matrix: CASE over a CTE COLUMN projection is supported, over an aggregate output is NOT | body_filter.inc PROBES block |

### Notes

- **The "Non-boolean term in WHERE condition" case was filter-11** (`o_clerk IS NULL`
  in the CTE body).  Root cause: a single-table CTE body emits its pre-GROUP-BY
  WHERE through `NdbScanFilter::apply_filter`, whose `switch (ce->op)` has no
  `case T_IS:`, so `IS NULL` / `IS NOT NULL` (AST op `T_IS`) hit the `default:`
  arm and throw.  Both o_clerk-IS-NULL CTE-body cases (former filter-10 / filter-11)
  share this gap and are disabled as NEXT-PHASE probes.  o_clerk NULL-semantics
  coverage is preserved GREEN by the **new** filter-10 / filter-11, which move the
  predicate to the supported position: the CTE aggregates `MAX(o_clerk)` and the
  MAIN query filters `WHERE clk.mc IS NULL` / `IS NOT NULL` on that aggregate
  output (the Phase I.1 CTE_LOOKUP filter path, same surface the joins family
  anti-join cases J8/J9 exercise).
- All probes are GENUINE-GAP / UNCERTAIN shapes, so they use form (2): commented
  out under a `# NEXT-PHASE:` marker and never executed.  None is a clean
  permanent rejection (no partial-key / outer-join-child cases are in scope for
  the filter family), so there are no `rejection-assert` (form-1) probes here.
- The CTE-body OR/DNF case (filter-09) is GREEN per the matrix
  ("CTE-body WHERE: ... OR/DNF"); the OR PROBE is specifically the *main-query*
  CTE-lookup filter, which is the uncertain surface.
- filter-18 uses a lineitem CTE grouped by `l_orderkey`, joined to `orders` on
  `o_orderkey`, with a P-GB main rolling up by `o_custkey` — this lineitem-keyed
  shape is GREEN under a P-GB (GROUP BY) main (empty diff at record time).  The
  D6 crash and the D4 hang are specific to, respectively, a SCALAR main agg and a
  col-vs-col CTE-body WHERE over the same lineitem-keyed CTE; the plain
  string-equality + P-GB combination is fine.
