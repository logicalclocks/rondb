# orderby_limit family — findings

Phase 1 of `ronsql_orderby_limit_plan.md` (verify + lock in main-SELECT
ORDER BY / LIMIT on aggregate join/CTE queries).  Test-only phase — the
sort+limit machinery is shared with the tested single-table path
(`ronsql_orderby.test`); ob-1..ob-22 give it its first join/CTE coverage
with the row ORDER actually verified (`$skip_sort=yes` strict diff with
total-order tie-breakers), unlike bigquery big-12 which sorts both
outputs and only checks the surviving values.

**Found + FIXED on first record — mixed-spelling ORDER BY vs GROUP BY
wrongly rejected.**  ob-1 (`... GROUP BY cu.c_nationkey ORDER BY
c_nationkey`) failed its first record with "Syntax error: ORDER BY
refers to column `c_nationkey` which is not in the GROUP BY clause"
while MySQL returned 25 rows.  Root cause: the parser's column registry
keys on the (qualifier, name) pair (`column_name_to_idx` vs
`qualified_column_name_to_idx`, RonSQLPreparer.cpp), so bare
`c_nationkey` and qualified `cu.c_nationkey` carry different col_idx
values, and `ResultPrinter::validate_orderby_columns` matches GROUP BY
membership by raw col_idx equality.  bigquery big-12 never hit this
because its unaliased `SELECT c.c_custkey` output made `ORDER BY
c_custkey` resolve as an OUTPUT_REF alias carrying the qualified
col_idx.  Fix: `RonSQLPreparer::canonicalize_orderby_columns()` —
called from `compile()` after scoped resolution, rewrites each
TABLE_COLUMN ORDER BY col_idx to the GROUP BY entry's col_idx when both
`ResolvedColumnRef`s denote the same underlying column (same kind +
join_op_idx + attr_id for StoredColumn, same cte_def_idx/cte_result_idx
for CteResultColumn).  Ambiguous unqualified names are already rejected
by I.23 scoped resolution, so a resolved spelling is unique.  ob-1 pins
bare-ORDER-BY vs qualified-GROUP-BY; ob-2 pins the reverse (bare
SELECT/GROUP BY + qualified ORDER BY).  The adjacent sharp edge —
SELECT output spelling differing from the GROUP BY spelling (e.g.
`SELECT c_nationkey ... GROUP BY cu.c_nationkey`) — is ORDER
BY-independent and left untouched.

**Found + FIXED on first record (2) — LIMIT 0 printed a bare TSV header
line.**  ob-5 (`... ORDER BY c_nationkey LIMIT 0`) diffed against
MySQL's empty output with a single `nk\tc\tsn` header line.  This was a
LONG-STANDING deviation, not a join-path regression: the mysql client
prints nothing at all for an empty result set, and both base-suite
LIMIT 0 baselines (`ronsql_basic.result`, `ronsql_orderby_stress.result`)
had the exact mismatch BAKED IN as a recorded `@@ -0,0 +1 @@` diff under
the non-strict `|| true` compare — the strict family is what finally
failed on it.  Two-line fix in ResultPrinter: the buffered path
(`print_result_ordered`) gates the TSV header on `print_count > 0`
(matching its existing `num_rows == 0` early return), and the streaming
path checks the LIMIT cutoff BEFORE the deferred header block (ob-20's
scalar `LIMIT 0` exercises that arm).  JSON output was already
consistent (`[]` both ways).  Re-recording `ronsql_basic` and
`ronsql_orderby_stress` is required after the fix since their baselines
memorialize the old behavior.

Prose notes:

- **The old body_mainmode.inc deferral markers were wrong on the why.**
  They recorded "ORDER BY / LIMIT / HAVING on CTE queries not yet
  supported" with `ORDER BY SUM(cust.t) DESC` probes — but that form is
  a grammar-level syntax error on EVERY query shape (ORDER BY takes only
  identifiers/aliases); the aggregate-alias form was never tried.  The
  markers were rewritten to point at this family; DISTINCT and
  projection-only ORDER BY / LIMIT remain genuinely deferred.
- The streaming (no-ORDER-BY) LIMIT cutoff is compare-tested via a
  value-uniform trick (ob-7: no group column in the SELECT list, so the
  nondeterministic choice of surviving groups prints identical rows).
- CTE-body / subquery ORDER BY + LIMIT rejection (Phase 0) is pinned
  separately by `ronsql_cte_dd_orderby_limit_reject.test` (base suite
  only — prepare-time rejection is topology-independent).

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| ORDER BY column not in GROUP BY | `WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT cu.c_nationkey AS nk, SUM(cf.n) AS sn FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey GROUP BY cu.c_nationkey ORDER BY c_mktsegment;` | rejection-assert (ob-P1) | Correct SQL semantics for aggregate queries (ResultPrinter::validate_orderby_columns) | body_orderby_limit.inc |
| ORDER BY aggregate-expression form | `... GROUP BY cu.c_nationkey ORDER BY SUM(cf.n) DESC;` | rejection-assert (ob-P2) | Grammar accepts identifiers only in ORDER BY; order aggregates via the SELECT output alias.  Expression/positional ORDER BY is plan Phase 6 | body_orderby_limit.inc |
| LIMIT offset,count | `... ORDER BY c_nationkey LIMIT 2,3;` | rejection-assert (ob-P3) | OFFSET support is plan Phase 6 | body_orderby_limit.inc |
| GROUP BY `<cte>.col` on the JOIN path | `WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT cf.k AS k, SUM(cf.n) AS sn, COUNT(*) AS c FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey WHERE cu.c_custkey <= 30 GROUP BY cf.k ORDER BY cf.k DESC LIMIT 10;` | NEXT-PHASE-disabled | Main-query GROUP BY over a linked CTE attribute — no green precedent (agg-17's re-agg by a CTE column is the CTE_SCAN-root shape, ob-21); equivalent queries group by the equi-joined parent column (ob-22) | body_orderby_limit.inc |
