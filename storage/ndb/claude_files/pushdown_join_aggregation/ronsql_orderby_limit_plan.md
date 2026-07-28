# RonSQL ORDER BY / LIMIT Support Plan

**Status: PLAN — not yet implemented.**

## Trigger

ORDER BY / LIMIT appears to be missing from RonSQL, yet is supported "in some
parts". Both halves of that impression are correct — the support is real but
uneven across query shapes, one shape silently ignores it (a correctness bug),
and the rondb-cli benchmark registry conservatively marks every ORDER BY /
LIMIT query `MySQLOnly` (`fs_topk`, `fs_history`, all `tpch_q*_official`).

## Current state (verified against the code, July 2026)

### Fully implemented — aggregate queries, main SELECT level

- **Grammar** (`RonSQLParser.y:791-812`): `ORDER BY col [ASC|DESC]`,
  multi-column, qualified names (`t.col`), plus `LIMIT N` (single
  non-negative integer; no `OFFSET` / `LIMIT off,cnt` syntax).
- **Alias resolution** (`RonSQLPreparer.cpp:902`
  `resolve_orderby_aliases`): unqualified ORDER BY names matching a SELECT
  output alias become `OUTPUT_REF`, so `ORDER BY revenue DESC` sorts by the
  aggregate. Pure aliases get an `AliasOnly` sentinel in resolved columns
  (`:1146`) so they don't fail column lookup.
- **ResultPrinter** (main constructor, used for every
  `m_is_aggregate_query` query — `RonSQLPreparer.cpp:5170`):
  - `validate_orderby_columns` (`ResultPrinter.cpp:192`) requires each
    ORDER BY target to be a GROUP BY column or an aggregate output
    (alias). Anything else → clean permanent error. Correct SQL semantics
    for aggregate queries.
  - `print_result_ordered` (`ResultPrinter.cpp:1003`) buffers all groups,
    sorts with charset-aware `NdbSqlUtil` comparators (string MIN/MAX
    results re-wrap the length prefix, `:928-969`), NULLs sort first in
    ASC (MySQL semantics), applies LIMIT post-sort (`:1053`).
  - Streaming (no-ORDER-BY) path applies LIMIT as a print cutoff
    (`:1344`, `:1386`).
  - HAVING or the cross-table sentinel slot also force the buffered path
    (`:1327`), so HAVING + ORDER BY + LIMIT compose.
- **Reached by both execution paths**: single-table aggregate
  (`RonSQLPreparer.cpp:5854`) and join/CTE aggregate (`:6360`) both drain
  through `m_resultprinter->print_result(...)`. So ORDER BY + LIMIT on
  join-agg and CTE-join-agg queries **should already work** — it is the
  same printer and the same validation.
- **EXPLAIN** already prints the ORDER BY and LIMIT sections
  (`RonSQLPreparer.cpp:11769-11785`; "Result limited to N rows"
  `ResultPrinter.cpp:2458`).
- **Test coverage**: single-table only (`ronsql_basic.test:250-284` —
  LIMIT with ORDER BY, LIMIT 0, LIMIT > result set). **Zero** join/CTE
  coverage — which is why the CLI registry assumed it unsupported.

### Rejected by design — projection-only (non-aggregate) shapes

The non-aggregate parse gate requires `!has_orderby && !has_limit` for
both accepted passthrough shapes:

- `RonSQLPreparer.cpp:569` — E.3 projection-only CTE_SCAN root.
- `RonSQLPreparer.cpp:588` — I.8/I.11/I.12 CTE-lookup join chains.

The passthrough drain (`execute_passthrough_drain`, `:6416`) streams rows
one at a time via `print_passthrough_row`; the passthrough ResultPrinter
constructor hardcodes `m_has_orderby = false` and carries no column
metadata (`ResultPrinter.cpp:165-189`). `fs_topk`
(`SELECT c_custkey, c_name, recent.cnt, recent.spend FROM customer JOIN
recent ... ORDER BY recent.spend DESC LIMIT 100`) is exactly this shape.

### Unsupported shape entirely — single-table non-aggregate SELECT

`SELECT cols FROM t WHERE ... ORDER BY ... LIMIT n` (`fs_history`) is
rejected at the `:523` gate no matter what — RonSQL has no non-aggregate
single-table execution path at all (tracked as general non-aggregate
support, `ronsql_join_phase7.md` step 45). ORDER BY/LIMIT is only part of
what's missing here.

### Silently wrong — CTE bodies and subqueries (correctness bug)

The grammar stores `orderby_columns` / `limit` on every SELECT body —
CTE bodies (`RonSQLParser.y:338-339`) and subqueries (`:743-744`) — but
**nothing ever consumes them**; only `ast_root`'s are read by
ResultPrinter. The only other references are `collect_scope_column_refs`
(marks the columns as referenced) and the EXPLAIN parse-tree print.

So `WITH c AS (SELECT ... ORDER BY x LIMIT 10) SELECT ... FROM c` runs the
body **without** the LIMIT and returns different results than MySQL —
silently. The correlated-subquery rewrite additionally strips
`GROUP BY / ORDER BY / LIMIT` textually (`:3177`, `:3686`).

## Phases

### Phase 0 — correctness: reject silently-ignored ORDER BY / LIMIT (small; ship first)

Reject with `RonSQLPermanentError` at prepare time:

- Any CTE body with `orderby_columns != NULL || limit >= 0`. Site: the
  CTE walk in `analyze_ctes` / `build_cte_scopes` (each `cte->stmt`).
  Message: "ORDER BY / LIMIT inside a CTE body is not supported; it was
  previously ignored, which silently changed results."
- Same check for subquery statements on the subquery load path (before
  the `:3177` text rewrite can strip them).

MTR: rejection cases in the `ronsql_cte` suite (body ORDER BY, body
LIMIT, subquery LIMIT). This is a behavior change — previously
"working" (wrong) queries now error; that is the point.

Implementing body-level ORDER BY + LIMIT for real means distributed
top-N in the kernel (per-fragment sorted+limited scan + merge) — a large
separate feature. Rejection is the honest v1; revisit in Phase 6.

### Phase 1 — verify and lock in: aggregate join/CTE ORDER BY + LIMIT

Expected to need little or no engine code — the machinery is shared with
the tested single-table path. Deliverable is an MTR family
(`body_orderby_limit.inc` + `ronsql_cte_dd_orderby_limit.test` ×5
topology suites, following the `body_main_root_index.inc` pattern):

- Join-agg: `ORDER BY <groupby col>` ASC/DESC; multi-column with unique
  tie-breaker; `LIMIT 0 / N / > result set`.
- `ORDER BY <aggregate alias>` for SUM / COUNT / MIN / MAX — including
  string MIN/MAX (charset collation order, I.6) and DATE/temporal
  outputs (D17 Bigunsigned packing must sort by packed value = date
  order; verify DESC display).
- CTE-join-agg (fs_batch shape) + ORDER BY + LIMIT; two-CTE query;
  HAVING + ORDER BY + LIMIT; scalar CTE + LIMIT.
- Qualified `ORDER BY cte.col` where the col is in GROUP BY (stays
  TABLE_COLUMN — validate path `:266`).
- Error case: ORDER BY column not in GROUP BY.

Likely fallout candidates to watch: charset metadata for CTE virt-table
GROUP BY columns in `column_metadata` (built from
`resolved_columns[].dict_column`, `:5118-5169` — virt columns plumb back
to source dict columns via `resolve_cte_output_columns_for_scope`, but
ORDER BY on a virt col is untested), and collation-order vs binary-order
mismatches versus MySQL baselines.

### Phase 2 — projection-only shapes: LIMIT alone (streaming)

Relax the two gates (`:569`, `:588`) to accept `has_limit` while still
requiring `!has_orderby`. In `execute_passthrough_drain`: count printed
rows, stop at the limit, then `query->close()` — early close of the scan
is the part of I.14 that already works at kernel/API level
(block-tested; see `cte_filter_phase_n.md`). LIMIT 0 keeps the
deferred-header convention (no header, matching empty-result baselines).
MTR: LIMIT on E.3 root scans and I.8 join chains, including a case where
LIMIT stops mid-scan on a large body.

### Phase 3 — projection-only shapes: ORDER BY [+ LIMIT] (buffered client-side sort)

Relax the gates fully. Rows arrive via per-row `NdbRecAttr` buffers that
are overwritten each fetch, so sorting requires buffering:

- **StoredPassthroughRow**: per row, arena-copy each output column's
  bytes + null flag (and any extra ORDER BY-only columns), mirroring the
  agg path's `store_record` / `print_stored_record` precedent in
  ResultPrinter.
- **Sort spec resolution**: map each ORDER BY target to a stored-column
  position. v1 scope: any resolvable main-scope column — real-table
  columns not in the SELECT get an extra `getValue` registration; CTE
  columns are always all fetched anyway (kernel emits every virt-table
  column). Aliases resolve via the existing `resolve_orderby_aliases`.
- **Comparator**: `NdbSqlUtil::getType(...).m_cmp` with the dict/virt
  column type + charset (available via `resolved_columns[].dict_column`
  / the virt-table column); NULLs first in ASC — same rules as
  `compare_rows`. LEFT JOIN NULL-marker rows (`effective_attrs`
  substitution) sort as NULL.
- **LIMIT**: `std::partial_sort` when `limit < rows`, else full sort;
  truncate post-sort.
- **Memory cap**: buffered unaggregated results are unbounded — enforce
  a cap (row count and/or bytes; either a RonSQL config knob or a fixed
  generous default) with a permanent error advising LIMIT-less queries
  to add aggregation or a tighter WHERE.
- Print via a stored-row variant of `print_passthrough_row`; the
  passthrough ResultPrinter constructor grows an optional
  column-metadata argument (or the sort lives entirely in the drain and
  ResultPrinter stays formatting-only — preferred).

Unlocks `fs_topk`. MTR: ORDER BY CTE aggregate output DESC + LIMIT,
ORDER BY real-table column not in SELECT, string collation order, LEFT
JOIN NULL ordering, tie-breakers.

### Phase 4 — single-table non-aggregate SELECT (new shape; unlocks fs_history)

Bigger than ORDER BY/LIMIT itself, but the missing pieces are mostly
assembly of existing machinery:

- **Front-end**: accept projection-only single-table queries
  (all-COLUMN outputs, no joins/CTEs/GROUP BY/HAVING) in the `:523`
  gate.
- **Planning**: `plan_index_and_filter` already runs for
  `!is_join_query()` and selects index/bounds/residual filter — no new
  work.
- **Execution**: new non-aggregate drain on the single-table path:
  `NdbScanOperation` / `NdbIndexScanOperation` + `getValue` per
  projected column + the existing interpreted filter, feeding the
  Phase 3 buffered sort (or streaming when no ORDER BY, with Phase 2
  LIMIT early close).
- **4b (optional, the real fs_history win)**: when ORDER BY is a prefix
  of the chosen ordered index with uniform direction, use
  `SF_OrderBy` (+ `SF_Descending`) so the multi-fragment merge scan
  streams in global index order — emit rows as they arrive, stop at
  LIMIT, close early. No buffering, no full scan. This resolves the
  long-standing `:5783` "todo Decide whether SF_OrderBy is good for
  performance" — under `ORDER BY indexed_col LIMIT n` it is exactly
  MySQL's top-N-via-index plan. Without a matching index, fall back to
  buffered sort.
- EXPLAIN: single-table plan print + existing ORDER BY/LIMIT lines;
  print whether the sort is index-order or buffered.

### Phase 5 — CLI enablement + benchmarks

**Partially delivered early** (ahead of Phase 1 MTR verification, since the
aggregate-path support already exists): `fs_topk` rewritten into its
aggregate-form equivalent (`GROUP BY c_custkey, c_name` with `MAX()` as the
per-unique-key identity, `ORDER BY top_spend DESC LIMIT 100`) and un-flagged;
`cte_tpch_q2` gained `ORDER BY p_mfgr LIMIT 100` and `cte_tpch_q22`
`ORDER BY c_nationkey` (deterministic output mirroring the officials);
registry comments and `ronsql_cli_benchmarks.md` updated. `cte_tpch_q11`
(single group) and `q13`/`q15` (single-row implicit agg) have nothing to
order. Audit result for the officials: **all five** `tpch_q*_official` have
blockers beyond ORDER BY/LIMIT — comma-join syntax (q2/q11/q15), derived
tables in FROM (q13/q22), correlated/scalar subqueries (all), LIKE /
SUBSTRING / IN (q2/q13/q22) — so none can flip regardless of this plan.

Remaining for this phase:
- Flip `MySQLOnly` off: `fs_topk` original projection-only form (after
  Phase 3 — optionally revert the aggregate-form rewrite then),
  `fs_history` (after Phase 4).
- Phase 1's MTR family retroactively locks in the shapes the early
  delivery relies on (ORDER BY aggregate alias DESC + LIMIT on a
  CTE-join aggregate; ORDER BY GROUP BY col).

### Phase 6 — deferred follow-ups (out of scope by default)

- `ORDER BY 1` positional refs and direct aggregate expressions
  (`ORDER BY SUM(x)` without alias) — small grammar + resolver work.
- `LIMIT offset, count` / `OFFSET` — trivial print-truncation change.
- Distributed top-N pushdown (per-fragment sorted+limited scans merged
  at the API/RDRS) — the only way LIMIT reduces *scan* work for
  non-index-order queries; large kernel feature, separate plan.
- CTE-body ORDER BY + LIMIT (top-N feature inputs) — requires the same
  distributed top-N; Phase 0's rejection stands until then.

## Risks / notes

- **Phase 0 is a behavior change**: previously-accepted (silently wrong)
  queries start erroring. Call it out in the commit message.
- **Sort memory**: the agg path buffers only groups (small); Phases 3-4
  buffer raw rows — the cap is mandatory, not optional.
- **Baseline determinism**: MTR ORDER BY cases must include a unique
  tie-breaker column, or rely on `ronsql_compare.inc`'s sorted
  comparison; mixed-collation columns can order differently than
  MySQL's — compare against MySQL, not intuition.
- `compare_rows` builds `std::string` copies per string comparison
  (`ResultPrinter.cpp:941-963`) — fine at group counts; if Phase 3/4
  profiling shows cost on big projection sorts, precompute sort keys.
- Aggregate-path LIMIT remains a print cutoff (all groups are always
  materialized and shipped) — inherent to aggregation, not a gap.

## Verification (user-run)

- `./mtr --suite=ronsql ronsql_basic` (regression) and the new
  `ronsql_cte_dd_orderby_limit` family ×5 topology suites.
- `.bench_ronsql fs_topk` / `.bench_sql fs_topk` (Phase 3),
  `.bench_ronsql fs_history` (Phase 4), `.explain_ronsql` showing the
  ORDER BY / LIMIT sections and (4b) index-order vs buffered sort.
