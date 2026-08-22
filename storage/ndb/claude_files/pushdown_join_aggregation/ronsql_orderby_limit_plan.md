# RonSQL ORDER BY / LIMIT Support Plan

**Status: Phases 0-3 SHIPPED (recorded green ×5 topology suites,
2026-08-21).**  Phase 3 = buffered client-side sort for projection-only
ORDER BY [+ LIMIT]: cloned-NdbRecAttr row buffering, NdbSqlUtil
comparators, partial_sort top-N, 1M-row/256MB cap;
`body_passthrough_orderby.inc` po-1..14 + P1 ×5 suites (first record
surfaced + fixed the shared-bare-spelling alias-mark poisoning — see
the Phase 3 section); retired the pl-P1/P2 + st-P1/P2
ORDER-BY-rejection probes (`body_passthrough_limit` +
`body_passthrough_single_table` re-recorded); CLI: fs_topk restored to
projection-only form, fs_history un-flagged.  Phase 4's
fs_history-unlock role collapsed into Phase 3 (the single-table shape
already existed); remaining Phase 4 value = the optional 4b SF_OrderBy
index-order streaming top-N; Phases 5-6 partially delivered / deferred
as noted inline.**
Phase 0 (2026-08-20, RONDB-1107): body/subquery ORDER BY / LIMIT
rejection.  Phase 1 (2026-08-21, commits `1d41ae61713` /
`8e3b7725493` / `9821d27f49c`): `body_orderby_limit.inc` ob-1..ob-22 +
ob-P1..P3 plus the two engine fixes the first record surfaced —
`canonicalize_orderby_columns` for mixed bare/qualified ORDER BY vs
GROUP BY spellings, and ResultPrinter LIMIT-0 header suppression
(mysql-client empty-result parity; `ronsql_basic` +
`ronsql_orderby_stress` baselines re-recorded).  Phase 2 (2026-08-21,
all green on first record): LIMIT streams with early close on all
three projection-only gate shapes; MTR `body_passthrough_limit.inc`
pl-1..14 + P1/P2 ×5 suites + the st-P2 conversion in
`body_passthrough_single_table.inc`.**  Phase 0
adds `reject_ignored_orderby_limit` (RonSQLPreparer) called from
`analyze_ctes` (per CTE body), `analyze_subqueries_ce` (scalar / IN /
EXISTS arms — checked on the ORIGINAL parsed statements before the
decorrelation text rewrites can strip the clauses), and
`analyze_select_subqueries` (SELECT-list subqueries).  MTR:
`ronsql_cte_dd_orderby_limit_reject.test` (obl-1..7, base suite only —
prepare-time rejection is topology-independent).  Audit confirmed no
existing MTR case or RonSQL-enabled benchmark query used ORDER BY /
LIMIT in a body/subquery position, so the behavior change breaks no
green coverage.

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

### Phase 0 — correctness: reject silently-ignored ORDER BY / LIMIT (small; ship first) — ✅ IMPLEMENTED (see Status)

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

### Phase 1 — verify and lock in: aggregate join/CTE ORDER BY + LIMIT — ✅ IMPLEMENTED (see Status)

**Second first-record finding (FIXED): LIMIT 0 printed a bare TSV
header.**  ob-5 diffed against MySQL's empty output with a lone header
line.  Long-standing, not join-specific: the mysql client prints
nothing for an empty result set, and the base-suite LIMIT 0 baselines
(`ronsql_basic.result`, `ronsql_orderby_stress.result`) had the exact
mismatch baked in as recorded diffs under the non-strict `|| true`
compare — the strict order-verifying family is what finally failed on
it.  Fix in ResultPrinter: buffered `print_result_ordered` gates the
TSV header on `print_count > 0` (matching its `num_rows == 0` early
return), and the streaming TSV path checks the LIMIT cutoff before the
deferred header block (ob-20's scalar LIMIT 0 arm).  JSON was already
consistent (`[]`).  `ronsql_basic` + `ronsql_orderby_stress` need
re-recording — their baselines memorialize the old behavior.

**First-record finding (FIXED — engine change 1 of this phase):**
ob-1 failed its first record: bare `ORDER BY c_nationkey` against
qualified `GROUP BY cu.c_nationkey` threw "ORDER BY column not in GROUP
BY clause" (MySQL accepts it).  The parser registers (qualifier, name)
pairs as distinct col_idx entries and
`ResultPrinter::validate_orderby_columns` matches GROUP BY membership by
raw col_idx equality; big-12 dodged it because its unaliased
`SELECT c.c_custkey` output made `ORDER BY c_custkey` an OUTPUT_REF.
Fix: `RonSQLPreparer::canonicalize_orderby_columns()` (called from
`compile()` before ResultPrinter construction) rewrites TABLE_COLUMN
ORDER BY col_idx values to the GROUP BY entry's col_idx when both
resolve to the same underlying column via
`m_main_scope.resolved_columns` (StoredColumn: join_op_idx + attr_id;
CteResultColumn: cte_def_idx + cte_result_idx); works for join and
single-table paths (both populate resolved_columns).  ob-1/ob-2 pin
both spelling directions.  The adjacent sharp edge — SELECT output
spelling differing from GROUP BY spelling — is ORDER BY-independent and
out of scope here.

**As implemented (2026-08-21):**
`body_orderby_limit.inc` + `ronsql_cte_dd_orderby_limit.test` in
`ronsql_cte` + the 4 topology siblings.  Unlike the other families (and
bigquery big-12), the ORDER BY cases set `$skip_sort=yes` so
`ronsql_compare.inc` strict-diffs the RonSQL and MySQL outputs in
DELIVERED row order — every ORDER BY is made a total order (unique
leading key or explicit tie-breaker) so the diff is deterministic.
Case map: ob-1/2/3 GROUP BY-column ordering (bare ORDER BY vs qualified
GROUP BY ASC, the reverse spelling mix DESC — both pinning the
canonicalize fix above — and multi-column mixed-direction with a
VARCHAR leading key); ob-4/5/6 LIMIT
10/0/1000 on the buffered ordered path; ob-7 streaming LIMIT cutoff
without ORDER BY (group column omitted from SELECT so the truncated
rows are value-uniform); ob-8..ob-15 aggregate-alias ordering — COUNT
DESC, SUM ASC with the LIMIT boundary inside a tie run, MIN/MAX int
unique-key ordering, string MIN/MAX (constant-per-group mktsegment ties
+ varying p_name), DATE MIN/MAX ASC+DESC (D17 packing), and
DATETIME/TIMESTAMP(6) via an evlog self-CTE-join; ob-16 fs_topk
aggregate shape (MAX-of-SUM(DECIMAL) alias DESC + LIMIT 20, plus
non-fatal EXPLAIN greps for "Result sorted by" / "Result limited to");
ob-17 two-CTE query with an all-tie primary sort key; ob-18 HAVING +
ORDER BY + LIMIT; ob-19 scalar-CTE cross-join (cs14 shape) + LIMIT 1;
ob-20 scalar aggregate + LIMIT 0; ob-21 qualified ORDER BY on a CHAR(1)
CTE virt column (agg-17 CTE_SCAN-root re-agg — the charset-metadata
fallout candidate); ob-22 big-12 shape with qualified GROUP BY-column
ordering.  Probes: ob-P1 ORDER BY column not in GROUP BY (permanent
error), ob-P2 `ORDER BY SUM(...)` pinned as a syntax error (identifiers
/ aliases only — this is why body_mainmode.inc's old deferral probes
never worked), ob-P3 `LIMIT off,cnt` syntax rejected (OFFSET is Phase
6).  GROUP BY `<cte>.col` on the JOIN path (vs the CTE_SCAN-root re-agg)
has no green precedent and is recorded as a NEXT-PHASE probe, not
executed.  body_mainmode.inc's stale "ORDER BY / LIMIT / HAVING on CTE
queries deferred" markers were rewritten to point here (DISTINCT and
projection-only ORDER BY / LIMIT remain deferred there).  MTR result
files need the first `--record` ×5 suites.

Original scope (all covered above):

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

### Phase 2 — projection-only shapes: LIMIT alone (streaming) — ✅ SHIPPED (2026-08-21, all green on first record ×5 topology suites)

**As implemented:** since this plan was written, the projection-only
envelope grew (non_aggregate_pushdown_plan.md Phases 1-5), so the "two
gates" are now the three shape conditions of the unified non-aggregate
gate — E.3 CTE_SCAN root, single-table, and join chains — and ALL three
drop `!has_limit` (ORDER BY stays rejected until Phase 3).  Streaming
cutoff in both drains:

- `execute_passthrough_drain` (multi-op): the fetch loop stops once
  `limit` rows are printed and returns — the caller's unconditional
  `query->close()` right after IS the early close (I.14 kernel/API
  early close, block-tested).  With LIMIT 0 the loop never runs, so
  the deferred TSV header is never printed (mysql-client empty-result
  parity); JSON keeps its `[` ... `]` framing.
- `execute_single_table_passthrough`: the scan arm gets the same loop
  restructure + an explicit `scanOp->close()` on early termination;
  the PK-lookup arm treats LIMIT 0 as "print nothing" (LIMIT >= 1
  cannot constrain a single-row lookup further).

EXPLAIN needed no work — the parse-tree section already prints
`LIMIT N` unconditionally.  MTR: `body_passthrough_limit.inc` +
`ronsql_cte_dd_passthrough_limit.test` ×5 topology suites (pl-1..14 +
pl-P1/P2): CTE_SCAN root truncating/batch-boundary (LIMIT 260 of 300
crosses the 256-row API batch)/LIMIT 0/LIMIT > set, join-chain
truncating/0/> set, real-table snowflake chain + residual WHERE +
LIMIT, single-table index-scan/full-scan/PK-lookup (hit + miss) with
LIMIT 1/0/> set; truncating cases project value-uniform columns for
determinism.  Probes pin ORDER BY (± LIMIT) still rejected.
`body_passthrough_single_table.inc` st-P2 ("LIMIT rejected") converted
to an ORDER BY + LIMIT rejection — that family needs re-recording ×5.
No CLI registry flips: fs_topk / fs_history both need ORDER BY
(Phases 3 / 4b).

Original scope: relax the gates to accept `has_limit` while still
requiring `!has_orderby`; count printed rows, stop at the limit, then
`query->close()`; LIMIT 0 keeps the deferred-header convention; MTR
incl. a case where LIMIT stops mid-scan on a large body.  (All covered
above.)

### Phase 3 — projection-only shapes: ORDER BY [+ LIMIT] (buffered client-side sort) — ✅ SHIPPED (2026-08-21, all green on record ×5 topology suites)

**First-record finding (FIXED): shared bare spelling poisoned output
resolution.**  po-1 (`SELECT k, t FROM cf ORDER BY k`) failed: the
ORDER BY name and the bare column output share one parser registry
entry, and `resolve_orderby_aliases`' `m_col_is_alias` mark made the
scoped resolver classify the entry AliasOnly before trying column
resolution — the output then failed the drain's kind check.  Fix: skip
the alias mark when the matched output is a plain COLUMN with the same
col_idx (the entry is provably a real column reference; the
single-table resolver already resolved column-first).  Also cures the
latent aggregate-path variant with all-bare `SELECT k ... GROUP BY k
ORDER BY k` over a CTE, which the always-aliased ob- family never hit.

**As implemented:** the gate drops its last ORDER BY restriction on all
three projection-only shapes.  Instead of the sketched
StoredPassthroughRow byte copies, rows are buffered as arrays of
`NdbRecAttr::clone()`s (public API, delete-by-application), which keeps
ResultPrinter formatting-only — the plan's preferred split — and reuses
`print_passthrough_row` unchanged; an RAII `PassthroughSortBuffer`
frees the clones on every exit path.  Sort keys resolve to stored-row
slots by resolved-column identity (`same_resolved_column`, shared with
the Phase 1 canonicalize fix): a sort column already in the SELECT
reuses its output slot, an ORDER BY-only real-table column gets an
extra `getValue` on its op, an ORDER BY-only CTE column reuses the
always-fetched virt-table registration.  Comparator =
`NdbSqlUtil::getType(col->getType()).m_cmp` with `col->getCharset()`
on the clones' raw attribute bytes (length prefixes included, same as
the aggregate path's GROUP BY-column arm); NULLs first in ASC; op-level
LEFT JOIN NULL rows stored as NULL pointers; column types without an
m_cmp are rejected at key-resolution time.  LIMIT applies post-sort via
`std::partial_sort`; LIMIT 0 skips the drain entirely (deferred-header
convention).  Fixed caps: 1,000,000 rows / 256 MB of cloned data →
clean permanent error (LIMIT cannot reduce the buffering).  The
PK-lookup arm sorts trivially (≤ 1 row) and is unchanged beyond its
Phase 2 LIMIT-0 suppression.  MTR: `body_passthrough_orderby.inc` +
`ronsql_cte_dd_passthrough_orderby.test` ×5 topology suites (po-1..14
+ po-P1), `$skip_sort=yes` order-verifying strict diffs with
total-order tie-breakers: CTE_SCAN root incl. ORDER BY a CTE aggregate
output DESC + LIMIT cutting inside a tie run and ORDER BY a
non-projected CTE column; join chains incl. a non-projected real-table
sort column (extra getValue) and string collation order; LEFT JOIN
NULLs-first-ASC / NULLs-last-DESC with LIMIT across the NULL run;
single-table DESC / non-projected sort column / DATE / DECIMAL / PK
lookup.  The Phase-2-era ORDER-BY-rejection probes (pl-P1/P2, st-P1/P2)
are retired — `body_passthrough_limit` and `body_passthrough_single_table`
need re-recording ×5 alongside the new family's first record.  CLI
(Phase 5 delivery): `fs_topk` restored to its natural projection-only
form; `fs_history` un-flagged (`MySQLOnly` removed).

Original sketch (superseded where noted above):

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

Remaining for this phase — **DONE with Phase 3 (2026-08-21)**:
- ~~Flip `MySQLOnly` off~~: `fs_topk` reverted to its original
  projection-only form and `fs_history` un-flagged — both ride the
  Phase 3 buffered client-side sort (fs_history's single-table shape
  had already been built by non_aggregate_phase_1.md, so Phase 4's
  role in unlocking it collapsed into Phase 3).
- ~~Phase 1's MTR family retroactively locks in the shapes~~ — done
  (body_orderby_limit.inc, recorded green ×5).

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
