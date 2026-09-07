# RonSQL COUNT(*) fragment-stats shortcut (the fs_floor win)

**Status: W1 + W2 IMPLEMENTED (September 2026, pending user build +
first --record of the cst family ×5); W3 fs_floor benchmark pending.**

W1 outcome notes: `is_count_star_only_query` (single table, no
WHERE/GROUP BY/HAVING/subqueries/CTEs, every output COUNT over a
constant — the parser lowers COUNT(*) to COUNT(1), detected via
`isLoadConstantInt`, so explicit COUNT(1) qualifies too and COUNT(col)
never does) + `execute_count_star_shortcut` (the ndb_table_stats
mechanics on the old API: getNdbScanOperation + LM_CommittedRead +
getValue(NdbDictionary::Column::ROW_COUNT) + a table-less
one-instruction interpret_exit_last_row program via
setInterpretedCode, per-fragment pseudo-rows summed; phase stats keep
the ndbprep/firstbatch/print names so x-ronsql-phases stays
comparable).  Short-circuits in execute() after the shared
transaction/rate-limit setup, before the NdbAggregator is built.
`ResultPrinter::print_count_star_result` mirrors the compiled print
program byte-for-byte (JSON `[{"name":value,...}\n]\n` incl. the
LIMIT-0 empty array; TSV header + row with LIMIT-0 full suppression;
ORDER BY a no-op on one row; values as Int64 like PRINT_AGGREGATE).
EXPLAIN prints `Execute as fragment-stats COUNT(*) (no row scan).`
W2: `body_count_star.inc` (cst-1..7 + controls cst-c1..c4, ×5
topology wrappers `ronsql_cte_dd_count_star`) — region/orders/empty
table, LIMIT 0, COUNT(1), multi-output, ORDER BY no-op, and the
non-eligible controls (COUNT(nullable) with EXPLAIN absence pin,
WHERE, GROUP BY, mixed aggregates).

Maintainer observation from
the single-group work: `SELECT COUNT(*) FROM t` with no WHERE and no
GROUP BY can take the same shortcut mysqld takes — read the ROW_COUNT
pseudo-column once per fragment instead of scanning every row.  On the
reference laptop that is ~25 µs of a 128 µs request (~20%), and the
"floor" query is a common feature-store health check:

```
fs_floor: SELECT COUNT(*) FROM region;   -- "Fixed-overhead floor:
          single-table COUNT(*) ... lower bound for every phase"
```

## The mysqld reference mechanism (verified in-tree)

`ndb_get_table_statistics` (storage/ndb/plugin/ndb_table_stats.cc:41):
an NdbRecord scan with

- an interpreted program of exactly `interpret_exit_last_row()` —
  DBTUP returns ONE pseudo-row per fragment instead of iterating rows;
- `SO_GETVALUE` extra-gets of the pseudo-columns
  (`NdbDictionary::Column::ROW_COUNT`, plus COMMIT_COUNT/sizes that
  RonSQL does not need);
- `LM_CommittedRead`, `SO_USE_STANDARD_SCAN`, batched;
- the caller sums ROW_COUNT across the fragment pseudo-rows.

The old-API equivalent RonSQL's single-table path already speaks:
`scanTable` + `readTuples(LM_CommittedRead)` +
`scan->getValue(NdbDictionary::Column::ROW_COUNT)` +
`scan->setInterpretedCode(&code)` with the one-instruction program.

## Semantics

ROW_COUNT is the committed per-fragment row count — the SAME source
mysqld's COUNT(*) fast path reads, so RonSQL-vs-mysqld parity holds by
construction (both engines answer from the same statistic; concurrent
uncommitted transactions are invisible to both).  In quiesced MTR
clusters the counts are exact, so the strict-diff families stay green.

## Eligibility (v1, deliberately literal)

- Single stored table (no joins, no CTEs anywhere in the statement).
- No WHERE, no GROUP BY, no HAVING.
- Every output is `COUNT(*)` (typically one).  `COUNT(col)` is NOT
  eligible — it counts non-NULL values and must scan.  Any other
  aggregate alongside disqualifies.
- Main-level ORDER BY / LIMIT on the one result row are the existing
  printer no-ops; LIMIT 0 already suppresses output before this path.

Everything else falls through to the existing aggregation scan
unchanged — the shortcut is pure fast-path, never a behavior change.

## Work items

- **W1 implementation**: detection in the single-table aggregate
  execute path (the plain-NDB-API branch — short-circuit before the
  AggInterpreter scan is built); a small fragment-stats read helper
  (the ndb_table_stats mechanics, ROW_COUNT only); sum and deliver the
  value through ResultPrinter's normal aggregate formatting so
  TEXT/JSON output formats and headers behave identically to the
  scanned path; EXPLAIN line `Access: fragment-stats COUNT(*)` (or
  equivalent) so tests can pin the path.
- **W2 MTR** (`ronsql_count_star` or an existing single-table family,
  ×5 topologies): shortcut hit with EXPLAIN pin + value compare vs
  mysqld; controls proving identical results AND no shortcut for
  `COUNT(col)`, `COUNT(*) WHERE ...`, `COUNT(*) ... GROUP BY`,
  `COUNT(*)` with a second aggregate; a larger table (orders, 1500
  rows) and an empty table (COUNT=0 via stats).
- **W3 benchmark**: `.bench_ronsql fs_floor` before/after on the prod
  build — the ~25 µs claim is the acceptance number; the
  `x-ronsql-phases` breakdown should show the drain/ndbprep phases
  collapsing.

## Covered for free: COUNT(*) as an uncorrelated scalar subquery

`execute_subqueries` constructs a full nested RonSQLPreparer for the
inner SQL and calls its execute(), so an uncorrelated
`(SELECT COUNT(*) FROM s)` takes the shortcut INSIDE the inner
preparer with zero extra code — the substituted value comes from the
fragment-stats read.  (A CORRELATED count subquery carries a WHERE on
the correlation column and correctly declines — it must scan.)
Pinned by cst-8.  The `m_has_subqueries` gate in the detection guards
the OUTER query only, which is exactly right: the outer must run its
normal path to perform substitution, while each inner query decides
for itself.

## Not covered: COUNT(*) inside a CTE body (v2 candidate)

v1 deliberately declines any statement with CTEs (`m_has_ctes` gate) —
both a COUNT(*)-only CTE BODY and a main-level COUNT(*) alongside
unrelated CTEs.  A scalar CTE body `WITH c AS (SELECT COUNT(*) AS n
FROM t)` today materializes through the full kernel pipeline
(per-thread body scan of every row -> scalar keyLen==0 redistribute ->
FINAL_REP -> CTE_READY); the fragment-stats trick has no seam there.

**v2 sketch — constant-fold the CTE via the subquery-substitution
precedent**: RonSQL already pre-executes scalar subqueries and
substitutes their values (`execute_subqueries` /
`substitute_subquery_results`).  A scalar CTE body that is exactly
COUNT(*)-only over one table could be answered by the same
fragment-stats read UP FRONT and the CTE dropped entirely — no
defineCte, no body scan, no SETUP/redistribute/FINAL_REP barrier: a
whole materialization phase deleted for one stats read.  Bigger win
than the main-level case, bigger surface too: CTE outputs are
referenced in projections and join keys, not only in WHERE comparisons
like the subquery precedent, so substitution must synthesize a
constant column (or v2a restricts to watermark-style WHERE-only
consumers, exactly the subquery shape).  Also composes with the
main-level shortcut: `WITH c AS (...) SELECT COUNT(*) FROM orders`
could lift the m_has_ctes gate once CTE presence provably cannot
affect the main COUNT — deferred until v2 makes that reasoning
necessary.

## Notes / edges

- The pseudo-row scan touches one record per fragment — on
  many-fragment tables it is still far cheaper than row scanning, and
  it parallelizes like any scan.
- Read-backup / fully-replicated tables: the standard scan reads
  primary fragments, as mysqld's does — no divergence.
- No config, no version gate, no wire change: this is entirely
  RonSQL-side, using public NDB API pseudo-columns.
