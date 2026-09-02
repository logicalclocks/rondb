# passthrough_orderby_index family — findings

Phase 4b of `ronsql_orderby_limit_plan.md`: ORDER BY served by
ordered-index scan order on the single-table pass-through path.  When
the planner's chosen ordered index delivers the ORDER BY order — the
ORDER BY list equals the index columns after any leading
equality-bound columns, with one uniform direction — the scan runs with
`SF_OrderBy [| SF_Descending]`; the NDB API (old/RecAttr path
auto-upgrades to `SF_OrderByFull`, adding the index key columns to the
read set and forcing full fragment parallelism) merge-sorts the
per-fragment ordered scans into one globally ordered stream, and the
drain streams through the Phase 2 LIMIT cutoff + early close instead
of buffering for the Phase 3 client-side sort.  Under
`ORDER BY indexed_col LIMIT n` that is MySQL's top-N-via-index plan:
roughly one batch per fragment is read.  This also resolves the old
`open_single_table_scan_op` "todo Decide whether SF_OrderBy is good
for performance" — it is, exactly when the ORDER BY asks for it; the
aggregate single-table scan never sets it (the aggregator consumes
every row, so the serialised merge would be pure cost).

Planner rules (`plan_index_and_filter` →
`add_orderby_scan_config_candidates`, pass-through only):

- **WHERE-first.**  The generic bound-based candidates keep their
  goodness; one whose index also serves the order is tagged
  `ScanConfig::index_order` and gets `ORDERBY_INDEX_BONUS` (10), a
  tie-breaker below any bound's value (100+).  A better bound on a
  non-ordering index still wins and the query sorts client-side
  (poi-11 fs_history shape, poi-12).
- **ORDER BY-driven choice only against the table scan.**  An index
  with no usable bound that serves the order is pushed as a bonus-only
  candidate (all conjuncts residual filters), so it beats the
  goodness-0 table scan (poi-4/5/6/7/8/17) — a full index scan in key
  order with early close under LIMIT; without LIMIT it trades the
  Phase 3 buffer (and its 1M-row / 256 MB cap) for the ordered merge.
- **Equality-prefix skipping.**  `WHERE a = 5 ORDER BY b` on index
  `(a, b)` is in b order (poi-1/2/9/16/18); a range-bound leading
  column cannot be skipped (poi-12).  Mixed directions never match
  (poi-10).
- **Hints.**  FORCE/USE/IGNORE INDEX apply to the ORDER BY pass with
  the generator's semantics (poi-13/14).  The generator's two FORCE
  INDEX satisfiability throws are deferred past the ORDER BY pass
  (`validate_force_hint_after_orderby`), so a forced index serving
  only the ORDER BY is accepted (poi-13); a forced index serving
  neither is rejected with the ORDER BY-aware messages (poi-P1/P2).
  The aggregate / CTE-body / join-root generators are unchanged.

EXPLAIN: the single-table scan block prints
`ORDER BY: index order (SF_OrderBy[ | SF_Descending] merge of the
fragment scans, streamed, no client-side sort).` or
`ORDER BY: client-side sort (rows buffered, then sorted; LIMIT applied
after the sort).` (PK lookup: `single-row primary key lookup, no sort
needed`); an ORDER BY-driven bound-less index candidate prints
`Chosen for ORDER BY index order (no bounds).` + FILTERS instead of
the CONDITIONS block.  The family's EXPLAIN greps are FATAL.

**Runtime verification (not just the plan).**  EXPLAIN greps prove what
the preparer chose; they cannot prove the drain actually streamed.  Every
case therefore also runs `suite/ronsql/include/ronsql_phase_rows.inc`,
which POSTs the query to RDRS, prints the `rows=` token of the
`x-ronsql-phases` header (`RonSQLPhaseStats::rows_drained` — rows the
RonSQL result loop fetched) into the baseline and asserts it with a
fatal grep.  The index-order path drains exactly min(LIMIT, matching
rows) because it stops at the LIMIT and closes the scan (poi-4: 12 of
1500, poi-8: 260 of 1500, poi-16: 6 of 100, poi-17: 62 of 300, poi-15:
0); the client-side fallbacks must drain every matching row before they
can sort (poi-10: 500, poi-11: 15, poi-12: 1000, poi-14: 1500).  Both
paths print identical, correctly ordered rows, so the order diff alone
could not tell them apart — the row counter can.

**Found + FIXED on first record — bound-less ordered scan hit NDB 4259.**
poi-4 (`ORDER BY o_orderdate DESC LIMIT 12`, no WHERE — the first
ORDER BY-driven candidate with no bounds) failed with "NDB Permanent
error 4259, Application error: Invalid set of range scan bounds /
Failed to set end of bound.": `open_single_table_scan_op` called
`end_of_bound(0)` unconditionally after the bound loop, and the old-API
`NdbIndexScanOperation::end_of_bound` rejects a range that no
`setBound()` opened (`currentRangeOldApi == nullptr`).  Before 4b every
index-scan candidate had at least one bound, so the call was always
legal.  Fix: track `any_bound` in the loop and close the range only
when one was added — a full index scan in key order needs no range.
poi-1..3 (bounded index-order scans) had already passed.

Prose notes:

- **Tie handling under strict diffs.**  Index-order cases cannot add
  a tie-breaker (it would break the prefix match).  o_orderdate is
  unique within one o_orderstatus value (n and n+1000 never share a
  status) and for dates ≥ 1996-05-16 (n+1000 > 1500); o_clerk = n+1 is
  unique for o_totalprice < 400.  Everything else projects only the
  ORDER BY key so tied rows print identically.
- **NULL placement** follows the ordered index (NULL sorts lowest):
  first in ASC, last in DESC — MySQL semantics, same as the Phase 3
  comparator (poi-6/7).
- **Batch boundary under the ordered merge** (poi-8, LIMIT 260 of
  1500): at least one receiver must fetch a second batch in every
  topology before the drain closes the scan early.
- **fs_history is NOT index-ordered** (poi-11): its WHERE picks
  idx_o_custkey, which is not in date order.  The CLI gained
  `fs_latest` (no WHERE, `ORDER BY o_orderdate DESC LIMIT 100`) as the
  index-order benchmark shape.

Deferred (not bugs):

- Switching away from a WHERE-chosen index to an ORDER BY index with
  the WHERE as filter (MySQL does this for small LIMITs) — needs
  selectivity estimates; conservative WHERE-first policy kept.
- Ordered scans on multi-range (DNF) bounds — the single-table path
  emits a single range (`end_of_bound(0)`), so not reachable here.
- ORDER BY an index column whose equality is a FILTER (not a bound)
  would still be in order; treated as no-match (conservative).
