# passthrough_orderby family — findings

Phase 3 of `ronsql_orderby_limit_plan.md`: ORDER BY [+ LIMIT] on every
projection-only pass-through shape via a buffered client-side sort.
The gate drops its last ORDER BY restriction; the drains buffer each
delivered row as an array of `NdbRecAttr::clone()`s (heap, freed by an
RAII buffer object on every exit path), sort with
`NdbSqlUtil::getType(...).m_cmp` + the column charset (NULLs first in
ASC; op-level LEFT JOIN NULL-extended rows sort as NULL), apply LIMIT
post-sort via `std::partial_sort`, and print through the unchanged
NdbRecAttr-based pass-through printer.  ORDER BY-only columns outside
the SELECT get an extra `getValue` (real tables) or reuse the
always-fetched virt-table registration (CTE columns), matched against
SELECT outputs by resolved-column identity (`same_resolved_column`, the
Phase 1 canonicalize helper) so mixed bare/qualified spellings unify.
A fixed cap (1,000,000 rows / 256 MB of cloned data) bounds the buffer
with a clean permanent error — LIMIT cannot reduce the buffering since
top-N must see every row.

**Found + FIXED on first record — shared bare spelling poisoned output
resolution.**  po-1 (`SELECT k, t FROM cf ORDER BY k`) failed with
"Pass-through drain: output is not a table or CTE column."  When the
ORDER BY name and a SELECT column output are BOTH spelled bare, they
share one parser registry entry; `resolve_orderby_aliases` converted
the ORDER BY to OUTPUT_REF and marked that entry `m_col_is_alias`, and
the scoped resolver treats an alias-marked entry as AliasOnly BEFORE
attempting column resolution — so the output's own entry stopped
resolving.  The single-table resolver was already robust (it tries the
real column first; its comment even anticipates the sharing), but the
join/CTE-scope resolver short-circuits.  Fix in
`resolve_orderby_aliases`: skip the alias mark when the matched output
is a plain COLUMN with the same col_idx — the entry is provably a real
column reference.  This also cures the latent AGGREGATE-path variant
(`SELECT k, SUM(x) FROM cf GROUP BY k ORDER BY k`, all bare — never
hit by the ob- family because its outputs were always aliased).

Prose notes:

- The clone-based buffering deliberately keeps ResultPrinter
  formatting-only (the plan's preferred option): no stored-row print
  variant, no column-metadata plumbing — `print_passthrough_row`
  consumes the cloned rows unchanged.
- po-2 pins partial_sort tie-handling (60-way tie runs cut mid-run by
  the tie-breaker); po-8/9 pin the LEFT JOIN NULL rules in both
  directions; po-4/6/11 pin the ORDER BY-only-column fetch paths (CTE
  reuse, join extra getValue, single-table extra getValue).
- The Phase-2-era ORDER-BY-rejection probes (pl-P1/P2 in
  body_passthrough_limit.inc, st-P1/P2 in
  body_passthrough_single_table.inc) were retired — both families need
  re-recording along with the first record of this one.
- CLI: `fs_topk` restored to its natural projection-only form and
  `fs_history` un-flagged (`MySQLOnly` removed) — both ride this
  phase's sort.

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| GROUP BY on a projection-only query | `WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT k FROM cf GROUP BY k ORDER BY k;` | rejection-assert (po-P1) | Non-aggregate gate still requires no GROUP BY / HAVING / expressions | body_passthrough_orderby.inc |

# NEXT-PHASE: sort-buffer cap behavior is untestable at MTR data sizes
#   (1M rows / 256 MB); block-level coverage would need a synthetic
#   large-row generator.  The cap error message and class
#   (RonSQLPermanentError) are code-reviewed only.
