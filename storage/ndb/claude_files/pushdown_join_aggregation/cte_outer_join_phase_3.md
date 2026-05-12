# Phase 3 — CTE_SCAN as outer-join child — DROPPED

## Decision

Phase 3 as originally planned (implement `cte_scan_parent_row` plus
per-parent state, match tracking, completion sweep, NULL-row
synthesis) has been dropped from this branch.

## Rationale

The SQL shape that would benefit from this — a real table LEFT JOIN
with an unkeyed `scanCte` child — translates to a cross-join
(for each parent row, scan every CTE group). This is uncommon in
practice; real CTE-joining SQL almost always uses key-based lookup:

| SQL shape | DBSPJ shape | Status |
|---|---|---|
| `FROM t LEFT JOIN cte ON t.x = cte.y` | scanTable + lookupCte | Phase 1 ✅ |
| `FROM cte LEFT JOIN t ON cte.y = t.x` | scanCte + readTuple    | Phase 2 ✅ |
| `FROM t LEFT JOIN (SELECT * FROM cte)` (cross) | scanTable + scanCte child | **dropped** |

The implementation cost is larger than Phases 1 and 2 combined —
new `parent_row`/`parent_batch_complete` handlers, per-parent
scan-state tracking in `CteScanData`, match-bit propagation through
`T_BUFFER_MATCH`, and a completion-time sweep to fire NULL rows for
unmatched parents. Shipping that for a SQL shape that isn't produced
by normal query rewrites is hard to justify.

If a future use case surfaces, the design sketch that was here
(earlier version in git history, or reconstructable from
`g_CteLookupOpInfo`'s outer-join patterns) can be revived.

## Alternative scope shipped

- Phase 1: outer-join `lookupCte` with main-SELECT API-direct path.
- Phase 2: `scanCte` as the LEFT-side parent of an outer join.
- Phase 4: consolidated test coverage of those shapes.

The CTE-subtree **agg-feed** NULL-injection case (Phase 1's
"deferred" bullet — a LEFT JOIN inside a CTE subtree that feeds into
an enclosing CTE's aggregator) is also tracked as future work — it
requires match-bit tracking in `execCTE_LOOKUP_CONF` and a
completion-time call into `handleAggAncestorComplete`. Not blocked
by Phase 3 specifically, but in the same class of "rarely-used shape
with substantial implementation cost".
