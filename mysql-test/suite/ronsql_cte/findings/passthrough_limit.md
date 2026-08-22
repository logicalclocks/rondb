# passthrough_limit family — findings

Phase 2 of `ronsql_orderby_limit_plan.md`: LIMIT (without ORDER BY) on
every projection-only pass-through shape — E.3 CTE_SCAN roots, the
I.8/I.11/I.12 + non-aggregate-plan join chains, and the Phase-1
single-table path.  Gate relaxation (the three shape conditions drop
`!has_limit`) plus streaming cutoffs in `execute_passthrough_drain` and
`execute_single_table_passthrough`: stop fetching at the limit and close
the still-open scan early (the multi-op drain returns to the caller's
unconditional `query->close()`; the single-table scan arm calls
`scanOp->close()` itself).  LIMIT 0 keeps the deferred-TSV-header
convention — no output at all, matching the mysql client; JSON keeps its
`[` ... `]` framing (`[]` for LIMIT 0).  The PK-lookup arm treats
LIMIT 0 as "print nothing" (any LIMIT >= 1 cannot constrain a
single-row lookup further).

Prose notes:

- **Truncating LIMIT without ORDER BY is value-nondeterministic**, so
  every truncating compare projects columns that are uniform across the
  candidate rows (cf.n = 5 for all 300 groups; c_nationkey fixed by the
  WHERE).  Non-truncating cases (LIMIT 0 / LIMIT > set) project freely
  under the sorted compare.
- pl-2 (LIMIT 260 of 300 CTE rows) pins the early close landing
  mid-scan past the 256-row API batch boundary.
- st-P2 in body_passthrough_single_table.inc previously pinned "LIMIT
  on a single-table projection" as a rejection; it is now converted to
  an ORDER BY + LIMIT rejection (that family needs re-recording).

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| ORDER BY alone on a pass-through shape | `WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT k, n FROM cf ORDER BY k;` | rejection-assert (pl-P1) | Buffered client-side sort — ronsql_orderby_limit_plan.md Phase 3 | body_passthrough_limit.inc |
| ORDER BY + LIMIT on a join chain | `... SELECT cu.c_custkey, cf.n FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey ORDER BY c_custkey LIMIT 5;` | rejection-assert (pl-P2) | Same — Phase 3 (top-N needs the sort before the cut) | body_passthrough_limit.inc |
