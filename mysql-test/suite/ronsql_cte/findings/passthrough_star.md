# passthrough_star family — findings

Phase 3 of `non_aggregate_pushdown_plan.md` (detailed plan:
`non_aggregate_phase_3.md`): star-schema fan-out, projection-only.
Cases sr-1..10 lock in the supported envelope; sr-P1 pins the new
alias-uniqueness rejection.

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| Duplicate table alias | `orders AS o JOIN customer AS cu ON ... JOIN customer AS cu ON ...` | rejection-assert (sr-P1) | new QueryPlanner alias-uniqueness check (MySQL ER_NONUNIQ_TABLE parity) — previously bound silently to the first match, applies to aggregate joins too | body_passthrough_star.inc |
| Genuine multi-batch bushy verification at scale | `lg_*` star via load_ronsql_large | NEXT-PHASE probe | sr-7 (480-row cross product) is the in-suite stress; many-SCAN_NEXTREQ verification of the sibling-scan repeat protocol needs the `ronsql_large` data (shared follow-up with Phase 2's scan-scan item) | body_passthrough_star.inc |
| Constant bounds on scan branches | `... JOIN star_m AS m ON m.m_eid = e.e_id WHERE m.m_seq >= 2` | **SHIPPED** (child_bounds feature): sr-6's branch predicates now emit as constant index bounds after the linked prefix (`m_seq >= 2` low, `v_seq <= 2` upper-only — the scanFrag_fixupBound fix case), pinned by fatal EXPLAIN `Bounds:` greps; full family in `body_child_bounds.inc` (`findings/child_bounds.md`) | next_steps.md items 2+3 done | body_passthrough_star.inc |
| Aggregate star fan-out | multi-leaf aggregation | out of scope here | RONDB-1044 (`star_schema_plan.md`) owns it | — |
