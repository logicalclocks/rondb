# child_bounds family — findings

The child_bounds feature (`next_steps.md` join-root follow-ups items
2+3): child-local constant WHERE conjuncts on non-root INDEX_SCAN join
children emit as constant index range bounds after the linked join-key
prefix (bounds prune reads; filters only prune transfer), with
best-child-index re-selection when another join-key-prefix candidate
binds strictly more columns.  Shared by the aggregate, pass-through,
and CTE-body-child paths (`assign_child_index_bounds` runs per scope).
Cases cb-1..12 in `body_child_bounds.inc` (local cb_p/cb_c/cb_x
tables); the shared-schema pin is sr-6's EXPLAIN greps in
`body_passthrough_star.inc`.

| Shape | Case | Disposition | Notes | Location |
|---|---|---|---|---|
| Upper-only const bound tail | cb-2, sr-6 (`v_seq <= 2`) | covered — required the DBSPJ `scanFrag_fixupBound` renumbering fix (an upper bound with no preceding lower for the same column got the previous column's id) | the same bug was latently reachable via cross-table `T_LT/T_LE` bounds (RONDB-1044), untested before | DbspjMain.cpp scanFrag_fixupBound |
| Best-child-index scoring | cb-5 (idx_x_a vs idx_x_ab), cb-10 (3-col EQ-continue) | covered — strictly-better switches, ties keep the planner choice (plan stability) | `QueryPlanner::collectOrderedIndexCandidates` + score walk | assign_child_index_bounds |
| Nullable indexed column | cb-8 (`c_note >= 4` on idx_cb_c_pn) | v1 guard: stays a filter — NULL sorts below all values in NDB ordered indexes vs SQL UNKNOWN comparison | the analogous ROOT-path hole (`build_scan_config_candidates` has no nullability guard) is a recorded correctness suspicion in next_steps.md | body_child_bounds.inc |
| Mixed linked + const bounds on one op | cb-11 (cross-table EQ + const range) | covered on the aggregate path | cross-table filters on the NON-aggregate path stay rejected (sn-P3) — mixed shapes are aggregate-only until that lifts | body_child_bounds.inc |
| Subquery-leaf inner filters | — | deliberately not bound-eligible (they merge into join_where_ce after the pass) | safe-retreat placement choice | RonSQLPreparer.cpp load_join |
| Permuted multi-key join binding | — | latent wrong-results hazard FIXED while landing: index matchers accept permuted key sets but emit binds positionally; keys now normalized to physical column order (exact bijection, duplicate ON columns rejected cleanly) | QueryPlanner::plan normalization | QueryPlanner.cpp |
