# passthrough_single_table family — findings

Phase 1 of `non_aggregate_pushdown_plan.md` (detailed plan:
`non_aggregate_phase_1.md`): single-table non-aggregate queries on the
plain NDB API path.  Cases st-1..14 lock in the supported envelope;
st-P1..P4 pin the rejections.

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| ORDER BY / LIMIT on a single-table projection | `SELECT o_orderkey FROM orders WHERE o_custkey = 7 ORDER BY o_orderkey;` | rejection-assert (st-P1/P2) | `ronsql_orderby_limit_plan.md` Phases 2-4 (streaming LIMIT, buffered sort, SF_OrderBy index-order top-N) | body_passthrough_single_table.inc |
| Expression in a projection-only SELECT list | `SELECT o_orderkey + 1 FROM orders WHERE o_orderkey = 77;` | rejection-assert (st-P3) | projection expressions are out of the non-aggregate envelope (parent plan, cross-cutting) | body_passthrough_single_table.inc |
| WITH prefix on a single-table projection | `WITH cf AS (...) SELECT o_orderkey FROM orders WHERE o_orderkey = 77;` | rejection-assert (st-P4) | shape C requires `cte_list == NULL` — a defined-but-unused CTE is rejected, not ignored | body_passthrough_single_table.inc |
| PK equality + residual conjunct executes as a scan | `SELECT o_orderkey FROM orders WHERE o_orderkey = 77 AND o_shippriority >= 0;` | covered (st-4), correct-but-blunt | v1 policy: the RecAttr-style NdbOperation read has no interpreted-code facility (`OO_INTERPRETED` is NdbRecord-only); NdbRecord interpreted read is the named follow-up in `non_aggregate_phase_1.md` | body_passthrough_single_table.inc |
| WHERE IN-subquery on a single-table projection | `SELECT o_orderkey FROM orders WHERE o_custkey IN (SELECT ...);` | NEXT-PHASE-disabled (untested probe) | subquery substitution machinery predates shape C; probe before relying on it | body_passthrough_single_table.inc |
