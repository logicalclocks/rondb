# passthrough_single_table family — findings

Phase 1 of `non_aggregate_pushdown_plan.md` (detailed plan:
`non_aggregate_phase_1.md`): single-table non-aggregate queries on the
plain NDB API path.  Cases st-1..22 lock in the supported envelope;
st-P3/P4 pin the rejections.

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| ORDER BY / LIMIT on a single-table projection | `SELECT o_orderkey FROM orders WHERE o_custkey = 7 ORDER BY o_orderkey;` | rejection-assert (st-P1/P2) | `ronsql_orderby_limit_plan.md` Phases 2-4 (streaming LIMIT, buffered sort, SF_OrderBy index-order top-N) | body_passthrough_single_table.inc |
| Expression in a projection-only SELECT list | `SELECT o_orderkey + 1 FROM orders WHERE o_orderkey = 77;` | rejection-assert (st-P3) | projection expressions are out of the non-aggregate envelope (parent plan, cross-cutting) | body_passthrough_single_table.inc |
| WITH prefix on a single-table projection | `WITH cf AS (...) SELECT o_orderkey FROM orders WHERE o_orderkey = 77;` | rejection-assert (st-P4) | shape C requires `cte_list == NULL` — a defined-but-unused CTE is rejected, not ignored | body_passthrough_single_table.inc |
| PK equality + residual conjunct | `SELECT o_orderkey FROM orders WHERE o_orderkey = 77 AND o_shippriority >= 0;` | **SHIPPED** (st-4/5/15..18, st-22): NdbRecord `readTuple` + `OO_INTERPRETED` filter + `OO_GETVALUE` RecAttr reads (`non_aggregate_pk_residual_lookup.md`); scan fallback retained for over-cap (st-19) and unsupported-type (st-20) residuals | interpreted-reject surfaces as 626 = NoDataFound == row-absent, by design | body_passthrough_single_table.inc |
| VARCHAR-PK lookup key encoding | `SELECT v_code FROM vkey1 WHERE v_code = 'beta' [AND residual];` | covered (st-21 RecAttr arm, st-22 NdbRecord key_row length-prefix encoding — the one genuinely new byte-level path) | local `vkey1` table (shared schema has only integer PKs) | body_passthrough_single_table.inc |
| WHERE IN-subquery on a single-table projection | `SELECT o_orderkey FROM orders WHERE o_custkey IN (SELECT ...);` | NEXT-PHASE-disabled (untested probe) | subquery substitution machinery predates shape C; probe before relying on it | body_passthrough_single_table.inc |
