# passthrough_snowflake family — findings

Phase 2 of `non_aggregate_pushdown_plan.md` (detailed plan:
`non_aggregate_phase_2.md`): real-table snowflake pushed joins,
projection-only.  Cases sn-1..15 lock in the supported envelope;
sn-P1..P3 pin the rejections.

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| INNER below a LEFT child (nullable link) | `orders AS o LEFT JOIN customer AS cl ON cl.c_custkey = o.o_clerk JOIN nation AS n ON n.n_nationkey = cl.c_nationkey` | **probe CONFIRMED wrong results on first record** (RonSQL delivered the NULL-extended rows MySQL's INNER join eliminates — o_orderkey 7/14/21/28/35 where o_clerk IS NULL); now rejection-assert (sn-15).  Root-cause analysis (`join_nest_semantics_plan.md`): SQL is left-associative — the query means `(o LEFT cl) INNER n` ≡ all-INNER via null-rejecting promotion, while RonSQL's tree expresses `o LEFT (cl INNER n)`.  Fix = join-condition LEFT→INNER promotion (Part B), not nest metadata; re-enable as a compare then | `join_nest_semantics_plan.md` Part B (promotion pre-pass, Phase J precedent); nest metadata deferred to Part C (parenthesized nests, not in grammar) | body_passthrough_snowflake.inc |
| INNER below LEFT on the AGGREGATE join path | same shape under `COUNT(*)` / `SUM(...)` | **CONFIRMED WRONG** by `body_nest_semantics.inc` ns-1 (recorded `COUNT(*) = 30` vs MySQL's 10 — lookup-miss AND NULL-key rows both counted): silent wrong results in shipped aggregate territory when the INNER node is the aggregate leaf.  `testMultiOuterJoinAggNdbApi` Test 2 stays green because its leaf is a LEFT node *below* the INNER — the kernel NULL-injection's INNER-awareness depends on leaf position | fix: `join_nest_semantics_plan.md` Part B (LEFT→INNER promotion), now a correctness fix; ns-1..4 NEXT-PHASE-disabled pending it | findings/nest_semantics.md |
| Type-mismatch join columns | `customer AS cu JOIN region AS r ON r.r_regionkey = cu.c_custkey` (TINYINT vs INT) | rejection-assert (sn-P1) | new planner pre-check for the NDB linked-operand identity rule (no implicit conversion) — applies to aggregate joins too | body_passthrough_snowflake.inc |
| Join column with no usable index | `orders AS o JOIN lineitem AS l ON l.l_quantity = o.o_orderkey` | rejection-assert (sn-P2) | existing planner classification error | body_passthrough_snowflake.inc |
| Cross-table WHERE residual on a pass-through join | `... WHERE cu.c_acctbal > o.o_totalprice` | rejection-assert (sn-P3) | cross-table filters need the aggregation sentinel machinery; kept restriction (parent plan) | body_passthrough_snowflake.inc |
| Genuine multi-batch scan-scan at scale | `lg_cust JOIN lg_orders` (load_ronsql_large) | NEXT-PHASE probe | sn-9 (300 x 1500 rows) is the in-suite stress; forcing many SCAN_NEXTREQ rounds through RT_REPEAT_SCAN_RESULT needs the `ronsql_large` data | body_passthrough_snowflake.inc |
| BLOB/TEXT join column rejection | — | code-covered only | shared schema has no BLOB/TEXT columns; planner check untested by MTR | QueryPlanner.cpp |
