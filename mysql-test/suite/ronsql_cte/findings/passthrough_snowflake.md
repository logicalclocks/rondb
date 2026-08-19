# passthrough_snowflake family — findings

Phase 2 of `non_aggregate_pushdown_plan.md` (detailed plan:
`non_aggregate_phase_2.md`): real-table snowflake pushed joins,
projection-only.  Cases sn-1..15 lock in the supported envelope;
sn-P1..P3 pin the rejections.

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| INNER below a LEFT child (nullable link) | `orders AS o LEFT JOIN customer AS cl ON cl.c_custkey = o.o_clerk JOIN nation AS n ON n.n_nationkey = cl.c_nationkey` | **probe CONFIRMED wrong results on first record** (RonSQL delivered the NULL-extended rows MySQL's INNER join eliminates — o_orderkey 7/14/21/28/35 where o_clerk IS NULL); now rejection-assert (sn-15) via a targeted parse-gate error; NEXT-PHASE = emit nest metadata and re-enable as a compare | RonSQL emits no outer-join nest options (`setFirstInnerJoin`/`setUpperJoin`); follow `ha_ndbcluster_push.cc:2589-2625` | body_passthrough_snowflake.inc |
| INNER below LEFT on the AGGREGATE join path | same shape under `COUNT(*)` / `SUM(...)` | NEXT-PHASE probe — **suspected latent wrong results**: the aggregate path emits the same MatchTypes with no nest metadata and the parse-gate restriction only guards pass-through queries; testMultiOuterJoinAggNdbApi's mixed chains are believed to be the safe (LEFT-under-INNER) direction | same nest-metadata gap | body_passthrough_snowflake.inc |
| Type-mismatch join columns | `customer AS cu JOIN region AS r ON r.r_regionkey = cu.c_custkey` (TINYINT vs INT) | rejection-assert (sn-P1) | new planner pre-check for the NDB linked-operand identity rule (no implicit conversion) — applies to aggregate joins too | body_passthrough_snowflake.inc |
| Join column with no usable index | `orders AS o JOIN lineitem AS l ON l.l_quantity = o.o_orderkey` | rejection-assert (sn-P2) | existing planner classification error | body_passthrough_snowflake.inc |
| Cross-table WHERE residual on a pass-through join | `... WHERE cu.c_acctbal > o.o_totalprice` | rejection-assert (sn-P3) | cross-table filters need the aggregation sentinel machinery; kept restriction (parent plan) | body_passthrough_snowflake.inc |
| Genuine multi-batch scan-scan at scale | `lg_cust JOIN lg_orders` (load_ronsql_large) | NEXT-PHASE probe | sn-9 (300 x 1500 rows) is the in-suite stress; forcing many SCAN_NEXTREQ rounds through RT_REPEAT_SCAN_RESULT needs the `ronsql_large` data | body_passthrough_snowflake.inc |
| BLOB/TEXT join column rejection | — | code-covered only | shared schema has no BLOB/TEXT columns; planner check untested by MTR | QueryPlanner.cpp |
