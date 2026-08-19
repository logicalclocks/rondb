# passthrough_snowflake family — findings

Phase 2 of `non_aggregate_pushdown_plan.md` (detailed plan:
`non_aggregate_phase_2.md`): real-table snowflake pushed joins,
projection-only.  Cases sn-1..15 lock in the supported envelope;
sn-P1..P3 pin the rejections.

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| INNER below a LEFT child (nullable link) | `orders AS o LEFT JOIN customer AS cl ON cl.c_custkey = o.o_clerk JOIN nation AS n ON n.n_nationkey = cl.c_nationkey` | **FIXED by Part B** (`promote_left_joins()`): sn-15 re-enabled as a compare with an EXPLAIN grep pinning the promotion.  History: the Part A probe recorded wrong results (NULL-extended rows delivered); root cause was associativity — RonSQL's tree expressed `o LEFT (cl INNER n)` where SQL means `(o LEFT cl) INNER n` ≡ all-INNER | `join_nest_semantics_plan.md`; nest metadata stays deferred to Part C (parenthesized nests, not in grammar) | body_passthrough_snowflake.inc |
| INNER below LEFT on the AGGREGATE join path | same shape under `COUNT(*)` / `SUM(...)` | **CONFIRMED WRONG by Part A** (ns-1 recorded `COUNT(*) = 30` vs MySQL's 10), **FIXED by Part B** — post-promotion, flat SQL never emits an INNER-below-LEFT tree; ns-1..4 re-enabled.  The kernel NULL-injection's leaf-position-dependent INNER-awareness (Test 2 green vs ns-1 red) remains noted for future genuine-nest work | findings/nest_semantics.md | join_nest_semantics_plan.md |
| Type-mismatch join columns | `customer AS cu JOIN region AS r ON r.r_regionkey = cu.c_custkey` (TINYINT vs INT) | rejection-assert (sn-P1) | new planner pre-check for the NDB linked-operand identity rule (no implicit conversion) — applies to aggregate joins too | body_passthrough_snowflake.inc |
| Join column with no usable index | `orders AS o JOIN lineitem AS l ON l.l_quantity = o.o_orderkey` | rejection-assert (sn-P2) | existing planner classification error | body_passthrough_snowflake.inc |
| Cross-table WHERE residual on a pass-through join | `... WHERE cu.c_acctbal > o.o_totalprice` | rejection-assert (sn-P3) | cross-table filters need the aggregation sentinel machinery; kept restriction (parent plan) | body_passthrough_snowflake.inc |
| Genuine multi-batch scan-scan at scale | `lg_cust JOIN lg_orders` (load_ronsql_large) | NEXT-PHASE probe | sn-9 (300 x 1500 rows) is the in-suite stress; forcing many SCAN_NEXTREQ rounds through RT_REPEAT_SCAN_RESULT needs the `ronsql_large` data | body_passthrough_snowflake.inc |
| BLOB/TEXT join column rejection | — | code-covered only | shared schema has no BLOB/TEXT columns; planner check untested by MTR | QueryPlanner.cpp |
