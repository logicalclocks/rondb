# body_passthrough_single_table.inc — single-table non-aggregate
# (projection-only) query family (non_aggregate_phase_1.md), data-driven
# suite.
#
# Phase 1 accepts the first queries with no aggregation and no CTE:
# projection-only single-table SELECTs.  Execution is the plain NDB API
# path (no NdbQueryBuilder / SPJ hop): a single-row PK lookup when the
# WHERE is fully consumed as PK equalities (v1 policy — any residual
# falls back to the scan-config path, since the RecAttr-style
# NdbOperation read has no interpreted-code facility), else the shared
# table/index scan setup with bounds + residual filter and a streaming
# drain through the pass-through printer.
#
# Case map (st-N):
#   1   PK hit, mixed-type projection (INT, DATE, DECIMAL, CHAR)
#   2   PK miss -> empty output (deferred TSV header)
#   3   composite PK full equality, predicates in reversed PK order
#   4   PK equality + accepting residual -> scan fallback (v1 policy)
#   5   PK equality + rejecting residual -> empty
#   6   partial composite PK -> scan path
#   7   secondary-index equality
#   8   half-open DATE range + residual filter (QUERY_FILE)
#   9   double-bounded DATE range (QUERY_FILE)
#   10  composite index prefix: status equality + date bound (QUERY_FILE)
#   11  residual-only WHERE, no usable index -> table scan
#   12  no WHERE -> full table scan (nation, 25 rows)
#   13  temporal projection sweep (evlog)
#   14  NULL values in a projected column (o_clerk)
#   (P1/P2 ORDER-BY-rejection probes retired — ORDER BY / LIMIT shipped
#    via ronsql_orderby_limit_plan.md Phases 2+3; see
#    body_passthrough_limit.inc / body_passthrough_orderby.inc)
#   P3  rejection: expression in the projection
#   P4  rejection: WITH prefix on a single-table projection
#
# NEXT-PHASE: WHERE IN-subquery on a single-table projection
#   (SELECT o_orderkey FROM orders WHERE o_custkey IN (SELECT ...)) —
#   the subquery substitution machinery predates shape C and is
#   untested in a non-aggregate outer query; probe before relying on it.

--source include/have_ndb.inc
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings
--let $suppress_ronsql_cli=yes
--let $strict_diff=yes
--source suite/ronsql_cte/include/cte_schema.inc
--source suite/ronsql_cte/include/cte_data.inc

DELIMITER |;

--echo
--echo === st-1: PK hit, mixed-type projection ===
let $QUERY=
SELECT o_orderkey, o_orderdate, o_totalprice, o_orderstatus
FROM orders WHERE o_orderkey = 77;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_1.out
let $QUERY=
SELECT o_orderkey, o_orderdate, o_totalprice, o_orderstatus
FROM orders WHERE o_orderkey = 77;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as primary key lookup.' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_orderdate, o_totalprice, o_orderstatus
FROM orders WHERE o_orderkey = 77|

--echo
--echo === st-2: PK miss, empty output ===
let $QUERY=
SELECT o_orderkey, o_orderdate FROM orders WHERE o_orderkey = 999999;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_orderdate FROM orders WHERE o_orderkey = 999999|

--echo
--echo === st-3: composite PK, predicates in reversed PK order ===
let $QUERY=
SELECT l_orderkey, l_linenumber, l_quantity, l_extendedprice
FROM lineitem WHERE l_linenumber = 2 AND l_orderkey = 10;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT l_orderkey, l_linenumber, l_quantity, l_extendedprice
FROM lineitem WHERE l_linenumber = 2 AND l_orderkey = 10|

--echo
--echo === st-4: PK equality + accepting residual (scan fallback) ===
let $QUERY=
SELECT o_orderkey, o_custkey FROM orders
WHERE o_orderkey = 77 AND o_shippriority >= 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_custkey FROM orders
WHERE o_orderkey = 77 AND o_shippriority >= 0|

--echo
--echo === st-5: PK equality + rejecting residual -> empty ===
let $QUERY=
SELECT o_orderkey, o_custkey FROM orders
WHERE o_orderkey = 77 AND o_custkey < 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_custkey FROM orders
WHERE o_orderkey = 77 AND o_custkey < 0|

--echo
--echo === st-6: partial composite PK -> scan path ===
let $QUERY=
SELECT l_orderkey, l_linenumber, l_quantity
FROM lineitem WHERE l_orderkey = 10;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT l_orderkey, l_linenumber, l_quantity
FROM lineitem WHERE l_orderkey = 10|

--echo
--echo === st-7: secondary-index equality ===
let $QUERY=
SELECT o_orderkey, o_custkey FROM orders WHERE o_custkey = 7;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_custkey FROM orders WHERE o_custkey = 7|

--echo
--echo === st-8: half-open DATE range + residual filter ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_st8.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate, o_orderstatus
FROM orders
WHERE o_orderdate >= '1997-06-01' AND o_shippriority >= 0;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_8.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_st8b.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate, o_orderstatus
FROM orders
WHERE o_orderdate >= '1997-06-01' AND o_shippriority >= 0;
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as index scan.' $EXPLAIN_FILE || true
--exec grep -qF 'FILTER: ' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_orderdate, o_orderstatus
FROM orders
WHERE o_orderdate >= '1997-06-01' AND o_shippriority >= 0|

--echo
--echo === st-9: double-bounded DATE range ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_st9.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate
FROM orders
WHERE o_orderdate >= '1997-06-01' AND o_orderdate < '1997-08-01';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_orderdate
FROM orders
WHERE o_orderdate >= '1997-06-01' AND o_orderdate < '1997-08-01'|

--echo
--echo === st-10: composite index prefix (status equality + date bound) ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_st10.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderstatus, o_orderdate
FROM orders
WHERE o_orderstatus = 'O' AND o_orderdate >= '1997-06-01';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_orderstatus, o_orderdate
FROM orders
WHERE o_orderstatus = 'O' AND o_orderdate >= '1997-06-01'|

--echo
--echo === st-11: residual-only WHERE -> table scan ===
let $QUERY=
SELECT o_orderkey, o_totalprice FROM orders
WHERE o_orderkey < 15 AND o_shippriority >= 0;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_11.out
let $QUERY=
SELECT o_orderkey, o_totalprice FROM orders
WHERE o_orderkey < 15 AND o_shippriority >= 0;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as table scan.' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_totalprice FROM orders
WHERE o_orderkey < 15 AND o_shippriority >= 0|

--echo
--echo === st-12: no WHERE -> full table scan ===
let $QUERY=
SELECT n_nationkey, n_name, n_regionkey FROM nation;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT n_nationkey, n_name, n_regionkey FROM nation|

--echo
--echo === st-13: temporal projection sweep ===
let $QUERY=
SELECT e_id, e_year, e_logged, e_split, e_stamp6
FROM evlog WHERE e_id < 10;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT e_id, e_year, e_logged, e_split, e_stamp6
FROM evlog WHERE e_id < 10|

--echo
--echo === st-14: NULL values in a projected column ===
let $QUERY=
SELECT o_orderkey, o_clerk FROM orders WHERE o_orderkey < 22;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_clerk FROM orders WHERE o_orderkey < 22|

DELIMITER ;|

# st-P1/P2 (ORDER BY / ORDER BY + LIMIT rejection probes) were retired
# when ronsql_orderby_limit_plan.md Phases 2+3 shipped LIMIT streaming
# and the buffered client-side sort; positive coverage lives in
# body_passthrough_limit.inc and body_passthrough_orderby.inc.

--echo
--echo === st-P3 (rejection): expression in the projection ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'SELECT o_orderkey + 1 FROM orders WHERE o_orderkey = 77;' 2>&1

--echo
--echo === st-P4 (rejection): WITH prefix on a single-table projection ===
# A CTE definition on a shape-C query stays rejected rather than
# silently ignoring the CTE.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT o_orderkey FROM orders WHERE o_orderkey = 77;' 2>&1

--source suite/ronsql_cte/include/cte_drop.inc
