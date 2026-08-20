# body_passthrough_scalar_cte.inc — scalar aggregate CTEs in
# projection-only queries (non_aggregate_phase_4.md), data-driven
# suite.
#
# Phase 4 accepts comma cross-joins of SCALAR (single-row, no GROUP BY)
# aggregating CTEs into pass-through queries.  A scalar CTE always
# produces exactly one row (the kernel emits COUNT->0 / MAX->NULL even
# over empty input), so the cross join is a semantics-preserving 1-row
# multiplier — matching MySQL.  The gate accepts the grammar's
# conditionless comma production only for scalar CTE operands
# (ScalarDummy coverage); emit, drain and printer needed no changes,
# plus one latent fix: the ScalarDummy setParent now uses
# tree_parent_op_idx so 3+ sibling scalar CTEs keep their SPJ chain
# (sc-8 is the first-ever 3-CTE probe, on either path).
#
# Deferred (sc-P1 pins it): real-table vs scalar-CTE comparisons
# (`t.col > s.m`) — rejected as cross-table on BOTH paths today; a new
# feature, not a gate change (see non_aggregate_phase_4.md).
#
# Case map (sc-N):
#   1  real root + scalar CTE (customer, s) — TABLE_SCAN root +
#      ScalarDummy child through the pass-through drain
#   2  two scalar CTEs, projection-only (the cs-probe-7 shape,
#      previously "Not an aggregate query")
#   3  scalar CTE appended to a snowflake chain
#   4  scalar CTE appended to a self-join star
#   5  WHERE on the scalar output — accepting + rejecting (the reject
#      empties the whole cross join)
#   6  NULL scalar (MAX over an empty subset) printed on every row
#   7  COUNT(*) over an empty subset prints 0
#   8  three scalar CTEs (FROM a, b, c) — the setParent chain fix
#   9  multi-output scalar CTE: temporal + DECIMAL outputs through the
#      pass-through metadata decode
#   10 AGGREGATE main + empty scalar CTE child — the aggregate-path
#      twin of the sc-6 kernel fix (COUNT silently returned 0 before)
#   P1 rejection: real column vs scalar output (deferred feature)
#   P2 rejection: comma cross-join of real tables
#   P3 rejection: grouped CTE comma-joined (Partial coverage)

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
--echo === sc-1: real root + scalar CTE cross-join ===
let $QUERY=
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders)
SELECT cu.c_custkey, s.m
FROM customer AS cu, s
WHERE cu.c_custkey < 6;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders)
SELECT cu.c_custkey, s.m
FROM customer AS cu, s
WHERE cu.c_custkey < 6|

--echo
--echo === sc-2: two scalar CTEs, projection-only ===
let $QUERY=
WITH a AS (SELECT MAX(o_orderkey) AS m FROM orders),
     b AS (SELECT COUNT(*) AS n FROM nation)
SELECT a.m, b.n FROM a, b;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (SELECT MAX(o_orderkey) AS m FROM orders),
     b AS (SELECT COUNT(*) AS n FROM nation)
SELECT a.m, b.n FROM a, b|

--echo
--echo === sc-3: scalar CTE appended to a snowflake chain ===
let $QUERY=
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders)
SELECT o.o_orderkey, cu.c_mktsegment, s.m
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey, s
WHERE o.o_orderkey < 8;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders)
SELECT o.o_orderkey, cu.c_mktsegment, s.m
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey, s
WHERE o.o_orderkey < 8|

--echo
--echo === sc-4: scalar CTE appended to a self-join star ===
let $QUERY=
WITH s AS (SELECT COUNT(*) AS n FROM region)
SELECT o.o_orderkey, cu.c_mktsegment AS m1, c2.c_mktsegment AS m2, s.n
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN customer AS c2 ON c2.c_custkey = o.o_clerk, s
WHERE o.o_orderkey < 30;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT COUNT(*) AS n FROM region)
SELECT o.o_orderkey, cu.c_mktsegment AS m1, c2.c_mktsegment AS m2, s.n
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN customer AS c2 ON c2.c_custkey = o.o_clerk, s
WHERE o.o_orderkey < 30|

--echo
--echo === sc-5: WHERE on the scalar output, accepting ===
let $QUERY=
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders)
SELECT cu.c_custkey, s.m
FROM customer AS cu, s
WHERE cu.c_custkey < 6 AND s.m > 100;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders)
SELECT cu.c_custkey, s.m
FROM customer AS cu, s
WHERE cu.c_custkey < 6 AND s.m > 100|

--echo
--echo === sc-5b: WHERE on the scalar output, rejecting -> empty ===
let $QUERY=
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders)
SELECT cu.c_custkey, s.m
FROM customer AS cu, s
WHERE cu.c_custkey < 6 AND s.m > 100000;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders)
SELECT cu.c_custkey, s.m
FROM customer AS cu, s
WHERE cu.c_custkey < 6 AND s.m > 100000|

--echo
--echo === sc-6: NULL scalar printed on every row ===
let $QUERY=
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders
           WHERE o_orderkey > 999999)
SELECT cu.c_custkey, s.m
FROM customer AS cu, s
WHERE cu.c_custkey < 4;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders
           WHERE o_orderkey > 999999)
SELECT cu.c_custkey, s.m
FROM customer AS cu, s
WHERE cu.c_custkey < 4|

--echo
--echo === sc-7: COUNT over an empty subset prints 0 ===
let $QUERY=
WITH s AS (SELECT COUNT(*) AS n FROM orders WHERE o_orderkey > 999999)
SELECT cu.c_custkey, s.n
FROM customer AS cu, s
WHERE cu.c_custkey < 4;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT COUNT(*) AS n FROM orders WHERE o_orderkey > 999999)
SELECT cu.c_custkey, s.n
FROM customer AS cu, s
WHERE cu.c_custkey < 4|

--echo
--echo === sc-8: three scalar CTEs — the setParent chain fix ===
let $QUERY=
WITH a AS (SELECT MAX(o_orderkey) AS m FROM orders),
     b AS (SELECT COUNT(*) AS n FROM nation),
     c AS (SELECT MIN(r_regionkey) AS r FROM region)
SELECT a.m, b.n, c.r FROM a, b, c;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (SELECT MAX(o_orderkey) AS m FROM orders),
     b AS (SELECT COUNT(*) AS n FROM nation),
     c AS (SELECT MIN(r_regionkey) AS r FROM region)
SELECT a.m, b.n, c.r FROM a, b, c|

--echo
--echo === sc-9: multi-output scalar CTE with temporal + DECIMAL ===
let $QUERY=
WITH s AS (SELECT MIN(o_orderdate) AS d, MAX(o_totalprice) AS p
           FROM orders)
SELECT cu.c_custkey, s.d, s.p
FROM customer AS cu, s
WHERE cu.c_custkey < 4;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MIN(o_orderdate) AS d, MAX(o_totalprice) AS p
           FROM orders)
SELECT cu.c_custkey, s.d, s.p
FROM customer AS cu, s
WHERE cu.c_custkey < 4|

--echo
--echo === sc-10: AGGREGATE main + empty scalar CTE child ===
# The aggregate-path twin of sc-6: before the kernel fix (DblqhMain
# scalar CTE_LOOKUP arm REFed GROUP_NOT_FOUND on processed_rows()==0),
# the empty scalar CTE dropped every cross-join parent row and this
# COUNT silently returned 0 instead of 3.
let $QUERY=
WITH s AS (SELECT COUNT(*) AS n FROM orders WHERE o_orderkey > 999999)
SELECT COUNT(*) AS c
FROM customer AS cu, s
WHERE cu.c_custkey < 4;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT COUNT(*) AS n FROM orders WHERE o_orderkey > 999999)
SELECT COUNT(*) AS c
FROM customer AS cu, s
WHERE cu.c_custkey < 4|

DELIMITER ;|

--echo
--echo === sc-P1 (rejection): real column vs scalar output ===
# Deferred feature: rejected as a cross-table condition on BOTH paths
# (even the aggregate embedded-filter machinery requires stored-table
# columns).  See non_aggregate_phase_4.md.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders) SELECT cu.c_custkey FROM customer AS cu, s WHERE cu.c_custkey > s.m;' 2>&1

--echo
--echo === sc-P2 (rejection): comma cross-join of real tables ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'SELECT cu.c_custkey, o.o_orderkey FROM customer AS cu, orders AS o;' 2>&1

--echo
--echo === sc-P3 (rejection): grouped CTE comma-joined ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT cu.c_custkey, g.n FROM customer AS cu, g;' 2>&1

--source suite/ronsql_cte/include/cte_drop.inc
