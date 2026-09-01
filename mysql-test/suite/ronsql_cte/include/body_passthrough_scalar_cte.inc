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
# Phase i26 (cte_filter_phase_i26.md) SHIPPED the former sc-P1
# deferral: real-table vs scalar-CTE comparisons (`t.col > s.m`, the
# watermark shape) now route to the CTE_LOOKUP's jump-table filter
# with the real column riding the linked-attr buffer as a linked
# parent projection (DBSPJ expands per parent row; DBLQH prepends
# ahead of the CTE outputs; typed-register comparisons via
# READ_LINKED_COLUMN_TO_REG).  Works on BOTH paths; sc-11..18 cover
# it, sc-P1/P4 pin what stays rejected.
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
#   11 the watermark shape (the exact former sc-P1 probe, seeded so
#      rows pass)
#   12a/b/c operator sweep on the watermark shape (<=, =, !=)
#   13 NULL watermark -> UNKNOWN rejects every row (empty result)
#   14 signed INT vs unsigned COUNT (typed-register compare)
#   15 mixed filter program: ancestor atom + CTE-vs-const atom
#   16 aggregate twin of 11 (COUNT over the filtered cross join)
#   17 GROUP BY parent col + ancestor atom + CTE-vs-const atom — pins
#      the linked-base offset (agg linked projections prepend ahead of
#      the CTE outputs; positions must agree three ways)
#   18 snowflake chain + watermark on the root column
#   19 DATE watermark: MIN(o_orderdate) scalar (Bigunsigned widening)
#      vs a real DATE ancestor column (the READ_LINKED_COLUMN_TO_REG
#      DATE arm)
#   P1 rejection: non-ancestor (sibling-branch) real column vs scalar
#      output
#   P2 rejection: comma cross-join of real tables
#   P3 rejection: grouped CTE comma-joined (Partial coverage)
#   P4 rejection: DECIMAL operand in a real-vs-CTE comparison

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

--echo
--echo === sc-11: the watermark shape (former sc-P1, seeded) ===
# s.m = MAX(o_orderkey WHERE < 5) = 4; customers 5..300 pass.
let $QUERY=
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders WHERE o_orderkey < 5)
SELECT cu.c_custkey
FROM customer AS cu, s
WHERE cu.c_custkey > s.m;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders WHERE o_orderkey < 5)
SELECT cu.c_custkey
FROM customer AS cu, s
WHERE cu.c_custkey > s.m|

--echo
--echo === sc-12a: watermark operator sweep, <= ===
let $QUERY=
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders WHERE o_orderkey < 5)
SELECT cu.c_custkey
FROM customer AS cu, s
WHERE cu.c_custkey <= s.m;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders WHERE o_orderkey < 5)
SELECT cu.c_custkey
FROM customer AS cu, s
WHERE cu.c_custkey <= s.m|

--echo
--echo === sc-12b: watermark operator sweep, = ===
let $QUERY=
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders WHERE o_orderkey < 5)
SELECT cu.c_custkey
FROM customer AS cu, s
WHERE cu.c_custkey = s.m;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders WHERE o_orderkey < 5)
SELECT cu.c_custkey
FROM customer AS cu, s
WHERE cu.c_custkey = s.m|

--echo
--echo === sc-12c: watermark operator sweep, != ===
let $QUERY=
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders WHERE o_orderkey < 5)
SELECT cu.c_custkey
FROM customer AS cu, s
WHERE cu.c_custkey != s.m AND cu.c_custkey < 8;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders WHERE o_orderkey < 5)
SELECT cu.c_custkey
FROM customer AS cu, s
WHERE cu.c_custkey != s.m AND cu.c_custkey < 8|

--echo
--echo === sc-13: NULL watermark -> empty result ===
# MAX over an empty subset is NULL: every comparison is UNKNOWN and
# rejects its row (the NULL guard branches before the typed loads).
let $QUERY=
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders
           WHERE o_orderkey > 999999)
SELECT cu.c_custkey
FROM customer AS cu, s
WHERE cu.c_custkey > s.m;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders
           WHERE o_orderkey > 999999)
SELECT cu.c_custkey
FROM customer AS cu, s
WHERE cu.c_custkey > s.m|

--echo
--echo === sc-14: signed INT vs unsigned COUNT ===
# COUNT is Bigunsigned in the virt table; cu.c_custkey is signed INT —
# the typed registers compare across the signedness mix.
let $QUERY=
WITH s AS (SELECT COUNT(*) AS n FROM orders WHERE o_custkey = 7)
SELECT cu.c_custkey
FROM customer AS cu, s
WHERE cu.c_custkey < s.n;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT COUNT(*) AS n FROM orders WHERE o_custkey = 7)
SELECT cu.c_custkey
FROM customer AS cu, s
WHERE cu.c_custkey < s.n|

--echo
--echo === sc-15: mixed program — ancestor atom + CTE-vs-const atom ===
let $QUERY=
WITH s AS (SELECT MAX(o_orderkey) AS m, COUNT(*) AS n FROM orders
           WHERE o_orderkey < 5)
SELECT cu.c_custkey, s.n
FROM customer AS cu, s
WHERE cu.c_custkey > s.m AND s.n > 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MAX(o_orderkey) AS m, COUNT(*) AS n FROM orders
           WHERE o_orderkey < 5)
SELECT cu.c_custkey, s.n
FROM customer AS cu, s
WHERE cu.c_custkey > s.m AND s.n > 0|

--echo
--echo === sc-16: aggregate twin of sc-11 ===
let $QUERY=
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders WHERE o_orderkey < 5)
SELECT COUNT(*) AS c
FROM customer AS cu, s
WHERE cu.c_custkey > s.m;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders WHERE o_orderkey < 5)
SELECT COUNT(*) AS c
FROM customer AS cu, s
WHERE cu.c_custkey > s.m|

--echo
--echo === sc-17: GROUP BY parent col + mixed atoms (base-offset pin) ===
# The GROUP BY linked projection (c_mktsegment) and the filter's
# ancestor projection (c_custkey) prepend ahead of the CTE outputs:
# the aggregation program, the filter emission, and DBSPJ's expansion
# must agree on the offset — this is the pin for the (previously
# latent) base-offset bug.
let $QUERY=
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders WHERE o_orderkey < 5)
SELECT cu.c_mktsegment, COUNT(*) AS c
FROM customer AS cu, s
WHERE cu.c_custkey > s.m AND s.m > 0
GROUP BY cu.c_mktsegment;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders WHERE o_orderkey < 5)
SELECT cu.c_mktsegment, COUNT(*) AS c
FROM customer AS cu, s
WHERE cu.c_custkey > s.m AND s.m > 0
GROUP BY cu.c_mktsegment|

--echo
--echo === sc-18: snowflake chain + watermark on the root column ===
let $QUERY=
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders WHERE o_orderkey < 5)
SELECT o.o_orderkey, cu.c_mktsegment
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey, s
WHERE o.o_orderkey > s.m AND o.o_orderkey < 20;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders WHERE o_orderkey < 5)
SELECT o.o_orderkey, cu.c_mktsegment
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey, s
WHERE o.o_orderkey > s.m AND o.o_orderkey < 20|

--echo
--echo === sc-19: DATE watermark (MIN(date) scalar vs real DATE) ===
# The mixed-type pairing: the CTE side is the Bigunsigned widening of
# MIN(o_orderdate) (D17 — same uint3korr packed encoding), the real
# ancestor side is Date via the new READ_LINKED_COLUMN_TO_REG DATE
# arm; compareTypedRegs orders them exactly.
let $QUERY=
WITH s AS (SELECT MIN(o_orderdate) AS d FROM orders
           WHERE o_custkey = 5)
SELECT o2.o_orderkey
FROM orders AS o2, s
WHERE o2.o_orderdate < s.d;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT MIN(o_orderdate) AS d FROM orders
           WHERE o_custkey = 5)
SELECT o2.o_orderkey
FROM orders AS o2, s
WHERE o2.o_orderdate < s.d|

DELIMITER ;|

--echo
--echo === sc-P1 (rejection): non-ancestor real column vs scalar output ===
# cu is a sibling branch of s (both children of o): the routing
# requires the real column's op to be a tree ancestor of the CTE op,
# so this stays on the cross-table rejection path.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders) SELECT o.o_orderkey FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey, s WHERE cu.c_custkey > s.m;' 2>&1

--echo
--echo === sc-P2 (rejection): comma cross-join of real tables ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'SELECT cu.c_custkey, o.o_orderkey FROM customer AS cu, orders AS o;' 2>&1

--echo
--echo === sc-P3 (rejection): grouped CTE comma-joined ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT cu.c_custkey, g.n FROM customer AS cu, g;' 2>&1

--echo
--echo === sc-P4 (rejection): DECIMAL operand in real-vs-CTE compare ===
# The typed-register envelope is integers + FLOAT + DOUBLE; a DECIMAL
# real column is routed but rejected at filter emit with a clear
# message.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH s AS (SELECT MAX(o_orderkey) AS m FROM orders) SELECT cu.c_custkey FROM customer AS cu, s WHERE cu.c_acctbal > s.m;' 2>&1

--source suite/ronsql_cte/include/cte_drop.inc
