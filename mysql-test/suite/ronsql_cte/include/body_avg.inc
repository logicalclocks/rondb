# body_avg.inc — AVG-in-CTE family, data-driven CTE suite
# (cte_avg_plan.md V6).
#
# FOCUS: AVG(col) as a CTE output on the pushed path — per-row
# SUM + hidden COUNT slots, merge + redistribute, the owner-side
# finalize divide at CTE_READY (DOUBLE; count==0 => NULL), and the
# RonSQL surface on top: fixed-scale display (exact-type args show
# scale+4 like MySQL), the CTE_LOOKUP filter on the finalized DOUBLE,
# watermark compares, re-aggregation, NULL semantics, and the
# rejection envelope (string / temporal / expression arguments).
#
# Display-parity note: AVG values are computed in double where MySQL
# uses exact decimal arithmetic for exact-type args; test data stays in
# value domains where %.*f rounding agrees (the accepted v1 deviation —
# see the plan doc's display-parity section).
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
--echo
--echo === avg-01: AVG(int) CTE output, pass-through display (scale-4) ===
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, AVG(o_shippriority) AS av
  FROM orders GROUP BY o_custkey)
SELECT a.k, a.av FROM a WHERE a.k < 30;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, AVG(o_shippriority) AS av
  FROM orders GROUP BY o_custkey)
SELECT a.k, a.av FROM a WHERE a.k < 30|

--echo === avg-02: AVG output in the main WHERE (CTE_LOOKUP filter on the finalized DOUBLE) ===
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, AVG(o_shippriority) AS av
  FROM orders GROUP BY o_custkey)
SELECT a.k, COUNT(*)
FROM customer AS c JOIN a ON a.k = c.c_custkey
WHERE a.av > 1.5
GROUP BY a.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, AVG(o_shippriority) AS av
  FROM orders GROUP BY o_custkey)
SELECT a.k, COUNT(*)
FROM customer AS c JOIN a ON a.k = c.c_custkey
WHERE a.av > 1.5
GROUP BY a.k|

--echo
--echo
--echo === avg-03: AVG(DECIMAL(12,2)), pass-through display (scale-6) ===
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, AVG(o_totalprice) AS av
  FROM orders GROUP BY o_custkey)
SELECT a.k, a.av FROM a WHERE a.k < 20;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, AVG(o_totalprice) AS av
  FROM orders GROUP BY o_custkey)
SELECT a.k, a.av FROM a WHERE a.k < 20|

--echo === avg-04: AVG(DOUBLE) in a filter compare (value-domain-safe consumption) ===
let $QUERY=
WITH a AS (
  SELECT l_orderkey AS k, AVG(l_tax) AS av
  FROM lineitem GROUP BY l_orderkey)
SELECT a.k, COUNT(*)
FROM orders AS o JOIN a ON a.k = o.o_orderkey
WHERE a.av > 0.045
GROUP BY a.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT l_orderkey AS k, AVG(l_tax) AS av
  FROM lineitem GROUP BY l_orderkey)
SELECT a.k, COUNT(*)
FROM orders AS o JOIN a ON a.k = o.o_orderkey
WHERE a.av > 0.045
GROUP BY a.k|

--echo
--echo
--echo === avg-05: AVG over a NULLABLE column, pass-through (NULLs excluded from count) ===
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, AVG(o_clerk) AS av, COUNT(o_clerk) AS n
  FROM orders GROUP BY o_custkey)
SELECT a.k, a.av, a.n FROM a WHERE a.k < 30;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, AVG(o_clerk) AS av, COUNT(o_clerk) AS n
  FROM orders GROUP BY o_custkey)
SELECT a.k, a.av, a.n FROM a WHERE a.k < 30|

--echo === avg-06: scalar AVG over ALL-NULL input is NULL ===
let $QUERY=
WITH s AS (
  SELECT AVG(o_clerk) AS a FROM orders WHERE o_clerk IS NULL)
SELECT s.a FROM s;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (
  SELECT AVG(o_clerk) AS a FROM orders WHERE o_clerk IS NULL)
SELECT s.a FROM s|

--echo
--echo === avg-07: scalar AVG watermark (real INT vs CTE DOUBLE, comma join) ===
let $QUERY=
WITH s AS (SELECT AVG(o_orderkey) AS a FROM orders)
SELECT o2.o_orderkey
FROM orders AS o2, s
WHERE o2.o_orderkey > s.a AND o2.o_orderkey < 600;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT AVG(o_orderkey) AS a FROM orders)
SELECT o2.o_orderkey
FROM orders AS o2, s
WHERE o2.o_orderkey > s.a AND o2.o_orderkey < 600|

--echo
--echo === avg-08: re-aggregation over the avg output (SUM / MIN / MAX of AVG) ===
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, AVG(o_shippriority) AS av
  FROM orders GROUP BY o_custkey)
SELECT SUM(a.av), MIN(a.av), MAX(a.av), COUNT(*)
FROM customer AS c JOIN a ON a.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, AVG(o_shippriority) AS av
  FROM orders GROUP BY o_custkey)
SELECT SUM(a.av), MIN(a.av), MAX(a.av), COUNT(*)
FROM customer AS c JOIN a ON a.k = c.c_custkey|

--echo
--echo
--echo === avg-09: AVG mixed with other aggregates (hidden-slot position stability) ===
# The hidden COUNT companion sits AFTER every visible slot; the outputs
# following the AVG (s, mn) must keep their positions in the virt
# table, the filter, and the emission paths.
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, COUNT(*) AS n, AVG(o_shippriority) AS av,
         SUM(o_shippriority) AS s, MIN(o_shippriority) AS mn
  FROM orders GROUP BY o_custkey)
SELECT a.k, a.n, a.av, a.s, a.mn FROM a
WHERE a.av > 1.0 AND a.s > 5;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, COUNT(*) AS n, AVG(o_shippriority) AS av,
         SUM(o_shippriority) AS s, MIN(o_shippriority) AS mn
  FROM orders GROUP BY o_custkey)
SELECT a.k, a.n, a.av, a.s, a.mn FROM a
WHERE a.av > 1.0 AND a.s > 5|

--echo
--echo === avg-10: two AVGs in one CTE (two hidden companions), pass-through ===
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, AVG(o_shippriority) AS av1, AVG(o_orderkey) AS av2
  FROM orders GROUP BY o_custkey)
SELECT a.k, a.av1, a.av2 FROM a WHERE a.k < 20;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, AVG(o_shippriority) AS av1, AVG(o_orderkey) AS av2
  FROM orders GROUP BY o_custkey)
SELECT a.k, a.av1, a.av2 FROM a WHERE a.k < 20|

DELIMITER ;|

# --- Rejection probes: the AVG argument envelope. ---

--echo
--echo === avg-P1 (rejection): AVG over a string column ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH a AS (SELECT o_custkey AS k, AVG(o_orderstatus) AS av FROM orders GROUP BY o_custkey) SELECT a.k, COUNT(*) FROM customer AS c JOIN a ON a.k = c.c_custkey GROUP BY a.k;' 2>&1

--echo
--echo === avg-P2 (rejection): AVG over a temporal column ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH a AS (SELECT o_custkey AS k, AVG(o_orderdate) AS av FROM orders GROUP BY o_custkey) SELECT a.k, COUNT(*) FROM customer AS c JOIN a ON a.k = c.c_custkey GROUP BY a.k;' 2>&1

--echo
--echo === avg-P3 (rejection): AVG over an expression ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH a AS (SELECT o_custkey AS k, AVG(o_shippriority + 1) AS av FROM orders GROUP BY o_custkey) SELECT a.k, COUNT(*) FROM customer AS c JOIN a ON a.k = c.c_custkey GROUP BY a.k;' 2>&1

--source suite/ronsql_cte/include/cte_drop.inc
