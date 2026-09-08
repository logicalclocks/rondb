# body_cte_dag.inc — per-CTE DAG scheduling (cte_dag_scheduler_plan.md),
# data-driven CTE suite.
#
# FOCUS: dependency-driven CTE scheduling.  The old scheduler ran CTEs
# in depth-derived phase waves — a dependent CTE waited for EVERY CTE
# of the previous wave, not just its own dependencies.  The DAG
# scheduler starts a CTE the moment every CTE in its depMask is READY
# (redistributed cluster-wide), so an independent slow CTE no longer
# gates an unrelated dependency chain.  These cases pin CORRECTNESS of
# the overlapping schedules (per-CTE scan reports, per-CTE
# redistributes, READY broadcasts, dependent starts) against mysqld;
# the latency win itself is benchmark territory (cte_tpch_q15/q22).
#
# Data facts used (cte_data.inc): custkey k has exactly 5 orders;
# o_shippriority = MOD(k,5) for all five orders of one custkey;
# customer.c_nationkey spreads 300 customers over 25 nations.
#
# Case map:
#   dag-1  the motivating skew shape: tiny dep-free CTE a (one custkey
#          slice), big dep-free CTE b (full lineitem-ish orders scan),
#          c depends only on a; main consumes b and c — c's
#          materialization overlaps b's
#   dag-2  same DAG, probe order flipped in the main join (b first) so
#          both delivery orders of the overlapped CTEs are pinned
#   dag-3  chained d<-a beside an independent b keyed on a different
#          column — two overlapping roots feeding one main join
#   dag-4  3-chain a -> b -> c (each start gated by exactly one READY
#          broadcast) plus main re-aggregation
#   dag-5  three mutually independent CTEs (pure width — no broadcast
#          is sent at all, every READY state catches up at start-main)
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
--echo === dag-1: skew shape — c depends on tiny a, big b independent ===
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_custkey < 20 GROUP BY o_custkey),
     b AS (
  SELECT o_custkey AS bk, SUM(o_shippriority) AS spr, COUNT(*) AS bn
  FROM orders GROUP BY o_custkey),
     c AS (
  SELECT k, SUM(n) AS n2
  FROM a GROUP BY k)
SELECT COUNT(*) AS cnt, SUM(c.n2) AS sn, SUM(b.spr) AS sp
FROM customer AS cu
JOIN c ON c.k = cu.c_custkey
JOIN b ON b.bk = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_custkey < 20 GROUP BY o_custkey),
     b AS (
  SELECT o_custkey AS bk, SUM(o_shippriority) AS spr, COUNT(*) AS bn
  FROM orders GROUP BY o_custkey),
     c AS (
  SELECT k, SUM(n) AS n2
  FROM a GROUP BY k)
SELECT COUNT(*) AS cnt, SUM(c.n2) AS sn, SUM(b.spr) AS sp
FROM customer AS cu
JOIN c ON c.k = cu.c_custkey
JOIN b ON b.bk = cu.c_custkey|

--echo
--echo === dag-2: same DAG, probe order flipped (b before c) ===
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_custkey < 20 GROUP BY o_custkey),
     b AS (
  SELECT o_custkey AS bk, SUM(o_shippriority) AS spr, COUNT(*) AS bn
  FROM orders GROUP BY o_custkey),
     c AS (
  SELECT k, SUM(n) AS n2
  FROM a GROUP BY k)
SELECT COUNT(*) AS cnt, SUM(b.bn) AS sb, SUM(c.n2) AS sn
FROM customer AS cu
JOIN b ON b.bk = cu.c_custkey
JOIN c ON c.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_custkey < 20 GROUP BY o_custkey),
     b AS (
  SELECT o_custkey AS bk, SUM(o_shippriority) AS spr, COUNT(*) AS bn
  FROM orders GROUP BY o_custkey),
     c AS (
  SELECT k, SUM(n) AS n2
  FROM a GROUP BY k)
SELECT COUNT(*) AS cnt, SUM(b.bn) AS sb, SUM(c.n2) AS sn
FROM customer AS cu
JOIN b ON b.bk = cu.c_custkey
JOIN c ON c.k = cu.c_custkey|

--echo
--echo === dag-3: chained d<-a beside independent b (nation-keyed) ===
# d depends only on a while b (keyed on a different column) scans
# independently; both feed the main SELECT.  A true two-bit depMask
# (diamond) is not expressible in RonSQL today — a CTE body joins one
# CTE — so kernel-level multi-bit masks stay covered by the block
# suites; this pins the two-root overlap with a dependent start.
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, COUNT(*) AS na
  FROM orders WHERE o_shippriority = 2 GROUP BY o_custkey),
     b AS (
  SELECT c_nationkey AS nk, COUNT(*) AS nb
  FROM customer GROUP BY c_nationkey),
     d AS (
  SELECT k, SUM(na) AS s
  FROM a GROUP BY k)
SELECT COUNT(*) AS cnt, SUM(d.s) AS sd, SUM(b.nb) AS snb
FROM customer AS cu
JOIN d ON d.k = cu.c_custkey
JOIN b ON b.nk = cu.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, COUNT(*) AS na
  FROM orders WHERE o_shippriority = 2 GROUP BY o_custkey),
     b AS (
  SELECT c_nationkey AS nk, COUNT(*) AS nb
  FROM customer GROUP BY c_nationkey),
     d AS (
  SELECT k, SUM(na) AS s
  FROM a GROUP BY k)
SELECT COUNT(*) AS cnt, SUM(d.s) AS sd, SUM(b.nb) AS snb
FROM customer AS cu
JOIN d ON d.k = cu.c_custkey
JOIN b ON b.nk = cu.c_nationkey|

--echo
--echo === dag-4: 3-chain a -> b -> c, each start gated by one READY ===
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS s
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, SUM(cnt) AS cnt2, MAX(s) AS mx
  FROM a GROUP BY k),
     c AS (
  SELECT k, SUM(cnt2) AS cnt3, MAX(mx) AS mx2
  FROM b GROUP BY k)
SELECT COUNT(*) AS cnt, SUM(c.cnt3) AS sc, MAX(c.mx2) AS m
FROM customer AS cu
JOIN c ON c.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS s
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, SUM(cnt) AS cnt2, MAX(s) AS mx
  FROM a GROUP BY k),
     c AS (
  SELECT k, SUM(cnt2) AS cnt3, MAX(mx) AS mx2
  FROM b GROUP BY k)
SELECT COUNT(*) AS cnt, SUM(c.cnt3) AS sc, MAX(c.mx2) AS m
FROM customer AS cu
JOIN c ON c.k = cu.c_custkey|

--echo
--echo === dag-5: three independent CTEs — no READY broadcast at all ===
let $QUERY=
WITH x AS (
  SELECT o_custkey AS k, COUNT(*) AS nx
  FROM orders GROUP BY o_custkey),
     y AS (
  SELECT c_nationkey AS nk, COUNT(*) AS ny
  FROM customer GROUP BY c_nationkey),
     z AS (
  SELECT o_custkey AS zk, MAX(o_shippriority) AS mz
  FROM orders WHERE o_shippriority > 0 GROUP BY o_custkey)
SELECT COUNT(*) AS cnt, SUM(x.nx) AS sx, SUM(y.ny) AS sy, SUM(z.mz) AS sz
FROM customer AS cu
JOIN x ON x.k = cu.c_custkey
JOIN y ON y.nk = cu.c_nationkey
JOIN z ON z.zk = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH x AS (
  SELECT o_custkey AS k, COUNT(*) AS nx
  FROM orders GROUP BY o_custkey),
     y AS (
  SELECT c_nationkey AS nk, COUNT(*) AS ny
  FROM customer GROUP BY c_nationkey),
     z AS (
  SELECT o_custkey AS zk, MAX(o_shippriority) AS mz
  FROM orders WHERE o_shippriority > 0 GROUP BY o_custkey)
SELECT COUNT(*) AS cnt, SUM(x.nx) AS sx, SUM(y.ny) AS sy, SUM(z.mz) AS sz
FROM customer AS cu
JOIN x ON x.k = cu.c_custkey
JOIN y ON y.nk = cu.c_nationkey
JOIN z ON z.zk = cu.c_custkey|

DELIMITER ;|

--source suite/ronsql_cte/include/cte_drop.inc
