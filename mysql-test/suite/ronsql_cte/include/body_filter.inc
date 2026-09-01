# body_filter.inc — filter family, data-driven CTE suite.
#
# FOCUS: WHERE-filter coverage in BOTH the CTE body (pre-GROUP BY) and the main
# query (on the CTE GROUP BY key + on the CTE aggregate outputs).  Backbone is
# the customer/orders join: a CTE grouped by o_custkey AS k joined to customer
# on c_custkey, matching the ronsql_cte_basic.test shapes.  String-literal
# filters (o_orderstatus = 'O', c_mktsegment = 'BUILDING', l_returnflag = 'R')
# use the QUERY_FILE form because the `let $QUERY=` path breaks on single quotes.
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

# =====================================================================
# Group 1 — CTE-body WHERE, one comparison operator per case on a
# numeric column (o_totalprice DECIMAL, o_custkey INT).  Pre-GROUP BY
# filter narrows the rows before aggregation.
# =====================================================================

--echo
--echo === filter-01: CTE-body WHERE > on o_totalprice ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders WHERE o_totalprice > 10000 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t), SUM(sums.n)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders WHERE o_totalprice > 10000 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t), SUM(sums.n)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k|

--echo
--echo === filter-02: CTE-body WHERE >= on o_totalprice ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_totalprice >= 10000 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_totalprice >= 10000 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k|

--echo
--echo === filter-03: CTE-body WHERE < on o_totalprice ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_totalprice < 10000 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_totalprice < 10000 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k|

--echo
--echo === filter-04: CTE-body WHERE <= on o_totalprice ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_totalprice <= 10000 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_totalprice <= 10000 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k|

--echo
--echo === filter-05: CTE-body WHERE = on o_shippriority ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_shippriority = 0 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_shippriority = 0 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k|

--echo
--echo === filter-06: CTE-body WHERE != on o_shippriority ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_shippriority != 0 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_shippriority != 0 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k|

--echo
--echo === filter-07: CTE-body WHERE range on o_custkey (two predicates, >= AND <=) ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_custkey >= 50 AND o_custkey <= 150 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_custkey >= 50 AND o_custkey <= 150 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k|

# =====================================================================
# Group 2 — CTE-body WHERE with AND / OR (DNF).
# =====================================================================

--echo
--echo === filter-08: CTE-body WHERE AND of two predicates on different cols ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS t
  FROM orders WHERE o_totalprice > 5000 AND o_shippriority = 2 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n), SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS t
  FROM orders WHERE o_totalprice > 5000 AND o_shippriority = 2 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n), SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k|

--echo
--echo === filter-09: CTE-body WHERE OR/DNF on o_shippriority ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_shippriority = 0 OR o_shippriority = 4 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_shippriority = 0 OR o_shippriority = 4 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k|

# =====================================================================
# Group 3 — IS NULL / IS NOT NULL on the nullable o_clerk.
#
# D10 (FIXED): IS NULL / IS NOT NULL in a *CTE body* WHERE is now
# supported.  A single-table CTE body emits its pre-GROUP-BY WHERE
# through NdbScanFilter::apply_filter, which now has a `case T_IS:` arm
# lowering to NdbScanFilter::isnull / isnotnull (previously the default
# arm threw "Non-boolean term in WHERE condition").  The SAME predicate
# on a main-query CTE_LOOKUP output uses Phase I.1 branch_linked_isnull
# (filter-10/11 below).  o_clerk is NULL when o_orderkey % 7 == 0
# (~1/7 of rows).
# filter-10a/11a exercise the CTE-body path (D10); filter-10/11 the
# main-query CTE-output path.
# =====================================================================

--echo
--echo === filter-10a (D10): CTE-body WHERE o_clerk IS NOT NULL ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS t
  FROM orders WHERE o_clerk IS NOT NULL GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n), SUM(sums.t)
FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS t
  FROM orders WHERE o_clerk IS NOT NULL GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n), SUM(sums.t)
FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k|

--echo
--echo === filter-11a (D10): CTE-body WHERE o_clerk IS NULL ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_clerk IS NULL GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n)
FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_clerk IS NULL GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n)
FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k|

# o_clerk NULL-semantics coverage that DOES work: aggregate o_clerk in the
# CTE body (MIN/MAX/COUNT(*)) and filter IS NULL / IS NOT NULL on the CTE
# AGGREGATE OUTPUT in the MAIN query (the supported Phase I.1 path).
# MAX(o_clerk) IS NULL exactly when ALL of a customer's orders have NULL
# o_clerk; with ~1/7 NULLs over 5 orders/customer that is rare, so both
# the IS NULL and IS NOT NULL partitions of the 300 customers are
# exercised and the aggregating main keeps the case GREEN.

--echo
--echo === filter-10: main WHERE on CTE output MAX(o_clerk) IS NOT NULL ===
let $QUERY=
WITH clk AS (
  SELECT o_custkey AS k, COUNT(*) AS n, MAX(o_clerk) AS mc
  FROM orders GROUP BY o_custkey)
SELECT clk.k, SUM(clk.n)
FROM customer AS c
JOIN clk ON clk.k = c.c_custkey
WHERE clk.mc IS NOT NULL
GROUP BY clk.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH clk AS (
  SELECT o_custkey AS k, COUNT(*) AS n, MAX(o_clerk) AS mc
  FROM orders GROUP BY o_custkey)
SELECT clk.k, SUM(clk.n)
FROM customer AS c
JOIN clk ON clk.k = c.c_custkey
WHERE clk.mc IS NOT NULL
GROUP BY clk.k|

--echo
--echo === filter-11: main WHERE on CTE output MAX(o_clerk) IS NULL ===
let $QUERY=
WITH clk AS (
  SELECT o_custkey AS k, COUNT(*) AS n, MAX(o_clerk) AS mc
  FROM orders GROUP BY o_custkey)
SELECT clk.k, SUM(clk.n)
FROM customer AS c
JOIN clk ON clk.k = c.c_custkey
WHERE clk.mc IS NULL
GROUP BY clk.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH clk AS (
  SELECT o_custkey AS k, COUNT(*) AS n, MAX(o_clerk) AS mc
  FROM orders GROUP BY o_custkey)
SELECT clk.k, SUM(clk.n)
FROM customer AS c
JOIN clk ON clk.k = c.c_custkey
WHERE clk.mc IS NULL
GROUP BY clk.k|

# =====================================================================
# Group 4 — CTE-body column-vs-column WHERE (D4 + D12 — now SUPPORTED,
# F-colvscol tier (a); cte_body_colvscol_plan.md).
#
# Identical-type stored-column pairs on the body root op pass the
# RonSQLPreparer::check_cte_body_col_vs_col gate and emit through
# apply_filter_cmp's NdbScanFilter attr-vs-attr arm (BRANCH_ATTR_OP_ATTR,
# SQL NULL semantics via setSqlCmpSemantics).  Gate and emit share the
# NdbDictionary::Column::isBindable predicate (identical type, precision,
# length, scale, charset; BLOB/TEXT excluded); mixed-type pairs and
# pairs not both on the body root op stay rejected — matrix + rejection
# probes in Group 8.
# History: filter-12/D12 (indexed left) was bound-misclassified into a
# 10x retry storm (since fixed by the join-root non-constant-RHS bound
# hardening); filter-13/D4 (non-indexed left) was recorded as a
# data-node hang in June 2026, re-verified September 2026 as a
# stale-binary artifact (W0 probe: all shapes green on current
# binaries; D19/D20 precedent).
# =====================================================================

--echo
--echo === filter-12 (D12): CTE-body col-vs-col on orders (indexed left) ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS t
  FROM orders WHERE o_custkey < o_orderkey GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n), SUM(sums.t)
FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS t
  FROM orders WHERE o_custkey < o_orderkey GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n), SUM(sums.t)
FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k|

--echo
--echo === filter-13 (D4): CTE-body col-vs-col on lineitem (non-indexed left) ===
let $QUERY=
WITH liq AS (
  SELECT l_orderkey AS k, COUNT(*) AS n, SUM(l_quantity) AS q
  FROM lineitem WHERE l_quantity < l_partkey GROUP BY l_orderkey)
SELECT o.o_custkey, SUM(liq.n), SUM(liq.q)
FROM orders AS o JOIN liq ON liq.k = o.o_orderkey GROUP BY o.o_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH liq AS (
  SELECT l_orderkey AS k, COUNT(*) AS n, SUM(l_quantity) AS q
  FROM lineitem WHERE l_quantity < l_partkey GROUP BY l_orderkey)
SELECT o.o_custkey, SUM(liq.n), SUM(liq.q)
FROM orders AS o JOIN liq ON liq.k = o.o_orderkey GROUP BY o.o_custkey|

# =====================================================================
# Group 5 — CTE-body GREATEST / LEAST in WHERE (D11 — now supported).
#
# GREATEST / LEAST are now accepted in a CTE-body WHERE conditional
# expression.  RonSQL lowers `GREATEST(...) <cmp> const` / `LEAST(...)
# <cmp> const` to a boolean OR/AND of per-argument column-vs-constant
# comparisons in simplify_ce (GREATEST > / LEAST < -> OR with IS NOT NULL
# guards; GREATEST < / LEAST > -> AND).  Constant arguments are folded;
# the comparand must be a constant and = / != are rejected.  Cases below
# cover GREATEST+OR (const-arg fold), LEAST+OR (two columns), and the
# AND direction via n-ary (3-arg) GREATEST/LEAST.  See _discovery_log.md (D11).
# =====================================================================

--echo
--echo === filter-14 (D11): CTE-body WHERE GREATEST(o_shippriority, 1) > 2 (OR dir, const-arg fold) ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE GREATEST(o_shippriority, 1) > 2 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n)
FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE GREATEST(o_shippriority, 1) > 2 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n)
FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k|

--echo
--echo === filter-14b (D11): CTE-body WHERE GREATEST(o_shippriority, o_custkey, 5) <= 50 (AND dir, n-ary) ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE GREATEST(o_shippriority, o_custkey, 5) <= 50 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n)
FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE GREATEST(o_shippriority, o_custkey, 5) <= 50 GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n)
FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k|

--echo
--echo === filter-15 (D11): CTE-body WHERE LEAST(l_quantity, l_partkey) < 10 (OR dir, two columns) ===
let $QUERY=
WITH liq AS (
  SELECT l_orderkey AS k, COUNT(*) AS n
  FROM lineitem WHERE LEAST(l_quantity, l_partkey) < 10 GROUP BY l_orderkey)
SELECT o.o_custkey, SUM(liq.n)
FROM orders AS o JOIN liq ON liq.k = o.o_orderkey GROUP BY o.o_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH liq AS (
  SELECT l_orderkey AS k, COUNT(*) AS n
  FROM lineitem WHERE LEAST(l_quantity, l_partkey) < 10 GROUP BY l_orderkey)
SELECT o.o_custkey, SUM(liq.n)
FROM orders AS o JOIN liq ON liq.k = o.o_orderkey GROUP BY o.o_custkey|

--echo
--echo === filter-15b (D11): CTE-body WHERE LEAST(l_quantity, l_partkey, 100) > 5 (AND dir, n-ary) ===
let $QUERY=
WITH liq AS (
  SELECT l_orderkey AS k, COUNT(*) AS n
  FROM lineitem WHERE LEAST(l_quantity, l_partkey, 100) > 5 GROUP BY l_orderkey)
SELECT o.o_custkey, SUM(liq.n)
FROM orders AS o JOIN liq ON liq.k = o.o_orderkey GROUP BY o.o_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH liq AS (
  SELECT l_orderkey AS k, COUNT(*) AS n
  FROM lineitem WHERE LEAST(l_quantity, l_partkey, 100) > 5 GROUP BY l_orderkey)
SELECT o.o_custkey, SUM(liq.n)
FROM orders AS o JOIN liq ON liq.k = o.o_orderkey GROUP BY o.o_custkey|

# =====================================================================
# Group 6 — CTE-body string-equality filters (single-quoted SQL
# literals).  QUERY_FILE form per the guide (Test 20 pattern).
# =====================================================================

--echo
--echo === filter-16: CTE-body WHERE o_orderstatus = 'O' (CHAR(1)) ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_filter16.sql
--write_file $QUERY_FILE
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS t
  FROM orders WHERE o_orderstatus = 'O' GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n), SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS t
  FROM orders WHERE o_orderstatus = 'O' GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n), SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
GROUP BY sums.k|

--echo
--echo === filter-17: CTE-body WHERE c_mktsegment = 'BUILDING' (VARCHAR) ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_filter17.sql
--write_file $QUERY_FILE
WITH segs AS (
  SELECT c_nationkey AS k, COUNT(*) AS n, SUM(c_nationkey) AS bal
  FROM customer WHERE c_mktsegment = 'BUILDING' GROUP BY c_nationkey)
SELECT segs.k, SUM(segs.n), SUM(segs.bal)
FROM nation AS n
JOIN segs ON segs.k = n.n_nationkey
GROUP BY segs.k;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH segs AS (
  SELECT c_nationkey AS k, COUNT(*) AS n, SUM(c_nationkey) AS bal
  FROM customer WHERE c_mktsegment = 'BUILDING' GROUP BY c_nationkey)
SELECT segs.k, SUM(segs.n), SUM(segs.bal)
FROM nation AS n
JOIN segs ON segs.k = n.n_nationkey
GROUP BY segs.k|

--echo
--echo === filter-18: CTE-body WHERE l_returnflag = 'R' (CHAR(1), lineitem) ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_filter18.sql
--write_file $QUERY_FILE
WITH liq AS (
  SELECT l_orderkey AS k, COUNT(*) AS n, SUM(l_quantity) AS q
  FROM lineitem WHERE l_returnflag = 'R' GROUP BY l_orderkey)
SELECT o.o_custkey, SUM(liq.n), SUM(liq.q)
FROM orders AS o
JOIN liq ON liq.k = o.o_orderkey
GROUP BY o.o_custkey;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH liq AS (
  SELECT l_orderkey AS k, COUNT(*) AS n, SUM(l_quantity) AS q
  FROM lineitem WHERE l_returnflag = 'R' GROUP BY l_orderkey)
SELECT o.o_custkey, SUM(liq.n), SUM(liq.q)
FROM orders AS o
JOIN liq ON liq.k = o.o_orderkey
GROUP BY o.o_custkey|

# =====================================================================
# Group 7 — Main-query WHERE on the CTE GROUP BY key, across all six
# operators.  Backbone CTE = sums(k = o_custkey, t = SUM(o_shippriority)).
# =====================================================================

--echo
--echo === filter-19: main WHERE = on CTE key ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.k = 100
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.k = 100
GROUP BY sums.k|

--echo
--echo === filter-20: main WHERE != on CTE key ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.k != 100
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.k != 100
GROUP BY sums.k|

--echo
--echo === filter-21: main WHERE < on CTE key ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.k < 50
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.k < 50
GROUP BY sums.k|

--echo
--echo === filter-22: main WHERE <= on CTE key ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.k <= 50
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.k <= 50
GROUP BY sums.k|

--echo
--echo === filter-23: main WHERE > on CTE key (crosses the 256-group batch boundary) ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.k > 250
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.k > 250
GROUP BY sums.k|

--echo
--echo === filter-24: main WHERE >= on CTE key ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.k >= 250
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.k >= 250
GROUP BY sums.k|

# =====================================================================
# Group 8 — Main-query WHERE on the CTE AGGREGATE output, across all
# six operators, plus a multi-conjunct AND combining a key predicate
# and an aggregate predicate.
# =====================================================================

--echo
--echo === filter-25: main WHERE = on CTE aggregate output ===
let $QUERY=
WITH cnt AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT cnt.k, SUM(cnt.n)
FROM customer AS c
JOIN cnt ON cnt.k = c.c_custkey
WHERE cnt.n = 5
GROUP BY cnt.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cnt AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT cnt.k, SUM(cnt.n)
FROM customer AS c
JOIN cnt ON cnt.k = c.c_custkey
WHERE cnt.n = 5
GROUP BY cnt.k|

--echo
--echo === filter-26: main WHERE != on CTE aggregate output ===
let $QUERY=
WITH cnt AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT cnt.k, SUM(cnt.n)
FROM customer AS c
JOIN cnt ON cnt.k = c.c_custkey
WHERE cnt.n != 5
GROUP BY cnt.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cnt AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT cnt.k, SUM(cnt.n)
FROM customer AS c
JOIN cnt ON cnt.k = c.c_custkey
WHERE cnt.n != 5
GROUP BY cnt.k|

--echo
--echo === filter-27: main WHERE < on CTE aggregate output (SUM) ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.t < 30000
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.t < 30000
GROUP BY sums.k|

--echo
--echo === filter-28: main WHERE <= on CTE aggregate output (SUM) ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.t <= 30000
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.t <= 30000
GROUP BY sums.k|

--echo
--echo === filter-29: main WHERE > on CTE aggregate output (SUM) ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.t > 60000
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.t > 60000
GROUP BY sums.k|

--echo
--echo === filter-30: main WHERE >= on CTE aggregate output (SUM) ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.t >= 60000
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.t >= 60000
GROUP BY sums.k|

--echo
--echo === filter-31: main WHERE multi-conjunct AND (key predicate AND aggregate predicate) ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.k >= 100 AND sums.t > 50000
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
WHERE sums.k >= 100 AND sums.t > 50000
GROUP BY sums.k|

# =====================================================================
# PROBES — uncertain/unsupported shapes.  Recorded but NOT executed
# (commented out under NEXT-PHASE markers), per the authoring guide.
# See findings/filter.md.
# =====================================================================

# filter-like (probe→SUPPORTED): LIKE on VARCHAR in a CTE-body WHERE.
# apply_filter routes T_LIKE → apply_filter_like (NdbScanFilter COND_LIKE), so
# this works end-to-end.  'A%' matches 'AUTOMOBILE'.  QUERY_FILE (string literal).
--echo
--echo === filter-like (probe): LIKE on VARCHAR c_mktsegment in CTE-body WHERE ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_filter_like.sql
--write_file $QUERY_FILE
WITH segs AS (
  SELECT c_nationkey AS k, COUNT(*) AS n
  FROM customer WHERE c_mktsegment LIKE 'A%' GROUP BY c_nationkey)
SELECT segs.k, SUM(segs.n) AS sn
FROM nation AS n JOIN segs ON segs.k = n.n_nationkey GROUP BY segs.k;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH segs AS (
  SELECT c_nationkey AS k, COUNT(*) AS n
  FROM customer WHERE c_mktsegment LIKE 'A%' GROUP BY c_nationkey)
SELECT segs.k, SUM(segs.n) AS sn
FROM nation AS n JOIN segs ON segs.k = n.n_nationkey GROUP BY segs.k|

# filter-between (probe→REJECTED): BETWEEN is a reserved keyword in RonSQL with
# no grammar rule, so it cleanly rejects with a syntax error.  Rejection-assert.
--echo
--echo === filter-between (probe): BETWEEN in CTE-body WHERE is rejected ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH sums AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t FROM orders WHERE o_totalprice BETWEEN 5000 AND 6000 GROUP BY o_custkey) SELECT sums.k, SUM(sums.t) FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k;' 2>&1

# filter-in (probe→SUPPORTED): IN-list in a CTE-body WHERE.  The grammar
# expands `col IN (a,b,c)` to `col = a OR col = b OR col = c` at parse time
# (RonSQLParser.y), so apply_filter's existing T_OR/T_EQUALS handle it — no
# kernel/preparer change needed.
--echo
--echo === filter-in (probe): IN-list (o_shippriority IN (0,2,4)) in CTE-body WHERE ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_shippriority IN (0,2,4) GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n) AS sn
FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_shippriority IN (0,2,4) GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n) AS sn
FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k|

# (String col-vs-col in a CTE-body WHERE is now live — see the Group 8
# matrix: filter-35 CHAR pair on cvc, filter-38 degenerate self-compare.)

# filter-or-main (probe→SUPPORTED): top-level OR in the MAIN-query CTE filter,
# across the CTE key (sums.k) and an aggregate output (sums.t).
# emit_cte_lookup_filter flattens top-level OR into DNF disjuncts (Phase I.2)
# and emits a per-disjunct fail-label chain, so this works — no code change.
--echo
--echo === filter-or-main (probe): OR across CTE key + aggregate output in main WHERE ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t) AS st
FROM customer AS c JOIN sums ON sums.k = c.c_custkey
WHERE sums.k = 100 OR sums.t > 50000
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t) AS st
FROM customer AS c JOIN sums ON sums.k = c.c_custkey
WHERE sums.k = 100 OR sums.t > 50000
GROUP BY sums.k|

# filter-in-main (probe→SUPPORTED): IN-list in the MAIN-query CTE filter.
# Completes the IN/OR x body/main matrix: the grammar expands sums.k IN (...)
# to sums.k = 50 OR sums.k = 100 OR sums.k = 150, which emit_cte_lookup_filter
# handles as DNF disjuncts (same path as filter-or-main) — no code change.
--echo
--echo === filter-in-main (probe): IN-list on a CTE key in the main WHERE ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t) AS st
FROM customer AS c JOIN sums ON sums.k = c.c_custkey
WHERE sums.k IN (50, 100, 150)
GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT sums.k, SUM(sums.t) AS st
FROM customer AS c JOIN sums ON sums.k = c.c_custkey
WHERE sums.k IN (50, 100, 150)
GROUP BY sums.k|

# NEXT-PHASE: CASE condition over a CTE AGGREGATE output — NOT supported
#   (matrix: CASE over a CTE COLUMN projection is supported, over an aggregate
#   output is not).
#   WITH sums AS (
#     SELECT o_custkey AS k, SUM(o_shippriority) AS t
#     FROM orders GROUP BY o_custkey)
#   SELECT sums.k, SUM(sums.t)
#   FROM customer AS c JOIN sums ON sums.k = c.c_custkey
#   WHERE CASE WHEN sums.t > 50000 THEN 1 ELSE 0 END = 1
#   GROUP BY sums.k;

# =====================================================================
# Group 8 — CTE-body col-vs-col type matrix (F-colvscol tier (a),
# cte_body_colvscol_plan.md).  Extends the Group 4 INT cases across the
# accepted identical-type envelope (nullable INT, CHAR, DATE, DECIMAL),
# operators (=, <>, <, <=, >), OR/DNF, and a scalar-body residual; the
# filter-P* probes pin the two refined rejections (mixed-type pair /
# pair not on the body root op).
#
# Local table cvc: identical-type CHAR/DATE/DECIMAL column PAIRS the
# shared TPC-H-lite schema lacks (each shared table has at most one
# column per exact type), plus a both-nullable INT pair.  12 rows spread
# <, =, > per pair; n1/n2 cover NULL-vs-set, set-vs-NULL, NULL-vs-NULL.
# =====================================================================

DELIMITER ;|

--disable_query_log
--disable_warnings
CREATE TABLE cvc (
  pk  INT NOT NULL,
  grp INT NOT NULL,
  a   INT NOT NULL,
  b   INT NOT NULL,
  v1  CHAR(8) NOT NULL,
  v2  CHAR(8) NOT NULL,
  d1  DATE NOT NULL,
  d2  DATE NOT NULL,
  m1  DECIMAL(10,2) NOT NULL,
  m2  DECIMAL(10,2) NOT NULL,
  n1  INT NULL,
  n2  INT NULL,
  PRIMARY KEY USING HASH (pk)
) ENGINE=NDB;
INSERT INTO cvc VALUES
  ( 1, 1, 1, 2, 'alpha', 'beta',  '2024-01-01', '2024-06-01', 10.50, 20.25,    1,    2),
  ( 2, 1, 2, 2, 'beta',  'beta',  '2024-06-01', '2024-06-01', 20.25, 20.25, NULL,    2),
  ( 3, 1, 3, 1, 'gamma', 'beta',  '2024-12-31', '2024-06-01', 30.00, 20.25,    3, NULL),
  ( 4, 2, 5, 9, 'delta', 'omega', '2023-05-15', '2025-05-15',  5.05, 99.99, NULL, NULL),
  ( 5, 2, 9, 5, 'omega', 'delta', '2025-05-15', '2023-05-15', 99.99,  5.05,    5,    4),
  ( 6, 2, 7, 7, 'kappa', 'kappa', '2024-02-29', '2024-02-29', 42.42, 42.42,    6,    6),
  ( 7, 3, 0, 1, 'aa',    'ab',    '2020-01-01', '2020-01-02',  0.01,  0.02, NULL,    1),
  ( 8, 3, 1, 0, 'ab',    'aa',    '2020-01-02', '2020-01-01',  0.02,  0.01,    8, NULL),
  ( 9, 3, 4, 8, 'mid',   'midz',  '2022-07-04', '2022-07-05', 12.34, 12.35,    9,   10),
  (10, 3, 8, 4, 'midz',  'mid',   '2022-07-05', '2022-07-04', 12.35, 12.34,   10,    9),
  (11, 1, 6, 6, 'same',  'same',  '2021-03-03', '2021-03-03',  7.77,  7.77,   11,   11),
  (12, 2, 2, 9, 'aaa',   'zzz',   '2019-09-09', '2026-09-01',  1.00,  2.00,   12, NULL);
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === filter-32: nullable INT vs NOT NULL INT (o_clerk < o_custkey, NULL rows rejected) ===
let $QUERY=
WITH clk AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_clerk < o_custkey GROUP BY o_custkey)
SELECT clk.k, SUM(clk.n)
FROM customer AS c JOIN clk ON clk.k = c.c_custkey GROUP BY clk.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH clk AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_clerk < o_custkey GROUP BY o_custkey)
SELECT clk.k, SUM(clk.n)
FROM customer AS c JOIN clk ON clk.k = c.c_custkey GROUP BY clk.k|

--echo
--echo === filter-33: col-vs-col inside body OR/DNF ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_custkey < o_orderkey OR o_shippriority = 0
  GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n)
FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_custkey < o_orderkey OR o_shippriority = 0
  GROUP BY o_custkey)
SELECT sums.k, SUM(sums.n)
FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k|

--echo
--echo === filter-33b: NULLABLE col-vs-col atom inside OR (UNKNOWN falls through to next disjunct) ===
let $QUERY=
WITH clk AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_clerk < o_custkey OR o_shippriority = 0
  GROUP BY o_custkey)
SELECT clk.k, SUM(clk.n)
FROM customer AS c JOIN clk ON clk.k = c.c_custkey GROUP BY clk.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH clk AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders WHERE o_clerk < o_custkey OR o_shippriority = 0
  GROUP BY o_custkey)
SELECT clk.k, SUM(clk.n)
FROM customer AS c JOIN clk ON clk.k = c.c_custkey GROUP BY clk.k|

--echo
--echo === filter-34: scalar body, PK equality + col-vs-col residual ===
let $QUERY=
WITH s AS (
  SELECT COUNT(*) AS n, SUM(o_shippriority) AS t
  FROM orders WHERE o_orderkey = 5 AND o_custkey < o_orderkey)
SELECT s.n, s.t FROM s;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (
  SELECT COUNT(*) AS n, SUM(o_shippriority) AS t
  FROM orders WHERE o_orderkey = 5 AND o_custkey < o_orderkey)
SELECT s.n, s.t FROM s|

--echo
--echo === filter-35: CHAR(8) vs CHAR(8) (charset-aware kernel compare) ===
let $QUERY=
WITH f AS (
  SELECT grp AS g, COUNT(*) AS n FROM cvc WHERE v1 < v2 GROUP BY grp)
SELECT f.g, f.n FROM f;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH f AS (
  SELECT grp AS g, COUNT(*) AS n FROM cvc WHERE v1 < v2 GROUP BY grp)
SELECT f.g, f.n FROM f|

--echo
--echo === filter-36: DATE vs DATE (<=) ===
let $QUERY=
WITH f AS (
  SELECT grp AS g, COUNT(*) AS n FROM cvc WHERE d1 <= d2 GROUP BY grp)
SELECT f.g, f.n FROM f;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH f AS (
  SELECT grp AS g, COUNT(*) AS n FROM cvc WHERE d1 <= d2 GROUP BY grp)
SELECT f.g, f.n FROM f|

--echo
--echo === filter-37: DECIMAL(10,2) vs DECIMAL(10,2) (>) ===
let $QUERY=
WITH f AS (
  SELECT grp AS g, COUNT(*) AS n FROM cvc WHERE m1 > m2 GROUP BY grp)
SELECT f.g, f.n FROM f;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH f AS (
  SELECT grp AS g, COUNT(*) AS n FROM cvc WHERE m1 > m2 GROUP BY grp)
SELECT f.g, f.n FROM f|

--echo
--echo === filter-38: degenerate self-compare (l_returnflag < l_returnflag, empty CTE drops the join) ===
let $QUERY=
WITH liq AS (
  SELECT l_orderkey AS k, COUNT(*) AS n
  FROM lineitem WHERE l_returnflag < l_returnflag GROUP BY l_orderkey)
SELECT o.o_custkey, SUM(liq.n)
FROM orders AS o JOIN liq ON liq.k = o.o_orderkey GROUP BY o.o_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH liq AS (
  SELECT l_orderkey AS k, COUNT(*) AS n
  FROM lineitem WHERE l_returnflag < l_returnflag GROUP BY l_orderkey)
SELECT o.o_custkey, SUM(liq.n)
FROM orders AS o JOIN liq ON liq.k = o.o_orderkey GROUP BY o.o_custkey|

--echo
--echo === filter-39: operator sweep on cvc INT pair (=, <>) ===
let $QUERY=
WITH f AS (
  SELECT grp AS g, COUNT(*) AS n FROM cvc WHERE a = b GROUP BY grp)
SELECT f.g, f.n FROM f;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH f AS (
  SELECT grp AS g, COUNT(*) AS n FROM cvc WHERE a = b GROUP BY grp)
SELECT f.g, f.n FROM f|

--echo
--echo === filter-39b: both-nullable INT pair (n1 <> n2; NULL on either side rejects) ===
let $QUERY=
WITH f AS (
  SELECT grp AS g, COUNT(*) AS n FROM cvc WHERE n1 <> n2 GROUP BY grp)
SELECT f.g, f.n FROM f;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH f AS (
  SELECT grp AS g, COUNT(*) AS n FROM cvc WHERE n1 <> n2 GROUP BY grp)
SELECT f.g, f.n FROM f|

# --- Rejection probes: the two refined permanent errors. ---

--echo
--echo === filter-P1 (rejection): mixed INT vs SMALLINT UNSIGNED ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH sums AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders WHERE o_custkey < o_shippriority GROUP BY o_custkey) SELECT sums.k, SUM(sums.n) FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k;' 2>&1

--echo
--echo === filter-P2 (rejection): mixed FLOAT vs DOUBLE ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH liq AS (SELECT l_orderkey AS k, COUNT(*) AS n FROM lineitem WHERE l_discount < l_tax GROUP BY l_orderkey) SELECT o.o_custkey, SUM(liq.n) FROM orders AS o JOIN liq ON liq.k = o.o_orderkey GROUP BY o.o_custkey;' 2>&1

--echo
--echo === filter-P3 (rejection): DECIMAL precision/scale mismatch (12,2 vs 8,0) ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH sup AS (SELECT s_nationkey AS k, COUNT(*) AS n FROM supplier WHERE s_acctbal < s_margin GROUP BY s_nationkey) SELECT sup.k, SUM(sup.n) FROM nation AS na JOIN sup ON sup.k = na.n_nationkey GROUP BY sup.k;' 2>&1

--echo
--echo === filter-P4 (rejection): pair on a non-root op of a join body ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH j AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders JOIN lineitem ON l_orderkey = o_orderkey WHERE l_quantity < l_partkey GROUP BY o_custkey) SELECT j.k, SUM(j.n) FROM customer AS c JOIN j ON j.k = c.c_custkey GROUP BY j.k;' 2>&1

DELIMITER ;|

--disable_query_log
DROP TABLE cvc;
--enable_query_log

--source suite/ronsql_cte/include/cte_drop.inc
