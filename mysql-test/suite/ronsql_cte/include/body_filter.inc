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
# Group 4 — CTE-body column-vs-column on SIGNED integers.
#
# DISPOSITION (D4 + D12, fix-plan H3): col-vs-col in a CTE-body WHERE is
# NOT yet supported and is now cleanly + PERMANENTLY rejected at prepare
# time (RonSQLPreparer::check_no_cte_body_col_vs_col, called from
# build_cte_scopes).  Previously it either retried 10x (indexed left
# column -> mis-classified as an index bound, encode_constant fails on
# the column RHS, thrown retryable; filter-12/D12) or hung the data node
# (non-indexed left column -> TABLE_SCAN attr-vs-attr NdbScanFilter;
# filter-13/D4).  Both are now rejection-asserts.  Supporting col-vs-col
# in a CTE-body filter is tracked for a later phase.
# =====================================================================

--echo
--echo == filter-12 (D12): CTE-body col-vs-col on orders (indexed left) — permanent rejection ==
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH sums AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS t FROM orders WHERE o_custkey < o_orderkey GROUP BY o_custkey) SELECT sums.k, SUM(sums.n), SUM(sums.t) FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k;' 2>&1

--echo
--echo == filter-13 (D4): CTE-body col-vs-col on lineitem (non-indexed left) — permanent rejection ==
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH liq AS (SELECT l_orderkey AS k, COUNT(*) AS n, SUM(l_quantity) AS q FROM lineitem WHERE l_quantity < l_partkey GROUP BY l_orderkey) SELECT o.o_custkey, SUM(liq.n), SUM(liq.q) FROM orders AS o JOIN liq ON liq.k = o.o_orderkey GROUP BY o.o_custkey;' 2>&1

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

# NEXT-PHASE: IN-list in CTE-body WHERE — needs feature work, not cheap.
#   apply_filter has no T_IN case and simplify_ce does not expand IN→OR, so
#   `col IN (a,b,c)` in a CTE-body WHERE is not handled.  To support: expand
#   T_IN to an OR-chain of equalities in simplify_ce (then apply_filter's
#   existing T_OR/T_EQUALS handle it).
#   WITH sums AS (
#     SELECT o_custkey AS k, COUNT(*) AS n
#     FROM orders WHERE o_shippriority IN (0,2,4) GROUP BY o_custkey)
#   SELECT sums.k, SUM(sums.n)
#   FROM customer AS c JOIN sums ON sums.k = c.c_custkey GROUP BY sums.k;

# NEXT-PHASE: string column-vs-column in CTE-body WHERE — only signed-int
#   col-vs-col is supported (matrix).  Two CHAR cols compared:
#   WITH liq AS (
#     SELECT l_orderkey AS k, COUNT(*) AS n
#     FROM lineitem WHERE l_returnflag < l_returnflag GROUP BY l_orderkey)
#   SELECT o.o_custkey, SUM(liq.n)
#   FROM orders AS o JOIN liq ON liq.k = o.o_orderkey GROUP BY o.o_custkey;

# NEXT-PHASE: OR combining predicates in the MAIN-query CTE filter — UNCERTAIN.
#   emit_cte_lookup_filter accepts top-level AND conjuncts; top-level OR across
#   key + aggregate-output predicates is not yet exercised from SQL.
#   WITH sums AS (
#     SELECT o_custkey AS k, SUM(o_shippriority) AS t
#     FROM orders GROUP BY o_custkey)
#   SELECT sums.k, SUM(sums.t)
#   FROM customer AS c JOIN sums ON sums.k = c.c_custkey
#   WHERE sums.k = 100 OR sums.t > 50000
#   GROUP BY sums.k;

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

DELIMITER ;|

--source suite/ronsql_cte/include/cte_drop.inc
