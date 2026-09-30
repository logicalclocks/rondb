# body_outer_chain_miss.inc — an outer-joined aggregation leaf CTE_LOOKUP
# whose tree parent is another CTE_LOOKUP, data-driven CTE suite.
#
# fs_ronsql finding F15 (mysql-test/suite/ronsql_fs/findings/envelope_fuzz.md):
# an aggregating main that LEFT JOINs two aggregated CTEs crashed both data
# nodes (ndbassert, DbspjMain.cpp appendFromParent) on the first leaf miss.
# RonSQL chains the second CTE_LOOKUP (the aggregation leaf) under the first,
# so the leaf's linked-column pattern is relative to the first CTE_LOOKUP's
# row, but DBSPJ's CTE_LOOKUP_REF(GROUP_NOT_FOUND) arm expanded the NULL row
# from the scan root's buffered row and walked P_PARENT past the root.
# Fixed: DBSPJ buffers the leaf's direct parent and NULL-extends that row.
#
# NULL-injection technique (as body_joins.inc): every custkey 1..300 has
# orders; the CTE bodies filter o_custkey so they materialise only part of
# the keys.  lo = 1..100, hi = 51..150, so the chain customer -> lo -> hi
# sees every combination:
#   1..50    lo hit,  hi miss  (the leaf's NULL row must keep lo's values)
#   51..100  lo hit,  hi hit
#   101..150 lo miss, hi hit   (lo answers with its NULL-extended row)
#   151..300 lo miss, hi miss  (the F15 statement's common case)
# customer has 300 rows, so the root scan crosses the 256-row batch
# boundary and the buffered parent rows are reset between batches.
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
--echo === ocm-1: LEFT cte LEFT cte, main groups by the root key (the F15 shape) ===
let $QUERY=
WITH lo AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS s
  FROM orders WHERE o_custkey <= 100 GROUP BY o_custkey),
     hi AS (
  SELECT o_custkey AS k, COUNT(*) AS n, MAX(o_orderkey) AS m
  FROM orders WHERE o_custkey >= 51 AND o_custkey <= 150 GROUP BY o_custkey)
SELECT c.c_custkey, MAX(lo.n), SUM(lo.s), MAX(hi.n), MAX(hi.m)
FROM customer AS c
LEFT JOIN lo ON lo.k = c.c_custkey
LEFT JOIN hi ON hi.k = c.c_custkey
GROUP BY c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH lo AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS s
  FROM orders WHERE o_custkey <= 100 GROUP BY o_custkey),
     hi AS (
  SELECT o_custkey AS k, COUNT(*) AS n, MAX(o_orderkey) AS m
  FROM orders WHERE o_custkey >= 51 AND o_custkey <= 150 GROUP BY o_custkey)
SELECT c.c_custkey, MAX(lo.n), SUM(lo.s), MAX(hi.n), MAX(hi.m)
FROM customer AS c
LEFT JOIN lo ON lo.k = c.c_custkey
LEFT JOIN hi ON hi.k = c.c_custkey
GROUP BY c.c_custkey|

--echo
--echo === ocm-2: LEFT cte LEFT cte, scalar main (COUNT(*) must be 300) ===
let $QUERY=
WITH lo AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS s
  FROM orders WHERE o_custkey <= 100 GROUP BY o_custkey),
     hi AS (
  SELECT o_custkey AS k, COUNT(*) AS n, MAX(o_orderkey) AS m
  FROM orders WHERE o_custkey >= 51 AND o_custkey <= 150 GROUP BY o_custkey)
SELECT COUNT(*), SUM(lo.n), SUM(lo.s), SUM(hi.n), MAX(hi.m)
FROM customer AS c
LEFT JOIN lo ON lo.k = c.c_custkey
LEFT JOIN hi ON hi.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH lo AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS s
  FROM orders WHERE o_custkey <= 100 GROUP BY o_custkey),
     hi AS (
  SELECT o_custkey AS k, COUNT(*) AS n, MAX(o_orderkey) AS m
  FROM orders WHERE o_custkey >= 51 AND o_custkey <= 150 GROUP BY o_custkey)
SELECT COUNT(*), SUM(lo.n), SUM(lo.s), SUM(hi.n), MAX(hi.m)
FROM customer AS c
LEFT JOIN lo ON lo.k = c.c_custkey
LEFT JOIN hi ON hi.k = c.c_custkey|

--echo
--echo === ocm-3: LEFT cte LEFT cte, main groups by a non-key root column ===
let $QUERY=
WITH lo AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS s
  FROM orders WHERE o_custkey <= 100 GROUP BY o_custkey),
     hi AS (
  SELECT o_custkey AS k, COUNT(*) AS n, MAX(o_orderkey) AS m
  FROM orders WHERE o_custkey >= 51 AND o_custkey <= 150 GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(lo.n), SUM(lo.s), SUM(hi.n), MAX(hi.m)
FROM customer AS c
LEFT JOIN lo ON lo.k = c.c_custkey
LEFT JOIN hi ON hi.k = c.c_custkey
GROUP BY c.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH lo AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS s
  FROM orders WHERE o_custkey <= 100 GROUP BY o_custkey),
     hi AS (
  SELECT o_custkey AS k, COUNT(*) AS n, MAX(o_orderkey) AS m
  FROM orders WHERE o_custkey >= 51 AND o_custkey <= 150 GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(lo.n), SUM(lo.s), SUM(hi.n), MAX(hi.m)
FROM customer AS c
LEFT JOIN lo ON lo.k = c.c_custkey
LEFT JOIN hi ON hi.k = c.c_custkey
GROUP BY c.c_nationkey|

--echo
--echo === ocm-4: INNER cte LEFT cte, the leaf misses under a matched parent ===
# lo drops customers 101..300; of the rest, 1..50 miss hi.
let $QUERY=
WITH lo AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS s
  FROM orders WHERE o_custkey <= 100 GROUP BY o_custkey),
     hi AS (
  SELECT o_custkey AS k, COUNT(*) AS n, MAX(o_orderkey) AS m
  FROM orders WHERE o_custkey >= 51 AND o_custkey <= 150 GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(lo.n), SUM(lo.s), SUM(hi.n), MAX(hi.m)
FROM customer AS c
JOIN lo ON lo.k = c.c_custkey
LEFT JOIN hi ON hi.k = c.c_custkey
GROUP BY c.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH lo AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS s
  FROM orders WHERE o_custkey <= 100 GROUP BY o_custkey),
     hi AS (
  SELECT o_custkey AS k, COUNT(*) AS n, MAX(o_orderkey) AS m
  FROM orders WHERE o_custkey >= 51 AND o_custkey <= 150 GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(lo.n), SUM(lo.s), SUM(hi.n), MAX(hi.m)
FROM customer AS c
JOIN lo ON lo.k = c.c_custkey
LEFT JOIN hi ON hi.k = c.c_custkey
GROUP BY c.c_nationkey|

--echo
--echo === ocm-5: LEFT cte, anti-joined cte leaf (WHERE hi.n IS NULL) ===
# Only the leaf's misses contribute: customers 1..50 (with lo's values)
# and 151..300 (lo NULL).
let $QUERY=
WITH lo AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS s
  FROM orders WHERE o_custkey <= 100 GROUP BY o_custkey),
     hi AS (
  SELECT o_custkey AS k, COUNT(*) AS n, MAX(o_orderkey) AS m
  FROM orders WHERE o_custkey >= 51 AND o_custkey <= 150 GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(lo.n), SUM(lo.s)
FROM customer AS c
LEFT JOIN lo ON lo.k = c.c_custkey
LEFT JOIN hi ON hi.k = c.c_custkey
WHERE hi.n IS NULL
GROUP BY c.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH lo AS (
  SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS s
  FROM orders WHERE o_custkey <= 100 GROUP BY o_custkey),
     hi AS (
  SELECT o_custkey AS k, COUNT(*) AS n, MAX(o_orderkey) AS m
  FROM orders WHERE o_custkey >= 51 AND o_custkey <= 150 GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(lo.n), SUM(lo.s)
FROM customer AS c
LEFT JOIN lo ON lo.k = c.c_custkey
LEFT JOIN hi ON hi.k = c.c_custkey
WHERE hi.n IS NULL
GROUP BY c.c_nationkey|

DELIMITER ;|

--source suite/ronsql_cte/include/cte_drop.inc
