# body_passthrough_orderby.inc — ORDER BY [+ LIMIT] on projection-only
# (pass-through) shapes (ronsql_orderby_limit_plan.md Phase 3),
# data-driven CTE suite.
#
# Phase 3 fully relaxes the non-aggregate parse gate: ORDER BY on a
# projection-only query buffers every delivered row client-side (arrays
# of NdbRecAttr clones), sorts with charset-aware NdbSqlUtil
# comparators (NULLs first in ASC, MySQL semantics; op-level LEFT JOIN
# NULL-extended rows sort as NULL), applies LIMIT post-sort via
# partial_sort, and prints through the unchanged pass-through printer.
# A sort column outside the SELECT list gets an extra getValue (real
# tables) or reuses the always-fetched virt-table registration (CTE
# columns).  A generous fixed cap (1M rows / 256 MB) bounds the buffer
# with a clean permanent error.
#
# ORDER VERIFICATION: ordered cases set $skip_sort=yes so
# ronsql_compare.inc strict-diffs RonSQL vs MySQL in DELIVERED row
# order; every ORDER BY below is a total order (unique leading key or
# explicit tie-breaker).
#
# Case map (po-N):
#   1   CTE_SCAN root, ORDER BY CTE key ASC (unique)
#   2   CTE_SCAN root, ORDER BY CTE aggregate output DESC + LIMIT
#       (tie run resolved by the k ASC tie-breaker; partial_sort)
#   3   CTE_SCAN root, ORDER BY + LIMIT 0 -> empty, no header
#   4   CTE_SCAN root, ORDER BY a CTE column NOT in the SELECT + LIMIT
#   5   join chain, ORDER BY root column DESC (OUTPUT_REF alias path)
#   6   join chain, ORDER BY a real-table column NOT in the SELECT
#       (extra getValue on the root op)
#   7   join chain, string collation order + tie-breaker
#   8   LEFT JOIN NULL ordering: NULLs first in ASC + LIMIT across the
#       NULL run
#   9   LEFT JOIN NULL ordering: DESC puts NULLs last + LIMIT
#   10  single-table, ORDER BY DESC (index-scan + buffered sort)
#   11  single-table, ORDER BY a column NOT in the SELECT + LIMIT
#   12  single-table, DATE ordering ASC + unique tie-breaker
#   13  single-table PK lookup + ORDER BY (single row, no-op sort)
#   14  single-table, DECIMAL ordering DESC + LIMIT
#   P1  rejection: GROUP BY on a projection-only query stays rejected
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

# ============================================================
# E.3 CTE_SCAN root.
# ============================================================

--echo
--echo === po-1: CTE_SCAN root, ORDER BY CTE key ASC ===
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
            FROM orders GROUP BY o_custkey)
SELECT k, t FROM cf ORDER BY k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
            FROM orders GROUP BY o_custkey)
SELECT k, t FROM cf ORDER BY k|

--echo
--echo === po-2: CTE_SCAN root, ORDER BY CTE aggregate output DESC + LIMIT ===
# t = 5*(k mod 5), so the t DESC runs are 60-way ties resolved by the
# k ASC tie-breaker; LIMIT 10 cuts inside the t=20 run (k=4,9,...,49).
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
            FROM orders GROUP BY o_custkey)
SELECT k, t FROM cf ORDER BY t DESC, k ASC LIMIT 10;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
            FROM orders GROUP BY o_custkey)
SELECT k, t FROM cf ORDER BY t DESC, k ASC LIMIT 10|

--echo
--echo === po-3: CTE_SCAN root, ORDER BY + LIMIT 0 (empty, no header) ===
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
            FROM orders GROUP BY o_custkey)
SELECT k, t FROM cf ORDER BY k LIMIT 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
            FROM orders GROUP BY o_custkey)
SELECT k, t FROM cf ORDER BY k LIMIT 0|

--echo
--echo === po-4: CTE_SCAN root, ORDER BY a CTE column NOT in the SELECT ===
# k is sorted on but not projected (the kernel emits every virt-table
# column, so no extra fetch is needed); output is t of custkeys 1..5.
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
            FROM orders GROUP BY o_custkey)
SELECT t FROM cf ORDER BY k LIMIT 5;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
            FROM orders GROUP BY o_custkey)
SELECT t FROM cf ORDER BY k LIMIT 5|

# ============================================================
# I.8 join chain: real-table root + complete-key CTE_LOOKUP child.
# ============================================================

--echo
--echo === po-5: join chain, ORDER BY root column DESC ===
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey, cf.n
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey <= 20
ORDER BY c_custkey DESC;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey, cf.n
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey <= 20
ORDER BY c_custkey DESC|

--echo
--echo === po-6: join chain, ORDER BY a real-table column NOT in the SELECT ===
# cu.c_custkey is sorted on but not projected -> extra getValue on the
# root op; the projected (n, segment) pairs are deterministic per
# custkey 10..1.
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cf.n, cu.c_mktsegment
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey <= 10
ORDER BY cu.c_custkey DESC;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cf.n, cu.c_mktsegment
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey <= 10
ORDER BY cu.c_custkey DESC|

--echo
--echo === po-7: join chain, string collation order + tie-breaker ===
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_mktsegment, cu.c_custkey
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey <= 25
ORDER BY c_mktsegment ASC, c_custkey ASC;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_mktsegment, cu.c_custkey
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey <= 25
ORDER BY c_mktsegment ASC, c_custkey ASC|

# ============================================================
# LEFT JOIN NULL ordering: unmatched rows sort as NULL.
# co covers custkeys 1..60 (one order each), so custkeys 61..70 of the
# WHERE window are NULL-extended.
# ============================================================

--echo
--echo === po-8: LEFT JOIN, NULLs first in ASC + LIMIT across the NULL run ===
--let $skip_sort=yes
let $QUERY=
WITH co AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_orderkey <= 60 GROUP BY o_custkey)
SELECT cu.c_custkey, co.n
FROM customer AS cu LEFT JOIN co ON co.k = cu.c_custkey
WHERE cu.c_custkey <= 70
ORDER BY co.n ASC, c_custkey ASC
LIMIT 15;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH co AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_orderkey <= 60 GROUP BY o_custkey)
SELECT cu.c_custkey, co.n
FROM customer AS cu LEFT JOIN co ON co.k = cu.c_custkey
WHERE cu.c_custkey <= 70
ORDER BY co.n ASC, c_custkey ASC
LIMIT 15|

--echo
--echo === po-9: LEFT JOIN, DESC puts NULLs last + LIMIT ===
--let $skip_sort=yes
let $QUERY=
WITH co AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_orderkey <= 60 GROUP BY o_custkey)
SELECT cu.c_custkey, co.n
FROM customer AS cu LEFT JOIN co ON co.k = cu.c_custkey
WHERE cu.c_custkey <= 70
ORDER BY co.n DESC, c_custkey DESC
LIMIT 5;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH co AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_orderkey <= 60 GROUP BY o_custkey)
SELECT cu.c_custkey, co.n
FROM customer AS cu LEFT JOIN co ON co.k = cu.c_custkey
WHERE cu.c_custkey <= 70
ORDER BY co.n DESC, c_custkey DESC
LIMIT 5|

# ============================================================
# Single-table pass-through (plain NDB API path).
# ============================================================

--echo
--echo === po-10: single-table, ORDER BY DESC ===
--let $skip_sort=yes
let $QUERY=
SELECT c_custkey, c_acctbal FROM customer
WHERE c_nationkey = 7 ORDER BY c_custkey DESC;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
SELECT c_custkey, c_acctbal FROM customer
WHERE c_nationkey = 7 ORDER BY c_custkey DESC|

--echo
--echo === po-11: single-table, ORDER BY a column NOT in the SELECT + LIMIT ===
--let $skip_sort=yes
let $QUERY=
SELECT c_acctbal FROM customer
WHERE c_nationkey = 7 ORDER BY c_custkey ASC LIMIT 5;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
SELECT c_acctbal FROM customer
WHERE c_nationkey = 7 ORDER BY c_custkey ASC LIMIT 5|

--echo
--echo === po-12: single-table, DATE ordering ASC + unique tie-breaker ===
--let $skip_sort=yes
let $QUERY=
SELECT o_orderkey, o_orderdate FROM orders
WHERE o_custkey = 57 ORDER BY o_orderdate ASC, o_orderkey ASC;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
SELECT o_orderkey, o_orderdate FROM orders
WHERE o_custkey = 57 ORDER BY o_orderdate ASC, o_orderkey ASC|

--echo
--echo === po-13: single-table PK lookup + ORDER BY (single row) ===
--let $skip_sort=yes
let $QUERY=
SELECT o_orderkey, o_orderstatus FROM orders
WHERE o_orderkey = 77 ORDER BY o_orderstatus;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
SELECT o_orderkey, o_orderstatus FROM orders
WHERE o_orderkey = 77 ORDER BY o_orderstatus|

--echo
--echo === po-14: single-table, DECIMAL ordering DESC + LIMIT ===
--let $skip_sort=yes
let $QUERY=
SELECT c_custkey, c_acctbal FROM customer
WHERE c_nationkey = 7 ORDER BY c_acctbal DESC LIMIT 5;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
SELECT c_custkey, c_acctbal FROM customer
WHERE c_nationkey = 7 ORDER BY c_acctbal DESC LIMIT 5|

DELIMITER ;|

# ============================================================
# PROBES (form 1): clean permanent rejection via ronsql_cli --error 1.
# ============================================================

--echo
--echo === po-P1 (rejection): GROUP BY on a projection-only query ===
# The non-aggregate gate accepts ORDER BY / LIMIT since Phases 2+3 but
# still requires no GROUP BY / HAVING / expressions.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT k FROM cf GROUP BY k ORDER BY k;' 2>&1

--source suite/ronsql_cte/include/cte_drop.inc
