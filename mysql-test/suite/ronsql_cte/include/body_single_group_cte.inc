# body_single_group_cte.inc — single-group CTEs (cte_single_group_plan.md
# G3), data-driven CTE suite.
#
# FOCUS: a grouped CTE body whose every GROUP BY column is
# equality-bound to a constant in the body WHERE materializes at most
# ONE group (the fs_point shape).  RonSQL classifies it
# (is_single_group_cte_body) and defineCte carries CTE_SINGLE_GROUP:
# per-node partials ship straight to the constant DBTC-node owner (no
# group hashing), probes route to that owner, an empty state is a
# probe miss, and the G2a probe-outcome miss cache serves repeated
# missing keys locally.  A misclassification would fail cleanly with
# kernel error 1273 (ZCTE_SINGLE_GROUP_VIOLATION) — the controls below
# pin that correctly-classified-NEGATIVE shapes stay on the normal
# grouped pipeline with identical results.
#
# Data facts used (cte_data.inc): custkey k has exactly 5 orders
# (n = k, k+300, ..., k+1200); SUM(o_totalprice) = 13.37*(5k+3000);
# o_shippriority = MOD(n,5) is the same for all five orders of one
# custkey (= MOD(k,5)); MIN(o_orderdate) for k <= 300 is
# 1995-01-01 + k days.  Customer 9999 does not exist (empty-group
# cases).
#
# Case map:
#   sg-1  fs_point native: scalar re-aggregation over the single-group
#         CTE + EXPLAIN pin of the [single-group body] tag
#   sg-2  INNER join consumer: one hit, 299 misses -> COUNT=1, SUM=5
#   sg-3  LEFT JOIN consumer: 300 rows, SUM counts only the hit
#   sg-4  empty group (no rows match the bound key), INNER -> COUNT=0
#   sg-5  empty group, LEFT JOIN -> 300 rows, SUM NULL
#   sg-6  pass-through main over the CTE_SCAN root
#   sg-7  multi-column GROUP BY, both columns equality-bound
#   sg-8  AVG output in a single-group body (finalize on the owner)
#   sg-9  composition with body ORDER BY/LIMIT (no-op at one group)
#   sg-10 repeated-miss fan-out (G2a): empty CTE joined against all
#         300 customers — every probe misses, later ones served from
#         the DBSPJ miss cache
#   sg-c1 control: partially-bound GROUP BY (second GB column unbound)
#         — NOT flagged (EXPLAIN absence pin), normal path, results
#         identical
#   sg-c2 control: inequality bound — NOT flagged, 42 groups correct
#   sg-c3 control: OR of two equalities — NOT flagged, 2 groups correct
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
--echo === sg-1: fs_point shape — scalar re-aggregation over the CTE ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_totalprice) AS s,
                   MIN(o_orderdate) AS d1, MAX(o_orderdate) AS d2
            FROM orders WHERE o_custkey = 42 GROUP BY o_custkey)
SELECT MAX(cf.n) AS mn, MAX(cf.s) AS ms, MIN(cf.d1) AS md1,
       MAX(cf.d2) AS md2
FROM cf;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_sg_1.out
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_totalprice) AS s,
                   MIN(o_orderdate) AS d1, MAX(o_orderdate) AS d2
            FROM orders WHERE o_custkey = 42 GROUP BY o_custkey)
SELECT MAX(cf.n) AS mn, MAX(cf.s) AS ms, MIN(cf.d1) AS md1,
       MAX(cf.d2) AS md2
FROM cf;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[single-group body]' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_totalprice) AS s,
                   MIN(o_orderdate) AS d1, MAX(o_orderdate) AS d2
            FROM orders WHERE o_custkey = 42 GROUP BY o_custkey)
SELECT MAX(cf.n) AS mn, MAX(cf.s) AS ms, MIN(cf.d1) AS md1,
       MAX(cf.d2) AS md2
FROM cf|

--echo
--echo === sg-2: INNER join consumer — one hit, 299 misses ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_custkey = 42 GROUP BY o_custkey)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_custkey = 42 GROUP BY o_custkey)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey|

--echo
--echo === sg-3: LEFT JOIN consumer — 300 rows, SUM counts the hit ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_custkey = 42 GROUP BY o_custkey)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu LEFT JOIN cf ON cf.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_custkey = 42 GROUP BY o_custkey)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu LEFT JOIN cf ON cf.k = cu.c_custkey|

--echo
--echo === sg-4: empty group, INNER join -> COUNT=0 ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_custkey = 9999 GROUP BY o_custkey)
SELECT COUNT(*) AS c
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_custkey = 9999 GROUP BY o_custkey)
SELECT COUNT(*) AS c
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey|

--echo
--echo === sg-5: empty group, LEFT JOIN -> 300 rows, SUM NULL ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_custkey = 9999 GROUP BY o_custkey)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu LEFT JOIN cf ON cf.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_custkey = 9999 GROUP BY o_custkey)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu LEFT JOIN cf ON cf.k = cu.c_custkey|

--echo
--echo === sg-6: pass-through main over the CTE_SCAN root ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_custkey = 42 GROUP BY o_custkey)
SELECT k, n FROM cf;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_custkey = 42 GROUP BY o_custkey)
SELECT k, n FROM cf|

--echo
--echo === sg-7: multi-column GROUP BY, both columns equality-bound ===
# All five orders of custkey 42 share o_shippriority = MOD(42,5) = 2,
# so binding both columns still yields exactly the one group (42,2).
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, o_shippriority AS p, COUNT(*) AS n
            FROM orders
            WHERE o_custkey = 42 AND o_shippriority = 2
            GROUP BY o_custkey, o_shippriority)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, o_shippriority AS p, COUNT(*) AS n
            FROM orders
            WHERE o_custkey = 42 AND o_shippriority = 2
            GROUP BY o_custkey, o_shippriority)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey|

--echo
--echo === sg-8: AVG output in a single-group body ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, AVG(o_totalprice) AS a, COUNT(*) AS n
            FROM orders WHERE o_custkey = 42 GROUP BY o_custkey)
SELECT MAX(cf.a) AS ma, MAX(cf.n) AS mn
FROM cf;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, AVG(o_totalprice) AS a, COUNT(*) AS n
            FROM orders WHERE o_custkey = 42 GROUP BY o_custkey)
SELECT MAX(cf.a) AS ma, MAX(cf.n) AS mn
FROM cf|

--echo
--echo === sg-9: composition with body ORDER BY / LIMIT (no-op at 1 group) ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_custkey = 42 GROUP BY o_custkey
            ORDER BY n LIMIT 1)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_custkey = 42 GROUP BY o_custkey
            ORDER BY n LIMIT 1)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey|

--echo
--echo === sg-10: repeated-miss fan-out (G2a miss cache) ===
# The empty CTE is probed by all 300 customers — every probe misses;
# the first eligible miss fills the DBSPJ probe-outcome cache and the
# later byte-identical probes never round-trip.  Different keys per
# probe means only the slot-winning key is served — correctness is
# order-independent either way.
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, SUM(o_totalprice) AS s
            FROM orders WHERE o_custkey = 9999 GROUP BY o_custkey)
SELECT COUNT(*) AS c
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, SUM(o_totalprice) AS s
            FROM orders WHERE o_custkey = 9999 GROUP BY o_custkey)
SELECT COUNT(*) AS c
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey|

--echo
--echo === sg-c1: control — partially-bound GROUP BY is NOT flagged ===
# o_shippriority is unbound, so classification declines even though
# the data happens to give one group; the normal grouped pipeline must
# produce identical results, and EXPLAIN must NOT carry the tag.
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, o_shippriority AS p, COUNT(*) AS n
            FROM orders WHERE o_custkey = 42
            GROUP BY o_custkey, o_shippriority)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_sg_c1.out
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, o_shippriority AS p, COUNT(*) AS n
            FROM orders WHERE o_custkey = 42
            GROUP BY o_custkey, o_shippriority)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_explain.inc
--error 1
--exec grep -qF '[single-group body]' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, o_shippriority AS p, COUNT(*) AS n
            FROM orders WHERE o_custkey = 42
            GROUP BY o_custkey, o_shippriority)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey|

--echo
--echo === sg-c2: control — inequality bound is NOT flagged ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_custkey <= 42 GROUP BY o_custkey)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders WHERE o_custkey <= 42 GROUP BY o_custkey)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey|

--echo
--echo === sg-c3: control — OR of two equalities is NOT flagged ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders
            WHERE o_custkey = 42 OR o_custkey = 43
            GROUP BY o_custkey)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders
            WHERE o_custkey = 42 OR o_custkey = 43
            GROUP BY o_custkey)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey|

DELIMITER ;|

--source suite/ronsql_cte/include/cte_drop.inc
