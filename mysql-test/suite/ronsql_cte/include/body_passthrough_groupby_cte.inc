# body_passthrough_groupby_cte.inc — grouped (GROUP BY) aggregating
# CTEs in projection-only queries (non_aggregate_phase_5.md),
# data-driven suite.
#
# Phase 5 is the final non-aggregate roadmap phase: grouped CTEs
# (complete-key CTE_LOOKUPs on the GROUP BY key) joined anywhere into
# the snowflake/star trees of Phases 2-3.  The audit found zero code
# changes needed — gate (I.8 heritage), planner sibling re-chaining,
# emit tree-parent pinning, drain and printer are all already general —
# so this family tests the cross product of shapes, including three
# first-ever-on-either-path topology probes (gc-7/8/9).
#
# The D5 fan-out rejection (linked_source_is_leaf_ancestor) does NOT
# apply here: it guards aggregation linked projections from
# non-ancestor siblings, and pass-through queries have none — only join
# keys linked from direct parents.  gc-3 exercises exactly the
# CTE-branch + real-branch star that D5 made famous on the aggregate
# path.
#
# Local table gckey provides the two-column join source for the
# multi-key CTE cases (the shared schema has no real table carrying
# both halves of a 2-key group).
#
# Case map (gc-N):
#   1  root real scan + grouped CTE arm (I.8 basic shape)
#   2  snowflake chain, CTE keyed on a lookup child (depth 2)
#   3  star: real branch + CTE branch on the same root
#   4  CTE keyed on an INDEX_SCAN child's column
#   5  LEFT JOIN grouped CTE with NULL groups
#   6  two grouped CTEs on the same parent (keyed sibling re-chain;
#      second CTE grouped over nullable o_clerk)
#   7  PROBE: two grouped CTEs on different parents (branches)
#   8  PROBE: real PK lookup below a CTE_LOOKUP, keyed on CTE output
#   9  PROBE: CTE_SCAN root + two-level real chain below
#   10 multi-key CTE joined complete-key in permuted order (I.20)
#   11 WHERE mix: root bound + CTE-output jump-table filter
#   12 DATE + DECIMAL grouped outputs through the pass-through printer
#   13 VARCHAR MIN/MAX grouped outputs through the pass-through printer
#   P1 rejection: partial-key join to the 2-key CTE
#   P2 rejection: multi-key CTE keyed off two different aliases
#   P3 rejection: non-aggregating (projection-only) CTE body

--source include/have_ndb.inc
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings
--let $suppress_ronsql_cli=yes
--let $strict_diff=yes
--source suite/ronsql_cte/include/cte_schema.inc
--source suite/ronsql_cte/include/cte_data.inc

--disable_query_log
--disable_warnings
CREATE TABLE gckey (
  g_a INT NOT NULL,
  g_b INT NOT NULL,
  PRIMARY KEY USING HASH (g_a)
) ENGINE=NDB;
# (g_a, g_b) pairs against orders' (o_custkey, o_clerk) groups:
# orders row n has custkey MOD(n-1,300)+1 and clerk MOD(n,40)+1 (NULL
# when n%7==0), so (1,2)/(2,3)/(15,16)/(45,6) hit real groups and
# (200,999) misses every group.
INSERT INTO gckey VALUES (1,2),(2,3),(15,16),(45,6),(200,999);
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === gc-1: root real scan + grouped CTE arm ===
let $QUERY=
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_orderkey) AS s
            FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, pc.cnt, pc.s
FROM customer AS c
JOIN pc ON pc.k = c.c_custkey
WHERE c.c_custkey < 8;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_orderkey) AS s
            FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, pc.cnt, pc.s
FROM customer AS c
JOIN pc ON pc.k = c.c_custkey
WHERE c.c_custkey < 8|

--echo
--echo === gc-2: snowflake chain, CTE keyed on a lookup child ===
let $QUERY=
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey)
SELECT o.o_orderkey, cu.c_mktsegment, pc.cnt
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN pc ON pc.k = cu.c_custkey
WHERE o.o_orderkey < 12;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey)
SELECT o.o_orderkey, cu.c_mktsegment, pc.cnt
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN pc ON pc.k = cu.c_custkey
WHERE o.o_orderkey < 12|

--echo
--echo === gc-3: star with a real branch and a CTE branch ===
let $QUERY=
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, n.n_name, pc.cnt
FROM customer AS c
JOIN nation AS n ON n.n_nationkey = c.c_nationkey
JOIN pc ON pc.k = c.c_custkey
WHERE c.c_custkey < 10;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, n.n_name, pc.cnt
FROM customer AS c
JOIN nation AS n ON n.n_nationkey = c.c_nationkey
JOIN pc ON pc.k = c.c_custkey
WHERE c.c_custkey < 10|

--echo
--echo === gc-4: CTE keyed on an INDEX_SCAN child ===
let $QUERY=
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey)
SELECT n.n_name, cu.c_custkey, pc.cnt
FROM nation AS n
JOIN customer AS cu ON cu.c_nationkey = n.n_nationkey
JOIN pc ON pc.k = cu.c_custkey
WHERE n.n_nationkey < 3;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey)
SELECT n.n_name, cu.c_custkey, pc.cnt
FROM nation AS n
JOIN customer AS cu ON cu.c_nationkey = n.n_nationkey
JOIN pc ON pc.k = cu.c_custkey
WHERE n.n_nationkey < 3|

--echo
--echo === gc-5: LEFT JOIN grouped CTE with NULL groups ===
let $QUERY=
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders WHERE o_orderkey < 100 GROUP BY o_custkey)
SELECT c.c_custkey, pc.cnt
FROM customer AS c
LEFT JOIN pc ON pc.k = c.c_custkey
WHERE c.c_custkey > 90 AND c.c_custkey < 110;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders WHERE o_orderkey < 100 GROUP BY o_custkey)
SELECT c.c_custkey, pc.cnt
FROM customer AS c
LEFT JOIN pc ON pc.k = c.c_custkey
WHERE c.c_custkey > 90 AND c.c_custkey < 110|

--echo
--echo === gc-6: two grouped CTEs on the same parent ===
let $QUERY=
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey),
     pk AS (SELECT o_clerk AS k, MAX(o_orderkey) AS mx
            FROM orders GROUP BY o_clerk)
SELECT c.c_custkey, pc.cnt, pk.mx
FROM customer AS c
JOIN pc ON pc.k = c.c_custkey
JOIN pk ON pk.k = c.c_custkey
WHERE c.c_custkey < 6;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey),
     pk AS (SELECT o_clerk AS k, MAX(o_orderkey) AS mx
            FROM orders GROUP BY o_clerk)
SELECT c.c_custkey, pc.cnt, pk.mx
FROM customer AS c
JOIN pc ON pc.k = c.c_custkey
JOIN pk ON pk.k = c.c_custkey
WHERE c.c_custkey < 6|

--echo
--echo === gc-7: two grouped CTEs on different parents (probe) ===
let $QUERY=
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey),
     pn AS (SELECT c_nationkey AS k, COUNT(*) AS nc
            FROM customer GROUP BY c_nationkey)
SELECT o.o_orderkey, pc.cnt, pn.nc
FROM orders AS o
JOIN pc ON pc.k = o.o_custkey
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN pn ON pn.k = cu.c_nationkey
WHERE o.o_orderkey < 10;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey),
     pn AS (SELECT c_nationkey AS k, COUNT(*) AS nc
            FROM customer GROUP BY c_nationkey)
SELECT o.o_orderkey, pc.cnt, pn.nc
FROM orders AS o
JOIN pc ON pc.k = o.o_custkey
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN pn ON pn.k = cu.c_nationkey
WHERE o.o_orderkey < 10|

--echo
--echo === gc-8: real PK lookup below a CTE_LOOKUP (probe) ===
let $QUERY=
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, pc.cnt, c2.c_mktsegment
FROM customer AS c
JOIN pc ON pc.k = c.c_custkey
JOIN customer AS c2 ON c2.c_custkey = pc.k
WHERE c.c_custkey < 6;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, pc.cnt, c2.c_mktsegment
FROM customer AS c
JOIN pc ON pc.k = c.c_custkey
JOIN customer AS c2 ON c2.c_custkey = pc.k
WHERE c.c_custkey < 6|

--echo
--echo === gc-9: CTE root + two-level real chain below (probe) ===
let $QUERY=
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey)
SELECT pc.k, pc.cnt, cu.c_mktsegment, n.n_name
FROM pc
JOIN customer AS cu ON cu.c_custkey = pc.k
JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
WHERE pc.k < 7;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey)
SELECT pc.k, pc.cnt, cu.c_mktsegment, n.n_name
FROM pc
JOIN customer AS cu ON cu.c_custkey = pc.k
JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
WHERE pc.k < 7|

--echo
--echo === gc-10: multi-key CTE, complete key permuted ===
let $QUERY=
WITH pt AS (SELECT o_custkey AS k, o_clerk AS cl, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey, o_clerk)
SELECT g.g_a, g.g_b, pt.cnt
FROM gckey AS g
JOIN pt ON pt.cl = g.g_b AND pt.k = g.g_a;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH pt AS (SELECT o_custkey AS k, o_clerk AS cl, COUNT(*) AS cnt
            FROM orders GROUP BY o_custkey, o_clerk)
SELECT g.g_a, g.g_b, pt.cnt
FROM gckey AS g
JOIN pt ON pt.cl = g.g_b AND pt.k = g.g_a|

--echo
--echo === gc-11: WHERE mix, root bound + CTE-output filter ===
let $QUERY=
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt, MAX(o_orderkey) AS mx
            FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, pc.cnt, pc.mx
FROM customer AS c
JOIN pc ON pc.k = c.c_custkey
WHERE c.c_custkey < 20 AND pc.mx > 1300;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH pc AS (SELECT o_custkey AS k, COUNT(*) AS cnt, MAX(o_orderkey) AS mx
            FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, pc.cnt, pc.mx
FROM customer AS c
JOIN pc ON pc.k = c.c_custkey
WHERE c.c_custkey < 20 AND pc.mx > 1300|

--echo
--echo === gc-12: DATE + DECIMAL grouped outputs ===
let $QUERY=
WITH pc AS (SELECT o_custkey AS k, MIN(o_orderdate) AS d,
                   MAX(o_totalprice) AS p
            FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, pc.d, pc.p
FROM customer AS c
JOIN pc ON pc.k = c.c_custkey
WHERE c.c_custkey < 5;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH pc AS (SELECT o_custkey AS k, MIN(o_orderdate) AS d,
                   MAX(o_totalprice) AS p
            FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, pc.d, pc.p
FROM customer AS c
JOIN pc ON pc.k = c.c_custkey
WHERE c.c_custkey < 5|

--echo
--echo === gc-13: VARCHAR MIN/MAX grouped outputs ===
let $QUERY=
WITH sm AS (SELECT c_nationkey AS k, MIN(c_mktsegment) AS lo,
                   MAX(c_mktsegment) AS hi
            FROM customer GROUP BY c_nationkey)
SELECT n.n_name, sm.lo, sm.hi
FROM nation AS n
JOIN sm ON sm.k = n.n_nationkey
WHERE n.n_nationkey < 4;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sm AS (SELECT c_nationkey AS k, MIN(c_mktsegment) AS lo,
                   MAX(c_mktsegment) AS hi
            FROM customer GROUP BY c_nationkey)
SELECT n.n_name, sm.lo, sm.hi
FROM nation AS n
JOIN sm ON sm.k = n.n_nationkey
WHERE n.n_nationkey < 4|

DELIMITER ;|

--echo
--echo === gc-P1 (rejection): partial-key join to the 2-key CTE ===
# The gate requires complete-key coverage (ExactOrdered/ExactPermuted);
# a partial key falls through to the generic non-aggregate rejection.
# The aggregate path's I.16 root-rewrite never runs here — the gate
# rejects at parse.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH pt AS (SELECT o_custkey AS k, o_clerk AS cl, COUNT(*) AS cnt FROM orders GROUP BY o_custkey, o_clerk) SELECT g.g_a, pt.cnt FROM gckey AS g JOIN pt ON pt.k = g.g_a;' 2>&1

--echo
--echo === gc-P2 (rejection): multi-key CTE keyed off two aliases ===
# The planner requires all ON conditions of one JOIN to reference the
# same parent table; per-key parent sources are a deferred feature.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH pt AS (SELECT o_custkey AS k, o_clerk AS cl, COUNT(*) AS cnt FROM orders GROUP BY o_custkey, o_clerk) SELECT o.o_orderkey, pt.cnt FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey JOIN pt ON pt.k = o.o_custkey AND pt.cl = cu.c_custkey;' 2>&1

--echo
--echo === gc-P3 (rejection): non-aggregating CTE body ===
# CTE bodies must aggregate (analyze_ctes); lifting this is a separate
# kernel-facing feature.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH v AS (SELECT o_orderkey AS k FROM orders) SELECT c.c_custkey FROM customer AS c JOIN v ON v.k = c.c_custkey;' 2>&1

--disable_query_log
DROP TABLE gckey;
--enable_query_log
--source suite/ronsql_cte/include/cte_drop.inc
