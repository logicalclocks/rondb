# body_single_row_cte.inc — single-row key-lookup CTE bodies
# (cte_single_row_kernel_plan.md), data-driven suite.
#
# A CTE body with no aggregate functions and no GROUP BY is accepted
# when its WHERE binds every primary key column of its single source
# table by equality with a constant (<= 1 row).  The body is emitted
# with the kernel's CTE_SINGLE_ROW mode: every projected column is a
# GROUP BY column with ZERO aggregate slots (the materialized "group"
# IS the row, stored at the constant DBTC-node owner), and consumers
# bind ANY SUBSET of the outputs via subset-key CTE_LOOKUP probes —
# including NONE: a comma cross join is a pure existence probe, so an
# EMPTY body gives the exact MySQL empty result on every consumer
# style (INNER/comma drop, LEFT NULL-extends).  No AST rewrite, no
# aggregate-machinery detour.
#
# Local table srbk provides the two-column join target for the
# multi-output key cases (analog of gckey in the
# passthrough_groupby_cte family).
#
# Case map (srb-N):
#   1  single-output body, keyed join consumer (pass-through)
#      + EXPLAIN pins: [single-row key lookup body], CTE_LOOKUP child
#   2  aggregate twin of 1
#   3  row absent (pk = -1): empty result
#   4  two-output body, both outputs bound via srbk
#   5  NULL output value in the join key (o_clerk NULL for pk 77;
#      srbk carries (77,38) so the NULL is what kills the match)
#   6  NATIVE SUBSET key: bind only the SECOND output (no I.16 root
#      rewrite — the CTE stays a CTE_LOOKUP child) + EXPLAIN pin
#   7  LEFT JOIN consumer, row present (mixed match / NULL-extend)
#   8  LEFT JOIN consumer, row absent (all rows NULL-extended)
#   9  residual conjunct beside the PK equality (accepts the row)
#   10 residual conjunct rejects the row: empty CTE
#   11 multi-column PK body (lineitem) + subset key on one output
#   12 VARCHAR output as the bound key
#   13 CTE as FROM root: VARCHAR + DECIMAL outputs through the
#      pass-through printer (CTE_SCAN path) + EXPLAIN pin
#   14 DATE output as a CTE_LOOKUP join key (aggregate path)
#   15 two single-row CTEs on one parent
#   16 chained: an aggregating CTE body joins a single-row CTE
#   17 aggregate consumer GROUP BY over the CTE output
#   18 COMMA cross join + watermark compare (aggregate main)
#   19 COMMA cross join + pure projection (pass-through)
#   20 COMMA cross join, row-absent body: EMPTY result (the money
#      case — exact MySQL comma-join-with-empty semantics)
#   21 COMMA cross join, row-absent body, aggregate main: COUNT = 0
#   22 CTE as FROM root with a WHERE filter on its outputs
#
# Probes:
#   P1 no WHERE: not a candidate; the parse gate's coverage message
#      (gc-P3 twin) is unchanged
#   P2 WHERE does not touch the PK -> enforcement message
#   P3 PK bound by range, not equality -> enforcement message
#   P4 PK bound to a column-containing expression -> enforcement
#      message (constant-side check)
#   P5 expression output in the body -> existing rejection
#   P6 ORDER BY inside the body still rejected (obl message)
#   P7 duplicate output columns (FROM-root consumer): falls through to
#      analyze_ctes' "must contain at least one aggregate function."
#      (first pin of that message)
#   P8 HAVING inside the body: same fall-through rejection
#   P9 unknown column as a lookup key -> the new single-row
#      WrongColumns message
#   P10 no-FROM qualified reference -> single-row CTEs excluded from
#       the scalar auto-FROM convenience (clean message)
#   P11 DATE watermark compare via comma join -> clean rejection (the
#       cross-column jump-table filter's typed-register set has no
#       DATE arm yet; DATE EQUI-joins work, see srb-14)

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
CREATE TABLE srbk (
  s_a INT NOT NULL,
  s_b INT NOT NULL,
  PRIMARY KEY USING HASH (s_a)
) ENGINE=NDB;
# (s_a, s_b) pairs against single-row bodies over orders: orderkey 45
# has (o_custkey, o_clerk) = (45, 6) so (45,6) matches; orderkey 77
# has clerk NULL (77 % 7 == 0) so (77,38) exists only to prove the
# NULL kills the 2-key match; (200,999) matches nothing.
INSERT INTO srbk VALUES (1,2),(2,3),(45,6),(77,38),(200,999);
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === srb-1: single-output body, keyed join consumer ===
let $QUERY=
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77)
SELECT c.c_custkey, c.c_nationkey
FROM customer AS c
JOIN r ON r.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_srb_1.out
let $QUERY=
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77)
SELECT c.c_custkey, c.c_nationkey
FROM customer AS c
JOIN r ON r.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[single-row key lookup body]' $EXPLAIN_FILE
--exec grep -qF 'CTE_LOOKUP CTE:r AS r' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77)
SELECT c.c_custkey, c.c_nationkey
FROM customer AS c
JOIN r ON r.k = c.c_custkey|

--echo
--echo === srb-2: aggregate twin of srb-1 ===
let $QUERY=
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77)
SELECT COUNT(*), MAX(c.c_nationkey)
FROM customer AS c
JOIN r ON r.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77)
SELECT COUNT(*), MAX(c.c_nationkey)
FROM customer AS c
JOIN r ON r.k = c.c_custkey|

--echo
--echo === srb-3: row absent (pk = -1) gives an empty result ===
let $QUERY=
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = -1)
SELECT c.c_custkey
FROM customer AS c
JOIN r ON r.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = -1)
SELECT c.c_custkey
FROM customer AS c
JOIN r ON r.k = c.c_custkey|

--echo
--echo === srb-4: two-output body, both outputs bound ===
let $QUERY=
WITH r AS (SELECT o_custkey AS k, o_clerk AS cl FROM orders
           WHERE o_orderkey = 45)
SELECT g.s_a, g.s_b
FROM srbk AS g
JOIN r ON r.k = g.s_a AND r.cl = g.s_b;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k, o_clerk AS cl FROM orders
           WHERE o_orderkey = 45)
SELECT g.s_a, g.s_b
FROM srbk AS g
JOIN r ON r.k = g.s_a AND r.cl = g.s_b|

--echo
--echo === srb-5: NULL output value in the join key kills the match ===
let $QUERY=
WITH r AS (SELECT o_custkey AS k, o_clerk AS cl FROM orders
           WHERE o_orderkey = 77)
SELECT g.s_a, g.s_b
FROM srbk AS g
JOIN r ON r.k = g.s_a AND r.cl = g.s_b;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k, o_clerk AS cl FROM orders
           WHERE o_orderkey = 77)
SELECT g.s_a, g.s_b
FROM srbk AS g
JOIN r ON r.k = g.s_a AND r.cl = g.s_b|

--echo
--echo === srb-6: native subset key on the second output only ===
# Binds output position 1 (cl) and not position 0 (k): subset keys
# are native to the single-row mode — no I.16 root rewrite, the CTE
# stays a CTE_LOOKUP child.
let $QUERY=
WITH r AS (SELECT o_custkey AS k, o_clerk AS cl FROM orders
           WHERE o_orderkey = 45)
SELECT g.s_a, g.s_b
FROM srbk AS g
JOIN r ON r.cl = g.s_b;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_srb_6.out
let $QUERY=
WITH r AS (SELECT o_custkey AS k, o_clerk AS cl FROM orders
           WHERE o_orderkey = 45)
SELECT g.s_a, g.s_b
FROM srbk AS g
JOIN r ON r.cl = g.s_b;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'CTE_LOOKUP CTE:r AS r' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k, o_clerk AS cl FROM orders
           WHERE o_orderkey = 45)
SELECT g.s_a, g.s_b
FROM srbk AS g
JOIN r ON r.cl = g.s_b|

--echo
--echo === srb-7: LEFT JOIN consumer, row present ===
let $QUERY=
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77)
SELECT c.c_custkey, r.k
FROM customer AS c
LEFT JOIN r ON r.k = c.c_custkey
WHERE c.c_custkey < 80;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77)
SELECT c.c_custkey, r.k
FROM customer AS c
LEFT JOIN r ON r.k = c.c_custkey
WHERE c.c_custkey < 80|

--echo
--echo === srb-8: LEFT JOIN consumer, row absent (all NULL-extended) ===
let $QUERY=
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = -1)
SELECT c.c_custkey, r.k
FROM customer AS c
LEFT JOIN r ON r.k = c.c_custkey
WHERE c.c_custkey < 5;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = -1)
SELECT c.c_custkey, r.k
FROM customer AS c
LEFT JOIN r ON r.k = c.c_custkey
WHERE c.c_custkey < 5|

--echo
--echo === srb-9: residual conjunct beside the PK equality ===
let $QUERY=
WITH r AS (SELECT o_custkey AS k FROM orders
           WHERE o_orderkey = 77 AND o_shippriority >= 0)
SELECT c.c_custkey
FROM customer AS c
JOIN r ON r.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k FROM orders
           WHERE o_orderkey = 77 AND o_shippriority >= 0)
SELECT c.c_custkey
FROM customer AS c
JOIN r ON r.k = c.c_custkey|

--echo
--echo === srb-10: residual conjunct rejects the row (empty CTE) ===
# o_custkey is always >= 1 in the data, so the residual is false for
# the (existing) pk = 77 row: zero rows materialize, empty result.
let $QUERY=
WITH r AS (SELECT o_custkey AS k FROM orders
           WHERE o_orderkey = 77 AND o_custkey < 0)
SELECT c.c_custkey
FROM customer AS c
JOIN r ON r.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k FROM orders
           WHERE o_orderkey = 77 AND o_custkey < 0)
SELECT c.c_custkey
FROM customer AS c
JOIN r ON r.k = c.c_custkey|

--echo
--echo === srb-11: multi-column PK body + subset key ===
let $QUERY=
WITH r AS (SELECT l_partkey AS pk2, l_quantity AS q FROM lineitem
           WHERE l_orderkey = 10 AND l_linenumber = 1)
SELECT p.p_name, r.q
FROM part AS p
JOIN r ON r.pk2 = p.p_partkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT l_partkey AS pk2, l_quantity AS q FROM lineitem
           WHERE l_orderkey = 10 AND l_linenumber = 1)
SELECT p.p_name, r.q
FROM part AS p
JOIN r ON r.pk2 = p.p_partkey|

--echo
--echo === srb-12: VARCHAR output as the bound key ===
let $QUERY=
WITH r AS (SELECT c_mktsegment AS seg FROM customer WHERE c_custkey = 9)
SELECT c2.c_custkey
FROM customer AS c2
JOIN r ON r.seg = c2.c_mktsegment
WHERE c2.c_custkey < 20;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT c_mktsegment AS seg FROM customer WHERE c_custkey = 9)
SELECT c2.c_custkey
FROM customer AS c2
JOIN r ON r.seg = c2.c_mktsegment
WHERE c2.c_custkey < 20|

--echo
--echo === srb-13: FROM-root consumer, VARCHAR + DECIMAL outputs ===
let $QUERY=
WITH r AS (SELECT c_mktsegment AS seg, c_acctbal AS bal FROM customer
           WHERE c_custkey = 9)
SELECT seg, bal FROM r;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_srb_13.out
let $QUERY=
WITH r AS (SELECT c_mktsegment AS seg, c_acctbal AS bal FROM customer
           WHERE c_custkey = 9)
SELECT seg, bal FROM r;|
--source suite/ronsql/include/ronsql_explain.inc
# A single-op FROM-root query prints no join-plan tree — pin the CTE
# definitions section instead.
--exec grep -qF '[single-row key lookup body]' $EXPLAIN_FILE
--exec grep -qF 'Outputs: seg, bal' $EXPLAIN_FILE
--exec grep -qF 'Body root: TABLE_SCAN' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT c_mktsegment AS seg, c_acctbal AS bal FROM customer
           WHERE c_custkey = 9)
SELECT seg, bal FROM r|

--echo
--echo === srb-14: DATE output as a CTE_LOOKUP join key ===
let $QUERY=
WITH r AS (SELECT o_orderdate AS d FROM orders WHERE o_orderkey = 30)
SELECT COUNT(*)
FROM orders AS o2
JOIN r ON r.d = o2.o_orderdate;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_orderdate AS d FROM orders WHERE o_orderkey = 30)
SELECT COUNT(*)
FROM orders AS o2
JOIN r ON r.d = o2.o_orderdate|

--echo
--echo === srb-15: two single-row CTEs on one parent ===
let $QUERY=
WITH r1 AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77),
     r2 AS (SELECT c_nationkey AS nk FROM customer WHERE c_custkey = 45)
SELECT c.c_custkey, c.c_nationkey
FROM customer AS c
JOIN r1 ON r1.k = c.c_custkey
JOIN r2 ON r2.nk = c.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r1 AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77),
     r2 AS (SELECT c_nationkey AS nk FROM customer WHERE c_custkey = 45)
SELECT c.c_custkey, c.c_nationkey
FROM customer AS c
JOIN r1 ON r1.k = c.c_custkey
JOIN r2 ON r2.nk = c.c_nationkey|

--echo
--echo === srb-16: aggregating CTE body joins a single-row CTE ===
let $QUERY=
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77),
     s AS (SELECT o2.o_custkey AS k2, COUNT(*) AS cnt
           FROM orders AS o2 JOIN r ON r.k = o2.o_custkey
           GROUP BY o2.o_custkey)
SELECT c.c_custkey, s.cnt
FROM customer AS c
JOIN s ON s.k2 = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77),
     s AS (SELECT o2.o_custkey AS k2, COUNT(*) AS cnt
           FROM orders AS o2 JOIN r ON r.k = o2.o_custkey
           GROUP BY o2.o_custkey)
SELECT c.c_custkey, s.cnt
FROM customer AS c
JOIN s ON s.k2 = c.c_custkey|

--echo
--echo === srb-17: aggregate consumer GROUP BY over the CTE output ===
let $QUERY=
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77)
SELECT r.k, COUNT(*)
FROM orders AS o2
JOIN r ON r.k = o2.o_custkey
GROUP BY r.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77)
SELECT r.k, COUNT(*)
FROM orders AS o2
JOIN r ON r.k = o2.o_custkey
GROUP BY r.k|

--echo
--echo === srb-18: comma cross join + watermark compare (aggregate) ===
# The natural single-row spelling of the watermark pattern: the comma
# join is a zero-key existence probe, the compare rides the CTE op's
# jump-table filter.
let $QUERY=
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77)
SELECT COUNT(*)
FROM orders AS o2, r
WHERE o2.o_custkey > r.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77)
SELECT COUNT(*)
FROM orders AS o2, r
WHERE o2.o_custkey > r.k|

--echo
--echo === srb-19: comma cross join + pure projection ===
let $QUERY=
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77)
SELECT o2.o_orderkey, r.k
FROM orders AS o2, r
WHERE o2.o_orderkey < 5;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77)
SELECT o2.o_orderkey, r.k
FROM orders AS o2, r
WHERE o2.o_orderkey < 5|

--echo
--echo === srb-20: comma cross join, row-absent body: EMPTY result ===
# The money case: MySQL's cross join with an empty CTE returns
# nothing; the zero-key existence probe MISSES on the empty state and
# the INNER semantics drop every parent row — no guard tricks.
let $QUERY=
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = -1)
SELECT o2.o_orderkey, r.k
FROM orders AS o2, r
WHERE o2.o_orderkey < 5;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = -1)
SELECT o2.o_orderkey, r.k
FROM orders AS o2, r
WHERE o2.o_orderkey < 5|

--echo
--echo === srb-21: comma cross join, row-absent body, aggregate ===
let $QUERY=
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = -1)
SELECT COUNT(*)
FROM orders AS o2, r
WHERE o2.o_custkey > r.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = -1)
SELECT COUNT(*)
FROM orders AS o2, r
WHERE o2.o_custkey > r.k|

--echo
--echo === srb-22: FROM root with a WHERE filter on the outputs ===
let $QUERY=
WITH r AS (SELECT o_custkey AS k, o_shippriority AS sp FROM orders
           WHERE o_orderkey = 77)
SELECT k FROM r WHERE sp >= 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS k, o_shippriority AS sp FROM orders
           WHERE o_orderkey = 77)
SELECT k FROM r WHERE sp >= 0|

DELIMITER ;|

--echo
--echo === srb-P1 (rejection): no WHERE - not a single-row candidate ===
# Same shape as gc-P3: without a WHERE clause there is no single-row
# guarantee, so the CTE is not marked and the parse gate's coverage
# message is unchanged.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH v AS (SELECT o_orderkey AS k FROM orders) SELECT c.c_custkey FROM customer AS c JOIN v ON v.k = c.c_custkey;' 2>&1

--echo
--echo === srb-P2 (rejection): WHERE does not touch the PK ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH r AS (SELECT o_orderdate AS d FROM orders WHERE o_custkey = 5) SELECT c.c_custkey FROM customer AS c JOIN r ON r.d = c.c_custkey;' 2>&1

--echo
--echo === srb-P3 (rejection): PK bound by range, not equality ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey > 77) SELECT c.c_custkey FROM customer AS c JOIN r ON r.k = c.c_custkey;' 2>&1

--echo
--echo === srb-P4 (rejection): PK bound to a column-containing expression ===
# pk = other_col + 1 can hold on several rows, so it must not count as
# single-row coverage (the bare col-vs-col form is caught earlier by
# check_no_cte_body_col_vs_col).
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = o_custkey + 1) SELECT c.c_custkey FROM customer AS c JOIN r ON r.k = c.c_custkey;' 2>&1

--echo
--echo === srb-P5 (rejection): expression output in the body ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH r AS (SELECT o_custkey + 1 AS k FROM orders WHERE o_orderkey = 77) SELECT c.c_custkey FROM customer AS c JOIN r ON r.k = c.c_custkey;' 2>&1

--echo
--echo === srb-P6 (rejection): ORDER BY inside the body still rejected ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77 ORDER BY o_custkey) SELECT c.c_custkey FROM customer AS c JOIN r ON r.k = c.c_custkey;' 2>&1

--echo
--echo === srb-P7 (rejection): duplicate output columns ===
# Not a candidate (virt-PK aliasing is deferred); the FROM-root
# consumer passes the parse gate, so this is the first pin of
# analyze_ctes' "must contain at least one aggregate function."
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH r AS (SELECT o_custkey AS a, o_custkey AS b FROM orders WHERE o_orderkey = 77) SELECT a, b FROM r;' 2>&1

--echo
--echo === srb-P8 (rejection): HAVING inside the body ===
# HAVING is parsed but not applied in CTE bodies, so a HAVING-carrying
# body must not silently become a single-row CTE.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH r AS (SELECT o_custkey AS a FROM orders WHERE o_orderkey = 77 HAVING a > 0) SELECT a FROM r;' 2>&1

--echo
--echo === srb-P9 (rejection): unknown column as a lookup key ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77) SELECT c.c_custkey FROM customer AS c JOIN r ON r.nosuch = c.c_custkey;' 2>&1

--echo
--echo === srb-P10 (rejection): no-FROM qualified reference ===
# The scalar-CTE auto-FROM convenience is deliberately not extended to
# single-row CTEs; the message points at the explicit comma join.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = 77) SELECT MAX(r.k);' 2>&1

--echo
--echo === srb-P11 (rejection): DATE watermark compare via comma join ===
# The cross-column jump-table filter's typed-register set has no DATE
# arm yet; DATE equi-joins work via subset keys (srb-14).
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH r AS (SELECT o_orderdate AS d FROM orders WHERE o_orderkey = 77) SELECT COUNT(*) FROM orders AS o2, r WHERE o2.o_orderdate > r.d;' 2>&1

--disable_query_log
DROP TABLE srbk;
--enable_query_log
--source suite/ronsql_cte/include/cte_drop.inc
