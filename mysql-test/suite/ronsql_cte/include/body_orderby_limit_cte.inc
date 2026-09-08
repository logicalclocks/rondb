# body_orderby_limit_cte.inc — ORDER BY / LIMIT in CTE BODIES
# (cte_orderby_limit_plan.md L5), data-driven CTE suite.
#
# FOCUS: kernel-applied top-N semantics — the body's ORDER BY + LIMIT
# decide the materialized SET (single-owner redistribution, bounded
# top-N select + truncation at the CTE finalize barrier, sequenced
# after the AVG divide).  Dropped groups are probe misses: INNER joins
# drop, LEFT JOINs NULL-extend, exactly the empty-CTE semantics.
# Lifts the Phase-0 rejection pinned by ronsql_cte_dd_orderby_limit_reject
# (whose body arms obl-1..3 are retired; the rejection probes that
# REMAIN valid under L4 live here as obc-P1..P7).
#
# SET DETERMINISM: ties at the LIMIT cutoff are arbitrary in MySQL and
# RonSQL alike, so every ORDER BY below is a total order over groups —
# either the key is provably unique per group (SUM(o_totalprice) and
# AVG(o_totalprice) are strictly increasing in o_custkey; MIN(o_orderdate)
# = 1995-01-01 + k days; GROUP BY keys are unique by definition) or a
# unique tie-breaker ends the list (obc-5).  Main SELECTs never ORDER,
# so the default sorted compare of ronsql_compare.inc applies.
#
# ENVELOPE NOTES:
#   * ORDER BY targets must be body OUTPUT columns (GROUP BY columns or
#     aggregate aliases); expressions / positional refs are grammar-level
#     syntax errors, non-output columns a clean prepare error (obc-P1).
#   * ORDER BY without LIMIT is accepted as a validated no-op (the SET
#     is unchanged and derived-table order is unspecified in MySQL too)
#     and emits NOTHING — obc-16 pins the result and the EXPLAIN absence.
#   * Scalar bodies accept ORDER BY / LIMIT >= 1 as no-ops (obc-21);
#     LIMIT 0 on a scalar body is rejected (obc-P3).
#   * Single-row bodies emit the trailer for real: LIMIT 1 keeps the
#     row (obc-19), LIMIT 0 empties it (obc-20 comma probe).
#   * String MIN/MAX outputs cannot be ORDER BY keys in v1 (obc-P2);
#     string GROUP BY keys CAN (obc-4, charset-aware kernel compare).
#
# Case map (obc-N):
#   1  ORDER BY SUM alias DESC LIMIT 3, CTE_SCAN-root pass-through main
#      + EXPLAIN pin of the CTE-definitions "ORDER BY s DESC LIMIT 3"
#   2  ORDER BY SUM alias ASC LIMIT 4, join-agg main re-sums the kept set
#   3  ORDER BY GROUP-BY key DESC LIMIT 5, join-agg main (probe misses)
#   4  VARCHAR GROUP-BY key DESC LIMIT 2, pass-through main
#   5  multi-key: tied first key (SUM(o_shippriority) = 5*MOD(k,5)) +
#      unique tie-breaker k, LIMIT 7
#   6  ORDER BY AVG alias DESC LIMIT 4 (finalize sequencing: divide
#      BEFORE the top-N select)
#   7  ORDER BY MIN(DATE) alias ASC LIMIT 3 (packed-temporal agg key)
#   8  LEFT JOIN over a top-10 CTE: 300 rows, SUM counts only kept
#   9  i26 watermark filter over a LIMIT CTE (parent col > CTE output)
#   10 re-aggregation main GROUP BY over a LIMIT CTE join
#   11 chained CTE grouping over a LIMIT CTE
#   12 LIMIT without ORDER BY: arbitrary kept set, pinned by COUNT +
#      SUM(n) which are invariant (n = 5 for every o_custkey group)
#   13 LIMIT 0, INNER join: empty (COUNT = 0)
#   14 LIMIT 0, LEFT JOIN: all NULL-extended (COUNT = 300, SUM NULL)
#   15 LIMIT 500 > 300 groups: full set (kernel fast path)
#   16 ORDER BY without LIMIT: accepted no-op + EXPLAIN absence pin
#   17 qualified GROUP BY + bare-column-name ORDER BY (TABLE_COLUMN
#      resolution through same_resolved_column)
#   18 bare GROUP BY + qualified ORDER BY (reverse spelling mix)
#   19 single-row body + ORDER BY DESC LIMIT 1 + EXPLAIN pins
#   20 single-row body + LIMIT 0: comma probe drops every parent row
#   21 scalar body + ORDER BY / LIMIT 1: validated no-op
#   P1 non-output ORDER BY column (rejection)
#   P2 string MIN/MAX output as ORDER BY key (rejection)
#   P3 scalar body LIMIT 0 (rejection)
#   P4 9 ORDER BY entries > kernel MAX_ORDER_COLS 8 (rejection)
#   P5 LIMIT above the 26-bit kOpLimit immediate (rejection)
#   P6 OFFSET (syntax error — grammar has no OFFSET)
#   P7 LIMIT x,y comma form (syntax error)
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
# A: top-N core shapes.
# ============================================================

--echo
--echo === obc-1: ORDER BY SUM alias DESC LIMIT 3, pass-through main ===
# SUM(o_totalprice) = 13.37*(5k + 3000): strictly increasing in k, so
# DESC keeps custkeys {300, 299, 298}.
let $QUERY=
WITH t AS (SELECT o_custkey AS k, SUM(o_totalprice) AS s
           FROM orders GROUP BY o_custkey ORDER BY s DESC LIMIT 3)
SELECT k FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_obc_1.out
let $QUERY=
WITH t AS (SELECT o_custkey AS k, SUM(o_totalprice) AS s
           FROM orders GROUP BY o_custkey ORDER BY s DESC LIMIT 3)
SELECT k FROM t;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'ORDER BY s DESC LIMIT 3' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o_custkey AS k, SUM(o_totalprice) AS s
           FROM orders GROUP BY o_custkey ORDER BY s DESC LIMIT 3)
SELECT k FROM t|

--echo
--echo === obc-2: ORDER BY SUM alias ASC LIMIT 4, join-agg main ===
# Kept custkeys {1..4}; the main re-sums the kept groups through the
# aggregate printer (proven DECIMAL-widened display path).
let $QUERY=
WITH t AS (SELECT o_custkey AS k, SUM(o_totalprice) AS s
           FROM orders GROUP BY o_custkey ORDER BY s LIMIT 4)
SELECT COUNT(*) AS c, SUM(t.s) AS ts
FROM customer AS cu JOIN t ON t.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o_custkey AS k, SUM(o_totalprice) AS s
           FROM orders GROUP BY o_custkey ORDER BY s LIMIT 4)
SELECT COUNT(*) AS c, SUM(t.s) AS ts
FROM customer AS cu JOIN t ON t.k = cu.c_custkey|

--echo
--echo === obc-3: ORDER BY GROUP-BY key DESC LIMIT 5, join-agg main ===
# Kept custkeys {296..300}; every non-kept customer's CTE_LOOKUP is a
# probe miss (GROUP_NOT_FOUND) and the INNER join drops the row.
let $QUERY=
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM orders GROUP BY o_custkey ORDER BY k DESC LIMIT 5)
SELECT COUNT(*) AS c, SUM(t.n) AS sn
FROM customer AS cu JOIN t ON t.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM orders GROUP BY o_custkey ORDER BY k DESC LIMIT 5)
SELECT COUNT(*) AS c, SUM(t.n) AS sn
FROM customer AS cu JOIN t ON t.k = cu.c_custkey|

--echo
--echo === obc-4: VARCHAR GROUP-BY key DESC LIMIT 2, pass-through main ===
# 5 segments, 60 customers each; DESC keeps {MACHINERY, HOUSEHOLD}.
# The kernel order compare on a string GROUP BY key goes through the
# charset-aware GBColTypeInfo::cmpFn (all-uppercase ASCII data, so the
# collation order is charset-independent).
let $QUERY=
WITH t AS (SELECT c_mktsegment AS seg, COUNT(*) AS n
           FROM customer GROUP BY c_mktsegment ORDER BY seg DESC LIMIT 2)
SELECT seg, n FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT c_mktsegment AS seg, COUNT(*) AS n
           FROM customer GROUP BY c_mktsegment ORDER BY seg DESC LIMIT 2)
SELECT seg, n FROM t|

--echo
--echo === obc-5: tied first key + unique tie-breaker, LIMIT 7 ===
# SUM(o_shippriority) = 5*MOD(k,5) (all five orders of custkey k share
# MOD(k,5)) — 60-way ties on the first key; the trailing unique k makes
# the kept set deterministic: {4, 9, 14, 19, 24, 29, 34}.
let $QUERY=
WITH t AS (SELECT o_custkey AS k, SUM(o_shippriority) AS sp
           FROM orders GROUP BY o_custkey ORDER BY sp DESC, k LIMIT 7)
SELECT k FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o_custkey AS k, SUM(o_shippriority) AS sp
           FROM orders GROUP BY o_custkey ORDER BY sp DESC, k LIMIT 7)
SELECT k FROM t|

--echo
--echo === obc-6: ORDER BY AVG alias DESC LIMIT 4 ===
# AVG(o_totalprice) = 13.37*(5k+3000)/5: strictly increasing, keeps
# {300, 299, 298, 297}.  Pins the finalize sequencing: the AVG divide
# runs BEFORE the top-N select, so the order key is the divided DOUBLE.
let $QUERY=
WITH t AS (SELECT o_custkey AS k, AVG(o_totalprice) AS a
           FROM orders GROUP BY o_custkey ORDER BY a DESC LIMIT 4)
SELECT k FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o_custkey AS k, AVG(o_totalprice) AS a
           FROM orders GROUP BY o_custkey ORDER BY a DESC LIMIT 4)
SELECT k FROM t|

--echo
--echo === obc-7: ORDER BY MIN(DATE) alias ASC LIMIT 3 ===
# MIN(o_orderdate) per custkey k is 1995-01-01 + k days (unique), so
# ASC keeps {1, 2, 3}.  The packed-temporal aggregate slot orders as
# its Bigunsigned day number; d is decoded to a date on display.
let $QUERY=
WITH t AS (SELECT o_custkey AS k, MIN(o_orderdate) AS d
           FROM orders GROUP BY o_custkey ORDER BY d LIMIT 3)
SELECT k, d FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o_custkey AS k, MIN(o_orderdate) AS d
           FROM orders GROUP BY o_custkey ORDER BY d LIMIT 3)
SELECT k, d FROM t|

# ============================================================
# B: consumers of a LIMIT CTE.
# ============================================================

--echo
--echo === obc-8: LEFT JOIN over a top-10 CTE ===
# Non-kept custkeys NULL-extend: 300 output rows feed the aggregate,
# SUM(t.n) counts only the 10 kept groups (5 orders each -> 50).
let $QUERY=
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_totalprice) AS s
           FROM orders GROUP BY o_custkey ORDER BY s DESC LIMIT 10)
SELECT COUNT(*) AS c, SUM(t.n) AS sn
FROM customer AS cu LEFT JOIN t ON t.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_totalprice) AS s
           FROM orders GROUP BY o_custkey ORDER BY s DESC LIMIT 10)
SELECT COUNT(*) AS c, SUM(t.n) AS sn
FROM customer AS cu LEFT JOIN t ON t.k = cu.c_custkey|

--echo
--echo === obc-9: i26 watermark filter over a LIMIT CTE ===
# Kept custkeys {1..100}; the ancestor-vs-CTE conjunct
# cu.c_custkey > t.sp routes to the CTE op's jump-table filter with a
# linked parent projection (INT vs Bigunsigned typed registers).
# sp = 5*MOD(k,5), so the count is 20+19+18+17+16 = 90.
let $QUERY=
WITH t AS (SELECT o_custkey AS k, SUM(o_shippriority) AS sp
           FROM orders GROUP BY o_custkey ORDER BY k LIMIT 100)
SELECT COUNT(*) AS c
FROM customer AS cu JOIN t ON t.k = cu.c_custkey
WHERE cu.c_custkey > t.sp;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o_custkey AS k, SUM(o_shippriority) AS sp
           FROM orders GROUP BY o_custkey ORDER BY k LIMIT 100)
SELECT COUNT(*) AS c
FROM customer AS cu JOIN t ON t.k = cu.c_custkey
WHERE cu.c_custkey > t.sp|

--echo
--echo === obc-10: re-aggregation main GROUP BY over a LIMIT CTE join ===
# Kept custkeys {1..50} regroup by nationkey (MOD(k,25)): 25 groups of
# 2 customers, 10 orders each.
let $QUERY=
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM orders GROUP BY o_custkey ORDER BY k LIMIT 50)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(t.n) AS sn
FROM customer AS cu JOIN t ON t.k = cu.c_custkey
GROUP BY cu.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM orders GROUP BY o_custkey ORDER BY k LIMIT 50)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(t.n) AS sn
FROM customer AS cu JOIN t ON t.k = cu.c_custkey
GROUP BY cu.c_nationkey|

--echo
--echo === obc-11: chained CTE grouping over a LIMIT CTE ===
# a keeps the top-20 sums (custkeys 281..300); b re-groups a by its
# unique key (a CTE_SCAN body root over the LIMIT CTE — non-owner
# nodes hold empty states after constant-owner redistribution).
let $QUERY=
WITH a AS (SELECT o_custkey AS k, SUM(o_totalprice) AS s
           FROM orders GROUP BY o_custkey ORDER BY s DESC LIMIT 20),
     b AS (SELECT k, SUM(s) AS t2 FROM a GROUP BY k)
SELECT COUNT(*) AS c, SUM(b.t2) AS tt
FROM customer AS cu JOIN b ON b.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (SELECT o_custkey AS k, SUM(o_totalprice) AS s
           FROM orders GROUP BY o_custkey ORDER BY s DESC LIMIT 20),
     b AS (SELECT k, SUM(s) AS t2 FROM a GROUP BY k)
SELECT COUNT(*) AS c, SUM(b.t2) AS tt
FROM customer AS cu JOIN b ON b.k = cu.c_custkey|

--echo
--echo === obc-12: LIMIT without ORDER BY, invariant aggregates ===
# The kept 10 groups are ARBITRARY (MySQL semantics too), so the main
# pins only invariants: COUNT(*) = 10 and SUM(n) = 50 (every o_custkey
# group has exactly 5 orders).
let $QUERY=
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM orders GROUP BY o_custkey LIMIT 10)
SELECT COUNT(*) AS c, SUM(t.n) AS sn FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM orders GROUP BY o_custkey LIMIT 10)
SELECT COUNT(*) AS c, SUM(t.n) AS sn FROM t|

--echo
--echo === obc-13: LIMIT 0, INNER join: empty ===
let $QUERY=
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM orders GROUP BY o_custkey ORDER BY k LIMIT 0)
SELECT COUNT(*) AS c
FROM customer AS cu JOIN t ON t.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM orders GROUP BY o_custkey ORDER BY k LIMIT 0)
SELECT COUNT(*) AS c
FROM customer AS cu JOIN t ON t.k = cu.c_custkey|

--echo
--echo === obc-14: LIMIT 0, LEFT JOIN: all NULL-extended ===
let $QUERY=
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM orders GROUP BY o_custkey ORDER BY k LIMIT 0)
SELECT COUNT(*) AS c, SUM(t.n) AS sn
FROM customer AS cu LEFT JOIN t ON t.k = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM orders GROUP BY o_custkey ORDER BY k LIMIT 0)
SELECT COUNT(*) AS c, SUM(t.n) AS sn
FROM customer AS cu LEFT JOIN t ON t.k = cu.c_custkey|

--echo
--echo === obc-15: LIMIT 500 > 300 groups: full set (fast path) ===
let $QUERY=
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM orders GROUP BY o_custkey ORDER BY k DESC LIMIT 500)
SELECT COUNT(*) AS c, SUM(t.n) AS sn FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM orders GROUP BY o_custkey ORDER BY k DESC LIMIT 500)
SELECT COUNT(*) AS c, SUM(t.n) AS sn FROM t|

--echo
--echo === obc-16: ORDER BY without LIMIT: accepted no-op ===
# The SET is unchanged and derived-table order is unspecified in MySQL
# too, so nothing is emitted — the EXPLAIN absence pin below proves the
# CTE stays on the normal hash-distributed redistribution.
let $QUERY=
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_totalprice) AS s
           FROM orders GROUP BY o_custkey ORDER BY s DESC)
SELECT COUNT(*) AS c, SUM(t.n) AS sn FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_obc_16.out
let $QUERY=
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_totalprice) AS s
           FROM orders GROUP BY o_custkey ORDER BY s DESC)
SELECT COUNT(*) AS c, SUM(t.n) AS sn FROM t;|
--source suite/ronsql/include/ronsql_explain.inc
--error 1
--exec grep -qF 'LIMIT' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_totalprice) AS s
           FROM orders GROUP BY o_custkey ORDER BY s DESC)
SELECT COUNT(*) AS c, SUM(t.n) AS sn FROM t|

# ============================================================
# C: spelling variants (same_resolved_column unification).
# ============================================================

--echo
--echo === obc-17: qualified GROUP BY + bare-column-name ORDER BY ===
# ORDER BY names the COLUMN (o_custkey), not the alias k: the entry
# stays TABLE_COLUMN and must unify with the qualified GROUP BY
# spelling through resolved-column identity.
let $QUERY=
WITH t AS (SELECT o.o_custkey AS k, SUM(o.o_totalprice) AS s
           FROM orders AS o GROUP BY o.o_custkey
           ORDER BY o_custkey DESC LIMIT 3)
SELECT k FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o.o_custkey AS k, SUM(o.o_totalprice) AS s
           FROM orders AS o GROUP BY o.o_custkey
           ORDER BY o_custkey DESC LIMIT 3)
SELECT k FROM t|

--echo
--echo === obc-18: bare GROUP BY + qualified ORDER BY ===
let $QUERY=
WITH t AS (SELECT o_custkey AS k, SUM(o_totalprice) AS s
           FROM orders AS o GROUP BY o_custkey
           ORDER BY o.o_custkey LIMIT 3)
SELECT k FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (SELECT o_custkey AS k, SUM(o_totalprice) AS s
           FROM orders AS o GROUP BY o_custkey
           ORDER BY o.o_custkey LIMIT 3)
SELECT k FROM t|

# ============================================================
# D: one-row bodies (single-row and scalar).
# ============================================================

--echo
--echo === obc-19: single-row body + ORDER BY DESC LIMIT 1 ===
# The trailer is emitted for real (CTE_SINGLE_ROW | CTE_LIMIT); on a
# <= 1-row body LIMIT 1 keeps the row.  Order key p is the second
# output (a DECIMAL virt-PK column).
let $QUERY=
WITH r AS (SELECT o_custkey AS a, o_totalprice AS p
           FROM orders WHERE o_orderkey = 77 ORDER BY p DESC LIMIT 1)
SELECT c.c_custkey, r.p
FROM customer AS c JOIN r ON r.a = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_obc_19.out
let $QUERY=
WITH r AS (SELECT o_custkey AS a, o_totalprice AS p
           FROM orders WHERE o_orderkey = 77 ORDER BY p DESC LIMIT 1)
SELECT c.c_custkey, r.p
FROM customer AS c JOIN r ON r.a = c.c_custkey;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[single-row key lookup body]' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY p DESC LIMIT 1' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS a, o_totalprice AS p
           FROM orders WHERE o_orderkey = 77 ORDER BY p DESC LIMIT 1)
SELECT c.c_custkey, r.p
FROM customer AS c JOIN r ON r.a = c.c_custkey|

--echo
--echo === obc-20: single-row body + LIMIT 0: comma probe drops all ===
# The kernel truncates the one group; every comma-join existence probe
# misses (the srb-20 row-absent semantics, reached via LIMIT).
let $QUERY=
WITH r AS (SELECT o_custkey AS a
           FROM orders WHERE o_orderkey = 77 LIMIT 0)
SELECT COUNT(*) AS c FROM customer AS c2, r;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH r AS (SELECT o_custkey AS a
           FROM orders WHERE o_orderkey = 77 LIMIT 0)
SELECT COUNT(*) AS c FROM customer AS c2, r|

--echo
--echo === obc-21: scalar body + ORDER BY / LIMIT 1: validated no-op ===
# One-row scalar result: ORDER BY mx / LIMIT 1 are provable no-ops,
# accepted and dropped (nothing emitted, no CTE_LIMIT flag).  The
# watermark compare uses the INT key column: a DECIMAL real side is
# the deliberately-rejected i26 col-vs-col shape (sc-P4).
let $QUERY=
WITH m AS (SELECT MAX(o_orderkey) AS mx FROM orders
           ORDER BY mx DESC LIMIT 1)
SELECT COUNT(*) AS c FROM orders AS o2, m
WHERE o2.o_orderkey < m.mx;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH m AS (SELECT MAX(o_orderkey) AS mx FROM orders
           ORDER BY mx DESC LIMIT 1)
SELECT COUNT(*) AS c FROM orders AS o2, m
WHERE o2.o_orderkey < m.mx|

DELIMITER ;|

# ============================================================
# P: rejection probes (prepare-time errors, no data touched).
# ============================================================

--echo
--echo === obc-P1 (rejection): non-output ORDER BY column ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey ORDER BY o_totalprice DESC LIMIT 3) SELECT COUNT(*) AS c FROM customer AS cu JOIN t ON t.k = cu.c_custkey;' 2>&1

--echo
--echo === obc-P2 (rejection): string MIN/MAX output as ORDER BY key ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH t AS (SELECT c_nationkey AS nk, MIN(c_mktsegment) AS m FROM customer GROUP BY c_nationkey ORDER BY m LIMIT 3) SELECT COUNT(*) AS c FROM customer AS cu JOIN t ON t.nk = cu.c_nationkey;' 2>&1

--echo
--echo === obc-P3 (rejection): LIMIT 0 on a scalar body ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH m AS (SELECT MAX(o_totalprice) AS mx FROM orders LIMIT 0) SELECT COUNT(*) AS c FROM customer AS cu, m WHERE cu.c_acctbal < m.mx;' 2>&1

--echo
--echo === obc-P4 (rejection): 9 ORDER BY entries (kernel max is 8) ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey ORDER BY n,n,n,n,n,n,n,n,n LIMIT 2) SELECT COUNT(*) AS c FROM customer AS cu JOIN t ON t.k = cu.c_custkey;' 2>&1

--echo
--echo === obc-P5 (rejection): LIMIT above the 26-bit immediate ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey ORDER BY k LIMIT 67108864) SELECT COUNT(*) AS c FROM customer AS cu JOIN t ON t.k = cu.c_custkey;' 2>&1

--echo
--echo === obc-P6 (syntax): OFFSET is not in the grammar ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey ORDER BY k LIMIT 5 OFFSET 2) SELECT COUNT(*) AS c FROM customer AS cu JOIN t ON t.k = cu.c_custkey;' 2>&1

--echo
--echo === obc-P7 (syntax): LIMIT x,y comma form is not in the grammar ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH t AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey ORDER BY k LIMIT 2,5) SELECT COUNT(*) AS c FROM customer AS cu JOIN t ON t.k = cu.c_custkey;' 2>&1

--source suite/ronsql_cte/include/cte_drop.inc
