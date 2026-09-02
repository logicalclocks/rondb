# body_main_root_index.inc — main-query root index scan family
# (join_root_index_scan_plan.md), data-driven CTE suite.
#
# Asserts BOTH result values (via ronsql_compare.inc) AND query plans
# (via ronsql_explain.inc + a grep -qF on a stable EXPLAIN substring).
#
# Focus: scan-config-selected index scans on the MAIN-QUERY ROOT of a
# join query —
#   * root secondary-index equality / range -> [ROOT] INDEX_SCAN using idx_...
#   * bound + non-indexable residual conjunct routing
#   * PRIMARY ordered-index range on the root (fs_batch shape; needs a
#     dedicated table because the shared schema uses hash-only PKs)
#   * regressions: full-PK-equality root and no-WHERE root stay on their
#     existing paths (planner type stays TABLE_SCAN)
#   * index hints FORCE / USE / IGNORE on the MAIN-QUERY root of a join
#     query (previously parsed but silently ignored)
#
# ENVELOPE: the MAIN SELECT must AGGREGATE over the CTE (scalar aggregate
# or GROUP BY + aggregates) — a projection-only main SELECT over a
# CTE_LOOKUP is outside the envelope, so every case below wraps the CTE
# columns in MIN/MAX/SUM/COUNT(*).
#
# The EXPLAIN greps are FATAL: the plan strings have been stable across
# several recording rounds (the non-fatal form was only for the very
# first --record).  Greps are --exec asserts, not recorded output, so
# the flip needed no re-record.
#
# String / DATE literals (single-quoted SQL) go through the QUERY_FILE
# path for BOTH compare and explain.  Note: both ronsql_compare.inc and
# ronsql_explain.inc --remove_file the QUERY_FILE when $QUERY is empty,
# so each step writes its own copy of the .sql file.
--source include/have_ndb.inc
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings
--let $suppress_ronsql_cli=yes
--let $strict_diff=yes
--source suite/ronsql_cte/include/cte_schema.inc
--source suite/ronsql_cte/include/cte_data.inc

# Dedicated table with an ORDERED primary-key index (plain PRIMARY KEY,
# no USING HASH — the shared schema is hash-only) so the fs_batch shape
# — a PK range on the main-query root — can be exercised.  Deterministic
# load via the same numbers-helper idiom as cte_data.inc.
--disable_query_log
--disable_warnings
CREATE TABLE acct (
  a_id  INT NOT NULL,
  a_grp INT NOT NULL,
  a_val INT NOT NULL,
  PRIMARY KEY (a_id)
) ENGINE=NDB;
CREATE TABLE _d2 (d INT) ENGINE=MEMORY;
INSERT INTO _d2 VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
CREATE TABLE _seq2 (n INT NOT NULL PRIMARY KEY) ENGINE=MEMORY;
INSERT INTO _seq2 SELECT a.d*100 + b.d*10 + c.d + 1 FROM _d2 a, _d2 b, _d2 c;
INSERT INTO acct SELECT n, MOD(n,10), n*3 FROM _seq2 WHERE n <= 200;
DROP TABLE _seq2;
DROP TABLE _d2;
--enable_warnings
--enable_query_log

DELIMITER |;

# ============================================================
# MAIN cases: secondary index on the main-query root.
# ============================================================

--echo
--echo === mri-1: root equality bound on idx_c_nationkey (fs_nation shape) ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS t
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn, SUM(cf.t) AS st
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_nationkey = 7
GROUP BY cu.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_mri_1.out
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS t
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn, SUM(cf.t) AS st
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_nationkey = 7
GROUP BY cu.c_nationkey;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[ROOT] INDEX_SCAN customer AS cu' $EXPLAIN_FILE
--exec grep -qF 'Index: idx_c_nationkey' $EXPLAIN_FILE
--exec grep -qF 'CONDITIONS (1 bound)' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS t
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn, SUM(cf.t) AS st
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_nationkey = 7
GROUP BY cu.c_nationkey|

--echo
--echo === mri-2: root double range bound on idx_c_nationkey ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_nationkey >= 5 AND cu.c_nationkey < 10
GROUP BY cu.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_mri_2.out
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_nationkey >= 5 AND cu.c_nationkey < 10
GROUP BY cu.c_nationkey;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[ROOT] INDEX_SCAN customer AS cu' $EXPLAIN_FILE
--exec grep -qF 'Index: idx_c_nationkey' $EXPLAIN_FILE
--exec grep -qF 'CONDITIONS (2 bounds)' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_nationkey >= 5 AND cu.c_nationkey < 10
GROUP BY cu.c_nationkey|

--echo
--echo === mri-3: root range bounds + non-indexable residual (c_custkey has hash PK only) ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn, MIN(cu.c_custkey) AS mn, MAX(cu.c_custkey) AS mx
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_nationkey >= 5 AND cu.c_nationkey < 10 AND cu.c_custkey > 150;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_mri_3.out
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn, MIN(cu.c_custkey) AS mn, MAX(cu.c_custkey) AS mx
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_nationkey >= 5 AND cu.c_nationkey < 10 AND cu.c_custkey > 150;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[ROOT] INDEX_SCAN customer AS cu' $EXPLAIN_FILE
--exec grep -qF 'CONDITIONS (2 bounds and 1 filter)' $EXPLAIN_FILE
--exec grep -qF 'FILTER: >' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT COUNT(*) AS c, SUM(cf.n) AS sn, MIN(cu.c_custkey) AS mn, MAX(cu.c_custkey) AS mx
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_nationkey >= 5 AND cu.c_nationkey < 10 AND cu.c_custkey > 150|

--echo
--echo === mri-4: root PRIMARY ordered-index range (fs_batch shape, acct table) ===
let $QUERY=
WITH g AS (SELECT a_grp AS k, COUNT(*) AS n, SUM(a_val) AS s
           FROM acct GROUP BY a_grp)
SELECT COUNT(*) AS c, SUM(g.n) AS sn, SUM(g.s) AS ss,
       MIN(a.a_id) AS mn, MAX(a.a_id) AS mx
FROM acct AS a JOIN g ON g.k = a.a_grp
WHERE a.a_id >= 50 AND a.a_id < 100;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_mri_4.out
let $QUERY=
WITH g AS (SELECT a_grp AS k, COUNT(*) AS n, SUM(a_val) AS s
           FROM acct GROUP BY a_grp)
SELECT COUNT(*) AS c, SUM(g.n) AS sn, SUM(g.s) AS ss,
       MIN(a.a_id) AS mn, MAX(a.a_id) AS mx
FROM acct AS a JOIN g ON g.k = a.a_grp
WHERE a.a_id >= 50 AND a.a_id < 100;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[ROOT] INDEX_SCAN acct AS a' $EXPLAIN_FILE
--exec grep -qF 'Index: PRIMARY' $EXPLAIN_FILE
--exec grep -qF 'CONDITIONS (2 bounds)' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT a_grp AS k, COUNT(*) AS n, SUM(a_val) AS s
           FROM acct GROUP BY a_grp)
SELECT COUNT(*) AS c, SUM(g.n) AS sn, SUM(g.s) AS ss,
       MIN(a.a_id) AS mn, MAX(a.a_id) AS mx
FROM acct AS a JOIN g ON g.k = a.a_grp
WHERE a.a_id >= 50 AND a.a_id < 100|

# ============================================================
# Regressions: shapes that must NOT switch to a scan-config root.
# ============================================================

--echo
--echo === mri-5 (regression): full-PK equality root keeps the PK-lookup path ===
# The selector is skipped when the root PK is fully equality-covered, so
# the planner type stays TABLE_SCAN and emit_root_op's readTuple branch
# serves the query as before.
let $QUERY=
WITH g AS (SELECT a_grp AS k, COUNT(*) AS n FROM acct GROUP BY a_grp)
SELECT COUNT(*) AS c, SUM(g.n) AS sn, MAX(a.a_val) AS mv
FROM acct AS a JOIN g ON g.k = a.a_grp
WHERE a.a_id = 77;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_mri_5.out
let $QUERY=
WITH g AS (SELECT a_grp AS k, COUNT(*) AS n FROM acct GROUP BY a_grp)
SELECT COUNT(*) AS c, SUM(g.n) AS sn, MAX(a.a_val) AS mv
FROM acct AS a JOIN g ON g.k = a.a_grp
WHERE a.a_id = 77;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[ROOT] TABLE_SCAN acct AS a' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT a_grp AS k, COUNT(*) AS n FROM acct GROUP BY a_grp)
SELECT COUNT(*) AS c, SUM(g.n) AS sn, MAX(a.a_val) AS mv
FROM acct AS a JOIN g ON g.k = a.a_grp
WHERE a.a_id = 77|

--echo
--echo === mri-6 (regression): no root WHERE stays TABLE_SCAN ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_mri_6.out
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[ROOT] TABLE_SCAN customer AS cu' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey|

# ============================================================
# MAIN cases: index hints on the MAIN-QUERY root of a join query.
# ============================================================

--echo
--echo === mri-7: FORCE INDEX (idx_c_nationkey) on the join-query root ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu FORCE INDEX (idx_c_nationkey) JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_nationkey >= 5 AND cu.c_nationkey < 10
GROUP BY cu.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_mri_7.out
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu FORCE INDEX (idx_c_nationkey) JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_nationkey >= 5 AND cu.c_nationkey < 10
GROUP BY cu.c_nationkey;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[ROOT] INDEX_SCAN customer AS cu' $EXPLAIN_FILE
--exec grep -qF 'Index: idx_c_nationkey' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu FORCE INDEX (idx_c_nationkey) JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_nationkey >= 5 AND cu.c_nationkey < 10
GROUP BY cu.c_nationkey|

--echo
--echo === mri-8: IGNORE INDEX (idx_c_nationkey) on the join-query root -> TABLE_SCAN ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu IGNORE INDEX (idx_c_nationkey) JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_nationkey >= 5 AND cu.c_nationkey < 10
GROUP BY cu.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_mri_8.out
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu IGNORE INDEX (idx_c_nationkey) JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_nationkey >= 5 AND cu.c_nationkey < 10
GROUP BY cu.c_nationkey;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[ROOT] TABLE_SCAN customer AS cu' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu IGNORE INDEX (idx_c_nationkey) JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_nationkey >= 5 AND cu.c_nationkey < 10
GROUP BY cu.c_nationkey|

# ============================================================
# Generator hardening: col-vs-col conjunct must stay a residual filter
# (encode_constant cannot serve an identifier right side as a bound).
# The col-vs-col conjunct comes FIRST in the WHERE so that, without the
# right-side constant check, the generator would claim it as the lower
# bound of PRIMARY(a_id) and throw from encode_constant at emit.  Both
# columns are INT — the interpreter's col-vs-col compare requires
# identical column types (a mixed-type pair like customer's INT
# c_custkey vs TINYINT c_nationkey fails "Failed to apply filter" on
# BOTH the old scanTable path and the new residual path; pre-existing
# limitation, see NEXT-PHASE note at the end of this file).
# ============================================================

--echo
--echo === mri-9: col-vs-col root conjunct routes to the residual filter ===
let $QUERY=
WITH g AS (SELECT a_grp AS k, COUNT(*) AS n FROM acct GROUP BY a_grp)
SELECT COUNT(*) AS c, SUM(g.n) AS sn
FROM acct AS a JOIN g ON g.k = a.a_grp
WHERE a.a_id > a.a_grp AND a.a_id >= 50 AND a.a_id < 100;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_mri_9.out
let $QUERY=
WITH g AS (SELECT a_grp AS k, COUNT(*) AS n FROM acct GROUP BY a_grp)
SELECT COUNT(*) AS c, SUM(g.n) AS sn
FROM acct AS a JOIN g ON g.k = a.a_grp
WHERE a.a_id > a.a_grp AND a.a_id >= 50 AND a.a_id < 100;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[ROOT] INDEX_SCAN acct AS a' $EXPLAIN_FILE
--exec grep -qF 'CONDITIONS (2 bounds and 1 filter)' $EXPLAIN_FILE
--exec grep -qF 'FILTER: >' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT a_grp AS k, COUNT(*) AS n FROM acct GROUP BY a_grp)
SELECT COUNT(*) AS c, SUM(g.n) AS sn
FROM acct AS a JOIN g ON g.k = a.a_grp
WHERE a.a_id > a.a_grp AND a.a_id >= 50 AND a.a_id < 100|

DELIMITER ;|

# ============================================================
# MAIN case: composite index on the root (CHAR eq + DATE range).
# String / DATE literals -> QUERY_FILE.
# ============================================================

--echo
--echo === mri-10: composite idx_o_status_date on the orders root ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_mri_10.sql
--write_file $QUERY_FILE
WITH li AS (SELECT l_orderkey AS k, COUNT(*) AS n, SUM(l_quantity) AS q
            FROM lineitem GROUP BY l_orderkey)
SELECT COUNT(*) AS c, SUM(li.n) AS sn, SUM(li.q) AS sq
FROM orders AS o JOIN li ON li.k = o.o_orderkey
WHERE o.o_orderstatus = 'F' AND o.o_orderdate >= '1995-03-01';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_mri_10.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_mri_10e.sql
--write_file $QUERY_FILE
WITH li AS (SELECT l_orderkey AS k, COUNT(*) AS n, SUM(l_quantity) AS q
            FROM lineitem GROUP BY l_orderkey)
SELECT COUNT(*) AS c, SUM(li.n) AS sn, SUM(li.q) AS sq
FROM orders AS o JOIN li ON li.k = o.o_orderkey
WHERE o.o_orderstatus = 'F' AND o.o_orderdate >= '1995-03-01';
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[ROOT] INDEX_SCAN orders AS o' $EXPLAIN_FILE
--exec grep -qF 'Index: idx_o_status_date' $EXPLAIN_FILE
--exec grep -qF 'CONDITIONS (2 bounds)' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH li AS (SELECT l_orderkey AS k, COUNT(*) AS n, SUM(l_quantity) AS q
            FROM lineitem GROUP BY l_orderkey)
SELECT COUNT(*) AS c, SUM(li.n) AS sn, SUM(li.q) AS sq
FROM orders AS o JOIN li ON li.k = o.o_orderkey
WHERE o.o_orderstatus = 'F' AND o.o_orderdate >= '1995-03-01';

# ============================================================
# PROBES (form 1): clean permanent rejection via ronsql_cli --error 1.
# ============================================================

--echo
--echo === mri-P1 (rejection): FORCE INDEX on the join root names an unusable index ===
# idx_c_mktsegment's leading column does not appear in the WHERE, so the
# forced index cannot serve a bound.  Before the main-root scan-config
# work this hint was silently ignored; now it throws the same FORCE
# error as the single-table and CTE-body paths.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT COUNT(*), SUM(cf.n) FROM customer AS cu FORCE INDEX (idx_c_mktsegment) JOIN cf ON cf.k = cu.c_custkey WHERE cu.c_nationkey = 7;' 2>&1

# ============================================================
# PROBES (form 2): genuine-gap / uncertain — recorded, NOT executed.
# ============================================================

# NEXT-PHASE: MIXED-TYPE col-vs-col in a root scan filter — the
#   interpreter's attr-vs-attr compare requires identical column types,
#   so NdbScanFilter::cmp(cond, col1, col2) fails with retryable
#   "Failed to apply filter." for e.g. INT-vs-TINYINT.  Identical on the
#   old scanTable root path and the new residual-filter path (NOT a
#   main-root-index-scan regression); mri-9 above covers the supported
#   same-type INT-vs-INT form.  Probe:
#   WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey)
#   SELECT COUNT(*), SUM(cf.n) FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
#   WHERE cu.c_nationkey >= 5 AND cu.c_nationkey < 10
#         AND cu.c_custkey > cu.c_nationkey;
#     -- want: widened same-signedness col-vs-col compare (or a clean
#     --       permanent "unsupported" rejection at prepare time);
#     -- got:  RonSQLRetryableError x10 "Failed to apply filter."

--disable_query_log
--disable_warnings
DROP TABLE acct;
--enable_warnings
--enable_query_log
--source suite/ronsql_cte/include/cte_drop.inc
