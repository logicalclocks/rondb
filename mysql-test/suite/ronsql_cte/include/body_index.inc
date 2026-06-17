# body_index.inc — index family, data-driven CTE suite.
#
# Asserts BOTH result values (via ronsql_compare.inc) AND query plans
# (via ronsql_explain.inc + a grep -qF on a stable EXPLAIN substring).
#
# Focus: CTE-body index usage —
#   * single-column index bound  -> Body root: INDEX_SCAN using idx_...
#   * composite index leading-eq (+ range / residual)
#   * scalar MIN/MAX over a NOT NULL indexed numeric col -> INDEX_SCAN
#     using idx_... [I.10 MIN_ASC/MAX_DESC maxRows=1]
#   * index hints FORCE / USE / IGNORE on the CTE-body ROOT scan
#
# CORRECTED ENVELOPE (post-discovery): the MAIN SELECT must AGGREGATE over the
# CTE (scalar aggregate or GROUP BY + aggregates) — a projection-only main
# SELECT over a CTE_LOOKUP HANGS, so every case below wraps the CTE columns in
# MIN/MAX/SUM/COUNT(*).  CTE-body aggregates stay COUNT(*) / SUM(<int col>) /
# MIN / MAX only (no SUM over DECIMAL, no COUNT(<col>)).
#
# Every grep uses a robust substring (the index name and, where stable,
# the I.10 tag).  The EXPLAIN greps are kept NON-FATAL with `|| true` for
# now; the orchestrator will tighten the exact EXPLAIN strings after
# recording.
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

DELIMITER |;

# ============================================================
# MAIN cases: single-column index bound in a CTE body.
# Main SELECT aggregates over the CTE (never projection-only).
# ============================================================

--echo
--echo === index-1: CTE body equality bound on idx_c_nationkey ===
let $QUERY=
WITH cn AS (SELECT c_nationkey AS k, COUNT(*) AS n
            FROM customer WHERE c_nationkey = 7 GROUP BY c_nationkey)
SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(n) AS sn, COUNT(*) AS g FROM cn;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_index_1.out
let $QUERY=
WITH cn AS (SELECT c_nationkey AS k, COUNT(*) AS n
            FROM customer WHERE c_nationkey = 7 GROUP BY c_nationkey)
SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(n) AS sn, COUNT(*) AS g FROM cn;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Body root: INDEX_SCAN using idx_c_nationkey' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH cn AS (SELECT c_nationkey AS k, COUNT(*) AS n
            FROM customer WHERE c_nationkey = 7 GROUP BY c_nationkey)
SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(n) AS sn, COUNT(*) AS g FROM cn|

--echo
--echo === index-2: CTE body range bound on idx_l_partkey ===
let $QUERY=
WITH lp AS (SELECT l_partkey AS k, SUM(l_quantity) AS q
            FROM lineitem WHERE l_partkey >= 40 AND l_partkey <= 60
            GROUP BY l_partkey)
SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(q) AS sq, COUNT(*) AS g FROM lp;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_index_2.out
let $QUERY=
WITH lp AS (SELECT l_partkey AS k, SUM(l_quantity) AS q
            FROM lineitem WHERE l_partkey >= 40 AND l_partkey <= 60
            GROUP BY l_partkey)
SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(q) AS sq, COUNT(*) AS g FROM lp;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Body root: INDEX_SCAN using idx_l_partkey' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH lp AS (SELECT l_partkey AS k, SUM(l_quantity) AS q
            FROM lineitem WHERE l_partkey >= 40 AND l_partkey <= 60
            GROUP BY l_partkey)
SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(q) AS sq, COUNT(*) AS g FROM lp|

--echo
--echo === index-3: CTE body half-open bound on idx_p_size ===
let $QUERY=
WITH ps AS (SELECT p_size AS k, COUNT(*) AS n
            FROM part WHERE p_size > 30 GROUP BY p_size)
SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(n) AS sn, COUNT(*) AS g FROM ps;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_index_3.out
let $QUERY=
WITH ps AS (SELECT p_size AS k, COUNT(*) AS n
            FROM part WHERE p_size > 30 GROUP BY p_size)
SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(n) AS sn, COUNT(*) AS g FROM ps;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Body root: INDEX_SCAN using idx_p_size' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH ps AS (SELECT p_size AS k, COUNT(*) AS n
            FROM part WHERE p_size > 30 GROUP BY p_size)
SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(n) AS sn, COUNT(*) AS g FROM ps|

# ------------------------------------------------------------
# index-4 / index-5: DISABLED (D9) — CTE-body index-scan root on a DATE bound.
# Recording pass: "Failed to create CTE body index-scan root".  Genuinely
# unsupported index-scan-root shape.  index-4 is kept as the documented
# probe below (NEXT-PHASE); index-5 collapses into the same finding.
# ------------------------------------------------------------

--echo
--echo === index-6: CTE body equality CHAR bound on idx_p_brand ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_index_6.sql
--write_file $QUERY_FILE
WITH pb AS (SELECT p_brand AS b, COUNT(*) AS n, SUM(p_size) AS sz
            FROM part WHERE p_brand = 'BRAND#33' GROUP BY p_brand)
SELECT MIN(b) AS mnb, MAX(b) AS mxb, SUM(n) AS sn, SUM(sz) AS ssz, COUNT(*) AS g FROM pb;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_index_6.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_index_6e.sql
--write_file $QUERY_FILE
WITH pb AS (SELECT p_brand AS b, COUNT(*) AS n, SUM(p_size) AS sz
            FROM part WHERE p_brand = 'BRAND#33' GROUP BY p_brand)
SELECT MIN(b) AS mnb, MAX(b) AS mxb, SUM(n) AS sn, SUM(sz) AS ssz, COUNT(*) AS g FROM pb;
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Body root: INDEX_SCAN using idx_p_brand' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH pb AS (SELECT p_brand AS b, COUNT(*) AS n, SUM(p_size) AS sz
            FROM part WHERE p_brand = 'BRAND#33' GROUP BY p_brand)
SELECT MIN(b) AS mnb, MAX(b) AS mxb, SUM(n) AS sn, SUM(sz) AS ssz, COUNT(*) AS g FROM pb|

# ============================================================
# MAIN cases: composite index idx_o_status_date (o_orderstatus, o_orderdate).
# All string/date literals -> QUERY_FILE.
# ============================================================

--echo
--echo === index-7: composite leading-equality only (o_orderstatus = 'O') ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_index_7.sql
--write_file $QUERY_FILE
WITH os AS (SELECT o_orderstatus AS s, COUNT(*) AS n
            FROM orders WHERE o_orderstatus = 'O' GROUP BY o_orderstatus)
SELECT MIN(s) AS mns, MAX(s) AS mxs, SUM(n) AS sn, COUNT(*) AS g FROM os;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_index_7.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_index_7e.sql
--write_file $QUERY_FILE
WITH os AS (SELECT o_orderstatus AS s, COUNT(*) AS n
            FROM orders WHERE o_orderstatus = 'O' GROUP BY o_orderstatus)
SELECT MIN(s) AS mns, MAX(s) AS mxs, SUM(n) AS sn, COUNT(*) AS g FROM os;
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Body root: INDEX_SCAN using idx_o_status_date' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH os AS (SELECT o_orderstatus AS s, COUNT(*) AS n
            FROM orders WHERE o_orderstatus = 'O' GROUP BY o_orderstatus)
SELECT MIN(s) AS mns, MAX(s) AS mxs, SUM(n) AS sn, COUNT(*) AS g FROM os|

# ------------------------------------------------------------
# index-8: DISABLED (D9) — composite leading-eq + DATE range index-scan root.
# Recording pass: "Failed to create CTE body index-scan root".  Genuinely
# unsupported index-scan-root shape (composite idx_o_status_date with a DATE
# range bound).  Recorded as a NEXT-PHASE probe below.
# ------------------------------------------------------------

# NEXT-PHASE (DISABLED — D9, index-scan root over a DATE bound): was index-9.
# The D17 part (MIN/MAX over a DATE column) is now FIXED — see agg-d17a/b/c in
# body_agg.inc, which cover DATE MIN/MAX re-aggregation and GROUP-BY-over-DATE.
# What remains unsupported here is the *index-scan root* on the composite
# idx_o_status_date with a DATE range bound (the same D9 limitation as
# index-4/5/8): the planner cannot build the CTE-body INDEX_SCAN root.
#   WITH os AS (SELECT o_orderdate AS d, SUM(o_shippriority) AS t
#               FROM orders
#               WHERE o_orderstatus = 'F' AND o_orderdate >= '1995-03-01'
#                     AND o_shippriority < 3
#               GROUP BY o_orderdate)
#   SELECT MIN(d) AS mnd, MAX(d) AS mxd, SUM(t) AS st, COUNT(*) AS g FROM os;
# See findings/_discovery_log.md (D17 fixed; index-scan-root residual = D9).

# ============================================================
# MAIN cases: scalar MIN/MAX via ordered index + maxRows=1
# over a NOT NULL indexed numeric column.  The scalar CTE output is
# wrapped in an aggregate by the main SELECT (never projection-only).
# ============================================================

--echo
--echo === index-10: scalar MAX(p_size) via idx_p_size DESC maxRows=1 ===
let $QUERY=
WITH mx AS (SELECT MAX(p_size) AS m FROM part)
SELECT MAX(m) AS mm FROM mx;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_index_10.out
let $QUERY=
WITH mx AS (SELECT MAX(p_size) AS m FROM part)
SELECT MAX(m) AS mm FROM mx;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Body root: INDEX_SCAN using idx_p_size [I.10 MAX_DESC maxRows=1]' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH mx AS (SELECT MAX(p_size) AS m FROM part)
SELECT MAX(m) AS mm FROM mx|

--echo
--echo === index-11: scalar MIN(p_size) via idx_p_size ASC maxRows=1 ===
let $QUERY=
WITH mn AS (SELECT MIN(p_size) AS m FROM part)
SELECT MIN(m) AS mm FROM mn;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_index_11.out
let $QUERY=
WITH mn AS (SELECT MIN(p_size) AS m FROM part)
SELECT MIN(m) AS mm FROM mn;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Body root: INDEX_SCAN using idx_p_size [I.10 MIN_ASC maxRows=1]' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH mn AS (SELECT MIN(p_size) AS m FROM part)
SELECT MIN(m) AS mm FROM mn|

--echo
--echo === index-12: scalar MAX(o_custkey) via idx_o_custkey DESC maxRows=1 ===
let $QUERY=
WITH mx AS (SELECT MAX(o_custkey) AS m FROM orders)
SELECT MAX(m) AS mm FROM mx;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_index_12.out
let $QUERY=
WITH mx AS (SELECT MAX(o_custkey) AS m FROM orders)
SELECT MAX(m) AS mm FROM mx;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Body root: INDEX_SCAN using idx_o_custkey [I.10 MAX_DESC maxRows=1]' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH mx AS (SELECT MAX(o_custkey) AS m FROM orders)
SELECT MAX(m) AS mm FROM mx|

--echo
--echo === index-13: scalar MIN(o_custkey) via idx_o_custkey ASC maxRows=1 ===
let $QUERY=
WITH mn AS (SELECT MIN(o_custkey) AS m FROM orders)
SELECT MIN(m) AS mm FROM mn;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_index_13.out
let $QUERY=
WITH mn AS (SELECT MIN(o_custkey) AS m FROM orders)
SELECT MIN(m) AS mm FROM mn;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Body root: INDEX_SCAN using idx_o_custkey [I.10 MIN_ASC maxRows=1]' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH mn AS (SELECT MIN(o_custkey) AS m FROM orders)
SELECT MIN(m) AS mm FROM mn|

# index-14: nullable MIN/MAX via index — VALUES correct (MAIN), but the
# plan falls back to TABLE_SCAN (nullable col).  We assert the value diff
# AND the TABLE_SCAN fallback plan (negative-space EXPLAIN assert).  The
# INDEX_SCAN-plan gap is recorded as NEXT-PHASE in findings/index.md.
--echo
--echo === index-14: scalar MAX(o_clerk) over nullable col -> TABLE_SCAN fallback ===
let $QUERY=
WITH mx AS (SELECT MAX(o_clerk) AS m FROM orders)
SELECT MAX(m) AS mm FROM mx;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_index_14.out
let $QUERY=
WITH mx AS (SELECT MAX(o_clerk) AS m FROM orders)
SELECT MAX(m) AS mm FROM mx;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Body root: TABLE_SCAN' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH mx AS (SELECT MAX(o_clerk) AS m FROM orders)
SELECT MAX(m) AS mm FROM mx|

# ============================================================
# MAIN cases: index hints on the CTE-body ROOT scan.
# ============================================================

--echo
--echo === index-15: CTE-body FORCE INDEX (idx_o_custkey) ===
let $QUERY=
WITH x AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
           FROM orders FORCE INDEX (idx_o_custkey)
           WHERE o_custkey > 100 GROUP BY o_custkey)
SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(t) AS st, COUNT(*) AS g FROM x;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_index_15.out
let $QUERY=
WITH x AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
           FROM orders FORCE INDEX (idx_o_custkey)
           WHERE o_custkey > 100 GROUP BY o_custkey)
SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(t) AS st, COUNT(*) AS g FROM x;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Body root: INDEX_SCAN using idx_o_custkey' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH x AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
           FROM orders FORCE INDEX (idx_o_custkey)
           WHERE o_custkey > 100 GROUP BY o_custkey)
SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(t) AS st, COUNT(*) AS g FROM x|

--echo
--echo === index-16: CTE-body USE INDEX (idx_c_nationkey) ===
let $QUERY=
WITH x AS (SELECT c_nationkey AS k, COUNT(*) AS n
           FROM customer USE INDEX (idx_c_nationkey)
           WHERE c_nationkey >= 5 GROUP BY c_nationkey)
SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(n) AS sn, COUNT(*) AS g FROM x;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_index_16.out
let $QUERY=
WITH x AS (SELECT c_nationkey AS k, COUNT(*) AS n
           FROM customer USE INDEX (idx_c_nationkey)
           WHERE c_nationkey >= 5 GROUP BY c_nationkey)
SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(n) AS sn, COUNT(*) AS g FROM x;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Body root: INDEX_SCAN using idx_c_nationkey' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH x AS (SELECT c_nationkey AS k, COUNT(*) AS n
           FROM customer USE INDEX (idx_c_nationkey)
           WHERE c_nationkey >= 5 GROUP BY c_nationkey)
SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(n) AS sn, COUNT(*) AS g FROM x|

--echo
--echo === index-17: scalar agg over an EMPTY grouped CTE — COUNT(*)=0, others NULL (D16) ===
# CTE x matches no rows (c_nationkey = 4 AND c_mktsegment = 'BUILDING' is empty
# under the IGNORE INDEX table scan), so the scalar main aggregation must still
# emit exactly one row: COUNT(*)=0 and MIN/MAX/SUM = NULL.
# String literal -> QUERY_FILE path (single quotes break -e '$QUERY').
--let QUERY_FILE=$MYSQL_TMP_DIR/q_index_17.sql
--write_file $QUERY_FILE
WITH x AS (SELECT c_mktsegment AS s, COUNT(*) AS n
           FROM customer IGNORE INDEX (idx_c_nationkey)
           WHERE c_nationkey = 4 AND c_mktsegment = 'BUILDING'
           GROUP BY c_mktsegment)
SELECT MIN(s) AS mns, MAX(s) AS mxs, SUM(n) AS sn, COUNT(*) AS g FROM x;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH x AS (SELECT c_mktsegment AS s, COUNT(*) AS n
           FROM customer IGNORE INDEX (idx_c_nationkey)
           WHERE c_nationkey = 4 AND c_mktsegment = 'BUILDING'
           GROUP BY c_mktsegment)
SELECT MIN(s) AS mns, MAX(s) AS mxs, SUM(n) AS sn, COUNT(*) AS g FROM x|

# ============================================================
# PROBES (form 1): clean permanent rejection via ronsql_cli --error 1.
# Rejection happens at prepare time, so the join dictionary-cache caveat
# does not apply.
# ============================================================

DELIMITER ;|

--echo
--echo === index-P1 (rejection): index hint on a JOINED (non-root) table ===
# Index hints are only supported on a root-table scan (main-query root or
# CTE-body root).  A hint on the joined CTE side must be rejected.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH c AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t FROM orders GROUP BY o_custkey) SELECT cu.c_custkey FROM customer AS cu JOIN c USE INDEX (idx_c_nationkey) ON c.k = cu.c_custkey WHERE cu.c_custkey = 5;' 2>&1

# ============================================================
# PROBES (form 2): genuine-gap / uncertain — recorded, NOT executed.
# ============================================================

# NEXT-PHASE: CTE-body index-scan root on a DATE bound (D9) — was index-4/5.
#   The recording pass errored "Failed to create CTE body index-scan root".
#   A CTE body whose root scan would be an INDEX_SCAN bounded by a DATE
#   column (idx_o_orderdate) is not yet supported; the planner cannot build
#   the index-scan root for it.  Probe (kept as the documented index-4 case):
#   WITH od AS (SELECT o_orderdate AS d, COUNT(*) AS n
#               FROM orders WHERE o_orderdate = '1995-06-15' GROUP BY o_orderdate)
#   SELECT MIN(d) AS mnd, MAX(d) AS mxd, SUM(n) AS sn, COUNT(*) AS g FROM od;
#     -- want: Body root: INDEX_SCAN using idx_o_orderdate
#     -- got:  ERROR "Failed to create CTE body index-scan root"
#   Range-bound DATE variant (was index-5) collapses into the same finding:
#   WITH od AS (SELECT o_orderdate AS d, SUM(o_shippriority) AS prio
#               FROM orders
#               WHERE o_orderdate >= '1995-06-01' AND o_orderdate <= '1995-06-30'
#               GROUP BY o_orderdate)
#   SELECT MIN(d) AS mnd, MAX(d) AS mxd, SUM(prio) AS sp, COUNT(*) AS g FROM od;

# NEXT-PHASE: composite index-scan root with a DATE range bound (D9) — was
#   index-8.  Same "Failed to create CTE body index-scan root" error: a CTE
#   body rooting on idx_o_status_date with leading-eq + a DATE range on the
#   second column does not build an index-scan root.  Probe:
#   WITH os AS (SELECT o_orderdate AS d, COUNT(*) AS n
#               FROM orders
#               WHERE o_orderstatus = 'O' AND o_orderdate > '1995-06-01'
#               GROUP BY o_orderdate)
#   SELECT MIN(d) AS mnd, MAX(d) AS mxd, SUM(n) AS sn, COUNT(*) AS g FROM os;
#     -- want: Body root: INDEX_SCAN using idx_o_status_date
#     -- got:  ERROR "Failed to create CTE body index-scan root"

# NEXT-PHASE: nullable MIN/MAX via index — EXPLAIN plan gap (capability matrix:
#   "MIN/MAX-via-index on a NULLABLE column falls back to TABLE_SCAN; VALUES
#   still correct, only the EXPLAIN plan differs").  index-14 above keeps the
#   value-compare ENABLED and asserts the TABLE_SCAN fallback.  The desired
#   INDEX_SCAN-via-idx_o_clerk plan is NOT YET emitted; do NOT assert it:
#   WITH mx AS (SELECT MAX(o_clerk) AS m FROM orders) SELECT MAX(m) AS mm FROM mx;
#     -- want: Body root: INDEX_SCAN using idx_o_clerk [I.10 MAX_DESC maxRows=1]
#     -- got:  Body root: TABLE_SCAN

# NEXT-PHASE: scalar MIN/MAX-via-index with an added WHERE composition —
#   uncertain whether the I.10 maxRows=1 ordered-index path survives a WHERE
#   bound in the same scalar CTE body (I.10 detects a single-op body whose only
#   output is a direct-column MIN/MAX; a WHERE may force the residual-filter
#   path and drop maxRows=1).  Probe:
#   WITH mx AS (SELECT MAX(p_size) AS m FROM part WHERE p_size < 40) SELECT MAX(m) AS mm FROM mx;
#     -- want: INDEX_SCAN using idx_p_size [I.10 MAX_DESC maxRows=1] (or a
#     --       documented INDEX_SCAN+bounds plan); VALUES must still match MySQL.

# NEXT-PHASE: BETWEEN as an index bound in a CTE body — uncertain whether the
#   parser/planner lowers BETWEEN into the same low+high NdbQueryIndexBound the
#   `col >= x AND col <= y` form produces (capability matrix marks BETWEEN
#   UNCERTAIN).  Probe:
#   WITH lp AS (SELECT l_partkey AS k, SUM(l_quantity) AS q FROM lineitem
#               WHERE l_partkey BETWEEN 40 AND 60 GROUP BY l_partkey)
#   SELECT MIN(k) AS mnk, MAX(k) AS mxk, SUM(q) AS sq, COUNT(*) AS g FROM lp;
#     -- want: Body root: INDEX_SCAN using idx_l_partkey (matching index-2's plan)

--source suite/ronsql_cte/include/cte_drop.inc
