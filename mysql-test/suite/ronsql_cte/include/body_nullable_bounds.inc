# body_nullable_bounds.inc — nullable-column index-bound NULL semantics
# (findings/nullable_bounds.md), data-driven suite.
#
# NULL sorts below every value in an NDB ordered index, so a HIGH-only
# bound (`col <= X` / `col < X`) leaves the low side empty and the scan
# starts at the index head — NULL entries included — while SQL
# comparison is UNKNOWN for NULL.  Consuming such a conjunct as a bare
# bound returned NULL rows SQL excludes.  The fix set:
#  - single-table path keeps the bound and appends the mysqld
#    range-optimizer idiom setBound(col, BoundLT, NULL) = "col > NULL"
#    (plan unchanged — EXPLAIN still shows the index bound);
#  - join-root / CTE-body roots (NdbQueryBuilder emit cannot express a
#    NULL bound operand) revert the conjunct to a residual filter in
#    build_scan_config_candidates;
#  - cross-table child bounds (RONDB-1044) reject the normalized
#    T_LT/T_LE direction on nullable child columns in
#    cross_table_bound_op (low bounds and EQ pairs exclude NULLs
#    naturally and stay allowed).
# orders.o_clerk (idx_o_clerk, ~1/7 NULL) is the shared-schema probe
# column; local nb_p/nb_c provide the cross-table child case.
#
# Case map (nb-N):
#   1   single-table pass-through `o_clerk <= 5` — bound KEPT (EXPLAIN
#       pin) + strict compare (pre-fix returned the NULL rows)
#   2   aggregate single-table twin (COUNT/MIN/MAX)
#   3   strict variant `o_clerk < 5`
#   4   low-only `o_clerk >= 35` — safe direction, bound kept
#   5   closed range `>= 2 AND <= 5` — safe, bounds kept
#   6   join-root aggregate `o.o_clerk <= 5` — conjunct reverted to a
#       filter, root stays TABLE_SCAN (EXPLAIN pin) + compare
#   7   CTE-body root `WHERE o_clerk <= 5` in the body — body falls
#       back to TABLE_SCAN + filter (EXPLAIN pin) + compare
#   8   cross-table child high bound on nullable c_note — stays a
#       cross-table filter (no Bounds line) + compare
#   8b  cross-table child LOW bound on nullable c_note — stays a BOUND
#       (safe direction; Bounds grep)
#   9   ORDER BY interplay: `o_clerk <= 5 ORDER BY o_clerk LIMIT 5` —
#       Phase 4b index-order streaming keeps the (NULL-excluded) bound

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
CREATE TABLE nb_p (
  p_id  INT NOT NULL,
  p_grp INT NOT NULL,
  PRIMARY KEY (p_id)
) ENGINE=NDB;
CREATE TABLE nb_c (
  c_pid  INT NOT NULL,
  c_seq  INT NOT NULL,
  c_note INT NULL,
  PRIMARY KEY USING HASH (c_pid, c_seq)
) ENGINE=NDB;
CREATE INDEX idx_nb_c_pn ON nb_c (c_pid, c_note);
CREATE TABLE _d6 (d INT) ENGINE=MEMORY;
INSERT INTO _d6 VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
INSERT INTO nb_p SELECT d + 1, MOD(d, 4) + 1 FROM _d6;
# c_note NULL when seq = 3; otherwise = seq
INSERT INTO nb_c SELECT p.d + 1, s.d, IF(s.d = 3, NULL, s.d)
                 FROM _d6 p, _d6 s
                 WHERE s.d BETWEEN 1 AND 5;
DROP TABLE _d6;
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === nb-1: single-table pass-through, high-only bound on nullable ===
# Pre-fix: the bound scanned from the index head and returned the
# NULL-o_clerk rows.  Post-fix: the NULL-excluding low bound keeps the
# plan on the index (EXPLAIN pin) with correct results.
let $QUERY=
SELECT o_orderkey, o_clerk FROM orders WHERE o_clerk <= 5;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_nb_1.out
let $QUERY=
SELECT o_orderkey, o_clerk FROM orders WHERE o_clerk <= 5;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as index scan.' $EXPLAIN_FILE
--exec grep -qF 'Index: `idx_o_clerk`' $EXPLAIN_FILE
--exec grep -qF 'INDEX[0]:' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_clerk FROM orders WHERE o_clerk <= 5|

--echo
--echo === nb-2: aggregate single-table twin ===
let $QUERY=
SELECT COUNT(*) AS c, MIN(o_clerk) AS lo, MAX(o_clerk) AS hi
FROM orders WHERE o_clerk <= 5;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c, MIN(o_clerk) AS lo, MAX(o_clerk) AS hi
FROM orders WHERE o_clerk <= 5|

--echo
--echo === nb-3: strict high-only bound ===
let $QUERY=
SELECT COUNT(*) AS c FROM orders WHERE o_clerk < 5;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c FROM orders WHERE o_clerk < 5|

--echo
--echo === nb-4: low-only bound on nullable (safe direction) ===
let $QUERY=
SELECT o_orderkey, o_clerk FROM orders WHERE o_clerk >= 35;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_nb_4.out
let $QUERY=
SELECT o_orderkey, o_clerk FROM orders WHERE o_clerk >= 35;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as index scan.' $EXPLAIN_FILE
--exec grep -qF 'INDEX[0]:' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_clerk FROM orders WHERE o_clerk >= 35|

--echo
--echo === nb-5: closed range on nullable (safe, bounds kept) ===
let $QUERY=
SELECT o_orderkey, o_clerk FROM orders
WHERE o_clerk >= 2 AND o_clerk <= 5;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_nb_5.out
let $QUERY=
SELECT o_orderkey, o_clerk FROM orders
WHERE o_clerk >= 2 AND o_clerk <= 5;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as index scan.' $EXPLAIN_FILE
--exec grep -qF 'CONDITIONS (2 bounds):' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_clerk FROM orders
WHERE o_clerk >= 2 AND o_clerk <= 5|

--echo
--echo === nb-6: join root, high-only on nullable -> filter fallback ===
# The NdbQueryBuilder emit cannot express a NULL-excluding low bound,
# so the generator reverts the conjunct to a residual filter and the
# root stays a TABLE_SCAN.
let $QUERY=
SELECT COUNT(*) AS c
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_clerk <= 5;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_nb_6.out
let $QUERY=
SELECT COUNT(*) AS c
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_clerk <= 5;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[ROOT] TABLE_SCAN orders' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_clerk <= 5|

--echo
--echo === nb-7: CTE-body root, high-only on nullable -> filter fallback ===
let $QUERY=
WITH x AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM orders WHERE o_clerk <= 5 GROUP BY o_custkey)
SELECT cu.c_custkey, x.n
FROM customer AS cu JOIN x ON x.k = cu.c_custkey
WHERE cu.c_custkey < 8;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_nb_7.out
let $QUERY=
WITH x AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM orders WHERE o_clerk <= 5 GROUP BY o_custkey)
SELECT cu.c_custkey, x.n
FROM customer AS cu JOIN x ON x.k = cu.c_custkey
WHERE cu.c_custkey < 8;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Body root: TABLE_SCAN' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH x AS (SELECT o_custkey AS k, COUNT(*) AS n
           FROM orders WHERE o_clerk <= 5 GROUP BY o_custkey)
SELECT cu.c_custkey, x.n
FROM customer AS cu JOIN x ON x.k = cu.c_custkey
WHERE cu.c_custkey < 8|

--echo
--echo === nb-8: cross-table child HIGH bound on nullable -> filter ===
# `c.c_note <= p.p_grp` normalizes to a high bound on nullable c_note:
# cross_table_bound_op now refuses it, so it stays a cross-table filter
# (embedded sentinel machinery) and the child op carries no bounds.
let $QUERY=
SELECT COUNT(*) AS c
FROM nb_p AS p JOIN nb_c AS c ON c.c_pid = p.p_id
WHERE c.c_note <= p.p_grp;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_nb_8.out
let $QUERY=
SELECT COUNT(*) AS c
FROM nb_p AS p JOIN nb_c AS c ON c.c_pid = p.p_id
WHERE c.c_note <= p.p_grp;|
--source suite/ronsql/include/ronsql_explain.inc
--error 1
--exec grep -qF 'Bounds:' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c
FROM nb_p AS p JOIN nb_c AS c ON c.c_pid = p.p_id
WHERE c.c_note <= p.p_grp|

--echo
--echo === nb-8b: cross-table child LOW bound on nullable -> bound kept ===
# The low direction naturally excludes NULLs (they sort below every
# value), so the RONDB-1044 linked bound stays.
let $QUERY=
SELECT COUNT(*) AS c
FROM nb_p AS p JOIN nb_c AS c ON c.c_pid = p.p_id
WHERE c.c_note >= p.p_grp;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_nb_8b.out
let $QUERY=
SELECT COUNT(*) AS c
FROM nb_p AS p JOIN nb_c AS c ON c.c_pid = p.p_id
WHERE c.c_note >= p.p_grp;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Bounds: c_note >= p.p_grp' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c
FROM nb_p AS p JOIN nb_c AS c ON c.c_pid = p.p_id
WHERE c.c_note >= p.p_grp|

--echo
--echo === nb-9: ORDER BY interplay, bound kept + index-order stream ===
# Projection limited to o_clerk so LIMIT-with-ties stays deterministic.
--let $skip_sort=yes
let $QUERY=
SELECT o_clerk FROM orders WHERE o_clerk <= 5
ORDER BY o_clerk LIMIT 5;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_nb_9.out
let $QUERY=
SELECT o_clerk FROM orders WHERE o_clerk <= 5
ORDER BY o_clerk LIMIT 5;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as index scan.' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: index order (SF_OrderBy' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
SELECT o_clerk FROM orders WHERE o_clerk <= 5
ORDER BY o_clerk LIMIT 5|

DELIMITER ;|

--disable_query_log
DROP TABLE nb_p;
DROP TABLE nb_c;
--enable_query_log
--source suite/ronsql_cte/include/cte_drop.inc
