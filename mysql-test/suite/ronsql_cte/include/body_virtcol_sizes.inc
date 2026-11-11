# body_virtcol_sizes.inc — a CTE virtual column has the size of its type
# for every type a GROUP BY key can have (C-3 in
# storage/ndb/claude_files/ronsql_executor/review_cte.md).
#
# build_cte_virtual_tables gave a virtual column a size only for the
# integer, FLOAT/DOUBLE, CHAR/VARCHAR, YEAR, DATE and old temporal types.
# DATETIME, TIMESTAMP and TIME in the current (fractional-second)
# formats, DECIMAL, BINARY, VARBINARY and BIT kept size 0. A CTE grouped
# by such a column then:
#  - undersized the receive buffer of a pass-through CTE_LOOKUP child:
#    an assertion in debug builds, a heap overflow in release builds;
#  - could not build the key of a lookupCte or single-group root
#    ("Failed to create lookupCte root.");
#  - counted the key as 0 bytes in the E-23 group-key size check.
#
# Case map (vs-N):
#   1-5  pass-through CTE_LOOKUP child keyed on DATETIME, DATETIME(6),
#        TIMESTAMP(6), TIME(3) and DECIMAL(12,2)
#   6-8  lookupCte root, WHERE key = constant: DATETIME, DATETIME(6),
#        DECIMAL(12,2)
#   9    single-group root (the body's WHERE fixes the GROUP BY column)
#   10-11 pass-through child keyed on VARBINARY(32) and BINARY(20)
#        values longer than NdbRecAttr's 16-byte inline storage
#
# Cases 1-5 return 300 rows each so that the child's batches fill up.
# Their full output is compared with mysqld (strict_diff); only the line
# count is recorded here.

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
CREATE TABLE vsbin (
  v_id  INT NOT NULL,
  v_var VARBINARY(32) NOT NULL,
  v_fix BINARY(20) NOT NULL,
  PRIMARY KEY USING HASH (v_id)
) ENGINE=NDB;
INSERT INTO vsbin VALUES
  (1, 'abcdefghijklmnopqrstu', 'ABCDEFGHIJKLMNOPQRST'),
  (2, 'abcdefghijklmnopqrstu', 'TSRQPONMLKJIHGFEDCBA'),
  (3, 'zyxwvutsrqponmlkjihgfedcba', 'ABCDEFGHIJKLMNOPQRST'),
  (4, 'zyxwvutsrqponmlkjihgfedcba', 'TSRQPONMLKJIHGFEDCBA'),
  (5, 'abcdefghijklmnopqrstu', 'ABCDEFGHIJKLMNOPQRST');
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === vs-1: pass-through child keyed on DATETIME ===
let $QUERY=
WITH g AS (SELECT e_modified AS t, COUNT(*) AS n
           FROM evlog GROUP BY e_modified)
SELECT e.e_id, g.t, g.n
FROM evlog AS e JOIN g ON g.t = e.e_modified;|
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === vs-2: pass-through child keyed on DATETIME(6) ===
let $QUERY=
WITH g AS (SELECT e_logged AS t, COUNT(*) AS n
           FROM evlog GROUP BY e_logged)
SELECT e.e_id, g.t, g.n
FROM evlog AS e JOIN g ON g.t = e.e_logged;|
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === vs-3: pass-through child keyed on TIMESTAMP(6) ===
let $QUERY=
WITH g AS (SELECT e_stamp6 AS t, COUNT(*) AS n
           FROM evlog GROUP BY e_stamp6)
SELECT e.e_id, g.t, g.n
FROM evlog AS e JOIN g ON g.t = e.e_stamp6;|
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === vs-4: pass-through child keyed on TIME(3) ===
let $QUERY=
WITH g AS (SELECT e_split AS t, COUNT(*) AS n
           FROM evlog GROUP BY e_split)
SELECT e.e_id, g.t, g.n
FROM evlog AS e JOIN g ON g.t = e.e_split;|
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === vs-5: pass-through child keyed on DECIMAL(12,2) ===
let $QUERY=
WITH g AS (SELECT o_totalprice AS p, COUNT(*) AS n
           FROM orders GROUP BY o_totalprice)
SELECT o.o_orderkey, g.p, g.n
FROM orders AS o JOIN g ON g.p = o.o_totalprice
WHERE o.o_orderkey <= 300;|
--source suite/ronsql/include/ronsql_compare.inc

# Queries with string literals (single-quoted SQL) go through the
# QUERY_FILE path: ronsql_compare.inc passes $QUERY inside single quotes
# on the shell command line, and removes $QUERY_FILE after use.
--echo
--echo === vs-6: lookupCte root on DATETIME ===
--let $QUERY=
--let QUERY_FILE=$MYSQL_TMP_DIR/q_vs6.sql
--write_file $QUERY_FILE
WITH g AS (SELECT e_modified AS t, COUNT(*) AS n
           FROM evlog GROUP BY e_modified)
SELECT t, n FROM g WHERE t = '2001-01-01 02:17:00';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT e_modified AS t, COUNT(*) AS n
           FROM evlog GROUP BY e_modified)
SELECT t, n FROM g WHERE t = '2001-01-01 02:17:00'|

--echo
--echo === vs-7: lookupCte root on DATETIME(6) ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_vs7.sql
--write_file $QUERY_FILE
WITH g AS (SELECT e_logged AS t, COUNT(*) AS n
           FROM evlog GROUP BY e_logged)
SELECT t, n FROM g WHERE t = '2001-06-15 12:01:29.123457';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT e_logged AS t, COUNT(*) AS n
           FROM evlog GROUP BY e_logged)
SELECT t, n FROM g WHERE t = '2001-06-15 12:01:29.123457'|

--echo
--echo === vs-8: lookupCte root on DECIMAL(12,2) ===
let $QUERY=
WITH g AS (SELECT o_totalprice AS p, COUNT(*) AS n
           FROM orders GROUP BY o_totalprice)
SELECT p, n FROM g WHERE p = 93.59;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT o_totalprice AS p, COUNT(*) AS n
           FROM orders GROUP BY o_totalprice)
SELECT p, n FROM g WHERE p = 93.59|

--echo
--echo === vs-9: single-group root on DATETIME ===
--let $QUERY=
--let QUERY_FILE=$MYSQL_TMP_DIR/q_vs9.sql
--write_file $QUERY_FILE
WITH g AS (SELECT e_modified AS t, COUNT(*) AS n
           FROM evlog WHERE e_modified = '2001-01-01 02:17:00'
           GROUP BY e_modified)
SELECT t, n FROM g;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT e_modified AS t, COUNT(*) AS n
           FROM evlog WHERE e_modified = '2001-01-01 02:17:00'
           GROUP BY e_modified)
SELECT t, n FROM g|

--echo
--echo === vs-10: pass-through child keyed on VARBINARY(32) ===
let $QUERY=
WITH g AS (SELECT v_var AS k, COUNT(*) AS n
           FROM vsbin GROUP BY v_var)
SELECT v.v_id, g.k, g.n
FROM vsbin AS v JOIN g ON g.k = v.v_var;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT v_var AS k, COUNT(*) AS n
           FROM vsbin GROUP BY v_var)
SELECT v.v_id, g.k, g.n
FROM vsbin AS v JOIN g ON g.k = v.v_var|

--echo
--echo === vs-11: pass-through child keyed on BINARY(20) ===
let $QUERY=
WITH g AS (SELECT v_fix AS k, COUNT(*) AS n
           FROM vsbin GROUP BY v_fix)
SELECT v.v_id, g.k, g.n
FROM vsbin AS v JOIN g ON g.k = v.v_fix;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT v_fix AS k, COUNT(*) AS n
           FROM vsbin GROUP BY v_fix)
SELECT v.v_id, g.k, g.n
FROM vsbin AS v JOIN g ON g.k = v.v_fix|

DELIMITER ;|

--let $QUERY=
--let QUERY_FILE=

--disable_query_log
--disable_warnings
DROP TABLE vsbin;
--enable_warnings
--enable_query_log
--source suite/ronsql_cte/include/cte_drop.inc
