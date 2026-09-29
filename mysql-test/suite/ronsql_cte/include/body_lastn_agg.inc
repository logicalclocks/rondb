# body_lastn_agg.inc — aggregates over the last N rows of an entity
# (count-based windows), WP-J phases J0 and J1 of
# storage/ndb/claude_files/fs_ronsql/ronsql_fs_support_plan.md.
#
# The natural statement
#   WITH t AS (SELECT ... FROM lastn_tx WHERE customer_id = K
#              ORDER BY event_time DESC LIMIT N)
#   SELECT AVG(amount), ... FROM t;
# was rejected by RonSQL before WP-J J1: a non-aggregating CTE body was only
# served as a single-row key lookup, and collapse_collect_cte only takes a
# projection-only main query.
#
# J0 expresses the same set with the existing kernel top-N for grouped CTE
# bodies: GROUP BY every output column, the full primary key (customer_id,
# event_time) first, plus a dummy COUNT(*) (as in the Hopsworks snowflake
# CTE).  The primary key is unique, so each group is exactly one row and
# the extra keys change nothing, while GROUP BY keys keep their source
# types.  MAX(col) was the first choice and does not work as a general
# carrier: a CTE MIN/MAX output is widened to the wire type (INT -> BIGINT,
# FLOAT -> DOUBLE, DECIMAL -> BIGINT / DOUBLE, temporal -> 8-byte
# unsigned), and a widened INT join key fails the pushed join's operand
# type check ("Failed to create child operation", first run of lastn-12).
# Every case checks the chain
#   RonSQL(J0) = MySQL(J0) = MySQL(natural)
# with lastn_twin_check.inc (the two MySQL results must be identical) and
# ronsql_compare.inc on the J0 text (strict diff).
#
# J1 (RonSQLPreparer::rewrite_lastn_cte_bodies) rewrites the natural
# statement into the J0 form at parse time; every case therefore also runs
# the natural statement itself on RonSQL (== J1 ==), and lastn-P1 pins the
# rewrite in EXPLAIN.  J2 (streaming fast path) must keep these results.
#
# SET DETERMINISM: event_time is the last primary-key column, so it is
# unique per customer and the ORDER BY decides the kept set exactly.
#
# DATA: lastn_tx has the Hopsworks online-table layout — PRIMARY KEY
# (entity, event_time), no PARTITION BY, so one customer's rows are spread
# over all fragments.  Row i of customer c is (i * 60 + c * 7) minutes
# before 2026-06-01 00:00:00 UTC, so i = 0 is the newest row.
#   customer 1: no rows          customer 2: 3 rows     customer 9: 1 row
#   customer 3: 25 rows          customer 4: 25 rows, amount NULL when
#   customers 5..8: 40 rows      i mod 4 = 1 (i = 1, 5, 9 among the newest 10)
# amount = 100 + (37c + 13i) mod 900, fee = (11c + 7i) mod 50 - 10,
# category cycles grocery/travel/fuel/dining, merchant_id = 1 + (5c + 3i)
# mod 12 with lastn_merchants holding 1..10 (11 and 12 dangle) and
# mcc = 5000 + merchant_id mod 4.
#
# ORDER: the core aggregate cases and the W constant-width regressions
# come first; string MIN/MAX, main-query
# GROUP BY over a CTE output, chained CTEs and joins onto the last-N CTE
# follow, so a first failure there still leaves the core recorded.
#
# Case map:
#   P1 the natural statement is served by the J1 rewrite (EXPLAIN pin)
#   P2 a LIMIT body without the whole primary key is rejected, naming it
#   1  COUNT(*), COUNT, SUM, MIN, MAX, AVG over the newest 10 of 25 rows
#   2  N above the row count (3 rows, LIMIT 10)
#   3  entity with no rows: COUNT 0, the rest NULL
#   4  NULLs in the aggregated column (COUNT(*) 10, COUNT(amount) 7)
#   5  LIMIT 1: the latest values
#   6  oldest N: ORDER BY event_time ASC LIMIT 5
#   7  time window tighter than N (6 rows in the window, LIMIT 10)
#   8  residual filter on a non-key column + LIMIT
#   W1 full primary-key lookup on the TIMESTAMP(0) key (constant width)
#   W2 IN list on the TIMESTAMP(0) key (N primary-key lookups)
#   W3 single-row CTE keyed on the TIMESTAMP(0) key (pushed-query constant)
#   9  string and temporal MIN / MAX over the last N
#   10 main-query GROUP BY a non-key column of the last-N CTE
#   11 distinct categories among the last 20 (chained CTE)
#   12 INNER JOIN onto a dimension, GROUP BY a dimension column
#   13 LEFT JOIN onto a dimension (dangling merchant ids)
#   14 the last N rows joined to a dimension, projection-only main
--source include/have_ndb.inc
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings
--let $suppress_ronsql_cli=yes
--let $strict_diff=yes

DELIMITER |;

--disable_query_log
SET time_zone = '+00:00'|
CREATE TABLE lastn_tx (
  customer_id BIGINT NOT NULL,
  event_time TIMESTAMP NOT NULL,
  amount BIGINT,
  fee INT,
  category VARCHAR(100),
  merchant_id INT,
  PRIMARY KEY (customer_id, event_time)
) ENGINE=NDB|
CREATE TABLE lastn_merchants (
  merchant_id INT NOT NULL,
  mcc INT NOT NULL,
  PRIMARY KEY (merchant_id)
) ENGINE=NDB|
INSERT INTO lastn_tx
  WITH RECURSIVE s (i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i < 39),
  ents (c, n) AS (SELECT 2, 3 UNION ALL SELECT 3, 25 UNION ALL SELECT 4, 25
                  UNION ALL SELECT 5, 40 UNION ALL SELECT 6, 40
                  UNION ALL SELECT 7, 40 UNION ALL SELECT 8, 40
                  UNION ALL SELECT 9, 1)
  SELECT ents.c,
         TIMESTAMP'2026-06-01 00:00:00' - INTERVAL (s.i * 60 + ents.c * 7) MINUTE,
         CASE WHEN ents.c = 4 AND MOD(s.i, 4) = 1 THEN NULL
              ELSE 100 + MOD(ents.c * 37 + s.i * 13, 900) END,
         MOD(ents.c * 11 + s.i * 7, 50) - 10,
         ELT(1 + MOD(ents.c + s.i, 4), 'grocery', 'travel', 'fuel', 'dining'),
         1 + MOD(ents.c * 5 + s.i * 3, 12)
  FROM ents JOIN s ON s.i < ents.n|
INSERT INTO lastn_merchants
  WITH RECURSIVE s (m) AS (SELECT 1 UNION ALL SELECT m + 1 FROM s WHERE m < 10)
  SELECT m, 5000 + MOD(m, 4) FROM s|
--enable_query_log
SELECT customer_id, COUNT(*) AS n_rows, COUNT(amount) AS n_amount,
       MIN(event_time) AS oldest, MAX(event_time) AS newest
FROM lastn_tx GROUP BY customer_id ORDER BY customer_id|

DELIMITER ;|

--echo
--echo === lastn-P1: the natural statement is served by the J1 rewrite ===
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_lastn_p1.out
--let QUERY_FILE=$MYSQL_TMP_DIR/lastn_p1.sql
--write_file $QUERY_FILE
WITH t AS (SELECT customer_id, event_time, amount FROM lastn_tx
           WHERE customer_id = 3 ORDER BY event_time DESC LIMIT 10)
SELECT AVG(amount) AS av FROM t;
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF "CTE 't' served as the last N rows" $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE

--echo
--echo === lastn-P2 (rejection): a LIMIT body without the whole primary key ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH t AS (SELECT customer_id, amount FROM lastn_tx WHERE customer_id = 3 ORDER BY amount DESC LIMIT 10) SELECT AVG(amount) AS av FROM t;' 2>&1

--echo
--echo === lastn-1: COUNT(*), COUNT, SUM, MIN, MAX, AVG over the newest 10 of 25 rows ===
--let LASTN_NATURAL_FILE=$MYSQL_TMP_DIR/lastn_1_nat.sql
--write_file $LASTN_NATURAL_FILE
WITH t AS (SELECT customer_id, event_time, amount, fee FROM lastn_tx
           WHERE customer_id = 3 ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(*) AS n, COUNT(amount) AS n_amount, SUM(amount) AS s,
       MIN(amount) AS mn, MAX(amount) AS mx, AVG(amount) AS av,
       MIN(fee) AS fee_min, MAX(fee) AS fee_max
FROM t;
EOF
--let LASTN_J0_FILE=$MYSQL_TMP_DIR/lastn_1_j0.sql
--write_file $LASTN_J0_FILE
WITH t AS (SELECT customer_id, event_time, amount, fee, COUNT(*) AS grp_rows
           FROM lastn_tx WHERE customer_id = 3
           GROUP BY customer_id, event_time, amount, fee
           ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(*) AS n, COUNT(amount) AS n_amount, SUM(amount) AS s,
       MIN(amount) AS mn, MAX(amount) AS mx, AVG(amount) AS av,
       MIN(fee) AS fee_min, MAX(fee) AS fee_max
FROM t;
EOF
--source suite/ronsql_cte/include/lastn_twin_check.inc
--let QUERY_FILE=$LASTN_J0_FILE
--source suite/ronsql/include/ronsql_compare.inc
--echo == J1: the natural statement on RonSQL ==
--let QUERY_FILE=$LASTN_NATURAL_FILE
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-2: N above the row count (3 rows, LIMIT 10) ===
--let LASTN_NATURAL_FILE=$MYSQL_TMP_DIR/lastn_2_nat.sql
--write_file $LASTN_NATURAL_FILE
WITH t AS (SELECT customer_id, event_time, amount FROM lastn_tx
           WHERE customer_id = 2 ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(*) AS n, SUM(amount) AS s, AVG(amount) AS av FROM t;
EOF
--let LASTN_J0_FILE=$MYSQL_TMP_DIR/lastn_2_j0.sql
--write_file $LASTN_J0_FILE
WITH t AS (SELECT customer_id, event_time, amount, COUNT(*) AS grp_rows
           FROM lastn_tx WHERE customer_id = 2
           GROUP BY customer_id, event_time, amount
           ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(*) AS n, SUM(amount) AS s, AVG(amount) AS av FROM t;
EOF
--source suite/ronsql_cte/include/lastn_twin_check.inc
--let QUERY_FILE=$LASTN_J0_FILE
--source suite/ronsql/include/ronsql_compare.inc
--echo == J1: the natural statement on RonSQL ==
--let QUERY_FILE=$LASTN_NATURAL_FILE
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-3: entity with no rows (COUNT 0, the rest NULL) ===
--let LASTN_NATURAL_FILE=$MYSQL_TMP_DIR/lastn_3_nat.sql
--write_file $LASTN_NATURAL_FILE
WITH t AS (SELECT customer_id, event_time, amount FROM lastn_tx
           WHERE customer_id = 1 ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(*) AS n, COUNT(amount) AS n_amount, SUM(amount) AS s,
       MIN(amount) AS mn, MAX(amount) AS mx, AVG(amount) AS av
FROM t;
EOF
--let LASTN_J0_FILE=$MYSQL_TMP_DIR/lastn_3_j0.sql
--write_file $LASTN_J0_FILE
WITH t AS (SELECT customer_id, event_time, amount, COUNT(*) AS grp_rows
           FROM lastn_tx WHERE customer_id = 1
           GROUP BY customer_id, event_time, amount
           ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(*) AS n, COUNT(amount) AS n_amount, SUM(amount) AS s,
       MIN(amount) AS mn, MAX(amount) AS mx, AVG(amount) AS av
FROM t;
EOF
--source suite/ronsql_cte/include/lastn_twin_check.inc
--let QUERY_FILE=$LASTN_J0_FILE
--source suite/ronsql/include/ronsql_compare.inc
--echo == J1: the natural statement on RonSQL ==
--let QUERY_FILE=$LASTN_NATURAL_FILE
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-4: NULLs in the aggregated column ===
--let LASTN_NATURAL_FILE=$MYSQL_TMP_DIR/lastn_4_nat.sql
--write_file $LASTN_NATURAL_FILE
WITH t AS (SELECT customer_id, event_time, amount FROM lastn_tx
           WHERE customer_id = 4 ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(*) AS n, COUNT(amount) AS n_amount, SUM(amount) AS s,
       MIN(amount) AS mn, MAX(amount) AS mx, AVG(amount) AS av
FROM t;
EOF
--let LASTN_J0_FILE=$MYSQL_TMP_DIR/lastn_4_j0.sql
--write_file $LASTN_J0_FILE
WITH t AS (SELECT customer_id, event_time, amount, COUNT(*) AS grp_rows
           FROM lastn_tx WHERE customer_id = 4
           GROUP BY customer_id, event_time, amount
           ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(*) AS n, COUNT(amount) AS n_amount, SUM(amount) AS s,
       MIN(amount) AS mn, MAX(amount) AS mx, AVG(amount) AS av
FROM t;
EOF
--source suite/ronsql_cte/include/lastn_twin_check.inc
--let QUERY_FILE=$LASTN_J0_FILE
--source suite/ronsql/include/ronsql_compare.inc
--echo == J1: the natural statement on RonSQL ==
--let QUERY_FILE=$LASTN_NATURAL_FILE
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-5: LIMIT 1, the latest values ===
--let LASTN_NATURAL_FILE=$MYSQL_TMP_DIR/lastn_5_nat.sql
--write_file $LASTN_NATURAL_FILE
WITH t AS (SELECT customer_id, event_time, amount, fee FROM lastn_tx
           WHERE customer_id = 3 ORDER BY event_time DESC LIMIT 1)
SELECT COUNT(*) AS n, MAX(amount) AS latest_amount, MAX(fee) AS latest_fee
FROM t;
EOF
--let LASTN_J0_FILE=$MYSQL_TMP_DIR/lastn_5_j0.sql
--write_file $LASTN_J0_FILE
WITH t AS (SELECT customer_id, event_time, amount, fee, COUNT(*) AS grp_rows
           FROM lastn_tx WHERE customer_id = 3
           GROUP BY customer_id, event_time, amount, fee
           ORDER BY event_time DESC LIMIT 1)
SELECT COUNT(*) AS n, MAX(amount) AS latest_amount, MAX(fee) AS latest_fee
FROM t;
EOF
--source suite/ronsql_cte/include/lastn_twin_check.inc
--let QUERY_FILE=$LASTN_J0_FILE
--source suite/ronsql/include/ronsql_compare.inc
--echo == J1: the natural statement on RonSQL ==
--let QUERY_FILE=$LASTN_NATURAL_FILE
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-6: oldest N, ORDER BY event_time ASC LIMIT 5 ===
--let LASTN_NATURAL_FILE=$MYSQL_TMP_DIR/lastn_6_nat.sql
--write_file $LASTN_NATURAL_FILE
WITH t AS (SELECT customer_id, event_time, amount, fee FROM lastn_tx
           WHERE customer_id = 5 ORDER BY event_time ASC LIMIT 5)
SELECT COUNT(*) AS n, SUM(amount) AS s, MIN(fee) AS fee_min FROM t;
EOF
--let LASTN_J0_FILE=$MYSQL_TMP_DIR/lastn_6_j0.sql
--write_file $LASTN_J0_FILE
WITH t AS (SELECT customer_id, event_time, amount, fee, COUNT(*) AS grp_rows
           FROM lastn_tx WHERE customer_id = 5
           GROUP BY customer_id, event_time, amount, fee
           ORDER BY event_time ASC LIMIT 5)
SELECT COUNT(*) AS n, SUM(amount) AS s, MIN(fee) AS fee_min FROM t;
EOF
--source suite/ronsql_cte/include/lastn_twin_check.inc
--let QUERY_FILE=$LASTN_J0_FILE
--source suite/ronsql/include/ronsql_compare.inc
--echo == J1: the natural statement on RonSQL ==
--let QUERY_FILE=$LASTN_NATURAL_FILE
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-7: time window tighter than N (6 rows in the window, LIMIT 10) ===
--let LASTN_NATURAL_FILE=$MYSQL_TMP_DIR/lastn_7_nat.sql
--write_file $LASTN_NATURAL_FILE
WITH t AS (SELECT customer_id, event_time, amount FROM lastn_tx
           WHERE customer_id = 5 AND event_time >= '2026-05-31 18:00:00'
           ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(*) AS n, SUM(amount) AS s, AVG(amount) AS av FROM t;
EOF
--let LASTN_J0_FILE=$MYSQL_TMP_DIR/lastn_7_j0.sql
--write_file $LASTN_J0_FILE
WITH t AS (SELECT customer_id, event_time, amount, COUNT(*) AS grp_rows
           FROM lastn_tx
           WHERE customer_id = 5 AND event_time >= '2026-05-31 18:00:00'
           GROUP BY customer_id, event_time, amount
           ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(*) AS n, SUM(amount) AS s, AVG(amount) AS av FROM t;
EOF
--source suite/ronsql_cte/include/lastn_twin_check.inc
--let QUERY_FILE=$LASTN_J0_FILE
--source suite/ronsql/include/ronsql_compare.inc
--echo == J1: the natural statement on RonSQL ==
--let QUERY_FILE=$LASTN_NATURAL_FILE
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-8: residual filter on a non-key column + LIMIT ===
--let LASTN_NATURAL_FILE=$MYSQL_TMP_DIR/lastn_8_nat.sql
--write_file $LASTN_NATURAL_FILE
WITH t AS (SELECT customer_id, event_time, amount, fee FROM lastn_tx
           WHERE customer_id = 6 AND fee >= 0
           ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(*) AS n, SUM(amount) AS s, MIN(fee) AS fee_min, MAX(fee) AS fee_max
FROM t;
EOF
--let LASTN_J0_FILE=$MYSQL_TMP_DIR/lastn_8_j0.sql
--write_file $LASTN_J0_FILE
WITH t AS (SELECT customer_id, event_time, amount, fee, COUNT(*) AS grp_rows
           FROM lastn_tx WHERE customer_id = 6 AND fee >= 0
           GROUP BY customer_id, event_time, amount, fee
           ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(*) AS n, SUM(amount) AS s, MIN(fee) AS fee_min, MAX(fee) AS fee_max
FROM t;
EOF
--source suite/ronsql_cte/include/lastn_twin_check.inc
--let QUERY_FILE=$LASTN_J0_FILE
--source suite/ronsql/include/ronsql_compare.inc
--echo == J1: the natural statement on RonSQL ==
--let QUERY_FILE=$LASTN_NATURAL_FILE
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-W1: full primary-key lookup on the TIMESTAMP(0) key (key-row width) ===
# encode_constant used to return 7 bytes for every TIMESTAMP constant; the
# key-row paths copied all 7 into the 4-byte TIMESTAMP(0) slot.
--let QUERY_FILE=$MYSQL_TMP_DIR/lastn_w1.sql
--write_file $QUERY_FILE
SELECT customer_id, event_time, amount, fee FROM lastn_tx
WHERE customer_id = 3 AND event_time = '2026-05-31 23:39:00';
EOF
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-W2: IN list on the TIMESTAMP(0) key (N primary-key lookups) ===
--let QUERY_FILE=$MYSQL_TMP_DIR/lastn_w2.sql
--write_file $QUERY_FILE
SELECT customer_id, event_time, amount FROM lastn_tx
WHERE customer_id = 3
  AND event_time IN ('2026-05-31 23:39:00', '2026-05-31 21:39:00',
                     '2026-05-31 20:00:00');
EOF
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-W3: single-row CTE keyed on the TIMESTAMP(0) key (pushed-query constant) ===
# The body's full-PK equality becomes NdbQueryBuilder::constValue(ptr, len),
# which requires exactly the column width (4803 with 7 bytes).
--let QUERY_FILE=$MYSQL_TMP_DIR/lastn_w3.sql
--write_file $QUERY_FILE
WITH r AS (SELECT customer_id, event_time, amount FROM lastn_tx
           WHERE customer_id = 3 AND event_time = '2026-05-31 23:39:00')
SELECT r.amount AS amount FROM r;
EOF
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-9: string and temporal MIN / MAX over the last N ===
--let LASTN_NATURAL_FILE=$MYSQL_TMP_DIR/lastn_9_nat.sql
--write_file $LASTN_NATURAL_FILE
WITH t AS (SELECT customer_id, event_time, category FROM lastn_tx
           WHERE customer_id = 3 ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(category) AS n, MIN(category) AS cat_min, MAX(category) AS cat_max,
       MIN(event_time) AS oldest_kept, MAX(event_time) AS newest_kept
FROM t;
EOF
--let LASTN_J0_FILE=$MYSQL_TMP_DIR/lastn_9_j0.sql
--write_file $LASTN_J0_FILE
WITH t AS (SELECT customer_id, event_time, category, COUNT(*) AS grp_rows
           FROM lastn_tx WHERE customer_id = 3
           GROUP BY customer_id, event_time, category
           ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(category) AS n, MIN(category) AS cat_min, MAX(category) AS cat_max,
       MIN(event_time) AS oldest_kept, MAX(event_time) AS newest_kept
FROM t;
EOF
--source suite/ronsql_cte/include/lastn_twin_check.inc
--let QUERY_FILE=$LASTN_J0_FILE
--source suite/ronsql/include/ronsql_compare.inc
--echo == J1: the natural statement on RonSQL ==
--let QUERY_FILE=$LASTN_NATURAL_FILE
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-10: main-query GROUP BY a non-key column of the last-N CTE ===
--let LASTN_NATURAL_FILE=$MYSQL_TMP_DIR/lastn_10_nat.sql
--write_file $LASTN_NATURAL_FILE
WITH t AS (SELECT customer_id, event_time, amount, category FROM lastn_tx
           WHERE customer_id = 7 ORDER BY event_time DESC LIMIT 20)
SELECT category, COUNT(*) AS n, SUM(amount) AS s FROM t GROUP BY category;
EOF
--let LASTN_J0_FILE=$MYSQL_TMP_DIR/lastn_10_j0.sql
--write_file $LASTN_J0_FILE
WITH t AS (SELECT customer_id, event_time, amount,
                  category, COUNT(*) AS grp_rows
           FROM lastn_tx WHERE customer_id = 7
           GROUP BY customer_id, event_time, amount, category
           ORDER BY event_time DESC LIMIT 20)
SELECT category, COUNT(*) AS n, SUM(amount) AS s FROM t GROUP BY category;
EOF
--source suite/ronsql_cte/include/lastn_twin_check.inc
--let QUERY_FILE=$LASTN_J0_FILE
--source suite/ronsql/include/ronsql_compare.inc
--echo == J1: the natural statement on RonSQL ==
--let QUERY_FILE=$LASTN_NATURAL_FILE
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-11: distinct categories among the last 20 (chained CTE) ===
--let LASTN_NATURAL_FILE=$MYSQL_TMP_DIR/lastn_11_nat.sql
--write_file $LASTN_NATURAL_FILE
WITH t AS (SELECT customer_id, event_time, category FROM lastn_tx
           WHERE customer_id = 7 ORDER BY event_time DESC LIMIT 20),
     g AS (SELECT category, COUNT(*) AS n FROM t GROUP BY category)
SELECT COUNT(*) AS n_categories, MAX(n) AS top_category_count FROM g;
EOF
--let LASTN_J0_FILE=$MYSQL_TMP_DIR/lastn_11_j0.sql
--write_file $LASTN_J0_FILE
WITH t AS (SELECT customer_id, event_time, category, COUNT(*) AS grp_rows
           FROM lastn_tx WHERE customer_id = 7
           GROUP BY customer_id, event_time, category
           ORDER BY event_time DESC LIMIT 20),
     g AS (SELECT category, COUNT(*) AS n FROM t GROUP BY category)
SELECT COUNT(*) AS n_categories, MAX(n) AS top_category_count FROM g;
EOF
--source suite/ronsql_cte/include/lastn_twin_check.inc
--let QUERY_FILE=$LASTN_J0_FILE
--source suite/ronsql/include/ronsql_compare.inc
--echo == J1: the natural statement on RonSQL ==
--let QUERY_FILE=$LASTN_NATURAL_FILE
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-12: INNER JOIN onto a dimension, GROUP BY a dimension column ===
--let LASTN_NATURAL_FILE=$MYSQL_TMP_DIR/lastn_12_nat.sql
--write_file $LASTN_NATURAL_FILE
WITH t AS (SELECT customer_id, event_time, amount, merchant_id FROM lastn_tx
           WHERE customer_id = 8 ORDER BY event_time DESC LIMIT 10)
SELECT m.mcc, COUNT(*) AS n, SUM(t.amount) AS s
FROM t JOIN lastn_merchants AS m ON m.merchant_id = t.merchant_id
GROUP BY m.mcc;
EOF
--let LASTN_J0_FILE=$MYSQL_TMP_DIR/lastn_12_j0.sql
--write_file $LASTN_J0_FILE
WITH t AS (SELECT customer_id, event_time, amount,
                  merchant_id, COUNT(*) AS grp_rows
           FROM lastn_tx WHERE customer_id = 8
           GROUP BY customer_id, event_time, amount, merchant_id
           ORDER BY event_time DESC LIMIT 10)
SELECT m.mcc, COUNT(*) AS n, SUM(t.amount) AS s
FROM t JOIN lastn_merchants AS m ON m.merchant_id = t.merchant_id
GROUP BY m.mcc;
EOF
--source suite/ronsql_cte/include/lastn_twin_check.inc
--let QUERY_FILE=$LASTN_J0_FILE
--source suite/ronsql/include/ronsql_compare.inc
--echo == J1: the natural statement on RonSQL ==
--let QUERY_FILE=$LASTN_NATURAL_FILE
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-13: LEFT JOIN onto a dimension (dangling merchant ids) ===
--let LASTN_NATURAL_FILE=$MYSQL_TMP_DIR/lastn_13_nat.sql
--write_file $LASTN_NATURAL_FILE
WITH t AS (SELECT customer_id, event_time, amount, merchant_id FROM lastn_tx
           WHERE customer_id = 8 ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(*) AS n, COUNT(m.mcc) AS n_known_merchant, SUM(t.amount) AS s
FROM t LEFT JOIN lastn_merchants AS m ON m.merchant_id = t.merchant_id;
EOF
--let LASTN_J0_FILE=$MYSQL_TMP_DIR/lastn_13_j0.sql
--write_file $LASTN_J0_FILE
WITH t AS (SELECT customer_id, event_time, amount,
                  merchant_id, COUNT(*) AS grp_rows
           FROM lastn_tx WHERE customer_id = 8
           GROUP BY customer_id, event_time, amount, merchant_id
           ORDER BY event_time DESC LIMIT 10)
SELECT COUNT(*) AS n, COUNT(m.mcc) AS n_known_merchant, SUM(t.amount) AS s
FROM t LEFT JOIN lastn_merchants AS m ON m.merchant_id = t.merchant_id;
EOF
--source suite/ronsql_cte/include/lastn_twin_check.inc
--let QUERY_FILE=$LASTN_J0_FILE
--source suite/ronsql/include/ronsql_compare.inc
--echo == J1: the natural statement on RonSQL ==
--let QUERY_FILE=$LASTN_NATURAL_FILE
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === lastn-14: the last N rows enriched from a dimension, projection only ===
--let LASTN_NATURAL_FILE=$MYSQL_TMP_DIR/lastn_14_nat.sql
--write_file $LASTN_NATURAL_FILE
WITH t AS (SELECT customer_id, event_time, amount, merchant_id FROM lastn_tx
           WHERE customer_id = 8 ORDER BY event_time DESC LIMIT 5)
SELECT t.event_time, t.amount, m.mcc
FROM t JOIN lastn_merchants AS m ON m.merchant_id = t.merchant_id;
EOF
--let LASTN_J0_FILE=$MYSQL_TMP_DIR/lastn_14_j0.sql
--write_file $LASTN_J0_FILE
WITH t AS (SELECT customer_id, event_time, amount,
                  merchant_id, COUNT(*) AS grp_rows
           FROM lastn_tx WHERE customer_id = 8
           GROUP BY customer_id, event_time, amount, merchant_id
           ORDER BY event_time DESC LIMIT 5)
SELECT t.event_time, t.amount, m.mcc
FROM t JOIN lastn_merchants AS m ON m.merchant_id = t.merchant_id;
EOF
--source suite/ronsql_cte/include/lastn_twin_check.inc
--let QUERY_FILE=$LASTN_J0_FILE
--source suite/ronsql/include/ronsql_compare.inc
--echo == J1: the natural statement on RonSQL ==
--let QUERY_FILE=$LASTN_NATURAL_FILE
--source suite/ronsql/include/ronsql_compare.inc

--disable_query_log
DROP TABLE lastn_tx;
DROP TABLE lastn_merchants;
SET time_zone = DEFAULT;
--enable_query_log
