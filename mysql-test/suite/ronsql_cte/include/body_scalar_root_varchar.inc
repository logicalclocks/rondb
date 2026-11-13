# body_scalar_root_varchar.inc — a scalar CTE (no GROUP BY) as the FROM
# root with a WHERE, whose first output is a string (D-5 = J-4 = C-7 in
# storage/ndb/claude_files/ronsql_executor/README.md).
#
# A filtered scalar CTE root is a lookupCte with a dummy key. The scalar
# virtual table's primary key is its first output column, so the key
# operand must have that column's type and size; the kernel ignores its
# value. The root built the key of a string column as getSizeInBytes()
# zero bytes. For a VARCHAR that size includes the 1- or 2-byte length
# prefix, which the generic const operand adds itself, so lookupCte
# refused the key (QRY_CHAR_OPERAND_TRUNCATED) and the query failed with
# "Failed to create scalar lookupCte root.". The cross-join child copy of
# the dummy key already passed an empty value for VAR columns (sc-20 in
# body_passthrough_scalar_cte.inc); both now share
# RonSQLPreparer::scalar_cte_dummy_key. A WHERE that is never TRUE after
# a NULL scalar subquery result is substituted (X-3) takes the same root.
#
# sr_t (latin1): vs VARCHAR(8) has a 1-byte length prefix, vl
# VARCHAR(300) a 2-byte one, ch CHAR(4) is fixed size.
#   MIN(vs) = apple   MAX(vs) = cherry
#   MIN(vl) = kiwi    MAX(vl) = mango
#   MIN(ch) = abcd    MAX(ch) = cdef
#   COUNT(*) = 3      SUM(x) = 60
# sr_n has one row whose n_s is NULL, so (SELECT MAX(n_s) FROM sr_n) is
# NULL.
#
# Case map (sr-N):
#   1-2   VARCHAR(8) first output, string filter keeps / rejects the row
#   3-4   VARCHAR(300) first output, string filter keeps / rejects
#   5-6   CHAR(4) first output (control: fixed size, worked before)
#   7-8   string first output, the filter on the second (numeric) output
#   9-10  aggregate over the root, keeps / rejects
#   11-12 WHERE never TRUE after a NULL subquery, pass-through and
#         aggregate
#
# String literals go through the QUERY_FILE path of ronsql_compare.inc;
# it removes the file, so each case writes its own.

--source include/have_ndb.inc

--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings

--let $suppress_ronsql_cli=yes
--let $strict_diff=yes

--disable_query_log
--disable_warnings
CREATE TABLE sr_t (
  id INT NOT NULL,
  vs VARCHAR(8) CHARACTER SET latin1,
  vl VARCHAR(300) CHARACTER SET latin1,
  ch CHAR(4) CHARACTER SET latin1,
  x  INT,
  PRIMARY KEY USING HASH (id)
) ENGINE=NDB;
CREATE TABLE sr_n (
  n_id INT NOT NULL,
  n_s  VARCHAR(8) CHARACTER SET latin1,
  PRIMARY KEY USING HASH (n_id)
) ENGINE=NDB;
INSERT INTO sr_t VALUES
  (1, 'apple', 'kiwi', 'abcd', 10),
  (2, 'banana', 'lemon', 'bcde', 20),
  (3, 'cherry', 'mango', 'cdef', 30);
INSERT INTO sr_n VALUES (1, NULL);
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === sr-1: VARCHAR(8) scalar root, the filter keeps the row ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_sr_1.sql
--write_file $QUERY_FILE
WITH s AS (SELECT MAX(vs) AS m FROM sr_t)
SELECT m FROM s WHERE m > 'b';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT MAX(vs) AS m FROM sr_t)
SELECT m FROM s WHERE m > 'b'|

--echo
--echo === sr-2: VARCHAR(8) scalar root, the filter rejects the row ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_sr_2.sql
--write_file $QUERY_FILE
WITH s AS (SELECT MIN(vs) AS m FROM sr_t)
SELECT m FROM s WHERE m > 'b';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT MIN(vs) AS m FROM sr_t)
SELECT m FROM s WHERE m > 'b'|

--echo
--echo === sr-3: VARCHAR(300) scalar root, the filter keeps the row ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_sr_3.sql
--write_file $QUERY_FILE
WITH s AS (SELECT MAX(vl) AS m FROM sr_t)
SELECT m FROM s WHERE m >= 'm';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT MAX(vl) AS m FROM sr_t)
SELECT m FROM s WHERE m >= 'm'|

--echo
--echo === sr-4: VARCHAR(300) scalar root, the filter rejects the row ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_sr_4.sql
--write_file $QUERY_FILE
WITH s AS (SELECT MAX(vl) AS m FROM sr_t)
SELECT m FROM s WHERE m = 'kiwi';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT MAX(vl) AS m FROM sr_t)
SELECT m FROM s WHERE m = 'kiwi'|

--echo
--echo === sr-5: CHAR(4) scalar root (control), the filter keeps the row ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_sr_5.sql
--write_file $QUERY_FILE
WITH s AS (SELECT MAX(ch) AS m FROM sr_t)
SELECT m FROM s WHERE m = 'cdef';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT MAX(ch) AS m FROM sr_t)
SELECT m FROM s WHERE m = 'cdef'|

--echo
--echo === sr-6: CHAR(4) scalar root (control), the filter rejects the row ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_sr_6.sql
--write_file $QUERY_FILE
WITH s AS (SELECT MIN(ch) AS m FROM sr_t)
SELECT m FROM s WHERE m > 'b';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT MIN(ch) AS m FROM sr_t)
SELECT m FROM s WHERE m > 'b'|

--echo
--echo === sr-7: VARCHAR(8) first output, the filter on COUNT keeps the row ===
let $QUERY=
WITH s AS (SELECT MAX(vs) AS m, COUNT(*) AS n FROM sr_t)
SELECT m, n FROM s WHERE n = 3;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT MAX(vs) AS m, COUNT(*) AS n FROM sr_t)
SELECT m, n FROM s WHERE n = 3|

--echo
--echo === sr-8: VARCHAR(300) first output, the filter on SUM rejects the row ===
let $QUERY=
WITH s AS (SELECT MIN(vl) AS m, SUM(x) AS t FROM sr_t)
SELECT m, t FROM s WHERE t > 100;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT MIN(vl) AS m, SUM(x) AS t FROM sr_t)
SELECT m, t FROM s WHERE t > 100|

--echo
--echo === sr-9: aggregate over a VARCHAR(8) scalar root, the row is kept ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_sr_9.sql
--write_file $QUERY_FILE
WITH s AS (SELECT MAX(vs) AS m, COUNT(*) AS n FROM sr_t)
SELECT COUNT(*) AS cnt, SUM(n) AS sn FROM s WHERE m > 'b';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT MAX(vs) AS m, COUNT(*) AS n FROM sr_t)
SELECT COUNT(*) AS cnt, SUM(n) AS sn FROM s WHERE m > 'b'|

--echo
--echo === sr-10: aggregate over a VARCHAR(300) scalar root, the row is rejected ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_sr_10.sql
--write_file $QUERY_FILE
WITH s AS (SELECT MAX(vl) AS m, COUNT(*) AS n FROM sr_t)
SELECT COUNT(*) AS cnt, SUM(n) AS sn FROM s WHERE m < 'l';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT MAX(vl) AS m, COUNT(*) AS n FROM sr_t)
SELECT COUNT(*) AS cnt, SUM(n) AS sn FROM s WHERE m < 'l'|

--echo
--echo === sr-11: VARCHAR(8) scalar root, comparison with a NULL subquery ===
let $QUERY=
WITH s AS (SELECT MAX(vs) AS m FROM sr_t)
SELECT m FROM s WHERE m = (SELECT MAX(n_s) FROM sr_n);|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT MAX(vs) AS m FROM sr_t)
SELECT m FROM s WHERE m = (SELECT MAX(n_s) FROM sr_n)|

--echo
--echo === sr-12: aggregate over a VARCHAR(300) scalar root, NULL subquery ===
let $QUERY=
WITH s AS (SELECT MAX(vl) AS m, COUNT(*) AS n FROM sr_t)
SELECT COUNT(*) AS cnt, SUM(n) AS sn FROM s
WHERE m > (SELECT MAX(n_s) FROM sr_n);|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT MAX(vl) AS m, COUNT(*) AS n FROM sr_t)
SELECT COUNT(*) AS cnt, SUM(n) AS sn FROM s
WHERE m > (SELECT MAX(n_s) FROM sr_n)|

DELIMITER ;|

--let $QUERY=

--disable_query_log
--disable_warnings
DROP TABLE sr_n;
DROP TABLE sr_t;
--enable_warnings
--enable_query_log
