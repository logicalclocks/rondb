# body_chained_keys.inc — GROUP BY keys passed up a chain of CTEs keep
# their output metadata (O-1 in
# storage/ndb/claude_files/ronsql_executor/review_output.md).
#
# A column of CTE b that refers to CTE a has no plumbed source column,
# so build_result_column_metadata took the metadata from
# resolve_chained_column_type, but kept only a non-zero scale. A string
# key lost its charset: with ORDER BY the sort compared it with a NULL
# CHARSET_INFO (a crash), without ORDER BY printing failed with "Could
# not find charset for VARCHAR column" (a stale-schema retry). A
# DECIMAL(p,0) key lost its precision and was printed from an
# uninitialized buffer. The charset and the DECIMAL precision are now
# kept.
#
# Temporal MIN/MAX outputs passed up a chain still print as packed
# integers; that is C-6 / O-3 / O-10 and is not covered here.
#
# ORDER BY cases compare the output unsorted ($skip_sort), so the order
# itself is checked against mysqld.

--source include/have_ndb.inc

--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings

--let $suppress_ronsql_cli=yes
--let $strict_diff=yes

--disable_query_log
--disable_warnings
CREATE TABLE ck_t (
  pk INT NOT NULL,
  s  VARCHAR(20) NOT NULL,
  k  DECIMAL(10,0) NOT NULL,
  k2 DECIMAL(10,2) NOT NULL,
  v  INT NOT NULL,
  PRIMARY KEY USING HASH (pk)
) ENGINE=NDB;
INSERT INTO ck_t VALUES
  (1, 'pear',   12345, 10.50, 1),
  (2, 'apple',  7,     2.25,  2),
  (3, 'fig',    12345, 10.50, 3),
  (4, 'apple',  99,    2.25,  4),
  (5, 'pear',   7,     7.75,  5),
  (6, 'banana', 99,    7.75,  6);
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === ck-1: VARCHAR key through two CTEs, ORDER BY the key ===
--let $skip_sort=yes
let $QUERY=
WITH a AS (SELECT s, COUNT(*) AS n FROM ck_t GROUP BY s),
     b AS (SELECT s, SUM(n) AS n2 FROM a GROUP BY s)
SELECT s, SUM(n2) FROM b GROUP BY s ORDER BY s;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH a AS (SELECT s, COUNT(*) AS n FROM ck_t GROUP BY s),
     b AS (SELECT s, SUM(n) AS n2 FROM a GROUP BY s)
SELECT s, SUM(n2) FROM b GROUP BY s ORDER BY s|

--echo
--echo === ck-2: VARCHAR key through two CTEs, no ORDER BY ===
let $QUERY=
WITH a AS (SELECT s, COUNT(*) AS n FROM ck_t GROUP BY s),
     b AS (SELECT s, SUM(n) AS n2 FROM a GROUP BY s)
SELECT s, SUM(n2) FROM b GROUP BY s;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (SELECT s, COUNT(*) AS n FROM ck_t GROUP BY s),
     b AS (SELECT s, SUM(n) AS n2 FROM a GROUP BY s)
SELECT s, SUM(n2) FROM b GROUP BY s|

--echo
--echo === ck-3: VARCHAR key through three CTEs, ORDER BY the key DESC ===
--let $skip_sort=yes
let $QUERY=
WITH a AS (SELECT s, SUM(v) AS t FROM ck_t GROUP BY s),
     b AS (SELECT s, SUM(t) AS t2 FROM a GROUP BY s),
     c AS (SELECT s, SUM(t2) AS t3 FROM b GROUP BY s)
SELECT s, SUM(t3) FROM c GROUP BY s ORDER BY s DESC;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH a AS (SELECT s, SUM(v) AS t FROM ck_t GROUP BY s),
     b AS (SELECT s, SUM(t) AS t2 FROM a GROUP BY s),
     c AS (SELECT s, SUM(t2) AS t3 FROM b GROUP BY s)
SELECT s, SUM(t3) FROM c GROUP BY s ORDER BY s DESC|

--echo
--echo === ck-4: DECIMAL(10,0) key through two CTEs ===
let $QUERY=
WITH a AS (SELECT k, COUNT(*) AS n FROM ck_t GROUP BY k),
     b AS (SELECT k, SUM(n) AS n2 FROM a GROUP BY k)
SELECT k, SUM(n2) FROM b GROUP BY k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (SELECT k, COUNT(*) AS n FROM ck_t GROUP BY k),
     b AS (SELECT k, SUM(n) AS n2 FROM a GROUP BY k)
SELECT k, SUM(n2) FROM b GROUP BY k|

--echo
--echo === ck-5: DECIMAL(10,0) key through two CTEs, ORDER BY the key DESC ===
--let $skip_sort=yes
let $QUERY=
WITH a AS (SELECT k, COUNT(*) AS n FROM ck_t GROUP BY k),
     b AS (SELECT k, SUM(n) AS n2 FROM a GROUP BY k)
SELECT k, SUM(n2) FROM b GROUP BY k ORDER BY k DESC;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH a AS (SELECT k, COUNT(*) AS n FROM ck_t GROUP BY k),
     b AS (SELECT k, SUM(n) AS n2 FROM a GROUP BY k)
SELECT k, SUM(n2) FROM b GROUP BY k ORDER BY k DESC|

--echo
--echo === ck-6: DECIMAL(10,2) key through two CTEs (control: scale > 0 kept before) ===
let $QUERY=
WITH a AS (SELECT k2, COUNT(*) AS n FROM ck_t GROUP BY k2),
     b AS (SELECT k2, SUM(n) AS n2 FROM a GROUP BY k2)
SELECT k2, SUM(n2) FROM b GROUP BY k2;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (SELECT k2, COUNT(*) AS n FROM ck_t GROUP BY k2),
     b AS (SELECT k2, SUM(n) AS n2 FROM a GROUP BY k2)
SELECT k2, SUM(n2) FROM b GROUP BY k2|

DELIMITER ;|

--let $QUERY=

--disable_query_log
--disable_warnings
DROP TABLE ck_t;
--enable_warnings
--enable_query_log
