# body_output_layout.inc — the order of a CTE's outputs (C-1 and J-2 in
# storage/ndb/claude_files/ronsql_executor/review_cte.md and
# review_join.md).
#
# The API numbers the columns of a CTE's virtual table in output order,
# and so do the readers of a CTE row (main-query columns, the main
# aggregation program, CTE filters, the keys of a CTE lookup). The data
# nodes number the materialized row as the GROUP BY keys in GROUP BY
# order, then one aggregate result per slot, and an aggregate repeated in
# the outputs shares the slot of its first use. Nothing checked that the
# two agree, so any other layout returned wrong results:
#  - an aggregate before a GROUP BY column swapped the values (ol-e1);
#  - outputs in another order than GROUP BY swapped the keys, and a root
#    lookup on them probed another group (J-2, ol-e2: WHERE g1 = 1 AND
#    g2 = 2 returned the count of group (2, 1));
#  - a GROUP BY column missing from the outputs shifted the columns
#    (ol-e3, ol-e4: n showed the key values);
#  - a repeated aggregate read as NULL (ol-e5, ol-e6), and so did a
#    column after the aggregates (ol-e7).
# Such a CTE is now refused at prepare time with an unsupported error
# (RonSQLPreparer::check_cte_output_layouts): a CTE must output its GROUP
# BY columns first, in GROUP BY order, then its aggregates, each once. A
# GROUP BY column must be written in the outputs as the GROUP BY writes it
# (ol-e8): the virtual primary key, and so the key of a CTE lookup, is
# found by the written column.
#
# Data, ol (id: g1, g2, g3, x, y):
#   1: 1, 2, a, 10, 5        5: 2, 1, b, 50, NULL
#   2: 1, 2, b, 20, NULL     6: 1, 1, b, 60, 9
#   3: 1, 2, a, 30, 7        7: 3, 3, c, 70, 11
#   4: 2, 1, a, 40, NULL
# The groups (g1, g2) = (1, 2) and (2, 1) both exist with different
# counts (3 and 2), so a key sent in the wrong order finds the other one.
# olp (pid: q, pname): 1: 2, one; 2: 1, two; 3: 3, three.
#
# Case map, accepted layouts compared with mysqld (strict_diff):
#   ol-1   one GROUP BY column, then SUM, COUNT, MIN, MAX
#   ol-2   two GROUP BY columns of different types, in GROUP BY order
#   ol-3   AVG (one slot in a CTE body) between other aggregates
#   ol-4   COUNT(*) and COUNT(col) are two aggregates
#   ol-5   scalar CTE (no GROUP BY)
#   ol-6   chained CTEs, both in this layout
#   ol-7   J-2: lookupCte root on both keys, WHERE in the other order
#   ol-8   J-2: lookupCte root joined to a table on its primary key
#   ol-9   CTE lookup child on both keys, ON in the other order
#   ol-10  main aggregate over the CTE with a filter on an aggregate
#   ol-11  J-2: single-group root (the body's WHERE fixes both keys)
#   ol-12  CTE body that joins two tables
# Refused through ronsql_cli:
#   ol-e1  an aggregate before the GROUP BY column
#   ol-e2  GROUP BY columns in another order (J-2 root lookup)
#   ol-e3  a GROUP BY column missing from the outputs
#   ol-e4  no GROUP BY column in the outputs
#   ol-e5  an aggregate repeated
#   ol-e6  an aggregate repeated in a scalar CTE
#   ol-e7  a column after the aggregates (a GROUP BY column twice)
#   ol-e8  a GROUP BY column written another way (o.g1 for g1)
#   ol-e9  ol-e1 through RDRS: HTTP 400, class unsupported, the message

--source include/have_ndb.inc
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings
--let $strict_diff=yes

--disable_query_log
--disable_warnings
CREATE TABLE ol (
  id INT NOT NULL,
  g1 INT NOT NULL,
  g2 INT NOT NULL,
  g3 VARCHAR(8) NOT NULL,
  x  INT NOT NULL,
  y  INT,
  PRIMARY KEY USING HASH (id)
) ENGINE=NDB;
CREATE TABLE olp (
  pid   INT NOT NULL,
  q     INT NOT NULL,
  pname VARCHAR(10) NOT NULL,
  PRIMARY KEY USING HASH (pid)
) ENGINE=NDB;
INSERT INTO ol VALUES
  (1, 1, 2, 'a', 10, 5), (2, 1, 2, 'b', 20, NULL), (3, 1, 2, 'a', 30, 7),
  (4, 2, 1, 'a', 40, NULL), (5, 2, 1, 'b', 50, NULL), (6, 1, 1, 'b', 60, 9),
  (7, 3, 3, 'c', 70, 11);
INSERT INTO olp VALUES (1, 2, 'one'), (2, 1, 'two'), (3, 3, 'three');
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === ol-1: one GROUP BY column, then the aggregates ===
let $QUERY=
WITH c AS (SELECT g1, COUNT(*) AS n, SUM(x) AS s, MIN(x) AS mn, MAX(x) AS mx
FROM ol GROUP BY g1)
SELECT g1, n, s, mn, mx FROM c;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH c AS (SELECT g1, COUNT(*) AS n, SUM(x) AS s, MIN(x) AS mn, MAX(x) AS mx
FROM ol GROUP BY g1)
SELECT g1, n, s, mn, mx FROM c|

--echo
--echo === ol-2: two GROUP BY columns of different types, in GROUP BY order ===
let $QUERY=
WITH c AS (SELECT g3, g1, COUNT(*) AS n, MAX(x) AS mx
FROM ol GROUP BY g3, g1)
SELECT g3, g1, n, mx FROM c;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH c AS (SELECT g3, g1, COUNT(*) AS n, MAX(x) AS mx
FROM ol GROUP BY g3, g1)
SELECT g3, g1, n, mx FROM c|

--echo
--echo === ol-3: AVG between other aggregates ===
let $QUERY=
WITH c AS (SELECT g1, SUM(x) AS s, AVG(x) AS av, COUNT(*) AS n
FROM ol GROUP BY g1)
SELECT g1, s, av, n FROM c;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH c AS (SELECT g1, SUM(x) AS s, AVG(x) AS av, COUNT(*) AS n
FROM ol GROUP BY g1)
SELECT g1, s, av, n FROM c|

--echo
--echo === ol-4: COUNT(*) and COUNT(col) are two aggregates ===
let $QUERY=
WITH c AS (SELECT g1, COUNT(*) AS n, COUNT(y) AS ny, SUM(y) AS sy
FROM ol GROUP BY g1)
SELECT g1, n, ny, sy FROM c;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH c AS (SELECT g1, COUNT(*) AS n, COUNT(y) AS ny, SUM(y) AS sy
FROM ol GROUP BY g1)
SELECT g1, n, ny, sy FROM c|

--echo
--echo === ol-5: scalar CTE, the aggregates in output order ===
let $QUERY=
WITH c AS (SELECT COUNT(*) AS n, SUM(x) AS s, MIN(y) AS mn, MAX(y) AS mx
FROM ol)
SELECT n, s, mn, mx FROM c;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH c AS (SELECT COUNT(*) AS n, SUM(x) AS s, MIN(y) AS mn, MAX(y) AS mx
FROM ol)
SELECT n, s, mn, mx FROM c|

--echo
--echo === ol-6: chained CTEs ===
let $QUERY=
WITH c1 AS (SELECT g1, g2, SUM(x) AS s FROM ol GROUP BY g1, g2),
c2 AS (SELECT g1, COUNT(*) AS n, SUM(s) AS t FROM c1 GROUP BY g1)
SELECT g1, n, t FROM c2;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH c1 AS (SELECT g1, g2, SUM(x) AS s FROM ol GROUP BY g1, g2),
c2 AS (SELECT g1, COUNT(*) AS n, SUM(s) AS t FROM c1 GROUP BY g1)
SELECT g1, n, t FROM c2|

--echo
--echo === ol-7: lookupCte root on both keys, WHERE in the other order ===
let $QUERY=
WITH c AS (SELECT g1, g2, COUNT(*) AS n, SUM(x) AS s
FROM ol GROUP BY g1, g2)
SELECT g1, g2, n, s FROM c WHERE g2 = 2 AND g1 = 1;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH c AS (SELECT g1, g2, COUNT(*) AS n, SUM(x) AS s
FROM ol GROUP BY g1, g2)
SELECT g1, g2, n, s FROM c WHERE g2 = 2 AND g1 = 1|

--echo
--echo === ol-8: lookupCte root joined to a table on its primary key ===
let $QUERY=
WITH c AS (SELECT g1, g2, COUNT(*) AS n FROM ol GROUP BY g1, g2)
SELECT c.g1, c.g2, c.n, p.pname
FROM c JOIN olp AS p ON p.pid = c.g2
WHERE c.g1 = 2 AND c.g2 = 1;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH c AS (SELECT g1, g2, COUNT(*) AS n FROM ol GROUP BY g1, g2)
SELECT c.g1, c.g2, c.n, p.pname
FROM c JOIN olp AS p ON p.pid = c.g2
WHERE c.g1 = 2 AND c.g2 = 1|

--echo
--echo === ol-9: CTE lookup child on both keys, ON in the other order ===
let $QUERY=
WITH c AS (SELECT g1, g2, COUNT(*) AS n FROM ol GROUP BY g1, g2)
SELECT p.pid, c.g1, c.g2, c.n
FROM olp AS p JOIN c ON c.g2 = p.q AND c.g1 = p.pid;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH c AS (SELECT g1, g2, COUNT(*) AS n FROM ol GROUP BY g1, g2)
SELECT p.pid, c.g1, c.g2, c.n
FROM olp AS p JOIN c ON c.g2 = p.q AND c.g1 = p.pid|

--echo
--echo === ol-10: main aggregate over the CTE, filter on an aggregate ===
let $QUERY=
WITH c AS (SELECT g1, COUNT(*) AS n, SUM(x) AS s FROM ol GROUP BY g1)
SELECT SUM(n) AS tn, MAX(s) AS ms FROM c WHERE n > 1;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH c AS (SELECT g1, COUNT(*) AS n, SUM(x) AS s FROM ol GROUP BY g1)
SELECT SUM(n) AS tn, MAX(s) AS ms FROM c WHERE n > 1|

--echo
--echo === ol-11: single-group root, the body's WHERE fixes both keys ===
let $QUERY=
WITH c AS (SELECT g1, g2, COUNT(*) AS n
FROM ol WHERE g2 = 1 AND g1 = 2 GROUP BY g1, g2)
SELECT g1, g2, n FROM c;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH c AS (SELECT g1, g2, COUNT(*) AS n
FROM ol WHERE g2 = 1 AND g1 = 2 GROUP BY g1, g2)
SELECT g1, g2, n FROM c|

--echo
--echo === ol-12: CTE body that joins two tables ===
let $QUERY=
WITH c AS (SELECT p.pname AS pname, COUNT(*) AS n, SUM(o.x) AS s
FROM ol AS o JOIN olp AS p ON p.pid = o.g1 GROUP BY p.pname)
SELECT pname, n, s FROM c;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH c AS (SELECT p.pname AS pname, COUNT(*) AS n, SUM(o.x) AS s
FROM ol AS o JOIN olp AS p ON p.pid = o.g1 GROUP BY p.pname)
SELECT pname, n, s FROM c|

DELIMITER ;|

--echo
--echo === ol-e1: an aggregate before the GROUP BY column ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH c AS (SELECT COUNT(*) AS n, g1 FROM ol GROUP BY g1) SELECT g1, n FROM c;' 2>&1

--echo
--echo === ol-e2: GROUP BY columns in another order, root lookup on both ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH c AS (SELECT g2, g1, COUNT(*) AS n FROM ol GROUP BY g1, g2) SELECT g1, g2, n FROM c WHERE g1 = 1 AND g2 = 2;' 2>&1

--echo
--echo === ol-e3: a GROUP BY column missing from the outputs ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH c AS (SELECT g1, COUNT(*) AS n FROM ol GROUP BY g1, g2) SELECT g1, n FROM c;' 2>&1

--echo
--echo === ol-e4: no GROUP BY column in the outputs ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH c AS (SELECT COUNT(*) AS n FROM ol GROUP BY g1) SELECT n FROM c;' 2>&1

--echo
--echo === ol-e5: an aggregate repeated ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH c AS (SELECT g1, SUM(x) AS s1, SUM(x) AS s2 FROM ol GROUP BY g1) SELECT g1, s1, s2 FROM c;' 2>&1

--echo
--echo === ol-e6: an aggregate repeated in a scalar CTE ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH c AS (SELECT COUNT(*) AS n, MAX(x) AS mx, COUNT(*) AS n2 FROM ol) SELECT n, mx, n2 FROM c;' 2>&1

--echo
--echo === ol-e7: a column after the aggregates ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH c AS (SELECT g1, COUNT(*) AS n, g1 AS k FROM ol GROUP BY g1) SELECT g1, n, k FROM c;' 2>&1

--echo
--echo === ol-e8: a GROUP BY column written another way in the outputs ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH c AS (SELECT o.g1 AS k, COUNT(*) AS n FROM ol AS o GROUP BY g1) SELECT k, n FROM c;' 2>&1

--echo
--echo === ol-e9: ol-e1 through RDRS ===
--exec printf '{"query":"WITH c AS (SELECT COUNT(*) AS n, g1 FROM ol GROUP BY g1) SELECT g1, n FROM c;","database":"test","outputFormat":"TEXT"}' > $MYSQL_TMP_DIR/ol-body
--exec curl -s -o $MYSQL_TMP_DIR/ol-rdrs.out -w 'HTTP %{http_code}\n' -D $MYSQL_TMP_DIR/ol-hdrs -X POST -H "Content-Type: application/json" http://$RDRS_NOKEY_HOST:$RDRS_NOKEY_PORT/0.1.0/ronsql --data-binary @$MYSQL_TMP_DIR/ol-body
--exec grep -i '^x-ronsql-error-class:' $MYSQL_TMP_DIR/ol-hdrs | tr -d '\r' | tr '[:upper:]' '[:lower:]'
--exec grep -F '[unsupported] Caught exception:' $MYSQL_TMP_DIR/ol-rdrs.out
--echo == RDRS still answers ==
--exec printf '{"query":"SELECT COUNT(*) AS n FROM ol;","database":"test","outputFormat":"TEXT"}' > $MYSQL_TMP_DIR/ol-body
--exec curl -s -w 'HTTP %{http_code}\n' -o /dev/null -X POST -H "Content-Type: application/json" http://$RDRS_NOKEY_HOST:$RDRS_NOKEY_PORT/0.1.0/ronsql --data-binary @$MYSQL_TMP_DIR/ol-body
--remove_file $MYSQL_TMP_DIR/ol-body
--remove_file $MYSQL_TMP_DIR/ol-hdrs
--remove_file $MYSQL_TMP_DIR/ol-rdrs.out

--disable_query_log
--disable_warnings
DROP TABLE olp;
DROP TABLE ol;
--enable_warnings
--enable_query_log
