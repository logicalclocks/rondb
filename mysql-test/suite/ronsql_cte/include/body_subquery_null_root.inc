# body_subquery_null_root.inc — a scalar subquery that returns NULL or no
# row, in the WHERE of a query over a CTE (X-3 = J-11 in
# storage/ndb/claude_files/ronsql_executor/review_dispatch_lifecycle.md
# and review_join.md).
#
# The subquery result replaces its node at execution time, after the
# root's access was planned. A NULL result became the key of a lookupCte
# root ("Expected integer constant for CTE root lookup key"), a filter
# constant, or the key of a real-table root, which encode_constant
# refused as a stale schema. The constants are now folded into the WHERE
# with three-valued logic, and a WHERE that is TRUE for no row makes the
# root read no row. Single-table and real-table join cases are in
# ronsql_subquery_null_where (suite ronsql).
#
# Subqueries: (SELECT MAX(e_x) FROM sn_e GROUP BY e_id) returns no row,
# (SELECT MAX(n_x) FROM sn_n) returns NULL.
#
# Case map (sn-N):
#   1-5   grouped CTE root: key, OR, IN list, IS NULL, aggregate without
#         GROUP BY
#   6-7   scalar CTE root
#   8-10  real-table root joined to a CTE: aggregate, OR, primary key
#   11    scalar CTE root under an aggregate
#
# A scalar CTE root whose WHERE is never TRUE (sn-6, sn-11) is the
# dummy-key lookupCte with a filter that rejects the row, as any filtered
# scalar root. As a filtered scanCte it hung: DBLQH's scalar CTE_SCAN arm
# counted the rejected group as a row sent, and the API waited for it
# until the receive timeout.

--source include/have_ndb.inc

--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings

--let $suppress_ronsql_cli=yes
--let $strict_diff=yes

--disable_query_log
--disable_warnings
CREATE TABLE sn_o (
  id INT NOT NULL,
  c  INT NOT NULL,
  v  INT,
  PRIMARY KEY USING HASH (id)
) ENGINE=NDB;
CREATE TABLE sn_l (
  id INT NOT NULL,
  ok INT NOT NULL,
  PRIMARY KEY USING HASH (id)
) ENGINE=NDB;
CREATE TABLE sn_e (
  e_id INT NOT NULL,
  e_x  INT,
  PRIMARY KEY USING HASH (e_id)
) ENGINE=NDB;
CREATE TABLE sn_n (
  n_id INT NOT NULL,
  n_x  INT,
  PRIMARY KEY USING HASH (n_id)
) ENGINE=NDB;
INSERT INTO sn_o VALUES
  (1, 1, 10), (2, 1, 20), (3, 2, NULL), (4, 2, 40), (5, 3, 50), (6, 3, NULL);
INSERT INTO sn_l VALUES
  (1, 1), (2, 1), (3, 2), (4, 3), (5, 5), (6, 6), (7, 6);
INSERT INTO sn_n VALUES (1, NULL);
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === sn-1: grouped CTE root, key compared with NULL ===
let $QUERY=
WITH s AS (SELECT c AS k, SUM(v) AS m FROM sn_o GROUP BY c)
SELECT k, m FROM s WHERE k = (SELECT MAX(n_x) FROM sn_n);|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT c AS k, SUM(v) AS m FROM sn_o GROUP BY c)
SELECT k, m FROM s WHERE k = (SELECT MAX(n_x) FROM sn_n)|

--echo
--echo === sn-2: grouped CTE root, the key is the other side of an OR ===
let $QUERY=
WITH s AS (SELECT c AS k, SUM(v) AS m FROM sn_o GROUP BY c)
SELECT k, m FROM s
WHERE m > (SELECT MAX(e_x) FROM sn_e GROUP BY e_id) OR k = 2;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT c AS k, SUM(v) AS m FROM sn_o GROUP BY c)
SELECT k, m FROM s
WHERE m > (SELECT MAX(e_x) FROM sn_e GROUP BY e_id) OR k = 2|

--echo
--echo === sn-3: grouped CTE root, IN list with a NULL subquery ===
let $QUERY=
WITH s AS (SELECT c AS k, SUM(v) AS m FROM sn_o GROUP BY c)
SELECT k, m FROM s WHERE k IN (1, 3, (SELECT MAX(n_x) FROM sn_n));|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT c AS k, SUM(v) AS m FROM sn_o GROUP BY c)
SELECT k, m FROM s WHERE k IN (1, 3, (SELECT MAX(n_x) FROM sn_n))|

--echo
--echo === sn-4: grouped CTE root, IS NULL of a NULL subquery is TRUE ===
let $QUERY=
WITH s AS (SELECT c AS k, COUNT(*) AS n FROM sn_o GROUP BY c)
SELECT k, n FROM s WHERE (SELECT MAX(n_x) FROM sn_n) IS NULL;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH s AS (SELECT c AS k, COUNT(*) AS n FROM sn_o GROUP BY c)
SELECT k, n FROM s WHERE (SELECT MAX(n_x) FROM sn_n) IS NULL|

--echo
--echo === sn-5: aggregate without GROUP BY over a grouped CTE root ===
let $QUERY=
WITH s AS (SELECT c AS k, SUM(v) AS m FROM sn_o GROUP BY c)
SELECT COUNT(*) AS cnt, SUM(m) AS sm FROM s
WHERE k = (SELECT MAX(e_x) FROM sn_e GROUP BY e_id);|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT c AS k, SUM(v) AS m FROM sn_o GROUP BY c)
SELECT COUNT(*) AS cnt, SUM(m) AS sm FROM s
WHERE k = (SELECT MAX(e_x) FROM sn_e GROUP BY e_id)|

--echo
--echo === sn-6: scalar CTE root, comparison with no row ===
let $QUERY=
WITH s AS (SELECT MAX(v) AS m FROM sn_o)
SELECT m FROM s WHERE m > (SELECT MAX(e_x) FROM sn_e GROUP BY e_id);|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT MAX(v) AS m FROM sn_o)
SELECT m FROM s WHERE m > (SELECT MAX(e_x) FROM sn_e GROUP BY e_id)|

--echo
--echo === sn-7: scalar CTE root, the NULL comparison drops out of an OR ===
let $QUERY=
WITH s AS (SELECT MAX(v) AS m FROM sn_o)
SELECT m FROM s WHERE m < (SELECT MAX(n_x) FROM sn_n) OR m > 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT MAX(v) AS m FROM sn_o)
SELECT m FROM s WHERE m < (SELECT MAX(n_x) FROM sn_n) OR m > 0|

--echo
--echo === sn-8: real-table root joined to a CTE, root filter ===
let $QUERY=
WITH ls AS (SELECT ok AS k, COUNT(*) AS m FROM sn_l GROUP BY ok)
SELECT o.c, SUM(ls.m) AS m_sum
FROM sn_o AS o JOIN ls ON ls.k = o.id
WHERE o.v > (SELECT MAX(n_x) FROM sn_n)
GROUP BY o.c;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH ls AS (SELECT ok AS k, COUNT(*) AS m FROM sn_l GROUP BY ok)
SELECT o.c, SUM(ls.m) AS m_sum
FROM sn_o AS o JOIN ls ON ls.k = o.id
WHERE o.v > (SELECT MAX(n_x) FROM sn_n)
GROUP BY o.c|

--echo
--echo === sn-9: real-table root joined to a CTE, OR ===
let $QUERY=
WITH ls AS (SELECT ok AS k, COUNT(*) AS m FROM sn_l GROUP BY ok)
SELECT o.c, SUM(ls.m) AS m_sum
FROM sn_o AS o JOIN ls ON ls.k = o.id
WHERE o.v > (SELECT MAX(n_x) FROM sn_n) OR o.c = 3
GROUP BY o.c;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH ls AS (SELECT ok AS k, COUNT(*) AS m FROM sn_l GROUP BY ok)
SELECT o.c, SUM(ls.m) AS m_sum
FROM sn_o AS o JOIN ls ON ls.k = o.id
WHERE o.v > (SELECT MAX(n_x) FROM sn_n) OR o.c = 3
GROUP BY o.c|

--echo
--echo === sn-10: real-table root joined to a CTE, primary key, pass-through ===
let $QUERY=
WITH ls AS (SELECT ok AS k, COUNT(*) AS m FROM sn_l GROUP BY ok)
SELECT o.id, ls.m
FROM sn_o AS o JOIN ls ON ls.k = o.id
WHERE o.id = (SELECT MAX(e_x) FROM sn_e GROUP BY e_id);|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH ls AS (SELECT ok AS k, COUNT(*) AS m FROM sn_l GROUP BY ok)
SELECT o.id, ls.m
FROM sn_o AS o JOIN ls ON ls.k = o.id
WHERE o.id = (SELECT MAX(e_x) FROM sn_e GROUP BY e_id)|

--echo
--echo === sn-11: scalar CTE root under an aggregate, comparison with NULL ===
let $QUERY=
WITH s AS (SELECT MAX(v) AS m FROM sn_o)
SELECT COUNT(*) AS cnt, SUM(m) AS sm FROM s
WHERE m = (SELECT MAX(n_x) FROM sn_n);|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH s AS (SELECT MAX(v) AS m FROM sn_o)
SELECT COUNT(*) AS cnt, SUM(m) AS sm FROM s
WHERE m = (SELECT MAX(n_x) FROM sn_n)|

DELIMITER ;|

--let $QUERY=

--disable_query_log
--disable_warnings
DROP TABLE sn_n;
DROP TABLE sn_e;
DROP TABLE sn_l;
DROP TABLE sn_o;
--enable_warnings
--enable_query_log
