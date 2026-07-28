# body_frags_per_worker.inc — FRAGS_PER_WORKER family, data-driven CTE suite.
#
# Coverage for the configurable fragsPerWorker feature
# (frags_per_worker_plan.md): the RonSQL statement-head option
#   [EXPLAIN] [FRAGS_PER_WORKER = <n>] [WITH ...] SELECT ...
# bundles n root fragments per SPJ worker for aggregate pushed queries.
#
# Every K variant is strict-diffed against the SAME bare-SQL MySQL baseline
# (the prefix travels only on the RDRS RonSQL side via $RONSQL_PREFIX in
# ronsql_compare.inc), so result equality across K = unset/1/2/4 follows
# transitively.  Shapes:
#
#   fpw-1..4  fs_batch shape — CTE_LOOKUP-probed aggregating CTE joined to a
#             real-table root with a range WHERE; K = unset, 1, 2, 4.
#             Exercises DBTC chunked SPJ-instance assignment, the combined
#             [fragCount, fragIds..., aggKeys...] SCAN_FRAGREQ section, and
#             the DBSPJ CTE-root multi-fragment scan.
#   fpw-5     non-CTE join aggregate with K = 4 — pure kernel bundling
#             without the CTE machinery.
#   fpw-6     root-lookup CTE query (WHERE pk = const) with K = 4 — the
#             lookup fires exactly once cluster-wide on the request with
#             rootFragId 0 (compareFragLocation fragId tiebreak).
#   fpw-7     scanCte main root (FROM cte) with K = 4 — pinned to
#             fragsPerWorker = 1 by the API (cte_scan_start virtual-fragment
#             mapping); must still return correct results.
#   fpw-8     EXPLAIN with the option — header line printed.
#   fpw-9     FRAGS_PER_WORKER = 0 — parse error.
#
# K > 1 only takes effect when the per-node fragment count is a
# power-of-two multiple of K; the API silently halves otherwise.  Result
# correctness (what this file asserts) is K-independent by design.

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

--echo
--echo === fpw-1: fs_batch shape, baseline (no FRAGS_PER_WORKER) ===

let $QUERY=
WITH cust_features AS (
  SELECT o_custkey AS k, COUNT(*) AS order_cnt, SUM(o_shippriority) AS prio_sum,
         MAX(o_orderdate) AS last_order
  FROM orders WHERE o_custkey >= 100 AND o_custkey < 200
  GROUP BY o_custkey)
SELECT c.c_custkey, COUNT(*), SUM(cust_features.order_cnt),
       SUM(cust_features.prio_sum), MAX(cust_features.last_order)
FROM customer AS c JOIN cust_features ON cust_features.k = c.c_custkey
WHERE c.c_custkey >= 100 AND c.c_custkey < 200
GROUP BY c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === fpw-2: fs_batch shape, FRAGS_PER_WORKER = 1 ===

--let $RONSQL_PREFIX=FRAGS_PER_WORKER = 1
let $QUERY=
WITH cust_features AS (
  SELECT o_custkey AS k, COUNT(*) AS order_cnt, SUM(o_shippriority) AS prio_sum,
         MAX(o_orderdate) AS last_order
  FROM orders WHERE o_custkey >= 100 AND o_custkey < 200
  GROUP BY o_custkey)
SELECT c.c_custkey, COUNT(*), SUM(cust_features.order_cnt),
       SUM(cust_features.prio_sum), MAX(cust_features.last_order)
FROM customer AS c JOIN cust_features ON cust_features.k = c.c_custkey
WHERE c.c_custkey >= 100 AND c.c_custkey < 200
GROUP BY c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === fpw-3: fs_batch shape, FRAGS_PER_WORKER = 2 ===

--let $RONSQL_PREFIX=FRAGS_PER_WORKER = 2
let $QUERY=
WITH cust_features AS (
  SELECT o_custkey AS k, COUNT(*) AS order_cnt, SUM(o_shippriority) AS prio_sum,
         MAX(o_orderdate) AS last_order
  FROM orders WHERE o_custkey >= 100 AND o_custkey < 200
  GROUP BY o_custkey)
SELECT c.c_custkey, COUNT(*), SUM(cust_features.order_cnt),
       SUM(cust_features.prio_sum), MAX(cust_features.last_order)
FROM customer AS c JOIN cust_features ON cust_features.k = c.c_custkey
WHERE c.c_custkey >= 100 AND c.c_custkey < 200
GROUP BY c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === fpw-4: fs_batch shape, FRAGS_PER_WORKER = 4 ===

--let $RONSQL_PREFIX=FRAGS_PER_WORKER = 4
let $QUERY=
WITH cust_features AS (
  SELECT o_custkey AS k, COUNT(*) AS order_cnt, SUM(o_shippriority) AS prio_sum,
         MAX(o_orderdate) AS last_order
  FROM orders WHERE o_custkey >= 100 AND o_custkey < 200
  GROUP BY o_custkey)
SELECT c.c_custkey, COUNT(*), SUM(cust_features.order_cnt),
       SUM(cust_features.prio_sum), MAX(cust_features.last_order)
FROM customer AS c JOIN cust_features ON cust_features.k = c.c_custkey
WHERE c.c_custkey >= 100 AND c.c_custkey < 200
GROUP BY c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === fpw-5: non-CTE join aggregate, FRAGS_PER_WORKER = 4 ===

--let $RONSQL_PREFIX=FRAGS_PER_WORKER = 4
let $QUERY=
SELECT c.c_nationkey, COUNT(*), SUM(o.o_shippriority)
FROM customer AS c JOIN orders AS o ON o.o_custkey = c.c_custkey
GROUP BY c.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === fpw-6: root-lookup CTE query (WHERE pk = const), FRAGS_PER_WORKER = 4 ===

--let $RONSQL_PREFIX=FRAGS_PER_WORKER = 4
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, SUM(sums.t)
FROM customer AS c JOIN sums ON sums.k = c.c_custkey
WHERE c.c_custkey = 42
GROUP BY c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc

--echo
--echo === fpw-7: scanCte main root (pinned to 1), FRAGS_PER_WORKER = 4 ===

--let $RONSQL_PREFIX=FRAGS_PER_WORKER = 4
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_custkey >= 100 AND o_custkey < 200
  GROUP BY o_custkey)
SELECT MAX(sums.t), COUNT(*)
FROM sums;|
--source suite/ronsql/include/ronsql_compare.inc

DELIMITER ;|

--echo
--echo === fpw-8: EXPLAIN prints the requested value ===
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_fpw_8.out
--let $QUERY=FRAGS_PER_WORKER = 4 SELECT c.c_nationkey, COUNT(*) FROM customer AS c JOIN orders AS o ON o.o_custkey = c.c_custkey GROUP BY c.c_nationkey;
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -oF 'FRAGS_PER_WORKER = 4 (requested' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE

--echo
--echo === fpw-9: FRAGS_PER_WORKER = 0 is a parse error ===
--exec printf '{"query":"FRAGS_PER_WORKER = 0 SELECT COUNT(*) FROM orders;","database":"test","outputFormat":"TEXT"}' > $MYSQL_TMP_DIR/fpw-err-body
--exec curl -s -X POST -H "Content-Type: application/json" http://$RDRS_NOKEY_HOST:$RDRS_NOKEY_PORT/0.1.0/ronsql --data-binary @$MYSQL_TMP_DIR/fpw-err-body | grep -oF 'FRAGS_PER_WORKER must be a positive integer'
--remove_file $MYSQL_TMP_DIR/fpw-err-body

--source suite/ronsql_cte/include/cte_drop.inc
