# body_count_star.inc — COUNT(*) fragment-stats shortcut
# (ronsql_count_star_shortcut_plan.md W2), data-driven suite.
#
# FOCUS: SELECT COUNT(*) FROM t (single table, no WHERE / GROUP BY /
# HAVING, COUNT-over-constant outputs only) is answered from the
# per-fragment ROW_COUNT pseudo-column (one interpreted
# exit_last_row pseudo-row per fragment, summed) instead of scanning —
# mysqld's own COUNT(*) fast path, so both engines read the same
# committed statistic and parity holds by construction.  The ×5
# topology suites matter here: the per-fragment sum must be correct
# across every node-group / replica layout.
#
# Case map:
#   cst-1  COUNT(*) over region (5 rows) + EXPLAIN pin of the
#          "fragment-stats COUNT(*)" access line
#   cst-2  COUNT(*) over orders (1500 rows, many fragments)
#   cst-3  COUNT(*) over an empty local table -> 0
#   cst-4  LIMIT 0 suppression (prints nothing)
#   cst-5  COUNT(1) — the parser lowers COUNT(*) to COUNT(1), so any
#          constant counts every row; also eligible
#   cst-6  several COUNT(*) outputs in one SELECT
#   cst-7  main-level ORDER BY alias — printer no-op on one row
#   cst-8  COUNT(*) as an uncorrelated scalar subquery — the nested
#          inner preparer takes the shortcut, substitution unchanged
#   cst-c1 control: COUNT(nullable_col) counts non-NULL -> NOT
#          eligible (EXPLAIN absence pin), scan path, correct value
#   cst-c2 control: COUNT(*) with WHERE -> scan path
#   cst-c3 control: COUNT(*) with GROUP BY -> aggregation path
#   cst-c4 control: COUNT(*) alongside MAX -> aggregation path
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
CREATE TABLE cstempty (
  ce_id INT NOT NULL,
  ce_val INT NOT NULL,
  PRIMARY KEY USING HASH (ce_id)
) ENGINE=NDB;
--enable_query_log

DELIMITER |;

--echo
--echo === cst-1: COUNT(*) over region + EXPLAIN pin ===
let $QUERY=
SELECT COUNT(*) AS c FROM region;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_cst_1.out
let $QUERY=
SELECT COUNT(*) AS c FROM region;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'fragment-stats COUNT(*)' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c FROM region|

--echo
--echo === cst-2: COUNT(*) over orders (1500 rows, many fragments) ===
let $QUERY=
SELECT COUNT(*) AS c FROM orders;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c FROM orders|

--echo
--echo === cst-3: COUNT(*) over an empty table -> 0 ===
let $QUERY=
SELECT COUNT(*) AS c FROM cstempty;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c FROM cstempty|

--echo
--echo === cst-4: LIMIT 0 suppression ===
let $QUERY=
SELECT COUNT(*) AS c FROM orders LIMIT 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c FROM orders LIMIT 0|

--echo
--echo === cst-5: COUNT(1) is the same shortcut ===
let $QUERY=
SELECT COUNT(1) AS c FROM orders;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(1) AS c FROM orders|

--echo
--echo === cst-6: several COUNT(*) outputs ===
let $QUERY=
SELECT COUNT(*) AS a, COUNT(*) AS b FROM region;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS a, COUNT(*) AS b FROM region|

--echo
--echo === cst-7: ORDER BY alias — printer no-op on one row ===
let $QUERY=
SELECT COUNT(*) AS c FROM orders ORDER BY c;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c FROM orders ORDER BY c|

--echo
--echo === cst-8: COUNT(*) as an uncorrelated scalar subquery ===
# execute_subqueries builds a full nested RonSQLPreparer for the inner
# SQL, so the shortcut fires INSIDE the inner execute() and the
# substituted value comes from the fragment-stats read (region -> 5).
let $QUERY=
SELECT COUNT(*) AS c FROM customer
WHERE c_custkey <= (SELECT COUNT(*) FROM region);|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c FROM customer
WHERE c_custkey <= (SELECT COUNT(*) FROM region)|

--echo
--echo === cst-c1: control — COUNT(nullable) is NOT the shortcut ===
# o_clerk is NULL for every 7th orderkey; COUNT(o_clerk) counts
# non-NULL values and must scan.  EXPLAIN must NOT carry the tag.
let $QUERY=
SELECT COUNT(o_clerk) AS c FROM orders;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_cst_c1.out
let $QUERY=
SELECT COUNT(o_clerk) AS c FROM orders;|
--source suite/ronsql/include/ronsql_explain.inc
--error 1
--exec grep -qF 'fragment-stats COUNT(*)' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT COUNT(o_clerk) AS c FROM orders|

--echo
--echo === cst-c2: control — WHERE forces the scan path ===
let $QUERY=
SELECT COUNT(*) AS c FROM orders WHERE o_custkey = 42;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c FROM orders WHERE o_custkey = 42|

--echo
--echo === cst-c3: control — GROUP BY forces the aggregation path ===
let $QUERY=
SELECT o_orderstatus AS s, COUNT(*) AS c FROM orders
GROUP BY o_orderstatus;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderstatus AS s, COUNT(*) AS c FROM orders
GROUP BY o_orderstatus|

--echo
--echo === cst-c4: control — COUNT(*) alongside MAX is not eligible ===
let $QUERY=
SELECT COUNT(*) AS c, MAX(o_orderkey) AS m FROM orders;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c, MAX(o_orderkey) AS m FROM orders|

DELIMITER ;|

--disable_query_log
DROP TABLE cstempty;
--enable_query_log
--source suite/ronsql_cte/include/cte_drop.inc
