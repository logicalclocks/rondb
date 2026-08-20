# body_passthrough_snowflake.inc — real-table snowflake pushed join
# (projection-only) family (non_aggregate_phase_2.md), data-driven
# suite.
#
# Phase 2 accepts the first non-aggregate JOINs with no CTE anywhere:
# projection-only left-deep chains over real tables, following foreign
# keys.  Lookup children (PK / unique index) ride the API's fully green
# path; ordered-index-scan children make the query a MultiScanQuery
# (RT_REPEAT_SCAN_RESULT batch protocol); LEFT OUTER joins NULL-extend
# via the drain's per-op isRowNULL substitution.  The gate change is
# one dropped conjunct (the at-least-one-CTE requirement); the planner
# gains two pre-checks (linked-column identity incl. BLOB rejection,
# and the internal-node budget where a unique lookup counts as 2).
#
# Case map (sn-N):
#   1   2-level PK chain (orders -> customer)
#   2   3-level PK chain (orders -> customer -> nation), EXPLAIN greps
#   3   4-level PK chain (... -> region) + per-table WHERE on root + mid
#   4   readTuple root (full PK equality) + PK chain
#   5   readTuple root + residual lookup filter (Phase 0a) + PK child
#   6   scan-config INDEX_SCAN root + PK child
#   7   INDEX_SCAN child (customer -> orders via idx_o_custkey)
#   8   3-level scan-scan chain (nation -> customer -> orders)
#   9   full customer JOIN orders (300 -> 1500 rows) — batch stress
#   10  LEFT JOIN on a nullable key (o_clerk) -> NULL-extended rows
#   11  same join INNER -> NULL keys drop rows
#   12  UNIQUE_LOOKUP child (local snow1.s_uk; 2 internal tree nodes)
#   13  projection touching only the root (child empty-projection guard)
#   14  LEFT child under an INNER chain (always matched)
#   15  INNER below a LEFT child — promoted to all-INNER by the Part B
#       LEFT->INNER pre-pass (join_nest_semantics_plan.md); the Part A
#       probe had recorded wrong results pre-promotion.  EXPLAIN pins
#       [INNER] on the promoted op.
#   P1  rejection: type-mismatch join columns (TINYINT vs INT)
#   P2  rejection: join column with no usable index
#   P3  rejection: cross-table WHERE residual on a pass-through join

--source include/have_ndb.inc
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings
--let $suppress_ronsql_cli=yes
--let $strict_diff=yes
--source suite/ronsql_cte/include/cte_schema.inc
--source suite/ronsql_cte/include/cte_data.inc

# Family-local table with a UNIQUE secondary index (the shared schema
# has none): s_uk mirrors s_id so customer keys 1..40 match.
--disable_query_log
--disable_warnings
CREATE TABLE snow1 (
  s_id  INT NOT NULL,
  s_uk  INT NOT NULL,
  s_val INT NOT NULL,
  PRIMARY KEY (s_id),
  UNIQUE KEY uk_snow1 (s_uk)
) ENGINE=NDB;
CREATE TABLE _d3 (d INT) ENGINE=MEMORY;
INSERT INTO _d3 VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
INSERT INTO snow1 SELECT a.d*10 + b.d + 1, a.d*10 + b.d + 1,
                         (a.d*10 + b.d + 1) * 7
                  FROM _d3 a, _d3 b WHERE a.d*10 + b.d + 1 <= 40;
DROP TABLE _d3;
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === sn-1: 2-level PK chain ===
let $QUERY=
SELECT o.o_orderkey, cu.c_mktsegment
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_orderkey < 20;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o.o_orderkey, cu.c_mktsegment
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_orderkey < 20|

--echo
--echo === sn-2: 3-level PK chain ===
let $QUERY=
SELECT o.o_orderkey, cu.c_mktsegment, n.n_name
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
WHERE o.o_orderkey < 15;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_sn_2.out
let $QUERY=
SELECT o.o_orderkey, cu.c_mktsegment, n.n_name
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
WHERE o.o_orderkey < 15;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[ROOT] TABLE_SCAN orders AS o' $EXPLAIN_FILE || true
--exec grep -qF '[INNER] PK_LOOKUP customer AS cu' $EXPLAIN_FILE || true
--exec grep -qF '[INNER] PK_LOOKUP nation AS n' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT o.o_orderkey, cu.c_mktsegment, n.n_name
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
WHERE o.o_orderkey < 15|

--echo
--echo === sn-3: 4-level PK chain + per-table WHERE on root and mid ===
let $QUERY=
SELECT o.o_orderkey, cu.c_mktsegment, n.n_name, r.r_name
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
JOIN region AS r ON r.r_regionkey = n.n_regionkey
WHERE o.o_orderkey < 50 AND cu.c_nationkey < 12;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o.o_orderkey, cu.c_mktsegment, n.n_name, r.r_name
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
JOIN region AS r ON r.r_regionkey = n.n_regionkey
WHERE o.o_orderkey < 50 AND cu.c_nationkey < 12|

--echo
--echo === sn-4: readTuple root (full PK equality) + PK chain ===
let $QUERY=
SELECT o.o_orderkey, cu.c_mktsegment, n.n_name
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
WHERE o.o_orderkey = 77;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o.o_orderkey, cu.c_mktsegment, n.n_name
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
WHERE o.o_orderkey = 77|

--echo
--echo === sn-5: readTuple root + residual lookup filter + PK child ===
let $QUERY=
SELECT o.o_orderkey, cu.c_mktsegment
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_orderkey = 77 AND o.o_shippriority >= 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o.o_orderkey, cu.c_mktsegment
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_orderkey = 77 AND o.o_shippriority >= 0|

--echo
--echo === sn-6: scan-config INDEX_SCAN root + PK child ===
let $QUERY=
SELECT cu.c_custkey, n.n_name
FROM customer AS cu JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
WHERE cu.c_nationkey = 7;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT cu.c_custkey, n.n_name
FROM customer AS cu JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
WHERE cu.c_nationkey = 7|

--echo
--echo === sn-7: INDEX_SCAN child ===
let $QUERY=
SELECT cu.c_custkey, o.o_orderkey, o.o_orderstatus
FROM customer AS cu JOIN orders AS o ON o.o_custkey = cu.c_custkey
WHERE cu.c_custkey < 30;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_sn_7.out
let $QUERY=
SELECT cu.c_custkey, o.o_orderkey, o.o_orderstatus
FROM customer AS cu JOIN orders AS o ON o.o_custkey = cu.c_custkey
WHERE cu.c_custkey < 30;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[INNER] INDEX_SCAN orders AS o' $EXPLAIN_FILE || true
--exec grep -qF 'Index: idx_o_custkey' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT cu.c_custkey, o.o_orderkey, o.o_orderstatus
FROM customer AS cu JOIN orders AS o ON o.o_custkey = cu.c_custkey
WHERE cu.c_custkey < 30|

--echo
--echo === sn-8: 3-level scan-scan chain ===
let $QUERY=
SELECT n.n_nationkey, cu.c_custkey, o.o_orderkey
FROM nation AS n
JOIN customer AS cu ON cu.c_nationkey = n.n_nationkey
JOIN orders AS o ON o.o_custkey = cu.c_custkey
WHERE n.n_nationkey < 4;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT n.n_nationkey, cu.c_custkey, o.o_orderkey
FROM nation AS n
JOIN customer AS cu ON cu.c_nationkey = n.n_nationkey
JOIN orders AS o ON o.o_custkey = cu.c_custkey
WHERE n.n_nationkey < 4|

--echo
--echo === sn-9: full customer JOIN orders — batch stress ===
let $QUERY=
SELECT cu.c_custkey, o.o_orderkey
FROM customer AS cu JOIN orders AS o ON o.o_custkey = cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT cu.c_custkey, o.o_orderkey
FROM customer AS cu JOIN orders AS o ON o.o_custkey = cu.c_custkey|

--echo
--echo === sn-10: LEFT JOIN on a nullable key — NULL-extended rows ===
let $QUERY=
SELECT o.o_orderkey, o.o_clerk, cl.c_mktsegment
FROM orders AS o LEFT JOIN customer AS cl ON cl.c_custkey = o.o_clerk
WHERE o.o_orderkey < 40;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o.o_orderkey, o.o_clerk, cl.c_mktsegment
FROM orders AS o LEFT JOIN customer AS cl ON cl.c_custkey = o.o_clerk
WHERE o.o_orderkey < 40|

--echo
--echo === sn-11: same join INNER — NULL keys drop rows ===
let $QUERY=
SELECT o.o_orderkey, o.o_clerk, cl.c_mktsegment
FROM orders AS o JOIN customer AS cl ON cl.c_custkey = o.o_clerk
WHERE o.o_orderkey < 40;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o.o_orderkey, o.o_clerk, cl.c_mktsegment
FROM orders AS o JOIN customer AS cl ON cl.c_custkey = o.o_clerk
WHERE o.o_orderkey < 40|

--echo
--echo === sn-12: UNIQUE_LOOKUP child ===
let $QUERY=
SELECT cu.c_custkey, s.s_val
FROM customer AS cu JOIN snow1 AS s ON s.s_uk = cu.c_custkey
WHERE cu.c_custkey < 12;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_sn_12.out
let $QUERY=
SELECT cu.c_custkey, s.s_val
FROM customer AS cu JOIN snow1 AS s ON s.s_uk = cu.c_custkey
WHERE cu.c_custkey < 12;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[INNER] UNIQUE_LOOKUP snow1 AS s' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT cu.c_custkey, s.s_val
FROM customer AS cu JOIN snow1 AS s ON s.s_uk = cu.c_custkey
WHERE cu.c_custkey < 12|

--echo
--echo === sn-13: projection touching only the root ===
let $QUERY=
SELECT cu.c_custkey
FROM customer AS cu JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
WHERE cu.c_custkey < 6;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT cu.c_custkey
FROM customer AS cu JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
WHERE cu.c_custkey < 6|

--echo
--echo === sn-14: LEFT child under an INNER chain (always matched) ===
let $QUERY=
SELECT o.o_orderkey, cu.c_custkey, n.n_name
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
LEFT JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
WHERE o.o_orderkey < 25;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o.o_orderkey, cu.c_custkey, n.n_name
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
LEFT JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
WHERE o.o_orderkey < 25|

--echo
--echo === sn-15: INNER below a LEFT child (promoted to all-INNER) ===
# History: the Part A probe recorded wrong results (RonSQL delivered
# the o_clerk-IS-NULL rows as "o_orderkey NULL NULL" — the emitted
# tree expressed `o LEFT (cl INNER n)` instead of SQL's
# left-associative `(o LEFT cl) INNER n`).  The Part B LEFT->INNER
# promotion (join_nest_semantics_plan.md) rewrites the chain to
# all-INNER, matching MySQL; the EXPLAIN grep pins the promotion.
let $QUERY=
SELECT o.o_orderkey, cl.c_custkey, n.n_name
FROM orders AS o
LEFT JOIN customer AS cl ON cl.c_custkey = o.o_clerk
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey
WHERE o.o_orderkey < 40;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_sn_15.out
let $QUERY=
SELECT o.o_orderkey, cl.c_custkey, n.n_name
FROM orders AS o
LEFT JOIN customer AS cl ON cl.c_custkey = o.o_clerk
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey
WHERE o.o_orderkey < 40;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[INNER] PK_LOOKUP customer AS cl' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT o.o_orderkey, cl.c_custkey, n.n_name
FROM orders AS o
LEFT JOIN customer AS cl ON cl.c_custkey = o.o_clerk
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey
WHERE o.o_orderkey < 40|

DELIMITER ;|

--echo
--echo === sn-P1 (rejection): type-mismatch join columns ===
# r_regionkey is TINYINT, c_custkey is INT — the NDB API's linked-operand
# rule requires identical declarations; the planner now rejects cleanly.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'SELECT cu.c_custkey, r.r_name FROM customer AS cu JOIN region AS r ON r.r_regionkey = cu.c_custkey;' 2>&1

--echo
--echo === sn-P2 (rejection): join column with no usable index ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'SELECT o.o_orderkey FROM orders AS o JOIN lineitem AS l ON l.l_quantity = o.o_orderkey;' 2>&1

--echo
--echo === sn-P3 (rejection): cross-table WHERE residual ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'SELECT o.o_orderkey FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey WHERE cu.c_acctbal > o.o_totalprice;' 2>&1

--disable_query_log
--disable_warnings
DROP TABLE snow1;
--enable_warnings
--enable_query_log
--source suite/ronsql_cte/include/cte_drop.inc
