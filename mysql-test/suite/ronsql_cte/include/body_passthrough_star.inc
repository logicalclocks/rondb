# body_passthrough_star.inc — star-schema fan-out (projection-only)
# family (non_aggregate_phase_3.md), data-driven suite.
#
# Phase 3 covers multiple children linked to the SAME parent: lookup
# stars (N readTuple children, incl. self-join stars via two key
# columns of one fact row) and bushy scan stars (PK-prefix INDEX_SCAN
# siblings — the NI_REPEAT_SCAN_RESULT case, where DBSPJ produces the
# CROSS PRODUCT of the sibling scans per parent row, exactly MySQL's
# semantics for the same flat SQL; the result sizes are the surprise,
# not the values).  Fan-out was already structurally admitted by the
# gate, planner, emit and drain; this phase adds coverage plus two
# gaps found while planning: alias-uniqueness validation in the
# planner (duplicate aliases silently bound to the first match before)
# and the EXPLAIN topology annotation ("<- parent" on child ops — a
# star and a chain printed identically before).
#
# Local tables for the bushy case (shared-schema PKs are USING HASH —
# no PK-prefix ordered index):
#   star_e: e_id 1..50
#   star_m: 4 rows per parent (m_seq 1..4), all 50 parents
#   star_v: 3 rows per parent (v_seq 1..3), parents 1..40 only
#           (41..50 empty -> LEFT-branch NULL extension)
#
# Case map (sr-N):
#   1  self-join lookup star: orders -> customer AS cu (o_custkey)
#      + customer AS c2 (o_clerk), both INNER (NULL clerks drop rows)
#   2  same with LEFT on the clerk branch (NULL-extended per branch)
#   3  star grafted onto a snowflake: orders -> {customer -> nation}
#      + customer AS c2
#   4  mixed star: customer -> nation (PK lookup) + orders (INDEX_SCAN)
#   5  bushy scan star: star_e -> star_m + star_v (4x3 cross/parent)
#   6  sr-5 + per-branch WHERE filters
#   7  bushy stress: full star_e, 40 parents x 12 = 480 rows (parents
#      41..50 dropped by the INNER star_v branch)
#   8  LEFT scan branch: parents without star_v rows NULL-extend
#   9  projection touching only the root (empty-projection guards x2)
#   10 EXPLAIN topology pins on sr-1's shape (both children "<- o")
#   P1 rejection: duplicate alias (MySQL ER_NONUNIQ_TABLE parity)

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
CREATE TABLE star_e (
  e_id   INT NOT NULL,
  e_name CHAR(8) NOT NULL,
  PRIMARY KEY (e_id)
) ENGINE=NDB;
CREATE TABLE star_m (
  m_eid INT NOT NULL,
  m_seq INT NOT NULL,
  m_val INT NOT NULL,
  PRIMARY KEY (m_eid, m_seq)
) ENGINE=NDB;
CREATE TABLE star_v (
  v_eid INT NOT NULL,
  v_seq INT NOT NULL,
  v_tag INT NOT NULL,
  PRIMARY KEY (v_eid, v_seq)
) ENGINE=NDB;
CREATE TABLE _d4 (d INT) ENGINE=MEMORY;
INSERT INTO _d4 VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
CREATE TABLE _seq4 (n INT NOT NULL PRIMARY KEY) ENGINE=MEMORY;
INSERT INTO _seq4 SELECT a.d*10 + b.d + 1 FROM _d4 a, _d4 b
                  WHERE a.d*10 + b.d + 1 <= 50;
INSERT INTO star_e SELECT n, CONCAT('E', n) FROM _seq4;
INSERT INTO star_m SELECT n, s.d, n*10 + s.d FROM _seq4, _d4 s
                   WHERE s.d BETWEEN 1 AND 4;
INSERT INTO star_v SELECT n, s.d, n*100 + s.d FROM _seq4, _d4 s
                   WHERE s.d BETWEEN 1 AND 3 AND n <= 40;
DROP TABLE _seq4;
DROP TABLE _d4;
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === sr-1: self-join lookup star, both branches INNER ===
let $QUERY=
SELECT o.o_orderkey, cu.c_mktsegment AS m1, c2.c_mktsegment AS m2
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN customer AS c2 ON c2.c_custkey = o.o_clerk
WHERE o.o_orderkey < 40;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o.o_orderkey, cu.c_mktsegment AS m1, c2.c_mktsegment AS m2
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN customer AS c2 ON c2.c_custkey = o.o_clerk
WHERE o.o_orderkey < 40|

--echo
--echo === sr-2: self-join lookup star, LEFT on the clerk branch ===
let $QUERY=
SELECT o.o_orderkey, cu.c_mktsegment AS m1, c2.c_mktsegment AS m2
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
LEFT JOIN customer AS c2 ON c2.c_custkey = o.o_clerk
WHERE o.o_orderkey < 40;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o.o_orderkey, cu.c_mktsegment AS m1, c2.c_mktsegment AS m2
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
LEFT JOIN customer AS c2 ON c2.c_custkey = o.o_clerk
WHERE o.o_orderkey < 40|

--echo
--echo === sr-3: star grafted onto a snowflake chain ===
let $QUERY=
SELECT o.o_orderkey, cu.c_custkey, n.n_name, c2.c_mktsegment AS m2
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
JOIN customer AS c2 ON c2.c_custkey = o.o_clerk
WHERE o.o_orderkey < 30;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o.o_orderkey, cu.c_custkey, n.n_name, c2.c_mktsegment AS m2
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
JOIN customer AS c2 ON c2.c_custkey = o.o_clerk
WHERE o.o_orderkey < 30|

--echo
--echo === sr-4: mixed star — PK-lookup branch + INDEX_SCAN branch ===
let $QUERY=
SELECT cu.c_custkey, n.n_name, o.o_orderkey
FROM customer AS cu
JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
JOIN orders AS o ON o.o_custkey = cu.c_custkey
WHERE cu.c_custkey < 15;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT cu.c_custkey, n.n_name, o.o_orderkey
FROM customer AS cu
JOIN nation AS n ON n.n_nationkey = cu.c_nationkey
JOIN orders AS o ON o.o_custkey = cu.c_custkey
WHERE cu.c_custkey < 15|

--echo
--echo === sr-5: bushy scan star — 4x3 cross product per parent ===
let $QUERY=
SELECT e.e_id, m.m_seq, m.m_val, v.v_seq, v.v_tag
FROM star_e AS e
JOIN star_m AS m ON m.m_eid = e.e_id
JOIN star_v AS v ON v.v_eid = e.e_id
WHERE e.e_id < 6;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT e.e_id, m.m_seq, m.m_val, v.v_seq, v.v_tag
FROM star_e AS e
JOIN star_m AS m ON m.m_eid = e.e_id
JOIN star_v AS v ON v.v_eid = e.e_id
WHERE e.e_id < 6|

--echo
--echo === sr-6: bushy scan star + per-branch WHERE bounds ===
# child_bounds (next_steps.md item 2, body_child_bounds.inc family):
# the per-branch constant predicates now emit as index bounds after
# the join-key prefix on the ordered composite PKs — m_seq >= 2 a low
# bound, v_seq <= 2 an upper-only bound (the scanFrag_fixupBound
# renumbering fix case) — instead of interpreted filters.
let $QUERY=
SELECT e.e_id, m.m_seq, v.v_seq
FROM star_e AS e
JOIN star_m AS m ON m.m_eid = e.e_id
JOIN star_v AS v ON v.v_eid = e.e_id
WHERE e.e_id < 6 AND m.m_seq >= 2 AND v.v_seq <= 2;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_sr_6.out
let $QUERY=
SELECT e.e_id, m.m_seq, v.v_seq
FROM star_e AS e
JOIN star_m AS m ON m.m_eid = e.e_id
JOIN star_v AS v ON v.v_eid = e.e_id
WHERE e.e_id < 6 AND m.m_seq >= 2 AND v.v_seq <= 2;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Bounds: m_seq >= 2' $EXPLAIN_FILE
--exec grep -qF 'Bounds: v_seq <= 2' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT e.e_id, m.m_seq, v.v_seq
FROM star_e AS e
JOIN star_m AS m ON m.m_eid = e.e_id
JOIN star_v AS v ON v.v_eid = e.e_id
WHERE e.e_id < 6 AND m.m_seq >= 2 AND v.v_seq <= 2|

--echo
--echo === sr-7: bushy stress — full scan, 40 parents x 12 rows ===
# Parents 41..50 have no star_v rows and drop via the INNER branch.
let $QUERY=
SELECT e.e_id, m.m_seq, v.v_seq
FROM star_e AS e
JOIN star_m AS m ON m.m_eid = e.e_id
JOIN star_v AS v ON v.v_eid = e.e_id;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT e.e_id, m.m_seq, v.v_seq
FROM star_e AS e
JOIN star_m AS m ON m.m_eid = e.e_id
JOIN star_v AS v ON v.v_eid = e.e_id|

--echo
--echo === sr-8: LEFT scan branch — parents without star_v NULL-extend ===
let $QUERY=
SELECT e.e_id, m.m_seq, v.v_seq, v.v_tag
FROM star_e AS e
JOIN star_m AS m ON m.m_eid = e.e_id
LEFT JOIN star_v AS v ON v.v_eid = e.e_id
WHERE e.e_id > 35 AND e.e_id <= 45;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT e.e_id, m.m_seq, v.v_seq, v.v_tag
FROM star_e AS e
JOIN star_m AS m ON m.m_eid = e.e_id
LEFT JOIN star_v AS v ON v.v_eid = e.e_id
WHERE e.e_id > 35 AND e.e_id <= 45|

--echo
--echo === sr-9: projection touching only the root ===
let $QUERY=
SELECT e.e_id
FROM star_e AS e
JOIN star_m AS m ON m.m_eid = e.e_id
JOIN star_v AS v ON v.v_eid = e.e_id
WHERE e.e_id = 7;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT e.e_id
FROM star_e AS e
JOIN star_m AS m ON m.m_eid = e.e_id
JOIN star_v AS v ON v.v_eid = e.e_id
WHERE e.e_id = 7|

--echo
--echo === sr-10: EXPLAIN topology annotation on the star shape ===
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_sr_10.out
let $QUERY=
SELECT o.o_orderkey, cu.c_mktsegment AS m1, c2.c_mktsegment AS m2
FROM orders AS o
JOIN customer AS cu ON cu.c_custkey = o.o_custkey
JOIN customer AS c2 ON c2.c_custkey = o.o_clerk
WHERE o.o_orderkey < 40;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'PK_LOOKUP customer AS cu  <- o' $EXPLAIN_FILE || true
--exec grep -qF 'PK_LOOKUP customer AS c2  <- o' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE

DELIMITER ;|

--echo
--echo === sr-P1 (rejection): duplicate table alias ===
# MySQL ER_NONUNIQ_TABLE parity — a duplicate alias previously bound
# ON conditions silently to the first match.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'SELECT cu.c_custkey FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey JOIN customer AS cu ON cu.c_custkey = o.o_clerk;' 2>&1

--disable_query_log
--disable_warnings
DROP TABLE star_v;
DROP TABLE star_m;
DROP TABLE star_e;
--enable_warnings
--enable_query_log
--source suite/ronsql_cte/include/cte_drop.inc
