# body_child_bounds.inc — child-local constant index bounds on non-root
# INDEX_SCAN join children (next_steps.md join-root follow-ups item 2 +
# item 3 best-child-index scoring; findings/child_bounds.md), data-driven
# suite.
#
# Child-local constant predicates (e.g. `c_seq >= 3` on a child whose
# ordered index is (c_pid, c_seq)) previously emitted as interpreted
# filters; they now become constant range bounds after the linked
# join-key prefix — bounds prune reads, filters only prune transfer.
# The planner also re-selects the child index when another join-key-
# prefix candidate binds strictly more columns (item 3), and the DBSPJ
# scanFrag_fixupBound renumbering fix makes upper-only bound tails
# (EQ,...,EQ,GE) correct.  EXPLAIN greps are FATAL from day one; the
# family's plans are deterministic.
#
# Local tables: cb_p/cb_c give the ordered-composite-PK parent/child
# shapes; cb_x carries the two-candidate-index (idx_x_a vs idx_x_ab)
# scoring setup; cb_c additionally has idx_cb_c_pn (nullable second
# column — pins the v1 NOT NULL guard) and idx_cb_c_3 (3-column
# EQ-continue shapes).
#
# Case map (cb-N):
#   1   low const bound (c_seq >= 3)
#   2   high-only const bound (c_seq <= 2) — the fixupBound fix case
#   3   closed range (>= AND <= on one column)
#   4   EQ const bound (both sides, BoundEQ collapse at serialization)
#   5   grandchild INDEX_SCAN + const bound, two-candidate scoring
#       (idx_x_ab beats idx_x_a)
#   6   aggregate twin: GROUP BY parent col + SUM/COUNT with the bound
#   7   fallback pin: non-index conjunct stays a filter, no bounds
#   8   v1 nullability guard: conjunct on nullable indexed column
#       stays a filter (idx_cb_c_pn scores zero)
#   9   LEFT JOIN + null-rejecting WHERE conjunct: promoted INNER,
#       then bound
#   10  3-col index EQ-continue: c_seq = k AND c_val >= m both bound
#       (idx_cb_c_3 beats the composite PK in scoring)
#   11  aggregate mixed: cross-table EQ bound + const range bound on
#       one child (RONDB-1044 linked bounds + child_bounds together)
#   12  range blocks later column: c_seq >= k AND c_val = m — c_val
#       stays a filter

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
CREATE TABLE cb_p (
  p_id  INT NOT NULL,
  p_grp INT NOT NULL,
  PRIMARY KEY (p_id)
) ENGINE=NDB;
CREATE TABLE cb_c (
  c_pid  INT NOT NULL,
  c_seq  INT NOT NULL,
  c_val  INT NOT NULL,
  c_note INT NULL,
  PRIMARY KEY (c_pid, c_seq)
) ENGINE=NDB;
CREATE INDEX idx_cb_c_pn ON cb_c (c_pid, c_note);
CREATE INDEX idx_cb_c_3 ON cb_c (c_pid, c_seq, c_val);
CREATE TABLE cb_x (
  x_pid INT NOT NULL,
  x_seq INT NOT NULL,
  x_val INT NOT NULL,
  PRIMARY KEY USING HASH (x_pid, x_seq)
) ENGINE=NDB;
CREATE INDEX idx_x_a  ON cb_x (x_pid);
CREATE INDEX idx_x_ab ON cb_x (x_pid, x_seq);
CREATE TABLE _d5 (d INT) ENGINE=MEMORY;
INSERT INTO _d5 VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
INSERT INTO cb_p SELECT a.d*10 + b.d + 1, MOD(a.d*10 + b.d, 4)
                 FROM _d5 a, _d5 b WHERE a.d*10 + b.d + 1 <= 20;
# cb_c: parents 1..20 x seq 1..5; c_note NULL when seq = 3
INSERT INTO cb_c SELECT p.n, s.d, p.n*10 + s.d,
                        IF(s.d = 3, NULL, s.d)
                 FROM (SELECT a.d*10 + b.d + 1 AS n FROM _d5 a, _d5 b
                       WHERE a.d*10 + b.d + 1 <= 20) p, _d5 s
                 WHERE s.d BETWEEN 1 AND 5;
# cb_x: pids 1..5 (joined from c_seq) x seq 1..4
INSERT INTO cb_x SELECT p.d, s.d, p.d*100 + s.d
                 FROM _d5 p, _d5 s
                 WHERE p.d BETWEEN 1 AND 5 AND s.d BETWEEN 1 AND 4;
DROP TABLE _d5;
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === cb-1: low const bound on the child index ===
let $QUERY=
SELECT p.p_id, c.c_seq, c.c_val
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq >= 3;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_cb_1.out
let $QUERY=
SELECT p.p_id, c.c_seq, c.c_val
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq >= 3;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Bounds: c_seq >= 3' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT p.p_id, c.c_seq, c.c_val
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq >= 3|

--echo
--echo === cb-2: high-only const bound (fixupBound fix case) ===
# The upper-only tail after the equality prefix is exactly the bound
# shape the scanFrag_fixupBound renumbering fix makes correct.
let $QUERY=
SELECT p.p_id, c.c_seq
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq <= 2;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_cb_2.out
let $QUERY=
SELECT p.p_id, c.c_seq
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq <= 2;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Bounds: c_seq <= 2' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT p.p_id, c.c_seq
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq <= 2|

--echo
--echo === cb-3: closed const range on one column ===
let $QUERY=
SELECT p.p_id, c.c_seq
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq >= 2 AND c.c_seq <= 4;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_cb_3.out
let $QUERY=
SELECT p.p_id, c.c_seq
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq >= 2 AND c.c_seq <= 4;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Bounds: c_seq >= 2 c_seq <= 4' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT p.p_id, c.c_seq
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq >= 2 AND c.c_seq <= 4|

--echo
--echo === cb-4: EQ const bound ===
let $QUERY=
SELECT p.p_id, c.c_val
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq = 3;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_cb_4.out
let $QUERY=
SELECT p.p_id, c.c_val
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq = 3;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Bounds: c_seq >= 3 c_seq <= 3' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT p.p_id, c.c_val
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq = 3|

--echo
--echo === cb-5: grandchild bound + two-candidate index scoring ===
# cb_x has idx_x_a(x_pid) and idx_x_ab(x_pid, x_seq): only the second
# can serve `x_seq >= 2` as a bound, so re-selection must pick it no
# matter which one findOrderedIndex saw first.
let $QUERY=
SELECT p.p_id, c.c_seq, x.x_seq
FROM cb_p AS p
JOIN cb_c AS c ON c.c_pid = p.p_id
JOIN cb_x AS x ON x.x_pid = c.c_seq
WHERE p.p_id <= 3 AND x.x_seq >= 2;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_cb_5.out
let $QUERY=
SELECT p.p_id, c.c_seq, x.x_seq
FROM cb_p AS p
JOIN cb_c AS c ON c.c_pid = p.p_id
JOIN cb_x AS x ON x.x_pid = c.c_seq
WHERE p.p_id <= 3 AND x.x_seq >= 2;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Bounds: x_seq >= 2' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT p.p_id, c.c_seq, x.x_seq
FROM cb_p AS p
JOIN cb_c AS c ON c.c_pid = p.p_id
JOIN cb_x AS x ON x.x_pid = c.c_seq
WHERE p.p_id <= 3 AND x.x_seq >= 2|

--echo
--echo === cb-6: aggregate twin with the child bound ===
let $QUERY=
SELECT p.p_grp, COUNT(*) AS cnt, SUM(c.c_val) AS s
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq >= 3
GROUP BY p.p_grp;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_cb_6.out
let $QUERY=
SELECT p.p_grp, COUNT(*) AS cnt, SUM(c.c_val) AS s
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq >= 3
GROUP BY p.p_grp;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Bounds: c_seq >= 3' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT p.p_grp, COUNT(*) AS cnt, SUM(c.c_val) AS s
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq >= 3
GROUP BY p.p_grp|

--echo
--echo === cb-7: non-index conjunct stays a filter ===
# c_val follows a range column in idx_cb_c_3 only; on its own it can
# serve no contiguous prefix, so the conjunct stays a residual filter
# and the op carries no bounds.
let $QUERY=
SELECT p.p_id, c.c_val
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_val > 100;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_cb_7.out
let $QUERY=
SELECT p.p_id, c.c_val
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_val > 100;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Residual filter: yes' $EXPLAIN_FILE
--error 1
--exec grep -qF 'Bounds:' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT p.p_id, c.c_val
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_val > 100|

--echo
--echo === cb-8: nullable indexed column stays a filter (v1 guard) ===
# idx_cb_c_pn(c_pid, c_note) could serve the conjunct positionally,
# but c_note is nullable: NULL sorts below every value in an NDB
# ordered index while SQL comparison is UNKNOWN for NULL — the v1
# guard keeps the conjunct as a filter (and the index scores zero).
let $QUERY=
SELECT p.p_id, c.c_note
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_note >= 4;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_cb_8.out
let $QUERY=
SELECT p.p_id, c.c_note
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_note >= 4;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Residual filter: yes' $EXPLAIN_FILE
--error 1
--exec grep -qF 'Bounds:' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT p.p_id, c.c_note
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_note >= 4|

--echo
--echo === cb-9: LEFT JOIN + null-rejecting WHERE -> promoted + bound ===
let $QUERY=
SELECT p.p_id, c.c_seq
FROM cb_p AS p LEFT JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq >= 4;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_cb_9.out
let $QUERY=
SELECT p.p_id, c.c_seq
FROM cb_p AS p LEFT JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq >= 4;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[INNER] INDEX_SCAN cb_c' $EXPLAIN_FILE
--exec grep -qF 'Bounds: c_seq >= 4' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT p.p_id, c.c_seq
FROM cb_p AS p LEFT JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq >= 4|

--echo
--echo === cb-10: 3-col index EQ-continue (both conjuncts bound) ===
# idx_cb_c_3(c_pid, c_seq, c_val): the EQ on c_seq lets the walk
# continue to c_val, and the scorer prefers this index (4) over the
# composite PK (3).
let $QUERY=
SELECT p.p_id, c.c_val
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq = 2 AND c.c_val >= 100;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_cb_10.out
let $QUERY=
SELECT p.p_id, c.c_val
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq = 2 AND c.c_val >= 100;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'c_seq >= 2' $EXPLAIN_FILE
--exec grep -qF 'c_val >= 100' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT p.p_id, c.c_val
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq = 2 AND c.c_val >= 100|

--echo
--echo === cb-11: aggregate mixed cross-table EQ + const range bounds ===
# RONDB-1044 parent-linked bound (c_seq = p.p_grp) and a child-local
# const bound (c_val >= 50) on one child op, via idx_cb_c_3.
let $QUERY=
SELECT SUM(c.c_val) AS s
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq = p.p_grp AND c.c_val >= 50;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_cb_11.out
let $QUERY=
SELECT SUM(c.c_val) AS s
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq = p.p_grp AND c.c_val >= 50;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'c_seq >= p.p_grp' $EXPLAIN_FILE
--exec grep -qF 'c_val >= 50' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT SUM(c.c_val) AS s
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq = p.p_grp AND c.c_val >= 50|

--echo
--echo === cb-12: range blocks the later index column ===
# c_seq >= 2 is a range, so c_val = 105 cannot become a bound on
# idx_cb_c_3 and stays a filter.
let $QUERY=
SELECT p.p_id, c.c_val
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq >= 2 AND c.c_val = 105;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_cb_12.out
let $QUERY=
SELECT p.p_id, c.c_val
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq >= 2 AND c.c_val = 105;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Bounds: c_seq >= 2' $EXPLAIN_FILE
--exec grep -qF 'Residual filter: yes' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT p.p_id, c.c_val
FROM cb_p AS p JOIN cb_c AS c ON c.c_pid = p.p_id
WHERE c.c_seq >= 2 AND c.c_val = 105|

DELIMITER ;|

--disable_query_log
DROP TABLE cb_p;
DROP TABLE cb_c;
DROP TABLE cb_x;
--enable_query_log
--source suite/ronsql_cte/include/cte_drop.inc
