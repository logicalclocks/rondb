# body_nest_semantics.inc — INNER-below-LEFT verification probes
# (join_nest_semantics_plan.md, Part A), data-driven suite.
#
# Closes the open cells from the Phase 2 sn-15 analysis.  SQL is
# left-associative: `t LEFT JOIN cl ... JOIN n ON n.ref(cl)` means
# `(t LEFT cl) INNER n`, which — with null-rejecting equality join
# conditions — is equivalent to all-INNER.  RonSQL's emitted tree
# (n as MatchNonNull child of cl, no nest metadata) expresses
# `t LEFT (cl INNER n)` instead.  On the pass-through row path this
# produced wrong results (sn-15, now gate-rejected); on the AGGREGATE
# path, testMultiOuterJoinAggNdbApi Test 2 proves the kernel
# NULL-injection is INNER-below-aware for lookup MISSES.  These probes
# close the remaining cells — aggregate queries skip the parse gate, so
# they are runnable today, and the first --record run IS the
# verification: a diff pinpoints the broken sub-case and converts that
# case to a NEXT-PHASE marker until the Part B LEFT->INNER promotion
# lands.
#
# Local table nestp: three row classes over 30 rows —
#   p_id  1..10: ref = p_id        (matching customer keys)
#   p_id 11..20: ref = 9000 + p_id (dangling — no such customer)
#   p_id 21..30: ref = NULL
#
# Case map (ns-N) — all compares since the Part B LEFT->INNER
# promotion; ns-1..4 were the Part A wrong-results probes (see the
# history note below):
#   1  COUNT(*) over the full mix — promoted to all-INNER (COUNT = 10;
#      Part A recorded 30 pre-promotion); EXPLAIN pins [INNER]
#   2  NULL-ref slice (COUNT = 0)
#   3  dangling-ref slice (COUNT = 0)
#   4  GROUP BY a root column (matched-only groups)
#   5  COUNT(n.n_nationkey) variant (10 — was green even
#      pre-promotion: injected rows carried NULL leaf values)
#   6  control: the same chain written all-INNER (COUNT = 10 — must
#      equal ns-1 post-promotion)
#   7  control: LEFT ... LEFT chain, no INNER below — promotion must
#      NOT fire (COUNT(*) = 30, COUNT(col) = 10; EXPLAIN pins
#      [LEFT JOIN])
#   8  cascade: LEFT LEFT chain + INNER at the end promotes the whole
#      chain (fixpoint; COUNT = 10; EXPLAIN pins [INNER] on both)

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
CREATE TABLE nestp (
  p_id INT NOT NULL,
  grp  INT NOT NULL,
  ref  INT NULL,
  PRIMARY KEY (p_id)
) ENGINE=NDB;
INSERT INTO nestp VALUES
  (1,1,1),(2,2,2),(3,0,3),(4,1,4),(5,2,5),
  (6,0,6),(7,1,7),(8,2,8),(9,0,9),(10,1,10),
  (11,2,9011),(12,0,9012),(13,1,9013),(14,2,9014),(15,0,9015),
  (16,1,9016),(17,2,9017),(18,0,9018),(19,1,9019),(20,2,9020),
  (21,0,NULL),(22,1,NULL),(23,2,NULL),(24,0,NULL),(25,1,NULL),
  (26,2,NULL),(27,0,NULL),(28,1,NULL),(29,2,NULL),(30,0,NULL);
--enable_warnings
--enable_query_log

DELIMITER |;

# History: ns-1..4 were NEXT-PHASE-disabled after the first record run
# (2026-08-19) CONFIRMED wrong results on the aggregate path — ns-1
# recorded COUNT(*) = 30 vs MySQL's 10 (miss AND NULL-key rows both
# counted through the MatchNonNull aggregate leaf).  Re-enabled by the
# Part B LEFT->INNER promotion pre-pass: SQL's left-associative reading
# makes these chains all-INNER, so the promoted plans never emit an
# INNER-below-LEFT tree.  ns-1's EXPLAIN grep pins the promotion
# ([INNER], not [LEFT JOIN], on the customer op).

--echo
--echo === ns-1: COUNT(*) over the full mix (promoted to all-INNER) ===
let $QUERY=
SELECT COUNT(*) AS c
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_ns_1.out
let $QUERY=
SELECT COUNT(*) AS c
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[INNER] PK_LOOKUP customer AS cl' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey|

--echo
--echo === ns-2: NULL-ref slice ===
let $QUERY=
SELECT COUNT(*) AS c
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey
WHERE t.p_id >= 21;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey
WHERE t.p_id >= 21|

--echo
--echo === ns-3: dangling-ref slice ===
let $QUERY=
SELECT COUNT(*) AS c
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey
WHERE t.p_id >= 11 AND t.p_id < 21;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey
WHERE t.p_id >= 11 AND t.p_id < 21|

--echo
--echo === ns-4: GROUP BY root column over the full mix ===
let $QUERY=
SELECT t.grp AS g, COUNT(*) AS c
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey
GROUP BY t.grp;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT t.grp AS g, COUNT(*) AS c
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey
GROUP BY t.grp|

--echo
--echo === ns-5: COUNT(n.n_nationkey) variant ===
let $QUERY=
SELECT COUNT(n.n_nationkey) AS c
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(n.n_nationkey) AS c
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey|

--echo
--echo === ns-6: control — same chain all-INNER ===
let $QUERY=
SELECT COUNT(*) AS c
FROM nestp AS t
JOIN customer AS cl ON cl.c_custkey = t.ref
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c
FROM nestp AS t
JOIN customer AS cl ON cl.c_custkey = t.ref
JOIN nation AS n ON n.n_nationkey = cl.c_nationkey|

--echo
--echo === ns-7: control — LEFT ... LEFT chain, no INNER below ===
# Both LEFTs stay LEFT: only LEFT joins reference them, so the
# promotion must NOT fire ([LEFT JOIN] pinned by the EXPLAIN grep).
let $QUERY=
SELECT COUNT(*) AS c, COUNT(n.n_nationkey) AS cn
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
LEFT JOIN nation AS n ON n.n_nationkey = cl.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_ns_7.out
let $QUERY=
SELECT COUNT(*) AS c, COUNT(n.n_nationkey) AS cn
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
LEFT JOIN nation AS n ON n.n_nationkey = cl.c_nationkey;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[LEFT JOIN] PK_LOOKUP customer AS cl' $EXPLAIN_FILE || true
--exec grep -qF '[LEFT JOIN] PK_LOOKUP nation AS n' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c, COUNT(n.n_nationkey) AS cn
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
LEFT JOIN nation AS n ON n.n_nationkey = cl.c_nationkey|

--echo
--echo === ns-8: cascade — INNER at the end promotes the whole LEFT chain ===
# region INNER-references nation, which promotes nation; nation's ON
# references customer, which promotes customer — the fixpoint cascade.
# Left-associative SQL agrees: region's null-rejecting equality drops
# every NULL-extended row, so the chain is all-INNER (COUNT = 10).
let $QUERY=
SELECT COUNT(*) AS c
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
LEFT JOIN nation AS n ON n.n_nationkey = cl.c_nationkey
JOIN region AS r ON r.r_regionkey = n.n_regionkey;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_ns_8.out
let $QUERY=
SELECT COUNT(*) AS c
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
LEFT JOIN nation AS n ON n.n_nationkey = cl.c_nationkey
JOIN region AS r ON r.r_regionkey = n.n_regionkey;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF '[INNER] PK_LOOKUP customer AS cl' $EXPLAIN_FILE || true
--exec grep -qF '[INNER] PK_LOOKUP nation AS n' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
LEFT JOIN nation AS n ON n.n_nationkey = cl.c_nationkey
JOIN region AS r ON r.r_regionkey = n.n_regionkey|

DELIMITER ;|

--disable_query_log
--disable_warnings
DROP TABLE nestp;
--enable_warnings
--enable_query_log
--source suite/ronsql_cte/include/cte_drop.inc
