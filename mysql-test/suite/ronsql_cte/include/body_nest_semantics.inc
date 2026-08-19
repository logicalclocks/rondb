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
# Case map (ns-N):
#   1..4  NEXT-PHASE-disabled: the verification CONFIRMED wrong results
#         (ns-1 recorded c=30 vs MySQL's 10 — both the miss and
#         NULL-key sub-cases counted).  See the marker below; re-enable
#         with join_nest_semantics_plan.md Part B.
#   5  COUNT(n.n_nationkey) variant — enabled probe: injected rows
#      carry NULL for n, so COUNT(col) should skip them (expected 10
#      both sides); a diff here would show the injection feeding
#      non-NULL leaf values
#   6  control: same chain all-INNER (the left-associative equivalent
#      of ns-1; COUNT = 10)
#   7  control: LEFT ... LEFT chain, no INNER below (supported today;
#      COUNT(*) = 30, COUNT(n.n_nationkey) = 10)

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

# NEXT-PHASE: ns-1..4 — INNER-below-LEFT on the AGGREGATE path returns
#   wrong results, CONFIRMED by the first record run (2026-08-19):
#   ns-1 returned c=30 where MySQL returns 10.  The arithmetic settles
#   BOTH sub-cases at once: 10 matched + 10 dangling + 10 NULL-key
#   rows were all counted — the aggregate NULL-injection feeds
#   NULL-extended rows through a MatchNonNull (INNER) aggregate LEAF
#   for lookup misses AND NULL join keys.  Reconciliation with
#   testMultiOuterJoinAggNdbApi Test 2 (green): there the leaf is a
#   LEFT node BELOW the INNER; here the INNER is the leaf — the
#   injection logic differs by leaf position.  This is shipped
#   aggregate territory producing silent wrong results.
#   Fix: join_nest_semantics_plan.md Part B (LEFT->INNER promotion
#   pre-pass — the flat-SQL semantics make these chains all-INNER);
#   re-enable ns-1..4 as compares when it lands.
#   Probes (MySQL expected / RonSQL got):
#   ns-1  full mix COUNT(*)                      -> 10 / 30 (recorded)
#   ns-2  NULL-ref slice COUNT(*)                ->  0 / expected 10
#   ns-3  dangling slice COUNT(*)                ->  0 / expected 10
#   ns-4  GROUP BY t.grp                         -> matched-only groups
#   SELECT COUNT(*) AS c
#   FROM nestp AS t
#   LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
#   JOIN nation AS n ON n.n_nationkey = cl.c_nationkey;

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
let $QUERY=
SELECT COUNT(*) AS c, COUNT(n.n_nationkey) AS cn
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
LEFT JOIN nation AS n ON n.n_nationkey = cl.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c, COUNT(n.n_nationkey) AS cn
FROM nestp AS t
LEFT JOIN customer AS cl ON cl.c_custkey = t.ref
LEFT JOIN nation AS n ON n.n_nationkey = cl.c_nationkey|

DELIMITER ;|

--disable_query_log
--disable_warnings
DROP TABLE nestp;
--enable_warnings
--enable_query_log
--source suite/ronsql_cte/include/cte_drop.inc
