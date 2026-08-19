# body_root_pk_residual.inc — root PK-equality cover + residual WHERE
# conjuncts family (non_aggregate_phase_0.md, Phase 0a), data-driven CTE
# suite.
#
# Before Phase 0a, emit_root_op's PK-equality-covered branches (real
# readTuple root, PK-covered + scan-child ordered-index root, full-key
# lookupCte root) built keys from the matched equalities and silently
# DROPPED every remaining WHERE conjunct — wrong results in exactly the
# shapes below.  These cases lock in the fixed behavior: residual
# conjuncts are applied as interpreted filters (lookup ops capped at 64
# words with a scan fallback), contradictory PK equalities yield empty
# results, and unsupportable residual atoms on a CTE-root lookup reject
# cleanly instead of succeeding wrongly.
#
# Case map (rpr-N):
#   1/2   aggregate join, readTuple root + accepting / rejecting residual
#   3/4   aggregate join, ordered-PK root (local acct) + INDEX_SCAN child
#         + accepting / rejecting residual
#   5/6   E.3 passthrough, full-key CTE root + accepting / rejecting
#         residual on an aggregate output
#   7/8   I.8 passthrough chain, PK-covered real root + accepting /
#         rejecting residual
#   9/10  contradictory PK equalities (aggregate / passthrough) -> empty
#   11    over-cap residual on a hash-PK root -> table-scan fallback
#   12    over-cap residual on an ordered-PK root -> PK-index-scan
#         fallback
#   13/14 agg + CTE, readTuple root + accepting / rejecting residual
#         (the scan-rooted compound shape where an aggregate readTuple
#         main root is protocol-legal)
#   P1    rejection: full-key CTE root + col-vs-col residual atom
#
# NOTE (discovered by rpr-1 on first record): aggregate queries WITHOUT
# CTEs whose root is PK-equality-covered used to emit a readTuple root,
# producing a lookup-rooted (TCKEYREQ) aggregate query — a shape the
# JoinAgg protocol does not support (DBTC's JOIN_AGG_SETUP only exists
# on the SCAN_TABREQ path), crashing data nodes at DbspjMain.cpp:8748
# (empty m_aggNodes).  Pre-existing, reachable before Phase 0a with a
# plain full-PK WHERE.  Fixed by emit-side suppression (scan fallbacks)
# + NdbQueryDefImpl prepare-time rejection; rpr-1/2/9/11 now exercise
# the scan fallbacks.

--source include/have_ndb.inc
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings
--let $suppress_ronsql_cli=yes
--let $strict_diff=yes
--source suite/ronsql_cte/include/cte_schema.inc
--source suite/ronsql_cte/include/cte_data.inc

# Family-local table with an ORDERED primary key (the shared schema is
# hash-PK only): a_id 1..200, a_grp = a_id % 10, a_val = a_id * 3.
--disable_query_log
--disable_warnings
CREATE TABLE acct (
  a_id  INT NOT NULL,
  a_grp INT NOT NULL,
  a_val INT NOT NULL,
  PRIMARY KEY (a_id)
) ENGINE=NDB;
CREATE TABLE _d2 (d INT) ENGINE=MEMORY;
INSERT INTO _d2 VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
CREATE TABLE _seq2 (n INT NOT NULL PRIMARY KEY) ENGINE=MEMORY;
INSERT INTO _seq2 SELECT a.d*100 + b.d*10 + c.d + 1 FROM _d2 a, _d2 b, _d2 c;
INSERT INTO acct SELECT n, MOD(n,10), n*3 FROM _seq2 WHERE n <= 200;
DROP TABLE _seq2;
DROP TABLE _d2;
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === rpr-1: agg join, readTuple root, residual accepts the row ===
let $QUERY=
SELECT COUNT(*) AS c, SUM(cu.c_nationkey) AS s
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_orderkey = 77 AND o.o_custkey < 401;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c, SUM(cu.c_nationkey) AS s
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_orderkey = 77 AND o.o_custkey < 401|

--echo
--echo === rpr-2: agg join, readTuple root, residual rejects the row ===
# Pre-0a this returned COUNT = 1 (the residual was dropped).
let $QUERY=
SELECT COUNT(*) AS c, SUM(cu.c_nationkey) AS s
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_orderkey = 77 AND o.o_custkey < 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c, SUM(cu.c_nationkey) AS s
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_orderkey = 77 AND o.o_custkey < 0|

--echo
--echo === rpr-3: ordered-PK root + INDEX_SCAN child, residual accepts ===
# acct has an ordered PK, orders joins via idx_o_custkey (scan child), so
# the root emits as a PK-equality ordered-index scan; the a_val residual
# rides its interpreted filter.
let $QUERY=
SELECT COUNT(*) AS c, SUM(o.o_shippriority) AS s
FROM acct AS a JOIN orders AS o ON o.o_custkey = a.a_grp
WHERE a.a_id = 7 AND a.a_val > 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c, SUM(o.o_shippriority) AS s
FROM acct AS a JOIN orders AS o ON o.o_custkey = a.a_grp
WHERE a.a_id = 7 AND a.a_val > 0|

--echo
--echo === rpr-4: ordered-PK root + INDEX_SCAN child, residual rejects ===
# a_val for a_id 7 is 21; pre-0a the residual was dropped and the join
# still counted a_grp 7's orders.
let $QUERY=
SELECT COUNT(*) AS c, SUM(o.o_shippriority) AS s
FROM acct AS a JOIN orders AS o ON o.o_custkey = a.a_grp
WHERE a.a_id = 7 AND a.a_val > 1000;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c, SUM(o.o_shippriority) AS s
FROM acct AS a JOIN orders AS o ON o.o_custkey = a.a_grp
WHERE a.a_id = 7 AND a.a_val > 1000|

--echo
--echo === rpr-5: passthrough, full-key CTE root, residual accepts ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cf.k, cf.n FROM cf WHERE cf.k = 7 AND cf.n > 1;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cf.k, cf.n FROM cf WHERE cf.k = 7 AND cf.n > 1|

--echo
--echo === rpr-6: passthrough, full-key CTE root, residual rejects ===
# Pre-0a the n > 100000 conjunct was silently dropped and the k = 7
# group came back even though its count cannot exceed 100000.
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cf.k, cf.n FROM cf WHERE cf.k = 7 AND cf.n > 100000;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cf.k, cf.n FROM cf WHERE cf.k = 7 AND cf.n > 100000|

--echo
--echo === rpr-7: passthrough chain, PK-covered real root, residual accepts ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey, cf.n
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey = 42 AND cu.c_nationkey >= 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey, cf.n
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey = 42 AND cu.c_nationkey >= 0|

--echo
--echo === rpr-8: passthrough chain, PK-covered real root, residual rejects ===
# Empty result: the filtered lookup root behaves as a miss, the deferred
# TSV header keeps the output empty to match the mysql baseline.
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey, cf.n
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey = 42 AND cu.c_nationkey < 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey, cf.n
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey = 42 AND cu.c_nationkey < 0|

--echo
--echo === rpr-9: contradictory PK equalities, aggregate ===
# collect_pk_equalities keeps the last equality as the key; the orphaned
# first one lands in the residual and rejects the row.  Pre-0a the query
# wrongly returned the o_orderkey = 78 row.
let $QUERY=
SELECT COUNT(*) AS c
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_orderkey = 77 AND o.o_orderkey = 78;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_orderkey = 77 AND o.o_orderkey = 78|

--echo
--echo === rpr-10: contradictory PK equalities, passthrough ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey, cf.n
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey = 42 AND cu.c_custkey = 43;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey, cf.n
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey = 42 AND cu.c_custkey = 43|

--echo
--echo === rpr-11: over-cap residual on a hash-PK root (table-scan fallback) ===
# 40 always-true conjuncts push the residual program past the 64-word
# lookup cap; orders' PK is USING HASH (no ordered PK index) so the emit
# cascades to a table scan with the full WHERE filter.  Correctness only
# — the plan is an emit-time decision and not visible in EXPLAIN.
let $QUERY=
SELECT COUNT(*) AS c, SUM(cu.c_nationkey) AS s
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_orderkey = 77
  AND o.o_custkey < 401 AND o.o_custkey < 402 AND o.o_custkey < 403
  AND o.o_custkey < 404 AND o.o_custkey < 405 AND o.o_custkey < 406
  AND o.o_custkey < 407 AND o.o_custkey < 408 AND o.o_custkey < 409
  AND o.o_custkey < 410 AND o.o_custkey < 411 AND o.o_custkey < 412
  AND o.o_custkey < 413 AND o.o_custkey < 414 AND o.o_custkey < 415
  AND o.o_custkey < 416 AND o.o_custkey < 417 AND o.o_custkey < 418
  AND o.o_custkey < 419 AND o.o_custkey < 420 AND o.o_custkey < 421
  AND o.o_custkey < 422 AND o.o_custkey < 423 AND o.o_custkey < 424
  AND o.o_custkey < 425 AND o.o_custkey < 426 AND o.o_custkey < 427
  AND o.o_custkey < 428 AND o.o_custkey < 429 AND o.o_custkey < 430
  AND o.o_custkey < 431 AND o.o_custkey < 432 AND o.o_custkey < 433
  AND o.o_custkey < 434 AND o.o_custkey < 435 AND o.o_custkey < 436
  AND o.o_custkey < 437 AND o.o_custkey < 438 AND o.o_custkey < 439
  AND o.o_custkey < 440;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c, SUM(cu.c_nationkey) AS s
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_orderkey = 77
  AND o.o_custkey < 401 AND o.o_custkey < 402 AND o.o_custkey < 403
  AND o.o_custkey < 404 AND o.o_custkey < 405 AND o.o_custkey < 406
  AND o.o_custkey < 407 AND o.o_custkey < 408 AND o.o_custkey < 409
  AND o.o_custkey < 410 AND o.o_custkey < 411 AND o.o_custkey < 412
  AND o.o_custkey < 413 AND o.o_custkey < 414 AND o.o_custkey < 415
  AND o.o_custkey < 416 AND o.o_custkey < 417 AND o.o_custkey < 418
  AND o.o_custkey < 419 AND o.o_custkey < 420 AND o.o_custkey < 421
  AND o.o_custkey < 422 AND o.o_custkey < 423 AND o.o_custkey < 424
  AND o.o_custkey < 425 AND o.o_custkey < 426 AND o.o_custkey < 427
  AND o.o_custkey < 428 AND o.o_custkey < 429 AND o.o_custkey < 430
  AND o.o_custkey < 431 AND o.o_custkey < 432 AND o.o_custkey < 433
  AND o.o_custkey < 434 AND o.o_custkey < 435 AND o.o_custkey < 436
  AND o.o_custkey < 437 AND o.o_custkey < 438 AND o.o_custkey < 439
  AND o.o_custkey < 440|

--echo
--echo === rpr-12: over-cap residual on an ordered-PK root (PK-index-scan fallback) ===
# Same over-cap shape, but acct's ordered PK lets the emit fall back to
# a PK-equality ordered-index scan carrying the big filter.
let $QUERY=
SELECT COUNT(*) AS c, SUM(cu.c_nationkey) AS s
FROM acct AS a JOIN customer AS cu ON cu.c_custkey = a.a_val
WHERE a.a_id = 7
  AND a.a_val < 9001 AND a.a_val < 9002 AND a.a_val < 9003
  AND a.a_val < 9004 AND a.a_val < 9005 AND a.a_val < 9006
  AND a.a_val < 9007 AND a.a_val < 9008 AND a.a_val < 9009
  AND a.a_val < 9010 AND a.a_val < 9011 AND a.a_val < 9012
  AND a.a_val < 9013 AND a.a_val < 9014 AND a.a_val < 9015
  AND a.a_val < 9016 AND a.a_val < 9017 AND a.a_val < 9018
  AND a.a_val < 9019 AND a.a_val < 9020 AND a.a_val < 9021
  AND a.a_val < 9022 AND a.a_val < 9023 AND a.a_val < 9024
  AND a.a_val < 9025 AND a.a_val < 9026 AND a.a_val < 9027
  AND a.a_val < 9028 AND a.a_val < 9029 AND a.a_val < 9030
  AND a.a_val < 9031 AND a.a_val < 9032 AND a.a_val < 9033
  AND a.a_val < 9034 AND a.a_val < 9035 AND a.a_val < 9036
  AND a.a_val < 9037 AND a.a_val < 9038 AND a.a_val < 9039
  AND a.a_val < 9040;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT COUNT(*) AS c, SUM(cu.c_nationkey) AS s
FROM acct AS a JOIN customer AS cu ON cu.c_custkey = a.a_val
WHERE a.a_id = 7
  AND a.a_val < 9001 AND a.a_val < 9002 AND a.a_val < 9003
  AND a.a_val < 9004 AND a.a_val < 9005 AND a.a_val < 9006
  AND a.a_val < 9007 AND a.a_val < 9008 AND a.a_val < 9009
  AND a.a_val < 9010 AND a.a_val < 9011 AND a.a_val < 9012
  AND a.a_val < 9013 AND a.a_val < 9014 AND a.a_val < 9015
  AND a.a_val < 9016 AND a.a_val < 9017 AND a.a_val < 9018
  AND a.a_val < 9019 AND a.a_val < 9020 AND a.a_val < 9021
  AND a.a_val < 9022 AND a.a_val < 9023 AND a.a_val < 9024
  AND a.a_val < 9025 AND a.a_val < 9026 AND a.a_val < 9027
  AND a.a_val < 9028 AND a.a_val < 9029 AND a.a_val < 9030
  AND a.a_val < 9031 AND a.a_val < 9032 AND a.a_val < 9033
  AND a.a_val < 9034 AND a.a_val < 9035 AND a.a_val < 9036
  AND a.a_val < 9037 AND a.a_val < 9038 AND a.a_val < 9039
  AND a.a_val < 9040|

--echo
--echo === rpr-13: agg + CTE, readTuple root, residual accepts ===
# Aggregate main query WITH a CTE: the CTE materialisation scan makes
# the compound query scan-rooted, so the readTuple main root is legal
# (fpw-6 shape) and carries the residual as an interpreted filter.
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey AS ck, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey = 42 AND cu.c_nationkey >= 0
GROUP BY cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey AS ck, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey = 42 AND cu.c_nationkey >= 0
GROUP BY cu.c_custkey|

--echo
--echo === rpr-14: agg + CTE, readTuple root, residual rejects ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey AS ck, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey = 42 AND cu.c_nationkey < 0
GROUP BY cu.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey AS ck, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey = 42 AND cu.c_nationkey < 0
GROUP BY cu.c_custkey|

DELIMITER ;|

--echo
--echo === rpr-P1 (rejection): full-key CTE root + col-vs-col residual ===
# The CTE_LOOKUP jump-table filter rejects col-vs-col atoms with a clean
# permanent error.  Pre-0a this query SUCCEEDED with wrong results (the
# cf.n > cf.k conjunct was silently dropped).
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT cf.k, cf.n FROM cf WHERE cf.k = 7 AND cf.n > cf.k;' 2>&1

--disable_query_log
--disable_warnings
DROP TABLE acct;
--enable_warnings
--enable_query_log
--source suite/ronsql_cte/include/cte_drop.inc
