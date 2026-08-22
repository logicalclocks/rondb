# body_passthrough_limit.inc — LIMIT on projection-only (pass-through)
# shapes (ronsql_orderby_limit_plan.md Phase 2), data-driven CTE suite.
#
# Phase 2 relaxes the non-aggregate parse gate to accept LIMIT (without
# ORDER BY) on every projection-only shape: E.3 CTE_SCAN roots, the
# I.8/I.11/I.12 + non-aggregate-plan join chains, and the Phase-1
# single-table path.  The drains stream rows, stop at the limit, and
# close the still-open scan early (I.14 kernel/API early close).
# LIMIT 0 keeps the deferred-TSV-header convention: no output at all,
# matching the mysql client's empty-result baseline.  ORDER BY is
# covered separately by body_passthrough_orderby.inc (plan Phase 3).
#
# DETERMINISM: without ORDER BY, WHICH rows survive a truncating LIMIT
# is nondeterministic — every truncating case below therefore projects
# columns that are value-uniform across the candidate rows (e.g. cf.n
# is 5 for every group), so the output is deterministic no matter which
# rows arrive first.  Non-truncating cases (LIMIT 0 / LIMIT > set) may
# project freely.
#
# Case map (pl-N):
#   1   CTE_SCAN root, truncating LIMIT 3 (uniform projection)
#   2   CTE_SCAN root, LIMIT 260 of 300 — stops mid-scan past the
#       256-row API batch boundary
#   3   CTE_SCAN root, LIMIT 0 -> empty, no header
#   4   CTE_SCAN root, LIMIT > result set (full 300 rows)
#   5   join chain (real root + CTE_LOOKUP child), truncating LIMIT 4
#   6   join chain, LIMIT 0 -> empty
#   7   join chain, LIMIT > result set
#   8   real-table snowflake chain (lookup child) + root residual
#       WHERE, truncating LIMIT 3 (uniform projection)
#   9   single-table index-scan + WHERE, truncating LIMIT 3 (uniform)
#   10  single-table full scan, LIMIT > result set (region, 5 rows)
#   11  single-table scan, LIMIT 0 -> empty
#   12  single-table PK lookup + LIMIT 1 (row survives)
#   13  single-table PK lookup + LIMIT 0 -> empty
#   14  single-table PK lookup MISS + LIMIT 1 -> empty
#   (the Phase-2-era P1/P2 ORDER-BY-rejection probes were retired when
#   Phase 3 shipped ORDER BY — see body_passthrough_orderby.inc)
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

# ============================================================
# E.3 CTE_SCAN root.
# ============================================================

--echo
--echo === pl-1: CTE_SCAN root, truncating LIMIT (uniform projection) ===
# cf.n is 5 for every one of the 300 groups (5 orders per custkey), so
# any 3 delivered rows print identically.
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT n FROM cf LIMIT 3;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_pl_1.out
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT n FROM cf LIMIT 3;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'LIMIT 3' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT n FROM cf LIMIT 3|

--echo
--echo === pl-2: CTE_SCAN root, LIMIT 260 stops mid-scan past the 256-row batch boundary ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT n FROM cf LIMIT 260;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT n FROM cf LIMIT 260|

--echo
--echo === pl-3: CTE_SCAN root, LIMIT 0 (empty result, no header) ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT k, n FROM cf LIMIT 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT k, n FROM cf LIMIT 0|

--echo
--echo === pl-4: CTE_SCAN root, LIMIT larger than the result set ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT k, n FROM cf LIMIT 1000;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT k, n FROM cf LIMIT 1000|

# ============================================================
# I.8 join chain: real-table root + complete-key CTE_LOOKUP child.
# ============================================================

--echo
--echo === pl-5: join chain, truncating LIMIT (uniform projection) ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cf.n FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey LIMIT 4;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cf.n FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey LIMIT 4|

--echo
--echo === pl-6: join chain, LIMIT 0 (empty result, no header) ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey, cf.n
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey LIMIT 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey, cf.n
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey LIMIT 0|

--echo
--echo === pl-7: join chain, LIMIT larger than the result set ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey, cf.n
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey LIMIT 2000;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey, cf.n
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey LIMIT 2000|

# ============================================================
# Real-table snowflake chain (non-aggregate Phase 2 shape).
# ============================================================

--echo
--echo === pl-8: real-table chain + root residual WHERE, truncating LIMIT ===
# orders root scan with the o_custkey=57 residual filter (5 rows), PK
# lookup child on customer; c_nationkey is 7 for every one of them
# (57 mod 25), so the truncated output is uniform.
let $QUERY=
SELECT cu.c_nationkey
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_custkey = 57 LIMIT 3;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT cu.c_nationkey
FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey
WHERE o.o_custkey = 57 LIMIT 3|

# ============================================================
# Single-table pass-through (plain NDB API path).
# ============================================================

--echo
--echo === pl-9: single-table index scan + WHERE, truncating LIMIT (uniform) ===
let $QUERY=
SELECT c_nationkey FROM customer WHERE c_nationkey = 7 LIMIT 3;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT c_nationkey FROM customer WHERE c_nationkey = 7 LIMIT 3|

--echo
--echo === pl-10: single-table full scan, LIMIT larger than the result set ===
let $QUERY=
SELECT r_regionkey, r_name FROM region LIMIT 100;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT r_regionkey, r_name FROM region LIMIT 100|

--echo
--echo === pl-11: single-table scan, LIMIT 0 (empty result, no header) ===
let $QUERY=
SELECT c_custkey FROM customer LIMIT 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT c_custkey FROM customer LIMIT 0|

--echo
--echo === pl-12: single-table PK lookup + LIMIT 1 ===
let $QUERY=
SELECT o_orderkey, o_orderstatus FROM orders WHERE o_orderkey = 77 LIMIT 1;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_orderstatus FROM orders WHERE o_orderkey = 77 LIMIT 1|

--echo
--echo === pl-13: single-table PK lookup + LIMIT 0 (empty, no header) ===
let $QUERY=
SELECT o_orderkey, o_orderstatus FROM orders WHERE o_orderkey = 77 LIMIT 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_orderstatus FROM orders WHERE o_orderkey = 77 LIMIT 0|

--echo
--echo === pl-14: single-table PK lookup MISS + LIMIT 1 (empty) ===
let $QUERY=
SELECT o_orderkey, o_orderstatus FROM orders WHERE o_orderkey = 999999 LIMIT 1;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_orderstatus FROM orders WHERE o_orderkey = 999999 LIMIT 1|

DELIMITER ;|

# The Phase-2-era pl-P1/P2 probes pinned "ORDER BY rejected on
# pass-through shapes"; ORDER BY is supported since Phase 3 (buffered
# client-side sort) and its positive coverage lives in
# body_passthrough_orderby.inc.

--source suite/ronsql_cte/include/cte_drop.inc
