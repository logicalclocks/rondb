# body_mainmode.inc — mainmode family, data-driven CTE suite.
#
# FOCUS: one aggregating CTE (`cust`) feeding the two main-SELECT modes the
# user cares about:
#   (a) normal-join PROJECTION-ONLY main SELECTs (no main aggregation), and
#   (b) AGGREGATING main SELECTs (P-GB — GROUP BY a parent-table column over a
#       CTE aggregate leaf),
# plus multi-batch (>256-group) redistribution stress (results ~300 rows that
# cross the API batch boundary and exercise multi-node redistribution).
#
# D3 was the headline finding of this family: projection-only main SELECTs over
# a CTE_LOOKUP used to HANG.  D3 is now FIXED (2026-06-08, RonSQLPreparer root-op
# dummy getValue), so BOTH modes are exercised and ENABLED here: mode (a)
# projection-only main SELECTs (MM1-MM6, MM13/MM14/MM16) and mode (b) aggregating
# / P-GB main SELECTs (MM7-MM12, MM15).  ORDER BY / LIMIT / HAVING on the
# AGGREGATING mode are covered by body_orderby_limit.inc; LIMIT and
# ORDER BY on the projection-only mode by body_passthrough_limit.inc /
# body_passthrough_orderby.inc (Phases 2+3).  Only DISTINCT remains
# deferred as a probe (see the tail of this file).  See
# findings/mainmode.md.
#
# CTE always aggregates.  The aggregating main SELECTs wrap the CTE outputs in
# SUM/COUNT/MIN/MAX and GROUP BY a parent column (or scalar).  The harness sorts
# both TSV outputs, so row order never matters.
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

# ============================================================================
# MODE (b): AGGREGATING main SELECTs over a CTE aggregate leaf.
#   The CTE `cust` aggregates orders by o_custkey (300 groups).  The main
#   SELECT joins customer to that CTE on the full virtual PK (k) and wraps the
#   CTE outputs in main aggregates (SUM/COUNT/MIN/MAX), with GROUP BY a parent
#   column or scalar.  This is P-GB — the only main-SELECT mode that WORKS over
#   a CTE_LOOKUP on this build.  (Projection-only main SELECTs over the same
#   CTE_LOOKUP HANG — finding D3 — and are disabled as probes below.)
# ============================================================================

--echo
--echo === MM7: P-GB — main SUM over CTE leaf, GROUP BY parent mktsegment (INNER) ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_mktsegment, SUM(cust.t), SUM(cust.n)
FROM customer AS c
JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_mktsegment;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_mktsegment, SUM(cust.t), SUM(cust.n)
FROM customer AS c
JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_mktsegment|

--echo
--echo === MM8: P-GB with LEFT JOIN — unmatched parents NULL-injected (filtered CTE) ===
# CTE body filtered to o_orderkey <= 150: custkeys 151..300 have no orders, so
# those customers are unmatched and contribute NULL CTE aggregates (SUM ignores
# NULL) to their c_mktsegment group.
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders WHERE o_orderkey <= 150 GROUP BY o_custkey)
SELECT c.c_mktsegment, SUM(cust.t), SUM(cust.n)
FROM customer AS c
LEFT JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_mktsegment;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders WHERE o_orderkey <= 150 GROUP BY o_custkey)
SELECT c.c_mktsegment, SUM(cust.t), SUM(cust.n)
FROM customer AS c
LEFT JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_mktsegment|

--echo
--echo === MM9: P-GB — GROUP BY parent c_nationkey over CTE leaf (INNER) ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_nationkey, SUM(cust.t), COUNT(*)
FROM customer AS c
JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_nationkey, SUM(cust.t), COUNT(*)
FROM customer AS c
JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_nationkey|

--echo
--echo === MM10: P-GB — main MIN/MAX over CTE agg columns, GROUP BY mktsegment ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_mktsegment, MIN(cust.t), MAX(cust.t), MAX(cust.n)
FROM customer AS c
JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_mktsegment;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_mktsegment, MIN(cust.t), MAX(cust.t), MAX(cust.n)
FROM customer AS c
JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_mktsegment|

--echo
--echo === MM11: aggregating main, scalar (no GROUP BY) over CTE leaf ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT SUM(cust.t), SUM(cust.n), COUNT(*)
FROM customer AS c
JOIN cust ON cust.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT SUM(cust.t), SUM(cust.n), COUNT(*)
FROM customer AS c
JOIN cust ON cust.k = c.c_custkey|

--echo
--echo === MM12: aggregating cte JOIN real_table (CTE parent), GROUP BY parent col ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_mktsegment, SUM(cust.t)
FROM cust JOIN customer AS c ON c.c_custkey = cust.k
GROUP BY c.c_mktsegment;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_mktsegment, SUM(cust.t)
FROM cust JOIN customer AS c ON c.c_custkey = cust.k
GROUP BY c.c_mktsegment|

# ============================================================================
# MULTI-BATCH (>256-group) redistribution stress.  This INTENTIONALLY produces
# ~300 result rows that cross the 256-row API batch boundary and exercise
# multi-node redistribution.  Do NOT bound the row count here — that is the
# point of this case.  Only the AGGREGATING multi-batch case stays enabled; the
# projection-only multi-batch cases HANG (D3) and are disabled below.
# ============================================================================

--echo
--echo === MM15: multi-batch — aggregating main GROUP BY parent custkey (~300 rows) ===
# GROUP BY the parent PK column (c_custkey) over the CTE aggregate leaf yields
# one output row per custkey -> 300 groups, crossing the batch boundary.  This
# is the family's redistribution-stress case.
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, SUM(cust.t), SUM(cust.n)
FROM customer AS c
JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, SUM(cust.t), SUM(cust.n)
FROM customer AS c
JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_custkey|

# ============================================================================
# PROBES — deferred / hanging shapes.  Recorded as commented, NEXT-PHASE-
# disabled queries; NOT executed.  See findings/mainmode.md.
# ============================================================================

# ----------------------------------------------------------------------------
# D3 FIXED (2026-06-08): projection-only main SELECTs over a CTE_LOOKUP no
# longer hang (RonSQLPreparer root-op dummy getValue).  The cases below were
# the disabled D3 probes, now re-enabled as live MAIN cases.  MM4 reads an
# INT CTE agg (MAX(o_shippriority)) rather than the original DECIMAL
# MAX(o_totalprice) to avoid the separate D15 DECIMAL-scale format gap.
# ----------------------------------------------------------------------------

--echo
--echo === MM1: proj-only INNER over complete-key CTE_LOOKUP (was D3 hang) ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, c.c_mktsegment, cust.t, cust.n
FROM customer AS c JOIN cust ON cust.k = c.c_custkey
WHERE c.c_custkey <= 20;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, c.c_mktsegment, cust.t, cust.n
FROM customer AS c JOIN cust ON cust.k = c.c_custkey
WHERE c.c_custkey <= 20|

--echo
--echo === MM2: proj-only LEFT over complete-key CTE_LOOKUP (was D3 hang) ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, c.c_mktsegment, cust.t, cust.n
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
WHERE c.c_custkey <= 20;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, c.c_mktsegment, cust.t, cust.n
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
WHERE c.c_custkey <= 20|

--echo
--echo === MM3: proj-only LEFT with unmatched parents (NULL-injected) (was D3 hang) ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders WHERE o_orderkey <= 150 GROUP BY o_custkey)
SELECT c.c_custkey, c.c_mktsegment, cust.t, cust.n
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
WHERE c.c_custkey <= 200;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders WHERE o_orderkey <= 150 GROUP BY o_custkey)
SELECT c.c_custkey, c.c_mktsegment, cust.t, cust.n
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
WHERE c.c_custkey <= 200|

--echo
--echo === MM4: proj-only INNER, single CTE agg column (int) (was D3 hang) ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, MAX(o_shippriority) AS mx
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, cust.mx
FROM customer AS c JOIN cust ON cust.k = c.c_custkey
WHERE c.c_custkey <= 15;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, MAX(o_shippriority) AS mx
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, cust.mx
FROM customer AS c JOIN cust ON cust.k = c.c_custkey
WHERE c.c_custkey <= 15|

--echo
--echo === MM5: proj-only INNER + main WHERE on a CTE aggregate output (was D3 hang) ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, cust.t, cust.n
FROM customer AS c JOIN cust ON cust.k = c.c_custkey
WHERE c.c_custkey <= 60 AND cust.n >= 5;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, cust.t, cust.n
FROM customer AS c JOIN cust ON cust.k = c.c_custkey
WHERE c.c_custkey <= 60 AND cust.n >= 5|

--echo
--echo === MM6: proj-only cte JOIN real_table (CTE_SCAN root + child) (was D3 hang) ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT cust.k, cust.t, c.c_mktsegment
FROM cust JOIN customer AS c ON c.c_custkey = cust.k
WHERE cust.k <= 25;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT cust.k, cust.t, c.c_mktsegment
FROM cust JOIN customer AS c ON c.c_custkey = cust.k
WHERE cust.k <= 25|

--echo
--echo === MM13: multi-batch proj-only over full 300-group CTE (was D3 hang) ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT cust.k, cust.t
FROM customer AS c JOIN cust ON cust.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT cust.k, cust.t
FROM customer AS c JOIN cust ON cust.k = c.c_custkey|

--echo
--echo === MM14: multi-batch proj-only CTE_SCAN root (was D3 hang) ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT cust.k, cust.t, cust.n
FROM cust JOIN customer AS c ON c.c_custkey = cust.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT cust.k, cust.t, cust.n
FROM cust JOIN customer AS c ON c.c_custkey = cust.k|

--echo
--echo === MM16: multi-batch LEFT join proj-only, all 300 custkeys (was D3 hang) ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, cust.t, cust.n
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, cust.t, cust.n
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey|

# MM17 (was D23, FIXED): a projection-only main SELECT that INNER-joins a
# CTE_LOOKUP child with NO user-selected columns from the CTE.  Segfaulted
# (NDB Error 6000 / Signal 11) in the kernel CTE_LOOKUP emit/projection path
# before the linked-column metadata fix; now green.
--echo
--echo === MM17: projection-only main SELECT over a CTE_LOOKUP child with no CTE cols selected (was D23 crash) ===
let $QUERY=
WITH cust AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t FROM orders GROUP BY o_custkey)
SELECT c.c_custkey FROM customer AS c JOIN cust ON cust.k = c.c_custkey WHERE c.c_custkey <= 20;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t FROM orders GROUP BY o_custkey)
SELECT c.c_custkey FROM customer AS c JOIN cust ON cust.k = c.c_custkey WHERE c.c_custkey <= 20|

# ----------------------------------------------------------------------------
# Clause-level shapes.  ORDER BY / LIMIT / HAVING on AGGREGATING main SELECTs
# over CTE joins are covered by body_orderby_limit.inc (ronsql_orderby_limit_
# plan.md Phase 1) — the earlier "deferred" markers here dated from before
# that family and used the ORDER BY SUM(...) expression form, which is a
# grammar-level syntax error (only identifiers/aliases are accepted in
# ORDER BY; pinned by ob-P2 there).  LIMIT and ORDER BY on the
# PROJECTION-ONLY modes are covered by body_passthrough_limit.inc /
# body_passthrough_orderby.inc (plan Phases 2+3).  Only DISTINCT remains
# deferred.
# ----------------------------------------------------------------------------

# NEXT-PHASE: DISTINCT in a CTE / main SELECT — deferred (matrix).
#   WITH cust AS (
#     SELECT DISTINCT o_custkey AS k, SUM(o_shippriority) AS t
#     FROM orders GROUP BY o_custkey)
#   SELECT DISTINCT c.c_mktsegment
#   FROM customer AS c JOIN cust ON cust.k = c.c_custkey;

DELIMITER ;|

--source suite/ronsql_cte/include/cte_drop.inc
