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
# CORRECTED ENVELOPE: projection-only main SELECTs over a CTE_LOOKUP HANG
# (discovery finding D3) — the headline finding of this family.  ALL
# projection-only main SELECTs (mode (a) and the projection-only multi-batch
# cases) are therefore DISABLED as `# NEXT-PHASE` probes; only the AGGREGATING
# main SELECTs (mode (b) / P-GB and the aggregating multi-batch case) stay
# ENABLED.  See findings/mainmode.md.
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
# HEADLINE FINDING (D3): projection-only main SELECTs over a CTE_LOOKUP HANG.
#   Any `SELECT cte.col, real.col FROM real JOIN cte ...` with NO main
#   aggregate never returns on this build.  The fix is to wrap the CTE outputs
#   in a main aggregate (mode (b) / P-GB above).  Every projection-only case
#   below — both bounded (mode (a)) and the unbounded multi-batch variants — is
#   the SAME hang class, disabled as a probe.  Discovery finding D3 / D2.
# ----------------------------------------------------------------------------

# NEXT-PHASE: projection-only INNER join over complete-key CTE lookup (HANG, D3)
#   WITH cust AS (
#     SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
#     FROM orders GROUP BY o_custkey)
#   SELECT c.c_custkey, c.c_mktsegment, cust.t, cust.n
#   FROM customer AS c JOIN cust ON cust.k = c.c_custkey
#   WHERE c.c_custkey <= 20;

# NEXT-PHASE: projection-only LEFT join over complete-key CTE lookup (HANG, D3)
#   WITH cust AS (
#     SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
#     FROM orders GROUP BY o_custkey)
#   SELECT c.c_custkey, c.c_mktsegment, cust.t, cust.n
#   FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
#   WHERE c.c_custkey <= 20;

# NEXT-PHASE: projection-only LEFT join with unmatched parents (HANG, D3)
#   WITH cust AS (
#     SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
#     FROM orders WHERE o_orderkey <= 150 GROUP BY o_custkey)
#   SELECT c.c_custkey, c.c_mktsegment, cust.t, cust.n
#   FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
#   WHERE c.c_custkey <= 200;

# NEXT-PHASE: projection-only INNER join, single CTE agg column (HANG, D3)
#   WITH cust AS (
#     SELECT o_custkey AS k, MAX(o_totalprice) AS mx
#     FROM orders GROUP BY o_custkey)
#   SELECT c.c_custkey, cust.mx
#   FROM customer AS c JOIN cust ON cust.k = c.c_custkey
#   WHERE c.c_custkey <= 15;

# NEXT-PHASE: projection-only INNER join + WHERE on a CTE aggregate output (HANG, D3)
#   WITH cust AS (
#     SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
#     FROM orders GROUP BY o_custkey)
#   SELECT c.c_custkey, cust.t, cust.n
#   FROM customer AS c JOIN cust ON cust.k = c.c_custkey
#   WHERE c.c_custkey <= 60 AND cust.n >= 5;

# NEXT-PHASE: projection-only cte JOIN real_table (CTE_SCAN root + child) (HANG, D3)
#   WITH cust AS (
#     SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
#     FROM orders GROUP BY o_custkey)
#   SELECT cust.k, cust.t, c.c_mktsegment
#   FROM cust JOIN customer AS c ON c.c_custkey = cust.k
#   WHERE cust.k <= 25;

# NEXT-PHASE: multi-batch projection-only over full 300-group CTE (HANG, D3)
#   WITH cust AS (
#     SELECT o_custkey AS k, SUM(o_shippriority) AS t
#     FROM orders GROUP BY o_custkey)
#   SELECT cust.k, cust.t
#   FROM customer AS c JOIN cust ON cust.k = c.c_custkey;

# NEXT-PHASE: multi-batch projection-only CTE_SCAN root (HANG, D3)
#   WITH cust AS (
#     SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
#     FROM orders GROUP BY o_custkey)
#   SELECT cust.k, cust.t, cust.n
#   FROM cust JOIN customer AS c ON c.c_custkey = cust.k;

# NEXT-PHASE: multi-batch LEFT join projection-only, all 300 custkeys (HANG, D3)
#   WITH cust AS (
#     SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
#     FROM orders GROUP BY o_custkey)
#   SELECT c.c_custkey, cust.t, cust.n
#   FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey;

# ----------------------------------------------------------------------------
# Clause-level deferrals per the capability matrix (ORDER BY / LIMIT / DISTINCT
# / HAVING on CTE queries are not yet supported).
# ----------------------------------------------------------------------------

# NEXT-PHASE: ORDER BY on a CTE query — deferred to broader LIMIT/ORDER BY
#   pushdown (matrix: "ORDER BY / LIMIT / DISTINCT / HAVING on CTE queries").
#   WITH cust AS (
#     SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
#     FROM orders GROUP BY o_custkey)
#   SELECT c.c_mktsegment, SUM(cust.t)
#   FROM customer AS c JOIN cust ON cust.k = c.c_custkey
#   GROUP BY c.c_mktsegment
#   ORDER BY SUM(cust.t) DESC;

# NEXT-PHASE: LIMIT on a CTE query — deferred to broader LIMIT/ORDER BY
#   pushdown (matrix).
#   WITH cust AS (
#     SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
#     FROM orders GROUP BY o_custkey)
#   SELECT c.c_mktsegment, SUM(cust.t)
#   FROM customer AS c JOIN cust ON cust.k = c.c_custkey
#   GROUP BY c.c_mktsegment
#   LIMIT 3;

# NEXT-PHASE: DISTINCT in a CTE / main SELECT — deferred (matrix).
#   WITH cust AS (
#     SELECT DISTINCT o_custkey AS k, SUM(o_shippriority) AS t
#     FROM orders GROUP BY o_custkey)
#   SELECT DISTINCT c.c_mktsegment
#   FROM customer AS c JOIN cust ON cust.k = c.c_custkey;

# NEXT-PHASE: HAVING on a CTE query — deferred (matrix).
#   WITH cust AS (
#     SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
#     FROM orders GROUP BY o_custkey)
#   SELECT c.c_mktsegment, SUM(cust.t)
#   FROM customer AS c JOIN cust ON cust.k = c.c_custkey
#   GROUP BY c.c_mktsegment
#   HAVING SUM(cust.t) > 1000000;

DELIMITER ;|

--source suite/ronsql_cte/include/cte_drop.inc
