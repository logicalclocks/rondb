# body_orderby_limit.inc — ORDER BY + LIMIT on aggregate join/CTE queries
# (ronsql_orderby_limit_plan.md Phase 1), data-driven CTE suite.
#
# FOCUS: main-SELECT-level ORDER BY / LIMIT on join-agg and CTE-join-agg
# queries.  The sort+limit machinery (charset-aware compare_rows, buffered
# print_result_ordered, streaming LIMIT cutoff) is shared with the tested
# single-table path (ronsql_orderby.test / ronsql_basic.test), but before
# this family there was ZERO join/CTE coverage beyond bigquery big-12 —
# which sorts both outputs before diffing and therefore only checks the
# surviving VALUES, not the row order.
#
# ORDER VERIFICATION: unlike the other families, every ORDER BY case here
# sets $skip_sort=yes so ronsql_compare.inc diffs the two outputs in
# DELIVERED row order.  For that diff to be stable, every ORDER BY below
# is a total order: either the leading key is provably unique per group,
# or a unique tie-breaker column ends the ORDER BY list.  NULLs sort
# first in ASC (MySQL semantics) on both sides.
#
# ENVELOPE: the main SELECT must AGGREGATE (scalar aggregate or GROUP BY
# + aggregates) — projection-only shapes still reject ORDER BY / LIMIT at
# the parse gate (plan Phases 2-3).  Subquery ORDER BY / LIMIT is
# permanently rejected (Phase 0, ronsql_cte_dd_orderby_limit_reject);
# CTE-BODY ORDER BY / LIMIT is kernel-applied since
# cte_orderby_limit_plan.md L4 (family ronsql_cte_dd_orderby_limit_cte).
# ORDER BY targets must be GROUP BY columns or aggregate output aliases;
# LIMIT is a single non-negative integer (no OFFSET).
#
# Fallout candidates this family deliberately exercises (see plan):
#   * charset metadata for CTE virt-table GROUP BY columns (ob-21 orders
#     by a CHAR(1) virt column of a CTE_SCAN-root re-aggregation)
#   * collation-order vs binary-order on string aggregate results
#     (ob-11 / ob-12 order by VARCHAR MIN/MAX outputs)
#   * D17 temporal packing must sort in date order incl. DESC display
#     (ob-13 / ob-14 DATE, ob-15 DATETIME + TIMESTAMP(6))
#
# FOUND + FIXED ON FIRST RECORD:
#   1. Bare `ORDER BY c_nationkey` against qualified `GROUP BY
#      cu.c_nationkey` was wrongly rejected ("ORDER BY column not in
#      GROUP BY clause") — the parser registers (qualifier, name) pairs
#      as distinct col_idx entries and the printer matched GROUP BY
#      membership by raw col_idx equality.  Fixed by
#      RonSQLPreparer::canonicalize_orderby_columns (resolved-column
#      identity rewrite before printer construction); ob-1 pins
#      bare-ORDER-BY-vs-qualified-GROUP-BY, ob-2 the reverse direction.
#   2. LIMIT 0 printed a bare TSV header line where the mysql client
#      prints nothing — a long-standing deviation baked into the
#      non-strict ronsql_basic / ronsql_orderby_stress baselines as
#      recorded diffs.  Fixed in ResultPrinter (buffered path gates the
#      header on print_count > 0; streaming path checks the LIMIT
#      cutoff before the deferred header).  ob-5 pins the buffered arm,
#      ob-20 the streaming arm; those two base-suite tests need
#      re-recording.
#
# The EXPLAIN greps are kept NON-FATAL with `|| true` for now; the
# orchestrator will tighten the exact EXPLAIN strings after recording.
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
# A: ORDER BY a GROUP BY column (join-agg over real table + CTE).
# ============================================================

--echo
--echo === ob-1: ORDER BY bare GROUP BY column ASC ===
# c_nationkey is the group key, hence unique per row: total order.
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS t
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn, SUM(cf.t) AS st
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey
ORDER BY c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS t
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn, SUM(cf.t) AS st
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey
ORDER BY c_nationkey|

--echo
--echo === ob-2: qualified ORDER BY DESC over a bare GROUP BY spelling ===
# Reverse spelling mix of ob-1 (bare SELECT/GROUP BY + qualified ORDER
# BY) — both directions are unified by canonicalize_orderby_columns;
# the qualified/qualified pairing is covered by ob-22.
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY c_nationkey
ORDER BY cu.c_nationkey DESC;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY c_nationkey
ORDER BY cu.c_nationkey DESC|

--echo
--echo === ob-3: multi-column ORDER BY, mixed directions (string first key) ===
# (c_mktsegment, c_nationkey) is the GROUP BY key, hence unique pairs;
# 5 segments x 5 nationkeys each.  All-uppercase ASCII values, so the
# collation order is charset-independent.
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_mktsegment AS seg, cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_mktsegment, cu.c_nationkey
ORDER BY c_mktsegment ASC, c_nationkey DESC;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_mktsegment AS seg, cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_mktsegment, cu.c_nationkey
ORDER BY c_mktsegment ASC, c_nationkey DESC|

# ============================================================
# B: LIMIT 0 / N / > result set on the ordered (buffered) path,
# plus the streaming (no-ORDER-BY) LIMIT cutoff.
# ============================================================

--echo
--echo === ob-4: ORDER BY + LIMIT N (10 of 25 groups) ===
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey
ORDER BY c_nationkey
LIMIT 10;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey
ORDER BY c_nationkey
LIMIT 10|

--echo
--echo === ob-5: ORDER BY + LIMIT 0 (empty result, no header) ===
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey
ORDER BY c_nationkey
LIMIT 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey
ORDER BY c_nationkey
LIMIT 0|

--echo
--echo === ob-6: ORDER BY DESC + LIMIT larger than result set ===
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey
ORDER BY c_nationkey DESC
LIMIT 1000;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey
ORDER BY c_nationkey DESC
LIMIT 1000|

--echo
--echo === ob-7: streaming LIMIT without ORDER BY (uniform rows) ===
# No ORDER BY -> the streaming print-cutoff path.  Which 3 of the 25
# groups survive is nondeterministic, so the SELECT list contains no
# group column: every group prints the identical row (12 customers per
# nationkey), making the truncated output value-deterministic.  Default
# sorted compare (no $skip_sort).
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT COUNT(*) AS c
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey
LIMIT 3;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT COUNT(*) AS c
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey
LIMIT 3|

# ============================================================
# C: ORDER BY an aggregate output alias (SUM / COUNT / MIN / MAX,
# including string and temporal aggregate results).
# ============================================================

--echo
--echo === ob-8: ORDER BY COUNT alias DESC with group-key tie-breaker ===
# WHERE c_custkey <= 137 skews the per-nationkey counts (6 for nk 1..12,
# 5 for nk 0 and 13..24); ties broken by nk ASC.
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey <= 137
GROUP BY cu.c_nationkey
ORDER BY c DESC, nk ASC;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey <= 137
GROUP BY cu.c_nationkey
ORDER BY c DESC, nk ASC|

--echo
--echo === ob-9: ORDER BY SUM alias ASC + LIMIT crossing a tie run ===
# sn is 25 for thirteen groups (nk 0 and 13..24) and 30 for twelve
# (nk 1..12); LIMIT 7 cuts inside the 25-tie run, so the nk ASC
# tie-breaker decides which groups survive: nk 0, 13..18.
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, SUM(cf.n) AS sn, COUNT(*) AS c
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey <= 137
GROUP BY cu.c_nationkey
ORDER BY sn ASC, nk ASC
LIMIT 7;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, SUM(cf.n) AS sn, COUNT(*) AS c
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey <= 137
GROUP BY cu.c_nationkey
ORDER BY sn ASC, nk ASC
LIMIT 7|

--echo
--echo === ob-10: ORDER BY MIN alias DESC (provably unique, no tie-breaker) ===
# MIN(c_custkey) per nationkey is distinct (nk for nk 1..24, 25 for nk 0),
# so a single-key ORDER BY is already a total order.
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, MIN(cu.c_custkey) AS mn, MAX(cu.c_custkey) AS mx, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey
ORDER BY mn DESC;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, MIN(cu.c_custkey) AS mn, MAX(cu.c_custkey) AS mx, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey
ORDER BY mn DESC|

--echo
--echo === ob-11: ORDER BY string MIN alias ASC (collation order, I.6) ===
# c_mktsegment is constant per nationkey (25 divides by 5), so MIN=MAX
# with 5 distinct VARCHAR values across the 25 groups; nk ASC breaks the
# 5-way ties.  Exercises the string-result re-wrap in the sort comparator.
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, MIN(cu.c_mktsegment) AS mseg, MAX(cu.c_mktsegment) AS xseg, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey
ORDER BY mseg ASC, nk ASC;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, MIN(cu.c_mktsegment) AS mseg, MAX(cu.c_mktsegment) AS xseg, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
GROUP BY cu.c_nationkey
ORDER BY mseg ASC, nk ASC|

--echo
--echo === ob-12: ORDER BY string MIN alias DESC, varying strings ===
# p_name is distinct per part row, so MIN(p_name) differs across the 5
# p_brand groups: genuinely varying VARCHAR aggregate results, DESC.
--let $skip_sort=yes
let $QUERY=
WITH pc AS (SELECT l_partkey AS k, COUNT(*) AS n
            FROM lineitem GROUP BY l_partkey)
SELECT p.p_brand AS b, MIN(p.p_name) AS mn, MAX(p.p_name) AS mx, SUM(pc.n) AS sn
FROM part AS p JOIN pc ON pc.k = p.p_partkey
GROUP BY p.p_brand
ORDER BY mn DESC;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH pc AS (SELECT l_partkey AS k, COUNT(*) AS n
            FROM lineitem GROUP BY l_partkey)
SELECT p.p_brand AS b, MIN(p.p_name) AS mn, MAX(p.p_name) AS mx, SUM(pc.n) AS sn
FROM part AS p JOIN pc ON pc.k = p.p_partkey
GROUP BY p.p_brand
ORDER BY mn DESC|

--echo
--echo === ob-13: ORDER BY DATE MIN alias ASC (D17 packed value = date order) ===
# MIN(o_orderdate) ties across custkeys 100 apart; ck ASC breaks them.
# The 1500-group lineitem CTE crosses the 256-row API batch boundary.
--let $skip_sort=yes
let $QUERY=
WITH li AS (SELECT l_orderkey AS k, COUNT(*) AS n
            FROM lineitem GROUP BY l_orderkey)
SELECT o.o_custkey AS ck, MIN(o.o_orderdate) AS md, MAX(o.o_orderdate) AS xd, SUM(li.n) AS sn
FROM orders AS o JOIN li ON li.k = o.o_orderkey
WHERE o.o_custkey <= 40
GROUP BY o.o_custkey
ORDER BY md ASC, ck ASC;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH li AS (SELECT l_orderkey AS k, COUNT(*) AS n
            FROM lineitem GROUP BY l_orderkey)
SELECT o.o_custkey AS ck, MIN(o.o_orderdate) AS md, MAX(o.o_orderdate) AS xd, SUM(li.n) AS sn
FROM orders AS o JOIN li ON li.k = o.o_orderkey
WHERE o.o_custkey <= 40
GROUP BY o.o_custkey
ORDER BY md ASC, ck ASC|

--echo
--echo === ob-14: ORDER BY DATE MAX alias DESC + LIMIT (DESC date display) ===
--let $skip_sort=yes
let $QUERY=
WITH li AS (SELECT l_orderkey AS k, COUNT(*) AS n
            FROM lineitem GROUP BY l_orderkey)
SELECT o.o_custkey AS ck, MAX(o.o_orderdate) AS xd, MIN(o.o_orderdate) AS md, SUM(li.n) AS sn
FROM orders AS o JOIN li ON li.k = o.o_orderkey
WHERE o.o_custkey <= 40
GROUP BY o.o_custkey
ORDER BY xd DESC, ck DESC
LIMIT 8;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH li AS (SELECT l_orderkey AS k, COUNT(*) AS n
            FROM lineitem GROUP BY l_orderkey)
SELECT o.o_custkey AS ck, MAX(o.o_orderdate) AS xd, MIN(o.o_orderdate) AS md, SUM(li.n) AS sn
FROM orders AS o JOIN li ON li.k = o.o_orderkey
WHERE o.o_custkey <= 40
GROUP BY o.o_custkey
ORDER BY xd DESC, ck DESC
LIMIT 8|

--echo
--echo === ob-15: ORDER BY DATETIME MIN alias DESC + LIMIT (temporal extension) ===
# evlog self-join via its own per-group CTE.  MIN(e_modified) is distinct
# per e_group (e_modified strictly increasing in the row number), so the
# order is total; g2 ASC is a safety tie-breaker.  MAX(e_stamp6) checks
# fractional TIMESTAMP display under the suite's UTC session.
--let $skip_sort=yes
let $QUERY=
WITH eg AS (SELECT e_group AS g, COUNT(*) AS n
            FROM evlog GROUP BY e_group)
SELECT ev.e_group AS g2, MIN(ev.e_modified) AS mm, MAX(ev.e_stamp6) AS xs, SUM(eg.n) AS sn
FROM evlog AS ev JOIN eg ON eg.g = ev.e_group
GROUP BY ev.e_group
ORDER BY mm DESC, g2 ASC
LIMIT 12;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH eg AS (SELECT e_group AS g, COUNT(*) AS n
            FROM evlog GROUP BY e_group)
SELECT ev.e_group AS g2, MIN(ev.e_modified) AS mm, MAX(ev.e_stamp6) AS xs, SUM(eg.n) AS sn
FROM evlog AS ev JOIN eg ON eg.g = ev.e_group
GROUP BY ev.e_group
ORDER BY mm DESC, g2 ASC
LIMIT 12|

# ============================================================
# D: composed shapes — fs_batch/fs_topk, two CTEs, HAVING, scalar CTEs.
# ============================================================

--echo
--echo === ob-16: fs_topk aggregate shape (CTE-join-agg, ORDER BY spend DESC LIMIT) ===
# Mirrors the rondb-cli fs_topk registry query (aggregate form): per-key
# GROUP BY with MAX() identity aggregates, ordered by a MAX-of-SUM(DECIMAL)
# alias.  spend is strictly increasing in custkey, so DESC is total; ck
# ASC is a safety tie-breaker.
--let $skip_sort=yes
let $QUERY=
WITH recent AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_totalprice) AS spend
  FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey AS ck, MAX(cu.c_acctbal) AS bal, MAX(recent.cnt) AS cnt, MAX(recent.spend) AS top_spend
FROM customer AS cu JOIN recent ON recent.k = cu.c_custkey
WHERE cu.c_custkey > 100
GROUP BY cu.c_custkey
ORDER BY top_spend DESC, ck ASC
LIMIT 20;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_ob_16.out
let $QUERY=
WITH recent AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_totalprice) AS spend
  FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey AS ck, MAX(cu.c_acctbal) AS bal, MAX(recent.cnt) AS cnt, MAX(recent.spend) AS top_spend
FROM customer AS cu JOIN recent ON recent.k = cu.c_custkey
WHERE cu.c_custkey > 100
GROUP BY cu.c_custkey
ORDER BY top_spend DESC, ck ASC
LIMIT 20;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Result sorted by' $EXPLAIN_FILE || true
--exec grep -qF 'Result limited to 20 rows' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
WITH recent AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_totalprice) AS spend
  FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey AS ck, MAX(cu.c_acctbal) AS bal, MAX(recent.cnt) AS cnt, MAX(recent.spend) AS top_spend
FROM customer AS cu JOIN recent ON recent.k = cu.c_custkey
WHERE cu.c_custkey > 100
GROUP BY cu.c_custkey
ORDER BY top_spend DESC, ck ASC
LIMIT 20|

--echo
--echo === ob-17: two-CTE query + ORDER BY + LIMIT (all-tie primary sort key) ===
# SUM(lc.n) is 1000 for every o_orderstatus group, so the st ASC
# tie-breaker fully decides the order (F, O, P) and the LIMIT boundary.
--let $skip_sort=yes
let $QUERY=
WITH lc AS (SELECT l_orderkey AS k, COUNT(*) AS n
            FROM lineitem GROUP BY l_orderkey),
     cc AS (SELECT c_custkey AS k2, COUNT(*) AS m
            FROM customer GROUP BY c_custkey)
SELECT o.o_orderstatus AS st, COUNT(*) AS c, SUM(lc.n) AS sl, SUM(cc.m) AS sc
FROM orders AS o
JOIN lc ON lc.k = o.o_orderkey
JOIN cc ON cc.k2 = o.o_custkey
GROUP BY o.o_orderstatus
ORDER BY sl DESC, st ASC
LIMIT 2;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH lc AS (SELECT l_orderkey AS k, COUNT(*) AS n
            FROM lineitem GROUP BY l_orderkey),
     cc AS (SELECT c_custkey AS k2, COUNT(*) AS m
            FROM customer GROUP BY c_custkey)
SELECT o.o_orderstatus AS st, COUNT(*) AS c, SUM(lc.n) AS sl, SUM(cc.m) AS sc
FROM orders AS o
JOIN lc ON lc.k = o.o_orderkey
JOIN cc ON cc.k2 = o.o_custkey
GROUP BY o.o_orderstatus
ORDER BY sl DESC, st ASC
LIMIT 2|

--echo
--echo === ob-18: HAVING + ORDER BY + LIMIT compose (buffered path) ===
# HAVING SUM(cf.n) > 27 keeps the twelve sn=30 groups (nk 1..12); all
# tie on sn, so nk ASC decides the LIMIT-5 cut: nk 1..5.
--let $skip_sort=yes
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey <= 137
GROUP BY cu.c_nationkey
HAVING SUM(cf.n) > 27
ORDER BY sn DESC, nk ASC
LIMIT 5;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT cu.c_nationkey AS nk, COUNT(*) AS c, SUM(cf.n) AS sn
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey <= 137
GROUP BY cu.c_nationkey
HAVING SUM(cf.n) > 27
ORDER BY sn DESC, nk ASC
LIMIT 5|

--echo
--echo === ob-19: scalar CTE cross-join + LIMIT 1 (single-row no-op cut) ===
# cs14 watermark shape (green) + LIMIT.  Default sorted compare (one row).
let $QUERY=
WITH a AS (SELECT MAX(o_totalprice) AS m FROM orders),
     b AS (SELECT MIN(o_totalprice) AS n FROM orders)
SELECT GREATEST(a.m, b.n) AS biggest FROM a, b LIMIT 1;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (SELECT MAX(o_totalprice) AS m FROM orders),
     b AS (SELECT MIN(o_totalprice) AS n FROM orders)
SELECT GREATEST(a.m, b.n) AS biggest FROM a, b LIMIT 1|

--echo
--echo === ob-20: scalar aggregate over a CTE + LIMIT 0 (empty result) ===
let $QUERY=
WITH a AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
           FROM orders GROUP BY o_custkey)
SELECT MAX(a.t) AS mx, MIN(a.t) AS mn, COUNT(*) AS n FROM a LIMIT 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
           FROM orders GROUP BY o_custkey)
SELECT MAX(a.t) AS mx, MIN(a.t) AS mn, COUNT(*) AS n FROM a LIMIT 0|

# ============================================================
# E: qualified ORDER BY cte.col where the column is in GROUP BY
# (stays TABLE_COLUMN through the validate path).
# ============================================================

--echo
--echo === ob-21: qualified ORDER BY on a CTE virt column (CTE_SCAN-root re-agg) ===
# agg-17 shape + ORDER BY: the main query re-aggregates the CTE by its
# CHAR(1) output column and orders by the qualified virt column DESC.
# Exercises charset metadata for CTE virt-table GROUP BY columns.
--let $skip_sort=yes
let $QUERY=
WITH ord AS (
  SELECT o_orderstatus AS st, o_shippriority AS pr, SUM(o_shippriority) AS sp
  FROM orders GROUP BY o_orderstatus, o_shippriority)
SELECT ord.st AS st, SUM(ord.sp) AS ssp, MIN(ord.sp) AS mnp, COUNT(*) AS c
FROM ord
GROUP BY ord.st
ORDER BY ord.st DESC;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH ord AS (
  SELECT o_orderstatus AS st, o_shippriority AS pr, SUM(o_shippriority) AS sp
  FROM orders GROUP BY o_orderstatus, o_shippriority)
SELECT ord.st AS st, SUM(ord.sp) AS ssp, MIN(ord.sp) AS mnp, COUNT(*) AS c
FROM ord
GROUP BY ord.st
ORDER BY ord.st DESC|

--echo
--echo === ob-22: big-12 shape + qualified GROUP BY column ORDER BY DESC + LIMIT ===
--let $skip_sort=yes
let $QUERY=
WITH feats AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS spr
  FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey AS ck, SUM(feats.cnt) AS tc, SUM(feats.spr) AS ts
FROM customer AS cu JOIN feats ON feats.k = cu.c_custkey
WHERE cu.c_custkey <= 25
GROUP BY cu.c_custkey
ORDER BY cu.c_custkey DESC
LIMIT 10;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
WITH feats AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS spr
  FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey AS ck, SUM(feats.cnt) AS tc, SUM(feats.spr) AS ts
FROM customer AS cu JOIN feats ON feats.k = cu.c_custkey
WHERE cu.c_custkey <= 25
GROUP BY cu.c_custkey
ORDER BY cu.c_custkey DESC
LIMIT 10|

DELIMITER ;|

# ============================================================
# PROBES (form 1): clean permanent rejection via ronsql_cli --error 1.
# ============================================================

--echo
--echo === ob-P1 (rejection): ORDER BY column not in GROUP BY ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT cu.c_nationkey AS nk, SUM(cf.n) AS sn FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey GROUP BY cu.c_nationkey ORDER BY c_mktsegment;' 2>&1

--echo
--echo === ob-P2 (rejection): ORDER BY aggregate-expression form is a syntax error ===
# The grammar accepts identifiers only in ORDER BY; the aggregate is
# ordered via its SELECT output alias (ob-8..ob-16).  Pins that the
# SUM(...) form fails cleanly instead of being misparsed.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT cu.c_nationkey AS nk, SUM(cf.n) AS sn FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey GROUP BY cu.c_nationkey ORDER BY SUM(cf.n) DESC;' 2>&1

--echo
--echo === ob-P3 (rejection): LIMIT offset,count syntax is not supported ===
# LIMIT takes a single non-negative integer (no OFFSET; plan Phase 6).
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT cu.c_nationkey AS nk, SUM(cf.n) AS sn FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey GROUP BY cu.c_nationkey ORDER BY c_nationkey LIMIT 2,3;' 2>&1

# ============================================================
# PROBES (form 2): genuine-gap / uncertain — recorded, NOT executed.
# ============================================================

# NEXT-PHASE: GROUP BY <cte>.col on the JOIN path (not the CTE_SCAN-root
#   re-agg of ob-21) — no green precedent; the main-query GROUP BY over a
#   linked CTE attribute is untested kernel territory.  Equivalent
#   queries group by the equi-joined parent column instead (ob-22).
#   Probe:
#   WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey)
#   SELECT cf.k AS k, SUM(cf.n) AS sn, COUNT(*) AS c
#   FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
#   WHERE cu.c_custkey <= 30
#   GROUP BY cf.k
#   ORDER BY cf.k DESC
#   LIMIT 10;

--source suite/ronsql_cte/include/cte_drop.inc
