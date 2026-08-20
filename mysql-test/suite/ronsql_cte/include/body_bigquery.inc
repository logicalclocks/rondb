# body_bigquery.inc — larger / combined CTE queries family, data-driven
# CTE suite.
#
# The other families prove individual shapes on 1-2 short CTEs; this
# family combines proven shapes into larger statements: 4 sibling CTEs,
# 3-level chains on realistic data, 300-group multi-batch at two levels,
# long DNF body filters, GREATEST/LEAST + CASE in body WHERE, anti-join
# combined with a positive join, multi-key (2-col) CTE lookup, index
# hints, scalar-CTE watermark, main-root INDEX_SCAN with two CTE
# children, and ORDER BY/LIMIT over an aggregate main.
#
# Deliberately avoided (known-unfixed kernel findings, see
# cte_fix_plan.md): a high-cardinality CTE key (l_orderkey-class) used as
# a lookup under a large root scan (D6 crash), and string MIN/MAX
# re-aggregation through an ordered-index child (D18).
#
# See test_benchmark_extension_plan.md (Phase T2).

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

--echo
--echo === big-01: four sibling CTEs (two entity-keyed, one nation-keyed, one string-keyed) in one aggregating main ===
let $QUERY=
WITH lifetime AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS spr
  FROM orders GROUP BY o_custkey),
     hiprio AS (
  SELECT o_custkey AS k2, MAX(o_orderdate) AS last_d
  FROM orders WHERE o_shippriority >= 2 GROUP BY o_custkey),
     nat_stats AS (
  SELECT c_nationkey AS nk, COUNT(*) AS ncnt
  FROM customer GROUP BY c_nationkey),
     seg_stats AS (
  SELECT c_mktsegment AS seg, COUNT(*) AS scnt
  FROM customer GROUP BY c_mktsegment)
SELECT COUNT(*), SUM(lifetime.cnt), SUM(lifetime.spr), MAX(hiprio.last_d),
       SUM(nat_stats.ncnt), SUM(seg_stats.scnt)
FROM customer AS c
JOIN lifetime ON lifetime.k = c.c_custkey
JOIN hiprio ON hiprio.k2 = c.c_custkey
JOIN nat_stats ON nat_stats.nk = c.c_nationkey
JOIN seg_stats ON seg_stats.seg = c.c_mktsegment;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH lifetime AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS spr
  FROM orders GROUP BY o_custkey),
     hiprio AS (
  SELECT o_custkey AS k2, MAX(o_orderdate) AS last_d
  FROM orders WHERE o_shippriority >= 2 GROUP BY o_custkey),
     nat_stats AS (
  SELECT c_nationkey AS nk, COUNT(*) AS ncnt
  FROM customer GROUP BY c_nationkey),
     seg_stats AS (
  SELECT c_mktsegment AS seg, COUNT(*) AS scnt
  FROM customer GROUP BY c_mktsegment)
SELECT COUNT(*), SUM(lifetime.cnt), SUM(lifetime.spr), MAX(hiprio.last_d),
       SUM(nat_stats.ncnt), SUM(seg_stats.scnt)
FROM customer AS c
JOIN lifetime ON lifetime.k = c.c_custkey
JOIN hiprio ON hiprio.k2 = c.c_custkey
JOIN nat_stats ON nat_stats.nk = c.c_nationkey
JOIN seg_stats ON seg_stats.seg = c.c_mktsegment|

--echo
--echo === big-02: three-level chained CTE on realistic data, scalar main ===
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS s
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, SUM(cnt) AS cnt2, MAX(s) AS mx
  FROM a GROUP BY k),
     c AS (
  SELECT k, MIN(mx) AS mn
  FROM b GROUP BY k)
SELECT COUNT(*), SUM(c.mn) FROM c;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS s
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, SUM(cnt) AS cnt2, MAX(s) AS mx
  FROM a GROUP BY k),
     c AS (
  SELECT k, MIN(mx) AS mn
  FROM b GROUP BY k)
SELECT COUNT(*), SUM(c.mn) FROM c|

--echo
--echo === big-03: chained CTE + sibling CTE mixed in one WITH list ===
let $QUERY=
WITH base AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt
  FROM orders GROUP BY o_custkey),
     roll AS (
  SELECT k, MAX(cnt) AS mx
  FROM base GROUP BY k),
     side AS (
  SELECT c_nationkey AS nk, COUNT(*) AS n
  FROM customer GROUP BY c_nationkey)
SELECT COUNT(*), SUM(roll.mx), SUM(side.n)
FROM customer AS cu
JOIN roll ON roll.k = cu.c_custkey
JOIN side ON side.nk = cu.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH base AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt
  FROM orders GROUP BY o_custkey),
     roll AS (
  SELECT k, MAX(cnt) AS mx
  FROM base GROUP BY k),
     side AS (
  SELECT c_nationkey AS nk, COUNT(*) AS n
  FROM customer GROUP BY c_nationkey)
SELECT COUNT(*), SUM(roll.mx), SUM(side.n)
FROM customer AS cu
JOIN roll ON roll.k = cu.c_custkey
JOIN side ON side.nk = cu.c_nationkey|

--echo
--echo === big-04: 300-group multi-batch at CTE and main level, wide output ===
let $QUERY=
WITH feats AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS spr,
         MIN(o_totalprice) AS mn_p, MAX(o_totalprice) AS mx_p,
         MIN(o_orderdate) AS mn_d, MAX(o_orderdate) AS mx_d
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, COUNT(*), SUM(feats.cnt), SUM(feats.spr),
       MIN(feats.mn_p), MAX(feats.mx_p), MIN(feats.mn_d), MAX(feats.mx_d)
FROM customer AS c JOIN feats ON feats.k = c.c_custkey
GROUP BY c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH feats AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS spr,
         MIN(o_totalprice) AS mn_p, MAX(o_totalprice) AS mx_p,
         MIN(o_orderdate) AS mn_d, MAX(o_orderdate) AS mx_d
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, COUNT(*), SUM(feats.cnt), SUM(feats.spr),
       MIN(feats.mn_p), MAX(feats.mx_p), MIN(feats.mn_d), MAX(feats.mx_d)
FROM customer AS c JOIN feats ON feats.k = c.c_custkey
GROUP BY c.c_custkey|

--echo
--echo === big-05: long DNF (6 disjuncts) in the CTE body WHERE ===
let $QUERY=
WITH flagged AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS spr
  FROM orders
  WHERE (o_shippriority = 0 AND o_totalprice > 10000.00)
     OR (o_shippriority = 1 AND o_totalprice < 5000.00)
     OR (o_shippriority = 2 AND o_clerk IS NULL)
     OR (o_shippriority = 3 AND o_clerk IS NOT NULL AND o_clerk < 10)
     OR (o_shippriority = 4 AND o_totalprice >= 15000.00)
     OR o_totalprice < 100.00
  GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(flagged.cnt), SUM(flagged.spr)
FROM customer AS c JOIN flagged ON flagged.k = c.c_custkey
GROUP BY c.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH flagged AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS spr
  FROM orders
  WHERE (o_shippriority = 0 AND o_totalprice > 10000.00)
     OR (o_shippriority = 1 AND o_totalprice < 5000.00)
     OR (o_shippriority = 2 AND o_clerk IS NULL)
     OR (o_shippriority = 3 AND o_clerk IS NOT NULL AND o_clerk < 10)
     OR (o_shippriority = 4 AND o_totalprice >= 15000.00)
     OR o_totalprice < 100.00
  GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(flagged.cnt), SUM(flagged.spr)
FROM customer AS c JOIN flagged ON flagged.k = c.c_custkey
GROUP BY c.c_nationkey|

--echo
--echo === big-06: GREATEST/LEAST body WHERE + CASE main aggregate ===
# Fixed 2026-08-20 (RONDB-1107): the 8-node RDRS abort was the
# ProcessRes partial-merge asserting on legitimately mixed-signedness
# SUM partials — CASE arms of differing signedness (unsigned gl.cnt vs
# signed literal 0) type each node's accumulator from the arm values
# its rows actually took, and only at 8 nodes / 4 node groups does
# some node own zero nation-7 customers.  Merges now share the
# signedness/promotion-correct helpers in NdbAggregationCommon.hpp.
# See test_benchmark_extension_plan.md §5b.
let $QUERY=
WITH gl AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS spr
  FROM orders
  WHERE GREATEST(o_shippriority, o_clerk, 2) >= 3
    AND LEAST(o_orderkey, 1200) > 100
  GROUP BY o_custkey)
SELECT COUNT(*), SUM(gl.cnt), SUM(gl.spr),
       SUM(CASE WHEN c.c_nationkey = 7 THEN gl.cnt ELSE 0 END)
FROM customer AS c JOIN gl ON gl.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH gl AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS spr
  FROM orders
  WHERE GREATEST(o_shippriority, o_clerk, 2) >= 3
    AND LEAST(o_orderkey, 1200) > 100
  GROUP BY o_custkey)
SELECT COUNT(*), SUM(gl.cnt), SUM(gl.spr),
       SUM(CASE WHEN c.c_nationkey = 7 THEN gl.cnt ELSE 0 END)
FROM customer AS c JOIN gl ON gl.k = c.c_custkey|

--echo
--echo === big-06b: MAX/MIN over mixed-signedness CASE arms ===
# Same defect family as big-06: an unsigned CTE column (gl.cnt, a
# COUNT) against a signed constant arm.  MAX's signed-value-vs-
# unsigned-accumulator kernel arm previously assigned the comparison
# RESULT (0/1) to the accumulator, and the cross-node MAX/MIN merges
# compared in a single signedness domain.  The negative arms exercise
# both mixed-sign orderings.  Expected: 300, 5, 5, 0, 5, -9.
let $QUERY=
WITH gl AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt
  FROM orders GROUP BY o_custkey)
SELECT COUNT(*),
       MAX(CASE WHEN c.c_nationkey = 7 THEN gl.cnt ELSE 0 END),
       MIN(CASE WHEN c.c_nationkey = 7 THEN gl.cnt ELSE 9 END),
       MIN(CASE WHEN c.c_nationkey = 7 THEN gl.cnt ELSE 0 END),
       MAX(CASE WHEN c.c_nationkey = 7 THEN gl.cnt ELSE -9 END),
       MIN(CASE WHEN c.c_nationkey = 7 THEN gl.cnt ELSE -9 END)
FROM customer AS c JOIN gl ON gl.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH gl AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt
  FROM orders GROUP BY o_custkey)
SELECT COUNT(*),
       MAX(CASE WHEN c.c_nationkey = 7 THEN gl.cnt ELSE 0 END),
       MIN(CASE WHEN c.c_nationkey = 7 THEN gl.cnt ELSE 9 END),
       MIN(CASE WHEN c.c_nationkey = 7 THEN gl.cnt ELSE 0 END),
       MAX(CASE WHEN c.c_nationkey = 7 THEN gl.cnt ELSE -9 END),
       MIN(CASE WHEN c.c_nationkey = 7 THEN gl.cnt ELSE -9 END)
FROM customer AS c JOIN gl ON gl.k = c.c_custkey|

--echo
--echo === big-07a: anti-join CTE + positive INNER CTE join ===
# Fixed 2026-08-20 (RONDB-1107): outer/anti CTE_LOOKUP intermediates in
# an aggregating main query use the row-shaped CTE_LOOKUP_OUTER_CHAIN
# protocol — DBLQH answers a miss with a NULL-extended CORR row + CONF
# (parent row continues down the chain) and an anti match with
# REF(GROUP_NOT_FOUND) (parent row dropped).  Previously every shape
# below REFed 1265 (CTE lookup attrinfo malformed) or hung to NDB 274.
# See test_benchmark_extension_plan.md §5b.
let $QUERY=
WITH hiprio AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt
  FROM orders WHERE o_shippriority >= 3 GROUP BY o_custkey),
     lifetime AS (
  SELECT o_custkey AS k2, COUNT(*) AS lcnt
  FROM orders GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(lifetime.lcnt)
FROM customer AS c
LEFT JOIN hiprio ON hiprio.k = c.c_custkey
JOIN lifetime ON lifetime.k2 = c.c_custkey
WHERE hiprio.cnt IS NULL
GROUP BY c.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH hiprio AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt
  FROM orders WHERE o_shippriority >= 3 GROUP BY o_custkey),
     lifetime AS (
  SELECT o_custkey AS k2, COUNT(*) AS lcnt
  FROM orders GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(lifetime.lcnt)
FROM customer AS c
LEFT JOIN hiprio ON hiprio.k = c.c_custkey
JOIN lifetime ON lifetime.k2 = c.c_custkey
WHERE hiprio.cnt IS NULL
GROUP BY c.c_nationkey|

--echo
--echo === big-07b: unreferenced LEFT CTE join + positive INNER CTE join ===
# Same shape without the IS NULL filter.  Found while reproducing
# big-07a: the LEFT + INNER two-CTE main was NOT green as the original
# probe note assumed (the bounding evidence covered normal-table chains
# and single-CTE LEFT joins only) — the plain-LEFT intermediate hit the
# same malformed-attrinfo REF.
let $QUERY=
WITH hiprio AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt
  FROM orders WHERE o_shippriority >= 3 GROUP BY o_custkey),
     lifetime AS (
  SELECT o_custkey AS k2, COUNT(*) AS lcnt
  FROM orders GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(lifetime.lcnt)
FROM customer AS c
LEFT JOIN hiprio ON hiprio.k = c.c_custkey
JOIN lifetime ON lifetime.k2 = c.c_custkey
GROUP BY c.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH hiprio AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt
  FROM orders WHERE o_shippriority >= 3 GROUP BY o_custkey),
     lifetime AS (
  SELECT o_custkey AS k2, COUNT(*) AS lcnt
  FROM orders GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(lifetime.lcnt)
FROM customer AS c
LEFT JOIN hiprio ON hiprio.k = c.c_custkey
JOIN lifetime ON lifetime.k2 = c.c_custkey
GROUP BY c.c_nationkey|

--echo
--echo === big-07c: unreferenced INNER CTE join + positive INNER CTE join ===
# Third latent variant of the same defect: an INNER CTE join whose
# columns are never referenced downstream carried only the CORR read
# (7-word attrinfo) and was rejected by the same >= 8-word gate.  Here
# hiprio acts as a pure join filter (drops customers with no
# o_shippriority >= 3 orders).
let $QUERY=
WITH hiprio AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt
  FROM orders WHERE o_shippriority >= 3 GROUP BY o_custkey),
     lifetime AS (
  SELECT o_custkey AS k2, COUNT(*) AS lcnt
  FROM orders GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(lifetime.lcnt)
FROM customer AS c
JOIN hiprio ON hiprio.k = c.c_custkey
JOIN lifetime ON lifetime.k2 = c.c_custkey
GROUP BY c.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH hiprio AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt
  FROM orders WHERE o_shippriority >= 3 GROUP BY o_custkey),
     lifetime AS (
  SELECT o_custkey AS k2, COUNT(*) AS lcnt
  FROM orders GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(lifetime.lcnt)
FROM customer AS c
JOIN hiprio ON hiprio.k = c.c_custkey
JOIN lifetime ON lifetime.k2 = c.c_custkey
GROUP BY c.c_nationkey|

--echo
--echo === big-08: multi-key (2-col, mixed int + string) CTE complete-key lookup ===
let $QUERY=
WITH seg AS (
  SELECT c_nationkey AS nk, c_mktsegment AS seg, COUNT(*) AS n,
         MAX(c_acctbal) AS mx
  FROM customer GROUP BY c_nationkey, c_mktsegment)
SELECT COUNT(*), SUM(seg.n), MAX(seg.mx)
FROM customer AS c
JOIN seg ON seg.nk = c.c_nationkey AND seg.seg = c.c_mktsegment;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH seg AS (
  SELECT c_nationkey AS nk, c_mktsegment AS seg, COUNT(*) AS n,
         MAX(c_acctbal) AS mx
  FROM customer GROUP BY c_nationkey, c_mktsegment)
SELECT COUNT(*), SUM(seg.n), MAX(seg.mx)
FROM customer AS c
JOIN seg ON seg.nk = c.c_nationkey AND seg.seg = c.c_mktsegment|

--echo
--echo === big-09: FORCE INDEX on the CTE-body root + residual filter + re-aggregation ===
let $QUERY=
WITH by_clerk AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt
  FROM orders FORCE INDEX (idx_o_clerk)
  WHERE o_clerk = 5 AND o_shippriority >= 1
  GROUP BY o_custkey)
SELECT COUNT(*), SUM(by_clerk.cnt)
FROM customer AS c JOIN by_clerk ON by_clerk.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH by_clerk AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt
  FROM orders FORCE INDEX (idx_o_clerk)
  WHERE o_clerk = 5 AND o_shippriority >= 1
  GROUP BY o_custkey)
SELECT COUNT(*), SUM(by_clerk.cnt)
FROM customer AS c JOIN by_clerk ON by_clerk.k = c.c_custkey|

--echo
--echo === big-10: scalar-CTE watermark — GREATEST over two scalar CTE outputs ===
let $QUERY=
WITH hi AS (SELECT MAX(o_orderkey) AS v FROM orders),
     lo AS (SELECT MIN(o_orderkey) AS w FROM orders)
SELECT GREATEST(hi.v, lo.w) FROM hi, lo;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH hi AS (SELECT MAX(o_orderkey) AS v FROM orders),
     lo AS (SELECT MIN(o_orderkey) AS w FROM orders)
SELECT GREATEST(hi.v, lo.w) FROM hi, lo|

--echo
--echo === big-11: main-root INDEX_SCAN (range on idx_c_nationkey) with two CTE children ===
let $QUERY=
WITH lifetime AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt
  FROM orders GROUP BY o_custkey),
     natstat AS (
  SELECT c_nationkey AS nk, COUNT(*) AS n
  FROM customer GROUP BY c_nationkey)
SELECT COUNT(*), SUM(lifetime.cnt), SUM(natstat.n)
FROM customer AS c
JOIN lifetime ON lifetime.k = c.c_custkey
JOIN natstat ON natstat.nk = c.c_nationkey
WHERE c.c_nationkey >= 5 AND c.c_nationkey <= 10;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH lifetime AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt
  FROM orders GROUP BY o_custkey),
     natstat AS (
  SELECT c_nationkey AS nk, COUNT(*) AS n
  FROM customer GROUP BY c_nationkey)
SELECT COUNT(*), SUM(lifetime.cnt), SUM(natstat.n)
FROM customer AS c
JOIN lifetime ON lifetime.k = c.c_custkey
JOIN natstat ON natstat.nk = c.c_nationkey
WHERE c.c_nationkey >= 5 AND c.c_nationkey <= 10|

--echo
--echo === big-12: ORDER BY aggregate alias + GROUP BY column, LIMIT, over a CTE join ===
let $QUERY=
WITH feats AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS spr
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, SUM(feats.cnt) AS total_cnt, SUM(feats.spr) AS total_spr
FROM customer AS c JOIN feats ON feats.k = c.c_custkey
GROUP BY c.c_custkey
ORDER BY total_spr DESC, c_custkey ASC
LIMIT 10;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH feats AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_shippriority) AS spr
  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, SUM(feats.cnt) AS total_cnt, SUM(feats.spr) AS total_spr
FROM customer AS c JOIN feats ON feats.k = c.c_custkey
GROUP BY c.c_custkey
ORDER BY total_spr DESC, c_custkey ASC
LIMIT 10|

DELIMITER ;|

# ---------------------------------------------------------------------
# Probes — NOT executed.
#
# NEXT-PHASE: high-cardinality CTE key under a large root scan — the D6
# DBSPJ assert (cte_fix_plan.md Phase 2, unfixed).  Canonical repro kept
# in body_agg.inc / findings; not duplicated here.
#
# NEXT-PHASE: string MIN/MAX re-aggregation through an ordered-index
# child (D18, Error 6000) — this family's string re-agg cases join via
# PK/complete-key lookups only.
# ---------------------------------------------------------------------

--source suite/ronsql_cte/include/cte_drop.inc
