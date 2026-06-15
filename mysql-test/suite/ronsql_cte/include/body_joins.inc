# body_joins.inc — Joins family, data-driven CTE suite.
#
# FOCUS: join shapes with an aggregating CTE.  Real-table JOIN cte (complete
# key, INNER), LEFT JOIN with genuine NULL injection (CTE omits some keys),
# cte JOIN real_table (CTE as scanned root), anti-join, multiple CTEs joined
# to one parent, LEFT->INNER promotion, multi-key CTE complete-key lookup,
# 3-table INNER chains.  Plus a partial-key rejection-assert and a CTE_SCAN
# outer-join-child probe.
#
# NULL-injection technique: every custkey 1..300 has orders, so an unfiltered
# orders CTE matches every customer.  To force unmatched parent rows, the CTE
# body filters `WHERE o_custkey <= 150` so it only materialises keys 1..150;
# customers 151..300 then inject NULL on a LEFT JOIN.
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
# MAIN (green) cases
# ============================================================================

--echo
--echo === J1: real_table JOIN cte, complete key, INNER, re-aggregate in main ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT cust.k, SUM(cust.t)
FROM customer AS c
JOIN cust ON cust.k = c.c_custkey
GROUP BY cust.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT cust.k, SUM(cust.t)
FROM customer AS c
JOIN cust ON cust.k = c.c_custkey
GROUP BY cust.k|

--echo
--echo === J2: real_table JOIN cte, COUNT per custkey, main groups by mktsegment ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_mktsegment, SUM(cust.n)
FROM customer AS c
JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_mktsegment;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_mktsegment, SUM(cust.n)
FROM customer AS c
JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_mktsegment|


# NEXT-PHASE (DISABLED — D1, SUM over DECIMAL): this case SUMs scale-2 DECIMAL
# MIN/MAX CTE outputs (SUM(cust.mn)/SUM(cust.mx), mn/mx = MIN/MAX(o_totalprice)).
# The D15 display-scale issue is now fixed (MIN/MAX format with scale — agg-08/09),
# but SUMming a DECIMAL-derived value goes through DOUBLE accumulation, which is
# the separate D1 limitation (SUM over DECIMAL not yet supported / lossy).
# See findings/_discovery_log.md (D1, D15).
--echo
--echo === J4: LEFT JOIN, CTE omits keys 151..300 -> genuine NULL injection ===
# CTE materialises only custkeys 1..150 (WHERE o_custkey <= 150); customers
# 151..300 are unmatched on the LEFT JOIN and contribute SUM(cust.t) = NULL.
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_custkey <= 150 GROUP BY o_custkey)
SELECT c.c_custkey, SUM(cust.t)
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_custkey <= 150 GROUP BY o_custkey)
SELECT c.c_custkey, SUM(cust.t)
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_custkey|

--echo
--echo === J5: LEFT JOIN over omitted-key CTE, main groups by parent column ===
# Mix of matched (nationkeys covering custkeys<=150) and NULL-injected groups.
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders WHERE o_custkey <= 150 GROUP BY o_custkey)
SELECT c.c_nationkey, SUM(cust.t), SUM(cust.n)
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t, COUNT(*) AS n
  FROM orders WHERE o_custkey <= 150 GROUP BY o_custkey)
SELECT c.c_nationkey, SUM(cust.t), SUM(cust.n)
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
GROUP BY c.c_nationkey|

--echo
--echo === J6: cte JOIN real_table (CTE as scanned root, real table joined) ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT c.c_mktsegment, SUM(cust.t)
FROM cust JOIN customer AS c ON c.c_custkey = cust.k
GROUP BY c.c_mktsegment;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT c.c_mktsegment, SUM(cust.t)
FROM cust JOIN customer AS c ON c.c_custkey = cust.k
GROUP BY c.c_mktsegment|

--echo
--echo === J7: cte JOIN real_table, CTE root, group by CTE key ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT cust.k, SUM(cust.n)
FROM cust JOIN customer AS c ON c.c_custkey = cust.k
GROUP BY cust.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT cust.k, SUM(cust.n)
FROM cust JOIN customer AS c ON c.c_custkey = cust.k
GROUP BY cust.k|

--echo
--echo === J8: anti-join — LEFT JOIN omitted-key CTE WHERE cte.t IS NULL, aggregate over parent ===
# Only the NULL-injected (unmatched) customers 151..300 survive the filter;
# main aggregates (COUNT + SUM/MIN/MAX over the surviving parent rows).
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_custkey <= 150 GROUP BY o_custkey)
SELECT COUNT(*), MIN(c.c_custkey), MAX(c.c_custkey), SUM(c.c_nationkey)
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
WHERE cust.t IS NULL;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_custkey <= 150 GROUP BY o_custkey)
SELECT COUNT(*), MIN(c.c_custkey), MAX(c.c_custkey), SUM(c.c_nationkey)
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
WHERE cust.t IS NULL|

--echo
--echo === J9: anti-join on CTE GB key IS NULL, aggregate over parent ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_custkey <= 150 GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*)
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
WHERE cust.k IS NULL
GROUP BY c.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_custkey <= 150 GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*)
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
WHERE cust.k IS NULL
GROUP BY c.c_nationkey|

--echo
--echo === J10: two independent aggregating CTEs joined to the same parent ===
let $QUERY=
WITH
  totals AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
             FROM orders GROUP BY o_custkey),
  counts AS (SELECT o_custkey AS k, COUNT(*) AS n
             FROM orders GROUP BY o_custkey)
SELECT c.c_mktsegment, SUM(totals.t), SUM(counts.n)
FROM customer AS c
JOIN totals ON totals.k = c.c_custkey
JOIN counts ON counts.k = c.c_custkey
GROUP BY c.c_mktsegment;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH
  totals AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
             FROM orders GROUP BY o_custkey),
  counts AS (SELECT o_custkey AS k, COUNT(*) AS n
             FROM orders GROUP BY o_custkey)
SELECT c.c_mktsegment, SUM(totals.t), SUM(counts.n)
FROM customer AS c
JOIN totals ON totals.k = c.c_custkey
JOIN counts ON counts.k = c.c_custkey
GROUP BY c.c_mktsegment|

--echo
--echo === J11: two CTEs grouped on different dimensions, joined to one parent ===
let $QUERY=
WITH
  totals AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
             FROM orders GROUP BY o_custkey),
  maxes  AS (SELECT o_custkey AS k, MAX(o_shippriority) AS mx
             FROM orders GROUP BY o_custkey)
SELECT totals.k, SUM(totals.t), SUM(maxes.mx)
FROM customer AS c
JOIN totals ON totals.k = c.c_custkey
JOIN maxes  ON maxes.k  = c.c_custkey
GROUP BY totals.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH
  totals AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
             FROM orders GROUP BY o_custkey),
  maxes  AS (SELECT o_custkey AS k, MAX(o_shippriority) AS mx
             FROM orders GROUP BY o_custkey)
SELECT totals.k, SUM(totals.t), SUM(maxes.mx)
FROM customer AS c
JOIN totals ON totals.k = c.c_custkey
JOIN maxes  ON maxes.k  = c.c_custkey
GROUP BY totals.k|

--echo
--echo === J12: LEFT->INNER promotion — LEFT JOIN cust + WHERE cust.t > thr ===
# WHERE on the CTE SUM output rejects NULL, so the optimizer promotes the
# LEFT JOIN to INNER; unmatched (omitted-key) customers are dropped.
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_custkey <= 150 GROUP BY o_custkey)
SELECT cust.k, SUM(cust.t)
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
WHERE cust.t > 50000
GROUP BY cust.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_custkey <= 150 GROUP BY o_custkey)
SELECT cust.k, SUM(cust.t)
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
WHERE cust.t > 50000
GROUP BY cust.k|

--echo
--echo === J13: LEFT->INNER promotion via WHERE on CTE GB key ===
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_custkey <= 150 GROUP BY o_custkey)
SELECT cust.k, SUM(cust.t)
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
WHERE cust.k > 100
GROUP BY cust.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders WHERE o_custkey <= 150 GROUP BY o_custkey)
SELECT cust.k, SUM(cust.t)
FROM customer AS c LEFT JOIN cust ON cust.k = c.c_custkey
WHERE cust.k > 100
GROUP BY cust.k|


--echo
--echo === J14: multi-key (2-col virt PK) complete-key CTE lookup, predicates in virt-PK order (D20 — now green) ===
# D20 was recorded as a hang against a stale rdrs2/ndbmtd during parallel suite
# authoring; multi-key complete-key CTE lookup executes correctly on the current
# build, in either predicate order (J14 virt-PK order, J15 reversed).
let $QUERY=
WITH x AS (SELECT o_custkey AS k1, o_orderstatus AS k2, COUNT(*) AS n
           FROM orders GROUP BY o_custkey, o_orderstatus)
SELECT o2.o_custkey, SUM(x.n) FROM orders AS o2
JOIN x ON x.k1 = o2.o_custkey AND x.k2 = o2.o_orderstatus GROUP BY o2.o_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH x AS (SELECT o_custkey AS k1, o_orderstatus AS k2, COUNT(*) AS n
           FROM orders GROUP BY o_custkey, o_orderstatus)
SELECT o2.o_custkey, SUM(x.n) FROM orders AS o2
JOIN x ON x.k1 = o2.o_custkey AND x.k2 = o2.o_orderstatus GROUP BY o2.o_custkey|

--echo
--echo === J15: same multi-key CTE lookup, predicates in REVERSED order (D20 — now green) ===
let $QUERY=
WITH x AS (SELECT o_custkey AS k1, o_orderstatus AS k2, COUNT(*) AS n
           FROM orders GROUP BY o_custkey, o_orderstatus)
SELECT o2.o_custkey, SUM(x.n) FROM orders AS o2
JOIN x ON x.k2 = o2.o_orderstatus AND x.k1 = o2.o_custkey GROUP BY o2.o_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH x AS (SELECT o_custkey AS k1, o_orderstatus AS k2, COUNT(*) AS n
           FROM orders GROUP BY o_custkey, o_orderstatus)
SELECT o2.o_custkey, SUM(x.n) FROM orders AS o2
JOIN x ON x.k2 = o2.o_orderstatus AND x.k1 = o2.o_custkey GROUP BY o2.o_custkey|

# D5 (J16/J17): 3-table INNER chain real customer JOIN cte cust JOIN real nation.
# The pushed aggregation needs columns from BOTH children of customer — SUM over
# the CTE child (cust) and GROUP BY a real-table child (nation) — i.e. a fan-out
# whose agg references a NON-ANCESTOR sibling.  This used to CRASH the RDRS server
# (NdbQueryBuilder.cpp appendLinkedOperand assert / null deref, finding D5 — was
# mis-recorded as a hang).  RonSQL now rejects it cleanly at prepare time
# (linked_source_is_leaf_ancestor guard in emit_child_ops).  Kept ENABLED as
# rejection-asserts in the REJECTION-ASSERTS section below.  Supporting fan-out
# aggregation across a real-table/CTE sibling is tracked for a later phase.

--echo
--echo === J18: real JOIN cte(agg-leaf), main-query WHERE on a parent column (D19 — now green) ===
# D19 was recorded as a hang against a stale rdrs2/ndbmtd during parallel suite
# authoring; it does not reproduce on the current build (the CTE join-agg
# routing/teardown hardening was already in HEAD).  A parent-column WHERE on the
# root TABLE_SCAN of a real-JOIN-cte(agg-leaf) query executes correctly.
let $QUERY=
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT cust.k, SUM(cust.t)
FROM customer AS c JOIN cust ON cust.k = c.c_custkey
WHERE c.c_nationkey = 5
GROUP BY cust.k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cust AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT cust.k, SUM(cust.t)
FROM customer AS c JOIN cust ON cust.k = c.c_custkey
WHERE c.c_nationkey = 5
GROUP BY cust.k|

--echo
--echo === J19: PARTIAL-key lookup to a multi-key CTE; root-eligible parent -> I.16c CTE_SCAN-root rewrite (D21 — now green) ===
# x has a 2-col virtual PK (k1=o_custkey, k2=o_orderstatus); the join binds only
# k1 (= c.c_custkey), a strict subset of the PK.  Because the bound parent alias
# (customer c) is the original root, the I.16c auto-rewrite promotes x to a
# CTE_SCAN root, sums x.t across all k2 values per customer, and linearizes the
# would-be fan-out (x -> customer -> nation).  Verified byte-identical to MySQL.
# (The original D21 finding pre-dated I.16c; partial-key here is supported, not
# rejected.)  Contrast J20 below — binding a NON-PK column is rejected.
let $QUERY=
WITH x AS (SELECT o_custkey AS k1, o_orderstatus AS k2, SUM(o_shippriority) AS t
           FROM orders GROUP BY o_custkey, o_orderstatus)
SELECT n.n_name, SUM(x.t) AS s
FROM customer AS c
JOIN nation AS n ON n.n_nationkey = c.c_nationkey
JOIN x ON x.k1 = c.c_custkey
GROUP BY n.n_name;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH x AS (SELECT o_custkey AS k1, o_orderstatus AS k2, SUM(o_shippriority) AS t
           FROM orders GROUP BY o_custkey, o_orderstatus)
SELECT n.n_name, SUM(x.t) AS s
FROM customer AS c
JOIN nation AS n ON n.n_nationkey = c.c_nationkey
JOIN x ON x.k1 = c.c_custkey
GROUP BY n.n_name|

# ============================================================================
# REJECTION-ASSERTS (clean permanent reject — kept ENABLED via ronsql_cli)
# ============================================================================
# Rejection happens at prepare time, so the join dictionary-cache caveat that
# forces $suppress_ronsql_cli for the green cases does not apply here.

DELIMITER ;|

--echo
--echo == J16 (D5): customer JOIN cte cust JOIN real nation, GROUP BY n_regionkey — fan-out agg across non-ancestor sibling, permanent rejection ==
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH cust AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t FROM orders GROUP BY o_custkey) SELECT n.n_regionkey, SUM(cust.t) FROM customer AS c JOIN cust ON cust.k = c.c_custkey JOIN nation AS n ON n.n_nationkey = c.c_nationkey GROUP BY n.n_regionkey;' 2>&1

--echo
--echo == J17 (D5): same 3-table chain, GROUP BY n_name, two SUMs — permanent rejection ==
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH cust AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS prio FROM orders GROUP BY o_custkey) SELECT n.n_name, SUM(cust.n), SUM(cust.prio) FROM customer AS c JOIN cust ON cust.k = c.c_custkey JOIN nation AS n ON n.n_nationkey = c.c_nationkey GROUP BY n.n_name;' 2>&1

--echo
--echo == J20 (D21): WRONG-column multi-key lookup — binds aggregate output x.n, not PK col k2 — permanent rejection ==
# x has virtual PK (k1, k2); the join binds k1 + the aggregate output x.n, leaving
# the PK column k2 unbound.  Unlike J19 (which binds a strict SUBSET of the PK with
# a root-eligible parent and is auto-rewritten to a CTE_SCAN root), binding a
# NON-PK column cannot be satisfied by a key lookup or the I.16c rewrite, so RonSQL
# rejects at prepare time: "CTE lookup key references a CTE output column that is
# not part of the virtual primary key."  (Resolves D21's wrong-column half.)
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH x AS (SELECT o_custkey AS k1, o_orderstatus AS k2, COUNT(*) AS n FROM orders GROUP BY o_custkey, o_orderstatus) SELECT o2.o_custkey, SUM(x.n) FROM orders AS o2 JOIN x ON x.k1 = o2.o_custkey AND x.n = o2.o_shippriority GROUP BY o2.o_custkey;' 2>&1

# ============================================================================
# PROBES (recorded + disabled)
# ============================================================================

# NEXT-PHASE: CTE_SCAN as an outer-join child — defensively rejected per the
# capability matrix (cte_outer_join_phase_3.md dropped CTE_SCAN-as-outer-join-
# child at the kernel level; RonSQL Phase G keeps a defensive reject).  Here a
# scanned CTE would have to be the dependent (right) side of a LEFT JOIN whose
# left side is a real table that does NOT bind the CTE's full virtual PK — so
# the CTE cannot be a CTE_LOOKUP and would need to be a CTE_SCAN child of the
# outer join.  Whether this surfaces as a clean permanent reject or a deeper
# gap is uncertain, so it is recorded here commented-out rather than run.
#   WITH bycust AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
#                   FROM orders GROUP BY o_custkey)
#   SELECT n.n_nationkey, SUM(bycust.t)
#   FROM nation AS n LEFT JOIN bycust ON bycust.k > n.n_nationkey
#   GROUP BY n.n_nationkey;

# NEXT-PHASE: AVG in a CTE body — AVG is deferred per the capability matrix
# (only COUNT/SUM/MIN/MAX supported).  Joining to such a CTE is the same join
# shape as J1 but the unsupported aggregate would fail at prepare; recorded
# rather than run so the family stays green.
#   WITH cust AS (SELECT o_custkey AS k, AVG(o_totalprice) AS a
#                 FROM orders GROUP BY o_custkey)
#   SELECT cust.k, SUM(cust.a)
#   FROM customer AS c JOIN cust ON cust.k = c.c_custkey
#   GROUP BY cust.k;

--source suite/ronsql_cte/include/cte_drop.inc
