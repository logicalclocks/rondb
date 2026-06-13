# body_agg.inc — agg family, data-driven CTE suite.
#
# FOCUS: the CTE-body AGGREGATION matrix across data types + GROUP BY shapes.
# Per the CORRECTED ENVELOPE, EVERY main SELECT AGGREGATES — either a scalar
# aggregate (SELECT MIN(cte.x), MAX(cte.y), SUM(cte.z), COUNT(*) FROM ...) or
# GROUP BY <parent dimension> with aggregates.  Projection-only main SELECTs
# over a CTE_LOOKUP child HANG and are NEVER built as MAIN cases (recorded as a
# disabled NEXT-PHASE probe, see D3).
#
# Main SELECT is exercised BOTH ways:
#   (A) aggregation over a CTE complete-key lookup leaf joined to a real table
#       (real_table JOIN cte ON cte.key = real.key, main re-aggregates), and
#   (B) aggregation over a CTE_SCAN root (WITH x AS (...agg...) SELECT agg FROM x).
#
# CTEs are keyed by a moderate-cardinality key joined to a SMALL parent:
#   o_custkey(300)->customer, s_nationkey->supplier/nation, p_brand/p_size->part,
#   c_nationkey->nation.  A CTE keyed by l_orderkey(1500) under an orders root
#   scan CRASHES (D6) and is NEVER used as a CTE_LOOKUP child — lineitem
#   aggregates are exercised via the CTE_SCAN root form instead.
#
# Aggregates exercised: COUNT(*) (NOT COUNT(<col>) — hangs, D2); SUM over
# INTEGER columns only (o_shippriority, l_quantity, s_nationkey — NOT DECIMAL,
# D1); MIN/MAX over every named width (TINYINT, SMALLINT UNSIGNED, MEDIUMINT,
# INT, BIGINT UNSIGNED), DECIMAL scale-2 and scale-0, and CHAR / VARCHAR.
# GROUP BY a single column and GROUP BY multiple columns.
#
# Probes (projection-only-over-CTE_LOOKUP hang, COUNT(col) hang, SUM(decimal)
# error, the l_orderkey-under-orders crash, AVG, COUNT(DISTINCT), DATE MIN/MAX,
# GROUP BY expression, post-aggregation SELECT expressions, FLOAT/DOUBLE SUM)
# are recorded below as NEXT-PHASE-disabled comment blocks and in
# findings/agg.md — they do NOT run.

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

# =====================================================================
# Group A — CTE complete-key lookup leaf joined to a real table; the
# main SELECT AGGREGATES over the CTE's per-group output.
# =====================================================================


# D15 (scale-2 DECIMAL MIN/MAX display scale) is FIXED — RonSQL now formats the
# DOUBLE-widened DECIMAL result with the source scale (20055.00 / 5275.50), see
# agg-08/09 below.  The agg-01 "flagship COUNT/SUM/MIN/MAX" case is NOT restored
# as a single query only because its SUM is over a DECIMAL column, which is the
# separate D1 limitation (SUM over DECIMAL not yet supported); its MIN/MAX
# coverage is agg-08/09.  See findings/_discovery_log.md (D15, D1).

# NEXT-PHASE (DISABLED — HANG, D2): COUNT(<column>) (here the nullable o_clerk)
# in a CTE body HANGS RonSQL/RDRS (never returns).  COUNT(*) in the same shape
# is fine.  Disabled so the suite stays runnable.  See findings/agg.md (agg-02).
# Repro:
#   WITH ord AS (SELECT o_custkey AS k, COUNT(*) AS n_all, COUNT(o_clerk) AS n_clerk
#                FROM orders GROUP BY o_custkey)
#   SELECT SUM(ord.n_all), SUM(ord.n_clerk) FROM customer AS c JOIN ord ON ord.k = c.c_custkey;

# =====================================================================
# MIN/MAX type coverage — one CTE per width, joined to its SMALL parent so the
# CTE-body aggregate type makes the full round trip through CTE_LOOKUP, and the
# main SELECT AGGREGATES (scalar or GROUP BY parent dimension).
# =====================================================================

--echo
--echo === agg-03: MIN/MAX over TINYINT (nation.n_nationkey), grouped by region ===
let $QUERY=
WITH nat AS (
  SELECT n_regionkey AS rk, MIN(n_nationkey) AS mn, MAX(n_nationkey) AS mx, COUNT(*) AS n
  FROM nation GROUP BY n_regionkey)
SELECT MIN(nat.mn), MAX(nat.mx), SUM(nat.n)
FROM region AS r
JOIN nat ON nat.rk = r.r_regionkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH nat AS (
  SELECT n_regionkey AS rk, MIN(n_nationkey) AS mn, MAX(n_nationkey) AS mx, COUNT(*) AS n
  FROM nation GROUP BY n_regionkey)
SELECT MIN(nat.mn), MAX(nat.mx), SUM(nat.n)
FROM region AS r
JOIN nat ON nat.rk = r.r_regionkey|

--echo
--echo === agg-04: MIN/MAX over SMALLINT UNSIGNED (orders.o_shippriority), grouped by custkey ===
let $QUERY=
WITH ord AS (
  SELECT o_custkey AS k, MIN(o_shippriority) AS mn, MAX(o_shippriority) AS mx
  FROM orders GROUP BY o_custkey)
SELECT MIN(ord.mn), MAX(ord.mx), SUM(ord.mx)
FROM customer AS c
JOIN ord ON ord.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH ord AS (
  SELECT o_custkey AS k, MIN(o_shippriority) AS mn, MAX(o_shippriority) AS mx
  FROM orders GROUP BY o_custkey)
SELECT MIN(ord.mn), MAX(ord.mx), SUM(ord.mx)
FROM customer AS c
JOIN ord ON ord.k = c.c_custkey|

--echo
--echo === agg-05: MIN/MAX over MEDIUMINT (part.p_size), grouped by brand (parent dimension) ===
let $QUERY=
WITH prt AS (
  SELECT p_brand AS brand, MIN(p_size) AS mn, MAX(p_size) AS mx, COUNT(*) AS n
  FROM part GROUP BY p_brand)
SELECT p.p_brand, MIN(prt.mn), MAX(prt.mx), SUM(prt.n)
FROM part AS p
JOIN prt ON prt.brand = p.p_brand
GROUP BY p.p_brand;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH prt AS (
  SELECT p_brand AS brand, MIN(p_size) AS mn, MAX(p_size) AS mx, COUNT(*) AS n
  FROM part GROUP BY p_brand)
SELECT p.p_brand, MIN(prt.mn), MAX(prt.mx), SUM(prt.n)
FROM part AS p
JOIN prt ON prt.brand = p.p_brand
GROUP BY p.p_brand|

# agg-06 (was D6, FIXED): scalar main aggregation over a high-cardinality CTE
# (l_orderkey/1500) used as a CTE_LOOKUP child of a large orders root scan.
# Crashed (DBSPJ Error 2343, m_cnt_active==0) before the CTE multi-batch
# completion + GROUP BY metadata fixes; now green.
--echo
--echo === agg-06: high-cardinality lineitem CTE (l_orderkey) as CTE_LOOKUP child of orders root, scalar main agg ===
let $QUERY=
WITH li AS (SELECT l_orderkey AS k, MIN(l_quantity) AS mn, MAX(l_quantity) AS mx, SUM(l_quantity) AS sq
            FROM lineitem GROUP BY l_orderkey)
SELECT MIN(li.mn), MAX(li.mx), SUM(li.sq) FROM orders AS o JOIN li ON li.k = o.o_orderkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH li AS (SELECT l_orderkey AS k, MIN(l_quantity) AS mn, MAX(l_quantity) AS mx, SUM(l_quantity) AS sq
            FROM lineitem GROUP BY l_orderkey)
SELECT MIN(li.mn), MAX(li.mx), SUM(li.sq) FROM orders AS o JOIN li ON li.k = o.o_orderkey|

--echo
--echo === agg-07: MIN/MAX/SUM over BIGINT UNSIGNED (supplier.s_total_sales), grouped by nation ===
let $QUERY=
WITH sup AS (
  SELECT s_nationkey AS nk, MIN(s_total_sales) AS mn, MAX(s_total_sales) AS mx, COUNT(*) AS n
  FROM supplier GROUP BY s_nationkey)
SELECT MIN(sup.mn), MAX(sup.mx), SUM(sup.n)
FROM nation AS n
JOIN sup ON sup.nk = n.n_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sup AS (
  SELECT s_nationkey AS nk, MIN(s_total_sales) AS mn, MAX(s_total_sales) AS mx, COUNT(*) AS n
  FROM supplier GROUP BY s_nationkey)
SELECT MIN(sup.mn), MAX(sup.mx), SUM(sup.n)
FROM nation AS n
JOIN sup ON sup.nk = n.n_nationkey|


# agg-08/09 (was D15, FIXED): scale-2 DECIMAL MIN/MAX type-coverage.  RonSQL now
# formats the DOUBLE-widened DECIMAL result with the source scale (e.g.
# 20055.00 / 5275.50) like MySQL.  Scale-0 DECIMAL MIN/MAX (agg-10) is below.

--echo
--echo === agg-08: MIN/MAX over DECIMAL(12,2) o_totalprice, re-aggregated through CTE_LOOKUP ===
let $QUERY=
WITH x AS (SELECT o_custkey AS k, MIN(o_totalprice) AS mn, MAX(o_totalprice) AS mx
           FROM orders GROUP BY o_custkey)
SELECT MIN(x.mn), MAX(x.mx) FROM customer AS c JOIN x ON x.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH x AS (SELECT o_custkey AS k, MIN(o_totalprice) AS mn, MAX(o_totalprice) AS mx
           FROM orders GROUP BY o_custkey)
SELECT MIN(x.mn), MAX(x.mx) FROM customer AS c JOIN x ON x.k = c.c_custkey|

--echo
--echo === agg-09: MIN/MAX over DECIMAL(12,2) c_acctbal, re-aggregated through CTE_LOOKUP ===
let $QUERY=
WITH cs AS (SELECT c_nationkey AS nk, MIN(c_acctbal) AS mn, MAX(c_acctbal) AS mx
            FROM customer GROUP BY c_nationkey)
SELECT MIN(cs.mn), MAX(cs.mx) FROM nation AS n JOIN cs ON cs.nk = n.n_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cs AS (SELECT c_nationkey AS nk, MIN(c_acctbal) AS mn, MAX(c_acctbal) AS mx
            FROM customer GROUP BY c_nationkey)
SELECT MIN(cs.mn), MAX(cs.mx) FROM nation AS n JOIN cs ON cs.nk = n.n_nationkey|

--echo
--echo === agg-10: MIN/MAX over DECIMAL scale-0 (supplier.s_margin) + SUM(int), grouped by nation ===
let $QUERY=
WITH sup AS (
  SELECT s_nationkey AS nk, MIN(s_margin) AS mn, MAX(s_margin) AS mx, SUM(s_nationkey) AS sm
  FROM supplier GROUP BY s_nationkey)
SELECT MIN(sup.mn), MAX(sup.mx), SUM(sup.sm)
FROM nation AS n
JOIN sup ON sup.nk = n.n_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sup AS (
  SELECT s_nationkey AS nk, MIN(s_margin) AS mn, MAX(s_margin) AS mx, SUM(s_nationkey) AS sm
  FROM supplier GROUP BY s_nationkey)
SELECT MIN(sup.mn), MAX(sup.mx), SUM(sup.sm)
FROM nation AS n
JOIN sup ON sup.nk = n.n_nationkey|

# --- string MIN/MAX (CHAR + VARCHAR) ---

# agg-11..15 (was D18, FIXED): the main query RE-AGGREGATES a string (CHAR /
# VARCHAR) MIN/MAX CTE output.  Crashed (NDB Error 6000) before the CTE linked-
# column type-metadata fix; now green.  Covers CHAR(1) o_orderstatus, VARCHAR(12)
# c_mktsegment (lookup form), and CHAR(1) l_returnflag, CHAR(10) p_brand,
# VARCHAR(40) p_name (CTE_SCAN-root form).

--echo
--echo === agg-11: CHAR(1) o_orderstatus MIN/MAX re-aggregated through CTE_LOOKUP ===
let $QUERY=
WITH ord AS (SELECT o_custkey AS k, MIN(o_orderstatus) AS mn, MAX(o_orderstatus) AS mx, COUNT(*) AS n
             FROM orders GROUP BY o_custkey)
SELECT MIN(ord.mn), MAX(ord.mx), SUM(ord.n) FROM customer AS c JOIN ord ON ord.k = c.c_custkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH ord AS (SELECT o_custkey AS k, MIN(o_orderstatus) AS mn, MAX(o_orderstatus) AS mx, COUNT(*) AS n
             FROM orders GROUP BY o_custkey)
SELECT MIN(ord.mn), MAX(ord.mx), SUM(ord.n) FROM customer AS c JOIN ord ON ord.k = c.c_custkey|

--echo
--echo === agg-12: VARCHAR(12) c_mktsegment MIN/MAX re-aggregated through CTE_LOOKUP ===
let $QUERY=
WITH cs AS (SELECT c_nationkey AS nk, MIN(c_mktsegment) AS mn, MAX(c_mktsegment) AS mx, COUNT(*) AS n
            FROM customer GROUP BY c_nationkey)
SELECT MIN(cs.mn), MAX(cs.mx), SUM(cs.n) FROM nation AS n JOIN cs ON cs.nk = n.n_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cs AS (SELECT c_nationkey AS nk, MIN(c_mktsegment) AS mn, MAX(c_mktsegment) AS mx, COUNT(*) AS n
            FROM customer GROUP BY c_nationkey)
SELECT MIN(cs.mn), MAX(cs.mx), SUM(cs.n) FROM nation AS n JOIN cs ON cs.nk = n.n_nationkey|

--echo
--echo === agg-13: CHAR(1) l_returnflag MIN/MAX re-aggregated over a CTE_SCAN root ===
let $QUERY=
WITH l AS (SELECT l_orderkey AS k, MIN(l_returnflag) AS mn, MAX(l_returnflag) AS mx
           FROM lineitem GROUP BY l_orderkey)
SELECT MIN(l.mn), MAX(l.mx), COUNT(*) FROM l;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH l AS (SELECT l_orderkey AS k, MIN(l_returnflag) AS mn, MAX(l_returnflag) AS mx
           FROM lineitem GROUP BY l_orderkey)
SELECT MIN(l.mn), MAX(l.mx), COUNT(*) FROM l|

--echo
--echo === agg-14: CHAR(10) p_brand MIN/MAX re-aggregated over a CTE_SCAN root ===
let $QUERY=
WITH p AS (SELECT p_size AS sz, MIN(p_brand) AS mn, MAX(p_brand) AS mx, COUNT(*) AS n
           FROM part GROUP BY p_size)
SELECT MIN(p.mn), MAX(p.mx), SUM(p.n) FROM p;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH p AS (SELECT p_size AS sz, MIN(p_brand) AS mn, MAX(p_brand) AS mx, COUNT(*) AS n
           FROM part GROUP BY p_size)
SELECT MIN(p.mn), MAX(p.mx), SUM(p.n) FROM p|

--echo
--echo === agg-15: VARCHAR(40) p_name MIN/MAX re-aggregated over a CTE_SCAN root ===
let $QUERY=
WITH p AS (SELECT p_brand AS b, MIN(p_name) AS mn, MAX(p_name) AS mx, COUNT(*) AS n
           FROM part GROUP BY p_brand)
SELECT MIN(p.mn), MAX(p.mx), SUM(p.n) FROM p;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH p AS (SELECT p_brand AS b, MIN(p_name) AS mn, MAX(p_name) AS mx, COUNT(*) AS n
           FROM part GROUP BY p_brand)
SELECT MIN(p.mn), MAX(p.mx), SUM(p.n) FROM p|

# =====================================================================
# GROUP BY shapes — single column (covered above) and multiple columns.
# =====================================================================

--echo
--echo === agg-16: CTE body GROUP BY two columns (o_orderstatus, o_shippriority), scalar main agg ===
let $QUERY=
WITH ord AS (
  SELECT o_orderstatus AS st, o_shippriority AS pr,
         COUNT(*) AS n, SUM(o_shippriority) AS sp
  FROM orders GROUP BY o_orderstatus, o_shippriority)
SELECT SUM(ord.n), SUM(ord.sp), MIN(ord.pr), MAX(ord.pr), COUNT(*)
FROM ord;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH ord AS (
  SELECT o_orderstatus AS st, o_shippriority AS pr,
         COUNT(*) AS n, SUM(o_shippriority) AS sp
  FROM orders GROUP BY o_orderstatus, o_shippriority)
SELECT SUM(ord.n), SUM(ord.sp), MIN(ord.pr), MAX(ord.pr), COUNT(*)
FROM ord|

--echo
--echo === agg-17: CTE body GROUP BY two columns, main re-aggregates by first key ===
let $QUERY=
WITH ord AS (
  SELECT o_orderstatus AS st, o_shippriority AS pr, SUM(o_shippriority) AS sp
  FROM orders GROUP BY o_orderstatus, o_shippriority)
SELECT ord.st, SUM(ord.sp), MIN(ord.sp), MAX(ord.sp), COUNT(*)
FROM ord
GROUP BY ord.st;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH ord AS (
  SELECT o_orderstatus AS st, o_shippriority AS pr, SUM(o_shippriority) AS sp
  FROM orders GROUP BY o_orderstatus, o_shippriority)
SELECT ord.st, SUM(ord.sp), MIN(ord.sp), MAX(ord.sp), COUNT(*)
FROM ord
GROUP BY ord.st|

# =====================================================================
# Group B — CTE_SCAN root: WITH x AS (...agg...) and the main query
# aggregates over x directly (no real-table join).
# =====================================================================

--echo
--echo === agg-18: CTE_SCAN root, scalar main aggregation over CTE output ===
let $QUERY=
WITH x AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT COUNT(*), SUM(x.t), MIN(x.t), MAX(x.t)
FROM x;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH x AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT COUNT(*), SUM(x.t), MIN(x.t), MAX(x.t)
FROM x|

# agg-19/20 (was D6, FIXED): scalar main aggregation over a HIGH-CARDINALITY
# lineitem CTE_SCAN root (keyed by l_orderkey/1500 and l_partkey).  Crashed
# (DBSPJ Error 2343, m_cnt_active==0) before the CTE multi-batch completion fix;
# now green.  (The high-cardinality CTE_SCAN-root STRING case — formerly agg-21
# — is covered by agg-13 above.)
--echo
--echo === agg-19: high-cardinality lineitem CTE_SCAN root (l_orderkey), scalar main agg ===
let $QUERY=
WITH li AS (SELECT l_orderkey AS k, SUM(l_quantity) AS sq FROM lineitem GROUP BY l_orderkey)
SELECT COUNT(*), SUM(li.sq), MIN(li.sq), MAX(li.sq) FROM li;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH li AS (SELECT l_orderkey AS k, SUM(l_quantity) AS sq FROM lineitem GROUP BY l_orderkey)
SELECT COUNT(*), SUM(li.sq), MIN(li.sq), MAX(li.sq) FROM li|

--echo
--echo === agg-20: high-cardinality lineitem CTE_SCAN root (l_partkey), scalar main agg ===
let $QUERY=
WITH li AS (SELECT l_partkey AS k, SUM(l_quantity) AS sq, MIN(l_quantity) AS mn, MAX(l_quantity) AS mx
            FROM lineitem GROUP BY l_partkey)
SELECT COUNT(*), SUM(li.sq), MIN(li.mn), MAX(li.mx) FROM li;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH li AS (SELECT l_partkey AS k, SUM(l_quantity) AS sq, MIN(l_quantity) AS mn, MAX(l_quantity) AS mx
            FROM lineitem GROUP BY l_partkey)
SELECT COUNT(*), SUM(li.sq), MIN(li.mn), MAX(li.mx) FROM li|
# NEXT-PHASE (DISABLED — HANG, D3): projection-only main SELECT over a CTE_LOOKUP
# child (selecting CTE columns without wrapping them in a main aggregate) HANGS
# RonSQL/RDRS (never returns).  Same class as the COUNT(col) hang (D2).  Every
# type-coverage case above was converted to an AGGREGATING main; this one probe
# documents the gap.  See findings/_discovery_log.md (D3).
#   WITH nat AS (SELECT n_regionkey AS rk, MIN(n_nationkey) AS mn, MAX(n_nationkey) AS mx, COUNT(*) AS n
#                FROM nation GROUP BY n_regionkey)
#   SELECT nat.rk, nat.mn, nat.mx, nat.n FROM region AS r JOIN nat ON nat.rk = r.r_regionkey;

# NEXT-PHASE (DISABLED — ERROR, D1): SUM over a DECIMAL column in a CTE body —
# "SUM over this column type in CTE not yet supported" (RonSQLPreparer.cpp:7090;
# SUM supports int + FLOAT/DOUBLE, not DECIMAL).  MAIN cases SUM int columns and
# MIN/MAX (not SUM) the decimal columns instead.  See findings/_discovery_log.md (D1).
#   WITH x AS (SELECT o_custkey AS k, SUM(o_totalprice) AS t FROM orders GROUP BY o_custkey)
#   SELECT SUM(x.t) FROM customer AS c JOIN x ON x.k = c.c_custkey;

# NEXT-PHASE: AVG aggregate in CTE body — matrix "NOT SUPPORTED / DEFERRED" (AVG).
#   WITH x AS (SELECT o_custkey AS k, AVG(o_totalprice) AS a
#              FROM orders GROUP BY o_custkey)
#   SELECT x.k, x.a FROM customer AS c JOIN x ON x.k = c.c_custkey;

# NEXT-PHASE: COUNT(DISTINCT col) in CTE body — matrix "NOT SUPPORTED" (COUNT(DISTINCT)).
#   WITH x AS (SELECT o_orderstatus AS st, COUNT(DISTINCT o_custkey) AS d
#              FROM orders GROUP BY o_orderstatus)
#   SELECT x.st, x.d FROM x;

# NEXT-PHASE: MIN/MAX over DATE column (orders.o_orderdate) — matrix UNCERTAIN (DATE MIN/MAX).
#   Orchestrator may enable if the DATE wire format round-trips correctly.
#   WITH x AS (SELECT o_custkey AS k, MIN(o_orderdate) AS mn, MAX(o_orderdate) AS mx
#              FROM orders GROUP BY o_custkey)
#   SELECT MIN(x.mn), MAX(x.mx) FROM customer AS c JOIN x ON x.k = c.c_custkey;

# NEXT-PHASE: GROUP BY an expression — matrix "NOT SUPPORTED" (GROUP BY expression).
#   Only direct columns are allowed in a CTE-body GROUP BY.
#   WITH x AS (SELECT (o_custkey % 10) AS bucket, COUNT(*) AS n
#              FROM orders GROUP BY (o_custkey % 10))
#   SELECT x.bucket, x.n FROM x;

# NEXT-PHASE: post-aggregation expression in SELECT — matrix "NOT SUPPORTED"
#   (SELECT of a post-aggregation expression, e.g. SUM(x)+1).
#   WITH x AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
#              FROM orders GROUP BY o_custkey)
#   SELECT x.k, SUM(x.t) + 1 FROM x GROUP BY x.k;

# NEXT-PHASE: SUM over FLOAT (lineitem.l_discount) — matrix UNCERTAIN (FLOAT SUM
#   may format differently between engines).  MIGHT pass; orchestrator verifies.
#   WITH x AS (SELECT l_orderkey AS k, SUM(l_discount) AS sd
#              FROM lineitem GROUP BY l_orderkey)
#   SELECT SUM(x.sd) FROM x;

# NEXT-PHASE: SUM over DOUBLE (lineitem.l_tax) — matrix UNCERTAIN (DOUBLE SUM
#   may format differently between engines).  MIGHT pass; orchestrator verifies.
#   WITH x AS (SELECT l_orderkey AS k, SUM(l_tax) AS stx
#              FROM lineitem GROUP BY l_orderkey)
#   SELECT SUM(x.stx) FROM x;

DELIMITER ;|

--source suite/ronsql_cte/include/cte_drop.inc
