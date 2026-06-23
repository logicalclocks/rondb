# body_chain_scalar.inc — chain_scalar family, data-driven CTE suite.
#
# FOCUS: chained CTE-of-CTE and scalar (no-GROUP-BY) CTEs at realistic
# scale.  The base CTE `a AS (SELECT o_custkey, SUM(<int>) ... GROUP BY
# o_custkey)` produces 300 groups (>256), so every chained phase that
# rolls up `a` crosses the 256-row API batch boundary and exercises
# multi-batch redistribution.  Scalar CTEs (no GROUP BY) materialise a
# single virtual-table row including for empty input (MAX -> NULL,
# COUNT -> 0).
#
# CORRECTED ENVELOPE (post-discovery): inner/chained CTE aggregates use
# COUNT(*) / SUM(<int>) / MIN / MAX only (no SUM(<decimal>), no
# COUNT(<col>)).  Every chained case's outer/main SELECT AGGREGATES
# (scalar aggregate or GROUP BY + aggregate) — projection-only main
# SELECTs over a CTE_LOOKUP/CTE_SCAN HANG and are never kept enabled.
# A scalar CTE consumed by a SINGLE-table main (project or aggregate its
# scalar output) is GREEN.  Watermark GREATEST/LEAST over two scalar CTEs
# comma cross-joined is now SUPPORTED (D8 FIXED — see cs14/cs15; the scalar
# cross-join dummy key now matches the virt PK column type).  The project-both
# form (SELECT a.m, b.n FROM a, b) is still rejected at the non-aggregate gate
# ("Not an aggregate query") and stays a documented probe (cs-probe-7).
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

# ===========================================================================
# Chained CTE-of-CTE.  Inner SUM is over o_shippriority (int).  Every main
# SELECT aggregates (scalar agg or GROUP BY + agg).
# ===========================================================================

--echo
--echo === cs01: 2-level chained CTE (a -> b), main scalar-aggregates b (300-group base) ===
# a: 300 groups (o_custkey).  b: per-custkey MAX over a (300 groups).  Main
# scalar-aggregates b's 300 rows, reading a multi-batch (>256) intermediate.
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(t) AS m
  FROM a GROUP BY k)
SELECT MAX(m) AS mx, MIN(m) AS mn, SUM(m) AS st, COUNT(*) AS n FROM b;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(t) AS m
  FROM a GROUP BY k)
SELECT MAX(m) AS mx, MIN(m) AS mn, SUM(m) AS st, COUNT(*) AS n FROM b|

--echo
--echo
--echo === cs02: 2-level chained CTE (a -> b), both grouped, main grouped ===
# b re-groups a on the same key, main query rolls up b.  Three GROUP-BY
# phases all at 300 groups.
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(t) AS m
  FROM a GROUP BY k)
SELECT k, SUM(m) FROM b GROUP BY k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(t) AS m
  FROM a GROUP BY k)
SELECT k, SUM(m) FROM b GROUP BY k|

--echo
--echo
--echo === cs03: 3-level chained CTE (a -> b -> c), main scalar-aggregates c ===
# Three chained CTE phases; c re-groups b; main scalar-aggregates c.
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(t) AS m
  FROM a GROUP BY k),
     c AS (
  SELECT k, MIN(m) AS mn
  FROM b GROUP BY k)
SELECT SUM(mn) AS gt, COUNT(*) AS ng FROM c;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(t) AS m
  FROM a GROUP BY k),
     c AS (
  SELECT k, MIN(m) AS mn
  FROM b GROUP BY k)
SELECT SUM(mn) AS gt, COUNT(*) AS ng FROM c|

--echo
--echo
--echo === cs04: 3-level chained CTE (a -> b -> c), all grouped, main grouped ===
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS s
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(s) AS m
  FROM a GROUP BY k),
     c AS (
  SELECT k, MIN(m) AS mn
  FROM b GROUP BY k)
SELECT k, SUM(mn) FROM c GROUP BY k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS s
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(s) AS m
  FROM a GROUP BY k),
     c AS (
  SELECT k, MIN(m) AS mn
  FROM b GROUP BY k)
SELECT k, SUM(mn) FROM c GROUP BY k|

--echo
--echo
--echo === cs05: chained CTE with WHERE filter on the intermediate ===
# b filters a's aggregate output before re-rolling.  SUM(o_shippriority)
# per custkey ranges roughly 0..20; threshold 8 keeps a realistic subset.
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(t) AS m
  FROM a WHERE t > 8 GROUP BY k)
SELECT k, SUM(m) FROM b GROUP BY k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(t) AS m
  FROM a WHERE t > 8 GROUP BY k)
SELECT k, SUM(m) FROM b GROUP BY k|

--echo
--echo
--echo === cs06: chained CTE feeding a real-table join in the outer SELECT ===
# b (chained roll-up of a) joins customer on the custkey; main aggregates.
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS s
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(s) AS m
  FROM a GROUP BY k)
SELECT c.c_nationkey, SUM(b.m)
FROM customer AS c JOIN b ON b.k = c.c_custkey
GROUP BY c.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS s
  FROM orders GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(s) AS m
  FROM a GROUP BY k)
SELECT c.c_nationkey, SUM(b.m)
FROM customer AS c JOIN b ON b.k = c.c_custkey
GROUP BY c.c_nationkey|

# ===========================================================================
# Sibling CTEs (two CTEs in the same WITH, both consumed by the main query)
# ===========================================================================

--echo
--echo
--echo === cs07: sibling CTEs, both joined to a parent table ===
# sums and cnts are independent aggregations over orders, both at 300
# groups; the main query joins customer to both and re-aggregates.
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS s
  FROM orders GROUP BY o_custkey),
     cnts AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_nationkey, SUM(sums.s), SUM(cnts.n)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
JOIN cnts ON cnts.k = c.c_custkey
GROUP BY c.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS s
  FROM orders GROUP BY o_custkey),
     cnts AS (
  SELECT o_custkey AS k, COUNT(*) AS n
  FROM orders GROUP BY o_custkey)
SELECT c.c_nationkey, SUM(sums.s), SUM(cnts.n)
FROM customer AS c
JOIN sums ON sums.k = c.c_custkey
JOIN cnts ON cnts.k = c.c_custkey
GROUP BY c.c_nationkey|

# cs08: sibling scalar CTEs comma cross-joined, PROJECTING BOTH outputs
# (SELECT a.m, b.n FROM a, b) — still DISABLED.  This is the projection-only
# form; it is rejected at the non-aggregate front-end gate ("Not an aggregate
# query") because a comma cross-join of two scalar CTEs is not (yet) a
# recognized projection-only shape.  Distinct from the watermark form (cs14/
# cs15, now GREEN): those wrap the two outputs in GREATEST/LEAST, so the
# implicit-MAX makes them aggregate queries that pass the gate.  See
# cs-probe-7.  Single-table scalar-CTE main is GREEN (cs09-cs13).

# ===========================================================================
# Scalar CTEs (no GROUP BY) — single virtual-table row, SINGLE-table main
# ===========================================================================

--echo
--echo
# cs09 (was D15, FIXED): pass-through of a scalar-CTE DECIMAL MAX.  RonSQL now
# formats the DOUBLE-widened DECIMAL with the source scale (20055.00) like MySQL.
--echo
--echo === cs09: scalar CTE MAX(o_totalprice) projected directly (pass-through) ===
let $QUERY=
WITH x AS (SELECT MAX(o_totalprice) AS m FROM orders) SELECT m FROM x;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH x AS (SELECT MAX(o_totalprice) AS m FROM orders) SELECT m FROM x|

--echo
--echo
--echo === cs10: scalar CTE COUNT/SUM/MIN variants over a populated table ===
let $QUERY=
WITH x AS (SELECT COUNT(*) AS n, SUM(o_shippriority) AS s, MIN(o_totalprice) AS lo
           FROM orders)
SELECT n, s, lo FROM x;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH x AS (SELECT COUNT(*) AS n, SUM(o_shippriority) AS s, MIN(o_totalprice) AS lo
           FROM orders)
SELECT n, s, lo FROM x|

--echo
--echo
# cs11 (was D15, FIXED): pass-through of mixed scalar-CTE aggregates — DECIMAL
# MIN/MAX (now scale-formatted) alongside an integer SUM and COUNT.
--echo
--echo === cs11: scalar CTE multiple aggregates projected together (pass-through) ===
let $QUERY=
WITH x AS (SELECT MIN(o_totalprice) AS lo, MAX(o_totalprice) AS hi,
                  SUM(o_shippriority) AS total, COUNT(*) AS n
           FROM orders)
SELECT lo, hi, total, n FROM x;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH x AS (SELECT MIN(o_totalprice) AS lo, MAX(o_totalprice) AS hi,
                  SUM(o_shippriority) AS total, COUNT(*) AS n
           FROM orders)
SELECT lo, hi, total, n FROM x|

--echo
--echo
--echo === cs12: scalar CTE MAX over EMPTY input — returns NULL ===
# o_custkey ranges 1..300; 999999 matches no rows.  MySQL scalar
# aggregate semantics: MAX over empty input is NULL, one row emitted.
let $QUERY=
WITH x AS (SELECT MAX(o_totalprice) AS m FROM orders WHERE o_custkey = 999999)
SELECT m FROM x;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH x AS (SELECT MAX(o_totalprice) AS m FROM orders WHERE o_custkey = 999999)
SELECT m FROM x|

--echo
--echo
--echo === cs13: scalar CTE COUNT(*) over EMPTY input — returns 0 ===
# COUNT(*) is the canonical "always emit a row" aggregate; the scalar
# path must surface 0, not zero rows.
let $QUERY=
WITH x AS (SELECT COUNT(*) AS n FROM orders WHERE o_custkey = 999999)
SELECT n FROM x;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH x AS (SELECT COUNT(*) AS n FROM orders WHERE o_custkey = 999999)
SELECT n FROM x|

# cs14 / cs15 (D8 verify): watermark GREATEST / LEAST over two scalar CTE
# outputs, comma cross-joined.  Recorded as a "Failed to create child
# operation" error during the original parallel discovery; Phase I.17 since
# added the scalar comma cross-join (Test 20 lookupCte dummy-key + setParent),
# so re-verify.  Top-level GREATEST/LEAST is wrapped in an implicit MAX, so
# these are aggregate queries.
--echo
--echo === cs14 (D8): watermark GREATEST over two scalar CTE outputs (comma cross-join) ===
let $QUERY=
WITH a AS (SELECT MAX(o_totalprice) AS m FROM orders),
     b AS (SELECT MIN(o_totalprice) AS n FROM orders)
SELECT GREATEST(a.m, b.n) AS biggest FROM a, b;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (SELECT MAX(o_totalprice) AS m FROM orders),
     b AS (SELECT MIN(o_totalprice) AS n FROM orders)
SELECT GREATEST(a.m, b.n) AS biggest FROM a, b|

--echo
--echo === cs15 (D8): watermark LEAST over two scalar CTE outputs (comma cross-join) ===
let $QUERY=
WITH a AS (SELECT MAX(o_totalprice) AS m FROM orders),
     b AS (SELECT MIN(o_totalprice) AS n FROM orders)
SELECT LEAST(a.m, b.n) AS smallest FROM a, b;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (SELECT MAX(o_totalprice) AS m FROM orders),
     b AS (SELECT MIN(o_totalprice) AS n FROM orders)
SELECT LEAST(a.m, b.n) AS smallest FROM a, b|

# ===========================================================================
# Main aggregation over a CTE_SCAN root with >256 groups (multi-batch)
# ===========================================================================

--echo
--echo
--echo === cs16: main GROUP BY on a CTE_SCAN root col + agg (300 groups) ===
# CTE has 300 groups; main query re-aggregates over the CTE_SCAN root.
# The CTE-scan-to-main-agg path crosses the 256-row batch boundary.
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT k, SUM(t) FROM sums GROUP BY k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT k, SUM(t) FROM sums GROUP BY k|

--echo
--echo
--echo === cs17: main scalar agg over a CTE_SCAN root (300 groups -> 1 row) ===
let $QUERY=
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT SUM(t) AS gt, COUNT(*) AS ng FROM sums;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS t
  FROM orders GROUP BY o_custkey)
SELECT SUM(t) AS gt, COUNT(*) AS ng FROM sums|

# ===========================================================================
# Skewed (single-group) CTE and empty-intermediate chain
# ===========================================================================

--echo
--echo
--echo === cs18: skewed CTE — WHERE narrows to a single group ===
# WHERE o_custkey = 100 narrows the CTE to one group.  After hash
# redistribution that group lives on one node; other nodes see an empty
# gb_map.  Main query joins customer and rolls up.
let $QUERY=
WITH skew AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS s
  FROM orders WHERE o_custkey = 100 GROUP BY o_custkey)
SELECT c.c_nationkey, SUM(skew.s)
FROM customer AS c JOIN skew ON skew.k = c.c_custkey
GROUP BY c.c_nationkey;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH skew AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS s
  FROM orders WHERE o_custkey = 100 GROUP BY o_custkey)
SELECT c.c_nationkey, SUM(skew.s)
FROM customer AS c JOIN skew ON skew.k = c.c_custkey
GROUP BY c.c_nationkey|

--echo
--echo
--echo === cs19: empty-intermediate chain (a always empty -> b empty) ===
# o_totalprice is always positive, so a is always empty; b (rolling up a)
# is therefore empty too.  Both chained phases hit the empty-gb_map fast
# path on every node.  Main groups + aggregates b.
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS s
  FROM orders WHERE o_totalprice < 0 GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(s) AS m
  FROM a GROUP BY k)
SELECT k, SUM(m) FROM b GROUP BY k;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS s
  FROM orders WHERE o_totalprice < 0 GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(s) AS m
  FROM a GROUP BY k)
SELECT k, SUM(m) FROM b GROUP BY k|

--echo
--echo
--echo
--echo
--echo === cs20: scalar agg over an EMPTY chained CTE — SUM=NULL, COUNT(*)=0 (D16) ===
# a is always empty (o_totalprice < 0 never matches), b re-groups a (also
# empty), and the main query SCALAR-aggregates b's empty output.  MySQL emits
# exactly one row: SUM(m)=NULL, COUNT(*)=0.  (Contrast cs19 above: same empty
# chain but a GROUP BY main query -> zero rows, which already matches.)
let $QUERY=
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS s
  FROM orders WHERE o_totalprice < 0 GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(s) AS m
  FROM a GROUP BY k)
SELECT SUM(m) AS gt, COUNT(*) AS ng FROM b;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH a AS (
  SELECT o_custkey AS k, SUM(o_shippriority) AS s
  FROM orders WHERE o_totalprice < 0 GROUP BY o_custkey),
     b AS (
  SELECT k, MAX(s) AS m
  FROM a GROUP BY k)
SELECT SUM(m) AS gt, COUNT(*) AS ng FROM b|

# ===========================================================================
# PROBES — recorded, disabled (genuine gaps / uncertain) or rejection-asserts
# ===========================================================================

# cs-probe-1: nullable scalar output feeding a top-level GREATEST.
# o_clerk is NULL when o_orderkey % 7 == 0; o_orderkey = 7 selects exactly
# one row whose o_clerk is NULL, so MAX(o_clerk) over that subset is NULL.
# Feeding an all-NULL scalar output into top-level GREATEST exercises the
# I.17/I.21 nullable-first-output path.  Also a scalar comma cross-join
# (two scalar CTEs), which independently hits the D8 gap.  Disabled.
# NEXT-PHASE: nullable scalar CTE output feeding top-level GREATEST — I.21
#   nullable-first-output / GREATEST NULL semantics + D8 scalar cross-join.
#   WITH a AS (SELECT MAX(o_clerk) AS m FROM orders WHERE o_orderkey = 7),
#        b AS (SELECT MIN(o_clerk) AS n FROM orders WHERE o_orderkey = 7)
#   SELECT GREATEST(a.m, b.n) AS g FROM a, b;

# cs-probe-2: chained CTE feeding a multi-key CTE complete-key lookup.
# A scalar/grouped chained CTE consumed by a multi-key-PK CTE lookup is
# an unusual topology; partial vs complete key coverage interacts with the
# chained materialisation order.  Suspected unsupported (chained CTE output
# is not a real keyed table, so binding it as a multi-key lookup parent is
# uncertain).  Disabled.
# NEXT-PHASE: chained CTE feeding a multi-key CTE complete-key lookup —
#   I.16/I.20 multi-key coverage over a chained intermediate, uncertain.
#   WITH a AS (SELECT o_custkey AS k, o_orderstatus AS st, SUM(o_shippriority) AS t
#              FROM orders GROUP BY o_custkey, o_orderstatus),
#        b AS (SELECT k, st, MAX(t) AS m FROM a GROUP BY k, st)
#   SELECT b.k, b.st, b.m
#   FROM customer AS c JOIN b ON b.k = c.c_custkey;

# cs-probe-3: post-aggregation expression on a scalar CTE output.
# SELECT of an arithmetic expression over a CTE aggregate (SUM(t)+1) is in
# the NOT-SUPPORTED/DEFERRED list (post-aggregation expression).  Disabled.
# NEXT-PHASE: post-aggregation expression in main SELECT over a CTE aggregate
#   — DEFERRED (SELECT of a post-aggregation expression), matrix.
#   WITH a AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t
#              FROM orders GROUP BY o_custkey)
#   SELECT k, SUM(t) + 1 FROM a GROUP BY k;

# cs-probe-4: AVG in a scalar CTE body.
# AVG is in the NOT-SUPPORTED/DEFERRED list.  Disabled.
# NEXT-PHASE: AVG in a (scalar) CTE body — DEFERRED (AVG), matrix.
#   WITH x AS (SELECT AVG(o_totalprice) AS a FROM orders)
#   SELECT a FROM x;

# cs-probe-7: scalar-CTE comma cross-join PROJECTING BOTH outputs (cs08).
# Still disabled — rejected at the non-aggregate front-end gate ("Not an
# aggregate query").  The watermark forms (GREATEST/LEAST) are now GREEN
# (cs14/cs15); only the projection-only form needs the gate extended to treat
# a scalar-CTE comma cross-join as a supported projection-only shape.
# NEXT-PHASE: scalar-CTE comma cross-join, project both outputs — non-aggregate
#   gate does not yet accept a scalar-CTE cross-join projection.
#   WITH a AS (SELECT MAX(o_totalprice) AS m FROM orders),
#        b AS (SELECT MIN(o_totalprice) AS n FROM orders)
#   SELECT a.m, b.n FROM a, b;

# cs-probe-8 / cs-probe-9: watermark GREATEST / LEAST over two scalar CTE
# outputs are now ENABLED as cs14 / cs15 (D8 FIXED — the scalar cross-join
# dummy key matches the virt PK column type, and the result carries the source
# DECIMAL scale).

# cs-probe-5 (rejection-assert): scalar CTE projecting a non-aggregate
# column with no GROUP BY.  RonSQL does not pass through CTE bodies, so a
# non-aggregate projection without GROUP BY is rejected at prepare time
# (clean permanent rejection, see ronsql_cte_scalar.test Test 5).
--echo
--echo
--echo === cs-probe-5 (rejection): non-aggregate col in a scalar CTE body ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH x AS (SELECT o_totalprice, MAX(o_totalprice) AS m FROM orders) SELECT o_totalprice, m FROM x;' 2>&1

# cs-probe-6 (rejection-assert): top-level GREATEST over an ordinary table
# column (not a scalar CTE output).  The implicit top-level GREATEST/LEAST
# aggregate wrapper is valid only for scalar CTE output columns; ordinary
# columns need an explicit aggregate (see ronsql_cte_scalar.test Test 14).
--echo
--echo
--echo === cs-probe-6 (rejection): top-level GREATEST over an ordinary column ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'SELECT GREATEST(o_totalprice, 1) AS g FROM orders;' 2>&1

DELIMITER ;|

--source suite/ronsql_cte/include/cte_drop.inc
