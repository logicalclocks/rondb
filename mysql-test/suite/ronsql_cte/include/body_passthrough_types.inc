# body_passthrough_types.inc — pass-through (projection-only) result
# type coverage family (non_aggregate_phase_0.md, Phase 0b), data-driven
# CTE suite.
#
# Before Phase 0b, print_passthrough_value rejected DATE / YEAR /
# DATETIME / TIME / TIMESTAMP / DECIMAL projections with a hard error,
# and CTE MIN/MAX outputs over temporal columns (delivered as Bigunsigned
# virt columns carrying the packed value) printed as raw integers —
# silently different from MySQL.  These cases lock in the fixed
# behavior: raw temporal / DECIMAL columns decode through the shared
# packed-temporal decoder and decimal_bin2str, CTE packed temporal
# outputs decode via the resolved source column's metadata, and
# unsupported types (BIT, VARBINARY) reject with a clean error naming
# the type.
#
# Projection half of the agg-d17a..d17j MIN/MAX-over-temporal matrix in
# body_agg.inc.  TEXT/TSV comparison only (the harness posts
# outputFormat TEXT); JSON quoting of temporals is exercised at the unit
# level, not here.
#
# Case map (pt-N):
#   1  DATE + DECIMAL(12,2) real columns through an I.8 chain
#   2  YEAR / DATETIME / DATETIME(6) / TIME / TIME(3) / TIMESTAMP /
#      TIMESTAMP(6) real columns through an I.8 chain
#   3  E.3 CTE_SCAN root: GROUP BY DATE column projected raw
#   4  CTE MIN/MAX packed temporal outputs (DATETIME, TIMESTAMP(6),
#      YEAR) through an I.8 chain — pre-0b printed raw integers
#   5  CTE MIN/MAX over DATE + DECIMAL scale-0/2 real columns
#   6  LEFT JOIN NULL substitution over temporal / DECIMAL outputs
#   P1 rejection: BIT projection (clean error, local table)
#   P2 rejection: VARBINARY projection (clean error, local table)

--source include/have_ndb.inc
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings
--let $suppress_ronsql_cli=yes
--let $strict_diff=yes
--source suite/ronsql_cte/include/cte_schema.inc
--source suite/ronsql_cte/include/cte_data.inc

# Family-local table for the BIT / VARBINARY rejection cases (the shared
# schema deliberately has neither type).
--disable_query_log
--disable_warnings
CREATE TABLE bin1 (
  b_id   INT NOT NULL,
  b_grp  INT NOT NULL,
  b_bits BIT(8),
  b_raw  VARBINARY(16),
  PRIMARY KEY (b_id)
) ENGINE=NDB;
INSERT INTO bin1 VALUES (1, 1, b'1010', 0x4142), (2, 1, b'0101', 0x4344);
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === pt-1: DATE + DECIMAL(12,2) real columns through an I.8 chain ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT o.o_orderkey, o.o_orderdate, o.o_totalprice, cf.n
FROM orders AS o JOIN cf ON cf.k = o.o_custkey
WHERE o.o_orderkey < 20;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n
            FROM orders GROUP BY o_custkey)
SELECT o.o_orderkey, o.o_orderdate, o.o_totalprice, cf.n
FROM orders AS o JOIN cf ON cf.k = o.o_custkey
WHERE o.o_orderkey < 20|

--echo
--echo === pt-2: YEAR / DATETIME / DATETIME(6) / TIME / TIME(3) / TIMESTAMP / TIMESTAMP(6) ===
let $QUERY=
WITH eg AS (SELECT e_group AS g, COUNT(*) AS n
            FROM evlog GROUP BY e_group)
SELECT e.e_id, e.e_year, e.e_modified, e.e_logged, e.e_elapsed,
       e.e_split, e.e_stamp, e.e_stamp6, eg.n
FROM evlog AS e JOIN eg ON eg.g = e.e_group
WHERE e.e_id < 10;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH eg AS (SELECT e_group AS g, COUNT(*) AS n
            FROM evlog GROUP BY e_group)
SELECT e.e_id, e.e_year, e.e_modified, e.e_logged, e.e_elapsed,
       e.e_split, e.e_stamp, e.e_stamp6, eg.n
FROM evlog AS e JOIN eg ON eg.g = e.e_group
WHERE e.e_id < 10|

--echo
--echo === pt-3: E.3 CTE_SCAN root, GROUP BY DATE column projected raw ===
let $QUERY=
WITH od AS (SELECT o_orderdate AS d, COUNT(*) AS n
            FROM orders GROUP BY o_orderdate)
SELECT od.d, od.n FROM od;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH od AS (SELECT o_orderdate AS d, COUNT(*) AS n
            FROM orders GROUP BY o_orderdate)
SELECT od.d, od.n FROM od|

--echo
--echo === pt-4: CTE MIN/MAX packed temporal outputs (pre-0b: raw integers) ===
let $QUERY=
WITH ev AS (SELECT e_group AS g, MIN(e_modified) AS mn,
                   MAX(e_stamp6) AS mx, MIN(e_year) AS my
            FROM evlog GROUP BY e_group)
SELECT e.e_id, ev.mn, ev.mx, ev.my
FROM evlog AS e JOIN ev ON ev.g = e.e_group
WHERE e.e_id < 5;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH ev AS (SELECT e_group AS g, MIN(e_modified) AS mn,
                   MAX(e_stamp6) AS mx, MIN(e_year) AS my
            FROM evlog GROUP BY e_group)
SELECT e.e_id, ev.mn, ev.mx, ev.my
FROM evlog AS e JOIN ev ON ev.g = e.e_group
WHERE e.e_id < 5|

--echo
--echo === pt-5: CTE MIN/MAX over DATE + DECIMAL real columns ===
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, MIN(o_orderdate) AS first_d,
                   MAX(o_orderdate) AS last_d
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey, cu.c_acctbal, cf.first_d, cf.last_d
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey < 8;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, MIN(o_orderdate) AS first_d,
                   MAX(o_orderdate) AS last_d
            FROM orders GROUP BY o_custkey)
SELECT cu.c_custkey, cu.c_acctbal, cf.first_d, cf.last_d
FROM customer AS cu JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey < 8|

--echo
--echo === pt-6: LEFT JOIN NULL substitution over temporal / DECIMAL outputs ===
# The CTE only covers customers appearing in the first 50 orders, so
# some LEFT JOIN parents are unmatched: their cf.d / cf.n must print as
# NULL (isRowNULL substitution), never as a decode of stale bytes.
let $QUERY=
WITH cf AS (SELECT o_custkey AS k, MIN(o_orderdate) AS d, COUNT(*) AS n
            FROM orders WHERE o_orderkey < 50 GROUP BY o_custkey)
SELECT cu.c_custkey, cu.c_acctbal, cf.d, cf.n
FROM customer AS cu LEFT JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey < 15;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH cf AS (SELECT o_custkey AS k, MIN(o_orderdate) AS d, COUNT(*) AS n
            FROM orders WHERE o_orderkey < 50 GROUP BY o_custkey)
SELECT cu.c_custkey, cu.c_acctbal, cf.d, cf.n
FROM customer AS cu LEFT JOIN cf ON cf.k = cu.c_custkey
WHERE cu.c_custkey < 15|

DELIMITER ;|

--echo
--echo === pt-P1 (rejection): BIT projection in a pass-through result ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH cb AS (SELECT b_grp AS g, COUNT(*) AS n FROM bin1 GROUP BY b_grp) SELECT b.b_bits, cb.n FROM bin1 AS b JOIN cb ON cb.g = b.b_grp;' 2>&1

--echo
--echo === pt-P2 (rejection): VARBINARY projection in a pass-through result ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH cb AS (SELECT b_grp AS g, COUNT(*) AS n FROM bin1 GROUP BY b_grp) SELECT b.b_raw, cb.n FROM bin1 AS b JOIN cb ON cb.g = b.b_grp;' 2>&1

--disable_query_log
--disable_warnings
DROP TABLE bin1;
--enable_warnings
--enable_query_log
--source suite/ronsql_cte/include/cte_drop.inc
