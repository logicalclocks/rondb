# body_passthrough_single_table.inc — single-table non-aggregate
# (projection-only) query family (non_aggregate_phase_1.md), data-driven
# suite.
#
# Phase 1 accepts the first queries with no aggregation and no CTE:
# projection-only single-table SELECTs.  Execution is the plain NDB API
# path (no NdbQueryBuilder / SPJ hop): a single-row PK lookup when the
# WHERE covers the full primary key with equalities, else the shared
# table/index scan setup with bounds + residual filter and a streaming
# drain through the pass-through printer.
#
# PK+residual follow-up (non_aggregate_pk_residual_lookup.md): residual
# conjuncts beside a full PK equality cover no longer force the scan
# fallback — they ride the lookup as an NdbRecord OO_INTERPRETED filter
# program (NdbScanFilter's reject path is error 626 = NoDataFound, so a
# fetched-but-rejected row and a missing row both yield an empty
# result).  The scan fallback remains for residual programs over the
# 64-word lookup cap or using types the filter emit does not support.
#
# Case map (st-N):
#   1   PK hit, mixed-type projection (INT, DATE, DECIMAL, CHAR)
#   2   PK miss -> empty output (deferred TSV header)
#   3   composite PK full equality, predicates in reversed PK order
#   4   PK equality + accepting residual -> PK lookup + filter
#   5   PK equality + rejecting residual -> empty (626 reject path)
#   6   partial composite PK -> scan path
#   7   secondary-index equality
#   8   half-open DATE range + residual filter (QUERY_FILE)
#   9   double-bounded DATE range (QUERY_FILE)
#   10  composite index prefix: status equality + date bound (QUERY_FILE)
#   11  residual-only WHERE, no usable index -> table scan
#   12  no WHERE -> full table scan (nation, 25 rows)
#   13  temporal projection sweep (evlog)
#   14  NULL values in a projected column (o_clerk)
#   15  PK equality + multi-conjunct accepting residual
#   16  OR / IS NULL nesting in the residual program
#   17  composite hash-PK full equality + residual
#   18a duplicate PK equality -> lookup (second equality is a residual)
#   18b contradictory PK equality -> lookup + rejecting filter -> empty
#   19  over-cap residual (long OR chain) -> scan fallback pinned
#   20  unsupported-type residual (TIME(3)) -> plan-time scan fallback,
#       execution still fails like the scan path (QUERY_FILE)
#   21  local VARCHAR-PK table, plain lookup (QUERY_FILE)
#   22  local VARCHAR-PK table + residual — pins the NdbRecord key_row
#       length-prefix encoding, the one new byte-level path (QUERY_FILE)
#   23  PROBE: WHERE IN-subquery on a single-table projection
#   24  PROBE: scalar subquery constant on a single-table projection
#   (P1/P2 ORDER-BY-rejection probes retired — ORDER BY / LIMIT shipped
#    via ronsql_orderby_limit_plan.md Phases 2+3; see
#    body_passthrough_limit.inc / body_passthrough_orderby.inc)
#   P3  rejection: expression in the projection
#   P4  rejection: WITH prefix on a single-table projection
#
# st-23/24 are the (former NEXT-PHASE) subquery probes: the subquery
# substitution machinery predates shape C and had never run with a
# non-aggregate outer query — the first --record decides their fate
# (green => pinned coverage; clean reject => convert to P-cases; wrong
# results => real finding, convert to rejection-assert + NEXT-PHASE).

--source include/have_ndb.inc
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings
--let $suppress_ronsql_cli=yes
--let $strict_diff=yes
--source suite/ronsql_cte/include/cte_schema.inc
--source suite/ronsql_cte/include/cte_data.inc

# Local table for the VARCHAR-PK lookup cases (st-21/st-22): the shared
# schema has only integer PKs, and the NdbRecord key_row encoding
# (length-prefix layout) is the genuinely new byte-level path of the
# PK+residual lookup.
--disable_query_log
--disable_warnings
CREATE TABLE vkey1 (
  v_code VARCHAR(20) NOT NULL,
  v_num  INT NOT NULL,
  v_note CHAR(8) NOT NULL,
  PRIMARY KEY (v_code)
) ENGINE=NDB;
INSERT INTO vkey1 VALUES
  ('alpha', 1, 'aa'), ('beta', 2, 'bb'), ('gamma-longer-key', 3, 'cc'),
  ('delta', 4, 'dd');
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === st-1: PK hit, mixed-type projection ===
let $QUERY=
SELECT o_orderkey, o_orderdate, o_totalprice, o_orderstatus
FROM orders WHERE o_orderkey = 77;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_1.out
let $QUERY=
SELECT o_orderkey, o_orderdate, o_totalprice, o_orderstatus
FROM orders WHERE o_orderkey = 77;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as primary key lookup.' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_orderdate, o_totalprice, o_orderstatus
FROM orders WHERE o_orderkey = 77|

--echo
--echo === st-2: PK miss, empty output ===
let $QUERY=
SELECT o_orderkey, o_orderdate FROM orders WHERE o_orderkey = 999999;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_orderdate FROM orders WHERE o_orderkey = 999999|

--echo
--echo === st-3: composite PK, predicates in reversed PK order ===
let $QUERY=
SELECT l_orderkey, l_linenumber, l_quantity, l_extendedprice
FROM lineitem WHERE l_linenumber = 2 AND l_orderkey = 10;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT l_orderkey, l_linenumber, l_quantity, l_extendedprice
FROM lineitem WHERE l_linenumber = 2 AND l_orderkey = 10|

--echo
--echo === st-4: PK equality + accepting residual (PK lookup + filter) ===
# Fatal EXPLAIN greps are the flip detector: the pre-follow-up scan
# fallback already pushed the whole WHERE server-side, so rows= was 1
# both before and after — the rows pin guards filter loss, the greps
# pin the access path.
let $QUERY=
SELECT o_orderkey, o_custkey FROM orders
WHERE o_orderkey = 77 AND o_shippriority >= 0;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_4.out
let $QUERY=
SELECT o_orderkey, o_custkey FROM orders
WHERE o_orderkey = 77 AND o_shippriority >= 0;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as primary key lookup.' $EXPLAIN_FILE
--exec grep -qF 'CONDITIONS (1 key and 1 filter):' $EXPLAIN_FILE
--exec grep -qF 'FILTER: ' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=1
let $QUERY=
SELECT o_orderkey, o_custkey FROM orders
WHERE o_orderkey = 77 AND o_shippriority >= 0;|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_custkey FROM orders
WHERE o_orderkey = 77 AND o_shippriority >= 0|

--echo
--echo === st-5: PK equality + rejecting residual -> empty ===
# Pins the 626 reject path: the interpreted filter refuses the fetched
# row, surfacing as NoDataFound == row-absent, so the output is empty
# (no TSV header, no error).
let $QUERY=
SELECT o_orderkey, o_custkey FROM orders
WHERE o_orderkey = 77 AND o_custkey < 0;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_5.out
let $QUERY=
SELECT o_orderkey, o_custkey FROM orders
WHERE o_orderkey = 77 AND o_custkey < 0;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as primary key lookup.' $EXPLAIN_FILE
--exec grep -qF 'FILTER: ' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=0
let $QUERY=
SELECT o_orderkey, o_custkey FROM orders
WHERE o_orderkey = 77 AND o_custkey < 0;|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_custkey FROM orders
WHERE o_orderkey = 77 AND o_custkey < 0|

--echo
--echo === st-6: partial composite PK -> scan path ===
let $QUERY=
SELECT l_orderkey, l_linenumber, l_quantity
FROM lineitem WHERE l_orderkey = 10;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT l_orderkey, l_linenumber, l_quantity
FROM lineitem WHERE l_orderkey = 10|

--echo
--echo === st-7: secondary-index equality ===
let $QUERY=
SELECT o_orderkey, o_custkey FROM orders WHERE o_custkey = 7;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_custkey FROM orders WHERE o_custkey = 7|

--echo
--echo === st-8: half-open DATE range + residual filter ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_st8.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate, o_orderstatus
FROM orders
WHERE o_orderdate >= '1997-06-01' AND o_shippriority >= 0;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_8.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_st8b.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate, o_orderstatus
FROM orders
WHERE o_orderdate >= '1997-06-01' AND o_shippriority >= 0;
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as index scan.' $EXPLAIN_FILE || true
--exec grep -qF 'FILTER: ' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_orderdate, o_orderstatus
FROM orders
WHERE o_orderdate >= '1997-06-01' AND o_shippriority >= 0|

--echo
--echo === st-9: double-bounded DATE range ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_st9.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate
FROM orders
WHERE o_orderdate >= '1997-06-01' AND o_orderdate < '1997-08-01';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_orderdate
FROM orders
WHERE o_orderdate >= '1997-06-01' AND o_orderdate < '1997-08-01'|

--echo
--echo === st-10: composite index prefix (status equality + date bound) ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_st10.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderstatus, o_orderdate
FROM orders
WHERE o_orderstatus = 'O' AND o_orderdate >= '1997-06-01';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_orderstatus, o_orderdate
FROM orders
WHERE o_orderstatus = 'O' AND o_orderdate >= '1997-06-01'|

--echo
--echo === st-11: residual-only WHERE -> table scan ===
let $QUERY=
SELECT o_orderkey, o_totalprice FROM orders
WHERE o_orderkey < 15 AND o_shippriority >= 0;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_11.out
let $QUERY=
SELECT o_orderkey, o_totalprice FROM orders
WHERE o_orderkey < 15 AND o_shippriority >= 0;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as table scan.' $EXPLAIN_FILE || true
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_totalprice FROM orders
WHERE o_orderkey < 15 AND o_shippriority >= 0|

--echo
--echo === st-12: no WHERE -> full table scan ===
let $QUERY=
SELECT n_nationkey, n_name, n_regionkey FROM nation;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT n_nationkey, n_name, n_regionkey FROM nation|

--echo
--echo === st-13: temporal projection sweep ===
let $QUERY=
SELECT e_id, e_year, e_logged, e_split, e_stamp6
FROM evlog WHERE e_id < 10;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT e_id, e_year, e_logged, e_split, e_stamp6
FROM evlog WHERE e_id < 10|

--echo
--echo === st-14: NULL values in a projected column ===
let $QUERY=
SELECT o_orderkey, o_clerk FROM orders WHERE o_orderkey < 22;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_clerk FROM orders WHERE o_orderkey < 22|

--echo
--echo === st-15: PK equality + multi-conjunct accepting residual ===
let $QUERY=
SELECT o_orderkey, o_totalprice FROM orders
WHERE o_orderkey = 77 AND o_shippriority >= 0 AND o_totalprice > 0;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_15.out
let $QUERY=
SELECT o_orderkey, o_totalprice FROM orders
WHERE o_orderkey = 77 AND o_shippriority >= 0 AND o_totalprice > 0;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as primary key lookup.' $EXPLAIN_FILE
--exec grep -qF 'CONDITIONS (1 key and 2 filters):' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=1
let $QUERY=
SELECT o_orderkey, o_totalprice FROM orders
WHERE o_orderkey = 77 AND o_shippriority >= 0 AND o_totalprice > 0;|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_totalprice FROM orders
WHERE o_orderkey = 77 AND o_shippriority >= 0 AND o_totalprice > 0|

--echo
--echo === st-16: OR / IS NULL nesting in the residual program ===
# o_orderkey 77 has o_clerk NULL (o_orderkey % 7 == 0), so the IS NULL
# disjunct accepts the row — exercises apply_filter group emission
# (OR + IS NULL) inside a lookup interpreted program.
let $QUERY=
SELECT o_orderkey, o_clerk FROM orders
WHERE o_orderkey = 77 AND (o_clerk IS NULL OR o_clerk > 0);|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_16.out
let $QUERY=
SELECT o_orderkey, o_clerk FROM orders
WHERE o_orderkey = 77 AND (o_clerk IS NULL OR o_clerk > 0);|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as primary key lookup.' $EXPLAIN_FILE
--exec grep -qF 'FILTER: ' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=1
let $QUERY=
SELECT o_orderkey, o_clerk FROM orders
WHERE o_orderkey = 77 AND (o_clerk IS NULL OR o_clerk > 0);|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_clerk FROM orders
WHERE o_orderkey = 77 AND (o_clerk IS NULL OR o_clerk > 0)|

--echo
--echo === st-17: composite hash-PK full equality + residual ===
let $QUERY=
SELECT l_orderkey, l_linenumber, l_quantity
FROM lineitem
WHERE l_orderkey = 10 AND l_linenumber = 2 AND l_quantity > 0;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_17.out
let $QUERY=
SELECT l_orderkey, l_linenumber, l_quantity
FROM lineitem
WHERE l_orderkey = 10 AND l_linenumber = 2 AND l_quantity > 0;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as primary key lookup.' $EXPLAIN_FILE
--exec grep -qF 'CONDITIONS (2 keys and 1 filter):' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=1
let $QUERY=
SELECT l_orderkey, l_linenumber, l_quantity
FROM lineitem
WHERE l_orderkey = 10 AND l_linenumber = 2 AND l_quantity > 0;|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
--sorted_result
SELECT l_orderkey, l_linenumber, l_quantity
FROM lineitem
WHERE l_orderkey = 10 AND l_linenumber = 2 AND l_quantity > 0|

--echo
--echo === st-18a: duplicate PK equality -> lookup ===
# The first equality binds the key; the duplicate becomes a residual
# (accepting) filter conjunct.
let $QUERY=
SELECT o_orderkey FROM orders
WHERE o_orderkey = 77 AND o_orderkey = 77;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_18a.out
let $QUERY=
SELECT o_orderkey FROM orders
WHERE o_orderkey = 77 AND o_orderkey = 77;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as primary key lookup.' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=1
let $QUERY=
SELECT o_orderkey FROM orders
WHERE o_orderkey = 77 AND o_orderkey = 77;|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey FROM orders
WHERE o_orderkey = 77 AND o_orderkey = 77|

--echo
--echo === st-18b: contradictory PK equality -> empty ===
# Key = 77, residual filter `o_orderkey = 78` rejects the fetched row:
# correct empty result by construction (the single-table twin of the
# Phase 0a contradictory-PK fix).
let $QUERY=
SELECT o_orderkey FROM orders
WHERE o_orderkey = 77 AND o_orderkey = 78;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_18b.out
let $QUERY=
SELECT o_orderkey FROM orders
WHERE o_orderkey = 77 AND o_orderkey = 78;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as primary key lookup.' $EXPLAIN_FILE
--exec grep -qF 'FILTER: ' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=0
let $QUERY=
SELECT o_orderkey FROM orders
WHERE o_orderkey = 77 AND o_orderkey = 78;|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey FROM orders
WHERE o_orderkey = 77 AND o_orderkey = 78|

--echo
--echo === st-19: over-cap residual (long OR chain) -> scan fallback ===
# The 24-term OR chain exceeds the 64-word lookup filter cap, so the
# detection-time trial build falls back to the scan-config path.
let $QUERY=
SELECT o_orderkey, o_clerk FROM orders
WHERE o_orderkey = 77 AND (o_clerk = 1 OR o_clerk = 2 OR o_clerk = 3
  OR o_clerk = 4 OR o_clerk = 5 OR o_clerk = 6 OR o_clerk = 7
  OR o_clerk = 8 OR o_clerk = 9 OR o_clerk = 10 OR o_clerk = 11
  OR o_clerk = 12 OR o_clerk = 13 OR o_clerk = 14 OR o_clerk = 15
  OR o_clerk = 16 OR o_clerk = 17 OR o_clerk = 18 OR o_clerk = 19
  OR o_clerk = 20 OR o_clerk = 21 OR o_clerk = 22 OR o_clerk = 23
  OR o_clerk = 24);|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_19.out
let $QUERY=
SELECT o_orderkey, o_clerk FROM orders
WHERE o_orderkey = 77 AND (o_clerk = 1 OR o_clerk = 2 OR o_clerk = 3
  OR o_clerk = 4 OR o_clerk = 5 OR o_clerk = 6 OR o_clerk = 7
  OR o_clerk = 8 OR o_clerk = 9 OR o_clerk = 10 OR o_clerk = 11
  OR o_clerk = 12 OR o_clerk = 13 OR o_clerk = 14 OR o_clerk = 15
  OR o_clerk = 16 OR o_clerk = 17 OR o_clerk = 18 OR o_clerk = 19
  OR o_clerk = 20 OR o_clerk = 21 OR o_clerk = 22 OR o_clerk = 23
  OR o_clerk = 24);|
--source suite/ronsql/include/ronsql_explain.inc
# The fallback pin: whatever scan plan wins, it must not be a lookup.
--error 1
--exec grep -qF 'Execute as primary key lookup.' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_clerk FROM orders
WHERE o_orderkey = 77 AND (o_clerk = 1 OR o_clerk = 2 OR o_clerk = 3
  OR o_clerk = 4 OR o_clerk = 5 OR o_clerk = 6 OR o_clerk = 7
  OR o_clerk = 8 OR o_clerk = 9 OR o_clerk = 10 OR o_clerk = 11
  OR o_clerk = 12 OR o_clerk = 13 OR o_clerk = 14 OR o_clerk = 15
  OR o_clerk = 16 OR o_clerk = 17 OR o_clerk = 18 OR o_clerk = 19
  OR o_clerk = 20 OR o_clerk = 21 OR o_clerk = 22 OR o_clerk = 23
  OR o_clerk = 24)|

--echo
--echo === st-20: unsupported-type residual -> plan-time scan fallback ===
# TIME(3) constants are outside the filter emit's type envelope: the
# detection-time trial build catches the throw and falls back to the
# scan path (EXPLAIN keeps working), and execution fails exactly as the
# scan path always has for this residual.
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_20.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_st20.sql
--write_file $QUERY_FILE
SELECT e_id, e_group FROM evlog
WHERE e_id = 5 AND e_split > '10:00:00';
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as table scan.' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e "SELECT e_id, e_group FROM evlog WHERE e_id = 5 AND e_split > '10:00:00';" 2>&1

--echo
--echo === st-21: VARCHAR-PK plain lookup ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_st21.sql
--write_file $QUERY_FILE
SELECT v_code, v_num FROM vkey1 WHERE v_code = 'beta';
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_21.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_st21e.sql
--write_file $QUERY_FILE
SELECT v_code, v_num FROM vkey1 WHERE v_code = 'beta';
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as primary key lookup.' $EXPLAIN_FILE
--exec grep -qF 'KEYS (1):' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=1
--let QUERY_FILE=$MYSQL_TMP_DIR/q_st21r.sql
--write_file $QUERY_FILE
SELECT v_code, v_num FROM vkey1 WHERE v_code = 'beta';
EOF
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
--sorted_result
SELECT v_code, v_num FROM vkey1 WHERE v_code = 'beta'|

--echo
--echo === st-22: VARCHAR-PK lookup + residual ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_st22.sql
--write_file $QUERY_FILE
SELECT v_code, v_num, v_note FROM vkey1
WHERE v_code = 'gamma-longer-key' AND v_num >= 0;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_st_22.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_st22e.sql
--write_file $QUERY_FILE
SELECT v_code, v_num, v_note FROM vkey1
WHERE v_code = 'gamma-longer-key' AND v_num >= 0;
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as primary key lookup.' $EXPLAIN_FILE
--exec grep -qF 'CONDITIONS (1 key and 1 filter):' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=1
--let QUERY_FILE=$MYSQL_TMP_DIR/q_st22r.sql
--write_file $QUERY_FILE
SELECT v_code, v_num, v_note FROM vkey1
WHERE v_code = 'gamma-longer-key' AND v_num >= 0;
EOF
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
--sorted_result
SELECT v_code, v_num, v_note FROM vkey1
WHERE v_code = 'gamma-longer-key' AND v_num >= 0|

--echo
--echo === st-23: PROBE — WHERE IN-subquery on a projection ===
let $QUERY=
SELECT o_orderkey FROM orders
WHERE o_custkey IN (SELECT c_custkey FROM customer
                    WHERE c_nationkey = 3);|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey FROM orders
WHERE o_custkey IN (SELECT c_custkey FROM customer
                    WHERE c_nationkey = 3)|

--echo
--echo === st-24: PROBE — scalar subquery constant on a projection ===
let $QUERY=
SELECT o_orderkey, o_custkey FROM orders
WHERE o_orderkey = (SELECT MAX(o_orderkey) FROM orders
                    WHERE o_orderkey < 100);|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
SELECT o_orderkey, o_custkey FROM orders
WHERE o_orderkey = (SELECT MAX(o_orderkey) FROM orders
                    WHERE o_orderkey < 100)|

DELIMITER ;|

# st-P1/P2 (ORDER BY / ORDER BY + LIMIT rejection probes) were retired
# when ronsql_orderby_limit_plan.md Phases 2+3 shipped LIMIT streaming
# and the buffered client-side sort; positive coverage lives in
# body_passthrough_limit.inc and body_passthrough_orderby.inc.

--echo
--echo === st-P3 (rejection): expression in the projection ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'SELECT o_orderkey + 1 FROM orders WHERE o_orderkey = 77;' 2>&1

--echo
--echo === st-P4 (rejection): WITH prefix on a single-table projection ===
# A CTE definition on a shape-C query stays rejected rather than
# silently ignoring the CTE.
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT o_orderkey FROM orders WHERE o_orderkey = 77;' 2>&1

--disable_query_log
DROP TABLE vkey1;
--enable_query_log
--source suite/ronsql_cte/include/cte_drop.inc
