# body_passthrough_orderby_index.inc — ORDER BY served by ordered-index
# scan order on the single-table pass-through path
# (ronsql_orderby_limit_plan.md Phase 4b), data-driven CTE suite.
#
# Phase 4b: when the planner's chosen ordered index delivers the ORDER BY
# order — the ORDER BY list equals the index columns after any leading
# equality-bound columns, with one uniform direction — the scan runs with
# SF_OrderBy [| SF_Descending] and the NDB API merge-sorts the per-fragment
# ordered scans into one globally ordered stream.  Rows then stream through
# the Phase 2 LIMIT cutoff (early close after the top-N) instead of being
# buffered for the Phase 3 client-side sort.  Index selection stays
# WHERE-first: a bound-based candidate keeps its goodness and only gets a
# tie-break bonus when it also serves the order; an index with no usable
# bound still beats the table scan when it delivers the order (full index
# scan in key order, every conjunct a residual filter).  FORCE/USE/IGNORE
# INDEX hints apply to the ORDER BY pass too, and FORCE INDEX on an index
# that serves only the ORDER BY (no WHERE bound) is now accepted.
# EXPLAIN reports the strategy: "ORDER BY: index order (SF_OrderBy ..." vs
# "ORDER BY: client-side sort ...".
#
# RUNTIME VERIFICATION: besides the EXPLAIN greps (plan choice), every
# case pins the x-ronsql-phases `rows=` counter through
# ronsql_phase_rows.inc — the rows the RonSQL drain actually fetched.  The
# index-order streaming path drains exactly min(LIMIT, matching rows) and
# closes the scan early (poi-4: 12 of 1500, poi-8: 260 of 1500, poi-16: 6
# of 100, poi-17: 62 of 300, poi-15: 0), whereas the client-side fallbacks
# must drain every matching row before sorting (poi-10: 500, poi-11: 15,
# poi-12: 1000, poi-14: 1500).  Identical, correctly ordered output from
# both paths cannot hide which one ran.
#
# ORDER VERIFICATION: every case sets $skip_sort=yes so ronsql_compare.inc
# strict-diffs RonSQL vs MySQL in DELIVERED row order.  Index-order cases
# cannot add a tie-breaker column (it would break the prefix match), so
# each ORDER BY below is either a total order on the projected rows
# (o_orderdate is unique within one o_orderstatus value and for dates
# >= 1996-05-16; o_clerk = n+1 is unique for o_totalprice < 400) or the
# projection is restricted to the ORDER BY key so tied rows print
# identically.  EXPLAIN greps are FATAL (no `|| true`): the plan choice is
# the point of this family.
#
# Case map (poi-N):
#   1   equality prefix: WHERE o_orderstatus = 'O' ORDER BY o_orderdate
#       LIMIT -> idx_o_status_date, index order ASC
#   2   same, DESC (SF_Descending)
#   3   bound on the ORDER BY column itself, no LIMIT: WHERE o_orderdate
#       >= X ORDER BY o_orderdate -> idx_o_orderdate, streamed
#   4   no WHERE: ORDER BY o_orderdate DESC LIMIT -> ORDER BY-driven
#       index choice (no bounds), DESC
#   5   no WHERE, ASC LIMIT across tied dates (key-only projection)
#   6   NULLs first in ASC index order + LIMIT across the NULL run
#       (residual filter, ORDER BY-driven idx_o_clerk)
#   7   NULLs last in DESC index order, no LIMIT (residual filter)
#   8   LIMIT 260 crosses a fragment batch boundary under the ordered
#       merge, then closes early
#   9   equality prefix with the conjuncts in reverse index order +
#       range bound on the ORDER BY column
#   10  mixed directions -> client-side sort (buffered)
#   11  fs_history shape: WHERE-chosen idx_o_custkey does not serve
#       ORDER BY o_orderdate -> client-side sort
#   12  second index column without a leading equality -> client-side
#       sort (WHERE-first: the range candidate beats the ORDER BY-only
#       candidate)
#   13  FORCE INDEX on an index that serves only the ORDER BY (no WHERE)
#       -> accepted, index order
#   14  IGNORE INDEX on the ORDER BY index -> table scan + client-side
#       sort
#   15  index order + LIMIT 0 -> no output
#   16  equality prefix + residual filter + LIMIT
#   17  VARCHAR index order (collation) + LIMIT across a value boundary
#   18  ORDER BY alias (OUTPUT_REF) served by the index, DESC + LIMIT
#   P1  rejection: FORCE INDEX on an index serving neither a WHERE
#       bound nor the ORDER BY (no WHERE)
#   P2  rejection: same with a residual-only WHERE
--source include/have_ndb.inc
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings
--let $suppress_ronsql_cli=yes
--let $strict_diff=yes
--let $skip_sort=yes
--source suite/ronsql_cte/include/cte_schema.inc
--source suite/ronsql_cte/include/cte_data.inc

DELIMITER |;

# ============================================================
# Index order: equality prefix + ORDER BY on the next index column.
# ============================================================

--echo
--echo === poi-1: equality prefix, ORDER BY next index column ASC + LIMIT ===
# idx_o_status_date (o_orderstatus, o_orderdate): the = bound fixes the
# leading column, so the scan delivers o_orderdate order.  Dates are unique
# within one status value (n and n+1000 never share a status).
--let $skip_sort=yes
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi1.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate FROM orders
WHERE o_orderstatus = 'O' ORDER BY o_orderdate LIMIT 10;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_poi_1.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi1e.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate FROM orders
WHERE o_orderstatus = 'O' ORDER BY o_orderdate LIMIT 10;
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as index scan.' $EXPLAIN_FILE
--exec grep -qF 'Index: `idx_o_status_date`' $EXPLAIN_FILE
--exec grep -qF 'CONDITIONS (1 bound)' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: index order (SF_OrderBy merge' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=10
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi1r.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate FROM orders
WHERE o_orderstatus = 'O' ORDER BY o_orderdate LIMIT 10;
EOF
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_orderkey, o_orderdate FROM orders
WHERE o_orderstatus = 'O' ORDER BY o_orderdate LIMIT 10|

--echo
--echo === poi-2: equality prefix, ORDER BY next index column DESC + LIMIT ===
--let $skip_sort=yes
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi2.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate, o_totalprice FROM orders
WHERE o_orderstatus = 'O' ORDER BY o_orderdate DESC LIMIT 10;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_poi_2.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi2e.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate, o_totalprice FROM orders
WHERE o_orderstatus = 'O' ORDER BY o_orderdate DESC LIMIT 10;
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Index: `idx_o_status_date`' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: index order (SF_OrderBy | SF_Descending merge' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=10
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi2r.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate, o_totalprice FROM orders
WHERE o_orderstatus = 'O' ORDER BY o_orderdate DESC LIMIT 10;
EOF
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_orderkey, o_orderdate, o_totalprice FROM orders
WHERE o_orderstatus = 'O' ORDER BY o_orderdate DESC LIMIT 10|

# ============================================================
# Index order: bound on the ORDER BY column / no WHERE at all.
# ============================================================

--echo
--echo === poi-3: range bound on the ORDER BY column, no LIMIT (streamed) ===
# idx_o_orderdate: the half-open bound and the ORDER BY share the column.
# Dates from 1996-05-16 on are unique (n + 1000 > 1500), so the 26 rows
# from 1997-09-01 form a total order without a tie-breaker.
--let $skip_sort=yes
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi3.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate, o_orderstatus FROM orders
WHERE o_orderdate >= '1997-09-01' ORDER BY o_orderdate;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_poi_3.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi3e.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate, o_orderstatus FROM orders
WHERE o_orderdate >= '1997-09-01' ORDER BY o_orderdate;
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Index: `idx_o_orderdate`' $EXPLAIN_FILE
--exec grep -qF 'CONDITIONS (1 bound)' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: index order (SF_OrderBy merge' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=26
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi3r.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate, o_orderstatus FROM orders
WHERE o_orderdate >= '1997-09-01' ORDER BY o_orderdate;
EOF
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_orderkey, o_orderdate, o_orderstatus FROM orders
WHERE o_orderdate >= '1997-09-01' ORDER BY o_orderdate|

--echo
--echo === poi-4: no WHERE, ORDER BY DESC + LIMIT (ORDER BY-driven index choice) ===
# No bound exists, so the ORDER BY pass pushes idx_o_orderdate as a
# bonus-only candidate that beats the table scan: full index scan in DESC
# key order, early close after 12 rows.  The 12 latest dates are unique.
--let $skip_sort=yes
let $QUERY=
SELECT o_orderkey, o_orderdate FROM orders
ORDER BY o_orderdate DESC LIMIT 12;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_poi_4.out
let $QUERY=
SELECT o_orderkey, o_orderdate FROM orders
ORDER BY o_orderdate DESC LIMIT 12;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as index scan.' $EXPLAIN_FILE
--exec grep -qF 'Index: `idx_o_orderdate`' $EXPLAIN_FILE
--exec grep -qF 'Chosen for ORDER BY index order (no bounds).' $EXPLAIN_FILE
--exec grep -qF 'No filters.' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: index order (SF_OrderBy | SF_Descending merge' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=12
let $QUERY=
SELECT o_orderkey, o_orderdate FROM orders
ORDER BY o_orderdate DESC LIMIT 12;|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_orderkey, o_orderdate FROM orders
ORDER BY o_orderdate DESC LIMIT 12|

--echo
--echo === poi-5: no WHERE, ORDER BY ASC + LIMIT across tied dates (key-only projection) ===
# Days 1..500 carry two orders each (n and n+1000) and day 0 one; the
# merge may interleave the tied rows in any order, so only the key is
# projected: 1 + 5*2 = 11 rows.
--let $skip_sort=yes
let $QUERY=
SELECT o_orderdate FROM orders
ORDER BY o_orderdate LIMIT 11;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPECT_ROWS=11
let $QUERY=
SELECT o_orderdate FROM orders
ORDER BY o_orderdate LIMIT 11;|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_orderdate FROM orders
ORDER BY o_orderdate LIMIT 11|

# ============================================================
# NULL placement under index order (nullable idx_o_clerk).
# ============================================================

--echo
--echo === poi-6: NULLs first in ASC index order + LIMIT across the NULL run ===
# o_totalprice < 1000 keeps n <= 74 (residual filter: no index on
# o_totalprice), so idx_o_clerk is chosen for the ORDER BY alone.  NULL
# clerks (n = 7,14,...,70: 10 rows) come first, then clerk 1 (n=40),
# then clerk 2 (n=1, 41).  LIMIT 13 = 10 NULL + 1 + 2.
--let $skip_sort=yes
let $QUERY=
SELECT o_clerk FROM orders
WHERE o_totalprice < 1000 ORDER BY o_clerk LIMIT 13;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_poi_6.out
let $QUERY=
SELECT o_clerk FROM orders
WHERE o_totalprice < 1000 ORDER BY o_clerk LIMIT 13;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Index: `idx_o_clerk`' $EXPLAIN_FILE
--exec grep -qF 'Chosen for ORDER BY index order (no bounds).' $EXPLAIN_FILE
--exec grep -qF 'FILTERS:' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: index order (SF_OrderBy merge' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=13
let $QUERY=
SELECT o_clerk FROM orders
WHERE o_totalprice < 1000 ORDER BY o_clerk LIMIT 13;|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_clerk FROM orders
WHERE o_totalprice < 1000 ORDER BY o_clerk LIMIT 13|

--echo
--echo === poi-7: NULLs last in DESC index order, no LIMIT ===
# o_totalprice < 400 keeps n <= 29, where o_clerk = n+1 is unique (NULL
# for n = 7,14,21,28): 25 distinct clerks descending, then 4 NULL rows
# (identical lines, so the tie among them cannot show).
--let $skip_sort=yes
let $QUERY=
SELECT o_clerk FROM orders
WHERE o_totalprice < 400 ORDER BY o_clerk DESC;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_poi_7.out
let $QUERY=
SELECT o_clerk FROM orders
WHERE o_totalprice < 400 ORDER BY o_clerk DESC;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Index: `idx_o_clerk`' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: index order (SF_OrderBy | SF_Descending merge' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=29
let $QUERY=
SELECT o_clerk FROM orders
WHERE o_totalprice < 400 ORDER BY o_clerk DESC;|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_clerk FROM orders
WHERE o_totalprice < 400 ORDER BY o_clerk DESC|

# ============================================================
# Batch boundary under the ordered merge + early close.
# ============================================================

--echo
--echo === poi-8: LIMIT 260 crosses a fragment batch boundary under the ordered merge ===
# 1500 rows spread over the table's fragments: at 260 delivered rows at
# least one receiver has had to fetch a second batch in every topology;
# the drain then closes the still-open scan.  Key-only projection (dates
# 1..500 are tied pairs).
--let $skip_sort=yes
let $QUERY=
SELECT o_orderdate FROM orders
ORDER BY o_orderdate LIMIT 260;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPECT_ROWS=260
let $QUERY=
SELECT o_orderdate FROM orders
ORDER BY o_orderdate LIMIT 260;|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_orderdate FROM orders
ORDER BY o_orderdate LIMIT 260|

# ============================================================
# Equality prefix: conjunct order + composite bounds.
# ============================================================

--echo
--echo === poi-9: equality prefix with reversed conjunct order + range on the ORDER BY column ===
# The = on the leading index column comes second in the WHERE; the
# candidate generator still binds both index columns (2 bounds) and the
# ORDER BY matches after the equality prefix.  Status 'P' dates are
# unique.
--let $skip_sort=yes
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi9.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate FROM orders
WHERE o_orderdate >= '1996-01-01' AND o_orderstatus = 'P'
ORDER BY o_orderdate LIMIT 8;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_poi_9.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi9e.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate FROM orders
WHERE o_orderdate >= '1996-01-01' AND o_orderstatus = 'P'
ORDER BY o_orderdate LIMIT 8;
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Index: `idx_o_status_date`' $EXPLAIN_FILE
--exec grep -qF 'CONDITIONS (2 bounds)' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: index order (SF_OrderBy merge' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=8
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi9r.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate FROM orders
WHERE o_orderdate >= '1996-01-01' AND o_orderstatus = 'P'
ORDER BY o_orderdate LIMIT 8;
EOF
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_orderkey, o_orderdate FROM orders
WHERE o_orderdate >= '1996-01-01' AND o_orderstatus = 'P'
ORDER BY o_orderdate LIMIT 8|

# ============================================================
# Fallbacks to the Phase 3 client-side sort (correct either way; the
# EXPLAIN greps pin the strategy).
# ============================================================

--echo
--echo === poi-10: mixed directions -> client-side sort ===
--let $skip_sort=yes
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi10.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate FROM orders
WHERE o_orderstatus = 'F'
ORDER BY o_orderstatus ASC, o_orderdate DESC LIMIT 5;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_poi_10.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi10e.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate FROM orders
WHERE o_orderstatus = 'F'
ORDER BY o_orderstatus ASC, o_orderdate DESC LIMIT 5;
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Index: `idx_o_status_date`' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: client-side sort' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=500
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi10r.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate FROM orders
WHERE o_orderstatus = 'F'
ORDER BY o_orderstatus ASC, o_orderdate DESC LIMIT 5;
EOF
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_orderkey, o_orderdate FROM orders
WHERE o_orderstatus = 'F'
ORDER BY o_orderstatus ASC, o_orderdate DESC LIMIT 5|

--echo
--echo === poi-11: fs_history shape, WHERE-chosen index does not serve the ORDER BY -> client-side sort ===
# idx_o_custkey wins on the range bound; ORDER BY o_orderdate is not its
# prefix and o_orderkey is in no index, so the rows buffer + sort.
--let $skip_sort=yes
let $QUERY=
SELECT o_orderkey, o_custkey, o_orderdate FROM orders
WHERE o_custkey >= 10 AND o_custkey < 13
ORDER BY o_orderdate DESC, o_orderkey DESC LIMIT 6;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_poi_11.out
let $QUERY=
SELECT o_orderkey, o_custkey, o_orderdate FROM orders
WHERE o_custkey >= 10 AND o_custkey < 13
ORDER BY o_orderdate DESC, o_orderkey DESC LIMIT 6;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Index: `idx_o_custkey`' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: client-side sort' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=15
let $QUERY=
SELECT o_orderkey, o_custkey, o_orderdate FROM orders
WHERE o_custkey >= 10 AND o_custkey < 13
ORDER BY o_orderdate DESC, o_orderkey DESC LIMIT 6;|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_orderkey, o_custkey, o_orderdate FROM orders
WHERE o_custkey >= 10 AND o_custkey < 13
ORDER BY o_orderdate DESC, o_orderkey DESC LIMIT 6|

--echo
--echo === poi-12: second index column without a leading equality -> client-side sort ===
# o_orderstatus >= 'O' is a RANGE on idx_o_status_date's leading column
# (goodness 110), which beats the bonus-only idx_o_orderdate candidate;
# a range-bound leading column cannot be skipped, so the scan is not in
# o_orderdate order and the rows sort client-side.  Key-only projection
# ('O' n and 'P' n+1000 can share a date).
--let $skip_sort=yes
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi12.sql
--write_file $QUERY_FILE
SELECT o_orderdate FROM orders
WHERE o_orderstatus >= 'O' ORDER BY o_orderdate LIMIT 5;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_poi_12.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi12e.sql
--write_file $QUERY_FILE
SELECT o_orderdate FROM orders
WHERE o_orderstatus >= 'O' ORDER BY o_orderdate LIMIT 5;
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Index: `idx_o_status_date`' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: client-side sort' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=1000
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi12r.sql
--write_file $QUERY_FILE
SELECT o_orderdate FROM orders
WHERE o_orderstatus >= 'O' ORDER BY o_orderdate LIMIT 5;
EOF
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_orderdate FROM orders
WHERE o_orderstatus >= 'O' ORDER BY o_orderdate LIMIT 5|

# ============================================================
# Index hints on the ORDER BY pass.
# ============================================================

--echo
--echo === poi-13: FORCE INDEX on an index serving only the ORDER BY (no WHERE) -> accepted ===
# Previously "FORCE INDEX cannot be used: the query has no WHERE condition
# that can serve as an index bound."; the deferred check now accepts the
# ORDER BY match.
--let $skip_sort=yes
let $QUERY=
SELECT o_orderkey, o_orderdate FROM orders FORCE INDEX (idx_o_orderdate)
ORDER BY o_orderdate DESC LIMIT 5;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_poi_13.out
let $QUERY=
SELECT o_orderkey, o_orderdate FROM orders FORCE INDEX (idx_o_orderdate)
ORDER BY o_orderdate DESC LIMIT 5;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Index: `idx_o_orderdate`' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: index order (SF_OrderBy | SF_Descending merge' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=5
let $QUERY=
SELECT o_orderkey, o_orderdate FROM orders FORCE INDEX (idx_o_orderdate)
ORDER BY o_orderdate DESC LIMIT 5;|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_orderkey, o_orderdate FROM orders FORCE INDEX (idx_o_orderdate)
ORDER BY o_orderdate DESC LIMIT 5|

--echo
--echo === poi-14: IGNORE INDEX on the ORDER BY index -> table scan + client-side sort ===
# Key-only projection: day 0 once, days 1 and 2 twice each = 5 rows.
--let $skip_sort=yes
let $QUERY=
SELECT o_orderdate FROM orders IGNORE INDEX (idx_o_orderdate)
ORDER BY o_orderdate LIMIT 5;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_poi_14.out
let $QUERY=
SELECT o_orderdate FROM orders IGNORE INDEX (idx_o_orderdate)
ORDER BY o_orderdate LIMIT 5;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Execute as table scan.' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: client-side sort' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=1500
let $QUERY=
SELECT o_orderdate FROM orders IGNORE INDEX (idx_o_orderdate)
ORDER BY o_orderdate LIMIT 5;|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_orderdate FROM orders IGNORE INDEX (idx_o_orderdate)
ORDER BY o_orderdate LIMIT 5|

# ============================================================
# LIMIT 0, residual filter, VARCHAR collation, alias target.
# ============================================================

--echo
--echo === poi-15: index order + LIMIT 0 -> no output ===
--let $skip_sort=yes
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi15.sql
--write_file $QUERY_FILE
SELECT o_orderkey FROM orders
WHERE o_orderstatus = 'O' ORDER BY o_orderdate LIMIT 0;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPECT_ROWS=0
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi15r.sql
--write_file $QUERY_FILE
SELECT o_orderkey FROM orders
WHERE o_orderstatus = 'O' ORDER BY o_orderdate LIMIT 0;
EOF
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_orderkey FROM orders
WHERE o_orderstatus = 'O' ORDER BY o_orderdate LIMIT 0|

--echo
--echo === poi-16: equality prefix + residual filter + LIMIT ===
# o_shippriority has no index: it stays a filter on the ordered scan
# (1 bound and 1 filter), rows still stream in o_orderdate order.
--let $skip_sort=yes
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi16.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate, o_shippriority FROM orders
WHERE o_orderstatus = 'O' AND o_shippriority = 2
ORDER BY o_orderdate LIMIT 6;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_poi_16.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi16e.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate, o_shippriority FROM orders
WHERE o_orderstatus = 'O' AND o_shippriority = 2
ORDER BY o_orderdate LIMIT 6;
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Index: `idx_o_status_date`' $EXPLAIN_FILE
--exec grep -qF 'CONDITIONS (1 bound and 1 filter)' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: index order (SF_OrderBy merge' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=6
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi16r.sql
--write_file $QUERY_FILE
SELECT o_orderkey, o_orderdate, o_shippriority FROM orders
WHERE o_orderstatus = 'O' AND o_shippriority = 2
ORDER BY o_orderdate LIMIT 6;
EOF
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_orderkey, o_orderdate, o_shippriority FROM orders
WHERE o_orderstatus = 'O' AND o_shippriority = 2
ORDER BY o_orderdate LIMIT 6|

--echo
--echo === poi-17: VARCHAR index order (collation) + LIMIT across a value boundary ===
# idx_c_mktsegment: 60 rows per segment; LIMIT 62 = all AUTOMOBILE + 2
# BUILDING (key-only projection, ties within a segment).
--let $skip_sort=yes
let $QUERY=
SELECT c_mktsegment FROM customer
ORDER BY c_mktsegment LIMIT 62;|
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_poi_17.out
let $QUERY=
SELECT c_mktsegment FROM customer
ORDER BY c_mktsegment LIMIT 62;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Index: `idx_c_mktsegment`' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: index order (SF_OrderBy merge' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=62
let $QUERY=
SELECT c_mktsegment FROM customer
ORDER BY c_mktsegment LIMIT 62;|
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT c_mktsegment FROM customer
ORDER BY c_mktsegment LIMIT 62|

--echo
--echo === poi-18: ORDER BY alias (OUTPUT_REF) served by the index, DESC + LIMIT ===
# The alias resolves through the SELECT output to o_orderdate; status 'F'
# dates are unique.
--let $skip_sort=yes
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi18.sql
--write_file $QUERY_FILE
SELECT o_orderdate AS d, o_orderkey AS k FROM orders
WHERE o_orderstatus = 'F' ORDER BY d DESC LIMIT 5;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_poi_18.out
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi18e.sql
--write_file $QUERY_FILE
SELECT o_orderdate AS d, o_orderkey AS k FROM orders
WHERE o_orderstatus = 'F' ORDER BY d DESC LIMIT 5;
EOF
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'Index: `idx_o_status_date`' $EXPLAIN_FILE
--exec grep -qF 'ORDER BY: index order (SF_OrderBy | SF_Descending merge' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
--let $EXPECT_ROWS=5
--let QUERY_FILE=$MYSQL_TMP_DIR/q_poi18r.sql
--write_file $QUERY_FILE
SELECT o_orderdate AS d, o_orderkey AS k FROM orders
WHERE o_orderstatus = 'F' ORDER BY d DESC LIMIT 5;
EOF
--source suite/ronsql/include/ronsql_phase_rows.inc
--echo == Expected result ==
SELECT o_orderdate AS d, o_orderkey AS k FROM orders
WHERE o_orderstatus = 'F' ORDER BY d DESC LIMIT 5|

DELIMITER ;|

# ============================================================
# PROBES (form 1): clean permanent rejection via ronsql_cli --error 1.
# ============================================================

--echo
--echo === poi-P1 (rejection): FORCE INDEX serving neither a bound nor the ORDER BY (no WHERE) ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'SELECT o_orderkey, o_orderdate FROM orders FORCE INDEX (idx_o_custkey) ORDER BY o_orderdate LIMIT 5;' 2>&1

--echo
--echo === poi-P2 (rejection): FORCE INDEX serving neither a bound nor the ORDER BY (residual WHERE) ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'SELECT o_orderkey, o_orderdate FROM orders FORCE INDEX (idx_o_custkey) WHERE o_totalprice < 100 ORDER BY o_orderdate LIMIT 5;' 2>&1

--source suite/ronsql_cte/include/cte_drop.inc
