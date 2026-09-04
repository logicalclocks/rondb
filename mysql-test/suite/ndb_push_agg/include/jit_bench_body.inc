--source include/have_ndb.inc

# RONDB-1056 — the JIT benchmark body (interpreter vs compiled).
#
# Sourced by ndb_push_agg/t/jit_bench.test (CompiledInterpreter=OFF, the
# interpreter arm) and ndb_push_agg_jit/t/jit_bench.test (ON, the JIT
# arm); the suite my.cnf files do the arming, exactly like the
# correctness differential. Every bench binary prints its per-iteration
# timings ("Iteration N: PASS", "Scan+Join: X ms", "Total: X ms", "SQL
# query: X ms") on stderr, which the --exec lines below capture into
# $MYSQLTEST_VARDIR/log/jit_bench/<bench>.txt — NOT into the .result (only the
# echo lines and the DROPs are recorded, so the result is deterministic).
# The driver claude_files/compiled_interpreter/jit_bench_off_vs_on.py
# runs both arms, collects those files and prints the OFF-vs-ON table.
#
# A bench's non-zero exit (a failed iteration or SQL verification) must
# not abort the run: every bench --exec is preceded by --error 0,1 and the
# driver flags FAILs from the captured output. The TPC-H load stays fatal.
#
# Parameters, read from the environment (the driver exports them; the
# defaults below apply to a bare mtr run):
#   JIT_BENCH_SF      TPC-H scale factor for load_tpch   (default 0.05)
#   JIT_BENCH_ITERS   iterations per bench               (default 5)
#   JIT_BENCH_ORDERS  --orders for the self-loading q12  (default 20000)

--let $sf = $JIT_BENCH_SF
if (!$sf)
{
  --let $sf = 0.05
}
--let $iters = $JIT_BENCH_ITERS
if (!$iters)
{
  --let $iters = 5
}
--let $orders = $JIT_BENCH_ORDERS
if (!$orders)
{
  --let $orders = 20000
}
# Output goes under $MYSQLTEST_VARDIR/log (2026-09-04): mtr's check-testcase
# lists MYSQLTEST_VARDIR/tmp and (with --parallel > 1) MYSQL_TMP_DIR before
# and after every test, so a jit_bench/ left there for the driver to collect
# failed the test with "does not preserve the state". The contents of
# MYSQLTEST_VARDIR/log are deliberately not listed (see
# include/check-testcase.test). The driver copies from var/log/jit_bench.
--let $out = $MYSQLTEST_VARDIR/log/jit_bench
--exec mkdir -p $out

--echo Loading TPC-H data
--exec $NDB_PUSH_AGG_DIR/load_tpch -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT --sf $sf > $out/load_tpch.txt 2>&1

# JIT-activity proof: ndbinfo.jit before and after the benches. The driver
# prints the per-column deltas per arm (rows_executed / programs_compiled
# must be > 0 under ON and 0 under OFF; programs_fallback shows rejects).
--exec $MYSQL -e "SELECT * FROM ndbinfo.jit" > $out/ndbinfo_jit_before.txt 2>&1

--echo Running bench_q2_ndbapi
--error 0,1
--exec $NDB_PUSH_AGG_DIR/bench_q2_ndbapi -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT --iterations $iters > $out/bench_q2_ndbapi.txt 2>&1

--echo Running bench_q3_ndbapi
--error 0,1
--exec $NDB_PUSH_AGG_DIR/bench_q3_ndbapi -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT --iterations $iters > $out/bench_q3_ndbapi.txt 2>&1

--echo Running bench_q4_ndbapi
--error 0,1
--exec $NDB_PUSH_AGG_DIR/bench_q4_ndbapi -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT --iterations $iters > $out/bench_q4_ndbapi.txt 2>&1

--echo Running bench_q5_ndbapi
--error 0,1
--exec $NDB_PUSH_AGG_DIR/bench_q5_ndbapi -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT --iterations $iters > $out/bench_q5_ndbapi.txt 2>&1

--echo Running bench_q9_dbtc
--error 0,1
--exec $NDB_PUSH_AGG_DIR/bench_q9_dbtc -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT --iterations $iters > $out/bench_q9_dbtc.txt 2>&1

--echo Running bench_q9_ndbapi
--error 0,1
--exec $NDB_PUSH_AGG_DIR/bench_q9_ndbapi -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT --iterations $iters > $out/bench_q9_ndbapi.txt 2>&1

--echo Running bench_q10_ndbapi
--error 0,1
--exec $NDB_PUSH_AGG_DIR/bench_q10_ndbapi -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT --iterations $iters > $out/bench_q10_ndbapi.txt 2>&1

--echo Running bench_q11_ndbapi
--error 0,1
--exec $NDB_PUSH_AGG_DIR/bench_q11_ndbapi -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT --iterations $iters > $out/bench_q11_ndbapi.txt 2>&1

--echo Running bench_minmax_ndbapi
--error 0,1
--exec $NDB_PUSH_AGG_DIR/bench_minmax_ndbapi -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT --iterations $iters > $out/bench_minmax_ndbapi.txt 2>&1

--echo Running bench_nogroup_ndbapi
--error 0,1
--exec $NDB_PUSH_AGG_DIR/bench_nogroup_ndbapi -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT --iterations $iters > $out/bench_nogroup_ndbapi.txt 2>&1

--echo Running bench_orderscan_ndbapi
--error 0,1
--exec $NDB_PUSH_AGG_DIR/bench_orderscan_ndbapi -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT --iterations $iters > $out/bench_orderscan_ndbapi.txt 2>&1

--echo Running bench_datescan_ndbapi
--error 0,1
--exec $NDB_PUSH_AGG_DIR/bench_datescan_ndbapi -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT --iterations $iters > $out/bench_datescan_ndbapi.txt 2>&1

--echo Running bench_q12_dbtc
--error 0,1
--exec $NDB_PUSH_AGG_DIR/bench_q12_dbtc -c "$NDB_CONNECTSTRING" -m $MASTER_MYPORT --orders $orders --iterations $iters > $out/bench_q12_dbtc.txt 2>&1

# Aggregation-HEAVY SQL benches over the loaded tpch_lineitem (pushdown
# aggregation through mysqld = the STANDALONE JIT path, no join): many
# aggregates per row, CASE arms, a scan filter, and a GROUP BY. Each
# query runs $iters times; the elapsed_us line lands in
# $out/sql_agg_<name>.txt (the driver reports med / min per arm). This is
# where the per-row aggregation program is a large share of the row's
# cost — the TPC-H binaries above are join-bound.
--echo Running sql_agg benches
--let $i = $iters
while ($i)
{
  --exec $MYSQL test -e "SET ndb_pushdown_aggregate=ON; SET @t0 = NOW(6); SELECT COUNT(*), SUM(l_quantity), SUM(l_extendedprice), SUM(l_discount), SUM(l_tax), MIN(l_quantity), MAX(l_quantity), MIN(l_extendedprice), MAX(l_extendedprice), SUM(l_partkey), SUM(l_suppkey), MAX(l_orderkey) FROM tpch_lineitem; SELECT CONCAT('elapsed_us=', TIMESTAMPDIFF(MICROSECOND, @t0, NOW(6))) AS t;" >> $out/sql_agg_wide.txt 2>&1
  --dec $i
}
--let $i = $iters
while ($i)
{
  --exec $MYSQL test -e "SET ndb_pushdown_aggregate=ON; SET @t0 = NOW(6); SELECT COUNT(*), SUM(CASE WHEN l_returnflag = 'R' THEN l_quantity ELSE 0 END), SUM(CASE WHEN l_linestatus = 'F' THEN 1 ELSE 0 END), SUM(CASE WHEN l_quantity > 25 THEN l_extendedprice ELSE 0 END) FROM tpch_lineitem; SELECT CONCAT('elapsed_us=', TIMESTAMPDIFF(MICROSECOND, @t0, NOW(6))) AS t;" >> $out/sql_agg_case.txt 2>&1
  --dec $i
}
--let $i = $iters
while ($i)
{
  --exec $MYSQL test -e "SET ndb_pushdown_aggregate=ON; SET @t0 = NOW(6); SELECT COUNT(*), SUM(l_quantity), SUM(l_extendedprice) FROM tpch_lineitem WHERE l_quantity > 25 AND l_discount < 0.05; SELECT CONCAT('elapsed_us=', TIMESTAMPDIFF(MICROSECOND, @t0, NOW(6))) AS t;" >> $out/sql_agg_filter.txt 2>&1
  --dec $i
}
--let $i = $iters
while ($i)
{
  --exec $MYSQL test -e "SET ndb_pushdown_aggregate=ON; SET @t0 = NOW(6); SELECT l_returnflag, l_linestatus, COUNT(*), SUM(l_quantity), SUM(l_extendedprice), MIN(l_shipdate), MAX(l_shipdate) FROM tpch_lineitem GROUP BY l_returnflag, l_linestatus; SELECT CONCAT('elapsed_us=', TIMESTAMPDIFF(MICROSECOND, @t0, NOW(6))) AS t;" >> $out/sql_agg_group.txt 2>&1
  --dec $i
}

--exec $MYSQL -e "SELECT * FROM ndbinfo.jit" > $out/ndbinfo_jit_after.txt 2>&1

# Clean up the TPC-H tables (the self-loading q12 drops its own).
DROP TABLE IF EXISTS tpch_lineitem;
DROP TABLE IF EXISTS tpch_orders;
DROP TABLE IF EXISTS tpch_partsupp;
DROP TABLE IF EXISTS tpch_customer;
DROP TABLE IF EXISTS tpch_supplier;
DROP TABLE IF EXISTS tpch_part;
DROP TABLE IF EXISTS tpch_nation;
DROP TABLE IF EXISTS tpch_region;
