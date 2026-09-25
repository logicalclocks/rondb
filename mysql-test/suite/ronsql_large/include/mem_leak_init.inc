# mem_leak_init.inc -- RonSQL data-node memory-leak harness (suite
# ronsql_large): parameters, report file and the cluster warm-up.
#
# Source once per test, after the data load.  The method is described in
# t/ronsql_large_mem_leak.test; the pieces are
#   mem_leak_register.inc     phase 1, one shape: first-use check,
#                             warm-up, MySQL comparison
#   mem_leak_measure_all.inc  phase 2, every shape: rounds of 1 / 10 / 50
#                             executions, no growth after round 1
#                             (mem_leak_measure.inc per shape)
#   mem_leak_error_case.inc   one error insert armed around a few requests
#   mem_leak_snapshot.inc     settle, then record the per-node baseline
#   mem_leak_verify.inc       poll until the level is back at the baseline
#   mem_leak_exec.inc         run shapes through RDRS / ronsql_cli
#   mem_leak_dumps.inc        record-level DUMP leak checks
#   mem_leak_finish.inc       DUMP checks, verdict, cleanup
#
# Knobs (set before sourcing this file; defaults in brackets):
#   $mlk_prefix          case name prefix in the result [mlk]
#   $mlk_round1/2/3      executions per measured round [1 / 10 / 50], >= 1.
#                        Environment RONSQL_MEM_LEAK_ROUNDS="a,b,c"
#                        overrides every shape (the printed counts then
#                        differ from the recorded .result).
#   $mlk_round_clients   concurrent RDRS clients per measured round [1]
#   $mlk_round_accept    ok | ok_or_503 for measured rounds [ok]
#   $mlk_warmup          executions per warm-up client [2]
#   $mlk_warmup_clients  concurrent warm-up clients [8]: at least the
#                        4 LDM + 3 TC worker instances per data node of
#                        this suite, so that per-instance one-off pages
#                        (CteScanIterState pool page per LDM instance,
#                        DBSPJ arena page per SPJ instance, DBTC
#                        AggCompleteRecord / CteScanFragHandle page per TC
#                        instance) are all allocated before any baseline.
#   $mlk_qm_tolerance    QUERY_MEMORY pages (32 KB) per node a measured
#                        round may leave above the round-1 level [0]
#   $mlk_tm_tolerance    the same for TRANSACTION_MEMORY [0]
#   $mlk_first_use_qm    QUERY_MEMORY pages per node the first executions
#                        of a shape may add for good [32]: covers the
#                        bounded per-instance one-offs above (at most ~15
#                        pages here) but not one pinned 2 MB pool segment
#                        (64 pages), which is how a leaked lc_ndbd_pool
#                        allocation (string MIN/MAX slots, redistribution
#                        buffers) shows up.
#   $mlk_first_use_tm    the same for TRANSACTION_MEMORY [16]
#   $mlk_settle_reads    identical 0.5 s readings that define "settled"
#                        [10 = 5 s: the DBSPJ row-page cache is released
#                        by a 100 ms timer within about 3 s]
#   $mlk_settle_max_polls  give up settling after this many readings
#                        [60 = 30 s]; the baseline is then marked
#                        UNSETTLED in the report (too high, never too low)
#   $mlk_verify_max_polls  0.5 s polls before a verify fails [120 = 60 s]
#   $mlk_collect         1: a failed check prints LEAK and the test goes
#                        on, mem_leak_finish.inc fails it; 0: fail at once
#                        [1]
#
# Report: every baseline, every round (with the QUERY_MEMORY total after
# each execution) and every residual is appended to
# $MYSQLTEST_VARDIR/log/ronsql_large_mem_leak.report.

--disable_query_log
if ($mlk_prefix == '')
{
  --let $mlk_prefix = mlk
}
if ($mlk_round1 == '')
{
  --let $mlk_round1 = 1
}
if ($mlk_round2 == '')
{
  --let $mlk_round2 = 10
}
if ($mlk_round3 == '')
{
  --let $mlk_round3 = 50
}
if ($mlk_round_clients == '')
{
  --let $mlk_round_clients = 1
}
if ($mlk_round_accept == '')
{
  --let $mlk_round_accept = ok
}
if ($mlk_warmup == '')
{
  --let $mlk_warmup = 2
}
if ($mlk_warmup_clients == '')
{
  --let $mlk_warmup_clients = 8
}
if ($mlk_qm_tolerance == '')
{
  --let $mlk_qm_tolerance = 0
}
if ($mlk_tm_tolerance == '')
{
  --let $mlk_tm_tolerance = 0
}
if ($mlk_first_use_qm == '')
{
  --let $mlk_first_use_qm = 32
}
if ($mlk_first_use_tm == '')
{
  --let $mlk_first_use_tm = 16
}
if ($mlk_settle_reads == '')
{
  --let $mlk_settle_reads = 10
}
if ($mlk_settle_max_polls == '')
{
  --let $mlk_settle_max_polls = 60
}
if ($mlk_verify_max_polls == '')
{
  --let $mlk_verify_max_polls = 120
}
if ($mlk_collect == '')
{
  --let $mlk_collect = 1
}
# Environment override of the round sizes, e.g.
#   RONSQL_MEM_LEAK_ROUNDS=1,10,200 ./mtr --suite=ronsql_large ...
--let $mlk_env_rounds = $RONSQL_MEM_LEAK_ROUNDS
--let $mlk_rounds_forced = 0
if ($mlk_env_rounds != '')
{
  --let $mlk_round1 = `SELECT CAST(SUBSTRING_INDEX('$mlk_env_rounds', ',', 1) AS UNSIGNED)`
  --let $mlk_round2 = `SELECT CAST(SUBSTRING_INDEX(SUBSTRING_INDEX('$mlk_env_rounds', ',', 2), ',', -1) AS UNSIGNED)`
  --let $mlk_round3 = `SELECT CAST(SUBSTRING_INDEX('$mlk_env_rounds', ',', -1) AS UNSIGNED)`
  --let $mlk_rounds_forced = 1
}
if (`SELECT LEAST($mlk_round1, $mlk_round2, $mlk_round3) < 1`)
{
  --die mem_leak_init: every round needs at least one execution ($mlk_round1/2/3, RONSQL_MEM_LEAK_ROUNDS)
}
# Signature of the current level (settle detection) and the number of
# (node, resource) rows above baseline + tolerance (verify).
--let $mlk_sig_sql = SELECT GROUP_CONCAT(CONCAT(node_id, ':', resource_name, '=', used) ORDER BY node_id, resource_name SEPARATOR ' ') FROM ndbinfo.resources WHERE resource_name IN ('QUERY_MEMORY', 'TRANSACTION_MEMORY')
--let $mlk_excess_sql = SELECT COUNT(*) FROM ndbinfo.resources AS r JOIN mlk_baseline AS b ON b.node_id = r.node_id AND b.resource_name = r.resource_name WHERE CAST(r.used AS SIGNED) > b.used + b.tolerance
--let $mlk_count = 0
--let $mlk_failures =
# Phase-2 parameters per registered shape (mem_leak_register.inc inserts,
# mem_leak_measure_all.inc reads).  A table, not per-shape include files:
# mysqltest replays a while loop from its line cache, so a "source" of a
# different file in each iteration would replay the first file.
--disable_warnings
--eval DROP TEMPORARY TABLE IF EXISTS mlk_shapes
--enable_warnings
--eval CREATE TEMPORARY TABLE mlk_shapes (k INT NOT NULL PRIMARY KEY, r1 INT NOT NULL, r2 INT NOT NULL, r3 INT NOT NULL, clients INT NOT NULL, accept_mode VARCHAR(16) NOT NULL, trace_on INT NOT NULL) ENGINE=MEMORY
if ($MLKE_TEST == '')
{
  --let MLKE_TEST = ronsql_large
}
--let MLKE_REPORT_LINE = ==== start: rounds $mlk_round1/$mlk_round2/$mlk_round3, warm-up $mlk_warmup x $mlk_warmup_clients clients, tolerance QM $mlk_qm_tolerance TM $mlk_tm_tolerance, first-use allowance QM $mlk_first_use_qm TM $mlk_first_use_tm (32 KB pages per data node) ====
--source suite/ronsql_large/include/mem_leak_report.inc
--enable_query_log

--echo
--echo === mem-leak harness: cluster warm-up (first CTE / SPJ lazy allocations happen here) ===
# Lazy one-off allocations of the first CTE query on a fresh cluster
# (QUERY_MEMORY +3, TRANSACTION_MEMORY +2..+4 pages per node), of the
# first pushed SPJ join, and the growth of RDRS's NDB object pool under
# concurrency happen here, before any baseline is taken.
--perl
use strict;
use warnings;
my $tmp = $ENV{MYSQL_TMP_DIR};
my %q = (
  w1 => "WITH cf AS (SELECT lg_cust AS k, COUNT(*) AS cnt, SUM(lg_amount) AS spend FROM lg_orders GROUP BY lg_cust) SELECT COUNT(*), SUM(cf.cnt), MAX(cf.spend), MIN(cf.spend) FROM cf;",
  w2 => "WITH cf AS (SELECT lg_cust AS k, COUNT(*) AS cnt, SUM(lg_qty) AS q FROM lg_orders GROUP BY lg_cust) SELECT c.lc_region, COUNT(*), SUM(cf.cnt), SUM(cf.q) FROM lg_cust AS c JOIN cf ON cf.k = c.lc_id GROUP BY c.lc_region;",
  w3 => "SELECT c.lc_id, o.lg_id FROM lg_cust AS c JOIN lg_orders AS o ON o.lg_cust = c.lc_id WHERE c.lc_id <= 200;",
);
for my $id (sort keys %q) {
  open(my $fh, '>', "$tmp/mlk_q$id.sql") or die "open: $!\n";
  print $fh "$q{$id}\n";
  close($fh);
  open($fh, '>', "$tmp/mlk_q$id.kind") or die "open: $!\n";
  print $fh "rdrs\n";
  close($fh);
}
EOF
--let MLKE_SHAPES = w1,w2,w3
--let MLKE_COUNT = 3
--let MLKE_CLIENTS = 1
--let MLKE_ACCEPT = ok
--let MLKE_LABEL = cluster warm-up
--let MLKE_QUIET = 0
--let MLKE_TRACE = 0
--let MLKE_CLEAR_ON_FAIL = 0
--source suite/ronsql_large/include/mem_leak_exec.inc
--let MLKE_COUNT = 1
--let MLKE_CLIENTS = $mlk_warmup_clients
--let MLKE_ACCEPT = ok_or_503
--source suite/ronsql_large/include/mem_leak_exec.inc
