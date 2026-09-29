# mem_leak_verify.inc -- poll ndbinfo.resources every 0.5 s until every
# data node's QUERY_MEMORY and TRANSACTION_MEMORY use is back at or below
# the mlk_baseline level + tolerance (mem_leak_snapshot.inc), for at most
# $mlk_verify_max_polls polls (60 s by default: the DBSPJ row-page cache
# drains within about 3 s, teardown CONTINUEB chains within a second).
#
# On success prints "$mlk_verify_label: ... $mlk_verify_what on all N data
# nodes".  On timeout prints "LEAK <tag>: <residual>" plus a per-node table
# (baseline, now, delta in pages and KB, tolerance), and then fails the
# test at once ($mlk_collect = 0) or records the residual in $mlk_failures
# for mem_leak_finish.inc ($mlk_collect = 1).
#
# How to read a residual: pages are 32 KB, per data node.  A step of 64
# pages (2 MB) or a multiple is a pinned lc_ndbd_pool segment, i.e. at
# least one leaked pool allocation on that LDM / TC thread.  A residual
# of a few pages is a record pool page (RWPool / TransientPool) kept by a
# leaked record, or a one-off per-instance page that the warm-up did not
# reach -- compare the rounds in the report file: a leak grows with the
# number of executions, a one-off step does not.
#
# Reclaim re-check.  A spare lc_ndbd_pool segment is not a leak: when two
# threads need a new segment of the same pool at once, both can be added
# (ndbd_malloc_impl.cpp, lc_mempool_long_lived_pool_malloc), and a segment
# is released only by a free that leaves it fully free, so one that
# nothing allocated from stays until the pool's next allocation: a +64
# QUERY_MEMORY page step on one data node that survives any idle time
# (ronsql_large_mem_leak_errors mle-e5126, 2026-09-25).  Join aggregation
# groups come from the pool of the DblqhProxy thread, and at idle the
# spare is that pool's only segment, so the next join aggregation
# allocates from it and releases it.  So when the timeout leaves only
# QUERY_MEMORY steps of 64 pages or more, two plain join aggregations run
# through RDRS and the level gets another $mlk_recheck_max_polls polls; a
# leaked allocation keeps its segment and still fails.  The re-check only
# shows in the report file.
--disable_query_log
--let $mlk_polls = 0
--let $mlk_verify_failed = 0
--let $mlk_excess = `$mlk_excess_sql`
while ($mlk_excess)
{
  --inc $mlk_polls
  if ($mlk_polls >= $mlk_verify_max_polls)
  {
    --let $mlk_verify_failed = 1
    --let $mlk_excess = 0
  }
  if ($mlk_excess)
  {
    --sleep 0.5
    --let $mlk_excess = `$mlk_excess_sql`
  }
}
--let $mlk_reclaim = 0
--let $mlk_note =
if ($mlk_verify_failed)
{
  --let $mlk_reclaim = `SELECT COUNT(*) > 0 AND SUM(r.resource_name <> 'QUERY_MEMORY' OR CAST(r.used AS SIGNED) - b.used < 64) = 0 FROM ndbinfo.resources AS r JOIN mlk_baseline AS b ON b.node_id = r.node_id AND b.resource_name = r.resource_name WHERE CAST(r.used AS SIGNED) > b.used + b.tolerance`
  --let $mlk_before = `SELECT GROUP_CONCAT(CONCAT('node ', r.node_id, ' ', IF(r.resource_name = 'QUERY_MEMORY', 'QM', 'TM'), ' +', CAST(r.used AS SIGNED) - b.used) ORDER BY r.node_id, r.resource_name SEPARATOR ', ') FROM ndbinfo.resources AS r JOIN mlk_baseline AS b ON b.node_id = r.node_id AND b.resource_name = r.resource_name WHERE CAST(r.used AS SIGNED) > b.used + b.tolerance`
  --let $mlk_note = , no reclaim query (not only QUERY_MEMORY steps of 64 pages or more)
}
# The perl block runs on every verify and does nothing unless
# MLKE_RECLAIM is set (a "perl" block is not put inside an if).
--let MLKE_RECLAIM = $mlk_reclaim
--let MLKE_RECLAIM_TAG = $mlk_tag
--perl
use strict;
use warnings;
exit(0) unless $ENV{MLKE_RECLAIM};
my $body = "$ENV{MYSQL_TMP_DIR}/mlk_reclaim.body";
die "mem_leak_verify: $body is missing (mem_leak_init.inc writes it)\n"
  unless -e $body;
my $url = 'http://' . ($ENV{RDRS_NOKEY_HOST} // '') . ':'
        . ($ENV{RDRS_NOKEY_PORT} // '') . '/0.1.0/ronsql';
my @codes;
for my $i (1 .. 2) {
  my $code = `curl -s -o /dev/null --max-time 300 -w '%{http_code}' -X POST -H 'Content-Type: application/json' --data-binary \@$body $url 2>/dev/null`;
  $code = 'curl-exit-' . ($? >> 8) if $?;
  push @codes, $code;
}
my $f = "$ENV{MYSQLTEST_VARDIR}/log/ronsql_large_mem_leak.report";
open(my $fh, '>>', $f) or die "mem_leak_verify: open $f: $!\n";
my @t = localtime();
printf $fh "%02d:%02d:%02d %s %s reclaim query: 2 x plain join "
  . "aggregation, HTTP %s\n", $t[2], $t[1], $t[0],
  ($ENV{MLKE_TEST} // '?'), ($ENV{MLKE_RECLAIM_TAG} // '?'),
  join(' ', @codes);
close($fh);
EOF
--let $mlk_recheck_polls = 0
--let $mlk_excess = $mlk_reclaim
while ($mlk_excess)
{
  --sleep 0.5
  --inc $mlk_recheck_polls
  --let $mlk_excess = `$mlk_excess_sql`
  if (!$mlk_excess)
  {
    --let $mlk_verify_failed = 0
  }
  if ($mlk_recheck_polls >= $mlk_recheck_max_polls)
  {
    --let $mlk_excess = 0
  }
}
if ($mlk_reclaim)
{
  --let $mlk_note = `SELECT CONCAT(', then the reclaim query and ', $mlk_recheck_polls * 0.5, ' s more (before the reclaim query: $mlk_before)')`
}
--let MLKE_REPORT_LINE = `SELECT CONCAT('$mlk_tag ', IF($mlk_verify_failed, 'LEAK', 'ok'), ' after ', $mlk_polls * 0.5, ' s$mlk_note: ', GROUP_CONCAT(CONCAT('node ', r.node_id, ' ', IF(r.resource_name = 'QUERY_MEMORY', 'QM', 'TM'), ' ', b.used, '->', r.used, ' (', IF(CAST(r.used AS SIGNED) >= b.used, '+', ''), CAST(r.used AS SIGNED) - b.used, ')') ORDER BY r.node_id, r.resource_name SEPARATOR ', ')) FROM ndbinfo.resources AS r JOIN mlk_baseline AS b ON b.node_id = r.node_id AND b.resource_name = r.resource_name`
--source suite/ronsql_large/include/mem_leak_report.inc
if ($mlk_verify_failed)
{
  --let $mlk_residual = `SELECT GROUP_CONCAT(CONCAT('node ', r.node_id, ' ', IF(r.resource_name = 'QUERY_MEMORY', 'QM', 'TM'), ' +', CAST(r.used AS SIGNED) - b.used, ' pages') ORDER BY r.node_id, r.resource_name SEPARATOR ', ') FROM ndbinfo.resources AS r JOIN mlk_baseline AS b ON b.node_id = r.node_id AND b.resource_name = r.resource_name WHERE CAST(r.used AS SIGNED) > b.used + b.tolerance`
  --let $mlk_wait_s = `SELECT $mlk_polls * 0.5`
  --echo LEAK $mlk_tag: not back within tolerance after $mlk_wait_s s$mlk_note: $mlk_residual
  --eval SELECT r.node_id, r.resource_name, b.used AS baseline_pages, r.used AS now_pages, CAST(r.used AS SIGNED) - b.used AS delta_pages, (CAST(r.used AS SIGNED) - b.used) * 32 AS delta_kb, b.tolerance AS tolerance_pages FROM ndbinfo.resources AS r JOIN mlk_baseline AS b ON b.node_id = r.node_id AND b.resource_name = r.resource_name ORDER BY r.node_id, r.resource_name
  if (!$mlk_collect)
  {
    # --die does not expand variables: the LEAK line above has the details.
    --die Data-node memory not released, see the LEAK line and the table above and var/log/ronsql_large_mem_leak.report
  }
  --let $mlk_failures = $mlk_failures [$mlk_tag: $mlk_residual]
}
if (!$mlk_verify_failed)
{
  --let $mlk_nodes = `SELECT COUNT(*) FROM mlk_baseline WHERE resource_name = 'QUERY_MEMORY'`
  --echo $mlk_verify_label: QUERY_MEMORY and TRANSACTION_MEMORY $mlk_verify_what on all $mlk_nodes data nodes
}
--enable_query_log
