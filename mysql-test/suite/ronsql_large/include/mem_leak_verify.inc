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
    --real_sleep 0.5
    --let $mlk_excess = `$mlk_excess_sql`
  }
}
--let MLKE_REPORT_LINE = `SELECT CONCAT('$mlk_tag ', IF($mlk_verify_failed, 'LEAK', 'ok'), ' after ', $mlk_polls * 0.5, ' s: ', GROUP_CONCAT(CONCAT('node ', r.node_id, ' ', IF(r.resource_name = 'QUERY_MEMORY', 'QM', 'TM'), ' ', b.used, '->', r.used, ' (', IF(CAST(r.used AS SIGNED) >= b.used, '+', ''), CAST(r.used AS SIGNED) - b.used, ')') ORDER BY r.node_id, r.resource_name SEPARATOR ', ')) FROM ndbinfo.resources AS r JOIN mlk_baseline AS b ON b.node_id = r.node_id AND b.resource_name = r.resource_name`
--source suite/ronsql_large/include/mem_leak_report.inc
if ($mlk_verify_failed)
{
  --let $mlk_residual = `SELECT GROUP_CONCAT(CONCAT('node ', r.node_id, ' ', IF(r.resource_name = 'QUERY_MEMORY', 'QM', 'TM'), ' +', CAST(r.used AS SIGNED) - b.used, ' pages') ORDER BY r.node_id, r.resource_name SEPARATOR ', ') FROM ndbinfo.resources AS r JOIN mlk_baseline AS b ON b.node_id = r.node_id AND b.resource_name = r.resource_name WHERE CAST(r.used AS SIGNED) > b.used + b.tolerance`
  --let $mlk_wait_s = `SELECT $mlk_polls * 0.5`
  --echo LEAK $mlk_tag: not back within tolerance after $mlk_wait_s s: $mlk_residual
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
