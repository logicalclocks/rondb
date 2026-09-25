# mem_leak_snapshot.inc -- wait until the data nodes' QUERY_MEMORY and
# TRANSACTION_MEMORY use has settled, then record it per node as the
# baseline (temporary table mlk_baseline, 32 KB pages).
#
# Settled = $mlk_settle_reads identical readings of ndbinfo.resources
# (node_id, resource_name, used) 0.5 s apart, i.e. 5 s by default.  Query
# memory is partly released asynchronously after a query returns: DBSPJ
# keeps freed row pages in a cache that a 100 ms timer trims to zero
# within about 3 s, and join-aggregation teardown and redistribution page
# chains are freed by CONTINUEB chains.  An idle level only goes down, so
# a baseline taken too early is too high (a leak could hide), never too
# low (no false alarm).  After $mlk_settle_max_polls readings the current
# level is used and the report line says UNSETTLED.
#
# Inputs: $mlk_tol_qm / $mlk_tol_tm (pages per node that mem_leak_verify.inc
# will tolerate above this baseline), $mlk_tag (report), $mlk_snapshot_label
# (printed).  Initialised by mem_leak_init.inc.
--disable_query_log
--let $mlk_polls = 0
--let $mlk_stable = 0
--let $mlk_unsettled = 0
--let $mlk_prev = `$mlk_sig_sql`
while ($mlk_stable < $mlk_settle_reads)
{
  --sleep 0.5
  --let $mlk_now = `$mlk_sig_sql`
  if ($mlk_now == $mlk_prev)
  {
    --inc $mlk_stable
  }
  if ($mlk_now != $mlk_prev)
  {
    --let $mlk_stable = 0
    --let $mlk_prev = $mlk_now
  }
  --inc $mlk_polls
  if ($mlk_polls >= $mlk_settle_max_polls)
  {
    --let $mlk_unsettled = 1
    --let $mlk_stable = $mlk_settle_reads
  }
}
--disable_warnings
--eval DROP TEMPORARY TABLE IF EXISTS mlk_baseline
--enable_warnings
--eval CREATE TEMPORARY TABLE mlk_baseline ENGINE=MEMORY SELECT node_id, resource_name, CAST(used AS SIGNED) AS used, IF(resource_name = 'QUERY_MEMORY', $mlk_tol_qm, $mlk_tol_tm) AS tolerance FROM ndbinfo.resources WHERE resource_name IN ('QUERY_MEMORY', 'TRANSACTION_MEMORY')
--let MLKE_REPORT_LINE = `SELECT CONCAT('$mlk_tag baseline', IF($mlk_unsettled, ' (UNSETTLED)', ''), ' after ', $mlk_polls * 0.5, ' s: ', GROUP_CONCAT(CONCAT('node ', node_id, ' ', IF(resource_name = 'QUERY_MEMORY', 'QM', 'TM'), '=', used) ORDER BY node_id, resource_name SEPARATOR ', '), ' (tolerance QM $mlk_tol_qm TM $mlk_tol_tm)') FROM mlk_baseline`
--source suite/ronsql_large/include/mem_leak_report.inc
--let $mlk_nodes = `SELECT COUNT(*) FROM mlk_baseline WHERE resource_name = 'QUERY_MEMORY'`
--enable_query_log
--echo $mlk_snapshot_label: QUERY_MEMORY and TRANSACTION_MEMORY recorded on all $mlk_nodes data nodes
