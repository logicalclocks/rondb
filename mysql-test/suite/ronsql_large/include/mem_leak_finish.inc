# mem_leak_finish.inc -- end of a memory-leak harness test: record-level
# DUMP checks, the verdict over all collected failures, cleanup of the
# harness files (only on success, so a failed run keeps its reference and
# mismatch files in $MYSQL_TMP_DIR).  Source with the default delimiter.
--echo
--echo === mem-leak harness: record-level leak checks ===
--source suite/ronsql_large/include/mem_leak_dumps.inc
if ($mlk_failures != '')
{
  --echo mem-leak harness: data-node memory growth detected:$mlk_failures
  # --die does not expand variables; the line above lists the failures.
  --die Data-node memory growth detected (see the LEAK lines), per-round levels and per-execution QUERY_MEMORY totals are in var/log/ronsql_large_mem_leak.report
}
--echo mem-leak harness: $mlk_count shape(s) measured, no data-node memory growth
--source suite/ronsql_large/include/mem_leak_cleanup.inc
