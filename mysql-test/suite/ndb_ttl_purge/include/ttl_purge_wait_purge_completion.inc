# suite/ndb_ttl_purge/include/ttl_purge_wait_purge_completion.inc
#
# Wait until the table looks empty to SQL (COUNT(*) = 0).
#
# NOTE: this proves SQL-visible emptiness only, NOT physical purge:
# COUNT(*) TTL-filters expired rows, so expired-but-unpurged rows are
# already invisible to it. Callers asserting actual purge activity must
# pair this with the purger's rows_purged metrics
# (ttl_purge_wait_rows_purged.inc), as all current callers do.
#
# Input:
#   $purge_db    - Database name
#   $purge_table - Table name
#   $purge_completion_timeout - Timeout in seconds (default: 60)

if (!$purge_completion_timeout)
{
  --let $purge_completion_timeout = 60
}

--let $wait_timeout = $purge_completion_timeout
--let $wait_condition = SELECT COUNT(*) = 0 FROM $purge_db.$purge_table
--source include/wait_condition.inc

if (!$success)
{
  --eval SELECT * FROM $purge_db.$purge_table
  --die Timeout waiting for purge completion on $purge_db.$purge_table
}

--let $purge_completion_timeout =
