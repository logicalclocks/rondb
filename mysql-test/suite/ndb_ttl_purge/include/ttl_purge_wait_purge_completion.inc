# suite/ndb_ttl_purge/include/ttl_purge_wait_purge_completion.inc
#
# Wait until all rows are purged from a table (row count = 0).
# Uses SELECT * and counts in perl to avoid COUNT(*) TTL visibility bug.
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
