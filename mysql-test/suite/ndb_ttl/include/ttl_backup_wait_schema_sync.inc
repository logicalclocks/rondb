# suite/ndb_ttl/include/ttl_backup_wait_schema_sync.inc
#
# Wait until a restored NDB table is visible and queryable.
# Handles NDB schema discovery delays after ndb_restore by
# polling with direct table access (which triggers auto-discovery).
#
# Input variables:
#   $ttl_sync_db      - database name
#   $ttl_sync_table   - table name
#   $ttl_sync_timeout - timeout in seconds (default 60)
#
# Prerequisites:
#   The database must exist on the mysqld side before calling this.
#   Use CREATE DATABASE IF NOT EXISTS before this include.
#
# Usage:
#   CREATE DATABASE IF NOT EXISTS ttl_backup;
#   --let $ttl_sync_db = ttl_backup
#   --let $ttl_sync_table = sz1
#   --source suite/ndb_ttl/include/ttl_backup_wait_schema_sync.inc

if (!$ttl_sync_timeout)
{
  --let $ttl_sync_timeout = 60
}

# Poll by attempting direct table access (triggers NDB auto-discovery)
--let $wait_timeout = $ttl_sync_timeout
--let $wait_condition = SELECT COUNT(*) >= 0 FROM $ttl_sync_db.$ttl_sync_table
--source include/wait_condition.inc

if (!$success)
{
  --die Timeout waiting for schema sync: $ttl_sync_db.$ttl_sync_table
}

# Buffer for transient error 1412 (table definition has changed)
sleep 3;

# Reset for next call
--let $ttl_sync_timeout =
