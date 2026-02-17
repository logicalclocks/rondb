# suite/ndb_ttl/include/ttl_wait_expired.inc
#
# Poll until the given wait condition becomes true, intended for
# waiting until TTL-expired rows become invisible.
#
# USAGE
#
#   --let $ttl_wait_condition = SELECT COUNT(*) = 0 FROM test.sz WHERE col_a = 1
#   --source suite/ndb_ttl/include/ttl_wait_expired.inc
#
# Optional: override the default timeout (default 10 seconds)
#   --let $ttl_wait_timeout = 20
#   --let $ttl_wait_condition = ...
#   --source suite/ndb_ttl/include/ttl_wait_expired.inc

if (!$ttl_wait_timeout)
{
  --let $ttl_wait_timeout = 10
}

--let $wait_timeout = $ttl_wait_timeout
--let $wait_condition = $ttl_wait_condition
--source include/wait_condition.inc

if (!$success)
{
  --die Timeout waiting for TTL expiry: $ttl_wait_condition
}

# Reset for next call
--let $ttl_wait_timeout =
