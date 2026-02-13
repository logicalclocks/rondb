# suite/ndb_ttl_purge/include/ttl_purge_setup.inc
#
# Define curl command variables for TTL purge API endpoints.
# Requires: $RDRS_HOST, $RDRS_PORT (from [ENV] section in my.cnf)

--source suite/ndb_ttl_purge/include/have_rdrs_curl.inc

--let $ttl_purge_base = http://$RDRS_HOST:$RDRS_PORT/0.1.0/ttl-purge

--let $ttl_purge_get_config  = curl -s $ttl_purge_base/config
--let $ttl_purge_put_config  = curl -s -X PUT -H "Content-Type: application/json" $ttl_purge_base/config -d
--let $ttl_purge_get_status  = curl -s $ttl_purge_base/status
--let $ttl_purge_get_metrics = curl -s $ttl_purge_base/metrics
--let $ttl_purge_get_tables  = curl -s $ttl_purge_base/tables
--let $ttl_purge_get_all     = curl -s $ttl_purge_base
