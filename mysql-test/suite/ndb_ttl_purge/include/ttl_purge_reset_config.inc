# suite/ndb_ttl_purge/include/ttl_purge_reset_config.inc
#
# Restore default purge configuration.
# Default: enabled=true, min_batch=5, max_batch=50, sleep=1500ms
#
# Requires: ttl_purge_setup.inc sourced

--exec $ttl_purge_put_config '{"enabled":true,"min_batch_size":5,"max_batch_size":50,"sleep_interval_ms":1500}' > /dev/null 2>&1
--sleep 1
