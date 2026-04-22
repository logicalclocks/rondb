# Common setup for Rondis MTR tests that drive redis-cli directly.
#
# - Creates the three Rondis tables in redis_0 (idempotent).
# - Pings Rondis to fail fast if redis-cli is missing (exit 127) or the
#   RDRS/Rondis port is unreachable (connection refused).
#
# Test files source this right after have_ndb.inc / not_parallel.inc.

--source suite/rondis/include/create_rondis_tables.inc
--exec redis-cli -h $RONDIS_HOST -p $RONDIS_PORT PING > /dev/null
