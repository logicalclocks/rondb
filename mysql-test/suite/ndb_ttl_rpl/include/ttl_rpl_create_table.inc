# suite/ndb_ttl_rpl/include/ttl_rpl_create_table.inc
#
# Create the global TTL test table test.sz used by most replication tests.
#
# Schema:
#   col_a INT PRIMARY KEY
#   col_b TIMESTAMP       -- TTL column
#   col_c INT UNIQUE KEY
#
# TTL defaults to 10 seconds on col_b (matching original Python test timing).
# A test that needs a longer, jitter-tolerant TTL can override it by setting
#   --let $ttl_rpl_ttl = <seconds>
# before sourcing this file (see ttl_rpl_upsert.test).
#
# The table is first created with TTL=66s to prevent premature
# expiry during setup, then altered to the test TTL value.

if (!$ttl_rpl_ttl)
{
  --let $ttl_rpl_ttl = 10
}

--disable_warnings
DROP TABLE IF EXISTS test.sz;
--enable_warnings

CREATE TABLE test.sz (
    col_a INT,
    col_b TIMESTAMP,
    col_c INT,
    PRIMARY KEY(col_a),
    UNIQUE KEY(col_c)
) ENGINE = NDB,
  COMMENT="NDB_TABLE=NOLOGGING=0,TTL=66@col_b";

# Wait for schema to propagate to all nodes
sleep 3;

# Switch to the test TTL value (eval expands $ttl_rpl_ttl in the query)
eval ALTER TABLE test.sz COMMENT="NDB_TABLE=NOLOGGING=0,TTL=$ttl_rpl_ttl@col_b";

# Wait for ALTER to propagate
sleep 3;

# Reset for the next caller
--let $ttl_rpl_ttl =
