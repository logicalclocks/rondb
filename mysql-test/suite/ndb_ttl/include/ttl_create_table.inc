# suite/ndb_ttl/include/ttl_create_table.inc
#
# Create the global TTL test table test.sz used by most tests.
#
# Schema:
#   col_a INT PRIMARY KEY
#   col_b TIMESTAMP       -- TTL column
#   col_c INT UNIQUE KEY
#
# TTL is set to 5 seconds on col_b.
#
# The table is first created with TTL=66s to prevent premature
# expiry during setup, then altered to the test TTL value.

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
  COMMENT="NDB_TABLE=TTL=66@col_b";

# Wait for schema to propagate to all nodes
sleep 3;

# Switch to the test TTL value
ALTER TABLE test.sz COMMENT="NDB_TABLE=TTL=5@col_b";

# Wait for ALTER to propagate
sleep 3;
