# suite/ndb_ttl_purge/include/ttl_purge_create_table.inc
#
# Create a TTL table with configurable parameters.
#
# Input:
#   $ttl_db         - Database name (required)
#   $ttl_table      - Table name (required)
#   $ttl_seconds    - TTL in seconds (required)
#   $ttl_col_type   - Column type: DATETIME or TIMESTAMP (default: DATETIME)
#   $ttl_extra_cols - Extra column definitions (default: data VARCHAR(100))
#   $ttl_create_index - Whether to create ttl_index (default: 1)
#   $ttl_partitions - Number of partitions (default: 0 = no partitioning)

if (!$ttl_col_type)
{
  --let $ttl_col_type = DATETIME
}
if (!$ttl_extra_cols)
{
  --let $ttl_extra_cols = data VARCHAR(100)
}
if (!$ttl_create_index)
{
  --let $ttl_create_index = 1
}
if (!$ttl_partitions)
{
  --let $ttl_partitions = 0
}

--eval CREATE DATABASE IF NOT EXISTS $ttl_db

--disable_warnings
--eval DROP TABLE IF EXISTS $ttl_db.$ttl_table
--enable_warnings

--let $partition_clause =
if ($ttl_partitions)
{
  --let $partition_clause = PARTITION BY KEY(id) PARTITIONS $ttl_partitions
}

if ($ttl_partitions)
{
  # For partitioned tables, create directly with final TTL
  # (ALTER TABLE with PARTITION_BALANCE changes is problematic)
  --eval CREATE TABLE $ttl_db.$ttl_table (id INT PRIMARY KEY, ttl_col $ttl_col_type, $ttl_extra_cols) ENGINE=NDB COMMENT="NDB_TABLE=TTL=$ttl_seconds@ttl_col" $partition_clause

  if ($ttl_create_index)
  {
    --eval CREATE INDEX ttl_index ON $ttl_db.$ttl_table(ttl_col)
  }

  --sleep 3
}

if (!$ttl_partitions)
{
  # Non-partitioned: create with long TTL first, then ALTER to actual value
  --eval CREATE TABLE $ttl_db.$ttl_table (id INT PRIMARY KEY, ttl_col $ttl_col_type, $ttl_extra_cols) ENGINE=NDB COMMENT="NDB_TABLE=TTL=3600@ttl_col"

  if ($ttl_create_index)
  {
    --eval CREATE INDEX ttl_index ON $ttl_db.$ttl_table(ttl_col)
  }

  --sleep 2

  --eval ALTER TABLE $ttl_db.$ttl_table COMMENT="NDB_TABLE=TTL=$ttl_seconds@ttl_col"

  --sleep 2
}

# Reset optional parameters
--let $ttl_col_type =
--let $ttl_extra_cols =
--let $ttl_create_index =
--let $ttl_partitions =
