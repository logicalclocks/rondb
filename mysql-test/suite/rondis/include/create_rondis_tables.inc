# Create Rondis tables for redis_0 database
# This include file creates the tables that Rondis needs to operate.
# Source this at the start of each test instead of relying on RDRS CreateTables config.

--disable_result_log
--disable_query_log

CREATE DATABASE IF NOT EXISTS redis_0;

CREATE TABLE IF NOT EXISTS redis_0.string_keys(
  redis_key_id BIGINT UNSIGNED NOT NULL,
  redis_key VARBINARY(3000) NOT NULL,
  rondb_key BIGINT UNSIGNED AUTO_INCREMENT NULL,
  value_data_type INT UNSIGNED NOT NULL,
  tot_value_len INT UNSIGNED NOT NULL,
  num_rows INT UNSIGNED NOT NULL,
  value_start VARBINARY(4096) NOT NULL,
  expiry_date TIMESTAMP,
  KEY ttl_index(expiry_date),
  PRIMARY KEY (redis_key_id, redis_key) USING HASH,
  UNIQUE KEY (rondb_key) USING HASH
) ENGINE NDB CHARSET=latin1 COMMENT="NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_8,TTL=0@expiry_date";

CREATE TABLE IF NOT EXISTS redis_0.hset_keys(
  redis_key VARBINARY(3000) NOT NULL,
  redis_key_id BIGINT UNSIGNED NULL DEFAULT NULL,
  field_count INT UNSIGNED NOT NULL DEFAULT 0,
  expiry_date TIMESTAMP,
  KEY ttl_index(expiry_date),
  PRIMARY KEY (redis_key) USING HASH,
  UNIQUE KEY (redis_key_id) USING HASH
) ENGINE NDB CHARSET=latin1 COMMENT="NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_8,TTL=0@expiry_date";

CREATE TABLE IF NOT EXISTS redis_0.hset_key_id_sequence(
  redis_key_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (redis_key_id) USING HASH
) ENGINE NDB CHARSET=latin1;

CREATE TABLE IF NOT EXISTS redis_0.string_values(
  rondb_key BIGINT UNSIGNED NOT NULL,
  ordinal INT UNSIGNED NOT NULL,
  expiry_date TIMESTAMP,
  value VARBINARY(29500) NOT NULL,
  KEY ttl_index(expiry_date),
  PRIMARY KEY (rondb_key, ordinal)
) ENGINE NDB CHARSET=latin1 COMMENT="NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_8,TTL=0@expiry_date";

--enable_query_log
--enable_result_log
