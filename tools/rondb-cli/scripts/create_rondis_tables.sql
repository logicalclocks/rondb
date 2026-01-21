CREATE TABLE IF NOT EXISTS string_keys(
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
) ENGINE NDB CHARSET = latin1;
CREATE TABLE IF NOT EXISTS hset_keys(
  redis_key VARBINARY(3000) NOT NULL,
  redis_key_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (redis_key) USING HASH,
  UNIQUE KEY (redis_key_id) USING HASH
) ENGINE NDB CHARSET latin1;
CREATE TABLE IF NOT EXISTS string_values(
  rondb_key BIGINT UNSIGNED NOT NULL,
  ordinal INT UNSIGNED NOT NULL,
  expiry_date TIMESTAMP,
  value VARBINARY(29500) NOT NULL,
  KEY ttl_index(expiry_date),
  PRIMARY KEY (rondb_key, ordinal)
) ENGINE NDB CHARSET latin1;

