DELIMITER $$

CREATE PROCEDURE CreateRondisTables(num_databases INT)
BEGIN
  DECLARE db_name VARCHAR(100);
  DECLARE done INT DEFAULT FALSE;
  DECLARE db_id INT DEFAULT 0;
  database_loop: LOOP
    SET @query = CONCAT('CREATE DATABASE IF NOT EXISTS redis_', CAST(db_id AS CHAR), ';');
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- Redis actually supports a max key size of 512MiB,
    -- but we choose not to support that here
    -- This is to save space when referencing the key in the value table
    -- TODO: Replace with Enum below
    -- value_data_type ENUM('string', 'number', 'binary_string'),
    -- Max 512MiB --> 512 * 1,048,576 bytes = 536,870,912 characters
    -- --> To describe the length, one needs at least UINT (4,294,967,295)
    -- Technically implicit
    -- Redis supports get/set of seconds/milliseconds
    -- Easier to sort and delete keys this way
    -- Each CHAR will use 1 byte
    SET @query = CONCAT('CREATE TABLE IF NOT EXISTS redis_', CAST(db_id AS CHAR), '.string_keys(
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
    ) ENGINE NDB CHARSET = latin1 COMMENT = "NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_8,TTL=0@expiry_date";');
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @query = CONCAT('CREATE TABLE IF NOT EXISTS redis_', CAST(db_id AS CHAR), '.hset_keys(
      redis_key VARBINARY(3000) NOT NULL,
      redis_key_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      PRIMARY KEY (redis_key) USING HASH,
      UNIQUE KEY (redis_key_id) USING HASH
    ) ENGINE NDB CHARSET latin1 COMMENT = "NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_8";');
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @query = CONCAT('CREATE TABLE IF NOT EXISTS redis_', CAST(db_id AS CHAR), '.string_values(
      rondb_key BIGINT UNSIGNED NOT NULL,
      ordinal INT UNSIGNED NOT NULL,
      expiry_date TIMESTAMP,
      value VARBINARY(29500) NOT NULL,
      KEY ttl_index(expiry_date),
      PRIMARY KEY (rondb_key, ordinal)
    ) ENGINE NDB CHARSET latin1 COMMENT = "NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_8,TTL=0@expiry_date";');
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET db_id = db_id + 1;
    -- Exit loop when reaching the target number of databases
    IF db_id = num_databases THEN
      LEAVE database_loop;
    END IF;
  END LOOP;

END $$

DELIMITER ;
