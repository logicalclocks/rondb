-- Add Oracle as a DatabaseType sub-type of SQL connector.
-- Oracle-specific wallet columns go on the connector (auth/connection concern).
-- Schema and table are NOT added here — they belong on the data_source table.

ALTER TABLE `hopsworks`.`feature_store_sql_connector`
  MODIFY COLUMN `database_type` ENUM('MYSQL','POSTGRESQL','ORACLE') NOT NULL,
  MODIFY COLUMN `host` varchar(128) DEFAULT NULL,
  ADD COLUMN `wallet_path` varchar(3000) DEFAULT NULL,
  ADD COLUMN `wallet_pwd_secret_uid` int DEFAULT NULL,
  ADD COLUMN `wallet_pwd_secret_name` varchar(200) DEFAULT NULL,
  ADD CONSTRAINT `fk_sql_connector_wallet_pwd_secret`
    FOREIGN KEY (`wallet_pwd_secret_uid`, `wallet_pwd_secret_name`)
    REFERENCES `hopsworks`.`secrets` (`uid`, `secret_name`) ON DELETE RESTRICT;
