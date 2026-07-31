ALTER TABLE `hopsworks`.`feature_store_snowflake_connector`
   ADD COLUMN `passphrase_secret_uid` int;

ALTER TABLE `hopsworks`.`feature_store_snowflake_connector`
    ADD COLUMN `passphrase_secret_name` VARCHAR(256);

ALTER TABLE `hopsworks`.`feature_store_snowflake_connector`
    ADD COLUMN `privatekey_secret_uid` int;

ALTER TABLE `hopsworks`.`feature_store_snowflake_connector`
    ADD COLUMN `privatekey_secret_name` VARCHAR(5000);
