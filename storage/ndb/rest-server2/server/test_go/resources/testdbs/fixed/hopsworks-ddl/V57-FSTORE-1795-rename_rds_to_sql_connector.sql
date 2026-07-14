-- Rename feature_store_rds_connector table to feature_store_sql_connector
RENAME TABLE `hopsworks`.`feature_store_rds_connector` TO `hopsworks`.`feature_store_sql_connector`;

-- Add database_type column to distinguish MySQL vs PostgreSQL
ALTER TABLE `hopsworks`.`feature_store_sql_connector`
    ADD COLUMN `database_type` ENUM('MYSQL', 'POSTGRESQL') NOT NULL DEFAULT 'POSTGRESQL';

-- Rename rds_id column to sql_id on feature_store_connector
ALTER TABLE `hopsworks`.`feature_store_connector`
    DROP FOREIGN KEY `fs_connector_rds_fk`;

ALTER TABLE `hopsworks`.`feature_store_connector`
    CHANGE COLUMN `rds_id` `sql_id` INT(11);

ALTER TABLE `hopsworks`.`feature_store_connector`
    ADD CONSTRAINT `fs_connector_sql_fk` FOREIGN KEY (`sql_id`) REFERENCES `hopsworks`.`feature_store_sql_connector` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION;
