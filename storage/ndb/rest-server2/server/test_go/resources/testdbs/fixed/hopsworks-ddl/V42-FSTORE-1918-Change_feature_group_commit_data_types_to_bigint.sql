ALTER TABLE `hopsworks`.`feature_group_commit` MODIFY COLUMN `num_rows_updated` BIGINT DEFAULT '0';
ALTER TABLE `hopsworks`.`feature_group_commit` MODIFY COLUMN `num_rows_inserted` BIGINT DEFAULT '0';
ALTER TABLE `hopsworks`.`feature_group_commit` MODIFY COLUMN `num_rows_deleted` BIGINT DEFAULT '0';
