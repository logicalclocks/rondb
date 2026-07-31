ALTER TABLE `hopsworks`.`schemas` DROP FOREIGN KEY project_idx_schemas;
ALTER TABLE `hopsworks`.`schemas` MODIFY COLUMN `schema` TEXT CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL;