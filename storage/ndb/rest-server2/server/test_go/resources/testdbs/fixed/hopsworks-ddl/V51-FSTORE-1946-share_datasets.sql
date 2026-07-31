ALTER TABLE `hopsworks`.`dataset_shared_with`
    DROP FOREIGN KEY `fk_accepted_by`,
    DROP COLUMN `accepted`,
    DROP COLUMN `accepted_by`;
