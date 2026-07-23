ALTER TABLE `hopsworks`.`cached_feature`
    MODIFY COLUMN `type` VARCHAR(20000) COLLATE latin1_general_cs NULL;