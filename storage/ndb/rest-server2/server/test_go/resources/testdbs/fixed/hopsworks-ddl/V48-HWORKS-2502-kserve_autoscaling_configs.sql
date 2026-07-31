ALTER TABLE `hopsworks`.`serving_depl_component` ADD COLUMN `scaling_config` varchar(500) CHARACTER SET latin1 COLLATE
  latin1_general_cs DEFAULT NULL;

-- RDRS-P1-PORT: data-migration procedure block commented out (backfill of pre-existing rows; no-op on fresh test fixtures; DELIMITER/procedures cannot run through the test executor). Schema statements above/below are kept.
-- DROP PROCEDURE IF EXISTS `hopsworks`.populate_scaling_config;

-- DELIMITER $$

-- CREATE PROCEDURE `hopsworks`.populate_scaling_config()
-- BEGIN
--   UPDATE `hopsworks`.`serving_depl_component`
--   SET `scaling_config` = CONCAT(
--     '{"minInstances": ', `num_instances`, '}'
--   )
--   WHERE `num_instances` IS NOT NULL;
-- END$$

-- DELIMITER ;

-- CALL `hopsworks`.populate_scaling_config();

-- DROP PROCEDURE `hopsworks`.populate_scaling_config;

ALTER TABLE `hopsworks`.`serving_depl_component` DROP COLUMN `num_instances`;
