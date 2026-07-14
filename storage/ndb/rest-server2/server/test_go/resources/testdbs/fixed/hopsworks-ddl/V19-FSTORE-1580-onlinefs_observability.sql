CREATE TABLE IF NOT EXISTS `hopsworks`.`online_ingestion`
(
    `id`                  INT(11) AUTO_INCREMENT PRIMARY KEY,
    `feature_group_id`    INT(11),
    `num_entries`         bigint,
    CONSTRAINT `oi_feature_group_fk` FOREIGN KEY (`feature_group_id`) REFERENCES `hopsworks`.`feature_group` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

ALTER TABLE `hopsworks`.`feature_store_activity` 
  ADD COLUMN `online_ingestion_id` INT(11) DEFAULT NULL,
  ADD CONSTRAINT `fs_act_online_ingestion_fk` FOREIGN KEY (`online_ingestion_id`) REFERENCES `hopsworks`.`online_ingestion` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION;

-- to make sure that online_ingestion_batch_result is created
UPDATE `hopsworks`.`project` set `online_feature_store_available`=0;
