CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_store_metrics_data` (
  `id` INT(11) AUTO_INCREMENT PRIMARY KEY,
  `metric` INT NOT NULL,
  `project` INT NOT NULL,
  `timestamp` BIGINT NOT NULL,
  `value` VARCHAR(64) NOT NULL,
   KEY `project` (`project`),
   CONSTRAINT `fsm_project_fk` FOREIGN KEY (`project`) REFERENCES `project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
 ) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

 CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_store_metrics_event_log` (
   `id` INT(11) AUTO_INCREMENT PRIMARY KEY,
   `event_type` VARCHAR(255) NOT NULL,
   `timestamp` BIGINT NOT NULL,
   `project` INT NOT NULL,
   `args` VARCHAR(2000),
   `status` VARCHAR(255) NOT NULL DEFAULT 'QUEUED',
   `error_msg` VARCHAR(2000),
   KEY `project` (`project`),
   CONSTRAINT `fsm_events_project_fk` FOREIGN KEY (`project`) REFERENCES `project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
 ) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

ALTER TABLE `hopsworks`.`feature_group_commit` ADD COLUMN `table_size_bytes` BIGINT DEFAULT 0;


