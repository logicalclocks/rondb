ALTER TABLE `hopsworks`.`job_alert`
    ADD COLUMN `threshold` INT UNSIGNED;

ALTER TABLE `hopsworks`.`project_service_alert`
    ADD COLUMN `threshold` INT UNSIGNED;

CREATE TABLE IF NOT EXISTS `hopsworks`.`triggered_alert`
(
    `id`                        INT AUTO_INCREMENT PRIMARY KEY,
    `submission_time`           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `execution_id`              INT,
    `job_alert_id`              INT,
    `project_service_alert_id`  INT,
    CONSTRAINT `execution_id_fk` FOREIGN KEY (`execution_id`) REFERENCES `hopsworks`.`executions` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `job_alert_id_fk` FOREIGN KEY (`job_alert_id`) REFERENCES `hopsworks`.`job_alert` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `project_service_alert_id_fk` FOREIGN KEY (`project_service_alert_id`) REFERENCES `hopsworks`.`project_service_alert` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;
