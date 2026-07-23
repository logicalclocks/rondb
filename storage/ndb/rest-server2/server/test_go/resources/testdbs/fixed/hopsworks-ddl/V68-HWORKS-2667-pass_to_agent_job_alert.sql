ALTER TABLE `hopsworks`.`job_alert`
    ADD COLUMN `pass_to_agent` TINYINT(1) NOT NULL DEFAULT 0;
