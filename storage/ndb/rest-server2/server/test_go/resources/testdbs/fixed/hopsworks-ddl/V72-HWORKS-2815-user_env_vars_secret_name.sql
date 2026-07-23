ALTER TABLE `hopsworks`.`user_env_vars`
  MODIFY COLUMN `env_value` VARBINARY(10000) NULL,
  ADD COLUMN `secret_name` VARCHAR(255) NULL AFTER `env_value`,
  ADD COLUMN `visibility` TINYINT(1) NOT NULL DEFAULT 0 AFTER `secret_name`,
  ADD COLUMN `project_id_scope` INT(11) DEFAULT NULL AFTER `visibility`,
  ADD KEY `user_env_vars_project_visible_idx` (`project_id_scope`, `visibility`);
