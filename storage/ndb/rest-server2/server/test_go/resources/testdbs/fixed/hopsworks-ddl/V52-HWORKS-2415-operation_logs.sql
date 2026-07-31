CREATE TABLE IF NOT EXISTS `hopsworks`.`operation_log`
(
    `id`                           INT           NOT NULL AUTO_INCREMENT,
    `operation`                    VARCHAR(128)  NOT NULL,
    `category`                     VARCHAR(45)   NOT NULL,
    `created_on`                   TIMESTAMP(4)  NOT NULL DEFAULT CURRENT_TIMESTAMP(4),
    `user_id`                      INT           DEFAULT NULL,
    `username`                     VARCHAR(10)   DEFAULT NULL,
    `user_groups`                  VARCHAR(256)  DEFAULT NULL,
    `project_id`                   INT           DEFAULT NULL,
    `project_name`                 VARCHAR(100)  DEFAULT NULL,
    `member_role`                  VARCHAR(32)   DEFAULT NULL,
    `is_service_user`              TINYINT       NOT NULL DEFAULT '0',
    `online_fs_db`                 VARCHAR(100)  DEFAULT NULL,
    `online_fs_user`               VARCHAR(200)  DEFAULT NULL,
    `shared_dataset_id`            INT           DEFAULT NULL,
    `shared_dataset_name`          VARCHAR(255)  DEFAULT NULL,
    `shared_with_project_id`       INT           DEFAULT NULL,
    `shared_with_project_name`     VARCHAR(100)  DEFAULT NULL,
    `permission`                   VARCHAR(45)   DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `hopsworks`.`operation_log_history`
(
    `id`                           INT           NOT NULL AUTO_INCREMENT,
    `operation`                    VARCHAR(128)  NOT NULL,
    `category`                     VARCHAR(45)   NOT NULL,
    `created_on`                   TIMESTAMP(4)  NOT NULL DEFAULT CURRENT_TIMESTAMP(4),
    `user_id`                      INT           DEFAULT NULL,
    `username`                     VARCHAR(10)   DEFAULT NULL,
    `user_groups`                  VARCHAR(256)  DEFAULT NULL,
    `project_id`                   INT           DEFAULT NULL,
    `project_name`                 VARCHAR(100)  DEFAULT NULL,
    `member_role`                  VARCHAR(32)   DEFAULT NULL,
    `is_service_user`              TINYINT       NOT NULL DEFAULT '0',
    `online_fs_db`                 VARCHAR(100)  DEFAULT NULL,
    `online_fs_user`               VARCHAR(200)  DEFAULT NULL,
    `shared_dataset_id`            INT           DEFAULT NULL,
    `shared_dataset_name`          VARCHAR(255)  DEFAULT NULL,
    `shared_with_project_id`       INT           DEFAULT NULL,
    `shared_with_project_name`     VARCHAR(100)  DEFAULT NULL,
    `permission`                   VARCHAR(45)   DEFAULT NULL,
    `operation_log_id`             INT           NOT NULL,
    PRIMARY KEY (`id`),
    KEY `index2` (`created_on`)
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE `hopsworks`.`operation_log_service_status` (
  `id`                        INT           NOT NULL AUTO_INCREMENT,
  `service`                   VARCHAR(45)   NOT NULL,
  `status`                    VARCHAR(20)   NOT NULL,
  `updated_on`                TIMESTAMP(4)  NOT NULL DEFAULT CURRENT_TIMESTAMP(4) ON UPDATE CURRENT_TIMESTAMP(4),
  `error_message`             VARCHAR(5000) NULL,
  `status_message`            VARCHAR(5000) NULL,
  `operation_log_id`          INT           NULL,
  `operation_log_history_id`  INT           NULL,
  `attempt_count`             INT           NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  INDEX `fk_operation_log_service_status_1_idx` (`operation_log_id`),
  INDEX `fk_operation_log_service_status_2_idx` (`operation_log_history_id`),
  UNIQUE KEY `unique_idx_1` (`service`, `operation_log_id`),
  UNIQUE KEY `unique_idx_2` (`service`, `operation_log_history_id`),
  CONSTRAINT `fk_operation_log_service_status_1`
    FOREIGN KEY (`operation_log_id`)
    REFERENCES `hopsworks`.`operation_log` (`id`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_operation_log_service_status_2`
    FOREIGN KEY (`operation_log_history_id`)
    REFERENCES `hopsworks`.`operation_log_history` (`id`)
    ON DELETE CASCADE
    ON UPDATE NO ACTION
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;
