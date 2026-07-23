CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_store_glue_connector`
(
    `id`              INT(11)       NOT NULL AUTO_INCREMENT,
    `catalog_id`      VARCHAR(255)  DEFAULT NULL,
    `database_name`   VARCHAR(255)  NOT NULL,
    `region`          VARCHAR(255)  DEFAULT NULL,
    `iam_role`        VARCHAR(2048) DEFAULT NULL,
    `arguments`       VARCHAR(2000) DEFAULT NULL,
    `key_secret_uid`  INT           DEFAULT NULL,
    `key_secret_name` VARCHAR(200)  DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `fk_fs_storage_connector_glue_secret_idx` (`key_secret_uid`, `key_secret_name`),
    CONSTRAINT `fk_fs_storage_connector_glue_secret`
        FOREIGN KEY (`key_secret_uid`, `key_secret_name`)
            REFERENCES `hopsworks`.`secrets` (`uid`, `secret_name`) ON DELETE RESTRICT
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

ALTER TABLE `hopsworks`.`feature_store_connector`
    ADD COLUMN `glue_id` INT(11) DEFAULT NULL,
    ADD CONSTRAINT `fs_connector_glue_fk`
        FOREIGN KEY (`glue_id`)
            REFERENCES `hopsworks`.`feature_store_glue_connector` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION;
