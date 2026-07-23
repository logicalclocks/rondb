CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_store_mongo_connector`
(
    `id`                       INT(11)       NOT NULL AUTO_INCREMENT,
    `connection_string`        VARCHAR(3000) NOT NULL,
    `database_name`            VARCHAR(255)  NOT NULL,
    `collection_name`          VARCHAR(255)  DEFAULT NULL,
    `username`                 VARCHAR(255)  DEFAULT NULL,
    `auth_source`              VARCHAR(64)   DEFAULT NULL,
    `auth_mechanism`           VARCHAR(64)   DEFAULT NULL,
    `arguments`                VARCHAR(8000) DEFAULT NULL,
    `database_pwd_secret_uid`  INT           DEFAULT NULL,
    `database_pwd_secret_name` VARCHAR(200)  DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `fk_fs_storage_connector_mongo_secret_idx` (`database_pwd_secret_uid`, `database_pwd_secret_name`),
    CONSTRAINT `fk_fs_storage_connector_mongo_secret`
        FOREIGN KEY (`database_pwd_secret_uid`, `database_pwd_secret_name`)
            REFERENCES `hopsworks`.`secrets` (`uid`, `secret_name`) ON DELETE RESTRICT
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

ALTER TABLE `hopsworks`.`feature_store_connector`
    ADD COLUMN `mongo_id` INT(11) DEFAULT NULL,
    ADD CONSTRAINT `fs_connector_mongo_fk`
        FOREIGN KEY (`mongo_id`)
            REFERENCES `hopsworks`.`feature_store_mongo_connector` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION;
