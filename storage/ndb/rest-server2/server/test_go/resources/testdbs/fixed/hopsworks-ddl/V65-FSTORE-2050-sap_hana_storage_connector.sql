CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_store_sap_hana_connector`
(
    `id`                       INT(11)       NOT NULL AUTO_INCREMENT,
    `host`                     VARCHAR(2000) NOT NULL,
    `port`                     INT(11)       NOT NULL,
    `database_name`            VARCHAR(255)  DEFAULT NULL,
    `database_schema`          VARCHAR(255)  DEFAULT NULL,
    `table_name`               VARCHAR(255)  DEFAULT NULL,
    `database_user`            VARCHAR(255)  NOT NULL,
    `application`              VARCHAR(50)   DEFAULT NULL,
    `arguments`                VARCHAR(8000) DEFAULT NULL,
    `database_pwd_secret_uid`  INT           DEFAULT NULL,
    `database_pwd_secret_name` VARCHAR(200)  DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `fk_fs_storage_connector_sap_hana_secret_idx` (`database_pwd_secret_uid`, `database_pwd_secret_name`),
    CONSTRAINT `fk_fs_storage_connector_sap_hana_secret`
        FOREIGN KEY (`database_pwd_secret_uid`, `database_pwd_secret_name`)
            REFERENCES `hopsworks`.`secrets` (`uid`, `secret_name`) ON DELETE RESTRICT
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

ALTER TABLE `hopsworks`.`feature_store_connector`
    ADD COLUMN `sap_hana_id` INT(11) DEFAULT NULL,
    ADD CONSTRAINT `fs_connector_sap_hana_fk`
        FOREIGN KEY (`sap_hana_id`)
            REFERENCES `hopsworks`.`feature_store_sap_hana_connector` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION;
