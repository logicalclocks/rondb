CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_store_opensearch_connector`
(
    `id`                      INT(11)       NOT NULL AUTO_INCREMENT,
    `host`                   VARCHAR(2000) NOT NULL,
    `port`                    INT(11)       NOT NULL,
    `scheme`                  VARCHAR(10)   NOT NULL,
    `verify`                  TINYINT(1)    DEFAULT 1,
    `username`                VARCHAR(255)  DEFAULT NULL,
    `arguments`               VARCHAR(2000) DEFAULT NULL,
    `truststore_path`         VARCHAR(2000) DEFAULT NULL,
    `credential_secret_uid`   INT           DEFAULT NULL,
    `credential_secret_name`  VARCHAR(200)  DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `fk_fs_storage_connector_opensearch_secret_idx` (`credential_secret_uid`, `credential_secret_name`),
    CONSTRAINT `fk_fs_storage_connector_opensearch_secret`
        FOREIGN KEY (`credential_secret_uid`, `credential_secret_name`)
            REFERENCES `hopsworks`.`secrets` (`uid`, `secret_name`) ON DELETE RESTRICT
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

ALTER TABLE `hopsworks`.`feature_store_connector`
    ADD COLUMN `opensearch_id` INT(11) DEFAULT NULL,
    ADD CONSTRAINT `fs_connector_opensearch_fk`
        FOREIGN KEY (`opensearch_id`)
            REFERENCES `hopsworks`.`feature_store_opensearch_connector` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION;
