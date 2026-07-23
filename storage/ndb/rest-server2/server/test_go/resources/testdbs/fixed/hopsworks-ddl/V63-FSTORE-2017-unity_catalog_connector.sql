-- Add Unity Catalog (Databricks) storage connector.
-- Stores workspace URL, optional default catalog, optional explicit AWS region;
-- access token is stored encrypted in the `secrets` table, referenced by a
-- composite FK. When `aws_region` is NULL the Arrow Flight read path infers the
-- region from the STS session-token Databricks returns.

CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_store_unity_catalog_connector`
(
    `id`                 INT(11)       NOT NULL AUTO_INCREMENT,
    `workspace_url`      VARCHAR(2048) NOT NULL,
    `default_catalog`    VARCHAR(128)  DEFAULT NULL,
    `aws_region`         VARCHAR(32)   DEFAULT NULL,
    `arguments`          VARCHAR(2000) DEFAULT NULL,
    `token_secret_uid`   INT           DEFAULT NULL,
    `token_secret_name`  VARCHAR(200)  DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `fk_feature_store_unity_catalog_connector_secret_idx` (`token_secret_uid`, `token_secret_name`),
    CONSTRAINT `fk_feature_store_unity_catalog_connector_secret`
        FOREIGN KEY (`token_secret_uid`, `token_secret_name`)
        REFERENCES `hopsworks`.`secrets` (`uid`, `secret_name`) ON DELETE RESTRICT
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

ALTER TABLE `hopsworks`.`feature_store_connector`
    ADD COLUMN `unity_catalog_id` INT(11) DEFAULT NULL,
    ADD CONSTRAINT `fs_connector_unity_catalog_fk`
        FOREIGN KEY (`unity_catalog_id`)
        REFERENCES `hopsworks`.`feature_store_unity_catalog_connector` (`id`)
        ON DELETE CASCADE ON UPDATE NO ACTION;
