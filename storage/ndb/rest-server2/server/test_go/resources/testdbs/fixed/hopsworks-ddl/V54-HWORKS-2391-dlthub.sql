CREATE TABLE `hopsworks`.`feature_store_crm_connector` (
  `id` int NOT NULL AUTO_INCREMENT,
  `crm_type` int NOT NULL,
  `key_secret_uid` int DEFAULT NULL,
  `key_secret_name` varchar(200) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_feature_store_crm_connector_1_idx` (`key_secret_uid`,`key_secret_name`),
  CONSTRAINT `fk_feature_store_crm_connector_secret` FOREIGN KEY (`key_secret_uid`, `key_secret_name`) REFERENCES `secrets` (`uid`, `secret_name`) ON DELETE RESTRICT
) ENGINE=ndbcluster AUTO_INCREMENT=2049 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

ALTER TABLE `hopsworks`.`data_source` ADD COLUMN `metrics` VARCHAR(1000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL;
ALTER TABLE `hopsworks`.`data_source` ADD COLUMN `dimensions` VARCHAR(1000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL;

ALTER TABLE `hopsworks`.`feature_store_connector`
  ADD `crm_id` int DEFAULT NULL,
  ADD KEY `fk_feature_store_connector_crm_idx` (`crm_id`),
  ADD CONSTRAINT `fs_connector_crm_fk` FOREIGN KEY (`crm_id`) REFERENCES `feature_store_crm_connector` (`id`) ON DELETE CASCADE;

ALTER TABLE `hopsworks`.`feature_group`
  ADD COLUMN `sink_enabled` tinyint(1) NOT NULL DEFAULT '0';

CREATE TABLE `hopsworks`.`feature_store_rest_connector` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `key_secret_uid` int DEFAULT NULL,
  `key_secret_name` VARCHAR(200) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
  `auth_type` INT NOT NULL,
  `client_config` VARCHAR(5000) NULL,
  PRIMARY KEY (`id`),
  KEY `fk_feature_store_rest_connector_secret_idx` (`key_secret_uid`, `key_secret_name`),
  CONSTRAINT `fk_feature_store_rest_connector_secret` FOREIGN KEY (`key_secret_uid`, `key_secret_name`) REFERENCES `secrets` (`uid`, `secret_name`) ON DELETE RESTRICT
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

ALTER TABLE `hopsworks`.`feature_store_connector`
  ADD `rest_id` int DEFAULT NULL,
  ADD KEY `fk_feature_store_connector_rest_idx` (`rest_id`),
  ADD CONSTRAINT `fs_connector_rest_fk` FOREIGN KEY (`rest_id`) REFERENCES `feature_store_rest_connector` (`id`) ON DELETE CASCADE;
