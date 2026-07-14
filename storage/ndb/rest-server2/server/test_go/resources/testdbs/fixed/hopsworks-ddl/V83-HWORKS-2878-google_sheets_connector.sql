SET SQL_SAFE_UPDATES = 0;
ALTER TABLE `hopsworks`.`data_source` ADD COLUMN `spreadsheet_id` VARCHAR(500) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL;
SET SQL_SAFE_UPDATES = 1;

CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_store_google_sheets_connector` (
  `id`              INT(11)       NOT NULL AUTO_INCREMENT,
  `spreadsheet_id`  VARCHAR(500)  DEFAULT NULL,
  `key_secret_uid`  INT(11)       DEFAULT NULL,
  `key_secret_name` VARCHAR(200)  DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `fk_fs_google_sheets_connector_secret_idx` (`key_secret_uid`, `key_secret_name`),
  CONSTRAINT `fk_fs_google_sheets_connector_secret`
    FOREIGN KEY (`key_secret_uid`, `key_secret_name`)
      REFERENCES `hopsworks`.`secrets` (`uid`, `secret_name`) ON DELETE RESTRICT
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

ALTER TABLE `hopsworks`.`feature_store_connector`
  ADD COLUMN `google_sheets_id` int(11) DEFAULT NULL,
  ADD CONSTRAINT `fsc_google_sheets_fk` FOREIGN KEY (`google_sheets_id`)
    REFERENCES `hopsworks`.`feature_store_google_sheets_connector` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION;
