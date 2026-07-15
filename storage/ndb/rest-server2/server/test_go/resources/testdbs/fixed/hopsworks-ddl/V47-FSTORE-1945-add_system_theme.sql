CREATE TABLE IF NOT EXISTS `hopsworks`.`system_theme` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `theme_data` MEDIUMTEXT NOT NULL,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=ndbcluster
  DEFAULT CHARSET=latin1
  COLLATE=latin1_general_cs;

-- Add feature flag for enabling custom branding variable
-- RDRS-P1-PORT: data-producing INSERT commented out. In this test pipeline the
-- migrations are DDL-only; the rows this statement produces are captured in
-- fixed/hopsworks-data/hopsworks_data.sql (dumped from a fully-migrated DB) and
-- would collide on re-seed.
-- INSERT INTO `hopsworks`.`variables` (`id`, `value`, `visibility`, `hide`)
-- VALUES ('enable_custom_branding', 'false', 0, 0)
-- ON DUPLICATE KEY UPDATE `id`=`id`;
