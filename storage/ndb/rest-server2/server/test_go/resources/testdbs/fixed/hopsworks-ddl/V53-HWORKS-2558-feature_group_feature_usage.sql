CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_group_feature_usage` (
  `feature_group_id` INT(11) NOT NULL,
  `feature_name` VARCHAR(1000) COLLATE latin1_general_cs NOT NULL,
  `source_feature_group_id` INT(11) NOT NULL,
  `source_feature_name` VARCHAR(1000) COLLATE latin1_general_cs NOT NULL,
  `feature_views_count` INT(11) NOT NULL DEFAULT 0,
  `models_count` INT(11) NOT NULL DEFAULT 0,
  `derived_feature_groups_count` INT(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`feature_group_id`, `feature_name`),
  KEY `feature_group_feature_usage_fg` (`feature_group_id`),
  KEY `feature_group_feature_usage_source_feature` (`source_feature_group_id`, `source_feature_name`)
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_group_feature_usage_feature_view` (
  `feature_group_id` INT(11) NOT NULL,
  `feature_name` VARCHAR(1000) COLLATE latin1_general_cs NOT NULL,
  `fv_id` INT(11) NOT NULL,
  `fv_version` INT(11) NOT NULL,
  `fv_name` VARCHAR(1000) COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`feature_group_id`, `feature_name`, `fv_id`, `fv_version`),
  KEY `feature_group_feature_usage_fv_fk` (`feature_group_id`, `feature_name`),
  KEY `feature_group_feature_usage_fv_id_idx` (`fv_id`),
  CONSTRAINT `feature_group_feature_usage_fv_fk`
    FOREIGN KEY (`feature_group_id`, `feature_name`) REFERENCES `feature_group_feature_usage` (`feature_group_id`, `feature_name`)
    ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_group_feature_usage_model` (
  `feature_group_id` INT(11) NOT NULL,
  `feature_name` VARCHAR(1000) COLLATE latin1_general_cs NOT NULL,
  `model_id` INT(11) NOT NULL,
  `model_version` INT(11) NOT NULL,
  `model_version_id` INT(11) NOT NULL,
  `model_name` VARCHAR(1000) COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`feature_group_id`, `feature_name`, `model_id`, `model_version`),
  KEY `feature_group_feature_usage_model_fk` (`feature_group_id`, `feature_name`),
  KEY `feature_group_feature_usage_model_version_id_idx` (`model_version_id`),
  CONSTRAINT `feature_group_feature_usage_model_fk`
    FOREIGN KEY (`feature_group_id`, `feature_name`) REFERENCES `feature_group_feature_usage` (`feature_group_id`, `feature_name`)
    ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_group_feature_usage_derived_feature_group` (
  `feature_group_id` INT(11) NOT NULL,
  `feature_name` VARCHAR(1000) COLLATE latin1_general_cs NOT NULL,
  `derived_feature_group_id` INT(11) NOT NULL,
  `derived_feature_group_version` INT(11) NOT NULL,
  `derived_feature_group_name` VARCHAR(1000) COLLATE latin1_general_cs NOT NULL,
  PRIMARY KEY (`feature_group_id`, `feature_name`, `derived_feature_group_id`, `derived_feature_group_version`),
  KEY `feature_group_feature_usage_dfg_fk` (`feature_group_id`, `feature_name`),
  KEY `feature_group_feature_usage_dfg_id_idx` (`derived_feature_group_id`),
  CONSTRAINT `feature_group_feature_usage_dfg_fk`
    FOREIGN KEY (`feature_group_id`, `feature_name`) REFERENCES `feature_group_feature_usage` (`feature_group_id`, `feature_name`)
    ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;
