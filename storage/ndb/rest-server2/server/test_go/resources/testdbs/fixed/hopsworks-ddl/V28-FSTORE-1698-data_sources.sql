-- extension on top of storage connector (storage connector deals with authentication while this specifies the data location)
CREATE TABLE IF NOT EXISTS `hopsworks`.`data_source`
(
    `id`                    INT(11) AUTO_INCREMENT PRIMARY KEY,
    -- query
    `query`                 VARCHAR(26000), -- defaults to using the query if provided (no support for dynamic queries)
    `database_name`         VARCHAR(100),
    `group_name`            VARCHAR(100),
    `table_name`            VARCHAR(100),
    -- path
    `path`                  VARCHAR(1000),
    -- ref
    `connector_id`          INT(11),
    `fg_id`                 INT(11), -- tmp removed after migration
    CONSTRAINT `ds_connector_fk` FOREIGN KEY (`connector_id`) REFERENCES `feature_store_connector` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

ALTER TABLE `hopsworks`.`feature_group`
    ADD `data_source_id`    INT(11),
    ADD CONSTRAINT `data_source_fk` FOREIGN KEY (`data_source_id`) REFERENCES `data_source` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION;

-- populate data
INSERT INTO `hopsworks`.`data_source` (`query`, `database_name`, `group_name`, `table_name`, `path`, `connector_id`, `fg_id`)
SELECT 
    odfg.`query`,
    COALESCE(
        redshift.`database_name`,
        bigquery.`query_project`,
        snowflake.`database_name`
    ) AS `database_name`,
    COALESCE(
        redshift.`database_group`,
        bigquery.`dataset`,
        snowflake.`database_schema`
    ) AS `group_name`,
    COALESCE(
        redshift.`table_name`,
        bigquery.`query_table`,
        snowflake.`table_name`
    ) AS `table_name`,
    fg.`path`,
    fg.`connector_id`,
    fg.`id`
FROM 
    `hopsworks`.`feature_group` AS fg
LEFT JOIN 
    `hopsworks`.`on_demand_feature_group` AS odfg
ON 
    fg.`on_demand_feature_group_id` = odfg.`id`
LEFT JOIN 
    `hopsworks`.`feature_store_connector` AS fsc
ON 
    fg.`connector_id` = fsc.`id`
LEFT JOIN 
    `hopsworks`.`feature_store_redshift_connector` AS redshift
ON 
    fsc.`redshift_id` = redshift.`id`
LEFT JOIN 
    `hopsworks`.`feature_store_bigquery_connector` AS bigquery
ON 
    fsc.`bigquery_id` = bigquery.`id`
LEFT JOIN 
    `hopsworks`.`feature_store_snowflake_connector` AS snowflake
ON 
    fsc.`snowflake_id` = snowflake.`id`;

SET SQL_SAFE_UPDATES = 0;
UPDATE `hopsworks`.`feature_group` AS fg
JOIN `hopsworks`.`data_source` AS ds
ON fg.`id` = ds.`fg_id`
SET fg.`data_source_id` = ds.`id`;
SET SQL_SAFE_UPDATES = 1;

-- remove unecassary columns
ALTER TABLE `hopsworks`.`data_source`
    DROP COLUMN `fg_id`;

ALTER TABLE `hopsworks`.`on_demand_feature_group`
    DROP COLUMN `query`;

ALTER TABLE `hopsworks`.`feature_group`
    DROP COLUMN `path`;

-- add column_name to on demand fg
ALTER TABLE `hopsworks`.`on_demand_feature`
    ADD COLUMN `column_name` VARCHAR(1000);

CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_store_rds_connector`
(
    `id`                        int             NOT NULL AUTO_INCREMENT,
    `host`                      varchar(128)    NOT NULL,
    `port`                      int,
    `database_name`             varchar(128),
    `database_user`             varchar(128),
    `arguments`                 varchar(2000),
    `database_pwd_secret_uid`   int,
    `database_pwd_secret_name`  varchar(200),
    PRIMARY KEY (`id`),
    KEY `fk_feature_store_rds_connector_idx` (`database_pwd_secret_uid`, `database_pwd_secret_name`),
    CONSTRAINT `fk_feature_store_rds_connector` FOREIGN KEY (`database_pwd_secret_uid`, `database_pwd_secret_name`)
        REFERENCES `hopsworks`.`secrets` (`uid`, `secret_name`) ON DELETE RESTRICT
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

ALTER TABLE `hopsworks`.`feature_store_connector` 
    ADD COLUMN `rds_id` INT(11),
    ADD CONSTRAINT `fs_connector_rds_fk` FOREIGN KEY (`rds_id`) REFERENCES `hopsworks`.`feature_store_rds_connector` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION;

ALTER TABLE `hopsworks`.`feature_store_snowflake_connector`
    MODIFY COLUMN `database_schema` VARCHAR(45) DEFAULT NULL;
