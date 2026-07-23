ALTER TABLE `hopsworks`.`cached_feature`
    ADD `type` varchar(1000) COLLATE latin1_general_cs NULL,
    ADD `partition_key` BOOLEAN NULL DEFAULT FALSE,
    ADD `default_value` VARCHAR(400) NULL,
    MODIFY `description` varchar(256) NULL DEFAULT '';

ALTER TABLE `hopsworks`.`feature_store_s3_connector`
    ADD `region` VARCHAR(50) DEFAULT NULL;

ALTER TABLE `hopsworks`.`feature_group`
    ADD `path` VARCHAR(1000) NULL,
    ADD `connector_id` INT(11) NULL,
    ADD CONSTRAINT `connector_fk` FOREIGN KEY (`connector_id`) REFERENCES `feature_store_connector` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION;

UPDATE `hopsworks`.`feature_group` AS fg
JOIN `hopsworks`.`on_demand_feature_group` AS on_demand_fg ON fg.`on_demand_feature_group_id` = on_demand_fg.`id`
SET fg.`path` = on_demand_fg.`path`,
    fg.`connector_id` = on_demand_fg.`connector_id`;

ALTER TABLE `hopsworks`.`on_demand_feature_group`
    DROP FOREIGN KEY `on_demand_conn_fk`,
    DROP COLUMN `path`,
    DROP COLUMN `connector_id`;