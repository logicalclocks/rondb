ALTER TABLE `hopsworks`.`feature_group`
    DROP FOREIGN KEY `connector_fk`,
    DROP COLUMN `connector_id`;

ALTER TABLE `hopsworks`.`training_dataset`
    ADD `data_source_id`    INT(11),
    ADD CONSTRAINT `data_source_td_fk` FOREIGN KEY (`data_source_id`) REFERENCES `data_source` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION;

-- migrate data source for training datasets
ALTER TABLE `hopsworks`.`data_source`
    ADD `td_id`    INT(11); -- tmp removed after migration

INSERT INTO `hopsworks`.`data_source` (`path`, `connector_id`, `td_id`)
SELECT
    td.`connector_path`,
    td.`connector_id`,
    td.`id`
FROM
    `hopsworks`.`training_dataset` AS td;

SET SQL_SAFE_UPDATES = 0;
UPDATE `hopsworks`.`training_dataset` AS td
JOIN `hopsworks`.`data_source` AS ds
ON td.`id` = ds.`td_id`
SET td.`data_source_id` = ds.`id`;
SET SQL_SAFE_UPDATES = 1;

-- remove unnecessary columns
ALTER TABLE `hopsworks`.`training_dataset`
    DROP FOREIGN KEY `td_conn_fk`,
    DROP COLUMN `connector_id`,
    DROP COLUMN `connector_path`;

ALTER TABLE `hopsworks`.`data_source`
    DROP COLUMN `td_id`;
