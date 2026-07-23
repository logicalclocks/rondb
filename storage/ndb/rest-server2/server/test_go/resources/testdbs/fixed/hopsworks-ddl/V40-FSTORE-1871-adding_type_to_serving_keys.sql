ALTER TABLE `hopsworks`.`serving_key`
   ADD `type` VARCHAR(1000);

UPDATE `hopsworks`.`serving_key` AS `sk`
JOIN `hopsworks`.`feature_group` AS `fg`
  ON `sk`.`feature_group_id` = `fg`.`id`
LEFT JOIN `hopsworks`.`cached_feature` AS `cf`
  ON (
    (`fg`.`cached_feature_group_id` = `cf`.`cached_feature_group_id` AND `cf`.`name` = `sk`.`feature_name`)
    OR (`fg`.`stream_feature_group_id` = `cf`.`stream_feature_group_id` AND `cf`.`name` = `sk`.`feature_name`)
  )
LEFT JOIN `hopsworks`.`on_demand_feature` AS `odf`
  ON `fg`.`on_demand_feature_group_id` = `odf`.`on_demand_feature_group_id`
  AND `odf`.`name` = `sk`.`feature_name`
SET `sk`.`type` = COALESCE(`odf`.`type`, `cf`.`type`);
