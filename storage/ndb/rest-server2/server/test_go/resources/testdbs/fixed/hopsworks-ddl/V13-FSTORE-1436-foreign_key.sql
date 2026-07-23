ALTER TABLE `hopsworks`.`cached_feature_extra_constraints`
    ADD COLUMN `foreign_column` tinyint(1) DEFAULT 0;

ALTER TABLE `hopsworks`.`on_demand_feature`
    ADD COLUMN `foreign_column` tinyint(1) DEFAULT 0;
