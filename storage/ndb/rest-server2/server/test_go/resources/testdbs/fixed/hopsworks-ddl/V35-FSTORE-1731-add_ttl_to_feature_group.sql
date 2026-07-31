ALTER TABLE `hopsworks`.`feature_group`
    ADD COLUMN `ttl` int DEFAULT NULL,
    ADD COLUMN `ttl_enabled` tinyint(1) NOT NULL DEFAULT '0'; 
