-- Cluster-level config: name of the project whose materialised feature groups
-- are auto-imported into a user's project when they hit the wizard's
-- "import public dataset" action (see FeaturestoreService#importDefaults).
INSERT INTO `hopsworks`.`variables` (`id`, `value`, `visibility`, `hide`)
VALUES ('default_featurestore_project_name', 'hopsworks_default', 0, 0)
ON DUPLICATE KEY UPDATE `id`=`id`;
