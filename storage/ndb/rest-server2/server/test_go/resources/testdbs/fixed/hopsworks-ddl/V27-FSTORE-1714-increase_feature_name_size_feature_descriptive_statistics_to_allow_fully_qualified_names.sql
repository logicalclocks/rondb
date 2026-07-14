-- Fully qualified names for features are in the form "project_name_feature_group_name_version_feature_name"
-- This migration increases the size of the feature_name column in the feature_descriptive_statistics to 100 + 1 + 63 + 1 + 3 + 1 + 63 = 232 characters 

ALTER TABLE `hopsworks`.`feature_descriptive_statistics`
   MODIFY `feature_name` VARCHAR(232) COLLATE latin1_general_cs NOT NULL;
