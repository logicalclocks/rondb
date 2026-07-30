
-- default query string to NULL for compatibility before 4.2
ALTER TABLE hopsworks.on_demand_feature_group
    MODIFY COLUMN query VARCHAR(26000) DEFAULT NULL;
