-- Add git source fields to agent serving deployments
ALTER TABLE hopsworks.serving_depl_component
    ADD COLUMN git_url VARCHAR(512) DEFAULT NULL,
    ADD COLUMN git_branch VARCHAR(255) DEFAULT NULL,
    ADD COLUMN git_provider VARCHAR(255) DEFAULT NULL;
