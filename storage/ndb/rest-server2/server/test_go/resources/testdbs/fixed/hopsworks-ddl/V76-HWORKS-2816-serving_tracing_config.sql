-- Add per-deployment OTel tracing config for agent deployments.

ALTER TABLE `hopsworks`.`serving_depl_component`
    ADD COLUMN `tracing_config` VARCHAR(500) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL;
