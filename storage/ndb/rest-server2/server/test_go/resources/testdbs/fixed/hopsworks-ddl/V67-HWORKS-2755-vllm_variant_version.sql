-- HWORKS-2755: Add vLLM variant and image tag fields to serving_deployment_component

ALTER TABLE `hopsworks`.`serving_depl_component`
    ADD COLUMN `vllm_variant`   VARCHAR(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL DEFAULT 'VLLM',
    ADD COLUMN `vllm_image_tag` VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL;

-- Backfill: all existing vLLM rows implicitly used vllm-omni (the only image shipped pre-feature)
-- model_server ordinal 2 = VLLM (ModelServer enum)
UPDATE `hopsworks`.`serving_depl_component`
SET `vllm_variant` = 'VLLM_OMNI'
WHERE `model_server` = 2;
