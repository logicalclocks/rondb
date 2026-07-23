ALTER TABLE `hopsworks`.`serving_deployment`
    ADD COLUMN `version`   INT(11)    NOT NULL DEFAULT 1;

-- /*
--  * Migrate serving_model_artifact.artifact_version to serving_deployment.version
--  */
-- RDRS-P1-PORT: data-migration procedure block commented out (backfill of pre-existing rows; no-op on fresh test fixtures; DELIMITER/procedures cannot run through the test executor). Schema statements above/below are kept.
-- DROP PROCEDURE IF EXISTS `hopsworks`.MigrateArtifactVersionToDeployment;

-- DELIMITER $$

-- CREATE PROCEDURE `hopsworks`.MigrateArtifactVersionToDeployment()
-- BEGIN
--     DECLARE row_count INT DEFAULT 0;
--     DECLARE batch_size INT DEFAULT 500;
--     DECLARE offset_val INT DEFAULT 0;

--     -- Get total rows to process
--     SELECT COUNT(*) INTO row_count FROM `hopsworks`.`serving_model_artifact`;

--     WHILE offset_val < row_count
--         DO
--             START TRANSACTION;

--             -- Update serving_deployment.version from serving_model_artifact.artifact_version
--             UPDATE `hopsworks`.`serving_deployment` sd
--                 JOIN (SELECT sma.`serving_depl_id`, sma.`artifact_version`
--                       FROM `hopsworks`.`serving_model_artifact` sma
--                       ORDER BY sma.`id`
--                       LIMIT batch_size OFFSET offset_val) AS batch
--                 ON sd.`id` = batch.`serving_depl_id`
--             SET sd.`version` = CASE 
--                                   WHEN batch.`artifact_version` = 0 THEN 1
--                                   ELSE batch.`artifact_version`
--                                END;

--             SET offset_val = offset_val + batch_size;
--             COMMIT;
--         END WHILE;
-- END$$

-- DELIMITER ;

-- CALL `hopsworks`.MigrateArtifactVersionToDeployment();

-- DROP PROCEDURE `hopsworks`.MigrateArtifactVersionToDeployment;

-- /*
--  * End - Migrate artifact_version to serving_deployment.version
--  */

ALTER TABLE `hopsworks`.`serving_model_artifact`
    DROP COLUMN `artifact_version`;
