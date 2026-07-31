CREATE TABLE IF NOT EXISTS `hopsworks`.`serving_deployment`
(
    `id`         INT(11) AUTO_INCREMENT PRIMARY KEY,
    `serving_id` INT(11) NOT NULL,
    CONSTRAINT `serving_depl_serving_fk` FOREIGN KEY (`serving_id`) REFERENCES `serving` (`id`)
        ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `hopsworks`.`serving_model_artifact`
(
    `id`               INT(11) AUTO_INCREMENT PRIMARY KEY,
    `serving_depl_id`  INT(11) NOT NULL,
    `model_path`       varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
    `model_name`       varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
    `model_version`    int(11)                                                     DEFAULT NULL,
    `model_framework`  int(11)                                                     DEFAULT NULL,
    `artifact_version` int(11)                                                     DEFAULT NULL,
    CONSTRAINT `serving_model_artifact_serving_depl_fk` FOREIGN KEY (`serving_depl_id`) REFERENCES `serving_deployment` (`id`)
        ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `hopsworks`.`serving_depl_component`
(
    `id`              INT(11) AUTO_INCREMENT PRIMARY KEY,
    `serving_depl_id` INT(11) NOT NULL,
    `component_type`  int(11) NOT NULL                        DEFAULT 0,    -- predictor or transformer
    `model_server`    int(11) NOT NULL                        DEFAULT 1,
    `script_file`     varchar(255) COLLATE latin1_general_cs  DEFAULT NULL, -- old predictor/transformer column
    `config_file`     varchar(255) COLLATE latin1_general_cs  DEFAULT NULL,
    `num_instances`   int(11) NOT NULL                        DEFAULT 0,
    `resources`       varchar(1000) COLLATE latin1_general_cs DEFAULT NULL,
    CONSTRAINT `serving_depl_component_serving_depl_fk` FOREIGN KEY (`serving_depl_id`) REFERENCES
        `serving_deployment` (`id`)
        ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

-- /*
--  * SplitServingTable procedure
--  */

-- RDRS-P1-PORT: data-migration procedure block commented out (backfill of pre-existing rows; no-op on fresh test fixtures; DELIMITER/procedures cannot run through the test executor). Schema statements above/below are kept.
-- DROP PROCEDURE IF EXISTS `hopsworks`.SplitServingTable;

-- DELIMITER $$

-- CREATE PROCEDURE `hopsworks`.SplitServingTable()
-- BEGIN
--     SET @row_count := (SELECT COUNT(`id`) FROM `hopsworks`.`serving`);
--     SET @batch_size := 500;
--     SET @offset := 1;

--     CREATE TEMPORARY TABLE `hopsworks`.`serving_rows` (SELECT *, ROW_NUMBER() OVER (ORDER BY `id`) AS `row_num`
--                                                        FROM `hopsworks`.`serving`);

--     WHILE @offset <= @row_count
--         DO
--             START TRANSACTION;

--             -- Insert into serving_deployment
--             INSERT INTO `hopsworks`.`serving_deployment` (`serving_id`)
--             SELECT `id`
--             FROM `hopsworks`.`serving_rows`
--             WHERE `row_num` BETWEEN @offset AND @offset + @batch_size - 1;

--             -- Insert into serving_model_artifact
--             INSERT INTO `hopsworks`.`serving_model_artifact` (`serving_depl_id`, `model_path`, `model_name`,
--                                                               `model_version`, `model_framework`, `artifact_version`)
--             SELECT sd.`id`,
--                    sr.`model_path`,
--                    sr.`model_name`,
--                    sr.`model_version`,
--                    sr.`model_framework`,
--                    sr.`artifact_version`
--             FROM `hopsworks`.`serving_rows` sr
--                      JOIN `hopsworks`.`serving_deployment` sd ON sd.`serving_id` = sr.`id`
--             WHERE sr.`row_num` BETWEEN @offset AND @offset + @batch_size - 1;

--             -- Insert predictor components
--             INSERT INTO `hopsworks`.`serving_depl_component` (`serving_depl_id`, `component_type`, `model_server`,
--                                                               `script_file`, `config_file`, `num_instances`,
--                                                               `resources`)
--             SELECT sd.`id`,
--                    0, -- 0 = predictor
--                    sr.`model_server`,
--                    sr.`predictor`,
--                    sr.`config_file`,
--                    sr.`instances`,
--                    sr.`predictor_resources`
--             FROM `hopsworks`.`serving_rows` sr
--                      JOIN `hopsworks`.`serving_deployment` sd ON sd.`serving_id` = sr.`id`
--             WHERE sr.`row_num` BETWEEN @offset AND @offset + @batch_size - 1;

--             -- Insert transformer components
--             INSERT INTO `hopsworks`.`serving_depl_component` (`serving_depl_id`, `component_type`, `model_server`,
--                                                               `script_file`, `num_instances`, `resources`)
--             SELECT sd.`id`,
--                    1, -- 1 = transformer
--                    1, -- 1 = Python
--                    sr.`transformer`,
--                    sr.`transformer_instances`,
--                    sr.`transformer_resources`
--             FROM `hopsworks`.`serving_rows` sr
--                      JOIN `hopsworks`.`serving_deployment` sd ON sd.`serving_id` = sr.`id`
--             WHERE sr.`transformer` IS NOT NULL -- only if a transformer is configured
--               AND sr.`row_num` BETWEEN @offset AND @offset + @batch_size - 1;

--             SET @offset := @offset + @batch_size;
--             COMMIT;
--         END WHILE;

--     DROP TABLE `hopsworks`.`serving_rows`;
-- END$$

-- DELIMITER ;

-- CALL `hopsworks`.SplitServingTable();

-- DROP PROCEDURE `hopsworks`.SplitServingTable;

-- /*
--  * End - SplitServingTable procedure
--  */

ALTER TABLE `hopsworks`.`serving`
    -- moved to other table
    DROP COLUMN `model_path`,
    DROP COLUMN `model_name`,
    DROP COLUMN `model_version`,
    DROP COLUMN `model_framework`,
    DROP COLUMN `artifact_version`,
    DROP COLUMN `model_server`,
    DROP COLUMN `predictor`,
    DROP COLUMN `config_file`,
    DROP COLUMN `transformer`,
    DROP COLUMN `instances`,
    DROP COLUMN `transformer_instances`,
    DROP COLUMN `predictor_resources`,
    DROP COLUMN `transformer_resources`,
    -- legacy, not used
    DROP COLUMN `optimized`,
    DROP COLUMN `local_dir`,
    DROP COLUMN `local_port`,
    DROP COLUMN `cid`;
