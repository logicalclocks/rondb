-- RDRS-P1-PORT: data-migration procedure only (backfills USER scope onto pre-existing serving api keys); no schema change; no-op on fresh test fixtures; procedures cannot run through the test executor's ';'-splitting. Entire file commented out.
-- DROP PROCEDURE IF EXISTS `hopsworks`.UpdateServingKeyScope;

-- DELIMITER $$

-- CREATE PROCEDURE `hopsworks`.UpdateServingKeyScope()
-- BEGIN
--     SET @row_count := (SELECT COUNT(`id`) from `hopsworks`.`api_key` WHERE (`name` LIKE 'serving%' AND `reserved`= 1));
--     SET @batch_size = 500;
--     SET @offset = 1;
--     CREATE TEMPORARY TABLE `hopsworks`.`serving_key_scopes` (SELECT *, ROW_NUMBER() OVER (ORDER BY `id`) as `row_num` from `api_key` WHERE (`name` LIKE 'serving%' AND `reserved`= 1));
--     WHILE @offset <= @row_count DO
--         -- Manually starting a transaction so that each batch insert occurs as one transaction. This prevents a MaxNoOfConcurrentOp error for occurring.
--         START TRANSACTION;
--         INSERT INTO `hopsworks`.`api_key_scope` (`api_key`, `scope`) (SELECT `id`, 'USER' from serving_key_scopes WHERE `row_num` BETWEEN @offset AND @offset + @batch_size - 1)
--         ON DUPLICATE KEY UPDATE `api_key`=VALUES(`api_key`), `scope`=VALUES(`scope`);
--         SET @offset = @offset + @batch_size;
--         COMMIT;
--     END WHILE;
--     DROP table `hopsworks`.`serving_key_scopes`;
-- END$$

-- DELIMITER ;

-- CALL `hopsworks`.UpdateServingKeyScope();

-- DROP PROCEDURE `hopsworks`.UpdateServingKeyScope;
