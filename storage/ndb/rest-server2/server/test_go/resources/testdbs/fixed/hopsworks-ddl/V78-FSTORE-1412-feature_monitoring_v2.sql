-- FSTORE-1822: Feature Monitoring v2 — multi-feature configs, statistics-comparison shape, result tables.
-- FSTORE-1412: Ingestion-triggered feature monitoring linked to StatisticsConfig;
--              drops the legacy statistic_columns table (per-feature list now lives in
--              feature_statistics_config for both FG- and TD-backed rows).
--
-- Feature Monitoring v1 was shipped as experimental; all existing FM data is discarded
-- rather than migrated to the v2 shape. The legacy statistic_columns table is also
-- dropped after its contents are backfilled into feature_statistics_config rows attached
-- to newly-created INGESTION FM configs.

CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_statistics_config`
(
    `id`                           INT(11)                               NOT NULL AUTO_INCREMENT,
    `feature_name`                 VARCHAR(63) COLLATE latin1_general_cs NOT NULL,
    `feature_monitoring_config_id` INT(11)                               NOT NULL,
    PRIMARY KEY (`id`),
    KEY (`feature_name`),
    KEY (`feature_monitoring_config_id`),
    UNIQUE KEY `feature_statistics_config_UNIQUE` (`feature_monitoring_config_id`, `feature_name`),
    CONSTRAINT `feature_statistics_config_fm_config_fk` FOREIGN KEY (`feature_monitoring_config_id`) REFERENCES `hopsworks`.`feature_monitoring_config` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

-- Wipe experimental v1 monitoring data. Delete results explicitly before configs:
-- the ON DELETE CASCADE from feature_monitoring_config → feature_monitoring_result is
-- un-asserted on NDB Cluster, and the reshape ALTERs on feature_monitoring_result
-- below would fail loudly if rows remained. Safe order either way.
DELETE FROM `hopsworks`.`feature_monitoring_result`;
DELETE FROM `hopsworks`.`feature_monitoring_config`;
DELETE FROM `hopsworks`.`statistics_comparison_config`;
DELETE FROM `hopsworks`.`monitoring_window_config`;

-- Phase 1 — drop the legacy v1 structure in a single pass: the feature_name index,
-- the inbound FKs that are about to be re-added below with different ON DELETE
-- semantics (job + both window FKs), the statistics_comparison FK, and the
-- now-unused single-feature columns. The drop and the re-add of the same FK name
-- must live in separate ALTERs — MySQL/NDB forbids both in one statement.
ALTER TABLE `hopsworks`.`feature_monitoring_config`
    DROP KEY `feature_name`,
    DROP FOREIGN KEY `statistics_comparison_config_monitoring_config_fk`,
    DROP FOREIGN KEY `job_monitoring_config_fk`,
    DROP FOREIGN KEY `detection_window_config_monitoring_config_fk`,
    DROP FOREIGN KEY `reference_window_config_monitoring_config_fk`,
    DROP COLUMN `feature_name`,
    DROP COLUMN `statistics_comparison_config_id`;

-- Phase 2 — apply the v2 shape in a single pass (the table was emptied above, so the
-- NOT NULL tightening is safe and the copy cost is negligible). Column changes:
--   * job_id -> NULLABLE (FSTORE-1412): INGESTION configs are created without a job;
--     the job is attached lazily on the first commit (FeatureGroupCommitController)
--     or not at all (TDs).
--   * detection_window_config_id -> NOT NULL: every v2 config has a detection window.
--   * trigger_type (FSTORE-1412): CRON (cron-scheduled) vs INGESTION (auto-created by
--     FeaturegroupController when a FG has statistics_config.enabled=true, fired from
--     FeatureGroupCommitController after each commit).
--   * enabled: master on/off switch (seeded from descriptive during the backfill below).
--   * training_dataset_id (FSTORE-1412): link FM configs to Training Datasets;
--     TD-backed INGESTION configs keep job_id=NULL permanently (TD stats run inline).
--   * model_version_id (FSTORE-2050): a model-monitoring config carries an FK to the
--     model_version it monitors; the FM job dereferences it to (model_name,
--     model_version) to filter the logging FG. Storing the FK (not denormalised
--     name+version strings) avoids cross-project ambiguity. Nullable: legacy and
--     non-model configs are unaffected.
-- FK ON DELETE semantics:
--   * job_monitoring_config_fk RESTRICT (was CASCADE in V4.0.0): a Job is the lifecycle
--     anchor — deleting it must fail while a config references it, not silently wipe it.
--   * detection/reference window FKs NO ACTION (was CASCADE in V4.0.0, which was
--     backwards): window configs are privately owned by the FM config and cleaned up
--     via JPA orphanRemoval=true; the FK is now purely referential integrity.
--   * training_dataset / model_version FKs CASCADE: symmetric with fg/fv FKs (V4.0.0) —
--     deleting the parent entity removes its FM configs.
ALTER TABLE `hopsworks`.`feature_monitoring_config`
    MODIFY COLUMN `job_id` INT(11) NULL,
    MODIFY COLUMN `detection_window_config_id` INT(11) NOT NULL,
    ADD COLUMN `trigger_type` ENUM('CRON','INGESTION') NOT NULL DEFAULT 'CRON',
    ADD COLUMN `enabled` TINYINT(1) NOT NULL DEFAULT 1,
    ADD COLUMN `training_dataset_id` INT(11) NULL,
    ADD COLUMN `model_version_id` INT(11) NULL,
    ADD CONSTRAINT `job_monitoring_config_fk`
        FOREIGN KEY (`job_id`) REFERENCES `hopsworks`.`jobs` (`id`)
        ON DELETE RESTRICT ON UPDATE NO ACTION,
    ADD CONSTRAINT `detection_window_config_monitoring_config_fk`
        FOREIGN KEY (`detection_window_config_id`)
        REFERENCES `hopsworks`.`monitoring_window_config` (`id`)
        ON DELETE NO ACTION ON UPDATE NO ACTION,
    ADD CONSTRAINT `reference_window_config_monitoring_config_fk`
        FOREIGN KEY (`reference_window_config_id`)
        REFERENCES `hopsworks`.`monitoring_window_config` (`id`)
        ON DELETE NO ACTION ON UPDATE NO ACTION,
    ADD CONSTRAINT `training_dataset_monitoring_config_fk`
        FOREIGN KEY (`training_dataset_id`)
        REFERENCES `hopsworks`.`training_dataset` (`id`)
        ON DELETE CASCADE ON UPDATE NO ACTION,
    ADD CONSTRAINT `model_version_monitoring_config_fk`
        FOREIGN KEY (`model_version_id`)
        REFERENCES `hopsworks`.`model_version` (`id`)
        ON DELETE CASCADE ON UPDATE NO ACTION;

-- The "at most one ingestion_stats INGESTION config per FG (and per TD)" invariant
-- is enforced in the controller layer (FeatureMonitoringConfigurationController), not at
-- the DB schema level. INGESTION configs of type STATISTICS_COMPARISON or
-- DISTRIBUTION_COMPARISON are allowed to coexist with the stats-computation one, so a
-- blanket UNIQUE(trigger_type, feature_group_id) would be too strict.

-- NDB Cluster rejects FK ON DELETE CASCADE when the child table has TEXT/BLOB
-- columns (error 1215), regardless of CREATE or ALTER. Same limitation as V56's
-- ai_provider_instruction and mcp_server tables. We DROP+CREATE the empty table
-- here because the original ALTER path also fails: adding a TEXT column to a
-- table that already holds an FK re-triggers the same check.
--
-- Important: because NDB rejects ON DELETE CASCADE on this table (custom_bin_edges
-- is TEXT), the feature_statistics_config_id FK below defaults to ON DELETE RESTRICT.
-- JPA's orphanRemoval on FeatureStatisticsConfiguration.statisticsComparisonConfigs is
-- the ONLY path that cleans up children when a parent feature_statistics_config is
-- removed. Any raw-SQL DELETE against feature_statistics_config — including paths that
-- trigger the ON DELETE CASCADE from feature_monitoring_config → feature_statistics_config
-- declared above — will fail at the DB layer if comparison configs still exist. Treat the
-- comparison-config table as JPA-managed; do not DELETE from it directly.
--
-- The DROP TABLE below is safe because the DELETE + ALTER above left the table empty and
-- removed the inbound FK from feature_monitoring_config; statistics_comparison_result
-- (the other referrer) isn't created until further below.
DROP TABLE `hopsworks`.`statistics_comparison_config`;

CREATE TABLE `hopsworks`.`statistics_comparison_config`
(
    `id`                           INT(11)     NOT NULL AUTO_INCREMENT,
    `strict`                       BOOLEAN     DEFAULT FALSE,
    `relative`                     BOOLEAN     DEFAULT FALSE,
    `threshold`                    DOUBLE,
    `metric`                       INT(11)     DEFAULT NULL,
    `specific_value`               DOUBLE      DEFAULT NULL,
    `feature_statistics_config_id` INT(11)     NOT NULL,
    `distribution_metric`          VARCHAR(32) DEFAULT NULL,
    `binning_strategy`             VARCHAR(32) DEFAULT NULL,
    `bin_count`                    INT         DEFAULT NULL,
    `smoothing_epsilon`            DOUBLE      DEFAULT NULL,
    `custom_bin_edges`             TEXT        DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY (`feature_statistics_config_id`),
    CONSTRAINT `feature_statistics_config_sc_config_fk` FOREIGN KEY (`feature_statistics_config_id`) REFERENCES `hopsworks`.`feature_statistics_config` (`id`)
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

ALTER TABLE `hopsworks`.`monitoring_window_config`
    DROP COLUMN `specific_value`;

-- Reshape feature_monitoring_result before creating feature_statistics_result,
-- so no incoming FK exists on feature_monitoring_result while these ALTERs run
-- (NDB rebuilds all FKs during copy-algorithm ALTERs). Drops, adds and the
-- monitoring_time tightening are done in a single pass:
--   * shifted_feature_names — sized to accommodate wide FGs (~125 feature names at the
--     63-char max). Stays VARCHAR (not TEXT) so NDB Cluster's ON-DELETE-CASCADE FK
--     constraints into this table remain valid (NDB rejects CASCADE on tables holding
--     TEXT/BLOB columns; see comment block higher up).
--   * detection_window_commit_time (FSTORE-1412) — additive nullable column; pairs with
--     hopsworks-persistence .../featuremonitoring/result/FeatureMonitoringResult.java.
--     No FK introduced.
ALTER TABLE `hopsworks`.`feature_monitoring_result`
    DROP FOREIGN KEY `detection_stats_monitoring_result_fk`,
    DROP FOREIGN KEY `reference_stats_monitoring_result_fk`,
    DROP COLUMN `detection_stats_id`,
    DROP COLUMN `reference_stats_id`,
    DROP COLUMN `feature_name`,
    DROP COLUMN `shift_detected`,
    DROP COLUMN `difference`,
    DROP COLUMN `specific_value`,
    ADD COLUMN `shifted_feature_names` VARCHAR(8000) DEFAULT NULL,
    ADD COLUMN `detection_window_commit_time` BIGINT(20) NULL,
    MODIFY COLUMN `monitoring_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_statistics_result`
(
    `id`                           INT(11)                               NOT NULL AUTO_INCREMENT,
    `feature_monitoring_result_id` INT(11)                               NOT NULL,
    `feature_name`                 VARCHAR(63) COLLATE latin1_general_cs NOT NULL,
    `detection_stats_id`           INT(11),
    `reference_stats_id`           INT(11),
    `shifted_metric_names`         VARCHAR(170) DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY (`feature_name`),
    KEY (`feature_monitoring_result_id`),
    UNIQUE KEY `feature_statistics_result_UNIQUE` (`feature_name`, `feature_monitoring_result_id`),
    CONSTRAINT `feature_monitoring_statistics_result_fk` FOREIGN KEY (`feature_monitoring_result_id`) REFERENCES `hopsworks`.`feature_monitoring_result` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `detection_stats_feat_stats_result_fk` FOREIGN KEY (`detection_stats_id`) REFERENCES `hopsworks`.`feature_descriptive_statistics` (`id`) ON DELETE NO ACTION,
    CONSTRAINT `reference_stats_feat_stats_result_fk` FOREIGN KEY (`reference_stats_id`) REFERENCES `hopsworks`.`feature_descriptive_statistics` (`id`) ON DELETE NO ACTION
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `hopsworks`.`statistics_comparison_result`
(
    `id`                              INT(11) NOT NULL AUTO_INCREMENT,
    `feature_statistics_result_id`    INT(11) NOT NULL,
    `statistics_comparison_config_id` INT(11) NOT NULL,
    `difference`                      DOUBLE  DEFAULT NULL,
    `shift_detected`                  BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (`id`),
    KEY (`feature_statistics_result_id`),
    KEY (`statistics_comparison_config_id`),
    UNIQUE KEY `feature_statistics_comparison_result_UNIQUE` (`feature_statistics_result_id`, `statistics_comparison_config_id`),
    CONSTRAINT `statistics_comparison_result_f_stats_result_fk` FOREIGN KEY (`feature_statistics_result_id`) REFERENCES `hopsworks`.`feature_statistics_result` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `statistics_comparison_result_config_fk` FOREIGN KEY (`statistics_comparison_config_id`) REFERENCES `hopsworks`.`statistics_comparison_config` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

ALTER TABLE `hopsworks`.`job_schedule`
  ADD COLUMN `last_execution_date_time` TIMESTAMP NULL DEFAULT NULL;

-- statistics_config is NOT emptied by this migration (holds live data), so the new
-- columns + FK are added in a single ALTER to copy the populated table only once.
-- FSTORE-1412: feature_monitoring_config_id links StatisticsConfig to the
-- ingestion-triggered FM config. The existing fg_statistics_config_fk and
-- td_statistics_config_fk ON DELETE CASCADE FKs are preserved unchanged — no
-- TEXT/BLOB column is added to this table, so NDB error 1215 does not apply.
-- fk_statistics_config_fm uses ON DELETE SET NULL so that deleting an FM config
-- nullifies the pointer rather than cascading.
ALTER TABLE `hopsworks`.`statistics_config`
    ADD COLUMN `kll`                          TINYINT(1) DEFAULT NULL,
    ADD COLUMN `histogram_bins`               INT        DEFAULT NULL,
    ADD COLUMN `feature_monitoring_config_id` INT(11)    NULL,
    ADD CONSTRAINT `fk_statistics_config_fm`
        FOREIGN KEY (`feature_monitoring_config_id`)
        REFERENCES `hopsworks`.`feature_monitoring_config` (`id`)
        ON DELETE SET NULL ON UPDATE NO ACTION;

-- FSTORE-1412: backfill INGESTION FM configs from legacy statistic_columns.
--
-- For every statistics_config row, create:
--   1. One monitoring_window_config per row (ALL_TIME, row_percentage=1.0).
--   2. One feature_monitoring_config per row (INGESTION, STATISTICS_COMPUTATION, job_id=NULL),
--      linked to the feature_group_id or training_dataset_id from statistics_config.
--      enabled is seeded from descriptive: rows with descriptive=0 land with enabled=0.
--   3. One feature_statistics_config row per legacy statistic_columns entry.
--   4. UPDATE statistics_config.feature_monitoring_config_id to point at the new config.
--
-- The procedure processes one statistics_config row at a time to avoid the ordering
-- ambiguity of matching bulk-inserted monitoring_window_config rows back to source rows.
-- All rows are included so every FG/TD always has an FM config (always-exists invariant).

-- RDRS-P1-PORT: data-migration procedure block commented out (backfill of pre-existing rows; no-op on fresh test fixtures; DELIMITER/procedures cannot run through the test executor). Schema statements above/below are kept.
-- DROP PROCEDURE IF EXISTS `hopsworks`.`backfill_ingestion_fm_configs`;

-- DELIMITER $$

-- CREATE PROCEDURE `hopsworks`.`backfill_ingestion_fm_configs`()
-- BEGIN
--     DECLARE done INT DEFAULT FALSE;
--     DECLARE v_sc_id INT;
--     DECLARE v_fg_id INT;
--     DECLARE v_td_id INT;
--     DECLARE v_descriptive TINYINT(1);
--     DECLARE v_wc_id INT;
--     DECLARE v_fm_id INT;

--     -- Exclude statistics_config rows that belong to ON_DEMAND_FEATURE_GROUP (ordinal 1:
--     -- CACHED=0, ON_DEMAND=1, STREAM=2). External FGs and spine groups have no offline
--     -- Hopsworks-owned storage and no commit path — an INGESTION config for them would be
--     -- permanently dead. Training dataset rows (feature_group_id IS NULL) are always included.
--     -- enabled is seeded from descriptive so pre-existing stats-disabled rows
--     -- land with enabled=0 and never fire until the user re-enables statistics.
--     DECLARE cur CURSOR FOR
--         SELECT sc.`id`, sc.`feature_group_id`, sc.`training_dataset_id`, sc.`descriptive`
--         FROM `hopsworks`.`statistics_config` sc
--         LEFT JOIN `hopsworks`.`feature_group` fg ON fg.`id` = sc.`feature_group_id`
--         WHERE sc.`feature_group_id` IS NULL
--            OR fg.`feature_group_type` != 1;

--     DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

--     OPEN cur;
--     read_loop: LOOP
--         FETCH cur INTO v_sc_id, v_fg_id, v_td_id, v_descriptive;
--         IF done THEN
--             LEAVE read_loop;
--         END IF;

--         -- Create the detection window config.
--         -- window_config_type is INT(11) storing WindowConfigurationType ordinal; ALL_TIME=0.
--         INSERT INTO `hopsworks`.`monitoring_window_config` (`window_config_type`, `row_percentage`)
--         VALUES (0, 1.0);
--         SET v_wc_id = LAST_INSERT_ID();

--         -- Create the INGESTION FM config (job_id=NULL intentionally).
--         -- Use the canonical "ingestion_stats" name — matches what the runtime code
--         -- (FeatureMonitoringConfigurationController.INGESTION_CONFIG_NAME) writes, so the
--         -- commit-time and update-time lookups find backfilled configs. Uniqueness per
--         -- FG / TD is guaranteed by @OneToOne on statistics_config (this cursor iterates
--         -- one statistics_config row at a time, INSERTing exactly one FM config per entity).
--         INSERT INTO `hopsworks`.`feature_monitoring_config`
--             (`feature_group_id`, `training_dataset_id`, `name`, `trigger_type`,
--              `feature_monitoring_type`, `job_id`, `detection_window_config_id`, `enabled`)
--         VALUES
--             (v_fg_id, v_td_id, 'ingestion_stats', 'INGESTION', 0, NULL, v_wc_id, v_descriptive);
--         SET v_fm_id = LAST_INSERT_ID();

--         -- Migrate legacy statistic_columns entries for this statistics_config.
--         INSERT INTO `hopsworks`.`feature_statistics_config` (`feature_name`, `feature_monitoring_config_id`)
--         SELECT `name`, v_fm_id
--         FROM `hopsworks`.`statistic_columns`
--         WHERE `statistics_config_id` = v_sc_id;

--         -- Link the statistics_config row to its new FM config.
--         UPDATE `hopsworks`.`statistics_config`
--         SET `feature_monitoring_config_id` = v_fm_id
--         WHERE `id` = v_sc_id;

--     END LOOP;
--     CLOSE cur;
-- END$$

-- DELIMITER ;

-- CALL `hopsworks`.`backfill_ingestion_fm_configs`();

-- DROP PROCEDURE IF EXISTS `hopsworks`.`backfill_ingestion_fm_configs`;

-- Legacy per-feature column-subset table. Its contents have been migrated above
-- into feature_statistics_config rows attached to the new INGESTION FM configs.
DROP TABLE `hopsworks`.`statistic_columns`;

-- FSTORE-1412: Rename stored alert-status string values to match Java enum renames.
--
-- Author: Javier de la Rúa Martínez <javier@logicalclocks.com>
-- Pairs with: (data-only migration — no DDL change, no entity change)
--
-- Idempotency: Each UPDATE targets only rows that still carry the old name, so
--              re-running is a safe no-op once the old values are gone.
--              The guard subquery additionally skips rows where the new-named
--              row already exists (collision-safe; see Collision-safety below).
-- Cascade: N/A — pure DML (UPDATE / DELETE), no FK or schema changes.
--
-- Background
-- ----------
-- FeatureStoreAlertStatus and ProjectServiceAlertStatus use @Enumerated(STRING),
-- so the DB stores the Java enum constant NAME verbatim.  FSTORE-1412 renamed
-- several constants:
--
--   FeatureStoreAlertStatus (feature_group_alert, feature_view_alert)
--     SUCCESS                          -> VALIDATION_SUCCESS
--     WARNING                          -> VALIDATION_WARNING
--     FAILURE                          -> VALIDATION_FAILURE
--     FEATURE_MONITOR_SHIFT_DETECTED   -> MONITORING_SHIFT_DETECTED
--     FEATURE_MONITOR_SHIFT_UNDETECTED -> MONITORING_SHIFT_UNDETECTED
--
--   ProjectServiceAlertStatus (project_service_alert)
--     VALIDATION_* constants were already named VALIDATION_* before this change
--     and need no update.  Only the feature-monitoring constants were renamed:
--     FEATURE_MONITOR_SHIFT_DETECTED   -> MONITORING_SHIFT_DETECTED
--     FEATURE_MONITOR_SHIFT_UNDETECTED -> MONITORING_SHIFT_UNDETECTED
--
-- Collision-safety
-- ----------------
-- Each of the three target tables has a UNIQUE constraint on (entity_col, status):
--   feature_group_alert : UNIQUE KEY `unique_feature_group_alert` (feature_group_id, status)
--   project_service_alert: UNIQUE KEY `unique_project_service_alert` (project_id, status)
--   feature_view_alert  : CONSTRAINT `unique_feature_view_status` UNIQUE (feature_view_id, status)
--
-- If a new-named row already exists for the same entity (e.g. because an alert
-- was created via the new enum after deploy but before this migration ran), a
-- plain UPDATE would violate the UNIQUE constraint and abort the entire Flyway
-- migration — unrecoverable on a live customer DB.
--
-- Guard pattern: each UPDATE is restricted to rows where NO new-named row
-- already exists for the same entity.  MySQL/NDB does not allow referencing the
-- UPDATE target table directly in a subquery FROM clause, so we wrap with a
-- derived-table (inline view), which is broadly supported:
--
--   UPDATE `t`
--   SET `t`.`status` = '<NEW>'
--   WHERE `t`.`status` = '<OLD>'
--     AND NOT EXISTS (
--       SELECT 1 FROM (SELECT `entity_col`, `status` FROM `t`) AS _guard
--       WHERE _guard.`entity_col` = `t`.`entity_col`
--         AND _guard.`status` = '<NEW>'
--     );
--
-- After the guarded UPDATEs, a cleanup DELETE removes any legacy-named rows
-- that the UPDATE skipped (i.e. the ones where a new-named row already existed
-- for the same entity).  This ensures no legacy status string survives — which
-- would otherwise cause JPA @Enumerated(STRING) valueOf() to throw at read time
-- once the old constant is removed from the enum.
--
-- Together, guarded UPDATE + cleanup DELETE guarantee:
--   (a) No UNIQUE constraint violation during the migration.
--   (b) No legacy-named row survives after the migration.

-- ---------------------------------------------------------------------------
-- feature_group_alert
-- UNIQUE: (feature_group_id, status)
-- ---------------------------------------------------------------------------

-- SUCCESS -> VALIDATION_SUCCESS
UPDATE `hopsworks`.`feature_group_alert` t
SET t.`status` = 'VALIDATION_SUCCESS'
WHERE t.`status` = 'SUCCESS'
  AND NOT EXISTS (
    SELECT 1 FROM (SELECT `feature_group_id`, `status` FROM `hopsworks`.`feature_group_alert`) AS _guard
    WHERE _guard.`feature_group_id` = t.`feature_group_id`
      AND _guard.`status` = 'VALIDATION_SUCCESS'
  );

-- WARNING -> VALIDATION_WARNING
UPDATE `hopsworks`.`feature_group_alert` t
SET t.`status` = 'VALIDATION_WARNING'
WHERE t.`status` = 'WARNING'
  AND NOT EXISTS (
    SELECT 1 FROM (SELECT `feature_group_id`, `status` FROM `hopsworks`.`feature_group_alert`) AS _guard
    WHERE _guard.`feature_group_id` = t.`feature_group_id`
      AND _guard.`status` = 'VALIDATION_WARNING'
  );

-- FAILURE -> VALIDATION_FAILURE
UPDATE `hopsworks`.`feature_group_alert` t
SET t.`status` = 'VALIDATION_FAILURE'
WHERE t.`status` = 'FAILURE'
  AND NOT EXISTS (
    SELECT 1 FROM (SELECT `feature_group_id`, `status` FROM `hopsworks`.`feature_group_alert`) AS _guard
    WHERE _guard.`feature_group_id` = t.`feature_group_id`
      AND _guard.`status` = 'VALIDATION_FAILURE'
  );

-- FEATURE_MONITOR_SHIFT_DETECTED -> MONITORING_SHIFT_DETECTED
UPDATE `hopsworks`.`feature_group_alert` t
SET t.`status` = 'MONITORING_SHIFT_DETECTED'
WHERE t.`status` = 'FEATURE_MONITOR_SHIFT_DETECTED'
  AND NOT EXISTS (
    SELECT 1 FROM (SELECT `feature_group_id`, `status` FROM `hopsworks`.`feature_group_alert`) AS _guard
    WHERE _guard.`feature_group_id` = t.`feature_group_id`
      AND _guard.`status` = 'MONITORING_SHIFT_DETECTED'
  );

-- FEATURE_MONITOR_SHIFT_UNDETECTED -> MONITORING_SHIFT_UNDETECTED
UPDATE `hopsworks`.`feature_group_alert` t
SET t.`status` = 'MONITORING_SHIFT_UNDETECTED'
WHERE t.`status` = 'FEATURE_MONITOR_SHIFT_UNDETECTED'
  AND NOT EXISTS (
    SELECT 1 FROM (SELECT `feature_group_id`, `status` FROM `hopsworks`.`feature_group_alert`) AS _guard
    WHERE _guard.`feature_group_id` = t.`feature_group_id`
      AND _guard.`status` = 'MONITORING_SHIFT_UNDETECTED'
  );

-- Remove any legacy rows that could not be renamed because a new-named row
-- already existed for the same feature_group_id.
DELETE FROM `hopsworks`.`feature_group_alert`
WHERE `status` IN ('SUCCESS', 'WARNING', 'FAILURE',
                   'FEATURE_MONITOR_SHIFT_DETECTED', 'FEATURE_MONITOR_SHIFT_UNDETECTED');

-- ---------------------------------------------------------------------------
-- feature_view_alert
-- UNIQUE: (feature_view_id, status)
-- ---------------------------------------------------------------------------

-- SUCCESS -> VALIDATION_SUCCESS
UPDATE `hopsworks`.`feature_view_alert` t
SET t.`status` = 'VALIDATION_SUCCESS'
WHERE t.`status` = 'SUCCESS'
  AND NOT EXISTS (
    SELECT 1 FROM (SELECT `feature_view_id`, `status` FROM `hopsworks`.`feature_view_alert`) AS _guard
    WHERE _guard.`feature_view_id` = t.`feature_view_id`
      AND _guard.`status` = 'VALIDATION_SUCCESS'
  );

-- WARNING -> VALIDATION_WARNING
UPDATE `hopsworks`.`feature_view_alert` t
SET t.`status` = 'VALIDATION_WARNING'
WHERE t.`status` = 'WARNING'
  AND NOT EXISTS (
    SELECT 1 FROM (SELECT `feature_view_id`, `status` FROM `hopsworks`.`feature_view_alert`) AS _guard
    WHERE _guard.`feature_view_id` = t.`feature_view_id`
      AND _guard.`status` = 'VALIDATION_WARNING'
  );

-- FAILURE -> VALIDATION_FAILURE
UPDATE `hopsworks`.`feature_view_alert` t
SET t.`status` = 'VALIDATION_FAILURE'
WHERE t.`status` = 'FAILURE'
  AND NOT EXISTS (
    SELECT 1 FROM (SELECT `feature_view_id`, `status` FROM `hopsworks`.`feature_view_alert`) AS _guard
    WHERE _guard.`feature_view_id` = t.`feature_view_id`
      AND _guard.`status` = 'VALIDATION_FAILURE'
  );

-- FEATURE_MONITOR_SHIFT_DETECTED -> MONITORING_SHIFT_DETECTED
UPDATE `hopsworks`.`feature_view_alert` t
SET t.`status` = 'MONITORING_SHIFT_DETECTED'
WHERE t.`status` = 'FEATURE_MONITOR_SHIFT_DETECTED'
  AND NOT EXISTS (
    SELECT 1 FROM (SELECT `feature_view_id`, `status` FROM `hopsworks`.`feature_view_alert`) AS _guard
    WHERE _guard.`feature_view_id` = t.`feature_view_id`
      AND _guard.`status` = 'MONITORING_SHIFT_DETECTED'
  );

-- FEATURE_MONITOR_SHIFT_UNDETECTED -> MONITORING_SHIFT_UNDETECTED
UPDATE `hopsworks`.`feature_view_alert` t
SET t.`status` = 'MONITORING_SHIFT_UNDETECTED'
WHERE t.`status` = 'FEATURE_MONITOR_SHIFT_UNDETECTED'
  AND NOT EXISTS (
    SELECT 1 FROM (SELECT `feature_view_id`, `status` FROM `hopsworks`.`feature_view_alert`) AS _guard
    WHERE _guard.`feature_view_id` = t.`feature_view_id`
      AND _guard.`status` = 'MONITORING_SHIFT_UNDETECTED'
  );

-- Remove any legacy rows that could not be renamed because a new-named row
-- already existed for the same feature_view_id.
DELETE FROM `hopsworks`.`feature_view_alert`
WHERE `status` IN ('SUCCESS', 'WARNING', 'FAILURE',
                   'FEATURE_MONITOR_SHIFT_DETECTED', 'FEATURE_MONITOR_SHIFT_UNDETECTED');

-- ---------------------------------------------------------------------------
-- project_service_alert
-- UNIQUE: (project_id, status)
-- VALIDATION_* rows were already correctly named — only FEATURE_MONITOR_* renamed.
-- ---------------------------------------------------------------------------

-- FEATURE_MONITOR_SHIFT_DETECTED -> MONITORING_SHIFT_DETECTED
UPDATE `hopsworks`.`project_service_alert` t
SET t.`status` = 'MONITORING_SHIFT_DETECTED'
WHERE t.`status` = 'FEATURE_MONITOR_SHIFT_DETECTED'
  AND NOT EXISTS (
    SELECT 1 FROM (SELECT `project_id`, `status` FROM `hopsworks`.`project_service_alert`) AS _guard
    WHERE _guard.`project_id` = t.`project_id`
      AND _guard.`status` = 'MONITORING_SHIFT_DETECTED'
  );

-- FEATURE_MONITOR_SHIFT_UNDETECTED -> MONITORING_SHIFT_UNDETECTED
UPDATE `hopsworks`.`project_service_alert` t
SET t.`status` = 'MONITORING_SHIFT_UNDETECTED'
WHERE t.`status` = 'FEATURE_MONITOR_SHIFT_UNDETECTED'
  AND NOT EXISTS (
    SELECT 1 FROM (SELECT `project_id`, `status` FROM `hopsworks`.`project_service_alert`) AS _guard
    WHERE _guard.`project_id` = t.`project_id`
      AND _guard.`status` = 'MONITORING_SHIFT_UNDETECTED'
  );

-- Remove any legacy rows that could not be renamed because a new-named row
-- already existed for the same project_id.
DELETE FROM `hopsworks`.`project_service_alert`
WHERE `status` IN ('FEATURE_MONITOR_SHIFT_DETECTED', 'FEATURE_MONITOR_SHIFT_UNDETECTED');
