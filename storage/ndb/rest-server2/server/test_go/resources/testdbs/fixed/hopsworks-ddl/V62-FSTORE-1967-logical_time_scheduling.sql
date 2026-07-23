-- Logical-time scheduling + per-execution env vars.
--
-- Consolidates the scheduler-side migrations of the FSTORE-1967 work:
--   * job_schedule: catchup / maxActiveRuns / start+end offsets / skip-to / max-catchup-runs.
--   * executions: logical_date + data_interval_end + run_env_vars per execution,
--     plus a (job_id, logical_date) helper index for reconciliation queries.
--
-- The `serving_depl_component.env_vars` column for user-defined deployment env vars
-- ships separately in V64 — it is unrelated to scheduling and touches a different table.
--
-- Originally split across V62/V63 while the scheduler double-fire was still being
-- investigated — V62 added a defensive UNIQUE (job_id, logical_date) index and V63 dropped
-- it once the real fix (app-layer monotonic firedThisTick counter) landed. In the
-- consolidated form we never add the unique index in the first place; the non-unique
-- helper `idx_executions_job_logical_date` stays. See the repo history for the full story.

--
-- 1. job_schedule: logical-time scheduling knobs
--
ALTER TABLE `hopsworks`.`job_schedule`
    ADD COLUMN `catchup` TINYINT(1) NOT NULL DEFAULT 0,
    ADD COLUMN `max_active_runs` INT NOT NULL DEFAULT 1,
    ADD COLUMN `start_time_offset_seconds` BIGINT DEFAULT 0,
    ADD COLUMN `end_time_offset_seconds` BIGINT DEFAULT NULL,
    ADD COLUMN `skip_to_date` TIMESTAMP(3) NULL DEFAULT NULL,
    ADD COLUMN `max_catchup_runs` INT NULL DEFAULT NULL;

--
-- 2. executions: logical_date, data_interval_end, run_env_vars
--
-- TIMESTAMP(3) (millisecond precision) is required: cron libraries and Instant.ofEpochMilli
-- emit sub-second values, and `logical_date` is queried by strict equality via
-- `ExecutionFacade.findByJobAndLogicalDate` for scheduler dedup / catchup. Plain TIMESTAMP
-- (seconds) would silently truncate on INSERT but keep client precision in the query
-- parameter, so equality would never match. Matches the sub-second precision used by the
-- rest of the hopsworks schema.
ALTER TABLE `hopsworks`.`executions`
    ADD COLUMN `logical_date` TIMESTAMP(3) NULL DEFAULT NULL,
    ADD COLUMN `data_interval_end` TIMESTAMP(3) NULL DEFAULT NULL,
    ADD COLUMN `run_env_vars` TEXT NULL DEFAULT NULL;

-- Reconciliation helper index for `findLatestByJobWithLogicalDate` /
-- `findByJobAndLogicalDate`. Non-unique — a given logical_date can legitimately have
-- multiple execution rows (retry of a failed run, manual rerun of a backfill window,
-- scheduled fire overlapping a manual trigger). NDB (RonDB) does not support DESCENDING
-- indexes; the ascending index still accelerates `ORDER BY logical_date DESC` via
-- backward traversal of the B-tree.
ALTER TABLE `hopsworks`.`executions`
    ADD INDEX `idx_executions_job_logical_date` (`job_id`, `logical_date`);
