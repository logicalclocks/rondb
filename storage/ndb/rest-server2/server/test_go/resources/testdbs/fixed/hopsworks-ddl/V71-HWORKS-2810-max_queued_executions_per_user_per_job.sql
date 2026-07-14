-- Per-user-per-job cap on queued (pre-RUNNING) executions. Backstop against
-- Airflow `@continuous` and similar tight scheduling loops flooding a single
-- user's queue for one job. Default 10; sentinel -1 disables the gate.
-- Settings.HopsworksSettingKeys.QUOTAS_MAX_QUEUED_EXECUTIONS_PER_USER_PER_JOB
-- reads this row; absence falls back to the enum default ("10"), so a fresh
-- install behaves the same with or without the migration. The row exists so
-- admins can override it via the variables UI.
INSERT INTO `hopsworks`.`variables` (`id`, `value`, `visibility`, `hide`)
VALUES ('quotas_max_queued_executions_per_user_per_job', '10', 0, 0)
ON DUPLICATE KEY UPDATE `id`=`id`;
