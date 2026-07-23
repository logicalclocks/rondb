-- serving_depl_component: user-defined env vars on deployment components.
--
-- Stored as a JSON array of "KEY=VALUE" strings via EnvVarsConverter (mirrors the
-- format already used by DockerJobConfiguration.envVars so the same parsing / override
-- rules apply on both jobs and deployments).
--
-- VARCHAR(8000) rather than TEXT: RonDB (NDB) refuses an online ALTER that adds a
-- TEXT/BLOB column to a table carrying a foreign key — it bails with a misleading
-- error 1215 "Cannot add foreign key constraint", and re-adding the FK afterwards
-- hits a separate NDB metadata bug. Plain VARCHAR works with the existing FK intact.
-- 8000 bytes gives room for ~30–40 KEY=VALUE pairs with realistic value lengths;
-- callers that need more can split the config or use a project-level env-var source.
--
-- NULL means "no user env vars" (the common case on existing rows).
ALTER TABLE `hopsworks`.`serving_depl_component`
    ADD COLUMN `env_vars` VARCHAR(8000) NULL DEFAULT NULL;
