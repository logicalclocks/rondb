-- [HWORKS-2789] Remove Brewer from Hopsworks
-- Drop Brewer-only tables in child-before-parent FK order: brewer_chat has an FK
-- to selected_agent (selected_agent_id, added in V50), and selected_agent has an
-- FK to agent (agent_id), so the referencing tables must be dropped first.

DROP TABLE IF EXISTS `hopsworks`.`brewer_chat`;
DROP TABLE IF EXISTS `hopsworks`.`selected_agent`;
DROP TABLE IF EXISTS `hopsworks`.`agent`;

-- Remove every row that stored a now-deleted Brewer enum value as a string.
-- ProjectServiceEnum.BREWER and ApiScope.BREWER are gone from the Java enums and
-- all three columns are @Enumerated(EnumType.STRING), so any surviving 'BREWER'
-- string would throw "No enum constant ...BREWER" when the row is materialised on
-- read (project load, project-service alerts, and serving-key auth). Older
-- clusters auto-granted ApiScope.BREWER to every user's reserved serving API key,
-- so api_key_scope in particular WILL have stale rows.
-- RDRS-P1-PORT: production-data cleanup DML commented out; no-op on fresh test
-- fixtures (no BREWER rows are seeded) and DELETEs with a non-key WHERE are
-- rejected by MySQL safe-update mode (Error 1175) in the test environment.
-- DELETE FROM `hopsworks`.`project_services` WHERE `service` = 'BREWER';
-- DELETE FROM `hopsworks`.`project_service_alert` WHERE `service` = 'BREWER';
-- DELETE FROM `hopsworks`.`api_key_scope` WHERE `scope` = 'BREWER';
