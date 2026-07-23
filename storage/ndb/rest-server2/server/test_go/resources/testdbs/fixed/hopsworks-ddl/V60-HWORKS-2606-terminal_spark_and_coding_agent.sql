--
-- Migrate terminal_session: replace persistent_home with terminal_type and Spark executor settings
--

ALTER TABLE `hopsworks`.`terminal_session`
  DROP COLUMN `persistent_home`,
  ADD COLUMN `terminal_type` ENUM('PYTHON', 'SPARK', 'GPU') NOT NULL DEFAULT 'PYTHON' COMMENT 'Terminal type: PYTHON, SPARK, or GPU',
  ADD COLUMN `num_executors` INT NULL DEFAULT NULL COMMENT 'Spark executor count (SPARK terminals only)',
  ADD COLUMN `executor_cores` DECIMAL(4,2) NULL DEFAULT NULL COMMENT 'Spark executor cores (SPARK terminals only)',
  ADD COLUMN `executor_memory` INT NULL DEFAULT NULL COMMENT 'Spark executor memory in MB (SPARK terminals only)';

--
-- Add terminal_type to mcp_server
--

ALTER TABLE `hopsworks`.`mcp_server`
  ADD COLUMN `terminal_type` ENUM('PYTHON', 'SPARK', 'GPU') NOT NULL DEFAULT 'PYTHON' COMMENT 'Terminal type this MCP server belongs to: PYTHON, SPARK, or GPU';

--
-- Coding agent configuration (system-wide settings for coding agents like Claude)
--

CREATE TABLE IF NOT EXISTS `hopsworks`.`coding_agent_config` (
  `coding_agent` VARCHAR(100) COLLATE latin1_general_cs NOT NULL COMMENT 'Coding agent name (e.g., claude)',
  `system_instruction` VARCHAR(20000) COLLATE latin1_general_cs NULL COMMENT 'System-wide instruction content (e.g., CLAUDE.md)',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`coding_agent`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
