--
-- Terminal session table for web terminal feature
--

CREATE TABLE IF NOT EXISTS `hopsworks`.`terminal_session` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `cid` VARCHAR(255) NOT NULL COMMENT 'Container/Pod ID in Kubernetes',
  `port` INT NOT NULL,
  `host` VARCHAR(255) NOT NULL,
  `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `expires` TIMESTAMP NOT NULL,
  `secret` VARCHAR(64) NOT NULL,
  `token` VARCHAR(255) NOT NULL,
  `project_id` INT NOT NULL,
  `uid` INT NOT NULL,
  `cores` DECIMAL(4,2) NULL DEFAULT 1.0,
  `memory` INT NULL DEFAULT 2048,
  `gpus` INT NULL DEFAULT 0,
  `persistent_home` TINYINT(1) NULL DEFAULT 0 COMMENT 'If true, use PVC for /home/terminal',
  PRIMARY KEY (`id`),
  UNIQUE KEY `terminal_session_project_user_uk` (`project_id`, `uid`),
  KEY `terminal_session_token_idx` (`token`),
  KEY `terminal_session_cid_idx` (`cid`),
  KEY `terminal_session_expires_idx` (`expires`),
  CONSTRAINT `terminal_session_project_fk`
    FOREIGN KEY (`project_id`) REFERENCES `hopsworks`.`project` (`id`) ON DELETE CASCADE,
  CONSTRAINT `terminal_session_user_fk`
    FOREIGN KEY (`uid`) REFERENCES `hopsworks`.`users` (`uid`) ON DELETE CASCADE
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

--
-- AI Provider configuration for terminal integration
-- Stores user's AI provider settings (Anthropic, OpenAI, etc.)
--

CREATE TABLE IF NOT EXISTS `hopsworks`.`ai_provider` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `uid` INT NOT NULL,
  `provider_type` VARCHAR(50) NOT NULL COMMENT 'ANTHROPIC, OPENAI, CUSTOM',
  `name` VARCHAR(100) NOT NULL,
  `api_key_secret_name` VARCHAR(200) NOT NULL COMMENT 'Reference to secrets table',
  `endpoint` VARCHAR(500) NULL COMMENT 'Custom API endpoint (e.g., Azure OpenAI)',
  `enabled` TINYINT(1) NOT NULL DEFAULT 1,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ai_provider_user_name_uk` (`uid`, `name`),
  KEY `ai_provider_user_idx` (`uid`),
  CONSTRAINT `ai_provider_user_fk`
    FOREIGN KEY (`uid`) REFERENCES `hopsworks`.`users` (`uid`) ON DELETE CASCADE
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

CREATE TABLE IF NOT EXISTS `hopsworks`.`ai_provider_instruction` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `ai_provider_id` INT NOT NULL,
  `source_type` VARCHAR(20) NOT NULL COMMENT 'URL or INLINE',
  `source_url` VARCHAR(1000) NULL COMMENT 'HTTPS URL to fetch instructions from',
  `content` MEDIUMTEXT NULL COMMENT 'Cached/inline instruction content',
  `priority` INT NOT NULL DEFAULT 0 COMMENT 'Merge order (lower = first)',
  `last_fetched` TIMESTAMP NULL COMMENT 'Last successful fetch from URL',
  PRIMARY KEY (`id`),
  KEY `ai_provider_instruction_provider_idx` (`ai_provider_id`),
  -- NDB Cluster: ON DELETE CASCADE not supported here, JPA handles with orphanRemoval=true
  CONSTRAINT `ai_provider_instruction_provider_fk`
    FOREIGN KEY (`ai_provider_id`) REFERENCES `hopsworks`.`ai_provider` (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

--
-- MCP Server configuration for terminal integration
-- Stores project's MCP server settings (per-project, not per-user)
--

CREATE TABLE IF NOT EXISTS `hopsworks`.`mcp_server` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `project_id` INT NOT NULL,
  `name` VARCHAR(100) NOT NULL,
  `repo_url` VARCHAR(500) NULL COMMENT 'GitHub repo URL this MCP came from',
  `command` VARCHAR(200) NOT NULL COMMENT 'Command to run (e.g., hopsworks-mcp-readonly)',
  `args` VARCHAR(500) NULL COMMENT 'Additional arguments',
  `port` INT NULL COMMENT 'Port to run on (e.g., 8000)',
  `enabled` TINYINT(1) NOT NULL DEFAULT 1,
  `env_vars` TEXT NULL COMMENT 'JSON object of environment variables',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `mcp_server_project_name_uk` (`project_id`, `name`),
  KEY `mcp_server_project_idx` (`project_id`),
  -- NDB Cluster: ON DELETE CASCADE not fully supported, JPA handles cleanup
  CONSTRAINT `mcp_server_project_fk`
    FOREIGN KEY (`project_id`) REFERENCES `hopsworks`.`project` (`id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
