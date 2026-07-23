-- Table to store deployable agents metadata information is extracted from git repositories
CREATE TABLE IF NOT EXISTS `hopsworks`.`agent` (
  `id`              INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `name`            VARCHAR(128) NOT NULL,
  `repo_url`        VARCHAR(512) NOT NULL,  -- reesource URL
  `created`         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `unique_agent` (`name`, `repo_url`)
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

-- User selects an agent for a project with optional version constraints (functions as a root for dependency resolution)
CREATE TABLE IF NOT EXISTS `hopsworks`.`selected_agent`
(
    `id`                  INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `project_id`          INT NOT NULL,
    `creator_id`          INT NOT NULL,
    `name`                VARCHAR(63) NOT NULL,
    `agent_id`            INT NOT NULL,  -- dependent agent
    `version_constraint`  VARCHAR(64),  -- e.g. ">=v1.0.0 <v2.0.0"
    UNIQUE KEY (`project_id`, `name`),
    FOREIGN KEY (`project_id`) REFERENCES `project` (`id`) ON DELETE CASCADE,
    FOREIGN KEY (`creator_id`) REFERENCES `users` (`uid`) ON DELETE CASCADE,
    FOREIGN KEY (`agent_id`) REFERENCES `agent` (`id`) ON DELETE CASCADE
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

-- Add NULLable selected_agent_id
ALTER TABLE `hopsworks`.`brewer_chat`
  ADD COLUMN `selected_agent_id` INT NULL,
  ADD FOREIGN KEY (`selected_agent_id`) REFERENCES `selected_agent` (`id`) ON DELETE CASCADE;
