CREATE TABLE IF NOT EXISTS `hopsworks`.`user_env_vars` (
  `uid` INT(11) NOT NULL,
  `env_name` VARCHAR(255) NOT NULL,
  `env_value` VARBINARY(10000) NOT NULL,
  `added_on` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_on` TIMESTAMP NULL DEFAULT NULL,
  PRIMARY KEY (`uid`, `env_name`),
  CONSTRAINT `fk_user_env_vars_uid`
    FOREIGN KEY (`uid`)
    REFERENCES `hopsworks`.`users` (`uid`)
    ON DELETE CASCADE
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

-- Platform-intelligence LLM configuration. Inserted as empty so existing
-- deployments don't fail to start; an admin sets the API key and (optionally)
-- a custom base URL via the admin UI.
INSERT INTO `hopsworks`.`variables` (`id`, `value`, `visibility`, `hide`)
VALUES ('platform_intelligence_llm_api_key', '', 0, 0)
ON DUPLICATE KEY UPDATE `id`=`id`;

INSERT INTO `hopsworks`.`variables` (`id`, `value`, `visibility`, `hide`)
VALUES ('platform_intelligence_llm_model', 'gpt-5.4-mini', 0, 0)
ON DUPLICATE KEY UPDATE `id`=`id`;

INSERT INTO `hopsworks`.`variables` (`id`, `value`, `visibility`, `hide`)
VALUES ('platform_intelligence_llm_base_url', '', 0, 0)
ON DUPLICATE KEY UPDATE `id`=`id`;
