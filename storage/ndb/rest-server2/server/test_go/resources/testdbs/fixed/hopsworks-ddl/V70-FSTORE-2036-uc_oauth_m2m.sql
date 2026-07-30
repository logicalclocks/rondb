-- Add OAuth 2.0 M2M (service principal client-credentials grant) auth to the
-- Unity Catalog connector, alongside the existing PAT path. Existing rows
-- default to PAT so behaviour is unchanged for connectors already in use.
--
-- client_secret is stored encrypted in the `secrets` table via a composite FK,
-- mirroring the PAT token FK added in V63. account_id + account_host are only
-- populated when oauth_endpoint = 'ACCOUNT' (Databricks account-level token
-- endpoint); workspace-level minting derives the endpoint from workspace_url.

ALTER TABLE `hopsworks`.`feature_store_unity_catalog_connector`
    ADD COLUMN `auth_method`         VARCHAR(16)  NOT NULL DEFAULT 'PAT',
    ADD COLUMN `client_id`           VARCHAR(255) DEFAULT NULL,
    ADD COLUMN `client_secret_uid`   INT          DEFAULT NULL,
    ADD COLUMN `client_secret_name`  VARCHAR(200) DEFAULT NULL,
    ADD COLUMN `oauth_endpoint`      VARCHAR(16)  DEFAULT NULL,
    ADD COLUMN `account_id`          VARCHAR(64)  DEFAULT NULL,
    ADD COLUMN `account_host`        VARCHAR(255) DEFAULT NULL,
    ADD KEY `fk_feature_store_unity_catalog_connector_client_secret_idx`
        (`client_secret_uid`, `client_secret_name`),
    ADD CONSTRAINT `fk_feature_store_unity_catalog_connector_client_secret`
        FOREIGN KEY (`client_secret_uid`, `client_secret_name`)
        REFERENCES `hopsworks`.`secrets` (`uid`, `secret_name`) ON DELETE RESTRICT;
