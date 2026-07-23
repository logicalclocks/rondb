-- Backfill explicit proxy routing mode for legacy Python app configs.
UPDATE `hopsworks`.`jobs`
SET `json_config` = JSON_SET(`json_config`, '$.proxyPathMode', 'PREFIX')
WHERE `type` = 'PYTHON_APP'
  AND JSON_VALID(`json_config`)
  AND JSON_CONTAINS_PATH(`json_config`, 'one', '$.proxyPathMode') = 0;
