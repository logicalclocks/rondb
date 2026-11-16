-- RONDB-1158: rate limit groups. Mirrors the Hopsworks migration that
-- creates the three group tables plus the one-row version counter the RDRS
-- sync loop short-circuits on (design doc section 8/15). The "one of two
-- columns is set" and no-ties rules are enforced by Hopsworks application
-- code, as for the sharing tables; the unique keys prevent duplicate rows.

CREATE TABLE `rate_limit_group` (
  `id`             int(11)      NOT NULL AUTO_INCREMENT,
  `name`           varchar(63)  COLLATE latin1_general_cs NOT NULL,
  `budget_percent` decimal(5,2) NOT NULL,
  `data_scope`     varchar(10)  COLLATE latin1_general_cs NOT NULL DEFAULT 'SELECTED',
  `created`        timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_by`     int(11)      DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  CONSTRAINT `rlg_created_by_fk` FOREIGN KEY (`created_by`) REFERENCES `users` (`uid`)
    ON DELETE SET NULL ON UPDATE NO ACTION
) ENGINE = ndbcluster;

CREATE TABLE `rate_limit_group_user` (
  `id`         int(11) NOT NULL AUTO_INCREMENT,
  `group_id`   int(11) NOT NULL,
  `uid`        int(11) DEFAULT NULL,
  `project_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `group_uid`     (`group_id`, `uid`),
  UNIQUE KEY `group_project` (`group_id`, `project_id`),
  CONSTRAINT `rlgu_group_fk`   FOREIGN KEY (`group_id`)   REFERENCES `rate_limit_group` (`id`) ON DELETE CASCADE,
  CONSTRAINT `rlgu_uid_fk`     FOREIGN KEY (`uid`)        REFERENCES `users`   (`uid`) ON DELETE CASCADE,
  CONSTRAINT `rlgu_project_fk` FOREIGN KEY (`project_id`) REFERENCES `project` (`id`)  ON DELETE CASCADE
) ENGINE = ndbcluster;

CREATE TABLE `rate_limit_group_data` (
  `id`               int(11) NOT NULL AUTO_INCREMENT,
  `group_id`         int(11) NOT NULL,
  `feature_group_id` int(11) DEFAULT NULL,
  `feature_store_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `group_fg` (`group_id`, `feature_group_id`),
  UNIQUE KEY `group_fs` (`group_id`, `feature_store_id`),
  CONSTRAINT `rlgd_group_fk` FOREIGN KEY (`group_id`)         REFERENCES `rate_limit_group` (`id`) ON DELETE CASCADE,
  CONSTRAINT `rlgd_fg_fk`    FOREIGN KEY (`feature_group_id`) REFERENCES `feature_group` (`id`)    ON DELETE CASCADE,
  CONSTRAINT `rlgd_fs_fk`    FOREIGN KEY (`feature_store_id`) REFERENCES `feature_store` (`id`)    ON DELETE CASCADE
) ENGINE = ndbcluster;

-- One row (id = 1). Hopsworks bumps `version` in every transaction that
-- writes the three tables above; the sync loop reads it back by primary key
-- and skips the run when it matches what it last applied.
CREATE TABLE `rate_limit_group_version` (
  `id`      int(11)    NOT NULL,
  `version` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE = ndbcluster;
