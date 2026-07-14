CREATE TABLE IF NOT EXISTS `hopsworks`.`shared_feature_store`
(
    `id`                    INT(11) AUTO_INCREMENT PRIMARY KEY,
    `feature_store`         INT(11) NOT NULL,
    `shared_by`             INT(11) NOT NULL,
    `shared_on`             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `shared_with_project`   INT(11) NOT NULL,
    `shared_entirely`       TINYINT(1) NOT NULL DEFAULT 1,
    CONSTRAINT `sfs_fs_fk` FOREIGN KEY (`feature_store`) REFERENCES `feature_store` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `sfs_user_fk` FOREIGN KEY (`shared_by`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `sfs_project_fk` FOREIGN KEY (`shared_with_project`) REFERENCES `project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster
DEFAULT CHARSET = latin1
COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `hopsworks`.`shared_feature_group`
(
    `id`                    INT(11) AUTO_INCREMENT PRIMARY KEY,
    `feature_store`         INT(11) NOT NULL,
    `feature_group`         INT(11) NOT NULL,
    `shared_by`             INT(11) NOT NULL,
    `shared_on`             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `shared_with_project`   INT(11) NOT NULL,
    `shared_entirely`       TINYINT(1) NOT NULL DEFAULT 1,
    CONSTRAINT `sfg_fs_fk` FOREIGN KEY (`feature_store`) REFERENCES `feature_store` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `sfg_fg_fk` FOREIGN KEY (`feature_group`) REFERENCES `feature_group` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `sfg_user_fk` FOREIGN KEY (`shared_by`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `sfg_project_fk` FOREIGN KEY (`shared_with_project`) REFERENCES `project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster
DEFAULT CHARSET = latin1
COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `hopsworks`.`shared_feature`
(
    `id`                    INT(11) AUTO_INCREMENT PRIMARY KEY,
    `feature_group`         INT(11) NOT NULL,
    `feature`               VARCHAR(63) NOT NULL,
    `shared_by`             INT(11) NOT NULL,
    `shared_on`             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `shared_with_project`   INT(11) NOT NULL,
    CONSTRAINT `sft_fg_fk` FOREIGN KEY (`feature_group`) REFERENCES `feature_group` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `sft_user_fk` FOREIGN KEY (`shared_by`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `sft_project_fk` FOREIGN KEY (`shared_with_project`) REFERENCES `project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster
DEFAULT CHARSET = latin1
COLLATE = latin1_general_cs;

-- RDRS-P1-PORT: production-data migration DML commented out. It backfills
-- shared_feature_store from pre-existing dataset_shared_with rows and then
-- deletes them - a no-op on fresh test fixtures. The DELETE..JOIN with a
-- non-key WHERE is also rejected by MySQL safe-update mode (Error 1175) in
-- the test environment.
-- Migrate existing shared dataset into the "shared feature store". Before this migration, all shared feature stores
-- are shared entirely.
-- INSERT INTO `hopsworks`.`shared_feature_store`(feature_store, shared_by, shared_on, shared_with_project, shared_entirely)
-- SELECT feature_store_id AS feature_store
--      , shared_by
--      , shared_on
--      , project AS shared_with_project
--      , 1 AS shared_entirely
-- FROM `hopsworks`.`dataset_shared_with` dsw
--     JOIN `hopsworks`.`dataset` ds ON dsw.dataset = ds.id
-- WHERE feature_store_id IS NOT NULL;

-- We should also remove feature store shared datasets from the table. The permissions will be adjusted by the timer
-- bean running in Hopsworks
-- DELETE dsw
-- FROM `hopsworks`.`dataset_shared_with` dsw
--     JOIN `hopsworks`.`dataset` ds ON dsw.dataset = ds.id
-- WHERE `inode_name` = "Statistics"
--        OR `inode_name` = "DataValidation"
--        OR `inode_name` LIKE "%_Training_Datasets"
--        OR `feature_store_id` IS NOT NULL;
