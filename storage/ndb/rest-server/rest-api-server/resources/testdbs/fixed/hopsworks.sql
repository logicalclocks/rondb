-- This file is part of the RonDB REST API Server
-- Copyright (c) 2023 Hopsworks AB
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation, version 3.
--
-- This program is distributed in the hope that it will be useful, but
-- WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
-- General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program. If not, see <http://www.gnu.org/licenses/>.

DROP DATABASE IF EXISTS hopsworks;

CREATE DATABASE hopsworks;

USE hopsworks;

CREATE TABLE `users` (
    `uid` int NOT NULL AUTO_INCREMENT,
    `username` varchar(10) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
    `password` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
    `email` varchar(150) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
    `fname` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
    `lname` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
    `activated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `title` varchar(10) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT '-',
    `false_login` int NOT NULL DEFAULT '-1',
    `status` int NOT NULL DEFAULT '-1',
    `isonline` int NOT NULL DEFAULT '-1',
    `secret` varchar(20) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
    `validation_key` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
    `validation_key_updated` timestamp NULL DEFAULT NULL,
    `validation_key_type` varchar(20) COLLATE latin1_general_cs DEFAULT NULL,
    `mode` int NOT NULL DEFAULT '0',
    `password_changed` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `notes` varchar(500) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT '-',
    `max_num_projects` int NOT NULL,
    `num_active_projects` int NOT NULL DEFAULT '0',
    `num_created_projects` int NOT NULL DEFAULT '0',
    `two_factor` tinyint(1) NOT NULL DEFAULT '1',
    `tours_state` tinyint(1) NOT NULL DEFAULT '0',
    `salt` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL DEFAULT '',
    PRIMARY KEY (`uid`),
    UNIQUE KEY `username` (`username`),
    UNIQUE KEY `email` (`email`)
) ENGINE = ndbcluster;

CREATE TABLE `project` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `inode_pid` bigint(20) NOT NULL,
    `inode_name` varchar(255) COLLATE latin1_general_cs NOT NULL,
    `partition_id` bigint(20) NOT NULL,
    `projectname` varchar(100) COLLATE latin1_general_cs NOT NULL,
    `username` varchar(150) COLLATE latin1_general_cs NOT NULL,
    `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `description` varchar(2000) COLLATE latin1_general_cs DEFAULT NULL,
    `payment_type` varchar(255) COLLATE latin1_general_cs NOT NULL DEFAULT 'PREPAID',
    `last_quota_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `kafka_max_num_topics` int(11) NOT NULL DEFAULT '100',
    `docker_image` varchar(255) COLLATE latin1_general_cs DEFAULT NULL,
    `python_env_id` int(11) DEFAULT NULL,
    `creation_status` tinyint(1) NOT NULL DEFAULT '0',
    PRIMARY KEY (`id`),
    UNIQUE KEY `projectname` (`projectname`),
    UNIQUE KEY `inode_pid` (`inode_pid`,`inode_name`,`partition_id`),
    KEY `user_idx` (`username`),
    -- "CONSTRAINT `FK_149_289` FOREIGN KEY (`inode_pid`, `inode_name`, `partition_id`) REFERENCES `hops`.`hdfs_inodes` (`parent_id`, `name`, `partition_id`) ON DELETE CASCADE,
    CONSTRAINT `FK_262_290` FOREIGN KEY (`username`) REFERENCES `users` (`email`)
) ENGINE = ndbcluster;

CREATE TABLE `project_team` (
    `project_id` int NOT NULL,
    `team_member` varchar(150) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
    `team_role` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
    `added` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`project_id`, `team_member`),
    KEY `team_member` (`team_member`),
    CONSTRAINT `FK_262_304` FOREIGN KEY (`team_member`) REFERENCES `users` (`email`) ON DELETE CASCADE,
    CONSTRAINT `FK_284_303` FOREIGN KEY (`project_id`) REFERENCES `project` (`id`) ON DELETE CASCADE
) ENGINE = ndbcluster;

CREATE TABLE `api_key` (
    `id` int NOT NULL AUTO_INCREMENT,
    `prefix` varchar(45) COLLATE latin1_general_cs NOT NULL,
    `secret` varchar(512) COLLATE latin1_general_cs NOT NULL,
    `salt` varchar(256) COLLATE latin1_general_cs NOT NULL,
    `created` timestamp NOT NULL,
    `modified` timestamp NOT NULL,
    `name` varchar(45) COLLATE latin1_general_cs NOT NULL,
    `user_id` int NOT NULL,
    `reserved` tinyint(1) DEFAULT '0',
    PRIMARY KEY (`id`),
    UNIQUE KEY `prefix_UNIQUE` (`prefix`),
    UNIQUE KEY `index4` (`user_id`, `name`),
    KEY `fk_api_key_1_idx` (`user_id`),
    CONSTRAINT `fk_api_key_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`uid`) ON DELETE CASCADE
) ENGINE = ndbcluster;

INSERT INTO
    `users`
VALUES
    (
        10000,
        'macho',
        '12fa520ec8f65d3a6feacfa97a705e622e1fea95b80b521ec016e43874dfed5a',
        'macho@hopsworks.ai',
        '',
        'macho',
        '2015-05-15 10:22:36',
        'Mr',
        0,
        2,
        1,
        'V3WBPS4G2WMQ53VA',
        NULL,
        NULL,
        NULL,
        0,
        '2015-04-28 15:18:42',
        NULL,
        30,
        2,
        1,
        0,
        3,
        '+9mTLmYSpnZROFEJEaednw8+GDH/s2J1QuRZy8okxW5myI/q8ek8Xu+ab5CyE9GzhWX6Sa4cr7KX8cAHi5IC4g=='
    );

INSERT INTO
    `project`
VALUES
    (
        999, 1, 'demo0', 0 , 'demo0', 'macho@hopsworks.ai', '2022-05-30 14:17:22', 'Some desc', 'NOLIMIT', '2022-05-30 14:17:38', 100, 'SomeDockerImage', 1, 0
    ),
    (
        1000, 2, 'fsdb001', 0, 'fsdb001', 'macho@hopsworks.ai', Timestamp('2023-03-16 14:27:17'), 'Some desc', 'NOLIMIT', Timestamp('2023-03-16 14:27:18'), 100, 'SomeDockerImage', 14.0, 0
    ),
    (
        1001, 3, 'fsdb002', 0, 'fsdb002', 'macho@hopsworks.ai', Timestamp('2023-04-20 16:14:15'), 'Some desc', 'NOLIMIT', Timestamp('2023-04-20 16:14:15'), 100, 'SomeDockerImage', 1025.0, 0
    );

INSERT INTO
    `project_team`
VALUES
    (
        999,
        'macho@hopsworks.ai',
        'Data scientist',
        '2022-06-01 13:28:05'
    );

-- We're inserting our test API key into the database, so we can test authentication
-- This API key is:
--      bkYjEz6OTZyevbqt.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub
INSERT INTO
    `api_key`
VALUES
    (
        1,
        'bkYjEz6OTZyevbqt',
        '709faa77accc3f30394cfb53b67253ba64881528cb3056eea110703ca430cce4',
        '1/1TxiaiIB01rIcY2E36iuwKP6fm2GzBaNaQqOVGMhH0AvcIlIzaUIw0fMDjKNLa0OWxAOrfTSPqAolpI/n+ug==',
        '2022-06-14 10:27:03',
        '2022-06-14 10:27:03',
        'myapikey1',
        10000,
        0
    );

CREATE TABLE `feature_store` (
                                 `id` int(11) NOT NULL AUTO_INCREMENT,
                                 `name` varchar(100) COLLATE latin1_general_cs NOT NULL,
                                 `project_id` int(11) NOT NULL,
                                 `created` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
                                 `hive_db_id` bigint(20) NOT NULL,
                                 PRIMARY KEY (`id`),
                                 KEY `project_id` (`project_id`),
                                 KEY `hive_db_id` (`hive_db_id`),
                                --  CONSTRAINT `FK_368_663` FOREIGN KEY (`hive_db_id`) REFERENCES `metastore`.`DBS` (`DB_ID`) ON DELETE CASCADE ON UPDATE NO ACTION,
                                 CONSTRAINT `FK_883_662` FOREIGN KEY (`project_id`) REFERENCES `project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=ndbcluster AUTO_INCREMENT=67 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

-- CREATE TABLE `on_demand_feature_group` (
--                                                          `id`                      INT(11)         NOT NULL AUTO_INCREMENT,
--                                                          `query`                   VARCHAR(26000),
--                                                          `connector_id`            INT(11)         NOT NULL,
--                                                          `description`             VARCHAR(1000)   NULL,
--                                                          `inode_pid`               BIGINT(20)      NOT NULL,
--                                                          `inode_name`              VARCHAR(255)    NOT NULL,
--                                                          `partition_id`            BIGINT(20)      NOT NULL,
--                                                          `data_format`             VARCHAR(10),
--                                                          `path`                    VARCHAR(1000),
--                                                          PRIMARY KEY (`id`)
--                                                         --  CONSTRAINT `on_demand_conn_fk` FOREIGN KEY (`connector_id`) REFERENCES `feature_store_connector` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
--                                                         --  CONSTRAINT `on_demand_inode_fk` FOREIGN KEY (`inode_pid`, `inode_name`, `partition_id`) REFERENCES `hops`.`hdfs_inodes` (`parent_id`, `name`, `partition_id`) ON DELETE CASCADE ON UPDATE NO ACTION
-- ) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

-- CREATE TABLE `cached_feature_group` (
--                                                       `id`                             INT(11)         NOT NULL AUTO_INCREMENT,
--                                                       `offline_feature_group`          BIGINT(20)      NOT NULL,
--                                                       `timetravel_format`              INT NOT NULL DEFAULT 1,
--                                                       PRIMARY KEY (`id`)
--                                                     --   CONSTRAINT `cached_fg_hive_fk` FOREIGN KEY (`offline_feature_group`) REFERENCES `metastore`.`TBLS` (`TBL_ID`) ON DELETE CASCADE ON UPDATE NO ACTION
-- ) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

-- CREATE TABLE `stream_feature_group` (
--                                                       `id`                             INT(11) NOT NULL AUTO_INCREMENT,
--                                                       `offline_feature_group`          BIGINT(20) NOT NULL,
--                                                       PRIMARY KEY (`id`)
--                                                     --   CONSTRAINT `stream_fg_hive_fk` FOREIGN KEY (`offline_feature_group`) REFERENCES `metastore`.`TBLS` (`TBL_ID`) ON DELETE CASCADE ON UPDATE NO ACTION
-- ) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

-- CREATE TABLE `cached_feature` (
--   `id` int(11) NOT NULL AUTO_INCREMENT,
--   `cached_feature_group_id` int(11) NULL,
--   `stream_feature_group_id` int(11) NULL,
--   `name` varchar(63) COLLATE latin1_general_cs NOT NULL,
--   `description` varchar(256) NOT NULL DEFAULT '',
--   PRIMARY KEY (`id`),
--   KEY `cached_feature_group_fk` (`cached_feature_group_id`),
--   KEY `stream_feature_group_fk` (`stream_feature_group_id`),
--   CONSTRAINT `cached_feature_group_fk2` FOREIGN KEY (`cached_feature_group_id`) REFERENCES `cached_feature_group` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
--   CONSTRAINT `stream_feature_group_fk2` FOREIGN KEY (`stream_feature_group_id`) REFERENCES `stream_feature_group` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
-- ) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

-- CREATE TABLE `on_demand_feature` (
--                                      `id` int(11) NOT NULL AUTO_INCREMENT,
--                                      `on_demand_feature_group_id` int(11) NULL,
--                                      `name` varchar(1000) COLLATE latin1_general_cs NOT NULL,
--                                      `primary_column` tinyint(1) NOT NULL DEFAULT '0',
--                                      `description` varchar(10000) COLLATE latin1_general_cs,
--                                      `type` varchar(1000) COLLATE latin1_general_cs NOT NULL,
--                                      `idx` int(11) NOT NULL DEFAULT 0,
--                                      `default_value` VARCHAR(400) NULL,
--                                      PRIMARY KEY (`id`),
--                                      KEY `on_demand_feature_group_fk` (`on_demand_feature_group_id`),
--                                      CONSTRAINT `on_demand_feature_group_fk1` FOREIGN KEY (`on_demand_feature_group_id`) REFERENCES `on_demand_feature_group` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
-- ) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

CREATE TABLE `feature_group` (
                                 `id` int(11) NOT NULL AUTO_INCREMENT,
                                 `name` varchar(63) NOT NULL,
                                 `feature_store_id` int(11) NOT NULL,
                                 `created` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
                                 `creator` int(11) NOT NULL,
                                 `version` int(11) NOT NULL,
                                 `feature_group_type` INT(11) NOT NULL DEFAULT '0',
                                 `on_demand_feature_group_id` INT(11) NULL,
                                 `cached_feature_group_id` INT(11) NULL,
                                 `stream_feature_group_id` INT(11) NULL,
                                 `event_time` VARCHAR(63) DEFAULT NULL,
                                 `online_enabled` TINYINT(1) NULL,
                                 PRIMARY KEY (`id`),
                                 UNIQUE KEY `name_version` (`feature_store_id`, `name`, `version`),
                                 KEY `feature_store_id` (`feature_store_id`),
                                 KEY `creator` (`creator`),
                                 KEY `on_demand_feature_group_fk` (`on_demand_feature_group_id`),
                                 KEY `cached_feature_group_fk` (`cached_feature_group_id`),
                                 KEY `stream_feature_group_fk` (`stream_feature_group_id`),
                                 CONSTRAINT `FK_1012_790` FOREIGN KEY (`creator`) REFERENCES `users` (`uid`) ON DELETE NO ACTION ON UPDATE NO ACTION,
                                 CONSTRAINT `FK_656_740` FOREIGN KEY (`feature_store_id`) REFERENCES `feature_store` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
                                --  CONSTRAINT `on_demand_feature_group_fk2` FOREIGN KEY (`on_demand_feature_group_id`) REFERENCES `on_demand_feature_group` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
                                --  CONSTRAINT `cached_feature_group_fk` FOREIGN KEY (`cached_feature_group_id`) REFERENCES `cached_feature_group` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
                                --  CONSTRAINT `stream_feature_group_fk` FOREIGN KEY (`stream_feature_group_id`) REFERENCES `stream_feature_group` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=ndbcluster AUTO_INCREMENT=13 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

CREATE TABLE `feature_view` (
                                 `id` int(11) NOT NULL AUTO_INCREMENT,
                                 `name` varchar(63) NOT NULL,
                                 `feature_store_id` int(11) NOT NULL,
                                 `created` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
                                 `creator` int(11) NOT NULL,
                                 `version` int(11) NOT NULL,
                                 `description` varchar(10000) COLLATE latin1_general_cs DEFAULT NULL,
                                 `inode_pid` BIGINT(20) NOT NULL,
                                 `inode_name` VARCHAR(255) NOT NULL,
                                 `partition_id` BIGINT(20) NOT NULL,
                                 PRIMARY KEY (`id`),
                                 UNIQUE KEY `name_version` (`feature_store_id`, `name`, `version`),
                                 KEY `feature_store_id` (`feature_store_id`),
                                 KEY `creator` (`creator`),
                                 CONSTRAINT `fv_creator_fk` FOREIGN KEY (`creator`) REFERENCES `users` (`uid`) ON
                                     DELETE NO ACTION ON UPDATE NO ACTION,
                                 CONSTRAINT `fv_feature_store_id_fk` FOREIGN KEY (`feature_store_id`) REFERENCES
                                     `feature_store` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
                                --  CONSTRAINT `fv_inode_fk` FOREIGN KEY (`inode_pid`, `inode_name`, `partition_id`) REFERENCES
                                    --  `hops`.`hdfs_inodes` (`parent_id`, `name`, `partition_id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=ndbcluster AUTO_INCREMENT=9 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

CREATE TABLE `training_dataset_join` (
                                         `id` int(11) NOT NULL AUTO_INCREMENT,
                                         `training_dataset` int(11) NULL,
                                         `feature_group` int(11) NULL,
                                         `feature_group_commit_id` BIGINT(20) NULL,
                                         `type` tinyint(5) NOT NULL DEFAULT 0,
                                         `idx` int(11) NOT NULL DEFAULT 0,
                                         `prefix` VARCHAR(63) NULL,
                                         `feature_view_id` INT(11) NULL,
                                         PRIMARY KEY (`id`),
                                         KEY `fg_key` (`feature_group`),
                                         CONSTRAINT `tdj_feature_view_fk` FOREIGN KEY  (`feature_view_id`) REFERENCES
                                             `feature_view` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
                                        --  CONSTRAINT `td_fk_tdj` FOREIGN KEY (`training_dataset`) REFERENCES `training_dataset` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
                                         CONSTRAINT `fg_left` FOREIGN KEY (`feature_group`) REFERENCES `feature_group` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

CREATE TABLE `training_dataset_feature` (
                                            `id` int(11) NOT NULL AUTO_INCREMENT,
                                            `training_dataset` int(11) NULL,
                                            `feature_group` int(11) NULL,
                                            `name` varchar(1000) COLLATE latin1_general_cs NOT NULL,
                                            `type` varchar(1000) COLLATE latin1_general_cs,
                                            `td_join`int(11) NULL,
                                            `idx` int(11) NULL,
                                            `label` tinyint(1) NOT NULL DEFAULT '0',
                                            `transformation_function`  int(11) NULL,
                                            `feature_view_id` INT(11) NULL,
                                            PRIMARY KEY (`id`),
                                            KEY `td_key` (`training_dataset`),
                                            KEY `fg_key` (`feature_group`),
                                            CONSTRAINT `tdf_feature_view_fk` FOREIGN KEY  (`feature_view_id`)
                                                REFERENCES `feature_view` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
                                            -- CONSTRAINT `join_fk_tdf` FOREIGN KEY (`td_join`) REFERENCES `training_dataset_join` (`id`) ON DELETE SET NULL ON UPDATE NO ACTION,
                                            -- CONSTRAINT `td_fk_tdf` FOREIGN KEY (`training_dataset`) REFERENCES `training_dataset` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
                                            CONSTRAINT `fg_fk_tdf` FOREIGN KEY (`feature_group`) REFERENCES `feature_group` (`id`) ON DELETE SET NULL ON UPDATE NO ACTION
                                            -- CONSTRAINT `tfn_fk_tdf` FOREIGN KEY (`transformation_function`) REFERENCES `transformation_function` (`id`) ON DELETE SET NULL ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

INSERT INTO
    `feature_store`
VALUES
    (
        67, "fsdb001", 1000, "2023-03-16 14:27:29", 4
    ),
    (
        1091, "fsdb002", 1001, "2023-03-16 14:27:29", 4
    );

INSERT INTO
    `feature_group`
VALUES
    (
        2068, 'sample_2', 1091, Timestamp('2023-04-21 09:32:38'), 10000, 1, 2, NULL, NULL, 2056, 'ts', 1
    ),
    (
        2069, 'sample_1', 67, Timestamp('2023-04-21 09:33:40'), 10000, 1, 2, NULL, NULL, 2057, 'ts', 1
    ),
    (
        2071, 'sample_2', 67, Timestamp('2023-04-21 09:37:25'), 10000, 1, 2, NULL, NULL, 2059, 'ts', 1
    ),
    (
        2072, 'sample_1', 1091, Timestamp('2023-04-21 10:00:40'), 10000, 1, 2, NULL, NULL, 2060, 'ts', 1
    ),
    (
        2070, 'sample_1', 67, Timestamp('2023-04-21 09:35:38'), 10000, 2, 2, NULL, NULL, 2058, 'ts', 1
    );

INSERT INTO
    `feature_view`
VALUES
    (
        2059, 'sample_1', 67, Timestamp('2023-04-21 09:52:51'), 10000, 1, '', 250, 'sample_1_1', 250
    ),
    (
        2060, 'sample_2', 67, Timestamp('2023-04-21 09:52:52'), 10000, 1, '', 250, 'sample_2_1', 250
    ),
    (
        2061, 'sample_1n2', 67, Timestamp('2023-04-21 09:52:53'), 10000, 1, '', 250, 'sample_1n2_1', 250
    ),
    (
        2064, 'sample_2', 1091, Timestamp('2023-04-21 10:03:49'), 10000, 1, '', 30509, 'sample_2_1', 30509
    ),
    (
        2065, 'sample_1n2', 1091, Timestamp('2023-04-21 10:03:51'), 10000, 1, '', 30509, 'sample_1n2_1', 30509
    ),
    (
        2066, 'sample_share_1n2', 67, Timestamp('2023-04-21 11:10:32'), 10000, 1, '', 250, 'sample_share_1n2_1', 250
    ),
    (
        2063, 'sample_1', 1091, Timestamp('2023-04-21 10:03:48'), 10000, 2, '', 30509, 'sample_1_2', 30509
    );

INSERT INTO 
    `training_dataset_join`
VALUES
    (
        2053, NULL, 2071, NULL, 0, 1, 'fg2_', 2061
    ),
    (
        2056, NULL, 2072, NULL, 0, 0, NULL, 2063
    ),
    (
        2058, NULL, 2068, NULL, 0, 1, 'fg2_', 2065
    ),
    (
        2059, NULL, 2072, NULL, 0, 0, NULL, 2065
    ),
    (
        2060, NULL, 2069, NULL, 0, 0, NULL, 2066
    ),
    (
        2061, NULL, 2068, NULL, 0, 1, 'fg2_', 2066
    ),
    (
        2051, NULL, 2069, NULL, 0, 0, NULL, 2059
    ),
    (
        2052, NULL, 2071, NULL, 0, 0, NULL, 2060
    ),
    (
        2054, NULL, 2069, NULL, 0, 0, NULL, 2061
    ),
    (
        2057, NULL, 2068, NULL, 0, 0, NULL, 2064
    );

INSERT INTO
    `training_dataset_feature`
VALUES
    (
        2058, NULL, 2069, 'id1', 'bigint', 2051, 0, 0, NULL, 2059
    ),
    (
        2059, NULL, 2069, 'ts', 'timestamp', 2051, 1, 0, NULL, 2059
    ),
    (
        2060, NULL, 2069, 'data2', 'bigint', 2051, 3, 0, NULL, 2059
    ),
    (
        2061, NULL, 2071, 'data2', 'string', 2052, 3, 0, NULL, 2060
    ),
    (
        2062, NULL, 2071, 'data1', 'string', 2052, 2, 0, NULL, 2060
    ),
    (
        2064, NULL, 2071, 'id1', 'bigint', 2052, 0, 0, NULL, 2060
    ),
    (
        2065, NULL, 2069, 'ts', 'timestamp', 2054, 1, 0, NULL, 2061
    ),
    (
        2066, NULL, 2071, 'id1', 'bigint', 2053, 4, 0, NULL, 2061
    ),
    (
        2067, NULL, 2071, 'data1', 'string', 2053, 6, 0, NULL, 2061
    ),
    (
        2070, NULL, 2069, 'data1', 'bigint', 2054, 2, 0, NULL, 2061
    ),
    (
        2072, NULL, 2069, 'data2', 'bigint', 2054, 3, 0, NULL, 2061
    ),
    (
        2077, NULL, 2072, 'data1', 'bigint', 2056, 2, 0, NULL, 2063
    ),
    (
        2079, NULL, 2072, 'id1', 'bigint', 2056, 0, 0, NULL, 2063
    ),
    (
        2080, NULL, 2072, 'ts', 'timestamp', 2056, 1, 0, NULL, 2063
    ),
    (
        2082, NULL, 2068, 'id1', 'bigint', 2057, 0, 0, NULL, 2064
    ),
    (
        2083, NULL, 2068, 'ts', 'date', 2057, 1, 0, NULL, 2064
    ),
    (
        2084, NULL, 2068, 'data2', 'string', 2057, 3, 0, NULL, 2064
    ),
    (
        2085, NULL, 2068, 'data2', 'string', 2058, 7, 0, NULL, 2065
    ),
    (
        2086, NULL, 2068, 'ts', 'date', 2058, 5, 0, NULL, 2065
    ),
    (
        2094, NULL, 2069, 'data1', 'bigint', 2060, 2, 0, NULL, 2066
    ),
    (
        2097, NULL, 2068, 'id1', 'bigint', 2061, 4, 0, NULL, 2066
    ),
    (
        2098, NULL, 2068, 'data2', 'string', 2061, 7, 0, NULL, 2066
    ),
    (
        2100, NULL, 2068, 'data1', 'string', 2061, 6, 0, NULL, 2066
    ),
    (
        2057, NULL, 2069, 'data1', 'bigint', 2051, 2, 0, NULL, 2059
    ),
    (
        2063, NULL, 2071, 'ts', 'date', 2052, 1, 0, NULL, 2060
    ),
    (
        2068, NULL, 2071, 'ts', 'date', 2053, 5, 0, NULL, 2061
    ),
    (
        2069, NULL, 2071, 'data2', 'string', 2053, 7, 0, NULL, 2061
    ),
    (
        2071, NULL, 2069, 'id1', 'bigint', 2054, 0, 0, NULL, 2061
    ),
    (
        2078, NULL, 2072, 'data2', 'bigint', 2056, 3, 0, NULL, 2063
    ),
    (
        2081, NULL, 2068, 'data1', 'string', 2057, 2, 0, NULL, 2064
    ),
    (
        2087, NULL, 2072, 'id1', 'bigint', 2059, 0, 0, NULL, 2065
    ),
    (
        2088, NULL, 2072, 'data2', 'bigint', 2059, 3, 0, NULL, 2065
    ),
    (
        2089, NULL, 2072, 'ts', 'timestamp', 2059, 1, 0, NULL, 2065
    ),
    (
        2090, NULL, 2072, 'data1', 'bigint', 2059, 2, 0, NULL, 2065
    ),
    (
        2091, NULL, 2068, 'id1', 'bigint', 2058, 4, 0, NULL, 2065
    ),
    (
        2092, NULL, 2068, 'data1', 'string', 2058, 6, 0, NULL, 2065
    ),
    (
        2093, NULL, 2069, 'id1', 'bigint', 2060, 0, 0, NULL, 2066
    ),
    (
        2095, NULL, 2069, 'data2', 'bigint', 2060, 3, 0, NULL, 2066
    ),
    (
        2096, NULL, 2069, 'ts', 'timestamp', 2060, 1, 0, NULL, 2066
    ),
    (
        2099, NULL, 2068, 'ts', 'date', 2061, 5, 0, NULL, 2066
    );
