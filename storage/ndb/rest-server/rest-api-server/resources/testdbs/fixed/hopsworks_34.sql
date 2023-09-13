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
    `two_factor` tinyint(1) NOT NULL DEFAULT '1',
    `tours_state` tinyint(1) NOT NULL DEFAULT '0',
    `salt` varchar(128) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL DEFAULT '',
    PRIMARY KEY (`uid`),
    UNIQUE KEY `username` (`username`),
    UNIQUE KEY `email` (`email`)
) ENGINE = ndbcluster;

CREATE TABLE `project` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
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
    KEY `user_idx` (`username`),
    CONSTRAINT `FK_262_290` FOREIGN KEY (`username`) REFERENCES `users` (`email`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=ndbcluster AUTO_INCREMENT=119 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

CREATE TABLE `project_team` (
    `project_id` int(11) NOT NULL,
    `team_member` varchar(150) COLLATE latin1_general_cs NOT NULL,
    `team_role` varchar(32) COLLATE latin1_general_cs NOT NULL,
    `added` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`project_id`,`team_member`),
    KEY `team_member` (`team_member`),
    CONSTRAINT `FK_262_304` FOREIGN KEY (`team_member`) REFERENCES `users` (`email`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `FK_284_303` FOREIGN KEY (`project_id`) REFERENCES `project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

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
        999, 'demo0', 'macho@hopsworks.ai', Timestamp('2022-05-30 14:17:22'), 'Some desc', 'NOLIMIT', Timestamp('2022-05-30 14:17:38'), 100, 'SomeDockerImage', 1, 0
    ),
    (
        1000, 'fsdb001', 'macho@hopsworks.ai', Timestamp('2023-03-16 14:27:17'), 'Some desc', 'NOLIMIT', Timestamp('2023-03-16 14:27:18'), 100, 'SomeDockerImage', 14.0, 0
    ),
    (
        1001, 'fsdb002', 'macho@hopsworks.ai', Timestamp('2023-04-20 16:14:15'), 'Some desc', 'NOLIMIT', Timestamp('2023-04-20 16:14:15'), 100, 'SomeDockerImage', 1025.0, 0
    ),
    (
        1002, 'fsdb_isolate', 'macho@hopsworks.ai', Timestamp('2023-04-20 16:14:15'), 'Some desc', 'NOLIMIT', Timestamp('2023-04-20 16:14:15'), 100, 'SomeDockerImage', 1025.0, 0
    );

INSERT INTO
    `project_team`
VALUES
    (
        999,
        'macho@hopsworks.ai',
        'Data scientist',
        '2022-06-01 13:28:05'
    ),
    (
        1000,
        'macho@hopsworks.ai',
        'Data scientist',
        '2022-06-01 13:28:05'
    ),
    (
        1001,
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
                                 CONSTRAINT `FK_883_662` FOREIGN KEY (`project_id`) REFERENCES `project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=ndbcluster AUTO_INCREMENT=67 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

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
) ENGINE=ndbcluster AUTO_INCREMENT=13 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

CREATE TABLE `feature_view` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `name` varchar(63) NOT NULL,
    `feature_store_id` int(11) NOT NULL,
    `created` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    `creator` int(11) NOT NULL,
    `version` int(11) NOT NULL,
    `description` varchar(10000) COLLATE latin1_general_cs DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `name_version` (`feature_store_id`, `name`, `version`),
    KEY `feature_store_id` (`feature_store_id`),
    KEY `creator` (`creator`),
    CONSTRAINT `fv_creator_fk` FOREIGN KEY (`creator`) REFERENCES `users` (`uid`) ON
        DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT `fv_feature_store_id_fk` FOREIGN KEY (`feature_store_id`) REFERENCES
        `feature_store` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
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
    CONSTRAINT `join_fk_tdf` FOREIGN KEY (`td_join`) REFERENCES `training_dataset_join` (`id`) ON DELETE SET NULL ON UPDATE NO ACTION,
    CONSTRAINT `fg_fk_tdf` FOREIGN KEY (`feature_group`) REFERENCES `feature_group` (`id`) ON DELETE SET NULL ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

INSERT INTO
    `feature_store`
VALUES
    (
        66, "fsdb_isolate", 1002, "2023-03-16 14:27:29", 4
    ),
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
    ),
    (
        2076, 'sample_3', 67, Timestamp('2023-05-08 15:20:51'), 10000, 1, 2, NULL, NULL, 2064, 'ts', 1
    ),
    (
        2067, 'sample_4', 66, Timestamp('2023-05-08 15:20:51'), 10000, 1, 2, NULL, NULL, 2055, 'ts', 1
    ),
    (
	    3089, 'sample_cache', 67, Timestamp('2023-06-15 11:46:25'), 10000, 1, 0, NULL, 1025, NULL, NULL, 1
    );

INSERT INTO
    `feature_view`
VALUES
    (
        2059, 'sample_1', 67, Timestamp('2023-04-21 09:52:51'), 10000, 1, ''
    ),
    (
        2060, 'sample_2', 67, Timestamp('2023-04-21 09:52:52'), 10000, 1, ''
    ),
    (
        2061, 'sample_1n2', 67, Timestamp('2023-04-21 09:52:53'), 10000, 1, ''
    ),
    (
        2064, 'sample_2', 1091, Timestamp('2023-04-21 10:03:49'), 10000, 1, ''
    ),
    (
        2065, 'sample_1n2', 1091, Timestamp('2023-04-21 10:03:51'), 10000, 1, ''
    ),
    (
        2066, 'sample_share_1n2', 67, Timestamp('2023-04-21 11:10:32'), 10000, 1, ''
    ),
    (
        2063, 'sample_1', 1091, Timestamp('2023-04-21 10:03:48'), 10000, 2, ''
    ),
    /**
    SELECT `fg0`.`id1` `id1`, `fg0`.`id2` `id2`, `fg0`.`ts` `ts`, `fg0`.`bigint` `bigint`
    FROM `test_ken_featurestore`.`sample_3_1` `fg0`
    */
    (
        2078, 'sample_3', 67, Timestamp('2023-05-09 06:59:06'), 10000, 1, ''
    ),
    /**
    SELECT `fg0`.`id1` `id1`, `fg0`.`id2` `id2`, `fg0`.`ts` `ts`, `fg0`.`bigint` `bigint`, `fg0`.`string` `string`, `fg0`.`date` `date`, `fg0`.`bool` `bool`, `fg0`.`float` `float`, `fg0`.`double` `double`, `fg0`.`binary` `binary`
    FROM `test_ken_featurestore`.`sample_3_1` `fg0`
    */
    (
	    2079, 'sample_3', 67, Timestamp('2023-05-09 12:10:53'), 10000, 2, ''
    ),
    /**
    SELECT `fg0`.`id1` `id1`, `fg0`.`ts` `ts`, `fg0`.`data1` `data1`, `fg0`.`data2` `data2`, `fg1`.`id1` `fg1_id1`, `fg1`.`ts` `fg1_ts`, `fg1`.`data1` `fg1_data1`, `fg1`.`data2` `fg1_data2`
    FROM `test_ken_featurestore`.`sample_1_1` `fg0`
    INNER JOIN `test_ken_featurestore`.`sample_1_2` `fg1` ON `fg0`.`id1` = `fg1`.`id1`
    */
    (
	    2080, 'sample_1n1', 67, Timestamp('2023-05-10 10:45:26'), 10000, 1, ''
    ),
    (
        2085, 'sample_4', 66, Timestamp('2023-05-23 15:31:53'), 10000, 1, ''
    ),
    /**
    SELECT `fg1`.`id1` `id1`, `fg1`.`ts` `ts`, `fg1`.`data1` `data1`, `fg1`.`data2` `data2`, `fg0`.`id1` `fg2_id1`, `fg0`.`ts` `fg2_ts`, `fg0`.`data1` `fg2_data1`, `fg0`.`data2` `fg2_data2`
    FROM `test_ken`.`sample_1_1` `fg1`
    INNER JOIN `test_ken`.`sample_2_1` `fg0` ON `fg1`.`id1` = `fg0`.`id1`
    */
    (
	    3082, 'sample_1n2_label', 67, Timestamp('2023-06-05 13:13:35'), 10000, 1, ''
    ),
    /**
    SELECT `fg1`.`data1` `data1`, `fg0`.`id1` `fg2_id1`, `fg0`.`ts` `fg2_ts`, `fg0`.`data1` `fg2_data1`, `fg0`.`data2` `fg2_data2`
    FROM `test_ken`.`sample_1_1` `fg1`
    INNER JOIN `test_ken`.`sample_2_1` `fg0` ON `fg1`.`id1` = `fg0`.`id1`
    */
    (
        3083, 'sample_1n2_labelonly', 67, Timestamp('2023-06-05 13:15:14'), 10000, 1, ''
    ),
    -- SELECT `fg0`.`id1` `id1`, `fg0`.`data` `data`\nFROM `test_ken`.`sample_cache_1` `fg0`
    (
	    3086, 'sample_cache', 67, Timestamp('2023-06-15 11:49:52'), 10000, 1, ''
    ),
    /**
    SELECT `fg0`.`id1` `id1`, `fg0`.`ts` `ts`, `fg0`.`data1` `data1`, `fg0`.`data2` `data2`, `fg1`.`id1` `fg1_id1`, `fg1`.`ts` `fg1_ts`, `fg1`.`data1` `fg1_data1`, `fg1`.`data2` `fg1_data2`
    FROM `test_ken_featurestore`.`sample_1_1` `fg0`
    INNER JOIN `test_ken_featurestore`.`sample_1_1` `fg1` ON `fg0`.`id1` = `fg1`.`id1`
    */
    (
	    3087, 'sample_1n1_self', 67, Timestamp('2023-05-10 10:45:26'), 10000, 1, ''
    ),
    (
	    3088, 'test_deleted_fg', 67, Timestamp('2023-05-10 10:45:26'), 10000, 1, ''
    ),
    (
	    3089, 'test_deleted_joint_fg', 67, Timestamp('2023-05-10 10:45:26'), 10000, 1, ''
    ),
    /**
    SELECT `fg0`.`id1` `id1`, `fg0`.`ts` `ts`, `fg0`.`data1` `data1`, `fg0`.`data2` `data2`, `fg1`.`bigint` `bigint`
    FROM `test_ken_featurestore`.`sample_1_1` `fg0`
    INNER JOIN `test_ken_featurestore`.`sample_3_1` `fg1` ON `fg0`.`id1` = `fg1`.`bigint`
    */
    (
        4113, 'sample_1n3', 67, Timestamp('2023-08-08 14:00:53'), 10000, 1, ''
    ),
    /**
    SELECT `fg0`.`id1` `id1`, `fg0`.`ts` `ts`, `fg0`.`data1` `data1`, `fg0`.`data2` `data2`, `fg1`.`bigint` `bigint`
    FROM `test_ken_featurestore`.`sample_1_1` `fg0`
    INNER JOIN `test_ken_featurestore`.`sample_3_1` `fg1` ON `fg0`.`data1` = `fg1`.`id1`
    */
    (
        4114, 'sample_1n3_joinoncol', 67, Timestamp('2023-08-09 09:08:02'), 10000, 1, ''
    ),
    /**
    SELECT `fg0`.`id1` `id1`, `fg0`.`ts` `ts`, `fg0`.`data1` `data1`, `fg0`.`data2` `data2`, `fg1`.`bigint` `bigint`
    FROM `test_ken_featurestore`.`sample_1_1` `fg0`
    INNER JOIN `test_ken_featurestore`.`sample_3_1` `fg1` ON `fg0`.`data1` = `fg1`.`bigint`
    */
    (
        4115, 'sample_1n3_joincoloncol', 67, Timestamp('2023-08-09 09:29:37'), 10000, 1, ''
    ),
    /**
    SELECT `fg1`.`id1` `id1`, `fg0`.`id1` `fg2_id1`
    FROM `test_ken`.`sample_1_1` `fg1`
    INNER JOIN `test_ken`.`sample_2_1` `fg0` ON `fg1`.`id1` = `fg0`.`id1`
    */
    (
        4116, 'sample_1n2_pkonly', 67, Timestamp('2023-06-05 13:13:35'), 10000, 1, ''
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
    ),
    (
	    2085, NULL, 2076, NULL, 0, 0, NULL, 2079
    ),
    (
	    2084, NULL, 2076, NULL, 0, 0, NULL, 2078
    ),
    (
	    2086, NULL, 2069, NULL, 0, 0, NULL, 2080
    ),
    (
        2087, NULL, 2070, NULL, 0, 1, 'fg1_', 2080
    ),
    (
	    2096, NULL, 2067, NULL, 0, 0, NULL, 2085
    ),
    (
        3074, NULL, 2071, NULL, 0, 1, 'fg2_', 3082
    ),
    (
        3075, NULL, 2069, NULL, 0, 0, NULL, 3082
    ),
    (
        3076, NULL, 2069, NULL, 0, 0, NULL, 3083
    ),
    (
        3077, NULL, 2071, NULL, 0, 1, 'fg2_', 3083
    ),
    (
	    3082, NULL, 3089, NULL, 0, 0, NULL, 3086
    ),
    (
	    3083, NULL, 2069, NULL, 0, 0, NULL, 3087
    ),
    (
        3084, NULL, 2069, NULL, 0, 1, 'fg1_', 3087
    ),
    (
        3085, NULL, 2069, NULL, 0, 1, 'fg1_', 3089
    ),
    (
        5126, NULL, 2069, NULL, 0, 0, NULL, 4113
    ),
    (
        5125, NULL, 2076, NULL, 0, 1, NULL, 4113
    ),
    (
        5127, NULL, 2069, NULL, 0, 0, NULL, 4114
    ),
    (
        5128, NULL, 2076, NULL, 0, 1, NULL, 4114
    ),
    (
        5129, NULL, 2076, NULL, 0, 1, NULL, 4115
    ),
    (
        5130, NULL, 2069, NULL, 0, 0, NULL, 4115
    ),
    (
        5131, NULL, 2071, NULL, 0, 1, 'fg2_', 4116
    ),
    (
        5132, NULL, 2069, NULL, 0, 0, NULL, 4116
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
    ),
    (
        2182, NULL, 2076, 'id2', 'string', 2084, 1, 0, NULL, 2078
    ),
    (
        2188, NULL, 2076, 'bigint', 'bigint', 2084, 3, 0, NULL, 2078
    ),
    (
        2183, NULL, 2076, 'ts', 'timestamp', 2084, 2, 0, NULL, 2078
    ),
    (
        2189, NULL, 2076, 'id1', 'bigint', 2084, 0, 0, NULL, 2078
    ),
    (
	    2192, NULL, 2076, 'bigint', 'bigint', 2085, 3, 0, NULL, 2079
    ),
    (
        2193, NULL, 2076, 'bool', 'boolean', 2085, 6, 0, NULL, 2079
    ),
    (
        2194, NULL, 2076, 'ts', 'timestamp', 2085, 2, 0, NULL, 2079
    ),
    (
        2195, NULL, 2076, 'date', 'date', 2085, 5, 0, NULL, 2079
    ),
    (
        2198, NULL, 2076, 'binary', 'binary', 2085, 9, 0, NULL, 2079
    ),
    (
        2199, NULL, 2076, 'id2', 'string', 2085, 1, 0, NULL, 2079
    ),
    (
        2190, NULL, 2076, 'double', 'double', 2085, 8, 0, NULL, 2079
    ),
    (
        2191, NULL, 2076, 'string', 'string', 2085, 4, 0, NULL, 2079
    ),
    (
        2196, NULL, 2076, 'id1', 'bigint', 2085, 0, 0, NULL, 2079
    ),
    (
        2197, NULL, 2076, 'float', 'float', 2085, 7, 0, NULL, 2079
    ),
    (
	    2202, NULL, 2070, 'data1', 'bigint', 2087, 6, 0, NULL, 2080
    ),
    (
        2203, NULL, 2069, 'id1', 'bigint', 2086, 0, 0, NULL, 2080
    ),
    (
        2207, NULL, 2070, 'ts', 'timestamp', 2087, 5, 0, NULL, 2080
    ),
    (
        2200, NULL, 2069, 'data1', 'bigint', 2086, 2, 0, NULL, 2080
    ),
    (
        2201, NULL, 2070, 'id1', 'bigint', 2087, 4, 0, NULL, 2080
    ),
    (
        2204, NULL, 2069, 'ts', 'timestamp', 2086, 1, 0, NULL, 2080
    ),
    (
        2205, NULL, 2070, 'data2', 'bigint', 2087, 7, 0, NULL, 2080
    ),
    (
        2206, NULL, 2069, 'data2', 'bigint', 2086, 3, 0, NULL, 2080
    ),
    (
	    2253, NULL, 2067, 'bigint', 'bigint', 2096, 3, 0, NULL, 2085
    ),
    (
        2254, NULL, 2067, 'ts', 'timestamp', 2096, 2, 0, NULL, 2085
    ),
    (
        2260, NULL, 2067, 'id1', 'bigint', 2096, 0, 0, NULL, 2085
    ),
    (
        2261, NULL, 2067, 'id2', 'string', 2096, 1, 0, NULL, 2085
    ),
    (
        3077, NULL, 2069, 'data1', 'bigint', 3075, 2, 1, NULL, 3082
    ),
    (
        3078, NULL, 2071, 'data2', 'string', 3074, 7, 0, NULL, 3082
    ),
    (
        3080, NULL, 2069, 'ts', 'timestamp', 3075, 1, 0, NULL, 3082
    ),
    (
        3081, NULL, 2069, 'data2', 'bigint', 3075, 3, 0, NULL, 3082
    ),
    (
        3084, NULL, 2069, 'id1', 'bigint', 3075, 0, 0, NULL, 3082
    ),
    (
        3087, NULL, 2071, 'data1', 'string', 3077, 3, 0, NULL, 3083
    ),
    (
        3089, NULL, 2071, 'ts', 'date', 3077, 2, 0, NULL, 3083
    ),
    (
        3079, NULL, 2071, 'ts', 'date', 3074, 5, 0, NULL, 3082
    ),
    (
        3082, NULL, 2071, 'id1', 'bigint', 3074, 4, 0, NULL, 3082
    ),
    (
        3083, NULL, 2071, 'data1', 'string', 3074, 6, 0, NULL, 3082
    ),
    (
        3085, NULL, 2071, 'data2', 'string', 3077, 4, 0, NULL, 3083
    ),
    (
        3086, NULL, 2071, 'id1', 'bigint', 3077, 1, 0, NULL, 3083
    ),
    (
        3088, NULL, 2069, 'data1', 'bigint', 3076, 0, 1, NULL, 3083
    ),
    (
        3106, NULL, 3089, 'data', 'bigint', 3082, 1, 0, NULL, 3086
    ),
    (
        3107, NULL, 3089, 'id1', 'bigint', 3082, 0, 0, NULL, 3086
    ),
    (
	    3108, NULL, 2069, 'data1', 'bigint', 3084, 6, 0, NULL, 3087
    ),
    (
        3109, NULL, 2069, 'id1', 'bigint', 3083, 0, 0, NULL, 3087
    ),
    (
        3110, NULL, 2069, 'ts', 'timestamp', 3084, 5, 0, NULL, 3087
    ),
    (
        3111, NULL, 2069, 'data1', 'bigint', 3083, 2, 0, NULL, 3087
    ),
    (
        3112, NULL, 2069, 'id1', 'bigint', 3084, 4, 0, NULL, 3087
    ),
    (
        3113, NULL, 2069, 'ts', 'timestamp', 3083, 1, 0, NULL, 3087
    ),
    (
        3114, NULL, 2069, 'data2', 'bigint', 3084, 7, 0, NULL, 3087
    ),
    (
        3115, NULL, 2069, 'data2', 'bigint', 3083, 3, 0, NULL, 3087
    ),
    (
        3116, NULL, NULL, 'id1', 'bigint', NULL, 0, 0, NULL, 3088
    ),
    (
        3117, NULL, NULL, 'id1', 'bigint', 3085, 0, 0, NULL, 3089
    ),
    (
        5131, NULL, 2069, 'data1', 'bigint', 5126, 2, 0, NULL, 4113
    ),
    (
        5134, NULL, 2069, 'data2', 'bigint', 5126, 3, 0, NULL, 4113
    ),
    (
        5133, NULL, 2076, 'bigint', 'bigint', 5125, 4, 0, NULL, 4113
    ),
    (
        5132, NULL, 2069, 'id1', 'bigint', 5126, 0, 0, NULL, 4113
    ),
    (
        5135, NULL, 2069, 'ts', 'timestamp', 5126, 1, 0, NULL, 4113
    ),
    (
        5139, NULL, 2069, 'ts', 'timestamp', 5127, 1, 0, NULL, 4114
    ),
    (
        5138, NULL, 2069, 'data1', 'bigint', 5127, 2, 0, NULL, 4114
    ),
    (
        5136, NULL, 2069, 'id1', 'bigint', 5127, 0, 0, NULL, 4114
    ),
    (
        5140, NULL, 2069, 'data2', 'bigint', 5127, 3, 0, NULL, 4114
    ),
    (
        5137, NULL, 2076, 'bigint', 'bigint', 5128, 4, 0, NULL, 4114
    ),
    (
        5141, NULL, 2076, 'bigint', 'bigint', 5129, 4, 0, NULL, 4115
    ),
    (
        5142, NULL, 2069, 'ts', 'timestamp', 5130, 1, 0, NULL, 4115
    ),
    (
        5143, NULL, 2069, 'data2', 'bigint', 5130, 3, 0, NULL, 4115
    ),
    (
        5144, NULL, 2069, 'data1', 'bigint', 5130, 2, 0, NULL, 4115
    ),
    (
        5145, NULL, 2069, 'id1', 'bigint', 5130, 0, 0, NULL, 4115
    ),
    (
        5146, NULL, 2071, 'id1', 'bigint', 5131, 1, 0, NULL, 4116
    ),
    (
        5147, NULL, 2069, 'id1', 'bigint', 5132, 0, 0, NULL, 4116
    );

CREATE TABLE `serving_key` (
                               `id` int(11) NOT NULL AUTO_INCREMENT,
                               `prefix` VARCHAR(63) NULL DEFAULT '',
                               `feature_name` VARCHAR(1000) NOT NULL,
                               `join_on` VARCHAR(1000) NULL,
                               `join_index` int(11) NOT NULL,
                               `feature_group_id` INT(11) NOT NULL,
                               `required` tinyint(1) NOT NULL DEFAULT '0',
                               `feature_view_id` INT(11) NULL,
                               PRIMARY KEY (`id`),
                               KEY `feature_view_id` (`feature_view_id`),
                               KEY `feature_group_id` (`feature_group_id`),
                               CONSTRAINT `feature_view_serving_key_fk` FOREIGN KEY (`feature_view_id`) REFERENCES `feature_view` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
                               CONSTRAINT `feature_group_serving_key_fk` FOREIGN KEY (`feature_group_id`) REFERENCES `feature_group` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

INSERT INTO 
    `serving_key`
VALUES
    (
        68, NULL, 'id1', NULL, 0, 2069, 1, 2059
    ),
    (
        53, NULL, 'id1', NULL, 0, 2071, 1, 2060
    ),
    (
        63, 'fg2_', 'id1', 'id1', 1, 2071, 0, 2061
    ),
    (
        62, NULL, 'id1', NULL, 0, 2069, 1, 2061
    ),
    (
        1, NULL, 'id1', NULL, 0, 2072, 1, 2063
    ),
    (
        2, NULL, 'id1', NULL, 0, 2068, 1, 2064
    ),
    (
        3, 'fg2_', 'id1', 'id1', 1, 2068, 0, 2065
    ),
    (
        4, NULL, 'id1', NULL, 0, 2072, 1, 2065
    ),
    (
        48, 'fg2_', 'id1', 'id1', 1, 2068, 0, 2066
    ),
    (
        47, NULL, 'id1', NULL, 0, 2069, 1, 2066
    ),
    (
        19, NULL, 'id1', NULL, 0, 2076, 1, 2078
    ),
    (
        20, NULL, 'id2', NULL, 0, 2076, 1, 2078
    ),
    (
        52, NULL, 'id2', NULL, 0, 2076, 1, 2079
    ),
    (
        51, NULL, 'id1', NULL, 0, 2076, 1, 2079
    ),
    (
        66, 'fg1_', 'id1', 'id1', 1, 2070, 0, 2080
    ),
    (
        67, NULL, 'id1', NULL, 0, 2069, 1, 2080
    ),
    (
        50, NULL, 'id2', NULL, 0, 2076, 1, 2085
    ),
    (
        49, NULL, 'id1', NULL, 0, 2076, 1, 2085
    ),
    (
        26, 'fg2_', 'id1', 'id1', 1, 2071, 0, 3082
    ),
    (
        27, NULL, 'id1', NULL, 0, 2069, 1, 3082
    ),
    (
        24, NULL, 'id1', NULL, 0, 2069, 0, 3083
    ),
    (
        25, 'fg2_', 'id1', 'id1', 1, 2071, 0, 3083
    ),
    (
        64, NULL, 'id1', NULL, 0, 2069, 1, 3087
    ),
    (
        65, 'fg1_', 'id1', 'id1', 1, 2069, 0, 3087
    ),
    (
        1503, '0_', 'id1', NULL, 1, 2076, 1, 4113
    ),
    (
        1502, NULL, 'id1', NULL, 0, 2069, 1, 4113
    ),
    (
        1504, NULL, 'id2', NULL, 1, 2076, 1, 4113
    ),
    (
        1469, NULL, 'id2', NULL, 1, 2076, 1, 4114
    ),
    (
        1468, '0_', 'id1', 'data1', 1, 2076, 1, 4114
    ),
    (
        1467, NULL, 'id1', NULL, 0, 2069, 1, 4114
    ),
    (
        59, NULL, 'id2', NULL, 1, 2076, 1, 4115
    ),
    (
        58, '0_', 'id1', NULL, 1, 2076, 1, 4115
    ),
    (
        57, NULL, 'id1', NULL, 0, 2069, 1, 4115
    ),
    (
        70, NULL, 'id1', NULL, 0, 3089, 1, 3086
    ),
    (
        1506, 'fg2_', 'id1', 'id1', 1, 2071, 0, 4116
    ),
    (
        1507, NULL, 'id1', NULL, 0, 2069, 1, 4116
    );