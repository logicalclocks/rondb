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
    `id` int NOT NULL AUTO_INCREMENT,
    `inode_pid` bigint NOT NULL,
    `inode_name` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
    `partition_id` bigint NOT NULL,
    `projectname` varchar(100) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
    `username` varchar(150) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
    `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `retention_period` date DEFAULT NULL,
    `archived` tinyint(1) DEFAULT '0',
    `logs` tinyint(1) DEFAULT '0',
    `deleted` tinyint(1) DEFAULT '0',
    `description` varchar(2000) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
    `payment_type` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL DEFAULT 'PREPAID',
    `last_quota_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `kafka_max_num_topics` int NOT NULL DEFAULT '100',
    `docker_image` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL,
    `python_env_id` int DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `projectname` (`projectname`),
    UNIQUE KEY `inode_pid` (`inode_pid`, `inode_name`, `partition_id`),
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
        999,
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
        999,
        322,
        'demo0',
        322,
        'demo0',
        'macho@hopsworks.ai',
        '2022-05-30 14:17:22',
        '2032-05-30',
        0,
        0,
        NULL,
        'A demo project for getting started with featurestore',
        'NOLIMIT',
        '2022-05-30 14:17:38',
        100,
        'demo_fs_meb10000:1653921933268-2.6.0-SNAPSHOT.1',
        1
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
        2049,
        'bkYjEz6OTZyevbqt',
        '709faa77accc3f30394cfb53b67253ba64881528cb3056eea110703ca430cce4',
        '1/1TxiaiIB01rIcY2E36iuwKP6fm2GzBaNaQqOVGMhH0AvcIlIzaUIw0fMDjKNLa0OWxAOrfTSPqAolpI/n+ug==',
        '2022-06-14 10:27:03',
        '2022-06-14 10:27:03',
        'myapikey1',
        999,
        0
    );
