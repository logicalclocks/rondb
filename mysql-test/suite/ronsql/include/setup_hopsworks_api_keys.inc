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
    `expiry` datetime DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `prefix_UNIQUE` (`prefix`),
    UNIQUE KEY `index4` (`user_id`, `name`),
    KEY `fk_api_key_1_idx` (`user_id`),
    CONSTRAINT `fk_api_key_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`uid`) ON DELETE CASCADE
) ENGINE = ndbcluster;

# The RDRS API key grant lookup (find_user_databases) also scans the
# fine-grained feature-store sharing tables introduced with RonDB-1088, so
# they must exist even though they stay empty here: the test key's access
# comes from project_team membership alone. DDL copied from the Go test
# fixtures (hopsworks_40_schema.sql plus migrations V45/V46); latin1 is
# required since RDRS asserts on the byte size of the name columns. The
# feature_group FKs to on_demand/cached/stream feature group tables are
# dropped as those tables are not part of this fixture.

CREATE TABLE `feature_store` (
    `id` int NOT NULL AUTO_INCREMENT,
    `name` varchar(100) COLLATE latin1_general_cs NOT NULL,
    `project_id` int NOT NULL,
    `created` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    KEY `project_id` (`project_id`),
    CONSTRAINT `FK_883_662` FOREIGN KEY (`project_id`) REFERENCES `project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE `feature_group` (
    `id` int NOT NULL AUTO_INCREMENT,
    `name` varchar(63) NOT NULL,
    `feature_store_id` int NOT NULL,
    `created` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
    `creator` int NOT NULL,
    `version` int NOT NULL,
    `description` varchar(1000) NULL,
    `feature_group_type` int NOT NULL DEFAULT '0',
    `on_demand_feature_group_id` int NULL,
    `cached_feature_group_id` int NULL,
    `stream_feature_group_id` int NULL,
    `event_time` varchar(63) DEFAULT NULL,
    `online_enabled` tinyint(1) NULL,
    `topic_name` varchar(255) DEFAULT NULL,
    `notification_topic_name` varchar(255) DEFAULT NULL,
    `deprecated` boolean DEFAULT FALSE,
    `for_log` tinyint(1) NOT NULL DEFAULT '0',
    PRIMARY KEY (`id`),
    UNIQUE KEY `name_version` (`feature_store_id`, `name`, `version`),
    KEY `feature_store_id` (`feature_store_id`),
    KEY `creator` (`creator`),
    CONSTRAINT `FK_1012_790` FOREIGN KEY (`creator`) REFERENCES `users` (`uid`) ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT `FK_656_740` FOREIGN KEY (`feature_store_id`) REFERENCES `feature_store` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE `shared_feature_store` (
    `id` int AUTO_INCREMENT PRIMARY KEY,
    `feature_store` int NOT NULL,
    `shared_by` int NOT NULL,
    `shared_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `shared_with_project` int NOT NULL,
    `shared_entirely` tinyint(1) NOT NULL DEFAULT 1,
    CONSTRAINT `sfs_fs_fk` FOREIGN KEY (`feature_store`) REFERENCES `feature_store` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `sfs_user_fk` FOREIGN KEY (`shared_by`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `sfs_project_fk` FOREIGN KEY (`shared_with_project`) REFERENCES `project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE `shared_feature_group` (
    `id` int AUTO_INCREMENT PRIMARY KEY,
    `feature_store` int NOT NULL,
    `feature_group` int NOT NULL,
    `shared_by` int NOT NULL,
    `shared_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `shared_with_project` int NOT NULL,
    `shared_entirely` tinyint(1) NOT NULL DEFAULT 1,
    CONSTRAINT `sfg_fs_fk` FOREIGN KEY (`feature_store`) REFERENCES `feature_store` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `sfg_fg_fk` FOREIGN KEY (`feature_group`) REFERENCES `feature_group` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `sfg_user_fk` FOREIGN KEY (`shared_by`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `sfg_project_fk` FOREIGN KEY (`shared_with_project`) REFERENCES `project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE `shared_feature` (
    `id` int AUTO_INCREMENT PRIMARY KEY,
    `feature_group` int NOT NULL,
    `feature` varchar(63) NOT NULL,
    `shared_by` int NOT NULL,
    `shared_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `shared_with_project` int NOT NULL,
    CONSTRAINT `sft_fg_fk` FOREIGN KEY (`feature_group`) REFERENCES `feature_group` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `sft_user_fk` FOREIGN KEY (`shared_by`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `sft_project_fk` FOREIGN KEY (`shared_with_project`) REFERENCES `project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE `restricted_feature_group_access` (
    `id` int AUTO_INCREMENT PRIMARY KEY,
    `feature_store` int NOT NULL,
    `feature_group` int NOT NULL,
    `granted_by` int NOT NULL,
    `granted_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `granted_to_user` int NOT NULL,
    `can_access_entirely` tinyint(1) NOT NULL DEFAULT 1,
    CONSTRAINT `rfga_fs_fk` FOREIGN KEY (`feature_store`) REFERENCES `feature_store` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `rfga_fg_fk` FOREIGN KEY (`feature_group`) REFERENCES `feature_group` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `rfga_granted_by_fk` FOREIGN KEY (`granted_by`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `rfga_granted_to_user_fk` FOREIGN KEY (`granted_to_user`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE `restricted_feature_access` (
    `id` int AUTO_INCREMENT PRIMARY KEY,
    `restricted_feature_group_access` int NOT NULL,
    `feature` varchar(63) NOT NULL,
    `granted_by` int NOT NULL,
    `granted_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `granted_to_user` int NOT NULL,
    CONSTRAINT `rfa_rfga_fk` FOREIGN KEY (`restricted_feature_group_access`) REFERENCES `restricted_feature_group_access` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `rfa_granted_by_fk` FOREIGN KEY (`granted_by`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `rfa_granted_to_user_fk` FOREIGN KEY (`granted_to_user`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

INSERT INTO
    `users`
VALUES
    (
        10000,
        'styrbiorn',
        '12fa520ec8f65d3a6feacfa97a705e622e1fea95b80b521ec016e43874dfed5a',
        'styrbiorn@grimsson.example',
        '',
        'styrbiorn',
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
        998, 1, 'db1', 0 , 'db1', 'styrbiorn@grimsson.example', '2022-05-30 14:17:22', 'Some desc', 'NOLIMIT', '2022-05-30 14:17:38', 100, 'SomeDockerImage', 1, 0
    );

INSERT INTO
    `project_team`
VALUES
    (
        998,
        'styrbiorn@grimsson.example',
        'Data scientist',
        '2022-06-01 13:28:05'
    );

# We're inserting our test API key into the database, so we can test authentication
# This API key is:
#      bkYjEz6OTZyevbqt.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub
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
        0,
        NULL
    );

let $HOPSWORKS_TEST_API_KEY=bkYjEz6OTZyevbqt.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub;
let $HOPSWORKS_TEST_WRONG_API_KEY=ckYjEz6OTZyevbqt.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub;

--source include/have_util_curl.inc
let $rdrs_ronsql_with_key = curl --fail-with-body -X POST
  -H "Content-Type: application/json"
  http://$RDRS_WITHKEY_HOST:$RDRS_WITHKEY_PORT/0.1.0/ronsql
  -H "x-api-key: $HOPSWORKS_TEST_API_KEY" -d;
let $rdrs_ronsql_with_wrong_key = curl --fail-with-body -X POST
  -H "Content-Type: application/json"
  http://$RDRS_WITHKEY_HOST:$RDRS_WITHKEY_PORT/0.1.0/ronsql
  -H "x-api-key: $HOPSWORKS_TEST_WRONG_API_KEY" -d;
