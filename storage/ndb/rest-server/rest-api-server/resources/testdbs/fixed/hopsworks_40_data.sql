-- This file is part of the RonDB REST API Server
-- Copyright (c) 2024 Hopsworks AB
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
        999, 'demo0', 'macho@hopsworks.ai', Timestamp('2022-05-30 14:17:22'), 'Some desc', 'NOLIMIT', Timestamp('2022-05-30 14:17:38'), 100, 'demo0', 1, 1
    ),
    (
        1000, 'fsdb001', 'macho@hopsworks.ai', Timestamp('2023-03-16 14:27:17'), 'Some desc', 'NOLIMIT', Timestamp('2023-03-16 14:27:18'), 100, 'fsdb001', 0, 1
    ),
    (
        1001, 'fsdb002', 'macho@hopsworks.ai', Timestamp('2023-04-20 16:14:15'), 'Some desc', 'NOLIMIT', Timestamp('2023-04-20 16:14:15'), 100, 'fsdb002', 0, 1
    ),
    (
        1002, 'fsdb_isolate', 'macho@hopsworks.ai', Timestamp('2023-04-20 16:14:15'), 'Some desc', 'NOLIMIT', Timestamp('2023-04-20 16:14:15'), 100, 'fsdb_isolate', 0, 1
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

INSERT INTO
    `feature_store`
VALUES
    (
        66, "fsdb_isolate", 1002, "2023-03-16 14:27:29"
    ),
    (
        67, "fsdb001", 1000, "2023-03-16 14:27:29"
    ),
    (
        1091, "fsdb002", 1001, "2023-03-16 14:27:29"
    );

INSERT INTO
    `cached_feature_group`
VALUES
    (
        1025, 1
    );

INSERT INTO
    `stream_feature_group`
VALUES
    (
        2056, 1
    ),
    (
        2057, 1
    ),
    (
        2059, 1
    ),
    (
        2060, 1
    ),
    (
        2058, 1
    ),
    (
        2064, 1
    ),
    (
        2055, 1
    ),
    (
        18, 1
    ),
    (
        2065, 1
    );

INSERT INTO
    `feature_group`
VALUES
    (
        2068, 'sample_2', 1091, Timestamp('2023-04-21 09:32:38'), 10000, 1, NULL, 2, NULL, NULL, 2056, 'ts', 1, NULL, NULL, FALSE, 0
    ),
    (
        2069, 'sample_1', 67, Timestamp('2023-04-21 09:33:40'), 10000, 1, NULL, 2, NULL, NULL, 2057, 'ts', 1, NULL, NULL, FALSE, 0
    ),
    (
        2071, 'sample_2', 67, Timestamp('2023-04-21 09:37:25'), 10000, 1, NULL, 2, NULL, NULL, 2059, 'ts', 1, NULL, NULL, FALSE, 0
    ),
    (
        2072, 'sample_1', 1091, Timestamp('2023-04-21 10:00:40'), 10000, 1, NULL, 2, NULL, NULL, 2060, 'ts', 1, NULL, NULL, FALSE, 0
    ),
    (
        2070, 'sample_1', 67, Timestamp('2023-04-21 09:35:38'), 10000, 2, NULL, 2, NULL, NULL, 2058, 'ts', 1, NULL, NULL, FALSE, 0
    ),
    (
        2076, 'sample_3', 67, Timestamp('2023-05-08 15:20:51'), 10000, 1, NULL, 2, NULL, NULL, 2064, 'ts', 1, NULL, NULL, FALSE, 0
    ),
    (
        2067, 'sample_4', 66, Timestamp('2023-05-08 15:20:51'), 10000, 1, NULL, 2, NULL, NULL, 2055, 'ts', 1, NULL, NULL, FALSE, 0
    ),
    (
        3089, 'sample_cache', 67, Timestamp('2023-06-15 11:46:25'), 10000, 1, NULL, 0, NULL, 1025, NULL, NULL, 1, NULL, NULL, FALSE, 0
    ),
    (
        31, 'sample_complex_type', 1091, Timestamp('2023-09-26 10:02:58'), 10000, 1, NULL, 2, NULL, NULL, 18, 'ts', 1, NULL, NULL, FALSE, 0
    ),
    (
        3090, 'sample_4', 67, Timestamp('2023-05-08 15:20:51'), 10000, 1, NULL, 2, NULL, NULL, 2065, 'ts', 1, NULL, NULL, FALSE, 0
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
        4119, 'sample_1n2_no_prefix', 67, Timestamp('2023-04-21 09:52:53'), 10000, 1, ''
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
    ),
    /**
    SELECT `fg0`.`id1` `id1`, `fg0`.`ts` `ts`, `fg0`.`array` `array`, `fg0`.`struct` `struct`
    FROM `test_ken_featurestore`.`sample_complex_type_1` `fg0`
    */
    (
	    19, 'sample_complex_type', 1091, Timestamp('2023-09-26 10:03:16'), 10000, 1, ''
    ),
    /**
    SELECT `fg0`.`id1` `id1`, `fg0`.`ts` `ts`, `fg0`.`data1` `data1`, `fg0`.`data2` `data2`, `fg1`.`id1` `id1`, `fg1`.`bigint` `bigint`
    FROM `test_ken_featurestore`.`sample_1_1` `fg0`
    INNER JOIN `test_ken_featurestore`.`sample_3_1` `fg1` ON `fg0`.`bigint` = `fg1`.`id1`
    */
    (
        4117, 'sample_1n3_pk', 67, Timestamp('2023-08-08 14:00:53'), 10000, 1, ''
    ),
    /**
    SELECT `fg0`.`id1` `id1`, `fg0`.`ts` `ts`, `fg0`.`data1` `data1`, `fg0`.`data2` `data2`, `fg1`.`id1` `id1`, `fg1`.`bigint` `bigint`
    FROM `test_ken_featurestore`.`sample_1_1` `fg0`
    INNER JOIN `test_ken_featurestore`.`sample_3_1` `fg1` ON `fg0`.`bigint` = `fg1`.`id1`
    */
    (
        4118, 'sample_1n3_no_prefix_pk', 67, Timestamp('2023-08-08 14:00:53'), 10000, 1, ''
    ),
    /**
    SELECT `fg0`.`id1` `id1`, `fg0`.`ts` `ts`, `fg0`.`data1` `data1`, `fg0`.`data2` `data2`, `fg1`.`id1` `right_id1`, `fg1`.`id2` `right_id2`, `fg1`.`bigint` `right_bigint`
    FROM `test_ken_featurestore`.`sample_4_1` `fg0`
    INNER JOIN `test_ken_featurestore`.`sample_3_1` `fg1` ON `fg0`.`id1` = `fg1`.`id2`
    */
    (
        4120, 'sample_4n3_on_id', 67, Timestamp('2023-08-08 14:00:53'), 10000, 1, ''    
    );

INSERT INTO 
    `training_dataset_join`
VALUES
    (
        2051, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 2059
    ),
    (
        2052, NULL, 2071, NULL, NULL, 0, 0, 0, NULL, 2060
    ),
    (
        2053, NULL, 2071, 2069, NULL, 0, 1, 0, 'fg2_', 2061
    ),
    (
        2054, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 2061
    ),
    (
        2056, NULL, 2072, NULL, NULL, 0, 0, 0, NULL, 2063
    ),
    (
        2057, NULL, 2068, NULL, NULL, 0, 0, 0, NULL, 2064
    ),
    (
        2058, NULL, 2068, 2072, NULL, 0, 1, 0, 'fg2_', 2065
    ),
    (
        2059, NULL, 2072, NULL, NULL, 0, 0, 0, NULL, 2065
    ),
    (
        2060, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 2066
    ),
    (
        2061, NULL, 2068, 2069, NULL, 0, 1, 0, 'fg2_', 2066
    ),
    (
	    2084, NULL, 2076, NULL, NULL, 0, 0, 0, NULL, 2078
    ),
    (
	    2085, NULL, 2076, NULL, NULL, 0, 0, 0, NULL, 2079
    ),
    (
	    2086, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 2080
    ),
    (
        2087, NULL, 2070, 2069, NULL, 0, 1, 0, 'fg1_', 2080
    ),
    (
	    2096, NULL, 2067, NULL, NULL, 0, 0, 0, NULL, 2085
    ),
    (
        3074, NULL, 2071, 2069, NULL, 0, 1, 0, 'fg2_', 3082
    ),
    (
        3075, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 3082
    ),
    (
        3076, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 3083
    ),
    (
        3077, NULL, 2071, 2069, NULL, 0, 1, 0, 'fg2_', 3083
    ),
    (
	    3082, NULL, 3089, NULL, NULL, 0, 0, 0, NULL, 3086
    ),
    (
	    3083, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 3087
    ),
    (
        3084, NULL, 2069, 2069, NULL, 0, 1, 0, 'fg1_', 3087
    ),
    (
        3085, NULL, 2069, NULL, NULL, 0, 1, 0, 'fg1_', 3089
    ),
    (
        5126, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 4113
    ),
    (
        5125, NULL, 2076, 2069, NULL, 0, 1, 0, NULL, 4113
    ),
    (
        5127, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 4114
    ),
    (
        5128, NULL, 2076, 2069, NULL, 0, 1, 0, NULL, 4114
    ),
    (
        5129, NULL, 2076, 2069, NULL, 0, 1, 0, NULL, 4115
    ),
    (
        5130, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 4115
    ),
    (
        5131, NULL, 2071, 2069, NULL, 0, 1, 0, 'fg2_', 4116
    ),
    (
        5132, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 4116
    ),
    (
        25, NULL, 31, NULL, NULL, 0, 0, 0, NULL, 19
    ),
    (
        5133, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 4117
    ),
    (
        5134, NULL, 2076, 2069, NULL, 0, 1, 0, 'right_', 4117
    ),
    (
        5135, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 4118
    ),
    (
        5136, NULL, 2076, 2069, NULL, 0, 1, 0, NULL, 4118
    ),
    (
        5138, NULL, 2071, 2069, NULL, 0, 1, 0, NULL, 4119
    ),
    (
        5137, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 4119
    ),
    (
        5139, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 4120
    ),
    (
        5140, NULL, 2076, 2069, NULL, 0, 1, 0, 'right_', 4120
    );

INSERT INTO
    `training_dataset_feature`
VALUES
    (
        2058, NULL, 2069, 'id1', 'bigint', 2051, 0, 0, 0, 0, 2059, NULL
    ),
    (
        2059, NULL, 2069, 'ts', 'timestamp', 2051, 1, 0, 0, 0, 2059, NULL
    ),
    (
        2060, NULL, 2069, 'data2', 'bigint', 2051, 3, 0, 0, 0, 2059, NULL
    ),
    (
        2061, NULL, 2071, 'data2', 'string', 2052, 3, 0, 0, 0, 2060, NULL
    ),
    (
        2062, NULL, 2071, 'data1', 'string', 2052, 2, 0, 0, 0, 2060, NULL
    ),
    (
        2064, NULL, 2071, 'id1', 'bigint', 2052, 0, 0, 0, 0, 2060, NULL
    ),
    (
        2065, NULL, 2069, 'ts', 'timestamp', 2054, 1, 0, 0, 0, 2061, NULL
    ),
    (
        2066, NULL, 2071, 'id1', 'bigint', 2053, 4, 0, 0, 0, 2061, NULL
    ),
    (
        2067, NULL, 2071, 'data1', 'string', 2053, 6, 0, 0, 0, 2061, NULL
    ),
    (
        2070, NULL, 2069, 'data1', 'bigint', 2054, 2, 0, 0, 0, 2061, NULL
    ),
    (
        2072, NULL, 2069, 'data2', 'bigint', 2054, 3, 0, 0, 0, 2061, NULL
    ),
    (
        2077, NULL, 2072, 'data1', 'bigint', 2056, 2, 0, 0, 0, 2063, NULL
    ),
    (
        2079, NULL, 2072, 'id1', 'bigint', 2056, 0, 0, 0, 0, 2063, NULL
    ),
    (
        2080, NULL, 2072, 'ts', 'timestamp', 2056, 1, 0, 0, 0, 2063, NULL
    ),
    (
        2082, NULL, 2068, 'id1', 'bigint', 2057, 0, 0, 0, 0, 2064, NULL
    ),
    (
        2083, NULL, 2068, 'ts', 'date', 2057, 1, 0, 0, 0, 2064, NULL
    ),
    (
        2084, NULL, 2068, 'data2', 'string', 2057, 3, 0, 0, 0, 2064, NULL
    ),
    (
        2085, NULL, 2068, 'data2', 'string', 2058, 7, 0, 0, 0, 2065, NULL
    ),
    (
        2086, NULL, 2068, 'ts', 'date', 2058, 5, 0, 0, 0, 2065, NULL
    ),
    (
        2094, NULL, 2069, 'data1', 'bigint', 2060, 2, 0, 0, 0, 2066, NULL
    ),
    (
        2097, NULL, 2068, 'id1', 'bigint', 2061, 4, 0, 0, 0, 2066, NULL
    ),
    (
        2098, NULL, 2068, 'data2', 'string', 2061, 7, 0, 0, 0, 2066, NULL
    ),
    (
        2100, NULL, 2068, 'data1', 'string', 2061, 6, 0, 0, 0, 2066, NULL
    ),
    (
        2057, NULL, 2069, 'data1', 'bigint', 2051, 2, 0, 0, 0, 2059, NULL
    ),
    (
        2063, NULL, 2071, 'ts', 'date', 2052, 1, 0, 0, 0, 2060, NULL
    ),
    (
        2068, NULL, 2071, 'ts', 'date', 2053, 5, 0, 0, 0, 2061, NULL
    ),
    (
        2069, NULL, 2071, 'data2', 'string', 2053, 7, 0, 0, 0, 2061, NULL
    ),
    (
        2071, NULL, 2069, 'id1', 'bigint', 2054, 0, 0, 0, 0, 2061, NULL
    ),
    (
        2078, NULL, 2072, 'data2', 'bigint', 2056, 3, 0, 0, 0, 2063, NULL
    ),
    (
        2081, NULL, 2068, 'data1', 'string', 2057, 2, 0, 0, 0, 2064, NULL
    ),
    (
        2087, NULL, 2072, 'id1', 'bigint', 2059, 0, 0, 0, 0, 2065, NULL
    ),
    (
        2088, NULL, 2072, 'data2', 'bigint', 2059, 3, 0, 0, 0, 2065, NULL
    ),
    (
        2089, NULL, 2072, 'ts', 'timestamp', 2059, 1, 0, 0, 0, 2065, NULL
    ),
    (
        2090, NULL, 2072, 'data1', 'bigint', 2059, 2, 0, 0, 0, 2065, NULL
    ),
    (
        2091, NULL, 2068, 'id1', 'bigint', 2058, 4, 0, 0, 0, 2065, NULL
    ),
    (
        2092, NULL, 2068, 'data1', 'string', 2058, 6, 0, 0, 0, 2065, NULL
    ),
    (
        2093, NULL, 2069, 'id1', 'bigint', 2060, 0, 0, 0, 0, 2066, NULL
    ),
    (
        2095, NULL, 2069, 'data2', 'bigint', 2060, 3, 0, 0, 0, 2066, NULL
    ),
    (
        2096, NULL, 2069, 'ts', 'timestamp', 2060, 1, 0, 0, 0, 2066, NULL
    ),
    (
        2099, NULL, 2068, 'ts', 'date', 2061, 5, 0, 0, 0, 2066, NULL
    ),
    (
        2182, NULL, 2076, 'id2', 'string', 2084, 1, 0, 0, 0, 2078, NULL
    ),
    (
        2188, NULL, 2076, 'bigint', 'bigint', 2084, 3, 0, 0, 0, 2078, NULL
    ),
    (
        2183, NULL, 2076, 'ts', 'timestamp', 2084, 2, 0, 0, 0, 2078, NULL
    ),
    (
        2189, NULL, 2076, 'id1', 'bigint', 2084, 0, 0, 0, 0, 2078, NULL
    ),
    (
	    2192, NULL, 2076, 'bigint', 'bigint', 2085, 3, 0, 0, 0, 2079, NULL
    ),
    (
        2193, NULL, 2076, 'bool', 'boolean', 2085, 6, 0, 0, 0, 2079, NULL
    ),
    (
        2194, NULL, 2076, 'ts', 'timestamp', 2085, 2, 0, 0, 0, 2079, NULL
    ),
    (
        2195, NULL, 2076, 'date', 'date', 2085, 5, 0, 0, 0, 2079, NULL
    ),
    (
        2198, NULL, 2076, 'binary', 'binary', 2085, 9, 0, 0, 0, 2079, NULL
    ),
    (
        2199, NULL, 2076, 'id2', 'string', 2085, 1, 0, 0, 0, 2079, NULL
    ),
    (
        2190, NULL, 2076, 'double', 'double', 2085, 8, 0, 0, 0, 2079, NULL
    ),
    (
        2191, NULL, 2076, 'string', 'string', 2085, 4, 0, 0, 0, 2079, NULL
    ),
    (
        2196, NULL, 2076, 'id1', 'bigint', 2085, 0, 0, 0, 0, 2079, NULL
    ),
    (
        2197, NULL, 2076, 'float', 'float', 2085, 7, 0, 0, 0, 2079, NULL
    ),
    (
	    2202, NULL, 2070, 'data1', 'bigint', 2087, 6, 0, 0, 0, 2080, NULL
    ),
    (
        2203, NULL, 2069, 'id1', 'bigint', 2086, 0, 0, 0, 0, 2080, NULL
    ),
    (
        2207, NULL, 2070, 'ts', 'timestamp', 2087, 5, 0, 0, 0, 2080, NULL
    ),
    (
        2200, NULL, 2069, 'data1', 'bigint', 2086, 2, 0, 0, 0, 2080, NULL
    ),
    (
        2201, NULL, 2070, 'id1', 'bigint', 2087, 4, 0, 0, 0, 2080, NULL
    ),
    (
        2204, NULL, 2069, 'ts', 'timestamp', 2086, 1, 0, 0, 0, 2080, NULL
    ),
    (
        2205, NULL, 2070, 'data2', 'bigint', 2087, 7, 0, 0, 0, 2080, NULL
    ),
    (
        2206, NULL, 2069, 'data2', 'bigint', 2086, 3, 0, 0, 0, 2080, NULL
    ),
    (
	    2253, NULL, 2067, 'bigint', 'bigint', 2096, 3, 0, 0, 0, 2085, NULL
    ),
    (
        2254, NULL, 2067, 'ts', 'timestamp', 2096, 2, 0, 0, 0, 2085, NULL
    ),
    (
        2260, NULL, 2067, 'id1', 'bigint', 2096, 0, 0, 0, 0, 2085, NULL
    ),
    (
        2261, NULL, 2067, 'id2', 'string', 2096, 1, 0, 0, 0, 2085, NULL
    ),
    (
        3077, NULL, 2069, 'data1', 'bigint', 3075, 2, 1, 0, 0, 3082, NULL
    ),
    (
        3078, NULL, 2071, 'data2', 'string', 3074, 7, 0, 0, 0, 3082, NULL
    ),
    (
        3080, NULL, 2069, 'ts', 'timestamp', 3075, 1, 0, 0, 0, 3082, NULL
    ),
    (
        3081, NULL, 2069, 'data2', 'bigint', 3075, 3, 0, 0, 0, 3082, NULL
    ),
    (
        3084, NULL, 2069, 'id1', 'bigint', 3075, 0, 0, 0, 0, 3082, NULL
    ),
    (
        3087, NULL, 2071, 'data1', 'string', 3077, 3, 0, 0, 0, 3083, NULL
    ),
    (
        3089, NULL, 2071, 'ts', 'date', 3077, 2, 0, 0, 0, 3083, NULL
    ),
    (
        3079, NULL, 2071, 'ts', 'date', 3074, 5, 0, 0, 0, 3082, NULL
    ),
    (
        3082, NULL, 2071, 'id1', 'bigint', 3074, 4, 0, 0, 0, 3082, NULL
    ),
    (
        3083, NULL, 2071, 'data1', 'string', 3074, 6, 0, 0, 0, 3082, NULL
    ),
    (
        3085, NULL, 2071, 'data2', 'string', 3077, 4, 0, 0, 0, 3083, NULL
    ),
    (
        3086, NULL, 2071, 'id1', 'bigint', 3077, 1, 0, 0, 0, 3083, NULL
    ),
    (
        3088, NULL, 2069, 'data1', 'bigint', 3076, 0, 1, 0, 0, 3083, NULL
    ),
    (
        3106, NULL, 3089, 'data', 'bigint', 3082, 1, 0, 0, 0, 3086, NULL
    ),
    (
        3107, NULL, 3089, 'id1', 'bigint', 3082, 0, 0, 0, 0, 3086, NULL
    ),
    (
	    3108, NULL, 2069, 'data1', 'bigint', 3084, 6, 0, 0, 0, 3087, NULL
    ),
    (
        3109, NULL, 2069, 'id1', 'bigint', 3083, 0, 0, 0, 0, 3087, NULL
    ),
    (
        3110, NULL, 2069, 'ts', 'timestamp', 3084, 5, 0, 0, 0, 3087, NULL
    ),
    (
        3111, NULL, 2069, 'data1', 'bigint', 3083, 2, 0, 0, 0, 3087, NULL
    ),
    (
        3112, NULL, 2069, 'id1', 'bigint', 3084, 4, 0, 0, 0, 3087, NULL
    ),
    (
        3113, NULL, 2069, 'ts', 'timestamp', 3083, 1, 0, 0, 0, 3087, NULL
    ),
    (
        3114, NULL, 2069, 'data2', 'bigint', 3084, 7, 0, 0, 0, 3087, NULL
    ),
    (
        3115, NULL, 2069, 'data2', 'bigint', 3083, 3, 0, 0, 0, 3087, NULL
    ),
    (
        3116, NULL, NULL, 'id1', 'bigint', NULL, 0, 0, 0, 0, 3088, NULL
    ),
    (
        3117, NULL, NULL, 'id1', 'bigint', 3085, 0, 0, 0, 0, 3089, NULL
    ),
    (
        5131, NULL, 2069, 'data1', 'bigint', 5126, 2, 0, 0, 0, 4113, NULL
    ),
    (
        5134, NULL, 2069, 'data2', 'bigint', 5126, 3, 0, 0, 0, 4113, NULL
    ),
    (
        5133, NULL, 2076, 'bigint', 'bigint', 5125, 4, 0, 0, 0, 4113, NULL
    ),
    (
        5132, NULL, 2069, 'id1', 'bigint', 5126, 0, 0, 0, 0, 4113, NULL
    ),
    (
        5135, NULL, 2069, 'ts', 'timestamp', 5126, 1, 0, 0, 0, 4113, NULL
    ),
    (
        5139, NULL, 2069, 'ts', 'timestamp', 5127, 1, 0, 0, 0, 4114, NULL
    ),
    (
        5138, NULL, 2069, 'data1', 'bigint', 5127, 2, 0, 0, 0, 4114, NULL
    ),
    (
        5136, NULL, 2069, 'id1', 'bigint', 5127, 0, 0, 0, 0, 4114, NULL
    ),
    (
        5140, NULL, 2069, 'data2', 'bigint', 5127, 3, 0, 0, 0, 4114, NULL
    ),
    (
        5137, NULL, 2076, 'bigint', 'bigint', 5128, 4, 0, 0, 0, 4114, NULL
    ),
    (
        5141, NULL, 2076, 'bigint', 'bigint', 5129, 4, 0, 0, 0, 4115, NULL
    ),
    (
        5142, NULL, 2069, 'ts', 'timestamp', 5130, 1, 0, 0, 0, 4115, NULL
    ),
    (
        5143, NULL, 2069, 'data2', 'bigint', 5130, 3, 0, 0, 0, 4115, NULL
    ),
    (
        5144, NULL, 2069, 'data1', 'bigint', 5130, 2, 0, 0, 0, 4115, NULL
    ),
    (
        5145, NULL, 2069, 'id1', 'bigint', 5130, 0, 0, 0, 0, 4115, NULL
    ),
    (
        5146, NULL, 2071, 'id1', 'bigint', 5131, 1, 0, 0, 0, 4116, NULL
    ),
    (
        5147, NULL, 2069, 'id1', 'bigint', 5132, 0, 0, 0, 0, 4116, NULL
    ),
    (
        57, NULL, 31, 'array', 'array<bigint>', 25, 2, 0, 0, 0, 19, NULL
    ),
    (
        55, NULL, 31, 'struct', 'struct<int1:bigint,int2:bigint>', 25, 3, 0, 0, 0, 19, NULL
    ),
    (
        58, NULL, 31, 'ts', 'bigint', 25, 1, 0, 0, 0, 19, NULL
    ),
    (
        56, NULL, 31, 'id1', 'bigint', 25, 0, 0, 0, 0, 19, NULL
    ),
    (
        5148, NULL, 2069, 'data1', 'bigint', 5133, 2, 0, 0, 0, 4117, NULL
    ),
    (
        5149, NULL, 2069, 'data2', 'bigint', 5133, 3, 0, 0, 0, 4117, NULL
    ),
    (
        5150, NULL, 2076, 'bigint', 'bigint', 5134, 6, 0, 0, 0, 4117, NULL
    ),
    (
        5151, NULL, 2069, 'id1', 'bigint', 5133, 0, 0, 0, 0, 4117, NULL
    ),
    (
        5152, NULL, 2069, 'ts', 'timestamp', 5133, 1, 0, 0, 0, 4117, NULL
    ),
    (
        5153, NULL, 2076, 'id1', 'bigint', 5134, 4, 0, 0, 0, 4117, NULL
    ),
    (
        5154, NULL, 2076, 'id2', 'varchar(100)', 5134, 5, 0, 0, 0, 4117, NULL
    ),
    (
        5155, NULL, 2069, 'data1', 'bigint', 5135, 2, 0, 0, 0, 4118, NULL
    ),
    (
        5156, NULL, 2069, 'data2', 'bigint', 5135, 3, 0, 0, 0, 4118, NULL
    ),
    (
        5157, NULL, 2076, 'bigint', 'bigint', 5136, 6, 0, 0, 0, 4118, NULL
    ),
    (
        5158, NULL, 2069, 'id1', 'bigint', 5135, 0, 0, 0, 0, 4118, NULL
    ),
    (
        5159, NULL, 2069, 'ts', 'timestamp', 5135, 1, 0, 0, 0, 4118, NULL
    ),
    (
        5160, NULL, 2076, 'id1', 'bigint', 5136, 4, 0, 0, 0, 4118, NULL
    ),
    (
        5161, NULL, 2076, 'id2', 'bigint', 5136, 5, 0, 0, 0, 4118, NULL
    ),
    (
        5162, NULL, 2069, 'ts', 'timestamp', 5137, 1, 0, 0, 0, 4119, NULL
    ),
    (
        5163, NULL, 2071, 'id1', 'bigint', 5138, 4, 0, 0, 0, 4119, NULL
    ),
    (
        5164, NULL, 2071, 'data1', 'string', 5138, 6, 0, 0, 0, 4119, NULL
    ),
    (
        5165, NULL, 2069, 'data1', 'bigint', 5137, 2, 0, 0, 0, 4119, NULL
    ),
    (
        5166, NULL, 2069, 'data2', 'bigint', 5137, 3, 0, 0, 0, 4119, NULL
    ),
    (
        5167, NULL, 2071, 'ts', 'date', 5138, 5, 0, 0, 0, 4119, NULL
    ),
    (
        5168, NULL, 2071, 'data2', 'string', 5138, 7, 0, 0, 0, 4119, NULL
    ),
    (
        5169, NULL, 2069, 'id1', 'bigint', 5137, 0, 0, 0, 0, 4119, NULL
    ),
    (
        5170, NULL, 3090, 'data1', 'bigint', 5139, 2, 0, 0, 0, 4120, NULL
    ),
    (
        5171, NULL, 3090, 'data2', 'bigint', 5139, 3, 0, 0, 0, 4120, NULL
    ),
    (
        5172, NULL, 2076, 'bigint', 'bigint', 5140, 6, 0, 0, 0, 4120, NULL
    ),
    (
        5173, NULL, 3090, 'id1', 'bigint', 5139, 0, 0, 0, 0, 4120, NULL
    ),
    (
        5174, NULL, 3090, 'ts', 'timestamp', 5139, 1, 0, 0, 0, 4120, NULL
    ),
    (
        5175, NULL, 2076, 'id1', 'bigint', 5140, 4, 0, 0, 0, 4120, NULL
    ),
    (
        5176, NULL, 2076, 'id2', 'varchar(100)', 5140, 5, 0, 0, 0, 4120, NULL
    );

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
    ),
    (
        1508, NULL, 'id1', NULL, 0, 31, 1, 19
    ),
    (
        1509, 'right_', 'id2', NULL, 1, 2076, 1, 4117
    ),
    (
        1510, 'right_', 'id1', 'data1', 1, 2076, 1, 4117
    ),
    (
        1511, NULL, 'id1', NULL, 0, 2069, 1, 4117
    ),
    (
        1512, NULL, 'id2', NULL, 1, 2076, 1, 4118
    ),
    (
        1513, '0_', 'id1', 'data1', 1, 2076, 1, 4118
    ),
    (
        1514, NULL, 'id1', NULL, 0, 2069, 1, 4118
    ),
    (
        1515, '0_', 'id1', 'id1', 1, 2071, 0, 4119
    ),
    (
        1516, NULL, 'id1', NULL, 0, 2069, 1, 4119
    ),
    (
        1517, 'right_', 'id1', NULL, 1, 2076, 1, 4120
    ),
    (
        1518, 'right_', 'id2', 'id1', 1, 2076, 0, 4120
    ),
    (
        1519, NULL, 'id1', NULL, 0, 3090, 1, 4120
    );

INSERT INTO
    `schemas`
VALUES
    (
	    20, '{"type":"record","name":"sample_complex_type_1","namespace":"test_ken_featurestore.db","fields":[{"name":"id1","type":["null","str"]},{"name":"ts","type":["null","long"]},{"name":"array","type":["null",{"type":"array","items":["null","long"]}]},{"name":"struct","type":["null",{"type":"record","name":"r854762204","namespace":"struct","fields":[{"name":"int1","type":["null","long"]},{"name":"int2","type":["null","long"]}]}]}]}', 1001
    ),
    (
	    21, '{"type":"record","name":"sample_complex_type_1","namespace":"test_ken_featurestore.db","fields":[{"name":"id1","type":["null","long"]},{"name":"ts","type":["null","long"]},{"name":"array","type":["null",{"type":"array","items":["null","long"]}]},{"name":"struct","type":["null",{"type":"record","name":"r854762204","namespace":"struct","fields":[{"name":"int1","type":["null","long"]},{"name":"int2","type":["null","long"]}]}]}]}', 1001
    );

INSERT INTO
    `subjects`
VALUES
    (
        21, 'sample_complex_type_1', 1, 20, 1001, Timestamp('2023-09-26 10:02:58')
    ),
    (
        22, 'sample_complex_type_1', 2, 21, 1001, Timestamp('2023-09-27 10:02:58')
    );
