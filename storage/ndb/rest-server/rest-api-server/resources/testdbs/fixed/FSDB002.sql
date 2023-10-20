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

-- Signed and unsigned number data types
DROP DATABASE IF EXISTS fsdb002;

CREATE DATABASE fsdb002;

USE fsdb002;

CREATE TABLE `sample_1_1` (
  `id1` bigint NOT NULL,
  `ts` timestamp NULL DEFAULT NULL,
  `data1` bigint DEFAULT NULL,
  `data2` bigint DEFAULT NULL,
  PRIMARY KEY (`id1`)
) ENGINE=ndbcluster;

INSERT INTO
    sample_1_1
VALUES
    (
        73, Timestamp('2022-01-16 00:00:00'), 17, 28
    ),
    (
        56, Timestamp('2022-01-13 00:00:00'), 12, 67
    ),
    (
        12, Timestamp('2022-01-23 00:00:00'), 61, 12
    ),
    (
        51, Timestamp('2022-01-22 00:00:00'), 70, 35
    ),
    (
        9, Timestamp('2022-01-01 00:00:00'), 2, 62
    ),
    (
        87, Timestamp('2022-01-04 00:00:00'), 16, 50
    ),
    (
        47, Timestamp('2022-01-20 00:00:00'), 51, 73
    ),
    (
        41, Timestamp('2022-01-16 00:00:00'), 26, 30
    ),
    (
        23, Timestamp('2022-01-18 00:00:00'), 14, 93
    ),
    (
        80, Timestamp('2022-01-11 00:00:00'), 20, 52
    );

CREATE TABLE `sample_2_1` (
  `id1` bigint NOT NULL,
  `ts` date DEFAULT NULL,
  `data1` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `data2` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id1`)
) ENGINE=ndbcluster;

INSERT INTO
    sample_2_1
VALUES
    (
        87, '2022-01-09', 'int1', 'str19'
    ),
    (
        74, '2022-01-23', 'int25', 'str44'
    ),
    (
        70, '2022-01-17', 'int98', 'str72'
    ),
    (
        16, '2022-01-27', 'int31', 'str24'
    ),
    (
        36, '2022-01-24', 'int24', 'str14'
    ),
    (
        71, '2022-01-22', 'int3', 'str97'
    ),
    (
        48, '2022-01-26', 'int92', 'str31'
    ),
    (
        29, '2022-01-03', 'int53', 'str91'
    ),
    (
        73, '2022-01-21', 'int37', 'str97'
    ),
    (
        53, '2022-01-25', 'int83', 'str79'
    );

CREATE TABLE `sample_complex_type_1` (
  `id1` bigint NOT NULL,
  `ts` bigint DEFAULT NULL,
  `array` varbinary(100) DEFAULT NULL,
  `struct` varbinary(100) DEFAULT NULL,
  PRIMARY KEY (`id1`)
) ENGINE=ndbcluster;

INSERT INTO
    sample_complex_type_1
VALUES
    (
        6, 1643414400000, 0x020402b60102820100, 0x0202480222
    ),
    (
        75, 1641254400000, 0x0204028401026c00, 0x0202c801022e
    ),
    (
        96, 1641859200000, 0x0204024e024200, 0x0202bc01023a
    ),
    (
        3, 1642550400000, 0x020402900102a80100, 0x020266026a
    ),
    (
        10, 1642982400000, 0x02040232020600, 0x020256028801
    ),
    (
        19, 1641859200000, 0x0204022a02b80100, 0x02022e0238
    ),
    (
        38, 1641686400000, 0x0204023c02aa0100, 0x02022a021e
    ),
    (
        45, 1641427200000, 0x020402860102a60100, 0x02028401026a
    ),
    (
        65, 1641168000000, 0x020402a80102ba0100, 0x0202920102b201
    ),
    (
        72, 1642896000000, 0x0204024e02880100, 0x0202a001026c
    ),
    (
        73, 1643414400000, 0x020402c20102c40100, 0x02027a029001
    ),
    (
        44, 1641081600000, 0x0204024c02b40100, 0x0202440238
    ),
    (
        53, 1642032000000, 0x0204028601022400, 0x0202c6010210
    ),
    (
        66, 1643414400000, 0x02040240026000, 0x02026c0248
    ),
    (
        90, 1642636800000, 0x0204025c027600, 0x0202c20102b801
    ),
    (
        5, 1641254400000, 0x0204026e021c00, 0x020234024e
    ),
    (
        23, 1643155200000, 0x020402b80102bc0100, 0x02029601026c
    ),
    (
        89, 1641168000000, 0x02040230023800, 0x02022e0272
    ),
    (
        95, 1642636800000, 0x0204022c022600, 0x0202b80102bc01
    ),
    (
        2, 1641168000000, 0x02040256026000, 0x02020e0212
    ),
    (
        26, 1642032000000, 0x0204029a01021000, 0x0202060204
    ),
    (
        33, 1643414400000, 0x0204027e02880100, 0x02022a0228
    ),
    (
        70, 1642032000000, 0x02040230026000, 0x020252029e01
    ),
    (
        91, 1643241600000, 0x020402ac01020200, 0x02025a0202
    ),
    (
        9, 1643155200000, 0x0204020a023200, 0x02021e0252
    ),
    (
        78, 1642377600000, 0x020402a001027e00, 0x02028e0102b801
    ),
    (
        79, 1641945600000, 0x02040216022000, 0x02029c01026a
    ),
    (
        99, 1641686400000, 0x0204028e01023600, 0x020264029e01
    ),
    (
        18, 1642809600000, 0x0204029001026400, 0x02024c028c01
    ),
    (
        32, 1643414400000, 0x02040246021c00, 0x02028201027c
    ),
    (
        64, 1641340800000, 0x02040268022e00, 0x02023c02be01
    ),
    (
        67, 1643241600000, 0x0204025c02a80100, 0x02024a02b001
    ),
    (
        93, 1642809600000, 0x0204025602c80100, 0x020210029201
    ),
    (
        22, 1641340800000, 0x0204029001025c00, 0x020288010258
    ),
    (
        35, 1643500800000, 0x020402a001021e00, 0x02026a0218
    ),
    (
        55, 1641859200000, 0x02040212020c00, 0x02020a020c
    ),
    (
        56, 1641859200000, 0x020402ac01022c00, 0x02021c02ac01
    ),
    (
        57, 1642896000000, 0x0204022802aa0100, 0x0202c0010268
    ),
    (
        60, 1642291200000, 0x0204023a027c00, 0x0202a801029e01
    ),
    (
        62, 1642118400000, 0x020402c201023600, 0x02024e028c01
    ),
    (
        83, 1642896000000, 0x0204026602940100, 0x02029001028a01
    ),
    (
        97, 1641600000000, 0x02040274020400, 0x0202a001029001
    ),
    (
        14, 1642377600000, 0x02040252025600, 0x02029e010200
    ),
    (
        17, 1641254400000, 0x020402a80102be0100, 0x02022a02ba01
    ),
    (
        20, 1641340800000, 0x02040256027600, 0x020226024a
    ),
    (
        21, 1641513600000, 0x020402be01020600, 0x020286010254
    ),
    (
        61, 1642550400000, 0x02040212027200, 0x0202380226
    ),
    (
        63, 1641513600000, 0x02040226024400, 0x0202660252
    ),
    (
        68, 1641513600000, 0x0204026e025800, 0x0202b201021c
    ),
    (
        74, 1641686400000, 0x0204027202b80100, 0x020222027c
    );