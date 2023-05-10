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
DROP DATABASE IF EXISTS fsdb001;

CREATE DATABASE fsdb001;

USE fsdb001;

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

CREATE TABLE `sample_1_2` (
  `id1` bigint NOT NULL,
  `ts` timestamp NULL DEFAULT NULL,
  `data1` bigint DEFAULT NULL,
  `data2` bigint DEFAULT NULL,
  PRIMARY KEY (`id1`)
) ENGINE=ndbcluster;

INSERT INTO
    sample_1_2
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

CREATE TABLE `sample_3_1` (
  `id1` bigint NOT NULL,
  `id2` varchar(100) COLLATE utf8_unicode_ci NOT NULL,
  `ts` timestamp NULL DEFAULT NULL,
  `bigint` bigint DEFAULT NULL,
  `string` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `date` date DEFAULT NULL,
  `bool` tinyint DEFAULT NULL,
  `float` float DEFAULT NULL,
  `double` double DEFAULT NULL,
  `binary` varbinary(100) DEFAULT NULL,
  PRIMARY KEY (`id1`,`id2`)
) ENGINE=ndbcluster DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

INSERT INTO
    sample_3_1
VALUES
    (
        93, 'id93', Timestamp('2022-01-05 00:00:00'), 34, '58', '2022-01-03', True, 91.0, 72.5, '0'
    ),
    (
        53, 'id53', Timestamp('2022-01-05 00:00:00'), 48, '16', '2022-01-16', True, 89.0, 60.0, '0'
    ),
    (
        45, 'id45', Timestamp('2022-01-09 00:00:00'), 48, '42', '2022-01-15', False, 82.0, 120.0, '0'
    ),
    (
        81, 'id81', Timestamp('2022-01-29 00:00:00'), 6, '90', '2022-01-07', True, 48.0, 147.5, '0'
    ),
    (
        20, 'id20', Timestamp('2022-01-04 00:00:00'), 58, '54', '2022-01-28', False, 16.0, 162.5, '0'
    ),
    (
        50, 'id50', Timestamp('2022-01-25 00:00:00'), 75, '19', '2022-01-02', True, 9.0, 200.0, '0'
    ),
    (
        23, 'id23', Timestamp('2022-01-04 00:00:00'), 49, '56', '2022-01-20', True, 14.0, 145.0, '0'
    ),
    (
        51, 'id51', Timestamp('2022-01-10 00:00:00'), 49, '2', '2022-01-21', False, 66.0, 102.5, '0'
    ),
    (
        95, 'id95', Timestamp('2022-01-09 00:00:00'), 51, '76', '2022-01-19', False, 12.0, 105.0, '0'
    ),
    (
        77, 'id77', Timestamp('2022-01-04 00:00:00'), 47, '93', '2022-01-20', True, 27.0, 45.0, '0'
    );
