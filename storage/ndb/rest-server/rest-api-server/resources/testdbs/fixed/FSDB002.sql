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
