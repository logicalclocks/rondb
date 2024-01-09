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

DROP DATABASE IF EXISTS db003;

CREATE DATABASE db003;

USE db003;

CREATE TABLE `date_table` (
    `id0` int NOT NULL,
    `col0` date DEFAULT NULL,
    `col1` time DEFAULT NULL,
    `col2` datetime DEFAULT NULL,
    `col3` timestamp NULL DEFAULT NULL,
    `col4` year DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

INSERT INTO
    date_table
values
    (
        1,
        "1111-11-11",
        "11:11:11",
        "1111-11-11 11:11:11",
        "1970-11-11 11:11:11",
        "11"
    );

INSERT INTO
    date_table
set
    id0 = 2;

CREATE TABLE `arrays_table` (
    `id0` int NOT NULL,
    `col0` char(100) DEFAULT NULL,
    `col2` varchar(100) DEFAULT NULL,
    `col3` binary(100) DEFAULT NULL,
    `col4` varbinary(100) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

INSERT INTO
    arrays_table
values
    (1, "abcd", "abcd", 0xFFFF, 0xFFFF);

INSERT INTO
    arrays_table
set
    id0 = 2;

CREATE TABLE `set_table` (
    `id0` int NOT NULL,
    `col0` enum('a', 'b', 'c', 'd') DEFAULT NULL,
    `col1`
    set
        ('a', 'b', 'c', 'd') DEFAULT NULL,
        PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

INSERT INTO
    `set_table`
VALUES
    (1, 'a', 'a');

INSERT INTO
    `set_table`
VALUES
    (2, 'b', 'a,b');

INSERT INTO
    set_table
set
    id0 = 3;

CREATE TABLE `special_table` (
    `id0` int NOT NULL,
    `col0` geometry DEFAULT NULL,
    `col1` point DEFAULT NULL,
    `col2` linestring DEFAULT NULL,
    `col3` polygon DEFAULT NULL,
    `col4` geomcollection DEFAULT NULL,
    `col5` multilinestring DEFAULT NULL,
    `col6` multipoint DEFAULT NULL,
    `col7` multipolygon DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

INSERT INTO
    special_table
set
    id0 = 1,
    col0 = ST_GeomFromText('POINT(1 1)'),
    col1 = ST_GeomFromText('POINT(1 1)'),
    col2 = ST_GeomFromText('LineString(1 1,2 2,3 3)'),
    col3 = ST_GeomFromText('Polygon((0 0,0 3,3 0,0 0),(1 1,1 2,2 1,1 1))'),
    col7 = ST_GeomFromText(
        'MultiPolygon(((0 0,0 3,3 3,3 0,0 0),(1 1,1 2,2 2,2 1,1 1)))'
    ),
    col4 = ST_GeomFromText(
        'GeometryCollection(Point(1 1),LineString(2 2, 3 3))'
    ),
    col6 = ST_MPointFromText('MULTIPOINT (1 1, 2 2, 3 3)'),
    col5 = ST_GeomFromText('MultiLineString((1 1,2 2,3 3),(4 4,5 5))');

INSERT INTO
    special_table
set
    id0 = 2;

CREATE TABLE `number_table` (
    `id0` int NOT NULL,
    `col0` tinyint DEFAULT NULL,
    `col1` smallint DEFAULT NULL,
    `col2` mediumint DEFAULT NULL,
    `col3` int DEFAULT NULL,
    `col4` bigint DEFAULT NULL,
    `col5` decimal(10, 0) DEFAULT NULL,
    `col6` float DEFAULT NULL,
    `col7` double DEFAULT NULL,
    `col8` bit(1) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

INSERT INTO
    `number_table`
VALUES
    (1, 99, 99, 99, 99, 99, 99, 99.99, 99.99, true);

INSERT INTO
    number_table
set
    id0 = 2;

CREATE TABLE `blob_table` (
    `id0` int NOT NULL,
    `col0` tinyblob,
    `col1` blob,
    `col2` mediumblob,
    `col3` longblob,
    `col4` tinytext,
    `col5` mediumtext,
    `col6` longtext,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

INSERT INTO
    blob_table
values
    (
        1,
        0xFFFF,
        0xFFFF,
        0xFFFF,
        0xFFFF,
        "abcd",
        "abcd",
        "abcd"
    );

INSERT INTO
    blob_table
set
    id0 = 2;
