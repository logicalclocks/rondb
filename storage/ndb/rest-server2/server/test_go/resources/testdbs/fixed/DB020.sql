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

-- Experimenting with different datetime lengths (0,3,6)
DROP DATABASE IF EXISTS db020;

CREATE DATABASE db020;

USE db020;

-- blobs in PK is not supported by Rondb
CREATE TABLE `date_table0` (
    `id0` datetime(0),
    `col0` datetime(0) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    date_table0
values
    ("1111-11-11 11:11:11", "1111-11-11 11:11:11");

insert into
    date_table0
set
    id0 = "1111-11-12 11:11:11";

CREATE TABLE `date_table3` (
    `id0` datetime(3),
    `col0` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    date_table3
values
    (
        "1111-11-11 11:11:11.123",
        "1111-11-11 11:11:11.123"
    );

insert into
    date_table3
set
    id0 = "1111-11-12 11:11:11.123";

CREATE TABLE `date_table6` (
    `id0` datetime(6),
    `col0` datetime(6) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    date_table6
values
    (
        "1111-11-11 11:11:11.123456",
        "1111-11-11 11:11:11.123456"
    );

insert into
    date_table6
set
    id0 = "1111-11-12 11:11:11.123456";
