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

DROP DATABASE IF EXISTS db024;

CREATE DATABASE db024;

USE db024;

-- blobs in PK is not supported by RonDB
CREATE TABLE `bit_table` (
    `id0` binary(100),
    `col0` bit(1) DEFAULT NULL,
    `col1` bit(3) DEFAULT NULL,
    `col2` bit(25) DEFAULT NULL,
    `col3` bit(39) DEFAULT NULL,
    col4 bit(64) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    bit_table
values
    (
        1,
        b'1',
        b'111',
        b'1111111111111111111111111',
        b'111111111111111111111111111111111111111',
        b'1111111111111111111111111111111111111111111111111111111111111111'
    );

insert into
    bit_table
values
    (
        2,
        b'0',
        b'000',
        b'0000000000000000000000000',
        b'000000000000000000000000000000000000000',
        b'0000000000000000000000000000000000000000000000000000000000000000'
    );

insert into
    bit_table
set
    id0 = "3";
