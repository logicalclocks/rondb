-- Copyright (C) 2023 Hopsworks AB
--
-- This program is free software; you can redistribute it and/or
-- modify it under the terms of the GNU General Public License
-- as published by the Free Software Foundation; either version 2
-- of the License, or (at your option) any later version.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
-- USA.

DROP DATABASE IF EXISTS db022;

CREATE DATABASE db022;

USE db022;

--  blobs in PK is not supported by Rondb
CREATE TABLE `ts_table0` (
    `id0` timestamp(0),
    `col0` timestamp(0) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    ts_table0
values
    ("2022-11-11 11:11:11", "2022-11-11 11:11:11");

insert into
    ts_table0
set
    id0 = "2022-11-12 11:11:11";

CREATE TABLE `ts_table3` (
    `id0` timestamp(3),
    `col0` timestamp(3) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    ts_table3
values
    (
        "2022-11-11 11:11:11.123",
        "2022-11-11 11:11:11.123"
    );

insert into
    ts_table3
set
    id0 = "2022-11-12 11:11:11.123";

CREATE TABLE `ts_table6` (
    `id0` timestamp(6),
    `col0` timestamp(6) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    ts_table6
values
    (
        "2022-11-11 11:11:11.123456",
        "2022-11-11 11:11:11.123456"
    );

insert into
    ts_table6
set
    id0 = "2022-11-12 11:11:11.123456";
