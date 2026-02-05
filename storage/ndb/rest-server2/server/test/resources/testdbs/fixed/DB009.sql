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

DROP DATABASE IF EXISTS db009;

CREATE DATABASE db009;

USE db009;

CREATE TABLE float_table1(
    id0 INT,
    col0 FLOAT,
    col1 FLOAT UNSIGNED,
    PRIMARY KEY(id0)
) ENGINE = ndbcluster;

INSERT INTO
    float_table1
VALUES
    (1, -123.123, 123.123);

INSERT INTO
    float_table1
VALUES
    (0, 0, 0);

-- NULL values for non primary columns
INSERT INTO
    float_table1
set
    id0 = 2;

CREATE TABLE float_table2(
    id0 FLOAT,
    col0 FLOAT,
    col1 FLOAT UNSIGNED,
    PRIMARY KEY(id0)
) ENGINE = ndbcluster;
