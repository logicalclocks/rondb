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

DROP DATABASE IF EXISTS db001;

CREATE DATABASE db001;

USE db001;

CREATE TABLE table_1(
    id0 VARCHAR(10),
    col0 VARCHAR(100),
    col1 VARCHAR(100),
    col2 VARCHAR(100),
    PRIMARY KEY(id0)
) ENGINE = ndbcluster;

INSERT INTO
    table_1
VALUES
    (
        'id0_data',
        'col_0_data',
        'col_1_data',
        'col_2_data'
    );
