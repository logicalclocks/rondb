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

-- This database is used for testing changing schema

DROP DATABASE IF EXISTS db025;

CREATE DATABASE db025;

USE db025;

-- During the test this table will be dropped and recreated with different schema 

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
        '1',
        'col0_data',
        'col1_data',
        'col2_data'
    );
