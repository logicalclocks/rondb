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
DROP DATABASE IF EXISTS db026;

CREATE DATABASE db026;

USE db026;

-- Could also do varchar with latin1

-- Maxing out the primary key column size
CREATE TABLE table_1 (
    id VARBINARY(3070),
    col0 VARBINARY,
    PRIMARY KEY(id)
) ENGINE = ndbcluster;

-- pk: 'A' in ASCII 3070 times; col0: 'B' in ASCII
-- Will be returned as base64 string in any case
INSERT INTO
    table_1 (id, col0)
VALUES
    (REPEAT(X'41', 3070), X'42');
