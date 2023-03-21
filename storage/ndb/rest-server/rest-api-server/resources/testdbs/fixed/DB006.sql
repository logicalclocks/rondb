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

DROP DATABASE IF EXISTS db006;

CREATE DATABASE db006;

USE db006;

CREATE TABLE tinyint_table(
    id0 TINYINT,
    id1 TINYINT UNSIGNED,
    col0 TINYINT,
    col1 TINYINT UNSIGNED,
    PRIMARY KEY(id0, id1)
) ENGINE = ndbcluster;

INSERT INTO
    tinyint_table
VALUES
    (127, 255, 127, 255);

INSERT INTO
    tinyint_table
VALUES
    (-128, 0, -128, 0);

INSERT INTO
    tinyint_table
VALUES
    (0, 0, 0, 0);

-- NULL values for non primary columns
INSERT INTO
    tinyint_table
set
    id0 = 1,
    id1 = 1;
