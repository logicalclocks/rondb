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

DROP DATABASE IF EXISTS db008;

CREATE DATABASE db008;

USE db008;

CREATE TABLE mediumint_table(
    id0 MEDIUMINT,
    id1 MEDIUMINT UNSIGNED,
    col0 MEDIUMINT,
    col1 MEDIUMINT UNSIGNED,
    PRIMARY KEY(id0, id1)
) ENGINE = ndbcluster;

INSERT INTO
    mediumint_table
VALUES
    (8388607, 16777215, 8388607, 16777215);

INSERT INTO
    mediumint_table
VALUES
    (-8388608, 0, -8388608, 0);

INSERT INTO
    mediumint_table
VALUES
    (0, 0, 0, 0);

-- NULL values for non primary columns
INSERT INTO
    mediumint_table
set
    id0 = 1,
    id1 = 1;
