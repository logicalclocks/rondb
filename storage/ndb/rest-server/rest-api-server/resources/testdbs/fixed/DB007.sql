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

DROP DATABASE IF EXISTS db007;

CREATE DATABASE db007;

USE db007;

CREATE TABLE smallint_table(
    id0 SMALLINT,
    id1 SMALLINT UNSIGNED,
    col0 SMALLINT,
    col1 SMALLINT UNSIGNED,
    PRIMARY KEY(id0, id1)
) ENGINE = ndbcluster;

INSERT INTO
    smallint_table
VALUES
    (32767, 65535, 32767, 65535);

INSERT INTO
    smallint_table
VALUES
    (-32768, 0, -32768, 0);

INSERT INTO
    smallint_table
VALUES
    (0, 0, 0, 0);

-- NULL values for non primary columns
INSERT INTO
    smallint_table
set
    id0 = 1,
    id1 = 1;
