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

-- Signed and unsigned number data types
DROP DATABASE IF EXISTS fsdb001;

CREATE DATABASE fsdb001;

USE fsdb001;

CREATE TABLE fs_table(
    id0 INT,
    id1 INT UNSIGNED,
    col0 INT,
    col1 INT UNSIGNED,
    PRIMARY KEY(id0, id1)
) ENGINE = ndbcluster;

INSERT INTO
    fs_table
VALUES
    (0, 0, 0, 0);

