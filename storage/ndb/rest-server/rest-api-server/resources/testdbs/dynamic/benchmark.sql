--
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

-- Sed keyword COLUMN_LENGTH
DROP DATABASE IF EXISTS rdrs_bench;

CREATE DATABASE rdrs_bench;

USE rdrs_bench;

CREATE TABLE table_1(
    id0 INT,
    col0 VARCHAR(COLUMN_LENGTH),
    PRIMARY KEY(id0)
) ENGINE = ndbcluster;
