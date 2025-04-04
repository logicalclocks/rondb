-- This file is part of the RonDB REST API Server
-- Copyright (c) 2025 Hopsworks AB
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
DROP DATABASE IF EXISTS FSDB003;

CREATE DATABASE FSDB003;

USE FSDB003;

CREATE TABLE `caps_1` (
  `id` int NOT NULL,
  `data` int DEFAULT NULL,
  PRIMARY KEY (`id`) USING HASH
) ENGINE=ndbcluster;

INSERT INTO
    caps_1
VALUES
    (
        1, 1
    );
