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
-- This database is used for testing changing schema
DROP DATABASE IF EXISTS db028;

CREATE DATABASE db028;

USE db028;

CREATE TABLE table_1 (
    id INT,
    col0 varchar(1000) DEFAULT "TEST DATA",
    col1 varchar(1000) DEFAULT NULL,
    col2 INT DEFAULT 100,
    col3 INT DEFAULT NULL,
    PRIMARY KEY(id)
) ENGINE = ndbcluster;

INSERT INTO
    table_1 (id)
VALUES
    ("1");
