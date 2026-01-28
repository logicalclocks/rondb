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
DROP DATABASE IF EXISTS db027;

CREATE DATABASE db027;

USE db027;

-- Could also do varchar with latin1

-- Maxing out the row size (30000 bytes)
CREATE TABLE table_1 (
    id VARBINARY(5),
    col0 VARBINARY(29990),
    PRIMARY KEY(id)
) ENGINE = ndbcluster;

-- col0: 'A' in ASCII 29990 times; col0: 'B' in ASCII
-- Will be returned as base64 string in any case
INSERT INTO
    table_1 (id, col0)
VALUES
    ("1", REPEAT(X'41', 29990));
