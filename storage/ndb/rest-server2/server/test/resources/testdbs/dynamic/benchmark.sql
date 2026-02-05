--
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

-- Sed keyword COLUMN_LENGTH, VARBINARY_PK_LENGTH, MANY_IDENTICAL_COLUMNS
DROP DATABASE IF EXISTS rdrs_bench;

CREATE DATABASE rdrs_bench;

USE rdrs_bench;

CREATE TABLE table_1(
    id0 INT,
    col0 VARCHAR(COLUMN_LENGTH),
    PRIMARY KEY(id0)
) ENGINE = ndbcluster CHARSET latin1;

CREATE TABLE table_2(
    id0 VARBINARY(VARBINARY_PK_LENGTH),
    col0 VARBINARY(COLUMN_LENGTH),
    PRIMARY KEY(id0)
) ENGINE = ndbcluster;

CREATE TABLE table_3(
    id0 INT,
    MANY_IDENTICAL_COLUMNS
    PRIMARY KEY(id0)
) ENGINE = ndbcluster CHARSET latin1;
