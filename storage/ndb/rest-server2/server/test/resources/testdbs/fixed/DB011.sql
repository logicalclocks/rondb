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

DROP DATABASE IF EXISTS db011;

CREATE DATABASE db011;

USE db011;

CREATE TABLE decimal_table(
    id0 DECIMAL(10, 5),
    id1 DECIMAL(10, 5) UNSIGNED,
    col0 DECIMAL(10, 5),
    col1 DECIMAL(10, 5) UNSIGNED,
    PRIMARY KEY(id0, id1)
) ENGINE = ndbcluster;

INSERT INTO
    decimal_table
VALUES
    (
        -12345.12345,
        12345.12345,
        -12345.12345,
        12345.12345
    );

INSERT INTO
    decimal_table
set
    id0 = -67890.12345,
    id1 = 67890.12345;
