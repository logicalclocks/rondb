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

DROP DATABASE IF EXISTS db005;

CREATE DATABASE db005;

USE db005;

CREATE TABLE bigint_table(
    id0 BIGINT,
    id1 BIGINT UNSIGNED,
    col0 BIGINT,
    col1 BIGINT UNSIGNED,
    PRIMARY KEY(id0, id1)
) ENGINE = ndbcluster;

INSERT INTO
    bigint_table
VALUES
    (
        9223372036854775807,
        18446744073709551615,
        9223372036854775807,
        18446744073709551615
    );

INSERT INTO
    bigint_table
VALUES
    (-9223372036854775808, 0, -9223372036854775808, 0);

INSERT INTO
    bigint_table
VALUES
    (0, 0, 0, 0);

-- NULL values for non primary columns
INSERT INTO
    bigint_table
set
    id0 = 1,
    id1 = 1;
