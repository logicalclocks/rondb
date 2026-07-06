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


USE db025;

DROP TABLE IF EXISTS table_1;

DROP TABLE IF EXISTS table_2;

DROP TABLE IF EXISTS table_3;

DROP TABLE IF EXISTS table_4;

CREATE TABLE table_1(
    id0 VARCHAR(10),
    new_col0 VARCHAR(100),
    new_col1 VARCHAR(100),
    new_col2 VARCHAR(100),
    PRIMARY KEY(id0)
) ENGINE = ndbcluster;

INSERT INTO
    table_1
VALUES
    (
        '1',
        'col0_data',
        'col1_data',
        'col2_data'
    );

CREATE TABLE table_2(
    id0 INT(10),
    new_col0 VARCHAR(100),
    new_col1 VARCHAR(100),
    new_col2 VARCHAR(100),
    PRIMARY KEY(id0)
) ENGINE = ndbcluster;

INSERT INTO
    table_2
VALUES
    (
        1,
        'new_col0_data',
        'new_col1_data',
        'new_col2_data'
    );

-- Recreate the identical pair in REVERSE order (table_4 before table_3) so NDB
-- hands table_3's freed internal table id to the new table_4, making a stale
-- cached "table_3" object resolve to table_4's storage. Markers are preserved
-- so the index-scan reproduction can detect a cross-table read.

CREATE TABLE table_4(
    id0 INT(10),
    col0 VARCHAR(100),
    col1 VARCHAR(100),
    col2 VARCHAR(100),
    PRIMARY KEY(id0),
    KEY ix_col0 (col0)
) ENGINE = ndbcluster;

CREATE TABLE table_3(
    id0 INT(10),
    col0 VARCHAR(100),
    col1 VARCHAR(100),
    col2 VARCHAR(100),
    PRIMARY KEY(id0),
    KEY ix_col0 (col0)
) ENGINE = ndbcluster;

INSERT INTO
    table_4
VALUES
    (
        1,
        'col0_data',
        'MARKER_TABLE_4',
        'col2_data'
    );

INSERT INTO
    table_3
VALUES
    (
        1,
        'col0_data',
        'MARKER_TABLE_3',
        'col2_data'
    );
