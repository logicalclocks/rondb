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

DROP DATABASE IF EXISTS db025;

CREATE DATABASE db025;

USE db025;

-- During the test this table will be dropped and recreated with different schema

CREATE TABLE table_1(
    id0 VARCHAR(10),
    col0 VARCHAR(100),
    col1 VARCHAR(100),
    col2 VARCHAR(100),
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
    col0 VARCHAR(100),
    col1 VARCHAR(100),
    col2 VARCHAR(100),
    PRIMARY KEY(id0)
) ENGINE = ndbcluster;

INSERT INTO
    table_2
VALUES
    (
        1,
        'col0_data',
        'col1_data',
        'col2_data'
    );

-- table_3 and table_4 are an IDENTICAL pair, each with a secondary index
-- ix_col0. Both rows share the same index key (col0='col0_data') but carry a
-- distinct col1 marker, so an ix_col0 scan of one table returning the other's
-- marker is the wrong-table-read bug. They are used only by the index-scan
-- reproduction and are independent of table_1/table_2.

CREATE TABLE table_3(
    id0 INT(10),
    col0 VARCHAR(100),
    col1 VARCHAR(100),
    col2 VARCHAR(100),
    PRIMARY KEY(id0),
    KEY ix_col0 (col0)
) ENGINE = ndbcluster;

INSERT INTO
    table_3
VALUES
    (
        1,
        'col0_data',
        'MARKER_TABLE_3',
        'col2_data'
    );

CREATE TABLE table_4(
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
