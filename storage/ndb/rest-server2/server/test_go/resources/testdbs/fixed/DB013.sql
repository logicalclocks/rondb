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

DROP DATABASE IF EXISTS db013;

CREATE DATABASE db013;

USE db013;

-- blobs in PK is not supported by Rondb
CREATE TABLE blob_table(id0 varbinary(255), col0 blob, col1 int, PRIMARY KEY(id0)) ENGINE = ndbcluster;

INSERT INTO
    blob_table
VALUES
    ("1", lpad("", 64000, 0x40), 1);

INSERT INTO
    blob_table
VALUES
    ("2", null, 1);

CREATE TABLE text_table(id0 varchar(255), col0 text, col1 int, PRIMARY KEY(id0)) ENGINE = ndbcluster;

INSERT INTO
    text_table
VALUES
    ("1", lpad("", 64000, "0"), 1);

INSERT INTO
    text_table
VALUES
    ("2", null, 1);

-- 0x9 is tab character
INSERT INTO
    text_table
VALUES
    ("3", lpad("", 64000, 0x9), 1);

-- 0x17 Non printable ASCII character (End of transmission block.)
INSERT INTO
    text_table
VALUES
    ("4", lpad("", 64000, 0x17), 1);

-- 0x17 Non printable unicode character U+2000 ==  0xe2 0x80 0x80 
INSERT INTO
    text_table
VALUES
    ("5", lpad("", 20000, 0xe28080), 1);
