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

-- Drop and recreate the identical pair in REVERSE order (perm_b before perm_a)
-- so NDB reuses/swaps their base-table ids. Because the NDB API keys its index
-- cache by base-table id (sys/def/<tabid>/<index>), a stale cached base-table
-- for perm_a then resolves its index to perm_b's storage - and since both
-- tables are identical and recreated together they share a version, so the
-- kernel raises no error and the wrong read is silent and permanent.

USE db025;

DROP TABLE IF EXISTS perm_a;

DROP TABLE IF EXISTS perm_b;

CREATE TABLE perm_b (
    id INT NOT NULL,
    col0 VARCHAR(100),
    col1 VARCHAR(100),
    updated_at TIMESTAMP(3) NULL DEFAULT NULL,
    PRIMARY KEY (id),
    KEY ix_col0 (col0),
    KEY ttl_index (updated_at)
) ENGINE = ndbcluster COMMENT = 'NDB_TABLE=READ_BACKUP=1,TTL=5184000@updated_at';

CREATE TABLE perm_a (
    id INT NOT NULL,
    col0 VARCHAR(100),
    col1 VARCHAR(100),
    updated_at TIMESTAMP(3) NULL DEFAULT NULL,
    PRIMARY KEY (id),
    KEY ix_col0 (col0),
    KEY ttl_index (updated_at)
) ENGINE = ndbcluster COMMENT = 'NDB_TABLE=READ_BACKUP=1,TTL=15552000@updated_at';

INSERT INTO perm_b VALUES (1, 'shared_key', 'PERM_MARKER_B', NOW(3));

INSERT INTO perm_a VALUES (1, 'shared_key', 'PERM_MARKER_A', NOW(3));
