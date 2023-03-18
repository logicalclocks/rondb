DROP DATABASE IF EXISTS db013;

CREATE DATABASE db013;

USE db013;

-- blobs in PK is not supported by Rondb
CREATE TABLE blob_table(id0 int, col0 blob, col1 int, PRIMARY KEY(id0)) ENGINE = ndbcluster;

INSERT INTO
    blob_table
VALUES
    (1, 0xFFFF, 1);

CREATE TABLE text_table(id0 int, col0 text, col1 int, PRIMARY KEY(id0)) ENGINE = ndbcluster;

INSERT INTO
    text_table
VALUES
    (1, "FFFF", 1);