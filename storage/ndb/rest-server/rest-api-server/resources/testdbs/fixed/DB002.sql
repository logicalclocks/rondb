DROP DATABASE IF EXISTS DB002;

CREATE DATABASE DB002;

USE DB002;

CREATE TABLE table_1(
    id0 VARCHAR(10),
    id1 VARCHAR(10),
    col_0 VARCHAR(100),
    col_1 VARCHAR(100),
    col_2 VARCHAR(100),
    PRIMARY KEY(id0, id1)
) ENGINE = ndbcluster;

INSERT INTO
    table_1
VALUES
    (
        'id0_data',
        'id1_data',
        'col_0_data',
        'col_1_data',
        'col_2_data'
    );