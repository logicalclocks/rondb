DROP DATABASE IF EXISTS DB001;

CREATE DATABASE DB001;

USE DB001;

CREATE TABLE table_1(
    id0 VARCHAR(10),
    col_0 VARCHAR(100),
    col_1 VARCHAR(100),
    col_2 VARCHAR(100),
    PRIMARY KEY(id0)
) ENGINE = ndbcluster;

INSERT INTO
    table_1
VALUES
(
        'id0_data',
        'col_0_data',
        'col_1_data',
        'col_2_data'
    );