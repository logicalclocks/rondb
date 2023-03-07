DROP DATABASE IF EXISTS db009;

CREATE DATABASE db009;

USE db009;

CREATE TABLE float_table1(
    id0 INT,
    col0 FLOAT,
    col1 FLOAT UNSIGNED,
    PRIMARY KEY(id0)
) ENGINE = ndbcluster;

INSERT INTO
    float_table1
VALUES
(1, -123.123, 123.123);

INSERT INTO
    float_table1
VALUES
(0, 0, 0);

-- NULL values for non primary columns
INSERT INTO
    float_table1
set
    id0 = 2;

CREATE TABLE float_table2(
    id0 FLOAT,
    col0 FLOAT,
    col1 FLOAT UNSIGNED,
    PRIMARY KEY(id0)
) ENGINE = ndbcluster;