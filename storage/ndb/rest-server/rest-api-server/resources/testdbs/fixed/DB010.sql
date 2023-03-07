DROP DATABASE IF EXISTS db010;

CREATE DATABASE db010;

USE db010;

CREATE TABLE double_table1(
    id0 INT,
    col0 DOUBLE,
    col1 DOUBLE UNSIGNED,
    PRIMARY KEY(id0)
) ENGINE = ndbcluster;

INSERT INTO
    double_table1
VALUES
(1, -123.123, 123.123);

INSERT INTO
    double_table1
VALUES
(0, 0, 0);

-- NULL values for non primary columns
INSERT INTO
    double_table1
set
    id0 = 2;

CREATE TABLE double_table2(
    id0 DOUBLE,
    col0 DOUBLE,
    col1 DOUBLE UNSIGNED,
    PRIMARY KEY(id0)
) ENGINE = ndbcluster;