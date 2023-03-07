DROP DATABASE IF EXISTS db006;

CREATE DATABASE db006;

USE db006;

CREATE TABLE tinyint_table(
    id0 TINYINT,
    id1 TINYINT UNSIGNED,
    col0 TINYINT,
    col1 TINYINT UNSIGNED,
    PRIMARY KEY(id0, id1)
) ENGINE = ndbcluster;

INSERT INTO
    tinyint_table
VALUES
(127, 255, 127, 255);

INSERT INTO
    tinyint_table
VALUES
(-128, 0, -128, 0);

INSERT INTO
    tinyint_table
VALUES
(0, 0, 0, 0);

-- NULL values for non primary columns
INSERT INTO
    tinyint_table
set
    id0 = 1,
    id1 = 1;