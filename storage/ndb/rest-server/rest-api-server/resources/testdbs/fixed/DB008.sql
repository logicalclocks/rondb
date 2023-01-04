DROP DATABASE IF EXISTS DB008;

CREATE DATABASE DB008;

USE DB008;

CREATE TABLE mediumint_table(
    id0 MEDIUMINT,
    id1 MEDIUMINT UNSIGNED,
    col0 MEDIUMINT,
    col1 MEDIUMINT UNSIGNED,
    PRIMARY KEY(id0, id1)
) ENGINE = ndbcluster;

INSERT INTO
    mediumint_table
VALUES
(8388607, 16777215, 8388607, 16777215);

INSERT INTO
    mediumint_table
VALUES
(-8388608, 0, -8388608, 0);

INSERT INTO
    mediumint_table
VALUES
(0, 0, 0, 0);

-- NULL values for non primary columns
INSERT INTO
    mediumint_table
set
    id0 = 1,
    id1 = 1;