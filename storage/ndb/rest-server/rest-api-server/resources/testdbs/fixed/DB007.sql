DROP DATABASE IF EXISTS db007;

CREATE DATABASE db007;

USE db007;

CREATE TABLE smallint_table(
    id0 SMALLINT,
    id1 SMALLINT UNSIGNED,
    col0 SMALLINT,
    col1 SMALLINT UNSIGNED,
    PRIMARY KEY(id0, id1)
) ENGINE = ndbcluster;

INSERT INTO
    smallint_table
VALUES
    (32767, 65535, 32767, 65535);

INSERT INTO
    smallint_table
VALUES
    (-32768, 0, -32768, 0);

INSERT INTO
    smallint_table
VALUES
    (0, 0, 0, 0);

-- NULL values for non primary columns
INSERT INTO
    smallint_table
set
    id0 = 1,
    id1 = 1;