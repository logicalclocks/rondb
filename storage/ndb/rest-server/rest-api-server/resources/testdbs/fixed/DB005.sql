DROP DATABASE IF EXISTS db005;

CREATE DATABASE db005;

USE db005;

CREATE TABLE bigint_table(
    id0 BIGINT,
    id1 BIGINT UNSIGNED,
    col0 BIGINT,
    col1 BIGINT UNSIGNED,
    PRIMARY KEY(id0, id1)
) ENGINE = ndbcluster;

INSERT INTO
    bigint_table
VALUES
    (
        9223372036854775807,
        18446744073709551615,
        9223372036854775807,
        18446744073709551615
    );

INSERT INTO
    bigint_table
VALUES
    (-9223372036854775808, 0, -9223372036854775808, 0);

INSERT INTO
    bigint_table
VALUES
    (0, 0, 0, 0);

-- NULL values for non primary columns
INSERT INTO
    bigint_table
set
    id0 = 1,
    id1 = 1;