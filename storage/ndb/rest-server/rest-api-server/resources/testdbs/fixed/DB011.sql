DROP DATABASE IF EXISTS db011;

CREATE DATABASE db011;

USE db011;

CREATE TABLE decimal_table(
    id0 DECIMAL(10, 5),
    id1 DECIMAL(10, 5) UNSIGNED,
    col0 DECIMAL(10, 5),
    col1 DECIMAL(10, 5) UNSIGNED,
    PRIMARY KEY(id0, id1)
) ENGINE = ndbcluster;

INSERT INTO
    decimal_table
VALUES
(
        -12345.12345,
        12345.12345,
        -12345.12345,
        12345.12345
    );

INSERT INTO
    decimal_table
set
    id0 = -67890.12345,
    id1 = 67890.12345;