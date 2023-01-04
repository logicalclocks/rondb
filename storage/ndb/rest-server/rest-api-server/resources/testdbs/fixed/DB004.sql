-- Signed and unsigned number data types
DROP DATABASE IF EXISTS DB004;

CREATE DATABASE DB004;

USE DB004;

CREATE TABLE int_table(
    id0 INT,
    id1 INT UNSIGNED,
    col0 INT,
    col1 INT UNSIGNED,
    PRIMARY KEY(id0, id1)
) ENGINE = ndbcluster;

INSERT INTO
    int_table
VALUES
(2147483647, 4294967295, 2147483647, 4294967295);

INSERT INTO
    int_table
VALUES
(-2147483648, 0, -2147483648, 0);

INSERT INTO
    int_table
VALUES
(0, 0, 0, 0);

-- NULL values for non primary columns;
INSERT INTO
    int_table
set
    id0 = 1,
    id1 = 1;

-- this table only has primary keys
CREATE TABLE int_table1(id0 INT, id1 INT UNSIGNED, PRIMARY KEY(id0, id1)) ENGINE = ndbcluster;

INSERT INTO
    int_table1
VALUES
(0, 0);