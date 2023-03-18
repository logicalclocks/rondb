-- Sed keyword COLUMN_LENGTH
DROP DATABASE IF EXISTS rdrs_bench;

CREATE DATABASE rdrs_bench;

USE rdrs_bench;

CREATE TABLE table_1(
    id0 INT,
    col_0 VARCHAR(COLUMN_LENGTH),
    PRIMARY KEY(id0)
) ENGINE = ndbcluster;