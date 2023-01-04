DROP DATABASE IF EXISTS DB023;

CREATE DATABASE DB023;

USE DB023;

-- blobs in PK is not supported by RonDB
CREATE TABLE `year_table` (
    `id0` year,
    `col0` year DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    year_table
values
("2022", "2022");

insert into
    year_table
set
    id0 = "2023";