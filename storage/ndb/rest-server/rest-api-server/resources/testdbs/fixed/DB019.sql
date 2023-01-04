DROP DATABASE IF EXISTS DB019;

CREATE DATABASE DB019;

USE DB019;

-- blobs in PK is not supported by RonDB
CREATE TABLE `date_table` (
    `id0` date,
    `col0` date DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    date_table
values
    ("1111-11-11", "1111:11:11");

insert into
    date_table
set
    id0 = "1111-11-12";