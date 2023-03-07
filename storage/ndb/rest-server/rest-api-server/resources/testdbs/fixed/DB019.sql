DROP DATABASE IF EXISTS db019;

CREATE DATABASE db019;

USE db019;

-- blobs in PK is not supported by Rondb
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