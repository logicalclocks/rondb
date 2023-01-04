DROP DATABASE IF EXISTS DB021;

CREATE DATABASE DB021;

USE DB021;

-- blobs in PK is not supported by RonDB
CREATE TABLE `time_table0` (
    `id0` time(0),
    `col0` time(0) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    time_table0
values
("11:11:11", "11:11:11");

insert into
    time_table0
set
    id0 = "12:11:11";

CREATE TABLE `time_table3` (
    `id0` time(3),
    `col0` time(3) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    time_table3
values
("11:11:11.123", "11:11:11.123");

insert into
    time_table3
set
    id0 = "12:11:11.123";

CREATE TABLE `time_table6` (
    `id0` time(6),
    `col0` time(6) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    time_table6
values
("11:11:11.123456", "11:11:11.123456");

insert into
    time_table6
set
    id0 = "12:11:11.123456";