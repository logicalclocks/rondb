-- Experimenting with different datetime lengths (0,3,6)

DROP DATABASE IF EXISTS DB020;

CREATE DATABASE DB020;

USE DB020;

-- blobs in PK is not supported by RonDB
CREATE TABLE `date_table0` (
    `id0` datetime(0),
    `col0` datetime(0) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    date_table0
values
    ("1111-11-11 11:11:11", "1111-11-11 11:11:11");

insert into
    date_table0
set
    id0 = "1111-11-12 11:11:11";

CREATE TABLE `date_table3` (
    `id0` datetime(3),
    `col0` datetime(3) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    date_table3
values
    (
        "1111-11-11 11:11:11.123",
        "1111-11-11 11:11:11.123"
    );

insert into
    date_table3
set
    id0 = "1111-11-12 11:11:11.123";

CREATE TABLE `date_table6` (
    `id0` datetime(6),
    `col0` datetime(6) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    date_table6
values
    (
        "1111-11-11 11:11:11.123456",
        "1111-11-11 11:11:11.123456"
    );

insert into
    date_table6
set
    id0 = "1111-11-12 11:11:11.123456";