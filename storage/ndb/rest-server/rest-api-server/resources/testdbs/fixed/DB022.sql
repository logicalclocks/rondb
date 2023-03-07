DROP DATABASE IF EXISTS db022;

CREATE DATABASE db022;

USE db022;

--  blobs in PK is not supported by Rondb
CREATE TABLE `ts_table0` (
    `id0` timestamp(0),
    `col0` timestamp(0) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    ts_table0
values
    ("2022-11-11 11:11:11", "2022-11-11 11:11:11");

insert into
    ts_table0
set
    id0 = "2022-11-12 11:11:11";

CREATE TABLE `ts_table3` (
    `id0` timestamp(3),
    `col0` timestamp(3) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    ts_table3
values
    (
        "2022-11-11 11:11:11.123",
        "2022-11-11 11:11:11.123"
    );

insert into
    ts_table3
set
    id0 = "2022-11-12 11:11:11.123";

CREATE TABLE `ts_table6` (
    `id0` timestamp(6),
    `col0` timestamp(6) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    ts_table6
values
    (
        "2022-11-11 11:11:11.123456",
        "2022-11-11 11:11:11.123456"
    );

insert into
    ts_table6
set
    id0 = "2022-11-12 11:11:11.123456";