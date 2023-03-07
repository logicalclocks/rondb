DROP DATABASE IF EXISTS db024;

CREATE DATABASE db024;

USE db024;

-- blobs in PK is not supported by RonDB
CREATE TABLE `bit_table` (
    `id0` binary(100),
    `col0` bit(1) DEFAULT NULL,
    `col1` bit(3) DEFAULT NULL,
    `col2` bit(25) DEFAULT NULL,
    `col3` bit(39) DEFAULT NULL,
    col4 bit(64) DEFAULT NULL,
    PRIMARY KEY (`id0`)
) ENGINE = ndbcluster;

insert into
    bit_table
values
(
        1,
        b '1',
        b '111',
        b '1111111111111111111111111',
        b '111111111111111111111111111111111111111',
        b '1111111111111111111111111111111111111111111111111111111111111111'
    );

insert into
    bit_table
values
(
        2,
        b '0',
        b '000',
        b '0000000000000000000000000',
        b '000000000000000000000000000000000000000',
        b '0000000000000000000000000000000000000000000000000000000000000000'
    );

insert into
    bit_table
set
    id0 = "3";