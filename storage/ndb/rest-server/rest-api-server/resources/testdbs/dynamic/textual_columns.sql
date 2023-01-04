-- Sed keywords DATABASE_NAME, COLUMN_TYPE, COLUMN_LENGTH

DROP DATABASE IF EXISTS DATABASE_NAME;

CREATE DATABASE DATABASE_NAME;

USE DATABASE_NAME;

-- blobs in PK is not supported by RonDB
CREATE TABLE table1(
    id0 COLUMN_TYPE (COLUMN_LENGTH),
    col0 COLUMN_TYPE (COLUMN_LENGTH),
    PRIMARY KEY(id0)
) ENGINE = ndbcluster;

INSERT INTO
    table1
VALUES
("1", "这是一个测验。 我不知道怎么读中文。");

INSERT INTO
    table1
VALUES
("2", 0x660066);

INSERT INTO
    table1
VALUES
("3", "a\nb");

INSERT INTO
    table1
VALUES
("这是一个测验", "12345");

-- some chars
INSERT INTO
    table1
VALUES
(
        "4",
        "ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïð"
    );

INSERT INTO
    table1
set
    id0 = 5;

-- in mysql \f is replaced by f
INSERT INTO
    table1
VALUES
("6", `"\\\b\f\n\r\t$%_?`);

-- testing quoted primary key
INSERT INTO
    table1
VALUES
(`"7"`, "abc");