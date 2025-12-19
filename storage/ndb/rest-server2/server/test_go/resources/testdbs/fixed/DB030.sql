-- Database: DB030
-- Tables for Index Range Scan Testing on Various Data Types

DROP DATABASE IF EXISTS db030;
CREATE DATABASE db030;
USE db030;

-- Integer types table with indexes
CREATE TABLE int_range_table (
    id INT NOT NULL AUTO_INCREMENT,
    col_tinyint TINYINT,
    col_tinyint_unsigned TINYINT UNSIGNED,
    col_smallint SMALLINT,
    col_smallint_unsigned SMALLINT UNSIGNED,
    col_mediumint MEDIUMINT,
    col_mediumint_unsigned MEDIUMINT UNSIGNED,
    col_int INT,
    col_int_unsigned INT UNSIGNED,
    col_bigint BIGINT,
    col_bigint_unsigned BIGINT UNSIGNED,
    PRIMARY KEY (id),
    KEY idx_tinyint (col_tinyint),
    KEY idx_tinyint_unsigned (col_tinyint_unsigned),
    KEY idx_smallint (col_smallint),
    KEY idx_smallint_unsigned (col_smallint_unsigned),
    KEY idx_mediumint (col_mediumint),
    KEY idx_mediumint_unsigned (col_mediumint_unsigned),
    KEY idx_int (col_int),
    KEY idx_int_unsigned (col_int_unsigned),
    KEY idx_bigint (col_bigint),
    KEY idx_bigint_unsigned (col_bigint_unsigned)
) ENGINE=ndbcluster;

-- Insert test data for integer types
INSERT INTO int_range_table (col_tinyint, col_tinyint_unsigned, col_smallint, col_smallint_unsigned,
    col_mediumint, col_mediumint_unsigned, col_int, col_int_unsigned, col_bigint, col_bigint_unsigned)
VALUES
    (-128, 0, -32768, 0, -8388608, 0, -2147483648, 0, -9223372036854775808, 0),
    (-64, 64, -16384, 16384, -4194304, 4194304, -1073741824, 1073741824, -4611686018427387904, 4611686018427387904),
    (-32, 96, -8192, 24576, -2097152, 6291456, -536870912, 1610612736, -2305843009213693952, 6917529027641081856),
    (0, 128, 0, 32768, 0, 8388608, 0, 2147483648, 0, 9223372036854775808),
    (32, 160, 8192, 40960, 2097152, 10485760, 536870912, 2684354560, 2305843009213693952, 11529215046068469760),
    (64, 192, 16384, 49152, 4194304, 12582912, 1073741824, 3221225472, 4611686018427387904, 13835058055282163712),
    (127, 255, 32767, 65535, 8388607, 16777215, 2147483647, 4294967295, 9223372036854775807, 18446744073709551615);

-- Float/Double types table with indexes
CREATE TABLE float_range_table (
    id INT NOT NULL AUTO_INCREMENT,
    col_float FLOAT,
    col_double DOUBLE,
    PRIMARY KEY (id),
    KEY idx_float (col_float),
    KEY idx_double (col_double)
) ENGINE=ndbcluster;

-- Insert test data for float/double types
INSERT INTO float_range_table (col_float, col_double)
VALUES
    (-1000.5, -1000000.123456),
    (-100.25, -100000.654321),
    (-10.125, -10000.111111),
    (0.0, 0.0),
    (10.125, 10000.111111),
    (100.25, 100000.654321),
    (1000.5, 1000000.123456);

-- Decimal types table with indexes
CREATE TABLE decimal_range_table (
    id INT NOT NULL AUTO_INCREMENT,
    col_decimal DECIMAL(10,2),
    col_decimal_unsigned DECIMAL(10,2) UNSIGNED,
    PRIMARY KEY (id),
    KEY idx_decimal (col_decimal),
    KEY idx_decimal_unsigned (col_decimal_unsigned)
) ENGINE=ndbcluster;

-- Insert test data for decimal types
INSERT INTO decimal_range_table (col_decimal, col_decimal_unsigned)
VALUES
    (-99999999.99, 0.00),
    (-50000000.50, 25000000.25),
    (-10000.00, 50000000.50),
    (0.00, 75000000.75),
    (10000.00, 80000000.00),
    (50000000.50, 90000000.00),
    (99999999.99, 99999999.99);

-- String types table with indexes
CREATE TABLE string_range_table (
    id INT NOT NULL AUTO_INCREMENT,
    col_char CHAR(20),
    col_varchar VARCHAR(50),
    PRIMARY KEY (id),
    KEY idx_char (col_char),
    KEY idx_varchar (col_varchar)
) ENGINE=ndbcluster;

-- Insert test data for string types
INSERT INTO string_range_table (col_char, col_varchar)
VALUES
    ('AAAA', 'alpha_001'),
    ('BBBB', 'beta_002'),
    ('CCCC', 'gamma_003'),
    ('DDDD', 'delta_004'),
    ('EEEE', 'epsilon_005'),
    ('FFFF', 'zeta_006'),
    ('GGGG', 'eta_007');

-- Binary types table with indexes
CREATE TABLE binary_range_table (
    id INT NOT NULL AUTO_INCREMENT,
    col_binary BINARY(8),
    col_varbinary VARBINARY(20),
    PRIMARY KEY (id),
    KEY idx_binary (col_binary),
    KEY idx_varbinary (col_varbinary)
) ENGINE=ndbcluster;

-- Insert test data for binary types (using hex values)
INSERT INTO binary_range_table (col_binary, col_varbinary)
VALUES
    (X'0000000000000001', X'0001'),
    (X'0000000000000010', X'0010'),
    (X'0000000000000100', X'0100'),
    (X'0000000000001000', X'1000'),
    (X'0000000000010000', X'00010000'),
    (X'0000000000100000', X'00100000'),
    (X'0000000001000000', X'01000000');

-- Date/Time types table with indexes
CREATE TABLE datetime_range_table (
    id INT NOT NULL AUTO_INCREMENT,
    col_date DATE,
    col_time TIME,
    col_datetime DATETIME,
    col_timestamp TIMESTAMP NULL,
    col_year YEAR,
    PRIMARY KEY (id),
    KEY idx_date (col_date),
    KEY idx_time (col_time),
    KEY idx_datetime (col_datetime),
    KEY idx_timestamp (col_timestamp),
    KEY idx_year (col_year)
) ENGINE=ndbcluster;

-- Insert test data for date/time types
INSERT INTO datetime_range_table (col_date, col_time, col_datetime, col_timestamp, col_year)
VALUES
    ('2020-01-01', '00:00:00', '2020-01-01 00:00:00', '2020-01-01 00:00:01', 2020),
    ('2021-03-15', '06:30:00', '2021-03-15 06:30:00', '2021-03-15 06:30:00', 2021),
    ('2022-06-30', '12:00:00', '2022-06-30 12:00:00', '2022-06-30 12:00:00', 2022),
    ('2023-09-15', '18:30:00', '2023-09-15 18:30:00', '2023-09-15 18:30:00', 2023),
    ('2024-12-31', '23:59:59', '2024-12-31 23:59:59', '2024-12-31 23:59:59', 2024),
    ('2025-06-15', '10:15:30', '2025-06-15 10:15:30', '2025-06-15 10:15:30', 2025),
    ('2026-01-01', '08:00:00', '2026-01-01 08:00:00', '2026-01-01 08:00:00', 2026);
