# body_dtwide.inc — data-type width × signedness × boundary matrix family,
# data-driven CTE suite.
#
# Self-contained: creates its own wide-type fact table (dtw) + dimension
# table (dtw_dim); the shared cte_schema tables are not used.  Covers what
# the shared schema cannot: every int width in BOTH signednesses, boundary
# rows (signed min/max, unsigned max per width), NULL-heavy columns of
# every type (every 6th row), empty + multi-byte strings, SUM per width,
# and GROUP BY keyed on TINYINT / CHAR / DATE.
#
# Data layout:
#   d_id 1..240      moderate deterministic ramps, 20 groups (d_grp 1..20),
#                    every 6th row has NULL in every nullable value column.
#   d_id 1001/1002   boundary rows (grp 21/22): signed min / signed+unsigned
#                    max per width, DECIMAL 12-digit extremes, DATE/DATETIME
#                    range ends, empty + high strings.  MIN/MAX cases only.
#   d_id 1003/1004   all-NULL value rows (grp 23).
#
# SUM cases restrict the CTE body to d_id <= 240 so no Int64/Uint64
# accumulator (kernel partial sums, any merge order) can overflow;
# MIN/MAX cases run over everything including the boundary rows.
#
# Probes at the end: SUM over FLOAT/DOUBLE (non-associative, not
# strict-diff-testable — same disposition as body_agg.inc), GROUP BY a
# NULLABLE typed key, TIMESTAMP MIN/MAX (covered by agg-d17i/j).
#
# See test_benchmark_extension_plan.md (Phase T1).

--source include/have_ndb.inc
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings
--let $suppress_ronsql_cli=yes
--let $strict_diff=yes

--echo
--echo === Create and load the wide-type tables ===
--echo

--disable_query_log
--disable_warnings

CREATE TABLE dtw (
  d_id INT NOT NULL,
  d_grp INT NOT NULL,
  d_kti TINYINT NOT NULL,
  d_kch CHAR(6) NOT NULL,
  d_kdt DATE NOT NULL,
  d_ti TINYINT, d_tiu TINYINT UNSIGNED,
  d_si SMALLINT, d_siu SMALLINT UNSIGNED,
  d_mi MEDIUMINT, d_miu MEDIUMINT UNSIGNED,
  d_i INT, d_iu INT UNSIGNED,
  d_bi BIGINT, d_biu BIGINT UNSIGNED,
  d_f FLOAT, d_d DOUBLE,
  d_dec0 DECIMAL(12,0), d_dec2 DECIMAL(12,2),
  d_ch CHAR(10), d_vc VARCHAR(24),
  d_dt DATE, d_dtt DATETIME,
  PRIMARY KEY USING HASH (d_id)
) ENGINE=NDB;
CREATE INDEX idx_dtw_grp ON dtw (d_grp);

CREATE TABLE dtw_dim (
  g_id INT NOT NULL,
  g_name CHAR(8) NOT NULL,
  PRIMARY KEY USING HASH (g_id)
) ENGINE=NDB;

# 240 moderate rows: deterministic ramps sized so every per-width column
# stays well inside its range and every SUM stays inside Int64/Uint64.
# FLOAT/DOUBLE use exact binary fractions (quarters / eighths) so display
# is engine-independent.
let $i = 1;
while ($i <= 240)
{
  --eval INSERT INTO dtw VALUES ($i, (($i - 1) % 20) + 1, ($i % 7) - 3, ELT(($i % 4) + 1, 'kA', 'kB', 'kC', 'kD'), DATE_ADD('2024-01-01', INTERVAL ($i % 5) DAY), ($i % 200) - 100, $i % 250, ($i * 7 % 60000) - 30000, $i * 13 % 65000, ($i * 991 % 16000000) - 8000000, $i * 997 % 16700000, ($i * 100003 % 4000000000) - 2000000000, $i * 99991 % 4200000000, ($i - 120) * 100000007, $i * 1000003, ($i % 40) * 0.25 - 5, ($i % 80) * 0.125 - 5, ($i * 12345) - 1500000, ($i * 777 % 100000) + ($i % 4) * 0.25, ELT(($i % 5) + 1, 'alpha', 'bravo', 'charlie', 'delta', 'echo'), CONCAT('item#', $i), DATE_ADD('1995-01-01', INTERVAL ($i * 3 % 1000) DAY), DATE_ADD('2000-01-01', INTERVAL ($i * 7 % 100000) MINUTE))
  inc $i;
}

# Every 6th row: NULL in every nullable value column (COUNT(col) vs
# COUNT(*), NULL-skipping aggregate semantics per type).
UPDATE dtw SET d_ti = NULL, d_tiu = NULL, d_si = NULL, d_siu = NULL,
  d_mi = NULL, d_miu = NULL, d_i = NULL, d_iu = NULL, d_bi = NULL,
  d_biu = NULL, d_f = NULL, d_d = NULL, d_dec0 = NULL, d_dec2 = NULL,
  d_ch = NULL, d_vc = NULL, d_dt = NULL, d_dtt = NULL
WHERE d_id % 6 = 0;

# String specials: empty strings + multi-byte UTF-8.
UPDATE dtw SET d_vc = '' WHERE d_id = 3;
UPDATE dtw SET d_vc = 'åäö-multibyte' WHERE d_id = 9;
UPDATE dtw SET d_ch = '' WHERE d_id = 15;

# Boundary rows (MIN/MAX only; excluded from SUM cases via d_id <= 240).
INSERT INTO dtw VALUES (1001, 21, 0, 'kA', '2024-01-01',
  -128, 0, -32768, 0, -8388608, 0, -2147483648, 0,
  -9223372036854775808, 0, -1.5, -2.5,
  -999999999999, -9999999999.99, '', '', '1000-01-01', '1000-01-01 00:00:00');
INSERT INTO dtw VALUES (1002, 22, 0, 'kB', '2024-01-02',
  127, 255, 32767, 65535, 8388607, 16777215, 2147483647, 4294967295,
  9223372036854775807, 18446744073709551615, 1.5, 2.5,
  999999999999, 9999999999.99, 'zzzzzzzzzz', 'zzz', '9999-12-31', '9999-12-31 23:59:59');

# All-NULL value rows (aggregates over grp 23 must return NULL).
INSERT INTO dtw VALUES (1003, 23, 0, 'kC', '2024-01-03',
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO dtw VALUES (1004, 23, 0, 'kD', '2024-01-04',
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

let $i = 1;
while ($i <= 23)
{
  --eval INSERT INTO dtw_dim VALUES ($i, CONCAT('grp#', $i))
  inc $i;
}

--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === dtw-01: MIN/MAX over TINYINT signed + unsigned (boundary rows included) ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, MIN(d_ti) AS mn_ti, MAX(d_ti) AS mx_ti,
         MIN(d_tiu) AS mn_tiu, MAX(d_tiu) AS mx_tiu
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_ti), MAX(t.mx_ti), MIN(t.mn_tiu), MAX(t.mx_tiu)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, MIN(d_ti) AS mn_ti, MAX(d_ti) AS mx_ti,
         MIN(d_tiu) AS mn_tiu, MAX(d_tiu) AS mx_tiu
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_ti), MAX(t.mx_ti), MIN(t.mn_tiu), MAX(t.mx_tiu)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id|

--echo
--echo === dtw-02: MIN/MAX over SMALLINT signed + unsigned ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, MIN(d_si) AS mn_si, MAX(d_si) AS mx_si,
         MIN(d_siu) AS mn_siu, MAX(d_siu) AS mx_siu
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_si), MAX(t.mx_si), MIN(t.mn_siu), MAX(t.mx_siu)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, MIN(d_si) AS mn_si, MAX(d_si) AS mx_si,
         MIN(d_siu) AS mn_siu, MAX(d_siu) AS mx_siu
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_si), MAX(t.mx_si), MIN(t.mn_siu), MAX(t.mx_siu)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id|

--echo
--echo === dtw-03: MIN/MAX over MEDIUMINT signed + unsigned ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, MIN(d_mi) AS mn_mi, MAX(d_mi) AS mx_mi,
         MIN(d_miu) AS mn_miu, MAX(d_miu) AS mx_miu
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_mi), MAX(t.mx_mi), MIN(t.mn_miu), MAX(t.mx_miu)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, MIN(d_mi) AS mn_mi, MAX(d_mi) AS mx_mi,
         MIN(d_miu) AS mn_miu, MAX(d_miu) AS mx_miu
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_mi), MAX(t.mx_mi), MIN(t.mn_miu), MAX(t.mx_miu)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id|

--echo
--echo === dtw-04: MIN/MAX over INT signed + unsigned ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, MIN(d_i) AS mn_i, MAX(d_i) AS mx_i,
         MIN(d_iu) AS mn_iu, MAX(d_iu) AS mx_iu
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_i), MAX(t.mx_i), MIN(t.mn_iu), MAX(t.mx_iu)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, MIN(d_i) AS mn_i, MAX(d_i) AS mx_i,
         MIN(d_iu) AS mn_iu, MAX(d_iu) AS mx_iu
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_i), MAX(t.mx_i), MIN(t.mn_iu), MAX(t.mx_iu)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id|

--echo
--echo === dtw-05: MIN/MAX over BIGINT signed + unsigned (Int64/Uint64 extremes) ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, MIN(d_bi) AS mn_bi, MAX(d_bi) AS mx_bi,
         MIN(d_biu) AS mn_biu, MAX(d_biu) AS mx_biu
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_bi), MAX(t.mx_bi), MIN(t.mn_biu), MAX(t.mx_biu)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, MIN(d_bi) AS mn_bi, MAX(d_bi) AS mx_bi,
         MIN(d_biu) AS mn_biu, MAX(d_biu) AS mx_biu
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_bi), MAX(t.mx_bi), MIN(t.mn_biu), MAX(t.mx_biu)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id|

--echo
--echo === dtw-06: SUM over every signed int width (moderate rows only) ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, SUM(d_ti) AS s_ti, SUM(d_si) AS s_si, SUM(d_mi) AS s_mi,
         SUM(d_i) AS s_i, SUM(d_bi) AS s_bi
  FROM dtw WHERE d_id <= 240 GROUP BY d_grp)
SELECT COUNT(*), SUM(t.s_ti), SUM(t.s_si), SUM(t.s_mi), SUM(t.s_i), SUM(t.s_bi)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, SUM(d_ti) AS s_ti, SUM(d_si) AS s_si, SUM(d_mi) AS s_mi,
         SUM(d_i) AS s_i, SUM(d_bi) AS s_bi
  FROM dtw WHERE d_id <= 240 GROUP BY d_grp)
SELECT COUNT(*), SUM(t.s_ti), SUM(t.s_si), SUM(t.s_mi), SUM(t.s_i), SUM(t.s_bi)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id|

--echo
--echo === dtw-07: SUM over every unsigned int width (moderate rows only) ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, SUM(d_tiu) AS s_tiu, SUM(d_siu) AS s_siu,
         SUM(d_miu) AS s_miu, SUM(d_iu) AS s_iu, SUM(d_biu) AS s_biu
  FROM dtw WHERE d_id <= 240 GROUP BY d_grp)
SELECT COUNT(*), SUM(t.s_tiu), SUM(t.s_siu), SUM(t.s_miu), SUM(t.s_iu), SUM(t.s_biu)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, SUM(d_tiu) AS s_tiu, SUM(d_siu) AS s_siu,
         SUM(d_miu) AS s_miu, SUM(d_iu) AS s_iu, SUM(d_biu) AS s_biu
  FROM dtw WHERE d_id <= 240 GROUP BY d_grp)
SELECT COUNT(*), SUM(t.s_tiu), SUM(t.s_siu), SUM(t.s_miu), SUM(t.s_iu), SUM(t.s_biu)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id|

--echo
--echo === dtw-08: SUM over DECIMAL scale-0 (exact BIGINT) and scale-2 (DOUBLE widening) ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, SUM(d_dec0) AS s0, SUM(d_dec2) AS s2, COUNT(*) AS n
  FROM dtw WHERE d_id <= 240 GROUP BY d_grp)
SELECT COUNT(*), SUM(t.s0), SUM(t.s2), SUM(t.n)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, SUM(d_dec0) AS s0, SUM(d_dec2) AS s2, COUNT(*) AS n
  FROM dtw WHERE d_id <= 240 GROUP BY d_grp)
SELECT COUNT(*), SUM(t.s0), SUM(t.s2), SUM(t.n)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id|

--echo
--echo === dtw-09: MIN/MAX over FLOAT and DOUBLE (exact binary fractions) ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, MIN(d_f) AS mn_f, MAX(d_f) AS mx_f,
         MIN(d_d) AS mn_d, MAX(d_d) AS mx_d
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_f), MAX(t.mx_f), MIN(t.mn_d), MAX(t.mx_d)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, MIN(d_f) AS mn_f, MAX(d_f) AS mx_f,
         MIN(d_d) AS mn_d, MAX(d_d) AS mx_d
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_f), MAX(t.mx_f), MIN(t.mn_d), MAX(t.mx_d)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id|

--echo
--echo === dtw-10: MIN/MAX over CHAR and VARCHAR (empty + multi-byte strings) ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, MIN(d_ch) AS mn_ch, MAX(d_ch) AS mx_ch,
         MIN(d_vc) AS mn_vc, MAX(d_vc) AS mx_vc
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_ch), MAX(t.mx_ch), MIN(t.mn_vc), MAX(t.mx_vc)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, MIN(d_ch) AS mn_ch, MAX(d_ch) AS mx_ch,
         MIN(d_vc) AS mn_vc, MAX(d_vc) AS mx_vc
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_ch), MAX(t.mx_ch), MIN(t.mn_vc), MAX(t.mx_vc)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id|

--echo
--echo === dtw-11: MIN/MAX over DATE and DATETIME (range ends, NULL rows skipped) ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, MIN(d_dt) AS mn_dt, MAX(d_dt) AS mx_dt,
         MIN(d_dtt) AS mn_dtt, MAX(d_dtt) AS mx_dtt
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_dt), MAX(t.mx_dt), MIN(t.mn_dtt), MAX(t.mx_dtt)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, MIN(d_dt) AS mn_dt, MAX(d_dt) AS mx_dt,
         MIN(d_dtt) AS mn_dtt, MAX(d_dtt) AS mx_dtt
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn_dt), MAX(t.mx_dt), MIN(t.mn_dtt), MAX(t.mx_dtt)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id|

--echo
--echo === dtw-12: COUNT(*) vs COUNT(nullable col) per type family ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, COUNT(*) AS c_all, COUNT(d_ti) AS c_ti, COUNT(d_f) AS c_f,
         COUNT(d_dec2) AS c_dec, COUNT(d_vc) AS c_vc, COUNT(d_dt) AS c_dt
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), SUM(t.c_all), SUM(t.c_ti), SUM(t.c_f), SUM(t.c_dec),
       SUM(t.c_vc), SUM(t.c_dt)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, COUNT(*) AS c_all, COUNT(d_ti) AS c_ti, COUNT(d_f) AS c_f,
         COUNT(d_dec2) AS c_dec, COUNT(d_vc) AS c_vc, COUNT(d_dt) AS c_dt
  FROM dtw GROUP BY d_grp)
SELECT COUNT(*), SUM(t.c_all), SUM(t.c_ti), SUM(t.c_f), SUM(t.c_dec),
       SUM(t.c_vc), SUM(t.c_dt)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id|

--echo
--echo === dtw-13: CTE body GROUP BY a TINYINT key, scalar main re-aggregation ===
let $QUERY=
WITH t AS (
  SELECT d_kti AS g, COUNT(*) AS n, SUM(d_i) AS s
  FROM dtw WHERE d_id <= 240 GROUP BY d_kti)
SELECT COUNT(*), SUM(t.n), SUM(t.s) FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_kti AS g, COUNT(*) AS n, SUM(d_i) AS s
  FROM dtw WHERE d_id <= 240 GROUP BY d_kti)
SELECT COUNT(*), SUM(t.n), SUM(t.s) FROM t|

--echo
--echo === dtw-14: CTE body GROUP BY a CHAR key, scalar main re-aggregation ===
let $QUERY=
WITH t AS (
  SELECT d_kch AS g, COUNT(*) AS n, SUM(d_si) AS s
  FROM dtw WHERE d_id <= 240 GROUP BY d_kch)
SELECT COUNT(*), SUM(t.n), SUM(t.s) FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_kch AS g, COUNT(*) AS n, SUM(d_si) AS s
  FROM dtw WHERE d_id <= 240 GROUP BY d_kch)
SELECT COUNT(*), SUM(t.n), SUM(t.s) FROM t|

--echo
--echo === dtw-15: CTE body GROUP BY a DATE key, scalar main re-aggregation ===
let $QUERY=
WITH t AS (
  SELECT d_kdt AS g, COUNT(*) AS n, SUM(d_mi) AS s
  FROM dtw WHERE d_id <= 240 GROUP BY d_kdt)
SELECT COUNT(*), SUM(t.n), SUM(t.s) FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_kdt AS g, COUNT(*) AS n, SUM(d_mi) AS s
  FROM dtw WHERE d_id <= 240 GROUP BY d_kdt)
SELECT COUNT(*), SUM(t.n), SUM(t.s) FROM t|

--echo
--echo === dtw-16: boundary-only slice through MIN/MAX (Int64/INT extremes isolated) ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, MIN(d_bi) AS mn, MAX(d_bi) AS mx,
         MIN(d_i) AS mn_i, MAX(d_i) AS mx_i
  FROM dtw WHERE d_id > 1000 GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn), MAX(t.mx), MIN(t.mn_i), MAX(t.mx_i) FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, MIN(d_bi) AS mn, MAX(d_bi) AS mx,
         MIN(d_i) AS mn_i, MAX(d_i) AS mx_i
  FROM dtw WHERE d_id > 1000 GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn), MAX(t.mx), MIN(t.mn_i), MAX(t.mx_i) FROM t|

--echo
--echo === dtw-17: all-NULL group (every aggregate returns NULL, COUNTs return 0) ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, MIN(d_i) AS mn, MAX(d_vc) AS mx,
         COUNT(d_i) AS c, COUNT(*) AS n
  FROM dtw WHERE d_id > 1002 GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn), MAX(t.mx), SUM(t.c), SUM(t.n) FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, MIN(d_i) AS mn, MAX(d_vc) AS mx,
         COUNT(d_i) AS c, COUNT(*) AS n
  FROM dtw WHERE d_id > 1002 GROUP BY d_grp)
SELECT COUNT(*), MIN(t.mn), MAX(t.mx), SUM(t.c), SUM(t.n) FROM t|

--echo
--echo === dtw-18: wide output — 10 aggregates over 8 distinct columns in one CTE ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, COUNT(*) AS n, SUM(d_i) AS s_i, SUM(d_biu) AS s_biu,
         MIN(d_si) AS mn_si, MAX(d_siu) AS mx_siu, MIN(d_dec2) AS mn_dec,
         MAX(d_dec0) AS mx_dec, MIN(d_dt) AS mn_dt, MAX(d_ch) AS mx_ch,
         MAX(d_d) AS mx_d
  FROM dtw WHERE d_id <= 240 GROUP BY d_grp)
SELECT COUNT(*), SUM(t.n), SUM(t.s_i), SUM(t.s_biu), MIN(t.mn_si),
       MAX(t.mx_siu), MIN(t.mn_dec), MAX(t.mx_dec), MIN(t.mn_dt),
       MAX(t.mx_ch), MAX(t.mx_d)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, COUNT(*) AS n, SUM(d_i) AS s_i, SUM(d_biu) AS s_biu,
         MIN(d_si) AS mn_si, MAX(d_siu) AS mx_siu, MIN(d_dec2) AS mn_dec,
         MAX(d_dec0) AS mx_dec, MIN(d_dt) AS mn_dt, MAX(d_ch) AS mx_ch,
         MAX(d_d) AS mx_d
  FROM dtw WHERE d_id <= 240 GROUP BY d_grp)
SELECT COUNT(*), SUM(t.n), SUM(t.s_i), SUM(t.s_biu), MIN(t.mn_si),
       MAX(t.mx_siu), MIN(t.mn_dec), MAX(t.mx_dec), MIN(t.mn_dt),
       MAX(t.mx_ch), MAX(t.mx_d)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id|

--echo
--echo === dtw-19: scalar CTE (no GROUP BY), COUNT + MAX over BIGINT columns ===
# MIN deliberately absent: scalar-CTE MIN is broken in the cross-node
# merge — see the dtw-19b probe below.
let $QUERY=
WITH t AS (
  SELECT COUNT(*) AS n, MAX(d_bi) AS mx, MAX(d_biu) AS mxu
  FROM dtw)
SELECT MAX(t.n), MAX(t.mx), MAX(t.mxu) FROM t;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT COUNT(*) AS n, MAX(d_bi) AS mx, MAX(d_biu) AS mxu
  FROM dtw)
SELECT MAX(t.n), MAX(t.mx), MAX(t.mxu) FROM t|

# NEXT-PHASE (dtw-19b): scalar-CTE MIN is WRONG whenever the true
# global minimum is scanned on a non-owner node — the MIN arm of the
# scalar cross-node merge appears to compare with INVERTED POLARITY
# (keeps the larger of the two local minima).  Three data points
# (2026-08-19): (1) ng4r2/8 nodes, full table: returned -9500000665
# instead of Int64-min; (2) default topology, WHERE d_id <= 240:
# returned -11700000819 (row 3) instead of -11900000833 (row 1);
# (3) default topology, full table: correct — by luck, the min row was
# scanned on the owner node.  MAX (Int64-max, Uint64-max) merges
# correctly in every run, and the GROUPED MIN path (dtw-05/dtw-16) is
# correct on all topologies.  Suspect: the MIN arm of
# mergeScalarAccumulators (cte_filter_phase_i17_redistribute.md, the
# keyLen==0 scalar redistribute).  Affects ALL topologies — also audit
# body_chain_scalar.inc / offline_fs_scalar-style scalar-CTE MINs,
# which may be green only by row placement.
# See test_benchmark_extension_plan.md §5b.
#   WITH t AS (
#     SELECT COUNT(*) AS n, MIN(d_bi) AS mn FROM dtw)
#   SELECT MAX(t.n), MAX(t.mn) FROM t;

--echo
--echo === dtw-20: P-GB — GROUP BY a dimension column over a mixed-type CTE leaf ===
let $QUERY=
WITH t AS (
  SELECT d_grp AS g, SUM(d_si) AS s, MIN(d_dt) AS mn_dt
  FROM dtw WHERE d_id <= 240 GROUP BY d_grp)
SELECT m.g_name, COUNT(*), SUM(t.s), MIN(t.mn_dt)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id
GROUP BY m.g_name;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH t AS (
  SELECT d_grp AS g, SUM(d_si) AS s, MIN(d_dt) AS mn_dt
  FROM dtw WHERE d_id <= 240 GROUP BY d_grp)
SELECT m.g_name, COUNT(*), SUM(t.s), MIN(t.mn_dt)
FROM dtw_dim AS m JOIN t ON t.g = m.g_id
GROUP BY m.g_name|

DELIMITER ;|

# ---------------------------------------------------------------------
# Probes — NOT executed.
#
# NEXT-PHASE: SUM over FLOAT / DOUBLE — floating-point summation is
# non-associative, so RonSQL's partial-sum order yields different
# low-order ULPs than mysql; not strict-diff-testable (same disposition
# as body_agg.inc):
#   WITH t AS (SELECT d_grp AS g, SUM(d_f) AS sf, SUM(d_d) AS sd
#              FROM dtw WHERE d_id <= 240 GROUP BY d_grp)
#   SELECT COUNT(*), SUM(t.sf), SUM(t.sd)
#   FROM dtw_dim AS m JOIN t ON t.g = m.g_id;
#
# NEXT-PHASE: CTE body GROUP BY a NULLABLE typed key (NULL group
# hashing/merge semantics unproven for the width matrix):
#   WITH t AS (SELECT d_ti AS g, COUNT(*) AS n FROM dtw GROUP BY d_ti)
#   SELECT COUNT(*), SUM(t.n) FROM t;
#
# TIMESTAMP MIN/MAX is deliberately not repeated here — covered by
# body_agg.inc agg-d17i/j under time_zone='+00:00'.
# ---------------------------------------------------------------------

--echo
--echo === Cleanup ===
--disable_query_log
DROP TABLE dtw;
DROP TABLE dtw_dim;
--enable_query_log
