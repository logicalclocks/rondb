# body_key_range.inc — an integer literal outside the domain of the key
# column of a CTE root lookup (J-3 = C-5 in
# storage/ndb/claude_files/ronsql_executor/review_join.md and
# review_cte.md).
#
# When the main-query WHERE binds every GROUP BY column of the CTE in
# FROM with an equality, the root is a lookupCte probe keyed on the
# constants.  emit_root_op built an integer key from the literal cast to
# Int32, Uint32 or Uint64, so a literal outside the key column's domain
# wrapped into another key: k = 4294967297 on an INT key probed key 1 and
# returned that group, where MySQL returns no row, and k = -1 on an INT
# UNSIGNED or BIGINT UNSIGNED key probed the largest value.  On TINYINT,
# SMALLINT and MEDIUMINT keys a literal just outside the domain failed
# the query builder's range check instead ("Failed to create lookupCte
# root.").
#
# The keys are now encoded as on the real-table root (encode_constant),
# which refuses such a literal with the permanent error that every other
# RonSQL comparison gives for it.  Folding it to FALSE, as MySQL does, is
# F-12 and is not done here, so the out-of-range cases are rejection
# probes and only the in-range cases are compared with mysqld.  The typed
# key operand also sent a MEDIUMINT key as 4 bytes where the group key
# has 3, so kr-9 and kr-10 check the MEDIUMINT key encoding too.
#
# RonSQL integer literals stop at 9223372036854775807: the BIGINT
# boundaries tested are -9223372036854775807 and 9223372036854775807, and
# no literal is out of range of a BIGINT key.
#
# Data: krange rows 1..5 carry, in every key column, the minimum of the
# column's type (row 1), 1 (rows 2 and 3) and the maximum (rows 4 and 5),
# except that bi is -9223372036854775807 in row 1, and bu is
# 9223372036854775807 in row 4 and 18446744073709551615 in row 5 (the key
# BIGINT UNSIGNED -1 wrapped to).
#
# Case map (kr-N):
#   1-4    in-range controls on an INT key: hit, miss, aggregate hit,
#          aggregate miss (COUNT 0, SUM NULL)
#   5-14   both ends of each integer type's domain still match: two-column
#          keys pairing the signed and the unsigned type of one width
#   P1-P12 rejection: a literal just above and just below the domain of
#          TINYINT, SMALLINT and MEDIUMINT, signed and unsigned
#   P13-15 rejection: INT 2147483648, -2147483649 and 4294967297 (before:
#          the groups -2147483648, 2147483647 and 1)
#   P16-17 rejection: INT UNSIGNED 4294967296 and -1 (before: the groups
#          0 and 4294967295)
#   P18    rejection: BIGINT UNSIGNED -1 (before: the group
#          18446744073709551615)
#   P19    rejection: aggregate main query over an out-of-range INT key
#   P20-21 rejection: two-column key with one literal out of range, in
#          either column (before: the group (1, 1))

--source include/have_ndb.inc
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings
--let $suppress_ronsql_cli=yes
--let $strict_diff=yes

--disable_query_log
--disable_warnings
CREATE TABLE krange (
  id INT NOT NULL,
  ti TINYINT NOT NULL,
  tu TINYINT UNSIGNED NOT NULL,
  si SMALLINT NOT NULL,
  su SMALLINT UNSIGNED NOT NULL,
  mi MEDIUMINT NOT NULL,
  mu MEDIUMINT UNSIGNED NOT NULL,
  ii INT NOT NULL,
  iu INT UNSIGNED NOT NULL,
  bi BIGINT NOT NULL,
  bu BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY USING HASH (id)
) ENGINE=NDB;
INSERT INTO krange VALUES
  (1, -128, 0, -32768, 0, -8388608, 0, -2147483648, 0,
   -9223372036854775807, 0),
  (2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
  (3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
  (4, 127, 255, 32767, 65535, 8388607, 16777215, 2147483647, 4294967295,
   9223372036854775807, 9223372036854775807),
  (5, 127, 255, 32767, 65535, 8388607, 16777215, 2147483647, 4294967295,
   9223372036854775807, 18446744073709551615);
--enable_warnings
--enable_query_log

DELIMITER |;

--echo
--echo === kr-1: INT key, in range, hit ===
let $QUERY=
WITH g AS (SELECT ii AS k, COUNT(*) AS n
           FROM krange GROUP BY ii)
SELECT k, n FROM g WHERE k = 1;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT ii AS k, COUNT(*) AS n
           FROM krange GROUP BY ii)
SELECT k, n FROM g WHERE k = 1|

--echo
--echo === kr-2: INT key, in range, miss ===
let $QUERY=
WITH g AS (SELECT ii AS k, COUNT(*) AS n
           FROM krange GROUP BY ii)
SELECT k, n FROM g WHERE k = 2;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT ii AS k, COUNT(*) AS n
           FROM krange GROUP BY ii)
SELECT k, n FROM g WHERE k = 2|

--echo
--echo === kr-3: INT key, in range, aggregate main, hit ===
let $QUERY=
WITH g AS (SELECT ii AS k, COUNT(*) AS n
           FROM krange GROUP BY ii)
SELECT COUNT(*) AS c, SUM(n) AS s FROM g WHERE k = 1;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT ii AS k, COUNT(*) AS n
           FROM krange GROUP BY ii)
SELECT COUNT(*) AS c, SUM(n) AS s FROM g WHERE k = 1|

--echo
--echo === kr-4: INT key, in range, aggregate main, miss ===
let $QUERY=
WITH g AS (SELECT ii AS k, COUNT(*) AS n
           FROM krange GROUP BY ii)
SELECT COUNT(*) AS c, SUM(n) AS s FROM g WHERE k = 2;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT ii AS k, COUNT(*) AS n
           FROM krange GROUP BY ii)
SELECT COUNT(*) AS c, SUM(n) AS s FROM g WHERE k = 2|

--echo
--echo === kr-5: TINYINT / TINYINT UNSIGNED key, maximum ===
let $QUERY=
WITH g AS (SELECT ti AS sk, tu AS uk, COUNT(*) AS n
           FROM krange GROUP BY ti, tu)
SELECT sk, uk, n FROM g WHERE sk = 127 AND uk = 255;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT ti AS sk, tu AS uk, COUNT(*) AS n
           FROM krange GROUP BY ti, tu)
SELECT sk, uk, n FROM g WHERE sk = 127 AND uk = 255|

--echo
--echo === kr-6: TINYINT / TINYINT UNSIGNED key, minimum ===
let $QUERY=
WITH g AS (SELECT ti AS sk, tu AS uk, COUNT(*) AS n
           FROM krange GROUP BY ti, tu)
SELECT sk, uk, n FROM g WHERE sk = -128 AND uk = 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT ti AS sk, tu AS uk, COUNT(*) AS n
           FROM krange GROUP BY ti, tu)
SELECT sk, uk, n FROM g WHERE sk = -128 AND uk = 0|

--echo
--echo === kr-7: SMALLINT / SMALLINT UNSIGNED key, maximum ===
let $QUERY=
WITH g AS (SELECT si AS sk, su AS uk, COUNT(*) AS n
           FROM krange GROUP BY si, su)
SELECT sk, uk, n FROM g WHERE sk = 32767 AND uk = 65535;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT si AS sk, su AS uk, COUNT(*) AS n
           FROM krange GROUP BY si, su)
SELECT sk, uk, n FROM g WHERE sk = 32767 AND uk = 65535|

--echo
--echo === kr-8: SMALLINT / SMALLINT UNSIGNED key, minimum ===
let $QUERY=
WITH g AS (SELECT si AS sk, su AS uk, COUNT(*) AS n
           FROM krange GROUP BY si, su)
SELECT sk, uk, n FROM g WHERE sk = -32768 AND uk = 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT si AS sk, su AS uk, COUNT(*) AS n
           FROM krange GROUP BY si, su)
SELECT sk, uk, n FROM g WHERE sk = -32768 AND uk = 0|

--echo
--echo === kr-9: MEDIUMINT / MEDIUMINT UNSIGNED key, maximum ===
let $QUERY=
WITH g AS (SELECT mi AS sk, mu AS uk, COUNT(*) AS n
           FROM krange GROUP BY mi, mu)
SELECT sk, uk, n FROM g WHERE sk = 8388607 AND uk = 16777215;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT mi AS sk, mu AS uk, COUNT(*) AS n
           FROM krange GROUP BY mi, mu)
SELECT sk, uk, n FROM g WHERE sk = 8388607 AND uk = 16777215|

--echo
--echo === kr-10: MEDIUMINT / MEDIUMINT UNSIGNED key, minimum ===
let $QUERY=
WITH g AS (SELECT mi AS sk, mu AS uk, COUNT(*) AS n
           FROM krange GROUP BY mi, mu)
SELECT sk, uk, n FROM g WHERE sk = -8388608 AND uk = 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT mi AS sk, mu AS uk, COUNT(*) AS n
           FROM krange GROUP BY mi, mu)
SELECT sk, uk, n FROM g WHERE sk = -8388608 AND uk = 0|

--echo
--echo === kr-11: INT / INT UNSIGNED key, maximum ===
let $QUERY=
WITH g AS (SELECT ii AS sk, iu AS uk, COUNT(*) AS n
           FROM krange GROUP BY ii, iu)
SELECT sk, uk, n FROM g WHERE sk = 2147483647 AND uk = 4294967295;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT ii AS sk, iu AS uk, COUNT(*) AS n
           FROM krange GROUP BY ii, iu)
SELECT sk, uk, n FROM g WHERE sk = 2147483647 AND uk = 4294967295|

--echo
--echo === kr-12: INT / INT UNSIGNED key, minimum ===
let $QUERY=
WITH g AS (SELECT ii AS sk, iu AS uk, COUNT(*) AS n
           FROM krange GROUP BY ii, iu)
SELECT sk, uk, n FROM g WHERE sk = -2147483648 AND uk = 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT ii AS sk, iu AS uk, COUNT(*) AS n
           FROM krange GROUP BY ii, iu)
SELECT sk, uk, n FROM g WHERE sk = -2147483648 AND uk = 0|

--echo
--echo === kr-13: BIGINT / BIGINT UNSIGNED key, largest literal ===
let $QUERY=
WITH g AS (SELECT bi AS sk, bu AS uk, COUNT(*) AS n
           FROM krange GROUP BY bi, bu)
SELECT sk, uk, n FROM g
WHERE sk = 9223372036854775807 AND uk = 9223372036854775807;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT bi AS sk, bu AS uk, COUNT(*) AS n
           FROM krange GROUP BY bi, bu)
SELECT sk, uk, n FROM g
WHERE sk = 9223372036854775807 AND uk = 9223372036854775807|

--echo
--echo === kr-14: BIGINT / BIGINT UNSIGNED key, smallest literal ===
let $QUERY=
WITH g AS (SELECT bi AS sk, bu AS uk, COUNT(*) AS n
           FROM krange GROUP BY bi, bu)
SELECT sk, uk, n FROM g
WHERE sk = -9223372036854775807 AND uk = 0;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
WITH g AS (SELECT bi AS sk, bu AS uk, COUNT(*) AS n
           FROM krange GROUP BY bi, bu)
SELECT sk, uk, n FROM g
WHERE sk = -9223372036854775807 AND uk = 0|

DELIMITER ;|

# Rejection probes.  Each literal lies outside the key column's domain;
# the query must fail once with the permanent out-of-range error
# ("Error handling: RPE"), as the same literal does on a real-table root.

--echo
--echo === kr-P1 (rejection): TINYINT key = 128 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT ti AS k, COUNT(*) AS n FROM krange GROUP BY ti) SELECT k, n FROM g WHERE k = 128;' 2>&1

--echo
--echo === kr-P2 (rejection): TINYINT key = -129 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT ti AS k, COUNT(*) AS n FROM krange GROUP BY ti) SELECT k, n FROM g WHERE k = -129;' 2>&1

--echo
--echo === kr-P3 (rejection): TINYINT UNSIGNED key = 256 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT tu AS k, COUNT(*) AS n FROM krange GROUP BY tu) SELECT k, n FROM g WHERE k = 256;' 2>&1

--echo
--echo === kr-P4 (rejection): TINYINT UNSIGNED key = -1 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT tu AS k, COUNT(*) AS n FROM krange GROUP BY tu) SELECT k, n FROM g WHERE k = -1;' 2>&1

--echo
--echo === kr-P5 (rejection): SMALLINT key = 32768 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT si AS k, COUNT(*) AS n FROM krange GROUP BY si) SELECT k, n FROM g WHERE k = 32768;' 2>&1

--echo
--echo === kr-P6 (rejection): SMALLINT key = -32769 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT si AS k, COUNT(*) AS n FROM krange GROUP BY si) SELECT k, n FROM g WHERE k = -32769;' 2>&1

--echo
--echo === kr-P7 (rejection): SMALLINT UNSIGNED key = 65536 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT su AS k, COUNT(*) AS n FROM krange GROUP BY su) SELECT k, n FROM g WHERE k = 65536;' 2>&1

--echo
--echo === kr-P8 (rejection): SMALLINT UNSIGNED key = -1 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT su AS k, COUNT(*) AS n FROM krange GROUP BY su) SELECT k, n FROM g WHERE k = -1;' 2>&1

--echo
--echo === kr-P9 (rejection): MEDIUMINT key = 8388608 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT mi AS k, COUNT(*) AS n FROM krange GROUP BY mi) SELECT k, n FROM g WHERE k = 8388608;' 2>&1

--echo
--echo === kr-P10 (rejection): MEDIUMINT key = -8388609 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT mi AS k, COUNT(*) AS n FROM krange GROUP BY mi) SELECT k, n FROM g WHERE k = -8388609;' 2>&1

--echo
--echo === kr-P11 (rejection): MEDIUMINT UNSIGNED key = 16777216 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT mu AS k, COUNT(*) AS n FROM krange GROUP BY mu) SELECT k, n FROM g WHERE k = 16777216;' 2>&1

--echo
--echo === kr-P12 (rejection): MEDIUMINT UNSIGNED key = -1 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT mu AS k, COUNT(*) AS n FROM krange GROUP BY mu) SELECT k, n FROM g WHERE k = -1;' 2>&1

--echo
--echo === kr-P13 (rejection): INT key = 2147483648 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT ii AS k, COUNT(*) AS n FROM krange GROUP BY ii) SELECT k, n FROM g WHERE k = 2147483648;' 2>&1

--echo
--echo === kr-P14 (rejection): INT key = -2147483649 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT ii AS k, COUNT(*) AS n FROM krange GROUP BY ii) SELECT k, n FROM g WHERE k = -2147483649;' 2>&1

--echo
--echo === kr-P15 (rejection): INT key = 4294967297 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT ii AS k, COUNT(*) AS n FROM krange GROUP BY ii) SELECT k, n FROM g WHERE k = 4294967297;' 2>&1

--echo
--echo === kr-P16 (rejection): INT UNSIGNED key = 4294967296 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT iu AS k, COUNT(*) AS n FROM krange GROUP BY iu) SELECT k, n FROM g WHERE k = 4294967296;' 2>&1

--echo
--echo === kr-P17 (rejection): INT UNSIGNED key = -1 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT iu AS k, COUNT(*) AS n FROM krange GROUP BY iu) SELECT k, n FROM g WHERE k = -1;' 2>&1

--echo
--echo === kr-P18 (rejection): BIGINT UNSIGNED key = -1 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT bu AS k, COUNT(*) AS n FROM krange GROUP BY bu) SELECT k, n FROM g WHERE k = -1;' 2>&1

--echo
--echo === kr-P19 (rejection): aggregate main, INT key = 4294967297 ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT ii AS k, COUNT(*) AS n FROM krange GROUP BY ii) SELECT COUNT(*) AS c, SUM(n) AS s FROM g WHERE k = 4294967297;' 2>&1

--echo
--echo === kr-P20 (rejection): two-column key, second literal out of range ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT ii AS sk, iu AS uk, COUNT(*) AS n FROM krange GROUP BY ii, iu) SELECT sk, uk, n FROM g WHERE sk = 1 AND uk = 4294967297;' 2>&1

--echo
--echo === kr-P21 (rejection): two-column key, first literal out of range ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e 'WITH g AS (SELECT ii AS sk, iu AS uk, COUNT(*) AS n FROM krange GROUP BY ii, iu) SELECT sk, uk, n FROM g WHERE sk = 4294967297 AND uk = 1;' 2>&1

--disable_query_log
--disable_warnings
DROP TABLE krange;
--enable_warnings
--enable_query_log
