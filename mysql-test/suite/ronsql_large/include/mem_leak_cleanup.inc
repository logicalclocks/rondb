# mem_leak_cleanup.inc -- remove the harness files from $MYSQL_TMP_DIR
# (mtr fails a passing test that leaves files there) and the baseline
# table.  The report file in $MYSQLTEST_VARDIR/log is kept.
--disable_query_log
--disable_warnings
--eval DROP TEMPORARY TABLE IF EXISTS mlk_baseline
--eval DROP TEMPORARY TABLE IF EXISTS mlk_shapes
--enable_warnings
--enable_query_log
--perl
use strict;
use warnings;
unlink(glob("$ENV{MYSQL_TMP_DIR}/mlk_*"));
EOF
