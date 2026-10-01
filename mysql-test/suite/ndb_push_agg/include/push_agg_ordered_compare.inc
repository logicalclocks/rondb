# push_agg_ordered_compare.inc -- compare one ORDER BY aggregate query with
# and without pushdown aggregation, row order included (F30).
#
# Input:
#   $poc_query  one-line SELECT without double quotes, with an ORDER BY that
#               makes its row order deterministic
#   $poc_row    the EXPLAIN row of the NDB table: 1 for a plain SELECT, 2 for
#               WITH t AS (... FROM <table> ...) SELECT ... FROM t
#   $poc_label  short name echoed with the verdict
#
# Runs $poc_query with ndb_pushdown_aggregate OFF and ON and fails the test
# unless both return the same rows in the same order (no --sorted_result:
# the order is what F30 got wrong).  Then echoes whether EXPLAIN shows the ON
# run as pushed, and prints the rows.  Leaves ndb_pushdown_aggregate ON.

--disable_query_log
--let $poc_off = $MYSQLTEST_VARDIR/tmp/push_agg_ordered_off.txt
--let $poc_on = $MYSQLTEST_VARDIR/tmp/push_agg_ordered_on.txt
SET ndb_pushdown_aggregate = ON;
--let $poc_extra = query_get_value("EXPLAIN $poc_query", Extra, $poc_row)
# Double quotes: Extra shows a pushed condition's string literals in single
# quotes.
--let $poc_pushed = `SELECT LOCATE('Using pushed aggregation', "$poc_extra") > 0`
SET ndb_pushdown_aggregate = OFF;
--output $poc_off
--eval $poc_query
SET ndb_pushdown_aggregate = ON;
--output $poc_on
--eval $poc_query
--diff_files $poc_off $poc_on
--enable_query_log
--echo # $poc_label: pushed=$poc_pushed
--cat_file $poc_off
--remove_file $poc_off
--remove_file $poc_on
