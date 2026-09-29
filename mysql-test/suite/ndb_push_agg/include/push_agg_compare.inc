# push_agg_compare.inc -- compare one aggregate query with and without
# pushdown aggregation.
#
# Input:
#   $pac_query  one-line SELECT without double quotes; its output columns
#               need distinct names (it is counted as a derived table)
#   $pac_label  short name echoed with the verdict
#
# Runs $pac_query with ndb_pushdown_aggregate OFF and ON and fails the test
# unless both return the same rows in any order. Then echoes whether EXPLAIN
# shows the ON run as pushed and how many rows the query returns.
# Leaves ndb_pushdown_aggregate ON.

--disable_query_log
--let $pac_off = $MYSQLTEST_VARDIR/tmp/push_agg_compare_off.txt
--let $pac_on = $MYSQLTEST_VARDIR/tmp/push_agg_compare_on.txt
SET ndb_pushdown_aggregate = ON;
--let $pac_extra = query_get_value("EXPLAIN $pac_query", Extra, 1)
--let $pac_pushed = `SELECT LOCATE('Using pushed aggregation', '$pac_extra') > 0`
SET ndb_pushdown_aggregate = OFF;
--let $pac_rows = `SELECT COUNT(*) FROM ($pac_query) AS pac_rows`
--sorted_result
--output $pac_off
--eval $pac_query
SET ndb_pushdown_aggregate = ON;
--sorted_result
--output $pac_on
--eval $pac_query
--diff_files $pac_off $pac_on
--remove_file $pac_off
--remove_file $pac_on
--enable_query_log
--echo # $pac_label: pushed=$pac_pushed rows=$pac_rows
