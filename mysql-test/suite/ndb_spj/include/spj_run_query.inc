#
# Run $spj_q once without stopping the test if it fails.
#
# A query that fails while its rows are read cannot be run as `$spj_q`:
# NDB errors arrive after the result set metadata, mysql_store_result()
# fails and mysqltest dies with "didn't return a result set" even under
# --disable_abort_on_error. The query is therefore run as
# SELECT ... INTO, which returns no result set, so a failure is an
# ordinary statement error.
#
# In:  $spj_q      a SELECT returning one row of two columns
# Out: $spj_errno  0 if the query succeeded, else the MySQL error number
#      $spj_res    on success the row as `$spj_q` would give it (columns
#                  separated by a tab), empty on failure
#      $spj_w      the first message of SHOW WARNINGS
# The query log is enabled on return.
#
--disable_query_log
--disable_result_log
--disable_abort_on_error
SET @spj_cnt = NULL, @spj_chk = NULL;
--eval $spj_q INTO @spj_cnt, @spj_chk
--let $spj_errno = $mysql_errno
--let $spj_w = query_get_value(SHOW WARNINGS, Message, 1)
--enable_abort_on_error
--enable_result_log
--let $spj_res =
if (!$spj_errno)
{
  --let $spj_res = `SELECT @spj_cnt, @spj_chk`
}
--enable_query_log
