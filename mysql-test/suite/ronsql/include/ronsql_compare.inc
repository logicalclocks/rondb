# This include-file, or subroutine, executes a query with both mysql client and
# ronsql_cli, then compares the results.
if ($QUERY!='')
{
  --let $QUERY_PARAMS = -e '$QUERY'
}
if ($QUERY=='')
{
  if ($QUERY_FILE=='')
  {
    --die Must define either QUERY or QUERY_FILE variable
  }
  --let $QUERY_PARAMS = < '$QUERY_FILE'
}
--let $QUERY_PARAMS = -D test $QUERY_PARAMS
--echo == Query ==
--echo $QUERY
--exec $MYSQL $QUERY_PARAMS > $MYSQL_TMP_DIR/mysql.out
if ($canonicalization_script) {
--exec sed -ri '$canonicalization_script' $MYSQL_TMP_DIR/mysql.out
}
--exec (head -n 1 $MYSQL_TMP_DIR/mysql.out && tail -n +2 $MYSQL_TMP_DIR/mysql.out | sort) > $MYSQL_TMP_DIR/mysql.out.sorted
if ($show_all_results) {
--echo
--echo == MySQL result ==
--cat_file $MYSQL_TMP_DIR/mysql.out.sorted
}
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING $QUERY_PARAMS > $MYSQL_TMP_DIR/ronsql.out
if ($canonicalization_script) {
--exec sed -ri '$canonicalization_script' $MYSQL_TMP_DIR/ronsql.out
}
--exec (head -n 1 $MYSQL_TMP_DIR/ronsql.out && tail -n +2 $MYSQL_TMP_DIR/ronsql.out | sort) > $MYSQL_TMP_DIR/ronsql.out.sorted
if ($show_all_results) {
--echo
--echo == RonSQL result ==
--cat_file $MYSQL_TMP_DIR/ronsql.out.sorted
}
if (!$show_all_results) {
--echo
--exec echo Number of output lines, including header: `grep -Fc '' '$MYSQL_TMP_DIR/mysql.out.sorted'`
}
--echo
--echo == Diff ==
# todo The "|| true" at the end makes this test succeed no matter what the
# results are. Remove it when possible.
--exec diff -u --label mysql.out $MYSQL_TMP_DIR/mysql.out.sorted --label ronsql.out $MYSQL_TMP_DIR/ronsql.out.sorted || true
--remove_file $MYSQL_TMP_DIR/mysql.out
--remove_file $MYSQL_TMP_DIR/mysql.out.sorted
--remove_file $MYSQL_TMP_DIR/ronsql.out
--remove_file $MYSQL_TMP_DIR/ronsql.out.sorted
--echo ================================================================================
--echo
--echo
--echo
if ($QUERY=='')
{
  --remove_file $QUERY_FILE
}
--let $QUERY=
--let $show_all_results=0
--let $canonicalization_script=
