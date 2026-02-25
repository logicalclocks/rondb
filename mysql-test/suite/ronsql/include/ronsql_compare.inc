# This include-file, or subroutine, executes a query with both mysql client and
# ronsql_cli, then compares the results.
--source include/have_util_sed.inc
--source include/have_util_curl.inc
--echo == Query ==
if ($QUERY!='')
{
  --echo $QUERY
  --let $QUERY_PARAMS = -e '$QUERY'
  --exec printf '%s' '$QUERY' > $MYSQL_TMP_DIR/ronsql-query.sql
}
if ($QUERY=='')
{
  if ($QUERY_FILE=='')
  {
    --die Must define either QUERY or QUERY_FILE variable
  }
  --exec cat $QUERY_FILE
  --let $QUERY_PARAMS = < '$QUERY_FILE'
  --exec cat $QUERY_FILE > $MYSQL_TMP_DIR/ronsql-query.sql
}
--exec printf '{"query":"' > $MYSQL_TMP_DIR/ronsql-rdrs-body-1
--exec perl -pe 's/\\/\\\\/g; s/"/\\"/g; s/\x08/\\b/g; s/\f/\\f/g; s/\n/\\n/g; s/\r/\\r/g; s/\t/\\t/g;' $MYSQL_TMP_DIR/ronsql-query.sql > $MYSQL_TMP_DIR/ronsql-rdrs-body-2
--exec printf '","database":"test","outputFormat":"TEXT"}' > $MYSQL_TMP_DIR/ronsql-rdrs-body-3
--exec cat $MYSQL_TMP_DIR/ronsql-rdrs-body-1 $MYSQL_TMP_DIR/ronsql-rdrs-body-2 $MYSQL_TMP_DIR/ronsql-rdrs-body-3 > $MYSQL_TMP_DIR/ronsql-rdrs-body
--let $QUERY_PARAMS = -D test $QUERY_PARAMS

# RonSQL doesn't support time zones yet, and works as if in UTC. MySQL uses
# system time zone, but it seems mtr somehow sets it to +03:00 before starting a
# test case. Until RonSQL and RDRS supports time zones, we will set UTC time
# zone explicitly in each test case where it has an impact. This does not carry
# over to MySQL clients launched with --exec, so we use the --init-command-add
# parameter for that.
--exec $MYSQL --default-character-set=utf8mb4 --init-command-add='SET time_zone = "+00:00";' $QUERY_PARAMS > $MYSQL_TMP_DIR/mysql.out
if ($canonicalization_script)
{
  --exec sed -r '$canonicalization_script' $MYSQL_TMP_DIR/mysql.out > $MYSQL_TMP_DIR/mysql.out.canon
  --move_file $MYSQL_TMP_DIR/mysql.out.canon $MYSQL_TMP_DIR/mysql.out
}
if ($canonicalization_perl)
{
  --exec perl -pe '$canonicalization_perl' $MYSQL_TMP_DIR/mysql.out > $MYSQL_TMP_DIR/mysql.out.canon
  --move_file $MYSQL_TMP_DIR/mysql.out.canon $MYSQL_TMP_DIR/mysql.out
}
if ($skip_sort=='')
{
  --exec (head -n 1 $MYSQL_TMP_DIR/mysql.out && tail -n +2 $MYSQL_TMP_DIR/mysql.out | sort) > $MYSQL_TMP_DIR/mysql.out.sorted
}
if ($skip_sort!='')
{
  --exec cp $MYSQL_TMP_DIR/mysql.out $MYSQL_TMP_DIR/mysql.out.sorted
}
--echo
--exec echo Number of output lines, including header: `grep -Fc '' '$MYSQL_TMP_DIR/mysql.out.sorted'`

--exec curl -s -X POST -H "Content-Type: application/json" http://$RDRS_NOKEY_HOST:$RDRS_NOKEY_PORT/0.1.0/ronsql --data-binary @$MYSQL_TMP_DIR/ronsql-rdrs-body > $MYSQL_TMP_DIR/ronsql-rdrs.out
if ($canonicalization_script)
{
  --exec sed -r '$canonicalization_script' $MYSQL_TMP_DIR/ronsql-rdrs.out > $MYSQL_TMP_DIR/ronsql-rdrs.out.canon
  --move_file $MYSQL_TMP_DIR/ronsql-rdrs.out.canon $MYSQL_TMP_DIR/ronsql-rdrs.out
}
if ($canonicalization_perl)
{
  --exec perl -pe '$canonicalization_perl' $MYSQL_TMP_DIR/ronsql-rdrs.out > $MYSQL_TMP_DIR/ronsql-rdrs.out.canon
  --move_file $MYSQL_TMP_DIR/ronsql-rdrs.out.canon $MYSQL_TMP_DIR/ronsql-rdrs.out
}
if ($skip_sort=='')
{
  --exec (head -n 1 $MYSQL_TMP_DIR/ronsql-rdrs.out && tail -n +2 $MYSQL_TMP_DIR/ronsql-rdrs.out | sort) > $MYSQL_TMP_DIR/ronsql-rdrs.out.sorted
}
if ($skip_sort!='')
{
  --exec cp $MYSQL_TMP_DIR/ronsql-rdrs.out $MYSQL_TMP_DIR/ronsql-rdrs.out.sorted
}

if ($suppress_ronsql_cli=='')
{
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING $QUERY_PARAMS > $MYSQL_TMP_DIR/ronsql-cli.out
if ($canonicalization_script)
{
  --exec sed -r '$canonicalization_script' $MYSQL_TMP_DIR/ronsql-cli.out > $MYSQL_TMP_DIR/ronsql-cli.out.canon
  --move_file $MYSQL_TMP_DIR/ronsql-cli.out.canon $MYSQL_TMP_DIR/ronsql-cli.out
}
if ($canonicalization_perl)
{
  --exec perl -pe '$canonicalization_perl' $MYSQL_TMP_DIR/ronsql-cli.out > $MYSQL_TMP_DIR/ronsql-cli.out.canon
  --move_file $MYSQL_TMP_DIR/ronsql-cli.out.canon $MYSQL_TMP_DIR/ronsql-cli.out
}
if ($skip_sort=='')
{
  --exec (head -n 1 $MYSQL_TMP_DIR/ronsql-cli.out && tail -n +2 $MYSQL_TMP_DIR/ronsql-cli.out | sort) > $MYSQL_TMP_DIR/ronsql-cli.out.sorted
}
if ($skip_sort!='')
{
  --exec cp $MYSQL_TMP_DIR/ronsql-cli.out $MYSQL_TMP_DIR/ronsql-cli.out.sorted
}
# todo The "|| true" at the end makes this test succeed no matter what the
# results are. Remove it when possible.
--exec diff -u --label ronsql-cli.out $MYSQL_TMP_DIR/ronsql-cli.out.sorted --label ronsql-rdrs.out $MYSQL_TMP_DIR/ronsql-rdrs.out.sorted || true
}

--echo
--echo == Diff ==
# todo The "|| true" at the end makes this test succeed no matter what the
# results are. Remove it when possible.
--exec diff -u --label mysql.out $MYSQL_TMP_DIR/mysql.out.sorted --label ronsql.out $MYSQL_TMP_DIR/ronsql-rdrs.out.sorted || true
--remove_file $MYSQL_TMP_DIR/ronsql-query.sql
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body-1
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body-2
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body-3
--remove_file $MYSQL_TMP_DIR/mysql.out
--remove_file $MYSQL_TMP_DIR/mysql.out.sorted
if ($suppress_ronsql_cli=='')
{
--remove_file $MYSQL_TMP_DIR/ronsql-cli.out
--remove_file $MYSQL_TMP_DIR/ronsql-cli.out.sorted
}
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs.out
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs.out.sorted
--echo ==========
--echo
--echo
--echo
if ($QUERY=='')
{
  --remove_file $QUERY_FILE
}
--let $QUERY=
--let $canonicalization_script=
--let $canonicalization_perl=
--let $skip_sort=
