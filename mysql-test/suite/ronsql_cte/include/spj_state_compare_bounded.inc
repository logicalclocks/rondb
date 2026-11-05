# spj_state_compare_bounded.inc
#
# Same contract as suite/ronsql/include/ronsql_compare.inc with
# $suppress_ronsql_cli=yes and $strict_diff=yes (mysql client baseline vs
# RonSQL over RDRS, sorted, strict diff), except that the RDRS request is
# bounded by curl --max-time $spj_state_max_time seconds (default 120).
#
# Used by spj_state_* tests whose suspected failure mode is a hang: a
# timed-out request makes the --exec fail (curl exit code 28) so the test
# fails in bounded time instead of running into the MTR testcase timeout.
#
# Input: $QUERY (single-quote free, as for ronsql_compare.inc).
--source include/have_util_sed.inc
--source include/have_util_curl.inc
if ($QUERY=='')
{
  --die spj_state_compare_bounded.inc needs QUERY
}
if ($spj_state_max_time=='')
{
  --let $spj_state_max_time=120
}
--echo == Query ==
--echo $QUERY
--exec printf '%s' '$QUERY' > $MYSQL_TMP_DIR/spj-state-query.sql
--exec printf '{"query":"' > $MYSQL_TMP_DIR/spj-state-body-1
--exec perl -pe 's/\\/\\\\/g; s/"/\\"/g; s/\x08/\\b/g; s/\f/\\f/g; s/\n/\\n/g; s/\r/\\r/g; s/\t/\\t/g;' $MYSQL_TMP_DIR/spj-state-query.sql > $MYSQL_TMP_DIR/spj-state-body-2
--exec printf '","database":"test","outputFormat":"TEXT"}' > $MYSQL_TMP_DIR/spj-state-body-3
--exec cat $MYSQL_TMP_DIR/spj-state-body-1 $MYSQL_TMP_DIR/spj-state-body-2 $MYSQL_TMP_DIR/spj-state-body-3 > $MYSQL_TMP_DIR/spj-state-body

--exec $MYSQL --default-character-set=utf8mb4 --init-command-add='SET time_zone = "+00:00";' -D test -e '$QUERY' > $MYSQL_TMP_DIR/spj-state-mysql.out
--exec (head -n 1 $MYSQL_TMP_DIR/spj-state-mysql.out && tail -n +2 $MYSQL_TMP_DIR/spj-state-mysql.out | sort) > $MYSQL_TMP_DIR/spj-state-mysql.out.sorted
--echo
--exec echo Number of output lines, including header: `grep -Fc '' '$MYSQL_TMP_DIR/spj-state-mysql.out.sorted'`

--echo RonSQL request bounded to $spj_state_max_time s
--exec curl -s --max-time $spj_state_max_time -X POST -H "Content-Type: application/json" http://$RDRS_NOKEY_HOST:$RDRS_NOKEY_PORT/0.1.0/ronsql --data-binary @$MYSQL_TMP_DIR/spj-state-body > $MYSQL_TMP_DIR/spj-state-rdrs.out
--exec (head -n 1 $MYSQL_TMP_DIR/spj-state-rdrs.out && tail -n +2 $MYSQL_TMP_DIR/spj-state-rdrs.out | sort) > $MYSQL_TMP_DIR/spj-state-rdrs.out.sorted

--echo
--echo == Diff ==
--exec diff -u --label mysql.out $MYSQL_TMP_DIR/spj-state-mysql.out.sorted --label ronsql.out $MYSQL_TMP_DIR/spj-state-rdrs.out.sorted

--remove_file $MYSQL_TMP_DIR/spj-state-query.sql
--remove_file $MYSQL_TMP_DIR/spj-state-body
--remove_file $MYSQL_TMP_DIR/spj-state-body-1
--remove_file $MYSQL_TMP_DIR/spj-state-body-2
--remove_file $MYSQL_TMP_DIR/spj-state-body-3
--remove_file $MYSQL_TMP_DIR/spj-state-mysql.out
--remove_file $MYSQL_TMP_DIR/spj-state-mysql.out.sorted
--remove_file $MYSQL_TMP_DIR/spj-state-rdrs.out
--remove_file $MYSQL_TMP_DIR/spj-state-rdrs.out.sorted
--echo ==========
--echo
--let $QUERY=
