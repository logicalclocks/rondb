# This include-file, or subroutine, converts a query to a SELECT EXPLAIN query
# and executes it with RonSQL over RDRS, saving the result in $EXPLAIN_FILE.
--source include/have_util_sed.inc
--source include/have_util_curl.inc
--echo == Explain Query ==
if ($EXPLAIN_FILE=='')
{
  --die Must define EXPLAIN_FILE variable
}
if ($QUERY!='')
{
  --let $QUERY_PARAMS = -e '$QUERY'
  --exec printf '%s' '$QUERY' > $MYSQL_TMP_DIR/ronsql-query.sql
}
if ($QUERY=='')
{
  if ($QUERY_FILE=='')
  {
    --die Must define either QUERY or QUERY_FILE variable
  }
  --let $QUERY_PARAMS = < '$QUERY_FILE'
  --exec cat $QUERY_FILE > $MYSQL_TMP_DIR/ronsql-query.sql
}
--exec printf '{"query":"' > $MYSQL_TMP_DIR/ronsql-rdrs-body-1
--exec perl -pe 's/\\/\\\\/g; s/"/\\"/g; s/\x08/\\b/g; s/\f/\\f/g; s/\n/\\n/g; s/\r/\\r/g; s/\t/\\t/g;' $MYSQL_TMP_DIR/ronsql-query.sql > $MYSQL_TMP_DIR/ronsql-rdrs-body-2
--exec printf '","database":"test","outputFormat":"TEXT","explainMode":"FORCE"}' > $MYSQL_TMP_DIR/ronsql-rdrs-body-3
--exec cat $MYSQL_TMP_DIR/ronsql-rdrs-body-1 $MYSQL_TMP_DIR/ronsql-rdrs-body-2 $MYSQL_TMP_DIR/ronsql-rdrs-body-3 > $MYSQL_TMP_DIR/ronsql-rdrs-body
--let $QUERY_PARAMS = -D test $QUERY_PARAMS
--exec curl --fail-with-body -X POST -H "Content-Type: application/json" http://$RDRS_NOKEY_HOST:$RDRS_NOKEY_PORT/0.1.0/ronsql --data-binary @$MYSQL_TMP_DIR/ronsql-rdrs-body > $EXPLAIN_FILE
--remove_file $MYSQL_TMP_DIR/ronsql-query.sql
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body-1
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body-2
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body-3
if ($QUERY=='')
{
  --remove_file $QUERY_FILE
}
--let $QUERY=
