# ronsql_phase_rows.inc — run $QUERY (or $QUERY_FILE) through RDRS RonSQL
# and pin the `rows=` counter of the x-ronsql-phases response header
# (RonSQLPhaseStats::rows_drained: rows the RonSQL result loop fetched from
# NDB on the last attempt).  The token is printed into the result file AND
# asserted against $EXPECT_ROWS with a fatal grep.
#
# This is the RUNTIME twin of an EXPLAIN grep: EXPLAIN says which plan the
# preparer chose, the row counter says what the drain actually did.  For
# pass-through ORDER BY queries (ronsql_orderby_limit_plan.md Phase 4b) the
# SF_OrderBy index-order streaming path drains exactly min(LIMIT, matching
# rows) and closes the scan early, while the Phase 3 buffered client-side
# sort must drain every matching row before it can sort — so `rows=` tells
# the two apart even though both print identical, correctly ordered output.
#
# Inputs: $QUERY or $QUERY_FILE (as ronsql_compare.inc / ronsql_explain.inc;
# a QUERY_FILE is removed afterwards), $EXPECT_ROWS.  Resets $QUERY and
# $EXPECT_ROWS.  Requires RDRS built with RONSQL_PHASE_STATS (the default).
--source include/have_util_curl.inc
--echo == Phase rows ==
if ($EXPECT_ROWS=='')
{
  --die Must define EXPECT_ROWS variable
}
if ($QUERY!='')
{
  --exec printf '%s' '$QUERY' > $MYSQL_TMP_DIR/ronsql-query.sql
}
if ($QUERY=='')
{
  if ($QUERY_FILE=='')
  {
    --die Must define either QUERY or QUERY_FILE variable
  }
  --exec cat $QUERY_FILE > $MYSQL_TMP_DIR/ronsql-query.sql
}
--exec printf '{"query":"' > $MYSQL_TMP_DIR/ronsql-rdrs-body-1
--exec perl -pe 's/\\/\\\\/g; s/"/\\"/g; s/\x08/\\b/g; s/\f/\\f/g; s/\n/\\n/g; s/\r/\\r/g; s/\t/\\t/g;' $MYSQL_TMP_DIR/ronsql-query.sql > $MYSQL_TMP_DIR/ronsql-rdrs-body-2
--exec printf '","database":"test","outputFormat":"TEXT"}' > $MYSQL_TMP_DIR/ronsql-rdrs-body-3
--exec cat $MYSQL_TMP_DIR/ronsql-rdrs-body-1 $MYSQL_TMP_DIR/ronsql-rdrs-body-2 $MYSQL_TMP_DIR/ronsql-rdrs-body-3 > $MYSQL_TMP_DIR/ronsql-rdrs-body
--exec curl -s -D $MYSQL_TMP_DIR/ronsql-phase-hdrs -o $MYSQL_TMP_DIR/ronsql-phase-out -X POST -H "Content-Type: application/json" http://$RDRS_NOKEY_HOST:$RDRS_NOKEY_PORT/0.1.0/ronsql --data-binary @$MYSQL_TMP_DIR/ronsql-rdrs-body
# Print the drained-row token (recorded in the baseline), then assert it.
--exec grep -i '^x-ronsql-phases:' $MYSQL_TMP_DIR/ronsql-phase-hdrs | tr -d '\r' | sed -r 's/.*,(rows=[0-9]+),.*/\1/'
--exec grep -i '^x-ronsql-phases:' $MYSQL_TMP_DIR/ronsql-phase-hdrs | tr -d '\r' | grep -qF ',rows=$EXPECT_ROWS,'
--remove_file $MYSQL_TMP_DIR/ronsql-query.sql
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body-1
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body-2
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body-3
--remove_file $MYSQL_TMP_DIR/ronsql-phase-hdrs
--remove_file $MYSQL_TMP_DIR/ronsql-phase-out
if ($QUERY=='')
{
  --remove_file $QUERY_FILE
}
--let $QUERY=
--let $EXPECT_ROWS=
