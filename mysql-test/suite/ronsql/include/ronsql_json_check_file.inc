# The check of ronsql_json_check.inc for a query in the file $QUERY_FILE,
# which, unlike $QUERY there, may hold backticks and quotes of either kind:
# run it with $JSON_FORMAT output (JSON or JSON_ASCII) on ronsql_cli and on
# RDRS, then feed each body to a JSON parser: the --exec fails on invalid
# JSON and otherwise prints the canonical re-encoding. The RDRS request
# body is the query escaped as a JSON string, built as in
# ronsql_compare.inc. The query is printed with a tab shown as <TAB>, and
# the file is removed at the end.
# Requires $QUERY_FILE, $JSON_FORMAT and $JSON_CANON from the caller.
--source include/have_util_curl.inc
--echo == Query ==
--exec perl -pe 's/\t/<TAB>/g' $QUERY_FILE
--echo == ronsql_cli $JSON_FORMAT ==
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test --output-format $JSON_FORMAT < '$QUERY_FILE' > $MYSQL_TMP_DIR/ronsql-json.out
--exec cat $MYSQL_TMP_DIR/ronsql-json.out
--exec $JSON_CANON < $MYSQL_TMP_DIR/ronsql-json.out
--echo == RDRS $JSON_FORMAT ==
--exec printf '{"query":"' > $MYSQL_TMP_DIR/ronsql-json-body
--exec perl -pe 's/\\/\\\\/g; s/"/\\"/g; s/\x08/\\b/g; s/\f/\\f/g; s/\n/\\n/g; s/\r/\\r/g; s/\t/\\t/g;' $QUERY_FILE >> $MYSQL_TMP_DIR/ronsql-json-body
--exec printf '","database":"test","outputFormat":"$JSON_FORMAT"}' >> $MYSQL_TMP_DIR/ronsql-json-body
--exec curl --no-progress-meter --fail-with-body -X POST -H "Content-Type: application/json" http://$RDRS_NOKEY_HOST:$RDRS_NOKEY_PORT/0.1.0/ronsql --data-binary @$MYSQL_TMP_DIR/ronsql-json-body > $MYSQL_TMP_DIR/ronsql-json.out
--exec cat $MYSQL_TMP_DIR/ronsql-json.out
--exec $JSON_CANON < $MYSQL_TMP_DIR/ronsql-json.out
--remove_file $MYSQL_TMP_DIR/ronsql-json-body
--remove_file $MYSQL_TMP_DIR/ronsql-json.out
--remove_file $QUERY_FILE
