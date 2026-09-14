# Run $QUERY (no backticks, no quotes of either kind: the curl body is a
# single-quoted shell word) with JSON output on
# ronsql_cli, on RDRS with an explicit outputFormat JSON and on RDRS with
# the default output format (what Hopsworks gets), then feed every body to
# a JSON parser: the --exec fails on invalid JSON and otherwise prints the
# canonical re-encoding, which keeps the quoting visible in the result.
# Requires $JSON_CANON and $RDRS_RONSQL from the caller.
--echo == Query ==
--echo $QUERY
--echo == ronsql_cli JSON ==
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test --output-format JSON -e "$QUERY" > $MYSQL_TMP_DIR/ronsql-json.out
--exec cat $MYSQL_TMP_DIR/ronsql-json.out
--exec $JSON_CANON < $MYSQL_TMP_DIR/ronsql-json.out
--echo == RDRS JSON (explicit outputFormat) ==
--exec $RDRS_RONSQL '{"query":"$QUERY","database":"test","outputFormat":"JSON"}' > $MYSQL_TMP_DIR/ronsql-json.out
--exec cat $MYSQL_TMP_DIR/ronsql-json.out
--exec $JSON_CANON < $MYSQL_TMP_DIR/ronsql-json.out
--echo == RDRS JSON (default output format) ==
--exec $RDRS_RONSQL '{"query":"$QUERY","database":"test"}' > $MYSQL_TMP_DIR/ronsql-json.out
--exec cat $MYSQL_TMP_DIR/ronsql-json.out
--exec $JSON_CANON < $MYSQL_TMP_DIR/ronsql-json.out
--remove_file $MYSQL_TMP_DIR/ronsql-json.out
--echo
--let $QUERY=
