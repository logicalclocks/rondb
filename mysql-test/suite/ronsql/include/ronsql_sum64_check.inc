# Inputs: $sum64_query and $sum64_overflow (0 = exact result, 1 = error 1860).
# Queries supplied by this test contain no quotes or JSON escape characters.
# Append the statement terminator required by the RonSQL parser.
--exec printf '{"query":"%s;","database":"test","outputFormat":"TEXT"}' '$sum64_query' > $MYSQL_TMP_DIR/sum64-body.json

if ($sum64_overflow)
{
  --error 1
  --exec $RONSQL_CLI_EXE --connect-string "$NDB_CONNECTSTRING" -D test -e '$sum64_query;' > $MYSQL_TMP_DIR/sum64-cli.out 2>&1
  --exec grep -q '^NDB Permanent error 1860,' $MYSQL_TMP_DIR/sum64-cli.out
}
if (!$sum64_overflow)
{
  --exec $RONSQL_CLI_EXE --connect-string "$NDB_CONNECTSTRING" -D test -e '$sum64_query;' > $MYSQL_TMP_DIR/sum64-cli.out
}

--exec curl -sS -D $MYSQL_TMP_DIR/sum64-headers -o $MYSQL_TMP_DIR/sum64-http.out -w '%{http_code}\n' -X POST -H 'Content-Type: application/json' http://$RDRS_NOKEY_HOST:$RDRS_NOKEY_PORT/0.1.0/ronsql --data-binary @$MYSQL_TMP_DIR/sum64-body.json > $MYSQL_TMP_DIR/sum64-status

if ($sum64_overflow)
{
  --exec grep -qx '400' $MYSQL_TMP_DIR/sum64-status
  --exec grep -Eiq '^X-RonSQL-Error-Class: semantic[[:space:]]*$' $MYSQL_TMP_DIR/sum64-headers
  --exec grep -Eiq '^X-RonSQL-NDB-Error: 1860[[:space:]]*$' $MYSQL_TMP_DIR/sum64-headers
}
if (!$sum64_overflow)
{
  --exec grep -qx '200' $MYSQL_TMP_DIR/sum64-status
  --exec $MYSQL --batch -D test -e '$sum64_query;' > $MYSQL_TMP_DIR/sum64-mysql.out
  --exec sort $MYSQL_TMP_DIR/sum64-mysql.out > $MYSQL_TMP_DIR/sum64-mysql.sorted
  --exec sort $MYSQL_TMP_DIR/sum64-cli.out > $MYSQL_TMP_DIR/sum64-cli.sorted
  --exec sort $MYSQL_TMP_DIR/sum64-http.out > $MYSQL_TMP_DIR/sum64-http.sorted
  --diff_files $MYSQL_TMP_DIR/sum64-mysql.sorted $MYSQL_TMP_DIR/sum64-cli.sorted
  --diff_files $MYSQL_TMP_DIR/sum64-mysql.sorted $MYSQL_TMP_DIR/sum64-http.sorted
  --remove_file $MYSQL_TMP_DIR/sum64-mysql.out
  --remove_file $MYSQL_TMP_DIR/sum64-mysql.sorted
  --remove_file $MYSQL_TMP_DIR/sum64-cli.sorted
  --remove_file $MYSQL_TMP_DIR/sum64-http.sorted
}
--remove_file $MYSQL_TMP_DIR/sum64-cli.out
--remove_file $MYSQL_TMP_DIR/sum64-http.out
--remove_file $MYSQL_TMP_DIR/sum64-headers
--remove_file $MYSQL_TMP_DIR/sum64-status
--remove_file $MYSQL_TMP_DIR/sum64-body.json
