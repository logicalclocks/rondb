# ronsql_fault_retry.inc -- one case of ronsql_single_table_faults: the
# statement $QUERY meets a temporary NDB error before it wrote anything,
# once, and is run again (README findings T-4, X-28).
#
# Inputs:
#   $QUERY          the statement, ending with ';', without quotes or
#                   backslashes (it is put in a JSON string as it is)
#   $fault_node     the data node that holds the primary replica of the
#                   table's one fragment
#   $fault_insert   the node-wide one-shot DBLQH error insert armed on
#                   $fault_node only: 5156 fails the node's next
#                   SCAN_FRAGREQ (NDB error 1217), 5155 its next
#                   LQHKEYREQ (1218), on whichever LDM instance or query
#                   worker runs it
#   $fault_table    the table's id, the insert's extra value: only a
#                   request on that table takes the node's one shot, so
#                   other reads on the node in the meantime (ronsql_cli
#                   may take 5 s to connect) can neither take nor clear it
#   $fault_error    the NDB error code the insert fails the request with
#   $fault_ordered  non-empty: the statement has a top-level ORDER BY,
#                   compare the rows in order; otherwise both outputs are
#                   sorted after the header
#
# RDRS: HTTP 200, the result of mysqld, attempts=2 in x-ronsql-phases.
# ronsql_cli: the result of mysqld after exactly one retry (one "->RRE"
# line) on the temporary error $fault_error.  The insert is cleared on
# every node ("all error 0") after each request.
--echo == Query ==
--echo $QUERY
--exec $MYSQL -D test -e '$QUERY' > $MYSQL_TMP_DIR/sf-mysql.out
if ($fault_ordered == '')
{
  --exec (head -n 1 $MYSQL_TMP_DIR/sf-mysql.out && tail -n +2 $MYSQL_TMP_DIR/sf-mysql.out | sort) > $MYSQL_TMP_DIR/sf-mysql.cmp
}
if ($fault_ordered != '')
{
  --exec cp $MYSQL_TMP_DIR/sf-mysql.out $MYSQL_TMP_DIR/sf-mysql.cmp
}

--echo == RDRS, error insert $fault_insert armed ==
--exec printf '{"query":"%s","database":"test","outputFormat":"TEXT"}' '$QUERY' > $MYSQL_TMP_DIR/sf-body
--exec $NDB_MGM -e "$fault_node error $fault_insert $fault_table" >> $NDB_TOOLS_OUTPUT
--exec curl -s -w 'HTTP %{http_code}\n' -D $MYSQL_TMP_DIR/sf-hdrs -o $MYSQL_TMP_DIR/sf-rdrs.out -X POST -H "Content-Type: application/json" -H "x-ronsql-phases: 1" http://$RDRS_NOKEY_HOST:$RDRS_NOKEY_PORT/0.1.0/ronsql --data-binary @$MYSQL_TMP_DIR/sf-body
--exec $NDB_MGM -e "all error 0" >> $NDB_TOOLS_OUTPUT
if ($fault_ordered == '')
{
  --exec (head -n 1 $MYSQL_TMP_DIR/sf-rdrs.out && tail -n +2 $MYSQL_TMP_DIR/sf-rdrs.out | sort) > $MYSQL_TMP_DIR/sf-rdrs.cmp
}
if ($fault_ordered != '')
{
  --exec cp $MYSQL_TMP_DIR/sf-rdrs.out $MYSQL_TMP_DIR/sf-rdrs.cmp
}
--exec echo Number of output lines, including header: `grep -Fc '' '$MYSQL_TMP_DIR/sf-rdrs.cmp'`
--echo == Diff with mysqld ==
--exec diff -u --label mysql.out $MYSQL_TMP_DIR/sf-mysql.cmp --label ronsql.out $MYSQL_TMP_DIR/sf-rdrs.cmp
--echo == The first attempt failed, the second succeeded (must be attempts=2) ==
--exec grep -i '^x-ronsql-phases:' $MYSQL_TMP_DIR/sf-hdrs | tr -d '\r' | grep -oE 'attempts=[0-9]+'

--echo == ronsql_cli, error insert $fault_insert armed ==
--exec $NDB_MGM -e "$fault_node error $fault_insert $fault_table" >> $NDB_TOOLS_OUTPUT
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e '$QUERY' > $MYSQL_TMP_DIR/sf-cli.out 2> $MYSQL_TMP_DIR/sf-cli.err || echo "UNEXPECTED: ronsql_cli failed"
--exec $NDB_MGM -e "all error 0" >> $NDB_TOOLS_OUTPUT
if ($fault_ordered == '')
{
  --exec (head -n 1 $MYSQL_TMP_DIR/sf-cli.out && tail -n +2 $MYSQL_TMP_DIR/sf-cli.out | sort) > $MYSQL_TMP_DIR/sf-cli.cmp
}
if ($fault_ordered != '')
{
  --exec cp $MYSQL_TMP_DIR/sf-cli.out $MYSQL_TMP_DIR/sf-cli.cmp
}
--echo == Diff with mysqld ==
--exec diff -u --label mysql.out $MYSQL_TMP_DIR/sf-mysql.cmp --label ronsql.out $MYSQL_TMP_DIR/sf-cli.cmp
--echo == Retried once, on NDB error $fault_error (must be 1 and 1) ==
--exec grep -c -e '->RRE$' $MYSQL_TMP_DIR/sf-cli.err || true
--exec grep -c 'NDB Temporary error $fault_error,' $MYSQL_TMP_DIR/sf-cli.err || true

--remove_file $MYSQL_TMP_DIR/sf-body
--remove_file $MYSQL_TMP_DIR/sf-hdrs
--remove_file $MYSQL_TMP_DIR/sf-mysql.out
--remove_file $MYSQL_TMP_DIR/sf-mysql.cmp
--remove_file $MYSQL_TMP_DIR/sf-rdrs.out
--remove_file $MYSQL_TMP_DIR/sf-rdrs.cmp
--remove_file $MYSQL_TMP_DIR/sf-cli.out
--remove_file $MYSQL_TMP_DIR/sf-cli.err
--remove_file $MYSQL_TMP_DIR/sf-cli.cmp
--let $QUERY=
--let $fault_ordered=
