# Wait for node $_ndb_nodeid to be starting at phase
# $_start_phase or a later phase.

--echo Waiting for node $_ndb_nodeid to reach start phase $_start_phase or later

--let $success= 0
--let $tmpfile= $MYSQLTEST_VARDIR/tmp/wait_start_phase_at_least.txt
--let $maxtime= 120

--disable_query_log
--disable_result_log

CREATE TEMPORARY TABLE test.tmpfile (txt VARCHAR(200));

while (!$success)
{
  --exec $NDB_MGM -e "$_ndb_nodeid STATUS" > $tmpfile
  eval LOAD DATA INFILE '$tmpfile' INTO TABLE test.tmpfile;
  eval SELECT COUNT(1)
         FROM test.tmpfile
        WHERE txt LIKE "%: starting (Last completed phase %"
          AND CAST(
                SUBSTRING_INDEX(
                  SUBSTRING_INDEX(txt, "Last completed phase ", -1),
                  ")", 1)
                AS UNSIGNED) >= $_start_phase
         INTO @match_count;

  --let $success= query_get_value(SELECT @match_count AS m, m, 1)
  DELETE FROM test.tmpfile;

  if (!$success)
  {
    --sleep 1
    --dec $maxtime
    if (!$maxtime)
    {
      --echo "Timeout waiting for $_ndb_nodeid to reach start phase $_start_phase"
      die "Timeout waiting for minimum start phase";
    }
  }
}

DROP TABLE test.tmpfile;
--remove_file $tmpfile

--enable_query_log
--enable_result_log
