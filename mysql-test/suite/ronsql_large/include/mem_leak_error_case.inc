# mem_leak_error_case.inc -- one error-insert case of the memory-leak
# harness: baseline, $ei_reps requests of a registered shape each with
# "ALL ERROR $ei_code $ei_extra" armed on every data node (cleared after
# each request, after an optional $ei_hold seconds), then the level must
# return to the baseline (tolerance $mlk_qm_tolerance / $mlk_tm_tolerance)
# and the record-level DUMP checks must pass.
#
# Inputs: $ei_name (case name), $ei_code, $ei_extra (may be empty),
#   $ei_shape (registered shape id), $ei_accept (ok | fail | any, see
#   mem_leak_exec.inc), $ei_reps, $ei_hold (seconds, may be empty),
#   $ei_what (description).  If a request has an unexpected outcome the
#   executor clears the inserts ("ALL ERROR 0") before failing the test.
--echo
--echo --- $mlk_prefix-$ei_name: error insert $ei_code on every data node, shape $mlk_prefix-$ei_shape: $ei_what ---
--let $ei_cmd = ALL ERROR $ei_code
if ($ei_extra != '')
{
  --let $ei_cmd = ALL ERROR $ei_code $ei_extra
}
--let $mlk_tol_qm = $mlk_qm_tolerance
--let $mlk_tol_tm = $mlk_tm_tolerance
--let $mlk_tag = $mlk_prefix-$ei_name
--let $mlk_snapshot_label = baseline
--source suite/ronsql_large/include/mem_leak_snapshot.inc
--let MLKE_SHAPES = $ei_shape
--let MLKE_COUNT = 1
--let MLKE_CLIENTS = 1
--let MLKE_ACCEPT = $ei_accept
--let MLKE_LABEL = $mlk_prefix-$ei_name
--let MLKE_QUIET = 1
--let MLKE_TRACE = 0
--let MLKE_CLEAR_ON_FAIL = 1
--let $ei_i = 0
while ($ei_i < $ei_reps)
{
  --exec $NDB_MGM -e "$ei_cmd" > /dev/null
  --source suite/ronsql_large/include/mem_leak_exec.inc
  if ($ei_hold)
  {
    --real_sleep $ei_hold
  }
  --exec $NDB_MGM -e "ALL ERROR 0" > /dev/null
  --inc $ei_i
}
--let MLKE_CLEAR_ON_FAIL = 0
--let MLKE_QUIET = 0
if ($ei_accept == 'ok')
{
  --echo $ei_reps request(s) with the insert armed: every result identical to the reference
}
if ($ei_accept == 'fail')
{
  --echo $ei_reps request(s) with the insert armed: every request failed, as the insert intends
}
if ($ei_accept == 'any')
{
  --echo $ei_reps request(s) with the insert armed: every request answered, no hang
}
--let $mlk_verify_label = after the error insert
--let $mlk_verify_what = back to the baseline
--source suite/ronsql_large/include/mem_leak_verify.inc
--source suite/ronsql_large/include/mem_leak_dumps.inc
--let $ei_extra =
--let $ei_hold =
