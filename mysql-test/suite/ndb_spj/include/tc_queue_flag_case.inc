#
# Run one Q case of testSpjMalformed and check its security report.
#
# DBTC's rate queue handler re-sends a queued request to its own DBTC
# instance with a mark that it was queued (the passQueue flag of
# TCKEYREQ, TCINDXREQ and SCAN_TABREQ, bit 2 of SCAN_NEXTREQ's stopScan).
# DBTC took the mark from any sender, and from an API an ndbrequire on
# the counts that a re-send raises stopped the data node. RONDB-1124: a
# request with the mark from any other sender is reported as
# tc_queue_flag_not_internal (violation 38, Tier A: the API node is
# disconnected). Bit 24 of a short TCKEYREQ or TCINDXREQ is part of its
# key length, so a short request is not checked.
#
# In: $spj_case, $spj_case_title  see spj_malformed_case.inc
#     $tc_queue_flag_reported     1: the case must be reported,
#                                 0: it must not be
#
let $tc_queue_flag_before = `SELECT COALESCE(SUM(total_count), 0) FROM ndbinfo.security_violation_counts WHERE violation_id = 38`;

--source suite/ndb_spj/include/spj_malformed_case.inc

if ($tc_queue_flag_reported)
{
  let $wait_condition=
    SELECT COALESCE(SUM(total_count), 0) > $tc_queue_flag_before
    FROM ndbinfo.security_violation_counts WHERE violation_id = 38;
  --source include/wait_condition.inc
  --echo # Reported as tc_queue_flag_not_internal: OK
}
if (!$tc_queue_flag_reported)
{
  let $tc_queue_flag_after = `SELECT COALESCE(SUM(total_count), 0) FROM ndbinfo.security_violation_counts WHERE violation_id = 38`;
  if ($tc_queue_flag_after != $tc_queue_flag_before)
  {
    --die tc_queue_flag_not_internal was reported for a request without the flag
  }
  --echo # Not reported: OK
}
