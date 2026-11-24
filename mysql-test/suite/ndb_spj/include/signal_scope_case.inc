#
# Run one S case of testSpjMalformed and check its security report.
#
# DBTC, DBSPJ, DBLQH and the DBLQH proxy pass the JOIN_AGG_* and CTE_*
# signals between data nodes only. They had no signal scope, so a data
# node ran their handlers for an API node too, and some stopped the data
# node on a word of the signal. RONDB-1124: SignalScopes.hpp declares
# them Remote, or Local for the three that never leave their node. A
# data node drops a Remote one that an API node sends and reports it as
# signal_scope_remote_violation (violation 32, Tier A: the API node is
# disconnected).
#
# In: $spj_case, $spj_case_title  see spj_malformed_case.inc
#
let $signal_scope_before = `SELECT COALESCE(SUM(total_count), 0) FROM ndbinfo.security_violation_counts WHERE violation_id = 32`;

--source suite/ndb_spj/include/spj_malformed_case.inc

let $wait_condition=
  SELECT COALESCE(SUM(total_count), 0) > $signal_scope_before
  FROM ndbinfo.security_violation_counts WHERE violation_id = 32;
--source include/wait_condition.inc
--echo # Reported as signal_scope_remote_violation: OK
