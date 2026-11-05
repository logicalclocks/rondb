#
# Run one case of testSpjMalformed after its positive control.
#
# testSpjMalformed sends crafted SCAN_TABREQ / TCKEYREQ signals (ViaSPJ
# flag set) with raw signals and checks that every data node is still
# started afterwards. DBSPJ and DBTC answer malformed input with a REF
# and report a security violation (reportMaliciousSignal; most types
# are Tier B, logged only, some Tier A, which disconnects the API node).
# A data node that STOPS is the failure. See
# storage/ndb/claude_files/dbspj/dbspj_attrinfo_protocol.html.
#
# Each case is its own MTR test (test_spj_malformed_<case>): MTR fails a
# test as soon as a data node crashes, so a crash in one case must not
# hide the result of the others. MTR restarts the cluster for the next
# test.
#
# The positive control runs first: if the well-formed raw SPJ scan does
# not get SCAN_TABCONF, the harness itself is broken and the result of
# the case cannot be trusted. The program creates and loads table T1.
#
# Before the hardening (2026-10-29) every case stopped a data node; with
# it every case is expected to pass.
#
# In: $spj_case        NDBT test case name (P1, P2, ...)
#     $spj_case_title  line echoed before the case
#
let NDBAPITEST_NAME=testSpjMalformed;

--echo # Positive control: well-formed raw SPJ scan, expect SCAN_TABCONF
let NDBAPITEST_ARGS=-n Control --stop-on-error T1;
--source suite/ndb/include/run_ndbapitest.inc

--echo # $spj_case_title
let NDBAPITEST_ARGS=-n $spj_case --stop-on-error T1;
--source suite/ndb/include/run_ndbapitest.inc
