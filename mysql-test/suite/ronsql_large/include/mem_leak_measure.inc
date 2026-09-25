# mem_leak_measure.inc -- phase 2 of the memory-leak harness for one
# shape (or a mix of shapes): rounds of increasing size, no growth after
# round 1.
#
#   round 1: $mlk_r1 execution(s), then settle and take the baseline
#   round 2: $mlk_r2 executions, the level must come back to the baseline
#   round 3: $mlk_r3 executions, the level must come back to the baseline
#
# A per-request leak grows with the round size; the bounded one-offs of a
# shape's first executions were absorbed by the warm-up and round 1.  The
# per-execution QUERY_MEMORY totals of every round are in the report file,
# so a failure can be classified: growth in round 3 larger than in round 2
# is a per-request leak, the same step in both is a late one-off.
#
# Inputs (set by mem_leak_measure_all.inc from the mlk_shapes table, or by
# the test): $mlk_k (name), $mlk_shapes (comma list of shape ids),
# $mlk_r1 / $mlk_r2 / $mlk_r3 (all >= 1), $mlk_clients, $mlk_accept,
# $mlk_trace.
#
# No "source" inside if-blocks here: this file is sourced from a while
# loop, and mysqltest replays loops from its line cache, so an include
# that was skipped in the first iteration would be missing later.
--echo
--echo --- $mlk_prefix-$mlk_k: rounds of $mlk_r1, $mlk_r2 and $mlk_r3 execution(s), the settled level must not grow after round 1 ---
--let MLKE_SHAPES = $mlk_shapes
--let MLKE_CLIENTS = $mlk_clients
--let MLKE_ACCEPT = $mlk_accept
--let MLKE_TRACE = $mlk_trace
--let MLKE_QUIET = 0
--let MLKE_CLEAR_ON_FAIL = 0

--let MLKE_LABEL = round 1
--let MLKE_COUNT = $mlk_r1
--source suite/ronsql_large/include/mem_leak_exec.inc
--let $mlk_tol_qm = $mlk_qm_tolerance
--let $mlk_tol_tm = $mlk_tm_tolerance
--let $mlk_tag = $mlk_prefix-$mlk_k round 1
--let $mlk_snapshot_label = baseline after round 1
--source suite/ronsql_large/include/mem_leak_snapshot.inc

--let MLKE_LABEL = round 2
--let MLKE_COUNT = $mlk_r2
--source suite/ronsql_large/include/mem_leak_exec.inc
--let $mlk_tag = $mlk_prefix-$mlk_k round 2
--let $mlk_verify_label = after round 2
--let $mlk_verify_what = back to the round-1 level
--source suite/ronsql_large/include/mem_leak_verify.inc

--let MLKE_LABEL = round 3
--let MLKE_COUNT = $mlk_r3
--source suite/ronsql_large/include/mem_leak_exec.inc
--let $mlk_tag = $mlk_prefix-$mlk_k round 3
--let $mlk_verify_label = after round 3
--let $mlk_verify_what = back to the round-1 level
--source suite/ronsql_large/include/mem_leak_verify.inc
