# mem_leak_measure_all.inc -- phase 2 for every shape registered with
# mem_leak_register.inc, in registration order.  Measuring only after all
# shapes were warmed up means that every shape's one-off allocations (and
# those of the shapes before it) are in place before any round-1 baseline.
# The per-shape parameters come from the temporary table mlk_shapes (a
# "source" of a different file per while-loop iteration would not work:
# mysqltest replays loops from its line cache).
--echo
--echo === mem-leak harness: phase 2, measured rounds ===
--let $mlk_j = 1
while ($mlk_j <= $mlk_count)
{
  --let $mlk_k = $mlk_j
  --let $mlk_shapes = $mlk_j
  --let $mlk_r1 = `SELECT r1 FROM mlk_shapes WHERE k = $mlk_j`
  --let $mlk_r2 = `SELECT r2 FROM mlk_shapes WHERE k = $mlk_j`
  --let $mlk_r3 = `SELECT r3 FROM mlk_shapes WHERE k = $mlk_j`
  --let $mlk_clients = `SELECT clients FROM mlk_shapes WHERE k = $mlk_j`
  --let $mlk_accept = `SELECT accept_mode FROM mlk_shapes WHERE k = $mlk_j`
  --let $mlk_trace = `SELECT trace_on FROM mlk_shapes WHERE k = $mlk_j`
  --source suite/ronsql_large/include/mem_leak_measure.inc
  --inc $mlk_j
}
