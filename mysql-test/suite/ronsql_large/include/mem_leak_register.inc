# mem_leak_register.inc -- phase 1 of the memory-leak harness for one
# query shape: store it, check its first use, warm it up, compare it.
#
# Inputs: $QUERY (may span lines and contain quotes; reset afterwards),
#   $mlk_label (printed), and optional per-shape overrides that are reset
#   afterwards:
#     $mlk_shape_kind      rdrs (default) | cli (ronsql_cli, a new NDB API
#                          connection per execution; sequential only; no
#                          MySQL comparison)
#     $mlk_shape_compare   yes (default) | no: skip ronsql_compare.inc
#     $mlk_shape_round1/2/3, $mlk_shape_clients, $mlk_shape_accept,
#     $mlk_shape_warmup_clients   override the test-wide knobs
#
# Steps:
#   1. idle level before the shape's first execution (mem_leak_snapshot.inc,
#      tolerance = the first-use allowance)
#   2. reference run: one execution, its sorted output is the reference
#   3. warm-up: $mlk_warmup executions per client with
#      $mlk_warmup_clients concurrent clients (HTTP 503 tolerated)
#   4. first-use check (mem_leak_verify.inc): the level must come back
#      within the first-use allowance.  Bounded per-instance one-offs fit;
#      one pinned 2 MB lc_ndbd_pool segment does not, so a leaked pool
#      allocation (e.g. a string MIN/MAX slot) fails here after the very
#      first executions, long before it would fill a segment in the rounds.
#   5. the MySQL comparison (suite/ronsql/include/ronsql_compare.inc,
#      $strict_diff as set by the test): RDRS once more, mysqld once.
#   6. records the phase-2 parameters (rounds, clients, accept mode) in the
#      temporary table mlk_shapes for mem_leak_measure_all.inc.
--inc $mlk_count
--let $mlk_k = $mlk_count
--echo
--echo === $mlk_prefix-$mlk_k: $mlk_label ===

--let $mlk_s_kind = rdrs
if ($mlk_shape_kind != '')
{
  --let $mlk_s_kind = $mlk_shape_kind
}
--let $mlk_s_compare = yes
if ($mlk_shape_compare != '')
{
  --let $mlk_s_compare = $mlk_shape_compare
}
--let $mlk_s_r1 = $mlk_round1
--let $mlk_s_r2 = $mlk_round2
--let $mlk_s_r3 = $mlk_round3
if (!$mlk_rounds_forced)
{
  if ($mlk_shape_round1 != '')
  {
    --let $mlk_s_r1 = $mlk_shape_round1
  }
  if ($mlk_shape_round2 != '')
  {
    --let $mlk_s_r2 = $mlk_shape_round2
  }
  if ($mlk_shape_round3 != '')
  {
    --let $mlk_s_r3 = $mlk_shape_round3
  }
}
--let $mlk_s_clients = $mlk_round_clients
if ($mlk_shape_clients != '')
{
  --let $mlk_s_clients = $mlk_shape_clients
}
--let $mlk_s_accept = $mlk_round_accept
if ($mlk_shape_accept != '')
{
  --let $mlk_s_accept = $mlk_shape_accept
}
--let $mlk_s_wclients = $mlk_warmup_clients
if ($mlk_shape_warmup_clients != '')
{
  --let $mlk_s_wclients = $mlk_shape_warmup_clients
}
--let $mlk_s_waccept = ok_or_503
if ($mlk_s_kind == 'cli')
{
  --let $mlk_s_compare = no
  --let $mlk_s_clients = 1
  --let $mlk_s_wclients = 1
  --let $mlk_s_waccept = ok
}
--let $mlk_s_trace = 1
if ($mlk_s_clients > 1)
{
  --let $mlk_s_trace = 0
}
if (`SELECT LEAST($mlk_s_r1, $mlk_s_r2, $mlk_s_r3) < 1`)
{
  --die mem_leak_register: every round needs at least one execution
}

# Store the query (a file: it may contain quotes and newlines) and the
# phase-2 parameters.
--let MLKE_QTEXT = $QUERY
--let MLKE_QBASE = $MYSQL_TMP_DIR/mlk_q$mlk_k
--let MLKE_GKIND = $mlk_s_kind
--perl
use strict;
use warnings;
my $b = $ENV{MLKE_QBASE};
sub spew {
  my ($f, $s) = @_;
  open(my $fh, '>', $f) or die "mem_leak_register: open $f: $!\n";
  print $fh $s;
  close($fh) or die "mem_leak_register: close $f: $!\n";
}
die "mem_leak_register: empty \$QUERY\n" if ($ENV{MLKE_QTEXT} // '') eq '';
spew("$b.sql", $ENV{MLKE_QTEXT} . "\n");
spew("$b.kind", $ENV{MLKE_GKIND} . "\n");
EOF
--disable_query_log
--eval INSERT INTO mlk_shapes VALUES ($mlk_k, $mlk_s_r1, $mlk_s_r2, $mlk_s_r3, $mlk_s_clients, '$mlk_s_accept', $mlk_s_trace)
--enable_query_log

# 1. Idle level before the first execution of this shape.
--let $mlk_tol_qm = $mlk_first_use_qm
--let $mlk_tol_tm = $mlk_first_use_tm
--let $mlk_tag = $mlk_prefix-$mlk_k first use
--let $mlk_snapshot_label = idle level before the first execution
--source suite/ronsql_large/include/mem_leak_snapshot.inc

# 2. Reference run.
--let MLKE_SHAPES = $mlk_k
--let MLKE_COUNT = 1
--let MLKE_CLIENTS = 1
--let MLKE_ACCEPT = ok
--let MLKE_LABEL = reference run
--let MLKE_QUIET = 0
--let MLKE_TRACE = 0
--let MLKE_CLEAR_ON_FAIL = 0
--source suite/ronsql_large/include/mem_leak_exec.inc

# 3. Warm-up, concurrent for RDRS shapes.
if ($mlk_warmup)
{
  --let MLKE_COUNT = $mlk_warmup
  --let MLKE_CLIENTS = $mlk_s_wclients
  --let MLKE_ACCEPT = $mlk_s_waccept
  --let MLKE_LABEL = warm-up
  --source suite/ronsql_large/include/mem_leak_exec.inc
}

# 4. First-use check.
--let $mlk_verify_label = first use
--let $mlk_verify_what = within the first-use allowance
--source suite/ronsql_large/include/mem_leak_verify.inc

# 5. Correctness against mysqld.
if ($mlk_s_compare == 'yes')
{
  --let $QUERY =
  --let $QUERY_FILE = $MYSQL_TMP_DIR/mlk_compare.sql
  --copy_file $MYSQL_TMP_DIR/mlk_q$mlk_k.sql $QUERY_FILE
  --source suite/ronsql/include/ronsql_compare.inc
}

--let $QUERY =
--let $mlk_shape_kind =
--let $mlk_shape_compare =
--let $mlk_shape_round1 =
--let $mlk_shape_round2 =
--let $mlk_shape_round3 =
--let $mlk_shape_clients =
--let $mlk_shape_accept =
--let $mlk_shape_warmup_clients =
