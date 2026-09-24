# Two node groups, two replicas: one victim stays down during retries.
# This tests ronsql_cli, not the RDRS HTTP retry loop.
--disable_query_log
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
CREATE TABLE ronsql_nf_src (
  id INT NOT NULL PRIMARY KEY,
  val INT NOT NULL
) ENGINE=NDB;
# Use a regular MEMORY table: MySQL cannot reopen a TEMPORARY table
# under multiple aliases in the same statement.
CREATE TABLE ronsql_nf_digits (n INT NOT NULL PRIMARY KEY) ENGINE=MEMORY;
--enable_warnings
INSERT INTO ronsql_nf_digits VALUES (0),(1),(2),(3),(4),(5),(6),(7);
INSERT INTO ronsql_nf_src
SELECT 1+a.n+8*b.n+64*c.n+512*d.n+4096*e.n,
       1+a.n+8*b.n+64*c.n+512*d.n+4096*e.n
FROM ronsql_nf_digits a, ronsql_nf_digits b, ronsql_nf_digits c,
     ronsql_nf_digits d, ronsql_nf_digits e;
--disable_warnings
DROP TABLE ronsql_nf_digits;
--enable_warnings
# Local and exported names must differ beyond case: mysqltest looks up
# variables case-insensitively and exports their original spelling.
--let $nodefail_nodes = `SELECT GROUP_CONCAT(node_id ORDER BY node_id) FROM ndbinfo.nodes`
--let $nodefail_cookie = `SELECT FLOOR(RAND() * 536870910) + 1`
--let RNF_NODES = $nodefail_nodes
--let RNF_COOKIE = $nodefail_cookie
--perl
use strict;
use warnings;
use POSIX qw(WNOHANG _exit);
use Text::ParseWords qw(shellwords);
use Time::HiRes qw(time sleep);

my $mode = $ENV{RNF_MODE} // '';
my @nodes = split /,/, ($ENV{RNF_NODES} // '');
my $cookie = $ENV{RNF_COOKIE} // '';
die "Invalid RonSQL node-failure parameters"
  unless $mode =~ /^(peer|coordinator)$/ && @nodes == 4
    && !(grep { !/^[1-9][0-9]*$/ } @nodes);
die "Invalid event cookie" unless $cookie =~ /^[1-9][0-9]*$/
  && $cookie < 0x20000000;
my %node = map { $_ => 1 } @nodes;
my $dir = "$ENV{MYSQLTEST_VARDIR}/mysql_cluster.1/ndb_mgmd.1";
my $prefix = "$ENV{MYSQLTEST_VARDIR}/tmp/ronsql_nf_$mode";
my $connect = $ENV{NDB_CONNECTSTRING} or die "Missing NDB_CONNECTSTRING";
my @cli = ($ENV{RONSQL_CLI_EXE}, '--connect-string', $connect,
           '-D', 'test', '--output-format', 'TEXT_NOHEADER');
my @mgm = (shellwords($ENV{NDB_MGM}), '--connect-string', $connect);
my @waiter = (shellwords($ENV{NDB_WAITER}), '--connect-string', $connect);

# Distinct groups cross the 64 KiB redistribution flow-control threshold.
# Both main queries aggregate, so nothing may be printed before success.
my $query =
  'WITH a AS (SELECT id AS k, SUM(val) AS total FROM ronsql_nf_src GROUP BY id)';
if ($mode eq 'coordinator') {
  $query .= ', b AS (SELECT k, SUM(total) AS total FROM a GROUP BY k)';
}
my $cte = $mode eq 'peer' ? 'a' : 'b';
$query .= " SELECT COUNT(*), SUM($cte.total) FROM ronsql_nf_src AS s "
        . "JOIN $cte ON $cte.k = s.id;";
my $expected = "32768\t536887296\n";
my (%running, %status);
my @diagnostics;
my ($victim, $query_pid, $armed, $restart_needed);
my $sequence = 0;
$| = 1;

sub read_file {
  my ($file) = @_;
  open(my $fh, '<', $file) or die "open $file: $!";
  local $/;
  return <$fh> // '';
}
sub spawn {
  my ($out, $err, @cmd) = @_;
  my $pid = fork();
  die "fork: $!" unless defined $pid;
  if (!$pid) {
    open(STDIN, '<', '/dev/null') or _exit(126);
    open(STDOUT, '>', $out) or _exit(126);
    open(STDERR, '>', $err) or _exit(126);
    exec { $cmd[0] } @cmd or _exit(127);
  }
  delete $status{$pid};  # A reaped PID may be reused by a later command.
  $running{$pid} = 1;
  push @diagnostics, $out, $err;
  return $pid;
}
sub poll_child {
  my ($pid) = @_;
  return 1 if exists $status{$pid};
  my $rc = waitpid($pid, WNOHANG);
  die "waitpid $pid: $!" if $rc == -1;
  return 0 if !$rc;
  $status{$pid} = $?;
  delete $running{$pid};
  return 1;
}
sub stop_child {
  my ($pid) = @_;
  return if poll_child($pid);
  kill 'TERM', $pid;
  my $until = time() + 2;
  while (time() < $until) {
    return if poll_child($pid);
    sleep 0.05;
  }
  kill 'KILL', $pid;
  waitpid($pid, 0);
  $status{$pid} = $?;
  delete $running{$pid};
}
sub await_child {
  my ($pid, $seconds, $what) = @_;
  my $until = time() + $seconds;
  until (poll_child($pid)) {
    if (time() >= $until) {
      stop_child($pid);
      die "$what timed out after $seconds s; diagnostics: $prefix.*\n";
    }
    sleep 0.05;
  }
  return $status{$pid};
}
sub command {
  my ($seconds, @cmd) = @_;
  my $file = "$prefix.command." . ++$sequence;
  my $pid = spawn("$file.out", "$file.err", @cmd);
  die "Command failed: @cmd; see $file.*\n"
    if await_child($pid, $seconds, "@cmd") != 0;
}
sub management {
  command(30, @mgm, '-e', $_[0]);
}
sub clear_inserts {
  my $errors = '';
  for my $n (@nodes) {
    next if $restart_needed && defined $victim && $n == $victim;
    eval { management("$n ERROR 0"); 1 } or $errors .= $@;
  }
  die $errors if $errors ne '';
}
sub cluster_lines {
  my @lines;
  for my $file (glob("$dir/ndb_*_cluster.log*")) {
    next unless open(my $fh, '<', $file);
    push @lines, <$fh>;
  }
  return @lines;
}
sub result_text {
  my ($phase) = @_;
  my $text = read_file("$prefix.$phase.out");
  # The API logger shares stdout. Strip only complete disconnect notices,
  # not query rows or receive/close timeout diagnostics. The notice may
  # carry a reason ("disconnected in recv with errnum: 54"), so accept
  # any tail after "disconnected" on that line.
  $text =~ s/^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d \[NdbApi\] INFO\s+-- Node \d+ transporter to node \d+ \(id \d+\) disconnected[^\r\n]*\r?\n//mg;
  if ($phase eq 'failure') {
    # VM_TRACE PollGuard resets WAIT_NODE_FAILURE to NO_WAIT (0) before
    # printing this line. Timeout state 2 must remain an error.
    $text =~ s/^ERR: receiveResponse - theImpl->theWaiter\.m_state = 0\r?\n//mg;
  }
  return $text;
}
sub start_query {
  my ($phase) = @_;
  return spawn("$prefix.$phase.out", "$prefix.$phase.err", @cli, '-e', $query);
}
sub clean_query {
  my ($phase) = @_;
  my $pid = start_query($phase);
  die "$phase query failed; see $prefix.$phase.err\n"
    if await_child($pid, 120, "$phase query") != 0;
  die "$phase query needed error recovery; see $prefix.$phase.err\n"
    if read_file("$prefix.$phase.err") =~ /(?:\(code \d+\)|^NDB .* \d+,|Time-out state|scan close failed)/m;
  die "$phase result mismatch; see $prefix.$phase.out\n"
    unless result_text($phase) eq $expected;
}

my $ok = eval {
  command(190, @waiter, '--timeout=180');
  clean_query('baseline');
  print "RonSQL $mode: baseline matched\n";
  $armed = 1;  # A failed management command may still deliver its insert.
  # 5144 bit 29 clears the hold in NODE_FAILREP, before CLI retries can
  # stall behind it. Management-side clearing alone can arrive too late.
  my $extra = $mode eq 'coordinator' ? ($cookie | 0x20000000) : $cookie;
  management("ALL ERROR " . ($mode eq 'peer' ? 5140 : 5144) . " $extra");
  $query_pid = start_query('failure');

  my $until = time() + 30;
  while (time() < $until && !defined $victim) {
    die "Query completed before the hold and kill\n" if poll_child($query_pid);
    for my $line (cluster_lines()) {
      next unless $line =~ /\[CTE_RONSQL_(REDIST|FEED)_HELD node=(\d+) cookie=$cookie requester=(\d+) coordinator=(\d+)\]/;
      my ($kind, $source, $requester, $tc) = ($1, $2, $3, $4);
      next unless $kind eq ($mode eq 'peer' ? 'REDIST' : 'FEED');
      next unless $node{$source} && $node{$requester} && $node{$tc};
      next if $source == $tc;
      next if $mode eq 'peer' && $source == $requester;
      $victim = $mode eq 'peer' ? $source : $tc;
      last;
    }
    sleep 0.05 unless defined $victim;
  }
  die "No matching held operation within 30 s\n" unless defined $victim;
  die "Query completed before kill\n" if poll_child($query_pid);
  $restart_needed = 1;
  management("$victim RESTART -a -n");
  # Clear surviving inserts, including the proxy's copy. Coordinator
  # feed holds already clear locally in NODE_FAILREP; do not make retry
  # progress depend on management-command completion.
  clear_inserts();
  $armed = 0;
  command(70, @waiter, "--wait-nodes=$victim", '--not-started', '--timeout=60');
  print "RonSQL $mode: observed hold and killed the intended node\n";

  my $rc = await_child($query_pid, 120, 'query after node failure');
  my $err = read_file("$prefix.failure.err");
  my $out = result_text('failure');
  die "Receive/close timeout instead of node-failure handling\n"
    if ($err . $out) =~ /(?:code\s+4008\b|code\s+4012\b|Time-out state|receive timeout|scan close failed)/i;
  my @errors = ($err =~ /\(code (\d+)\)/g);
  push @errors, ($err =~ /^NDB [^\n]*? (\d+),/mg);
  # Node-failure reports from DBTC / DBLQH (286), DBSPJ (20016) and the
  # API (40xx), plus SETUP_NOT_RECEIVED (1274) from the 10 ms placeholder
  # sweeper: it REFs the consumers parked at the failure, and a retried
  # attempt that starts while the survivors still handle the failure can
  # see its SETUP processed later than its consumers arrive (plan F-10).
  # All of them are temporary errors, which RonSQL retries (it retries
  # nothing else); the final result is checked below.
  my %allowed = map { $_ => 1 } (286, 20016, 4010, 4025, 4028, 4031, 1274);
  die "Unexpected/missing node-failure diagnostic; see $prefix.failure.err\n"
    unless @errors && !(grep { !$allowed{$_} } @errors);
  if ($rc == 0) {
    die "Retry returned an incomplete or duplicated result\n" unless $out eq $expected;
  } else {
    # Only temporary errors were reported, so a failure must be the
    # exhausted retry budget (exit 3), never a permanent error (exit 1).
    die "Unexpected CLI exit status $rc\n" unless $rc == (3 << 8);
    die "Failed aggregate query emitted a result\n" unless $out eq '';
    die "Missing final CLI error\n"
      unless $err =~ /ronsql_cli caught retryable error after 3 attempts\./;
  }
  print "RonSQL $mode: bounded completion with a checked result or node-failure error\n";

  management("$victim START");
  command(190, @waiter, '--timeout=180');
  $restart_needed = 0;
  clean_query('recovery');

  # Worker-local dumps acknowledge instance 1; any leaking worker aborts.
  my @dumps = (2361, 2362, 2363, 2560, 2650);
  for my $dump (@dumps) { management("ALL DUMP $dump $cookie"); }
  my (%seen, $checked);
  $until = time() + 30;
  while (time() < $until) {
    for my $line (cluster_lines()) {
      if ($line =~ /\[JOIN_AGG_LEAK_CHECK_OK node=(\d+) dump=(\d+) cookie=$cookie\]/) {
        $seen{"$1:$2"} = 1;
      }
    }
    my @missing;
    for my $n (@nodes) {
      push @missing, grep { !$seen{"$n:$_"} } @dumps;
    }
    if (!@missing) { $checked = 1; last; }
    sleep 0.1;
  }
  die "Leak-check acknowledgements missing\n" unless $checked;
  command(40, @waiter, '--timeout=30');
  print "RonSQL $mode: recovery query matched and leak checks acknowledged\n";
  1;
};
my $failure = $@;
# Clear surviving holds, reap the CLI and restart our victim on failure.
# Preserve the original diagnostic even when cleanup also fails.
if (!$ok) {
  if ($armed) {
    eval { clear_inserts(); 1 } or $failure .= $@;
  }
  for my $pid (keys %running) { stop_child($pid); }
  if ($restart_needed) {
    eval {
      management("$victim START");
      command(190, @waiter, '--timeout=180');
      1;
    } or $failure .= $@;
  }
  die "$failure\nDiagnostics retained in $prefix.*\n";
}
# MTR checks that successful tests leave no new files in vardir/tmp.
# Remove only this run's outputs; the failure path above retains them.
for my $file (@diagnostics) {
  unlink($file) or die "unlink $file: $!";
}
EOF
--disable_warnings
DROP TABLE ronsql_nf_src;
--enable_warnings
--enable_query_log
