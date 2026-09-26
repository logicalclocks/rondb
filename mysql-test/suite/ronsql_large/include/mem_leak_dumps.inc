# mem_leak_dumps.inc -- record-level leak checks on every data node,
# complementing the page-level ndbinfo.resources checks (they look at
# pools and hash tables, not at QUERY_MEMORY's page count).  Each DUMP
# aborts its node (ndbabort) if it finds a leak, and with a cookie as
# second word acknowledges a clean pool with the cluster-log line
#   [JOIN_AGG_LEAK_CHECK_OK node=N dump=D cookie=C]
#
#   2361 DBLQH  JoinAggregationState pool empty       (VM_TRACE/ERROR_INSERT)
#   2362 DBLQH  CteScanIterState pool empty           (VM_TRACE/ERROR_INSERT)
#   2363 DBLQH  join-agg identity table and park pool (VM_TRACE/ERROR_INSERT)
#   2364 DBLQH  redistribution page counter is zero   (VM_TRACE/ERROR_INSERT)
#   2560 DBTC   no AggCompleteRecord, no CteScanFragHandle, no scan in a
#               join-agg wait state or CLOSING_SCAN   (every build)
#   2650 DBSPJ  no scan or lookup request left        (every build)
#
# The DBLQH checks only exist in debug-compiled data nodes; this include
# asks ndb_mgm whether it is debug compiled (like include/check_ndb_debug.inc)
# and otherwise sends 2560 and 2650 only.  Run it only on an idle cluster:
# 2560 would also flag an unrelated scan that happens to be closing.
#
# A missing acknowledgement means the node aborted (or is hung): its
# ndb_<id>_out.log / error log names the leaked record ("DUMP 2361: leaked
# agg state ...", "DUMP 2650: leaked DBSPJ request ...").
--let $mlk_dump_cookie = `SELECT FLOOR(RAND() * 536870910) + 1`
--let $mlk_dump_nodes = `SELECT GROUP_CONCAT(node_id ORDER BY node_id) FROM ndbinfo.nodes`
--let MLKE_DUMP_COOKIE = $mlk_dump_cookie
--let MLKE_DUMP_NODES = $mlk_dump_nodes
--perl
use strict;
use warnings;
use POSIX qw(_exit);
use Text::ParseWords qw(shellwords);
use Time::HiRes qw(time sleep);

my $cookie = $ENV{MLKE_DUMP_COOKIE} // '';
my @nodes = split /,/, ($ENV{MLKE_DUMP_NODES} // '');
die "mem_leak_dumps: bad cookie '$cookie' or node list '@nodes'\n"
  unless $cookie =~ /^[1-9]\d*$/ && @nodes
    && !grep({ !/^[1-9]\d*$/ } @nodes);
my @mgm = (shellwords($ENV{NDB_MGM} // ''),
           '--connect-string', ($ENV{NDB_CONNECTSTRING} // ''));
my $dir = "$ENV{MYSQLTEST_VARDIR}/mysql_cluster.1/ndb_mgmd.1";
my $report = "$ENV{MYSQLTEST_VARDIR}/log/ronsql_large_mem_leak.report";

sub mgm {
  my ($cmd, $capture) = @_;
  my $pid = open(my $ph, '-|');
  die "mem_leak_dumps: fork: $!\n" unless defined $pid;
  if (!$pid) {
    open(STDERR, '>&', \*STDOUT);
    exec { $mgm[0] } @mgm, '-e', $cmd or _exit(127);
  }
  local $/;
  my $out = <$ph>;
  close($ph);
  die "mem_leak_dumps: ndb_mgm -e '$cmd' failed: " . ($out // '') . "\n"
    if $? != 0;
  return $capture ? ($out // '') : '';
}

my $debug = mgm('help', 1) =~ /debug compiled/;
my @dumps = $debug ? (2361, 2362, 2363, 2364, 2560, 2650) : (2560, 2650);
mgm("ALL DUMP $_ $cookie", 0) for @dumps;

my %seen;
my @missing;
my $deadline = time() + 60;
while (1) {
  for my $file (glob("$dir/ndb_*_cluster.log*")) {
    next unless open(my $fh, '<', $file);
    while (my $line = <$fh>) {
      $seen{"$1:$2"} = 1
        if $line =~ /\[JOIN_AGG_LEAK_CHECK_OK node=(\d+) dump=(\d+) cookie=$cookie\]/;
    }
    close($fh);
  }
  @missing = ();
  for my $n (@nodes) {
    push @missing, map { "$n:$_" } grep { !$seen{"$n:$_"} } @dumps;
  }
  last if !@missing || time() > $deadline;
  sleep(0.2);
}
if (open(my $fh, '>>', $report)) {
  my @t = localtime();
  printf $fh "%02d:%02d:%02d %s DUMP %s cookie %d: %s\n", $t[2], $t[1], $t[0],
    ($ENV{MLKE_TEST} // '?'), join(' ', @dumps), $cookie,
    @missing ? "MISSING node:dump @missing" : 'acknowledged by all nodes';
  close($fh);
}
die "mem_leak_dumps: no acknowledgement (node:dump) @missing within 60 s: "
  . "a node that finds a leaked record aborts; see its ndb_<id>_out.log\n"
  if @missing;
print "structural leak checks acknowledged by all " . scalar(@nodes)
    . " data nodes\n";
EOF
