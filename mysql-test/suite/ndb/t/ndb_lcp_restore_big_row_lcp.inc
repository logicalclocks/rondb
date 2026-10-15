# Takes an LCP with dump 7099 and waits until it has completed.
#
# An LCP that was running at the dump may have checkpointed a fragment
# before its newest rows were written, so wait for an LCP that started
# after the dump: "Local checkpoint N completed" in the cluster log with N
# above the newest "Local checkpoint N started" before it. The dump is
# repeated while waiting since it does nothing while an LCP is running.
#
# Writes the time of the dump to
# $MYSQLTEST_VARDIR/tmp/ndb_lcp_restore_big_row.since, LCP data files
# written by this LCP are not older than that.

let BIG_ROW_VARDIR = $MYSQLTEST_VARDIR;
--perl
use strict;
my $vardir = $ENV{BIG_ROW_VARDIR};
my @logs = glob("$vardir/mysql_cluster.1/ndb_mgmd.*/*_cluster.log");
die "No cluster log found\n" unless @logs;

sub newest_lcp {
  my ($event) = @_;
  my $max = 0;
  foreach my $f (@logs) {
    open(my $fh, '<', $f) or die "open $f: $!";
    while (<$fh>) {
      $max = $1 if /Local checkpoint (\d+) $event/ && $1 > $max;
    }
    close($fh);
  }
  return $max;
}

my $started = newest_lcp('started');
open(my $out, '>', "$vardir/tmp/ndb_lcp_restore_big_row.since")
  or die "open since file: $!";
print $out time(), "\n";
close($out);

for (my $i = 0; $i < 240; $i++) {
  system("$ENV{NDB_MGM} -e \"all dump 7099\" > /dev/null 2>&1")
    if $i % 20 == 0;
  exit(0) if newest_lcp('completed') > $started;
  select(undef, undef, undef, 0.5);
}
die "No LCP that started after LCP $started completed within 120 s\n";
EOF
