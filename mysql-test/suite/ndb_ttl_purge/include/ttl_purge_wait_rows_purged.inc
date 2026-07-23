# suite/ndb_ttl_purge/include/ttl_purge_wait_rows_purged.inc
#
# Poll GET /metrics until rows_purged_total >= $expected_purge_total
# (i.e., the cumulative total reaches or exceeds the target).
#
# The entire poll (curl + JSON parse + sleep) runs inside ONE --perl block,
# which writes a single result file that is --source'd ONCE afterwards. This
# deliberately avoids regenerating a fixed-name file and re-`--source`ing it
# once per while-iteration to pass a value back: mysqltest caches sourced
# files (do_source / q_lines), so a while-loop replay re-executes the FIRST
# iteration's cached content and never sees the rewritten value -- the loop
# then times out spuriously even though the metric already advanced.
# (Same rationale as ttl_purge_wait_state.inc.)
#
# Input:
#   $expected_purge_total - Target value for rows_purged_total
#   $purge_wait_timeout   - Timeout in iterations (default: 30, ~60s at 2s/iter)
#
# Requires: ttl_purge_setup.inc sourced

if (!$purge_wait_timeout)
{
  --let $purge_wait_timeout = 30
}

--exec echo '$ttl_purge_base/metrics' > $MYSQL_TMP_DIR/_purge_wait_url.txt
--exec echo '$expected_purge_total' > $MYSQL_TMP_DIR/_purge_target.txt
--exec echo '$purge_wait_timeout' > $MYSQL_TMP_DIR/_purge_timeout.txt

--perl
use strict;
use warnings;
use JSON::PP;
my $tmp = $ENV{MYSQL_TMP_DIR};
sub rd { open my $f, '<', $_[0] or return ''; my $l = <$f>; chomp $l; close $f; return $l; }
my $url     = rd("$tmp/_purge_wait_url.txt");
my $target  = rd("$tmp/_purge_target.txt") || 0;
my $timeout = rd("$tmp/_purge_timeout.txt") || 30;
my $cur     = '';
my $ok      = 0;
for (my $i = 0; $i < $timeout; $i++) {
  my $json = `curl -s $url 2>/dev/null`;
  if (defined $json && $json ne '') {
    my $d = eval { decode_json($json) };
    if ($d && defined $d->{rows_purged_total}) { $cur = $d->{rows_purged_total}; }
  }
  if ($cur ne '' && $cur >= $target) { $ok = 1; last; }
  sleep 2;
}
open my $o, '>', "$tmp/_purge_wait_result.inc" or die "Cannot write: $!";
if ($ok) {
  print $o "--let \$_purge_wait_met = 1\n";
} else {
  print $o "--echo [ttl_purge_wait_rows_purged] TIMEOUT waiting for rows_purged_total >= $target (last seen: '$cur')\n";
  print $o "--let \$_purge_wait_met = 0\n";
}
close $o;
EOF
--source $MYSQL_TMP_DIR/_purge_wait_result.inc

if (!$_purge_wait_met)
{
  --die Timeout waiting for rows to be purged
}

--let $_purge_wait_met =
--let $purge_wait_timeout =
