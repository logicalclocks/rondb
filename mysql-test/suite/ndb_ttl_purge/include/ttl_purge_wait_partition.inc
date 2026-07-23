# suite/ndb_ttl_purge/include/ttl_purge_wait_partition.inc
#
# Read (or wait for a change of) a table's purge rotation pointer via
# GET /tables/{db}/{table} "current_partition".
#
# The entire poll (curl + JSON parse + sleep) runs inside ONE --perl block,
# which writes a single result file that is --source'd ONCE afterwards (see
# ttl_purge_wait_state.inc for why the rewrite-and-re-source-per-iteration
# pattern must not be used: mysqltest replays stale cached content).
#
# Input:
#   $wait_db          - Database name
#   $wait_table       - Table name
#   $partition_prev   - optional: poll (up to ~40s) until current_partition
#                       differs from this value. Empty/unset: return a value
#                       immediately (subject to $partition_stable).
#   $partition_stable - optional: poll until two reads 2s apart return the
#                       SAME value and return that. Use after disabling the
#                       purger: the 'disabled' status is set inside the table
#                       loop BEFORE the round's final metrics publication, so
#                       an immediate read can race one last update.
# Output:
#   $partition_value - the observed current_partition
#   $partition_next  - (current_partition + 1) % partition_count, i.e. the
#                      expected next published value if rotation continues
#
# Requires: ttl_purge_setup.inc sourced

--exec echo '$ttl_purge_base/tables/$wait_db/$wait_table' > $MYSQL_TMP_DIR/_part_url.txt
--exec echo '$partition_prev' > $MYSQL_TMP_DIR/_part_prev.txt
--exec echo '$partition_stable' > $MYSQL_TMP_DIR/_part_stable.txt

--perl
use strict;
use warnings;
use JSON::PP;
my $tmp = $ENV{MYSQL_TMP_DIR};
sub rd { open my $f, '<', $_[0] or return ''; my $l = <$f>; close $f; return '' unless defined $l; chomp $l; return $l; }
my $url    = rd("$tmp/_part_url.txt");
my $prev   = rd("$tmp/_part_prev.txt");
my $stable = rd("$tmp/_part_stable.txt");
sub read_part {
  my $json = `curl -s $url 2>/dev/null`;
  return ('', 0) unless defined $json && $json ne '';
  my $d = eval { decode_json($json) };
  return ('', 0) unless $d && defined $d->{current_partition};
  my $c = defined $d->{partition_count} ? $d->{partition_count} : 0;
  return ($d->{current_partition}, $c);
}
my ($val, $cnt) = ('', 0);
my $ok = 0;
for (my $i = 0; $i < 40; $i++) {
  my ($v, $c) = read_part();
  if ($v ne '') {
    ($val, $cnt) = ($v, $c);
    if ($prev eq '' || "$val" ne "$prev") {
      if ($stable) {
        sleep 2;
        my ($v2, $c2) = read_part();
        if ($v2 ne '' && "$v2" eq "$val") { $ok = 1; last; }
        next;  # still moving (or read failed): keep polling
      }
      $ok = 1;
      last;
    }
  }
  sleep 1;
}
open my $o, '>', "$tmp/_part_result.inc" or die "Cannot write: $!";
if ($ok) {
  my $next = ($cnt && $cnt > 0) ? (($val + 1) % $cnt) : '';
  print $o "--let \$partition_value = $val\n";
  print $o "--let \$partition_next = $next\n";
  print $o "--let \$_part_met = 1\n";
} else {
  print $o "--echo [ttl_purge_wait_partition] TIMEOUT (prev='$prev', stable='$stable', last seen: '$val')\n";
  print $o "--let \$_part_met = 0\n";
}
close $o;
EOF
--source $MYSQL_TMP_DIR/_part_result.inc

if (!$_part_met)
{
  --die Timeout waiting for purge partition rotation
}

--let $_part_met =
--let $partition_prev =
--let $partition_stable =
