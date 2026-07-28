# suite/ndb_ttl_purge/include/ttl_purge_wait_state.inc
#
# Poll the TTL purge /status endpoint until its "state" field equals a target
# value, or fail after a timeout. Use this instead of a fixed --sleep when
# asserting an asynchronous state transition (e.g. enabled->disabled): the
# purge worker only updates its reported state at a round boundary, so a hard
# sleep races with the worker and flakes under load (ASan / parallel MTR).
#
# The entire poll (curl + JSON parse + sleep) runs inside ONE --perl block,
# which writes a single result file that is --source'd ONCE afterwards. This
# deliberately avoids regenerating a fixed-name file and re-`--source`ing it
# once per loop iteration to pass a value back: re-sourcing a rewritten
# same-name file in a tight loop yields a STALE value, which previously made
# this poll compare a stale state forever and time out spuriously even though
# /status had already flipped. The success path emits no output, so .result
# is unchanged; on timeout the generated file --echo's the last-seen state and
# $ws_state_reached stays 0, so the --die below fails the test with context.
#
# Input:
#   $wait_state_target  - desired state string, e.g. disabled / running / paused
#   $wait_state_timeout - max seconds to wait (default: 30)
#
# Requires $ttl_purge_base (set by ttl_purge_setup.inc).

if (!$wait_state_timeout)
{
  --let $wait_state_timeout = 30
}

--exec echo '$wait_state_target' > $MYSQL_TMP_DIR/_ws_target.txt
--exec echo '$wait_state_timeout' > $MYSQL_TMP_DIR/_ws_timeout.txt
--exec echo '$ttl_purge_base' > $MYSQL_TMP_DIR/_ws_base.txt

--perl
use strict;
use warnings;
use JSON::PP;
my $tmp = $ENV{MYSQL_TMP_DIR};
sub rd { open my $f, '<', $_[0] or return ''; my $l = <$f>; chomp $l; close $f; return $l; }
my $target  = rd("$tmp/_ws_target.txt");
my $timeout = rd("$tmp/_ws_timeout.txt") || 30;
my $base    = rd("$tmp/_ws_base.txt");
my $url     = "$base/status";
my $state   = '';
my $ok      = 0;
for (my $i = 0; $i < $timeout; $i++) {
  my $json = `curl -s $url 2>/dev/null`;
  if (defined $json && $json ne '') {
    my $d = eval { decode_json($json) };
    if ($d && defined $d->{state}) { $state = $d->{state}; }
  }
  if ($state eq $target) { $ok = 1; last; }
  sleep 1;
}
open my $o, '>', "$tmp/_ws_result.inc" or die "Cannot write: $!";
if ($ok) {
  print $o "--let \$ws_state_reached = 1\n";
} else {
  print $o "--echo [ttl_purge_wait_state] TIMEOUT after ${timeout}s waiting for state='$target' (last seen: '$state')\n";
  print $o "--let \$ws_state_reached = 0\n";
}
close $o;
EOF
--source $MYSQL_TMP_DIR/_ws_result.inc

if (!$ws_state_reached)
{
  --die TTL purge did not reach the expected state within the timeout
}

--let $wait_state_timeout =
--let $ws_state_reached =
