# HTTP helpers for rdrs2-golang_node_outage.test (RONDB-1104).
#
# Talks to the rdrs.1.1 REST server started by MTR. The port is read from
# the generated rdrs.1.1_config.json in the var directory. HTTP::Tiny is
# core perl, so no extra dependency.

use strict;
use warnings;
use HTTP::Tiny;

my $rdrs_port;

sub _rdrs_port {
  return $rdrs_port if defined $rdrs_port;
  my $cfg = "$ENV{MYSQLTEST_VARDIR}/rdrs.1.1_config.json";
  open(my $fh, '<', $cfg) or die "Cannot open $cfg: $!\n";
  local $/;
  my $json = <$fh>;
  close($fh);
  ($rdrs_port) = $json =~ /"ServerPort"\s*:\s*(\d+)/
    or die "No ServerPort found in $cfg\n";
  return $rdrs_port;
}

sub _health_status {
  my $ua = HTTP::Tiny->new(timeout => 5);
  my $res = $ua->get('http://127.0.0.1:' . _rdrs_port() . '/0.1.0/health');
  return $res->{status};
}

# Fire a pk-read with a well-formed but nonexistent API key. The key
# lookup reads RonDB through the metadata connection, so during an outage
# this exercises the production reconnection trigger. The response is
# deliberately ignored.
sub _poke_pkread {
  my $ua = HTTP::Tiny->new(timeout => 5);
  my $key = ('X' x 16) . '.' . ('x' x 64);
  $ua->post(
    'http://127.0.0.1:' . _rdrs_port() . '/0.1.0/nodb/notab/pk-read',
    { headers => { 'Content-Type' => 'application/json',
                   'X-API-KEY' => $key },
      content => '{"filters":[{"column":"id","value":1}]}' });
}

sub _poll_health {
  my ($want, $max_seconds, $what, $poke) = @_;
  my $last = 'none';
  for (my $i = 0; $i < $max_seconds; $i++) {
    _poke_pkread() if $poke;
    $last = _health_status();
    return if $last == $want;
    sleep(1);
  }
  die "Timeout waiting for $what: wanted HTTP $want, last got $last\n";
}

sub poll_health {
  my ($want, $max_seconds, $what) = @_;
  _poll_health($want, $max_seconds, $what, 0);
}

sub poll_health_with_poke {
  my ($want, $max_seconds, $what) = @_;
  _poll_health($want, $max_seconds, $what, 1);
}

1;
