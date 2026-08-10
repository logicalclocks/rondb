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
  # Anchor on the REST object: a config with Rondis enabled has a second
  # "ServerPort" and the bare key could match the wrong one.
  ($rdrs_port) = $json =~ /"REST"\s*:\s*\{[^{}]*"ServerPort"\s*:\s*(\d+)/
    or die "No REST.ServerPort found in $cfg\n";
  return $rdrs_port;
}

sub _health_status {
  my $ua = HTTP::Tiny->new(timeout => 5);
  my $res = $ua->get('http://127.0.0.1:' . _rdrs_port() . '/0.1.0/health');
  return $res->{status};
}

# Fire a pk-read with a well-formed but nonexistent API key. The key
# lookup reads RonDB through the metadata connection, so during an outage
# this exercises the production reconnection trigger. The status may be
# 401/500/503/599 depending on phase - but it must NEVER be 200 or 404:
# 200 or 404 for a data read while the cluster cannot answer is exactly
# the answer-laundering bug RONDB-1104 fixes.
sub _poke_pkread {
  my $ua = HTTP::Tiny->new(timeout => 5);
  my $key = ('X' x 16) . '.' . ('x' x 64);
  my $res = $ua->post(
    'http://127.0.0.1:' . _rdrs_port() . '/0.1.0/nodb/notab/pk-read',
    { headers => { 'Content-Type' => 'application/json',
                   'X-API-KEY' => $key },
      content => '{"filters":[{"column":"id","value":1}]}' });
  my $st = $res->{status};
  if ($st == 200 || $st == 404) {
    die "pk-read poke returned HTTP $st: a cluster outage must surface " .
        "as an error, not as success or 'not found'\n";
  }
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
