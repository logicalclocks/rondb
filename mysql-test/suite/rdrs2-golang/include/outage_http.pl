# HTTP helpers for rdrs2-golang_node_outage.test (RONDB-1104).
#
# Talks to two REST servers started by MTR:
#   rdrs.1.1 - API keys enabled; used for /health and for pokes whose key
#              lookup exercises the production reconnection trigger.
#   rdrs.3.1 - InsecureAllowAll; used to assert the DATA path (pk-read of a
#              real row) before, during and after the outage.
# Ports are read from the generated <name>_config.json files in the var
# directory. HTTP::Tiny is core perl, so no extra dependency.

use strict;
use warnings;
use HTTP::Tiny;

my %rdrs_rest_key;

# Extract one key from the REST object of the generated config. The block is
# isolated first: Rondis has its own "ServerPort", and REST itself holds two
# ports (ServerPort and ProbePort), so a bare key match could hit the wrong
# one.
sub _rdrs_rest_key {
  my ($server, $key) = @_;
  return $rdrs_rest_key{"$server.$key"}
    if defined $rdrs_rest_key{"$server.$key"};
  my $cfg = "$ENV{MYSQLTEST_VARDIR}/${server}_config.json";
  open(my $fh, '<', $cfg) or die "Cannot open $cfg: $!\n";
  local $/;
  my $json = <$fh>;
  close($fh);
  my ($block) = $json =~ /"REST"\s*:\s*\{([^{}]*)\}/
    or die "No REST object found in $cfg\n";
  ($rdrs_rest_key{"$server.$key"}) = $block =~ /"\Q$key\E"\s*:\s*(\d+)/
    or die "No REST.$key found in $cfg\n";
  return $rdrs_rest_key{"$server.$key"};
}

sub _rdrs_port {
  my ($server) = @_;
  return _rdrs_rest_key($server, 'ServerPort');
}

# The dedicated probe listener of the same server (REST.ProbePort). Serves
# only ping and health, never authenticated, and must answer even when the
# main port's threads are all blocked.
sub _rdrs_probe_port {
  my ($server) = @_;
  return _rdrs_rest_key($server, 'ProbePort');
}

sub _health_status {
  my $ua = HTTP::Tiny->new(timeout => 5);
  my $res = $ua->get(
    'http://127.0.0.1:' . _rdrs_port('rdrs.1.1') . '/0.1.0/health');
  return $res->{status};
}

sub _probe_health_status {
  my $ua = HTTP::Tiny->new(timeout => 5);
  my $res = $ua->get(
    'http://127.0.0.1:' . _rdrs_probe_port('rdrs.1.1') . '/0.1.0/health');
  return $res->{status};
}

sub _probe_ping_status {
  my $ua = HTTP::Tiny->new(timeout => 5);
  my $res = $ua->get(
    'http://127.0.0.1:' . _rdrs_probe_port('rdrs.1.1') . '/0.1.0/ping');
  return $res->{status};
}

# Fire a pk-read at the auth-enabled server with a well-formed but
# nonexistent API key. The key lookup reads RonDB through the metadata
# connection, so during an outage this exercises the production
# reconnection trigger. The status may be 401/500/503/599 depending on
# phase - but it must NEVER be 200 or 404: 200 or 404 for a data read
# while the cluster cannot answer is exactly the answer-laundering bug
# RONDB-1104 fixes.
sub _poke_pkread {
  my $ua = HTTP::Tiny->new(timeout => 5);
  my $key = ('X' x 16) . '.' . ('x' x 64);
  my $res = $ua->post(
    'http://127.0.0.1:' . _rdrs_port('rdrs.1.1') . '/0.1.0/nodb/notab/pk-read',
    { headers => { 'Content-Type' => 'application/json',
                   'X-API-KEY' => $key },
      content => '{"filters":[{"column":"id","value":1}]}' });
  my $st = $res->{status};
  if ($st == 200 || $st == 404) {
    die "pk-read poke returned HTTP $st: a cluster outage must surface " .
        "as an error, not as success or 'not found'\n";
  }
}

# pk-read the row the test created, through the no-auth server. Returns
# the HTTP status; on 200 also verifies the row content, so a recovered
# server is proven to serve correct data, not just to answer.
sub _data_read {
  my $ua = HTTP::Tiny->new(timeout => 5);
  my $res = $ua->post(
    'http://127.0.0.1:' . _rdrs_port('rdrs.3.1') .
      '/0.1.0/rdrs_outage/t1/pk-read',
    { headers => { 'Content-Type' => 'application/json' },
      content => '{"filters":[{"column":"id","value":1}]}' });
  my $st = $res->{status};
  if ($st == 200 && $res->{content} !~ /"val"\s*:\s*42/) {
    die "data pk-read returned 200 but not the expected row: " .
        $res->{content} . "\n";
  }
  return $st;
}

sub _poll {
  my ($what, $max_seconds, $check) = @_;
  my $last = 'none';
  for (my $i = 0; $i < $max_seconds; $i++) {
    $last = $check->();
    return if $last eq 'ok';
    sleep(1);
  }
  die "Timeout waiting for $what: last state: $last\n";
}

sub poll_health {
  my ($want, $max_seconds, $what) = @_;
  _poll($what, $max_seconds, sub {
    my $st = _health_status();
    return $st == $want ? 'ok' : "health=$st";
  });
}

# Both-port invariant, checked at every health checkpoint:
# - /ping on the probe port answers 200 no matter what state the cluster is
#   in - it is the liveness probe and must never fail.
# - /health must reach the wanted status on BOTH the main port and the probe
#   port. The two read the same state, so they must agree; polling until
#   both agree tolerates the instant where one has seen a transition the
#   other has not.
sub poll_health_both {
  my ($want, $max_seconds, $what) = @_;
  _poll($what, $max_seconds, sub {
    my $ping = _probe_ping_status();
    die "probe-port ping returned HTTP $ping: the probe port must always"
      . " answer 200\n" if $ping != 200;
    my $main_st = _health_status();
    my $probe_st = _probe_health_status();
    return ($main_st == $want && $probe_st == $want)
      ? 'ok' : "health=$main_st probe_health=$probe_st";
  });
}

sub poll_health_with_poke {
  my ($want, $max_seconds, $what) = @_;
  _poll($what, $max_seconds, sub {
    _poke_pkread();
    my $st = _health_status();
    return $st == $want ? 'ok' : "health=$st";
  });
}

# poll_health_both plus the pk-read poke that exercises the production
# reconnection trigger (see _poke_pkread).
sub poll_health_both_with_poke {
  my ($want, $max_seconds, $what) = @_;
  _poll($what, $max_seconds, sub {
    _poke_pkread();
    my $ping = _probe_ping_status();
    die "probe-port ping returned HTTP $ping: the probe port must always"
      . " answer 200\n" if $ping != 200;
    my $main_st = _health_status();
    my $probe_st = _probe_health_status();
    return ($main_st == $want && $probe_st == $want)
      ? 'ok' : "health=$main_st probe_health=$probe_st";
  });
}

# The row must be readable: proves the data plane serves real data.
sub poll_data_readable {
  my ($max_seconds, $what) = @_;
  _poll($what, $max_seconds, sub {
    my $st = _data_read();
    # The literal RONDB-1104 customer bug: a read during/around an outage
    # must never turn into 'row/table not found'.
    die "data pk-read returned HTTP 404 for an existing row\n"
      if $st == 404;
    return $st == 200 ? 'ok' : "data=$st";
  });
}

# The read must fail with a server error: proves an outage is reported
# honestly - not as 200 (success) and not as 404 (missing data).
sub poll_data_unavailable {
  my ($max_seconds, $what) = @_;
  _poll($what, $max_seconds, sub {
    my $st = _data_read();
    die "data pk-read returned HTTP $st during the outage: must be a " .
        "server error, not success or 'not found'\n"
      if $st == 200 || $st == 404;
    return $st == 500 ? 'ok' : "data=$st";
  });
}

1;
