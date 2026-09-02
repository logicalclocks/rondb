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

my %rdrs_port;

sub _rdrs_port {
  my ($server) = @_;
  return $rdrs_port{$server} if defined $rdrs_port{$server};
  my $cfg = "$ENV{MYSQLTEST_VARDIR}/${server}_config.json";
  open(my $fh, '<', $cfg) or die "Cannot open $cfg: $!\n";
  local $/;
  my $json = <$fh>;
  close($fh);
  # Anchor on the REST object: a config with Rondis enabled has a second
  # "ServerPort" and the bare key could match the wrong one.
  ($rdrs_port{$server}) =
    $json =~ /"REST"\s*:\s*\{[^{}]*"ServerPort"\s*:\s*(\d+)/
    or die "No REST.ServerPort found in $cfg\n";
  return $rdrs_port{$server};
}

sub _health_status {
  my $ua = HTTP::Tiny->new(timeout => 5);
  my $res = $ua->get(
    'http://127.0.0.1:' . _rdrs_port('rdrs.1.1') . '/0.1.0/health');
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

sub poll_health_with_poke {
  my ($want, $max_seconds, $what) = @_;
  _poll($what, $max_seconds, sub {
    _poke_pkread();
    my $st = _health_status();
    return $st == $want ? 'ok' : "health=$st";
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
