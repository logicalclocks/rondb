#
# Write a minimal rdrs2 JSON config file for the rdrs2 binary.
# The config connects to the NDB cluster via the management server.
#
# Requires: NDB_MGMD_PORT environment variable
#           $rdrs2_node_ids mysqltest variable written to
#           $MYSQLTEST_VARDIR/tmp/rdrs2_node_ids.txt before sourcing
# Creates:  $MYSQLTEST_VARDIR/tmp/rdrs2_config.json
#           $MYSQLTEST_VARDIR/tmp/rdrs2.pid (at rdrs2 startup)
#

--perl
  use strict;
  use IO::Socket::INET;

  my $vardir = $ENV{'MYSQLTEST_VARDIR'}
    or die "MYSQLTEST_VARDIR not set";
  my $mgmd_port = $ENV{'NDB_MGMD_PORT'}
    or die "NDB_MGMD_PORT not set";

  # Kill any stale rdrs2 from a previous interrupted run
  my $pid_file = "$vardir/tmp/rdrs2.pid";
  if (-f $pid_file) {
    open(my $pfh, '<', $pid_file);
    if ($pfh) {
      my $old_pid = <$pfh>;
      close $pfh;
      chomp $old_pid if defined $old_pid;
      if (defined $old_pid && $old_pid > 0 && kill(0, $old_pid)) {
        kill('TERM', $old_pid);
        for (my $i = 0; $i < 100; $i++) {
          last unless kill(0, $old_pid);
          select(undef, undef, undef, 0.1);
        }
      }
      unlink $pid_file;
    }
  }

  my $config_file = "$vardir/tmp/rdrs2_config.json";

  # Read node IDs from file written by the test
  my $node_ids_str = "";
  my $node_ids_file = "$vardir/tmp/rdrs2_node_ids.txt";
  if (-f $node_ids_file) {
    open(my $nfh, '<', $node_ids_file) or die "Cannot read $node_ids_file: $!";
    $node_ids_str = <$nfh>;
    close $nfh;
    chomp $node_ids_str if defined $node_ids_str;
  }

  # Build NodeIDs array if specified
  my $node_ids_config = "";
  if ($node_ids_str && $node_ids_str ne "") {
    $node_ids_config = qq(,\n    "NodeIDs": [$node_ids_str]);
  }

  # Find a free port for the REST server
  my $sock = IO::Socket::INET->new(
    LocalAddr => 'localhost',
    LocalPort => 0,
    Proto     => 'tcp',
    Listen    => 1,
    ReuseAddr => 1,
  ) or die "Cannot create socket to find free port: $!";
  my $rest_port = $sock->sockport();
  $sock->close();

  my $config = qq({
  "PIDFile": "$pid_file",
  "REST": {
    "ServerIP": "localhost",
    "ServerPort": $rest_port
  },
  "RonDB": {
    "Mgmds": [{"IP": "localhost", "Port": $mgmd_port}]$node_ids_config,
    "ConnectionRetries": 5,
    "ConnectionRetryDelayInSec": 1
  },
  "Security": {
    "InsecureAllowAll": true,
    "TLS": {
      "EnableTLS": false,
      "RequireAndVerifyClientCert": false
    },
    "APIKey": {
      "UseHopsworksAPIKeys": false
    }
  },
  "Log": {
    "Level": "INFO"
  }
});

  open(my $fh, '>', $config_file)
    or die "Cannot open $config_file for writing: $!";
  print $fh $config;
  close $fh;
EOF
