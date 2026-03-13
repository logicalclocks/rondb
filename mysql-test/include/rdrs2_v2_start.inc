#
# Start the rdrs2_v2 server in the background and wait for it to be ready.
#
# Requires:
#   RDRS2_V2_EXE environment variable (path to rdrs2_v2 binary)
#   Config file at $MYSQLTEST_VARDIR/tmp/rdrs2_v2_config.json
#     (created by rdrs2_v2_write_config.inc)
#

--exec_in_background $RDRS2_V2_EXE --config $MYSQLTEST_VARDIR/tmp/rdrs2_v2_config.json > $MYSQLTEST_VARDIR/log/rdrs2_v2.log 2>&1

# Wait for the PID file to appear and verify the process is running
--perl
  use strict;
  my $pid_file = "$ENV{'MYSQLTEST_VARDIR'}/tmp/rdrs2_v2.pid";
  my $timeout = 30;  # seconds
  for (my $i = 0; $i < $timeout * 10; $i++) {
    if (-f $pid_file) {
      open(my $fh, '<', $pid_file) or next;
      my $pid = <$fh>;
      close $fh;
      chomp $pid if defined $pid;
      if (defined $pid && $pid > 0 && kill(0, $pid)) {
        exit 0;
      }
    }
    select(undef, undef, undef, 0.1);
  }
  die "Timeout waiting for rdrs2_v2 to start (PID file: $pid_file)";
EOF
