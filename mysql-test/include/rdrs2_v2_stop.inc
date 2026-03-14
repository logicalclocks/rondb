#
# Stop the rdrs2_v2 server by sending SIGTERM and waiting for it to exit.
#
# Reads the PID from $MYSQLTEST_VARDIR/tmp/rdrs2_v2.pid
#

--perl
  use strict;
  my $pid_file = "$ENV{'MYSQLTEST_VARDIR'}/tmp/rdrs2_v2.pid";

  if (! -f $pid_file) {
    # Already stopped or never started
    exit 0;
  }

  open(my $fh, '<', $pid_file)
    or die "Cannot open $pid_file: $!";
  my $pid = <$fh>;
  close $fh;
  chomp $pid;
  $pid += 0;

  if ($pid <= 0) {
    die "Invalid PID in $pid_file: $pid";
  }

  # Check if process is still running
  if (!kill(0, $pid)) {
    unlink $pid_file;
    exit 0;
  }

  # Send SIGTERM
  kill('TERM', $pid)
    or die "Cannot send TERM signal to rdrs2_v2 (PID $pid): $!";

  # Wait for process to exit
  my $timeout = 30;  # seconds
  for (my $i = 0; $i < $timeout * 10; $i++) {
    if (!kill(0, $pid)) {
      unlink $pid_file;
      exit 0;
    }
    select(undef, undef, undef, 0.1);
  }
  die "Timeout waiting for rdrs2_v2 (PID $pid) to stop";
EOF
