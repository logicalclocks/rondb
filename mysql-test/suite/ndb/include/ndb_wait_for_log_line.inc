# ==== Purpose ====
#
# Wait until a line matching a pattern appears in a log file after the
# last line matching a marker, which is how include/assert_grep.inc counts
# with $assert_only_after. Fails the test when the timeout is reached.
#
# Use it in place of a fixed sleep before asserting that a timed event
# (a heartbeat failure, a disconnect) has been logged: the event's time
# varies with the phase of the timer that triggers it and with the load
# on the host, so a sleep sized for the nominal time races with it.
#
# ==== Usage ====
#
# --let $wait_log_file = FILENAME
# --let $wait_log_select = PERL_REGEX
# --let $wait_log_after = PERL_REGEX
# --let $wait_log_timeout = SECONDS
# --source suite/ndb/include/ndb_wait_for_log_line.inc
#
# Parameters:
#
#   $wait_log_file
#     The file to search in.
#
#   $wait_log_select
#     The pattern of the line to wait for.
#
#   $wait_log_after
#     Only lines after the last line matching this pattern count.
#
#   $wait_log_timeout
#     How long to wait, in seconds.
#
# Prints nothing on success.

--let _NWFLL_FILE = $wait_log_file
--let _NWFLL_SELECT = $wait_log_select
--let _NWFLL_AFTER = $wait_log_after
--let _NWFLL_TIMEOUT = $wait_log_timeout

--perl
  use strict;
  use Time::HiRes qw(sleep time);
  my $file = $ENV{'_NWFLL_FILE'} or die "wait_log_file is not set";
  my $select = $ENV{'_NWFLL_SELECT'} or die "wait_log_select is not set";
  my $after = $ENV{'_NWFLL_AFTER'} or die "wait_log_after is not set";
  my $timeout = $ENV{'_NWFLL_TIMEOUT'} or die "wait_log_timeout is not set";
  my $deadline = time() + $timeout;
  while (1) {
    my $count = 0;
    open(my $fd, "<", $file) or die("Unable to open $file: $!\n");
    while (my $line = <$fd>) {
      $count = 0 if $line =~ /$after/;
      $count++ if $line =~ /$select/;
    }
    close($fd) or die("Error closing $file: $!\n");
    last if $count > 0;
    die "Timed out after $timeout s waiting for '$select' after " .
        "'$after' in $file\n" if time() > $deadline;
    sleep(0.1);
  }
EOF
