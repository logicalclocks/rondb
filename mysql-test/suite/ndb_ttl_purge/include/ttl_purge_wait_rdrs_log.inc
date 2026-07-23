# suite/ndb_ttl_purge/include/ttl_purge_wait_rdrs_log.inc
#
# Poll (up to ~40s) until the RDRS log content appended since the last
# ttl_purge_mark_rdrs_log.inc matches a perl regex. Requires that mark
# include to have been sourced first.
#
# The entire poll runs inside ONE --perl block whose result file is sourced
# ONCE (see ttl_purge_wait_state.inc for why the rewrite-and-re-source-per-
# iteration pattern must not be used).
#
# Input:
#   $log_pattern - perl regex to search for in the appended log content

--exec echo '$log_pattern' > $MYSQL_TMP_DIR/_rdrs_log_pat.txt

--perl
use strict;
use warnings;
my $tmp = $ENV{MYSQL_TMP_DIR};
my $log = "$ENV{MYSQLTEST_VARDIR}/log/rdrs.1.1.log";
sub rd { open my $f, '<', $_[0] or return ''; my $l = <$f>; close $f; return '' unless defined $l; chomp $l; return $l; }
my $pat    = rd("$tmp/_rdrs_log_pat.txt");
my $offset = rd("$tmp/_rdrs_log_offset.txt") || 0;
my $ok = 0;
for (my $i = 0; $i < 40; $i++) {
  if (open my $f, '<', $log) {
    seek($f, $offset, 0);
    local $/;
    my $c = <$f>;
    close $f;
    if (defined $c && $c =~ /$pat/) { $ok = 1; last; }
  }
  sleep 1;
}
open my $o, '>', "$tmp/_rdrs_log_check.inc" or die "Cannot write: $!";
if ($ok) {
  print $o "--let \$_rdrs_log_met = 1\n";
} else {
  print $o "--echo [ttl_purge_wait_rdrs_log] TIMEOUT waiting for pattern '$pat' after offset $offset\n";
  print $o "--let \$_rdrs_log_met = 0\n";
}
close $o;
EOF
--source $MYSQL_TMP_DIR/_rdrs_log_check.inc

if (!$_rdrs_log_met)
{
  --die Timeout waiting for expected RDRS log line
}

--let $_rdrs_log_met =
--let $log_pattern =
