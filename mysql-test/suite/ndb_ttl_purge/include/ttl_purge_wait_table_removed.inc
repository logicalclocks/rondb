# suite/ndb_ttl_purge/include/ttl_purge_wait_table_removed.inc
#
# Poll GET /tables/{db}/{table} until the table disappears from the purge API
# (a clean HTTP 404), or fail after a timeout (~40s). Only an actual 404 from
# a successful curl counts: transport failures (code 000, empty output) and
# other statuses keep polling, so an RDRS outage cannot fake a removal.
#
# The entire poll (curl + sleep) runs inside ONE --perl block, which writes a
# single result file that is --source'd ONCE afterwards. This deliberately
# avoids regenerating a fixed-name file and re-`--source`ing it once per
# while-iteration to pass a value back: mysqltest caches sourced files
# (do_source / q_lines), so a while-loop replay re-executes the FIRST
# iteration's cached content and never sees the rewritten value -- the loop
# then times out spuriously even though the endpoint already answered.
# (Same rationale as ttl_purge_wait_state.inc.)
#
# Input:
#   $wait_db    - Database name
#   $wait_table - Table name
#
# Requires: ttl_purge_setup.inc sourced

--exec echo '$ttl_purge_base/tables/$wait_db/$wait_table' > $MYSQL_TMP_DIR/_tbl_remove_url.txt

--perl
use strict;
use warnings;
my $tmp = $ENV{MYSQL_TMP_DIR};
open my $f, '<', "$tmp/_tbl_remove_url.txt" or die "Cannot read url file: $!";
my $url = <$f>; chomp $url; close $f;
my $code = '';
my $ok = 0;
for (my $i = 0; $i < 20; $i++) {
  $code = `curl -s -o /dev/null -w "%{http_code}" $url 2>/dev/null`;
  my $rc = $?;
  chomp $code;
  # Table is removed only on a clean HTTP 404; retry anything else.
  if ($rc == 0 && $code eq '404') { $ok = 1; last; }
  sleep 2;
}
open my $o, '>', "$tmp/_tbl_remove_result.inc" or die "Cannot write: $!";
if ($ok) {
  print $o "--let \$_tbl_remove_met = 1\n";
} else {
  print $o "--echo [ttl_purge_wait_table_removed] TIMEOUT waiting for '$url' (last code: '$code')\n";
  print $o "--let \$_tbl_remove_met = 0\n";
}
close $o;
EOF
--source $MYSQL_TMP_DIR/_tbl_remove_result.inc

if (!$_tbl_remove_met)
{
  --die Timeout waiting for table to be removed from purge API
}

--let $_tbl_remove_met =
