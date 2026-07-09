# suite/ndb_ttl_purge/include/ttl_purge_wait_table_detected.inc
#
# Poll GET /tables/{db}/{table} until the table appears in the purge API
# (HTTP 200), or fail after a timeout (~40s).
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

--exec echo '$ttl_purge_base/tables/$wait_db/$wait_table' > $MYSQL_TMP_DIR/_tbl_detect_url.txt

--perl
use strict;
use warnings;
my $tmp = $ENV{MYSQL_TMP_DIR};
open my $f, '<', "$tmp/_tbl_detect_url.txt" or die "Cannot read url file: $!";
my $url = <$f>; chomp $url; close $f;
my $code = '';
my $ok = 0;
for (my $i = 0; $i < 20; $i++) {
  $code = `curl -s -o /dev/null -w "%{http_code}" $url 2>/dev/null`;
  chomp $code;
  if ($code eq '200') { $ok = 1; last; }
  sleep 2;
}
open my $o, '>', "$tmp/_tbl_detect_result.inc" or die "Cannot write: $!";
if ($ok) {
  print $o "--let \$_tbl_detect_met = 1\n";
} else {
  print $o "--echo [ttl_purge_wait_table_detected] TIMEOUT waiting for '$url' (last code: '$code')\n";
  print $o "--let \$_tbl_detect_met = 0\n";
}
close $o;
EOF
--source $MYSQL_TMP_DIR/_tbl_detect_result.inc

if (!$_tbl_detect_met)
{
  --die Timeout waiting for table to be detected by purge API
}

--let $_tbl_detect_met =
