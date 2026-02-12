# suite/ndb_ttl_purge/include/ttl_purge_wait_table_detected.inc
#
# Poll GET /tables/{db}/{table} until the table appears in the purge API.
#
# Input:
#   $wait_db    - Database name
#   $wait_table - Table name
#
# Requires: ttl_purge_setup.inc sourced

--let $_tbl_detect_met = 0
--let $_tbl_detect_i = 0

while (!$_tbl_detect_met)
{
  --exec curl -s -o $MYSQL_TMP_DIR/_tbl_detect.json -w "%{http_code}" $ttl_purge_base/tables/$wait_db/$wait_table > $MYSQL_TMP_DIR/_tbl_detect_code.txt 2>/dev/null

  --perl
  my $tmp = $ENV{MYSQL_TMP_DIR};
  open my $f, '<', "$tmp/_tbl_detect_code.txt" or die;
  my $code = <$f>; chomp $code; close $f;
  open my $o, '>', "$tmp/_tbl_detect_check.inc" or die;
  print $o "--let \$_tbl_detect_met = " . ($code eq '200' ? 1 : 0) . "\n";
  close $o;
  EOF
  --source $MYSQL_TMP_DIR/_tbl_detect_check.inc

  if (!$_tbl_detect_met)
  {
    --inc $_tbl_detect_i
    if ($_tbl_detect_i >= 20)
    {
      --die Timeout waiting for table $wait_db.$wait_table to be detected by purge API
    }
    --sleep 2
  }
}
