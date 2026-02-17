# suite/ndb_ttl_purge/include/ttl_purge_wait_table_removed.inc
#
# Poll GET /tables/{db}/{table} until the table disappears from the purge API.
#
# Input:
#   $wait_db    - Database name
#   $wait_table - Table name
#
# Requires: ttl_purge_setup.inc sourced

--let $_tbl_remove_met = 0
--let $_tbl_remove_i = 0

while (!$_tbl_remove_met)
{
  --exec curl -s -o $MYSQL_TMP_DIR/_tbl_remove.json -w "%{http_code}" $ttl_purge_base/tables/$wait_db/$wait_table > $MYSQL_TMP_DIR/_tbl_remove_code.txt 2>/dev/null

  --perl
  my $tmp = $ENV{MYSQL_TMP_DIR};
  open my $f, '<', "$tmp/_tbl_remove_code.txt" or die;
  my $code = <$f>; chomp $code; close $f;
  open my $o, '>', "$tmp/_tbl_remove_check.inc" or die;
  # Table is removed if we get non-200 (404)
  print $o "--let \$_tbl_remove_met = " . ($code ne '200' ? 1 : 0) . "\n";
  close $o;
  EOF
  --source $MYSQL_TMP_DIR/_tbl_remove_check.inc

  if (!$_tbl_remove_met)
  {
    --inc $_tbl_remove_i
    if ($_tbl_remove_i >= 20)
    {
      --die Timeout waiting for table $wait_db.$wait_table to be removed from purge API
    }
    --sleep 2
  }
}
