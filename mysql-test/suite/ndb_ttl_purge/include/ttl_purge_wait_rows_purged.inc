# suite/ndb_ttl_purge/include/ttl_purge_wait_rows_purged.inc
#
# Poll GET /metrics until rows_purged_total >= $expected_purge_total
# (i.e., the cumulative total reaches or exceeds the target).
#
# Input:
#   $expected_purge_total - Target value for rows_purged_total
#   $purge_wait_timeout   - Timeout in iterations (default: 30, ~60s at 2s/iter)
#
# Requires: ttl_purge_setup.inc sourced

if (!$purge_wait_timeout)
{
  --let $purge_wait_timeout = 30
}

--let $_purge_wait_met = 0
--let $_purge_wait_i = 0

while (!$_purge_wait_met)
{
  --exec $ttl_purge_get_metrics > $MYSQL_TMP_DIR/_purge_poll.json 2>/dev/null

  --let $json_file = $MYSQL_TMP_DIR/_purge_poll.json
  --let $json_field = rows_purged_total
  --source suite/ndb_ttl_purge/include/ttl_purge_json_parse.inc

  --exec echo '$json_value' > $MYSQL_TMP_DIR/_purge_current.txt
  --exec echo '$expected_purge_total' > $MYSQL_TMP_DIR/_purge_target.txt

  --perl
  my $tmp = $ENV{MYSQL_TMP_DIR};
  sub rf { open my $f,'<',$_[0] or return 0; my $l=<$f>; chomp $l; close $f; $l }
  my $cur = rf("$tmp/_purge_current.txt") || 0;
  my $tgt = rf("$tmp/_purge_target.txt") || 0;
  open my $o, '>', "$tmp/_purge_wait_check.inc" or die;
  print $o "--let \$_purge_wait_met = " . ($cur >= $tgt ? 1 : 0) . "\n";
  close $o;
  EOF
  --source $MYSQL_TMP_DIR/_purge_wait_check.inc

  if (!$_purge_wait_met)
  {
    --inc $_purge_wait_i
    if ($_purge_wait_i >= $purge_wait_timeout)
    {
      --echo Expected rows_purged_total >= $expected_purge_total, got $json_value
      --die Timeout waiting for rows to be purged
    }
    --sleep 2
  }
}

--let $purge_wait_timeout =
