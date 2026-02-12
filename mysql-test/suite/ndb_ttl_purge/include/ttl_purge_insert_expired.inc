# suite/ndb_ttl_purge/include/ttl_purge_insert_expired.inc
#
# Insert already-expired rows into a TTL table.
# Uses DATE_SUB(NOW(), INTERVAL 5 MINUTE) so rows are immediately expired.
#
# Input:
#   $insert_db       - Database name
#   $insert_table    - Table name
#   $insert_count    - Number of rows to insert
#   $insert_start_id - Starting id value (default: 1)

if (!$insert_start_id)
{
  --let $insert_start_id = 1
}

--exec echo '$insert_db' > $MYSQL_TMP_DIR/_ins_exp_db.txt
--exec echo '$insert_table' > $MYSQL_TMP_DIR/_ins_exp_tbl.txt
--exec echo '$insert_count' > $MYSQL_TMP_DIR/_ins_exp_count.txt
--exec echo '$insert_start_id' > $MYSQL_TMP_DIR/_ins_exp_start.txt

--perl
my $tmp = $ENV{MYSQL_TMP_DIR};
sub rf { open my $f,'<',$_[0] or return ''; my $l=<$f>; chomp $l; close $f; $l }
my $db = rf("$tmp/_ins_exp_db.txt");
my $tbl = rf("$tmp/_ins_exp_tbl.txt");
my $count = rf("$tmp/_ins_exp_count.txt") || 1;
my $start = rf("$tmp/_ins_exp_start.txt") || 1;

open my $o, '>', "$tmp/_ins_exp_stmts.inc" or die;
for my $i (0 .. $count - 1) {
  my $id = $start + $i;
  print $o "--eval INSERT INTO $db.$tbl (id, ttl_col, data) VALUES ($id, DATE_SUB(NOW(), INTERVAL 5 MINUTE), 'expired_row_$id')\n";
}
close $o;
EOF

--source $MYSQL_TMP_DIR/_ins_exp_stmts.inc

--let $insert_start_id =
