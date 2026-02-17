# suite/ndb_ttl_purge/include/ttl_purge_insert_valid.inc
#
# Insert valid (non-expired) rows into a TTL table.
# Uses DATE_ADD(NOW(), INTERVAL 1 HOUR) so rows will not expire soon.
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

--exec echo '$insert_db' > $MYSQL_TMP_DIR/_ins_val_db.txt
--exec echo '$insert_table' > $MYSQL_TMP_DIR/_ins_val_tbl.txt
--exec echo '$insert_count' > $MYSQL_TMP_DIR/_ins_val_count.txt
--exec echo '$insert_start_id' > $MYSQL_TMP_DIR/_ins_val_start.txt

--perl
my $tmp = $ENV{MYSQL_TMP_DIR};
sub rf { open my $f,'<',$_[0] or return ''; my $l=<$f>; chomp $l; close $f; $l }
my $db = rf("$tmp/_ins_val_db.txt");
my $tbl = rf("$tmp/_ins_val_tbl.txt");
my $count = rf("$tmp/_ins_val_count.txt") || 1;
my $start = rf("$tmp/_ins_val_start.txt") || 1;

open my $o, '>', "$tmp/_ins_val_stmts.inc" or die;
for my $i (0 .. $count - 1) {
  my $id = $start + $i;
  print $o "--eval INSERT INTO $db.$tbl (id, ttl_col, data) VALUES ($id, DATE_ADD(NOW(), INTERVAL 1 HOUR), 'valid_row_$id')\n";
}
close $o;
EOF

--source $MYSQL_TMP_DIR/_ins_val_stmts.inc

--let $insert_start_id =
