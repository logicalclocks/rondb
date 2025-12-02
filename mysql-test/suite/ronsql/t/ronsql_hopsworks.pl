#!/usr/bin/perl
# vim: set fileencoding=utf-8 : -*- coding: utf-8 -*-

use strict;
use warnings;
use utf8;
binmode(STDOUT, ":utf8");

# Data type definitions with SQL value representations
my @data_types = (
  {typename => 'TINYINT',
   values => ['-128', '-1', '0', '1', '127', 'NULL'],
   eq => "val = 0", range => "val >= -1 AND val < 1"},
  {typename => 'TINYINT UNSIGNED',
   values => ['0', '1', '100', '255', 'NULL'],
   eq => "val = 1", range => "val >= 1 AND val < 100"},
  {typename => 'SMALLINT',
   values => ['-32768', '-1', '0', '1', '32767', 'NULL'],
   eq => "val = 0", range => "val >= -1 AND val < 1"},
  {typename => 'SMALLINT UNSIGNED',
   values => ['0', '1', '1000', '65535', 'NULL'],
   eq => "val = 1", range => "val >= 1 AND val < 1000"},
  {typename => 'MEDIUMINT',
   values => ['-8388608', '-1', '0', '1', '8388607', 'NULL'],
   eq => "val = 0", range => "val >= -1 AND val < 1"},
  {typename => 'MEDIUMINT UNSIGNED',
   values => ['0', '1', '10000', '16777215', 'NULL'],
   eq => "val = 1", range => "val >= 1 AND val < 10000"},
  {typename => 'INT',
   values => ['-2147483648', '-1', '0', '1', '2147483647', 'NULL'],
   eq => "val = 0", range => "val >= -1 AND val < 1"},
  {typename => 'INT UNSIGNED',
   values => ['0', '1', '1000000', '4294967295', 'NULL'],
   eq => "val = 1", range => "val >= 1 AND val < 1000000"},
  {typename => 'BIGINT',
   values => ['-9223372036854775808', '-1', '0', '1', '9223372036854775807', 'NULL'],
   eq => "val = 0", range => "val >= -1 AND val < 1"},
  {typename => 'BIGINT UNSIGNED',
   values => ['0', '1', '1000000000', '18446744073709551615', 'NULL'],
   eq => "val = 1", range => "val >= 1 AND val < 1000000000"},
#  {typename => 'FLOAT',
#   values => ['-1000000', '-1.5', '0', '1.5', '1000000', 'NULL'],
#   eq => "val = 0", range => "val >= -1.5 AND val < 1.5"},
#  {typename => 'DOUBLE',
#   values => ['-1000000000', '-1.5', '0', '1.5', '1000000000', 'NULL'],
#   eq => "val = 0", range => "val >= -1.5 AND val < 1.5"},
#  {typename => 'DECIMAL(5,2)',
#   values => ['-999.99', '0.00', '1.50', '999.99', 'NULL'],
#   eq => "val = 0.0", range => "val >= 0.00 AND val < 1.50"},
#  {typename => 'DECIMAL(15,4)',
#   values => ['-99999999999.9999', '0', '1.5', '99999999999.9999', 'NULL'],
#   eq => "val = 0", range => "val >= 0 AND val < 1.5"},
#  {typename => 'DECIMAL(30,10)',
#   values => ['-99999999999999999999.9999999999', '0', '1.5',
#              '99999999999999999999.9999999999', 'NULL'],
#   eq => "val = 0", range => "val >= 0 AND val < 1.5"},
#  {typename => 'DECIMAL(10,0)',
#   values => ['-9999999999', '0', '1', '9999999999', 'NULL'],
#   eq => "val = 0", range => "val >= 0 AND val < 1"},
#  {typename => 'DECIMAL(10,10)',
#   values => ['-0.9999999999', '0', '0.5', '0.9999999999', 'NULL'],
#   eq => "val = 0", range => "val >= 0 AND val < 0.5"},
#  {typename => 'DECIMAL(65,30)',
#   values => ['-' . ('9' x 35) . '.' . ('9' x 30), '0', '1.5',
#              ('9' x 35) . '.' . ('9' x 30), 'NULL'],
#   eq => "val = 0", range => "val >= 0 AND val < 1.5"},
#  {typename => 'DATE',
#   values => ["'2025-01-01'", "'2025-01-10'", "'2025-01-15'", "'2025-01-20'",
#              "'2025-01-31'", 'NULL'],
#   eq => "val = '2025-01-15'",
#   range => "val >= '2025-01-10' AND val < '2025-01-20'"},
#  {typename => 'DATETIME',
#   values => ["'2025-01-01 00:00:00'", "'2025-01-10 12:00:00'",
#              "'2025-01-15 12:00:00'", "'2025-01-20 00:00:00'",
#              "'2025-01-31 23:59:59'", 'NULL'],
#   eq => "val = '2025-01-15 12:00:00'",
#   range => "val >= '2025-01-10 00:00:00' AND val < '2025-01-20 00:00:00'"},
#  {typename => 'DATETIME(3)',
#   values => ["'2025-01-01 00:00:00.000'", "'2025-01-10 12:00:00.000'",
#              "'2025-01-15 12:00:00.000'", "'2025-01-20 00:00:00.000'",
#              "'2025-01-31 23:59:59.999'", 'NULL'],
#   eq => "val = '2025-01-15 12:00:00.000'",
#   range => "val >= '2025-01-10 00:00:00' AND val < '2025-01-20 00:00:00'"},
#  {typename => 'DATETIME(6)',
#   values => ["'2025-01-01 00:00:00.000000'", "'2025-01-10 12:00:00.000000'",
#              "'2025-01-15 12:00:00.000000'", "'2025-01-20 00:00:00.000000'",
#              "'2025-01-31 23:59:59.999999'", 'NULL'],
#   eq => "val = '2025-01-15 12:00:00.000000'",
#   range => "val >= '2025-01-10 00:00:00' AND val < '2025-01-20 00:00:00'"},
#  {typename => 'TIMESTAMP',
#   values => ["'2025-01-01 00:00:00'", "'2025-01-10 12:00:00'",
#              "'2025-01-15 12:00:00'", "'2025-01-20 00:00:00'",
#              "'2025-01-31 23:59:59'", 'NULL'],
#   eq => "val = '2025-01-15 12:00:00'",
#   range => "val >= '2025-01-10 00:00:00' AND val < '2025-01-20 00:00:00'"},
#  {typename => 'TIMESTAMP(3)',
#   values => ["'2025-01-01 00:00:00.000'", "'2025-01-10 12:00:00.000'",
#              "'2025-01-15 12:00:00.000'", "'2025-01-20 00:00:00.000'",
#              "'2025-01-31 23:59:59.999'", 'NULL'],
#   eq => "val = '2025-01-15 12:00:00.000'",
#   range => "val >= '2025-01-10 00:00:00' AND val < '2025-01-20 00:00:00'"},
#  {typename => 'TIMESTAMP(6)',
#   values => ["'2025-01-01 00:00:00.000000'", "'2025-01-10 12:00:00.000000'",
#              "'2025-01-15 12:00:00.000000'", "'2025-01-20 00:00:00.000000'",
#              "'2025-01-31 23:59:59.999999'", 'NULL'],
#   eq => "val = '2025-01-15 12:00:00.000000'",
#   range => ["val >= DATE_SUB('2025-01-15 12:00:00', INTERVAL 6 HOUR) AND val < '2025-01-15 12:00:00'",
#             "val >= DATE_SUB('2025-01-15 00:00:00', INTERVAL 24 HOUR) AND val < '2025-01-15 00:00:00'",
#             "val >= DATE_SUB('2025-01-20 00:00:00', INTERVAL 7 DAY) AND val < '2025-01-20 00:00:00'",
#             "val >= DATE_ADD('2025-01-10 00:00:00', INTERVAL 12 HOUR) AND val < DATE_ADD('2025-01-10 00:00:00', INTERVAL 36 HOUR)",
#             "val >= DATE_SUB(DATE_ADD('2025-01-15 00:00:00', INTERVAL 2 DAY), INTERVAL 1 DAY) AND val < DATE_ADD('2025-01-15 00:00:00', INTERVAL 1 DAY)"]},
#  {typename => 'VARCHAR(200)',
#   values => ["''", "'a'", "'åäö_测试'", "'𝕳𝖊𝖑𝖑𝖔🌍'", "'val1'", "'val2'", "'val3'",
#              "'val4'", "'val5'", "'" . ('x' x 200) . "'", 'NULL'],
#   eq => "val = 'val1'",
#   range => ["val >= 'a' AND val < 'val5'",
#             "val >= '" . ('x' x 200) . "'"]},
#  {typename => 'VARCHAR(1000)',
#   values => ["''", "'a'", "'åäö_测试'", "'𝕳𝖊𝖑𝖑𝖔🌍'", "'val1'", "'val2'", "'val3'",
#              "'" . ('y' x 1000) . "'", 'NULL'],
#   eq => "val = 'val1'",
#   range => ["val >= 'a' AND val < 'val3'",
#             "val >= '" . ('y' x 1000) . "'"]},
);

# Setup
my $test_file = $ENV{TEST_INC_FILE} // die "TEST_INC_FILE not set";
open(my $test_fh, '>', $test_file) or die "Cannot open $test_file: $!";
binmode($test_fh, ":utf8");

my $seed = $ENV{RAND_SEED} // die "RAND_SEED not set";
my $num_rows = $ENV{NUM_ROWS} // 2000;

# ============================================================================
# Utility Functions
# ============================================================================

sub l {
    my ($line) = @_;
    print $test_fh $line . "\n";
}

sub rand_int {
  my ($min, $max) = @_;
  return int(rand($max - $min + 1)) + $min;
}

# Helper to create table without index and generate batched INSERT statements
sub create_table {
  my ($type) = @_;
  l "--disable_query_log";
  l "CREATE TABLE tbl (";
  l "  pk INT PRIMARY KEY AUTO_INCREMENT,";
  l "  val " . $type->{typename} . ",";
  l "  foo INT,";
  l "  bar INT";
  l ") ENGINE=NDB;";
  l "";
  # Generate batched INSERT statements
  srand($seed);
  my $batch_size = 100;
  my @values = @{$type->{values}};
  for (my $row = 0; $row < $num_rows; $row += $batch_size) {
    my $this_batch = ($row + $batch_size > $num_rows) ? ($num_rows - $row) : $batch_size;
    l "INSERT INTO tbl (val, foo, bar) VALUES";
    for (my $i = 0; $i < $this_batch; $i++) {
      my $val_sql = $values[rand_int(0, $#values)];
      my $foo_sql = rand_int(-100, 100);
      my $bar_sql = rand_int(0, 10);
      l "  ($val_sql, $foo_sql, $bar_sql)" . (($i < $this_batch - 1) ? "," : ";");
    }
  }
  l "--enable_query_log";
  l "";
}

sub drop_table {
  l "--disable_query_log";
  l "DROP TABLE tbl;";
  l "--enable_query_log";
  l "";
}

sub go {
  # todo remove canonicalization when formatting is fixed
  l "--let \$canonicalization_script=" .
      "s/(\\.5813)[0-9]+\\t/\\1\\t/;" .
      "s/\\.0000\\t/\\t/;" .
      "s/(-0\\.612|5\\.255)\\t/\\10\\t/;" .
      "s/((-?63|-?0|-64)\\.5)\\t/\\1000\\t/;" .
      "s/(2\\.87)\\t/\\100\\t/;" .
      "s/(-?42\\.3333)3*6\\t/\\1\\t/;" .
      "s/(-42\\.666)6*4\\t/\\17\\t/;" .
      "s/(-0\\.666)6*\\t/\\17\\t/;" .
      "s/((32|0)\\.25)\\t/\\100\\t/;" .
      "s/(1\\.405)06[0-9]*\\t/\\11\\t/;" .
      "s/(88\\.2185|414\\.5696|15896\\.0331|106184\\.6962|4002581\\.3311)[0-9]*\\t/\\1\\t/;" .
      "s/(27183337\\.101)2[0-9]*\\t/\\13\\t/;" .
      "s/(1024237236\\.165)5[0-9]*\\t/\\16\\t/;" .
      "s/(116751544770313\\.)5949\\t/\\16\\t/;" .
      "s/4\\.(39789952088439)7e15\\t/4\\16.5364\\t/;";
  l "--source suite/ronsql/include/ronsql_compare.inc";
  l "";
}

sub explain {
  my ($expected_patterns) = @_;
  $expected_patterns //= [];
  l "--source suite/ronsql/include/ronsql_explain.inc";
  l "--cat_file \$EXPLAIN_FILE";
  l "--echo ==========";
  foreach my $pattern (@{$expected_patterns}) {
    l "--exec grep -qF '$pattern' \$EXPLAIN_FILE";
  }
  l "--remove_file \$EXPLAIN_FILE"
}

# ============================================================================
# Test Generation
# ============================================================================

foreach my $type (@data_types) {
  my $disp = $type->{typename};
  my @range_preds = ref($type->{range}) eq 'ARRAY' ? @{$type->{range}} : ($type->{range});
  my $aggregable = $type->{typename} !~ /^(DATE|DATETIME|TIMESTAMP|VARCHAR)/;
  my $summable = $aggregable && $type->{typename} !~ /^BIGINT/;

  l "--echo";
  l "--echo ==================== Start testing data type: $disp ====================";

  # Create table and generate INSERT statements for this type
  l "--echo $disp: Create table and load data";
  create_table($type);

  l "DELIMITER |;";
  l "";

  l "--echo $disp: SELECT with GROUP BY";
  l "let \$QUERY=";
  l "SELECT val, COUNT(*) as cnt";
  l "FROM tbl";
  l "GROUP BY val;|";
  go;

  if ($aggregable) {
    l "--echo $disp: All aggregates";
    l "let \$QUERY=";
    l "SELECT COUNT(val) as avg_val,";
    if ($summable) {
      l "       AVG(val) as cnt_val,";
      l "       SUM(val) as sum_val,";
    }
    l "       MIN(val) as min_val,";
    l "       MAX(val) as max_val";
    l "FROM tbl;|";
    go;
  } else {
    l "--echo $disp is not aggregable.";
  }

  l "--echo $disp: Filter equality. No index, expect TABLE SCAN.";
  l "let \$QUERY=SELECT COUNT(*) as cnt FROM tbl WHERE " . $type->{eq} . ";|";
  explain ["Execute as table scan."];
  go;

  foreach my $range_pred (@range_preds) {
    l "--echo $disp: Filter comparison WHERE $range_pred. No index, expect TABLE SCAN.";
    l "let \$QUERY=SELECT COUNT(*) as cnt FROM tbl WHERE $range_pred;|";
    explain ["Execute as table scan."];
    go;
  }

  l "--echo $disp: Add indexes";
  l "ALTER TABLE tbl ADD INDEX idx_val_foo (val, foo);|";
  l "ALTER TABLE tbl ADD INDEX idx_foo_val (foo, val);|";

  l "--echo $disp: Index scan equality. Expect INDEX SCAN on idx_val_foo.";
  l "let \$QUERY=SELECT COUNT(*) as cnt FROM tbl WHERE " . $type->{eq} . ";|";
  explain ["Execute as index scan.", "(1 bound)"];
  go;

  foreach my $range_pred (@range_preds) {
    l "--echo $disp: Index scan range WHERE $range_pred. Expect INDEX SCAN on idx_val_foo.";
    l "let \$QUERY=SELECT COUNT(*) as cnt FROM tbl WHERE $range_pred;|";
    explain ["Execute as index scan.", "(2 bounds)"];
    go;
  }

  l "--echo $disp: Composite index scan. Expect INDEX SCAN on idx_val_foo.";
  l "let \$QUERY=SELECT COUNT(*) as cnt FROM tbl WHERE 0 <= foo AND " . $type->{eq} . " AND foo < 50;|";
  explain ["Execute as index scan.", "(3 bounds)"];
  go;

  foreach my $range_pred (@range_preds) {
    l "--echo $disp: Composite index scan WHERE $range_pred. Expect INDEX SCAN on idx_foo_val.";
    l "let \$QUERY=SELECT COUNT(*) as cnt FROM tbl WHERE $range_pred AND 0 = foo;|";
    explain ["Execute as index scan.", "(3 bounds)"];
    go;
  }

  l "--echo $disp: Composite index scan. Expect INDEX SCAN on idx_val_foo and 2 FILTERs on bar.";
  l "--echo $disp: 3-column filter val+foo+bar";
  l "let \$QUERY=";
  l "SELECT COUNT(*) as cnt";
  l "FROM tbl";
  l "WHERE 0 <= foo AND 0 <= bar AND " . $type->{eq} . " AND foo < 50 AND bar < 5;|";
  explain ["Execute as index scan.", "(3 bounds and 2 filters)"];
  go;

  if ($aggregable) {
    l "--echo $disp: Multi-column aggregates";
    l "let \$QUERY=";
    l "SELECT COUNT(*) as count_all";
    l "     , COUNT(val) as count_val";
    l "     , COUNT(foo) as count_foo";
    l "     , COUNT(bar) as count_bar";
    if ($summable) {
      l "     , SUM(val) as sum_val";
      l "     , SUM(foo) as sum_foo";
      l "     , SUM(bar) as sum_bar";
      l "     , AVG(val) as avg_val";
      l "     , AVG(foo) as avg_foo";
      l "     , AVG(bar) as avg_bar";
    }
    l "     , MAX(val) as max_val";
    l "     , MAX(foo) as max_foo";
    l "     , MAX(bar) as max_bar";
    l "FROM tbl;|";
    go;
  }

  l "--echo $disp: GROUP BY with multiple columns.";
  l "let \$QUERY=";
  l "SELECT val, foo, SUM(bar), COUNT(*)";
  l "FROM tbl";
  l "WHERE foo >= -10 AND foo <= 10";
  l "GROUP BY val, foo";
  l "ORDER BY val, foo;|";
  go;

  l "--echo $disp: Empty data set, one result row.";
  l "let \$QUERY=";
  l "SELECT";
  l "  COUNT(*) as cnt,";
  if ($aggregable) {
    l "  MIN(val) as min_val,";
  }
  if ($summable) {
    l "  AVG(val) as avg_val,";
  }
  l "  AVG(foo) as avg_foo";
  l "FROM tbl";
  l "WHERE foo = -9999999;|";
  go;

  l "--echo $disp: GROUP BY and ORDER BY";
  l "let \$QUERY=";
  l "SELECT val, COUNT(*) as cnt";
  l "FROM tbl";
  l "GROUP BY val";
  l "ORDER BY val DESC;|";
  go;

  l "DELIMITER ;|";

  # Drop table with index
  drop_table;
}

close($test_fh);

print "Generated tests for " . scalar(@data_types) . " data types\n";
