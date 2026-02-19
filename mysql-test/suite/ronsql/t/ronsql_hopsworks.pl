#!/usr/bin/perl
# vim: set fileencoding=utf-8 : -*- coding: utf-8 -*-

use strict;
use warnings;
use utf8;
binmode(STDOUT, ":utf8");

# Data type definitions with SQL value representations
my @data_types = (
  {typename => 'TINYINT',
   values => ['-128', '-1', '0', '1', '127', "NULL"],
   eq => "val = 0", range => "val >= -1 AND val < 1"},
  {typename => 'TINYINT UNSIGNED',
   values => ['0', '1', '100', '255', "NULL"],
   eq => "val = 1", range => "val >= 1 AND val < 100"},
  {typename => 'SMALLINT',
   values => ['-32768', '-1', '0', '1', '32767', "NULL"],
   eq => "val = 0", range => "val >= -1 AND val < 1"},
  {typename => 'SMALLINT UNSIGNED',
   values => ['0', '1', '1000', '65535', "NULL"],
   eq => "val = 1", range => "val >= 1 AND val < 1000"},
  {typename => 'MEDIUMINT',
   values => ['-8388608', '-1', '0', '1', '8388607', "NULL"],
   eq => "val = 0", range => "val >= -1 AND val < 1"},
  {typename => 'MEDIUMINT UNSIGNED',
   values => ['0', '1', '10000', '16777215', "NULL"],
   eq => "val = 1", range => "val >= 1 AND val < 10000"},
  {typename => 'INT',
   values => ['-2147483648', '-1', '0', '1', '2147483647', "NULL"],
   eq => "val = 0", range => "val >= -1 AND val < 1"},
  {typename => 'INT UNSIGNED',
   values => ['0', '1', '1000000', '4294967295', "NULL"],
   eq => "val = 1", range => "val >= 1 AND val < 1000000"},
  {typename => 'BIGINT',
   values => ['-9223372036854775808', '-1', '0', '1', '9223372036854775807', "NULL"],
   eq => "val = 0", range => "val >= -1 AND val < 1"},
  {typename => 'BIGINT UNSIGNED',
   values => ['0', '1', '1000000000', '18446744073709551615', "NULL"],
   eq => "val = 1", range => "val >= 1 AND val < 1000000000"},
  {typename => 'FLOAT',
   values => ['-1000000', '-1.5', '0', '1.5', '1000000', "NULL"],
   eq => "val = 0", range => "val >= -1.5 AND val < 1.5"},
  {typename => 'DOUBLE',
   values => ['-1000000000', '-1.5', '0', '1.5', '1000000000', "NULL"],
   eq => "val = 0", range => "val >= -1.5 AND val < 1.5"},
  {typename => 'DECIMAL(5,2)',
   values => ['-999.99', '0.00', '1.50', '999.99', "NULL"],
   eq => "val = 0.0", range => "val >= 0.00 AND val < 1.50"},
  {typename => 'DECIMAL(15,4)',
   values => ['-99999999999.9999', '0', '1.5', '99999999999.9999', "NULL"],
   eq => "val = 0", range => "val >= 0 AND val < 1.5"},
  {typename => 'DECIMAL(30,10)',
   values => ['-99999999999999999999.9999999999', '0', '1.5',
              '99999999999999999999.9999999999', "NULL"],
   eq => "val = 0", range => "val >= 0 AND val < 1.5"},
  {typename => 'DECIMAL(10,0)',
   values => ['-9999999999', '0', '1', '9999999999', "NULL"],
   eq => "val = 0", range => "val >= 0 AND val < 1"},
  {typename => 'DECIMAL(10,10)',
   values => ['-0.9999999999', '0', '0.5', '0.9999999999', "NULL"],
   eq => "val = 0", range => "val >= 0 AND val < 0.5"},
  {typename => 'DECIMAL(65,30)',
   values => ['-' . ('9' x 35) . '.' . ('9' x 30), '0', '1.5',
              '5' . ('0' x 34) . '.' . ('0' x 29) . '1',
              '5' . ('0' x 34) . '.' . ('0' x 29) . '2',
              '5' . ('0' x 34) . '.' . ('0' x 29) . '3',
              ('9' x 35) . '.' . ('9' x 30), "NULL"],
   eq => "val = 0",
   range => ["val >= 0 AND val < 1.5",
             "val >= 5" . ('0' x 34) . '.' . ('0' x 29) . "2 AND val < 7"]},
  {typename => 'DECIMAL(30,10) UNSIGNED',
   values => ['0', '1.5', '99999999999999999999.9999999999', "NULL"],
   eq => "val = 0", range => "val >= 0 AND val < 1.5"},
  {typename => 'DECIMAL(10,0) UNSIGNED',
   values => ['0', '1', '9999999999', "NULL"],
   eq => "val = 0", range => "val >= 0 AND val < 1"},
  {typename => 'DATE',
   values => ["'2025-01-01'", "'2025-01-10'", "'2025-01-15'", "'2025-01-20'",
              "'2025-01-31'", "NULL"],
   eq => "val = '2025-01-15'",
   range => "val >= '2025-01-10' AND val < '2025-01-20'"},
  {typename => 'DATETIME',
   values => ["'2025-01-01 00:00:00'", "'2025-01-10 12:00:00'",
              "'2025-01-15 12:00:00'", "'2025-01-20 00:00:00'",
              "'2025-01-31 23:59:59'", "NULL"],
   eq => "val = '2025-01-15 12:00:00'",
   range => "val >= '2025-01-10 00:00:00' AND val < '2025-01-20 00:00:00'"},
  {typename => 'DATETIME(3)',
   values => ["'2025-01-01 00:00:00.000'", "'2025-01-10 12:00:00.000'",
              "'2025-01-15 12:00:00.000'", "'2025-01-20 00:00:00.000'",
              "'2025-01-31 23:59:59.999'", "NULL"],
   eq => "val = '2025-01-15 12:00:00.000'",
   range => "val >= '2025-01-10 00:00:00' AND val < '2025-01-20 00:00:00'"},
  {typename => 'DATETIME(6)',
   values => ["'2025-01-01 00:00:00.000000'", "'2025-01-10 12:00:00.000000'",
              "'2025-01-15 12:00:00.000000'", "'2025-01-20 00:00:00.000000'",
              "'2025-01-31 23:59:59.999999'", "NULL"],
   eq => "val = '2025-01-15 12:00:00.000000'",
   range => "val >= '2025-01-10 00:00:00' AND val < '2025-01-20 00:00:00'"},
  {typename => 'TIMESTAMP',
   values => ["'2025-01-01 00:00:00'", "'2025-01-10 12:00:00'",
              "'2025-01-15 12:00:00'", "'2025-01-20 00:00:00'",
              "'2025-01-31 23:59:59'", "NULL"],
   eq => "val = '2025-01-15 12:00:00'",
   range => "val >= '2025-01-10 00:00:00' AND val < '2025-01-20 00:00:00'"},
  {typename => 'TIMESTAMP(3)',
   values => ["'2025-01-01 00:00:00.000'", "'2025-01-10 12:00:00.000'",
              "'2025-01-15 12:00:00.000'", "'2025-01-20 00:00:00.000'",
              "'2025-01-31 23:59:59.999'", "NULL"],
   eq => "val = '2025-01-15 12:00:00.000'",
   range => "val >= '2025-01-10 00:00:00' AND val < '2025-01-20 00:00:00'"},
  {typename => 'TIMESTAMP(6)',
   values => ["'2025-01-01 00:00:00.000000'", "'2025-01-10 12:00:00.000000'",
              "'2025-01-15 12:00:00.000000'", "'2025-01-20 00:00:00.000000'",
              "'2025-01-31 23:59:59.999999'", "NULL"],
   eq => "val = '2025-01-15 12:00:00.000000'",
   range => ["val >= DATE_SUB('2025-01-15 12:00:00', INTERVAL 6 HOUR) AND val < '2025-01-15 12:00:00'",
             "val >= DATE_SUB('2025-01-15 00:00:00', INTERVAL 24 HOUR) AND val < '2025-01-15 00:00:00'",
             "val >= DATE_SUB('2025-01-20 00:00:00', INTERVAL 7 DAY) AND val < '2025-01-20 00:00:00'",
             "val >= DATE_ADD('2025-01-10 00:00:00', INTERVAL 12 HOUR) AND val < DATE_ADD('2025-01-10 00:00:00', INTERVAL 36 HOUR)",
             "val >= DATE_SUB(DATE_ADD('2025-01-15 00:00:00', INTERVAL 2 DAY), INTERVAL 1 DAY) AND val < DATE_ADD('2025-01-15 00:00:00', INTERVAL 1 DAY)"]},
  {typename => 'VARCHAR(30) CHARACTER SET utf8mb4',
   values => ["''", "'a'", "'åäö_测试'", "'𝕳𝖊𝖑𝖑𝖔🌍'",
              "'val1'", "'val2'", "'val3'",
              "'y" . ('𝕳' x 29) . "'",
              "'" . ('y' x 29) . "x'",
              "'" . ('y' x 29) . "y'",
              "'" . ('y' x 29) . "z'",
              "NULL"],
   eq => "val = 'val1'",
   range => ["val >= 'a' AND val < 'val3'",
             "val >= '" . ('y' x 30) . "' AND val <= 'z'",
             "val > '" . ('y' x 30) . "' AND val < 'z'",
             "val <= '" . ('y' x 30) . "' AND val > 'a'",
             "val < '" . ('y' x 30) . "' AND val >= 'a'",
             "val <= 'y" . ('𝕳' x 29) . "' AND val >= 'a'"]},
  {typename => 'VARCHAR(767) CHARACTER SET utf8mb4',
   values => ["''", "'a'", "'åäö_测试'", "'𝕳𝖊𝖑𝖑𝖔🌍'",
              "'val1'", "'val2'", "'val3'",
              "'y" . ('𝕳' x 766) . "'",
              "'" . ('y' x 767) . "'",
              "NULL"],
   eq => "val = 'val1'",
   range => ["val >= 'a' AND val < 'val3'",
             "val >= '" . ('y' x 767) . "' AND val <= 'z'"]},
  {typename => 'VARCHAR(7) CHARACTER SET latin1',
   values => ["''", "'a'", "'val1'", "'val2'", "'val3'",
              "'yyyyyyy'", "NULL"],
   eq => "val = 'val1'",
   range => ["val >= 'a' AND val < 'val3'",
             "val >= 'yyyyyyy' AND val < 'z'"]},
  {typename => 'VARCHAR(1000) CHARACTER SET latin1',
   values => ["''", "'a'", "'val1'", "'val2'", "'val3'",
              "'" . ('y' x 1000) . "'", "NULL"],
   eq => "val = 'val1'",
   range => ["val >= 'a' AND val < 'val3'",
             "val >= '" . ('y' x 1000) . "' AND val < 'z'"]},
  {typename => 'VARCHAR(100) CHARACTER SET latin2',
   values => ["''", "'a'", "'val1'", "'val2'", "'val3'",
              "'Łódź'", "'Łódź_test'",
              "'ABC'", "'Kz'", "'Ln'", "'Lodz'", "'Lp'", "'Ma'", "'XYZ'",
              "NULL"],
   eq => "val = 'val1'",
   range => ["val >= 'a' AND val < 'val3'",
             "val >= 'Łódź' AND val < 'z'"]},
);

# Setup
my $test_file = $ENV{TEST_INC_FILE} // die "TEST_INC_FILE not set";
open(my $test_fh, '>', $test_file) or die "Cannot open $test_file: $!";
binmode($test_fh, ":utf8");

my $seed = $ENV{RAND_SEED} // die "RAND_SEED not set";
my $num_rows = $ENV{NUM_ROWS} // 2000;

my $query_file = "\$MYSQL_TMP_DIR/ronsql_hopsworks_query.sql";

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
  my @args = @_;
  my $explain_patterns = undef;

  # If last arg is array ref, treat as explain patterns
  if (@args > 0 && ref($args[-1]) eq 'ARRAY') {
    $explain_patterns = pop @args;
  }

  # Write query to file
  l "--write_file $query_file";
  l join("\n", @args);
  l "EOF";

  # Run comparison first
  # todo remove canonicalization when formatting is fixed
  l "--let \$canonicalization_script=" .
      "s/(\\.5813)[0-9]+\\t/\\1\\t/;" .
      "s/\\.0000\\t/\\t/;" .
      "s/(-0\\.612|5\\.255)\\t/\\10\\t/;" .
      "s/((-?63|-?0|-64)\\.5)\\t/\\1000\\t/;" .
      "s/(2\\.87)\\t/\\100\\t/;" .
      "s/(-?42\\.3333)3*6\\t/\\1\\t/;" .
      "s/(-42\\.666)6*4\\t/\\17\\t/;" .
      "s/(-0\\.666|23\\.499999999)6[0-9]*\\t/\\17\\t/;" .
      "s/(300000000061\\.4997)6\\t/\\1\\t/;" .
      "s/(300000000061\\.499)(5|6|63|8)\\t/\\17\\t/;" .
      "s/(3061\\.4)6999[0-9]*\\t/\\17\\t/;" .
      "s/(198675496\\.940)39[0-9]*\\t/\\14\\t/;" .
      "s/(1986754967.294)6[0-9]*\\t/\\170000\\t/;" .
      "s/((32|0)\\.25)\\t/\\100\\t/;" .
      "s/(1\\.405)06[0-9]*\\t/\\11\\t/;" .
      "s/(3061\\.47|1986754967\\.2947)0[0-9]*\\t/\\1\\t/;" .
      "s/(88\\.2185|414\\.5696|15896\\.0331|106184\\.6962|4002581\\.3311|0\\.15562913907086|3142857142\\.95)[0-9]*\\t/\\1\\t/;" .
      "s/(27183337\\.101)2[0-9]*\\t/\\13\\t/;" .
      "s/(1024237236\\.165|116751544770313\\.|20\\.27463)5[0-9]*\\t/\\16\\t/;" .
      "s/(3061\\.47)0+3\\t/\\1\\t/;";
  l "--let \$QUERY_FILE=$query_file";
  l "--source suite/ronsql/include/ronsql_compare.inc";
  l "";

  # Then, if patterns are provided, run explain
  if (defined $explain_patterns) {
    # Write query to file
    l "--write_file $query_file";
    l join("\n", @args);
    l "EOF";
    # Run EXPLAIN query
    l "--let \$QUERY_FILE=$query_file";
    l "--source suite/ronsql/include/ronsql_explain.inc";
    l "--cat_file \$EXPLAIN_FILE";
    l "--echo ==========";
    l "--echo";
    l "--echo";
    l "--echo";
    foreach my $pattern (@{$explain_patterns}) {
      l "--exec grep -qF '$pattern' \$EXPLAIN_FILE";
    }
    l "--remove_file \$EXPLAIN_FILE";
  }
}

# ============================================================================
# Test Generation
# ============================================================================

foreach my $type (@data_types) {
  my $disp = $type->{typename};
  my @range_preds = ref($type->{range}) eq 'ARRAY' ? @{$type->{range}} : ($type->{range});
  my $aggregable = $type->{typename} !~ /^(DATE|DATETIME|TIMESTAMP|VARCHAR)/;
  my $summable = $aggregable && $type->{typename} !~ /^BIGINT/;
  my $goodness = sub {
    my ($with_bonus, $without_bonus) = @_;
    my $value = ($type->{typename} !~ /^VARCHAR/) ? $with_bonus : $without_bonus;
    return "goodness $value ";
  };

  l "--echo";
  l "--echo ==================== Start testing data type: $disp ====================";

  # Create table and generate INSERT statements for this type
  l "--echo $disp: Create table and load data";
  create_table($type);

  l "--echo $disp: SELECT with GROUP BY";
  go("SELECT val, COUNT(*) as cnt",
     "FROM tbl",
     "GROUP BY val;");

  if ($aggregable) {
    l "--echo $disp: All aggregates";
    go("SELECT COUNT(val) as cnt_val,",
       $summable ? "       AVG(val) as avg_val," : (),
       $summable ? "       SUM(val) as sum_val," : (),
       "       MIN(val) as min_val,",
       "       MAX(val) as max_val",
       "FROM tbl;");
  } else {
    l "--echo $disp is not aggregable.";
  }

  l "--echo $disp: Filter equality. No index, expect TABLE SCAN.";
  go("SELECT COUNT(*) as cnt FROM tbl WHERE " . $type->{eq} . ";",
     ["Execute as table scan."]);

  foreach my $range_pred (@range_preds) {
    l "--echo $disp: Filter comparison WHERE $range_pred. No index, expect TABLE SCAN.";
    go("SELECT COUNT(*) as cnt FROM tbl WHERE $range_pred;",
       ["Execute as table scan."]);
  }

  l "--echo $disp: Add indexes";
  l "ALTER TABLE tbl ADD INDEX idx_val_foo (val, foo);";
  l "ALTER TABLE tbl ADD INDEX idx_foo_val (foo, val);";

  l "--echo $disp: Index scan equality. Expect INDEX SCAN on idx_val_foo.";
  go("SELECT COUNT(*) as cnt FROM tbl WHERE " . $type->{eq} . ";",
     ["Execute as index scan.",
      "Index: `idx_val_foo`(`val`, `foo`)",
      "(1 bound)",
      $goodness->("110000", "100000"),
     ]);

  foreach my $range_pred (@range_preds) {
    l "--echo $disp: Index scan range WHERE $range_pred. Expect INDEX SCAN on idx_val_foo.";
    go("SELECT COUNT(*) as cnt FROM tbl WHERE $range_pred;",
       ["Execute as index scan.",
        "Index: `idx_val_foo`(`val`, `foo`)",
        "(2 bounds)",
        $goodness->("1100", "1000"),
       ]);
  }

  l "--echo $disp: Composite index scan. Expect INDEX SCAN on idx_val_foo.";
  go("SELECT COUNT(*) as cnt FROM tbl WHERE 0 <= foo AND " . $type->{eq} . " AND foo < 50;",
     ["Execute as index scan.",
      "Index: `idx_val_foo`(`val`, `foo`)",
      "(3 bounds)",
      $goodness->("111100", "101100"),
     ]);

  foreach my $range_pred (@range_preds) {
    l "--echo $disp: Composite index scan WHERE $range_pred. Expect INDEX SCAN on idx_foo_val.";
    go("SELECT COUNT(*) as cnt FROM tbl WHERE $range_pred AND 0 = foo;",
       ["Execute as index scan.",
        "Index: `idx_foo_val`(`foo`, `val`)",
        "(3 bounds)",
        $goodness->("111100", "111000"),
       ]);
  }

  # Change the table in a backward compatible way. This will increase the table
  # object version without recreating indexes.
  l "ALTER TABLE tbl ADD COLUMN baz INT NULL;";

  l "--echo $disp: Composite index scan. Expect INDEX SCAN on idx_val_foo and 2 FILTERs on bar.";
  l "--echo $disp: 3-column filter val+foo+bar";
  go("SELECT COUNT(*) as cnt",
     "FROM tbl",
     "WHERE 0 <= foo AND 0 <= bar AND " . $type->{eq} . " AND foo < 50 AND bar < 5;",
     ["Execute as index scan.",
      "Index: `idx_val_foo`(`val`, `foo`)",
      "(3 bounds and 2 filters)",
      $goodness->("111100", "101100"),
     ]);

  if ($aggregable) {
    l "--echo $disp: Multi-column aggregates";
    go("SELECT COUNT(*) as cnt_all",
       "     , COUNT(val) as cnt_val",
       "     , COUNT(foo) as cnt_foo",
       "     , COUNT(bar) as cnt_bar",
       $summable ? "     , SUM(val) as sum_val" : (),
       $summable ? "     , SUM(foo) as sum_foo" : (),
       $summable ? "     , SUM(bar) as sum_bar" : (),
       $summable ? "     , AVG(val) as avg_val" : (),
       $summable ? "     , AVG(foo) as avg_foo" : (),
       $summable ? "     , AVG(bar) as avg_bar" : (),
       "     , MAX(val) as max_val",
       "     , MAX(foo) as max_foo",
       "     , MAX(bar) as max_bar",
       "FROM tbl;");
  }

  l "--echo $disp: GROUP BY with multiple columns.";
  go("SELECT val, foo, SUM(bar), COUNT(*)",
     "FROM tbl",
     "WHERE foo >= -10 AND foo <= 10",
     "GROUP BY val, foo;");

  l "--echo $disp: Empty data set, one result row.";
  go("SELECT",
     "  COUNT(*) as cnt,",
     $aggregable ? "  MIN(val) as min_val," : (),
     $summable ? "  AVG(val) as avg_val," : (),
     "  AVG(foo) as avg_foo",
     "FROM tbl",
     "WHERE foo = -9999999;");

  l "--let \$rdrs_ronsql = curl --no-progress-meter --fail-with-body -X POST -H \"Content-Type: application/json\" http://\$RDRS_NOKEY_HOST:\$RDRS_NOKEY_PORT/0.1.0/ronsql -d";
  l "--echo $disp: JSON format GROUP BY";
  l "--exec \$rdrs_ronsql '{\"query\":\"SELECT val, COUNT(*) as cnt FROM tbl GROUP BY val;\", \"database\":\"test\"}'";
  l "--echo ==========";
  l "--echo";
  l "--echo";
  l "--echo";
  if ($aggregable) {
    l "--echo $disp: JSON format aggregated";
    l "--exec \$rdrs_ronsql '{\"query\":\"SELECT foo, MAX(val) FROM tbl WHERE foo >= -5 AND foo <= 5 GROUP BY foo;\", \"database\":\"test\"}'";
    l "--echo ==========";
    l "--echo";
    l "--echo";
    l "--echo";
  }

  # Drop table with index
  drop_table;
}

close($test_fh);

print "Generated tests for " . scalar(@data_types) . " data types\n";
