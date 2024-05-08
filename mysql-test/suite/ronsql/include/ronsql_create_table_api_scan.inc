--echo Create test table api_scan with $NUM_ROWS rows
--disable_query_log
CREATE TABLE api_scan (
  sint8 TINYINT NOT NULL,
  uint8 TINYINT UNSIGNED NOT NULL,
  sint16 SMALLINT NOT NULL,
  uint16 SMALLINT UNSIGNED NOT NULL,
  sint24 MEDIUMINT NOT NULL,
  uint24 MEDIUMINT UNSIGNED NOT NULL,
  sint32 INT NOT NULL,
  uint32 INT UNSIGNED NOT NULL,
  sint64 BIGINT NOT NULL,
  uint64 BIGINT UNSIGNED NOT NULL,
  cfloat FLOAT NOT NULL,
  cdouble DOUBLE NOT NULL,
  cchar VARCHAR(29) NOT NULL,
  PRIMARY KEY USING HASH (sint32)) ENGINE=NDB CHARSET=latin1;
CREATE INDEX
  index_sint24
  ON api_scan
  (sint24);
--let TEST_DATA_TSV=$MYSQL_TMP_DIR/test_data.tsv
perl;
  use strict;
  use warnings;
  # Setup
  my $file_path = $ENV{TEST_DATA_TSV} // die "SQL_DATA_FILE environment variable is not set";
  my $files_cnt = 1; # This should be set according to your specific needs
  open(my $fh, '>', $file_path) or die "Could not open file '$file_path' $!";
  # Random seed
  my $seed = $ENV{RAND_SEED} // die "RAND_SEED environment variable is not set";
  srand($seed);
  # Random Data Generation Functions
  sub rand_int {
    my ($min, $max) = @_;
    return int(rand($max - $min + 1)) + $min;
  }
  sub rand_float {
    my ($min, $max) = @_;
    return rand($max - $min) + $min;
  }
  # Number of rows to generate
  my $NUM = $ENV{NUM_ROWS} // die "NUM_ROWS environment variable is not set";
  # SQL Generation and Writing
  for (my $i = 0; $i < $NUM; $i++) {
    my $sint8 = rand_int(-128, 127);
    my $uint8 = rand_int(0, 255);
    my $sint16 = rand_int(-32768, 32767);
    my $uint16 = rand_int(0, 65535);
    # The full range would be:
    #   my $sint24 = rand_int(-8388608, 8388607);
    # but we use this column for grouping etc.
    my $sint24 = rand_int(-10, 10);
    my $uint24 = rand_int(0, 16777215);
    my $sint32 = rand_int(-2147483648, 2147483647);
    my $uint32 = rand_int(0, 4294967295);
    # For 64-bit types, use limits about 10000x smaller since 64-bit overflow is
    # tested in ronsql_regressions.test
    my $sint64 = rand_int(-92233720368547, 92233720368547);
    my $uint64 = rand_int(0, 184467440737095);
    my $cfloat = rand_float(-32768, 32767);
    my $cdouble = rand_float(-92233720368547, 92233720368547);
    $uint16 = 0 if (rand_int(0, 19) == 6);
    $uint64 = 0 if (rand_int(0, 19) == 6);
    $cdouble = 0 if (rand_int(0, 19) == 6);
    my $cchar = "GROUPxxx_" . ($i % 4 + 1);
    print $fh "$sint8\t$uint8\t$sint16\t$uint16\t$sint24\t$uint24\t$sint32\t$uint32\t$sint64\t$uint64\t$cfloat\t$cdouble\t$cchar\n";
  }
  # Close File
  close($fh);
EOF
--enable_query_log
--echo First 10 lines of the generated data:
--exec head -n 10 $TEST_DATA_TSV
--disable_query_log
eval LOAD DATA INFILE '$TEST_DATA_TSV' INTO TABLE api_scan;
--enable_query_log
--remove_file $TEST_DATA_TSV

--echo Number of rows in the table:
select count(*) from api_scan;
