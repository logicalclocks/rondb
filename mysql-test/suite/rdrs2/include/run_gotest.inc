#
# Run an RDRS2 Unit test from MTR
#

--source include/have_ndb.inc

perl END_OF_PERL;

use strict;
use warnings;
use File::Spec;
use File::Basename;

# Environment variables
my $MYSQL_TMP_DIR  = $ENV{'MYSQL_TMP_DIR'};
my $MYSQL_BASEDIR  = $ENV{'MYSQL_BASEDIR'};
my $MYSQL_TEST_DIR = $ENV{'MYSQL_TEST_DIR'};
my $MYSQLTEST_FILE = $ENV{'MYSQLTEST_FILE'};

# Directories
my $TEST_GO_DIR     = File::Spec->catdir($MYSQL_BASEDIR, 'storage', 'ndb', 'rest-server2', 'server', 'test_go');
my $RDRS2_SUITE_DIR = File::Spec->catdir($MYSQL_TEST_DIR, 'suite', 'rdrs2', 't');
die "Directory $TEST_GO_DIR does not exist\n" unless -d $TEST_GO_DIR;
die "Directory $RDRS2_SUITE_DIR does not exist\n" unless -d $RDRS2_SUITE_DIR;

# Mapping from MTR test to Go test.
# After adding a Golang test package, add a ../t/rdrs2_*.test file with a single
# line "--source suite/rdrs2/include/run_gotest.inc" and add a mapping below.
my %mtr_to_go = (
#  MTR test file name            #   Golang test package name                                         #
#--------------------------------#--------------------------------------------------------------------#
  'rdrs2_api.test'               => 'hopsworks.ai/rdrs2/pkg/api',                                     #
  'rdrs2_batchfeaturestore.test' => 'hopsworks.ai/rdrs2/internal/integrationtests/batchfeaturestore', #
  'rdrs2_batchpkread.test'       => 'hopsworks.ai/rdrs2/internal/integrationtests/batchpkread',       #
  'rdrs2_common.test'            => 'hopsworks.ai/rdrs2/internal/common',                             #
  'rdrs2_config.test'            => 'hopsworks.ai/rdrs2/internal/config',                             #
  'rdrs2_feature_store.test'     => 'hopsworks.ai/rdrs2/internal/feature_store',                      #
  'rdrs2_health.test'            => 'hopsworks.ai/rdrs2/internal/integrationtests/health',            #
  'rdrs2_int_feature_store.test' => 'hopsworks.ai/rdrs2/internal/integrationtests/feature_store',     #
  'rdrs2_integrationtests.test'  => 'hopsworks.ai/rdrs2/internal/integrationtests',                   #
  'rdrs2_log.test'               => 'hopsworks.ai/rdrs2/internal/log',                                #
  'rdrs2_metrics.test'           => 'hopsworks.ai/rdrs2/internal/metrics',                            #
  'rdrs2_ping.test'              => 'hopsworks.ai/rdrs2/internal/integrationtests/ping',              #
  'rdrs2_pkread.test'            => 'hopsworks.ai/rdrs2/internal/integrationtests/pkread',            #
  'rdrs2_stat.test'              => 'hopsworks.ai/rdrs2/internal/integrationtests/stat',              #
  'rdrs2_testclient.test'        => 'hopsworks.ai/rdrs2/internal/integrationtests/testclient',        #
  'rdrs2_testdbs.test'           => 'hopsworks.ai/rdrs2/resources/testdbs',                           #
  'rdrs2_testutils.test'         => 'hopsworks.ai/rdrs2/internal/testutils',                          #
  'rdrs2_tlsutils.test'          => 'hopsworks.ai/rdrs2/internal/security/tlsutils',                  #
  'rdrs2_version.test'           => 'hopsworks.ai/rdrs2/version',                                     #
#--------------------------------#--------------------------------------------------------------------#
);

# Switching to test_go dir for `go list` and `go test`.
chdir $TEST_GO_DIR or die "Cannot change directory to $TEST_GO_DIR: $!";

# Make sure the existing MTR tests match the keys in the map above
opendir(my $dh, $RDRS2_SUITE_DIR) or die "Cannot open directory $RDRS2_SUITE_DIR: $!";
my @actual_mtr = sort grep { /\.test$/ && -f File::Spec->catfile($RDRS2_SUITE_DIR, $_) } readdir($dh);
closedir($dh);
my @expected_mtr = sort keys %mtr_to_go;
if (join("\n", @actual_mtr) ne join("\n", @expected_mtr)) {
    print "MTR tests listed in run_gotest.inc:\n", join("\n", @expected_mtr), "\n";
    print "Existing MTR tests:\n", join("\n", @actual_mtr), "\n";
    die "Mismatch in MTR test lists\n";
}

# Make sure the existing Golang tests match the values in the map above
my @actual_go = sort `go list ./...`;
chomp @actual_go;
my @expected_go = sort values %mtr_to_go;
if (join("\n", @actual_go) ne join("\n", @expected_go)) {
    print "Golang tests listed in run_gotest.inc:\n", join("\n", @expected_go), "\n";
    print "Existing Golang tests:\n", join("\n", @actual_go), "\n";
    die "Mismatch in Golang test lists\n";
}

# Find the go test package mapped to by the currently executing MTR test
my $this_file = basename($MYSQLTEST_FILE);
my $go_test_package = $mtr_to_go{$this_file} // '';
if ($go_test_package eq '') {
    die "No Golang test package found for $this_file\n";
}

# Run Golang test, capture exit code and stdout.
# * `-p 1` ensures that only one test is run in parallel by the `go test`
#   process. This is necessary since some of these tests change the database
#   state. This is unrelated to parallelism in MTR.
# * `-count 1` is the idiomatic way to prevent `go test` from using cached test
#   runs. This is necessary since the `go test` logic doesn't look into (all)
#   C++ sources to detect changes, meaning cache invalidation of test runs
#   wouldn't work correctly.
my $stdouterr = `go test -p 1 -count 1 $go_test_package 2>&1`;
my $exit_code = $? >> 8;

# Remove test duration column to achieve deterministic output
$stdouterr =~ s/\t[0-9.]+s$//g;

print "$stdouterr\n",
      "Golang test for $go_test_package exited with code $exit_code\n";

exit $exit_code;
END_OF_PERL

# Cleanup
--disable_query_log
--disable_warnings

DROP DATABASE IF EXISTS hopsworks;
DROP DATABASE IF EXISTS db000;
DROP DATABASE IF EXISTS db001;
DROP DATABASE IF EXISTS db002;
DROP DATABASE IF EXISTS db003;
DROP DATABASE IF EXISTS db004;
DROP DATABASE IF EXISTS db005;
DROP DATABASE IF EXISTS db006;
DROP DATABASE IF EXISTS db007;
DROP DATABASE IF EXISTS db008;
DROP DATABASE IF EXISTS db009;
DROP DATABASE IF EXISTS db010;
DROP DATABASE IF EXISTS db011;
DROP DATABASE IF EXISTS db012;
DROP DATABASE IF EXISTS db013;
DROP DATABASE IF EXISTS db014;
DROP DATABASE IF EXISTS db015;
DROP DATABASE IF EXISTS db016;
DROP DATABASE IF EXISTS db017;
DROP DATABASE IF EXISTS db018;
DROP DATABASE IF EXISTS db019;
DROP DATABASE IF EXISTS db020;
DROP DATABASE IF EXISTS db021;
DROP DATABASE IF EXISTS db022;
DROP DATABASE IF EXISTS db023;
DROP DATABASE IF EXISTS db024;
DROP DATABASE IF EXISTS db025;
DROP DATABASE IF EXISTS db026;
DROP DATABASE IF EXISTS db027;
DROP DATABASE IF EXISTS db028;
DROP DATABASE IF EXISTS db029;
DROP DATABASE IF EXISTS db030;
DROP DATABASE IF EXISTS fsdb001;
DROP DATABASE IF EXISTS fsdb002;
DROP DATABASE IF EXISTS fsdb003;
DROP DATABASE IF EXISTS rdrs_bench;
DROP DATABASE IF EXISTS sentinel;

--enable_warnings
--enable_query_log
