#
# Run the ronsqltpch Go test from MTR
#

--source include/have_ndb.inc

perl END_OF_PERL;

use strict;
use warnings;
use File::Spec;

# Environment variables
my $MYSQL_BASEDIR  = $ENV{'MYSQL_BASEDIR'};

# Directories
my $TEST_GO_DIR = File::Spec->catdir($MYSQL_BASEDIR, 'storage', 'ndb', 'rest-server2', 'server', 'test_go');
die "Directory $TEST_GO_DIR does not exist\n" unless -d $TEST_GO_DIR;

# Switching to test_go dir for `go test`.
chdir $TEST_GO_DIR or die "Cannot change directory to $TEST_GO_DIR: $!";

my $go_test_package = 'hopsworks.ai/rdrs2/internal/integrationtests/ronsqltpch';

sub run_test {
    my $stdouterr = `go test -p 1 -count 1 $go_test_package 2>&1`;
    my $exit_code = $? >> 8;

    # Remove test duration column to achieve deterministic output
    $stdouterr =~ s/\t[0-9.]+s$//g;

    print "$stdouterr\n",
          "Golang test for $go_test_package exited with code $exit_code\n";

    if ($exit_code != 0) {
        exit $exit_code;
    }
}

$ENV{'RDRS_CONFIG_FILE'} = $ENV{'RDRS_CONFIG_FILE_NO_TLS'};
print "## Run ronsqltpch without TLS\n";
run_test();

$ENV{'RDRS_CONFIG_FILE'} = $ENV{'RDRS_CONFIG_FILE_TLS'};
print "## Run ronsqltpch with TLS and server-side cert\n";
run_test();

exit 0;
END_OF_PERL
