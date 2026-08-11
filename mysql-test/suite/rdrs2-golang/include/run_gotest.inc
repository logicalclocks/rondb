#
# Run the RDRS2 Golang tests from MTR.
#
# By default this runs ALL Golang test packages in a single `go test ./...`
# invocation, so the hopsworks database is seeded once (the first package that
# runs creates the "sentinel" DB; the rest reuse it) and the cluster is torn
# down once. Running every package in its own MTR test instead re-seeds
# hopsworks for each package, which is what made the suite slow.
#
# To run only one (or a few) packages, set RDRS_GO_PACKAGE to a Go package
# name (or a space-separated list), e.g.:
#
#   RDRS_GO_PACKAGE=hopsworks.ai/rdrs2/internal/integrationtests/pkread \
#     ./mtr --suite rdrs2-golang
#
# The package names are exactly what `go list ./...` prints. To see the
# up-to-date list, run this from the test_go module directory
# (storage/ndb/rest-server2/server/test_go):
#
#   go list ./...
#
# The packages worth running on their own are the integration-test packages
# below (each hits the REST server a different way). Copy a name verbatim into
# RDRS_GO_PACKAGE:
#
#   hopsworks.ai/rdrs2/internal/integrationtests/ping              # server is alive (/ping)
#   hopsworks.ai/rdrs2/internal/integrationtests/health            # cluster health (/health)
#   hopsworks.ai/rdrs2/internal/integrationtests/stat              # server statistics
#   hopsworks.ai/rdrs2/internal/integrationtests/pkread            # single primary-key reads
#   hopsworks.ai/rdrs2/internal/integrationtests/batchpkread       # batched primary-key reads
#   hopsworks.ai/rdrs2/internal/integrationtests/index_scan        # index / range scans
#   hopsworks.ai/rdrs2/internal/integrationtests/feature_store     # single feature-vector lookups
#   hopsworks.ai/rdrs2/internal/integrationtests/batchfeaturestore # batched feature-vector lookups
#   hopsworks.ai/rdrs2/internal/integrationtests/sharing           # cross-project feature sharing
#
# Example running two of them at once:
#
#   RDRS_GO_PACKAGE="hopsworks.ai/rdrs2/internal/integrationtests/pkread hopsworks.ai/rdrs2/internal/integrationtests/ping" \
#     ./mtr --suite rdrs2-golang
#
# The recorded result is intentionally package-count independent: on success
# each run prints a constant "PASS", so a full run and a single-package run
# produce identical output and both match the .result. The real `go test`
# output is captured and only emitted when a run fails, in which case the test
# also exits non-zero so MTR fails it regardless of the output.
#

--source include/have_ndb.inc

perl END_OF_PERL;

use strict;
use warnings;
use File::Spec;

my $MYSQL_BASEDIR = $ENV{'MYSQL_BASEDIR'};
my $TEST_GO_DIR = File::Spec->catdir($MYSQL_BASEDIR, 'storage', 'ndb',
                                     'rest-server2', 'server', 'test_go');
die "Directory $TEST_GO_DIR does not exist\n" unless -d $TEST_GO_DIR;

# Switch to the test_go module for `go test`.
chdir $TEST_GO_DIR or die "Cannot change directory to $TEST_GO_DIR: $!";

# Packages to run. Default is every package (enumerated with `go list ./...`);
# a caller can narrow it via RDRS_GO_PACKAGE (space-separated list allowed).
#
# Each package is run as its own `go test` invocation on purpose: when several
# packages are passed to one `go test`, it buffers each package's output and
# only flushes it when that package finishes, so `tail -f` sees nothing until a
# package completes. A single-package invocation streams live instead. Running
# them one at a time therefore lets progress be followed live, and the shared
# "sentinel" DB means hopsworks is still seeded only once, by whichever package
# runs first.
my @packages;
if (defined $ENV{'RDRS_GO_PACKAGE'} && $ENV{'RDRS_GO_PACKAGE'} ne '') {
    @packages = split(' ', $ENV{'RDRS_GO_PACKAGE'});
} else {
    @packages = split(/\n/, `go list ./... 2>/dev/null`);
}
die "No Go packages found to test\n" unless @packages;

# The full go test output is streamed here live, for both success and failure,
# so progress can be watched with `tail -f`. This is deliberately NOT
# var/log/<test>.log: MTR deletes that file when the test passes and it is not
# reliably populated on failure. This file is left in place so the detailed
# output can always be inspected afterwards.
my $log_dir = $ENV{'MYSQLTEST_VARDIR'} ? "$ENV{'MYSQLTEST_VARDIR'}/log"
                                       : $ENV{'MYSQL_TMP_DIR'};
my $go_log_file = "$log_dir/rdrs2-golang_gotest.output.log";
# Start each run with a fresh file.
if (open(my $trunc, '>', $go_log_file)) { close($trunc); }

sub log_line {
    if (open(my $fh, '>>', $go_log_file)) { print $fh $_[0]; close($fh); }
}

sub run_test {
    my ($label) = @_;
    log_line("## $label\n");

    foreach my $pkg (@packages) {
        # Stream this package straight to the log file so `tail -f` follows it
        # live.
        # * `-v`       per-test progress.
        # * `-count 1` disables go's test-result cache, which cannot see
        #              changes in the C++ sources.
        log_line("### $pkg\n");
        system("go test -v -count 1 $pkg >> \"$go_log_file\" 2>&1");
        my $exit_code = $? >> 8;
        next if $exit_code == 0;

        # Failure: echo the streamed log to stdout so it is visible in the MTR
        # console, and exit non-zero so MTR fails the test regardless of result
        # matching.
        print "## $label\n";
        print "FAIL: `go test $pkg` exited with code $exit_code\n";
        print "Full log: $go_log_file\n";
        if (open(my $fh, '<', $go_log_file)) { print while <$fh>; close($fh); }
        exit $exit_code;
    }

    # Constant, package-count independent output so the recorded result is the
    # same whether we ran all packages or just one.
    print "## $label\n";
    print "PASS\n";
}

$ENV{'RDRS_CONFIG_FILE'} = $ENV{'RDRS_CONFIG_FILE_NO_TLS'};
run_test('Go tests (without TLS)');

$ENV{'RDRS_CONFIG_FILE'} = $ENV{'RDRS_CONFIG_FILE_TLS'};
run_test('Go tests (with TLS and server-side cert)');

exit 0;
END_OF_PERL

# Cleanup on default connection (mysqld.1.1)
--disable_query_log
--disable_warnings

# The Golang tests create and drop many databases in quick succession;
# under load the schema distribution can time out transiently and mysqld
# logs a warning that would otherwise fail the test's error log check.
call mtr.add_suppression("Schema dist coordinator detected timeout");

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
DROP DATABASE IF EXISTS fsdb004;
DROP DATABASE IF EXISTS usera_project;
DROP DATABASE IF EXISTS rdrs_bench;
DROP DATABASE IF EXISTS sentinel;

# Also cleanup on mysqld.2.1 to avoid NDB schema distribution delay
# causing check-testcase to see stale databases.
--connect(cleanup_server2, 127.0.0.1, root, , test, $MASTER_MYPORT1)

call mtr.add_suppression("Schema dist coordinator detected timeout");

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
DROP DATABASE IF EXISTS fsdb004;
DROP DATABASE IF EXISTS usera_project;
DROP DATABASE IF EXISTS rdrs_bench;
DROP DATABASE IF EXISTS sentinel;

--disconnect cleanup_server2
--connection default

# The Go tests provision USER rate-limit entities: the test API key prefix
# (testutils.ProvisionDefaultRateLimitUsers, apikey identity mode) and the
# project-user identities of the databases the rate limit tests bill to
# (username identity mode, server 2; see testutils.rateLimitIdentity).
# Drop them here so check-testcase sees no leftover $-entities in the data
# nodes. Tolerate failure: an entity does not exist when the Go tests were
# skipped or ran in only one identity mode. The result log is disabled
# because the commands' output depends on whether the entity existed.
--disable_result_log
--error 0,255,65280
--exec $NDB_MGM -e "USER DROP bkYjEz6OTZyevbqt"
--error 0,255,65280
--exec $NDB_MGM -e "USER DROP db004_macho"
--error 0,255,65280
--exec $NDB_MGM -e "USER DROP db029_macho"
--error 0,255,65280
--exec $NDB_MGM -e "USER DROP fsdb001_macho"
--enable_result_log

--enable_warnings
--enable_query_log
