#
# Run the RDRS2 Golang tests from MTR. (The RDRS2 C++ test binaries,
# which reuse the same seeded databases, run as the separate MTR test
# rdrs2-golang_cpptest - see include/run_cpptest.inc - so that neither
# test runs close to the MTR testcase timeout.)
#
# The Go tests run in two passes:
#   1. the FULL suite against the TLS server (rdrs.2.1): every handler plus
#      the production transport;
#   2. one hand-picked SIMPLE test per integration package against the
#      plain-HTTP server (rdrs.1.1): plain HTTP is a shipped configuration
#      with its own config-validation and listener-bootstrap code, and one
#      test per package proves it serves every endpoint family without
#      paying for the full suite twice. See %smoke_test below.
#
# By default this covers ALL Golang test packages, and the hopsworks
# database is seeded once (the first package that runs creates the
# "sentinel" DB; the rest reuse it) with the cluster torn down once.
# Running every package in its own MTR test instead re-seeds hopsworks for
# each package, which is what made the suite slow.
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

sub fail_run {
    my ($label, $message) = @_;
    # Echo the streamed log to stdout so it is visible in the MTR console,
    # and exit non-zero so MTR fails the test regardless of result
    # matching.
    print "## $label\n";
    print "FAIL: $message\n";
    print "Full log: $go_log_file\n";
    if (open(my $fh, '<', $go_log_file)) { print while <$fh>; close($fh); }
    exit 1;
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
        fail_run($label, "`go test $pkg` exited with code $exit_code");
    }

    # Constant, package-count independent output so the recorded result is the
    # same whether we ran all packages or just one.
    print "## $label\n";
    print "PASS\n";
}

# One hand-picked SIMPLE test per integration package: enough to prove the
# configuration under test can serve every endpoint family, without paying
# for the full suite twice. Unit packages prove nothing about a listener
# configuration and are skipped. stat has no runnable tests (the endpoint
# is not implemented yet).
#
# ronsql is deliberately absent: its only runnable test is TestRateLimit,
# which skips unless rate limits are enabled (proving nothing about the
# listener) and otherwise floods the endpoint to provoke 429s - running it a
# second time here would disturb the rate limit bucket the TLS pass just
# exercised. The full TLS pass above covers the RonSQL endpoint.
my %smoke_test = (
    'hopsworks.ai/rdrs2/internal/integrationtests/ping'
        => 'TestPing',
    'hopsworks.ai/rdrs2/internal/integrationtests/health'
        => 'TestHealth',
    'hopsworks.ai/rdrs2/internal/integrationtests/pkread'
        => 'TestDataTypesInt',
    'hopsworks.ai/rdrs2/internal/integrationtests/batchpkread'
        => 'TestBatchSimple',
    'hopsworks.ai/rdrs2/internal/integrationtests/index_scan'
        => 'Test_SimpleComparison',
    'hopsworks.ai/rdrs2/internal/integrationtests/feature_store'
        => 'Test_GetFeatureVector_Success',
    'hopsworks.ai/rdrs2/internal/integrationtests/batchfeaturestore'
        => 'Test_GetFeatureVector_Success_1entries',
    'hopsworks.ai/rdrs2/internal/integrationtests/sharing'
        => 'Test_Sharing_PKRead',
);

sub run_smoke_tests {
    my ($label) = @_;
    log_line("## $label\n");

    foreach my $pkg (@packages) {
        my $test = $smoke_test{$pkg};
        if (!defined $test) {
            log_line("### $pkg: no smoke test configured, skipped\n");
            next;
        }
        log_line("### $pkg -> $test\n");
        my $out = `go test -v -count 1 -run '^${test}\$' $pkg 2>&1`;
        my $exit_code = $? >> 8;
        log_line($out);
        if ($exit_code != 0) {
            fail_run($label,
                     "`go test -run '^${test}\$' $pkg` exited with code " .
                     $exit_code);
        }
        # `go test -run` with a pattern matching no test exits 0, so a
        # renamed/deleted smoke test would silently turn this pass into a
        # no-op. Fail loudly instead so the list above cannot rot.
        if ($out =~ /no tests to run/) {
            fail_run($label,
                     "smoke test $test not found in $pkg " .
                     "(renamed or deleted? update the smoke_test map)");
        }
    }

    print "## $label\n";
    print "PASS\n";
}

# The full suite runs over TLS: it exercises every handler AND the
# production transport. Plain HTTP is still a shipped configuration with
# its own config-validation and listener-bootstrap code, so it gets one
# simple test from every integration package - enough to prove the plain
# listener serves each endpoint family.
$ENV{'RDRS_CONFIG_FILE'} = $ENV{'RDRS_CONFIG_FILE_TLS'};
run_test('Go tests (with TLS and server-side cert)');

$ENV{'RDRS_CONFIG_FILE'} = $ENV{'RDRS_CONFIG_FILE_NO_TLS'};
run_smoke_tests('Go tests (without TLS, one simple test per package)');

exit 0;
END_OF_PERL

--source suite/rdrs2-golang/include/cleanup_testdbs.inc
