#
# Run the RDRS2 Golang tests from MTR, followed by the RDRS2 C++ test
# binaries (which reuse the databases the Go tests seeded).
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

# Suppress benign "Failed to acquire MDL for db" errors from the schema
# distribution participant.  At the end of this include we run DROP
# DATABASE directly on both mysqld.1.1 and mysqld.2.1 (to avoid waiting
# for schema distribution propagation, see comment near the second
# cleanup block below).  When the direct DROP on one server races the
# participant action triggered by the OTHER server's DROP, the
# participant's MDL_INTENTION_EXCLUSIVE request can time out against
# the MDL_EXCLUSIVE held by the direct DROP — logged as MY-010865.
# Either path ends with the database dropped, so the error is benign;
# only intermittent on Linux where scheduling can serialize the two
# cleanup connections in the racey order.
--disable_query_log
--disable_warnings
call mtr.add_suppression("Failed to acquire MDL for db");
--connect(rdrs2_suppress_server2, 127.0.0.1, root, , test, $MASTER_MYPORT1)
call mtr.add_suppression("Failed to acquire MDL for db");
--disconnect rdrs2_suppress_server2
--connection default
--enable_warnings
--enable_query_log

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

# ---------------------------------------------------------------------------
# C++ tests (gtest binaries from storage/ndb/rest-server2/server/test).
#
# They connect to the cluster directly through the NDB API and reuse the
# databases the Go tests above seeded, so they MUST run here - after the Go
# tests, but before the cleanup section below drops those databases.
#
# The same constant-output convention as run_test applies: on success only
# "PASS" is printed so the recorded result is stable; each binary's full
# gtest output is streamed to the log file, and echoed to the MTR console
# only on failure.
# ---------------------------------------------------------------------------
my @cpp_tests = ('error_handling_test',   # pure error-classification tests
                 'reconnect_test',        # RONDB-1104 reconnection cycle
                 'api_key_test',
                 'feature_store_test');

# Source builds place binaries in runtime_output_directory, packaged
# builds in bin; multi-config generators add a configuration subdirectory.
my $bindir = $ENV{'MYSQL_BINDIR'} // $MYSQL_BASEDIR;
my @bin_candidates;
foreach my $dir ('runtime_output_directory', 'bin') {
    push(@bin_candidates, File::Spec->catdir($bindir, $dir));
    foreach my $cfg ('Release', 'RelWithDebInfo', 'Debug') {
        push(@bin_candidates, File::Spec->catdir($bindir, $dir, $cfg));
    }
}

$ENV{'RDRS_CONFIG_FILE'} = $ENV{'RDRS_CONFIG_FILE_NO_TLS'};
log_line("## C++ tests\n");
foreach my $test (@cpp_tests) {
    my ($exe) = grep { -x $_ }
                map { File::Spec->catfile($_, $test) } @bin_candidates;
    if (!defined $exe) {
        # Every listed binary is built whenever WITH_RDRS is on, which this
        # suite requires, so a missing one is a broken or stale build
        # directory - not an optional test program. Skipping it would drop
        # its coverage while the section still printed PASS: a stale build
        # tree really can contain some of these binaries and not others (a
        # partial `make <target>` after adding a test does exactly that).
        print "## C++ tests\n";
        print "FAIL: test binary '$test' not found under: @bin_candidates\n";
        exit 1;
    }
    log_line("### $exe\n");
    # NOTE: the redirect makes perl run this through `sh -c`, and the shell
    # reports a signal death as exit code 128+N, so a crashing test binary
    # fails the check below. Do NOT "simplify" this to list-form system()
    # or a bare command: a direct signal death then yields $? >> 8 == 0 and
    # a crash would silently PASS.
    system("\"$exe\" >> \"$go_log_file\" 2>&1");
    my $exit_code = $? >> 8;
    if ($exit_code == 0) {
        next;
    }

    print "## C++ tests\n";
    print "FAIL: `$test` exited with code $exit_code\n";
    print "Full log: $go_log_file\n";
    if (open(my $fh, '<', $go_log_file)) { print while <$fh>; close($fh); }
    exit $exit_code;
}
print "## C++ tests\n";
print "PASS\n";

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

# The Go tests provision a USER rate-limit entity for the test API key
# prefix (testutils.ProvisionDefaultRateLimitUsers, key constant
# HOPSWORKS_TEST_API_KEY). Drop it here so check-testcase sees no leftover
# $-entity in the data nodes. Tolerate failure: the entity does not exist
# when the Go tests were skipped. The result log is disabled because the
# command's output depends on whether the entity existed.
--disable_result_log
--error 0,255,65280
--exec $NDB_MGM -e "USER DROP bkYjEz6OTZyevbqt"
--enable_result_log

--enable_warnings
--enable_query_log
