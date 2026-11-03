#
# Run one group of the RDRS2 Golang test packages from MTR. (The RDRS2 C++
# test binaries, which reuse the same seeded databases, run as the separate
# MTR test cpptest - see include/run_cpptest.inc.)
#
# The Go packages are split into groups, one MTR test per group (RONDB-1152):
# the whole suite in one MTR test took 10-15 minutes, within a hair of the
# 900 s testcase timeout on x86 and past it on arm, and a single test
# cannot use more than one MTR worker. Each group is a separate test
# t/<group>.test, named after the packages it runs, that sets RDRS_GO_GROUP
# and sources this file, so with --parallel the groups run on different
# workers at the same time and each has its own 900 s. A group runs every
# test of its packages.
#
# The groups (%groups below):
#   pkread_batchpkread, index_scan,
#   feature_store_batchfeaturestore_sharing, ronsql
#                the packages of that name;
#   ping_health_stat_smoke
#                the remaining integration packages, the unit packages, and
#                the plain-HTTP smoke pass (see %smoke_test).
#
# Every integration package found by `go list ./...` must be in exactly one
# group, otherwise the run fails: a package added to the tree but to no
# group would otherwise silently never run. Packages outside
# internal/integrationtests (unit tests) belong to the last group
# automatically.
#
# The Go tests run in two passes:
#   1. the group's packages against the TLS server (rdrs.2.1): every
#      handler plus the production transport;
#   2. (smoke group only) one hand-picked SIMPLE test per integration
#      package against the plain-HTTP server (rdrs.1.1): plain HTTP is a
#      shipped configuration with its own config-validation and
#      listener-bootstrap code, and one test per package proves it serves
#      every endpoint family without paying for the full suite twice.
#
# Within one MTR test the databases are seeded once (the first package that
# runs creates the "sentinel" DB; the rest reuse it) and the cluster is
# torn down once.
#
# To run only one (or a few) packages, set RDRS_GO_PACKAGE to a Go package
# name (or a space-separated list) and run ONE of the group tests, e.g.:
#
#   RDRS_GO_PACKAGE=hopsworks.ai/rdrs2/internal/integrationtests/pkread \
#     ./mtr rdrs2-golang.pkread_batchpkread
#
# The override replaces the group's packages. The package names are exactly
# what `go list ./...` prints from the test_go module directory
# (storage/ndb/rest-server2/server/test_go).
#
# The recorded result is intentionally package-count independent: on success
# each pass prints a constant "PASS", so a full run and a single-package run
# produce identical output and both match the .result. The real `go test`
# output is streamed to var/log/rdrs2-golang_<group>.output.log and only
# echoed when a run fails, in which case the test also exits non-zero so MTR
# fails it regardless of the output.
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

my $it = 'hopsworks.ai/rdrs2/internal/integrationtests';

# group name => its packages, and whether the plain-HTTP smoke pass runs.
my %groups = (
    'pkread_batchpkread' => {
        packages => [ map { "$it/$_" } qw(pkread batchpkread) ],
        smoke    => 0,
    },
    'index_scan' => {
        packages => [ "$it/index_scan" ],
        smoke    => 0,
    },
    'feature_store_batchfeaturestore_sharing' => {
        packages => [ map { "$it/$_" } qw(feature_store batchfeaturestore
                                           sharing) ],
        smoke    => 0,
    },
    'ronsql' => {
        packages => [ "$it/ronsql" ],
        smoke    => 0,
    },
    # bufferpool and starvation skip here (their tests are driven by the
    # buffer_pool, memory_regression and ping_starvation tests);
    # spillfixture and testclient are helper packages without tests. All
    # are listed so the coverage check below can stay strict.
    'ping_health_stat_smoke' => {
        packages => [ map { "$it/$_" } qw(ping health stat bufferpool
                                           starvation spillfixture
                                           testclient) ],
        smoke    => 1,
    },
);

# Every integration package must be claimed by exactly one group.
my %claimed;
foreach my $name (sort keys %groups) {
    foreach my $pkg (@{ $groups{$name}{packages} }) {
        die "package $pkg is in groups $claimed{$pkg} and $name\n"
            if exists $claimed{$pkg};
        $claimed{$pkg} = $name;
    }
}

my @all_packages = split(/\n/, `go list ./... 2>/dev/null`);
die "No Go packages found to test\n" unless @all_packages;
my %exists = map { $_ => 1 } @all_packages;

# Unit packages (outside internal/integrationtests) go to the smoke group;
# an integration package nobody claims is an error, see the header.
foreach my $pkg (@all_packages) {
    if ($pkg !~ m{^\Q$it\E/}) {
        push @{ $groups{'ping_health_stat_smoke'}{packages} }, $pkg;
    } elsif (!exists $claimed{$pkg}) {
        die "Go package $pkg belongs to no group: add it to \%groups in " .
            "suite/rdrs2-golang/include/run_gotest.inc\n";
    }
}
foreach my $pkg (keys %claimed) {
    die "Go package $pkg of group $claimed{$pkg} does not exist " .
        "(removed or renamed? update \%groups in run_gotest.inc)\n"
        unless $exists{$pkg};
}

my $group_name = $ENV{'RDRS_GO_GROUP'};
my $group;
my @packages;
if (defined $ENV{'RDRS_GO_PACKAGE'} && $ENV{'RDRS_GO_PACKAGE'} ne '') {
    # Developer override: these packages instead of the group's.
    $group_name = 'override';
    @packages = split(' ', $ENV{'RDRS_GO_PACKAGE'});
    $group = { smoke => 1 };
} else {
    die "RDRS_GO_GROUP is not set: source this file from a " .
        "t/<group>.test that sets it\n"
        unless defined $group_name && $group_name ne '';
    $group = $groups{$group_name}
        or die "unknown Go test group '$group_name'; known: " .
               join(', ', sort keys %groups) . "\n";
    @packages = @{ $group->{packages} };
}
die "No Go packages to test\n" unless @packages;

# The full go test output is streamed here live, for both success and failure,
# so progress can be watched with `tail -f`. This is deliberately NOT
# var/log/<test>.log: MTR deletes that file when the test passes and it is not
# reliably populated on failure. This file is left in place so the detailed
# output can always be inspected afterwards.
my $log_dir = $ENV{'MYSQLTEST_VARDIR'} ? "$ENV{'MYSQLTEST_VARDIR'}/log"
                                       : $ENV{'MYSQL_TMP_DIR'};
my $go_log_file = "$log_dir/rdrs2-golang_$group_name.output.log";
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
    log_line("### group $group_name\n");

    # Each package is run as its own `go test` invocation on purpose: when
    # several packages are passed to one `go test`, it buffers each
    # package's output and only flushes it when that package finishes, so
    # `tail -f` sees nothing until a package completes. A single-package
    # invocation streams live instead, and the shared "sentinel" DB means
    # the databases are still seeded only once, by whichever package runs
    # first.
    foreach my $pkg (@packages) {
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
# exercised. The ronsql test covers the RonSQL endpoint.
my %smoke_test = (
    "$it/ping"              => 'TestPing',
    "$it/health"            => 'TestHealth',
    "$it/pkread"            => 'TestDataTypesInt',
    "$it/batchpkread"       => 'TestBatchSimple',
    "$it/index_scan"        => 'Test_SimpleComparison',
    "$it/feature_store"     => 'Test_GetFeatureVector_Success',
    "$it/batchfeaturestore" => 'Test_GetFeatureVector_Success_1entries',
    "$it/sharing"           => 'Test_Sharing_PKRead',
);

sub run_smoke_tests {
    my ($label) = @_;
    log_line("## $label\n");

    # The smoke pass covers every endpoint family, so it runs the packages
    # of the map, not those of the group (with the developer override, only
    # the overridden packages that have a smoke test).
    my @smoke_packages = $group_name eq 'override'
        ? grep { exists $smoke_test{$_} } @packages
        : sort keys %smoke_test;
    foreach my $pkg (@smoke_packages) {
        my $test = $smoke_test{$pkg};
        if (!$exists{$pkg}) {
            fail_run($label, "smoke test package $pkg does not exist " .
                             "(removed or renamed? update the smoke_test map)");
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

# The group runs over TLS: it exercises every handler AND the production
# transport. Plain HTTP is still a shipped configuration with its own
# config-validation and listener-bootstrap code, so the smoke group gives it
# one simple test from every integration package - enough to prove the
# plain listener serves each endpoint family.
$ENV{'RDRS_CONFIG_FILE'} = $ENV{'RDRS_CONFIG_FILE_TLS'};
run_test('Go tests (with TLS and server-side cert)');

if ($group->{smoke}) {
    $ENV{'RDRS_CONFIG_FILE'} = $ENV{'RDRS_CONFIG_FILE_NO_TLS'};
    run_smoke_tests('Go tests (without TLS, one simple test per package)');
}

exit 0;
END_OF_PERL

--source suite/rdrs2-golang/include/cleanup_testdbs.inc
