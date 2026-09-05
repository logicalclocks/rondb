#
# Run the RDRS2 C++ test binaries (gtest, from
# storage/ndb/rest-server2/server/test) from MTR.
#
# They connect to the cluster directly through the NDB API and reuse the
# test databases the Go test module seeds. They used to run at the end of
# rdrs2-golang_gotest, but that pushed the whole test to 14-15 minutes on
# a fast idle machine - within seconds of MTR's default 15 minute testcase
# timeout, and well past it on a loaded machine - so they run as their own
# MTR test instead.
#
# The databases are seeded by running one quick Go test first: any
# integration package's TestMain (InitialiseTesting) creates ALL test
# databases when the "sentinel" database is absent, and its readiness
# barrier only returns once the seed is actually usable. This test cleans
# the databases up again at the end (as rdrs2-golang_gotest also does), so
# each of the two tests seeds afresh and they can run in either order.
#
# The same constant-output convention as run_gotest.inc applies: on
# success each section prints only "PASS" so the recorded result is
# stable; the full go test / gtest output is streamed to a log file that
# is left in place (var/log/rdrs2-golang_cpptest.output.log), and echoed
# to the MTR console only on failure.
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

# Switch to the test_go module for the seeding `go test`.
chdir $TEST_GO_DIR or die "Cannot change directory to $TEST_GO_DIR: $!";

# The full output is streamed here live, for both success and failure, so
# progress can be watched with `tail -f`. This is deliberately NOT
# var/log/<test>.log: MTR deletes that file when the test passes and it is
# not reliably populated on failure. This file is left in place so the
# detailed output can always be inspected afterwards.
my $log_dir = $ENV{'MYSQLTEST_VARDIR'} ? "$ENV{'MYSQLTEST_VARDIR'}/log"
                                       : $ENV{'MYSQL_TMP_DIR'};
my $log_file = "$log_dir/rdrs2-golang_cpptest.output.log";
# Start each run with a fresh file.
if (open(my $trunc, '>', $log_file)) { close($trunc); }

sub log_line {
    if (open(my $fh, '>>', $log_file)) { print $fh $_[0]; close($fh); }
}

sub fail_run {
    my ($label, $message) = @_;
    # Echo the streamed log to stdout so it is visible in the MTR console,
    # and exit non-zero so MTR fails the test regardless of result
    # matching.
    print "## $label\n";
    print "FAIL: $message\n";
    print "Full log: $log_file\n";
    if (open(my $fh, '<', $log_file)) { print while <$fh>; close($fh); }
    exit 1;
}

# ---------------------------------------------------------------------------
# Seed the test databases with one quick Go test (see the header comment).
# The C++ tests use the plain-HTTP server's config for the cluster
# connect string, so seed against the same server.
# ---------------------------------------------------------------------------
$ENV{'RDRS_CONFIG_FILE'} = $ENV{'RDRS_CONFIG_FILE_NO_TLS'};
my $seed_pkg  = 'hopsworks.ai/rdrs2/internal/integrationtests/ping';
my $seed_test = 'TestPing';

log_line("## Seed Go test ($seed_pkg -> $seed_test)\n");
# Stream the seed's output straight to the log file so the (minutes-long)
# database creation can be followed live with `tail -f`, same as the C++
# binaries below. Remember where this section starts so the no-tests
# guard below can re-read just the seed's output.
my $seed_out_start = -s $log_file;
system("go test -v -count 1 -run '^${seed_test}\$' $seed_pkg " .
       ">> \"$log_file\" 2>&1");
my $exit_code = $? >> 8;
if ($exit_code != 0) {
    fail_run('Seed Go test',
             "`go test -run '^${seed_test}\$' $seed_pkg` exited with code " .
             $exit_code);
}
# `go test -run` with a pattern matching no test exits 0, so a renamed or
# deleted seed test would silently skip the seeding (and its readiness
# barrier). Fail loudly instead so the choice above cannot rot.
my $seed_out = '';
if (open(my $fh, '<', $log_file)) {
    seek($fh, $seed_out_start, 0);
    local $/;
    $seed_out = <$fh>;
    close($fh);
}
if ($seed_out =~ /no tests to run/) {
    fail_run('Seed Go test',
             "seed test $seed_test not found in $seed_pkg " .
             "(renamed or deleted? pick another quick test)");
}
print "## Seed Go test\n";
print "PASS\n";

# ---------------------------------------------------------------------------
# C++ tests.
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
    system("\"$exe\" >> \"$log_file\" 2>&1");
    $exit_code = $? >> 8;
    if ($exit_code == 0) {
        next;
    }

    print "## C++ tests\n";
    print "FAIL: `$test` exited with code $exit_code\n";
    print "Full log: $log_file\n";
    if (open(my $fh, '<', $log_file)) { print while <$fh>; close($fh); }
    exit $exit_code;
}
print "## C++ tests\n";
print "PASS\n";

exit 0;
END_OF_PERL

--source suite/rdrs2-golang/include/cleanup_testdbs.inc
