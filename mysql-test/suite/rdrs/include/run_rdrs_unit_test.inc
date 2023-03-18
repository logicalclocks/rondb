--perl
use strict;
use lib "lib/";
use My::Exec;
use My::Platform;
use Cwd; 

# Check parameters
my $vardir = $ENV{MYSQLTEST_VARDIR} or die "Need MYSQLTEST_VARDIR";
my $rdrs_config_file = $ENV{RDRS_CONFIG_FILE} || die "ERROR: RDRS config not found";
my $rdrs_lib_dir = $ENV{RDRS_LIB_DIR} || die "ERROR: RDRS lib dir not set";
my $cgo_ldflags = $ENV{CGO_LDFLAGS} || die "ERROR: CGO_LDFLAGS not set";
my $rdrs_src_dir = $ENV{RDRS_SRC_DIR} || die "ERROR: RDRS src dir not set";
my $ld_library_path = $ENV{LD_LIBRARY_PATH} || die "ERROR: RDRS src dir not set";
my $rdrs_test_cmd = $ENV{RDRS_TEST_CMD} || die "ERROR: RDRS src dir not set";

my $F = IO::File->new("$vardir/tmp/run_rdrs_unit_test.result", "w") || die "Couldn't open varfile for writing";

my $cwd = getcwd();
chdir($rdrs_src_dir);

my $cmd = "go clean -testcache";
my $res = exec_print_on_error($cmd, 1000);
if (!$res)
{
  # Rather than --die, we change the output
  # That will cause a result-content-mismatch giving more
  # visible context info
  print $F "--echo \'$cmd\' run failed;\n";
}


my $cmd="export DYLD_LIBRARY_PATH=$ENV{LD_LIBRARY_PATH}; export LD_LIBRARY_PATH=$ENV{LD_LIBRARY_PATH}; export CGO_LDFLAGS=$ENV{CGO_LDFLAGS}; $rdrs_test_cmd";
my $res = exec_print_on_error($cmd, 1000);
if ($res)
{
  print $F "# Success\n";
}
else
{
  print $F "--echo \'$cmd\' run failed;\n";
}
$F->close();

chdir($cwd);

EOF

--source $MYSQLTEST_VARDIR/tmp/run_rdrs_unit_test.result
