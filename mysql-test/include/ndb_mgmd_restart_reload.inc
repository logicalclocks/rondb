#
# Restart ndb_mgmd to apply config changes.
#
# The test should append new [cluster_config.*] sections to my.cnf
# BEFORE sourcing this include.
#
# MTR handles the config.ini generation automatically when it sees
# --config-change in the .expect file (see generate_ndb_config_ini()
# in mysql-test-run.pl).
#
# Requires:
#   NDB_MGMD_NODEID env var (set in test .cnf [ENV] section)
#

# Step 1: Write .expect file telling MTR to restart with config change
--write_file $MYSQLTEST_VARDIR/tmp/cluster_config.ndb_mgmd.1.1.expect
restart:--config-change
EOF

# Step 2: Stop ndb_mgmd
--perl
  use strict;
  my $ndb_mgm = $ENV{'NDB_MGM'} or die "NDB_MGM not set";
  my $mgmd_id = $ENV{'NDB_MGMD_NODEID'} or die "NDB_MGMD_NODEID not set";
  system("$ndb_mgm -e \"$mgmd_id STOP\" >/dev/null 2>&1");
EOF

# Step 3: Wait for MTR to process the .expect file and restart ndb_mgmd
--perl
  use strict;
  my $vardir = $ENV{'MYSQLTEST_VARDIR'} or die "MYSQLTEST_VARDIR not set";
  my $expect_file = "$vardir/tmp/cluster_config.ndb_mgmd.1.1.expect";

  my $timeout = 120;
  my $start = time();
  while (-f $expect_file) {
    if (time() - $start > $timeout) {
      die "Timeout waiting for MTR to process .expect file";
    }
    select(undef, undef, undef, 0.5);
  }
EOF

# Step 4: Wait for the restarted ndb_mgmd to open the MGM client port.
# The .expect file is removed by MTR just before ndb_mgmd is started, so
# step 3 says nothing about the new mgmd being usable. On top of that the
# mgmd keeps ordinary MGM client commands blocked until it has become
# arbitrator (or ArbitrationRankWait, default 60s, expires) and answers them
# with "Management server is waiting for arbitrator selection". Poll until
# a plain 'show' gets through, otherwise the first mgm command issued by the
# test races the arbitrator startup gate and fails with "Illegal node status".
--perl
  use strict;
  my $ndb_mgm = $ENV{'NDB_MGM'} or die "NDB_MGM not set";

  my $timeout = 180;
  my $start = time();
  while (1) {
    my $out = `$ndb_mgm -e "show" 2>&1`;
    my $rc = $?;
    last if ($rc == 0 && $out !~ /waiting for arbitrator selection/);
    if (time() - $start > $timeout) {
      die "Timeout waiting for restarted ndb_mgmd to accept commands: $out";
    }
    select(undef, undef, undef, 0.5);
  }
EOF

# Step 5: Wait for data nodes to reconnect to the restarted mgmd
# and reach STARTED state (not just mgmd contactable)
--exec $NDB_WAITER --timeout=120 > /dev/null 2>&1
