#
# Restart ndb_mgmd to apply config changes.
#
# Generates a complete config.ini from ALL [cluster_config*] and [api]
# sections in the generated my.cnf (which has inlined all !include files).
# Restarts ndb_mgmd with --config-file pointing to it, using --no-defaults
# to avoid mixing my.cnf and config.ini formats.
#
# The test should append new [cluster_config.*] sections to my.cnf
# BEFORE sourcing this include.
#
# Requires:
#   NDB_MGMD_NODEID env var (set in test .cnf [ENV] section)
#

# Step 1: Generate complete config.ini from my.cnf
--perl
  use strict;

  my $vardir = $ENV{'MYSQLTEST_VARDIR'} or die "MYSQLTEST_VARDIR not set";
  my $mycnf = "$vardir/my.cnf";
  my $config_ini = "$vardir/tmp/ndb_config.ini";

  # Parse my.cnf - extract all sections
  open(my $fh, '<', $mycnf) or die "Cannot read $mycnf: $!";
  my %sections;
  my @section_order;
  my $current;
  while (my $line = <$fh>) {
    chomp $line;
    $line =~ s/\r$//;
    next if $line =~ /^[#;]/ || $line =~ /^\s*$/;

    if ($line =~ /^\[(.+)\]$/) {
      $current = $1;
      if (!exists $sections{$current}) {
        $sections{$current} = [];
        push @section_order, $current;
      }
      next;
    }

    if (defined $current && $line =~ /^(\S+?)\s*=\s*(.*)$/) {
      my ($key, $val) = ($1, $2);
      $val =~ s/\s+$//;
      push @{$sections{$current}}, [$key, $val];
    }
  }
  close $fh;

  # MTR meta-variable names to skip
  my %skip_key = map { $_ => 1 } qw(ndbd ndb_mgmd mysqld ndbapi);

  # MTR-internal options to skip (start with '#', or MTR-only like Dedicated)
  sub should_skip {
    my ($key) = @_;
    return 1 if $skip_key{$key};
    return 1 if $key =~ /^#/;
    return 0;
  }

  # Map my.cnf section type to config.ini header
  my %type_to_header = (
    'ndbd'     => 'ndbd',
    'ndb_mgmd' => 'ndb_mgmd',
    'mysqld'   => 'mysqld',
    'ndbapi'   => 'api',
  );

  # Build parameter-to-section mapping from ConfigInfo.cpp.
  # Each parameter definition has: {CFG_*, "ParamName", "SECTION", ...}
  # where SECTION is DB, API, MGM, TCP, SHM, or SYSTEM.
  my %param_section;  # param_name => section (DB, API, MGM, TCP, ...)
  {
    my $srcdir = $ENV{'MYSQL_TEST_DIR'} || "$vardir/..";
    # Walk up from vardir to find the source tree
    my $config_info;
    for my $base ($ENV{'MYSQL_TEST_DIR'}, "$vardir/..", "$vardir/../..") {
      next unless defined $base;
      # Try source tree relative paths
      for my $rel ("storage/ndb/src/common/mgmcommon/ConfigInfo.cpp",
                   "../storage/ndb/src/common/mgmcommon/ConfigInfo.cpp",
                   "../../storage/ndb/src/common/mgmcommon/ConfigInfo.cpp") {
        my $path = "$base/$rel";
        if (-f $path) {
          $config_info = $path;
          last;
        }
      }
      last if $config_info;
    }
    if ($config_info && open(my $ci, '<', $config_info)) {
      # Read entire file to handle multi-line definitions
      my $content = do { local $/; <$ci> };
      close $ci;
      # Match definitions: {CFG_*, "ParamName", "SECTION"  or TOKEN
      # May span multiple lines
      while ($content =~ /\{\s*CFG_\w+\s*,\s*"(\w+)"\s*,\s*(?:"(\w+)"|(\w+_TOKEN))/g) {
        my $name = $1;
        my $sec = $2 || $3;
        next if $sec && ($sec eq 'SCI' || $sec eq 'SHM');  # Skip SCI/SHM, tests use TCP
        $sec = 'DB'  if $sec && $sec eq 'DB_TOKEN';
        $sec = 'API' if $sec && $sec eq 'API_TOKEN';
        $sec = 'MGM' if $sec && $sec eq 'MGM_TOKEN';
        # Keep first match (e.g. TCP before SHM for Checksum)
        $param_section{$name} = $sec if $sec && !$param_section{$name};
      }
      my $dbg = "$vardir/tmp/configinfo_debug.txt";
      open(my $df, '>', $dbg);
      print $df "Parsed " . scalar(keys %param_section) .
                " params from $config_info\n";
      print $df "Checksum=" .
                ($param_section{'Checksum'} || 'NOT FOUND') . "\n";
      print $df "DefaultOperationRedoProblemAction=" .
                ($param_section{'DefaultOperationRedoProblemAction'} || 'NOT FOUND') . "\n";
      print $df "DataMemory=" .
                ($param_section{'DataMemory'} || 'NOT FOUND') . "\n";
      close $df;
    } else {
      my $dbg = "$vardir/tmp/configinfo_debug.txt";
      open(my $df, '>', $dbg);
      print $df "ConfigInfo.cpp NOT FOUND\n";
      print $df "MYSQL_TEST_DIR=" . ($ENV{'MYSQL_TEST_DIR'} || 'undef') . "\n";
      print $df "vardir=$vardir\n";
      close $df;
    }
  }

  open(my $out, '>', $config_ini)
    or die "Cannot write $config_ini: $!";

  # 1) Collect defaults from [cluster_config] and [cluster_config.N],
  #    separating ndbd defaults from tcp defaults
  {
    my @ndbd_defaults;
    my @tcp_defaults;
    my @all_default_pairs;

    # Collect from [cluster_config] (from default_ndbd.cnf)
    if (my $opts = $sections{'cluster_config'}) {
      push @all_default_pairs, @$opts;
    }
    # Collect from [cluster_config.N] sections
    for my $name (@section_order) {
      next unless $name =~ /^cluster_config\.\d+$/;
      push @all_default_pairs, @{$sections{$name}};
    }

    my @api_extra_defaults;
    my @mgm_defaults;
    for my $pair (@all_default_pairs) {
      next if should_skip($pair->[0]);
      my $sec = $param_section{$pair->[0]} || 'DB';
      my $entry = "$pair->[0]=$pair->[1]";
      if    ($sec eq 'TCP' || $sec eq 'SHM') { push @tcp_defaults, $entry; }
      elsif ($sec eq 'API')                  { push @api_extra_defaults, $entry; }
      elsif ($sec eq 'MGM')                  { push @mgm_defaults, $entry; }
      else                                   { push @ndbd_defaults, $entry; }
    }

    if (@ndbd_defaults) {
      print $out "[ndbd default]\n";
      print $out "$_\n" for @ndbd_defaults;
      print $out "\n";
    }
    if (@tcp_defaults) {
      print $out "[tcp default]\n";
      print $out "$_\n" for @tcp_defaults;
      print $out "\n";
    }
    # Store api/mgm extras to merge with their default sections below
    $sections{'_api_extra_defaults'} = \@api_extra_defaults;
  }

  # 2) [api default] from [api] section (e.g. UseOnlyIPv4=1)
  #    plus any api-level params from [cluster_config] sections
  {
    my @api_defaults;
    if (my $opts = $sections{'api'}) {
      for my $pair (@$opts) {
        next if should_skip($pair->[0]);
        push @api_defaults, "$pair->[0]=$pair->[1]";
      }
    }
    if (my $extras = $sections{'_api_extra_defaults'}) {
      push @api_defaults, @$extras;
    }
    if (@api_defaults) {
      print $out "[api default]\n";
      print $out "$_\n" for @api_defaults;
      print $out "\n";
    }
  }

  # 3) Individual node sections: [cluster_config.TYPE.N.SUFFIX] -> [TYPE]
  for my $name (@section_order) {
    next unless $name =~ /^cluster_config\.(\w+)\.\d+\.\d+$/;
    my $type = $1;
    my $header = $type_to_header{$type};
    next unless defined $header;

    my @node_opts;
    for my $pair (@{$sections{$name}}) {
      next if should_skip($pair->[0]);
      push @node_opts, "$pair->[0]=$pair->[1]";
    }
    print $out "[$header]\n";
    print $out "$_\n" for @node_opts;
    print $out "\n";
  }

  close $out;
EOF

# Step 2: Write .expect file for MTR
--perl
  use strict;
  my $vardir = $ENV{'MYSQLTEST_VARDIR'} or die "MYSQLTEST_VARDIR not set";
  my $config_ini = "$vardir/tmp/ndb_config.ini";
  my $expect_file = "$vardir/tmp/cluster_config.ndb_mgmd.1.1.expect";
  open(my $fh, '>', $expect_file)
    or die "Cannot write $expect_file: $!";
  print $fh "restart:--reload --config-file=$config_ini";
  close $fh;
EOF

# Step 3: Stop ndb_mgmd
--perl
  use strict;
  my $ndb_mgm = $ENV{'NDB_MGM'} or die "NDB_MGM not set";
  my $mgmd_id = $ENV{'NDB_MGMD_NODEID'} or die "NDB_MGMD_NODEID not set";
  system("$ndb_mgm -e \"$mgmd_id STOP\" >/dev/null 2>&1");
EOF

# Step 4: Wait for MTR to process the .expect file and restart ndb_mgmd
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

# Step 5: Wait for data nodes to reconnect to the restarted mgmd
# and reach STARTED state (not just mgmd contactable)
--exec $NDB_WAITER --timeout=120 > /dev/null 2>&1
