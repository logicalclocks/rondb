# Wait for cookie-matched completion, not a fixed cleanup sleep.
# PG_STAGE=freed requires a completed chain on every node and at least
# one chain exceeding 256 pages. PG_STAGE=checked requires DUMP 2364 ACKs.
--perl
  use strict;
  use warnings;
  my $cookie = $ENV{'PG_COOKIE'};
  my @nodes = split /,/, $ENV{'PG_NODES'};
  my $dir = $ENV{'PG_LOG_DIR'};
  my $stage = $ENV{'PG_STAGE'};
  die "Invalid PG-1 cookie/nodes/stage"
    unless $cookie =~ /^[1-9][0-9]*$/ && @nodes >= 2
      && scalar(grep { !/^[1-9][0-9]*$/ } @nodes) == 0
      && ($stage eq 'freed' || $stage eq 'checked');
  my $deadline = time() + 30;
  my (%seen, $multi_batch);
  while (time() < $deadline) {
    for my $file (glob("$dir/ndb_*_cluster.log*")) {
      next unless open(my $fh, '<', $file);
      while (my $line = <$fh>) {
        if ($stage eq 'freed' &&
            $line =~ /\[CTE_REDIST_PAGES_FREED node=(\d+) pages=(\d+) cookie=$cookie\]/) {
          $seen{$1} = 1;
          $multi_batch = 1 if $2 > 256;
        }
        if ($stage eq 'checked' &&
            $line =~ /\[JOIN_AGG_LEAK_CHECK_OK node=(\d+) dump=2364 cookie=$cookie\]/) {
          $seen{$1} = 1;
        }
      }
      close($fh);
    }
    my @missing = grep { !$seen{$_} } @nodes;
    exit 0 if !@missing && ($stage eq 'checked' || $multi_batch);
    select(undef, undef, undef, 0.1);
  }
  die "PG-1 $stage: missing nodes [" .
      join(',', grep { !$seen{$_} } @nodes) .
      "], multi-batch completion=" . ($multi_batch ? 1 : 0);
EOF
