# Counts the aliases ndb_user_objects_big created, and which of them and
# their target hold ids above the 10428 table and index ids.

--perl
use strict;
use IO::Socket::INET;
my ($host, $port) = split(/:/, (split(/[;,]/, $ENV{'NDB_CONNECTSTRING'}))[0]);
my $sock = IO::Socket::INET->new(PeerAddr => $host, PeerPort => $port,
                                 Proto => 'tcp') or die;
sub mgm {
  my ($cmd, %args) = @_;
  print $sock "$cmd\n";
  print $sock "$_: $args{$_}\n" for keys %args;
  print $sock "\n";
  my %header;
  while (defined(my $line = <$sock>)) {
    chomp $line;
    last if $line eq '';
    $header{$1} = $2 if $line =~ /^(\w+): (.*)$/;
  }
  my @rows;
  for (my $i = 0; $i < ($header{'num_rows'} || 0); $i++) {
    my $line = <$sock>;
    chomp $line;
    push(@rows, $line);
  }
  return (\%header, \@rows);
}
my ($h, $rows) = mgm('get user', 'username' => 'ub_target');
my ($user_id) = join("\n", @$rows) =~ /userId = (\d+)/;
my ($count, $above, $max) = (0, 0, 0);
my $next = 0;
while (1) {
  ($h, $rows) = mgm('list user alias', 'nextAliasId' => $next);
  last if !$h->{num_rows};
  my ($id) = $rows->[1] =~ /aliasId = (\d+)/;
  $count++;
  $above++ if $id >= 10428;
  $max = $id if $id > $max;
  $next = $h->{nextAliasId};
}
print "Aliases: $count, above the table ids: $above, highest id: $max,",
      " the user's id: $user_id\n";
close($sock);
EOF
