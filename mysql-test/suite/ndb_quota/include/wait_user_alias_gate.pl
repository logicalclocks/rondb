# Waits until mgmd's user alias gate is refused or accepted, the first
# argument, polling USER ALIAS LIST. mgmd learns a data node's version
# from the node's heartbeat replies, so a change takes a few heartbeats.
use strict;

my $until = $ARGV[0] // '';
die "Expected refused or accepted, not '$until'"
  unless $until eq 'refused' || $until eq 'accepted';
my $mgm = $ENV{'NDB_MGM'} or die "NDB_MGM not set";
for (my $i = 0; $i < 120; $i++) {
  my $rc = system("$mgm -e \"USER ALIAS LIST\" > /dev/null 2>&1");
  if (($until eq 'refused') == ($rc != 0)) {
    print "The user alias gate is $until\n";
    exit(0);
  }
  select(undef, undef, undef, 0.5);
}
die "The user alias gate did not become $until within 60 seconds";
