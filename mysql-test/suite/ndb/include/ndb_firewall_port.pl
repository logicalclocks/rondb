#!/usr/bin/perl
#
# Block or unblock new incoming TCP connections to a port of this host,
# see ndb_firewall_port.inc.
#
# Usage: perl ndb_firewall_port.pl check|block|unblock <port> <vardir>
#        perl ndb_firewall_port.pl status|cleanup [<vardir>]
#
# check writes <vardir>/tmp/ndb_firewall_skip.inc, which sets
# $ndb_firewall_skip to why the firewall cannot be changed on this host,
# empty if it can.
#
# The rule of a block belongs to the mysqltest process of the test: its
# pf anchor, nft table or iptables comment is named ndb_mtr_<pid>. A
# watchdog removes the rule when that process ends, so a test that fails
# before it unblocks, or is killed, does not leave the port blocked for
# the next tests and their clusters. block and unblock also remove rules
# whose mysqltest process has ended, left behind when their watchdog was
# killed as well. Rules of tests that run, also in other mtr runs on this
# host, are left alone.
#
# status and cleanup are run by hand before the test suites, see
# storage/ndb/claude_files/qmgr_join_protocol/testing.md. status tells
# whether the firewall tests can run and how to let them change the
# firewall without running mtr as root, and finds rules left behind and
# files of another user in <vardir>, which mtr cannot remove. It exits
# with 1 when something needs to be done. cleanup removes the rules left
# behind.
#
use strict;
use warnings;
use File::Find ();
use POSIX ();

my @args = @ARGV;
my $action = $args[0] // '';
my ($port, $vardir) = ('', '');
if ($action =~ /^(check|block|unblock)$/) {
  ($port, $vardir) = ($args[1] // '', $args[2] // '');
} elsif ($action =~ /^(status|cleanup)$/) {
  $vardir = $args[1] // '';
}
die "Usage: $0 check|block|unblock <port> <vardir>\n" .
    "       $0 status|cleanup [<vardir>]\n" .
    "got: '" . join("' '", @args) . "'\n"
  unless $action =~ /^(status|cleanup)$/ ||
         ($action =~ /^(check|block|unblock)$/ && $vardir ne '');

my $sudo = ($> == 0) ? "" : "sudo -n ";
my $state_file = "$vardir/tmp/ndb_firewall_state";
my $rule_file = "$vardir/tmp/ndb_firewall_pf_rules";
my $skip_file = "$vardir/tmp/ndb_firewall_skip.inc";

# Run a command, return whether it succeeded and its output
sub run {
  my ($cmd) = @_;
  my $out = `$cmd 2>&1`;
  return ($? == 0, $out);
}

# As run, but only the standard output. pfctl on macOS writes "No ALTQ
# support in kernel" to standard error on every call, which must not be
# taken for a listed rule or anchor.
sub run_stdout {
  my ($cmd) = @_;
  my $out = `$cmd 2>/dev/null`;
  return ($? == 0, $out);
}

# The firewall tool that can be used on this host, undef if none
sub find_tool {
  if ($^O eq 'darwin') {
    my ($ok) = run("${sudo}pfctl -s info");
    return $ok ? 'pf' : undef;
  }
  if ($^O eq 'linux') {
    my ($ok) = run("${sudo}nft list tables");
    return 'nft' if $ok;
    ($ok) = run("${sudo}iptables -S INPUT");
    return 'iptables' if $ok;
  }
  return undef;
}

my $tool = find_tool();

# Why the firewall of this host cannot be changed, '' if it can
sub skip_reason {
  return '' if defined $tool;
  return "Needs pfctl as root or with passwordless sudo" if $^O eq 'darwin';
  return "Needs nft or iptables as root or with passwordless sudo"
    if $^O eq 'linux';
  return "Needs macOS or Linux to block a port";
}

if ($action eq 'check') {
  my $skip = skip_reason();
  open(my $f, '>', $skip_file) or die "Cannot write $skip_file: $!\n";
  print $f "let \$ndb_firewall_skip= $skip;\n";
  close($f);
  exit(0);
}

# The mysqltest process that runs this script, through a shell or not
sub mysqltest_pid {
  my $pid = getppid();
  for (1 .. 4) {
    my $out = `ps -o ppid= -o comm= -p $pid 2>/dev/null`;
    my ($ppid, $comm) = $out =~ /^\s*(\d+)\s+(.*?)\s*$/;
    last unless defined $comm;
    return $pid if $comm =~ /mysqltest/;
    $pid = $ppid;
  }
  die "Cannot find the mysqltest process running $0\n";
}

# The names of the rules of this script that the firewall has
sub rule_names {
  my %names;
  if ($tool eq 'pf') {
    my ($ok, $out) = run_stdout("${sudo}pfctl -a com.apple -s Anchors");
    foreach my $name ($out =~ m{com\.apple/(ndb_mtr\S*)}g) {
      # macOS keeps listing an anchor whose rules were flushed
      my ($rok, $rules) =
        run_stdout("${sudo}pfctl -a com.apple/$name -s rules");
      $names{$name} = 1 if $rok && $rules =~ /^block\b/m;
    }
  } elsif ($tool eq 'nft') {
    my ($ok, $out) = run_stdout("${sudo}nft list tables");
    $names{$_} = 1 foreach $out =~ /table inet (ndb_mtr\S*)/g;
  } else {
    my ($ok, $out) = run_stdout("${sudo}iptables -S INPUT");
    $names{$_} = 1 foreach $out =~ /--comment "?(ndb_mtr[^"\s]*)/g;
  }
  return sort keys %names;
}

# Whether the mysqltest process that the rule $name belongs to runs
sub owner_runs {
  my ($name) = @_;
  my ($pid) = $name =~ /^ndb_mtr_(\d+)$/;
  return 0 unless defined $pid;
  my $comm = `ps -o comm= -p $pid 2>/dev/null`;
  return $comm =~ /mysqltest/ ? 1 : 0;
}

# The rules of this script whose mysqltest process has ended
sub stale_rules {
  return grep { !owner_runs($_) } rule_names();
}

# Remove the rules named @_
sub remove_rules {
  my %remove = map { $_ => 1 } grep { defined } @_;
  return unless %remove;
  if ($tool eq 'pf') {
    run("${sudo}pfctl -a com.apple/$_ -F rules") foreach sort keys %remove;
  } elsif ($tool eq 'nft') {
    run("${sudo}nft delete table inet $_") foreach sort keys %remove;
  } else {
    my ($ok, $out) = run("${sudo}iptables -S INPUT");
    foreach my $line (split(/\n/, $out)) {
      next unless $line =~ /^-A INPUT .*--comment "?(ndb_mtr\S*?)"? /;
      next unless $remove{$1};
      (my $del = $line) =~ s/^-A /-D /;
      run("${sudo}iptables $del");
    }
  }
}

# The sudoers line that lets this user change the firewall with sudo -n
sub sudoers_hint {
  my @tools = ($^O eq 'darwin') ? ('pfctl')
            : ($^O eq 'linux')  ? ('nft', 'iptables')
            : ();
  my @paths;
  foreach my $t (@tools) {
    foreach my $dir (qw(/sbin /usr/sbin /bin /usr/bin)) {
      if (-x "$dir/$t") {
        push @paths, "$dir/$t";
        last;
      }
    }
  }
  return '' unless @paths;
  my $user = getpwuid($<) // $ENV{USER} // 'user';
  my $line = "$user ALL=(root) NOPASSWD: " . join(', ', @paths);
  return "  To let the firewall tests run without running mtr as root, let\n" .
         "  $user run " . join(' and ', @paths) . " with sudo -n:\n" .
         "    echo '$line' | sudo tee /etc/sudoers.d/ndb-mtr-firewall\n" .
         "    sudo chmod 0440 /etc/sudoers.d/ndb-mtr-firewall\n" .
         "    sudo visudo -c\n";
}

# The number of files in $dir that belong to another user
sub foreign_files {
  my ($dir) = @_;
  my $n = 0;
  local $SIG{__WARN__} = sub { };
  File::Find::find({ no_chdir => 1, wanted => sub {
                       my @st = lstat($_);
                       $n++ if @st && $st[4] != $<;
                     } }, $dir);
  return $n;
}

if ($action eq 'status') {
  my $problems = 0;
  if (defined $tool) {
    my $as = ($> == 0) ? "as root" : "with sudo -n";
    print "ok      Firewall: $tool, changed $as, the firewall tests run\n";
    if ($tool eq 'pf') {
      my ($ok, $out) = run_stdout("${sudo}pfctl -s rules");
      if ($ok && $out =~ m{^anchor "com\.apple/\*"}m) {
        print "ok      pf evaluates the anchors com.apple/*\n";
      } else {
        print "PROBLEM pf does not evaluate the anchors com.apple/*, so a\n" .
              "        block has no effect. /etc/pf.conf needs the line\n" .
              "        anchor \"com.apple/*\", as in the macOS default.\n";
        $problems++;
      }
    }
    my @names = rule_names();
    print "ok      No rules of the firewall tests\n" unless @names;
    foreach my $name (@names) {
      if (owner_runs($name)) {
        print "ok      Rule $name belongs to a test that runs\n";
      } else {
        print "PROBLEM Rule $name was left behind, it blocks a port for\n" .
              "        the tests that use it. Remove it with:\n" .
              "          perl $0 cleanup\n";
        $problems++;
      }
    }
  } else {
    print "PROBLEM Firewall: " . skip_reason() .
          ", the firewall tests are skipped\n" . sudoers_hint();
    $problems++;
  }
  if ($vardir eq '') {
    print "-       No vardir given, its files are not checked\n";
  } elsif ($> == 0 || !-d $vardir) {
    print "ok      $vardir: nothing to check\n";
  } else {
    my $n = foreign_files($vardir);
    if ($n == 0) {
      print "ok      $vardir: all files are yours\n";
    } else {
      print "PROBLEM $vardir: $n files belong to another user, likely\n" .
            "        from an mtr run with sudo. mtr cannot remove them,\n" .
            "        remove the directory first:\n" .
            "          sudo rm -rf $vardir\n";
      $problems++;
    }
  }
  print $problems ? "Not ready: $problems problem(s)\n" : "Ready\n";
  exit($problems ? 1 : 0);
}

die "No firewall tool to $action with: " . skip_reason() . "\n"
  unless defined $tool;

if ($action eq 'cleanup') {
  my @stale = stale_rules();
  remove_rules(@stale);
  print @stale ? "Removed " . join(', ', @stale) . "\n"
               : "No rules were left behind\n";
  exit(0);
}

die "Bad port '$port'\n" unless $port =~ /^\d+$/;

# Release a reference that pfctl -E returned
sub release_pf {
  my ($token) = @_;
  run("${sudo}pfctl -X $token") if defined $token && $token =~ /^\d+$/;
}

sub read_state {
  open(my $f, '<', $state_file) or return (undef, undef);
  chomp(my $name = <$f> // '');
  chomp(my $token = <$f> // '');
  close($f);
  return ($name eq '' ? undef : $name, $token eq '' ? undef : $token);
}

if ($action eq 'unblock') {
  my ($name, $token) = read_state();
  $name //= eval { "ndb_mtr_" . mysqltest_pid() };
  remove_rules($name, stale_rules());
  release_pf($token) if $tool eq 'pf';
  unlink($state_file);
  exit(0);
}

# block
my $owner = mysqltest_pid();
my $name = "ndb_mtr_$owner";
{
  my ($old_name, $old_token) = read_state();
  release_pf($old_token) if $tool eq 'pf';
  unlink($state_file);
}
remove_rules($name, stale_rules());

my $token;
if ($tool eq 'pf') {
  # pf filters only while enabled, -E takes a reference that unblock
  # releases, pf stays enabled while others hold a reference
  my ($ok, $out) = run("${sudo}pfctl -E");
  die "pfctl -E failed: $out\n" unless $ok;
  ($token) = $out =~ /Token : (\d+)/;
  open(my $f, '>', $rule_file) or die "Cannot write $rule_file: $!\n";
  print $f "block return quick proto tcp from any to any port $port"
         . " flags S/SA\n";
  close($f);
  ($ok, $out) = run("${sudo}pfctl -a com.apple/$name -f $rule_file");
  unlink($rule_file);
  die "pfctl -a com.apple/$name -f failed: $out\n" unless $ok;
  ($ok, $out) = run("${sudo}pfctl -a com.apple/$name -s rules");
  die "No rule for port $port in pf anchor com.apple/$name: $out\n"
    unless $ok && $out =~ /port = $port\b/;
} elsif ($tool eq 'nft') {
  foreach my $cmd (
      "nft add table inet $name",
      "nft 'add chain inet $name input"
        . " { type filter hook input priority 0 ; }'",
      "nft 'add rule inet $name input tcp dport $port"
        . " tcp flags & (syn | ack) == syn reject with tcp reset'") {
    my ($ok, $out) = run("${sudo}$cmd");
    die "$cmd failed: $out\n" unless $ok;
  }
} else {
  my $cmd = "iptables -I INPUT -p tcp --dport $port --syn"
            . " -m comment --comment $name"
            . " -j REJECT --reject-with tcp-reset";
  my ($ok, $out) = run("${sudo}$cmd");
  die "$cmd failed: $out\n" unless $ok;
}

open(my $f, '>', $state_file) or die "Cannot write $state_file: $!\n";
print $f "$name\n" . ($token // '') . "\n";
close($f);

# Watchdog: remove the rule when the mysqltest process ends. It is
# detached and holds none of the output of this script, which mysqltest
# reads until the end.
my $pid = fork();
die "fork failed: $!\n" unless defined $pid;
if ($pid == 0) {
  POSIX::setsid();
  open(STDIN, '<', '/dev/null');
  open(STDOUT, '>', '/dev/null');
  open(STDERR, '>', '/dev/null');
  sleep(1) while kill(0, $owner);
  remove_rules($name);
  release_pf($token) if $tool eq 'pf';
  POSIX::_exit(0);
}
exit(0);
