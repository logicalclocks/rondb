#!/usr/bin/env perl
# ronsql_mirrors.pl -- write and check the RonSQL mirror suites.
#
# A mirror suite runs the tests of a base suite in another configuration:
# with the compiled interpreter on (ronsql_jit, ronsql_cte_jit,
# ronsql_fs_jit) or on another topology (ronsql_cte_ng*, ronsql_fs_ng*,
# ronsql_ng2r2).  Its t/*.test and r/*.result files are generated from the
# base suite's files by this script; ronsql_mirrors.cfg (next to it) says,
# for every mirror suite, which base tests are mirrored, which are not and
# why, and the per-test differences (a recorded JIT fallback delta, a test
# that is not strict-armed, a header comment of its own).  Never edit a
# mirror file by hand: edit the base test or the config, then run
#
#   perl mysql-test/suite/ronsql/mirrors/ronsql_mirrors.pl --write
#
# --write   (re)writes every mirror .test and .result that differs from
#           what the config and the base suite give, and creates the
#           missing ones.  It deletes nothing.
# --check   writes nothing; lists every mirror file that is missing or
#           differs, every base test that is neither mirrored nor
#           excluded, and every file in a mirror suite that the config
#           does not account for.  Exit status 1 if there is any.  The
#           test ronsql.ronsql_mirror_check runs this.
#
# Both modes print the problems they cannot fix and exit 1 if there are
# any (e.g. a mirror file whose base test is gone: remove it with git rm).
#
# Options: --config FILE (default: ronsql_mirrors.cfg next to this script),
#          --suites DIR  (default: the mysql-test/suite directory above it).
#
# Files are compared and written as bytes: some results hold bytes that
# are not UTF-8.

use strict;
use warnings;
use File::Basename qw(dirname basename);
use Cwd qw(abs_path);

my $mode;
my $script_dir = dirname(abs_path($0));
my $cfg_file = "$script_dir/ronsql_mirrors.cfg";
my $suite_dir = abs_path("$script_dir/../..");

while (@ARGV) {
  my $arg = shift @ARGV;
  if ($arg eq '--check' || $arg eq '--write') {
    die "Give only one of --check and --write\n" if defined $mode;
    $mode = substr($arg, 2);
  } elsif ($arg eq '--config') {
    $cfg_file = shift @ARGV;
  } elsif ($arg eq '--suites') {
    $suite_dir = shift @ARGV;
  } else {
    die "Usage: $0 --check | --write [--config FILE] [--suites DIR]\n";
  }
}
die "Usage: $0 --check | --write [--config FILE] [--suites DIR]\n"
  unless defined $mode;
die "No suite directory $suite_dir\n" unless -d $suite_dir;

# ---------------------------------------------------------------------------
# The configuration (see the head of ronsql_mirrors.cfg for the syntax).

my %families;      # mirror suite -> family hash
my @family_order;  # mirror suites in the order of first appearance

sub family {
  my ($name) = @_;
  if (!$families{$name}) {
    $families{$name} = {
      name => $name, templates => {}, exclude => {}, own => {},
      mirror => {}, subst => {},
    };
    push @family_order, $name;
  }
  return $families{$name};
}

sub read_config {
  my ($file) = @_;
  open(my $fh, '<:raw', $file) or die "Cannot read $file: $!\n";
  my @section;
  my $text;    # array ref the following "|" lines go to
  my $pairs;   # array ref the following "-" / "+" lines go to
  my $lineno = 0;
  my $cfg_die = sub { die "$file:$lineno: $_[0]\n"; };
  while (my $line = <$fh>) {
    $lineno++;
    $line =~ s/\r?\n\z//;
    if ($line =~ /^\|(?: (.*))?\z/s) {
      $cfg_die->('a "|" line belongs after template or mirror') unless $text;
      push @$text, defined $1 ? $1 : '';
      next;
    }
    if ($pairs && $line =~ /^([-+])(?: (.*))?\z/s) {
      my ($sign, $body) = ($1, defined $2 ? $2 : '');
      if ($sign eq '-') {
        push @$pairs, [$body, undef];
      } else {
        $cfg_die->('a "+" line must follow a "-" line')
          unless @$pairs && !defined $pairs->[-1][1];
        $pairs->[-1][1] = $body;
      }
      next;
    }
    if ($pairs && @$pairs && !defined $pairs->[-1][1]) {
      $cfg_die->('the "-" line above has no "+" line');
    }
    undef $text;
    undef $pairs;
    next if $line =~ /^\s*(#|\z)/;
    if ($line =~ /^\[\s*(.*?)\s*\]\s*\z/) {
      @section = split ' ', $1;
      $cfg_die->('empty section') unless @section;
      family($_) for @section;
      next;
    }
    $cfg_die->('a directive outside a [suite] section') unless @section;
    my ($kw, $rest) = $line =~ /^\s*(\S+)\s*(.*?)\s*\z/;
    my @fams = map { $families{$_} } @section;
    if ($kw eq 'base' || $kw eq 'kind' || $kw eq 'arm' ||
        $kw eq 'data_nodes') {
      $cfg_die->("$kw needs a value") if $rest eq '';
      $cfg_die->("unknown kind $rest")
        if $kw eq 'kind' && $rest !~ /^(jit|copy|pin)\z/;
      $cfg_die->('data_nodes must be a number')
        if $kw eq 'data_nodes' && $rest !~ /^\d+\z/;
      for my $f (@fams) {
        $cfg_die->("$kw set twice for $f->{name}") if defined $f->{$kw};
        $f->{$kw} = $rest;
      }
    } elsif ($kw eq 'subset') {
      $cfg_die->('subset takes no value') if $rest ne '';
      $_->{subset} = 1 for @fams;
    } elsif ($kw eq 'template') {
      $cfg_die->('template needs a name') unless $rest =~ /^\S+\z/;
      $text = [];
      for my $f (@fams) {
        $cfg_die->("template $rest defined twice for $f->{name}")
          if $f->{templates}{$rest};
        $f->{templates}{$rest} = $text;
      }
    } elsif ($kw eq 'exclude' || $kw eq 'own') {
      my ($test, $reason) = $rest =~ /^(\S+)\s*(.*)\z/;
      $cfg_die->("$kw needs a test name and a reason")
        unless defined $reason && $reason ne '';
      for my $f (@fams) {
        $cfg_die->("$test is listed twice for $f->{name}")
          if $f->{exclude}{$test} || $f->{own}{$test} || $f->{mirror}{$test};
        $f->{$kw}{$test} = { reason => $reason, line => $lineno };
      }
    } elsif ($kw eq 'mirror') {
      my ($test, @opts) = split ' ', $rest;
      $cfg_die->('mirror needs a test name') unless defined $test;
      my %m = (line => $lineno);
      for my $o (@opts) {
        if ($o eq 'unarmed') { $m{unarmed} = 1; }
        elsif ($o =~ /^var=(\w+)\z/) { $m{var} = $1; }
        elsif ($o =~ /^delta=(-?\d+)\z/) { $m{delta} = $1; }
        elsif ($o =~ /^header=(\S+)\z/) { $m{template} = $1; }
        else { $cfg_die->("unknown mirror option $o"); }
      }
      $text = $m{header} = [];
      for my $f (@fams) {
        $cfg_die->("$test is listed twice for $f->{name}")
          if $f->{exclude}{$test} || $f->{own}{$test} || $f->{mirror}{$test};
        $f->{mirror}{$test} = \%m;
      }
    } elsif ($kw eq 'subst') {
      $cfg_die->('subst needs a test name') unless $rest =~ /^\S+\z/;
      $pairs = [];
      for my $f (@fams) {
        $cfg_die->("subst for $rest given twice for $f->{name}")
          if $f->{subst}{$rest};
        $f->{subst}{$rest} = $pairs;
      }
    } else {
      $cfg_die->("unknown directive $kw");
    }
  }
  if ($pairs && @$pairs && !defined $pairs->[-1][1]) {
    $cfg_die->('the "-" line above has no "+" line');
  }
  close($fh);
  for my $name (@family_order) {
    my $f = $families{$name};
    for my $kw ('base', 'kind') {
      die "$file: [$name] has no $kw\n" unless defined $f->{$kw};
    }
    die "$file: [$name] (kind jit) has no arm\n"
      if $f->{kind} eq 'jit' && !defined $f->{arm};
    die "$file: [$name] (kind pin) has no data_nodes\n"
      if $f->{kind} eq 'pin' && !defined $f->{data_nodes};
    die "$file: [$name] (kind $f->{kind}) needs a template default\n"
      if $f->{kind} ne 'copy' && !$f->{templates}{default};
    die "$file: [$name] is a subset family: list its tests with mirror, " .
        "not exclude\n" if $f->{subset} && %{$f->{exclude}};
    for my $t (sort keys %{$f->{mirror}}) {
      my $m = $f->{mirror}{$t};
      die "$file:$m->{line}: [$name] has no template $m->{template}\n"
        if defined $m->{template} && !$f->{templates}{$m->{template}};
      die "$file:$m->{line}: header=$m->{template} and a header of its own\n"
        if defined $m->{template} && @{$m->{header}};
      die "$file:$m->{line}: unarmed, var= and delta= are for kind jit\n"
        if $f->{kind} ne 'jit' &&
           ($m->{unarmed} || defined $m->{var} || defined $m->{delta});
      die "$file:$m->{line}: a copy mirror has no header\n"
        if $f->{kind} eq 'copy' && (defined $m->{template} || @{$m->{header}});
    }
  }
}

# ---------------------------------------------------------------------------
# Generation.

sub slurp {
  my ($path) = @_;
  open(my $fh, '<:raw', $path) or return undef;
  local $/;
  my $data = <$fh>;
  close($fh);
  return defined $data ? $data : '';
}

sub tests_in {
  my ($dir, $ext) = @_;
  opendir(my $dh, $dir) or return ();
  my @names = sort map { substr($_, 0, -length($ext)) }
              grep { /\Q$ext\E\z/ && -f "$dir/$_" } readdir($dh);
  closedir($dh);
  return @names;
}

sub header_text {
  my ($f, $test) = @_;
  my $m = $f->{mirror}{$test} || {};
  my $lines;
  if ($m->{header} && @{$m->{header}}) {
    $lines = $m->{header};
  } else {
    $lines = $f->{templates}{defined $m->{template} ? $m->{template}
                                                    : 'default'};
  }
  my $text = join('', map { "$_\n" } @$lines);
  $text =~ s/\@NAME\@/$test/g;
  $text =~ s/\@BASE\@/$f->{base}/g;
  $text =~ s/\@SUITE\@/$f->{name}/g;
  return $text;
}

# Returns (wrapper, result) for one mirrored test.
sub generate {
  my ($f, $test, $base_test, $base_result) = @_;
  $base_result .= "\n" if $base_result ne '' && $base_result !~ /\n\z/;
  my $m = $f->{mirror}{$test} || {};
  my $source = "--source suite/$f->{base}/t/$test.test\n";
  my ($wrapper, $result);
  if ($f->{kind} eq 'copy') {
    ($wrapper, $result) = ($base_test, $base_result);
  } elsif ($f->{kind} eq 'pin') {
    my $pin = "SELECT COUNT(*) AS data_nodes FROM ndbinfo.nodes;\n";
    $wrapper = header_text($f, $test) . $pin . $source;
    $result = $pin . "data_nodes\n$f->{data_nodes}\n" . $base_result;
  } else {
    my $var = defined $m->{var} ? $m->{var} : 'jit_c0';
    my $delta = defined $m->{delta} ? $m->{delta} : 0;
    my $set = "SET \@$var = (SELECT SUM(programs_fallback) FROM ndbinfo.jit);\n";
    my $select = "SELECT CAST((SELECT SUM(programs_fallback) FROM ndbinfo.jit)" .
                 " - \@$var AS SIGNED)\n";
    my $alias = "${test}_jit_fallback_delta";
    $wrapper = header_text($f, $test) . $set;
    $wrapper .= "--source $f->{arm}/jit_strict_arm.inc\n" unless $m->{unarmed};
    $wrapper .= $source;
    $wrapper .= "--source $f->{arm}/jit_strict_disarm.inc\n"
      unless $m->{unarmed};
    $wrapper .= $select . "       AS $alias;\n";
    $result = $set . $base_result . $select . "AS $alias;\n$alias\n$delta\n";
  }
  return ($wrapper, $result);
}

# Applies the subst pairs of a test to its generated result.  Every "-"
# line must occur in it as a whole line: a pair that matches nothing is
# stale and reported.
sub apply_subst {
  my ($f, $test, $result, $problems) = @_;
  my $pairs = $f->{subst}{$test} or return $result;
  my @lines = split /\n/, $result, -1;
  for my $p (@$pairs) {
    my ($old, $new) = @$p;
    my $hits = 0;
    for my $l (@lines) {
      if ($l eq $old) { $l = $new; $hits++; }
    }
    push @$problems, "$f->{name}: subst for $test: no line \"$old\" in " .
                     "the generated result" unless $hits;
  }
  return join("\n", @lines);
}

sub write_file {
  my ($path, $data) = @_;
  open(my $fh, '>:raw', $path) or die "Cannot write $path: $!\n";
  print $fh $data;
  close($fh) or die "Cannot write $path: $!\n";
}

# ---------------------------------------------------------------------------

read_config($cfg_file);

my @problems;
my @written;
my $files = 0;

for my $name (@family_order) {
  my $f = $families{$name};
  my $base_dir = "$suite_dir/$f->{base}";
  my $mir_dir = "$suite_dir/$name";
  if (!-d "$base_dir/t") {
    push @problems, "$name: base suite $f->{base} has no t/ directory";
    next;
  }
  if (!-d "$mir_dir/t" || !-d "$mir_dir/r") {
    push @problems, "$name: suite directory $name/t or $name/r is missing " .
                    "(create the suite and its my.cnf first)";
    next;
  }
  my %base = map { $_ => 1 } tests_in("$base_dir/t", '.test');
  for my $kw ('exclude', 'mirror') {
    for my $t (sort keys %{$f->{$kw}}) {
      push @problems, "$name: $kw $t (config line $f->{$kw}{$t}{line}): " .
                      "no such test in suite $f->{base}" unless $base{$t};
    }
  }
  for my $t (sort keys %{$f->{subst}}) {
    push @problems, "$name: subst for $t, which is not mirrored"
      unless $base{$t} && !$f->{exclude}{$t} && !$f->{own}{$t} &&
             (!$f->{subset} || $f->{mirror}{$t});
  }
  my @tests = $f->{subset}
            ? grep { $base{$_} } sort keys %{$f->{mirror}}
            : grep { !$f->{exclude}{$_} && !$f->{own}{$_} } sort keys %base;
  my %generated;
  for my $t (@tests) {
    for my $own_file ("$t.cnf", "$t-master.opt", "$t-slave.opt") {
      push @problems, "$name: base test $t has its own $own_file; " .
                      "exclude it (a mirror would run without it)"
        if -e "$base_dir/t/$own_file";
    }
    my $base_test = slurp("$base_dir/t/$t.test");
    my $base_result = slurp("$base_dir/r/$t.result");
    if (!defined $base_result) {
      push @problems, "$name: base test $t has no r/$t.result";
      next;
    }
    my ($wrapper, $result) = generate($f, $t, $base_test, $base_result);
    $result = apply_subst($f, $t, $result, \@problems);
    $generated{$t} = 1;
    my @stale;
    for my $out (["t/$t.test", $wrapper], ["r/$t.result", $result]) {
      my ($rel, $want) = @$out;
      $files++;
      my $have = slurp("$mir_dir/$rel");
      next if defined $have && $have eq $want;
      if ($mode eq 'write') {
        write_file("$mir_dir/$rel", $want);
        push @written, "$name/$rel" . (defined $have ? '' : ' (new)');
      } else {
        push @stale, [$rel, defined $have];
      }
    }
    if (@stale == 2 && !$stale[0][1] && !$stale[1][1]) {
      push @problems, "$name: base test $t has no mirror (run --write, or " .
                      ($f->{subset} ? "drop its mirror line)"
                                    : "add an exclude line)");
    } else {
      push @problems, "$name/$_->[0]: " .
        ($_->[1] ? 'differs from the generated file' : 'missing') for @stale;
    }
  }
  for my $t (sort keys %{$f->{own}}) {
    push @problems, "$name: own test $t (config line $f->{own}{$t}{line}) " .
                    "has no t/$t.test" unless -f "$mir_dir/t/$t.test";
  }
  for my $pair (['t', '.test'], ['r', '.result']) {
    my ($sub, $ext) = @$pair;
    for my $t (tests_in("$mir_dir/$sub", $ext)) {
      next if $generated{$t} || $f->{own}{$t};
      my $why = !$base{$t} ? "suite $f->{base} has no test $t (git rm it, " .
                             "or add an own line if it is a test of $name)"
              : $f->{exclude}{$t} ? "$t is excluded (git rm it, or drop " .
                                    "the exclude line)"
              : "$t is not in the subset (git rm it, or add a mirror line)";
      push @problems, "$name/$sub/$t$ext: not generated: $why";
    }
  }
}

if (@written) {
  print "Wrote $_\n" for @written;
}
if (@problems) {
  print "ronsql_mirrors.pl --$mode: ", scalar(@problems), " problem(s):\n";
  print "  $_\n" for @problems;
  print "Fix: edit suite/ronsql/mirrors/ronsql_mirrors.cfg or the base " .
        "test, then run\n" .
        "  perl suite/ronsql/mirrors/ronsql_mirrors.pl --write\n" .
        "from mysql-test/ (see the head of ronsql_mirrors.cfg).\n";
  exit 1;
}
if ($mode eq 'check') {
  print "All mirror suites match their base suites.\n";
} elsif (!@written) {
  print "All $files mirror files were up to date.\n";
}
exit 0;
