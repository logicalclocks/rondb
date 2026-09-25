# mem_leak_exec.inc -- run registered shape(s) through RDRS (HTTP
# /0.1.0/ronsql) or ronsql_cli, optionally with concurrent clients, and
# check every result against the shape's reference output.
#
# Inputs (environment variables, set with --let NAME = value):
#   MLKE_SHAPES   comma list of shape ids; files $MYSQL_TMP_DIR/mlk_q<id>.sql
#                 (query) and .kind (rdrs | cli) must exist
#   MLKE_COUNT    executions per client
#   MLKE_CLIENTS  concurrent clients (forked); client c runs shape
#                 ids[(c + i) % n] in its i-th execution
#   MLKE_ACCEPT   ok        every execution must return the reference
#                 ok_or_503 like ok, but HTTP 503 (retry budget exhausted,
#                           e.g. out of query memory under concurrency)
#                           is tolerated
#                 fail      every execution must fail (error inserts)
#                 any       success or error both fine, no hang; a
#                           successful result must still match
#   MLKE_LABEL    text printed before the outcome
#   MLKE_QUIET    1: print nothing (the caller prints its own line)
#   MLKE_TRACE    1: after every execution, sample the QUERY_MEMORY total
#                 over all data nodes into the report file
#   MLKE_CLEAR_ON_FAIL  1: "ALL ERROR 0" before failing (error-insert tests)
#
# The reference output (header + sorted rows) of a shape is written by
# its first successful execution to mlk_q<id>.ref (only with one client).
# A differing result is kept as mlk_q<id>.mismatch_<client>_<n>.  Counts
# (successes, 503s, other errors, NDB attempts from the x-ronsql-phases
# header, seconds) go to the report file.
--perl
use strict;
use warnings;
use POSIX qw(_exit);
use Text::ParseWords qw(shellwords);
use Time::HiRes qw(time);

$| = 1;
my $tmp     = $ENV{MYSQL_TMP_DIR} // die "mem_leak_exec: MYSQL_TMP_DIR is not set\n";
my @ids     = split /,/, ($ENV{MLKE_SHAPES} // '');
my $count   = $ENV{MLKE_COUNT} // '';
my $clients = $ENV{MLKE_CLIENTS} // '';
my $accept  = $ENV{MLKE_ACCEPT} // '';
my $label   = $ENV{MLKE_LABEL} // 'run';
my $quiet   = $ENV{MLKE_QUIET} || 0;
my $trace   = $ENV{MLKE_TRACE} || 0;
my $report  = "$ENV{MYSQLTEST_VARDIR}/log/ronsql_large_mem_leak.report";
die "mem_leak_exec: bad parameters: shapes='@ids' count='$count' "
  . "clients='$clients' accept='$accept'\n"
  unless @ids && !grep({ !/^\w+$/ } @ids)
    && $count =~ /^\d+$/ && $clients =~ /^[1-9]\d*$/
    && $accept =~ /^(?:ok|ok_or_503|fail|any)$/;

sub slurp {
  my ($f) = @_;
  open(my $fh, '<', $f) or die "mem_leak_exec: open $f: $!\n";
  local $/;
  my $s = <$fh>;
  close($fh);
  return defined $s ? $s : '';
}

sub spew {
  my ($f, $s) = @_;
  open(my $fh, '>', $f) or die "mem_leak_exec: open $f: $!\n";
  print $fh $s;
  close($fh) or die "mem_leak_exec: close $f: $!\n";
}

sub note {
  my ($line) = @_;
  return unless open(my $fh, '>>', $report);
  my @t = localtime();
  printf $fh "%02d:%02d:%02d %s %s\n", $t[2], $t[1], $t[0],
    ($ENV{MLKE_TEST} // '?'), $line;
  close($fh);
}

# Run @cmd with stdout / stderr redirected to files; return the wait status.
sub run_cmd {
  my ($out, $err, @cmd) = @_;
  my $pid = fork();
  die "mem_leak_exec: fork: $!\n" unless defined $pid;
  if (!$pid) {
    open(STDIN, '<', '/dev/null') or _exit(126);
    open(STDOUT, '>', $out) or _exit(126);
    open(STDERR, '>', $err) or _exit(126);
    exec { $cmd[0] } @cmd or _exit(127);
  }
  waitpid($pid, 0);
  return $?;
}

sub clear_inserts {
  return unless $ENV{MLKE_CLEAR_ON_FAIL};
  my @mgm = (shellwords($ENV{NDB_MGM} // ''),
             '--connect-string', ($ENV{NDB_CONNECTSTRING} // ''));
  run_cmd('/dev/null', '/dev/null', @mgm, '-e', 'ALL ERROR 0');
}

# Header line first, data rows sorted: RonSQL does not promise row order.
# ronsql_cli shares stdout with the NDB API logger; drop its lines.
sub canon {
  my ($text) = @_;
  $text =~ s/^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d \[NdbApi\][^\n]*\n//mg;
  my @lines = split /\n/, $text;
  return '' unless @lines;
  my $head = shift @lines;
  return join("\n", $head, sort @lines) . "\n";
}

my @mysql = shellwords($ENV{MYSQL} // '');
sub qm_total {
  return '?' unless @mysql;
  my $pid = open(my $ph, '-|');
  return '?' unless defined $pid;
  if (!$pid) {
    open(STDERR, '>', '/dev/null');
    exec { $mysql[0] } @mysql, '-N', '-B', '-e',
      "SELECT SUM(used) FROM ndbinfo.resources WHERE resource_name = 'QUERY_MEMORY'"
      or _exit(127);
  }
  my $v = <$ph>;
  close($ph);
  $v = '?' unless defined $v;
  chomp($v);
  return $v;
}

my $url = 'http://' . ($ENV{RDRS_NOKEY_HOST} // '') . ':'
        . ($ENV{RDRS_NOKEY_PORT} // '') . '/0.1.0/ronsql';
my %shape;
for my $id (@ids) {
  next if $shape{$id};
  my $base = "$tmp/mlk_q$id";
  die "mem_leak_exec: $base.sql is missing\n" unless -e "$base.sql";
  my $kind = -e "$base.kind" ? slurp("$base.kind") : 'rdrs';
  $kind =~ s/\s+//g;
  die "mem_leak_exec: unknown client kind '$kind' for shape $id\n"
    unless $kind eq 'rdrs' || $kind eq 'cli';
  if ($kind eq 'rdrs' && !-e "$base.body") {
    # The same JSON escaping as suite/ronsql/include/ronsql_compare.inc.
    my $q = slurp("$base.sql");
    $q =~ s/\\/\\\\/g;
    $q =~ s/"/\\"/g;
    $q =~ s/\x08/\\b/g;
    $q =~ s/\f/\\f/g;
    $q =~ s/\n/\\n/g;
    $q =~ s/\r/\\r/g;
    $q =~ s/\t/\\t/g;
    spew("$base.body",
         '{"query":"' . $q . '","database":"test","outputFormat":"TEXT"}');
  }
  $shape{$id} = { id => $id, base => $base, kind => $kind };
}
die "mem_leak_exec: ronsql_cli has a single API node slot in this suite "
  . "and cannot run with concurrent clients\n"
  if $clients > 1 && grep { $shape{$_}{kind} eq 'cli' } @ids;

sub run_one {
  my ($s, $tag) = @_;
  my $p = "$tmp/mlk_run_$tag";
  my %r = (attempts => 1);
  if ($s->{kind} eq 'rdrs') {
    my $st = run_cmd("$p.code", "$p.err", 'curl', '-s', '-S',
                     '--max-time', '300', '-o', "$p.out", '-D', "$p.hdr",
                     '-w', '%{http_code}', '-X', 'POST',
                     '-H', 'Content-Type: application/json',
                     '--data-binary', "\@$s->{base}.body", $url);
    my $code = -e "$p.code" ? slurp("$p.code") : '';
    $code =~ s/\s+//g;
    $r{code} = $st == 0 ? $code : 'curl-exit-' . ($st >> 8);
    $r{status} = ($st == 0 && $code =~ /^\d+$/) ? 1 : 0;
    my $hdr = -e "$p.hdr" ? slurp("$p.hdr") : '';
    $r{attempts} = $1 if $hdr =~ /^x-ronsql-phases:[^\n]*\battempts=(\d+)/mi;
  } else {
    my $st = run_cmd("$p.out", "$p.err", $ENV{RONSQL_CLI_EXE},
                     '--connect-string', $ENV{NDB_CONNECTSTRING},
                     '-D', 'test', '--execute-file', "$s->{base}.sql");
    $r{code} = $st == 0 ? '200' : 'exit-' . ($st >> 8);
    $r{status} = ($st & 127) == 0 ? 1 : 0;
  }
  $r{body} = -e "$p.out" ? slurp("$p.out") : '';
  $r{err}  = -e "$p.err" ? slurp("$p.err") : '';
  unlink("$p.code", "$p.out", "$p.hdr", "$p.err");
  return \%r;
}

sub client_loop {
  my ($ci, $n) = @_;
  my %st = (ok => 0, e503 => 0, eother => 0, attempts => 0, trace => []);
  for my $i (1 .. $n) {
    my $s = $shape{$ids[($ci + $i) % scalar(@ids)]};
    my $r = run_one($s, "${ci}_$i");
    $st{attempts} += $r->{attempts};
    my $where = "$label, shape $s->{id}, client $ci, execution $i";
    if ($r->{code} eq '200') {
      die "$where: the request succeeded, but the error insert should "
        . "have made it fail\n" if $accept eq 'fail';
      my $got = canon($r->{body});
      my $ref = "$s->{base}.ref";
      if (!-e $ref) {
        die "$where: no reference output yet\n" if $clients > 1;
        spew($ref, $got);
      } elsif ($got ne slurp($ref)) {
        my $keep = "$s->{base}.mismatch_${ci}_$i";
        spew($keep, $got);
        die "$where: the result differs from the reference $ref "
          . "(kept in $keep)\n";
      }
      $st{ok}++;
    } else {
      my $allowed = $r->{status}
        && ($accept eq 'any' || $accept eq 'fail'
            || ($accept eq 'ok_or_503' && $r->{code} eq '503'));
      if (!$allowed) {
        my $detail = substr($r->{body} . ' ' . $r->{err}, 0, 2000);
        $detail =~ s/\s+/ /g;
        die "$where: unexpected outcome '$r->{code}': $detail\n";
      }
      if ($r->{code} eq '503') { $st{e503}++; } else { $st{eother}++; }
    }
    push @{$st{trace}}, qm_total() if $trace;
  }
  return \%st;
}

my $t0 = time();
my @done;
if ($clients == 1) {
  my $st = eval { client_loop(0, $count) };
  if (!$st) {
    my $e = $@;
    clear_inserts();
    note("$label FAILED: $e");
    die $e;
  }
  push @done, $st;
} else {
  my %kid;
  for my $ci (0 .. $clients - 1) {
    my $pid = fork();
    die "mem_leak_exec: fork: $!\n" unless defined $pid;
    if (!$pid) {
      my $st = eval { client_loop($ci, $count) };
      my $e = $@;
      if (open(my $fh, '>', "$tmp/mlk_client_$ci.status")) {
        if ($st) {
          print $fh "ok=$st->{ok} e503=$st->{e503} eother=$st->{eother} "
                  . "attempts=$st->{attempts}\n";
        } else {
          $e =~ s/\n/ /g;
          print $fh "error=$e\n";
        }
        close($fh);
      }
      _exit($st ? 0 : 1);
    }
    $kid{$pid} = $ci;
  }
  my @errors;
  for my $pid (sort { $a <=> $b } keys %kid) {
    waitpid($pid, 0);
    my $rc = $?;
    my $ci = $kid{$pid};
    my $f = "$tmp/mlk_client_$ci.status";
    my $line = -e $f ? slurp($f) : '';
    unlink($f);
    if ($line =~ /^ok=(\d+) e503=(\d+) eother=(\d+) attempts=(\d+)/) {
      push @done, { ok => $1, e503 => $2, eother => $3, attempts => $4,
                    trace => [] };
    } else {
      $line =~ s/^error=//;
      $line =~ s/\s+$//;
      push @errors, "client $ci: "
        . ($line ne '' ? $line : "exit status $rc, no status file");
    }
  }
  if (@errors) {
    clear_inserts();
    note("$label FAILED: " . join(' | ', @errors));
    die join('', map { "mem_leak_exec: $_\n" } @errors);
  }
}

my %sum = (ok => 0, e503 => 0, eother => 0, attempts => 0);
my @samples;
for my $st (@done) {
  $sum{$_} += $st->{$_} for qw(ok e503 eother attempts);
  push @samples, @{$st->{trace}};
}
my $total = $count * $clients;
my %kinds = map { $shape{$_}{kind} => 1 } @ids;
my $via = join('/', map { $_ eq 'cli' ? 'ronsql_cli' : 'RDRS' } sort keys %kinds);
note(sprintf("%s: shapes=%s executions=%d clients=%d ok=%d http503=%d "
             . "other_errors=%d ndb_attempts=%d secs=%.1f%s",
             $label, join(',', @ids), $total, $clients, $sum{ok},
             $sum{e503}, $sum{eother}, $sum{attempts}, time() - $t0,
             @samples ? ' qm_pages_total_after_each=' . join(' ', @samples)
                      : ''));
if (!$quiet) {
  my %what = (
    ok        => 'every result identical to the reference',
    ok_or_503 => 'every 200 response identical to the reference',
    fail      => 'every request failed, as the error insert intends',
    any       => 'every request answered',
  );
  my $conc = $clients > 1 ? ", $clients concurrent clients" : '';
  print "$label: $total execution(s) via $via$conc, $what{$accept}\n";
}
EOF
