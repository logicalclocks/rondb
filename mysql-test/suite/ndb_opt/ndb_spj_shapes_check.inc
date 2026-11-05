# ndb_spj_shapes_check.inc -- differential check of one query shape.
#
# Runs $spj_query with the pushdown under test OFF and then ON, compares
# the two results inside the test, and checks with EXPLAIN that the ON run
# really was pushed.  Source ndb_spj_shapes_init.inc once before the first
# check and ndb_spj_shapes_end.inc after the last one.
#
# Input:
#   $spj_label       shape name without spaces, echoed with the verdict
#   $spj_query       the SELECT.  It must not contain double quotes: it is
#                    passed through query_get_value("EXPLAIN ...").
#   $spj_ordered     1: compare the rows in order (the query has an ORDER
#                    BY that makes the row order unique); 0: compare the
#                    results as multisets
#   $spj_agg         0: toggle ndb_join_pushdown
#                    1: reference run with ndb_join_pushdown and
#                       ndb_join_pushdown_aggregate OFF, checked run with
#                       ndb_join_pushdown, ndb_join_pushdown_aggregate and
#                       ndb_join_pushdown_aggregate_outer_join ON
#                    2: keep ndb_join_pushdown ON and toggle the
#                       single-table ndb_pushdown_aggregate (no pushed
#                       join is required, $spj_min_pushed is ignored)
#   $spj_min_pushed  least number of tables EXPLAIN (ON) must show as
#                    members of pushed joins; 0 only prints the number
#   $spj_expect_agg  with $spj_agg=1 (2): 1 means EXPLAIN (ON) must show
#                    'Using pushed join aggregation' ('Using pushed
#                    aggregation'); 0 only prints whether it does
# $spj_ordered, $spj_agg, $spj_min_pushed and $spj_expect_agg are reset to
# 0, 0, 2 and 1 after each check.
#
# Prints one line:
#   # <label>: rows=<r> pushed=<t> exec=<e>[ agg=<a>]
#     r  result rows without the pushdown
#     t  tables EXPLAIN shows as members of a pushed join (ON)
#     e  1 if Ndb_pushed_queries_executed grew during the ON run
#     a  (aggregation checks) 1 if EXPLAIN shows pushed join aggregation
# A failure (different result, SQL error, pushdown missing) adds
#   #   FAILED: <reason>
# and up to six differing rows, and appends the label to $spj_fail_list.

--disable_query_log
--let $spj_dir= $MYSQLTEST_VARDIR/tmp
--let $spj_off_file= $spj_dir/spj_shapes_off.txt
--let $spj_on_file= $spj_dir/spj_shapes_on.txt

--let $spj_set_off= SET SESSION ndb_join_pushdown = OFF, ndb_join_pushdown_aggregate = OFF, ndb_pushdown_aggregate = OFF
--let $spj_set_on= SET SESSION ndb_join_pushdown = ON, ndb_join_pushdown_aggregate = OFF, ndb_pushdown_aggregate = OFF
--let $spj_agg_text= Using pushed join aggregation
if ($spj_agg == 1)
{
  # The reference run uses no pushed join at all (pure MySQL execution).
  --let $spj_set_off= SET SESSION ndb_join_pushdown = OFF, ndb_join_pushdown_aggregate = OFF, ndb_join_pushdown_aggregate_outer_join = OFF, ndb_pushdown_aggregate = OFF
  --let $spj_set_on= SET SESSION ndb_join_pushdown = ON, ndb_join_pushdown_aggregate = ON, ndb_join_pushdown_aggregate_outer_join = ON, ndb_pushdown_aggregate = OFF
}
if ($spj_agg == 2)
{
  # Single-table aggregation: no pushed join is expected.
  --let $spj_set_off= SET SESSION ndb_join_pushdown = ON, ndb_join_pushdown_aggregate = OFF, ndb_pushdown_aggregate = OFF
  --let $spj_set_on= SET SESSION ndb_join_pushdown = ON, ndb_join_pushdown_aggregate = OFF, ndb_pushdown_aggregate = ON
  --let $spj_agg_text= Using pushed aggregation
  --let $spj_min_pushed= 0
}

# What does EXPLAIN show with the pushdown ON?  A plain EXPLAIN first:
# query_get_value() cannot report an error, and building the pushed join
# may fail the statement.
--eval $spj_set_on
--let $spj_pushed= 0
--let $spj_agg_pushed= 0
--let $spj_row= 1
--disable_abort_on_error
--disable_result_log
--disable_warnings
--eval EXPLAIN $spj_query
--let $spj_explain_err= $mysql_errno
--enable_warnings
--enable_result_log
--enable_abort_on_error
--let $spj_more= 1
if ($spj_explain_err)
{
  --let $spj_more= 0
}
while ($spj_more)
{
  --let $spj_extra= query_get_value("EXPLAIN $spj_query", Extra, $spj_row)
  --let $spj_more= `SELECT "$spj_extra" <> 'No such row'`
  if ($spj_more)
  {
    --let $spj_hit= `SELECT LOCATE('pushed join@', "$spj_extra") > 0`
    if ($spj_hit)
    {
      --inc $spj_pushed
    }
    --let $spj_hit= `SELECT LOCATE('$spj_agg_text', "$spj_extra") > 0`
    if ($spj_hit)
    {
      --let $spj_agg_pushed= 1
    }
    --inc $spj_row
  }
}

# Run without and with the pushdown, each result into its own file.
# Warnings stay out of the files: a dropped pushed join warns (1296)
# without changing the result.
--disable_warnings
--disable_abort_on_error
--eval $spj_set_off
--output $spj_off_file
--eval $spj_query
--let $spj_err_off= $mysql_errno

--eval $spj_set_on
--let $spj_exec0= query_get_value(SHOW SESSION STATUS LIKE 'Ndb_pushed_queries_executed', Value, 1)
--output $spj_on_file
--eval $spj_query
--let $spj_err_on= $mysql_errno
--let $spj_exec1= query_get_value(SHOW SESSION STATUS LIKE 'Ndb_pushed_queries_executed', Value, 1)
--enable_abort_on_error
--enable_warnings

# Compare the two files: as multisets, or row by row if $spj_ordered.
# mysqltest looks variables up without regard to case, so an environment
# variable must not share its name with a $variable: "let SPJ_ON_FILE="
# would update $spj_on_file and export it as spj_on_file.
--let SPJ_CMP_OFF= $spj_off_file
--let SPJ_CMP_ON= $spj_on_file
--let SPJ_CMP_ROW_ORDER= $spj_ordered
--let SPJ_VERDICT_FILE= $spj_dir/spj_shapes_verdict.inc
--let SPJ_REPORT_FILE= $spj_dir/spj_shapes_report.inc
--perl
use strict;
use warnings;

sub slurp {
  my ($name) = @_;
  my $file = $ENV{$name};
  # --output always creates the file, so a missing one is a test bug that
  # would otherwise compare two empty results.
  die "$name is not set\n" unless defined $file;
  open(my $fh, '<', $file) or die "Cannot open $file: $!";
  my @lines = <$fh>;
  close($fh);
  chomp(@lines);
  return @lines;
}

sub printable {
  my ($line) = @_;
  $line =~ s/\t/ | /g;
  $line =~ s/\$/S/g;
  $line =~ s/[^\x20-\x7e]/?/g;
  $line = substr($line, 0, 160) . '...' if length($line) > 160;
  return $line;
}

my $ordered = $ENV{SPJ_CMP_ROW_ORDER} ? 1 : 0;
my @off = slurp('SPJ_CMP_OFF');
my @on = slurp('SPJ_CMP_ON');
# The first line of a non-empty result is the column header.
my $rows_off = @off ? scalar(@off) - 1 : 0;
my $rows_on = @on ? scalar(@on) - 1 : 0;
my @a = $ordered ? @off : sort @off;
my @b = $ordered ? @on : sort @on;

my $same = (scalar(@a) == scalar(@b)) ? 1 : 0;
if ($same) {
  for my $i (0 .. $#a) {
    if ($a[$i] ne $b[$i]) { $same = 0; last; }
  }
}

my @report;
if (!$same) {
  if ($ordered) {
    my $n = (@a > @b) ? scalar(@a) : scalar(@b);
    for my $i (0 .. $n - 1) {
      my $x = defined $a[$i] ? $a[$i] : '<no row>';
      my $y = defined $b[$i] ? $b[$i] : '<no row>';
      next if $x eq $y;
      push @report, "line $i without pushdown: " . printable($x);
      push @report, "line $i with pushdown:    " . printable($y);
      last if @report >= 6;
    }
  } else {
    my %count;
    $count{$_}++ for @a;
    $count{$_}-- for @b;
    my @only_off = grep { $count{$_} > 0 } sort keys %count;
    my @only_on = grep { $count{$_} < 0 } sort keys %count;
    for my $l (@only_off[0 .. ($#only_off < 2 ? $#only_off : 2)]) {
      push @report, "only without pushdown (x$count{$l}): " . printable($l);
    }
    for my $l (@only_on[0 .. ($#only_on < 2 ? $#only_on : 2)]) {
      push @report, "only with pushdown (x" . (-$count{$l}) . "): " .
                    printable($l);
    }
  }
}

open(my $v, '>', $ENV{SPJ_VERDICT_FILE})
  or die "Cannot write $ENV{SPJ_VERDICT_FILE}: $!";
print $v "--let \$spj_same= $same\n";
print $v "--let \$spj_rows= $rows_off\n";
print $v "--let \$spj_rows_on= $rows_on\n";
close($v);

open(my $r, '>', $ENV{SPJ_REPORT_FILE})
  or die "Cannot write $ENV{SPJ_REPORT_FILE}: $!";
print $r "--echo #     $_\n" for @report;
close($r);
EOF
--source $spj_dir/spj_shapes_verdict.inc

--let $spj_exec= `SELECT $spj_exec1 > $spj_exec0`
--let $spj_line= # $spj_label: rows=$spj_rows pushed=$spj_pushed exec=$spj_exec
if ($spj_agg)
{
  --let $spj_line= $spj_line agg=$spj_agg_pushed
}
--echo $spj_line

--let $spj_bad=
if ($spj_pushed < $spj_min_pushed)
{
  --let $spj_bad= EXPLAIN shows $spj_pushed pushed table(s), expected at least $spj_min_pushed
}
if ($spj_agg)
{
  if ($spj_expect_agg)
  {
    if (!$spj_agg_pushed)
    {
      --let $spj_bad= EXPLAIN does not show '$spj_agg_text'
    }
  }
}
if (!$spj_same)
{
  --let $spj_bad= results differ, $spj_rows row(s) without and $spj_rows_on row(s) with pushdown
}
if ($spj_err_on)
{
  --let $spj_bad= error $spj_err_on with pushdown
}
if ($spj_explain_err)
{
  --let $spj_bad= EXPLAIN failed with error $spj_explain_err
}
if ($spj_err_off)
{
  --let $spj_bad= error $spj_err_off without pushdown
}
if ($spj_bad)
{
  --echo #   FAILED: $spj_bad
  if (!$spj_same)
  {
    --source $spj_dir/spj_shapes_report.inc
  }
  --inc $spj_nfail
  --let $spj_fail_list= $spj_fail_list $spj_label
}

--remove_files_wildcard $spj_dir spj_shapes_*
--let $spj_ordered= 0
--let $spj_agg= 0
--let $spj_min_pushed= 2
--let $spj_expect_agg= 1
SET SESSION ndb_join_pushdown = ON, ndb_join_pushdown_aggregate = OFF, ndb_join_pushdown_aggregate_outer_join = OFF, ndb_pushdown_aggregate = OFF;
--enable_query_log
