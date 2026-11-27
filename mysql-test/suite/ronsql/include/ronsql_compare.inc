# This include-file, or subroutine, executes a query with both mysql client and
# ronsql_cli, then compares the results.
#
# Optional: $RONSQL_PREFIX — RonSQL-only statement-head syntax (e.g.
# "FRAGS_PER_WORKER = 4") prepended to the query on the RDRS RonSQL side
# only; the MySQL baseline runs the bare query.  Requires
# $suppress_ronsql_cli=yes since the ronsql_cli invocation would need the
# same prefixing.  Reset at the end like $QUERY.
#
# Every comparison is strict (T-1, T-24): a difference between ronsql_cli
# and RDRS, or between mysqld and RDRS, fails the test instead of being
# recorded into the .result file.  $strict_diff is still accepted (many
# tests set it once, and it persists) but changes nothing.
#
# Optional: $known_diff — the name of a file in suite/ronsql/known_diffs/
# that holds the exact diff -u output of mysqld against RDRS for a RonSQL
# bug that is still present.  The file name starts with the finding ID
# (e.g. Q-6_...).  The diff must equal the file: a changed wrong value, and
# a fixed one, fail the test.  On success the include prints one line,
# "KNOWN-DIFF <file>", instead of the diff.  ronsql_cli against RDRS stays
# strict.  Reset at the end like $QUERY.
#
# RDRS must run the statement once (T-7): the request asks for the
# x-ronsql-phases header, and the comparison fails unless it says
# attempts=1, printing the response status and the RonSQL headers, so a
# statement RDRS retried (a temporary error, a stale schema) cannot pass
# unseen.  Nothing is printed when it holds.  Optional: $allow_retry — set
# it for a comparison that may be retried (an error insert, a node
# failure).  Reset at the end like $QUERY, so that it covers only the
# comparison it is set for and cannot switch the check off for the rest
# of a test, as a $strict_diff left unset did.
#
# Row order (T-11): the body lines of the three outputs are sorted before
# the diffs, except when the query has a top-level ORDER BY (one outside
# parentheses, so not the ORDER BY of a CTE body or a subquery): then the
# outputs are compared in the order they come, which checks RonSQL's
# ORDER BY.  Optional: $skip_sort — never sort; $force_sort — sort even
# with a top-level ORDER BY, for an ORDER BY whose keys leave ties (rows
# that mysqld and RonSQL may give in either order).  Both reset at the end
# like $QUERY.
#
# Optional (T-23, used by ronsql_check.inc), each reset at the end like
# $QUERY: $expect_rows — RDRS's x-ronsql-phases must say rows=<this>;
# $expect_fetched_max — its fetched= must be at most this;
# $_ronsql_keep_mysql_out — keep mysqld's output, as compared (sorted
# unless the query has a top-level ORDER BY), in
# $MYSQL_TMP_DIR/ronsql-check-mysql.out for the caller.
--source include/have_util_sed.inc
--source include/have_util_curl.inc
if ($RONSQL_PREFIX!='')
{
  if ($suppress_ronsql_cli=='')
  {
    --die RONSQL_PREFIX requires suppress_ronsql_cli=yes
  }
}
--echo == Query ==
if ($QUERY!='')
{
  --echo $QUERY
  --let $QUERY_PARAMS = -e '$QUERY'
  --exec printf '%s' '$QUERY' > $MYSQL_TMP_DIR/ronsql-query.sql
}
if ($QUERY=='')
{
  if ($QUERY_FILE=='')
  {
    --die Must define either QUERY or QUERY_FILE variable
  }
  --exec cat $QUERY_FILE
  --let $QUERY_PARAMS = < '$QUERY_FILE'
  --exec cat $QUERY_FILE > $MYSQL_TMP_DIR/ronsql-query.sql
}
if ($RONSQL_PREFIX!='')
{
  --echo RonSQL-side prefix: $RONSQL_PREFIX
  --exec printf '{"query":"%s ' '$RONSQL_PREFIX' > $MYSQL_TMP_DIR/ronsql-rdrs-body-1
}
if ($RONSQL_PREFIX=='')
{
  --exec printf '{"query":"' > $MYSQL_TMP_DIR/ronsql-rdrs-body-1
}
--exec perl -pe 's/\\/\\\\/g; s/"/\\"/g; s/\x08/\\b/g; s/\f/\\f/g; s/\n/\\n/g; s/\r/\\r/g; s/\t/\\t/g;' $MYSQL_TMP_DIR/ronsql-query.sql > $MYSQL_TMP_DIR/ronsql-rdrs-body-2
--exec printf '","database":"test","outputFormat":"TEXT"}' > $MYSQL_TMP_DIR/ronsql-rdrs-body-3
--exec cat $MYSQL_TMP_DIR/ronsql-rdrs-body-1 $MYSQL_TMP_DIR/ronsql-rdrs-body-2 $MYSQL_TMP_DIR/ronsql-rdrs-body-3 > $MYSQL_TMP_DIR/ronsql-rdrs-body
--let $QUERY_PARAMS = -D test $QUERY_PARAMS

# T-11: is there an ORDER BY outside parentheses?  Quoted text and comments
# are blanked first.  The answer goes to ronsql-order-by.inc.
--perl
use strict;
use warnings;
my $dir = $ENV{MYSQL_TMP_DIR} or die "ronsql_compare.inc: MYSQL_TMP_DIR is not set\n";
open(my $in, '<', "$dir/ronsql-query.sql") or die "ronsql_compare.inc: ronsql-query.sql: $!\n";
my $q = do { local $/; <$in> } // '';
close($in);
my ($top, $depth, $i, $n) = ('', 0, 0, length($q));
while ($i < $n) {
  my $c = substr($q, $i, 1);
  if ($c eq "'" || $c eq '"' || $c eq '`') {
    my $j = $i + 1;
    while ($j < $n) {
      my $d = substr($q, $j, 1);
      if ($d eq "\\" && $c ne '`') { $j += 2; next; }
      if ($d eq $c) {
        if ($j + 1 < $n && substr($q, $j + 1, 1) eq $c) { $j += 2; next; }
        last;
      }
      $j++;
    }
    $top .= ' ' if $depth == 0;
    $i = $j + 1;
    next;
  }
  if ($c eq '#' || substr($q, $i, 3) =~ /^--\s/) {
    my $j = index($q, "\n", $i);
    $i = $j < 0 ? $n : $j;
    next;
  }
  if (substr($q, $i, 2) eq '/*') {
    my $j = index($q, '*/', $i + 2);
    $top .= ' ' if $depth == 0;
    $i = $j < 0 ? $n : $j + 2;
    next;
  }
  if ($c eq '(') { $top .= ' ' if $depth == 0; $depth++; $i++; next; }
  if ($c eq ')') { $depth-- if $depth > 0; $i++; next; }
  $top .= $c if $depth == 0;
  $i++;
}
my $ordered = $top =~ /\bORDER\s+BY\b/i ? 'yes' : '';
open(my $out, '>', "$dir/ronsql-order-by.inc") or die "ronsql_compare.inc: ronsql-order-by.inc: $!\n";
print $out "--let \$_ronsql_top_order_by = $ordered\n";
close($out);
EOF
--source $MYSQL_TMP_DIR/ronsql-order-by.inc
--remove_file $MYSQL_TMP_DIR/ronsql-order-by.inc
--let $_ronsql_sort = yes
if ($_ronsql_top_order_by != '')
{
  --let $_ronsql_sort =
}
if ($force_sort != '')
{
  --let $_ronsql_sort = yes
}
if ($skip_sort != '')
{
  --let $_ronsql_sort =
}

# RonSQL doesn't support time zones yet, and works as if in UTC. MySQL uses
# system time zone, but it seems mtr somehow sets it to +03:00 before starting a
# test case. Until RonSQL and RDRS supports time zones, we will set UTC time
# zone explicitly in each test case where it has an impact. This does not carry
# over to MySQL clients launched with --exec, so we use the --init-command-add
# parameter for that.
--exec $MYSQL --default-character-set=utf8mb4 --init-command-add='SET time_zone = "+00:00";' $QUERY_PARAMS > $MYSQL_TMP_DIR/mysql.out
if ($canonicalization_script)
{
  --exec sed -r '$canonicalization_script' $MYSQL_TMP_DIR/mysql.out > $MYSQL_TMP_DIR/mysql.out.canon
  --move_file $MYSQL_TMP_DIR/mysql.out.canon $MYSQL_TMP_DIR/mysql.out
}
if ($canonicalization_perl)
{
  --exec perl -pe '$canonicalization_perl' $MYSQL_TMP_DIR/mysql.out > $MYSQL_TMP_DIR/mysql.out.canon
  --move_file $MYSQL_TMP_DIR/mysql.out.canon $MYSQL_TMP_DIR/mysql.out
}
if ($_ronsql_sort!='')
{
  --exec (head -n 1 $MYSQL_TMP_DIR/mysql.out && tail -n +2 $MYSQL_TMP_DIR/mysql.out | sort) > $MYSQL_TMP_DIR/mysql.out.sorted
}
if ($_ronsql_sort=='')
{
  --exec cp $MYSQL_TMP_DIR/mysql.out $MYSQL_TMP_DIR/mysql.out.sorted
}
--echo
--exec echo Number of output lines, including header: `grep -Fc '' '$MYSQL_TMP_DIR/mysql.out.sorted'`

--exec curl -s -X POST -H "Content-Type: application/json" -H "x-ronsql-phases: 1" -D $MYSQL_TMP_DIR/ronsql-rdrs.hdr http://$RDRS_NOKEY_HOST:$RDRS_NOKEY_PORT/0.1.0/ronsql --data-binary @$MYSQL_TMP_DIR/ronsql-rdrs-body > $MYSQL_TMP_DIR/ronsql-rdrs.out
if ($canonicalization_script)
{
  --exec sed -r '$canonicalization_script' $MYSQL_TMP_DIR/ronsql-rdrs.out > $MYSQL_TMP_DIR/ronsql-rdrs.out.canon
  --move_file $MYSQL_TMP_DIR/ronsql-rdrs.out.canon $MYSQL_TMP_DIR/ronsql-rdrs.out
}
if ($canonicalization_perl)
{
  --exec perl -pe '$canonicalization_perl' $MYSQL_TMP_DIR/ronsql-rdrs.out > $MYSQL_TMP_DIR/ronsql-rdrs.out.canon
  --move_file $MYSQL_TMP_DIR/ronsql-rdrs.out.canon $MYSQL_TMP_DIR/ronsql-rdrs.out
}
if ($_ronsql_sort!='')
{
  --exec (head -n 1 $MYSQL_TMP_DIR/ronsql-rdrs.out && tail -n +2 $MYSQL_TMP_DIR/ronsql-rdrs.out | sort) > $MYSQL_TMP_DIR/ronsql-rdrs.out.sorted
}
if ($_ronsql_sort=='')
{
  --exec cp $MYSQL_TMP_DIR/ronsql-rdrs.out $MYSQL_TMP_DIR/ronsql-rdrs.out.sorted
}

if ($suppress_ronsql_cli=='')
{
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING $QUERY_PARAMS > $MYSQL_TMP_DIR/ronsql-cli.out
if ($canonicalization_script)
{
  --exec sed -r '$canonicalization_script' $MYSQL_TMP_DIR/ronsql-cli.out > $MYSQL_TMP_DIR/ronsql-cli.out.canon
  --move_file $MYSQL_TMP_DIR/ronsql-cli.out.canon $MYSQL_TMP_DIR/ronsql-cli.out
}
if ($canonicalization_perl)
{
  --exec perl -pe '$canonicalization_perl' $MYSQL_TMP_DIR/ronsql-cli.out > $MYSQL_TMP_DIR/ronsql-cli.out.canon
  --move_file $MYSQL_TMP_DIR/ronsql-cli.out.canon $MYSQL_TMP_DIR/ronsql-cli.out
}
if ($_ronsql_sort!='')
{
  --exec (head -n 1 $MYSQL_TMP_DIR/ronsql-cli.out && tail -n +2 $MYSQL_TMP_DIR/ronsql-cli.out | sort) > $MYSQL_TMP_DIR/ronsql-cli.out.sorted
}
if ($_ronsql_sort=='')
{
  --exec cp $MYSQL_TMP_DIR/ronsql-cli.out $MYSQL_TMP_DIR/ronsql-cli.out.sorted
}
--exec diff -u --label ronsql-cli.out $MYSQL_TMP_DIR/ronsql-cli.out.sorted --label ronsql-rdrs.out $MYSQL_TMP_DIR/ronsql-rdrs.out.sorted
}

--echo
--echo == Diff ==
if ($known_diff=='')
{
--exec diff -u --label mysql.out $MYSQL_TMP_DIR/mysql.out.sorted --label ronsql.out $MYSQL_TMP_DIR/ronsql-rdrs.out.sorted
}
if ($known_diff!='')
{
--let $_ronsql_known_diff_file = $MYSQL_TEST_DIR/suite/ronsql/known_diffs/$known_diff
--file_exists $_ronsql_known_diff_file
--error 0,1
--exec diff -u --label mysql.out $MYSQL_TMP_DIR/mysql.out.sorted --label ronsql.out $MYSQL_TMP_DIR/ronsql-rdrs.out.sorted > $MYSQL_TMP_DIR/ronsql-known.diff
# On a mismatch the failure output shows the diff against the known diff,
# then the whole diff of this run (the new content of the file if the
# change is intended; remove $known_diff when the bug is fixed).
--exec diff -u --label known_diffs/$known_diff $_ronsql_known_diff_file --label this-run $MYSQL_TMP_DIR/ronsql-known.diff || (echo "The diff of mysqld and RonSQL is not the known diff $known_diff. The diff of this run:"; cat $MYSQL_TMP_DIR/ronsql-known.diff; exit 1)
--echo KNOWN-DIFF $known_diff
--remove_file $MYSQL_TMP_DIR/ronsql-known.diff
}
if ($allow_retry=='')
{
--exec grep -qiE '^x-ronsql-phases:.*,attempts=1,' $MYSQL_TMP_DIR/ronsql-rdrs.hdr || (echo "RDRS did not run the statement exactly once (attempts=1); set allow_retry for a comparison that may be retried. Response status and RonSQL headers:"; grep -iE '^(HTTP/|x-ronsql-)' $MYSQL_TMP_DIR/ronsql-rdrs.hdr | tr -d '\r'; exit 1)
}
if ($expect_rows!='')
{
--exec grep -qiE '^x-ronsql-phases:.*,rows=$expect_rows,' $MYSQL_TMP_DIR/ronsql-rdrs.hdr || (echo "RDRS did not return rows=$expect_rows. RonSQL headers:"; grep -iE '^x-ronsql-' $MYSQL_TMP_DIR/ronsql-rdrs.hdr | tr -d '\r'; exit 1)
}
if ($expect_fetched_max!='')
{
--let RONSQL_COMPARE_FETCHED_MAX = $expect_fetched_max
--perl
use strict;
use warnings;
my $dir = $ENV{MYSQL_TMP_DIR};
my $max = $ENV{RONSQL_COMPARE_FETCHED_MAX};
open(my $in, '<', "$dir/ronsql-rdrs.hdr") or die "ronsql_compare.inc: ronsql-rdrs.hdr: $!\n";
my $fetched;
while (my $line = <$in>) {
  $fetched = $1 if $line =~ /^x-ronsql-phases:.*,fetched=(\d+),/i;
}
close($in);
die "ronsql_compare.inc: no fetched= in x-ronsql-phases\n" unless defined $fetched;
die "ronsql_compare.inc: RDRS fetched $fetched rows, more than $max\n" if $fetched > $max;
EOF
}
if ($_ronsql_keep_mysql_out!='')
{
--copy_file $MYSQL_TMP_DIR/mysql.out.sorted $MYSQL_TMP_DIR/ronsql-check-mysql.out
}
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs.hdr
--remove_file $MYSQL_TMP_DIR/ronsql-query.sql
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body-1
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body-2
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs-body-3
--remove_file $MYSQL_TMP_DIR/mysql.out
--remove_file $MYSQL_TMP_DIR/mysql.out.sorted
if ($suppress_ronsql_cli=='')
{
--remove_file $MYSQL_TMP_DIR/ronsql-cli.out
--remove_file $MYSQL_TMP_DIR/ronsql-cli.out.sorted
}
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs.out
--remove_file $MYSQL_TMP_DIR/ronsql-rdrs.out.sorted
--echo ==========
--echo
--echo
--echo
if ($QUERY=='')
{
  --remove_file $QUERY_FILE
}
--let $QUERY=
--let $RONSQL_PREFIX=
--let $canonicalization_script=
--let $canonicalization_perl=
--let $skip_sort=
--let $force_sort=
--let $known_diff=
--let $allow_retry=
--let $expect_rows=
--let $expect_fetched_max=
--let $_ronsql_keep_mysql_out=
