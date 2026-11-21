# ronsql_check.inc (RONDB-1124 T-23): one case from one query text.
#
# A case written with ronsql_compare.inc repeats its query: for the
# comparison, for EXPLAIN, and in an "== Expected result ==" block that
# runs it in mysqld a fourth time, and nothing keeps the copies the same.
# Here the query is given once, in $QUERY or in the file $QUERY_FILE (for
# a query with a quote, which $QUERY cannot carry to the shell), and the
# include:
#   - compares mysqld, RDRS and ronsql_cli (ronsql_compare.inc, strict,
#     RDRS run once, with its options $known_diff, $allow_retry,
#     $skip_sort, $force_sort, $suppress_ronsql_cli,
#     $canonicalization_script and $canonicalization_perl);
#   - prints mysqld's output of that comparison under
#     "== Expected result ==": the header and the rows, sorted unless the
#     query has a top-level ORDER BY, nothing for an empty result (as the
#     mysql client prints it);
#   - when set, checks $expect_rows (rows= of RDRS's x-ronsql-phases),
#     $expect_fetched_max (fetched= at most this) and $explain_greps: one
#     fixed string per line, each of which EXPLAIN of the query must
#     contain (the EXPLAIN text is printed only when one is missing).
# Every option resets at the end, like $QUERY.
--let $_rc_query = $QUERY
--let $_rc_file = $MYSQL_TMP_DIR/ronsql-check-query.sql
if ($_rc_query == '')
{
  if ($QUERY_FILE == '')
  {
    --die ronsql_check.inc: set QUERY or QUERY_FILE
  }
  --copy_file $QUERY_FILE $_rc_file
}
--let $_rc_greps = $explain_greps

--let $_ronsql_keep_mysql_out = yes
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--cat_file $MYSQL_TMP_DIR/ronsql-check-mysql.out
--remove_file $MYSQL_TMP_DIR/ronsql-check-mysql.out

if ($_rc_greps != '')
{
  --let $QUERY = $_rc_query
  if ($_rc_query == '')
  {
    --let $QUERY_FILE = $_rc_file
  }
  --let $EXPLAIN_FILE = $MYSQL_TMP_DIR/ronsql-check-explain.out
  --source suite/ronsql/include/ronsql_explain.inc
  --let RONSQL_CHECK_GREPS = $_rc_greps
  --perl
use strict;
use warnings;
my $dir = $ENV{MYSQL_TMP_DIR};
open(my $in, '<', "$dir/ronsql-check-explain.out")
  or die "ronsql_check.inc: ronsql-check-explain.out: $!\n";
my $explain = do { local $/; <$in> };
close($in);
my @missing;
for my $pattern (split /\n/, $ENV{RONSQL_CHECK_GREPS}) {
  $pattern =~ s/^\s+|\s+$//g;
  next if $pattern eq '';
  push @missing, $pattern if index($explain, $pattern) < 0;
}
if (@missing) {
  print "EXPLAIN lacks: $_\n" for @missing;
  print "EXPLAIN:\n$explain";
  die "ronsql_check.inc: EXPLAIN does not have the expected plan\n";
}
EOF
  --remove_file $EXPLAIN_FILE
}
if ($_rc_query == '')
{
  if ($_rc_greps == '')
  {
    --remove_file $_rc_file
  }
}
--let $QUERY =
--let $QUERY_FILE =
--let $explain_greps =
--let $_rc_query =
--let $_rc_greps =
