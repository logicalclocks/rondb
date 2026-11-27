# The JSON check of one result path (README finding T-12): the checks of
# ronsql_json_check.inc (JSON on ronsql_cli, on RDRS with an explicit
# outputFormat and with the default output format), the same query under
# JSON_ASCII on ronsql_cli and on RDRS, each body fed to a JSON parser
# (the --exec fails on invalid JSON and otherwise prints the canonical
# re-encoding), and the rows of the two RDRS bodies compared with
# mysqld's result of the query: each JSON row is printed with mysqld's
# column names in mysqld's order (null as NULL) and the lists are
# compared in order when the query ends with an ORDER BY (one after its
# last closing parenthesis), sorted otherwise.  The JSON_ASCII bodies
# must hold ASCII bytes only.
# $QUERY as for ronsql_json_check.inc (no backticks, no quotes of either
# kind).  Requires $JSON_CANON and $RDRS_RONSQL from the caller.
--let $jp_query=$QUERY
--source suite/ronsql/include/ronsql_json_check.inc
--echo == ronsql_cli JSON_ASCII ==
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test --output-format JSON_ASCII -e "$jp_query" > $MYSQL_TMP_DIR/jp-cli-ascii.out
--exec cat $MYSQL_TMP_DIR/jp-cli-ascii.out
--exec $JSON_CANON < $MYSQL_TMP_DIR/jp-cli-ascii.out
--echo == RDRS JSON_ASCII ==
--exec $RDRS_RONSQL '{"query":"$jp_query","database":"test","outputFormat":"JSON_ASCII"}' > $MYSQL_TMP_DIR/jp-rdrs-ascii.out
--exec cat $MYSQL_TMP_DIR/jp-rdrs-ascii.out
--exec $JSON_CANON < $MYSQL_TMP_DIR/jp-rdrs-ascii.out
--echo == Rows of the RDRS bodies vs mysqld ==
--exec $RDRS_RONSQL '{"query":"$jp_query","database":"test"}' > $MYSQL_TMP_DIR/jp-rdrs.out
--exec $MYSQL --default-character-set=utf8mb4 -D test -e '$jp_query' > $MYSQL_TMP_DIR/jp-mysql.out
# The query reaches perl through a file: $ENV{JP_QUERY}, set with
# --let JP_QUERY=..., was not defined in the perl block.
--exec printf '%s' '$jp_query' > $MYSQL_TMP_DIR/jp-query.sql
--perl
use strict;
use warnings;
use JSON::PP;
use Encode qw(decode);
my $dir = $ENV{MYSQL_TMP_DIR};
sub slurp {
  my ($path) = @_;
  open(my $fh, '<:raw', $path) or die "Cannot open $path: $!";
  local $/;
  my $s = <$fh>;
  close($fh);
  return defined($s) ? $s : '';
}
binmode(STDOUT, ':encoding(UTF-8)');
# The main query is ordered when an ORDER BY follows its last ')'.
my $ordered = (slurp("$dir/jp-query.sql") =~ /ORDER\s+BY[^)]*$/i);
my @mysql = split(/\n/, decode('UTF-8', slurp("$dir/jp-mysql.out")));
my @header = @mysql ? split(/\t/, shift(@mysql), -1) : ();
my $ascii_only = (slurp("$dir/jp-cli-ascii.out") .
                  slurp("$dir/jp-rdrs-ascii.out")) !~ /[\x80-\xff]/;
foreach my $format ('JSON', 'JSON_ASCII') {
  my $file = ($format eq 'JSON') ? "$dir/jp-rdrs.out"
                                 : "$dir/jp-rdrs-ascii.out";
  my $rows = JSON::PP->new->utf8->decode(slurp($file))->{data};
  my @got;
  foreach my $row (@$rows) {
    my @fields;
    foreach my $name (@header) {
      push(@fields, !exists($row->{$name}) ? '<missing>'
                    : defined($row->{$name}) ? "$row->{$name}" : 'NULL');
    }
    my %known = map { $_ => 1 } @header;
    my $extra = grep { !$known{$_} } keys(%$row);
    push(@fields, "<$extra more keys>") if $extra;
    push(@got, join("\t", @fields));
  }
  my @want = @mysql;
  if (!$ordered) {
    @got = sort(@got);
    @want = sort(@want);
  }
  my $tail = '';
  if ($format eq 'JSON_ASCII') {
    $tail = $ascii_only ? ', ASCII only' : ', NOT ASCII ONLY';
  }
  if (join("\n", @got) eq join("\n", @want)) {
    print "RDRS $format: ", scalar(@got), " row(s), as mysqld$tail\n";
  } else {
    print "RDRS $format differs from mysqld$tail\n";
    print "mysqld: $_\n" foreach @want;
    print "RDRS:   $_\n" foreach @got;
  }
}
EOF
--remove_file $MYSQL_TMP_DIR/jp-query.sql
--remove_file $MYSQL_TMP_DIR/jp-cli-ascii.out
--remove_file $MYSQL_TMP_DIR/jp-rdrs-ascii.out
--remove_file $MYSQL_TMP_DIR/jp-rdrs.out
--remove_file $MYSQL_TMP_DIR/jp-mysql.out
--let $jp_query=
