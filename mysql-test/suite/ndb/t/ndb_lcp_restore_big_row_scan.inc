# Reads the LCP data files of fragment 0 of table $BIG_ROW_TABLE_ID that
# the last LCP from ndb_lcp_restore_big_row_lcp.inc wrote, on all data
# nodes, and finds the offset and length in words of each record.
#
# $BIG_ROW_MODE = probe: the table has one row with a VARCHAR of
#   $BIG_ROW_PROBE_V bytes. From where its record starts and its length,
#   choose the VARCHAR lengths (step 4 bytes, one word) of a small row,
#   $v_small, and of three big rows of 8800 words, $v_big, so that the
#   record of the first big row ends exactly at the end of the second 32 kB
#   page of the data file. Writes them as let commands to
#   $MYSQLTEST_VARDIR/tmp/ndb_lcp_restore_big_row_lengths.inc.
#
# $BIG_ROW_MODE = check: every data node must have a data file with four
#   records, the last three above 8192 words (a page), the second ending at
#   a page end.

let BIG_ROW_VARDIR = $MYSQLTEST_VARDIR;
--perl
use strict;
my $vardir = $ENV{BIG_ROW_VARDIR};
my $tab = $ENV{BIG_ROW_TABLE_ID};
my $mode = $ENV{BIG_ROW_MODE};
my $page_words = 8192;
my $tmp = "$vardir/tmp/ndb_lcp_restore_big_row.raw";

open(my $in, '<', "$vardir/tmp/ndb_lcp_restore_big_row.since")
  or die "open since file: $!";
my $since = <$in>;
close($in);
chomp $since;

# [offset, length] in words of each record in an LCP data file. The file
# starts with the magic and the backup version (3 words) followed by
# sections of type and length. Records follow the FRAGMENT_HEADER (2)
# section, each with a header word of length - 1 and record type << 16, and
# end with a header word of 0.
sub records {
  my ($file) = @_;
  unlink($tmp);
  system($ENV{NDBXFRM_EXE}, '--no-defaults', $file, $tmp) == 0
    or die "ndbxfrm failed on $file\n";
  open(my $fh, '<:raw', $tmp) or die "open $tmp: $!";
  my $buf = do { local $/; <$fh> };
  close($fh);
  unlink($tmp);
  my @w = unpack('N*', $buf);
  my @recs;
  my $pos = 3;
  my $reading = 0;
  while ($pos + 1 < @w) {
    if ($reading) {
      my $len = ($w[$pos] + 1) & 0xFFFF;
      if ($len == 1) {
        $reading = 0;
        $pos++;
        next;
      }
      push @recs, [$pos, $len];
      $pos += $len;
    } else {
      my ($type, $len) = ($w[$pos], $w[$pos + 1]);
      last if $len == 0;
      $reading = 1 if $type == 2;
      $pos += $len;
    }
  }
  return @recs;
}

my @files = grep { (stat($_))[9] >= $since }
  glob("$vardir/mysql_cluster.1/ndbd.*/ndb_*_fs/LCP/*/T${tab}F0.Data");

if ($mode eq 'probe') {
  my %found;
  foreach my $f (@files) {
    my @r = records($f);
    next unless @r;
    die "Probe LCP data file $f has " . scalar(@r) . " records, expected 1\n"
      if @r != 1;
    $found{"$r[0][0] $r[0][1]"} = 1;
  }
  die "The LCP wrote no data file with the probe row\n" unless %found;
  die "Probe records differ: " . join(', ', sort keys %found) . "\n"
    if keys %found > 1;
  my ($first, $probe_len) = split / /, (keys %found)[0];
  my $len_big = 8800;
  my $len_small = 2 * $page_words - $first - $len_big;
  my %v;
  foreach my $p (['small', $len_small], ['big', $len_big]) {
    my ($name, $len) = @$p;
    my $v = $ENV{BIG_ROW_PROBE_V} + 4 * ($len - $probe_len);
    die "The $name record of $len words would need a VARCHAR of $v bytes "
      . "(probe record starts at word $first, $probe_len words)\n"
      if $v < 256 || $v > 32614;
    $v{$name} = $v;
  }
  open(my $out, '>', "$vardir/tmp/ndb_lcp_restore_big_row_lengths.inc")
    or die "open lengths file: $!";
  print $out "let \$v_small = $v{small};\n";
  print $out "let \$v_big = $v{big};\n";
  close($out);
} elsif ($mode eq 'check') {
  my %ok;
  my @seen;
  foreach my $f (@files) {
    my ($node) = $f =~ m{/(ndbd\.\d+)/};
    my @r = records($f);
    push @seen, "$node: " . join(' ', map { "$_->[0]+$_->[1]" } @r);
    next unless @r == 4 && (grep { $_->[1] > $page_words } @r[1 .. 3]) == 3;
    $ok{$node} = 1 if ($r[1][0] + $r[1][1]) % $page_words == 0;
  }
  foreach my $dir (glob("$vardir/mysql_cluster.1/ndbd.*")) {
    my ($node) = $dir =~ m{/(ndbd\.\d+)$};
    die "$node has no LCP data file of T${tab}F0 with four records, the "
      . "last three above $page_words words and the second ending at a "
      . "page end, so the restarts would not test the restore of such "
      . "records. Records (offset+length) in the files the LCP wrote:\n"
      . join("\n", @seen) . "\n"
      unless $ok{$node};
  }
  print "Each data node has an LCP data file with three records above "
      . "8192 words, the first of them ends at a page end\n";
} else {
  die "Unknown BIG_ROW_MODE '$mode'\n";
}
EOF
