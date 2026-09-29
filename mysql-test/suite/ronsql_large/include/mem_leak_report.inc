# mem_leak_report.inc -- append MLKE_REPORT_LINE (environment) to
# $MYSQLTEST_VARDIR/log/ronsql_large_mem_leak.report, prefixed with the
# time of day and MLKE_TEST.  Not part of the result file.
--perl
use strict;
use warnings;
my $f = "$ENV{MYSQLTEST_VARDIR}/log/ronsql_large_mem_leak.report";
open(my $fh, '>>', $f) or die "mem_leak_report: open $f: $!\n";
my @t = localtime();
printf $fh "%02d:%02d:%02d %s %s\n", $t[2], $t[1], $t[0],
  ($ENV{MLKE_TEST} // '?'), ($ENV{MLKE_REPORT_LINE} // '');
close($fh);
EOF
