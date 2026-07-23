# suite/ndb_ttl_purge/include/ttl_purge_mark_rdrs_log.inc
#
# Record the current size of the RDRS log so a following
# ttl_purge_wait_rdrs_log.inc only matches content appended AFTER this point
# (the log persists across tests, so absolute searches can false-match old
# lines).

--perl
use strict;
use warnings;
my $log = "$ENV{MYSQLTEST_VARDIR}/log/rdrs.1.1.log";
my $sz = -s $log;
$sz = 0 unless defined $sz;
open my $o, '>', "$ENV{MYSQL_TMP_DIR}/_rdrs_log_offset.txt" or die "Cannot write: $!";
print $o "$sz\n";
close $o;
EOF
