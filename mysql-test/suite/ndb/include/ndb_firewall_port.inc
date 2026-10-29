#
# Block or unblock new incoming TCP connections to a port of this host
# with the firewall: pf (pfctl) on macOS, nft or iptables on Linux. Only
# connection attempts (SYN) are blocked, connections that exist go on. A
# blocked attempt is refused with a TCP reset, so that the client does
# not wait for a connect timeout. Changing the firewall needs root, so
# unless the test runs as root the commands run through "sudo -n", which
# needs passwordless sudo for pfctl, nft or iptables.
#
# Parameters:
#   $ndb_firewall_action  check, block or unblock
#   $ndb_firewall_port    the TCP port, for block and unblock
#
# check skips the test when this host's firewall cannot be changed.
# The rule is kept apart from other rules, in the pf anchor
# com.apple/ndb_mtr_<pid>, the nft table inet ndb_mtr_<pid> or with the
# iptables comment ndb_mtr_<pid>, where <pid> is the mysqltest process of
# the test. It is removed when that process ends, also when the test
# fails before it unblocks. The work is done by ndb_firewall_port.pl.
#
# Before the test suites, "perl ndb_firewall_port.pl status <vardir>"
# tells whether these tests can run and what to do if not, see
# storage/ndb/claude_files/qmgr_join_protocol/testing.md.
#

--let $_ndb_firewall_port= $ndb_firewall_port
if (!$_ndb_firewall_port)
{
  --let $_ndb_firewall_port= 0
}
--exec perl $MYSQL_TEST_DIR/suite/ndb/include/ndb_firewall_port.pl $ndb_firewall_action $_ndb_firewall_port $MYSQLTEST_VARDIR

if ($ndb_firewall_action == check)
{
  --source $MYSQLTEST_VARDIR/tmp/ndb_firewall_skip.inc
  --remove_file $MYSQLTEST_VARDIR/tmp/ndb_firewall_skip.inc
  if ($ndb_firewall_skip)
  {
    --skip $ndb_firewall_skip
  }
}
