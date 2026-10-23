# Testing the QMGR join patches

The commits of RONDB-1148:

- *RONDB-1148: Abort the inclusion of a data node that cannot connect to all nodes*
- *RONDB-1148: Time the node id reservation of a data node from its allocation*
- *RONDB-1148: Exit after a failed inclusion, without failing the nodes that joined*

They change `ndbmtd`, `ndbd` and `ndb_mgmd`. The tests need a debug
build with error inserts. `test_mgm` and the NDBT cases also need
`-DWITH_NDB_TEST=1`, which builds `testMgm` and `testNodeRestart`.

```
cd debug_build && make -j"$(sysctl -n hw.ncpu)" ndb_mgmd ndbmtd ndbd testMgm testNodeRestart
```

## Before the test suites: the firewall

The three `*_unreachable_node*` tests block new connections to the port
of one data node with the firewall of this host:

- **macOS:** `pfctl`.
- **Linux:** `nft`, or `iptables` when there is no `nft`.

Changing the firewall needs root, so a test that doesn't run as root
uses `sudo -n`. If neither works, the test is **skipped**, not failed,
so a run can look green without them.

Check this host first. The vardir is the one mtr uses, by default
`mysql-test/var` in the build directory:

```
perl mysql-test/suite/ndb/include/ndb_firewall_port.pl status debug_build/mysql-test/var
```

`status` checks these four things, prints a line for each, and exits
with 1 when one needs attention:

| Line | What to do |
|------|------------|
| `Firewall: ... the firewall tests are skipped` | Allow passwordless sudo for the firewall tool, see below |
| `pf does not evaluate the anchors com.apple/*` (macOS) | Restore the line `anchor "com.apple/*"` in `/etc/pf.conf`, as in the macOS default |
| `Rule ndb_mtr_<pid> was left behind` | `perl mysql-test/suite/ndb/include/ndb_firewall_port.pl cleanup` |
| `<vardir>: N files belong to another user` | An earlier `sudo ./mtr` left files that mtr cannot remove: `sudo rm -rf <vardir>` |

### Recommended: passwordless sudo for the firewall tool only

Allow your user to run only the firewall tool without a password. Then
every test, firewall or not, runs in one `./mtr` run as yourself, and
the vardir stays yours. `status` prints the lines for this host. On
macOS:

```
echo "$USER ALL=(root) NOPASSWD: /sbin/pfctl" | sudo tee /etc/sudoers.d/ndb-mtr-firewall
sudo chmod 0440 /etc/sudoers.d/ndb-mtr-firewall
sudo visudo -c
```

On Linux, list `/usr/sbin/nft` and/or `/usr/sbin/iptables` instead. This
lets your user change any firewall rule without a password, so set it up
on development and test machines only. To undo it, remove
`/etc/sudoers.d/ndb-mtr-firewall`.

### Alternative: run the firewall tests with sudo, apart

```
sudo ./mtr --vardir=$PWD/var-firewall --suite=ndb ndb_sr_unreachable_node ndb_nr_unreachable_node ndb_sr_unreachable_node_group
```

Give it its own `--vardir`, so the default vardir doesn't end up owned
by root. If that already happened, remove it before the next run as
yourself: `sudo rm -rf debug_build/mysql-test/var`.

### How a block is cleaned up

- **Naming:** a blocking rule is named after the mysqltest process of
  its test, `ndb_mtr_<pid>`. On macOS it lives in the pf anchor
  `com.apple/ndb_mtr_<pid>`. On Linux it is the nft table `inet
  ndb_mtr_<pid>`, or an iptables rule with that comment.
- **Watchdog:** a watchdog removes the rule when that mysqltest process
  ends, also when the test fails, times out or is killed.
- **Left behind:** a rule stays only if the watchdog is killed as well.
  Then the next firewall test removes it, and so does `cleanup`. Rules
  of tests that are still running are left alone, also those of mtr
  runs in other worktrees on this host. A reboot also clears them,
  since the rules aren't saved.
- **pf on macOS:** pf is enabled with a reference that is released at
  the unblock, so pf ends up as it was.
- **Parallel runs:** the firewall tests are `not_parallel`, so with
  `--parallel` they run at the end, one at a time.

## 1. New and changed MTR tests

Run all eight together, as yourself, once passwordless sudo is set up:

```
cd debug_build/mysql-test
./mtr --suite=ndb \
  ndb_sr_join_crash ndb_nr_join_crash ndb_sr_join_timeout ndb_nr_join_timeout \
  ndb_nr_join_president_failure \
  ndb_sr_unreachable_node ndb_nr_unreachable_node ndb_sr_unreachable_node_group
```

| Test | Needs | Fault | Checks |
|------|-------|-------|--------|
| `ndb_sr_join_crash` | error inserts | 963: node 4 crashes at `CM_REGCONF` in a system restart | Nodes 1-3, joined and held in start phase 2, go on, the president aborts the inclusion, node 4 joins later |
| `ndb_nr_join_crash` | error inserts | 963 in a node restart | Node 2, joined and held, survives the crash of node 4 |
| `ndb_sr_join_timeout` | error inserts | 964: no `CM_NODEINFOREQ`, system restart | Aborted twice, asks once more in between, exits with 6054, the held nodes go on |
| `ndb_nr_join_timeout` | error inserts | 964 in a node restart | Exits with 6054 at the first abort, node 2 goes on |
| `ndb_nr_join_president_failure` | error inserts | 964, then the president crashes (9999) | The other two nodes go on, the president and the joiner join again |
| `ndb_sr_unreachable_node` | firewall | Node 4 cannot reach node 3 | The system restart completes without the left out node, which asks once more and exits with 6054 |
| `ndb_nr_unreachable_node` | firewall | Node 4 cannot reach node 3 in a node restart | Node 4 exits at the first abort, node 2 restarts meanwhile |
| `ndb_sr_unreachable_node_group` | firewall, six data nodes | A node group cannot reach node 4 | Every stalled inclusion is aborted, also when node group {5,6} is left empty |

## 2. Existing MTR tests of the same code

```
./mtr --suite=ndb \
  ndb_restart_barrier_failures ndb_arbitration_president_failover ndb_restart \
  ndb_restart_debug ndb_addnode_restart \
  test_mgm ndb_mgm ndb_mgmd ndb_multi_mgmd ndb_connection_pool_nodeids \
  ndb_high_nodeid ndb_rolling_restart_mgmd_first
```

| Test | Why |
|------|-----|
| `ndb_restart_barrier_failures` | Node failures during restarts, through the `execDISCONNECT_REP` branch that now lets a node survive a disconnect |
| `ndb_arbitration_president_failover` | President failure, which now also fails the starting nodes the members left to the president |
| `ndb_restart`, `ndb_restart_debug`, `ndb_addnode_restart` | Inclusion in node restarts, system restarts and when nodes are added |
| `test_mgm` | `testMgm`: node id allocation, among other things |
| `ndb_mgm`, `ndb_mgmd`, `ndb_multi_mgmd`, `ndb_connection_pool_nodeids`, `ndb_high_nodeid`, `ndb_rolling_restart_mgmd_first` | Node id allocation and the local reservations of `ndb_mgmd` |

## 3. NDBT cases of testNodeRestart

These run in autotest (`storage/ndb/test/run-test/daily-*.txt`), not in
MTR. They create their table `TEST_DB.T1` through the NDB API, so run
them against a cluster without MySQL Servers, or the way
`ndb_restart_barrier_failures` does, which shuts its servers down
around them:

```
NDB_CONNECTSTRING=<host:port> debug_build/runtime_output_directory/testNodeRestart -n Bug42422 -l 1 T1
```

| Case | Autotest list | Why |
|------|---------------|-----|
| `Bug42422` | daily-devel--02 | A member must not fail a starting node on its own, the rule `lost_starting_node` keeps |
| `FailRepBeforeJoin` | none | A member crashes while X joins (962) and X must fail through `FAIL_REP`. Error 932 catches a run where X exits on the member's `DISCONNECT_REP` first, which a member's disconnect still does |
| `Bug24543` | daily-basic--11 | The president crashes in `ALLOC_NODEID_REQ` (934): node id allocation when the president fails |
| `Bug25554`, `Bug18612SR` | daily-basic--10, daily-basic--08 | Error 932 in the `DISCONNECT_REP` branch of a node that has not started |
| `Bug18612`, `Bug16007980` | -, daily-devel--04 | As above |
