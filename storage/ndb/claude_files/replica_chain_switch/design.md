# Replica chain switch during node restart

Status: steps 1 (COMMIT_STORED keeps the replica chain), 2 (DBDIH chain
clock and fragment stamps, DBTC check), 3 (rebalance in start phase 120,
old behaviour with older data nodes) and 4 (tests) implemented. The MTR
tests `ndb_rebalance_primaries` and `ndb_security` pass (2026-11-24);
the testNodeRestart test cases have not run yet.

2026-11-25: a replacement is proposed in `same_row_serialization.md`:
DBTC holds back an operation while an earlier operation of the same
transaction on the same row is in flight, and steps 1-3 are rolled back.

2026-11-26: the replacement is implemented and steps 1-3 and their tests
are reverted: COMMIT_STORED switches the primary as before, and DBTC
holds back a write while an earlier write of the same row in the same
transaction is in flight along another chain. The Problem section below
stays valid; the Design sections describe the reverted code and are kept
as history.

## Problem

`testNodeRestart -n InplaceCharPkChange*` fails now and then with 630 (tuple
already exists) on an insert while a node restarts. Each of its
transactions deletes a row and inserts the same row again, with a key that
differs only in case or trailing spaces.

DBTC asks DBDIH for the replica chain of each operation separately
(`DIGETNODES`). DBDIH changes the chain of a fragment when the copy of that
fragment to the restarting node completes (COMMIT_STORED). A transaction
whose operations on one row straddle that change sends them along two
different chains, and a later operation can overtake an earlier one on a
replica.

### Evidence

daily-devel--07 2026-11-21, `InplaceCharPkChangeCISlowCopy`, 3 data nodes,
3 replicas, node 3 restarting, transaction coordinator on node 4. The
DUMP 2368 KEYREF output on node 4:

```
KEYREF 630 from ACC LDM(5) backup tab(15,1) op 2 seqNoReplica 1
  lastReplicaNo 2 tc node 4 next node 3 ... fragStatus 1
KEYREF_AT_WIRE_ROWID tab(15,1) op 2 rowid(0,352) ... unc 0
  req '\x02x3\x00' stored '\x03X3 '
```

- The transaction deleted `x3` (which matches the stored `'X3 '` in the
  case insensitive collation) and inserted `x3`.
- The insert reached node 4 as a backup (chain 2 -> 4 -> 3) while the row
  still held `'X3 '` with no operation of the transaction on it: the
  delete had not arrived.
- Node 3 had logged the last copy operation of this fragment 0.3 s before.

The 2026-11-19 run (2 replicas) is the same with a primary switch: the
delete went to the old primary node 5 and on to node 4, the insert went
straight to node 4 as the new primary and got 630 there.

### The chain change

- STORED (copy starts): `insertBackup()` puts the starting node right after
  the primary: `[2,4]` becomes `[2,3,4]`. It must be there so that copy
  rows and live writes both reach it from the primary over one link, in
  order. The distribution key is bumped, and the copy scan's COPY_FRAGREQ
  makes the primary refuse writes with the old key.
- COMMIT_STORED (copy of the fragment completed), before step 1:
  `removeOldStoredReplica()` + `linkStoredReplica()` append the replica at
  the end of the stored list and `updateNodeInfo()` rebuilds `activeNodes`
  from it, switching in the primary the starting node calculated at the
  start of the copy (`calc_primary_replicas()`). `[2,3,4]` becomes
  `[2,4,3]` or `[3,2,4]`. The distribution key is not bumped.

### Consequences

- Delete + insert of the same row: a spurious 630 (or 626), the
  transaction aborts cleanly.
- Two updates of the same column: the overtaken replica applies them in the
  wrong order. DBTUP builds each operation on the previous one's copy of
  the row and the last arrived operation wins at commit, so that replica
  commits a different value than the primary, silently. Its REDO log has
  the same order.
- Primary switch: a locked read after a write in the same transaction can
  reach the new primary before the write and return the old row.

The same exposure exists wherever DBDIH changes a primary or the order of
existing replicas while transactions run: the graceful-stop switch
(`DIH_SWITCH_REPLICA_REQ`, `make_table_use_new_node_order()`) and the
primary recalculation at node failure (`removeNodeFromTable()`).

## Design

### Step 1: COMMIT_STORED keeps the chain (implemented)

- The starting node sends the current primary (`activeNodes[0]`) as
  `primaryNode` in `UPDATE_FRAG_STATEREQ(COMMIT_STORED)` instead of the
  primary it calculated at the start of the copy. That calculation is
  removed.
- `make_table_use_new_replica(COMMIT_STORED)` relinks the stored replicas
  in `activeNodes` order before `updateNodeInfo()`, so the rebuilt chain is
  the copy-phase chain with the new node right after the primary. Later
  `updateNodeInfo()` calls rebuild the same order.
- Mixed versions (added with step 3): keeping the chain needs the
  rebalance of step 3 to move primaries to the restarted node later, so
  it is done only when every alive data node supports that rebalance
  (`is_rebalance_primaries_supported()`, version 26.10.1). With an older
  data node in the cluster all nodes do as the older versions do:
  - the starting node decides once, in `execSTART_TOCONF()`
    (`c_takeover_keep_chain`); in a mixed cluster it sets
    `TabRecord::m_calc_primary_replicas`, calculates the new primaries
    when the copy of each table starts and sends them in COMMIT_STORED;
  - every receiver relinks the stored replicas
    (`orderStoredReplicasAsActiveNodes()`) only when all nodes support the
    rebalance, else it appends the new replica as before;
  - an older starting node always sends its calculated primary, and
    without dynamic primaries `updateNodeInfo()` switches to the preferred
    primary, so all nodes agree on the primary.

  The switch changes the chain, so the fragment gets a new stamp (step 2,
  not version gated): DBTC on nodes of this version refuses transactions
  that straddle it with 1204, older DBTCs keep the old race until the
  upgrade completes. The last restart of a rolling upgrade has only new
  nodes alive, keeps the chain and rebalances the whole cluster.
- Rare case: an older node is included (another node group) while a new
  node copies with the chain kept; receivers then append without
  relinking, the backup order changes as in the old behaviour, and step 2
  catches it on new DBTCs.

### Step 2: chain clock and fragment stamps in DBDIH, check in DBTC (implemented)

- DBDIH keeps a node-wide 64-bit logical clock, `m_chain_clock`, and each
  fragment a 64-bit stamp, `Fragmentstore::m_chain_stamp`. The clock ticks
  when the primary changes or a backup present in both chains receives
  writes from a different predecessor. The fragment gets the new clock
  value. A new fragment has stamp 0.
- `insertBackup()`, `updateNodeInfo()` and `make_table_use_new_node_order()`
  call `check_chain_change()` under the table write lock. At STORED,
  changing `[2,4]` to `[2,3,4]` stamps the fragment because node 4 now
  receives writes from node 3. Growing `[2]` to `[2,3]` preserves the stamp.
- `execDIGETNODESREQ` returns the fragment's stamp (the larger of the two
  while a reorg moves rows, DBTC writes both) and the clock in
  `DiGetNodesConf` (two words each, only for direct execution), read inside
  its read sections.
- DBTC: `ApiConnectRecord::m_chain_start_clock`, 0 until the first key
  operation that is not a committed read, which stores the clock. Each
  later such operation on a fragment whose stamp is larger refuses with
  `ZDISTRIBUTION_CHANGED_ERROR` (1204, temporary, the application retries)
  through `releaseAtErrorLab()`, which aborts the transaction, like a
  `DIGETNODES` failure. Reset in `initApiConnectRec()`. One 64-bit word per
  transaction, one comparison per operation. Scans need nothing: lock
  take-over updates go to the node holding the scan lock.
- Why it is safe: the operations of a transaction run in order in one DBTC
  thread. If it reads a fragment's chain and later a different chain of
  the same fragment, the write section that changed it ran between the two
  reads, so its tick came after the transaction took its clock, and the
  stamp is larger. The clock of the first operation is read in the same
  read section as its chain, so it never covers a change that operation
  did not see. A transaction reading the clock of a later tick from
  another table's section then sees the changed table's seqlock odd or
  advanced (wmb after the lock in the writer, rmb in the reader).
- Aborts: only transactions that use a fragment whose chain changed after
  they started; transactions that use rebalanced and not yet rebalanced
  tables are not aborted. The clock has one writer (the DBDIH thread) and
  starts at 1, so 0 means not set in DBTC; 64 bits do not wrap.

### Step 3: rebalance in start phase 120, after the restart barrier (implemented)

- The restart barrier (110) releases when every restarting node has
  recovered, so no fragment copy runs anywhere after it, and one rebalance
  covers all restarts released together. The node still reports
  "starting" during phase 120, so a Kubernetes rolling restart waits until
  the primaries are balanced. Phase 120 counts as recovered
  (`getNodeRecovered()`, start phase >= 110), so a node failure there
  takes the normal path in NDBCNTR and DBDIH.
- Signals `DIH_REBALANCE_REQ` (987) and `DIH_REBALANCE_CONF` (988), DBDIH
  to DBDIH, scope Remote, classes `DihRebalanceReq`/`DihRebalanceConf`
  in `DihSwitchReplica.hpp`. DBDIH asks for STTOR 120
  (`sendSTTORRY()`), `rebalance_start_phase()`.
- Requester (restarted node, node restart or initial node restart only;
  other start types log the step as skipped): `[NODE-START]` step 17/17
  (rebalance), sends `DIH_REBALANCE_REQ(Request)` to the master and
  continues its start phases on `DIH_REBALANCE_CONF(Done)`, or when the
  master fails. Skips when some alive node lacks support.
- Master (`rebalance_master_request()`): one run at a time. A request
  during a run joins it: the run started when no node was below the
  barrier, so the requester had completed its copy. Otherwise
  `rebalance_master_check()`:
  - not supported by all alive nodes: answer, no run;
  - The master skips a busy request during LCP master take-over, start
    inclusion, a graceful stop, a fragment take-over, or while a node is
    below the restart barrier. The requester can finish phase 120 and
    release its restart lock. If another starting node fails before
    becoming alive in DIH, the restarted node may retain zero primaries.
  - node failure handling in progress (`m_remove_node_from_table_ongoing`):
    answer, it recomputes every table.
  
  A run sends `DIH_REBALANCE_REQ(Run, runId)` to all alive DBDIHs, itself
  included, and answers the requesters when all have replied
  `RunDone`. While a run is going on the master refuses `START_PERMREQ`
  (`ZNODE_ALREADY_STARTING_ERROR`, retried every 3 s) and `STOP_PERM_REQ`
  (`NodeStartInProgress`, retried every 100 ms).
- Participant (`rebalance_tables()`): a CONTINUEB loop
  (`ZREBALANCE_TABLES`, guarded by a generation) over all `TS_ACTIVE`
  tables, about 1024 units of work per step (one per table record plus
  one per fragment, so small tables share a step). Per table
  (`rebalance_table()`): `calc_primary_replicas()` over all fragments,
  then under the table write lock `updateNodeInfo()` on each, which
  stamps the fragments whose chain changed; the computation of node
  failure handling (`removeNodeFromTable()`). A table whose description
  is in use (`tabCopyStatus != CS_IDLE`) is retried shortly. No
  `m_node_view_lock`: with stamps per fragment the table lock suffices.
- Primaries are computed locally on each node. `calc_primary_replicas()`
  writes only its `m_temp_*` counters and each fragment's `primaryNode`;
  its inputs are the fragment layout, the alive nodes and the stored
  replica order (identical on all nodes after step 1).
- Schema operations: the restarting node keeps its shared
  `NodeRestartLock` until SL_STARTED (`execNODE_START_REP`), which keeps
  schema transactions out. An exclusive DICT lock would deadlock with it.
- Node failure: `startRemoveFailedNode()` calls `rebalance_node_failed()`
  for every failed node that was alive in DBDIH. Participants stop their
  loop, the master answers the requesters (SkippedNodeFailure), a
  requester whose master failed continues. Failure handling recomputes
  the primaries of every table (`removeNodeFromTables()`). Each participant
  records the failure number and ignores a delayed `Run` issued before the
  most recent failure of an alive node, even when the master survives.
- Signal checks (`execDIH_REBALANCE_REQ/CONF`): both requests carry the
  sender's `cfailurenr`. One that overtook a node failure here (sender's
  number larger, e.g. a new master's Run before this node's NODE_FAILREP)
  is executed again after 100 ms. A Run from a node that is not the master
  with another failure number is a stale run of a failed master and is
  ignored (info line). Every signal that no correct data node sends is
  logged as an error, ignored, and its sender reported with
  `reportMaliciousSignal(VT_DIH_REBALANCE_PROTOCOL)` (Tier A, violation
  id 38), so QMGR disconnects it instead of this node crashing: a sender
  field that does not match the sending node, a Run or Done not from the
  master (same failure number), a Request to a non-master, a failure
  number more than `MAX_NDB_NODES` ahead, an unknown type or result. Debug
  builds also stop on such a signal (`ndbassert`) to catch the bug in the
  sender; production builds only disconnect it. Such a signal from this
  node itself is a bug here (`ndbrequire`).
- Version gate: `ndbd_support_rebalance_primaries()` (26.10.1) for every
  alive data node; the same gate decides whether COMMIT_STORED keeps the
  chain (step 1).
- Logs: the master logs start and completion of each run, each
  participant its tables, changed fragments and elapsed time; NDBCNTR
  logs "Phase 120 was used by DIH to rebalance the primary replicas".

### Step 4: tests (implemented)

The `testNodeRestart` cases are listed in `daily-devel--07-tests.txt`
(3 replicas on 2026-11-21) and, except the existing `InplaceCharPkChange`
ones, in `weekly-devel--04-tests.txt` (2 replicas). The autotest run
command chooses the cluster, so the tests work with any number of
replicas and node groups.

- `ndb_rebalance_primaries` (MTR, debug build, 2 data nodes): node 2
  restarts with DUMP 7037 1 and must come back primary for no fragment
  of t1. A subsequent normal restart of node 2
  must leave them balanced as soon as it has started (start phase 120
  runs before the start completes). Each check also requires both data
  nodes to see the same primaries in `ndbinfo.ndb$table_fragments_all`.
  It prints booleans only, so it does not depend on the number of
  fragments.

- `InplaceCharPkChangeCS/CI/CISlowCopy` (existing): no 630 while nodes
  restart.
- `InplaceCharPkChangeCIOldDih`: as `CISlowCopy` with all data nodes in
  DUMP 7037 mode 2, the behaviour with an older data node in the cluster:
  the primary moves to the restarted node at each COMMIT_STORED, and only
  the DBTC check (1204) keeps the delete and insert of a row on one chain.
- `ChainSwitchDoubleUpdate`: on a Read Backup table, each transaction
  updates the same rows twice in one batch (update counts u and u + 1,
  all columns), so both operations are on their way at the same time,
  while each node restarts with slow fragment copies (error 5106).
  Afterwards: all data nodes see the same primaries and each node is
  primary for its share (the rebalance of start phase 120),
  `verifyTableReplicas` reads every row through every data node (with
  Read Backup each reads its own replica), and the Hugo values are
  checked. The steps count the 1204 errors.
- `ChainSwitchDoubleUpdateOldDih`: the same with DUMP 7037 mode 2; only
  checks that all data nodes see the same primaries.
- `ChainSwitchRebalanceAtRestart` restarts each node during double updates and
  scan updates. It checks primary placement after each restart and verifies
  the replicas and Hugo values at the end.
- `ndb_security` (MTR): the catalog row of `VT_DIH_REBALANCE_PROTOCOL`.
  Injecting it with DUMP 9100 would disconnect a data node, and a real
  faulty signal stops the receiver in debug builds (`ndbassert`), so no
  injection test.

Test support:

- DUMP 7037 `<mode>` (`DihRebalanceTestMode`, error insert builds only):
  0 normal, 1 this node does not ask for a rebalance in start phase 120,
  2 this node acts as if some data node did not support the rebalance
  (set it on all nodes). A node held in NOSTART gets the DUMP before
  READ_CONFIG_REQ, so the mode is cleared in the constructor, not in
  `initData()`, and handled first in `execDUMP_STATE_ORD`; a restart
  clears it.
  `runRestarter` sets it on each restarted node when the test case sets
  `RebalanceTestMode`.
- The primaries come from ndbinfo `table_fragments_all` (in SQL
  `ndbinfo.ndb$table_fragments_all`): `current_primary` as each data node
  sees it. `table_fragments` has the rows of the master DBDIH only, so it
  cannot show whether the nodes agree; the checks also require a row from
  every data node. Each node gets exactly its share only when the
  fragments of the table divide evenly among the data nodes; otherwise
  only the agreement is checked.

## Open points

- The length of a rebalance step for a large schema (budget 1024).
- A real mixed-version run (upgrade test with a 26.10.0 data node); the
  tests above only simulate it with DUMP 7037 2.
