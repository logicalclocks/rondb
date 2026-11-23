# Replica chain switch during node restart

Status: step 1 implemented (COMMIT_STORED keeps the replica chain). Steps
2 to 4 are designed, not implemented.

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
- Mixed versions: an old starting node still sends a new primary, and an
  old cluster without dynamic primaries switches to the preferred primary.
  `updateNodeInfo()` still switches in whatever primary the signal or the
  preferred primary gives, so all nodes agree on the primary; only the
  backup order can differ between nodes, which is harmless.
- Cost until step 3: a restarted node is primary for no fragment.

### Step 2: distribution counter in DBDIH, check in DBTC

- DBDIH keeps one node-wide 32-bit counter, bumped whenever the primary or
  the order of existing replicas of a fragment changes. Adding a replica at
  STORED and removing a failed node do not need it (the distribution key
  covers the first, DBTC aborts transactions with operations on a failed
  node).
- `execDIGETNODESREQ` returns the counter in `DiGetNodesConf`, read inside
  the existing retry loop over the table lock and `m_node_view_lock`, so a
  reader sees chain and counter together.
- DBTC: one word in the transaction record, set by the first key operation
  that is not a committed read. Each later such operation compares it with
  the counter `DIGETNODES` returned; on a mismatch it refuses the operation
  with 1204 (temporary, the application retries). One word per transaction,
  one comparison per operation. Scans need nothing: lock take-over updates
  go to the node holding the scan lock.
- The node failure path (`removeNodeFromTable()`, under the table write
  lock) and the graceful-stop switch bump the counter too.

### Step 3: rebalance in a start phase after the restart barrier (110)

- The restart barrier releases when every restarting node has recovered,
  so no fragment copy runs anywhere after it, and one rebalance covers all
  restarts released together. The node still reports "starting" during the
  phase, so a Kubernetes rolling restart waits until primaries are
  balanced.
- Flow: in the new phase (e.g. 120, to check against the phase lists) the
  DBDIH of each restarting node asks the master. The master combines
  requests that arrive together into one run and sends `DIH_REBALANCE_REQ`
  to all live DBDIHs. Each runs a CONTINUEB loop: per step, with
  `m_node_view_lock` held, as many tables as fit a fragment budget (small
  tables share a step), for each table the table write lock,
  `calc_primary_replicas()` over all fragments and `updateNodeInfo()` on
  each, and one counter bump per step. Then it replies; the master answers
  the requesters, which continue their start phases.
- Primaries are computed locally on each node. `calc_primary_replicas()`
  writes only its `m_temp_*` counters and each fragment's `primaryNode`;
  its inputs are the fragment layout, the alive nodes and the stored
  replica order (identical on all nodes after step 1).
- Schema operations: the restarting node keeps its shared
  `NodeRestartLock` until SL_STARTED (`execNODE_START_REP`), which keeps
  schema transactions out. An exclusive DICT lock would deadlock with it.
- New node starts: while a rebalance runs, the master refuses
  `START_PERMREQ` with `ZNODE_ALREADY_STARTING_ERROR` (the starter retries
  every 3 s). Distribution info is copied only after permission. If a
  start was already permitted when a request arrives, the master answers
  without a run; that node's own phase rebalances later.
- Node or master failure: every DBDIH stops its loop. Failure handling
  recomputes the primaries of every table (`removeNodeFromTables()`), which
  is the same computation, so the requester treats the phase as done.
- Only node restarts and initial node restarts; system restart computes
  primaries itself. The step appears in the `[NODE-START]` log.
- Version gate: only when all nodes support it, like
  `is_dynamic_primary_replicas_supported()`.

### Step 4: tests

- `InplaceCharPkChange*` with 2 and 3 replicas: no 630.
- A test where each transaction updates the same column twice across node
  restarts, then checks that the replicas agree.
- A DUMP that triggers the rebalance on demand, to run load across it.

## Open points

- The length of a rebalance step for a large schema; DBTC threads retry
  `DIGETNODES` while `m_node_view_lock` is held.
- The phase number after 110 and the `[NODE-START]` step.
