# Same-row serialization in DBTC

Status: proposed 2026-11-25; implemented 2026-11-26 on
RONDB-1124-autotest-fixes, not yet built or tested; locked reads added
the same day (see Which operations), and since 2026-11-27 ordered
against a write only when the read has the higher savepoint (see
Ordering rules). It replaces the
approach in `design.md` (keep the replica chain at COMMIT_STORED, chain
clock and fragment stamps with the DBTC 1204 check, rebalance in start
phase 120), which is reverted.
The Problem section of `design.md` (630 in `InplaceCharPkChange*`, the
evidence, the chain changes at STORED and COMMIT_STORED) stays valid.

## Idea

A replica chain change is only harmful when one transaction has two
writes of the same row in flight at the same time and they take
different chains: the later one can reach some replica before the earlier
one, so the replicas apply them in different orders. DBTC prevents that
directly: a write is not sent while an earlier write of the same
transaction on the same row (same table, fragment and row hash) is in
flight along a different chain. It waits in DBTC and is sent when the
earlier write gets LQHKEYCONF or LQHKEYREF. A write that takes the same
chain as the earlier one is sent at once, as today. A read and a write
of the row in one batch need no order: the read has the write's
savepoint and returns the row as before the batch either way. A locked
read with a higher savepoint than a write in flight (the read of an
immediate trigger) is ordered the same way: it keeps its order with the
write at once only if it goes to the write's first replica.

DBDIH may then change chains whenever it needs to, as it did before:
COMMIT_STORED switches the primary back to the restarted node fragment by
fragment, and no rebalance, chain clock or version gate is needed.

A limit on the operations a transaction has in flight bounds the memory
for this and is flow control in its own right.

## Ordering rules this relies on

- The NDB API gives no order between the operations of one batch
  (documented). The only requirement within a batch is that all replicas
  apply the writes of a row in the same order. This design keeps the
  batch order for writes of one row (waiters are resumed first in, first
  out), which is what the RonDB comment at the read routing in
  `DbtcMain.cpp` promises for rows a transaction writes itself.
- A read sees the transaction's own changes by savepoint, not by the
  order of execution. DBTC gives every operation of a batch the
  transaction's current savepoint and increases it after the operation
  with the execute flag, the last one of the batch
  (`Dbtc::execTCKEYREQ()`). DBTUP shows a read of the transaction only
  the changes of lower savepoints (`Dbtup::setup_read()`,
  `find_savepoint()`), committed reads of the transaction included. So
  every read of a batch, locked or committed, returns the row as it was
  before the batch, whether a write of the row in the batch executes
  before or after it. Writes are not affected: a write applies to the
  latest version of the row.
- The exception is the read of an immediate trigger (foreign key
  checks): it gets the savepoint of the triggering operation plus one, so
  it sees the writes of that batch that executed before it.
- Committed reads are not ordered against writes of the same batch: by
  the savepoints they return the row as before the batch (the NDB API
  documents reading your own writes within a batch as undefined).
- Locked reads are ordered against a write of the same row only when the
  read's savepoint is higher than the write's (decision 5). Otherwise the
  read does not see the write in any order. The comment at the query
  thread decision in `Dbtc::sendlqhkeyreq()` says that the metadata
  handling (`ndb_schema`) and the replication code rely on a locked read
  of a row reading the row before a following write of it in the same
  batch; the savepoints give that in any order. The order matters for
  the read of an immediate trigger, which must see a write of its batch
  that may still be in flight along another path: a foreign key check of
  a child row inserted in the same batch as its parent could otherwise
  miss the parent.
- Any read sees the transaction's own writes of earlier batches: the API
  starts the next batch only after the previous one has completed, and
  DBTC confirms a write only after LQHKEYCONF, which the last replica
  sends after every replica has executed it (linear prepare,
  `LqhKeyReq::setNormalProtocolFlag`).
- Committed reads do not report back to DBTC: there is no LQHKEYCONF for
  them, DBTC releases them when they are sent (`releaseDirtyRead()`), and
  nothing here may depend on them coming back.
- Signal order is kept between two block instances. Two writes of one
  fragment along the same chain go through the same LDM instances over
  the same links, so the later one cannot overtake the earlier one. This
  is what batches with repeated rows rely on today.

## Why it is enough

- When TC has LQHKEYCONF for a write, every replica in its chain has
  executed it. A later write sent only then cannot overtake it anywhere,
  whatever chain it gets.
- Commit order does not matter, only prepare order. DBTUP commits the
  whole operation list of a row at once, whichever operation's COMMIT
  arrives first (`DbtupCommit.cpp`, `execute_real_commit()`,
  `get_execute_commit_operation()`). COMMIT and COMPLETE of each
  operation go along the chain stored in that operation (`tcNodedata`),
  so operations with different chains commit correctly. This already
  happens today for writes of one transaction in different batches
  across a chain change.
- Different transactions are ordered by row locks in each replica: a later
  transaction's write waits in each replica until the earlier one has
  committed there, whatever chain either took. That is why switching the
  primary at COMMIT_STORED was always correct except for the case above.
- A write that waited asks DBDIH again when it is resumed (see Resuming),
  so it never goes out with a chain that STORED has extended meanwhile;
  the primary's distribution key check (the comment at the check in
  `Dblqh::execLQHKEYREQ`) stays a rare race, as today.

Covered by this, with no other mechanism:

- the 630 of `InplaceCharPkChange*` (delete and insert of one row in one
  batch, with 2 or 3 replicas);
- two updates of one row in one batch, which today can commit different
  values in different replicas (`ChainSwitchDoubleUpdate`);
- a backup that gets a new predecessor when STORED inserts the starting
  node right after the primary (`[P,B]` to `[P,N,B]`; the insert position
  is required for the copy);
- the chain changes at COMMIT_STORED, at a graceful stop
  (`make_table_use_new_node_order()`) and at node failure
  (`removeNodeFromTable()`);
- trigger operations: unique index, foreign key, reorg and fully
  replicated triggers all enter DBTC through
  `EXECUTE_DIRECT(DBTC, GSN_TCKEYREQ)` and `tckeyreq050Lab()`, the same
  path as the API's operations. An index row deleted by one trigger and
  inserted by another in the same batch is serialized like any row.

It aborts nothing and delays nothing unless a chain changed between two
writes of one row in one batch, or between a write and a trigger's read
of the row. Today's chain clock also aborts
transactions that are safe: `ndb_stored_chain` updates a row, lets STORED
insert node 3, and updates the row again in the next statement; the
second UPDATE gets 1204 (shown as lock wait timeout) although the first
one had completed on all replicas.

## Not covered (not covered today either)

- Dirty writes of different transactions: they hold no lock past their
  own execution in each replica, and the primary's distribution key check
  skips them, so a chain change can reorder two of them on the same row.
  `DisallowDirtyWrites` (95510d8ac36) remains the protection.
- Locked reads take their lock only in the primary. When the primary
  switches between a locked read and a later write of the row in another
  batch, another transaction can lock the row in the new primary first;
  the two then wait for each other until deadlock detection aborts one.
  This is the behaviour before the chain clock, correct and rare.

## Design

### Which operations

- Writes: insert, update, write, delete, refresh, including dirty writes
  inside a transaction and trigger operations. Interpreted updates are
  updates. These are holders and can be waiters.
- Locked reads: shared, exclusive and simple reads, including the reads
  of unique index lookups and foreign key checks. Holders and waiters
  against writes of the row with a lower savepoint than the read's,
  never against other reads. (`isRowOrderedOperation()` is every
  tracked operation but an unlock.)
- Committed reads, unlock operations and scans: not involved. A
  committed read never reports back; an unlock goes by its lock handle to
  the node with the lock, which the API has only after the read returned.

### Key, path and lifetime

- Key: transaction, table id, fragment id (from `DiGetNodesConf`) and the
  row hash `thashValue` that `hash()` computes in `tckeyreq050Lab()`. The
  row hash is taken after collation normalisation, so `x3` and `'X3 '` in
  a case insensitive collation give the same key. The fragment id is part
  of the key because reorg and fully replicated trigger operations write
  the same row to other fragments; those are other rows and must not wait
  for each other or for the main operation. Hash collisions only make a
  write wait unnecessarily.
- Path of a write: the node list from `DiGetNodesConf` (`tcNodedata`,
  `noOfNodes`). Within one fragment the LDM instance is the same in every
  node, so equal node lists mean the same LDM instances and links.
  `V_QUERY` is resolved to DBLQH for writes.
- Path of a locked read: the one node it is sent to (`tcNodedata[0]`,
  the primary, or a backup for a simple read that may use one), and
  whether it goes to DBLQH there (`SF_READ_VIA_LDM`): not through DBSPJ,
  and not to `V_QUERY`, which may pick a query thread when the request is
  sent. Reads in a batch with other operations go to the primary's DBLQH
  (the batch-unsafe case in `getNodesForOperation()`), so this is the
  common case.
- Order kept without waiting: two writes with the same node list; a
  locked read and a write when the read goes to the write's first
  replica's DBLQH. No order needed: a locked read and a write when the
  read's savepoint is not higher than the write's (`savePointId`, which
  every operation has), and two reads.
- Holder: a write or locked read from sending LQHKEYREQ until
  LQHKEYCONF, LQHKEYREF or its release in an abort.
- All write holders of a key have the same node list, but read holders
  need not have the same node, so a new operation is compared with every
  holder of its key in the bucket.
- A holder never waits for anything in DBTC: it has been sent and only
  LQH decides when it is answered. Fired triggers are executed only after
  the triggering operation's LQHKEYCONF (`execLQHKEYCONF`, `numFired`), and
  the key is released at the start of `execLQHKEYCONF`, before them. The
  wait therefore adds no deadlock inside DBTC; waits between transactions
  on row locks are unchanged. A locked read that waits in LQH for another
  transaction's lock does not hold back a later write of its row in the
  same batch; that write waits for the same lock in LQH.

### Limit on operations in flight

- `MaxParallelOperationsPerTransaction` (data node parameter, id 712,
  default 1024, minimum 1) limits the operations DBTC has sent for one
  transaction and not yet got LQHKEYCONF or LQHKEYREF for. It counts all
  operations except committed reads, reads with locks and unlock
  operations included: committed reads never report back to DBTC, so
  DBTC cannot know when they are done.
- Scans are not counted; they have their own limits (fragments scanned in
  parallel, batch sizes). Lock take-over operations of a scan are key
  operations and are counted.
- An operation beyond the limit waits like a write on a busy key and is
  resumed when an operation of the transaction is answered. Waiting
  operations hold no slot and no lock, and a slot is freed at LQHKEYCONF,
  before the operation's triggers run, so the limit cannot deadlock a
  transaction against itself.
- The limit is flow control: one transaction with a very large batch is
  sent as a window of the limit's size instead of flooding the LDM
  threads and the send buffers. The key table does not depend on it (see
  Key table). The default of 1024 is to be measured against MySQL
  batches (`ndb_batch_size`, 32 KB by default, 2 MB for the replica
  applier), `ndb_restore` and the REST server.

### Where

- `tckeyreq050Lab()` keeps the checks, the hash, the TTL decision and the
  `anyNode` choice, and saves the inputs of DIGETNODESREQ in the cache
  record (`m_distr_hash_value`, `m_dih_any_node`, `m_ttl_table`,
  `m_ttl_can_go_to_replica`, `m_fired_frag_id`).
- `getNodesForOperation()` is the part after it: DIGETNODESREQ, the reorg
  handling, the chain into `tcNodedata`, the read routing. It runs once
  at TCKEYREQ and again for each resumed operation.
- The key and limit check (`mustWaitToSend()`) is at the start of the
  send in `attrinfoDihReceivedLab()`, when the operation is fully received:
  for a short TCKEYREQ the last ATTRINFO comes later
  (`execATTRINFO()` calls `attrinfoDihReceivedLab()`). An operation that
  may be sent is entered there (`trackSentOperation()`): counted in the
  transaction, and a write or locked read is linked into the key table,
  except one sent alone (see below).
- On that path `attrinfoDihReceivedLab()` checks once whether the
  operation is tracked (not a committed read); `mustWaitToSend()` only
  asserts it, and the queue holds only tracked operations. If the
  transaction has no operation in flight and none waiting
  (`m_ops_in_flight | m_parked_ops` is 0), nothing can hold the operation
  back (the limit is at least 1, every holder in the key table is in
  flight), so `mustWaitToSend()` is not called. That is the first
  operation of each batch: the NDB API sends the next batch only after
  all operations of the previous one are answered. The operation is still
  entered with `trackSentOperation()`, but sent alone: it is counted and
  its key and path flags are set, but it is not linked into the key
  table; the transaction remembers it (`m_unlinked_row_op`). The next
  write or locked read checked by `mustWaitToSend()` while it is in flight
  links it first (`linkUnlinkedRowOp()`); if it is answered before that,
  `releaseSentOperation()` only forgets it. So a batch of one operation,
  the most common batch, never touches the key table.
- This is not decided from the batch having one operation
  (`TF_SINGLE_EXEC_FLAG`, the execute flag on the first operation of a
  batch). That flag is set on the transaction for the whole batch, so the
  operations that a unique index lookup or triggers send in it have it
  too and must still be checked against each other (a unique index
  trigger of an update sends a delete and an insert of the index table).
  An asynchronous API may also send the next batch before the answer, and
  that batch must be checked against the operation in flight. The
  operation's own triggers run only after its LQHKEYCONF (all
  FIRE_TRIG_ORD and the fired count from LQHKEYCONF), so they never
  overlap it.
- The DBTC 1204 check of the chain clock is gone (reverted); 1204 remains
  the error of the primary's distribution key check in DBLQH.

### Parking an operation

- State `OS_WAIT_DIH`, which was defined but never set. The abort and node
  failure code already handles it with the states of operations that have
  not contacted any LQH (`abort015Lab()`, the failure data setup).
  Operation states are not shown in ndbinfo.
- DBTC has one cache record per transaction at a time
  (`ApiConnectRecord::cachePtr`): for a long TCKEYREQ the block's
  `m_local_cache_record` (`cachePtr` ~0), for a short one a record from the
  transient pool; it is released when LQHKEYREQ is sent
  (`packLqhkeyreq040Lab()`, `releaseAttrinfo()`). A parked operation keeps
  its cache record in `TcConnectRecord::m_parked_cache`, owned while
  `SF_PARKED` is set (until `packLqhkeyreq040Lab()` releases it after the
  resume, or the operation is released); the local record
  is copied into a pooled one first. The transaction's `cachePtr` becomes
  RNIL, so its next operation is received as usual. `releaseAttrinfo()`
  now releases the record it is given and resets the transaction's
  `cachePtr` only if it is that record, since a resumed operation is sent
  while the transaction may be receiving another short TCKEYREQ.
- The transaction state has already changed when the operation parks
  (`CS_RECEIVING` to `CS_STARTED`, `CS_REC_COMMITTING` to
  `CS_START_COMMITTING`).
- `lqhkeyreqrec` was already incremented in `execTCKEYREQ`, so a parked
  operation counts as outstanding: TCKEYCONF, commit and the execute-flag
  handling wait for it without other changes.
- One first-in, first-out queue per transaction (`m_parked_head`,
  `m_parked_tail`, `TcConnectRecord::m_parked_next`, `m_parked_ops`). A new
  operation waits whenever the queue is not empty, so the operations that
  wait keep their order, whatever made the first of them wait. Committed
  reads never wait.

### Resuming

- When an operation in flight is answered and the transaction has
  operations waiting, one CONTINUEB ZRESUME_PARKED_OPS (transaction
  i-value and transaction id) is sent, from a local Signal25 since
  `execLQHKEYCONF`/`execLQHKEYREF` still use their signal data
  (`saveTriggeringOpState()`), and at most one is on its way per
  transaction (`m_resume_scheduled`).
- `resumeParkedOps()` ignores the CONTINUEB if the transaction id differs
  or the transaction is not in a state that receives or executes
  operations (aborting, committing, released). Otherwise, while the
  queue is not empty and a slot is free, the first operation asks DBDIH
  again (`getNodesForOperation()` with resume) and is checked again
  (`mustWaitToSend()` with resume); if its row is still held along
  another path it stays first, and the holder's answer resumes it.
  Otherwise it leaves the queue and is sent (`attrinfoDihReceivedLab()`
  with resumed). At most 16 operations per CONTINUEB, then another.
- The new DIGETNODES is required: STORED may have inserted a node
  meanwhile (the new node would miss the write, and the primary refuses
  the old distribution key once the copy has started), or a node in the
  chain may have failed. On resume `SOF_REORG_MOVING` is cleared before
  DIGETNODES (set again if the row still moves) and `firedFragId` of a
  reorg trigger operation is restored from the cache record. If the new
  answer gives another fragment, the operation takes the new key. If it
  says the row is not moved (reorg, `tcNodedata[0]` 0), the operation
  completes without being sent, as it would have at TCKEYREQ.
- The local read and write counters are counted at the first DIGETNODES
  only.

### Key table

- One table per DBTC instance, not per transaction: `c_row_key_hash`,
  16384 buckets of TcConnectRecord i-values (64 KB), linked through
  `m_row_key_next`/`m_row_key_prev`. The key includes the transaction, so
  other transactions only make a bucket longer. No pool, no sizing from
  the limit and nothing to release when a transaction ends.
- The bucket is computed from the transaction, table id and row hash,
  not the fragment id (which is still compared as part of the key). So
  `tckeyreq050Lab()` computes it right after `hash()` and prefetches the
  bucket for write, and DIGETNODES and the routing hide a cache miss on
  it before `trackSentOperation()` writes it. The prefetch is done for
  every operation: leaving out committed reads and unlocks would take
  branches that cost more than the prefetch. An operation sent alone does
  not use the prefetched bucket (see Where); leaving it out would take one
  more test, of `m_ops_in_flight | m_parked_ops`, which is the same at
  TCKEYREQ and at the send unless the operation waits for short
  ATTRINFO. Only the copies of a row
  that reorg and fully replicated triggers write to other fragments
  share a bucket because of the missing fragment id. LQHKEYCONF removes
  the key with little work before it, so it has no prefetch.
- The bucket is not saved in the operation: removing the first entry of
  a bucket computes it again from `apiConnect`, the table id and the row
  hash, which do not change while the key is linked (the operations of a
  transaction move to another `ApiConnectRecord` only in the commit and
  complete phases, when none is in flight).
- `TcConnectRecord`: `m_send_flags` (`SF_IN_FLIGHT`, `SF_ROW_KEY`,
  `SF_READ`, `SF_READ_VIA_LDM`, `SF_PARKED`) with the fragment id as
  `Uint16` next to it (`MAX_NDB_PARTITIONS` is at most 8160), row hash,
  table id, the two links, `m_parked_cache`, `m_parked_next`. Only
  `m_send_flags` is initialised when the record is seized (the pool
  constructs each record on seize); every other field is written before
  its flag is set and read only while it is set, so a committed read
  writes one byte more at seize and nothing else. `releaseTcCon()` tests
  only `m_send_flags != 0`, and `packLqhkeyreq040Lab()` clears
  `SF_PARKED` in the same byte. `ApiConnectRecord`: `m_ops_in_flight`,
  `m_unlinked_row_op`, the queue and `m_resume_scheduled`, reset in
  `initApiConnectRec()`.

### Abort, errors, failures

- LQHKEYCONF and LQHKEYREF release the slot and the key
  (`releaseSentOperation()`) and schedule the resume.
- `AbortOnError`: the first LQHKEYREF aborts the transaction; parked
  operations are released with it and never reach LQH.
- `IgnoreError`: LQHKEYREF releases the key and the slot, and the queue
  goes on.
- Transaction timeout, API abort, API disconnect, failure of a data node
  in a chain: parked operations are in the transaction's operation list
  in `OS_WAIT_DIH` and are released like the others. `releaseTcCon()`
  releases the slot and key of an operation released without an answer,
  and the cache record of a parked operation, and drops the queue of the
  transaction (nothing is resumed in an aborted transaction). A take-over
  by another TC never sees them; the API gets the failure.

### Observability and test support

- `ndbinfo.counters` of DBTC: `SAME_ROW_WAITS` (32), writes and locked
  reads that found an earlier operation of their row in flight along
  another path (counted at each such wait), and `PARALLEL_OP_WAITS` (33),
  operations that waited for the limit or behind other waiting
  operations.
- Error insert 8321: a write or locked read waits whenever an earlier
  write or locked read of the row is in flight (not two reads, and a
  read and a write only when the read has the higher savepoint), as if
  every path had changed. Error insert 8322: the limit is 4.

## Costs

- CPU: one key table insert and delete per write and locked read, and a
  lookup (a walk of one bucket), unless it is sent alone (the first
  operation of its batch) and answered before another one is checked; a
  counter per tracked operation for the limit; the saved DIGETNODESREQ
  inputs. A batch of one operation does not touch the key table.
  Committed reads nothing.
- Latency: a write waits only when the chain changed between it and an
  earlier write of the same row in the same batch; a trigger's locked
  read only when a write of its row with a lower savepoint is in flight
  along another path; then about one round trip. Operations beyond the limit wait for
  a free slot, which is the intended flow control.
- Memory: about 28 bytes per `TcConnectRecord`, 24 per `ApiConnectRecord`,
  12 per cache record, 64 KB of buckets per DBTC instance, and one cache
  record per waiting operation.
- No aborts from this mechanism.

## Mixed versions

Nothing goes between nodes: the wait is local to the DBTC that coordinates
the transaction, and DIGETNODES is local. No version gate. A transaction
coordinated by an older data node behaves as before this work (the race
remains for it until the upgrade completes). Since the chain clock and the
rebalance protocol are removed, there are no new signals to gate either.

## Commits on RONDB-1124-autotest-fixes

The branch is pushed, so the rollback uses `git revert` (no history
rewrite), newest first. Done 2026-11-26 as 2071c782072 (217dc8322b9),
91f3ee4fcb2 (1b1a10e7a91), e7f57fba527 (7b6889d9c00), 38b89343ccd (the
part of 051016ad56d), b575e711c75 (0a43454a422), b10c6ce04e8
(8e4607d4378) and 956b382d306 (eccea74b295); `ndb_stored_chain` adapted
in 3fce60b6307. The reverts leave `design.md` and the index as they were;
DBDIH, DBLQH and the shared headers are as before eccea74b295.

### Roll back

| Commit | What it did | Why it goes |
|---|---|---|
| 217dc8322b9 | DBDIH: ignore rebalance runs overtaken by an alive-node failure | rebalance protocol removed |
| 1b1a10e7a91 | DBDIH: keep primary rebalancing within node restart (drops the pending retry and DUMP 7036) | rebalance protocol removed |
| 7b6889d9c00 | DBDIH: stamp a fragment when a backup changes predecessor (also a DBLQH comment) | stamps removed; the case is covered by serialization. Restore or rewrite the DBLQH comment |
| 0a43454a422 | DBDIH: rebalance in start phase 120 (DIH_REBALANCE_REQ/CONF 987/988, `ndbd_support_rebalance_primaries`, VT_DIH_REBALANCE_PROTOCOL 38, NODE-START step 17, NDBCNTR phase 120 use, DUMP 7036) | not needed when COMMIT_STORED switches the primary again |
| 8e4607d4378 | DBDIH, DBTC: chain clock, fragment stamps, `DiGetNodesConf` clock words, DBTC 1204 check, `m_chain_start_clock` | replaced by the wait in DBTC; the new check goes where the 1204 check was |
| eccea74b295 | DBDIH: keep the replica chain at COMMIT_STORED (`orderStoredReplicasAsActiveNodes()`, removal of `m_calc_primary_replicas`) | restores the switch of the primary at COMMIT_STORED and the primary calculation at the start of each table's copy |

Notes:

- `ndbinfo_plans.result` was re-recorded in 0a43454a422 together with
  changes from other commits; re-record it after the revert instead of
  reverting the file.
- Violation id 38 and GSNs 987/988 are freed. If any build with them has
  left the branch, keep 38 reserved in `ViolationType.hpp` instead.
- 1204 remains: it is the existing error of the primary's distribution
  key check ("Temporary failure, distribution changed").

### Roll back in part: 051016ad56d (tests for stable replica chains)

Remove:

- DUMP 7037 (`DihRebalanceTestMode`) in `DumpStateOrd.hpp`, `Dbdih.hpp`,
  `DbdihInit.cpp`, `DbdihMain.cpp`, and `RebalanceTestMode` in
  `runRestarter`, `setRebalanceTestMode`, `runRebalanceTestModeOn/Off`;
- `InplaceCharPkChangeCIOldDih`, `ChainSwitchDoubleUpdateOldDih`,
  `ChainSwitchRebalanceAtRestart` (renamed from
  `ChainSwitchRebalanceOnDemand` in 1b1a10e7a91) and
  `runRebalanceAtRestart`, with their lines in `daily-devel--07-tests.txt`
  and `weekly-devel--04-tests.txt`;
- MTR `ndb_rebalance_primaries`;
- Test 4 of `ndb_security` (the catalog row of violation 38).

Keep:

- `ChainSwitchDoubleUpdate` and its helpers (`runCreateReadBackupTable`,
  `runPkDoubleUpdateUntilStopped`, `readTablePrimaries`,
  `waitTablePrimaries`, `runCheckTablePrimaries`, `runVerifyTableReplicas`)
  and its lines in both suites. With the primary switched at COMMIT_STORED
  the balance check still holds after the restarts. The 1204 count it
  prints becomes the distribution key race only.

### Keep and adapt: b953d013480 (`ndb_stored_chain`)

The scenario stays useful: a transaction updates a row, STORED inserts
node 3 right after the primary, and the transaction updates the row again.
The second UPDATE now succeeds instead of failing with 1204; the expected
result changes, and the test should commit and check the value through
every replica.

### Keep unchanged

| Commit | What |
|---|---|
| ebd562b74f1 | the design note; `design.md` keeps the problem description, its design sections become history (see its status line) |
| e5b70f2e5e0 | NdbApi: free a partly received long row with `delete[]` (unrelated) |
| 42d63f44634 | NdbApi: atomic node failure completion flag (unrelated) |
| 95510d8ac36 | DBTC: `DisallowDirtyWrites` (dirty writes are not covered by serialization) |
| 0542e0eff13, 78f6346d7fc | DBLQH scan batch fix, debug prints (unrelated) |
| earlier | `InplaceCharPkChangeCS/CI/CISlowCopy`, error 5106 slow copy, NrKeyDebug and the other DUMP-enabled debug output |

## Tests

- Existing, must pass without 630 or replica differences:
  `InplaceCharPkChangeCS/CI/CISlowCopy`, `ChainSwitchDoubleUpdate`, with
  2 and 3 replicas (daily-devel--07 and weekly-devel--04).
- New in testNodeRestart, in both suites:
  - `SameRowWriteWait`: batches with update/update, delete/insert,
    exclusive read/update, update/shared read and insert/update/delete of
    the same rows with error insert 8321; the writes apply in batch
    order (the later writes wait), both reads return the row as it was
    before the batch and do not wait (all operations of a batch have the
    same savepoint, see Ordering rules), `SAME_ROW_WAITS` increases, all
    replicas agree (`verifyTableReplicas` on a Read Backup table).
  - `ParallelOperationLimit`: the same with error insert 8322;
    `PARALLEL_OP_WAITS` increases.
  - `WaitingOperationAbort`: with 8322, a transaction whose first four
    updates wait for row locks of another transaction times out (266)
    with 16 operations waiting; an `AbortOnError` batch fails on its first
    operation (630) with operations waiting; an `IgnoreError` batch goes
    on after the error and commits. Nothing of the aborted ones remains.
- MTR: `ndb_stored_chain` (second UPDATE after STORED inserted node 3
  succeeds, the row is read through every replica), `ndbinfo` (the two
  counter names).
- Not written yet: a unique key swap between two rows in one batch while
  nodes restart; double updates on a fully replicated table while nodes
  restart; `MaxParallelOperationsPerTransaction` in the configuration
  tests.

## Plan

1. Settle the open points (done 2026-11-26, see Settled points).
2. Revert the commits listed above, one revert commit each, newest first;
   adapt the tests that stay (done 2026-11-26, local commits).
3. Implement in DBTC (done 2026-11-26, not built).
4. New tests (done 2026-11-26, see Tests), then the autotest suites with
   2 and 3 replicas.
5. Update `design.md` and the index in `storage/ndb/CLAUDE.md` (done).
6. Re-record `ndbinfo_plans` (error messages of DisallowDirtyWrites, the
   counters estimate of 13 rows per DBTC instance).

## Settled points (2026-11-26)

1. Reorg trigger operations on resume: `firedFragId` is saved in the cache
   record at TCKEYREQ and restored into the transaction for the repeated
   DIGETNODES, where the reorg handling reads it and resets it as before.
   Fully replicated triggers pass their fragment in the TCKEYREQ (saved
   `anyNode` 3).
2. The limit: `MaxParallelOperationsPerTransaction`, default 1024,
   minimum 1, not for scans; committed reads stay outside it.
3. The key table: one hash table per DBTC instance with the transaction
   in the key, 16384 buckets, so no pool, no per-transaction buckets and
   no dependency on the limit.
4. Order of answers within a batch: nothing relies on it. The NDB API
   matches each TCKEYCONF entry by the operation's receiver and counts
   completed operations (`NdbTransaction::receiveTCKEYCONF()`), and answers
   for rows in different fragments already come back in any order.

5. Locked reads are ordered against writes of the row too (was the open
   point: `ndb_schema` and replication rely on a locked read and a
   following write of a row in one batch executing in that order, which
   a primary change between them could break; the chain clock refused it
   with 1204). See Which operations and Key, path and lifetime. Changed
   2026-11-27 after weekly-devel--04, whose test expected a locked read
   after an update in the same batch to see the update: within a batch
   the savepoints make a read return the row as before the batch in any
   order (Ordering rules), so a read and a write are ordered only when
   the read has the higher savepoint, the read of an immediate trigger.

Also decided while implementing: the existing unused state `OS_WAIT_DIH`
instead of a new one; one strict first-in, first-out queue (a later
operation also waits behind an operation that waits for its row, which
is rare); one CONTINUEB per transaction instead of one per operation;
`attrinfoDihReceivedLab()` checks once whether an operation is tracked
and skips `mustWaitToSend()` when the transaction has nothing in flight
or waiting (see Where); such an operation is linked into the key table
only when another one is checked while it is in flight, instead of
skipping a batch of one operation by `TF_SINGLE_EXEC_FLAG`, which index
and trigger operations of the batch share (see Where).
