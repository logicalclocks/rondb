# Same-row write serialization in DBTC

Status: proposed 2026-11-25, not implemented. It replaces the approach in
`design.md` (keep the replica chain at COMMIT_STORED, chain clock and
fragment stamps with the DBTC 1204 check, rebalance in start phase 120).
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
chain as the earlier one is sent at once, as today.

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
- Reads, committed or locked, are not ordered against writes of the same
  batch: reading your own writes within a batch is documented as unsafe,
  the read returns either the old or the new row. A read sees the
  transaction's own writes of earlier batches: the API starts the next
  batch only after the previous one has completed, and DBTC confirms a
  write only after LQHKEYCONF, which the last replica sends after every
  replica has executed it (linear prepare,
  `LqhKeyReq::setNormalProtocolFlag`). So reads take no part in the wait
  and are never delayed by it.
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
writes of one row in one batch. Today's chain clock also aborts
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
- Reads of any lock mode, unlock operations and scans: not involved.

### Key, path and lifetime

- Key: table id, fragment id (from `DiGetNodesConf`) and the row hash
  `thashValue` that `hash()` computes in `tckeyreq050Lab()`. The row hash
  is taken after collation normalisation, so `x3` and `'X3 '` in a case
  insensitive collation give the same key. The fragment id is part of the
  key because reorg and fully replicated trigger operations write the
  same row to other fragments; those are other rows and must not wait for
  each other or for the main operation. Hash collisions only make a write
  wait unnecessarily.
- Path: the node list from `DiGetNodesConf` (`tcNodedata`). Within one
  fragment the LDM instance is the same in every node, so equal node
  lists mean the same LDM instances and links.
- Holder: a write from sending LQHKEYREQ until LQHKEYCONF, LQHKEYREF or
  its abort.
- A new write on a key is sent at once when the key has no waiting write
  and every holder of the key has the same path. Otherwise it waits.
  Waiters of a key are resumed first in, first out.
- A holder never waits for anything in DBTC: it has been sent and only
  LQH decides when it is answered. Fired triggers are executed only after
  the triggering operation's LQHKEYCONF (`execLQHKEYCONF`, `numFired`), so
  the key is released before any of its trigger operations is created.
  The wait therefore adds no deadlock inside DBTC; waits between
  transactions on row locks are unchanged.

### Limit on operations in flight

- A data node configuration parameter (name to be decided, e.g.
  `MaxParallelOperationsPerTransaction`) limits the operations DBTC has
  sent for one transaction and not yet got LQHKEYCONF or LQHKEYREF for.
  It counts the operations DBTC tracks to LQHKEYCONF, reads with locks
  included. Committed reads are not counted: they never report back to
  DBTC, so DBTC cannot know when they are done.
- An operation beyond the limit waits like a write on a busy key and is
  resumed when an operation of the transaction completes. Waiting
  operations hold no slot and no lock, and a slot is freed at LQHKEYCONF,
  before the operation's triggers run, so the limit cannot deadlock a
  transaction against itself.
- With the limit, the key table of a transaction never holds more than
  the limit, so it has a fixed maximum size.
- In itself the limit keeps one transaction with a very large batch from
  flooding the LDM threads and the send buffers; such a batch is sent as
  a window of the limit's size instead.
- Default to be measured: large enough that MySQL batches, `ndb_restore`
  and the REST server's batches are not slowed (a few hundred to a few
  thousand).

### Where

`tckeyreq050Lab()` is split at the call to `execDIGETNODESREQ`: the part
before it (hash, table check, `DiGetNodesReq`) and the part after it
(reorg handling, chain into `tcNodedata`, read routing,
`attrinfoDihReceivedLab()`). The key and limit check comes right after
`DiGetNodesConf`, where the 1204 check is today (the 1204 check goes). A
holder enters the key table just before LQHKEYREQ is sent
(`sendlqhkeyreq()`).

### Parking an operation

- New operation state, e.g. `OS_WAIT_SEND`. The abort and node failure
  code already has case labels for operations that have not contacted any
  LQH (`OS_WAIT_DIH`, defined but never set, `OS_WAIT_KEYINFO`,
  `OS_WAIT_ATTR`, `OS_CONNECTED` in `abort015Lab()` and the failure data
  setup); the new state joins them, plus any state names in dumps.
- DBTC has one cache record per transaction (`ApiConnectRecord::cachePtr`),
  released when LQHKEYREQ is sent (`packLqhkeyreq040Lab()`,
  `releaseAttrinfo()`). A parked operation takes it along (a field in
  `TcConnectRecord`), and the transaction's `cachePtr` becomes RNIL, so the
  next TCKEYREQ seizes a new record from the transient pool. The cache
  record keeps the KeyInfo and AttrInfo sections.
- The operation is fully received when it parks, so the transaction state
  changes as at the end of `tckeyreq050Lab()` today (`CS_RECEIVING` to
  `CS_STARTED`, `CS_REC_COMMITTING` to `CS_START_COMMITTING`).
- `lqhkeyreqrec` was already incremented in `execTCKEYREQ`, so a parked
  operation counts as outstanding: TCKEYCONF, commit and the execute-flag
  handling wait for it without other changes.
- The operation keeps the inputs of its `DiGetNodesReq`; for a reorg
  trigger operation that includes `firedFragId`, which today lives in the
  transaction only for the duration of the TCKEYREQ.
- One first-in, first-out queue of parked operations per transaction.
  When a slot or a key is released, the queue is scanned from the front:
  an operation is resumed when a slot is free and its key allows it; an
  operation that must still wait blocks later operations on the same key,
  not others. The queue is never longer than the operations the API or
  the triggers sent, and only exists while something waits.

### Resuming

- A resume is a CONTINUEB to DBTC (operation and transaction i-values plus
  the transaction id, checked on arrival; an operation aborted meanwhile
  is gone). The CONTINUEB keeps the resume out of the LQHKEYCONF context,
  its globals (`tcConnectptr`) and its recursion; waits are rare, so the
  extra signal does not matter.
- The resumed operation calls `execDIGETNODESREQ` again with the saved
  inputs and continues in the second half of `tckeyreq050Lab()`. It must
  not reuse the chain it got before parking: STORED may have inserted a
  node meanwhile (the new node would miss the write, and the primary would
  refuse the old distribution key once the copy has started), or a node in
  it may have failed. The key and limit check is done again with the new
  result; if it says wait, the operation goes back to its place at the
  front of the queue.
- If the new `DiGetNodesConf` gives another fragment (a reorg moved the
  hash meanwhile), the operation takes the new key.

### Key table

- `TcConnectRecord`: row hash, fragment id, table id, next in bucket, next
  in the transaction's wait queue. About 16 bytes per operation record.
- `ApiConnectRecord`: count of operations in flight, a few bucket heads
  inline (for example 8), and the i-value of a bucket array from a DBTC
  pool, sized from the limit and taken only when a transaction has more
  writes in flight than the inline buckets serve well. Released when the
  transaction ends.
- The path of a holder is its `tcNodedata`, already in the record.

### Abort, errors, failures

- `AbortOnError`: the first LQHKEYREF aborts the transaction; parked
  operations are released with it (cache record, sections, key table)
  and never reach LQH.
- `IgnoreError`: LQHKEYREF releases the key and the slot, and the queue
  is scanned.
- Transaction timeout, API abort, API disconnect, TC node failure
  handling for a failed data node: parked operations are in the
  transaction's operation list in a state that has contacted no LQH, so
  they are released like `OS_WAIT_ATTR` operations today. A take-over by
  another TC never sees them; the API gets the failure.
- The key table and the queue must be empty when the transaction record
  is released (ndbrequire in debug builds, reset otherwise).

### Observability and test support

- DBTC counters of writes that waited for a key and of operations that
  waited for the limit (for example in `ndbinfo.counters`, which changes
  the ndbinfo MTR results), so tests can show that the paths ran.
- An error insert in DBTC that treats every path as different, so the
  key wait runs without a chain change, and one that delays LQHKEYCONF of
  one table, so a second write of a row deterministically waits.

## Costs

- CPU: one key table lookup, insert and delete per write; a counter per
  tracked operation for the limit. Reads: nothing.
- Latency: a write waits only when a chain changed between it and an
  earlier write of the same row in the same batch; then about one round
  trip. Operations beyond the limit wait for a free slot, which is the
  intended flow control.
- Memory: about 16 bytes per `TcConnectRecord`, the inline buckets per
  `ApiConnectRecord`, a bounded bucket array for transactions with many
  writes in flight.
- No aborts from this mechanism.

## Mixed versions

Nothing goes between nodes: the wait is local to the DBTC that coordinates
the transaction, and DIGETNODES is local. No version gate. A transaction
coordinated by an older data node behaves as before this work (the race
remains for it until the upgrade completes). Since the chain clock and the
rebalance protocol are removed, there are no new signals to gate either.

## Commits on RONDB-1124-autotest-fixes

The branch is pushed, so the rollback uses `git revert` (no history
rewrite), newest first.

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
- New NDBT cases:
  - batches with two and more writes of one row (update/update,
    delete/insert, insert/update/delete) with the error insert that
    treats every path as different: results as with the batch executed in
    order, the key-wait counter increases, replicas agree
    (`verifyTableReplicas`);
  - the same with the error insert that delays LQHKEYCONF;
  - a batch larger than the limit (inserts, then updates of the same
    rows): completes, the limit counter increases;
  - a unique key swap between two rows in one batch while nodes restart;
  - double updates on a fully replicated table while nodes restart
    (trigger operations on other fragments must not wait for each other);
  - abort with parked operations: `AbortOnError` failure of the holder,
    API abort, transaction timeout, failure of a data node in the chain.
- MTR: `ndb_stored_chain` adapted; the new configuration parameter in the
  configuration tests; the counters in the ndbinfo results.

## Plan

1. Settle the open points below.
2. Revert the commits listed above (one revert commit each, newest first),
   adapt the tests that stay, re-record `ndbinfo_plans`.
3. Implement in DBTC: split `tckeyreq050Lab()`, key table, limit, park,
   CONTINUEB resume with a new DIGETNODES, release on
   LQHKEYCONF/LQHKEYREF/abort, the new state in the abort and failure
   paths, counters, error inserts, configuration parameter.
4. New tests, then the autotest suites with 2 and 3 replicas.
5. Update `design.md` and the index in `storage/ndb/CLAUDE.md`.

## Open points

1. How a reorg trigger operation repeats its DIGETNODES on resume
   (`firedFragId` saved in the operation, or a DIGETNODES variant that
   takes the fragment id); fully replicated triggers already pass their
   fragment in the TCKEYREQ.
2. Name and default of the limit, and whether it should also apply to
   scans. Committed reads stay outside it: they do not report back.
3. Pool and size of the bucket arrays.
4. Whether anything relies on the order of answers within a batch for
   different rows (an operation that waits is answered after operations
   behind it; the NDB API matches answers by operation).
