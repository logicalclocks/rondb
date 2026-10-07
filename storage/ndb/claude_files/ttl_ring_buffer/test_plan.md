# TTL on ring buffer tables: test plan and coverage map (v2, 2026-09-22)

Companion to `design.md` (v1.0). Section 11 of the design lists the tests written with
the v1 implementation; this document maps the remaining risk areas (handover gap list
A-G) to tests, records what was added in the second pass, and states what is deferred
and why. Status markers: DONE (written and passing), WRITTEN (written, needs a build or
a run), DEFERRED (reason given), NO TEST (decision given).

## 1. Test inventory

MTR suite `ndb_ring_buffer` (2 data nodes, 2 mysqlds):
- `create.test` Case 17, `alter.test` Case 7: DDL rules (v1).
- `ttl_ring_buffer.test`: single-row DML semantics, meta row maximum, wrap over expired
  slots, prefix DELETE, ttl_index (v1).
- `ttl_ring_buffer_dml.test` (v2): multi-row INSERT, LOAD DATA, INSERT ... SELECT, unique
  index with an expired owner, scan UPDATE, NULL TTL values, DELETE forms and
  `ttl_expired_rows_visible_in_delete`, transactions, TRUNCATE, OPTIMIZE.
- `ttl_ring_buffer_ddl.test` (v2): CREATE ... LIKE / AS SELECT, RENAME TABLE, modifier-only
  comment (re-injection), TTL column rename guard, TTL seconds change (visibility only),
  copying ALTER with expired rows, ordered index on a user column.
- `node_restart.test` Case 6, `backup_restore.test` Case 12: restarts with logical holes (v1).
- `ndbapi_ring_buffer_test.test` -> `ndbapi_ring_buffer_test.cpp` Tests 28, 33-36.
- `clusterj_ring_buffer.test` -> `RingBufferTest.testTtlRing` (TIMESTAMP TTL column).

MTR suite `ndb_ttl_purge` (2 data nodes, 2 mysqlds, rdrs2 purger):
- `ttl_purge_ring_buffer.test`: reclaim with index scan and table scan (v1); v2 adds
  precision columns (TIMESTAMP(3), DATETIME(6)), NULL TTL rows, unique value reuse after
  the purge, blob parts of purged rows.
- `ttl_purge_ring_buffer_concurrent.test` (v2): writers racing the purge on the same
  prefixes: single-row inserts, 10-row insert transactions with deadlock-timeout retry,
  prefix DELETE plus immediate re-INSERT; invariants after the purge settles.
- `ttl_purge_ring_buffer_restart.test` (v2): node restart, initial node restart and system
  restart with purged holes below next_pos; refill over the holes; purge after the
  system restart.
- `ttl_purge_ring_buffer_expiry.test` (v3): rows that expire while the test runs (the other
  tests insert rows that are expired already or live for the whole test). Case 1: every read
  path (table scan, prefix scan, PK read, ttl_index scan, COUNT(*), second mysqld) switches
  together, the meta row does not change, the next insert goes to next_pos. Case 2: a
  transaction overwrites a slot that expired after it locked the meta row. Case 3: rows a
  transaction locked stay visible, updatable (a TTL column update revives them) and
  deletable to it after they expire; unlocked expired rows are hidden from it, and a prefix
  DELETE without the locks removes only the meta row. Case 4: purge and writers on slots
  that have just expired. Case 5: TIMESTAMP(3)/DATETIME(6) rows; the purge reclaims the
  early rows and not a row due later.

MTR suite `ndb_rpl`: `ndb_rpl_ring_buffer.test` Case 14 (v1).

## 2. Gap list A-G mapped to tests

A. Purge x writers, concurrency
- A1 concurrent inserts vs purge on one prefix: DONE `ttl_purge_ring_buffer_concurrent`
  phases 1 and 2 (single-row and 10-row transactions; the retry loop absorbs the
  deadlock timeout that design section 6 predicts).
- A2 purge while a prefix is deleted / rewritten: DONE phase 3 (prefix DELETE of 10
  prefixes under the purge, re-INSERT while the purge may hold the leftover slot).
- A3 many prefixes / partitions / active window / batch sizes: DEFERRED. The purge
  machinery (sharding, window, batches) is table-type agnostic and covered by the
  existing purge suite; the ring adds nothing to those paths. Revisit if a customer
  configuration differs.
- A4 TTL column precision: DONE `ttl_purge_ring_buffer` test_precision_columns (handler
  writer, purge) and DONE `ndbapi_ring_buffer_test` Test 36 (a) (NdbRingBufferWriter,
  TIMESTAMP(3)). ClusterJ: the packing code is shared for all precisions (fraction bytes
  zero); no separate test.
- A5 NULL TTL values, TTL=0: DONE `ttl_ring_buffer_dml` Cases 5-6 and
  `ttl_purge_ring_buffer` test_null_ttl_rows. TTL=0 is not a ring-specific rule; no test.
- A6 ALTER TTL seconds while purging, DROP + recreate: DEFERRED. The seconds change is
  a DICT metadata update the purger re-reads per round; `ttl_ring_buffer_ddl` Case 6
  pins the visibility effect without the purger.
- A7 blob columns and unique index under purge: DONE `ttl_purge_ring_buffer`
  test_blob_column and test_unique_value_reuse_after_purge.

B. Recovery with physical holes
- B1 node restart, initial node restart, system restart after the purge: DONE
  `ttl_purge_ring_buffer_restart` Cases 1-3, plus refill and purge after the restart.
- B2 LCP / restore row counting with refilled holes: DEFERRED. The 2352 history
  (`ttl_node_restart_rowcount.test`) concerns partial LCP part-pairs of relocated rows,
  not ring tables; a ring variant needs the same 24-round error-insert choreography.
  Candidate for a later pass if the base test is ever generalised.
- B3 ndb_restore of a backup with holes, --promote-attributes on a TTL ring: DEFERRED.
  `backup_restore.test` Case 12 covers a TTL ring with a logical hole; the restore path
  copies rows verbatim and does not consult ring_meta.

C. Replication of the purge
- C1 purge deletes replicating: DEFERRED. `ndb_ttl_rpl` has no rdrs2 and the SQL layer
  cannot delete a single expired ring row (DELETE with ring_idx is blocked;
  `ttl_expired_rows_visible_in_delete` deletes whole prefixes). Options: add an rdrs2 to
  the `ndb_ttl_rpl` my.cnf, or give the NDB API example a purge-simulator mode. The
  replication semantics are those of any TTL table: the applier's delete of an already
  expired replica row is a swallowed 626 and the replica's own purger reclaims it.
- C2 replica clock difference: same as any TTL table (`ttl_rpl_expired_apply`).

D. Writers and SQL DML
- D1 handler paths: DONE `ttl_ring_buffer_dml` (multi-row INSERT, LOAD DATA, INSERT ...
  SELECT, INSERT with a unique dup, scan UPDATE, prefix and IN DELETEs, TRUNCATE,
  transactions, OPTIMIZE). REPLACE and INSERT ... ON DUPLICATE KEY UPDATE are rejected on
  every ring table (`insert.test` Cases 8-9); DELETE by ring_idx is blocked
  (`update_delete.test` Case 12), so "DELETE by PK of an expired row" does not exist.
  A DELETE must name its prefixes with = or IN (`update_delete.test` Case 40); it locks
  their meta rows first (`concurrent.test` Case 11).
- D2 NdbRingBufferWriter with a record lacking the TTL column, explicit TTL values:
  DONE Test 36 (b) (record without the TTL column: meta TTL column NULL, K1 keeps it
  live, a later SQL insert sets the maximum). Explicit TTL values per row are ordinary
  column values; Test 34 and Test 35 already insert past values through SQL.
- D3 ClusterJ DynamicObject, DATETIME TTL column, concurrent ClusterJ writers: DEFERRED.
  The Java DATETIME packing constant (FE F3 FF 7E FB, sign bit + 9999-12-31 23:59:59)
  was verified by hand against NdbSqlUtil::pack_datetime2; a ClusterJ DATETIME model
  class is the next cheapest addition if wanted.

E. DDL on TTL rings
- E1 copying ALTER with expired rows: DONE `ttl_ring_buffer_ddl` Case 7 (expired rows
  dropped by the copy scan, meta row copied as is, ring continues).
- E2 ADD/DROP INDEX on the TTL column with expired rows: DONE `ttl_ring_buffer` Case 7;
  user column index: `ttl_ring_buffer_ddl` Case 8.
- E3 other modifiers, RENAME TABLE, TTL column rename: DONE `ttl_ring_buffer_ddl`
  Cases 3-5.
- E4 raw NDB API DDL: DONE Test 28 (FR + ring rejected, enable/disable TTL 741). Disk
  TTL column and ring-size change on a TTL ring follow the existing per-feature rules.

F. Kernel edges (K1/K2)
- F1 only-expired delete of the meta row: DONE Test 35 step (d) (626, meta untouched).
- F2 OO_TTL_IGNORE + OO_TTL_ONLY_EXPIRED delete of a live ring row passes the guard:
  CLOSED at the NDB API (review fix): the option pair is rejected with 4360 on key
  operations and on scans; Test 35 step (e) covers a live row and the meta row. The
  kernel guard is unchanged (the purger inherits its flags from the scan).
- F3 zero/NULL meta TTL column under index scan and purge: PARTLY. Test 34 (PK read,
  writer update) and Test 36 (b) (NULL, then repaired by the next SQL insert). Under
  the purge the meta row is live by K1 whatever the column holds; the index entry of a
  NULL/zero value is one wasted candidate per round (design 5.3), no correctness effect.
- F4 only-expired ordered-index scan through the API: DEFERRED (the purger is the only
  user; covered end to end by the purge tests).

G. Observability / upgrade
- G1 REST endpoints for ring tables: the purge tests read /metrics; the per-table
  endpoint reports the same counters for any TTL table. No ring-specific field exists.
- G2 mixed-version gate messages: cannot run here (single binary). Documented (D10).

## 3. Expected outcomes pinned by the new tests (for review)

- A prefix DELETE reports visible rows + 1 (meta row) and leaves expired rows physically;
  a fully expired prefix reports 1; with `ttl_expired_rows_visible_in_delete = 1` the
  expired rows go too. TRUNCATE removes everything.
- A copying ALTER drops expired rows (they are not in the copy scan) and copies the meta
  row unchanged, so `count` may exceed the physical rows; the ring continues at next_pos.
- Changing the TTL seconds changes visibility only; a longer TTL brings expired rows
  that are still present back, a purged row cannot come back.
- A unique value owned by an expired row is blocked for other primary keys until the
  ring overwrites the slot or the purge reclaims the row.
- Under a concurrent purge the final state of a prefix is exactly what the writers
  wrote: live rows in the slots they took, next_pos / count / total_inserts as if the
  purge had not run, every expired row gone.
- Restarts of any kind preserve holes, live rows and meta rows on both replicas.

## 4. Runs (2026-09-22, rondb-bin built 14:53 from this tree)

- Full regression `--suite=ndb_ring_buffer,ndb_ttl_purge,ndb_ttl,ndb_ttl_rpl --parallel=3`: all 98
  tests passed, 7 skipped (debug-only builds / have_debug_sync). 64 min wall clock.
- `ndb_ring_buffer.ttl_ring_buffer_dml`, `ndb_ring_buffer.ttl_ring_buffer_ddl`: recorded with
  `--record`, every recorded value audited against the expectation written in the test
  comments (visible sets, ring_meta HEX, affected rows, physical row counts, the
  ER_DUP_ENTRY on the expired unique owner, the copying ALTER dropping the expired row and
  keeping the meta row, `ttl_expired_rows_visible_in_delete` removing expired ring rows).
  No deviation.
- `ndb_ttl_purge.ttl_purge_ring_buffer` (6 sections): PASS, recorded and audited (meta maxima
  '2038-01-19 03:14:07.000' / '9999-12-31 23:59:59.000000', NULL rows kept, unique value free
  after the purge, blob parts 4x -> 1x -> 1x). Two probe corrections on the way, both test-side:
  `ndb$frag_mem_use.rows` lags behind commits, and `memory_per_fragment.parent_fq_name` alone
  also matches the ordered indexes; the blob-part probe now uses the exact `fixed_elem_count` of
  the NDB$BLOB table.
- `ndb_ttl_purge.ttl_purge_ring_buffer_concurrent`: PASS (26 s). All 20 prefixes end with
  exactly the writers' rows and identical ring_meta after each phase (total_inserts 65 / 120 /
  160, next_pos 16 / 21 / 11, count 50; deleted prefix 1 restarts at total_inserts 1), every
  expired row reclaimed (physical 320 / 220 / 112). No lost live row, no resurrected expired
  row, no skipped slot. The 10-row transactions completed (a deadlock-timeout retry, if any,
  leaves no trace: the meta values are exactly one statement per prefix).
- `ndb_ttl_purge.ttl_purge_ring_buffer_restart`: PASS (133 s), recorded. Node
  restart, initial node restart and system restart keep the 8 holes, the 6 live rows and the 4
  meta rows identical on both replicas (10 physical rows per node); the rings refill over the
  holes (p1 slots 1-3, p2 slots 1-2, p4 slot 5) with the expected meta values; the purge
  reclaims new expired rows after the system restart (REST server reconnected). Two test-side
  fixes on the way: `have_multi_ndb.inc` for the server2 connection, `--remove_file
  $NDB_TOOLS_OUTPUT` for MTR's check-testcase.
- NDB API example Test 35 step (d) and Test 36: PASS after zhao's build (19:21),
  `ndb_ring_buffer.ndbapi_ring_buffer_test` 36 tests passed. Test 36 (b) shows the writer
  accepts an NdbRecord without the TTL column: the meta row's TTL column stays NULL and the
  next SQL insert on the prefix sets the maximum.

## 5. Review round (Codex, 2026-09-22) and the fixes

Seven findings; five were defects (three pre-existing in the ring buffer feature, made
worse by TTL), one a documented bypass now closed, one a release decision:
- Sparse write masks (High): an INSERT that omits a column kept the overwritten slot's
  value on a wrap (plain rings too); on a TTL ring an omitted TTL column inherited the
  expired value and the new row vanished (reproduced: ring of 1, expired slot, INSERT
  without ts -> 0 visible rows). Fix: the handler writes every stored column; the NDB API
  and ClusterJ writers require the TTL column (4359 / ClusterJUserException). Tests:
  `ttl_ring_buffer_dml` Case 10, NDB API Test 36 (b)(c), ClusterJ testTtlRing.
- NdbRingBufferWriter column counts (High): counts from the table, arrays filled from the
  record -> uninitialised entries with a partial record. Fix: counts follow the record, a
  missing PK column is rejected (4118).
- Replication with ndb_log_update_as_write = 0 (High): a wrap is an UPDATE event; a
  replica that purged the slot would drop it. Fix: ring tables are logged as write row
  events. Test: `ndb_rpl_ring_buffer` Case 17 (replica-side purge simulated with an
  unlogged delete).
- NDB column number used as MySQL field index (Medium): a virtual generated column before
  `ring_idx` broke every INSERT (reproduced: "Cannot specify ring_idx column"). Fix: all
  ring/TTL column lookups go through `Ndb_table_map::get_field_for_column`. Test:
  `ttl_ring_buffer_ddl` Case 9.
- Batched meta path without the TTL maximum (Low): reproduced (NULL, or zero for a NOT NULL
  column). Fix: shared helper. Test: `ttl_ring_buffer_dml` Case 11.
- Option pair OO_TTL_IGNORE + OO_TTL_ONLY_EXPIRED (Low): rejected at the API (4360), key
  operations and scans. Test: NDB API Test 35 (e).
- Gate value 26.5.0 (Medium): unchanged; the plain ring gate has the same property. The
  release owner decides the first release that carries the code (design D10).
Codex delta review (same session): 5 of 7 addressed; two configuration cases remained and
were closed in a third pass: (1) a ring table with a conflict function kept UPDATE events
-> ring tables are now logged as writes unconditionally; (2) under `ndb_log_updated_only`
a sparse NDB API / ClusterJ write cannot be rebuilt on a purged replica -> on TTL ring
tables both writers require every column (record and mask; 4359 / ClusterJUserException;
Test 36 (b)(c), ClusterJ negative case). Left open: the gate value (release owner) and
the .result files of the changed tests (need a build).
A third Codex pass on those changes ran out of ChatGPT quota after five minutes (retry
possible after 2026-09-23 01:12); its captured mid-flight note was acted on: ClusterJ
cannot write BLOB/TEXT columns on ring tables, so the every-column rule exempts them there
(the C++ writer keeps them mandatory: blob columns are set through their handles).
Runs after the fixes (2026-09-23, rondb-bin rebuilt 22:18 the day before):
- `ndb_ring_buffer.ttl_ring_buffer_dml` (Cases 10-11) and `.ttl_ring_buffer_ddl` (Case 9): recorded
  and audited. Case 10: the wrap writes `note = 'dflt'` and a NULL timestamp for the omitted
  columns (the row is visible); multi-row INSERT and LOAD DATA behave the same. Case 11: meta
  TTL column '2038-01-19 03:14:07' / '9999-12-31 23:59:59' after multi-row INSERTs, nullable and
  NOT NULL. Case 9: the table with a virtual column before `ring_idx` inserts, wraps (meta
  next_pos 2, count 3, total_inserts 4, TTL maximum), updates and deletes (4 affected rows).
- `ndb_ring_buffer.ndbapi_ring_buffer_test`: PASS, 36 tests including Test 35 (e) (4360 on a live
  row and on the meta row) and Test 36 (b)(c) (4359 for a record without ts, a mask without ts, a
  mask without event_data; nothing written). `.clusterj_ring_buffer`: PASS with the negative case.
- `ndb_rpl.ndb_rpl_ring_buffer` (Case 17): recorded and audited. With `ndb_log_update_as_write =
  0`, after the replica's local purge of the only slot and its meta row, the source's wrap
  re-creates both on the replica: source and replica show slot 1 = 'wrap_2' and meta
  0100000001000000010000000000000002... (next_pos 1, count 1, total_inserts 2).
- Full regression `ndb_ring_buffer,ndb_ttl_purge,ndb_ttl,ndb_ttl_rpl,ndb_rpl --parallel=4`:
  219 of 221 passed (skips are debug-only tests). The two failures:
  - `ndb_ttl_purge.ttl_purge_ring_buffer_restart`: a race in the test, not the product: the
    pre-purge probe ('20 physical rows') ran after the purger had already reclaimed four
    rows. Fixed in this test and, pre-emptively, in every section of `ttl_purge_ring_buffer`
    by disabling the purger while seeding and re-enabling it with `ttl_purge_reset_config.inc`
    right before the wait (the pattern of the concurrent test). Both re-recorded and
    audited afterwards: PASS, same values as before.
  - `ndb_rpl.ndb_rpl_3site_no_log_updates`: fails standalone too (4 of 4 attempts), before and
    independently of this patch's code paths: the test reads `Relay_Log_File` from SHOW REPLICA
    STATUS right after START REPLICA and gets `cluster2-relay-bin.000001`, while the IO thread
    then creates `.000002` (Rotate to `cluster1-bin.000001;pos=4`) and all events land there.
    The relayed events themselves are exactly the expected sequence (Table_map t1, Table_map
    ndb_apply_status, Write_rows, Write_rows, COMMIT) on a plain table without ring or TTL,
    and the injector change only concerns TE_UPDATE on ring buffer tables. No earlier full
    `ndb_rpl` run exists on this branch to serve as a baseline; a run on the base commit would
    settle whether it is a pre-existing failure of the branch or of the environment.

## 6. Expiry while the test runs (2026-09-28)

`ndb_ttl_purge.ttl_purge_ring_buffer_expiry` added (section 1). TTL 5 s, crossing rows
inserted with ttl_col = NOW() + 10 s; the checks after the expiry wait with wait_condition.
Recorded and audited on the 2026-09-22 22:18 build, PASS twice (85 s, 84 s); rdrs2 log free
of purge errors. No product defect found. Case 4 cannot show whether the purge or the writer
reached a given slot 2 first; the settled state is the same either way and is asserted.

## 7. Plain-ring coverage of the pre-existing fixes, purge vs user locks (2026-09-28)

- The three pre-existing ring fixes (commit "write every column on insert, map columns through
  the table map, log wraps as writes") were covered only on TTL rings. Plain-ring cases added:
  `ndb_ring_buffer.insert` Case 14 (omitted columns, TEXT included, single-row, multi-row and
  LOAD DATA wraps) and Case 15 (virtual generated column before ring_idx), and
  `ndb_rpl.ndb_rpl_ring_buffer` Case 18 (wrap logged as a write with
  ndb_log_update_as_write = 0; the replica re-creates the slot and the meta row).
- `ndb_ttl_purge.ttl_purge_lock_wait` (own .cnf, TransactionDeadlockDetectionTimeout 1200 ms):
  a user lock on an expired row makes the purge transaction time out (274) and retry every
  ~2.5 s while the other expired rows are reclaimed; on a TTL ring without ttl_index an open
  INSERT leaves that prefix's expired rows unpurged until it commits, with ttl_index it does not
  block the purge; a lock held for about 25 s makes the purge worker give up after its 10
  retries ("Has retried for 10 times"), quit with state error and be restarted. The last
  point is pinned as current behaviour; whether a lock timeout should count toward the worker
  restart is an open question for the purger.

## 8. The deferred tests (2026-09-28)

The five deferred or missing items of section 2 now have tests:
- B3: `ndb_ttl_purge.ttl_purge_ring_buffer_backup`: backup of rings with purged holes and
  unpurged expired rows; data restore into the same schema and with --promote-attributes
  VARCHAR -> TEXT (staging table). Visible rows, meta rows and physical rows equal the backed-up
  state; the purge reclaims the restored expired rows; the rings continue at next_pos.
- Node restart while rows expire: `ndb_ttl_purge.ttl_purge_ring_buffer_nr_expiry`: initial
  node restart; rows expire and are purged while the node is down, writers wrap into the
  purged slots, short-lived rows expire and are purged while the node copies its fragments,
  a writer runs during the start. Both replicas end with the same rows and meta rows; the
  restarted node alone serves the same result.
- D3: ClusterJ `RingBufferTest.testTtlRingDatetime` and `testTtlRingDatetimeConcurrent`
  (table `ring_buffer_ttl_dt`, DATETIME TTL column, ttl_index): annotation interface and
  DynamicObject inserts, meta row ts = 9999-12-31 23:59:59 and last in ttl_index, wrap over the
  expired slot, 4 concurrent writers without a lost meta update.
- A6: `ndb_ttl_purge.ttl_purge_ring_buffer_alter`: TTL shortened (rows become expired and are
  purged), lengthened (rows visible again, not purged), copying ALTER (the purge follows the new
  table), DROP and CREATE with the same name, all while the purge runs.
- C1: `ndb_ttl_rpl.ttl_rpl_ring_buffer_purge` (own .cnf: a REST server with the purge on the
  source and on the replica cluster): the source purge's deletes replicate and remove the same
  rows on the replica (expired there too), the wrap re-creates the purged slots on the replica,
  the replica's own purge then has nothing left. The first MTR test with a purge on both sides
  of replication; it applies to plain TTL tables as well (a plain TTL table is part of it).
  Note: the header of `ttl_rpl_expired_apply.test` says a replicated DELETE of a row that is
  expired on the replica is a no-op (626 swallowed); in this test such deletes removed the
  rows physically.
