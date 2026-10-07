# TTL on Ring Buffer Tables - Design

Status: v1.0, 2026-09-22. Implemented on `consolidate_RingBufferTable_26.05` (section 14
lists the files); the implementation commits follow the pre-implementation tip 78e642ec7d9.
Line references in this document (sections 5 and 13) point at 78e642ec7d9, the tree BEFORE
the implementation, so that the arguments can be re-read against the code they describe.
Branch history: v0.1 referenced `consolidate_RingBufferTable_26.04` @ 169d581df70; the ring
buffer sources are identical on both, only the mainline underneath differs.
Author: Claude (Fable 5.1) with zhao; decisions in section 8 are zhao's.
User-facing companion: `user_view.md` in this directory, published on Confluence as "TTL on
Ring Buffer Tables".
Changes in v1.0: open questions 2-7 resolved (section 9, decisions D10-D13); DICT enforces the
creation-time rule for the raw NDB API (D12); ClusterJ has no `deleteOldest`, so W2 is C++
only; section 11 lists the tests as written; section 14 added.
Changes in v0.2: line references re-pointed to the 26.05 branch; K4 deferred (D9, question 1
resolved); section 6 lock-cycle and purge-order corrections; section 4 order assumption;
D6 argument trimmed.

## 1. Purpose

TTL and Ring Buffer Tables (`MAX_ROWS_PER_PK`) are mutually exclusive today. This document
proposes how one table can carry both, what the combined semantics are, which code changes
are needed, and which alternatives were rejected. It is meant to be finalized in review,
then used as the implementation brief.

## 2. Background

### 2.1 Ring Buffer Table (as implemented)

A table with `COMMENT='NDB_TABLE=MAX_ROWS_PER_PK=N@ring_idx@ring_meta'` keeps at most N
rows per primary-key prefix. `ring_idx` (INT, last PK part) is the slot number 1..N. Slot 0
is a hidden meta row whose `ring_meta` column packs `Ring_meta`
(`storage/ndb/plugin/ha_ndbcluster_ring_buffer.cc:86`):

    version(U16) reserved_0(U16) next_pos(U32) count(U32) reserved_1(U32)
    total_inserts(U64) reserved_2(U64)            RING_META_VERSION = 1 (checked on read)

Every insert, in all three writer layers (SQL handler, `NdbRingBufferWriter`, ClusterJ
`RingBufferWriter`), reads the meta row with an exclusive lock, writes the data row at slot
`next_pos` with `writeTuple` (upsert), advances the meta
(`next_pos = next_pos % N + 1; if (count < N) count++; total_inserts++`) and writes it back.
The overwrite of the row at `next_pos` IS the eviction. The kernel knows only:
`Tablerec::m_ring_buffer_size` (Dbtup.hpp:1558), a write guard that rejects writes without the
`ring_buffer_op` flag (error 940, `is_ring_buffer_write_blocked`, Dbtup.hpp:4609), a read
filter that hides slot 0 from reads without `ring_buffer_op`/`ring_buffer_show_meta` (error
626, Dbtup.hpp:4628) and a trigger skip that keeps meta rows out of unique indexes and FKs
(DbtupTrigger.cpp:690, 717). Ordered (TUX) indexes are NOT skipped for meta rows
(DbtupExecQuery.cpp:2582 calls `executeTuxInsertTriggers` unconditionally).

Online resize is rejected in the handler and in DBDICT (Dbdict.cpp:9928). `deleteOldest(N)`
exists in the NDB API and ClusterJ writers: it deletes N slots from the tail computed from
`(next_pos, count)` and decrements `count` (NdbRingBufferWriter.cpp:792-832).

### 2.2 TTL (as implemented)

`COMMENT='NDB_TABLE=TTL=<sec>@<col>'`, column DATETIME2 or TIMESTAMP2. A row is expired when
`col + sec <= now (UTC)`. Expiry is a LOGICAL policy: DBTUP `checkTTL` makes an expired row
look absent (626) on user reads, updates and deletes (DbtupExecQuery.cpp:3156, 3519, 4772)
unless the operation carries `ttl_ignore` (recovery paths, `OO_TTL_IGNORE`). An upsert
(`ZWRITE`) skips the check entirely (`original_op_type != ZWRITE` gate, :3522). A duplicate
`ZINSERT` on a TTL table is converted by DBACC into an in-place write (DbaccMain.cpp:1491)
unless `NoTTLDupConvert` is set. Physical reclamation is done by the TTL purger in rdrs2
(`storage/ndb/rest-server2/server/src/ttl_purge.cpp`): per table per round it runs an
exclusive-lock scan with `SF_OnlyExpiredScan` and deletes each returned row with
`deleteCurrentTuple()` (no operation options, :2044 / :2207). If the table has an ordered
index literally named `ttl_index` whose first column is the TTL column, the scan is a bounded
index range scan; otherwise a full partition scan (both correct; index path measured at 0 ms
per empty round versus 3-9 ms per 20k rows for the fallback, 2026-07-27).

### 2.3 Why they are exclusive today (verified)

1. DDL rejection in the handler (`ha_ndbcluster.cc:10313`, and the inplace-ALTER path
   `:17279`) and in DBDICT (`Dbdict.cpp:6488`), message
   "A table cannot be both TTL and MAX_ROWS_PER_PK". MTR: `ndb_ring_buffer/create.test`
   Case 17, `alter.test` Case 7.
2. The recorded reason is real. The writers fill the meta row's NOT NULL user columns with
   `Field::reset()` zero (ha_ndbcluster_ring_buffer.cc:963-974). DBTUP runs the TTL check
   BEFORE the meta-hide check (:3156 then :3235) and `ring_buffer_show_meta` does not bypass
   TTL. A zero TIMESTAMP2 is expired (`ttl_expiry.hpp:115`), so the writer's own meta read
   returns 626, the writer re-inserts the meta row, DBACC converts the duplicate insert into
   an in-place write, and the ring state is silently reset. (A zero DATETIME2 happens to
   overflow to "never expires", `ttl_expiry.hpp:129-138`; that is an accident, not a design.)
3. The purger's take-over deletes carry no ring flag, so on a ring table every delete would
   hit 940, go to `table_err` (:2363), retry 10 times and abort the worker.
4. `deleteOldest` and the SQL prefix DELETE use plain deletes, which return 626 on expired
   rows.

What already works: the slot overwrite (upsert) ignores TTL; every recovery path sets the
TTL lever and the ring lever independently (copy fragment, REDO replay, LCP restore, backup,
SUMA, ndb_restore); the two LQHKEYREQ flags live in different words (DblqhMain.cpp:9399 vs
:9416; the ring op flag sits in the attrLen word, ttl_ignore in requestInfo) and never collide; DBDICT rejects any ALTER carrying the ring change bit, so the
TTL-then-ring else-if chains in DbtupMeta.cpp (1481-1495, 1633-1663) are unreachable.

## 3. Goals and non-goals

Goals
- One table may be both `MAX_ROWS_PER_PK` and `TTL`.
- Expired rows are physically reclaimed by the existing TTL purger, with no purger code
  changes.
- No new invariants that the three writer layers must keep in lockstep beyond the two
  described in 5.3.
- Zero cost on tables that use neither feature; no new cost on plain TTL tables.

Non-goals (for v1)
- Enforcing time order inside a ring (monotonic TTL column). Optional later, see 9.
- A ring-aware purge that keeps `count` exact.
- A queue-consumer API (`deleteOldest`) on TTL rings.
- Reclaiming CAPACITY (a purged slot is refilled only when `next_pos` reaches it).

## 4. Semantics of a TTL ring table

- Capacity: at most N rows per prefix, newest overwrites the slot at `next_pos`. Unchanged.
- Visibility: a row is visible iff it is within TTL. Unchanged TTL semantics for reads,
  updates, deletes, including read-before-write removal.
- Reclamation: the TTL purger deletes expired rows physically, in whatever order its scan
  produces them. The ring meta row is never modified by the purger.
- `count` in `Ring_meta` means "span of occupied slots since the last wrap" and is an upper
  bound on physical rows and on visible rows. (With TTL it is already an upper bound on
  visible rows, purge or not.) `total_inserts` stays exact.
- Empty slots (holes) may exist anywhere in the span. Inserts do not seek holes; they write
  at `next_pos`.
- `deleteOldest` is not available on a TTL ring table.
- The TTL column of a data row may be updated like on any TTL table.
- Being a TTL ring is a creation-time property: TTL seconds may be changed by ALTER; TTL
  cannot be enabled or disabled on an existing ring table; ring size cannot change (already
  the rule).
- Order assumption, not enforced (D5): the TTL column is expected to hold the insertion
  time, so ring order and TTL order agree and expired rows sit at the tail. Nothing in v1
  depends on this (section 6). If the orders disagree, a live far-future row can be evicted
  by the ring while a near-expiry row waits for the purger; both features keep their
  contracts.

## 5. Design

### 5.1 Kernel

K1. Ring meta rows never expire. At the three `checkTTL` sites (handleReadReq :3156,
    handleUpdateReq gate :3519, handleDeleteReq :4772) skip the TTL block when
    `is_ring_buffer_table(tab) && isRingBufferMetaRow(tab, tuple)`. One compare on TTL
    tables only; `m_ring_buffer_size` shares the cache line with `m_ttl_sec`.
    Effect: writer meta reads/updates see the meta row regardless of the TTL column's value;
    a duplicate meta insert converted by DBACC lands in the "not expired + ZINSERT_TTL"
    branch (:3543-3553) and returns 630, exactly today's first-insert race outcome, so the
    silent meta reset of 2.3(2) is gone. The only-expired purge scan sees the meta row as
    live and skips it (and the meta-hide filter hides it anyway).

K2. The write guard admits only-expired deletes. In `is_ring_buffer_write_blocked`
    (Dbtup.hpp:4609) return false for `Roptype == ZDELETE && regOperPtr->ttl_only_expired`.
    Sound because a take-over delete from an `SF_OnlyExpiredScan` inherits
    `OF_TTL_ONLY_EXPIRED` (NdbScanOperation.cpp:3186-3192) and DBTUP then refuses to delete a
    live row (:4797-4803); such a delete can only remove an expired row, which never breaks
    a ring invariant under section 4. Alternative: have the purger pass `OO_RING_BUFFER_OP`
    via the NdbRecord `deleteCurrentTuple` variant; rejected for v1 because the purger uses
    the old API and would need a partial rewrite for no semantic gain.

K3. Lift the exclusion: `Dbdict.cpp:6488` `tabRequire`, `ha_ndbcluster.cc:10313` and
    `:17279`. Replace the DICT comment with a pointer to K1/K2. Keep the FR exclusion.

K4. (Deferred, not in v1; D9.) Skip ordered-index (TUX) maintenance for ring meta rows.
    Facts verified 2026-09-16 on the code:
    - Cost today: every ring insert updates the meta row, and the TUX update trigger adds an
      entry for the new tuple version in every ordered index with no change-mask check
      (DbtupTrigger.cpp:2226 -> `addTuxEntries`); commit removes the old version's entry
      (:2307 -> `removeTuxEntries` :2391). Per ordered index a ring insert costs one TUX
      operation for the data row and two for the meta row. Every ring table has a PRIMARY
      ordered index unless the PK is declared USING HASH.
    - The PRIMARY ordered index must keep its meta-row entries: the validated prefix DELETE
      opens its scan with show-meta (ha_ndbcluster.cc:3836/:3961/:14689 via
      `show_meta_active`, ha_ndbcluster_ring_buffer.cc:210) and finds the meta row through
      the PRIMARY range scan. Removing it from that index would leave a stale meta row
      behind after DELETE. So K4 can only cover user-created ordered indexes such as
      `ttl_index`: it saves two of six TUX operations per insert on a TTL ring with
      `ttl_index`, plus one index entry per prefix, and needs DBTUP to tell the PRIMARY
      index's trigger apart. Not measured end to end.
    - No legacy-entry hazard and no gate: ordered indexes are not persisted. DBDICT rebuilds
      every ordered index at each node restart (Dbdict.cpp:3792 `rebuildIndexes`) through
      DBTUP's own page walk, so a node running K4 code never holds an index built without
      it. K4 can therefore land in any later release and may cover plain ring tables too.
    - Sites that must agree: insert trigger (DbtupExecQuery.cpp:2582), update trigger
      (:2721), commit and abort removal (DbtupTrigger.cpp:2307/:2346), the page-walk build
      used for online ADD INDEX and restart (DbtupIndex.cpp:604 `buildIndex`) and the
      multi-threaded offline build (DbtupIndex.cpp:852 -> DbtuxBuild.cpp:82). A remove for
      an entry that was never added is a SearchError (DbtuxMaint.cpp:148-161) that
      `removeTuxEntries` turns into a node crash; an add with no later remove leaves a
      dangling entry.
    With K4 deferred, W1 handles the meta entry's placement in `ttl_index` (5.3).

(Review fix, 2026-09-22.) The NDB API rejects OO_TTL_IGNORE combined with
OO_TTL_ONLY_EXPIRED on key operations and SO_TTL_IGNORE with an only-expired scan (error
4360), so the K2 admission cannot be combined with a skipped expiry check by a client; the
purger inherits its flags from the scan and is unaffected.

Dropped: "ring_buffer_op implies ttl_ignore". The handler sets `OO_RING_BUFFER_OP` on every
user UPDATE/DELETE on ring tables (ha_ndbcluster.cc:5820, :6214); with read-before-write
removal a coupled TTL bypass would let plain SQL resurrect or delete invisible rows. K1
makes the coupling unnecessary.

Hygiene (not required): DbtupMeta.cpp else-if chains; TcKeyReq/LqhKey signal printers.

### 5.2 Purger (rdrs2)

No code change. Membership stays `tab->isTTLEnabled()` (ttl_purge.cpp:1138, :1169, :1065).
On a ring table the only-expired scan returns expired data rows only (meta rows are live per
K1 and hidden by the meta filter), and each take-over delete passes the guard per K2.
Deletion order is irrelevant to the ring (section 6). `ttl_index` is recommended, not
required: without it each round is a full partition scan over `prefixes x (N+1)` rows.
Sharding, active window, batch adaptation and metrics apply unchanged.

### 5.3 Writers (handler, NdbRingBufferWriter, ClusterJ RingBufferWriter)

W1. Fill the meta row's TTL column with the type maximum instead of zero/NULL
    (TIMESTAMP2 '2038-01-19 03:14:07', DATETIME2 '9999-12-31 23:59:59'), regardless of
    nullability. Purpose: the meta entry sits past the purge range in `ttl_index` (a zero or
    NULL sorts first and costs one wasted candidate per prefix per round). Not needed for
    correctness (K1 covers that). In v1 (D9); it becomes redundant for any index that a
    later K4 excludes.

W2. Reject `deleteOldest` when `table->isTTLEnabled()` (C++ and Java), with a dedicated
    error. Rationale in 8/D4. No change to `deleteOldest` on plain rings.

W3. (Review fix, 2026-09-22.) A slot write is a `writeTuple`, which DBACC turns into an
    update when the slot is occupied: a column absent from the write mask keeps the
    overwritten row's value. For the TTL column that would hide a new row behind the old
    row's expired value, and for any column it leaves a replica whose purge removed the
    slot unable to rebuild the row from an updated-only event. NdbRingBufferWriter
    therefore requires, on a TTL table, every column of the table in the NdbRecord
    (constructor, error 4359) and every column except ring_idx/ring_meta in every row's
    mask (`addRow`, 4359; blob columns are set through their handles and must be in the
    mask too); the ClusterJ writer rejects a row whose mask lacks any such column
    (ClusterJUserException) except BLOB/TEXT columns, which ClusterJ cannot write on a ring
    buffer table at all, so they keep the overwritten slot's value on a wrap (use SQL for
    such tables). Plain ring tables keep the old contract (omitted
    columns retain the overwritten slot's value; documented in the header). The SQL
    handler writes every stored column of a ring data row (5.4), so it never has the
    problem. The writer's column counts now follow the columns present in the NdbRecord
    (a record lacking a NOT NULL column no longer reads uninitialised metadata) and a
    record missing a primary key column is rejected (4118).

No `Ring_meta` format change in v1. `reserved_2` stays free for a future max-TTL field.

### 5.4 Handler DDL and DML rules

- CREATE with both modifiers: allowed (after K3). TTL column rules unchanged (DATETIME2 or
  TIMESTAMP2, not hidden/virtual, not on-disk). FK stays excluded on both.
- ALTER changing TTL seconds: allowed. ALTER enabling or disabling TTL on a ring table:
  rejected with a clear message (DROP + CREATE). Rationale 8/D6. The handler rejects it on
  the copy path (`ER_ILLEGAL_HA_CREATE_OPTION`, "Cannot enable/disable TTL on a ring table;
  use DROP+CREATE") and on the inplace path (same text as the unsupported reason); DICT
  rejects the same change from the raw NDB API with 741 UnsupportedChange (D12).
- Index rules unchanged: no index on `ring_idx`/`ring_meta`; `ttl_index` on the TTL column
  is allowed and recommended. Optional: warn at CREATE/ALTER when a TTL table lacks it.
- UPDATE of the TTL column on data rows: allowed (existing ring guards for `ring_idx`,
  `ring_meta`, PK columns and the meta row stay).
- (Review fix, 2026-09-22.) A ring INSERT writes every stored column of the data row
  (`bitmap_set_all(write_set)` before the write): omitted columns get their default or
  NULL from record[0] instead of keeping the overwritten slot's value (before the fix a
  plain ring kept the old value of any omitted column on a wrap, and a TTL ring could
  inherit an expired TTL value and hide the new row). The meta row's TTL maximum is
  written by both the single-row and the batched meta path (one helper). Ring columns
  and the TTL column are NDB column numbers and are mapped to MySQL fields through
  `Ndb_table_map::get_field_for_column` (virtual generated columns shift the numbering;
  before the fix a ring table with a virtual column before `ring_idx` rejected every
  INSERT).
- SQL DELETE rules unchanged. Optional H1: set the ignore-TTL extra on the validated
  prefix-DELETE scan so expired rows are removed together with the meta row (cosmetic; the
  purger removes them anyway).
- Rolling upgrade: gate the combined DDL like the existing ring DDL gate
  (commit cc18f76c7de, gate 26.5.0), so old data nodes (no K1/K2) and old writers (no W1/W2) never see a
  TTL ring table. Own constant `NDBD_SUPPORT_TTL_RING_BUFFER` (D10); message "TTL ring
  buffer not supported by current data node versions".

## 6. Correctness arguments

- Insert vs purge on one slot: both need the row lock. For a single-row insert transaction
  there is no cycle: the purge never takes the meta lock, the inserter takes meta then one
  slot. A transaction that inserts several rows into one prefix holds the meta lock and
  slots S1..Sk; if the purge batch holds the expired slot S(k+1) and the exclusive purge
  scan then reaches S1, both wait (a locking scan waits on a locked row) and the
  transaction deadlock timeout resolves it; the transaction is retried. Plain TTL tables
  have the same exposure today. If the purge commits first, the upsert becomes an insert;
  otherwise the purge locks the new row version, DBTUP sees it live and refuses (626), the
  purger retries the batch.
- Holes: every writer uses `writeTuple` for data rows (handler; NdbRingBufferWriter.cpp:583;
  RingBufferWriter.java:453) and chooses insert/update only for the meta row by existence.
  A purged slot is indistinguishable from a never-filled slot, a state that exists before
  the first wrap. The old grow adjustment that rewrote `next_pos` was removed with resize.
- Purge order and TTL-value order: the visible set of a prefix is always {the newest N
  inserts by slot} intersected with {rows within TTL}. Writers never consult physical
  presence, readers cannot observe expired rows, and the purge removes only rows already
  outside the visible set, so neither the order in which the purger deletes nor the order
  of TTL values across slots changes what is visible. Monotonic TTL values only make the
  expired rows contiguous at the tail, so holes are refilled first; without them a live
  row can be evicted while an expired row elsewhere waits for the purger, which is the
  ring's contract, not a fault. Index order is not slot order anyway (it sorts by TTL value
  across prefixes).
- Why the purger must not fix `count`: it would need the meta exclusive lock, creating an
  inserter/purger lock cycle and stalling behind bulk-insert transactions.
- Recovery: copy fragment, REDO, LCP restore, backup and ndb_restore copy rows and meta
  verbatim; a missing slot is already representable. Replication: purge deletes replicate as
  PK deletes; the replica gets identical holes; the replica's own purger races idempotently.
  (Review fix, 2026-09-22.) A wrap onto an occupied slot is an UPDATE event; with
  `ndb_log_update_as_write = 0` (or a USE_UPDATE binlog type) it would be logged as an
  update row event, and a replica whose purger already removed the slot would drop the
  missing-key update (idempotent apply) while its meta row advances. The injector logs
  ring buffer table events as write row events regardless of that setting. Conflict
  functions are rejected at CREATE of a ring buffer table, and setup_conflict_fn drops
  them for a ring table that a mysql.ndb_replication row matches later: conflict
  detection applies write row events as inserts, so every meta update and every wrap
  would be treated as a conflict and the replica ring would stop advancing. With `ndb_log_updated_only` the
  after image carries the written columns only; the every-column rule (W3, 5.4) makes
  that the whole row.
- The pre-existing "ZINSERT_TTL not REDO-safe" hazard is not reachable from ring writers
  (meta insert on an existing row now yields 630 per K1; data rows are upserts).

## 7. Cost

| Situation | Per round per table |
|---|---|
| `ttl_index`, nothing expired | one empty-range seek (~0 ms) |
| no index, nothing expired | full partition scan, 3-9 ms per 20k rows scanned |
| expired rows present | one exclusive-lock delete each + REDO + binlog event |

- Per ring insert with `ttl_index`: one TUX add for the data row (as on any TTL table) plus
  one for the meta-row update, unless K4. Meta entries: one per prefix, unless K4.
- Contention: the purge holds locks on expired slots for one batch (5-50 rows by default);
  an inserter on that prefix waits while holding the meta lock, so writers of that prefix
  queue for milliseconds. Only lukewarm/cold prefixes; hot rings have nothing expired. A
  multi-row insert transaction can, rarely, cycle with the purge scan and pay the
  transaction deadlock timeout (section 6).
- Kernel hot path: K1/K2 are one compare each, TTL tables only. Non-TTL, non-ring tables
  unchanged.

## 8. Decision log

D1. Purge reclaims expired rows on ring tables (not "purger skips ring tables").
    zhao: "I'd prefer to make the purge reclaim the expired rows".
D2. The purger never modifies the meta row; `count` becomes the occupied slot span.
    Verified harmless for every consumer (section 6).
D3. Purge order (index vs table scan) is irrelevant; `ttl_index` recommended, not required.
D4. `deleteOldest` disabled on TTL rings. zhao: "Disable it." Reason: slot-unit semantics
    versus visible-row semantics cause consumer double-processing once TTL is on; a TTL-aware
    version is a redesign. Consumers use offsets (`ring_idx`, `total_inserts`).
D5. Monotonic TTL column NOT enforced in v1 (zhao's original idea 1). Reclaim does not need
    it; hard rejection would fail inserts under writer clock skew; clamping mutates data.
D6. TTL ring is a creation-time property: seconds changeable, enable/disable rejected.
    Enable: existing meta rows carry zero/NULL TTL (permanent wasted index candidates until
    every prefix's meta row is rewritten); old writers.
    Disable: `deleteOldest` becomes legal on a ring with holes and aborts on not-found.
D7. TTL column updates on data rows allowed (guard withdrawn with D5).
D8. No "ring_buffer_op implies ttl_ignore" kernel coupling (5.1, Dropped).
D9. K4 deferred, W1 ships in v1. zhao 2026-09-16: "Agree." The PRIMARY ordered index must
    keep meta rows for the prefix DELETE, so K4 saves two of six TUX operations per insert;
    ordered indexes are rebuilt at every restart, so K4 is not now-or-never; five kernel
    sites plus PRIMARY-index detection for an unmeasured gain.
D10. Version gate: own constant `NDBD_SUPPORT_TTL_RING_BUFFER` in ndb_version.h.in, value
    26.5.0 = the same release as `NDBD_SUPPORT_RING_BUFFER` on the assumption that the ring
    buffer feature and TTL ring tables ship together. If the ring buffer feature is released
    first, the constant must be raised to the first release with K1/K2 (the header comment
    says so). Decided by Claude under the handover's recommendation; to be confirmed by the
    release owner.
D11. Errors and messages: `deleteOldest` on a TTL ring returns NDB API error 4358
    ("deleteOldest is not supported on a ring buffer table with TTL", application error).
    Rejected ALTERs: "Cannot enable/disable TTL on a ring table; use DROP+CREATE" (58 chars,
    `ER_ILLEGAL_HA_CREATE_OPTION` clips at 64). Gate: "TTL ring buffer not supported by
    current data node versions" (59 chars).
D12. DICT enforces D6 for the raw NDB API: `alterTable_parse` rejects an ALTER on a ring
    buffer table whose TTL enabled-state changes with AlterTableRef::UnsupportedChange (741),
    the code the ring-size change already uses. A TTL seconds change passes. Same pattern as
    the ring-size backstop (Dbdict.cpp:9928 on 78e642ec7d9).
D13. Questions 2, 3, 6, 7 answered no / later: no DDL warning for a missing `ttl_index`
    (separate change if wanted, applies to all TTL tables), no conditional enable/disable,
    no new physical-row view (`total_inserts`, COUNT(*) and ndbinfo.memory_per_fragment
    suffice; the purge test uses the latter), `reserved_2` and a time-aware `deleteOldest`
    stay future work.

## 9. Open questions (all resolved in v1.0)

1. Resolved (D9): W1 in v1, K4 deferred.
2. DDL warning when a TTL table has no `ttl_index`: no (D13).
3. Conditional enable/disable of TTL on an existing ring: no (D6, D13).
4. Error codes/messages: D11.
5. Version gate value: D10 (26.5.0, own constant; confirm with the release owner).
6. Physical rows per prefix exposed: no (D13).
7. `reserved_2` as running max TTL, time-aware `deleteOldest`: future work (D13).

## 10. Alternatives considered and rejected

- Purger skips ring tables; rely on overwrite only. Leaves up to N expired rows per cold
  prefix forever. Rejected by D1.
- Meta-driven tail-drain purge (`deleteOldest` while tail expired, max TTL in meta). Keeps
  `count` exact but needs monotonic TTL for completeness, cannot use an index (meta rows are
  not indexable), scans every row each round, and is a new purger code path.
- Purger updates `count` per deleted row. Lock cycle and stalls (section 6).
- Far-future TTL value in the meta row instead of K1. Fragile (TIMESTAMP max minus a large
  TTL can already be in the past); kept only as W1 for index placement.
- `ring_buffer_op` implies `ttl_ignore` (+ `NoTTLDupConvert`). Changes SQL UPDATE/DELETE
  semantics on ring tables through read-before-write removal (D8).
- TTL-aware `deleteOldest` (skip empty/expired slots, count live deletions only). Needs the
  deleted row's TTL value on delete and client-side expiry; several round trips; redesign.
- K4 (TUX skip for meta rows) in v1. Deferred, not rejected: D9.

## 11. Tests (as written in v1.0)

MTR `ndb_ring_buffer`:
- `create.test` Case 17: TTL + MAX_ROWS_PER_PK create succeeds (TIMESTAMP and DATETIME).
- `alter.test` Case 7: enabling TTL on a ring rejected (copy and inplace paths), TTL seconds
  change on a TTL ring allowed, TTL=OFF rejected, comment without TTL keeps it.
- New `ttl_ring_buffer.test`: visibility follows TTL, meta row visible with show_meta and
  carrying the TIMESTAMP / DATETIME maximum, ring wrap over an expired slot, TTL column
  UPDATE on a data row, UPDATE of an expired row matches 0 rows, prefix DELETE with an
  expired slot then ring restart, fully expired prefix keeps its meta row, `ttl_index`
  allowed and hides meta rows.
- `node_restart.test` Case 6 and `backup_restore.test` Case 12: TTL ring with an expired
  slot survives a system restart / backup+restore with `HEX(ring_meta)` unchanged and the
  ring continuing over the expired slot.
MTR `ndb_ttl_purge` (needs rdrs2): new `ttl_purge_ring_buffer.test`: 3 prefixes with expired
and future rows, wait for `rows_purged_total`, visible rows unchanged, physical rows (sum of
`ndbinfo.memory_per_fragment.fixed_elem_count` over one replica) reduced to meta + live
rows, meta rows untouched (`HEX(ring_meta)`, TTL column at the DATETIME maximum), inserts
continue into the reclaimed slots; with and without `ttl_index`.
MTR `ndb_rpl`: `ndb_rpl_ring_buffer.test` Case 14: TTL ring DDL, visible set and meta row
replicate byte-identical; wrap over the expired slot follows on the replica. (Purge deletes
are ordinary deletes and replicate like any other; not exercised without rdrs2.)
NDB API `ndbapi_ring_buffer_test`: Test 28 reworked (FR + ring still rejected; ALTER enabling
or disabling TTL on a ring rejected with 741 and TTL state unchanged); Test 33 writer on a
TTL ring, meta TTL column = maximum, `deleteOldest` -> 4358 with the table untouched; Test 34
(K1) meta row with a zeroed TTL column stays visible and the next insert continues the ring
(total_inserts 2, not a silent reset); Test 35 (K2) only-expired delete of an expired ring
row passes the guard, of a live row -> 626, plain delete -> 940, meta untouched, ring
continues past the hole.
ClusterJ `RingBufferTest.testTtlRing`: table `ring_buffer_ttl`; expired row invisible, meta
row ts = TIMESTAMP maximum via SQL, inserts continue, `find` of the expired slot is null.
Second pass (2026-09-22, `test_plan.md` maps the handover gap list A-G to these):
`ttl_ring_buffer_dml.test` (multi-row INSERT, LOAD DATA, INSERT ... SELECT, unique index with
an expired owner, scan UPDATE, NULL TTL values, DELETE forms and
`ttl_expired_rows_visible_in_delete`, transactions, TRUNCATE, OPTIMIZE);
`ttl_ring_buffer_ddl.test` (CREATE ... LIKE / AS SELECT, RENAME TABLE, modifier-only comment,
TTL column rename guard, TTL seconds change, copying ALTER with expired rows, user index);
`ndb_ttl_purge.ttl_purge_ring_buffer` (added: TIMESTAMP(3)/DATETIME(6) TTL columns, NULL TTL rows,
unique value reuse after the purge, blob parts of purged rows);
`ndb_ttl_purge.ttl_purge_ring_buffer_concurrent` (writers racing the purge on 20 prefixes:
single-row inserts, 10-row insert transactions with deadlock-timeout retry, prefix DELETE plus
re-INSERT; invariants once the purge has settled); `ndb_ttl_purge.ttl_purge_ring_buffer_restart`
(node restart, initial node restart, system restart with purged holes; refill; purge after the
system restart); NDB API Test 35 step (d) (only-expired delete of the meta row -> 626) and
Test 36 (TIMESTAMP(3) meta maximum through the writer; an NdbRecord without the TTL column).
Not covered: the purge-delete replication path (`ndb_ttl_rpl` has no rdrs2), ClusterJ with a
DATETIME TTL column, the documented OO_TTL_IGNORE + OO_TTL_ONLY_EXPIRED bypass (see
`test_plan.md` section 2 for the reasons).

## 12. Implementation size

Kernel: K1 three sites in DbtupExecQuery.cpp (~45 lines with comments), K2 one line plus
comment in Dbtup.hpp, K3 + D12 in Dbdict.cpp (~25 lines). NDB API: W1 ~70 lines
(NdbRingBufferWriter.cpp/.hpp, NdbSqlUtil pack helpers), W2 ~10 lines, one ndberror.cpp entry.
ClusterJ: two jtie getters (NdbDictionary.java, ndbapi_jtie.hpp), Table/TableImpl TTL
accessors, W1 ~40 lines in RingBufferWriter.java. Handler: ~70 lines (rules, gate, W1). H1 not
done. Purger: 0 lines. Tests are the bulk (section 11).

## 13. Reference line index (branch 78e642ec7d9)

ha_ndbcluster.cc 10313 / 17279 exclusions; 5620-5673 ring UPDATE guards; 5019-5023 applier
ring flag; 5813-5822, 6207-6217 update/delete flags; 3836/3961/14689 show_meta_active scan
sites; ha_ndbcluster.h 781 should_ignore_ttl.
ha_ndbcluster_ring_buffer.cc 72 RING_META_VERSION; 86 Ring_meta; 131-135 advance; 352-387
check_index_columns; 719-725 meta read; 963-974 zero fill; 210 show_meta_active.
Dbdict.cpp 6488-6492 exclusion; 9928 ring-bit ALTER rejected; 3792 rebuildIndexes (restart).
DbtupExecQuery.cpp 3156/3235 TTL then meta-hide; 3519-3559 update gate and ZINSERT_TTL
branches; 4772-4814 delete TTL; 2582 TUX insert (no meta skip); 2721 TUX update.
Dbtup.hpp 4609 write guard; 4628 meta hidden; 1552-1558 Tablerec fields; 1190-1193 op flags.
DbaccMain.cpp 1491-1498 TTL dup convert (ring-unaware).
DblqhMain.cpp 9399 ttl_ignore unpack; 9416 ring_buffer_op unpack (attrLen word).
DbtupMeta.cpp 1481-1495, 1633-1663 else-if chains. DbtupTrigger.cpp 690/717 skip_idx_fk;
2226 executeTuxUpdateTriggers; 2307/2346 executeTuxCommit/AbortTriggers; 2391
removeTuxEntries. DbtupIndex.cpp 604 buildIndex; 852 buildIndexOffline. DbtuxBuild.cpp 82
mt_buildIndexFragment. DbtuxMaint.cpp 148-161 OpRemove -> SearchError.
NdbScanOperation.cpp 3186-3192 take-over inherits only-expired. NdbOperation.hpp 1135/1138/
1147 OO_TTL_IGNORE / OO_TTL_ONLY_EXPIRED / OO_RING_BUFFER_OP.
NdbRingBufferWriter.cpp 583 writeTuple; 632/636 meta insert/update; 792-832 deleteOldest.
RingBufferWriter.java 316/319 meta; 453 writeTuple. NdbDictionary.hpp 1360 isTTLEnabled;
1366 getRingBufferSize; 1371 isRingBuffer.
ttl_purge.cpp 1131 UpdateLocalCache; 1138/1169/1065 membership; 1911-1918 index scan;
2044/2207 deleteCurrentTuple; 2160 table scan; 2363 table_err.
ttl_expiry.hpp 115 zero TIMESTAMP expired; 129-138 zero DATETIME never expires.

## 14. Implementation summary (v1.0)

Files changed, by concern (one commit per concern is the intended split):

C1 kernel: `storage/ndb/src/kernel/blocks/dbtup/Dbtup.hpp` (K2),
`storage/ndb/src/kernel/blocks/dbtup/DbtupExecQuery.cpp` (K1 x3).
C2 DDL rules: `storage/ndb/src/kernel/blocks/dbdict/Dbdict.cpp` (K3, D12),
`storage/ndb/plugin/ha_ndbcluster.cc` (create + inplace rules, gate),
`storage/ndb/include/ndb_version.h.in` (gate constant).
C3 writers W1: `storage/ndb/plugin/ha_ndbcluster_ring_buffer.cc`,
`storage/ndb/src/ndbapi/NdbRingBufferWriter.cpp`, `storage/ndb/include/ndbapi/NdbRingBufferWriter.hpp`,
`storage/ndb/src/ndbjtie/com/mysql/ndbjtie/ndbapi/NdbDictionary.java`, `storage/ndb/src/ndbjtie/ndbapi_jtie.hpp`,
`storage/ndb/clusterj/clusterj-core/.../store/Table.java`, `storage/ndb/clusterj/clusterj-tie/.../TableImpl.java`,
`storage/ndb/clusterj/clusterj-tie/.../RingBufferWriter.java`.
C4 W2: `storage/ndb/src/ndbapi/NdbRingBufferWriter.cpp` (deleteOldest), `storage/ndb/src/ndbapi/ndberror.cpp` (4358).
C5 tests: section 11 (v1 and second pass); `test_plan.md` = coverage map.
C6 (H1) not done.

Verification (2026-09-22): built; MTR PASS for ndb_ring_buffer create, alter, ttl_ring_buffer,
ndbapi_ring_buffer_test (35 tests), node_restart, backup_restore, clusterj_ring_buffer,
ndb_ttl_purge.ttl_purge_ring_buffer (purge reclaimed exactly the expired rows: physical rows
15 -> 7 -> 9 with `ttl_index`, 8 -> 3 without; meta rows untouched) and
ndb_rpl.ndb_rpl_ring_buffer. One fact learned while recording: a prefix DELETE on any ring table
reports N+1 affected rows because the validated prefix-delete scan surfaces the hidden meta row and
MySQL counts its deletion (control-verified on a plain ring); the TTL filter still applies to that
scan, so expired rows are not deleted by it (ttl_ring_buffer.test Case 5 shows 1 physical row left).

Second pass (2026-09-22): full regression `ndb_ring_buffer,ndb_ttl_purge,ndb_ttl,ndb_ttl_rpl` all 98
tests PASS (7 skipped, debug-only). Facts pinned by the new tests: a copying ALTER drops expired
rows (the copy scan has no TTL bypass) and copies the meta row unchanged, so `count` may exceed the
physical rows afterwards; `SET SESSION ttl_expired_rows_visible_in_delete = 1` makes prefix and bare
DELETEs remove expired ring rows too (H1 exists as an opt-in); changing the TTL seconds inplace only
changes visibility; a unique value of an expired row is blocked for other primary keys until the
wrap or the purge removes the row. Run results per test: `test_plan.md` section 4.
