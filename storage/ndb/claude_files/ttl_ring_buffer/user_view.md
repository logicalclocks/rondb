# TTL on Ring Buffer Tables: What You Gain and What You Trade

Status: v1.1, 2026-10-02. Companion to `design.md` in this directory, which holds the
engineering design. This document describes the v1 behaviour as agreed on 2026-09-16 from
the point of view of someone who creates, writes to and reads from such a table. Implemented
on `consolidate_RingBufferTable_26.05`; error numbers and messages below are the shipped ones.

## 1. In one paragraph

A Ring Buffer Table keeps the newest N rows per key prefix. A TTL table hides rows once they
are older than a fixed time. Today a table can be one or the other, not both. With this
feature a table can be both: it keeps at most N rows per prefix, hides every row whose TTL
has passed, and the existing TTL purger removes the hidden rows from memory. Writers and
readers use the same SQL statements, NDB API calls and ClusterJ calls as before; NDB API
and ClusterJ inserts must set every column (section 3.2). What you give up is listed in section 5: the `deleteOldest` queue-consumer call, an exact `count`
in the ring meta row, and the ability to add or remove TTL on a ring table after creation.

## 2. Creating a TTL ring table

```sql
CREATE TABLE user_events (
  user_id   INT NOT NULL,
  ring_idx  INT NOT NULL DEFAULT 0,
  ring_meta VARBINARY(64),
  ts        TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  payload   VARCHAR(200),
  PRIMARY KEY (user_id, ring_idx),
  INDEX ttl_index (ts)
) ENGINE=NDB
  COMMENT='NDB_TABLE=TTL=86400@ts,MAX_ROWS_PER_PK=100@ring_idx@ring_meta';
```

This table keeps the newest 100 events per user and hides every event older than one day.

Rules that already apply to each feature on its own still apply:

- The TTL column is a DATETIME or TIMESTAMP column, not virtual, not hidden, not stored on
  disk. NULL in the TTL column means the row never expires.
- The table has a declared PRIMARY KEY. `ring_idx` is an INT, the last column of the
  primary key, and declared after the other primary key columns. `ring_meta` is a VARBINARY
  column that the writers own. Neither may carry an index.
- No foreign keys. No user-defined PARTITION BY. The ring size cannot be changed later, and
  ALTER TABLE cannot turn an existing table into a ring table ("Cannot enable ring buffer
  via ALTER; use DROP+CREATE").

Three rules are new:

- The TTL column must not be part of the primary key. The writers store the maximum
  timestamp in the meta row's TTL column (section 5.7); in a key column that would move the
  meta row away from its prefix.

- The TTL index is recommended. Name it exactly `ttl_index` and put the TTL column first.
  Without it every purge round scans the whole table; with it a round on a table with nothing
  expired costs almost nothing. Section 5.5 has the numbers.
- All data nodes must run a version that supports TTL ring tables before you can create one.
  During a rolling upgrade the CREATE fails with a clear error until the last node is upgraded.

## 3. How it behaves

One rule covers everything you can observe:

    At any moment, the visible rows of a prefix are the newest N inserted rows,
    minus those whose TTL has passed.

Every INSERT writes the next slot and overwrites whatever was there: a live row, an expired
row, or nothing. Expired rows behave exactly as on any TTL table: SELECT does not return them,
UPDATE and DELETE report zero affected rows. The purger deletes expired rows in the
background. Purging never changes what you see. It only returns memory. A purged slot is
empty until the ring reaches it again, and the next INSERT that lands there fills it.

The `ring_idx` value of a row and the `total_inserts` counter in the meta row keep their
meaning, so consumers that track their position with them keep working. You may UPDATE the
TTL column of a data row, as on any TTL table.

### 3.1 Timeline

Ring size 3, TTL one hour, one user. Slots are numbered 1 to 3, slot 0 is the hidden meta row.

| Time | Action | Slot 1 | Slot 2 | Slot 3 | Visible | Physical rows | Meta `count` |
|---|---|---|---|---|---|---|---|
| 00:00 | insert A, B, C | A | B | C | A B C | 3 | 3 |
| 00:30 | insert D | D | B | C | D B C | 3 | 3 |
| 01:01 | B and C expire | D | B (hidden) | C (hidden) | D | 3 | 3 |
| 01:02 | purger runs | D | empty | empty | D | 1 | 3 |
| 01:10 | insert E | D | E | empty | D E | 2 | 3 |
| 01:31 | D expires | D (hidden) | E | empty | E | 2 | 3 |
| 01:40 | insert F | D (hidden) | E | F | E F | 3 | 3 |
| 01:50 | insert G | G | E | F | G E F | 3 | 3 |

At 01:50 the INSERT of G overwrote the hidden row D, so the purger never had to touch it. At
01:02 the purger removed B and C because the ring was not going to overwrite them soon. Both
paths lead to the same visible result. Note the last column: `count` stays at 3 from the
first wrap onward even when only one physical row exists. Section 5.2 explains why.

### 3.2 Reading and writing

```sql
-- Only live rows come back. Slot position is not time order, so order by ts.
SELECT payload FROM user_events WHERE user_id = 7 ORDER BY ts;

-- Live rows only. An expired row counts as absent.
SELECT COUNT(*) FROM user_events WHERE user_id = 7;

-- Allowed. Moves the row's expiry.
UPDATE user_events SET ts = NOW(6) WHERE user_id = 7 AND ring_idx = 42;

-- Allowed. Removes the prefix's live rows and its hidden meta row; the ring restarts at
-- slot 1. Expired rows not purged yet stay (hidden) until the purger or the ring removes them.
DELETE FROM user_events WHERE user_id = 7;
```

A DELETE on a ring table may filter only on the primary key columns before `ring_idx`, and
its WHERE must be deterministic: `RAND()`, stored functions and UDFs are rejected.

The NDB API `NdbRingBufferWriter` and the ClusterJ `RingBufferWriter` insert as they do on a
plain ring table, with one extra rule: on a TTL ring every insert must set every column of
the table except `ring_idx` and `ring_meta` (NDB API error 4359, "Inserts on a ring buffer
table with TTL must set every column"). When the ring wraps, an insert overwrites an
occupied slot, and a column left out would keep the overwritten row's value, including a TTL
value that may already have passed. ClusterJ cannot write ring tables that have BLOB/TEXT
columns; use SQL for them. SQL INSERTs always write every column (omitted columns get their
default). Two SQL INSERTs into a new prefix at the same moment can both find no meta row; one
of them then fails with a deadlock error ("try restarting transaction") and must be
retried, also with INSERT IGNORE. The NDB API writer's `deleteOldest` call returns error 4358 on a
TTL ring table, see section 5.1 (the ClusterJ writer has no `deleteOldest`).

## 4. What you gain

| Property | Ring only | TTL only | TTL ring |
|---|---|---|---|
| Rows per prefix bounded by N | yes | no | yes |
| Rows hidden after a fixed age | no | yes | yes |
| Stale rows of an inactive prefix reclaimed | no, N rows stay forever | yes, by the purger | yes, by the purger |
| Memory bounded even if the purger is down | yes | no | yes |
| Same writers and readers as today | yes | yes | yes |
| `deleteOldest` available | yes | n/a | no |
| Meta `count` equals live rows | yes | n/a | no, upper bound |

The two gains that matter most:

- **Freshness on a ring.** A ring alone keeps the last 100 events of a user who left a year
  ago, forever. With TTL those rows become invisible after a day and the purger frees them.
  For a table with millions of prefixes that is the difference between memory that grows with
  the number of prefixes ever seen and memory that follows the number of active prefixes.
- **A hard memory bound on a TTL table.** A TTL table alone can grow without limit if the
  writer outpaces the purger or the purger is stopped. On a TTL ring the ring overwrite keeps
  every prefix at N rows plus one meta row no matter what the purger does.

Everything around the table is unchanged: node restart, system restart, backup and
`ndb_restore`, replication, ClusterJ, the REST server, and the TTL purger itself.

## 5. What you trade

### 5.1 No `deleteOldest`

`deleteOldest(n)` removes n slots from the tail of a ring. On a TTL ring a tail slot may
hold an expired row or nothing at all, so "delete the oldest n rows" and "delete the oldest n
slots" no longer mean the same thing, and a consumer using it would double-process or skip
rows. Rather than ship a call with unclear semantics, v1 rejects it with a dedicated error on
TTL ring tables: NDB API error 4358, "deleteOldest is not supported on a ring buffer table
with TTL"; the call leaves the transaction untouched. Plain ring tables keep it. Consumers on
a TTL ring track their position with `ring_idx` and `total_inserts` instead.

### 5.2 The meta row's `count` is an upper bound

The meta row records `next_pos`, `count` and `total_inserts`. On a plain ring `count` is the
number of rows present. On a TTL ring the purger removes rows without touching the meta row,
so `count` becomes "how many slots have been written since the last wrap": an upper bound
on both physical and visible rows. `total_inserts` stays exact. If you need the number of
live rows, use `SELECT COUNT(*)` with the prefix. The purger does not maintain `count`
because doing so would need the meta row's lock on every purge, which would make purging and
inserting on the same prefix block each other.

### 5.3 TTL is decided at CREATE time

`ALTER TABLE` may change the TTL duration. It may not add TTL to an existing ring table and
may not remove it (`TTL=OFF`). Both are rejected with "Cannot enable/disable TTL on a ring
table; use DROP+CREATE"; the way to change them is to create a new table and copy the data.
The same rule holds for the raw NDB API's `alterTable` (error 741). Adding TTL later would leave existing meta rows with a
zero timestamp inside the TTL index, and removing it would make `deleteOldest` legal on a
ring that already has holes. The ring size cannot change either, which is an existing rule.
ALTER may move TTL to another column. That re-evaluates every row against the new column:
rows that had expired under the old column can become visible again.

### 5.4 Ring order and TTL order are independent

The ring evicts by insertion order. TTL hides by the value in the TTL column. If the TTL
column is the insertion time, as in the example, the two orders agree and the oldest row is
always the first to expire. If the TTL column holds something else, or you update it, a row
with a far-future TTL can be evicted by the ring before a row that is about to expire. That
is not a malfunction, both features keep their contracts, but it may not be what you want.
Recommendation: make the TTL column a `DEFAULT CURRENT_TIMESTAMP` column and do not update it
unless you intend exactly that effect. v1 does not enforce this.

### 5.5 Purge cost

The purger treats a TTL ring like any TTL table: one scan per table per round, one delete per
expired row. Costs per round per table:

| Situation | Cost |
|---|---|
| `ttl_index` present, nothing expired | one empty index range seek, about 0 ms |
| no `ttl_index`, nothing expired | full scan of prefixes x (N + 1) rows, 3 to 9 ms per 20k rows |
| expired rows present | one locked delete per row, plus REDO log and one binlog event each |

Two things to know:

- On a busy prefix the ring would soon overwrite the expired tail anyway. The purger deletes
  those rows regardless, so some purge work is redundant. It has no visible effect.
- Purge deletes are ordinary deletes: they are logged, replicated, and visible to binlog
  consumers as row deletes.

### 5.6 Brief waits on prefixes being purged

While the purger holds a batch of expired rows of a prefix, an INSERT into that prefix that
reaches one of those slots waits until the batch commits. Batches are small, so this is a
wait of milliseconds, and it only happens on prefixes that have expired rows, that is,
prefixes that are not busy. A transaction that inserts several rows into one prefix can, in
rare cases, form a lock cycle with the purge scan; the transaction deadlock timeout resolves
it and the transaction must be retried. Plain TTL tables have the same exposure today.

The reverse also happens on a table without `ttl_index`. The purge then scans the whole
table, and an INSERT transaction that is still open holds its prefix's meta row and new
slot; the purge waits for those locks, times out after the deadlock detection timeout, and
leaves that table for the next round. With `ttl_index` the purge visits only expired rows,
so an open INSERT does not block it. A user transaction that keeps an expired row locked has
the same effect on that row's table. In both cases the purger goes on with the other tables
and retries the blocked one every round; it does not stop or restart.

### 5.7 What you see in the meta row

With the session variable `ndb_ring_buffer_show_meta` set, SELECT returns the hidden meta
rows. On a TTL ring the meta row's TTL column shows the maximum value of its type, for
example `2038-01-19 03:14:07` for TIMESTAMP, instead of zero. The writers store that value so
the meta row's entry in `ttl_index` sorts after every real row and the purger never picks it
up. Each prefix therefore adds one entry to `ttl_index`, which matters only when sizing index
memory for tables with very many prefixes.

### 5.8 Rolling upgrade

A TTL ring table can only be created once every data node runs the new version; until then
the CREATE fails with "TTL ring buffer not supported by current data node versions".
Existing plain ring tables and plain TTL tables are unaffected by the upgrade.

## 6. Operational notes

- **The purger must run.** It is part of the REST server (rdrs2), as for every TTL table. Its
  status and per-table counters are available at the `/0.1.0/ttl-purge` endpoints.
- **Memory sizing.** Worst case per prefix is N data rows plus one meta row, independent of
  purger health. Typical usage is lower because the purger frees expired rows of inactive
  prefixes.
- **If the purger is down**, expired rows stay hidden but occupy memory until it comes back
  or the ring overwrites them. Nothing becomes visible that should not be.
- **Replication.** Purge deletes replicate as row deletes. The replica's own purger may have
  removed the row already; the applier ignores that, so both sides converge to the same rows.
  An insert that overwrites a slot is logged as a row write, not an update, so a replica
  whose purger already removed that slot re-creates it. Conflict functions (`NDB$MAX`,
  `NDB$EPOCH2` and the others in `mysql.ndb_replication`) are not supported on ring tables:
  CREATE TABLE fails with "Conflict functions are not supported on ring buffer tables". A
  ring table that such a row matches later (after RENAME TABLE, TRUNCATE, ALTER or a
  restart) is replicated without the conflict function, and mysqld logs a warning.
- **Backup and restore.** A backup contains expired rows that have not been purged yet, as it
  does for any TTL table. After restore they are hidden and the purger removes them.
- **Restarts.** Node and system restarts restore rows and meta rows as they were, including
  empty slots.

## 7. Questions we expect

**Can I opt a single table out of purging?** No. This is an existing limit of the purger, not
specific to ring tables. Without a purger the ring still bounds memory, see section 4.

**What happens to a row whose TTL column is NULL?** It never expires. It is evicted only when
the ring overwrites it. Same as on any TTL table.

**Does an expired but not yet purged row cost anything besides memory?** No. Reads skip it,
the ring overwrites it like any other slot, and `COUNT(*)` does not count it.

**Why not let the ring overwrite do all the reclaiming and skip the purger?** Because it never
reaches inactive prefixes. The purger is what frees the memory of prefixes that stopped
receiving inserts.

**Does the TTL column have to increase from one insert to the next?** No. See section 5.4 for
what happens when it does not.

**Can I use TTL ring tables from ClusterJ?** Yes, with the same `RingBufferWriter`. Every
insert must set every column (section 3.2); ring tables with BLOB/TEXT columns cannot be
written from ClusterJ. The ClusterJ writer has no `deleteOldest`.

## 8. Under the hood, briefly

Three small kernel changes make the combination safe. The ring meta row is exempt from the
TTL check, so its zero or maximum timestamp can never make it disappear. Deletes that are
restricted to expired rows, which is what the purger issues, are allowed through the guard
that otherwise blocks direct writes to ring tables. And the rule that rejected the combined
DDL is removed. The writers make three changes: they store the maximum timestamp in the meta
row's TTL column, they require every column on inserts into TTL rings, and they reject
`deleteOldest` on TTL rings. The purger treats a TTL ring like any TTL table; one general
change applies to every TTL table: when a purge scan times out waiting for a lock, the
purger skips that table until its next round instead of retrying it at once. The engineering design, the correctness arguments and the rejected alternatives are in
`design_ttl_ring_buffer_table.md`.
