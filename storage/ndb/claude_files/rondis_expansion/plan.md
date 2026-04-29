# Rondis feature-expansion plan — PR1/PR2/PR3 in small phases

## Context

Rondis currently supports 25 Redis commands. Three concrete gaps hurt
real-world users the most:

1. **Key introspection & TTL exposure.** Rondis stores `expiry_date`
   per string key but never exposes `TTL` / `EXPIRE` / `PERSIST`.
   Likewise `EXISTS` and `TYPE` — the two most basic key probes — are
   missing. Worse: `GET` does not filter expired keys today (expired
   rows are returned as-is), so once we expose TTL to users we must
   also honor it on read.
2. **Hash enumeration.** `HGETALL` (and its siblings `HKEYS` / `HVALS`
   / `HLEN`) is the dominant hash access pattern in real code; without
   it Rondis is usable only for narrow pre-known-field workloads.
   `HEXISTS`, `HSTRLEN`, `HSETNX` round out the set.
3. **Client-handshake niceties.** Several clients (node-redis, ioredis,
   redis-exporter, `redis-cli --stat`) issue `COMMAND` / `INFO` on
   connect and refuse to proceed without a reply. `APPEND`, `UNLINK`,
   `GETDEL`, `SETNX` / `SETEX` / `PSETEX` are short wrappers that
   appear in most Redis tutorials and are frequently the first commands
   a user tries.

All three groups have existing in-tree infrastructure we can reuse
(STRLEN is the template for single-row metadata reads; HGET is the
template for single-field reads; SET NX's interpreted-code guard is
the template for HSETNX; SETRANGE's interpreter program is the
template for APPEND). None of the three PRs require schema changes
or new NDB features — they stack on patterns already validated by the
existing commands.

Work is split into three separately-landable PRs. Within each PR,
**every new command gets its own phase** — small, compiles clean,
has a dedicated MTR test with recorded baseline. This follows the
same cadence as the RONDB-1052 test rollout and the RONDB-1052
bug-fix plan.

---

## Phase 0 — Land this plan in the tree

Before any code change, commit this plan at
`storage/ndb/claude_files/rondis_expansion/plan.md` so reviewers
have the same reference the implementation phases cite. One commit,
label `RONDB-1053`.

---

## PR 1 — Key introspection, TTL exposure, and expired-key filtering

Adds `EXISTS`, `TYPE`, `TTL`, `PTTL`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`,
`PEXPIREAT`, `PERSIST`, plus two correctness fixes:
1. **`hset_keys` is now the authoritative "does this hash exist"
   signal** — Phase 1.0 adds a `field_count` column and wires HSET
   and HDEL to maintain it so the row is removed when the last
   field is deleted (fixes the orphan-row bug documented in
   `rondis_namespace_split.test`).
2. **Expired keys are filtered out on read** once TTL is a
   first-class user-visible feature (Phase 1.11).

All commands operate on string keys and hash keys (look in both
`string_keys` and `hset_keys`). Shared infrastructure:

- **Read-only probes** reuse the STRLEN template
  (`rondb_strlen_command` @ `src/commands.cc:2198` → `prepare_get_simple_key_row`
  @ `src/db_operations.cc:973` with a metadata-only column mask).
- **Write-only mutations** of `expiry_date` reuse the INCR/DECR
  interpreted-code template (`incr_decr_key_row` @
  `src/db_operations.cc:1054`) — `writeTuple` with a selective mask
  updating only the expiry column.
- Existing helpers: `generate_expire_at` @ `src/commands.cc:284`
  converts `ttl` seconds to a stored `expiry_date` value with
  `g_max_expire_at` as the "no TTL" sentinel.

### Phase 1.0 — `hset_keys.field_count` and hash-lifecycle cleanup

**Why this is a prerequisite for EXISTS/TYPE:** today the
`hset_keys` row persists after `HDEL` of the last field (only fields
are removed from `string_keys`; the name→id mapping row leaks).
After Phase 1.0 the row is deleted when `field_count` hits 0, so
"row exists in `hset_keys`" becomes equivalent to "hash exists". Both
EXISTS and TYPE can then trust a two-probe design
(`string_keys(0, k)` + `hset_keys(k)`) without seeing ghost hashes.
It also turns Phase 2.4 (HLEN) from a table-scan into a PK read.

**Scope:** one schema change, plus four small maintenance phases.

#### Phase 1.0.1 — Schema: add `field_count` and `expiry_date` to `hset_keys`

Two columns, added together because both require the same schema
migration and downstream record/struct updates. The rationale for
`expiry_date` is that Redis's `EXPIRE` / `TTL` / `PERSIST` work on
hash keys as well as strings — and once a hash can expire, the
canonical place to store its TTL is on the hash-name row (same
pattern as `string_keys.expiry_date` for string keys).

**Files:**
- `sql/create_rondis_tables.sql` — add both columns to the
  `hset_keys` CREATE (in-place since the script uses
  `CREATE TABLE IF NOT EXISTS`):
  ```sql
  field_count INT UNSIGNED NOT NULL DEFAULT 0,
  expiry_date TIMESTAMP,
  KEY ttl_index(expiry_date),
  ```
  Mirror the `TTL=0@expiry_date` NDB table comment from `string_keys`
  so NDB lazy-expires the mapping row when its TTL passes.
- `sql/HSET_key.sql` — mirror the change for standalone schema
  install.
- `mysql-test/suite/rondis/include/create_rondis_tables.inc` — mirror.
- `include/table_definitions.h` — add `Uint32 field_count` and
  `Int32 expiry_date` to the `hset_key_table` struct; add
  `HSET_KEY_TABLE_COL_field_count` and
  `HSET_KEY_TABLE_COL_expiry_date` defines.
- `src/table_definitions.cc` `init_hset_key_records` — fetch both
  new columns via `tab->getColumn`; add them to
  `read_all_column_map` so `entire_hset_key_record` projects them.
  `pk_hset_key_record` stays unchanged (PK is still just `redis_key`).

**Hash-TTL semantics (applies to Phases 1.3/1.5-1.9/1.11):**
- `EXPIRE user:1 N` on a hash sets `hset_keys(user:1).expiry_date`.
  The field rows in `string_keys` keep whatever per-row
  `expiry_date` they had — they are *not* mass-updated. NDB's
  built-in `TTL=0@expiry_date` on `hset_keys` then lazy-drops the
  mapping row when it expires; any hash read that reaches an
  expired mapping row returns `nil` (Phase 1.11's read-filter
  covers this path explicitly).
- **Known limitation (flagged in the out-of-scope section):** when
  the `hset_keys` row expires, its field rows in `string_keys`
  become orphaned (unreachable by name, but still on disk). They
  are eventually reclaimed either by being overwritten under a
  freshly-allocated `redis_key_id` on a later HSET of the same
  name, or by a separate ops-level GC (out of scope for v1). A
  follow-up ticket can close this by having Phase 1.11's read
  path also issue a partitioned scan-delete for the orphaned
  field rows on first-observed hash expiry.

**Migration:** existing `redis_0.hset_keys` rows need `field_count`
backfilled — 1 is safe (at-least-one is the current invariant for a
non-orphaned row; true-count resync is a separate ops task). Do the
backfill in the CREATE script post-create via `UPDATE ... SET
field_count = 1 WHERE field_count = 0` when the column is freshly
added (idempotent on re-run). `expiry_date` defaults to NULL, which
we treat as "no TTL" (alongside the existing `g_max_expire_at`
sentinel convention); no backfill needed. Note both in the upgrade
notes.

**Test:** (no standalone test — verified by Phase 1.0.2-1.0.4 tests
and by Phase 1.5-1.9's EXPIRE-on-hash assertions).

#### Phase 1.0.2 — HSET atomically bumps `field_count` on new-field writes

**Goal:** every HSET that inserts a new field into `string_keys`
also bumps `hset_keys.field_count` **in the same NDB transaction**,
so a worker crash cannot leave the counter too low. If the write
ends up overwriting an existing field (UPDATE branch), no
`hset_keys` op is issued — the counter already reflects reality.

**Why this needs a transaction-flow refactor**

The existing simple-path HSET writes each field in its own
single-op transaction, committed in one round-trip via
`commit_simple_write_transaction` (`executeAsynchPrepare(Commit,
&simple_write_callback)`). That leaves no window to add a second
op on `hset_keys` to the same transaction.

The existing complex-path HSET (field values that overflow into
`string_values`) already demonstrates the multi-op-per-transaction
pattern we need:
1. `write_data_to_key_op(..., commit_flag=false)` stages op1 on
   `string_keys`.
2. `prepare_write_transaction` issues `executeAsynchPrepare(NoCommit,
   &write_callback)`.
3. `write_callback` (db_operations.cc:488) reads op1's interpreter
   outputs (`prev_num_rows`, `rondb_key`, `new_field` via
   `NdbRecAttr::u_64_value()`) — **mid-transaction**, same trans
   handle.
4. `send_next_write_batch` (commands.cc:796) inspects the callback-
   set state and either adds more ops to the same trans
   (`send_value_write` / `send_delete_write`) or commits it
   (`commit_write_value_transaction`).

Phase 1.0.2 extends that pattern to the simple path and slots a
conditional `hset_keys` op into both paths.

**Sub-phases**

Three commits, each independently bisectable:

##### Phase 1.0.2a — Unify simple and complex write paths into one NoCommit-first pipeline

With NoCommit as the first step, the current `set_simple_rows`
abort-and-retry pattern for RESTRICT_VALUE_ROWS_ERROR (6000 from
the simple interpreter) is no longer needed: a single transaction
can service both inline single-row writes and multi-row extension
writes. Merging the two paths also makes room for Phase 1.0.2b to
attach the `hset_keys.field_count` bump op atomically on the same
transaction. As a side benefit, multi-row HSET drops from 3+ NDB
round-trips (simple 6000 abort + complex reopen + ext writes +
commit) to 2+ (NoCommit + ext writes + commit). Inline HSET pays
1 extra RT (1 → 2), which we accept as the price of atomicity with
`hset_keys`.

Three-phase in-function state machine, one transaction per key,
all keys processed in parallel by the existing async NDB drain
loop:

- **Phase A** — NoCommit write on `string_keys` with the complex
  interpreter (`write_key_row_no_commit`) for every key. No
  `value_len > INLINE_VALUE_LEN` pre-filter. `write_callback`
  consumes the interpreter's OUTPUT_INDEX_0 (prev_num_rows),
  OUTPUT_INDEX_1 (rondb_key), and OUTPUT_INDEX_3 (new_field)
  and transitions the key to `MultiRowRWValue` (existing state —
  means "first write succeeded, trans still open, needs dispatch").
- **Phase B** — dispatch per key (mirrors today's
  `send_next_write_batch` logic): if `m_num_rows == 0 &&
  m_prev_num_rows == 0`, commit directly. If extension rows
  needed, issue them NoCommit. If old ext rows need deletion,
  issue deletes. Drain any deferred writes.
- **Phase C** — commit multi-row keys after their extension / delete
  ops complete. Drain.

Split into three bisectable sub-commits to keep each change small:

**Phase 1.0.2a.i** — Remove the pre-filter and route all keys through
the complex path. Concretely: in `set_simple_rows`, delete the
`if (value_len > INLINE_VALUE_LEN)` branch that pre-marks keys as
`MultiRow`; keep everything else as is. `set_complex_rows` now
receives no pre-filtered keys through this path — but set_simple_rows
is still reached first and handles the inline case on its own. Verify
existing tests pass. (This step on its own is slightly wasteful —
large values hit the 6000 abort + set_complex_rows reopen — but it's
a small, low-risk diff that makes the next step trivial.)

**Phase 1.0.2a.ii** — Switch set_simple_rows writes to
`commit_flag=false` + `prepare_write_transaction` + the existing
`write_callback`. After the NoCommit drain, set_simple_rows becomes
responsible for the dispatch loop (Phase B + Phase C). Either:
- inline `send_next_write_batch` inside set_simple_rows and run a
  second drain; OR
- fall through to `set_complex_rows` for the drain (since
  set_complex_rows already has the dispatch loop — but it expects
  keys in `MultiRow` state, which our NoCommit drain leaves them in
  `MultiRowRWValue`; a small change to set_complex_rows accepts
  either starting state).

The second form is smaller: we use set_complex_rows purely as the
tail drain, and set_simple_rows as the head submit. Validate: all
existing rondis tests pass. Simple interpreter (`write_key_row_commit`)
becomes dead code in this step but is not yet removed.

**Phase 1.0.2a.iii** — Merge the two functions. Delete
`set_complex_rows` as a separately-called entry point; inline its
dispatch loop into set_simple_rows (or rename set_simple_rows to
`set_rows`). Delete `write_key_row_commit` (simple interpreter),
`commit_simple_write_transaction` (replaced by
`commit_write_value_transaction` / in-flow commits), and the
`commit_flag` parameter on `write_data_to_key_op`. Drop
`RESTRICT_VALUE_ROWS_ERROR` from `common.h` if no other caller
uses it.

After 1.0.2a.iii lands, the HSET / MSET write pipeline is one
uniform state machine per key: startTransaction → writeTuple
(NoCommit) → callback (phaseA) → dispatch (commit OR ext writes)
→ optional ext drain → commit. Ready for 1.0.2b to slot the
`hset_keys` bump op into each commit site.

Validates after each sub-commit: `./mtr --suite=rondis` green.

##### Phase 1.0.2b — Insert the conditional `hset_keys` bump op

Adds the cross-table op and integrates it into both simple and
complex paths' pre-commit dispatchers.

- `src/interpreted_code.cc` — new
  `init_hset_field_count_bump_code(code, tab, delta)`: emits the
  tiny interpreter program `read field_count → add delta → write
  field_count → exit_ok`. Reuses `read_attr` / `load_const_u64`
  / `add_reg` / `write_attr` — primitives already used by
  `initNdbCodeIncrDecr` (interpreted_code.cc:44-115).
- `src/db_operations.cc` — new `add_hset_field_count_bump_op(
  trans, tab_hset, hash_name, hash_name_len, delta, database_id,
  response)`: appends an `interpretedUpdateTuple` on the **same**
  `NdbTransaction` as the field write, with
  `entire_hset_key_record[database_id]` as attr_rec and the
  bump interpreter code. No record-level mask needed — all
  updates go through `write_attr` inside the interpreter.
- `src/commands.cc` `set_simple_rows` Phase B dispatcher — before
  calling `commit_simple_write_transaction`, if the key is a
  hash field (`redis_key_id != STRING_REDIS_KEY_ID`) and state is
  `SimpleWriteDoneNewField`, call
  `add_hset_field_count_bump_op(..., delta=1, ...)` on the
  existing `m_trans`.
- `src/commands.cc` `send_next_write_batch` / `send_value_write` /
  `send_delete_write` (complex path pre-commit sites) — at each
  call to `commit_write_value_transaction`, if the key is a hash
  field and the callback-captured new_field was 1, insert the
  same `add_hset_field_count_bump_op` call before the commit.
  Use a KeyStorage flag (`m_was_new_field`) that
  `write_callback` sets when it reads OUTPUT_INDEX_3 == 1, so
  the pre-commit dispatcher doesn't have to consult the NdbRecAttr
  again.

Transaction-level correctness:
- INSERT branch (op1 writes new `string_keys` row, op2 bumps
  `hset_keys.field_count`) — commits as one atomic unit.
- UPDATE branch (op1 overwrites existing row, no op2) —
  `hset_keys.field_count` unchanged. Correct.
- Conditional-fail / error on op1 — transaction aborts before
  any op2 can be queued. Correct.

Atomicity: **guaranteed by NDB** — both ops live on the same
transaction, Commit is all-or-nothing.

##### Phase 1.0.2c — Test and record baseline

- `t/rondis_hash_lifecycle.test` phase 1 — HSET on a new hash:
  assert `field_count` equals N via `SELECT field_count FROM
  redis_0.hset_keys WHERE redis_key = ...`. HSET of overwrites:
  `field_count` unchanged. HSET mixing new + overwrite: counter
  goes up only by the new count. Also exercise the complex path
  by HSETing a field with value length > `INLINE_VALUE_LEN` so
  the hset_keys bump lands through `set_complex_rows`' pre-
  commit dispatcher.
- Re-record existing rondis_hash / rondis_hash_counters baselines
  only if their values actually change (they should not, since
  the user-visible HSET reply shape stays identical).

**Notes:**
- HMSET and HSETNX ride on the same `rondb_mset` infrastructure
  and will also maintain `field_count` correctly for free. No
  separate phase needed.
- The simple-path callback changes are small enough to be one
  commit with 1.0.2b, but keeping 1.0.2a as a standalone commit
  makes the refactor bisectable if something regresses in
  rondis_stress_*.

#### Phase 1.0.3 — HDEL decrements `field_count`; deletes row at 0

**Scope:** after an HDEL batch completes, compute
`deleted_count = num_keys - m_num_read_errors` (the existing "fields
actually deleted" derivation at `src/commands.cc:693`). If
`deleted_count > 0`, issue an interpreted-code UPDATE on
`hset_keys` that subtracts from `field_count`; if the new value is
0, immediately delete the `hset_keys` row and purge the in-memory
cache.

**Files:**
- `src/commands.cc` `rondb_hdel_command` — after `rondb_del`
  returns, call a new `decrement_hset_field_count` helper with the
  `deleted_count` derived from `get_ctrl`.
- `src/db_operations.cc` — new `decrement_hset_field_count(ndb,
  redis_key, delta, database_id, response)`:
  1. Interpreted-code UPDATE that reads `field_count`, subtracts
     `delta`, writes back, and emits the resulting value as
     `OUTPUT_INDEX_0`.
  2. If the emitted value is 0: issue a follow-up DELETE on
     `hset_keys` (PK = redis_key), and erase the in-memory
     `redis_key_id_hash` entry. Both ops can be folded into one
     transaction (delete + interpreted UPDATE in the same commit),
     but splitting is acceptable for v1.
- `src/db_operations.cc` — `redis_key_id_hash` accesses gain a
  `std::mutex` guard (see Phase 1.0.4).

**Test:** `t/rondis_hash_lifecycle.test` phase 2 — HSET 3 fields,
HDEL 1 (`field_count`=2 and row present), HDEL 2 more
(`field_count` reaches 0, row deleted, cache purged — verified via
SQL `SELECT COUNT(*) FROM redis_0.hset_keys WHERE redis_key =
...`). HDEL on missing fields does not decrement.

#### Phase 1.0.4 — Lock the `redis_key_id_hash` cache

**Scope:** Pre-existing latent bug — `redis_key_id_hash` at
`src/db_operations.cc:1196` is a global `std::unordered_map`
accessed from multiple worker threads without synchronization.
Phase 1.0.3 introduces erasures (previously only lookups +
insertions), which makes the race observable. Add an
`std::mutex` guard around all accesses.

**Files:**
- `src/db_operations.cc` — wrap find/insert/erase in a helper with
  `std::lock_guard<std::mutex>`.

**Test:** no dedicated test — covered by the existing rondis stress
tests under `--repeat=3` after 1.0.3 lands.

#### Phase 1.0.5 — HSET on an existing hash after cleanup

**Scope:** After 1.0.3, a hash that was fully `HDEL`'d returns to
"doesn't exist" state. A subsequent HSET must re-create the
`hset_keys` row cleanly — `rondb_get_redis_key_id` (@
`src/db_operations.cc:1197`) already handles this because its
cache was purged. Validate end-to-end.

**Files:** no code changes — just verification.

**Test:** `t/rondis_hash_lifecycle.test` phase 3 — HSET, full HDEL,
HSET on same name: verify `redis_key_id` may be reused or a fresh
id allocated; HGET works on the new fields; old hash's field rows
are not visible (if any stale rows in string_keys from the
pre-cleanup era existed, they would be under the old id and thus
unreachable via the name).

### Phase 1.1 — EXISTS

**Scope:** `EXISTS key [key ...]` — integer count of keys that exist.
Duplicates in the argument list count each time (per Redis canonical).

After Phase 1.0, the presence of an `hset_keys` row is authoritative
for hash existence — no ghost rows, no scan. EXISTS becomes two
batched HASH PK probes per key: first `string_keys(0, k)`, then
`hset_keys(k)` for anything that missed.

**Files:**
- `src/rondb.cc` — dispatcher entry (NDB-dependent block), arity `>= 2`
- `src/commands.cc` — new `rondb_exists_command`. Pass 1 reuses
  `rondb_get_func` with a new `m_probe_only = true` flag on
  `GetControl` (mask `0x10`, `num_rows` forced to 0, multi-row
  branch of `simple_read_callback` short-circuited). Pass 2 uses a
  new `exists_batch_probe_hset` helper — async batched PK reads on
  `hset_keys` with the same window size as pass 1 (the code for
  both passes is already drafted on the work branch).
- `include/table_definitions.h` — add `bool m_probe_only` to
  `GetControl`.
- `include/commands.h` — declaration.

**Reply shape:** `:N\r\n`.

**Test:** `t/rondis_keyinfo_exists.test` — existing string key,
existing hash key (verified correct after Phase 1.0 cleanup — so
fully HDEL'd hashes count as 0), missing key, mixed batch,
duplicates counted separately, case-sensitivity.

### Phase 1.2 — TYPE

**Scope:** `TYPE key` → `"string"` | `"hash"` | `"none"` (simple
string reply, not bulk).

**Files:**
- `src/rondb.cc` — dispatcher entry, arity `== 2`
- `src/commands.cc` — new `rondb_type_command`: single-key variant
  of the Phase 1.1 two-probe machinery. Emit `+string\r\n` if the
  first probe (`string_keys(0, k)`) hits, `+hash\r\n` if the second
  probe (`hset_keys(k)`) hits, `+none\r\n` otherwise. (Post-Phase
  1.0 `hset_keys` can no longer lie about an empty hash.)
- `include/commands.h` — declaration

**Reply shape:** `+string\r\n` / `+hash\r\n` / `+none\r\n`.

**Test:** `t/rondis_keyinfo_type.test` — all three outcomes; string
key and hash key with the same name each report their own type
(namespace-split per `rondis_namespace_split.test`); fully-HDEL'd
hash reports `none`.

### Phase 1.3 — TTL

**Scope:** `TTL key` → seconds remaining, `-1` if no TTL, `-2` if
missing. Also `-2` if row exists but `expiry_date < now` (already
expired — Phase 1.11 will also hide the row from GET/HGET, but
TTL's reply is decided here). Works on both string and hash keys.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 2`
- `src/commands.cc` — new `rondb_ttl_command(... bool millis)` shared
  with PTTL (Phase 1.4), `millis=false` here. Probe `string_keys(0,
  k).expiry_date` first (metadata-only mask that now also covers
  expiry); on miss, probe `hset_keys(k).expiry_date` — both reads
  reuse the Phase 1.1 two-probe machinery, just projecting one more
  column.
- `src/db_operations.cc` — extend metadata-only mask to include
  `expiry_date`; reuse `prepare_get_simple_key_row` unchanged. Add
  a parallel `prepare_get_simple_hset_key_row` variant that
  projects `field_count` and `expiry_date` from `hset_keys`.
- `include/commands.h` — declaration

**Test:** `t/rondis_keyinfo_ttl.test` — SET with EX then TTL
(string, in window), SET without EX (-1), TTL missing (-2); EXPIRE
on a hash then TTL on the hash-name (in window); TTL on a fully
HDEL'd hash (-2).

### Phase 1.4 — PTTL

**Scope:** `PTTL key` — same as TTL but milliseconds.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 2`
- `src/commands.cc` — same `rondb_ttl_command` shared helper with
  `millis=true`

**Test:** `t/rondis_keyinfo_pttl.test` — mirrors TTL but validates
reply is ~1000× larger and in-window. Skips absolute-value asserts;
uses range check.

### Phase 1.5 — EXPIRE

**Scope:** `EXPIRE key seconds` → `:1\r\n` if applied, `:0\r\n` if
missing. Works on both string and hash keys. Redis 7's NX/XX/GT/LT
flags deferred (noted in the follow-ups section).

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 3`
- `src/commands.cc` — new `rondb_expire_command(... ExpireMode)` where
  `ExpireMode ∈ {EX, PX, EXAT, PXAT}` drives the ttl-to-epoch
  conversion. Rejects `seconds <= 0` per C12 precedent. Dispatch:
  probe `string_keys(0, k)` first — if hit, update its
  `expiry_date`; else probe `hset_keys(k)` — if hit, update its
  `expiry_date`; else `:0`.
- `src/db_operations.cc` — two helpers:
  - `update_expiry_string_row` modelled on `incr_decr_key_row` @
    `:1054` — `writeTuple` on `string_keys` with mask covering only
    PK + `expiry_date`.
  - `update_expiry_hset_row` — same pattern against `hset_keys`
    (`entire_hset_key_record` now projects `expiry_date` after
    Phase 1.0.1).
- `src/interpreted_code.cc` — a short interpreter program: load new
  expiry, write_attr, exit_ok; used by both helpers. On missing row
  the write naturally fails and the caller returns `:0`.
- `include/commands.h`, `include/db_operations.h` — declarations

**Reuses:** `generate_expire_at` @ `commands.cc:284`.

**Test:** `t/rondis_keyinfo_expire.test` — EXPIRE on existing
string key (returns 1, TTL reflects it), EXPIRE on existing hash
key (returns 1, TTL on hash-name reflects it), EXPIRE on missing
(returns 0), EXPIRE 0 / EXPIRE -1 rejected.

### Phase 1.6 — PEXPIRE

**Scope:** `PEXPIRE key milliseconds` — millisecond precision
variant of EXPIRE.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 3`
- `src/commands.cc` — call `rondb_expire_command` with `ExpireMode=PX`;
  add a `generate_expire_at_ms` helper alongside `generate_expire_at`
  that converts millis to the stored-epoch second-granularity
  expiry_date (round-up, so the caller's TTL is not truncated to 0)

**Test:** `t/rondis_keyinfo_pexpire.test` — PEXPIRE 1500 → TTL
reads ~2s, PEXPIRE 0 rejected.

### Phase 1.7 — EXPIREAT

**Scope:** `EXPIREAT key unix-timestamp-seconds` — absolute-time
variant. Reply semantics identical to EXPIRE.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 3`
- `src/commands.cc` — call `rondb_expire_command` with
  `ExpireMode=EXAT`. Input is already a stored-epoch value; just
  validate range and pass through.

**Test:** `t/rondis_keyinfo_expireat.test` — timestamp in future
(applied), timestamp in past (applied, subsequent GET nil per
Phase 1.10), timestamp = 0 rejected.

### Phase 1.8 — PEXPIREAT

**Scope:** `PEXPIREAT key unix-timestamp-millis`.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 3`
- `src/commands.cc` — call `rondb_expire_command` with `PXAT`;
  divides by 1000 (round-up) before storing

**Test:** `t/rondis_keyinfo_pexpireat.test` — mirrors EXPIREAT with
millisecond input.

### Phase 1.9 — PERSIST

**Scope:** `PERSIST key` → `:1\r\n` if TTL cleared, `:0\r\n` if
missing or already had no TTL. Works on both string and hash keys.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 2`
- `src/commands.cc` — new `rondb_persist_command`: probe+dispatch
  pattern from Phase 1.5. For whichever table holds the key,
  pre-read `expiry_date` to decide the 1-vs-0 reply, then call the
  appropriate `update_expiry_*_row` helper with
  `expiry_date = g_max_expire_at`.

**Test:** `t/rondis_keyinfo_persist.test` — SET with EX, PERSIST
(returns 1), TTL (-1), PERSIST again (returns 0), PERSIST missing
(returns 0); EXPIRE on hash then PERSIST on hash (returns 1, TTL
on hash -1).

### Phase 1.10 — Expired-key filtering on read

**Scope:** Correctness fix. Once TTL is a user-visible feature, GET
/ MGET / HGET / HMGET / STRLEN / GETRANGE / EXISTS / TYPE must treat
rows where `expiry_date < now` as absent. A still-present expired
row is an eventual-consistency artifact of lazy expiry and must not
leak back to the client. Covers both `string_keys.expiry_date`
(per-key string or per-field hash TTL) and `hset_keys.expiry_date`
(whole-hash TTL set by EXPIRE on the hash name).

**Files:**
- `src/interpreted_code.cc` — extend the read interpreter program
  (the one generated alongside `prepare_get_simple_key_row`) with
  a branch that compares `expiry_date` against `now` and emits
  `interpret_exit_nok(RONDB_EXPIRED_KEY)` (new sentinel) if
  expired. Add the same branch to the hset_keys read program used
  by Phase 1.1's pass 2.
- `src/common.h` — define `RONDB_EXPIRED_KEY` sentinel
- `src/db_operations.cc` — in the read callbacks, translate the
  sentinel to `KeyState::CompletedFailed` with a code that the
  reply path already treats as "key does not exist" (i.e. emits
  `$-1\r\n` / counts as miss). Mirrors C5a/C5b/C7 plumbing. Also:
  the `rondb_get_redis_key_id` path (used by HGET/HMGET/HSET/HDEL)
  must honor expiry on `hset_keys` — an expired mapping row must
  look like "hash not found" rather than yielding a stale
  `redis_key_id`.
- `src/commands.cc` — EXISTS counting and TYPE probes must also
  honor the sentinel (their reply builders are new in Phase
  1.1/1.2 so this just wires in the translation).

**Test:** `t/rondis_keyinfo_expired_read.test` — SET with EX 1, SQL-
clocked sleep until `expiry_date < now` (reuse the `rondis_ttl.test`
deterministic pattern, not `sleep 2`), then GET returns nil, EXISTS
returns 0, TYPE returns none. Hash variant: HSET fields + EXPIRE
on the hash name, sleep past expiry, then HGET field returns nil,
HLEN returns 0, EXISTS returns 0.

**Note:** This phase depends on Phases 1.0/1.1/1.2/1.3/1.5 having
landed so the translation has call sites to plumb through. Lands
as the capstone of PR 1.

### Status snapshot — Phase 1.10 split + 1.10c namespace unification

The original Phase 1.10 (expired-key filtering on read) was split into
two landing-sized phases during implementation, plus a follow-up
`1.10c` group that extends the design to *type* unification on top of
expiry. State as of `2026-04-29`:

- **Phase 1.10a — probe-side expiry filter (DONE).** EXISTS / TYPE /
  EXPIRE / PERSIST now treat rows with `expiry_date < now` as absent.
  Read interpreter gained a `RONDB_EXPIRED_KEY` branch that the
  callbacks translate to `CompletedFailed` with a code the reply
  builder treats as "miss".
- **Phase 1.10b — read-path expiry filter (DONE,
  commit `3f3777bf0e6`).** GET / MGET / STRLEN / GETRANGE / HGET /
  HMGET went through the same translation. Closed the eventual-
  consistency leak documented in Phase 1.10's original scope.

#### Phase 1.10c — namespace unification (in progress)

A separate correctness gap surfaced once 1.10b landed: a name like
`mykey` could simultaneously live as a string row in `string_keys`
*and* as a hash row in `hset_keys`, with each command path picking
the row of its own type. Real Redis disallows this — a key has
exactly one type at a time. The 1.10c sub-phases collapse the two
namespaces into one by making `hset_keys` the authoritative
name-registry for *both* string and hash rows; the type
discriminator is `redis_key_id IS NULL` (string) vs non-null (hash).

The schema change for this lands in 1.10c.1:

```sql
-- before:
redis_key_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
-- after (rondis is alpha; no migration):
redis_key_id BIGINT UNSIGNED NULL DEFAULT NULL,
```

`redis_key_id` deliberately remains the indexed ownership column:
`NULL` means a string owns the name and a non-NULL value means a hash
owns it. It must **not** remain `AUTO_INCREMENT`, because writing
`NULL` to an auto-increment column is allocation input in MySQL/NDB
semantics rather than a reliable stored discriminator. Hash id
generation moves to a separate one-column sequence table:

```sql
CREATE TABLE redis_0.hset_key_id_sequence (
  redis_key_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (redis_key_id) USING HASH
) ENGINE NDB;
```

The `NULL` change is also load-bearing for concurrency: the original
plan implicitly stored `redis_key_id = 0` for strings, and `UNIQUE
KEY (redis_key_id)` then serialized concurrent string-INSERTs on the
unique-index entry for `0`, producing cross-trans 266 lock-wait
timeouts under MSET / parallel-worker SET. Multiple `NULL` entries
in a UNIQUE index are allowed, which removes the contention. Keeping
string registry rows at `NULL` also avoids updating the unique index
for SET / MSET operations; only hash rows write a generated non-NULL
id into that unique index.

Sub-phases (each its own commit, separately bisectable):

- **1.10c.1 — SET / MSET dual-write claim (WIP, commit
  `86afca50e32`).** SET / MSET issue an additional `writeTuple` on
  `hset_keys(key)` in the same NDB trans as the `string_keys` write.
  An interpreter program rejects the trans with `RONDB_WRONGTYPE`
  (= 6010) when the existing `hset_keys` row carries a non-null
  `redis_key_id` (i.e. the name is owned by a hash). On INSERT, the
  row buffer + `null_bits = 0x3` + `mask = 0xF` writes
  `redis_key_id = NULL`, `field_count = 0`, `expiry_date = NULL`.
  HSET no longer calls `getAutoIncrementValue` on `hset_keys`;
  it preallocates from `hset_key_id_sequence` and writes that
  explicit id into `hset_keys.redis_key_id` only for hash-owned
  rows. Phase 1 (HSET) and HDEL Phase 1 callbacks map the read-back
  NULL back to `0` so the existing `m_hset_redis_key_id == 0`
  "string row" check downstream stays valid. **Open issue:** the
  current WIP still has `redis_key_id ... AUTO_INCREMENT`, so
  rondis_basic Test 3 (SET overwrite) trips WRONGTYPE because the
  first SET can allocate a non-NULL `redis_key_id`. Fix by removing
  AUTO_INCREMENT from `hset_keys.redis_key_id`, adding the sequence
  table, and switching HSET preallocation to that table.
- **1.10c.2 — DEL deletes both rows + supports hash DEL (PENDING).**
  String DEL also drops the `hset_keys` row; hash DEL routes through
  `hset_keys` first.
- **1.10c.3 — HSET / HMSET WRONGTYPE on existing string (PENDING,
  partly already in tree).** The check at `set_rows_hset` Phase 1
  (`commands.cc:2039`) already exists; this phase will add the
  matching MTR coverage.
- **1.10c.4 — EXISTS / TYPE single-probe (PENDING).** With unified
  namespace, both can read `hset_keys` alone instead of probing
  both tables.
- **1.10c.5 — TTL / EXPIRE / PERSIST single-probe (PENDING).**
  Same simplification for the TTL family.
- **1.10c.6 — Remove `redis_key_id_hash` cache (DONE,
  commit `a90a5aa3389`).**
  Once the `hset_keys` row is the authoritative type registry, the
  process-local cache becomes a hazard (no cross-server invalidation,
  and no safe invalidation point for DEL / type replacement). Stripped
  before silent replace lands; every hash read resolves through
  `hset_keys`.

  Implementation:
  - `rondb_get_redis_key_id()` stops allocating ids and stops writing
    `hset_keys` rows for read paths. It becomes a lookup/read helper:
    existing hash row with `redis_key_id IS NOT NULL` and
    `field_count > 0` returns the id; missing row, string row
    (`redis_key_id IS NULL`), or empty hash row (`field_count == 0`)
    returns "not found" to the caller.
  - HSET remains the only read/write path that allocates a fresh hash
    id from `hset_key_id_sequence`; HDEL keeps the persistent empty
    hash row while `field_count == 0`.
  - HGET / HMGET / HINCR* callers map "not found" to Redis nil / 0
    semantics without creating any durable row.

  Test coverage:
  - `HGET no_such f; DEL no_such` returns `0` and leaves no
    `hset_keys` row.
  - `HSET h f v; HGET h f; DEL h; HGET h f` returns nil after DEL,
    proving a warmed same-process read cannot see orphan field rows.
  - `HSET h f v; HDEL h f; TYPE h` remains `none` while the empty
    registry row can still persist internally.

- **1.10c.6b — HINCR / HDECR field-count repair (DONE).**
  Cache removal made `hset_keys.field_count` authoritative for hash
  visibility. HINCR / HINCRBY / HDECR / HDECRBY already write the
  field row through the generic INCR interpreter, but a missing-field
  INSERT must bump `hset_keys.field_count`. Without that bump, the
  hash becomes invisible after a later HDEL can decrement the count
  to zero even when other fields still exist.

  Implementation:
  - Detect whether the counter operation inserted a new hash field
    versus updated an existing field. Reuse the same interpreter
    output shape used by HSET (`OUTPUT_INDEX_3` / new-field flag) or
    add an equivalent final-value output to the INCR path.
  - For hash counters only (`redis_key_id != 0`), if a new field was
    inserted, update `hset_keys.field_count` in the same transaction
    as the counter write. String INCR / DECR continue to only claim
    the string namespace row in `hset_keys`.
  - Preserve current cache-removal semantics: HINCR* / HDECR* on a
    never-existed hash still returns `0` and creates no row; the
    field-count repair applies only after the hash row is already
    visible (`field_count > 0`).

  Test coverage:
  - `HSET h anchor 0; HINCR h f; HDEL h f; TYPE h` returns `hash`,
    because the anchor field keeps `field_count > 0`.
  - Same coverage for `HINCRBY`, `HDECR`, and `HDECRBY`.
  - `rondis_stress_hashes` should not need extra per-counter anchor
    resets once this is fixed; a single anchor before the counter
    section is enough.

- **1.10c.7 — Redis-canonical silent replace (IN PROGRESS;
  HSET-on-string done at 1.10c.7a, SET-on-hash + combined
  test still pending).** Real Redis SET on a hash silently drops
  the hash; HSET on a string silently drops the string. Replace
  1.10c.1's WRONGTYPE intermediate behavior with the
  Redis-canonical drop semantics. This phase runs after 1.10c.6
  so all reads consult authoritative `hset_keys` state instead of
  process-local cached hash ids.

  Both directions land **fully in-trans** (atomic with the type
  flip). Detection is free — each path's existing Phase-1
  interpreter already branches on the type discriminator
  (`redis_key_id IS NULL` vs not), so the only added cost is the
  cleanup work itself, which only runs on collision. NDB
  transactions can carry the work of either direction in the
  same trans as the type flip; the transaction-size limit is the
  only ceiling. For v1 we accept "trans grew too big" as a
  command-level failure (rare in practice — the worst case is
  SET on a hash with millions of fields); a future bounded-batch
  variant is out of scope.

  Sub-phases (one commit each):

  - **1.10c.7a — HSET-on-string silent replace (DONE,
    commit `9a81f36d70c`).**
    `init_hset_lock_claim_code` gained a third interpreter output
    (`OUTPUT_INDEX_2 = was-string-flag`). The UPDATE-on-string
    branch flips from "emit `(0, 0)`" to "write
    `redis_key_id = prealloc_id`, `field_count = 0`; emit
    `(prealloc_id, 0, 1)`". UPDATE-on-hash and INSERT branches
    emit `OUTPUT_INDEX_2 = 0`. `add_hset_lock_claim_op` registers
    a 3rd `GetValueSpec`; `hset_phase1_callback` captures the
    flag onto `GetControl::m_hset_was_string_replaced`.
    `set_rows_hset` drops the WRONGTYPE check; on
    `m_hset_was_string_replaced` it issues a Phase 1.5 on the
    same trans:
    1. NoCommit pass A: deleteTuple-with-readback on
       `string_keys(0, name)` capturing `rondb_key` +
       `num_rows` (mask 0x34, same projection as
       `prepare_complex_delete_row`).
    2. NoCommit pass B (only when `num_rows > 0`): per-ordinal
       deleteTuple on `string_values` for ordinals
       `0..num_rows-1` keyed by the captured `rondb_key`.
    3. Phase 2 (existing): field writes proceed. The new field
       rows live at `(prealloc_id, field_name)`, disjoint from
       the deleted string row's PK `(0, name)`, so no conflict.

    Existing tests `rondis_namespace_split`,
    `rondis_keyinfo_namespace_unified`, `rondis_keyinfo_type` were
    rewritten to assert silent-replace shape instead of WRONGTYPE
    on HSET-on-string; `rondis_keyinfo_hset_wrongtype` deleted.

  - **1.10c.7b — SET-on-hash silent replace.**
    This is still required after 1.10c.7a; current behavior remains
    WRONGTYPE for SET on a hash-owned name.
    `init_hset_string_claim_code` (interpreted_code.cc:202)
    drops `interpret_exit_nok(RONDB_WRONGTYPE)`. The UPDATE-on-
    hash branch reads existing `redis_key_id` and `field_count`,
    emits them as `OUTPUT_INDEX_0/1`, and exits OK. The
    writeTuple's row buffer + `mask` already overwrites
    `redis_key_id` back to NULL (string ownership) — the
    interpreter just publishes the old values for the caller
    to act on. `add_hset_string_claim_op` registers a 2nd
    `GetValueSpec` for `OUTPUT_INDEX_1`; `write_callback`
    captures both onto `KeyStorage` (per-key trans, so per-key
    storage is fine). The dispatcher (`set_simple_rows` Phase D
    / equivalent) then, when the captured `old_id != 0`, queues
    a Phase 1.5 partitioned scan on `string_keys` filtered by
    `redis_key_id = old_id`, with `LM_Exclusive` and
    `takeOverScanOpForDelete` per row, plus per-ordinal
    deleteTuple on `string_values` for any field with
    `num_rows > 0` (`rondb_key` and `num_rows` come back in the
    scan projection). `RONDB_WRONGTYPE` translation in
    `write_callback` and the `CompletedTypeError` state become
    dead; remove them.

    Required tests:
    - `HSET k f v; SET k string` returns `OK`, `GET k` returns
      `string`, `HGET k f` returns nil, and `TYPE k` returns
      `string`.
    - SQL confirms all old hash field rows under the replaced
      `redis_key_id` are gone, including large field values with
      `string_values` extension rows.
    - SET-on-hash with the `GET` option and TTL variants (`EX`,
      `PX`, `KEEPTTL` where applicable) behaves consistently with
      the normal string SET path and does not leave stale hash rows.

  - **1.10c.7c — Test coverage.**
    `t/rondis_keyinfo_silent_replace.test`:
    1. `SET k "string"`; `HSET k f v` → returns `1`; `GET k` →
       `(nil)`; `HGET k f` → `v`; `TYPE k` → `hash`; the old
       `string_keys(0, k)` row and any value-extension rows
       are gone.
    2. `HSET k f v`; `SET k "string"` → returns `OK`; `GET k`
       → `string`; `HGET k f` → `(nil)`; `TYPE k` → `string`;
       the old hash field rows under `redis_key_id = old_id`
       are gone (`SELECT COUNT(*) FROM string_keys WHERE
       redis_key_id = old_id` → 0).
    3. Round-trip: SET → HSET → SET → HSET on the same name;
       each transition is silent.
    4. SET-on-hash with a 5000-byte value (forces ext-row
       writes on the new string side) over a hash with several
       fields, at least one of which has a 5000-byte value
       (forces ext-row deletes on the old hash side); both
       sides clean up correctly.
    5. The new hash from (1) and the new string from (2)
       interact correctly with EXPIRE / PERSIST / DEL.

  Open question deferred: the trans-size ceiling on (1.10c.7b).
  An HSET-then-SET on a hash with millions of field rows will
  exceed NDB's per-trans op buffer and surface as a generic NDB
  error. Acceptable for v1 (alpha tool, plan.md's "no SLA"
  posture). Follow-up ticket: bounded-batch silent replace that
  drains in chunks.

The 1.10c group preserves the empty-hash registry invariant
documented in `feedback_hset_keys_row_persistent.md`: HDEL of the
last field never deletes the `hset_keys` row, so a freshly-empty
hash keeps its `redis_key_id` internally. 1.10c.6 removes the
process-local dependency on that id, and 1.10c.7's silent-replace
only triggers when the *owning type* of the name flips, never on
within-type field churn.

### Phase 1.11 — PR 1 final

- Record all ten `.result` baselines with `./mtr --record`.
- Run `./mtr --suite=rondis --repeat=3` on both debug and prod builds.
- One final commit with label `RONDB-1053`.

---

## PR 2 — Hash enumeration

Adds `HEXISTS`, `HSTRLEN`, `HSETNX`, `HLEN`, `HKEYS`, `HVALS`,
`HGETALL`.

Hash fields live as rows in `string_keys` keyed by `(redis_key_id,
field_name)` — the hash's own `redis_key_id > 0` is obtained via
`rondb_get_redis_key_id` against `hset_keys`. So:

- `HEXISTS` / `HSTRLEN` are single-row PK reads, exactly like HGET.
- `HSETNX` is a field-level write with the same NX conditional-store
  guard as SET NX (C7 plumbing already in place).
- `HLEN` / `HKEYS` / `HVALS` / `HGETALL` need a **scan** over
  `string_keys` filtered by `redis_key_id = X`. Current PKs are
  HASH-only, so this is a partitioned NDB table scan with an equality
  filter pushed via `NdbScanFilter` — efficient because
  `redis_key_id` is the first PK column and hence the partition key,
  so the scan is partition-local.

### Phase 2.1 — HEXISTS

**Scope:** `HEXISTS key field` → `:1\r\n` / `:0\r\n`.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 3`
- `src/commands.cc` — new `rondb_hexists_command` — clone
  `rondb_hget_command` @ `:1909`, metadata-only mask, integer reply

**Test:** `t/rondis_hash_exists.test` — existing field, missing
field, missing hash.

### Phase 2.2 — HSTRLEN

**Scope:** `HSTRLEN key field` → `:N\r\n` (value length, 0 if missing).

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 3`
- `src/commands.cc` — new `rondb_hstrlen_command` — clone STRLEN @
  `:2198`, route via `rondb_get_redis_key_id` first, read
  `tot_value_len`

**Test:** `t/rondis_hash_strlen.test` — inline-length field,
overflow field, missing field, missing hash.

### Phase 2.3 — HSETNX

**Scope:** `HSETNX key field value` → `:1\r\n` if new, `:0\r\n` if
field already existed.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 4`
- `src/commands.cc` — new `rondb_hsetnx_command` — reuses
  `rondb_mset` write path with `IsInsert` set_type and a new flag
  that tells the reply builder to emit `:0\r\n` instead of `$-1\r\n`
  on the `CompletedConditionalFail` path
- No new interpreted-code work — SET NX's program already emits
  `interpret_exit_nok(RONDB_CONDITIONAL_STORE_NOT_MET)` on duplicate

**Test:** `t/rondis_hash_setnx.test` — new field (1 + visible),
existing field (0 + unchanged).

### Phase 2.4 — HLEN

**Scope:** `HLEN key` → `:N\r\n` (field count, 0 if hash missing).

After Phase 1.0 this is a **single PK read on `hset_keys`** —
`field_count` is the authoritative count. Not a scan.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 2`
- `src/commands.cc` — new `rondb_hlen_command` — PK read on
  `hset_keys` by name using `pk_hset_key_record` /
  `entire_hset_key_record`; return `:field_count\r\n` on hit,
  `:0\r\n` on miss.

(The "first scan path" role originally pinned on Phase 2.4 moves
to Phase 2.5 — HKEYS is now the first command that actually needs
a table scan.)

**Test:** `t/rondis_hash_len.test` — empty hash (0 — verifies
Phase 1.0 cleanup emitted a `DELETE` rather than a 0-count row),
1-field, 10-field, missing hash.

### Phase 2.5 — HKEYS (first scan path)

**Scope:** `HKEYS key` → `*N\r\n` + N bulk strings (field names).

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 2`
- `src/commands.cc` — new `rondb_hkeys_command`
- `src/db_operations.cc` — new `scan_hash_fields` — partitioned
  NDB table scan on `string_keys` with
  `NdbScanFilter::cmp(Equal, redis_key_id_col, hash_id)`; project
  `redis_key` (= field name) per row. First scan-based command in
  Rondis; validates the scan-with-filter pattern before Phases 2.6
  and 2.7 layer value projection on top.

**Test:** `t/rondis_hash_keys.test` — populated hash (order is
unspecified per Redis spec; use `--sorted_result`), empty hash (`*0`),
missing hash (`*0`).

### Phase 2.6 — HVALS

**Scope:** `HVALS key` → `*N\r\n` + N bulk strings (values).

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 2`
- `src/commands.cc` — new `rondb_hvals_command`: reuses
  `scan_hash_fields` from Phase 2.5 but projects `value_start` (inline
  value) and `num_rows` (to detect overflow). For fields with
  `num_rows > 0`, defer to the existing extension-row read path per
  field — slow path documented as a follow-up for batching.

**Test:** `t/rondis_hash_vals.test` — inline-only fields, mixed
inline + overflow (validates extension-row path), empty, missing.

### Phase 2.7 — HGETALL

**Scope:** `HGETALL key` → `*2N\r\n` + 2N bulk strings interleaved
field, value.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 2`
- `src/commands.cc` — new `rondb_hgetall_command`: reuses
  `scan_hash_fields` from Phase 2.5/2.6, projects both field name and
  value, assembles the interleaved reply

**Test:** `t/rondis_hash_getall.test` — populated hash
(`--sorted_result` since order unspecified), empty, missing.

### Phase 2.8 — PR 2 final

- Record `.result` baselines.
- Run `./mtr --suite=rondis --repeat=3`.
- Commit with label `RONDB-1054`.

**Documented limitation:** v1 issues one extra round-trip per
overflow-sized field in HVALS/HGETALL. For typical small-value hashes
this is invisible. Follow-up ticket: batch extension-row reads across
fields.

---

## PR 3 — Client-handshake niceties

Adds `UNLINK`, `SETNX`, `SETEX`, `PSETEX`, `APPEND`, `GETDEL`,
`COMMAND`, `COMMAND COUNT`, `COMMAND DOCS`, `INFO`.

### Phase 3.1 — UNLINK

**Scope:** `UNLINK key [key ...]` — literal alias for DEL. Redis's
async-delete semantics don't apply on RonDB; functional alias is
correct per Redis spec.

**Files:**
- `src/rondb.cc` — dispatcher shares `rondb_del_command` path with
  DEL (case-insensitive compare against both names)

**Test:** `t/rondis_unlink.test` — multi-key, missing keys counted
correctly, same reply shape as DEL.

### Phase 3.2 — SETNX

**Scope:** `SETNX key value` → `:1\r\n` if set, `:0\r\n` if already
existed. Integer-reply variant of `SET ... NX`.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 3`
- `src/commands.cc` — new `rondb_setnx_command` — builds an inner
  argv `{SET, key, value, NX}` and calls `rondb_set_command`; then
  rewrites the reply based on `CompletedConditionalFail` (→ `:0\r\n`)
  vs `CompletedSuccess` (→ `:1\r\n`) rather than the bulk/OK reply
  SET emits

**Test:** `t/rondis_setnx.test` — new key, existing key, no TTL
flags accepted.

### Phase 3.3 — SETEX

**Scope:** `SETEX key seconds value` → `+OK\r\n`. Thin wrapper
equivalent to `SET key value EX seconds`.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 4`
- `src/commands.cc` — new `rondb_setex_command` — builds inner argv
  `{SET, key, value, EX, seconds}` and calls `rondb_set_command`;
  reply is the SET reply unchanged

**Test:** `t/rondis_setex.test` — basic, SETEX 0 rejected (per C12),
TTL reflected after.

### Phase 3.4 — PSETEX

**Scope:** `PSETEX key millis value` → `+OK\r\n`.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 4`
- `src/commands.cc` — new `rondb_psetex_command` — same pattern as
  SETEX with `PX` flag

**Test:** `t/rondis_psetex.test` — basic, PSETEX 0 rejected.

### Phase 3.5 — APPEND

**Scope:** `APPEND key value` → `:N\r\n` (new length). Creates key
if missing.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 3`
- `src/commands.cc` — new `rondb_append_command`: reads current
  `tot_value_len` (0 if missing), invokes the SETRANGE write path @
  `:2355` with `offset = tot_value_len`. Reply is the SETRANGE reply
  (new length, same shape).

**Test:** `t/rondis_append.test` — append to missing (creates,
returns len), append to existing (returns sum), append to
overflow-size (validates extension-row update).

### Phase 3.6 — GETDEL

**Scope:** `GETDEL key` → old value (bulk), or nil if missing.
Removes the key.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 2`
- `src/commands.cc` — new `rondb_getdel_command`: two-roundtrip —
  `rondb_get_func` (capture), then `rondb_del_command` (skip if nil).
  Single-transaction fold is C9-adjacent risk not taken here.

**Test:** `t/rondis_getdel.test` — existing key, missing key,
repeated GETDEL.

### Phase 3.7 — COMMAND

**Scope:** `COMMAND [INFO [cmd ...]]` → `*0\r\n` (empty array).
Satisfies clients that probe but don't parse the reply contents.

**Files:**
- `src/rondb.cc` — non-NDB block (alongside CONFIG), arity `>= 1`

**Test:** `t/rondis_command_probe.test` — bare COMMAND, COMMAND INFO
with and without cmd names.

### Phase 3.8 — COMMAND COUNT

**Scope:** `COMMAND COUNT` → `:N\r\n` where N is a hardcoded constant
matching the dispatcher entry count (updated when new commands land).

**Files:**
- `src/rondb.cc` — extend the COMMAND handler to dispatch on
  subcommand. The constant lives next to the dispatcher for
  co-location.

**Test:** extend `rondis_command_probe.test` — value is an integer
greater than 25; exact value pinned in baseline (updated whenever
commands are added).

### Phase 3.9 — COMMAND DOCS

**Scope:** `COMMAND DOCS [cmd ...]` → `*0\r\n` (empty array). Full
doc array out of scope for v1.

**Files:**
- `src/rondb.cc` — extend the COMMAND handler with DOCS branch

**Test:** extend `rondis_command_probe.test` — COMMAND DOCS,
COMMAND DOCS GET (any specific cmd) both return empty array.

### Phase 3.10 — INFO

**Scope:** `INFO [section]` → bulk string with a minimal set of fields
that satisfy `redis-cli INFO`, `redis-cli --stat`, and basic
monitoring tools.

**Files:**
- `src/rondb.cc` — non-NDB block. Build a `# Server` section bulk
  string with `redis_version:<MYSQL_SERVER_VERSION>`,
  `process_id:<getpid()>`, `tcp_port:<g_rondis_port>`. If section
  argument is given, return only that section or an empty bulk for
  unknown. Version source: `MYSQL_SERVER_VERSION` from
  `include/mysql_version.h`.

**Test:** `t/rondis_info.test` — bare INFO (masks process_id,
version via `--replace_regex`), INFO server, INFO unknown (empty
bulk).

### Phase 3.11 — PR 3 final

- Record `.result` baselines.
- Run `./mtr --suite=rondis --repeat=3`.
- Commit with label `RONDB-1055`.

---

## Verification

After every phase:

1. Rebuild: `cd debug_build && make -j$(nproc) rondis`
2. Run the affected MTR test alone:
   `cd debug_build/mysql-test && ./mtr --suite=rondis <test_name>`

At PR boundaries:

3. `./mtr --suite=rondis --repeat=3` on debug build
4. After PR 3, also full repeat on prod build to catch any
   debug-only guards masking a real issue

All baselines must be deterministic — no hostname, no absolute path,
no fresh timestamps. Use the `--let $r = \`SELECT ...\`` pattern
(from `rondis_ttl.test`) for anything that depends on wall-clock math.

---

## Files touched (summary)

**New MTR tests (one per phase plus capstones):**

PR 1 (11 tests):
- `rondis_hash_lifecycle.test` (Phase 1.0 — schema + HSET/HDEL
  maintenance, end-to-end)
- `rondis_keyinfo_exists.test`
- `rondis_keyinfo_type.test`
- `rondis_keyinfo_ttl.test`
- `rondis_keyinfo_pttl.test`
- `rondis_keyinfo_expire.test`
- `rondis_keyinfo_pexpire.test`
- `rondis_keyinfo_expireat.test`
- `rondis_keyinfo_pexpireat.test`
- `rondis_keyinfo_persist.test`
- `rondis_keyinfo_expired_read.test`

PR 2 (7 tests):
- `rondis_hash_exists.test`
- `rondis_hash_strlen.test`
- `rondis_hash_setnx.test`
- `rondis_hash_len.test`
- `rondis_hash_keys.test`
- `rondis_hash_vals.test`
- `rondis_hash_getall.test`

PR 3 (10 tests):
- `rondis_unlink.test`
- `rondis_setnx.test`
- `rondis_setex.test`
- `rondis_psetex.test`
- `rondis_append.test`
- `rondis_getdel.test`
- `rondis_command_probe.test` (covers COMMAND / COMMAND COUNT / COMMAND DOCS)
- `rondis_info.test`

**Rondis source:**
- `src/rondb.cc` — all three PRs extend the dispatcher
- `src/commands.cc` — all three PRs add new command implementations;
  PR 1 Phase 1.0.2/1.0.3 adds hset_keys-maintenance hooks on HSET
  and HDEL paths
- `src/db_operations.cc` — PR 1 Phase 1.0.2/1.0.3 adds
  `bump_hset_field_count` / `decrement_hset_field_count`; PR 1
  Phase 1.5 adds `update_expiry_key_row`; PR 2 adds a single
  scan-based enumeration helper `scan_hash_fields` (Phase 2.5+);
  PR 1 Phase 1.0.4 adds a mutex around `redis_key_id_hash`
- `src/interpreted_code.cc` — PR 1 Phase 1.0.2/1.0.3 adds short
  add/subtract programs for `hset_keys.field_count`; PR 1 Phase
  1.5 adds a short expiry-update program; PR 1 Phase 1.11 extends
  the read program with an expired-filter branch
- `src/table_definitions.cc` — PR 1 Phase 1.0.1 adds the
  `field_count` column to `entire_hset_key_record`
- `include/table_definitions.h` — PR 1 Phase 1.0.1 adds the
  `field_count` field to `hset_key_table`; PR 1 Phase 1.1 adds
  `m_probe_only` to `GetControl`
- `include/commands.h`, `include/db_operations.h`, `include/common.h`
  — declarations and the new `RONDB_EXPIRED_KEY` sentinel

**Schema (PR 1):**
- `sql/create_rondis_tables.sql`, `sql/HSET_key.sql`,
  `mysql-test/suite/rondis/include/create_rondis_tables.inc` —
  Phase 1.0.1 adds `field_count INT UNSIGNED NOT NULL DEFAULT 0`
  to `hset_keys`; Phase 1.10c.1 makes `hset_keys.redis_key_id`
  nullable without `AUTO_INCREMENT` and adds
  `hset_key_id_sequence` as the hash-id allocator
- Existing `redis_0.hset_keys` rows backfilled to 1 in the
  CREATE script (idempotent)

**Plan (Phase 0):**
- `storage/ndb/claude_files/rondis_expansion/plan.md` — this file

**PR 2 and PR 3 have no schema changes.**

---

## Rough effort

- PR 1: 30-36h — now with eleven phases. Phase 1.0 is the largest
  single chunk — ~14h:
  - 1.0.1 schema (done, ~2h)
  - 1.0.2a set_simple_rows NoCommit+Commit refactor (~4h —
    mirrors the complex-path two-phase pattern)
  - 1.0.2b conditional `hset_keys` bump op integrated into
    both simple and complex pre-commit dispatchers (~4h)
  - 1.0.2c lifecycle test phase 1 (~1h)
  - 1.0.3 HDEL decrement + row delete (~3h)
  - 1.0.4 mutex on `redis_key_id_hash` (~0.5h)
  - 1.0.5 end-to-end validation test phase 3 (~0.5h)
  The EXPIRE / TTL / PERSIST phases (1.3, 1.5-1.9) each gain a
  small hash-dispatch extension (~+1h each). Phase 1.11
  (expired-key filtering) is the other large unknown, ~5h (must
  cover both `string_keys` and `hset_keys` expiry).
- PR 2: 14-18h — hinges on the scan-with-filter pattern (Phase 2.5,
  now the first scan path); once that lands, Phases 2.6/2.7 are
  "add projection columns". Phase 2.4 (HLEN) drops from a scan to a
  single PK read thanks to Phase 1.0's `field_count` column.
- PR 3: 10-14h — lots of small surface area but each phase reuses
  existing paths; the biggest unknown is reply-rewriting for
  SETNX / HSETNX's integer reply.

Total: ~46-60h plus review overhead.

## Out of scope (follow-ups)

- `EXPIRE NX|XX|GT|LT` flags (Redis 7 additions) — separate follow-up.
- `SCAN` / `HSCAN` as first-class cursor commands — needs a stable
  cursor encoding across NDB scan restarts.
- `INCRBYFLOAT` / `HINCRBYFLOAT` — interpreter float math extension.
- `RENAME` / `RENAMENX` / `COPY` — transactional key moves.
- `DBSIZE`, `FLUSHDB`, `KEYS pattern` — ops-shaped, need care
  around blocking semantics.
- Batching extension-row reads across fields in HVALS/HGETALL —
  noted in PR 2 Phase 2.8 as a performance follow-up.
- Cascade-delete of field rows when `hset_keys.expiry_date`
  expires — v1 leaves the orphan rows in `string_keys`,
  unreachable by name. Follow-up: hook the Phase 1.11 expired-key
  detector to issue a partitioned scan-delete on first-observed
  hash expiry, or add an ops-level GC job.
- Per-field hash TTL (Redis 7.4's `HEXPIRE` / `HPEXPIRE` /
  `HPERSIST` / `HTTL`) — requires extending the per-row
  `expiry_date` on `string_keys` hash-field rows with a new
  command set. Not covered by the plan.
- Lists / Sets / Sorted Sets / Streams — new schemas, feature-sized.
- `MULTI` / `EXEC` / `WATCH`, Pub/Sub — protocol-level, separate
  design.
