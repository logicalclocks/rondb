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
`PEXPIREAT`, `PERSIST`, plus a correctness fix: **expired keys are
filtered out on read** once TTL is a first-class user-visible feature.

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

### Phase 1.1 — EXISTS

**Scope:** `EXISTS key [key ...]` — integer count of keys that exist.
Duplicates in the argument list count each time (per Redis canonical).

**Files:**
- `src/rondb.cc` — dispatcher entry (NDB-dependent block), arity `>= 2`
- `src/commands.cc` — new `rondb_exists_command`
- `src/db_operations.cc` — new helper: metadata-only PK read on
  `string_keys` with `redis_key_id=0`; if miss, probe `hset_keys`
  via `rondb_get_redis_key_id` path
- `include/commands.h` — declaration

**Reply shape:** `:N\r\n`.

**Test:** `t/rondis_keyinfo_exists.test` — existing string key,
existing hash key, missing key, mixed batch, duplicates counted
separately, case-sensitivity.

### Phase 1.2 — TYPE

**Scope:** `TYPE key` → `"string"` | `"hash"` | `"none"` (simple
string reply, not bulk).

**Files:**
- `src/rondb.cc` — dispatcher entry, arity `== 2`
- `src/commands.cc` — new `rondb_type_command`: probe `string_keys`
  with `redis_key_id=0`; on miss, probe `hset_keys` by name; on miss,
  emit `+none\r\n`
- `include/commands.h` — declaration

**Reply shape:** `+string\r\n` / `+hash\r\n` / `+none\r\n`.

**Test:** `t/rondis_keyinfo_type.test` — all three outcomes; string
key and hash key with the same name each report their own type
(namespace-split per `rondis_namespace_split.test`).

### Phase 1.3 — TTL

**Scope:** `TTL key` → seconds remaining, `-1` if no TTL, `-2` if
missing. Also `-2` if row exists but `expiry_date < now` (already
expired — phase 1.10 will also hide the row from GET, but TTL's
reply is decided here).

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 2`
- `src/commands.cc` — new `rondb_ttl_command(... bool millis)` shared
  with PTTL (Phase 1.4), `millis=false` here
- `src/db_operations.cc` — extend metadata-only mask to include
  `expiry_date`; reuse `prepare_get_simple_key_row` unchanged
- `include/commands.h` — declaration

**Test:** `t/rondis_keyinfo_ttl.test` — SET with EX then TTL (value
in window), SET without EX then TTL (-1), TTL missing (-2). Uses the
`--let $r = \`SELECT ...\`` pattern from `rondis_ttl.test` for
determinism.

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
missing. Redis 7's NX/XX/GT/LT flags deferred (noted in the
follow-ups section).

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 3`
- `src/commands.cc` — new `rondb_expire_command(... ExpireMode)` where
  `ExpireMode ∈ {EX, PX, EXAT, PXAT}` drives the ttl-to-epoch
  conversion. Rejects `seconds <= 0` per C12 precedent.
- `src/db_operations.cc` — new `update_expiry_key_row` modelled on
  `incr_decr_key_row` @ `:1054`: `writeTuple` with mask covering only
  PK + `expiry_date`
- `src/interpreted_code.cc` — a short interpreter program: load new
  expiry, write_attr, exit_ok; on missing row the write naturally
  fails and the reply is `:0\r\n`
- `include/commands.h`, `include/db_operations.h` — declarations

**Reuses:** `generate_expire_at` @ `commands.cc:284`.

**Test:** `t/rondis_keyinfo_expire.test` — EXPIRE on existing key
(returns 1, TTL reflects it), EXPIRE on missing (returns 0), EXPIRE
0 / EXPIRE -1 rejected.

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
missing or already had no TTL.

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 2`
- `src/commands.cc` — new `rondb_persist_command`: pre-read
  `expiry_date` to decide the 1-vs-0 reply, then call
  `update_expiry_key_row` (from Phase 1.5) with
  `expiry_date = g_max_expire_at`

**Test:** `t/rondis_keyinfo_persist.test` — SET with EX, PERSIST
(returns 1), TTL (-1), PERSIST again (returns 0), PERSIST missing
(returns 0).

### Phase 1.10 — Expired-key filtering on read

**Scope:** Correctness fix. Once TTL is a user-visible feature, GET /
MGET / HGET / HMGET / STRLEN / GETRANGE / EXISTS / TYPE must treat
rows where `expiry_date < now` as absent. A still-present expired
row is an eventual-consistency artifact of lazy expiry and must not
leak back to the client.

**Files:**
- `src/interpreted_code.cc` — extend the read interpreter program
  (the one generated alongside `prepare_get_simple_key_row`) with a
  branch that compares `expiry_date` against `now` and emits
  `interpret_exit_nok(RONDB_EXPIRED_KEY)` (new sentinel) if expired
- `src/common.h` — define `RONDB_EXPIRED_KEY` sentinel
- `src/db_operations.cc` — in the read callbacks, translate the
  sentinel to `KeyState::CompletedFailed` with a code that the
  reply path already treats as "key does not exist" (i.e. emits
  `$-1\r\n` / counts as miss). Mirrors C5a/C5b/C7 plumbing.
- `src/commands.cc` — EXISTS counting and TYPE probes must also
  honor the sentinel (their reply builders are new in Phase 1.1/1.2
  so this just wires in the translation)

**Test:** `t/rondis_keyinfo_expired_read.test` — SET with EX 1, sleep
until expiry_date < now via a SQL-clocked sleep (not `sleep 2`; use
the existing TTL-test pattern for determinism), then GET returns
nil, EXISTS returns 0, TYPE returns none, HGET (hash variant) nil.

**Note:** This phase depends on Phases 1.1/1.2/1.3/1.5 having landed
so the translation has call sites to plumb through. The phase lands
as the capstone of PR 1.

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

### Phase 2.4 — HLEN (first scan path)

**Scope:** `HLEN key` → `:N\r\n` (field count, 0 if hash missing).

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 2`
- `src/commands.cc` — new `rondb_hlen_command`
- `src/db_operations.cc` — new `scan_hash_fields_count` — partitioned
  NDB table scan on `string_keys` with `NdbScanFilter::cmp(Equal,
  redis_key_id_col, hash_id)`; count rows in the scan callback

First scan-based command in Rondis. Validates the scan-with-filter
pattern before Phases 2.5-2.7 layer projection on top.

**Test:** `t/rondis_hash_len.test` — empty hash (0 after HDEL-all),
1-field, 10-field, missing hash.

### Phase 2.5 — HKEYS

**Scope:** `HKEYS key` → `*N\r\n` + N bulk strings (field names).

**Files:**
- `src/rondb.cc` — dispatcher, arity `== 2`
- `src/commands.cc` — new `rondb_hkeys_command`
- `src/db_operations.cc` — new `scan_hash_fields` — extends
  `scan_hash_fields_count` from Phase 2.4 to project `redis_key`
  (= field name) per row

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

PR 1 (10 tests):
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
- `src/commands.cc` — all three PRs add new command implementations
- `src/db_operations.cc` — PR 1 adds `update_expiry_key_row`; PR 2
  adds scan-based enumeration helpers (`scan_hash_fields_count`,
  `scan_hash_fields`)
- `src/interpreted_code.cc` — PR 1 adds a short expiry-update program
  (Phase 1.5) and extends the read program with an expired-filter
  branch (Phase 1.10)
- `include/commands.h`, `include/db_operations.h`, `include/common.h`
  — declarations and the new `RONDB_EXPIRED_KEY` sentinel

**Plan (Phase 0):**
- `storage/ndb/claude_files/rondis_expansion/plan.md` — this file

**No schema changes across any of the three PRs.**

---

## Rough effort

- PR 1: 16-20h — ten phases, but most reuse STRLEN / INCR templates;
  Phase 1.10 (expired-key filtering) is the biggest unknown because
  it extends the read interpreter program.
- PR 2: 16-20h — hinges on the scan-with-filter pattern (Phase 2.4);
  once that lands, Phases 2.5/2.6/2.7 are "add projection columns".
- PR 3: 10-14h — lots of small surface area but each phase reuses
  existing paths; the biggest unknown is reply-rewriting for
  SETNX / HSETNX's integer reply.

Total: ~45-55h plus review overhead. Distributes cleanly across the
28 phases in this plan.

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
- Lists / Sets / Sorted Sets / Streams — new schemas, feature-sized.
- `MULTI` / `EXEC` / `WATCH`, Pub/Sub — protocol-level, separate
  design.
