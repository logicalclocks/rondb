# Rondis test coverage expansion — plan (RONDB-1052)

## Context

Rondis currently supports 25 commands (PING, ECHO, CONFIG GET, SELECT;
GET, SET, MGET, MSET, DEL; INCR, INCRBY, DECR, DECRBY; STRLEN, GETRANGE,
SETRANGE; HGET, HMGET, HSET, HMSET, HDEL; HINCR, HINCRBY, HDECR, HDECRBY).
The existing MTR suite (`rondis_basic`, `rondis_advanced`, the three
`rondis_stress_*` wrappers) hits happy paths and throughput but leaves
three large coverage holes:

1. **Four commands have zero tests**: ECHO, CONFIG GET, SELECT, HMSET.
2. **All eight SET option flags are untested**: NX, XX, GET, EX, PX, EXAT,
   PXAT, KEEPTTL — and zero tests exercise TTL/expiry behavior.
3. **Zero negative coverage** — no arity errors, no numeric-parse errors,
   no overflow, no syntax-error cases asserted anywhere.

The goal is a proper MTR layer for **what Rondis actually supports today**,
with positive *and* negative tests for every command, so that regressions
in error replies, flag handling, and TTL behavior get caught in CI.

## Up-front design decisions

- **Error-string fidelity**: new negative tests assert Redis-canonical
  strings (e.g. `ERR value is not an integer or out of range`). Two
  Rondis error macros diverge from canonical and must be fixed as part
  of this work (see Phase 0 below).
- **Namespace split**: Rondis keeps string keys (`STRING_REDIS_KEY_ID`
  constant in `redis_0.string_keys`) and hash keys (auto-increment ids
  in `redis_0.hset_keys`) in separate namespaces, so Redis's `WRONGTYPE`
  does not apply — `GET foo` after `HSET foo f v` returns nil. A small
  dedicated test documents this current behavior as a regression anchor;
  no architectural change attempted.
- **Driver**: all new tests use `redis-cli -h $RONDIS_HOST -p $RONDIS_PORT`
  rather than `rondb-cli`. `redis-cli` is already a suite dependency
  (stress tests) and its `(error) ERR ...` rendering is the canonical
  reference for negative tests. Keep `rondis_basic` / `rondis_advanced`
  on `rondb-cli` — they stay as the short smoke suite.
- **TTL verification is SQL-based**, not sleep-based. Deterministic:
  inspect `redis_0.string_keys.expiry_date` via `SELECT` and assert the
  delta to `NOW()` falls in a ±2s window. PXAT/EXAT use absolute
  timestamps and can be asserted exactly.

## Phased execution

Five landable commits, each self-contained with its own verification
gate. A phase only starts once the previous phase is green.

### Phase 0 — commands.cc fidelity fixes

**Scope (C++ only, no test changes yet):**

Update `storage/ndb/src/rondis/include/common.h`:

- `FAILED_INCRBY_DECRBY_PARAMETER "Wrong parameter, should be Int64"`
  → `"value is not an integer or out of range"`
- `REDIS_INVALID_INTEGER "invalid integer '%s' in command"`
  → `"value is not an integer or out of range"` (drop `%s` formatter,
  tidy call site at `commands.cc:272` accordingly)
- Add `REDIS_OFFSET_OUT_OF_RANGE "offset is out of range"`

Update `storage/ndb/src/rondis/src/commands.cc`:

- SETRANGE at lines 2319/2323/2349 — replace `REDIS_SYNTAX_ERROR` with
  `REDIS_OFFSET_OUT_OF_RANGE` for negative offset / offset-over-limit /
  combined-length-over-limit.
- Audit HINCR / HDECR / HINCRBY / HDECRBY (lines 2032/2056/2093/2117).
  HINCRBY / HDECRBY already use `FAILED_INCRBY_DECRBY_PARAMETER` (so the
  macro rename covers them); confirm HINCR / HDECR emit the same macro
  on non-numeric-field / overflow and fix if not.

**Exit gate**: `./mtr --suite=rondis` stays green — no existing .result
 asserts any of the three diverging strings, so nothing should move.
 Also full build succeeds in both debug and prod.

**Commit**: `RONDB-1052: Align Rondis error strings with Redis canonical`

### Phase 1 — setup helper + zero-coverage commands

**Scope:**

- Create `mysql-test/suite/rondis/include/rondis_setup.inc` — sources
  `create_rondis_tables.inc`, pings Rondis via redis-cli to fail fast on
  missing binary or unreachable server. (Does **not** define a `$RCLI`
  var — MTR expands its own vars, so each test uses `--exec redis-cli
  -h $RONDIS_HOST -p $RONDIS_PORT …` directly for clarity.)

- Create four new positive tests covering the four zero-coverage
  commands, plus a namespace anchor:

  | File | Covers |
  | --- | --- |
  | `t/rondis_connection.test` | PING (baseline, ECHO reply), ECHO (with arg, empty arg, binary-ish arg), CONFIG GET (known param, unknown param → empty array), SELECT (valid 0, reject >0) |
  | `t/rondis_hash.test` (partial) | **HMSET** baseline — verifies `+OK` reply vs HSET's field-count. Full HSET/HGET/HMGET/HDEL coverage lands in Phase 2. |
  | `t/rondis_namespace_split.test` | SET foo v; HSET foo f x; GET foo → v; HGET foo f → x. Regression anchor for the namespace divergence. |

**Exit gate**: new tests pass on `--record`; full suite `./mtr
--suite=rondis` green.

**Commit**: `RONDB-1052: MTR coverage for previously untested Rondis commands`

### Phase 2 — comprehensive positive tests for covered commands

**Scope:**

Create six positive-path files. Each exercises one command group with
full variation coverage, not just the smoke-case of rondis_basic /
rondis_advanced.

| File | Scope |
| --- | --- |
| `t/rondis_strings.test` | GET, SET baseline, MGET, MSET, DEL. Empty value, binary byte payload, key near 3000-byte limit, multi-key DEL return count, MGET with mixed existing/non-existing |
| `t/rondis_counters.test` | INCR, INCRBY, DECR, DECRBY. Non-existent key starts at 0, negative increments, GET round-trip, multi-step sequences |
| `t/rondis_substring.test` | STRLEN (empty / non-existent → 0 / large), GETRANGE (positive, negative, inverted, out-of-bounds), SETRANGE (offset=0, mid-string, beyond-length padding, growing string) |
| `t/rondis_hash.test` (final) | HGET, HMGET (mix of existing/nil), HSET (single + multi-field), HMSET, HDEL (single + multi-field), field-overwrite returns 0 |
| `t/rondis_hash_counters.test` | HINCR, HINCRBY, HDECR, HDECRBY — mirror of string counters |
| `t/rondis_set_flags.test` | **SET flag matrix** — NX present/absent, XX present/absent, GET (returns old value or nil), EX/PX positive seconds/ms, EXAT/PXAT absolute, KEEPTTL preserves, combinations (NX+GET, XX+EX) |

**Exit gate**: full `./mtr --suite=rondis` green, all six new tests
recorded. Re-running `--record` produces no diff (no nondeterminism).

**Commit**: `RONDB-1052: Comprehensive positive-path MTR tests for Rondis`

### Phase 3 — TTL verification

**Scope:**

Create `t/rondis_ttl.test` using SQL to assert TTL placement
deterministically.

Pattern per assertion:

```
--exec redis-cli -h $RONDIS_HOST -p $RONDIS_PORT SET k v EX 60
--let $now = `SELECT UNIX_TIMESTAMP(NOW())`
SELECT
  UNIX_TIMESTAMP(expiry_date) - $now BETWEEN 58 AND 62 AS ttl_in_window
FROM redis_0.string_keys WHERE redis_key = 'k';
```

Cases:

- `SET k v EX 60` → expiry ≈ now + 60s
- `SET k v PX 60000` → same window
- `SET k v EXAT <absolute>` → exact match assertion
- `SET k v PXAT <absolute_ms>` → exact match
- `SET k v EX 60`; `SET k v KEEPTTL` → expiry preserved
- `SET k v EX 60`; `SET k v` → expiry_date becomes NULL (TTL cleared)
- `SET k v` (no TTL) → expiry_date IS NULL

Separated from Phase 2 because the SQL-verification pattern is distinct
and lands/fails independently of plain redis-cli assertions.

**Exit gate**: `./mtr --suite=rondis rondis_ttl` green; re-run across ≥5
iterations to confirm no time-window flakes.

**Commit**: `RONDB-1052: SQL-verified TTL tests for Rondis SET`

### Phase 4 — negative-path tests (requires Phase 0)

**Scope:** five negative-path files. All assert canonical
`(error) ERR …` strings that Phase 0 fixed up.

| File | Scope |
| --- | --- |
| `t/rondis_negative_arity.test` | Every command with wrong arg count: `GET`, `GET a b c`, `SET a`, `MSET a b c` (odd), `HSET a b` (missing value), `HSET a b c d e` (unpaired), `HGET a`, `INCRBY a`, `DECRBY a`, `STRLEN`, `GETRANGE a 0`, `SETRANGE a 0`, `HINCRBY a b`, `CONFIG` (no subcommand), `CONFIG SET …` (unsupported). Assert `(error) ERR wrong number of arguments for '<cmd>' command` or `(error) ERR unknown command …`. |
| `t/rondis_negative_counters.test` | Non-numeric value on INCR/DECR; non-numeric delta on INCRBY/DECRBY; INT64 overflow (SET k 9223372036854775807; INCR k); INT64 underflow; HINCRBY on non-numeric field |
| `t/rondis_negative_set_flags.test` | `SET k v NX XX` (syntax); `SET k v EX abc` (integer err); `SET k v EX -1`; `SET k v EX 0`; `SET k v KEEPTTL EX 10` (conflict) |
| `t/rondis_negative_substring.test` | `GETRANGE k a b` (integer err); `SETRANGE k -1 v` (offset out of range); `SETRANGE k 536870912 v` (>512MB) |
| `t/rondis_negative_select.test` | `SELECT abc` (integer err); `SELECT 16` (out of range); `SELECT -1` |

**Exit gate**: all five tests pass on `--record`; full suite green.
Then do a **negative-sanity loop**: temporarily revert one of the three
Phase 0 error-string macros, run the matching negative test, confirm the
diff output actually points at the exact string mismatch. Restore and
re-verify. Demonstrates the tests actually catch regressions.

**Commit**: `RONDB-1052: Negative-path MTR tests for Rondis commands`

### Phase 5 — suite-wide verification

**Scope:** no new code. Run:

```
./mtr --record --suite=rondis      # confirm all baselines clean
./mtr --suite=rondis               # confirm all green
./mtr --suite=rondis --repeat=3    # confirm no flakes
```

Fix any drift surfaced by re-record (ideally none). Check timing: the
full suite should remain ≤ ~3 min wall-clock (≈ 5 existing + 15 new
tests at ~10s each average).

**No commit** if everything stays clean. Otherwise a small follow-up
commit for stabilization.

## Files to modify / create (total)

**Modify (Phase 0):**

- `storage/ndb/src/rondis/include/common.h`
- `storage/ndb/src/rondis/src/commands.cc`

**Create (Phases 1–4):**

```
mysql-test/suite/rondis/include/rondis_setup.inc
mysql-test/suite/rondis/t/rondis_connection.test          (+ .result)
mysql-test/suite/rondis/t/rondis_namespace_split.test     (+ .result)
mysql-test/suite/rondis/t/rondis_strings.test             (+ .result)
mysql-test/suite/rondis/t/rondis_counters.test            (+ .result)
mysql-test/suite/rondis/t/rondis_substring.test           (+ .result)
mysql-test/suite/rondis/t/rondis_hash.test                (+ .result)
mysql-test/suite/rondis/t/rondis_hash_counters.test       (+ .result)
mysql-test/suite/rondis/t/rondis_set_flags.test           (+ .result)
mysql-test/suite/rondis/t/rondis_ttl.test                 (+ .result)
mysql-test/suite/rondis/t/rondis_negative_arity.test      (+ .result)
mysql-test/suite/rondis/t/rondis_negative_counters.test   (+ .result)
mysql-test/suite/rondis/t/rondis_negative_set_flags.test  (+ .result)
mysql-test/suite/rondis/t/rondis_negative_substring.test  (+ .result)
mysql-test/suite/rondis/t/rondis_negative_select.test     (+ .result)
```

Total: **14 .test + 14 .result** files + 1 helper + 2 source edits.

## Existing utilities to reuse

- `mysql-test/suite/rondis/include/create_rondis_tables.inc` — table
  setup. Already idempotent.
- `RONDIS_HOST` / `RONDIS_PORT` env vars from `suite/rondis/my.cnf:98-99`.
- `redis-cli` binary — already a suite dependency since the
  `rondis_stress_*` tests landed.
- `assign_generic_err_to_response` at `storage/ndb/src/rondis/src/common.cc:61`
  handles `-ERR ` prefix wrapping. No changes needed.

## Rough effort

- Phase 0: ~30 lines of C++, <1h including build/test cycle.
- Phase 1: ~3 small .test files + helper, ~1h.
- Phase 2: 6 moderate .test files (~30 lines each), ~3h.
- Phase 3: 1 file, careful SQL assertions, ~1h.
- Phase 4: 5 files, most mechanical once Phase 0 is in, ~2h.
- Phase 5: verification only, ~30 min.

Total ~8h of focused work; ≈600–800 lines of new MTR code + small C++
changes.

## Out of scope (follow-up tickets)

- Unifying string / hash key namespaces so that Redis-canonical
  `WRONGTYPE` becomes applicable. Tracked via
  `rondis_namespace_split.test` as the regression anchor.
- Real-time TTL expiry tests (waiting for `expiry_date` to trigger
  eviction). Needs sleeps and depends on Rondis's background eviction
  worker cadence.
- Parallel / stress negative tests (race conditions under load).
- Pipeline / transaction (MULTI/EXEC) — Rondis does not support these.
- RESP3 / HELLO — not supported.

### C12 — EX/PX accept non-positive values; EX -1 conflates with sentinel

Surfaced while recording `t/rondis_negative_set_flags.test`.

Real Redis rejects `SET k v EX 0` and `SET k v EX <negative>` with
`ERR invalid expire time in set`. Rondis accepts both and stamps
`expiry_date` accordingly:

- `SET k v EX 0` — `ttl=0`, `generate_expire_at` computes `now + 0`,
  key is immediately expired (relies on the background TTL purger to
  remove it).
- `SET k v EX -1` — the inner `ttl` variable lands at `-1`, which
  `generate_expire_at` treats as the sentinel for "never expires"
  (commands.cc:280) and stamps `expiry_date = g_max_expire_at`
  (2038-01-19). So EX -1 is not even "already expired" - it is
  accepted as a permanent key! That is a direct semantic collision
  between the wire-level `-1` input and the internal sentinel.

**Fix direction**: reject `EX <= 0` (and `PX <= 0`) at the parse site
in `commands.cc:1040-1053` before calling `generate_expire_at`, and
stop reusing `-1` as a sentinel - switch to an explicit `set_ttl`
/ `no_ttl` flag (already present as `set_ttl`, so the sentinel
collision is avoidable with a small refactor).

`t/rondis_negative_set_flags.test` records the current clamped-to-2038
behavior via SQL so a fix surfaces as a visible diff.

### C13 — HDEL with no fields crashes the server

Surfaced while recording `t/rondis_negative_arity.test`.

`HDEL <key>` with no field arguments passes the dispatcher arity
check at `storage/ndb/src/rondis/src/rondb.cc:732`
(`argv.size() >= 2`), falls into `rondb_del` at
`storage/ndb/src/rondis/src/commands.cc:562`, and hits
`assert(num_keys > 0)`. On debug builds that aborts the Rondis worker
and the next client command gets "Server closed the connection"; on
release builds the assert is compiled out and the handler proceeds
with `num_keys = 0`, which is undefined behavior.

**Fix**: tighten the HDEL dispatcher to `argv.size() >= 3` (one key
plus at least one field), and keep the inner `assert(num_keys > 0)`
as defense in depth. The negative-arity test currently exercises
`HDEL` with no args and deliberately skips the `HDEL key` probe to
avoid the crash.

### C10, C11 — HSET / HMSET reply semantics (discovered in Phase 2)

Surfaced while recording `t/rondis_hash.test`.

**C10. HSET returns the total pair count instead of the count of new
fields.**
Expected (Redis): HSET returns the number of fields that did **not**
previously exist and were therefore added. Updates of existing fields
do not count.
Actual: Rondis returns the total number of `field value` pairs
supplied, regardless of whether they were new or overwrites.
- `HSET k newfield v` on a brand-new field returns `1` (coincidentally
  correct).
- `HSET k existingfield v` overwriting a known field returns `1`
  (should be `0`).
- `HSET k existing v1 new1 v2 new2 v3` returns `3` (should be `1`).

Root cause: `rondb_hset_command` at
`storage/ndb/src/rondis/src/commands.cc:1316` delegates to
`rondb_mset`, which at `:1271-1280` replies with
`get_ctrl->m_num_keys_requested` — the number of supplied pairs. The
path has no concept of "which pairs hit an existing row". A proper
fix needs the MSET/HSET interpreted program to emit, per row, a bit
indicating insert-vs-update, aggregate those bits on the client side,
and return the aggregate.

**C11. HMSET returns a field count instead of `+OK`.**
Expected (Redis): HMSET returns the simple string `+OK` (it is the
deprecated variant of HSET with a different, simpler reply shape).
Actual: Rondis's HMSET dispatcher points at the same handler as HSET,
so it emits the integer field count.

Root cause: same code-reuse at `rondb_hset_command:1316`. HMSET needs
its own handler (or a flag into `rondb_mset` that swaps the reply
shape after a successful store). Low-risk fix once C10's plumbing is
in — the MSET function already branches on `STRING_REDIS_KEY_ID` for
its reply; HMSET just needs a third branch that emits `+OK`.

`rondis_hash.result` was recorded with the current (wrong) replies so
that both fixes, when they land, surface as visible .result diffs.
In-line comments in `rondis_hash.test` flag every assertion that is
currently divergent.

### C7, C8, C9 — SET flag paths are broken (discovered in Phase 2)

The comprehensive SET flag matrix test (`t/rondis_set_flags.test`)
surfaced three broken paths in Rondis's conditional-store
implementation. The test captures current behavior via `--error 0,1`
so regressions or fixes both show as visible .result diffs, but the
commands themselves need real fixes.

**C7. `SET k v NX` on an existing key errors out.**
Expected (Redis): reply is `$-1\r\n` (nil) and the store is skipped.
Actual: the interpreted-code program built for the "insert only if
absent" path has an invalid branch target, and NDB refuses to compile
it: `ERR Failed to create Interpreted code; NDB(4517) Bad label in
branch instruction`. The bad-label error is deterministic, so this
path has never worked. Fix is in the SET-NX program construction,
likely in `storage/ndb/src/rondis/src/interpreted_code.cc` (whichever
routine builds the conditional-insert program referenced from
`rondb_set_command` at `commands.cc:1290`).

**C8. `SET k v XX` on a non-existent key errors out.**
Expected: reply is `$-1\r\n` (nil) and the store is skipped.
Actual: `ERR Failed to execute MSET operation; NDB(6000)`. Two
distinct problems: (a) the XX-absent path emits a transaction-abort
error instead of the canonical nil; (b) the error message incorrectly
references MSET because the XX path reuses `FAILED_EXECUTE_MSET` as
its generic failure string. The message fix is cosmetic; the nil-on-
miss behavior is the real work.

**C9. `SET k v GET` errors out on a non-existent key.**
Expected: store the value and return `$-1\r\n` (nil old value).
Actual: the server returns an error that causes redis-cli to exit 1.
The GET-flag path is probably merging the old-value read into the
same interpreted-code program and falling off a cliff when the row
does not exist. Needs the SET+GET code path to treat
"key-not-found-before-insert" as success with a nil old-value reply,
not as an error.

**Fix order suggestion**: C8 (message fix is trivial, behavior fix
isolates cleanly), then C7 (related interpreted-code construction),
then C9 (likely shares code with C7). Each should re-record
`rondis_set_flags.result` in lockstep.

### C5b (partial fix) — INCR/DECR integer overflow now surfaces canonically

Update to the earlier analysis: the NDB interpreter already detects
add/sub overflow and returns `ZCALC_OVERFLOW_ERROR` (code 854 in
`storage/ndb/src/kernel/blocks/dbtup/Dbtup.hpp:157`). It was not a
silent wrap — Rondis just failed to map the code, so the generic
"Failed to increment key; NDB(854) Calculation resulted in overflow"
reply leaked through.

Phase-0.5 fix in `db_operations.cc` now maps NDB 854 to the canonical
`ERR increment or decrement would overflow`, mirroring the C5a
pattern for 853. New macro `RONDB_INTERP_CALC_OVERFLOW` in
`common.h`, new canonical string `FAILED_INCRBY_DECRBY_OVERFLOW`
"increment or decrement would overflow".

### C14 — `std::stoll` silently accepts trailing garbage on PX / EX

Discovered while recording `t/rondis_negative_set_flags.test`.

`get_int64` at `storage/ndb/src/rondis/src/commands.cc:262` uses
`std::stoll`, which parses the longest valid integer prefix and
returns it without complaint — so `SET k v PX 1.5` is accepted as
`PX 1`, `SET k v EX 42abc` as `EX 42`. Redis rejects both with
`value is not an integer or out of range`.

Fix: switch `get_int64` to `strtoll` with an end-pointer check (or
explicit `std::from_chars`) so a non-integer suffix is detected.
Low-risk one-file change in a helper used by EX/PX/EXAT/PXAT/
GETRANGE/SETRANGE.

### C15 — "NDB(1)" sentinel leaks into non-NDB error replies

`assign_err_to_response` at `common.cc:48` formats
`-ERR %s; NDB(%u)` even when there is no NDB error in play — call
sites pass the literal `1` as a placeholder. The current baselines
show this as `ERR Wrong parameter to SELECT command; NDB(1)` and
`ERR value is not an integer or out of range; NDB(1)` across the
negative-path tests. Redis-canonical replies never carry such a
suffix; clients parsing the reply text will trip on the extra token.

Fix: migrate every `assign_err_to_response(..., 1)` call site to
`assign_generic_err_to_response(...)` (which does not append the
suffix), or keep the helper but gate the `; NDB(%u)` emission on
`code != 0`. Affects SELECT error paths and the INCRBY/HINCRBY
integer-error paths. Once fixed, the five negative-path .result
files need re-recording.

### C5c — INCR near INT64_MAX / INT64_MIN closes the connection

Discovered while recording `t/rondis_negative_counters.test`. The
first `INCR` on a counter set to `INT64_MAX - 1` (or the first `DECR`
on `INT64_MIN + 1`) closes the client connection ("Error: Server
closed the connection" from redis-cli). A subsequent reconnect
reproduces the overflow error cleanly, so the data node and the
transaction state recover.

Hypothesis: the first INCR successfully wraps to INT64_MAX (ADD_REG
succeeds because `MAX - 1 + 1 == MAX` has no overflow), but the
INT64_TO_STR instruction then formats a value at the extreme edge and
fails, tearing down the connection. Could also be related to buffer
sizing around `MAX_LONG_LONG_STRING = 32` plus the 4-byte length
prefix the Rondis value_start format uses.

Needs on-host repro with DEBUG_INCR enabled and a core-dump watch.
Separately tracked; negative-counters test uses `SET x INT64_MAX`
then `INCR x` so the overflow path lands in a single operation and
the connection-close path is not exercised. Plan to revisit once the
data-node flow is instrumented.

### C5b (original) — INCR/DECR silently wraps (SUPERSEDED by above)

While fixing C5a (non-numeric stored value mapped to the canonical
`ERR value is not an integer or out of range`), the NDB interpreted-code
program that backs INCR/DECR/HINCR/HDECR was reviewed in
`storage/ndb/src/rondis/src/interpreted_code.cc:74-98`. The arithmetic
uses the plain `add_reg` / `sub_reg` NDB instructions with no overflow
check:

```
code->load_const_u64(REG5, inc_dec_value);   // desired delta
code->add_reg(REG5, REG4, REG5);             // 2's-complement wrap on overflow
```

Concretely:

```
SET k 9223372036854775806   # INT64_MAX - 1
INCR k                      # -> 9223372036854775807 (correct)
INCR k                      # -> -9223372036854775808 (WRAP - should be error)
```

Redis returns `ERR increment or decrement would overflow`. Rondis
silently flips the sign. This is not just a conformance gap — it is a
**correctness bug with data-loss implications**: a counter wraps and
subsequent reads look legitimate.

**Why it is deferred**: fixing this requires either (a) a new overflow-
checked variant of the ADD_REG_REG / SUB_REG_REG interpreter opcode
added to `storage/ndb/src/kernel/blocks/dbtup/DbtupExecQuery.cpp` plus
the corresponding `NdbInterpretedCode` accessor, or (b) a client-side
CAS loop that pre-validates `delta + current` fits in Int64 (gives up
atomicity). Option (a) is the right answer and deserves its own ticket
with a Dbtup-format bump review; option (b) breaks the atomicity
guarantee Rondis currently provides.

**Phase 4 handling**: the negative-counters test will SET a value near
INT64_MAX, INCR twice, capture whatever Rondis currently returns, and
annotate the .test file with a comment pointing back to this entry.
When C5b lands, that baseline gets re-recorded with the canonical
overflow error.
