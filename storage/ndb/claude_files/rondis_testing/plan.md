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
