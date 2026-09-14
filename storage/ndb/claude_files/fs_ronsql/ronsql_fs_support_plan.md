# RonSQL support plan for the Feature Store query generator

**Input:** the RONDB-1121 findings (F0–F22, `mysql-test/suite/ronsql_fs/findings/`),
the requirements report (`phase_e8.md` §4: 12 supported, 4 Hopsworks-gated,
4 unsupported, identical on the interpreter, JIT and ng2r2 arms) and the
`fs_hw` benchmarks (`benchmarks.md` §8).
**Output:** the engine work that makes RonSQL serve every statement the
Hopsworks `PreparedStatementBuilder` emits, correctly and fast enough, with
the RONDB-1121 framework as the acceptance gate for each step.
**Date:** 2026-09-14. Engine fixes live in the engine tree; each work package
names the framework evidence that flips when it lands.

## 0. Acceptance definition

The generator's contract is the requirements manifest (`cases/requirements.go`,
`req-v1`). Acceptance means:

1. `.fs_verify --requirements --all --vectors` reports `acceptance=PASS` on
   the base, JIT and ng2r2 arms: every emitted branch SUPPORTED, every
   Hopsworks gate HOPSWORKS-GATED, nothing UNSUPPORTED / UNTESTED / FAILED.
   Today's four UNSUPPORTED rows and the findings behind them:

   | requirement | findings | branch |
   |---|---|---|
   | R-S6 | F0 | collect last-N, the emitted CTE form |
   | R-F1 | F1 | a string feature aggregated twice (e.g. `category: [count, min, max]`) |
   | R-A2-binary | F7 | binary / complex feature projected through a snowflake template |
   | R-A5-types | F4, F5, F6, F9 | FLOAT display, DECIMAL beyond 2^53, BIGINT SUM overflow, temporal MIN/MAX in JSON |

2. The regression suites stay green three times (`ronsql_fs`, `ronsql_fs_jit`
   strict-armed, `ronsql_fs_ng2r2`), the fuzzers report no unclassified
   failure, and every expectation-table entry retired by a fix is removed
   (the runners print `PASS(was-known-*)` / `PASS(was-expected-reject)` when
   an entry is stale).
3. Serving performance: the `fs_hw` benchmarks meet the targets in §2 (WP-F,
   WP-G); the plan pins flip from the recorded table scans to index plans.

Not everything below is needed for (1): the plan separates what Hopsworks
emits today (P0/P1) from production performance (P2) and from hardening of
the envelope beyond the generator (P3), which the fuzzers reach but Hopsworks
does not.

## 1. Inventory

| # | subsystem | severity | emitted by Hopsworks | package |
|---|---|---|---|---|
| F0 | RonSQL planner: non-aggregating CTE body | UNSUPPORTED, clean reject | yes (every collect feature) | WP-A |
| F1 | aggregation program: string register reuse | CRASH (RDRS / data node) | yes (count+min+max over one string feature) | WP-B |
| F7 | pass-through result printer: binary types | UNSUPPORTED, clean reject | yes (binary / array features in snowflake) | WP-C |
| F9 | ResultPrinter: temporal aggregate quoting in JSON | INVALID JSON | yes (min/max over a timestamp feature; JSON is the RDRS default) | WP-D1 |
| F5, F21, F2 | DECIMAL aggregation on the DOUBLE path | WRONG VALUE (>2^53), topology-dependent digit, scale formatting | yes (decimal money features) | WP-D2 |
| F6, F22 | BIGINT SUM overflow: per-fragment 1860 vs unchecked merge | clean error on small clusters, wrapped value on large | yes (sum over bigint) | WP-D3 (overflow overhaul, separate task) |
| F3, F4 | AVG / FLOAT display rules | formatting (tolerated by the canonicalizer) | yes | WP-D4 |
| F14 | CTE body bound on a VARCHAR primary key | WRONG RESULT (no rows) | yes (string entity keys + snowflake) — not yet in the manifest | WP-E |
| F12 | planner: IN list → table scan | PERFORMANCE 200–1000× | yes (batch serving, 10–1000 keys) | WP-F |
| F13 | CTE_SCAN + PK_LOOKUP round trips | PERFORMANCE 3–4× | yes (every snowflake point read) | WP-G |
| F15 | DBSPJ join-aggregation null row over a CTE_LOOKUP miss | DATA NODE CRASH | no (LEFT JOIN onto an aggregated CTE) | WP-H |
| F18 | partial-key CTE lookup now runs | WRONG RESULT (regression from a clean reject) | no | WP-H |
| F19 | CTE_SCAN as outer-join child now runs | WRONG RESULT (regression from a clean reject) | no | WP-H |
| F17 | HAVING + ORDER BY + LIMIT | internal error instead of a clean reject | no (HAVING unsupported) | WP-H |
| F16 | AVG over a non-numeric column | generic "report a bug" message | no | WP-H |
| F10, F11 | mysqld pushdown aggregation (ndbcluster) | mysqld CRASH / error 4120 | MySQL path of `queryOnline` | WP-I (separate track) |
| F20 | NDB API dictionary cache (RONDB-1092 follow-up) | RDRS CRASH | — | FIXED (`76cc05701c6`), backport recommended |
| F8 | framework rule (collation-equal MIN/MAX) | — | — | done |

## 2. Work packages

Each package: what Hopsworks needs, the mechanism as found, the proposed
design, and the framework evidence that flips.

### WP-A — Collect last-N in the emitted CTE form (F0) — P0

**Need.** Every collect feature (`array<struct<…>>` last-N per entity) is
served by

```
WITH t AS (SELECT <pk…>, <fields…> FROM <fg> WHERE <entity key> = ?
           ORDER BY <event_time> DESC|ASC LIMIT n)
SELECT <pk…>, <fields…> FROM t;
```

RonSQL rejects it: `Non-aggregating CTE body is not a single-row key lookup.`
The equivalent direct form (S6b, the body alone) is correct and, per the
benchmarks, 1.1–1.3× faster than MySQL's `ROW_NUMBER()` twin, so the
engine already has the right execution path; only the CTE wrapping is
missing.

**Design.** Recognize the shape in the planner and execute it as the S6b
pass-through: a CTE whose body is a non-aggregating, ordered and limited
range over a primary-key prefix, referenced by a main query that is a
plain projection of the CTE's own columns (no joins, no aggregation, no
WHERE). Map it to the ordered index scan with LIMIT that S6b uses; no
materialization, no CTE_SCAN. Reject anything else with the existing
message. A general non-aggregating CTE (materialized, then scanned) is a
larger feature and is not required by the generator.

**Evidence.** `.fs_verify --shape S6` (cases `S6-cte-k21/k31/k16` from
`REJECT(expected)` to `PASS`), `--vectors --spec V-S6-cte-n5`, the spec
fuzzer's collect cases from `CLEAN-REJECT` to `PASS` (`.fs_fuzz spec --seed
1`, expect `clean-reject` to drop by the collect count), retire
`Known["S6-cte"]`, R-S6 SUPPORTED, re-record `ronsql_fs_templates`.

### WP-B — String aggregate re-use crash (F1) — P0

**Need.** A string feature with more than one aggregate function
(`count`, `min`, `max` on `category`) is a legal Hopsworks aggregate spec
and is emitted as `COUNT(category), …, MAX(category)` in one statement.
Today this crashes RDRS (`NdbSqlUtil.cpp:501 require` in `cmpLongvarchar`
while merging partials) or the data node (`minMaxString` in the LDM
thread) whenever another column load sits between the two aggregates of
the same string column.

**Design.** In the aggregation program builder (RonSQL → `AggInterpreter`
program), give each string aggregate its own register/buffer instead of
re-using the string load; verify the kernel's `minMaxString` and the API's
partial merge (`aggMergeMin/Max` string branch) read the length prefix of
the right buffer. Cover Char, Varchar and Longvarchar. The spec fuzzer's
"one function per string column" avoidance (`fuzz/spec.go`
`validAggregate`) is lifted once fixed.

**Evidence.** `.fs_verify --case EDGE-F1-string-reuse --include-hazards`
and the `-nonull` variant to `PASS`, then clear `Hazard` on both cases
(they leave the commented-out block of the templates test), R-F1
SUPPORTED, fuzz spec generator allows repeated string functions.

### WP-C — Binary and complex projections (F7) — P0

**Need.** Hopsworks emits snowflake templates projecting `binary` (and
serialized `array`) features — embeddings are the common case. RonSQL's
pass-through printer rejects them: `Unsupported column type (17) in
pass-through result.`

**Design.** Support Binary / Varbinary / Longvarbinary (and Blob if
Hopsworks maps large binaries to it) in `ResultPrinter` pass-through:
TEXT output as the raw bytes (MySQL client behaviour), JSON output with an
explicit encoding. Use the encoding RDRS already uses for binary columns in
pk-read responses (base64) so the Hopsworks client decodes both endpoints
the same way; document it in the RonSQL output contract. Aggregates over
binary columns stay rejected.

**Evidence.** `EDGE-comp-binary` to `PASS` (framework: `canon` compares
binary cells by bytes; the JSON decoder must un-base64 — a small framework
change), fixture `snowflake_binary` under `.fs_verify --golden`,
R-A2-binary SUPPORTED, expectation-table `F7` retired.

### WP-D — Type fidelity in aggregates (R-A5-types) — P0/P1

**D1. Temporal MIN/MAX quoting in JSON (F9) — P0, trivial.**
`ResultPrinter::print_aggregate_value` (`ResultPrinter.cpp:2401`) calls
`print_temporal_packed(…, "")` regardless of `m_quote`; pass-through
temporal columns use `m_quote` and are fine. Fix the call, add a JSON
regression. Hopsworks reads JSON (it never sets `outputFormat`), so today
any feature view with `min`/`max` over a timestamp feature gets an
unparsable serving response. Evidence: `EDGE-date-range`, `EDGE-ts*`,
`EDGE-float-exact` from `KNOWN-ERROR` to `PASS`; `Known["F9"]` retired;
fuzzers' `known-error` counts to 0.

**D2. Exact DECIMAL aggregation (F5, F21, F2) — P1.** DECIMAL with
scale > 0 takes the DOUBLE path, so values beyond 2^53 cents lose digits
(F5: `MAX(dec_big)` = `1000000000000000` vs `999999999999999.99`), the
last digit of a SUM depends on the topology's combine order (F21:
`6.0600000000000005` on two node groups), and MIN/MAX drop the scale
(F2: `10.5` vs `10.50`). Design: keep DECIMAL(p,s) as a scaled integer
through the per-fragment aggregation, the wire partials and the API
merge (int128 accumulator for SUM; MIN/MAX as the scaled integer), and
print with the column's scale. That makes DECIMAL aggregates exact,
deterministic across topologies and formatted like MySQL in one change.
Evidence: `EDGE-decimal-large`, `EDGE-null-decimal` to `PASS`, `Known["F5"]`
retired, `EDGE-float-exact` may regain `SUM(dec_val)`, canonicalizer's
DECIMAL rule can become strict.

**D3. BIGINT SUM overflow semantics (F6, F22) — separate task (overflow
overhaul).** Today: NDB error 1860 when the overflow happens inside one
fragment, a silently wrapped value when the partials overflow only at the
API merge (`aggMergeSum`: unchecked `val_uint64 +=`). MySQL widens
`SUM(BIGINT)` to DECIMAL. Decide one semantics and apply it everywhere:
either widen (int128 / DECIMAL accumulator, which D2's accumulator gives
for free) or error consistently (checked add in the merge, raise 1860).
Widening matches the MySQL twin and is what the generator's clients
expect. Evidence: `EDGE-big-overflow` from `REJECT(expected)` /
`KNOWN-WRONG` to `PASS` on every topology, `Known["F6"]` and `["F22"]`
retired, `ronsql_fs_ng4r2` records the same output as the base suite.

**D4. Display rules (F3, F4) — P2.** AVG prints four decimals regardless
of the input type (`0.5000` vs MySQL `0.5` for DOUBLE; MySQL uses scale+4
for exact types and `%g`-style for doubles); FLOAT MIN/MAX prints the
exact binary32 value (`123456.7890625` vs MySQL's 6-significant-digit
`123457`). Align the printer with MySQL's rules per input type. The
canonicalizer tolerates both today (AVG within 1e-4, FLOAT pinned), so
these matter for byte-identical clients only; do them with D2 since the
printer is touched anyway. Evidence: `S1-avg-k31`, `EDGE-float-rounding`
strict, `Known["F4"]` retired, MTR numeric canonicalization hook removable.

### WP-E — String entity keys in snowflake templates (F14) — P1

**Need.** String entity keys are common in Hopsworks; with a snowflake
join the template's CTE body is `SELECT region_id, COUNT(*) FROM
customers_str_1 WHERE customer_key = 'k' GROUP BY region_id`. Standalone
the body returns its group; through `CTE_SCAN` + `PK_LOOKUP` the
statement returns no rows, for both stored case forms, with a plan
identical to the integer-key case (`Body root: INDEX_SCAN using PRIMARY`).

**Design.** The CTE-body root's index bound on a VARCHAR primary key is
encoded or compared differently from the standalone aggregate path
(length prefix / collation key); find the divergence in the CTE body
materialization (`QueryPlanner` / CTE body root construction) and reuse
the standalone bound builder. Add a kernel-side or API-side regression
with a VARCHAR PK.

**Evidence.** Spec fuzzer `known-wrong` (F14) to 0, `Known["F14"]` retired;
framework: add string-keyed snowflake cases (`S7-1hop-str`, `S8-chains-str`)
and manifest rows `R-S7-str` / `R-S8-str` so the requirements report covers
the branch.

### WP-F — Batch IN lists (F12) — P2, largest serving-performance item

**Need.** Batch serving sends `WHERE customer_id IN (k1 … kn) GROUP BY
customer_id` with 10–1000 keys (S3, S10 batch, the batch snowflake body).
RonSQL plans every IN list as `Execute as table scan.` with the OR chain
as a filter: 208 ms / 612 ms / 4.4 s for 10 / 100 / 1000 keys over the
3.6 M-row history at sf 1, against MySQL's 1 / 10 / 169 ms pushed
(0.25 / 1.8 / 42 ms unpushed) — 200–1000× slower, and the batch snowflake
body scans the whole entity table.

**Design.** Plan an IN list over the leading primary-key column as one
PK-prefix index range per key (multi-range read), pushed into the
aggregation scan with GROUP BY on the key, so each key costs one ordered
range; when the full PK is bound, batched PK lookups. Apply the same to
CTE bodies. Keep the OR-filter fallback for non-key columns. Measure with
`.bench_ronsql fs_hw` (`agg_batch10/100/1000`, `agg_batch100_window`,
`snow1_batch100`, `strkey_batch100`) and the pins in `benchmarks.md` §7.

**Targets** (sf 1, 1 thread): batch100 ≤ 20 ms (≤ 2× MySQL pushed),
batch1000 ≤ 300 ms, batch10 ≤ 3 ms; plan pins show index ranges, not a
table scan. Evidence: pins re-recorded, `benchmarks.md` §8 run 4.

### WP-G — Snowflake round trips (F13) — P2

**Need.** Every snowflake point read (S7 INNER, S8 per-chain LEFT, S8b)
costs 480–530 µs against MySQL's 120–175 µs (2–3 plain PK reads): the
point aggregate on the same table takes 70 µs on the data node, so ~350
µs is CTE materialization plus `CTE_SCAN` protocol round trips.

**Design.** A CTE body bound by the full primary key of its table is a
single-row lookup; execute it as a PK read that feeds the `PK_LOOKUP`
children inside one SPJ tree instead of scan + materialize + scan. The
batch variant (`snow1_batch100`) follows from WP-F. Target: ≤ 1.5× MySQL
(≈ 200 µs at 1 thread); the JIT arm is already in place for the compiled
path. Evidence: `fs_hw_snow*` pins and timings, `benchmarks.md` §8.

### WP-H — Envelope hardening — P3 (not emitted; reachable by any client)

Ordered by consequence:

1. **F15** data-node crash: an aggregating main that LEFT JOINs a
   `CTE_LOOKUP` whose key misses hits `sendJoinAggNullRow` →
   `appendFromParent` (`DbspjMain.cpp:15201`, RONDB-733 join
   aggregation). Fix the null-row emission for a CTE_LOOKUP leaf or
   reject the shape cleanly; until then it is a hazard in
   `fuzz/hazards.go` (F15) and `cte-per-fg` emits INNER only.
2. **F18 / F19** silent wrong results that replaced clean rejections:
   a partial-key CTE lookup (`Partial CTE lookup key not supported`) and
   `CTE_SCAN` as an outer-join child now run and return wrong rows /
   diverging output. Either restore the guards or implement the
   semantics; the envelope fuzzer's `KnownWrong` rows flip to `PASS` or
   back to `CLEAN-REJECT` accordingly.
3. **F17** HAVING with ORDER BY on an aggregate alias and LIMIT throws
   `Got record with fewer aggregates than expected. Please report a
   bug.`; plain HAVING rejects with `Could not find column`. Reject
   HAVING uniformly (one message) until it is supported.
4. **F16** AVG over a string / temporal column falls through to `Failed
   writing aggregation program. Please report a bug.` although the
   specific guards exist; route these types to the guards.
5. The `ronsql_cte` discovery-log hazards (D3–D23) translated in
   `fuzz/hazards.go` — tracked by the CTE plan, listed here because the
   `--include-hazards` run reproduces them on this schema.

### WP-I — mysqld pushdown aggregation (F10, F11) — separate track

Hopsworks' `queryOnline` path goes through MySQL; with
`ndb_pushdown_aggregate=ON` a VARCHAR `GROUP BY` with an IN list crashes
mysqld in `NdbSqlUtil::likeLongvarchar` (F10) and a pushed point aggregate
fails with NDB 4120 `Scan already complete` (F11). Both are ndbcluster
(`ha_ndbcluster`) issues, reproduced by `.bench_sql fs_hw` with pushdown
on; the F10 comparator failure is the same `require` as F1's, so WP-B's
string-buffer fix may share a root. Track in the pushdown-aggregation
plan; the fs framework's `.bench_sql` runs are the acceptance check.

## 3. Sequencing

| milestone | packages | acceptance evidence |
|---|---|---|
| **M1 — emitted branches served** (detail: `m1_plan.md`) | error codes (M1.0), D1 (F9), B (F1), A (F0), C (F7) | requirements report: R-S6, R-F1, R-A2-binary SUPPORTED; only R-A5-types left; RonSQL errors carry 400/413/503/500 by class |
| **M2 — type fidelity** | D2 (F5/F21/F2), D3 (F6/F22, the overflow overhaul), D4 (F3/F4) | `acceptance=PASS` on base / jit / ng2r2; `ronsql_fs_ng4r2` identical to base |
| **M3 — serving performance** | F (F12), G (F13) | `fs_hw` targets met, plan pins re-recorded, `benchmarks.md` §8 run 4 |
| **M4 — hardening** | E (F14 + manifest rows), H (F15, F18, F19, F17, F16) | spec fuzzer `known-wrong` = 0, envelope fuzzer `known-wrong` = 0, hazards list shrinks |
| **parallel** | I (F10, F11) | `.bench_sql fs_hw` with pushdown on, no crash / no 4120 |

M1 is small and high-value: D1 is a one-line fix, B and C are contained,
and A maps onto an execution path that already exists. M2's D2 is the one
structurally larger item (a DECIMAL accumulator through kernel, wire and
merge) and D3 is the separate overflow task the user has scheduled. M3 is
where RonSQL either becomes the serving path for batch and snowflake
vectors or stays behind the SQL path (today: collect is faster on RonSQL,
snowflake and batch are faster through MySQL).

## 4. Verification workflow per change

1. Unit: `go test ./internal/fsq/... ./internal/shell` (generator
   expectations, conformance, manifest self-check).
2. Targeted: `.fs_verify --case <id>` / `--shape S<n>`, `--vectors --spec
   <id>`; an entry that starts passing prints `PASS(was-known-wrong)` /
   `PASS(was-known-error)` / `PASS(was-expected-reject)` — remove the
   expectation-table entry (`cases/known.go`, `fuzz/envelope.go`) in the
   same change and re-record `ronsql_fs_templates` (`.fs_emit_mtr … --cases`).
3. Fuzz: `.fs_fuzz spec --seed 1 --count 200` and `.fs_fuzz envelope
   --seed 1 --count 200` must stay free of unclassified failures; seed 2
   at 1000 for the larger sweep; `--include-hazards` on a disposable
   cluster when a hazard is fixed.
4. Acceptance: `.fs_verify --requirements --all --vectors --label <arm>
   --json …` on base, jit and ng2r2 (reports under
   `requirements_reports/<date>/`).
5. Regression: `./mtr --suite=ronsql_fs,ronsql_fs_jit,ronsql_fs_ng2r2
   --parallel=4 --repeat=3` (strict JIT arming on the Hopsworks tests),
   `ronsql_fs_ng4r2` for topology-dependent items.
6. Performance: `.bench_ronsql fs_hw` / `.bench_sql fs_hw` at sf 1 with
   the plan pins; record in `benchmarks.md` §8.

## 5. Effort and risk (rough, one engineer)

| package | effort | risk |
|---|---|---|
| D1 F9 | hours | none |
| B F1 | days | kernel + API string buffers; F10 may share the root |
| A F0 | ~1 week | planner recognition only; no new execution path |
| C F7 | ~1 week | JSON encoding contract with the Hopsworks client |
| E F14 | days | bound encoding on the CTE body root |
| D2 F5/F21/F2 | 2–3 weeks | DECIMAL through kernel, wire and merge |
| D3 F6/F22 | separate task | semantics decision first |
| D4 F3/F4 | days | printer only |
| F F12 | 2–4 weeks | planner + executor multi-range, largest item |
| G F13 | 1–2 weeks | SPJ plan shape for single-row CTE bodies |
| H F15 | 1 week | DBSPJ null-row path |
| H F18/F19 | days (restore guards) / weeks (implement) | choose per shape |
| H F17/F16 | hours | messages / guards |
| I F10/F11 | separate track | ndbcluster pushdown |
