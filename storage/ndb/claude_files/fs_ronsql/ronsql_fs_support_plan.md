# RonSQL support plan for the Feature Store query generator

**Input:** the RONDB-1121 findings (F0–F22, `mysql-test/suite/ronsql_fs/findings/`),
the requirements report (`phase_e8.md` §4: 12 supported, 4 Hopsworks-gated,
4 unsupported, identical on the interpreter, JIT and ng2r2 arms) and the
`fs_hw` benchmarks (`benchmarks.md` §8).
**Output:** the engine work that makes RonSQL serve every statement the
Hopsworks `PreparedStatementBuilder` emits, correctly and fast enough, with
the RONDB-1121 framework as the acceptance gate for each step.
**Date:** 2026-09-14; WP-J (count-based windows) added 2026-09-28. Engine
fixes live in the engine tree; each work package names the framework
evidence that flips when it lands.

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
| F14 | CTE body bound on a VARCHAR primary key | WRONG RESULT (no rows) — FIXED 2026-09-24 in WP-F F3 (double VARCHAR length prefix on pushed-query constants) | yes (string entity keys + snowflake) — not yet in the manifest | WP-E (engine part done) |
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
| — | RonSQL planner: aggregate or join over a non-aggregating LIMIT CTE (count-based window, e.g. AVG of the last 10 rows) | UNSUPPORTED, clean reject | no (new shape; Hopsworks rejects collect + aggregate: `AGGREGATE_WITH_COLLECT`) | WP-J |

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

**D2. Exact DECIMAL aggregation (F5, F21, F2) — deferred.** The first
version retains signed/unsigned 64-bit integers and double. DECIMAL
conversion and range restrictions remain; scaled DECIMAL values can lose
precision through the double path. Do not retire these findings or promise
exact DECIMAL results based on display-only changes. No accumulator or
wire widening is planned.

**D3. BIGINT SUM overflow semantics (F6, F22) — checked 64-bit.**
Row accumulation and partial-result merges must report NDB error 1860
when an integer addition exceeds its signed/unsigned range. MySQL may
return a wider DECIMAL; this is an intentional difference. Intermediate
overflow can still depend on evaluation order for mixed-sign inputs.
The shared checked merge, API/kernel error propagation, CTE cancellation
and recovery, distributed SUM tests, and RonSQL scalar/grouped boundary
and overflow tests with interpreter/JIT parity are complete (`2f618dae6bf`).
Retire the unchecked-merge finding only with matching regression evidence;
represent the remaining MySQL range difference explicitly.

**D4. Display rules (F3, F4) — in progress.** AVG column formatting is
complete with passing base/JIT regressions (`2ca8fc243f5`); AVG arithmetic
expressions retain the existing four-digit rule. FLOAT MIN/MAX, projection,
and GROUP BY rendering now use source-type metadata through chained CTEs;
SUM/AVG remain double. Strict CLI/HTTP FLOAT regressions and the chained CTE
lookup startup fix passed user testing (`df820fa3540`, pushed). Framework
F4/F22 exemptions are retired and M2.4 validation is complete across base,
JIT, ng2r2, and ng4r2. R-A5-types remains unsupported for F5/F6; original
req-v1 acceptance remains FAIL. See the 2026-09-17 closeout in `phase_e8.md`.
Formatting must not imply exact DECIMAL arithmetic. Remove canonicalization
and retire findings only where strict comparisons demonstrate resolution.

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

**Status (2026-09-24).** Engine part done within WP-F F3: the divergence
was a doubled VARCHAR length prefix on every pushed-query constant
(`NdbQueryBuilder::constValue(ptr, len)` takes the value without it), not
the CTE materialization; spec fuzzer `known-wrong` 2 → 0 and the F14
markers retired. The framework additions below remain.

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

### WP-J — Aggregates over the last N rows (count-based windows) — new shape

Added 2026-09-28. Not emitted by Hopsworks today: this is a new feature
type, so it needs a Hopsworks-side definition and emitter as well as the
engine work below.

**Need.** Features such as "average amount of the last 10 transactions" or
"max fee over the last 20 events": a window defined by a row count instead
of a time span. The natural statement is an aggregate over a last-N CTE:

```
WITH t AS (SELECT `amount`, `fee` FROM `transactions_1`
           WHERE `customer_id` = ? [AND filters]
           ORDER BY `event_time` DESC LIMIT 10)
SELECT COUNT(`amount`) AS `amount_count`, AVG(`amount`) AS `amount_avg`,
       MAX(`fee`) AS `fee_max`
FROM t;
```

MySQL runs it as written (CTE bodies may carry ORDER BY / LIMIT), so it
has a MySQL twin with the same text.

**Mechanism as found.**
- RonSQL rejects the statement: the main query aggregates, so
  `collapse_collect_cte` (`RonSQLPreparer.cpp` ~2069, WP-A) does not
  apply, and the body then fails the non-aggregating CTE gate (~6296:
  `Non-aggregating CTE body is not a single-row key lookup.`).
  Joining the last-N CTE to another table is rejected for the same
  reason.
- A grouped CTE body with ORDER BY / LIMIT is supported: the kernel
  keeps the top N groups at the CTE finalize barrier
  (`analyze_cte_body_orderby_limit` ~6082, `emit_cte_orderby_limit`;
  tests `ronsql_cte_dd_orderby_limit_cte` obc-1..21 on every topology
  and the JIT). Restrictions: ORDER BY names CTE outputs only (not
  string MIN/MAX outputs), at most 8 ORDER BY columns, LIMIT at most
  67 108 863, no OFFSET.
- The collect path (single-table ORDER BY + LIMIT) streams: ordered
  index scan with `SF_OrderBy`, batch = LIMIT per fragment (C3), ordered
  merge in the API, stop after N rows. It reads at most N × fragments
  rows (`fetched=` in `x-ronsql-phases`). fs_hw_collect5 / collect50:
  77 / 78 µs at T=1 on the benchmark box (run 4).
- Hopsworks online tables have no `PARTITION BY`, so they are
  partitioned on the full primary key `(entity…, event_time)`: one
  entity's rows are spread over all fragments. The last N rows exist
  only after the API's ordered merge, so per-fragment partial
  aggregates cannot be combined into the last-N aggregate. The
  aggregation must happen after the merge, in the RonSQL layer. The
  data nodes' aggregation interpreter (`dbtup/AggInterpreter*.cpp`) is
  kernel-only; `NdbAggregator` decodes and merges data-node results and
  has no path that aggregates raw rows.
- Hopsworks rejects collect + aggregate on the same feature group at
  definition time (`AGGREGATE_WITH_COLLECT`, golden fixture
  `definition_collect_and_aggregate.json`).

**Design, in phases.**

*J0 — stop-gap form, no engine change (verify first).* Express the
body as a grouped CTE with one group per row: every output column is a
GROUP BY key, the full primary key first, plus a dummy COUNT(*) as in the
Hopsworks snowflake CTE. The existing kernel top-N then keeps the last N
rows:

```
WITH t AS (SELECT `customer_id`, `event_time`, `amount`, COUNT(*) AS `grp_rows`
           FROM `transactions_1` WHERE `customer_id` = 42
           GROUP BY `customer_id`, `event_time`, `amount`
           ORDER BY `event_time` DESC LIMIT 10)
SELECT COUNT(`amount`), AVG(`amount`) FROM t;
```

The primary key is unique, so each group is one row, the extra keys
change nothing (NULLs included), and the result equals the natural
statement. GROUP BY keys keep their source types. MAX(col) as the carrier
of a non-key column was the first choice and fails as a general form:
a CTE MIN/MAX output is widened to its wire type (INT → BIGINT,
FLOAT → DOUBLE, DECIMAL → BIGINT / DOUBLE with DECIMAL's precision loss,
temporal → 8-byte unsigned), and a widened INT join key fails the pushed
join's operand type check (`Failed to create child operation`, lastn-12
on 2026-09-29). Joins and GROUP BY in the main
query over the CTE work through the existing grouped-CTE machinery
(obc-8, obc-10, obc-11). To verify: GROUP BY on the TIMESTAMP key, AVG
in a main query rooted on the CTE (the main scope decomposes AVG into
SUM + COUNT). Cost: the body reads the entity's whole history and
materializes one group per row, so it grows with the history length, not
with N (F24 measured ~6 µs per group at 150k groups; the small-group cost
is not measured). J0 is also the correctness oracle for J2.

*J0 status (2026-09-29).* First run: lastn-P1 and lastn-1..6 pass, so
the stop-gap form works, including main-query AVG over the CTE and GROUP
BY on the TIMESTAMP key. lastn-7 (time window in the CTE body) found F28:
TIMESTAMP / DATETIME constants had the widest width instead of the
column's, which fails CTE-body bounds with 4803 and overran TIMESTAMP(0)
key slots (`ronsql_fs/findings/BUGS_TODO.md`). Fix in `encode_constant`;
regression cases lastn-W1..W3 added. Second run, with the fix: lastn-7..11
and W1..W3 pass, including string and temporal MIN/MAX in the main query,
main GROUP BY on a CTE output and a chained CTE. lastn-12 (INNER JOIN
onto `lastn_merchants`) failed on the MAX() widening described above, so
every J0 body now uses the GROUP-BY-every-column form. Third run: every
case passes; the base result is committed in `3ab0fcd1df2` (the mirrors
are recorded with J1).

The test: `mysql-test/suite/ronsql_cte/include/body_lastn_agg.inc`, run by
`ronsql_cte_dd_lastn_agg` in `ronsql_cte`, the four `_ng*` layouts and
`ronsql_cte_jit`. Table `lastn_tx` has the Hopsworks layout (PRIMARY KEY
(customer_id, event_time), no PARTITION BY) plus a `lastn_merchants`
dimension. `lastn_twin_check.inc` diffs MySQL's natural statement against
MySQL's J0 form; `ronsql_compare.inc` then strict-diffs RonSQL against
MySQL on the J0 form. Cases:
- lastn-P1 pinned the rejection of the natural statement; since J1 it
  pins the rewrite in EXPLAIN.
- lastn-1..8 are the core: the aggregate set, N above the row count, no
  rows, NULLs, LIMIT 1, oldest N, a time window tighter than N, and a
  residual filter.
- lastn-W1..W3 are F28's regressions on the TIMESTAMP(0) key: a
  primary-key lookup, an IN list, and a single-row CTE.
- lastn-9..13 are the uncertain features, ordered last so the core is
  recorded first: string and temporal MIN/MAX in the main query, main
  GROUP BY on a CTE output, a chained CTE, and INNER / LEFT joins onto
  the dimension.

*J1 — parse-time rewrite of the natural statement into J0.*
`RonSQLPreparer::rewrite_lastn_cte_bodies()`, called in `parse()` after
`collapse_collect_cte()` and before the CTE aggregate registration and
`analyze_ctes()`. A CTE body is rewritten when all of these hold
(anything else keeps today's rules):
- ORDER BY and LIMIT are present; without ORDER BY the kept rows are
  arbitrary.
- The CTE is read only as a FROM root, by the main query or a later CTE
  body. As a join child it would be probed by a subset of its GROUP BY
  keys, the partial-key CTE lookup behind F18. A main query rooted on it
  must aggregate or join; a projection-only main over the CTE alone is
  WP-A's collapse and keeps its rules (`ronsql_cte_collect_collapse`
  cc-P1, cc-P4, cc-P5 unchanged).
- The body reads one real table with plain, distinct column outputs and
  no GROUP BY, HAVING, joins, aggregates, arithmetic or subqueries.
- The WHERE does not bind the whole primary key by equality with
  constants; such a body has at most one row and stays on the
  CTE_SINGLE_ROW path (obc-19 / obc-20 unchanged).
- Every primary-key column is an output. Otherwise the statement is
  rejected, naming the missing columns (lastn-P2).

The rewrite sets GROUP BY to every output in output order and appends a
hidden `COUNT(*) AS ronsql$lastn_rows`, in a new aggregate compiler for
the body. Output names and types stay as written. The primary key needs
the dictionary, which `load()` only fetches after the CTE analysis, so
the function looks the body's table up itself, and only for bodies that
already match the rest of the pattern. There is no rewrite in ParseOnly
mode or without a connection. EXPLAIN reports `CTE 't' served as the
last N rows: …` (lastn-P1 pins it). Known limits: an ORDER BY on a column
the body does not select gets the grouped-body rejection (non-output
ORDER BY column), and ties on a non-unique ORDER BY column make the kept
set arbitrary on both engines. The 128-column GROUP BY limit is far above
any feature group's width.

*J1 status (2026-09-29): written, not built.* `ronsql_cte_dd_lastn_agg`
now also runs every natural statement on RonSQL (`== J1 ==`), pins the
rewrite in EXPLAIN (lastn-P1) and the missing-key rejection (lastn-P2),
spreads lastn-12 over three `mcc` groups, and adds lastn-14 (the last N
rows joined to a dimension, projection-only main).

*J2 — per-fragment limit on the ordered body scan (revised 2026-09-29).*
The first design (collect scan plus a new aggregation evaluator in the
RonSQL layer) is replaced by one that keeps every aggregate in the kernel.
When a J1-rewritten body's index scan delivers the ORDER BY order within
each fragment, only a fragment's first N rows can be among the global last
N: any row of the global top N is in the top N of its own fragment. So
each fragment scan may stop after N delivered rows, and the kernel's CTE
top-N keeps the global N from at most N × fragments groups.

The mechanism exists: Phase I.10 serves scalar MIN/MAX CTE bodies with an
ordered index scan and `NdbQueryOptions::setMaxRows`, a per-fragment row
limit after which DBSPJ closes the fragment scan, and puts the aggregation
on a self-join `readTuple` leaf. The leaf is needed because rows
aggregated in place on the scan are never reported to DBSPJ and would not
count towards maxRows. J2 uses the same shape with the body's bounds and
residual filter on the scan root (`emit_index_scan_root`), so only
matching rows count, and the J1 grouped aggregation on the leaf.

`select_cte_body_lastn_scan()` (called in `plan_cte_bodies` after the root
scan config and the I.10 check) applies it when all of these hold:
- the CTE was rewritten by J1;
- the body is a single-op body whose root is the INDEX_SCAN chosen by
  `select_root_scan_config`, with one range (an IN-list multi-range scan
  delivers each range in order, but not the ranges);
- LIMIT >= 1;
- `body_index_serves_orderby()` holds: the ORDER BY columns are stored
  columns of the root table matching the index columns in order, all in
  one direction, where index columns bound by equality may be skipped.
  This is the body-scope twin of `index_serves_orderby()`.

For the Hopsworks layout, PRIMARY KEY (entity, event_time) with the entity
bound and ORDER BY event_time DESC, that is always the case, with or
without a time window. EXPLAIN adds `[last-N DESC maxRows=N per fragment]`
to the body root line. Anything else keeps the J1 plan: an ORDER BY on a
non-index column (lastn-15), or no index bound (lastn-16).

*J2 status: committed `83046377ccb`, then measured slower than J1 and
switched off (2026-09-29).* The spot run (`benchmarks.md` §8, "WP-J spot
run"), last 10 of 300 rows, 1 thread:

| plan | avg |
|---|---|
| J2 | 2.54 ms |
| J1 grouped body | 1.90 ms |
| MySQL | 0.62 ms |

Every row the limited scan keeps goes through the self-join lookup, about
17 µs per row, against about 3.5 µs per grouped row for J1, so J2 only
wins for histories far longer than N × fragments. RonSQL cannot see that
at plan time.

`select_cte_body_lastn_scan()` returns at once
(`kLastNPerFragmentLimit = false`). The code stays for such workloads and
for a kernel-side row limit on in-place aggregation scans, which would
remove the self-join leaf. lastn-P1 and lastn-15 pin the absent EXPLAIN
annotation.

*J1 with several CTEs.* Each CTE in the WITH list is judged on its own.
Several last-N CTEs in one statement are all rewritten when each is read
only as a FROM root, e.g. two branches each aggregated in its own scalar
CTE and combined with a comma join (lastn-17). A last-N CTE joined as a
child keeps the rejection.

*J5 — the fast path: collect scan plus aggregation in the RonSQL layer
(next, agreed 2026-09-29; design, not implemented).*

Why. The WP-J spot run (`benchmarks.md` §8) puts the CTE protocol's fixed
cost at about 0.6 ms (`fs_hw_snow1_point`), and J1 adds about 3.5 µs per
history row on top. The ordered collect scan costs 0.40 ms
(`fs_hw_collect5`), below MySQL's 0.62 ms. A path that reads the last N
rows that way and aggregates them without the CTE protocol is expected
near the collect cost: about 1.5× faster than MySQL and 4–5× faster than
J1 on 300-row histories.

Scope, v1:
- The statement has exactly one CTE, and the main query reads FROM it
  alone: no joins, WHERE, GROUP BY, HAVING, ORDER BY or LIMIT on the main
  query.
- Every main output is `COUNT(*)`, or `COUNT` / `SUM` / `MIN` / `MAX` /
  `AVG` of a plain column of the CTE. No arithmetic or GREATEST/LEAST in
  the arguments, so the main aggregation program holds only `Load`s and
  aggregate instructions.
- The body has `collapse_collect_cte()`'s shape: one real table, plain
  and distinct columns, ORDER BY and LIMIT >= 1, and no GROUP BY, HAVING,
  aggregates or subqueries.
- Unlike J1, the body need not select the primary key: J5 aggregates the
  delivered rows themselves, so no grouping trick is involved.
- Everything else keeps J1 (joins, main GROUP BY, several CTEs) or
  today's rules.

Execution:
1. **Recognition and rewrite (parse time).** A new
   `route_lastn_aggregate_to_api()` runs before
   `rewrite_lastn_cte_bodies()`, so J1 never sees a J5 statement.
   - It rewrites like `collapse_collect_cte()`. The root takes the body's
     table, WHERE, ORDER BY and LIMIT; the CTE list is dropped; the main
     outputs' column references are redirected to the body's columns
     (the collapse's col_idx remapping). The main aggregates are then
     registered against the base table, as for a single-table aggregate.
   - It sets a new `m_api_side_aggregation`, so the aggregation program is
     never attached to the scan.
2. **Scan.** `execute_single_table_passthrough()` runs unchanged:
   - the PK-lookup arm, index-order streaming with batch = LIMIT, or the
     buffered client-side sort;
   - LIMIT applied after the ordered merge, or after the sort.

   Its three print sites (PK arm, streaming, sorted output) go through a
   row sink: print today, aggregate under J5. The scan reads the columns
   the program loads.
3. **Aggregation.** The main `NdbAggregator` is built from the compiled
   program exactly as for the pushdown path, but never attached to a
   scan. For each delivered row, and for each aggregate slot:
   - decode the slot's source column into a `Register` with the shared
     decoder (item 5);
   - turn it into a one-row partial `AggResItem`: COUNT becomes 1, or 0
     for NULL; SUM / MIN / MAX get the decoded register, with `is_null`
     for NULL;
   - pass all slots to `NdbAggregator::MergeLocalGroup()` (item 4).

   The existing merge rules then do the arithmetic, exactly as they merge
   partials from the data nodes. That covers checked 64-bit SUM (error
   1860), DOUBLE SUM, MIN/MAX across signedness, and string MIN/MAX with
   the column collation. AVG is the main scope's SUM + COUNT
   decomposition, printed with `PRINT_AVG` as today.
4. **NDB API: `NdbAggregator::MergeLocalGroup(gb_key, gb_len, items)`.**
   Factored out of `ProcessRes`' per-group body: insert or merge, the
   RONDB-831 COUNT fixup, and the NULL / first-contribution handling.
   `ProcessRes` calls it, so there is one merge path.
   - String slots arrive with caller-owned `val_ptr` buffers in the
     `resolveStringSlots` layout, and an insert copies them.
   - It returns 0 or the NDB error code (1860).
   - The scalar case (`n_gb_cols == 0`) is the only one J5 v1 uses.
5. **Shared decode.** The per-type switch of
   `AggInterpreterBase::loadColumnTypedFromBuf` moves into one inline
   helper in `NdbAggregationCommon.hpp` (or a new sibling header), which
   the kernel and the API both call:
   ```
   aggLoadColumn(type, is_unsigned, precision, scale, data, byte_size,
                 is_null, Register* out, decimal_t* scratch) -> error
   ```
   - It covers integer widths, unsigned, DATE / YEAR / DATETIME2 / TIME2
     / TIMESTAMP2 as packed unsigned values, FLOAT / DOUBLE, DECIMAL
     through `bin2decimal` / `decimal2double` / `decimal2longlong`, and
     strings.
   - For the kernel this is a pure refactor: same behaviour, still
     inline.
   - The row bytes J5 decodes come from `NdbRecAttr`, which carries the
     same NDB storage format as the kernel's attribute read. So the value
     conversion is shared code, not a mirror.
6. **Output.** The existing aggregate `ResultPrinter` path prints the
   aggregator's result, so display rules (AVG scale, DECIMAL scale, FLOAT)
   are the pushdown path's.
7. **EXPLAIN.** A line such as `CTE 't' aggregated in RonSQL over the
   ORDER BY / LIMIT scan of its body (last N rows)`, after the collapse's
   scan-plan lines.

To confirm while implementing:
- **The empty scalar result.** An entity with no rows must print COUNT 0
  and NULL for the rest, as the pushdown path does. Check how
  `ResultPrinter` handles an aggregator that received no group, or feed
  one all-NULL partial.
- **The string register layout.** Check the kernel's CHAR / VARCHAR load
  (pointer and length) and its conversion to the merge layout.
- **The 1860 surfacing.** A merge error in the API must produce the same
  RonSQL error class, HTTP status and message as the kernel-detected
  overflow.
- **The kernel refactor.** It must leave the JIT's load lowering
  untouched, and every aggregate suite unchanged: ronsql, ronsql_jit,
  ronsql_cte*, ronsql_fs* and the ndb pushdown-agg tests.

Tests:
- **`ronsql_cte_dd_lastn_agg`.**
  - The natural statements in scope run J5. That is lastn-1..9, 15 (the
    buffered-sort arm) and 16 (a table scan with sort). Their J0 forms
    keep the kernel path, which is the differential, and everything is
    compared with MySQL.
  - lastn-P1 pins the J5 EXPLAIN line.
  - New cases: DECIMAL(12,2), DOUBLE and FLOAT columns (added to
    `lastn_tx`); a BIGINT SUM overflow (1860 raised by the API merge); a
    string MIN/MAX across a case-insensitive tie ('Grocery' vs
    'grocery'); a body whose WHERE binds the whole primary key (the PK
    arm); a body without the primary key in its outputs.
- **`NdbAggregatorMerge-t`.** `MergeLocalGroup` cases for insert, merge,
  NULLs, COUNT, overflow and strings.
- **Benchmarks.** `fs_hw_agg_last10`, `_last100` and `_last10_tx300` pin
  the J5 line. The target is `fs_hw_collect5` plus a few tens of µs, and
  below MySQL. The `_tx300_grouped` entry stays the CTE-plan baseline.

Order of work:
1. The shared decode refactor, kernel side only, with the aggregate
   suites green.
2. `MergeLocalGroup` plus its unit tests.
3. The RonSQL route, row sink and EXPLAIN, with the lastn tests.
4. The benchmark pins and a spot run.

Effort: 1–2 weeks.

*J5 step 1 status (2026-09-29): written, not built.* The new header
`storage/ndb/include/util/AggColumnLoad.hpp` holds `aggTypeSupported`,
`aggIsUnsignedType`, `aggAlignedType`, `aggLoadColumnValue` and
`aggStringPayload`, with errors returned as an `AggLoadStatus` the kernel
maps to its `ZAGG_*` codes.
- `AggInterpreterBase::loadColumnTypedFromBuf` calls the decode.
  `TypeSupported`, `IsUnsigned` and `AlignedType` delegate to the shared
  functions.
- The string capture (charset, declared size, read high-water mark) stays
  in the kernel and uses `aggStringPayload`.
- The JIT bridge (`DbtupJitGlue.cpp`) keeps its own numeric decode for its
  register layout. A later cleanup could make it call the helper.
- The new unit test `AggColumnLoad-t` (`src/ndbapi/AggColumnLoadTest.cpp`)
  covers every integer width signed and unsigned at their limits,
  FLOAT / DOUBLE, DATE / YEAR / packed DATETIME2 and TIMESTAMP2 at
  several fraction widths, DECIMAL signed and unsigned at scale 0 and 2
  (plus the negative-unsigned conversion error), NULL, the string payload
  helper and an unsupported type.

*J5 step 1 committed `b1e38db2150` (aggregate suites unchanged).*

*J5 step 2 status (2026-09-29): written, not built.*
`NdbAggregator::MergeLocalGroup(gb_key, gb_len, items)` merges one partial
built in memory exactly as `ProcessRes` merges one from the wire.
- `ProcessRes`' two per-slot loops became private helpers, each with its
  existing NULL / UNDEFINED ordering kept:
  - `mergeGroupSlots` (a partial into an existing group);
  - `mergeScalarSlots` (into `agg_results_`).

  Around them: `fixupCountSlots` (RONDB-831, a new group's COUNT starts
  at 0) and `freeUntransferredStrings`. `ProcessRes` calls them.
- `MergeLocalGroup` first deep-copies the caller's string slots into the
  `resolveStringSlots` layout (`copyStringSlots`, which also sets NULL
  string slots to nullptr). A new group is allocated as one key-plus-slots
  block, like the wire path.
- The empty scalar result needs nothing special. `Finalize()` starts COUNT
  slots at 0 and the rest at NULL, so an aggregator that received no row
  prints COUNT 0 and NULLs (lastn-3 checks it end to end in step 3).
- `NdbAggregatorMerge-t` gains:
  - 18 `runLocalCase` cases, the in-memory twins of the wire string /
    SUM / overflow cases, with the caller's buffers overwritten after the
    merge to prove the copies;
  - one COUNT fix-up case.

*J5 step 2 committed `c851929eff6`.*

*J5 step 3 status (2026-09-30): green. The lastn suites were recorded (JIT fallback delta 0), and the ronsql, ronsql_cte and ronsql_fs regressions pass.*
- **Route.** `route_lastn_aggregate_to_api()` runs in `parse()` right
  after `flatten_single_group_cte()`, before the main aggregates are bound.
  It checks the v1 scope, gives the main outputs a new compiler whose loads
  name the body columns (COUNT(*) keeps its constant), and moves the body's
  table, WHERE, ORDER BY and LIMIT to the root. It sets
  `m_api_side_aggregation`. Bare ORDER BY names that a main output alias
  could capture are qualified with the body alias, as in the collapse.
- **Planning.** J5 is treated as a pass-through wherever the scan is
  chosen:
  - ORDER BY index candidates (`plan_index_and_filter`);
  - the single-key primary key lookup (`detect_pk_lookup`);
  - the EXPLAIN ORDER BY strategy line.

  `is_count_star_only_query()` declines J5, since without a body WHERE it
  would count the whole table. A program that loads no column (COUNT(*)
  alone) reads the first ORDER BY column, because the pass-through reads at
  least one.
- **Printer.** The aggregate `ResultPrinter` gets a copy of the statement
  without ORDER BY and LIMIT, which belong to the scan.
- **Execution.** `execute_api_side_aggregate()` builds the `NdbAggregator`
  with `programAggregator()` and `Finalize()`. It never sends it.
  - `execute_single_table_passthrough(api_rows)` reads the columns the
    program loads. Its three delivery sites hand rows to
    `ApiRowAggregation::add_row` instead of printing: the PK arm, the
    streaming arm (LIMIT cutoff kept), and the sorted arm (the first
    min(LIMIT, n) rows in sort order, via
    `for_each_sorted_passthrough_row`).
  - `add_row` runs the program per row: loads go through
    `aggLoadColumnValue`, `LoadConstantInteger` and `Mov` are handled, and
    each aggregate builds the kernel's one-row partial. Then it calls
    `MergeLocalGroup`.
  - A failed merge is raised with `throw_classified_ndb_error`, so the
    message is the scan path's "Failed to execute scan aggregation.", with
    the same class and NDB code (1860 → semantic, HTTP 400).
  - `PrepareResults()`, then the aggregate printer.
- **EXPLAIN.** "CTE 't' aggregated in RonSQL over the ORDER BY / LIMIT scan
  of its body (last N rows): ...".
- **Fix on the way.** `collapse_collect_cte()` could mark a column it had
  just qualified past the end of its `live` array, because
  `qualified_column_name_to_idx` may add a registry entry. Both it and the
  J5 route now skip such entries.
- **Tests** (`body_lastn_agg.inc`).
  - P1 now pins J5: the J5 line, SF_OrderBy | SF_Descending, and neither
    the J1 line nor CTE definitions.
  - New P1b pins J1 for a main GROUP BY.
  - P2 is now a J1-only statement, because the old P2 body is served by
    J5.
  - lastn-1..9, 15 and 16 are labelled `== J5 ==`. lastn-15 pins the
    client-side sort arm.
  - New cases:
    - 18: DECIMAL(12,2), DOUBLE and FLOAT columns, which were added to
      `lastn_tx` with binary-exact values;
    - 19: string MIN/MAX where the byte order differs from the collation
      order (column `label`);
    - 20: bodies without the primary key, including an output alias and
      ORDER BY on a non-output column, and COUNT(*) alone with and without
      a body WHERE;
    - 21: a whole-PK body, served by the PK lookup arm (EXPLAIN pin), with
      a hit and a miss;
    - 22: BIGINT SUM over `lastn_big`, with no overflow at LIMIT 2 and
      1860 at LIMIT 3 (via `ronsql_sum64_check.inc`).
  - Every result file was recorded again; the JIT mirror's fallback delta
    stayed 0.
- **Not done.** The pass-through transaction carries no rate-limit
  identity (`setUserId`), exactly like the collect form it shares.

*J5 step 3 committed `bf75969256c`.*

*J5 step 4 status (2026-09-30): pins written, spot run pending.*
- `fs_hw_agg_last10`, `_last100` and `_last10_tx300` now pin three things:
  the J5 EXPLAIN line, `Execute as index scan.`, and the descending
  index-order line.
- `_tx300_grouped` keeps the J0 CTE plan as the baseline.
- The golden registry dump and `benchmarks.md` §2 and §7 follow.
- The spot run repeats the 2026-09-29 one: 1 thread × 5000 requests on
  the user's cluster, with `collect5` as the floor and `.bench_sql` for
  MySQL. Its table goes into `benchmarks.md` §8.

Later (J5b and beyond):
- **Main GROUP BY over the last N.** The group key must be encoded
  exactly as the kernel encodes GB keys, collation included, which is a
  separate risk.
- **Several independent last-N branches combined with a comma join**
  (lastn-17's shape). Their scans are defined in one transaction and
  executed together, then aggregated per branch and combined.
- **Joins over a last-N CTE** stay on J1.

*J2 benchmarks (2026-09-29).* Four `fs_hw` entries (`benchmarks.md` §2):
- `fs_hw_agg_last10` and `fs_hw_agg_last100` use `{KEY}`, the serving mix
  of history lengths.
- `fs_hw_agg_last10_tx300` uses customers with 300 rows (new placeholder
  `{TXKEY:n}`).
- `fs_hw_agg_last10_tx300_grouped` is the hand-written J0 form, the
  CTE-plan baseline.

MySQL runs the same natural text through `.bench_sql`.

*J3 — last N enriched with a dimension, then aggregated* (e.g. distinct
merchant categories among the last 20 transactions). J1 serves it
functionally (lastn-12..14); J5's scope excludes joins, so this stays on
J1 until a J5 extension adds the lookups.

*J4 — batch (last N per entity for an IN list).* Needs a per-key
ordered range with a per-key limit, which the multi-range scan (WP-F F2)
does not provide; alternatively one ordered scan per key in one
transaction. Hopsworks gates collect batch today
(`collect_batch_gated.json`), so this waits for a Hopsworks decision.

*Hopsworks side.* A count-based window on aggregate features (e.g.
`lastN` beside `aggregateWindow`), lifting `AGGREGATE_WITH_COLLECT` for
that combination or adding a separate definition, and an emitter for
the shape above (new catalog shape S11, both engines' text). The
framework follows `adding_a_shape.md`: emitter port, golden fixture,
requirements row, vector oracle fold, fuzzer production, fs_hw entry.

**Evidence.**
- J0: a new MTR test (`ronsql_cte_dd_lastn_agg`, JIT and topology mirrors)
  comparing the J0 form with MySQL's natural statement. Cases: N below
  the entity's row count, N above it, an entity with no rows (COUNT 0,
  AVG / SUM / MIN / MAX NULL), NULLs in the aggregated column, a time
  window combined with LIMIT, a main-query GROUP BY, a LEFT JOIN
  onto a dimension.
- J1: the same cases written naturally, strict-diffed against MySQL;
  EXPLAIN pins the rewrite; clean rejects for ORDER BY on a non-key
  column, string MIN/MAX as the ORDER BY key and OFFSET.
- J2: the J1 cases on the limited scan, with the J0 forms (full grouped
  scan) as the differential and EXPLAIN pins for the annotation and its
  absence. `fetched=` does not apply: the body rows stay in the kernel.
  Aggregate semantics are unchanged, since the kernel still computes
  them. Bench entries `fs_hw_agg_last10`, `_last100` and `_last10_tx300`
  run against MySQL (same text) and against
  `fs_hw_agg_last10_tx300_grouped` (the J1-without-J2 baseline), recorded
  in `benchmarks.md` §8.
- Hopsworks: golden fixture for the new definition, requirement row
  SUPPORTED on base / jit / ng2r2.

## 3. Sequencing

| milestone | packages | acceptance evidence |
|---|---|---|
| **M1 — emitted branches served** (detail: `m1_plan.md`) — **DONE 2026-09-15 on RONDB-1124** | error codes (M1.0), D1 (F9), B (F1), A (F0), C (F7) | requirements report (`requirements_reports/2026-09-15/`): R-S6, R-F1, R-A2-binary SUPPORTED on base / jit / ng2r2; only R-A5-types left (F4/F5/F6); RonSQL errors carry 400/413/503/500 by class |
| **M2 — 64-bit numeric fidelity** (detail: `m2_plan.md`) | D3 checked overflow, D4 display; D2 deferred | base / jit / ng2r2 / ng4r2 verify the limited contract; retain unmet exact-DECIMAL requirements |
| **M3 — serving performance** (census: `m3_plan.md`; experiments: `m3_experiments.md`; WP-F detail: `m3_wpf_plan.md`) | F (F12 → F23, first), then F24 many-group aggregation, F25 idle-wake stall, throughput; G (F13) closed by RONDB-1120 (192–227 µs); F27 node failure investigated in parallel | `fs_hw` and `core` targets met (`m3_wpf_plan.md` §0), plan pins re-recorded, `benchmarks.md` §8 |
| **M4 — hardening** | E (F14 + manifest rows), H (F15, F18, F19, F17, F16) | spec fuzzer `known-wrong` = 0, envelope fuzzer `known-wrong` = 0, hazards list shrinks |
| **parallel** | I (F10, F11) | `.bench_sql fs_hw` with pushdown on, no crash / no 4120 |
| **M5 — count-based windows** (new shape, with Hopsworks) | J: J0 verify the stop-gap form, J1 rewrite, J2 per-fragment limit (off: slower), J5 collect scan + RonSQL-layer aggregation; J3 / J4 later | `ronsql_cte_dd_lastn_agg` + mirrors strict against MySQL, `fs_hw_agg_last10` / `_last100` recorded, S11 requirement SUPPORTED once Hopsworks emits it |

M1 is small and high-value: D1 is a one-line fix, B and C are contained,
and A maps onto an execution path that already exists. M2 now focuses on
checked 64-bit overflow and display rules; exact DECIMAL accumulation is
deferred. See the detailed plan for completed work and remaining gates. M3 is
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
| J0 last-N stop-gap | days (tests only) | TIMESTAMP GROUP BY key, main-scope AVG over a CTE |
| J1 last-N rewrite | ~1 week | planner only, like WP-A; cost grows with the entity's history |
| J2 per-fragment limit | done, off | measured slower than J1 (self-join leaf per kept row) |
| J5 collect scan + RonSQL-layer aggregation | 1–2 weeks | per-row conversion to the kernel's accumulator types must mirror AggInterpreter exactly |
| J3 / J4 | after J2 | batched lookups; per-key limited ranges; Hopsworks decisions |
