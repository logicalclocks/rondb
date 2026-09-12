# Benchmark design (P5, v1 — 2026-09-08)

**Status: E5 code written 2026-09-10 (`phase_e5.md`): the `fs_hw`
registry is generated from the emitter, `.bench_ronsql fs_hw` /
`.bench_sql fs_hw` and `ronsql_bench_matrix.py --queries fs_hw --load fs`
run it; the results tables in §8 are empty until the first run.** Builds on the rondb-cli benchmark
registry (`tools/rondb-cli/internal/shell/ronsql_bench.go`), the
`ronsqlcrunch` cluster (`mysql-test/suite/ronsqlcrunch/t/setup.test`),
and the matrix driver
(`storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py`),
documented in `pushdown_join_aggregation/ronsql_cli_benchmarks.md` and
`compiled_interpreter/ronsql_bench_matrix.md`.

---

## 1. What the benchmarks answer

1. **RonSQL vs MySQL on the exact Hopsworks statement** — the same
   bound template text on both engines (`.bench_ronsql fs_hw_X` vs
   `.bench_sql fs_hw_X`). A slower RonSQL is an engine performance
   issue.
2. **RonSQL template vs the MySQL production twin** — what a Hopsworks
   client actually experiences on each path (`fs_hw_X` vs
   `fs_hw_X_twin`, MySQL only). This is the number that decides whether
   the REST/RonSQL path is worth preferring for a shape.
3. **Scaling knobs per shape** — batch size (1 / 10 / 100 / 1000),
   collect N (5 / 50), snowflake depth (1 / 2), hash-only vs
   hash+ordered PK, client threads (1 / 8), interpreter vs JIT
   (`CompiledInterpreter OFF/ON`), `FRAGS_PER_WORKER` hint.
4. **Where the time goes** — the `x-ronsql-phases` header (parse …
   execute, rows) versus HTTP + client overhead, and the known RDRS
   schema-cache gap (`listIndexes` ≈ 400 µs per request,
   `ronsql_bench_matrix.md`), reported as its own line so engine time
   is not mistaken for RDRS time.

---

## 2. Registry entries (`fs_hw` category)

All entries run against database `fs_bench` (loaded by `.fs_load 1 8
500`), are **generated at init from `fsq/cases`** (the same emitter
that produces the golden tests), and use the placeholder scheme of §3.
`Twin` entries are `MySQLOnly` and carry the production MySQL
statement from `fsq/mysqltwin`.

| name | shape | statement (bound form) | placeholders | rows |
|---|---|---|---|---|
| `fs_hw_floor` | — | `SELECT COUNT(*) FROM countries_1;` | — | 1 |
| `fs_hw_agg_point` | S1 | `SELECT COUNT(amount) AS amount_count, SUM(amount) AS amount_sum, MAX(amount) AS amount_max, MIN(fee) AS fee_min FROM transactions_1 WHERE customer_id = {KEY};` | KEY | 1 |
| `fs_hw_agg_window7d` | S2 | S1 + `AND event_time >= {NOW-7d}` | KEY, NOW-7d | 1 |
| `fs_hw_agg_greatest` | S5 | `SELECT COUNT(*) AS count, MAX(GREATEST(amount, fee)) AS amount_fee_greatest, MAX(LEAST(amount, fee)) AS amount_fee_least FROM transactions_1 WHERE customer_id = {KEY};` | KEY | 1 |
| `fs_hw_agg_filter` | S4 | S1 + `AND category = 'grocery' AND amount >= 300` | KEY | 1 |
| `fs_hw_agg_batch10` / `_batch100` / `_batch1000` | S3 | `SELECT customer_id, COUNT(amount) AS amount_count, SUM(amount) AS amount_sum FROM transactions_1 WHERE customer_id IN ({KEYS:10}) GROUP BY customer_id;` | KEYS:n | ≤ n |
| `fs_hw_agg_batch100_window` | S3+S2 | batch 100 + `AND event_time >= {NOW-30d}` | KEYS:100, NOW-30d | ≤ 100 |
| `fs_hw_collect5` / `_collect50` | S6b | `SELECT customer_id, event_time, amount, category FROM transactions_1 WHERE customer_id = {KEY} ORDER BY event_time DESC LIMIT 5;` | KEY | ≤ N |
| `fs_hw_collect5_cte` | S6 | the Hopsworks CTE form of the above | KEY | ≤ 5 (or clean reject, see R1) |
| `fs_hw_collect5_twin` | S6 twin | MySQL `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_time DESC)` statement with rank `<= 5` | KEY | ≤ 5 |
| `fs_hw_snow1_point` | S7 | `WITH b AS (SELECT region_id, COUNT(*) AS hw_cnt FROM customers_1 WHERE customer_id = {KEY} GROUP BY region_id) SELECT j2.region_name AS r_region_name, j2.population AS r_population FROM b JOIN regions_1 AS j2 ON j2.region_id = b.region_id;` | KEY | ≤ 1 |
| `fs_hw_snow2_point` | S7 | + `JOIN countries_1 AS j3 ON j3.country_id = j2.country_id`, projecting `j3.country_name AS c_country_name` too | KEY | ≤ 1 |
| `fs_hw_snow1_batch100` | S7 batch | CTE selects `customer_id, region_id`, `WHERE customer_id IN ({KEYS:100}) GROUP BY customer_id, region_id`, projection adds `b.customer_id AS customer_id` | KEYS:100 | ≤ 100 |
| `fs_hw_snow2_left_chain` | S8 | the per-chain template for the `countries` node (two INNER hops, one projection) | KEY | ≤ 1 |
| `fs_hw_snow2_left_single` | S8b | same as `fs_hw_snow2_point` with `LEFT JOIN` hops | KEY | 1 |
| `fs_hw_snow1_twin` / `fs_hw_snow2_twin` | S7 twin | MySQL `SELECT … FROM customers_1 AS fg0 INNER JOIN regions_1 AS j2 … WHERE fg0.customer_id = {KEY}`; all hops INNER | KEY | ≤ 1 |
| `fs_hw_strkey_point` | S10 | `fs_hw_agg_point` over `transactions_str_1` with `customer_key = {SKEY}` | SKEY | 1 |
| `fs_hw_strkey_batch100` | S10+S3 | batch 100 over string keys | SKEYS:100 | ≤ 100 |
| `fs_hw_composite_point` | S9 | `SELECT COUNT(*) AS count, SUM(delta) AS delta_sum FROM balance_hist_1 WHERE account_id = {ACCT} AND currency = {CUR} AND event_time >= {NOW-90d};` | ACCT, CUR, NOW-90d | 1 |
| `fs_hw_hash_point` | S1 on hash-only PK | `fs_hw_agg_point` over `transactions_hash_1` | KEY | 1 |
| `fs_hw_sessions_window2h` | S2, TIMESTAMP(3) + ttl index | `SELECT COUNT(*) AS count, SUM(duration) AS duration_sum FROM sessions_1 WHERE customer_id = {KEY} AND event_time >= {NOW-2h};` | KEY, NOW-2h | 1 |

Twenty-five entries. Names are stable identifiers (the shape ids in
`shape_catalog.md` are the cross-reference); the exact SQL is whatever
the emitter produces for the corresponding case, and a golden dump
(`fsq/cases/testdata/fs_hw_registry.golden`) lets reviewers read it
without running the CLI.

---

## 3. Placeholders

Extends the existing `{KEY}` / `{KEY2}` substitution
(`substituteBenchKey`, `ronsql_bench.go:809`) into a small resolver
driven by the case's parameter list:

| placeholder | value | source |
|---|---|---|
| `{KEY}` | random integer entity key in `[1, maxKey]` | existing; `KeySQL = "SELECT MAX(customer_id) FROM fs_bench.customers_1"`, `KeyDefault` from `sf` |
| `{KEYS:n}` | `n` distinct random keys, comma-separated | new |
| `{SKEY}` | a customer in the target string-history domain `1..E/10`, rendered by `fsq/data`; explicit miss cases separate | new |
| `{SKEYS:n}` | list of `n` | new |
| `{ACCT}`, `{CUR}` | random account and one of its currencies (`1 + a mod 3` choices) | new |
| `{NOW-7d}` | TIMESTAMP literal `FS_NOW − 7 days` (also `1h`, `2h`, `30d`, `90d`) | new; `--now` overrides `FS_NOW` |

Keys are drawn uniformly, so about 1/16 of point reads hit a customer
with no rows and 1/16 hit a 300-row customer; the mix is intentional
(it is what serving sees) and the per-request `rows` phase counter
makes the distribution visible. `--key-class` (E5 stretch) restricts
draws to one class for micro-benchmarks.

A case declares its target-table key domain; batch resolvers use that
same domain and reject requests for more distinct keys than it holds.
Use identical seeded key sequences for paired engine/twin runs.
Before timing, verify representative hit/miss results and expected
matched-row counts, not just scalar aggregate output rows. The 2h
sessions window includes the newest 90-minute-old row for nonempty
entities; the old 1h window matched no generated sessions.
S7 comparisons use INNER twins. Comparing the full S8 chain set with
a LEFT twin requires a vector-read benchmark; timing one chain cannot
stand in for that comparison.

---

## 4. Runners

- `.bench_ronsql fs_hw_<name> [T] [N]` and `.bench_sql fs_hw_<name>
  [T] [N]` — unchanged runners; `fs_hw` becomes a fourth listing
  section in `.bench_ronsql list` / `.bench_sql list`, and `all`
  includes it only when the `fs_bench` database exists (probe with
  `SELECT 1 FROM fs_bench.countries_1 LIMIT 1`), so TPC-H-only setups
  keep working.
- `.bench_ronsql fs_hw` / `.bench_sql fs_hw` — run the whole category.
- **Vector-read benchmark (E5 stretch)**: `.bench_vector <view> [T] [N]`
  executes, per request, the full statement set of one feature-view
  spec (e.g. S1 + S6b + S7 for one entity, or the batch variants) on
  one engine and reports the end-to-end latency of assembling the
  vector. This is the client-visible number and needs a new runner
  loop (sequential statements per request; the same collectors).
- Warmup, progress, `LatencyCollector` (min / avg / max / p95 / p99 /
  p99.9), error collection, and the phase breakdown table are reused
  as is.

---

## 5. Cluster and load

```
cd <build>/mysql-test
./mtr --suite=ronsqlcrunch setup --start-and-exit [--defaults-extra-file=suite/ronsqlcrunch/cpubind.cnf]
<build>/runtime_output_directory/rondb --mysql-port <MASTER_MYPORT> --rdrs-port <RDRS port> --no-rondis
.fs_load 1 8 500 --hash-twin    # include the table required by fs_hw_hash_point
.bench_ronsql fs_hw 1 200       # 1 thread × 200 requests per query
.bench_sql fs_hw 1 200
```
`.fs_load` is idempotent (`CREATE … IF NOT EXISTS`, skips tables whose
checksum already matches) so repeated matrix runs do not reload.
Every benchmark declares required tables; check these before timing.
A requested case with missing prerequisites is an error, not a timing
sample. At sf 3 omit hash_point explicitly, as the hash twin does not
fit the planned memory budget.

---

## 6. Matrix driver changes (`ronsql_bench_matrix.py`)

- `--queries fs_hw` → new category lambda `n.startswith('fs_hw_')`, and
  the existing `fs` lambda excludes `fs_hw_` so the TPC-H family is
  unchanged.
- `--load tpch|fs|both` (default `tpch` for backward compatibility):
  `fs` runs `.fs_load <sf> <load-threads> <load-batch>` instead of
  `.load_tpch`; `--sf` applies to whichever is loaded. The selected
  case prerequisites determine whether `--hash-twin` is needed;
  reject incompatible scale/memory selections before loading.
- Report sections A–F apply unchanged (compiler OFF/ON per engine,
  RonSQL vs MySQL, phase breakdown, mysqld NDB-API wait split,
  `ndbinfo.jit` deltas, thread scaling). One extra derived column for
  RonSQL rows: `client_overhead = latency − execute − prepare`.
- Output directory convention for this framework:
  `<out>/fs_hw/<date>-<build>/` with `results.json`, `report.md`,
  `cases/*.txt`.

---

## 7. Plan pins

Each entry carries the plan facts the benchmark assumes; `.fs_explain`
checks them at warmup and prints a warning (not a failure) when the
plan differs, so a regression in latency can be told apart from a plan
change:

| entry | expected RonSQL plan facts |
|---|---|
| `fs_hw_agg_point`, `_window7d`, `_greatest`, `_filter`, `strkey_point`, `composite_point`, `sessions_window2h` | `Execute as index scan.` + `` Index: `PRIMARY` `` (observed run 1: goodness 110001 / 110111, 1 or 2 bounds) |
| `fs_hw_agg_batch*`, `strkey_batch100` | **observed run 1: `Execute as table scan.` for every list size** (F12) — pinned as observed, so a planner fix (index ranges per key) shows up as a pin warning and explains the latency drop |
| `fs_hw_collect5/50` | `Execute as index scan.`, `ORDER BY: index order (SF_OrderBy | SF_Descending merge of the fragment scans, streamed, no client-side sort).`, `Result limited to N rows.` (observed) |
| `fs_hw_snow1/2_point`, `snow2_left_chain` | `Body root: INDEX_SCAN using PRIMARY`, `[ROOT] CTE_SCAN CTE:b AS b`, `[INNER] PK_LOOKUP regions_1 AS j2` (+ `countries_1 AS j3`) (observed) |
| `fs_hw_snow1_batch100` | observed: `Body root: TABLE_SCAN` (the IN list on the CTE body, F12), then CTE_SCAN + PK_LOOKUP |
| `fs_hw_snow2_left_single` | `[LEFT JOIN] PK_LOOKUP` ×2 (observed) |
| `fs_hw_hash_point` | `Execute as table scan.` (no ordered index) — the point of the entry; not yet observed (run 1 stopped before it) |

---

## 8. Results and thresholds

Filled by E5 from the first matrix run (interpreter and JIT arms,
1 and 8 threads, sf 1, `ronsqlcrunch` topology). Stored as
`bench_results/<date>-<build>.md` next to this file (report.md copy,
≤ 30 KB) plus the `results.json`.

### Run 2 — 2026-09-11, `prod_build`, macOS (Apple silicon), `ronsqlcrunch` topology, sf 1, RonSQL vs the unpushed MySQL server, 1 and 8 threads, interpreter OFF (JIT ON within noise on every entry)

Stored as `bench_results/2026-09-11-prod_build-run2/`.  Complete: 188
cases, all 25 entries incl. the twins; `collect5_cte` rejects on RonSQL
(F0) and `hash_point` had no `transactions_hash_1` (the hash twin is
loaded only with `--hash-twin` at sf > 0.1; the driver now skips the
entry and says so).  `nopush` = MySQL with pushdown aggregation OFF (the
pushed arm crashes / fails on this set, F10 / F11; its numbers are in
run 1 below).  `ratio` = nopush / RonSQL at 1 thread (> 1 = RonSQL faster).

| entry (fs_hw_) | RonSQL avg | RonSQL p99 | execute | rows/req | nopush avg | ratio | RonSQL @8 | nopush @8 |
|---|---|---|---|---|---|---|---|---|
| floor | 102 µs | 170 µs | 69 µs | 0.0 | 92 µs | 0.90x | 271 µs | 260 µs |
| agg_point | 112 µs | 153 µs | 74 µs | 0.0 | 113 µs | 1.01x | 299 µs | 288 µs |
| agg_window7d | 124 µs | 253 µs | 85 µs | 0.0 | 113 µs | 0.91x | 298 µs | 286 µs |
| agg_greatest | 106 µs | 154 µs | 73 µs | 0.0 | 111 µs | 1.05x | 298 µs | 289 µs |
| agg_filter | 106 µs | 156 µs | 72 µs | 0.0 | 112 µs | 1.06x | 300 µs | 293 µs |
| agg_batch10 | 208 ms | 214 ms | 208 ms | 0.0 | 237 µs | 0.00x | 825 ms | 557 µs |
| agg_batch100 | 612 ms | 642 ms | 612 ms | 0.0 | 1.86 ms | 0.00x | 3033 ms | 4.90 ms |
| agg_batch1000 | 4381 ms | 4390 ms | 4380 ms | 0.0 | 42.77 ms | 0.01x | 26584 ms | 153 ms |
| agg_batch100_window | 553 ms | 556 ms | 553 ms | 0.0 | 1.33 ms | 0.00x | 3199 ms | 3.68 ms |
| collect5 | 108 µs | 177 µs | 74 µs | 4.2 | 116 µs | 1.07x | 288 µs | 293 µs |
| collect50 | 112 µs | 177 µs | 77 µs | 17.6 | 129 µs | 1.14x | 293 µs | 299 µs |
| collect5_cte | FAIL | FAIL | - | - | 119 µs | - | FAIL | 299 µs |
| snow1_point | 519 µs | 937 µs | 465 µs | 0.9 | 125 µs | 0.24x | 872 µs | 316 µs |
| snow2_point | 526 µs | 739 µs | 476 µs | 0.8 | 174 µs | 0.33x | 910 µs | 409 µs |
| snow1_batch100 | 19.97 ms | 30.06 ms | 19.85 ms | 89.0 | 4.71 ms | 0.24x | 90.27 ms | 12.67 ms |
| snow2_left_chain | 530 µs | 774 µs | 479 µs | 0.8 | 171 µs | 0.32x | 937 µs | 415 µs |
| snow2_left_single | 531 µs | 818 µs | 479 µs | 1.0 | 175 µs | 0.33x | 904 µs | 406 µs |
| strkey_point | 111 µs | 164 µs | 77 µs | 0.0 | 120 µs | 1.08x | 296 µs | 292 µs |
| strkey_batch100 | 369 ms | 375 ms | 369 ms | 0.0 | 3.92 ms | 0.01x | 2166 ms | 6.09 ms |
| composite_point | 108 µs | 182 µs | 74 µs | 0.0 | 113 µs | 1.05x | 287 µs | 278 µs |
| hash_point | FAIL | FAIL | - | - | FAIL | - | FAIL | FAIL |
| sessions_window2h | 107 µs | 184 µs | 73 µs | 0.0 | 106 µs | 0.99x | 288 µs | 272 µs |

RonSQL template vs the MySQL production twin (question 2 of §1; `same
text` = the RonSQL statement on the MySQL server, `twin` = the statement
Hopsworks runs on the SQL path; ratio = twin / RonSQL, > 1 = the RonSQL
path is faster):

| entry (fs_hw_) | RonSQL | same text | twin | twin/RonSQL @1 | RonSQL @8 | same text @8 | twin @8 | twin/RonSQL @8 |
|---|---|---|---|---|---|---|---|---|
| collect5 | 108 µs | 116 µs | 142 µs | 1.31x | 288 µs | 293 µs | 321 µs | 1.12x |
| snow1_point | 519 µs | 125 µs | 126 µs | 0.24x | 872 µs | 316 µs | 296 µs | 0.34x |
| snow2_point | 526 µs | 174 µs | 144 µs | 0.27x | 910 µs | 409 µs | 347 µs | 0.38x |

Reading of run 2 (adds to run 1 below):

- **Point shapes** (S1, S2, S4, S5, S9, S10, S6b): parity at 1 thread
  (RonSQL 104–124 µs, MySQL 106–129 µs) and at 8 threads (~290–300 µs
  on both, ~26 k q/s RonSQL vs ~27 k q/s MySQL — this laptop's 8-thread
  ceiling, T8/T1 ≈ 3× for both engines).  The collect twin
  (ROW_NUMBER window) costs 1.3× the RonSQL template at 1 thread and
  1.1× at 8, so for collect the RonSQL path is the faster serving path.
- **Snowflake** (S7, S8, S8b): 3–4× slower than MySQL at 1 thread
  (F13); at 8 threads the gap narrows to 2.2–2.9× (RonSQL scales 4.5–4.8×
  with threads, MySQL 3.2–3.4×), consistent with a round-trip-bound
  execution.  The production nested-join twins are the fastest of all
  (126 / 144 µs), so today the SQL path serves snowflake vectors 4×
  faster than the RonSQL templates.
- **Batch IN lists** (S3, S10 batch): F12 unchanged; at 8 threads RonSQL
  degrades further (825 ms / 3.0 s / 26.6 s for 10 / 100 / 1000 keys,
  T8/T1 1.3–2×: the table scans serialise on the data nodes) while
  MySQL keeps scaling (557 µs / 4.9 ms / 153 ms).
- **Interpreter vs JIT**: no entry differs by more than noise.

### Run 3 — 2026-09-11, the pushed mysqld arm (`ndb_pushdown_aggregate=ON`) on the run-2 cluster, 19 entries (F10 / F11 entries excluded), 1 and 8 threads

Stored as `bench_results/2026-09-11-prod_build-run3-pushed/`.  All 76
cases succeeded.  Pushed against unpushed (run 2), 1 thread / 8 threads:

| entry (fs_hw_) | pushed @1 | unpushed @1 | pushed @8 | unpushed @8 | pushed scan batches/req |
|---|---|---|---|---|---|
| floor, agg_window7d, agg_greatest, composite_point, sessions_window2h | 106–119 µs | 92–113 µs | 282–290 µs | 260–289 µs | 3–4 |
| collect5 / collect50 / collect5_twin | 113 / 116 / 146 µs | 116 / 129 / 142 µs | 289 / 300 / 322 µs | 293 / 299 / 321 µs | 3 |
| snow1_point / snow2_point / twins / LEFT variants | 119–169 µs | 125–175 µs | 299–409 µs | 296–415 µs | 0 (no push) |
| agg_batch10 / 100 / 1000 | 972 µs / 10.4 ms / 172 ms | 237 µs / 1.9 ms / 43 ms | 2.2 ms / 23.6 ms / 541 ms | 557 µs / 4.9 ms / 153 ms | 31 / 313 / 3132 |
| agg_batch100_window | 10.1 ms | 1.3 ms | 22.6 ms | 3.7 ms | 294 |
| snow1_batch100 | 8.6 ms | 4.7 ms | 21.4 ms | 12.7 ms | 0 |

Pushdown aggregation is neutral on every point shape (the pushed scan
returns the same rows in the same 3–4 batches) and 4× slower on the
batch IN lists, one pushed index scan per key range against the MRR
reads of the unpushed path; it also cannot run three point entries
(F11) and crashes on the string-key batch (F10).  For the Hopsworks
shapes the unpushed MySQL server is the reference baseline.

### Run 1 — 2026-09-11, `prod_build`, macOS (Apple silicon), `ronsqlcrunch` topology, sf 1, 1 thread, interpreter OFF (JIT ON within noise)

Stored as `bench_results/2026-09-11-prod_build-run1/` (`report.md`,
`results.json`); raw CLI output stayed in `/tmp/fs_hw_run1/cases/`.  The
run stopped at the 22nd entry when the pushed-aggregation mysqld arm
crashed on `strkey_batch100` (F10), so `composite_point`, `hash_point`,
`sessions_window2h`, the three `_twin` entries and every 8-thread case
are missing; see the rerun plan below.  `mysqld` = pushdown aggregation
ON, `nopush` = OFF; `FAIL` = the warmup request failed (F11 on the
pushed arm, F0 for the CTE collect on RonSQL).

| entry (fs_hw_) | RonSQL avg | RonSQL p99 | execute | client overhead | rows/req | mysqld avg | nopush avg |
|---|---|---|---|---|---|---|---|
| floor | 99 µs | 169 µs | 67 µs | 30 µs | 0.0 | 95 µs | 100 µs |
| agg_point | 104 µs | 144 µs | 72 µs | 29 µs | 0.0 | FAIL | 111 µs |
| agg_window7d | 120 µs | 247 µs | 82 µs | 34 µs | 0.0 | 126 µs | 112 µs |
| agg_greatest | 105 µs | 146 µs | 73 µs | 28 µs | 0.0 | 110 µs | 111 µs |
| agg_filter | 103 µs | 141 µs | 71 µs | 28 µs | 0.0 | FAIL | 116 µs |
| agg_batch10 | 204 ms | 208 ms | 204 ms | 144 µs | 0.0 | 988 µs | 251 µs |
| agg_batch100 | 572 ms | 577 ms | 572 ms | 173 µs | 0.0 | 10.09 ms | 1.81 ms |
| agg_batch1000 | 4384 ms | 4427 ms | 4384 ms | 192 µs | 0.0 | 169 ms | 42.32 ms |
| agg_batch100_window | 545 ms | 547 ms | 545 ms | 172 µs | 0.0 | 10.11 ms | 1.22 ms |
| collect5 | 104 µs | 159 µs | 73 µs | 29 µs | 4.2 | 113 µs | 110 µs |
| collect50 | 116 µs | 180 µs | 80 µs | 32 µs | 17.6 | 118 µs | 121 µs |
| collect5_cte | FAIL | - | - | - | - | 121 µs | 119 µs |
| snow1_point | 483 µs | 665 µs | 435 µs | 43 µs | 0.9 | 120 µs | 119 µs |
| snow2_point | 512 µs | 686 µs | 462 µs | 43 µs | 0.8 | 172 µs | 166 µs |
| snow1_batch100 | 19.60 ms | 29.75 ms | 19.49 ms | 91 µs | 89.0 | 8.17 ms | 4.45 ms |
| snow2_left_chain | 513 µs | 685 µs | 465 µs | 43 µs | 0.8 | 164 µs | 164 µs |
| snow2_left_single | 511 µs | 687 µs | 462 µs | 43 µs | 1.0 | 164 µs | 163 µs |
| strkey_point | 108 µs | 155 µs | 75 µs | 29 µs | 0.0 | FAIL | 119 µs |
| strkey_batch100 | 381 ms | 384 ms | 381 ms | 164 µs | 0.0 | - | - |

Reading (the four questions of §1):

1. **Same text, RonSQL vs MySQL.**  Point aggregates (S1, S2, S4, S5,
   S10) and collect (S6b) are at parity: ~100–120 µs end to end on
   both, of which ~70 µs is the data-node round trip (`firstbatch`) and
   ~30 µs RDRS HTTP + client.  **Batch IN lists (S3) are 200–1000×
   slower on RonSQL** (204 ms / 572 ms / 4.4 s for 10 / 100 / 1000 keys
   against 0.99 / 10 / 169 ms pushed and 0.25 / 1.8 / 42 ms unpushed):
   the IN list becomes an OR chain and the plan is a table scan of the
   3.6 M-row history with the OR filter — F12.  **Snowflake point reads
   (S7, S8, S8b) are 3–4× slower** (483–525 µs vs 120–172 µs): the CTE
   body is an `INDEX_SCAN using PRIMARY` and the children `PK_LOOKUP`s,
   but the CTE_SCAN execution costs ~350 µs of round trips that MySQL's
   two or three plain PK reads do not pay — F13.  The LEFT variants
   cost the same as INNER.  `snow1_batch100` is 2.4× slower than pushed
   MySQL and its CTE body is a `TABLE_SCAN` of `customers_1` (F12 again).
2. **Template vs production twin** — not measured: the `_twin` entries
   sit at the end of the query order and were not reached.
3. **Knobs.**  Interpreter vs JIT: no measurable difference on any entry
   (the per-request row counts are tiny; the batch scans gain ≤ 3 %).
   Batch size: linear in the OR-chain length on RonSQL, linear in keys
   on MySQL.  8 threads: not reached.
4. **Where the time goes.**  RDRS schema cache: `load` is 1–2 µs per
   request in this run (the cache is warm), so the ~400 µs `listIndexes`
   gap of `ronsql_bench_matrix.md` is not present here; engine time is
   `firstbatch`.  Client overhead is a flat ~30 µs (43 µs for the
   snowflake shapes, 90–200 µs for the batch results).

Side observation for the MySQL side: with pushdown aggregation ON the
batch IN list is 4× slower than unpushed (one pushed scan per key range,
3132 scan batches per request at 1000 keys, against MRR reads of the
rows), and pushed point aggregates fail (F11) or crash (F10) — the
pushed-aggregation path is not ready for the S3/S10 shapes either.

Rerun plan: (a) `--engines ronsql,mysqld_nopush --threads 1,8
--keep-cluster` for a complete, crash-free matrix incl. the twins and the
three missing entries; (b) `--engines mysqld --no-start --no-load
--queries <all but fs_hw_strkey_batch100>` on the kept cluster for the
pushed arm.

Regression rule (applied by hand or by a small script over two
`results.json` files): flag when `avg` worsens by more than 15 % or
`p99` by more than 25 % against the stored baseline for the same
build type and topology; a plan-pin warning on the same entry
explains it as a plan change rather than a code regression.

Expectations to sanity-check the first run against (not thresholds):
point aggregates and 1-hop snowflakes should be sub-millisecond in
engine time at 1 thread; `batch1000` and `collect50` are dominated by
result size; `hash_point` should be visibly worse than `agg_point`;
`collect5_cte` is expected to reject cleanly until the engine supports
the shape (R1), and its row stays in the table as `REJECT`.

---

## 9. Deliverables for E5

1. `fsq/cases` bench entries + golden dump + Go test that the registry
   equals the emitter output.
2. `ronsql_bench.go`: category `benchCatFSHW`, placeholder resolver,
   listing section, `fs_hw` group run, `KeySQL` per entry, twins.
3. `ronsql_bench_matrix.py`: `--queries fs_hw`, `--load fs|tpch|both`,
   `client_overhead` column.
4. `.fs_explain` warmup plan pins (warnings).
5. First results in `bench_results/`, this file's §8 filled, and a
   paragraph in `ronsql_cli_benchmarks.md` pointing here.
6. Stretch: `.bench_vector`, `--key-class`.
