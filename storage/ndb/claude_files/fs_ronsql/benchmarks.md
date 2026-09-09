# Benchmark design (P5, v1 — 2026-09-08)

**Status: design complete, nothing built; the results tables in §8 are
empty until the first E5 run.** Builds on the rondb-cli benchmark
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
| `fs_hw_agg_point`, `_window7d`, `_greatest`, `_filter` | single-table, ordered PK index range on `customer_id` (+ `event_time` bound), no table scan |
| `fs_hw_agg_batch*` | index ranges per key (OR chain) — if the planner falls back to a table scan for large lists, record the threshold |
| `fs_hw_collect5/50` | pass-through, `ORDER BY: index order (SF_OrderBy | SF_Descending …)`, `rows=N` |
| `fs_hw_snow*` | `[ROOT] CTE_SCAN b`, `[INNER] PK_LOOKUP regions_1 AS j2`, `[INNER] PK_LOOKUP countries_1 AS j3` |
| `fs_hw_hash_point` | table scan with filter (no ordered index) — the point of the entry |
| `fs_hw_sessions_window2h` | which index serves the bound: PK ordered (`customer_id, event_time`) rather than `ttl_index(event_time)` |

---

## 8. Results and thresholds

Filled by E5 from the first matrix run (interpreter and JIT arms,
1 and 8 threads, sf 1, `ronsqlcrunch` topology). Stored as
`bench_results/<date>-<build>.md` next to this file (report.md copy,
≤ 30 KB) plus the `results.json`.

| entry | engine | threads | avg | p99 | rows/req | execute µs | client overhead µs |
|---|---|---|---|---|---|---|---|
| (empty until E5) | | | | | | | |

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
