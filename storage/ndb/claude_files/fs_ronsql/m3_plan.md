# M3 — serving performance: the performance census (M3.0)

Written 2026-09-22. `ronsql_fs_support_plan.md` §3 defines M3 as the
milestone where the `fs_hw` targets of WP-F (F12, batch IN lists) and
WP-G (F13, snowflake round trips) are met. M3.0 is the step before any
engine work: measure the whole RonSQL feature set on the current engine,
latency and throughput, and let the numbers say which shapes need work
and in which order.

**Status: tooling written 2026-09-22, not yet built or run (the user
builds and runs). §6 is empty until run 4 exists.**

## 0. Why a census, and why now

- The only performance numbers on record are the three `fs_hw` runs of
  2026-09-11 (`benchmarks.md` §8, `findings/bench.md` F10–F13). They
  predate everything that changed the execution paths since: M1 (the
  collect CTE collapse F0, binary projections F7, error classes), M2
  (checked 64-bit SUM in the shared merge, AVG / FLOAT formatting, CTE
  result routing through TC) and the RONDB-1120 CTE work (identity
  addressing, `JOIN_AGG_SETUP` overlapped with execution, single-word
  feed signals, scan slots and lookup close). None of those was
  measured; any of them may have moved a shape in either direction.
- The `fs_hw` set answers the Hopsworks question only. The rest of the
  registry (`fs`, `offline_fs`, `tpch_cte`, 23 entries over TPC-H) was
  last run on the compiled-interpreter branch and never through the
  regression rule. Nothing isolates the engine primitives the shapes are
  built from, so a slow shape today needs a manual EXPLAIN-and-guess to
  attribute.
- `prod_build` is from 2026-09-11 (pre-M1). Numbers need a rebuild
  first; a debug build is only good for proving the plumbing.

## 1. The benchmark set

One matrix run covers every category of the rondb-cli registry
(`.bench_ronsql list`); the MySQL server on the identical statement is
the reference on every entry, and the production twins (`fs_hw_*_twin`,
`tpch_q*_official`) run on MySQL only.

| category | entries (RonSQL-capable + MySQL-only) | data set | what it answers |
|---|---|---|---|
| `core` (new, M3.0) | 10 | tpch sf 1 | engine primitives, one access path or execution stage each (§2) |
| `fs` | 11 | tpch | online feature-store CTE shapes: point / segment CTE bodies, CTE_LOOKUP children, projection over a CTE, ORDER BY / LIMIT on the pass-through path, DNF filters |
| `offline_fs` | 7 | tpch | full-table CTE materialization: scan + group + merge at 1.5–6 M rows, chained CTEs, anti-join, wide output |
| `tpch_cte` | 5 + 5 official | tpch | the analytics envelope: CTE rewrites of Q2 / Q11 / Q13 / Q15 / Q22 against MySQL on the same text and on the official form |
| `fs_hw` | 22 + 3 twins | fs_bench sf 1 | the Hopsworks serving shapes S1–S10 (`benchmarks.md` §2), batch sizes 10 / 100 / 1000, collect 5 / 50, snowflake depth 1 / 2, string keys, composite keys, hash-only PK |

55 RonSQL-capable entries. Every entry runs on `ronsql` and on
`mysqld_nopush` (the unpushed MySQL server is the reference baseline for
the Hopsworks shapes since run 3: the pushed arm crashes on F10 and fails
F11), at 1 thread (latency) and 8 threads (throughput on this laptop's
8-thread ceiling), with the compiled interpreter OFF and ON, interleaved
per query so cache warm-up cannot masquerade as a JIT effect.

### 1.1 The `core` category

`tools/rondb-cli/internal/shell/ronsql_bench.go` (category
`benchCatCore`). Every entry carries plan pins for the access path it is
meant to measure, so the warmup prints its EXPLAIN into the case log and a
`Plan pin missing` warning says the entry now measures something else.
`{KEY}` / `{KEYS:n}` on `orders` resolve to existing order keys
(`tpchOrderKeyResolver`: `.load_tpch` writes multiples of 4, so the plain
uniform draw would miss three requests in four); the customer-keyed
entries reuse the `fs_hw` resolver.

| entry | statement (shape) | rows touched at sf 1 | pins |
|---|---|---|---|
| `core_pk_lookup` | projection of one order by `o_orderkey = {KEY}` | 1 | `Execute as primary key lookup.` |
| `core_in_pk100` | `COUNT / SUM / MAX … WHERE o_orderkey IN ({KEYS:100})` | 100 | none: the run records the plan (batched PK reads or a filtered scan is the question) |
| `core_in_idx100` | `o_custkey, COUNT, SUM … WHERE o_custkey IN ({KEYS:100}) GROUP BY o_custkey` | ~1 k via `idx_orders_custkey` | none (F12 off the PK) |
| `core_idx_range` | one month of orders via `idx_orders_orderdate`, three aggregates | ~19 k | index scan on `idx_orders_orderdate` |
| `core_avg_range` | `COUNT, AVG, AVG, MIN` over a random 100-customer segment | ~1 k | index scan on `idx_orders_custkey` |
| `core_pass_range` | four columns of the same segment, no ORDER BY | ~1 k drained | index scan on `idx_orders_custkey` |
| `core_scan_agg` | five scalar aggregates over all of `lineitem` | 6 M | table scan, `No filters.` |
| `core_scan_filter` | `COUNT, SUM` over `lineitem` with a 3-conjunct filter on unindexed columns | 6 M scanned, ~4 % qualify | table scan, `FILTERS:` |
| `core_group_few` | all of `orders` `GROUP BY o_orderstatus` | 1.5 M, 3 groups | table scan |
| `core_group_many` | all of `orders` `GROUP BY o_custkey` | 1.5 M, ~100 k groups | table scan |

## 2. What the pairs isolate

The value of `core` is in the differences, read together with the shape
entries:

| difference | attributes |
|---|---|
| `core_pk_lookup` vs `fs_hw_agg_point` (index scan + aggregation on ~36 rows) | the cost of the aggregation program and the scan protocol over a plain PK read; both against the MySQL PK read |
| `core_in_pk100` vs `core_in_idx100` vs `fs_hw_agg_batch100` (PK prefix) | whether F12's table scan is specific to the PK-prefix IN list, to secondary indexes, or to every IN list; and whether a complete-PK list is already batched lookups |
| `core_idx_range` vs `core_avg_range` | the AVG path (M2.3) against COUNT / SUM / MAX on the same access path |
| `core_pass_range` vs `fs_history` (same rows, ORDER BY + LIMIT) | the client-side sort of the pass-through path |
| `core_pass_range` vs `core_avg_range` | result drain and printing of ~1 k rows against a one-row aggregate over the same scan |
| `core_scan_agg` OFF vs ON | the compiled interpreter on the row volume where it can show; `core_scan_filter` adds the per-row filter |
| `core_group_few` vs `core_group_many` | per-fragment group tables, the API-side partial merge and a 100 k-row result, at equal scan volume |
| `core_group_many` vs `offline_fs_scalar` (the same grouping as a CTE body, scalar main) | CTE materialization of a 100 k-group body against printing it |
| `fs_hw_snow1_point` vs `core_pk_lookup` × 2 | F13: the CTE_SCAN + PK_LOOKUP round trips against two plain PK reads |

## 3. Run recipe

Prerequisites, in order:

1. Rebuild `prod_build` at HEAD (its binaries are dated 2026-09-11,
   before M1): `ndbmtd ndb_mgmd ndb_mgm mysqld mysql mysqladmin rdrs2
   rondb-cli`. The `core` entries exist only in a rebuilt `rondb-cli`.
2. Stop the other cluster on this machine (`rondb_2604_main/debug_build`,
   two `ndbmtd` at ~9 % CPU and 1.6 GB each, uptime three days at the
   time of writing): it shares the CPUs and the memory the census needs.
3. Nothing else heavy on the laptop for ~2 hours.

One run, both data sets, every category (`census.cnf` raises
`DataMemory` to 6 G so TPC-H sf 1 and fs_bench sf 1 fit side by side):

```
cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py \
    --build prod_build --queries all --load both --sf 1 \
    --engines ronsql,mysqld_nopush --threads 1,8 --seconds 5 \
    --cpubind mysql-test/suite/ronsqlcrunch/census.cnf \
    --keep-cluster --out /tmp/census_run4
```

Sizing: 63 registry entries (55 RonSQL-capable + 8 MySQL-only) × up to 2
engines × 2 arms × 2 thread counts ≈ 460 cases of ~5 s plus the probe
per (engine, query, arm); the full scans (`core_scan_*`, `offline_fs_*`,
`core_group_*`) and `fs_hw_agg_batch1000` run at the 5-request minimum
and dominate the wall time. Expect 1.5–2.5 hours. `--keep-cluster`
leaves the loaded cluster up for re-runs of single entries
(`--no-start --no-load --mysql-port … --rdrs-port … --connectstring …`
as printed by the driver's last log line) and for `.explain_ronsql` on
anything the triage flags; stop it with `--stop`.

On a dedicated Linux benchmark computer, the same run with the cluster
bound to its own CPUs and the client on others, and a wider thread
sweep for the throughput ceiling (the machine, not the client, should
be the limit): copy `cpubind.cnf`'s `cpubind=` lines into `census.cnf`
(one file goes through `--cpubind`), set `NumCPUs` to the data-node set
size, and run

```
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py \
    --build prod_build --queries all --load both --sf 1 \
    --engines ronsql,mysqld_nopush --threads 1,8,32 --seconds 10 \
    --cpubind mysql-test/suite/ronsqlcrunch/census.cnf --client-cpus <list> \
    --keep-cluster --out /tmp/census_run4
```

The triage script uses the lowest thread count for latency and the
highest for throughput, whatever they are.

Then the triage:

```
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_triage.py /tmp/census_run4 \
    --baseline storage/ndb/claude_files/fs_ronsql/bench_results/2026-09-11-prod_build-run2 \
    --out /tmp/census_run4/triage.md
```

The baseline covers the `fs_hw` entries only (run 2 is the last complete
`ronsql` + `mysqld_nopush` matrix); the other categories get their
baseline from this run.

Archive: copy `report.md`, `results.json`, `triage.md` and the
`cases/*.txt` of every entry the triage lists to
`bench_results/2026-09-<dd>-prod_build-run4/`, fill §6 here and
`benchmarks.md` §8 (run 4), and add findings to `findings/bench.md`.

## 4. Triage rules

`ronsql_bench_triage.py` (next to the matrix driver) turns `results.json`
into the ranked list the census exists for:

- **Latency class** at the lowest thread count, interpreter arm:
  RonSQL avg / MySQL avg. `PARITY` ≤ 1.25×, `SLOW` ≤ 3×, `CRITICAL`
  above, `FAIL` when the RonSQL case did not run.
- **Throughput class** at the highest thread count: MySQL q/s / RonSQL
  q/s, same bands; plus the T8/T1 scaling of each engine, which separates
  "slow per request" from "serialises under load" (run 2: the batch IN
  lists scaled 1.3–2× against MySQL's 3×).
- **Attribution** per RonSQL case: execute share of the latency, the
  dominant server-side phase (`firstbatch` = data-node time, `drain` =
  result volume, `print` = formatting, `load` = RDRS dictionary), the
  `http+client` remainder, rows per request, the OFF/ON ratio, and any
  `Plan pin missing` line from the case log.
- **Regression rule** (`benchmarks.md` §8) against `--baseline`: avg
  worse by more than 15 % or p99 by more than 25 % on the same (query,
  engine, arm, threads) is `REGRESSION`; avg better by more than 15 % is
  `IMPROVED`. A pin warning on the same entry explains it as a plan
  change first.
- Section 1 of the report is the work list: FAIL, then CRITICAL, SLOW and
  regressions, ranked by the worse of the two ratios.

Reading the list into work packages:

| triage outcome | goes to |
|---|---|
| `fs_hw_agg_batch*`, `strkey_batch100`, `snow1_batch100`, `core_in_*` CRITICAL with `firstbatch` dominant and a table-scan plan | WP-F (F12), scoped by which IN-list forms are affected (§2) |
| `fs_hw_snow*` SLOW/CRITICAL with `firstbatch` ≫ two PK reads | WP-G (F13); `core_pk_lookup` gives the floor the design must approach |
| any entry whose `http+client` or `print` / `drain` dominates | RDRS / printer work, not the data node — a new package |
| `core_scan_*` or `offline_fs_*` with OFF/ON ≈ 1 | the compiled interpreter is not reaching that program; RONDB-1056 coverage |
| REGRESSION without a pin warning on an entry that was PARITY in run 2 | bisect over the M1 / M2 / RONDB-1120 commits before anything else |
| FAIL | a correctness or support gap: ledger entry first, benchmark second |

Expected picture, to be confirmed or overturned by run 4: point shapes
and collect at parity; F12 and F13 still open (no engine commit since
run 1 touched IN-list planning or the CTE_SCAN plan shape); the collect
CTE form (`fs_hw_collect5_cte`, `collect50_cte`) now at the cost of the
direct form (M1.3); `hash_point` measured for the first time
(`--load both` at sf 1 needs `--hash-twin` for it; otherwise it is
skipped with a log line).

## 5. Deliverables and verification (user-run)

Code written 2026-09-22:

| piece | where |
|---|---|
| `core` category: constant, ten entries, listing sections, plan-pin check, `tpchOrderKeyResolver` | `tools/rondb-cli/internal/shell/ronsql_bench.go` |
| help text | `tools/rondb-cli/internal/shell/repl.go` (query families) |
| driver: `--queries core`, `all` no longer picks up the `fs_hw` category-runner line as a query, help | `storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py` |
| triage script | `storage/ndb/claude_files/compiled_interpreter/ronsql_bench_triage.py` |
| memory for both data sets | `mysql-test/suite/ronsqlcrunch/census.cnf` |

```
# unit tests (the registry test checks name uniqueness across all categories)
cd tools/rondb-cli && go test ./internal/shell ./internal/fsq/...

# the listing: ten core_* entries under "Engine primitives" in both namespaces
cd ../../prod_build && make rondb-cli
runtime_output_directory/rondb --no-mysql --no-rdrs --no-rondis -e ".bench_ronsql list" | sed -n '/Engine primitives/,/fs_hw/p'
runtime_output_directory/rondb --no-mysql --no-rdrs --no-rondis -e ".query_ronsql core_in_pk100"

# plumbing on the debug build: sf 0.1, 1 thread, 2 s per case, the new category only
cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py \
    --build debug_build --queries core --quick --engines ronsql,mysqld_nopush --out /tmp/census_smoke
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_triage.py /tmp/census_smoke

# the census itself: §3
```

Expected from the smoke run: every `core_*` warmup prints its EXPLAIN
with no `Plan pin missing` warning except possibly `core_pk_lookup` (the
pin assumes the pass-through PK path prints the same line as the
aggregate one; if it warns, the case log has the wording to pin); no
FAIL rows; `core_in_pk100` and `core_in_idx100` show their plan in the
case log. The triage script was checked against runs 1 and 2 of
2026-09-11 (it reproduces F12 / F13 as the CRITICAL rows and flags the
p99-only movements between the two runs).

## 6. Results

Filled from run 4.
